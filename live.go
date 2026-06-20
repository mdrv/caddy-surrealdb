package caddysurrealdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	"go.uber.org/zap"
)

// LiveFormat selects how live notifications are streamed to clients.
type LiveFormat string

const (
	LiveSSE LiveFormat = "sse" // Server-Sent Events
	LiveWS  LiveFormat = "ws"  // WebSocket
)

// LiveQueryDef declares a named LIVE SELECT. The Table must exist before the
// query is started; you can define it via Schema or rely on SurrealDB's
// SCHEMALESS defaults.
type LiveQueryDef struct {
	Name   string     `json:"name"`
	Table  string     `json:"table"`
	Where  string     `json:"where,omitempty"`  // optional WHERE clause (without the WHERE keyword)
	Diff   bool       `json:"diff,omitempty"`   // emit JSON-patch arrays instead of full records
	Format LiveFormat `json:"format,omitempty"` // default sse; ws requires WS-capable endpoint
}

// LiveRoute binds a LiveQueryDef to a (method, path) on the public surface.
type LiveRoute struct {
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	QueryName      string            `json:"query"` // LiveQueryDef.Name
	RequireHeaders map[string]string `json:"require_headers,omitempty"`
}

// liveHub tracks running live queries by id so we can share a single
// SurrealDB live query across many SSE/WS subscribers.
type liveHub struct {
	mgr    *Manager
	logger *zap.Logger

	mu   sync.Mutex
	subs map[string]*liveSubscription // by LiveQueryDef.Name
	defs map[string]*LiveQueryDef
}

type liveSubscription struct {
	def           *LiveQueryDef
	uuid          string
	refs          int
	notifications chan connection.Notification
	stop          chan struct{}
	done          chan struct{}
}

func newLiveHub(mgr *Manager, defs []LiveQueryDef, logger *zap.Logger) *liveHub {
	h := &liveHub{
		mgr:    mgr,
		logger: logger,
		subs:   make(map[string]*liveSubscription),
		defs:   make(map[string]*LiveQueryDef, len(defs)),
	}
	for i := range defs {
		h.defs[defs[i].Name] = &defs[i]
	}
	return h
}

func (h *liveHub) def(name string) (*LiveQueryDef, bool) {
	d, ok := h.defs[name]
	return d, ok
}

// subscribe registers a per-client fan-out and returns a notification channel.
// The first subscriber for a given LiveQueryDef starts the underlying
// SurrealDB LIVE SELECT; the last unsubscribe Kill()s it.
func (h *liveHub) subscribe(ctx context.Context, name string) (<-chan connection.Notification, error) {
	def, ok := h.def(name)
	if !ok {
		return nil, fmt.Errorf("unknown live query %q", name)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	sub, exists := h.subs[name]
	if !exists {
		conn, err := h.mgr.Get(ctx)
		if err != nil {
			return nil, err
		}
		liveID, err := surrealdb.Live(ctx, conn, models.Table(def.Table), def.Diff)
		if err != nil {
			return nil, fmt.Errorf("start live query: %w", err)
		}
		liveIDStr := uuidString(liveID)
		rawCh, err := conn.LiveNotifications(liveIDStr)
		if err != nil {
			_ = surrealdb.Kill(ctx, conn, liveIDStr)
			return nil, fmt.Errorf("open notifications: %w", err)
		}
		sub = &liveSubscription{
			def:           def,
			uuid:          liveIDStr,
			notifications: rawCh,
			stop:          make(chan struct{}),
			done:          make(chan struct{}),
		}
		h.subs[name] = sub
		h.logger.Info("surrealdb: live query started",
			zap.String("name", name),
			zap.String("table", def.Table),
			zap.String("live_id", sub.uuid))
	} else {
		h.logger.Info("surrealdb: reusing existing live query",
			zap.String("name", name), zap.Int("refs", sub.refs))
	}

	// Fan-out: per-subscriber channel with a small buffer.
	out := make(chan connection.Notification, 16)
	sub.refs++
	go func() {
		defer close(out)
		for {
			select {
			case n, ok := <-sub.notifications:
				if !ok {
					return
				}
				select {
				case out <- n:
				default:
					// drop on slow consumer
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	_ = out // returned below
	return out, nil
}

// unsubscribe decrements the refcount; when it hits zero the underlying live
// query is killed.
func (h *liveHub) unsubscribe(ctx context.Context, name string) {
	h.mu.Lock()
	sub, ok := h.subs[name]
	if !ok {
		h.mu.Unlock()
		return
	}
	sub.refs--
	if sub.refs > 0 {
		h.mu.Unlock()
		return
	}
	delete(h.subs, name)
	h.mu.Unlock()

	conn, err := h.mgr.Get(ctx)
	if err == nil {
		_ = surrealdb.Kill(ctx, conn, sub.uuid)
	}
	h.logger.Info("surrealdb: live query killed",
		zap.String("name", name),
		zap.String("live_id", sub.uuid))
}

// serveSSE streams notifications to an HTTP client using Server-Sent Events.
func (h *liveHub) serveSSE(w http.ResponseWriter, r *http.Request, name string) {
	h.logger.Info("surrealdb: SSE request",
		zap.String("name", name), zap.String("path", r.URL.Path))
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	ch, err := h.subscribe(ctx, name)
	if err != nil {
		h.logger.Warn("surrealdb: SSE subscribe failed", zap.String("name", name), zap.Error(err))
		fmt.Fprintf(w, "event: error\ndata: %s\n\n", jsonString(err.Error()))
		flusher.Flush()
		return
	}
	h.logger.Info("surrealdb: SSE subscribed", zap.String("name", name))
	defer h.unsubscribe(context.Background(), name)

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case n, ok := <-ch:
			if !ok {
				return
			}
			payload, err := encodeNotification(n)
			if err != nil {
				continue
			}
			event := "message"
			if n.Action != "" {
				event = string(n.Action)
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// encodeNotification marshals a live notification to JSON, normalizing any
// embedded RecordID values to their "table:id" string form (otherwise the
// Go JSON encoder would emit them as `{}`).
func encodeNotification(n connection.Notification) ([]byte, error) {
	normalized := normalizeForJSON(n.Result)
	type wire struct {
		ID     string `json:"id,omitempty"`
		Action string `json:"action"`
		Result any    `json:"result"`
	}
	out := wire{
		ID:     uuidString(n.ID),
		Action: string(n.Action),
		Result: normalized,
	}
	return json.Marshal(out)
}

// normalizeForJSON rewrites models.RecordID values encountered anywhere in
// `v` (maps, slices, direct) into "table:id" strings. Required because the
// default JSON encoder sees RecordID as a bare struct without field tags.
func normalizeForJSON(v any) any {
	switch x := v.(type) {
	case models.RecordID:
		return recordIDString(x)
	case *models.RecordID:
		if x == nil {
			return nil
		}
		return recordIDString(*x)
	case map[string]any:
		for k, vv := range x {
			x[k] = normalizeForJSON(vv)
		}
		return x
	case []any:
		for i := range x {
			x[i] = normalizeForJSON(x[i])
		}
		return x
	default:
		return v
	}
}

func recordIDString(r models.RecordID) string {
	if r.ID == nil {
		return r.Table
	}
	return fmt.Sprintf("%s:%v", r.Table, r.ID)
}

func uuidString(u *models.UUID) string {
	if u == nil {
		return ""
	}
	return fmt.Sprintf("%s", u.UUID)
}

func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%q", fmt.Sprintf("%v", v))
	}
	return string(b)
}
