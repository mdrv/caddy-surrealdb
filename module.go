package caddysurrealdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(&Middleware{})
	httpcaddyfile.RegisterHandlerDirective("surrealdb", parseCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("surrealdb", httpcaddyfile.Before, "file_server")
}

// Middleware is the caddy-surrealdb HTTP handler module.
type Middleware struct {
	// SurrealDB endpoint URL (e.g. ws://127.0.0.1:596, http://localhost:8000).
	Endpoint string `json:"endpoint,omitempty"`
	// Namespace and Database to use after auth.
	Namespace string `json:"namespace,omitempty"`
	Database  string `json:"database,omitempty"`

	// Auth — credentials and scope.
	Auth AuthConfig `json:"auth,omitempty"`

	// Schema — SurrealQL statements (typically DEFINE TABLE OVERWRITE ...)
	// executed once on Provision after the connection becomes ready. Each
	// string is sent as one Query call.
	Schema []string `json:"schema,omitempty"`

	// Queries and Routes mirror caddy-duckdb.
	Queries []QueryDef   `json:"queries,omitempty"`
	Routes  []QueryRoute `json:"routes,omitempty"`

	// Live queries.
	LiveQueries []LiveQueryDef `json:"live_queries,omitempty"`
	LiveRoutes  []LiveRoute    `json:"live_routes,omitempty"`

	// Request logging.
	LogTable         string   `json:"log_table,omitempty"`
	LogExcludePaths  []string `json:"log_exclude_paths,omitempty"`
	LogExcludeFields []string `json:"log_exclude_fields,omitempty"`

	// Tuning.
	BatchSize     int           `json:"batch_size,omitempty"`
	FlushInterval time.Duration `json:"flush_interval,omitempty"`
	BufferSize    int           `json:"buffer_size,omitempty"`
	Overflow      string        `json:"overflow,omitempty"`
	MaxRows       int           `json:"max_rows,omitempty"`

	// Resilience.
	TokenRefreshThreshold time.Duration `json:"token_refresh_threshold,omitempty"`
	HeartbeatInterval     time.Duration `json:"heartbeat_interval,omitempty"`
	HeartbeatTable        string        `json:"heartbeat_table,omitempty"`
	SchemaTimeout         time.Duration `json:"schema_timeout,omitempty"`

	// Management API.
	APIPath  string `json:"api_path,omitempty"`
	APIToken string `json:"api_token,omitempty"`
	RawSQL   bool   `json:"raw_sql,omitempty"`

	// CORS.
	CORSOrigin string `json:"cors_origin,omitempty"`

	// Internal state — set in Provision.
	mgr         *Manager
	reg         *QueryRegistry
	hub         *liveHub
	writer      *BatchWriter
	queryRoutes []*matchedRoute
	liveRoutes  []*matchedRoute
	logger      *zap.Logger
	ctx         caddy.Context
}

// CaddyModule returns the Caddy module info.
func (*Middleware) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.surrealdb",
		New: func() caddy.Module { return new(Middleware) },
	}
}

// Provision sets up the manager, registry, hub, and writer.
func (m *Middleware) Provision(ctx caddy.Context) error {
	m.ctx = ctx
	m.logger = ctx.Logger()

	m.applyDefaults()

	repl := caddy.NewReplacer()
	m.Endpoint = repl.ReplaceAll(m.Endpoint, "")
	m.Namespace = repl.ReplaceAll(m.Namespace, "")
	m.Database = repl.ReplaceAll(m.Database, "")
	m.Auth.Username = repl.ReplaceAll(m.Auth.Username, "")
	m.Auth.Password = repl.ReplaceAll(m.Auth.Password, "")
	m.Auth.Token = repl.ReplaceAll(m.Auth.Token, "")
	m.Auth.Access = repl.ReplaceAll(m.Auth.Access, "")
	m.Auth.BearerKey = repl.ReplaceAll(m.Auth.BearerKey, "")
	m.LogTable = repl.ReplaceAll(m.LogTable, "")

	if m.Endpoint == "" {
		return fmt.Errorf("surrealdb: endpoint is required")
	}
	if m.Auth.Level == "" {
		return fmt.Errorf("surrealdb: auth level is required (root|namespace|database|record|token)")
	}

	needsWS := len(m.LiveQueries) > 0
	m.mgr = NewManager(m.Endpoint, m.Namespace, m.Database, m.Auth,
		WithTokenRefresh(m.TokenRefreshThreshold),
		WithHeartbeat(m.HeartbeatInterval),
		WithHeartbeatTable(m.HeartbeatTable),
		WithLogger(m.logger),
		WithNeedsWS(needsWS),
	)
	if err := m.mgr.Start(ctx); err != nil {
		return err
	}

	// Wait briefly for first connect so Provision can apply schema. If the
	// server is down, proceed and let queries fail open until reconnect.
	readyCtx, cancel := context.WithTimeout(ctx, m.SchemaTimeout)
	defer cancel()
	if err := m.mgr.AwaitReady(readyCtx); err != nil {
		m.logger.Warn("surrealdb: not ready at Provision; schema will be applied on first reconnect",
			zap.Error(err), zap.Duration("waited", m.SchemaTimeout))
	} else if len(m.Schema) > 0 {
		m.applySchema(ctx)
	}

	m.reg = newQueryRegistry(m.mgr, m.MaxRows, m.logger)
	for i := range m.Queries {
		if err := m.reg.Register(&m.Queries[i]); err != nil {
			return err
		}
	}
	m.hub = newLiveHub(m.mgr, m.LiveQueries, m.logger)

	if m.LogTable != "" {
		m.writer = NewBatchWriter(m.mgr, BatchWriterConfig{
			Table:         m.LogTable,
			ExcludePaths:  m.LogExcludePaths,
			OmitFields:    m.LogExcludeFields,
			BatchSize:     m.BatchSize,
			FlushInterval: m.FlushInterval,
			BufferSize:    m.BufferSize,
			Overflow:      m.Overflow,
		}, m.logger)
	}

	// Pre-compile routes.
	for i := range m.Routes {
		m.queryRoutes = append(m.queryRoutes, &matchedRoute{
			def:      &m.Routes[i],
			segments: compileRoute(m.Routes[i].Path),
		})
	}
	for i := range m.LiveRoutes {
		m.liveRoutes = append(m.liveRoutes, &matchedRoute{
			liveDef:  &m.LiveRoutes[i],
			segments: compileRoute(m.LiveRoutes[i].Path),
		})
	}
	return nil
}

// applyDefaults fills in defaults for any field left unspecified.
func (m *Middleware) applyDefaults() {
	if m.BatchSize == 0 {
		m.BatchSize = 500
	}
	if m.FlushInterval == 0 {
		m.FlushInterval = 200 * time.Millisecond
	}
	if m.BufferSize == 0 {
		m.BufferSize = 8192
	}
	if m.Overflow == "" {
		m.Overflow = "drop"
	}
	if m.MaxRows == 0 {
		m.MaxRows = 10000
	}
	if m.TokenRefreshThreshold == 0 {
		m.TokenRefreshThreshold = 5 * time.Minute
	}
	if m.HeartbeatInterval == 0 {
		m.HeartbeatInterval = 30 * time.Second
	}
	// HeartbeatTable intentionally has no default — leaving it empty keeps the
	// user's SurrealDB clean. The Version() ping still runs every
	// HeartbeatInterval for liveness / token-refresh purposes; opt in to a
	// status row via `heartbeat_table <name>` if you need cross-client
	// visibility from inside SurrealDB.
	if m.SchemaTimeout == 0 {
		m.SchemaTimeout = 5 * time.Second
	}
	if m.APIPath == "" {
		m.APIPath = "/_surreal"
	}
}

// applySchema executes Schema statements once the connection is ready.
func (m *Middleware) applySchema(ctx context.Context) {
	conn, err := m.mgr.Get(ctx)
	if err != nil {
		m.logger.Warn("surrealdb: schema skipped (not connected)", zap.Error(err))
		return
	}
	for i, stmt := range m.Schema {
		if _, err := surrealdb.Query[any](ctx, conn, stmt, nil); err != nil {
			m.logger.Error("surrealdb: schema statement failed",
				zap.Int("index", i), zap.String("sql", truncate(stmt, 120)),
				zap.Error(err))
		}
	}
	m.logger.Info("surrealdb: schema applied", zap.Int("statements", len(m.Schema)))
}

// Cleanup stops all subsystems.
func (m *Middleware) Cleanup() error {
	if m.writer != nil {
		m.writer.Close()
	}
	if m.mgr != nil {
		m.mgr.Stop()
	}
	return nil
}

// Validate checks internal consistency.
func (m *Middleware) Validate() error {
	// Routes reference known queries.
	qnames := make(map[string]bool, len(m.Queries))
	for _, q := range m.Queries {
		qnames[q.Name] = true
	}
	for _, r := range m.Routes {
		if r.QueryName == "" {
			return fmt.Errorf("route %s %s references no query", r.Method, r.Path)
		}
		if !qnames[r.QueryName] {
			return fmt.Errorf("route %s %s references unknown query %q",
				r.Method, r.Path, r.QueryName)
		}
	}
	liveNames := make(map[string]bool, len(m.LiveQueries))
	for _, q := range m.LiveQueries {
		liveNames[q.Name] = true
	}
	for _, r := range m.LiveRoutes {
		if !liveNames[r.QueryName] {
			return fmt.Errorf("live route %s %s references unknown live query %q",
				r.Method, r.Path, r.QueryName)
		}
	}
	return nil
}

// ServeHTTP is the main request entry point.
func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	m.applyCORS(w)

	// 1. Live routes first (they want streaming).
	for _, rt := range m.liveRoutes {
		if !strings.EqualFold(rt.liveDef.Method, r.Method) {
			continue
		}
		params := matchPath(rt.segments, r.URL.Path)
		if params == nil {
			continue
		}
		if !m.checkHeaders(r, rt.liveDef.RequireHeaders) {
			continue
		}
		format := m.liveFormat(rt.liveDef)
		switch format {
		case LiveWS:
			// WebSocket upgrade is handled by caddy's reverse_proxy / standard
			// websocket module in most deployments. We support SSE here; for
			// raw WS, declare format=ws and route via SSE passthrough.
			m.hub.serveSSE(w, r, rt.liveDef.QueryName)
		default:
			m.hub.serveSSE(w, r, rt.liveDef.QueryName)
		}
		return nil
	}

	// 2. Query routes.
	for _, rt := range m.queryRoutes {
		if !strings.EqualFold(rt.def.Method, r.Method) {
			continue
		}
		params := matchPath(rt.segments, r.URL.Path)
		if params == nil {
			continue
		}
		if !m.checkHeaders(r, rt.def.RequireHeaders) {
			continue
		}
		if rt.def.RateLimit != nil {
			rlKey := r.Method + "|" + r.URL.Path + "|" + extractIP(r)
			if !m.reg.checkRateLimit(rlKey, rt.def.RateLimit) {
				errorJSON(w, http.StatusTooManyRequests, "rate limit exceeded")
				return nil
			}
		}
		ctx := context.WithValue(r.Context(), pathParamsKey, params)
		rows, err := m.reg.Execute(ctx, rt.def.QueryName, r.WithContext(ctx), params)
		if err != nil {
			m.logger.Error("surrealdb: query failed",
				zap.String("query", rt.def.QueryName), zap.Error(err))
			errorJSON(w, http.StatusInternalServerError, "%v", err)
			return nil
		}
		// find OutputConfig for this query
		out := m.outputFor(rt.def.QueryName)
		writeOutput(w, out, rows)
		return nil
	}

	// 3. Management API.
	if strings.HasPrefix(r.URL.Path, m.APIPath+"/") || r.URL.Path == m.APIPath {
		return m.serveAPI(w, r)
	}

	// 4. Default: pass through and log.
	if m.writer != nil {
		rec := &responseRecorder{ResponseWriter: w}
		start := time.Now()
		err := next.ServeHTTP(rec, r)
		status := rec.status
		if status == 0 {
			// Handler returned without writing (e.g. file_server 404 via
			// HandlerError). Reconstruct from the error if possible.
			status = http.StatusOK
			if err != nil {
				var hErr caddyhttp.HandlerError
				if errors.As(err, &hErr) && hErr.StatusCode != 0 {
					status = hErr.StatusCode
				} else {
					status = http.StatusInternalServerError
				}
			}
		}
		m.writer.Write(RequestRecord{
			TS:        start.UTC(),
			IP:        extractIP(r),
			Method:    r.Method,
			Host:      r.Host,
			Path:      r.URL.Path,
			Query:     r.URL.RawQuery,
			Status:    status,
			LatencyMs: time.Since(start).Milliseconds(),
			BytesSent: rec.bytes,
			UserAgent: r.UserAgent(),
		})
		return err
	}
	return next.ServeHTTP(w, r)
}

// outputFor returns the OutputConfig for a named query (defaults to zero-value).
func (m *Middleware) outputFor(name string) OutputConfig {
	for _, q := range m.Queries {
		if q.Name == name {
			return q.Output
		}
	}
	return OutputConfig{Status: 200, Format: "json"}
}

// liveFormat resolves a live route's format (defaults to SSE).
func (m *Middleware) liveFormat(rt *LiveRoute) LiveFormat {
	for _, q := range m.LiveQueries {
		if q.Name == rt.QueryName {
			if q.Format == "" {
				return LiveSSE
			}
			return q.Format
		}
	}
	return LiveSSE
}

// checkHeaders returns false if any required header is missing or mismatches.
// Values support Caddy placeholders only at Provision (we already replaced them
// there for static config); runtime headers are compared verbatim.
func (m *Middleware) checkHeaders(r *http.Request, required map[string]string) bool {
	for k, v := range required {
		got := r.Header.Get(k)
		if v == "" {
			if got == "" {
				return false
			}
			continue
		}
		if got != v {
			return false
		}
	}
	return true
}

// applyCORS sets CORS headers if configured.
func (m *Middleware) applyCORS(w http.ResponseWriter) {
	if m.CORSOrigin == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", m.CORSOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	if m.CORSOrigin != "*" {
		w.Header().Set("Vary", "Origin")
	}
}

// serveAPI exposes GET /queries, GET /query/<name>, POST /sql.
func (m *Middleware) serveAPI(w http.ResponseWriter, r *http.Request) error {
	if m.APIToken != "" {
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if provided != m.APIToken {
			errorJSON(w, http.StatusUnauthorized, "unauthorized")
			return nil
		}
	}
	path := strings.TrimPrefix(r.URL.Path, m.APIPath)
	path = strings.TrimPrefix(path, "/")

	switch {
	case path == "" || path == "queries":
		if r.Method != http.MethodGet {
			errorJSON(w, http.StatusMethodNotAllowed, "GET only")
			return nil
		}
		writeJSON(w, OutputConfig{Status: 200}, m.reg.Names())
		return nil

	case strings.HasPrefix(path, "query/"):
		name := strings.TrimPrefix(path, "query/")
		if !m.reg.Has(name) {
			errorJSON(w, http.StatusNotFound, "no such query: %s", name)
			return nil
		}
		rows, err := m.reg.Execute(r.Context(), name, r, nil)
		if err != nil {
			errorJSON(w, http.StatusInternalServerError, "%v", err)
			return nil
		}
		writeOutput(w, m.outputFor(name), rows)
		return nil

	case path == "sql":
		if !m.RawSQL {
			errorJSON(w, http.StatusForbidden, "raw_sql disabled")
			return nil
		}
		if r.Method != http.MethodPost {
			errorJSON(w, http.StatusMethodNotAllowed, "POST only")
			return nil
		}
		conn, err := m.mgr.Get(r.Context())
		if err != nil {
			errorJSON(w, http.StatusServiceUnavailable, "%v", err)
			return nil
		}
		stmt := r.FormValue("q")
		if stmt == "" {
			// Fall back to raw request body (allows Content-Type: text/plain
			// clients and curl --data-binary without urlencoding).
			buf, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			stmt = strings.TrimSpace(string(buf))
		}
		if stmt == "" {
			errorJSON(w, http.StatusBadRequest, "empty query (pass form field 'q' or raw body)")
			return nil
		}
		res, qerr := surrealdb.Query[any](r.Context(), conn, stmt, nil)
		if qerr != nil {
			errorJSON(w, http.StatusBadRequest, "%v", qerr)
			return nil
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(res)
		return nil
	}

	errorJSON(w, http.StatusNotFound, "unknown api path: %s", path)
	return nil
}

// truncate clips s to n characters for logging.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Interface guards.
var (
	_ caddy.Provisioner           = (*Middleware)(nil)
	_ caddy.CleanerUpper          = (*Middleware)(nil)
	_ caddy.Validator             = (*Middleware)(nil)
	_ caddyhttp.MiddlewareHandler = (*Middleware)(nil)
)
