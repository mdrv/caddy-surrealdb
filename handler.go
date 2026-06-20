package caddysurrealdb

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// matchedRoute is a precompiled QueryRoute (method+path pattern + pathParam keys).
type matchedRoute struct {
	def      *QueryRoute
	liveDef  *LiveRoute
	segments []segment
}
type segment struct {
	literal string
	isParam bool
	param   string
}

// compileRoute parses the path pattern into segments.
func compileRoute(path string) []segment {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	segs := make([]segment, 0, len(parts))
	for _, p := range parts {
		if strings.HasPrefix(p, ":") {
			segs = append(segs, segment{isParam: true, param: p[1:]})
		} else {
			segs = append(segs, segment{literal: p})
		}
	}
	return segs
}

// matchPath attempts to match a request path against the route's segments.
// On match it returns the extracted path params; otherwise nil.
func matchPath(segs []segment, path string) map[string]string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != len(segs) {
		// allow trailing slash collapse
		if !(len(parts) == len(segs)+1 && parts[len(parts)-1] == "") {
			return nil
		}
	}
	params := make(map[string]string, len(segs))
	for i, s := range segs {
		if i >= len(parts) {
			return nil
		}
		if s.isParam {
			if parts[i] == "" {
				return nil
			}
			params[s.param] = parts[i]
		} else if parts[i] != s.literal {
			return nil
		}
	}

	return params
}

// extractIP returns the client IP, honoring X-Forwarded-For / X-Real-IP.
func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.Index(xff, ","); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return xr
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		return host[:i]
	}
	return host
}

// headerChecker validates required headers (with Caddy replacer applied).
type headerChecker func(r *http.Request, repl *caddyReplacer) bool

type caddyReplacer interface {
	ReplaceAll(in string, _ any) string
}

// writeOutput renders rows to the response per OutputConfig.
func writeOutput(w http.ResponseWriter, out OutputConfig, rows any) {
	if out.Body != "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(coalesceStatus(out.Status, http.StatusOK))
		_, _ = w.Write([]byte(out.Body))
		return
	}
	switch strings.ToLower(out.Format) {
	case "csv":
		writeCSV(w, out, rows)
	case "ndjson":
		writeNDJSON(w, out, rows)
	case "raw":
		writeRaw(w, out, rows)
	default:
		writeJSON(w, out, rows)
	}
}

func coalesceStatus(s, def int) int {
	if s > 0 {
		return s
	}
	return def
}

func writeJSON(w http.ResponseWriter, out OutputConfig, rows any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(coalesceStatus(out.Status, http.StatusOK))
	rowSlice, _ := rows.([]map[string]any)
	var payload any = rowSlice
	if out.Envelope {
		payload = map[string]any{
			"data": rowSlice,
			"meta": map[string]any{
				"count":     len(rowSlice),
				"generated": time.Now().UTC().Format(time.RFC3339Nano),
			},
		}
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(payload)
}

func writeNDJSON(w http.ResponseWriter, out OutputConfig, rows any) {
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.WriteHeader(coalesceStatus(out.Status, http.StatusOK))
	rowSlice, _ := rows.([]map[string]any)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, row := range rowSlice {
		_ = enc.Encode(row)
	}
}

func writeCSV(w http.ResponseWriter, out OutputConfig, rows any) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.WriteHeader(coalesceStatus(out.Status, http.StatusOK))
	rowSlice, _ := rows.([]map[string]any)
	cw := csv.NewWriter(w)
	if len(rowSlice) == 0 {
		cw.Flush()
		return
	}
	// Header
	headers := make([]string, 0, len(rowSlice[0]))
	for k := range rowSlice[0] {
		headers = append(headers, k)
	}
	_ = cw.Write(headers)
	for _, row := range rowSlice {
		line := make([]string, 0, len(headers))
		for _, h := range headers {
			line = append(line, toString(row[h]))
		}
		_ = cw.Write(line)
	}
	cw.Flush()
}

func writeRaw(w http.ResponseWriter, out OutputConfig, rows any) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(coalesceStatus(out.Status, http.StatusOK))
	b, _ := json.Marshal(rows)
	_, _ = w.Write(b)
}

// errorJSON writes a structured error.
func errorJSON(w http.ResponseWriter, status int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  fmt.Sprintf(format, args...),
		"status": status,
	})
}

// statusFromString is used for Caddyfile parsing of HTTP status ints.
func statusFromString(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return n
}

// responseRecorder captures status + size of a wrapped handler.
type responseRecorder struct {
	http.ResponseWriter
	status   int
	bytes    int64
	recorded bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.recorded {
		return
	}
	r.status = status
	r.recorded = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.recorded {
		r.status = http.StatusOK
		r.recorded = true
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ctxKey for stashing path params in the request context.
type ctxKey int

const pathParamsKey ctxKey = 1

// paramsFromContext retrieves stashed path params (set during route matching).
func paramsFromContext(ctx context.Context) map[string]string {
	if v, ok := ctx.Value(pathParamsKey).(map[string]string); ok {
		return v
	}
	return nil
}

// nopReplacer is a no-op replacer when Caddy's isn't available yet.
type nopReplacer struct{}

func (nopReplacer) ReplaceAll(in string, _ any) string { return in }

var _ http.ResponseWriter = (*responseRecorder)(nil)
var _ = zap.NewNop // silence unused import during scaffolding
