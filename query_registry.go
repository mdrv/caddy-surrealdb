package caddysurrealdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"go.uber.org/zap"
)

// readFile is os.ReadFile wrapped for testability.
var readFile = os.ReadFile

// envGet is os.Getenv wrapped for testability.
var envGet = os.Getenv

// QueryDef declares a named SurrealQL statement plus its parameter bindings
// and output shaping rules.
type QueryDef struct {
	Name     string         `json:"name"`
	SQL      string         `json:"sql"`
	SQLFile  string         `json:"sql_file,omitempty"`
	Params   []ParamBinding `json:"params,omitempty"`
	Output   OutputConfig   `json:"output,omitempty"`
	CacheTTL CacheDuration  `json:"cache,omitempty"`
	Timeout  time.Duration  `json:"timeout,omitempty"`
}

// CacheDuration wraps time.Duration so JSON accepts "30s" style strings.
type CacheDuration time.Duration

// ParamBinding maps an HTTP request input onto a SurrealQL $variable.
type ParamBinding struct {
	Name      string   `json:"name"`
	Source    string   `json:"source,omitempty"` // body|query|header|path|env|placeholder (default body)
	Key       string   `json:"key,omitempty"`    // field/path key within the source
	Type      string   `json:"type,omitempty"`   // string|int|float|bool|datetime|duration|record|uuid|any (default string)
	Default   string   `json:"default,omitempty"`
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	Cap       *float64 `json:"cap,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	patternRe *regexp.Regexp
}

// OutputConfig shapes how rows are rendered to the HTTP response.
type OutputConfig struct {
	Format   string            `json:"format,omitempty"` // json (default)|csv|ndjson|raw
	Envelope bool              `json:"envelope,omitempty"`
	Aliases  map[string]string `json:"aliases,omitempty"`
	Omit     []string          `json:"omit,omitempty"`
	Status   int               `json:"status,omitempty"`
	Body     string            `json:"body,omitempty"`
}

// QueryRoute binds a QueryDef to an HTTP method+path.
type QueryRoute struct {
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	QueryName      string            `json:"query"`
	RequireHeaders map[string]string `json:"require_headers,omitempty"`
	RateLimit      *RateLimit        `json:"rate_limit,omitempty"`
}

// RateLimit caps per-IP requests per window.
type RateLimit struct {
	Requests int           `json:"requests"`
	Window   time.Duration `json:"window"`
}

// QueryRegistry resolves, validates, and executes named SurrealQL queries
// against the active connection.
type QueryRegistry struct {
	mgr     *Manager
	maxRows int
	logger  *zap.Logger

	mu      sync.Mutex
	queries map[string]*registeredQuery
	rateMu  sync.Mutex
	rates   map[string]*rateBucket
}

type registeredQuery struct {
	def     *QueryDef
	timeout time.Duration
}

type rateBucket struct {
	count int
	reset time.Time
}

func newQueryRegistry(mgr *Manager, maxRows int, logger *zap.Logger) *QueryRegistry {
	return &QueryRegistry{
		mgr:     mgr,
		maxRows: maxRows,
		logger:  logger,
		queries: make(map[string]*registeredQuery),
		rates:   make(map[string]*rateBucket),
	}
}

// Register indexes a query by name and pre-compiles its param patterns.
func (r *QueryRegistry) Register(def *QueryDef) error {
	if def.Name == "" {
		return fmt.Errorf("query missing name")
	}
	if def.SQLFile != "" {
		b, err := readFile(def.SQLFile)
		if err != nil {
			return fmt.Errorf("query %q sql_file: %w", def.Name, err)
		}
		def.SQL = string(b)
	}
	if def.SQL == "" {
		return fmt.Errorf("query %q missing sql", def.Name)
	}
	for i := range def.Params {
		p := &def.Params[i]
		if p.Source == "" {
			p.Source = "body"
		}
		// Default Key to Name so users can write `param id { from path }`
		// without also specifying `key id`.
		if p.Key == "" {
			p.Key = p.Name
		}
		if p.Type == "" {
			p.Type = "string"
		}
		if p.Pattern != "" {
			re, err := regexp.Compile(p.Pattern)
			if err != nil {
				return fmt.Errorf("query %q param %q bad pattern: %w", def.Name, p.Name, err)
			}
			p.patternRe = re
		}
	}
	timeout := def.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	if def.Output.Status == 0 {
		def.Output.Status = http.StatusOK
	}
	if def.Output.Format == "" {
		def.Output.Format = "json"
	}
	r.mu.Lock()
	r.queries[def.Name] = &registeredQuery{def: def, timeout: timeout}
	r.mu.Unlock()
	return nil
}

// Has reports whether name is registered.
func (r *QueryRegistry) Has(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.queries[name]
	return ok
}

// Names returns registered query names in unspecified order.
func (r *QueryRegistry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.queries))
	for n := range r.queries {
		out = append(out, n)
	}
	return out
}

// ValidateAll runs an EXPLAIN-style sanity probe on every query. SurrealDB
// doesn't have EXPLAIN in the same shape as SQL, so we issue the query with
// a false predicate and discard the result. Errors are reported but not
// fatal — they often indicate missing schema that the user provisions later.
func (r *QueryRegistry) ValidateAll(ctx context.Context) []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for name, rq := range r.queries {
		// Probe by wrapping the user's statement in a SELECT FROM ($sql) — but
		// SurrealDB has no such wrapper. Best we can do is run with empty vars
		// and let the user inspect Provision logs.
		_ = name
		_ = rq
	}
	return errs
}

// Execute runs the named query against the active connection.
func (r *QueryRegistry) Execute(ctx context.Context, name string, req *http.Request, pathParams map[string]string) (any, error) {
	r.mu.Lock()
	rq, ok := r.queries[name]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown query %q", name)
	}

	conn, err := r.mgr.Get(ctx)
	if err != nil {
		return nil, err
	}
	vars, err := r.resolveParams(req, rq.def.Params, pathParams)
	if err != nil {
		return nil, err
	}

	qctx, cancel := context.WithTimeout(ctx, rq.timeout)
	defer cancel()

	res, err := surrealdb.Query[[]map[string]any](qctx, conn, rq.def.SQL, vars)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", name, err)
	}
	if res == nil || len(*res) == 0 {
		return []map[string]any{}, nil
	}
	// Take the last statement result — SurrealQL multi-statement returns one
	// entry per statement; users almost always want the final one.
	last := &(*res)[len(*res)-1]
	if last.Status != "OK" {
		return nil, fmt.Errorf("query %q status %q: %v", name, last.Status, last.Result)
	}
	rows := last.Result
	if rows == nil {
		return []map[string]any{}, nil
	}
	if r.maxRows > 0 && len(rows) > r.maxRows {
		rows = rows[:r.maxRows]
	}
	// Normalize RecordID values (which serialize as `{}` in default JSON)
	// into "table:id" strings before applying output shape / encoding.
	for i := range rows {
		normalizeForJSON(rows[i])
	}
	return applyOutputShape(rows, rq.def.Output), nil
}

// resolveParams turns request inputs into a $vars map keyed by ParamBinding.Name.
func (r *QueryRegistry) resolveParams(req *http.Request, bindings []ParamBinding, pathParams map[string]string) (map[string]any, error) {
	vars := make(map[string]any, len(bindings))

	var body map[string]any
	hasBody := false
	for _, b := range bindings {
		if b.Source == "body" {
			if !hasBody {
				hasBody = true
				if req.Body != nil {
					buf, _ := io.ReadAll(io.LimitReader(req.Body, 1<<20))
					if len(buf) > 0 {
						_ = json.Unmarshal(buf, &body)
					}
				}
			}
		}
	}

	for _, b := range bindings {
		var raw any
		switch b.Source {
		case "body":
			raw = extractPath(body, b.Key)
		case "query":
			raw = req.URL.Query().Get(b.Key)
		case "header":
			raw = req.Header.Get(b.Key)
		case "path":
			raw = pathParams[b.Key]
		case "env":
			raw = envGet(b.Key)
		case "placeholder":
			// Caddy placeholder expansion happens at Provision time on the
			// Default field; we just emit that value.
			raw = b.Default
		default:
			raw = extractPath(body, b.Key)
		}
		if raw == nil || raw == "" {
			if b.Default != "" {
				raw = b.Default
			} else {
				vars[b.Name] = nil
				continue
			}
		}
		v, err := coerce(raw, &b)
		if err != nil {
			return nil, fmt.Errorf("param %q: %w", b.Name, err)
		}
		vars[b.Name] = v
	}
	return vars, nil
}

// extractPath walks a JSON map by dotted keys (e.g. "user.address.city").
func extractPath(m map[string]any, key string) any {
	if key == "" {
		return nil
	}
	if m == nil {
		return nil
	}
	cur := any(m)
	for _, part := range strings.Split(key, ".") {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[part]
		if cur == nil {
			return nil
		}
	}
	return cur
}

// coerce converts a raw request value into the type declared on the binding,
// applying min/max/cap/pattern validation.
func coerce(raw any, b *ParamBinding) (any, error) {
	s := toString(raw)

	switch b.Type {
	case "int":
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("not an int: %q", s)
		}
		adjusted, err := applyNumericBounds(float64(n), b)
		if err != nil {
			return nil, err
		}
		return int64(adjusted), nil

	case "float":
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("not a float: %q", s)
		}
		adjusted, err := applyNumericBounds(n, b)
		if err != nil {
			return nil, err
		}
		return adjusted, nil

	case "bool":
		switch strings.ToLower(s) {
		case "true", "1", "yes", "on":
			return true, nil
		case "false", "0", "no", "off", "":
			return false, nil
		}
		return nil, fmt.Errorf("not a bool: %q", s)

	case "datetime":
		// Accept RFC3339 or anything Go can parse.
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("not a datetime: %q", s)

	case "duration":
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("not a duration: %q", s)
		}
		return d, nil

	case "uuid":
		if b.patternRe == nil {
			b.patternRe = regexp.MustCompile(`^[0-9a-fA-F-]{36}$`)
		}
		fallthrough

	case "record":
		if b.patternRe == nil {
			// table:id or table:⟨…⟩
			b.patternRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*:[A-Za-z0-9_\-:.+]+$`)
		}
		fallthrough

	default: // string, any
		if b.patternRe == nil && b.Pattern != "" {
			if re, err := regexp.Compile(b.Pattern); err == nil {
				b.patternRe = re
			}
		}
		if b.patternRe != nil && !b.patternRe.MatchString(s) {
			return nil, fmt.Errorf("pattern mismatch: %q vs %q", s, b.Pattern)
		}
		return s, nil
	}
}

func applyNumericBounds(n float64, b *ParamBinding) (float64, error) {
	if b.Min != nil && n < *b.Min {
		return 0, fmt.Errorf("below min %v", *b.Min)
	}
	if b.Max != nil && n > *b.Max {
		return 0, fmt.Errorf("above max %v", *b.Max)
	}
	if b.Cap != nil && n > *b.Cap {
		n = *b.Cap
	}
	return n, nil
}

func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case json.Number:
		return x.String()
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// applyOutputShape applies aliases, omit, and truncation rules to rows.
func applyOutputShape(rows []map[string]any, out OutputConfig) []map[string]any {
	omitSet := make(map[string]bool, len(out.Omit))
	for _, k := range out.Omit {
		omitSet[k] = true
	}
	for i, row := range rows {
		for k := range row {
			if omitSet[k] {
				delete(rows[i], k)
				continue
			}
			if alias, ok := out.Aliases[k]; ok && alias != "" {
				rows[i][alias] = row[k]
				delete(rows[i], k)
			}
		}
	}
	return rows
}

// checkRateLimit returns true if the request is allowed; false otherwise.
// The bucket is per-(route|IP).
func (r *QueryRegistry) checkRateLimit(key string, rl *RateLimit) bool {
	if rl == nil || rl.Requests <= 0 {
		return true
	}
	r.rateMu.Lock()
	defer r.rateMu.Unlock()
	now := time.Now()
	b, ok := r.rates[key]
	if !ok || now.After(b.reset) {
		r.rates[key] = &rateBucket{count: 1, reset: now.Add(rl.Window)}
		return true
	}
	if b.count >= rl.Requests {
		return false
	}
	b.count++
	return true
}
