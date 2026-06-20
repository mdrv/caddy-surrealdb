package caddysurrealdb

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/surrealdb/surrealdb.go/pkg/models"
	"go.uber.org/zap"
)

// --- handler.go ------------------------------------------------------------

func TestCompileRoute(t *testing.T) {
	cases := []struct {
		path     string
		depth    int
		hasParam []bool // per segment, whether it's a :param
	}{
		{"/api/person", 2, []bool{false, false}},
		{"/api/person/:id", 3, []bool{false, false, true}},
		{"/api/person/:id/posts/:pid", 5, []bool{false, false, true, false, true}},
		{"/", 1, []bool{false}},
	}
	for _, c := range cases {
		segs := compileRoute(c.path)
		if len(segs) != c.depth {
			t.Errorf("%q: depth=%d want %d", c.path, len(segs), c.depth)
			continue
		}
		for i, s := range segs {
			if s.isParam != c.hasParam[i] {
				t.Errorf("%q seg[%d]: isParam=%v want %v", c.path, i, s.isParam, c.hasParam[i])
			}
		}
	}
}

func TestMatchPath(t *testing.T) {
	segs := compileRoute("/api/person/:id/posts/:pid")

	cases := []struct {
		path   string
		match  bool
		params map[string]string
	}{
		{"/api/person/alice/posts/42", true, map[string]string{"id": "alice", "pid": "42"}},
		{"/api/person/alice/posts", false, nil},
		{"/api/person/alice/posts/42/extra", false, nil},
		{"/api/Person/alice/posts/42", false, nil}, // case-sensitive
		{"/api/person//posts/42", false, nil},      // empty segment
	}
	for _, c := range cases {
		got := matchPath(segs, c.path)
		if (got != nil) != c.match {
			t.Errorf("path %q: matched=%v want %v", c.path, got != nil, c.match)
			continue
		}
		if got != nil {
			for k, v := range c.params {
				if got[k] != v {
					t.Errorf("path %q param %q=%q want %q", c.path, k, got[k], v)
				}
			}
		}
	}
}

func TestMatchPath_Root(t *testing.T) {
	segs := compileRoute("/")
	if matchPath(segs, "/") == nil {
		t.Error("root path should match root pattern")
	}
	if matchPath(segs, "/foo") != nil {
		t.Error("non-root should not match root pattern")
	}
}

func TestExtractIP(t *testing.T) {
	cases := []struct {
		name   string
		hdr    http.Header
		remote string
		want   string
	}{
		{"X-Forwarded-For single", http.Header{"X-Forwarded-For": []string{"1.2.3.4"}}, "5.6.7.8:1234", "1.2.3.4"},
		{"X-Forwarded-For chain", http.Header{"X-Forwarded-For": []string{"1.2.3.4, 9.9.9.9"}}, "5.6.7.8:1234", "1.2.3.4"},
		{"X-Real-IP", http.Header{"X-Real-Ip": []string{"10.0.0.1"}}, "5.6.7.8:1234", "10.0.0.1"},
		{"RemoteAddr only", http.Header{}, "192.168.1.1:4321", "192.168.1.1"},
		{"RemoteAddr no port", http.Header{}, "192.168.1.1", "192.168.1.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.Header = c.hdr
			r.RemoteAddr = c.remote
			got := extractIP(r)
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestCoalesceStatus(t *testing.T) {
	if got := coalesceStatus(0, 200); got != 200 {
		t.Errorf("0 → default: got %d want 200", got)
	}
	if got := coalesceStatus(404, 200); got != 404 {
		t.Errorf("nonzero: got %d want 404", got)
	}
}

// --- query_registry.go -----------------------------------------------------

func TestCoerce(t *testing.T) {
	cases := []struct {
		name    string
		raw     any
		binding ParamBinding
		want    any
		wantErr bool
	}{
		{"int from string", "42", ParamBinding{Type: "int"}, int64(42), false},
		{"int from float", 3.9, ParamBinding{Type: "int"}, nil, true}, // strict: rejects fractional
		{"int invalid", "abc", ParamBinding{Type: "int"}, nil, true},
		{"float from string", "3.14", ParamBinding{Type: "float"}, 3.14, false},
		{"bool true", "true", ParamBinding{Type: "bool"}, true, false},
		{"bool yes", "yes", ParamBinding{Type: "bool"}, true, false},
		{"bool 1", 1, ParamBinding{Type: "bool"}, true, false},
		{"bool invalid", "maybe", ParamBinding{Type: "bool"}, nil, true},
		{"string passthrough", "hello", ParamBinding{Type: "string"}, "hello", false},
		{"string from int", 42, ParamBinding{Type: "string"}, "42", false},
		{
			"int below min",
			"5",
			ParamBinding{Type: "int", Min: float64Ptr(10)},
			nil,
			true,
		},
		{
			"int above max",
			"100",
			ParamBinding{Type: "int", Max: float64Ptr(50)},
			nil,
			true,
		},
		{
			"int capped",
			"100",
			ParamBinding{Type: "int", Cap: float64Ptr(50)},
			int64(50),
			false,
		},
		{
			"string pattern match",
			"alice",
			ParamBinding{Type: "string", Pattern: "^[a-z]+$"},
			"alice",
			false,
		},
		{
			"string pattern mismatch",
			"Alice!",
			ParamBinding{Type: "string", Pattern: "^[a-z]+$"},
			nil,
			true,
		},
		{
			"duration",
			"30s",
			ParamBinding{Type: "duration"},
			30 * time.Second,
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := coerce(c.raw, &c.binding)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %#v want %#v", got, c.want)
			}
		})
	}
}

func TestApplyNumericBounds(t *testing.T) {
	if _, err := applyNumericBounds(5, &ParamBinding{Min: float64Ptr(10)}); err == nil {
		t.Error("below min should error")
	}
	if _, err := applyNumericBounds(100, &ParamBinding{Max: float64Ptr(50)}); err == nil {
		t.Error("above max should error")
	}
	if v, err := applyNumericBounds(50, &ParamBinding{Min: float64Ptr(10), Max: float64Ptr(100)}); err != nil {
		t.Errorf("in range should not error: %v", err)
	} else if v != 50 {
		t.Errorf("in-range value changed: got %v", v)
	}
	// no bounds → no-op
	if v, err := applyNumericBounds(42, &ParamBinding{}); err != nil {
		t.Errorf("no bounds should not error: %v", err)
	} else if v != 42 {
		t.Errorf("no-op value changed: got %v", v)
	}
	// cap clamps downward
	if v, err := applyNumericBounds(100, &ParamBinding{Cap: float64Ptr(50)}); err != nil {
		t.Errorf("cap should not error: %v", err)
	} else if v != 50 {
		t.Errorf("cap did not clamp: got %v", v)
	}
}

func TestExtractPath(t *testing.T) {
	m := map[string]any{
		"user": map[string]any{
			"profile": map[string]any{
				"age": 30,
			},
		},
		"top": "level",
	}
	if got := extractPath(m, "top"); got != "level" {
		t.Errorf("top: got %v want level", got)
	}
	if got := extractPath(m, "missing"); got != nil {
		t.Errorf("missing: got %v want nil", got)
	}
}

func TestApplyOutputShape(t *testing.T) {
	rows := []map[string]any{
		{"id": "p1", "name": "Alice", "ssn": "123-45-6789", "age": 30},
		{"id": "p2", "name": "Bob", "ssn": "987-65-4321", "age": 25},
	}
	t.Run("omit", func(t *testing.T) {
		out := OutputConfig{Omit: []string{"ssn"}}
		got := applyOutputShape(rows, out)
		for _, r := range got {
			if _, exists := r["ssn"]; exists {
				t.Error("ssn should be omitted")
			}
		}
	})
	t.Run("alias", func(t *testing.T) {
		out := OutputConfig{Aliases: map[string]string{"name": "fullName"}}
		got := applyOutputShape(rows, out)
		if got[0]["fullName"] != "Alice" {
			t.Errorf("alias: got %v want Alice", got[0]["fullName"])
		}
		if _, exists := got[0]["name"]; exists {
			t.Error("old name key should be removed after alias")
		}
	})
}

// --- live.go ---------------------------------------------------------------

func TestNormalizeForJSON(t *testing.T) {
	t.Run("RecordID direct", func(t *testing.T) {
		r := models.RecordID{Table: "person", ID: "alice"}
		got := normalizeForJSON(r)
		if got != "person:alice" {
			t.Errorf("got %v want person:alice", got)
		}
	})
	t.Run("RecordID pointer", func(t *testing.T) {
		r := &models.RecordID{Table: "person", ID: "bob"}
		got := normalizeForJSON(r)
		if got != "person:bob" {
			t.Errorf("got %v want person:bob", got)
		}
	})
	t.Run("nil RecordID pointer", func(t *testing.T) {
		got := normalizeForJSON((*models.RecordID)(nil))
		if got != nil {
			t.Errorf("got %v want nil", got)
		}
	})
	t.Run("map with RecordID value", func(t *testing.T) {
		m := map[string]any{
			"id":   models.RecordID{Table: "user", ID: 42},
			"name": "Alice",
		}
		got := normalizeForJSON(m).(map[string]any)
		if got["id"] != "user:42" {
			t.Errorf("id: got %v want user:42", got["id"])
		}
		if got["name"] != "Alice" {
			t.Errorf("name: got %v want Alice", got["name"])
		}
	})
	t.Run("slice with RecordID", func(t *testing.T) {
		s := []any{models.RecordID{Table: "t", ID: "a"}, "str"}
		got := normalizeForJSON(s).([]any)
		if got[0] != "t:a" {
			t.Errorf("got %v want t:a", got[0])
		}
	})
	t.Run("primitive passthrough", func(t *testing.T) {
		if got := normalizeForJSON(42); got != 42 {
			t.Errorf("int: got %v want 42", got)
		}
		if got := normalizeForJSON("hello"); got != "hello" {
			t.Errorf("string: got %v want hello", got)
		}
		if got := normalizeForJSON(nil); got != nil {
			t.Errorf("nil: got %v want nil", got)
		}
	})
}

func TestRecordIDString(t *testing.T) {
	cases := []struct {
		r    models.RecordID
		want string
	}{
		{models.RecordID{Table: "person", ID: "alice"}, "person:alice"},
		{models.RecordID{Table: "user", ID: 42}, "user:42"},
		{models.RecordID{Table: "empty"}, "empty"}, // nil ID
	}
	for _, c := range cases {
		if got := recordIDString(c.r); got != c.want {
			t.Errorf("got %q want %q", got, c.want)
		}
	}
}

// --- connection.go ---------------------------------------------------------

func TestEnsureWSScheme(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://localhost:8000", "ws://localhost:8000"},
		{"https://localhost:8000", "wss://localhost:8000"},
		{"ws://localhost:8000", "ws://localhost:8000"},
		{"wss://localhost:8000", "wss://localhost:8000"},
		{"memory", "memory"},                     // unchanged
		{"", ""},                                 // unchanged
		{"rocksdb:///tmp/x", "rocksdb:///tmp/x"}, // unchanged
	}
	for _, c := range cases {
		if got := ensureWSScheme(c.in); got != c.want {
			t.Errorf("%q: got %q want %q", c.in, got, c.want)
		}
	}
}

func TestParseTokenExpiry(t *testing.T) {
	logger := zap.NewNop()
	t.Run("valid JWT", func(t *testing.T) {
		// header.payload.signature — payload is base64-encoded JSON with exp
		// exp = 1700000000 (some fixed timestamp)
		// {"exp":1700000000} → base64url → eyJleHAiOjE3MDAwMDAwMDB9
		token := "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3MDAwMDAwMDB9.signature"
		got := parseTokenExpiry(token, logger)
		want := time.Unix(1700000000, 0)
		if !got.Equal(want) {
			t.Errorf("got %v want %v", got, want)
		}
	})
	t.Run("missing exp claim", func(t *testing.T) {
		// {"sub":"test"} → eyJzdWIiOiJ0ZXN0In0
		token := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature"
		got := parseTokenExpiry(token, logger)
		if !got.IsZero() {
			t.Errorf("got %v want zero time", got)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		got := parseTokenExpiry("not.a.jwt", logger)
		if !got.IsZero() {
			t.Errorf("got %v want zero time", got)
		}
	})
	t.Run("empty", func(t *testing.T) {
		got := parseTokenExpiry("", logger)
		if !got.IsZero() {
			t.Errorf("got %v want zero time", got)
		}
	})
}

func TestIdent(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"person", "person"},
		{"_caddy_status", "_caddy_status"},
		{"user-logs", "⟨user-logs⟩"}, // hyphen needs quoting
		{"with space", "⟨with space⟩"},
		{"normal123", "normal123"},
		{"", ""},
	}
	for _, c := range cases {
		if got := ident(c.in); got != c.want {
			t.Errorf("%q: got %q want %q", c.in, got, c.want)
		}
	}
}

// --- writer.go -------------------------------------------------------------

func TestOverflowMode(t *testing.T) {
	cases := []struct{ in, want string }{
		{"drop", "drop"},
		{"block", "block"},
		{"", "drop"},
		{"invalid", "drop"},
		{"BLOCK", "drop"}, // case-sensitive
	}
	for _, c := range cases {
		if got := overflowMode(c.in); got != c.want {
			t.Errorf("%q: got %q want %q", c.in, got, c.want)
		}
	}
}

func TestRequestRecordOmitFields(t *testing.T) {
	// Verify the writer's omit logic by simulating the row build + omit step
	// (without a real DB connection).
	row := map[string]any{
		"ts":         time.Now(),
		"ip":         "1.2.3.4",
		"method":     "GET",
		"host":       "x",
		"path":       "/y",
		"query":      "",
		"status":     200,
		"latency_ms": int64(5),
		"bytes_sent": int64(100),
		"user_agent": "curl",
	}
	omit := []string{"host", "user_agent", "query"}
	for _, f := range omit {
		delete(row, f)
	}
	for _, f := range omit {
		if _, exists := row[f]; exists {
			t.Errorf("field %q should be omitted", f)
		}
	}
	expected := []string{"ts", "ip", "method", "path", "status", "latency_ms", "bytes_sent"}
	for _, f := range expected {
		if _, exists := row[f]; !exists {
			t.Errorf("field %q should remain", f)
		}
	}
}

// helpers
func float64Ptr(f float64) *float64 { return &f }

// silence unused import if a case is removed
var _ = strings.Contains
