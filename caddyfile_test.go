package caddysurrealdb

import (
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

// dispense builds a caddyfile.Dispenser from raw source so we can exercise
// UnmarshalCaddyfile without spinning up a real Caddy runtime.
func dispense(t *testing.T, src string) (*Middleware, *caddyfile.Dispenser) {
	t.Helper()
	d := caddyfile.NewTestDispenser(src)
	m := new(Middleware)
	if err := m.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("UnmarshalCaddyfile: %v", err)
	}
	return m, d
}

func TestUnmarshal_Minimal(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth root caddy caddy
	}`
	m, _ := dispense(t, src)
	if m.Endpoint != "ws://127.0.0.1:596" {
		t.Errorf("endpoint: got %q", m.Endpoint)
	}
	if m.Namespace != "caddy" || m.Database != "caddy" {
		t.Errorf("ns/db: %q/%q", m.Namespace, m.Database)
	}
	if m.Auth.Level != "root" || m.Auth.Username != "caddy" || m.Auth.Password != "caddy" {
		t.Errorf("auth: %+v", m.Auth)
	}
}

func TestUnmarshal_NamespaceAuth(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth namespace caddy caddy
	}`
	m, _ := dispense(t, src)
	if m.Auth.Level != "namespace" {
		t.Errorf("level: got %q want namespace", m.Auth.Level)
	}
}

func TestUnmarshal_TokenAuth(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth token eyJhbGciOiJIUzI1NiJ9.payload.sig
	}`
	m, _ := dispense(t, src)
	if m.Auth.Level != "token" || m.Auth.Token != "eyJhbGciOiJIUzI1NiJ9.payload.sig" {
		t.Errorf("token auth: %+v", m.Auth)
	}
}

func TestUnmarshal_AuthBlock(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		auth {
			database caddy caddy
		}
	}`
	m, _ := dispense(t, src)
	if m.Auth.Level != "database" || m.Auth.Username != "caddy" {
		t.Errorf("block auth: %+v", m.Auth)
	}
}

func TestUnmarshal_Schema(t *testing.T) {
	src := "surrealdb {\n" +
		"	endpoint ws://127.0.0.1:596\n" +
		"	namespace caddy\n" +
		"	database caddy\n" +
		"	auth root caddy caddy\n" +
		"	schema {\n" +
		"		DEFINE TABLE OVERWRITE person SCHEMAFULL;\n" +
		"		DEFINE FIELD OVERWRITE name ON person TYPE string;\n" +
		"	}\n" +
		"}\n"
	m, _ := dispense(t, src)
	if len(m.Schema) != 2 {
		t.Fatalf("schema: got %d statements: %v", len(m.Schema), m.Schema)
	}
	if !strings.Contains(m.Schema[0], "DEFINE TABLE") {
		t.Errorf("schema[0]: %q", m.Schema[0])
	}
}

func TestUnmarshal_QueryWithParamAndOutput(t *testing.T) {
	src := "surrealdb {\n" +
		"	endpoint ws://127.0.0.1:596\n" +
		"	namespace caddy\n" +
		"	database caddy\n" +
		"	auth root caddy caddy\n" +
		"	query get_person {\n" +
		"		sql `SELECT * FROM type::record('person', $id)`\n" +
		"		param id {\n" +
		"			from path\n" +
		"			type string\n" +
		"			pattern ^[a-zA-Z0-9_-]+$\n" +
		"		}\n" +
		"		output {\n" +
		"			format json\n" +
		"			envelope off\n" +
		"			status 200\n" +
		"			omit meta\n" +
		"			alias id _id\n" +
		"		}\n" +
		"		cache 30s\n" +
		"		timeout 5s\n" +
		"	}\n" +
		"}\n"
	m, _ := dispense(t, src)
	if len(m.Queries) != 1 {
		t.Fatalf("queries: got %d", len(m.Queries))
	}
	q := m.Queries[0]
	if q.Name != "get_person" {
		t.Errorf("name: %q", q.Name)
	}
	if !strings.Contains(q.SQL, "type::record") {
		t.Errorf("sql: %q", q.SQL)
	}
	if len(q.Params) != 1 || q.Params[0].Name != "id" {
		t.Errorf("params: %+v", q.Params)
	}
	if q.Params[0].Source != "path" || q.Params[0].Type != "string" {
		t.Errorf("param detail: %+v", q.Params[0])
	}
	if q.Params[0].Pattern != "^[a-zA-Z0-9_-]+$" {
		t.Errorf("pattern: %q", q.Params[0].Pattern)
	}
	if q.Output.Format != "json" || q.Output.Envelope {
		t.Errorf("output: %+v", q.Output)
	}
	if q.Output.Status != 200 {
		t.Errorf("status: %d", q.Output.Status)
	}
	if !contains(q.Output.Omit, "meta") {
		t.Errorf("omit: %v", q.Output.Omit)
	}
	if q.Output.Aliases["id"] != "_id" {
		t.Errorf("aliases: %v", q.Output.Aliases)
	}
	if time.Duration(q.CacheTTL) != 30*time.Second {
		t.Errorf("cache ttl: %v", q.CacheTTL)
	}
	if q.Timeout != 5*time.Second {
		t.Errorf("timeout: %v", q.Timeout)
	}
}

func TestUnmarshal_QueryRouteWithRateLimit(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth root caddy caddy
		query create_person {
			sql ` + "`" + `CREATE person CONTENT $body` + "`" + `
		}
		route POST /api/person create_person {
			require_header Authorization "Bearer secret"
			rate_limit 100 per 1m
		}
	}`
	m, _ := dispense(t, src)
	if len(m.Routes) != 1 {
		t.Fatalf("routes: %d", len(m.Routes))
	}
	r := m.Routes[0]
	if r.Method != "POST" || r.Path != "/api/person" || r.QueryName != "create_person" {
		t.Errorf("route: %+v", r)
	}
	if v, ok := r.RequireHeaders["Authorization"]; !ok || v != "Bearer secret" {
		t.Errorf("require_header: %v", r.RequireHeaders)
	}
	if r.RateLimit == nil || r.RateLimit.Requests != 100 || r.RateLimit.Window != time.Minute {
		t.Errorf("rate_limit: %+v", r.RateLimit)
	}
}

func TestUnmarshal_LiveQueryAndRoute(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth root caddy caddy
		live_query people {
			table person
			diff off
			format sse
		}
		live_route GET /api/live/people people
	}`
	m, _ := dispense(t, src)
	if len(m.LiveQueries) != 1 {
		t.Fatalf("live queries: %d", len(m.LiveQueries))
	}
	lq := m.LiveQueries[0]
	if lq.Name != "people" || lq.Table != "person" || lq.Diff || lq.Format != "sse" {
		t.Errorf("live_query: %+v", lq)
	}
	if len(m.LiveRoutes) != 1 {
		t.Fatalf("live routes: %d", len(m.LiveRoutes))
	}
	lr := m.LiveRoutes[0]
	if lr.Method != "GET" || lr.Path != "/api/live/people" || lr.QueryName != "people" {
		t.Errorf("live_route: %+v", lr)
	}
}

func TestUnmarshal_LogBlock(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth root caddy caddy
		log {
			table _caddy_requests
			batch_size 200
			flush_interval 500ms
			buffer_size 4096
			overflow block
			exclude_path /health
			omit internal_id
		}
	}`
	m, _ := dispense(t, src)
	if m.LogTable != "_caddy_requests" {
		t.Errorf("log table: %q", m.LogTable)
	}
	if m.BatchSize != 200 {
		t.Errorf("batch_size: %d", m.BatchSize)
	}
	if m.FlushInterval != 500*time.Millisecond {
		t.Errorf("flush_interval: %v", m.FlushInterval)
	}
	if m.BufferSize != 4096 {
		t.Errorf("buffer_size: %d", m.BufferSize)
	}
	if m.Overflow != "block" {
		t.Errorf("overflow: %q", m.Overflow)
	}
	if !contains(m.LogExcludePaths, "/health") {
		t.Errorf("exclude_path: %v", m.LogExcludePaths)
	}
	if !contains(m.LogExcludeFields, "internal_id") {
		t.Errorf("omit: %v", m.LogExcludeFields)
	}
}

func TestUnmarshal_HeartbeatAndAPISettings(t *testing.T) {
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth root caddy caddy
		heartbeat 30s
		heartbeat_table _caddy_status
		token_refresh 3m
		api_path /_surreal
		api_token supersecret
		raw_sql on
		cors_origin *
	}`
	m, _ := dispense(t, src)
	if m.HeartbeatInterval != 30*time.Second {
		t.Errorf("heartbeat: %v", m.HeartbeatInterval)
	}
	if m.HeartbeatTable != "_caddy_status" {
		t.Errorf("heartbeat_table: %q", m.HeartbeatTable)
	}
	if m.TokenRefreshThreshold != 3*time.Minute {
		t.Errorf("token_refresh: %v", m.TokenRefreshThreshold)
	}
	if m.APIPath != "/_surreal" || m.APIToken != "supersecret" {
		t.Errorf("api: %q/%q", m.APIPath, m.APIToken)
	}
	if !m.RawSQL {
		t.Errorf("raw_sql: %v", m.RawSQL)
	}
	if m.CORSOrigin != "*" {
		t.Errorf("cors: %q", m.CORSOrigin)
	}
}

func TestUnmarshal_DefaultsAfterApply(t *testing.T) {
	// Apply defaults and confirm they match what module.go's applyDefaults sets.
	src := `surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth root caddy caddy
	}`
	m, _ := dispense(t, src)
	m.applyDefaults()
	if m.HeartbeatInterval != 30*time.Second {
		t.Errorf("default heartbeat: %v", m.HeartbeatInterval)
	}
	if m.TokenRefreshThreshold != 5*time.Minute {
		t.Errorf("default token_refresh: %v", m.TokenRefreshThreshold)
	}
	if m.BatchSize != 500 || m.FlushInterval != 200*time.Millisecond {
		t.Errorf("default batch/flush: %d/%v", m.BatchSize, m.FlushInterval)
	}
	if m.BufferSize != 8192 {
		t.Errorf("default buffer_size: %d", m.BufferSize)
	}
	if m.Overflow != "drop" {
		t.Errorf("default overflow: %q", m.Overflow)
	}
	if m.LogTable != "" {
		t.Errorf("default log_table should be empty (opt-in), got %q", m.LogTable)
	}
	if m.APIPath != "/_surreal" {
		t.Errorf("default api_path: %q", m.APIPath)
	}
	// HeartbeatTable default MUST be empty (opt-in, see m0191/m0200).
	if m.HeartbeatTable != "" {
		t.Errorf("default heartbeat_table should be empty (opt-in), got %q", m.HeartbeatTable)
	}
}

func TestUnmarshal_DirectiveMissingArg(t *testing.T) {
	src := `surrealdb {
		namespace
	}`
	d := caddyfile.NewTestDispenser(src)
	m := new(Middleware)
	if err := m.UnmarshalCaddyfile(d); err == nil {
		t.Error("expected error for namespace directive without arg")
	}
}

// contains is a tiny helper since we don't want to pull in slices for older Go.
func contains(slice []string, want string) bool {
	for _, s := range slice {
		if s == want {
			return true
		}
	}
	return false
}
