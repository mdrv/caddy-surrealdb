# caddy-surrealdb

A [Caddy](https://caddyserver.com/) v2 module that exposes [SurrealDB](https://surrealdb.com/)
as a first-class backend for HTTP requests. Declare named SurrealQL queries, map them to
HTTP routes, stream live-query changes over SSE or WebSocket, and log every request —
all from your Caddyfile.

## What it does

- **Schema-as-migration** — apply `DEFINE ... OVERWRITE` blocks on startup so your
  namespace, database, tables, fields, indexes, and access methods are reconciled every
  time Caddy provisions the module.
- **Named SurrealQL queries** — define parameterized queries once, then bind them to
  HTTP routes. Variables come from JSON body, query string, headers, URL path, env, or
  Caddy placeholders.
- **HTTP routes** — `route GET /users/:id get_user` style mappings with path wildcards,
  required headers (Caddy placeholders allowed), and per-IP rate limiting.
- **Live queries → SSE / WebSocket** — declare a `LIVE SELECT` once and stream record
  changes to browser clients. Reference-counted shared subscriptions, fan-out over SSE,
  15 s pings, JSON-normalized `RecordId`s.
- **Async request logging** — every request that passes through is written in batches
  to a SurrealDB table (`_caddy_requests` by default), with non-blocking buffer + drop
  or block overflow.
- **Robust connection lifecycle** — proactive JWT refresh (re-signs in before the 1-hour
  token TTL lapses), reactive reconnect after three consecutive heartbeat failures, and
  exponential backoff (1 s → 30 s). Supports root, namespace, database, record, and
  pre-issued token authentication.
- **Optional management API** — list/run queries, run raw SurrealQL (when enabled), and
  inspect runtime state under `/_surrealdb` by default.

## Build

```bash
./build.sh
# → ./caddy-surrealdb (custom Caddy binary with the module bundled)
```

Requires Go 1.26+ and [`xcaddy`](https://github.com/caddyserver/xcaddy). The build is
pure-Go (no CGO) because `surrealdb.go` only depends on `gorilla/websocket`. The
module pins `github.com/surrealdb/surrealdb.go v1.4.0` via `go.mod`; to test against
a local checkout, run `SURREALDB_GO_DIR=/path/to/surrealdb.go ./build.sh`.

## Test

```bash
go test -count=1 -race ./...
```

Unit tests cover pure functions only (no live SurrealDB required):
`handler_test.go` exercises path matching, IP extraction, parameter coercion,
JSON normalization, token-expiry parsing, and numeric bounds. `caddyfile_test.go`
round-trips every Caddyfile sub-block through `UnmarshalCaddyfile` and asserts
the resulting `Middleware` fields, including default values applied by
`applyDefaults`.

## Caddyfile

```
example.com {
	surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy

		# Pick one auth form:
		#   root <user> <pass>
		#   namespace <user> <pass>
		#   database <user> <pass>
		#   record <access-method> <bearer-or-credentials>
		#   token <pre-issued-JWT>
		auth namespace caddy caddy

		# Applied with OVERWRITE / IF NOT EXISTS on each provision.
		schema {
			DEFINE TABLE OVERWRITE person SCHEMAFULL;
			DEFINE FIELD OVERWRITE name ON person TYPE string;
			DEFINE FIELD OVERWRITE age  ON person TYPE option<int>;
			DEFINE INDEX OVERWRITE idx_person_name ON person COLUMNS name UNIQUE;
		}

		query get_person {
			sql `SELECT * FROM person WHERE id = type::thing('person', $id)`
			param id { from path, type string }
			output { format json; envelope on; status 200 }
		}

		query create_person {
			sql `CREATE type::thing('person', $id) SET name = $name, age = $age`
			param id   { from body, key id,   type string }
			param name { from body, key name, type string }
			param age  { from body, key age,  type int, default 0 }
			output { status 201 }
		}

		route  GET    /persons/:id get_person
		route  POST   /persons      create_person

		# Live query streamed to clients at /persons/live
		live_query  person_live { table person; diff off; format sse }
		live_route  GET /persons/live person_live

		log {
			table         _caddy_requests
			batch_size    500
			flush_interval 200ms
			buffer_size   8192
			overflow      drop
			exclude_path  /persons/live
		}

		heartbeat       15s
		heartbeat_table _caddy_status
		token_refresh   5m
		api_path        /_surrealdb
		raw_sql         off
		cors_origin     *
	}

	reverse_proxy upstream:8080
}
```

Order the directive in global options if you need it to run before other handlers:

```
{
	order surrealdb before reverse_proxy
}
```

## Architecture

```
                   HTTP request
                        │
                        ▼
┌──────────────────────────────────────────────────────────────┐
│ Middleware.ServeHTTP                                         │
│   1. live_route  ?──▶ liveHub ──▶ SurrealDB LIVE SELECT     │
│   2. route        ?──▶ QueryRegistry ──▶ surrealdb.Query     │
│   3. api_path     ?──▶ internal API (list / run / raw SQL)   │
│   4. otherwise    ──▶ next handler ──▶ BatchWriter (async)   │
└──────────────────────────────────────────────────────────────┘
       ▲                                  ▲
       │                                  │
┌──────┴───────┐                ┌─────────┴────────┐
│  Manager     │◀── heartbeat ──│  every N seconds │
│  (supervisor)│   token refresh│                  │
│  reconnect   │   on auth fail │                  │
└──────────────┘                └──────────────────┘
       │
       ▼
  SurrealDB  (ws:// or http://)
```

### Files

| File                | Role                                                                      |
| ------------------- | ------------------------------------------------------------------------- |
| `module.go`         | `Middleware` struct, `Provision` / `Cleanup`, defaults, validation.       |
| `caddyfile.go`      | Caddyfile directive parser and all sub-block parsers.                     |
| `connection.go`     | Connection `Manager`: dial, sign-in, token refresh, reconnect, heartbeat. |
| `query_registry.go` | Named-query DSL, parameter binding/coercion, route matching, rate limit.  |
| `live.go`           | Ref-counted live subscriptions, SSE fan-out, JSON normalization.          |
| `writer.go`         | Async batch writer for request logs.                                      |
| `handler.go`        | `ServeHTTP` routing, output formatting, response recorder.                |

## Auth levels

| Level     | Caddyfile form                  | SurrealDB scope             |
| --------- | ------------------------------- | --------------------------- |
| Root      | `auth root <u> <p>`             | Any NS / DB                 |
| Namespace | `auth namespace <u> <p>`        | The named NS only           |
| Database  | `auth database <u> <p>`         | The named NS + DB only      |
| Record    | `auth record <access> <bearer>` | `DEFINE ACCESS TYPE RECORD` |
| Token     | `auth token <jwt>`              | Pre-issued JWT              |

Live queries require a **WebSocket** endpoint (`ws://` or `wss://`). If you declare any
`live_query` block but use an `http://` endpoint, provisioning fails fast.

## Status

Early development. API surface is stable but field-level semantics may shift before v1.
SurrealDB 2.x and 3.x are both supported via the `surrealdb.go` driver.

## License

MIT — same as Caddy.
