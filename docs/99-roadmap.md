# Roadmap

This file tracks work that is **known to be incomplete**, intentionally deferred,
or blocked on an upstream dependency. Items are roughly ordered by impact.

## Known limitations

### 1. WebSocket live transport (`format ws`) is a stub

`live_route` accepts `format ws` in the Caddyfile, but `module.go` routes both
`LiveWS` and `LiveSSE` to `serveSSE` for now. To deliver real WebSocket frames
the module needs an HTTP-upgrade handler that:

- accepts the `Upgrade: websocket` request,
- spawns a reader for client→server control frames (subscribe, unsubscribe,
  ping),
- forwards each `connection.Notification` from the shared live subscription as
  a binary/text frame,
- closes the socket on client disconnect and decrements the hub refcount.

The liveHub plumbing is already transport-agnostic (`subscribe` returns a
`<-chan connection.Notification`), so the work is bounded to the HTTP
upgrade + write loop. Use `gorilla/websocket.Upgrader` (already an indirect
dependency via surrealdb.go).

Until implemented, declare live routes with `format sse` (the default).

### 2. Proactive token refresh path is not exercised end-to-end

The Manager performs a re-SignIn when the JWT `exp` claim drifts inside the
configured `token_refresh_threshold` (default 5m; SurrealDB tokens last 1h by
default). The implementation exists in `connection.go` (`supervise`,
`parseTokenExpiry`, `refreshAuthIfNeeded`) but the live test only ran for a
few minutes, so the refresh branch was never observed firing.

To verify, lower `token_refresh_threshold` and `heartbeat_interval` in a throwaway
Caddyfile and tail the logs for `surrealdb: token refreshed`. The heartbeat
UPSERT also surfaces the new `token_exp` in the `_caddy_status` row, which can
be cross-checked from `surreal sql`.

### 3. `unavailable ResponseChannel` log noise after live unsubscribe

After `surrealdb.Kill(ctx, conn, id)` returns, the SurrealDB server still
delivers one final `KILLED` notification for that live query id. Because
`Kill` already closed the local notification channel (see
`pkg/connection/gorillaws/connection.go:494` and `db.go:474`), the driver
logs an ERROR-level message:

```
unavailable ResponseChannel <uuid>
```

This is a **benign driver race** — the live query IS killed, there is no
resource leak, and SSE clients have already disconnected. It cannot be fixed
from this module; it requires an upstream change to `Kill` ordering (close
channel first, then RPC) or a "pending kill" marker in the notification
dispatcher. Tracked indirectly via the surrealdb.go driver.

### 4. `LiveQueryDef.Where` is declared but ignored

The struct field exists in `live.go:31` so that users can declare
`live_query foo { table person; where "age > 18" }`, but the value is not
passed to `surrealdb.Live()`. The Go driver only exposes
`Live(ctx, s, table, diff)` — there is no parameter for a WHERE clause.

Workaround until the driver exposes `LIVE SELECT ... WHERE $x`: post-filter
notifications in the per-subscriber fan-out goroutine, evaluating the WHERE
expression against `notification.Result`. A simple safe subset (field
comparisons, `AND`/`OR`) would cover most real-world filters.

### 5. Per-client live sessions isolation

All live subscriptions share the Manager's primary connection. If two
subscribers ask for the same `live_query`, the second one piggybacks on the
first (refcounted in `liveHub.subs`). That is the desired behaviour for a
fan-out, but it means all clients share the same SurrealDB auth context.

For per-client auth (e.g. record-level access methods, scoped row visibility),
the module should expose a `Session` per subscriber via `db.Attach()` (driver
requires a WebSocket endpoint). Each session can then `SignIn(Auth{NS, DB, AC,
...})` with credentials sourced from the request (cookie, Bearer token, etc.).
This is the same pattern used by the kotaete project's
`live-subscriptions.md`.

### 6. Record-level auth (`DEFINE ACCESS TYPE RECORD`) is unverified

The Manager supports `Auth.Level = "record"` with username/password, but the
sign-in flow for `Auth{Level: "record", Username, Password, Access}` was not
exercised live because no test target has a record access method defined.
SurrealDB v3 also introduces `SignInWithRefresh` returning
`{Access, Refresh}` — wiring that up (with refresh-token rotation) is the
right path for long-lived record sessions.

### 7. Integration tests still missing

`handler_test.go` and `caddyfile_test.go` cover the pure-function surface
(path matching, coercion, JSON normalization, Caddyfile parsing, defaults).
What is still missing is a live integration test using the `caddytest`
harness (the pattern used by `caddy-duckdb`):

- A `TestManager_ConnectAndHeartbeat` that provisions a Manager against the
  test target and verifies the heartbeat row appears.
- A `TestQueryRegistry_Execute` that registers a `SELECT 1+1 AS v` query and
  checks the result.
- A `TestLiveHub_Subscribe` that creates a table, opens an SSE-style
  subscriber, inserts a row, and asserts the notification arrives.

Run against the local target documented in `README.md` (`ws://127.0.0.1:596`,
NS `caddy`, DB `caddy`, NS user `caddy`/`caddy`). Skip via `testing.Short()`
or an env var (`SURREALDB_TEST_ENDPOINT`) when the target is unreachable.

### 8. SCHEMAFULL databases silently break the auto-created log table

The writer's lazy `DEFINE TABLE IF NOT EXISTS _caddy_requests SCHEMALESS`
is rejected by SurrealDB when the database itself is SCHEMAFULL, so inserts
silently fail with `Warn`-level logs and the `_caddy_requests` table never
appears. The user must define the table explicitly in the `schema` block:

```
schema {
    DEFINE TABLE OVERWRITE _caddy_requests SCHEMALESS PERMISSIONS FOR select, create WHERE true;
    DEFINE FIELD OVERWRITE ts         ON _caddy_requests TYPE datetime;
    DEFINE FIELD OVERWRITE ip         ON _caddy_requests TYPE string;
    DEFINE FIELD OVERWRITE method     ON _caddy_requests TYPE string;
    DEFINE FIELD OVERWRITE host       ON _caddy_requests TYPE string;
    DEFINE FIELD OVERWRITE path       ON _caddy_requests TYPE string;
    DEFINE FIELD OVERWRITE query      ON _caddy_requests TYPE option<string>;
    DEFINE FIELD OVERWRITE status     ON _caddy_requests TYPE int;
    DEFINE FIELD OVERWRITE latency_ms ON _caddy_requests TYPE int;
    DEFINE FIELD OVERWRITE bytes_sent ON _caddy_requests TYPE int;
    DEFINE FIELD OVERWRITE user_agent ON _caddy_requests TYPE option<string>;
}
```

Consider: detect a SCHEMAFULL database at Provision time and either (a) emit
a clearer log pointing at this snippet, or (b) auto-emit the typed schema
from the struct tags in `RequestRecord`.

## Future enhancements

- **`DEFINE ACCESS` provisioning**: First-class Caddyfile subdirective to
  declare SurrealDB record access methods alongside the schema.
- **Caddy admin API integration**: Surface `_caddy_status` heartbeats, hub
  refcounts, and writer queue depth as Caddy metrics or a JSON status page.
- **Per-route schema migration**: Allow each `query` block to declare
  `migration { schema_file ./path.surql; }` so refactors are co-located with
  the queries that depend on them.
- **CSV/NDJSON output for live streams**: Currently live notifications are
  always JSON; `format ndjson` would let downstream log processors consume
  the stream directly.
- **Backpressure strategy**: Add an `overflow block` mode to the liveHub (the
  writer already has this) and a configurable `max_subscribers_per_query`.
- **Multi-tenant endpoints**: Support multiple `surrealdb {}` blocks in the
  same Caddyfile, each with its own Manager. Today this works mechanically
  but `RegisterDirectiveOrder("surrealdb", Before, "file_server")` only
  declares one global directive.
