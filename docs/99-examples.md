# Examples

End-to-end recipes for `caddy-surrealdb`. Each recipe is self-contained: schema,
queries, routes, and the equivalent `curl` calls.

## Todo CRUD over GET (JSON)

A complete Create / Read / Update / Delete + List surface that uses **only GET**
requests — handy for server-rendered pages, server-side includes, no-JS clients,
or anywhere a POST/PUT/DELETE would be awkward.

All five operations return JSON. Mutations (`create` / `update` / `delete`) take
their arguments as query-string params, so the entire API is bookmarkable.

> Caddyfile syntax note: `{` must be the **last token on its line** — every
> block below opens on its own line. One-liners like `output { foo }` are
> rejected by the Caddyfile adapter.

### Schema

```surql
DEFINE TABLE OVERWRITE todo SCHEMAFULL;
DEFINE FIELD OVERWRITE title      ON todo TYPE string;
DEFINE FIELD OVERWRITE completed  ON todo TYPE bool DEFAULT false;
DEFINE FIELD OVERWRITE created_at ON todo TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_todo_created ON todo COLUMNS created_at;
```

### Caddyfile

```
example.com {
	surrealdb {
		endpoint ws://127.0.0.1:596
		namespace caddy
		database caddy
		auth namespace caddy caddy

		schema {
			DEFINE TABLE OVERWRITE todo SCHEMAFULL;
			DEFINE FIELD OVERWRITE title      ON todo TYPE string;
			DEFINE FIELD OVERWRITE completed  ON todo TYPE bool DEFAULT false;
			DEFINE FIELD OVERWRITE created_at ON todo TYPE datetime DEFAULT time::now();
			DEFINE INDEX OVERWRITE idx_todo_created ON todo COLUMNS created_at;
		}

		# --- List all todos (newest first) ---
		query list_todos {
			sql `SELECT id, title, completed, created_at FROM todo ORDER BY created_at DESC`
			output {
				format json
				envelope on
				status 200
			}
		}

		# --- Get a single todo by id ---
		query get_todo {
			sql `SELECT id, title, completed, created_at FROM type::record('todo', $id)`
			param id {
				from path
				type string
			}
			output {
				format json
				status 200
			}
		}

		# --- Create a new todo ---
		# $id may be omitted; here we let the caller pick a slug.
		query create_todo {
			sql `CREATE type::record('todo', $id) SET title = $title, completed = $completed`
			param id {
				from query
				key id
				type string
			}
			param title {
				from query
				key title
				type string
			}
			param completed {
				from query
				key completed
				type bool
				default false
			}
			output {
				format json
				status 201
			}
		}

		# --- Update an existing todo (full replace of title/completed) ---
		query update_todo {
			sql `UPDATE type::record('todo', $id) SET title = $title, completed = $completed RETURN AFTER`
			param id {
				from path
				type string
			}
			param title {
				from query
				key title
				type string
			}
			param completed {
				from query
				key completed
				type bool
				default false
			}
			output {
				format json
				status 200
			}
		}

		# --- Delete a todo, return the deleted record for client confirmation ---
		query delete_todo {
			sql `DELETE FROM type::record('todo', $id) RETURN BEFORE`
			param id {
				from path
				type string
			}
			output {
				format json
				status 200
			}
		}

		# Route order matters: literal segments must be declared before :params
		# at the same depth, otherwise `/todos/create` would match `:id=create`.
		route GET /todos                  list_todos
		route GET /todos/create           create_todo
		route GET /todos/:id              get_todo
		route GET /todos/:id/update       update_todo
		route GET /todos/:id/delete       delete_todo

		raw_sql off
	}
}
```

### Usage

```bash
# Create
curl -sG 'http://localhost:8181/todos/create' \
     --data-urlencode 'id=buy-milk' \
     --data-urlencode 'title=Buy milk' \
     --data-urlencode 'completed=false'
# [{"id":"todo:buy-milk","title":"Buy milk","completed":false,"created_at":"2026-06-20T..."}]

# Create another
curl -sG 'http://localhost:8181/todos/create' \
     --data-urlencode 'id=write-docs' \
     --data-urlencode 'title=Write examples'

# List
curl -s 'http://localhost:8181/todos'
# {"data":[{"id":"todo:write-docs",...},{"id":"todo:buy-milk",...}],"meta":{"count":2,...}}

# Read one
curl -s 'http://localhost:8181/todos/buy-milk'
# [{"id":"todo:buy-milk","title":"Buy milk","completed":false,"created_at":"2026-06-20T..."}]

# Update (mark complete)
curl -sG 'http://localhost:8181/todos/buy-milk/update' \
     --data-urlencode 'title=Buy oat milk' \
     --data-urlencode 'completed=true'
# [{"id":"todo:buy-milk","title":"Buy oat milk","completed":true,...}]

# Delete
curl -s 'http://localhost:8181/todos/buy-milk/delete'
# [{"id":"todo:buy-milk","title":"Buy oat milk","completed":true,...}]
```

### Notes

- **Path-order**: `/todos/create` is declared **before** `/todos/:id` because the
  router matches in declaration order. If `:id` came first, `/todos/create`
  would resolve with `id="create"`.
- **`type::record('todo', $id)`** is the SurrealDB v3 idiom for building a
  `todo:<id>` RecordID from a string. (`type::thing` still works as an alias.)
- **`RETURN AFTER` / `RETURN BEFORE`** make UPDATE / DELETE return the affected
  rows so the HTTP response is informative instead of empty.
- **Bool coercion**: `from query; type bool` accepts `true`/`false`/`1`/`0`/
  `yes`/`no` (case-insensitive). Other values return a 400 error.
- **Envelope**: `list_todos` wraps results as `{data, meta}`; the single-record
  routes return a bare JSON array for brevity. Set `envelope on` per-query if
  you want a consistent shape everywhere.
