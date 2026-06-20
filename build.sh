#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
TMPDIR="${TMPDIR:-$REPO_ROOT/.tmp}"
GOCACHE="${GOCACHE:-$REPO_ROOT/.gocache}"

mkdir -p "$TMPDIR" "$GOCACHE"

which xcaddy >/dev/null 2>&1 || go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest

# surrealdb.go is pure-Go (gorilla/websocket); no CGO, no special build tags.
export CGO_ENABLED=0
export TMPDIR
export GOCACHE

# Build against the module's pinned surrealdb.go version (see go.mod).
# Override for local dev: SURREALDB_GO_DIR=/path/to/surrealdb.go ./build.sh
SURREALDB_GO_DIR="${SURREALDB_GO_DIR:-}"

XCADDY_ARGS=(
    --with "github.com/mdrv/caddy-surrealdb=$REPO_ROOT"
    --output "$REPO_ROOT/caddy-surrealdb"
)
if [[ -n "$SURREALDB_GO_DIR" ]]; then
    XCADDY_ARGS+=(--with "github.com/surrealdb/surrealdb.go=$SURREALDB_GO_DIR")
fi

xcaddy build "${XCADDY_ARGS[@]}"

echo "Built: $REPO_ROOT/caddy-surrealdb ($(du -sh "$REPO_ROOT/caddy-surrealdb" | cut -f1))"
