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

SURREALDB_GO_DIR="${SURREALDB_GO_DIR:-/x/g/_ext/surrealdb.go}"

xcaddy build \
    --with github.com/mdrv/caddy-surrealdb="$REPO_ROOT" \
    --with github.com/surrealdb/surrealdb.go="$SURREALDB_GO_DIR" \
    --output "$REPO_ROOT/caddy-surrealdb"

echo "Built: $REPO_ROOT/caddy-surrealdb ($(du -sh "$REPO_ROOT/caddy-surrealdb" | cut -f1))"
