#!/bin/sh
# Serve qualified WAL and refuse legacy inputs before repairing a stale PID.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PSQL=${PSQL:-psql}
PORT=${PORT:-6186}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-wal-namespace.XXXXXX")
SERVER=
cleanup() {
    if [ -n "$SERVER" ]; then kill -KILL "$SERVER" 2>/dev/null || :; wait "$SERVER" 2>/dev/null || :; fi
    echo "wal-namespace: artifacts=$TMP"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$1"
}
start() {
    test_launch_lisp_server "$PORT" "$TMP/source" "$TMP/c.sock" "$TMP/source.log"
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
}
stop() { kill -INT "$SERVER"; wait "$SERVER"; SERVER=; }
reject() {
    data=$1
    shift
    printf '1\nretired-namespace-fixture\n' >"$data/postmaster.pid"
    cp -a "$data" "$data.before"
    status=0
    timeout -k 2 20 "$BASE/pg-lisp.sh" "$@" </dev/null >"$data.log" 2>&1 || status=$?
    test "$status" -eq 1
    grep -Fq 'startup requires timeline-qualified WAL' "$data.log"
    diff -r --no-dereference "$data.before" "$data"
}
start
sql 'CREATE TABLE namespace_rows (id integer PRIMARY KEY); INSERT INTO namespace_rows VALUES (1)'
stop
if [ -n "${WAL_NAMESPACE_SEED:-}" ]; then
    cp -a "$WAL_NAMESPACE_SEED" "$TMP/legacy"
else
    cp -a "$TMP/source" "$TMP/legacy"
    python3 "$BASE/t/interop/wal-namespace-fixture.py" "$TMP/legacy"
fi
cp -a "$TMP/legacy" "$TMP/offline"
cp -a "$TMP/source" "$TMP/legacy-control"
cp "$TMP/legacy/global/pg_wal_control" "$TMP/legacy-control/global/pg_wal_control"
reject "$TMP/legacy" -p "$PORT" -D "$TMP/legacy"
reject "$TMP/offline" --single -D "$TMP/offline" template1
reject "$TMP/legacy-control" -p "$PORT" -D "$TMP/legacy-control"
cp -a "$TMP/source" "$TMP/alias"
ln -s missing-legacy-target "$TMP/alias/pg_wal/0000000000000063.wal"
reject "$TMP/alias" -p "$PORT" -D "$TMP/alias"
start
test "$(sql 'SELECT id FROM namespace_rows')" = 1
sql 'INSERT INTO namespace_rows VALUES (2)'
stop
start
test "$(sql 'SELECT id FROM namespace_rows ORDER BY id')" = "$(printf '1\n2')"
stop
echo 'wal-namespace: legacy refusal preserves storage and stale PID; qualified restart PASS'
