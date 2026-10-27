#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PSQL=${PSQL:-psql}
SBCL=${SBCL:-sbcl}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6620}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-readers.XXXXXX")
SOURCE= TARGET=
cleanup() {
    status=$?
    trap - EXIT
    : >"$TMP/release"
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "promotion-readers: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() { PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }
checked() {
    printf '%s\n(quit)\n' "$2" | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" "CREATE TABLE promotion_sql_items (id integer PRIMARY KEY,label text); INSERT INTO promotion_sql_items VALUES (1,'before')"
checked source "(physical-base-backup \"$TMP/copy/\" \"promotion_readers\")" backup
start_target
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"promotion_readers\")" follow
SBCL="$SBCL" "$PATRONI_PYTHON" "$BASE/t/interop/promotion-readers.py" "$BASE" "$TMP" "$PORT" "$TARGET_PORT" "$TARGET"
if [ -z "${PROMOTION_CRASH:-}" ]; then kill -KILL "$TARGET"; fi
wait "$TARGET" 2>/dev/null || :; TARGET=
start_target
test "$(sql "$TARGET_PORT" 'SELECT pg_is_in_recovery()')" = f
if [ -n "${PROMOTION_ONLY_SNAPSHOT:-}" ]; then
    test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM promotion_sql_items')" = 5
elif [ -n "${PROMOTION_CRASH:-}" ]; then
    test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM promotion_sql_items')" = 3
    test "$(sql "$TARGET_PORT" 'SELECT label FROM promotion_sql_items WHERE id=1')" = after
else
    test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM promotion_sql_items')" = 7
fi
kill -INT "$TARGET"; wait "$TARGET"; TARGET=
kill -INT "$SOURCE"; wait "$SOURCE"; SOURCE=
if [ -n "${PROMOTION_CRASH:-}" ]; then
    echo "promotion-readers: retained readers at $PROMOTION_CRASH and child crash restart PASS"
else
    echo 'promotion-readers: retained transactions, new writer and crash restart PASS'
fi
