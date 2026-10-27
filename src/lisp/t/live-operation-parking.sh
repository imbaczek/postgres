#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PSQL=${PSQL:-psql}
SBCL=${SBCL:-sbcl}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6660}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-operation-parking.XXXXXX")
SOURCE= TARGET=
cleanup() {
    status=$?
    trap - EXIT
    : >"$TMP/release"
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "live-operation-parking: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() { PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }
checked() {
    printf '%s\n(quit)\n' "$2" | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" "CREATE TABLE promotion_sql_items (id integer PRIMARY KEY,label text); INSERT INTO promotion_sql_items VALUES (1,'before')"
checked source "(physical-base-backup \"$TMP/copy/\" \"operation_parking\")" backup
"$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >"$TMP/target.log" 2>&1 &
TARGET=$!
test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"operation_parking\")" follow
SBCL="$SBCL" "$PATRONI_PYTHON" "$BASE/t/interop/live-operation-parking.py" "$BASE" "$TMP" "$PORT" "$TARGET_PORT" "$TARGET"
kill -INT "$TARGET"; wait "$TARGET"; TARGET=
kill -INT "$SOURCE"; wait "$SOURCE"; SOURCE=
echo 'live-operation-parking: inline continuation, promotion, deferred reload and retained RR PASS'
