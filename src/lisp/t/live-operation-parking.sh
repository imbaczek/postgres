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
signal_owned() {
    "$PATRONI_PYTHON" "$BASE/t/interop/signal-owned-server.py" "$@"
}
cleanup() {
    status=$?
    trap - EXIT
    debug_interrupted=0
    trap 'status=130; debug_interrupted=1' HUP INT TERM
    : >"$TMP/release"
    if [ "$status" -ne 0 ] && [ "${PARKING_KEEP_ON_FAILURE:-0}" = 1 ]; then
        printf 'SOURCE=%s\nTARGET=%s\n' "$SOURCE" "$TARGET" >"$TMP/owned-servers"
        echo "live-operation-parking: retained owned servers for debugging; artifacts=$TMP"
        remaining=${PARKING_DEBUG_SECONDS:-600}
        while [ ! -f "$TMP/debug-release" ] && [ "$remaining" -gt 0 ] && [ "$debug_interrupted" -eq 0 ]; do
            sleep 1
            remaining=$((remaining - 1))
        done
    fi
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then
            if [ "$pid" = "$TARGET" ]; then data="$TMP/copy"; else data="$TMP/source"; fi
            if signal_owned "$pid" "$data" KILL 5; then
                wait "$pid" 2>/dev/null || :
            fi
        fi
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
if [ -n "${PARKING_SETUP_DRIVER:-}" ]; then
    timeout --foreground --kill-after=5 "${PARKING_DRIVER_TIMEOUT:-180}s" \
        "$PATRONI_PYTHON" "$PARKING_SETUP_DRIVER" --setup "$PORT"
fi
checked source "(physical-base-backup \"$TMP/copy/\" \"operation_parking\")" backup
"$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >"$TMP/target.log" 2>&1 &
TARGET=$!
test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"operation_parking\")" follow
SBCL="$SBCL" timeout --foreground --kill-after=5 "${PARKING_DRIVER_TIMEOUT:-180}s" \
    "$PATRONI_PYTHON" "${PARKING_DRIVER:-$BASE/t/interop/live-operation-parking.py}" \
    "$BASE" "$TMP" "$PORT" "$TARGET_PORT" "$TARGET"
signal_owned "$TARGET" "$TMP/copy" INT 20; wait "$TARGET"; TARGET=
signal_owned "$SOURCE" "$TMP/source" INT 20; wait "$SOURCE"; SOURCE=
echo 'live-operation-parking: driver assertions and owned peer cleanup PASS'
