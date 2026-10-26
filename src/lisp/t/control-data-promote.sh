#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6588}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-control-promote.XXXXXX")
SERVER= HELPER=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$HELPER" "$SERVER"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "control-data-promote: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ -n "${PROMOTION_SEED:-}" ]; then SEED=$PROMOTION_SEED
else
    PORT=$PORT "$BASE/t/physical-restartpoint-auto.sh" >"$TMP/seed.log" 2>&1
    SEED=$(sed -n 's/^physical-restartpoint-auto: artifacts=//p' "$TMP/seed.log")
fi
for point in after-child-control-rename after-retire-pending; do
    CASE=$TMP/$point
    mkdir -m 700 "$CASE"
    cp -a "$SEED/copy" "$CASE/data"
    rm -f "$CASE/data/physical-follow.control"
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" PG_PROMOTION_SOCKET="$CASE/control.sock" \
        PG_PROMOTION_PAUSE="$point" PG_PROMOTION_MARKER="$CASE/marker" PG_PROMOTION_RELEASE="$CASE/release" \
        "$SBCL" --script "$BASE/t/promotion-online-native.lisp" >"$CASE/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
    "$PATRONI_PYTHON" "$BASE/t/interop/patroni-promote.py" "$CASE/data" >"$CASE/helper.log" 2>&1 &
    HELPER=$!
    attempt=0
    while [ ! -e "$CASE/marker" ]; do
        kill -0 "$HELPER" 2>/dev/null || { cat "$CASE/helper.log" >&2; exit 1; }
        kill -0 "$SERVER"
        attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
    done
    read -r marker_pid <"$CASE/marker"
    test "$marker_pid" = "$SERVER"
    if [ "$point" = after-retire-pending ]; then
        python3 "$BASE/t/interop/promotion-primary-inspect.py" drop-retired "$CASE/data"
    fi
    python3 "$BASE/t/interop/control-data-inspect.py" "$CASE/data" 'in crash recovery'
    sleep 1.2
    kill -0 "$HELPER"
    if grep -q 'timeline PASS' "$CASE/helper.log"; then exit 1; fi
    : >"$CASE/release"
    wait "$HELPER"
    HELPER=
    grep -q 'timeline PASS' "$CASE/helper.log"
    python3 "$BASE/t/interop/control-data-inspect.py" "$CASE/data" 'in production'
    test "$(PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c 'SELECT count(*) FROM data')" = 5
    kill -INT "$SERVER"; wait "$SERVER"; SERVER=
    python3 "$BASE/t/interop/control-data-inspect.py" "$CASE/data" 'shut down'
    echo "control-data-promote: $point remains transitional until ready PASS"
done
