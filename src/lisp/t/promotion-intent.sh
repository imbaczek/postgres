#!/bin/sh
# Actual private intent publication; incomplete promotion never admits a writer.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-6446}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-intent.XXXXXX")
HELPER= SERVER=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$HELPER" "$SERVER"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "promotion-intent: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ -n "${PROMOTION_SEED:-}" ]; then
    SEED=$PROMOTION_SEED
else
    PORT=$PORT "$BASE/t/physical-restartpoint-auto.sh" >"$TMP/seed.log" 2>&1
    SEED=$(sed -n 's/^physical-restartpoint-auto: artifacts=//p' "$TMP/seed.log")
fi
test -d "$SEED/copy"
test -d "$SEED/source"
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$1"
}
for point in normal after-retained-intent-publication after-fsync after-rename after-directory-fsync after-retained-intent-control after-retained-intent-handoff after-write; do
    CASE=$TMP/$point
    mkdir -m 700 "$CASE"
    cp -a "$SEED/copy" "$CASE/data"
    before=$(sha256sum "$CASE/data/global/pg_wal_control")
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$TARGET_PORT" \
        PG_PROMOTION_PAUSE="$point" PG_PROMOTION_MARKER="$CASE/marker" \
        "$SBCL" --script "$BASE/t/promotion-intent-native.lisp" >"$CASE/publisher.log" 2>&1 &
    HELPER=$!
    if [ "$point" = normal ]; then
        wait "$HELPER"
        HELPER=
    else
        attempt=0
        while [ ! -f "$CASE/marker" ]; do
            kill -0 "$HELPER" 2>/dev/null || { cat "$CASE/publisher.log" >&2; exit 1; }
            attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
        done
        read -r marked_pid <"$CASE/marker"
        test "$marked_pid" = "$HELPER"
        kill -KILL "$HELPER"
        wait "$HELPER" 2>/dev/null || :
        HELPER=
    fi
    if [ "$point" = after-fsync ] || [ "$point" = after-retained-intent-publication ]; then
        "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$CASE/data" >"$CASE/startup.log" 2>&1 &
        SERVER=$!
        test_wait_owned_sql_server "$SERVER" "$TARGET_PORT" "$PSQL"
        test "$(sql 'SELECT pg_is_in_recovery()')" = t
        sql 'SELECT id FROM data ORDER BY id' >"$CASE/data.rows"
        diff -u "$SEED/source.rows" "$CASE/data.rows"
        kill -INT "$SERVER"
        wait "$SERVER"
        SERVER=
    else
        python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --snapshot-pending \
            "$CASE/data" "$SEED/source" "$CASE/before.json"
        # Valid pending startup completion is checked by promotion-commit.sh.
    fi
    test "$before" = "$(sha256sum "$CASE/data/global/pg_wal_control")"
done
echo 'promotion-intent: actual publication and process-crash boundaries PASS'
