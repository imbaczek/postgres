#!/bin/sh
# t/psql-smoke.sh -- acceptance test for the Lisp PostgreSQL server.
#
# Boots a server on a throwaway PGDATA, drives t/psql-smoke.sql through a
# real psql, and diffs the transcript against t/psql-smoke.out.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5461}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-smoke.XXXXXX")
LOG="$TMP/server.log"
OUT="$TMP/out"
GOLDEN="$BASE/t/psql-smoke.out"
SRV=
. "$BASE/t/test-support.sh"
test_debug_setup psql-smoke "$PORT" "$TMP" "$LOG"

cleanup() {
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

start_server() {
    test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data"

    i=0
    while [ "$i" -lt 150 ]; do
        if PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
                -c '' >/dev/null 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.2
    done

    echo "psql-smoke: server did not start; log follows" >&2
    cat "$LOG" >&2
    return 1
}

start_server

PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -v ON_ERROR_STOP=0 -A -t -F'|' < "$BASE/t/psql-smoke.sql" > "$OUT" 2>&1 || true

# Verify the unique-index metadata and backing-constraint ownership survive a
# clean restart against the same PGDATA.
kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
start_server
PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -v ON_ERROR_STOP=0 -A -t -F'|' < "$BASE/t/psql-smoke-persist.sql" >> "$OUT" 2>&1 || true

# EXPLAIN ANALYZE reports measured timings; normalize only those volatile
# numbers so the regression transcript remains deterministic.
sed -E \
    -e 's/actual time=[0-9]+\.[0-9]+\.\.[0-9]+\.[0-9]+ rows=/actual time=<timing> rows=/' \
    -e 's/Execution Time: [0-9]+\.[0-9]+ ms/Execution Time: <timing> ms/' \
    "$OUT" > "$TMP/normalized-out"
mv "$TMP/normalized-out" "$OUT"

if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
    cp "$OUT" "$GOLDEN"
    echo "psql-smoke: updated expected output"
elif diff -u "$GOLDEN" "$OUT"; then
    echo "psql-smoke: PASS"
else
    echo "psql-smoke: FAIL (above: expected vs actual)" >&2
    echo "psql-smoke: server log follows" >&2
    cat "$LOG" >&2
    exit 1
fi
