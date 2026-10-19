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

cleanup() {
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" > "$LOG" 2>&1 &
SRV=$!

# Wait for the server to accept connections.
i=0
while [ "$i" -lt 150 ]; do
    if PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
            -c '' >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done

if [ "$i" -ge 150 ]; then
    echo "psql-smoke: server did not start; log follows" >&2
    cat "$LOG" >&2
    exit 1
fi

PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -v ON_ERROR_STOP=0 -A -t -F'|' < "$BASE/t/psql-smoke.sql" > "$OUT" 2>&1 || true

if diff -u "$GOLDEN" "$OUT"; then
    echo "psql-smoke: PASS"
else
    echo "psql-smoke: FAIL (above: expected vs actual)" >&2
    echo "psql-smoke: server log follows" >&2
    cat "$LOG" >&2
    exit 1
fi
