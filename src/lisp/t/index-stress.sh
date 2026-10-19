#!/bin/sh
# t/index-stress.sh -- repeated updates to one unique value.
#
# Updates the same indexed value many times (in one transaction), which
# leaves a long run of index entries with equal keys spread over several
# leaves.  After committing, a fresh transaction that tries to insert the
# same value must still detect the live conflicting tuple.  This is the
# regression test for duplicate keys straddling a B-tree leaf split.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5462}
UPDATES=${UPDATES:-1000}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-stress.XXXXXX")
LOG="$TMP/server.log"
SRV=
. "$BASE/t/test-support.sh"
test_debug_setup index-stress "$PORT" "$TMP" "$LOG"

cleanup() {
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data"

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
    echo "index-stress: server did not start; log follows" >&2
    cat "$LOG" >&2
    exit 1
fi

run() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -v ON_ERROR_STOP=0 "$@"
}

{
    echo "create table s (id int primary key, v int unique);"
    echo "insert into s values (1, 7);"
    echo "begin;"
    n=0
    while [ "$n" -lt "$UPDATES" ]; do
        echo "update s set v = 7 where id = 1;"
        n=$((n + 1))
    done
    echo "commit;"
} > "$TMP/stress.sql"

# -q keeps the transcript free of the per-statement command tags.
run -q -A -t < "$TMP/stress.sql" > "$TMP/stress.out" 2>&1 || true
if grep -qi 'error' "$TMP/stress.out"; then
    echo "index-stress: FAIL errors during repeated updates" >&2
    cat "$TMP/stress.out" >&2
    cat "$LOG" >&2
    exit 1
fi

# The live row must still be found among the stale same-key entries.
dup=$(run -q -A -t -c "insert into s values (2, 7);" 2>&1 || true)
case "$dup" in
    *duplicate*) : ;;
    *)
        echo "index-stress: FAIL expected duplicate-key error, got: $dup" >&2
        cat "$LOG" >&2
        exit 1
        ;;
esac

# A different value must still be accepted and visible.
run -q -c "insert into s values (2, 8);" >/dev/null 2>&1
rows=$(run -q -A -t -c "select id, v from s order by id;")
expected=$(printf '1|7\n2|8')
if [ "$rows" != "$expected" ]; then
    echo "index-stress: FAIL unexpected rows: $rows" >&2
    cat "$LOG" >&2
    exit 1
fi

echo "index-stress: PASS ($UPDATES repeated updates)"
