#!/bin/sh
# Verify SQL recovery from actual SBCL sort-arena capacity exhaustion.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5490}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 1))}
SBCL=${SBCL:-sbcl}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-sort-arena.XXXXXX")
PGDATA="$TMP/data"
LOG="$TMP/server.log"
SRV=
CLIENT=
. "$BASE/t/test-support.sh"
test_debug_setup sort-arena "$PORT" "$TMP" "$LOG"

cleanup() {
    [ -z "$CLIENT" ] || kill "$CLIENT" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$CLIENT" ] || wait "$CLIENT" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "sort-arena: FAIL $1" >&2
    [ ! -f "$LOG" ] || cat "$LOG" >&2
    [ ! -f "$TMP/transaction.out" ] || cat "$TMP/transaction.out" >&2
    exit 1
}

run() {
    PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
        -U postgres -d postgres -v ON_ERROR_STOP=1 -v VERBOSITY=verbose "$@"
}

control() {
    { cat; printf '%s\n' '(quit)'; } \
        | timeout 30 "$SBCL" --script "$BASE/t/control-client.lisp" \
            tcp "127.0.0.1:$CONTROL_PORT"
}

# The explicit option must override the environment's disabled setting.
PG_QUERY_SORT_ARENA_LIMIT_BYTES=-1 \
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
    --control-port "$CONTROL_PORT" --sort-arena-limit-bytes=65536 \
    > "$LOG" 2>&1 &
SRV=$!
i=0
until run -c '' >/dev/null 2>&1; do
    kill -0 "$SRV" 2>/dev/null || fail "server exited during startup"
    i=$((i + 1))
    [ "$i" -lt 150 ] || fail "server did not start"
    sleep 0.2
done

run -c 'create table arena_rows (id int, label text)' >/dev/null
awk 'BEGIN {
    printf "insert into arena_rows values "
    for (i = 1000; i >= 1; i--)
        printf "%s(%d, '\''row-%d'\'')", i == 1000 ? "" : ",", i, i
    print ";"
}' > "$TMP/populate.sql"
run < "$TMP/populate.sql" >/dev/null || fail "could not populate sort input"

if run -c 'select id, label from arena_rows order by id' > "$TMP/oom.out" 2>&1; then
    fail "64 KiB arena unexpectedly held the whole sort"
fi
grep -Fq '53200' "$TMP/oom.out" || fail "arena exhaustion did not return 53200"

# Remain on one wire connection across OOM, failed transaction, rollback,
# a subsequent successful sort, and a write after all arena storage is gone.
run -v ON_ERROR_STOP=0 > "$TMP/transaction.out" 2>&1 <<'SQL'
select pg_backend_pid();
begin;
insert into arena_rows values (2000, 'must roll back');
select id from arena_rows order by id;
select 123;
rollback;
select pg_backend_pid();
select count(*) from arena_rows;
select 7 order by 1;
insert into arena_rows values (1001, 'survived OOM');
select count(*) from arena_rows;
SQL
grep -Fq '53200' "$TMP/transaction.out" || fail "explicit transaction did not exhaust its arena"
grep -Fq '25P02' "$TMP/transaction.out" || fail "OOM did not abort the transaction"
numeric=$(sed -n '/^[0-9][0-9]*$/p' "$TMP/transaction.out")
expected=$(printf '%s\n' "$numeric" | sed -n '1p')
expected=$(printf '%s\n%s\n1000\n7\n1001' "$expected" "$expected")
[ "$numeric" = "$expected" ] || fail "connection, rollback, or subsequent queries did not recover"

# Small work_mem creates many runs; arena storage must be reset per run.
control <<'LISP' >/dev/null
(setf pg::*query-work-memory-bytes* 8192)
LISP
run -c 'select id from arena_rows order by id' > "$TMP/sort-one.out" 2>&1 &
CLIENT=$!
run -c 'select id from arena_rows order by id' > "$TMP/sort-two.out" 2>&1 \
    || fail "second concurrent sort failed"
wait "$CLIENT" || fail "first concurrent sort failed"
CLIENT=
for output in "$TMP/sort-one.out" "$TMP/sort-two.out"; do
    awk '($0 != NR) { exit 1 } END { if (NR != 1001) exit 1 }' "$output" \
        || fail "concurrent sort returned wrong rows"
done
spill_file=$(find "$PGDATA/pgsql_tmp" -type f -name 'pg-query-*.tmp' -print -quit)
[ -z "$spill_file" ] || fail "arena errors or successful merges left spill files"

echo "sort-arena: PASS (53200, transaction abort, same-connection recovery, concurrent external sorts)"
