#!/bin/sh
# t/update-concurrency.sh -- READ COMMITTED rechecks and REPEATABLE READ conflicts.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5471}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-update-race.XXXXXX")
LOG="$TMP/server.log"
SRV=
WRITER=
WAITER=
. "$BASE/t/test-support.sh"
test_debug_setup update-concurrency "$PORT" "$TMP" "$LOG"

cleanup() {
    [ -z "$WAITER" ] || kill "$WAITER" 2>/dev/null || true
    [ -z "$WRITER" ] || kill "$WRITER" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$WAITER" ] || wait "$WAITER" 2>/dev/null || true
    [ -z "$WRITER" ] || wait "$WRITER" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "update-concurrency: FAIL $1" >&2
    for output in "$TMP"/*-waiter.out "$TMP"/*-writer.out; do
        [ -f "$output" ] || continue
        echo "--- $output" >&2
        sed -n '1,80p' "$output" >&2
    done
    [ -f "$LOG" ] && cat "$LOG" >&2
    exit 1
}

run() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -A -t -v ON_ERROR_STOP=1 "$@"
}

scalar() {
    PGPASSWORD=x psql -X -q -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -A -t -v ON_ERROR_STOP=1 "$@"
}

populate_index_selectivity_fixture() {
    table=$1
    awk -v table="$table" 'BEGIN {
        printf "insert into %s values ", table
        for (id = 2; id <= 2001; id++)
            printf "%s(%d, %d)", id == 2 ? "" : ", ", id, id + 100
        print ";"
    }' > "$TMP/index-fixture.sql"
    run < "$TMP/index-fixture.sql" > "$TMP/index-fixture.out" 2>&1 \
        || fail "could not populate selective index fixture for $table"
}

wait_for_marker() {
    file=$1
    marker=$2
    i=0
    while [ "$i" -lt 50 ]; do
        if [ -f "$file" ] && grep -q "$marker" "$file"; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    fail "session did not reach $marker"
}

await_xid_wait() {
    control_output=$(printf '%s\n(quit)\n' \
        '(loop repeat 100 for waiting = (some (lambda (backend) (backend-waiting-xid backend)) (list-backends)) when waiting return :waiting do (sleep 0.05) finally (return :timeout))' \
        | timeout 20 "$SBCL" --script "$BASE/t/control-client.lisp" \
            tcp "127.0.0.1:$CONTROL_PORT" 2>&1) \
        || fail "could not inspect backend wait state"
    printf '%s\n' "$control_output" | grep -q ':WAITING' \
        || fail "waiter never entered a transaction-ID wait: $control_output"
}

# Both transactions increment the same row. The second UPDATE must follow the
# first transaction's ctid and reevaluate value + 1 against the committed row.
start_writer() {
    fifo=$1
    output=$2
    marker=$3
    statement=$4
    mkfifo "$fifo"
    run < "$fifo" > "$output" 2>&1 &
    WRITER=$!
    exec 3> "$fifo"
    printf 'begin;\n%s;\nselect %s;\n' "$statement" "$marker" >&3
    wait_for_marker "$output" "$marker"
}

finish_writer() {
    printf 'commit;\n' >&3
    exec 3>&-
    wait "$WRITER" || fail "writer transaction failed"
    WRITER=
}

start_repeatable_reader() {
    fifo=$1
    output=$2
    marker=$3
    query=$4
    mkfifo "$fifo"
    PGPASSWORD=x psql -X -q -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -A -t > "$output" 2>&1 < "$fifo" &
    WAITER=$!
    exec 4> "$fifo"
    printf '\\set VERBOSITY verbose\nbegin isolation level repeatable read;\n%s;\nselect %s;\n' \
        "$query" "$marker" >&4
    wait_for_marker "$output" "$marker"
}

finish_repeatable_reader() {
    printf 'rollback;\n\\q\n' >&4
    exec 4>&-
    wait "$WAITER" || fail "repeatable-read reader session failed"
    WAITER=
}

check_repeatable_read_conflict() {
    name=$1
    reader_select=$2
    writer_statement=$3
    reader_statement=$4
    expected_error=$5
    verification_query=$6
    expected_value=$7
    reader_marker=$8
    writer_marker=$9

    start_repeatable_reader "$TMP/$name-reader.sql" "$TMP/$name-reader.out" \
        "$reader_marker" "$reader_select"
    start_writer "$TMP/$name-writer.sql" "$TMP/$name-writer.out" \
        "$writer_marker" "$writer_statement"
    printf '%s;\n' "$reader_statement" >&4
    await_xid_wait
    finish_writer
    wait_for_marker "$TMP/$name-reader.out" "40001"
    grep -q "$expected_error" "$TMP/$name-reader.out" \
        || fail "$name returned the wrong serialization failure"
    finish_repeatable_reader
    actual=$(scalar -c "$verification_query")
    [ "$actual" = "$expected_value" ] \
        || fail "$name left unexpected row state: $actual"
}

test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" \
    --control-port "$CONTROL_PORT"
i=0
while [ "$i" -lt 150 ]; do
    if run -c '' >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done
[ "$i" -lt 150 ] || fail "server did not start"

run -c "create table update_increment (id int, value int)" >/dev/null
run -c "insert into update_increment values (1, 0)" >/dev/null
start_writer "$TMP/increment.sql" "$TMP/increment-writer.out" \
    74101 \
    "update update_increment set value = value + 1 where id = 1"
run -c "update update_increment set value = value + 1 where id = 1" \
    > "$TMP/increment-waiter.out" 2>&1 &
WAITER=$!
await_xid_wait
finish_writer
wait "$WAITER" || fail "increment waiter failed"
WAITER=
if ! grep -q '^UPDATE 1$' "$TMP/increment-waiter.out"; then
    cat "$TMP/increment-waiter.out" >&2
    fail "waiter did not update the latest row version"
fi
[ "$(scalar -c 'select value from update_increment where id = 1')" = 2 ] \
    || fail "concurrent increments did not both apply"

# The old row matches B's predicate when its statement starts. A commits a
# value that makes the predicate false; B must follow the chain and return 0.
run -c "create table update_predicate (id int, value int)" >/dev/null
run -c "insert into update_predicate values (1, 1)" >/dev/null
populate_index_selectivity_fixture update_predicate
run -c "create index update_predicate_value_idx on update_predicate (value)" >/dev/null
start_writer "$TMP/predicate.sql" "$TMP/predicate-writer.out" \
    74102 \
    "update update_predicate set value = 2 where id = 1"
run -c "update update_predicate set value = value + 1 where id = 1 and value = 1" \
    > "$TMP/predicate-waiter.out" 2>&1 &
WAITER=$!
await_xid_wait
finish_writer
wait "$WAITER" || fail "predicate waiter failed"
WAITER=
grep -q '^UPDATE 0$' "$TMP/predicate-waiter.out" \
    || fail "waiter failed to recheck the now-false WHERE clause"
[ "$(scalar -c 'select value from update_predicate where id = 1')" = 2 ] \
    || fail "predicate recheck changed a row that no longer matched"
scalar -c "analyze update_predicate" >/dev/null
plan=$(scalar -c 'explain select id from update_predicate where value = 2')
plan_contains_indented_node "$plan" "Index Scan using update_predicate_value_idx on update_predicate" \
    || fail "equality lookup after concurrent UPDATE did not use the index: $plan"
indexed_rows=$(scalar -c 'select id from update_predicate where value = 2')
[ "$indexed_rows" = 1 ] \
    || fail "IndexScan missed the version produced by concurrent UPDATE"
[ -z "$(scalar -c 'select id from update_predicate where value = 1')" ] \
    || fail "IndexScan exposed the old version after concurrent UPDATE"
run -c "drop index update_predicate_value_idx" >/dev/null
sequential_rows=$(scalar -c 'select id from update_predicate where value = 2')
[ "$indexed_rows" = "$sequential_rows" ] \
    || fail "indexed and sequential results differed after concurrent UPDATE"

# DELETE must use the same READ COMMITTED recheck after a conflicting UPDATE.
run -c "create table delete_predicate (id int, value int)" >/dev/null
run -c "insert into delete_predicate values (1, 1)" >/dev/null
populate_index_selectivity_fixture delete_predicate
run -c "create index delete_predicate_value_idx on delete_predicate (value)" >/dev/null
start_writer "$TMP/delete.sql" "$TMP/delete-writer.out" \
    74103 \
    "update delete_predicate set value = 2 where id = 1"
run -c "delete from delete_predicate where id = 1 and value = 1" \
    > "$TMP/delete-waiter.out" 2>&1 &
WAITER=$!
await_xid_wait
finish_writer
wait "$WAITER" || fail "delete predicate waiter failed"
WAITER=
grep -q '^DELETE 0$' "$TMP/delete-waiter.out" \
    || fail "DELETE did not recheck the now-false WHERE clause"
[ "$(scalar -c 'select value from delete_predicate where id = 1')" = 2 ] \
    || fail "DELETE removed a row that no longer matched"
scalar -c "analyze delete_predicate" >/dev/null
plan=$(scalar -c 'explain select id from delete_predicate where value = 2')
plan_contains_indented_node "$plan" "Index Scan using delete_predicate_value_idx on delete_predicate" \
    || fail "equality lookup after concurrent DELETE recheck did not use the index: $plan"
[ "$(scalar -c 'select id from delete_predicate where value = 2')" = 1 ] \
    || fail "IndexScan missed the surviving version after concurrent DELETE recheck"
run -c "delete from delete_predicate where value = 2" >/dev/null
run -c "vacuum delete_predicate" >/dev/null
indexed_rows=$(scalar -c 'select id from delete_predicate where value = 2')
run -c "drop index delete_predicate_value_idx" >/dev/null
sequential_rows=$(scalar -c 'select id from delete_predicate where value = 2')
[ "$indexed_rows" = "$sequential_rows" ] \
    || fail "indexed and sequential results differed after DELETE and VACUUM"

# A transaction-level snapshot that predates another writer's commit must fail
# when it tries to update the row version that writer replaced.
run -c "create table rr_update_conflict (id int, value int)" >/dev/null
run -c "insert into rr_update_conflict values (1, 1)" >/dev/null
check_repeatable_read_conflict rr-update \
    "select value from rr_update_conflict where id = 1" \
    "update rr_update_conflict set value = 2 where id = 1" \
    "update rr_update_conflict set value = 3 where id = 1" \
    "could not serialize access due to concurrent update" \
    "select value from rr_update_conflict where id = 1" 2 74104 74105

# DELETE uses the same heap update-chain result and must also abort on a
# post-snapshot UPDATE instead of following the new version.
run -c "create table rr_delete_conflict (id int, value int)" >/dev/null
run -c "insert into rr_delete_conflict values (1, 1)" >/dev/null
check_repeatable_read_conflict rr-delete-update \
    "select value from rr_delete_conflict where id = 1" \
    "update rr_delete_conflict set value = 2 where id = 1" \
    "delete from rr_delete_conflict where id = 1" \
    "could not serialize access due to concurrent update" \
    "select value from rr_delete_conflict where id = 1" 2 74106 74107

# A concurrent DELETE keeps ctid on the old tuple, but it still conflicts with
# an UPDATE based on the retained snapshot.
run -c "create table rr_delete_row_conflict (id int, value int)" >/dev/null
run -c "insert into rr_delete_row_conflict values (1, 1)" >/dev/null
check_repeatable_read_conflict rr-delete-row \
    "select value from rr_delete_row_conflict where id = 1" \
    "delete from rr_delete_row_conflict where id = 1" \
    "update rr_delete_row_conflict set value = 3 where id = 1" \
    "could not serialize access due to concurrent delete" \
    "select value from rr_delete_row_conflict where id = 1" "" 74108 74109

echo "update-concurrency: PASS"
