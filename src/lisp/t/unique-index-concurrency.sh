#!/bin/sh
# t/unique-index-concurrency.sh -- unique-index builds wait for table writers.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5467}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-unique-lock.XXXXXX")
LOG="$TMP/server.log"
SRV=
WRITER=
BUILDER=
DROPPER=
STALE_INSERT=
READER=
. "$BASE/t/test-support.sh"
test_debug_setup unique-index-concurrency "$PORT" "$TMP" "$LOG"

cleanup() {
    [ -z "$BUILDER" ] || kill "$BUILDER" 2>/dev/null || true
    [ -z "$WRITER" ] || kill "$WRITER" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$DROPPER" ] || kill "$DROPPER" 2>/dev/null || true
    [ -z "$STALE_INSERT" ] || kill "$STALE_INSERT" 2>/dev/null || true
    [ -z "$READER" ] || kill "$READER" 2>/dev/null || true
    [ -z "$BUILDER" ] || wait "$BUILDER" 2>/dev/null || true
    [ -z "$WRITER" ] || wait "$WRITER" 2>/dev/null || true
    [ -z "$DROPPER" ] || wait "$DROPPER" 2>/dev/null || true
    [ -z "$STALE_INSERT" ] || wait "$STALE_INSERT" 2>/dev/null || true
    [ -z "$READER" ] || wait "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
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
    echo "unique-index-concurrency: server did not start; log follows" >&2
    cat "$LOG" >&2
    exit 1
fi

run() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -q -A -t -v ON_ERROR_STOP=0 "$@"
}

assert_blocked() {
    pid=$1
    label=$2
    output=$3
    sleep 0.3
    state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
    case "$state" in
        ''|Z*|X*)
            echo "unique-index-concurrency: $label did not block" >&2
            cat "$output" >&2
            exit 1
            ;;
    esac
}

run -c "create table insert_race (i int)" >/dev/null
{
    printf "begin; insert into insert_race values (1); select 'writer-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/writer.out" ] && grep -q writer-ready "$TMP/writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: insert transaction did not reach ready state" >&2
    cat "$TMP/writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "create unique index insert_race_idx on insert_race (i)" \
    > "$TMP/insert-build.out" 2>&1 &
BUILDER=$!
assert_blocked "$BUILDER" "build on an in-progress insert" "$TMP/insert-build.out"
wait "$WRITER"
WRITER=
if ! wait "$BUILDER"; then
    echo "unique-index-concurrency: build failed after waiting for an in-progress insert" >&2
    cat "$TMP/insert-build.out" >&2
    cat "$LOG" >&2
    exit 1
fi
BUILDER=
dup=$(run -c "insert into insert_race values (1)" 2>&1 || true)
case "$dup" in
    *duplicate*) : ;;
    *)
        echo "unique-index-concurrency: built index missed an in-progress insert: $dup" >&2
        cat "$LOG" >&2
        exit 1
        ;;
esac

run -c "create table delete_race (id int, v int)" >/dev/null
run -c "insert into delete_race values (1, 7), (2, 7)" >/dev/null
{
    printf "begin; delete from delete_race where id = 2; select 'delete-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/delete-writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/delete-writer.out" ] && grep -q delete-ready "$TMP/delete-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: delete transaction did not reach ready state" >&2
    cat "$TMP/delete-writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "create unique index delete_race_idx on delete_race (v)" \
    > "$TMP/delete-build.out" 2>&1 &
BUILDER=$!
assert_blocked "$BUILDER" "build on an in-progress delete" "$TMP/delete-build.out"
wait "$WRITER"
WRITER=
if ! wait "$BUILDER"; then
    echo "unique-index-concurrency: build retained a row deleted by a committed writer" >&2
    cat "$TMP/delete-build.out" >&2
    cat "$LOG" >&2
    exit 1
fi
BUILDER=

dup=$(run -c "insert into delete_race values (3, 7)" 2>&1 || true)
case "$dup" in
    *duplicate*) : ;;
    *)
        echo "unique-index-concurrency: built index did not enforce uniqueness: $dup" >&2
        cat "$LOG" >&2
        exit 1
        ;;
esac

run -c "create table drop_build_race (i int)" >/dev/null
{
    printf "begin; insert into drop_build_race values (1); select 'drop-writer-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/drop-writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/drop-writer.out" ] && grep -q drop-writer-ready "$TMP/drop-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: drop/build writer did not reach ready state" >&2
    cat "$TMP/drop-writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "create unique index drop_build_race_idx on drop_build_race (i)" \
    > "$TMP/drop-build.out" 2>&1 &
BUILDER=$!
assert_blocked "$BUILDER" "build before DROP TABLE" "$TMP/drop-build.out"

run -c "drop table drop_build_race" > "$TMP/drop-table.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "DROP TABLE behind index build" "$TMP/drop-table.out"

wait "$WRITER"
WRITER=
if ! wait "$BUILDER"; then
    echo "unique-index-concurrency: index build failed during concurrent DROP TABLE" >&2
    cat "$TMP/drop-build.out" >&2
    cat "$LOG" >&2
    exit 1
fi
BUILDER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: DROP TABLE failed after index build" >&2
    cat "$TMP/drop-table.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=

run -c "create table drop_build_race (i int)" >/dev/null
run -c "create unique index drop_build_race_idx on drop_build_race (i)" >/dev/null
run -c "drop table drop_build_race" >/dev/null

run -c "create table drop_first_build (i int)" >/dev/null
{
    printf "begin; insert into drop_first_build values (1); select 'drop-first-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/drop-first-writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/drop-first-writer.out" ] && grep -q drop-first-ready "$TMP/drop-first-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: drop-first writer did not reach ready state" >&2
    cat "$TMP/drop-first-writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "drop table drop_first_build" > "$TMP/drop-first-table.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "DROP TABLE before index build" "$TMP/drop-first-table.out"

run -c "create unique index drop_first_build_idx on drop_first_build (i)" \
    > "$TMP/drop-first-index.out" 2>&1 &
BUILDER=$!
assert_blocked "$BUILDER" "index build behind DROP TABLE" "$TMP/drop-first-index.out"

wait "$WRITER"
WRITER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: DROP TABLE failed before index build" >&2
    cat "$TMP/drop-first-table.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=
wait "$BUILDER" || true
BUILDER=
if ! grep -q 'relation "drop_first_build" does not exist' "$TMP/drop-first-index.out"; then
    echo "unique-index-concurrency: index build used a table dropped while it waited" >&2
    cat "$TMP/drop-first-index.out" >&2
    cat "$LOG" >&2
    exit 1
fi
run -c "create table drop_first_build (i int)" >/dev/null
run -c "create unique index drop_first_build_idx on drop_first_build (i)" >/dev/null
run -c "drop table drop_first_build" >/dev/null

run -c "create table drop_dml_race (i int)" >/dev/null
{
    printf "begin; insert into drop_dml_race values (1); select 'dml-writer-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/dml-writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/dml-writer.out" ] && grep -q dml-writer-ready "$TMP/dml-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: stale-DML writer did not reach ready state" >&2
    cat "$TMP/dml-writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "drop table drop_dml_race" > "$TMP/dml-drop.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "DROP TABLE behind an active writer" "$TMP/dml-drop.out"

run -c "insert into drop_dml_race values (2)" > "$TMP/stale-insert.out" 2>&1 &
STALE_INSERT=$!
assert_blocked "$STALE_INSERT" "INSERT behind DROP TABLE" "$TMP/stale-insert.out"

wait "$WRITER"
WRITER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: DROP TABLE failed during stale-DML test" >&2
    cat "$TMP/dml-drop.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=
wait "$STALE_INSERT" || true
STALE_INSERT=
if ! grep -q 'relation "drop_dml_race" does not exist' "$TMP/stale-insert.out"; then
    echo "unique-index-concurrency: stale INSERT did not recheck the dropped relation" >&2
    cat "$TMP/stale-insert.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "create table drop_index_race (i int)" >/dev/null
run -c "create unique index drop_index_race_idx on drop_index_race (i)" >/dev/null
{
    printf "begin; insert into drop_index_race values (7); select 'drop-index-writer-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/drop-index-writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/drop-index-writer.out" ] && grep -q drop-index-writer-ready "$TMP/drop-index-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: DROP INDEX writer did not reach ready state" >&2
    cat "$TMP/drop-index-writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "drop index drop_index_race_idx" > "$TMP/drop-index.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "DROP INDEX behind an active writer" "$TMP/drop-index.out"
wait "$WRITER"
WRITER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: DROP INDEX failed after the writer committed" >&2
    cat "$TMP/drop-index.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=
if ! run -c "insert into drop_index_race values (7)" >/dev/null 2>&1; then
    echo "unique-index-concurrency: DROP INDEX left a stale uniqueness entry" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "create table multi_drop_a (i int)" >/dev/null
run -c "create table multi_drop_b (i int)" >/dev/null
run -c "insert into multi_drop_a values (1)" >/dev/null
run -c "insert into multi_drop_b values (1)" >/dev/null
{
    printf "begin; select i from multi_drop_b; select 'multi-drop-reader-ready';\n"
    sleep 1.5
    printf "select i from multi_drop_a; select 'multi-drop-reader-finished';\n"
    sleep 0.5
    printf 'commit;\n'
} | run > "$TMP/multi-drop-reader.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/multi-drop-reader.out" ] \
            && grep -q multi-drop-reader-ready "$TMP/multi-drop-reader.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: multi-table reader did not reach ready state" >&2
    cat "$TMP/multi-drop-reader.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "drop table multi_drop_a, multi_drop_b" \
    > "$TMP/multi-drop-table.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "multi-table DROP behind a reader" "$TMP/multi-drop-table.out"
i=0
while [ "$i" -lt 50 ]; do
    if grep -q multi-drop-reader-finished "$TMP/multi-drop-reader.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: reader deadlocked across multi-table DROP gates" >&2
    cat "$TMP/multi-drop-reader.out" >&2
    cat "$TMP/multi-drop-table.out" >&2
    cat "$LOG" >&2
    exit 1
fi
wait "$WRITER"
WRITER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: multi-table DROP failed after the reader committed" >&2
    cat "$TMP/multi-drop-table.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=
for table in multi_drop_a multi_drop_b; do
    missing=$(run -c "select i from $table" 2>&1 || true)
    case "$missing" in
        *"relation \"$table\" does not exist"*) : ;;
        *)
            echo "unique-index-concurrency: multi-table DROP left $table present: $missing" >&2
            cat "$TMP/multi-drop-table.out" >&2
            cat "$LOG" >&2
            exit 1
            ;;
    esac
done

run -c "create table multi_index_a (i int)" >/dev/null
run -c "create table multi_index_b (i int)" >/dev/null
run -c "create unique index multi_index_a_idx on multi_index_a (i)" >/dev/null
run -c "create unique index multi_index_b_idx on multi_index_b (i)" >/dev/null
{
    printf "begin; insert into multi_index_b values (1); select 'multi-index-writer-ready';\n"
    sleep 1.5
    printf "insert into multi_index_a values (1); select 'multi-index-writer-finished';\n"
    sleep 0.5
    printf 'commit;\n'
} | run > "$TMP/multi-index-writer.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/multi-index-writer.out" ] \
            && grep -q multi-index-writer-ready "$TMP/multi-index-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: multi-index writer did not reach ready state" >&2
    cat "$TMP/multi-index-writer.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "drop index multi_index_a_idx, multi_index_b_idx" \
    > "$TMP/multi-drop-index.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "multi-index DROP behind a writer" "$TMP/multi-drop-index.out"
i=0
while [ "$i" -lt 50 ]; do
    if grep -q multi-index-writer-finished "$TMP/multi-index-writer.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: writer deadlocked across multi-index DROP gates" >&2
    cat "$TMP/multi-index-writer.out" >&2
    cat "$TMP/multi-drop-index.out" >&2
    cat "$LOG" >&2
    exit 1
fi
wait "$WRITER"
WRITER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: multi-index DROP failed after the writer committed" >&2
    cat "$TMP/multi-drop-index.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=
for table in multi_index_a multi_index_b; do
    run -c "insert into $table values (2), (2)" >/dev/null 2>&1
    rows=$(run -c "select i from $table where i = 2" 2>&1 || true)
    expected=$(printf '2\n2')
    if [ "$rows" != "$expected" ]; then
        echo "unique-index-concurrency: multi-index DROP left an index on $table (rows=$rows)" >&2
        cat "$TMP/multi-drop-index.out" >&2
        cat "$LOG" >&2
        exit 1
    fi
done

run -c "create table drop_reader_race (i int)" >/dev/null
run -c "insert into drop_reader_race values (1)" >/dev/null
{
    printf "begin; select i from drop_reader_race; select 'reader-ready';\n"
    sleep 2
    printf 'commit;\n'
} | run > "$TMP/drop-reader.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/drop-reader.out" ] && grep -q reader-ready "$TMP/drop-reader.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: read transaction did not reach ready state" >&2
    cat "$TMP/drop-reader.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "drop table drop_reader_race" > "$TMP/drop-reader-table.out" 2>&1 &
DROPPER=$!
assert_blocked "$DROPPER" "DROP TABLE behind a read transaction" "$TMP/drop-reader-table.out"
wait "$WRITER"
WRITER=
if ! wait "$DROPPER"; then
    echo "unique-index-concurrency: DROP TABLE failed after the reader committed" >&2
    cat "$TMP/drop-reader-table.out" >&2
    cat "$LOG" >&2
    exit 1
fi
DROPPER=
missing=$(run -c "select i from drop_reader_race" 2>&1 || true)
case "$missing" in
    *'relation "drop_reader_race" does not exist'*) : ;;
    *)
        echo "unique-index-concurrency: relation remained visible after DROP TABLE: $missing" >&2
        cat "$LOG" >&2
        exit 1
        ;;
esac

run -c "create table build_reader_upgrade (i int)" >/dev/null
run -c "insert into build_reader_upgrade values (1)" >/dev/null
{
    printf "begin; insert into build_reader_upgrade values (2); select 'upgrade-blocker-ready';\n"
    sleep 4
    printf 'commit;\n'
} | run > "$TMP/upgrade-blocker.out" 2>&1 &
WRITER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/upgrade-blocker.out" ] && grep -q upgrade-blocker-ready "$TMP/upgrade-blocker.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: build blocker did not reach ready state" >&2
    cat "$TMP/upgrade-blocker.out" >&2
    cat "$LOG" >&2
    exit 1
fi

{
    printf "begin; select i from build_reader_upgrade; select 'upgrade-reader-ready';\n"
    sleep 1.5
    printf "insert into build_reader_upgrade values (1); select 'upgrade-insert-ready';\n"
    sleep 0.5
    printf 'commit;\n'
} | run > "$TMP/upgrade-reader.out" 2>&1 &
READER=$!

i=0
while [ "$i" -lt 50 ]; do
    if [ -f "$TMP/upgrade-reader.out" ] && grep -q upgrade-reader-ready "$TMP/upgrade-reader.out"; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
if [ "$i" -ge 50 ]; then
    echo "unique-index-concurrency: read transaction did not reach index-build race" >&2
    cat "$TMP/upgrade-reader.out" >&2
    cat "$LOG" >&2
    exit 1
fi

run -c "create unique index build_reader_upgrade_idx on build_reader_upgrade (i)" \
    > "$TMP/upgrade-build.out" 2>&1 &
BUILDER=$!
assert_blocked "$BUILDER" "unique build behind active writer" "$TMP/upgrade-build.out"
wait "$WRITER"
WRITER=
if ! wait "$BUILDER"; then
    echo "unique-index-concurrency: unique build failed during reader upgrade" >&2
    cat "$TMP/upgrade-build.out" >&2
    cat "$TMP/upgrade-reader.out" >&2
    cat "$LOG" >&2
    exit 1
fi
BUILDER=
wait "$READER"
READER=
if grep -q 'INSERT 0 1' "$TMP/upgrade-reader.out" \
        || ! grep -q 'duplicate key value' "$TMP/upgrade-reader.out"; then
    echo "unique-index-concurrency: reader upgraded to an unchecked writer during index build" >&2
    cat "$TMP/upgrade-reader.out" >&2
    cat "$TMP/upgrade-build.out" >&2
    cat "$LOG" >&2
    exit 1
fi
dup=$(run -c "insert into build_reader_upgrade values (1)" 2>&1 || true)
case "$dup" in
    *duplicate*) : ;;
    *)
        echo "unique-index-concurrency: build omitted a row exposed by reader upgrade: $dup" >&2
        cat "$LOG" >&2
        exit 1
        ;;
esac

echo "unique-index-concurrency: PASS"
