#!/bin/sh
# Three-server logical replication regression: one source and two subscribers.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT_BASE=${PORT_BASE:-5610}
SOURCE_PORT=$PORT_BASE
REPLICA_ONE_PORT=$((PORT_BASE + 1))
REPLICA_TWO_PORT=$((PORT_BASE + 2))
REPLICA_DRIFT_PORT=$((PORT_BASE + 3))
SOURCE_DRIFT_PORT=$((PORT_BASE + 4))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical.XXXXXX")
SOURCE_PID=
SOURCE_DRIFT_PID=
REPLICA_ONE_PID=
REPLICA_TWO_PID=
REPLICA_DRIFT_PID=
WORKER_ONE_PID=
WORKER_TWO_PID=
SOURCE_DDL_PID=

sql() {
    SQL_PORT=$1
    SQL_TEXT=$2
    psql -X -h 127.0.0.1 -p "$SQL_PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' -c "$SQL_TEXT"
}

seed_copy_stress_table() {
    sql "$SOURCE_PORT" "CREATE TABLE copy_stress (id integer PRIMARY KEY, a varchar(126), b varchar(126), c varchar(126), d varchar(126)); INSERT INTO copy_stress VALUES (1, 'copy-1a', 'copy-1b', 'copy-1c', 'copy-1d'), (2, 'copy-2a', 'copy-2b', 'copy-2c', 'copy-2d'), (3, 'copy-3a', 'copy-3b', 'copy-3c', 'copy-3d'), (4, 'copy-4a', 'copy-4b', 'copy-4c', 'copy-4d'), (5, 'copy-5a', 'copy-5b', 'copy-5c', 'copy-5d'), (6, 'copy-6a', 'copy-6b', 'copy-6c', 'copy-6d'), (7, 'copy-7a', 'copy-7b', 'copy-7c', 'copy-7d'), (8, 'copy-8a', 'copy-8b', 'copy-8c', 'copy-8d'), (9, 'copy-9a', 'copy-9b', 'copy-9c', 'copy-9d'), (10, 'copy-10a', 'copy-10b', 'copy-10c', 'copy-10d'), (11, 'copy-11a', 'copy-11b', 'copy-11c', 'copy-11d'), (12, 'copy-12a', 'copy-12b', 'copy-12c', 'copy-12d')" >/dev/null
}

start_server() {
    SERVER_PORT=$1
    SERVER_DATA=$2
    SERVER_LOG=$3
    "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
        > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    i=0
    while [ "$i" -lt 200 ]; do
        if psql -X -h 127.0.0.1 -p "$SERVER_PORT" -U postgres -d postgres \
                -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "logical-replication: server exited during startup: $SERVER_LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: server did not start on port $SERVER_PORT" >&2
    return 1
}

wait_for_log() {
    LOG_FILE=$1
    LOG_TEXT=$2
    i=0
    while [ "$i" -lt 300 ]; do
        if grep -F "$LOG_TEXT" "$LOG_FILE" >/dev/null 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: timed out waiting for '$LOG_TEXT' in $LOG_FILE" >&2
    return 1
}

wait_for_file() {
    wait_for_file_timeout "$1" 300
}

wait_for_file_timeout() {
    FILE_PATH=$1
    WAIT_ITERATIONS=$2
    i=0
    while [ "$i" -lt "$WAIT_ITERATIONS" ]; do
        if [ -f "$FILE_PATH" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: timed out waiting for $FILE_PATH" >&2
    return 1
}

wait_for_state() {
    SQL_PORT=$1
    SLOT_NAME=$2
    STATE_NAME=$3
    i=0
    while [ "$i" -lt 300 ]; do
        state=$(sql "$SQL_PORT" \
            "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME'")
        if [ "$state" = "$STATE_NAME" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: slot $SLOT_NAME did not reach state $STATE_NAME" >&2
    return 1
}

replicated_rows() {
    sql "$1" "SELECT id, amount, label FROM replicated_rows ORDER BY id"
}

nullable_rows() {
    sql "$1" "SELECT nullable_key, label, j, b FROM nullable_rows ORDER BY nullable_key"
}

sequence_state() {
    sql "$1" "SELECT pg_lisp_sequence_state('$2')"
}

sequence_definition_state() {
    sequence_state "$1" "$2" \
        | sed -n 's/.*:start \([-0-9][0-9]*\) :increment \([-0-9][0-9]*\) :min \([-0-9][0-9]*\) :max \([-0-9][0-9]*\).*/\1|\2|\3|\4/p'
}

sequence_state_position() {
    sequence_state "$1" "$2" \
        | sed -n 's/.*:last \([-0-9][0-9]*\).*:called \([^)]*\).*/\1|\2/p'
}

wait_for_rows() {
    SQL_PORT=$1
    EXPECTED_ROWS=$2
    i=0
    while [ "$i" -lt 300 ]; do
        actual=$(replicated_rows "$SQL_PORT")
        if [ "$actual" = "$EXPECTED_ROWS" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: rows on port $SQL_PORT did not reach expected state" >&2
    echo "expected: $EXPECTED_ROWS" >&2
    echo "actual:   $actual" >&2
    return 1
}

wait_for_nullable_rows() {
    SQL_PORT=$1
    EXPECTED_ROWS=$2
    i=0
    while [ "$i" -lt 300 ]; do
        actual=$(nullable_rows "$SQL_PORT")
        if [ "$actual" = "$EXPECTED_ROWS" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: nullable rows on port $SQL_PORT did not reach expected state" >&2
    echo "expected: $EXPECTED_ROWS" >&2
    echo "actual:   $actual" >&2
    return 1
}

wait_for_progress_change() {
    SQL_PORT=$1
    SLOT_NAME=$2
    BEFORE_LSN=$3
    i=0
    while [ "$i" -lt 300 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME' AND state = 'ready'")
        if [ -n "$current" ] && [ "$current" != "$BEFORE_LSN" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: slot $SLOT_NAME made no progress past $BEFORE_LSN" >&2
    return 1
}

normalize_lsn() {
    LSN_HIGH=${1%/*}
    LSN_LOW=${1#*/}
    LSN_HIGH=$(printf '%8s' "$LSN_HIGH" | tr ' ' '0')
    LSN_LOW=$(printf '%8s' "$LSN_LOW" | tr ' ' '0')
    printf '%s/%s' "$LSN_HIGH" "$LSN_LOW"
}

wait_for_lsn_barrier() {
    SQL_PORT=$1
    SLOT_NAME=$2
    BARRIER=$(normalize_lsn "$3")
    i=0
    while [ "$i" -lt 1800 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME' AND state = 'ready'")
        if [ -n "$current" ]; then
            current=$(normalize_lsn "$current")
            first_lsn=$(printf '%s\n%s\n' "$BARRIER" "$current" \
                | LC_ALL=C sort | sed -n '1p')
            if [ "$first_lsn" = "$BARRIER" ]; then
                return 0
            fi
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: slot $SLOT_NAME did not reach source LSN $BARRIER within 180 seconds" >&2
    return 1
}

wait_for_slot_confirmation() {
    SQL_PORT=$1
    SLOT_NAME=$2
    BARRIER=$(normalize_lsn "$3")
    i=0
    while [ "$i" -lt 1800 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'")
        if [ -n "$current" ]; then
            current=$(normalize_lsn "$current")
            first_lsn=$(printf '%s\n%s\n' "$BARRIER" "$current" \
                | LC_ALL=C sort | sed -n '1p')
            if [ "$first_lsn" = "$BARRIER" ]; then
                return 0
            fi
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication: slot $SLOT_NAME did not confirm source LSN $BARRIER within 180 seconds" >&2
    return 1
}

cleanup() {
    for pid in "$WORKER_ONE_PID" "$WORKER_TWO_PID" "$SOURCE_DDL_PID" \
               "$SOURCE_PID" "$SOURCE_DRIFT_PID" \
               "$REPLICA_ONE_PID" "$REPLICA_TWO_PID" \
               "$REPLICA_DRIFT_PID"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$WORKER_ONE_PID" "$WORKER_TWO_PID" "$SOURCE_DDL_PID" \
               "$SOURCE_PID" "$SOURCE_DRIFT_PID" \
               "$REPLICA_ONE_PID" "$REPLICA_TWO_PID" \
               "$REPLICA_DRIFT_PID"; do
        if [ -n "$pid" ]; then
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$TMP"
}

on_exit() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        echo "logical-replication: failure; server and worker logs follow" >&2
          for log in "$TMP/source.log" "$TMP/source-drift.log" \
                     "$TMP/replica-one.log" \
                   "$TMP/replica-two.log" "$TMP/worker-one.log" \
                   "$TMP/replica-drift.log" "$TMP/worker-initial-drift.log" \
                   "$TMP/worker-two.log" "$TMP/worker-two-feedback-fault.log" \
                   "$TMP/worker-two-restart.log" "$TMP/worker-one-row-fault.log" \
                   "$TMP/worker-one-row-restart.log" \
                   "$TMP/worker-one-drift-rejected.log" \
                   "$TMP/worker-one-schema-drift.log"; do
            if [ -f "$log" ]; then
                echo "--- $log" >&2
                cat "$log" >&2
            fi
        done
    fi
    cleanup
    exit "$status"
}

trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source.log"
SOURCE_PID=$SERVER_PID
start_server "$REPLICA_ONE_PORT" "$TMP/replica-one-data" "$TMP/replica-one.log"
REPLICA_ONE_PID=$SERVER_PID
start_server "$REPLICA_TWO_PORT" "$TMP/replica-two-data" "$TMP/replica-two.log"
REPLICA_TWO_PID=$SERVER_PID

seed_copy_stress_table
sql "$SOURCE_PORT" "SET DateStyle TO 'ISO, DMY'; SET TimeZone TO 'Europe/Warsaw'; CREATE TABLE bootstrap_temporal (id integer PRIMARY KEY, d date DEFAULT '01/02/2024' CHECK(d >= '01/02/2024') CHECK(d = ANY('{01/02/2024,04/03/2024}')), z timestamptz DEFAULT '01/02/2024 12:00:00', created timestamp DEFAULT 'now')" >/dev/null
sql "$SOURCE_PORT" "CREATE TABLE replicated_rows (id integer PRIMARY KEY, amount bigint NOT NULL, label text); CREATE TABLE bootstrap_constraint_order (a integer DEFAULT '1.5'::numeric CHECK(a > '0.25'::numeric), b integer, c integer, CONSTRAINT bootstrap_constraint_order_pkey PRIMARY KEY (a), CONSTRAINT bootstrap_constraint_order_b_key UNIQUE (b), CONSTRAINT bootstrap_constraint_order_c_key UNIQUE (c)); CREATE TABLE nullable_rows (nullable_key integer, label bytea, j json, b jsonb); CREATE UNIQUE INDEX nullable_rows_key_idx ON nullable_rows (nullable_key) NULLS NOT DISTINCT; CREATE TABLE ignored_rows (id integer PRIMARY KEY, label varchar(64)); CREATE SEQUENCE bootstrap_counter START WITH 10 INCREMENT BY 2; ALTER SEQUENCE bootstrap_counter INCREMENT BY -2; SELECT setval('bootstrap_counter', 100, true); INSERT INTO replicated_rows VALUES (1, 10, 'initial'), (9, 90, 'remove'); INSERT INTO nullable_rows VALUES (NULL, 'initial-null'), (2, 'two'); INSERT INTO ignored_rows VALUES (1, 'before')"

long_text=$(awk 'BEGIN { for (i=0;i<3000;i++) printf "x" }')
sql "$SOURCE_PORT" "UPDATE replicated_rows SET label = '$long_text' WHERE id = 1" >/dev/null
long_bytes=$(awk 'BEGIN { for (i=0;i<1500;i++) printf "00ff" }')
sql "$SOURCE_PORT" "UPDATE nullable_rows SET label = '\\x$long_bytes' WHERE nullable_key = 2" >/dev/null
sql "$SOURCE_PORT" "UPDATE nullable_rows SET j = '{\"k\":\"$long_text\", \"k\":2}' WHERE nullable_key = 2" >/dev/null
sql "$SOURCE_PORT" "UPDATE nullable_rows SET b = '{\"v\":\"$long_text\",\"v\":\"$long_text\"}' WHERE nullable_key = 2" >/dev/null

# Isolate the failed initial-copy slot on a disposable publisher. Its exported
# snapshot intentionally remains available for retry, so it must not hold the
# schema lease used by the main publisher's later DDL regression.
start_server "$SOURCE_DRIFT_PORT" "$TMP/source-drift-data" "$TMP/source-drift.log"
SOURCE_DRIFT_PID=$SERVER_PID
sql "$SOURCE_PORT" "ALTER TABLE bootstrap_constraint_order ADD CONSTRAINT bootstrap_later CHECK(b IS NULL OR b>0) NOT VALID" >/dev/null

sql "$SOURCE_DRIFT_PORT" "CREATE TABLE replicated_rows (id integer PRIMARY KEY, amount bigint NOT NULL, label text); CREATE SEQUENCE bootstrap_counter START WITH 10 INCREMENT BY 2; SELECT setval('bootstrap_counter', 100, true)"

# A fresh subscriber with a pre-existing incompatible table and sequence must
# fail before initial-copy reset or sequence-state bootstrap changes either one.
start_server "$REPLICA_DRIFT_PORT" "$TMP/replica-drift-data" "$TMP/replica-drift.log"
REPLICA_DRIFT_PID=$SERVER_PID
sql "$REPLICA_DRIFT_PORT" "CREATE TABLE replicated_rows (id varchar(12) PRIMARY KEY, amount integer NOT NULL, label varchar(64)); INSERT INTO replicated_rows VALUES ('keep', 42, 'sentinel'); CREATE SEQUENCE bootstrap_counter START WITH 10 INCREMENT BY 1; SELECT setval('bootstrap_counter', 77, true)"
drift_rows_before=$(sql "$REPLICA_DRIFT_PORT" \
    "SELECT id, amount, label FROM replicated_rows")
drift_sequence_before=$(sequence_state_position "$REPLICA_DRIFT_PORT" bootstrap_counter)
drift_sequence_definition_before=$(sequence_definition_state \
    "$REPLICA_DRIFT_PORT" bootstrap_counter)
if "$BASE/pg-lisp-subscribe" --slot logical_initial_drift \
        --source-port "$SOURCE_DRIFT_PORT" --target-port "$REPLICA_DRIFT_PORT" \
        --table replicated_rows > "$TMP/worker-initial-drift.log" 2>&1; then
    echo "logical-replication: initial copy accepted an incompatible target schema" >&2
    exit 1
fi
if ! grep -F "target has existing schema definitions that differ from source" \
        "$TMP/worker-initial-drift.log" >/dev/null; then
    echo "logical-replication: initial schema drift failed for an unexpected reason" >&2
    exit 1
fi
if [ "$(sql "$REPLICA_DRIFT_PORT" "SELECT id, amount, label FROM replicated_rows")" \
        != "$drift_rows_before" ] || \
        [ "$(sequence_state_position "$REPLICA_DRIFT_PORT" bootstrap_counter)" \
        != "$drift_sequence_before" ] || \
        [ "$(sequence_definition_state "$REPLICA_DRIFT_PORT" bootstrap_counter)" \
        != "$drift_sequence_definition_before" ]; then
    echo "logical-replication: initial drift rejection changed target rows or sequence state" >&2
    exit 1
fi
kill "$SOURCE_DRIFT_PID" 2>/dev/null || true
wait "$SOURCE_DRIFT_PID" 2>/dev/null || true
SOURCE_DRIFT_PID=
kill "$REPLICA_DRIFT_PID" 2>/dev/null || true
wait "$REPLICA_DRIFT_PID" 2>/dev/null || true
REPLICA_DRIFT_PID=

PG_LISP_SUBSCRIBE_COPY_DELAY_MS=5000 \
    "$BASE/pg-lisp-subscribe" --slot logical_replica_one \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
    --table replicated_rows --table nullable_rows > "$TMP/worker-one.log" 2>&1 &
WORKER_ONE_PID=$!
# Exercise two durable copy chunks quickly; the production 8 MiB boundary is
# covered separately by test-logical-copy-chunk-byte-boundaries.
PG_LISP_TEST_FAILPOINT=after-copy-chunk-commit \
PG_LISP_TEST_FAILPOINT_ACTION=crash \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/worker-two-copy-crash.marker" \
PG_LISP_TEST_FAILPOINT_HIT=2 \
PG_LISP_TEST_COPY_CHUNK_BYTES=300 \
PG_LISP_SUBSCRIBE_COPY_DELAY_MS=5000 \
    "$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
    --table copy_stress --table replicated_rows --table nullable_rows > "$TMP/worker-two.log" 2>&1 &
WORKER_TWO_PID=$!

wait_for_log "$TMP/worker-one.log" "snapshot imported at"
wait_for_log "$TMP/worker-two.log" "snapshot imported at"
wait_for_log "$TMP/worker-one.log" "snapshot imported; copy pause"
wait_for_log "$TMP/worker-two.log" "snapshot imported; copy pause"
wait_for_state "$REPLICA_ONE_PORT" logical_replica_one copying
wait_for_state "$REPLICA_TWO_PORT" logical_replica_two copying
copy_boundary_two=$(sql "$REPLICA_TWO_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two'")

# Both exported snapshots already exist. Commit row and sequence changes while
# the copy is paused; they must appear after the snapshot boundary, following
# snapbuild.c's consistent-point and copy/stream handoff.
sql "$SOURCE_PORT" "SELECT nextval('bootstrap_counter')" >/dev/null
sql "$SOURCE_PORT" "BEGIN; INSERT INTO replicated_rows VALUES (2, 20, 'inserted'), (3, 30, 'temporary'); UPDATE replicated_rows SET amount = 11, label = NULL WHERE id = 1; DELETE FROM replicated_rows WHERE id = 9; DELETE FROM replicated_rows WHERE id = 3; UPDATE nullable_rows SET label = 'updated-null' WHERE nullable_key IS NULL; COMMIT"
sql "$SOURCE_PORT" "CREATE TABLE logical_copy_race (id integer PRIMARY KEY)" \
    > "$TMP/source-ddl.log" 2>&1 &
SOURCE_DDL_PID=$!

wait_for_file_timeout "$TMP/worker-two-copy-crash.marker" 1800
if ! grep -Fi "after-copy-chunk-commit" \
        "$TMP/worker-two-copy-crash.marker" >/dev/null; then
    echo "logical-replication: worker did not crash after a committed copy chunk" >&2
    cat "$TMP/worker-two-copy-crash.marker" >&2
    exit 1
fi
if wait "$WORKER_TWO_PID"; then
    echo "logical-replication: copy-chunk failpoint returned without crashing the process" >&2
    exit 1
else
    worker_status=$?
fi
if [ "$worker_status" -ne 137 ]; then
    echo "logical-replication: expected copy worker SIGKILL status 137, got $worker_status" >&2
    exit 1
fi
WORKER_TWO_PID=

if [ "$(sql "$REPLICA_TWO_PORT" \
        "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two'")" != copying ]; then
    echo "logical-replication: committed initial-copy chunk made the subscription ready" >&2
    exit 1
fi
if [ "$(sql "$REPLICA_TWO_PORT" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two'")" != "$copy_boundary_two" ]; then
    echo "logical-replication: initial-copy chunk advanced progress before copy completion" >&2
    exit 1
fi
if [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM copy_stress")" != 4 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM replicated_rows")" != 0 ]; then
    echo "logical-replication: second committed copy chunk or per-table copy order was not durable" >&2
    exit 1
fi

PG_LISP_TEST_FAILPOINT=after-target-commit-before-feedback \
PG_LISP_TEST_FAILPOINT_ACTION=crash \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/worker-two-crash.marker" \
    "$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
      --table copy_stress --table replicated_rows --table nullable_rows \
    > "$TMP/worker-two-feedback-fault.log" 2>&1 &
WORKER_TWO_PID=$!

wait_for_file "$TMP/worker-two-crash.marker"
if ! grep -Fi "after-target-commit-before-feedback" \
        "$TMP/worker-two-crash.marker" >/dev/null; then
    echo "logical-replication: worker missed target-commit/feedback failpoint" >&2
    cat "$TMP/worker-two-crash.marker" >&2
    exit 1
fi
if wait "$WORKER_TWO_PID"; then
    echo "logical-replication: target-commit failpoint returned without crashing" >&2
    exit 1
else
    worker_status=$?
fi
if [ "$worker_status" -ne 137 ]; then
    echo "logical-replication: expected target-commit SIGKILL status 137, got $worker_status" >&2
    exit 1
fi
WORKER_TWO_PID=

after_first_commit=$(sql "$REPLICA_TWO_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two' AND state = 'ready'")
if [ -z "$after_first_commit" ] || [ "$after_first_commit" = "$copy_boundary_two" ]; then
    echo "logical-replication: fault hook did not leave a durable target marker" >&2
    exit 1
fi

# A ready worker must bind its durable marker to the same publisher and table
# allowlist. Both checks run before it opens a source connection or slot.
if "$BASE/pg-lisp-subscribe" --slot logical_replica_two \
        --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
        --table replicated_rows > "$TMP/worker-wrong-allowlist.log" 2>&1; then
    echo "logical-replication: changed table allowlist was accepted on resume" >&2
    exit 1
fi
if ! grep -F "source or table allowlist does not match saved progress" \
        "$TMP/worker-wrong-allowlist.log" >/dev/null; then
    echo "logical-replication: allowlist mismatch did not report its saved config" >&2
    cat "$TMP/worker-wrong-allowlist.log" >&2
    exit 1
fi
if "$BASE/pg-lisp-subscribe" --slot logical_replica_two \
        --source-port "$((SOURCE_PORT + 100))" --target-port "$REPLICA_TWO_PORT" \
        --table copy_stress --table replicated_rows --table nullable_rows \
        > "$TMP/worker-wrong-source.log" 2>&1; then
    echo "logical-replication: changed source endpoint was accepted on resume" >&2
    exit 1
fi
if ! grep -F "source or table allowlist does not match saved progress" \
        "$TMP/worker-wrong-source.log" >/dev/null; then
    echo "logical-replication: source mismatch did not report its saved config" >&2
    cat "$TMP/worker-wrong-source.log" >&2
    exit 1
fi

expected_first=$(sql "$SOURCE_PORT" "SELECT id, amount, label FROM replicated_rows ORDER BY id")
wait_for_rows "$REPLICA_ONE_PORT" "$expected_first"
wait_for_rows "$REPLICA_TWO_PORT" "$expected_first"
expected_nullable_first=$(nullable_rows "$SOURCE_PORT")
wait_for_nullable_rows "$REPLICA_ONE_PORT" "$expected_nullable_first"
wait_for_nullable_rows "$REPLICA_TWO_PORT" "$expected_nullable_first"
if [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM copy_stress")" != 12 ] \
        || [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM copy_stress")" != 0 ]; then
    echo "logical-replication: initial copy chunking ignored the selected-table boundary" >&2
    exit 1
fi

# The exported slot boundary includes schema and sequence state. A later
# nextval is delivered as a standalone pgoutput Message and durably applied on
# both subscribers, like sequence.c's nontransactional sequence WAL update.
if [ "$(sequence_state_position "$SOURCE_PORT" bootstrap_counter)" != \
     "$(sequence_state_position "$REPLICA_ONE_PORT" bootstrap_counter)" ] \
        || [ "$(sequence_state_position "$SOURCE_PORT" bootstrap_counter)" != \
             "$(sequence_state_position "$REPLICA_TWO_PORT" bootstrap_counter)" ]; then
    echo "logical-replication: sequence state did not reach both subscribers" >&2
    exit 1
fi
if [ "$(sequence_definition_state "$SOURCE_PORT" bootstrap_counter)" != \
     "$(sequence_definition_state "$REPLICA_ONE_PORT" bootstrap_counter)" ] \
        || [ "$(sequence_definition_state "$SOURCE_PORT" bootstrap_counter)" != \
             "$(sequence_definition_state "$REPLICA_TWO_PORT" bootstrap_counter)" ]; then
    echo "logical-replication: bootstrap changed sequence start, increment, or bounds" >&2
    exit 1
fi
wait "$SOURCE_DDL_PID"
SOURCE_DDL_PID=

# The strict schema contract covers the whole supported catalog. Concurrent
# CREATE TABLE statements must therefore publish before/after shapes in one
# total order, even though PostgreSQL's tablecmds.c relation locks allow
# disjoint relations to change independently.
sql "$SOURCE_PORT" "CREATE TABLE concurrent_schema_one (id integer)" \
    > "$TMP/concurrent-ddl-one.log" 2>&1 &
concurrent_ddl_one_pid=$!
sql "$SOURCE_PORT" "CREATE TABLE concurrent_schema_two (id integer)" \
    > "$TMP/concurrent-ddl-two.log" 2>&1 &
concurrent_ddl_two_pid=$!
wait "$concurrent_ddl_one_pid"
wait "$concurrent_ddl_two_pid"

sql "$SOURCE_PORT" "BEGIN; INSERT INTO replicated_rows VALUES (77, 770, 'aborted'); ROLLBACK"

# Commit additional work while replica two is disconnected. It must restart from
# its target's durable marker without duplicating the transaction applied above.
sql "$SOURCE_PORT" "BEGIN; UPDATE replicated_rows SET amount = 22, label = 'resumed' WHERE id = 2; INSERT INTO replicated_rows VALUES (4, 40, 'later'); DELETE FROM nullable_rows WHERE nullable_key IS NULL; COMMIT"
sql "$SOURCE_PORT" "ALTER TABLE replicated_rows ADD CONSTRAINT replicated_rows_amount_key UNIQUE (amount); CREATE INDEX replicated_rows_id_amount_idx ON replicated_rows (id, amount); CREATE INDEX ignored_rows_label_idx ON ignored_rows (label); CREATE SEQUENCE streaming_counter START WITH 10 INCREMENT BY 5; SELECT nextval('streaming_counter'); ALTER SEQUENCE streaming_counter INCREMENT BY -3; SELECT nextval('streaming_counter')"
long_text_stream=$(awk 'BEGIN { for (i=0;i<3000;i++) printf "y" }')
sql "$SOURCE_PORT" "UPDATE replicated_rows SET label = '$long_text_stream' WHERE id = 1" >/dev/null
long_bytes_stream=$(awk 'BEGIN { for (i=0;i<1500;i++) printf "ff00" }')
sql "$SOURCE_PORT" "UPDATE nullable_rows SET label = '\\x$long_bytes_stream' WHERE nullable_key = 2" >/dev/null
sql "$SOURCE_PORT" "UPDATE nullable_rows SET j = '{\"k\":\"$long_text_stream\"}' WHERE nullable_key = 2" >/dev/null
sql "$SOURCE_PORT" "UPDATE nullable_rows SET b = '{\"v\":\"$long_text_stream\"}' WHERE nullable_key = 2" >/dev/null
disconnected_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
expected_final=$(sql "$SOURCE_PORT" "SELECT id, amount, label FROM replicated_rows ORDER BY id")
"$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
      --table copy_stress --table replicated_rows --table nullable_rows \
    > "$TMP/worker-two-restart.log" 2>&1 &
WORKER_TWO_PID=$!
wait_for_rows "$REPLICA_ONE_PORT" "$expected_final"
wait_for_rows "$REPLICA_TWO_PORT" "$expected_final"
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$disconnected_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$disconnected_barrier"
if [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM concurrent_schema_one")" != 0 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM concurrent_schema_one")" != 0 ] \
        || [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM concurrent_schema_two")" != 0 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM concurrent_schema_two")" != 0 ]; then
    echo "logical-replication: concurrent DDL schema contracts did not converge" >&2
    exit 1
fi
expected_nullable_final=$(nullable_rows "$SOURCE_PORT")
wait_for_nullable_rows "$REPLICA_ONE_PORT" "$expected_nullable_final"
wait_for_nullable_rows "$REPLICA_TWO_PORT" "$expected_nullable_final"
if [ "$(nullable_rows "$REPLICA_ONE_PORT")" != "$expected_nullable_final" ] \
        || [ "$(nullable_rows "$REPLICA_TWO_PORT")" != "$expected_nullable_final" ]; then
    echo "logical-replication: NULLS NOT DISTINCT key delete did not replicate" >&2
    echo "expected: $expected_nullable_final" >&2
    echo "replica one: $(nullable_rows "$REPLICA_ONE_PORT")" >&2
    echo "replica two: $(nullable_rows "$REPLICA_TWO_PORT")" >&2
    exit 1
fi
if [ "$(sequence_state_position "$SOURCE_PORT" streaming_counter)" != \
     "$(sequence_state_position "$REPLICA_ONE_PORT" streaming_counter)" ] \
        || [ "$(sequence_state_position "$SOURCE_PORT" streaming_counter)" != \
             "$(sequence_state_position "$REPLICA_TWO_PORT" streaming_counter)" ]; then
    echo "logical-replication: streamed sequence DDL or state diverged" >&2
    exit 1
fi
if [ "$(sequence_definition_state "$SOURCE_PORT" streaming_counter)" != \
     "$(sequence_definition_state "$REPLICA_ONE_PORT" streaming_counter)" ] \
        || [ "$(sequence_definition_state "$SOURCE_PORT" streaming_counter)" != \
             "$(sequence_definition_state "$REPLICA_TWO_PORT" streaming_counter)" ]; then
    echo "logical-replication: streamed sequence bounds or definition diverged" >&2
    exit 1
fi
if sql "$REPLICA_ONE_PORT" "CREATE INDEX replicated_rows_id_amount_idx ON replicated_rows (id, amount)" \
        >/dev/null 2>&1 \
        || sql "$REPLICA_TWO_PORT" "CREATE INDEX replicated_rows_id_amount_idx ON replicated_rows (id, amount)" \
            >/dev/null 2>&1; then
    echo "logical-replication: streamed index DDL did not reach both subscribers" >&2
    exit 1
fi

# A source transaction for an excluded table has no target row operation. Its
# commit still advances per-slot durable progress, just as no-row WAL intervals
# advance through logical keepalives in walsender.c.
progress_one_before=$(sql "$REPLICA_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")
progress_two_before=$(sql "$REPLICA_TWO_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two' AND state = 'ready'")
sql "$SOURCE_PORT" "UPDATE ignored_rows SET label = 'filtered' WHERE id = 1"
wait_for_progress_change "$REPLICA_ONE_PORT" logical_replica_one "$progress_one_before"
wait_for_progress_change "$REPLICA_TWO_PORT" logical_replica_two "$progress_two_before"

# Standalone DDL is applied before the worker acknowledges its terminal LSN.
# Stop the current worker and wait for its committed target marker to reach the
# slot. This drains any feedback already in flight before checking that a later
# schema rejection leaves confirmation at the old boundary.
kill -KILL "$WORKER_ONE_PID"
wait "$WORKER_ONE_PID" 2>/dev/null || true
WORKER_ONE_PID=
progress_one_before=$(sql "$REPLICA_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")
progress_two_before=$(sql "$REPLICA_TWO_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two' AND state = 'ready'")
wait_for_slot_confirmation "$SOURCE_PORT" logical_replica_one "$progress_one_before"
confirmed_one_before_drift=$(sql "$SOURCE_PORT" \
    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'logical_replica_one'")
rows_one_before_drift=$(replicated_rows "$REPLICA_ONE_PORT")
sql "$REPLICA_ONE_PORT" "CREATE TABLE subscriber_schema_drift (id integer)"
"$BASE/pg-lisp-subscribe" --slot logical_replica_one \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
      --table replicated_rows --table nullable_rows \
    > "$TMP/worker-one-drift-rejected.log" 2>&1 &
WORKER_ONE_PID=$!
sql "$SOURCE_PORT" "CREATE TABLE logical_progress_only (id integer)"
if ! wait_for_log "$TMP/worker-one-drift-rejected.log" \
        "subscriber schema differs from publisher schema before DDL"; then
    echo "logical-replication: subscriber schema drift was not rejected" >&2
    exit 1
fi
if wait "$WORKER_ONE_PID"; then
    echo "logical-replication: worker continued after incompatible DDL schema" >&2
    exit 1
fi
WORKER_ONE_PID=
confirmed_one_after_drift=$(sql "$SOURCE_PORT" \
    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'logical_replica_one'")
if [ "$confirmed_one_after_drift" != "$confirmed_one_before_drift" ]; then
    echo "logical-replication: schema rejection advanced publisher confirmation" >&2
    exit 1
fi
schema_drift_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
progress_one_after_drift=$(sql "$REPLICA_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")
barrier_normalized=$(normalize_lsn "$schema_drift_barrier")
progress_normalized=$(normalize_lsn "$progress_one_after_drift")
first_lsn=$(printf '%s\n%s\n' "$barrier_normalized" "$progress_normalized" \
    | LC_ALL=C sort | sed -n '1p')
if [ "$first_lsn" = "$barrier_normalized" ]; then
    echo "logical-replication: schema drift advanced progress through unapplied DDL" >&2
    exit 1
fi
if sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM logical_progress_only" \
        >/dev/null 2>&1; then
    echo "logical-replication: incompatible DDL changed the subscriber catalog" >&2
    exit 1
fi
if [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$rows_one_before_drift" ]; then
    echo "logical-replication: incompatible DDL changed subscriber table data" >&2
    exit 1
fi
sql "$REPLICA_ONE_PORT" "DROP TABLE subscriber_schema_drift"
"$BASE/pg-lisp-subscribe" --slot logical_replica_one \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
      --table replicated_rows --table nullable_rows \
    > "$TMP/worker-one-schema-drift.log" 2>&1 &
WORKER_ONE_PID=$!
wait_for_progress_change "$REPLICA_ONE_PORT" logical_replica_one "$progress_one_after_drift"
wait_for_progress_change "$REPLICA_TWO_PORT" logical_replica_two "$progress_two_before"
if [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM logical_copy_race")" != 0 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM logical_copy_race")" != 0 ]; then
    echo "logical-replication: DDL held by initial copy did not reach both subscribers" >&2
    exit 1
fi
if [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM logical_progress_only")" != 0 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM logical_progress_only")" != 0 ]; then
    echo "logical-replication: CREATE TABLE did not reach both subscribers" >&2
    exit 1
fi

# DDL for a populated publisher table outside the row allowlist was committed
# while replica two was disconnected. Both catalogs receive the supported index
# definition on resume, while row changes remain excluded, following the schema
# versus table-sync split in pgoutput.c and logical/tablesync.c.
if [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM ignored_rows")" != 0 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM ignored_rows")" != 0 ]; then
    echo "logical-replication: DDL for an excluded table copied its publisher rows" >&2
    exit 1
fi
if sql "$REPLICA_ONE_PORT" \
        "CREATE INDEX ignored_rows_label_idx ON ignored_rows(label)" >/dev/null 2>&1 \
        || sql "$REPLICA_TWO_PORT" \
            "CREATE INDEX ignored_rows_label_idx ON ignored_rows(label)" >/dev/null 2>&1; then
    echo "logical-replication: DDL for an excluded table did not reach both subscribers" >&2
    exit 1
fi

source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$source_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$source_barrier"

if [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$expected_final" ] \
        || [ "$(replicated_rows "$REPLICA_TWO_PORT")" != "$expected_final" ]; then
    echo "logical-replication: independent replicas diverged" >&2
    exit 1
fi
if [ "$(sql "$REPLICA_ONE_PORT" "SELECT count(*) FROM replicated_rows WHERE id = 77")" != 0 ] \
        || [ "$(sql "$REPLICA_TWO_PORT" "SELECT count(*) FROM replicated_rows WHERE id = 77")" != 0 ]; then
    echo "logical-replication: aborted source transaction became visible" >&2
    exit 1
fi

# Interrupt a target transaction after one of several row changes. The open
# target transaction must roll back both that row and its progress marker,
# matching the transactional rollback in logical/worker.c.
progress_one_before=$(sql "$REPLICA_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")
kill -KILL "$WORKER_ONE_PID"
wait "$WORKER_ONE_PID" 2>/dev/null || true
WORKER_ONE_PID=
PG_LISP_TEST_FAILPOINT=after-row-change \
PG_LISP_TEST_FAILPOINT_ACTION=crash \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/worker-one-row-crash.marker" \
  PG_LISP_TEST_FAILPOINT_HIT=2 \
    "$BASE/pg-lisp-subscribe" --slot logical_replica_one \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
      --table replicated_rows --table nullable_rows \
    > "$TMP/worker-one-row-fault.log" 2>&1 &
WORKER_ONE_PID=$!
sql "$SOURCE_PORT" "BEGIN; UPDATE replicated_rows SET label = 'row-failpoint'; COMMIT"
wait_for_file "$TMP/worker-one-row-crash.marker"
if ! grep -Fi "after-row-change" "$TMP/worker-one-row-crash.marker" >/dev/null; then
    echo "logical-replication: worker missed the row-change failpoint" >&2
    cat "$TMP/worker-one-row-crash.marker" >&2
    exit 1
fi
if wait "$WORKER_ONE_PID"; then
    echo "logical-replication: row-change failpoint returned without crashing" >&2
    exit 1
else
    worker_status=$?
fi
if [ "$worker_status" -ne 137 ]; then
    echo "logical-replication: expected row-change SIGKILL status 137, got $worker_status" >&2
    exit 1
fi
WORKER_ONE_PID=
if [ "$(sql "$REPLICA_ONE_PORT" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")" != "$progress_one_before" ]; then
    echo "logical-replication: row failpoint committed progress without its transaction" >&2
    exit 1
fi
if [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$expected_final" ]; then
    echo "logical-replication: a partial target row transaction survived worker crash" >&2
    exit 1
fi

"$BASE/pg-lisp-subscribe" --slot logical_replica_one \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
      --table replicated_rows --table nullable_rows \
    > "$TMP/worker-one-row-restart.log" 2>&1 &
WORKER_ONE_PID=$!
row_fault_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$row_fault_barrier"
row_fault_expected=$(replicated_rows "$SOURCE_PORT")
if [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$row_fault_expected" ]; then
    echo "logical-replication: row transaction did not replay atomically after restart" >&2
    exit 1
fi
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$row_fault_barrier"

  # Crash after all row effects are staged but before the target COMMIT. The
  # open transaction must still roll back both rows and the durable marker.
  kill -KILL "$WORKER_ONE_PID"
  wait "$WORKER_ONE_PID" 2>/dev/null || true
  WORKER_ONE_PID=
  progress_before_commit=$(sql "$REPLICA_ONE_PORT" \
      "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")
  rows_before_commit=$(replicated_rows "$REPLICA_ONE_PORT")
  PG_LISP_TEST_FAILPOINT=before-target-commit \
  PG_LISP_TEST_FAILPOINT_ACTION=crash \
  PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/worker-one-before-commit.marker" \
      "$BASE/pg-lisp-subscribe" --slot logical_replica_one \
      --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
      --table replicated_rows --table nullable_rows \
      > "$TMP/worker-one-before-commit.log" 2>&1 &
  WORKER_ONE_PID=$!
  sql "$SOURCE_PORT" \
      "BEGIN; UPDATE replicated_rows SET label = 'before-target-commit'; COMMIT"
  wait_for_file "$TMP/worker-one-before-commit.marker"
  if wait "$WORKER_ONE_PID"; then
      echo "logical-replication: before-target-commit failpoint returned without crashing" >&2
      exit 1
  else
      worker_status=$?
  fi
  [ "$worker_status" -eq 137 ] || {
      echo "logical-replication: expected pre-commit SIGKILL 137, got $worker_status" >&2
      exit 1
  }
  WORKER_ONE_PID=
  if [ "$(sql "$REPLICA_ONE_PORT" \
          "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")" != "$progress_before_commit" ] \
          || [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$rows_before_commit" ]; then
      echo "logical-replication: target effects or progress survived a pre-commit crash" >&2
      exit 1
  fi
  "$BASE/pg-lisp-subscribe" --slot logical_replica_one \
      --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
      --table replicated_rows --table nullable_rows \
      > "$TMP/worker-one-before-commit-restart.log" 2>&1 &
  WORKER_ONE_PID=$!
  before_commit_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
  wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$before_commit_barrier"
  if [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$(replicated_rows "$SOURCE_PORT")" ]; then
      echo "logical-replication: pre-commit transaction did not replay after restart" >&2
      exit 1
  fi

# Hold one subscriber offline while a large transaction and concurrent writers
# advance WAL. The slot must retain the unapplied interval until that target
# commits it; then repeat the reconnect once more to cover repeated resumes.
kill -KILL "$WORKER_TWO_PID"
wait "$WORKER_TWO_PID" 2>/dev/null || true
WORKER_TWO_PID=
progress_two_before=$(sql "$REPLICA_TWO_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two' AND state = 'ready'")
bulk_values=
bulk_id=10000
while [ "$bulk_id" -lt 10300 ]; do
    if [ -n "$bulk_values" ]; then
        bulk_values="$bulk_values,"
    fi
    bulk_values="$bulk_values($bulk_id,$((bulk_id * 10)),'bulk')"
    bulk_id=$((bulk_id + 1))
done
sql "$SOURCE_PORT" \
    "BEGIN; INSERT INTO replicated_rows VALUES $bulk_values; COMMIT" >/dev/null
sql "$SOURCE_PORT" "INSERT INTO replicated_rows VALUES (20001, 200010, 'writer-one')" \
    > "$TMP/stress-writer-one.log" 2>&1 &
writer_one_pid=$!
sql "$SOURCE_PORT" "INSERT INTO replicated_rows VALUES (30001, 300010, 'writer-two')" \
    > "$TMP/stress-writer-two.log" 2>&1 &
writer_two_pid=$!
wait "$writer_one_pid"
wait "$writer_two_pid"
if [ "$(sql "$REPLICA_TWO_PORT" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two' AND state = 'ready'")" != "$progress_two_before" ]; then
    echo "logical-replication: disconnected subscriber advanced its durable marker" >&2
    exit 1
fi
slot_retained=$(sql "$SOURCE_PORT" \
    "SELECT retained_wal_bytes FROM pg_replication_slots WHERE slot_name = 'logical_replica_two'")
slot_active=$(sql "$SOURCE_PORT" \
    "SELECT active FROM pg_replication_slots WHERE slot_name = 'logical_replica_two'")
if [ "$slot_active" != f ] || [ -z "$slot_retained" ] || [ "$slot_retained" -le 0 ]; then
    echo "logical-replication: lagging slot did not retain its unapplied WAL" >&2
    exit 1
fi
"$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
      --table copy_stress --table replicated_rows --table nullable_rows \
    > "$TMP/worker-two-stress-restart.log" 2>&1 &
WORKER_TWO_PID=$!
stress_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$stress_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$stress_barrier"
source_stress_rows=$(sql "$SOURCE_PORT" \
    "SELECT id, amount, label FROM replicated_rows WHERE id >= 10000 ORDER BY id")
if [ "$(sql "$REPLICA_ONE_PORT" \
        "SELECT id, amount, label FROM replicated_rows WHERE id >= 10000 ORDER BY id")" != "$source_stress_rows" ] \
        || [ "$(sql "$REPLICA_TWO_PORT" \
             "SELECT id, amount, label FROM replicated_rows WHERE id >= 10000 ORDER BY id")" != "$source_stress_rows" ]; then
    echo "logical-replication: large transaction or concurrent writer changes diverged" >&2
    exit 1
fi

kill -KILL "$WORKER_TWO_PID"
wait "$WORKER_TWO_PID" 2>/dev/null || true
WORKER_TWO_PID=
sql "$SOURCE_PORT" "INSERT INTO replicated_rows VALUES (40001, 400010, 'reconnect-two')" >/dev/null
"$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
      --table copy_stress --table replicated_rows --table nullable_rows \
    > "$TMP/worker-two-second-reconnect.log" 2>&1 &
WORKER_TWO_PID=$!
reconnect_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$reconnect_barrier"
if [ "$(replicated_rows "$SOURCE_PORT")" != "$(replicated_rows "$REPLICA_TWO_PORT")" ]; then
    echo "logical-replication: repeated subscriber reconnect diverged" >&2
    exit 1
fi

# TRUNCATE is a transactional row operation. A rollback must emit no target
# effect; a commit must clear each selected table with its source commit marker.
rows_before_truncate=$(replicated_rows "$SOURCE_PORT")
sql "$SOURCE_PORT" "BEGIN; TRUNCATE TABLE replicated_rows; ROLLBACK"
truncate_rollback_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$truncate_rollback_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$truncate_rollback_barrier"
if [ "$(replicated_rows "$SOURCE_PORT")" != "$rows_before_truncate" ] \
        || [ "$(replicated_rows "$REPLICA_ONE_PORT")" != "$rows_before_truncate" ] \
        || [ "$(replicated_rows "$REPLICA_TWO_PORT")" != "$rows_before_truncate" ]; then
    echo "logical-replication: rolled-back TRUNCATE changed source or subscriber rows" >&2
    exit 1
fi

sql "$SOURCE_PORT" "TRUNCATE TABLE replicated_rows"
truncate_commit_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$truncate_commit_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$truncate_commit_barrier"
if [ -n "$(replicated_rows "$SOURCE_PORT")" ] \
        || [ -n "$(replicated_rows "$REPLICA_ONE_PORT")" ] \
        || [ -n "$(replicated_rows "$REPLICA_TWO_PORT")" ]; then
    echo "logical-replication: committed TRUNCATE did not clear selected tables" >&2
    exit 1
fi

# Persist expression ASTs and replay each new CHECK/default catalog action.
sql "$SOURCE_PORT" "ALTER TABLE bootstrap_constraint_order ALTER a SET DEFAULT '4.5'::numeric; ALTER TABLE bootstrap_constraint_order ADD CONSTRAINT streamed_check CHECK(a<100) NOT VALID; ALTER TABLE bootstrap_constraint_order VALIDATE CONSTRAINT streamed_check; ALTER TABLE bootstrap_constraint_order RENAME CONSTRAINT streamed_check TO renamed_check; ALTER TABLE bootstrap_constraint_order DROP CONSTRAINT renamed_check" >/dev/null
expression_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$expression_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$expression_barrier"
created_reference=$(sql "$SOURCE_PORT" 'BEGIN; INSERT INTO bootstrap_temporal(id) VALUES(1) RETURNING created; ROLLBACK')
for port in "$SOURCE_PORT" "$REPLICA_ONE_PORT" "$REPLICA_TWO_PORT"; do
    test "$(sql "$port" 'BEGIN; INSERT INTO bootstrap_constraint_order DEFAULT VALUES RETURNING a; ROLLBACK')" = 5
    test "$(sql "$port" 'BEGIN; INSERT INTO bootstrap_temporal(id) VALUES(1) RETURNING d,z; ROLLBACK')" = '2024-02-01|2024-02-01 11:00:00+00'
    test "$(sql "$port" 'BEGIN; INSERT INTO bootstrap_temporal(id) VALUES(1) RETURNING created; ROLLBACK')" = "$created_reference"
done

# A streamed ALTER must retain the source's cooked date and time zone semantics.
sql "$SOURCE_PORT" "SET DateStyle TO 'ISO, DMY'; SET TimeZone TO 'Europe/Warsaw'; ALTER TABLE bootstrap_temporal ALTER d SET DEFAULT '04/03/2024'; ALTER TABLE bootstrap_temporal ADD CONSTRAINT later_date CHECK(d >= '01/03/2024') NOT VALID; ALTER TABLE bootstrap_temporal ALTER z SET DEFAULT '04/03/2024 12:00:00'" >/dev/null
temporal_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$temporal_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$temporal_barrier"
for port in "$SOURCE_PORT" "$REPLICA_ONE_PORT" "$REPLICA_TWO_PORT"; do
    test "$(sql "$port" 'BEGIN; INSERT INTO bootstrap_temporal(id) VALUES(1) RETURNING d,z; ROLLBACK')" = '2024-03-04|2024-03-04 11:00:00+00'
done

echo "logical-replication: PASS"
