#!/bin/sh
# Three-server logical replication regression: one source and two subscribers.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT_BASE=${PORT_BASE:-5610}
SOURCE_PORT=$PORT_BASE
REPLICA_ONE_PORT=$((PORT_BASE + 1))
REPLICA_TWO_PORT=$((PORT_BASE + 2))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical.XXXXXX")
SOURCE_PID=
REPLICA_ONE_PID=
REPLICA_TWO_PID=
WORKER_ONE_PID=
WORKER_TWO_PID=
SOURCE_DDL_PID=

sql() {
    SQL_PORT=$1
    SQL_TEXT=$2
    psql -X -h 127.0.0.1 -p "$SQL_PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' -c "$SQL_TEXT"
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
    sql "$1" "SELECT nullable_key, label FROM nullable_rows ORDER BY nullable_key"
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
    while [ "$i" -lt 300 ]; do
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
    echo "logical-replication: slot $SLOT_NAME did not reach source LSN $BARRIER" >&2
    return 1
}

cleanup() {
    for pid in "$WORKER_ONE_PID" "$WORKER_TWO_PID" "$SOURCE_DDL_PID" \
               "$SOURCE_PID" "$REPLICA_ONE_PID" "$REPLICA_TWO_PID"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$WORKER_ONE_PID" "$WORKER_TWO_PID" "$SOURCE_DDL_PID" \
               "$SOURCE_PID" "$REPLICA_ONE_PID" "$REPLICA_TWO_PID"; do
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
        for log in "$TMP/source.log" "$TMP/replica-one.log" \
                   "$TMP/replica-two.log" "$TMP/worker-one.log" \
                    "$TMP/worker-two.log" "$TMP/worker-two-restart.log"; do
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

sql "$SOURCE_PORT" "CREATE TABLE replicated_rows (id integer PRIMARY KEY, amount bigint NOT NULL, label varchar(64)); CREATE TABLE nullable_rows (nullable_key integer, label varchar(64)); CREATE UNIQUE INDEX nullable_rows_key_idx ON nullable_rows (nullable_key) NULLS NOT DISTINCT; CREATE TABLE ignored_rows (id integer PRIMARY KEY, label varchar(64)); CREATE SEQUENCE bootstrap_counter START WITH 10 INCREMENT BY 2; ALTER SEQUENCE bootstrap_counter INCREMENT BY -2; SELECT setval('bootstrap_counter', 100, true); INSERT INTO replicated_rows VALUES (1, 10, 'initial'), (9, 90, 'remove'); INSERT INTO nullable_rows VALUES (NULL, 'initial-null'), (2, 'two'); INSERT INTO ignored_rows VALUES (1, 'before')"

PG_LISP_SUBSCRIBE_COPY_DELAY_MS=5000 \
    "$BASE/pg-lisp-subscribe" --slot logical_replica_one \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_ONE_PORT" \
    --table replicated_rows --table nullable_rows > "$TMP/worker-one.log" 2>&1 &
WORKER_ONE_PID=$!
PG_LISP_SUBSCRIBE_COPY_DELAY_MS=5000 \
PG_LISP_SUBSCRIBE_DROP_FEEDBACK_ONCE=1 \
    "$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
    --table replicated_rows --table nullable_rows > "$TMP/worker-two.log" 2>&1 &
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

wait_for_log "$TMP/worker-two.log" "dropping feedback after durable target commit"
wait "$WORKER_TWO_PID"
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
        --table replicated_rows --table nullable_rows \
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
sql "$SOURCE_PORT" "BEGIN; INSERT INTO replicated_rows VALUES (77, 770, 'aborted'); ROLLBACK"

# Commit additional work while replica two is disconnected. It must restart from
# its target's durable marker without duplicating the transaction applied above.
sql "$SOURCE_PORT" "BEGIN; UPDATE replicated_rows SET amount = 22, label = 'resumed' WHERE id = 2; INSERT INTO replicated_rows VALUES (4, 40, 'later'); DELETE FROM nullable_rows WHERE nullable_key IS NULL; COMMIT"
sql "$SOURCE_PORT" "ALTER TABLE replicated_rows ADD CONSTRAINT replicated_rows_amount_key UNIQUE (amount); CREATE INDEX replicated_rows_id_amount_idx ON replicated_rows (id, amount); CREATE SEQUENCE streaming_counter START WITH 10 INCREMENT BY 5; SELECT nextval('streaming_counter'); ALTER SEQUENCE streaming_counter INCREMENT BY -3; SELECT nextval('streaming_counter')"
disconnected_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
expected_final=$(sql "$SOURCE_PORT" "SELECT id, amount, label FROM replicated_rows ORDER BY id")
"$BASE/pg-lisp-subscribe" --slot logical_replica_two \
    --source-port "$SOURCE_PORT" --target-port "$REPLICA_TWO_PORT" \
    --table replicated_rows --table nullable_rows \
    > "$TMP/worker-two-restart.log" 2>&1 &
WORKER_TWO_PID=$!
wait_for_rows "$REPLICA_ONE_PORT" "$expected_final"
wait_for_rows "$REPLICA_TWO_PORT" "$expected_final"
wait_for_lsn_barrier "$REPLICA_ONE_PORT" logical_replica_one "$disconnected_barrier"
wait_for_lsn_barrier "$REPLICA_TWO_PORT" logical_replica_two "$disconnected_barrier"
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
progress_one_before=$(sql "$REPLICA_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_one' AND state = 'ready'")
progress_two_before=$(sql "$REPLICA_TWO_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'logical_replica_two' AND state = 'ready'")
sql "$SOURCE_PORT" "CREATE TABLE logical_progress_only (id integer)"
wait_for_progress_change "$REPLICA_ONE_PORT" logical_replica_one "$progress_one_before"
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

echo "logical-replication: PASS"
