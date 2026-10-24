#!/bin/sh
# Exercise slot loss, importer cleanup, and reseeding with a stalled consumer.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT_BASE=${PORT_BASE:-5640}
SOURCE_PORT=$PORT_BASE
FAST_PORT=$((PORT_BASE + 1))
SLOW_PORT=$((PORT_BASE + 2))
RESEED_PORT=$((PORT_BASE + 3))
BLOCKED_PORT=$((PORT_BASE + 4))
SLOT_CAP_BYTES=${SLOT_CAP_BYTES:-40960}
FAST_WORKER_RSS_MAX_KB=${FAST_WORKER_RSS_MAX_KB:-262144}
BLOCKED_WORKER_RSS_MAX_KB=${BLOCKED_WORKER_RSS_MAX_KB:-262144}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-backpressure.XXXXXX")
SOURCE_PID=
FAST_PID=
SLOW_PID=
RESEED_PID=
BLOCKED_PID=
FAST_WORKER_PID=
SLOW_WORKER_PID=
RESEED_WORKER_PID=
RSS_SAMPLER_PID=

sql() {
    SQL_PORT=$1
    SQL_TEXT=$2
    psql -X -h 127.0.0.1 -p "$SQL_PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' -c "$SQL_TEXT"
}

sql_file() {
    SQL_FILE_PORT=$1
    SQL_FILE_PATH=$2
    psql -X -h 127.0.0.1 -p "$SQL_FILE_PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' < "$SQL_FILE_PATH"
}

start_server() {
    SERVER_PORT=$1
    SERVER_DATA=$2
    SERVER_LOG=$3
    shift 3
    PG_LISP_TEST_SOCKET_SEND_BUFFER_BYTES=4096 \
        "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" "$@" \
        > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    i=0
    while [ "$i" -lt 200 ]; do
        if psql -X -h 127.0.0.1 -p "$SERVER_PORT" -U postgres -d postgres \
                -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "logical-slot-backpressure: server exited during startup: $SERVER_LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-slot-backpressure: server did not start on port $SERVER_PORT" >&2
    return 1
}

wait_for_state() {
    SQL_PORT=$1
    SLOT_NAME=$2
    STATE_NAME=$3
    STATE_WAIT_ATTEMPTS=${4:-1200}
    i=0
    while [ "$i" -lt "$STATE_WAIT_ATTEMPTS" ]; do
        state=$(sql "$SQL_PORT" \
            "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME'" \
            2>/dev/null || true)
        [ "$state" = "$STATE_NAME" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-slot-backpressure: $SLOT_NAME did not reach $STATE_NAME" >&2
    return 1
}

wait_for_lsn() {
    SQL_PORT=$1
    SLOT_NAME=$2
    BARRIER=$3
    i=0
    while [ "$i" -lt 1800 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME' AND state = 'ready'" \
            2>/dev/null || true)
        if [ -n "$current" ]; then
            high=${current%/*}
            low=${current#*/}
            barrier_high=${BARRIER%/*}
            barrier_low=${BARRIER#*/}
            current=$(printf '%08s/%08s' "$high" "$low" | tr ' ' '0')
            barrier=$(printf '%08s/%08s' "$barrier_high" "$barrier_low" | tr ' ' '0')
            first=$(printf '%s\n%s\n' "$barrier" "$current" | LC_ALL=C sort | sed -n '1p')
            [ "$first" = "$barrier" ] && return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-slot-backpressure: $SLOT_NAME did not reach source LSN $BARRIER" >&2
    return 1
}

lsn_at_least() {
    lsn_left=$1
    lsn_right=$2
    lsn_left_high=${lsn_left%/*}
    lsn_left_low=${lsn_left#*/}
    lsn_right_high=${lsn_right%/*}
    lsn_right_low=${lsn_right#*/}
    lsn_left=$(printf '%08s/%08s' "$lsn_left_high" "$lsn_left_low" | tr ' ' '0')
    lsn_right=$(printf '%08s/%08s' "$lsn_right_high" "$lsn_right_low" | tr ' ' '0')
    lsn_first=$(printf '%s\n%s\n' "$lsn_left" "$lsn_right" | LC_ALL=C sort | sed -n '1p')
    [ "$lsn_first" = "$lsn_right" ]
}

lsn_greater_than() {
    lsn_at_least "$1" "$2" && ! lsn_at_least "$2" "$1"
}

wait_for_slot_confirmation() {
    SLOT_NAME=$1
    BARRIER=$2
    i=0
    while [ "$i" -lt 1800 ]; do
        confirmation_current=$(sql "$SOURCE_PORT" \
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'")
        if [ -n "$confirmation_current" ] && \
                lsn_at_least "$confirmation_current" "$BARRIER"; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-slot-backpressure: $SLOT_NAME did not confirm source LSN $BARRIER" >&2
    return 1
}

slot_status() {
    sql "$SOURCE_PORT" \
        "SELECT wal_status FROM pg_replication_slots WHERE slot_name = '$1'"
}

slot_positions() {
    sql "$SOURCE_PORT" \
        "SELECT restart_lsn, confirmed_flush_lsn, retained_wal_bytes, wal_status FROM pg_replication_slots WHERE slot_name = '$1'"
}

target_marker() {
    sql "$1" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$2' AND state = 'ready'"
}

timed_insert() {
    metric_file=$1
    row_id=$2
    row_label=$3
    started_ms=$(date +%s%3N)
    sql "$SOURCE_PORT" \
        "INSERT INTO cap_rows VALUES ($row_id, '$row_label')" >/dev/null
    finished_ms=$(date +%s%3N)
    printf '%s\n' "$((finished_ms - started_ms))" > "$metric_file"
}

wait_for_file() {
    FILE_PATH=$1
    i=0
    while [ "$i" -lt 300 ]; do
        [ -f "$FILE_PATH" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-slot-backpressure: timed out waiting for $FILE_PATH" >&2
    return 1
}

wait_for_process_exit() {
    PROCESS_PID=$1
    i=0
    while [ "$i" -lt 600 ]; do
        process_state=$(ps -o stat= -p "$PROCESS_PID" 2>/dev/null || true)
        case "$process_state" in
            ""|Z*) return 0 ;;
        esac
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-slot-backpressure: process $PROCESS_PID did not exit" >&2
    return 1
}

rows() {
    sql "$1" "SELECT id, label FROM cap_rows ORDER BY id"
}

sample_rss() {
    SAMPLE_PID=$1
    SAMPLE_FILE=$2
    while kill -0 "$SAMPLE_PID" 2>/dev/null; do
        ps -o rss= -p "$SAMPLE_PID" 2>/dev/null | awk 'NF { print $1 }' >> "$SAMPLE_FILE"
        sleep 0.05
    done
}

cleanup() {
    exit_status=$1
    for pid in "$RSS_SAMPLER_PID" "$SLOW_WORKER_PID" "$FAST_WORKER_PID" \
               "$RESEED_WORKER_PID" "$SOURCE_PID" "$FAST_PID" "$SLOW_PID" \
               "$RESEED_PID" "$BLOCKED_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    for pid in "$RSS_SAMPLER_PID" "$SLOW_WORKER_PID" "$FAST_WORKER_PID" \
               "$RESEED_WORKER_PID" "$SOURCE_PID" "$FAST_PID" "$SLOW_PID" \
               "$RESEED_PID" "$BLOCKED_PID"; do
        [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
    done
    if [ "$exit_status" -ne 0 ] && [ "${KEEP_TMP_ON_FAILURE:-0}" = 1 ]; then
        echo "logical-slot-backpressure: preserved artifacts in $TMP" >&2
    else
        rm -rf "$TMP"
    fi
}

on_exit() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        echo "logical-slot-backpressure: failure; logs follow" >&2
        for log in "$TMP"/*.log; do
            [ -f "$log" ] || continue
            echo "--- $log" >&2
            case "$log" in
                *-rss-kb.log) tail -n 5 "$log" >&2 ;;
                *) cat "$log" >&2 ;;
            esac
        done
    fi
    cleanup "$status"
    exit "$status"
}

trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source.log" \
    "--max-slot-wal-keep-bytes=$SLOT_CAP_BYTES"
SOURCE_PID=$SERVER_PID
start_server "$FAST_PORT" "$TMP/fast-data" "$TMP/fast.log"
FAST_PID=$SERVER_PID
start_server "$SLOW_PORT" "$TMP/slow-data" "$TMP/slow.log"
SLOW_PID=$SERVER_PID
sql "$SOURCE_PORT" \
    "CREATE TABLE cap_rows (id integer PRIMARY KEY, label varchar(64)); CREATE TABLE block_rows (id integer PRIMARY KEY, label varchar(64), p1 varchar(126), p2 varchar(126), p3 varchar(126), p4 varchar(126), p5 varchar(126), p6 varchar(126), p7 varchar(126), p8 varchar(126)); INSERT INTO cap_rows VALUES (1, 'seed')" \
    >/dev/null

"$BASE/pg-lisp-subscribe" --slot fast_slot \
    --source-port "$SOURCE_PORT" --target-port "$FAST_PORT" \
    --table cap_rows > "$TMP/fast-worker.log" 2>&1 &
FAST_WORKER_PID=$!
wait_for_state "$FAST_PORT" fast_slot ready

"$BASE/pg-lisp-subscribe" --slot slow_slot \
    --source-port "$SOURCE_PORT" --target-port "$SLOW_PORT" \
    --table cap_rows > "$TMP/slow-worker.log" 2>&1 &
SLOW_WORKER_PID=$!
wait_for_state "$SLOW_PORT" slow_slot ready

# Record a low-load write with both subscribers current. This is a baseline,
# not a pass/fail threshold; compare it with the concurrent write during lag.
timed_insert "$TMP/baseline-writer-ms" 3 baseline
baseline_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$FAST_PORT" fast_slot "$baseline_barrier"
wait_for_lsn "$SLOW_PORT" slow_slot "$baseline_barrier"
wait_for_slot_confirmation fast_slot "$baseline_barrier"
wait_for_slot_confirmation slow_slot "$baseline_barrier"

# First leave a small committed interval unapplied. Checkpoint must preserve a
# reserved slot and its WAL under the cap; reconnect then catches it up. The
# pause failpoint is enabled only for that reconnect, where it stops after the
# target commit and before publisher feedback.
kill -KILL "$SLOW_WORKER_PID"
wait "$SLOW_WORKER_PID" 2>/dev/null || true
SLOW_WORKER_PID=
sql "$SOURCE_PORT" "INSERT INTO cap_rows VALUES (2, 'under-cap')" >/dev/null
under_cap_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
under_cap_retained=$(sql "$SOURCE_PORT" \
    "SELECT retained_wal_bytes FROM pg_replication_slots WHERE slot_name = 'slow_slot'")
under_cap_slow_marker=$(target_marker "$SLOW_PORT" slow_slot)
under_cap_slow_positions=$(slot_positions slow_slot)
if [ "$under_cap_retained" -le 0 ] || [ "$under_cap_retained" -gt "$SLOT_CAP_BYTES" ]; then
    echo "logical-slot-backpressure: setup interval is not within the WAL cap" >&2
    exit 1
fi
sql "$SOURCE_PORT" "CHECKPOINT" >/dev/null
if [ "$(slot_status slow_slot)" != reserved ]; then
    echo "logical-slot-backpressure: checkpoint invalidated an under-cap slot" >&2
    exit 1
fi
under_cap_retained_after=$(sql "$SOURCE_PORT" \
    "SELECT retained_wal_bytes FROM pg_replication_slots WHERE slot_name = 'slow_slot'")
if [ "$under_cap_retained_after" -le 0 ] || \
        [ "$under_cap_retained_after" -gt "$SLOT_CAP_BYTES" ]; then
    echo "logical-slot-backpressure: checkpoint did not preserve bounded required WAL" >&2
    exit 1
fi
PG_LISP_TEST_FAILPOINT=after-target-commit-before-feedback \
PG_LISP_TEST_FAILPOINT_ACTION=pause \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/slow-feedback.marker" \
PG_LISP_TEST_FAILPOINT_RELEASE_FILE="$TMP/release-slow-worker" \
    "$BASE/pg-lisp-subscribe" --slot slow_slot \
    --source-port "$SOURCE_PORT" --target-port "$SLOW_PORT" \
    --table cap_rows > "$TMP/slow-worker.log" 2>&1 &
SLOW_WORKER_PID=$!
wait_for_lsn "$SLOW_PORT" slow_slot "$under_cap_barrier"
if [ "$(sql "$SLOW_PORT" \
        "SELECT id, label FROM cap_rows WHERE id = 2")" != "2|under-cap" ]; then
    echo "logical-slot-backpressure: under-cap resume advanced progress without applying its row" >&2
    exit 1
fi
wait_for_file "$TMP/slow-feedback.marker"
paused_slow_marker=$(target_marker "$SLOW_PORT" slow_slot)
paused_slow_positions=$(slot_positions slow_slot)
if ! lsn_at_least "$paused_slow_marker" "$under_cap_barrier"; then
    echo "logical-slot-backpressure: paused target marker is behind its applied row" >&2
    exit 1
fi

bulk_values=
bulk_id=10000
while [ "$bulk_id" -lt 11000 ]; do
    [ -z "$bulk_values" ] || bulk_values="$bulk_values,"
    bulk_values="$bulk_values($bulk_id,'bulk')"
    bulk_id=$((bulk_id + 1))
done
sample_rss "$FAST_WORKER_PID" "$TMP/fast-worker-rss-kb.log" &
RSS_SAMPLER_PID=$!
bulk_start=$(date +%s%3N)
sql "$SOURCE_PORT" "BEGIN; INSERT INTO cap_rows VALUES $bulk_values; COMMIT" \
    > "$TMP/bulk-writer.log" 2>&1 &
bulk_pid=$!
timed_insert "$TMP/concurrent-writer-ms" 20001 concurrent-writer \
    > "$TMP/concurrent-writer.log" 2>&1 &
writer_pid=$!
wait "$bulk_pid"
bulk_end=$(date +%s%3N)
wait_for_file "$TMP/slow-feedback.marker"
wait "$writer_pid"
fast_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$FAST_PORT" fast_slot "$fast_barrier"
wait_for_slot_confirmation fast_slot "$fast_barrier"
kill "$RSS_SAMPLER_PID" 2>/dev/null || true
wait "$RSS_SAMPLER_PID" 2>/dev/null || true
RSS_SAMPLER_PID=
if [ ! -s "$TMP/fast-worker-rss-kb.log" ]; then
    echo "logical-slot-backpressure: fast-worker RSS sampler produced no samples" >&2
    exit 1
fi
fast_rss_high_water=$(sort -nr "$TMP/fast-worker-rss-kb.log" | sed -n '1p')
case "$fast_rss_high_water" in
    ''|*[!0-9]*)
        echo "logical-slot-backpressure: fast-worker RSS sample is not numeric" >&2
        exit 1
        ;;
esac
if [ "$fast_rss_high_water" -gt "$FAST_WORKER_RSS_MAX_KB" ]; then
    echo "logical-slot-backpressure: fast worker exceeded RSS limit: ${fast_rss_high_water} KiB" >&2
    exit 1
fi

slow_retained=$(sql "$SOURCE_PORT" \
    "SELECT retained_wal_bytes FROM pg_replication_slots WHERE slot_name = 'slow_slot'")
slow_positions=$(slot_positions slow_slot)
slow_target_marker=$(target_marker "$SLOW_PORT" slow_slot)
fast_positions=$(slot_positions fast_slot)
fast_target_marker=$(target_marker "$FAST_PORT" fast_slot)
slow_restart_lsn=${slow_positions%%|*}
slow_position_rest=${slow_positions#*|}
slow_confirmed_lsn=${slow_position_rest%%|*}
fast_position_rest=${fast_positions#*|}
fast_confirmed_lsn=${fast_position_rest%%|*}
fast_confirmed=$(sql "$SOURCE_PORT" \
    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'fast_slot'")
if [ "$slow_retained" -le "$SLOT_CAP_BYTES" ]; then
    echo "logical-slot-backpressure: slow slot did not cross its configured cap" >&2
    exit 1
fi
if [ "$paused_slow_marker" != "$slow_target_marker" ] \
        || [ -z "$under_cap_slow_positions" ] || [ -z "$paused_slow_positions" ] \
        || [ -z "$slow_positions" ] \
        || [ -z "$fast_positions" ] || [ -z "$fast_target_marker" ]; then
    echo "logical-slot-backpressure: paused slot positions or target marker changed unexpectedly" >&2
    exit 1
fi
if [ "$slow_target_marker" = "$slow_confirmed_lsn" ] \
        || ! lsn_at_least "$slow_target_marker" "$slow_restart_lsn" \
        || ! lsn_at_least "$fast_target_marker" "$fast_confirmed_lsn"; then
    echo "logical-slot-backpressure: target progress was not ordered with slot positions" >&2
    exit 1
fi
fast_status_before=$(slot_status fast_slot)
slow_status_before=$(slot_status slow_slot)
if [ "$fast_status_before" != reserved ] || [ "$slow_status_before" != reserved ]; then
    echo "logical-slot-backpressure: a slot was lost before checkpoint" >&2
    exit 1
fi

# Checkpoint persists the slow slot's loss before recycling WAL. The fast
# consumer has already committed and acknowledged the same interval.
sql "$SOURCE_PORT" "CHECKPOINT" >/dev/null
if [ "$(slot_status slow_slot)" != lost ]; then
    echo "logical-slot-backpressure: checkpoint did not persist slow-slot loss" >&2
    exit 1
fi
if [ "$(slot_status fast_slot)" != reserved ]; then
    echo "logical-slot-backpressure: checkpoint invalidated the caught-up subscriber" >&2
    exit 1
fi
if [ "$(sql "$SOURCE_PORT" \
        "SELECT active FROM pg_replication_slots WHERE slot_name = 'slow_slot'")" != f ]; then
    echo "logical-slot-backpressure: checkpoint left the lost slot's reader active" >&2
    exit 1
fi
fast_confirmed_after=$(sql "$SOURCE_PORT" \
    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'fast_slot'")
if [ -z "$fast_confirmed" ] || [ -z "$fast_confirmed_after" ] || \
        ! lsn_at_least "$fast_confirmed_after" "$fast_confirmed"; then
    echo "logical-slot-backpressure: fast subscriber's durable acknowledgment regressed" >&2
    exit 1
fi
slow_positions_after_loss=$(slot_positions slow_slot)
fast_positions_after_checkpoint=$(slot_positions fast_slot)

# The slow client is paused after target COMMIT and before feedback. The source
# checkpoint must terminate its blocked walsender; release the pause so the
# client can observe that its old slot connection is gone.
: > "$TMP/release-slow-worker"
i=0
while kill -0 "$SLOW_WORKER_PID" 2>/dev/null && [ "$i" -lt 100 ]; do
    i=$((i + 1))
    sleep 0.1
done
if kill -0 "$SLOW_WORKER_PID" 2>/dev/null; then
    echo "logical-slot-backpressure: invalidated reader did not unwind" >&2
    exit 1
fi
wait "$SLOW_WORKER_PID" 2>/dev/null || true
SLOW_WORKER_PID=

# Losing the slot revokes its exported token and stops the importer, so DDL can
# acquire the schema lock instead of waiting for the stalled copy. CREATE TABLE
# is part of the currently implemented DDL subset and is streamed database-wide.
sql "$SOURCE_PORT" "CREATE TABLE after_loss (id integer)" >/dev/null
fast_ddl_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$FAST_PORT" fast_slot "$fast_ddl_barrier"
if [ "$(sql "$FAST_PORT" "SELECT count(*) FROM after_loss")" != 0 ]; then
    echo "logical-slot-backpressure: fast subscriber missed DDL after slow-slot loss" >&2
    exit 1
fi

if "$BASE/pg-lisp-subscribe" --slot slow_slot \
        --source-port "$SOURCE_PORT" --target-port "$SLOW_PORT" \
        --table cap_rows > "$TMP/lost-slot-reconnect.log" 2>&1; then
    echo "logical-slot-backpressure: reconnect accepted a slot whose WAL was lost" >&2
    exit 1
fi
if ! grep -F "has lost its WAL" "$TMP/lost-slot-reconnect.log" >/dev/null; then
    echo "logical-slot-backpressure: lost-slot reconnect failed for an unexpected reason" >&2
    cat "$TMP/lost-slot-reconnect.log" >&2
    exit 1
fi

# The source may have changed schema since the slow subscriber disconnected, so
# use a fresh target for this reseed. Stop its worker after the first row of a
# large transaction. A tiny send buffer blocks CopyData; CHECKPOINT must close
# that socket before it waits for the walsender to unwind.
start_server "$BLOCKED_PORT" "$TMP/blocked-data" "$TMP/blocked.log"
BLOCKED_PID=$SERVER_PID
PG_LISP_TEST_FAILPOINT=after-row-change \
PG_LISP_TEST_FAILPOINT_ACTION=pause \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/blocked-row.marker" \
PG_LISP_TEST_FAILPOINT_RELEASE_FILE="$TMP/release-blocked-worker" \
    "$BASE/pg-lisp-subscribe" --slot blocked_slot \
    --source-port "$SOURCE_PORT" --target-port "$BLOCKED_PORT" \
    --table block_rows > "$TMP/blocked-worker.log" 2>&1 &
SLOW_WORKER_PID=$!
wait_for_state "$BLOCKED_PORT" blocked_slot ready
blocked_values=
blocked_id=50000
blocked_payload=$(awk 'BEGIN { for (i = 0; i < 126; i++) printf "x" }')
while [ "$blocked_id" -lt 50300 ]; do
    [ -z "$blocked_values" ] || blocked_values="$blocked_values,"
    blocked_values="$blocked_values($blocked_id,'blocked','$blocked_payload','$blocked_payload','$blocked_payload','$blocked_payload','$blocked_payload','$blocked_payload','$blocked_payload','$blocked_payload')"
    blocked_id=$((blocked_id + 1))
done
blocked_marker_before=$(target_marker "$BLOCKED_PORT" blocked_slot)
blocked_pre_write_lsn=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
blocked_sql_file="$TMP/blocked-writer.sql"
printf 'BEGIN; INSERT INTO block_rows VALUES %s; COMMIT\n' "$blocked_values" \
    > "$blocked_sql_file"
sql_file "$SOURCE_PORT" "$blocked_sql_file" \
    > "$TMP/blocked-writer.log" 2>&1 &
blocked_writer_pid=$!
wait_for_file "$TMP/blocked-row.marker"
sample_rss "$SLOW_WORKER_PID" "$TMP/blocked-worker-rss-kb.log" &
RSS_SAMPLER_PID=$!
wait "$blocked_writer_pid"
sleep 0.5
kill "$RSS_SAMPLER_PID" 2>/dev/null || true
wait "$RSS_SAMPLER_PID" 2>/dev/null || true
RSS_SAMPLER_PID=
if [ ! -s "$TMP/blocked-worker-rss-kb.log" ]; then
    echo "logical-slot-backpressure: blocked-worker RSS sampler produced no samples" >&2
    exit 1
fi
blocked_worker_rss_high_water=$(sort -nr "$TMP/blocked-worker-rss-kb.log" | sed -n '1p')
case "$blocked_worker_rss_high_water" in
    ''|*[!0-9]*)
        echo "logical-slot-backpressure: blocked-worker RSS sample is not numeric" >&2
        exit 1
        ;;
esac
if [ "$blocked_worker_rss_high_water" -gt "$BLOCKED_WORKER_RSS_MAX_KB" ]; then
    echo "logical-slot-backpressure: blocked worker exceeded RSS limit: ${blocked_worker_rss_high_water} KiB" >&2
    exit 1
fi
blocked_retained=$(sql "$SOURCE_PORT" \
    "SELECT retained_wal_bytes FROM pg_replication_slots WHERE slot_name = 'blocked_slot'")
if [ "$blocked_retained" -le "$SLOT_CAP_BYTES" ]; then
    echo "logical-slot-backpressure: blocked sender did not exceed the slot WAL cap" >&2
    exit 1
fi
timeout 30s psql -X -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres \
    -q -v ON_ERROR_STOP=1 -c "CHECKPOINT" >/dev/null
if [ "$(slot_status blocked_slot)" != lost ]; then
    echo "logical-slot-backpressure: checkpoint did not invalidate the blocked sender slot" >&2
    exit 1
fi
: > "$TMP/release-blocked-worker"
wait_for_process_exit "$SLOW_WORKER_PID"
if wait "$SLOW_WORKER_PID"; then
    echo "logical-slot-backpressure: worker survived blocked-sender invalidation" >&2
    exit 1
fi
SLOW_WORKER_PID=
if [ "$(sql "$BLOCKED_PORT" "SELECT count(*) FROM block_rows WHERE id >= 50000")" != 0 ]; then
    echo "logical-slot-backpressure: partial target transaction survived sender shutdown" >&2
    exit 1
fi
blocked_marker_after=$(target_marker "$BLOCKED_PORT" blocked_slot)
if ! lsn_at_least "$blocked_marker_after" "$blocked_marker_before"; then
    echo "logical-slot-backpressure: target marker regressed during lost-slot rollback" >&2
    exit 1
fi
if lsn_greater_than "$blocked_marker_after" "$blocked_pre_write_lsn"; then
    echo "logical-slot-backpressure: lost-slot transaction advanced past its pre-write marker" >&2
    exit 1
fi

start_server "$RESEED_PORT" "$TMP/reseed-data" "$TMP/reseed.log"
RESEED_PID=$SERVER_PID
"$BASE/pg-lisp-subscribe" --slot reseeded_slot \
    --source-port "$SOURCE_PORT" --target-port "$RESEED_PORT" \
    --table cap_rows > "$TMP/reseed-worker.log" 2>&1 &
RESEED_WORKER_PID=$!
wait_for_state "$RESEED_PORT" reseeded_slot ready
reseed_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$RESEED_PORT" reseeded_slot "$reseed_barrier"
if [ "$(rows "$RESEED_PORT")" != "$(rows "$SOURCE_PORT")" ]; then
    echo "logical-slot-backpressure: fresh-slot reseed did not catch up" >&2
    exit 1
fi

bulk_elapsed_ms=$((bulk_end - bulk_start))
baseline_writer_ms=$(cat "$TMP/baseline-writer-ms")
writer_latency_ms=$(cat "$TMP/concurrent-writer-ms")
echo "logical-slot-backpressure: cap=$SLOT_CAP_BYTES under_cap_slow_marker=$under_cap_slow_marker under_cap_slow_slot=$under_cap_slow_positions"
echo "logical-slot-backpressure: slow_marker=$slow_target_marker slow_slot=$slow_positions slow_after_loss=$slow_positions_after_loss"
echo "logical-slot-backpressure: fast_marker=$fast_target_marker fast_slot=$fast_positions fast_after_checkpoint=$fast_positions_after_checkpoint"
echo "logical-slot-backpressure: bulk_ms=$bulk_elapsed_ms baseline_writer_ms=$baseline_writer_ms concurrent_writer_ms=$writer_latency_ms fast_worker_rss_high_water_kb=$fast_rss_high_water blocked_worker_rss_high_water_kb=$blocked_worker_rss_high_water rss_limit_kb=$BLOCKED_WORKER_RSS_MAX_KB"
echo "logical-slot-backpressure: PASS"
