#!/bin/sh
# Crash/restart checks for logical-copy handoff state transitions.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT_BASE=${PORT_BASE:-5620}
SOURCE_PORT=$PORT_BASE
TARGET_ONE_PORT=$((PORT_BASE + 1))
TARGET_TWO_PORT=$((PORT_BASE + 2))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-faults.XXXXXX")
SOURCE_PID=
TARGET_ONE_PID=
TARGET_TWO_PID=
WORKER_ONE_PID=
WORKER_TWO_PID=

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
    shift 3
    if [ "$#" -eq 2 ]; then
        FAULT_POINT=$1
        FAULT_MARKER=$2
        PG_LISP_TEST_FAILPOINT=$FAULT_POINT \
        PG_LISP_TEST_FAILPOINT_ACTION=crash \
        PG_LISP_TEST_FAILPOINT_ONCE_FILE=$FAULT_MARKER \
            "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
            > "$SERVER_LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
            > "$SERVER_LOG" 2>&1 &
    fi
    SERVER_PID=$!
    i=0
    while [ "$i" -lt 200 ]; do
        if psql -X -h 127.0.0.1 -p "$SERVER_PORT" -U postgres -d postgres \
                -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "logical-replication-faults: server exited during startup: $SERVER_LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication-faults: server did not start on port $SERVER_PORT" >&2
    return 1
}

wait_for_file() {
    FILE_PATH=$1
    i=0
    while [ "$i" -lt 300 ]; do
        [ -f "$FILE_PATH" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication-faults: timed out waiting for $FILE_PATH" >&2
    return 1
}

wait_for_log() {
    LOG_FILE=$1
    LOG_TEXT=$2
    i=0
    while [ "$i" -lt 300 ]; do
        grep -F "$LOG_TEXT" "$LOG_FILE" >/dev/null 2>&1 && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication-faults: timed out waiting for '$LOG_TEXT' in $LOG_FILE" >&2
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
        [ "$state" = "$STATE_NAME" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication-faults: slot $SLOT_NAME did not reach $STATE_NAME" >&2
    return 1
}

normalize_lsn() {
    LSN_HIGH=${1%/*}
    LSN_LOW=${1#*/}
    LSN_HIGH=$(printf '%8s' "$LSN_HIGH" | tr ' ' '0')
    LSN_LOW=$(printf '%8s' "$LSN_LOW" | tr ' ' '0')
    printf '%s/%s' "$LSN_HIGH" "$LSN_LOW"
}

wait_for_lsn() {
    SQL_PORT=$1
    SLOT_NAME=$2
    BARRIER=$(normalize_lsn "$3")
    i=0
    while [ "$i" -lt 300 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME' AND state = 'ready'")
        if [ -n "$current" ]; then
            current=$(normalize_lsn "$current")
            first=$(printf '%s\n%s\n' "$BARRIER" "$current" | LC_ALL=C sort | sed -n '1p')
            [ "$first" = "$BARRIER" ] && return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication-faults: slot $SLOT_NAME did not reach $BARRIER" >&2
    return 1
}

wait_for_slot_confirmation() {
    SQL_PORT=$1
    SLOT_NAME=$2
    BARRIER=$(normalize_lsn "$3")
    i=0
    while [ "$i" -lt 300 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'")
        if [ -n "$current" ]; then
            current=$(normalize_lsn "$current")
            first=$(printf '%s\n%s\n' "$BARRIER" "$current" | LC_ALL=C sort | sed -n '1p')
            [ "$first" = "$BARRIER" ] && return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-replication-faults: slot $SLOT_NAME did not confirm $BARRIER" >&2
    return 1
}

rows() {
    sql "$1" "SELECT id, label FROM fault_rows ORDER BY id"
}

start_copy_fault_worker() {
    COPY_FAULT_SLOT=$1
    COPY_FAULT_POINT=$2
    COPY_FAULT_MARKER=$3
    COPY_FAULT_LOG=$4
    PG_LISP_TEST_FAILPOINT=$COPY_FAULT_POINT \
    PG_LISP_TEST_FAILPOINT_ACTION=crash \
    PG_LISP_TEST_FAILPOINT_ONCE_FILE=$COPY_FAULT_MARKER \
        "$BASE/pg-lisp-subscribe" --slot "$COPY_FAULT_SLOT" \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
        --table fault_rows > "$COPY_FAULT_LOG" 2>&1 &
    WORKER_ONE_PID=$!
}

start_copy_worker() {
    COPY_SLOT=$1
    COPY_LOG=$2
    "$BASE/pg-lisp-subscribe" --slot "$COPY_SLOT" \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
        --table fault_rows > "$COPY_LOG" 2>&1 &
    WORKER_ONE_PID=$!
}

target_copy_state() {
    sql "$TARGET_ONE_PORT" \
        "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot = '$1'"
}

target_copy_token() {
    sql "$TARGET_ONE_PORT" \
        "SELECT snapshot_token FROM pg_lisp_subscription_progress WHERE source_slot = '$1'"
}

wait_for_sigkill() {
    KILLED_PID=$1
    FAULT_NAME=$2
    if wait "$KILLED_PID"; then
        echo "logical-replication-faults: $FAULT_NAME returned without crashing" >&2
        return 1
    else
        KILLED_STATUS=$?
    fi
    [ "$KILLED_STATUS" -eq 137 ] || {
        echo "logical-replication-faults: expected $FAULT_NAME SIGKILL 137, got $KILLED_STATUS" >&2
        return 1
    }
}

exercise_copy_worker_fault() {
    COPY_POINT=$1
    COPY_SLOT=$2
    EXPECTED_STATE=$3
    EXPECTED_ROWS=$4
    COPY_MARKER=$TMP/$COPY_SLOT.marker
    BEFORE_ROWS=

    sql "$TARGET_ONE_PORT" \
        "UPDATE fault_rows SET label = 'copy-sentinel' WHERE id = 1" >/dev/null
    BEFORE_ROWS=$(rows "$TARGET_ONE_PORT")
    start_copy_fault_worker "$COPY_SLOT" "$COPY_POINT" "$COPY_MARKER" \
        "$TMP/$COPY_SLOT-fault.log"
    wait_for_file "$COPY_MARKER"
    wait_for_sigkill "$WORKER_ONE_PID" "$COPY_POINT"
    WORKER_ONE_PID=

    [ "$(target_copy_state "$COPY_SLOT")" = "$EXPECTED_STATE" ] || {
        echo "logical-replication-faults: $COPY_POINT left the wrong target progress state" >&2
        return 1
    }
    if [ "$EXPECTED_STATE" = copying ] && [ -z "$(target_copy_token "$COPY_SLOT")" ]; then
        echo "logical-replication-faults: $COPY_POINT lost the live snapshot token" >&2
        return 1
    fi
    case "$EXPECTED_ROWS" in
        untouched)
            [ "$(rows "$TARGET_ONE_PORT")" = "$BEFORE_ROWS" ] || {
                echo "logical-replication-faults: $COPY_POINT changed rows before reset" >&2
                return 1
            }
            ;;
        empty)
            [ -z "$(rows "$TARGET_ONE_PORT")" ] || {
                echo "logical-replication-faults: $COPY_POINT committed a partial copy chunk" >&2
                return 1
            }
            ;;
        copied)
            [ "$(rows "$TARGET_ONE_PORT")" = "$(rows "$SOURCE_PORT")" ] || {
                echo "logical-replication-faults: $COPY_POINT lost committed initial-copy rows" >&2
                return 1
            }
            ;;
    esac

    start_copy_worker "$COPY_SLOT" "$TMP/$COPY_SLOT-retry.log"
    wait_for_state "$TARGET_ONE_PORT" "$COPY_SLOT" ready
    source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
    wait_for_lsn "$TARGET_ONE_PORT" "$COPY_SLOT" "$source_barrier"
    [ "$(rows "$TARGET_ONE_PORT")" = "$(rows "$SOURCE_PORT")" ] || {
        echo "logical-replication-faults: $COPY_POINT retry did not converge" >&2
        return 1
    }
    [ "$(sql "$SOURCE_PORT" \
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$COPY_SLOT'")" = 1 ] || {
        echo "logical-replication-faults: $COPY_POINT retry left an orphaned or missing slot" >&2
        return 1
    }
    stop_worker "$WORKER_ONE_PID"
    WORKER_ONE_PID=
}

exercise_worker_before_copying_progress_fault() {
    COPY_SLOT=$1
    COPY_MARKER=$TMP/$COPY_SLOT.marker
    sql "$TARGET_ONE_PORT" \
        "UPDATE fault_rows SET label = 'pre-progress-sentinel' WHERE id = 1" >/dev/null
    BEFORE_ROWS=$(rows "$TARGET_ONE_PORT")
    start_copy_fault_worker "$COPY_SLOT" \
        after-source-slot-created-before-copying-progress "$COPY_MARKER" \
        "$TMP/$COPY_SLOT-fault.log"
    wait_for_file "$COPY_MARKER"
    wait_for_sigkill "$WORKER_ONE_PID" \
        after-source-slot-created-before-copying-progress
    WORKER_ONE_PID=
    [ -z "$(target_copy_state "$COPY_SLOT")" ] || {
        echo "logical-replication-faults: source-slot handoff published incomplete target progress" >&2
        return 1
    }
    [ "$(rows "$TARGET_ONE_PORT")" = "$BEFORE_ROWS" ] || {
        echo "logical-replication-faults: pre-progress crash changed target rows" >&2
        return 1
    }
    [ "$(sql "$SOURCE_PORT" \
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$COPY_SLOT'")" = 1 ] || {
        echo "logical-replication-faults: pre-progress crash lost the durable source slot" >&2
        return 1
    }
    start_copy_worker "$COPY_SLOT" "$TMP/$COPY_SLOT-retry.log"
    wait_for_state "$TARGET_ONE_PORT" "$COPY_SLOT" ready
    source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
    wait_for_lsn "$TARGET_ONE_PORT" "$COPY_SLOT" "$source_barrier"
    [ "$(rows "$TARGET_ONE_PORT")" = "$(rows "$SOURCE_PORT")" ] || {
        echo "logical-replication-faults: pre-progress retry did not reseed cleanly" >&2
        return 1
    }
    [ "$(sql "$SOURCE_PORT" \
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$COPY_SLOT'")" = 1 ] || {
        echo "logical-replication-faults: pre-progress retry left duplicate source slots" >&2
        return 1
    }
    stop_worker "$WORKER_ONE_PID"
    WORKER_ONE_PID=
}

stop_worker() {
    WORKER_PID=$1
    [ -n "$WORKER_PID" ] || return 0
    kill -KILL "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
}

cleanup() {
    for pid in "$WORKER_ONE_PID" "$WORKER_TWO_PID" "$SOURCE_PID" \
               "$TARGET_ONE_PID" "$TARGET_TWO_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    for pid in "$WORKER_ONE_PID" "$WORKER_TWO_PID" "$SOURCE_PID" \
               "$TARGET_ONE_PID" "$TARGET_TWO_PID"; do
        [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMP"
}

on_exit() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        echo "logical-replication-faults: failure; logs follow" >&2
        for log in "$TMP/source.log" "$TMP/source-restart.log" \
                   "$TMP/target-one.log" "$TMP/target-two.log" \
                   "$TMP/worker-one.log" "$TMP/worker-one-restart.log" \
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

# Crash after the slot's consistent LSN is finalized, before the exported
# snapshot lease is released. This exercises snapbuild.c's copy/stream handoff.
start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source.log" \
    after-copy-slot-finalization-before-snapshot-release "$TMP/finalize.marker"
SOURCE_PID=$SERVER_PID
start_server "$TARGET_ONE_PORT" "$TMP/target-one-data" "$TMP/target-one.log"
TARGET_ONE_PID=$SERVER_PID
sql "$SOURCE_PORT" \
    "CREATE TABLE fault_rows (id integer PRIMARY KEY, label varchar(64)); INSERT INTO fault_rows VALUES (1, 'one'), (2, 'two')" \
    >/dev/null

"$BASE/pg-lisp-subscribe" --slot fault_before_release \
    --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
    --table fault_rows > "$TMP/worker-one.log" 2>&1 &
WORKER_ONE_PID=$!
wait_for_file "$TMP/finalize.marker"
if ! grep -Fi "after-copy-slot-finalization-before-snapshot-release" \
        "$TMP/finalize.marker" >/dev/null; then
    echo "logical-replication-faults: source missed slot-finalization failpoint" >&2
    exit 1
fi
if wait "$SOURCE_PID"; then
    echo "logical-replication-faults: source failpoint returned without crashing" >&2
    exit 1
else
    source_status=$?
fi
[ "$source_status" -eq 137 ] || {
    echo "logical-replication-faults: expected source SIGKILL 137, got $source_status" >&2
    exit 1
}
SOURCE_PID=
wait_for_state "$TARGET_ONE_PORT" fault_before_release ready
[ "$(rows "$TARGET_ONE_PORT")" = "1|one
2|two" ] || {
    echo "logical-replication-faults: initial target copy was not durable before slot finalization" >&2
    exit 1
}
stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=

start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-restart.log"
SOURCE_PID=$SERVER_PID
"$BASE/pg-lisp-subscribe" --slot fault_before_release \
    --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
    --table fault_rows > "$TMP/worker-one-restart.log" 2>&1 &
WORKER_ONE_PID=$!
wait_for_log "$TMP/worker-one-restart.log" "streaming from"
source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$TARGET_ONE_PORT" fault_before_release "$source_barrier"
sql "$SOURCE_PORT" "INSERT INTO fault_rows VALUES (3, 'after-restart')" >/dev/null
source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$TARGET_ONE_PORT" fault_before_release "$source_barrier"
[ "$(rows "$TARGET_ONE_PORT")" = "1|one
2|two
3|after-restart" ] || {
    echo "logical-replication-faults: finalized slot did not resume after source restart" >&2
    exit 1
}

stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=
kill -TERM "$SOURCE_PID"
wait "$SOURCE_PID" 2>/dev/null || true
SOURCE_PID=

# A crash before slot finalization must preserve the ready target marker while
# leaving the source slot unconfirmed. Restart finalizes the same boundary and
# streams without resetting the already-copied target table.
start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-before-finalize.log" \
    before-copy-slot-finalization "$TMP/before-finalize.marker"
SOURCE_PID=$SERVER_PID
start_copy_worker fault_before_finalize "$TMP/worker-before-finalize.log"
wait_for_file "$TMP/before-finalize.marker"
wait_for_sigkill "$SOURCE_PID" before-copy-slot-finalization
SOURCE_PID=
stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=
wait_for_state "$TARGET_ONE_PORT" fault_before_finalize ready
pre_finalize_marker=$(sql "$TARGET_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'fault_before_finalize'")
sql "$TARGET_ONE_PORT" \
    "UPDATE fault_rows SET label = 'preserve-ready-marker' WHERE id = 1" >/dev/null

start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-before-finalize-restart.log"
SOURCE_PID=$SERVER_PID
[ "$(sql "$SOURCE_PORT" \
        "SELECT confirmed_flush_lsn IS NULL FROM pg_replication_slots WHERE slot_name = 'fault_before_finalize'")" = t ] || {
    echo "logical-replication-faults: slot was confirmed before copy finalization retry" >&2
    exit 1
}
start_copy_worker fault_before_finalize "$TMP/worker-before-finalize-retry.log"
wait_for_log "$TMP/worker-before-finalize-retry.log" "streaming from"
wait_for_slot_confirmation "$SOURCE_PORT" fault_before_finalize "$pre_finalize_marker"
[ "$(sql "$SOURCE_PORT" \
        "SELECT confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name = 'fault_before_finalize'")" = t ] || {
    echo "logical-replication-faults: restart did not finalize the pending copy boundary" >&2
    exit 1
}
confirmed_lsn=$(sql "$SOURCE_PORT" \
    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'fault_before_finalize'")
[ "$(normalize_lsn "$confirmed_lsn")" = "$(normalize_lsn "$pre_finalize_marker")" ] || {
    echo "logical-replication-faults: restart finalized a different copy boundary" >&2
    exit 1
}
[ "$(sql "$TARGET_ONE_PORT" "SELECT label FROM fault_rows WHERE id = 1")" = preserve-ready-marker ] || {
    echo "logical-replication-faults: restart recopied a ready target marker" >&2
    exit 1
}
resumed_marker=$(sql "$TARGET_ONE_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'fault_before_finalize'")
resumed_marker=$(normalize_lsn "$resumed_marker")
pre_finalize_marker=$(normalize_lsn "$pre_finalize_marker")
first_marker=$(printf '%s\n%s\n' "$pre_finalize_marker" "$resumed_marker" | LC_ALL=C sort | sed -n '1p')
[ "$first_marker" = "$pre_finalize_marker" ] || {
    echo "logical-replication-faults: resumed marker moved behind the durable copy boundary" >&2
    exit 1
}
sql "$TARGET_ONE_PORT" "UPDATE fault_rows SET label = 'one' WHERE id = 1" >/dev/null
stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=
kill -TERM "$SOURCE_PID"
wait "$SOURCE_PID" 2>/dev/null || true
SOURCE_PID=

# Crash after the snapshot schema lease has actually been released. The slot
# and target marker are already durable, so restart must resume at that marker.
start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-release.log" \
    after-snapshot-schema-lease-release "$TMP/release.marker"
SOURCE_PID=$SERVER_PID
start_server "$TARGET_TWO_PORT" "$TMP/target-two-data" "$TMP/target-two.log"
TARGET_TWO_PID=$SERVER_PID
"$BASE/pg-lisp-subscribe" --slot fault_after_release \
    --source-port "$SOURCE_PORT" --target-port "$TARGET_TWO_PORT" \
    --table fault_rows > "$TMP/worker-two.log" 2>&1 &
WORKER_TWO_PID=$!
wait_for_file "$TMP/release.marker"
if ! grep -Fi "after-snapshot-schema-lease-release" "$TMP/release.marker" >/dev/null; then
    echo "logical-replication-faults: source missed snapshot-release failpoint" >&2
    exit 1
fi
if wait "$SOURCE_PID"; then
    echo "logical-replication-faults: snapshot-release failpoint returned without crashing" >&2
    exit 1
else
    source_status=$?
fi
[ "$source_status" -eq 137 ] || {
    echo "logical-replication-faults: expected source SIGKILL 137, got $source_status" >&2
    exit 1
}
SOURCE_PID=
wait_for_state "$TARGET_TWO_PORT" fault_after_release ready
[ "$(rows "$TARGET_TWO_PORT")" = "1|one
2|two
3|after-restart" ] || {
    echo "logical-replication-faults: copy was not durable at snapshot-release crash" >&2
    exit 1
}
stop_worker "$WORKER_TWO_PID"
WORKER_TWO_PID=

start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-release-restart.log"
SOURCE_PID=$SERVER_PID
"$BASE/pg-lisp-subscribe" --slot fault_after_release \
    --source-port "$SOURCE_PORT" --target-port "$TARGET_TWO_PORT" \
    --table fault_rows > "$TMP/worker-two-restart.log" 2>&1 &
WORKER_TWO_PID=$!
wait_for_log "$TMP/worker-two-restart.log" "streaming from"
source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$TARGET_TWO_PORT" fault_after_release "$source_barrier"
if [ "$(rows "$TARGET_TWO_PORT")" != "$(rows "$SOURCE_PORT")" ]; then
    echo "logical-replication-faults: resumed subscriber diverged after snapshot release" >&2
    exit 1
fi

# Restart the publisher after real SIGKILLs across snapshot capture, slot-file
# durability, in-memory slot publication, and token publication. This follows
# snapmgr.c's snapshot handoff and ReplicationSlotPersist's file-before-visible
# ordering. A crash before rename leaves no slot; after rename the filesystem
# may expose either image until directory fsync, and restart must accept either.
stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=
stop_worker "$WORKER_TWO_PID"
WORKER_TWO_PID=
kill -TERM "$SOURCE_PID"
wait "$SOURCE_PID" 2>/dev/null || true
SOURCE_PID=
for slot_fault in \
    "after-snapshot-boundary-capture|fault_boundary_capture|absent" \
    "before-initial-slot-publication|fault_before_slot_publication|absent" \
    "slot-file-before-write|fault_slot_before_write|absent" \
    "slot-file-after-write|fault_slot_after_write|absent" \
    "slot-file-before-fsync|fault_slot_before_fsync|absent" \
    "slot-file-after-fsync|fault_slot_after_fsync|absent" \
    "slot-file-before-rename|fault_slot_before_rename|absent" \
    "slot-file-after-rename|fault_slot_after_rename|present" \
    "slot-file-before-directory-fsync|fault_slot_before_directory_fsync|either" \
    "slot-file-after-directory-fsync-before-memory-publication|fault_slot_after_sync|present" \
    "after-initial-slot-file-publication|fault_after_slot_publication|present" \
    "before-snapshot-token-publication|fault_before_token_publication|present" \
    "after-snapshot-token-publication|fault_after_token_publication|present"; do
    fault_point=${slot_fault%%|*}
    slot_rest=${slot_fault#*|}
    slot_name=${slot_rest%%|*}
    expected_slot=${slot_rest#*|}
    fault_log="$TMP/$slot_name-source.log"
    marker="$TMP/$slot_name.marker"
    start_server "$SOURCE_PORT" "$TMP/source-data" "$fault_log" \
        "$fault_point" "$marker"
    SOURCE_PID=$SERVER_PID
    worker_log="$TMP/$slot_name-worker.log"
    "$BASE/pg-lisp-subscribe" --slot "$slot_name" \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
        --table fault_rows > "$worker_log" 2>&1 &
    WORKER_ONE_PID=$!
    wait_for_file "$marker"
    if wait "$SOURCE_PID"; then
        echo "logical-replication-faults: $fault_point did not crash the source" >&2
        exit 1
    else
        source_status=$?
    fi
    [ "$source_status" -eq 137 ] || {
        echo "logical-replication-faults: $fault_point returned source status $source_status" >&2
        exit 1
    }
    SOURCE_PID=
    stop_worker "$WORKER_ONE_PID"
    WORKER_ONE_PID=

    start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/$slot_name-restart.log"
    SOURCE_PID=$SERVER_PID
    progress_state=$(sql "$TARGET_ONE_PORT" \
        "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot = '$slot_name'")
    if [ -n "$progress_state" ]; then
        echo "logical-replication-faults: $fault_point published target progress before slot setup returned" >&2
        exit 1
    fi
    slot_file_row=$(sql "$SOURCE_PORT" \
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '$slot_name'")
    if [ "$expected_slot" = present ] && [ "$slot_file_row" != "$slot_name" ]; then
        echo "logical-replication-faults: restart did not load the renamed slot image" >&2
        exit 1
    fi
    if [ "$expected_slot" = absent ] && [ -n "$slot_file_row" ]; then
        echo "logical-replication-faults: pre-rename crash published a slot image" >&2
        exit 1
    fi
    if [ "$expected_slot" = either ] && [ -n "$slot_file_row" ] \
            && [ "$slot_file_row" != "$slot_name" ]; then
        echo "logical-replication-faults: directory-sync boundary loaded an invalid slot image" >&2
        exit 1
    fi

    "$BASE/pg-lisp-subscribe" --slot "$slot_name" \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
        --table fault_rows > "$TMP/$slot_name-retry.log" 2>&1 &
    WORKER_ONE_PID=$!
    wait_for_state "$TARGET_ONE_PORT" "$slot_name" ready
    source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
    wait_for_lsn "$TARGET_ONE_PORT" "$slot_name" "$source_barrier"
    if [ "$(rows "$TARGET_ONE_PORT")" != "$(rows "$SOURCE_PORT")" ]; then
        echo "logical-replication-faults: $fault_point retry did not complete the copy" >&2
        exit 1
    fi
    stop_worker "$WORKER_ONE_PID"
    WORKER_ONE_PID=
    kill -TERM "$SOURCE_PID"
    wait "$SOURCE_PID" 2>/dev/null || true
    SOURCE_PID=
done

# Crash the publisher after a worker has durably recorded :copying and imported
# the exported snapshot. The process-local token must expire; retry drops the
# pending slot, reseeds, and retains no partial target effects. This is the
# exported-snapshot cleanup path from snapmgr.c's transaction lifecycle.
sql "$TARGET_ONE_PORT" \
    "UPDATE fault_rows SET label = 'expired-token-sentinel' WHERE id = 1" >/dev/null
start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-import-crash.log" \
    after-snapshot-import "$TMP/after-snapshot-import.marker"
SOURCE_PID=$SERVER_PID
"$BASE/pg-lisp-subscribe" --slot fault_after_import \
    --source-port "$SOURCE_PORT" --target-port "$TARGET_ONE_PORT" \
    --table fault_rows > "$TMP/fault-after-import-worker.log" 2>&1 &
WORKER_ONE_PID=$!
wait_for_file "$TMP/after-snapshot-import.marker"
wait_for_sigkill "$SOURCE_PID" after-snapshot-import
SOURCE_PID=
stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=
wait_for_state "$TARGET_ONE_PORT" fault_after_import copying
[ -n "$(sql "$TARGET_ONE_PORT" \
        "SELECT snapshot_token FROM pg_lisp_subscription_progress WHERE source_slot = 'fault_after_import'")" ] || {
    echo "logical-replication-faults: source crash lost the resumable snapshot token" >&2
    exit 1
}
[ "$(rows "$TARGET_ONE_PORT")" = "1|expired-token-sentinel
2|two
3|after-restart" ] || {
    echo "logical-replication-faults: imported-snapshot crash changed target rows before reset" >&2
    exit 1
}
start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source-import-restart.log"
SOURCE_PID=$SERVER_PID
start_copy_worker fault_after_import "$TMP/fault-after-import-retry.log"
wait_for_log "$TMP/fault-after-import-retry.log" "exported snapshot expired; reseeding"
wait_for_state "$TARGET_ONE_PORT" fault_after_import ready
source_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
wait_for_lsn "$TARGET_ONE_PORT" fault_after_import "$source_barrier"
[ "$(rows "$TARGET_ONE_PORT")" = "$(rows "$SOURCE_PORT")" ] || {
    echo "logical-replication-faults: expired snapshot did not reseed to source rows" >&2
    exit 1
}
[ "$(sql "$SOURCE_PORT" \
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'fault_after_import'")" = 1 ] || {
    echo "logical-replication-faults: expired snapshot reseed left an orphaned slot" >&2
    exit 1
}
stop_worker "$WORKER_ONE_PID"
WORKER_ONE_PID=

# Exercise the subscriber side of each initial-copy transition. A live exported
# token resumes partial copies; target row effects before chunk commit roll back,
# and ready progress appears only after all copy rows are durable.
exercise_worker_before_copying_progress_fault fault_before_copy_progress
exercise_copy_worker_fault after-copying-progress-commit \
    fault_after_copying_progress copying untouched
exercise_copy_worker_fault before-copy-reset \
    fault_before_copy_reset copying untouched
exercise_copy_worker_fault after-copy-reset \
    fault_after_copy_reset copying empty
exercise_copy_worker_fault before-copy-row-change \
    fault_before_copy_row copying empty
exercise_copy_worker_fault after-copy-row-change \
    fault_after_copy_row copying empty
exercise_copy_worker_fault before-copy-chunk-commit \
    fault_before_copy_commit copying empty
exercise_copy_worker_fault after-copy-snapshot-transaction-commit \
    fault_after_copy_snapshot_commit copying copied
exercise_copy_worker_fault before-copy-ready-commit \
    fault_before_copy_ready copying copied
exercise_copy_worker_fault after-copy-ready-commit \
    fault_after_copy_ready ready copied

echo "logical-replication-faults: PASS"
