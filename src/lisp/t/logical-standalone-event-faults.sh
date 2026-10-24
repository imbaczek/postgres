#!/bin/sh
# Crash windows around durable standalone DDL/sequence apply and feedback.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT_BASE=${PORT_BASE:-5660}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-event-faults.XXXXXX")
SOURCE_PID=
TARGET_PID=
WORKER_PID=
SOURCE_DATA=$TMP/source-data
SOURCE_LOG=$TMP/source.log
TARGET_DATA=
TARGET_LOG=

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
      if [ "$#" -eq 8 ]; then
          FAULT_POINT=$4
          FAULT_MARKER=$5
          FAULT_HIT=$6
          FAULT_LSN_AFTER=$7
          FAULT_GUARD_FILE=$8
          PG_LISP_TEST_FAILPOINT=$FAULT_POINT \
          PG_LISP_TEST_FAILPOINT_ACTION=crash \
          PG_LISP_TEST_FAILPOINT_ONCE_FILE=$FAULT_MARKER \
          PG_LISP_TEST_FAILPOINT_HIT=$FAULT_HIT \
          PG_LISP_TEST_FAILPOINT_LSN_AFTER=$FAULT_LSN_AFTER \
          PG_LISP_TEST_FAILPOINT_GUARD_FILE=$FAULT_GUARD_FILE \
              "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
              > "$SERVER_LOG" 2>&1 &
      elif [ "$#" -eq 7 ]; then
        FAULT_POINT=$4
        FAULT_MARKER=$5
        FAULT_HIT=$6
        FAULT_LSN_AFTER=$7
        PG_LISP_TEST_FAILPOINT=$FAULT_POINT \
        PG_LISP_TEST_FAILPOINT_ACTION=crash \
        PG_LISP_TEST_FAILPOINT_ONCE_FILE=$FAULT_MARKER \
        PG_LISP_TEST_FAILPOINT_HIT=$FAULT_HIT \
        PG_LISP_TEST_FAILPOINT_LSN_AFTER=$FAULT_LSN_AFTER \
            "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
            > "$SERVER_LOG" 2>&1 &
    elif [ "$#" -eq 6 ]; then
        FAULT_POINT=$4
        FAULT_MARKER=$5
        FAULT_HIT=$6
        PG_LISP_TEST_FAILPOINT=$FAULT_POINT \
        PG_LISP_TEST_FAILPOINT_ACTION=crash \
        PG_LISP_TEST_FAILPOINT_ONCE_FILE=$FAULT_MARKER \
        PG_LISP_TEST_FAILPOINT_HIT=$FAULT_HIT \
            "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
            > "$SERVER_LOG" 2>&1 &
    elif [ "$#" -eq 5 ]; then
        FAULT_POINT=$4
        FAULT_MARKER=$5
        PG_LISP_TEST_FAILPOINT=$FAULT_POINT \
        PG_LISP_TEST_FAILPOINT_ACTION=crash \
        PG_LISP_TEST_FAILPOINT_ONCE_FILE=$FAULT_MARKER \
            "$BASE/pg-lisp.sh" -p "$SERVER_PORT" -D "$SERVER_DATA" \
            > "$SERVER_LOG" 2>&1 &
      elif [ "$#" -eq 4 ]; then
          SCAN_BUDGET_BYTES=$4
          PG_LISP_TEST_LOGICAL_SCAN_BYTES=$SCAN_BUDGET_BYTES \
          PG_LISP_TEST_FAILPOINT=after-scan-progress-before-feedback \
          PG_LISP_TEST_FAILPOINT_ACTION=crash \
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
            echo "logical-standalone-event-faults: server exited during startup: $SERVER_LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-standalone-event-faults: server did not start on port $SERVER_PORT" >&2
    return 1
}

restart_target() {
    kill -KILL "$TARGET_PID" 2>/dev/null || true
    wait "$TARGET_PID" 2>/dev/null || true
    TARGET_PID=
    fault_point=${1-}
    fault_marker=${2-}
    restart_log=${3:-$TARGET_LOG}
    if [ -n "$fault_point" ]; then
        start_server "$TARGET_PORT" "$TARGET_DATA" "$restart_log" \
            "$fault_point" "$fault_marker"
    else
        start_server "$TARGET_PORT" "$TARGET_DATA" "$restart_log"
    fi
    TARGET_PID=$SERVER_PID
    TARGET_LOG=$restart_log
}

restart_source() {
    fault_point=${1-}
    fault_marker=${2-}
    fault_hit=${3-}
    fault_lsn_after=${4-}
    restart_log=${5:-$SOURCE_LOG}
      fault_guard_file=${6-}
    kill -KILL "$SOURCE_PID" 2>/dev/null || true
    wait "$SOURCE_PID" 2>/dev/null || true
    SOURCE_PID=
      if [ -n "$fault_point" ] && [ -n "$fault_hit" ] \
              && [ -n "$fault_lsn_after" ] && [ -n "$fault_guard_file" ]; then
          start_server "$SOURCE_PORT" "$SOURCE_DATA" "$restart_log" \
              "$fault_point" "$fault_marker" "$fault_hit" \
              "$fault_lsn_after" "$fault_guard_file"
      elif [ -n "$fault_point" ] && [ -n "$fault_hit" ] \
            && [ -n "$fault_lsn_after" ]; then
        start_server "$SOURCE_PORT" "$SOURCE_DATA" "$restart_log" \
            "$fault_point" "$fault_marker" "$fault_hit" "$fault_lsn_after"
    elif [ -n "$fault_point" ] && [ -n "$fault_hit" ]; then
        start_server "$SOURCE_PORT" "$SOURCE_DATA" "$restart_log" \
            "$fault_point" "$fault_marker" "$fault_hit"
    elif [ -n "$fault_point" ]; then
        start_server "$SOURCE_PORT" "$SOURCE_DATA" "$restart_log" \
            "$fault_point" "$fault_marker"
    else
        start_server "$SOURCE_PORT" "$SOURCE_DATA" "$restart_log"
    fi
    SOURCE_PID=$SERVER_PID
    SOURCE_LOG=$restart_log
}

  restart_source_with_scan_budget() {
      scan_budget_bytes=$1
      kill -KILL "$SOURCE_PID" 2>/dev/null || true
      wait "$SOURCE_PID" 2>/dev/null || true
      SOURCE_PID=
      start_server "$SOURCE_PORT" "$SOURCE_DATA" \
          "$TMP/scan-budget-source.log" "$scan_budget_bytes"
      SOURCE_PID=$SERVER_PID
      SOURCE_LOG=$TMP/scan-budget-source.log
  }

wait_for_file() {
    FILE_PATH=$1
    i=0
    while [ "$i" -lt 300 ]; do
        [ -f "$FILE_PATH" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-standalone-event-faults: timed out waiting for $FILE_PATH" >&2
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
    echo "logical-standalone-event-faults: timed out waiting for '$LOG_TEXT' in $LOG_FILE" >&2
    return 1
}

normalize_lsn() {
    LSN_HIGH=${1%/*}
    LSN_LOW=${1#*/}
    LSN_HIGH=$(printf '%8s' "$LSN_HIGH" | tr ' ' '0')
    LSN_LOW=$(printf '%8s' "$LSN_LOW" | tr ' ' '0')
    printf '%s/%s' "$LSN_HIGH" "$LSN_LOW"
}

lsn_is_after() {
    CANDIDATE=$(normalize_lsn "$1")
    PREVIOUS=$(normalize_lsn "$2")
    [ "$CANDIDATE" != "$PREVIOUS" ] || return 1
    first=$(printf '%s\n%s\n' "$CANDIDATE" "$PREVIOUS" | LC_ALL=C sort | sed -n '1p')
    [ "$first" = "$PREVIOUS" ]
}

lsn_at_least() {
    [ "$(normalize_lsn "$1")" = "$(normalize_lsn "$2")" ] \
        || lsn_is_after "$1" "$2"
}

wait_for_progress_advance() {
    SQL_PORT=$1
    SLOT_NAME=$2
    PREVIOUS=$(normalize_lsn "$3")
    i=0
    while [ "$i" -lt 1800 ]; do
        current=$(sql "$SQL_PORT" \
            "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME' AND state = 'ready'")
        if [ -n "$current" ]; then
            current=$(normalize_lsn "$current")
            if lsn_is_after "$current" "$PREVIOUS"; then
                printf '%s' "$current"
                return 0
            fi
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-standalone-event-faults: target progress did not advance past $PREVIOUS" >&2
    return 1
}

wait_for_slot_confirmation() {
    SLOT_NAME=$1
    BARRIER=$(normalize_lsn "$2")
    i=0
    while [ "$i" -lt 1800 ]; do
        current=$(sql "$SOURCE_PORT" \
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'")
        if [ -n "$current" ]; then
            current=$(normalize_lsn "$current")
            first=$(printf '%s\n%s\n' "$BARRIER" "$current" | LC_ALL=C sort | sed -n '1p')
            [ "$first" = "$BARRIER" ] && return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-standalone-event-faults: slot did not confirm source LSN $BARRIER" >&2
    return 1
}

stop_worker() {
    [ -n "$WORKER_PID" ] || return 0
    kill -KILL "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
    WORKER_PID=
}

start_worker() {
    WORKER_LOG=$1
    shift
    if [ "$#" -eq 2 ]; then
        FAILPOINT=$1
        MARKER=$2
        PG_LISP_TEST_FAILPOINT=$FAILPOINT \
        PG_LISP_TEST_FAILPOINT_ACTION=crash \
        PG_LISP_TEST_FAILPOINT_ONCE_FILE=$MARKER \
            "$BASE/pg-lisp-subscribe" --slot standalone_event_slot \
            --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
            --table fault_rows > "$WORKER_LOG" 2>&1 &
    else
        "$BASE/pg-lisp-subscribe" --slot standalone_event_slot \
            --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
            --table fault_rows > "$WORKER_LOG" 2>&1 &
    fi
    WORKER_PID=$!
}

start_paused_worker() {
    WORKER_LOG=$1
    PAUSE_MARKER=$2
    RELEASE_FILE=$3
    PG_LISP_TEST_FAILPOINT=after-target-commit-before-feedback \
    PG_LISP_TEST_FAILPOINT_ACTION=pause \
    PG_LISP_TEST_FAILPOINT_ONCE_FILE=$PAUSE_MARKER \
    PG_LISP_TEST_FAILPOINT_RELEASE_FILE=$RELEASE_FILE \
        "$BASE/pg-lisp-subscribe" --slot standalone_event_slot \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
        --table fault_rows > "$WORKER_LOG" 2>&1 &
    WORKER_PID=$!
}

start_paused_event_apply_worker() {
    WORKER_LOG=$1
    PAUSE_MARKER=$2
    RELEASE_FILE=$3
    PG_LISP_TEST_FAILPOINT=after-standalone-target-apply-before-progress \
    PG_LISP_TEST_FAILPOINT_ACTION=pause \
    PG_LISP_TEST_FAILPOINT_ONCE_FILE=$PAUSE_MARKER \
    PG_LISP_TEST_FAILPOINT_RELEASE_FILE=$RELEASE_FILE \
        "$BASE/pg-lisp-subscribe" --slot standalone_event_slot \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
        --table fault_rows > "$WORKER_LOG" 2>&1 &
    WORKER_PID=$!
}

  start_scan_failpoint_worker() {
      WORKER_LOG=$1
      MARKER=$2
      PG_LISP_TEST_FAILPOINT=after-scan-progress-before-feedback \
      PG_LISP_TEST_FAILPOINT_ACTION=crash \
      PG_LISP_TEST_FAILPOINT_ONCE_FILE=$MARKER \
          "$BASE/pg-lisp-subscribe" --slot standalone_event_slot \
          --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
          --table fault_rows > "$WORKER_LOG" 2>&1 &
      WORKER_PID=$!
  }

target_progress() {
    sql "$TARGET_PORT" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'standalone_event_slot' AND state = 'ready'"
}

slot_confirmation() {
    sql "$SOURCE_PORT" \
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'standalone_event_slot'"
}

wait_for_stable_progress() {
    previous=
    stable_index=0
    while [ "$stable_index" -lt 300 ]; do
        candidate=$(target_progress)
        if [ -n "$candidate" ] && [ "$candidate" = "$previous" ]; then
              confirmation=$(slot_confirmation)
            after_wait=$(target_progress)
              after_confirmation=$(slot_confirmation)
              if [ "$confirmation" = "$candidate" ] \
                      && [ "$after_wait" = "$candidate" ] \
                      && [ "$after_confirmation" = "$candidate" ]; then
                printf '%s' "$candidate"
                return 0
            fi
            previous=
        else
            previous=$candidate
        fi
        stable_index=$((stable_index + 1))
        sleep 0.1
    done
    echo "logical-standalone-event-faults: target progress did not stabilize (target=$candidate previous=$previous)" >&2
    return 1
}

exercise_catalog_terminal_fault() {
    fault_point=$1
    object_name=$2
    terminal_durable=$3
    event_kind=${4:-ddl}
    marker=$TMP/$fault_point-$object_name-$event_kind.marker
    worker_log=$TMP/$fault_point-worker.log
    retry_log=$TMP/$fault_point-retry.log
      progress_before=$(wait_for_stable_progress)
    confirmed_before=$(slot_confirmation)
    if [ "$event_kind" = sequence ]; then
        sequence_before=$(sql "$TARGET_PORT" \
            "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')
    fi
    stop_worker

    restart_target "$fault_point" "$marker" "$TMP/$fault_point-target.log"
    faulted_target_pid=$TARGET_PID
    start_worker "$worker_log"
    wait_for_log "$worker_log" "streaming from"
    progress_before=$(wait_for_stable_progress)
    confirmed_before=$(slot_confirmation)
      source_lsn_before_event=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
    if [ "$event_kind" = sequence ]; then
        sql "$SOURCE_PORT" "SELECT nextval('fault_counter')" >/dev/null
        source_sequence_after=$(sql "$SOURCE_PORT" \
            "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')
    else
        sql "$SOURCE_PORT" "CREATE TABLE $object_name (id integer PRIMARY KEY)" >/dev/null
    fi
    wait_for_file "$marker"
    if wait "$faulted_target_pid"; then
        echo "logical-standalone-event-faults: $fault_point did not crash the target" >&2
        exit 1
    else
        target_status=$?
    fi
    [ "$target_status" -eq 137 ] || {
        echo "logical-standalone-event-faults: expected target SIGKILL 137, got $target_status" >&2
        exit 1
    }
    TARGET_PID=
    stop_worker

    restart_target "" "" "$TMP/$fault_point-recovery.log"
    progress_after=$(target_progress)
      lsn_at_least "$source_lsn_before_event" "$progress_after" || {
          echo "logical-standalone-event-faults: catalog terminal fault advanced target progress past the event boundary (before=$source_lsn_before_event progress=$progress_after; stable-before=$progress_before)" >&2
        exit 1
    }
      confirmation_after=$(slot_confirmation)
      lsn_at_least "$source_lsn_before_event" "$confirmation_after" || {
          echo "logical-standalone-event-faults: catalog terminal fault advanced publisher feedback past the event boundary (before=$source_lsn_before_event confirmation=$confirmation_after; prior=$confirmed_before)" >&2
        exit 1
    }
    if [ "$event_kind" = sequence ]; then
        sequence_after_restart=$(sql "$TARGET_PORT" \
            "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')
        if [ "$terminal_durable" = yes ]; then
            [ "$sequence_after_restart" = "$source_sequence_after" ] || {
                echo "logical-standalone-event-faults: durable sequence event did not recover" >&2
                exit 1
            }
        else
            [ "$sequence_after_restart" = "$sequence_before" ] || {
                echo "logical-standalone-event-faults: pre-terminal sequence mutation survived restart" >&2
                exit 1
            }
        fi
    elif [ "$terminal_durable" = yes ]; then
        [ "$(sql "$TARGET_PORT" "SELECT count(*) FROM $object_name")" = 0 ] || {
            echo "logical-standalone-event-faults: durable catalog terminal did not recover" >&2
            exit 1
        }
    elif sql "$TARGET_PORT" "SELECT count(*) FROM $object_name" >/dev/null 2>&1; then
        echo "logical-standalone-event-faults: pre-terminal catalog mutation survived restart" >&2
        exit 1
    fi

    start_worker "$retry_log"
    wait_for_log "$retry_log" "streaming from"
    event_progress=$(wait_for_progress_advance \
                     "$TARGET_PORT" standalone_event_slot "$progress_before")
    wait_for_slot_confirmation standalone_event_slot "$event_progress"
    if [ "$event_kind" = sequence ]; then
        [ "$(sql "$TARGET_PORT" \
                "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" \
            = "$source_sequence_after" ] || {
            echo "logical-standalone-event-faults: sequence state did not apply after retry" >&2
            exit 1
        }
    else
        [ "$(sql "$TARGET_PORT" "SELECT count(*) FROM $object_name")" = 0 ] || {
            echo "logical-standalone-event-faults: catalog DDL did not apply after retry" >&2
            exit 1
        }
    fi
}

exercise_slot_acknowledgment_fault() {
    fault_point=$1
    row_id=$2
    row_label=$3
    expected_ack=$4
    marker=$TMP/$fault_point-source.marker
    progress_before=$(target_progress)
    wait_for_slot_confirmation standalone_event_slot "$progress_before"
    confirmed_before=$(slot_confirmation)
    stop_worker

    restart_source "$fault_point" "$marker" 1 "$confirmed_before" \
          "$TMP/$fault_point-source.log" "$TMP/$fault_point-target.marker"
    start_paused_worker "$TMP/$fault_point-worker.log" \
        "$TMP/$fault_point-target.marker" "$TMP/$fault_point-release"
    wait_for_log "$TMP/$fault_point-worker.log" "streaming from"
    sql "$SOURCE_PORT" \
        "INSERT INTO fault_rows VALUES ($row_id, '$row_label')" >/dev/null
    wait_for_file "$TMP/$fault_point-target.marker"
    progress_after=$(target_progress)
    lsn_is_after "$progress_after" "$progress_before" || {
        echo "logical-standalone-event-faults: target row commit did not advance its marker" >&2
        exit 1
    }
    [ "$(sql "$TARGET_PORT" "SELECT count(*) FROM fault_rows WHERE id = $row_id")" = 1 ] || {
        echo "logical-standalone-event-faults: row was not durable before publisher feedback" >&2
        exit 1
    }
    : > "$TMP/$fault_point-release"
    wait_for_file "$marker"
    if wait "$SOURCE_PID"; then
        echo "logical-standalone-event-faults: $fault_point did not crash the publisher" >&2
        exit 1
    else
        source_status=$?
    fi
    [ "$source_status" -eq 137 ] || {
        echo "logical-standalone-event-faults: expected publisher SIGKILL 137, got $source_status" >&2
        exit 1
    }
    SOURCE_PID=
    stop_worker
    restart_source "" "" "" "" "$TMP/$fault_point-source-recovery.log"

    confirmation_after_fault=$(slot_confirmation)
    if [ "$expected_ack" = before ]; then
        lsn_at_least "$confirmation_after_fault" "$confirmed_before" || {
            echo "logical-standalone-event-faults: pre-ack crash moved the slot backward (before=$confirmed_before after=$confirmation_after_fault)" >&2
            exit 1
        }
        lsn_is_after "$progress_after" "$confirmation_after_fault" || {
            echo "logical-standalone-event-faults: pre-ack crash persisted the row commit (confirmation=$confirmation_after_fault row-commit=$progress_after)" >&2
            exit 1
        }
    else
        lsn_at_least "$confirmation_after_fault" "$progress_after" || {
            echo "logical-standalone-event-faults: post-ack crash lost the row commit acknowledgment (before=$confirmed_before after=$confirmation_after_fault row-commit=$progress_after)" >&2
            exit 1
        }
        lsn_at_least "$progress_after" "$confirmation_after_fault" || {
            echo "logical-standalone-event-faults: publisher acknowledgment passed the durable row marker (confirmation=$confirmation_after_fault row-commit=$progress_after)" >&2
            exit 1
        }
    fi

    start_worker "$TMP/$fault_point-worker-restart.log"
    wait_for_log "$TMP/$fault_point-worker-restart.log" "streaming from"
    wait_for_slot_confirmation standalone_event_slot "$progress_after"
    [ "$(sql "$TARGET_PORT" "SELECT count(*) FROM fault_rows WHERE id = $row_id")" = 1 ] || {
        echo "logical-standalone-event-faults: reconnect duplicated or lost the row" >&2
        exit 1
    }
}

cleanup() {
    for pid in "$WORKER_PID" "$SOURCE_PID" "$TARGET_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    for pid in "$WORKER_PID" "$SOURCE_PID" "$TARGET_PID"; do
        [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMP"
}

on_exit() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        echo "logical-standalone-event-faults: failure; logs follow" >&2
        for log in "$TMP"/*.log; do
            [ -f "$log" ] || continue
            echo "--- $log" >&2
            cat "$log" >&2
        done
    fi
    cleanup
    exit "$status"
}

trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

SOURCE_DATA=$TMP/source-data
SOURCE_LOG=$TMP/source.log
TARGET_DATA=$TMP/target-data
TARGET_LOG=$TMP/target.log
start_server "$SOURCE_PORT" "$SOURCE_DATA" "$SOURCE_LOG"
SOURCE_PID=$SERVER_PID
start_server "$TARGET_PORT" "$TARGET_DATA" "$TARGET_LOG"
TARGET_PID=$SERVER_PID
sql "$SOURCE_PORT" \
    "CREATE TABLE fault_rows (id integer PRIMARY KEY, label varchar(64)); CREATE SEQUENCE fault_counter START WITH 10 INCREMENT BY 3; INSERT INTO fault_rows VALUES (1, 'seed')" \
    >/dev/null
start_worker "$TMP/worker-initial.log"
wait_for_log "$TMP/worker-initial.log" "streaming from"
progress_before=$(sql "$TARGET_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'standalone_event_slot' AND state = 'ready'")
wait_for_slot_confirmation standalone_event_slot "$progress_before"
stop_worker

# Pause after the target catalog terminal and its event high-water commit, while
# the separate subscriber-progress update and publisher feedback are still ahead.
start_paused_event_apply_worker "$TMP/worker-ddl-apply-fault.log" \
    "$TMP/ddl-apply.marker" "$TMP/ddl-apply-release"
wait_for_log "$TMP/worker-ddl-apply-fault.log" "streaming from"
progress_before=$(wait_for_stable_progress)
sql "$SOURCE_PORT" "CREATE TABLE applied_before_progress (id integer PRIMARY KEY)" >/dev/null
wait_for_file "$TMP/ddl-apply.marker"
progress_while_paused=$(target_progress)
confirmed_while_paused=$(slot_confirmation)
if [ "$progress_while_paused" != "$progress_before" ] \
        && ! lsn_is_after "$progress_while_paused" "$progress_before"; then
    echo "logical-standalone-event-faults: safe progress moved backward before DDL apply" >&2
    exit 1
fi
[ "$progress_while_paused" = "$confirmed_while_paused" ] || {
    echo "logical-standalone-event-faults: publisher feedback passed the durable target marker" >&2
    exit 1
}
[ "$(sql "$TARGET_PORT" "SELECT count(*) FROM applied_before_progress")" = 0 ] || {
    echo "logical-standalone-event-faults: DDL catalog terminal was not durable before progress" >&2
    exit 1
}
faulted_worker_pid=$WORKER_PID
kill -KILL "$faulted_worker_pid" 2>/dev/null || true
if wait "$faulted_worker_pid"; then
    echo "logical-standalone-event-faults: paused apply worker did not crash" >&2
    exit 1
else
    worker_status=$?
fi
[ "$worker_status" -eq 137 ] || {
    echo "logical-standalone-event-faults: expected paused apply worker SIGKILL 137, got $worker_status" >&2
    exit 1
}
WORKER_PID=
restart_target
progress_after=$(sql "$TARGET_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'standalone_event_slot' AND state = 'ready'")
[ "$progress_after" = "$progress_while_paused" ] || {
    echo "expected target progress $progress_while_paused, got $progress_after" >&2
    echo "logical-standalone-event-faults: target progress changed while recovering the apply/progress gap" >&2
    exit 1
}
[ "$(sql "$TARGET_PORT" "SELECT count(*) FROM applied_before_progress")" = 0 ] || {
    echo "logical-standalone-event-faults: committed DDL did not survive the apply/progress gap" >&2
    exit 1
}
[ "$(sql "$SOURCE_PORT" \
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'standalone_event_slot'")" = "$confirmed_while_paused" ] || {
    echo "logical-standalone-event-faults: publisher acknowledged DDL before target progress" >&2
    exit 1
}
start_worker "$TMP/worker-ddl-retry.log"
wait_for_log "$TMP/worker-ddl-retry.log" "streaming from"
ddl_progress=$(wait_for_progress_advance \
    "$TARGET_PORT" standalone_event_slot "$progress_while_paused")
wait_for_slot_confirmation standalone_event_slot "$ddl_progress"

# Crash after sequence state and its target progress row commit, but before
# feedback. Reconnect starts from that marker, so the sequence event is not
# replayed and publisher confirmation catches up from the durable target state.
progress_before=$(sql "$TARGET_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'standalone_event_slot' AND state = 'ready'")
wait_for_slot_confirmation standalone_event_slot "$progress_before"
stop_worker
confirmed_before=$(sql "$SOURCE_PORT" \
    "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'standalone_event_slot'")
PG_LISP_TEST_FAILPOINT=after-standalone-progress-before-feedback \
PG_LISP_TEST_FAILPOINT_ACTION=crash \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/sequence-progress.marker" \
    start_worker "$TMP/worker-sequence-progress-fault.log" \
        after-standalone-progress-before-feedback "$TMP/sequence-progress.marker"
wait_for_log "$TMP/worker-sequence-progress-fault.log" "streaming from"
sql "$SOURCE_PORT" "SELECT nextval('fault_counter')" >/dev/null
wait_for_file "$TMP/sequence-progress.marker"
if wait "$WORKER_PID"; then
    echo "logical-standalone-event-faults: progress-before-feedback failpoint did not crash" >&2
    exit 1
else
    worker_status=$?
fi
[ "$worker_status" -eq 137 ] || {
    echo "logical-standalone-event-faults: expected progress-before-feedback SIGKILL 137, got $worker_status" >&2
    exit 1
}
WORKER_PID=
restart_target
progress_after=$(sql "$TARGET_PORT" \
    "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = 'standalone_event_slot' AND state = 'ready'")
lsn_is_after "$progress_after" "$progress_before" || {
    echo "logical-standalone-event-faults: sequence progress was not durable before feedback" >&2
    exit 1
}
# OIDs are local to each catalog; compare the replicated definition and state.
sequence_after_apply=$(sql "$TARGET_PORT" \
    "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')
source_sequence_state=$(sql "$SOURCE_PORT" \
    "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')
[ "$sequence_after_apply" = "$source_sequence_state" ] || {
    echo "logical-standalone-event-faults: sequence event did not commit with target progress" >&2
    exit 1
}
  confirmed_after_fault=$(sql "$SOURCE_PORT" \
      "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'standalone_event_slot'")
  lsn_is_after "$progress_after" "$confirmed_after_fault" || {
      echo "logical-standalone-event-faults: publisher acknowledged the sequence event before feedback (progress=$progress_after confirmation=$confirmed_after_fault; before=$confirmed_before)" >&2
      exit 1
  }
start_worker "$TMP/worker-sequence-restart.log"
wait_for_log "$TMP/worker-sequence-restart.log" "streaming from"
wait_for_slot_confirmation standalone_event_slot "$progress_after"
[ "$(sql "$TARGET_PORT" "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" = "$sequence_after_apply" ] || {
    echo "logical-standalone-event-faults: reconnect changed an already applied sequence state" >&2
    exit 1
}

# A sequence-state Message carries schema identity as well as its nontransactional
# value. Keep the worker connected while target drift is introduced so the event
# apply path itself must reject it, matching strict table/index DDL handling.
  progress_before_drift=$(wait_for_stable_progress)
confirmed_before_drift=$(slot_confirmation)
sql "$TARGET_PORT" "ALTER SEQUENCE fault_counter INCREMENT BY 7" >/dev/null
target_drifted_state=$(sql "$TARGET_PORT" \
    "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')
  # Capture the writer position before nextval emits its sequence-state event.
  # pg_current_wal_lsn() reports this boundary in xlogfuncs.c.
  source_lsn_before_event=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
sql "$SOURCE_PORT" "SELECT nextval('fault_counter')" >/dev/null
wait_for_log "$TMP/worker-sequence-restart.log" \
    "definition does not match publisher"
if wait "$WORKER_PID"; then
    echo "logical-standalone-event-faults: sequence-state apply accepted target definition drift" >&2
    exit 1
else
    worker_status=$?
fi
[ "$worker_status" -ne 0 ] || {
    echo "logical-standalone-event-faults: drifted sequence worker exited successfully" >&2
    exit 1
}
WORKER_PID=
  progress_after_rejection=$(target_progress)
  lsn_at_least "$source_lsn_before_event" "$progress_after_rejection" || {
      echo "logical-standalone-event-faults: rejected sequence event advanced target progress (before-event=$source_lsn_before_event progress=$progress_after_rejection; stable-before=$progress_before_drift)" >&2
    exit 1
}
  confirmation_after_rejection=$(slot_confirmation)
  lsn_at_least "$source_lsn_before_event" "$confirmation_after_rejection" || {
      echo "logical-standalone-event-faults: publisher acknowledged a rejected sequence event (before-event=$source_lsn_before_event confirmation=$confirmation_after_rejection; stable-before=$confirmed_before_drift)" >&2
    exit 1
}
[ "$(sql "$TARGET_PORT" \
        "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" \
    = "$target_drifted_state" ] || {
    echo "logical-standalone-event-faults: rejected sequence event changed target schema or value" >&2
    exit 1
}

# Repair the exact source definition; the unchanged slot marker must replay the
# pending event and apply only its value/called state.
sql "$TARGET_PORT" "ALTER SEQUENCE fault_counter INCREMENT BY 3" >/dev/null
start_worker "$TMP/worker-sequence-drift-retry.log"
wait_for_log "$TMP/worker-sequence-drift-retry.log" "streaming from"
drift_retry_progress=$(wait_for_progress_advance \
    "$TARGET_PORT" standalone_event_slot "$progress_before_drift")
wait_for_slot_confirmation standalone_event_slot "$drift_retry_progress"
[ "$(sql "$TARGET_PORT" \
        "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" \
    = "$(sql "$SOURCE_PORT" \
        "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" ] || {
    echo "logical-standalone-event-faults: repaired sequence did not replay the pending event" >&2
    exit 1
}

# Crash the target on both sides of the catalog WAL terminal. The pre-terminal
# case must redo neither schema nor sequence state; the post-terminal case must
# recover the catalog image and its event high-water before retry advances
# subscription progress. See snapmgr.c's snapshot boundary and logical.c's
# LogicalConfirmReceivedLocation feedback ordering.
exercise_catalog_terminal_fault \
    before-catalog-event-wal-terminal catalog_before_terminal no
exercise_catalog_terminal_fault \
    after-catalog-event-wal-terminal catalog_after_terminal yes
exercise_catalog_terminal_fault \
    before-catalog-event-wal-terminal ignored_before_terminal no sequence
exercise_catalog_terminal_fault \
    after-catalog-event-wal-terminal ignored_after_terminal yes sequence

# Kill the publisher immediately before and after the slot file is made durable.
# A replay after the pre-ack crash uses the target marker to avoid duplicate
# application; a post-ack restart must retain its durable confirmation.
exercise_slot_acknowledgment_fault \
    before-slot-acknowledgment 101 before-ack before
exercise_slot_acknowledgment_fault \
    after-slot-acknowledgment 102 after-ack after

# The safe scan frontier is durable on the target before the publisher receives
# feedback. A narrow test-only reader budget makes a real CHECKPOINT hit this
# boundary without constructing 16 MiB of unrelated WAL.
stop_worker
restart_source_with_scan_budget 1
start_worker "$TMP/scan-budget-drain.log"
wait_for_log "$TMP/scan-budget-drain.log" "streaming from"
progress_before_scan=$(wait_for_stable_progress)
confirmed_before_scan=$(slot_confirmation)
stop_worker
start_scan_failpoint_worker "$TMP/scan-progress-fault.log" \
    "$TMP/scan-progress.marker"
wait_for_log "$TMP/scan-progress-fault.log" "streaming from"
sql "$SOURCE_PORT" "CHECKPOINT" >/dev/null
wait_for_file "$TMP/scan-progress.marker"
if wait "$WORKER_PID"; then
    echo "logical-standalone-event-faults: safe-progress crash failpoint did not fire" >&2
    exit 1
else
    worker_status=$?
fi
[ "$worker_status" -eq 137 ] || {
    echo "logical-standalone-event-faults: expected safe-progress SIGKILL 137, got $worker_status" >&2
    exit 1
}
WORKER_PID=
progress_after_scan=$(target_progress)
lsn_is_after "$progress_after_scan" "$progress_before_scan" || {
    echo "logical-standalone-event-faults: safe scan did not durably advance target progress" >&2
    exit 1
}
confirmation_after_scan=$(slot_confirmation)
lsn_at_least "$confirmation_after_scan" "$confirmed_before_scan" || {
    echo "logical-standalone-event-faults: safe-scan crash moved publisher acknowledgment backward (before=$confirmed_before_scan after=$confirmation_after_scan)" >&2
    exit 1
}
lsn_is_after "$progress_after_scan" "$confirmation_after_scan" || {
    echo "logical-standalone-event-faults: publisher acknowledged the crashed safe-scan frontier (confirmation=$confirmation_after_scan target=$progress_after_scan)" >&2
    exit 1
}
restart_target
[ "$(target_progress)" = "$progress_after_scan" ] || {
    echo "logical-standalone-event-faults: safe scan progress did not survive target restart" >&2
    exit 1
}
confirmation_after_target_restart=$(slot_confirmation)
lsn_at_least "$confirmation_after_target_restart" "$confirmation_after_scan" || {
    echo "logical-standalone-event-faults: target restart moved publisher acknowledgment backward" >&2
    exit 1
}
lsn_is_after "$progress_after_scan" "$confirmation_after_target_restart" || {
    echo "logical-standalone-event-faults: target restart acknowledged the crashed safe-scan frontier (confirmation=$confirmation_after_target_restart target=$progress_after_scan)" >&2
    exit 1
}
sql "$SOURCE_PORT" "SELECT nextval('fault_counter')" >/dev/null
scan_retry_barrier=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
start_worker "$TMP/scan-progress-retry.log"
wait_for_log "$TMP/scan-progress-retry.log" "streaming from $progress_after_scan"
wait_for_slot_confirmation standalone_event_slot "$scan_retry_barrier"
[ "$(sql "$TARGET_PORT" \
        "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" \
    = "$(sql "$SOURCE_PORT" \
        "SELECT pg_lisp_sequence_state('fault_counter')" | sed -E 's/:OID [0-9]+//')" ] || {
    echo "logical-standalone-event-faults: pending sequence event did not replay after safe progress" >&2
    exit 1
}

echo "logical-standalone-event-faults: PASS"
