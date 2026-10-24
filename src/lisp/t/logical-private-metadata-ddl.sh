#!/bin/sh
# Replicated DDL must not alter subscriber-owned progress or config tables.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT_BASE=${PORT_BASE:-5710}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-private-ddl.XXXXXX")
SOURCE_PID=
TARGET_PID=
WORKER_PID=

sql() {
    SQL_PORT=$1
    SQL_TEXT=$2
    psql -X -h 127.0.0.1 -p "$SQL_PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' -c "$SQL_TEXT"
}

cleanup() {
    for pid in "$WORKER_PID" "$TARGET_PID" "$SOURCE_PID"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$WORKER_PID" "$TARGET_PID" "$SOURCE_PID"; do
        if [ -n "$pid" ]; then
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

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
            echo "logical-private-metadata-ddl: server exited: $SERVER_LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-private-metadata-ddl: server did not start on port $SERVER_PORT" >&2
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
    SLOT_NAME=$1
    BARRIER=$(normalize_lsn "$2")
    i=0
    while [ "$i" -lt 600 ]; do
        current=$(sql "$TARGET_PORT" \
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
    echo "logical-private-metadata-ddl: slot $SLOT_NAME did not reach $BARRIER" >&2
    return 1
}

wait_for_ready() {
    SLOT_NAME=$1
    i=0
    while [ "$i" -lt 600 ]; do
        if state=$(sql "$TARGET_PORT" \
                "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME'" \
                2>/dev/null); then
            if [ "$state" = ready ]; then
                return 0
            fi
        fi
        if [ -n "$WORKER_PID" ] && ! kill -0 "$WORKER_PID" 2>/dev/null; then
            echo "logical-private-metadata-ddl: worker exited before slot $SLOT_NAME was ready" >&2
            return 1
        fi
        if [ -n "$WORKER_PID" ]; then
            state=$(ps -o stat= -p "$WORKER_PID" 2>/dev/null || true)
            case "$state" in
                Z*)
                    echo "logical-private-metadata-ddl: worker exited before slot $SLOT_NAME was ready" >&2
                    return 1
                    ;;
            esac
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-private-metadata-ddl: slot $SLOT_NAME did not become ready" >&2
    return 1
}

wait_for_slot_confirmation() {
    SLOT_NAME=$1
    BARRIER=$(normalize_lsn "$2")
    i=0
    while [ "$i" -lt 600 ]; do
        if current=$(sql "$SOURCE_PORT" \
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'" \
                2>/dev/null) && [ -n "$current" ]; then
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
    echo "logical-private-metadata-ddl: slot $SLOT_NAME did not confirm $BARRIER" >&2
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
    echo "logical-private-metadata-ddl: timed out waiting for '$LOG_TEXT'" >&2
    return 1
}

start_worker() {
    SLOT_NAME=$1
    WORKER_LOG=$2
    "$BASE/pg-lisp-subscribe" --slot "$SLOT_NAME" \
        --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
        --table logical_guard_rows > "$WORKER_LOG" 2>&1 &
    WORKER_PID=$!
    wait_for_ready "$SLOT_NAME"
    PROGRESS_LSN=$(sql "$TARGET_PORT" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME'")
    wait_for_slot_confirmation "$SLOT_NAME" "$PROGRESS_LSN"
    CONFIRMED_LSN=$(sql "$SOURCE_PORT" \
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'")
}

wait_for_worker_failure() {
    i=0
    while [ "$i" -lt 300 ]; do
        if ! kill -0 "$WORKER_PID" 2>/dev/null; then
            break
        fi
        state=$(ps -o stat= -p "$WORKER_PID" 2>/dev/null || true)
        case "$state" in
            Z*) break ;;
        esac
        i=$((i + 1))
        sleep 0.1
    done
    if [ "$i" -ge 300 ]; then
        echo "logical-private-metadata-ddl: rejected DDL did not stop the worker" >&2
        return 1
    fi
    if wait "$WORKER_PID"; then
        WORKER_STATUS=0
    else
        WORKER_STATUS=$?
    fi
    WORKER_PID=
    if [ "$WORKER_STATUS" -eq 0 ]; then
        echo "logical-private-metadata-ddl: worker succeeded after rejected DDL" >&2
        return 1
    fi
}

assert_rejected_ddl() {
    SLOT_NAME=$1
    TABLE_NAME=$2
    DDL=$3
    WORKER_LOG="$TMP/$SLOT_NAME.log"

    start_worker "$SLOT_NAME" "$WORKER_LOG"
    BARRIER=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
    wait_for_lsn_barrier "$SLOT_NAME" "$BARRIER"
    wait_for_slot_confirmation "$SLOT_NAME" "$BARRIER"
    sql "$SOURCE_PORT" "$DDL" >/dev/null
    DDL_BARRIER=$(sql "$SOURCE_PORT" "SELECT pg_current_wal_lsn()")
    wait_for_log "$WORKER_LOG" \
        "replicated DDL cannot target subscriber metadata relation"
    if ! grep -F "$TABLE_NAME" "$WORKER_LOG" >/dev/null; then
        echo "logical-private-metadata-ddl: guard named the wrong relation for $SLOT_NAME" >&2
        return 1
    fi
    wait_for_worker_failure

    PROGRESS_AFTER=$(sql "$TARGET_PORT" \
        "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME' AND state = 'ready'")
    CONFIRMED_AFTER=$(sql "$SOURCE_PORT" \
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'")
    if [ "$PROGRESS_AFTER" != "$CONFIRMED_AFTER" ]; then
        echo "logical-private-metadata-ddl: target progress and publisher confirmation disagree for $SLOT_NAME" >&2
        return 1
    fi
    PROGRESS_NORMALIZED=$(normalize_lsn "$PROGRESS_AFTER")
    DDL_NORMALIZED=$(normalize_lsn "$DDL_BARRIER")
    FIRST_LSN=$(printf '%s\n%s\n' "$PROGRESS_NORMALIZED" "$DDL_NORMALIZED" \
        | LC_ALL=C sort | sed -n '1p')
    if [ "$FIRST_LSN" != "$PROGRESS_NORMALIZED" ] \
            || [ "$PROGRESS_NORMALIZED" = "$DDL_NORMALIZED" ]; then
        echo "logical-private-metadata-ddl: rejection acknowledged through DDL end for $SLOT_NAME" >&2
        return 1
    fi
    if [ "$(sql "$TARGET_PORT" \
            "SELECT count(*) FROM pg_lisp_subscription_progress WHERE source_slot = '$SLOT_NAME'")" != 1 ] \
            || [ "$(sql "$TARGET_PORT" \
            "SELECT count(*) FROM pg_lisp_subscription_config WHERE source_slot = '$SLOT_NAME'")" != 1 ]; then
        echo "logical-private-metadata-ddl: rejected DDL damaged target metadata for $SLOT_NAME" >&2
        return 1
    fi
}

start_server "$SOURCE_PORT" "$TMP/source-data" "$TMP/source.log"
SOURCE_PID=$SERVER_PID
start_server "$TARGET_PORT" "$TMP/target-data" "$TMP/target.log"
TARGET_PID=$SERVER_PID
sql "$SOURCE_PORT" \
    "CREATE TABLE logical_guard_rows (id integer PRIMARY KEY, label varchar(64)); INSERT INTO logical_guard_rows VALUES (1, 'keep')" \
    >/dev/null

# CREATE must be rejected before CREATE TABLE IF NOT EXISTS can skip the target's
# subscriber-owned relation. The matching source shape lets the later DROP case
# start from a fresh slot with a schema that the ordinary bootstrap path accepts.
assert_rejected_ddl logical_create_progress pg_lisp_subscription_progress \
    "CREATE TABLE IF NOT EXISTS pg_lisp_subscription_progress (source_slot varchar PRIMARY KEY, source_lsn varchar, state varchar NOT NULL, snapshot_token varchar)"
assert_rejected_ddl logical_qualified_alter_progress pg_lisp_subscription_progress \
    "ALTER TABLE \"public.public.pg_lisp_subscription_progress\" ADD UNIQUE (source_slot, source_lsn)"
assert_rejected_ddl logical_drop_progress pg_lisp_subscription_progress \
    "DROP TABLE pg_lisp_subscription_progress"

assert_rejected_ddl logical_create_config pg_lisp_subscription_config \
    "CREATE TABLE IF NOT EXISTS pg_lisp_subscription_config (source_slot varchar PRIMARY KEY, source_host varchar NOT NULL, source_port integer NOT NULL, source_database varchar NOT NULL, table_allowlist varchar NOT NULL)"
assert_rejected_ddl logical_qualified_alter_config pg_lisp_subscription_config \
    "ALTER TABLE \"public.public.pg_lisp_subscription_config\" ADD UNIQUE (source_slot, source_host)"
assert_rejected_ddl logical_drop_config pg_lisp_subscription_config \
    "DROP TABLE pg_lisp_subscription_config"

echo "logical-private-metadata-ddl: PASS"
