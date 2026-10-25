#!/bin/sh
# A standard C-source subscription must survive extension bootstrap and DDL.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
: "${C_PG_BINDIR:?set C_PG_BINDIR to the unmodified C reference installation}"
PORT_BASE=${PORT_BASE:-5830}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
C_PORT=$((PORT_BASE + 2))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-mixed-mode.XXXXXX")
SOURCE_PID= TARGET_PID= WORKER_PID= STANDARD_PID=
C_STARTED=no
METADATA=pg_lisp_standard_subscriptions
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$WORKER_PID" "$STANDARD_PID"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    if [ "$C_STARTED" = yes ]; then
        "$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -m immediate -w stop >/dev/null 2>&1 || :
    fi
    for pid in "$SOURCE_PID" "$TARGET_PID"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    if [ "$status" -ne 0 ] || [ "${KEEP_ARTIFACTS:-0}" = 1 ]; then
        echo "logical-mixed-mode: artifacts $TMP" >&2
    else rm -rf "$TMP"; fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    "$C_PG_BINDIR/psql" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
wait_value() {
    wait_port=$1 wait_query=$2 wait_expected=$3
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        if value=$(sql "$wait_port" "$wait_query" 2>/dev/null) && [ "$value" = "$wait_expected" ]; then return; fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "logical-mixed-mode: timed out: $wait_query; expected $wait_expected, got $value" >&2
    return 1
}
start_extension() {
    SLOT=mixed_guard_$1
    WORKER_LOG=$TMP/$SLOT.log
    "$BASE/pg-lisp-subscribe" --slot "$SLOT" --source-port "$SOURCE_PORT" \
        --target-port "$TARGET_PORT" --table guarded_extension_rows >"$WORKER_LOG" 2>&1 &
    WORKER_PID=$!
    wait_value "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='$SLOT'" ready
    wait_value "$TARGET_PORT" 'SELECT amount FROM guarded_extension_rows WHERE id=1' 11
}
reject_ddl() {
    sql "$SOURCE_PORT" "$1" >/dev/null
    ddl_end=$(sql "$SOURCE_PORT" 'SELECT pg_current_wal_lsn()')
    attempt=0
    while ! grep -F "replicated DDL cannot target subscriber metadata relation" "$WORKER_LOG" >/dev/null; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 300 || { echo "missing DDL rejection: $1" >&2; return 1; }
        sleep 0.1
    done
    grep -F "$METADATA" "$WORKER_LOG" >/dev/null
    attempt=0
    while kill -0 "$WORKER_PID" 2>/dev/null; do
        case $(ps -o stat= -p "$WORKER_PID" 2>/dev/null || :) in Z*) break;; esac
        attempt=$((attempt + 1))
        test "$attempt" -lt 100 || { echo 'rejected worker did not exit' >&2; return 1; }
        sleep 0.1
    done
    if wait "$WORKER_PID"; then echo 'rejected worker exited successfully' >&2; return 1; fi
    WORKER_PID=
    progress=$(sql "$TARGET_PORT" "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot='$SLOT'")
    test "$progress" = "$(sql "$SOURCE_PORT" "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='$SLOT'")"
    test "$(sql "$SOURCE_PORT" "SELECT '$progress'::pg_lsn < '$ddl_end'::pg_lsn")" = t
    test "$(sql "$TARGET_PORT" "SELECT actual_slot FROM $METADATA WHERE subscription_key='mixed_standard' AND state='ready'")" = "$ACTUAL_SLOT"
    test "$(sql "$TARGET_PORT" "SELECT count(*) FROM pg_class WHERE relname='owned_standard_idx'")" = 1
    amount=$((amount + 1))
    sql "$C_PORT" 'UPDATE guarded_standard_rows SET amount=amount+1 WHERE id=1' >/dev/null
    wait_value "$TARGET_PORT" 'SELECT amount FROM guarded_standard_rows WHERE id=1' "$amount"
    echo "logical-mixed-mode: rejected $1; standard subscription kept applying"
}
"$C_PG_BINDIR/postgres" --version
"$C_PG_BINDIR/initdb" -D "$TMP/c-data" -U postgres -A trust --no-locale -E UTF8 >"$TMP/initdb.log"
printf '\nwal_level=logical\nmax_replication_slots=20\nmax_wal_senders=20\n' >>"$TMP/c-data/postgresql.conf"
C_STARTED=yes
"$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -l "$TMP/c.log" -o "-p $C_PORT -h 127.0.0.1 -k $TMP" -w start >"$TMP/pg_ctl.log"
"$BASE/pg-lisp.sh" -p "$SOURCE_PORT" -D "$TMP/source-data" >"$TMP/source.log" 2>&1 &
SOURCE_PID=$!
"$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/target-data" >"$TMP/target.log" 2>&1 &
TARGET_PID=$!
test_wait_owned_sql_server "$SOURCE_PID" "$SOURCE_PORT" "$C_PG_BINDIR/psql"
test_wait_owned_sql_server "$TARGET_PID" "$TARGET_PORT" "$C_PG_BINDIR/psql"
for port in "$SOURCE_PORT" "$TARGET_PORT" "$C_PORT"; do
    sql "$port" 'CREATE TABLE guarded_standard_rows (id integer PRIMARY KEY, amount bigint)' >/dev/null
done
sql "$C_PORT" 'INSERT INTO guarded_standard_rows VALUES (1,7); CREATE PUBLICATION mixed_pub FOR TABLE guarded_standard_rows' >/dev/null
"$BASE/pg-lisp-subscribe" --mode standard --publication mixed_pub --slot mixed_standard \
    --source-port "$C_PORT" --target-port "$TARGET_PORT" --table guarded_standard_rows >"$TMP/standard.log" 2>&1 &
STANDARD_PID=$!
wait_value "$TARGET_PORT" "SELECT state FROM $METADATA WHERE subscription_key='mixed_standard'" ready
wait_value "$TARGET_PORT" 'SELECT amount FROM guarded_standard_rows WHERE id=1' 7
ACTUAL_SLOT=$(sql "$TARGET_PORT" "SELECT actual_slot FROM $METADATA WHERE subscription_key='mixed_standard'")
amount=7
sql "$TARGET_PORT" "CREATE INDEX owned_standard_idx ON $METADATA (source_lsn)" >/dev/null
# The private publisher intentionally has an incompatible same-named table.
# Neither it nor its indexes belongs in the target's extension schema contract.
sql "$SOURCE_PORT" "CREATE TABLE guarded_extension_rows (id integer PRIMARY KEY, amount bigint); INSERT INTO guarded_extension_rows VALUES (1,11); CREATE TABLE $METADATA (subscription_key varchar PRIMARY KEY, source_lsn varchar, marker integer); CREATE INDEX owned_standard_idx ON $METADATA (source_lsn); CREATE INDEX publisher_standard_idx ON $METADATA (marker)" >/dev/null
start_extension bootstrap
test "$(sql "$TARGET_PORT" "SELECT count(*) FROM pg_class WHERE relname='publisher_standard_idx'")" = 0
sql "$SOURCE_PORT" 'UPDATE guarded_extension_rows SET amount=amount+1 WHERE id=1' >/dev/null
wait_value "$TARGET_PORT" 'SELECT amount FROM guarded_extension_rows WHERE id=1' 12
sql "$SOURCE_PORT" 'UPDATE guarded_extension_rows SET amount=amount-1 WHERE id=1' >/dev/null
wait_value "$TARGET_PORT" 'SELECT amount FROM guarded_extension_rows WHERE id=1' 11
reject_ddl 'DROP INDEX owned_standard_idx'
sql "$SOURCE_PORT" "CREATE INDEX owned_standard_idx ON $METADATA (source_lsn)" >/dev/null
start_extension rename
reject_ddl 'ALTER INDEX owned_standard_idx RENAME TO renamed_standard_idx'
test "$(sql "$TARGET_PORT" "SELECT count(*) FROM pg_class WHERE relname='renamed_standard_idx'")" = 0
start_extension create_index
reject_ddl "CREATE INDEX extra_standard_idx ON $METADATA (source_lsn)"
test "$(sql "$TARGET_PORT" "SELECT count(*) FROM pg_class WHERE relname='extra_standard_idx'")" = 0
start_extension alter_table
reject_ddl "ALTER TABLE $METADATA ADD UNIQUE (subscription_key,source_lsn)"
start_extension drop_table
reject_ddl "DROP TABLE $METADATA"
start_extension create_table
reject_ddl "CREATE TABLE IF NOT EXISTS $METADATA (id integer)"
echo 'logical-mixed-mode: PASS'
