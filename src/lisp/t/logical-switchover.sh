#!/bin/sh
# Isolated, operator-ordered logical writer switchover rehearsal; no coordinator.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
DIRECTION=${1:-all}
case "$DIRECTION" in
    all)
        result=0
        for direction in lisp-to-lisp lisp-to-c c-to-lisp; do
            "$0" "$direction" || result=1
        done
        if [ "$result" -eq 0 ]; then echo 'logical-switchover: all PASS'; fi
        exit "$result";;
    lisp-to-lisp) SOURCE_KIND=lisp; TARGET_KIND=lisp;;
    lisp-to-c) SOURCE_KIND=lisp; TARGET_KIND=c;;
    c-to-lisp) SOURCE_KIND=c; TARGET_KIND=lisp;;
    *) echo "usage: $0 [all|lisp-to-lisp|lisp-to-c|c-to-lisp]" >&2; exit 2;;
esac
if [ "$SOURCE_KIND" = c ] || [ "$TARGET_KIND" = c ]; then
    : "${C_PG_BINDIR:?set C_PG_BINDIR to the reference C installation bin directory}"
    : "${C_PG_REVISION:?set C_PG_REVISION to the exact reference build revision}"
    for program in postgres initdb pg_ctl psql; do test -x "$C_PG_BINDIR/$program"; done
    PSQL=$C_PG_BINDIR/psql
    echo "logical-switchover: C revision=$C_PG_REVISION version=$("$C_PG_BINDIR/postgres" --version)"
else PSQL=${PSQL:-psql}; fi
case "${PAUSE_AFTER_SWITCHOVER:-0}" in
    0) ;; 1) test -t 0 || { echo 'pause requires an interactive terminal' >&2; exit 2; };;
    *) echo 'PAUSE_AFTER_SWITCHOVER must be 0 or 1' >&2; exit 2;;
esac
unset PG_LISP_TEST_FAILPOINT PG_LISP_TEST_FAILPOINT_OBJECT PG_LISP_TEST_FAILPOINT_ACTION PG_LISP_TEST_FAILPOINT_GUARD_FILE PG_LISP_TEST_FAILPOINT_LSN_AFTER PG_LISP_TEST_FAILPOINT_HIT PG_LISP_TEST_FAILPOINT_ONCE_FILE PG_LISP_TEST_FAILPOINT_RELEASE_FILE PG_LISP_TEST_COPY_CHUNK_BYTES
SOURCE_PORT=${PORT_BASE:-5850}
TARGET_PORT=$((SOURCE_PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-switchover.XXXXXX")
SOURCE_PID= TARGET_PID= WORKER_PID= WRITER_PID= RETIRED_WORKER_PID=
PHASE=starting
cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ] && [ "${DEBUG_ON_FAILURE:-0}" = 1 ] && [ -t 0 ]; then
        echo "Switchover failed at $PHASE; source PID=$SOURCE_PID, target PID=$TARGET_PID, artifacts=$TMP" >&2
        echo "Lisp control sockets: $TMP/source.control and $TMP/target.control" >&2
        echo 'Inspect the live peers, then press Enter to clean up. The source will not be restarted.' >&2
        IFS= read -r debug_reply || :
    fi
    exec 9>&-
    for pid in "$WRITER_PID" "$WORKER_PID"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    # Always stop remaining peers; never restart the retired source on failure.
    if [ -n "$TARGET_PID" ]; then stop_node "$TARGET_KIND" target "$TARGET_PID" || :; fi
    if [ -n "$SOURCE_PID" ]; then stop_node "$SOURCE_KIND" source "$SOURCE_PID" || :; fi
    echo "logical-switchover: direction=$DIRECTION last-phase=$PHASE artifacts=$TMP"
    if [ "$status" -eq 0 ] && [ "${KEEP_ARTIFACTS:-1}" = 0 ]; then rm -rf "$TMP"; fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
phase() {
    PHASE=$1
    printf 'direction=%s\nphase=%s\nsource_port=%s\ntarget_port=%s\n' \
        "$DIRECTION" "$PHASE" "$SOURCE_PORT" "$TARGET_PORT" >"$TMP/phase.tmp"
    mv "$TMP/phase.tmp" "$TMP/phase.state"
    echo "logical-switchover: $DIRECTION $PHASE"
}
wait_value() {
    wait_port=$1 wait_query=$2 wait_expected=$3
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        if actual=$(sql "$wait_port" "$wait_query" 2>/dev/null) && [ "$actual" = "$wait_expected" ]; then return; fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "switchover timed out: $wait_query; expected $wait_expected, got $actual" >&2
    return 1
}
wait_log() {
    attempt=0
    while ! grep -F "$2" "$1" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 200 || { echo "missing writer marker: $2" >&2; return 1; }
        sleep 0.1
    done
}
wait_process() {
    attempt=0
    while kill -0 "$1" 2>/dev/null; do
        case $(ps -o stat= -p "$1" 2>/dev/null || :) in Z*) break;; esac
        attempt=$((attempt + 1))
        test "$attempt" -lt 200 || { echo "process $1 did not exit" >&2; return 1; }
        sleep 0.1
    done
}
start_node() {
    node_kind=$1 node_role=$2 node_port=$3
    if [ "$node_kind" = c ]; then
        if [ ! -f "$TMP/$node_role-data/PG_VERSION" ]; then
            "$C_PG_BINDIR/initdb" -D "$TMP/$node_role-data" -U postgres -A trust --no-locale -E UTF8 >"$TMP/$node_role-initdb.log"
            printf '\nwal_level=logical\nmax_replication_slots=20\nmax_wal_senders=20\n' >>"$TMP/$node_role-data/postgresql.conf"
        fi
        if ! "$C_PG_BINDIR/pg_ctl" -D "$TMP/$node_role-data" -l "$TMP/$node_role.log" \
                -o "-p $node_port -h 127.0.0.1 -k $TMP" -w start >"$TMP/$node_role-start.log"; then
            if [ -f "$TMP/$node_role-data/postmaster.pid" ]; then
                "$C_PG_BINDIR/pg_ctl" -D "$TMP/$node_role-data" -m immediate -w stop >>"$TMP/$node_role-stop.log" 2>&1 || :
            fi
            return 1
        fi
        IFS= read -r NODE_PID <"$TMP/$node_role-data/postmaster.pid"
    else
        rm -f "$TMP/$node_role.control"
        "$BASE/pg-lisp.sh" -p "$node_port" -D "$TMP/$node_role-data" \
            --control-socket "$TMP/$node_role.control" >>"$TMP/$node_role.log" 2>&1 &
        NODE_PID=$!
    fi
    case "$node_role" in source) SOURCE_PID=$NODE_PID;; target) TARGET_PID=$NODE_PID;; esac
    test_wait_owned_sql_server "$NODE_PID" "$node_port" "$PSQL"
}
stop_node() {
    if [ "$1" = c ]; then
        "$C_PG_BINDIR/pg_ctl" -D "$TMP/$2-data" -m fast -w stop >>"$TMP/$2-stop.log" 2>&1
    else
        kill -INT "$3"
        wait_process "$3"
        wait "$3"
    fi
}
progress_query() {
    if [ "$TARGET_KIND" = c ]; then
        printf "SELECT pg_replication_origin_progress('pg_%s',true)" "$SUBSCRIPTION_OID"
    elif [ "$SOURCE_KIND" = c ]; then
        printf "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='switchover_sub' AND state='ready'"
    else
        printf "SELECT source_lsn FROM pg_lisp_subscription_progress WHERE source_slot='switchover_sub' AND state='ready'"
    fi
}
snapshot() {
    snapshot_port=$1 snapshot_prefix=$2
    for table in switchover_rows switchover_other switchover_barrier; do
        table_oid=$(sql "$snapshot_port" "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='$table'")
        case "$table_oid" in ''|*[!0-9]*) echo "missing table OID: $table" >&2; return 1;; esac
        sql "$snapshot_port" "SELECT * FROM $table ORDER BY id" >"$TMP/$snapshot_prefix-$table.rows"
        sql "$snapshot_port" "SELECT attname,atttypid,atttypmod,attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=$table_oid AND attnum>0 AND NOT attisdropped ORDER BY attnum" >"$TMP/$snapshot_prefix-$table.columns"
        sql "$snapshot_port" "SELECT indkey,indnkeyatts,indisprimary,indisvalid FROM pg_catalog.pg_index WHERE indexrelid=pg_get_replica_identity_index($table_oid)" >"$TMP/$snapshot_prefix-$table.keys"
    done
}
compare_snapshot() {
    for table in switchover_rows switchover_other switchover_barrier; do
        for kind in rows columns keys; do
            diff -u "$TMP/$1-$table.$kind" "$TMP/$2-$table.$kind"
        done
    done
}
assert_source_fenced() {
    test -z "$(ss -H -ltn "sport = :$SOURCE_PORT")"
    if sql "$SOURCE_PORT" 'SELECT 1' >/dev/null 2>&1; then
        echo 'retired source still accepts connections' >&2
        return 1
    fi
}
assert_apply_stopped() {
    if [ "$TARGET_KIND" = c ]; then
        test "$(sql "$TARGET_PORT" "SELECT subenabled FROM pg_subscription WHERE subname='switchover_sub'")" = f
        test "$(sql "$TARGET_PORT" "SELECT count(*) FROM pg_stat_subscription WHERE subname='switchover_sub' AND pid IS NOT NULL")" = 0
    else
        test -n "$RETIRED_WORKER_PID"
        if kill -0 "$RETIRED_WORKER_PID" 2>/dev/null; then
            echo 'retired apply process is still alive' >&2
            return 1
        fi
    fi
}

echo "logical-switchover: pg-lisp revision=$(git -C "$BASE" describe --always --dirty)"
start_node "$SOURCE_KIND" source "$SOURCE_PORT"
start_node "$TARGET_KIND" target "$TARGET_PORT"
for port in "$SOURCE_PORT" "$TARGET_PORT"; do
    sql "$port" "CREATE SEQUENCE switchover_ids START WITH 1000 INCREMENT BY 1; CREATE TABLE switchover_rows (id integer PRIMARY KEY, amount bigint, label varchar(64)); CREATE TABLE switchover_other (id integer PRIMARY KEY, amount bigint); CREATE TABLE switchover_barrier (id integer PRIMARY KEY, sequence_floor bigint)" >/dev/null
done
sql "$SOURCE_PORT" "INSERT INTO switchover_rows VALUES (nextval('switchover_ids'),10,'before copy'); INSERT INTO switchover_other VALUES (1,20)" >/dev/null
if [ "$DIRECTION" != lisp-to-lisp ]; then
    sql "$SOURCE_PORT" 'CREATE PUBLICATION switchover_pub FOR TABLE switchover_rows,switchover_other,switchover_barrier' >/dev/null
fi
if [ "$TARGET_KIND" = c ]; then
    sql "$TARGET_PORT" "CREATE SUBSCRIPTION switchover_sub CONNECTION 'host=127.0.0.1 port=$SOURCE_PORT dbname=postgres user=postgres' PUBLICATION switchover_pub WITH (copy_data=true,streaming=off,binary=false,two_phase=false,failover=false,origin='any')" >/dev/null
    SUBSCRIPTION_OID=$(sql "$TARGET_PORT" "SELECT oid FROM pg_subscription WHERE subname='switchover_sub'")
    wait_value "$TARGET_PORT" "SELECT count(*) FROM pg_subscription_rel WHERE srsubid=$SUBSCRIPTION_OID AND srsubstate='r'" 3
else
    if [ "$SOURCE_KIND" = c ]; then set -- --mode standard --publication switchover_pub
    else set --; fi
    "$BASE/pg-lisp-subscribe" "$@" --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" \
        --slot switchover_sub --table switchover_rows --table switchover_other --table switchover_barrier >"$TMP/worker.log" 2>&1 &
    WORKER_PID=$!
    if [ "$SOURCE_KIND" = c ]; then
        wait_value "$TARGET_PORT" "SELECT state FROM pg_lisp_standard_subscriptions WHERE subscription_key='switchover_sub'" ready
    else
        wait_value "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='switchover_sub'" ready
    fi
fi
wait_value "$TARGET_PORT" 'SELECT amount FROM switchover_rows WHERE id=1000' 10
phase replicating

# Consume an ID in a rolled-back transaction; MAX(id) is not sequence state.
sql "$SOURCE_PORT" "BEGIN; INSERT INTO switchover_rows VALUES (nextval('switchover_ids'),99,'aborted'); ROLLBACK" >/dev/null
mkfifo "$TMP/writer.sql"
PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 <"$TMP/writer.sql" >"$TMP/writer.log" 2>&1 &
WRITER_PID=$!
exec 9>"$TMP/writer.sql"
printf "BEGIN;\nINSERT INTO switchover_rows VALUES (nextval('switchover_ids'),30,'drained writer');\nUPDATE switchover_rows SET amount=amount+7 WHERE id=1000;\nINSERT INTO switchover_other VALUES (2,40);\nSELECT 'writer-open';\n" >&9
wait_log "$TMP/writer.log" writer-open
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM switchover_rows WHERE id=1002')" = 0
# Operator quiesces application input, drains its outstanding transaction and
# closes the only application connection. No arbitrary external writers exist.
printf "COMMIT;\nSELECT 'writer-drained';\n\\q\n" >&9
exec 9>&-
wait_process "$WRITER_PID"
wait "$WRITER_PID"
WRITER_PID=
wait_log "$TMP/writer.log" writer-drained
phase writers-drained
PROGRESS_QUERY=$(progress_query)
PRE_BARRIER_PROGRESS=$(sql "$TARGET_PORT" "$PROGRESS_QUERY")
test -n "$PRE_BARRIER_PROGRESS"
SEQUENCE_FLOOR=$(sql "$SOURCE_PORT" "SELECT nextval('switchover_ids')")
test "$SEQUENCE_FLOOR" = 1003
sql "$SOURCE_PORT" "BEGIN; UPDATE switchover_other SET amount=amount+5 WHERE id=1; INSERT INTO switchover_barrier VALUES (1,$SEQUENCE_FLOOR); COMMIT" >/dev/null
wait_value "$TARGET_PORT" 'SELECT sequence_floor FROM switchover_barrier WHERE id=1' "$SEQUENCE_FLOOR"
DURABLE_PROGRESS=$(sql "$TARGET_PORT" "$PROGRESS_QUERY")
test -n "$DURABLE_PROGRESS"
test "$(sql "$TARGET_PORT" "SELECT '$DURABLE_PROGRESS'::pg_lsn > '$PRE_BARRIER_PROGRESS'::pg_lsn")" = t
printf '%s\n' "$PRE_BARRIER_PROGRESS" >"$TMP/pre-barrier-progress.txt"
snapshot "$SOURCE_PORT" source-final
snapshot "$TARGET_PORT" target-applied
compare_snapshot source-final target-applied
printf '%s\n' "$DURABLE_PROGRESS" >"$TMP/durable-progress.txt"
printf '%s\n' "$SEQUENCE_FLOOR" >"$TMP/sequence-floor.txt"
phase barrier-durable

# Process shutdown is the source fence, not the endpoint-file update below.
stop_node "$SOURCE_KIND" source "$SOURCE_PID"
SOURCE_PID=
assert_source_fenced
phase source-fenced
if [ "$TARGET_KIND" = c ]; then
    sql "$TARGET_PORT" 'ALTER SUBSCRIPTION switchover_sub DISABLE' >/dev/null
    wait_value "$TARGET_PORT" "SELECT count(*) FROM pg_stat_subscription WHERE subname='switchover_sub' AND pid IS NOT NULL" 0
else
    RETIRED_WORKER_PID=$WORKER_PID
    kill "$WORKER_PID" 2>/dev/null || :
    wait_process "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || :
    WORKER_PID=
fi
assert_apply_stopped
phase apply-detached
SETTLED_PROGRESS=$(sql "$TARGET_PORT" "$PROGRESS_QUERY")
test "$(sql "$TARGET_PORT" "SELECT '$SETTLED_PROGRESS'::pg_lsn >= '$DURABLE_PROGRESS'::pg_lsn")" = t
DURABLE_PROGRESS=$SETTLED_PROGRESS
printf '%s\n' "$DURABLE_PROGRESS" >"$TMP/durable-progress.txt"
stop_node "$TARGET_KIND" target "$TARGET_PID"
TARGET_PID=
start_node "$TARGET_KIND" target "$TARGET_PORT"
assert_apply_stopped
assert_source_fenced
snapshot "$TARGET_PORT" target-restarted
compare_snapshot source-final target-restarted
test "$(sql "$TARGET_PORT" "$PROGRESS_QUERY")" = "$DURABLE_PROGRESS"
phase target-validated

# Target sequence is precreated but standard row replication never advances it.
sql "$TARGET_PORT" "SELECT setval('switchover_ids',$SEQUENCE_FLOOR,true)" >/dev/null
printf 'PGHOST=127.0.0.1\nPGPORT=%s\nPGDATABASE=postgres\nPGUSER=postgres\n' "$TARGET_PORT" >"$TMP/new-writer.env.tmp"
mv "$TMP/new-writer.env.tmp" "$TMP/new-writer.env"
NEW_ID=$(sql "$TARGET_PORT" "SELECT nextval('switchover_ids')")
test "$NEW_ID" = 1004
sql "$TARGET_PORT" "BEGIN; INSERT INTO switchover_rows VALUES ($NEW_ID,50,'new writer'); UPDATE switchover_rows SET amount=amount+3 WHERE id=1000; INSERT INTO switchover_other VALUES (3,60); COMMIT" >/dev/null
phase target-writer
wait_value "$TARGET_PORT" 'SELECT id,amount FROM switchover_rows ORDER BY id' "$(printf '1000|20\n1002|30\n1004|50')"
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM switchover_rows WHERE id=1001')" = 0
snapshot "$TARGET_PORT" new-writer
stop_node "$TARGET_KIND" target "$TARGET_PID"
TARGET_PID=
start_node "$TARGET_KIND" target "$TARGET_PORT"
assert_source_fenced
assert_apply_stopped
snapshot "$TARGET_PORT" new-writer-restarted
compare_snapshot new-writer new-writer-restarted
test "$(sql "$TARGET_PORT" "$PROGRESS_QUERY")" = "$DURABLE_PROGRESS"
test "$(sql "$TARGET_PORT" "SELECT nextval('switchover_ids')")" = 1005
phase complete
echo "logical-switchover: direction=$DIRECTION PASS; retired source remains stopped"
if [ "${PAUSE_AFTER_SWITCHOVER:-0}" = 1 ]; then
    echo "New writer is live on $TARGET_PORT; source $SOURCE_PORT is stopped. Endpoint: $TMP/new-writer.env"
    echo 'Inspect from another terminal, then press Enter to stop the rehearsal target.'
    IFS= read -r pause_reply
fi
