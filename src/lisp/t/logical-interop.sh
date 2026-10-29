#!/bin/sh
# Native C PostgreSQL peer acceptance. Missing prerequisites are failures.
set -eu

BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIRECTION=${1:-both}
case "$DIRECTION" in lisp-to-c|c-to-lisp|both) ;; *)
    echo "usage: $0 [lisp-to-c|c-to-lisp|both]" >&2; exit 2;; esac
# Each isolated topology selects its own fault injection deterministically.
unset PG_LISP_TEST_FAILPOINT PG_LISP_TEST_FAILPOINT_OBJECT PG_LISP_TEST_FAILPOINT_ACTION PG_LISP_TEST_FAILPOINT_GUARD_FILE PG_LISP_TEST_FAILPOINT_LSN_AFTER PG_LISP_TEST_FAILPOINT_HIT PG_LISP_TEST_FAILPOINT_ONCE_FILE PG_LISP_TEST_FAILPOINT_RELEASE_FILE PG_LISP_TEST_COPY_CHUNK_BYTES
: "${C_PG_BINDIR:?set C_PG_BINDIR to the reference build installation bin directory}"
: "${C_PG_REVISION:?set C_PG_REVISION to the exact reference build Git revision}"
case "$C_PG_BINDIR" in /*) ;; *) echo 'C_PG_BINDIR must be absolute' >&2; exit 2;; esac
if [ "$DIRECTION" = both ]; then
    result=0
    "$0" lisp-to-c || result=1
    "$0" c-to-lisp || result=1
    if [ "$result" -eq 0 ]; then echo 'logical-interop: both PASS'; fi
    exit "$result"
fi
for program in postgres initdb pg_ctl psql pg_config; do
    test -x "$C_PG_BINDIR/$program" || {
        echo "missing reference executable: $C_PG_BINDIR/$program" >&2; exit 1;
    }
done
REFERENCE_VERSION=$("$C_PG_BINDIR/postgres" --version)
for program in initdb pg_ctl psql; do
    version=$("$C_PG_BINDIR/$program" --version)
    test "${version#*PostgreSQL) }" = "${REFERENCE_VERSION#*PostgreSQL) }" || {
        echo "reference executable version mismatch: $version" >&2; exit 1;
    }
done
test "$("$C_PG_BINDIR/pg_config" --bindir)" = "$C_PG_BINDIR" || {
    echo 'pg_config belongs to a different installation' >&2; exit 1;
}
PORT_BASE=${PORT_BASE:-5790}
LISP_PORT=$PORT_BASE
C_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-interop.XXXXXX")
LISP_PID=
WORKER_PID=
LOCK_PID=
PUB_DML_PID=
C_STARTED=no
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    exec 9>&-
    if [ -n "$LOCK_PID" ]; then kill "$LOCK_PID" 2>/dev/null || :; wait "$LOCK_PID" 2>/dev/null || :; fi
    touch "$TMP/copy.release"
    touch "$TMP/standard-copy.release"
    for paused in "$TMP"/native-*.paused; do
        test -f "$paused" || continue
        touch "${paused%.paused}.release"
    done
    if [ -n "$PUB_DML_PID" ]; then kill "$PUB_DML_PID" 2>/dev/null || :; wait "$PUB_DML_PID" 2>/dev/null || :; fi
    if [ -n "$WORKER_PID" ]; then kill "$WORKER_PID" 2>/dev/null || :; wait "$WORKER_PID" 2>/dev/null || :; fi
    if [ "$C_STARTED" = yes ]; then
        "$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -m immediate -w stop >/dev/null 2>&1 || :
    fi
    # Disconnect native subscribers before a smart shutdown waits for them.
    if [ -n "$LISP_PID" ]; then kill "$LISP_PID" 2>/dev/null || :; wait "$LISP_PID" 2>/dev/null || :; fi
    if [ "$status" -ne 0 ] || [ "${KEEP_ARTIFACTS:-0}" = 1 ]; then
        echo "logical-interop: artifacts $TMP" >&2
    else
        rm -rf "$TMP"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

sql() {
    if [ "$1" = "$C_PORT" ]; then
        PGOPTIONS='-c search_path=public' "$C_PG_BINDIR/psql" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
            -qAt -v ON_ERROR_STOP=1 -c "$2"
    else
        "$C_PG_BINDIR/psql" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
            -qAt -v ON_ERROR_STOP=1 -c "$2"
    fi
}
wait_sql() {
    port=$1
    attempt=0
    while [ "$attempt" -lt 200 ]; do
        if sql "$port" '' >/dev/null 2>&1; then return; fi
        kill -0 "$LISP_PID" 2>/dev/null || return 1
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "logical-interop: server startup timed out on $port" >&2
    return 1
}
wait_rows() {
    target=$1
    expected=$2
    query=${3:-SELECT id, amount, label FROM interop_rows ORDER BY id}
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        if [ -n "$WORKER_PID" ] && ! kill -0 "$WORKER_PID" 2>/dev/null; then
            echo "logical-interop: subscriber exited; see $TMP/worker.log" >&2
            return 1
        fi
        actual=$(sql "$target" "$query")
        if [ "$actual" = "$expected" ]; then return; fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "logical-interop: row convergence timed out on $target" >&2
    return 1
}
wait_native_marker() {
    native_marker=$1
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        test ! -f "$native_marker" || return 0
        kill -0 "$LISP_PID" 2>/dev/null || return 1
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "logical-interop: native COPY pause was not reached: $native_marker" >&2
    return 1
}
native_control() {
    timeout 20 "${SBCL:-sbcl}" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock"
}
native_publication_capture_race() {
    printf '(install-native-publication-race-hook "%s")\n(quit)\n' "$TMP" | native_control >"$TMP/publication-control.log"
    grep -F ':NATIVE-PUBLICATION-HOOK-INSTALLED' "$TMP/publication-control.log" >/dev/null
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub DISABLE' >/dev/null
    "$C_PG_BINDIR/psql" -X -h 127.0.0.1 -p "$LISP_PORT" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 \
        -c "INSERT INTO interop_rows VALUES (24,240,'historic capture before ALTER')" >"$TMP/publication-insert.log" 2>&1 &
    PUB_DML_PID=$!
    wait_native_marker "$TMP/native-publication.paused"
    test "$(sql "$C_PORT" 'SELECT count(*) FROM interop_rows WHERE id=24')" = 0
    sql "$LISP_PORT" 'ALTER PUBLICATION interop_pub DROP TABLE interop_rows' >/dev/null
    touch "$TMP/native-publication.release"
    wait "$PUB_DML_PID"
    PUB_DML_PID=
    # Keep the changed catalog across a crash before replaying the older row.
    kill -KILL "$LISP_PID"
    wait "$LISP_PID" 2>/dev/null || :
    LISP_PID=
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$LISP_PORT" -D "$TMP/lisp-data" --control-socket "$TMP/control.sock" >>"$TMP/lisp.log" 2>&1 &
    LISP_PID=$!
    wait_sql "$LISP_PORT"
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub ENABLE' >/dev/null
    wait_rows "$C_PORT" 24 'SELECT id FROM interop_rows WHERE id=24'
    sql "$LISP_PORT" 'ALTER PUBLICATION interop_pub ADD TABLE interop_rows' >/dev/null
    echo 'logical-interop: historic membership survived concurrent ALTER after mutation and publisher SIGKILL before replay'
}
record_native_sync() {
    native_table=$1
    native_generation=$2
    native_relid=$(sql "$C_PORT" "SELECT 'public.$native_table'::regclass::oid")
    native_slot="pg_${native_sub}_sync_${native_relid}_${native_system}"
    test "$(sql "$C_PORT" "SELECT srsubstate FROM pg_subscription_rel WHERE srsubid=$native_sub AND srrelid=$native_relid")" = d
    test "$(sql "$LISP_PORT" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$native_slot'")" = 1
    native_boundary=$(sql "$LISP_PORT" "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='$native_slot'")
    test "$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_${native_sub}_${native_relid}',true) >= '$native_boundary'::pg_lsn AND '$native_boundary'::pg_lsn > '0/0'::pg_lsn")" = t
    printf '%s|%s|%s|%s\n' "$native_sub" "$native_relid" "$native_slot" "$native_boundary" >"$TMP/$native_table-generation-$native_generation.txt"
    echo "logical-interop: native table=$native_table generation=$native_generation slot=$native_slot boundary=$native_boundary"
}
native_copy_recovery() {
    native_sub=$(sql "$C_PORT" "SELECT oid FROM pg_subscription WHERE subname='interop_sub'")
    native_system=$(sql "$C_PORT" 'SELECT system_identifier FROM pg_control_system()')
    for native_table in interop_rows interop_other; do
        wait_native_marker "$TMP/native-$native_table-1.paused"
        record_native_sync "$native_table" 1
    done
    sql "$LISP_PORT" "INSERT INTO interop_rows VALUES (23,230,'between native generations')" >/dev/null
    for native_table in interop_rows interop_other; do
        native_pid=$(sql "$C_PORT" "SELECT pid FROM pg_stat_subscription WHERE subname='interop_sub' AND relid='public.$native_table'::regclass")
        test -n "$native_pid"
        test "$(sql "$C_PORT" "SELECT pg_terminate_backend($native_pid,5000)")" = t
    done
    # Leave old source COPY sessions paused while retries recreate the same
    # slot names. Old cleanup must preserve replacement snapshot ownership.
    for native_table in interop_rows interop_other; do
        wait_native_marker "$TMP/native-$native_table-2.paused"
        record_native_sync "$native_table" 2
        first_boundary=$(cut -d '|' -f 4 "$TMP/$native_table-generation-1.txt")
        test "$(sql "$C_PORT" "SELECT '$native_boundary'::pg_lsn > '$first_boundary'::pg_lsn")" = t
        touch "$TMP/native-$native_table-1.release"
    done
    sql "$LISP_PORT" "BEGIN; INSERT INTO interop_rows VALUES (10,100,'during copy'); UPDATE interop_rows SET amount=11 WHERE id=1; INSERT INTO interop_other VALUES (2,20,'during copy'); COMMIT; BEGIN; INSERT INTO interop_rows VALUES (99,99,'abort during copy'); ROLLBACK" >/dev/null
    for native_table in interop_rows interop_other; do touch "$TMP/native-$native_table-2.release"; done
    wait_native_ready
    sql "$LISP_PORT" "INSERT INTO interop_rows VALUES (16,160,'native sync barrier')" >/dev/null
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    test "$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true) >= max(srsublsn) AND min(srsublsn) > '0/0'::pg_lsn FROM pg_subscription_rel WHERE srsubid=$native_sub")" = t
    for native_table in interop_rows interop_other; do
        native_relid=$(sql "$C_PORT" "SELECT 'public.$native_table'::regclass::oid")
        native_slot="pg_${native_sub}_sync_${native_relid}_${native_system}"
        test "$(sql "$LISP_PORT" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$native_slot'")" = 0
        test "$(sql "$C_PORT" "SELECT count(*) FROM pg_replication_origin WHERE roname='pg_${native_sub}_${native_relid}'")" = 0
    done
    echo 'logical-interop: both native sync workers recovered, crossed independent snapshots, reached READY and retired sync slots/origins'
}
wait_native_ready() {
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        state=$(sql "$C_PORT" "SELECT count(*) FROM pg_subscription_rel JOIN pg_subscription ON oid=srsubid WHERE subname='interop_sub' AND srsubstate='r'")
        test "$state" != 2 || return 0
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo 'logical-interop: native relations did not both reach READY' >&2
    return 1
}
setup_tables() {
    for port in "$LISP_PORT" "$C_PORT"; do
        if [ "$DIRECTION" = c-to-lisp ] && [ "$port" = "$C_PORT" ]; then
            # INCLUDE attributes must not become replica-identity key columns.
            sql "$port" 'CREATE TABLE interop_rows (id integer NOT NULL, amount bigint, label varchar(100), price numeric, rate double precision, units smallint, workday date, at_time time, stamp timestamp, span interval day to second(2), zoned_time timetz, PRIMARY KEY(id) INCLUDE(label)); CREATE TABLE interop_other (id integer NOT NULL, amount bigint, label varchar(100), PRIMARY KEY(id) INCLUDE(label)); CREATE TABLE interop_unpublished (id integer PRIMARY KEY, amount bigint, label varchar(100))' >/dev/null
            # Worker connections inherit a path that shadows public tables.
            # Harness SQL explicitly selects public through PGOPTIONS above.
            sql "$port" "CREATE SCHEMA shadow; CREATE TABLE shadow.interop_rows (id integer PRIMARY KEY, amount bigint, label varchar(100)); INSERT INTO shadow.interop_rows VALUES (404,404,'wrong namespace'); ALTER ROLE postgres SET search_path=shadow,public" >/dev/null
        else
            sql "$port" 'CREATE TABLE interop_rows (id integer PRIMARY KEY, amount bigint, label varchar(100), price numeric, rate double precision, units smallint, workday date, at_time time, stamp timestamp, span interval day to second(2), zoned_time timetz); CREATE TABLE interop_other (id integer PRIMARY KEY, amount bigint, label varchar(100)); CREATE TABLE interop_unpublished (id integer PRIMARY KEY, amount bigint, label varchar(100))' >/dev/null
        fi
        for table in interop_rows interop_other interop_unpublished; do
            sql "$port" "ALTER TABLE $table ALTER amount SET DEFAULT 40+2; ALTER TABLE $table ADD CONSTRAINT ${table}_amount_check CHECK(amount IS NULL OR amount >= '-9223372036854775808'::bigint)" >/dev/null
        done
    done
}
start_standard_worker() {
    worker_point=${1:-}
    worker_action=${2:-}
    worker_marker=${3:-}
    worker_log=${4:-$TMP/worker.log}
    (
        unset PG_LISP_TEST_FAILPOINT PG_LISP_TEST_FAILPOINT_OBJECT PG_LISP_TEST_FAILPOINT_GUARD_FILE PG_LISP_TEST_FAILPOINT_LSN_AFTER PG_LISP_TEST_FAILPOINT_HIT PG_LISP_TEST_FAILPOINT_ONCE_FILE PG_LISP_TEST_FAILPOINT_RELEASE_FILE PG_LISP_TEST_COPY_CHUNK_BYTES
        if [ -n "$worker_point" ]; then
            export PG_LISP_TEST_FAILPOINT="$worker_point"
            export PG_LISP_TEST_FAILPOINT_ACTION="$worker_action"
            export PG_LISP_TEST_FAILPOINT_ONCE_FILE="$worker_marker"
            export PG_LISP_TEST_FAILPOINT_RELEASE_FILE="$TMP/standard-copy.release"
        fi
        exec "$BASE/pg-lisp-subscribe" --mode standard --publication interop_pub \
            --source-port "$C_PORT" --target-port "$LISP_PORT" --slot interop_sub \
            --table interop_rows --table interop_other
    ) >>"$worker_log" 2>&1 &
    WORKER_PID=$!
}
wait_worker_marker() {
    marker=$1
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        test ! -f "$marker" || return 0
        kill -0 "$WORKER_PID" 2>/dev/null || break
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "logical-interop: worker did not reach $marker; see $TMP/worker.log" >&2
    return 1
}
wait_standard_slot_inactive() {
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        active=$(sql "$C_PORT" "SELECT active FROM pg_replication_slots WHERE slot_name='$1'")
        test "$active" = t || return 0
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo 'logical-interop: old copy connection did not release its slot' >&2
    return 1
}
wait_standard_slot_active() {
    attempt=0
    while [ "$attempt" -lt 300 ]; do
        active=$(sql "$C_PORT" "SELECT active FROM pg_replication_slots WHERE slot_name='$1'")
        test "$active" != t || return 0
        kill -0 "$WORKER_PID" 2>/dev/null || break
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo 'logical-interop: standard stream did not become active' >&2
    return 1
}
standard_copy_recovery() {
    # The stable subscription key is not a license to drop a same-named slot.
    sql "$C_PORT" "SELECT slot_name FROM pg_create_logical_replication_slot('interop_sub','pgoutput')" >/dev/null
    start_standard_worker after-source-slot-created-before-copying-progress crash "$TMP/create.crashed"
    wait_worker_marker "$TMP/create.crashed"
    if wait "$WORKER_PID"; then echo 'logical-interop: expected create crash' >&2; return 1; fi
    WORKER_PID=
    test "$(sql "$LISP_PORT" "SELECT state FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = creating
    first_slot=$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    wait_standard_slot_inactive "$first_slot"
    start_standard_worker after-copy-chunk-commit crash "$TMP/chunk.crashed"
    wait_worker_marker "$TMP/chunk.crashed"
    if wait "$WORKER_PID"; then echo 'logical-interop: expected copy chunk crash' >&2; return 1; fi
    WORKER_PID=
    test "$(sql "$LISP_PORT" "SELECT state FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = copying
    second_slot=$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    test "$second_slot" != "$first_slot"
    test "$(sql "$C_PORT" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$first_slot'")" = 0
    test "$(sql "$LISP_PORT" 'SELECT count(*) FROM interop_rows')" -gt 0
    test "$(sql "$LISP_PORT" 'SELECT count(*) FROM interop_other')" = 0
    wait_standard_slot_inactive "$second_slot"
    start_standard_worker after-copying-progress-commit pause "$TMP/standard-copy.paused"
    wait_worker_marker "$TMP/standard-copy.paused"
    test "$(sql "$C_PORT" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$second_slot'")" = 0
    sql "$C_PORT" "BEGIN; INSERT INTO interop_rows VALUES (10,100,'during copy'); UPDATE interop_rows SET amount=11 WHERE id=1; INSERT INTO interop_other VALUES (2,20,'during copy'); COMMIT; BEGIN; INSERT INTO interop_rows VALUES (99,99,'abort during copy'); ROLLBACK" >/dev/null
    touch "$TMP/standard-copy.release"
    test "$(sql "$C_PORT" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='interop_sub'")" = 1
    echo 'logical-interop: standard copy recovered CREATE-response and committed-chunk crashes; concurrent handoff released'
}
standard_apply_recovery() {
    ready_slot=$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")
    before_marker=$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    before_amount=$(sql "$LISP_PORT" 'SELECT amount FROM interop_rows WHERE id=1')
    kill -KILL "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || :
    WORKER_PID=
    wait_standard_slot_inactive "$ready_slot"
    start_standard_worker after-row-change crash "$TMP/apply.crashed"
    wait_standard_slot_active "$ready_slot"
    sql "$C_PORT" "BEGIN; INSERT INTO interop_rows VALUES (13,130,'uncommitted apply'); UPDATE interop_rows SET amount=amount+3 WHERE id=1; INSERT INTO interop_other VALUES (6,60,'atomic replay'); COMMIT" >/dev/null
    wait_worker_marker "$TMP/apply.crashed"
    if wait "$WORKER_PID"; then echo 'logical-interop: expected apply crash' >&2; return 1; fi
    WORKER_PID=
    test "$(sql "$LISP_PORT" 'SELECT count(*) FROM interop_rows WHERE id=13')" = 0
    test "$(sql "$LISP_PORT" 'SELECT amount FROM interop_rows WHERE id=1')" = "$before_amount"
    test "$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = "$before_marker"
    wait_standard_slot_inactive "$ready_slot"
    start_standard_worker
    wait_standard_slot_active "$ready_slot"
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_other ORDER BY id')" 'SELECT id, amount, label FROM interop_other ORDER BY id'
    test "$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = "$ready_slot"
    echo 'logical-interop: standard uncommitted apply rolled back and replayed atomically'
}
standard_endpoint_recovery() {
    endpoint_slot=$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")
    endpoint_marker=$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    kill -KILL "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || :
    WORKER_PID=
    wait_standard_slot_inactive "$endpoint_slot"
    kill -KILL "$LISP_PID"
    wait "$LISP_PID" 2>/dev/null || :
    LISP_PID=
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$LISP_PORT" -D "$TMP/lisp-data" --control-socket "$TMP/control.sock" >>"$TMP/lisp.log" 2>&1 &
    LISP_PID=$!
    wait_sql "$LISP_PORT"
    test "$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")" = "$endpoint_marker"
    test "$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = "$endpoint_slot"
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    sql "$C_PORT" "INSERT INTO interop_rows VALUES (14,140,'after target crash')" >/dev/null
    start_standard_worker
    wait_standard_slot_active "$endpoint_slot"
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    endpoint_marker=$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    "$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -m immediate -w stop >"$TMP/c-restart-stop.log" 2>&1
    C_STARTED=no
    # Closing the source must make the worker close its target session too.
    attempt=0
    while kill -0 "$WORKER_PID" 2>/dev/null && [ "$attempt" -lt 100 ]; do
        attempt=$((attempt + 1))
        sleep 0.1
    done
    if kill -0 "$WORKER_PID" 2>/dev/null; then echo 'logical-interop: worker ignored source shutdown' >&2; return 1; fi
    if wait "$WORKER_PID"; then echo 'logical-interop: expected source-disconnect error' >&2; return 1; fi
    WORKER_PID=
    C_STARTED=yes
    "$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -l "$TMP/c.log" -o "-p $C_PORT -h 127.0.0.1 -k $TMP" -w start >"$TMP/c-restart-start.log" 2>&1
    test "$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")" = "$endpoint_marker"
    test "$(sql "$C_PORT" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$endpoint_slot'")" = 1
    sql "$C_PORT" "INSERT INTO interop_rows VALUES (15,150,'after source crash')" >/dev/null
    start_standard_worker
    wait_standard_slot_active "$endpoint_slot"
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_other ORDER BY id')" 'SELECT id, amount, label FROM interop_other ORDER BY id'
    test "$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = "$endpoint_slot"
    echo 'logical-interop: standard target SIGKILL and C publisher immediate restart retained durable progress and slot'
}
standard_feedback_recovery() {
    ready_slot=$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")
    test -n "$ready_slot"
    kill -KILL "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || :
    WORKER_PID=
    wait_standard_slot_inactive "$ready_slot"
    # READY startup must refuse observable publication drift before streaming
    # or changing durable target state. Restore the source contract afterwards.
    sql "$C_PORT" 'ALTER PUBLICATION interop_pub DROP TABLE interop_other' >/dev/null
    expect_standard_start_failure 'standard publication tables must match the configured table allowlist'
    sql "$C_PORT" 'ALTER PUBLICATION interop_pub ADD TABLE interop_other' >/dev/null
    sql "$C_PORT" "ALTER PUBLICATION interop_pub SET (publish='insert')" >/dev/null
    expect_standard_start_failure 'standard mode requires an explicit-table publication with all four publish actions'
    sql "$C_PORT" "ALTER PUBLICATION interop_pub SET (publish='insert,update,delete,truncate')" >/dev/null
    start_standard_worker after-target-commit-before-feedback crash "$TMP/feedback.crashed"
    wait_standard_slot_active "$ready_slot"
    sql "$C_PORT" "INSERT INTO interop_rows VALUES (11,110,'lost feedback')" >/dev/null
    wait_worker_marker "$TMP/feedback.crashed"
    if wait "$WORKER_PID"; then echo 'logical-interop: expected lost-feedback crash' >&2; return 1; fi
    WORKER_PID=
    test "$(sql "$LISP_PORT" 'SELECT count(*) FROM interop_rows WHERE id=11')" = 1
    test "$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")" = "$ready_slot"
    target_marker=$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    test "$(sql "$C_PORT" "SELECT confirmed_flush_lsn < '$target_marker'::pg_lsn FROM pg_replication_slots WHERE slot_name='$ready_slot'")" = t
    wait_standard_slot_inactive "$ready_slot"
    start_standard_worker
    wait_standard_slot_active "$ready_slot"
    test "$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")" = "$ready_slot"
    sql "$C_PORT" "INSERT INTO interop_rows VALUES (12,120,'after worker restart')" >/dev/null
    wait_rows "$LISP_PORT" "$(sql "$C_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    echo 'logical-interop: standard READY and lost-feedback recovery preserved attempt slot and target data'
}
expect_standard_start_failure() {
    unchanged_marker=$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
    drift_sequence=$((${drift_sequence:-0} + 1))
    drift_log="$TMP/publication-drift-$drift_sequence.log"
    start_standard_worker '' '' '' "$drift_log"
    attempt=0
    while kill -0 "$WORKER_PID" 2>/dev/null && [ "$attempt" -lt 100 ]; do
        attempt=$((attempt + 1))
        sleep 0.1
    done
    if kill -0 "$WORKER_PID" 2>/dev/null; then
        echo 'logical-interop: standard subscriber accepted publication drift' >&2
        return 1
    fi
    if wait "$WORKER_PID"; then echo 'logical-interop: expected publication rejection' >&2; return 1; fi
    WORKER_PID=
    grep -F "$1" "$drift_log" >/dev/null
    test "$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")" = "$unchanged_marker"
    test "$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")" = "$ready_slot"
    echo "logical-interop: READY startup rejected expected publication drift ($drift_sequence)"
}
workload() {
    source=$1
    sql "$source" "UPDATE interop_rows SET price=12.0300,rate='NaN',units=-32768,workday='2024-02-29',at_time='12:34:56.12',stamp='2000-01-01 12:34:56',span='1 mon 2 days 00:00:01.125',zoned_time='12:34:56+01:23' WHERE id=1" >/dev/null
    wait_rows "$target" '1|12.0300|NaN|-32768|2024-02-29|12:34:56.12|2000-01-01 12:34:56|1 mon 2 days 00:00:01.13|12:34:56+01:23' 'SELECT id,price,rate,units,workday,at_time,stamp,span,zoned_time FROM interop_rows WHERE id=1'
    sql "$source" "BEGIN; INSERT INTO interop_rows VALUES (3, 30, 'third'); UPDATE interop_rows SET amount = amount + 7 WHERE id = 1; UPDATE interop_rows SET id = 4 WHERE id = 2; DELETE FROM interop_rows WHERE id = 3; COMMIT; BEGIN; INSERT INTO interop_rows VALUES (99, 99, 'abort'); ROLLBACK; INSERT INTO interop_unpublished VALUES (1, 1, 'excluded'); BEGIN; TRUNCATE TABLE interop_other; INSERT INTO interop_other VALUES (5, 50, 'after truncate'); COMMIT" >/dev/null
    # Observe every committed action before removing the temporary row. A final
    # unchanged row inventory would miss absent INSERT/UPDATE/DELETE events.
    sql "$source" "INSERT INTO interop_rows SELECT 80,amount,'dml source' FROM interop_rows WHERE id=1 RETURNING id" >/dev/null
    wait_rows "$target" "$(sql "$source" 'SELECT id,amount,label FROM interop_rows WHERE id=80')" 'SELECT id,amount,label FROM interop_rows WHERE id=80'
    sql "$source" "INSERT INTO interop_rows VALUES(80,800,'dml update') ON CONFLICT(id) DO UPDATE SET amount=excluded.amount,label=excluded.label RETURNING old.amount,new.amount; INSERT INTO interop_rows VALUES(80,801,'dml skipped') ON CONFLICT DO NOTHING RETURNING id" >/dev/null
    wait_rows "$target" '80|800|dml update' 'SELECT id,amount,label FROM interop_rows WHERE id=80'
    sql "$source" "UPDATE interop_rows t SET amount=s.amount FROM (SELECT 850::bigint AS amount) s WHERE t.id=80 RETURNING t.id,s.amount" >/dev/null
    wait_rows "$target" '80|850|dml update' 'SELECT id,amount,label FROM interop_rows WHERE id=80'
    sql "$source" "BEGIN; INSERT INTO interop_rows SELECT 81,amount,'dml rollback' FROM interop_rows WHERE id=1 RETURNING id; INSERT INTO interop_rows VALUES(80,802,'rollback update') ON CONFLICT(id) DO UPDATE SET amount=excluded.amount RETURNING id; DELETE FROM interop_rows WHERE id=80 RETURNING id; ROLLBACK; DELETE FROM interop_rows t USING (SELECT 80 AS id) s WHERE t.id=s.id RETURNING t.id,s.id" >/dev/null
    wait_rows "$target" '' 'SELECT id,amount,label FROM interop_rows WHERE id=80 OR id=81'
}
restart_native_publisher() {
    subscription=$(sql "$C_PORT" "SELECT oid FROM pg_subscription WHERE subname='interop_sub'")
    test -n "$subscription"
    # Force the native origin's WAL durable before destroying the publisher.
    origin=$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$subscription',true)")
    test "$(sql "$C_PORT" "SELECT '$origin'::pg_lsn > '0/0'::pg_lsn")" = t
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub DISABLE' >/dev/null
    attempt=0
    while [ "$(sql "$LISP_PORT" "SELECT active FROM pg_replication_slots WHERE slot_name='interop_sub'")" = t ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 300 || return 1
        sleep 0.1
    done
    slot_floor=$(sql "$LISP_PORT" "SELECT confirmed_flush_lsn,restart_lsn FROM pg_replication_slots WHERE slot_name='interop_sub'")
    echo "logical-interop: publisher-crash native-subscription=$subscription durable-origin=$origin"
    kill -KILL "$LISP_PID"
    wait "$LISP_PID" 2>/dev/null || :
    LISP_PID=
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$LISP_PORT" -D "$TMP/lisp-data" --control-socket "$TMP/control.sock" >>"$TMP/lisp.log" 2>&1 &
    LISP_PID=$!
    wait_sql "$LISP_PORT"
    test "$(sql "$LISP_PORT" "SELECT confirmed_flush_lsn,restart_lsn FROM pg_replication_slots WHERE slot_name='interop_sub'")" = "$slot_floor"
    test "$(sql "$C_PORT" "SELECT oid FROM pg_subscription WHERE subname='interop_sub'")" = "$subscription"
    sql "$LISP_PORT" "INSERT INTO interop_rows VALUES (11,110,'after publisher crash')" >/dev/null
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub ENABLE' >/dev/null
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_native_ready
    echo 'logical-interop: publisher SIGKILL/restart resumed the existing native subscription'
}
native_apply_recovery() {
    printf '(load "%s/t/interop/native-failpoints.lisp")\n(install-native-transaction-end-hook "%s" (list 17))\n(quit)\n' "$BASE" "$TMP" | native_control >"$TMP/apply-end-control.log"
    grep -F ':NATIVE-TRANSACTION-HOOK-INSTALLED' "$TMP/apply-end-control.log" >/dev/null
    mkfifo "$TMP/native-lock.sql"
    PGAPPNAME=interop-lock "$C_PG_BINDIR/psql" -X -h 127.0.0.1 -p "$C_PORT" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 <"$TMP/native-lock.sql" >"$TMP/native-lock.log" 2>&1 &
    LOCK_PID=$!
    exec 9>"$TMP/native-lock.sql"
    printf "BEGIN;\nSELECT id FROM public.interop_rows WHERE id=1 FOR UPDATE;\nSELECT 'native-lock-held';\n" >&9
    attempt=0
    while [ "$(sql "$C_PORT" "SELECT count(*) FROM pg_stat_activity WHERE application_name='interop-lock' AND state='idle in transaction' AND query LIKE '%native-lock-held%'")" != 1 ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 100 || return 1
        sleep 0.1
    done
    lock_backend=$(sql "$C_PORT" "SELECT pid FROM pg_stat_activity WHERE application_name='interop-lock'")
    apply_backend=$(sql "$C_PORT" "SELECT pid FROM pg_stat_subscription WHERE subname='interop_sub' AND relid IS NULL")
    test -n "$apply_backend"
    apply_origin=$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")
    sql "$LISP_PORT" "BEGIN; INSERT INTO interop_rows VALUES (17,170,'native partial apply'); UPDATE interop_rows SET amount=amount+3 WHERE id=1; INSERT INTO interop_other VALUES (6,60,'native atomic replay'); COMMIT" >/dev/null
    wait_native_marker "$TMP/native-transaction-17.ready"
    failed_end=$(tr -d '\n' <"$TMP/native-transaction-17.end")
    attempt=0
    while [ "$(sql "$C_PORT" "SELECT count(*) FROM pg_stat_activity WHERE pid=$apply_backend AND wait_event_type='Lock' AND pg_blocking_pids(pid) @> ARRAY[$lock_backend]")" != 1 ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 300 || return 1
        sleep 0.1
    done
    test "$(sql "$C_PORT" 'SELECT count(*) FROM interop_rows WHERE id=17')" = 0
    test "$(sql "$C_PORT" "SELECT pg_terminate_backend($apply_backend,5000)")" = t
    test "$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")" = "$apply_origin"
    confirmed=$(sql "$LISP_PORT" "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='interop_sub'")
    test "$(sql "$C_PORT" "SELECT '$confirmed'::pg_lsn < '$failed_end'::pg_lsn")" = t
    printf 'ROLLBACK;\n' >&9
    exec 9>&-
    wait "$LOCK_PID"
    LOCK_PID=
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_other ORDER BY id')" 'SELECT id, amount, label FROM interop_other ORDER BY id'
    echo 'logical-interop: native main apply worker rolled back partial effects, withheld failed confirmation, and replayed atomically'
}
verify_final_barrier() {
    if [ "$direction" = lisp-to-c ]; then
        printf '(load "%s/t/interop/native-failpoints.lisp")\n(install-native-transaction-end-hook "%s" (list 90))\n(quit)\n' "$BASE" "$TMP" | native_control >"$TMP/barrier-control.log"
        grep -F ':NATIVE-TRANSACTION-HOOK-INSTALLED' "$TMP/barrier-control.log" >/dev/null
    fi
    sql "$source" "INSERT INTO interop_rows VALUES (90,900,'final durable barrier')" >/dev/null
    wait_rows "$target" "$(sql "$source" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$target" "$(sql "$source" 'SELECT id, amount, label FROM interop_other ORDER BY id')" 'SELECT id, amount, label FROM interop_other ORDER BY id'
    test "$(sql "$target" 'SELECT count(*) FROM interop_unpublished')" = 0
    if [ "$direction" = lisp-to-c ]; then
        wait_native_marker "$TMP/native-transaction-90.ready"
        barrier_end=$(tr -d '\n' <"$TMP/native-transaction-90.end")
        barrier_origin=$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")
        test "$(sql "$C_PORT" "SELECT '$barrier_origin'::pg_lsn >= '$barrier_end'::pg_lsn")" = t
    else
        # This source position came from a received Commit and became durable
        # with target effects, rather than being sampled from global source WAL.
        barrier_end=$(sql "$LISP_PORT" "SELECT source_lsn FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub' AND state='ready'")
        final_slot=$(sql "$LISP_PORT" "SELECT actual_slot FROM pg_lisp_standard_subscriptions WHERE subscription_key='interop_sub'")
        test -n "$barrier_end"
        attempt=0
        while [ "$(sql "$C_PORT" "SELECT confirmed_flush_lsn >= '$barrier_end'::pg_lsn FROM pg_replication_slots WHERE slot_name='$final_slot'")" != t ]; do
            attempt=$((attempt + 1))
            test "$attempt" -lt 300 || return 1
            sleep 0.1
        done
    fi
    printf '%s\n' "$barrier_end" >"$TMP/final-published-barrier.txt"
    echo "logical-interop: direction=$direction durable published barrier=$barrier_end; canonical rows match"
}
native_target_recovery() {
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub DISABLE' >/dev/null
    durable_origin=$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")
    ready_states=$(sql "$C_PORT" "SELECT srrelid,srsubstate,srsublsn FROM pg_subscription_rel WHERE srsubid=$native_sub ORDER BY srrelid")
    "$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -m immediate -w stop >"$TMP/native-restart-stop.log" 2>&1
    C_STARTED=no
    sql "$LISP_PORT" "INSERT INTO interop_rows VALUES (18,180,'after native target crash')" >/dev/null
    C_STARTED=yes
    "$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -l "$TMP/c.log" -o "-p $C_PORT -h 127.0.0.1 -k $TMP" -w start >"$TMP/native-restart-start.log" 2>&1
    test "$(sql "$C_PORT" "SELECT oid FROM pg_subscription WHERE subname='interop_sub'")" = "$native_sub"
    test "$(sql "$C_PORT" "SELECT srrelid,srsubstate,srsublsn FROM pg_subscription_rel WHERE srsubid=$native_sub ORDER BY srrelid")" = "$ready_states"
    test "$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")" = "$durable_origin"
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub ENABLE' >/dev/null
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_other ORDER BY id')" 'SELECT id, amount, label FROM interop_other ORDER BY id'
    echo 'logical-interop: native target immediate restart retained durable origin and per-table READY frontiers'
}
native_feedback_recovery() {
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub DISABLE' >/dev/null
    attempt=0
    while [ "$(sql "$LISP_PORT" "SELECT active FROM pg_replication_slots WHERE slot_name='interop_sub'")" = t ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 300 || return 1
        sleep 0.1
    done
    feedback_after=$(sql "$LISP_PORT" 'SELECT pg_current_wal_lsn()')
    printf '(load "%s/t/interop/native-failpoints.lisp")\n(install-native-feedback-test-hook "%s" (parse-replication-lsn "%s"))\n(quit)\n' "$BASE" "$TMP" "$feedback_after" | native_control >"$TMP/feedback-control.log"
    grep -F ':NATIVE-FEEDBACK-HOOK-INSTALLED' "$TMP/feedback-control.log" >/dev/null
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub ENABLE' >/dev/null
    sql "$LISP_PORT" "INSERT INTO interop_rows VALUES (19,190,'native lost feedback')" >/dev/null
    wait_native_marker "$TMP/native-feedback.paused"
    test "$(sql "$C_PORT" 'SELECT count(*) FROM interop_rows WHERE id=19')" = 1
    feedback_position=$(tr -d '\n' <"$TMP/native-feedback-position.txt")
    durable_feedback_origin=$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")
    test "$(sql "$C_PORT" "SELECT '$durable_feedback_origin'::pg_lsn >= '$feedback_position'::pg_lsn")" = t
    lost_floor=$(sql "$LISP_PORT" "SELECT confirmed_flush_lsn,restart_lsn FROM pg_replication_slots WHERE slot_name='interop_sub'")
    lost_confirmed=$(printf '%s' "$lost_floor" | cut -d '|' -f 1)
    test "$(sql "$C_PORT" "SELECT '$lost_confirmed'::pg_lsn < '$feedback_position'::pg_lsn")" = t
    kill -KILL "$LISP_PID"
    wait "$LISP_PID" 2>/dev/null || :
    LISP_PID=
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub DISABLE' >/dev/null
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$LISP_PORT" -D "$TMP/lisp-data" --control-socket "$TMP/control.sock" >>"$TMP/lisp.log" 2>&1 &
    LISP_PID=$!
    wait_sql "$LISP_PORT"
    test "$(sql "$LISP_PORT" "SELECT confirmed_flush_lsn,restart_lsn FROM pg_replication_slots WHERE slot_name='interop_sub'")" = "$lost_floor"
    test "$(sql "$C_PORT" "SELECT pg_replication_origin_progress('pg_$native_sub',true)")" = "$durable_feedback_origin"
    sql "$C_PORT" 'ALTER SUBSCRIPTION interop_sub ENABLE' >/dev/null
    sql "$LISP_PORT" "INSERT INTO interop_rows VALUES (20,200,'after native lost feedback')" >/dev/null
    wait_rows "$C_PORT" "$(sql "$LISP_PORT" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    echo 'logical-interop: native durable apply survived publisher crash before slot acknowledgment, resumed from origin without duplicates'
}
run_direction() {
    direction=$1
    if [ "$direction" = lisp-to-c ]; then source=$LISP_PORT; target=$C_PORT
    else source=$C_PORT; target=$LISP_PORT; fi
    echo "logical-interop: direction=$direction phase=initial-copy copy_data=true"
    echo 'logical-interop: requested streaming=off binary=false two_phase=false failover=false origin=any'
    sql "$source" "INSERT INTO interop_rows VALUES (1, 10, 'first'), (2, 20, 'second'); INSERT INTO interop_other VALUES (1, 10, 'copied'); CREATE PUBLICATION interop_pub FOR TABLE interop_rows, interop_other WITH (publish = 'insert, update, delete, truncate')" >/dev/null
    unicode_seed=$(printf "INSERT INTO interop_rows VALUES (21,NULL,'żółw ''quote'' \\\\N\tline\nend'),(22,9223372036854775807,NULL),(25,CAST('-9223372036854775808' AS bigint),'bigint minimum'),(26,260,'\\\\N')")
    sql "$source" "$unicode_seed" >/dev/null
    if [ "$direction" = lisp-to-c ]; then
        sql "$target" "CREATE SUBSCRIPTION interop_sub CONNECTION 'host=127.0.0.1 port=$source dbname=postgres user=postgres application_name=interop_sub' PUBLICATION interop_pub WITH (copy_data = true, streaming = off, binary = false, two_phase = false, failover = false, origin = 'any')" >/dev/null
        sql "$target" "SELECT substream, subbinary, subtwophasestate, subfailover, suborigin FROM pg_subscription WHERE subname = 'interop_sub'"
        native_copy_recovery
    else
        standard_copy_recovery
    fi
    wait_rows "$target" "$(sql "$source" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$target" "$(sql "$source" 'SELECT id, amount, label FROM interop_other ORDER BY id')" 'SELECT id, amount, label FROM interop_other ORDER BY id'
    if [ "$direction" = lisp-to-c ]; then
        negotiated=$(awk -F '|' '$1 == "interop_sub" {print $2; exit}' "$TMP/native-protocol.txt")
        case "$negotiated" in 1|2|3|4) ;; *) echo 'logical-interop: native protocol was not observed' >&2; return 1;; esac
        echo "logical-interop: direction=lisp-to-c protocol=$negotiated observed by publisher before CopyBoth"
    else
        grep -F "proto_version '1'" "$TMP/c.log" >"$TMP/standard-protocol.log"
        echo 'logical-interop: direction=c-to-lisp protocol=1 observed in C publisher log before applied transaction'
    fi
    if [ "$direction" = lisp-to-c ]; then native_publication_capture_race; fi
    echo "logical-interop: direction=$direction phase=live-stream"
    workload "$source"
    wait_rows "$target" "$(sql "$source" 'SELECT id, amount, label FROM interop_rows ORDER BY id')"
    wait_rows "$target" '5|50|after truncate' 'SELECT id, amount, label FROM interop_other ORDER BY id'
    test "$(sql "$target" 'SELECT count(*) FROM interop_unpublished')" = 0
    if [ "$direction" = c-to-lisp ]; then
        # EXTERNAL guarantees this 20000-digit value remains out of line on C.
        # Updating only amount or the replica key makes pgoutput send 'u'.
        sql "$C_PORT" 'ALTER TABLE interop_rows ALTER COLUMN price SET STORAGE EXTERNAL' >/dev/null
        toast_number=$(awk 'BEGIN { s=1; printf "7"; for (i=1;i<20000;i++) { s=(s*1664525+1013904223)%4294967296; printf "%d", int(s/16777216)%10 } }')
        sql "$C_PORT" "INSERT INTO interop_rows (id,amount,price) VALUES (71,710,'$toast_number'::numeric)" >/dev/null
        wait_rows "$LISP_PORT" '710|20000|t' "SELECT amount, length(price::text), price = '$toast_number'::numeric FROM interop_rows WHERE id=71"
        test "$(sql "$C_PORT" 'SELECT pg_column_size(price) > 8160 FROM interop_rows WHERE id=71')" = t
        sql "$C_PORT" "SELECT slot_name FROM pg_create_logical_replication_slot('toast_observe','pgoutput')" >/dev/null
        sql "$C_PORT" 'UPDATE interop_rows SET amount=711 WHERE id=71' >/dev/null
        wait_rows "$LISP_PORT" '711|20000|t' "SELECT amount, length(price::text), price = '$toast_number'::numeric FROM interop_rows WHERE id=71"
        sql "$C_PORT" 'UPDATE interop_rows SET id=72, amount=720 WHERE id=71' >/dev/null
        wait_rows "$LISP_PORT" '720|20000|t' "SELECT amount, length(price::text), price = '$toast_number'::numeric FROM interop_rows WHERE id=72"
        observed=$(sql "$C_PORT" "SELECT count(*) FROM pg_logical_slot_get_binary_changes('toast_observe',NULL,NULL,'proto_version','1','publication_names','interop_pub') WHERE substring(data,1,1)=decode('55','hex') AND position(decode('6e756e6e6e6e6e6e6e','hex') in data)>0")
        test "$observed" = 2 || { echo "logical-interop: expected two unchanged TOAST markers, got $observed" >&2; return 1; }
        sql "$C_PORT" "SELECT pg_drop_replication_slot('toast_observe')" >/dev/null
    fi
    if [ "$direction" = lisp-to-c ]; then
        native_apply_recovery
        restart_native_publisher
        native_target_recovery
        native_feedback_recovery
    fi
    if [ "$direction" = c-to-lisp ]; then
        standard_feedback_recovery
        standard_apply_recovery
        standard_endpoint_recovery
    fi
    verify_final_barrier
    echo "logical-interop: direction=$direction PASS"
}

echo "logical-interop: C revision=$C_PG_REVISION version=$REFERENCE_VERSION"
sha256sum "$C_PG_BINDIR/postgres" "$C_PG_BINDIR/initdb" "$C_PG_BINDIR/pg_ctl" "$C_PG_BINDIR/psql"
echo "logical-interop: pg-lisp revision=$(git -C "$BASE" describe --always --abbrev=40 --dirty)"
"$C_PG_BINDIR/initdb" -D "$TMP/c-data" -U postgres -A trust --no-locale -E UTF8 >"$TMP/initdb.log" 2>&1
printf '\nwal_level=logical\nmax_replication_slots=20\nmax_wal_senders=20\nlog_statement=all\nlog_replication_commands=on\nlog_connections=all\n' >>"$TMP/c-data/postgresql.conf"
C_STARTED=yes
"$C_PG_BINDIR/pg_ctl" -D "$TMP/c-data" -l "$TMP/c.log" -o "-p $C_PORT -h 127.0.0.1 -k $TMP" -w start >"$TMP/pg_ctl.log" 2>&1
"$BASE/pg-lisp.sh" -p "$LISP_PORT" -D "$TMP/lisp-data" --control-socket "$TMP/control.sock" >"$TMP/lisp.log" 2>&1 &
LISP_PID=$!
wait_sql "$LISP_PORT"
echo "logical-interop: pg-lisp version=$(sql "$LISP_PORT" 'SHOW server_version')"
setup_tables
if [ "$DIRECTION" = lisp-to-c ]; then
    printf '(load "%s/t/interop/native-failpoints.lisp")\n(install-native-copy-test-hook "%s")\n(quit)\n' "$BASE" "$TMP" | native_control >"$TMP/control.log"
    grep -F ':NATIVE-COPY-HOOK-INSTALLED' "$TMP/control.log" >/dev/null
fi
run_direction "$DIRECTION"
