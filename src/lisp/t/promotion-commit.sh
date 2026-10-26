#!/bin/sh
# Automatic pending completion and committed-primary restart, without coordination.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6454}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-commit.XXXXXX")
HELPER= SERVER=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$HELPER" "$SERVER"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "promotion-commit: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ -n "${PROMOTION_SEED:-}" ]; then
    SEED=$PROMOTION_SEED
else
    PORT=$PORT "$BASE/t/physical-restartpoint-auto.sh" >"$TMP/seed.log" 2>&1
    SEED=$(sed -n 's/^physical-restartpoint-auto: artifacts=//p' "$TMP/seed.log")
fi
if [ -n "${PROMOTION_PENDING_SEED:-}" ]; then
    PENDING=$PROMOTION_PENDING_SEED
else
    PROMOTION_SEED=$SEED PORT=$PORT "$BASE/t/promotion-prefix.sh" >"$TMP/prefix.log" 2>&1
    PENDING=$(sed -n 's/^promotion-prefix: artifacts=//p' "$TMP/prefix.log")
fi
inspect() { python3 "$BASE/t/interop/promotion-primary-inspect.py" "$@"; }
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$1"
}
start() {
    rm -f "$CASE/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$CASE/data" --control-socket "$CASE/control.sock" >>"$CASE/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
    test "$(sql 'SELECT pg_is_in_recovery()')" = f
    test "$(sql 'SELECT count(*) FROM data')" = 5
    identity=$(PGCONNECT_TIMEOUT=2 "$PSQL" -X "host=127.0.0.1 port=$PORT user=postgres dbname=postgres replication=true" -qAt -v ON_ERROR_STOP=1 -c IDENTIFY_SYSTEM)
    test "$(printf '%s\n' "$identity" | cut -d '|' -f 2)" = 2
}
stop() {
    kill -INT "$SERVER"
    wait "$SERVER"
    SERVER=
}
crash() {
    kill -KILL "$SERVER"
    wait "$SERVER" 2>/dev/null || :
    SERVER=
}
cycle() {
    sql 'CREATE TABLE promoted_items (id integer); INSERT INTO promoted_items VALUES (101)'
    crash
    start
    test "$(sql 'SELECT count(*) FROM promoted_items')" = 1
    sql 'INSERT INTO promoted_items VALUES (102)'
    printf '(load "%s/t/promotion-fill-wal.lisp")\n(promotion-test-fill-wal)\n(quit)\n' "$BASE" |
        "$SBCL" --script "$BASE/t/control-client.lisp" unix "$CASE/control.sock" >"$CASE/fill.log"
    if grep -q ';; error:' "$CASE/fill.log"; then cat "$CASE/fill.log" >&2; exit 1; fi
    sql CHECKPOINT
    test ! -e "$CASE/data/pg_wal/000000020000000000000000.wal"
    crash
    start
    test "$(sql 'SELECT count(*) FROM promoted_items')" = 2
    inspect check "$CASE/data" "$CASE/expected.json" later
    stop
}
points='normal after-child-history-sync after-child-eor-write after-child-eor-flush after-child-batch-write after-child-checkpoint-write after-child-checkpoint-flush after-child-data-sync after-child-control-rename after-child-control-directory-fsync after-child-primary-initialization after-retire-required after-retire-manifest after-retire-follow after-retire-pointer after-retire-pending after-child-primary-ready'
for point in $points; do
    CASE=$TMP/$point
    mkdir -m 700 "$CASE"
    cp -a "$PENDING/after-child-unlink/data" "$CASE/data"
    inspect expected "$CASE/data" "$CASE/expected.json"
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" PG_PROMOTION_PAUSE="$point" \
        PG_PROMOTION_MARKER="$CASE/marker" "$SBCL" --script "$BASE/t/promotion-commit-native.lisp" >"$CASE/commit.log" 2>&1 &
    HELPER=$!
    if [ "$point" = normal ]; then
        wait "$HELPER"
        HELPER=
    else
        attempt=0
        while [ ! -f "$CASE/marker" ]; do
            kill -0 "$HELPER" 2>/dev/null || { cat "$CASE/commit.log" >&2; exit 1; }
            attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
        done
        read -r marked_pid <"$CASE/marker"
        test "$marked_pid" = "$HELPER"
        kill -KILL "$HELPER"
        wait "$HELPER" 2>/dev/null || :
        HELPER=
    fi
    case "$point" in
        after-child-checkpoint-write|after-child-control-rename) cp -a "$CASE/data" "$CASE/crash-data" ;;
    esac
    case "$point" in
        after-child-control-*|after-child-primary-*|after-retire-*)
            # Child control alone must survive retirement of all obsolete GEN/spool authorities.
            inspect drop-retired "$CASE/data" ;;
    esac
    start
    inspect check "$CASE/data" "$CASE/expected.json" initial
    case "$point" in
        normal)
            EXPECTED_TIMELINE=2 "$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$PORT" primary
            cycle ;;
        after-child-control-rename|after-retire-pending) cycle ;;
        *) stop ;;
    esac
    echo "promotion-commit: $point PASS"
done
for kind in torn-1 torn-31 torn-87 torn-119 torn-120; do
    CASE=$TMP/$kind
    mkdir -m 700 "$CASE"
    cp -a "$TMP/after-child-checkpoint-write/crash-data" "$CASE/data"
    cp "$TMP/after-child-checkpoint-write/expected.json" "$CASE/expected.json"
    inspect damage "$CASE/data" "$CASE/expected.json" "$kind"
    start
    inspect check "$CASE/data" "$CASE/expected.json" initial
    stop
done
CASE=$TMP/orphan-staging
mkdir -m 700 "$CASE"
cp -a "$PENDING/after-child-unlink/data" "$CASE/data"
inspect expected "$CASE/data" "$CASE/expected.json"
inspect damage "$CASE/data" "$CASE/expected.json" orphan-staging
start
test ! -e "$CASE/data/pg_wal/000000020000000000000009.wal"
inspect check "$CASE/data" "$CASE/expected.json" initial
stop
for kind in foreign-staging oversized-staging orphan-history bad-child-checkpoint wrong-pending wrong-history; do
    CASE=$TMP/reject-$kind
    mkdir -m 700 "$CASE"
    case "$kind" in
        *staging|orphan-history) source_case=$TMP/after-child-checkpoint-write ;;
        *) source_case=$TMP/after-child-control-rename ;;
    esac
    cp -a "$source_case/crash-data" "$CASE/data"
    cp "$source_case/expected.json" "$CASE/expected.json"
    inspect damage "$CASE/data" "$CASE/expected.json" "$kind"
    python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --snapshot-storage "$CASE/data" "$CASE/before.json"
    set +e
    timeout -k 2 15 "$BASE/pg-lisp.sh" -p "$PORT" -D "$CASE/data" >"$CASE/startup.log" 2>&1
    startup_status=$?
    set -e
    test "$startup_status" -eq 1
    case "$kind" in
        foreign-staging) expected_error='child checkpoint contains a different complete record' ;;
        oversized-staging) expected_error='child checkpoint prefix or extent differs' ;;
        orphan-history) expected_error='published child history has an unexplained WAL segment' ;;
        bad-child-checkpoint) expected_error='WAL checksum mismatch' ;;
        wrong-pending|wrong-history) expected_error='committed child contradicts its pending recovery metadata' ;;
    esac
    grep -F "$expected_error" "$CASE/startup.log" >/dev/null
    python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --verify-pending "$CASE/data" "$CASE/before.json"
done
for stage in prefix wal; do
    if [ "$stage" = prefix ]; then
        stage_seed=$PENDING
        stage_points='normal after-prefix-fsync after-prefix-directory-fsync after-child-unlink after-child-unlink-directory-fsync after-child-rename after-child-directory-fsync'
    else
        if [ -n "${PROMOTION_WAL_SEED:-}" ]; then
            stage_seed=$PROMOTION_WAL_SEED
        else
            PROMOTION_SEED=$SEED PORT=$PORT "$BASE/t/promotion-wal.sh" >"$TMP/wal.log" 2>&1
            stage_seed=$(sed -n 's/^promotion-prefix: artifacts=//p' "$TMP/wal.log")
        fi
        stage_points='normal before-history after-history-install after-history before-eor-write after-eor-write after-eor-fsync after-eor-directory-fsync'
    fi
    for point in $stage_points; do
        CASE=$TMP/old-$stage-$point
        mkdir -m 700 "$CASE"
        cp -a "$stage_seed/$point/data" "$CASE/data"
        inspect expected "$CASE/data" "$CASE/expected.json"
        start
        inspect check "$CASE/data" "$CASE/expected.json" initial
        stop
    done
done
if [ -n "${PROMOTION_QUALIFIED_SEED:-}" ]; then
    QUALIFIED=$PROMOTION_QUALIFIED_SEED
else
    SOURCE_WAL_NAMESPACE=timeline PORT=$PORT "$BASE/t/physical-restartpoint-auto.sh" >"$TMP/qualified-seed.log" 2>&1
    QUALIFIED=$(sed -n 's/^physical-restartpoint-auto: artifacts=//p' "$TMP/qualified-seed.log")
fi
CASE=$TMP/qualified-source
mkdir -m 700 "$CASE"
cp -a "$QUALIFIED/copy" "$CASE/data"
PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" PG_PROMOTION_PAUSE=normal \
    "$SBCL" --script "$BASE/t/promotion-intent-native.lisp" >"$CASE/intent.log" 2>&1
inspect expected "$CASE/data" "$CASE/expected.json"
start
inspect check "$CASE/data" "$CASE/expected.json" initial
EXPECTED_TIMELINE=2 "$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$PORT" primary
cycle
echo 'promotion-commit: automatic pending recovery, irreversible control and child SQL/restart/recycling PASS'
