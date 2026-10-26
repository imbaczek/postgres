#!/bin/sh
# Native allocation WAL, independent from row changes and writer coordination.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-6096}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-xid-reservation.XXXXXX")
SOURCE= TARGET= READER=
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
control() {
    printf '%s\n(quit)\n' "$2" | timeout 25 "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
cleanup() {
    status=$?
    trap - EXIT
    : >"$TMP/release"
    if [ -d "$TMP/source/pg_wal" ]; then
        : >"$TMP/source/pg_wal/test-resume-after-wal-bytes-flush"
    fi
    if [ -n "$READER" ]; then wait "$READER" 2>/dev/null || :; fi
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "xid-reservation: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_source() {
    rm -f "$TMP/source.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >>"$TMP/source.log" 2>&1 &
    SOURCE=$!
    test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
}
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
wait_applied() {
    end=$(awk '{print $3}' "$TMP/source/global/pg_wal_control")
    lsn=$(python3 -c 'import sys; n=int(sys.argv[1]); print(f"{n >> 32:X}/{n & 0xffffffff:X}")' "$end")
    attempt=0
    while [ "$(sql "$TARGET_PORT" "SELECT pg_last_wal_replay_lsn() >= '$lsn'::pg_lsn")" != t ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 200
        kill -0 "$TARGET"; sleep 0.05
    done
}
assert_target() {
    checked target "(let ((state (getf *physical-recovery-state* :source-reservation))) (assert (and (getf state :complete) (= (getf state :bound) (1- $1)) (= (getf state :through) *last-replay-lsn*) (= *next-xid* $visible) (= *xid-batch-end* $visible))))" "$2"
    cmp "$TMP/saved-target-control" "$TMP/copy/global/pg_control"
}
legacy_migration() {
    kind=$1
    kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
    cp -a "$TMP/copy" "$TMP/migration-$kind"
    for version in 1 2; do
        rm -rf "$TMP/copy"
        cp -a "$TMP/migration-$kind" "$TMP/copy"
        PG_XID_MIGRATION_ACTION=fixture PG_XID_MIGRATION_DATA="$TMP/copy" \
            PG_XID_MIGRATION_VERSION=$version "$SBCL" --script "$BASE/t/xid-legacy-migration.lisp" \
            >"$TMP/legacy-$kind-$version-fixture.log" 2>&1
        barrier="$TMP/legacy-$kind-$version-dirty"
        PG_XID_MIGRATION_BARRIER="$barrier" "$SBCL" --script "$BASE/t/xid-legacy-migration.lisp" \
            -p "$TARGET_PORT" -D "$TMP/copy" >"$TMP/legacy-$kind-$version-crash.log" 2>&1 &
        TARGET=$!
        attempt=0
        while [ ! -f "$barrier" ]; do
            attempt=$((attempt + 1)); test "$attempt" -lt 200
            kill -0 "$TARGET"; sleep 0.05
        done
        kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
        python3 - "$BASE" "$TMP/copy" "$TMP/migration-$kind" "$TMP/base-checkpoint.json" <<'PY'
import json, runpy, sys
from pathlib import Path
decode = runpy.run_path(str(Path(sys.argv[1]) / 't/interop/recovery-control-state.py'))['decode']
root = Path(sys.argv[2])
control = decode((root / 'physical-recovery.control').read_bytes())
previous = decode((Path(sys.argv[3]) / 'physical-recovery.control').read_bytes())
assert control['version'] == 3 and control['state'] == 5
assert control['next_xid'] == json.loads(Path(sys.argv[4]).read_text())['next_xid']
assert control['next_xid'] < int((root / 'global/pg_control').read_text())
assert control['applied'] == previous['applied']
PY
        if [ "$kind" = full ]; then
            start_target
            assert_target "$new_end" "legacy-$kind-$version-recovered"
            kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
        else
            "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" \
                >"$TMP/legacy-$kind-$version-refused.log" 2>&1 &
            TARGET=$!
            attempt=0
            while kill -0 "$TARGET" 2>/dev/null; do
                attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.05
            done
            result=0; wait "$TARGET" || result=$?; TARGET=
            test "$result" -ne 0
            grep -q 'legacy restartpoint lost actual XID history; reseed required' \
                "$TMP/legacy-$kind-$version-refused.log"
        fi
    done
    rm -rf "$TMP/copy"
    cp -a "$TMP/migration-$kind" "$TMP/copy"
    start_target
}
start_source
sql "$PORT" 'CREATE TABLE xid_rows (id integer PRIMARY KEY); INSERT INTO xid_rows VALUES (1)'
mkdir -m 700 "$TMP/copy"
checked source "(physical-base-backup \"$TMP/copy/\" \"xid_base\")" backup
python3 "$BASE/t/interop/checkpoint-snapshot.py" "$TMP/copy" --save "$TMP/base-checkpoint.json"
visible=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["next_xid"])' "$TMP/base-checkpoint.json")
test "$visible" -lt "$(cat "$TMP/copy/global/pg_control")"
cp "$TMP/copy/global/pg_control" "$TMP/saved-target-control"
start_target
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"xid_base\")" follow

# Skip unused capacity, never backwards, then stop inside the first new reserve.
checked source '(assert (active-xids-empty-p))' no-active
checked source '(setf *next-xid* *xid-batch-end*)' consume-old-capacity
old_end=$(cat "$TMP/source/global/pg_control")
checked source "(setf *xid-reservation-test-hook* (lambda (point end) (when (eq point :after-wal-flush) (setf *xid-reservation-test-hook* nil) (with-open-file (out \"$TMP/flushed-end\" :direction :output) (format out \"~d~%\" end)) (loop repeat 400 until (probe-file \"$TMP/release\") do (sleep 0.05)) (unless (probe-file \"$TMP/release\") (error \"reservation barrier timed out\")))))" barrier
sql "$PORT" 'BEGIN; SELECT count(*) FROM xid_rows; ROLLBACK' >"$TMP/reader.log" 2>&1 &
READER=$!
attempt=0
while [ ! -s "$TMP/flushed-end" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200
    kill -0 "$SOURCE"; sleep 0.05
done
new_end=$(cat "$TMP/flushed-end")
test "$(cat "$TMP/source/global/pg_control")" = "$old_end"
kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=
if wait "$READER"; then exit 1; fi
READER=
start_source
checked source "(assert (= *next-xid* *xid-batch-end* (load-next-xid) $new_end))" startup-floor
wait_applied
assert_target "$new_end" replayed-floor
# Publish a genuine generation while retaining the complete receive prefix.
checked target '(with-physical-store (*physical-receive-store*) (physical-restartpoint-publish (physical-store-prefix-digest-under-lock *physical-receive-store* *last-replay-lsn*)))' retained-prefix-point
legacy_migration full
sql "$TARGET_PORT" CHECKPOINT
kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
start_target
assert_target "$new_end" restored-floor

# Reservation bytes may reach disk before pg_wal_control advances, too.
cp "$TMP/source/global/pg_wal_control" "$TMP/old-wal-control"
checked source '(setf *wal-test-pause-at* "after-wal-bytes-flush")' arm-reservation-bytes
sql "$PORT" 'BEGIN; SELECT count(*) FROM xid_rows; ROLLBACK' >"$TMP/reader-bytes.log" 2>&1 &
READER=$!
attempt=0
while [ ! -f "$TMP/source/pg_wal/test-paused-after-wal-bytes-flush" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200
    kill -0 "$SOURCE"; sleep 0.05
done
cmp "$TMP/old-wal-control" "$TMP/source/global/pg_wal_control"
test "$(cat "$TMP/source/global/pg_control")" = "$new_end"
kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=
if wait "$READER"; then exit 1; fi
READER=
new_end=$((new_end + 1000))
start_source
checked source "(assert (= *next-xid* *xid-batch-end* (load-next-xid) $new_end))" adopted-tail-floor
wait_applied
assert_target "$new_end" adopted-tail-replica

# Reader transactions allocate native IDs but produce no row-change WAL.
python3 - "$TMP/readers.sql" <<'PY'
import sys
with open(sys.argv[1], "w") as stream:
    for _ in range(1005):
        stream.write("BEGIN; SELECT count(*) FROM xid_rows; ROLLBACK;\n")
PY
PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 -f "$TMP/readers.sql" >"$TMP/readers.log"
final_end=$(cat "$TMP/source/global/pg_control")
test "$final_end" -gt "$new_end"
wait_applied
assert_target "$final_end" read-only-floor
sql "$TARGET_PORT" CHECKPOINT
checked target "(let* ((rp (physical-restartpoint-read *physical-recovery-state*)) (state (getf rp :source-reservation))) (assert (and (= 3 (getf rp :version)) (getf state :complete) (= (getf state :bound) (1- $final_end)) (= (getf state :through) (getf rp :lsn)))))" compacted-floor
kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
start_target
assert_target "$final_end" restored-read-only-floor
# A source checkpoint carries actual progress from transactions without row WAL.
sql "$PORT" CHECKPOINT
python3 "$BASE/t/interop/checkpoint-snapshot.py" "$TMP/source" --save "$TMP/source-checkpoint.json"
visible=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["next_xid"])' "$TMP/source-checkpoint.json")
test "$visible" -lt "$final_end"
wait_applied
assert_target "$final_end" checkpoint-actual-visibility
sql "$TARGET_PORT" CHECKPOINT
kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
start_target
assert_target "$final_end" restored-checkpoint-actual-visibility
legacy_migration compacted
assert_target "$final_end" preserved-after-legacy-refusal
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM xid_rows')" = 1
if sql "$TARGET_PORT" 'INSERT INTO xid_rows VALUES (2)' >"$TMP/write-guard.log" 2>&1; then exit 1; fi
checked target '(physical-recovery-prepare-shutdown)' prepare
checked source '(physical-source-pin-release "xid_base")' release
echo 'xid-reservation: reservation crashes, actual visibility, restartpoints and legacy migration crash/reseed paths PASS'
