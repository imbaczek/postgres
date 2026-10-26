#!/bin/sh
# Native checkpoint durability and replica metadata, without coordination.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-6056}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-checkpoint-snapshot.XXXXXX")
SOURCE= TARGET= CHECKPOINT=
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
control() {
    printf '%s\n(quit)\n' "$2" | timeout 20 "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
cleanup() {
    status=$?
    trap - EXIT
    if [ -d "$TMP/source/pg_wal" ]; then
        : >"$TMP/source/pg_wal/test-resume-after-checkpoint-wal-flush"
        : >"$TMP/source/pg_wal/test-resume-after-checkpoint-control"
    fi
    if [ -n "$CHECKPOINT" ]; then wait "$CHECKPOINT" 2>/dev/null || :; fi
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "checkpoint-snapshot: artifacts=$TMP"
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
probe() { python3 "$BASE/t/interop/checkpoint-snapshot.py" "$TMP/source" "$@"; }
start_source
sql "$PORT" 'CREATE TABLE cp_data (id integer PRIMARY KEY); INSERT INTO cp_data VALUES (1); CHECKPOINT'
probe >"$TMP/initial-copy.json"
kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=
# Convert only this stopped test image to the old empty v5 record and PGWAL3.
probe --legacy-fixture
start_source
test "$(sql "$PORT" 'SELECT count(*) FROM cp_data')" = 1
checked source '(assert (null *wal-checkpoint-snapshot*))' legacy-unknown
sql "$PORT" CHECKPOINT
probe --save "$TMP/old.json" >"$TMP/initial-copy.json"
sql "$PORT" 'INSERT INTO cp_data VALUES (2)'
checked source '(setf *wal-test-pause-at* "after-checkpoint-wal-flush")' arm
sql "$PORT" CHECKPOINT >"$TMP/paused-checkpoint.log" 2>&1 &
CHECKPOINT=$!
attempt=0
while [ ! -f "$TMP/source/pg_wal/test-paused-after-checkpoint-wal-flush" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200
    kill -0 "$SOURCE"; sleep 0.05
done
# The candidate record is durable, but its pages have not been installed yet.
probe --expect "$TMP/old.json" >"$TMP/paused-copy.json"
kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=
if wait "$CHECKPOINT"; then exit 1; fi
CHECKPOINT=
start_source
test "$(sql "$PORT" 'SELECT count(*) FROM cp_data')" = 2
probe --expect "$TMP/old.json" >"$TMP/restarted-old-copy.json"
sql "$PORT" CHECKPOINT
probe --save "$TMP/new.json" >"$TMP/published-copy.json"
if cmp -s "$TMP/old.json" "$TMP/new.json"; then exit 1; fi

# The opposite crash boundary: new copy is published, recycling has not run.
sql "$PORT" 'INSERT INTO cp_data VALUES (3)'
checked source '(setf *wal-test-pause-at* "after-checkpoint-control")' arm-published
sql "$PORT" CHECKPOINT >"$TMP/published-checkpoint.log" 2>&1 &
CHECKPOINT=$!
attempt=0
while [ ! -f "$TMP/source/pg_wal/test-paused-after-checkpoint-control" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200
    kill -0 "$SOURCE"; sleep 0.05
done
probe --save "$TMP/committed.json" >"$TMP/before-recycle-copy.json"
kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=
if wait "$CHECKPOINT"; then exit 1; fi
CHECKPOINT=
cp "$TMP/source/global/pg_wal_control" "$TMP/saved-wal-control"
probe --corrupt-copy-fixture
if timeout 20 "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" >"$TMP/corrupt-start.log" 2>&1; then exit 1; fi
grep -q 'checkpoint differs from its durable control copy' "$TMP/corrupt-start.log"
cp "$TMP/saved-wal-control" "$TMP/source/global/pg_wal_control"
start_source
test "$(sql "$PORT" 'SELECT count(*) FROM cp_data')" = 3
probe --expect "$TMP/committed.json" >"$TMP/restarted-published-copy.json"

mkdir -m 700 "$TMP/copy"
checked source "(physical-base-backup \"$TMP/copy/\" \"checkpoint_base\")" backup
start_target
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"checkpoint_base\")" follow
sql "$PORT" 'INSERT INTO cp_data VALUES (4); CHECKPOINT'
source_flush=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
source_hex=$(probe --hex)
attempt=0
while [ "$(sql "$TARGET_PORT" "SELECT pg_last_wal_replay_lsn() >= '$source_flush'::pg_lsn")" != t ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200
    kill -0 "$TARGET"; sleep 0.05
done
checked target "(assert (equal (wal-checkpoint-snapshot-hex (getf *physical-recovery-state* :source-checkpoint)) \"$source_hex\"))" replayed-copy
sql "$TARGET_PORT" CHECKPOINT
checked target "(assert (equal (wal-checkpoint-snapshot-hex (getf (physical-restartpoint-read *physical-recovery-state*) :source-checkpoint)) \"$source_hex\"))" restartpoint-copy
kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
start_target
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM cp_data')" = 4
checked target "(assert (equal (wal-checkpoint-snapshot-hex (getf *physical-recovery-state* :source-checkpoint)) \"$source_hex\"))" restored-copy
if sql "$TARGET_PORT" 'INSERT INTO cp_data VALUES (5)' >"$TMP/write-guard.log" 2>&1; then exit 1; fi
checked target '(physical-recovery-prepare-shutdown)' prepare
checked source '(physical-source-pin-release "checkpoint_base")' release
echo 'checkpoint-snapshot: legacy upgrade, both crash boundaries, corrupt copy rejection and replay/restartpoint preservation PASS'
