#!/bin/sh
# Crash and restart the server at WAL batch, page staging, B-tree split, and
# checkpoint boundaries. Each phase reuses the same PGDATA.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5462}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-wal-crash.XXXXXX")
PGDATA="$TMP/data"
LOG="$TMP/server.log"
SRV=
. "$BASE/t/test-support.sh"
test_debug_setup wal-crash-recovery "$PORT" "$TMP" "$LOG"

cleanup() {
    if [ -n "${SRV:-}" ]; then
        kill "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

start_server() {
    pause_at=${1:-}
    if [ -n "$pause_at" ]; then
        rm -f "$PGDATA/pg_wal/test-paused-$pause_at"
        PG_WAL_TEST_PAUSE_AT="$pause_at" "$BASE/pg-lisp.sh" \
            -p "$PORT" -D "$PGDATA" >> "$LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" >> "$LOG" 2>&1 &
    fi
    SRV=$!

    i=0
    while [ "$i" -lt 150 ]; do
        if PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres \
                -d postgres -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SRV" 2>/dev/null; then
            echo "wal-crash-recovery: server exited during startup" >&2
            cat "$LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.2
    done

    echo "wal-crash-recovery: server did not start" >&2
    cat "$LOG" >&2
    return 1
}

stop_server() {
    kill -TERM "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    SRV=
}

crash_server() {
    kill -KILL "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    SRV=
}

wait_for_pause() {
    point=$1
    marker="$PGDATA/pg_wal/test-paused-$point"
    i=0
    while [ "$i" -lt 300 ]; do
        if [ -f "$marker" ]; then
            return 0
        fi
        if ! kill -0 "$SRV" 2>/dev/null; then
            echo "wal-crash-recovery: server exited before $point" >&2
            cat "$LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "wal-crash-recovery: timed out waiting for $point" >&2
    return 1
}

psql_db() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres \
        -d postgres -v ON_ERROR_STOP=1 "$@"
}

row_count() {
    psql_db -A -t -c 'SELECT id FROM wal_rows ORDER BY id' | wc -l | tr -d ' '
}

start_server
psql_db -c 'CREATE TABLE wal_rows (id int primary key)'
stop_server

# Kill after commit WAL is durable but before CLOG and relation pages are
# updated. Recovery must replay both and preserve the committed row.
start_server after-wal-flush
psql_db -c 'INSERT INTO wal_rows VALUES (1)' > "$TMP/commit.out" 2>&1 &
CLIENT=$!
wait_for_pause after-wal-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 1')" = 1 ]
psql_db -c 'INSERT INTO wal_rows VALUES (2)'
stop_server

# If WAL reaches disk before the control-file durable LSN advances, startup
# adopts and syncs the complete tail before replaying it.
start_server after-wal-bytes-flush
psql_db -c 'INSERT INTO wal_rows VALUES (5)' > "$TMP/control-boundary.out" 2>&1 &
CLIENT=$!
wait_for_pause after-wal-bytes-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 5')" = 5 ]
[ "$(row_count)" -eq 3 ]
stop_server

# A complete page-image record without its commit terminator must be ignored.
start_server after-page-images
psql_db -c 'INSERT INTO wal_rows VALUES (4)' > "$TMP/incomplete-batch.out" 2>&1 &
CLIENT=$!
wait_for_pause after-page-images
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(row_count)" -eq 3 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 4')" ]
stop_server

# Stop after a heap page is staged by an open transaction, before any commit
# record exists. Full and short trailing records test unflushed-tail handling.
start_server after-page-stage
psql_db -c 'BEGIN; INSERT INTO wal_rows VALUES (3)' > "$TMP/uncommitted.out" 2>&1 &
CLIENT=$!
wait_for_pause after-page-stage
crash_server
wait "$CLIENT" 2>/dev/null || true
python3 - "$PGDATA/pg_wal" <<'PY'
import glob
import os
import struct
import sys

segments = sorted(glob.glob(sys.argv[1] + "/*.wal"))
if not segments:
    raise SystemExit("no WAL segment found for tail-recovery check")
path = segments[-1]
segment = int(os.path.basename(path)[:-4], 16)
with open(path, "ab") as stream:
    lsn = segment * (16 * 1024 * 1024) + stream.tell()
    record = bytearray(32)
    record[:4] = b"PGWL"
    struct.pack_into("<HHIQ", record, 4, 2, 6, 64, lsn)
    stream.write(record)
PY

start_server
[ "$(row_count)" -eq 3 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 3')" ]
stop_server

python3 - "$PGDATA/pg_wal" <<'PY'
import glob
import sys

segments = sorted(glob.glob(sys.argv[1] + "/*.wal"))
with open(segments[-1], "ab") as stream:
    stream.write(b"PGWLpartial")
PY

start_server
[ "$(row_count)" -eq 3 ]
stop_server

# A committed UPDATE must recover its replacement tuple and index key. The old
# unique key can be reused after restart, while the new key remains protected.
start_server after-wal-flush
psql_db -c 'UPDATE wal_rows SET id = 10 WHERE id = 1' > "$TMP/update.out" 2>&1 &
CLIENT=$!
wait_for_pause after-wal-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 10')" = 10 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 1')" ]
if psql_db -c 'INSERT INTO wal_rows VALUES (10)' > /dev/null 2>&1; then
    echo "wal-crash-recovery: recovered UPDATE lost the unique index entry" >&2
    exit 1
fi
psql_db -c 'INSERT INTO wal_rows VALUES (1)'
stop_server

# A partially staged UPDATE from an uncommitted transaction must leave the old
# tuple visible and its replacement invisible after restart.
start_server after-page-stage
psql_db -c 'BEGIN; UPDATE wal_rows SET id = 11 WHERE id = 10' > "$TMP/uncommitted-update.out" 2>&1 &
CLIENT=$!
wait_for_pause after-page-stage
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 10')" = 10 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 11')" ]
[ "$(row_count)" -eq 4 ]
stop_server

# A complete UPDATE after-image set without COMMIT must also be discarded.
start_server after-page-images
psql_db -c 'BEGIN; UPDATE wal_rows SET id = 12 WHERE id = 10; COMMIT' \
    > "$TMP/incomplete-update-batch.out" 2>&1 &
CLIENT=$!
wait_for_pause after-page-images
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 10')" = 10 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 12')" ]
[ "$(row_count)" -eq 4 ]
stop_server

# The one-transaction batch crosses a leaf split. Kill after the first data
# page write; replay must restore the whole split and every committed key.
python3 - "$TMP/split.sql" <<'PY'
import sys

with open(sys.argv[1], "w") as stream:
    stream.write("INSERT INTO wal_rows VALUES ")
    stream.write(",".join("(%d)" % value for value in range(100, 1100)))
    stream.write(";\n")
PY
start_server after-btree-split-page-write
psql_db -f "$TMP/split.sql" > "$TMP/split.out" 2>&1 &
CLIENT=$!
wait_for_pause after-btree-split-page-write
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(row_count)" -eq 1004 ]
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 1099')" = 1099 ]
if psql_db -c 'INSERT INTO wal_rows VALUES (1099)' > /dev/null 2>&1; then
    echo "wal-crash-recovery: recovered B-tree split lost a unique key" >&2
    exit 1
fi
psql_db -c 'INSERT INTO wal_rows VALUES (1200)'
stop_server

# Crash once before checkpoint data writes and once after the checkpoint
# pointer is durable. Both restart paths must retain the committed rows.
start_server after-checkpoint-wal-flush
psql_db -c 'CHECKPOINT' > "$TMP/checkpoint-before.out" 2>&1 &
CLIENT=$!
wait_for_pause after-checkpoint-wal-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(row_count)" -eq 1005 ]
stop_server

start_server after-checkpoint-control
psql_db -c 'CHECKPOINT' > "$TMP/checkpoint-after.out" 2>&1 &
CLIENT=$!
wait_for_pause after-checkpoint-control
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(row_count)" -eq 1005 ]
psql_db -c 'INSERT INTO wal_rows VALUES (1100)'
stop_server

expect_bad_wal_startup() {
    bad_data=$1
    expected_message=$2
    error_log=$3
    if timeout 5 "$BASE/pg-lisp.sh" -p "$PORT" -D "$bad_data" > "$error_log" 2>&1; then
        echo "wal-crash-recovery: startup accepted corrupt WAL in $bad_data" >&2
        exit 1
    fi
    if ! grep -Fq "$expected_message" "$error_log"; then
        echo "wal-crash-recovery: startup failed for the wrong reason" >&2
        cat "$error_log" >&2
        exit 1
    fi
}

corrupt_wal_record() {
    pg_wal=$1
    mode=$2
    python3 - "$pg_wal" "$mode" <<'PY'
import glob
import os
import struct
import sys

segments = sorted(glob.glob(sys.argv[1] + "/*.wal"))
records = []
for path in segments:
    data = bytearray(open(path, "rb").read())
    offset = 0
    while offset + 32 <= len(data):
        if data[offset:offset + 4] != b"PGWL":
            break
        length = struct.unpack_from("<I", data, offset + 8)[0]
        if length < 32 or offset + length > len(data):
            break
        records.append((path, offset, length, data))
        offset += length

mode = sys.argv[2]
if mode == "interior-length":
    path, offset, _, data = records[0]
    struct.pack_into("<I", data, offset + 8, 0)
elif mode in ("final-length", "final-inflated", "final-checksum"):
    path, offset, length, data = records[-1]
    record_type = struct.unpack_from("<H", data, offset + 6)[0]
    if length != 32 or record_type not in (2, 3, 4, 5):
        raise SystemExit("last durable record is not a terminal record")
    if mode == "final-length":
        struct.pack_into("<I", data, offset + 8, 0)
    elif mode == "final-inflated":
        struct.pack_into("<I", data, offset + 8, 4096)
    else:
        data[offset + 28] ^= 1
else:
    raise SystemExit("unknown corruption mode: " + mode)

with open(path, "wb") as stream:
    stream.write(data)
PY
}

# Corruption in the durable prefix must never be mistaken for a torn tail.
cp -a "$PGDATA" "$TMP/bad-interior"
corrupt_wal_record "$TMP/bad-interior/pg_wal" interior-length
expect_bad_wal_startup "$TMP/bad-interior" 'invalid WAL record length' "$TMP/bad-interior.log"

cp -a "$PGDATA" "$TMP/bad-final-length"
corrupt_wal_record "$TMP/bad-final-length/pg_wal" final-length
expect_bad_wal_startup "$TMP/bad-final-length" 'invalid WAL record length' "$TMP/bad-final-length.log"

cp -a "$PGDATA" "$TMP/bad-inflated"
corrupt_wal_record "$TMP/bad-inflated/pg_wal" final-inflated
expect_bad_wal_startup "$TMP/bad-inflated" 'incomplete durable WAL record' "$TMP/bad-inflated.log"

cp -a "$PGDATA" "$TMP/bad-checksum"
corrupt_wal_record "$TMP/bad-checksum/pg_wal" final-checksum
expect_bad_wal_startup "$TMP/bad-checksum" 'WAL checksum mismatch' "$TMP/bad-checksum.log"

echo "wal-crash-recovery: PASS"
