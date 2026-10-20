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

# A DDL catalog record must restore both metadata and index pages if the server
# exits after the WAL flush and before the catalog image is written.
start_server after-catalog-wal-flush
psql_db -c 'CREATE TABLE wal_catalog_created (id int primary key)' \
    > "$TMP/catalog-create.out" 2>&1 &
CLIENT=$!
wait_for_pause after-catalog-wal-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
psql_db -c 'INSERT INTO wal_catalog_created VALUES (7)'
[ "$(psql_db -A -t -c 'SELECT id FROM wal_catalog_created WHERE id = 7')" = 7 ]
stop_server

# Relation-file deletion is deferred until its catalog image is durable and
# replayed as part of that same record after an unclean exit.
start_server after-catalog-wal-flush
psql_db -c 'DROP TABLE wal_catalog_created' > "$TMP/catalog-drop.out" 2>&1 &
CLIENT=$!
wait_for_pause after-catalog-wal-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
if psql_db -c 'SELECT * FROM wal_catalog_created' > /dev/null 2>&1; then
    echo "wal-crash-recovery: dropped table returned after recovery" >&2
    exit 1
fi
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
# The client received success. A hard server exit before checkpoint must still
# preserve this committed INSERT and a subsequently acknowledged UPDATE.
crash_server

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 2')" = 2 ]
psql_db -c 'UPDATE wal_rows SET id = 6 WHERE id = 2'
crash_server

start_server
[ "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 6')" = 6 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 2')" ]
stop_server

# A durable DELETE must replay after a crash before the heap and index pages
# have been written. Reinsert the row afterward to preserve later row counts.
start_server after-wal-flush
psql_db -c 'DELETE FROM wal_rows WHERE id = 6' > "$TMP/delete.out" 2>&1 &
CLIENT=$!
wait_for_pause after-wal-flush
crash_server
wait "$CLIENT" 2>/dev/null || true

start_server
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 6')" ]
psql_db -c 'INSERT INTO wal_rows VALUES (6)'
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

# A checkpoint may flush another transaction's staged page images. CLOG must
# still keep that row invisible after the active transaction is lost in a crash.
start_server
mkfifo "$TMP/open-transaction.sql"
psql_db < "$TMP/open-transaction.sql" > "$TMP/open-transaction.out" 2>&1 &
CLIENT=$!
exec 3> "$TMP/open-transaction.sql"
printf 'BEGIN;\nINSERT INTO wal_rows VALUES (13);\n\\! touch %s\n' \
    "$TMP/open-transaction-staged" >&3
i=0
while [ ! -f "$TMP/open-transaction-staged" ]; do
    if ! kill -0 "$CLIENT" 2>/dev/null; then
        echo "wal-crash-recovery: open transaction exited before staging" >&2
        cat "$TMP/open-transaction.out" >&2
        exit 1
    fi
    i=$((i + 1))
    if [ "$i" -ge 100 ]; then
        echo "wal-crash-recovery: timed out waiting for staged transaction" >&2
        exit 1
    fi
    sleep 0.1
done
psql_db -c 'CHECKPOINT'
crash_server
exec 3>&-
wait "$CLIENT" 2>/dev/null || true

start_server
[ "$(row_count)" -eq 4 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 13')" ]
stop_server

# Also crash after the checkpoint record is durable but before its page images
# are written. Recovery must replay the checkpoint batch without exposing the
# still-uncommitted transaction that supplied one of those images.
start_server after-checkpoint-wal-flush
mkfifo "$TMP/open-checkpoint-transaction.sql"
psql_db < "$TMP/open-checkpoint-transaction.sql" \
    > "$TMP/open-checkpoint-transaction.out" 2>&1 &
CLIENT=$!
exec 3> "$TMP/open-checkpoint-transaction.sql"
printf 'BEGIN;\nINSERT INTO wal_rows VALUES (14);\n\\! touch %s\n' \
    "$TMP/open-checkpoint-transaction-staged" >&3
i=0
while [ ! -f "$TMP/open-checkpoint-transaction-staged" ]; do
    if ! kill -0 "$CLIENT" 2>/dev/null; then
        echo "wal-crash-recovery: checkpoint transaction exited before staging" >&2
        cat "$TMP/open-checkpoint-transaction.out" >&2
        exit 1
    fi
    i=$((i + 1))
    if [ "$i" -ge 100 ]; then
        echo "wal-crash-recovery: timed out waiting for checkpoint transaction" >&2
        exit 1
    fi
    sleep 0.1
done
psql_db -c 'CHECKPOINT' > "$TMP/active-checkpoint.out" 2>&1 &
CHECKPOINT_CLIENT=$!
wait_for_pause after-checkpoint-wal-flush
crash_server
exec 3>&-
wait "$CLIENT" 2>/dev/null || true
wait "$CHECKPOINT_CLIENT" 2>/dev/null || true

# Record the expected heap-page LSN from the checkpoint's own image records.
# This makes the recovery assertion prove those images were replayed, not just
# that an earlier INSERT record happened to restore the committed rows.
python3 - "$PGDATA/pg_wal" "$PGDATA/global/pg_catalog.dat" \
    > "$TMP/checkpoint-heap-pages.txt" <<'PY'
import glob
import os
import re
import struct
import sys

segment_size = 16 * 1024 * 1024
page_size = 8192
catalog = open(sys.argv[2], encoding="utf-8").read()
match = re.search(r'wal_rows"\s+:relfilenode\s+(\d+)', catalog, re.IGNORECASE)
if not match:
    raise SystemExit("could not find wal_rows in the serialized catalog")
heap_relfilenode = int(match.group(1))
checkpoint_images = {}
group_images = None

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.wal"))):
    segment = int(os.path.basename(path)[:16], 16)
    data = open(path, "rb").read()
    offset = 0
    while offset + 32 <= len(data):
        if data[offset:offset + 4] != b"PGWL":
            raise SystemExit("invalid WAL while inspecting checkpoint images")
        record_type = struct.unpack_from("<H", data, offset + 6)[0]
        length = struct.unpack_from("<I", data, offset + 8)[0]
        if length < 32 or offset + length > len(data):
            raise SystemExit("truncated WAL while inspecting checkpoint images")
        payload = offset + 32
        if record_type == 6:
            group_images = {}
        elif record_type == 1 and group_images is not None:
            count = struct.unpack_from("<I", data, payload)[0]
            expected = 32 + 4 + count * (8 + page_size)
            if length != expected:
                raise SystemExit("invalid page-image length in checkpoint WAL")
            item = payload + 4
            for _ in range(count):
                relfilenode, block = struct.unpack_from("<II", data, item)
                page = item + 8
                if relfilenode == heap_relfilenode:
                    page_lsn = struct.unpack_from("<Q", data, page)[0]
                    group_images[block] = page_lsn
                item += 8 + page_size
        elif record_type == 4 and group_images is not None:
            checkpoint_images = group_images
            group_images = None
        offset += length

if not checkpoint_images:
    raise SystemExit("checkpoint WAL contains no image for the wal_rows heap")
for block, page_lsn in sorted(checkpoint_images.items()):
    print(block, page_lsn)
PY

start_server
python3 - "$PGDATA/base" "$PGDATA/global/pg_catalog.dat" \
    "$TMP/checkpoint-heap-pages.txt" <<'PY'
import re
import sys

page_size = 8192
catalog = open(sys.argv[2], encoding="utf-8").read()
match = re.search(r'wal_rows"\s+:relfilenode\s+(\d+)', catalog, re.IGNORECASE)
if not match:
    raise SystemExit("could not find wal_rows in the recovered catalog")
relfilenode = match.group(1)
with open(sys.argv[1] + "/" + relfilenode, "rb") as relation:
    for line in open(sys.argv[3], encoding="utf-8"):
        block, expected_lsn = map(int, line.split())
        relation.seek(block * page_size)
        page = relation.read(page_size)
        if len(page) != page_size:
            raise SystemExit("checkpoint heap page is missing after recovery")
        actual_lsn = int.from_bytes(page[:8], "little")
        if actual_lsn != expected_lsn:
            raise SystemExit("checkpoint heap image was not replayed")
PY
[ "$(row_count)" -eq 4 ]
[ -z "$(psql_db -A -t -c 'SELECT id FROM wal_rows WHERE id = 14')" ]
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
INDEX_REL=$(python3 - "$PGDATA/global/pg_catalog.dat" <<'PY'
import re
import sys

catalog = open(sys.argv[1], encoding="utf-8").read()
match = re.search(
    r'wal_rows_pkey"\)?\s+:relfilenode\s+(\d+)',
    catalog,
    re.IGNORECASE,
)
if not match:
    raise SystemExit("could not find wal_rows_pkey in the serialized catalog")
print(match.group(1))
PY
)
start_server after-btree-split-page-write
psql_db -f "$TMP/split.sql" > "$TMP/split.out" 2>&1 &
CLIENT=$!
wait_for_pause after-btree-split-page-write
crash_server
wait "$CLIENT" 2>/dev/null || true

# Damage the B-tree meta page after its committed after-image is durable. WAL
# redo must replace the damaged bytes before normal page reads verify checksum.
python3 - "$PGDATA/base/$INDEX_REL" <<'PY'
import sys

with open(sys.argv[1], "r+b") as stream:
    stream.seek(100)
    original = stream.read(1)
    if len(original) != 1:
        raise SystemExit("B-tree meta page is missing")
    stream.seek(100)
    stream.write(bytes([original[0] ^ 0xFF]))
PY

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
elif mode in ("final-length", "final-inflated", "final-checksum",
              "final-magic", "final-version", "final-lsn", "final-type"):
    path, offset, length, data = records[-1]
    record_type = struct.unpack_from("<H", data, offset + 6)[0]
    if length != 32 or record_type not in (2, 3, 4, 5):
        raise SystemExit("last durable record is not a terminal record")
    if mode == "final-length":
        struct.pack_into("<I", data, offset + 8, 0)
    elif mode == "final-inflated":
        struct.pack_into("<I", data, offset + 8, 4096)
    elif mode == "final-checksum":
        data[offset + 28] ^= 1
    elif mode == "final-magic":
        data[offset] ^= 1
    elif mode == "final-version":
        struct.pack_into("<H", data, offset + 4, 1)
    elif mode == "final-lsn":
        struct.pack_into("<Q", data, offset + 12, 0)
    else:
        struct.pack_into("<H", data, offset + 6, 0xFFFF)
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

for mode in final-magic final-version final-lsn final-type; do
    bad_data="$TMP/bad-$mode"
    cp -a "$PGDATA" "$bad_data"
    corrupt_wal_record "$bad_data/pg_wal" "$mode"
    expect_bad_wal_startup "$bad_data" 'invalid WAL record at LSN' \
        "$bad_data.log"
done

cp -a "$PGDATA" "$TMP/bad-control"
python3 - "$TMP/bad-control/global/pg_wal_control" <<'PY'
import sys

path = sys.argv[1]
with open(path, "r+b") as stream:
    stream.seek(-2, 2)
    byte = stream.read(1)
    stream.seek(-2, 2)
    stream.write(bytes([byte[0] ^ 1]))
PY
expect_bad_wal_startup "$TMP/bad-control" 'WAL control file' \
    "$TMP/bad-control.log"

cp -a "$PGDATA" "$TMP/missing-wal-segment"
python3 - "$TMP/missing-wal-segment/pg_wal" <<'PY'
import glob
import os
import sys

for path in glob.glob(os.path.join(sys.argv[1], "*.wal")):
    os.unlink(path)
PY
expect_bad_wal_startup "$TMP/missing-wal-segment" 'WAL checkpoint LSN' \
    "$TMP/missing-wal-segment.log"

echo "wal-crash-recovery: PASS"
