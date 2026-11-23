#!/bin/sh
# Native source pin survives checkpoints and SIGKILL while SQL remains correct.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PSQL=${PSQL:-psql}
PORT=${PORT:-5864}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-source.XXXXXX")
SRV=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$SRV" ]; then
        kill -INT "$SRV" 2>/dev/null || :
        wait "$SRV" 2>/dev/null || :
    fi
    echo "physical-source: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres \
        -d postgres -qAt -v ON_ERROR_STOP=1 "$@"
}
start() {
    test_launch_lisp_server "$PORT" "$TMP/data" "$TMP/control.sock" "$TMP/server.log"
    SRV=$!
    test_wait_owned_sql_server "$SRV" "$PORT" "$PSQL"
}
control() {
    test_lisp_control "$TMP/control.sock" "$1"
}
start
sql -c 'CREATE TABLE physical_pin_fixture (id integer PRIMARY KEY, payload varchar(120)); CHECKPOINT;'
control '(let ((pin (pg::physical-source-pin-create "native_bootstrap"))) (assert (probe-file (pg::physical-source-pin-path "native_bootstrap"))) (assert (= (pg::physical-source-pin-consistent-lsn pin) pg::*wal-checkpoint-lsn*)) (format t "PIN-CREATED~%"))' >"$TMP/create.log"
grep -q 'PIN-CREATED$' "$TMP/create.log"
python3 - "$TMP/writes.sql" <<'PY'
import sys
with open(sys.argv[1], "w") as stream:
    for index in range(1800):
        stream.write("INSERT INTO physical_pin_fixture VALUES (%d, '%s');\n" % (index, "x" * 100))
    stream.write("CHECKPOINT;\n")
PY
sql -f "$TMP/writes.sql" >"$TMP/writes.log"
control '(let* ((pin (gethash "native_bootstrap" pg::*physical-source-pins*)) (segment (floor (pg::physical-source-pin-restart-lsn pin) pg::+wal-segment-size+))) (assert (> (floor pg::*wal-checkpoint-lsn* pg::+wal-segment-size+) segment)) (assert (probe-file (pg::wal-segment-path segment))) (format t "PIN-RETAINED~%"))' >"$TMP/retained.log"
grep -q 'PIN-RETAINED$' "$TMP/retained.log"
sql -c 'SELECT id, payload FROM physical_pin_fixture ORDER BY id;' >"$TMP/rows.expected"
kill -KILL "$SRV"
wait "$SRV" 2>/dev/null || :
SRV=
start
test "$(sql -c 'SELECT count(*) FROM physical_pin_fixture;')" = 1800
sql -c 'SELECT id, payload FROM physical_pin_fixture ORDER BY id;' >"$TMP/rows.restarted"
diff -u "$TMP/rows.expected" "$TMP/rows.restarted"
control '(let* ((pin (gethash "native_bootstrap" pg::*physical-source-pins*)) (path (pg::wal-segment-path (floor (pg::physical-source-pin-restart-lsn pin) pg::+wal-segment-size+)))) (assert pin) (assert (gethash pin pg::*wal-snapshot-retention-pins*)) (assert (probe-file path)) (pg::physical-source-pin-release "native_bootstrap") (pg::wal-checkpoint) (assert (not (probe-file path))) (format t "PIN-RESTORED-AND-RELEASED~%"))' >"$TMP/restart.log"
grep -q 'PIN-RESTORED-AND-RELEASED$' "$TMP/restart.log"
test "$(sql -c 'SELECT count(*) FROM physical_pin_fixture;')" = 1800
sql -c 'SELECT id, payload FROM physical_pin_fixture ORDER BY id;' >"$TMP/rows.released"
diff -u "$TMP/rows.expected" "$TMP/rows.released"
control '(progn (pg::physical-source-pin-create "retirement_probe") (format t "RETIREMENT-PIN-CREATED~%"))' \
    >"$TMP/retirement-create.log"
grep -q 'RETIREMENT-PIN-CREATED$' "$TMP/retirement-create.log"
kill -INT "$SRV"
wait "$SRV"
SRV=
pin="$TMP/data/pg_physical_pins/retirement_probe.pin"
cp -p "$pin" "$TMP/current-pin"
PYTHONPATH="$BASE/t/interop${PYTHONPATH:+:$PYTHONPATH}" python3 - "$TMP" <<'PY'
import sys
from pathlib import Path

from crc32c_reference import crc32c

root = Path(sys.argv[1])
line = (root / "current-pin").read_bytes()
assert line.endswith(b"\n")
prefix, checksum = line[:-1].rsplit(b" ", 1)
fields = prefix.split()
assert len(fields) == 9 and fields[:2] == [b"PGPHYSPIN2", b"retirement_probe"]
assert fields[3] == b"1" and fields[6] == b"-1" and fields[8] == b"reserved"
assert int(checksum, 16) == crc32c(prefix)
legacy = b" ".join([b"PGPHYSPIN1", *fields[1:6]])
(root / "retired-pin").write_bytes(legacy + f" {crc32c(legacy):08X}\n".encode())
PY
cp "$TMP/retired-pin" "$pin"
result=0
timeout 30 "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" >"$TMP/retired-pin.log" 2>&1 || result=$?
test "$result" -eq 1
grep -Fq "corrupt or foreign physical source pin $pin" "$TMP/retired-pin.log"
cmp "$TMP/retired-pin" "$pin"
cp -p "$TMP/current-pin" "$pin"
start
control '(progn (assert (gethash "retirement_probe" pg::*physical-source-pins*)) (format t "RETIREMENT-PIN-RESTORED~%"))' \
    >"$TMP/retirement-restored.log"
grep -q 'RETIREMENT-PIN-RESTORED$' "$TMP/retirement-restored.log"
sql -c 'SELECT id, payload FROM physical_pin_fixture ORDER BY id;' >"$TMP/rows.restored-pin"
diff -u "$TMP/rows.expected" "$TMP/rows.restored-pin"
kill -INT "$SRV"
wait "$SRV"
SRV=
printf 'corrupt physical bootstrap pin\n' >"$TMP/data/pg_physical_pins/rejected.pin"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" >"$TMP/rejected.log" 2>&1 &
SRV=$!
if wait "$SRV"; then
    SRV=
    echo 'physical-source: corrupt pin unexpectedly admitted startup' >&2
    exit 1
fi
SRV=
grep -q 'corrupt or foreign physical source pin' "$TMP/rejected.log"
echo 'physical-source: PASS'
