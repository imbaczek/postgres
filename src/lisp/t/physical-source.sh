#!/bin/sh
# Native source pin survives checkpoints and SIGKILL while SQL remains correct.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
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
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" \
        --control-socket "$TMP/control.sock" >>"$TMP/server.log" 2>&1 &
    SRV=$!
    test_wait_owned_sql_server "$SRV" "$PORT" "$PSQL"
}
control() {
    printf '%s\n(quit)\n' "$1" |
        "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock"
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
