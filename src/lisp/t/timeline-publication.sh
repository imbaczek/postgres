#!/bin/sh
# C-compatible history publication; no promotion or writer coordination.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PATRONI_PYTHON=${PATRONI_PYTHON:-python3}
PORT=${PORT:-6156}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-timeline-publication.XXXXXX")
SERVER= FIRST= SECOND= PUBLISHER=
cleanup() {
    result=$?
    trap - EXIT
    : >"$TMP/release"
    for pid in "$PUBLISHER" "$FIRST" "$SECOND" "$SERVER"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "timeline-publication: artifacts=$TMP"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$1"
}
start_server() {
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock" >>"$TMP/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
}
restart_server() {
    kill -KILL "$SERVER"; wait "$SERVER" 2>/dev/null || :; SERVER=
    start_server
}
publish() {
    PG_HISTORY_DATA="$TMP/data" PG_HISTORY_CHILD=$1 PG_HISTORY_PARENT=$2 PG_HISTORY_FORK=$3 \
        PG_HISTORY_REASON=$4 PG_HISTORY_POINT=${5:-} PG_HISTORY_MARKER="$TMP/paused" \
        PG_HISTORY_RELEASE="$TMP/release" "$SBCL" --script "$BASE/t/timeline-publication-worker.lisp"
}
wait_marker() {
    attempt=0
    while [ ! -f "$TMP/paused" ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 300
        kill -0 "$FIRST"; sleep 0.05
    done
    PUBLISHER=$(cat "$TMP/paused.pid")
    python3 - "$FIRST" "$PUBLISHER" "$BASE/t/timeline-publication-worker.lisp" "$TMP/data" <<'PY'
import sys
from pathlib import Path
launcher, publisher = map(int, sys.argv[1:3])
root = Path('/proc') / str(publisher)
assert sys.argv[3].encode() in (root / 'cmdline').read_bytes().split(b'\0')
assert b'PG_HISTORY_DATA=' + sys.argv[4].encode() in (root / 'environ').read_bytes().split(b'\0')
parent = next(int(line.split()[1]) for line in (root / 'status').read_text().splitlines() if line.startswith('PPid:'))
assert publisher == launcher or parent == launcher, (launcher, publisher, parent)
PY
}
probe() { "$PATRONI_PYTHON" "$BASE/t/interop/timeline-publication.py" "$TMP/data" "$PORT" "$@"; }
start_server
sql 'CREATE TABLE history_rows (id integer); INSERT INTO history_rows VALUES (1)'
# Reload the actual SQL image and use its owning control endpoint as well.
printf '(pg::reload)\n(quit)\n' | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reload.log"
if grep -q ';; error:' "$TMP/reload.log"; then exit 1; fi

publish 2 1 64 first AFTER-FILE-FSYNC >"$TMP/before-install.log" 2>&1 &
FIRST=$!; wait_marker
kill -KILL "$PUBLISHER"; wait "$FIRST" 2>/dev/null || :; FIRST=; PUBLISHER=
restart_server
probe 2 missing
publish 2 1 64 first >"$TMP/first.log" 2>&1
probe 2 first

for phase in AFTER-INSTALL AFTER-DIRECTORY-FSYNC; do
    rm -f "$TMP/paused" "$TMP/release"
    if [ "$phase" = AFTER-INSTALL ]; then child=3; parent=2; fork=128; else child=4; parent=3; fork=192; fi
    publish "$child" "$parent" "$fork" "$phase" "$phase" >"$TMP/$phase.log" 2>&1 &
    FIRST=$!; wait_marker
    kill -KILL "$PUBLISHER"; wait "$FIRST" 2>/dev/null || :; FIRST=; PUBLISHER=
    restart_server
    probe "$child" "$phase"
done

# Two process-local mutexes cannot serialize these independent publishers.
rm -f "$TMP/paused" "$TMP/release"
publish 5 4 256 loser AFTER-FILE-FSYNC >"$TMP/loser.log" 2>&1 &
FIRST=$!; wait_marker
publish 5 4 256 winner >"$TMP/winner.log" 2>&1 &
SECOND=$!; wait "$SECOND"; SECOND=
cp "$TMP/data/pg_wal/00000005.history" "$TMP/winner.history"
: >"$TMP/release"
status=0; wait "$FIRST" || status=$?; FIRST=; PUBLISHER=
test "$status" -eq 1
cmp "$TMP/winner.history" "$TMP/data/pg_wal/00000005.history"
probe 5 winner

# An existing dangling symlink is an existing name, too.
ln -s missing-history-target "$TMP/data/pg_wal/00000006.history"
if publish 6 5 320 forbidden >"$TMP/symlink.log" 2>&1; then exit 1; fi
test "$(readlink "$TMP/data/pg_wal/00000006.history")" = missing-history-target
test ! -e "$TMP/data/pg_wal/missing-history-target"
echo 'timeline-publication: crash boundaries, concurrent no-clobber, exact wire bytes and Patroni parsing PASS'
