#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT=${PORT:-5894}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-capture-spill.XXXXXX")
SERVER= CLIENT=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$CLIENT" "$SERVER"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "logical-capture-spill: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
sql() {
    PGCONNECT_TIMEOUT=2 psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -c "$1"
}
start_server() {
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock" >>"$TMP/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" psql 300
}
wait_for_line() {
    i=0
    until grep -Fxq "$1" "$TMP/client.log"; do
        kill -0 "$CLIENT" 2>/dev/null || { cat "$TMP/client.log" >&2; exit 1; }
        i=$((i + 1)); [ "$i" -lt 300 ] || exit 1; sleep 0.1
    done
}
start_client() {
    rm -f "$TMP/client.fifo" "$TMP/client.log"
    mkfifo "$TMP/client.fifo"
    PGCONNECT_TIMEOUT=2 psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres <"$TMP/client.fifo" >"$TMP/client.log" 2>&1 &
    CLIENT=$!
    exec 7>"$TMP/client.fifo"
}
finish_client() {
    exec 7>&-
    wait "$CLIENT"
    CLIENT=
}
spill_count() {
    (rg --files --hidden --no-ignore "$TMP/data/pgsql_tmp" || :) | wc -l | tr -d ' '
}
start_server
sql 'CREATE TABLE spool_rows (id integer PRIMARY KEY, value text)' >/dev/null
printf '%s\n(quit)\n' '(setf *logical-capture-memory-bytes* 0)' |
    sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/control.log"
if grep -q ';; error:' "$TMP/control.log"; then cat "$TMP/control.log" >&2; exit 1; fi
start_client
printf '%s\n' 'BEGIN;' "INSERT INTO spool_rows VALUES (1, 'first');" "INSERT INTO spool_rows VALUES (2, 'second');" "SELECT 'captured';" >&7
wait_for_line captured
[ "$(spill_count)" = 1 ]
printf '%s\n' 'COMMIT;' '\q' >&7
finish_client
[ "$(spill_count)" = 0 ]
[ "$(sql 'SELECT id,value FROM spool_rows ORDER BY id')" = "$(printf '1|first\n2|second')" ]
start_client
printf '%s\n' 'BEGIN;' "INSERT INTO spool_rows VALUES (3, 'rollback');" "SELECT 'rollback-ready';" >&7
wait_for_line rollback-ready
[ "$(spill_count)" = 1 ]
printf '%s\n' 'ROLLBACK;' '\q' >&7
finish_client
[ "$(spill_count)" = 0 ]
[ "$(sql 'SELECT count(*) FROM spool_rows WHERE id=3')" = 0 ]
start_client
printf '%s\n' 'BEGIN;' "INSERT INTO spool_rows VALUES (4, 'disconnect');" "SELECT 'disconnect-ready';" >&7
wait_for_line disconnect-ready
[ "$(spill_count)" = 1 ]
printf '%s\n' '\q' >&7
finish_client
i=0
until [ "$(spill_count)" = 0 ]; do i=$((i + 1)); [ "$i" -lt 300 ] || exit 1; sleep 0.1; done
[ "$(sql 'SELECT count(*) FROM spool_rows WHERE id=4')" = 0 ]
kill "$SERVER"
wait "$SERVER" 2>/dev/null || :
SERVER=
start_server
[ "$(sql 'SELECT id,value FROM spool_rows ORDER BY id')" = "$(printf '1|first\n2|second')" ]
echo 'logical-capture-spill: PASS'
