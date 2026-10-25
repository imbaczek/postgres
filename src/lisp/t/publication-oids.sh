#!/bin/sh
# Legacy publication identities are persisted before SQL admission and recovery.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT_BASE:-5685}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-publication-oids.XXXXXX")
DATA=$TMP/data
SERVER_PID=

sql() {
    PGCONNECT_TIMEOUT=2 psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' -c "$1"
}

stop_server() {
    [ -n "$SERVER_PID" ] || return 0
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=
}

start_server() {
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$DATA" > "$TMP/$1.log" 2>&1 &
    SERVER_PID=$!
    attempt=0
    while [ "$attempt" -lt 200 ]; do
        if sql '' >/dev/null 2>&1; then
            return 0
        fi
        kill -0 "$SERVER_PID" 2>/dev/null || return 1
        attempt=$((attempt + 1))
        sleep 0.1
    done
    return 1
}

on_exit() {
    status=$?
    trap - EXIT
    stop_server
    if [ "$status" -eq 0 ]; then
        rm -rf "$TMP"
    else
        echo "publication-oids: FAIL; retained artifacts: $TMP" >&2
    fi
    exit "$status"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Serialized catalog data from the format preceding publication OIDs. Deliberate
# reverse publication order checks that upgrade assigns identities by name.
mkdir -p "$DATA/global"
printf '3\n' > "$DATA/global/pg_control"
printf '%s\n' '(:cluster-identity "legacy-publication-oid-test" :next-oid 30000
 :databases ((:name "postgres" :oid 16384 :tables nil :indexes nil :sequences nil
 :publications ((:name "legacy_b" :all-tables nil :tables nil :sequences nil :generation 0)
                (:name "legacy_a" :all-tables nil :tables nil :sequences nil :generation 0)))))' \
    > "$DATA/global/pg_catalog.dat"

start_server upgrade
expected=$(printf 'legacy_a|30000\nlegacy_b|30001')
test "$(sql 'SELECT pubname,oid FROM pg_catalog.pg_publication ORDER BY pubname')" = "$expected"
# No intervening catalog-writing SQL: kill after read-only observation, proving
# startup itself persisted the migration rather than relying on shutdown.
kill -KILL "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=

start_server recovered
test "$(sql 'SELECT pubname,oid FROM pg_catalog.pg_publication ORDER BY pubname')" = "$expected"
sql 'ALTER PUBLICATION legacy_a SET (publish = '\''insert'\'')' >/dev/null
test "$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='legacy_a'")" = 30000
sql 'CREATE PUBLICATION fresh_publication FOR ALL TABLES' >/dev/null
fresh=$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='fresh_publication'")
test "$fresh" -eq 30002
stop_server

start_server restarted
test "$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='fresh_publication'")" = "$fresh"
sql 'DROP PUBLICATION fresh_publication; CREATE PUBLICATION fresh_publication FOR ALL TABLES' >/dev/null
replacement=$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='fresh_publication'")
test "$replacement" -gt "$fresh"
echo 'publication-oids: PASS (legacy upgrade, crash recovery, restart, replacement)'
