#!/bin/sh
# Publication OIDs and cluster identity are required and persist across recovery.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
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
    test_wait_owned_sql_server "$SERVER_PID" "$PORT" psql
}

catalog_image_checksums() {
    (cd "$DATA" && find . -type f -exec sha256sum {} + | sort)
}

reject_catalog() {
    cp "$TMP/$1" "$DATA/global/pg_catalog.dat"
    catalog_image_checksums >"$TMP/$1.before"
    result=0
    timeout 20 "$BASE/pg-lisp.sh" -p "$PORT" -D "$DATA" >"$TMP/$1.log" 2>&1 || result=$?
    test "$result" -eq 1
    grep -Fq "$2" "$TMP/$1.log"
    catalog_image_checksums >"$TMP/$1.after"
    diff -u "$TMP/$1.before" "$TMP/$1.after"
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

# Seed an old catalog without publication OIDs and verify startup rejects it.
mkdir -m 700 "$DATA"
mkdir "$DATA/global"
# The ownership lock is a persistent empty file, including after failed startup.
(umask 077; : >"$DATA/.pg_lisp.lock")
printf '3\n' > "$DATA/global/pg_control"
: >"$DATA/pg_xact"
printf '%s\n' '(:cluster-identity "legacy-publication-oid-test" :next-oid 30000
 :databases ((:name "postgres" :oid 16384 :tables nil :indexes nil :sequences nil
   :publications ((:name "legacy_b" :all-tables nil :tables nil :sequences nil :generation 0)
                  (:name "legacy_a" :all-tables nil :tables nil :sequences nil :generation 0)))))' \
    > "$DATA/global/pg_catalog.dat"

cp "$DATA/global/pg_catalog.dat" "$TMP/old-catalog"
reject_catalog old-catalog 'invalid or missing publication OID in catalog'
sed -e 's/:next-oid 30000/:next-oid 30002/' \
    -e 's/:name "legacy_a"/:oid 30000 :name "legacy_a"/' \
    -e 's/:name "legacy_b"/:oid 30001 :name "legacy_b"/' \
    "$TMP/old-catalog" >"$TMP/current-catalog"
sed 's/:cluster-identity "legacy-publication-oid-test" //' "$TMP/current-catalog" >"$TMP/missing-identity"
sed 's/:cluster-identity "legacy-publication-oid-test"/:cluster-identity ""/' \
    "$TMP/current-catalog" >"$TMP/empty-identity"
for fixture in missing-identity empty-identity; do
    reject_catalog "$fixture" 'invalid or missing cluster identity in catalog'
done
cp "$TMP/current-catalog" "$DATA/global/pg_catalog.dat"
start_server native
expected=$(printf 'legacy_a|30000\nlegacy_b|30001')
test "$(sql 'SELECT pubname,oid FROM pg_catalog.pg_publication ORDER BY pubname')" = "$expected"
test "$(sql 'SELECT pg_lisp_cluster_identity()')" = legacy-publication-oid-test
# Crash after read-only observation to prove startup retained the stored OIDs.
kill -KILL "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=

start_server recovered
test "$(sql 'SELECT pubname,oid FROM pg_catalog.pg_publication ORDER BY pubname')" = "$expected"
test "$(sql 'SELECT pg_lisp_cluster_identity()')" = legacy-publication-oid-test
sql 'ALTER PUBLICATION legacy_a SET (publish = '\''insert'\'')' >/dev/null
test "$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='legacy_a'")" = 30000
sql 'CREATE PUBLICATION fresh_publication FOR ALL TABLES' >/dev/null
fresh=$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='fresh_publication'")
test "$fresh" -eq 30002
stop_server

start_server restarted
test "$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='fresh_publication'")" = "$fresh"
test "$(sql 'SELECT pg_lisp_cluster_identity()')" = legacy-publication-oid-test
sql 'DROP PUBLICATION fresh_publication; CREATE PUBLICATION fresh_publication FOR ALL TABLES' >/dev/null
replacement=$(sql "SELECT oid FROM pg_catalog.pg_publication WHERE pubname='fresh_publication'")
test "$replacement" -gt "$fresh"
echo 'publication-oids: PASS (catalog rejection, identity preservation, crash recovery, restart, replacement)'
