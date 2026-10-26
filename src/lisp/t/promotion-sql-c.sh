#!/bin/sh
# Pinned C-to-C SQL promotion oracle; both nodes belong to this test.
set -eu
export LC_ALL=C
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
: "${C_PG_BINDIR:?set C_PG_BINDIR to the pinned C installation bin directory}"
: "${C_PG_REVISION:?set C_PG_REVISION to the exact C build revision}"
C=$C_PG_BINDIR
SOURCE_PORT=${PORT_BASE:-6596}
TARGET_PORT=$((SOURCE_PORT + 1))
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-sql-c.XXXXXX")
SOURCE_ATTEMPTED=no TARGET_ATTEMPTED=no
cleanup() {
    status=$?
    trap - EXIT
    if [ "$TARGET_ATTEMPTED" = yes ]; then "$C/pg_ctl" -D "$TMP/standby" -m immediate -w stop >/dev/null 2>&1 || :; fi
    if [ "$SOURCE_ATTEMPTED" = yes ]; then "$C/pg_ctl" -D "$TMP/source" -m fast -w stop >/dev/null 2>&1 || :; fi
    echo "promotion-sql-c: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
echo "promotion-sql-c: C revision supplied by caller=$C_PG_REVISION"
"$C/postgres" --version
"$C/initdb" -D "$TMP/source" -A trust -U postgres --no-locale --encoding=UTF8 >"$TMP/initdb.log"
SOURCE_ATTEMPTED=yes
"$C/pg_ctl" -D "$TMP/source" -l "$TMP/source.log" \
    -o "-p $SOURCE_PORT -h 127.0.0.1 -c unix_socket_directories=$TMP" -w start >/dev/null
"$C/psql" -X -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE TABLE promotion_sql_items (id integer PRIMARY KEY, label text); INSERT INTO promotion_sql_items VALUES (1,'before')" >"$TMP/fixture.log"
"$C/pg_basebackup" -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -D "$TMP/standby" -R -X stream -c fast
TARGET_ATTEMPTED=yes
"$C/pg_ctl" -D "$TMP/standby" -l "$TMP/standby.log" \
    -o "-p $TARGET_PORT -h 127.0.0.1 -c unix_socket_directories=$TMP" -w start >/dev/null
"$PATRONI_PYTHON" "$BASE/t/interop/promotion-sql-contract.py" "$SOURCE_PORT" "$TARGET_PORT"
echo 'promotion-sql-c: PASS'
