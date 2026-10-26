#!/bin/sh
# Pinned C-to-C state oracle; never feeds C physical WAL to pg-lisp.
set -eu
export LC_ALL=C
: "${C_PG_BINDIR:?set C_PG_BINDIR to the pinned C installation bin directory}"
: "${C_PG_REVISION:?set C_PG_REVISION to the exact C build revision}"
C=$C_PG_BINDIR
SOURCE_PORT=${PORT_BASE:-6026}
TARGET_PORT=$((SOURCE_PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-control-state-c.XXXXXX")
echo "C recovery-control revision=$C_PG_REVISION"
SOURCE_STARTED=no TARGET_STARTED=no
cleanup() {
    status=$?
    trap - EXIT
    if [ "$TARGET_STARTED" = yes ]; then "$C/pg_ctl" -D "$TMP/standby" -m immediate -w stop >/dev/null 2>&1 || :; fi
    if [ "$SOURCE_STARTED" = yes ]; then "$C/pg_ctl" -D "$TMP/source" -m fast -w stop >/dev/null 2>&1 || :; fi
    echo "C recovery-control artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
"$C/postgres" --version
"$C/initdb" -D "$TMP/source" -A trust -U postgres --no-locale --encoding=UTF8 >"$TMP/initdb.log"
"$C/pg_ctl" -D "$TMP/source" -l "$TMP/source.log" -o "-p $SOURCE_PORT -h 127.0.0.1 -c unix_socket_directories=$TMP -c max_wal_senders=8" -w start >/dev/null
SOURCE_STARTED=yes
"$C/pg_basebackup" -h 127.0.0.1 -p $SOURCE_PORT -U postgres -D "$TMP/standby" -R -X stream -c fast
for attempt in 1 2; do
    "$C/pg_ctl" -D "$TMP/standby" -l "$TMP/standby.log" -o "-p $TARGET_PORT -h 127.0.0.1 -c unix_socket_directories=$TMP" -w start >/dev/null
    TARGET_STARTED=yes
    test "$("$C/psql" -X -h 127.0.0.1 -p $TARGET_PORT -U postgres -d postgres -qAt -c 'select pg_is_in_recovery()')" = t
    "$C/pg_controldata" "$TMP/standby" >"$TMP/active-$attempt.txt"
    grep -Eq '^Database cluster state: +in archive recovery$' "$TMP/active-$attempt.txt"
    "$C/pg_ctl" -D "$TMP/standby" -m smart -w stop >/dev/null
    TARGET_STARTED=no
    "$C/pg_controldata" "$TMP/standby" >"$TMP/clean-$attempt.txt"
    grep -Eq '^Database cluster state: +shut down in recovery$' "$TMP/clean-$attempt.txt"
done
point1=$(awk -F ': *' '/^Latest checkpoint location:/ {print $2}' "$TMP/clean-1.txt")
point2=$(awk -F ': *' '/^Latest checkpoint location:/ {print $2}' "$TMP/clean-2.txt")
test -n "$point1"
test "$point1" = "$point2"
echo "C recovery-control: active/clean transitions and unchanged checkpoint $point1 PASS"
