#!/bin/sh
# Print configured values without acquiring PGDATA or opening a listener.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-config-output.XXXXXX")
REFERENCE_POSTGRES=${REFERENCE_POSTGRES:-/tmp/pg-lisp-c-reference/usr/local/pgsql/bin/postgres}
trap 'rm -rf "$TMP"' EXIT
mkdir -m 700 "$TMP/empty-data"
cat >"$TMP/server.conf" <<EOF
data_directory = '$TMP/empty-data'
include = 'included.conf'
checkpoint_timeout = '90s'
max_wal_size = '2GB'
wal_keep_size = '1GB'
hot_standby = 'of'
recovery_target_timeline = '0x10'
EOF
cat >"$TMP/included.conf" <<EOF
port = 5540
EOF
native() { "$BASE/pg-lisp.sh" -D "$TMP/empty-data" -c "config_file=$TMP/server.conf" "$@"; }
test "$(native -C port)" = 5540
test "$(native -Cport -c port=5541)" = 5541
test "$(native -c port=5542 -C PORT)" = 5542
test "$(native -C checkpoint_timeout)" = 90
test "$(native -C max_wal_size)" = 2048
test "$(native -C wal_keep_size)" = 1024
test "$(native -C hot_standby)" = off
test "$(native -C recovery_target_timeline)" = 0x10
test "$(native -C restore_command)" = ''
if native -C missing_setting >"$TMP/unknown.out" 2>"$TMP/unknown.err"; then
    echo 'config-output: unknown parameter unexpectedly succeeded' >&2
    exit 1
fi
if [ -x "$REFERENCE_POSTGRES" ]; then
    for name in port checkpoint_timeout max_wal_size wal_keep_size hot_standby recovery_target_timeline restore_command data_directory; do
        expected=$("$REFERENCE_POSTGRES" -D "$TMP/empty-data" -c "config_file=$TMP/server.conf" -C "$name")
        actual=$(native -C "$name")
        if [ "$actual" != "$expected" ]; then
            printf 'config-output: %s: native <%s>, C <%s>\n' "$name" "$actual" "$expected" >&2
            exit 1
        fi
    done
    echo 'config-output: C metadata parity PASS'
fi
rmdir "$TMP/empty-data"
echo 'config-output: includes, overrides, base units and no storage initialization PASS'
