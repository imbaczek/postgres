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
max_locks_per_transaction = '137'
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
test "$(native -C max_locks_per_transaction)" = 137
test "$(native -c max_locks_per_transaction=149 -C max_locks_per_transaction)" = 149
test "$(native -c max_locks_per_transaction=0b10000000 -C max_locks_per_transaction)" = 128
test "$(native -C wal_keep_size)" = 1024
test "$(native -C hot_standby)" = off
test "$(native -C recovery_target_timeline)" = 0x10
test "$(native -C restore_command)" = ''
if native -C missing_setting >"$TMP/unknown.out" 2>"$TMP/unknown.err"; then
    echo 'config-output: unknown parameter unexpectedly succeeded' >&2
    exit 1
fi
if [ -x "$REFERENCE_POSTGRES" ]; then
    for name in port checkpoint_timeout max_wal_size max_locks_per_transaction wal_keep_size hot_standby recovery_target_timeline restore_command data_directory; do
        expected=$("$REFERENCE_POSTGRES" -D "$TMP/empty-data" -c "config_file=$TMP/server.conf" -C "$name")
        actual=$(native -C "$name")
        if [ "$actual" != "$expected" ]; then
            printf 'config-output: %s: native <%s>, C <%s>\n' "$name" "$actual" "$expected" >&2
            exit 1
        fi
    done
    for option in max_connections=0b1100100 max_locks_per_transaction=+0B1010 checkpoint_timeout=0b11110s max_slot_wal_keep_size=-0b1; do
        name=${option%%=*}
        expected=$("$REFERENCE_POSTGRES" -D "$TMP/empty-data" -c "config_file=$TMP/server.conf" -c "$option" -C "$name")
        actual=$(native -c "$option" -C "$name")
        test "$actual" = "$expected"
    done
    for value in 0b 0b2 0b10e1 0b10.5; do
        if "$REFERENCE_POSTGRES" -D "$TMP/empty-data" -c "config_file=$TMP/server.conf" -c "max_locks_per_transaction=$value" -C max_locks_per_transaction >"$TMP/c-invalid.out" 2>&1; then
            echo 'config-output: C accepted malformed binary integer' >&2
            exit 1
        fi
        if native -c "max_locks_per_transaction=$value" -C max_locks_per_transaction >"$TMP/native-invalid.out" 2>&1; then
            echo 'config-output: native accepted malformed binary integer' >&2
            exit 1
        fi
    done
    echo 'config-output: C signed binary prefix parity PASS'
    echo 'config-output: C metadata parity PASS'
fi
rmdir "$TMP/empty-data"
echo 'config-output: includes, overrides, base units and no storage initialization PASS'
