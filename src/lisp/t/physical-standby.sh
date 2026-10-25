#!/bin/sh
# Actual native static standby: role/progress, immutable data and SQL write exclusion.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5872}
SETUP_LOG=$(mktemp "${TMPDIR:-/tmp}/pg-static-standby-setup.XXXXXX")
"$BASE/t/physical-bootstrap.sh" >"$SETUP_LOG" 2>&1
TMP=$(awk -F= '/^physical-bootstrap: artifacts=/{print $2}' "$SETUP_LOG")
test -n "$TMP"
SRV=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$SRV" ]; then kill -INT "$SRV" 2>/dev/null || :; wait "$SRV" 2>/dev/null || :; fi
    echo "physical-standby: artifacts=$TMP setup=$SETUP_LOG"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 "$@"
}
repl() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X \
        -d "host=127.0.0.1 port=$PORT user=postgres dbname=postgres replication=database" \
        -qAt -v ON_ERROR_STOP=1 "$@"
}
start() {
    rm -f "$TMP/standby.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/copy" \
        --control-socket "$TMP/standby.sock" >>"$TMP/standby.log" 2>&1 &
    SRV=$!
    test_wait_owned_sql_server "$SRV" "$PORT" "$PSQL"
}
N=0
reject() {
    N=$((N + 1))
    if sql -v VERBOSITY=sqlstate -c "$2" >"$TMP/reject-$N.log" 2>&1; then
        echo "standby unexpectedly accepted: $2" >&2
        exit 1
    fi
    if ! grep -q "$1" "$TMP/reject-$N.log"; then
        echo "unexpected SQLSTATE for: $2 (wanted $1); log=$TMP/reject-$N.log" >&2
        exit 1
    fi
}
start
test "$(sql -c 'SELECT pg_is_in_recovery();')" = t
test "$(sql -c 'SHOW in_hot_standby;')" = on
test "$(sql -c 'SHOW transaction_read_only;')" = on
test "$(sql -c 'SELECT pg_last_wal_receive_lsn() IS NULL;')" = t
test "$(sql -c 'SELECT pg_last_wal_replay_lsn() IS NOT NULL;')" = t
sql -c 'SELECT id, label FROM bootstrap_data ORDER BY id;' >"$TMP/rows.standby"
diff -u "$TMP/rows.expected" "$TMP/rows.standby"
sequence_before=$(sql -c "SELECT pg_lisp_sequence_state('standby_seq');")
for query in \
    "INSERT INTO bootstrap_data VALUES (9,'write')" \
    "UPDATE bootstrap_data SET label='write' WHERE id=1" \
    "DELETE FROM bootstrap_data WHERE id=1" \
    "TRUNCATE bootstrap_data" \
    "CREATE TABLE forbidden (id integer)" \
    "DROP TABLE bootstrap_data" \
    "ALTER TABLE bootstrap_data ADD CONSTRAINT forbidden_unique UNIQUE(label)" \
    "CREATE INDEX forbidden_idx ON bootstrap_data(id)" \
    "CREATE SEQUENCE forbidden_seq" \
    "ALTER SEQUENCE standby_seq RESTART WITH 99" \
    "DROP SEQUENCE standby_seq" \
    "CREATE PUBLICATION forbidden_pub FOR ALL TABLES" \
    "ANALYZE bootstrap_data" \
    "VACUUM bootstrap_data" \
    "VACUUM FULL bootstrap_data" \
    "SELECT nextval('standby_seq')" \
    "SELECT setval('standby_seq',99)" \
    "COPY (SELECT nextval('standby_seq')) TO STDOUT" \
    "EXPLAIN ANALYZE SELECT nextval('standby_seq')"; do
    reject 25006 "$query"
done
# C classifies CHECKPOINT as read-only in recovery; a static base has no new R.
sql -c 'BEGIN; CHECKPOINT; ROLLBACK'
reject 0A000 'BEGIN READ WRITE'
reject 0A000 'BEGIN; SET TRANSACTION READ WRITE'
reject 55000 'SELECT pg_current_wal_lsn()'
reject 55000 'SELECT pg_current_wal_flush_lsn()'
repl -c 'IDENTIFY_SYSTEM' >"$TMP/identify.standby"
test "$(awk -F'|' '{print $3}' "$TMP/identify.standby")" = "$(sql -c 'SELECT pg_last_wal_replay_lsn();')"
for command in 'CREATE_REPLICATION_SLOT forbidden LOGICAL pgoutput' \
               'DROP_REPLICATION_SLOT forbidden' \
               'START_REPLICATION SLOT forbidden LOGICAL 0/0'; do
    N=$((N + 1))
    if repl -v VERBOSITY=sqlstate -c "$command" >"$TMP/reject-$N.log" 2>&1; then exit 1; fi
    grep -q 25006 "$TMP/reject-$N.log"
done
printf "BEGIN;\nSELECT nextval('standby_seq');\nINSERT INTO bootstrap_data VALUES (99,'failed');\nROLLBACK;\n" |
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=0 -v VERBOSITY=sqlstate >"$TMP/failed-transaction.log" 2>&1
grep -q 25006 "$TMP/failed-transaction.log"
grep -q 25P02 "$TMP/failed-transaction.log"
sql -c "SET search_path=public; RESET ALL;"
test "$(sql -c 'SHOW transaction_read_only;')" = on
test "$sequence_before" = "$(sql -c "SELECT pg_lisp_sequence_state('standby_seq');")"
if PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d missing_database \
        -c '' >"$TMP/missing-database.log" 2>&1; then exit 1; fi
grep -q 'does not exist' "$TMP/missing-database.log"
printf '(reload)\n(quit)\n' | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/standby.sock" >"$TMP/reload.log"
grep -q 'reload complete' "$TMP/reload.log"
test "$(sql -c 'SELECT pg_is_in_recovery();')" = t
reject 25006 "INSERT INTO bootstrap_data VALUES (9,'after-reload')"
kill -KILL "$SRV"
wait "$SRV" 2>/dev/null || :
SRV=
start
test "$(sql -c 'SELECT pg_is_in_recovery();')" = t
sql -c 'SELECT id, label FROM bootstrap_data ORDER BY id;' >"$TMP/rows.restarted-standby"
diff -u "$TMP/rows.expected" "$TMP/rows.restarted-standby"
test "$sequence_before" = "$(sql -c "SELECT pg_lisp_sequence_state('standby_seq');")"
kill -INT "$SRV"
wait "$SRV"
SRV=
"$SBCL" --script "$BASE/t/physical-bootstrap-verify.lisp" "$TMP/copy/" >"$TMP/unchanged.log" 2>&1
grep -q 'physical-bootstrap-verify: PASS' "$TMP/unchanged.log"
for damage in missing-markers missing-control corrupt-control missing-xid corrupt-page; do
    python3 - "$TMP" "$damage" <<'PY'
from pathlib import Path
import shutil
import sys
root, damage = Path(sys.argv[1]), sys.argv[2]
image = root / ("invalid-" + damage)
shutil.copytree(root / "copy", image)
if damage == "missing-markers":
    (image / "recovery.required").unlink()
    (image / "physical-backup.manifest").unlink()
elif damage == "missing-control":
    (image / "physical-recovery.control").unlink()
elif damage == "missing-xid":
    (image / "global/pg_control").unlink()
else:
    path = (image / "physical-recovery.control" if damage == "corrupt-control"
            else sorted((image / "base").iterdir())[0])
    with path.open("r+b") as stream:
        value = stream.read(1)
        stream.seek(0)
        stream.write(bytes([value[0] ^ 1]))
PY
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/invalid-$damage" >"$TMP/invalid-$damage.log" 2>&1 &
    SRV=$!
    attempt=0
    while kill -0 "$SRV" 2>/dev/null; do
        case $(ps -o stat= -p "$SRV" 2>/dev/null || :) in Z*) break;; esac
        attempt=$((attempt + 1))
        if [ "$attempt" -gt 100 ]; then
            echo "damaged recovery image admitted startup: $damage" >&2
            exit 1
        fi
        sleep 0.1
    done
    if wait "$SRV"; then SRV=; exit 1; fi
    SRV=
    grep -q 'pg-lisp: fatal:' "$TMP/invalid-$damage.log"
    if [ "$damage" = missing-xid ]; then test ! -e "$TMP/invalid-$damage/global/pg_control"; fi
    if [ "$damage" = missing-control ]; then test ! -e "$TMP/invalid-$damage/physical-recovery.control"; fi
done
echo 'physical-standby: PASS'
