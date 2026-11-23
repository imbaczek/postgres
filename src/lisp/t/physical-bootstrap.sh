#!/bin/sh
# A quiesced base image starts in recovery; it is never promoted by this test.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5870}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-bootstrap.XXXXXX")
SRV= TARGET= BACKUP= WRITER= TX=
cleanup() {
    status=$?
    trap - EXIT
    touch "$TMP/copy.release"
    for pid in "$BACKUP" "$WRITER" "$TX"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    for pid in "$TARGET" "$SRV"; do
        if [ -n "$pid" ]; then
            kill -INT "$pid" 2>/dev/null || :
            wait "$pid" 2>/dev/null || :
        fi
    done
    echo "physical-bootstrap: artifacts=$TMP"
    exit "$status"
}
control() {
    test_lisp_control "$TMP/source.sock" "$1"
}
start_source() {
    test_launch_lisp_server "$PORT" "$TMP/source" "$TMP/source.sock" "$TMP/source.log"
    SRV=$!
    test_wait_owned_sql_server "$SRV" "$PORT" "$PSQL"
}
wait_file() {
    attempt=0
    while [ ! -f "$1" ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 200 || { echo "missing bootstrap marker $1" >&2; return 1; }
        kill -0 "$SRV"
        sleep 0.1
    done
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres \
        -d postgres -qAt -v ON_ERROR_STOP=1 -c "$1"
}
mkdir -m 700 "$TMP/copy"
start_source
sql "CREATE TABLE bootstrap_data (id integer PRIMARY KEY, label varchar(32)); INSERT INTO bootstrap_data VALUES (1,'base'),(2,'copy'); CREATE SEQUENCE standby_seq START WITH 7;"
mkdir "$TMP/rejected"
mkfifo "$TMP/transaction.fifo"
PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 <"$TMP/transaction.fifo" >"$TMP/transaction.log" 2>&1 &
TX=$!
exec 7>"$TMP/transaction.fifo"
printf "BEGIN;\nINSERT INTO bootstrap_data VALUES (-1,'idle');\nSELECT 12345;\n" >&7
attempt=0
while ! grep -q '^12345$' "$TMP/transaction.log"; do
    attempt=$((attempt + 1))
    test "$attempt" -lt 100
    sleep 0.1
done
control "(physical-base-backup \"$TMP/rejected/\" \"idle_copy\")" >"$TMP/idle-rejected.log"
grep -q 'requires no active transaction IDs' "$TMP/idle-rejected.log"
test ! -f "$TMP/rejected/recovery.required"
printf 'ROLLBACK;\n\\q\n' >&7
exec 7>&-
wait "$TX"
TX=
sql 'SELECT id, label FROM bootstrap_data ORDER BY id;' >"$TMP/rows.expected"
control "(progn (load \"$BASE/t/physical-bootstrap-native.lisp\") (install-physical-bootstrap-pause #p\"$TMP/\"))" >"$TMP/hooks.log"
printf '(physical-base-backup "%s/" "base_copy")\n(quit)\n' "$TMP/copy" |
    "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/source.sock" >"$TMP/copy.log" &
BACKUP=$!
wait_file "$TMP/copy.paused"
sql "INSERT INTO bootstrap_data VALUES (3,'after-copy');" >"$TMP/writer.log" 2>&1 &
WRITER=$!
wait_file "$TMP/writer.blocked"
kill -0 "$WRITER"
touch "$TMP/copy.release"
wait "$BACKUP"
BACKUP=
wait "$WRITER"
WRITER=
test -f "$TMP/copy/recovery.required"
test -f "$TMP/copy/physical-backup.manifest"
"$SBCL" --script "$BASE/t/physical-bootstrap-verify.lisp" "$TMP/copy/" >"$TMP/verify.log" 2>&1
grep -q 'physical-bootstrap-verify: PASS' "$TMP/verify.log"
for data in source copy; do
    python3 "$BASE/t/interop/wal-namespace-inspect.py" "$TMP/$data" --namespace timeline
    test -z "$(find "$TMP/$data/pg_wal" -maxdepth 1 -name '????????????????.wal' -print)"
done
"$SBCL" --script "$BASE/t/physical-bootstrap-rows.lisp" "$TMP/copy/" "$TMP/rows.copy" >"$TMP/rows.log" 2>&1
diff -u "$TMP/rows.expected" "$TMP/rows.copy"
test "$(sql 'SELECT count(*) FROM bootstrap_data;')" = 3
manifest="$TMP/copy/physical-backup.manifest"
cp -p "$manifest" "$TMP/manifest-current"
PYTHONPATH="$BASE/t/interop${PYTHONPATH:+:$PYTHONPATH}" python3 - "$TMP" <<'PY'
import re
import sys
from pathlib import Path

from crc32c_reference import crc32c

root = Path(sys.argv[1])
header, payload = (root / "manifest-current").read_bytes().split(b"\n", 1)
assert payload.endswith(b"\n")
assert header == f"PGPHYSICALBACKUP1 {crc32c(payload[:-1]):08X}".encode()
without_start, count = re.subn(rb"\s+:checkpoint-start\s+\d+", b"", payload,
                               count=1, flags=re.I)
assert count == 1
for fixture, version, body in (("v1", 1, without_start), ("v2", 2, payload),
                               ("v3-missing-start", 3, without_start)):
    if version < 3:
        body, count = re.subn(rb"\s+:auth-policy-required\s+1(?=\)\n$)", b"", body,
                              count=1, flags=re.I)
        assert count == 1
    body, count = re.subn(rb"(:version\s+)3\b", rb"\g<1>" + str(version).encode(), body,
                          count=1, flags=re.I)
    assert count == 1
    header = f"PGPHYSICALBACKUP1 {crc32c(body[:-1]):08X}\n".encode()
    (root / f"manifest-{fixture}").write_bytes(header + body)
PY
backup_payload_checksums() {
    (cd "$TMP/copy" &&
        find . -type f ! -path './physical-backup.manifest' -exec sha256sum {} + | sort)
}
backup_payload_checksums >"$TMP/manifest-payload.before"
for fixture in v1 v2 v3-missing-start; do
    rejected="$TMP/manifest-$fixture"
    cp "$rejected" "$manifest"
    status=0
    timeout 60 "$BASE/pg-lisp.sh" -p "$((PORT + 1))" -D "$TMP/copy" \
        >"$rejected.log" 2>&1 || status=$?
    test "$status" -eq 1
    grep -q 'unsupported or malformed physical recovery manifest' "$rejected.log"
    cmp "$rejected" "$manifest"
    backup_payload_checksums >"$TMP/manifest-payload.after"
    diff -u "$TMP/manifest-payload.before" "$TMP/manifest-payload.after"
done
cp -p "$TMP/manifest-current" "$manifest"
"$BASE/pg-lisp.sh" -p "$((PORT + 1))" -D "$TMP/copy" >"$TMP/target.log" 2>&1 &
TARGET=$!
test_wait_owned_sql_server "$TARGET" "$((PORT + 1))" "$PSQL"
test "$(PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$((PORT + 1))" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c 'SELECT pg_is_in_recovery();')" = t
kill -INT "$TARGET"
wait "$TARGET"
TARGET=
printf '(assert (gethash "base_copy" pg::*physical-source-pins*))\n(quit)\n' |
    "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/source.sock" >"$TMP/pin.log"
if grep -q ';; error:' "$TMP/pin.log"; then exit 1; fi
mkdir "$TMP/interrupted"
control '(install-physical-bootstrap-crash)' >"$TMP/crash-hook.log"
control "(physical-base-backup \"$TMP/interrupted/\" \"broken_copy\")" >"$TMP/interrupted.log" || :
attempt=0
while kill -0 "$SRV" 2>/dev/null; do
    case $(ps -o stat= -p "$SRV" 2>/dev/null || :) in Z*) break;; esac
    attempt=$((attempt + 1))
    test "$attempt" -lt 100 || { echo 'bootstrap crash hook did not stop source' >&2; exit 1; }
    sleep 0.1
done
crash_status=0
wait "$SRV" 2>/dev/null || crash_status=$?
test "$crash_status" -eq 137
SRV=
test -f "$TMP/interrupted/recovery.required"
test ! -f "$TMP/interrupted/physical-backup.manifest"
start_source
test "$(sql 'SELECT count(*) FROM bootstrap_data;')" = 3
control '(progn (assert (gethash "base_copy" pg::*physical-source-pins*)) (assert (gethash "broken_copy" pg::*physical-source-pins*)) (format t "CRASH-PINS-RESTORED~%"))' >"$TMP/restored.log"
grep -q 'CRASH-PINS-RESTORED$' "$TMP/restored.log"
echo 'physical-bootstrap: PASS'
