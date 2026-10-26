#!/bin/sh
# Native namespace migration without changing timeline, role, or coordination.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-6186}
OLD_PORT=$((PORT + 1))
NEW_PORT=$((PORT + 2))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-wal-namespace.XXXXXX")
SOURCE= OLD= NEW= MOVER=
cleanup() {
    result=$?
    trap - EXIT
    for pid in "$NEW" "$OLD" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    if [ -n "$MOVER" ]; then wait "$MOVER" 2>/dev/null || :; fi
    echo "wal-namespace: artifacts=$TMP"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
control() {
    printf '%s\n(quit)\n' "$2" | timeout 40 "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
start_source() {
    rm -f "$TMP/source.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >>"$TMP/source.log" 2>&1 &
    SOURCE=$!
    test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
}
stop_source() { kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=; }
start_old() {
    rm -f "$TMP/old.sock"
    "$BASE/pg-lisp.sh" -p "$OLD_PORT" -D "$TMP/legacy-copy" --control-socket "$TMP/old.sock" >>"$TMP/old.log" 2>&1 &
    OLD=$!
    test_wait_owned_sql_server "$OLD" "$OLD_PORT" "$PSQL"
}
start_new() {
    rm -f "$TMP/new.sock"
    "$BASE/pg-lisp.sh" -p "$NEW_PORT" -D "$TMP/qualified-copy" --control-socket "$TMP/new.sock" >>"$TMP/new.log" 2>&1 &
    NEW=$!
    test_wait_owned_sql_server "$NEW" "$NEW_PORT" "$PSQL"
}
wait_rows() {
    attempt=0
    while [ "$(sql "$1" 'SELECT count(*) FROM namespace_rows')" != "$2" ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 300
        if [ "$1" = "$OLD_PORT" ]; then kill -0 "$OLD"; else kill -0 "$NEW"; fi
        sleep 0.05
    done
}
inspect_source() { python3 "$BASE/t/interop/wal-namespace-inspect.py" "$TMP/source" "$@"; }
restore_baseline() { rm -rf "$TMP/source"; cp -a "$TMP/baseline" "$TMP/source"; }
if [ -n "${WAL_NAMESPACE_SEED:-}" ]; then
    # Optional stopped, task-owned fixture reuse for iteration on crash boundaries.
    cp -a "$WAL_NAMESPACE_SEED" "$TMP/source"
    start_source
    test "$(sql "$PORT" 'SELECT count(*) FROM namespace_rows')" = 1
else
    start_source
    sql "$PORT" 'CREATE TABLE namespace_rows (id integer PRIMARY KEY); INSERT INTO namespace_rows VALUES (1)'
    mkdir -m 700 "$TMP/retention-copy"
    checked source "(physical-base-backup \"$TMP/retention-copy/\" \"namespace_old\")" retention-backup
    # Retain the original base pin while real committed updates force WAL rollover.
    echo 'wal-namespace: generating committed multi-segment fixture'
    python3 - "$TMP/updates.sql" <<'PY'
import sys
with open(sys.argv[1], 'w') as stream:
    for _ in range(2200):
        stream.write('UPDATE namespace_rows SET id = id;\n')
PY
    "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -qAt \
        -v ON_ERROR_STOP=1 -f "$TMP/updates.sql" >"$TMP/updates.log"
fi
sql "$PORT" CHECKPOINT
checked source '(assert (>= (length (wal-discover-segments)) 2))' multiple-segments
stop_source
cp -a "$TMP/source" "$TMP/baseline"
inspect_source --namespace legacy --save "$TMP/before.json"

for phase in after-link after-links-sync after-control-rename after-control-before-memory after-memory after-cleanup-link; do
    restore_baseline
    start_source
    marker="$TMP/$phase.marker"
    release="$TMP/$phase.release"
    checked source "(load \"$BASE/t/wal-namespace-failpoints.lisp\")" "$phase-load"
    checked source "(install-namespace-test-hook \"$phase\" \"$marker\" \"$release\")" "$phase-install"
    control source '(wal-migrate-timeline-namespace)' >"$TMP/$phase-move.log" 2>&1 &
    MOVER=$!
    attempt=0
    while [ ! -f "$marker" ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 300
        kill -0 "$SOURCE"; sleep 0.05
    done
    test "$(cat "$marker")" -eq "$SOURCE"
    stop_source
    wait "$MOVER" 2>/dev/null || :; MOVER=
    case "$phase" in after-link|after-links-sync) mode=legacy;; *) mode=timeline;; esac
    inspect_source --namespace "$mode" --expect "$TMP/before.json"
    case "$phase" in after-link|after-control-before-memory)
        startup_marker="$TMP/$phase.startup-marker"
        PG_NAMESPACE_STARTUP_MARKER="$startup_marker" PG_NAMESPACE_STARTUP_RELEASE="$TMP/$phase.startup-release" \
            "$SBCL" --script "$BASE/t/wal-namespace-startup-crash.lisp" \
            -p "$PORT" -D "$TMP/source" >"$TMP/$phase-startup-crash.log" 2>&1 &
        SOURCE=$!
        attempt=0
        while [ ! -f "$startup_marker" ]; do
            attempt=$((attempt + 1)); test "$attempt" -lt 300
            kill -0 "$SOURCE"; sleep 0.05
        done
        test "$(cat "$startup_marker")" -eq "$SOURCE"
        stop_source
        inspect_source --namespace "$mode" --expect "$TMP/before.json"
        echo "wal-namespace: $phase startup-reconciliation crash preserves authoritative bytes PASS"
        ;;
    esac
    start_source
    inspect_source --namespace "$mode" --expect "$TMP/before.json"
    if [ "$mode" = legacy ]; then inactive=timeline; else inactive=legacy; fi
    checked source "(assert (null (wal-discover-segments 1 :$inactive)))" "$phase-startup-reconciled"
    checked source '(wal-migrate-timeline-namespace)' "$phase-retry"
    checked source '(assert (null (wal-discover-segments 1 :legacy)))' "$phase-cleanup"
    test "$(sql "$PORT" 'SELECT count(*) FROM namespace_rows')" = 1
    sql "$PORT" 'INSERT INTO namespace_rows VALUES (2)'
    stop_source
    start_source
    test "$(sql "$PORT" 'SELECT count(*) FROM namespace_rows')" = 2
    checked source '(assert (and (eq *wal-segment-namespace* :timeline) (= *wal-insert-timeline* *wal-checkpoint-timeline* 1) (not (recovery-in-progress-p))))' "$phase-identity"
    stop_source
    echo "wal-namespace: $phase crash/restart/retry/post-migration commit PASS"
done

# Keep an old-base standby connected while the source changes filename grammar.
restore_baseline
start_source
alias="$TMP/source/pg_wal/000000010000000000000063.wal"
ln -s missing-qualified-target "$alias"
control source '(wal-migrate-timeline-namespace)' >"$TMP/unmatched-symlink.log"
grep -q 'unmatched qualified WAL alias' "$TMP/unmatched-symlink.log"
checked source '(assert (and (eq *wal-segment-namespace* :legacy) (not *wal-poisoned*)))' rejected-alias
test -L "$alias"
inspect_source --namespace legacy --expect "$TMP/before.json"
rm "$alias"
mkdir -m 700 "$TMP/legacy-copy"
checked source "(physical-base-backup \"$TMP/legacy-copy/\" \"namespace_follow\")" legacy-follow-backup
start_old
checked old "(physical-follow-start \"127.0.0.1\" $PORT \"namespace_follow\")" old-follow
sql "$PORT" 'INSERT INTO namespace_rows VALUES (2)'
wait_rows "$OLD_PORT" 2
checked source '(wal-migrate-timeline-namespace)' live-move
sql "$PORT" 'INSERT INTO namespace_rows VALUES (3)'
wait_rows "$OLD_PORT" 3
checked old '(assert (eq *wal-segment-namespace* :legacy))' old-base-unchanged

# A fresh base carries qualified files/control; receive spool remains independent.
mkdir -m 700 "$TMP/qualified-copy"
checked source "(physical-base-backup \"$TMP/qualified-copy/\" \"namespace_new\")" qualified-backup
start_new
checked new '(assert (eq *wal-segment-namespace* :timeline))' qualified-base
checked new "(physical-follow-start \"127.0.0.1\" $PORT \"namespace_new\")" new-follow
sql "$PORT" 'INSERT INTO namespace_rows VALUES (4)'
wait_rows "$OLD_PORT" 4
wait_rows "$NEW_PORT" 4
sql "$NEW_PORT" CHECKPOINT
kill -KILL "$NEW"; wait "$NEW" 2>/dev/null || :; NEW=
start_new
wait_rows "$NEW_PORT" 4
stop_source
start_source
sql "$PORT" 'INSERT INTO namespace_rows VALUES (5)'
wait_rows "$OLD_PORT" 5
wait_rows "$NEW_PORT" 5
python3 "$BASE/t/interop/checkpoint-snapshot.py" "$TMP/source" >"$TMP/final-checkpoint.json"
if sql "$NEW_PORT" 'INSERT INTO namespace_rows VALUES (999)' >"$TMP/recovery-write.log" 2>&1; then exit 1; fi
checked old '(physical-recovery-prepare-shutdown)' old-stop
checked new '(physical-recovery-prepare-shutdown)' new-stop
checked source '(assert (loop repeat 100 thereis (with-wal-critical (zerop (hash-table-count *physical-source-senders*))) do (sleep 0.05)))' senders-stopped
checked source '(physical-source-pin-release "namespace_old")' old-release
checked source '(physical-source-pin-release "namespace_follow")' follow-release
checked source '(physical-source-pin-release "namespace_new")' new-release
echo 'wal-namespace: all crash boundaries, byte preservation, continued follow and qualified bootstrap/restart PASS'
