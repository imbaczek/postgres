#!/bin/sh
# Compare threshold arrays and their wire metadata with an owned C server.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PG_C_BINDIR=${PG_C_BINDIR:?set PG_C_BINDIR to the repository C PostgreSQL bin directory}
PYTHON=${PYTHON:-python3}
PORT=${PORT:-6742}
REFERENCE_PORT=${REFERENCE_PORT:-6743}
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/pg-array-types.XXXXXX")
NATIVE= C_RUNNING=
cleanup() {
    status=$?
    trap - EXIT
    trap 'status=130' HUP INT TERM
    if [ -n "$NATIVE" ]; then
        "$PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" INT 20 || \
        "$PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" KILL 5 || :
        wait "$NATIVE" 2>/dev/null || :
    fi
    if [ -n "$C_RUNNING" ]; then
        "$PG_C_BINDIR/pg_ctl" -D "$ROOT/c" -m fast -w stop >"$ROOT/c-stop.log" 2>&1 || :
    fi
    echo "array-types: artifacts=$ROOT"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
export LD_LIBRARY_PATH="${PG_C_LIBDIR:-$PG_C_BINDIR/../lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"$PG_C_BINDIR/initdb" -D "$ROOT/c" -U postgres -A trust --no-locale -E UTF8 >"$ROOT/c-init.log"
C_RUNNING=1
"$PG_C_BINDIR/pg_ctl" -D "$ROOT/c" -l "$ROOT/c.log" \
    -o "-h 127.0.0.1 -p $REFERENCE_PORT -k $ROOT -c wal_level=logical" -w -t 20 start >"$ROOT/c-start.log"
read -r C_PID <"$ROOT/c/postmaster.pid"
test_wait_owned_sql_server "$C_PID" "$REFERENCE_PORT" "${PSQL:-psql}"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$ROOT/native" -c wal_level=logical \
    --control-socket "$ROOT/native.sock" >"$ROOT/native.log" 2>&1 &
NATIVE=$!
test_wait_owned_sql_server "$NATIVE" "$PORT" "${PSQL:-psql}"
timeout --foreground --kill-after=5 120 "$PYTHON" "$BASE/t/interop/array-types.py" "$REFERENCE_PORT" "$PORT"
"$PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" KILL 20
wait "$NATIVE" 2>/dev/null || [ "$?" -eq 137 ]
NATIVE=
"$BASE/pg-lisp.sh" -p "$PORT" -D "$ROOT/native" -c wal_level=logical \
    --control-socket "$ROOT/native-restart.sock" >"$ROOT/native-restart.log" 2>&1 &
NATIVE=$!
test_wait_owned_sql_server "$NATIVE" "$PORT" "${PSQL:-psql}"
restarted=$("${PSQL:-psql}" -XAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" \
    -U postgres -d postgres <<'SQL'
INSERT INTO stored_defaults DEFAULT VALUES;
SELECT id, flags, ids, count(*) FROM stored_defaults GROUP BY id, flags, ids;
SQL
)
[ "$restarted" = "$(printf '%s\n' 'INSERT 0 1' \
    '00112233-4455-6677-8899-aabbccddeeff|{f,NULL,t}|{00112233-4455-6677-8899-aabbccddeeff,NULL}|2')" ]
copied=$("${PSQL:-psql}" -XAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" \
    -U postgres -d postgres -c 'COPY (SELECT id, enabled, b FROM stored_common) TO STDOUT')
[ "$copied" = "$(printf '%s\t%s\t%s' '00112233-4455-6677-8899-aabbccddeeff' 'f' \
    '[0:1][-1:0]={{f,NULL},{t,f}}')" ]
lookups=$("${PSQL:-psql}" -XAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" \
    -U postgres -d postgres <<'SQL'
SELECT marker FROM stored_uuid_paths WHERE id='000000000000000000000000000000ff';
SELECT marker FROM stored_uuid_paths WHERE id='000000000000000000000000000000ff' OR marker=-1;
SQL
)
[ "$lookups" = "$(printf '%s\n' 255 255)" ]
