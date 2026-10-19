#!/bin/sh
# t/swank.sh -- Swank is preloaded, opt-in, and bound to the requested address.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5468}
SWANK_PORT=${SWANK_PORT:-5469}
WILDCARD_PORT=${WILDCARD_PORT:-5470}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-swank.XXXXXX")
SOCK="$TMP/control.sock"
LOG="$TMP/server.log"
SRV=
OCCUPIER=

cleanup() {
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    if [ -n "$OCCUPIER" ]; then
        kill -TERM "$OCCUPIER" 2>/dev/null || true
        wait "$OCCUPIER" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

fail() {
    echo "swank: FAIL $1" >&2
    if [ -f "$LOG" ]; then
        cat "$LOG" >&2
    fi
    exit 1
}

stop_server() {
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
        SRV=
    fi
}

start_server() {
    "$BASE/pg-lisp.sh" -p "$PORT" -h 0.0.0.0 -D "$TMP/data" "$@" \
        > "$LOG" 2>&1 &
    SRV=$!
}

wait_sql() {
    sql_port=$1
    pid=$2
    i=0
    while [ "$i" -lt 100 ]; do
        if PGPASSWORD=x psql -X -h 127.0.0.1 -p "$sql_port" \
                -U postgres -d postgres -c '' >/dev/null 2>&1; then
            return
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            fail "server exited before SQL became ready"
        fi
        i=$((i + 1))
        sleep 0.1
    done
    fail "server did not become ready"
}

listener_rows() {
    ss -H -ltn "sport = :$1"
}

assert_binding() {
    port=$1
    address=$2
    rows=$(listener_rows "$port")
    case "$rows" in
        *" $address:$port "*) ;;
        *) fail "expected listener at $address:$port, got: ${rows:-none}" ;;
    esac
}

if ! command -v ss >/dev/null 2>&1; then
    echo "swank: ss is required to inspect listener addresses" >&2
    exit 127
fi

# A bind address by itself must not turn the listener on. SQL is bound to all
# interfaces so this also checks that the default Swank address is independent.
# The control socket verifies Swank is loaded in the image despite no listener.
DEFAULT_4005_BEFORE=$(listener_rows 4005)
start_server --swank-bind-address 0.0.0.0 --control-socket "$SOCK"
wait_sql "$PORT" "$SRV"
[ -S "$SOCK" ] || fail "control socket was not created"
control_out=$(printf '(if (and (find-package "SWANK") (fboundp (find-symbol "CREATE-SERVER" "SWANK"))) :swank-loaded :missing)\n(quit)\n' \
    | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$SOCK")
echo "$control_out" | grep -q ':SWANK-LOADED' || fail "Swank server was not loaded into the image: $control_out"
rows=$(listener_rows "$SWANK_PORT")
[ -z "$rows" ] || fail "bind address alone opened a Swank listener: $rows"
default_rows=$(listener_rows 4005)
if [ -z "$DEFAULT_4005_BEFORE" ]; then
    [ -z "$default_rows" ] || fail "listener opened on default Swank port 4005"
elif [ "$DEFAULT_4005_BEFORE" != "$default_rows" ]; then
    fail "existing listener on default Swank port 4005 changed"
fi
stop_server

# An invalid Swank port fails before either listener is opened.
if "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/invalid-data" \
        --swank-port 65536 > "$TMP/invalid-port.log" 2>&1; then
    fail "server started with an out-of-range Swank port"
fi
case "$(cat "$TMP/invalid-port.log")" in
    *"option --swank-port expects a port in 1..65535"*) ;;
    *) fail "invalid Swank port did not produce a clean argument error" ;;
esac
[ -z "$(listener_rows "$PORT")" ] || fail "SQL listener opened after invalid Swank port"

# With no explicit Swank address, the listener must remain loopback-only even
# though SQL is listening on 0.0.0.0.
start_server --swank-port "$SWANK_PORT"
wait_sql "$PORT" "$SRV"
assert_binding "$SWANK_PORT" 127.0.0.1
nc -z -w 1 127.0.0.1 "$SWANK_PORT" || fail "loopback Swank port is not reachable"
stop_server

# An explicit address is honored when (and only when) a Swank port is enabled.
start_server --swank-port "$WILDCARD_PORT" --swank-bind-address 0.0.0.0
wait_sql "$PORT" "$SRV"
assert_binding "$WILDCARD_PORT" 0.0.0.0
nc -z -w 1 127.0.0.1 "$WILDCARD_PORT" || fail "wildcard Swank port is not reachable"
stop_server

# An occupied Swank port fails startup before the SQL listener is opened.
"$BASE/pg-lisp.sh" -p "$SWANK_PORT" -D "$TMP/occupier-data" \
    > "$TMP/occupier.log" 2>&1 &
OCCUPIER=$!
wait_sql "$SWANK_PORT" "$OCCUPIER"
FAILED_SOCK="$TMP/failing.sock"
if "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/failing-data" \
        --control-socket "$FAILED_SOCK" --swank-port "$SWANK_PORT" \
        > "$LOG" 2>&1; then
    fail "server started on an occupied Swank port"
fi
case "$(cat "$LOG")" in
    *"cannot start Swank on 127.0.0.1:$SWANK_PORT"*) ;;
    *) fail "occupied Swank port did not produce a clean startup error" ;;
esac
[ -z "$(listener_rows "$PORT")" ] || fail "SQL listener remained after Swank startup failure"
[ ! -e "$FAILED_SOCK" ] || fail "control socket was left after Swank startup failure"
kill -TERM "$OCCUPIER" 2>/dev/null || true
wait "$OCCUPIER" 2>/dev/null || true
OCCUPIER=

echo "swank: PASS (preloaded/default off, invalid/occupied ports, loopback and explicit bind)"
