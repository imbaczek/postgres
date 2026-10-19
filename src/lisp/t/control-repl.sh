#!/bin/sh
# t/control-repl.sh -- the out-of-band control REPL over TCP and Unix sockets.
#
# Starts a server with --control-port (loopback TCP) and --control-socket
# (AF_UNIX), evaluates a few forms on each, checks that (reload) still works
# there, and that ordinary SQL is unaffected.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5464}
CPORT=${CPORT:-5465}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-control.XXXXXX")
SOCK="$TMP/control.sock"
LOG="$TMP/server.log"

cleanup() {
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" \
    --control-port "$CPORT" --control-socket "$SOCK" > "$LOG" 2>&1 &
SRV=$!

i=0
while [ "$i" -lt 150 ]; do
    if PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
            -c '' >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done
if [ "$i" -ge 150 ]; then
    echo "control-repl: server did not start; log follows" >&2
    cat "$LOG" >&2
    exit 1
fi

fail() {
    echo "control-repl: FAIL $1" >&2
    cat "$LOG" >&2
    exit 1
}

client() {
    timeout 20 "$SBCL" --script "$BASE/t/control-client.lisp" "$@"
}

# ---- loopback TCP control port -------------------------------------------

tcp_out=$(printf '(+ 1 2)\n(list :a 1 "b")\n(reload)\n(quit)\n' \
    | client tcp "127.0.0.1:$CPORT" || true)

echo "$tcp_out" | grep -q '^3$'             || fail "TCP: expected 3 from (+ 1 2)"
echo "$tcp_out" | grep -q '(:A 1 "b")'      || fail "TCP: bad list result"
echo "$tcp_out" | grep -q 'reload complete' || fail "TCP: (reload) did not complete"
echo "$tcp_out" | grep -q 'bye'             || fail "TCP: (quit) did not close the session"

# ---- Unix domain socket ---------------------------------------------------

[ -S "$SOCK" ] || fail "Unix control socket $SOCK was not created"

unix_out=$(printf '(* 6 7)\n(package-name *package*)\n(quit)\n' \
    | client unix "$SOCK" || true)

echo "$unix_out" | grep -q '^42$' || fail "unix: expected 42 from (* 6 7)"
echo "$unix_out" | grep -q '"PG"' || fail "unix: REPL not in the PG package"
echo "$unix_out" | grep -q 'bye'  || fail "unix: (quit) did not close the session"

# ---- ordinary SQL still works after the reload ---------------------------

serving=$(PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -q -A -t -c 'select 1 as ok')
[ "$serving" = "1" ] || fail "SQL stopped working after control-plane reload"

echo "control-repl: PASS (TCP $CPORT + unix socket, reload, SQL intact)"
