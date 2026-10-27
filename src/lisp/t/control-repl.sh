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
SRV=
. "$BASE/t/test-support.sh"
test_debug_setup control-repl "$PORT" "$TMP" "$LOG"

cleanup() {
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" \
    --control-port "$CPORT" --control-socket "$SOCK"

if ! test_wait_owned_sql_server "$SRV" "$PORT" psql 300; then
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

ss -H -ltnp "sport = :$CPORT" | grep -F "pid=$SRV," >/dev/null \
    || fail "TCP control listener does not belong to the test server"

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

# SQL requests the same asynchronous SIGHUP reload as pg_ctl reload.
printf 'checkpoint_timeout = 61\n' > "$TMP/data/postgresql.conf"
reload_result=$(PGPASSWORD=x timeout 20 psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
    -q -A -t -v ON_ERROR_STOP=1 -c 'SELECT pg_reload_conf()')
[ "$reload_result" = "t" ] || fail "SQL configuration reload request failed"
i=0
while [ "$i" -lt 100 ]; do
    timeout_value=$(PGPASSWORD=x timeout 20 psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -q -A -t -v ON_ERROR_STOP=1 -c 'SHOW checkpoint_timeout')
    [ "$timeout_value" = "61s" ] && break
    i=$((i + 1))
    sleep 0.05
done
[ "$timeout_value" = "61s" ] || fail "SQL reload did not apply configuration"

echo "control-repl: PASS (TCP $CPORT + unix socket, control and SQL reload, SQL intact)"
