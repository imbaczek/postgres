#!/bin/sh
# t/hot-reload.sh -- SIGHUP recompiles and reloads code in the running server.
#
# Temporarily edits the version string, sends SIGHUP, and checks that the
# already-running process reports the new value without a restart.  The
# original source is restored on exit.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5463}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-reload.XXXXXX")
SRC="$BASE/include/pg-protocol.lisp"
BAK="$TMP/pg-protocol.lisp.bak"
LOG="$TMP/server.log"

cp "$SRC" "$BAK"
restore() {
    cp "$BAK" "$SRC"
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
trap restore EXIT INT TERM

"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" > "$LOG" 2>&1 &
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
    echo "hot-reload: server did not start; log follows" >&2
    cat "$LOG" >&2
    exit 1
fi

q() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -q -A -t -c "$1"
}

before=$(q "show server_version")

# A per-run sentinel guarantees the reload actually recompiled the source,
# even if ASDF's one-second timestamp resolution serves a stale fasl on boot.
SENTINEL="reloaded-$$"
sed -i "s|^(defparameter \*server-version\* \".*\")$|(defparameter *server-version* \"$SENTINEL\")|" "$SRC"
kill -HUP "$SRV"

i=0
while [ "$i" -lt 150 ]; do
    if grep -q 'reload complete' "$LOG"; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done

after=$(q "show server_version")
serving=$(q "select 1 as ok")

if [ "$after" != "$SENTINEL" ]; then
    echo "hot-reload: FAIL expected '$SENTINEL', got '$after'" >&2
    cat "$LOG" >&2
    exit 1
fi
if [ "$serving" != "1" ]; then
    echo "hot-reload: FAIL server stopped serving after reload" >&2
    cat "$LOG" >&2
    exit 1
fi

echo "hot-reload: PASS ($before -> $after, pid $SRV unchanged)"
