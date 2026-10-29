#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-7180}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-hot-standby.XXXXXX")
cleanup() {
    status=$?
    trap - EXIT
    for instance in native reference; do
        for role in standby source; do
            registry="$TMP/$instance/$role/owned"
            if [ -d "$registry" ]; then
                if ! "$PYTHON" "$BASE/t/interop/owned-peer.py" cleanup "$registry"; then
                    [ "$status" -ne 0 ] || status=1
                fi
            fi
        done
    done
    echo "hot-standby: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
timeout --foreground --kill-after=5 "${HOT_STANDBY_DRIVER_TIMEOUT:-300}s" \
    "$PYTHON" "$BASE/t/interop/hot-standby.py" "$BASE" "$TMP/native" "$PORT" "$BASE/pg-lisp.sh" --native
if [ -n "${REFERENCE_POSTGRES:-}" ]; then
    timeout --foreground --kill-after=5 "${HOT_STANDBY_DRIVER_TIMEOUT:-300}s" \
        "$PYTHON" "$BASE/t/interop/hot-standby.py" "$BASE" "$TMP/reference" "$((PORT + 2))" "$REFERENCE_POSTGRES"
fi
