#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-7170}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-wal-keep.XXXXXX")
cleanup() {
    status=$?
    trap - EXIT
    for instance in native reference; do
        if [ -d "$TMP/$instance/owned" ]; then
            if ! "$PYTHON" "$BASE/t/interop/owned-peer.py" cleanup "$TMP/$instance/owned"; then
                [ "$status" -ne 0 ] || status=1
            fi
        fi
    done
    echo "wal-keep: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
timeout --foreground --kill-after=5 "${WAL_KEEP_DRIVER_TIMEOUT:-240}s" \
    "$PYTHON" "$BASE/t/interop/wal-keep.py" "$BASE" "$TMP/native" "$PORT" "$BASE/pg-lisp.sh" --native
if [ -n "${REFERENCE_POSTGRES:-}" ]; then
    timeout --foreground --kill-after=5 "${WAL_KEEP_DRIVER_TIMEOUT:-240}s" \
        "$PYTHON" "$BASE/t/interop/wal-keep.py" "$BASE" "$TMP/reference" "$((PORT + 1))" "$REFERENCE_POSTGRES"
fi
