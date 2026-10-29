#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON=${PATRONI_PYTHON:-python3}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-libpq-helper.XXXXXX")
cleanup() {
    status=$?
    trap - EXIT
    if [ -d "$TMP/reference/owned" ]; then
        if ! "$PYTHON" "$BASE/t/interop/owned-peer.py" cleanup "$TMP/reference/owned"; then
            [ "$status" -ne 0 ] || status=1
        fi
    fi
    echo "libpq-helper: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ -n "${REFERENCE_POSTGRES:-}" ] && [ -z "${PG_CONFIG:-}" ]; then
    PG_CONFIG="$(dirname -- "$REFERENCE_POSTGRES")/pg_config"
    export PG_CONFIG
fi
"$BASE/t/build-libpq-helper.sh" "$TMP/helper"
export PG_LIBPQ_HELPER="$TMP/helper"
timeout --foreground --kill-after=5 120s \
    "$PYTHON" "$BASE/t/interop/libpq-helper-tests.py"
timeout --foreground --kill-after=5 120s \
    "$PYTHON" "$BASE/t/interop/libpq-bridge-tests.py"
if [ -n "${REFERENCE_POSTGRES:-}" ]; then
    timeout --foreground --kill-after=5 180s \
        "$PYTHON" "$BASE/t/interop/libpq-helper-wire.py" "$BASE" "$TMP/reference" \
        "$REFERENCE_POSTGRES" "${PORT:-7190}"
fi
