#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -z "${PG_LISP_TEST_RUN_DIR:-}" ]; then
    exec "$BASE/t/test-ramdisk.sh" run python3 "$BASE/t/test-campaign.py" "$@"
fi
exec python3 "$BASE/t/test-campaign.py" "$@"
