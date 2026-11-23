#!/bin/sh
# Give each campaign command its own workspace unless already inside one.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -z "${PG_LISP_TEST_RUN_DIR:-}" ]; then
    . "$BASE/t/test-workspace.sh"
    test_create_workspace run
fi
exec python3 "$BASE/t/test-campaign.py" "$@"
