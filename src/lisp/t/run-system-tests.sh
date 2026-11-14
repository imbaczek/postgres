#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -z "${PG_LISP_TEST_RUN_DIR:-}" ]; then
    exec "$BASE/t/test-ramdisk.sh" run "$BASE/t/run-system-tests.sh" "$@"
fi
exec "${SBCL:-sbcl}" --noinform --disable-debugger --non-interactive --load "$BASE/t/run-system-tests.lisp" --end-toplevel-options "$@"
