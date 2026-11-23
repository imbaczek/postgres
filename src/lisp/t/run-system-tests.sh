#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -z "${PG_LISP_TEST_RUN_DIR:-}" ]; then
    PG_LISP_TEST_RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/pg-lisp-tests.XXXXXX")
    PG_LISP_TEST_RUN_DIR=$(CDPATH= cd -- "$PG_LISP_TEST_RUN_DIR" && pwd -P)
    mkdir "$PG_LISP_TEST_RUN_DIR/tmp" "$PG_LISP_TEST_RUN_DIR/cache" "$PG_LISP_TEST_RUN_DIR/logs"
    TMPDIR="$PG_LISP_TEST_RUN_DIR/tmp"
    XDG_CACHE_HOME="$PG_LISP_TEST_RUN_DIR/cache"
    PYTHONPYCACHEPREFIX="$XDG_CACHE_HOME/python"
    PG_LISP_TEST_LOG_DIR="$PG_LISP_TEST_RUN_DIR/logs"
    export PG_LISP_TEST_RUN_DIR TMPDIR XDG_CACHE_HOME PYTHONPYCACHEPREFIX PG_LISP_TEST_LOG_DIR
    printf 'Test workspace %s\n' "$PG_LISP_TEST_RUN_DIR" >&2
fi
# An explicitly supplied workspace still owns temporary test data.
if [ -z "${TMPDIR:-}" ]; then
    if [ -d "$PG_LISP_TEST_RUN_DIR/tmp" ]; then
        TMPDIR="$PG_LISP_TEST_RUN_DIR/tmp"
    else
        TMPDIR="$PG_LISP_TEST_RUN_DIR"
    fi
    export TMPDIR
fi
exec "${SBCL:-sbcl}" --noinform --disable-debugger --non-interactive --load "$BASE/t/run-system-tests.lisp" --end-toplevel-options "$@"
