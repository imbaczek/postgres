# Per-run data, compiler cache and logs for the direct and campaign launchers.
test_create_workspace() {
    PG_LISP_TEST_RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/$1.XXXXXX")
    PG_LISP_TEST_RUN_DIR=$(CDPATH= cd -- "$PG_LISP_TEST_RUN_DIR" && pwd -P)
    mkdir -m 700 "$PG_LISP_TEST_RUN_DIR/tmp" "$PG_LISP_TEST_RUN_DIR/cache" "$PG_LISP_TEST_RUN_DIR/logs"
    TMPDIR="$PG_LISP_TEST_RUN_DIR/tmp"
    XDG_CACHE_HOME="$PG_LISP_TEST_RUN_DIR/cache"
    PYTHONPYCACHEPREFIX="$XDG_CACHE_HOME/python"
    PG_LISP_TEST_LOG_DIR="$PG_LISP_TEST_RUN_DIR/logs"
    export PG_LISP_TEST_RUN_DIR TMPDIR XDG_CACHE_HOME PYTHONPYCACHEPREFIX PG_LISP_TEST_LOG_DIR
    printf 'Test workspace %s\n' "$PG_LISP_TEST_RUN_DIR" >&2
}
