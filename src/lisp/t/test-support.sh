# Shared helpers for server-backed shell tests.

test_debug_setup() {
    TEST_DEBUG_NAME=$1
    TEST_DEBUG_SQL_PORT=$2
    TEST_DEBUG_TMP=$3
    TEST_DEBUG_LOG=$4
    TEST_DEBUG_ON_FAILURE=${DEBUG_ON_FAILURE:-0}
    TEST_DEBUG_SWANK_PORT=${DEBUG_SWANK_PORT:-}
    TEST_DEBUG_SQL_PORT_DECIMAL=$TEST_DEBUG_SQL_PORT
    case "$TEST_DEBUG_SQL_PORT_DECIMAL" in
        ''|*[!0-9]*) TEST_DEBUG_SQL_PORT_DECIMAL=0 ;;
    esac
    while [ "${#TEST_DEBUG_SQL_PORT_DECIMAL}" -gt 1 ] \
            && [ "${TEST_DEBUG_SQL_PORT_DECIMAL#0}" != "$TEST_DEBUG_SQL_PORT_DECIMAL" ]; do
        TEST_DEBUG_SQL_PORT_DECIMAL=${TEST_DEBUG_SQL_PORT_DECIMAL#0}
    done
    if [ "$TEST_DEBUG_ON_FAILURE" = 1 ] && [ -z "$TEST_DEBUG_SWANK_PORT" ]; then
        TEST_DEBUG_SWANK_PORT=$((TEST_DEBUG_SQL_PORT_DECIMAL + 1000))
    fi
    TEST_DEBUG_INTERRUPTED=0
    TEST_DEBUG_EXITING=0
}

test_debug_start_server() {
    TEST_DEBUG_START_LOG=$1
    shift
    if [ "$TEST_DEBUG_ON_FAILURE" = 1 ]; then
        "$BASE/pg-lisp.sh" "$@" --swank-port "$TEST_DEBUG_SWANK_PORT" \
            >> "$TEST_DEBUG_START_LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" "$@" >> "$TEST_DEBUG_START_LOG" 2>&1 &
    fi
    SRV=$!
}

test_debug_install_traps() {
    TEST_DEBUG_CLEANUP=$1
    trap 'test_debug_exit "$?"' EXIT
    trap 'test_debug_signal 130' INT
    trap 'test_debug_signal 143' TERM
}

test_debug_signal() {
    TEST_DEBUG_INTERRUPTED=1
    if [ "$TEST_DEBUG_EXITING" = 1 ]; then
        TEST_DEBUG_STATUS=$1
    else
        exit "$1"
    fi
}

test_debug_exit() {
    TEST_DEBUG_STATUS=$1
    trap - EXIT
    TEST_DEBUG_EXITING=1

    if [ "$TEST_DEBUG_STATUS" -ne 0 ] \
            && [ "$TEST_DEBUG_ON_FAILURE" = 1 ] \
            && [ "$TEST_DEBUG_INTERRUPTED" = 0 ]; then
        TEST_DEBUG_SERVER_STATE=
        if [ -n "${SRV:-}" ]; then
            TEST_DEBUG_SERVER_STATE=$(ps -o stat= -p "$SRV" 2>/dev/null || true)
        fi
        case "$TEST_DEBUG_SERVER_STATE" in
            ''|Z*|X*)
                echo "$TEST_DEBUG_NAME: test failed after the server exited; no live Swank session to attach" >&2
                ;;
            *)
                echo "$TEST_DEBUG_NAME: test failed; pausing before cleanup for debugging" >&2
                echo "  SQL:    127.0.0.1:$TEST_DEBUG_SQL_PORT_DECIMAL" >&2
                echo "  Swank:  127.0.0.1:$TEST_DEBUG_SWANK_PORT" >&2
                echo "Connect with M-x slime-connect (host 127.0.0.1, port $TEST_DEBUG_SWANK_PORT)." >&2
                echo "  PGDATA: $TEST_DEBUG_TMP/data" >&2
                echo "  log:    $TEST_DEBUG_LOG" >&2
                if [ -t 0 ]; then
                    echo "Press Enter here to clean up and exit." >&2
                    IFS= read -r TEST_DEBUG_RESUME || :
                else
                    echo "stdin is not a terminal, so the debug pause is skipped; run this test from a terminal to attach." >&2
                fi
                ;;
        esac
    fi

    "$TEST_DEBUG_CLEANUP" || true
    exit "$TEST_DEBUG_STATUS"
}
