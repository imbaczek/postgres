# Shared helpers for server-backed shell tests.

# A successful psql probe alone can hit an unrelated server on an occupied port.
# Verify the launched PID owns the TCP listener before sending even a SQL probe.
test_wait_owned_sql_server() {
    TEST_WAIT_PID=$1
    TEST_WAIT_PORT=$2
    TEST_WAIT_PSQL=${3:-psql}
    TEST_WAIT_LIMIT=${4:-200}
    command -v ss >/dev/null || { echo 'SQL test readiness requires ss' >&2; return 1; }
    TEST_WAIT_ATTEMPT=0
    while [ "$TEST_WAIT_ATTEMPT" -lt "$TEST_WAIT_LIMIT" ]; do
        kill -0 "$TEST_WAIT_PID" 2>/dev/null || {
            echo "SQL test server $TEST_WAIT_PID exited before owning port $TEST_WAIT_PORT" >&2
            return 1
        }
        TEST_WAIT_LISTENER=$(ss -H -ltnp "sport = :$TEST_WAIT_PORT")
        if [ -n "$TEST_WAIT_LISTENER" ]; then
            if ! printf '%s\n' "$TEST_WAIT_LISTENER" | grep -F "pid=$TEST_WAIT_PID," >/dev/null; then
                if printf '%s\n' "$TEST_WAIT_LISTENER" | grep -Eq 'pid=[0-9]'; then
                    echo "SQL test port $TEST_WAIT_PORT belongs to another process: $TEST_WAIT_LISTENER" >&2
                    return 1
                fi
                # ss can see the listener before it can attribute its owner.
                # Keep waiting without probing SQL on an unverified endpoint.
                TEST_WAIT_ATTEMPT=$((TEST_WAIT_ATTEMPT + 1))
                sleep 0.1
                continue
            fi
            if PGCONNECT_TIMEOUT=1 "$TEST_WAIT_PSQL" -X -h 127.0.0.1 \
                    -p "$TEST_WAIT_PORT" -U postgres -d postgres -c '' >/dev/null 2>&1; then
                return 0
            fi
        fi
        TEST_WAIT_ATTEMPT=$((TEST_WAIT_ATTEMPT + 1))
        sleep 0.1
    done
    echo "SQL test server $TEST_WAIT_PID did not own port $TEST_WAIT_PORT" >&2
    return 1
}

test_wait_for_marker() (
    marker=$1
    attempts=$2
    while [ "$attempts" -gt 0 ]; do
        [ -f "$marker" ] && return 0
        attempts=$((attempts - 1))
        sleep 0.1
    done
    echo "Timed out waiting for marker $marker" >&2
    return 1
)

test_wait_for_log_text() (
    log=$1
    pattern=$2
    attempts=${3:-300}
    while [ "$attempts" -gt 0 ]; do
        grep -F -e "$pattern" "$log" >/dev/null 2>&1 && return 0
        attempts=$((attempts - 1))
        sleep 0.1
    done
    echo "Timed out waiting for '$pattern' in $log" >&2
    return 1
)

# Launch within the caller's private fixture directory. The caller records
# $! before waiting for readiness so its cleanup trap also covers startup failure.
test_launch_lisp_server() {
    TEST_LAUNCH_PORT=$1 TEST_LAUNCH_DATA=$2 TEST_LAUNCH_SOCKET=$3 TEST_LAUNCH_LOG=$4
    shift 4
    if [ -n "$TEST_LAUNCH_SOCKET" ]; then
        rm -f "$TEST_LAUNCH_SOCKET" || return
        set -- --control-socket "$TEST_LAUNCH_SOCKET" "$@"
    fi
    "$BASE/pg-lisp.sh" -p "$TEST_LAUNCH_PORT" -D "$TEST_LAUNCH_DATA" \
        "$@" >>"$TEST_LAUNCH_LOG" 2>&1 &
}

test_lisp_control() {
    TEST_CONTROL_FORM=$2
    TEST_CONTROL_TIMEOUT=${3-}
    set -- "${SBCL:-sbcl}" --script "$BASE/t/control-client.lisp" unix "$1"
    if [ -n "$TEST_CONTROL_TIMEOUT" ]; then
        set -- timeout "$TEST_CONTROL_TIMEOUT" "$@"
    fi
    printf '%s\n(quit)\n' "$TEST_CONTROL_FORM" | "$@"
}

test_lisp_control_checked() {
    test_lisp_control "$1" "$2" "${4-}" >"$3" || return
    if grep -Eq ';; (read )?error:' "$3"; then
        cat "$3" >&2
        return 1
    fi
}

# EXPLAIN emits a tree, so check for a node as an indented exact line instead
# of comparing the full plan output to a single scan node.
plan_contains_indented_node() {
    printf '%s\n' "$1" |
        awk -v expected="$2" '
            /^[[:space:]]/ {
                node = $0
                sub(/^[[:space:]]+/, "", node)
                if (node == expected)
                    found = 1
            }
            END { exit !found }'
}

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
