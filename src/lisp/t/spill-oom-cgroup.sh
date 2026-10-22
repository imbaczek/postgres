#!/bin/sh
# t/spill-oom-cgroup.sh -- verify spill recovery after an isolated server OOM.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5488}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 1))}
SBCL=${SBCL:-sbcl}
ROWS=${ROWS:-30000}
MEMORY_MAX=${MEMORY_MAX:-805306368}
MEMORY_RESERVE=${MEMORY_RESERVE:-268435456}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-spill-oom.XXXXXX")
PGDATA="$TMP/data"
LOG="$TMP/server.log"
SRV=
QUERY=
OOM_CLIENT=
CGROUP=
CGROUP_PARENT=
CGROUP_SUPERVISOR=
SUPERVISOR_MOVED=0
ENABLED_MEMORY=0
CGROUP_CREATED=0
ANCESTOR_EVENTS="$TMP/ancestor-oom-events"
. "$BASE/t/test-support.sh"
test_debug_setup spill-oom-cgroup "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/spill-release" 2>/dev/null || true
    [ -z "$OOM_CLIENT" ] || kill "$OOM_CLIENT" 2>/dev/null || true
    [ -z "$QUERY" ] || kill "$QUERY" 2>/dev/null || true
    if [ -n "$SRV" ]; then
      if [ "$CGROUP_CREATED" = 1 ] && [ -n "$CGROUP" ] \
          && [ -w "$CGROUP/cgroup.kill" ]; then
            printf '1\n' > "$CGROUP/cgroup.kill" 2>/dev/null || true
        fi
        kill -KILL "$SRV" 2>/dev/null || true
    fi
    [ -z "$OOM_CLIENT" ] || wait "$OOM_CLIENT" 2>/dev/null || true
    [ -z "$QUERY" ] || wait "$QUERY" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
      if [ "$CGROUP_CREATED" = 1 ] && [ -n "$CGROUP" ]; then
        i=0
        while [ -s "$CGROUP/cgroup.procs" ] && [ "$i" -lt 50 ]; do
            i=$((i + 1))
            sleep 0.1
        done
        rmdir "$CGROUP" 2>/dev/null || true
    fi
    if [ "$ENABLED_MEMORY" = 1 ] && [ -n "$CGROUP_PARENT" ]; then
        printf '%s\n' '-memory' > "$CGROUP_PARENT/cgroup.subtree_control" 2>/dev/null || true
    fi
    if [ "$SUPERVISOR_MOVED" = 1 ]; then
        printf '%s\n' "$$" > "$CGROUP_PARENT/cgroup.procs" 2>/dev/null || true
    fi
    if [ -n "$CGROUP_SUPERVISOR" ]; then
        rmdir "$CGROUP_SUPERVISOR" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "spill-oom-cgroup: FAIL $1" >&2
    [ ! -f "$LOG" ] || cat "$LOG" >&2
    [ ! -f "$TMP/oom-control.out" ] || cat "$TMP/oom-control.out" >&2
    exit 1
}

skip() {
    echo "spill-oom-cgroup: SKIP $1" >&2
    exit 0
}

run() {
    PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
        -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

control() {
    { cat; printf '%s\n' '(quit)'; } \
        | timeout 180 "$SBCL" --script "$BASE/t/control-client.lisp" \
            tcp "127.0.0.1:$CONTROL_PORT"
}

read_memory_event() {
    awk -v event="$2" \
        '$1 == event { print $2; found = 1 } END { if (!found) exit 1 }' \
        "$1"
}

CGROUP_PATH=$(awk -F: '$1 == "0" { print $3; exit }' /proc/self/cgroup)
MOUNT_LINE=$(awk '$0 ~ / - cgroup2 / { print; exit }' /proc/self/mountinfo)
[ -n "$CGROUP_PATH" ] && [ -n "$MOUNT_LINE" ] \
    || skip "cgroup v2 is unavailable"
CGROUP_MOUNT=$(printf '%s\n' "$MOUNT_LINE" | awk '{ print $5 }')
MOUNT_ROOT=$(printf '%s\n' "$MOUNT_LINE" | awk '{ print $4 }')
if [ "$MOUNT_ROOT" = / ]; then
    CGROUP_RELATIVE=$CGROUP_PATH
else
    case "$CGROUP_PATH" in
        "$MOUNT_ROOT"/*) CGROUP_RELATIVE=${CGROUP_PATH#"$MOUNT_ROOT"} ;;
        *) skip "the current cgroup is outside the mounted cgroup subtree" ;;
    esac
fi
CGROUP_PARENT="${CGROUP_MOUNT%/}${CGROUP_RELATIVE%/}"
[ -r "$CGROUP_PARENT/cgroup.controllers" ] \
    || skip "the current cgroup controller files are not readable"
grep -qw memory "$CGROUP_PARENT/cgroup.controllers" \
    || skip "the memory controller is unavailable to this cgroup"
[ -w "$CGROUP_PARENT/cgroup.subtree_control" ] \
    || skip "the current cgroup does not delegate subtree control"
if ! grep -qw memory "$CGROUP_PARENT/cgroup.subtree_control"; then
    # Domain controllers require an empty parent. Only move this harness;
    # never rearrange unrelated processes in a shared scope such as tmux.
    while IFS= read -r pid; do
        [ "$pid" = "$$" ] \
            || skip "the current cgroup is shared; run with systemd-run --user --scope -p Delegate=memory $BASE/t/spill-oom-cgroup.sh"
    done < "$CGROUP_PARENT/cgroup.procs"
    CGROUP_SUPERVISOR="$CGROUP_PARENT/pg-lisp-spill-supervisor-$$"
    if ! mkdir "$CGROUP_SUPERVISOR" 2>/dev/null; then
        CGROUP_SUPERVISOR=
        skip "cannot create a delegated supervisor cgroup"
    fi
    printf '%s\n' "$$" > "$CGROUP_SUPERVISOR/cgroup.procs" \
        || skip "cannot move the harness into its supervisor cgroup"
    SUPERVISOR_MOVED=1
    if printf '%s\n' '+memory' > "$CGROUP_PARENT/cgroup.subtree_control" 2>/dev/null; then
        ENABLED_MEMORY=1
    else
        skip "the memory controller cannot be enabled for a child cgroup"
    fi
fi

case "$MEMORY_MAX:$MEMORY_RESERVE" in
    *[!0-9:]*|:*|*:)
        skip "MEMORY_MAX and MEMORY_RESERVE must be nonnegative byte counts" ;;
esac
[ "$MEMORY_MAX" -gt 0 ] \
    || skip "MEMORY_MAX must be greater than zero"

host_available_kb=$(awk '$1 == "MemAvailable:" { print $2; exit }' /proc/meminfo)
[ -n "$host_available_kb" ] \
    || skip "cannot determine host memory availability"
host_required_kb=$(((MEMORY_MAX + MEMORY_RESERVE + 1023) / 1024))
[ "$host_available_kb" -ge "$host_required_kb" ] \
    || skip "host has less than $host_required_kb KiB of available memory"

: > "$ANCESTOR_EVENTS"
ancestor=$CGROUP_PARENT
while :; do
    # The actual hierarchy root has no memory limit or local event files.
    # A mounted subtree root still has controls and must be checked below.
    if [ "$ancestor" = "$CGROUP_MOUNT" ] && [ "$MOUNT_ROOT" = / ] \
        && [ ! -e "$ancestor/memory.max" ]; then
        break
    fi
    [ -r "$ancestor/memory.max" ] \
        && [ -r "$ancestor/memory.current" ] \
        && [ -r "$ancestor/memory.events.local" ] \
        || skip "cannot verify memory headroom and local OOM events at $ancestor"
    IFS= read -r ancestor_max < "$ancestor/memory.max"
    if [ "$ancestor_max" != max ]; then
        IFS= read -r ancestor_current < "$ancestor/memory.current"
        required_headroom=$((MEMORY_MAX + MEMORY_RESERVE))
        available_headroom=$((ancestor_max - ancestor_current))
        [ "$available_headroom" -ge "$required_headroom" ] \
            || skip "ancestor $ancestor has less than $required_headroom bytes of memory headroom"
    fi
    ancestor_max_before=$(read_memory_event "$ancestor/memory.events.local" max) \
        || skip "ancestor $ancestor does not report local memory.max events"
    ancestor_oom_before=$(read_memory_event "$ancestor/memory.events.local" oom) \
        || skip "ancestor $ancestor does not report local OOM events"
    ancestor_oom_kill_before=$(read_memory_event "$ancestor/memory.events.local" oom_kill) \
        || skip "ancestor $ancestor does not report local OOM kill events"
    printf '%s\t%s\t%s\t%s\n' "$ancestor" "$ancestor_max_before" \
        "$ancestor_oom_before" "$ancestor_oom_kill_before" \
        >> "$ANCESTOR_EVENTS"
    [ "$ancestor" = "$CGROUP_MOUNT" ] && break
    parent=${ancestor%/*}
    [ -n "$parent" ] || parent=/
    [ "$parent" != "$ancestor" ] || break
    ancestor=$parent
done

CGROUP="$CGROUP_PARENT/pg-lisp-spill-oom-$$"
if ! mkdir "$CGROUP" 2>/dev/null; then
    skip "cannot create a delegated child cgroup"
fi
CGROUP_CREATED=1
[ -w "$CGROUP/memory.max" ] && [ -w "$CGROUP/cgroup.procs" ] \
    || skip "the child cgroup does not expose writable memory controls"
printf '%s\n' "$MEMORY_MAX" > "$CGROUP/memory.max" \
    || skip "cannot set the child memory limit"
[ -w "$CGROUP/memory.swap.max" ] \
    || skip "the child cgroup cannot bound swap use"
printf '0\n' > "$CGROUP/memory.swap.max" \
    || skip "cannot disable swap for the child cgroup"
[ -w "$CGROUP/memory.oom.group" ] \
    || skip "the child cgroup cannot guarantee whole-server OOM termination"
printf '1\n' > "$CGROUP/memory.oom.group" \
    || skip "cannot configure whole-server OOM termination"
test -r "$CGROUP/memory.events" \
    || skip "the child cgroup does not expose OOM event counters"
test -r "$CGROUP/memory.events.local" \
    || skip "the child cgroup does not expose local OOM event counters"
CHILD_MAX_BEFORE=$(read_memory_event "$CGROUP/memory.events.local" max) \
    || skip "the child cgroup does not report memory.max events"
CHILD_OOM_BEFORE=$(read_memory_event "$CGROUP/memory.events.local" oom) \
    || skip "the child cgroup does not report OOM events"
OOM_KILL_BEFORE=$(read_memory_event "$CGROUP/memory.events.local" oom_kill) \
    || skip "the child cgroup does not report local OOM kill events"

cat > "$TMP/enter-cgroup" <<'EOF'
#!/bin/sh
printf '%s\n' "$$" > "$PG_SPILL_CGROUP/cgroup.procs"
exec "$@"
EOF
chmod +x "$TMP/enter-cgroup"

start_server() {
    mode=${1:-regular}
    if [ "$mode" = oom ]; then
        PG_QUERY_TEMP_TEST_READY_FILE="$TMP/spill-ready" \
            PG_QUERY_TEMP_TEST_RELEASE_FILE="$TMP/spill-release" \
            PG_SPILL_CGROUP="$CGROUP" \
            "$TMP/enter-cgroup" "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            --control-port "$CONTROL_PORT" >> "$LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            --control-port "$CONTROL_PORT" >> "$LOG" 2>&1 &
    fi
    SRV=$!
    i=0
    while [ "$i" -lt 150 ]; do
        if run -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SRV" 2>/dev/null; then
            fail "server exited during startup"
        fi
        i=$((i + 1))
        sleep 0.2
    done
    fail "server did not start"
}

wait_for_spill() {
    i=0
    while [ "$i" -lt 3000 ]; do
        [ ! -f "$TMP/spill-ready" ] || return 0
        if ! kill -0 "$SRV" 2>/dev/null; then
            fail "server exited before writing a spill record"
        fi
        if [ -n "$QUERY" ] && ! kill -0 "$QUERY" 2>/dev/null; then
            fail "sort query exited before creating a spill file"
        fi
        i=$((i + 1))
        sleep 0.02
    done
    fail "timed out waiting for the spill-write marker"
}

start_server oom
run -c "create table spill_oom_rows (id int)" >/dev/null
awk -v rows="$ROWS" -v batch=1000 'BEGIN {
    for (i = 1; i <= rows; i++) {
        if ((i - 1) % batch == 0) printf "insert into spill_oom_rows values "
        printf "%s(%d)", (i - 1) % batch == 0 ? "" : ", ", i
        if (i % batch == 0 || i == rows) print ";"
    }
}' > "$TMP/populate.sql"
run < "$TMP/populate.sql" > "$TMP/populate.out" 2>&1 \
    || fail "could not populate OOM sort input"
control <<'LISP' >/dev/null
(setf pg::*query-work-memory-bytes* 1)
LISP
run -c "select id from spill_oom_rows order by id" \
    > "$TMP/sort.out" 2>&1 &
QUERY=$!
wait_for_spill
spill_file=$(find "$PGDATA/pgsql_tmp" -type f -name 'pg-query-*.tmp' -print -quit)
[ -n "$spill_file" ] && [ -s "$spill_file" ] \
    || fail "the cgroup test did not observe a nonempty spill file"

(
    control <<'LISP'
(let ((chunks nil))
  (loop repeat 80
        do (push (make-array (* 32 1024 1024)
                             :element-type '(unsigned-byte 8)
                             :initial-element #xa5)
                 chunks)
           (sleep 0.05))
  (length chunks))
LISP
) > "$TMP/oom-control.out" 2>&1 &
OOM_CLIENT=$!
i=0
OOM_KILL_AFTER=$OOM_KILL_BEFORE
CHILD_MAX_AFTER=$CHILD_MAX_BEFORE
CHILD_OOM_AFTER=$CHILD_OOM_BEFORE
while [ "$i" -lt 1200 ]; do
    OOM_KILL_AFTER=$(read_memory_event "$CGROUP/memory.events.local" oom_kill)
    CHILD_MAX_AFTER=$(read_memory_event "$CGROUP/memory.events.local" max)
    CHILD_OOM_AFTER=$(read_memory_event "$CGROUP/memory.events.local" oom)
    [ "$OOM_KILL_AFTER" -gt "$OOM_KILL_BEFORE" ] && break
    if ! kill -0 "$SRV" 2>/dev/null && [ ! -s "$CGROUP/cgroup.procs" ]; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done
[ "$OOM_KILL_AFTER" -gt "$OOM_KILL_BEFORE" ] \
    || fail "the constrained server did not increment memory.events oom_kill"
[ "$CHILD_MAX_AFTER" -gt "$CHILD_MAX_BEFORE" ] \
    || fail "the child memory.max limit did not trigger during allocation"
[ "$CHILD_OOM_AFTER" -gt "$CHILD_OOM_BEFORE" ] \
    || fail "the child cgroup did not report an OOM event"
while IFS="$(printf '\t')" read -r ancestor ancestor_max_before \
        ancestor_oom_before ancestor_oom_kill_before; do
    ancestor_max_after=$(read_memory_event "$ancestor/memory.events.local" max)
    ancestor_oom_after=$(read_memory_event "$ancestor/memory.events.local" oom)
    ancestor_oom_kill_after=$(read_memory_event "$ancestor/memory.events.local" oom_kill)
    [ "$ancestor_max_after" = "$ancestor_max_before" ] \
        && [ "$ancestor_oom_after" = "$ancestor_oom_before" ] \
        && [ "$ancestor_oom_kill_after" = "$ancestor_oom_kill_before" ] \
        || fail "OOM activity escaped the isolated child cgroup at ancestor $ancestor"
done < "$ANCESTOR_EVENTS"
set +e
wait "$SRV"
server_status=$?
set -e
SRV=
[ "$server_status" -ne 0 ] \
    || fail "the server unexpectedly survived its cgroup OOM event"
wait "$OOM_CLIENT" 2>/dev/null || true
OOM_CLIENT=
wait "$QUERY" 2>/dev/null || true
QUERY=

start_server regular
stale_namespace=$(find "$PGDATA/pgsql_tmp" -mindepth 1 -maxdepth 1 \
    -type d -name 'pgsql_tmp-*' -print -quit)
[ -z "$stale_namespace" ] \
    || fail "restart retained the OOM-killed server namespace $stale_namespace"
count=$(run -c "select count(*) from spill_oom_rows")
[ "$count" = "$ROWS" ] \
    || fail "committed rows changed after cgroup OOM and restart"

kill -TERM "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
echo "spill-oom-cgroup: PASS (oom_kill advanced from $OOM_KILL_BEFORE to $OOM_KILL_AFTER; $count rows survived restart)"
