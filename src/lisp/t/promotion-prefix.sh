#!/bin/sh
# Canonical pending child installation; no history/EOR or writer authority yet.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-6448}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-prefix.XXXXXX")
HELPER=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$HELPER" ]; then kill -KILL "$HELPER" 2>/dev/null || :; wait "$HELPER" 2>/dev/null || :; fi
    echo "promotion-prefix: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ -n "${PROMOTION_SEED:-}" ]; then
    SEED=$PROMOTION_SEED
else
    PORT=$PORT "$BASE/t/physical-restartpoint-auto.sh" >"$TMP/seed.log" 2>&1
    SEED=$(sed -n 's/^physical-restartpoint-auto: artifacts=//p' "$TMP/seed.log")
fi
test -d "$SEED/copy"
test -d "$SEED/source"
for point in normal after-prefix-fsync after-prefix-directory-fsync after-child-unlink \
    after-child-unlink-directory-fsync after-child-rename after-child-directory-fsync; do
    CASE=$TMP/$point
    mkdir -m 700 "$CASE"
    cp -a "$SEED/copy" "$CASE/data"
    before=$(sha256sum "$CASE/data/global/pg_wal_control")
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" \
        PG_PROMOTION_PAUSE="$point" PG_PROMOTION_MARKER="$CASE/marker" \
        "$SBCL" --script "$BASE/t/promotion-prefix-native.lisp" >"$CASE/publisher.log" 2>&1 &
    HELPER=$!
    if [ "$point" = normal ]; then
        wait "$HELPER"
        HELPER=
    else
        attempt=0
        while [ ! -f "$CASE/marker" ]; do
            kill -0 "$HELPER" 2>/dev/null || { cat "$CASE/publisher.log" >&2; exit 1; }
            attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
        done
        read -r marked_pid <"$CASE/marker"
        test "$marked_pid" = "$HELPER"
        kill -KILL "$HELPER"
        wait "$HELPER" 2>/dev/null || :
        HELPER=
    fi
    case "$point" in
        after-prefix-*) expected=stale ;;
        after-child-unlink*) expected=absent ;;
        *) expected=installed ;;
    esac
    python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --check-child-prefix \
        "$CASE/data" "$SEED/source" "$expected"
    python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --snapshot-pending \
        "$CASE/data" "$SEED/source" "$CASE/before.json"
    set +e
    timeout 15 "$BASE/pg-lisp.sh" -p "$PORT" -D "$CASE/data" >"$CASE/startup.log" 2>&1
    startup_status=$?
    set -e
    test "$startup_status" -eq 1
    grep -q 'pending native promotion requires reconciliation' "$CASE/startup.log"
    python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --verify-pending \
        "$CASE/data" "$CASE/before.json"
    test "$before" = "$(sha256sum "$CASE/data/global/pg_wal_control")"
done
echo 'promotion-prefix: canonical installation and process-crash boundaries PASS'
