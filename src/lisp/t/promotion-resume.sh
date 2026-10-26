#!/bin/sh
# Fresh-process pending reconstruction; ordinary startup still refuses writer admission.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-6452}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-resume.XXXXXX")
HELPER=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$HELPER" ]; then kill -KILL "$HELPER" 2>/dev/null || :; wait "$HELPER" 2>/dev/null || :; fi
    echo "promotion-resume: artifacts=$TMP"
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
if [ -n "${PROMOTION_PREFIX_SEED:-}" ]; then
    PREFIX_SEED=$PROMOTION_PREFIX_SEED
else
    PROMOTION_SEED=$SEED PORT=$PORT "$BASE/t/promotion-prefix.sh" >"$TMP/prefix.log" 2>&1
    PREFIX_SEED=$(sed -n 's/^promotion-prefix: artifacts=//p' "$TMP/prefix.log")
fi
if [ -n "${PROMOTION_WAL_SEED:-}" ]; then
    WAL_SEED=$PROMOTION_WAL_SEED
else
    PROMOTION_SEED=$SEED PORT=$PORT "$BASE/t/promotion-wal.sh" >"$TMP/wal.log" 2>&1
    WAL_SEED=$(sed -n 's/^promotion-prefix: artifacts=//p' "$TMP/wal.log")
fi
inspect() { python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" "$@"; }
resume() {
    timeout -k 2 20 env PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" \
        "$SBCL" --script "$BASE/t/promotion-resume-native.lisp" >"$CASE/resume.log" 2>&1
}
verify() {
    grep -q 'promotion-resume-native: restored.* PASS' "$CASE/resume.log"
    inspect --check-promotion-eor "$CASE/data" "$SEED/source" eor
    inspect --snapshot-pending "$CASE/data" "$SEED/source" "$CASE/after.json"
    set +e
    timeout -k 2 15 "$BASE/pg-lisp.sh" -p "$PORT" -D "$CASE/data" >"$CASE/startup.log" 2>&1
    startup_status=$?
    set -e
    test "$startup_status" -eq 1
    grep -q 'pending native promotion requires reconciliation' "$CASE/startup.log"
    inspect --verify-pending "$CASE/data" "$CASE/after.json"
}
for stage in prefix wal; do
    if [ "$stage" = prefix ]; then
        stage_seed=$PREFIX_SEED
        points='normal after-prefix-fsync after-prefix-directory-fsync after-child-unlink after-child-unlink-directory-fsync after-child-rename after-child-directory-fsync'
    else
        stage_seed=$WAL_SEED
        points='normal before-history after-history-install after-history before-eor-write after-eor-write after-eor-fsync after-eor-directory-fsync'
    fi
    for point in $points; do
        CASE=$TMP/$stage-$point
        mkdir -m 700 "$CASE"
        cp -a "$stage_seed/$point/data" "$CASE/data"
        before=$(sha256sum "$CASE/data/physical-recovery.control" "$CASE/data/global/pg_wal_control" "$CASE/data/physical-restartpoint.control")
        resume
        verify
        test "$before" = "$(sha256sum "$CASE/data/physical-recovery.control" "$CASE/data/global/pg_wal_control" "$CASE/data/physical-restartpoint.control")"
        echo "promotion-resume: $stage/$point PASS"
    done
done
for damage in torn-1 torn-17 torn-55 torn-56 unacknowledged ahead-catalog; do
    CASE=$TMP/$damage
    mkdir -m 700 "$CASE"
    cp -a "$WAL_SEED/normal/data" "$CASE/data"
    inspect --damage-resume "$CASE/data" "$damage"
    if [ "$damage" = unacknowledged ]; then inspect --snapshot-storage "$CASE/data" "$CASE/before.json"; fi
    resume
    verify
    if [ "$damage" = unacknowledged ]; then inspect --verify-pending "$CASE/data" "$CASE/before.json"; fi
done
for damage in marker pointer generation allocation receive backlog lock child missing-child history base-control fork system valid-tail oversized-tail; do
    CASE=$TMP/reject-$damage
    mkdir -m 700 "$CASE"
    cp -a "$WAL_SEED/normal/data" "$CASE/data"
    # Make premature catalog restoration observable even when validation fails later.
    inspect --damage-resume "$CASE/data" ahead-catalog
    inspect --damage-resume "$CASE/data" "$damage"
    inspect --snapshot-storage "$CASE/data" "$CASE/before.json"
    set +e
    resume
    resume_status=$?
    set -e
    test "$resume_status" -eq 1
    grep -q 'promotion-resume-native: failed=T' "$CASE/resume.log"
    case "$damage" in
        backlog) grep -q 'promotion must replay complete durable receive backlog' "$CASE/resume.log" ;;
        allocation) grep -q 'promotion lacks exact durable restartpoint, checkpoint or allocation proof' "$CASE/resume.log" ;;
        fork) grep -q 'still-live base relation fork is missing' "$CASE/resume.log" ;;
        valid-tail) grep -q 'pending child contains a different complete record' "$CASE/resume.log" ;;
        oversized-tail) grep -q 'pending child prefix or EOR extent differs' "$CASE/resume.log" ;;
    esac
    inspect --verify-pending "$CASE/data" "$CASE/before.json"
    echo "promotion-resume: reject $damage PASS"
done
for point in before-pending-restore after-pending-restore; do
    CASE=$TMP/crash-$point
    mkdir -m 700 "$CASE"
    cp -a "$PREFIX_SEED/after-child-unlink/data" "$CASE/data"
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" PG_PROMOTION_PAUSE="$point" \
        PG_PROMOTION_MARKER="$CASE/marker" "$SBCL" --script "$BASE/t/promotion-resume-native.lisp" \
        >"$CASE/interrupted.log" 2>&1 &
    HELPER=$!
    attempt=0
    while [ ! -f "$CASE/marker" ]; do
        kill -0 "$HELPER" 2>/dev/null || { cat "$CASE/interrupted.log" >&2; exit 1; }
        attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
    done
    read -r marked_pid <"$CASE/marker"
    test "$marked_pid" = "$HELPER"
    kill -KILL "$HELPER"
    wait "$HELPER" 2>/dev/null || :
    HELPER=
    resume
    verify
done
echo 'promotion-resume: fresh pending recovery, rejection and repeated process crashes PASS'
