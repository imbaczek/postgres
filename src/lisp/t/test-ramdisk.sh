#!/usr/bin/env bash
# Keep test data, compiler caches, and live logs in bounded, unswappable RAM.
set -euo pipefail
umask 077

uid=$(id -u)
gid=$(id -g)
root=/dev/shm/pg-lisp-tests-$uid
state=/dev/shm/pg-lisp-tests-$uid-control
source_name=pg-lisp-tests-$uid

fail() { printf '%s\n' "$*" >&2; exit 1; }

# Stable lock files live outside the mount so unmounting cannot replace a lease.
[[ ! -L $state ]] || fail "Refusing symlink $state"
mkdir -p "$state"
[[ $(stat -c %u:%a "$state") == "$uid:700" ]] || fail "Unsafe control directory $state"
exec 8>>"$state/control.lock"
flock -x 8

verify_mount() {
    local actual_source actual_type options
    mountpoint -q "$root" || fail "RAM workspace is not mounted at $root"
    read -r actual_source actual_type options < <(findmnt -rn -M "$root" -o SOURCE,FSTYPE,OPTIONS)
    [[ $actual_source == "$source_name" && $actual_type == tmpfs ]] || fail "Unexpected mount at $root"
    [[ ,$options, == *,noswap,* ]] || fail "RAM workspace permits swapping"
    [[ ,$options, != *,noexec,* ]] || fail "RAM workspace cannot execute test helpers"
    [[ $(stat -c %u:%a "$root") == "$uid:700" ]] || fail "Unsafe workspace permissions"
}

verify_runs() {
    [[ -d $root/runs && ! -L $root/runs ]] || fail "Unsafe runs directory"
    [[ $(stat -c %u:%a "$root/runs") == "$uid:700" ]] || fail "Unsafe runs permissions"
    [[ $(findmnt -rn -T "$root/runs" -o TARGET) == "$root" ]] || fail "Runs directory is outside RAM workspace"
}

setup() {
    local size=${1:-4G}
    [[ $size =~ ^[1-9][0-9]*[MG]$ ]] || fail "Size must be a positive integer followed by M or G"
    [[ ! -L $root ]] || fail "Refusing symlink $root"
    if mountpoint -q "$root"; then
        verify_mount
    else
        mkdir -p "$root"
        [[ $(stat -c %u:%a "$root") == "$uid:700" ]] || fail "Unsafe mount directory"
        local entries
        entries=$(bash -c 'shopt -s nullglob dotglob; a=("$1"/*); echo "${#a[@]}"' _ "$root")
        [[ $entries == 0 ]] || fail "Mount directory is not empty"
        sudo -n mount -t tmpfs -o "size=$size,noswap,uid=$uid,gid=$gid,mode=0700,nodev,nosuid" "$source_name" "$root"
        verify_mount
    fi
    [[ ! -L $root/runs ]] || fail "Refusing symlink runs directory"
    mkdir -p "$root/runs"
    verify_runs
    printf '%s\n' "$root"
}

run() {
    [[ $# -gt 0 ]] || fail "run requires a command"
    [[ ${1:-} != -- ]] || shift
    [[ $# -gt 0 ]] || fail "run requires a command"
    verify_mount
    verify_runs
    exec 9>>"$state/usage.lock"
    flock -s 9
    local job
    job=$(mktemp -d "$root/runs/run.XXXXXX")
    mkdir "$job/tmp" "$job/cache" "$job/logs"
    flock -u 8
    exec 8>&-
    export TMPDIR="$job/tmp" XDG_CACHE_HOME="$job/cache"
    export PG_LISP_TEST_RUN_DIR="$job" PG_LISP_TEST_LOG_DIR="$job/logs"
    printf 'RAM test workspace %s\n' "$job" >&2
    exec "$@"
}

teardown() {
    exec 9>>"$state/usage.lock"
    flock -n -x 9 || fail "RAM workspace has active jobs"
    if ! mountpoint -q "$root"; then
        [[ ! -e $root ]] || rmdir "$root"
        return
    fi
    verify_mount
    if [[ $# -gt 0 ]]; then
        local archive job
        mkdir -p "$1"
        archive=$(realpath "$1")
        [[ $archive != "$root" && $archive != "$root/"* ]] || fail "Archive must be outside RAM workspace"
        archive=$(mktemp -d "$archive/ramdisk-logs.XXXXXX")
        for job in "$root"/runs/run.*; do
            [[ -d $job/logs && ! -L $job && ! -L $job/logs ]] || continue
            cp -a -- "$job/logs" "$archive/$(basename "$job")"
        done
        printf 'Archived logs to %s\n' "$archive"
    fi
    sudo -n umount "$root"
    rmdir "$root"
}

case ${1:-} in
    setup) shift; [[ $# -le 1 ]] || fail "setup accepts one size"; setup "$@" ;;
    run) shift; run "$@" ;;
    teardown) shift; [[ $# -le 1 ]] || fail "teardown accepts one archive directory"; teardown "$@" ;;
    *) fail "Usage: $0 setup [4G] | run COMMAND [ARG...] | teardown [ARCHIVE_DIRECTORY]" ;;
esac
