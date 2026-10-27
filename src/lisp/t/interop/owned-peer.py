"""Register test servers before exec so outer cleanup survives driver death."""
import json
import os
import select
import signal
import stat
import sys
from pathlib import Path


def start_ticks(pid):
    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])


def checked_directory(registry, directory):
    directory = Path(directory).resolve()
    assert directory.is_relative_to(registry.parent.resolve())
    assert directory != registry.parent.resolve()
    return directory


def register_and_exec(registry, directory, arguments):
    registry.mkdir(mode=0o700, exist_ok=True)
    node = registry.lstat()
    assert stat.S_ISDIR(node.st_mode) and node.st_uid == os.getuid() and not node.st_mode & 0o077
    directory = checked_directory(registry, directory)
    pid, ticks = os.getpid(), start_ticks(os.getpid())
    record = {"pid": pid, "start_ticks": ticks, "pgdata": str(directory)}
    temporary = registry / f"{pid}-{ticks}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(record, output)
    temporary.rename(registry / f"{pid}-{ticks}.json")
    if (registry / "closed").exists():
        return
    os.execv(arguments[0], arguments)


def stop_record(registry, path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as input_:
        node = os.fstat(input_.fileno())
        assert stat.S_ISREG(node.st_mode) and node.st_uid == os.getuid() and node.st_size <= 4096
        record = json.load(input_)
    pid, ticks = record["pid"], record["start_ticks"]
    assert type(pid) is int and pid > 0 and type(ticks) is int and ticks > 0
    assert path.name == f"{pid}-{ticks}.json"
    directory = checked_directory(registry, record["pgdata"])
    try:
        process = os.pidfd_open(pid)
    except ProcessLookupError:
        path.unlink()
        return
    try:
        if not select.select([process], [], [], 0)[0]:
            if start_ticks(pid) != ticks:
                path.unlink()
                return
            arguments = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            actual = None
            if b"-D" in arguments:
                actual = Path(os.fsdecode(arguments[arguments.index(b"-D") + 1])).resolve()
            if actual != directory:
                assert select.select([process], [], [], 0)[0], f"PGDATA mismatch for owned peer {pid}"
            else:
                try:
                    signal.pidfd_send_signal(process, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                assert select.select([process], [], [], 5)[0], f"owned peer {pid} did not exit"
                print(f"owned-peer: stopped registered PID {pid}", flush=True)
        path.unlink()
    finally:
        os.close(process)


def cleanup(registry):
    node = registry.lstat()
    assert stat.S_ISDIR(node.st_mode) and node.st_uid == os.getuid() and not node.st_mode & 0o077
    (registry / "closed").touch()
    failed = False
    for path in sorted(registry.glob("*.json")):
        try:
            stop_record(registry, path)
        except Exception as error:
            print(f"owned-peer: refusing unchecked cleanup of {path}: {error}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    action, registry = sys.argv[1], Path(sys.argv[2])
    if action == "start":
        register_and_exec(registry, sys.argv[3], sys.argv[4:])
    else:
        assert action == "cleanup"
        sys.exit(cleanup(registry))
