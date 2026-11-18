"""Exercise pinned Patroni's initial native primary bootstrap without a DCS."""

import ctypes
import importlib.metadata
import os
from pathlib import Path
import runpy
import signal
import socket
import subprocess
import tempfile

from patroni.postgresql import Postgresql
from patroni.postgresql.mpp import Null


FOLLOW = runpy.run_path(str(Path(__file__).with_name("patroni-follow.py")))
BIN = FOLLOW["BASE"] / "bin"


def check_rejected_targets(root):
    occupied = root / "occupied"
    occupied.mkdir(mode=0o700)
    marker = occupied / ".hidden"
    marker.write_bytes(b"keep")
    for target in (occupied, root / "linked"):
        if target != occupied:
            target.symlink_to(occupied, target_is_directory=True)
        result = subprocess.run([BIN / "initdb", "-D", target], capture_output=True, timeout=20)
        assert result.returncode == 1 and marker.read_bytes() == b"keep", result
        assert list(occupied.iterdir()) == [marker]
    unsupported = root / "unsupported"
    result = subprocess.run([BIN / "initdb", "--auth-host=scram-sha-256", unsupported],
                            capture_output=True, timeout=20)
    assert result.returncode == 2 and not unsupported.exists(), result
    failed = root / "failed"
    result = subprocess.run([BIN / "initdb", failed], capture_output=True, timeout=20,
                            env={**os.environ, "SBCL": str(root / "missing-sbcl")})
    assert result.returncode == 1 and not failed.exists(), result
    assert not list(root.glob(".pg-lisp-initdb-*"))


def check_sync_config(node, port):
    pid = node.is_running().pid
    names = "FIRST 1 (missing_peer)"
    show = "SHOW synchronous_standby_names"
    assert node.config.set_synchronous_standby_names(names) is True
    FOLLOW["wait_for"](lambda: FOLLOW["query"](port, show) == [(names,)])
    assert node.config.set_synchronous_standby_names(names) is None
    assert node.config.set_synchronous_standby_names(None) is True
    FOLLOW["wait_for"](lambda: FOLLOW["query"](port, show) == [("",)])
    assert node.is_running().pid == pid
    print("Patroni 4.1.5 synchronous standby names set, no-op and clear PASS", flush=True)


def check_bootstrap(root, precreated):
    root.mkdir(mode=0o700)
    data = root / "fresh pgdata"
    if precreated:
        data.mkdir(mode=0o700)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    node = Postgresql(FOLLOW["configuration"](data, port, root, major_version=160000), Null())
    start = node.start
    offline_identity = []

    def check_before_start(*args, **kwargs):
        facts = node.controldata()
        assert facts["Database cluster state"] == "shut down", facts
        assert not (data / "postmaster.pid").exists()
        assert (data / "PG_VERSION").stat().st_mode & 0o777 == 0o600
        offline_identity.append(facts["Database system identifier"])
        return start(*args, **kwargs)

    node.start = check_before_start
    try:
        assert node.bootstrap.bootstrap({"initdb": ["no-locale", {"encoding": "UTF8"}]})
        assert len(offline_identity) == 1
        assert FOLLOW["query"](port, "SELECT pg_is_in_recovery(), current_user") == [(False, "postgres")]
        FOLLOW["query"](port, "CREATE TABLE fresh_primary (id integer); INSERT INTO fresh_primary VALUES (42)")
        assert FOLLOW["query"](port, "SELECT id FROM fresh_primary") == [(42,)]
        assert node.controldata()["Database system identifier"] == offline_identity[0]
        assert (data / "PG_VERSION").is_file() and not list(root.glob(".pg-lisp-initdb-*"))
        retry = subprocess.run([BIN / "initdb", data], capture_output=True, timeout=20)
        assert retry.returncode == 1
        assert FOLLOW["query"](port, "SELECT id FROM fresh_primary") == [(42,)]
        if not precreated:
            check_sync_config(node, port)
    finally:
        node.connection_pool.close()
        process = node.is_running()
        if process:
            process.send_signal(signal.SIGINT)
            process.wait(timeout=45)
    assert node.controldata()["Database cluster state"] == "shut down"
    print("Patroni 4.1.5 native initdb, primary start, write and clean shutdown PASS", flush=True)


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    with tempfile.TemporaryDirectory(prefix="pg-patroni-bootstrap-") as path:
        root = Path(path)
        check_rejected_targets(root)
        check_bootstrap(root / "absent", precreated=False)
        check_bootstrap(root / "empty", precreated=True)


if __name__ == "__main__":
    main()
