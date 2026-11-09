"""Exercise pinned Patroni bootstrap cancellation and cold clone retry."""
import argparse
import copy
import ctypes
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import socket
import threading
import time

import psutil
from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.postgresql.mpp import Null


HERE = Path(__file__).resolve().parent

spec = importlib.util.spec_from_file_location("bootstrap_reseed_driver", HERE / "patroni-reseed.py")
reseed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reseed)
follow = reseed.follow


class Acceptance(reseed.Acceptance):
    def unpublished(self, target):
        assert not target.exists() or (target.is_dir() and not list(target.iterdir()))
        assert not list(target.parent.glob(".pg-native-backup.*"))

    def cancel_clone(self, node, member, target):
        ready = self.server.root / "patroni-export.ready"
        release = self.server.root / "patroni-export.release"
        self.control("(progn (load #P" + reseed.cancellation.lisp_string(HERE / "patroni-bootstrap-native.lisp") +
                     ") (values))")
        self.control("(pg::native-patroni-bootstrap-pause " + reseed.cancellation.lisp_string(ready) +
                     " " + reseed.cancellation.lisp_string(release) + ")")
        results, errors = [], []

        def clone():
            try:
                results.append(node.bootstrap.clone(member))
            except BaseException as error:
                errors.append(repr(error))

        thread = threading.Thread(target=clone, name="patroni-bootstrap-clone", daemon=True)
        process, children = None, []
        started = time.monotonic()
        thread.start()
        try:
            follow.wait_for(ready.exists)
            with node.cancellable._lock:
                process = node.cancellable._process
                assert process is not None and process.is_running()
                children = process.children(recursive=True)
            assert any(Path(child.exe()) == Path(self.helper).resolve() for child in children)
            self.result["observed_children"] = [
                {"pid": child.pid, "created": child.create_time(), "command": child.cmdline()}
                for child in children]
            follow.query(self.server.port, "INSERT INTO patroni_bootstrap_probe VALUES(99)")
            assert follow.query(self.server.port, "SELECT id FROM patroni_bootstrap_probe ORDER BY id") == [(1,), (99,)]
            follow.query(self.server.port, "DELETE FROM patroni_bootstrap_probe WHERE id=99")
            node.cancellable.cancel()
            thread.join(timeout=25)
            assert not thread.is_alive() and not errors and results == [False], (results, errors)
            assert node.cancellable.is_cancelled
            assert process.returncode == 130, process.returncode
            self.unpublished(target)
        finally:
            release.touch()
            node.cancellable.cancel()
            thread.join(timeout=25)
            self.control("(setf pg::*physical-bootstrap-test-hook* nil)")
            if process is not None:
                self.result["processes"].append(
                    {"kind": "cancelled-basebackup", "pid": process.pid,
                     "exit": process.returncode, "expected_exit": 130})
            assert not thread.is_alive(), "clone thread did not finish after cancellation"
        follow.wait_for(lambda: all(not child.is_running() for child in children))
        for child in self.result["observed_children"]:
            child["absent"] = not Path("/proc", str(child["pid"])).exists()
            assert child["absent"]
        metadata = self.server.data.stat()
        stages, = self.server.root.glob(f".pg-native-backups-{metadata.st_dev:X}-{metadata.st_ino:X}-*")
        follow.wait_for(lambda: sorted(path.name for path in stages.iterdir()) == [".owner"])
        assert follow.query(self.server.port, "SELECT slot_name FROM pg_replication_slots") == [("bootstrap_one",)]
        assert follow.query(self.server.port, "SELECT 1") == [(1,)]
        self.result["checks"].append("patroni-bootstrap-cancel")
        self.result.setdefault("timings", []).append(
            {"case": "cancel", "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})

    def configured(self):
        follow.query(self.server.port, "CREATE TABLE patroni_bootstrap_probe(id integer)")
        follow.query(self.server.port, "INSERT INTO patroni_bootstrap_probe VALUES(1)")
        follow.query(self.server.port, "SELECT pg_create_physical_replication_slot('bootstrap_one',true)")
        root = self.server.root / "replica"
        target = root / "data"
        root.mkdir(mode=0o700)
        target.mkdir(mode=0o700)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = follow.configuration(self.server.data, port, root)
        config.update(name="bootstrap_one", data_dir=str(target))
        member = Member(0, "source", None,
                        {"conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres"})
        node = Postgresql(copy.deepcopy(config), Null())
        try:
            self.unpublished(target)
            self.cancel_clone(node, member, target)
            node.connection_pool.close()
            self.stop()
            started = time.monotonic()
            node = Postgresql(copy.deepcopy(config), Null())
            assert not node.cancellable.is_cancelled
            assert node.bootstrap.clone(member) is False
            self.unpublished(target)
            assert not psutil.Process().children(recursive=True)
            self.result["checks"].append("patroni-bootstrap-source-offline")
            self.result.setdefault("timings", []).append(
                {"case": "offline", "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})
            node.connection_pool.close()
            self.start()
            started = time.monotonic()
            node = Postgresql(copy.deepcopy(config), Null())
            assert not node.cancellable.is_cancelled
            assert node.bootstrap.clone(member) is True
            assert (target / "physical-backup.manifest").is_file()
            assert (target / "postgresql.conf").is_file()
            assert node.follow(member, timeout=90) is True
            follow.query(self.server.port, "INSERT INTO patroni_bootstrap_probe VALUES(2)")
            follow.wait_for(lambda: follow.query(port, "SELECT id FROM patroni_bootstrap_probe ORDER BY id")
                            == [(1,), (2,)])
            assert follow.query(port, "SELECT status,slot_name FROM pg_stat_get_wal_receiver()") == [
                ("streaming", "bootstrap_one")]
            assert follow.query(port, "SELECT pg_is_in_recovery()") == [(True,)]
            try:
                follow.query(port, "INSERT INTO patroni_bootstrap_probe VALUES(999)")
            except follow.psycopg2.Error as error:
                assert error.pgcode == "25006", error
            else:
                raise AssertionError("standby accepted a write")
            old_pid = node.is_running().pid
            self.stop_target(node)
            assert node.start(timeout=90) is True
            assert node.is_running().pid != old_pid
            follow.query(self.server.port, "INSERT INTO patroni_bootstrap_probe VALUES(3)")
            follow.wait_for(lambda: follow.query(port, "SELECT id FROM patroni_bootstrap_probe ORDER BY id")
                            == [(1,), (2,), (3,)])
            assert follow.query(port, "SELECT pg_is_in_recovery()") == [(True,)]
            assert follow.query(port, "SELECT status,slot_name FROM pg_stat_get_wal_receiver()") == [
                ("streaming", "bootstrap_one")]
            self.result["checks"].append("patroni-bootstrap-cold-retry-follow")
            self.result.setdefault("timings", []).append(
                {"case": "cold-retry-follow", "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})
        finally:
            self.stop_target(node)
        children = psutil.Process().children(recursive=True)
        assert [child.pid for child in children] == [self.server.process.pid]


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    acceptance = Acceptance(parser.parse_args().output)
    try:
        acceptance.execute(["configured"])
    finally:
        children = psutil.Process().children(recursive=True)
        if children:
            acceptance.result["complete"] = False
            acceptance.result["unexpected_children"] = [child.pid for child in children]
            for child in reversed(children):
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            psutil.wait_procs(children, timeout=10)
            (acceptance.output / "result.json").write_text(json.dumps(acceptance.result, indent=2) + "\n")
            raise AssertionError("unexpected owned descendants remained at exit")


if __name__ == "__main__":
    main()
