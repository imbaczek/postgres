"""Exercise pinned Patroni follow/start after journaled native reseed."""
import argparse
import ctypes
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import signal
import socket
import time

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.postgresql.mpp import Null
import psutil


HERE = Path(__file__).resolve().parent


def load_driver(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cancellation = load_driver("patroni_reseed_cancellation", "reseed-cancellation.py")
follow = load_driver("patroni_reseed_follow", "patroni-follow.py")


class Acceptance(cancellation.Acceptance):
    def prepare_active(self, target, workspace, port):
        registry = "(asdf:initialize-source-registry '(:source-registry (:tree " + cancellation.lisp_string(str(cancellation.LISP) + "/") + ") :inherit-configuration))"
        cache = "(asdf:initialize-output-translations '(:output-translations (t " + cancellation.lisp_string(str(self.output / "cache") + "/") + ") :ignore-inherited-configuration))"
        arguments = [self.sbcl, "--noinform", "--no-sysinit", "--no-userinit", "--non-interactive",
                     "--eval", "(require :asdf)", "--eval", registry, "--eval", cache,
                     "--eval", '(asdf:load-system "pg")', "--load", str(HERE / "patroni-reseed-native.lisp")]
        prefix = arguments.copy()
        form = "(pg::native-patroni-reseed-activate #P" + cancellation.lisp_string(str(target) + "/")
        form += " #P" + cancellation.lisp_string(str(workspace) + "/")
        form += " " + str(port) + " " + cancellation.lisp_string(self.helper) + ")"
        arguments += ["--eval", form, "--eval", "(pg::native-patroni-reseed-retire #P" +
                      cancellation.lisp_string(str(target) + "/") + " " + str(port) + ")",
                      "--eval", '(write-line "PATRONI-RESEED-PREPARED")']
        _, output = self.process("activate-retire", arguments, timeout=900)
        assert b"PATRONI-RESEED-PREPARED" in output
        assert not (target / "postmaster.pid").exists()
        _, output = self.process("finalize", prefix + ["--eval", "(pg::native-patroni-reseed-finalize #P" +
                                 cancellation.lisp_string(str(target) + "/") + " " + str(port) + ")",
                                 "--eval", '(write-line "PATRONI-RESEED-FINALIZED")'], timeout=600)
        assert b"PATRONI-RESEED-FINALIZED" in output
        assert not (target / "postmaster.pid").exists()
        assert (target / "postgresql.conf").read_bytes() == b""
        assert all(not (target / name).exists() for name in (
            "physical-reseed.attempt", "physical-reseed.attempt.tmp", "physical-reseed.install",
            "physical-reseed.install.tmp", "physical-reseed.follow", "physical-reseed.follow.tmp",
            "physical-reseed.retired", "physical-reseed.retired.tmp"))

    def snapshot(self, label, roots, target):
        inventory = self.configured_inventory(roots, target)
        for name in ("physical-reseed.follow", "physical-reseed.follow.tmp", "physical-reseed.retired"):
            path = target / name
            inventory[str(path)] = cancellation.facts(path) if path.exists() else None
        rows = {key: repr(value) for key, value in inventory.items()}
        (self.output / (label + "-inventory.json")).write_text(json.dumps(rows, indent=2) + "\n")

    def stop_target(self, node):
        process = node.is_running()
        record = None
        if process:
            record = {"kind": "patroni-target", "pid": process.pid, "exit": None, "expected_exit": 0}
            self.result["processes"].append(record)
        try:
            node.connection_pool.close()
        finally:
            if process:
                try:
                    process.send_signal(signal.SIGINT)
                except psutil.NoSuchProcess:
                    pass
                try:
                    record["exit"] = process.wait(timeout=45)
                except psutil.TimeoutExpired:
                    try:
                        process.kill()
                    except psutil.NoSuchProcess:
                        pass
                    record["exit"] = process.wait(timeout=10)

    def check_standby(self, port, expected):
        assert follow.query(port, "SELECT pg_is_in_recovery()") == [(True,)]
        assert follow.query(port, "SELECT id FROM patroni_reseed_probe ORDER BY id") == expected
        try:
            follow.query(port, "INSERT INTO patroni_reseed_probe VALUES(99)")
        except follow.psycopg2.Error as error:
            assert error.pgcode == "25006", error
        else:
            raise AssertionError("standby accepted a write")

    def configured(self):
        started = time.monotonic()
        label = "patroni-reseed"
        follow.query(self.server.port, "CREATE TABLE patroni_reseed_probe(id integer)")
        follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(1)")
        # Provide the slot normally managed by the Patroni HA loop.
        follow.query(self.server.port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
        target, _, _ = self.install(7201, label, selected=True)
        self.run("prove", target)
        workspace = self.server.root / (label + "-workspace")
        workspace.mkdir(mode=0o700)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        self.prepare_active(target, workspace, port)
        roots = [self.server.root / (label + "-" + suffix)
                 for suffix in ("target", "stage", "incoming", "quarantine", "workspace")]
        protected = self.configured_inventory(roots[1:], target)
        self.snapshot("before-patroni", roots, target)
        node = None
        try:
            config = follow.configuration(target, port, self.server.root)
            node = Postgresql(config, Null())
            self.snapshot("after-constructor", roots, target)
            member = Member(0, "source", None,
                            {"conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres"})
            assert node.follow(member, timeout=90) is True
            self.snapshot("after-follow", roots, target)
            assert follow.query(port, "SELECT pg_is_in_recovery()") == [(True,)]
            follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(2)")
            follow.wait_for(lambda: follow.query(port, "SELECT id FROM patroni_reseed_probe ORDER BY id")
                            == [(1,), (2,)])
            self.check_standby(port, [(1,), (2,)])
            initial_pid = node.is_running().pid
            self.stop_target(node)
            assert node.start(timeout=90) is True
            assert node.is_running().pid != initial_pid
            self.check_standby(port, [(1,), (2,)])
            self.stop()
            self.stop_target(node)
            assert node.start(timeout=90) is True
            self.check_standby(port, [(1,), (2,)])
            self.start()
            follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(3)")
            follow.wait_for(lambda: follow.query(port, "SELECT id FROM patroni_reseed_probe ORDER BY id")
                            == [(1,), (2,), (3,)])
            self.check_standby(port, [(1,), (2,), (3,)])
            self.result["checks"].append(label)
            self.result.setdefault("timings", []).append(
                {"case": label, "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})
        finally:
            try:
                if node is not None:
                    self.stop_target(node)
            finally:
                self.snapshot("final", roots, target)
                assert self.configured_inventory(roots[1:], target) == protected


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    Acceptance(arguments.output).execute(["configured"])


if __name__ == "__main__":
    main()
