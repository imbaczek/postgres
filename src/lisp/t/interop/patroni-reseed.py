"""Exercise pinned Patroni follow/start after journaled native reseed."""
import argparse
import ctypes
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import signal
import socket
import stat
import time

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.postgresql.mpp import Null
import psutil
from patroni.async_executor import CriticalTask
from psycopg2.extras import PhysicalReplicationConnection


HERE = Path(__file__).resolve().parent


def load_driver(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cancellation = load_driver("patroni_reseed_cancellation", "reseed-cancellation.py")
follow = load_driver("patroni_reseed_follow", "patroni-follow.py")


class Acceptance(cancellation.Acceptance):
    label = "patroni-reseed"

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
        label = self.label
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


def identity(port, history=False):
    connection = follow.psycopg2.connect(
        host="127.0.0.1", port=port, user="postgres", dbname="postgres",
        connection_factory=PhysicalReplicationConnection, connect_timeout=2)
    try:
        cursor = connection.cursor()
        cursor.execute("IDENTIFY_SYSTEM")
        system, timeline, position, _ = cursor.fetchone()
        result = {"system": str(system), "timeline": int(timeline), "position": position}
        if history:
            cursor.execute("TIMELINE_HISTORY " + str(timeline))
            filename, content = cursor.fetchone()
            result.update(history_file=filename, history=content)
        return result
    finally:
        connection.close()


def preserved_entry(path):
    metadata = path.lstat()
    assert stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode), str(path)
    row = [metadata.st_dev, metadata.st_ino, metadata.st_mode,
           metadata.st_uid, metadata.st_gid, metadata.st_nlink]
    if stat.S_ISREG(metadata.st_mode):
        row += [metadata.st_size, hashlib.sha256(path.read_bytes()).hexdigest()]
    return row


def preserved_tree(root):
    return {str(path.relative_to(root)): preserved_entry(path)
            for path in sorted(root.rglob("*")) if path != root / ".pg_lisp.lock"}


class FormerPrimaryAcceptance(Acceptance):
    label = "patroni-reseed-former-primary"

    def initialize_target(self, target):
        started = time.monotonic()
        root = self.server.root / "replacement-source"
        data = root / "data"
        data.mkdir(mode=0o700, parents=True)
        follow.query(self.server.port, "SELECT pg_create_physical_replication_slot('replacement_source',true)")
        self.control("(pg::physical-base-backup " +
                     cancellation.lisp_string(str(data) + "/") + " \"former_primary_seed\")")
        (data / "postgresql.conf").write_text("# Native replacement source.\n")
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = follow.configuration(data, port, root)
        config["name"] = "replacement_source"
        node = Postgresql(config, Null())
        try:
            member = Member(0, "original", None,
                            {"conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres"})
            assert node.follow(member, timeout=90) is True
            follow.wait_for(lambda: follow.query(port, "SELECT id FROM patroni_reseed_probe") == [(1,)])
            original = identity(self.server.port)
            self.stop()
            assert node.promote(30, CriticalTask()) is True
            assert follow.query(port, "SELECT pg_is_in_recovery()") == [(False,)]
            follow.query(port, "CREATE TABLE patroni_reseed_child_only(id integer)")
            follow.query(port, "INSERT INTO patroni_reseed_child_only VALUES(42)")
            promoted = identity(port, history=True)
            assert original["system"] == promoted["system"]
            assert original["timeline"] == 1 and promoted["timeline"] == 2
            history = [line.split() for line in promoted["history"].splitlines()
                       if line and not line.startswith("#")]
            assert len(history) == 1 and history[0][0] == "1", history
            high, low = history[0][1].split("/")
            fork = int(high, 16) * 2**32 + int(low, 16)
            self.start()
            follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(999)")
            assert follow.query(self.server.port, "SELECT id FROM patroni_reseed_probe ORDER BY id") == [(1,), (999,)]
            divergent = identity(self.server.port)
            position = divergent["position"].split("/")
            assert int(position[0], 16) * 2**32 + int(position[1], 16) > fork
            assert divergent["system"] == promoted["system"] and divergent["timeline"] == 1
            assert follow.query(port, "SELECT id FROM patroni_reseed_probe ORDER BY id") == [(1,)]
            self.result["former_primary"] = {"original": original, "source": promoted,
                                              "divergent": divergent, "fork_lsn": fork}
            self.stop()
        finally:
            self.stop_target(node)
        self.server.data.rename(target)
        self.server.data = data
        self.server.port = port
        self.start()
        assert identity(port)["timeline"] == 2
        if not follow.query(port, "SELECT slot_name FROM pg_replication_slots WHERE slot_name='follow_one'"):
            follow.query(port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
        (target / "opaque-old-data").write_bytes(b"original data\x00\xff")
        self.old_tree = preserved_tree(target)
        (self.output / "former-primary-inventory.json").write_text(json.dumps(self.old_tree, indent=2) + "\n")
        assert not (target / "postmaster.pid").exists()
        self.result.setdefault("timings", []).append(
            {"case": "former-primary-construction", "elapsed_seconds": time.monotonic() - started,
             "outcome": "pass"})

    def configured(self):
        super().configured()
        quarantine = self.server.root / (self.label + "-quarantine")
        expected = self.old_tree.copy()
        assert "physical-reseed.install" not in expected
        expected["physical-reseed.install"] = self.quarantine_guard
        assert preserved_tree(quarantine) == expected
        self.result["former_primary"]["preserved_entries"] = len(self.old_tree)

    def install(self, index, label, selected):
        result = super().install(index, label, selected)
        guard = self.server.root / (label + "-quarantine") / "physical-reseed.install"
        assert guard.read_bytes() == b"offline reseed auxiliary directory"
        self.quarantine_guard = preserved_entry(guard)
        (self.output / "former-primary-quarantine-guard.json").write_text(
            json.dumps(self.quarantine_guard) + "\n")
        return result

    def check_standby(self, port, expected):
        super().check_standby(port, expected)
        assert follow.query(port, "SELECT id FROM patroni_reseed_child_only") == [(42,)]


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=("synthetic", "former-primary"), default="synthetic")
    arguments = parser.parse_args()
    acceptance = FormerPrimaryAcceptance if arguments.case == "former-primary" else Acceptance
    acceptance(arguments.output).execute(["configured"])


if __name__ == "__main__":
    main()
