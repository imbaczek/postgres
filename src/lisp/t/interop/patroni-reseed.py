"""Exercise pinned Patroni follow/start after journaled native reseed."""
import argparse
from contextlib import closing
import ctypes
import hashlib
import importlib.metadata
import importlib.util
import json
import logging
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
from threading import Event
import time
from unittest.mock import patch
from patroni_native_node import native_node

from patroni import thread_pool
from patroni.dcs import Cluster, Leader, Member
from patroni.ha import Ha
from patroni.postgresql import Postgresql
from patroni.postgresql.misc import PostgresqlRole
from patroni.postgresql.mpp import Null
from patroni.postgresql.rewind import REWIND_STATUS, Rewind
import psutil
from patroni.async_executor import AsyncExecutor, CriticalTask
from psycopg2.extras import PhysicalReplicationConnection


HERE = Path(__file__).resolve().parent


def load_driver(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cancellation = load_driver("patroni_reseed_cancellation", "reseed-cancellation.py")
follow = load_driver("patroni_reseed_follow", "patroni-follow.py")
CommitLedger = load_driver("manual_failover_ledger", "failover-ledger.py").CommitLedger


class Acceptance(cancellation.Acceptance):
    label = "patroni-reseed"

    def native_arguments(self):
        registry = "(asdf:initialize-source-registry '(:source-registry (:tree " + cancellation.lisp_string(str(cancellation.LISP) + "/") + ") :inherit-configuration))"
        cache = "(asdf:initialize-output-translations '(:output-translations (t " + cancellation.lisp_string(str(self.output / "cache") + "/") + ") :ignore-inherited-configuration))"
        return [self.sbcl, "--noinform", "--no-sysinit", "--no-userinit", "--non-interactive",
                "--eval", "(require :asdf)", "--eval", registry, "--eval", cache,
                "--eval", '(asdf:load-system "pg")', "--load", str(HERE / "patroni-reseed-native.lisp")]

    def prepare_active(self, target, workspace, port):
        arguments = self.native_arguments()
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
        return record

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


def lsn_number(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


class FormerPrimaryAcceptance(Acceptance):
    label = "patroni-reseed-former-primary"

    def fence_original(self, node, port):
        self.stop()

    def prove_divergence(self):
        self.start()
        follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(999)")
        assert follow.query(self.server.port, "SELECT id FROM patroni_reseed_probe ORDER BY id") == [(1,), (999,)]
        divergent = identity(self.server.port)
        self.stop()
        return divergent

    def verify_promotion(self, port):
        pass

    def check_divergence_policy(self, target, original_port, source_port, fork):
        config = follow.configuration(target, original_port, target.parent)
        config.update(name="former_primary", use_pg_rewind=True)
        leader = Leader(0, None, Member(0, "replacement_source", None, {
            "conn_url": f"postgres://127.0.0.1:{source_port}/postgres",
            "role": PostgresqlRole.PRIMARY}))
        before = preserved_tree(target)
        node = Postgresql(config, Null())
        control = node.controldata()
        assert control["Database cluster state"] == "shut down", control
        assert control["Latest checkpoint's TimeLineID"] == "1", control
        checkpoint = lsn_number(control["Latest checkpoint location"])
        assert checkpoint >= fork, control
        rewind = Rewind(node)
        assert rewind.enabled and not rewind.can_rewind
        assert not rewind.can_rewind_or_reinitialize_allowed
        rewind.trigger_check_diverged_lsn()
        assert not rewind.rewind_or_reinitialize_needed_and_possible(leader)

        config["remove_data_directory_on_diverged_timelines"] = True
        rewind = Rewind(Postgresql(config, Null()))
        assert rewind.can_rewind_or_reinitialize_allowed
        rewind.trigger_check_diverged_lsn()
        assert rewind.rewind_or_reinitialize_needed_and_possible(leader)
        assert preserved_tree(target) == before
        self.result["former_primary"]["patroni_divergence_policy"] = {
            "native_rewind_available": False,
            "divergence_detected_with_removal_opt_in": True,
            "offline_checkpoint": checkpoint}

    def seed_child(self, data):
        data.mkdir(mode=0o700, parents=True)
        follow.query(self.server.port, "SELECT pg_create_physical_replication_slot('replacement_source',true)")
        self.control("(pg::physical-base-backup " +
                     cancellation.lisp_string(str(data) + "/") + " \"former_primary_seed\")")

    def initialize_target(self, target):
        started = time.monotonic()
        # Leave room for target.sock under the AF_UNIX path length limit.
        root = self.server.root / "child"
        data = root / "data"
        self.seed_child(data)
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
            self.fence_original(node, port)
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
            fork = lsn_number(history[0][1])
            divergent = self.prove_divergence()
            assert lsn_number(divergent["position"]) > fork
            assert divergent["system"] == promoted["system"] and divergent["timeline"] == 1
            assert follow.query(port, "SELECT id FROM patroni_reseed_probe ORDER BY id") == [(1,)]
            self.verify_promotion(port)
            self.result["former_primary"] = {"original": original, "source": promoted,
                                              "divergent": divergent, "fork_lsn": fork}
        finally:
            self.stop_target(node)
        original_port = self.server.port
        self.server.data.rename(target)
        self.server.data = data
        self.server.port = port
        self.start()
        assert identity(port)["timeline"] == 2
        if type(self) is FormerPrimaryAcceptance:
            self.check_divergence_policy(target, original_port, port, fork)
        if not follow.query(port, "SELECT slot_name FROM pg_replication_slots WHERE slot_name='follow_one'"):
            follow.query(port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
        (target / "opaque-old-data").write_bytes(b"original data\x00\xff")
        self.old_tree = preserved_tree(target)
        (self.output / "former-primary-inventory.json").write_text(json.dumps(self.old_tree, indent=2) + "\n")
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


class RewindSharedHistoryAcceptance(FormerPrimaryAcceptance):
    label = "rewind-shared-history"

    def capture(self, label):
        path = self.server.root / label
        conninfo = f"host=127.0.0.1 port={self.server.port} user=postgres dbname=postgres"
        self.process(label, [str(cancellation.LISP / "bin/pg_basebackup"),
                             "-D", str(path), "-X", "stream", "-d", conninfo],
                     env=dict(os.environ, PG_LIBPQ_HELPER=self.helper))
        return path

    def seed_child(self, data):
        data.parent.mkdir(mode=0o700)
        shutil.copytree(self.origin, data)

    def fence_original(self, node, port):
        follow.query(self.server.port, "INSERT INTO rewind_shared_marker VALUES(71)")
        follow.wait_for(lambda: follow.query(port, "SELECT id FROM rewind_shared_marker") == [(71,)])
        super().fence_original(node, port)

    def check_retained_origin(self, target, fork):
        reference = load_driver("rewind_export_origin", "export-origin.py")
        manifest = (self.origin / "physical-backup.manifest").read_bytes()
        system = int(re.search(rb":SYSTEM-ID\s+(\d+)", manifest)[1])
        base = int(re.search(rb":END\s+(\d+)", manifest)[1])
        manifest_digest = reference.fnv(manifest)
        digest = reference.hash_history(target, base, fork)
        output = self.control(
            "(let* ((fields (multiple-value-list (wal-read-control-fields))) "
            "(history (timeline-read-history (fourth fields))) "
            f"(request (list :system-id {system} :base-timeline 1 :base {base} "
            f":manifest-digest {manifest_digest} :end {fork}))) "
            f"(assert (= {digest} (physical-origin-proof-hash-root fields history request))) "
            '(write-line "REWIND-SHARED-ORIGIN PASS"))')
        assert "REWIND-SHARED-ORIGIN PASS" in output
        self.result["former_primary"]["retained_origin"] = dict(
            base=base, fork=fork, manifest_digest=manifest_digest, fork_digest=digest)

    def create_rewind_target(self):
        follow.query(self.server.port, "CREATE TABLE patroni_reseed_probe(id integer)")
        follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(1)")
        follow.query(self.server.port, "CREATE TABLE rewind_shared_marker(id integer)")
        follow.query(self.server.port, "CREATE TABLE rewind_reuse_payload(id integer, payload text)")
        for i in range(48):
            payload = hashlib.shake_256(str(i).encode()).hexdigest(3000)
            follow.query(self.server.port,
                         f"INSERT INTO rewind_reuse_payload VALUES({i},'{payload}')")
        follow.query(self.server.port, "SELECT pg_create_physical_replication_slot('replacement_source',true)")
        self.origin = self.capture("rewind-origin")
        target = self.server.root / "rewind-target"
        self.initialize_target(target)
        follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(2001)")
        return target

    def configured(self):
        connection = follow.psycopg2.connect(
            host="127.0.0.1", port=self.server.port, user="postgres", dbname="postgres",
            connection_factory=PhysicalReplicationConnection, connect_timeout=2)
        try:
            with connection.cursor() as cursor:
                try:
                    cursor.execute("IDENTIFY_REWIND_ORIGIN")
                except follow.psycopg2.Error as error:
                    assert error.pgcode == "55000", error
                else:
                    raise AssertionError("initial primary advertised a retained rewind origin")
                cursor.execute("IDENTIFY_SYSTEM")
                assert int(cursor.fetchone()[1]) == 1
        finally:
            connection.close()
        target = self.create_rewind_target()
        source = self.capture("rewind-source")
        config = source / "postgresql.conf"
        assert config.read_bytes() == b""
        config.unlink()
        fork = self.result["former_primary"]["fork_lsn"]
        self.check_retained_origin(target, fork)
        before = {path: preserved_tree(path) for path in (target, source, self.origin)}
        arguments = " ".join("#P" + cancellation.lisp_string(str(path) + "/")
                             for path in (target, self.origin, source))
        _, output = self.process(self.label, self.native_arguments() + [
            "--eval", f"(pg::native-patroni-rewind-shared-history {arguments} {fork})"])
        assert b"REWIND-SHARED-HISTORY PASS" in output
        leased = self.server.root / "rewind-leased"
        leased.mkdir(mode=0o700)
        arguments = " ".join("#P" + cancellation.lisp_string(str(path) + "/")
                             for path in (target, self.origin, leased))
        digest = self.result["former_primary"]["retained_origin"]["fork_digest"]
        helper = cancellation.lisp_string(self.helper)
        _, output = self.process("rewind-leased-origin", self.native_arguments() + [
            "--eval", f"(pg::native-patroni-rewind-leased-origin {arguments} "
                      f"{self.server.port} {helper} {fork} {digest})"])
        assert b"REWIND-LEASED-ORIGIN PASS" in output
        assert all(preserved_tree(path) == inventory for path, inventory in before.items())
        self.result["checks"].append(self.label)


class RewindInstallAcceptance(RewindSharedHistoryAcceptance):
    label = "rewind-install"

    def configured(self):
        target = self.create_rewind_target()
        stage, incoming, quarantine, workspace = [
            self.server.root / name for name in ("rw-stage", "rw-incoming", "rw-old", "rw-follow")]
        for directory in (stage, incoming, quarantine, workspace):
            directory.mkdir(mode=0o700)
        capture_paths = " ".join("#P" + cancellation.lisp_string(str(path) + "/")
                                 for path in (target, self.origin, stage))
        install_paths = " ".join("#P" + cancellation.lisp_string(str(path) + "/")
                                 for path in (target, incoming, quarantine))
        helper = cancellation.lisp_string(self.helper)
        _, output = self.process("rewind-capture", self.native_arguments() + [
            "--eval", f"(pg::native-patroni-rewind-capture {capture_paths} {self.server.port} {helper})"])
        assert b"REWIND-JOURNALED-CAPTURE PASS" in output
        _, output = self.process("rewind-install", self.native_arguments() + [
            "--load", str(HERE / "reseed-cancellation-native.lisp"),
            "--eval", f"(pg::native-patroni-rewind-install {install_paths} {helper})"],
            timeout=600)
        assert b"REWIND-JOURNALED-INSTALL PASS" in output
        self.check_quarantine(quarantine)
        self.run("prove", target)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        self.prepare_active(target, workspace, port)
        self.check_rejoined(target, port)

    def check_quarantine(self, quarantine):
        old = preserved_tree(quarantine)
        assert (quarantine / "physical-reseed.install").read_bytes() == b"offline reseed auxiliary directory"
        del old["physical-reseed.install"]
        assert old == self.old_tree

    def check_rejoined(self, target, port):
        node = Postgresql(follow.configuration(target, port, self.server.root), Null())
        member = Member(0, "source", None,
                        {"conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres"})
        try:
            assert node.follow(member, timeout=90) is True
            self.check_standby(port, [(1,), (2001,)])
            follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(2002)")
            follow.wait_for(lambda: follow.query(port, "SELECT id FROM patroni_reseed_probe ORDER BY id")
                            == [(1,), (2001,), (2002,)])
            before = node.is_running().pid
            assert node.restart(timeout=90) is True
            assert node.is_running().pid != before
            self.check_standby(port, [(1,), (2001,), (2002,)])
        finally:
            self.stop_target(node)
        self.result["checks"].append(self.label)


class RewindCliAcceptance(RewindInstallAcceptance):
    label = "rewind-cli"

    def configured(self):
        target = self.create_rewind_target()
        conninfo = f"host=127.0.0.1 port={self.server.port} user=postgres dbname=postgres"
        _, output = self.process(self.label, [
            str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target), "--source-server", conninfo],
            timeout=900, env=dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper))
        assert b"target is ready to follow the source" in output
        workspace = Path(re.search(rb"^pg_rewind workspace: (.+)$", output, re.M)[1].decode())
        assert workspace.parent == target.parent and workspace.name.startswith(".pg-rewind.")
        assert stat.S_IMODE(workspace.stat().st_mode) == 0o700
        self.check_quarantine(workspace / "quarantine")
        assert not (target / "postmaster.pid").exists()
        config = target / "postgresql.conf"
        assert config.read_bytes() == b""
        config.unlink()
        _, finished = self.process("rewind-finish", [
            str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target), "--finish"],
            timeout=90, env=dict(os.environ, SBCL=self.sbcl))
        assert b"target is ready to follow the source" in finished
        assert config.read_bytes() == b""
        assert not (target / "postmaster.pid").exists()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        self.check_rejoined(target, port)


class RewindPatroniAcceptance(RewindInstallAcceptance):
    label = "rewind-patroni"
    checksum_version_on_invalid_page = None

    def recover_target(self, target, node, rewind, leader):
        pass

    def check_checksum_rejection(self, target, node):
        path = next(path for path in sorted((target / "base").iterdir())
                    if path.name.isdecimal() and path.stat().st_size >= 8192)
        with path.open("r+b", buffering=0) as stream:
            header = stream.read(10)
            assert len(header) == 10 and any(header[:8])
            invalid_headers = (bytes(10), header[:8] + bytes([header[8] ^ 1, header[9]]))
            try:
                for invalid in invalid_headers:
                    stream.seek(0)
                    stream.write(invalid)
                    facts = node.controldata()
                    assert facts["Database cluster state"] == "shut down"
                    assert facts.get("Data page checksum version") == self.checksum_version_on_invalid_page
            finally:
                stream.seek(0)
                stream.write(header)

    def configured(self):
        target = self.create_rewind_target()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = follow.configuration(target, port, self.server.root)
        config.update(use_pg_rewind=True, remove_data_directory_on_diverged_timelines=False,
                      remove_data_directory_on_rewind_failure=False)
        node = Postgresql(config, Null())
        rewind = Rewind(node)
        follow.query(self.server.port, "CHECKPOINT")
        leader = Leader(0, None, Member(0, "source", None, {
            "conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres",
            "role": PostgresqlRole.PRIMARY, "checkpoint_after_promote": True}))
        logger = logging.getLogger("patroni.postgresql.rewind")
        level = logger.level
        with closing(logging.FileHandler(self.output / "patroni-rewind.log", mode="x")) as handler:
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            try:
                with patch.dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper):
                    self.recover_target(target, node, rewind, leader)
                    assert rewind.can_rewind, node.controldata()
                    self.check_checksum_rejection(target, node)
                    rewind.trigger_check_diverged_lsn()
                    assert rewind.rewind_or_reinitialize_needed_and_possible(leader)
                    rewind.execute(leader)
            finally:
                logger.setLevel(level)
                logger.removeHandler(handler)
                node.connection_pool.close()
        assert rewind.executed and not rewind.failed
        assert "pg_rewind exit code=0" in (self.output / "patroni-rewind.log").read_text()
        workspace, = target.parent.glob(".pg-rewind.*")
        self.check_quarantine(workspace / "quarantine")
        assert not (target / "postmaster.pid").exists()
        self.check_rejoined(target, port)


class RewindDirtyPatroniAcceptance(RewindPatroniAcceptance):
    label = "rewind-patroni-dirty"
    segment_size = 16 * 1024 * 1024
    checksum_version_on_invalid_page = "1"

    def prove_divergence(self):
        self.process("enable-checksums", [str(cancellation.LISP / "bin/pg_checksums"),
                                          "--enable", "-D", str(self.server.data)])
        self.start()
        node = native_node(self.server.data)
        node.config["use_pg_rewind"] = True
        rewind = Rewind(node)
        assert rewind.can_rewind, node.controldata()
        rewind.trigger_check_diverged_lsn()
        assert rewind._state is REWIND_STATUS.CHECK
        follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(999)")
        follow.query(self.server.port, "SELECT pg_drop_replication_slot('replacement_source')")
        follow.query(self.server.port, "CREATE TABLE rewind_dirty_payload(payload text)")
        start = lsn_number(identity(self.server.port)["position"])
        connection = follow.psycopg2.connect(host="127.0.0.1", port=self.server.port,
                                             user="postgres", dbname="postgres")
        with closing(connection), connection.cursor() as cursor:
            connection.autocommit = True
            for index in range(1000):
                payload = hashlib.shake_256(str(index).encode()).hexdigest(65536)
                cursor.execute("INSERT INTO rewind_dirty_payload VALUES (%s)", (payload,))
                cursor.execute("SELECT pg_current_wal_flush_lsn()")
                if lsn_number(cursor.fetchone()[0]) >= start + self.segment_size:
                    break
            else:
                raise AssertionError("dirty target did not cross a WAL segment")
        divergent = identity(self.server.port)
        process = self.server.process
        self.server.stop(immediate=True)
        assert process.returncode == -signal.SIGKILL
        self.result["processes"].append(
            {"kind": "source-crash", "pid": process.pid, "exit": process.returncode,
             "expected_exit": -signal.SIGKILL})
        return divergent

    def recover_target(self, target, node, rewind, leader):
        assert (target / "postmaster.pid").is_file()
        assert node.controldata()["Database cluster state"] == "in production"
        assert rewind.can_rewind, node.controldata()
        (target / "postgresql.conf").write_text("")
        (target / "pg_wal/archive_status").mkdir(exist_ok=True)
        finished = Event()
        thread_pool.configure_global_pool(1)
        ha = Ha.__new__(Ha)
        empty = Cluster.empty()
        ha.cluster = Cluster(None, None, leader, empty.status, [], None, empty.sync, None, None)
        assert not ha.cluster.is_unlocked()
        ha._crash_recovery_started = 0
        ha._rewind = rewind
        ha._async_executor = AsyncExecutor(node.cancellable, finished.set)
        try:
            assert ha._handle_crash_recovery() == "doing crash recovery in a single user mode"
            assert finished.wait(90), "Patroni crash recovery did not finish"
        finally:
            if ha._async_executor.busy:
                ha._async_executor.cancel()
        assert not (target / "postmaster.pid").exists()
        facts = node.controldata()
        assert facts["Database cluster state"] == "shut down"
        assert (target / "global/pg_wal_control").read_bytes().split()[-2] == b"C1"
        fork = self.result["former_primary"]["fork_lsn"]
        assert lsn_number(facts["Latest checkpoint location"]) // self.segment_size > fork // self.segment_size
        self.check_retained_origin(target, fork)
        self.old_tree = preserved_tree(target)
        (self.output / "former-primary-inventory.json").write_text(json.dumps(self.old_tree, indent=2) + "\n")
        self.result["former_primary"]["offline_recovery"] = "patroni"

    def check_rejoined(self, target, port):
        assert (target / "global/pg_wal_control").read_bytes().split()[-2] != b"C1"
        super().check_rejoined(target, port)


class ManualFailoverAcceptance(FormerPrimaryAcceptance):
    label = "manual-physical-failover"

    def initialize_target(self, target):
        self.ledger = CommitLedger()
        follow.query(self.server.port, "CREATE TABLE failover_witness(id integer PRIMARY KEY CHECK (id <> 3))")
        follow.query(self.server.port, "INSERT INTO failover_witness VALUES(1)")
        self.ledger.record(1, "acknowledged")
        super().initialize_target(target)

    def witnesses(self, port):
        return [row[0] for row in follow.query(port, "SELECT id FROM failover_witness ORDER BY id")]

    def fence_original(self, node, port):
        standby = node.is_running()
        assert standby
        predecessor = (standby.pid, standby.create_time())
        follow.query(self.server.port, "INSERT INTO failover_witness VALUES(5)")
        self.ledger.record(5, "acknowledged")
        follow.wait_for(lambda: self.witnesses(port) == [1, 5])
        self.standby_frontier = follow.query(
            port, "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()")[0]
        assert all(self.standby_frontier)
        received, replayed = map(lsn_number, self.standby_frontier)
        assert received >= replayed > 0
        assert node.is_running() == standby
        # Stop this exact standby before making source-only commits.  Restart it
        # only after the source is dead, so it cannot receive those commits.
        stopped = self.stop_target(node)
        assert stopped and stopped["pid"] == standby.pid and stopped["exit"] == 0
        follow.query(self.server.port, "INSERT INTO failover_witness VALUES(2)")
        self.ledger.record(2, "acknowledged")
        try:
            follow.query(self.server.port, "INSERT INTO failover_witness VALUES(3)")
        except follow.psycopg2.Error as error:
            assert error.pgcode == "23514", error
            self.ledger.record(3, "rejected")
        else:
            raise AssertionError("source accepted the rejected witness")
        source = cancellation.wire.Connection(self.server.port)
        try:
            source.query("BEGIN")
            source.query("INSERT INTO failover_witness VALUES(4)")
            # This client sends COMMIT but never reads its response. A second
            # connection independently witnesses the old primary's outcome.
            source.send(b"Q", b"COMMIT\0")
            self.ledger.record(4, "unknown")
            follow.wait_for(lambda: self.witnesses(self.server.port) == [1, 2, 4, 5])
            self.crashed_identity = identity(self.server.port)
            process = self.server.process
            self.server.stop(immediate=True)
            self.result["processes"].append(
                {"kind": "source-crash", "pid": process.pid,
                 "exit": process.returncode, "expected_exit": -signal.SIGKILL})
            assert process.returncode == -signal.SIGKILL
        finally:
            source.close()
        with socket.socket() as probe:
            probe.settimeout(2)
            assert probe.connect_ex(("127.0.0.1", self.server.port)) != 0
        assert node.start(timeout=90) is True
        restarted = node.is_running()
        assert restarted and (restarted.pid, restarted.create_time()) != predecessor
        assert self.witnesses(port) == [1, 5]

    def prove_divergence(self):
        assert self.server.process is None
        return self.crashed_identity

    def verify_promotion(self, port):
        observed = self.ledger.compare(self.witnesses(port))
        assert observed["acknowledged"] == {"present": [1, 5], "absent": [2]}
        assert observed["rejected"] == {"present": [], "absent": [3]}
        assert observed["unknown"] == {"present": [], "absent": [4]}
        self.result["client_ledger"] = observed
        self.result["standby_receive_replay_before_cut"] = self.standby_frontier
        # Use a fixed ID outside the source workload's range, independent of
        # the old primary's sequence state.
        follow.query(port, "INSERT INTO failover_witness VALUES(1000)")
        assert self.witnesses(port) == [1, 5, 1000]

    def check_standby(self, port, expected):
        super().check_standby(port, expected)
        assert self.witnesses(port) == [1, 5, 1000]


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    cases = {"synthetic": Acceptance, "former-primary": FormerPrimaryAcceptance,
             "rewind-shared-history": RewindSharedHistoryAcceptance,
             "rewind-install": RewindInstallAcceptance,
             "rewind-cli": RewindCliAcceptance,
             "rewind-patroni": RewindPatroniAcceptance,
             "rewind-patroni-dirty": RewindDirtyPatroniAcceptance,
             "manual-failover": ManualFailoverAcceptance}
    parser.add_argument("--case", choices=cases, default="synthetic")
    arguments = parser.parse_args()
    cases[arguments.case](arguments.output).execute(["configured"])


if __name__ == "__main__":
    main()
