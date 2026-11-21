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
import shlex
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

    def prepare_child_for_follow(self, node, member):
        pass

    def initialize_target(self, target):
        started = time.monotonic()
        # Leave room for target.sock under the AF_UNIX path length limit.
        root = self.server.root / "child"
        data = root / "data"
        self.seed_child(data)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        # Read PG_VERSION from the source because a clone destination starts empty.
        config = follow.configuration(self.server.data, port, root)
        config.update(name="replacement_source", data_dir=str(data))
        node = Postgresql(config, Null())
        try:
            member = Member(0, "original", None,
                            {"conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres"})
            self.prepare_child_for_follow(node, member)
            (data / "postgresql.conf").touch()
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
    replay_pages = False

    def capture(self, label):
        path = self.server.root / label
        conninfo = f"host=127.0.0.1 port={self.server.port} user=postgres dbname=postgres"
        self.process(label, [str(cancellation.LISP / "bin/pg_basebackup"),
                             "-D", str(path), "-X", "stream", "-d", conninfo],
                     env=dict(os.environ, PG_LIBPQ_HELPER=self.helper))
        return path

    def seed_child(self, data):
        self.origin = self.capture("rewind-origin")
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
        replay = ""
        if self.replay_pages:
            path = self.server.root / "rewind-page-full-replay"
            path.mkdir(mode=0o700)
            replay = " #P" + cancellation.lisp_string(str(path) + "/")
        _, output = self.process("rewind-leased-origin", self.native_arguments() + [
            "--eval", f"(pg::native-patroni-rewind-leased-origin {arguments} "
                      f"{self.server.port} {helper} {fork} {digest}{replay})"])
        assert b"REWIND-LEASED-ORIGIN PASS" in output
        if self.replay_pages:
            assert b"REWIND-PAGE-REUSE PASS" in output
        assert all(preserved_tree(path) == inventory for path, inventory in before.items())
        self.result["checks"].append(self.label)


class ArchiveHistoryAcceptance(RewindSharedHistoryAcceptance):
    label = "archive-promotion-history"

    def seed_child(self, data):
        super().seed_child(data)
        self.archive = self.server.root / "promotion-archive"
        self.archive.mkdir(mode=0o700)
        self.promoted_data = data
        command = 'cp -a -- "%p" ' + shlex.quote(str(self.archive)) + '/"%f"'
        (data / "postgresql.auto.conf").write_text(
            "archive_mode = on\narchive_command = '" + command.replace("'", "''") + "'\n")

    def fence_original(self, node, port):
        assert follow.query(port, "SHOW archive_mode") == [("on",)]
        assert not any(self.archive.iterdir())
        super().fence_original(node, port)

    def verify_promotion(self, port):
        promoted = identity(port, history=True)
        name = promoted["history_file"]
        wal = self.promoted_data / "pg_wal"
        destination = self.archive / name
        receipt = wal / "archive_status" / (name + ".done")
        follow.wait_for(receipt.is_file)
        assert stat.S_ISREG(destination.lstat().st_mode)
        assert destination.read_bytes() == (wal / name).read_bytes() == promoted["history"].encode()
        fork = lsn_number(promoted["history"].split()[1])
        segment, offset = divmod(fork, 16 * 1024 * 1024)
        assert offset > 0
        partial, = wal.glob("*.wal.partial")
        assert partial.name in {f"{segment:016X}.wal.partial",
                                f"00000001{segment // 256:08X}{segment % 256:08X}.wal.partial"}
        child = wal / f"00000002{segment // 256:08X}{segment % 256:08X}.wal"
        assert stat.S_ISREG(partial.lstat().st_mode)
        assert partial.stat().st_size == offset
        assert partial.read_bytes() == child.read_bytes()[:offset]
        partial_receipt = wal / "archive_status" / (partial.name + ".done")
        follow.wait_for(partial_receipt.is_file)
        archived_partial = self.archive / partial.name
        assert stat.S_ISREG(archived_partial.lstat().st_mode)
        assert archived_partial.read_bytes() == partial.read_bytes()
        proof_name = partial.name + ".proof"
        follow.wait_for((wal / "archive_status" / (proof_name + ".done")).is_file)
        proof = (self.archive / proof_name).read_bytes()
        assert proof == (wal / proof_name).read_bytes()
        assert len(proof) == 52 and proof[:4] == b"PWAP"
        assert int.from_bytes(proof[32:40], "little") == fork
        assert not (self.archive / partial.name.removesuffix(".partial")).exists()
        self.result["archive_history"] = {"name": name, "text": promoted["history"],
                                          "local_partial": partial.name, "partial_size": offset}

    def configured(self):
        self.create_rewind_target()


class ArchiveDescendantAcceptance(ArchiveHistoryAcceptance):
    label = "archive-descendant"

    def verify_promotion(self, port):
        super().verify_promotion(port)
        follow.query(port, "CREATE TABLE archive_descendant_padding(id integer)")
        with closing(follow.psycopg2.connect(
                host="127.0.0.1", port=port, user="postgres", dbname="postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                for row in range(3000):
                    cursor.execute("INSERT INTO archive_descendant_padding VALUES (%s)", (row,))
                    if row % 100 == 0:
                        cursor.execute("SELECT pg_current_wal_flush_lsn() >= '0/1000000'::pg_lsn")
                        if cursor.fetchone() == (True,):
                            break
                else:
                    raise AssertionError("promoted source did not close the child segment")
        proof = "000000020000000000000000.wal.proof"
        follow.wait_for((self.archive / proof).is_file)

        root = self.server.root / "archive-observer"
        data = root / "data"
        root.mkdir(mode=0o700)
        shutil.copytree(self.origin, data)
        (data / "postgresql.conf").write_text("# Archive-only observer.\n")
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            observer_port = reservation.getsockname()[1]
        config = follow.configuration(data, observer_port, root)
        archive = shlex.quote(str(self.archive))
        self.configure_archive_observer(config, archive, root, port)
        node = Postgresql(config, Null())
        try:
            self.start_archive_observer(node, root, self.archive / proof)
            self.check_archive_route(observer_port)
            follow.wait_for(lambda: self.has_child_row(observer_port))
            self.check_archive_handoff(observer_port, port, root, proof)
            assert follow.query(observer_port, "SELECT pg_is_in_recovery()") == [(True,)]
            assert node.restart(timeout=90) is True
            follow.wait_for(lambda: self.has_child_row(observer_port))
            self.check_archive_restart(observer_port, port)
            self.result["checks"].append(self.label)
        finally:
            self.stop_target(node)

    def start_archive_observer(self, node, root, proof):
        assert node.follow(None, timeout=90) is True

    def configure_archive_observer(self, config, archive, root, source_port):
        config["recovery_conf"] = {"restore_command": f'test -f {archive}/"%f" && cp -- {archive}/"%f" "%p"'}

    def check_archive_route(self, port):
        assert follow.query(port, "SHOW primary_conninfo") == [("",)]

    def check_archive_handoff(self, observer_port, source_port, root, proof):
        pass

    def check_archive_restart(self, observer_port, source_port):
        pass

    @staticmethod
    def has_child_row(port):
        try:
            return follow.query(port, "SELECT id FROM patroni_reseed_child_only WHERE id = 42") == [(42,)]
        except follow.psycopg2.errors.UndefinedTable:
            return False


class ArchiveDescendantBoundAcceptance(ArchiveDescendantAcceptance):
    label = "archive-descendant-bound"

    def configure_archive_observer(self, config, archive, root, source_port):
        super().configure_archive_observer(config, archive, root, source_port)
        attempts = shlex.quote(str(root / "restored-files"))
        config["recovery_conf"]["restore_command"] += f' && printf "%%s\\n" "%f" >> {attempts}'
        follow.query(source_port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
        self.archive_member = Member(0, "promoted", None,
                                     {"conn_url": f"postgres://127.0.0.1:{source_port}/postgres"})

    def start_archive_observer(self, node, root, proof):
        assert node.follow(self.archive_member, timeout=90) is True

    def check_archive_route(self, port):
        assert follow.query(port, "SHOW primary_conninfo")[0][0]

    def check_archive_handoff(self, observer_port, source_port, root, proof):
        attempts = root / "restored-files"
        history = self.result["archive_history"]
        parent_partial = history["local_partial"]
        required = {history["name"], parent_partial, parent_partial + ".proof", proof}
        follow.wait_for(lambda: attempts.is_file() and
                        required.issubset(attempts.read_text().splitlines()), seconds=45)
        proof_bytes = (self.archive / proof).read_bytes()
        assert len(proof_bytes) == 52 and proof_bytes[:4] == b"PWAP"
        archive_end = int.from_bytes(proof_bytes[32:40], "little")
        cursor = ("SELECT status,slot_name,receive_start_tli,"
                  "pg_wal_lsn_diff(receive_start_lsn,'0/0'::pg_lsn)::bigint "
                  "FROM pg_stat_get_wal_receiver()")
        follow.wait_for(lambda: follow.query(observer_port, cursor) ==
                        [("streaming", "follow_one", 2, archive_end)])
        self.check_streamed_row(observer_port, source_port, 43)

    def check_archive_restart(self, observer_port, source_port):
        follow.wait_for(lambda: follow.query(observer_port,
                        "SELECT id FROM patroni_reseed_child_only WHERE id = 43") == [(43,)])
        self.check_streamed_row(observer_port, source_port, 44)

    @staticmethod
    def check_streamed_row(observer_port, source_port, row):
        receiver = "SELECT status,slot_name FROM pg_stat_get_wal_receiver()"
        follow.wait_for(lambda: follow.query(observer_port, receiver) == [("streaming", "follow_one")])
        follow.query(source_port, f"INSERT INTO patroni_reseed_child_only VALUES ({row})")
        follow.wait_for(lambda: follow.query(observer_port,
                        f"SELECT id FROM patroni_reseed_child_only WHERE id = {row}") == [(row,)])


class ArchiveDescendantCrashAcceptance(ArchiveDescendantAcceptance):
    label = "archive-descendant-crash"

    def start_archive_observer(self, node, root, proof):
        data = root / "data"
        history = self.result["archive_history"]["name"]
        canonical = data / "pg_wal" / history
        control_socket = root / "target.sock"
        receive = data / "pg_physical_receive"
        archived = self.archive / history
        cuts = (("after-history-publication", 0, lambda: node.follow(None, timeout=90)),
                ("after-history-selection", int(canonical.stem, 16), lambda: node.start(timeout=90)))
        for point, selected, start in cuts:
            self.crash_history_publication(node, control_socket, proof, point, start)
            control = (receive / "receive.control").read_bytes()
            assert len(control) in (68, 80, 96) and control[:8] == b"PGPHYS03"
            # The 68-byte format ends with its checksum at this offset.
            target = int.from_bytes(control[64:68], "little") if len(control) >= 80 else 0
            assert target == selected
            blob, = receive.glob(f"{canonical.stem}-*.history")
            if selected:
                digest = int.from_bytes(control[68:76], "little")
                assert blob == published == receive / f"{target:08X}-{digest:016X}.history"
            else:
                published = blob
            assert not canonical.exists()
            control_socket.unlink(missing_ok=True)
        # Startup must republish the selected history with no archive copy available.
        withheld = root / history
        archived.rename(withheld)
        try:
            assert node.start(timeout=90) is True
            assert canonical.read_bytes() == withheld.read_bytes()
        finally:
            withheld.rename(archived)

    def crash_history_publication(self, node, control_socket, proof, point, start):
        withheld = control_socket.with_name(proof.name)
        proof.rename(withheld)
        try:
            assert start() is True
            process = node.is_running()
            marker = control_socket.with_name(f"{point}.marker")
            follow.control(process, control_socket,
                           "(setf *physical-store-test-hook* "
                           "(lambda (point store) (declare (ignore store)) "
                           f"(when (eq point :{point}) "
                           f"(with-open-file (out {cancellation.lisp_string(str(marker))} :direction :output) "
                           "(format out \"~d~%\" (sb-posix:getpid)) (fsync-stream out)) "
                           "(sb-posix:kill (sb-posix:getpid) 9))))")
        finally:
            withheld.rename(proof)
        status = process.wait(timeout=90)
        self.result["processes"].append(
            {"kind": point, "pid": process.pid, "exit": status,
             "expected_exit": -signal.SIGKILL})
        assert status == -signal.SIGKILL and int(marker.read_text()) == process.pid


class RewindPageReuseAcceptance(RewindSharedHistoryAcceptance):
    label = "rewind-page-reuse"
    replay_pages = True

    def create_rewind_target(self):
        follow.query(self.server.port, "CREATE TABLE rewind_page_probe(id integer, payload text)")
        for i in range(32):
            payload = hashlib.shake_256(str(i).encode()).hexdigest(750)
            follow.query(self.server.port, f"INSERT INTO rewind_page_probe VALUES({i},'{payload}')")
        target = super().create_rewind_target()
        follow.query(self.server.port, "UPDATE rewind_page_probe SET payload='source side' WHERE id=16")
        return target

    def prove_divergence(self):
        self.start()
        follow.query(self.server.port, "UPDATE rewind_page_probe SET payload='target side' WHERE id=0")
        follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(999)")
        divergent = identity(self.server.port)
        self.stop()
        return divergent


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

    def execute_rewind(self, target, conninfo):
        _, output = self.process(self.label, [
            str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target), "--source-server", conninfo],
            timeout=900, env=dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper))
        return output

    def configured(self):
        target = self.create_rewind_target()
        conninfo = f"host=127.0.0.1 port={self.server.port} user=postgres dbname=postgres"
        output = self.execute_rewind(target, conninfo)
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


class RewindArchiveCliAcceptance(RewindCliAcceptance):
    label = "rewind-cli-target-archive"

    def prepare_target_archive(self, target):
        archive = target.parent / "target-archive"
        archive.mkdir(mode=0o700)
        files = list((target / "pg_wal").glob("*.wal"))
        assert files
        for path in files:
            path.rename(archive / path.name)
        return archive, {path.name for path in files}

    def execute_rewind(self, target, conninfo):
        archive, removed = self.prepare_target_archive(target)
        archived = preserved_tree(archive)
        self.old_tree = preserved_tree(target)
        config = target.parent / "target-archive.conf"
        trace = target.parent / "target-archive-reads"
        env = dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper)
        command = [str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target),
                   "--source-server", conninfo]
        self.process("rewind-without-target-archive", command, timeout=90, env=env, expected=1)
        assert preserved_tree(target) == self.old_tree
        restore = ["--restore-target-wal", f"--config-file={config}"]
        config.write_text("restore_command = 'printf x > %p'\n")
        self.process("rewind-with-truncated-archive", command + restore,
                     timeout=90, env=env, expected=1)
        assert preserved_tree(target) == self.old_tree
        assert not list(target.parent.glob(".pg-rewind-wal.*"))
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        settings = follow.configuration(target, port, self.server.root)
        settings["config_dir"] = str(target.parent / "archive-config")
        Path(settings["config_dir"]).mkdir(mode=0o700)
        node = Postgresql(settings, Null())
        try:
            template = f'cd "{archive}" && echo %f >> "{trace}" && cp "%f" "%p"'
            Path(node.config.postgresql_conf).write_text(f"restore_command = '{template}'\n")
            with patch.dict(os.environ, env), patch.object(node.cancellable, "call", wraps=node.cancellable.call) as call:
                assert node.get_guc_value("restore_command") == template
                assert Rewind(node).pg_rewind({"host": "127.0.0.1", "port": self.server.port,
                                               "user": "postgres", "dbname": "postgres"})
            arguments = call.call_args.args[0]
            assert "--restore-target-wal" in arguments
            assert f"--config-file={node.config.postgresql_conf}" in arguments
            output = call.call_args.kwargs["communicate"]["stdout"]
        finally:
            node.connection_pool.close()
        restored = trace.read_text().splitlines()
        assert restored and len(set(restored)) == len(restored)
        assert set(restored) <= removed
        self.result["restored_target_wal"] = restored
        assert preserved_tree(archive) == archived
        assert not list(target.parent.glob(".pg-rewind-wal.*"))
        return output


class RewindWorkerArchiveAcceptance(RewindArchiveCliAcceptance):
    label = "rewind-worker-target-archive"

    def prove_divergence(self):
        self.archive = self.server.root / "worker-target-archive"
        self.archive.mkdir(mode=0o700)
        data = self.server.data
        auto_conf = data / "postgresql.auto.conf"
        old_config = auto_conf.read_bytes() if auto_conf.exists() else None
        command = 'cp -a -- "%p" ' + shlex.quote(str(self.archive)) + '/"%f"'
        try:
            auto_conf.write_bytes((old_config or b"") + (
                "\narchive_mode = on\narchive_command = '" + command.replace("'", "''") + "'\n").encode())
            self.start()
            follow.query(self.server.port, "INSERT INTO patroni_reseed_probe VALUES(999)")
            follow.query(self.server.port, "CREATE TABLE archive_wal_padding(id integer)")
            with closing(follow.psycopg2.connect(
                    host="127.0.0.1", port=self.server.port, user="postgres",
                    dbname="postgres", connect_timeout=2)) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    for row in range(2200):
                        cursor.execute("INSERT INTO archive_wal_padding VALUES (%s)", (row,))
            divergent = identity(self.server.port)
            assert lsn_number(divergent["position"]) >= 16 * 1024 * 1024
            wal = data / "pg_wal"
            name = "000000010000000000000000.wal"
            receipt = wal / "archive_status" / (name + ".done")
            follow.wait_for(receipt.is_file)
            archived = self.archive / name
            assert stat.S_ISREG(archived.lstat().st_mode)
            assert archived.read_bytes() == (wal / name).read_bytes()
            self.produced = {"name": name, "bytes": archived.stat().st_size,
                             "sha256": hashlib.sha256(archived.read_bytes()).hexdigest()}
            self.produced_receipt = receipt.read_text()
            return divergent
        finally:
            try:
                self.stop()
            finally:
                if old_config is None:
                    auto_conf.unlink(missing_ok=True)
                else:
                    auto_conf.write_bytes(old_config)

    def prepare_target_archive(self, target):
        assert self.result["former_primary"]["fork_lsn"] < 16 * 1024 * 1024
        name = self.produced["name"]
        wal = target / "pg_wal"
        receipt = wal / "archive_status" / (name + ".done")
        assert receipt.read_text() == self.produced_receipt
        archived = self.archive / name
        assert hashlib.sha256(archived.read_bytes()).hexdigest() == self.produced["sha256"]
        local = wal / name
        if local.exists():
            assert local.read_bytes() == archived.read_bytes()
            local.unlink()
        assert not local.exists()
        self.result["worker_archive"] = self.produced
        return self.archive, {name}


class RewindCaptureResumeCliAcceptance(RewindCliAcceptance):
    label = "rewind-cli-capture-resume"
    capture_hook = "native-patroni-rewind-crash-after-capture"
    captured = True

    def retained_rewind_stages(self):
        return list(self.server.data.parent.glob(".pg-native-backups-*/rewind.*"))

    def execute_rewind(self, target, conninfo):
        env = dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper)
        self.process("rewind-capture-crash", self.native_arguments() + [
            "--eval", f"(pg::{self.capture_hook})",
            "--load", str(cancellation.LISP / "bin/pg-rewind.lisp"), "--end-toplevel-options",
            "-D", str(target), "--source-server", conninfo],
            timeout=240, env=env, expected=-signal.SIGKILL)
        assert (target / "physical-reseed.attempt").is_file()
        assert not (target / "physical-reseed.install").exists()
        retained = self.retained_rewind_stages()
        if self.captured:
            stage, = retained
            assert stat.S_IMODE(stage.stat().st_mode) == 0o700
            seal = stage / "rewind.seal"
            assert stat.S_IMODE(seal.stat().st_mode) == 0o600
            stage_inode = stage.stat().st_ino
            seal_inode = seal.stat().st_ino
            self.stop()
            orphan = stage.parent / "rewind.orphan_probe"
            orphan.mkdir(mode=0o700)
            self.start()
            assert self.retained_rewind_stages() == retained
            assert not orphan.exists()
            assert stage.stat().st_ino == stage_inode and seal.stat().st_ino == seal_inode
            replay = self.server.root / "rewind-replay"
            replay.mkdir(mode=0o700)
            arguments = " ".join("#P" + cancellation.lisp_string(str(path) + "/")
                                 for path in (target, replay))
            helper = cancellation.lisp_string(self.helper)
            _, output = self.process("rewind-replay", self.native_arguments() + [
                "--eval", f"(pg::native-patroni-rewind-replay {arguments} {helper})"],
                timeout=240, env=env)
            assert b"REWIND-REPLAY PASS" in output
        else:
            assert not retained
        _, resumed = self.process("rewind-capture-resume", [
            str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target), "--resume"],
            timeout=900, env=env)
        assert not self.retained_rewind_stages()
        return resumed


class RewindFirstCaptureCliAcceptance(RewindCaptureResumeCliAcceptance):
    label = "rewind-cli-first-capture"
    capture_hook = "native-patroni-rewind-crash-before-capture"
    captured = False


class RewindPartialCaptureCliAcceptance(RewindCaptureResumeCliAcceptance):
    label = "rewind-cli-partial-capture"

    def interrupt(self, target, step, arguments, env):
        self.process("rewind-capture-crash" if step == "after-partial-receive" else "rewind-repair-crash",
                     self.native_arguments() + [
                         "--eval", f"(pg::native-patroni-rewind-crash-at-image-step :{step})",
                         "--load", str(cancellation.LISP / "bin/pg-rewind.lisp"),
                         "--end-toplevel-options", "-D", str(target), *arguments],
                     timeout=240, env=env, expected=-signal.SIGKILL)

    def execute_rewind(self, target, conninfo):
        env = dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper)
        self.interrupt(target, "after-partial-receive", ["--source-server", conninfo], env)
        stage, = target.parent.glob(".pg-rewind.*/stage")
        retained, = self.retained_rewind_stages()
        journal = (target / "physical-reseed.attempt").read_bytes()
        partial, = [path for path in stage.rglob("*") if path.is_file()
                    and 0 < path.stat().st_size < (retained / path.relative_to(stage)).stat().st_size]
        original = partial.read_bytes()
        stage_inode, file_inode = stage.stat().st_ino, partial.stat().st_ino
        with (retained / partial.relative_to(stage)).open("rb") as file:
            assert file.read(len(original)) == original
        self.interrupt(target, "after-rewind-prefix-append", ["--resume"], env)
        assert (target / "physical-reseed.attempt").read_bytes() == journal
        assert not (target / "physical-reseed.install").exists()
        assert stage.stat().st_ino == stage_inode and partial.stat().st_ino == file_inode
        extended = partial.read_bytes()
        assert len(extended) > len(original) and extended.startswith(original)
        with (retained / partial.relative_to(stage)).open("rb") as file:
            assert file.read(len(extended)) == extended
        _, resumed = self.process("rewind-capture-resume", [
            str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target), "--resume"],
            timeout=900, env=env)
        assert not self.retained_rewind_stages()
        assert stage.stat().st_ino == stage_inode and partial.stat().st_ino == file_inode
        assert partial.read_bytes().startswith(extended)
        return resumed


class RewindResumeCliAcceptance(RewindCliAcceptance):
    label = "rewind-cli-resume"

    def execute_rewind(self, target, conninfo):
        env = dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper)
        _, interrupted = self.process("rewind-install-crash", self.native_arguments() + [
            "--eval", "(pg::native-patroni-rewind-crash-after-install-step)",
            "--load", str(cancellation.LISP / "bin/pg-rewind.lisp"), "--end-toplevel-options",
            "-D", str(target), "--source-server", conninfo],
            timeout=240, env=env, expected=-signal.SIGKILL)
        journal = (target / "physical-reseed.install").read_bytes()
        assert re.search(rb"^STEP 1 [0-9A-F]{8}$", journal, re.M)
        assert not re.search(rb"^STEP 2 ", journal, re.M)
        self.process("rewind-configuration-crash", self.native_arguments() + [
            "--eval", "(pg::native-patroni-rewind-crash-after-configuration-step)",
            "--load", str(cancellation.LISP / "bin/pg-rewind.lisp"), "--end-toplevel-options",
            "-D", str(target), "--resume"],
            timeout=900, env=env, expected=-signal.SIGKILL)
        assert (target / "physical-reseed.follow").is_file()
        self.process("rewind-retirement-crash", self.native_arguments() + [
            "--eval", "(pg::native-patroni-rewind-crash-after-retirement-move)",
            "--load", str(cancellation.LISP / "bin/pg-rewind.lisp"), "--end-toplevel-options",
            "-D", str(target), "--resume"],
            timeout=900, env=env, expected=-signal.SIGKILL)
        assert not (target / "physical-reseed.install").exists()
        assert (target / "physical-reseed.attempt").is_file()
        _, resumed = self.process("rewind-install-resume", [
            str(cancellation.LISP / "bin/pg_rewind"), "-D", str(target), "--resume"],
            timeout=900, env=env)
        return interrupted + resumed


class RewindPatroniAcceptance(RewindInstallAcceptance):
    label = "rewind-patroni"
    checksum_version_on_invalid_page = None

    def recover_target(self, target, node, rewind, leader):
        pass

    def checkpoint_hint(self, config):
        return {"checkpoint_after_promote": True}

    def execute_patroni_rewind(self, rewind, leader):
        rewind.execute(leader)

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
        checkpoint_hint = self.checkpoint_hint(config)
        node = Postgresql(config, Null())
        rewind = Rewind(node)
        follow.query(self.server.port, "CHECKPOINT")
        leader = Leader(0, None, Member(0, "source", None, {
            "conn_url": f"postgres://127.0.0.1:{self.server.port}/postgres",
            "role": PostgresqlRole.PRIMARY, **checkpoint_hint}))
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
                    self.execute_patroni_rewind(rewind, leader)
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


class PatroniLifecycleAcceptance(RewindPatroniAcceptance):
    label = "patroni-lifecycle"
    receiver_status = "SELECT status, slot_name FROM pg_stat_get_wal_receiver()"

    def seed_child(self, data):
        data.parent.mkdir(mode=0o700)
        data.mkdir(mode=0o700)

    def prepare_child_for_follow(self, node, member):
        started = time.monotonic()
        with patch.dict(os.environ, SBCL=self.sbcl, PG_LIBPQ_HELPER=self.helper):
            assert node.bootstrap.clone(member) is True
        data = Path(node.data_dir)
        assert (data / "physical-backup.manifest").is_file()
        assert (data / "postgresql.conf").is_file()
        self.result["checks"].append("patroni-clone")
        self.result.setdefault("timings", []).append(
            {"case": "patroni-clone", "elapsed_seconds": time.monotonic() - started,
             "outcome": "pass"})

    def configured(self):
        (self.server.data / "postgresql.conf").touch()
        follow.query(self.server.port, "CREATE TABLE patroni_lifecycle_sync(id integer)")
        super().configured()

    def fence_original(self, node, port):
        source = self.server.port
        self.wait_for_slot(port, "replacement_source")
        previous = node.is_running().pid
        assert node.restart(timeout=90) is True
        assert node.is_running().pid != previous
        self.wait_for_slot(port, "replacement_source")
        config = self.server.data / "postgresql.conf"
        original = config.read_bytes()
        names = "FIRST 1 (replacement_source)"
        try:
            config.write_bytes(original + f"\nsynchronous_standby_names = '{names}'\n".encode())
            assert follow.query(source, "SELECT pg_reload_conf()") == [(True,)]
            follow.wait_for(lambda: follow.query(
                source, "SELECT w.sync_state FROM pg_stat_get_wal_senders() w, "
                        "pg_stat_get_activity(w.pid) "
                        "WHERE application_name = 'replacement_source' "
                        "AND w.state = 'streaming'") == [("sync",)])
            with closing(follow.psycopg2.connect(
                    host="127.0.0.1", port=source, user="postgres", dbname="postgres",
                    connect_timeout=2)) as connection, connection.cursor() as cursor:
                connection.autocommit = True
                cursor.execute("SET statement_timeout TO '30s'")
                cursor.execute("SET synchronous_commit TO remote_apply")
                cursor.execute("INSERT INTO patroni_lifecycle_sync VALUES(70)")
            self.check_synchronous_commit(port)
        finally:
            config.write_bytes(original)
            assert follow.query(source, "SELECT pg_reload_conf()") == [(True,)]
            follow.wait_for(lambda: follow.query(source, "SHOW synchronous_standby_names") == [("",)])
        self.result["checks"].append("patroni-reconnect-physical-slot-and-remote-apply")
        super().fence_original(node, port)

    def check_synchronous_commit(self, port):
        assert follow.query(port, "SELECT id FROM patroni_lifecycle_sync") == [(70,)]

    def wait_for_slot(self, port, slot):
        follow.wait_for(lambda: follow.query(port, self.receiver_status) == [("streaming", slot)])

    def verify_promotion(self, port):
        self.check_synchronous_commit(port)

    def check_standby(self, port, expected):
        super().check_standby(port, expected)
        self.check_synchronous_commit(port)
        self.wait_for_slot(port, "follow_one")


class RewindPatroniCheckpointAcceptance(RewindPatroniAcceptance):
    label = "rewind-patroni-checkpoint-query"

    def checkpoint_hint(self, config):
        config["authentication"]["rewind"] = {"username": "postgres",
                                               "application_name": "rewind_checkpoint_probe"}
        config_dir = self.server.root / "checkpoint-probe-config"
        config_dir.mkdir(mode=0o700)
        (config_dir / "postgresql.conf").write_text("archive_mode = off\n")
        config["config_dir"] = str(config_dir)
        return {}

    def execute_patroni_rewind(self, rewind, leader):
        assert leader.checkpoint_after_promote is None
        assert rewind._postgresql.get_guc_value("archive_mode") == "off"
        assert rewind._postgresql.get_guc_value("archive_command") == "(disabled)"
        assert rewind.get_archive_command() is None
        with patch.object(rewind, "check_leader_has_run_checkpoint",
                          wraps=rewind.check_leader_has_run_checkpoint) as gate:
            rewind.execute(leader)
        gate.assert_called_once()
        assert gate.call_args.args[0]["application_name"] == "rewind_checkpoint_probe"


class RewindPatroniScramAcceptance(PatroniLifecycleAcceptance):
    label = "patroni-lifecycle-scram"
    rewind_user = "rewind_lifecycle"
    rewind_password = "rewind-lifecycle-fixture"

    def checkpoint_hint(self, config):
        # The promoted source stays available to the normal fixture probes.
        # Only the dedicated rewind account must use SCRAM.
        source = self.server.port
        follow.query(source, f"CREATE ROLE {self.rewind_user} LOGIN NATIVE_REWIND PASSWORD '{self.rewind_password}'")
        for signature in ("pg_ls_dir(text, boolean, boolean)", "pg_stat_file(text, boolean)",
                          "pg_read_binary_file(text)", "pg_read_binary_file(text, bigint, bigint, boolean)"):
            follow.query(source, f"GRANT EXECUTE ON FUNCTION pg_catalog.{signature} TO {self.rewind_user}")
        hba = self.server.data / "pg_hba.conf"
        hba.write_text(f"host all {self.rewind_user} 127.0.0.1/32 scram-sha-256\n"
                       f"host replication {self.rewind_user} 127.0.0.1/32 scram-sha-256\n"
                       + hba.read_text())
        assert follow.query(source, "SELECT pg_reload_conf()") == [(True,)]
        config["authentication"]["rewind"] = {"username": self.rewind_user,
                                                "password": self.rewind_password}
        return {}

    def execute_patroni_rewind(self, rewind, leader):
        credentials = rewind._conn_kwargs(leader, {"username": self.rewind_user,
                                                   "password": self.rewind_password})
        assert rewind.check_leader_has_run_checkpoint(credentials) is None
        with patch.object(rewind, "check_leader_has_run_checkpoint",
                          wraps=rewind.check_leader_has_run_checkpoint) as gate:
            rewind.execute(leader)
        gate.assert_called_once()
        assert gate.call_args.args[0]["user"] == self.rewind_user


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
               "archive-promotion-history": ArchiveHistoryAcceptance,
                  "archive-descendant": ArchiveDescendantAcceptance,
                  "archive-descendant-bound": ArchiveDescendantBoundAcceptance,
                 "archive-descendant-crash": ArchiveDescendantCrashAcceptance,
               "rewind-page-reuse": RewindPageReuseAcceptance,
             "rewind-install": RewindInstallAcceptance,
             "rewind-cli": RewindCliAcceptance,
               "rewind-cli-target-archive": RewindArchiveCliAcceptance,
                "rewind-worker-target-archive": RewindWorkerArchiveAcceptance,
             "rewind-cli-capture-resume": RewindCaptureResumeCliAcceptance,
             "rewind-cli-first-capture": RewindFirstCaptureCliAcceptance,
               "rewind-cli-partial-capture": RewindPartialCaptureCliAcceptance,
             "rewind-cli-resume": RewindResumeCliAcceptance,
             "rewind-patroni": RewindPatroniAcceptance,
                 "patroni-lifecycle": PatroniLifecycleAcceptance,
                   "patroni-lifecycle-scram": RewindPatroniScramAcceptance,
               "rewind-patroni-checkpoint-query": RewindPatroniCheckpointAcceptance,
             "rewind-patroni-dirty": RewindDirtyPatroniAcceptance,
             "manual-failover": ManualFailoverAcceptance}
    parser.add_argument("--case", choices=cases, default="synthetic")
    arguments = parser.parse_args()
    cases[arguments.case](arguments.output).execute(["configured"])


if __name__ == "__main__":
    main()
