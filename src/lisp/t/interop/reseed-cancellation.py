"""Run native cancellation acceptance with disposable source and target images.

Run from any directory with python3 src/lisp/t/interop/reseed-cancellation.py.
SBCL and PG_LIBPQ_HELPER overrides use the normal project discovery paths.
The printed result path contains process evidence and retained subprocess logs.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time

if not __debug__:
    raise RuntimeError("native cancellation tests require Python assertions")

sys.dont_write_bytecode = True
LISP = Path(__file__).resolve().parents[2]
DRIVER = Path(__file__).with_name("reseed-cancellation-native.lisp")
spec = importlib.util.spec_from_file_location("cancellation_owned_source", LISP / "t/command-complete.py")
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def lisp_string(value):
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def facts(path):
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_mode, stat.st_nlink, stat.st_uid,
            stat.st_gid, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, path.read_bytes())


class Acceptance:
    def __init__(self, output):
        self.sbcl = shutil.which(os.environ.get("SBCL", "sbcl"))
        if self.sbcl is None:
            raise RuntimeError("SBCL is unavailable")
        os.environ["SBCL"] = self.sbcl
        self.server = wire.OwnedLisp()
        self.output = (output or self.server.root / "acceptance").resolve()
        self.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.environ["ASDF_OUTPUT_TRANSLATIONS"] = (
            "(:output-translations (t " + lisp_string(str(self.output / "source-cache") + "/") +
            ") :ignore-inherited-configuration)")
        self.result = {"root": str(self.server.root), "checks": [], "processes": [], "complete": False}
        self.server.control = self.control
        self.helper = None

    def process(self, kind, arguments, *, env=None, text_input=None, timeout=240, expected=0):
        log = self.output / f"{len(self.result['processes'])}-{kind}.log"
        started = time.monotonic()
        with log.open("xb") as stream:
            process = subprocess.Popen(arguments, cwd=LISP, env=env, stdout=stream,
                                       stderr=subprocess.STDOUT,
                                       stdin=subprocess.PIPE if text_input is not None else subprocess.DEVNULL)
            try:
                process.communicate(None if text_input is None else text_input.encode(), timeout=timeout)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                self.result["processes"].append(
                    {"kind": kind, "pid": process.pid, "exit": process.returncode,
                       "expected_exit": expected, "log": str(log),
                       "elapsed_seconds": time.monotonic() - started})
        assert process.returncode == expected, (kind, process.returncode, str(log))
        output = log.read_bytes()
        assert b"\0" not in output, str(log)
        return process.pid, output

    def control(self, expression, append_quit=True):
        assert self.server.process is not None and self.server.process.poll() is None
        _, output = self.process(
            "control", [self.sbcl, "--script", str(LISP / "t/control-client.lisp"),
                        "unix", str(self.server.control_socket)],
            text_input=expression + ("\n(quit)\n" if append_quit else "\n"), timeout=60)
        assert b";; error:" not in output, output.decode()
        return output.decode()

    def start(self):
        self.server.start()
        self.control("(progn (load #P" + lisp_string(DRIVER) + ") (values))")
        self.control("(progn (load #P" + lisp_string(DRIVER.with_name("reseed-settlement-native.lisp")) +
                     ") (values))")

    def stop(self):
        process = self.server.process
        self.server.stop()
        if process is not None:
            self.result["processes"].append(
                {"kind": "source", "pid": process.pid, "exit": process.returncode, "expected_exit": 0})

    def run(self, action, target, phase="none", connections=None, outcome="complete", *,
            cut=None, lose_reply=None, kill_effect=False, refusal_cause=None,
            configured=False, workspace=None, cut_phase="cleaned", recovery="none", evidence=None):
        killed = cut is not None or kill_effect
        assert not (killed and connections is not None)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("NATIVE_CANCELLATION_")}
        env.update({"NATIVE_CANCELLATION_TARGET": str(target),
                    "NATIVE_CANCELLATION_ACTION": action,
                    "NATIVE_CANCELLATION_PHASE": phase,
                    "NATIVE_CANCELLATION_OUTCOME": outcome,
                    "NATIVE_CANCELLATION_HELPER": self.helper})
        if configured:
            env["NATIVE_CANCELLATION_CONFIGURED"] = "yes"
        if workspace is not None:
            env["NATIVE_CANCELLATION_WORKSPACE"] = str(workspace)
        env["NATIVE_CANCELLATION_RECOVERY"] = recovery
        if evidence is not None:
            env["NATIVE_CANCELLATION_EVIDENCE"] = str(evidence)
        if connections is not None:
            env["NATIVE_CANCELLATION_EXPECT_CONNECTIONS"] = str(connections)
        if cut is not None:
            env["NATIVE_CANCELLATION_CUT"] = cut
            env["NATIVE_CANCELLATION_CUT_PHASE"] = cut_phase
            if cut == "write":
                env["NATIVE_CANCELLATION_PARTIAL"] = "yes"
        if lose_reply is not None:
            env["NATIVE_CANCELLATION_LOSE_REPLY"] = lose_reply
        if kill_effect:
            assert lose_reply is not None
            env["NATIVE_CANCELLATION_KILL_EFFECT"] = "yes"
        if refusal_cause is not None:
            env["NATIVE_CANCELLATION_REFUSAL_CAUSE"] = refusal_cause
        registry = "(asdf:initialize-source-registry '(:source-registry (:tree " + lisp_string(str(LISP) + "/") + ") :inherit-configuration))"
        cache = "(asdf:initialize-output-translations '(:output-translations (t " + lisp_string(str(self.output / "cache") + "/") + ") :ignore-inherited-configuration))"
        pid, output = self.process(
            action, [self.sbcl, "--noinform", "--no-sysinit", "--no-userinit", "--non-interactive",
                     "--eval", "(require :asdf)", "--eval", registry, "--eval", cache,
                     "--eval", '(asdf:load-system "pg")', "--load", str(DRIVER),
                     "--eval", "(pg::native-cancellation-run)"], env=env, expected=-9 if killed else 0)
        self.result["processes"][-1].update(
            {"phase": phase, "connections": connections, "cut": cut,
               "lose_reply": lose_reply, "kill_effect": kill_effect, "refusal_cause": refusal_cause,
               "cut_phase": cut_phase if cut is not None else None, "recovery": recovery,
               "target": str(target), "evidence": str(evidence) if evidence is not None else None})
        if cut is not None:
            point = {"create": "AFTER-FOLLOW-TEMPORARY-CREATE", "write": "AFTER-FOLLOW-WRITE-CHUNK",
                     "fsync": "AFTER-FOLLOW-TEMPORARY-FSYNC", "rename": "AFTER-FOLLOW-RENAME"}[cut]
            assert f"NATIVE-CANCELLATION-CUT {pid} :{point} {target}/".encode() in output
            if evidence is not None:
                before = Path(str(evidence) + ".before").read_bytes()
                after = Path(str(evidence) + ".after").read_bytes()
                assert before != after
                count = {"pending": 0, "source-closed": 1, "cleaned": 2}[cut_phase]
                marker = f"NATIVE-CANCELLATION-CANDIDATE {pid} :{cut_phase.upper()} {len(before)} {len(after)} {count}"
                assert marker.encode() in output.splitlines()
        elif not killed:
            assert f"NATIVE-CANCELLATION-CHECKED {pid} {action}".encode() in output
        if lose_reply is not None:
            ordinal = 2 if lose_reply == "issuer" else 1
            assert f"NATIVE-CANCELLATION-DISCARDED {pid} {lose_reply} {ordinal}".encode() in output
        if action == "recover-refused":
            assert refusal_cause and refusal_cause.encode() in output
            assert f"NATIVE-CANCELLATION-REFUSED {pid} ".encode() in output
        if action == "factory-refused":
            assert refusal_cause and refusal_cause.encode() in output
            assert f"NATIVE-CANCELLATION-FACTORY-REFUSED {pid} {recovery} ".encode() in output
        if action == "reuse-refused":
            assert f"NATIVE-CANCELLATION-REUSE-REFUSED {pid}".encode() in output
        if connections is not None:
            assert f"NATIVE-CANCELLATION-CONNECTIONS {pid} {connections}".encode() in output
        if not killed and (phase == "cleaned" or action == "cleanup"):
            assert not os.path.lexists(target / "physical-reseed.follow.tmp")
        return pid

    def initialize_target(self, target):
        target.mkdir(mode=0o700)
        (target / ".pg_lisp.lock").touch(mode=0o600)
        (target / "opaque-old-data").write_bytes(b"original data\x00\xff")

    def install(self, index, label, selected):
        paths = [self.server.root / (label + "-" + suffix)
                 for suffix in ("target", "stage", "incoming", "quarantine")]
        for path in paths[1:]:
            path.mkdir(mode=0o700)
        target = paths[0]
        self.initialize_target(target)
        go = self.server.root / (label + "-go")
        done = self.server.root / (label + "-done")
        arguments = " ".join("#P" + lisp_string(str(path) + "/") for path in paths)
        follow = lisp_string("native_target_follow_" + str(index)) if selected else "nil"
        self.control("(progn (pg::native-cancellation-start-initial-install " + arguments +
                     f" {index} {self.server.port} " + lisp_string(self.helper) +
                     " #P" + lisp_string(go) + " #P" + lisp_string(done) + " " + follow + ") (values))")
        go.touch()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            assert self.server.process.poll() is None
            if done.exists() and done.read_text().strip():
                break
            time.sleep(0.05)
        assert done.exists() and done.read_text().strip(), str(done)
        self.control("(progn (pg::native-cancellation-join-initial-install) (values))")
        assert done.read_text().strip() == "PASS", done.read_text()
        fixed = [target / name for name in
                 ("physical-reseed.attempt", "physical-reseed.install", "physical-recovery.control",
                  "global/pg_wal_control", "global/pg_control", ".pg_lisp.lock")]
        fixed.append(paths[3] / "opaque-old-data")
        return target, fixed, [facts(path) for path in fixed]

    def audit(self, index, aborted=False, selected_present=False):
        lease = lisp_string("reseed_" + format(index, "032x"))
        expression = ("(pg::native-cancellation-check-aborted-source " + lease + ")" if aborted else
                      "(pg::native-cancellation-check-source " + lease + " " +
                      lisp_string("native_target_follow_" + str(index)) +
                      (" t)" if selected_present else ")"))
        self.control("(progn " + expression + " (values))")

    def handoff(self, index, target, allocated=True, absent=False):
        if allocated:
            self.run("prove", target)
        self.control("(progn (pg::native-cancellation-pause-closing) (values))")
        self.run("handoff-allocated" if allocated else "handoff-legacy", target)
        self.control("(progn (pg::native-cancellation-lose-selection " +
                     lisp_string("reseed_" + format(index, "032x")) + " " +
                     lisp_string("native_target_follow_" + str(index)) + " " +
                     ("t" if absent else "nil") + ") (values))")

    def selected(self):
        for index, (allocated, absent) in enumerate(
                ((True, False), (True, True), (False, False), (False, True)), 6001):
            label = "selected-" + ("allocated" if allocated else "legacy") + ("-absent" if absent else "-lost")
            target, fixed, before = self.install(index, label, selected=True)
            self.handoff(index, target, allocated, absent)
            self.run("begin", target, "pending", connections=0)
            self.run("cleanup", target, "cleaned", connections=2)
            saved = facts(target / "physical-reseed.follow")
            self.run("cleanup", target, "cleaned", connections=0)
            assert facts(target / "physical-reseed.follow") == saved
            assert [facts(path) for path in fixed] == before
            self.audit(index)
            self.result["checks"].append(label)

    def configured_inventory(self, roots, target):
        inventory = {}
        for root in roots:
            for path in [root, *sorted(root.rglob("*"))]:
                if path.parent == target and path.name in (
                        "physical-reseed.follow", "physical-reseed.follow.tmp"):
                    continue
                metadata = path.lstat()
                if stat.S_ISDIR(metadata.st_mode):
                    inventory[str(path)] = (metadata.st_dev, metadata.st_ino,
                                            metadata.st_mode, metadata.st_uid, metadata.st_gid)
                else:
                    assert stat.S_ISREG(metadata.st_mode), str(path)
                    inventory[str(path)] = facts(path)
        return inventory

    def configured_startup_fence(self, target, roots, before, phase, *, temporary=None,
                                 diagnostic="pg-lisp: fatal: retirement requires completed activation"):
        assert self.configured_inventory(roots, target) == before
        journal = facts(target / "physical-reseed.follow")
        temporary_path = target / "physical-reseed.follow.tmp"
        assert (facts(temporary_path) if os.path.lexists(temporary_path) else None) == temporary
        pid, output = self.process(
            "configured-startup", [str(LISP / "pg-lisp.sh"), "-D", str(target),
                                   "-p", str(self.server.port)], expected=1)
        assert diagnostic.encode() in output
        assert not os.path.lexists(target / "postmaster.pid")
        assert (facts(temporary_path) if os.path.lexists(temporary_path) else None) == temporary
        assert facts(target / "physical-reseed.follow") == journal
        after = self.configured_inventory(roots, target)
        lock = str(target / ".pg_lisp.lock")
        # Opening the data lock applies mode 0600 and can update its ctime.
        previous, current = before[lock], after[lock]
        assert previous[:8] == current[:8] and previous[9:] == current[9:]
        expected = dict(before)
        expected[lock] = current
        assert after == expected
        self.result.setdefault("startup_fences", []).append(
            {"pid": pid, "target": str(target), "phase": phase,
             "lock_ctime_before_ns": previous[8], "lock_ctime_after_ns": current[8],
             "diagnostic": diagnostic, "temporary_size": len(temporary[-1]) if temporary is not None else None})
        return after

    def audit_configured_selection(self, index):
        self.control("(progn (pg::native-settlement-test-source-audit " +
                     lisp_string("reseed_" + format(index, "032x")) + " " +
                     lisp_string("native_target_follow_" + str(index)) +
                     ' "handed-off") (values))')

    def prepare_configured(self, index, label, settled):
        target, _, _ = self.install(index, label, selected=True)
        self.run("prove", target)
        workspace = self.server.root / (label + "-workspace")
        workspace.mkdir(mode=0o700)
        self.run("configure", target, configured=True, workspace=workspace, connections=0)
        if settled:
            self.run("settle", target, configured=True)
            self.audit_configured_selection(index)
        # Take the inventory after configuration has moved its files.
        roots = [self.server.root / (label + "-" + suffix)
                 for suffix in ("target", "stage", "incoming", "quarantine", "workspace")]
        assert any(path.is_file() for path in (target / "base").rglob("*"))
        return target, roots, self.configured_inventory(roots, target)

    def audit_configured_allocation(self, index, state):
        assert state in ("ready", "aborted", "released")
        self.control("(progn (pg::native-cancellation-check-source-allocation " +
                     lisp_string("reseed_" + format(index, "032x")) + " " +
                     lisp_string("native_target_follow_" + str(index)) + " :" + state +
                     ") (values))")
        self.result.setdefault("allocation_checks", []).append({"index": index, "state": state})

    def configured_reuse_refused(self, index, target, roots, before):
        saved = facts(target / "physical-reseed.follow")
        # Arm the source observer for each probe, including after restart.
        self.control("(progn (pg::native-cancellation-reset-source-events) " +
                     "(pg::native-cancellation-observe-reuse) (values))")
        self.run("reuse-refused", target, "cleaned", configured=True, connections=1)
        self.control("(progn (pg::native-cancellation-check-reuse-witness " +
                     lisp_string("native_target_follow_" + str(index)) + " t) " +
                     "(pg::native-cancellation-check-source-events nil) (values))")
        self.audit(index)
        self.audit_configured_allocation(index, "released")
        assert facts(target / "physical-reseed.follow") == saved
        assert self.configured_inventory(roots, target) == before
        self.result.setdefault("configured_reuse_checks", []).append({"index": index})

    def configured(self):
        for index, settled in enumerate((True, False), 6801):
            started = time.monotonic()
            label = "configured-" + ("selected" if settled else "generation-debt")
            target, roots, before = self.prepare_configured(index, label, settled)
            if not settled:
                self.audit_configured_allocation(index, "ready")
            before = self.configured_startup_fence(target, roots, before, "inert")
            self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
            self.run("begin", target, "pending", configured=True, connections=0)
            before = self.configured_startup_fence(target, roots, before, "pending")
            phase, outcome = ("cleaned", "complete") if settled else ("source-closed", "deferred")
            self.run("cleanup", target, phase, configured=True,
                     connections=2 if settled else 1, outcome=outcome)
            events = "(:status :cleanup :cleanup :release)" if settled else "(:status :abort)"
            self.control("(progn (pg::native-cancellation-check-source-events '" + events + ") (values))")
            self.audit(index, aborted=not settled)
            self.audit_configured_allocation(index, "released" if settled else "aborted")
            before = self.configured_startup_fence(target, roots, before, phase)
            saved = facts(target / "physical-reseed.follow")
            if settled:
                self.configured_reuse_refused(index, target, roots, before)
            self.stop()
            self.run("cleanup", target, phase, configured=True, connections=0, outcome=outcome)
            assert facts(target / "physical-reseed.follow") == saved
            self.start()
            self.run("cleanup", target, phase, configured=True, connections=0, outcome=outcome)
            assert facts(target / "physical-reseed.follow") == saved
            assert self.configured_inventory(roots, target) == before
            self.audit(index, aborted=not settled)
            self.audit_configured_allocation(index, "released" if settled else "aborted")
            if settled:
                self.configured_reuse_refused(index, target, roots, before)
            self.result["checks"].append(label)
            self.result.setdefault("timings", []).append(
                {"case": label, "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})

    def configured_effects(self):
        for index, kind in enumerate(("observer", "issuer", "release"), 6901):
            started = time.monotonic()
            label = "configured-lost-" + kind
            target, roots, before = self.prepare_configured(index, label, settled=True)
            before = self.configured_startup_fence(target, roots, before, "inert")
            self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
            self.run("begin", target, "pending", configured=True, connections=0)
            pending = facts(target / "physical-reseed.follow")
            phase = "pending" if kind == "observer" else "source-closed"
            # Discard the reply in the target after the real source command returns.
            self.run("lose-reply", target, phase, configured=True,
                     connections=1 if phase == "pending" else 2, lose_reply=kind)
            if phase == "pending":
                assert facts(target / "physical-reseed.follow") == pending
            assert not os.path.lexists(target / "physical-reseed.follow.tmp")
            events = {"observer": "(:status :cleanup)",
                      "issuer": "(:status :cleanup :cleanup)",
                      "release": "(:status :cleanup :cleanup :release)"}[kind]
            self.control("(progn (pg::native-cancellation-check-source-events '" + events + ") (values))")
            if kind == "release":
                self.audit(index)
            else:
                self.audit_configured_selection(index)
            before = self.configured_startup_fence(target, roots, before, phase)
            interrupted = facts(target / "physical-reseed.follow")
            self.stop()
            self.start()
            assert facts(target / "physical-reseed.follow") == interrupted
            if kind == "release":
                self.audit(index)
            else:
                self.audit_configured_selection(index)
            self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
            self.run("cleanup", target, "cleaned", configured=True,
                     connections=2 if phase == "pending" else 1)
            events = "(:status :cleanup :cleanup :release)" if phase == "pending" else "(:cleanup :release)"
            self.control("(progn (pg::native-cancellation-check-source-events '" + events + ") (values))")
            self.audit(index)
            before = self.configured_startup_fence(target, roots, before, "cleaned")
            saved = facts(target / "physical-reseed.follow")
            self.stop()
            self.run("cleanup", target, "cleaned", configured=True, connections=0)
            assert facts(target / "physical-reseed.follow") == saved
            self.start()
            self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
            self.run("cleanup", target, "cleaned", configured=True, connections=0)
            self.control("(progn (pg::native-cancellation-check-source-events nil) (values))")
            assert facts(target / "physical-reseed.follow") == saved
            assert self.configured_inventory(roots, target) == before
            self.audit(index)
            self.result["checks"].append(label)
            self.result.setdefault("timings", []).append(
                {"case": label, "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})

    def configured_crash_case(self, index, phase, cut):
        started = time.monotonic()
        label = "configured-crash-" + phase + "-" + cut
        target, roots, before = self.prepare_configured(index, label, settled=True)
        before = self.configured_startup_fence(target, roots, before, "inert")
        if phase != "pending":
            self.run("begin", target, "pending", configured=True, connections=0)
        self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
        evidence = self.output / (label + "-evidence")
        crashed = self.run("begin" if phase == "pending" else "cleanup", target, phase,
                           configured=True, cut=cut, cut_phase=phase, evidence=evidence)
        predecessor = Path(str(evidence) + ".before").read_bytes()
        candidate = Path(str(evidence) + ".after").read_bytes()
        final = target / "physical-reseed.follow"
        temporary = target / "physical-reseed.follow.tmp"
        renamed = cut == "rename"
        partial = cut in ("create", "write")
        saved_final = facts(final)
        saved_temporary = facts(temporary) if os.path.lexists(temporary) else None
        assert saved_final[-1] == (candidate if renamed else predecessor)
        if renamed:
            assert saved_temporary is None
        elif partial:
            length = len(saved_temporary[-1])
            assert length == (0 if cut == "create" else 257)
            assert length < len(candidate) and saved_temporary[-1] == candidate[:length]
        else:
            assert saved_temporary[-1] == candidate
        events = {"pending": "nil", "source-closed": "(:status :cleanup)",
                  "cleaned": "(:status :cleanup :cleanup :release)"}[phase]
        self.control("(progn (pg::native-cancellation-check-source-events '" + events + ") (values))")
        if phase == "cleaned":
            self.audit(index)
        else:
            self.audit_configured_selection(index)
        self.stop()
        self.start()
        assert facts(final) == saved_final
        assert (facts(temporary) if os.path.lexists(temporary) else None) == saved_temporary
        self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
        diagnostic = {"create": "target follow journal is not a bounded private single-link regular file",
                      "write": "physical backup lease checksum mismatch",
                      "rename": "retirement requires completed activation",
                      "fsync": "observed retirement prefix disagrees with recorded progress"}[cut]
        stable_phase = "cleaned" if renamed else {"pending": "inert", "source-closed": "pending",
                                                   "cleaned": "source-closed"}[phase]
        before = self.configured_startup_fence(
            target, roots, before, stable_phase, temporary=saved_temporary, diagnostic=diagnostic)
        modes = []
        if not renamed:
            modes.append(("none", "configured cancellation temporary requires explicit reconciliation"))
            if partial:
                cause = ("target follow journal is not a bounded private single-link regular file" if cut == "create"
                         else "physical backup lease checksum mismatch")
                modes.extend((("local", cause), ("cleaned", cause)))
            elif phase == "cleaned":
                modes.append(("local", "cleanup publication requires the exact source-closed successor"))
        recovery_pids = []
        # Repeat partial-temporary refusals through fresh recovery processes.
        for _ in range(2 if partial else 1):
            for mode, cause in modes:
                recovery_pids.append(self.run(
                    "factory-refused", target, stable_phase, configured=True,
                    recovery=mode, connections=0, refusal_cause=cause))
                assert facts(final) == saved_final and facts(temporary) == saved_temporary
                assert self.configured_inventory(roots, target) == before
        self.control("(progn (pg::native-cancellation-check-source-events nil) (values))")
        if not partial:
            mode = "cleaned" if phase == "cleaned" else "local"
            recovery_pids.append(self.run("factory-recover", target, phase, configured=True,
                                          recovery=mode, connections=1 if phase == "cleaned" and not renamed else 0))
            assert facts(final)[-1] == candidate and not os.path.lexists(temporary)
            if saved_temporary is not None:
                promoted = facts(final)
                assert promoted[:8] == saved_temporary[:8] and promoted[9:] == saved_temporary[9:]
            else:
                assert facts(final) == saved_final
            events = "(:cleanup :release)" if phase == "cleaned" and not renamed else "nil"
            self.control("(progn (pg::native-cancellation-check-source-events '" + events + ") (values))")
            if phase != "cleaned":
                self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
                self.run("cleanup", target, "cleaned", configured=True,
                         connections=2 if phase == "pending" else 1)
                events = "(:status :cleanup :cleanup :release)" if phase == "pending" else "(:cleanup :release)"
                self.control("(progn (pg::native-cancellation-check-source-events '" + events + ") (values))")
            self.audit(index)
            before = self.configured_startup_fence(target, roots, before, "cleaned")
            saved = facts(final)
            self.stop()
            self.run("cleanup", target, "cleaned", configured=True, connections=0)
            assert facts(final) == saved
            self.start()
            self.control("(progn (pg::native-cancellation-reset-source-events) (values))")
            self.run("cleanup", target, "cleaned", configured=True, connections=0)
            self.control("(progn (pg::native-cancellation-check-source-events nil) (values))")
            assert facts(final) == saved
        assert len(set([crashed, *recovery_pids])) == 1 + len(recovery_pids)
        assert self.configured_inventory(roots, target) == before
        self.audit(index)
        self.result["checks"].append(label)
        self.result.setdefault("timings", []).append(
            {"case": label, "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})

    def configured_crash(self):
        cases = (("pending", "fsync"), ("source-closed", "fsync"),
                 ("cleaned", "create"), ("cleaned", "write"),
                 ("cleaned", "fsync"), ("cleaned", "rename"))
        for index, (phase, cut) in enumerate(cases, 7001):
            self.configured_crash_case(index, phase, cut)

    def abort(self):
        for index, zero in enumerate((True, False), 6101):
            label = "abort-zero" if zero else "abort-generation-debt"
            target, fixed, before = self.install(index, label, selected=not zero)
            if not zero:
                self.run("prove", target)
            self.run("begin", target, "pending", connections=0)
            phase, outcome = ("cleaned", "complete") if zero else ("source-closed", "deferred")
            self.run("cleanup", target, phase, connections=2 if zero else 1, outcome=outcome)
            saved = facts(target / "physical-reseed.follow")
            self.run("cleanup", target, phase, connections=0, outcome=outcome)
            self.audit(index, aborted=True)
            self.stop()
            self.start()
            self.run("cleanup", target, phase, connections=0, outcome=outcome)
            assert facts(target / "physical-reseed.follow") == saved
            assert [facts(path) for path in fixed] == before
            self.audit(index, aborted=True)
            self.result["checks"].append(label)

    def crash(self):
        cases = ((allocated, cut) for cut in ("create", "write", "fsync", "rename")
                 for allocated in (True, False))
        for index, (allocated, cut) in enumerate(cases, 6201):
            label = "crash-" + ("allocated-" if allocated else "legacy-") + cut
            target, fixed, before = self.install(index, label, selected=True)
            self.handoff(index, target, allocated)
            self.run("begin", target, "pending", connections=0)
            self.run("cleanup", target, cut=cut)
            temporary = target / "physical-reseed.follow.tmp"
            final = target / "physical-reseed.follow"
            if cut == "rename":
                assert not os.path.lexists(temporary)
                saved = facts(final)
            else:
                saved, predecessor = facts(temporary), facts(final)
                assert saved[-1] != predecessor[-1]
                if cut in ("create", "write"):
                    assert len(saved[-1]) == (0 if cut == "create" else 257)
            self.stop()
            self.start()
            if cut == "rename":
                self.run("cleanup", target, "cleaned", connections=0)
                assert facts(final) == saved
            else:
                assert facts(temporary) == saved and facts(final) == predecessor
                if cut in ("create", "write"):
                    cause = ("target follow journal is not a bounded private single-link regular file"
                             if cut == "create" else "physical backup lease checksum mismatch")
                    for retry in range(2):
                        self.run("recover-refused", target, "source-closed", connections=0,
                                 refusal_cause=cause)
                        assert facts(temporary) == saved and facts(final) == predecessor
                else:
                    self.run("recover", target, "cleaned", connections=1)
                    after = facts(final)
                    # Renaming the captured inode can change ctime.
                    assert after[:8] == saved[:8] and after[9:] == saved[9:]
            if cut not in ("create", "write"):
                self.run("cleanup", target, "cleaned", connections=0)
            assert [facts(path) for path in fixed] == before
            self.audit(index)
            self.result["checks"].append(label)

    def effects(self, kill=False):
        cases = ("observer", "finalization") if kill else ("abort", "observer", "issuer", "release")
        for index, kind in enumerate(cases, 6401 if kill else 6301):
            zero = kind == "abort"
            label = ("early-" if kill else "lost-") + kind
            target, fixed, before = self.install(index, label, selected=not zero)
            if not zero:
                self.handoff(index, target)
            self.run("begin", target, "pending", connections=0)
            pending = facts(target / "physical-reseed.follow")
            phase = "pending" if kind in ("abort", "observer") else "source-closed"
            connections = None if kill else (1 if phase == "pending" else 2)
            self.run("lose-reply", target, phase, connections=connections,
                     lose_reply=kind, kill_effect=kill)
            if phase == "pending":
                assert facts(target / "physical-reseed.follow") == pending
            assert not os.path.lexists(target / "physical-reseed.follow.tmp")
            self.audit(index, aborted=zero, selected_present=kind in ("observer", "issuer", "finalization"))
            failed = facts(target / "physical-reseed.follow")
            assert [facts(path) for path in fixed] == before
            self.stop()
            self.start()
            assert facts(target / "physical-reseed.follow") == failed
            self.run("cleanup", target, "cleaned", connections=2 if phase == "pending" else 1)
            saved = facts(target / "physical-reseed.follow")
            self.run("cleanup", target, "cleaned", connections=0)
            self.audit(index, aborted=zero)
            self.stop()
            self.start()
            self.run("cleanup", target, "cleaned", connections=0)
            assert facts(target / "physical-reseed.follow") == saved
            assert [facts(path) for path in fixed] == before
            self.audit(index, aborted=zero)
            self.result["checks"].append(label)

    def reuse(self):
        for index, (allocated, absent) in enumerate(
                ((True, False), (True, True), (False, False), (False, True)), 6501):
            label = "reuse-" + ("allocated" if allocated else "legacy") + ("-absent" if absent else "-lost")
            target, fixed, before = self.install(index, label, selected=True)
            self.handoff(index, target, allocated, absent)
            self.run("begin", target, "pending", connections=0)
            self.run("cleanup", target, "cleaned", connections=2)
            saved = facts(target / "physical-reseed.follow")
            for attempt in range(2):
                self.control("(progn (pg::native-cancellation-observe-reuse) (values))")
                self.run("reuse-refused", target, "cleaned", connections=1)
                self.control("(progn (pg::native-cancellation-check-reuse-witness " +
                             lisp_string("native_target_follow_" + str(index)) + " " +
                             ("t" if allocated else "nil") + ") (values))")
                self.audit(index)
                assert facts(target / "physical-reseed.follow") == saved
                if attempt == 0:
                    self.stop()
                    self.start()
            assert [facts(path) for path in fixed] == before
            self.result["checks"].append(label)

    def access(self, authentication=False):
        for index, zero in enumerate((True, False), 6701 if authentication else 6601):
            label = ("auth-" if authentication else "offline-") + ("pending" if zero else "issuer")
            target, fixed, before = self.install(index, label, selected=not zero)
            if not zero:
                self.handoff(index, target)
            self.run("begin", target, "pending", connections=0)
            phase = "pending" if zero else "source-closed"
            if not zero:
                self.run("lose-reply", target, phase, connections=2, lose_reply="issuer")
                self.audit(index, selected_present=True)
            saved = facts(target / "physical-reseed.follow")
            hba = self.server.data / "pg_hba.conf"
            original_hba = hba.read_bytes()
            self.stop()
            try:
                if authentication:
                    hba.write_bytes(b"host replication postgres 127.0.0.1/32 reject\n" + original_hba)
                    self.start()
                    self.control('(progn (assert (eq :reject (pg::hba-match-rules '
                                 '(pg::authentication-policy-rules (pg::authentication-capture-policy)) '
                                 '"postgres" "replication" #(127 0 0 1) :replication :physical))) (values))')
                    connection = wire.Connection(self.server.port)
                    try:
                        assert connection.query("SELECT 1") == [["1"]]
                    finally:
                        connection.close()
                for retry in range(2):
                    self.run("cleanup-unavailable", target, phase, connections=1)
                    assert facts(target / "physical-reseed.follow") == saved
                    assert not os.path.lexists(target / "physical-reseed.follow.tmp")
                    assert [facts(path) for path in fixed] == before
            finally:
                self.stop()
                if authentication:
                    hba.write_bytes(original_hba)
            self.start()
            self.run("cleanup", target, "cleaned", connections=2 if zero else 1)
            saved = facts(target / "physical-reseed.follow")
            self.run("cleanup", target, "cleaned", connections=0)
            self.audit(index, aborted=zero)
            self.stop()
            self.start()
            self.run("cleanup", target, "cleaned", connections=0)
            assert facts(target / "physical-reseed.follow") == saved
            assert [facts(path) for path in fixed] == before
            self.audit(index, aborted=zero)
            self.result["checks"].append(label)

    def execute(self, suites):
        started = time.monotonic()
        try:
            _, output = self.process("resolve-helper", [str(LISP / "bin/pg-libpq-helper-path")], timeout=70)
            self.helper = output.decode().splitlines()[-1]
            assert Path(self.helper).is_file() and os.access(self.helper, os.X_OK)
            self.start()
            connection = wire.Connection(self.server.port)
            try:
                connection.query("CREATE TABLE native_cancellation_items(id int)")
                connection.query("INSERT INTO native_cancellation_items VALUES(17), (29)")
            finally:
                connection.close()
            groups = {"selected": self.selected, "abort": self.abort, "crash": self.crash,
                      "lost": self.effects, "early": lambda: self.effects(kill=True),
                      "reuse": self.reuse, "offline": self.access,
                        "auth": lambda: self.access(authentication=True), "configured": self.configured,
                        "configured-lost": self.configured_effects,
                        "configured-crash": self.configured_crash}
            for name, function in groups.items():
                if "all" in suites or name in suites:
                    suite_started = time.monotonic()
                    complete = False
                    try:
                        function()
                        complete = True
                    finally:
                        self.result.setdefault("timings", []).append(
                            {"suite": name, "elapsed_seconds": time.monotonic() - suite_started,
                             "outcome": "pass" if complete else "fail"})
            connection = wire.Connection(self.server.port)
            try:
                assert connection.query("SELECT id FROM native_cancellation_items ORDER BY id") == [["17"], ["29"]]
                connection.query("INSERT INTO native_cancellation_items VALUES(41)")
            finally:
                connection.close()
            self.result["complete"] = True
        finally:
            self.stop()
            self.result["elapsed_seconds"] = time.monotonic() - started
            processes = self.result["processes"]
            self.result["all_recorded_pids_absent"] = all(
                not Path("/proc", str(item["pid"])).exists() for item in processes)
            self.result["all_expected_exits"] = all(
                item["exit"] == item["expected_exit"] for item in processes)
            (self.output / "result.json").write_text(json.dumps(self.result, indent=2) + "\n")
        assert self.result["all_recorded_pids_absent"] and self.result["all_expected_exits"]
        print(self.output / "result.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", nargs="+", default=["all"],
                          choices=("all", "selected", "abort", "crash", "lost", "early", "reuse", "offline", "auth", "configured", "configured-lost", "configured-crash"))
    parser.add_argument("--output", type=Path, help="new directory for retained logs and process evidence")
    arguments = parser.parse_args()
    Acceptance(arguments.output).execute(arguments.suite)


if __name__ == "__main__":
    main()

