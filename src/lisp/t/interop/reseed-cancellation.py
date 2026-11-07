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
                     "expected_exit": expected, "log": str(log)})
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

    def stop(self):
        process = self.server.process
        self.server.stop()
        if process is not None:
            self.result["processes"].append(
                {"kind": "source", "pid": process.pid, "exit": process.returncode, "expected_exit": 0})

    def run(self, action, target, phase="none", connections=None, outcome="complete", *,
            cut=None, lose_reply=None, kill_effect=False, refusal_cause=None):
        killed = cut is not None or kill_effect
        assert not (killed and connections is not None)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("NATIVE_CANCELLATION_")}
        env.update({"NATIVE_CANCELLATION_TARGET": str(target),
                    "NATIVE_CANCELLATION_ACTION": action,
                    "NATIVE_CANCELLATION_PHASE": phase,
                    "NATIVE_CANCELLATION_OUTCOME": outcome,
                    "NATIVE_CANCELLATION_HELPER": self.helper})
        if connections is not None:
            env["NATIVE_CANCELLATION_EXPECT_CONNECTIONS"] = str(connections)
        if cut is not None:
            env["NATIVE_CANCELLATION_CUT"] = cut
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
             "lose_reply": lose_reply, "kill_effect": kill_effect, "refusal_cause": refusal_cause})
        if cut is not None:
            point = {"create": "AFTER-FOLLOW-TEMPORARY-CREATE", "write": "AFTER-FOLLOW-WRITE-CHUNK",
                     "fsync": "AFTER-FOLLOW-TEMPORARY-FSYNC", "rename": "AFTER-FOLLOW-RENAME"}[cut]
            assert f"NATIVE-CANCELLATION-CUT {pid} :{point} {target}/".encode() in output
        elif not killed:
            assert f"NATIVE-CANCELLATION-CHECKED {pid} {action}".encode() in output
        if lose_reply is not None:
            ordinal = 2 if lose_reply == "issuer" else 1
            assert f"NATIVE-CANCELLATION-DISCARDED {pid} {lose_reply} {ordinal}".encode() in output
        if action == "recover-refused":
            assert refusal_cause and refusal_cause.encode() in output
            assert f"NATIVE-CANCELLATION-REFUSED {pid} ".encode() in output
        if action == "reuse-refused":
            assert f"NATIVE-CANCELLATION-REUSE-REFUSED {pid}".encode() in output
        if connections is not None:
            assert f"NATIVE-CANCELLATION-CONNECTIONS {pid} {connections}".encode() in output
        if phase == "cleaned" and not killed:
            assert not os.path.lexists(target / "physical-reseed.follow.tmp")

    def install(self, index, label, selected):
        paths = [self.server.root / (label + "-" + suffix)
                 for suffix in ("target", "stage", "incoming", "quarantine")]
        for path in paths:
            path.mkdir(mode=0o700)
        target = paths[0]
        (target / ".pg_lisp.lock").touch(mode=0o600)
        (target / "opaque-old-data").write_bytes(b"original data\x00\xff")
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
                      "auth": lambda: self.access(authentication=True)}
            for name, function in groups.items():
                if "all" in suites or name in suites:
                    function()
            connection = wire.Connection(self.server.port)
            try:
                assert connection.query("SELECT id FROM native_cancellation_items ORDER BY id") == [["17"], ["29"]]
                connection.query("INSERT INTO native_cancellation_items VALUES(41)")
            finally:
                connection.close()
            self.result["complete"] = True
        finally:
            self.stop()
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
                        choices=("all", "selected", "abort", "crash", "lost", "early", "reuse", "offline", "auth"))
    parser.add_argument("--output", type=Path, help="new directory for retained logs and process evidence")
    arguments = parser.parse_args()
    Acceptance(arguments.output).execute(arguments.suite)


if __name__ == "__main__":
    main()

