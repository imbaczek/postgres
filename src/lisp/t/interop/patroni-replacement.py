"""Exercise configured cancellation replacement through pinned Patroni follow."""
import argparse
import ctypes
import importlib.metadata
import importlib.util
from pathlib import Path
import time


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("replacement_reseed_driver", HERE / "patroni-reseed.py")
reseed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reseed)


class Acceptance(reseed.Acceptance):
    def __init__(self, output, phase):
        super().__init__(output)
        self.phase = phase
        self.label = "patroni-replacement-" + phase
        self.old_target = None

    def install(self, index, label, selected):
        return super().install(index, label, selected, replacement=self.old_target)

    def replacement_child(self, action, target, stage, *, aborted=False, expected=0):
        quote = reseed.cancellation.lisp_string
        registry = "(push #P" + quote(str(reseed.cancellation.LISP) + "/") + " asdf:*central-registry*)"
        cache = "(asdf:initialize-output-translations '(:output-translations (t " + quote(str(self.output / "cache") + "/") + ") :ignore-inherited-configuration))"
        arguments = [self.sbcl, "--noinform", "--no-sysinit", "--no-userinit", "--non-interactive",
                     "--eval", "(require :asdf)", "--eval", registry, "--eval", cache,
                     "--eval", '(asdf:load-system "pg")', "--load", str(HERE / "patroni-replacement-native.lisp")]
        form = "(pg::native-replacement-" + action
        form += " " + " ".join("#P" + quote(str(path) + "/") for path in (self.old_target, target, stage))
        form += " 7400 " + str(self.server.port) + " " + quote(self.helper)
        if action == "reopen-after-crash":
            form += " t" if aborted else " nil"
        _, output = self.process("replacement-" + action, arguments + ["--eval", form + ")"],
                                 timeout=180, expected=expected)
        if action == "reopen-after-crash":
            assert b"REPLACEMENT-CRASH-REOPEN-PASS" in output

    def capture_crash(self):
        started = time.monotonic()
        target = self.server.root / "replacement-crash-target"
        stage = self.server.root / "replacement-crash-stage"
        target.mkdir(mode=0o700)
        stage.mkdir(mode=0o700)
        (target / ".pg_lisp.lock").touch(mode=0o600)
        self.replacement_child("crash-after-journal", target, stage, expected=93)
        assert (target / "physical-reseed.attempt").is_file()
        assert not (target / "physical-reseed.attempt.tmp").exists()
        assert not list(stage.iterdir())
        self.replacement_child("reopen-after-crash", target, stage)
        aborted = reseed.cancellation.facts(target / "physical-reseed.attempt")
        self.replacement_child("reopen-after-crash", target, stage, aborted=True)
        assert reseed.cancellation.facts(target / "physical-reseed.attempt") == aborted
        assert not list(stage.iterdir())
        self.result["checks"].append("replacement-capture-journal-crash-and-cold-abort")
        self.result.setdefault("timings", []).append(
            {"case": "replacement-capture-crash", "elapsed_seconds": time.monotonic() - started,
             "outcome": "pass"})

    def configured(self):
        started = time.monotonic()
        old, roots, before = self.prepare_configured(
            7300, self.label + "-history", settled=self.phase != "source-closed")
        self.run("begin", old, "pending", configured=True, connections=0)
        if self.phase != "pending":
            cleaned = self.phase == "cleaned"
            self.run("cleanup", old, self.phase, configured=True,
                     connections=2 if cleaned else 1,
                     outcome="complete" if cleaned else "deferred")
        before = self.configured_startup_fence(old, roots, before, self.phase)
        journal = reseed.cancellation.facts(old / "physical-reseed.follow")
        self.snapshot("replacement-history-before", roots, old)
        self.old_target = old
        if self.phase == "pending":
            self.capture_crash()
            assert reseed.cancellation.facts(old / "physical-reseed.follow") == journal
            assert self.configured_inventory(roots, old) == before
        super().configured()
        assert reseed.cancellation.facts(old / "physical-reseed.follow") == journal
        assert self.configured_inventory(roots, old) == before
        before = self.configured_startup_fence(old, roots, before, self.phase)
        self.snapshot("replacement-history-after", roots, old)
        # The retained root still owns its original cleanup route and debt.
        deferred = self.phase == "source-closed"
        self.run("cleanup", old, "source-closed" if deferred else "cleaned", configured=True,
                 connections=2 if self.phase == "pending" else 0,
                 outcome="deferred" if deferred else "complete")
        assert self.configured_inventory(roots, old) == before
        if deferred:
            assert reseed.cancellation.facts(old / "physical-reseed.follow") == journal
        self.audit(7300, aborted=deferred)
        self.audit_configured_allocation(7300, "aborted" if deferred else "released")
        self.result["checks"].append("preserved-cancellation-" + self.phase)
        self.result.setdefault("timings", []).append(
            {"case": "replacement-with-history-" + self.phase,
             "elapsed_seconds": time.monotonic() - started, "outcome": "pass"})


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=("pending", "source-closed", "cleaned"), default="pending")
    arguments = parser.parse_args()
    Acceptance(arguments.output, arguments.phase).execute(["configured"])


if __name__ == "__main__":
    main()
