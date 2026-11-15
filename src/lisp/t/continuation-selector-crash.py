#!/usr/bin/env python3
"""Kill an owned initial-selector publisher, then recover the same PGDATA."""

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def slot_manifest(data):
    root = data / "pg_replslot"
    result = {}
    for entry in root.iterdir():
        if entry.name.startswith("."):
            continue
        assert entry.is_dir() and not entry.is_symlink()
        for path in [entry, *entry.rglob("*")]:
            assert not path.is_symlink()
            stat = path.stat()
            result[str(path.relative_to(root))] = (
                stat.st_dev, stat.st_ino, stat.st_mode, stat.st_uid, stat.st_nlink,
                stat.st_ctime_ns, stat.st_mtime_ns, digest(path) if path.is_file() else None,
            )
    return result


def main():
    if os.environ.get("PG_NATIVE_HOST_DISPOSABLE") != "1":
        raise RuntimeError("requires an explicitly disposable native host fixture")
    script = Path(sys.argv[1]).resolve()
    data = Path(os.environ["PG_NATIVE_PROOF_DATA"]).resolve()
    logs = Path(os.environ["PG_LISP_TEST_LOG_DIR"]).resolve()
    if not logs.is_relative_to(Path("/dev/shm/pg-lisp-tests-1000")):
        raise RuntimeError("native crash logs require the private RAM workspace")
    proof = data / "logical-slot-copy.source-proof"
    proof_identity = (proof.stat().st_dev, proof.stat().st_ino, digest(proof))
    root_identity = (data.stat().st_dev, data.stat().st_ino)
    selected = data / "pg_logical_continuation" / "selector"
    results = []
    start = time.monotonic()

    def run(label, cut=None, adversarial=False):
        env = dict(os.environ)
        env.pop("PG_NATIVE_SELECTOR_CUT", None)
        env.pop("PG_NATIVE_SELECTOR_ADVERSARIAL", None)
        env["PG_NATIVE_SELECTOR_RECOVER"] = "1"
        if cut:
            env["PG_NATIVE_SELECTOR_CUT"] = cut
        if adversarial:
            env["PG_NATIVE_SELECTOR_ADVERSARIAL"] = "1"
        log = logs / (label + ".log")
        began = time.monotonic()
        with log.open("wb") as output:
            completed = subprocess.run(
                [env.get("SBCL", "sbcl"), "--script", str(script)],
                env=env, stdout=output, stderr=subprocess.STDOUT, timeout=180,
            )
        text = log.read_text(errors="replace")
        if cut:
            assert completed.returncode in (-signal.SIGKILL, 128 + signal.SIGKILL)
            assert "NATIVE-SELECTOR-CUT " + cut in text
        else:
            assert completed.returncode == 0, str(log)
            assert "NATIVE-SELECTOR-RECOVER PASS" in text
            if adversarial:
                assert "NATIVE-SELECTOR-ADVERSARIAL PASS cases=7" in text
        assert "dropped=0 historical-rows=2" in text
        assert (proof.stat().st_dev, proof.stat().st_ino, digest(proof)) == proof_identity
        assert (data.stat().st_dev, data.stat().st_ino) == root_identity
        assert not [p for p in (data / "pg_replslot").iterdir() if not p.name.startswith(".")]
        result = {"label": label, "cut": cut, "exit_code": completed.returncode,
                  "wall_seconds": time.monotonic() - began, "log": str(log)}
        results.append(result)
        print("CONTINUATION-SELECTOR-CHILD " + json.dumps(result), flush=True)

    run("initial-recovery")
    stable_digest = digest(selected)
    for cut in ("after-temp-write", "after-temp-sync", "before-install",
                "after-install", "after-directory-sync", "after-restoration-sync"):
        run("cut-" + cut, cut)
        names = sorted(p.name for p in selected.parent.iterdir())
        assert names in (["selector"], ["selector.tmp"]), names
        survivor = selected.parent / names[0]
        identity = (survivor.stat().st_dev, survivor.stat().st_ino)
        run("recover-" + cut)
        assert sorted(p.name for p in selected.parent.iterdir()) == ["selector"]
        assert (selected.stat().st_dev, selected.stat().st_ino) == identity
        assert digest(selected) == stable_digest
    run("adversarial-recovery", adversarial=True)
    assert digest(selected) == stable_digest
    if os.environ.get("PG_NATIVE_ACTIVE_PROOF_DATA"):
        active = Path(os.environ["PG_NATIVE_ACTIVE_PROOF_DATA"]).resolve()
        before = slot_manifest(active)
        assert before
        env = dict(os.environ, PG_NATIVE_PROOF_DATA=str(active), PG_NATIVE_SELECTOR_REQUIRE_ACTIVE="1")
        env.pop("PG_NATIVE_SELECTOR_CUT", None)
        env.pop("PG_NATIVE_SELECTOR_RECOVER", None)
        env.pop("PG_NATIVE_SELECTOR_ADVERSARIAL", None)
        log = logs / "active-refusal.log"
        began = time.monotonic()
        with log.open("wb") as output:
            completed = subprocess.run([env.get("SBCL", "sbcl"), "--script", str(script)],
                                       env=env, stdout=output, stderr=subprocess.STDOUT, timeout=180)
        assert completed.returncode == 0, str(log)
        assert "NATIVE-SELECTOR-ACTIVE-REFUSAL PASS" in log.read_text(errors="replace")
        after = slot_manifest(active)
        assert before == after
        results.append({"label": "active-refusal", "exit_code": 0,
                        "wall_seconds": time.monotonic() - began, "log": str(log)})
    summary = {"exit_code": 0, "cuts": 6, "runs": results,
               "wall_seconds": time.monotonic() - start,
               "stable_sha256": stable_digest, "original_pgdata_identity": root_identity,
               "original_proof_identity": proof_identity,
                "limits": "Empty selector crash matrix; optional active selector proof retains all saved P7 rows. GC and admission remain fenced."}
    (logs / "selector-crash-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("CONTINUATION-SELECTOR-CRASH PASS " + json.dumps({k: v for k, v in summary.items() if k != "runs"}), flush=True)


if __name__ == "__main__":
    main()
