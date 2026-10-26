import os
import pathlib
import subprocess
import sys
import tempfile

root = pathlib.Path(sys.argv[1]).resolve()
utility = pathlib.Path(__file__).resolve().parents[2] / "bin/pg_ctl"
signal_file = root / "promote"
with tempfile.TemporaryDirectory(prefix="pg-ctl-reject-", dir=root.parent) as directory:
    scratch = pathlib.Path(directory)
    sentinel = scratch / "sentinel"
    sentinel.write_bytes(b"preserve this inode\n")
    for kind in ("symlink", "hardlink", "fifo"):
        if kind == "symlink":
            signal_file.symlink_to(sentinel)
        elif kind == "hardlink":
            os.link(sentinel, signal_file)
        else:
            os.mkfifo(signal_file)
        try:
            result = subprocess.run([str(utility), "promote", "-D", str(root), "-W"],
                                    capture_output=True, timeout=5)
            assert result.returncode == 1, (kind, result)
            assert sentinel.read_bytes() == b"preserve this inode\n"
        finally:
            signal_file.unlink()
    metadata = (root / "postmaster.pid").read_text().splitlines()
    metadata[0], metadata[1] = sys.argv[2], str(scratch)
    (scratch / "postmaster.pid").write_text("\n".join(metadata) + "\n")
    (scratch / ".pg_lisp.lock").touch()
    result = subprocess.run([str(utility), "promote", "-D", str(scratch), "-W"],
                            capture_output=True, timeout=5)
    assert result.returncode == 1 and b"PID does not own" in result.stderr, result
    assert not (scratch / "promote").exists()
print("pg-ctl-promotion-reject: special signal files and unrelated live PID reject without writes PASS")
