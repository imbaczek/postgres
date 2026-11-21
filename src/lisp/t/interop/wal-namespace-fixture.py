"""Re-encode a stopped, test-owned TLI-1 cluster as a legacy WAL fixture."""

import os
import runpy
import sys
from pathlib import Path

from crc32c_reference import crc32c


def make_legacy_fixture(root):
    pidfile = root / "postmaster.pid"
    if pidfile.exists():
        pid = int(pidfile.read_text().splitlines()[0])
        assert pid > 0
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise AssertionError("fixture conversion requires a stopped, owned test server")
    inspect = runpy.run_path(str(Path(__file__).with_name("wal-namespace-inspect.py")))["inspect"]
    facts, namespace = inspect(root)
    assert namespace == "timeline"
    control = root / "global/pg_wal_control"
    fields = control.read_bytes().rstrip(b"\n").split(b" ")
    assert fields[5] == b"timeline"
    paths = [
        (
            root / "pg_wal" / f"00000001{segment // 256:08X}{segment % 256:08X}.wal",
            root / "pg_wal" / f"{segment:016X}.wal",
        )
        for segment in map(int, facts["files"])
    ]
    assert all(not target.exists() and not target.is_symlink() for _, target in paths)
    for source, target in paths:
        source.rename(target)
    fields[5] = b"legacy"
    prefix = b" ".join(fields[:-1])
    control.write_bytes(prefix + f" {crc32c(prefix):08X}\n".encode("ascii"))
    assert inspect(root) == (facts, "legacy")


if __name__ == "__main__":
    make_legacy_fixture(Path(sys.argv[1]))
