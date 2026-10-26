import pathlib
import runpy
import shutil
import struct
import subprocess
import sys
import zlib

helpers = runpy.run_path(str(pathlib.Path(__file__).with_name("control-data-inspect.py")))
storage = helpers["storage"]
seed, artifacts = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
utility = pathlib.Path(__file__).resolve().parents[2] / "bin/pg_controldata"
artifacts.mkdir(mode=0o700)
sentinel = artifacts / "sentinel"
sentinel.write_bytes(b"control inspection must not write here\n")
for kind in ("generation-symlink", "generation-fifo", "generation-corrupt", "pointer-fifo",
             "manifest-fifo", "control-symlink", "control-corrupt", "control-state",
             "recovery-corrupt", "recovery-missing"):
    root = artifacts / kind
    shutil.copytree(seed, root)
    if kind.startswith("generation"):
        pointer = (root / "physical-restartpoint.control").read_bytes()
        lsn, digest = struct.unpack_from("<QQ", pointer, 8)
        path = root / "pg_physical_restartpoints" / f"{lsn:016X}-{digest:016X}.rp"
    elif kind == "pointer-fifo":
        path = root / "physical-restartpoint.control"
    elif kind == "manifest-fifo":
        path = root / "physical-backup.manifest"
    elif kind.startswith("control"):
        path = root / "global/pg_wal_control"
    else:
        path = root / "physical-recovery.control"
    if kind.endswith("symlink"):
        path.unlink()
        path.symlink_to(sentinel.resolve())
    elif kind.endswith("fifo"):
        path.unlink()
        import os
        os.mkfifo(path)
    elif kind.endswith("missing"):
        path.unlink()
    elif kind == "control-state":
        fields = path.read_bytes().split()
        prefix = b" ".join(fields[:7] + [b"42", b"7", b"0"])
        path.write_bytes(prefix + f" {zlib.crc32(prefix):08X}\n".encode())
    else:
        data = bytearray(path.read_bytes())
        data[len(data) // 2] ^= 1
        path.write_bytes(data)
    before = storage(root)
    result = subprocess.run([str(utility), str(root)], capture_output=True, timeout=10)
    assert result.returncode == 1 and b"pg_controldata:" in result.stderr, (kind, result)
    assert storage(root) == before, f"{kind}: inspection changed storage"
    assert sentinel.read_bytes() == b"control inspection must not write here\n"
    print(f"control-data-reject: {kind} unchanged PASS")
