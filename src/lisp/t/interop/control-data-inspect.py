import hashlib
import os
import pathlib
import stat
import struct
import subprocess
import sys
import zlib

from patroni_native_node import native_node

STATES = ("starting up", "shut down", "shut down in recovery", "shutting down",
          "in crash recovery", "in archive recovery", "in production")


def storage(root):
    result = {}
    for path in root.rglob("*"):
        info = path.lstat()
        if stat.S_ISREG(info.st_mode):
            with path.open("rb") as stream:
                value = hashlib.file_digest(stream, "sha256").hexdigest()
        elif path.is_symlink():
            value = os.readlink(path)
        else:
            value = stat.S_IFMT(info.st_mode)
        result[str(path.relative_to(root))] = (stat.S_IMODE(info.st_mode), value)
    return result


def run(root, expected_state=None):
    utility = pathlib.Path(__file__).resolve().parents[2] / "bin/pg_controldata"
    result = subprocess.run([str(utility), str(root)], text=True, capture_output=True, timeout=10,
                            env={**os.environ, "LANG": "C", "LC_ALL": "C"})
    assert result.returncode == 0, result.stderr
    rows = dict(line.split(":", 1) for line in result.stdout.splitlines())
    rows = {key: value.strip() for key, value in rows.items()}
    fields = (root / "global/pg_wal_control").read_bytes().split()
    assert fields[0] == b"PGWAL6" and len(fields) in (8, 11, 17)
    assert zlib.crc32(b" ".join(fields[:-1])) == int(fields[-1], 16)
    if len(fields) in (11, 17) and not (root / "physical-recovery.control").exists():
        assert rows["Database system identifier"] == fields[7].decode()
        assert rows["Database cluster state"] == STATES[int(fields[8])]
        if fields[6] != b"-":
            snapshot = bytes.fromhex(fields[6].decode())
            start, end, redo = struct.unpack_from("<QQQ", snapshot, 8)
            assert int(fields[1]) == end and int(fields[2]) >= end
            assert rows["Latest checkpoint location"] == f"{start >> 32:X}/{start & 0xFFFFFFFF:08X}"
            assert rows["Latest checkpoint's REDO location"] == f"{redo >> 32:X}/{redo & 0xFFFFFFFF:08X}"
            assert int(rows["Latest checkpoint's TimeLineID"]) == struct.unpack_from("<I", snapshot, 64)[0]
            assert int(rows["Latest checkpoint's PrevTimeLineID"]) == struct.unpack_from("<I", snapshot, 68)[0]
            if int(fields[8]) == 1:
                assert struct.unpack_from("<Q", snapshot, 32)[0] == struct.unpack_from("<Q", snapshot, 72)[0]
    if expected_state:
        assert rows["Database cluster state"] == expected_state, rows
    assert rows["Bytes per WAL segment"] == "16777216" and rows["Database block size"] == "8192"
    assert "Data page checksum version" not in rows and "wal_log_hints setting" not in rows
    assert native_node(root).controldata() == rows, "Patroni parsed different control facts"
    print(f'control-data-inspect: {rows["Database cluster state"]} checkpoint={rows["Latest checkpoint location"]} PASS')
    return rows


if __name__ == "__main__":
    root = pathlib.Path(sys.argv[1])
    before = storage(root)
    run(root, sys.argv[2] if len(sys.argv) > 2 else None)
    assert storage(root) == before, "control inspection changed PGDATA"
