#!/usr/bin/env python3
"""Independently compare a selected GEN2 prefix with stopped/idle source WAL."""

import pathlib
import hashlib
import json
import re
import struct
import sys
import zlib


def fnv(data):
    value = 0xCBF29CE484222325
    for byte in data:
        value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


def checked_crc(data, offset):
    saved = struct.unpack_from("<I", data, offset)[0]
    scratch = bytearray(data)
    struct.pack_into("<I", scratch, offset, 0)
    assert zlib.crc32(scratch) == saved, "CRC mismatch"


def field(text, name):
    tokens = re.findall(r'"(?:\\.|[^"\\])*"|[()]|[^\s()]+', text)
    depth, values = 0, []
    for index, token in enumerate(tokens):
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 1 and token.upper() == ":" + name.upper():
            values.append(tokens[index + 1])
    assert len(values) == 1, f"missing or ambiguous {name}"
    return int(values[0])


def main(target, source):
    target, source = pathlib.Path(target), pathlib.Path(source)
    pointer = (target / "physical-restartpoint.control").read_bytes()
    assert len(pointer) == 28 and pointer[:8] == b"PGRPPTR1"
    checked_crc(pointer, 24)
    lsn, digest = struct.unpack_from("<QQ", pointer, 8)
    name = f"{lsn:016X}-{digest:016X}.rp"
    image = (target / "pg_physical_restartpoints" / name).read_bytes()
    assert fnv(image) == digest, "generation hash mismatch"
    assert image[:8] == b"PGRPGEN2", "missing embedded-prefix generation"
    checked_crc(image, 24)
    text_length = struct.unpack_from("<I", image, 8)[0]
    catalog_length = struct.unpack_from("<Q", image, 12)[0]
    prefix_length = struct.unpack_from("<I", image, 20)[0]
    assert len(image) == 32 + text_length + catalog_length + prefix_length
    text = image[32 : 32 + text_length].decode("ascii")
    assert re.search(r"^\(\s*:VERSION\s+4\b", text, re.I)
    assert field(text, "LSN") == lsn
    timeline = field(text, "TIMELINE")
    prefix = image[32 + text_length + catalog_length :]
    assert prefix_length == lsn % (16 * 1024 * 1024)
    assert fnv(prefix) == field(text, "WAL-PREFIX-DIGEST")
    if prefix:
        segment = lsn // (16 * 1024 * 1024)
        legacy = source / "pg_wal" / f"{segment:016X}.wal"
        qualified = source / "pg_wal" / f"{timeline:08X}{segment // 256:08X}{segment % 256:08X}.wal"
        path = legacy if timeline == 1 and legacy.exists() else qualified
        with path.open("rb") as stream:
            assert stream.read(prefix_length) == prefix, "prefix differs from source WAL"
    print(f"restartpoint-prefix-inspect: PASS lsn={lsn} bytes={prefix_length}")
    return lsn, digest


def storage_snapshot(root):
    directories = {"base", "global", "pg_wal", "pg_physical_receive", "pg_physical_restartpoints"}
    names = {"PG_VERSION", "pg_xact", "physical-backup.manifest", "physical-recovery.control",
             "physical-restartpoint.control", "recovery.required"}
    result = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.parts[0] in directories or str(relative) in names:
            assert not path.is_symlink(), f"unexpected fixture symlink: {relative}"
            if path.is_file():
                result[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def prepare_pending(target, source, snapshot):
    root = pathlib.Path(target)
    lsn, digest = main(target, source)
    path = root / "physical-recovery.control"
    ordinary = path.read_bytes()
    assert len(ordinary) == 72 and ordinary[:8] == b"PGRSTAT3"
    checked_crc(ordinary, 68)
    phase = struct.unpack_from("<I", ordinary, 8)[0]
    assert phase & 0x7FFFFFFF == 2
    assert struct.unpack_from("<Q", ordinary, 40)[0] == lsn
    assert struct.unpack_from("<I", ordinary, 12)[0] == 1
    fields = (root / "global" / "pg_wal_control").read_bytes().split()
    assert fields[0] == b"PGWAL6"
    assert zlib.crc32(b" ".join(fields[:-1])) == int(fields[-1], 16)
    source_namespace = {b"legacy": 0, b"timeline": 1}[fields[5]]
    pending = bytearray(88)
    pending[:68] = ordinary[:68]
    pending[:8] = b"PGRSTAT4"
    struct.pack_into("<I", pending, 8, (phase & 0x80000000) | 3)
    struct.pack_into("<I", pending, 56, 5)  # DB_IN_ARCHIVE_RECOVERY
    struct.pack_into("<IIQ", pending, 68, 2, source_namespace, digest)
    struct.pack_into("<I", pending, 84, zlib.crc32(pending))
    path.write_bytes(pending)
    pathlib.Path(snapshot).write_text(json.dumps(storage_snapshot(root), sort_keys=True))
    print(f"pending-control-fixture: prepared fork={lsn} child=2 generation={digest:016X}")


def verify_pending(target, snapshot):
    expected = json.loads(pathlib.Path(snapshot).read_text())
    assert storage_snapshot(pathlib.Path(target)) == expected, "pending startup mutated storage"
    print(f"pending-control-fixture: unchanged {len(expected)} storage files PASS")


if __name__ == "__main__":
    if sys.argv[1] == "--pending-fixture":
        prepare_pending(*sys.argv[2:])
    elif sys.argv[1] == "--verify-pending":
        verify_pending(*sys.argv[2:])
    else:
        main(*sys.argv[1:])
