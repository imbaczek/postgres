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


def snapshot_pending(target, source, snapshot):
    root = pathlib.Path(target)
    lsn, digest = main(target, source)
    control = (root / "physical-recovery.control").read_bytes()
    assert len(control) == 88 and control[:8] == b"PGRSTAT4"
    checked_crc(control, 84)
    assert struct.unpack_from("<I", control, 8)[0] & 0x7FFFFFFF == 3
    assert struct.unpack_from("<I", control, 12)[0] == 1
    assert struct.unpack_from("<Q", control, 40)[0] == lsn
    assert struct.unpack_from("<Q", control, 76)[0] == digest
    assert struct.unpack_from("<I", control, 68)[0] == 2
    fields = (root / "global" / "pg_wal_control").read_bytes().split()
    assert fields[0] == b"PGWAL6"
    assert zlib.crc32(b" ".join(fields[:-1])) == int(fields[-1], 16)
    assert struct.unpack_from("<I", control, 72)[0] == {b"legacy": 0, b"timeline": 1}[fields[5]]
    pathlib.Path(snapshot).write_text(json.dumps(storage_snapshot(root), sort_keys=True))
    print(f"pending-intent: actual publisher bound fork={lsn} generation={digest:016X} PASS")


def check_child_prefix(target, source, expected):
    root = pathlib.Path(target)
    lsn, digest = main(target, source)
    control = (root / "physical-recovery.control").read_bytes()
    checked_crc(control, 84)
    assert control[:8] == b"PGRSTAT4" and len(control) == 88
    assert struct.unpack_from("<I", control, 8)[0] & 0x7FFFFFFF == 3
    assert struct.unpack_from("<Q", control, 40)[0] == lsn
    assert struct.unpack_from("<Q", control, 76)[0] == digest
    child = struct.unpack_from("<I", control, 68)[0]
    assert not (root / "pg_wal" / f"{child:08X}.history").exists()
    segment = lsn // (16 * 1024 * 1024)
    path = root / "pg_wal" / f"{child:08X}{segment // 256:08X}{segment % 256:08X}.wal"
    if expected == "absent":
        assert not path.exists(), "child should be durably unlinked"
    elif expected == "stale":
        assert path.read_bytes() == b"pending-stale-child"
    else:
        assert expected == "installed"
        image = (root / "pg_physical_restartpoints" / f"{lsn:016X}-{digest:016X}.rp").read_bytes()
        length = struct.unpack_from("<I", image, 20)[0]
        prefix = image[-length:] if length else b""
        assert path.read_bytes() == prefix, "child differs from authenticated prefix"
    print(f"promotion-child-prefix: {expected} PASS")


def check_promotion_eor(target, source, expected):
    root = pathlib.Path(target)
    fork, digest = main(target, source)
    control = (root / "physical-recovery.control").read_bytes()
    assert len(control) == 88 and control[:8] == b"PGRSTAT4"
    checked_crc(control, 84)
    assert struct.unpack_from("<I", control, 8)[0] & 0x7FFFFFFF == 3
    parent = struct.unpack_from("<I", control, 12)[0]
    child = struct.unpack_from("<I", control, 68)[0]
    assert struct.unpack_from("<Q", control, 40)[0] == fork
    assert struct.unpack_from("<Q", control, 76)[0] == digest
    image = (root / "pg_physical_restartpoints" / f"{fork:016X}-{digest:016X}.rp").read_bytes()
    length = struct.unpack_from("<I", image, 20)[0]
    prefix = image[-length:] if length else b""
    size = 16 * 1024 * 1024
    start = fork if fork % size + 56 <= size else (fork // size + 1) * size
    def child_path(lsn):
        segment = lsn // size
        return root / "pg_wal" / f"{child:08X}{segment // 256:08X}{segment % 256:08X}.wal"
    history = root / "pg_wal" / f"{child:08X}.history"
    if expected == "no-history":
        assert not history.exists()
    else:
        assert history.read_bytes() == f"{parent}\t{fork >> 32:X}/{fork & 0xFFFFFFFF:08X}\tpg-lisp promotion\n".encode()
    first = child_path(fork).read_bytes()
    assert first[:length] == prefix
    if expected != "eor":
        assert first == prefix
        if start != fork:
            assert child_path(start).read_bytes() == b""
    else:
        assert expected == "eor"
        if start != fork:
            assert first == prefix
        data = child_path(start).read_bytes()
        record = data[start % size:]
        assert len(record) == 56 and record[:4] == b"PGWL"
        assert struct.unpack_from("<HHIQQ", record, 4) == (7, 13, 56, start, 0)
        checked_crc(record, 28)
        timestamp = (struct.unpack_from("<q", control, 60)[0] - 946684800) * 1000000
        assert struct.unpack_from("<qIIII", record, 32) == (timestamp, child, parent, 2, 0)
    print(f"promotion-history-eor: {expected} PASS")


if __name__ == "__main__":
    if sys.argv[1] == "--pending-fixture":
        prepare_pending(*sys.argv[2:])
    elif sys.argv[1] == "--verify-pending":
        verify_pending(*sys.argv[2:])
    elif sys.argv[1] == "--snapshot-pending":
        snapshot_pending(*sys.argv[2:])
    elif sys.argv[1] == "--check-child-prefix":
        check_child_prefix(*sys.argv[2:])
    elif sys.argv[1] == "--check-promotion-eor":
        check_promotion_eor(*sys.argv[2:])
    else:
        main(*sys.argv[1:])
