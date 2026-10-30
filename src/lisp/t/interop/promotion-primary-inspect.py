#!/usr/bin/env python3
"""Independent child-checkpoint/control checks for stopped, task-owned fixtures."""
import hashlib
import json
import pathlib
import re
import runpy
import shutil
import struct
import sys
import zlib

SIZE = 16 * 1024 * 1024
ARCHIVE = runpy.run_path(str(pathlib.Path(__file__).with_name("restartpoint-prefix-inspect.py")))
CHECK_OPTIONAL_CONTROL = runpy.run_path(str(pathlib.Path(__file__).with_name("resource_control_fields.py")))["check_optional_fields"]


def crc(data, offset):
    scratch = bytearray(data)
    saved = struct.unpack_from("<I", scratch, offset)[0]
    struct.pack_into("<I", scratch, offset, 0)
    assert zlib.crc32(scratch) == saved


def control(root):
    fields = (root / "global/pg_wal_control").read_bytes().split()
    assert fields[0] == b"PGWAL6" and len(fields) in (8, 11, 15, 17, 21)
    assert zlib.crc32(b" ".join(fields[:-1])) == int(fields[-1], 16)
    CHECK_OPTIONAL_CONTROL(fields[:-1])
    return int(fields[1]), int(fields[2]), int(fields[3]), int(fields[4]), fields[5], bytes.fromhex(fields[6].decode())


def parents(root):
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (root / "pg_wal").iterdir()
            if re.fullmatch(r"(?:[0-9A-F]{16}|00000001[0-9A-F]{16})\.wal", path.name)}


def path_for(root, child, start):
    segment = start // SIZE
    return root / "pg_wal" / f"{child:08X}{segment // 256:08X}{segment % 256:08X}.wal"


def save_expected(target, output):
    root = pathlib.Path(target)
    state = ARCHIVE["checked_pending_control"](root)
    fork = struct.unpack_from("<Q", state, 40)[0]
    actual = (1 << 64) if struct.unpack_from("<I", state, 8)[0] & (1 << 31) else struct.unpack_from("<Q", state, 48)[0]
    generation = struct.unpack_from("<Q", state, 76)[0]
    image = (root / "pg_physical_restartpoints" / f"{fork:016X}-{generation:016X}.rp").read_bytes()
    crc(image, 24)
    assert ARCHIVE["fnv"](image) == generation
    text_size, catalog_size = struct.unpack_from("<IQ", image, 8)
    text = image[32:32 + text_size]
    parent_history, parent_digest = ARCHIVE["pending_parent_history"](root, state)
    ARCHIVE["check_retained_archive"](root, None, state, text.decode("ascii"),
                                     parent_history=parent_history, parent_digest=parent_digest)
    catalog = image[32 + text_size:32 + text_size + catalog_size]
    bounds = re.findall(rb":BOUND\s+([0-9]+)", text)
    oids = re.findall(rb":NEXT-OID\s+([0-9]+)", catalog)
    assert len(bounds) == len(oids) == 1
    floor = max(actual, int(bounds[0]) + 1, int((root / "global/pg_control").read_text()))
    if floor < (1 << 64) and floor & 0xFFFFFFFF < 3:
        floor = (floor & ~0xFFFFFFFF) + 3
    expected = {"fork": fork, "actual": actual, "floor": floor, "oid": int(oids[0]),
                "system_id": struct.unpack_from("<Q", state, 16)[0],
                "child": struct.unpack_from("<I", state, 68)[0],
                "parent": struct.unpack_from("<I", state, 84 if len(state) == 148 else 12)[0],
                "time": struct.unpack_from("<q", state, 60)[0], "parents": parents(root),
                "parent_history": parent_history, "parent_digest": parent_digest,
                "pending_control": state.hex(),
                "retained_reference": ARCHIVE["retained_reference"](state)}
    pathlib.Path(output).write_text(json.dumps(expected, sort_keys=True))


def read_record(root, child, start, length, kind):
    data = path_for(root, child, start).read_bytes()[start % SIZE:start % SIZE + length]
    assert len(data) == length and data[:4] == b"PGWL"
    assert struct.unpack_from("<HHIQQ", data, 4) == (8, kind, length, start, 0)
    crc(data, 28)
    return data


def after(cursor, length):
    return cursor if cursor % SIZE + length <= SIZE else (cursor // SIZE + 1) * SIZE


def check_primary(target, expected_path, initial):
    root = pathlib.Path(target)
    expected = json.loads(pathlib.Path(expected_path).read_text())
    checkpoint, durable, child, checkpoint_child, namespace, snapshot = control(root)
    fields = (root / "global/pg_wal_control").read_bytes().split()
    assert len(fields) == 17, "committed child lost its retained reference"
    assert int(fields[7]) == expected["system_id"] and int(fields[8]) == 6
    assert [int(value) for value in fields[10:16]] == expected["retained_reference"]
    assert child == checkpoint_child == expected["child"] and namespace == b"timeline"
    assert len(snapshot) == 88 and snapshot[:8] == b"PGCKPT02"
    start, end, redo = struct.unpack_from("<QQQ", snapshot, 8)
    assert checkpoint == end == start + 120 and redo == start and checkpoint <= durable
    assert struct.unpack_from("<II", snapshot, 64) == (child, expected["parent"])
    assert read_record(root, child, start, 120, 4)[32:] == snapshot
    assert parents(root) == expected["parents"], "parent WAL changed"
    history = (root / "pg_wal" / f"{child:08X}.history").read_bytes()
    parsed = ARCHIVE["parsed_history"](child, history.decode("ascii"))
    assert (parsed[-2][0], parsed[-1][1]) == (expected["parent"], expected["fork"])
    lines = history.splitlines(keepends=True)
    parent_bytes = b"".join(lines[:-1])
    if expected["parent"] > 1:
        assert parent_bytes.endswith(b"\n")
        parent_bytes = parent_bytes[:-1]  # C adds one separator after parent bytes.
    parent_history = parent_bytes.decode("ascii")
    assert parent_history == expected["parent_history"]
    assert lines[-1] == f'{expected["parent"]}\t{expected["fork"] >> 32:X}/{expected["fork"] & 0xFFFFFFFF:08X}\tpg-lisp promotion\n'.encode()
    state = bytes.fromhex(expected["pending_control"])
    digest = expected["retained_reference"][-1]
    ARCHIVE["check_retained_archive"](root, None, state, f"(:PREFIX-DIGEST {digest})",
                                     parent_history=parent_history, parent_digest=expected["parent_digest"])
    if initial == "initial":
        eor_start = after(expected["fork"], 56)
        batch_start = after(eor_start + 56, 32)
        checkpoint_start = after(batch_start + 32, 120)
        assert start == checkpoint_start
        flags = struct.unpack_from("<I", snapshot, 84)[0]
        next_xid = (1 << 64) if flags & 1 else struct.unpack_from("<Q", snapshot, 32)[0]
        active = (1 << 64) if flags & 2 else struct.unpack_from("<Q", snapshot, 72)[0]
        assert next_xid == active == expected["floor"]
        assert struct.unpack_from("<Q", snapshot, 40)[0] == next_xid - 1
        assert struct.unpack_from("<Q", snapshot, 48)[0] == expected["oid"]
        assert struct.unpack_from("<q", snapshot, 56)[0] == expected["time"]
        eor = read_record(root, child, eor_start, 56, 13)
        assert struct.unpack_from("<qIIII", eor, 32) == ((expected["time"] - 946684800) * 1000000,
                                                       child, expected["parent"], 2, 0)
        read_record(root, child, batch_start, 32, 6)
    else:
        assert initial == "later"
    for name in ("recovery.required", "physical-backup.manifest", "physical-recovery.control",
                 "physical-restartpoint.control", "physical-follow.control"):
        assert not (root / name).exists(), f"retired metadata remains: {name}"
    print(f"promotion-primary-inspect: {initial} child={child} checkpoint={checkpoint} PASS")


def drop_retired(target):
    root = pathlib.Path(target)
    assert control(root)[2] > 1, "cannot remove recovery proof before child commitment"
    for name in ("pg_physical_receive", "pg_physical_restartpoints"):
        if (root / name).exists():
            shutil.rmtree(root / name)
    (root / "physical-restartpoint.control").unlink(missing_ok=True)


def checkpoint_bytes(expected):
    eor = after(expected["fork"], 56)
    batch = after(eor + 56, 32)
    start = after(batch + 32, 120)
    snapshot = bytearray(88)
    snapshot[:8] = b"PGCKPT02"
    floor = expected["floor"]
    struct.pack_into("<QQQQQQqIIQII", snapshot, 8, start, start + 120, start,
                     floor if floor < (1 << 64) else 0, floor - 1, expected["oid"],
                     expected["time"], expected["child"], expected["parent"],
                     floor if floor < (1 << 64) else 0, 1, 3 if floor == (1 << 64) else 0)
    record = bytearray(struct.pack("<4sHHIQQI", b"PGWL", 8, 4, 120, start, 0, 0) + snapshot)
    struct.pack_into("<I", record, 28, zlib.crc32(record))
    return start, record


def damage(target, expected_path, kind):
    root = pathlib.Path(target)
    expected = json.loads(pathlib.Path(expected_path).read_text())
    start, record = checkpoint_bytes(expected)
    path = path_for(root, expected["child"], start)
    offset = start % SIZE
    if kind in ("orphan-staging", "orphan-history"):
        path_for(root, expected["child"], 9 * SIZE).write_bytes(b"stale child staging")
        if kind == "orphan-staging":
            return
    elif kind.startswith("torn-"):
        fragment = bytearray(record[:int(kind[5:])])
        if fragment:
            fragment[-1] ^= 1
        path.write_bytes(path.read_bytes()[:offset] + fragment)
        return
    elif kind == "foreign-staging":
        struct.pack_into("<I", record, 96, expected["child"] + 1)
        struct.pack_into("<I", record, 28, 0)
        struct.pack_into("<I", record, 28, zlib.crc32(record))
        path.write_bytes(path.read_bytes()[:offset] + record)
    elif kind == "oversized-staging":
        path.write_bytes(path.read_bytes()[:offset] + record + b"x")
    elif kind == "bad-child-checkpoint":
        data = bytearray(path.read_bytes())
        data[offset + 119] ^= 1
        path.write_bytes(data)
    elif kind == "wrong-pending":
        state_path = root / "physical-recovery.control"
        state = bytearray(state_path.read_bytes()[:68] + b"\0" * 4)
        state[:8] = b"PGRSTAT3"
        struct.pack_into("<I", state, 8, (struct.unpack_from("<I", state, 8)[0] & (1 << 31)) | 2)
        struct.pack_into("<I", state, 68, zlib.crc32(state))
        state_path.write_bytes(state)
    elif kind == "wrong-history":
        fork = expected["fork"] + 1
        (root / "pg_wal" / f'{expected["child"]:08X}.history').write_bytes(
            f'{expected["parent"]}\t{fork >> 32:X}/{fork & 0xFFFFFFFF:08X}\tpg-lisp promotion\n'.encode())
    else:
        raise AssertionError(kind)
    catalog_path = root / "global/pg_catalog.dat"
    data, count = re.subn(rb"(:NEXT-OID\s+)([0-9]+)",
                         lambda match: match[1] + str(int(match[2]) + 17).encode(),
                         catalog_path.read_bytes(), count=1)
    assert count == 1
    catalog_path.write_bytes(data)


if __name__ == "__main__":
    {"expected": save_expected, "check": check_primary, "drop-retired": drop_retired,
     "damage": damage}[sys.argv[1]](*sys.argv[2:])
