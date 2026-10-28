#!/usr/bin/env python3
"""Independently compare a selected GEN2 prefix with stopped/idle source WAL."""

import pathlib
import hashlib
import json
import re
import struct
import sys
import zlib


def fnv(data, value=0xCBF29CE484222325):
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


def literal(text):
    """Read the bounded list/string/integer subset used by archive metadata."""
    assert len(text) <= 16 * 1024 * 1024
    tokens = iter(re.findall(r'"(?:\\.|[^"\\])*"|[()]|[^\s()]+', text))

    def read(token, depth=0):
        assert depth < 32
        if token == "(":
            result = []
            for item in tokens:
                if item == ")":
                    return result
                result.append(read(item, depth + 1))
            raise AssertionError("unclosed archive list")
        if token.startswith('"'):
            assert token.endswith('"')
            return re.sub(r"\\(.)", r"\1", token[1:-1], flags=re.S)
        if token.upper() == "NIL":
            return None
        if token.startswith(":"):
            return token.upper()
        assert re.fullmatch(r"[0-9]+", token), f"invalid archive literal: {token}"
        return int(token)

    result = read(next(tokens))
    assert next(tokens, None) is None
    return result


def plist(values):
    assert isinstance(values, list) and len(values) % 2 == 0
    keys = values[::2]
    assert all(isinstance(key, str) and key.startswith(":") for key in keys)
    assert len(set(keys)) == len(keys)
    return dict(zip(keys, values[1::2]))


def checked_pending_control(root):
    control = (root / "physical-recovery.control").read_bytes()
    assert len(control) in (88, 104, 132, 148) and control[:8] == b"PGRSTAT4"
    checked_crc(control, len(control) - 4)
    assert struct.unpack_from("<I", control, 8)[0] & 0x7FFFFFFF == 3
    return control


def retained_reference(control):
    assert len(control) in (132, 148), "publisher omitted its retained archive"
    offset = 100 if len(control) == 148 else 84
    root_id, timeline, base, manifest, end, digest = struct.unpack_from("<QIQQQQ", control, offset)
    assert timeline == struct.unpack_from("<I", control, 12)[0]
    assert base == struct.unpack_from("<Q", control, 32)[0] <= end
    assert manifest == struct.unpack_from("<Q", control, 24)[0]
    assert end == struct.unpack_from("<Q", control, 40)[0]
    return root_id, timeline, base, manifest, end, digest


def archive_object(root, kind, object_id):
    extension = "index" if kind == "inventory" else "wal"
    path = root / "pg_physical_retained" / f"{kind}-{object_id:016X}.{extension}"
    size = path.stat().st_size
    assert 16 <= size <= 64 * 1024 * 1024 and not path.is_symlink()
    image = path.read_bytes()
    assert fnv(image) == object_id, f"{kind} object hash mismatch"
    checked_crc(image, len(image) - 4)
    assert image[:8] == {"inventory": b"PGRINV01", "extent": b"PGREXT01", "overlap": b"PGROVL01"}[kind]
    metadata_length = struct.unpack_from("<I", image, 8)[0]
    start = 12 if kind == "inventory" else 16
    data_length = 0 if kind == "inventory" else struct.unpack_from("<I", image, 12)[0]
    assert len(image) == start + metadata_length + data_length + 4
    metadata = plist(literal(image[start:start + metadata_length].decode("ascii")))
    return path, image, metadata, image[start + metadata_length:-4]


def parsed_history(target, text):
    entries, begin, previous = [], 0, 0
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = line.split()
        assert len(fields) >= 2 and fields[0].isdecimal()
        timeline = int(fields[0])
        assert previous < timeline < target
        assert re.fullmatch(r"[0-9A-Fa-f]+/[0-9A-Fa-f]{1,8}", fields[1])
        high, low = (int(value, 16) for value in fields[1].split("/"))
        end = (high << 32) | low
        assert begin <= end < 1 << 64
        entries.append((timeline, begin, end))
        begin, previous = end, timeline
    assert (target == 1) == (not entries)
    return entries + [(target, begin, None)]


def pending_parent_history(root, control):
    base = struct.unpack_from("<I", control, 12)[0]
    parent = struct.unpack_from("<I", control, 84)[0] if len(control) == 148 else base
    text = (root / "pg_wal" / f"{parent:08X}.history").read_text() if parent > 1 else ""
    if parent == base:
        return text, None
    assert len(control) == 148
    target, digest = struct.unpack_from("<IQ", control, 88)
    for _ in range(256):
        assert target >= parent and digest is not None
        path = root / "pg_physical_receive" / f"{target:08X}-{digest:016X}.history"
        assert not path.is_symlink() and 16 <= path.stat().st_size <= 16 * 1024 * 1024
        image = path.read_bytes()
        assert image[:8] == b"PGPHSH01" and len(image) == 16 + struct.unpack_from("<I", image, 8)[0]
        checked_crc(image, len(image) - 4)
        assert fnv(image) == digest
        metadata = plist(literal(image[12:-4].decode("ascii")))
        assert metadata[":SYSTEM-ID"] == struct.unpack_from("<Q", control, 16)[0]
        assert metadata[":BASE-TIMELINE"] == base
        assert metadata[":BASE"] == struct.unpack_from("<Q", control, 32)[0]
        assert metadata[":TARGET"] == target
        history = parsed_history(target, metadata[":HISTORY-TEXT"])
        if target == parent:
            assert metadata[":HISTORY-TEXT"] == text
            return text, digest
        target, digest = history[-2][0], metadata[":PREVIOUS-DIGEST"]
    raise AssertionError("pending parent selection exceeds chain bound")


def check_archive_records(data, segment, first, history, logical_start):
    position = first
    while position < len(data):
        assert position + 32 <= len(data)
        magic, version, kind, size, start, _, _ = struct.unpack_from("<4sHHIQQI", data, position)
        assert magic == b"PGWL" and version == 7 and size >= 32
        assert start == segment * 16 * 1024 * 1024 + position
        record = data[position:position + size]
        assert len(record) == size
        checked_crc(record, 28)
        timeline = next(entry for entry in history
                        if entry[1] <= start and (entry[2] is None or start < entry[2]))
        assert timeline[2] is None or start + size <= timeline[2]
        if kind == 13:
            assert size == 56
            child, parent, level, padding = struct.unpack_from("<IIII", record, 40)
            index = history.index(timeline)
            assert index > 0 and (child, parent) == (timeline[0], history[index - 1][0])
            fork = timeline[1]
            expected = fork if fork % (16 * 1024 * 1024) + 56 <= 16 * 1024 * 1024 else (fork // (16 * 1024 * 1024) + 1) * 16 * 1024 * 1024
            assert start == expected and level in (0, 1, 2) and padding == 0
        elif kind == 4 and size == 120:
            assert record[32:40] == b"PGCKPT02"
            assert struct.unpack_from("<QQQ", record, 40) == (start, start + size, start)
            assert struct.unpack_from("<I", record, 96)[0] == timeline[0]
        if position == first and logical_start < segment * 16 * 1024 * 1024:
            assert start == (logical_start // (16 * 1024 * 1024) + 1) * 16 * 1024 * 1024
            assert size > start - logical_start
            assert not any(logical_start < entry[1] < start for entry in history)
            if timeline[1] == logical_start:
                assert kind == 13
        position += size
    assert position == len(data)


def check_oracle_fixtures():
    boundary = 16 * 1024 * 1024

    def frame(kind, payload=b"", start=boundary):
        record = bytearray(struct.pack("<4sHHIQQI", b"PGWL", 7, kind, 32 + len(payload), start, 0, 0) + payload)
        struct.pack_into("<I", record, 28, zlib.crc32(record))
        return record

    ordinary = frame(6)
    plain = [(1, 0, None)]
    fork = boundary - 8
    split = [(1, 0, fork), (2, fork, None)]
    check_archive_records(ordinary, 1, 0, plain, fork)
    check_archive_records(frame(13, struct.pack("<qIIII", 0, 2, 1, 2, 0)), 1, 0, split, fork)
    corrupt = bytearray(ordinary)
    corrupt[-1] ^= 1
    rejected = [(ordinary, boundary - 32, plain),
                (ordinary, fork, [(1, 0, boundary - 4), (2, boundary - 4, None)]),
                (ordinary, fork, split),
                (corrupt, fork, plain),
                (frame(6, start=boundary + 1), fork, plain)]
    for record, previous, history in rejected:
        try:
            check_archive_records(record, 1, 0, history, previous)
        except AssertionError:
            continue
        raise AssertionError("invalid rotation or record was accepted")
    print("retained-oracle: ordinary/EOR rotation, fork gaps and record integrity PASS")


def check_retained_archive(root, source, control, generation_text, *, parent_history=None, parent_digest=None):
    root_id, timeline, base, manifest, end, digest = retained_reference(control)
    assert digest == field(generation_text, "PREFIX-DIGEST")
    identity = {":SYSTEM-ID": struct.unpack_from("<Q", control, 16)[0],
                ":BASE-TIMELINE": timeline, ":BASE": base, ":MANIFEST-DIGEST": manifest}
    _, _, index, _ = archive_object(root, "inventory", root_id)
    assert plist(index[":IDENTITY"]) == identity
    parent = struct.unpack_from("<I", control, 84)[0] if len(control) == 148 else timeline
    assert index[":HISTORY-TARGET"] == parent
    if parent_history is None:
        parent_history, parent_digest = pending_parent_history(root, control)
    history = parsed_history(parent, parent_history)
    assert index[":HISTORY-TEXT"] == parent_history
    assert parent_digest is None if parent == timeline else isinstance(parent_digest, int)
    overlap = index[":BASE-OVERLAP"]
    assert isinstance(overlap, list) and len(overlap) == 3
    _, image, metadata, data = archive_object(root, "overlap", overlap[0])
    assert len(image) == overlap[1] and metadata == plist(overlap[2])
    assert all(metadata[key] == value for key, value in identity.items())
    entry = metadata[":FILE"]
    assert len(data) == entry[1] and fnv(data) == entry[2]
    if source is not None:
        with (source / entry[0]).open("rb") as stream:
            assert stream.read(len(data)) == data, "base overlap differs from source WAL"
    cursor, current = base, 0xCBF29CE484222325
    for object_id, size, expected in index[":ENTRIES"] or []:
        _, image, metadata, data = archive_object(root, "extent", object_id)
        assert size == len(image) and metadata == plist(expected)
        assert all(metadata[key] == value for key, value in identity.items())
        assert metadata[":HISTORY-TARGET"] == parent
        assert metadata[":HISTORY-TEXT"] == parent_history
        assert metadata[":HISTORY-DIGEST"] == parent_digest
        assert metadata[":START"] == cursor and metadata[":START-DIGEST"] == current
        assert cursor < metadata[":END"] <= end
        segment, carrier = metadata[":SEGMENT"], metadata[":FILE-TIMELINE"]
        if source is not None:
            path = source / "pg_wal" / f"{carrier:08X}{segment // 256:08X}{segment % 256:08X}.wal"
            if carrier == 1 and not path.exists():
                path = source / "pg_wal" / f"{segment:016X}.wal"
            with path.open("rb") as stream:
                assert stream.read(len(data)) == data, "retained extent differs from source WAL"
        assert fnv(data) == metadata[":PREFIX-DIGEST"]
        first = max(0, cursor - segment * 16 * 1024 * 1024)
        last = metadata[":END"] - segment * 16 * 1024 * 1024
        assert 0 <= first < last == len(data)
        carrier_entry = next(entry for entry in history if entry[0] == carrier)
        assert carrier_entry[1] // (16 * 1024 * 1024) <= segment
        assert carrier_entry[2] is None or segment * 16 * 1024 * 1024 < carrier_entry[2]
        check_archive_records(data, segment, first, history, cursor)
        current = fnv(data[first:last], current)
        assert current == metadata[":END-DIGEST"]
        cursor = metadata[":END"]
    assert (cursor, current) == (end, digest), "retained archive does not cover the exact fork"


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
    directories = {"base", "global", "pg_wal", "pg_physical_receive",
                   "pg_physical_restartpoints", "pg_physical_retained"}
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
    # Schema alone is insufficient: this fixture deliberately lacks generation ownership.
    struct.pack_into("<IIQ", pending, 68, 2, source_namespace, digest ^ 1)
    struct.pack_into("<I", pending, 84, zlib.crc32(pending))
    path.write_bytes(pending)
    pathlib.Path(snapshot).write_text(json.dumps(storage_snapshot(root), sort_keys=True))
    print(f"pending-control-fixture: prepared unproven fork={lsn} child=2 generation={digest ^ 1:016X}")


def verify_pending(target, snapshot):
    expected = json.loads(pathlib.Path(snapshot).read_text())
    assert storage_snapshot(pathlib.Path(target)) == expected, "pending startup mutated storage"
    print(f"pending-control-fixture: unchanged {len(expected)} storage files PASS")


def snapshot_pending(target, source, snapshot):
    root = pathlib.Path(target)
    lsn, digest = main(target, source)
    control = checked_pending_control(root)
    assert struct.unpack_from("<I", control, 8)[0] & 0x7FFFFFFF == 3
    assert struct.unpack_from("<I", control, 12)[0] == 1
    assert struct.unpack_from("<Q", control, 40)[0] == lsn
    assert struct.unpack_from("<Q", control, 76)[0] == digest
    assert struct.unpack_from("<I", control, 68)[0] == 2
    fields = (root / "global" / "pg_wal_control").read_bytes().split()
    assert fields[0] == b"PGWAL6"
    assert zlib.crc32(b" ".join(fields[:-1])) == int(fields[-1], 16)
    assert struct.unpack_from("<I", control, 72)[0] == {b"legacy": 0, b"timeline": 1}[fields[5]]
    image = (root / "pg_physical_restartpoints" / f"{lsn:016X}-{digest:016X}.rp").read_bytes()
    text_length = struct.unpack_from("<I", image, 8)[0]
    check_retained_archive(root, pathlib.Path(source), control, image[32:32 + text_length].decode("ascii"))
    pathlib.Path(snapshot).write_text(json.dumps(storage_snapshot(root), sort_keys=True))
    print(f"pending-intent: actual publisher bound fork={lsn} generation={digest:016X} PASS")


def check_child_prefix(target, source, expected):
    root = pathlib.Path(target)
    lsn, digest = main(target, source)
    control = checked_pending_control(root)
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
    control = checked_pending_control(root)
    assert struct.unpack_from("<I", control, 8)[0] & 0x7FFFFFFF == 3
    parent = struct.unpack_from("<I", control, 12)[0]
    child = struct.unpack_from("<I", control, 68)[0]
    assert struct.unpack_from("<Q", control, 40)[0] == fork
    assert struct.unpack_from("<Q", control, 76)[0] == digest
    image = (root / "pg_physical_restartpoints" / f"{fork:016X}-{digest:016X}.rp").read_bytes()
    text_length = struct.unpack_from("<I", image, 8)[0]
    catalog_length = struct.unpack_from("<Q", image, 12)[0]
    actual_next = (1 << 64) if struct.unpack_from("<I", control, 8)[0] & (1 << 31) else struct.unpack_from("<Q", control, 48)[0]
    assert field(image[32:32 + text_length].decode("ascii"), "NEXT-XID") == actual_next
    assert (root / "global" / "pg_catalog.dat").read_bytes() == image[32 + text_length:32 + text_length + catalog_length]
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


def snapshot_storage(target, snapshot):
    pathlib.Path(snapshot).write_text(json.dumps(storage_snapshot(pathlib.Path(target)), sort_keys=True))


def damage_resume(target, change):
    root = pathlib.Path(target)
    control_path = root / "physical-recovery.control"
    control = bytearray(control_path.read_bytes())
    fork = struct.unpack_from("<Q", control, 40)[0]
    child = struct.unpack_from("<I", control, 68)[0]
    segment = fork // (16 * 1024 * 1024)
    child_path = root / "pg_wal" / f"{child:08X}{segment // 256:08X}{segment % 256:08X}.wal"
    pointer_path = root / "physical-restartpoint.control"
    pointer = bytearray(pointer_path.read_bytes())
    digest = struct.unpack_from("<Q", pointer, 16)[0]
    generation_path = root / "pg_physical_restartpoints" / f"{fork:016X}-{digest:016X}.rp"
    if change == "marker":
        (root / "recovery.required").unlink()
    elif change == "pointer":
        pointer[-1] ^= 1
        pointer_path.write_bytes(pointer)
    elif change == "generation":
        data = bytearray(generation_path.read_bytes())
        data[32] ^= 1
        generation_path.write_bytes(data)
    elif change in ("retained-inventory", "retained-extent", "retained-overlap"):
        root_id = retained_reference(control)[0]
        inventory_path, image, index, _ = archive_object(root, "inventory", root_id)
        if change == "retained-inventory":
            path = inventory_path
        else:
            kind = change.removeprefix("retained-")
            object_id = index[":ENTRIES"][0][0] if kind == "extent" else index[":BASE-OVERLAP"][0]
            path, image, _, _ = archive_object(root, kind, object_id)
        damaged = bytearray(image)
        damaged[-1] ^= 1
        path.write_bytes(damaged)
    elif change == "allocation":
        data = generation_path.read_bytes()
        length = struct.unpack_from("<I", data, 8)[0]
        text = data[32:32 + length]
        text, count = re.subn(
            rb"(:SOURCE-RESERVATION\s+\([^()]*:COMPLETE\s+)T(?=[\s)])",
            rb"\g<1>NIL", text, flags=re.I)
        assert count == 1, "missing or ambiguous complete source allocation proof"
        image = bytearray(data[:32] + text + data[32 + length:])
        struct.pack_into("<I", image, 8, len(text))
        struct.pack_into("<I", image, 24, 0)
        struct.pack_into("<I", image, 24, zlib.crc32(image))
        new_digest = fnv(image)
        (generation_path.parent / f"{fork:016X}-{new_digest:016X}.rp").write_bytes(image)
        struct.pack_into("<Q", pointer, 16, new_digest)
        struct.pack_into("<I", pointer, 24, 0)
        struct.pack_into("<I", pointer, 24, zlib.crc32(pointer))
        pointer_path.write_bytes(pointer)
        struct.pack_into("<Q", control, 76, new_digest)
        struct.pack_into("<I", control, len(control) - 4, 0)
        struct.pack_into("<I", control, len(control) - 4, zlib.crc32(control))
        control_path.write_bytes(control)
    elif change == "receive":
        path = root / "pg_physical_receive" / "receive.control"
        data = bytearray(path.read_bytes())
        data[-1] ^= 1
        path.write_bytes(data)
    elif change == "backlog":
        path = root / "pg_physical_receive" / "receive.control"
        receive = bytearray(path.read_bytes())
        assert len(receive) in (68, 96)
        if len(receive) == 96:
            assert struct.unpack_from("<I", receive, 64)[0] == 0
        flush = struct.unpack_from("<Q", receive, 32)[0]
        assert flush == fork and flush % (16 * 1024 * 1024) + 64 <= 16 * 1024 * 1024
        records = []
        for kind, start in ((6, flush), (9, flush + 32)):
            record = bytearray(struct.pack("<4sHHIQQI", b"PGWL", 7, kind, 32, start, 0, 0))
            struct.pack_into("<I", record, 28, zlib.crc32(record))
            records.append(record)
        batch = b"".join(records)
        value = struct.unpack_from("<Q", receive, 40)[0]
        for byte in batch:
            value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
        segment_path = path.parent / f"{flush // (16 * 1024 * 1024):016X}.wal"
        offset = flush % (16 * 1024 * 1024)
        prefix = segment_path.read_bytes()[:offset] if segment_path.exists() else b"\0" * offset
        assert len(prefix) == offset
        segment_path.write_bytes(prefix + batch)
        struct.pack_into("<Q", receive, 32, flush + 64)
        struct.pack_into("<Q", receive, 40, value)
        crc_offset = len(receive) - 4
        struct.pack_into("<I", receive, crc_offset, 0)
        struct.pack_into("<I", receive, crc_offset, zlib.crc32(receive))
        path.write_bytes(receive)
    elif change == "lock":
        (root / "pg_physical_receive" / "receive.lock").unlink()
    elif change == "child":
        data = bytearray(child_path.read_bytes())
        data[0] ^= 1
        child_path.write_bytes(data)
    elif change == "missing-child":
        child_path.unlink()
    elif change == "history":
        (root / "pg_wal" / f"{child:08X}.history").write_bytes(b"1\t0/00000000\tforeign\n")
    elif change == "base-control":
        path = root / "global" / "pg_wal_control"
        path.write_bytes(path.read_bytes() + b" ")
    elif change == "ahead-catalog":
        path = root / "global" / "pg_catalog.dat"
        data, count = re.subn(rb"(:NEXT-OID\s+)([0-9]+)",
                             lambda match: match[1] + str(int(match[2]) + 17).encode(),
                             path.read_bytes(), count=1)
        assert count == 1
        path.write_bytes(data)
    elif change == "fork":
        paths = [path for path in (root / "base").iterdir()
                 if path.name.isdecimal() and path.is_file() and path.stat().st_size > 0]
        assert paths
        paths[0].unlink()
    elif change == "system":
        struct.pack_into("<Q", control, 16, struct.unpack_from("<Q", control, 16)[0] ^ 1)
        struct.pack_into("<I", control, len(control) - 4, 0)
        struct.pack_into("<I", control, len(control) - 4, zlib.crc32(control))
        control_path.write_bytes(control)
    elif change == "valid-tail":
        data = child_path.read_bytes()
        offset = fork % (16 * 1024 * 1024)
        record = bytearray(data[offset:])
        assert len(record) == 56
        struct.pack_into("<I", record, 40, child + 1)
        struct.pack_into("<I", record, 28, 0)
        struct.pack_into("<I", record, 28, zlib.crc32(record))
        child_path.write_bytes(data[:offset] + record)
    elif change == "oversized-tail":
        with child_path.open("ab") as stream:
            stream.write(b"x")
    elif change == "unacknowledged":
        path = root / "pg_physical_receive" / "receive.control"
        flush = struct.unpack_from("<Q", path.read_bytes(), 32)[0]
        path = path.parent / f"{flush // (16 * 1024 * 1024):016X}.wal"
        offset = flush % (16 * 1024 * 1024)
        prefix = path.read_bytes()[:offset] if path.exists() else b"\0" * offset
        assert len(prefix) == offset
        path.write_bytes(prefix + b"unacknowledged-tail")
    elif change.startswith("torn-"):
        count = int(change[5:])
        child_path.write_bytes(child_path.read_bytes()[:fork % (16 * 1024 * 1024)] + b"\xA5" * count)
    else:
        raise AssertionError(f"unknown resume damage: {change}")


if __name__ == "__main__":
    if sys.argv[1] == "--check-oracle-fixtures":
        check_oracle_fixtures()
    elif sys.argv[1] == "--pending-fixture":
        prepare_pending(*sys.argv[2:])
    elif sys.argv[1] == "--verify-pending":
        verify_pending(*sys.argv[2:])
    elif sys.argv[1] == "--snapshot-pending":
        snapshot_pending(*sys.argv[2:])
    elif sys.argv[1] == "--check-child-prefix":
        check_child_prefix(*sys.argv[2:])
    elif sys.argv[1] == "--check-promotion-eor":
        check_promotion_eor(*sys.argv[2:])
    elif sys.argv[1] == "--snapshot-storage":
        snapshot_storage(*sys.argv[2:])
    elif sys.argv[1] == "--damage-resume":
        damage_resume(*sys.argv[2:])
    else:
        main(*sys.argv[1:])
