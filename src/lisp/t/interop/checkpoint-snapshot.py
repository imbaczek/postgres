#!/usr/bin/env python3
"""Independent decoder for native checkpoint copies; this is not C WAL."""
import argparse
import json
import os
from pathlib import Path
import runpy
import struct
import zlib

CHECK_OPTIONAL_CONTROL = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["check_optional_fields"]
CHECK_RESOURCES = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["checkpoint_resource_fact"]


def checkpoint(data):
    resources = CHECK_RESOURCES(data)
    start, end, redo, xid, reserved, oid = struct.unpack_from("<6Q", data, 8)
    timestamp = struct.unpack_from("<q", data, 56)[0]
    timeline, previous, active, known_oid, tail = struct.unpack_from("<IIQII", data, 64)
    assert tail == 0 if data[:8] == b"PGCKPT01" else 0 <= tail <= 3
    if tail & 1:
        assert xid == 0
        xid = 1 << 64
    if tail & 2:
        assert active == 0
        active = 1 << 64
    assert end == start + 32 + len(data) and start // 16777216 == (end - 1) // 16777216
    assert redo == start and 3 <= xid <= reserved + 1 and reserved >= 2
    assert active == 0 or 3 <= active <= xid
    assert 1 <= previous <= timeline and known_oid in (0, 1)
    assert known_oid or oid == 0
    facts = dict(start=start, end=end, redo=redo, next_xid=xid,
                 reserved_through=reserved, next_oid=oid if known_oid else None,
                 time=timestamp, timeline=timeline, previous=previous, active=active)
    if resources is not None:
        facts["resource_fact"] = resources
    return facts


def read_copy(pgdata):
    raw = (pgdata / "global/pg_wal_control").read_text(encoding="ascii")
    assert raw.endswith("\n") and raw.count("\n") == 1
    prefix, crc = raw.rstrip("\n").rsplit(" ", 1)
    assert zlib.crc32(prefix.encode("ascii")) == int(crc, 16)
    fields = prefix.split(" ")
    namespace = "legacy"
    if fields[0] == "PGWAL6":
        CHECK_OPTIONAL_CONTROL(fields)
        version, end, durable, insertion, checkpoint_tli, namespace, payload = fields[:7]
        assert 1 <= int(checkpoint_tli) <= int(insertion)
        assert namespace in ("legacy", "timeline")
    elif fields[0] == "PGWAL5":
        version, end, durable, insertion, checkpoint_tli, payload = fields
        assert int(insertion) == int(checkpoint_tli) == 1
    else:
        version, end, durable, payload = fields
        assert version == "PGWAL4"
    data = bytes.fromhex(payload)
    facts = checkpoint(data)
    if version in ("PGWAL5", "PGWAL6"):
        assert facts["timeline"] == int(checkpoint_tli)
    else:
        assert facts["timeline"] == 1
    assert facts["end"] == int(end) <= int(durable)
    number = facts['start'] // 16777216
    name = (f"{number:016X}" if namespace == "legacy" else f"{facts['timeline']:08X}{number // 256:08X}{number % 256:08X}")
    segment = pgdata / f"pg_wal/{name}.wal"
    with segment.open("rb") as stream:
        stream.seek(facts["start"] % 16777216)
        record = bytearray(stream.read(32 + len(data)))
    magic, wal_version, kind, size, start, xid, crc = struct.unpack_from("<4sHHIQQI", record)
    assert wal_version in (6, 7, 8)
    assert wal_version >= 7 or data[:8] == b"PGCKPT01"
    assert wal_version >= 8 or len(data) == 88
    assert (magic, kind, size, start, xid) == (b"PGWL", 4, 32 + len(data), facts["start"], 0)
    struct.pack_into("<I", record, 28, 0)
    assert zlib.crc32(record) == crc and record[32:] == data
    return facts, payload


def check_stopped(pgdata):
    pid = int((pgdata / "postmaster.pid").read_text().splitlines()[0])
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    raise AssertionError("fixture conversion requires a stopped, owned test server")


def convert_fixture(pgdata, facts, payload, legacy, unsupported_insertion=False):
    """Only the shell regression calls this, after killing and waiting its PID."""
    check_stopped(pgdata)
    assert facts["timeline"] == facts["previous"] == 1
    control = pgdata / "global/pg_wal_control"
    fields = control.read_text().split()
    assert int(fields[1]) == int(fields[2]) == facts["end"]
    if unsupported_insertion:
        prefix = f"PGWAL5 {facts['end']} {facts['end']} 2 1 {payload}"
    elif legacy:
        segment = pgdata / f"pg_wal/{facts['start'] // 16777216:016X}.wal"
        assert sorted((pgdata / "pg_wal").glob("*.wal"))[-1] == segment
        header = bytearray(struct.pack("<4sHHIQQI", b"PGWL", 5, 4, 32, facts["start"], 0, 0))
        struct.pack_into("<I", header, 28, zlib.crc32(header))
        with segment.open("r+b") as stream:
            assert stream.seek(0, 2) == facts["end"] - facts["start"] // 16777216 * 16777216
            stream.seek(facts["start"] % 16777216)
            stream.write(header)
            stream.truncate()
            stream.flush()
            os.fsync(stream.fileno())
        end = facts["start"] + 32
        prefix = f"PGWAL3 {end} {end}"
    else:
        data = bytearray.fromhex(payload)
        struct.pack_into("<q", data, 56, facts["time"] + 1)
        prefix = f"PGWAL4 {facts['end']} {facts['end']} {data.hex().upper()}"
    with control.open("wb") as stream:
        stream.write(f"{prefix} {zlib.crc32(prefix.encode()):08X}\n".encode())
        stream.flush()
        os.fsync(stream.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pgdata", type=Path)
    parser.add_argument("--save", type=Path)
    parser.add_argument("--expect", type=Path)
    parser.add_argument("--hex", action="store_true")
    fixture = parser.add_mutually_exclusive_group()
    fixture.add_argument("--legacy-fixture", action="store_true")
    fixture.add_argument("--corrupt-copy-fixture", action="store_true")
    fixture.add_argument("--unsupported-insertion-fixture", action="store_true")
    args = parser.parse_args()
    facts, payload = read_copy(args.pgdata)
    if args.legacy_fixture or args.corrupt_copy_fixture or args.unsupported_insertion_fixture:
        convert_fixture(pgdata=args.pgdata, facts=facts, payload=payload, legacy=args.legacy_fixture,
                        unsupported_insertion=args.unsupported_insertion_fixture)
        return
    if args.expect:
        assert facts == json.loads(args.expect.read_text())
    if args.save:
        args.save.write_text(json.dumps(facts, sort_keys=True) + "\n")
    print(payload if args.hex else json.dumps(facts, sort_keys=True))


if __name__ == "__main__":
    main()
