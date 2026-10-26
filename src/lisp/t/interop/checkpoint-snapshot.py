#!/usr/bin/env python3
"""Independent decoder for native checkpoint copies; this is not C WAL."""
import argparse
import json
import os
from pathlib import Path
import struct
import zlib


def checkpoint(data):
    assert len(data) == 88 and data[:8] == b"PGCKPT01"
    start, end, redo, xid, reserved, oid = struct.unpack_from("<6Q", data, 8)
    timestamp = struct.unpack_from("<q", data, 56)[0]
    timeline, previous, active, known_oid, tail = struct.unpack_from("<IIQII", data, 64)
    assert end == start + 120 and start // 16777216 == (end - 1) // 16777216
    assert redo == start and 3 <= xid <= reserved + 1 and reserved >= 2
    assert active == 0 or 3 <= active <= xid
    assert timeline == previous == 1 and known_oid in (0, 1) and tail == 0
    assert known_oid or oid == 0
    return dict(start=start, end=end, redo=redo, next_xid=xid,
                reserved_through=reserved, next_oid=oid if known_oid else None,
                time=timestamp, timeline=timeline, previous=previous, active=active)


def read_copy(pgdata):
    raw = (pgdata / "global/pg_wal_control").read_text(encoding="ascii")
    assert raw.endswith("\n") and raw.count("\n") == 1
    prefix, crc = raw.rstrip("\n").rsplit(" ", 1)
    assert zlib.crc32(prefix.encode("ascii")) == int(crc, 16)
    version, end, durable, payload = prefix.split(" ")
    assert version == "PGWAL4"
    data = bytes.fromhex(payload)
    facts = checkpoint(data)
    assert facts["end"] == int(end) <= int(durable)
    segment = pgdata / f"pg_wal/{facts['start'] // 16777216:016X}.wal"
    with segment.open("rb") as stream:
        stream.seek(facts["start"] % 16777216)
        record = bytearray(stream.read(120))
    magic, wal_version, kind, size, start, xid, crc = struct.unpack_from("<4sHHIQQI", record)
    assert (magic, wal_version, kind, size, start, xid) == (b"PGWL", 6, 4, 120, facts["start"], 0)
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


def convert_fixture(pgdata, facts, payload, legacy):
    """Only the shell regression calls this, after killing and waiting its PID."""
    check_stopped(pgdata)
    control = pgdata / "global/pg_wal_control"
    fields = control.read_text().split()
    assert int(fields[1]) == int(fields[2]) == facts["end"]
    if legacy:
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
    args = parser.parse_args()
    facts, payload = read_copy(args.pgdata)
    if args.legacy_fixture or args.corrupt_copy_fixture:
        convert_fixture(args.pgdata, facts, payload, args.legacy_fixture)
        return
    if args.expect:
        assert facts == json.loads(args.expect.read_text())
    if args.save:
        args.save.write_text(json.dumps(facts, sort_keys=True) + "\n")
    print(payload if args.hex else json.dumps(facts, sort_keys=True))


if __name__ == "__main__":
    main()
