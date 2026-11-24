#!/usr/bin/env python3
"""Independent decoder for native checkpoint copies; this is not C WAL."""
import argparse
import json
import os
from pathlib import Path
import runpy
import struct
from crc32c_reference import crc32c

CHECK_OPTIONAL_CONTROL = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["check_optional_fields"]
CHECK_RESOURCES = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["checkpoint_resource_fact"]
CHECK_COMMIT_TS = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["checkpoint_commit_ts_fact"]
CHECK_FPW = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["checkpoint_full_page_writes_fact"]


def checkpoint(data):
    resources = CHECK_RESOURCES(data)
    commit_ts = CHECK_COMMIT_TS(data)
    full_page_writes = CHECK_FPW(data)
    start, end, redo, xid, reserved, oid = struct.unpack_from("<6Q", data, 8)
    timestamp = struct.unpack_from("<q", data, 56)[0]
    timeline, previous, active, known_oid, tail = struct.unpack_from("<IIQII", data, 64)
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
    if commit_ts is not None:
        facts["oldest_commit_ts_xid"], facts["newest_commit_ts_xid"] = commit_ts
    if full_page_writes is not None:
        facts["full_page_writes"] = full_page_writes
    if tail & 32:
        facts["logical_decoding_enabled"] = bool(tail & 64)
    if tail & 128:
        facts["logical_on_start_lsn"], facts["logical_on_start_timeline"] = struct.unpack_from("<QI", data, len(data) - 16)
    return facts


def checkpoint_segment(pgdata, facts):
    number = facts["start"] // 16777216
    name = f"{facts['timeline']:08X}{number // 256:08X}{number % 256:08X}"
    return pgdata / "pg_wal" / f"{name}.wal"


def read_copy(pgdata):
    raw = (pgdata / "global/pg_wal_control").read_text(encoding="ascii")
    assert raw.endswith("\n") and raw.count("\n") == 1
    prefix, crc = raw.rstrip("\n").rsplit(" ", 1)
    assert crc32c(prefix.encode("ascii")) == int(crc, 16)
    fields = prefix.split(" ")
    assert fields[0] == "PGWAL6"
    CHECK_OPTIONAL_CONTROL(fields)
    _, end, durable, insertion, checkpoint_tli, namespace, payload = fields[:7]
    assert 1 <= int(checkpoint_tli) <= int(insertion)
    assert namespace == "timeline"
    data = bytes.fromhex(payload)
    facts = checkpoint(data)
    assert facts["timeline"] == int(checkpoint_tli)
    assert facts["end"] == int(end) <= int(durable)
    segment = checkpoint_segment(pgdata, facts)
    with segment.open("rb") as stream:
        stream.seek(facts["start"] % 16777216)
        record = bytearray(stream.read(32 + len(data)))
    magic, wal_version, kind, size, start, xid, crc = struct.unpack_from("<4sHHIQQI", record)
    assert wal_version == 8
    assert (magic, kind, size, start, xid) == (b"PGWL", 4, 32 + len(data), facts["start"], 0)
    struct.pack_into("<I", record, 28, 0)
    assert crc32c(record) == crc and record[32:] == data
    return facts, payload


def check_stopped(pgdata):
    pid = int((pgdata / "postmaster.pid").read_text().splitlines()[0])
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    raise AssertionError("fixture conversion requires a stopped, owned test server")


def convert_fixture(pgdata, facts, payload, fixture):
    """Only the shell regression calls this, after killing and waiting its PID."""
    check_stopped(pgdata)
    assert facts["timeline"] == facts["previous"] == 1
    control = pgdata / "global/pg_wal_control"
    fields = control.read_text().split()[:-1]
    assert int(fields[1]) == int(fields[2]) == facts["end"]
    if fixture == "unsupported-insertion":
        fields[3] = "2"
    elif fixture == "retired-control":
        fields = ["PGWAL5", *fields[1:5], payload]
    elif fixture == "retired-wal":
        segment = checkpoint_segment(pgdata, facts)
        offset = facts["start"] % 16777216
        with segment.open("r+b") as stream:
            stream.seek(offset)
            record = bytearray(stream.read(facts["end"] - facts["start"]))
            struct.pack_into("<H", record, 4, 7)
            struct.pack_into("<I", record, 28, 0)
            struct.pack_into("<I", record, 28, crc32c(record))
            stream.seek(offset)
            stream.write(record)
            stream.flush()
            os.fsync(stream.fileno())
        return
    else:
        data = bytearray.fromhex(payload)
        if fixture == "retired-snapshot":
            assert data[:8] == b"PGCKPT02"
            # PGCKPT01 used only the base payload, with no presence flags.
            del data[88:]
            struct.pack_into("<I", data, 84, 0)
            end = facts["start"] + 32 + len(data)
            struct.pack_into("<Q", data, 16, end)
            assert checkpoint(data)["end"] == end
            fields[1] = str(end)
            data[:8] = b"PGCKPT01"
        else:
            assert fixture == "corrupt-copy"
            struct.pack_into("<q", data, 56, facts["time"] + 1)
        fields[6] = data.hex().upper()
    prefix = " ".join(fields)
    with control.open("wb") as stream:
        stream.write(f"{prefix} {crc32c(prefix.encode()):08X}\n".encode())
        stream.flush()
        os.fsync(stream.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pgdata", type=Path)
    parser.add_argument("--save", type=Path)
    parser.add_argument("--expect", type=Path)
    parser.add_argument("--hex", action="store_true")
    fixture = parser.add_mutually_exclusive_group()
    for kind in ("retired-wal", "retired-control", "retired-snapshot", "corrupt-copy",
                 "unsupported-insertion"):
        fixture.add_argument(f"--{kind}-fixture", dest="fixture", action="store_const", const=kind)
    args = parser.parse_args()
    facts, payload = read_copy(args.pgdata)
    if args.fixture:
        convert_fixture(args.pgdata, facts, payload, args.fixture)
        return
    if args.expect:
        assert facts == json.loads(args.expect.read_text())
    if args.save:
        args.save.write_text(json.dumps(facts, sort_keys=True) + "\n")
    print(payload if args.hex else json.dumps(facts, sort_keys=True))


if __name__ == "__main__":
    main()
