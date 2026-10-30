"""Check parameter-report order directly from native WAL, independently of Lisp."""
from pathlib import Path
import struct
import sys
import zlib


def reports(directory):
    found = []
    for path in (Path(directory) / "pg_wal").glob("*.wal"):
        stem = path.stem
        assert len(stem) in (16, 24), path
        timeline = int(stem[:8], 16) if len(stem) == 24 else 1
        segment = int(stem[-16:], 16)
        data = path.read_bytes()
        offset = 0
        while offset < len(data):
            assert offset + 32 <= len(data), (path, offset)
            magic, version, kind, size, start, xid, crc = struct.unpack_from("<4sHHIQQI", data, offset)
            assert magic == b"PGWL" and version >= 5 and size >= 32
            assert start == segment * 16777216 + offset
            assert offset + size <= len(data)
            record = bytearray(data[offset:offset + size])
            record[28:32] = bytes(4)
            assert zlib.crc32(record) == crc, (path, offset)
            if kind == 16:
                assert version == 8 and xid == 0 and size == 64
                assert record[32:40] == b"PGRES001" and record[60:64] == bytes(4)
                found.append((start + size, timeline, struct.unpack_from("<5I", record, 40)))
            offset += size
    return sorted(found)


if __name__ == "__main__":
    history = reports(sys.argv[1])
    expected = [tuple(map(int, value.split(","))) for value in sys.argv[2:]]
    assert [entry[2] for entry in history[-len(expected):]] == expected, history
    print("resource-report-history: durable report order PASS")
