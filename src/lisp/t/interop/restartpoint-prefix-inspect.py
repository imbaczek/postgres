#!/usr/bin/env python3
"""Independently compare a selected GEN2 prefix with stopped/idle source WAL."""

import pathlib
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


if __name__ == "__main__":
    main(*sys.argv[1:])
