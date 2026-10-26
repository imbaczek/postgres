"""Independent native control decoding; this is not C pg_control disk format."""
import argparse
import struct
import zlib
from pathlib import Path


def decode(raw):
    assert len(raw) in (60, 72), ('length', len(raw))
    legacy = len(raw) == 60
    assert raw[:8] == (b'PGRSTATE' if legacy else b'PGRSTAT2'), 'magic'
    crc_offset = 56 if legacy else 68
    expected, = struct.unpack_from('<I', raw, crc_offset)
    checked = bytearray(raw)
    struct.pack_into('<I', checked, crc_offset, 0)
    assert zlib.crc32(checked) == expected, 'CRC'
    phase, timeline, system_id, digest, checkpoint, applied, xid = struct.unpack_from('<IIQQQQQ', raw, 8)
    assert phase in (0, 1, 2) and timeline == 1
    assert 32 <= checkpoint <= applied and xid >= 3
    assert phase == 2 or checkpoint == applied
    assert phase != 0 or digest == 0
    state = None if legacy else struct.unpack_from('<I', raw, 56)[0]
    when = None if legacy else struct.unpack_from('<q', raw, 60)[0]
    if not legacy:
        assert state in ((0,) if phase == 0 else (2, 5)), 'DBState'
        assert -(2**63) <= when < 2**63, 'state time'
    return dict(version=1 if legacy else 2, phase=phase, system_id=system_id,
                applied=applied, state=state, time=when)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=Path)
    parser.add_argument('expected', choices=['legacy', 'startup', 'active', 'clean'])
    parser.add_argument('--applied')
    parser.add_argument('--legacy-fixture', action='store_true',
                        help='convert an owned, stopped test target to the legacy fixture')
    args = parser.parse_args()
    raw = args.path.read_bytes()
    decode(raw)
    if args.legacy_fixture:
        assert args.expected == 'legacy' and len(raw) == 72
        old = bytearray(raw[:56] + bytes(4))
        old[:8] = b'PGRSTATE'
        struct.pack_into('<I', old, 56, zlib.crc32(old))
        args.path.write_bytes(old)
        raw = args.path.read_bytes()
    value = decode(raw)
    assert value['state'] == dict(legacy=None, startup=0, active=5, clean=2)[args.expected], value
    if args.applied:
        high, low = args.applied.split('/')
        assert value['applied'] == (int(high, 16) << 32) + int(low, 16), value
    print('recovery-control-state:', args.expected, value, 'PASS')


if __name__ == '__main__':
    main()
