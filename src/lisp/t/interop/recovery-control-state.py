"""Independent native control decoding; this is not C pg_control disk format."""
import argparse
import runpy
import struct
import zlib
from pathlib import Path

RECOVERY_LAYOUT = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["recovery_control_layout"]

def decode(raw):
    legacy = len(raw) == 60
    versions = {b'PGRSTATE': 1} if legacy else {b'PGRSTAT2': 2, b'PGRSTAT3': 3}
    magic = bytes(raw[:8])
    assert magic in versions, 'magic'
    version = versions[magic]
    if version == 3:
        layout, resources = RECOVERY_LAYOUT(raw)
    else:
        assert len(raw) == (60 if legacy else 72)
        layout, resources = len(raw), None
    crc_offset = len(raw) - 4
    expected, = struct.unpack_from('<I', raw, crc_offset)
    checked = bytearray(raw)
    struct.pack_into('<I', checked, crc_offset, 0)
    assert zlib.crc32(checked) == expected, 'CRC'
    phase, timeline, system_id, digest, checkpoint, applied, xid = struct.unpack_from('<IIQQQQQ', raw, 8)
    if version == 3 and phase & (1 << 31):
        assert xid == 0, 'hidden exhausted XID value'
        xid = 1 << 64
        phase &= ~(1 << 31)
    assert phase in (0, 1, 2) and 1 <= timeline < 2**32
    assert version == 3 or timeline == 1
    if layout == 88:
        applied_timeline, target, _ = struct.unpack_from('<IIQ', raw, 68)
        assert phase == 2 and timeline <= applied_timeline <= target and target > timeline
    assert 32 <= checkpoint <= applied and xid >= 3
    assert phase == 2 or checkpoint == applied
    assert phase != 0 or digest == 0
    state = None if legacy else struct.unpack_from('<I', raw, 56)[0]
    when = None if legacy else struct.unpack_from('<q', raw, 60)[0]
    if not legacy:
        assert state in ((0,) if phase == 0 else (2, 5)), 'DBState'
        assert -(2**63) <= when < 2**63, 'state time'
    result = dict(version=version, phase=phase, system_id=system_id,
                  applied=applied, next_xid=xid, state=state, time=when)
    if resources is not None:
        result['resource_fact'] = resources
    return result


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
        assert not (struct.unpack_from('<I', raw, 8)[0] & (1 << 31)), 'legacy cannot represent exhaustion'
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
