"""Independent native control decoding; this is not C pg_control disk format."""
import argparse
import runpy
import struct
from crc32c_reference import crc32c
from pathlib import Path

RECOVERY_FIELDS = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["recovery_control_fields"]
VALIDATE_LOGICAL_FACT = runpy.run_path(str(Path(__file__).resolve().parents[2] / "bin/pg_ctl"))["validate_logical_fact"]

def decode(raw):
    assert raw[:8] == b'PGRSTAT3', 'magic'
    crc_offset = len(raw) - 4
    expected, = struct.unpack_from('<I', raw, crc_offset)
    checked = bytearray(raw)
    struct.pack_into('<I', checked, crc_offset, 0)
    assert crc32c(checked) == expected, 'CRC'
    phase, timeline, system_id, digest, checkpoint, applied, xid = struct.unpack_from('<IIQQQQQ', raw, 8)
    logical = None
    if phase & (1 << 30):
        suffix, = struct.unpack_from('<I', raw, crc_offset - 4)
        assert suffix in (0, 16) and crc_offset >= 68 + 88 + suffix
        logical_start = crc_offset - 88 - suffix
        logical = raw[logical_start:crc_offset]
        phase &= ~(1 << 30)
    layout, resources, fpw = RECOVERY_FIELDS(raw)
    if phase & (1 << 31):
        assert xid == 0, 'hidden exhausted XID value'
        xid = 1 << 64
        phase &= ~(1 << 31)
    assert phase in (0, 1, 2) and 1 <= timeline < 2**32
    applied_timeline = timeline
    if layout == 88:
        applied_timeline, target, _ = struct.unpack_from('<IIQ', raw, 68)
        assert phase == 2 and timeline <= applied_timeline <= target and target > timeline
    if logical is not None:
        VALIDATE_LOGICAL_FACT(logical, applied, applied_timeline)
    assert 32 <= checkpoint <= applied and xid >= 3
    assert phase == 2 or checkpoint == applied
    assert phase != 0 or digest == 0
    state, when = struct.unpack_from('<Iq', raw, 56)
    assert state in ((0,) if phase == 0 else (2, 5)), 'DBState'
    result = dict(version=3, phase=phase, system_id=system_id,
                  applied=applied, next_xid=xid, state=state, time=when)
    if resources is not None:
        result['resource_fact'] = resources
    if fpw is not None:
        result['fpw_fact'] = fpw
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=Path)
    parser.add_argument('expected', choices=['startup', 'active', 'clean'])
    parser.add_argument('--applied')
    parser.add_argument('--retired-fixture', choices=['1', '2'],
                        help='replace an owned, stopped test target control with a retired fixture')
    args = parser.parse_args()
    raw = args.path.read_bytes()
    value = decode(raw)
    assert value['state'] == dict(startup=0, active=5, clean=2)[args.expected], value
    if args.applied:
        high, low = args.applied.split('/')
        assert value['applied'] == (int(high, 16) << 32) + int(low, 16), value
    if args.retired_fixture:
        assert args.expected == 'clean' and struct.unpack_from('<I', raw, 12)[0] == 1
        assert value['next_xid'] < (1 << 64), 'retired controls cannot represent exhaustion'
        version = args.retired_fixture
        old = bytearray(raw[:(56 if version == '1' else 68)] + bytes(4))
        old[:8] = b'PGRSTATE' if version == '1' else b'PGRSTAT2'
        struct.pack_into('<I', old, 8, value['phase'])
        struct.pack_into('<I', old, len(old) - 4, crc32c(old))
        args.path.write_bytes(old)
    print('recovery-control-state:', args.expected, value, 'PASS')


if __name__ == '__main__':
    main()
