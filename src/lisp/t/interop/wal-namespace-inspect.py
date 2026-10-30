"""Independent native filename/control identity and unchanged-byte verification."""
import argparse
import hashlib
import json
from pathlib import Path
import runpy
import zlib

CHECK_OPTIONAL_CONTROL = runpy.run_path(str(Path(__file__).with_name("resource_control_fields.py")))["check_optional_fields"]


def inspect(root):
    raw = (root / 'global/pg_wal_control').read_text(encoding='ascii')
    assert raw.endswith('\n') and raw.count('\n') == 1
    prefix, crc = raw.rstrip('\n').rsplit(' ', 1)
    assert zlib.crc32(prefix.encode()) == int(crc, 16)
    fields = prefix.split(' ')
    CHECK_OPTIONAL_CONTROL(fields)
    magic, checkpoint, durable, insertion, record_tli, namespace, payload = fields[:7]
    assert magic == 'PGWAL6' and insertion == record_tli == '1'
    assert namespace in ('legacy', 'timeline')
    files = {}
    for path in sorted((root / 'pg_wal').glob('*.wal')):
        name = path.stem
        if namespace == 'legacy':
            if len(name) != 16:
                continue
            segment = int(name, 16)
            assert name == f'{segment:016X}'
        else:
            if len(name) != 24 or name[:8] != '00000001':
                continue
            log, low = int(name[8:16], 16), int(name[16:], 16)
            assert low < 256 and name == f'00000001{log:08X}{low:08X}'
            segment = log * 256 + low
        assert not path.is_symlink() and path.is_file()
        assert segment < 2**40 and str(segment) not in files
        files[str(segment)] = hashlib.sha256(path.read_bytes()).hexdigest()
    assert files
    return dict(checkpoint=int(checkpoint), durable=int(durable), insertion=int(insertion),
                checkpoint_tli=int(record_tli), payload=payload, files=files), namespace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--namespace', choices=('legacy', 'timeline'), required=True)
    parser.add_argument('--save', type=Path)
    parser.add_argument('--expect', type=Path)
    args = parser.parse_args()
    facts, namespace = inspect(args.root)
    assert namespace == args.namespace
    if args.expect:
        assert facts == json.loads(args.expect.read_text()), (facts, args.expect)
    if args.save:
        args.save.write_text(json.dumps(facts, sort_keys=True) + '\n')
    print(f'wal-namespace-inspect: {namespace} exact control identity/LSNs/segment bytes PASS', flush=True)


if __name__ == '__main__':
    main()
