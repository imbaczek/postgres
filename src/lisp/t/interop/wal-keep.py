"""Compare wal_keep_size metadata and live reload on owned native and C peers."""
import argparse
import importlib
from pathlib import Path
import subprocess
import sys

# Reuse the admission oracle's ownership-checked startup and control client.
peer = importlib.import_module('backend-admission')
SIZE = 16 * 1024 * 1024


def lsn(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def exercise(base, root, port, native):
    connection = peer.connect(port)
    try:
        connection.query("SELECT pg_create_physical_replication_slot('keep_test', true)")
        for megabytes in (0, 1, 15, 16, 17, 31, 32, 64):
            (root / 'data/postgresql.conf').write_text(
                f'wal_keep_size={megabytes}\nmax_slot_wal_keep_size=0\n')
            assert connection.query('SELECT pg_reload_conf()') == [['t']]
            peer.wait(lambda: connection.query(
                "SELECT setting FROM pg_catalog.pg_settings WHERE name='wal_keep_size'") == [[str(megabytes)]], 120)
            shown = '0' if megabytes == 0 else f'{megabytes}MB'
            assert connection.query('SHOW wal_keep_size') == [[shown]]
            assert connection.query(
                "SELECT unit, context, vartype, pending_restart FROM pg_catalog.pg_settings "
                "WHERE name='wal_keep_size'") == [['MB', 'sighup', 'integer', 'f']]
            before = lsn(connection.query('SELECT pg_current_wal_lsn()')[0][0])
            restart, safe = connection.query(
                "SELECT restart_lsn, safe_wal_size FROM pg_catalog.pg_replication_slots "
                "WHERE slot_name='keep_test'")[0]
            after = lsn(connection.query('SELECT pg_current_wal_lsn()')[0][0])
            boundary = (lsn(restart) // SIZE + megabytes // 16 + 1) * SIZE
            assert boundary - after <= int(safe) <= boundary - before, (megabytes, restart, safe, before, after)
        if native:
            assert 'reload complete' in peer.control(base, root / 'control.sock', '(reload)')
            assert connection.query('SHOW wal_keep_size') == [['64MB']]
        (root / 'data/postgresql.conf').write_text('wal_keep_size=64\nmax_slot_wal_keep_size=-1\n')
        assert connection.query('SELECT pg_reload_conf()') == [['t']]
        peer.wait(lambda: connection.query('SHOW max_slot_wal_keep_size') == [['-1']], 120)
        assert connection.query(
            "SELECT safe_wal_size FROM pg_catalog.pg_replication_slots WHERE slot_name='keep_test'") == [[None]]
        print('wal-keep: units, live reload, segment thresholds, safe_wal_size and unlimited slots PASS', flush=True)
    finally:
        connection.close()


def main():
    if not __debug__:
        raise RuntimeError('Run this assertion-based oracle without Python -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('base', type=Path)
    parser.add_argument('root', type=Path)
    parser.add_argument('port', type=int)
    parser.add_argument('executable', type=Path)
    parser.add_argument('--native', action='store_true')
    args = parser.parse_args()
    base, root, executable = args.base.resolve(), args.root.resolve(), args.executable.resolve()
    root.mkdir(mode=0o700)
    directory = root / 'data'
    if args.native:
        directory.mkdir(mode=0o700)
    else:
        with (root / 'initdb.log').open('w') as output:
            subprocess.run([str(executable.parent / 'initdb'), '-D', str(directory),
                            '-A', 'trust', '-U', 'postgres', '--no-locale'],
                           stdout=output, stderr=output, check=True)
    (directory / 'postgresql.conf').write_text('wal_keep_size=0\nmax_slot_wal_keep_size=0\n')
    processes = []
    try:
        process = peer.start(base, root, executable, args.port, 'server', args.native, processes)
        exercise(base, root, args.port, args.native)
        peer.stop(base, root, process)
    finally:
        if (root / 'owned').exists():
            subprocess.run([sys.executable, str(base / 't/interop/owned-peer.py'),
                            'cleanup', str(root / 'owned')], check=True)
        for process in processes:
            process.wait(timeout=10)


if __name__ == '__main__':
    main()
