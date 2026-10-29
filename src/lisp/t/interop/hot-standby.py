"""Compare disabled hot standby, continued replay and promotion on owned peers."""
import argparse
import importlib
from pathlib import Path
import signal
import subprocess
import sys

peer = importlib.import_module('backend-admission')


def position(text):
    high, low = text.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def disabled(port):
    for mode in (None, 'true', 'database'):
        try:
            connection = peer.connect(port, mode)
        except peer.WireError as error:
            assert error.sqlstate == '57P03' and error.severity == 'FATAL', error
            assert not error.authenticated, error
            if error.fields.get('D') != 'Hot standby mode is disabled.':
                return False
        else:
            connection.close()
            raise AssertionError(('disabled standby accepted startup', mode))
    return True


def exercise(base, root, executable, port, native, processes):
    source, standby = root / 'source', root / 'standby'
    for directory in (source, standby):
        directory.mkdir(mode=0o700)
    if native:
        (source / 'data').mkdir(mode=0o700)
    else:
        with (source / 'initdb.log').open('w') as output:
            subprocess.run([str(executable.parent / 'initdb'), '-D', str(source / 'data'),
                            '-A', 'trust', '-U', 'postgres', '--no-locale'],
                           stdout=output, stderr=output, check=True)
    publisher = peer.start(base, source, executable, port, 'server', native, processes)
    sql = peer.connect(port)
    try:
        sql.query('CREATE TABLE hot_standby_data (id integer PRIMARY KEY)')
        sql.query('INSERT INTO hot_standby_data VALUES (1)')
        if native:
            (standby / 'data').mkdir(mode=0o700)
            peer.control(base, source / 'control.sock',
                         f'(physical-base-backup "{standby / "data"}/" "hot_standby_test")')
        else:
            subprocess.run([str(executable.parent / 'pg_basebackup'), '-h', '127.0.0.1',
                            '-p', str(port), '-U', 'postgres', '-D', str(standby / 'data'),
                            '-R', '-X', 'stream', '-c', 'fast'], check=True, timeout=60)
        configuration = standby / 'data/postgresql.conf'
        configuration.write_text('hot_standby=off\n')
        target = peer.start(base, standby, executable, port + 1, 'off', native, processes, sql_ready=False)
        if native:
            peer.wait(lambda: (standby / 'control.sock').exists())
            peer.control(base, standby / 'control.sock', '(physical-recovery-enable-streaming)')
            peer.control(base, standby / 'control.sock',
                         f'(physical-follow-start "127.0.0.1" {port} "hot_standby_test")')
        peer.wait(lambda: disabled(port + 1), 60)
        sql.query('INSERT INTO hot_standby_data VALUES (2)')
        configuration.write_text('hot_standby=on\n')
        if native:
            assert 'reload complete' in peer.control(base, standby / 'control.sock', '(reload)')
            peer.control(base, standby / 'control.sock',
                         '(assert (and (not *enable-hot-standby*) '
                         '(getf (gethash "hot_standby" *startup-server-settings*) :pending-restart)))')
        else:
            target.send_signal(signal.SIGHUP)
        assert disabled(port + 1)
        peer.stop(base, standby, target)
        target = peer.start(base, standby, executable, port + 1, 'on', native, processes)
        reader = peer.connect(port + 1)
        try:
            assert reader.query('SHOW hot_standby') == [['on']]
            assert reader.query('SHOW in_hot_standby') == [['on']]
            assert reader.query(
                "SELECT setting, unit, context, vartype, pending_restart FROM pg_catalog.pg_settings "
                "WHERE name='hot_standby'") == [['on', None, 'postmaster', 'bool', 'f']]
            configuration.write_text('hot_standby=off\n')
            assert reader.query('SELECT pg_reload_conf()') == [['t']]
            peer.wait(lambda: reader.query(
                "SELECT pending_restart FROM pg_catalog.pg_settings WHERE name='hot_standby'") == [['t']], 120)
            assert reader.query('SHOW hot_standby') == [['on']]
        finally:
            reader.close()
        peer.stop(base, standby, target)
        target = peer.start(base, standby, executable, port + 1, 'off-again', native, processes, sql_ready=False)
        peer.wait(lambda: disabled(port + 1), 60)
        sql.query('INSERT INTO hot_standby_data VALUES (3)')
        endpoint = position(sql.query('SELECT pg_current_wal_flush_lsn()')[0][0])
        if native:
            def replayed():
                return 'HOT_REPLAY=T' in peer.control(
                    base, standby / 'control.sock',
                    f'(format t "HOT_REPLAY=~a~%" (and *last-replay-lsn* (>= *last-replay-lsn* {endpoint})))')
        else:
            def replayed():
                rows = sql.query('SELECT replay_lsn FROM pg_stat_replication')
                return bool(rows and rows[0][0] and position(rows[0][0]) >= endpoint)
        peer.wait(replayed, 60)
        utility = base / 'bin/pg_ctl' if native else executable.parent / 'pg_ctl'
        subprocess.run([str(utility), 'promote', '-D', str(standby / 'data'), '-w', '-t', '30'],
                       check=True, timeout=40)
        reader = peer.available(port + 1)
        try:
            assert reader.query('SHOW hot_standby') == [['off']]
            assert reader.query('SELECT pg_is_in_recovery()') == [['f']]
            assert reader.query('SELECT count(*) FROM hot_standby_data') == [['3']]
            reader.query('INSERT INTO hot_standby_data VALUES (4)')
        finally:
            reader.close()
        peer.stop(base, standby, target)
        peer.stop(base, source, publisher)
    finally:
        sql.close()
    print('hot-standby: pre-auth rejection, reload, restart, replay while off and promotion PASS', flush=True)


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
    base, root = args.base.resolve(), args.root.resolve()
    root.mkdir(mode=0o700)
    processes = []
    try:
        exercise(base, root, args.executable.resolve(), args.port, args.native, processes)
    finally:
        for name in ('standby', 'source'):
            registry = root / name / 'owned'
            if registry.exists():
                subprocess.run([sys.executable, str(base / 't/interop/owned-peer.py'),
                                'cleanup', str(registry)], check=True)
        for process in processes:
            process.wait(timeout=10)


if __name__ == '__main__':
    main()
