"""Clone over TCP with unchanged Patroni, then cold-start and follow native WAL."""
import argparse
from contextlib import ExitStack
import importlib.metadata
import os
from pathlib import Path
import runpy
import shlex
import socket
import subprocess

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.async_executor import CriticalTask
from patroni.postgresql.misc import PostgresqlRole
from patroni.postgresql.mpp import Null


api = runpy.run_path(str(Path(__file__).with_name('patroni-sync-monitor.py')))
follow = api['follow']
BASE = Path(__file__).resolve().parents[2]


def clone_promoted(source, promoted, cleanup):
    source.stop(immediate=True)
    assert promoted.promote(30, CriticalTask()) is True
    port = int(promoted.connection_pool.conn_kwargs['port'])
    follow['query'](port, 'INSERT INTO clone_rows VALUES (4)')
    promoted.query("SELECT pg_create_physical_replication_slot('clone_two',true)")
    root = source.root / 'child_replica'
    root.mkdir(mode=0o700)
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        child_port = reservation.getsockname()[1]
    config = follow['configuration'](source.data, child_port, root)
    config.update(name='clone_two', data_dir=str(root / 'data'))
    wrapper = root / 'postgres'
    wrapper.write_text(wrapper.read_text().rstrip('\n') + ' >> '
                       + shlex.quote(str(root / 'server.log')) + ' 2>&1\n')
    child = Postgresql(config, Null())
    cleanup.callback(api['stop_follower'], child)
    member = Member(0, 'promoted', None, {'conn_url': f'postgres://127.0.0.1:{port}/postgres'})
    assert child.bootstrap.clone(member) is True
    follow['query'](port, 'INSERT INTO clone_rows VALUES (5)')
    assert child.follow(member, timeout=90) is True
    follow['wait_for'](lambda: child.query('SELECT id FROM public.clone_rows ORDER BY id') == [
        (1,), (2,), (3,), (4,), (5,)])
    assert child.query('SELECT received_tli FROM pg_stat_get_wal_receiver()') == [(2,)]
    print('native-basebackup: unchanged clone and follow from promoted timeline 2 PASS', flush=True)


def check(binary):
    assert importlib.metadata.version('patroni') == '4.1.5'
    os.environ['PG_CONFIG'] = str(binary / 'pg_config')
    os.environ['PATH'] = str(binary) + os.pathsep + os.environ['PATH']
    source = api['peers']['OwnedLisp']()
    os.environ.pop('PG_LIBPQ_HELPER', None)
    helper = subprocess.check_output([str(BASE / 'bin/pg-libpq-helper-path')],
                                     text=True, timeout=65).rstrip('\n')
    # Only the wire fixture needs an explicit executable. Production children
    # inherit no override and must discover the helper independently.
    os.environ['PG_LIBPQ_HELPER'] = helper
    try:
        importlib.import_module('libpq-helper-tests')
    finally:
        os.environ.pop('PG_LIBPQ_HELPER')
    connection = None
    try:
        source.start()
        connection = api['peers']['connect'](source.port, source.process)
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE clone_rows (id integer PRIMARY KEY)')
            cursor.execute('INSERT INTO clone_rows VALUES (1)')
            cursor.execute("SELECT pg_create_physical_replication_slot('clone_one',true)")
        faults = runpy.run_path(str(Path(__file__).with_name('native-basebackup-faults.py')))
        faults['check_live'](BASE, source, follow)
        faults['check'](BASE, source.root, source.port)
        root = source.root / 'replica'
        root.mkdir(mode=0o700)
        data = root / 'data'
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        config = follow['configuration'](source.data, port, root)
        config.update(name='clone_one', data_dir=str(data))
        wrapper = root / 'postgres'
        wrapper.write_text(wrapper.read_text().rstrip('\n') + ' >> '
                           + shlex.quote(str(root / 'server.log')) + ' 2>&1\n')
        node = Postgresql(config, Null())
        with ExitStack() as cleanup:
            cleanup.callback(api['stop_follower'], node)
            member = Member(0, 'source', None,
                            {'conn_url': f'postgres://127.0.0.1:{source.port}/postgres'})
            assert node.bootstrap.clone(member) is True
            assert (data / 'physical-backup.manifest').is_file()
            assert (data / 'postgresql.conf').is_file()
            stat = source.data.stat()
            prefix = f'.pg-native-backups-{stat.st_dev:X}-{stat.st_ino:X}-'
            stages, = source.data.parent.glob(prefix + '*')
            assert sorted(path.name for path in stages.iterdir()) == ['.owner']
            follow['control'](source.process, source.control_socket,
                              '(progn (defparameter pg::*orphan-backup-stage* '
                              '(pg::physical-backup-create-stage)) '
                              '(with-open-file (out (merge-pathnames "payload" pg::*orphan-backup-stage*) '
                              ':direction :output :element-type (quote (unsigned-byte 8))) '
                              '(write-sequence (make-array 65536 :element-type (quote (unsigned-byte 8)) '
                              ':initial-element 7) out) (pg::fsync-stream out)) (values))')
            orphan = next(path for path in stages.iterdir() if path.name.startswith('stage.'))
            assert (orphan / 'payload').stat().st_size == 65536
            connection.close()
            connection = None
            source.stop(immediate=True)
            assert orphan.exists()
            source.start()
            connection = api['peers']['connect'](source.port, source.process)
            assert not orphan.exists()
            assert sorted(path.name for path in stages.iterdir()) == ['.owner']
            print('native-basebackup: source crash orphan swept before listener startup PASS', flush=True)
            with connection.cursor() as cursor:
                cursor.execute('SELECT slot_name FROM pg_replication_slots')
                assert cursor.fetchall() == [('clone_one',)]
                cursor.execute('INSERT INTO clone_rows VALUES (2)')
            assert node.follow(member, timeout=90) is True
            follow['wait_for'](lambda: node.query('SELECT id FROM public.clone_rows ORDER BY id') == [(1,), (2,)])
            assert node.query('SELECT status,slot_name FROM pg_stat_get_wal_receiver()') == [
                ('streaming', 'clone_one')]
            assert 'PG_LIBPQ_HELPER' not in os.environ
            print('native-basebackup: CLI and native follow discover helper without an override PASS', flush=True)
            print('native-basebackup: unchanged Patroni clone, cold startup and post-backup follow PASS', flush=True)
            process = node.is_running()
            assert process is not None
            node.connection_pool.close()
            process.kill()
            process.wait(timeout=20)
            (root / 'target.sock').unlink(missing_ok=True)
            assert node.start(timeout=90, role=PostgresqlRole.REPLICA) is True
            assert node.is_running().pid != process.pid
            follow['wait_for'](lambda: node.query('SELECT id FROM public.clone_rows ORDER BY id') == [(1,), (2,)])
            with connection.cursor() as cursor:
                cursor.execute('INSERT INTO clone_rows VALUES (3)')
            follow['wait_for'](lambda: node.query('SELECT id FROM public.clone_rows ORDER BY id') == [(1,), (2,), (3,)])
            print('native-basebackup: cold restart and continued streaming PASS', flush=True)
            connection.close()
            connection = None
            clone_promoted(source, node, cleanup)
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            source.stop(immediate=True)
            print(f'artifacts: {source.root}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    check(parser.parse_args().reference_bin.resolve())
