"""Check C concurrent and native quiescent primary backups with FPW off."""
import argparse
from contextlib import ExitStack
import ctypes
import importlib.metadata
import json
import os
from pathlib import Path
import runpy
import signal
import socket
import subprocess
import tempfile
import time

import psutil


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without -O.')

HERE = Path(__file__).resolve().parent
BASE = HERE.parents[1]
cold = runpy.run_path(str(HERE / 'source-fpw-cold.py'))
api = cold['api']


def port():
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        return reservation.getsockname()[1]


def stop(process):
    children = []
    identities = []
    if process.poll() is None:
        children = psutil.Process(process.pid).children(recursive=True)
        identities = [dict(pid=child.pid, birth=child.create_time()) for child in children]
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    for child in children:
        if child.is_running():
            try:
                child.wait(timeout=10)
            except psutil.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
        assert not child.is_running(), (child.pid, 'owned C child remains')
    print(json.dumps(dict(stopped=process.pid, status=process.poll(),
                          children=identities)), flush=True)
    return process.poll()


def start_c(binary, data, root, cleanup):
    number = port()
    with (root / f'server-{number}.log').open('ab') as log:
        process = subprocess.Popen([str(binary / 'postgres'), '-D', str(data),
                                    '-p', str(number), '-h', '127.0.0.1', '-k', str(root)],
                                   stdout=log, stderr=log)
    cleanup.callback(stop, process)
    connection = api['peers']['connect'](number, process)
    cleanup.callback(connection.close)
    return process, connection, number


def seed(connection):
    assert api['scalar'](connection, 'SHOW full_page_writes') == 'off'
    with connection.cursor() as cursor:
        cursor.execute('CREATE TABLE primary_off_probe (n integer)')
        cursor.execute('INSERT INTO primary_off_probe VALUES (17)')
        cursor.execute('CHECKPOINT')


def write_c(binary, data, connection, value, relation, root, label):
    start = api['scalar'](connection, 'SELECT pg_current_wal_insert_lsn()')
    with connection.cursor() as cursor:
        cursor.execute('UPDATE primary_off_probe SET n=%s', (value,))
    end = api['scalar'](connection, 'SELECT pg_current_wal_flush_lsn()')
    result = subprocess.run([str(binary / 'pg_waldump'), '--path', str(data / 'pg_wal'),
                             '--start', start, '--end', end, '--relation', relation,
                             '--fullpage'], text=True, capture_output=True, check=True, timeout=20)
    (root / f'{label}.waldump').write_text(result.stdout + result.stderr)
    return [line for line in result.stdout.splitlines() if line.startswith('rmgr:')]


def check_c(binary):
    started = time.monotonic()
    root = Path(tempfile.mkdtemp(prefix='pg-primary-off-c-'))
    data, backup = root / 'data', root / 'backup'
    with (root / 'initdb.log').open('w') as log:
        subprocess.run([str(binary / 'initdb'), '-D', str(data), '-A', 'trust',
                        '-U', 'postgres', '--no-locale', '--encoding=UTF8', '--no-data-checksums'],
                       stdout=log, stderr=log, check=True, timeout=60)
    (data / 'postgresql.auto.conf').write_text('full_page_writes=off\n')
    with ExitStack() as cleanup:
        source, connection, number = start_c(binary, data, root, cleanup)
        seed(connection)
        assert api['scalar'](connection,
                             'SELECT full_page_writes FROM pg_control_checkpoint()') is False
        path = api['scalar'](connection, "SELECT pg_relation_filepath('public.primary_off_probe')")
        _, database, relfilenode = path.split('/')
        relation = f'1663/{database}/{relfilenode}'
        assert not write_c(binary, data, connection, 18, relation, root, 'before-backup')
        with (root / 'basebackup.log').open('w') as log:
            copier = subprocess.Popen([str(binary / 'pg_basebackup'), '-h', '127.0.0.1',
                                       '-p', str(number), '-U', 'postgres', '-D', str(backup),
                                       '--wal-method=stream', '--checkpoint=fast', '--max-rate=1M',
                                       '--no-password', '--no-clean'], stdout=log, stderr=log)
            cleanup.callback(stop, copier)
            def copying():
                assert copier.poll() is None, 'backup ended before concurrent write'
                with connection.cursor() as cursor:
                    cursor.execute('SELECT phase FROM pg_stat_progress_basebackup')
                    return ('streaming database files',) in cursor.fetchall()
            api['follow']['wait_for'](copying, seconds=30)
            images = write_c(binary, data, connection, 19, relation, root, 'during-backup')
            assert images, 'OFF primary did not force an image during backup'
            assert copier.wait(timeout=90) == 0
        with connection.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        assert not write_c(binary, data, connection, 20, relation, root, 'after-backup')
        source_identity = (source.pid, psutil.Process(source.pid).create_time())
        assert stop(source) == 0
        restored, target, _ = start_c(binary, backup, root, cleanup)
        assert (restored.pid, psutil.Process(restored.pid).create_time()) != source_identity
        assert api['scalar'](target, 'SELECT pg_is_in_recovery()') is False
        assert api['scalar'](target, 'SHOW full_page_writes') == 'off'
        assert api['scalar'](target,
                             'SELECT full_page_writes FROM pg_control_checkpoint()') is False
        assert api['scalar'](target, 'SELECT n FROM public.primary_off_probe') == 19
    result = dict(peer='C', root=str(root), elapsed_seconds=time.monotonic() - started)
    print(json.dumps(result), flush=True)
    return result


def check_native():
    started = time.monotonic()
    source = api['peers']['OwnedLisp']()
    source.data.mkdir(mode=0o700)
    (source.data / 'postgresql.conf').write_text('full_page_writes=off\n')
    with ExitStack() as cleanup:
        cleanup.callback(source.stop, immediate=True)
        source.start()
        connection = api['peers']['connect'](source.port, source.process)
        cleanup.callback(connection.close)
        seed(connection)
        node = api['start_follower'](connection, source, 'primary_off', cleanup)
        base, _ = cold['read_copy'](Path(node.data_dir))
        assert base['full_page_writes'] is False
        fact = cold['recovery'](node)['fpw_fact']
        assert fact['enabled'] is False and fact['witness_kind'] == 'checkpoint', fact
        source.process.kill()
        assert source.process.wait(timeout=20) == -signal.SIGKILL
        process = node.is_running()
        previous = dict(pid=process.pid, birth=process.create_time())
        assert node.stop(mode='fast', block_callbacks=True, stop_timeout=30) is True
        target = cold['restart'](node, cold['PostgresqlRole'].REPLICA, previous)
        assert cold['recovery'](node)['fpw_fact']['enabled'] is False
        assert api['scalar'](target, 'SELECT pg_is_in_recovery()') is True
        assert api['scalar'](target, 'SELECT n FROM public.primary_off_probe') == 17
        assert api['scalar'](target, 'SELECT pg_promote(true, 30)') is True
        child, _ = cold['read_copy'](Path(node.data_dir))
        assert child['timeline'] > base['timeline'] and child['full_page_writes'] is True
        assert api['scalar'](target, 'SELECT n FROM public.primary_off_probe') == 17
        process = node.is_running()
        previous = dict(pid=process.pid, birth=process.create_time())
        node.connection_pool.close()
        process.send_signal(signal.SIGKILL)
        assert process.wait(timeout=20) == -signal.SIGKILL
        target = cold['restart'](node, cold['PostgresqlRole'].PRIMARY, previous)
        restarted, _ = cold['read_copy'](Path(node.data_dir))
        assert restarted['timeline'] == child['timeline']
        assert restarted['full_page_writes'] is True
        assert api['scalar'](target, 'SELECT pg_is_in_recovery()') is False
        assert api['scalar'](target, 'SELECT n FROM public.primary_off_probe') == 17
    result = dict(peer='native', root=str(source.root),
                  elapsed_seconds=time.monotonic() - started)
    print(json.dumps(result), flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    args = parser.parse_args()
    binary = args.reference_bin.resolve()
    os.environ.setdefault('PG_CONFIG', str(binary / 'pg_config'))
    os.environ['PATH'] = str(binary) + os.pathsep + os.environ['PATH']
    assert importlib.metadata.version('patroni') == '4.1.5'
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'cannot adopt owned native server processes')
    helper = Path(tempfile.mkdtemp(prefix='pg-primary-off-helper-')) / 'libpq-helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)], check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    results = [check_c(binary), check_native()]
    print(json.dumps(dict(result='PASS', cases=results)), flush=True)
