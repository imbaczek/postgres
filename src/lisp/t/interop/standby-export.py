"""Export a quiescent standby checkpoint, cold restore and resume following."""
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


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without -O.')

HERE = Path(__file__).resolve().parent
BASE = HERE.parents[1]
cold = runpy.run_path(str(HERE / 'source-fpw-cold.py'))
api = cold['api']


def checkpoint(connection):
    with connection.cursor() as cursor:
        cursor.execute('CHECKPOINT')


def follow_image(data, source, name, cleanup):
    (data / 'postgresql.conf').write_text('# Native standby checkpoint export.\n')
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        number = reservation.getsockname()[1]
    config = api['follow']['configuration'](data, number, data.parent)
    config['name'] = name
    node = api['Postgresql'](config, api['Null']())
    cleanup.callback(api['stop_follower'], node)
    member = api['Member'](0, 'source', None,
                           {'conn_url': f'postgres://127.0.0.1:{source.port}/postgres'})
    assert node.follow(member, timeout=90) is True
    return node


class NodeSource:
    def __init__(self, node, connection):
        self.node = node
        self.data = Path(node.data_dir)
        self.root = Path(tempfile.mkdtemp(prefix='sx-'))
        self.port = int(connection.get_dsn_parameters()['port'])

    def control(self, form):
        return cold['control'](self.node, form)


def check_child_source(node, connection, cleanup):
    connection = node.connection_pool.get('export_workload').get()
    # Patroni's driver appends search_path=pg_catalog to startup options.
    # Set the workload path after connecting, as an ordinary SQL client does.
    with connection.cursor() as cursor:
        cursor.execute('SET search_path TO public')
        cursor.execute('SHOW search_path')
        assert cursor.fetchone()[0] == 'public'
    source = NodeSource(node, connection)
    middle = api['start_follower'](connection, source, 'child_export_source', cleanup)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_create_physical_replication_slot('child_export_clone', true)")
        cursor.execute('INSERT INTO standby_export_probe VALUES (29)')
    data, exported = export_at_checkpoint(source, connection, middle, 'child_standby_export')
    assert exported['timeline'] > 1
    clone = follow_image(data, source, 'child_export_clone', cleanup)
    target = clone.connection_pool.get('heartbeat').get()
    assert api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 88
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO standby_export_probe VALUES (31)')
    api['follow']['wait_for'](
        lambda: api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 119)
    process = clone.is_running()
    previous = dict(pid=process.pid, birth=process.create_time())
    assert clone.stop(mode='fast', block_callbacks=True, stop_timeout=30) is True
    target = cold['restart'](clone, cold['PostgresqlRole'].REPLICA, previous)
    assert api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 119
    print('child-timeline standby export, cold restore and continued follow PASS', flush=True)


def rejected_export(standby, data, expected):
    data.mkdir(parents=True, mode=0o700)
    try:
        process = standby.is_running()
        result = subprocess.run(
            [os.environ['SBCL'], '--script', str(BASE / 't/control-client.lisp'),
             'unix', str(Path(standby.data_dir).parent / 'target.sock')],
            input=f'(assert (= (sb-posix:getpid) {process.pid}))\n'
                  f'(physical-standby-base-backup {cold["lisp_string"](str(data) + "/")} "fault")\n'
                  '(quit)\n', text=True, capture_output=True, check=True, timeout=45)
        assert ';; error:' in result.stdout, result.stdout
        assert expected in result.stdout, result.stdout
    finally:
        cold['control'](standby, '(setf *physical-bootstrap-test-hook* nil)')
    assert (data / 'recovery.required').is_file()
    assert not (data / 'physical-backup.manifest').exists()


def reject_interrupted_export(standby, data):
    cold['control'](standby,
                    '(setf *physical-bootstrap-test-hook* (lambda (point target) '
                    '(declare (ignore target)) (when (eq point :after-file) '
                    '(error "injected standby export failure"))))')
    rejected_export(standby, data, 'injected standby export failure')
    cold['control'](standby,
                    '(assert (handler-case (progn (recovery-read-manifest '
                    f'{cold["lisp_string"](str(data) + "/")}) nil) '
                    '(sb-posix:syscall-error (condition) '
                    '(= sb-posix:enoent (sb-posix:syscall-errno condition)))))')
    before = (data / 'physical-recovery.control').read_bytes()
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    result = subprocess.run([str(BASE / 'pg-lisp.sh'), '-D', str(data), '-p', str(port)],
                            text=True, capture_output=True, timeout=30)
    assert result.returncode != 0, (result.stdout, result.stderr)
    assert 'pg-lisp: fatal:' in result.stderr, (result.stdout, result.stderr)
    assert 'No such file or directory' in result.stderr, result.stderr
    (data.parent / 'interrupted-startup.log').write_text(result.stdout + result.stderr)
    assert (data / 'physical-recovery.control').read_bytes() == before
    assert (data / 'recovery.required').is_file()
    print('interrupted standby export remains guarded; cold startup rejected PASS', flush=True)


def reject_changed_catalog(standby, data):
    cold['control'](standby,
                    '(setf *physical-bootstrap-test-hook* (let ((done nil)) '
                    '(lambda (point target) (when (and (eq point :after-file) (not done) '
                    '(probe-file (merge-pathnames "global/pg_catalog.dat" target))) '
                    '(let* ((*pgdata* target) (*catalog* (load-catalog))) '
                    '(incf (catalog-next-oid *catalog*)) '
                    '(wal-atomic-write-bytes (catalog-path) (catalog-image-bytes)) '
                    '(setf done t))))))')
    rejected_export(standby, data, 'standby copied catalog differs from its frozen checkpoint state')
    print('changed target catalog rejected before manifest publication PASS', flush=True)


def export_at_checkpoint(source, connection, standby, name):
    data = source.root / name / 'data'
    data.mkdir(parents=True, mode=0o700)
    for _ in range(5):
        checkpoint(connection)
        source_checkpoint, _ = cold['read_copy'](source.data)
        end = source_checkpoint['end']
        api['follow']['wait_for'](lambda: cold['recovery'](standby)['applied'] >= end)
        if cold['recovery'](standby)['applied'] == end:
            break
    else:
        raise AssertionError('could not hold an exact source checkpoint boundary')
    target = standby.connection_pool.get('heartbeat').get()
    checkpoint(target)
    cold['control'](standby,
                    '(with-physical-store (*physical-receive-store*) '
                    '(let ((store (copy-physical-store *physical-receive-store*)) '
                    '(snapshot (copy-tree (getf *physical-recovery-state* :source-checkpoint)))) '
                    '(incf (physical-store-floor-digest store)) '
                    '(assert (handler-case (progn (physical-standby-backup-prefix store snapshot) nil) '
                    '(error (condition) (search "receive floor differs" (princ-to-string condition)))))))')
    reject_interrupted_export(standby, data.parent / 'interrupted')
    reject_changed_catalog(standby, data.parent / 'changed-catalog')
    cold['control'](standby, f'(physical-standby-base-backup {cold["lisp_string"](str(data) + "/")} "{name}")')
    exported, _ = cold['read_copy'](data)
    for key in ('start', 'end', 'timeline', 'next_xid', 'full_page_writes'):
        assert exported[key] == source_checkpoint[key], (key, exported, source_checkpoint)
    assert cold['decode']((data / 'physical-recovery.control').read_bytes())['phase'] == 0
    assert cold['recovery'](standby)['applied'] == end
    return data, exported


def check(policy):
    started = time.monotonic()
    source = api['peers']['OwnedLisp']()
    source.data.mkdir(mode=0o700)
    (source.data / 'postgresql.conf').write_text(f'full_page_writes={policy}\n')
    with ExitStack() as cleanup:
        cleanup.callback(source.stop, immediate=True)
        source.start()
        connection = api['peers']['connect'](source.port, source.process)
        cleanup.callback(connection.close)
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE standby_export_probe (n integer)')
            cursor.execute('INSERT INTO standby_export_probe VALUES (17)')
        middle = api['start_follower'](connection, source, 'export_source', cleanup)
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_create_physical_replication_slot('export_clone', true)")
            cursor.execute('INSERT INTO standby_export_probe VALUES (19)')
        data, exported = export_at_checkpoint(source, connection, middle, 'standby_export')
        clone = follow_image(data, source, 'export_clone', cleanup)
        target = clone.connection_pool.get('heartbeat').get()
        assert api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 36
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO standby_export_probe VALUES (23)')
        api['follow']['wait_for'](
            lambda: api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 59)
        original = middle.connection_pool.get('heartbeat').get()
        api['follow']['wait_for'](
            lambda: api['scalar'](original, 'SELECT SUM(n) FROM public.standby_export_probe') == 59)
        api['stop_follower'](middle)
        source.process.kill()
        assert source.process.wait(timeout=20) == -signal.SIGKILL
        process = clone.is_running()
        previous = dict(pid=process.pid, birth=process.create_time())
        assert clone.stop(mode='fast', block_callbacks=True, stop_timeout=30) is True
        target = cold['restart'](clone, cold['PostgresqlRole'].REPLICA, previous)
        assert api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 59
        assert api['scalar'](target, 'SELECT pg_promote(true, 30)') is True
        child, _ = cold['read_copy'](data)
        assert child['timeline'] > exported['timeline'] and child['full_page_writes'] is True
        process = clone.is_running()
        previous = dict(pid=process.pid, birth=process.create_time())
        clone.connection_pool.close()
        process.send_signal(signal.SIGKILL)
        assert process.wait(timeout=20) == -signal.SIGKILL
        target = cold['restart'](clone, cold['PostgresqlRole'].PRIMARY, previous)
        final, _ = cold['read_copy'](data)
        assert final['timeline'] == child['timeline']
        assert api['scalar'](target, 'SELECT SUM(n) FROM public.standby_export_probe') == 59
        check_child_source(clone, target, cleanup)
    result = dict(policy=policy, root=str(source.root),
                  elapsed_seconds=time.monotonic() - started, result='PASS')
    print(json.dumps(result), flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', choices=['on', 'off', 'both'], default='both')
    args = parser.parse_args()
    assert importlib.metadata.version('patroni') == '4.1.5'
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'cannot adopt owned native server processes')
    helper = Path(tempfile.mkdtemp(prefix='pg-standby-export-helper-')) / 'libpq-helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)], check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    for policy in ['on', 'off'] if args.policy == 'both' else [args.policy]:
        check(policy)
