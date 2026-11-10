"""Crash native source-FPW replay and promotion using complete owned PGDATA.

Use pinned Patroni's PostgreSQL control layer without a DCS or HA loop.
The default case publishes policy through the control REPL; --configured
uses server configuration and --live-promotion tests the live child path.
"""
import argparse
from contextlib import ExitStack
import ctypes
import importlib.metadata
import json
import os
from pathlib import Path
import runpy
import signal
import subprocess
import tempfile
import time

import psutil
import psycopg2
from patroni.postgresql.misc import PostgresqlRole


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without -O.')

HERE = Path(__file__).resolve().parent
BASE = HERE.parents[1]
api = runpy.run_path(str(HERE / 'patroni-sync-monitor.py'))
decode = runpy.run_path(str(HERE / 'recovery-control-state.py'))['decode']
read_copy = runpy.run_path(str(HERE / 'checkpoint-snapshot.py'))['read_copy']


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def control(node, form):
    output = api['follow']['control'](
        node.is_running(), Path(node.data_dir).parent / 'target.sock', form)
    assert ';; read error:' not in output, output
    return output


def recovery(node):
    return decode((Path(node.data_dir) / 'physical-recovery.control').read_bytes())


def check_source_off(node, start, end):
    facts = recovery(node)
    fact = facts['fpw_fact']
    assert facts['applied'] >= end, facts
    assert fact['enabled'] is False and fact['witness_kind'] == 'change', fact
    assert (fact['witness_start'], fact['witness_end']) == (start, end), fact
    assert fact['last_disable_start'] == start, fact
    assert fact['witness_timeline'] == fact['last_disable_timeline'] == 1, fact
    return fact


def await_exit(node, marker, process, previous, expected_status):
    assert process is not None
    api['follow']['wait_for'](lambda: marker.is_file(), seconds=60)
    status = process.wait(timeout=20)
    assert status == expected_status, (status, expected_status, previous)
    assert not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
    node.connection_pool.close()
    return dict(previous, observed_exit_status=status)


def restart(node, role, previous):
    node.connection_pool.close()
    (Path(node.data_dir).parent / 'target.sock').unlink(missing_ok=True)
    assert node.start(timeout=90, role=role) is True
    process = node.is_running()
    assert process is not None
    assert (process.pid, process.create_time()) != (previous['pid'], previous['birth'])
    return node.connection_pool.get('heartbeat').get()


def arm_replay_cut(node, point, marker):
    control(node, f'''(let ((policy *wal-fpw-state*) (owner *wal-storage-incarnation*))
      (setf *physical-replay-test-hook*
        (lambda (point record)
          (when (and (eq point :{point})
                     (= (wal-record-type record) +wal-record-full-page-writes-change+))
            (assert (eq policy *wal-fpw-state*))
            (assert (eq owner *wal-storage-incarnation*))
            (with-open-file (out {lisp_string(marker)} :direction :output :if-exists :error)
              (format out "~d ~d~%" (wal-record-start record) (wal-record-end record)))
            (sb-ext:exit :code 93 :abort t)))))''')


def interrupted_promotion(node, connection, root, configured=False):
    marker = root / 'promotion.cut'
    control(node, f'''(setf *physical-promotion-test-hook*
      (lambda (point)
        (when (eq point :after-child-eor-flush)
          (with-open-file (out {lisp_string(marker)} :direction :output :if-exists :error)
            (write-line "after-child-eor-flush" out))
          (sb-ext:exit :code 94 :abort t))))''')
    process = node.is_running()
    assert process is not None
    previous = dict(pid=process.pid, birth=process.create_time())
    try:
        api['scalar'](connection, 'SELECT pg_promote(true, 30)')
    except psycopg2.Error:
        pass
    else:
        raise AssertionError('promotion fault did not interrupt the SQL waiter')
    exited = await_exit(node, marker, process, previous, 94)
    assert exited['pid'] == previous['pid']
    if configured:
        # The pending child must keep frozen OFF intent despite new startup ON.
        (Path(node.data_dir) / 'postgresql.auto.conf').write_text('full_page_writes=on\n')
    connection = restart(node, PostgresqlRole.PRIMARY, exited)
    assert api['scalar'](connection, 'SELECT pg_is_in_recovery()') is False
    checkpoint, _ = read_copy(Path(node.data_dir))
    assert checkpoint['timeline'] > 1, checkpoint
    assert checkpoint['full_page_writes'] is (not configured), checkpoint
    control(node, '(assert (wal-fpw-state-enabled *wal-fpw-state*))')
    return connection, checkpoint, exited


def check(point, configured=False, live_promotion=False):
    started = time.monotonic()
    source = api['peers']['OwnedLisp']()
    result = dict(point=point, configured=configured, live_promotion=live_promotion,
                  root=str(source.root), checks=[], processes=[])
    with ExitStack() as cleanup:
        cleanup.callback(source.stop, immediate=True)
        source.start()
        connection = api['peers']['connect'](source.port, source.process)
        cleanup.callback(connection.close)
        if not configured:
            source.control('(wal-report-full-page-writes t)')
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE fpw_cold_probe (n integer)')
            cursor.execute('INSERT INTO fpw_cold_probe VALUES (17)')
            cursor.execute('CHECKPOINT')
        node = api['start_follower'](connection, source, 'fpw_cold', cleanup)
        target = node.connection_pool.get('heartbeat').get()
        assert api['scalar'](target, 'SELECT count(*) FROM public.fpw_cold_probe') == 1
        assert recovery(node)['fpw_fact']['enabled'] is True
        if configured:
            (Path(node.data_dir) / 'postgresql.auto.conf').write_text('full_page_writes=off\n')
            node.is_running().send_signal(signal.SIGHUP)
            api['follow']['wait_for'](
                lambda: api['scalar'](target, 'SHOW full_page_writes') == 'off')
            assert recovery(node)['fpw_fact']['enabled'] is True
        with target.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        marker = source.root / 'replay.cut'
        arm_replay_cut(node, point, marker)
        process = node.is_running()
        assert process is not None
        previous = dict(pid=process.pid, birth=process.create_time())
        if configured:
            (source.data / 'postgresql.auto.conf').write_text('full_page_writes=off\n')
            source.process.send_signal(signal.SIGHUP)
        else:
            source.control('(wal-report-full-page-writes nil)')
        exited = await_exit(node, marker, process, previous, 93)
        result['processes'].append(exited)
        start, end = map(int, marker.read_text().split())
        before = recovery(node)
        if point == 'after-redo':
            assert before['applied'] < end and before['fpw_fact']['enabled'] is True, before
        else:
            check_source_off(node, start, end)
        source.process.kill()
        assert source.process.wait(timeout=20) == -signal.SIGKILL
        target = restart(node, PostgresqlRole.REPLICA, exited)
        check_source_off(node, start, end)
        assert api['scalar'](target, 'SELECT pg_is_in_recovery()') is True
        assert api['scalar'](target, 'SELECT count(*) FROM public.fpw_cold_probe') == 1
        result['checks'].append('source-offline-cold-replica-replays-exact-off-witness')
        print(f'{point}: source-offline cold replica PASS', flush=True)
        process = node.is_running()
        previous = dict(pid=process.pid, birth=process.create_time())
        assert node.stop(mode='fast', block_callbacks=True, stop_timeout=30) is True
        assert recovery(node)['state'] == 2  # DB_SHUTDOWNED_IN_RECOVERY
        target = restart(node, PostgresqlRole.REPLICA, previous)
        check_source_off(node, start, end)
        result['checks'].append('fast-shutdown-and-second-cold-replica-preserve-off')
        if live_promotion:
            control(node, '''(setf *physical-promotion-test-hook*
              (lambda (point)
                (when (member point '(:after-child-primary-initialization :after-child-primary-ready))
                  (assert (sb-thread:holding-mutex-p *physical-follow-generation-lock*)))))''')
            assert api['scalar'](target, 'SELECT pg_promote(true, 30)') is True
            checkpoint, _ = read_copy(Path(node.data_dir))
            assert checkpoint['full_page_writes'] is (not configured)
            control(node, f'''(let ((state *wal-fpw-state*))
              (assert (eq :checkpoint (wal-fpw-state-witness-kind state)))
              (assert (= {checkpoint['end']} (wal-fpw-state-end state))))''')
            result['checks'].append('live-promotion-keeps-exact-child-checkpoint-policy-witness')
        else:
            target, checkpoint, promoted_exit = interrupted_promotion(node, target, source.root, configured)
            result['processes'].append(promoted_exit)
            result['checks'].append('cold-pending-promotion-finishes-local-on-policy')
        assert api['scalar'](target, 'SELECT count(*) FROM public.fpw_cold_probe') == 1
        process = node.is_running()
        previous = dict(pid=process.pid, birth=process.create_time())
        node.connection_pool.close()
        process.send_signal(signal.SIGKILL)
        assert process.wait(timeout=20) == -signal.SIGKILL
        target = restart(node, PostgresqlRole.PRIMARY, previous)
        current, _ = read_copy(Path(node.data_dir))
        assert current['timeline'] == checkpoint['timeline']
        assert current['full_page_writes'] is (not configured)
        enabled = 'nil' if configured and live_promotion else 't'
        control(node, f'(assert (eql {enabled} (wal-fpw-state-enabled *wal-fpw-state*)))')
        assert api['scalar'](target, 'SELECT count(*) FROM public.fpw_cold_probe') == 1
        result['checks'].append('independent-child-cold-restart-preserves-policy-and-data')
    result['elapsed_seconds'] = time.monotonic() - started
    (source.root / 'source-fpw-cold-results.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--point', choices=['after-redo', 'after-progress', 'both'], default='both')
    parser.add_argument('--configured', action='store_true',
                        help='publish through SIGHUP and change local policy during pending promotion')
    parser.add_argument('--live-promotion', action='store_true',
                        help='keep the promoted process alive and verify its exact checkpoint witness')
    args = parser.parse_args()
    assert importlib.metadata.version('patroni') == '4.1.5'
    # Adopt daemonized native servers so waitpid proves each injected exit code.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER (Linux)
        raise OSError(ctypes.get_errno(), 'cannot adopt owned server processes')
    helper = Path(tempfile.mkdtemp(prefix='pg-fpw-cold-helper-')) / 'libpq-helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)], check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    points = ['after-redo', 'after-progress'] if args.point == 'both' else [args.point]
    for point in points:
        check(point, configured=args.configured, live_promotion=args.live_promotion)
