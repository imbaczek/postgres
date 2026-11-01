"""Crash primaries before/after ACK and cold-recover credited native followers.

C primaries use immediate shutdown (no shutdown checkpoint). Native primaries
and followers use SIGKILL. C scripted receivers do not prove remote replay;
the native after-ACK case proves both real followers recover with no source.
"""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import traceback

import psutil


ack = runpy.run_path(str(Path(__file__).with_name('synchronous-ack.py')))
api, BASE = ack['api'], ack['BASE']


def stop_process(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=20)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)


def eof_without_success(actor):
    seen = []
    try:
        while True:
            kind, _ = actor.receive()
            seen.append(kind)
            assert kind != b'C', ('unacknowledged commit reported success', seen)
    except (EOFError, ConnectionResetError):
        pass


def restart_c(binary, data, port, cleanup):
    log = cleanup.enter_context((data.parent / 'crash-restart.log').open('ab'))
    process = subprocess.Popen([str(binary / 'postgres'), '-D', str(data), '-p', str(port),
                                '-h', '127.0.0.1', '-k', str(data.parent)],
                               stdout=log, stderr=log)
    cleanup.callback(stop_process, process)
    connection = api['peers']['connect'](port, process)
    cleanup.callback(connection.close)
    return connection


def cold_recover(receiver, row_id):
    node = receiver.node
    process = node.is_running()
    assert process is not None
    node.connection_pool.close()
    process.kill()
    process.wait(timeout=20)
    assert not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
    receiver.control_path.unlink(missing_ok=True)
    assert node.start(timeout=90, role=api['PostgresqlRole'].REPLICA) is True
    port = int(node.connection_pool.conn_kwargs['port'])
    assert api['follow']['query'](port, 'SELECT pg_is_in_recovery()') == [(True,)]
    assert api['follow']['query'](port,
        f'SELECT id FROM synchronous_ack_rows WHERE id={row_id}') == [(row_id,)]


def exercise(connection, server, binary, after_ack):
    label = 'native' if server is not None else 'C'
    cleanup = ExitStack()
    source = ack['Source'](connection, server)
    receivers = {}
    actor = None
    source_down = False
    try:
        source.rows('CREATE TABLE synchronous_ack_rows (id integer PRIMARY KEY)')
        directory = Path(tempfile.mkdtemp(prefix='pg-crash-ack-receivers-'))
        for name in ('sync_a', 'sync_b'):
            if server is None:
                receiver = ack['CReceiver'](connection, name, directory)
                cleanup.callback(receiver.close)
            else:
                receiver = ack['NativeReceiver'](connection, server, name, cleanup)
            receivers[name] = receiver
        ack['wait'](lambda: set(source.statuses()) == set(receivers)
                    and all(row[2] and row[2] > 0 for row in source.statuses().values()),
                    'positive flush for both independent receivers')
        api['configure'](connection, 'ANY 2 (sync_a,sync_b)')
        for receiver in receivers.values():
            receiver.arm()
        actor = ack['wire']['QueryConnection'](source.port, 'crash-ack')
        actor.socket.settimeout(10)
        actor.query('SET synchronous_commit = on')
        ack['send_commit'](actor, 1, 'simple')
        boundary = ack['wait'](lambda: source.boundary(actor, 'on'), 'locally durable sync waiter')
        assert api['lsn_number'](api['scalar'](connection, 'SELECT pg_current_wal_flush_lsn()')) >= boundary
        for receiver in receivers.values():
            ack['wait'](lambda: receiver.arrived(boundary), 'actual durable receiver progress')
        ack['pending'](actor, source, 'on', boundary)
        if after_ack:
            for name, receiver in receivers.items():
                receiver.release(2)
                ack['wait'](lambda: source.statuses()[name][2] >= boundary, 'credited receiver flush')
            ack['complete'](actor)
            actor.close()
            actor = None
        data = Path(api['scalar'](connection, 'SHOW data_directory'))
        if server is None:
            subprocess.run([str(binary / 'pg_ctl'), '-D', str(data), '-m', 'immediate',
                            '-w', 'stop'], check=True, timeout=30, capture_output=True)
        else:
            process = server.process
            server.stop(immediate=True)
            assert process.returncode == -signal.SIGKILL
        source_down = True
        if actor is not None:
            eof_without_success(actor)
        if server is not None and after_ack:
            for receiver in receivers.values():
                cold_recover(receiver, 1)
            assert server.process is None
            print('native: ACKed ANY2 flush survives source SIGKILL and both follower cold restarts '
                  'while source stays down PASS', flush=True)
        else:
            if server is None:
                recovered = restart_c(binary, data, source.port, cleanup)
            else:
                for receiver in receivers.values():
                    receiver.release(3)
                server.start()
                recovered = api['peers']['connect'](source.port, server.process)
                cleanup.callback(recovered.close)
            source.connection = recovered
            source_down = False
            assert source.rows('SELECT id FROM synchronous_ack_rows') == [(1,)]
            print(f'{label}: {"after" if after_ack else "before"} ACK crash preserves locally '
                  'durable row on same-PGDATA restart PASS', flush=True)
    finally:
        failed = sys.exc_info()[0] is not None
        if actor is not None:
            cleanup.callback(actor.close)
        if not source_down:
            cleanup.callback(api['configure'], source.connection, '')
        try:
            cleanup.close()
        except Exception:
            if not failed:
                raise
            traceback.print_exc()


def check(native, reference, server, binary, after_ack):
    exercise(reference, None, binary, after_ack)
    exercise(native, server, binary, after_ack)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    arguments = parser.parse_args()
    binary = arguments.reference_bin.resolve()
    os.environ.setdefault('PG_CONFIG', str(binary / 'pg_config'))
    os.environ['PATH'] = str(binary) + os.pathsep + os.environ['PATH']
    helper = Path(tempfile.mkdtemp(prefix='pg-crash-ack-libpq-')) / 'helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)], check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    for after_ack in (False, True):
        api['peers']['run'](binary,
            lambda native, reference, server: check(native, reference, server, binary, after_ack),
            'sync-crash-after' if after_ack else 'sync-crash-before', with_server=True)
