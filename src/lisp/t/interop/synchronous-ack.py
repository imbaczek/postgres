"""Compare public FIRST/ANY ACK credit with independent C and native receivers.

C clients retain received WAL bytes but only simulate apply credit. Native
followers receive, flush and replay normally; their transport reports capped
prefixes. Neither fixture edits the primary's sender registry or wait queues.
"""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import re
import runpy
import select
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection


api = runpy.run_path(str(Path(__file__).with_name('patroni-sync-monitor.py')))
wire = runpy.run_path(str(Path(__file__).with_name('subqueries.py')))
extended = runpy.run_path(str(Path(__file__).with_name('binary_values.py')))
BASE = Path(__file__).resolve().parents[2]


def wait(predicate, label, timeout=20):
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(label)
        time.sleep(0.02)


class CReceiver:
    def __init__(self, connection, name, directory):
        self.cleanup = ExitStack()
        self.dsn, self.name = connection.dsn, name
        try:
            self.sender = psycopg2.connect(self.dsn, application_name=name,
                                           connection_factory=PhysicalReplicationConnection)
            self.cleanup.callback(self.sender.close)
            self.cursor = self.sender.cursor()
            self.start = api['lsn_number'](api['scalar'](connection, 'SELECT pg_current_wal_flush_lsn()'))
            self.actual = self.start
            self.reported = [self.start] * 3
            self.output = (directory / (name + '.wal')).open('w+b')
            self.cleanup.callback(self.output.close)
            self.cursor.start_replication(start_lsn=self.start)
            self.release(3)
        except BaseException:
            self.cleanup.close()
            raise

    def arm(self):
        pass

    def arrived(self, boundary):
        while True:
            message = self.cursor.read_message()
            if message is None:
                return self.actual >= boundary
            begin = message.data_start
            assert begin <= self.actual, (begin, self.actual)
            self.output.seek(begin - self.start)
            self.output.write(message.payload)
            self.output.flush()
            os.fsync(self.output.fileno())
            self.actual = max(self.actual, begin + len(message.payload))

    def release(self, level):
        for index in range(level):
            self.reported[index] = self.actual
        self.cursor.send_feedback(write_lsn=self.reported[0], flush_lsn=self.reported[1],
                                  apply_lsn=self.reported[2], force=True)

    def close(self):
        self.cleanup.close()

    def disconnect(self):
        self.sender.close()

    def reconnect(self):
        self.sender = psycopg2.connect(self.dsn, application_name=self.name,
                                       connection_factory=PhysicalReplicationConnection)
        self.cleanup.callback(self.sender.close)
        self.cursor = self.sender.cursor()
        self.cursor.start_replication(start_lsn=self.actual)
        self.release(3)


class NativeReceiver:
    def __init__(self, connection, server, name, cleanup):
        self.node = api['start_follower'](connection, server, name, cleanup)
        self.control_path = server.root / name / 'target.sock'
        self.control(f'(load "{BASE / "t/syncrep-feedback-native.lisp"}")')
        wait(lambda: ':FEEDBACK-READY' in self.control(
            '(when *syncrep-test-feedback-reported* :feedback-ready)').upper(),
             'first independent native feedback')

    def control(self, form):
        return api['follow']['control'](self.node.is_running(), self.control_path, form)

    def arm(self):
        self.control('(syncrep-test-arm-feedback)')

    def arrived(self, boundary):
        return ':FEEDBACK-ARRIVED' in self.control(
            '(sb-thread:with-mutex (*syncrep-test-feedback-lock*) '
            f'(when (and *syncrep-test-feedback-actual* '
            f'(every (lambda (value) (>= value {boundary})) *syncrep-test-feedback-actual*)) '
            ':feedback-arrived))').upper()

    def release(self, level):
        self.control(f'(syncrep-test-release-feedback {level})')

    def disconnect(self):
        api['stop_follower'](self.node)

    def reconnect(self):
        self.control_path.unlink(missing_ok=True)
        assert self.node.start(timeout=90, role=api['PostgresqlRole'].REPLICA) is True
        self.control(f'(load "{BASE / "t/syncrep-feedback-native.lisp"}")')
        wait(lambda: ':FEEDBACK-READY' in self.control(
            '(when *syncrep-test-feedback-reported* :feedback-ready)').upper(),
             'fresh native feedback after reconnect')


class Source:
    def __init__(self, connection, server=None):
        self.connection, self.server = connection, server
        self.port = int(connection.get_dsn_parameters()['port'])

    def rows(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall() if cursor.description else None

    def statuses(self):
        return {row[0]: row[1:] for row in self.rows(
            "SELECT application_name,w.pid,pg_wal_lsn_diff(write_lsn,'0/0')::bigint,"
            "pg_wal_lsn_diff(flush_lsn,'0/0')::bigint,"
            "pg_wal_lsn_diff(replay_lsn,'0/0')::bigint "
            "FROM pg_stat_get_wal_senders() w,pg_stat_get_activity(w.pid) "
            "WHERE w.state='streaming'")}

    def boundary(self, actor, mode):
        if self.server is not None:
            key = {'remote_write': 'write', 'on': 'flush', 'remote_apply': 'apply'}[mode]
            output = self.server.control(
                '(sb-thread:with-mutex (*wal-sender-lock*) '
                f'(let ((waiter (find {actor.backend_pid} '
                f'(getf (sync-replication-state-queues *sync-replication-state*) :{key}) '
                ':key (lambda (waiter) (backend-pid (sync-replication-waiter-backend waiter)))))) '
                '(when waiter (list :boundary (sync-replication-waiter-lsn waiter)))))')
            match = re.search(r':BOUNDARY\s+(\d+)', output.upper())
            return int(match.group(1)) if match else None
        if self.rows(f"SELECT wait_event FROM pg_stat_activity WHERE pid={actor.backend_pid}") != [('SyncRep',)]:
            return None
        return api['lsn_number'](api['scalar'](self.connection, 'SELECT pg_current_wal_flush_lsn()'))


def pending(actor, source, mode, boundary):
    def still_waiting():
        observed = source.boundary(actor, mode)
        assert observed is not None, 'backend left the synchronous wait after insufficient credit'
        if source.server is not None:
            assert observed == boundary
    still_waiting()
    assert not select.select([actor.socket], [], [], 0.15)[0], 'client received a premature response'
    still_waiting()


def complete(actor, protocol='simple'):
    if protocol != 'extended':
        assert actor.receive() == (b'C', b'COMMIT\0' if protocol == 'explicit' else b'INSERT 0 1\0')
    assert actor.receive() == (b'Z', b'I')


def finish_trial(actor, receivers):
    failed = sys.exc_info()[0] is not None
    cleanup = ExitStack()
    cleanup.callback(actor.close)
    for receiver in receivers.values():
        cleanup.callback(receiver.release, 3)
    try:
        cleanup.close()
    except Exception:
        if not failed:
            raise
        traceback.print_exc()


def send_commit(actor, row_id, protocol):
    query = f'INSERT INTO synchronous_ack_rows VALUES ({row_id})'
    if protocol == 'extended':
        actor.socket.sendall(extended['parse'](query) + extended['bind']()
                             + extended['frame'](b'E', b'\0' + (0).to_bytes(4, 'big'))
                             + extended['frame'](b'H', b''))
        assert actor.receive() == (b'1', b'')
        assert actor.receive() == (b'2', b'')
        assert actor.receive() == (b'C', b'INSERT 0 1\0')
        actor.socket.sendall(extended['frame'](b'S', b''))
    else:
        if protocol == 'explicit':
            actor.query('BEGIN')
            actor.query(query)
            query = 'COMMIT'
        body = query.encode() + b'\0'
        actor.socket.sendall(b'Q' + (len(body) + 4).to_bytes(4, 'big') + body)


def trial(source, receivers, row_id, names, mode, winner, protocol='simple'):
    api['configure'](source.connection, names)
    for receiver in receivers.values():
        receiver.arm()
    actor = wire['QueryConnection'](source.port, 'independent-ack')
    actor.socket.settimeout(10)
    try:
        actor.query(f'SET synchronous_commit = {mode}')
        old_pids = {name: row[0] for name, row in source.statuses().items()}
        send_commit(actor, row_id, protocol)
        boundary = wait(lambda: source.boundary(actor, mode), 'source synchronous waiter')
        for receiver in receivers.values():
            wait(lambda: receiver.arrived(boundary), 'receiver actual durable/replay progress')
        pending(actor, source, mode, boundary)
        field = {'remote_write': 1, 'on': 2, 'remote_apply': 3}[mode]
        assert all(row[field] < boundary for row in source.statuses().values())
        for level in range(1, field):
            for name, receiver in receivers.items():
                receiver.release(level)
                wait(lambda: source.statuses()[name][level] >= boundary, 'lower-level ACK')
            pending(actor, source, mode, boundary)
        receivers[winner].release(field)
        wait(lambda: source.statuses()[winner][field] >= boundary, 'first tested ACK')
        selected = 'sync_a' if names.startswith('FIRST 1 (sync_a') else 'sync_b'
        if names.startswith('ANY 2') or (names.startswith('FIRST') and winner != selected):
            pending(actor, source, mode, boundary)
            other = 'sync_b' if winner == 'sync_a' else 'sync_a'
            receivers[other].release(field)
            wait(lambda: source.statuses()[other][field] >= boundary, 'required second ACK')
        complete(actor, protocol)
        assert {name: row[0] for name, row in source.statuses().items()} == old_pids
        assert source.rows(f'SELECT id FROM synchronous_ack_rows WHERE id={row_id}') == [(row_id,)]
        print(f'independent-ack: {names} {mode} {protocol} first={winner} boundary={boundary} PASS',
              flush=True)
    finally:
        finish_trial(actor, receivers)


def disconnect_trial(source, receivers, row_id, names):
    api['configure'](source.connection, names)
    for receiver in receivers.values():
        receiver.arm()
    actor = wire['QueryConnection'](source.port, 'disconnect-ack')
    actor.socket.settimeout(10)
    try:
        actor.query('SET synchronous_commit = on')
        old_pid = source.statuses()['sync_a'][0]
        send_commit(actor, row_id, 'simple')
        boundary = wait(lambda: source.boundary(actor, 'on'), 'disconnect synchronous waiter')
        for receiver in receivers.values():
            wait(lambda: receiver.arrived(boundary), 'durable prefix before disconnect')
        receivers['sync_b'].release(2)
        wait(lambda: source.statuses()['sync_b'][2] >= boundary, 'surviving peer ACK')
        pending(actor, source, 'on', boundary)
        receivers['sync_a'].disconnect()
        wait(lambda: set(source.statuses()) == {'sync_b'}, 'sender removal after real disconnect')
        receivers['sync_b'].release(2)
        if names.startswith('ANY'):
            pending(actor, source, 'on', boundary)
            receivers['sync_a'].reconnect()
            wait(lambda: (source.statuses().get('sync_a', (None, 0, 0))[2] or 0) >= boundary,
                 'second peer ACK after reconnect')
        complete(actor)
        if names.startswith('FIRST'):
            receivers['sync_a'].reconnect()
        wait(lambda: source.statuses().get('sync_a', (old_pid,))[0] != old_pid,
             'new sender identity after reconnect')
        print(f'independent-ack: {names} real disconnect/reconnect PASS', flush=True)
    finally:
        finish_trial(actor, receivers)


def matrix(source, receivers):
    wait(lambda: set(source.statuses()) == set(receivers)
         and all(row[2] and row[2] > 0 for row in source.statuses().values()), 'eligible senders')
    row_id = 0
    for names, winner in (('FIRST 1 (sync_a,sync_b)', 'sync_b'),
                          ('FIRST 1 (sync_b,sync_a)', 'sync_a'),
                          ('FIRST 1 (sync_a,sync_b)', 'sync_a'),
                          ('FIRST 1 (sync_b,sync_a)', 'sync_b'),
                          ('ANY 1 (sync_a,sync_b)', 'sync_a'),
                          ('ANY 1 (sync_a,sync_b)', 'sync_b'),
                          ('ANY 2 (sync_a,sync_b)', 'sync_a')):
        for mode in ('remote_write', 'on', 'remote_apply'):
            row_id += 1
            trial(source, receivers, row_id, names, mode, winner)
    trial(source, receivers, row_id + 1, 'FIRST 1 (sync_a,sync_b)', 'on', 'sync_b', 'explicit')
    trial(source, receivers, row_id + 2, 'ANY 2 (sync_a,sync_b)', 'on', 'sync_a', 'extended')
    return row_id + 2


def run_cases(source, receivers, case):
    last = matrix(source, receivers) if case != 'disconnect' else 0
    if case != 'ack':
        disconnect_trial(source, receivers, last + 1, 'FIRST 1 (sync_a,sync_b)')
        disconnect_trial(source, receivers, last + 2, 'ANY 2 (sync_a,sync_b)')


def check(native, reference, server, case='all'):
    for connection in (reference, native):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE synchronous_ack_rows (id integer PRIMARY KEY)')
    directory = Path(tempfile.mkdtemp(prefix='pg-c-ack-receivers-'))
    with ExitStack() as cleanup:
        receivers = {}
        for name in ('sync_a', 'sync_b'):
            receiver = CReceiver(reference, name, directory)
            cleanup.callback(receiver.close)
            receivers[name] = receiver
        cleanup.callback(api['configure'], reference, '')
        run_cases(Source(reference), receivers, case)
    cleanup = ExitStack()
    try:
        receivers = {name: NativeReceiver(native, server, name, cleanup)
                     for name in ('sync_a', 'sync_b')}
        run_cases(Source(native, server), receivers, case)
    finally:
        failed = sys.exc_info()[0] is not None
        cleanup.callback(api['configure'], native, '')
        try:
            cleanup.close()
        except Exception:
            if not failed:
                raise
            traceback.print_exc()


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    parser.add_argument('--case', choices=('all', 'ack', 'disconnect'), default='all')
    arguments = parser.parse_args()
    os.environ.setdefault('PG_CONFIG', str(arguments.reference_bin.resolve() / 'pg_config'))
    os.environ['PATH'] = str(arguments.reference_bin.resolve()) + os.pathsep + os.environ['PATH']
    helper = Path(tempfile.mkdtemp(prefix='pg-ack-libpq-')) / 'helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)], check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    api['peers']['run'](arguments.reference_bin.resolve(),
        lambda native, reference, server: check(native, reference, server, arguments.case),
        'independent-ack', with_server=True)
