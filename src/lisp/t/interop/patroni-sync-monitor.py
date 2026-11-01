"""Run pinned Patroni synchronous monitoring through its real SQL connection pool."""
import argparse
from contextlib import ExitStack
import importlib.metadata
from pathlib import Path
import runpy
import os
import signal
import socket
import subprocess
import sys
import tempfile
import traceback
from threading import Lock
import time

import psycopg2
import psutil
from psycopg2.extras import PhysicalReplicationConnection

from patroni import global_config
from patroni.dcs import Member
from patroni.exceptions import PostgresConnectionException
from patroni.postgresql import Postgresql
from patroni.postgresql.connection import ConnectionPool
from patroni.postgresql.misc import PostgresqlRole, PostgresqlState
from patroni.postgresql.mpp import Null
from patroni.postgresql.slots import SlotsHandler
from patroni.postgresql.sync import SyncHandler
from patroni.utils import Retry


peers = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'dollar-quoted-strings.py'))
follow = runpy.run_path(str(Path(__file__).with_name('patroni-follow.py')))


def scalar(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()[0]


def monitor(connection, role, directory):
    node = Postgresql.__new__(Postgresql)
    node._major_version = connection.server_version
    node._data_dir = str(directory)
    node._state_lock, node._role_lock = Lock(), Lock()
    node._state, node._role = PostgresqlState.RUNNING, role
    node._cluster_info_state, node._should_query_slots = {}, False
    node.slots_handler = SlotsHandler(node)
    node.connection_pool = ConnectionPool()
    parameters = connection.get_dsn_parameters()
    node.connection_pool.conn_kwargs = {
        key: parameters[key] for key in ('host', 'port', 'user', 'dbname')}
    node._connection = node.connection_pool.get('heartbeat')
    node.retry = Retry(max_tries=1, deadline=3, max_delay=0.1,
                       retry_exceptions=PostgresConnectionException)
    node._is_leader_retry = node.retry.copy()
    trace = []
    original = node._connection.query

    def traced(sql, *parameters):
        trace.append(sql)
        return original(sql, *parameters)

    node._connection.query = traced
    return node, trace


def configure(connection, names):
    directory = Path(scalar(connection, 'SHOW data_directory'))
    with (directory / 'postgresql.conf').open('a') as config:
        config.write(f"\nsynchronous_standby_names = '{names}'\n")
    assert scalar(connection, 'SELECT pg_reload_conf()')
    deadline = time.monotonic() + 10
    while scalar(connection, "SELECT current_setting('synchronous_standby_names')") != names:
        if time.monotonic() >= deadline:
            raise TimeoutError('owned server did not reload synchronous standby names')
        time.sleep(0.02)
    return directory


def check_one(connection, label):
    directory = configure(connection, 'FIRST 1 (missing_peer)')
    for role in (PostgresqlRole.PRIMARY, PostgresqlRole.PROMOTED):
        node, trace = monitor(connection, role, directory)
        try:
            query = node.cluster_info_query
            row = node.query(query)[0]
            assert len(row) == 16, row
            assert row[0] > 0 and row[13:] == ('on', 'FIRST 1 (missing_peer)', None), row
            assert node.synchronous_standby_names() == 'FIRST 1 (missing_peer)'
            before = node.last_operation()
            wal_before = scalar(connection, 'SELECT pg_current_wal_lsn()')
            handler = SyncHandler(node)
            heartbeat = node._connection.get()
            handler._handle_synchronous_standby_names_change()
            assert handler._primary_flush_lsn == before
            assert node._cluster_info_state == {}, node._cluster_info_state
            deadline = time.monotonic() + 5
            while scalar(connection, 'SELECT pg_current_wal_lsn()') == wal_before:
                if time.monotonic() >= deadline:
                    raise TimeoutError('Patroni DO did not produce observable WAL traffic')
                time.sleep(0.02)
            assert node.query('SHOW synchronous_commit') == [('on',)]
            assert node._connection.get() is heartbeat
            do_queries = [sql for sql in trace if sql.startswith('DO $$')]
            assert len(do_queries) == 1, trace
            assert do_queries[0] == """DO $$
BEGIN
    SET local synchronous_commit = 'off';
    PERFORM * FROM pg_catalog.txid_current();
END;$$"""
            handler._handle_synchronous_standby_names_change()
            assert sum(sql.startswith('DO $$') for sql in trace) == 1, trace
            assert query in trace
            with (directory.parent / f'patroni-sync-{role.value}.trace').open('w') as output:
                output.write('\n\n'.join(trace))
            print(f'{label}: unchanged Patroni {role.value} monitor and sync-change method PASS',
                  flush=True)
        finally:
            node.connection_pool.close()


def lsn_number(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def check_streaming(connection, label):
    directory = Path(scalar(connection, 'SHOW data_directory'))
    node, trace = monitor(connection, PostgresqlRole.PRIMARY, directory)
    senders = []
    try:
        start = lsn_number(scalar(connection, 'SELECT pg_current_wal_flush_lsn()'))
        for name in ('sync_a', 'sync_b'):
            sender = psycopg2.connect(connection.dsn, application_name=name,
                                      connection_factory=PhysicalReplicationConnection)
            senders.append(sender)
            cursor = sender.cursor()
            cursor.start_replication(start_lsn=start)
            cursor.send_feedback(write_lsn=start, flush_lsn=start, apply_lsn=start, force=True)
        for names, states in (
                ('FIRST 1 (sync_a,sync_b)', {'sync_a': 'sync', 'sync_b': 'potential'}),
                ('ANY 1 (sync_a,sync_b)', {'sync_a': 'quorum', 'sync_b': 'quorum'})):
            configure(connection, names)
            deadline = time.monotonic() + 10
            while True:
                node.reset_cluster_info_state(None)
                rows = node._cluster_info_state_get('pg_stat_replication') or []
                actual = {row['application_name']: row for row in rows}
                if (set(actual) == set(states)
                        and all(actual[name]['sync_state'] == state for name, state in states.items())
                        and all(row['flush_lsn'] == start for row in rows)):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError((label, names, rows))
                time.sleep(0.02)
            for row in rows:
                assert set(row) == {'pid', 'application_name', 'sync_state',
                                    'write_lsn', 'flush_lsn', 'replay_lsn'}, row
                assert row['pid'] > 0
                assert row['write_lsn'] == row['replay_lsn'] == start, row
        node._should_query_slots = True
        assert len(node.query(node.cluster_info_query)[0]) == 16
        senders.pop().close()
        deadline = time.monotonic() + 10
        while True:
            node.reset_cluster_info_state(None)
            rows = node._cluster_info_state_get('pg_stat_replication') or []
            if [row['application_name'] for row in rows] == ['sync_a']:
                break
            if time.monotonic() >= deadline:
                raise AssertionError((label, 'disconnected sender still visible', rows))
            time.sleep(0.02)
        assert node.cluster_info_query in trace
        print(f'{label}: unchanged Patroni FIRST/ANY streaming sender JSON and disconnect PASS',
              flush=True)
    finally:
        for sender in senders:
            sender.close()
        node.connection_pool.close()
        configure(connection, '')


def stop_follower(node):
    try:
        node.connection_pool.close()
    finally:
        process = node.is_running()
        if process:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=30)
            except psutil.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def check_native_streaming(connection, server):
    cleanup = ExitStack()
    monitor_node, trace = monitor(connection, PostgresqlRole.PRIMARY, server.data)
    cleanup.callback(monitor_node.connection_pool.close)
    try:
        for name in ('sync_a', 'sync_b'):
            root = server.root / name
            data = root / 'data'
            data.mkdir(parents=True, mode=0o700)
            scalar(connection, f"SELECT pg_create_physical_replication_slot('{name}',true)")
            server.control(f'(physical-base-backup "{data}/" "monitor_{name}")')
            (data / 'postgresql.conf').write_text('# Native base managed by Patroni.\n')
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                port = reservation.getsockname()[1]
            config = follow['configuration'](data, port, root)
            config['name'] = name
            node = Postgresql(config, Null())
            cleanup.callback(stop_follower, node)
            member = Member(0, 'source', None,
                            {'conn_url': f'postgres://127.0.0.1:{server.port}/postgres'})
            assert node.follow(member, timeout=90) is True
        monitor_node._should_query_slots = True
        slot_row = monitor_node.query(monitor_node.cluster_info_query)[0]
        assert len(slot_row) == 16
        assert {slot['slot_name'] for slot in slot_row[12]} == {'sync_a', 'sync_b'}, slot_row[12]
        handler = SyncHandler(monitor_node)
        for names, states in (
                ('FIRST 1 (sync_a,sync_b)', {'sync_a': 'sync', 'sync_b': 'potential'}),
                ('ANY 1 (sync_a,sync_b)', {'sync_a': 'quorum', 'sync_b': 'quorum'})):
            configure(connection, names)
            barrier = lsn_number(scalar(connection, 'SELECT pg_current_wal_flush_lsn()'))
            deadline = time.monotonic() + 20
            while True:
                monitor_node.reset_cluster_info_state(None)
                rows = monitor_node._cluster_info_state_get('pg_stat_replication') or []
                actual = {row['application_name']: row for row in rows}
                if (set(actual) == set(states)
                        and all(actual[name]['sync_state'] == state for name, state in states.items())
                        and all(row['replay_lsn'] is not None and row['replay_lsn'] >= barrier
                                for row in rows)):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError((names, barrier, rows))
                time.sleep(0.02)
            for row in rows:
                assert set(row) == {'pid', 'application_name', 'sync_state',
                                    'write_lsn', 'flush_lsn', 'replay_lsn'}, row
                assert 0 < row['replay_lsn'] <= row['flush_lsn'] <= row['write_lsn'], row
            handler._handle_synchronous_standby_names_change()
            assert monitor_node._cluster_info_state == {}
        assert any(sql.startswith('DO $$') for sql in trace)
        print('native: real Patroni followers, FIRST/ANY monitoring and unchanged sync-change DO PASS',
              flush=True)
    finally:
        failed = sys.exc_info()[0] is not None
        cleanup.callback(configure, connection, '')
        cleanup.callback((server.root / 'patroni-sync-live.trace').write_text, '\n\n'.join(trace))
        try:
            cleanup.close()
        except Exception:
            if not failed:
                raise
            traceback.print_exc()


def check(native, reference, server):
    assert importlib.metadata.version('patroni') == '4.1.5'
    global_config.update(None, {'synchronous_mode': True})
    assert global_config.is_synchronous_mode
    check_one(reference, 'C')
    check_one(native, 'native')
    check_streaming(reference, 'C')
    check_native_streaming(native, server)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    arguments = parser.parse_args()
    os.environ.setdefault('PG_CONFIG', str(arguments.reference_bin.resolve() / 'pg_config'))
    os.environ['PATH'] = str(arguments.reference_bin.resolve()) + os.pathsep + os.environ['PATH']
    helper_root = Path(tempfile.mkdtemp(prefix='pg-patroni-sync-helper-'))
    helper = helper_root / 'libpq-helper'
    subprocess.run([str(Path(__file__).resolve().parents[1] / 'build-libpq-helper.sh'), str(helper)],
                   check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    peers['run'](arguments.reference_bin.resolve(), check, 'patroni-sync-monitor', with_server=True)
