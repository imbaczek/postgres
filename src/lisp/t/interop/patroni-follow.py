"""Exercise pinned Patroni's real follow method against owned native peers.

Only the PostgreSQL control layer runs: no DCS, election or HA loop is involved.
Artifacts are retained for inspection, and the actual server control socket is
available for debugging startup and receiver failures.
"""
import ctypes
import importlib.metadata
import json
import logging
import os
from pathlib import Path
import shutil
import shlex
import signal
import socket
import subprocess
import tempfile
import time

import psycopg2
import psutil
from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.async_executor import CriticalTask
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.mpp import Null
from patroni.postgresql.misc import PostgresqlRole


BASE = Path(__file__).resolve().parents[2]


def query(port, sql):
    connection = psycopg2.connect(host='127.0.0.1', port=port, user='postgres',
                                  dbname='postgres', connect_timeout=2)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall() if cursor.description else None
    finally:
        connection.close()


def wait_for(function, seconds=90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            if function():
                return
        except psycopg2.OperationalError as error:
            last = error
        time.sleep(0.1)
    raise AssertionError(f'timeout: {last}')


def control(process, path, form):
    assert process.poll() is None
    result = subprocess.run(
        [os.environ.get('SBCL', 'sbcl'), '--script', str(BASE / 't/control-client.lisp'),
         'unix', str(path)], text=True, capture_output=True, check=True, timeout=45,
        input=f'(assert (= (sb-posix:getpid) {process.pid}))\n{form}\n(quit)\n')
    assert ';; error:' not in result.stdout, result.stdout
    return result.stdout


def configuration(data, port, root):
    # Keep Patroni's real process path; the wrapper only adds a private debug
    # socket and forwards every standard argument unchanged to the native server.
    executable = root / 'postgres'
    native = shlex.quote(str(BASE / 'pg-lisp.sh'))
    executable.write_text('#!/bin/sh\ncase "$1" in\n'
                          '--version|-V|--describe-config) exec ' + native + ' "$@";;\nesac\n'
                          'exec ' + native
                          + ' "$@" --control-socket '
                          + shlex.quote(str(root / 'target.sock')) + '\n')
    executable.chmod(0o700)
    major_version = int((data / 'PG_VERSION').read_text().strip()) * 10000
    parameters = {name: option[0] for name, option in ConfigHandler.CMDLINE_OPTIONS.items()
                  if option[0] is not None and option[2] <= major_version}
    return dict(
        name='follow_one', scope='pg-lisp-follow', data_dir=str(data),
        bin_dir=str(BASE / 'bin'),
        bin_name={'postgres': str(executable),
                  'pg_isready': shutil.which('pg_isready')},
        listen=f'127.0.0.1:{port}', connect_address=f'127.0.0.1:{port}',
        retry_timeout=10, pgpass=str(root / 'replication passfile'),
        parameters=parameters,
        authentication={'superuser': {'username': 'postgres'},
                        'replication': {'username': 'postgres'}},
        pg_hba=['host all all 127.0.0.1/32 trust',
                'host replication all 127.0.0.1/32 trust'])


def exercise(root, ports, source_log):
    source_data, target_data = root / 'source', root / "replica 'one'"
    source_socket = root / 'source.sock'
    source = subprocess.Popen(
        [str(BASE / 'pg-lisp.sh'), '-D', str(source_data), '-p', str(ports[0]),
         '--control-socket', str(source_socket)], stdout=source_log, stderr=source_log)
    node = None
    try:
        def source_ready():
            assert source.poll() is None, f'source exited: {root}'
            if query(ports[0], 'SELECT 1') != [(1,)]:
                return False
            assert int((source_data / 'postmaster.pid').read_text().splitlines()[0]) == source.pid
            return True

        wait_for(source_ready)
        # A ready-looking PID file must not let reload signal an unrelated owner.
        foreign = root / 'foreign data'
        foreign.mkdir(mode=0o700)
        fields = (source_data / 'postmaster.pid').read_text().splitlines()
        fields[1] = str(foreign)
        (foreign / 'postmaster.pid').write_text('\n'.join(fields) + '\n')
        (foreign / '.pg_lisp.lock').touch(mode=0o600)
        refused = subprocess.run([str(BASE / 'bin/pg_ctl'), 'reload', '-D', str(foreign)],
                                 capture_output=True, text=True, timeout=10)
        assert refused.returncode == 1 and 'does not own' in refused.stderr, refused.stderr
        query(ports[0], 'CREATE TABLE follow_items (id integer PRIMARY KEY)')
        query(ports[0], 'INSERT INTO follow_items VALUES (1)')
        query(ports[0], "SELECT pg_create_physical_replication_slot('follow_one',true)")
        target_data.mkdir(mode=0o700)
        control(source, source_socket,
                f'(physical-base-backup {json.dumps(str(target_data) + "/")} "patroni_follow")')
        (target_data / 'postgresql.conf').write_text('# Native base, managed by Patroni.\n')
        node = Postgresql(configuration(target_data, ports[1], root), Null())
        assert node.controldata()['max_locks_per_xact setting'] == '128'
        member = Member(0, 'source', None,
                        {'conn_url': f'postgres://127.0.0.1:{ports[0]}/postgres'})
        assert node.follow(member, timeout=90) is True
        target_pid = int((target_data / 'postmaster.pid').read_text().splitlines()[0])
        assert node.is_running().pid == target_pid
        assert query(ports[1], 'SELECT pg_is_in_recovery()') == [(True,)]
        query(ports[0], 'INSERT INTO follow_items VALUES (2)')
        wait_for(lambda: query(ports[1], 'SELECT id FROM follow_items ORDER BY id') == [(1,), (2,)])
        assert query(ports[1], 'SELECT status,slot_name FROM pg_stat_get_wal_receiver()') == [
            ('streaming', 'follow_one')]
        replacement = Member(0, 'source', None,
                             {'conn_url': f'postgres://localhost:{ports[0]}/postgres'})
        assert node.follow(replacement, do_reload=True) is True
        wait_for(lambda: 'host=localhost' in query(ports[1], 'SHOW primary_conninfo')[0][0]
                 and query(ports[1], 'SELECT status,slot_name FROM pg_stat_get_wal_receiver()')
                 == [('streaming', 'follow_one')])
        query(ports[0], 'INSERT INTO follow_items VALUES (3)')
        wait_for(lambda: query(ports[1], 'SELECT id FROM follow_items ORDER BY id') == [(1,), (2,), (3,)])
        assert int((target_data / 'postmaster.pid').read_text().splitlines()[0]) == target_pid
        assert node.follow(replacement, timeout=90) is True
        assert node.is_running().pid != target_pid
        query(ports[0], 'INSERT INTO follow_items VALUES (4)')
        wait_for(lambda: query(ports[1], 'SELECT id FROM follow_items ORDER BY id')
                 == [(1,), (2,), (3,), (4,)])
        assert query(ports[1], 'SELECT status,slot_name FROM pg_stat_get_wal_receiver()') == [
            ('streaming', 'follow_one')]
        restarted_pid = node.is_running().pid
        assert node.follow(None, do_reload=True) is True
        wait_for(lambda: query(ports[1], 'SHOW primary_conninfo') == [('',)]
                 and query(ports[1], 'SELECT pid FROM pg_stat_get_wal_receiver()') == [(None,)])
        wait_for(lambda: query(ports[0], "SELECT active FROM pg_replication_slots WHERE slot_name='follow_one'")
                 == [(False,)])
        query(ports[0], 'INSERT INTO follow_items VALUES (5)')
        assert query(ports[1], 'SELECT id FROM follow_items ORDER BY id') == [(1,), (2,), (3,), (4,)]
        assert node.follow(member, do_reload=True) is True
        wait_for(lambda: query(ports[1], 'SELECT id FROM follow_items ORDER BY id')
                 == [(1,), (2,), (3,), (4,), (5,)])
        assert node.is_running().pid == restarted_pid
        source.send_signal(signal.SIGINT)
        assert source.wait(timeout=45) == 0
        assert node.promote(30, CriticalTask()) is True
        assert node.role == PostgresqlRole.PRIMARY
        assert query(ports[1], 'SELECT pg_is_in_recovery()') == [(False,)]
        query(ports[1], 'INSERT INTO follow_items VALUES (6)')
        assert query(ports[1], 'SELECT id FROM follow_items ORDER BY id') == [
            (1,), (2,), (3,), (4,), (5,), (6,)]
        print('patroni-follow: real start, configured streaming, reload, restart, clear and reattach PASS',
              flush=True)
        print('patroni-follow: fenced source, real Patroni promotion and child writes PASS', flush=True)
    finally:
        try:
            if node is not None:
                try:
                    node.connection_pool.close()
                finally:
                    process = node.is_running()
                    if process:
                        process.send_signal(signal.SIGINT)
                        try:
                            process.wait(timeout=45)
                        except psutil.TimeoutExpired:
                            process.signal_kill()
                            process.wait(timeout=10)
        finally:
            if source.poll() is None:
                source.send_signal(signal.SIGINT)
                try:
                    source.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    source.kill()
                    source.wait(timeout=10)


def main():
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    assert importlib.metadata.version('patroni') == '4.1.5'
    assert importlib.metadata.version('psycopg2-binary') == '2.9.11'
    assert importlib.metadata.version('psutil') == '7.1.3'
    assert shutil.which('pg_isready'), 'pg_isready is required'
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    logging.basicConfig(level=logging.INFO)
    root = Path(tempfile.mkdtemp(prefix='pg-patroni-follow.'))
    root.chmod(0o700)
    helper = root / 'libpq-helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)],
                   check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    # Reserve distinct ephemeral choices together, then release before startup.
    with socket.socket() as first, socket.socket() as second:
        first.bind(('127.0.0.1', 0))
        second.bind(('127.0.0.1', 0))
        ports = first.getsockname()[1], second.getsockname()[1]
    print(f'patroni-follow: artifacts={root}; ports={ports}', flush=True)
    with (root / 'source.log').open('w') as log:
        exercise(root, ports, log)


if __name__ == '__main__':
    main()
