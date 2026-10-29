"""Compare SQL and WAL-sender session quotas on owned native or C peers."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time

import psutil

from parking_wire import RawConnection, WireError


def connect(port, mode=None, options=None):
    connection = RawConnection.__new__(RawConnection)
    connection.socket = socket.create_connection(('127.0.0.1', port), timeout=5)
    connection.socket.settimeout(120)
    connection.rows_seen = []
    connection.cancel_key = None
    authenticated = False
    values = ['user', 'postgres', 'database', 'postgres', 'application_name', 'quota-test']
    if mode:
        values += ['replication', mode]
    if options:
        values += ['options', options]
    body = struct.pack('!I', 196608) + '\0'.join(values).encode() + b'\0\0'
    try:
        connection.socket.sendall(struct.pack('!I', len(body) + 4) + body)
        while True:
            kind, body = connection.receive()
            if kind == b'R':
                assert body == struct.pack('!I', 0)
                authenticated = True
            elif kind == b'K':
                connection.cancel_key = struct.unpack('!II', body)
            elif kind == b'E':
                fields = connection.error_fields(body)
                error = WireError(fields)
                error.authenticated = authenticated
                error.severity = fields.get('V', fields.get('S'))
                raise error
            elif kind == b'Z':
                assert authenticated and connection.cancel_key
                return connection
    except BaseException:
        connection.close()
        raise


def available(port, mode=None):
    deadline = time.monotonic() + 5
    while True:
        try:
            return connect(port, mode)
        except WireError as error:
            assert error.sqlstate == '53300' and time.monotonic() < deadline, error
            time.sleep(.01)


def reject(port, mode=None):
    try:
        connection = connect(port, mode)
    except WireError as error:
        assert error.sqlstate == '53300' and error.severity == 'FATAL', error
        assert not error.authenticated, 'quota rejection came after AuthenticationOk'
    else:
        connection.close()
        raise AssertionError(('quota admitted an extra session', mode))


def wait(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < deadline, 'admission barrier timed out'
        time.sleep(.02)


def query_cancel_at_capacity(base, root, port, holders, native, table):
    holders[1].query(f'CREATE TABLE {table} (id integer PRIMARY KEY)')
    holders[1].query(f'INSERT INTO {table} VALUES (0)')
    holders[1].query('BEGIN')
    holders[1].query(f'UPDATE {table} SET id=0 WHERE id=0')
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            future = pool.submit(holders[0].query, f'UPDATE {table} SET id=1 WHERE id=0')
            pid = holders[0].cancel_key[0]
            def waiting():
                assert not future.done(), 'contending UPDATE did not wait for its row lock'
                if native:
                    expression = (f'(let ((backend (lookup-backend {pid}))) '
                                  '(format t "QUOTA_WAIT=~a~%" (and backend (not (null (backend-waiting-xid backend))))))')
                    return 'QUOTA_WAIT=T' in control(base, root / 'control.sock', expression)
                return holders[2].query(
                    f'SELECT wait_event_type FROM pg_stat_activity WHERE pid={pid}') == [['Lock']]
            wait(waiting)
            with socket.create_connection(('127.0.0.1', port), timeout=5) as cancel:
                cancel.sendall(struct.pack('!IIII', 16, 80877102, *holders[0].cancel_key))
                assert cancel.recv(1) == b''
            try:
                future.result(timeout=5)
            except WireError as error:
                assert error.sqlstate == '57014', error
            else:
                raise AssertionError('CancelRequest did not cancel the query')
        finally:
            try:
                holders[1].query('ROLLBACK')
            except Exception:
                holders[1].close()
    assert holders[0].query('SELECT 1') == [['1']]
    reject(port)


def control(base, path, form):
    result = subprocess.run(
        [os.environ.get('SBCL', 'sbcl'), '--script', str(base / 't/control-client.lisp'),
         'unix', str(path)], input=form + '\n(quit)\n', capture_output=True,
        text=True, timeout=60, check=True)
    assert ';; error:' not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout + result.stderr


def exercise(base, root, port, connections, senders, native, reload_limits=False):
    holders, replication = [], []
    pending = socket.create_connection(('127.0.0.1', port), timeout=5)
    try:
        for _ in range(connections):
            holders.append(available(port))
        reject(port)
        for index in range(senders):
            mode = 'true' if index % 2 == 0 else 'database'
            replication.append(available(port, mode))
        reject(port, 'true')
        reject(port, 'database')
        assert holders[0].query('SHOW max_connections') == [[str(connections)]]
        assert holders[0].query('SHOW max_wal_senders') == [[str(senders)]]
        query_cancel_at_capacity(base, root, port, holders, native,
                                 f'quota_wait_{connections}_{senders}')
        if reload_limits:
            (root / 'data/postgresql.conf').write_text(
                f'max_connections={connections + 1}\nmax_wal_senders={senders + 1}\n')
            assert holders[0].query('SELECT pg_reload_conf()') == [['t']]
            wait(lambda: holders[0].query(
                "SELECT pending_restart FROM pg_catalog.pg_settings WHERE name='max_connections'") == [['t']])
            assert holders[0].query('SHOW max_connections') == [[str(connections)]]
            assert holders[0].query('SHOW max_wal_senders') == [[str(senders)]]
            if native:
                assert 'reload complete' in control(base, root / 'control.sock', '(reload)')
            reject(port)
            reject(port, 'true')
        holders.pop().close()
        # Failed startup options occur after class reservation and must release it.
        deadline = time.monotonic() + 5
        while True:
            try:
                failed = connect(port, options='-c statement_timeout=-1')
            except WireError as error:
                if error.sqlstate == '53300' and time.monotonic() < deadline:
                    time.sleep(.01)
                    continue
                assert error.sqlstate == '22023', error
                break
            else:
                failed.close()
                raise AssertionError('invalid startup options were accepted')
        holders.append(available(port))
        reject(port)
        if replication:
            replication.pop().close()
            replication.append(available(port, 'database'))
            reject(port, 'true')
        print(f'backend-admission: sql={connections} senders={senders}, pre-auth FATAL, '
              'independence, release, startup failure and saturated cancellation PASS', flush=True)
    finally:
        pending.close()
        for connection in holders + replication:
            connection.close()


def start(base, root, executable, port, label, native, processes):
    registry, directory = root / 'owned', root / 'data'
    arguments = [str(executable), '-D', str(directory), '-p', str(port)]
    if native:
        arguments += ['--control-socket', str(root / 'control.sock')]
    else:
        arguments += ['-h', '127.0.0.1', '-k', str(root)]
    with (root / f'{label}.log').open('w') as log:
        process = subprocess.Popen(
            [sys.executable, str(base / 't/interop/owned-peer.py'), 'start',
             str(registry), str(directory), *arguments], stdout=log, stderr=subprocess.STDOUT)
    processes.append(process)
    def ready():
        assert process.poll() is None, (root / f'{label}.log').read_text()
        pidfile = directory / 'postmaster.pid'
        listening = pidfile.exists() and int(pidfile.read_text().splitlines()[0]) == process.pid and any(
            item.status == psutil.CONN_LISTEN and item.laddr.port == port
            for item in psutil.Process(process.pid).net_connections(kind='tcp'))
        if not listening:
            return False
        try:
            probe = connect(port)
        except WireError as error:
            assert error.sqlstate == '57P03', error
            return False
        probe.close()
        return True
    wait(ready, 60)
    return process


def stop(base, root, process):
    subprocess.run([sys.executable, str(base / 't/interop/signal-owned-server.py'),
                    str(process.pid), str(root / 'data'), 'INT', '20'], check=True)
    assert process.wait(timeout=20) == 0


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
    processes = []
    try:
        for label, connections, senders in [('initial', 5, 2), ('restart', 6, 3), ('disabled', 5, 0)]:
            (directory / 'postgresql.conf').write_text(
                f'max_connections={connections}\nmax_wal_senders={senders}\n')
            process = start(base, root, executable, args.port, label, args.native, processes)
            exercise(base, root, args.port, connections, senders, args.native, label == 'initial')
            stop(base, root, process)
    finally:
        registry = root / 'owned'
        if registry.exists():
            subprocess.run([sys.executable, str(base / 't/interop/owned-peer.py'),
                            'cleanup', str(registry)], check=True)
        for process in processes:
            process.wait(timeout=10)
    print(f'backend-admission: restart-applied caps and zero-sender configuration PASS; artifacts={root}')


if __name__ == '__main__':
    main()
