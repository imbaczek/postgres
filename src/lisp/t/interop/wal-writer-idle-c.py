"""Show C's WAL writer flushing a nontransactional record before COMMIT."""
import argparse
from pathlib import Path
import runpy
import signal
import socket
import subprocess
import tempfile
import time

import psycopg2.extensions


peers = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'dollar-quoted-strings.py'))


def scalar(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()[0]


def lsn(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def check(binary):
    root = Path(tempfile.mkdtemp(prefix='pg-idle-writer-c-'))
    process = connection = None
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    try:
        with (root / 'initdb.log').open('w') as log:
            subprocess.run([str(binary / 'initdb'), '-D', str(root / 'data'), '-A', 'trust',
                            '-U', 'postgres', '--no-locale', '--encoding=UTF8'],
                           stdout=log, stderr=log, check=True, timeout=60)
        with (root / 'server.log').open('w') as log:
            process = subprocess.Popen(
                [str(binary / 'postgres'), '-D', str(root / 'data'), '-p', str(port),
                 '-h', '127.0.0.1', '-k', str(root), '-c', 'wal_writer_delay=1000ms',
                 '-c', 'wal_writer_flush_after=16MB', '-c', 'autovacuum=off'],
                stdout=log, stderr=log)
        connection = peers['connect'](port, process)
        with connection.cursor() as cursor:
            cursor.execute('CHECKPOINT')
            cursor.execute('BEGIN READ ONLY')
        # Require observed debt, rather than assuming which writer cycle ran.
        for attempt in range(20):
            target = lsn(scalar(connection, 'SELECT pg_log_standby_snapshot()'))
            before = lsn(scalar(connection, 'SELECT pg_current_wal_flush_lsn()'))
            if before < target:
                break
        else:
            raise AssertionError('could not observe an unflushed nontransactional record')
        assert scalar(connection, 'SELECT pg_current_xact_id_if_assigned()') is None
        deadline = time.monotonic() + 10
        while lsn(scalar(connection, 'SELECT pg_current_wal_flush_lsn()')) < target:
            assert time.monotonic() < deadline, 'C WAL writer did not flush the snapshot record'
            time.sleep(0.02)
        assert connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INTRANS
        assert scalar(connection, 'SELECT pg_current_xact_id_if_assigned()') is None
        print('C: unassigned read-only transaction; nontransactional WAL reached flush before COMMIT PASS',
              flush=True)
    finally:
        if connection is not None:
            connection.close()
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=15)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
        print(f'artifacts: {root}', flush=True)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    check(parser.parse_args().reference_bin.resolve())
