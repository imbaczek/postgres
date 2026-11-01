"""Compare SQL dollar-quoted strings with an owned original C PostgreSQL server."""
import argparse
from pathlib import Path
import runpy
import signal
import socket
import subprocess
import tempfile
import time

import psycopg2


api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
OwnedLisp = api['OwnedLisp']


def connect(port):
    deadline = time.monotonic() + 20
    while True:
        try:
            connection = psycopg2.connect(host='127.0.0.1', port=port, user='postgres',
                                          dbname='postgres', connect_timeout=1)
            connection.autocommit = True
            return connection
        except psycopg2.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


def result(connection, query):
    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            return ('rows', cursor.fetchall())
        except psycopg2.Error as error:
            return ('error', error.pgcode, error.diag.statement_position)


def run(binary):
    root = Path(tempfile.mkdtemp(prefix='pg-dollar-quotes-c-oracle-'))
    server = OwnedLisp()
    process = native = reference = None
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    try:
        with (root / 'initdb.log').open('w') as log:
            subprocess.run([str(binary / 'initdb'), '-D', str(root / 'data'), '-A', 'trust',
                            '-U', 'postgres', '--no-locale', '--encoding=UTF8'],
                           stdout=log, stderr=log, check=True)
        with (root / 'server.log').open('w') as log:
            process = subprocess.Popen([str(binary / 'postgres'), '-D', str(root / 'data'),
                                        '-p', str(port), '-h', '127.0.0.1', '-k', str(root)],
                                       stdout=log, stderr=log)
        reference = connect(port)
        server.start()
        native = connect(server.port)
        queries = [
            'SELECT $$$$',
            r"SELECT $$a'b\c; -- not a comment$$",
            'SELECT $Tag$a$tag$b$Tag$',
            'SELECT $_a12$BEGIN\nPERFORM 1;\nEND;$_a12$',
            'SELECT $é$🙂\nquoted\'"$é$',
            'SELECT $a$x$junk$a$',
            'SELECT $$' + ('x' * 1000000) + '$$',
            'SELECT $$unterminated',
            'SELECT $Tag$x$tag$',
            'SELECT $1$x$1$',
            'SELECT $a-b$x$a-b$',
            'SELECT $$a$$$',
        ]
        for index, query in enumerate(queries, 1):
            expected, actual = result(reference, query), result(native, query)
            assert actual == expected, (index, query[:120], expected, actual)
        print(f'dollar-quoted-strings: {len(queries)} C/native value and error-position cases PASS',
              flush=True)
    finally:
        if native is not None:
            native.close()
        if reference is not None:
            reference.close()
        server.stop(immediate=True)
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
        print(f'artifacts: native={server.root} C={root}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    run(parser.parse_args().reference_bin.resolve())
