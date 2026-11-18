"""Exercise the native SCRAM exchange with a real libpq client."""

import os
from pathlib import Path
import runpy
import socket
import struct
import subprocess
import time


OwnedLisp = runpy.run_path(str(Path(__file__).parents[1] / 'command-complete.py'))['OwnedLisp']
LISP = Path(__file__).resolve().parents[2]


def query(server, password):
    dsn = (f'host=127.0.0.1 port={server.port} user=postgres dbname=postgres '
           'sslmode=disable connect_timeout=3')
    return subprocess.run(
        ['psql', '-XAwqt', '-d', dsn, '-c', 'SELECT 42'],
        env={**os.environ, 'PGPASSWORD': password, 'LC_ALL': 'C'},
        capture_output=True, text=True, timeout=15)


def read_frame(stream):
    kind, length = struct.unpack('!cI', stream.read(5))
    return kind, stream.read(length - 4)


def read_sasl_request(peer, stream):
    startup = b'user\0postgres\0database\0postgres\0\0'
    mechanism = b'SCRAM-SHA-256\0'
    peer.sendall(struct.pack('!II', len(startup) + 8, 196608) + startup)
    assert read_frame(stream) == (b'R', struct.pack('!I', 10) + mechanism + b'\0')
    return mechanism


def check_empty_initial_response(server):
    with socket.create_connection(('127.0.0.1', server.port), timeout=5) as peer:
        with peer.makefile('rb') as stream:
            mechanism = read_sasl_request(peer, stream)
            peer.sendall(b'p' + struct.pack('!I', len(mechanism) + 8)
                         + mechanism + struct.pack('!i', -1))
            assert read_frame(stream) == (b'R', struct.pack('!I', 11))
            peer.sendall(b'Q')
            kind, error = read_frame(stream)
            assert kind == b'E' and b'C08P01\0' in error, (kind, error)


def check_authentication_timeout(server):
    with socket.create_connection(('127.0.0.1', server.port), timeout=15) as peer:
        with peer.makefile('rb') as stream:
            read_sasl_request(peer, stream)
            kind, error = read_frame(stream)
            assert kind == b'E' and b'C57014\0' in error, (kind, error)
    assert query(server, 'wire-fixture').returncode == 0


def start_and_check_login(server):
    with (server.root / 'server.log').open('ab') as log:
        server.process = subprocess.Popen(
            [str(LISP / 'pg-lisp.sh'), '-D', str(server.data), '-p', str(server.port)],
            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 45
    while True:
        assert server.process.poll() is None, server.root
        good = query(server, 'wire-fixture')
        if good.returncode == 0:
            assert good.stdout.strip() == '42', good
            return
        assert time.monotonic() < deadline, good
        time.sleep(0.1)


def check_backup_login(source):
    clone = OwnedLisp()
    try:
        dsn = f'host=127.0.0.1 port={source.port} user=postgres sslmode=disable'
        result = subprocess.run(
            [str(LISP / 'bin/pg_basebackup'), '-D', str(clone.data), '-X', 'stream', '-d', dsn],
            env={**os.environ, 'PGPASSWORD': 'wire-fixture'},
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        relative = Path('global/pg_bootstrap_scram')
        assert (clone.data / relative).stat().st_mode & 0o777 == 0o600
        assert (clone.data / relative).read_bytes() == (source.data / relative).read_bytes()
        start_and_check_login(clone)
        bad = query(clone, 'wrong-fixture-password')
        assert bad.returncode != 0 and 'password authentication failed' in bad.stderr, bad
        print('scram-wire: authenticated native base backup retains the private verifier PASS')
    finally:
        clone.stop()


def main():
    server = OwnedLisp()
    try:
        password = server.root / 'pwfile'
        password.write_text('wire-fixture\n')
        subprocess.run([str(LISP / 'bin/initdb'), '-D', str(server.data),
                        '--auth-host=scram-sha-256', '--pwfile', str(password)],
                       check=True, timeout=60)
        password.unlink()
        verifier = server.data / 'global/pg_bootstrap_scram'
        assert verifier.stat().st_mode & 0o077 == 0
        assert 'wire-fixture' not in verifier.read_text()

        for attempt in range(2):
            start_and_check_login(server)
            bad = query(server, 'wrong-fixture-password')
            assert bad.returncode != 0 and 'password authentication failed' in bad.stderr, bad
            check_empty_initial_response(server)
            if attempt == 0:
                check_authentication_timeout(server)
                check_backup_login(server)
            server.stop()
        print('scram-wire: initdb credentials, restart and startup frame validation PASS')
    finally:
        server.stop()
        print(f'artifacts: {server.root}')


if __name__ == '__main__':
    main()
