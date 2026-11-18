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


def query(server, password, statement='SELECT 42'):
    dsn = (f'host=127.0.0.1 port={server.port} user=postgres dbname=postgres '
           'sslmode=disable connect_timeout=3')
    return subprocess.run(
        ['psql', '-XAwqt', '-d', dsn, '-c', statement],
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


def start_and_check_login(server, password='wire-fixture'):
    with (server.root / 'server.log').open('ab') as log:
        server.process = subprocess.Popen(
            [str(LISP / 'pg-lisp.sh'), '-D', str(server.data), '-p', str(server.port)],
            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 45
    while True:
        assert server.process.poll() is None, server.root
        good = query(server, password)
        if good.returncode == 0:
            assert good.stdout.strip() == '42', good
            return
        assert time.monotonic() < deadline, good
        time.sleep(0.1)


def check_backup_login(source, password='wire-fixture'):
    clone = OwnedLisp()
    try:
        dsn = f'host=127.0.0.1 port={source.port} user=postgres sslmode=disable'
        result = subprocess.run(
            [str(LISP / 'bin/pg_basebackup'), '-D', str(clone.data), '-X', 'stream', '-d', dsn],
            env={**os.environ, 'PGPASSWORD': password},
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        relative = Path('global/pg_bootstrap_scram')
        assert (clone.data / relative).stat().st_mode & 0o777 == 0o600
        assert (clone.data / relative).read_bytes() == (source.data / relative).read_bytes()
        for node in (source, clone):
            assert (node.data / 'global/pg_catalog.dat').stat().st_mode & 0o777 == 0o600
        start_and_check_login(clone, password)
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
            else:
                rolled_back = query(server, 'wire-fixture',
                                    "BEGIN; ALTER ROLE postgres PASSWORD 'unused-fixture'; ROLLBACK")
                assert rolled_back.returncode == 0, rolled_back.stderr
                failed = query(server, 'wire-fixture',
                               "ALTER ROLE postgres PASSWORD 'unused-fixture'; SELECT 1/0")
                assert failed.returncode != 0 and 'division by zero' in failed.stderr, failed
                changed = query(server, 'wire-fixture', "ALTER ROLE postgres PASSWORD 'rotated-fixture'")
                assert changed.returncode == 0, changed.stderr
                assert query(server, 'wire-fixture').returncode != 0
                assert query(server, 'rotated-fixture').returncode == 0
            server.stop()
        start_and_check_login(server, 'rotated-fixture')
        assert query(server, 'wire-fixture').returncode != 0
        version = query(server, 'rotated-fixture',
                        "SELECT pg_catalog.pg_read_binary_file('PG_VERSION')")
        assert version.returncode == 0 and version.stdout.strip().startswith('\\x'), version
        prefix = query(server, 'rotated-fixture',
                       "SELECT octet_length(pg_read_binary_file('PG_VERSION', 0, 1, false))")
        assert prefix.returncode == 0 and prefix.stdout.strip() == '1', prefix
        root = query(server, 'rotated-fixture',
                     "SELECT pg_ls_dir('.', false, true) AS entry ORDER BY entry")
        assert root.returncode == 0 and root.stdout.splitlines() == ['.', '..', 'PG_VERSION', 'base', 'global', 'pg_xact'], root
        global_dir = query(server, 'rotated-fixture',
                           "SELECT * FROM pg_catalog.pg_ls_dir('global', false, false)")
        assert global_dir.returncode == 0 and global_dir.stdout.strip() == 'pg_control', global_dir
        metadata = query(server, 'rotated-fixture',
                         "SELECT size, isdir FROM pg_catalog.pg_stat_file('PG_VERSION', false)")
        assert metadata.returncode == 0 and metadata.stdout.strip() == f"{(server.data / 'PG_VERSION').stat().st_size}|f", metadata
        directory = query(server, 'rotated-fixture',
                          "SELECT (pg_stat_file('base', false)).isdir")
        assert directory.returncode == 0 and directory.stdout.strip() == 't', directory
        private_dir = query(server, 'rotated-fixture', "SELECT pg_ls_dir('pg_wal', false, false)")
        assert private_dir.returncode != 0 and 'outside the rewind read scope' in private_dir.stderr, private_dir
        for path in ('../pwfile', 'global/pg_bootstrap_scram', 'global/pg_catalog.dat'):
            denied = query(server, 'rotated-fixture', f"SELECT pg_read_binary_file('{path}')")
            assert denied.returncode != 0 and 'outside the rewind read scope' in denied.stderr, denied
        check_backup_login(server, 'rotated-fixture')
        print('scram-wire: rollback and committed password rotation survive restart and clone PASS')
        print('scram-wire: initdb credentials, restart and startup frame validation PASS')
    finally:
        server.stop()
        print(f'artifacts: {server.root}')


if __name__ == '__main__':
    main()
