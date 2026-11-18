"""Exercise the native SCRAM exchange with a real libpq client."""

import os
from pathlib import Path
import runpy
import socket
import struct
import subprocess


OwnedLisp = runpy.run_path(str(Path(__file__).parents[1] / 'command-complete.py'))['OwnedLisp']


def query(server, password):
    dsn = (f'host=127.0.0.1 port={server.port} user=postgres dbname=postgres '
           'sslmode=disable')
    return subprocess.run(
        ['psql', '-XAwqt', '-d', dsn, '-c', 'SELECT 42'],
        env={**os.environ, 'PGPASSWORD': password, 'LC_ALL': 'C'},
        capture_output=True, text=True, timeout=15)


def read_frame(stream):
    kind, length = struct.unpack('!cI', stream.read(5))
    return kind, stream.read(length - 4)


def check_empty_initial_response(server):
    startup = b'user\0postgres\0database\0postgres\0\0'
    mechanism = b'SCRAM-SHA-256\0'
    with socket.create_connection(('127.0.0.1', server.port), timeout=5) as peer:
        with peer.makefile('rb') as stream:
            peer.sendall(struct.pack('!II', len(startup) + 8, 196608) + startup)
            assert read_frame(stream) == (b'R', struct.pack('!I', 10) + mechanism + b'\0')
            peer.sendall(b'p' + struct.pack('!I', len(mechanism) + 8)
                         + mechanism + struct.pack('!i', -1))
            assert read_frame(stream) == (b'R', struct.pack('!I', 11))
            peer.sendall(b'Q')
            kind, error = read_frame(stream)
            assert kind == b'E' and b'C08P01\0' in error, (kind, error)


def main():
    server = OwnedLisp()
    try:
        server.start()
        # Only the fixture supplies a verifier. The call drives the same
        # PostgreSQL authentication frames that an HBA rule will send.
        server.control('''
          (let ((original (symbol-function 'send-auth-ok))
                (salt (string->bytes "fixed-test-salt!")))
            (multiple-value-bind (stored server)
                (scram-derive-keys (string->bytes "wire-fixture") salt 4096)
              (setf (symbol-function 'send-auth-ok)
                    (lambda (stream)
                      (scram-authenticate-stream stream salt 4096 stored server)
                      (funcall original stream)))))
        ''')
        good = query(server, 'wire-fixture')
        assert good.returncode == 0 and good.stdout.strip() == '42', good
        bad = query(server, 'wrong-fixture-password')
        assert bad.returncode != 0 and 'password authentication failed' in bad.stderr, bad
        check_empty_initial_response(server)
        print('scram-wire: libpq proofs and startup frame validation PASS')
    finally:
        server.stop()
        print(f'artifacts: {server.root}')


if __name__ == '__main__':
    main()
