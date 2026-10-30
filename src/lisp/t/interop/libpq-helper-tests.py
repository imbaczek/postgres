"""Contracts for the isolated physical-follow libpq helper, not the raw parser."""
import contextlib
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

if not __debug__:
    raise RuntimeError('libpq helper tests require Python assertions')

HEADER = struct.Struct('!4sQQBI')
SESSION = 0x123456789ABCDEF0
LIMIT = 256 * 1024 * 1024
HELPER = Path(os.environ['PG_LIBPQ_HELPER']).resolve()


def string(value):
    value = value.encode() if isinstance(value, str) else value
    return struct.pack('!I', len(value)) + value


def read_exact(descriptor, size, timeout=5):
    deadline, parts = time.monotonic() + timeout, []
    while size:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([descriptor], [], [], remaining)[0]:
            raise TimeoutError('helper response deadline')
        data = os.read(descriptor, size)
        if not data:
            raise EOFError('helper pipe closed')
        parts.append(data)
        size -= len(data)
    return b''.join(parts)


class Helper:
    def __init__(self, *, env=None, pass_fds=()):
        self.process = subprocess.Popen(
            [str(HELPER), str(os.getpid()), f'{SESSION:016x}'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=0, env=env, pass_fds=pass_fds)
        self.request_id = 0
        session, request, kind, body = self.receive()
        assert (session, request, kind) == (SESSION, 0, b'H')
        pid, parent, self.version, self.limit = struct.unpack('!IIII', body)
        assert (pid, parent, self.limit) == (self.process.pid, os.getpid(), LIMIT)

    def receive(self):
        magic, session, request, kind, size = HEADER.unpack(read_exact(self.process.stdout.fileno(), HEADER.size))
        assert magic == b'PGLQ' and size <= 17 * 1024 * 1024
        return session, request, bytes([kind]), read_exact(self.process.stdout.fileno(), size)

    def send(self, kind, body=b'', session=SESSION, request=None, declared_size=None):
        if request is None:
            self.request_id += 1
            request = self.request_id
        header = HEADER.pack(b'PGLQ', session, request, kind[0],
                             len(body) if declared_size is None else declared_size)
        self.process.stdin.write(header + body)
        self.process.stdin.flush()

    def call(self, kind, body=b''):
        self.send(kind, body)
        session, request, reply, payload = self.receive()
        assert (session, request) == (SESSION, self.request_id)
        return reply, payload

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            stream.close()


def wire_frame(connection, kind, body=b''):
    connection.sendall(kind + struct.pack('!I', 4 + len(body)) + body)


def wire_read(connection, size):
    parts = []
    while size:
        part = connection.recv(size)
        if not part:
            raise EOFError('fake source closed')
        parts.append(part)
        size -= len(part)
    return b''.join(parts)


@contextlib.contextmanager
def source(script, startup_notice=None, timeout=5, connections=1):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(timeout)
    parameters, failures = {}, []

    def serve():
        try:
            for _ in range(connections):
                with listener.accept()[0] as connection:
                    connection.settimeout(timeout)
                    size = struct.unpack('!I', wire_read(connection, 4))[0]
                    body = wire_read(connection, size - 4)
                    assert body[:4] == struct.pack('!I', 196608)
                    values = body[4:].rstrip(b'\0').split(b'\0')
                    parameters.update(zip(values[::2], values[1::2]))
                    wire_frame(connection, b'R', struct.pack('!I', 0))
                    for name, value in ((b'server_version', b'20devel'), (b'client_encoding', b'UTF8')):
                        wire_frame(connection, b'S', name + b'\0' + value + b'\0')
                    wire_frame(connection, b'K', struct.pack('!II', 4242, 99))
                    if startup_notice:
                        wire_frame(connection, b'N', b'SNOTICE\0VNOTICE\0C00000\0M' + startup_notice + b'\0\0')
                    wire_frame(connection, b'Z', b'I')
                    script(connection)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1], parameters
    finally:
        listener.close()
        thread.join(timeout=timeout + 1)
        assert not thread.is_alive(), 'fake source did not finish'
        if failures:
            raise failures[0]


class HelperProtocol(unittest.TestCase):
    def setUp(self):
        self.helper = Helper()
        self.addCleanup(self.helper.close)

    def test_hello_reports_library_and_fixed_process_budget(self):
        self.assertGreaterEqual(self.helper.version, 160000)
        limits = Path(f'/proc/{self.helper.process.pid}/limits').read_text()
        address_space = next(line for line in limits.splitlines() if line.startswith('Max address space'))
        self.assertEqual(address_space.split()[3:5], [str(LIMIT), str(LIMIT)])
        self.assertEqual(self.helper.call(b'X'), (b'A', b''))
        self.assertEqual(self.helper.process.wait(timeout=2), 0)

    def test_oversized_ipc_header_fails_without_waiting_for_body(self):
        self.helper.send(b'O', declared_size=2 * 1024 * 1024)
        self.assertEqual(self.helper.receive()[2], b'!')
        self.assertNotEqual(self.helper.process.wait(timeout=2), 0)

    def test_stale_session_fails_closed(self):
        self.helper.send(b'Q', session=SESSION + 1)
        self.assertEqual(self.helper.receive()[2], b'!')
        self.assertNotEqual(self.helper.process.wait(timeout=2), 0)

    def test_unknown_operation_fails_closed(self):
        self.assertEqual(self.helper.call(b'?')[0], b'!')
        self.assertNotEqual(self.helper.process.wait(timeout=2), 0)

    def test_invalid_conninfo_is_not_echoed_in_diagnostics(self):
        marker = b'secret-value-must-not-appear'
        kind, body = self.helper.call(b'O', string(marker) + string('walreceiver'))
        self.assertEqual(kind, b'!')
        self.helper.process.wait(timeout=2)
        self.assertNotIn(marker, body + self.helper.process.stderr.read())

    def test_inherited_descriptors_are_closed_before_network_use(self):
        with tempfile.TemporaryFile() as extra:
            helper = Helper(pass_fds=(extra.fileno(),))
            self.addCleanup(helper.close)
            self.assertEqual({item.name for item in Path(f'/proc/{helper.process.pid}/fd').iterdir()},
                             {'0', '1', '2'})

    def test_parent_death_terminates_an_idle_helper(self):
        code = ("import importlib, os, sys; "
                "h=importlib.import_module('libpq-helper-tests').Helper(); "
                "print(h.process.pid, flush=True); sys.stdin.buffer.read(1); os._exit(0)")
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parent))
        parent = subprocess.Popen([sys.executable, '-c', code], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, bufsize=0)
        pidfd = None
        try:
            line = b''
            while not line.endswith(b'\n'):
                line += read_exact(parent.stdout.fileno(), 1)
            pidfd = os.pidfd_open(int(line))
            parent.stdin.write(b'x')
            self.assertEqual(parent.wait(timeout=2), 0)
            self.assertTrue(select.select([pidfd], [], [], 2)[0], 'helper outlived its parent')
        finally:
            if parent.poll() is None:
                parent.kill()
                parent.wait(timeout=2)
            if pidfd is not None:
                os.close(pidfd)
            for stream in (parent.stdin, parent.stdout, parent.stderr):
                stream.close()

    def test_explicit_application_name_beats_fallback_and_query_is_typed(self):
        marker = b'untrusted-notice-must-not-be-logged'

        def answer(connection):
            self.assertEqual(wire_read(connection, 1), b'Q')
            size = struct.unpack('!I', wire_read(connection, 4))[0]
            self.assertEqual(wire_read(connection, size - 4), b'IDENTIFY_SYSTEM\0')
            wire_frame(connection, b'N', b'SNOTICE\0VNOTICE\0C00000\0M' + marker + b'\0\0')
            field = b'systemid\0' + struct.pack('!IhIhih', 0, 0, 25, -1, -1, 0)
            wire_frame(connection, b'T', struct.pack('!H', 1) + field)
            wire_frame(connection, b'D', struct.pack('!Hi', 1, 2) + b'42')
            wire_frame(connection, b'C', b'IDENTIFY_SYSTEM\0')
            wire_frame(connection, b'Z', b'I')
            self.assertEqual(wire_read(connection, 1), b'X')
            self.assertEqual(wire_read(connection, 4), struct.pack('!I', 4))

        with source(answer, startup_notice=marker) as (port, parameters):
            conninfo = (f'host=127.0.0.1 port={port} user=postgres dbname=postgres '
                        'sslmode=disable gssencmode=disable application_name=explicit')
            self.assertEqual(self.helper.call(b'O', string(conninfo) + string('fallback'))[0], b'O')
            self.assertEqual(parameters[b'application_name'], b'explicit')
            self.assertEqual(parameters[b'replication'], b'database')
            self.assertEqual(parameters[b'client_encoding'], b'UTF8')
            kind, body = self.helper.call(b'Q', struct.pack('!I', 4096) + string('IDENTIFY_SYSTEM'))
            self.assertEqual(kind, b'T')
            expected = (b'\0' + string('IDENTIFY_SYSTEM') + struct.pack('!II', 1, 1)
                        + string('systemid') + struct.pack('!IiIiii', 0, 0, 25, -1, -1, 0)
                        + string('42'))
            self.assertEqual(body, expected)
            self.assertEqual(self.helper.call(b'X'), (b'A', b''))
            self.helper.process.wait(timeout=2)
            self.assertNotIn(marker, body + self.helper.process.stderr.read())

    def test_copy_both_keeps_feedback_before_frontend_copy_done(self):
        observed = []

        def frame(connection):
            kind = wire_read(connection, 1)
            length = struct.unpack('!I', wire_read(connection, 4))[0]
            return kind, wire_read(connection, length - 4)

        def answer(connection):
            self.assertEqual(frame(connection), (b'Q', b'START_STREAMING\0'))
            wire_frame(connection, b'W', b'\0\0\0')
            # Stock libpq discards empty CopyData; the semantic API keeps that rule.
            wire_frame(connection, b'd', b'')
            wire_frame(connection, b'd', b'payload')
            wire_frame(connection, b'c')
            observed.append(frame(connection))
            observed.append(frame(connection))
            self.assertEqual(observed, [(b'd', b'truthful-feedback'), (b'c', b'')])
            columns = b''
            for name, oid, size in ((b'next_tli', 20, 8), (b'next_tli_startpos', 25, -1)):
                columns += name + b'\0' + struct.pack('!IhIhih', 0, 0, oid, size, -1, 0)
            wire_frame(connection, b'T', struct.pack('!H', 2) + columns)
            wire_frame(connection, b'D', struct.pack('!H', 2) + string('2') + string('0/40'))
            wire_frame(connection, b'C', b'START_STREAMING\0')
            wire_frame(connection, b'Z', b'I')
            self.assertEqual(frame(connection), (b'X', b''))

        with source(answer) as (port, parameters):
            conninfo = (f'host=127.0.0.1 port={port} user=postgres dbname=postgres '
                        'sslmode=disable gssencmode=disable')
            self.assertEqual(self.helper.call(b'O', string(conninfo) + string('fallback'))[0], b'O')
            self.assertEqual(parameters[b'application_name'], b'fallback')
            self.assertEqual(self.helper.call(b'Q', struct.pack('!I', 4096) + string('START_STREAMING')),
                             (b'B', b'\0\0\0\0\0'))
            self.assertEqual(self.helper.call(b'R', struct.pack('!II', 65532, 1000)), (b'D', b'payload'))
            self.assertEqual(self.helper.call(b'R', struct.pack('!II', 65532, 1000)), (b'F', b''))
            self.assertEqual(self.helper.call(b'S', b'truthful-feedback'), (b'A', b''))
            kind, body = self.helper.call(b'E', struct.pack('!I', 4096))
            self.assertEqual(kind, b'T')
            self.assertIn(b'START_STREAMING', body)
            self.assertIn(b'next_tli_startpos', body)
            self.assertEqual(self.helper.call(b'X'), (b'A', b''))

    def test_copy_receive_accepts_full_native_and_stock_batches_with_a_bound(self):
        maximum = 128 * 1024 + 25
        for length in (65532, 65533, 65536 + 25, maximum, maximum + 1):
            with self.subTest(length=length):
                helper = Helper()
                try:
                    def answer(connection):
                        self.assertEqual(wire_read(connection, 1), b'Q')
                        size = struct.unpack('!I', wire_read(connection, 4))[0]
                        wire_read(connection, size - 4)
                        wire_frame(connection, b'W', b'\0\0\0')
                        wire_frame(connection, b'd', b'x' * length)
                        while connection.recv(1024):
                            pass

                    with source(answer) as (port, _):
                        conninfo = (f'host=127.0.0.1 port={port} user=postgres dbname=postgres '
                                    'sslmode=disable gssencmode=disable')
                        self.assertEqual(helper.call(b'O', string(conninfo) + string('fallback'))[0], b'O')
                        self.assertEqual(helper.call(b'Q', struct.pack('!I', 4096)
                                                     + string('START_STREAMING'))[0], b'B')
                        kind, body = helper.call(b'R', struct.pack('!II', maximum, 1000))
                        if length <= maximum:
                            self.assertEqual((kind, body), (b'D', b'x' * length))
                        else:
                            self.assertEqual(kind, b'!')
                            self.assertEqual(body[:5], b'08P01')
                        helper.close()
                finally:
                    helper.close()

    def test_frozen_open_ignores_service_environment_and_keeps_database_literal(self):
        def answer(connection):
            self.assertEqual(wire_read(connection, 1), b'Q')
            size = struct.unpack('!I', wire_read(connection, 4))[0]
            self.assertEqual(wire_read(connection, size - 4), b'IDENTIFY_SYSTEM\0')
            field = b'systemid\0' + struct.pack('!IhIhih', 0, 0, 25, -1, -1, 0)
            wire_frame(connection, b'T', struct.pack('!H', 1) + field)
            wire_frame(connection, b'D', struct.pack('!H', 1) + string('42'))
            wire_frame(connection, b'C', b'IDENTIFY_SYSTEM\0')
            wire_frame(connection, b'Z', b'I')
            self.assertEqual(wire_read(connection, 1), b'X')
            self.assertEqual(wire_read(connection, 4), struct.pack('!I', 4))

        helper = Helper(env=dict(os.environ, PGSERVICE='missing-service-must-not-resolve',
                                 PGHOST='wrong.invalid', PGAPPNAME='wrong-name'))
        self.addCleanup(helper.close)
        with source(answer) as (port, parameters):
            options = [('host', 'tls-host.invalid'), ('hostaddr', '127.0.0.1'),
                       ('port', str(port)), ('dbname', 'db=literal'), ('user', 'postgres'),
                       ('sslmode', 'disable'), ('gssencmode', 'disable'),
                       ('application_name', 'frozen-name'), ('target_session_attrs', 'read-write')]
            body = struct.pack('!I', len(options)) + b''.join(string(key) + string(value)
                                                            for key, value in options)
            self.assertEqual(helper.call(b'P', body)[0], b'O')
            self.assertEqual(parameters[b'database'], b'db=literal')
            self.assertEqual(parameters[b'application_name'], b'frozen-name')
            self.assertEqual(parameters[b'replication'], b'database')
            self.assertEqual(helper.call(b'Q', struct.pack('!I', 4096) + string('IDENTIFY_SYSTEM'))[0], b'T')
            self.assertEqual(helper.call(b'X'), (b'A', b''))
            helper.process.wait(timeout=2)

    def test_physical_open_overrides_database_and_replication_options(self):
        def answer(connection):
            self.assertEqual(wire_read(connection, 1), b'X')
            self.assertEqual(wire_read(connection, 4), struct.pack('!I', 4))

        with source(answer) as (port, parameters):
            helper = Helper()
            try:
                conninfo = (f'host=127.0.0.1 port={port} user=postgres dbname=unused '
                            'replication=database sslmode=disable gssencmode=disable')
                self.assertEqual(helper.call(b'W', string(conninfo) + string('physical-fallback'))[0], b'O')
                self.assertEqual(parameters[b'replication'], b'true')
                self.assertEqual(parameters[b'database'], b'replication')
                self.assertEqual(parameters[b'application_name'], b'physical-fallback')
                self.assertEqual(helper.call(b'X'), (b'A', b''))
                helper.process.wait(timeout=2)
            finally:
                helper.close()

    def test_frozen_physical_open_preserves_replication_mode(self):
        def answer(connection):
            self.assertEqual(wire_read(connection, 1), b'X')
            self.assertEqual(wire_read(connection, 4), struct.pack('!I', 4))

        with source(answer) as (port, parameters):
            helper = Helper(env=dict(os.environ, PGREPLICATION='database', PGDATABASE='wrong'))
            try:
                options = [('host', '127.0.0.1'), ('hostaddr', '127.0.0.1'),
                           ('port', str(port)), ('dbname', 'replication'), ('user', 'postgres'),
                           ('replication', 'true'), ('sslmode', 'disable'), ('gssencmode', 'disable')]
                body = struct.pack('!I', len(options)) + b''.join(string(key) + string(value)
                                                                for key, value in options)
                self.assertEqual(helper.call(b'P', body)[0], b'O')
                self.assertEqual(parameters[b'replication'], b'true')
                self.assertEqual(parameters[b'database'], b'replication')
                self.assertEqual(helper.call(b'X'), (b'A', b''))
                helper.process.wait(timeout=2)
            finally:
                helper.close()

    def test_frozen_open_rejects_service_and_missing_selected_address(self):
        for options in ([('service', 'must-not-resolve')], [('host', 'localhost'), ('port', '5432')]):
            with self.subTest(options=options):
                helper = Helper()
                try:
                    body = struct.pack('!I', len(options)) + b''.join(string(key) + string(value)
                                                                    for key, value in options)
                    kind, error = helper.call(b'P', body)
                    self.assertEqual(kind, b'!')
                    self.assertEqual(error[:5], b'08P01')
                finally:
                    helper.close()

    def test_large_advertised_copy_is_contained_in_the_helper(self):
        def answer(connection):
            self.assertEqual(wire_read(connection, 1), b'Q')
            size = struct.unpack('!I', wire_read(connection, 4))[0]
            wire_read(connection, size - 4)
            wire_frame(connection, b'W', b'\0\0\0')
            connection.sendall(b'd' + struct.pack('!I', 512 * 1024 * 1024))
            while connection.recv(1024):
                pass

        with source(answer) as (port, _):
            conninfo = f'host=127.0.0.1 port={port} user=postgres sslmode=disable gssencmode=disable'
            self.assertEqual(self.helper.call(b'O', string(conninfo) + string('fallback'))[0], b'O')
            self.assertEqual(self.helper.call(b'Q', struct.pack('!I', 4096) + string('START_STREAMING'))[0], b'B')
            self.assertEqual(self.helper.call(b'R', struct.pack('!II', 65532, 1000))[0], b'!')
            self.assertNotEqual(self.helper.process.wait(timeout=2), 0)

    def test_extra_query_results_are_not_published_as_success(self):
        def answer(connection):
            self.assertEqual(wire_read(connection, 1), b'Q')
            size = struct.unpack('!I', wire_read(connection, 4))[0]
            wire_read(connection, size - 4)
            wire_frame(connection, b'C', b'ONE\0')
            wire_frame(connection, b'C', b'TWO\0')
            wire_frame(connection, b'Z', b'I')
            while connection.recv(1024):
                pass

        with source(answer) as (port, _):
            conninfo = f'host=127.0.0.1 port={port} user=postgres sslmode=disable gssencmode=disable'
            self.assertEqual(self.helper.call(b'O', string(conninfo) + string('fallback'))[0], b'O')
            kind, body = self.helper.call(b'Q', struct.pack('!I', 4096) + string('TWO_COMMANDS'))
            self.assertEqual(kind, b'!')
            self.assertEqual(body[:5], b'08P01')
            self.assertNotEqual(self.helper.process.wait(timeout=2), 0)


if __name__ == '__main__':
    unittest.main()
