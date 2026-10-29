"""Exercise the Lisp transport API through the real libpq helper."""
import importlib.util
import os
from pathlib import Path
import shlex
import struct
import subprocess

if not __debug__:
    raise RuntimeError('libpq bridge tests require Python assertions')

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('helper_tests', HERE / 'libpq-helper-tests.py')
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def frame(connection):
    kind = wire.wire_read(connection, 1)
    size = struct.unpack('!I', wire.wire_read(connection, 4))[0]
    return kind, wire.wire_read(connection, size - 4)


def query(connection):
    assert frame(connection) == (b'Q', b'IDENTIFY_SYSTEM\0')
    column = b'systemid\0' + struct.pack('!IhIhih', 0, 0, 25, -1, -1, 0)
    wire.wire_frame(connection, b'T', struct.pack('!H', 1) + column)
    wire.wire_frame(connection, b'D', struct.pack('!H', 1) + wire.string('42'))
    wire.wire_frame(connection, b'C', b'IDENTIFY_SYSTEM\0')
    wire.wire_frame(connection, b'Z', b'I')
    assert frame(connection) == (b'X', b'')


def switch_row(connection):
    columns = b''
    for name, oid, size in ((b'next_tli', 20, 8), (b'next_tli_startpos', 25, -1)):
        columns += name + b'\0' + struct.pack('!IhIhih', 0, 0, oid, size, -1, 0)
    wire.wire_frame(connection, b'T', struct.pack('!H', 2) + columns)
    wire.wire_frame(connection, b'D', struct.pack('!H', 2) + wire.string('2') + wire.string('0/40'))
    wire.wire_frame(connection, b'C', b'START_STREAMING\0')
    wire.wire_frame(connection, b'Z', b'I')


def identity(connection):
    assert frame(connection) == (b'Q', b'IDENTIFY_SYSTEM\0')
    columns = b''
    for name, oid, size in ((b'systemid', 25, -1), (b'timeline', 20, 8),
                            (b'xlogpos', 25, -1), (b'dbname', 25, -1)):
        columns += name + b'\0' + struct.pack('!IhIhih', 0, 0, oid, size, -1, 0)
    wire.wire_frame(connection, b'T', struct.pack('!H', 4) + columns)
    wire.wire_frame(connection, b'D', struct.pack('!H', 4)
                    + b''.join(wire.string(value) for value in ('42', '1', '0/40', 'postgres')))
    wire.wire_frame(connection, b'C', b'IDENTIFY_SYSTEM\0')
    wire.wire_frame(connection, b'Z', b'I')
    assert frame(connection) == (b'X', b'')


def idle(connection):
    assert frame(connection) == (b'Q', b'START_STREAMING\0')
    wire.wire_frame(connection, b'W', b'\0\0\0')
    assert frame(connection) == (b'd', b'idle-feedback')
    wire.wire_frame(connection, b'c')
    assert frame(connection) == (b'd', b'final-feedback')
    assert frame(connection) == (b'c', b'')
    switch_row(connection)
    assert frame(connection) == (b'X', b'')


def immediate(connection):
    assert frame(connection) == (b'Q', b'START_STREAMING\0')
    switch_row(connection)
    assert frame(connection) == (b'X', b'')


def copy(connection):
    assert frame(connection) == (b'Q', b'START_STREAMING\0')
    wire.wire_frame(connection, b'W', b'\0\0\0')
    wire.wire_frame(connection, b'd', b'w' + struct.pack('!QQq', 0, 65536, 0) + b'z' * 65536)
    wire.wire_frame(connection, b'c')
    assert frame(connection) == (b'd', b'truthful-feedback')
    assert frame(connection) == (b'c', b'')
    switch_row(connection)
    assert frame(connection) == (b'X', b'')


frozen_connections = 0


def frozen(connection):
    global frozen_connections
    frozen_connections += 1
    if frozen_connections == 1:
        assert frame(connection) == (b'X', b'')
    else:
        query(connection)


for mode, script in (('query', query), ('copy', copy), ('immediate', immediate),
                     ('identity', identity), ('idle', idle), ('frozen', frozen)):
    with wire.source(script, timeout=30, connections=2 if mode == 'frozen' else 1) as (port, parameters):
        environment = dict(os.environ, PG_LIBPQ_TEST_MODE=mode, PG_LIBPQ_TEST_PORT=str(port),
                           PGPASSFILE=b'/tmp/pg-libpq-nonutf8-\xff')
        result = subprocess.run(
            shlex.split(os.environ.get('SBCL', 'sbcl'))
            + ['--script', str(HERE.parent / 'libpq-bridge-runner.lisp')],
            env=environment, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)
        assert f'libpq-bridge {mode}: PASS' in result.stdout
        assert parameters[b'application_name'] == b'bridge-explicit'
        assert parameters[b'replication'] == b'database'
        assert parameters[b'client_encoding'] == b'UTF8'
        print(f'libpq-bridge {mode}: PASS', flush=True)

for mode in ('cancelled', 'connect-failure'):
    result = subprocess.run(
        shlex.split(os.environ.get('SBCL', 'sbcl'))
        + ['--script', str(HERE.parent / 'libpq-bridge-runner.lisp')],
        env=dict(os.environ, PG_LIBPQ_TEST_MODE=mode),
        capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise AssertionError(result.stdout + result.stderr)
    assert f'libpq-bridge {mode}: PASS' in result.stdout
    print(f'libpq-bridge {mode}: PASS', flush=True)
