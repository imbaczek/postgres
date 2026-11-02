"""Reject damaged backup streams without publishing a destination directory."""
import importlib
import json
import struct
import subprocess

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection


def origin_files(source):
    directory = source.data / 'pg_physical_origins'
    return {path.name: path.read_bytes() for path in directory.glob('*.origin')}


def check_export_origins(base, source, follow, dsn, stages):
    before = origin_files(source)
    assert before, 'cancelled verified export did not retain its origin'
    command = [str(base / 'bin/pg_basebackup'), '-X', 'stream', '--dbname=' + dsn]

    def settled():
        follow['wait_for'](lambda: sorted(path.name for path in stages.iterdir()) == ['.owner'])
        assert follow['query'](source.port, 'SELECT slot_name FROM pg_replication_slots') == [('clone_one',)]

    try:
        for name in ('PG_VERSION', 'physical-backup.manifest', 'authentication.initialized'):
            form = ('(setf *physical-bootstrap-test-hook* (lambda (point target) '
                    '(when (eq point :after-manifest) '
                    f'(with-open-file (output (merge-pathnames {json.dumps(name)} target) '
                    ':direction :output :if-exists :overwrite) (write-char #\\X output)))))')
            follow['control'](source.process, source.control_socket, form)
            target = source.root / ('invalid-origin-' + name)
            result = subprocess.run(command + ['--pgdata=' + str(target)],
                                    capture_output=True, text=True, timeout=60)
            assert result.returncode != 0 and not target.exists(), (name, result.stderr)
            settled()
            assert origin_files(source) == before, 'invalid stage published origin authority'
    finally:
        follow['control'](source.process, source.control_socket, '(setf *physical-bootstrap-test-hook* nil)')

    limit_file = source.root / 'origin-limit'
    follow['control'](source.process, source.control_socket,
                      f'(with-open-file (output {json.dumps(str(limit_file))} :direction :output '
                      ':if-exists :supersede) (format output "~d" *physical-export-origin-limit*))')
    old_limit = int(limit_file.read_text())
    try:
        follow['control'](source.process, source.control_socket,
                          f'(setf *physical-export-origin-limit* {len(before)})')
        target = source.root / 'origin-capacity-rejected'
        result = subprocess.run(command + ['--pgdata=' + str(target)],
                                capture_output=True, text=True, timeout=60)
        assert result.returncode != 0 and not target.exists()
        settled()
        assert origin_files(source) == before
        assert follow['query'](source.port, 'SELECT 1') == [(1,)]
        name, image = next(iter(before.items()))
        fields = image.decode('ascii').split()
        identity = ' '.join(fields[1:5])
        follow['control'](source.process, source.control_socket,
                          f"(physical-export-origin-forget '({identity}))")
        assert not (source.data / 'pg_physical_origins' / name).exists()
        target = source.root / 'origin-capacity-reused'
        result = subprocess.run(command + ['--pgdata=' + str(target)],
                                capture_output=True, text=True, timeout=60)
        assert result.returncode == 0 and (target / 'physical-backup.manifest').exists(), result.stderr
        settled()
        assert len(origin_files(source)) == len(before)
    finally:
        follow['control'](source.process, source.control_socket,
                          f'(setf *physical-export-origin-limit* {old_limit})')
    print('native-basebackup: verified origins reject damaged stages; explicit forget restores capacity PASS',
          flush=True)


def check_hba_rejection(base, source, follow, dsn):
    hba = source.data / 'pg_hba.conf'
    original = hba.read_text()

    def rejected():
        try:
            connection = psycopg2.connect(dsn, connection_factory=PhysicalReplicationConnection)
        except psycopg2.Error as error:
            assert error.pgcode in (None, '28000'), error
            assert 'pg_hba.conf rejects replication connection' in str(error), error
            return True
        connection.close()
        return False

    def accepted():
        try:
            connection = psycopg2.connect(dsn, connection_factory=PhysicalReplicationConnection)
        except psycopg2.Error:
            return False
        connection.close()
        return True

    try:
        hba.write_text('host replication all 127.0.0.1/32 reject\n' + original)
        follow['query'](source.port, 'SELECT pg_reload_conf()')
        follow['wait_for'](rejected)
        target = source.root / 'hba-rejected-clone'
        result = subprocess.run([str(base / 'bin/pg_basebackup'), '--pgdata=' + str(target),
                                 '-X', 'stream', '--dbname=' + dsn],
                                capture_output=True, text=True, timeout=60)
        assert result.returncode != 0 and not target.exists()
        assert not list(source.root.glob('.pg-native-backups-*'))
        assert not list(source.root.glob('.pg-native-backup.*'))
    finally:
        hba.write_text(original)
        follow['query'](source.port, 'SELECT pg_reload_conf()')
        follow['wait_for'](accepted)
    print('native-basebackup: HBA rejection creates no export stage or destination PASS', flush=True)


def check_live(base, source, follow):
    root, port = source.root, source.port
    dsn = f'host=127.0.0.1 port={port} user=postgres sslmode=disable gssencmode=disable'
    before = sorted(root.glob('.pg-native-backups-*'))
    connection = psycopg2.connect(dsn, connection_factory=PhysicalReplicationConnection)
    try:
        try:
            connection.cursor().execute("BASE_BACKUP_NATIVE (native_format 'unsupported')")
        except psycopg2.Error as error:
            assert error.pgcode == '0A000', error
        else:
            raise AssertionError('unsupported backup format was accepted')
    finally:
        connection.close()
    assert sorted(root.glob('.pg-native-backups-*')) == before
    check_hba_rejection(base, source, follow, dsn)
    target = root / 'occupied'
    target.mkdir(mode=0o700)
    witness = target / '.witness'
    witness.write_bytes(b'preserve this destination')
    result = subprocess.run([str(base / 'bin/pg_basebackup'), '--pgdata=' + str(target),
                             '-X', 'stream', '--dbname=' + dsn],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    assert witness.read_bytes() == b'preserve this destination'
    assert sorted(path.name for path in target.iterdir()) == ['.witness']
    assert sorted(root.glob('.pg-native-backups-*')) == before
    ready, release = root / 'stream.ready', root / 'stream.release'
    form = ('(setf pg::*physical-bootstrap-test-hook* (lambda (point target) '
            '(declare (ignore target)) (when (eq point :before-stream) '
            f'(with-open-file (out {json.dumps(str(ready))} :direction :output '
            ':if-exists :supersede) (write-line "ready" out)) '
            f'(loop until (probe-file {json.dumps(str(release))}) do (sleep 0.02)))))')
    client = None
    try:
        follow['control'](source.process, source.control_socket, form)
        slow_target = root / 'cancelled-clone'
        client = subprocess.Popen([str(base / 'bin/pg_basebackup'), '--pgdata=' + str(slow_target),
                                   '-X', 'stream', '--dbname=' + dsn],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        follow['wait_for'](ready.exists)
        follow['query'](port, 'INSERT INTO clone_rows VALUES (99)')
        assert follow['query'](port, 'SELECT id FROM clone_rows ORDER BY id') == [(1,), (99,)]
        follow['query'](port, 'DELETE FROM clone_rows WHERE id=99')
        assert follow['query'](port, 'SELECT slot_name FROM pg_replication_slots') == [('clone_one',)]
        client.terminate()
        output, error = client.communicate(timeout=10)
        (root / 'cancelled-clone.log').write_text(output + error)
        assert client.returncode != 0
        assert not slow_target.exists()
        assert not list(root.glob('.pg-native-backup.*'))
        client = None
    finally:
        release.touch()
        if client is not None and client.poll() is None:
            client.kill()
            client.communicate(timeout=10)
        follow['control'](source.process, source.control_socket,
                          '(setf pg::*physical-bootstrap-test-hook* nil)')
    stat = source.data.stat()
    prefix = f'.pg-native-backups-{stat.st_dev:X}-{stat.st_ino:X}-'
    stages, = root.glob(prefix + '*')
    follow['wait_for'](lambda: sorted(path.name for path in stages.iterdir()) == ['.owner'])
    assert follow['query'](port, 'SELECT slot_name FROM pg_replication_slots') == [('clone_one',)]
    print('native-basebackup: format/destination refusal, SQL during slow export and disconnect cleanup PASS',
          flush=True)
    check_export_origins(base, source, follow, dsn, stages)


def capture(ipc, port):
    helper = ipc.Helper()
    frames = []
    try:
        dsn = f'host=127.0.0.1 port={port} user=postgres sslmode=disable gssencmode=disable'
        assert helper.call(b'W', ipc.string(dsn) + ipc.string('backup-oracle'))[0] == b'O'
        query = "BASE_BACKUP_NATIVE (native_format 'pg_lisp_wal_8')"
        assert helper.call(b'Q', struct.pack('!I', 4096) + ipc.string(query))[0] == b'U'
        while True:
            kind, body = helper.call(b'R', struct.pack('!II', 65536, 1000))
            if kind == b'C':
                assert b'BASE_BACKUP_NATIVE' in body
                return frames
            assert kind in (b'D', b'I'), (kind, body[:100])
            if kind == b'D':
                frames.append(body)
                assert sum(map(len, frames)) <= 16 * 1024 * 1024
    finally:
        helper.close()


def check(base, root, source_port):
    ipc = importlib.import_module('libpq-helper-tests')
    frames = capture(ipc, source_port)
    assert frames[0].startswith(b'BPGNATIVEBACKUP1') and frames[-1][0:1] == b'Z'
    for mode in ('wrong-tag', 'missing-completion', 'trailing-data', 'bundle-checksum', 'truncated'):
        selected = list(frames)
        if mode == 'bundle-checksum':
            damaged = bytearray(selected[-1])
            damaged[-1] ^= 1
            selected[-1] = bytes(damaged)
        elif mode == 'truncated':
            selected.pop()

        def answer(connection):
            assert ipc.wire_read(connection, 1) == b'Q'
            size = struct.unpack('!I', ipc.wire_read(connection, 4))[0]
            assert ipc.wire_read(connection, size - 4).startswith(b'BASE_BACKUP_NATIVE ')
            try:
                ipc.wire_frame(connection, b'H', b'\1\0\0')
                for frame in selected:
                    ipc.wire_frame(connection, b'd', frame)
                if mode == 'missing-completion':
                    return
                if mode == 'trailing-data':
                    ipc.wire_frame(connection, b'd', b'after-bundle')
                ipc.wire_frame(connection, b'c')
                tag = b'WRONG' if mode == 'wrong-tag' else b'BASE_BACKUP_NATIVE'
                ipc.wire_frame(connection, b'C', tag + b'\0')
                ipc.wire_frame(connection, b'Z', b'I')
                while connection.recv(1024):
                    pass
            except (BrokenPipeError, ConnectionResetError):
                pass

        target = root / ('fault-' + mode)
        with ipc.source(answer, timeout=30) as (port, _):
            result = subprocess.run(
                [str(base / 'bin/pg_basebackup'), '--pgdata=' + str(target), '-X', 'stream',
                 f'--dbname=host=127.0.0.1 port={port} user=postgres sslmode=disable gssencmode=disable'],
                text=True, capture_output=True, timeout=60)
        (root / (mode + '.log')).write_text(result.stdout + result.stderr)
        assert result.returncode != 0, mode
        assert not target.exists(), mode
        assert not list(root.glob('.pg-native-backup.*')), mode
    print('native-basebackup: five damaged TCP streams rejected without publication or client stages PASS', flush=True)
