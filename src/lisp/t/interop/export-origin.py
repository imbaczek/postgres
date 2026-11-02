"""Verify source-owned export history through recycling and a manual handoff."""
import argparse
from contextlib import ExitStack
import hashlib
import importlib.metadata
import os
from pathlib import Path
import random
import re
import runpy
import shlex
import socket
import struct
import subprocess
import zlib

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.async_executor import CriticalTask
from patroni.postgresql.mpp import Null


BASE = Path(__file__).resolve().parents[2]
SEGMENT = 16 * 1024 * 1024
SEED = 0xCBF29CE484222325
api = runpy.run_path(str(Path(__file__).with_name('patroni-sync-monitor.py')))
follow = api['follow']


def fnv(data, digest=SEED):
    for byte in data:
        digest = ((digest ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return digest


def lsn(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def origin(data, identity):
    system, timeline, base, manifest = identity
    name = f'{system:016X}-{timeline:08X}-{base:016X}-{manifest:016X}.origin'
    image = (data / 'pg_physical_origins' / name).read_bytes()
    fields = image.split()
    assert len(fields) == 8 and fields[0] == b'PGPHYSORIGIN1'
    assert zlib.crc32(b' '.join(fields[:-1])) == int(fields[-1], 16)
    numbers = tuple(map(int, fields[1:7]))
    assert numbers[:4] == identity
    return numbers[4:]


def control(data):
    image = (data / 'global/pg_wal_control').read_bytes()
    fields = image.split()
    assert fields[0] == b'PGWAL6' and zlib.crc32(b' '.join(fields[:-1])) == int(fields[-1], 16)
    assert fields[3:6] == [b'1', b'1', b'legacy']
    return int(fields[1]), int(fields[2]), int(fields[8])


def hash_history(data, cursor, endpoint, digest=SEED):
    while cursor < endpoint:
        segment, offset = divmod(cursor, SEGMENT)
        with (data / 'pg_wal' / f'{segment:016X}.wal').open('rb') as stream:
            stream.seek(offset)
            header = stream.read(32)
        start = cursor
        if not header:
            assert offset
            segment += 1
            offset = 0
            start = segment * SEGMENT
        with (data / 'pg_wal' / f'{segment:016X}.wal').open('rb') as stream:
            stream.seek(offset)
            header = stream.read(32)
            assert len(header) == 32
            magic, version, kind, size, encoded_start, _, crc = struct.unpack('<4sHHIQQI', header)
            assert magic == b'PGWL' and version == 8 and 1 <= kind <= 16
            assert encoded_start == start and 32 <= size <= SEGMENT - offset
            assert start + size <= endpoint
            if start != cursor:
                assert cursor % SEGMENT + size > SEGMENT, 'illegal rotation gap'
            raw = header + stream.read(size - 32)
        assert len(raw) == size
        checked = raw[:28] + b'\0\0\0\0' + raw[32:]
        assert zlib.crc32(checked) == crc
        digest = fnv(raw, digest)
        cursor = start + size
    assert cursor == endpoint
    return digest


def snapshot_bytes(data):
    return {str(path.relative_to(data)): hashlib.sha256(path.read_bytes()).digest()
            for path in data.rglob('*') if path.is_file()}


def check(binary):
    assert importlib.metadata.version('patroni') == '4.1.5'
    os.environ['PG_CONFIG'] = str(binary / 'pg_config')
    os.environ['PATH'] = str(binary) + os.pathsep + os.environ['PATH']
    source = api['peers']['OwnedLisp']()
    connection = None
    try:
        source.start()
        connection = api['peers']['connect'](source.port, source.process)
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE origin_rows (id integer PRIMARY KEY, payload text)')
            cursor.execute("INSERT INTO origin_rows VALUES (0,'seed')")
            cursor.execute("SELECT pg_create_physical_replication_slot('origin_clone',true)")
            cursor.execute("SELECT pg_create_physical_replication_slot('oracle_hold',true)")
        root = source.root / 'replica'
        root.mkdir(mode=0o700)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        config = follow['configuration'](source.data, port, root)
        config.update(name='origin_clone', data_dir=str(root / 'data'))
        wrapper = root / 'postgres'
        wrapper.write_text(wrapper.read_text().rstrip('\n') + ' >> '
                           + shlex.quote(str(root / 'server.log')) + ' 2>&1\n')
        node = Postgresql(config, Null())
        with ExitStack() as cleanup:
            cleanup.callback(api['stop_follower'], node)
            member = Member(0, 'source', None, {'conn_url': f'postgres://127.0.0.1:{source.port}/postgres'})
            assert node.bootstrap.clone(member) is True
            data = root / 'data'
            manifest = (data / 'physical-backup.manifest').read_bytes()
            base = int(re.search(rb':END\s+(\d+)', manifest).group(1))
            identity = (int(re.search(rb':SYSTEM-ID\s+(\d+)', manifest).group(1)),
                        1, base, fnv(manifest))
            assert origin(source.data, identity) == (base, SEED)
            assert not (data / 'pg_physical_origins').exists(), 'clone inherited local origin authority'
            assert node.follow(member, timeout=90) is True
            random_bytes = random.Random(73)
            count = 0
            endpoint = base
            with connection.cursor() as cursor:
                while endpoint < base + 2 * SEGMENT or endpoint % SEGMENT > SEGMENT - 1024 * 1024:
                    count += 1
                    assert count <= 1000, 'WAL volume did not grow as expected'
                    payload = random_bytes.randbytes(65536).hex()
                    cursor.execute('INSERT INTO origin_rows VALUES (%s,%s)', (count, payload))
                    cursor.execute('SELECT pg_current_wal_flush_lsn()::text')
                    endpoint = lsn(cursor.fetchone()[0])
                    if count % 32 == 0:
                        print(f'export-origin: rows={count} WAL={endpoint:X}', flush=True)
            follow['wait_for'](lambda: node.query(f'SELECT id FROM public.origin_rows WHERE id={count}') == [(count,)])
            follow['wait_for'](lambda: origin(source.data, identity)[0] >= endpoint)
            digest = hash_history(source.data, base, endpoint)
            assert origin(source.data, identity) == (endpoint, digest)
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_drop_replication_slot('oracle_hold')")
            connection.close()
            connection = None
            process = source.process
            source.stop()
            assert process.returncode == 0, 'primary did not exit cleanly'
            checkpoint, durable, state = control(source.data)
            assert checkpoint == durable and state == 1 and durable > endpoint
            digest = hash_history(source.data, endpoint, durable, digest)
            assert origin(source.data, identity) == (durable, digest)
            old_segment = source.data / 'pg_wal' / f'{base // SEGMENT:016X}.wal'
            assert not old_segment.exists(), 'export origin retained historical WAL'
            print('export-origin: independent multisegment digest and final checkpoint survive WAL recycling PASS', flush=True)

            source.start()
            assert not old_segment.exists(), 'startup reconstructed old WAL'
            connection = api['peers']['connect'](source.port, source.process)
            with connection.cursor() as cursor:
                count += 1
                cursor.execute("INSERT INTO origin_rows VALUES (%s,'after-restart')", (count,))
                cursor.execute('SELECT pg_current_wal_flush_lsn()::text')
                resumed = lsn(cursor.fetchone()[0])
            follow['wait_for'](lambda: node.query(f'SELECT id FROM public.origin_rows WHERE id={count}') == [(count,)])
            follow['wait_for'](lambda: origin(source.data, identity)[0] >= resumed)
            digest = hash_history(source.data, durable, resumed, digest)
            assert origin(source.data, identity) == (resumed, digest)
            connection.close()
            connection = None
            process = source.process
            source.stop()
            assert process.returncode == 0
            checkpoint, final_end, state = control(source.data)
            assert checkpoint == final_end and state == 1
            digest = hash_history(source.data, resumed, final_end, digest)
            assert origin(source.data, identity) == (final_end, digest)
            assert not old_segment.exists()
            assert node.query('SELECT count(*) FROM public.origin_rows') == [(count + 1,)]
            # Promotion validates the complete retained prefix. This case hashes
            # several segments, unlike the small follow smoke test.
            assert node.promote(180, CriticalTask()) is True
            assert node.query('SELECT pg_is_in_recovery()') == [(False,)]
            proof_path = source.root / 'promoted-proof'
            follow['control'](
                node.is_running(), root / 'target.sock',
                '(multiple-value-bind (snapshot timeline metadata) '
                '(sb-thread:with-mutex (*wal-lock*) '
                '(values (copy-tree *wal-checkpoint-snapshot*) *wal-insert-timeline* '
                '(copy-tree *wal-control-metadata*))) '
                '(physical-promotion-check-primary-retained snapshot (timeline-read-history timeline) metadata) '
                f'(with-open-file (output "{proof_path}" :direction :output :if-exists :supersede) '
                '(let ((reference (getf metadata :retained-reference))) '
                '(format output "~d ~d ~d ~d ~d" (getf reference :base-timeline) '
                '(getf reference :base) (getf reference :manifest-digest) '
                '(getf reference :end) (getf reference :end-digest)))))')
            assert tuple(map(int, proof_path.read_text().split())) == (1, base, identity[3], final_end, digest)
            before = snapshot_bytes(source.data)
            result = subprocess.run(
                [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--script',
                 str(BASE / 't/export-origin-offline-check.lisp'), str(source.data),
                 *map(str, (*identity, final_end, digest))],
                capture_output=True, text=True, timeout=90)
            (source.root / 'offline-origin-check.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stdout + result.stderr
            assert snapshot_bytes(source.data) == before, 'offline history load changed primary files'
            print('export-origin: cold restart and promoted-root commitment match after pruning PASS', flush=True)
    finally:
        if connection is not None:
            connection.close()
        source.stop(immediate=True)
        print(f'artifacts: {source.root}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    check(parser.parse_args().reference_bin.resolve())
