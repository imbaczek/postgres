"""Verify source-owned export history through recycling and a manual handoff."""
import argparse
from contextlib import ExitStack
import ctypes
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
from crc32c_reference import crc32c
from patroni_native_node import native_node

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.postgresql.misc import PostgresqlRole
from patroni.async_executor import CriticalTask
from patroni.postgresql.mpp import Null
from patroni.postgresql.rewind import Rewind


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
    assert crc32c(b' '.join(fields[:-1])) == int(fields[-1], 16)
    numbers = tuple(map(int, fields[1:7]))
    assert numbers[:4] == identity
    return numbers[4:]


def control(data):
    image = (data / 'global/pg_wal_control').read_bytes()
    fields = image.split()
    assert fields[0] == b'PGWAL6' and crc32c(b' '.join(fields[:-1])) == int(fields[-1], 16)
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
        assert crc32c(checked) == crc
        digest = fnv(raw, digest)
        cursor = start + size
    assert cursor == endpoint
    return digest


def snapshot_bytes(data):
    return {str(path.relative_to(data)): hashlib.sha256(path.read_bytes()).digest()
            for path in data.rglob('*') if path.is_file()}


def start_adopted_with_patroni(former, promoted_node, source_port, count, cleanup):
    data = former.data
    fences = [data / f'physical-adoption.{name}'
              for name in ('attempt', 'install', 'handoff', 'follow')]
    assert all(path.is_file() for path in fences)
    identity = [path.stat().st_ino for path in (data, data / '.pg_lisp.lock')]
    quarantine = former.root / 'install' / 'quarantine'
    saved = snapshot_bytes(quarantine)
    assert saved
    root = former.root / 'adopt'
    root.mkdir(mode=0o700)
    config = follow['configuration'](data, former.port, root)
    # Patroni can write its configuration outside the fenced data directory.
    # Keep the installed HBA and ident files until the postmaster retires it.
    config.update(name='clean_adopted', config_dir=str(root))
    (root / 'postgresql.conf').touch()
    config['parameters'].update(hba_file=str(data / 'pg_hba.conf'),
                                ident_file=str(data / 'pg_ident.conf'))
    node = Postgresql(config, Null())
    cleanup.callback(api['stop_follower'], node)
    assert node.start(timeout=90, role=PostgresqlRole.REPLICA) is True
    assert node.query('SELECT pg_is_in_recovery()') == [(True,)]
    assert all(not path.exists() for path in fences)
    member = Member(0, 'promoted', None, {'conn_url': f'postgres://127.0.0.1:{source_port}/postgres'})
    follow['query'](source_port, "SELECT pg_create_physical_replication_slot('clean_adopted',true)")
    assert node.follow(member, timeout=90) is True
    assert node.query('SELECT pg_is_in_recovery()') == [(True,)]
    assert all(not path.exists() for path in fences)
    follow['wait_for'](lambda: node.query(
        'SELECT status,slot_name,received_tli FROM pg_stat_get_wal_receiver()')
        == [('streaming', 'clean_adopted', 2)])
    assert follow['query'](source_port, "SELECT active FROM pg_replication_slots "
                          "WHERE slot_name='clean_adopted'") == [(True,)]
    assert node.query('SELECT count(*) FROM public.origin_rows') == [(count + 1,)]
    try:
        follow['query'](former.port, "INSERT INTO origin_rows VALUES (-1,'read-only check')")
    except follow['psycopg2'].Error as error:
        assert error.pgcode == '25006', error
    else:
        raise AssertionError('adopted standby accepted a write')
    follow['query'](source_port, f"INSERT INTO origin_rows VALUES ({count + 1},'after adoption')")
    follow['wait_for'](lambda: node.query('SELECT max(id) FROM public.origin_rows') == [(count + 1,)])
    predecessor = node.is_running()
    assert predecessor
    assert node.restart(timeout=90) is True
    restarted = node.is_running()
    assert restarted and restarted.pid != predecessor.pid
    assert node.query('SELECT pg_is_in_recovery()') == [(True,)]
    follow['query'](source_port, f"INSERT INTO origin_rows VALUES ({count + 2},'after restart')")
    follow['wait_for'](lambda: node.query('SELECT max(id) FROM public.origin_rows') == [(count + 2,)])
    old_source = promoted_node.is_running()
    assert old_source
    assert promoted_node.stop(mode='fast', checkpoint=False, stop_timeout=30)
    assert not promoted_node.is_running()
    assert node.restart(timeout=90) is True
    assert node.query('SELECT pg_is_in_recovery()') == [(True,)]
    assert node.query('SELECT max(id) FROM public.origin_rows') == [(count + 2,)]
    assert promoted_node.start(timeout=90, role=PostgresqlRole.PRIMARY) is True
    assert promoted_node.is_running().pid != old_source.pid
    assert promoted_node.query('SELECT pg_is_in_recovery()') == [(False,)]
    follow['query'](source_port, f"INSERT INTO origin_rows VALUES ({count + 3},'after source restart')")
    follow['wait_for'](lambda: node.query('SELECT max(id) FROM public.origin_rows') == [(count + 3,)])
    assert identity == [path.stat().st_ino for path in (data, data / '.pg_lisp.lock')]
    assert snapshot_bytes(quarantine) == saved
    print('export-origin: pinned Patroni clean adoption, source-offline restart and reconnect PASS', flush=True)


def check(binary, adopt=False):
    assert importlib.metadata.version('patroni') == '4.1.5'
    if adopt:
        assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
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
            stopped_node = native_node(source.data)
            facts = stopped_node.controldata()
            checkpoint_timeline = int(facts["Latest checkpoint's TimeLineID"])
            checkpoint_start = facts['Latest checkpoint location']
            assert Rewind(stopped_node)._get_checkpoint_end(checkpoint_timeline, lsn(checkpoint_start)) == final_end
            out, err = stopped_node.waldump(checkpoint_timeline + 1, checkpoint_start, 2)
            assert out == b'' and b'do not select the clean shutdown checkpoint' in err
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
                '(format output "~d ~d ~d ~d ~d ~d" (getf reference :base-timeline) '
                '(getf reference :base) (getf reference :manifest-digest) '
                '(getf reference :end) (getf reference :end-digest) (getf reference :root-id)))))')
            selected = tuple(map(int, proof_path.read_text().split()))
            assert selected[:5] == (1, base, identity[3], final_end, digest)
            before = snapshot_bytes(source.data)
            result = subprocess.run(
                [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--script',
                 str(BASE / 't/retained-proof-native.lisp'), str(source.data), str(port),
                 *map(str, (*identity, final_end, digest, selected[5]))],
                capture_output=True, text=True, timeout=240)
            (source.root / 'retained-proof-native.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stdout + result.stderr
            assert snapshot_bytes(source.data) == before, 'remote proof changed former-primary files'
            print('export-origin: native and libpq commands prove the selected root independently PASS', flush=True)
            cuts = runpy.run_path(str(Path(__file__).with_name('retained-proof-cuts.py')))
            cuts['check'](BASE, source, node, root, port, identity, final_end, digest, selected[5], follow)
            assert snapshot_bytes(source.data) == before, 'proof cuts changed former-primary files'
            stage = source.root / 'adoption-stage'
            stage.mkdir(mode=0o700)
            environment = os.environ.copy()
            if adopt:
                environment['PG_ADOPTION_INSTALL_TEST'] = '1'
                for phase in ('install', 'handoff', 'follow'):
                    workspace = source.root / phase
                    workspace.mkdir(mode=0o700)
                    environment[f'PG_ADOPTION_{phase.upper()}_WORKSPACE'] = str(workspace)
            result = subprocess.run(
                [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--script',
                 str(BASE / 't/adoption-stage-native.lisp'), str(source.data), str(stage), str(port),
                 *map(str, (*identity, final_end, digest, selected[5]))],
                capture_output=True, text=True, timeout=600 if adopt else 240, env=environment)
            (source.root / 'adoption-stage-native.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stdout + result.stderr
            if adopt:
                assert 'fenced handoff prepared' in result.stdout
                start_adopted_with_patroni(source, node, port, count, cleanup)
                return
            assert snapshot_bytes(source.data) == before, 'canonical staging changed former-primary files'
            print('export-origin: verified canonical image and remote proof agree without installing it PASS', flush=True)
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
    parser.add_argument('--patroni-adoption', action='store_true')
    args = parser.parse_args()
    check(args.reference_bin.resolve(), adopt=args.patroni_adoption)
