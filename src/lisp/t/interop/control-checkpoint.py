"""Check checkpoint SQL metadata and unchanged Patroni standby-leader monitoring."""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import runpy
import signal
import subprocess
import struct
import tempfile

import psutil
import psycopg2

from binary_values import bind, cycle, frame, parse
from parking_wire import RawConnection

from patroni.postgresql.misc import PostgresqlRole


api = runpy.run_path(str(Path(__file__).with_name('patroni-sync-monitor.py')))
BASE = Path(__file__).resolve().parents[2]
UNAVAILABLE = {5, 6, 9, 10, 11, 12, 14, 15, 16, 17, 18}
read_copy = runpy.run_path(str(Path(__file__).with_name('checkpoint-snapshot.py')))['read_copy']


def check_native_copy(directory, record):
    facts, _ = read_copy(directory)
    assert api['lsn_number'](record[0]) == facts['start']
    assert api['lsn_number'](record[1]) == facts['redo']
    segment = facts['redo'] // 16777216
    assert record[2] == f"{facts['timeline']:08X}{segment // 256:08X}{segment % 256:08X}"
    assert record[3:5] == (facts['timeline'], facts['previous'])
    assert record[7] == f"{facts['next_xid'] >> 32}:{facts['next_xid'] & 0xffffffff}"
    assert int(record[8]) == facts['next_oid']
    assert int(record[13]) == facts['active'] & 0xffffffff
    assert record[19].timestamp() == facts['time']


def check_oid8(native, reference):
    values = ('0', '4294967296', '18446744073709551615', '-1',
              '-18446744073709551615', '+17', ' 17\t', '017', '0xFF', '0b101',
              '', ' ', '08', '1_0', '0x', '1x', '18446744073709551616',
              '-18446744073709551616')
    with ExitStack() as cleanup:
        peers = []
        for connection in (native, reference):
            peer = RawConnection(int(connection.get_dsn_parameters()['port']), 'oid8-input')
            cleanup.callback(peer.close)
            peers.append(peer)
        for value in values:
            frames = [parse('SELECT $1', (6437,)), bind((value.encode(),)),
                      frame(b'D', b'P\0'), frame(b'E', b'\0' + struct.pack('!I', 0))]
            outputs = [cycle(peer, frames) for peer in peers]
            assert outputs[0] == outputs[1], (value, outputs)
            sql = 'SELECT ' + "'" + value.replace("'", "''") + "'::pg_catalog.oid8"
            outcomes = [api['peers']['result'](connection, sql)
                        for connection in (native, reference)]
            # This input oracle compares values and SQLSTATE, not source locations.
            outcomes = [outcome[:2] if outcome[0] == 'error' else outcome
                        for outcome in outcomes]
            assert outcomes[0] == outcomes[1], (value, outcomes)
    print(f'oid8: {len(values)} C/native text Bind and SQL cast cases PASS', flush=True)
    expressions = [
        '(-1::smallint)::oid8', '(-1::integer)::oid8', '(-1::bigint)::oid8',
        "('18446744073709551615'::oid8)::bigint", "('4294967295'::oid)::oid8",
        "'18446744073709551615'::oid8 > '9223372036854775807'::oid8",
        "'18446744073709551615'::oid8 = -1::bigint",
        "'0'::oid8 < -1::integer",
        "COALESCE(NULL::oid8, -1::bigint)",
    ]
    for expression in expressions:
        outputs = [api['peers']['result'](connection, 'SELECT ' + expression)
                   for connection in (native, reference)]
        assert outputs[0] == outputs[1], (expression, outputs)
    print(f'oid8: {len(expressions)} C/native numeric casts and comparisons PASS', flush=True)


def checkpoint(connection):
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM pg_catalog.pg_control_checkpoint()')
        metadata = [(column.name, column.type_code, column.internal_size)
                    for column in cursor.description]
        rows = cursor.fetchall()
        assert len(rows) == 1 and len(rows[0]) == 20, rows
        return metadata, rows[0]


def check(native, reference, server):
    check_oid8(native, reference)
    expected, _ = checkpoint(reference)
    actual, _ = checkpoint(native)
    assert actual == expected, (expected, actual)
    for connection, label in ((reference, 'C'), (native, 'native')):
        with connection.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        _, before = checkpoint(connection)
        with connection.cursor() as cursor:
            cursor.execute('SELECT to_json(next_oid),to_json(oldest_active_xid) '
                           'FROM pg_control_checkpoint()')
            oid, xid = cursor.fetchone()
            assert isinstance(oid, str) and isinstance(xid, str), (label, oid, xid)
        api['scalar'](connection, 'SELECT txid_current()')
        _, after = checkpoint(connection)
        assert before == after, (label, before, after)
        if label == 'native':
            check_native_copy(server.data, before)
            assert all(before[index] is None for index in UNAVAILABLE), before
            assert all(before[index] is not None for index in range(20)
                       if index not in UNAVAILABLE), before
        print(f'{label}: checkpoint metadata and persisted snapshot stability PASS', flush=True)
    with ExitStack() as cleanup:
        follower = api['start_follower'](native, server, 'checkpoint_peer', cleanup)
        connection = follower.connection_pool.get('heartbeat').get()
        actual, record = checkpoint(connection)
        assert actual == expected, (expected, actual)
        node, trace = api['monitor'](connection, PostgresqlRole.STANDBY_LEADER,
                                     Path(follower.data_dir))
        cleanup.callback(node.connection_pool.close)
        query = node.cluster_info_query
        assert 'pg_catalog.pg_control_checkpoint()' in query, query
        row = node.query(query)[0]
        assert len(row) == 16 and row[0] == 0, row
        assert row[5] == record[3] and row[5] > 0, (row, record)
        assert node._cluster_info_state_get('pg_control_timeline') == record[3]
        (server.root / 'patroni-standby-leader.trace').write_text('\n\n'.join(trace))
        print('native: real follower, unchanged 16-column Patroni standby-leader query PASS',
              flush=True)
        with native.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        _, source_checkpoint = checkpoint(native)
        assert source_checkpoint[0] != record[0], (record, source_checkpoint)
        barrier = api['lsn_number'](api['scalar'](native, 'SELECT pg_current_wal_flush_lsn()'))
        api['follow']['wait_for'](
            lambda: api['lsn_number'](api['scalar'](
                connection, 'SELECT pg_last_wal_replay_lsn()')) >= barrier)
        assert checkpoint(connection)[1] == record, 'replay alone changed durable checkpoint facts'
        with connection.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        assert checkpoint(connection)[1] == source_checkpoint
        node.connection_pool.close()
        follower.connection_pool.close()
        process = follower.is_running()
        assert process is not None
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=20)
        assert not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
        server.process.kill()
        assert server.process.wait(timeout=20) == -signal.SIGKILL
        (Path(follower.data_dir).parent / 'target.sock').unlink(missing_ok=True)
        assert follower.start(timeout=90, role=PostgresqlRole.REPLICA) is True
        connection = follower.connection_pool.get('heartbeat').get()
        assert checkpoint(connection)[1] == source_checkpoint
        print('native: replay does not publish checkpoint facts; restartpoint and source-offline cold restart PASS',
              flush=True)
        pointer = Path(follower.data_dir) / 'physical-restartpoint.control'
        original = pointer.read_bytes()
        try:
            pointer.write_bytes(bytes([original[0] ^ 1]) + original[1:])
            try:
                checkpoint(connection)
            except psycopg2.Error as error:
                assert error.pgcode == 'XX000', error
                assert 'restartpoint' in error.diag.message_primary.lower(), error
            else:
                raise AssertionError('corrupt selected restartpoint fell back to older facts')
        finally:
            pointer.write_bytes(original)
        assert checkpoint(connection)[1] == source_checkpoint
        print('native: corrupt selected restartpoint errors without falling back PASS', flush=True)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    arguments = parser.parse_args()
    binary = arguments.reference_bin.resolve()
    os.environ.setdefault('PG_CONFIG', str(binary / 'pg_config'))
    os.environ['PATH'] = str(binary) + os.pathsep + os.environ['PATH']
    helper = Path(tempfile.mkdtemp(prefix='pg-checkpoint-helper-')) / 'libpq-helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)],
                   check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    api['peers']['run'](binary, check, 'control-checkpoint', with_server=True)
