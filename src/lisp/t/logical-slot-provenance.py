"""Check public logical slot identity across a native server crash and restart."""
from pathlib import Path
import re
import runpy
import signal
import time


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without optimization.')

api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
from parking_wire import RawConnection


def slot_image(server, name, timeline=1, on_pair=None):
    raw = (server.data / 'pg_replslot' / name / 'state').read_bytes()
    assert raw.endswith(b'\n') and raw.count(b'\n') == 1, raw
    fields = raw.decode('ascii').split()
    assert len(fields) == 17 and fields[0] == 'PGSLOT6', fields
    assert fields[1] == name and fields[6] == 'pgoutput', fields
    assert len(fields[9]) == 32 and all(c in '0123456789abcdef' for c in fields[9]), fields
    assert fields[10] == fields[13] == fields[14] == str(timeline), fields
    if on_pair is None:
        assert int(fields[11]) > 0 and fields[12] == '1', fields
    else:
        assert tuple(fields[11:13]) == on_pair, (fields, on_pair)
    assert fields[15] == 'none', fields
    return raw, fields


def lsn_number(value):
    high, low = value.split('/')
    return int(high, 16) << 32 | int(low, 16)


def check_restored_slots(server, connection, before, timeline=1, on_pair=None):
    for name, (raw, fields) in before.items():
        assert slot_image(server, name, timeline, on_pair)[0] == raw, name
        rows = connection.query(
            'SELECT slot_type,active,restart_lsn,confirmed_flush_lsn '
            f"FROM pg_replication_slots WHERE slot_name='{name}'")
        assert len(rows) == 1 and rows[0][:2] == ['logical', 'f'], rows
        restart, confirmed = map(lsn_number, rows[0][2:])
        assert (restart, confirmed) == (int(fields[3]), int(fields[5])), (rows, fields)


def crash(server):
    process = server.process
    server.stop(immediate=True)
    assert process.returncode == -signal.SIGKILL, process.returncode
    return process.pid


def check():
    started = time.monotonic()
    server = api['OwnedLisp']()
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE provenance_rows (id integer)')
            connection.query('INSERT INTO provenance_rows VALUES (1)')
            connection.query("SELECT pg_create_logical_replication_slot('sql_identity','pgoutput')")
            replication = RawConnection(server.port, replication='database')
            try:
                replication.query('CREATE_REPLICATION_SLOT wire_identity LOGICAL pgoutput NOEXPORT_SNAPSHOT')
            finally:
                replication.close()
            before = {name: slot_image(server, name) for name in ('sql_identity', 'wire_identity')}
            assert before['sql_identity'][1][8] == before['wire_identity'][1][8]
            assert before['sql_identity'][1][9] != before['wire_identity'][1][9]
            connection.query('INSERT INTO provenance_rows VALUES (2)')
        finally:
            connection.close()
        first_pid = crash(server)
        server.start()
        assert server.process.pid != first_pid
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2']]
            check_restored_slots(server, connection, before)
            connection.query('INSERT INTO provenance_rows VALUES (3)')
            assert connection.query('SELECT count(*) FROM provenance_rows') == [['3']]
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (SQL and wire slots, native crash/restart) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


def logical_on_pair(server):
    response = server.control(
        '(sb-thread:with-mutex (*wal-lock*) '
        '(let ((fields (wal-current-logical-decoding-fields))) '
        '(assert (getf fields :logical-decoding-enabled)) '
        '(format t "PROVENANCE-ON=~a ~a~%" '
        '(or (getf fields :logical-on-start-lsn) "-") '
        '(or (getf fields :logical-on-start-timeline) "-"))))')
    matches = re.findall(r'PROVENANCE-ON=(\d+|-) (\d+|-)', response)
    assert len(matches) == 1, response
    return matches[0]


def check_promoted():
    started = time.monotonic()
    source, child = api['OwnedLisp'](), None
    print(f'logical-slot-provenance: promoted case source={source.root}', flush=True)
    try:
        source.data.mkdir(mode=0o700)
        (source.data / 'postgresql.conf').write_text('wal_level=logical\n')
        source.start()
        child = api['OwnedLisp']()
        assert child.port != source.port
        print(f'logical-slot-provenance: promoted case child={child.root}', flush=True)
        connection = RawConnection(source.port)
        try:
            assert connection.query('SHOW wal_level') == [['logical']]
            connection.query('CREATE TABLE provenance_rows (id integer)')
            connection.query('INSERT INTO provenance_rows VALUES (1)')
            assert connection.query('SELECT count(*) FROM pg_replication_slots') == [['0']]
        finally:
            connection.close()
        replication = RawConnection(source.port, replication='database')
        try:
            identity = replication.query('IDENTIFY_SYSTEM')[0]
            assert identity[1] == '1', identity
            system_id = identity[0]
        finally:
            replication.close()
        on_pair = logical_on_pair(source)
        assert on_pair[0].isdigit() and int(on_pair[0]) > 0 and on_pair[1] == '1', on_pair
        child.data.mkdir(mode=0o700)
        source.control(f'(physical-base-backup "{child.data}/" "provenance_promotion")', timeout=60)
        (child.data / 'postgresql.conf').write_text('wal_level=logical\n')
        child.start()
        child.control('(physical-recovery-enable-streaming)')
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "provenance_promotion")')
        connection = RawConnection(source.port)
        try:
            connection.query('INSERT INTO provenance_rows VALUES (2)')
            durable = connection.query('SELECT pg_current_wal_flush_lsn()')[0][0]
        finally:
            connection.close()
        connection = RawConnection(child.port)
        try:
            assert connection.query('SHOW wal_level') == [['logical']]
            assert connection.query('SELECT pg_is_in_recovery()') == [['t']]
            deadline = time.monotonic() + 30
            while connection.query(f"SELECT pg_last_wal_replay_lsn() >= '{durable}'::pg_lsn") != [['t']]:
                assert source.process.poll() is None and child.process.poll() is None
                assert time.monotonic() < deadline, 'child did not replay source commit'
                time.sleep(0.05)
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2']]
        finally:
            connection.close()
        crash(source)
        connection = RawConnection(child.port)
        try:
            assert connection.query('SELECT pg_promote(true,30)') == [['t']]
            assert connection.query('SELECT pg_is_in_recovery()') == [['f']]
            assert connection.query("SELECT count(*) FROM pg_replication_slots WHERE slot_type='logical'") == [['0']]
            assert logical_on_pair(child) == on_pair
            history = (child.data / 'pg_wal/00000002.history').read_text().splitlines()
            parent, fork, _ = history[-1].split('\t', 2)
            assert parent == '1', history
            fork = lsn_number(fork)
            connection.query('INSERT INTO provenance_rows VALUES (3)')
            connection.query('CHECKPOINT')
            connection.query("SELECT pg_create_logical_replication_slot('sql_child_identity','pgoutput')")
            replication = RawConnection(child.port, replication='database')
            try:
                assert replication.query('IDENTIFY_SYSTEM')[0][:2] == [system_id, '2']
                replication.query('CREATE_REPLICATION_SLOT wire_child_identity LOGICAL pgoutput NOEXPORT_SNAPSHOT')
            finally:
                replication.close()
            before = {name: slot_image(child, name, 2, on_pair)
                      for name in ('sql_child_identity', 'wire_child_identity')}
            assert len({fields[9] for _, fields in before.values()}) == 2
            for _, fields in before.values():
                assert fields[8] == system_id, fields
                assert fork < int(fields[3]) <= int(fields[4]) == int(fields[5]), (fork, fields)
            check_restored_slots(child, connection, before, 2, on_pair)
            connection.query('INSERT INTO provenance_rows VALUES (4)')
            tail = lsn_number(connection.query('SELECT pg_current_wal_flush_lsn()')[0][0])
            assert all(int(fields[5]) < tail for _, fields in before.values()), (before, tail)
        finally:
            connection.close()
        first_pid = crash(child)
        child.start()
        assert child.process.pid != first_pid
        connection = RawConnection(child.port)
        try:
            assert connection.query('SELECT pg_is_in_recovery()') == [['f']]
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2'], ['3'], ['4']]
            check_restored_slots(child, connection, before, 2, on_pair)
            replication = RawConnection(child.port, replication='database')
            try:
                assert replication.query('IDENTIFY_SYSTEM')[0][:2] == [system_id, '2']
            finally:
                replication.close()
            connection.query('INSERT INTO provenance_rows VALUES (5)')
            assert connection.query('SELECT count(*) FROM provenance_rows') == [['5']]
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (new child-tail SQL/wire slots, promotion, crash/restart) '
              f'{time.monotonic() - started:.6f}s; artifacts: {child.root}', flush=True)
    finally:
        if child is not None:
            child.stop(immediate=True)
        source.stop(immediate=True)


if __name__ == '__main__':
    check()
    check_promoted()
