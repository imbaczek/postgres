"""Check public logical slot identity across a native server crash and restart."""
from pathlib import Path
import runpy
import time


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without optimization.')

api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
from parking_wire import RawConnection


def slot_image(server, name):
    raw = (server.data / 'pg_replslot' / name / 'state').read_bytes()
    assert raw.endswith(b'\n') and raw.count(b'\n') == 1, raw
    fields = raw.decode('ascii').split()
    assert len(fields) == 17 and fields[0] == 'PGSLOT6', fields
    assert fields[1] == name and fields[6] == 'pgoutput', fields
    assert len(fields[9]) == 32 and all(c in '0123456789abcdef' for c in fields[9]), fields
    assert fields[10] == fields[12] == fields[13] == fields[14] == '1', fields
    assert int(fields[11]) > 0 and fields[15] == 'none', fields
    return raw, fields


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
        first_pid = server.process.pid
        server.stop(immediate=True)
        server.start()
        assert server.process.pid != first_pid
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2']]
            for name, (raw, fields) in before.items():
                assert slot_image(server, name)[0] == raw, name
                rows = connection.query(
                    'SELECT slot_type,active,restart_lsn,confirmed_flush_lsn '
                    f"FROM pg_replication_slots WHERE slot_name='{name}'")
                assert len(rows) == 1 and rows[0][:2] == ['logical', 'f'], rows
                restart, confirmed = (int(v.split('/')[0], 16) << 32 | int(v.split('/')[1], 16)
                                      for v in rows[0][2:])
                assert (restart, confirmed) == (int(fields[3]), int(fields[5])), (rows, fields)
            connection.query('INSERT INTO provenance_rows VALUES (3)')
            assert connection.query('SELECT count(*) FROM provenance_rows') == [['3']]
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (SQL and wire slots, native crash/restart) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    check()
