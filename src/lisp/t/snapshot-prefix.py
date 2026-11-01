"""Fresh snapshots settle pending WAL; retained RR and portal snapshots do not."""
from pathlib import Path
import runpy
import struct


pending = runpy.run_path(str(Path(__file__).with_name('pending-pages.py')))
portal = runpy.run_path(str(Path(__file__).with_name('portal-snapshot.py')))
OwnedLisp = pending['OwnedLisp']
Connection = portal['Connection']
stage = pending['stage_maintenance_predecessor']


def check_pending(server, retained):
    expression = ('(assert (eq *queue-test-prior* *wal-pending-page-publication*))'
                  if retained else
                  '(assert (eq :released (wal-page-publication-state *queue-test-prior*))) '
                  '(assert (null *wal-pending-page-publication*))')
    server.control('(sb-thread:with-mutex (*wal-lock*) ' + expression + ')')


def run():
    server = OwnedLisp()
    writer = reader = None
    try:
        server.start()
        writer, reader = Connection(server.port), Connection(server.port)
        writer.query('CREATE TABLE snapshot_prefix_rows (id int PRIMARY KEY)')
        writer.query('INSERT INTO snapshot_prefix_rows VALUES (1), (2)')
        oid = int(writer.query("SELECT oid FROM pg_class WHERE relname = 'snapshot_prefix_rows'")[0][0])

        reader.query('BEGIN ISOLATION LEVEL REPEATABLE READ')
        assert reader.query('SELECT id FROM snapshot_prefix_rows ORDER BY id') == [['1'], ['2']]
        stage(server, oid)
        assert reader.query('SELECT id FROM snapshot_prefix_rows ORDER BY id') == [['1'], ['2']]
        check_pending(server, True)
        assert writer.query('SELECT id FROM snapshot_prefix_rows ORDER BY id') == [['1'], ['2']]
        check_pending(server, False)
        reader.query('COMMIT')
        print('snapshot-prefix: RR reuse retains debt; fresh RC capture settles it PASS', flush=True)

        reader.query('BEGIN ISOLATION LEVEL READ COMMITTED')
        reader.prepare('snapshot_prefix_rows')
        stage(server, oid)
        reader.send(b'E', b'portal\0' + struct.pack('!I', 1))
        reader.send(b'H')
        first = []
        while True:
            kind, body = reader.receive()
            assert kind != b'E', reader.error_fields(body)
            if kind == b'D':
                first.append(reader.data_row(body))
            if kind == b's':
                break
        assert len(first) == 1, first
        check_pending(server, True)
        rest, commands, status, errors = reader.execute()
        assert sorted(first + rest) == [['1'], ['2']], (first, rest)
        assert not errors and commands and status == b'T', (commands, status, errors)
        check_pending(server, True)
        reader.close_portal()
        reader.prepare('snapshot_prefix_rows', statement='fresh', portal='fresh')
        check_pending(server, False)
        reader.close_portal('fresh')
        reader.query('COMMIT')
        print('snapshot-prefix: portal Execute/resume retains debt; fresh Bind settles it PASS', flush=True)
    finally:
        if reader is not None:
            reader.close()
        if writer is not None:
            writer.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()
