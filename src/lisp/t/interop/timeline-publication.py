"""Inspect exact published history through the pinned Patroni replication driver."""
from pathlib import Path
from types import SimpleNamespace
import importlib.metadata
import sys

import psycopg2
from patroni.postgresql import Postgresql
from patroni.postgresql.connection import get_connection_cursor


def main():
    assert importlib.metadata.version('patroni') == '4.1.5'
    root, port, child, expected = Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
    path = root / 'pg_wal' / f'{child:08X}.history'
    content = None if expected == 'missing' else path.read_bytes()
    if content is not None:
        assert content.endswith(expected.encode() + b'\n'), content
        rows = Postgresql.get_history(SimpleNamespace(wal_dir=str(root / 'pg_wal'), name='history-writer'), child)
        assert rows[-1][2] == expected, rows
    for mode in ('true', 'database'):
        with get_connection_cursor(host='127.0.0.1', port=port, user='postgres',
                                   dbname='postgres', replication=mode) as cursor:
            cursor.execute('IDENTIFY_SYSTEM')
            before = cursor.fetchone()
            assert before[1] == 1, before
            try:
                cursor.execute(f'TIMELINE_HISTORY {child}')
            except psycopg2.Error as error:
                assert content is None and error.pgcode == '58P01', error
            else:
                assert content is not None
                assert cursor.fetchone() == (path.name, content.decode())
                assert cursor.statusmessage == 'TIMELINE_HISTORY'
            cursor.execute('IDENTIFY_SYSTEM')
            assert cursor.fetchone() == before
    with psycopg2.connect(host='127.0.0.1', port=port, user='postgres', dbname='postgres') as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT pg_is_in_recovery(), count(*) FROM history_rows')
            assert cursor.fetchone() == (False, 1)
    print(f'timeline-publication: child={child} expected={expected} exact bytes/Patroni/unchanged role PASS', flush=True)


if __name__ == '__main__':
    main()
