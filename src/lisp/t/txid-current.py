"""Compare transaction-ID SQL semantics with an owned C PostgreSQL server."""
import argparse
from pathlib import Path
import runpy


scalar = runpy.run_path(str(Path(__file__).with_name('scalar-functions-from.py')))
observe = scalar['observe']


def check(native, reference):
    queries = [
        'SELECT txid_current() = txid_current() AS same_xid',
        'SELECT f = txid_current() AS same_xid FROM txid_current() f',
        'SELECT f.v = txid_current() AS same_xid FROM pg_catalog.txid_current() f(v)',
        'SELECT txid_current(1)',
        'SELECT public.txid_current()',
        'SELECT txid_current(VARIADIC ARRAY[1])',
        'BEGIN READ ONLY',
        'SELECT txid_current() = txid_current() AS same_xid',
        'ROLLBACK',
    ]
    for query in queries:
        expected, actual = observe(reference, query), observe(native, query)
        assert actual == expected, (query, expected, actual)
    for connection in (reference, native):
        with connection.cursor() as cursor:
            cursor.execute('BEGIN')
            cursor.execute('SELECT txid_current()')
            first = cursor.fetchone()[0]
            assert cursor.description[0].type_code == 20
            cursor.execute('SELECT txid_current()')
            assert cursor.fetchone()[0] == first
            cursor.execute('COMMIT')
            cursor.execute('SELECT txid_current()')
            assert cursor.fetchone()[0] > first
            cursor.execute('BEGIN')
            cursor.execute('SET LOCAL synchronous_commit = off')
            cursor.execute('SELECT * FROM txid_current()')
            assert cursor.fetchone()[0] > first
            cursor.execute('COMMIT')
            cursor.execute('SHOW synchronous_commit')
            assert cursor.fetchone()[0] == 'on'
    print(f'txid-current: {len(queries)} C/native cases, transaction ownership and OFF commit PASS',
          flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    scalar['run_peers'](parser.parse_args().reference_bin.resolve(), check, 'txid-current')
