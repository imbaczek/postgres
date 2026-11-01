"""Compare scalar function relations, metadata and evaluation counts with C."""
import argparse
from pathlib import Path
import runpy
import struct

import psycopg2


peers = runpy.run_path(str(Path(__file__).with_name('dollar-quoted-strings.py')))
run_peers = peers['run']
wire = runpy.run_path(str(Path(__file__).with_name('interop') / 'binary_values.py'))


def observe(connection, query):
    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            if cursor.description is None:
                return ('command', cursor.statusmessage)
            columns = [(c.name, c.type_code, c.internal_size, c.precision, c.scale,
                        c.table_oid, c.table_column) for c in cursor.description]
            return ('rows', columns, cursor.fetchall())
        except psycopg2.Error as error:
            return ('error', error.pgcode)


def check(native, reference):
    queries = [
        "SELECT * FROM length('abc')",
        "SELECT f FROM length('abc') AS f",
        "SELECT f.v,f FROM length('abc') AS f(v)",
        "SELECT to_json(f)::text,to_json(f.*)::text FROM length('abc') AS f(v)",
        "SELECT to_json(d)::text FROM (SELECT * FROM length('abc') f(v)) d",
        "SELECT * FROM length(NULL::text) f(v)",
        'SELECT * FROM pg_is_in_recovery()',
        "SELECT * FROM length('abc') AS \"MiX\"",
        "SELECT * FROM pg_catalog.int2abs(-2::smallint) f(v)",
        "SELECT * FROM numeric_abs(1.20::numeric(8,2)) f(v)",
        "SELECT d.x,f.v FROM (SELECT 'a' AS x UNION ALL SELECT 'abcd') d, length(d.x) f(v) ORDER BY f.v",
        "SELECT (SELECT f) FROM length('abc') f(v)",
        "SELECT to_json(f)::text FROM (SELECT 0 AS n) d LEFT JOIN LATERAL length('abc') f(v) ON false",
        "SELECT * FROM abs((SELECT sum(d.x) FROM (SELECT -2 AS x UNION ALL SELECT -3) d)) f",
        "SELECT * FROM abs((SELECT row_number() OVER ())) f",
        "SELECT json_typeof(to_json(a)) FROM pg_stat_get_activity(pg_backend_pid()) a",
        "SELECT * FROM sum(1)",
        "SELECT * FROM abs(sum(1))",
        "SELECT * FROM abs(row_number() OVER ())",
        "SELECT * FROM row_number() OVER ()",
        "SELECT * FROM no_such_scalar_function()",
        "SELECT * FROM length('a') f(a,b)",
        "SELECT (length('a')).*",
        "SELECT * FROM (SELECT 1 AS x) d RIGHT JOIN abs(d.x) f(v) ON true",
        "SELECT * FROM (SELECT 1 AS x) d FULL JOIN abs(d.x) f(v) ON true",
        "SELECT * FROM to_json(pg_stat_get_activity(NULL))",
        "SELECT * FROM json_build_array(pg_stat_get_wal_senders())",
        'CREATE SEQUENCE scalar_once_seq',
        "SELECT f.v,f.v FROM (SELECT 1 AS x UNION ALL SELECT 2) d, nextval('scalar_once_seq') f(v)",
        "SELECT nextval('scalar_once_seq')",
        'CREATE SEQUENCE scalar_corr_seq',
        "SELECT d.x,f.v,f.v FROM (SELECT 1 AS x UNION ALL SELECT 2) d, abs(nextval('scalar_corr_seq')+d.x*0) f(v) ORDER BY d.x",
        "SELECT nextval('scalar_corr_seq')",
        'CREATE SEQUENCE scalar_pull_seq',
        "WITH q AS MATERIALIZED (SELECT f.v,f.v AS w FROM (SELECT 1 AS x UNION ALL SELECT 2) d, abs(nextval('scalar_pull_seq')+d.x*0) f(v)) SELECT * FROM q ORDER BY v",
        "SELECT nextval('scalar_pull_seq')",
    ]
    for query in queries:
        expected, actual = observe(reference, query), observe(native, query)
        assert actual == expected, (query, expected, actual)
    connections = [wire['RawConnection'](int(c.get_dsn_parameters()['port']), 'scalar-from-extended')
                   for c in (reference, native)]
    try:
        for index, value in enumerate((b'abc', b'abcdefgh')):
            frames = ([wire['parse']('SELECT f.v,f FROM substr($1,1) f(v)', (25,))] if index == 0 else [])
            frames += [wire['bind']((value,), (1,), (1,)),
                      wire['frame'](b'D', b'P\0'),
                      wire['frame'](b'E', b'\0' + struct.pack('!I', 0))]
            outputs = [wire['cycle'](connection, frames, with_message=True) for connection in connections]
            assert outputs[0] == outputs[1], outputs
            assert [kind for kind, _ in outputs[0]] == [b'T', b'D'], outputs
    finally:
        for connection in connections:
            connection.close()
    print(f'scalar-functions-from: {len(queries)} C/native SQL cases and two binary Bind cases PASS',
          flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    run_peers(parser.parse_args().reference_bin.resolve(), check, 'scalar-from')
