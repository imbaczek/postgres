"""Compare the supported inline PL/pgSQL subset with C PostgreSQL."""
import argparse
from pathlib import Path
import runpy
import struct


scalar = runpy.run_path(str(Path(__file__).with_name('scalar-functions-from.py')))
observe = scalar['observe']
wire = scalar['wire']


def cycle(connection, frames):
    connection.socket.sendall(b''.join(frames) + wire['frame'](b'S', b''))
    result = []
    while True:
        kind, body = connection.receive()
        if kind == b'Z':
            result.append((kind, body))
            return result
        if kind == b'E':
            result.append((kind, connection.error_fields(body)['C']))
        elif kind != b'S':
            result.append((kind, body))


def check_extended(native, reference):
    connections = [wire['RawConnection'](int(c.get_dsn_parameters()['port']), 'do-extended')
                   for c in (reference, native)]
    try:
        cases = [
            # The invalid body must survive Parse and Describe unchanged.
            [wire['parse']('DO $$not valid plpgsql$$', name=b'invalid'),
             wire['frame'](b'D', b'Sinvalid\0')],
            [wire['bind'](statement=b'invalid'),
             wire['frame'](b'D', b'P\0'),
             wire['frame'](b'E', b'\0' + struct.pack('!I', 0))],
            [wire['parse']("DO $$BEGIN SET LOCAL synchronous_commit = off; PERFORM * FROM txid_current(); END;$$"),
             wire['bind'](), wire['frame'](b'D', b'P\0'),
             wire['frame'](b'E', b'\0' + struct.pack('!I', 1))],
            # DO parameters belong to the outer prepared statement only.
            [wire['parse']('DO $$BEGIN PERFORM $1; END;$$', (25,)),
             wire['bind']((b'not-visible-in-body',)),
             wire['frame'](b'E', b'\0' + struct.pack('!I', 0))],
        ]
        for frames in cases:
            outputs = [cycle(connection, frames) for connection in connections]
            assert outputs[0] == outputs[1], outputs
        assert cases
    finally:
        for connection in connections:
            connection.close()


def check(native, reference):
    queries = [
        'DO $$BEGIN END$$',
        'DO $$BEGIN SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; PERFORM 1; END;$$',
        'DO $$BEGIN PERFORM 1; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; END;$$',
        'DO $$BEGIN SET TRANSACTION ISOLATION LEVEL READ COMMITTED; PERFORM 1; END;$$',
        'DO $$BEGIN SET TRANSACTION READ ONLY; PERFORM 1; END;$$',
        'DO LANGUAGE plpgsql $$BEGIN NULL; END;$$',
        "DO $$BEGIN BEGIN PERFORM 1; NULL; END; END;$$ LANGUAGE 'plpgsql'",
        "DO $$BEGIN SET LOCAL synchronous_commit = 'off'; PERFORM * FROM pg_catalog.txid_current(); END;$$",
        'SHOW synchronous_commit',
        "SET application_name = 'do-baseline'",
        "DO $$BEGIN SET application_name = 'do-success'; END;$$",
        'SHOW application_name',
        "DO $$BEGIN SET application_name = 'do-failed'; PERFORM 1/0; END;$$",
        'SHOW application_name',
        "SET TIME ZONE 'UTC'",
        "DO $$BEGIN SET TIME ZONE 'Europe/Warsaw'; PERFORM 1/0; END;$$",
        "SELECT current_setting('timezone') AS zone",
        'BEGIN',
        "DO $$BEGIN SET LOCAL synchronous_commit = off; END;$$",
        'SHOW synchronous_commit',
        'COMMIT',
        'SHOW synchronous_commit',
        'BEGIN',
        "DO $$BEGIN SET application_name = 'do-aborted'; PERFORM 1/0; END;$$",
        'SELECT 1',
        'ROLLBACK',
        'SHOW application_name',
        "DO $$BEGIN SET LOCAL synchronous_commit = off; END;$$; SELECT current_setting('synchronous_commit')",
        'SHOW synchronous_commit',
        'CREATE SEQUENCE do_perform_seq',
        "DO $$BEGIN PERFORM nextval('do_perform_seq') FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) d; END;$$",
        "SELECT nextval('do_perform_seq')",
        "DO $$BEGIN PERFORM NULL::text; PERFORM false; END;$$",
        "DO $$not valid plpgsql$$",
        "DO LANGUAGE no_such_language $$BEGIN END$$",
        "DO LANGUAGE sql $$BEGIN END$$",
        "DO LANGUAGE plpgsql $$BEGIN END$$ LANGUAGE plpgsql",
        "DO $$BEGIN END$$ $$BEGIN END$$",
        'DO LANGUAGE plpgsql',
        "DO $$BEGIN NULL END;$$",
    ]
    for index, query in enumerate(queries, 1):
        expected, actual = observe(reference, query), observe(native, query)
        assert actual == expected, (index, query, expected, actual)
    check_extended(native, reference)
    print(f'do-perform: {len(queries)} C/native SQL cases and four extended cycles PASS',
          flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    scalar['run_peers'](parser.parse_args().reference_bin.resolve(), check, 'do-perform')
