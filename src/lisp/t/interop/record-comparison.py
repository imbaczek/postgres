"""Compare whole-row operators with a C peer over simple and extended queries.

Usage: python3 t/interop/record-comparison.py NATIVE_PORT REFERENCE_PORT
Both peers must be isolated, trust-authenticated test servers.
"""
import struct
import sys

from parking_wire import RawConnection, WireError


def extended_query(connection, sql, parameter):
    def frame(kind, body):
        return kind + struct.pack('!I', len(body) + 4) + body

    value = parameter.encode()
    connection.socket.sendall(
        frame(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, 25))
        + frame(b'B', b'\0\0' + struct.pack('!HHI', 0, 1, len(value)) + value
                + struct.pack('!H', 0))
        + frame(b'D', b'P\0') + frame(b'E', b'\0' + struct.pack('!I', 0))
        + frame(b'S', b''))
    rows, failure = [], None
    while True:
        kind, body = connection.receive()
        if kind == b'D':
            rows.append(connection.data_row(body))
        elif kind == b'E':
            failure = WireError(connection.error_fields(body))
        elif kind == b'Z':
            if failure:
                raise failure
            return rows


def observe(connection, sql, extended):
    try:
        if extended:
            return ('rows', extended_query(connection, sql, '{"a":1}'))
        return ('rows', connection.query(sql.replace('$1', "'{\"a\":1}'")))
    except WireError as error:
        return ('error', error.sqlstate)


def main():
    cases = [
        ('SELECT q = q FROM (SELECT $1::json AS j) q', '42883'),
        ('SELECT q <> q FROM (SELECT $1::json AS j) q', '42883'),
        ('SELECT q < q FROM (SELECT $1::json AS j) q', '42883'),
        ('SELECT q = q FROM (SELECT NULL::json AS j, $1::text AS unused) q', '42883'),
        ('SELECT q = q FROM (SELECT r FROM (SELECT $1::json AS j) r) q', '42883'),
        ('SELECT q FROM (SELECT $1::json AS j UNION ALL SELECT NULL::json) q ORDER BY q', '42883'),
        ('SELECT DISTINCT q FROM (SELECT $1::json AS j UNION ALL SELECT NULL::json) q', '42883'),
        ('SELECT DISTINCT q FROM (SELECT $1::json AS j) q', None),
        ('SELECT DISTINCT q FROM (SELECT $1::jsonb AS j, 1 AS id UNION ALL SELECT NULL::jsonb, 2) q ORDER BY q', None),
        ('SELECT DISTINCT q FROM (SELECT $1::jsonb AS j, 1 AS id UNION ALL SELECT $1::jsonb, 2) q ORDER BY q DESC', None),
        ('SELECT DISTINCT q FROM (SELECT 1 AS id, $1::json AS j UNION ALL SELECT 2, NULL::json) q', None),
        ('SELECT DISTINCT q, n.id FROM (SELECT $1::json AS j) q CROSS JOIN (SELECT 1 AS id UNION ALL SELECT 2) n ORDER BY n.id', None),
        ('SELECT DISTINCT q, n.id FROM (SELECT $1::json AS j) q CROSS JOIN (SELECT 2 AS id UNION ALL SELECT 1) n ORDER BY q,n.id LIMIT 1', None),
        ('SELECT DISTINCT n.id, r FROM (SELECT 1 AS id UNION ALL SELECT 2) n CROSS JOIN LATERAL (SELECT to_json(n.id) AS j, $1::text AS unused) r ORDER BY n.id', '42883'),
        ('SELECT DISTINCT q FROM (SELECT $1::json AS j, id FROM (SELECT 1 AS id UNION ALL SELECT 2) n) q', '42883'),
        ('SELECT DISTINCT q FROM (SELECT r FROM (SELECT $1::json AS j UNION ALL SELECT NULL::json) r) q', '42883'),
        ('SELECT q = q FROM (SELECT $1::jsonb AS j) q', None),
        ('SELECT DISTINCT q FROM (SELECT $1::jsonb AS j UNION ALL SELECT NULL::jsonb) q ORDER BY q', None),
        ('SELECT q = q FROM (SELECT 1 AS id) a LEFT JOIN (SELECT $1::json AS j) q ON false', None),
        ('SELECT q = q FROM (SELECT NULL::json[] AS j, $1::text AS unused) q', '42883'),
        ('SELECT a = b FROM (SELECT ARRAY[$1::json] AS j) a CROSS JOIN (SELECT ARRAY[[$1::json]] AS j) b', '42883'),
        ('SELECT to_json(q) FROM (SELECT $1::json AS j) q', None),
        ('SELECT json_agg(q) FROM (SELECT $1::json AS j) q', None),
        ('SELECT a = b, a < b FROM (SELECT 1 AS id, $1::json AS j) a CROSS JOIN (SELECT 2 AS id, NULL::json AS j) b', None),
    ]
    count = 0
    native, reference = (RawConnection(int(port)) for port in sys.argv[1:3])
    try:
        for extended in (False, True):
            for sql, expected_error in cases:
                expected = observe(reference, sql, extended)
                actual = observe(native, sql, extended)
                assert actual == expected, (extended, sql, expected, actual)
                if expected_error:
                    assert expected == ('error', expected_error), (sql, expected)
                else:
                    assert expected[0] == 'rows', (sql, expected)
                count += 1
    finally:
        native.close()
        reference.close()
    print(f'record-comparison: {count} simple/extended C differential cases PASS')


if __name__ == '__main__':
    main()
