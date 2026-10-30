"""Compare extended-protocol RETURNING portals with the C server."""
import struct
import sys

from binary_values import frame, parse
from parking_wire import RawConnection


def bind(formats, values=()):
    body = b'\0\0' + struct.pack('!HH', 0, len(values))
    for value in values:
        body += struct.pack('!i', -1 if value is None else len(value))
        if value is not None:
            body += value
    return frame(b'B', body + struct.pack('!H', len(formats)) +
                 b''.join(struct.pack('!H', f) for f in formats))


def describe(kind):
    return frame(b'D', kind + b'\0')


def execute(limit=0):
    return frame(b'E', b'\0' + struct.pack('!I', limit))


def normalize_description(body):
    # Relation OIDs differ across the independently initialized databases.
    body = bytearray(body)
    count = struct.unpack_from('!H', body)[0]
    offset = 2
    for _ in range(count):
        offset = body.index(0, offset) + 1
        body[offset:offset + 4] = b'\0' * 4
        offset += 18
    assert offset == len(body)
    return bytes(body)


def cycle(conn, frames):
    conn.socket.sendall(b''.join(frames) + frame(b'S', b''))
    result = []
    while True:
        kind, body = conn.receive()
        if kind == b'T':
            body = normalize_description(body)
        elif kind == b'E':
            fields = conn.error_fields(body)
            body = (fields.get('C'), fields.get('M'))
        result.append((kind, body))
        if kind == b'Z':
            return result


def compare(peers, frames, label):
    results = [cycle(peer, frames) for peer in peers]
    if results[0] != results[1]:
        summary = [[(kind, (len(body) if kind == b'D' else body))
                    for kind, body in result] for result in results]
        raise AssertionError((label, summary))
    return results[0]


def main(ports):
    peers = [RawConnection(port, 'returning-portals') for port in ports]
    try:
        for peer in peers:
            peer.query('CREATE TABLE returning_portal '
                       '(id int PRIMARY KEY, t text, v varchar, b bytea, j json, k jsonb, n numeric)')
            for column in ('t', 'v', 'b', 'j', 'k', 'n'):
                peer.query(f'ALTER TABLE returning_portal ALTER {column} SET STORAGE EXTERNAL')
        # Distinct, deterministic data cannot be compressed into a small inline value.
        text = ''.join(chr(97 + ((i * 7919 + i // 13) % 26)) for i in range(9000))
        digits = '7' + ''.join(str((i * 7 + i // 11) % 10) for i in range(1, 9000))
        values = (text.encode(), text.encode(), b'\\x' + text.encode().hex().encode(),
                  ('"' + text + '"').encode(), ('"' + text + '"').encode(), digits.encode())
        insert = ('INSERT INTO returning_portal VALUES (1,$1,$2,$3,$4,$5,$6),'
                  '(2,NULL,NULL,NULL,NULL,NULL,NULL) '
                  'RETURNING t,v,b,j,k,n')
        for formats in ((0,), (1,), (1, 0, 1, 0, 1, 1)):
            for peer in peers:
                peer.query('TRUNCATE returning_portal')
            frames = [parse(insert, (25, 1043, 17, 114, 3802, 1700)), describe(b'S'),
                      bind(formats, values), describe(b'P'), execute()]
            result = compare(peers, frames, ('insert', formats))
            assert [k for k, _ in result] == [b'1', b't', b'T', b'2', b'T', b'D', b'D', b'C', b'Z'], result
            for sql in ('UPDATE returning_portal SET id=id+10 RETURNING t,v,b,j,k,n',
                        'DELETE FROM returning_portal RETURNING t,v,b,j,k,n'):
                result = compare(peers, [parse(sql), bind(formats), describe(b'P'), execute()], sql)
                assert [k for k, _ in result] == [b'1', b'2', b'T', b'D', b'D', b'C', b'Z'], result
            result = compare(peers, [parse('DELETE FROM returning_portal RETURNING t,v,b,j,k,n'),
                                     bind(formats), describe(b'P'), execute()], 'zero rows')
            assert [k for k, _ in result] == [b'1', b'2', b'T', b'C', b'Z'], result
        for sql in ('INSERT INTO returning_portal(id) VALUES(1),(1) RETURNING id',
                    'INSERT INTO returning_portal(id) VALUES(1),(2) RETURNING 1/(2-id)'):
            result = compare(peers, [parse(sql), bind((0,)), describe(b'P'), execute()], sql)
            assert b'E' in [k for k, _ in result] and b'D' not in [k for k, _ in result], result
            assert all(peer.query('SELECT count(*) FROM returning_portal') == [['0']] for peer in peers)
        for literal in ('123456789.001200', '-0.00001000', '0.00'):
            sql = 'INSERT INTO returning_portal(id,n,k) VALUES (1,$1,$2) RETURNING n,k'
            result = compare(peers, [parse(sql, (1700, 3802)), describe(b'S'),
                                     bind((1,), (literal.encode(), b'{"a":1.20,"b":[true,null]}')),
                                     describe(b'P'), execute()], ('scaled numeric', literal))
            assert [k for k, _ in result] == [b'1', b't', b'T', b'2', b'T', b'D', b'C', b'Z'], result
            for peer in peers:
                peer.query('TRUNCATE returning_portal')
        print('returning-portals: C wire and six scalar types PASS', flush=True)
    finally:
        for peer in peers:
            peer.close()


if __name__ == '__main__':
    main([int(port) for port in sys.argv[1:]])
