"""Compare extended-protocol binary scalar parameters and results with C."""
import struct
import sys
from parking_wire import RawConnection


def frame(kind, body):
    return kind + struct.pack('!I', len(body) + 4) + body


def cycle(conn, frames, with_message=False):
    conn.socket.sendall(b''.join(frames) + frame(b'S', b''))
    messages = []
    while True:
        kind, body = conn.receive()
        if kind == b'Z':
            return messages
        if kind == b'E':
            fields = conn.error_fields(body)
            messages.append((kind, fields.get('C'), fields.get('M')) if with_message
                            else (kind, fields.get('C')))
        elif kind in (b'D', b'T'):
            messages.append((kind, body))


def parse(sql, oids=()):
    return frame(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(oids)) +
                 b''.join(struct.pack('!I', x) for x in oids))


def bind(values=(), formats=(), results=()):
    body = b'\0\0' + struct.pack('!H', len(formats))
    body += b''.join(struct.pack('!H', f) for f in formats)
    body += struct.pack('!H', len(values))
    for v in values:
        body += struct.pack('!i', -1 if v is None else len(v))
        if v is not None:
            body += v
    body += struct.pack('!H', len(results))
    body += b''.join(struct.pack('!H', f) for f in results)
    return frame(b'B', body)


def main(ports):
    peers = [RawConnection(port, 'binary-values') for port in ports]
    try:
        for p in peers:
            p.query('CREATE TABLE binary_values (t text, v varchar, b bytea, j json, k jsonb, n numeric)')
        oids = (25, 1043, 17, 114, 3802, 1700)
        numbers = [bytes.fromhex(x) for x in (
            '00030001400000040001092902a6', '0000000000000002',
            '00000000c0000000', '00000000d0000020', '00000000f0000020',
            '000200000000000100010929', '0001ffff0000000204d2',
            '00010003c00000070001')]
        for i, number in enumerate(numbers):
            values = ('żółć'.encode(), b'', b'\x00\xff', b' {"x":1,"x":2} ',
                      b'\x01{"x":1,"x":2}', number)
            if i == 1:
                values = (None,) * 5 + (number,)
            sql = 'INSERT INTO binary_values VALUES ($1,$2,$3,$4,$5,$6)'
            outputs = [cycle(p, [parse(sql, oids), bind(values, (1,)), frame(b'E', b'\0' + struct.pack('!I', 0))]) for p in peers]
            assert outputs == [[], []], outputs
        # Cross the 1 MiB Bind spool threshold and externalize large values.
        large = b'x' * 1100000
        values = (large, large, large, b'"' + large + b'"', b'\x01"' + large + b'"', numbers[0])
        outputs = [cycle(p, [parse(sql, oids), bind(values, (1,)), frame(b'E', b'\0' + struct.pack('!I', 0))]) for p in peers]
        assert outputs == [[], []], outputs
        mixed = (b'mixed', b'varchar', b'\x00\xff', b'{"a":1}', b'\x01{}', numbers[0])
        outputs = [cycle(p, [parse(sql, oids), bind(mixed, (0, 0, 1, 0, 1, 1)),
                             frame(b'E', b'\0' + struct.pack('!I', 0))]) for p in peers]
        assert outputs == [[], []], outputs
        for formats in ((1,), (1, 0, 1, 0, 1, 1)):
            frames = [parse('SELECT * FROM binary_values'), bind(results=formats),
                      frame(b'D', b'P\0'), frame(b'E', b'\0' + struct.pack('!I', 0))]
            outputs = [cycle(p, frames) for p in peers]
            # RowDescription includes relation OIDs, which differ between peers.
            rows = [[body for kind, body in out if kind == b'D'] for out in outputs]
            assert rows[0] == rows[1] and len(rows[0]) == len(numbers) + 2
        for oid, value in ((1700, b''), (1700, bytes.fromhex('0000000011110000')),
                           (1700, bytes.fromhex('0000000000004000')),
                           (1700, bytes.fromhex('00010000000000002710')),
                           (1700, bytes.fromhex('000000000000000000')),
                           (3802, b''), (3802, b'\x02{}'),
                           (25, b'\xff'), (25, b'a\x00b'), (114, b'{')):
            column = {1700: 'n', 3802: 'k', 25: 't', 114: 'j'}[oid]
            frames = [parse(f'INSERT INTO binary_values ({column}) VALUES ($1)', (oid,)),
                      bind((value,), (1,)), frame(b'E', b'\0' + struct.pack('!I', 0))]
            outputs = [cycle(p, frames) for p in peers]
            assert outputs[0] == outputs[1] and outputs[0][0][0] == b'E', (oid, value, outputs)
        for value in (bytes.fromhex('000000001111'), bytes.fromhex('0001000000000000271000')):
            frames = [parse('INSERT INTO binary_values (n) VALUES ($1)', (1700,)),
                      bind((value,), (1,)), frame(b'E', b'\0' + struct.pack('!I', 0))]
            outputs = [cycle(p, frames, with_message=True) for p in peers]
            assert outputs[0] == outputs[1] and outputs[0][0][0] == b'E', (value, outputs)
        print('binary-values: scalar Bind and DataRow C comparisons PASS', flush=True)
    finally:
        for p in peers:
            p.close()


if __name__ == '__main__':
    main([int(x) for x in sys.argv[1:]])
