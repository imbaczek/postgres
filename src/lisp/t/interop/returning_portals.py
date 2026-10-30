"""Compare extended-protocol RETURNING portals with the C server."""
import struct
import sys
import time

from binary_values import frame, parse
from parking_wire import RawConnection


def bind(formats, values=(), portal=b'', statement=b''):
    body = portal + b'\0' + statement + b'\0' + struct.pack('!HH', 0, len(values))
    for value in values:
        body += struct.pack('!i', -1 if value is None else len(value))
        if value is not None:
            body += value
    return frame(b'B', body + struct.pack('!H', len(formats)) +
                 b''.join(struct.pack('!H', f) for f in formats))


def describe(kind):
    return frame(b'D', kind + b'\0')


def execute(limit=0, portal=b''):
    return frame(b'E', portal + b'\0' + struct.pack('!I', limit & 0xffffffff))


def close_portal(portal):
    return frame(b'C', b'P' + portal + b'\0')


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


def expect_kinds(result, expected):
    assert [kind for kind, _ in result] == expected, result


def select_plan_cases(peers):
    for peer in peers:
        peer.query('CREATE TABLE portal_plans (id int PRIMARY KEY, grp int, v text)')
        peer.query("INSERT INTO portal_plans VALUES (1,1,'a'),(2,1,'b'),(3,2,'c'),(4,2,'d')")
        peer.query('CREATE INDEX portal_plans_grp ON portal_plans(grp)')
        peer.query('ANALYZE portal_plans')
        peer.query('CREATE TABLE portal_index_plan (id int PRIMARY KEY, v int)')
        peer.query('INSERT INTO portal_index_plan VALUES ' +
                   ','.join(f'({i},{i})' for i in range(1000)))
        peer.query('ANALYZE portal_index_plan')
        peer.query('CREATE PUBLICATION portal_plan_pub FOR TABLE portal_plans')
        peer.query('CREATE PUBLICATION portal_empty_pub FOR TABLE portal_plans')
        peer.query('ALTER PUBLICATION portal_empty_pub DROP TABLE portal_plans')
        peer.query('CREATE PUBLICATION portal_two_pub FOR TABLE portal_plans,portal_index_plan')
        peer.query('CREATE SEQUENCE portal_pull_seq')
        peer.query('CREATE SEQUENCE portal_key_seq')
        peer.query('BEGIN')

    # No sort or other blocking node may evaluate later rows during a pull.
    start = [parse('SELECT nextval($1) FROM portal_plans', (25,), name=b'pull'),
             bind((), (b'portal_pull_seq',), portal=b'pull', statement=b'pull')]
    expect_kinds(compare(peers, start, 'pull setup'), [b'1', b'2', b'Z'])
    for i in range(1, 5):
        expect_kinds(compare(peers, [execute(1, b'pull')], ('pull nextval', i)), [b'D', b's', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
    expect_kinds(compare(peers, [execute(1, b'pull')], 'pull end'), [b'C', b'Z'])

    for suffix, count in (('', 4), (' LIMIT 2', 2)):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        portal = b'ordered' if not suffix else b'ordered_limit'
        sql = 'SELECT nextval($1) FROM portal_plans ORDER BY id' + suffix
        compare(peers, [parse(sql, (25,), name=portal),
                        bind((), (b'portal_pull_seq',), portal=portal, statement=portal)],
                ('ordered setup', suffix))
        for i in range(1, count + 1):
            expect_kinds(compare(peers, [execute(1, portal)], ('ordered nextval', suffix, i)),
                         [b'D', b's', b'Z'])
            for peer in peers:
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
        expect_kinds(compare(peers, [execute(1, portal)], ('ordered end', suffix)), [b'C', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(count)]]

    for peer in peers:
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    sql = ('SELECT id,nextval($1),(pg_get_publication_tables('
           'CASE WHEN id=1 THEN $2 ELSE $3 END)).relid IS NOT NULL '
           'FROM portal_plans ORDER BY id LIMIT 2')
    compare(peers, [parse(sql, (25, 25, 25), name=b'srf_empty_first'),
                    bind((), (b'portal_pull_seq', b'portal_empty_pub', b'portal_plan_pub'),
                         portal=b'srf_empty_first', statement=b'srf_empty_first')], 'empty-first setup')
    for i in (1, 2):
        result = compare(peers, [execute(1, b'srf_empty_first')], ('empty-first fetch', i))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == [str(i + 1), str(i), 't'], result
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
    expect_kinds(compare(peers, [execute(1, b'srf_empty_first')], 'empty-first end'), [b'C', b'Z'])

    sql = ('SELECT id,(pg_get_publication_tables($1)).relid IS NOT NULL,'
           '(pg_get_publication_tables($2)).relid IS NOT NULL FROM portal_plans WHERE id=1')
    compare(peers, [parse(sql, (25, 25), name=b'srf_zip'),
                    bind((), (b'portal_two_pub', b'portal_plan_pub'), portal=b'srf_zip', statement=b'srf_zip')],
            'SRF zip setup')
    for expected in (['1', 't', 't'], ['1', 't', 'f']):
        result = compare(peers, [execute(1, b'srf_zip')], ('SRF zip', expected))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == expected, result
    expect_kinds(compare(peers, [execute(1, b'srf_zip')], 'SRF zip end'), [b'C', b'Z'])

    sql = ('SELECT id,(pg_get_publication_tables($1)).relid IS NOT NULL,'
           '(pg_get_publication_tables($2)).relid IS NOT NULL AS b '
           'FROM portal_plans ORDER BY id,b DESC LIMIT 3')
    expect_kinds(compare(peers, [parse(sql, (25, 25), name=b'srf_zip_sort'),
                                 bind((), (b'portal_two_pub', b'portal_plan_pub'),
                                      portal=b'srf_zip_sort', statement=b'srf_zip_sort')], 'sorted zip setup'),
                 [b'1', b'2', b'Z'])
    for expected in (['1', 't', 't'], ['1', 't', 'f'], ['2', 't', 't']):
        result = compare(peers, [execute(1, b'srf_zip_sort')], ('sorted zip', expected))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == expected, result
    expect_kinds(compare(peers, [execute(1, b'srf_zip_sort')], 'sorted zip end'), [b'C', b'Z'])

    for peer in peers:
        peer.query("SELECT setval('portal_key_seq',1,false)")
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    sql = ('SELECT id,nextval($3),(pg_get_publication_tables($2)).relid IS NOT NULL '
           'FROM portal_plans ORDER BY nextval($1)+0 LIMIT 3')
    expect_kinds(compare(peers, [parse(sql, (25, 25, 25), name=b'srf_hidden_key'),
                                 bind((), (b'portal_key_seq', b'portal_two_pub', b'portal_pull_seq'),
                                      portal=b'srf_hidden_key', statement=b'srf_hidden_key')], 'hidden key setup'),
                 [b'1', b'2', b'Z'])
    for i, source_id in enumerate((1, 1, 2), 1):
        result = compare(peers, [execute(1, b'srf_hidden_key')], ('hidden key fetch', i))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == [str(source_id), str(i), 't'], result
        for peer in peers:
            assert peer.query("SELECT currval('portal_key_seq')") == [['4']]
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
    expect_kinds(compare(peers, [execute(1, b'srf_hidden_key')], 'hidden key end'), [b'C', b'Z'])
    for peer in peers:
        assert peer.query("SELECT currval('portal_key_seq')") == [['4']]
        assert peer.query("SELECT currval('portal_pull_seq')") == [['3']]

    for peer in peers:
        peer.query("SELECT setval('portal_key_seq',1,false)")
    sql = ('SELECT id,(pg_get_publication_tables($1)).relid IS NOT NULL AS k,'
           '(pg_get_publication_tables(CASE WHEN nextval($2)>0 THEN $3 ELSE $3 END)).relid IS NOT NULL '
           'FROM portal_plans ORDER BY k,id LIMIT 4')
    expect_kinds(compare(peers, [parse(sql, (25, 25, 25), name=b'srf_key_with_volatile'),
                                 bind((), (b'portal_two_pub', b'portal_key_seq', b'portal_plan_pub'),
                                      portal=b'srf_key_with_volatile', statement=b'srf_key_with_volatile')],
                         'SRF key and volatile SRF setup'), [b'1', b'2', b'Z'])
    # Equal sort keys permit either order of the NULL-padded rows within each id.
    rows_by_peer = [[] for _ in peers]
    for i in range(4):
        for index, peer in enumerate(peers):
            result = cycle(peer, [execute(1, b'srf_key_with_volatile')])
            expect_kinds(result, [b'D', b's', b'Z'])
            rows_by_peer[index].append(peer.data_row(result[0][1]))
            assert peer.query("SELECT currval('portal_key_seq')") == [['4']]
    for rows in rows_by_peer:
        assert sorted(rows) == [['1', 't', 'f'], ['1', 't', 't'], ['2', 't', 'f'], ['2', 't', 't']], rows
    expect_kinds(compare(peers, [execute(1, b'srf_key_with_volatile')], 'SRF key and volatile SRF end'), [b'C', b'Z'])

    for name, order in ((b'srf_alias', 'z,id'), (b'srf_ordinal', '2,id')):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        sql = f'SELECT nextval($1),(pg_get_publication_tables($2)).relid IS NOT NULL AS z FROM portal_plans ORDER BY {order} LIMIT 2'
        compare(peers, [parse(sql, (25, 25), name=name),
                        bind((), (b'portal_pull_seq', b'portal_plan_pub'), portal=name, statement=name)],
                ('SRF sort setup', order))
        for i in (1, 2):
            result = compare(peers, [execute(1, name)], ('SRF sort fetch', order, i))
            expect_kinds(result, [b'D', b's', b'Z'])
            assert peers[0].data_row(result[0][1]) == [str(i), 't'], result
            for peer in peers:
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
        expect_kinds(compare(peers, [execute(1, name)], ('SRF sort end', order)), [b'C', b'Z'])

    for name, publication, suffix in ((b'srf_empty', b'portal_empty_pub', ''),
                                      (b'srf_limit_zero', b'portal_plan_pub', ' LIMIT 0')):
        before = [peer.query("SELECT currval('portal_pull_seq')") for peer in peers]
        sql = ('SELECT nextval($1),(pg_get_publication_tables($2)).relid IS NOT NULL '
               'FROM portal_plans ORDER BY id' + suffix)
        result = compare(peers, [parse(sql, (25, 25), name=name),
                                 bind((), (b'portal_pull_seq', publication), portal=name, statement=name),
                                 execute(1, name)], ('empty SRF or limit zero', suffix))
        expect_kinds(result, [b'1', b'2', b'C', b'Z'])
        assert [peer.query("SELECT currval('portal_pull_seq')") for peer in peers] == before

    # The bare record contains database-specific OIDs. Check frame types and
    # sequence values independently, leaving that record field opaque.
    for peer in peers:
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    sql = 'SELECT nextval($1),pg_get_publication_tables($2) FROM portal_plans ORDER BY id LIMIT 2'
    frames = [parse(sql, (25, 25), name=b'srf_bare'),
              bind((), (b'portal_pull_seq', b'portal_plan_pub'), portal=b'srf_bare', statement=b'srf_bare')]
    expect_kinds(compare(peers, frames, 'bare SRF setup'), [b'1', b'2', b'Z'])
    for expected in (1, 3):
        results = [cycle(peer, [execute(1, b'srf_bare')]) for peer in peers]
        for peer, result in zip(peers, results):
            expect_kinds(result, [b'D', b's', b'Z'])
            row = peer.data_row(result[0][1])
            assert row[0] == str(expected) and row[1], row
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(expected)]]
    expect_kinds(compare(peers, [execute(1, b'srf_bare')], 'bare SRF end'), [b'C', b'Z'])
    for peer in peers:
        assert peer.query("SELECT currval('portal_pull_seq')") == [['3']]
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    result = compare(peers, [parse(sql, (25, 25), name=b'srf_bare_empty'),
                             bind((), (b'portal_pull_seq', b'portal_empty_pub'),
                                  portal=b'srf_bare_empty', statement=b'srf_bare_empty'),
                             execute(1, b'srf_bare_empty')], 'bare empty SRF')
    expect_kinds(result, [b'1', b'2', b'C', b'Z'])
    for peer in peers:
        assert peer.query("SELECT currval('portal_pull_seq')") == [['4']]

    for peer in peers:
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    sql = 'SELECT nextval($1),pg_get_publication_tables($2) FROM portal_plans ORDER BY id'
    expect_kinds(compare(peers, [parse(sql, (25, 25), name=b'srf_bare_all'),
                                 bind((), (b'portal_pull_seq', b'portal_plan_pub'),
                                      portal=b'srf_bare_all', statement=b'srf_bare_all')],
                         'bare unlimited SRF setup'), [b'1', b'2', b'Z'])
    for expected in (1, 3, 5, 7):
        results = [cycle(peer, [execute(1, b'srf_bare_all')]) for peer in peers]
        for peer, result in zip(peers, results):
            expect_kinds(result, [b'D', b's', b'Z'])
            row = peer.data_row(result[0][1])
            assert row[0] == str(expected) and row[1], row
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(expected)]]
    expect_kinds(compare(peers, [execute(1, b'srf_bare_all')], 'bare unlimited SRF end'), [b'C', b'Z'])
    for peer in peers:
        assert peer.query("SELECT currval('portal_pull_seq')") == [['8']]

    srf = 'pg_get_publication_tables(CASE WHEN nextval($1)>0 THEN $2 ELSE $2 END)'
    for name, targets, scalar_column, expected in (
        (b'srf_arg_scalar_first', 'nextval($1),' + srf, 0, (1, 4)),
        (b'srf_arg_scalar_last', srf + ',nextval($1)', 1, (2, 5)),
    ):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        sql = 'SELECT ' + targets + ' FROM portal_plans ORDER BY id LIMIT 2'
        expect_kinds(compare(peers, [parse(sql, (25, 25), name=name),
                                     bind((), (b'portal_pull_seq', b'portal_plan_pub'),
                                          portal=name, statement=name)], ('volatile SRF argument setup', name)),
                     [b'1', b'2', b'Z'])
        for value, current in zip(expected, (2, 5)):
            for peer in peers:
                result = cycle(peer, [execute(1, name)])
                expect_kinds(result, [b'D', b's', b'Z'])
                row = peer.data_row(result[0][1])
                assert row[scalar_column] == str(value) and row[1-scalar_column], row
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(current)]]
        expect_kinds(compare(peers, [execute(1, name)], ('volatile SRF argument end', name)), [b'C', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [['5']]

    # A bare record used as a sort key is produced before sorting; the separate
    # volatile scalar is evaluated only for each returned row.
    for name, order, suffix, count in ((b'bare_sort_alias', 'r', '', 4),
                                       (b'bare_sort_ordinal', '2', '', 4),
                                       (b'bare_sort_alias_limit', 'r', ' LIMIT 2', 2),
                                       (b'bare_sort_ordinal_limit', '2', ' LIMIT 2', 2)):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        sql = ('SELECT nextval($1),pg_get_publication_tables($2) AS r '
               f'FROM portal_plans ORDER BY {order}' + suffix)
        expect_kinds(compare(peers, [parse(sql, (25, 25), name=name),
                                     bind((), (b'portal_pull_seq', b'portal_plan_pub'),
                                          portal=name, statement=name)], ('bare sort setup', name)),
                     [b'1', b'2', b'Z'])
        for expected in range(1, count + 1):
            results = [cycle(peer, [execute(1, name)]) for peer in peers]
            for peer, result in zip(peers, results):
                expect_kinds(result, [b'D', b's', b'Z'])
                row = peer.data_row(result[0][1])
                assert row[0] == str(expected) and row[1], row
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(expected)]]
        expect_kinds(compare(peers, [execute(1, name)], ('bare sort end', name)), [b'C', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(count)]]

    for peer in peers:
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    sql = 'SELECT nextval($1) = ANY(SELECT 1) FROM portal_plans ORDER BY id'
    compare(peers, [parse(sql, (25,), name=b'any_volatile'),
                    bind((), (b'portal_pull_seq',), portal=b'any_volatile', statement=b'any_volatile')],
            'ANY volatile setup')
    for i in range(1, 5):
        result = compare(peers, [execute(1, b'any_volatile')], ('ANY volatile fetch', i))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == [('t' if i == 1 else 'f')], result
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
    expect_kinds(compare(peers, [execute(1, b'any_volatile')], 'ANY volatile end'), [b'C', b'Z'])
    for peer in peers:
        assert peer.query("SELECT currval('portal_pull_seq')") == [['4']]

    for peer in peers:
        peer.query("SELECT setval('portal_pull_seq',1,false)")
        peer.query("SET application_name='mixed_before'")
    sql = 'SELECT current_setting($1),nextval($2) FROM portal_plans ORDER BY id'
    result = compare(peers, [parse(sql, (25, 25), name=b'mixed_projection'),
                             bind((), (b'application_name', b'portal_pull_seq'),
                                  portal=b'mixed_projection', statement=b'mixed_projection'),
                             execute(1, b'mixed_projection')], 'mixed projection first')
    expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
    assert peers[0].data_row(result[2][1]) == ['mixed_before', '1'], result
    for peer in peers:
        peer.query("SET application_name='mixed_after'")
    for i in range(2, 5):
        result = compare(peers, [execute(1, b'mixed_projection')], ('mixed projection', i))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == ['mixed_before', str(i)], result
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
    expect_kinds(compare(peers, [execute(1, b'mixed_projection')], 'mixed projection end'), [b'C', b'Z'])

    for peer in peers:
        peer.query("SELECT setval('portal_pull_seq',1,false)")
    sql = 'SELECT nextval($1),nextval($1) FROM portal_plans ORDER BY id'
    compare(peers, [parse(sql, (25,), name=b'two_volatile'),
                    bind((), (b'portal_pull_seq',), portal=b'two_volatile', statement=b'two_volatile')],
            'two volatile setup')
    for i in range(4):
        result = compare(peers, [execute(1, b'two_volatile')], ('two volatile', i))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == [str(2*i+1), str(2*i+2)], result
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(2*i+2)]]
    expect_kinds(compare(peers, [execute(1, b'two_volatile')], 'two volatile end'), [b'C', b'Z'])

    for name, ordering in ((b'alias_key', 'n'), (b'position_key', '1'),
                           (b'expression_key', 'nextval($1)')):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        sql = f'SELECT nextval($1) AS n FROM portal_plans ORDER BY {ordering}'
        result = compare(peers, [parse(sql, (25,), name=name),
                                 bind((), (b'portal_pull_seq',), portal=name, statement=name),
                                 execute(1, name)], ('volatile sort key', ordering))
        expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
        assert peers[0].data_row(result[2][1]) == ['1'], result
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [['4']]
        for i in range(3):
            result = compare(peers, [execute(1, name)], ('volatile key rest', ordering, i))
            expect_kinds(result, [b'D', b's', b'Z'])
            assert peers[0].data_row(result[0][1]) == [str(i + 2)], result
            for peer in peers:
                assert peer.query("SELECT currval('portal_pull_seq')") == [['4']]
        expect_kinds(compare(peers, [execute(1, name)], ('volatile key end', ordering)), [b'C', b'Z'])

    wide_project_sets = (
        (b'srf_window', 'SELECT id,row_number() OVER(ORDER BY id),nextval($1),'
         '(pg_get_publication_tables($2)).relid IS NOT NULL FROM portal_plans ORDER BY id+0',
         [['1', '1', '1', 't'], ['2', '2', '2', 't'], ['3', '3', '3', 't'], ['4', '4', '4', 't']]),
        (b'srf_aggregate', 'SELECT grp,count(*),nextval($1),'
         '(pg_get_publication_tables($2)).relid IS NOT NULL FROM portal_plans GROUP BY grp ORDER BY grp',
         [['1', '2', '1', 't'], ['2', '2', '2', 't']]),
        (b'srf_nested', 'WITH q AS MATERIALIZED(SELECT id,(pg_get_publication_tables($2)).relid IS NOT NULL AS b '
         'FROM portal_plans) SELECT id,b,nextval($1) FROM q ORDER BY id',
         [['1', 't', '1'], ['2', 't', '2'], ['3', 't', '3'], ['4', 't', '4']]),
    )
    for name, sql, expected_rows in wide_project_sets:
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        expect_kinds(compare(peers, [parse(sql, (25, 25), name=name),
                                     bind((), (b'portal_pull_seq', b'portal_plan_pub'),
                                          portal=name, statement=name)], ('wide ProjectSet setup', name)),
                     [b'1', b'2', b'Z'])
        for i, expected in enumerate(expected_rows, 1):
            result = compare(peers, [execute(1, name)], ('wide ProjectSet fetch', name, i))
            expect_kinds(result, [b'D', b's', b'Z'])
            assert peers[0].data_row(result[0][1]) == expected, result
            for peer in peers:
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
        expect_kinds(compare(peers, [execute(1, name)], ('wide ProjectSet end', name)), [b'C', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(len(expected_rows))]]

    for name, order in ((b'srf_no_from_constant', '1+0'), (b'srf_no_from_key', 'z')):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        sql = ('SELECT nextval($1),(pg_get_publication_tables($2)).relid IS NOT NULL AS z '
               'ORDER BY ' + order)
        expect_kinds(compare(peers, [parse(sql, (25, 25), name=name),
                                     bind((), (b'portal_pull_seq', b'portal_two_pub'),
                                          portal=name, statement=name)], ('no-FROM ProjectSet setup', order)),
                     [b'1', b'2', b'Z'])
        for expected in (1, 2):
            result = compare(peers, [execute(1, name)], ('no-FROM ProjectSet fetch', order, expected))
            expect_kinds(result, [b'D', b's', b'Z'])
            assert peers[0].data_row(result[0][1]) == [str(expected), 't'], result
            for peer in peers:
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(expected)]]
        expect_kinds(compare(peers, [execute(1, name)], ('no-FROM ProjectSet end', order)), [b'C', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [['2']]

    for peer in peers:
        peer.query("SET application_name='portal_before'")
    stable = ('SELECT current_setting($1),id FROM portal_plans ORDER BY id')
    result = compare(peers, [parse(stable, (25,), name=b'stable_sort'),
                             bind((), (b'application_name',), portal=b'stable_sort', statement=b'stable_sort'),
                             execute(1, b'stable_sort')], 'stable first')
    expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
    assert peers[0].data_row(result[2][1])[0] == 'portal_before', result
    for peer in peers:
        peer.query("SET application_name='portal_after'")
    result = compare(peers, [execute(0, b'stable_sort')], 'stable remainder')
    assert all(peers[0].data_row(body)[0] == 'portal_before'
               for kind, body in result if kind == b'D'), result

    cases = (
        ('index', 'SELECT v FROM portal_index_plan WHERE id=500'),
        ('join', 'SELECT a.id,b.id FROM portal_plans a JOIN portal_plans b ON a.id=b.id WHERE a.id<4'),
        ('right_join', 'SELECT a.id,b.id FROM portal_plans a RIGHT JOIN portal_plans b ON a.id=b.id+2 ORDER BY b.id'),
        ('full_join', 'SELECT a.id,b.id FROM portal_plans a FULL JOIN portal_plans b ON a.id=b.id+2 ORDER BY a.id,b.id'),
        ('lateral', 'SELECT a.id,q.n FROM portal_plans a CROSS JOIN LATERAL (SELECT a.id+1 AS n) q'),
        ('subquery', 'SELECT a.id,(SELECT a.id+1) FROM portal_plans a'),
        ('cte', 'WITH q AS MATERIALIZED (SELECT id FROM portal_plans) SELECT id FROM q'),
        ('recursive', 'WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<4) SELECT n FROM q'),
        ('union_all', 'SELECT id FROM portal_plans WHERE id<3 UNION ALL SELECT id FROM portal_plans WHERE id>2 ORDER BY id'),
        ('distinct', 'SELECT DISTINCT grp FROM portal_plans ORDER BY grp'),
        ('aggregate', 'SELECT grp,count(*) FROM portal_plans GROUP BY grp ORDER BY grp'),
        ('window', 'SELECT id,row_number() OVER (ORDER BY id) FROM portal_plans ORDER BY id'),
        ('sort', 'SELECT id FROM portal_plans ORDER BY id DESC'),
        ('top_n', 'SELECT id FROM portal_plans ORDER BY id DESC LIMIT 3'),
        ('slice', 'SELECT substr(v,1,1) FROM portal_plans ORDER BY id'),
    )
    for name, sql in cases:
        portal = name.encode()
        result = compare(peers, [parse(sql, name=portal), bind((), portal=portal, statement=portal),
                                 execute(1, portal)], (name, 'first'))
        expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
        result = compare(peers, [execute(0, portal)], (name, 'rest'))
        assert result[-2][0] == b'C' and result[-1][0] == b'Z', (name, result)
        compare(peers, [close_portal(portal)], (name, 'close'))

    sql = 'SELECT id,(pg_get_publication_tables($1)).relid IS NOT NULL FROM portal_plans ORDER BY id'
    result = compare(peers, [parse(sql, (25,), name=b'project_set'),
                             bind((), (b'portal_plan_pub',), portal=b'project_set', statement=b'project_set'),
                             execute(1, b'project_set')], 'set-returning first')
    expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
    assert peers[0].data_row(result[2][1]) == ['1', 't'], result
    result = compare(peers, [execute(0, b'project_set')], 'set-returning remainder')
    expect_kinds(result, [b'D', b'D', b'D', b'C', b'Z'])
    assert [peers[0].data_row(body) for kind, body in result if kind == b'D'] == [
        ['2', 't'], ['3', 't'], ['4', 't']]

    for suffix, count in (('', 4), (' LIMIT 2', 2)):
        for peer in peers:
            peer.query("SELECT setval('portal_pull_seq',1,false)")
        name = b'project_set_volatile' if not suffix else b'project_set_volatile_limit'
        sql = ('SELECT nextval($1),(pg_get_publication_tables($2)).relid IS NOT NULL '
               'FROM portal_plans ORDER BY id' + suffix)
        compare(peers, [parse(sql, (25, 25), name=name),
                        bind((), (b'portal_pull_seq', b'portal_plan_pub'), portal=name, statement=name)],
                ('project-set volatile setup', suffix))
        for i in range(1, count + 1):
            result = compare(peers, [execute(1, name)], ('project-set volatile fetch', suffix, i))
            expect_kinds(result, [b'D', b's', b'Z'])
            assert peers[0].data_row(result[0][1]) == [str(i), 't'], result
            for peer in peers:
                assert peer.query("SELECT currval('portal_pull_seq')") == [[str(i)]]
        expect_kinds(compare(peers, [execute(1, name)], ('project-set volatile end', suffix)), [b'C', b'Z'])
        for peer in peers:
            assert peer.query("SELECT currval('portal_pull_seq')") == [[str(count)]]

    for name, sql in ((b'left', 'SELECT id FROM portal_plans'),
                      (b'right', 'SELECT grp FROM portal_plans')):
        expect_kinds(compare(peers, [parse(sql, name=name), bind((), portal=name, statement=name),
                                     execute(1, name)], ('interleaved first', name)),
                     [b'1', b'2', b'D', b's', b'Z'])
    for name in (b'right', b'left'):
        result = compare(peers, [execute(1, name), execute(0, name)], ('interleaved rest', name))
        expect_kinds(result, [b'D', b's', b'D', b'D', b'C', b'Z'])
    compare(peers, [parse('SELECT id FROM portal_plans ORDER BY id', name=b'simple_cid_select')],
            'prepare SELECT before simple write')
    for peer in peers:
        peer.query('UPDATE portal_plans SET id=11 WHERE id=1')
    result = compare(peers, [bind((), portal=b'simple_cid_select', statement=b'simple_cid_select'),
                             execute(0, b'simple_cid_select')], 'SELECT bind after simple write')
    expect_kinds(result, [b'2', b'D', b'D', b'D', b'D', b'C', b'Z'])
    assert [peers[0].data_row(body) for kind, body in result if kind == b'D'] == [
        ['2'], ['3'], ['4'], ['11']]
    for peer in peers:
        peer.query('ROLLBACK')

    for peer in peers:
        peer.query('BEGIN')
    sql = 'SELECT 1/(id-3),nextval($1) FROM portal_plans ORDER BY id'
    result = compare(peers, [parse(sql, (25,), name=b'sort_error'),
                             bind((), (b'portal_pull_seq',), portal=b'sort_error', statement=b'sort_error'),
                             execute(1, b'sort_error')], 'sort error before first row')
    assert b'D' not in [kind for kind, _ in result], result
    assert any(kind == b'E' and body[0] == '22012' for kind, body in result), result
    for peer in peers:
        peer.query('ROLLBACK')
    print('returning-portals: resumed SELECT plan cases PASS', flush=True)


def select_portal_edges(peers):
    for peer in peers:
        peer.query('CREATE TABLE portal_select_edges(id int PRIMARY KEY)')
        peer.query('INSERT INTO portal_select_edges VALUES (1),(2),(3)')

    for limit in (-1, -2147483648):
        for peer in peers:
            peer.query('BEGIN')
        result = compare(peers, [parse('SELECT id FROM portal_select_edges ORDER BY id', name=b'edge_signed'),
                                 bind((), portal=b'edge_signed', statement=b'edge_signed'),
                                 execute(limit, b'edge_signed')], ('edge signed', limit))
        expect_kinds(result, [b'1', b'2', b'D', b'D', b'D', b'C', b'Z'])
        for peer in peers:
            peer.query('ROLLBACK')
        compare(peers, [frame(b'C', b'Sedge_signed\0')], 'close signed statement')

    writers = [RawConnection(peer.socket.getpeername()[1], 'select-edge-writer') for peer in peers]
    try:
        for peer in peers:
            peer.query('BEGIN')
        compare(peers, [parse('SELECT id FROM portal_select_edges ORDER BY id', name=b'edge_snapshot'),
                        bind((), portal=b'edge_snapshot', statement=b'edge_snapshot')], 'edge snapshot bind')
        for writer in writers:
            writer.query('INSERT INTO portal_select_edges VALUES (4)')
        result = compare(peers, [execute(0, b'edge_snapshot')], 'edge snapshot execute')
        expect_kinds(result, [b'D', b'D', b'D', b'C', b'Z'])
        assert [body[6:] for kind, body in result if kind == b'D'] == [b'1', b'2', b'3']
        for peer in peers:
            peer.query('ROLLBACK')
        for writer in writers:
            writer.query('DELETE FROM portal_select_edges WHERE id=4')
    finally:
        for writer in writers:
            writer.close()

    for peer in peers:
        peer.query('BEGIN')
    sql = 'SELECT id FROM portal_select_edges ORDER BY id'
    compare(peers, [parse(sql, name=b'edge_cid'), bind((), portal=b'edge_old', statement=b'edge_cid')],
            'edge old bind')
    expect_kinds(compare(peers, [parse('UPDATE portal_select_edges SET id=11 WHERE id=1', name=b'edge_update'),
                                 bind((), portal=b'edge_update', statement=b'edge_update'),
                                 execute(0, b'edge_update')], 'edge update'), [b'1', b'2', b'C', b'Z'])
    compare(peers, [bind((), portal=b'edge_new', statement=b'edge_cid')], 'edge new bind')
    old = compare(peers, [execute(0, b'edge_old')], 'edge old command snapshot')
    new = compare(peers, [execute(0, b'edge_new')], 'edge new command snapshot')
    assert [body[6:] for kind, body in old if kind == b'D'] == [b'1', b'2', b'3']
    assert [body[6:] for kind, body in new if kind == b'D'] == [b'2', b'3', b'11']
    for peer in peers:
        peer.query('ROLLBACK')

    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [bind((), portal=b'edge_exact', statement=b'edge_cid'), execute(1, b'edge_exact')],
            'edge first')
    expect_kinds(compare(peers, [execute(2, b'edge_exact')], 'edge exact'), [b'D', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(1, b'edge_exact')], 'edge completion'), [b'C', b'Z'])
    compare(peers, [execute(1, b'edge_exact')], 'edge completed repeat')
    expect_kinds(compare(peers, [close_portal(b'edge_exact')], 'edge named close'), [b'3', b'Z'])
    compare(peers, [execute(1, b'edge_exact')], 'edge execute after close')
    for peer in peers:
        peer.query('ROLLBACK')
        peer.query('BEGIN')
    empty = [parse('SELECT id FROM portal_select_edges WHERE false', name=b'edge_empty'),
             bind((), portal=b'edge_empty', statement=b'edge_empty'), execute(1, b'edge_empty')]
    expect_kinds(compare(peers, empty, 'edge empty'), [b'1', b'2', b'C', b'Z'])
    compare(peers, [execute(1, b'edge_empty')], 'edge empty repeat')
    for peer in peers:
        peer.query('ROLLBACK')

    for with_sync in (True, False):
        for peer in peers:
            peer.query('BEGIN')
            peer.query("SET statement_timeout='100ms'")
        frames = [bind((), portal=b'edge_timed', statement=b'edge_cid'), execute(1, b'edge_timed')]
        if with_sync:
            expect_kinds(compare(peers, frames, 'edge timed sync'), [b'2', b'D', b's', b'Z'])
        else:
            for peer in peers:
                peer.socket.sendall(b''.join(frames) + frame(b'H', b''))
            outcomes = [[peer.receive() for _ in range(3)] for peer in peers]
            assert outcomes[0] == outcomes[1], outcomes
            expect_kinds(outcomes[0], [b'2', b'D', b's'])
        time.sleep(0.3)
        result = compare(peers, [execute(0, b'edge_timed')], ('edge timeout', with_sync))
        expect_kinds(result, [b'D', b'D', b'C', b'Z'] if with_sync else [b'E', b'Z'])
        if not with_sync:
            assert result[0][1][0] == '57014', result
        for peer in peers:
            peer.query('ROLLBACK')

    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT 1/(3-id) FROM portal_select_edges', name=b'edge_error'),
                    bind((), portal=b'edge_error', statement=b'edge_error'), execute(1, b'edge_error')],
            'edge error first')
    result = compare(peers, [execute(0, b'edge_error')], 'edge error continuation')
    assert any(kind == b'E' and body[0] == '22012' for kind, body in result), result
    for request in (frame(b'D', b'Pedge_error\0'), execute(1, b'edge_error')):
        result = compare(peers, [request], 'edge aborted portal identity')
        expect_kinds(result, [b'E', b'Z'])
        assert result[0][1][0] == '25P02', result
    for peer in peers:
        peer.query('ROLLBACK')
    print('returning-portals: SELECT portal edge cases PASS', flush=True)


def resumed_portals(peers, values):
    for peer in peers:
        peer.query('CREATE TABLE portal_resume (id int PRIMARY KEY, v text)')
        peer.query('ALTER TABLE portal_resume ALTER v SET STORAGE EXTERNAL')
        peer.query('CREATE SEQUENCE portal_sequence')
        peer.query('INSERT INTO portal_resume(id) VALUES (1),(2),(3)')
        peer.query('CREATE TABLE portal_suspended_cid(id int)')

    # Rebind a statement parsed before the RETURNING command; no Parse or
    # completed Execute intervenes between suspension and this Bind.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT count(*) FROM portal_suspended_cid', name=b'pre_dml_select'),
                    bind((), portal=b'pre_dml_old', statement=b'pre_dml_select')],
            'prepare select before suspended DML')
    expect_kinds(compare(peers, [parse('INSERT INTO portal_suspended_cid VALUES(1),(2) RETURNING id',
                                      name=b'suspended_insert'),
                                 bind((), portal=b'suspended_insert', statement=b'suspended_insert'),
                                 execute(1, b'suspended_insert')], 'suspended insert'),
                 [b'1', b'2', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [bind((), portal=b'pre_dml_new', statement=b'pre_dml_select')],
                         'bind select after suspended DML'), [b'2', b'Z'])
    old = compare(peers, [execute(0, b'pre_dml_old')], 'old portal after suspended DML')
    new = compare(peers, [execute(0, b'pre_dml_new')], 'new portal after suspended DML')
    expect_kinds(old, [b'D', b'C', b'Z'])
    expect_kinds(new, [b'D', b'C', b'Z'])
    assert peers[0].data_row(old[0][1]) == ['0'], old
    assert peers[0].data_row(new[0][1]) == ['2'], new
    expect_kinds(compare(peers, [execute(0, b'suspended_insert')], 'finish suspended insert'),
                 [b'D', b'C', b'Z'])
    for peer in peers:
        peer.query('ROLLBACK')

    # Flush does not advance the command counter: a pre-parsed SELECT rebound
    # before Sync still sees the command's pre-insert snapshot.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT count(*) FROM portal_suspended_cid', name=b'pre_flush_select')],
            'prepare select before flush-only DML')
    frames = [parse('INSERT INTO portal_suspended_cid VALUES(1),(2) RETURNING id', name=b'flush_insert'),
              bind((), portal=b'flush_insert', statement=b'flush_insert'), execute(1, b'flush_insert')]
    for peer in peers:
        peer.socket.sendall(b''.join(frames) + frame(b'H', b''))
    outcomes = [[peer.receive() for _ in range(4)] for peer in peers]
    assert outcomes[0] == outcomes[1], outcomes
    expect_kinds(outcomes[0], [b'1', b'2', b'D', b's'])
    frames = [bind((), portal=b'pre_flush_select', statement=b'pre_flush_select'),
              execute(0, b'pre_flush_select')]
    for peer in peers:
        peer.socket.sendall(b''.join(frames) + frame(b'H', b''))
    outcomes = [[peer.receive() for _ in range(3)] for peer in peers]
    assert outcomes[0] == outcomes[1], outcomes
    expect_kinds(outcomes[0], [b'2', b'D', b'C'])
    assert peers[0].data_row(outcomes[0][1][1]) == ['0'], outcomes
    expect_kinds(compare(peers, [], 'sync after flush-only DML'), [b'Z'])
    for peer in peers:
        peer.query('ROLLBACK')

    # Closing the suspended writer before Sync must not lose its used command
    # ID. The old SELECT keeps its snapshot while a later Bind sees the writes.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT count(*) FROM portal_suspended_cid', name=b'close_cid_select'),
                    bind((), portal=b'close_cid_old', statement=b'close_cid_select')],
            'bind old before close/sync')
    frames = [parse('INSERT INTO portal_suspended_cid VALUES(1),(2) RETURNING id', name=b'close_cid_insert'),
              bind((), portal=b'close_cid_insert', statement=b'close_cid_insert'),
              execute(1, b'close_cid_insert'), close_portal(b'close_cid_insert')]
    for peer in peers:
        peer.socket.sendall(b''.join(frames) + frame(b'H', b''))
    outcomes = [[peer.receive() for _ in range(5)] for peer in peers]
    assert outcomes[0] == outcomes[1], outcomes
    expect_kinds(outcomes[0], [b'1', b'2', b'D', b's', b'3'])
    expect_kinds(compare(peers, [], 'sync after close suspended DML'), [b'Z'])
    expect_kinds(compare(peers, [bind((), portal=b'close_cid_new', statement=b'close_cid_select')],
                         'bind new after close/sync'), [b'2', b'Z'])
    old = compare(peers, [execute(0, b'close_cid_old')], 'old after close/sync')
    new = compare(peers, [execute(0, b'close_cid_new')], 'new after close/sync')
    expect_kinds(old, [b'D', b'C', b'Z'])
    expect_kinds(new, [b'D', b'C', b'Z'])
    assert peers[0].data_row(old[0][1]) == ['0'], old
    assert peers[0].data_row(new[0][1]) == ['2'], new
    for peer in peers:
        peer.query('ROLLBACK')

    for limit in (-1, -2147483648):
        for peer in peers:
            peer.query('BEGIN')
        result = compare(peers, [parse('SELECT id FROM portal_resume ORDER BY id', name=b'signed'),
                                 bind((), portal=b'signed', statement=b'signed'), execute(limit, b'signed')],
                         ('signed maxRows', limit))
        expect_kinds(result, [b'1', b'2', b'D', b'D', b'D', b'C', b'Z'])
        for peer in peers:
            peer.query('ROLLBACK')
        compare(peers, [frame(b'C', b'Ssigned\0')], 'close signed statement')

    # Bind a SELECT, change the table in another session, then execute. DML
    # obtains its own statement snapshot on first Execute, not at Bind.
    writers = [RawConnection(peer.socket.getpeername()[1], 'portal-snapshot-writer') for peer in peers]
    try:
        for peer in peers:
            peer.query('BEGIN')
        compare(peers, [parse('SELECT id FROM portal_resume ORDER BY id', name=b'bind_snapshot'),
                        bind((), portal=b'bind_snapshot', statement=b'bind_snapshot')], 'select bind snapshot')
        for writer in writers:
            writer.query('INSERT INTO portal_resume(id) VALUES(4)')
        result = compare(peers, [execute(0, b'bind_snapshot')], 'select execute bind snapshot')
        expect_kinds(result, [b'D', b'D', b'D', b'C', b'Z'])
        for peer in peers:
            peer.query('ROLLBACK')
        for writer in writers:
            writer.query('DELETE FROM portal_resume WHERE id=4')
        for peer in peers:
            peer.query('BEGIN')
        compare(peers, [parse('UPDATE portal_resume SET id=id+10 RETURNING id', name=b'dml_snapshot'),
                        bind((), portal=b'dml_snapshot', statement=b'dml_snapshot')], 'dml bind snapshot')
        for writer in writers:
            writer.query('INSERT INTO portal_resume(id) VALUES(4)')
        result = compare(peers, [execute(0, b'dml_snapshot')], 'dml execute snapshot')
        expect_kinds(result, [b'D', b'D', b'D', b'D', b'C', b'Z'])
        for peer in peers:
            peer.query('ROLLBACK')
        for writer in writers:
            writer.query('DELETE FROM portal_resume WHERE id=4')
    finally:
        for writer in writers:
            writer.close()

    # Two portals from one transaction can retain different command snapshots.
    # The older one must still see its pre-update row after a later portal writes.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT id FROM portal_resume ORDER BY id', name=b'old_cid'),
                    bind((), portal=b'old_cid', statement=b'old_cid')], 'bind old cid')
    expect_kinds(compare(peers, [parse('UPDATE portal_resume SET id=11 WHERE id=1', name=b'change_cid'),
                                 bind((), portal=b'change_cid', statement=b'change_cid'),
                                 execute(0, b'change_cid')], 'write between bindings'),
                 [b'1', b'2', b'C', b'Z'])
    compare(peers, [parse('SELECT id FROM portal_resume ORDER BY id', name=b'new_cid'),
                    bind((), portal=b'new_cid', statement=b'new_cid')], 'bind new cid')
    old = compare(peers, [execute(0, b'old_cid')], 'old cid after write')
    new = compare(peers, [execute(0, b'new_cid')], 'new cid after write')
    expect_kinds(old, [b'D', b'D', b'D', b'C', b'Z'])
    expect_kinds(new, [b'D', b'D', b'D', b'C', b'Z'])
    assert [body[6:] for kind, body in old if kind == b'D'] == [b'1', b'2', b'3']
    assert [body[6:] for kind, body in new if kind == b'D'] == [b'2', b'3', b'11']
    for peer in peers:
        peer.query('ROLLBACK')
    for name in (b'old_cid', b'change_cid', b'new_cid'):
        compare(peers, [frame(b'C', b'S' + name + b'\0')], ('close cid statement', name))

    # Rebinding an existing prepared statement after a Simple Query write must
    # observe that write even though no new Parse occurs between them.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT id FROM portal_resume ORDER BY id', name=b'simple_cid')],
            'prepare before simple write')
    for peer in peers:
        peer.query('UPDATE portal_resume SET id=11 WHERE id=1')
    result = compare(peers, [bind((), portal=b'simple_cid', statement=b'simple_cid'),
                             execute(0, b'simple_cid')], 'bind after simple write')
    expect_kinds(result, [b'2', b'D', b'D', b'D', b'C', b'Z'])
    assert [body[6:] for kind, body in result if kind == b'D'] == [b'2', b'3', b'11']
    for peer in peers:
        peer.query('ROLLBACK')
    compare(peers, [frame(b'C', b'Ssimple_cid\0')], 'close simple cid statement')

    # Sync ends the current statement timeout; Flush alone leaves it active.
    for with_sync in (True, False):
        for peer in peers:
            peer.query('BEGIN')
            peer.query("SET statement_timeout='100ms'")
        frames = [parse('SELECT id FROM portal_resume ORDER BY id', name=b'timed'),
                  bind((), portal=b'timed', statement=b'timed'), execute(1, b'timed')]
        if with_sync:
            expect_kinds(compare(peers, frames, 'timeout with sync'), [b'1', b'2', b'D', b's', b'Z'])
        else:
            for peer in peers:
                peer.socket.sendall(b''.join(frames) + frame(b'H', b''))
            outcomes = []
            for peer in peers:
                outcomes.append([peer.receive() for _ in range(4)])
            assert outcomes[0] == outcomes[1], outcomes
            expect_kinds(outcomes[0], [b'1', b'2', b'D', b's'])
        time.sleep(0.3)
        result = compare(peers, [execute(0, b'timed')], ('resume after timeout', with_sync))
        if with_sync:
            expect_kinds(result, [b'D', b'D', b'C', b'Z'])
        else:
            expect_kinds(result, [b'E', b'Z'])
            assert result[0][1][0] == '57014', result
        for peer in peers:
            peer.query('ROLLBACK')
        compare(peers, [frame(b'C', b'Stimed\0')], 'close timed statement')

    # Sync preserves named portals in an explicit transaction. Exactly matching
    # the remaining row count suspends; the next Execute discovers the end.
    for peer in peers:
        peer.query('BEGIN')
    start = [parse('SELECT id FROM portal_resume ORDER BY id', name=b's'),
             bind((), portal=b'p', statement=b's'), execute(1, b'p')]
    expect_kinds(compare(peers, start, 'select first'), [b'1', b'2', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(2, b'p')], 'select exact boundary'), [b'D', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(1, b'p')], 'select end'), [b'C', b'Z'])
    compare(peers, [execute(1, b'p')], 'execute completed select')
    compare(peers, [close_portal(b'p')], 'close completed select')
    for peer in peers:
        peer.query('COMMIT')

    # Two independent portals and a volatile expression expose accidental
    # re-execution when a client alternates between them.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT nextval($1) FROM portal_resume ORDER BY id', (25,),
                          name=b'volatile')], 'volatile parse')
    for name in (b'a', b'b'):
        expect_kinds(compare(peers, [bind((), (b'portal_sequence',), portal=name,
                                          statement=b'volatile'), execute(1, name)],
                             ('volatile first', name)), [b'2', b'D', b's', b'Z'])
    for name in (b'a', b'b'):
        expect_kinds(compare(peers, [execute(0, name)], ('volatile rest', name)),
                     [b'D', b'D', b'C', b'Z'])
    for peer in peers:
        assert peer.query("SELECT currval('portal_sequence')") == [['6']]
        peer.query('ROLLBACK')

    # All mutations occur once on the first Execute, while the stored result
    # is consumed over several Execute messages.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('UPDATE portal_resume SET id=id+10 RETURNING id', name=b'u'),
                    bind((), portal=b'u', statement=b'u'), execute(1, b'u')], 'update first')
    for peer in peers:
        assert peer.query('SELECT id FROM portal_resume ORDER BY id') == [['11'], ['12'], ['13']]
    expect_kinds(compare(peers, [execute(2, b'u')], 'update exact boundary'), [b'D', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(1, b'u')], 'update end'), [b'C', b'Z'])
    repeated = compare(peers, [execute(1, b'u')], 'execute completed update')
    expect_kinds(repeated, [b'C', b'Z'])
    assert repeated[0][1] == b'UPDATE 0\0', repeated
    for peer in peers:
        peer.query('ROLLBACK')

    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [parse('SELECT 1/(3-id) FROM portal_resume', name=b'late_error'),
                    bind((), portal=b'late_error', statement=b'late_error'), execute(1, b'late_error')],
            'late error first')
    result = compare(peers, [execute(0, b'late_error')], 'late error continuation')
    assert any(k == b'E' and body[0] == '22012' for k, body in result), result
    for request in (frame(b'D', b'Plate_error\0'), execute(1, b'late_error')):
        result = compare(peers, [request], 'failed transaction retains portal identity')
        expect_kinds(result, [b'E', b'Z'])
        assert result[0][1][0] == '25P02', result
    for peer in peers:
        peer.query('ROLLBACK')

    # All supported large scalar formats survive suspended reads and writes.
    for peer in peers:
        peer.query('BEGIN')
    sql = ('INSERT INTO returning_portal VALUES (1,$1,$2,$3,$4,$5,$6),'
           '(2,NULL,NULL,NULL,NULL,NULL,NULL) RETURNING t,v,b,j,k,n')
    formats = (1, 0, 1, 0, 1, 1)
    expect_kinds(compare(peers, [parse(sql, (25, 1043, 17, 114, 3802, 1700), name=b'large_insert'),
                                 bind(formats, values, portal=b'large_insert', statement=b'large_insert'),
                                 execute(1, b'large_insert')], 'large mixed returning first'),
                 [b'1', b'2', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(0, b'large_insert')], 'large mixed returning rest'),
                 [b'D', b'C', b'Z'])
    expect_kinds(compare(peers, [parse('SELECT t,v,b,j,k,n FROM returning_portal ORDER BY id', name=b'large_select'),
                                 bind(formats, portal=b'large_select', statement=b'large_select'),
                                 execute(1, b'large_select')], 'large mixed select first'),
                 [b'1', b'2', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(0, b'large_select')], 'large mixed select rest'),
                 [b'D', b'C', b'Z'])
    for peer in peers:
        peer.query('ROLLBACK')
        assert peer.query('SELECT id FROM portal_resume ORDER BY id') == [['1'], ['2'], ['3']]

    # Close a suspended portal, and check that a Sync outside an explicit
    # transaction ends its implicit transaction and destroys the portal.
    for peer in peers:
        peer.query('BEGIN')
    compare(peers, [bind((), portal=b'closed', statement=b's'), execute(1, b'closed')],
            'close suspended setup')
    expect_kinds(compare(peers, [close_portal(b'closed')], 'close suspended'), [b'3', b'Z'])
    compare(peers, [execute(1, b'closed')], 'execute closed portal')
    for peer in peers:
        peer.query('ROLLBACK')
    expect_kinds(compare(peers, [bind((), portal=b'implicit', statement=b's'), execute(1, b'implicit')],
                         'implicit suspended'), [b'2', b'D', b's', b'Z'])
    compare(peers, [execute(1, b'implicit')], 'implicit portal after sync')

    # Snapshot stays with the suspended SELECT despite another connection's
    # committed changes. External values exercise continuation with TOAST.
    large = ''.join(chr(97 + (i * 13 + i // 7) % 26) for i in range(9000))
    for peer in peers:
        peer.query("UPDATE portal_resume SET v='" + large + "'")
        peer.query('BEGIN')
    readers = [RawConnection(peer.socket.getpeername()[1], 'portal-writer') for peer in peers]
    try:
        compare(peers, [parse('SELECT v FROM portal_resume ORDER BY id', name=b'toast'),
                        bind((1,), portal=b'toast', statement=b'toast'), execute(1, b'toast')],
                'toast first')
        for reader in readers:
            reader.query('DELETE FROM portal_resume WHERE id=2')
            reader.query('INSERT INTO portal_resume(id) VALUES(4)')
        expect_kinds(compare(peers, [execute(0, b'toast')], 'toast snapshot continuation'),
                     [b'D', b'D', b'C', b'Z'])
    finally:
        for reader in readers:
            reader.close()
        for peer in peers:
            peer.query('ROLLBACK')

    for peer in peers:
        peer.query('BEGIN')
    expect_kinds(compare(peers, [parse('UPDATE portal_resume SET id=id+10 RETURNING v', name=b'toast_dml'),
                                 bind((1,), portal=b'toast_dml', statement=b'toast_dml'),
                                 execute(1, b'toast_dml')], 'toast returning first'),
                 [b'1', b'2', b'D', b's', b'Z'])
    expect_kinds(compare(peers, [execute(0, b'toast_dml')], 'toast returning rest'),
                 [b'D', b'D', b'C', b'Z'])
    for peer in peers:
        peer.query('ROLLBACK')

    # Empty results complete immediately and transaction end destroys portals.
    for peer in peers:
        peer.query('BEGIN')
    expect_kinds(compare(peers, [parse('SELECT id FROM portal_resume WHERE false', name=b'empty'),
                                 bind((), portal=b'empty', statement=b'empty'), execute(1, b'empty')],
                         'empty'), [b'1', b'2', b'C', b'Z'])
    for peer in peers:
        peer.query('COMMIT')
    compare(peers, [execute(1, b'empty')], 'portal after commit')
    print('returning-portals: resumable SELECT and DML wire comparisons PASS', flush=True)


def toasted_array_portals(peers, values):
    for peer in peers:
        peer.query('CREATE TABLE portal_arrays(id int PRIMARY KEY,b bytea[],j json[],k jsonb[])')
        for column in ('b', 'j', 'k'):
            peer.query(f'ALTER TABLE portal_arrays ALTER {column} SET STORAGE EXTERNAL')
        peer.query('BEGIN')
    sql = ('INSERT INTO portal_arrays VALUES '
           '(1,ARRAY[$1,NULL],ARRAY[$2,NULL],ARRAY[$3,NULL]),'
           '(2,NULL,NULL,NULL) RETURNING b,j,k')
    params = (values[2], values[3], values[4])
    result = compare(peers, [parse(sql, (17, 114, 3802), name=b'array_insert'),
                             bind((0,), params, portal=b'array_insert', statement=b'array_insert'),
                             execute(1, b'array_insert')], 'array insert first')
    expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
    row = peers[0].data_row(result[2][1])
    assert all(value is not None and 'NULL' in value and len(value) > 9000 for value in row), [len(v) if v else v for v in row]
    result = compare(peers, [execute(1, b'array_insert')], 'array insert null row')
    expect_kinds(result, [b'D', b's', b'Z'])
    assert peers[0].data_row(result[0][1]) == [None] * 3, result
    expect_kinds(compare(peers, [execute(1, b'array_insert')], 'array insert end'), [b'C', b'Z'])

    for name, sql in (
        (b'array_select', 'SELECT b,j,k FROM portal_arrays ORDER BY id'),
        (b'array_update', 'UPDATE portal_arrays SET b=b,j=j,k=k RETURNING b,j,k'),
        (b'array_delete', 'DELETE FROM portal_arrays RETURNING b,j,k'),
    ):
        result = compare(peers, [parse(sql, name=name), bind((0,), portal=name, statement=name),
                                 execute(1, name)], (name, 'first'))
        expect_kinds(result, [b'1', b'2', b'D', b's', b'Z'])
        assert peers[0].data_row(result[2][1]) == row, name
        result = compare(peers, [execute(1, name)], (name, 'null'))
        expect_kinds(result, [b'D', b's', b'Z'])
        assert peers[0].data_row(result[0][1]) == [None] * 3, name
        expect_kinds(compare(peers, [execute(1, name)], (name, 'end')), [b'C', b'Z'])
    for peer in peers:
        peer.query('ROLLBACK')
    print('returning-portals: TOASTable array continuation PASS', flush=True)


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
        resumed_portals(peers, values)
        toasted_array_portals(peers, values)
        select_plan_cases(peers)
        select_portal_edges(peers)
    finally:
        for peer in peers:
            peer.close()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--select-only':
        select_peers = [RawConnection(int(port), 'returning-portals-select') for port in sys.argv[2:]]
        try:
            select_plan_cases(select_peers)
            select_portal_edges(select_peers)
        finally:
            for peer in select_peers:
                peer.close()
    else:
        main([int(port) for port in sys.argv[1:]])
