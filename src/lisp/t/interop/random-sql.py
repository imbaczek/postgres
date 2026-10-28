"""Differential tests for seeded random calls and session state."""
import sys
from subqueries import QueryConnection
from parking_wire import WireError


def run(port):
    a, b = QueryConnection(port), QueryConnection(port)
    try:
        values = []
        expressions = ['random()', 'random(-100,100)',
                       "random('-9223372036854775808'::bigint,9223372036854775807::bigint)",
                       'random(-1.23::numeric,98765432101234567890.12::numeric)',
                       'random(0.00001::numeric,0.99999::numeric)',
                       'random(0::numeric,9999999999999999999999999999999999999999::numeric)',
                       'random(0.00000001::numeric,99.99999999::numeric)',
                       'random(-1.2::numeric,1.00003::numeric)',
                       'random(1.00000::numeric,1.0::numeric)',
                       'random(4,4)', 'random(1.00::numeric,1.00::numeric)', 'random()',
                       'random(max=>10,min=>1)']
        for seed in ['0.5', '-0.25', '1', '-1', '0', '-0.0']:
            values.append(a.observe('select setseed(' + seed + ')'))
            b.query('select setseed(' + seed + ')')
            for expr in expressions:
                values.append(a.observe('select ' + expr))
            assert b.observe('select random()') == values[-len(expressions)], 'session isolation'
        a.query('begin')
        a.query('select setseed(-0.25)')
        a.query('rollback')
        after = a.observe('select random()')
        b.query('select setseed(-0.25)')
        assert b.observe('select random()') == after, 'rollback state'
        for expr in ['setseed(2)', "setseed('NaN'::float8)", 'random(2,1)',
                     'random(2.0,1.0)', 'setseed(NULL)', 'random(NULL::int,1)',
                     'random(NULL,NULL)', "random('1','2')",
                     'random(1::smallint,2::smallint)', 'random(NULL,1::smallint)',
                     'random(1::smallint,2::int)']:
            values.append(a.observe('select ' + expr))
        diagnostics = []
        diagnostic_expressions = [
            'setseed(' + expr + ')' for expr in
            ['2', '1.23456789', '12345678', "'NaN'::float8",
             "'Infinity'::float8", "'-Infinity'::float8"]]
        diagnostic_expressions += [
            "random('NaN'::numeric,1::numeric)",
            "random(1::numeric,'NaN'::numeric)",
            "random('-Infinity'::numeric,1::numeric)",
            "random(1::numeric,'Infinity'::numeric)",
            "random('Infinity'::numeric,'-Infinity'::numeric)",
            "random('NaN'::numeric,'Infinity'::numeric)",
            'setseed(seed=>0.5)', 'setseed(foo=>NULL)',
            'setseed(foo=>0.5,foo=>0.5)', 'setseed(foo=>0.5,0.5)',
            'random(min=>1)', 'random(foo=>1,max=>2)',
            'random(1,min=>2)', 'random(min=>1,min=>2)',
            'random(min=>1,min=>missing)', 'random(min=>1,missing)',
            'random(max=>2,1)', 'random(min=>1,max=>2,extra=>3)',
            'random(NULL,NULL)', "random('1','2')",
            'random(NULL,1::smallint)',
            'random(max=>1::smallint,min=>NULL)',
            'random(max=>2::float8,min=>1)',
            'random(NULL)', 'setseed(NULL,NULL)',
            'random(NULL::text,NULL::text)']
        for expr in diagnostic_expressions:
            try:
                a.query('select ' + expr)
                raise AssertionError(expr)
            except WireError as error:
                diagnostics.append((expr, str(error)))
        return values, after, diagnostics
    finally:
        a.close()
        b.close()

reference, native = run(sys.argv[1]), run(sys.argv[2])
assert reference == native, (reference, native)
print('random-sql: seeded sequences, isolation, rollback, void and errors PASS')
