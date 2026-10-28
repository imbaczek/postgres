"""Compare width_bucket threshold arrays and their wire types with PostgreSQL."""
import sys

from subqueries import QueryConnection
from parking_wire import WireError


NAME_PREFIX = "a" * 63


CASES = (
    "SELECT width_bucket(true, ARRAY[false,true])",
    "SELECT width_bucket('b'::varchar, '{a,b,c}'::varchar[])",
    "SELECT width_bucket('b'::name, ARRAY['a'::name,'b'::name])",
    *(f"SELECT width_bucket('{NAME_PREFIX}'::{operand_type}, "
      f"ARRAY['{NAME_PREFIX}b'::{threshold_type}])"
      for operand_type, threshold_type in (("name", "text"), ("name", "varchar"),
                                           ("text", "name"), ("varchar", "name"),
                                           ("text", "text"), ("varchar", "text"))),
    *(f"SELECT width_bucket('{NAME_PREFIX}'::{operand_type}, '{{{NAME_PREFIX}b}}')"
      for operand_type in ("name", "text")),
    "SELECT width_bucket('b'::\"char\", ARRAY['a'::\"char\",'b'::\"char\"])",
    "SELECT width_bucket('2020-01-02'::date, ARRAY['2020-01-01'::date,'2020-01-02'::date])",
    "SELECT width_bucket('12:00'::time, ARRAY['09:00'::time,'12:00'::time])",
    "SELECT width_bucket('12:00+00'::timetz, ARRAY['12:00+00'::timetz])",
    "SELECT width_bucket('12:00'::time, ARRAY['12:00+00'::timetz])",
    "SELECT width_bucket('12:00+00'::timetz, ARRAY['12:00'::time])",
    "SELECT width_bucket('12:00'::time, ARRAY['12:00+00'::timetz]), width_bucket('12:00+00'::timetz, ARRAY['12:00'::time])",
    "SELECT width_bucket('12:00+00'::timetz, ARRAY['09:00+00'::timetz,'12:00+00'::timetz])",
    "SELECT width_bucket('23:00+00'::timetz, ARRAY['01:00+01'::timetz,'05:00-02'::timetz,'12:00+00'::timetz,'18:00+03'::timetz])",
    "SELECT width_bucket('05:00-02'::timetz, '{01:00+01,05:00-02,12:00+00,18:00+03}'::timetz[])",
    "SELECT width_bucket('2020-01-02'::timestamp, ARRAY['2020-01-01'::timestamp,'2020-01-02'::timestamp])",
    "SELECT width_bucket('2020-01-02 00:00+00'::timestamptz, ARRAY['2020-01-01 00:00+00'::timestamptz,'2020-01-02 00:00+00'::timestamptz])",
    "SELECT width_bucket('2 days'::interval, ARRAY['1 day'::interval,'2 days'::interval])",
    "SELECT width_bucket(2::oid, ARRAY[1,2,3])",
    "SELECT width_bucket('0/10'::pg_lsn, ARRAY['0/01'::pg_lsn,'0/10'::pg_lsn])",
    "SELECT width_bucket(B'10', ARRAY[B'01',B'10'])",
    "SELECT width_bucket(2, '1 2 3'::int2vector)",
    "SELECT '1 2'::int2vector::int2[], '1 2'::int2vector::int4[]",
    "SELECT ARRAY[1,2]::int2vector::text, ARRAY[1,2]::int2vector::int2[]",
    "SELECT ARRAY[1,2]::int2vector = '1 2'::int2vector, ARRAY[1,2]::int2vector > '1 2'::int2vector",
    "SELECT ARRAY[]::int2vector = ''::int2vector, ''::int2vector::int2[]",
    "SELECT array_length(''::int2vector::int2[],1), array_length(''::int2vector::int4[],1), array_length(''::int2vector::int2[]::int4[],1)",
    "SELECT ARRAY[]::int2vector::text",
    "SELECT ARRAY[1,NULL]::int2vector::text",
    "SELECT ARRAY[[1,2],[3,4]]::int2vector::text",
    "SELECT DISTINCT x FROM (SELECT ARRAY[1,2]::int2vector AS x UNION ALL SELECT '1 2'::int2vector) q ORDER BY x",
    "SELECT ARRAY[1,2]::int2vector IN ('1 2'::int2vector, ARRAY[1,2]::int2vector)",
    "SELECT x FROM (SELECT '1 2'::int2vector AS x UNION ALL SELECT ARRAY[1,2]::int2[]) q ORDER BY x",
    "SELECT '{1,2}'::int2[]::int2vector",
    "SELECT width_bucket(NULL::name, ARRAY['a'::name])",
    "SELECT width_bucket(true, NULL::bool[])",
    "SELECT width_bucket(true, ARRAY[false,NULL])",
    "SELECT width_bucket(true, ARRAY[[true,false],[true,false]])",
    "SELECT width_bucket('x'::date, ARRAY['2020-01-01'::date])",
    "SELECT width_bucket(true, ARRAY[1,2])",
    "SELECT width_bucket(1::oid, ARRAY[1.0::numeric])",
    "SELECT width_bucket('12:00'::time, ARRAY['2020-01-01'::date])",
    "SELECT width_bucket('not a date', ARRAY['2020-01-01'::date])",
    "SELECT ARRAY[false,true], ARRAY['a'::name], ARRAY['a'::varchar], ARRAY['a'::\"char\"]",
    "SELECT ARRAY['2020-01-01'::date], ARRAY['12:00'::time], ARRAY['12:00+00'::timetz]",
    "SELECT ARRAY['2020-01-01'::timestamp], ARRAY['2020-01-01 00:00+00'::timestamptz], ARRAY['1 day'::interval]",
    "SELECT ARRAY[1::oid], ARRAY['0/01'::pg_lsn], ARRAY[B'10']",
    "SELECT '{1 day,2 days}'::interval[], '{12:00+00,13:00+02}'::timetz[]",
    "SELECT x FROM (SELECT ARRAY['2 days'::interval] AS x UNION ALL SELECT ARRAY['1 day'::interval]) q ORDER BY x",
    "SELECT x FROM (SELECT ARRAY['13:00+00'::timetz] AS x UNION ALL SELECT ARRAY['12:00+00'::timetz]) q ORDER BY x",
    "SELECT x,count(*) FROM (SELECT ARRAY['1 day'::interval] AS x UNION ALL SELECT ARRAY['24 hours'::interval]) q GROUP BY x",
    "SELECT x,count(*) FROM (SELECT ARRAY['12:00+00'::timetz] AS x UNION ALL SELECT ARRAY['12:00+00'::timetz]) q GROUP BY x",
    "SET TIME ZONE 'America/New_York'",
    "SELECT width_bucket('12:00'::time, ARRAY['12:00'::timetz]), width_bucket('12:00'::timetz, ARRAY['12:00'::time])",
    "SELECT '12:00'::time::timetz, '12:00-04'::timetz::time",
)


def check(reference, native):
    peers = [QueryConnection(port, "array-types") for port in (reference, native)]
    failures = []
    try:
        for peer in peers:
            peer.observe("SET TIME ZONE 'UTC'")
        for sql in CASES:
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes[0] != outcomes[1]:
                failures.append((sql, outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        for sql in ("SELECT width_bucket(true, ARRAY['12:00'::time])",
                    "SELECT width_bucket(true, ARRAY['2020-01-01'::date])",
                    "SELECT width_bucket(true, ARRAY['a'::varchar])"):
            outcomes = []
            for peer in peers:
                try:
                    peer.query(sql)
                    outcomes.append("unexpected success")
                except WireError as error:
                    outcomes.append(str(error))
            if outcomes[0] != outcomes[1]:
                failures.append((sql, outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        assert not failures, failures
        print(f"array-types: {len(CASES)} C comparisons PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]))
