"""WAL SQL acceptance on owned throwaway native and pinned C clusters."""
from decimal import Decimal
import random
import sys

import psycopg2


def lsn(value):
    return f"{value >> 32:X}/{value & 0xffffffff:X}"


def main():
    connection = psycopg2.connect(host="127.0.0.1", port=int(sys.argv[1]),
                                  user="postgres", dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            generator = random.Random(42)
            bounds = [0, 1, (1 << 31) - 1, 1 << 31, (1 << 63) - 1,
                      1 << 63, (1 << 64) - 1]
            cases = [(left, right) for left in bounds for right in bounds]
            cases += [(generator.getrandbits(64), generator.getrandbits(64))
                      for _ in range(50)]
            for left, right in cases:
                cursor.execute("SELECT pg_catalog.pg_wal_lsn_diff(%s, %s)",
                               (lsn(left), lsn(right)))
                assert cursor.description[0].type_code == 1700
                value = cursor.fetchone()[0]
                assert isinstance(value, Decimal) and value == left - right, (left, right, value)
            for left, right in [(None, "0/0"), ("0/0", None), (None, None)]:
                cursor.execute("SELECT pg_wal_lsn_diff(%s, %s)", (left, right))
                assert cursor.description[0].type_code == 1700
                assert cursor.fetchone() == (None,)
            for left, right in [("7FFFFFFF/FFFFFFFF", "0/0"),
                                ("0/0", "80000000/0")]:
                cursor.execute("SELECT pg_wal_lsn_diff(%s, %s)::bigint", (left, right))
                assert cursor.description[0].type_code == 20
                assert cursor.fetchone()[0] == (int(left.replace("/", ""), 16)
                                               if right == "0/0" else -(1 << 63))
            for query, state in [
                ("SELECT pg_wal_lsn_diff('FFFFFFFF/FFFFFFFF', '0/0')::bigint", "22003"),
                ("SELECT pg_wal_lsn_diff('0/0', 'FFFFFFFF/FFFFFFFF')::bigint", "22003"),
                ("SELECT pg_wal_lsn_diff('0/80000000', '0/0')::int", "22003"),
                ("SELECT pg_wal_lsn_diff(1, 2)", "42883"),
                ("SELECT pg_wal_lsn_diff('0/0'::text, '0/0')", "42883"),
                ("SELECT pg_wal_lsn_diff('invalid', '0/0')", "22P02"),
            ]:
                try:
                    cursor.execute(query)
                except psycopg2.Error as error:
                    assert error.pgcode == state, (query, error.pgcode)
                else:
                    raise AssertionError((query, "unexpected success"))
            cursor.execute("SELECT pg_wal_lsn_diff('0/10', '0/0') > 0")
            assert cursor.fetchone() == (True,)
            for query, oid, expected, modifier in [
                ("SELECT COALESCE(NULL, '0/10'::pg_lsn, '0/20')", 3220, "0/00000010", None),
                ("SELECT COALESCE(NULL, NULL)", 25, None, None),
                ("SELECT COALESCE(NULL, 1, 2::bigint)", 20, 1, None),
                ("SELECT COALESCE(1)", 23, 1, None),
                ("SELECT COALESCE(NULL, pg_wal_lsn_diff('0/10','0/0'), 0)", 1700, Decimal(16), None),
                ("SELECT COALESCE(false, current_setting('missing.setting')::bool)",
                 16, False, None),
                ("SELECT COALESCE(0, current_setting('missing.setting')::int)", 23, 0, None),
                ("SELECT COALESCE('a'::varchar(4), 'b'::varchar(4))", 1043, "a", 4),
                ("SELECT COALESCE('a'::varchar(4), 'b'::varchar(5))", 1043, "a", -1),
                ("SELECT COALESCE(NULL, 1)::bigint", 20, 1, None),
            ]:
                cursor.execute(query)
                column = cursor.description[0]
                assert (column.name, column.type_code) == ("coalesce", oid), (query, column)
                actual = cursor.fetchone()
                assert actual == (expected,), (query, actual, expected)
                if oid == 1043:
                    assert column.internal_size == modifier, (query, column.internal_size, modifier)
            cursor.execute("SELECT pg_wal_lsn_diff(COALESCE(pg_last_wal_receive_lsn(), '0/0'), '0/0')::bigint")
            assert cursor.fetchone() == (0,)  # This fixture runs on an owned primary.
            for query, state in [
                ("SELECT COALESCE('0/1'::pg_lsn, 'invalid')", "22P02"),
                ("SELECT COALESCE(1, true)", "42804"),
                ("SELECT COALESCE()", "42601"),
                ("SELECT COALESCE(NULL, current_setting('missing.setting'))", "42704"),
                ('SELECT "coalesce"(NULL, 1)', "42883"),
                ("SELECT pg_catalog.coalesce(NULL, 1)", "42883"),
                ("SELECT COALESCE(pg_get_publication_tables('missing'), NULL)", "0A000"),
            ]:
                try:
                    cursor.execute(query)
                except psycopg2.Error as error:
                    assert error.pgcode == state, (query, error.pgcode)
                else:
                    raise AssertionError((query, "unexpected success"))
            cursor.execute("CREATE TABLE coalesce_wire_items (k integer, a integer, b integer)")
            try:
                cursor.execute("INSERT INTO coalesce_wire_items VALUES (1,NULL,NULL),(1,NULL,2),(2,0,9),(2,5,NULL)")
                cursor.execute("SELECT k, COALESCE(count(*),0) FROM coalesce_wire_items GROUP BY k ORDER BY k")
                assert cursor.fetchall() == [(1, 2), (2, 2)]
                cursor.execute("SELECT COALESCE(array_agg(a::text),'{}'::text[]) FROM coalesce_wire_items WHERE k=3")
                assert cursor.fetchone() == ([],)
                cursor.execute("SELECT count(*) FROM coalesce_wire_items WHERE COALESCE(a,b)>0")
                assert cursor.fetchone() == (2,)
            finally:
                cursor.execute("DROP TABLE coalesce_wire_items")
        print(f"wal-sql: {len(cases)} exact numeric differences, NULL/type/overflow checks PASS")
        print("wal-sql: COALESCE lazy values/types/metadata/errors and receive-LSN fallback PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
