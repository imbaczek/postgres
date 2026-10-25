"""Read-only WAL SQL acceptance, shared unchanged by native and pinned C peers."""
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
        print(f"wal-sql: {len(cases)} exact numeric differences, NULL/type/overflow checks PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
