"""Fixed-bit expression acceptance shared by owned native and pinned C peers."""
import random
import sys

import psycopg2


def main():
    connection = psycopg2.connect(host="127.0.0.1", port=int(sys.argv[1]),
                                  user="postgres", dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for expression, expected, modifier in [
                ("'101'::bit", "1", 1), ('\'101\'::"bit"', "101", -1),
                ("'101'::pg_catalog.bit", "101", -1),
                ("'b101'::bit(5)", "10100", 5), ("'xF'::bit(3)", "111", 3),
                ("''::bit", "0", 1), ('\'\'::"bit"', "", -1),
                ("B'1'::bit(3)", "100", 3), ("X'ff'", "11111111", -1),
                ("1::bit(3)", "001", 3), ("(-1)::bit(40)", "1" * 40, 40),
                ('(\'101\'::bit(3))::"bit"', "101", -1),
            ]:
                cursor.execute("SELECT " + expression)
                column = cursor.description[0]
                assert column.type_code == 1560
                assert column.internal_size == (modifier - 4 if modifier >= 0 else -1)
                assert cursor.fetchone() == (expected,), expression
            for expression, expected, oid in [
                ("B'1'::int", 1, 23), ("'x80000000'::bit(32)::int", -(1 << 31), 23),
                ("'xFFFFFFFF'::bit(32)::int", -1, 23),
                ("'xFFFFFFFF'::bit(32)::bigint", (1 << 32) - 1, 20),
                ("'x8000000000000000'::bit(64)::bigint", -(1 << 63), 20),
                ('\'\'::"bit"::int', 0, 23),
            ]:
                cursor.execute("SELECT " + expression)
                assert cursor.description[0].type_code == oid
                assert cursor.fetchone() == (expected,), expression
            for expression, expected in [("9223372036854775808", 1 << 63),
                                         ("-9223372036854775809", -(1 << 63) - 1)]:
                cursor.execute("SELECT " + expression)
                assert cursor.description[0].type_code == 1700
                assert cursor.fetchone()[0] == expected
            generator = random.Random(42)
            for _ in range(100):
                unsigned = generator.getrandbits(32)
                signed = unsigned if unsigned < 1 << 31 else unsigned - (1 << 32)
                # Synthetic filenames test decoding; they never change a server timeline.
                filename = f"{unsigned:08X}0000000000000001"
                cursor.execute("SELECT ('x'||pg_catalog.substr(%s,1,8))::bit(32)::int", (filename,))
                assert cursor.fetchone() == (signed,)
                width = generator.randrange(1, 32)
                cursor.execute("SELECT (%s::int)::bit(%s)::int", (signed, width))
                assert cursor.fetchone() == (unsigned & ((1 << width) - 1),)
                cursor.execute("SELECT (%s::int)::bit(64)::bigint", (signed,))
                assert cursor.fetchone() == (signed,)
            for expression, state in [
                ("'120'::bit(1)", "22P02"), ("'xG'::bit(1)", "22P02"),
                ("NULL::bit(0)", "22023"), ("NULL::bit(-1)", "22023"),
                ("NULL::bit(83886081)", "22023"), ("NULL::bit(1,2)", "22023"),
                ("NULL::bit(foo)", "22P02"), ("NULL::bit(1,'bad')", "22P02"),
                ("NULL::bit(1.5)", "22P02"), ("NULL::bit(-1.5)", "22P02"),
                ("NULL::bit(.5)", "22P02"), ("NULL::bit(1.)", "22P02"),
                ("NULL::bit(1e1)", "22P02"), ("NULL::bit(1_0.5)", "22P02"),
                ("NULL::bit(+8)", "42601"), ("NULL::bit(+1.5)", "42601"),
                ("NULL::bit(1.5,1+1)", "42601"),
                ("'1'::bit(33)::int", "22003"), ("'0'::bit(65)::bigint", "22003"),
                ("1::smallint::bit(8)", "42846"), ("B'1'::smallint", "42846"),
                ("9223372036854775808::bit(64)", "42846"),
                ("B'1'+B'0'", "42883"),
            ]:
                try:
                    cursor.execute("SELECT " + expression)
                except psycopg2.Error as error:
                    assert error.pgcode == state, (expression, error.pgcode)
                else:
                    raise AssertionError((expression, "unexpected success"))
            cursor.execute("SELECT NULL::bit(83886080),NULL::bit('8')")
            assert cursor.fetchone() == (None, None)
            cursor.execute("SELECT NULL::bit(0x20),NULL::bit(1_0),0o7_7,0b1_1")
            assert cursor.fetchone() == (None, None, 63, 3)
            cursor.execute("SELECT q.v FROM (SELECT B'10' AS v UNION ALL SELECT B'1' UNION ALL SELECT B'11') q ORDER BY v")
            assert cursor.fetchall() == [("1",), ("10",), ("11",)]
        print("bit-sql: input/resize/types/errors, 100 timeline patterns and signed roundtrips PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
