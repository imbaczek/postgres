"""Text expression probes shared unchanged by owned native and UTF8 C peers."""
import random
import sys

import psycopg2


def main():
    connection = psycopg2.connect(host="127.0.0.1", port=int(sys.argv[1]),
                                  user="postgres", dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for sql, expected in [
                ("'x'||1+2", "x3"), ("1+2||'x'", "3x"),
                ("'a'||'b'||'c'", "abc"), ("'x'||true", "xtrue"),
                ("'x'||'0/1'::pg_lsn", "x0/00000001"),
                ("'λ'||'🙂'", "λ🙂"), ("NULL||'x'", None), ("'x'||NULL", None),
                ("'x'||'λ'::name", "xλ"),
                ("'x'||(-5 % 2)", "x-1"),
                ("'x'||pg_catalog.substr('000000010000000000000001',1,8)", "x00000001"),
            ]:
                cursor.execute("SELECT " + sql)
                assert cursor.description[0].type_code == 25
                assert cursor.fetchone() == (expected,), sql
            cursor.execute("SELECT 'x'||1+2 = 'x3'")
            assert cursor.fetchone() == (True,)
            cursor.execute("SELECT substr('abc',2::smallint,1::smallint)")
            assert cursor.fetchone() == ("b",)
            generator = random.Random(42)
            samples = ["", "abc", "aλ🙂é", "e\u0301🙂"]
            cases = [("abc", 0, 2), ("abc", -2, 4), ("abc", -2, 2),
                     ("abc", 2, 2147483647), ("abc", 2147483647, 2147483647),
                     ("abc", -2147483648, 2147483647)]
            cases += [(generator.choice(samples) * generator.randrange(8),
                       generator.randrange(-20, 40), generator.choice([None, generator.randrange(40)]))
                      for _ in range(100)]
            for text, start, length in cases:
                if length is None:
                    cursor.execute("SELECT pg_catalog.substr(%s,%s)", (text, start))
                    expected = text[max(0, start - 1):]
                else:
                    cursor.execute("SELECT pg_catalog.substr(%s,%s,%s)", (text, start, length))
                    expected = text[max(0, start - 1):max(0, start + length - 1)]
                assert (cursor.description[0].name, cursor.description[0].type_code) == ("substr", 25)
                assert cursor.fetchone() == (expected,), (text, start, length)
            for sql in ["substr(NULL,1,-1)", "substr('a',NULL)", "substr('a',1,NULL)"]:
                cursor.execute("SELECT " + sql)
                assert cursor.fetchone() == (None,)
            for sql, state in [
                ("1||2", "42883"), ("substr('a',1,-1)", "22011"),
                ("substr(1,1)", "42883"), ("substr('a',1::bigint)", "42883"),
                ("substr('a','bad')", "22P02"),
                ("substr('abc',2147483647+1)", "22003"),
                ("'x'||(2147483647+1)", "22003"),
                ("9223372036854775807::bigint+1", "22003"),
                ("-('-2147483648'::int)", "22003"),
                ("-2147483648 / -1", "22003"),
            ]:
                try:
                    cursor.execute("SELECT " + sql)
                except psycopg2.Error as error:
                    assert error.pgcode == state, (sql, error.pgcode)
                else:
                    raise AssertionError((sql, "unexpected success"))
        print(f"string-sql: concat precedence/coercion/NULL and {len(cases)} UTF8 substr cases PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
