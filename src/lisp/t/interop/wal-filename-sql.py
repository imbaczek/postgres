"""WAL filename SQL contracts shared by native and pinned C primary peers."""
import random
import sys

import psycopg2


def main():
    connection = psycopg2.connect(host="127.0.0.1", port=int(sys.argv[1]),
                                  user="postgres", dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            generator = random.Random(42)
            locations = [0, (1 << 24) - 1, 1 << 24, (1 << 32) - 1,
                         1 << 32, (1 << 64) - 1]
            locations += [generator.getrandbits(64) for _ in range(100)]
            for location in locations:
                lsn = f"{location >> 32:X}/{location & 0xFFFFFFFF:X}"
                segment = location // (1 << 24)
                expected = f"00000001{segment // 256:08X}{segment % 256:08X}"
                cursor.execute("SELECT pg_catalog.pg_walfile_name(%s)", (lsn,))
                assert cursor.description[0].type_code == 25
                assert cursor.fetchone() == (expected,), lsn
            cursor.execute("SELECT pg_walfile_name(NULL)")
            assert cursor.fetchone() == (None,)
            for expression, state in [("1", "42883"), ("'0/0'::text", "42883"),
                                      ("'bad'", "22P02")]:
                try:
                    cursor.execute(f"SELECT pg_walfile_name({expression})")
                except psycopg2.Error as error:
                    assert error.pgcode == state, (expression, error.pgcode)
                else:
                    raise AssertionError(expression)
        print("wal-filename-sql: PASS (106 locations, metadata, NULL, input errors)")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
