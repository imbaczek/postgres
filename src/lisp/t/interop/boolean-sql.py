"""Boolean execution and analysis contracts shared with pinned C PostgreSQL."""
import itertools
import sys

import psycopg2


def main():
    connection = psycopg2.connect(host="127.0.0.1", port=int(sys.argv[1]),
                                  user="postgres", dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            values = [("true", True), ("false", False), ("NULL", None)]
            for (left, a), (right, b) in itertools.product(values, repeat=2):
                conjunction = False if a is False or b is False else None if None in (a, b) else True
                disjunction = True if a is True or b is True else None if None in (a, b) else False
                cursor.execute(f"SELECT {left} AND {right}, {left} OR {right}")
                assert cursor.fetchone() == (conjunction, disjunction)
                assert [column.type_code for column in cursor.description] == [16, 16]
            cursor.execute("SELECT 'ye' AND true, 'of' OR false, NOT '0'")
            assert cursor.fetchone() == (True, False, True)
            for expression, state in [("false AND 1", "42804"), ("true OR 1", "42804"),
                                      ("NOT 1", "42804"), ("false AND 'true'::text", "42804"),
                                      ("false AND 'bad'", "22P02"),
                                      ("true AND pg_wal_replay_pause()", "42804"),
                                      ("true OR 1::oid", "42804")]:
                try:
                    cursor.execute("SELECT " + expression)
                except psycopg2.Error as error:
                    assert error.pgcode == state, (expression, error.pgcode)
                    if "pg_wal_replay_pause" in expression:
                        assert error.diag.message_primary == "argument of AND must be type boolean, not type void"
                    if "::oid" in expression:
                        assert error.diag.message_primary == "argument of OR must be type boolean, not type oid"
                else:
                    raise AssertionError(expression)
            cursor.execute("CREATE SEQUENCE boolean_evaluation_order")
            try:
                cursor.execute("SELECT false AND nextval('boolean_evaluation_order') > 0, "
                               "true OR nextval('boolean_evaluation_order') > 0")
                assert cursor.fetchone() == (False, True)
                cursor.execute("SELECT nextval('boolean_evaluation_order')")
                assert cursor.fetchone() == (1,)
                cursor.execute("SELECT NULL AND nextval('boolean_evaluation_order') > 0, "
                               "NULL OR nextval('boolean_evaluation_order') > 0")
                assert cursor.fetchone() == (None, True)
                cursor.execute("SELECT nextval('boolean_evaluation_order')")
                assert cursor.fetchone() == (4,)
            finally:
                cursor.execute("DROP SEQUENCE boolean_evaluation_order")
        print("boolean-sql: truth tables, coercion, eager analysis, volatile short circuit PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
