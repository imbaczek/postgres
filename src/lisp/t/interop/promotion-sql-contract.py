"""SQL promotion assertions for task-owned source and standby peers.

The harness must create promotion_sql_items with row (1, 'before').
This test writes source rows and promotes the standby with two old RR views open.
It currently passes against C; native SQL promotion remains unimplemented.
"""

import sys
import time
from contextlib import closing

import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS


def connect(port):
    return psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres",
                            application_name="sql-promotion-contract", connect_timeout=3)


def scalar(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()[0]


def rejects(connection, sql, code):
    try:
        scalar(connection, sql)
    except psycopg2.Error as error:
        assert error.pgcode == code, (sql, error.pgcode, error)
    else:
        raise AssertionError(f"{sql}: expected SQLSTATE {code}")


def retained_view(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT id, label FROM promotion_sql_items ORDER BY id")
        return cursor.fetchall()


def check(primary, standby, reader):
    primary.autocommit = standby.autocommit = True
    for connection in (primary, standby):
        assert scalar(connection, "SELECT pg_promote(NULL::boolean,1)") is None
        assert scalar(connection, "SELECT pg_promote(false,NULL::integer)") is None
        rejects(connection, "SELECT pg_promote(1)", "42883")
    for sql in ("SELECT pg_promote()", "SELECT pg_promote(false,0)",
                "SELECT pg_promote(true,-1)", "SELECT pg_promote(wait_seconds => 1)"):
        rejects(primary, sql, "55000")
    for seconds in (0, -1):
        rejects(standby, f"SELECT pg_promote(false,{seconds})", "22003")
    assert scalar(standby, "SELECT pg_is_in_recovery()") is True
    print("promotion-sql-contract: defaults, strict NULLs and error order PASS", flush=True)

    standby.autocommit = False
    for connection in (standby, reader):
        connection.set_session(isolation_level="REPEATABLE READ")
        assert retained_view(connection) == [(1, "before")]
    with primary.cursor() as cursor:
        cursor.execute("UPDATE promotion_sql_items SET label='after' WHERE id=1")
        cursor.execute("INSERT INTO promotion_sql_items VALUES (2,'new')")
    position = scalar(primary, "SELECT pg_current_wal_flush_lsn()")
    with closing(connect(standby.info.port)) as observer:
        observer.autocommit = True
        deadline = time.monotonic() + 10
        while not scalar(observer, f"SELECT pg_last_wal_replay_lsn() >= '{position}'::pg_lsn"):
            assert time.monotonic() < deadline, "standby did not replay the source changes"
            time.sleep(0.05)
    for connection in (standby, reader):
        assert retained_view(connection) == [(1, "before")]

    with standby.cursor() as cursor:
        cursor.execute("""
            SELECT set_config('application_name',current_setting('application_name') || ':before',false),
                   pg_promote(wait_seconds => 5),
                   set_config('application_name',current_setting('application_name') || ':after',false)
        """)
        assert cursor.fetchone() == ("sql-promotion-contract:before", True,
                                    "sql-promotion-contract:before:after")
    for connection in (standby, reader):
        assert connection.get_transaction_status() == TRANSACTION_STATUS_INTRANS
        assert scalar(connection, "SELECT pg_is_in_recovery()") is False
        assert retained_view(connection) == [(1, "before")]
        assert scalar(connection, "SHOW transaction_read_only") == "on"
        connection.commit()
        assert retained_view(connection) == [(1, "after"), (2, "new")]
        assert scalar(connection, "SHOW transaction_read_only") == "off"
        connection.commit()
    print("promotion-sql-contract: inline wait, volatile order and retained RR views PASS", flush=True)
    assert scalar(standby, "SELECT current_setting('application_name')") == \
        "sql-promotion-contract:before:after"
    standby.commit()


if __name__ == "__main__":
    connections = [connect(int(port)) for port in (sys.argv[1], sys.argv[2], sys.argv[2])]
    try:
        check(*connections)
    finally:
        for connection in connections:
            connection.close()
