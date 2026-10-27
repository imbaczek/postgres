"""SQL promotion assertions for task-owned source and standby peers.

The harness must create promotion_sql_items with row (1, 'before').
This test writes source rows and promotes the standby with two old RR views open.
The same assertions run against pinned C and native physical peers.
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


def signature_contract(connection):
    for expression in ("pg_promote(NULL)", "pg_promote(wait_seconds=>NULL)",
                       "pg_promote(wait_seconds:=NULL)", "pg_promote(VARIADIC NULL::boolean)",
                       "pg_promote(NULL::boolean,1)", "pg_promote(false,NULL::integer)"):
        assert scalar(connection, "SELECT " + expression) is None
    for expression, state in (
        ("pg_promote(VARIADIC wait_seconds=>1)", "42883"),
        ("pg_promote('bad',1::bigint)", "42883"),
        ("pg_promote('bad',wrong=>1)", "42883"),
        ("pg_promote(1)", "42883"),
        ("pg_promote(true,1::bigint)", "42883"),
        ("pg_promote('true'::text,1)", "42883"),
        ("pg_promote(wrong=>NULL)", "42883"),
        ('pg_promote("WAIT"=>NULL)', "42883"),
        ("pg_promote(true,wait=>false)", "42883"),
        ("pg_promote(wait=>true,wait=>false)", "42601"),
        ("pg_promote(wait=>true,true)", "42601"),
        ("pg_promote(NULL,'bad')", "22P02"),
        ("pg_promote(true,'2147483648')", "22003"),
    ):
        rejects(connection, "SELECT " + expression, state)
    scalar(connection, "SELECT set_config('application_name','trace',false)")
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT pg_promote(
                wait_seconds=>CASE WHEN set_config('application_name',current_setting('application_name')||':seconds',false)
                    IS NOT NULL THEN NULL::integer END,
                wait=>CASE WHEN set_config('application_name',current_setting('application_name')||':wait',false)
                    IS NOT NULL THEN true END), current_setting('application_name')
        """)
        assert cursor.fetchone() == (None, "trace:wait:seconds")
    scalar(connection, "SELECT set_config('application_name','sql-promotion-contract',false)")


def check(primary, standby, reader):
    primary.autocommit = standby.autocommit = True
    for connection in (primary, standby):
        signature_contract(connection)
    for sql in ("SELECT pg_promote()", "SELECT pg_promote(false,0)",
                "SELECT pg_promote(true,-1)", "SELECT pg_promote(wait_seconds => 1)",
                "SELECT pg_promote(true,1::smallint)", "SELECT pg_promote(VARIADIC true)",
                "SELECT pg_promote('on',0)"):
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
