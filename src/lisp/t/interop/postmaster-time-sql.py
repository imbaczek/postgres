"""Startup timestamp contract shared by native and pinned C postmasters."""
import datetime
import importlib.metadata
from pathlib import Path
import sys
from threading import get_ident

import psycopg2
from patroni.postgresql import Postgresql


def connect(port):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    return connection


def timestamp(connection):
    with connection.cursor() as cursor:
        cursor.execute("SET timezone TO 'UTC'")
        cursor.execute("SELECT " + Postgresql.POSTMASTER_START_TIME)
        column = cursor.description[0]
        assert (column.name, column.type_code, column.internal_size) == ("pg_postmaster_start_time", 1184, 8)
        result = cursor.fetchone()[0]
        assert isinstance(result, datetime.datetime) and result.tzinfo
        assert result <= datetime.datetime.now(datetime.timezone.utc)
        return result


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    connection = connect(int(sys.argv[1]))
    try:
        value = timestamp(connection)
        assert timestamp(connection) == value
        other = connect(int(sys.argv[1]))
        try:
            assert timestamp(other) == value
        finally:
            other.close()
        # Exercise the real pinned method, without creating a controller or DCS.
        peer = Postgresql.__new__(Postgresql)
        peer._Postgresql__thread_ident = get_ident()

        def query(sql, retry=True):
            assert sql == "SELECT " + Postgresql.POSTMASTER_START_TIME and retry
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()

        peer.query = query
        assert peer.postmaster_start_time() == value.isoformat(sep=" ")
        if len(sys.argv) > 2:
            baseline = Path(sys.argv[2])
            expected = value.isoformat()
            if baseline.exists():
                assert baseline.read_text().strip() == expected
            else:
                baseline.write_text(expected + "\n")
        for argument in ("1", "NULL"):
            with connection.cursor() as cursor:
                try:
                    cursor.execute(f"SELECT pg_postmaster_start_time({argument})")
                except psycopg2.Error as error:
                    assert error.pgcode == "42883"
                else:
                    raise AssertionError(argument)
        print("postmaster-time-sql: type, stable connections/reload, and real Patroni method PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
