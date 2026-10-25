"""Read-only timeline interface acceptance using an explicitly synthetic file.

Run only against the task-owned throwaway cluster named on the command line.
The file tests an existing-history read; it never changes the current timeline.
"""
from pathlib import Path
from types import SimpleNamespace
import sys

import psycopg2
from patroni.postgresql import Postgresql
from patroni.postgresql.connection import get_connection_cursor


def main():
    data, port = Path(sys.argv[1]), int(sys.argv[2])
    history = data / "pg_wal" / "00000004.history"
    content = b"1\t0/40\tfirst fork\n3\t0/80\tsecond fork\n"
    with history.open("xb") as stream:
        stream.write(content)
    try:
        parsed = Postgresql.get_history(SimpleNamespace(wal_dir=str(data / "pg_wal"), name="history-test"), 4)
        assert parsed[0][:3] == (1, 64, "first fork")
        assert parsed[1][:3] == (3, 128, "second fork")
        assert parsed[1][-1] == "history-test"
        for mode in ("true", "database"):
            database = "unbound_database" if mode == "true" else "postgres"
            with get_connection_cursor(host="127.0.0.1", port=port, user="postgres",
                                       dbname=database, replication=mode) as cursor:
                cursor.execute("IDENTIFY_SYSTEM")
                assert [(column.name, column.type_code) for column in cursor.description] == [
                    ("systemid", 25), ("timeline", 20), ("xlogpos", 25), ("dbname", 25)]
                identity = cursor.fetchone()
                assert identity[1] == 1  # Reading a fixture never promotes this node.
                assert identity[3] == (None if mode == "true" else "postgres")
                cursor.execute("SHOW statement_timeout")
                assert cursor.fetchone() == ("2s",)
                if mode == "database":
                    cursor.execute("/* SQL fallback */ SHOW statement_timeout")
                    assert cursor.fetchone() == ("2s",)
                cursor.execute("TIMELINE_HISTORY 4")
                assert [(column.name, column.type_code) for column in cursor.description] == [
                    ("filename", 25), ("content", 25)]
                assert cursor.fetchone() == (history.name, content.decode())
                assert cursor.statusmessage == "TIMELINE_HISTORY"
                for query, state in (("TIMELINE_HISTORY 1", "58P01"),
                                     ("TIMELINE_HISTORY 0", "42601")):
                    try:
                        cursor.execute(query)
                    except psycopg2.Error as error:
                        assert error.pgcode == state, (query, error.pgcode)
                    else:
                        raise AssertionError("invalid/missing history unexpectedly succeeded")
                if mode == "true":
                    for query, state in (("SELECT 1", "0A000"),
                                         ("/* SQL fallback */ SHOW statement_timeout", "0A000"),
                                         ("CREATE_REPLICATION_SLOT forbidden LOGICAL pgoutput", "55000")):
                        try:
                            cursor.execute(query)
                        except psycopg2.Error as error:
                            assert error.pgcode == state, (query, error.pgcode)
                        else:
                            raise AssertionError("physical connection admitted database work")
                cursor.execute("IDENTIFY_SYSTEM")
                after = cursor.fetchone()
                assert after[:2] == identity[:2] and after[3] == identity[3]
        print("patroni-history: real Patroni reader/replication cursor and exact history bytes PASS", flush=True)
    finally:
        history.unlink()


if __name__ == "__main__":
    main()
