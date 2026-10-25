"""Run pinned Patroni's unchanged five-column TL_LSN fragment; no HA loop."""
import importlib.metadata
import sys

import psycopg2
from patroni.postgresql import Postgresql


def main():
    assert importlib.metadata.version("patroni") == "4.1.5"
    port, role = int(sys.argv[1]), sys.argv[2]
    assert role in ("primary", "standby")
    paused = len(sys.argv) > 3 and sys.argv[3] == "paused"
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for flush in ("", "_flush"):
                # Use the installed Patroni constant verbatim, including its
                # recovery guard around the primary-invalid replay-pause call.
                cursor.execute("SELECT " + Postgresql.TL_LSN.format("wal", "lsn", flush))
                actual = cursor.fetchone()
                assert [column.type_code for column in cursor.description] == [23, 20, 20, 20, 16]
                if role == "primary":
                    cursor.execute(f"SELECT pg_current_wal{flush}_lsn(), "
                                   "pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn()")
                    current, replay, receive = cursor.fetchone()
                    expected = (1, lsn(current), lsn(replay), lsn(receive) or 0, False)
                else:
                    cursor.execute("SELECT pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn()")
                    replay, receive = cursor.fetchone()
                    expected = (0, 0, lsn(replay), lsn(receive) or 0, paused)
                assert actual == expected, (flush, actual, expected)
        print(f"patroni-tl-lsn: {role} paused={paused} write/flush fragments PASS")
    finally:
        connection.close()


def lsn(value):
    if value is None:
        return None
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


if __name__ == "__main__":
    main()
