"""Receiver record and unchanged pinned Patroni ordinary cluster-status query."""
import datetime
import importlib.metadata
import sys
from threading import Lock

import psycopg2
from patroni import global_config
from patroni.postgresql import Postgresql
from patroni.postgresql.misc import PostgresqlRole
from patroni.postgresql.slots import SlotsHandler


def cluster_query(recovery):
    # Run the installed query builders without constructing an HA controller,
    # starting slot workers, or changing their query text.
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert not global_config.is_synchronous_mode
    peer = Postgresql.__new__(Postgresql)
    peer._major_version = 200000
    peer._role_lock = Lock()
    peer._role = PostgresqlRole.REPLICA if recovery else PostgresqlRole.PRIMARY
    peer._should_query_slots = False
    peer.slots_handler = SlotsHandler.__new__(SlotsHandler)
    peer.slots_handler._postgresql = peer
    return peer.cluster_info_query


def lsn(value):
    if value is None:
        return None
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def main():
    port, state, backend = int(sys.argv[1]), sys.argv[2], sys.argv[3]
    assert state in ("absent", "connecting", "streaming", "paused")
    assert backend in ("native", "c")
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=2)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM pg_catalog.pg_stat_get_wal_receiver()")
            assert [column.name for column in cursor.description] == [
                "pid", "status", "receive_start_lsn", "receive_start_tli", "written_lsn",
                "flushed_lsn", "received_tli", "last_msg_send_time", "last_msg_receipt_time",
                "latest_end_lsn", "latest_end_time", "slot_name", "sender_host", "sender_port", "conninfo"]
            assert [column.type_code for column in cursor.description] == [
                23, 25, 3220, 23, 3220, 3220, 23, 1184, 1184, 3220, 1184, 25, 25, 23, 25]
            rows = cursor.fetchall()
            assert len(rows) == 1
            row = rows[0]
            if state == "absent":
                assert row == (None,) * 15, row
                cursor.execute("SELECT pg_stat_get_wal_receiver()")
                assert cursor.fetchone() == (None,)
            else:
                assert row[0] > 0 and row[1] == ("connecting" if state == "connecting" else "streaming"), row
                assert row[3] == row[6] == 1
                assert lsn(row[5]) <= lsn(row[4])
                for index in (7, 8, 10):
                    assert isinstance(row[index], datetime.datetime) and row[index].tzinfo, (index, row)
                    assert abs((datetime.datetime.now(datetime.timezone.utc) - row[index]).total_seconds()) < 120
                assert row[11] is None  # neither peer uses a C physical slot
                if state == "connecting":
                    assert row[9] is None and row[12:] == (None, None, None), row
                elif backend == "native":
                    assert row[12] == "127.0.0.1" and row[13] == int(sys.argv[4])
                    parameters = psycopg2.extensions.parse_dsn(row[14])
                    assert parameters == dict(host="127.0.0.1", port=sys.argv[4], user="postgres",
                                              dbname="postgres", application_name="pg_lisp_physical_receiver",
                                              replication="database", sslmode="disable")
                else:
                    assert row[12] == "127.0.0.1" and row[13] == int(sys.argv[4]) and row[14]
            cursor.execute("SELECT pg_is_in_recovery()")
            recovery = cursor.fetchone()[0]
            cursor.execute("SELECT " + Postgresql.TL_LSN.format("wal", "lsn", "_flush"))
            positions = cursor.fetchone()
            assert positions[4] == (state == "paused")
            cursor.execute(cluster_query(recovery))
            assert len(cursor.description) == 16
            actual = cursor.fetchall()
            expected = positions + (0, row[6] if row[9] is not None else None,
                                    lsn(row[4]), row[11], row[14], row[1], "", None, "on", "", None)
            assert actual == [expected], (actual, expected)
        print(f"receiver-sql: {backend} {state}, 15-field record and 16-field Patroni query PASS")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
