"""Exercise logical-slot SQL against an owned, trust-authenticated test server."""

import os
import sys

from parking_wire import RawConnection, WireError


def main(port):
    connection = RawConnection(port, "logical-slot-sql")
    prefix = f"slot_sql_{os.getpid()}"
    table = prefix + "_rows"
    slots = [prefix + suffix for suffix in ("_patroni", "_from", "_star", "_read", "_abort")]

    def error(sql, state):
        try:
            connection.query(sql)
        except WireError as condition:
            assert condition.sqlstate == state, (sql, condition)
        else:
            raise AssertionError((sql, "expected", state))

    try:
        connection.query(f"CREATE TABLE {table} (id integer)")
        connection.query(f"INSERT INTO {table} VALUES (1)")
        for index in range(5):
            args = [f"'{prefix}_null'", "'pgoutput'", "false", "false", "false"]
            args[index] = "NULL"
            assert connection.query("SELECT pg_create_logical_replication_slot(" + ",".join(args) + ")") == [[None]]
        for option in ("temporary", "twophase", "failover"):
            error(f"SELECT pg_create_logical_replication_slot('{prefix}_bad','pgoutput',{option}=>true)", "0A000")
        error(f"SELECT pg_create_logical_replication_slot('{prefix}_bad','unsupported')", "0A000")

        patroni = (f"SELECT pg_catalog.pg_create_logical_replication_slot('{slots[0]}','pgoutput') "
                   f"WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots "
                   f"WHERE slot_type='logical' AND slot_name='{slots[0]}')")
        first = connection.query(patroni)
        assert len(first) == 1 and first[0][0].startswith(f"({slots[0]},"), first
        assert connection.query(patroni) == []
        row = connection.query(f"SELECT slot_name,lsn FROM pg_create_logical_replication_slot('{slots[1]}','pgoutput')")
        assert len(row) == 1 and row[0][0] == slots[1] and row[0][1], row
        # C ExpandRowReference copies a volatile call for each selected field.
        # Its first invocation persists the slot; the second sees the same name.
        error(f"SELECT (pg_create_logical_replication_slot('{slots[2]}','pgoutput')).*", "42710")

        connection.query("BEGIN")
        assert connection.query(f"SELECT id FROM {table}") == [["1"]]
        connection.query(f"SELECT pg_create_logical_replication_slot('{slots[3]}','pgoutput')")
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query(f"SELECT pg_create_logical_replication_slot('{slots[4]}','pgoutput')")
        connection.query("ROLLBACK")
        for name in slots:
            row = connection.query(f"SELECT plugin,slot_type,active,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{name}'")
            assert len(row) == 1 and row[0][:3] == ["pgoutput", "logical", "f"], row
            assert row[0][3] and row[0][4], row

        connection.query("BEGIN")
        connection.query("SELECT txid_current()")
        error(f"SELECT pg_create_logical_replication_slot('{prefix}_bad','pgoutput')", "25001")
        connection.query("ROLLBACK")
        connection.query("BEGIN")
        connection.query(f"SELECT id FROM {table}")
        connection.query(f"INSERT INTO {table} VALUES (2)")
        assert connection.query(f"SELECT count(*) FROM {table}") == [["2"]]
        error(f"SELECT pg_create_logical_replication_slot('{prefix}_bad','pgoutput')", "25001")
        connection.query("ROLLBACK")
        assert connection.query(f"SELECT count(*) FROM {table}") == [["1"]]
        connection.query("BEGIN")
        connection.query(f"SELECT id FROM {table}")
        connection.query(f"UPDATE {table} SET id=3 WHERE id=1")
        assert connection.query(f"SELECT id FROM {table}") == [["3"]]
        connection.query("COMMIT")
        connection.query("BEGIN")
        connection.query(f"SELECT id FROM {table}")
        connection.query(f"TRUNCATE {table}")
        connection.query("COMMIT")
        assert connection.query(f"SELECT count(*) FROM {table}") == [["0"]]
        assert connection.query(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name='{prefix}_bad'") == []
        print("logical-slot-sql: PASS (Patroni retry, record forms, read/write upgrade, transaction lifetime)")
    finally:
        connection.query("ROLLBACK")
        for name in slots:
            if connection.query(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name='{name}'"):
                connection.query(f"SELECT pg_drop_replication_slot('{name}')")
        connection.query(f"DROP TABLE IF EXISTS {table}")
        connection.close()


if __name__ == "__main__":
    main(int(sys.argv[1]))
