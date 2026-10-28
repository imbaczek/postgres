"""Public physical-slot contract shared by native and pinned C peers."""
import argparse
import time

import psycopg2


def query(connection, statement, parameters=()):
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        return cursor.fetchall() if cursor.description else None


def expect_state(connection, statement, parameters, state):
    try:
        query(connection, statement, parameters)
    except psycopg2.Error as error:
        assert error.pgcode == state, (error.pgcode, state, str(error))
    else:
        raise AssertionError(("expected SQLSTATE", state, statement))


def lsn_number(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def wait_absent(connection, name):
    deadline = time.monotonic() + 5
    while query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)):
        assert time.monotonic() < deadline, ("temporary slot remains after creator exit", name)
        time.sleep(0.01)


def exercise(connection, prefix):
    plain, reserved, temporary = [prefix + suffix for suffix in ("_plain", "_reserved", "_temporary")]
    names = (plain, reserved, temporary)
    assert query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots "
                 "WHERE slot_name IN (%s,%s,%s)", names) == []
    other = None
    try:
        assert query(connection, "SELECT * FROM pg_catalog.pg_create_physical_replication_slot(%s)",
                     (plain,)) == [(plain, None)]
        assert query(connection, "SELECT slot_type,plugin,database,datoid,temporary,active,"
                     "restart_lsn,confirmed_flush_lsn,wal_status FROM pg_catalog.pg_replication_slots "
                     "WHERE slot_name=%s", (plain,)) == [
                         ("physical", None, None, None, False, False, None, None, None)]
        expect_state(connection, "SELECT pg_catalog.pg_create_physical_replication_slot(%s)",
                     (plain,), "42710")
        expect_state(connection, "SELECT pg_catalog.pg_replication_slot_advance(%s,'0/1')",
                     (plain,), "55000")
        expect_state(connection, "SELECT pg_catalog.pg_create_physical_replication_slot(%s)",
                     ("Invalid-Slot",), "42602")
        assert query(connection, "SELECT pg_catalog.pg_create_physical_replication_slot(NULL)") == [(None,)]
        created = query(connection, "SELECT * FROM pg_catalog.pg_create_physical_replication_slot(%s,true)",
                        (reserved,))
        assert len(created) == 1 and created[0][0] == reserved and created[0][1] is not None, created
        initial = created[0][1]
        expect_state(connection, "SELECT pg_catalog.pg_replication_slot_advance(%s,'0/0')",
                     (reserved,), "22023")
        assert query(connection, "SELECT * FROM pg_catalog.pg_replication_slot_advance(%s,%s::pg_lsn)",
                     (reserved, initial)) == [(reserved, initial)]
        # SQL advancement clamps to actual durable progress, even for a future target.
        query(connection, "CHECKPOINT")
        end = query(connection, "SELECT pg_current_wal_flush_lsn()")[0][0]
        interior = lsn_number(end) - 1
        assert interior > lsn_number(initial), (initial, end)
        interior_text = f"{interior >> 32:X}/{interior & 0xffffffff:08X}"
        assert query(connection, "SELECT * FROM pg_catalog.pg_replication_slot_advance(%s,%s::pg_lsn)",
                     (reserved, interior_text)) == [(reserved, interior_text)]
        advanced = query(connection, "SELECT * FROM pg_catalog.pg_replication_slot_advance(%s,'FFFFFFFF/FFFFFFFF')",
                         (reserved,))
        after = query(connection, "SELECT pg_current_wal_flush_lsn()")[0][0]
        assert advanced[0][0] == reserved and lsn_number(end) <= lsn_number(advanced[0][1]) <= lsn_number(after), advanced
        assert query(connection, "SELECT confirmed_flush_lsn FROM pg_catalog.pg_replication_slots "
                     "WHERE slot_name=%s", (reserved,)) == [(None,)]
        if initial != end:
            expect_state(connection, "SELECT pg_catalog.pg_replication_slot_advance(%s,%s::pg_lsn)",
                         (reserved, initial), "55000")
        other = psycopg2.connect(connection.dsn)
        other.autocommit = True
        assert query(other, "SELECT * FROM pg_catalog.pg_create_physical_replication_slot(%s,true,true)",
                     (temporary,))[0][0] == temporary
        assert query(connection, "SELECT temporary,active,active_pid IS NOT NULL "
                     "FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (temporary,)) == [(True, True, True)]
        expect_state(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (temporary,), "55006")
        other.close()
        other = None
        wait_absent(connection, temporary)
        query(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (plain,))
        expect_state(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (plain,), "42704")
        print("physical-slots: public create, NULL, catalog, advance, duplicate and temporary ownership PASS", flush=True)
    finally:
        if other is not None:
            other.close()
        for name in names:
            try:
                query(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (name,))
            except psycopg2.Error as error:
                if error.pgcode not in ("42704", "42883"):
                    raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--prefix", default="native_slot_contract")
    args = parser.parse_args()
    assert args.prefix and len(args.prefix) <= 48
    assert all(character in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in args.prefix)
    connection = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                                  dbname="postgres", connect_timeout=3)
    connection.autocommit = True
    try:
        exercise(connection, args.prefix)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
