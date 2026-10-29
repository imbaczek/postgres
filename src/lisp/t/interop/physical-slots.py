"""Public physical-slot contract shared by native and pinned C peers."""
import argparse
import os
import signal
import threading
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


def exercise_slot_srf(connection, name):
    fields = ("slot_name,plugin,slot_type,datoid,temporary,active,active_pid,xmin,catalog_xmin,"
              "restart_lsn,confirmed_flush_lsn,wal_status,safe_wal_size,two_phase,two_phase_at,"
              "inactive_since,conflicting,invalidation_reason,failover,synced,slotsync_skip_reason")
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM pg_catalog.pg_get_replication_slots() WHERE slot_name=%s", (name,))
        assert [column.name for column in cursor.description] == fields.split(",")
        assert [column.type_code for column in cursor.description] == [
            19, 19, 25, 26, 16, 16, 23, 28, 28, 3220, 3220, 25, 20, 16, 3220, 1184, 16, 25, 16, 16, 25]
        rows = cursor.fetchall()
    assert rows == query(connection, f"SELECT {fields} FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,))
    expect_state(connection, "SELECT pg_catalog.pg_get_replication_slots(1)", (), "42883")
    for statement, state in (
            ("SELECT pg_create_physical_replication_slot(bogus=>1, missing_column)", "42703"),
            ("SELECT pg_create_physical_replication_slot(slot_name=>'a', slot_name=>missing_column)", "42703"),
            ("SELECT pg_create_physical_replication_slot(temporary=>'junk')", "42883"),
            ("SELECT pg_replication_slot_advance(upto_lsn=>'junk')", "42883"),
            ("SELECT pg_drop_replication_slot(slot_name=>'absent_slot')", "42883"),
            ("SELECT pg_get_replication_slots(bogus=>1)", "42883"),
            ("SELECT pg_get_replication_slots(bogus=>1,bogus=>2)", "42601"),
            ("SELECT pg_create_physical_replication_slot(bogus=>1,bogus=>2)", "42601")):
        expect_state(connection, statement, (), state)


def exercise_patroni(connection, prefix):
    # Use the installed SlotsHandler unchanged, with only its database transport supplied here.
    from types import SimpleNamespace
    from patroni.postgresql.slots import SlotsHandler

    postgres = SimpleNamespace(major_version=200000, wal_name="wal", lsn_name="lsn",
                               can_advance_slots=True,
                               query=lambda sql, *params, **kwargs: query(connection, sql, params))
    handler = object.__new__(SlotsHandler)
    handler._postgresql = postgres
    handler._replication_slots = {}
    handler._schedule_load_slots = True
    handler._force_readiness_check = False
    name = prefix + "_patroni"
    assert query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == []
    try:
        handler._ensure_physical_slots({name: {"type": "physical"}}, False)
        handler.load_replication_slots()
        slot = handler._replication_slots[name]
        assert slot["type"] == "physical" and slot["restart_lsn"] > 0, slot
        for use_function in (False, True):
            rows = query(connection, handler.pg_replication_slots_query(use_function))
            assert any(row[0] == name and row[1] == "physical" and row[3] == slot["restart_lsn"] for row in rows), rows
        query(connection, "CHECKPOINT")
        end = lsn_number(query(connection, "SELECT pg_current_wal_flush_lsn()")[0][0])
        assert end > slot["restart_lsn"], (end, slot)
        handler._ensure_physical_slots({name: {"type": "physical", "expected_active": False, "lsn": end}}, False)
        actual = query(connection, "SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,))
        assert lsn_number(actual[0][0]) == end, actual
        assert handler.drop_replication_slot(name) == (False, True)
        wait_absent(connection, name)
        print("physical-slots: unmodified Patroni SlotsHandler create, view/SRF queries, advance and drop PASS", flush=True)
    finally:
        if query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)):
            query(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (name,))


def exercise_standby(connection, prefix):
    """Require a caller-owned paused standby with durable receipt ahead of replay."""
    assert query(connection, "SELECT pg_is_in_recovery(),pg_is_wal_replay_paused()") == [(True, True)]
    replay, received = query(connection, "SELECT pg_last_wal_replay_lsn(),pg_last_wal_receive_lsn()")[0]
    assert lsn_number(received) > lsn_number(replay) > 0, (replay, received)
    name = prefix + "_standby"
    assert query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == []
    try:
        created = query(connection, "SELECT * FROM pg_catalog.pg_create_physical_replication_slot(%s,true)", (name,))
        assert created[0][0] == name and 0 < lsn_number(created[0][1]) <= lsn_number(replay), created
        exercise_slot_srf(connection, name)
        interior = lsn_number(replay) - 1
        if interior > lsn_number(created[0][1]):
            value = f"{interior >> 32:X}/{interior & 0xffffffff:08X}"
            advanced = query(connection, "SELECT * FROM pg_catalog.pg_replication_slot_advance(%s,%s::pg_lsn)", (name, value))
            assert advanced[0][0] == name and lsn_number(advanced[0][1]) == interior, advanced
        advanced = query(connection, "SELECT * FROM pg_catalog.pg_replication_slot_advance(%s,'FFFFFFFF/FFFFFFFF')", (name,))
        assert advanced[0][0] == name and lsn_number(advanced[0][1]) == lsn_number(replay), advanced
        assert query(connection, "SELECT confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == [(None,)]
        assert query(connection, "SELECT pg_last_wal_replay_lsn()")[0][0] == replay
        print("physical-slots: paused standby reservation, interior advance, replay clamp and SRF PASS", flush=True)
    finally:
        if query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)):
            query(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (name,))


def exercise_temporary_slot_loss(connection, prefix, switch_wal=False):
    """Require a finite cap: one byte for native, less than one segment for C."""
    name = prefix + "_owner_loss"
    assert query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == []
    creator = psycopg2.connect(connection.dsn)
    creator.autocommit = True
    try:
        pid = query(creator, "SELECT pg_backend_pid()")[0][0]
        query(creator, "SELECT pg_create_physical_replication_slot(%s,true,true)", (name,))
        assert query(connection, "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == [(pid,)]
        if switch_wal:
            query(connection, "SELECT pg_create_restore_point('slot_owner_loss')")
            query(connection, "SELECT pg_switch_wal()")
        query(connection, "CHECKPOINT")
        wait_absent(connection, name)
        try:
            query(creator, "SELECT 1")
        except psycopg2.Error as error:
            assert error.pgcode in (None, "57P01"), (error.pgcode, str(error))
        else:
            raise AssertionError("invalidated temporary-slot creator remains usable")
        assert query(connection, "SELECT 1") == [(1,)]
        print("physical-slots: checkpoint terminates temporary-slot creator and removes slot PASS", flush=True)
    finally:
        creator.close()
        wait_absent(connection, name)


def exercise_active_owner_oracle(connection, prefix):
    """Stop the owned C sender and verify CHECKPOINT waits beyond five seconds.

    Check that the slot remains active and not lost until the sender resumes,
    then verify the slot is lost after CHECKPOINT completes.
    """
    from psycopg2.extras import PhysicalReplicationConnection

    name = prefix + "_active_owner"
    sender = None
    sender_fd = None
    sender_stopped = False
    checkpoint = None
    finished = threading.Event()
    errors = []

    def checkpoint_query():
        try:
            with psycopg2.connect(connection.dsn) as check_connection:
                check_connection.autocommit = True
                query(check_connection, "CHECKPOINT")
        except BaseException as error:
            errors.append(error)
        finally:
            finished.set()

    def parent_pid(pid):
        with open(f"/proc/{pid}/stat", encoding="ascii") as stat:
            return int(stat.read().rsplit(")", 1)[1].split()[1])

    try:
        query(connection, "CHECKPOINT")
        restart = query(connection, "SELECT lsn FROM pg_create_physical_replication_slot(%s,true)", (name,))[0][0]
        sender = psycopg2.connect(connection.dsn, connection_factory=PhysicalReplicationConnection)
        sender.cursor().start_replication(slot_name=name, start_lsn=restart)
        sender_pid = sender.get_backend_pid()
        assert query(connection, "SELECT active_pid FROM pg_replication_slots WHERE slot_name=%s", (name,)) == [(sender_pid,)]
        sender_fd = os.pidfd_open(sender_pid)
        assert os.stat(f"/proc/{sender_pid}").st_uid == os.geteuid()
        assert parent_pid(sender_pid) == parent_pid(connection.get_backend_pid())
        signal.pidfd_send_signal(sender_fd, signal.SIGSTOP)
        sender_stopped = True
        for step in range(2):
            query(connection, "SELECT pg_create_restore_point(%s)", (f"active_owner_{step}",))
            query(connection, "SELECT pg_switch_wal()")
        checkpoint = threading.Thread(target=checkpoint_query, name="C active-slot checkpoint")
        checkpoint.start()
        deadline = time.monotonic() + 10
        while not query(connection, "SELECT pid FROM pg_stat_activity WHERE backend_type='checkpointer' "
                        "AND wait_event='ReplicationSlotDrop'"):
            assert not finished.is_set(), errors
            assert time.monotonic() < deadline, "checkpoint did not wait for the stopped slot owner"
            time.sleep(0.02)
        assert query(connection, "SELECT active_pid,restart_lsn IS NOT NULL,invalidation_reason "
                     "FROM pg_replication_slots WHERE slot_name=%s", (name,)) == [(sender_pid, True, None)]
        assert not finished.wait(5.2), ("checkpoint returned while the sender was stopped", errors)
        signal.pidfd_send_signal(sender_fd, signal.SIGCONT)
        sender_stopped = False
        assert finished.wait(10), "checkpoint did not finish after sender release"
        checkpoint.join()
        assert not errors, errors
        assert query(connection, "SELECT active,restart_lsn,wal_status,invalidation_reason "
                     "FROM pg_replication_slots WHERE slot_name=%s", (name,)) == [(False, None, "lost", "wal_removed")]
        print("physical-slots: C CHECKPOINT waits beyond five seconds for active sender release before loss PASS", flush=True)
    finally:
        if sender_fd is not None:
            try:
                if sender_stopped:
                    signal.pidfd_send_signal(sender_fd, signal.SIGCONT)
            except ProcessLookupError:
                pass
            os.close(sender_fd)
        if sender is not None:
            sender.close()
        if checkpoint is not None:
            checkpoint.join(timeout=10)
        if query(connection, "SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s", (name,)):
            query(connection, "SELECT pg_drop_replication_slot(%s)", (name,))


def exercise_physical_feedback_oracle(connection, prefix):
    from psycopg2.extras import PhysicalReplicationConnection

    name = prefix + "_feedback"
    sender = None

    def wait_state(restart, active=True):
        deadline = time.monotonic() + 10
        while True:
            row = query(connection, "SELECT restart_lsn,active FROM pg_replication_slots WHERE slot_name=%s", (name,))[0]
            if (None if row[0] is None else lsn_number(row[0])) == restart and row[1] == active:
                return
            assert time.monotonic() < deadline, row
            time.sleep(0.02)

    try:
        query(connection, "CHECKPOINT")
        start = lsn_number(query(connection, "SELECT pg_current_wal_flush_lsn()")[0][0])
        query(connection, "SELECT * FROM pg_create_physical_replication_slot(%s)", (name,))
        sender = psycopg2.connect(connection.dsn, connection_factory=PhysicalReplicationConnection)
        cursor = sender.cursor()
        cursor.start_replication(slot_name=name, start_lsn=start)
        wait_state(None)
        query(connection, "SELECT pg_create_restore_point('physical_feedback')")
        query(connection, "CHECKPOINT")
        high = lsn_number(query(connection, "SELECT pg_current_wal_flush_lsn()")[0][0])
        assert high > start
        cursor.send_feedback(write_lsn=high, flush_lsn=start, apply_lsn=start, force=True)
        wait_state(start)
        cursor.send_feedback(write_lsn=high, flush_lsn=high, apply_lsn=start, force=True)
        wait_state(high)
        sender.close()
        sender = None
        wait_state(high, active=False)
        query(connection, "CHECKPOINT")
        sender = psycopg2.connect(connection.dsn, connection_factory=PhysicalReplicationConnection)
        cursor = sender.cursor()
        cursor.start_replication(slot_name=name, start_lsn=start)
        wait_state(high)
        cursor.send_feedback(write_lsn=high, flush_lsn=start, apply_lsn=start, force=True)
        wait_state(start)
        sender.close()
        sender = None
        wait_state(start, active=False)
        print("physical-slots: C START leaves unreserved state, flush drives retention, and reconnect accepts backward feedback PASS", flush=True)
    finally:
        if sender is not None:
            sender.close()
        if query(connection, "SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s", (name,)):
            query(connection, "SELECT pg_drop_replication_slot(%s)", (name,))


def exercise_retention_oracle(connection, prefix):
    """Check the pinned C oracle on a dedicated 16 MB-segment peer with a 1 MB cap."""
    assert query(connection, "SELECT current_setting('max_slot_wal_keep_size'),"
                 "current_setting('wal_segment_size'),current_setting('wal_keep_size')") == [("1MB", "16MB", "0")]
    name = prefix + "_cap"
    assert query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == []
    try:
        query(connection, "CHECKPOINT")
        created = query(connection, "SELECT * FROM pg_catalog.pg_create_physical_replication_slot(%s,true)", (name,))
        restart = lsn_number(created[0][1])
        segment_size = 16 * 1024 * 1024
        failure = (restart // segment_size + 1) * segment_size
        observations = []
        for step in range(3):
            row = query(connection, "SELECT restart_lsn,wal_status,safe_wal_size,pg_current_wal_lsn() "
                        "FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,))[0]
            write = lsn_number(row[3])
            assert lsn_number(row[0]) == restart and row[2] == failure - write, row
            assert row[1] == ("reserved" if step == 0 else "unreserved"), row
            if step == 1:
                assert row[2] == 0, row
            elif step == 2:
                assert row[2] == -segment_size, row
            observations.append(row)
            if step < 2:
                if step == 1:
                    # A switch at an untouched segment boundary is a no-op in C.
                    query(connection, "SELECT pg_create_restore_point('slot_cap_boundary')")
                query(connection, "SELECT pg_switch_wal()")
        query(connection, "CHECKPOINT")
        assert query(connection, "SELECT restart_lsn,wal_status,safe_wal_size,invalidation_reason "
                     "FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)) == [
                         (None, "lost", None, "wal_removed")]
        print("physical-slots: C subsegment-cap, exact-boundary, negative-safe-size and checkpoint loss PASS", observations, flush=True)
        exercise_temporary_slot_loss(connection, prefix, switch_wal=True)
        exercise_active_owner_oracle(connection, prefix)
        exercise_physical_feedback_oracle(connection, prefix)
    finally:
        if query(connection, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_name=%s", (name,)):
            query(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (name,))


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
        exercise_slot_srf(connection, reserved)
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
        for end in ("COMMIT", "ROLLBACK"):
            query(other, "BEGIN")
            query(other, end)
            assert query(connection, "SELECT temporary,active FROM pg_catalog.pg_replication_slots "
                         "WHERE slot_name=%s", (temporary,)) == [(True, True)]
        expect_state(other, "SELECT 1/0", (), "22012")
        wait_absent(connection, temporary)
        query(other, "SELECT pg_catalog.pg_create_physical_replication_slot(%s,true,true)", (temporary,))
        query(other, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (temporary,))
        wait_absent(connection, temporary)
        query(other, "SELECT pg_catalog.pg_create_physical_replication_slot(%s,true,true)", (temporary,))
        other.close()
        other = None
        wait_absent(connection, temporary)
        assert query(connection, "SELECT pg_catalog.pg_drop_replication_slot(%s)", (plain,)) == [("",)]
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
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--patroni", action="store_true", help="also exercise the installed Patroni SlotsHandler")
    modes.add_argument("--standby", action="store_true", help="test a paused standby with receipt ahead of replay")
    modes.add_argument("--retention-oracle", action="store_true", help="test a dedicated C peer configured with a 1 MB slot cap")
    modes.add_argument("--temporary-loss", action="store_true", help="test a native peer with a one-byte slot retention cap")
    args = parser.parse_args()
    assert args.prefix and len(args.prefix) <= 48
    assert all(character in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in args.prefix)
    connection = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                                  dbname="postgres", connect_timeout=3)
    connection.autocommit = True
    try:
        if args.retention_oracle:
            exercise_retention_oracle(connection, args.prefix)
        elif args.temporary_loss:
            exercise_temporary_slot_loss(connection, args.prefix)
        elif args.standby:
            exercise_standby(connection, args.prefix)
        else:
            exercise(connection, args.prefix)
        if args.patroni:
            exercise_patroni(connection, args.prefix)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
