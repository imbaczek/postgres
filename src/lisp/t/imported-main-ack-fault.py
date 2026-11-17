"""Observe actual-main post-intent ACK failures without a clean-shutdown claim."""

from concurrent.futures import ThreadPoolExecutor
import time
import sys
import re
import struct
import os

from interop.parking_wire import RawConnection, WireError


def wait_file(path):
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError('ACK fault boundary timed out: ' + path.name)
        time.sleep(0.005)


def query_until_terminal_close(connection, sql):
    body = sql.encode() + b'\0'
    connection.socket.sendall(b'Q' + struct.pack('!I', len(body) + 4) + body)
    errors = []
    described = False
    while True:
        try:
            kind, body = connection.receive()
        except EOFError:
            break
        if kind == b'T' and not described and not errors:
            described = True
            continue
        assert kind == b'E', (kind, body)
        errors.append(connection.error_fields(body))
    assert len(errors) == 1, errors
    assert errors[0]['C'] == '55000', errors
    assert errors[0]['M'] == 'continuation group is uncertain; cold reconciliation required', errors
    return 'refused-55000-then-eof'


def exercise(port, job, source, cut, operation, control):
    began = time.monotonic()
    control(f'(progn (load "{source}/t/imported-main-ack.lisp") '
            f'(load "{source}/t/imported-main-ack-fault.lisp") :ack-fault-loaded)\n(quit)\n',
            ':ACK-FAULT-LOADED')
    creator = RawConnection(port, 'ack-fault-creator', replication='database')
    observer = RawConnection(port, 'ack-fault-observer', replication='database')
    creator.socket.settimeout(15)
    observer.socket.settimeout(15)
    executor = ThreadPoolExecutor(max_workers=1)
    directory = job / 'tmp/ack-fault'
    markers = job / 'tmp/ack-fault-sql'
    directory.mkdir(mode=0o700)
    markers.mkdir(mode=0o700)
    response = markers / 'response'
    response.mkdir(mode=0o700)
    pending = None
    instrumented = False
    terminal_close = False
    create = "SELECT slot_name FROM pg_create_physical_replication_slot('ack_barrier',true,true)"
    drop = "SELECT pg_drop_replication_slot('ack_barrier')"
    try:
        pid = creator.query('SELECT pg_backend_pid()')[0][0]
        observer_pid = observer.query('SELECT pg_backend_pid()')[0][0]
        if operation == 'retire':
            assert creator.query(create) == [['ack_barrier']]
            if os.environ.get('PG_NATIVE_MAIN_ACK_TRANSACTION') == '1':
                creator.query('BEGIN')
        observer.query('CREATE TABLE imported_fault_rows (n integer)')
        observer.query('INSERT INTO imported_fault_rows VALUES (1)')
        target = observer.query('SELECT pg_current_wal_flush_lsn()')[0][0]
        high, low = target.split('/')
        numeric = (int(high,16) << 32) | int(low,16)
        control(f'(pg::imported-main-ack-instrument-sql "{markers}/" :repeat-markers t)\n(quit)\n', ':ACK-SQL-INSTRUMENTED')
        instrumented = True
        if operation == 'retire':
            control(f'(pg::imported-main-ack-fault-instrument-refusal {int(observer_pid)})\n(quit)\n',
                    ':ACK-REFUSAL-INSTRUMENTED')
        control(f'(pg::imported-main-ack-fault-start {numeric} "{directory}/" :{cut})\n(quit)\n',
                ':ACK-FAULT-STARTED')
        wait_file(directory / 'reached')
        before, after = ('create-query','created') if operation == 'create' else ('drop-query','retired')
        if operation == 'retire':
            pending = executor.submit(query_until_terminal_close, creator, drop)
        else:
            pending = executor.submit(creator.query, create)
        wait_file(markers / before)
        assert not pending.done() and not (markers / after).exists()
        (directory / 'release').touch(mode=0o600)
        wait_file(directory / 'done')
        if operation == 'retire':
            wait_file(response / 'reached')
            observer.close()
            wait_file(markers / 'observer-reaped')
            assert not pending.done()
            (response / 'release').touch(mode=0o600)
            try:
                outcome = pending.result(timeout=15)
            finally:
                pending = None
            terminal_close = True
            assert (markers / 'refusal-validated').is_file()
            assert (markers / 'termination-deferred').is_file()
            terminal = markers / 'terminal'
            wait_file(terminal)
            deadline = time.monotonic() + 15
            while not (snapshot := terminal.read_text()).endswith('\n'):
                if time.monotonic() >= deadline:
                    raise TimeoutError('ACK terminal report was not flushed')
                time.sleep(0.005)
            assert snapshot.startswith('ACK-FAULT-FENCED '), snapshot
            return dict(passed=True, cut=cut, operation=operation, queued_sql=outcome,
                        snapshot=snapshot.strip(), target=target,
                        wall_seconds=time.monotonic()-began)
        try:
            rows = pending.result(timeout=15)
            if operation == 'create':
                assert rows == [['ack_barrier']]
            else:
                assert rows == []
            outcome = 'completed'
        except WireError as error:
            assert error.sqlstate == '55000', error.sqlstate
            outcome = 'refused-55000'
        pending = None
        control('(pg::imported-main-ack-fault-join)\n(quit)\n', ':ACK-FAULT-WORKER-REAPED')
        control('(pg::imported-main-ack-restore-sql)\n(quit)\n', ':ACK-SQL-RESTORED')
        instrumented = False
        snapshot = control('(pg::imported-main-ack-fault-check)\n(quit)\n', 'ACK-FAULT-FENCED')
        temporary = control(f'(pg::imported-main-ack-fault-temporary {pid} "ack_barrier")\n(quit)\n',
                            'ACK-FAULT-TEMP')
        try:
            observer.query("SELECT pg_create_physical_replication_slot('ack_after',true,true)")
        except WireError as error:
            assert error.sqlstate == '55000', error.sqlstate
        else:
            raise AssertionError('Fresh post-cleanup creation bypassed uncertainty')
        states = re.findall(r'ACK-FAULT-TEMP (PRESENT|ABSENT)', temporary)
        assert len(states) == 1, temporary
        expected_present = ((operation == 'create' and outcome == 'completed')
                            or (operation == 'retire' and outcome == 'refused-55000'))
        assert (states[0] == 'PRESENT') == expected_present, (operation,outcome,states)
        if states[0] == 'PRESENT':
            try:
                creator.query(drop)
            except WireError as error:
                assert error.sqlstate == '55000', error.sqlstate
            else:
                raise AssertionError('Fresh post-cleanup retirement bypassed uncertainty')
        control('(pg::imported-main-ack-fault-check)\n(quit)\n', 'ACK-FAULT-FENCED')
        return dict(passed=True,cut=cut,operation=operation,queued_sql=outcome,
                    snapshot=snapshot,temporary=temporary,target=target,
                    wall_seconds=time.monotonic()-began)
    finally:
        primary = sys.exc_info()[1]
        failures = []
        (directory / 'release').touch(mode=0o600,exist_ok=True)
        (response / 'release').touch(mode=0o600,exist_ok=True)
        try:
            if pending is not None:
                pending.result(timeout=15)
        except BaseException as error:
            failures.append(error)
        try:
            executor.shutdown(wait=True)
        finally:
            try:
                if not terminal_close:
                    control('(pg::imported-main-ack-fault-join)\n(quit)\n', ':ACK-FAULT-WORKER-REAPED')
            except BaseException as error:
                failures.append(error)
            finally:
                try:
                    if instrumented and not terminal_close:
                        control('(pg::imported-main-ack-restore-sql)\n(quit)\n', ':ACK-SQL-RESTORED')
                except BaseException as error:
                    failures.append(error)
                finally:
                    creator.close()
                    observer.close()
        if failures:
            if primary is None:
                raise failures[0]
            if hasattr(primary, 'add_note'):
                primary.add_note('ACK fault cleanup also failed: ' + ', '.join(type(error).__name__ for error in failures))
