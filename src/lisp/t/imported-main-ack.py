"""Schedule real typed child ACKs against SQL temporary slot lifetimes."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import time
import sys


def wait_file(path):
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError('Imported ACK barrier timed out: ' + path.name)
        time.sleep(0.005)


def exercise(connection, control, job, source, peer_progress):
    began = time.monotonic()
    helper = source / 't/imported-main-ack.lisp'
    control(f'(progn (load "{helper}") :ack-helper-loaded)\n(quit)\n', ':ACK-HELPER-LOADED')
    connection.query('CREATE TABLE imported_ack_rows (n integer)')
    results = []
    peer_checks = []
    active = None
    sql = None
    executor = ThreadPoolExecutor(max_workers=1)
    instruments = job / 'tmp/ack-sql-boundaries'
    instruments.mkdir(mode=0o700)
    control(f'(pg::imported-main-ack-instrument-sql "{instruments}/")\n(quit)\n', ':ACK-SQL-INSTRUMENTED')

    def start(point):
        nonlocal active
        count = len(results) + 1
        connection.query(f'INSERT INTO imported_ack_rows VALUES ({count})')
        target = connection.query('SELECT pg_current_wal_flush_lsn()')[0][0]
        high, low = target.split('/')
        numeric = (int(high, 16) << 32) | int(low, 16)
        active = job / ('tmp/ack-stage-' + str(count))
        active.mkdir(mode=0o700)
        control(f'(pg::imported-main-ack-start "alpha" {numeric} "{active}/" :{point})\n(quit)\n',
                ':IMPORTED-ACK-STARTED')
        wait_file(active / 'reached')
        if point == 'lifetime-acquired':
            before = peer_progress()
            deadline = time.monotonic() + 5
            while peer_progress() <= before:
                if time.monotonic() > deadline:
                    raise TimeoutError('Physical peer made no progress during typed ACK lifetime')
                time.sleep(0.005)
            peer_checks.append(dict(before=before,after=peer_progress()))
        return target

    def finish(target, event):
        nonlocal active
        (active / 'release').touch(mode=0o600)
        wait_file(active / 'done')
        response = control('(pg::imported-main-ack-result)\n(quit)\n', 'IMPORTED-ACK-DONE')
        generations = re.findall(r':GENERATION (\d+) :CONFIRMED (\d+) :FORK (\d+) :BARRIERS 1', response)
        assert len(generations) == 1, response
        generation, confirmed, fork = map(int, generations[0])
        high, low = target.split('/')
        assert confirmed == (int(high, 16) << 32) | int(low, 16)
        if results:
            assert generation == results[-1]['generation'] + 1
            assert fork == results[-1]['fork']
        results.append(dict(event=event,generation=generation,confirmed_lsn=target,fork=fork))
        active = None

    create = "SELECT slot_name FROM pg_create_physical_replication_slot('ack_barrier',true,true)"
    drop = "SELECT pg_drop_replication_slot('ack_barrier')"
    try:
        target = start('lifetime-acquired')
        assert connection.query(create) == [['ack_barrier']]
        assert (instruments / 'created').exists()
        finish(target, 'durable temporary creation before fresh child ACK proof')
        assert connection.query("SELECT active,temporary FROM pg_replication_slots WHERE slot_name='ack_barrier'") == [['t','t']]
        target = start('lifetime-acquired')
        connection.query(drop)
        assert (instruments / 'retired').exists()
        finish(target, 'durable temporary retirement before fresh child ACK proof')
        assert connection.query("SELECT slot_name FROM pg_replication_slots WHERE slot_name='ack_barrier'") == []
        for operation, before, after, event in [
                (create, 'create-query', 'created', 'temporary creation waits behind child ACK publication'),
                (drop, 'drop-query', 'retired', 'temporary retirement waits behind child ACK publication')]:
            for name in ('creating','created','retiring','retired','create-query','drop-query'):
                (instruments / name).unlink(missing_ok=True)
            target = start('before-intent')
            sql = executor.submit(connection.query, operation)
            wait_file(instruments / before)
            assert not (instruments / after).exists() and not sql.done()
            # No SQL/control inspection while the writer holds the WAL lock.
            (active / 'release').touch(mode=0o600)
            sql.result(timeout=15)
            sql = None
            assert (instruments / after).exists()
            finish(target, event)
        assert connection.query("SELECT slot_name FROM pg_replication_slots WHERE slot_name='ack_barrier'") == []
        return dict(results=results,confirmed_lsn=results[-1]['confirmed_lsn'],
                    rows=list(range(1,len(results)+1)),peer_during_ack=peer_checks,wall_seconds=time.monotonic()-began)
    finally:
        primary = sys.exc_info()[1]
        failures = []
        if active is not None:
            (active / 'release').touch(mode=0o600,exist_ok=True)
        try:
            if sql is not None:
                sql.result(timeout=15)
        except BaseException as error:
            failures.append(error)
        try:
            executor.shutdown(wait=True)
        finally:
            try:
                control('(pg::imported-main-ack-join)\n(quit)\n', ':ACK-WORKER-REAPED')
            except BaseException as error:
                failures.append(error)
            finally:
                try:
                    control('(pg::imported-main-ack-restore-sql)\n(quit)\n', ':ACK-SQL-RESTORED')
                except BaseException as error:
                    failures.append(error)
        if failures:
            if primary is None:
                raise failures[0]
            if hasattr(primary, 'add_note'):
                primary.add_note('ACK cleanup also failed: ' + ', '.join(type(error).__name__ for error in failures))
