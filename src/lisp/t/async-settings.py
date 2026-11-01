"""Check public OFF settings and synchronous storage exceptions in an owned server."""
from pathlib import Path
import runpy
import psycopg2


api = runpy.run_path(str(Path(__file__).with_name('async-commit.py')))
OwnedLisp, Connection = api['OwnedLisp'], api['Connection']
pause_writer, resume_writer = api['pause_writer'], api['resume_writer']
wait_control = api['wait_control']


def assert_flushed_receipt(server):
    server.control(
        '(with-wal-critical '
        '(assert *settings-test-receipt*) '
        '(assert (wal-commit-receipt-wal-durable-p *settings-test-receipt*)) '
        '(assert (<= (wal-commit-receipt-terminal-end *settings-test-receipt*) *wal-durable-lsn*)) '
        '(assert (null *wal-pending-page-publication*)))')


def prove_async_write(server, actor, row_id, label, local=False):
    paused = pause_writer(server, label)
    try:
        server.control(
            '(defparameter *settings-probe-wal* (symbol-function \'wal-commit-transaction)) '
            '(defparameter *settings-probe-receipt* nil) '
            '(setf (symbol-function \'wal-commit-transaction) '
            '(lambda (&rest arguments) '
            '(multiple-value-bind (end receipt) (apply *settings-probe-wal* arguments) '
            '(setf *settings-probe-receipt* receipt) (values end receipt))))')
        if local:
            actor.query('BEGIN')
            actor.query('SET LOCAL synchronous_commit=off')
        actor.query(f'INSERT INTO off_rows VALUES ({row_id})')
        if local:
            actor.query('COMMIT')
        server.control(
            '(with-wal-critical '
            '(assert *settings-probe-receipt*) '
            '(assert (null (wal-commit-receipt-wal-durable-p *settings-probe-receipt*))) '
            '(assert (< *wal-durable-lsn* (wal-commit-receipt-terminal-end *settings-probe-receipt*))) '
            '(assert *wal-pending-page-publication*))')
    finally:
        server.control('(setf (symbol-function \'wal-commit-transaction) *settings-probe-wal*)')
        resume_writer(server, paused)
    wait_control(server, '(list :drained (null *wal-pending-page-publication*))', ':DRAINED T')


def run():
    server = OwnedLisp()
    actor = None
    startup = None
    paused = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE off_rows (id int PRIMARY KEY)')
        startup = psycopg2.connect(host='127.0.0.1', port=server.port, user='postgres',
                                   dbname='postgres', options='-c synchronous_commit=off')
        startup.autocommit = True
        with startup.cursor() as cursor:
            cursor.execute("SELECT setting,source FROM pg_settings WHERE name='synchronous_commit'")
            assert cursor.fetchall() == [('off', 'client')]
        startup.close()
        startup = None
        for alias in ('off', 'false', 'no', '0'):
            actor.query(f"SET synchronous_commit='{alias}'")
            assert actor.query('SHOW synchronous_commit') == [['off']]
            actor.query('RESET synchronous_commit')
            assert actor.query('SHOW synchronous_commit') == [['on']]
        for terminal in ('COMMIT', 'ROLLBACK'):
            actor.query('BEGIN')
            actor.query('SET LOCAL synchronous_commit=off')
            assert actor.query('SHOW synchronous_commit') == [['off']]
            actor.query(terminal)
            assert actor.query('SHOW synchronous_commit') == [['on']]
        actor.query('BEGIN')
        actor.query('SET synchronous_commit=off')
        actor.query('ROLLBACK')
        assert actor.query('SHOW synchronous_commit') == [['on']]
        prove_async_write(server, actor, 9, 'local-write', local=True)
        assert actor.query('SHOW synchronous_commit') == [['on']]
        actor.query('SET synchronous_commit=off')
        paused = pause_writer(server, 'settings')
        server.control(
            '(defvar *settings-test-receipt* nil) '
            '(defvar *settings-test-owner-limit* (getf *wal-publication-pressure-limits* :owners)) '
            '(defvar *settings-test-wal* (symbol-function \'wal-commit-transaction)) '
            '(setf (symbol-function \'wal-commit-transaction) '
            '(lambda (&rest arguments) '
            '(multiple-value-bind (end receipt) (apply *settings-test-wal* arguments) '
            '(setf *settings-test-receipt* receipt) (values end receipt)))) '
            '(setf (getf *wal-publication-pressure-limits* :owners) 0)')
        actor.query('INSERT INTO off_rows VALUES (1)')
        assert_flushed_receipt(server)
        server.control('(setf (getf *wal-publication-pressure-limits* :owners) *settings-test-owner-limit*)')
        actor.query('BEGIN')
        actor.query('TRUNCATE off_rows')
        actor.query('INSERT INTO off_rows VALUES (2)')
        actor.query('COMMIT')
        assert_flushed_receipt(server)
        actor.query('CREATE TABLE off_ddl (id int)')
        server.control('(with-wal-critical (assert (null *wal-pending-page-publication*)) '
                       '(assert (= *wal-end-lsn* *wal-durable-lsn*)))')
        server.control('(wal-writer-test-start-stop)')
        resume_writer(server, paused)
        paused = None
        wait_control(server, '(list :done (not (sb-thread:thread-alive-p *writer-test-stop-worker*)))',
                     ':DONE T')
        server.control('(assert (null *writer-test-stop-error*))')
        actor.query('INSERT INTO off_rows VALUES (3)')
        assert_flushed_receipt(server)
        actor.close()
        actor = None
        server.stop(immediate=True)
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM off_rows ORDER BY id') == [['2'], ['3']]
        assert actor.query('SELECT count(*) FROM off_ddl') == [['0']]
        actor.close()
        actor = None
        server.stop()
        (server.data / 'postgresql.conf').write_text(
            "synchronous_commit=off\nsynchronous_standby_names='FIRST 1 (missing_peer)'\n")
        server.start()
        actor = Connection(server.port)
        assert actor.query('SHOW synchronous_commit') == [['off']]
        assert actor.query('SHOW synchronous_standby_names') == [['FIRST 1 (missing_peer)']]
        assert actor.query("SELECT setting,source FROM pg_settings WHERE name='synchronous_commit'") == [
            ['off', 'configuration file']]
        prove_async_write(server, actor, 4, 'cold-default-write')
        actor.query('SET synchronous_commit=on')
        actor.query('RESET synchronous_commit')
        assert actor.query('SHOW synchronous_commit') == [['off']]
        startup = psycopg2.connect(host='127.0.0.1', port=server.port, user='postgres',
                                   dbname='postgres', options='-c synchronous_commit=off')
        startup.autocommit = True
        actor.query('SET synchronous_commit=off')
        (server.data / 'postgresql.conf').write_text('synchronous_commit=local\n')
        assert actor.query('SELECT pg_reload_conf()') == [['t']]
        wait_control(server,
                     '(list :default (let ((*backend* nil)) (session-synchronous-commit-value)))',
                     ':DEFAULT "local"')
        assert actor.query('SHOW synchronous_commit') == [['off']]
        actor.query('RESET synchronous_commit')
        assert actor.query('SHOW synchronous_commit') == [['local']]
        with startup.cursor() as cursor:
            cursor.execute('SET synchronous_commit=on')
            cursor.execute('RESET synchronous_commit')
            cursor.execute('SHOW synchronous_commit')
            assert cursor.fetchall() == [('off',)]
        (server.data / 'postgresql.conf').write_text('synchronous_commit=off\n')
        assert actor.query('SELECT pg_reload_conf()') == [['t']]
        wait_control(server,
                     '(list :default (let ((*backend* nil)) (session-synchronous-commit-value)))',
                     ':DEFAULT "off"')
        prove_async_write(server, actor, 5, 'reloaded-default-write')
        print('async-settings: aliases, LOCAL/RESET/abort, pressure/reset/DDL/stopped-writer flush, '
              'SIGKILL recovery, client/cold defaults and reload precedence PASS', flush=True)
    finally:
        resume_writer(server, paused)
        if actor is not None:
            actor.close()
        if startup is not None:
            startup.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()
