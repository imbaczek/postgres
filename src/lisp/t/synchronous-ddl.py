"""Exercise final catalog commit boundaries with controlled synchronous feedback."""
from pathlib import Path
import runpy
import select


api = runpy.run_path(str(Path(__file__).with_name('synchronous-commit.py')))
BaseOwnedLisp, Connection = api['OwnedLisp'], api['Connection']
configure, wait_for_commit, acknowledge = api['configure'], api['wait_for_commit'], api['acknowledge']


class OwnedLisp(BaseOwnedLisp):
    def control(self, expression):
        output = super().control(expression)
        assert ';; read error:' not in output, output
        return output


def receive_ready(connection):
    frames = []
    while True:
        frame = connection.receive()
        frames.append(frame)
        if frame[0] == b'Z':
            return frames


def expect_closed(connection):
    try:
        frame = connection.receive()
    except TimeoutError as error:
        raise AssertionError('postcommit failure did not close the connection') from error
    except (EOFError, ConnectionResetError):
        return
    raise AssertionError(f'postcommit failure emitted a frame: {frame!r}')


def ddl_case(server, connection, statement, outcome='ack', with_xid=False):
    configure(server, 'FIRST 1 (peer)')
    connection.send(b'Q', statement.encode() + b'\0')
    wait_for_commit(server)
    assert not select.select([connection.socket], [], [], 0)[0], statement
    xid_check = '(not (null (backend-xid owner)))' if with_xid else '(null (backend-xid owner))'
    endpoint = 'transaction-wal-obligation-user-end' if with_xid else 'transaction-wal-obligation-final-ddl-end'
    server.control(
        '(let* ((owner (sb-thread:with-mutex (*wal-sender-lock*) '
        '(sync-replication-waiter-backend (first (getf '
        '(sync-replication-state-queues *sync-replication-state*) :flush))))) '
        '(context (backend-wal-obligation owner))) '
        f'(assert {xid_check}) '
        '(assert (transaction-wal-obligation-final-ddl-p context)) '
        '(sb-thread:with-mutex (*wal-sender-lock*) '
        f'(assert (= ({endpoint} context) '
        '(sync-replication-waiter-lsn (first (getf '
        '(sync-replication-state-queues *sync-replication-state*) :flush)))))))')
    # Acquiring these locks through the owning server proves the waiter released
    # publication locks. A control-client timeout fails the test if it did not.
    server.control(
        '(with-schema-write-lock (with-write-catalog-locks '
        '(sb-thread:with-mutex (*wal-lock*) :publication-locks-available)))')
    server.control('(sb-thread:with-mutex (*logical-ddl-event-order-lock*) :event-lock-available)')
    if outcome == 'ack':
        acknowledge(server)
    elif outcome == 'disable':
        configure(server, '')
    elif outcome in ('cancel', 'terminate'):
        operation = 'cancel-backend' if outcome == 'cancel' else 'terminate-backend'
        server.control(
            '(let ((owner (sb-thread:with-mutex (*wal-sender-lock*) '
            '(sync-replication-waiter-backend (first (getf '
            '(sync-replication-state-queues *sync-replication-state*) :flush)))))) '
            f'({operation} owner))')
    elif outcome == 'source':
        server.control(
            '(multiple-value-bind (source generation) (sync-replication-configuration-state) '
            '(sync-replication-install-configuration "" (gensym "DDL-SOURCE-") source generation))')
    else:
        assert outcome == 'timeout', outcome
    if outcome in ('terminate', 'source'):
        expect_closed(connection)
    else:
        frames = receive_ready(connection)
        expected = [b'N', b'C', b'Z'] if outcome in ('cancel', 'timeout') else [b'C', b'Z']
        assert [kind for kind, _ in frames] == expected, (statement, frames)
        if outcome in ('cancel', 'timeout'):
            assert b'already committed locally' in frames[0][1], frames
    configure(server, '')
    print(f'synchronous-ddl: {statement} PASS', flush=True)


def failed_and_skipped(server, connection):
    configure(server, 'FIRST 1 (peer)')
    connection.query('CREATE TABLE IF NOT EXISTS ddl_rows (id int)')
    configure(server, '')
    connection.query('INSERT INTO ddl_rows VALUES (1), (1)')
    configure(server, 'FIRST 1 (peer)')
    connection.send(b'Q', b'CREATE UNIQUE INDEX ddl_bad ON ddl_rows(id)\0')
    frames = receive_ready(connection)
    assert [kind for kind, _ in frames] == [b'E', b'Z'], frames
    assert connection.query('SELECT 1') == [['1']]
    configure(server, '')
    connection.query('CREATE SEQUENCE ddl_seq')
    configure(server, 'FIRST 1 (peer)')
    connection.query('BEGIN')
    assert connection.query("SELECT nextval('ddl_seq')") == [['1']]
    connection.query('ROLLBACK')
    configure(server, '')
    assert connection.query("SELECT nextval('ddl_seq')") == [['2']]
    print('synchronous-ddl: skipped DDL, failed index and sequence rollback PASS', flush=True)


def preparation_cases(server, connection):
    server.control('(defvar *ddl-test-before-wal* nil)')
    connection.query('SET synchronous_commit = on')
    server.control('(setf *ddl-test-before-wal* *wal-end-lsn*)')
    connection.send(b'Q', b"SET synchronous_commit = 'invalid'\0")
    frames = receive_ready(connection)
    assert [kind for kind, _ in frames] == [b'E', b'Z'], frames
    server.control('(assert (= *ddl-test-before-wal* *wal-end-lsn*))')
    assert connection.query('SHOW synchronous_commit') == [['on']]
    server.control('(defvar *ddl-test-save* (symbol-function \'save-catalog))')
    server.control(
        '(setf (symbol-function \'save-catalog) '
        '(lambda (&rest arguments) '
        '(multiple-value-prog1 (apply *ddl-test-save* arguments) '
        '(when (getf arguments :final-ddl-p) '
        '(session-set-synchronous-commit "local")))))')
    try:
        ddl_case(server, connection, 'CREATE TABLE ddl_captured_mode (id int)')
        assert connection.query('SHOW synchronous_commit') == [['local']]
        configure(server, 'FIRST 1 (peer)')
        connection.query('CREATE TABLE ddl_subsequent_local (id int)')
        configure(server, '')
        connection.query('SET synchronous_commit = on')
        ddl_case(server, connection, 'ALTER TABLE ddl_rows ADD CONSTRAINT captured_check '
                 "CHECK (nextval('ddl_seq') > 0)", with_xid=True)
    finally:
        server.control('(setf (symbol-function \'save-catalog) *ddl-test-save*)')
        connection.query('SET synchronous_commit = on')
    print('synchronous-ddl: prepublication validation and captured mode PASS', flush=True)


def late_failure(server, witness, with_xid, nonlocal_exit=False):
    connection = Connection(server.port)
    name = 'ddl_late_xid' if with_xid else 'ddl_late'
    if nonlocal_exit:
        name += '_exit'
    if with_xid:
        witness.query(f'CREATE TABLE {name} (id int)')
        witness.query(f'INSERT INTO {name} VALUES (1)')
    server.control('(defvar *ddl-test-save* (symbol-function \'save-catalog))')
    server.control('(defvar *ddl-test-xid* nil)')
    escape = ('(sb-thread:return-from-thread :ddl-test-exit)' if nonlocal_exit
              else '(error "injected error after final DDL save")')
    server.control(
        '(setf (symbol-function \'save-catalog) '
        '(lambda (&rest arguments) '
        '(multiple-value-prog1 (apply *ddl-test-save* arguments) '
        '(when (getf arguments :final-ddl-p) '
        '(setf *ddl-test-xid* (backend-xid *backend*)) '
        f'{escape}))))')
    try:
        statement = (f'ALTER TABLE {name} ADD CONSTRAINT late_check '
                     "CHECK (nextval('ddl_seq') > 0)" if with_xid
                     else f'CREATE TABLE {name} (id int)')
        connection.send(b'Q', statement.encode() + b'\0')
        expect_closed(connection)
    finally:
        server.control('(setf (symbol-function \'save-catalog) *ddl-test-save*)')
        connection.close()
    assert witness.query(f'SELECT count(*) FROM {name}') == [['1' if with_xid else '0']]
    if with_xid:
        server.control('(assert *ddl-test-xid*)')
        server.control('(assert (= +clog-aborted+ (clog-get *ddl-test-xid*)))')
        server.control('(assert (not (member *ddl-test-xid* *active-xids*)))')
    else:
        server.control('(assert (null *ddl-test-xid*))')
    server.control('(assert (not *wal-poisoned*))')
    print(f'synchronous-ddl: late failure xid={with_xid} nonlocal={nonlocal_exit} PASS', flush=True)


def run():
    server = OwnedLisp()
    connection = None
    try:
        server.start()
        connection = Connection(server.port)
        ddl_case(server, connection, 'CREATE TABLE ddl_rows (id int)')
        ddl_case(server, connection, 'ALTER TABLE ddl_rows ADD CONSTRAINT ddl_check CHECK (id > 0)')
        ddl_case(server, connection, 'CREATE INDEX ddl_index ON ddl_rows(id)')
        ddl_case(server, connection, 'DROP INDEX ddl_index', outcome='disable')
        failed_and_skipped(server, connection)
        preparation_cases(server, connection)
        late_failure(server, connection, False)
        late_failure(server, connection, True)
        late_failure(server, connection, False, nonlocal_exit=True)
        late_failure(server, connection, True, nonlocal_exit=True)
        for outcome in ('cancel', 'timeout', 'terminate', 'source'):
            actor = Connection(server.port)
            try:
                if outcome == 'timeout':
                    actor.query('SET statement_timeout = 1000')
                ddl_case(server, actor, f'CREATE TABLE ddl_{outcome} (id int)', outcome)
            finally:
                actor.close()
        ddl_case(server, connection, 'DROP TABLE ddl_rows')
    finally:
        if connection is not None:
            connection.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()
