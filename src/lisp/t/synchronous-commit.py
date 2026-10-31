"""Exercise committed synchronous-wait outcomes through an owned SQL server.

The control REPL supplies controlled sender acknowledgements; it does not run a
replica or a coordinator. Public synchronous settings are a later milestone.
"""
from pathlib import Path
import runpy
import select
import struct
import time


fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp, Connection = fixture["OwnedLisp"], fixture["Connection"]


def configure(server, names):
    server.control(
        '(multiple-value-bind (source generation) '
        '(sync-replication-configuration-state) '
        f'(sync-replication-install-configuration "{names}" source source generation))')


def wait_for_commit(server):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        output = server.control(
            '(sb-thread:with-mutex (*wal-sender-lock*) '
            '(list :waiting (length (getf '
            '(sync-replication-state-queues *sync-replication-state*) :flush))))')
        if ':WAITING 1' in output:
            return
        if server.process.poll() is not None:
            raise RuntimeError('owned SQL server exited during synchronous commit')
        time.sleep(0.02)
    raise TimeoutError('SQL commit did not enqueue a synchronous wait')


def acknowledge(server):
    server.control(
        '(let* ((end (sb-thread:with-mutex (*wal-sender-lock*) '
        '(sync-replication-waiter-lsn (first (getf '
        '(sync-replication-state-queues *sync-replication-state*) :flush))))) '
        '(sender (wal-sender-register '
        '(make-backend :pid 999999 :replication-mode :database :application-name "peer")))) '
        '(unwind-protect (progn (wal-sender-set-state sender "streaming") '
        '(wal-sender-note-sent sender end) '
        '(wal-sender-note-reply sender end end end 1)) '
        '(wal-sender-unregister (wal-sender-status-backend sender))))')


def run():
    server = OwnedLisp()
    connection = witness = None
    try:
        server.start()
        witness = Connection(server.port)
        witness.query('CREATE TABLE synchronous_rows (id int)')
        for number, outcome in enumerate(('ack', 'cancel', 'timeout', 'terminate', 'source'), 1):
            connection = Connection(server.port)
            if outcome == 'timeout':
                connection.query('SET statement_timeout = 1000')
            configure(server, 'FIRST 1 (peer)')
            connection.send(b'Q', f'INSERT INTO synchronous_rows VALUES ({number})'.encode() + b'\0')
            wait_for_commit(server)
            assert not select.select([connection.socket], [], [], 0)[0], outcome
            # The committing XID remains in the active set until its wait ends.
            assert witness.query('SELECT count(*) FROM synchronous_rows') == [[str(number - 1)]]
            if outcome == 'ack':
                acknowledge(server)
            elif outcome in ('cancel', 'terminate'):
                operation = 'cancel-backend' if outcome == 'cancel' else 'terminate-backend'
                server.control(
                    '(let ((owner (sb-thread:with-mutex (*wal-sender-lock*) '
                    '(sync-replication-waiter-backend (first (getf '
                    '(sync-replication-state-queues *sync-replication-state*) :flush)))))) '
                    f'({operation} owner))')
            elif outcome == 'source':
                server.control(
                    '(multiple-value-bind (source generation) '
                    '(sync-replication-configuration-state) '
                    '(sync-replication-install-configuration "" '
                    '(gensym "TEST-SOURCE-") source generation))')
            frames = []
            try:
                while True:
                    kind, body = connection.receive()
                    frames.append((kind, body))
                    if kind == b'Z':
                        break
            except TimeoutError as error:
                raise AssertionError(f'{outcome}: connection did not close or complete') from error
            except (EOFError, ConnectionResetError):
                assert outcome in ('terminate', 'source'), (outcome, frames)
            if outcome in ('terminate', 'source'):
                assert not frames, (outcome, frames)
            else:
                assert [kind for kind, _ in frames] == (
                    [b'C', b'Z'] if outcome == 'ack' else [b'N', b'C', b'Z']), (outcome, frames)
                if outcome != 'ack':
                    assert b'already committed locally' in frames[0][1], frames
            configure(server, '')
            connection.close()
            connection = None
            print(f'synchronous-commit: {outcome} PASS', flush=True)
        assert witness.query('SELECT count(*) FROM synchronous_rows') == [['5']]
        for number, protocol in ((6, 'explicit'), (7, 'extended')):
            connection = Connection(server.port)
            configure(server, 'FIRST 1 (peer)')
            if protocol == 'explicit':
                connection.query('BEGIN')
                connection.query(f'INSERT INTO synchronous_rows VALUES ({number})')
                connection.send(b'Q', b'COMMIT\0')
            else:
                connection.socket.sendall(
                    fixture['parse'](f'INSERT INTO synchronous_rows VALUES ({number})')
                    + fixture['bind']()
                    + fixture['frame'](b'E', b'\0' + struct.pack('!I', 0))
                    + fixture['frame'](b'H', b''))
                assert connection.receive() == (b'1', b'')
                assert connection.receive() == (b'2', b'')
                assert connection.receive() == (b'C', b'INSERT 0 1\0')
                connection.send(b'S', b'')
            wait_for_commit(server)
            assert not select.select([connection.socket], [], [], 0)[0], protocol
            acknowledge(server)
            if protocol == 'explicit':
                assert connection.receive() == (b'C', b'COMMIT\0')
            assert connection.receive() == (b'Z', b'I')
            configure(server, '')
            connection.close()
            connection = None
            print(f'synchronous-commit: {protocol} PASS', flush=True)
        assert witness.query('SELECT count(*) FROM synchronous_rows') == [['7']]
    finally:
        if connection is not None:
            connection.close()
        if witness is not None:
            witness.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()
