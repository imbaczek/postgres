"""Verify public asynchronous commit acknowledgment and crash recovery."""
from pathlib import Path
import runpy


api = runpy.run_path(str(Path(__file__).with_name('synchronous-ddl.py')))
OwnedLisp, Connection = api['OwnedLisp'], api['Connection']
expect_closed = api['expect_closed']
barriers = runpy.run_path(str(Path(__file__).with_name('wal-writer-control.py')))
pause_writer, resume_writer = barriers['pause_writer'], barriers['resume_writer']
wait_control = barriers['wait_control']


def run_case(case):
    server = OwnedLisp()
    actor = None
    paused = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE async_rows (id int PRIMARY KEY)')
        actor.query('SET synchronous_commit=off')
        assert actor.query('SHOW synchronous_commit') == [['off']]
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        paused = pause_writer(server, case)
        failure_case = case in ('missing', 'wrong-source', 'nonlocal')
        tail = {
            'missing': '(values end nil)',
            'wrong-source': '(values end (%make-wal-commit-receipt '
                            '(wal-commit-receipt-xid receipt) :wrong-source end nil))',
            'nonlocal': '(sb-thread:return-from-thread :async-acceptance-tail-exit)',
        }.get(case, '(values end receipt)')
        server.control(
            '(defvar *async-test-receipt* nil) '
            '(defvar *async-test-backend* nil) '
            '(defvar *async-test-wal* (symbol-function \'wal-commit-transaction)) '
            '(defvar *async-test-available* (symbol-function \'primary-wal-writer-available-under-lock-p)) '
            '(setf (symbol-function \'wal-commit-transaction) '
            '(lambda (&rest arguments) '
            '(multiple-value-bind (end receipt) (apply *async-test-wal* arguments) '
            '(setf *async-test-receipt* receipt *async-test-backend* *backend*) '
            f'{tail})))')
        if case == 'stop-race':
            # Observe readiness, request stop, then permit logical acceptance.
            server.control(
                '(setf (symbol-function \'primary-wal-writer-available-under-lock-p) '
                '(lambda () (prog1 (funcall *async-test-available*) '
                '(wal-writer-test-start-stop) '
                '(loop until (sb-thread:with-mutex (*primary-wal-writer-lock*) '
                '(primary-wal-writer-stop-p *primary-wal-writer*)) do (sleep 0.001)))))')
        if failure_case:
            actor.send(b'Q', b'INSERT INTO async_rows VALUES (1)\0')
            expect_closed(actor)
        else:
            actor.query('INSERT INTO async_rows VALUES (1)')
        # No SELECT here: a fresh snapshot would synchronously settle the FIFO.
        server.control(
            '(with-wal-critical '
            '(assert *async-test-receipt*) '
            '(assert (null (wal-commit-receipt-wal-durable-p *async-test-receipt*))) '
            '(assert (< *wal-durable-lsn* (wal-commit-receipt-terminal-end *async-test-receipt*))) '
            '(assert (null (backend-xid *async-test-backend*))) '
            '(assert (eq :idle (backend-state *async-test-backend*))) '
            '(let ((xid (wal-commit-receipt-xid *async-test-receipt*))) '
            '(assert (not (member xid *active-xids*))) '
            '(assert (= +clog-committed+ (clog-get xid))) '
            '(sb-thread:with-mutex (*clog-lock*) '
            '(assert (= +clog-in-progress+ (clog-read-status-under-lock xid))))) '
            '(assert *wal-pending-page-publication*))')
        actor.close()
        actor = None
        wait_control(server, f'(list :gone (null (lookup-backend {pid})))', ':GONE T')
        server.control('(with-wal-critical (assert *wal-pending-page-publication*))')
        if failure_case:
            server.control('(assert (backend-frontend-output-suppressed-reason *async-test-backend*)) '
                           '(assert (not *wal-poisoned*))')
        if case == 'crash':
            server.stop(immediate=True)
            resume_writer(server, paused)
            paused = None
        else:
            resume_writer(server, paused)
            paused = None
            if case == 'stop-race':
                wait_control(server,
                             '(list :done (not (sb-thread:thread-alive-p *writer-test-stop-worker*)))',
                             ':DONE T')
                server.control('(assert (null *writer-test-stop-error*)) '
                               '(assert (primary-wal-writer-joined-p *primary-wal-writer*))')
            wait_control(server, '(list :drained (null *wal-pending-page-publication*))', ':DRAINED T')
            server.control(
                '(with-wal-critical '
                '(assert (<= (wal-commit-receipt-terminal-end *async-test-receipt*) *wal-durable-lsn*))) '
                '(sb-thread:with-mutex (*clog-lock*) '
                '(assert (= +clog-committed+ '
                '(clog-read-status-under-lock (wal-commit-receipt-xid *async-test-receipt*)))))')
            server.stop(immediate=True)
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM async_rows') == ([] if case == 'crash' else [['1']])
        response = 'suppressed invalid response' if failure_case else 'acknowledged nondurable commit'
        print(f'async-commit: {response}; disconnect; {case}; restart PASS', flush=True)
    finally:
        resume_writer(server, paused)
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    for scenario in ('drain', 'crash', 'stop-race', 'missing', 'wrong-source', 'nonlocal'):
        run_case(scenario)
