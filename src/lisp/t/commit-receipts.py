"""Check postcommit receipt failures in the owning SQL server's live image."""
from pathlib import Path
import runpy


api = runpy.run_path(str(Path(__file__).with_name('synchronous-ddl.py')))
OwnedLisp, Connection = api['OwnedLisp'], api['Connection']
expect_closed = api['expect_closed']


def instrumentation(case, pid):
    target = f'(and *backend* (= (backend-pid *backend*) {pid}))'
    if case == 'xid-only':
        return ('(setf (symbol-function \'wal-commit-transaction) '
                '(lambda (&rest arguments) '
                '(multiple-value-bind (end receipt) (apply *receipt-test-wal* arguments) '
                f'(when {target} '
                '(let ((source (nth-value 0 (sync-replication-configuration-state)))) '
                '(assert (eql source (wal-commit-receipt-source-id receipt)))) '
                '(assert (null (backend-wal-obligation *backend*)))) '
                '(values end receipt))))')
    if case in ('error', 'nonlocal'):
        exit_form = '(error "injected receipt validation failure")' if case == 'error' else (
            '(sb-thread:return-from-thread :receipt-validation-exit)')
        return ('(setf (symbol-function \'validate-wal-commit-receipt) '
                '(lambda (&rest arguments) '
                f'(when {target} '
                '(setf *receipt-test-xid* (wal-commit-receipt-xid (first arguments)) '
                '*receipt-test-backend* *backend*) '
                f'{exit_form}) '
                '(apply *receipt-test-validate* arguments)))')
    if case in ('error-before-return', 'nonlocal-before-return'):
        exit_form = '(error "injected acceptance tail failure")' if case == 'error-before-return' else (
            '(sb-thread:return-from-thread :acceptance-tail-exit)')
        return ('(setf (symbol-function \'wal-commit-transaction) '
                '(lambda (&rest arguments) '
                '(multiple-value-bind (end receipt) (apply *receipt-test-wal* arguments) '
                f'(when {target} '
                '(setf *receipt-test-xid* (first arguments) *receipt-test-backend* *backend*) '
                f'{exit_form}) (values end receipt))))')
    if case == 'missing-durable':
        return ('(setf (symbol-function \'wal-commit-transaction) '
                '(lambda (&rest arguments) '
                '(let ((adjusted (copy-list arguments))) '
                f'(when {target} '
                '(setf *receipt-test-xid* (first arguments) *receipt-test-backend* *backend* '
                '(getf (cddr adjusted) :terminal-durable-function) nil)) '
                '(apply *receipt-test-wal* adjusted))))')
    replacement = 'nil' if case == 'missing' else (
        '(%make-wal-commit-receipt (wal-commit-receipt-xid receipt) :wrong-source '
        '(wal-commit-receipt-terminal-end receipt) (wal-commit-receipt-wal-durable-p receipt))')
    return ('(setf (symbol-function \'wal-commit-transaction) '
            '(lambda (&rest arguments) '
            '(multiple-value-bind (end receipt) (apply *receipt-test-wal* arguments) '
            f'(if {target} '
            '(progn (setf *receipt-test-xid* (first arguments) *receipt-test-backend* *backend*) '
            f'(values end {replacement})) '
            '(values end receipt)))))')


def instrument_pending_clog(server, pid):
    server.control(
        '(setf *receipt-test-staged* nil '
        '(symbol-function \'clog-stage-commit-under-wal-lock) '
        '(lambda (xid end) '
        '(let ((tag (funcall *receipt-test-stage* xid end))) '
        f'(when (and *backend* (= (backend-pid *backend*) {pid})) '
        '(assert (sb-thread:holding-mutex-p *wal-lock*)) '
        '(assert (<= end *wal-durable-lsn*)) '
        '(assert (= +clog-committed+ (clog-get xid))) '
        '(sb-thread:with-mutex (*clog-lock*) '
        '(assert (= +clog-in-progress+ (clog-read-status-under-lock xid)))) '
        '(setf *receipt-test-staged* tag)) tag)))')


def check_pending_clog(server):
    server.control('(assert *receipt-test-staged*) '
                   '(sb-thread:with-mutex (*clog-lock*) '
                   '(assert (zerop (hash-table-count *clog-pending-commits*))))')


def restore(server):
    server.control('(setf (symbol-function \'wal-commit-transaction) *receipt-test-wal* '
                   '(symbol-function \'validate-wal-commit-receipt) *receipt-test-validate* '
                   '(symbol-function \'clog-stage-commit-under-wal-lock) *receipt-test-stage*)')


def run():
    server = OwnedLisp()
    witness = actor = None
    try:
        server.start()
        witness = Connection(server.port)
        witness.query('CREATE TABLE receipt_rows (id int PRIMARY KEY)')
        server.control('(defvar *receipt-test-wal* (symbol-function \'wal-commit-transaction))')
        server.control('(defvar *receipt-test-validate* (symbol-function \'validate-wal-commit-receipt))')
        server.control('(defvar *receipt-test-stage* (symbol-function \'clog-stage-commit-under-wal-lock))')
        server.control('(defvar *receipt-test-staged* nil)')
        server.control('(defvar *receipt-test-xid* nil)')
        server.control('(defvar *receipt-test-backend* nil)')
        cases = ('missing', 'wrong-source', 'missing-durable', 'error', 'nonlocal',
                 'error-before-return', 'nonlocal-before-return')
        for number, case in enumerate(cases, 1):
            actor = Connection(server.port)
            pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
            try:
                instrument_pending_clog(server, pid)
                server.control(instrumentation(case, pid))
                actor.send(b'Q', f'INSERT INTO receipt_rows VALUES ({number})'.encode() + b'\0')
                expect_closed(actor)
            finally:
                restore(server)
                actor.close()
                actor = None
            assert witness.query('SELECT count(*) FROM receipt_rows') == [[str(number)]]
            check_pending_clog(server)
            server.control('(assert (not *wal-poisoned*)) '
                           '(assert (not (member *receipt-test-xid* *active-xids*))) '
                           '(assert (backend-frontend-output-suppressed-reason *receipt-test-backend*)) '
                           f'(assert (null (lookup-backend {pid})))')
            print(f'commit-receipts: {case} closes without output after committed cleanup PASS', flush=True)
        actor = Connection(server.port)
        actor.query('BEGIN')
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        server.control(f'(let ((*backend* (lookup-backend {pid}))) (assign-xid *backend*))')
        try:
            instrument_pending_clog(server, pid)
            server.control(instrumentation('xid-only', pid))
            actor.query('COMMIT')
        finally:
            restore(server)
        assert actor.query('SELECT 1') == [['1']]
        check_pending_clog(server)
        print('commit-receipts: XID-only commit completes with its source receipt PASS', flush=True)
    finally:
        if actor is not None:
            actor.close()
        if witness is not None:
            witness.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()
