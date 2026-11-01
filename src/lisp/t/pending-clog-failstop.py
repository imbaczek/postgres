"""Keep failed CLOG debt until fail-stop, then recover its flushed commit."""
from pathlib import Path
import re
import runpy
import signal
import time


api = runpy.run_path(str(Path(__file__).with_name('synchronous-ddl.py')))
OwnedLisp, Connection, expect_closed = api['OwnedLisp'], api['Connection'], api['expect_closed']


def install_failure(server, pid, after_write):
    write = ('(funcall *clog-failure-write* xid status :fsync fsync) ' if after_write else '')
    server.control(
        '(defvar *clog-failure-write* (symbol-function \'clog-write-status-under-lock)) '
        '(defvar *clog-failure-stop* (symbol-function \'wal-fail-stop)) '
        '(defvar *clog-failure-xid* nil) (defvar *clog-failure-tag* nil) '
        '(defvar *clog-failure-paused* nil)')
    server.control(
        '(setf (symbol-function \'clog-write-status-under-lock) '
        '(lambda (xid status &key fsync) '
        f'(if (and *backend* (= (backend-pid *backend*) {pid}) (= status +clog-committed+)) '
        '(progn (setf *clog-failure-xid* xid '
        '*clog-failure-tag* (clog-commit-completion-tag '
        '(gethash (xid32-of xid) *clog-pending-commits*))) '
        f'{write}(error "injected pending CLOG write failure")) '
        '(funcall *clog-failure-write* xid status :fsync fsync))) '
        '(symbol-function \'wal-fail-stop) (lambda (condition) '
        f'(when (and *backend* (= (backend-pid *backend*) {pid}) (not *clog-failure-paused*)) '
        '(assert (not (sb-thread:holding-mutex-p *wal-lock*))) '
        '(assert (not (sb-thread:holding-mutex-p *clog-lock*))) '
        '(setf *clog-failure-paused* t) '
        '(let ((deadline (+ (get-internal-real-time) (* 15 internal-time-units-per-second)))) '
        f'(loop until (probe-file #p"{server.root / "continue"}") '
        'do (when (>= (get-internal-real-time) deadline) (error "CLOG failure release timeout")) '
        '(sleep 0.01)))) (funcall *clog-failure-stop* condition)))')


def run_case(after_write):
    server = OwnedLisp()
    actor = witness = None
    try:
        server.start()
        witness = Connection(server.port)
        witness.query('CREATE TABLE pending_clog_rows (id int PRIMARY KEY)')
        actor = Connection(server.port)
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        install_failure(server, pid, after_write)
        actor.send(b'Q', b'INSERT INTO pending_clog_rows VALUES (1)\0')
        deadline = time.monotonic() + 10
        while ':PAUSED T' not in server.control('(list :paused *clog-failure-paused*)'):
            assert server.process.poll() is None, 'server exited before debt inspection'
            assert time.monotonic() < deadline, 'CLOG failure boundary was not reached'
            time.sleep(0.02)
        server.control(
            '(assert *wal-poisoned*) '
            '(assert (= +clog-committed+ (clog-get *clog-failure-xid*))) '
            '(sb-thread:with-mutex (*wal-lock*) (sb-thread:with-mutex (*clog-lock*) '
            '(assert (eq *clog-failure-tag* '
            '(clog-commit-completion-tag '
            '(gethash (xid32-of *clog-failure-xid*) *clog-pending-commits*)))) '
            '(assert (eq *wal-storage-incarnation* (clog-commit-tag-incarnation *clog-failure-tag*))) '
            '(assert (= *wal-insert-timeline* (clog-commit-tag-timeline *clog-failure-tag*))) '
            '(assert (<= (clog-commit-tag-terminal-end *clog-failure-tag*) *wal-durable-lsn*)) '
            f'(assert (= {1 if after_write else 0} '
            '(clog-read-status-under-lock *clog-failure-xid*)))))')
        (server.root / 'continue').touch()
        expect_closed(actor)
        actor.close()
        actor = None
        server.process.wait(timeout=15)
        witness.close()
        witness = None
        server.stop()
        server.start()
        witness = Connection(server.port)
        assert witness.query('SELECT id FROM pending_clog_rows') == [['1']]
        server.control('(assert (not *wal-poisoned*)) '
                       '(sb-thread:with-mutex (*clog-lock*) '
                       '(assert (zerop (hash-table-count *clog-pending-commits*))))')
        print(f'pending-clog: after_write={after_write} retained debt, closed output and recovered commit PASS',
              flush=True)
    finally:
        (server.root / 'continue').touch()
        if actor is not None:
            actor.close()
        if witness is not None:
            witness.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


def run_crash_case():
    server = OwnedLisp()
    actor = witness = None
    try:
        server.start()
        witness = Connection(server.port)
        witness.query('CREATE TABLE pending_clog_rows (id int PRIMARY KEY)')
        actor = Connection(server.port)
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        server.control('(defvar *clog-crash-stage* (symbol-function \'clog-stage-commit-under-wal-lock)) '
                       '(defvar *clog-crash-tag* nil) (defvar *clog-crash-xid* nil)')
        server.control(
            '(setf (symbol-function \'clog-stage-commit-under-wal-lock) '
            '(lambda (xid end) (let ((tag (funcall *clog-crash-stage* xid end))) '
            f'(when (and *backend* (= (backend-pid *backend*) {pid})) '
            '(assert (sb-thread:holding-mutex-p *wal-lock*)) '
            '(setf *clog-crash-xid* xid *clog-crash-tag* tag) '
            '(let ((deadline (+ (get-internal-real-time) (* 15 internal-time-units-per-second)))) '
            f'(loop until (probe-file #p"{server.root / "continue"}") '
            'do (when (>= (get-internal-real-time) deadline) (error "CLOG crash release timeout")) '
            '(sleep 0.01)))) tag)))')
        actor.send(b'Q', b'INSERT INTO pending_clog_rows VALUES (1)\0')
        deadline = time.monotonic() + 10
        while ':STAGED T' not in server.control('(list :staged (not (null *clog-crash-tag*)))'):
            assert server.process.poll() is None, 'server exited before staged crash boundary'
            assert time.monotonic() < deadline, 'CLOG stage boundary was not reached'
            time.sleep(0.02)
        # The backend intentionally owns WAL here. Inspection takes only CLOG.
        server.control(
            '(assert (not *wal-poisoned*)) '
            '(assert (= +clog-committed+ (clog-get *clog-crash-xid*))) '
            '(sb-thread:with-mutex (*clog-lock*) '
            '(assert (eq *clog-crash-tag* (clog-commit-completion-tag '
            '(gethash (xid32-of *clog-crash-xid*) *clog-pending-commits*)))) '
            '(assert (<= (clog-commit-tag-terminal-end *clog-crash-tag*) *wal-durable-lsn*)) '
            '(assert (= +clog-in-progress+ (clog-read-status-under-lock *clog-crash-xid*))))')
        process = server.process
        server.stop(immediate=True)
        assert process.returncode == -signal.SIGKILL, process.returncode
        expect_closed(actor)
        actor.close()
        actor = None
        witness.close()
        witness = None
        server.start()
        witness = Connection(server.port)
        assert witness.query('SELECT id FROM pending_clog_rows') == [['1']]
        server.control('(assert (not *wal-poisoned*)) '
                       '(sb-thread:with-mutex (*clog-lock*) '
                       '(assert (zerop (hash-table-count *clog-pending-commits*))))')
        print('pending-clog: SIGKILL after stage recovered the flushed commit PASS', flush=True)
    finally:
        (server.root / 'continue').touch()
        if actor is not None:
            actor.close()
        if witness is not None:
            witness.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


def run_reset_crash_case():
    server = OwnedLisp()
    witness = None
    try:
        server.start()
        witness = Connection(server.port)
        witness.query('CREATE TABLE reset_clog_rows (id int PRIMARY KEY)')
        witness.query('INSERT INTO reset_clog_rows VALUES (1)')
        output = server.control(
            '(defvar *clog-reset-old-xid* (1- *next-xid*)) '
            '(assert (= +clog-committed+ (clog-get *clog-reset-old-xid*))) '
            '(assert (null *active-xids*)) '
            '(vacuum-all-relations-for-xid-wraparound *next-xid* :fsync-function #\'smgr-fsync) '
            '(list :reset-xid *clog-reset-old-xid*)')
        match = re.search(r':RESET-XID\s+(\d+)', output)
        assert match, output
        old_xid = int(match.group(1))
        assert witness.query('SELECT id FROM reset_clog_rows') == [['1']]
        server.control(
            '(defvar *clog-reset-file* (symbol-function \'clog-reset-file-under-wal-lock)) '
            '(defvar *clog-reset-boundary* nil) (defvar *clog-reset-error* nil) '
            '(setf (symbol-function \'clog-reset-file-under-wal-lock) '
            '(lambda (sync directory-sync) '
            '(assert (sb-thread:holding-mutex-p *wal-lock*)) '
            '(setf *clog-reset-boundary* *wal-durable-lsn*) '
            '(let ((deadline (+ (get-internal-real-time) (* 15 internal-time-units-per-second)))) '
            f'(loop until (probe-file #p"{server.root / "continue"}") '
            'do (when (>= (get-internal-real-time) deadline) (error "CLOG reset release timeout")) '
            '(sleep 0.01))) (funcall *clog-reset-file* sync directory-sync))) '
            '(defvar *clog-reset-worker* '
            '(sb-thread:make-thread '
            '(lambda () (handler-case (with-live-operation (clog-reset)) '
            '(error (condition) (setf *clog-reset-error* (princ-to-string condition))))) '
            ':name "owned CLOG reset crash boundary"))')
        deadline = time.monotonic() + 10
        while ':PAUSED T' not in server.control(
                '(assert (null *clog-reset-error*)) (list :paused (not (null *clog-reset-boundary*)))'):
            assert server.process.poll() is None, 'server exited before reset crash boundary'
            assert time.monotonic() < deadline, 'CLOG reset boundary was not reached'
            time.sleep(0.02)
        server.control(
            '(assert (not *wal-poisoned*)) '
            '(assert (= *clog-reset-boundary* *wal-durable-lsn*)) '
            f'(assert (= +clog-committed+ (clog-get {old_xid})))')
        process = server.process
        server.stop(immediate=True)
        assert process.returncode == -signal.SIGKILL, process.returncode
        witness.close()
        witness = None
        server.start()
        witness = Connection(server.port)
        assert witness.query('SELECT id FROM reset_clog_rows') == [['1']]
        server.control(
            '(assert (not *wal-poisoned*)) '
            f'(assert (= +clog-in-progress+ (clog-get {old_xid}))) '
            '(sb-thread:with-mutex (*clog-lock*) '
            '(assert (zerop (hash-table-count *clog-pending-commits*))))')
        print('pending-clog: SIGKILL after reset WAL flush redid CLOG replacement PASS', flush=True)
    finally:
        (server.root / 'continue').touch()
        if witness is not None:
            witness.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    for after_write in (False, True):
        run_case(after_write)
    run_crash_case()
    run_reset_crash_case()
