"""Inspect pending PAGE ownership in the server, then recover after failure."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import runpy
import signal
import time


api = runpy.run_path(str(Path(__file__).with_name('synchronous-ddl.py')))
OwnedLisp, Connection, expect_closed = api['OwnedLisp'], api['Connection'], api['expect_closed']


def install_boundary(server, pid, phase, crash):
    server.control(
        '(defvar *page-test-pause* (symbol-function \'wal-test-pause)) '
        '(defvar *page-test-close* (symbol-function \'close-logical-spool)) '
        '(defvar *page-test-write* (symbol-function \'smgr-write)) '
        '(defvar *page-test-stop* (symbol-function \'wal-fail-stop)) '
        '(defvar *page-test-owner* nil) (defvar *page-test-paused* nil) '
        '(setf *wal-dirty-resident-limit-pages* 0 *logical-capture-memory-bytes* 0)')
    release = (
        '(let ((deadline (+ (get-internal-real-time) (* 20 internal-time-units-per-second)))) '
        f'(loop until (probe-file #p"{server.root / "continue"}") '
        'do (when (>= (get-internal-real-time) deadline) (error "PAGE release timeout")) '
        '(sleep 0.01)))')
    selected = f'(and *backend* (= (backend-pid *backend*) {pid}))'
    if phase == 'apply':
        action = f'(setf *page-test-paused* t) {release}' if crash else '(error "injected PAGE write failure")'
        server.control(
            '(setf (symbol-function \'smgr-write) '
            '(lambda (node block page &optional (fork :main)) '
            '(funcall *page-test-write* node block page fork) '
            f'(when (and {selected} *wal-pending-page-publication* (not *page-test-owner*)) '
            '(setf *page-test-owner* *wal-pending-page-publication*) '
            f'{action})))')
    elif phase == 'close':
        server.control(
            '(setf (symbol-function \'close-logical-spool) '
            '(lambda (spool) (funcall *page-test-close* spool) '
            f'(when (and {selected} *wal-pending-page-publication* '
            '(eq spool (wal-page-publication-spool *wal-pending-page-publication*))) '
            '(setf *page-test-owner* *wal-pending-page-publication*) '
            '(error "injected PAGE close failure"))))')
    else:
        point = {'transfer': ':after-page-publication-transfer',
                 'reset': ':after-relation-resets'}.get(phase, ':after-wal-flush')
        action = f'(setf *page-test-paused* t) {release}' if crash else '(error "injected PAGE publication failure")'
        server.control(
            '(setf (symbol-function \'wal-test-pause) (lambda (point) '
            f'(when (and {selected} (eq point {point})) '
            '(setf *page-test-owner* *wal-pending-page-publication*) '
            f'{action}) (funcall *page-test-pause* point)))')
    if not crash:
        server.control(
            '(setf (symbol-function \'wal-fail-stop) (lambda (condition) '
            f'(when (and {selected} (not *page-test-paused*)) '
            '(assert (not (sb-thread:holding-mutex-p *wal-lock*))) '
            f'(setf *page-test-paused* t) {release}) '
            '(funcall *page-test-stop* condition)))')


def run_case(phase, crash=False):
    server = OwnedLisp()
    actor = witness = None
    try:
        server.start()
        witness = Connection(server.port)
        witness.query('CREATE TABLE pending_page_rows (id int PRIMARY KEY)')
        if phase == 'reset':
            witness.query('INSERT INTO pending_page_rows VALUES (3)')
        actor = Connection(server.port)
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        install_boundary(server, pid, phase, crash)
        if phase == 'reset':
            actor.query('BEGIN')
            actor.query('TRUNCATE pending_page_rows')
            actor.query('INSERT INTO pending_page_rows VALUES (1)')
            actor.send(b'Q', b'COMMIT\0')
        else:
            witness.query('BEGIN')
            witness.query('INSERT INTO pending_page_rows VALUES (2)')
            server.control('(defvar *page-test-other-xid* '
                           '(sb-thread:with-mutex (*xid-lock*) '
                           '(assert (= 1 (length *active-xids*))) (first *active-xids*)))')
            actor.send(b'Q', b'INSERT INTO pending_page_rows VALUES (1)\0')
        deadline = time.monotonic() + 15
        while ':PAUSED T' not in server.control('(list :paused *page-test-paused*)'):
            assert server.process.poll() is None, 'server exited before PAGE inspection'
            assert time.monotonic() < deadline, 'PAGE boundary was not reached'
            time.sleep(0.02)
        # At a crash boundary the actor holds WAL and waits only on our file.
        # At a fail-stop boundary it has unwound WAL. Neither can mutate the owner.
        server.control(
            '(assert (eq *page-test-owner* *wal-pending-page-publication*)) '
            '(assert (plusp (hash-table-count (wal-page-publication-pages *page-test-owner*)))) '
            '(assert (zerop (hash-table-count *wal-page-cache*))) '
            '(assert (eq *wal-storage-incarnation* (wal-page-publication-incarnation *page-test-owner*))) '
            '(assert (= *wal-insert-timeline* (wal-page-publication-timeline *page-test-owner*))) '
            f'(assert (eq :{ "applied" if phase == "close" else "pending"} '
            '(wal-page-publication-state *page-test-owner*))) '
            f'(assert (eql {"t" if phase == "close" else "nil"} '
            '(logical-spool-closed-p (wal-page-publication-spool *page-test-owner*)))) '
            '(assert (plusp (wal-dirty-store-disk-refs *wal-dirty-store*))) '
            + ('' if phase == 'reset' else
               '(sb-thread:with-mutex (*xid-lock*) '
               '(assert (member *page-test-other-xid* *active-xids*)) '
               '(assert (gethash *page-test-other-xid* *wal-xid-first-lsn*))) ')
            + ('' if phase == 'transfer' else
               '(assert (<= (wal-page-publication-terminal-end *page-test-owner*) *wal-durable-lsn*)) '))
        if crash:
            process = server.process
            server.stop(immediate=True)
            assert process.returncode == -signal.SIGKILL, process.returncode
        else:
            (server.root / 'continue').touch()
        expect_closed(actor)
        actor.close()
        actor = None
        if not crash:
            server.process.wait(timeout=20)
        witness.close()
        witness = None
        server.stop()
        server.start()
        witness = Connection(server.port)
        rows = witness.query('SELECT id FROM pending_page_rows')
        if phase == 'transfer':
            # A complete, unflushed terminal may survive process death.
            assert rows in ([], [['1']]), rows
        else:
            assert rows == [['1']], rows
        assert witness.query('SELECT id FROM pending_page_rows WHERE id = 2') == []
        server.control('(assert (not *wal-poisoned*)) '
                       '(assert (null *wal-pending-page-publication*))')
        print(f'pending-pages: phase={phase} crash={crash} ownership and recovery PASS', flush=True)
    finally:
        (server.root / 'continue').touch()
        if actor is not None:
            actor.close()
        if witness is not None:
            witness.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


def run_contributor_case():
    server = OwnedLisp()
    actor = contributor = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE pending_contributors (id int PRIMARY KEY)')
        contributor = Connection(server.port)
        contributor.query('BEGIN')
        contributor.query('INSERT INTO pending_contributors VALUES (2)')
        server.control('(defvar *page-test-other-xid* '
                       '(sb-thread:with-mutex (*xid-lock*) '
                       '(assert (= 1 (length *active-xids*))) (first *active-xids*)))')
        actor.query('INSERT INTO pending_contributors VALUES (1)')
        server.control(
            '(assert (null *wal-pending-page-publication*)) '
            '(defvar *page-test-other-floor* '
            '(sb-thread:with-mutex (*xid-lock*) '
            '(assert (member *page-test-other-xid* *active-xids*)) '
            '(or (gethash *page-test-other-xid* *wal-xid-first-lsn*) '
            '(error "other contributor lost its WAL floor")))) '
            '(wal-checkpoint-core) '
            '(sb-thread:with-mutex (*xid-lock*) '
            '(assert (= *page-test-other-floor* '
            '(gethash *page-test-other-xid* *wal-xid-first-lsn*))))')
        assert actor.query('SELECT id FROM pending_contributors') == [['1']]
        contributor.query('COMMIT')
        assert actor.query('SELECT id FROM pending_contributors ORDER BY id') == [['1'], ['2']]
        server.control('(assert (null *wal-pending-page-publication*)) '
                       '(sb-thread:with-mutex (*xid-lock*) '
                       '(assert (null (gethash *page-test-other-xid* *wal-xid-first-lsn*))))')
        contributor.close()
        contributor = None
        actor.close()
        actor = None
        server.stop()
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM pending_contributors ORDER BY id') == [['1'], ['2']]
        print('pending-pages: later contributor retained through drain and checkpoint PASS', flush=True)
    finally:
        if contributor is not None:
            contributor.close()
        if actor is not None:
            actor.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


def run_competing_commit_case():
    server = OwnedLisp()
    actor = competitor = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE competing_commits (id int PRIMARY KEY)')
        competitor = Connection(server.port)
        competitor.query('BEGIN')
        competitor.query('INSERT INTO competing_commits VALUES (2)')
        actor_pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        competitor_pid = int(competitor.query('SELECT pg_backend_pid()')[0][0])
        install_boundary(server, actor_pid, 'transfer', crash=True)
        server.control(
            '(defvar *page-test-commit* (symbol-function \'wal-commit-transaction)) '
            '(defvar *page-test-competing-entered* nil) '
            '(setf (symbol-function \'wal-commit-transaction) '
            '(lambda (&rest arguments) '
            f'(when (and *backend* (= (backend-pid *backend*) {competitor_pid})) '
            '(setf *page-test-competing-entered* t)) '
            '(apply *page-test-commit* arguments)))')
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(actor.query, 'INSERT INTO competing_commits VALUES (1)')
            deadline = time.monotonic() + 15
            while ':PAUSED T' not in server.control('(list :paused *page-test-paused*)'):
                assert server.process.poll() is None
                assert time.monotonic() < deadline, 'first commit did not transfer ownership'
                time.sleep(0.02)
            second = pool.submit(competitor.query, 'COMMIT')
            while ':ENTERED T' not in server.control('(list :entered *page-test-competing-entered*)'):
                assert server.process.poll() is None
                assert time.monotonic() < deadline, 'competing commit did not enter publication'
                time.sleep(0.02)
            (server.root / 'continue').touch()
            first.result(timeout=15)
            second.result(timeout=15)
        assert actor.query('SELECT id FROM competing_commits ORDER BY id') == [['1'], ['2']]
        server.control('(assert (not *wal-poisoned*)) '
                       '(assert (null *wal-pending-page-publication*))')
        print('pending-pages: competing commit waits for the current page owner PASS', flush=True)
    finally:
        (server.root / 'continue').touch()
        if actor is not None:
            actor.close()
        if competitor is not None:
            competitor.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run_case('transfer')
    run_case('flush')
    run_case('close')
    run_case('apply')
    run_case('flush', crash=True)
    run_case('apply', crash=True)
    run_case('reset', crash=True)
    run_contributor_case()
    run_competing_commit_case()
