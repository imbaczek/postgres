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


def stage_maintenance_predecessor(server, oid):
    # This private operation returns no transaction acceptance receipt.
    server.control(
        '(defvar *queue-test-node* (let ((*backend* (make-backend))) '
        f'(pg-class-relfilenode (find-table-by-oid {oid})))) '
        '(defvar *queue-test-prior* nil) (defvar *queue-test-seen* nil) '
        '(defvar *queue-test-pause* (symbol-function \'wal-test-pause)) '
        '(with-wal-publication-fence (begin-publication) '
        '(wal-require-no-pending-storage) (begin-publication) '
        '(wal-dirty-install-page (wal-page-key *queue-test-node* 0) '
        '(wal-copy-dirty-page (wal-visible-page-under-lock *queue-test-node* 0 :main))) '
        '(let ((keys (wal-primary-page-keys))) '
        '(with-wal-primary-page-spool (spool owner) '
        '(wal-primary-record-page-xids keys (wal-append-batch-start)) '
        '(wal-primary-append-pages keys spool) (wal-primary-seal-page-spool spool) '
        '(wal-prepare-page-publication-under-lock owner 0 nil nil :kind :maintenance) '
        '(wal-publish-page-publication-under-lock owner '
        '(wal-append-record +wal-record-maintenance+ 0 '
        '(make-array 0 :element-type \'(unsigned-byte 8)))) '
        '(setf *queue-test-prior* owner) (wal-flush))))')


def run_queued_predecessor_case(maintenance=False, reset=False):
    server = OwnedLisp()
    actor = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE queued_prefix_rows (id int PRIMARY KEY)')
        actor.query('INSERT INTO queued_prefix_rows VALUES (1)')
        oid = int(actor.query("SELECT oid FROM pg_class WHERE relname = 'queued_prefix_rows'")[0][0])
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        if not maintenance:
            actor.query('BEGIN')
            if not reset:
                actor.query('INSERT INTO queued_prefix_rows VALUES (2)')
        stage_maintenance_predecessor(server, oid)
        kind = ':maintenance' if maintenance else ':transaction'
        if reset:
            server.control(
                '(defvar *queue-test-publish* '
                '(symbol-function \'publish-transaction-replacements-under-wal-lock)) '
                '(setf (symbol-function \'publish-transaction-replacements-under-wal-lock) '
                '(lambda (backend &optional direct-p) '
                f'(when (and backend (= (backend-pid backend) {pid})) '
                '(assert (eq :released (wal-page-publication-state *queue-test-prior*))) '
                '(assert (null *wal-pending-page-publication*))) '
                '(funcall *queue-test-publish* backend direct-p)))')
        inspect = (
            '(assert (eq :released (wal-page-publication-state *queue-test-prior*))) '
            '(assert (eq *wal-pending-page-publication* *wal-pending-page-publication-tail*)) '
            '(assert (wal-page-publication-resets *wal-pending-page-publication*)) '
            if reset else
            '(assert (eq *queue-test-prior* *wal-pending-page-publication*)) '
            '(let ((current (wal-page-publication-next *queue-test-prior*))) '
            '(assert current) (assert (eq current *wal-pending-page-publication-tail*)) '
            '(assert (null (wal-page-publication-next current))) '
            f'(assert (eq {kind} (wal-page-publication-kind current)))) ')
        server.control(
            '(setf (symbol-function \'wal-test-pause) (lambda (point) '
            f'(when (and *backend* (= (backend-pid *backend*) {pid}) '
            '(eq point :after-page-publication-transfer) (not *queue-test-seen*)) '
            f'{inspect}'
            '(setf *queue-test-seen* t)) (funcall *queue-test-pause* point)))')
        if maintenance:
            actor.query('VACUUM queued_prefix_rows')
            expected = [['1']]
        elif reset:
            actor.query('TRUNCATE queued_prefix_rows')
            actor.query('INSERT INTO queued_prefix_rows VALUES (9)')
            actor.query('COMMIT')
            expected = [['9']]
        else:
            actor.query('INSERT INTO queued_prefix_rows VALUES (3)')
            actor.query('COMMIT')
            expected = [['1'], ['2'], ['3']]
        server.control('(assert *queue-test-seen*) '
                       '(assert (eq :released (wal-page-publication-state *queue-test-prior*))) '
                       '(assert (null *wal-pending-page-publication*)) '
                       '(assert (null *wal-pending-page-publication-tail*)) '
                       '(assert (not *wal-poisoned*))')
        assert actor.query('SELECT id FROM queued_prefix_rows ORDER BY id') == expected
        actor.close()
        actor = None
        server.stop()
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM queued_prefix_rows ORDER BY id') == expected
        print(f'pending-pages: maintenance={maintenance} reset={reset} real publisher drains its predecessor PASS',
              flush=True)
    finally:
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def run_synchronous_barrier_case(operation):
    server = OwnedLisp()
    actor = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE queued_barrier_rows (id int PRIMARY KEY)')
        actor.query('INSERT INTO queued_barrier_rows VALUES (1)')
        oid = int(actor.query("SELECT oid FROM pg_class WHERE relname = 'queued_barrier_rows'")[0][0])
        if operation == 'clog-reset':
            server.control('(vacuum-all-relations-for-xid-wraparound *next-xid* '
                           ':fsync-function #\'smgr-fsync)')
        stage_maintenance_predecessor(server, oid)
        hook = {'recycling': 'wal-prepare-slot-recycling-under-lock',
                'namespace': 'wal-namespace-migration-plan'}.get(operation, 'wal-append-batch-start')
        server.control(
            '(defvar *queue-barrier-seen* nil) '
            f'(defvar *queue-barrier-batch* (symbol-function \'{hook})) '
            f'(setf (symbol-function \'{hook}) (lambda (&rest arguments) '
            '(unless *queue-barrier-seen* '
            '(assert (eq :released (wal-page-publication-state *queue-test-prior*))) '
            '(assert (null *wal-pending-page-publication*)) '
            '(assert (null *wal-pending-page-publication-tail*)) '
            '(setf *queue-barrier-seen* t)) (apply *queue-barrier-batch* arguments)))')
        if operation == 'catalog':
            actor.query('CREATE TABLE after_queued_prefix (id int)')
        elif operation == 'checkpoint':
            server.control('(wal-checkpoint-core)')
        elif operation == 'clog-reset':
            server.control('(clog-reset)')
        elif operation == 'xid-reservation':
            server.control(
                '(sb-thread:with-mutex (*wal-lock*) '
                '(sb-thread:with-mutex (*xid-lock*) (assert (null *active-xids*)) '
                '(setf *next-xid* *xid-batch-end*))) '
                '(defvar *queue-settlement-outside-xid* nil) '
                '(defvar *queue-barrier-flush* (symbol-function \'wal-flush-core)) '
                '(setf (symbol-function \'wal-flush-core) (lambda () '
                '(when *wal-pending-page-publication* '
                '(assert (not (sb-thread:holding-mutex-p *xid-lock*))) '
                '(setf *queue-settlement-outside-xid* t)) '
                '(funcall *queue-barrier-flush*)))')
            actor.query('INSERT INTO queued_barrier_rows VALUES (9)')
            server.control('(assert *queue-settlement-outside-xid*)')
        elif operation == 'resources':
            server.control(
                '(let ((current (wal-control-resource-parameters *wal-control-metadata*))) '
                '(wal-report-resource-parameters '
                '(%make-resource-parameters (1+ (resource-parameters-connections current)) '
                '(resource-parameters-workers current) (resource-parameters-senders current) '
                '(resource-parameters-prepared current) (resource-parameters-locks current))))')
        elif operation == 'recycling':
            server.control('(wal-remove-checkpointed-segments *wal-checkpoint-lsn* :wait nil)')
        elif operation == 'namespace':
            server.control(
                '(defvar *queue-migration-error* nil) (defvar *queue-migration-done* nil) '
                '(defvar *queue-migration-worker* (sb-thread:make-thread (lambda () '
                '(handler-case (progn '
                '(let ((deadline (+ (get-internal-real-time) (* 20 internal-time-units-per-second)))) '
                f'(loop until (probe-file #p"{server.root / "migrate"}") '
                'do (when (>= (get-internal-real-time) deadline) (error "migration release timeout")) '
                '(sleep 0.01))) (wal-migrate-timeline-namespace)) '
                '(error (condition) (setf *queue-migration-error* (princ-to-string condition)))) '
                '(setf *queue-migration-done* t)) :name "owned queued namespace migration"))')
            (server.root / 'migrate').touch()
            deadline = time.monotonic() + 20
            while ':DONE T' not in server.control(
                    '(assert (null *queue-migration-error*)) (list :done *queue-migration-done*)'):
                assert server.process.poll() is None
                assert time.monotonic() < deadline, 'namespace migration did not finish'
                time.sleep(0.02)
        elif operation in ('startup-guard', 'state-guard'):
            action = '(wal-mark-primary-startup)' if operation == 'startup-guard' else \
                '(wal-set-primary-state +db-shutdowned+)'
            server.control(
                '(let ((before (recovery-read-bytes (wal-control-path))) (rejected nil)) '
                f'(handler-case {action} '
                '(wal-input-validation-error () (setf rejected t))) '
                '(assert rejected) '
                '(assert (equalp before (recovery-read-bytes (wal-control-path)))) '
                '(assert (eq *queue-test-prior* *wal-pending-page-publication*)) '
                '(assert (not *wal-poisoned*)) (setf *queue-barrier-seen* t)) '
                '(progn (with-wal-critical (wal-settle-page-publications-under-lock)) (values))')
        else:
            raise AssertionError(f'unknown barrier operation {operation}')
        server.control('(assert *queue-barrier-seen*) (assert (not *wal-poisoned*)) '
                       '(assert (null *wal-pending-page-publication*)) '
                       '(assert (null *wal-pending-page-publication-tail*))')
        expected = [['1'], ['9']] if operation == 'xid-reservation' else [['1']]
        assert actor.query('SELECT id FROM queued_barrier_rows ORDER BY id') == expected
        actor.close()
        actor = None
        server.stop()
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM queued_barrier_rows ORDER BY id') == expected
        behavior = 'rejects debt before control publication' if operation.endswith('-guard') else \
            'settles its predecessor before publication'
        print(f'pending-pages: {operation} {behavior} PASS', flush=True)
    finally:
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
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
    run_queued_predecessor_case()
    run_queued_predecessor_case(maintenance=True)
    run_queued_predecessor_case(reset=True)
    for operation in ('catalog', 'checkpoint', 'clog-reset', 'xid-reservation', 'resources',
                      'recycling', 'namespace', 'startup-guard', 'state-guard'):
        run_synchronous_barrier_case(operation)
