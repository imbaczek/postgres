"""Verify the real primary writer without a foreground snapshot draining its queue."""
from pathlib import Path
import runpy
import signal
import time


api = runpy.run_path(str(Path(__file__).with_name('synchronous-ddl.py')))
OwnedLisp, Connection = api['OwnedLisp'], api['Connection']
barriers = runpy.run_path(str(Path(__file__).with_name('wal-writer-control.py')))
pause_writer, resume_writer = barriers['pause_writer'], barriers['resume_writer']
wait_file, wait_control = barriers['wait_file'], barriers['wait_control']


def run_idle_drain_and_reload():
    server = OwnedLisp()
    actor = None
    paused = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE writer_rows (id int PRIMARY KEY)')
        server.control('(setf *wal-dirty-resident-limit-pages* 0 *logical-capture-memory-bytes* 0)')
        paused = pause_writer(server, 'idle-drain')
        actor.query('BEGIN')
        actor.query('INSERT INTO writer_rows VALUES (1)')
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        server.control(
            '(defvar *writer-witness* *primary-wal-writer*) '
            '(defvar *writer-thread-witness* (primary-wal-writer-thread *primary-wal-writer*)) '
            f'(wal-writer-test-admit-backend {pid}) '
            '(with-wal-critical '
            '(assert (eq *writer-test-owner* *wal-pending-page-publication*)) '
            '(assert (< *wal-durable-lsn* *writer-test-end*)) '
            '(assert (= +clog-in-progress+ (clog-get (backend-xid *writer-test-backend*)))))')
        resume_writer(server, paused)
        paused = None
        # A SELECT could synchronously settle the queue. Observe raw state first.
        wait_control(server, '(list :drained (wal-writer-test-prefix-drained-p))', ':DRAINED T')
        server.control(
            '(with-wal-critical '
            '(assert (= *writer-test-end* (primary-wal-writer-last-drained-end *writer-witness*))) '
            '(assert (logical-spool-closed-p (wal-page-publication-spool *writer-test-owner*)))) '
            '(wal-writer-test-finish-backend)')
        assert actor.query('SELECT id FROM writer_rows') == [['1']]
        reload_started = time.monotonic()
        server.control('(reload)', timeout=120)
        print(f'wal-writer: code reload elapsed_seconds={time.monotonic() - reload_started:.6f}',
              flush=True)
        server.control(
            '(assert (eq *writer-witness* *primary-wal-writer*)) '
            '(assert (eq *writer-thread-witness* (primary-wal-writer-thread *primary-wal-writer*))) '
            '(assert (sb-thread:thread-alive-p *writer-thread-witness*)) '
            '(assert (null (primary-wal-writer-failure *primary-wal-writer*)))')
        assert actor.query('SELECT id FROM writer_rows') == [['1']]
        actor.close()
        actor = None
        process = server.process
        server.stop()
        assert process.returncode == 0, process.returncode
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM writer_rows') == [['1']]
        print('wal-writer: idle thread drained SQL pages/CLOG; reload and restart preserved row PASS', flush=True)
    finally:
        resume_writer(server, paused)
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def run_reset_stop_race():
    server = OwnedLisp()
    paused = None
    release_reset = server.root / 'reset-release'
    reset_ready = server.root / 'reset-ready'
    try:
        server.start()
        paused = pause_writer(server, 'reset-stop')
        server.control(
            '(defvar *writer-before-reset* *primary-wal-writer*) '
            '(defvar *writer-epoch-before-reset* (primary-wal-writer-epoch *primary-wal-writer*)) '
            f'(wal-writer-test-start-reset #p"{reset_ready}" #p"{release_reset}")')
        wait_file(server, reset_ready)
        server.control('(wal-writer-test-start-stop)')
        wait_control(server,
                     '(sb-thread:with-mutex (*primary-wal-writer-lock*) '
                     '(list :stopping (primary-wal-writer-stop-p *writer-before-reset*)))',
                     ':STOPPING T')
        resume_writer(server, paused)
        paused = None
        server.control(
            '(sb-thread:with-mutex (*primary-wal-writer-lock*) '
            '(assert (eq *writer-epoch-before-reset* (primary-wal-writer-epoch *writer-before-reset*))) '
            '(assert (not (primary-wal-writer-joined-p *writer-before-reset*))))')
        release_reset.touch()
        wait_control(server,
                     '(list :done (and (not (sb-thread:thread-alive-p *writer-test-reset-worker*)) '
                     '(not (sb-thread:thread-alive-p *writer-test-stop-worker*))))', ':DONE T')
        server.control(
            '(assert (null *writer-test-reset-error*)) (assert (null *writer-test-stop-error*)) '
            '(assert (eq *writer-before-reset* *primary-wal-writer*)) '
            '(assert (primary-wal-writer-joined-p *writer-before-reset*)) '
            '(assert (eq :stopped (primary-wal-writer-state *writer-before-reset*))) '
            '(assert (not (eq *writer-epoch-before-reset* (primary-wal-writer-epoch *writer-before-reset*)))) '
            '(assert (eq *wal-storage-incarnation* '
            '(wal-writer-epoch-incarnation (primary-wal-writer-epoch *writer-before-reset*)))) '
            '(assert (not *wal-poisoned*)) '
            '(progn (start-primary-wal-writer) (values)) '
            '(assert (= (1+ (primary-wal-writer-generation *writer-before-reset*)) '
            '(primary-wal-writer-generation *primary-wal-writer*)))')
        process = server.process
        server.stop()
        assert process.returncode == 0, process.returncode
        print('wal-writer: in-flight CLOG reset completed before stop/join; replacement started PASS', flush=True)
    finally:
        release_reset.touch()
        resume_writer(server, paused)
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def run_flush_failure(after_flush):
    server = OwnedLisp()
    actor = None
    paused = None
    evidence = server.root / 'writer-failure-fenced'
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE writer_fault_rows (id int PRIMARY KEY)')
        paused = pause_writer(server, 'flush-failure')
        actor.query('BEGIN')
        actor.query('INSERT INTO writer_fault_rows VALUES (1)')
        pid = int(actor.query('SELECT pg_backend_pid()')[0][0])
        server.control(f'(wal-writer-test-admit-backend {pid}) '
                       f'(wal-writer-test-install-flush-failure #p"{evidence}" '
                       f'{"t" if after_flush else "nil"})')
        process = server.process
        resume_writer(server, paused)
        paused = None
        wait_file(server, evidence)
        # Require actual fast shutdown; cleanup must not kill the process to pass.
        process.wait(timeout=15)
        assert process.returncode is not None
        actor.close()
        actor = None
        server.stop()
        server.start()
        actor = Connection(server.port)
        expected = [['1']] if after_flush else []
        assert actor.query('SELECT id FROM writer_fault_rows') == expected
        server.control('(assert (null (primary-wal-writer-failure *primary-wal-writer*)))')
        print(f'wal-writer: flush failure after_flush={after_flush} fenced outside locks; '
              'retained debt and recovered expected prefix PASS', flush=True)
    finally:
        resume_writer(server, paused)
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def run_queued_wakeups(count=12, pressure=False):
    server = OwnedLisp()
    actors = []
    paused = None
    try:
        server.start()
        setup = Connection(server.port)
        try:
            setup.query('CREATE TABLE writer_queue_rows (id int PRIMARY KEY)')
        finally:
            setup.close()
        server.control('(setf *wal-dirty-resident-limit-pages* 0 *logical-capture-memory-bytes* 0)')
        if pressure:
            server.control('(with-wal-critical '
                           '(let ((limits (copy-list *wal-publication-pressure-limits*))) '
                           '(setf (getf limits :owners) 2 *wal-publication-pressure-limits* limits)))')
        paused = pause_writer(server, 'queued-wakeups')
        pids = []
        for value in range(count):
            actor = Connection(server.port)
            actors.append(actor)
            actor.query('BEGIN')
            actor.query(f'INSERT INTO writer_queue_rows VALUES ({value})')
            pids.append(int(actor.query('SELECT pg_backend_pid()')[0][0]))
        server.control(
            '(defvar *writer-pressure-owners* nil) (defvar *writer-pressure-backends* nil) '
            f"(dolist (pid '({' '.join(map(str, pids))})) "
            '(wal-writer-test-admit-backend pid) '
            '(push *writer-test-owner* *writer-pressure-owners*) '
            '(push *writer-test-backend* *writer-pressure-backends*)) '
            '(sb-thread:with-mutex (*wal-lock*) '
            '(dotimes (index 100) (notify-primary-wal-writer-under-lock)) '
            f'(assert (= {count} (length *writer-pressure-owners*))) '
            '(assert (zerop (hash-table-count (wal-page-publication-pages *writer-test-owner*)))) '
            '(assert (= 1 (sb-thread:semaphore-count (primary-wal-writer-wake *primary-wal-writer*)))))')
        if pressure:
            remaining = 1 + (count - 1) % 2
            server.control(
                '(with-wal-critical '
                f'(assert (= {remaining} (getf (wal-publication-pressure-footprint) :owners))) '
                f'(assert (= {count - remaining} (count :released *writer-pressure-owners* '
                ':key #\'wal-page-publication-state))))')
        resume_writer(server, paused)
        paused = None
        wait_control(server, '(list :drained (wal-writer-test-prefix-drained-p))', ':DRAINED T')
        server.control(
            '(dolist (owner *writer-pressure-owners*) '
            '(assert (eq :released (wal-page-publication-state owner)))) '
            '(dolist (backend *writer-pressure-backends*) '
            '(finish-committed-transaction backend (backend-xid backend))) '
            '(assert (null (primary-wal-writer-failure *primary-wal-writer*)))')
        assert actors[0].query('SELECT count(*) FROM writer_queue_rows') == [[str(count)]]
        for actor in actors:
            actor.close()
        actors.clear()
        process = server.process
        server.stop()
        assert process.returncode == 0
        server.start()
        check = Connection(server.port)
        try:
            assert check.query('SELECT count(*) FROM writer_queue_rows') == [[str(count)]]
        finally:
            check.close()
        label = 'pre-admission pressure settled predecessors' if pressure else 'coalesced wakeups'
        print(f'wal-writer: {label}; drained {count} owners including zero-page commits PASS', flush=True)
    finally:
        resume_writer(server, paused)
        for actor in actors:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def run_xid_wraparound():
    server = OwnedLisp()
    actor = None
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE writer_wrap_rows (id int PRIMARY KEY)')
        actor.query('INSERT INTO writer_wrap_rows VALUES (1)')
        source = Path(__file__).with_name('wal-writer-failpoints.lisp').resolve()
        server.control(
            f'(load #p"{source}") '
            '(defvar *writer-before-wrap* *primary-wal-writer*) '
            '(defvar *writer-epoch-before-wrap* (primary-wal-writer-epoch *primary-wal-writer*)) '
            '(wal-writer-test-prepare-wrap-boundary) '
            '(defvar *writer-wrap-boundary* *next-xid*)')
        # Normal statement preparation now has to freeze the old XID cycle.
        actor.query('INSERT INTO writer_wrap_rows VALUES (2)')
        server.control(
            '(assert (eql *writer-wrap-boundary* *xid-wrap-completed-through*)) '
            '(assert (null (load-xid-wrap-marker))) '
            '(assert (eq *writer-before-wrap* *primary-wal-writer*)) '
            '(assert (not (eq *writer-epoch-before-wrap* (primary-wal-writer-epoch *primary-wal-writer*)))) '
            '(assert (sb-thread:thread-alive-p (primary-wal-writer-thread *primary-wal-writer*))) '
            '(assert (null (primary-wal-writer-failure *primary-wal-writer*)))')
        assert actor.query('SELECT id FROM writer_wrap_rows ORDER BY id') == [['1'], ['2']]
        actor.close()
        actor = None
        process = server.process
        server.stop()
        assert process.returncode == 0
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM writer_wrap_rows ORDER BY id') == [['1'], ['2']]
        print('wal-writer: SQL XID wraparound replaced epoch without replacing service; restart PASS', flush=True)
    finally:
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def run_pressure_prefix_crash():
    server = OwnedLisp()
    actors = []
    paused = None
    incoming_ready = server.root / 'incoming-pressure-ready'
    incoming_release = server.root / 'incoming-pressure-release'
    try:
        server.start()
        setup = Connection(server.port)
        try:
            setup.query('CREATE TABLE pressure_crash_rows (id int PRIMARY KEY)')
        finally:
            setup.close()
        paused = pause_writer(server, 'pressure-crash')
        pids = []
        for value in (1, 2):
            actor = Connection(server.port)
            actors.append(actor)
            actor.query('BEGIN')
            actor.query(f'INSERT INTO pressure_crash_rows VALUES ({value})')
            pids.append(int(actor.query('SELECT pg_backend_pid()')[0][0]))
        server.control(
            f'(wal-writer-test-admit-backend {pids[0]}) '
            '(defvar *pressure-crash-old* *writer-test-owner*) '
            '(with-wal-critical (let ((limits (copy-list *wal-publication-pressure-limits*))) '
            '(setf (getf limits :owners) 1 *wal-publication-pressure-limits* limits))) '
            f'(wal-pressure-test-start-incoming-at-page-boundary {pids[1]} '
            f'#p"{incoming_ready}" #p"{incoming_release}")')
        wait_file(server, incoming_ready)
        # The incoming actor holds WAL. Read only stable fields while it is paused.
        server.control(
            '(assert (null *writer-test-pressure-error*)) '
            '(assert (eq :released (wal-page-publication-state *pressure-crash-old*))) '
            '(assert (null *wal-pending-page-publication*)) '
            '(assert (>= *wal-durable-lsn* (wal-page-publication-terminal-end *pressure-crash-old*)))')
        process = server.process
        server.stop(immediate=True)
        assert process.returncode == -signal.SIGKILL
        resume_writer(server, paused)
        paused = None
        for actor in actors:
            actor.close()
        actors.clear()
        server.start()
        check = Connection(server.port)
        try:
            assert check.query('SELECT id FROM pressure_crash_rows ORDER BY id') == [['1']]
        finally:
            check.close()
        print('wal-writer: pressure made predecessor durable before incoming terminal; SIGKILL recovery PASS',
              flush=True)
    finally:
        incoming_release.touch()
        resume_writer(server, paused)
        for actor in actors:
            actor.close()
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run_idle_drain_and_reload()
    run_reset_stop_race()
    for after_flush in (False, True):
        run_flush_failure(after_flush)
    run_queued_wakeups()
    run_xid_wraparound()
    run_queued_wakeups(count=5, pressure=True)
    run_pressure_prefix_crash()
