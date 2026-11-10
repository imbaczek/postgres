"""Exercise writer policy reload, write-only output and hibernation on owned servers."""
import argparse
from pathlib import Path
import runpy
import signal
import time


BASE = Path(__file__).resolve().parents[1]
api = runpy.run_path(str(BASE / 't/wal-writer.py'))
OwnedLisp, Connection = api['OwnedLisp'], api['Connection']
wait_file, wait_control = api['wait_file'], api['wait_control']


def control(server, label, expression, timeout=15):
    output = server.control(expression, timeout=timeout)
    (server.root / f'{label}.log').write_text(output)
    return output


def configure(server, delay, blocks):
    (server.data / 'postgresql.conf').write_text(
        f'wal_writer_delay={delay}\nwal_writer_flush_after={blocks}\n')
    started = time.monotonic()
    completed = (server.root / 'server.log').read_text().count('pg-lisp: reload complete')
    server.process.send_signal(signal.SIGHUP)
    expected = f':POLICY {delay} {blocks}'
    while time.monotonic() - started < 120:
        output = control(
            server, f'configuration-{delay}-{blocks}',
            '(multiple-value-bind (delay blocks) (startup-wal-writer-settings) '
            '(list :policy delay blocks))', timeout=120)
        reloaded = (server.root / 'server.log').read_text().count('pg-lisp: reload complete')
        if expected in output and reloaded > completed:
            print(f'writer-policy: settings={delay},{blocks} '
                  f'elapsed_seconds={time.monotonic() - started:.6f}', flush=True)
            return
        time.sleep(0.05)
    raise TimeoutError('writer policy reload did not become visible')


def install_write_barrier(server, ready, release):
    expression = (
        f'(load "{BASE}/t/wal-writer-failpoints.lisp") '
        '(defvar *policy-original-write* (symbol-function \'wal-write-core)) '
        '(defvar *policy-write-complete* nil) (defvar *policy-target* nil) '
        '(defvar *policy-thread* (primary-wal-writer-thread *primary-wal-writer*)) '
        '(setf (symbol-function \'wal-write-core) '
        '(lambda () (funcall *policy-original-write*) '
        '(setf *policy-write-complete* t *policy-target* *wal-end-lsn*))) '
        '(setf *primary-wal-writer-test-hook* '
        '(lambda (point writer) '
        '(when (and (eq point :after-cycle) *policy-write-complete*) '
        '(assert (eq writer *primary-wal-writer*)) '
        '(assert (eq sb-thread:*current-thread* (primary-wal-writer-thread writer))) '
        '(assert (null *backend*)) (assert (zerop *live-operation-depth*)) '
        '(assert (not (sb-thread:holding-mutex-p *wal-lock*))) '
        '(assert (not (sb-thread:holding-mutex-p *reload-definition-lock*))) '
        '(setf *primary-wal-writer-test-hook* nil) '
        f'(wal-writer-test-file-barrier #p"{ready}" #p"{release}")))) '
        ':policy-armed')
    assert ':POLICY-ARMED' in control(server, 'arm-write-barrier', expression)


def check_policy():
    server = OwnedLisp()
    actor = None
    release = server.root / 'release-write'
    started = time.monotonic()
    try:
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE writer_policy_rows (id int PRIMARY KEY)')
        actor.query('SET synchronous_commit=off')
        actor.query('INSERT INTO writer_policy_rows VALUES (1)')
        wait_control(server, '(list :warm (and (null *wal-pending-page-publication*) '
                     '(not (null (primary-wal-writer-last-flush-time *primary-wal-writer*)))))',
                     ':WARM T')
        configure(server, 10000, 2048)
        control(server, 'rearm-background-flush',
                '(sb-thread:with-mutex (*primary-wal-writer-lock*) '
                '(setf (primary-wal-writer-last-flush-time *primary-wal-writer*) nil))')
        actor.query('BEGIN')
        actor.query('SELECT txid_current()')
        actor.query('COMMIT')
        control(server, 'wake-background-flush',
                '(with-wal-critical (notify-primary-wal-writer-under-lock))')
        wait_control(server, '(list :warm (and (null *wal-pending-page-publication*) '
                     '(not (null (primary-wal-writer-last-flush-time *primary-wal-writer*)))))',
                     ':WARM T')
        ready = server.root / 'write-ready'
        install_write_barrier(server, ready, release)
        actor.query('INSERT INTO writer_policy_rows VALUES (2)')
        control(server, 'wake-write', '(with-wal-critical (notify-primary-wal-writer-under-lock))')
        wait_file(server, ready)
        control(server, 'write-only-proof',
                '(with-wal-critical (assert *wal-pending-page-publication*) '
                '(assert (< *wal-durable-lsn* *policy-target*)) '
                '(assert (>= (wal-explicit-written-lsn) *policy-target*)) '
                '(assert (= +clog-in-progress+ (sb-thread:with-mutex (*clog-lock*) '
                '(clog-read-status-under-lock '
                '(wal-page-publication-xid *wal-pending-page-publication*))))))')
        configure(server, 1, 0)
        control(server, 'reload-thread-proof',
                '(assert (eq *policy-thread* (primary-wal-writer-thread *primary-wal-writer*))) '
                '(assert (null (primary-wal-writer-failure *primary-wal-writer*)))')
        release.touch()
        wait_control(server, '(list :flushed (and (null *wal-pending-page-publication*) '
                     '(>= *wal-durable-lsn* *policy-target*)))', ':FLUSHED T',
                     timeout=5, call_timeout=1)
        assert actor.query('SELECT id FROM writer_policy_rows ORDER BY id') == [['1'], ['2']]
        assert actor.query("SELECT setting, unit, context, pending_restart FROM pg_settings "
                           "WHERE name='wal_writer_delay'") == [['1', 'ms', 'sighup', 'f']]
        assert actor.query('SHOW wal_writer_flush_after') == [['0']]
        configure(server, 1000, 2048)
        control(server, 'watch-hibernation',
                '(defvar *policy-original-semaphore-wait* '
                '(symbol-function \'sb-thread:wait-on-semaphore)) '
                '(defvar *policy-wait-cycle* nil) '
                '(defvar *policy-short-wait-cycle* nil) '
                '(sb-ext:without-package-locks '
                '(setf (symbol-function \'sb-thread:wait-on-semaphore) '
                '(lambda (semaphore &rest arguments) '
                '(when (and (eq sb-thread:*current-thread* *policy-thread*) '
                '(eq semaphore (primary-wal-writer-wake *primary-wal-writer*))) '
                '(sb-thread:with-mutex (*primary-wal-writer-lock*) '
                '(unless (primary-wal-writer-wake-pending-p *primary-wal-writer*) '
                '(case (primary-wal-writer-idle-cycles *primary-wal-writer*) '
                '(49 (assert (= (getf arguments :timeout) 1.0)) '
                '(setf *policy-short-wait-cycle* '
                '(primary-wal-writer-cycles *primary-wal-writer*))) '
                '(50 (assert (= (getf arguments :timeout) 25.0)) '
                '(unless *policy-wait-cycle* '
                '(setf *policy-wait-cycle* '
                '(primary-wal-writer-cycles *primary-wal-writer*)))))))) '
                '(apply *policy-original-semaphore-wait* semaphore arguments))))')
        control(server, 'arm-hibernation',
                '(with-wal-critical (sb-thread:with-mutex (*primary-wal-writer-lock*) '
                '(setf (primary-wal-writer-idle-cycles *primary-wal-writer*) 48)) '
                '(notify-primary-wal-writer-under-lock))')
        wait_control(server, '(list :sleeping (sb-thread:with-mutex (*primary-wal-writer-lock*) '
                     '(and *policy-short-wait-cycle* *policy-wait-cycle* '
                     '(< *policy-short-wait-cycle* *policy-wait-cycle*) '
                     '(= (primary-wal-writer-idle-cycles *primary-wal-writer*) 50) '
                     '(not (primary-wal-writer-wake-pending-p *primary-wal-writer*)) '
                     '(= *policy-wait-cycle* (primary-wal-writer-cycles *primary-wal-writer*)))))',
                     ':SLEEPING T', timeout=5, call_timeout=1)
        control(server, 'capture-idle-cycle',
                '(defvar *policy-idle-cycle-witness* *policy-wait-cycle*)')
        time.sleep(1.25)
        control(server, 'extended-idle-proof',
                '(sb-thread:with-mutex (*primary-wal-writer-lock*) '
                '(assert (= *policy-idle-cycle-witness* '
                '(primary-wal-writer-cycles *primary-wal-writer*))) '
                '(assert (= (primary-wal-writer-idle-cycles *primary-wal-writer*) 50)) '
                '(assert (not (primary-wal-writer-wake-pending-p *primary-wal-writer*))))')
        wake_started = time.monotonic()
        actor.query('INSERT INTO writer_policy_rows VALUES (3)')
        wait_control(server, '(list :awake (null *wal-pending-page-publication*))', ':AWAKE T',
                     timeout=5, call_timeout=1)
        wake_elapsed = time.monotonic() - wake_started
        assert wake_elapsed < 5, wake_elapsed
        assert actor.query('SELECT id FROM writer_policy_rows ORDER BY id') == [['1'], ['2'], ['3']]
        actor.close()
        actor = None
        process = server.process
        server.stop()
        assert process.returncode == 0
        print(f'writer-policy: write-only debt, SIGHUP, unchanged live writer, '
              f'hibernation wake={wake_elapsed:.6f}s PASS', flush=True)
    finally:
        release.touch()
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'writer-policy: artifacts={server.root} '
              f'elapsed_seconds={time.monotonic() - started:.6f}', flush=True)


def check_legacy_restart(legacy):
    server = OwnedLisp()
    actor = None
    runtime = OwnedLisp.start.__globals__
    original = runtime['LISP']
    try:
        runtime['LISP'] = legacy
        server.start()
        actor = Connection(server.port)
        actor.query('CREATE TABLE writer_legacy_rows (id int PRIMARY KEY)')
        actor.query('INSERT INTO writer_legacy_rows VALUES (7)')
        actor.close()
        actor = None
        process = server.process
        server.stop()
        assert process.returncode == 0
        runtime['LISP'] = original
        server.start()
        actor = Connection(server.port)
        assert actor.query('SELECT id FROM writer_legacy_rows') == [['7']]
        actor.close()
        actor = None
        process = server.process
        server.stop()
        assert process.returncode == 0
        print('writer-policy: cold restart of pre-policy WAL/control preserved row PASS', flush=True)
    finally:
        runtime['LISP'] = original
        if actor is not None:
            actor.close()
        server.stop(immediate=True)
        print(f'writer-policy: legacy artifacts={server.root}', flush=True)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--legacy-lisp', type=Path)
    arguments = parser.parse_args()
    check_policy()
    if arguments.legacy_lisp:
        check_legacy_restart(arguments.legacy_lisp.resolve())
