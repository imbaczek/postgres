"""Terminate SQL writers at the XID ownership handoff in an owned server."""
import json
from contextlib import closing
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time

import psycopg2


BASE = Path(__file__).resolve().parents[2]


def wait_for(predicate, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError('timed out waiting for XID ownership test')


def control(process, path, form):
    assert process.poll() is None
    result = subprocess.run(
        [os.environ.get('SBCL', 'sbcl'), '--script', str(BASE / 't/control-client.lisp'),
         'unix', str(path)], input=f'(assert (= (sb-posix:getpid) {process.pid}))\n'
         + form + '\n(quit)\n', text=True, capture_output=True, timeout=30, check=True)
    assert ';; error:' not in result.stdout, result.stdout
    return result.stdout


def exercise_writer(process, root, connection, fast):
    ready, release = root / 'ready', root / 'release'
    ready.unlink(missing_ok=True)
    release.unlink(missing_ok=True)
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_backend_pid()')
        backend_pid = cursor.fetchone()[0]
    control(process, root / 'control.sock', f'''
      (defparameter *owner-test-original* (symbol-function 'xid-allocate-under-lock))
      (defparameter *owner-test-before* *wal-end-lsn*)
      (assert {'(> *xid-batch-end* *next-xid*)' if fast else '(<= *xid-batch-end* *next-xid*)'})
      (setf (symbol-function 'xid-allocate-under-lock)
        (lambda (&rest arguments)
          (let ((xid (apply *owner-test-original* arguments)))
            (when (and *backend* (= (backend-pid *backend*) {backend_pid}))
              (with-open-file (out {json.dumps(str(ready) + '.tmp')} :direction :output
                                   :if-exists :supersede)
                (format out "~d ~s" xid (backend-xid *backend*)))
              (sb-posix:rename {json.dumps(str(ready) + '.tmp')} {json.dumps(str(ready))})
              (loop repeat 600 until (probe-file {json.dumps(str(release))})
                    do (sleep 0.05)
                    finally (assert (probe-file {json.dumps(str(release))}))))
            xid)))''')
    outcome = []

    def write():
        try:
            with connection.cursor() as cursor:
                cursor.execute('INSERT INTO owner_test VALUES (1)')
            outcome.append('unexpected success')
        except psycopg2.Error as error:
            outcome.append((type(error).__name__, error.pgcode))

    actor = threading.Thread(target=write, daemon=True)
    actor.start()
    try:
        wait_for(ready.exists)
        xid, owner = ready.read_text().split()
        assert xid == owner, (xid, owner)
        control(process, root / 'control.sock', f'''
          (let ((backend (sb-thread:with-mutex (*backend-registry-lock*)
                           (gethash {backend_pid} *backend-registry*))))
            (assert backend)
            (terminate-backend backend))''')
        release.touch()
        actor.join(15)
        assert not actor.is_alive(), 'terminated SQL writer did not exit'
        assert outcome and outcome != ['unexpected success'], outcome
        # The frontend may close before sending a FATAL frame. Cleanup is checked
        # independently through the server's control REPL and durable CLOG.
        control(process, root / 'control.sock', f'''
          (assert (null *active-xids*))
          (assert (zerop (hash-table-count *logical-capture-xid-modes*)))
          (assert (= (clog-get {xid}) +clog-aborted+))
          (assert (not *wal-poisoned*))
          (assert ({'=' if fast else '<'} *owner-test-before* *wal-end-lsn*))
          (assert (sb-thread:with-mutex (*backend-registry-lock*)
                    (null (gethash {backend_pid} *backend-registry*))))''')
        print(f'{"cached" if fast else "reserved"} XID {xid}: cleanup passed {outcome}', flush=True)
    finally:
        release.touch()
        actor.join(15)
        control(process, root / 'control.sock',
                "(setf (symbol-function 'xid-allocate-under-lock) *owner-test-original*)")
        connection.close()


def main():
    root = Path(tempfile.mkdtemp(prefix='pg-xid-owner-'))
    root.chmod(0o700)
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    print(f'artifacts={root}', flush=True)
    with (root / 'server.log').open('w') as log:
        process = subprocess.Popen([str(BASE / 'pg-lisp.sh'), '-D', str(root / 'data'),
                                    '-p', str(port), '--control-socket', str(root / 'control.sock')],
                                   stdout=log, stderr=log)
        def connect():
            return psycopg2.connect(host='127.0.0.1', port=port, user='postgres',
                                    dbname='postgres', connect_timeout=2)

        def ready():
            assert process.poll() is None, f'server exited: {root}'
            try:
                with closing(connect()) as connection:
                    connection.autocommit = True
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT 1')
                return (root / 'control.sock').exists()
            except psycopg2.OperationalError:
                return False

        try:
            wait_for(ready, 120)
            assert int((root / 'data/postmaster.pid').read_text().splitlines()[0]) == process.pid
            with closing(connect()) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute('CREATE TABLE owner_test (id integer)')
            for fast in (False, True):
                connection = connect()
                connection.autocommit = True
                exercise_writer(process, root, connection, fast)
            with closing(connect()) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute('SELECT count(*) FROM owner_test')
                    assert cursor.fetchone() == (0,)
                    cursor.execute('INSERT INTO owner_test VALUES (2)')
            process.send_signal(signal.SIGINT)
            assert process.wait(30) == 0, f'unclean shutdown: {root}'
            print('PASS: SQL ownership cleanup and clean source shutdown', flush=True)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(10)


if __name__ == '__main__':
    main()
