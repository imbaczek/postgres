"""Exercise checkpoint-bound capture in the source image that owns the data."""
from pathlib import Path
import os
import runpy
import time

api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
from parking_wire import RawConnection


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def wait_capture_result(server, result):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        outcome = result.read_text() if result.exists() else ''
        if outcome:
            assert outcome.startswith('PASS '), (outcome, server.root)
            return
        if server.process.poll() is not None:
            raise RuntimeError(f'capture server exited; see {server.root / "server.log"}')
        time.sleep(0.05)
    raise TimeoutError(f'capture worker did not finish; see {server.root}')


def check_idle_transaction_refusal(server):
    connection = RawConnection(server.port)
    result = server.root / 'capture-idle-transaction-result'
    try:
        connection.query('BEGIN')
        connection.query('INSERT INTO capture_rows VALUES (99)')
        server.control(f'''(progn
          (assert (not (active-xids-empty-p)))
          (sb-thread:make-thread
           (lambda ()
             (with-open-file (out {lisp_string(result)} :direction :output :if-exists :error)
               (handler-case
                   (let ((entered nil) (caught nil))
                     (handler-case
                         (call-with-logical-slot-copy-writer-paused
                          (lambda (gate ownership)
                            (declare (ignore gate ownership))
                            (setf entered t)))
                       (error (condition) (setf caught condition)))
                     (assert (and (not entered) caught
                                  (search "no active transaction IDs" (princ-to-string caught))))
                     (assert (not *wal-poisoned*))
                     (assert *live-operation-admission-open*)
                     (sb-thread:with-mutex (*wal-lock*)
                       (assert (primary-wal-writer-available-under-lock-p)))
                     (format out "PASS idle transaction refused before copy callback~%"))
                 (error (condition) (format out "FAIL ~a~%" condition))))))
          (values))''')
        wait_capture_result(server, result)
        connection.query('ROLLBACK')
    finally:
        connection.close()


def check_capture():
    started = time.monotonic()
    server = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE capture_rows (id integer)')
            connection.query('INSERT INTO capture_rows VALUES (1)')
            connection.query("SELECT * FROM pg_create_logical_replication_slot('capture_slot', 'pgoutput')")
        finally:
            connection.close()
        check_idle_transaction_refusal(server)
        server.control(f'''(progn
          (defvar *capture-test-old-policy* nil)
          (defvar *capture-test-expired* nil)
          (sb-thread:with-mutex (*wal-lock*)
            (setf *capture-test-old-policy* (wal-checked-logical-policy-under-lock)))
          (values))''')
        result = server.root / 'capture-result'
        server.control(f'''(progn
          (sb-thread:make-thread
           (lambda ()
             (with-open-file (out {lisp_string(result)} :direction :output :if-exists :error)
               (handler-case
                   (handler-bind ((error (lambda (condition)
                                           (format out "CONDITION ~a~%" condition)
                                           (sb-debug:print-backtrace :stream out :count 24))))
                     (call-with-logical-slot-copy-writer-paused
                      (lambda (gate ownership)
                        (wal-checkpoint-core)
                        (physical-recovery-check-checkpoint-resources
                         *wal-checkpoint-snapshot* *wal-control-metadata*)
                        (call-with-logical-slot-copy-source-directory
                         gate ownership
                         (lambda (context)
                           (call-with-logical-slot-copy-capture
                            gate ownership context
                            (lambda (capture)
                              (assert (logical-slot-copy-check-capture capture :files t))
                              (let* ((facts (logical-slot-copy-capture-facts capture))
                                     (slots (getf facts :slots)))
                                (assert (= (getf facts :end) (getf facts :checkpoint) *wal-durable-lsn*))
                                (assert (= (getf facts :system-id) (catalog-system-identifier)))
                                (assert (= 1 (length slots)))
                                (assert (equal "capture_slot" (caar slots)))
                                (assert (equalp (second (car slots))
                                                (read-bounded-regular-file
                                                 (wal-replication-slot-path "capture_slot")
                                                 +slot-state-maximum-bytes+)))
                                (assert (getf facts :files)))
                              (assert
                               (handler-case
                                   (progn (logical-slot-copy-check-capture
                                           (copy-logical-slot-copy-capture capture)) nil)
                                 (error () t)))
                              (let* ((refused nil)
                                     (thread (sb-thread:make-thread
                                              (lambda ()
                                                (setf refused
                                                      (handler-case
                                                          (progn (logical-slot-copy-check-capture capture) nil)
                                                        (error () t)))))))
                                (assert (not (eq :timeout (sb-thread:join-thread
                                                          thread :timeout 5 :default :timeout))))
                                (assert refused))
                              (assert (logical-slot-copy-check-capture capture))
                              (setf *capture-test-expired* capture)))))))
                     (assert (handler-case
                                 (progn (logical-slot-copy-check-capture *capture-test-expired*) nil)
                               (error () t)))
                     (let ((caught nil))
                       (handler-case
                           (call-with-logical-slot-copy-writer-paused
                            (lambda (gate ownership)
                              (wal-checkpoint-core)
                              (call-with-logical-slot-copy-source-directory
                               gate ownership
                               (lambda (context)
                                 (let ((current nil))
                                   (sb-thread:with-mutex (*wal-lock*)
                                     (setf current *wal-logical-policy*)
                                     (assert (< (wal-logical-status-end
                                                 (wal-logical-policy-state *capture-test-old-policy*))
                                                *wal-durable-lsn*))
                                     (setf *wal-logical-policy* *capture-test-old-policy*))
                                   (unwind-protect
                                        (call-with-logical-slot-copy-capture
                                         gate ownership context
                                         (lambda (capture)
                                           (declare (ignore capture))
                                           (error "stale checkpoint policy entered capture callback")))
                                     (sb-thread:with-mutex (*wal-lock*)
                                       (setf *wal-logical-policy* current))))))))
                         (error (condition) (setf caught condition)))
                       (assert (and caught
                                    (search "published checkpoint policy" (princ-to-string caught)))))
                     (dolist (mutation '(:facts :active-owner :next-xid :cluster-identity))
                       (let ((caught nil) (revoked nil))
                         (handler-case
                             (call-with-logical-slot-copy-writer-paused
                              (lambda (gate ownership)
                                (wal-checkpoint-core)
                                (call-with-logical-slot-copy-source-directory
                                 gate ownership
                                 (lambda (context)
                                   (call-with-logical-slot-copy-capture
                                    gate ownership context
                                    (lambda (capture)
                                      (let* ((facts (logical-slot-copy-capture-facts capture))
                                             (slot (second (first (logical-slot-copy-capture-owners capture))))
                                             (old-xid (getf facts :next-xid))
                                             (old-next *next-xid*)
                                             (old-cluster (catalog-cluster-identity *catalog*)))
                                        (sb-thread:with-mutex (*wal-lock*)
                                          (unwind-protect
                                               (progn
                                                 (ecase mutation
                                                   (:facts (incf (getf facts :next-xid)))
                                                   (:next-xid (incf *next-xid*))
                                                   (:cluster-identity
                                                    (setf (catalog-cluster-identity *catalog*)
                                                          (new-catalog-cluster-identity)))
                                                   (:active-owner
                                                    (setf (wal-replication-slot-active-owner slot) :changed)))
                                                 (assert
                                                  (handler-case
                                                      (progn
                                                        (logical-slot-copy-check-captured-source-under-lock capture)
                                                        nil)
                                                    (error (condition)
                                                      (search (if (eq mutation :active-owner)
                                                                  "registered slot changed"
                                                                  "frozen source changed")
                                                              (princ-to-string condition))))))
                                            (setf (getf facts :next-xid) old-xid
                                                  *next-xid* old-next
                                                  (catalog-cluster-identity *catalog*) old-cluster
                                                  (wal-replication-slot-active-owner slot) nil)))
                                        (setf revoked
                                              (handler-case
                                                  (progn (logical-slot-copy-check-capture capture) nil)
                                                (error (condition)
                                                  (search "scope ended" (princ-to-string condition)))))
                                        (assert revoked))))))))
                           (error (condition) (setf caught condition)))
                         (assert (and revoked caught
                                      (search "scope ended" (princ-to-string caught))))))
                     (dolist (kind '(:pending :marker))
                       (call-with-logical-slot-copy-writer-paused
                        (lambda (gate ownership)
                          (let ((*xid-wrap-pending* (eq kind :pending))
                                (marker (xid-wrap-marker-path)))
                            (unwind-protect
                                 (progn
                                   (when (eq kind :marker)
                                     (with-open-file (stream marker :direction :output :if-exists :error)
                                       (write-line "pending wrap test" stream)))
                                   (assert
                                    (handler-case
                                        (progn (logical-slot-copy-check-identity gate ownership) nil)
                                      (error (condition)
                                        (search "pending XID wrap" (princ-to-string condition))))))
                              (when (eq kind :marker) (delete-file marker)))))))
                     (assert *live-operation-admission-open*)
                     (assert (not *wal-poisoned*))
                     (sb-thread:with-mutex (*wal-lock*)
                       (assert (primary-wal-writer-available-under-lock-p)))
                     (format out "PASS exact source slots, checkpoint policy, scope, stale witness and revocation~%"))
                 (error (condition) (format out "FAIL ~a~%" condition))))))
          (values))''')
        wait_capture_result(server, result)
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM capture_rows') == [['1']]
            connection.query('INSERT INTO capture_rows VALUES (2)')
        finally:
            connection.close()
        print(f'logical-slot-copy-capture: PASS {time.monotonic() - started:.6f}s; '
              f'artifacts: {server.root}', flush=True)
    except BaseException:
        if os.environ.get('PG_LISP_CAPTURE_DEBUG'):
            print(f'CAPTURE-DEBUG PID={server.process.pid} ROOT={server.root} '
                  f'CONTROL={server.control_socket}', flush=True)
            input()
        raise
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    check_capture()
