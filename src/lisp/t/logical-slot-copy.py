"""Exercise the real writer lifecycle needed by the explicit slot-copy path."""
from pathlib import Path
import runpy
import time

api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
from parking_wire import RawConnection


def check_writer_pause():
    started = time.monotonic()
    server = api['OwnedLisp']()
    try:
        server.start()
        prefix_bytes = b'0123456789'
        for name in ('copy-prefix-probe', 'copy-prefix-race', 'copy-prefix-overwrite'):
            probe = server.data / 'global' / name
            probe.write_bytes(prefix_bytes)
            probe.chmod(0o600)
        prefix_stat = (server.data / 'global/copy-prefix-probe').stat()
        prefix_facts = ' '.join(str(value) for value in (
            prefix_stat.st_dev, prefix_stat.st_ino, prefix_stat.st_mode,
            prefix_stat.st_nlink, prefix_stat.st_uid, prefix_stat.st_size,
            prefix_stat.st_mtime_ns, prefix_stat.st_ctime_ns))
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE copy_lifecycle (id integer)')
            connection.query('INSERT INTO copy_lifecycle VALUES (1)')
        finally:
            connection.close()
        result = server.root / 'copy-lifecycle-result'
        result_name = str(result).replace('\\', '\\\\').replace('"', '\\"')
        target = server.root / 'copy-target'
        target.mkdir(mode=0o700)
        target_name = str(target).replace('\\', '\\\\').replace('"', '\\"')
        # The worker enters after this control operation releases admission.
        # Joining it inside the control operation would prevent its gate drain.
        server.control(f'''(progn
          (sb-thread:make-thread
           (lambda ()
             (with-open-file (out "{result_name}" :direction :output :if-exists :error)
               (handler-case
                   (let ((initial *primary-wal-writer-generation*))
                     (flet ((healthy ()
                              (sb-thread:with-mutex (*wal-lock*)
                                (assert (primary-wal-writer-available-under-lock-p)))
                              (assert *live-operation-admission-open*)
                              (assert (not *live-operation-terminal-shutdown*))
                              (assert (not *wal-poisoned*))))
                       (assert
                        (equal '(:copied 27)
                               (multiple-value-list
                                (call-with-logical-slot-copy-writer-paused
                                 (lambda (gate ownership)
                                   (check-primary-storage-startup-context gate ownership)
                                   (wal-checkpoint-core)
                                   (assert (= *wal-checkpoint-lsn* *wal-durable-lsn*))
                                   (call-with-logical-slot-copy-source-directory
                                    gate ownership
                                    (lambda (source)
                                      (call-with-logical-slot-copy-target
                                       "{target_name}"
                                       (lambda (owner destination copy-id)
                                         (assert (= 32 (length copy-id)))
                                         (logical-slot-copy-check-target owner destination)
                                         (let* ((bytes (read-bounded-regular-file
                                                        (pgdata-path "PG_VERSION") 32))
                                                (digest (physical-store-update-digest
                                                         #xCBF29CE484222325 bytes)))
                                           (physical-image-copy-file source destination "PG_VERSION"
                                                                      (length bytes) digest)
                                            (let* ((prefix (string->bytes "0123"))
                                                   (digest (physical-store-update-digest
                                                            #xCBF29CE484222325 prefix)))
                                              (with-physical-image-input
                                                  (input (pgdata-path "global/copy-prefix-probe")
                                                         :element-type '(unsigned-byte 8))
                                                (let ((facts (physical-image-file-facts
                                                              (sb-sys:fd-stream-fd input))))
                                                  (assert (equal '({prefix_facts}) (subseq facts 0 8)))
                                                  (assert (plusp (ninth facts)))))
                                              (dolist (arguments '(nil (:source-size 9) (:source-size 3)))
                                                (let ((caught nil))
                                                  (handler-case
                                                      (apply #'physical-image-copy-file source destination
                                                             "global/copy-prefix-probe" 4 digest arguments)
                                                    (error (condition) (setf caught condition)))
                                                  (assert (and caught
                                                               (search (if (equal arguments '(:source-size 3))
                                                                           "bounded file entry"
                                                                           "source length changed")
                                                                       (princ-to-string caught))))))
                                              (assert
                                               (equal (list "global/copy-prefix-probe" 4 digest)
                                                      (physical-image-copy-file
                                                       source destination "global/copy-prefix-probe"
                                                       4 digest :source-size 10)))
                                              (dolist (relative '("global/copy-prefix-race"
                                                                  "global/copy-prefix-overwrite"))
                                                (let ((*physical-image-test-hook*
                                                        (lambda (point actual)
                                                          (when (and (eq point :after-copy-chunk)
                                                                     (equal actual relative))
                                                            (with-open-file
                                                                (output (pgdata-path relative) :direction :io
                                                                        :if-exists :overwrite
                                                                        :element-type '(unsigned-byte 8))
                                                              (file-position output
                                                                (if (equal relative "global/copy-prefix-race") 10 9))
                                                              (write-byte
                                                                (if (equal relative "global/copy-prefix-race") 33 88)
                                                                output)))))
                                                      (caught nil))
                                                  (handler-case
                                                      (physical-image-copy-file
                                                       source destination relative 4 digest :source-size 10)
                                                    (error (condition) (setf caught condition)))
                                                  (assert (and caught
                                                               (search "identity or checksum changed"
                                                                       (princ-to-string caught))))))))))))
                                   (values :copied 27))))))
                       (healthy)
                       (assert (= (1+ initial) *primary-wal-writer-generation*))
                       (let ((sentinel (make-condition 'simple-error :format-control "copy refused"))
                             (caught nil))
                         (handler-case
                             (call-with-logical-slot-copy-writer-paused
                              (lambda (gate ownership)
                                (check-primary-storage-startup-context gate ownership)
                                (error sentinel)))
                           (error (condition) (setf caught condition)))
                         (assert (eq sentinel caught)))
                       (healthy)
                       (assert (= (+ 2 initial) *primary-wal-writer-generation*))
                       (assert
                        (eq :unwound
                            (catch 'copy-exit
                              (call-with-logical-slot-copy-writer-paused
                               (lambda (gate ownership)
                                 (check-primary-storage-startup-context gate ownership)
                                 (throw 'copy-exit :unwound))))))
                       (healthy)
                       (assert (= (+ 3 initial) *primary-wal-writer-generation*))
                       (let ((writer *primary-wal-writer*))
                         (dolist (lock (list *wal-lock* *primary-wal-writer-lock*
                                             *reload-definition-lock* *reload-run-lock*
                                             *physical-follow-generation-lock* *live-operation-lock*))
                           (let ((called nil) (caught nil))
                             (sb-thread:with-mutex (lock)
                               (handler-case
                                   (call-with-logical-slot-copy-writer-paused
                                    (lambda (&rest ignored)
                                      (declare (ignore ignored)) (setf called t)))
                                 (error (condition) (setf caught condition))))
                             (assert (and caught
                                          (search "join requires an outside lifecycle caller"
                                                  (princ-to-string caught))))
                             (assert (not called))))
                         (let ((caught nil))
                           (with-live-operation
                             (handler-case
                                 (call-with-logical-slot-copy-writer-paused
                                  (lambda (&rest ignored)
                                    (declare (ignore ignored))
                                    (error "unexpected admitted copy")))
                               (error (condition) (setf caught condition))))
                           (assert (and caught
                                        (search "join requires an outside lifecycle caller"
                                                (princ-to-string caught)))))
                         (assert (eq writer *primary-wal-writer*)))
                       (healthy)
                       (format out "PASS ~d ~d~%" initial *primary-wal-writer-generation*)))
                 (error (condition) (format out "FAIL ~a~%" condition))))))
          (values))''')
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if result.exists():
                outcome = result.read_text()
                if outcome:
                    assert outcome.startswith('PASS '), (outcome, server.root)
                    break
            if server.process.poll() is not None:
                raise RuntimeError(f'owned server exited; see {server.root / "server.log"}')
            time.sleep(0.05)
        else:
            raise TimeoutError(f'copy lifecycle did not finish; see {server.root}')
        connection = RawConnection(server.port)
        try:
            assert (target / 'PG_VERSION').read_bytes() == (server.data / 'PG_VERSION').read_bytes()
            assert (target / 'global/copy-prefix-probe').read_bytes() == prefix_bytes[:4]
            assert (server.data / 'global/copy-prefix-probe').read_bytes() == prefix_bytes
            assert (server.data / 'global/copy-prefix-race').read_bytes() == prefix_bytes + b'!'
            assert (server.data / 'global/copy-prefix-overwrite').read_bytes() == prefix_bytes[:-1] + b'X'
            assert (target / 'logical-slot-copy.in-progress').is_file()
            assert connection.query('SELECT id FROM copy_lifecycle') == [['1']]
            connection.query('INSERT INTO copy_lifecycle VALUES (2)')
            assert connection.query('SELECT id FROM copy_lifecycle ORDER BY id') == [['1'], ['2']]
        finally:
            connection.close()
        incomplete = api['OwnedLisp']()
        incomplete.data = target
        try:
            try:
                incomplete.start()
            except RuntimeError:
                assert incomplete.process.poll() is not None
                assert 'unfinished logical slot copy cannot start' in (
                    incomplete.root / 'server.log').read_text()
                assert not (target / 'global' / 'pg_catalog.dat').exists()
            else:
                raise AssertionError('unfinished target accepted startup')
        finally:
            incomplete.stop(immediate=True)
        print(f'logical-slot-copy: PASS (real writer pause, checkpoint, unwind, lock guards) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


def check_writer_failure(kind):
    started = time.monotonic()
    server = api['OwnedLisp']()
    try:
        server.start()
        result = server.root / 'copy-failure-result'
        result_name = str(result).replace('\\', '\\\\').replace('"', '\\"')
        server.control(f'''(progn
          (sb-thread:make-thread
           (lambda ()
             (with-open-file (out "{result_name}" :direction :output :if-exists :error)
               (handler-case
                   (let* ((kind :{kind})
                          (primary (make-condition 'simple-error :format-control "first copy failure"))
                          (secondary (make-condition 'simple-error :format-control "writer restart failure"))
                          (old-hook *primary-wal-writer-test-hook*)
                          (old-start (symbol-function 'start-primary-wal-writer))
                          (old-stop (symbol-function 'request-primary-wal-writer-stop))
                          (old-join (symbol-function 'stop-primary-wal-writer))
                          (old-fast (symbol-function 'request-fast-shutdown))
                          (reached (sb-thread:make-semaphore :count 0))
                          (release (sb-thread:make-semaphore :count 0))
                          (armed t) (requests 0) (called nil) (caught nil))
                     (unwind-protect
                          (progn
                            ;; Defer the final process exit until the fail-closed
                            ;; gate and exact primary/secondary conditions are observed.
                            (setf (symbol-function 'request-fast-shutdown)
                                  (lambda (&rest ignored)
                                    (declare (ignore ignored)) (incf requests)))
                            (ecase kind
                              (:writer
                               (setf *primary-wal-writer-test-hook*
                                     (lambda (point writer)
                                       (declare (ignore writer))
                                       (when (and armed (eq point :before-cycle))
                                         (setf armed nil)
                                         (sb-thread:signal-semaphore reached)
                                         (assert (sb-thread:wait-on-semaphore release :timeout 10))
                                         (error primary))))
                               (assert (sb-thread:wait-on-semaphore reached :timeout 10))
                               (setf (symbol-function 'request-primary-wal-writer-stop)
                                     (lambda (&rest arguments)
                                       (multiple-value-prog1 (apply old-stop arguments)
                                         (sb-thread:signal-semaphore release)))))
                              (:restart
                               (setf (symbol-function 'start-primary-wal-writer)
                                     (lambda () (error secondary))))
                              (:stopped-poison
                               (setf (symbol-function 'stop-primary-wal-writer)
                                     (lambda (&rest arguments)
                                       (multiple-value-prog1 (apply old-join arguments)
                                         (setf *wal-poisoned* t)))))
                              (:poison nil))
                            (handler-case
                                (call-with-logical-slot-copy-writer-paused
                                 (lambda (gate ownership)
                                   (check-primary-storage-startup-context gate ownership)
                                   (setf called t)
                                   (ecase kind
                                     (:writer (error "failed writer entered copy callback"))
                                     (:stopped-poison (error "poisoned source entered copy callback"))
                                     (:restart (error primary))
                                     (:poison (wal-fail-stop primary)))))
                              (error (condition) (setf caught condition)))
                            (if (eq kind :stopped-poison)
                                (progn
                                  (assert caught)
                                  (assert (search "WAL I/O previously failed" (princ-to-string caught)))
                                  (assert (null (primary-wal-writer-failure *primary-wal-writer*))))
                                  (assert (eq (if (eq kind :restart) secondary primary) caught)))
                              (let ((operation-error
                                      (getf *logical-slot-copy-restoration-failure* :primary)))
                                (if (eq kind :stopped-poison)
                                    (assert (search "WAL I/O previously failed"
                                                    (princ-to-string operation-error)))
                                    (assert (eq primary operation-error))))
                            (assert (getf *logical-slot-copy-restoration-failure* :restoration))
                            (when (eq kind :restart)
                              (assert (eq secondary
                                          (getf *logical-slot-copy-restoration-failure* :restoration))))
                            (assert (eql called (not (member kind '(:writer :stopped-poison)))))
                            (assert (plusp requests))
                            (assert *wal-poisoned*)
                            (assert *live-operation-terminal-shutdown*)
                            (assert (not *live-operation-admission-open*))
                            (format out "PASS ~a ~d~%" kind requests)
                            (finish-output out))
                       (setf *primary-wal-writer-test-hook* old-hook
                             (symbol-function 'start-primary-wal-writer) old-start
                             (symbol-function 'request-primary-wal-writer-stop) old-stop
                             (symbol-function 'stop-primary-wal-writer) old-join
                             (symbol-function 'request-fast-shutdown) old-fast)
                       (sb-thread:signal-semaphore release))
                     (funcall old-fast))
                 (error (condition) (format out "FAIL ~a~%" condition))))))
          (values))''')
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            outcome = result.read_text() if result.exists() else ''
            if outcome:
                assert outcome.startswith('PASS '), (kind, outcome, server.root)
                break
            if server.process.poll() is not None:
                raise RuntimeError(f'owned failure server exited; see {server.root / "server.log"}')
            time.sleep(0.05)
        else:
            raise TimeoutError(f'copy failure case did not finish; see {server.root}')
        server.process.wait(timeout=15)
        print(f'logical-slot-copy: PASS (fail-closed {kind}, primary condition retained) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    check_writer_pause()
    for failure_kind in ('writer', 'restart', 'poison', 'stopped-poison'):
        check_writer_failure(failure_kind)
