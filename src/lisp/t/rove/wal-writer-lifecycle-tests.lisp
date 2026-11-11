;;;; Tracked primary WAL writer lifecycle, with no external storage I/O.
(in-package #:pg)

(defun make-wal-writer-service-test-thread (function)
  (let* ((names '(*wal-lock* *primary-wal-writer-lock* *reload-definition-lock*
                  *wal-ready* *wal-poisoned* *wal-storage-incarnation*
                  *wal-end-lsn* *wal-durable-lsn* *wal-logical-policy* *logical-capture-force*
                  *wal-insert-timeline* *wal-control-metadata* *postmaster-data-lock*
                  *physical-recovery-state* *wal-pending-page-publication*
                  *wal-pending-page-publication-tail* *clog-pending-commits*
                  *primary-wal-writer-delay* *primary-wal-writer-test-hook*))
         (values (mapcar #'symbol-value names)))
    (sb-thread:make-thread (lambda () (progv names values (funcall function)))
                           :name "owned primary WAL writer lifecycle test")))

(defun call-with-wal-writer-service-test (function &key hook)
  (let* ((saved-writer *primary-wal-writer*)
         (saved-generation *primary-wal-writer-generation*)
         (saved-shutdown-p *primary-wal-writer-shutdown-p*)
         (*wal-lock* (sb-thread:make-mutex :name "writer service test WAL"))
         (*primary-wal-writer-lock* (sb-thread:make-mutex :name "writer service test state"))
         (*reload-definition-lock* (sb-thread:make-mutex :name "writer service test definitions"))
         (*wal-ready* t) (*wal-poisoned* nil)
         (*wal-end-lsn* 0) (*wal-durable-lsn* 0)
         (*wal-storage-incarnation* (gensym "WRITER-SERVICE-TEST-"))
         (*wal-logical-policy* nil) (*logical-capture-force* nil)
         (*wal-insert-timeline* 1)
         (*wal-control-metadata* (make-wal-control-metadata 1 +db-in-production+))
         (*postmaster-data-lock* (list :owned-directory))
         (*physical-recovery-state* nil)
         (*wal-pending-page-publication* nil) (*wal-pending-page-publication-tail* nil)
         (*clog-pending-commits* (make-hash-table :test #'eql))
         (*primary-wal-writer-delay* 0.02)
         (*primary-wal-writer-test-hook* hook)
         (make-thread (symbol-function 'make-primary-wal-writer-thread))
         (owned (symbol-function 'postmaster-pid-owned-p))
         (shutdown (symbol-function 'shutdown-in-progress-p)))
    (unless (null saved-writer) (error "writer service test requires no installed service"))
    (unwind-protect
         (progn
           (setf *primary-wal-writer-generation* 0
                 *primary-wal-writer-shutdown-p* nil
                 (symbol-function 'postmaster-pid-owned-p) (lambda () t)
                 (symbol-function 'shutdown-in-progress-p) (lambda () nil)
                 (symbol-function 'make-primary-wal-writer-thread)
                 (lambda (writer)
                   (make-wal-writer-service-test-thread
                    (lambda () (primary-wal-writer-main writer)))))
           (funcall function))
      (when *primary-wal-writer* (stop-primary-wal-writer))
      (setf *primary-wal-writer* saved-writer
            *primary-wal-writer-generation* saved-generation
            *primary-wal-writer-shutdown-p* saved-shutdown-p
            (symbol-function 'make-primary-wal-writer-thread) make-thread
            (symbol-function 'postmaster-pid-owned-p) owned
            (symbol-function 'shutdown-in-progress-p) shutdown)))
  (values))

(rove:deftest primary-writer-registers-before-cycling-and-stops-outside-admission
    (let ((cycled (sb-thread:make-semaphore)))
      (call-with-wal-writer-service-test
       (lambda ()
         (let ((writer (start-primary-wal-writer)))
           (rove:ok (sb-thread:wait-on-semaphore cycled :timeout 2))
           (rove:ok (eq writer *primary-wal-writer*))
           (rove:ok (= 1 (primary-wal-writer-generation writer)))
           (stop-primary-wal-writer)
           (rove:ok (primary-wal-writer-joined-p writer))
           (rove:ok (eq :stopped (primary-wal-writer-state writer)))
           (rove:ok (not (sb-thread:thread-alive-p (primary-wal-writer-thread writer))))
           (rove:ok (null (primary-wal-writer-failure writer)))
           (let ((next (start-primary-wal-writer)))
             (rove:ok (= 2 (primary-wal-writer-generation next)))
             (rove:ok (not (eq writer next))))))
       :hook (lambda (point writer)
               (when (eq point :after-cycle)
                 (assert (eq writer *primary-wal-writer*))
                 (assert (eq (primary-wal-writer-thread writer) sb-thread:*current-thread*))
                 (assert (null *backend*))
                 (assert (zerop *live-operation-depth*))
                 (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                 (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
                 (sb-thread:signal-semaphore cycled))))))

(rove:deftest primary-writer-start-does-not-wait-for-definition-exclusion
    (let ((cycled (sb-thread:make-semaphore)) (writer nil))
      (call-with-wal-writer-service-test
       (lambda ()
         (sb-thread:with-mutex (*reload-definition-lock*)
           (setf writer (start-primary-wal-writer))
           (rove:ok (sb-thread:thread-alive-p (primary-wal-writer-thread writer)))
           (rove:ok (not (sb-thread:wait-on-semaphore cycled :timeout 0.03)))
           (rove:ok (handler-case (progn (stop-primary-wal-writer) nil) (error () t)))
           (rove:ok (not (primary-wal-writer-stop-p writer)))
           (request-primary-wal-writer-stop writer))
         (stop-primary-wal-writer)
         (rove:ok (primary-wal-writer-joined-p writer)))
       :hook (lambda (point writer)
               (declare (ignore writer))
               (when (eq point :after-cycle) (sb-thread:signal-semaphore cycled))))))

(rove:deftest primary-writer-thread-creation-failure-does-not-publish-a-service
    (call-with-wal-writer-service-test
     (lambda ()
       (let ((create (symbol-function 'make-primary-wal-writer-thread))
             (failure (make-condition 'simple-error :format-control "writer thread creation failed")))
         (unwind-protect
              (progn
                (setf (symbol-function 'make-primary-wal-writer-thread)
                      (lambda (writer) (declare (ignore writer)) (error failure)))
                (rove:ok (eq failure (handler-case (start-primary-wal-writer)
                                       (error (condition) condition))))
                (rove:ok (null *primary-wal-writer*))
                (rove:ok (zerop *primary-wal-writer-generation*)))
           (setf (symbol-function 'make-primary-wal-writer-thread) create))))))

(rove:deftest primary-writer-failure-is-fenced-after-cycle-locks-and-remains-tracked
    (let ((failed (sb-thread:make-semaphore))
          (failure (make-condition 'simple-error :format-control "writer cycle failed"))
          (observed nil))
      (call-with-wal-writer-service-test
       (lambda ()
         (let ((fail-stop (symbol-function 'wal-fail-stop)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
                          (setf observed condition)
                          (sb-thread:signal-semaphore failed)
                          (error condition)))
                  (let ((writer (start-primary-wal-writer)))
                    (rove:ok (sb-thread:wait-on-semaphore failed :timeout 2))
                    (stop-primary-wal-writer)
                    (rove:ok (eq failure observed))
                    (rove:ok (eq failure (primary-wal-writer-failure writer)))
                    (rove:ok (eq :failed (primary-wal-writer-state writer)))
                    (rove:ok (eq writer *primary-wal-writer*))
                    (rove:ok (primary-wal-writer-joined-p writer))
                    (rove:ok (handler-case (progn (start-primary-wal-writer) nil)
                               (wal-input-validation-error () t)))))
             (when *primary-wal-writer* (stop-primary-wal-writer))
             (setf (symbol-function 'wal-fail-stop) fail-stop))))
       :hook (lambda (point writer)
               (declare (ignore writer))
               (when (eq point :before-cycle) (error failure))))))

(rove:deftest primary-writer-shutdown-closes-startup-even-with-no-registered-thread
    (call-with-wal-writer-service-test
     (lambda ()
       (stop-primary-wal-writer :shutdown-p t)
       (rove:ok *primary-wal-writer-shutdown-p*)
       (rove:ok (null *primary-wal-writer*))
       (rove:ok (handler-case (progn (start-primary-wal-writer) nil)
                  (wal-input-validation-error () t)))
       (rove:ok (zerop *primary-wal-writer-generation*)))))

(rove:deftest primary-writer-shutdown-captures-a-thread-being-created
    (call-with-wal-writer-service-test
     (lambda ()
       (let ((entered (sb-thread:make-semaphore))
             (release (sb-thread:make-semaphore))
             (stopping (sb-thread:make-semaphore))
             (stopped (sb-thread:make-semaphore))
             (create (symbol-function 'make-primary-wal-writer-thread))
             (starter nil) (stopper nil) (start-error nil) (stop-error nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'make-primary-wal-writer-thread)
                      (lambda (writer)
                        (sb-thread:signal-semaphore entered)
                        (unless (sb-thread:wait-on-semaphore release :timeout 2)
                          (error "writer creation test release timed out"))
                        (funcall create writer)))
                (setf starter
                      (make-wal-writer-service-test-thread
                       (lambda ()
                         (handler-case (start-primary-wal-writer)
                           (error (condition) (setf start-error condition))))))
                (rove:ok (sb-thread:wait-on-semaphore entered :timeout 2))
                (setf stopper
                      (make-wal-writer-service-test-thread
                       (lambda ()
                         (sb-thread:signal-semaphore stopping)
                         (handler-case (stop-primary-wal-writer :shutdown-p t)
                           (error (condition) (setf stop-error condition)))
                         (sb-thread:signal-semaphore stopped))))
                (rove:ok (sb-thread:wait-on-semaphore stopping :timeout 2))
                (rove:ok (not (sb-thread:wait-on-semaphore stopped :timeout 0.03)))
                (sb-thread:signal-semaphore release)
                (rove:ok (sb-thread:wait-on-semaphore stopped :timeout 2))
                (sb-thread:join-thread starter)
                (sb-thread:join-thread stopper)
                (rove:ok (null start-error))
                (rove:ok (null stop-error))
                (rove:ok *primary-wal-writer-shutdown-p*)
                (rove:ok (primary-wal-writer-joined-p *primary-wal-writer*))
                (rove:ok (not (sb-thread:thread-alive-p
                               (primary-wal-writer-thread *primary-wal-writer*))))
                (rove:ok (handler-case (progn (start-primary-wal-writer) nil)
                           (wal-input-validation-error () t))))
           (sb-thread:signal-semaphore release)
           (dolist (thread (list starter stopper))
             (when thread (sb-thread:join-thread thread :timeout 2 :default :timeout)))
           (setf (symbol-function 'make-primary-wal-writer-thread) create))))))

(rove:deftest primary-writer-abandoned-thread-exit-is-a-tracked-failure
    (let ((failed (sb-thread:make-semaphore)) (observed nil))
      (call-with-wal-writer-service-test
       (lambda ()
         (let ((fail-stop (symbol-function 'wal-fail-stop)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
                          (setf observed condition)
                          (sb-thread:signal-semaphore failed)
                          (error condition)))
                  (let ((writer (start-primary-wal-writer)))
                    (rove:ok (sb-thread:wait-on-semaphore failed :timeout 2))
                    (stop-primary-wal-writer)
                    (rove:ok (typep observed 'error))
                    (rove:ok (eq observed (primary-wal-writer-failure writer)))
                    (rove:ok (eq :failed (primary-wal-writer-state writer)))
                    (rove:ok (primary-wal-writer-joined-p writer))))
             (when *primary-wal-writer* (stop-primary-wal-writer))
             (setf (symbol-function 'wal-fail-stop) fail-stop))))
       :hook (lambda (point writer)
               (declare (ignore writer))
               (when (eq point :before-cycle) (sb-thread:return-from-thread :abandoned))))))
