(in-package #:pg-physical-tests)

(defvar *parking-test-threads* nil)

(defun parking-test-wait (semaphore)
  (unless (sb-thread:wait-on-semaphore semaphore :timeout 5)
    (error "parking test semaphore timed out")))

(defun parking-test-spawn (function name)
  (let ((failure (list nil)))
    (sb-sys:without-interrupts
      (let ((thread (sb-thread:make-thread
                     (lambda ()
                       (handler-case (funcall function)
                         (serious-condition (condition) (setf (car failure) condition))))
                     :name name)))
        (push (cons thread failure) *parking-test-threads*)
        thread))))

(defun parking-test-join (thread)
  (sb-thread:join-thread thread :timeout 5 :default :timed-out)
  (when (sb-thread:thread-alive-p thread)
    (error "parking test thread did not finish: ~a" (sb-thread:thread-name thread)))
  (let ((failure (cdr (assoc thread *parking-test-threads*))))
    (when (car failure) (error (car failure)))))

(defun call-with-parking-test-state (function)
  (let* ((settings
           (list (cons 'pg::*live-operation-lock* (sb-thread:make-mutex :name "parking test"))
                 (cons 'pg::*live-operation-cv* (sb-thread:make-waitqueue))
                 (cons 'pg::*reload-run-lock* (sb-thread:make-mutex :name "parking test reload"))
                 (cons 'pg::*reload-requested* nil)
                 (cons 'pg::*live-operation-count* 0)
                 (cons 'pg::*live-operation-backend-counts* (make-hash-table :test #'eq))
                 (cons 'pg::*live-operation-admission-open* t)
                 (cons 'pg::*live-operation-drain-open* nil)
                 (cons 'pg::*live-operation-terminal-shutdown* nil)
                 (cons 'pg::*live-operation-quiescence-owner* nil)
                 (cons 'pg::*live-operation-quiescence-owner-thread* nil)
                 (cons 'pg::*live-operation-drain-backends* nil)
                 (cons 'pg::*live-operation-cleanup-backends* nil)
                 (cons 'pg::*live-operation-pending-cancel-requests* nil)
                 (cons 'pg::*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
                 (cons 'pg::*live-operation-parked* (make-hash-table :test #'eq))
                 (cons 'pg::*live-operation-park-generation* 0)
                 (cons 'pg::*live-operation-parking-test-hook* nil)
                 (cons 'pg::*live-operation-admission-wait-hook* nil)
                 (cons 'pg::*live-operation-before-open-hook* nil)
                 (cons 'pg::*backend-registry* (make-hash-table :test #'eql))
                 (cons 'pg::*shutdown-abandoned-backends* nil)))
         (saved (mapcar (lambda (entry)
                          (list (car entry) (boundp (car entry))
                                (when (boundp (car entry)) (symbol-value (car entry)))))
                        settings)))
    (unwind-protect
         (progn
           (dolist (entry settings) (set (car entry) (cdr entry)))
           (let ((pg::*backend* (pg::make-backend :pid 1 :state :in-progress :read-only t))
                 (pg::*live-operation-depth* 0)
                 (*parking-test-threads* nil))
             (unwind-protect (funcall function)
               (dolist (entry *parking-test-threads*) (parking-test-join (car entry))))))
      (dolist (entry saved)
        (if (second entry) (set (first entry) (third entry))
            (makunbound (first entry)))))))

(deftest parked-operation-preserves-depth-owner-and-multiple-values
    (call-with-parking-test-state
     (lambda ()
       (pg::with-live-operation
         (let ((charged pg::*backend*))
           (pg::with-live-operation
             (let ((pg::*backend* (pg::make-backend :pid 2)))
               (ok (equal '(17 nil 23)
                          (multiple-value-list
                           (pg::call-with-live-operation-parked
                            (lambda ()
                              (ok (zerop pg::*live-operation-depth*))
                              (ok (zerop pg::*live-operation-count*))
                              (ok (zerop (hash-table-count pg::*live-operation-backend-counts*)))
                              (ok (= 1 (pg::live-operation-parked-count)))
                              (values 17 nil 23))))))
               (ok (= 2 pg::*live-operation-depth*))
               (ok (= 1 pg::*live-operation-count*))
               (ok (= 1 (gethash charged pg::*live-operation-backend-counts*)))
               (ok (not (gethash pg::*backend* pg::*live-operation-backend-counts*))))
             (ok (zerop (pg::live-operation-parked-count))))))
       (ok (zerop pg::*live-operation-count*)))))

(deftest parked-operation-restores-before-nonlocal-cleanup
    (call-with-parking-test-state
     (lambda ()
       (pg::with-live-operation
         (ok (eq :injected
                 (catch 'parking-body-fault
                   (unwind-protect
                        (pg::call-with-live-operation-parked
                         (lambda () (throw 'parking-body-fault :injected)))
                     (ok (= 1 pg::*live-operation-count*))
                     (ok (zerop (pg::live-operation-parked-count))))))))
       (ok (zerop pg::*live-operation-count*)))))

(deftest parked-operation-allows-sequential-parks-and-rejects-callback-admission
    (call-with-parking-test-state
     (lambda ()
       (pg::with-live-operation
         (dotimes (index 2)
           (ok (= index
                  (pg::call-with-live-operation-parked
                   (lambda ()
                     (ok (intake-fails-p (lambda () (pg::with-live-operation :invalid))))
                     index)))))
         (ok (= 1 pg::*live-operation-count*))
         (ok (zerop (pg::live-operation-parked-count))))
       (ok (zerop pg::*live-operation-count*)))))

(deftest parked-callback-cannot-charge-admission-through-a-direct-entry
    (call-with-parking-test-state
     (lambda ()
       (pg::with-live-operation
         (pg::call-with-live-operation-parked
          (lambda ()
            (ok (intake-fails-p
                 (lambda ()
                   (pg::call-with-live-operation-lock
                    (lambda () (pg::enter-live-operation-locked :normal nil)))))))))
       (ok (zerop pg::*live-operation-count*)))))

(deftest parked-callback-cannot-defer-a-cancel-request-through-a-closed-gate
    (call-with-parking-test-state
     (lambda ()
       (let ((capture (symbol-function 'pg::capture-live-operation-cancel-target))
             (target (cons pg::*backend* (gensym "QUERY-"))))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::capture-live-operation-cancel-target)
                      (lambda (body) (declare (ignore body)) target))
                (pg::with-live-operation
                  (pg::call-with-live-operation-parked
                   (lambda ()
                     (let ((pg::*live-operation-admission-open* nil)
                           (pg::*live-operation-drain-open* nil))
                       (ok (intake-fails-p
                            (lambda ()
                              (pg::with-live-operation-cancel-request (nil) :invalid))))
                       (ok (null pg::*live-operation-pending-cancel-requests*))
                       (let ((pg::*live-operation-parked-p* nil))
                         (ok (eq :queued
                                 (pg::try-enter-live-operation-cancel-request nil nil))))
                       (ok (equal (list target) pg::*live-operation-pending-cancel-requests*))
                       (setf pg::*live-operation-pending-cancel-requests* nil)))))
                (ok (zerop pg::*live-operation-count*)))
           (setf (symbol-function 'pg::capture-live-operation-cancel-target) capture))))))

(deftest parking-rejects-an-already-owned-admission-mutex-before-publication
    (call-with-parking-test-state
     (lambda ()
       (let ((original (symbol-function 'pg::park-live-operation-under-lock))
             (published nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::park-live-operation-under-lock)
                      (lambda (&rest ignored)
                        (declare (ignore ignored))
                        (setf published t)
                        (error "unexpected parked publication under owned mutex")))
                (pg::with-live-operation
                  (pg::call-with-live-operation-lock
                   (lambda ()
                     (ok (intake-fails-p
                          (lambda ()
                            (pg::call-with-live-operation-parked (lambda () :invalid)))))))))
           (setf (symbol-function 'pg::park-live-operation-under-lock) original))
         (ok (not published))
         (ok (zerop pg::*live-operation-count*))
         (ok (zerop (pg::live-operation-parked-count)))))))

(deftest parked-callback-cannot-start-a-terminal-fence
    (call-with-parking-test-state
     (lambda ()
       (pg::with-live-operation
         (pg::call-with-live-operation-parked
          (lambda ()
            (unwind-protect
                 (let ((pg::*live-operation-parked* (make-hash-table :test #'eq)))
                   (ok (intake-fails-p #'pg::fence-live-operation-shutdown))
                   (ok (not pg::*live-operation-terminal-shutdown*)))
              (setf pg::*live-operation-terminal-shutdown* nil
                    pg::*live-operation-admission-open* t)))))
       (ok (zerop pg::*live-operation-count*)))))

(deftest parked-operation-keeps-exact-nested-snapshot-ownership
    (call-with-parking-test-state
     (lambda ()
       (let ((pg::*active-snapshots* 0)
             (pg::*active-snapshot-set* (make-hash-table :test #'eq))
             (pg::*active-xids* nil) (pg::*next-xid* 50)
             (pg::*physical-recovery-state* nil))
         (setf (pg::backend-isolation-level pg::*backend*) :repeatable-read)
         (unwind-protect
              (pg::with-live-operation
                (pg::with-statement-snapshot
                    (outer (lambda (registration)
                             (pg::begin-transaction-snapshot pg::*backend* 1 registration)))
                  (pg::with-statement-snapshot
                      (inner (lambda (registration) (pg::make-snapshot 2 :register registration)))
                    (let ((owner pg::*backend*))
                      (pg::call-with-live-operation-parked
                       (lambda ()
                         (pg::with-live-operation-quiescence
                           (let ((parked (pg::live-operation-parked-owners-under-quiescence)))
                             (ok (= 1 (length parked)))
                             (ok (= 2 (length (fourth (first parked)))))
                             (ok (= 3 pg::*active-snapshots*))
                             (ok (progn (pg::physical-promotion-check-reader-snapshots (list owner) parked) t))
                             (incf (second (first (fourth (first parked)))))
                             (ok (intake-fails-p
                                  (lambda ()
                                    (pg::physical-promotion-check-reader-snapshots (list owner) parked))))))))
                      (ok (eq inner pg::*current-snapshot*))))))
           (pg::reset-backend-transaction pg::*backend*))
         (ok (zerop pg::*active-snapshots*))
         (ok (zerop (pg::live-operation-parked-count)))))))

(deftest parked-operation-terminal-restoration-is-counted-and-fatal
    (call-with-parking-test-state
     (lambda ()
       (let ((continued nil) (cleanup-owned nil))
         (ok (handler-case
                 (pg::with-live-operation
                   (unwind-protect
                        (progn
                          (pg::call-with-live-operation-parked
                           (lambda () (setf pg::*live-operation-terminal-shutdown* t)))
                          (setf continued t))
                     (setf cleanup-owned (= 1 pg::*live-operation-count*))))
               (pg::pg-error (condition) (pg::fatal-error-p condition))))
         (ok cleanup-owned)
         (ok (not continued))
         (ok (zerop pg::*live-operation-count*))
         (ok (zerop (pg::live-operation-parked-count)))))))

(deftest parked-reload-request-is-queued
    (call-with-parking-test-state
     (lambda ()
       (pg::with-live-operation
         (pg::call-with-live-operation-parked
          (lambda ()
            (ok (eq :queued (pg::reload)))
            (ok pg::*reload-requested*)))))))

(deftest parked-operation-restores-at-every-transfer-boundary
    (dolist (boundary '(:before-park :after-park :before-restore :after-restore))
      (call-with-parking-test-state
       (lambda ()
         (let ((seen nil))
           (setf pg::*live-operation-parking-test-hook*
                 (lambda (point token)
                   (declare (ignore token))
                   (when (eq point boundary)
                     (setf seen t)
                     (throw 'transfer-fault :injected))))
           (pg::with-live-operation
             (ok (eq :injected
                     (catch 'transfer-fault
                       (unwind-protect
                            (pg::call-with-live-operation-parked (lambda () :normal))
                         (ok (= 1 pg::*live-operation-count*))
                         (ok (zerop (pg::live-operation-parked-count))))))))
           (ok seen)
           (ok (zerop pg::*live-operation-count*)))))))

(deftest parked-operation-allows-promotion-while-reload-backs-off
    (call-with-parking-test-state
     (lambda ()
       (let* ((backoff (sb-thread:make-semaphore))
              (promotion-done (sb-thread:make-semaphore))
              (continued nil) (compiled nil) (promotion-saw-parked nil)
              (reloader nil) (promoter nil))
         (setf pg::*live-operation-before-open-hook*
               (lambda ()
                 (when (and (string= "parking reloader" (sb-thread:thread-name sb-thread:*current-thread*))
                            (plusp (pg::live-operation-parked-count)))
                   (sb-thread:signal-semaphore backoff))))
         (pg::with-live-operation
           (pg::call-with-live-operation-parked
            (lambda ()
              (setf reloader
                    (parking-test-spawn
                     (lambda ()
                       (pg::call-with-live-operation-reload
                        (lambda () (setf compiled continued))))
                     "parking reloader"))
              (parking-test-wait backoff)
              (setf promoter
                    (parking-test-spawn
                     (lambda ()
                       (sb-thread:with-mutex (pg::*reload-run-lock*)
                         (pg::with-live-operation-quiescence
                           (setf promotion-saw-parked (= 1 (pg::live-operation-parked-count)))))
                       (sb-thread:signal-semaphore promotion-done))
                     "parking promotion"))
              (parking-test-wait promotion-done)
              (ok (not compiled))))
           (setf continued t))
         (parking-test-join reloader)
         (parking-test-join promoter)
         (ok promotion-saw-parked)
         (ok compiled)
         (ok (zerop pg::*live-operation-count*))
         (ok (zerop (pg::live-operation-parked-count)))))))

(deftest reload-rechecks-when-an-active-query-parks-during-drain
    (call-with-parking-test-state
     (lambda ()
       (let ((backoff (sb-thread:make-semaphore))
             (continued nil) (compiled nil) (reloader nil))
         (setf pg::*live-operation-before-open-hook*
               (lambda ()
                 (when (plusp (pg::live-operation-parked-count))
                   (sb-thread:signal-semaphore backoff))))
         (pg::with-live-operation
           (setf reloader
                 (parking-test-spawn
                  (lambda ()
                    (pg::call-with-live-operation-reload
                     (lambda () (setf compiled continued))))
                  "parking late reloader"))
           (loop with deadline = (+ (get-internal-real-time) (* 5 internal-time-units-per-second))
                 until (pg::call-with-live-operation-lock
                        (lambda () pg::*live-operation-drain-open*))
                 do (when (> (get-internal-real-time) deadline)
                      (error "reload never entered operation drain"))
                    (sleep 0.001))
           (pg::call-with-live-operation-parked
            (lambda ()
              (parking-test-wait backoff)
              (ok (not compiled))))
           (setf continued t))
         (parking-test-join reloader)
         (ok compiled)
         (ok (zerop pg::*live-operation-count*))
         (ok (zerop (pg::live-operation-parked-count)))))))

(deftest terminal-parking-cleanup-waits-for-the-foreign-storage-owner
    (call-with-parking-test-state
     (lambda ()
       (let* ((owner-ready (sb-thread:make-semaphore))
              (release-owner (sb-thread:make-semaphore))
              (restoring (sb-thread:make-semaphore))
              (fence-done (sb-thread:make-semaphore))
              (cleanup-ready (sb-thread:make-semaphore))
              (release-cleanup (sb-thread:make-semaphore))
              (owner nil) (finalizer nil) (controller nil)
              (observed nil) (cleanup-owned nil) (cleanup-blocked-fence nil) (continued nil))
         (setf pg::*live-operation-parking-test-hook*
               (lambda (point token)
                 (declare (ignore token))
                 (when (eq point :before-restore) (sb-thread:signal-semaphore restoring))))
         (unwind-protect
              (progn
                (setf controller
                      (parking-test-spawn
                       (lambda ()
                         (parking-test-wait restoring)
                         (setf observed
                               (pg::call-with-live-operation-lock
                                (lambda ()
                                  (and (zerop (sb-thread:semaphore-count fence-done))
                                       pg::*live-operation-terminal-shutdown*
                                       pg::*live-operation-quiescence-owner*
                                       (zerop pg::*live-operation-count*)
                                       (= 1 (hash-table-count pg::*live-operation-parked*))))))
                         (sb-thread:signal-semaphore release-owner)
                         (parking-test-wait cleanup-ready)
                         (setf cleanup-blocked-fence
                               (and (= 1 pg::*live-operation-count*)
                                    (zerop (pg::live-operation-parked-count))
                                    (zerop (sb-thread:semaphore-count fence-done))))
                         (sb-thread:signal-semaphore release-cleanup))
                       "parking shutdown controller"))
                (ok (handler-case
                        (pg::with-live-operation
                          (unwind-protect
                               (progn
                                 (pg::call-with-live-operation-parked
                                  (lambda ()
                                    (setf owner
                                          (parking-test-spawn
                                           (lambda ()
                                             (pg::with-live-operation-quiescence
                                               (sb-thread:signal-semaphore owner-ready)
                                               (parking-test-wait release-owner)))
                                           "parking foreign storage owner"))
                                    (parking-test-wait owner-ready)
                                    (setf finalizer
                                          (parking-test-spawn
                                           (lambda ()
                                             (pg::fence-live-operation-shutdown)
                                             (sb-thread:signal-semaphore fence-done))
                                           "parking shutdown fence"))
                                    (pg::call-with-live-operation-lock
                                     (lambda ()
                                       (loop until pg::*live-operation-terminal-shutdown*
                                             do (sb-thread:condition-wait
                                                 pg::*live-operation-cv* pg::*live-operation-lock*))))))
                                 (setf continued t))
                            (setf cleanup-owned (= 1 pg::*live-operation-count*))
                            (sb-thread:signal-semaphore cleanup-ready)
                            (parking-test-wait release-cleanup)))
                      (pg::pg-error (condition) (pg::fatal-error-p condition))))
                (parking-test-wait fence-done)
                (parking-test-join owner)
                (parking-test-join controller)
                (parking-test-join finalizer)
                (ok observed)
                (ok cleanup-owned)
                (ok cleanup-blocked-fence)
                (ok (not continued))
                (ok (zerop pg::*live-operation-count*))
                (ok (zerop (pg::live-operation-parked-count))))
           (sb-thread:signal-semaphore release-owner)
           (sb-thread:signal-semaphore restoring)
           (sb-thread:signal-semaphore cleanup-ready)
           (sb-thread:signal-semaphore release-cleanup))))))

(deftest repeated-fatal-interrupts-during-restoration-do-not-lose-admission
    (call-with-parking-test-state
     (lambda ()
       (let* ((begin-owner (sb-thread:make-semaphore))
              (owner-ready (sb-thread:make-semaphore))
              (release-owner (sb-thread:make-semaphore))
              (restoring (sb-thread:make-semaphore))
              (backend pg::*backend*)
              (fatal-count 0) (first-credit nil) (cleanup-owned nil) (continued nil)
              (sql-thread nil)
              (owner
                (parking-test-spawn
                 (lambda ()
                   (parking-test-wait begin-owner)
                   (pg::with-live-operation-quiescence
                     (sb-thread:signal-semaphore owner-ready)
                     (parking-test-wait release-owner)))
                 "parking interrupted owner")))
         (setf pg::*live-operation-parking-test-hook*
               (lambda (point token)
                 (declare (ignore token))
                 (when (eq point :before-restore) (sb-thread:signal-semaphore restoring))))
         (unwind-protect
              (progn
                (setf sql-thread
                      (parking-test-spawn
                       (lambda ()
                         (let ((pg::*backend* backend))
                           (sb-sys:without-interrupts
                             (handler-case
                                 (sb-sys:allow-with-interrupts
                                   (pg::with-live-operation
                                     (unwind-protect
                                          (progn
                                            (pg::call-with-live-operation-parked
                                             (lambda ()
                                               (sb-thread:signal-semaphore begin-owner)
                                               (parking-test-wait owner-ready)))
                                            (setf continued t))
                                       (setf cleanup-owned (= 1 pg::*live-operation-count*)))))
                               (pg::pg-error (condition)
                                 (unless (pg::fatal-error-p condition) (error condition))))
                             (loop until (= fatal-count 2)
                                   do (handler-case
                                          (sb-sys:allow-with-interrupts
                                            (sb-sys:with-local-interrupts (sb-thread:thread-yield)))
                                        (pg::pg-error (condition)
                                          (unless (pg::fatal-error-p condition) (error condition))))))))
                       "parking interrupted SQL"))
                (parking-test-wait restoring)
                (dotimes (index 2)
                  (sb-thread:interrupt-thread
                   sql-thread
                   (lambda ()
                     (when (zerop fatal-count) (setf first-credit pg::*live-operation-count*))
                     (incf fatal-count)
                     (pg::ereport-fatal "57P01" "injected repeated termination"))))
                (sb-thread:signal-semaphore release-owner)
                (parking-test-join sql-thread)
                (parking-test-join owner)
                (ok (= 2 fatal-count))
                (ok (= 1 first-credit))
                (ok cleanup-owned)
                (ok (not continued))
                (ok (zerop pg::*live-operation-count*))
                (ok (zerop (pg::live-operation-parked-count))))
           (sb-thread:signal-semaphore begin-owner)
           (sb-thread:signal-semaphore release-owner))))))

(deftest expired-synchronous-deadline-is-reported-after-admission-restoration
    (call-with-parking-test-state
     (lambda ()
       (let* ((begin-owner (sb-thread:make-semaphore))
              (owner-ready (sb-thread:make-semaphore))
              (release-owner (sb-thread:make-semaphore))
              (restoring (sb-thread:make-semaphore))
              (cleanup-owned nil) (caught nil)
              (owner
                (parking-test-spawn
                 (lambda ()
                   (parking-test-wait begin-owner)
                   (pg::with-live-operation-quiescence
                     (sb-thread:signal-semaphore owner-ready)
                     (parking-test-wait release-owner)))
                 "parking deadline owner"))
              (controller
                (parking-test-spawn
                 (lambda ()
                   (parking-test-wait restoring)
                   (sleep 0.3)
                   (sb-thread:signal-semaphore release-owner))
                 "parking deadline controller")))
         (setf pg::*live-operation-parking-test-hook*
               (lambda (point token)
                 (declare (ignore token))
                 (when (eq point :before-restore) (sb-thread:signal-semaphore restoring))))
         (unwind-protect
              (progn
                (handler-case
                    (pg::with-live-operation
                      (unwind-protect
                           (sb-sys:with-deadline (:seconds 0.2)
                             (pg::call-with-live-operation-parked
                              (lambda ()
                                (sb-thread:signal-semaphore begin-owner)
                                (parking-test-wait owner-ready))))
                        (setf cleanup-owned (= 1 pg::*live-operation-count*))))
                  (sb-sys:deadline-timeout () (setf caught t)))
                (parking-test-join controller)
                (parking-test-join owner)
                (ok caught)
                (ok cleanup-owned)
                (ok (zerop pg::*live-operation-count*))
                (ok (zerop (pg::live-operation-parked-count))))
           (sb-thread:signal-semaphore begin-owner)
           (sb-thread:signal-semaphore release-owner)
           (sb-thread:signal-semaphore restoring))))))
