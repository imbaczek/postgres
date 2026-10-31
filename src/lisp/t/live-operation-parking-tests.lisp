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
                 (cons 'pg::*reload-requests* 0)
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
            (ok (pg::reload-request-p pg::+reload-request-code+))
            (ok (pg::reload-request-p pg::+reload-request-configuration+))))))))

(deftest reload-request-consumers-preserve-other-work-and-new-arrivals
    (dolist (order (list (list pg::+reload-request-configuration+ pg::+reload-request-code+)
                         (list pg::+reload-request-code+ pg::+reload-request-configuration+)))
      (let ((pg::*reload-requests* 0))
        (destructuring-bind (first second) order
          (pg::request-reload)
          (ok (pg::claim-reload-request first))
          (ok (not (pg::reload-request-p first)))
          (ok (pg::reload-request-p second))
          (pg::request-reload)
          (ok (pg::claim-reload-request second))
          (ok (pg::reload-request-p first))
          (ok (not (pg::reload-request-p second)))
          (ok (pg::claim-reload-request first))
          (ok (zerop pg::*reload-requests*))
          (setf pg::*reload-requests* pg::+reload-service-stopped+)
          (ok (not (pg::request-reload)))
          (ok (not (pg::request-configuration-reload)))
          (ok (not (pg::claim-reload-request first)))
          (ok (= pg::+reload-service-stopped+ pg::*reload-requests*))))))

(deftest concurrent-reload-consumers-do-not-overwrite-each-others-claims
    (call-with-parking-test-state
     (lambda ()
       (let ((configuration (sb-thread:make-semaphore :count 0))
             (code (sb-thread:make-semaphore :count 0))
             (done (sb-thread:make-semaphore :count 0))
             (configuration-count 0) (code-count 0))
         (parking-test-spawn
          (lambda ()
            (dotimes (round 50)
              (parking-test-wait configuration)
              (when (pg::claim-reload-request pg::+reload-request-configuration+)
                (incf configuration-count))
              (sb-thread:signal-semaphore done)))
          "configuration request consumer")
         (parking-test-spawn
          (lambda ()
            (dotimes (round 50)
              (parking-test-wait code)
              (when (pg::claim-reload-request pg::+reload-request-code+)
                (incf code-count))
              (sb-thread:signal-semaphore done)))
          "code request consumer")
         (dotimes (round 50)
           (pg::request-reload)
           (sb-thread:signal-semaphore configuration)
           (sb-thread:signal-semaphore code)
           (parking-test-wait done)
           (parking-test-wait done)
           (ok (zerop pg::*reload-requests*)))
         (ok (= 50 configuration-count code-count))))))

(deftest timed-reload-wait-reacquires-the-mutex-before-returning
    (let ((mutex (sb-thread:make-mutex :name "timed reload wait"))
          (queue (sb-thread:make-waitqueue))
          (ownership nil))
      (sb-thread:with-mutex (mutex)
        (pg::wait-for-reload-condition
         queue mutex (lambda () (push (sb-thread:holding-mutex-p mutex) ownership)))
        (ok (sb-thread:holding-mutex-p mutex)))
      (ok (equal '(t nil t) (nreverse ownership)))))

(defun exercise-cancellable-code-drain (kind)
  (call-with-parking-test-state
   (lambda ()
     (let ((entered (sb-thread:make-semaphore :count 0))
           (entered-p nil) (ran nil) (stopped nil) (thread nil))
       (labels ((exercise ()
                  (let ((prior-owner pg::*live-operation-quiescence-owner*))
                    (unwind-protect
                         (progn
                           (setf thread
                                 (parking-test-spawn
                                  (lambda ()
                                    (let ((pg::*live-operation-drain-cancel-check*
                                            (lambda ()
                                              (when
                                                  (and (not entered-p)
                                                       (case kind
                                                         (:active
                                                          (and (pg::live-operation-lock-owned-p)
                                                               (plusp pg::*live-operation-count*)))
                                                         (:gate
                                                          (and (pg::live-operation-lock-owned-p)
                                                               pg::*live-operation-quiescence-owner*
                                                               (not (eq pg::*live-operation-quiescence-owner-thread*
                                                                        sb-thread:*current-thread*))))
                                                         (:parked
                                                          (and (pg::live-operation-lock-owned-p)
                                                               (null pg::*live-operation-quiescence-owner*)
                                                               (plusp (hash-table-count pg::*live-operation-parked*))))
                                                         (:owner
                                                          (sb-thread:holding-mutex-p
                                                           pg::*query-execution-owner-lock*))))
                                                (setf entered-p t)
                                                (sb-thread:signal-semaphore entered))
                                              (pg::check-reload-service-running))))
                                      (handler-case
                                          (pg::call-with-live-operation-reload (lambda () (setf ran t)))
                                        (pg::reload-service-stopped () (setf stopped t)))))
                                  "cancellable code drain"))
                           (parking-test-wait entered)
                           (setf pg::*reload-requests* pg::+reload-service-stopped+)
                           (parking-test-join thread)
                           (ok stopped)
                           (ok (not ran))
                           (if (eq kind :gate)
                               (progn
                                 (ok (eq prior-owner pg::*live-operation-quiescence-owner*))
                                 (ok (not pg::*live-operation-admission-open*)))
                               (progn
                                 (ok (null pg::*live-operation-quiescence-owner*))
                                 (ok pg::*live-operation-admission-open*))))
                      (setf pg::*reload-requests* pg::+reload-service-stopped+)
                      (when thread (parking-test-join thread))))))
         (ecase kind
           (:active (pg::with-live-operation (exercise) (ok (= 1 pg::*live-operation-count*))))
           (:gate (pg::with-live-operation-quiescence (exercise)))
           (:parked
            (pg::with-live-operation
              (pg::call-with-live-operation-parked
               (lambda () (exercise) (ok (= 1 (pg::live-operation-parked-count)))))))
           (:owner
            (let ((owner (pg::make-query-execution-owner)))
              (unwind-protect
                   (progn (exercise) (ok (eq :open (pg::query-execution-owner-state owner))))
                (pg::close-query-execution-owner owner))))))))))

(deftest stopped-code-reload-cancels-each-drain-without-stealing-another-gate
    (dolist (kind '(:active :gate :parked :owner))
      (exercise-cancellable-code-drain kind)))

(defun call-with-reload-worker-test-state (function replacements)
  (call-with-parking-test-state
   (lambda ()
     (let* ((settings
              (list (cons 'pg::*reload-definition-lock* (sb-thread:make-mutex :name "reload test definitions"))
                    (cons 'pg::*reload-service-failure* nil)
                    (cons 'pg::*reload-configuration-worker* nil)
                    (cons 'pg::*reload-code-worker* nil)
                    (cons 'pg::*reload-worker-wake* (sb-thread:make-semaphore :count 0))
                    (cons 'pg::*shutdown* pg::+no-shutdown+)))
            (saved (mapcar (lambda (entry) (cons (car entry) (symbol-value (car entry)))) settings))
            (functions (mapcar (lambda (entry) (cons (car entry) (symbol-function (car entry))))
                               replacements)))
       (unwind-protect
            (progn
              (dolist (entry settings) (set (car entry) (cdr entry)))
              (dolist (entry replacements) (setf (symbol-function (car entry)) (cdr entry)))
              (funcall function))
         (unwind-protect
              (pg::stop-reload-workers)
           (dolist (entry functions) (setf (symbol-function (car entry)) (cdr entry)))
           (dolist (entry saved) (set (car entry) (cdr entry)))))))))

(deftest configuration-worker-handles-repeated-requests-while-code-drain-waits
    (dolist (smart-shutdown '(nil t))
      (let ((holder-ready (sb-thread:make-semaphore :count 0))
            (waiter-ready (sb-thread:make-semaphore :count 0))
            (release-holder (sb-thread:make-semaphore :count 0))
            (configuration-ready (sb-thread:make-semaphore :count 0))
            (code-ready (sb-thread:make-semaphore :count 0))
            (data-lock (sb-thread:make-mutex :name "reload held data lock"))
            (configuration-observations nil) (code-observations nil)
            (holder nil) (waiter nil))
        (call-with-reload-worker-test-state
         (lambda ()
           (unwind-protect
                (progn
                  (setf holder
                        (parking-test-spawn
                         (lambda ()
                           (let ((pg::*backend* (pg::make-backend :pid 11 :state :in-progress)))
                             (pg::with-live-operation
                               (sb-thread:with-mutex (data-lock)
                                 (sb-thread:signal-semaphore holder-ready)
                                 (parking-test-wait release-holder)))))
                         "reload lock holder"))
                  (parking-test-wait holder-ready)
                  (setf waiter
                        (parking-test-spawn
                         (lambda ()
                           (let ((pg::*backend* (pg::make-backend :pid 12 :state :in-progress)))
                             (pg::with-live-operation
                               (sb-thread:signal-semaphore waiter-ready)
                               (sb-thread:with-mutex (data-lock) (values)))))
                         "reload dependent waiter"))
                  (parking-test-wait waiter-ready)
                  (when smart-shutdown (setf pg::*shutdown* pg::+smart-shutdown+))
                  (pg::start-reload-workers)
                  (let ((configuration pg::*reload-configuration-worker*) (code pg::*reload-code-worker*))
                    (pg::start-reload-workers)
                    (ok (eq configuration pg::*reload-configuration-worker*))
                    (ok (eq code pg::*reload-code-worker*)))
                  (dotimes (request 2)
                    (pg::request-reload)
                    (parking-test-wait configuration-ready)
                    (ok (null code-observations))
                    (ok (= 2 pg::*live-operation-count*)))
                  (ok (equal '((t 0 nil) (t 0 nil)) configuration-observations))
                  (sb-thread:signal-semaphore release-holder)
                  (parking-test-join holder)
                  (parking-test-join waiter)
                  (parking-test-wait code-ready)
                  (ok (every (lambda (entry) (equal '(t t t 0) entry)) code-observations))
                  (pg::stop-reload-workers)
                  (ok (pg::reload-service-stopped-p))
                  (ok (not (sb-thread:thread-alive-p pg::*reload-configuration-worker*)))
                  (ok (not (sb-thread:thread-alive-p pg::*reload-code-worker*)))
                  (ok (not (pg::request-reload))))
             (sb-thread:signal-semaphore release-holder)
             (when holder (parking-test-join holder))
             (when waiter (parking-test-join waiter))))
         (list
          (cons 'pg::startup-prepare-reload-installation
                (lambda ()
                  (unless (sb-thread:holding-mutex-p pg::*reload-run-lock*)
                    (push (list (sb-thread:holding-mutex-p pg::*reload-definition-lock*)
                                pg::*live-operation-depth*
                                (sb-thread:holding-mutex-p pg::*reload-run-lock*))
                          configuration-observations)
                    (sb-thread:signal-semaphore configuration-ready))
                  (values nil nil)))
          (cons 'asdf:load-system
                (lambda (&rest arguments)
                  (declare (ignore arguments))
                  (push (list (sb-thread:holding-mutex-p pg::*reload-definition-lock*)
                              (sb-thread:holding-mutex-p pg::*reload-run-lock*)
                              (eq pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
                              pg::*live-operation-count*)
                        code-observations)
                  (sb-thread:signal-semaphore code-ready)
                  t)))))))

(deftest configuration-retries-reprepare-after-root-source-and-desired-changes
    (let ((reads 0) (attempts nil) (published nil))
      (call-with-reload-worker-test-state
       (lambda ()
         (ok (pg::configuration-reload-step))
         (ok (= 4 reads (length attempts)))
         (ok (= 4 (length (remove-duplicates attempts :test #'eq))))
         (ok (eq published (first attempts))))
       (list
        (cons 'pg::startup-prepare-reload-installation
              (lambda ()
                (incf reads)
                (values (list :fresh-read reads) nil)))
        (cons 'pg::startup-commit-installation
              (lambda (candidate)
                (push candidate attempts)
                (case (length attempts)
                  (1 (error 'pg::startup-configuration-changed :format-control "root changed"))
                  (2 (error 'pg::startup-configuration-changed :format-control "source changed"))
                  (3 (error 'pg::physical-follow-generation-changed))
                  (t (setf published candidate)))))))))

(deftest code-reload-uses-the-preparer-loaded-after-definition-exclusion
    (let ((reads nil) (locked t))
      (call-with-reload-worker-test-state
       (lambda ()
         (ok (pg::configuration-reload-step))
         (ok (pg::reload))
         (ok locked)
         (ok (equal '(:new :old) reads)))
       (list
        (cons 'pg::startup-prepare-reload-installation
              (lambda () (push :old reads) (values nil nil)))
        (cons 'asdf:load-system
              (lambda (&rest arguments)
                (declare (ignore arguments))
                (setf locked (and locked (sb-thread:holding-mutex-p pg::*reload-definition-lock*)))
                (setf (symbol-function 'pg::startup-prepare-reload-installation)
                      (lambda ()
                        (setf locked (and locked (sb-thread:holding-mutex-p pg::*reload-definition-lock*)))
                        (push :new reads)
                        (values nil nil)))
                t))))))

(deftest invalid-configuration-is-consumed-once-and-a-new-request-can-recover
    (let ((invalid (sb-thread:make-semaphore :count 0))
          (valid (sb-thread:make-semaphore :count 0))
          (reject t) (reads 0) (logs 0) (loads 0))
      (call-with-reload-worker-test-state
       (lambda ()
         (let ((root (pg::startup-capture-configuration)) (desired pg::*physical-follow-desired*))
           (pg::start-reload-workers)
           (pg::request-configuration-reload)
           (parking-test-wait invalid)
           (ok (not (sb-thread:wait-on-semaphore invalid :timeout 0.15)))
           (ok (= 1 reads logs))
           (ok (eq root (pg::startup-capture-configuration)))
           (ok (eq desired pg::*physical-follow-desired*))
           (setf reject nil)
           (pg::request-configuration-reload)
           (parking-test-wait valid)
           (ok (= 2 reads))
           (ok (= 1 logs))
           (ok (zerop loads))))
       (list
        (cons 'pg::startup-prepare-reload-installation
              (lambda ()
                (incf reads)
                (when reject (error "invalid HBA or setting"))
                (sb-thread:signal-semaphore valid)
                (values nil nil)))
        (cons 'pg::admin-log
              (lambda (&rest arguments)
                (declare (ignore arguments))
                (incf logs)
                (sb-thread:signal-semaphore invalid)))
        (cons 'asdf:load-system
              (lambda (&rest arguments) (declare (ignore arguments)) (incf loads) t))))))

(deftest reload-stop-waits-for-an-attempt-before-fencing-later-publication
    (let ((prepared (sb-thread:make-semaphore :count 0))
          (release (sb-thread:make-semaphore :count 0))
          (reads 0) (commits 0) (stopper nil) (waiting nil))
      (call-with-reload-worker-test-state
       (lambda ()
         (unwind-protect
              (progn
                (pg::start-reload-workers)
                (pg::request-configuration-reload)
                (parking-test-wait prepared)
                (setf stopper (parking-test-spawn #'pg::stop-reload-workers "reload stop fence"))
                (loop repeat 1000
                      when (eq pg::*reload-definition-lock* (sb-thread::thread-waiting-for stopper))
                        do (setf waiting t) (return)
                      do (sleep 0.001))
                (ok waiting)
                (ok (not (pg::reload-service-stopped-p)))
                (sb-thread:signal-semaphore release)
                (parking-test-join stopper)
                (ok (= 1 reads commits))
                (ok (pg::reload-service-stopped-p))
                (ok (not (pg::request-reload)))
                (ok (not (sb-thread:thread-alive-p pg::*reload-configuration-worker*)))
                (ok (not (sb-thread:thread-alive-p pg::*reload-code-worker*))))
           (sb-thread:signal-semaphore release)
           (when stopper (parking-test-join stopper))))
       (list
        (cons 'pg::startup-prepare-reload-installation
              (lambda ()
                (incf reads)
                (sb-thread:signal-semaphore prepared)
                (parking-test-wait release)
                (values (list :candidate reads) nil)))
        (cons 'pg::startup-commit-installation
              (lambda (candidate)
                (when (pg::reload-service-stopped-p) (error "publication crossed stop fence"))
                (incf commits)
                candidate))))))

(deftest reload-worker-failure-stops-the-service-instead-of-accepting-lost-requests
    (dolist (mode '(:logger :nonlocal))
      (let ((stopped nil))
        (call-with-reload-worker-test-state
         (lambda ()
           (pg::start-reload-workers)
           (pg::request-configuration-reload)
           (loop repeat 1000
                 when (pg::reload-service-stopped-p) do (setf stopped t) (return)
                   do (sleep 0.001))
           (ok stopped)
           (pg::stop-reload-workers)
           (if (eq mode :logger)
               (progn
                 (ok (typep pg::*reload-service-failure* 'error))
                 (ok (search "logger failed" (princ-to-string pg::*reload-service-failure*))))
               (ok (eq :abnormal-exit pg::*reload-service-failure*)))
           (ok (not (pg::request-reload)))
           (ok (not (pg::request-configuration-reload)))
           (ok (not (sb-thread:thread-alive-p pg::*reload-configuration-worker*)))
           (ok (not (sb-thread:thread-alive-p pg::*reload-code-worker*))))
         (list
          (cons 'pg::startup-prepare-reload-installation
                (lambda ()
                  (if (eq mode :nonlocal)
                      (sb-thread:return-from-thread nil)
                      (error "invalid configuration"))))
          (cons 'pg::admin-log
                (lambda (&rest arguments)
                  (declare (ignore arguments)) (error "logger failed"))))))))

(deftest reload-join-timeout-prevents-storage-finalization-and-late-asdf
    (let ((owned (sb-thread:make-semaphore :count 0))
          (release (sb-thread:make-semaphore :count 0))
          (owner nil) (waiting nil) (storage-calls 0) (loads 0))
      (call-with-reload-worker-test-state
       (lambda ()
         (unwind-protect
              (progn
                (setf owner
                      (parking-test-spawn
                       (lambda ()
                         (sb-thread:with-mutex (pg::*reload-run-lock*)
                           (sb-thread:signal-semaphore owned)
                           (parking-test-wait release)))
                       "reload run-lock owner"))
                (parking-test-wait owned)
                (pg::start-reload-workers)
                (pg::request-reload-bits pg::+reload-request-code+)
                (loop repeat 1000
                      when (eq pg::*reload-run-lock*
                               (sb-thread::thread-waiting-for pg::*reload-code-worker*))
                        do (setf waiting t) (return)
                      do (sleep 0.001))
                (ok waiting)
                (let ((pg::*reload-worker-join-timeout* 0.05)
                      (pg::*shutdown-finalizing-p* t) (pg::*shutdown-finished* nil)
                      (pg::*physical-recovery-state* nil))
                  (ok (handler-case (progn (pg::prepare-postmaster-shutdown) nil)
                        (error (condition)
                          (search "before storage finalization" (princ-to-string condition)))))
                  (ok (not pg::*shutdown-finished*)))
                (ok (pg::reload-service-stopped-p))
                (ok (= 0 storage-calls loads))
                (sb-thread:signal-semaphore release)
                (parking-test-join owner)
                (pg::stop-reload-workers)
                (ok (= 0 storage-calls loads)))
           (sb-thread:signal-semaphore release)
           (when owner (parking-test-join owner))))
       (list
        (cons 'asdf:load-system
              (lambda (&rest arguments) (declare (ignore arguments)) (incf loads) t))
        (cons 'pg::startup-prepare-reload-installation (lambda () (values nil nil)))
        (cons 'pg::managed-worker-shutdown (lambda () (incf storage-calls)))
        (cons 'pg::physical-follow-controller-stop-worker (lambda () (incf storage-calls)))
        (cons 'pg::prepare-primary-shutdown (lambda () (incf storage-calls)))
        (cons 'pg::physical-recovery-prepare-shutdown (lambda () (incf storage-calls)))))))

(deftest reload-start-preserves-pending-work-and-publishes-both-handles-first
    (let ((configuration (sb-thread:make-semaphore :count 0))
          (code (sb-thread:make-semaphore :count 0))
          (configuration-owned nil) (code-owned nil))
      (call-with-reload-worker-test-state
       (lambda ()
         (pg::request-reload)
         (pg::start-reload-workers)
         (parking-test-wait configuration)
         (parking-test-wait code)
         (ok configuration-owned)
         (ok code-owned)
         (ok (zerop pg::*reload-requests*))
         (pg::stop-reload-workers)
         (ok (null pg::*reload-service-failure*)))
       (list
        (cons 'pg::startup-prepare-reload-installation
              (lambda ()
                (unless (sb-thread:holding-mutex-p pg::*reload-run-lock*)
                  (setf configuration-owned
                        (and (eq pg::*reload-configuration-worker* sb-thread:*current-thread*)
                             pg::*reload-code-worker*
                             (sb-thread:thread-alive-p pg::*reload-code-worker*)))
                  (sb-thread:signal-semaphore configuration))
                (values nil nil)))
        (cons 'asdf:load-system
              (lambda (&rest arguments)
                (declare (ignore arguments))
                (setf code-owned
                      (and (eq pg::*reload-code-worker* sb-thread:*current-thread*)
                           pg::*reload-configuration-worker*
                           (sb-thread:thread-alive-p pg::*reload-configuration-worker*)))
                (sb-thread:signal-semaphore code)
                t))))))

(deftest failed-reload-worker-start-releases-its-gated-child-and-rejects-requests
    (call-with-reload-worker-test-state
     (lambda ()
       (let ((tag (gensym "RELOAD-START-")) (target sb-thread:*current-thread*))
         (unwind-protect
              (progn
                (sb-int:encapsulate
                 'sb-thread:make-thread tag
                 (lambda (original &rest arguments)
                   (when (and (eq target sb-thread:*current-thread*)
                              (equal "pg code reloader" (getf (rest arguments) :name)))
                     (error "second reload worker creation failed"))
                   (apply original arguments)))
                (ok (intake-fails-p #'pg::start-reload-workers))
                (ok (pg::reload-service-stopped-p))
                (ok pg::*reload-configuration-worker*)
                (ok (null pg::*reload-code-worker*))
                (ok (not (pg::request-reload))))
           (sb-int:unencapsulate 'sb-thread:make-thread tag)))
       (pg::stop-reload-workers)
       (ok (not (sb-thread:thread-alive-p pg::*reload-configuration-worker*)))
       (ok (intake-fails-p #'pg::start-reload-workers)))
     nil))

(deftest configuration-preparation-keeps-asdf-outside-its-definition-lock
    (let ((prepared (sb-thread:make-semaphore :count 0))
          (release (sb-thread:make-semaphore :count 0))
          (loaded (sb-thread:make-semaphore :count 0))
          (waiting nil) (loads 0))
      (call-with-reload-worker-test-state
       (lambda ()
         (unwind-protect
              (progn
                (pg::start-reload-workers)
                (pg::request-configuration-reload)
                (parking-test-wait prepared)
                (pg::request-reload-bits pg::+reload-request-code+)
                (loop repeat 1000
                      when (eq pg::*reload-definition-lock*
                               (sb-thread::thread-waiting-for pg::*reload-code-worker*))
                        do (setf waiting t) (return)
                      do (sleep 0.001))
                (ok waiting)
                (ok (zerop loads))
                (sb-thread:signal-semaphore release)
                (parking-test-wait loaded)
                (ok (= 1 loads)))
           (sb-thread:signal-semaphore release)))
       (list
        (cons 'pg::startup-prepare-reload-installation
              (lambda ()
                (unless (sb-thread:holding-mutex-p pg::*reload-run-lock*)
                  (sb-thread:signal-semaphore prepared)
                  (parking-test-wait release))
                (values nil nil)))
        (cons 'asdf:load-system
              (lambda (&rest arguments)
                (declare (ignore arguments))
                (incf loads)
                (sb-thread:signal-semaphore loaded)
                t))))))

(deftest retained-query-owner-allows-repeated-configuration-before-code-reload
    (let ((configuration (sb-thread:make-semaphore :count 0))
          (waiting (sb-thread:make-semaphore :count 0))
          (loaded (sb-thread:make-semaphore :count 0))
          (hook pg::*query-execution-owner-reload-wait-hook*)
          (loads 0) (reads 0))
      (unwind-protect
           (progn
             (setf pg::*query-execution-owner-reload-wait-hook*
                   (lambda (generation) (declare (ignore generation))
                     (sb-thread:signal-semaphore waiting)))
             (call-with-reload-worker-test-state
              (lambda ()
                (let ((owner (pg::make-query-execution-owner)))
                  (unwind-protect
                       (progn
                         (pg::start-reload-workers)
                         (pg::request-reload)
                         (parking-test-wait configuration)
                         (parking-test-wait waiting)
                         (ok pg::*live-operation-admission-open*)
                         (ok (zerop loads))
                         (pg::request-reload)
                         (parking-test-wait configuration)
                         (ok (= 2 reads))
                         (ok (zerop loads))
                         (ok (eq :open (pg::query-execution-owner-state owner)))
                         (ok pg::*live-operation-admission-open*)
                         (pg::close-query-execution-owner owner)
                         (parking-test-wait loaded)
                         (ok (plusp loads)))
                    (pg::close-query-execution-owner owner))))
              (list
               (cons 'pg::startup-prepare-reload-installation
                     (lambda ()
                       (unless (sb-thread:holding-mutex-p pg::*reload-run-lock*)
                         (incf reads)
                         (sb-thread:signal-semaphore configuration))
                       (values nil nil)))
               (cons 'asdf:load-system
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (incf loads)
                       (sb-thread:signal-semaphore loaded)
                       t)))))
        (setf pg::*query-execution-owner-reload-wait-hook* hook))))

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
