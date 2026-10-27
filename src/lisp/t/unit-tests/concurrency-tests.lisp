;;;; concurrency-tests.lisp -- live-operation and query lifecycle tests.

(in-package #:pg)

(defun test-exported-snapshot-schema-lock-coordination ()
  (let* ((test-lock (sb-thread:make-mutex :name "test schema lock state"))
         (test-cv (sb-thread:make-waitqueue :name "test schema lock state"))
         (writer-entered nil)
         (release-writer nil)
         (reader-entered nil)
         (writer nil)
         (reader nil)
         (worker-error nil))
    (unwind-protect
         (progn
           (with-schema-read-lock
             (setf writer
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (with-schema-write-lock
                            (sb-thread:with-mutex (test-lock)
                              (setf writer-entered t)
                              (sb-thread:condition-broadcast test-cv)
                              (loop until release-writer
                                    do (sb-thread:condition-wait
                                        test-cv test-lock))))
                        (error (condition)
                          (setf worker-error condition)))
                      (values))
                    :name "test schema writer"))
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*schema-lock-mutex*)
                         (plusp *schema-write-lock-waiters*))))
                    "schema writer queues behind an exported snapshot reader")
             (setf reader
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (with-schema-read-lock
                            (sb-thread:with-mutex (test-lock)
                              (setf reader-entered t)
                              (sb-thread:condition-broadcast test-cv)))
                        (error (condition)
                          (setf worker-error condition)))
                      (values))
                    :name "test later schema reader"))
             (check (not (sb-thread:with-mutex (test-lock) writer-entered))
                    "schema writer remains blocked for the snapshot lifetime")
             (check (not (sb-thread:with-mutex (test-lock) reader-entered))
                    "queued schema writer prevents later readers from starving it"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) writer-entered)))
                  "schema writer proceeds when the exported snapshot releases")
           (check (not (sb-thread:with-mutex (test-lock) reader-entered))
                  "schema writer runs before readers queued behind it")
           (sb-thread:with-mutex (test-lock)
             (setf release-writer t)
             (sb-thread:condition-broadcast test-cv))
           (when writer
             (sb-thread:join-thread writer)
             (setf writer nil))
           (when reader
             (sb-thread:join-thread reader)
             (setf reader nil))
           (check (sb-thread:with-mutex (test-lock) reader-entered)
                  "queued schema reader proceeds after the DDL writer")
           (check (null worker-error)
                  "schema lock waiters finish without errors"))
      (sb-thread:with-mutex (test-lock)
        (setf release-writer t)
        (sb-thread:condition-broadcast test-cv))
      (when writer (sb-thread:join-thread writer))
      (when reader (sb-thread:join-thread reader)))))

(defun test-schema-lock-nesting-and-read-upgrade-rejection ()
  (check (with-schema-write-lock
           (with-schema-write-lock
             (with-schema-read-lock t)))
         "schema write lock nests without reacquiring the gate")
  (let ((original-check (symbol-function 'check-for-interrupts)))
    (unwind-protect
         (progn
           (setf (symbol-function 'check-for-interrupts)
                 (lambda () (error "nested schema read tried to wait")))
           (check (handler-case
                      (progn
                        (with-schema-read-lock
                          (let ((*schema-write-lock-waiters* 1))
                            (with-schema-read-lock t)))
                        t)
                    (error () nil))
                  "nested schema readers do not wait behind queued writers"))
      (setf (symbol-function 'check-for-interrupts) original-check)))
  (check (handler-case
             (progn
               (with-schema-read-lock
                 (with-schema-write-lock t))
               nil)
           (error () t))
         "schema read scope rejects a same-thread write-lock upgrade")
  (sb-thread:with-mutex (*schema-lock-mutex*)
    (check (and (zerop *schema-read-lock-count*)
                (null *schema-write-lock-owners*)
                (zerop *schema-write-lock-waiters*))
           "schema lock macros release all counters and ownership on errors")))

(defun test-schema-gate-allows-independent-writers ()
  (let ((lock (sb-thread:make-mutex :name "test schema cohorts"))
        (cv (sb-thread:make-waitqueue :name "test schema cohorts"))
        (other-entered nil)
        (release-other nil)
        (reader-waiting nil)
        (reader-entered nil)
        (worker-error nil)
        (other nil)
        (reader nil)
        (original-wait (symbol-function 'condition-wait-with-timeout)))
    (unwind-protect
         (progn
           (setf (symbol-function 'condition-wait-with-timeout)
                 (lambda (waitqueue mutex)
                   (when (eq waitqueue *schema-lock-cv*)
                     (sb-thread:with-mutex (lock) (setf reader-waiting t)))
                   (funcall original-wait waitqueue mutex)))
           (with-schema-write-lock
             (setf other
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (with-schema-write-lock
                            (sb-thread:with-mutex (lock)
                              (setf other-entered t)
                              (loop until release-other
                                    do (sb-thread:condition-wait cv lock))))
                        (error (condition)
                          (sb-thread:with-mutex (lock)
                            (setf worker-error condition)))))))
             (check (test-wait-until
                     (lambda () (sb-thread:with-mutex (lock) other-entered)))
                    "independent DDL can enter while another schema changer is active")
             (setf reader
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (with-schema-read-lock
                            (sb-thread:with-mutex (lock) (setf reader-entered t)))
                        (error (condition)
                          (sb-thread:with-mutex (lock)
                            (setf worker-error condition)))))))
             (check (test-wait-until
                     (lambda () (sb-thread:with-mutex (lock) reader-waiting)))
                    "exported snapshot waits behind active schema changers"))
           (check (sb-thread:with-mutex (*schema-lock-mutex*)
                    (= (length *schema-write-lock-owners*) 1))
                  "releasing one DDL token retains the other schema hold")
           (check (not (sb-thread:with-mutex (lock) reader-entered))
                  "exported snapshot cannot enter before every schema changer exits")
           (sb-thread:with-mutex (lock)
             (setf release-other t)
             (sb-thread:condition-broadcast cv))
           (sb-thread:join-thread other)
           (setf other nil)
           (sb-thread:join-thread reader)
           (setf reader nil)
           (check (and (sb-thread:with-mutex (lock) reader-entered)
                       (null worker-error))
                  "exported snapshot proceeds after the final schema changer exits"))
      (sb-thread:with-mutex (lock)
        (setf release-other t)
        (sb-thread:condition-broadcast cv))
      (when other (sb-thread:join-thread other))
      (when reader (sb-thread:join-thread reader))
      (setf (symbol-function 'condition-wait-with-timeout) original-wait))))

(defun test-schema-lock-acquisition-interruption ()
  (let ((*schema-lock-mutex* (sb-thread:make-mutex :name "test schema lock"))
        (*schema-lock-cv* (sb-thread:make-waitqueue :name "test schema waiters"))
        (*schema-read-lock-count* 0)
        (*schema-write-lock-owners* nil)
        (*schema-write-lock-waiters* 0)
        (*schema-read-lock-held* nil)
        (*schema-write-lock-held* nil)
        (original-check (symbol-function 'check-for-interrupts)))
    (unwind-protect
         (progn
           (setf (symbol-function 'check-for-interrupts)
                 (lambda () (error "injected schema-lock cancellation")))
           (setf *schema-write-lock-owners* (list :held))
           (check (handler-case
                      (progn (with-schema-read-lock t) nil)
                    (error () t))
                  "reader cancellation interrupts while waiting behind a writer")
           (check (zerop *schema-read-lock-count*)
                  "cancelled schema reader never publishes an orphaned hold")
           (setf *schema-write-lock-owners* nil
                 *schema-read-lock-count* 1)
           (check (handler-case
                      (progn (with-schema-write-lock t) nil)
                    (error () t))
                  "writer cancellation interrupts while waiting behind a reader")
           (check (and (zerop *schema-write-lock-waiters*)
                       (null *schema-write-lock-owners*))
                  "cancelled schema writer clears its waiter count"))
      (setf (symbol-function 'check-for-interrupts) original-check))))

(defun test-schema-gate-queued-writers-enter-together ()
  (let ((lock (sb-thread:make-mutex :name "test queued schema changers"))
        (cv (sb-thread:make-waitqueue :name "test queued schema changers"))
        (entered 0)
        (release nil)
        (workers nil))
    (unwind-protect
         (progn
           (with-schema-read-lock
             (dotimes (index 2)
               (push (sb-thread:make-thread
                      (lambda ()
                        (with-schema-write-lock
                          (sb-thread:with-mutex (lock)
                            (incf entered)
                            (loop until release
                                  do (sb-thread:condition-wait cv lock))))))
                     workers))
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*schema-lock-mutex*)
                         (= *schema-write-lock-waiters* 2))))
                    "both DDL operations queue behind an exported snapshot")
             (check (zerop (sb-thread:with-mutex (lock) entered))
                    "queued schema changers respect the active snapshot"))
           (check (test-wait-until
                   (lambda () (sb-thread:with-mutex (lock) (= entered 2))))
                  "both queued schema changers enter after the snapshot drains"))
      (sb-thread:with-mutex (lock)
        (setf release t)
        (sb-thread:condition-broadcast cv))
      (dolist (worker workers) (sb-thread:join-thread worker)))))

(defun test-atomic-relation-reader-registration ()
  (let* ((xid 987781)
         (first-rel 987782)
         (blocked-rel 987783)
         (gate (acquire-relation-build-lock blocked-rel nil t))
         (test-lock (sb-thread:make-mutex :name "test atomic reader lock"))
         (wait-entered nil)
         (done nil)
         (worker-error nil)
         (worker nil)
         (original-wait (symbol-function 'condition-wait-with-timeout)))
    (unwind-protect
         (progn
           (setf (symbol-function 'condition-wait-with-timeout)
                 (lambda (waitqueue mutex)
                   (sb-thread:with-mutex (test-lock)
                     (setf wait-entered t))
                   (funcall original-wait waitqueue mutex)))
           (setf worker
                 (sb-thread:make-thread
                  (lambda ()
                    (handler-case
                        (register-relation-readers
                         (list first-rel blocked-rel) xid)
                      (error (condition)
                        (setf worker-error condition)))
                    (sb-thread:with-mutex (test-lock)
                      (setf done t)))
                  :name "test atomic relation reader"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) wait-entered)))
                  "multi-relation reader waits after checking the blocked set")
           (sb-thread:with-mutex (*relation-lock-mutex*)
             (check (null (gethash first-rel *relation-readers*))
                    "blocked atomic reader did not register its first relation")
             (check (null (gethash blocked-rel *relation-readers*))
                    "blocked atomic reader registered no partial relation set"))
           (release-relation-build-lock blocked-rel gate)
           (setf gate nil)
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) done)))
                  "multi-relation reader proceeds after the DDL gate opens")
           (check (null worker-error)
                  "multi-relation reader completes without an error")
           (sb-thread:with-mutex (*relation-lock-mutex*)
             (check (and (member xid (gethash first-rel *relation-readers*)
                                 :test #'=)
                         (member xid (gethash blocked-rel *relation-readers*)
                                 :test #'=))
                    "multi-relation reader publishes every registration together")))
      (when gate
        (release-relation-build-lock blocked-rel gate))
      (when worker
        (ignore-errors (sb-thread:join-thread worker :timeout 2)))
      (setf (symbol-function 'condition-wait-with-timeout) original-wait)
      (release-relation-writer-xid xid))))

(defun test-single-relation-lock-macros-share-set-guards ()
  (let ((build-acquisitions '())
        (build-releases '())
        (build-evaluations 0)
        (maintenance-acquisitions '())
        (maintenance-releases '())
        (maintenance-evaluations 0)
        (expected-xid (and *backend* (backend-xid *backend*)))
        (build-token (gensym "BUILD-TOKEN-"))
        (maintenance-token (gensym "MAINTENANCE-TOKEN-")))
    (flet ((acquire-relation-build-locks (relfilenodes xid wait-readers-p on-acquired)
             (push (list relfilenodes xid wait-readers-p) build-acquisitions)
             (funcall on-acquired relfilenodes build-token)
             build-token)
           (release-relation-build-locks (relfilenodes token)
             (push (list relfilenodes token) build-releases))
           (acquire-relation-maintenance-locks (relfilenodes on-acquired)
             (let ((normalized (sort (remove-duplicates (copy-list relfilenodes)
                                                        :test #'=)
                                     #'<)))
               (push (list relfilenodes normalized) maintenance-acquisitions)
               (funcall on-acquired normalized maintenance-token)
               (values normalized maintenance-token)))
           (release-relation-maintenance-locks (relfilenodes token)
             (push (list relfilenodes token) maintenance-releases)))
      (check (eq (with-relation-build-lock
                     ((progn (incf build-evaluations) 987791) t)
                   :build-body)
                 :build-body)
             "single relation build guard preserves its body result")
      (check (handler-case
                 (progn
                   (with-relation-build-lock
                       ((progn (incf build-evaluations) 987792))
                     (error "injected build body error"))
                   nil)
               (error () t))
             "single relation build guard propagates body errors")
      (check (and (= build-evaluations 2)
                  (equal (reverse build-acquisitions)
                         (list (list '(987791) expected-xid t)
                               (list '(987792) expected-xid nil)))
                  (equal (reverse build-releases)
                         (list (list '(987791) build-token)
                               (list '(987792) build-token))))
             "single relation build guards capture once and always release")
      (check (eq (with-relation-maintenance-lock
                     ((progn (incf maintenance-evaluations) 987793))
                   :maintenance-body)
                 :maintenance-body)
             "single relation maintenance guard preserves its body result")
      (check (handler-case
                 (progn
                   (with-relation-maintenance-lock
                       ((progn (incf maintenance-evaluations) 987794))
                     (error "injected maintenance body error"))
                   nil)
               (error () t))
             "single relation maintenance guard propagates body errors")
      (check (and (= maintenance-evaluations 2)
                  (equal (reverse maintenance-acquisitions)
                         '(((987793) (987793)) ((987794) (987794))))
                  (equal (reverse maintenance-releases)
                         (list (list '(987793) maintenance-token)
                               (list '(987794) maintenance-token))))
             "single relation maintenance guards capture once and always release"))))

(defun test-select-index-reader-blocks-drop-after-registration ()
  (let* ((xid 987784)
         (heap-relfilenode 987785)
         (index-relfilenode 987786)
         (index-oid 987787)
         (table (make-pg-class :oid 987788 :name "reader_gate"
                               :relfilenode heap-relfilenode
                               :columns #() :indexes (list index-oid)))
         (index (make-pg-index :oid index-oid :name "reader_gate_idx"
                               :relfilenode index-relfilenode
                               :table-oid (pg-class-oid table)
                               :columns '(1) :valid-p t))
         (drop-lock-relfilenodes
           (sort
            (remove-duplicates
             (drop-index-relation-lock-set
              (list (cons (pg-index-name index) index))
              (lambda (table-oid)
                (when (= table-oid (pg-class-oid table)) table)))
             :test #'=)
            #'<))
         (path (make-select-scan-path :kind :index-scan :index index))
         (relation (list :table table :columns (pg-class-columns table)
                         :path path))
         (test-lock (sb-thread:make-mutex :name "test SELECT index DDL lock"))
         (test-cv (sb-thread:make-waitqueue :name "test SELECT index DDL cv"))
         (query-paused nil)
         (resume-query nil)
         (query-body-ran nil)
         (query-done nil)
         (drop-done nil)
         (query-error nil)
         (drop-error nil)
         (reader-registrations-complete nil)
         (xid-wait-entered nil)
         (original-xid-active
           (sb-thread:with-mutex (*xid-lock*)
             (member xid *active-xids* :test #'=)))
         (original-wait-for-xid (symbol-function 'wait-for-xid))
         (threads '()))
    (labels ((start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads))
             (wait-for-flag (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader))))))
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*xid-lock*)
               (setf *active-xids* (adjoin xid *active-xids* :test #'=)))
             (setf (symbol-function 'wait-for-xid)
                   (lambda (waiting-xid)
                     (when (= waiting-xid xid)
                       (sb-thread:with-mutex (test-lock)
                         (setf xid-wait-entered t)
                         (sb-thread:condition-broadcast test-cv)))
                     (funcall original-wait-for-xid waiting-xid)))
             (start-thread
              (lambda ()
                (unwind-protect
                     (handler-case
                         (flet ((get-current-reader-xid () xid)
                                (ensure-current-table (current-table)
                                  (unless (eq current-table table)
                                    (error "SELECT table changed")))
                                (select-index-current-p (current-table current-index)
                                  (declare (ignore current-table current-index))
                                  ;; Pause after the atomic heap+index reader
                                  ;; registration, before catalog revalidation.
                                  (sb-thread:with-mutex (test-lock)
                                    (setf query-paused t)
                                    (sb-thread:condition-broadcast test-cv)
                                    (loop until resume-query
                                          do (sb-thread:condition-wait
                                              test-cv test-lock)))
                                  t))
                           (with-registered-select-relations
                               (registered (list relation))
                             (declare (ignore registered))
                             (sb-thread:with-mutex (test-lock)
                               (setf query-body-ran t))))
                       (error (condition) (setf query-error condition)))
                  (release-xid xid)
                  (sb-thread:with-mutex (test-lock)
                    (setf query-done t)
                    (sb-thread:condition-broadcast test-cv))))
              "SELECT holding heap and index readers")
             (check (wait-for-flag (lambda () query-paused))
                    "indexed SELECT reaches revalidation after registering readers")
             (start-thread
              (lambda ()
                (let ((locked-relations
                        (sort (list heap-relfilenode index-relfilenode) #'<))
                      (token nil))
                  (unwind-protect
                       (handler-case
                           (progn
                             (setf token
                                   (acquire-relation-build-locks
                                    locked-relations nil t))
                             (sb-thread:with-mutex (test-lock)
                               (setf drop-done :acquired)
                               (sb-thread:condition-broadcast test-cv)))
                         (error (condition) (setf drop-error condition)))
                    (when token
                      (release-relation-build-locks locked-relations token))
                    (sb-thread:with-mutex (test-lock)
                      (setf drop-done t)
                      (sb-thread:condition-broadcast test-cv)))))
              "DROP INDEX waiting for SELECT")
             (check (equal drop-lock-relfilenodes
                           (sort (list heap-relfilenode index-relfilenode) #'<))
                    "DROP INDEX lock selection includes its index and owning heap")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*relation-lock-mutex*)
                         (let ((token (gethash heap-relfilenode
                                               *relation-builders*)))
                           (and token
                                (eq token (gethash index-relfilenode
                                                   *relation-builders*)))))))
                    "DROP INDEX gates the heap and index as one relation group")
             (sb-thread:with-mutex (*relation-lock-mutex*)
               (setf reader-registrations-complete
                     (and (member xid (gethash heap-relfilenode
                                               *relation-readers*)
                                  :test #'=)
                          (member xid (gethash index-relfilenode
                                               *relation-readers*)
                                  :test #'=))))
             (check reader-registrations-complete
                    "indexed SELECT registers heap and index readers atomically")
             (check (wait-for-flag (lambda () xid-wait-entered))
                    "DROP INDEX reaches the active-reader XID wait")
             (check (not (sb-thread:with-mutex (test-lock) drop-done))
                    "DROP INDEX waits while the registered SELECT is active")
             (sb-thread:with-mutex (test-lock)
               (setf resume-query t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for-flag (lambda () query-done))
                    "indexed SELECT exits and releases its transaction readers")
             (check (wait-for-flag (lambda () (eq drop-done t)))
                    "DROP INDEX proceeds after the SELECT transaction ends")
             (check (and query-body-ran (null query-error) (null drop-error))
                    (format nil "reader/DDL handoff completes cleanly (~a, ~a)"
                            query-error drop-error)))
        (sb-thread:with-mutex (test-lock)
          (setf resume-query t)
          (sb-thread:condition-broadcast test-cv))
        (release-xid xid)
        (let ((worker-timed-out nil))
          (dolist (thread threads)
            (ignore-errors (sb-thread:join-thread thread :timeout 2))
            (when (sb-thread:thread-alive-p thread)
              (setf worker-timed-out t)
              (sb-thread:terminate-thread thread)
              (ignore-errors (sb-thread:join-thread thread))))
          (setf (symbol-function 'wait-for-xid) original-wait-for-xid)
          (sb-thread:with-mutex (*xid-lock*)
            (setf *active-xids*
                  (if original-xid-active
                      (adjoin xid *active-xids* :test #'=)
                      (remove xid *active-xids* :test #'=))))
          (check (not worker-timed-out)
                 "SELECT and DROP INDEX test workers stop before fixture cleanup"))))))

(defun test-live-operation-gate ()
  (test-reset-live-operation-admission)
  (let ((test-lock (sb-thread:make-mutex :name "test live-operation lock"))
        (test-cv (sb-thread:make-waitqueue :name "test live-operation condition"))
        (backend *backend*)
        (old-state (backend-state *backend*))
        (worker-ready nil)
        (release-worker nil)
        (reload-body-ran nil)
        (reloader-done nil)
        (drain-entered nil)
        (ordinary-attempting nil)
        (ordinary-entered nil)
        (threads '()))
    (setf (backend-state backend) :in-progress)
    (labels ((wait-for-flag (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads)))
      (unwind-protect
           (progn
             (start-thread
              (lambda ()
                (with-live-operation
                  (sb-thread:with-mutex (test-lock)
                    (setf worker-ready t)
                    (sb-thread:condition-broadcast test-cv)
                    (loop until release-worker
                          do (sb-thread:condition-wait test-cv test-lock)))))
              "test live operation")
             (check (wait-for-flag (lambda () worker-ready))
                    "live operation worker starts")
             (start-thread
              (lambda ()
                (with-live-operation-quiescence
                  (sb-thread:with-mutex (test-lock)
                    (setf reload-body-ran t)))
                (sb-thread:with-mutex (test-lock)
                  (setf reloader-done t)))
              "test reload quiescence")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes live-operation admission")
             (start-thread
              (lambda ()
                (let ((*backend* backend)
                      (*live-operation-drain-backends* (list backend)))
                  (with-live-operation-transaction-drain
                    (sb-thread:with-mutex (test-lock)
                      (setf drain-entered t)))))
              "test drain operation")
             (check (wait-for-flag (lambda () drain-entered))
                    "transaction-ending work can enter during drain")
             (start-thread
              (lambda ()
                (sb-thread:with-mutex (test-lock)
                  (setf ordinary-attempting t))
                (with-live-operation
                  (sb-thread:with-mutex (test-lock)
                    (setf ordinary-entered t))))
              "test waiting operation")
             (check (wait-for-flag (lambda () ordinary-attempting))
                    "ordinary operation attempts entry during reload")
             (sleep 0.05)
             (check (sb-thread:with-mutex (test-lock)
                      (not ordinary-entered))
                    "ordinary work waits while reload drains")
             (sb-thread:with-mutex (test-lock)
               (setf release-worker t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for-flag (lambda () reload-body-ran))
                    "reload starts after existing work leaves")
             (check (wait-for-flag (lambda () reloader-done))
                    "reload reopens live-operation admission")
             (check (wait-for-flag (lambda () ordinary-entered))
                    "waiting work enters after reload"))
        (sb-thread:with-mutex (test-lock)
          (setf release-worker t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread threads)
          (ignore-errors (sb-thread:join-thread thread)))
        (setf (backend-state backend) old-state)
        (test-reset-live-operation-admission)))))

(defun test-live-operation-quiescence-ownership ()
  (let ((original-finish (symbol-function 'finish-live-operation-drain)))
    (unwind-protect
         (progn
           (setf (symbol-function 'finish-live-operation-drain)
                 (lambda () (error "injected drain failure")))
           (check (handler-case
                      (progn (with-live-operation-quiescence :unreachable) nil)
                    (error () t))
                  "a drain failure propagates from gate acquisition")
           (check (and *live-operation-admission-open*
                       (null *live-operation-quiescence-owner*))
                  "a failed owner releases its own gate"))
      (setf (symbol-function 'finish-live-operation-drain) original-finish))
    (unwind-protect
         (progn
           ;; SBCL condition-wait can unwind after releasing the mutex. Ensure
           ;; recovery reacquires it and only the current owner reopens the gate.
           (setf (symbol-function 'finish-live-operation-drain)
                 (lambda ()
                   (sb-thread:release-mutex *live-operation-lock*)
                   (error "injected condition-wait failure after unlock")))
           (check (handler-case
                      (progn (with-live-operation-quiescence :unreachable) nil)
                    (error () t))
                  "an unwind after releasing the gate mutex propagates")
           (check (and *live-operation-admission-open*
                       (null *live-operation-quiescence-owner*))
                  "interrupted gate acquisition restores admission safely"))
      (setf (symbol-function 'finish-live-operation-drain) original-finish)))
  (let ((test-lock (sb-thread:make-mutex :name "test quiescence owner lock"))
        (test-cv (sb-thread:make-waitqueue :name "test quiescence owner condition"))
        (owner-entered nil)
        (release-owner nil)
        (owner-done nil)
        (owner-thread nil))
    (test-reset-live-operation-admission)
    (unwind-protect
         (progn
           (setf owner-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (with-live-operation-quiescence
                      (sb-thread:with-mutex (test-lock)
                        (setf owner-entered t)
                        (sb-thread:condition-broadcast test-cv)
                        (loop until release-owner
                              do (sb-thread:condition-wait test-cv test-lock))))
                    (sb-thread:with-mutex (test-lock)
                      (setf owner-done t)))
                  :name "test quiescence gate owner"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) owner-entered)))
                  "the first quiescence operation owns the closed gate")
           (let ((*live-operation-depth* 1))
             (check (handler-case
                        (progn (with-live-operation-quiescence :unreachable) nil)
                      (error () t))
                    "a nested quiescence attempt fails before acquisition"))
           (check (and (not *live-operation-admission-open*)
                       *live-operation-quiescence-owner*)
                  "a failed nested close cannot reopen another owner's gate")
           (sb-thread:with-mutex (test-lock)
             (setf release-owner t)
             (sb-thread:condition-broadcast test-cv))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) owner-done)))
                  "the original quiescence owner completes")
           (check (and *live-operation-admission-open*
                       (null *live-operation-quiescence-owner*))
                  "the original owner reopens admission on exit"))
      (sb-thread:with-mutex (test-lock)
        (setf release-owner t)
        (sb-thread:condition-broadcast test-cv))
      (when owner-thread
        (unless (sb-thread:join-thread owner-thread :timeout 2)
          (ignore-errors
           (sb-thread:interrupt-thread
            owner-thread (lambda () (error "quiescence owner test timed out"))))
          (ignore-errors (sb-thread:join-thread owner-thread :timeout 2))))
      (test-reset-live-operation-admission))))

(defun test-nested-control-repl-reload ()
  (let ((test-lock (sb-thread:make-mutex :name "test nested reload lock"))
        (test-cv (sb-thread:make-waitqueue :name "test nested reload condition"))
        (backend *backend*)
        (worker-ready nil)
        (allow-control-eval nil)
        (nested-done nil)
        (reload-done nil)
        (nested-output nil)
        (load-count 0)
        (threads '())
        (original-load-system (symbol-function 'asdf:load-system)))
    (setf *reload-requested* nil
          (symbol-function 'asdf:load-system)
          (lambda (system &rest options)
            (declare (ignore system options))
            (sb-thread:with-mutex (test-lock)
              (incf load-count))
            t))
    (labels ((start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads))
             (wait-for-flag (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader))))))
      (unwind-protect
           (progn
             (start-thread
              (lambda ()
                (let ((*backend* backend))
                  (with-live-operation
                    (sb-thread:with-mutex (test-lock)
                      (setf worker-ready t)
                      (sb-thread:condition-broadcast test-cv)
                      (loop until allow-control-eval
                            do (sb-thread:condition-wait test-cv test-lock)))
                    (let ((stream (make-string-output-stream)))
                      (control-eval stream '(progn (reload)))
                      (sb-thread:with-mutex (test-lock)
                        (setf nested-output (get-output-stream-string stream)
                              nested-done t))))))
              "test indirect control reload")
             (check (wait-for-flag (lambda () worker-ready))
                    "the control expression enters a live operation")
             ;; Exercise the same request flag the SIGHUP coordinator consumes.
             (request-reload)
             (start-thread
              (lambda ()
                (loop until (sb-ext:compare-and-swap *reload-requested* t nil)
                      do (sleep 0.01))
                (reload)
                (sb-thread:with-mutex (test-lock)
                  (setf reload-done t)))
              "test SIGHUP reload coordinator")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "the SIGHUP reload closes admission while control is active")
             (sb-thread:with-mutex (test-lock)
               (setf allow-control-eval t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for-flag (lambda () nested-done))
                    "an indirect REPL reload returns instead of deadlocking")
             (check (wait-for-flag (lambda () reload-done))
                    "the SIGHUP reload finishes after the REPL operation leaves")
             (check (and nested-output (search ":QUEUED" nested-output))
                    (format nil "an indirect REPL reload is queued (~s)"
                            nested-output))
             (check (= load-count 1)
                    "the SIGHUP coordinator performs one reload")
             (check *reload-requested*
                    (format nil "the nested reload request remains queued (~s, loads ~d)"
                            *reload-requested* load-count)))
        (sb-thread:with-mutex (test-lock)
          (setf allow-control-eval t)
          (sb-thread:condition-broadcast test-cv))
        (setf (symbol-function 'asdf:load-system) original-load-system
              *reload-requested* nil)
        (dolist (thread threads)
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "nested reload test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2))))))))

(defun test-legacy-live-operation-gate-call-shapes ()
  (let ((*live-operation-admission-open* nil)
        (*live-operation-drain-open* t))
    (check (eq (live-operation-admitted-p t) :drain)
           "legacy boolean drain predicates remain supported")
    (check (null (live-operation-admitted-p nil))
           "legacy false drain predicates remain closed"))
  (test-reset-live-operation-admission)
  ;; Simulate the first reload from an image whose old close function knew
  ;; nothing about owner tokens. The reload mutex identifies that old caller.
  (let ((*live-operation-admission-open* nil)
        (*live-operation-quiescence-owner* nil)
        (*live-operation-quiescence-owner-thread* nil)
        (*live-operation-drain-open* nil))
    (sb-thread:with-mutex (*reload-run-lock*)
      (adopt-legacy-live-operation-quiescence)
      (check (eq *live-operation-quiescence-owner-thread*
                 sb-thread:*current-thread*)
             "the first reload adopts its ownerless closed gate")
      (open-live-operation-admission)
      (check *live-operation-admission-open*
             "the first reload's legacy open call restores admission"))))

(defun test-live-operation-entry-interrupt-cleanup ()
  (let ((count-before *live-operation-count*)
        (original-try-enter (symbol-function 'try-enter-live-operation))
        (interrupt-queued nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'try-enter-live-operation)
                 (lambda (drain-p &optional backend)
                   (let ((entered-p (funcall original-try-enter drain-p backend)))
                     (when (and entered-p (not interrupt-queued))
                       (setf interrupt-queued t)
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (error "forced operation-entry interrupt"))))
                     entered-p)))
           (check (handler-case
                      (progn
                        (with-live-operation :body-should-not-finish)
                        nil)
                    (error () t))
                  "an interrupt after admission unwinds the live operation")
           (check interrupt-queued
                  "the operation-entry interrupt was queued")
           (check (= count-before *live-operation-count*)
                  "operation-entry interruption releases the reload count"))
      (setf (symbol-function 'try-enter-live-operation) original-try-enter))))

(defun test-transaction-ending-statement-classifier ()
  (dolist (query '("COMMIT" "END" "ROLLBACK" "ABORT"))
    (check (some #'transaction-ending-statement-p (parse-statements query))
           (format nil "transaction-ending AST recognized: ~a" query)))
  (let* ((statements (parse-statements "; ; SELECT 1; COMMIT; SELECT 2"))
         (ending-position (position-if #'transaction-ending-statement-p
                                       statements)))
    (check (= ending-position 1)
           "AST classifier finds a terminal after a nonterminal prefix")
    (check (= (length statements) 3)
           "the full simple-query message is parsed before dispatch"))
  (check (simple-query-payload-starts-with-transaction-end-p
          (frontend-message-payload
           (test-query-message "COMMIT; CHECKPOINT")))
         "a transaction-ending first command may enter drain admission")
  (dolist (query (list "/* leading comment */ COMMIT"
                       (format nil "-- leading comment~%ROLLBACK")
                       "; ; COMMIT"))
    (check (simple-query-payload-starts-with-transaction-end-p
            (frontend-message-payload (test-query-message query)))
           (format nil "SQL trivia before a transaction end is ignored: ~a"
                   query)))
  (check (not (simple-query-payload-starts-with-transaction-end-p
               (frontend-message-payload
                (test-query-message "SELECT 1; COMMIT"))))
         "work before COMMIT cannot enter drain admission")
  (check (not (simple-query-payload-starts-with-transaction-end-p
               (frontend-message-payload
                (test-query-message "SELECT 'COMMIT'"))))
         "a transaction keyword inside a string cannot enter drain admission")
  (check (handler-case
             (progn (parse-statements "COMMIT; SELECT") nil)
           (pg-error () t)
           (error () t))
         "a syntax error after COMMIT is detected while parsing the full message"))

(defun test-simple-query-terminal-statement-drain ()
  (let* ((backend *backend*)
         (events '())
         (query "COMMIT")
         (statements '((:type :commit)))
         (old-state (backend-state backend))
         (original-query-string (symbol-function 'query-string))
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (unwind-protect
         (progn
           (set-backend-state backend :in-progress)
           (setf (symbol-function 'query-string)
                 (lambda (message)
                   (declare (ignore message))
                   query)
                 (symbol-function 'exec-simple-query)
                 (lambda (stream query &rest options)
                   (declare (ignore stream query))
                   (push (list :exec options *live-operation-current-lane*) events)
                   (if (getf options :prepare-only-p)
                       (values statements t)
                       (values :ok t)))
                 (symbol-function 'send-ready-for-query)
                 (lambda (stream status)
                   (declare (ignore stream status))
                   (push (list :ready *live-operation-current-lane*) events)))
           (let ((*backend* backend)
                 (*live-operation-admission-open* nil)
                 (*live-operation-drain-open* t)
                 (*live-operation-drain-backends* (list backend))
                 (*live-operation-count* 0)
                 (*live-operation-backend-counts* (make-hash-table :test #'eq)))
             (test-dispatch-query-frame nil query))
           (check (equal (nreverse events)
                         '((:exec (:prepare-only-p t :start-execution-p nil
                                   :finish-execution-p nil) :drain)
                           (:exec (:statements ((:type :commit))
                                   :start-execution-p nil
                                   :finish-execution-p nil) :drain)
                           (:ready :drain)))
                  "a standalone transaction end executes in drain admission")
           (setf events '()
                 query "COMMIT; CHECKPOINT"
                 statements '((:type :commit) (:type :checkpoint)))
           (set-backend-state backend :idle)
           (let ((*backend* backend))
             (test-dispatch-query-frame nil query))
           (check (equal (nreverse events)
                         '((:exec (:start-execution-p nil
                                   :finish-execution-p nil) :normal)
                           (:ready :normal)))
                  "an idle COMMIT cannot admit unrelated SQL to the drain lane"))
      (setf (symbol-function 'query-string) original-query-string
            (symbol-function 'exec-simple-query) original-exec-simple-query
            (symbol-function 'send-ready-for-query) original-send-ready)
      (set-backend-state backend old-state))))

(defun test-cancel-query-waiting-for-live-operation-admission ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend))
         (test-lock (sb-thread:make-mutex :name "test queued cancel lock"))
         (test-cv (sb-thread:make-waitqueue :name "test queued cancel condition"))
         (query-waiting-p nil)
         (blocker-ready nil)
         (release-blocker nil)
         (reload-entered nil)
         (release-reload nil)
         (reopen-attempting nil)
         (cancel-admission-locked nil)
         (release-cancel-admission nil)
         (cancel-done nil)
         (query-done nil)
         (query-executed nil)
         (query-error nil)
         (error-sqlstate nil)
         (ready-sent nil)
         (startup-registration-called nil)
         (cancel-path (merge-pathnames
                       (format nil "cancel-request-~d.bin"
                               (get-internal-real-time))
                       *pgdata*))
         (cancel-pid (+ #x70000000 (mod (get-internal-real-time) #x0fffffff)))
         (cancel-key #x1234abcd)
         (blocker-thread nil)
         (reload-thread nil)
         (query-thread nil)
         (second-query-thread nil)
         (idle-reload-thread nil)
         (idle-query-thread nil)
         (cancel-thread nil)
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-error (symbol-function 'send-error-response))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (labels ((wait-for (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (sb-thread:make-thread function :name name)))
      (unwind-protect
           (progn
             (setf (backend-state backend) :in-progress
                   (backend-pid backend) cancel-pid
                   (backend-cancel-key backend) cancel-key
                   (backend-executing backend) nil
                   (backend-cancel-pending backend) nil
                   (symbol-function 'exec-simple-query)
                   (lambda (stream query &rest options)
                     (declare (ignore stream query options))
                     (sb-thread:with-mutex (test-lock)
                       (setf query-executed t))
                     (values :ok t))
                   (symbol-function 'send-error-response)
                   (lambda (stream condition)
                     (declare (ignore stream))
                     (sb-thread:with-mutex (test-lock)
                       (setf error-sqlstate
                             (and (typep condition 'pg-error)
                                  (pg-error-sqlstate condition)))))
                   (symbol-function 'send-ready-for-query)
                   (lambda (stream status)
                     (declare (ignore stream status))
                     (sb-thread:with-mutex (test-lock)
                       (setf ready-sent t))))
             (with-open-file (stream cancel-path
                                     :direction :output
                                     :if-exists :supersede
                                     :if-does-not-exist :create
                                     :element-type '(unsigned-byte 8))
               (write-sequence
                (concatenate '(vector (unsigned-byte 8))
                             (u32-to-bytes 16)
                             (u32-to-bytes +cancel-request-code+)
                             (u32-to-bytes cancel-pid)
                             (u32-to-bytes cancel-key))
                stream))
             (register-backend backend)
             (test-reset-live-operation-admission)
             (setf blocker-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* nil))
                        (with-live-operation
                          (sb-thread:with-mutex (test-lock)
                            (setf blocker-ready t)
                            (sb-thread:condition-broadcast test-cv)
                            (loop until release-blocker
                                  do (sb-thread:condition-wait test-cv
                                                               test-lock))))))
                    "test live operation holding reload drain"))
             (check (wait-for (lambda () blocker-ready))
                    "an admitted operation holds reload in the drain lane")
             (setf reload-thread
                   (start-thread
                    (lambda ()
                      (let ((*live-operation-before-open-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf reopen-attempting t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (with-live-operation-quiescence
                          (sb-thread:with-mutex (test-lock)
                            (setf reload-entered t)
                            (sb-thread:condition-broadcast test-cv)
                            (loop until release-reload
                                  do (sb-thread:condition-wait test-cv test-lock))))))
                    "test queued query cancel reload"))
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes admission before the query arrives")
             (setf query-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* backend)
                            (*live-operation-admission-wait-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf query-waiting-p t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (handler-case
                            (handle-simple-query
                             nil (test-query-message "SELECT 1"))
                          (error (condition)
                            (sb-thread:with-mutex (test-lock)
                              (setf query-error condition))))
                        (sb-thread:with-mutex (test-lock)
                          (setf query-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test query queued for reload admission"))
             (check (wait-for (lambda () query-waiting-p))
                    "the query reaches the reload admission condition wait")
             (with-open-file (stream cancel-path
                                     :direction :input
                                     :element-type '(unsigned-byte 8))
               (let ((*backend* nil))
                 (check (null
                         (read-startup-until-ready
                          stream
                          (lambda ()
                            (setf startup-registration-called t))))
                        "a CancelRequest completes without starting a session")))
             (check (not startup-registration-called)
                    "CancelRequest bypasses ordinary backend registration")
             (check (wait-for (lambda () query-done))
                    "a canceled transaction can return its response during drain")
             (check (and (equal error-sqlstate "57014")
                         ready-sent
                         (null query-error))
                    "drain-time cancellation returns SQLSTATE 57014 and ReadyForQuery")
             (check (not reload-entered)
                    "the active counted operation still holds reload in drain")
             (sb-thread:with-mutex (test-lock)
               (setf release-blocker t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () reload-entered))
                    "reload enters ASDF quiescence after the drain completes")
             (sb-thread:with-mutex (test-lock)
               (setf query-waiting-p nil
                     query-done nil
                     query-error nil
                     error-sqlstate nil
                     ready-sent nil
                     cancel-done nil))
             (setf second-query-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* backend)
                            (*live-operation-admission-wait-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf query-waiting-p t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (handler-case
                            (handle-simple-query
                             nil (test-query-message "SELECT 2"))
                          (error (condition)
                            (sb-thread:with-mutex (test-lock)
                              (setf query-error condition))))
                        (sb-thread:with-mutex (test-lock)
                          (setf query-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test query queued during ASDF reload"))
             (check (wait-for (lambda () query-waiting-p))
                    "a new query waits while ASDF reload is active")
             (setf cancel-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* nil)
                            (*live-operation-cancel-request-admission-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf cancel-admission-locked t)
                                  (sb-thread:condition-broadcast test-cv)
                                  (loop until release-cancel-admission
                                        do (sb-thread:condition-wait test-cv
                                                                     test-lock))))))
                        (with-open-file (stream cancel-path
                                                :direction :input
                                                :element-type '(unsigned-byte 8))
                          (read-startup-until-ready stream
                                                    (lambda ()
                                                      (setf startup-registration-called t))))
                        (sb-thread:with-mutex (test-lock)
                          (setf cancel-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test cancel request deferred during ASDF quiescence"))
             (check (wait-for (lambda () cancel-admission-locked))
                    "CancelRequest reaches the atomic admission decision")
             (check (not query-done)
                    "the queued query cannot race ahead of deferred cancellation")
             (sb-thread:with-mutex (test-lock)
               (setf release-reload t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () reopen-attempting))
                    "reload attempts to reopen while CancelRequest owns admission")
             (sb-thread:with-mutex (test-lock)
               (setf release-cancel-admission t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () cancel-done))
                    "CancelRequest is queued without entering reloadable code")
             (check (wait-for (lambda () query-done))
                    "the deferred cancellation is delivered when admission reopens")
             (check (and (equal error-sqlstate "57014")
                         ready-sent
                         (null query-error))
                    "deferred CancelRequest cancels before SQL dispatch resumes")
             (check (null *live-operation-pending-cancel-requests*)
                    "deferred CancelRequests drain before admission reopens")
             (check (not query-executed)
                    "a canceled queued query never reaches SQL execution")
             (check (and (not (backend-executing backend))
                         (not (backend-cancel-pending backend)))
                    "queued cancellation clears backend interrupt state")
             ;; An idle CancelRequest received during ASDF must not survive
             ;; until this backend starts its next query.
             (ignore-errors (sb-thread:join-thread reload-thread :timeout 2))
             (sb-thread:with-mutex (test-lock)
               (setf reload-entered nil
                     release-reload nil
                     query-waiting-p nil
                     query-done nil
                     query-error nil
                     error-sqlstate nil
                     ready-sent nil
                     cancel-done nil
                     startup-registration-called nil))
             (setf idle-reload-thread
                   (start-thread
                    (lambda ()
                      (with-live-operation-quiescence
                        (sb-thread:with-mutex (test-lock)
                          (setf reload-entered t)
                          (sb-thread:condition-broadcast test-cv)
                          (loop until release-reload
                                do (sb-thread:condition-wait test-cv test-lock)))))
                    "test idle cancel reload"))
             (check (wait-for
                     (lambda ()
                       (and reload-entered
                            (sb-thread:with-mutex (*live-operation-lock*)
                              (not *live-operation-admission-open*)))))
                    "a second reload holds admission closed")
             (with-open-file (stream cancel-path
                                     :direction :input
                                     :element-type '(unsigned-byte 8))
               (let ((*backend* nil))
                 (check (null
                         (read-startup-until-ready stream
                                                   (lambda () nil)))
                        "an idle CancelRequest is ignored during ASDF")))
             (check (null *live-operation-pending-cancel-requests*)
                    "an idle CancelRequest is not deferred")
             (setf idle-query-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* backend)
                            (*live-operation-admission-wait-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf query-waiting-p t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (handler-case
                            (handle-simple-query nil (test-query-message "SELECT 3"))
                          (error (condition)
                            (sb-thread:with-mutex (test-lock)
                              (setf query-error condition))))
                        (sb-thread:with-mutex (test-lock)
                          (setf query-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test query after idle cancel"))
             (check (wait-for (lambda () query-waiting-p))
                    "the next query waits behind the reload gate")
             (check (and (backend-executing backend)
                         (not (backend-cancel-pending backend)))
                    "the next query starts without inheriting an idle cancel")
             (sb-thread:with-mutex (test-lock)
               (setf release-reload t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () query-done))
                    "the next query completes after reload admission reopens")
             (check (and (null query-error)
                         (null error-sqlstate)
                         ready-sent
                         query-executed
                         (not (backend-cancel-pending backend)))
                    "an idle deferred cancel cannot cancel a later query"))
        (sb-thread:with-mutex (test-lock)
          (setf release-blocker t
                release-reload t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread (list blocker-thread reload-thread query-thread
                              second-query-thread cancel-thread
                              idle-reload-thread idle-query-thread))
          (when thread
            (unless (sb-thread:join-thread thread :timeout 2)
              (ignore-errors
               (sb-thread:interrupt-thread
                thread (lambda () (error "queued cancellation test timed out"))))
              (ignore-errors (sb-thread:join-thread thread :timeout 2)))))
        (unregister-backend backend)
        (when (probe-file cancel-path)
          (delete-file cancel-path))
        (setf (symbol-function 'exec-simple-query) original-exec-simple-query
              (symbol-function 'send-error-response) original-send-error
              (symbol-function 'send-ready-for-query) original-send-ready)
        (set-backend-state backend old-state)
        (test-reset-live-operation-admission)))))

(defun test-late-cancel-is-cleared-at-query-end ()
  (let* ((backend *backend*)
         (ready-sent nil)
         (query-error nil)
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (unwind-protect
         (progn
           (test-reset-live-operation-admission)
           (setf (symbol-function 'exec-simple-query)
                 (lambda (stream query &rest options)
                   (declare (ignore stream query options))
                   (values :ok t))
                 (symbol-function 'send-ready-for-query)
                 (lambda (stream status)
                   (declare (ignore stream status))
                   ;; A protocol CancelRequest racing with the final response
                   ;; must be cleared by query cleanup, not become a new error.
                   (let ((*backend* nil))
                     (cancel-backend backend))
                   (setf ready-sent t)))
           (handler-case
               (handle-simple-query nil (test-query-message "SELECT 1"))
             (error (condition)
               (setf query-error condition)))
           (check (null query-error)
                  "a CancelRequest after ReadyForQuery does not escape cleanup")
           (check ready-sent
                  "the late-cancel query reaches ReadyForQuery")
           (check (and (not (backend-executing backend))
                       (not (backend-cancel-pending backend)))
                  "query-end cleanup clears a cancellation that arrived late"))
      (setf (symbol-function 'exec-simple-query) original-exec-simple-query
            (symbol-function 'send-ready-for-query) original-send-ready)
      (test-reset-live-operation-admission))))

(defun test-query-token-cleans-up-after-publication-error ()
  (let* ((backend (make-backend))
         (token-published nil)
         (original-begin (symbol-function 'begin-backend-query-execution)))
    (unwind-protect
         (let ((*backend* backend)
               (*live-operation-backend-query-tokens*
                 (make-hash-table :test #'eq)))
           (test-reset-live-operation-admission)
           (setf (symbol-function 'begin-backend-query-execution)
                 (lambda (target)
                   (funcall original-begin target)
                   (setf token-published
                         (not (null (gethash target
                                             *live-operation-backend-query-tokens*))))
                   (error "injected interrupt after query token publication")))
           (check (handler-case
                      (progn
                        (handle-simple-query nil (test-query-message "SELECT 1"))
                        nil)
                    (error () t))
                  "query setup error propagates after token publication")
           (check token-published
                  "query setup published its token before the injected error")
           (check (and (not (backend-executing backend))
                       (not (gethash backend
                                     *live-operation-backend-query-tokens*)))
                  "the installed unwind cleanup removes a partially published query"))
      (setf (symbol-function 'begin-backend-query-execution) original-begin)
      (test-reset-live-operation-admission))))
