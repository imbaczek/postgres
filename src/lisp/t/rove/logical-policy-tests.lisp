(in-package #:pg)

(rove:deftest logical-info-barrier-acknowledges-assigned-xids-with-pending-cache
    (let ((owner (gensym "LOGICAL-INFO-OWNER-")))
      (dolist (before '(nil t))
        (dolist (after '(nil t))
          (dolist (xid '(nil 42))
            (let* ((shared (%make-wal-logical-info owner 2 after))
                   (cache (%make-wal-logical-info-cache 1 before nil nil))
                   (next (wal-logical-info-deliver-cache cache shared xid)))
              (rove:ok (= 2 (wal-logical-info-cache-generation next)))
              (rove:ok (eq (if xid before after) (wal-logical-info-cache-enabled next)))
              (rove:ok (eq (not (null xid)) (wal-logical-info-cache-pending next)))
              (rove:ok (= 1 (wal-logical-info-cache-generation cache)))
              (rove:ok (eq before (wal-logical-info-cache-enabled cache)))))))
      (let* ((shared (%make-wal-logical-info owner 3 t))
             (cache (wal-logical-info-deliver-cache nil shared nil)))
        (rove:ok (wal-logical-info-cache-enabled cache))
        (rove:ok (not (wal-logical-info-cache-pending cache)))
        (rove:ok (rove:signals (wal-logical-info-deliver-cache nil shared 42) 'error)))))

(rove:deftest logical-info-transaction-end-installs-latest-shared-value
    (let* ((owner (gensym "LOGICAL-INFO-OWNER-"))
           (cache (%make-wal-logical-info-cache 1 t nil nil))
           (off (%make-wal-logical-info owner 2 nil))
           (on (%make-wal-logical-info owner 3 t))
           (last (%make-wal-logical-info owner 4 nil))
           (pending (wal-logical-info-deliver-cache cache off 42))
           (again (wal-logical-info-deliver-cache pending on 42))
           (finished (wal-logical-info-end-cache again last 42 nil)))
      (rove:ok (wal-logical-info-cache-enabled pending))
      (rove:ok (wal-logical-info-cache-enabled again))
      (rove:ok (= 4 (wal-logical-info-cache-generation finished)))
      (rove:ok (not (wal-logical-info-cache-enabled finished)))
      (rove:ok (not (wal-logical-info-cache-pending finished)))
      (rove:ok (null (wal-logical-info-cache-held-xid finished)))))

(rove:deftest logical-info-failed-block-retains-only-real-transaction-cache
    (let* ((owner (gensym "LOGICAL-INFO-OWNER-"))
           (cache (%make-wal-logical-info-cache 1 t nil nil))
           (off (%make-wal-logical-info owner 2 nil))
           (failed (wal-logical-info-end-cache cache off 42 t))
           (pending (wal-logical-info-deliver-cache failed off nil))
           (finished (wal-logical-info-end-cache pending off nil nil))
           (no-xid (wal-logical-info-end-cache cache off nil t)))
      (rove:ok (= 42 (wal-logical-info-cache-held-xid failed)))
      (rove:ok (wal-logical-info-cache-enabled failed))
      (rove:ok (wal-logical-info-cache-enabled pending))
      (rove:ok (wal-logical-info-cache-pending pending))
      (rove:ok (not (wal-logical-info-cache-enabled finished)))
      (rove:ok (null (wal-logical-info-cache-held-xid finished)))
      (rove:ok (not (wal-logical-info-cache-enabled no-xid)))
      (rove:ok (null (wal-logical-info-cache-held-xid no-xid)))))

(cl-spec:defproperty logical-info-deferred-cache-is-independent-of-intermediate-values
    ((initial (member nil t)) (middle (member nil t)) (final (member nil t)))
  (:about wal-logical-info-deliver-cache)
  (:trials (:normal 100))
  (let* ((owner (gensym "LOGICAL-INFO-OWNER-"))
         (cache (%make-wal-logical-info-cache 1 initial nil nil))
         (first (wal-logical-info-deliver-cache cache (%make-wal-logical-info owner 2 middle) 42))
         (second (wal-logical-info-deliver-cache first (%make-wal-logical-info owner 3 final) 42))
         (end (wal-logical-info-end-cache second (%make-wal-logical-info owner 3 final) 42 nil)))
    (and (eq initial (wal-logical-info-cache-enabled first))
         (eq initial (wal-logical-info-cache-enabled second))
         (eq final (wal-logical-info-cache-enabled end))
         (not (wal-logical-info-cache-pending end))
         (= 1 (wal-logical-info-cache-generation cache)))))

(rove:deftest logical-info-generated-deferred-cache-contract
    (unit-check-property 'logical-info-deferred-cache-is-independent-of-intermediate-values))

(rove:deftest logical-info-cache-rejects-invalid-and-future-generations
    (let* ((owner (gensym "LOGICAL-INFO-OWNER-"))
           (shared (%make-wal-logical-info owner 2 t))
           (cache (%make-wal-logical-info-cache 1 nil nil nil)))
      (dolist (bad (list (%make-wal-logical-info nil 2 t)
                         (%make-wal-logical-info owner 0 t)
                         (%make-wal-logical-info owner 2 :unknown)))
        (rove:ok (rove:signals (wal-logical-info-deliver-cache cache bad nil) 'error)))
      (dolist (bad (list (%make-wal-logical-info-cache 3 nil nil nil)
                         (%make-wal-logical-info-cache 1 :unknown nil nil)
                         (%make-wal-logical-info-cache 1 nil :pending nil)
                         (%make-wal-logical-info-cache 1 nil nil -42)))
        (rove:ok (rove:signals (wal-logical-info-deliver-cache bad shared nil) 'error)))
      (rove:ok (rove:signals (wal-logical-info-deliver-cache cache shared -42) 'error))
      (let ((held (%make-wal-logical-info-cache 1 t nil 42)))
        (rove:ok (rove:signals (wal-logical-info-deliver-cache held shared 43) 'error))
        (rove:ok (rove:signals (wal-logical-info-end-cache held shared 43 nil) 'error)))
      (rove:ok (rove:signals (wal-logical-info-end-cache cache shared nil :unknown) 'error))))

(defun call-with-logical-info-barrier-test (function &key enabled)
  (let* ((*wal-lock* (sb-thread:make-mutex :name "logical info test WAL"))
         (*xid-lock* (sb-thread:make-mutex :name "logical info test XID"))
         (*backend-registry-lock* (sb-thread:make-mutex :name "logical info test registry"))
         (*live-operation-lock* (sb-thread:make-mutex :name "logical info test admission"))
         (*backend-registry* (make-hash-table :test #'eql))
         (*backend-admission-classes* (make-hash-table :test #'eq))
         (*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
         (*live-operation-admission-open* t) (*live-operation-cleanup-backends* nil)
         (*shutdown-abandoned-backends* nil) (*n-backends* 0) (*backend* nil)
         (*wal-storage-incarnation* (gensym "LOGICAL-INFO-TEST-"))
         (*wal-logical-info* nil) (*wal-logical-info-activations* (make-hash-table :test #'eq))
         (*wal-logical-info-disable-pending* nil)
         (*wal-ready* t) (*wal-poisoned* nil) (*physical-recovery-state* nil)
         (*wal-logical-policy* nil) (*wal-fpw-state* nil)
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
         (*primary-wal-writer* nil)
         (*wal-end-lsn* 0) (*wal-durable-lsn* 0)
         (*wal-pending-page-publication* nil) (*wal-pending-page-publication-tail* nil)
         (*clog-pending-commits* (make-hash-table :test #'eql))
         (*active-xids* nil) (*next-xid* 42)
         (*logical-capture-xid-modes* (make-hash-table :test #'eql))
         (*logical-capture-arming* (make-hash-table :test #'eq))
         (*logical-capture-live-slots* 0) (*logical-capture-force* enabled)
         (*wal-level* :replica)
         (*startup-installed-configuration*
           (make-startup-configuration :settings (make-hash-table :test #'equal) :wal-level :replica))
         (first (make-backend :pid 1)) (second (make-backend :pid 2)))
    (sb-thread:with-mutex (*wal-lock*)
      (wal-logical-info-publish-under-lock enabled :rebase t))
    (register-backend first)
    (register-backend second)
    (funcall function first second)))

(rove:deftest logical-info-registration-and-barrier-have-one-publication-boundary
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (rove:ok (not (wal-logical-info-backend-enabled first)))
       (sb-thread:with-mutex (*wal-lock*) (wal-logical-info-publish-under-lock t))
       (rove:ok (wal-logical-info-backend-enabled first))
       (rove:ok (wal-logical-info-backend-enabled second))
       (let ((late (make-backend :pid 3)))
         (register-backend late)
         (rove:ok (wal-logical-info-backend-enabled late))
         (rove:ok (= 3 *n-backends*))
         (rove:ok (= (wal-logical-info-generation *wal-logical-info*)
                     (wal-logical-info-cache-generation (backend-logical-info-cache late))))))))

(rove:deftest logical-info-barrier-defers-through-release-until-backend-cleanup
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock first))
       (sb-thread:with-mutex (*wal-lock*) (wal-logical-info-publish-under-lock nil))
       (rove:ok (wal-logical-info-backend-enabled first))
       (rove:ok (not (wal-logical-info-backend-enabled second)))
       (sb-thread:with-mutex (*xid-lock*) (setf *active-xids* nil))
       (sb-thread:with-mutex (*wal-lock*)
         (wal-logical-info-publish-under-lock t)
         (wal-logical-info-publish-under-lock nil))
       (rove:ok (wal-logical-info-backend-enabled first))
       (rove:ok (wal-logical-info-cache-pending (backend-logical-info-cache first)))
       (sb-thread:with-mutex (*xid-lock*)
         (wal-logical-info-finish-backend-under-xid-lock first nil))
       (rove:ok (null (backend-xid first)))
       (rove:ok (not (wal-logical-info-backend-enabled first))))
     :enabled t))

(rove:deftest logical-info-failed-block-latch-survives-resource-cleanup
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (declare (ignore second))
       (sb-thread:with-mutex (*xid-lock*)
         (xid-allocate-under-lock first)
         (setf *active-xids* nil)
         (wal-logical-info-finish-backend-under-xid-lock first t))
       (rove:ok (null (backend-xid first)))
       (rove:ok (= 42 (wal-logical-info-cache-held-xid (backend-logical-info-cache first))))
       (sb-thread:with-mutex (*wal-lock*) (wal-logical-info-publish-under-lock nil))
       (rove:ok (wal-logical-info-backend-enabled first))
       (sb-thread:with-mutex (*xid-lock*)
         (wal-logical-info-finish-backend-under-xid-lock first nil))
       (rove:ok (not (wal-logical-info-backend-enabled first)))
       (rove:ok (null (wal-logical-info-cache-held-xid (backend-logical-info-cache first)))))
     :enabled t))

(rove:deftest logical-info-arming-does-not-infer-a-process-cache
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (setf (gethash :test-arm *logical-capture-arming*) t)
       (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock first))
       (rove:ok (eq :capture (gethash (backend-xid first) *logical-capture-xid-modes*)))
       (rove:ok (not (wal-logical-info-backend-enabled first)))
       (sb-thread:with-mutex (*wal-lock*) (wal-logical-info-publish-under-lock t))
       (rove:ok (not (wal-logical-info-backend-enabled first)))
       (rove:ok (wal-logical-info-cache-pending (backend-logical-info-cache first)))
       (rove:ok (wal-logical-info-backend-enabled second)))))

(rove:deftest logical-info-sql-surfaces-use-each-backends-transaction-cache
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock first))
       (sb-thread:with-mutex (*wal-lock*) (wal-logical-info-publish-under-lock nil))
       (loop for backend in (list first second)
             for expected in '("logical" "replica")
             do (let* ((*backend* backend)
                       (row (find "effective_wal_level" (startup-settings-rows)
                                  :key (lambda (row) (aref row 0)) :test #'string=)))
                  (rove:ok (string= expected (sql-setting-value "effective_wal_level")))
                  (rove:ok (string= expected (aref row 1)))))
       (sb-thread:with-mutex (*xid-lock*)
         (setf *active-xids* nil)
         (wal-logical-info-finish-backend-under-xid-lock first nil))
       (let ((*backend* first))
         (rove:ok (string= "replica" (sql-setting-value "effective_wal_level")))))
     :enabled t))

(rove:deftest logical-info-pre-marker-read-does-not-acquire-the-wal-lock
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (declare (ignore first))
       (sb-thread:with-mutex (*wal-lock*)
         (wal-logical-info-publish-under-lock t)
         (let ((*backend* second))
           (rove:ok (string= "logical" (sql-setting-value "effective_wal_level")))
           (let ((row (startup-effective-wal-level-settings-row *startup-installed-configuration*)))
             (rove:ok (string= "logical" (aref row 1)))))))))

(rove:deftest logical-info-failed-provisional-on-keeps-capture-until-writer-cleanup
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (let ((before *wal-end-lsn*))
         (rove:ok
          (eq :cut
              (catch 'logical-info-cut
                (call-with-logical-capture-armed
                 (lambda () (throw 'logical-info-cut :cut))
                 :activation (make-wal-logical-info-activation)))))
         (rove:ok *wal-logical-info-disable-pending*)
         (rove:ok (wal-logical-info-backend-enabled first))
         (rove:ok (zerop (hash-table-count *logical-capture-arming*)))
         (rove:ok (zerop (hash-table-count *wal-logical-info-activations*)))
         (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock first))
         (rove:ok (eq :capture (gethash (backend-xid first) *logical-capture-xid-modes*)))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (wal-logical-catalog-event-needed-under-lock-p 16384))
           (rove:ok (eq :disabled
                        (wal-process-pending-logical-disable-under-lock (lambda () nil))))
           (rove:ok (not (wal-logical-capture-required-under-lock-p))))
         (rove:ok (= before *wal-end-lsn*))
         (rove:ok (not *wal-logical-info-disable-pending*))
         (rove:ok (wal-logical-info-backend-enabled first))
         (rove:ok (not (wal-logical-info-backend-enabled second)))
         (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock second))
         (rove:ok (eq :skip (gethash (backend-xid second) *logical-capture-xid-modes*)))))))

(rove:deftest logical-info-last-slot-loss-requeues-provisional-disable
    (dolist (loss '(:drop :lost))
      (call-with-logical-info-barrier-test
       (lambda (first second)
         (declare (ignore second))
         (catch 'logical-info-cut
           (call-with-logical-capture-armed
            (lambda () (throw 'logical-info-cut :cut))
            :activation (make-wal-logical-info-activation)))
         (let ((slot (make-wal-replication-slot :name "valid" :kind :logical :restart-lsn 0)))
           (sb-thread:with-mutex (*wal-lock*)
             (setf (gethash "valid" *wal-replication-slots*) slot)
             (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
             (wal-process-pending-logical-disable-under-lock (lambda () nil))
             (rove:ok (not *wal-logical-info-disable-pending*))
             (rove:ok (wal-logical-info-backend-enabled first))
             (ecase loss
               (:drop (remhash "valid" *wal-replication-slots*))
               (:lost (setf (wal-replication-slot-wal-status slot) :lost)))
             (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
             (rove:ok *wal-logical-info-disable-pending*)
             (wal-process-pending-logical-disable-under-lock (lambda () nil)))
           (rove:ok (not (wal-logical-info-backend-enabled first))))))))

(rove:deftest logical-info-clog-rebind-preserves-interest-but-not-old-writer-authority
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (declare (ignore second))
       (let* ((token (gensym "LOGICAL-INFO-ACTIVATION-"))
              (old *wal-storage-incarnation*)
              (activation (make-wal-logical-info-activation :incarnation old)))
         (unwind-protect
              (progn
                (sb-thread:with-mutex (*xid-lock*)
                  (setf (gethash token *logical-capture-arming*) t))
                (wal-logical-info-begin-activation token activation)
                (let ((generation (wal-logical-info-generation *wal-logical-info*)))
                  (sb-thread:with-mutex (*wal-lock*)
                    (setf *wal-logical-info-disable-pending* t)
                    (let ((plan (prepare-primary-wal-writer-clog-reset-under-lock)))
                      (publish-primary-wal-writer-clog-reset-under-lock plan)))
                  (rove:ok (not (eq old *wal-storage-incarnation*)))
                  (rove:ok (= generation (wal-logical-info-generation *wal-logical-info*)))
                  (rove:ok *wal-logical-info-disable-pending*)
                  (rove:ok (eq activation (gethash token *wal-logical-info-activations*)))
                  (rove:ok (wal-logical-info-backend-enabled first))
                  (rove:ok (rove:signals (wal-report-logical-decoding-status t old) 'error))
                  (let ((effects 0))
                    (sb-thread:with-mutex (*wal-lock*)
                      (rove:ok
                       (rove:signals
                        (wal-report-logical-decoding-status-under-lock
                         t old (lambda () (incf effects)))
                        'error)))
                    (rove:ok (zerop effects)))
                  (rove:ok (not *wal-poisoned*))
                  (rove:ok (wal-logical-info-backend-enabled first))))
           (unwind-protect (wal-logical-info-end-activation token t)
             (sb-thread:with-mutex (*xid-lock*) (remhash token *logical-capture-arming*))))
         (sb-thread:with-mutex (*wal-lock*)
           (wal-process-pending-logical-disable-under-lock (lambda () nil)))
         (rove:ok (not (wal-logical-info-backend-enabled first)))))))

(rove:deftest logical-info-clog-plan-rejects-a-new-shared-generation
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (declare (ignore first second))
       (sb-thread:with-mutex (*wal-lock*)
         (let ((plan (prepare-primary-wal-writer-clog-reset-under-lock))
               (old *wal-storage-incarnation*))
           (wal-logical-info-publish-under-lock t)
           (let ((current *wal-logical-info*))
             (rove:ok (rove:signals
                       (validate-primary-wal-writer-clog-reset-plan-under-lock plan) 'error))
             (rove:ok (eq old *wal-storage-incarnation*))
             (rove:ok (eq current *wal-logical-info*))
             (rove:ok (not *wal-poisoned*))))))))

(rove:deftest logical-info-overlapping-activation-failure-defers-disable
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (declare (ignore second))
       (call-with-logical-capture-armed
        (lambda ()
          (catch 'activation-cut
            (call-with-logical-capture-armed
             (lambda () (throw 'activation-cut :cut))
             :activation (make-wal-logical-info-activation)))
          (rove:ok (= 1 (hash-table-count *wal-logical-info-activations*)))
          (rove:ok *wal-logical-info-disable-pending*)
          (sb-thread:with-mutex (*wal-lock*)
            (rove:ok (eq :deferred (wal-process-pending-logical-disable-under-lock (lambda () nil)))))
          (rove:ok (wal-logical-info-backend-enabled first)))
        :activation (make-wal-logical-info-activation))
       (sb-thread:with-mutex (*wal-lock*)
         (rove:ok (eq :disabled
                      (wal-process-pending-logical-disable-under-lock (lambda () nil)))))
       (rove:ok (not (wal-logical-info-backend-enabled first)))
       (rove:ok (zerop *wal-end-lsn*)))))

(rove:deftest logical-info-activation-publishes-before-old-xid-drain
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock first))
       (let ((wait (symbol-function 'wait-for-xid)) (waited nil) (called nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'wait-for-xid)
                      (lambda (xid)
                        (rove:ok (= 42 xid))
                        (rove:ok (not (sb-thread:holding-mutex-p *xid-lock*)))
                        (rove:ok (wal-logical-info-backend-enabled second))
                        (rove:ok (not (wal-logical-info-backend-enabled first)))
                        (rove:ok (wal-logical-info-cache-pending (backend-logical-info-cache first)))
                        (rove:ok (zerop *wal-end-lsn*))
                        (setf waited t)))
                (call-with-logical-capture-armed
                 (lambda () (setf called t))
                 :activation (make-wal-logical-info-activation))
                (rove:ok waited)
                (rove:ok called))
           (setf (symbol-function 'wait-for-xid) wait))))))

(rove:deftest logical-info-storage-replacement-rejects-a-pre-activation-arm
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (declare (ignore first second))
       (let ((old *wal-storage-incarnation*) (info *wal-logical-info*))
         (sb-thread:with-mutex (*xid-lock*)
           (setf (gethash :pre-activation *logical-capture-arming*) t))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok
            (rove:signals
             (wal-logical-info-publish-under-lock
              nil :rebase t :incarnation (gensym "REPLACEMENT-"))
             'error)))
         (rove:ok (eq old *wal-storage-incarnation*))
         (rove:ok (eq info *wal-logical-info*))
         (rove:ok (not *wal-poisoned*))))))

(defun call-with-effective-level-test (level source recovery function &key pending)
  (let* ((snapshot (unless (eq source :unknown)
                     (%make-wal-logical-status source 32 152 1 nil nil)))
         (*startup-installed-configuration*
           (make-startup-configuration :settings (make-hash-table :test #'equal) :wal-level level))
         (*physical-recovery-state*
           (when recovery
             (list :timeline 1 :replay-timeline 1
                   :source-logical-fact (physical-logical-fact-from-state snapshot :checkpoint 32 1 152 1 nil nil)
                   :required-logical-fact
                   (physical-logical-fact-from-state
                    (%make-wal-logical-status (not source) 152 272 1 nil nil)
                    :checkpoint 152 1 272 1 nil nil))))
         (*last-replay-lsn* 152)
         (*backend* (make-backend))
         (*wal-storage-incarnation* (gensym "EFFECTIVE-LEVEL-"))
         (*wal-logical-info*
           (when snapshot
             (%make-wal-logical-info
              *wal-storage-incarnation*
              (sb-thread:with-mutex (*xid-lock*) (incf *wal-logical-info-generation*))
              source)))
         (*wal-level* (if (eq level :logical) :replica :logical))
         (*wal-ready* t) (*wal-poisoned* nil) (*wal-insert-timeline* 1)
         (*wal-durable-lsn* 152) (*wal-end-lsn* 152)
         (*logical-capture-force* (and snapshot source))
         (*wal-logical-policy*
           (%make-wal-logical-policy *wal-storage-incarnation*
                                     snapshot pending)))
    (sb-thread:with-mutex (*xid-lock*)
      (wal-logical-info-initialize-backend-under-xid-lock *backend*))
    (funcall function)))

(cl-spec:defproperty effective-level-recovery-uses-published-source
    ((source (member :unknown nil t)) (level (member :replica :logical)))
  (:about startup-effective-wal-level)
  (:trials (:normal 100))
  (call-with-effective-level-test
   level source t
   (lambda ()
     (let ((before (copy-tree *physical-recovery-state*)))
       (and (string= (if (eq source t) "logical" "replica") (startup-effective-wal-level))
            (equal before *physical-recovery-state*))))))

(rove:deftest effective-level-recovery-ignores-local-and-future-policy
    (dolist (level '(:replica :logical))
      (dolist (source '(:unknown nil t))
        (call-with-effective-level-test
         level source t
         (lambda ()
           (let ((before (copy-tree *physical-recovery-state*)))
             (rove:ok (string= (if (eq source t) "logical" "replica")
                               (startup-effective-wal-level)))
             (rove:ok (equal before *physical-recovery-state*))
             (when (eq source :unknown)
               (rove:ok (null (getf *physical-recovery-state* :source-logical-fact))))))))))

(rove:deftest effective-level-primary-uses-configured-level-and-published-status
    (dolist (level '(:replica :logical))
      (dolist (source '(nil t))
        (dolist (pending (if source '(nil t) '(nil)))
          (call-with-effective-level-test
           level source nil
           (lambda ()
             (rove:ok (string= (if (or source (eq level :logical)) "logical" "replica")
                               (startup-effective-wal-level))))
           :pending pending))))
  (call-with-effective-level-test
   :replica :unknown nil
   (lambda ()
     (rove:ok (rove:signals (startup-effective-wal-level) 'error))
     (setf *wal-logical-policy* :unrecovered)
     (rove:ok (rove:signals (startup-effective-wal-level) 'error)))))

(rove:deftest effective-level-sql-value-and-settings-row-share-readonly-authority
    (call-with-effective-level-test
     :logical nil t
     (lambda ()
       (rove:ok (string= "replica" (sql-setting-value "EFFECTIVE_WAL_LEVEL")))
       (let* ((row (startup-effective-wal-level-settings-row *startup-installed-configuration*))
              (choices (sql-array-elements (aref row 6))))
         (rove:ok (equalp (vector "effective_wal_level" "replica" +null+ "internal" "enum" "default")
                          (subseq row 0 6)))
         (rove:ok (equalp #("minimal" "replica" "logical") choices))
         (rove:ok (every (lambda (x) (eq x +null+)) (list (aref row 7) (aref row 8))))
         (rove:ok (null (aref row 9))))
       (let ((rows (remove-if-not
                    (lambda (row) (string= "effective_wal_level" (aref row 0)))
                    (startup-settings-rows))))
         (rove:ok (= 1 (length rows)))
         (rove:ok (string= "replica" (aref (first rows) 1))))
       (dolist (operation
                (list (lambda () (startup-check-session-setting "effective_wal_level"))
                      (lambda () (exec-set (make-broadcast-stream)
                                           (first (parse-statements "SET effective_wal_level = logical"))))
                      (lambda () (exec-reset (make-broadcast-stream)
                                             (first (parse-statements "RESET effective_wal_level"))))
                      (lambda () (set-config-value "effective_wal_level" "logical" nil))))
         (let ((condition (handler-case (progn (funcall operation) nil)
                            (pg-error (condition) condition))))
           (rove:ok (and condition (string= "55P02" (pg-error-sqlstate condition))))))
       (rove:ok (null (assoc "effective_wal_level" *startup-setting-specs* :test #'string=))))))

(rove:deftest effective-level-generated-recovery-contract
    (unit-check-property 'effective-level-recovery-uses-published-source))

(defun call-with-logical-policy-test (function)
  (let ((*wal-logical-policy* nil) (*logical-capture-force* nil)
                                   (*wal-level* :replica) (*wal-logical-policy-test-hook* nil)
                                   (*logical-capture-live-slots* 0)
                                   (*logical-capture-arming* (make-hash-table :test #'eq))
                                   (*logical-capture-xid-modes* (make-hash-table :test #'eql)))
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((*backend* nil))
         (sb-thread:with-mutex (*wal-lock*)
           (wal-logical-info-publish-under-lock nil :rebase t))
         (funcall function writer))))))

(rove:deftest logical-info-activation-continues-through-clog-reset
    (call-with-logical-policy-test
     (lambda (writer)
       (let ((old *wal-storage-incarnation*) (generation nil))
         (rove:ok
          (eq :cut
              (catch 'activation-cut
                (call-with-logical-capture-armed
                 (lambda ()
                   (setf generation (wal-logical-info-generation *wal-logical-info*))
                   (clog-reset)
                   (rove:ok (not (eq old *wal-storage-incarnation*)))
                   (rove:ok
                    (eq *wal-storage-incarnation*
                        (wal-writer-epoch-incarnation (primary-wal-writer-epoch writer))))
                   (rove:ok (= generation (wal-logical-info-generation *wal-logical-info*)))
                   (rove:ok (= 65 (wal-report-logical-decoding-status t)))
                   (throw 'activation-cut :cut))
                 :activation (make-wal-logical-info-activation)))))
         (rove:ok *wal-logical-info-disable-pending*)
         (rove:ok (wal-logical-status-enabled (wal-logical-policy-state *wal-logical-policy*)))
         (with-wal-publication-fence (begin)
           (wal-process-pending-logical-disable-under-lock #'begin))
         (rove:ok (= 130 *wal-end-lsn*))
         (rove:ok (not (wal-logical-status-enabled (wal-logical-policy-state *wal-logical-policy*))))
         (rove:ok (not (wal-logical-info-enabled *wal-logical-info*)))
         (with-wal-publication-fence (begin)
           (wal-process-pending-logical-disable-under-lock #'begin))
         (rove:ok (= 130 *wal-end-lsn*))
         (rove:ok (not *wal-poisoned*))))))

(rove:deftest logical-info-arm-during-off-pins-capture-independently-of-reporting-cache
    (call-with-logical-info-barrier-test
     (lambda (first second)
       (call-with-logical-policy-test
        (lambda (writer)
          (declare (ignore writer))
          (wal-report-logical-decoding-status t)
          (let ((token (gensym "RACING-ACTIVATION-")) (armed nil) (assigned nil))
            (unwind-protect
                 (progn
                   (let ((*wal-logical-policy-test-hook*
                           (lambda (point candidate)
                             (unless (wal-logical-status-enabled
                                      (wal-logical-policy-state candidate))
                               (case point
                                 (:before-record
                                  (sb-thread:with-mutex (*xid-lock*)
                                    (setf (gethash token *logical-capture-arming*) t
                                          armed t)))
                                 (:after-state
                                  (sb-thread:with-mutex (*xid-lock*)
                                    (xid-allocate-under-lock first)
                                    (setf assigned t))))))))
                     (wal-report-logical-decoding-status nil))
                   (rove:ok armed)
                   (rove:ok assigned)
                   (rove:ok (not (wal-logical-info-backend-enabled first)))
                   (rove:ok (eq :capture
                                (gethash (backend-xid first) *logical-capture-xid-modes*)))
                   (rove:ok (logical-note-xid-mutation-needs-capture-p (backend-xid first)))
                   (rove:ok (= 195 (wal-report-logical-decoding-status t)))
                   (rove:ok (not (wal-logical-info-backend-enabled first)))
                   (rove:ok (wal-logical-info-cache-pending (backend-logical-info-cache first)))
                   (rove:ok (wal-logical-info-backend-enabled second))
                   (rove:ok (logical-capture-xid-needed-p (backend-xid first)))
                   (rove:ok (eq :capture-written
                                (gethash (backend-xid first) *logical-capture-xid-modes*)))
                   (rove:ok (not *wal-poisoned*)))
              (sb-thread:with-mutex (*xid-lock*)
                (remhash token *logical-capture-arming*)))))))))

(cl-spec:defproperty logical-policy-epoch-transfer-preserves-state
    ((status (member :unknown nil t)) (pending (member nil t)))
  (:about wal-logical-policy-rebind-incarnation)
  (:trials (:normal 100))
  (let* ((old (gensym "LOGICAL-OLD-")) (new (gensym "LOGICAL-NEW-"))
                                       (state (wal-logical-status-from-checkpoint
                                               (unit-logical-status-snapshot status nil nil :unknown)))
                                       (pending (and pending state (wal-logical-status-enabled state)))
                                       (policy (%make-wal-logical-policy old state pending))
                                       (copy (wal-logical-policy-rebind-incarnation policy old new)))
    (and (not (eq policy copy))
         (eq old (wal-logical-policy-incarnation policy))
         (eq new (wal-logical-policy-incarnation copy))
         (eq state (wal-logical-policy-state copy))
         (eq pending (wal-logical-policy-pending-disable copy)))))

(rove:deftest logical-policy-publication-and-pending-disable-keep-capture
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (rove:ok (= 65 (wal-report-logical-decoding-status t)))
       (rove:ok *logical-capture-force*)
       (let ((slot (make-wal-replication-slot :name "logical" :kind :logical :restart-lsn 0)))
         (sb-thread:with-mutex (*wal-lock*)
           (setf (gethash "logical" *wal-replication-slots*) slot)
           (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
           (rove:ok (not (wal-logical-policy-pending-disable *wal-logical-policy*)))
           (remhash "logical" *wal-replication-slots*)
           (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
           (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
           (rove:ok (eql 0 *logical-capture-live-slots*))
           (rove:ok *logical-capture-force*)))
       (let ((first (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock))))
         (rove:ok (eq :capture (gethash first *logical-capture-xid-modes*)))
         (with-wal-publication-fence (begin)
           (wal-process-pending-logical-disable-under-lock #'begin))
         (rove:ok (not *logical-capture-force*))
         (rove:ok (not (wal-logical-policy-pending-disable *wal-logical-policy*)))
         (rove:ok (eq :capture (gethash first *logical-capture-xid-modes*)))
         (let ((next (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock))))
           (rove:ok (eq :skip (gethash next *logical-capture-xid-modes*))))))))

(rove:deftest logical-policy-disable-defers-an-arm-without-losing-the-request
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (let ((token (gensym "PENDING-ARM-")) (end *wal-end-lsn*))
         (sb-thread:with-mutex (*xid-lock*) (setf (gethash token *logical-capture-arming*) t))
         (with-wal-publication-fence (begin)
           (rove:ok (eq :deferred (wal-process-pending-logical-disable-under-lock #'begin))))
         (rove:ok (= end *wal-end-lsn*))
         (rove:ok *logical-capture-force*)
         (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
         (sb-thread:with-mutex (*xid-lock*) (remhash token *logical-capture-arming*))
         (with-wal-publication-fence (begin)
           (wal-process-pending-logical-disable-under-lock #'begin))
         (rove:ok (> *wal-end-lsn* end))
         (rove:ok (not *logical-capture-force*))))))

(rove:deftest logical-policy-physical-slots-do-not-enable-capture
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status nil)
       (sb-thread:with-mutex (*wal-lock*)
         (setf (gethash "physical" *wal-replication-slots*)
               (make-wal-replication-slot :name "physical" :kind :physical))
         (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
         (rove:ok (equal '(:logical-decoding-enabled nil)
                         (wal-current-logical-decoding-fields)))
         (rove:ok (not *logical-capture-force*))
         (rove:ok (not (wal-logical-policy-pending-disable *wal-logical-policy*)))))))

(rove:deftest logical-policy-clog-reset-preserves-status-and-pending-disable
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (let ((old *wal-logical-policy*) (state (wal-logical-policy-state *wal-logical-policy*)))
         (clog-reset)
         (rove:ok (not (eq old *wal-logical-policy*)))
         (rove:ok (not (eq (wal-logical-policy-incarnation old) *wal-storage-incarnation*)))
         (rove:ok (eq *wal-storage-incarnation* (wal-logical-policy-incarnation *wal-logical-policy*)))
         (rove:ok (eq state (wal-logical-policy-state *wal-logical-policy*)))
         (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
         (rove:ok *logical-capture-force*)))))

(rove:deftest logical-policy-disable-tolerates-an-arm-between-rechecks
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (let ((count (symbol-function 'wal-logical-arm-count-under-lock))
             (calls 0) (token (gensym "RACING-ARM-")) (end *wal-end-lsn*))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-logical-arm-count-under-lock)
                      (lambda ()
                        (incf calls)
                        (when (= calls 2)
                          (sb-thread:with-mutex (*xid-lock*)
                            (setf (gethash token *logical-capture-arming*) t)))
                        (funcall count)))
                (with-wal-publication-fence (begin)
                  (rove:ok (eq :deferred
                               (wal-process-pending-logical-disable-under-lock #'begin))))
                (rove:ok (= 2 calls))
                (rove:ok (= end *wal-end-lsn*))
                (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
                (rove:ok *logical-capture-force*)
                (rove:ok (not *wal-poisoned*)))
           (setf (symbol-function 'wal-logical-arm-count-under-lock) count)
           (sb-thread:with-mutex (*xid-lock*)
             (remhash token *logical-capture-arming*)))))))

(rove:deftest logical-policy-checkpoint-fences-incomplete-disable
    (dolist (point '(:before-record :after-record :after-flush :after-state))
      (dolist (nonlocal '(nil t))
        (call-with-logical-policy-test
         (lambda (writer)
           (declare (ignore writer))
           (wal-report-logical-decoding-status t)
           (let ((shutdown (symbol-function 'request-fast-shutdown))
                 (stops 0)
                 (*wal-logical-policy-test-hook*
                   (lambda (stage candidate)
                     (declare (ignore candidate))
                     (when (eq stage point)
                       (if nonlocal (throw 'logical-checkpoint-exit :escaped)
                           (error "injected checkpoint logical status failure"))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'request-fast-shutdown) (lambda () (incf stops)))
                    (rove:ok (unit-commit-ts-error-p
                              (lambda () (catch 'logical-checkpoint-exit (wal-checkpoint-core)))))
                    (rove:ok *wal-poisoned*)
                    (rove:ok (plusp stops))
                    (rove:ok (zerop *wal-checkpoint-lsn*)))
               (setf (symbol-function 'request-fast-shutdown) shutdown))))))))

(rove:deftest logical-policy-stale-clog-plan-has-no-file-effects
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (sb-thread:with-mutex (*wal-lock*)
         (let ((plan (prepare-primary-wal-writer-clog-reset-under-lock))
               (incarnation *wal-storage-incarnation*) (effects 0))
           (wal-logical-set-pending-under-lock *wal-logical-policy* nil)
           (rove:ok
            (handler-case
                (progn
                  (clog-reset-file-under-wal-lock
                   (lambda (stream) (declare (ignore stream)) (incf effects))
                   (lambda (directory) (declare (ignore directory)) (incf effects))
                   plan)
                  nil)
              (wal-input-validation-error () t)))
           (rove:ok (zerop effects))
           (rove:ok (eq incarnation *wal-storage-incarnation*))
           (rove:ok (not *wal-poisoned*)))))))

(rove:deftest logical-policy-pending-disable-requires-enabled-state
    (let ((owner (gensym "PENDING-OWNER-")) (target (gensym "PENDING-TARGET-")))
      (dolist (status '(:unknown nil))
        (let* ((state (wal-logical-status-from-checkpoint
                       (unit-logical-status-snapshot status nil nil :unknown)))
               (policy (%make-wal-logical-policy owner state t)))
          (rove:ok (not (wal-logical-policy-valid-p policy)))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (wal-logical-policy-rebind-incarnation policy owner target))))))))

(rove:deftest logical-policy-generated-epoch-transfer
    (unit-check-property 'logical-policy-epoch-transfer-preserves-state))
