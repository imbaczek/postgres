;;;; exec_owner.lisp -- query execution lifetime and reload ownership.
(in-package #:pg)

(defstruct (query-execution-owner (:constructor %make-query-execution-owner))
  ;; The creating thread owns the retained bindings, snapshot and resources.
  ;; The global census keeps the owner visible until cleanup really completes.
  backend thread bindings snapshot-registration temp-owner cleanup-functions
  (state :open))

(defvar *query-execution-owner* nil)

(defvar *query-executor-tables* nil
  "Physical scan tables retained by this query owner until it closes.")

(defun retain-query-plan-executor-tables (plan relations)
  "Record physical scan tables used by PLAN and its child plans.
Lazy child plans count even before they run.  Virtual relations do not retain a
table.  With no query owner there is no persistent executor to record."
  (when *query-execution-owner*
    (let ((seen (make-hash-table :test #'eq)))
      (labels ((visit (plan relations)
                 (unless (gethash plan seen)
                   (setf (gethash plan seen) t)
                   (dolist (node (collect-query-plan-nodes (query-plan-physical plan)))
                     (case (first node)
                       (:with-ctes
                           ;; C initializes shared producers even if their readers
                           ;; were removed. Unused definitions have no plan.
                           (dolist (definition (second node))
                             (let ((child (sql-cte-plan definition)))
                               (when (and child (sql-cte-shared-p definition))
                                 (visit child (subquery-registered-relations child))))))
                       ((:seq-scan :index-scan)
                        (let ((relation (physical-scan-relation node relations)))
                          (unless (getf relation :virtual-p)
                            (pushnew (getf relation :table) *query-executor-tables* :test #'eq))))
                       (:cte-scan
                        (let* ((relation (physical-scan-relation node relations))
                               (definition (getf relation :cte))
                               (child (or (getf relation :cte-reference-plan)
                                          (and definition (sql-cte-plan definition)))))
                          (when child (visit child (subquery-registered-relations child)))))))
                   (dolist (descriptor (query-plan-expression-subplans plan))
                     (let ((child (sql-subplan-plan descriptor)))
                       (visit child (subquery-registered-relations child)))))))
        (visit plan relations)))))

(defvar *query-execution-owners* (make-hash-table :test #'eq))

(defvar *query-execution-owner-lock*
  (sb-thread:make-mutex :name "pg persistent query owners"))

(defvar *query-execution-owner-cv*
  (sb-thread:make-waitqueue :name "pg persistent query owner change"))

(defvar *query-execution-owner-generation* 0)

(defmacro with-query-execution-owner-registry-update (&body body)
  "Publish a census change and wake reload waiters under the registry lock."
  `(sb-thread:with-mutex (*query-execution-owner-lock*)
     ,@body
     (incf *query-execution-owner-generation*)
     (sb-thread:condition-broadcast *query-execution-owner-cv*)))

(defvar *query-execution-owner-reload-wait-hook* nil)

(defparameter *query-execution-owner-context-symbols*
  '(*query-parameter-types* *query-parameter-values*
    *subquery-execution-active-p* *subquery-result-cache* *subquery-registered-relations*
    *subquery-outer-frames* *subquery-output-value*
    *subquery-instrumentation-depth* *cte-execution-context* *cte-open-states*
    *cte-active-plan* *sql-open-iterators* *query-executor-tables* *recursive-cte-worktables*
    *recursive-cte-generation-caches* *lateral-outer-environment*
    *datum-origin-capture* *query-row-origins* *query-row-presence* *select-toast-demand*
    *select-toast-slice-plan* *toast-slice-current* *query-plan-instrumentation-p*
    *select-index-scans* *select-index-lease-domain* *select-index-lease-groups*
    *select-result-formats* *select-suppress-description* *select-expected-result-columns*
    *query-temp-session-id* *dml-column-defaults* *dml-returning-spool*))

(defun query-execution-owner-closed-p (owner)
  (eq (query-execution-owner-state owner) :closed))

(defun query-execution-owner-snapshot (owner)
  (snapshot-registration-snapshot (query-execution-owner-snapshot-registration owner)))

(defun query-execution-owner-binding (owner symbol)
  (let ((entry (assoc symbol (query-execution-owner-bindings owner))))
    (unless entry (error "query owner does not retain ~s" symbol))
    (if (eq owner *query-execution-owner*) (symbol-value symbol) (cdr entry))))

(defun (setf query-execution-owner-binding) (value owner symbol)
  (unless (and (eq (query-execution-owner-thread owner) sb-thread:*current-thread*)
               (eq (query-execution-owner-state owner) :open))
    (error "query owner is closed or belongs to another thread"))
  (let ((entry (assoc symbol (query-execution-owner-bindings owner))))
    (unless entry (error "query owner does not retain ~s" symbol))
    (when (eq owner *query-execution-owner*) (setf (symbol-value symbol) value))
    (setf (cdr entry) value)))

(defun query-execution-owner-reload-state ()
  "Copy the owners retaining executable state, together with their census generation."
  (sb-thread:with-mutex (*query-execution-owner-lock*)
    (values (loop for owner being the hash-keys of *query-execution-owners* collect owner)
            *query-execution-owner-generation*)))

(defun query-execution-owner-thread-retains-state-p (&optional (thread sb-thread:*current-thread*))
  (sb-thread:with-mutex (*query-execution-owner-lock*)
    (loop for owner being the hash-keys of *query-execution-owners*
            thereis (eq thread (query-execution-owner-thread owner)))))

(defun wait-for-reload-condition (queue mutex check-cancel)
  "Return with MUTEX owned, including after an SBCL timed wait releases it.
A timed-out wait leaves MUTEX unowned; check cancellation in that state and
reacquire before returning. Caller cleanup must permit cancellation while unowned.
Without CHECK-CANCEL, use the ordinary untimed condition wait."
  (when check-cancel (funcall check-cancel))
  (if check-cancel
      (unless (sb-thread:condition-wait queue mutex :timeout 0.05)
        (funcall check-cancel)
        (sb-sys:without-interrupts
          (sb-sys:allow-with-interrupts (sb-thread:grab-mutex mutex))))
      (sb-thread:condition-wait queue mutex))
  (when check-cancel (funcall check-cancel))
  (values))

(defun wait-for-query-execution-owner-change (generation &optional check-cancel)
  "Wait after reload admission has reopened, allowing clients to retire owners."
  (when *query-execution-owner-reload-wait-hook*
    (funcall *query-execution-owner-reload-wait-hook* generation))
  (sb-thread:with-mutex (*query-execution-owner-lock*)
    (loop while (and (= generation *query-execution-owner-generation*)
                     (plusp (hash-table-count *query-execution-owners*)))
          do (wait-for-reload-condition *query-execution-owner-cv* *query-execution-owner-lock*
                                        check-cancel)))
  (values))

(defun make-query-execution-owner (&key (backend *backend*) bindings)
  "Publish ownership before snapshot, iterator, or temporary-file acquisition."
  (let* ((context
           (mapcar (lambda (symbol) (cons symbol (and (boundp symbol) (symbol-value symbol))))
                   *query-execution-owner-context-symbols*))
         (quota (backend-query-temp-quota backend))
         (temporary (make-query-temp-scope-owner quota))
         (owner (%make-query-execution-owner
                 :backend backend :thread sb-thread:*current-thread* :bindings context
                 :snapshot-registration (make-snapshot-registration) :temp-owner temporary)))
    (setf (query-temp-cleanup-owner-temporary-directory temporary) *query-temp-directory*)
    ;; A new owner must not inherit executor tables or other mutable state from
    ;; an enclosing query. Its saved bindings survive until the owner closes.
    (dolist (symbol '(*cte-execution-context* *recursive-cte-worktables*
                      *recursive-cte-generation-caches* *subquery-outer-frames*
                      *subquery-output-value* *subquery-registered-relations*
                      *cte-active-plan* *query-executor-tables*
                      *lateral-outer-environment* *query-row-origins*
                      *toast-slice-current* *select-index-scans*
                      *select-index-lease-domain* *select-index-lease-groups*))
      (setf (cdr (assoc symbol context)) nil))
    (dolist (symbol '(*subquery-result-cache* *cte-open-states* *sql-open-iterators*))
      (setf (cdr (assoc symbol context)) (make-hash-table :test #'eq)))
    (setf (cdr (assoc '*subquery-execution-active-p* context)) t
          (cdr (assoc '*subquery-instrumentation-depth* context)) 0)
    (sb-sys:without-interrupts
      (with-query-execution-owner-registry-update
        (setf (gethash owner *query-execution-owners*) t))
      (let ((complete nil))
        (unwind-protect
             (sb-sys:with-local-interrupts
               (dolist (entry bindings)
                 (setf (query-execution-owner-binding owner (car entry)) (cdr entry)))
               (setf complete t)
               owner)
          (unless complete (close-query-execution-owner owner)))))))

(defun call-with-query-execution-owner-context (owner function)
  ;; Rebind the saved query state for this pull and save changes on exit.
  ;; Nested use of the same owner already has those dynamic bindings.
  (unless (eq (query-execution-owner-thread owner) sb-thread:*current-thread*)
    (error "query owner belongs to another thread"))
  (if (eq owner *query-execution-owner*)
      (funcall function)
      (let* ((entries (query-execution-owner-bindings owner))
             (*query-execution-owner* owner)
             (*backend* (or (query-execution-owner-backend owner) *backend*))
             (*current-snapshot* (query-execution-owner-snapshot owner))
             (*statement-snapshot-registrations*
               (cons (query-execution-owner-snapshot-registration owner)
                     *statement-snapshot-registrations*)))
        (progv (mapcar #'car entries) (mapcar #'cdr entries)
          (call-with-query-temp-owner
           (query-execution-owner-temp-owner owner)
           (lambda ()
             (unwind-protect
                  (funcall function)
               ;; Close may have run inside FUNCTION. Never restore its released
               ;; context from these still dynamically bound values.
               (unless (query-execution-owner-closed-p owner)
                 (dolist (entry entries)
                   (setf (cdr entry) (symbol-value (car entry)))))))
           (eq (query-execution-owner-state owner) :open))))))

(defun call-with-query-execution-owner (owner function)
  "Resume a query with its original dynamic state, retaining it on return."
  (unless (eq (query-execution-owner-state owner) :open)
    (error "query owner is not open"))
  (if (eq owner *query-execution-owner*)
      (funcall function)
      (call-with-query-execution-owner-context
       owner (lambda ()
               (unwind-protect (funcall function)
                 (detach-select-index-scans))))))

(defun query-execution-owner-acquire-snapshot (owner acquire-function)
  "Pass the owner's unique token to the snapshot acquisition boundary."
  (let ((registration (query-execution-owner-snapshot-registration owner)))
    (when (or (snapshot-registration-active-p registration)
              (snapshot-registration-releasing-p registration))
      (error "query owner already retains a snapshot"))
    (call-with-query-execution-owner
     owner
     (lambda ()
       (let ((snapshot (funcall acquire-function registration)))
         (unless (eq snapshot (snapshot-registration-snapshot registration))
           (error "snapshot acquisition did not publish the owner's token"))
         (setf *current-snapshot* snapshot))))))

(defun query-execution-owner-pin-snapshot (owner snapshot)
  "Copy an already protected snapshot before the enclosing registration releases.
The owner retains its own registration until its executable state is retired."
  (query-execution-owner-acquire-snapshot
   owner
   (lambda (registration)
     (sb-thread:with-mutex (*xid-lock*)
       (sb-thread:with-mutex (*snapshot-count-lock*)
         (unless (gethash snapshot *active-snapshot-set*)
           (error "source snapshot has no live registration")))
       (register-active-snapshot (copy-snapshot snapshot) registration)))))

(defun query-execution-owner-add-cleanup (owner function)
  "Retain a resource closer before returning control to the frontend."
  (unless (and (eq (query-execution-owner-state owner) :open)
               (eq (query-execution-owner-thread owner) sb-thread:*current-thread*))
    (error "query owner is not open on this thread"))
  (push function (query-execution-owner-cleanup-functions owner))
  function)

(defun close-query-execution-owner (owner)
  "Close owned state without a registry mutex. Return success and cleanup failures.
Retain the snapshot and census entry while any closer needs a retry."
  (when (query-execution-owner-closed-p owner)
    (return-from close-query-execution-owner t))
  (when (eq (query-execution-owner-state owner) :closing)
    (return-from close-query-execution-owner nil))
  (unless (eq (query-execution-owner-thread owner) sb-thread:*current-thread*)
    (error "query owner belongs to another thread"))
  (sb-sys:without-interrupts
    (setf (query-execution-owner-state owner) :closing)
    (unwind-protect
         (let ((failures nil) (retry nil) (fatal-condition nil))
           (flet ((attempt (function)
                    (handler-case (funcall function)
                      (error (condition)
                        (when (and (fatal-error-p condition) (null fatal-condition))
                          (setf fatal-condition condition))
                        (push condition failures)
                        (push function retry))
                      (sb-ext:timeout (condition)
                        (push condition failures) (push function retry)))))
             (call-with-query-execution-owner-context
              owner
              (lambda ()
                (dolist (function (query-execution-owner-cleanup-functions owner))
                  (attempt function))
                (dolist (iterator (loop for iterator being the hash-keys of *sql-open-iterators*
                                        collect iterator))
                  (let ((owned iterator))
                    (attempt (lambda () (close-sql-iterator owned)))))
                (dolist (state (loop for state being the hash-keys of *cte-open-states*
                                     collect state))
                  (let ((owned state))
                    (attempt (lambda () (close-cte-execution-state owned)))))
                (attempt (lambda () (close-cte-frame-cache *subquery-result-cache*)))
                (attempt #'detach-select-index-scans)
                (attempt (lambda () (close-query-temp-owner
                                     (query-execution-owner-temp-owner owner))))
                ;; Keep tuple visibility protected until executable state has
                ;; actually retired, including a closer that must be retried.
                (unless retry
                  (attempt (lambda () (release-snapshot-registration
                                       (query-execution-owner-snapshot-registration owner)))))))
             (setf (query-execution-owner-cleanup-functions owner) (nreverse retry))
             (if (or retry
                     (snapshot-registration-active-p
                      (query-execution-owner-snapshot-registration owner))
                     (snapshot-registration-releasing-p
                      (query-execution-owner-snapshot-registration owner)))
                 (setf (query-execution-owner-state owner) :cleanup-pending)
                 (progn
                   (setf (query-execution-owner-state owner) :closed
                         (query-execution-owner-bindings owner) nil
                         (query-execution-owner-backend owner) nil)
                   (with-query-execution-owner-registry-update
                     (remhash owner *query-execution-owners*))))
             (when fatal-condition (error fatal-condition))
             (values (query-execution-owner-closed-p owner) (nreverse failures))))
      (when (eq (query-execution-owner-state owner) :closing)
        (setf (query-execution-owner-state owner) :cleanup-pending)))))

(defun close-query-execution-owners-for-backend (backend)
  "Retire BACKEND's owners before propagating a fatal cleanup condition."
  (let ((owners
          (sb-thread:with-mutex (*query-execution-owner-lock*)
            (loop for owner being the hash-keys of *query-execution-owners*
                  when (eq backend (query-execution-owner-backend owner)) collect owner)))
        (fatal-condition nil))
    (dolist (owner owners)
      (dotimes (attempt 2)
        (unless (query-execution-owner-closed-p owner)
          (handler-case (close-query-execution-owner owner)
            (error (condition)
              (when (and (fatal-error-p condition) (null fatal-condition))
                (setf fatal-condition condition)))
            (sb-ext:timeout () nil)))))
    (when fatal-condition (error fatal-condition))
    (remove-if #'query-execution-owner-closed-p owners)))

(defun retire-query-execution-owners-for-backend (backend)
  "Discard executable state only after its backend's frontend stack has unwound."
  (let* ((owners
           (remove-if-not (lambda (owner) (eq backend (query-execution-owner-backend owner)))
                          (query-execution-owner-reload-state)))
         (failures nil))
    (labels ((attempt (function)
               (handler-case (funcall function)
                 (error (condition) (push condition failures) nil)
                 (sb-ext:timeout (condition) (push condition failures) nil))))
      (dolist (owner owners)
        (unless (eq (query-execution-owner-thread owner) sb-thread:*current-thread*)
          (error "terminal query cleanup requires the owning backend thread"))
        (dotimes (retry 2)
          (unless (query-execution-owner-closed-p owner)
            (attempt (lambda ()
                       (multiple-value-bind (closed errors) (close-query-execution-owner owner)
                         (setf failures (append errors failures))
                         closed))))))
      (dolist (owner owners)
        (unless (query-execution-owner-closed-p owner)
          (setf (query-execution-owner-state owner) :closing
                (query-execution-owner-cleanup-functions owner) nil)
          (call-with-query-execution-owner-context
           owner
           (lambda ()
             (dolist (iterator (loop for iterator being the hash-keys of *sql-open-iterators*
                                     collect iterator))
               (attempt (lambda () (close-sql-iterator iterator))))
             (dolist (state (loop for state being the hash-keys of *cte-open-states* collect state))
               (attempt (lambda () (close-cte-execution-state state))))
             ;; Exit discards unread TIDs. Parking's detachment could allocate a
             ;; spool and deliberately retain a lease after an I/O or quota error.
             (let ((leases
                     (remove-duplicates
                      (append *btree-read-leases*
                              (mapcar #'select-index-scan-lease *select-index-scans*)
                              (mapcan (lambda (group)
                                        (mapcar #'cdr (select-index-lease-group-leases group)))
                                      *select-index-lease-groups*))
                      :test #'eq)))
               (dolist (lease leases)
                 (when lease (attempt (lambda () (release-btree-read-lease lease)))))
               (when (some (lambda (lease) (and lease (btree-read-lease-active-p lease))) leases)
                 (error "terminal query cleanup left an owned B-tree lease")))
             (setf *select-index-scans* nil *select-index-lease-groups* nil)
             (close-query-temp-owner (query-execution-owner-temp-owner owner))
             ;; SQL owner registrations have no independent WAL-retention pin.
             ;; Release is idempotent and may finish an interrupted generation.
             (let ((registration (query-execution-owner-snapshot-registration owner)))
               (attempt (lambda () (release-snapshot-registration registration)))
               (when (snapshot-registration-releasing-p registration)
                 (finish-snapshot-registration-release
                  registration (snapshot-registration-lifecycle-generation registration)
                  (snapshot-registration-wal-retention-lsn registration)))
               (when (or (snapshot-registration-active-p registration)
                         (snapshot-registration-releasing-p registration))
                 (error "terminal query cleanup left an active snapshot")))))
          (setf (query-execution-owner-state owner) :closed
                (query-execution-owner-bindings owner) nil
                (query-execution-owner-backend owner) nil)
          (with-query-execution-owner-registry-update
            (remhash owner *query-execution-owners*))))
      (dolist (condition failures)
        (format *error-output* "~&pg-lisp: backend query cleanup: ~a~%" condition))
      (nreverse failures))))
