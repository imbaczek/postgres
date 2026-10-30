;;;; exec_cte.lisp -- lexical owners and parameter-epoch shared WITH state.
(in-package #:pg)

(defstruct cte-execution-state definition store cache closed-p)

(defvar *recursive-cte-worktables* nil)

(defvar +recursive-cte-cache-key+ (gensym "RECURSIVE-CTE-CACHE-"))

(defun recursive-cte-parameter-cache (owner generation)
  ;; The ordinary parameter owner holds a private map of generation subcaches.
  ;; Register the owner in the generation too, so either lifetime can clean up.
  (let* ((namespaces (or (gethash +recursive-cte-cache-key+ owner)
                         (setf (gethash +recursive-cte-cache-key+ owner)
                               (make-hash-table :test #'eq))))
         (cache (or (gethash generation namespaces)
                    (setf (gethash generation namespaces) (make-hash-table :test #'eq)))))
    (setf (gethash owner generation) cache)
    cache))

(defun close-recursive-cte-generation (generation)
  ;; Remove both registrations after closing each subcache; retaining owners
  ;; here would retain per-row parameter frames beyond the generation.
  (when generation
    (dolist (owner (loop for owner being the hash-keys of generation collect owner))
      (let* ((namespaces (gethash +recursive-cte-cache-key+ owner))
             (cache (and namespaces (gethash generation namespaces))))
        (unwind-protect (when cache (close-cte-frame-cache cache))
          (when namespaces
            (remhash generation namespaces)
            (when (zerop (hash-table-count namespaces))
              (remhash +recursive-cte-cache-key+ owner)))
          (remhash owner generation))))))

(defstruct recursive-cte-seen
  (table (make-hash-table :test #'equal)) (bytes 0)
  (paths (make-array 64 :initial-element nil))
  (streams (make-array 64 :initial-element nil)))

(defun recursive-cte-seen-key (row targets)
  ;; Use SQL grouping keys for supported integral numeric, NULL, array and
  ;; record values. EQUAL keeps character values case-sensitive.
  (loop for value across row for target in targets
        collect (sql-group-value-key value (getf target :type))))

(defun recursive-cte-seen-write (seen key)
  (let* ((bucket (mod (sxhash key) (length (recursive-cte-seen-streams seen))))
         (stream (aref (recursive-cte-seen-streams seen) bucket)))
    (unless stream
      (sb-sys:without-interrupts
        (multiple-value-bind (path output) (open-query-temp-output)
          (setf stream output
                (aref (recursive-cte-seen-paths seen) bucket) path
                (aref (recursive-cte-seen-streams seen) bucket) output))))
    (file-position stream :end)
    (query-spill-write-record stream key)))

(defun recursive-cte-seen-new-p (seen key)
  ;; UNION's seen set is global across generations. Once work memory is full,
  ;; spill to buckets; each disk lookup scans its bucket linearly.
  (let ((table (recursive-cte-seen-table seen)))
    (when table
      (when (gethash key table) (return-from recursive-cte-seen-new-p nil))
      (when (<= (+ (recursive-cte-seen-bytes seen) 128 (query-spill-value-size key))
                *query-work-memory-bytes*)
        (setf (gethash key table) t)
        (incf (recursive-cte-seen-bytes seen) (+ 128 (query-spill-value-size key)))
        (return-from recursive-cte-seen-new-p t))
      (maphash (lambda (old present) (declare (ignore present))
                 (recursive-cte-seen-write seen old)) table)
      (clrhash table)
      (setf (recursive-cte-seen-table seen) nil
            (recursive-cte-seen-bytes seen) 0)))
  (let* ((bucket (mod (sxhash key) (length (recursive-cte-seen-streams seen))))
         (stream (aref (recursive-cte-seen-streams seen) bucket)))
    (when stream
      (finish-output stream)
      (with-query-temp-input (input (aref (recursive-cte-seen-paths seen) bucket))
        (loop for old = (query-spill-read-record input)
              until (eq old +query-spill-eof+)
              do (check-for-interrupts)
                 (when (equal old key) (return-from recursive-cte-seen-new-p nil)))))
    (recursive-cte-seen-write seen key)
    t))

(defun close-recursive-cte-seen (seen)
  (when seen
    (unwind-protect
         (loop for stream across (recursive-cte-seen-streams seen)
               when stream do (finish-query-temp-stream stream nil))
      (loop for path across (recursive-cte-seen-paths seen)
            when path do (delete-query-temp-file path)))
    (setf (recursive-cte-seen-table seen) nil)
    (fill (recursive-cte-seen-streams seen) nil)
    (fill (recursive-cte-seen-paths seen) nil)))

(defun make-recursive-cte-frontier ()
  (let ((store (make-query-replay-store (make-sql-iterator (lambda () (values nil nil))))))
    (setf (query-replay-store-eof-p store) t)
    store))

(defun make-cte-worktable-iterator (relation)
  (let ((binding (assoc (getf relation :cte) *recursive-cte-worktables* :test #'eq)))
    (unless binding (error "Recursive worktable has no active generation"))
    (make-query-replay-reader (cdr binding))))

(defun make-recursive-union-iterator (node relations)
  ;; Self scans read the sealed working generation while new rows fill the
  ;; separate intermediate store. Close the child before swapping or deleting
  ;; its worktable and generation cache. Rows are yielded lazily.
  (let* ((definition (second node))
         (targets (getf (sql-cte-prepared definition) :targets))
         (all-p (getf (sql-cte-prepared definition) :recursive-union-all))
         (seen (unless all-p (make-recursive-cte-seen)))
         (working (make-recursive-cte-frontier))
         (intermediate nil) (input nil) (recursing nil) (done nil)
         (outer-worktables *recursive-cte-worktables*)
         (outer-generation-caches *recursive-cte-generation-caches*)
         (generation-cache nil))
    (labels ((worktables ()
               (if recursing (acons definition working outer-worktables) outer-worktables))
             (generation-caches ()
               (if recursing (acons definition generation-cache outer-generation-caches) outer-generation-caches))
             (open-input ()
               (let ((*recursive-cte-worktables* (worktables))
                     (*recursive-cte-generation-caches* (generation-caches)))
                 (setf input
                       (make-append-sql-iterator
                        (list (list (nth (if recursing 1 0) (fifth node))
                                    (if recursing (fourth node) (third node))))
                        relations))))
             (close-input ()
               (close-sql-iterator input)
               (setf input nil))
             (next-generation ()
               (close-input)
               (close-recursive-cte-generation generation-cache)
               (setf generation-cache nil)
               (if (not recursing)
                   (progn
                     (setf recursing t)
                     (query-row-spool-seal (query-replay-store-spool working))
                     (when (zerop (query-row-spool-count (query-replay-store-spool working)))
                       (setf done t)))
                   (if (zerop (query-row-spool-count (query-replay-store-spool intermediate)))
                       (setf done t)
                       (progn
                         (close-query-replay-store working)
                         (setf working intermediate intermediate nil)
                         (query-row-spool-seal (query-replay-store-spool working)))))
               (unless done (setf intermediate (make-recursive-cte-frontier)
                                  generation-cache (make-hash-table :test #'eq)))))
      (make-sql-iterator
       (lambda ()
         (loop
           (when done (return (values nil nil)))
           (unless input (open-input))
           (multiple-value-bind (row present origins)
               (let ((*recursive-cte-worktables* (worktables))
                     (*recursive-cte-generation-caches* (generation-caches)))
                 (sql-iterator-next input))
             (if present
                 (let ((values (car row)))
                   (when (or all-p (recursive-cte-seen-new-p seen (recursive-cte-seen-key values targets)))
                     (append-query-replay-row (if recursing intermediate working) values origins)
                     (return (values row t origins))))
                 (next-generation)))))
       (lambda ()
         (unwind-protect (unwind-protect (close-input) (close-recursive-cte-generation generation-cache))
           (unwind-protect (when working (close-query-replay-store working))
             (unwind-protect (when intermediate (close-query-replay-store intermediate))
               (close-recursive-cte-seen seen))))
         (setf working nil intermediate nil seen nil outer-worktables nil))))))

(defun query-cte-relations (prepared)
  (mapcan (lambda (definition)
            (when (sql-cte-plan definition)
              (copy-list (getf (query-plan-prepared (sql-cte-plan definition)) :relations))))
          (getf prepared :ctes)))

(defun close-cte-execution-state (state)
  (sb-sys:without-interrupts
    (unless (cte-execution-state-closed-p state)
      (setf (cte-execution-state-closed-p state) t)
      (unwind-protect
           (sb-sys:with-local-interrupts
             (when (cte-execution-state-store state)
               (close-query-replay-store (cte-execution-state-store state))))
        (let ((cache (cte-execution-state-cache state))
              (definition (cte-execution-state-definition state)))
          (when (and cache (eq state (gethash definition cache))) (remhash definition cache)))
        (when *cte-open-states* (remhash state *cte-open-states*))
        (setf (cte-execution-state-store state) nil
              (cte-execution-state-cache state) nil
              (cte-execution-state-definition state) nil))))
  (values))

(defun close-cte-frame-cache (cache)
  (when cache
    (let ((namespaces (gethash +recursive-cte-cache-key+ cache)))
      (when namespaces
        (unwind-protect
             (maphash (lambda (generation child)
                        (unwind-protect (close-cte-frame-cache child)
                          (remhash cache generation)))
                      namespaces)
          (remhash +recursive-cte-cache-key+ cache))))
    (let ((states (loop for value being the hash-values of cache
                        when (cte-execution-state-p value) collect value)))
      (dolist (state states)
        (handler-case (close-cte-execution-state state)
          (error () nil) (sb-ext:timeout () nil)))))
  (values))

(defun extend-cte-execution-context (definitions)
  (append (mapcar (lambda (definition) (cons definition *subquery-outer-frames*)) definitions)
          *cte-execution-context*))

(defun call-with-cte-definitions (definitions function)
  (let ((*cte-execution-context* (extend-cte-execution-context definitions)))
    (funcall function)))

(defun make-with-ctes-iterator (definitions node relations)
  (let* ((context (extend-cte-execution-context definitions))
         (input (let ((*cte-execution-context* context))
                  (make-physical-plan-iterator node relations))))
    (make-sql-iterator
     (lambda ()
       (let ((*cte-execution-context* context)) (sql-iterator-next input)))
     (lambda () (close-sql-iterator input) (setf context nil)))))

(defun cte-state-cache (definition frames)
  (when (sql-cte-shared-p definition)
    (let* ((dependencies (sql-cte-dependencies definition))
           (owner (if (null dependencies) *subquery-result-cache*
                      (or (cdr (nth (- (first dependencies) 2) frames))
                          (error "CTE producer has no live dependency owner"))))
           (generation (prepared-query-worktable-cache (sql-cte-prepared definition))))
      (if (and owner generation) (recursive-cte-parameter-cache owner generation) owner))))

(defun plan-cte-reference-filters (prepared)
  ;; Per-reference plans push only safe filters across supported simple shapes;
  ;; they do not flatten the CTE into a joint optimizer search.
  (let ((safe-joins (every (lambda (join) (member (getf join :type) '(:inner :cross)))
                           (getf prepared :joins))))
    (dolist (relation (getf prepared :relations))
      (let* ((definition (getf relation :cte))
             (body (and definition (sql-cte-prepared definition))))
        (when (and definition (not (getf relation :cte-worktable))
                   (not (sql-cte-shared-p definition)))
          (when (and safe-joins
                     (not (getf body :set-operation)) (not (getf body :aggregate-p))
                     (not (getf body :windows)) (not (getf body :window-keys))
                     (not (getf body :set-functions))
                     (not (getf body :distinct)) (null (getf body :limit)))
            (let ((quals (remove-if-not (lambda (form) (cte-reference-filter-p form relation))
                                        (where-conjuncts (getf prepared :where)))))
              (when quals
                (setf (getf relation :cte-filter)
                      (substitute-cte-output-columns
                       (reduce (lambda (left right) (list :and left right :type :bool :typmod -1))
                               (rest quals) :initial-value (first quals))
                       relation)))))
          (let* ((tree (copy-tree (cte-reference-producer-tree
                                   (query-plan-physical (sql-cte-plan definition))
                                   (getf relation :cte-filter))))
                 (plan (%make-query-plan :physical tree :logical tree :prepared body)))
            (dolist (node (collect-query-plan-nodes tree))
              (setf (gethash node (query-plan-metadata plan)) (make-plan-node-metadata)))
            (setf (getf relation :cte-reference-plan) plan))))))
  prepared)

(defun make-cte-producer-state (definition frames context cache &optional reference-plan)
  (let* ((capture-p *datum-origin-capture*)
         (own-frame (cons #() (make-hash-table :test #'eq)))
         (producer-frames (cons own-frame frames))
         (worktables *recursive-cte-worktables*)
         (generation-caches *recursive-cte-generation-caches*)
         (plan (or reference-plan (sql-cte-plan definition)))
         (relations (subquery-registered-relations plan))
         (input
           (let ((*datum-origin-capture* capture-p)
                 (*subquery-outer-frames* producer-frames)
                 (*cte-execution-context* context)
                 (*recursive-cte-worktables* worktables)
                 (*recursive-cte-generation-caches* generation-caches)
                 (*cte-active-plan* plan))
             (make-physical-plan-iterator (query-plan-physical plan) relations)))
         (producer
           (make-sql-iterator
            (lambda ()
              (let ((*datum-origin-capture* capture-p)
                    (*subquery-outer-frames* producer-frames)
                    (*cte-execution-context* context)
                    (*recursive-cte-worktables* worktables)
                    (*recursive-cte-generation-caches* generation-caches)
                    (*cte-active-plan* plan))
                (multiple-value-bind (row present origins) (sql-iterator-next input)
                  (values (when present (car row)) present origins))))
            (lambda ()
              (unwind-protect
                   (close-sql-iterator input)
                (unwind-protect
                     (close-cte-frame-cache (cdr own-frame))
                  (setf input nil producer-frames nil own-frame nil context nil relations nil plan nil))))))
         (state (make-cte-execution-state
                 :definition definition :cache cache :store (make-query-replay-store producer))))
    (when cache (setf (gethash definition cache) state))
    (when *cte-open-states* (setf (gethash state *cte-open-states*) t))
    state))

(defun make-cte-scan-iterator (relation)
  ;; Shared readers have independent cursors over a lazy appendable replay store.
  (let* ((definition (getf relation :cte))
         (binding (assoc definition *cte-execution-context* :test #'eq)))
    (unless binding (error "CTE reader has no lexical declaration owner"))
    (let* ((frames (cdr binding))
           (cache (cte-state-cache definition frames))
           (state (or (and cache (gethash definition cache))
                      (make-cte-producer-state definition frames *cte-execution-context* cache (getf relation :cte-reference-plan))))
           (reader (if (sql-cte-shared-p definition)
                       (make-query-replay-reader (cte-execution-state-store state))
                       (query-replay-store-producer (cte-execution-state-store state)))))
      (make-sql-iterator
       (lambda () (sql-iterator-next reader))
       (lambda ()
         (close-sql-iterator reader)
         (unless cache (close-cte-execution-state state))
         (setf reader nil state nil))))))

(defstruct (query-execution-owner (:constructor %make-query-execution-owner))
  ;; The creating thread owns the retained bindings, snapshot and resources.
  ;; The global census keeps the owner visible until cleanup really completes.
  backend thread bindings snapshot-registration temp-owner cleanup-functions
  (state :open))

(defvar *query-execution-owner* nil)

(defvar *query-execution-owners* (make-hash-table :test #'eq))

(defvar *query-execution-owner-lock*
  (sb-thread:make-mutex :name "pg persistent query owners"))

(defvar *query-execution-owner-cv*
  (sb-thread:make-waitqueue :name "pg persistent query owner change"))

(defvar *query-execution-owner-generation* 0)

(defvar *query-execution-owner-reload-wait-hook* nil)

(defparameter *query-execution-owner-context-symbols*
  '(*query-parameter-types* *query-parameter-values*
    *subquery-execution-active-p* *subquery-result-cache* *subquery-registered-relations*
    *subquery-outer-frames* *subquery-output-value* *subquery-backend-entry*
    *subquery-instrumentation-depth* *cte-execution-context* *cte-open-states*
    *cte-active-plan* *sql-open-iterators* *recursive-cte-worktables*
    *recursive-cte-generation-caches* *lateral-outer-environment*
    *datum-origin-capture* *query-row-origins* *select-toast-demand*
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

(defun wait-for-query-execution-owner-change (generation)
  "Wait after reload admission has reopened, allowing clients to retire owners."
  (when *query-execution-owner-reload-wait-hook*
    (funcall *query-execution-owner-reload-wait-hook* generation))
  (sb-thread:with-mutex (*query-execution-owner-lock*)
    (loop while (and (= generation *query-execution-owner-generation*)
                     (plusp (hash-table-count *query-execution-owners*)))
          do (sb-thread:condition-wait *query-execution-owner-cv* *query-execution-owner-lock*)))
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
    (dolist (symbol '(*cte-execution-context* *recursive-cte-worktables*
                      *recursive-cte-generation-caches* *subquery-outer-frames*
                      *subquery-output-value* *subquery-registered-relations*
                      *cte-active-plan* *lateral-outer-environment* *query-row-origins*
                      *toast-slice-current* *select-index-scans*
                      *select-index-lease-domain* *select-index-lease-groups*))
      (setf (cdr (assoc symbol context)) nil))
    (dolist (symbol '(*subquery-result-cache* *cte-open-states* *sql-open-iterators*))
      (setf (cdr (assoc symbol context)) (make-hash-table :test #'eq)))
    (setf (cdr (assoc '*subquery-execution-active-p* context)) t
          (cdr (assoc '*subquery-instrumentation-depth* context)) 0
          (cdr (assoc '*subquery-backend-entry* context)) (query-plan-backend-entry :interpreter))
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*query-execution-owner-lock*)
        (setf (gethash owner *query-execution-owners*) t)
        (incf *query-execution-owner-generation*)
        (sb-thread:condition-broadcast *query-execution-owner-cv*))
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
  "Copy an already protected snapshot before the enclosing registration releases."
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
                   (sb-thread:with-mutex (*query-execution-owner-lock*)
                     (remhash owner *query-execution-owners*)
                     (incf *query-execution-owner-generation*)
                     (sb-thread:condition-broadcast *query-execution-owner-cv*))))
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

(defun call-with-query-cte-execution (root-p function)
  (if (not root-p) (funcall function)
      (let ((*cte-execution-context* nil)
            (*cte-open-states* (make-hash-table :test #'eq))
            (*sql-open-iterators* (make-hash-table :test #'eq)))
        (with-query-temp-files ()
                               (unwind-protect (funcall function)
                                 (dolist (iterator (loop for iterator being the hash-keys of *sql-open-iterators*
                                                         collect iterator))
                                   (handler-case (close-sql-iterator iterator)
                                     (error () nil) (sb-ext:timeout () nil)))
                                 (dolist (state (loop for state being the hash-keys of *cte-open-states* collect state))
                                   (handler-case (close-cte-execution-state state)
                                     (error () nil) (sb-ext:timeout () nil))))))))

(defun query-plan-cte-definitions (plan)
  (let ((result nil))
    (dolist (node (collect-query-plan-nodes (query-plan-physical plan)) (nreverse result))
      (when (eq (first node) :with-ctes)
        (dolist (definition (second node))
          (when (sql-cte-plan definition) (pushnew definition result :test #'eq)))))))

(defun query-plan-cte-reference-plans (plan)
  (remove-duplicates
   (loop for relation in (getf (query-plan-prepared plan) :relations)
         when (getf relation :cte-reference-plan) collect (getf relation :cte-reference-plan))
   :test #'eq))
