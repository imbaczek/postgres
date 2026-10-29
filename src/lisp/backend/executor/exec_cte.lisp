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
           (multiple-value-bind (row present)
               (let ((*recursive-cte-worktables* (worktables))
                     (*recursive-cte-generation-caches* (generation-caches)))
                 (sql-iterator-next input))
             (if present
                 (let ((values (car row)))
                   (when (or all-p (recursive-cte-seen-new-p seen (recursive-cte-seen-key values targets)))
                     (append-query-replay-row (if recursing intermediate working) values)
                     (return (values row t))))
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
