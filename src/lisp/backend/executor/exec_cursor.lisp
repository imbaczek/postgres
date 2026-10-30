;;;; exec_cursor.lisp -- resumable physical plans for demand-driven producers.
(in-package #:pg)

(defun make-mapping-sql-iterator (input function &key preserve-toast-context)
  ;; The fourth iterator value is a TOAST slice context for the current row.
  ;; Bind it only while evaluating that row; pass it on only when requested.
  (make-sql-iterator
   (lambda ()
     (loop
       (multiple-value-bind (row present origins toast-context) (sql-iterator-next input)
         (unless present (return (values nil nil nil nil)))
         ;; A scan's saved pointer belongs only to this input row. Nested
         ;; subqueries must compute their own demand and slice plans.
         (let ((*query-row-origins* (and *datum-origin-capture* origins))
               (*toast-slice-current* toast-context)
               (*select-toast-demand* nil)
               (*select-toast-slice-plan* nil))
           (multiple-value-bind (result keep result-origins) (funcall function row)
             (when keep
               (return (values result t result-origins
                               (and preserve-toast-context toast-context)))))))))
   (lambda () (close-sql-iterator input))))

(defun make-buffered-sql-iterator (producer &optional input)
  (let ((store nil) (reader nil) (capture-p *datum-origin-capture*))
    (make-sql-iterator
     (lambda ()
       (let ((*datum-origin-capture* capture-p))
         (unless store
           (setf store (make-query-replay-store nil))
           (funcall producer (lambda (row) (append-query-replay-row store row)))
           (query-row-spool-seal (query-replay-store-spool store))
           (setf (query-replay-store-eof-p store) t
                 reader (make-query-replay-reader store)))
         (sql-iterator-next reader)))
     (lambda ()
       (when reader (close-sql-iterator reader))
       (when store (close-query-replay-store store))
       (when input (close-sql-iterator input))))))

(defvar *lateral-outer-environment* nil)

(defun sort-project-set-info (node child)
  ;; Only this shape can sort source rows before expanding the set results.
  ;; Origin capture keeps the ordinary path, which preserves datum origins.
  (when (and (not *datum-origin-capture*) (member (first node) '(:sort :top-n))
             (eq (first child) :project) (eq (first (third child)) :project-set))
    (let* ((set-node (third child)) (width (fourth set-node))
                                    (calls (second set-node)) (targets (second child)))
      (when (and (integerp width) (not (minusp width)) calls)
        (values set-node width calls targets)))))

(defun make-visible-heap-page-iterator (page block columns snapshot)
  "Visit visible heap tuples from an owned PAGE one at a time.

Check tuple visibility before deforming it, and leave later tuples untouched
until their next pull.  SNAPSHOT must remain registered for this iterator."
  (let ((offnum 0) (count (page-lp-count page)))
    (make-sql-iterator
     (lambda ()
       (loop while (< offnum count)
             do (check-for-interrupts)
                (incf offnum)
                (with-live-page-item (page offnum offset length)
                  (let ((xmin (full-xid-from-32
                               (page-u32 page (+ offset +heap-tuple-xmin-off+))))
                        (xmax (full-xid-from-32
                               (page-u32 page (+ offset +heap-tuple-xmax-off+))))
                        (cid (page-u32 page (+ offset +heap-tuple-cid-off+)))
                        (infomask (page-u16 page (+ offset +heap-tuple-infomask-off+))))
                    (when (heap-tuple-visible-p xmin xmax cid snapshot
                                                (and *backend* (backend-xid *backend*)) infomask)
                      (multiple-value-bind (values raw-xmin raw-xmax raw-cid raw-infomask
                                            ctid-block ctid-offnum physical-compressed)
                          (deform-heap-tuple (subseq page offset (+ offset length)) columns)
                        (declare (ignore raw-xmin raw-xmax raw-cid raw-infomask))
                        (return
                          (values (make-htup :block block :offnum offnum
                                             :ctid-block ctid-block :ctid-offnum ctid-offnum
                                             :xmin xmin :xmax xmax :cid cid :infomask infomask
                                             :values values :physical-compressed physical-compressed)
                                  t))))))))
     (lambda () (setf page nil)))))

(defun make-select-index-tid-iterator (table path environment)
  ;; Buffer matching TIDs under the index lease, then release the lease before
  ;; fetching heap tuples. The buffer may spill and is owned by the iterator.
  (let ((*datum-origin-capture* nil))
    (make-buffered-sql-iterator
     (lambda (emit)
       (let* ((index (select-scan-path-index path))
              (key (select-scan-index-key table path environment)))
         (when key
           (call-with-current-select-index-domain
            (pg-index-relfilenode index)
            (lambda ()
              (call-with-btree-equal-cursor
               (pg-index-relfilenode index) key
               (lambda (cursor lease)
                 (call-with-select-index-cursor
                  cursor lease
                  (lambda (tid)
                    (check-for-interrupts)
                    (funcall emit (vector (car tid) (cdr tid)))))))))))
       (detach-select-index-scans)))))

(defun make-select-index-scan-iterator (table path snapshot relation relations environment capture-p demand)
  ;; Fetch and check one buffered TID at a time against the scan snapshot.
  ;; Closing this iterator also closes the TID buffer.
  (let ((tids (make-select-index-tid-iterator table path environment)))
    (make-sql-iterator
     (lambda ()
       (loop
         (multiple-value-bind (tid present) (sql-iterator-next tids)
           (unless present (return (values nil nil nil)))
           (let* ((*datum-origin-capture* capture-p)
                  (tuple (heapam-get-tuple (pg-class-relfilenode table)
                                           (aref tid 0) (aref tid 1)
                                           (pg-class-columns table))))
             (when (and tuple (htup-visible-p tuple snapshot))
               (let ((row (htup-values (materialize-select-tuple table tuple snapshot :demand demand)))
                     (origins (and capture-p (htup-origins tuple))))
                 (return
                   (values (if (getf relation :join-order-base)
                               (make-select-relation-environment relations relation row)
                               row)
                           t
                           (if (getf relation :join-order-base)
                               (make-select-relation-origins relations relation origins)
                               origins)))))))))
     (lambda () (close-sql-iterator tids)))))

(defun make-select-scan-iterator (node relations)
  ;; Capture the parameter environment and statement snapshot for this scan's
  ;; lifetime; later outer rows must not change its index key or visibility.
  (let* ((relation (physical-scan-relation node relations))
         (table (getf relation :table))
         (path (physical-scan-path node relation))
         (environment (copy-query-sort-value *lateral-outer-environment*))
         (source (getf relation :row-source))
         (block 0) (nblocks nil) (rows nil) (page-reader nil) (initialized nil)
         (snapshot *current-snapshot*)
         (capture-p *datum-origin-capture*)
         (demand *select-toast-demand*)
         (slice-plan *select-toast-slice-plan*))
    (when (eq (select-scan-path-kind path) :index-scan)
      (return-from make-select-scan-iterator
        (make-select-index-scan-iterator table path snapshot relation relations
                                         environment capture-p demand)))
    (unless (eq (select-scan-path-kind path) :seq-scan)
      (error "A resumable producer requires a SeqScan plan"))
    (make-sql-iterator
     (lambda ()
       (unless initialized
         (setf initialized t)
         (if source (setf rows (funcall source))
             (setf nblocks (buf-nblocks (pg-class-relfilenode table)))))
       (loop
         (check-for-interrupts)
         (when rows
           (let ((row (pop rows)))
             (return
               (values (if (getf relation :join-order-base)
                           (make-select-relation-environment relations relation row)
                           row)
                       t nil))))
         (when page-reader
           (multiple-value-bind (tuple present) (sql-iterator-next page-reader)
             (when present
               (let* ((*datum-origin-capture* capture-p)
                      (values (htup-values tuple))
                      (toast-context
                        (when (and slice-plan (eq table (car slice-plan)))
                          (loop for (expression . slot) in (cdr slice-plan)
                                for value = (aref values (1- slot))
                                when (heap-external-varlena-p value)
                                  collect (cons expression
                                                (list (pg-class-toast table) value snapshot values)))))
                      (row (htup-values (materialize-select-tuple table tuple snapshot :demand demand)))
                      (origins (and capture-p (htup-origins tuple))))
                 (return
                   (values (if (getf relation :join-order-base)
                               (make-select-relation-environment relations relation row)
                               row)
                           t
                           (if (getf relation :join-order-base)
                               (make-select-relation-origins relations relation origins)
                               origins)
                           toast-context))))
             (close-sql-iterator page-reader)
             (setf page-reader nil)))
         (when (or source (>= block nblocks)) (return (values nil nil)))
         (let* ((current block)
                (page (buf-read (pg-class-relfilenode table) current)))
           (incf block)
           (when page
             (setf page-reader (make-visible-heap-page-iterator
                                page current (pg-class-columns table) snapshot))))))
     (lambda ()
       (unwind-protect (close-sql-iterator page-reader)
         (setf rows nil page-reader nil))))))

(defun make-append-sql-iterator (arms relations)
  (let ((remaining arms) (input nil) (coercions nil))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless input
           (unless remaining (return (values nil nil)))
           (let ((arm (pop remaining)))
             (setf coercions (first arm)
                   input (make-physical-plan-iterator (second arm) relations))))
         (multiple-value-bind (row present origins) (sql-iterator-next input)
           (if present
               (let ((values (make-array (length coercions)))
                     (result-origins (and *datum-origin-capture*
                                          (make-array (length coercions) :initial-element nil))))
                 (loop for expression in coercions for i from 0
                       do (multiple-value-bind (value origin)
                              (if *datum-origin-capture*
                                  (eval-expr-with-origin expression (car row) origins)
                                  (values (eval-expr expression (car row)) nil))
                            (setf (aref values i) value)
                            (when result-origins (setf (aref result-origins i) origin))))
                 (return (values (cons values values) t result-origins)))
               (progn (close-sql-iterator input) (setf input nil))))))
     (lambda () (close-sql-iterator input)))))

(defun make-project-set-iterator (input calls &key attempt-function)
  ;; Keep each input row until all its sets are exhausted. Each pull zips the
  ;; sets, padding exhausted ones with NULL and suppressing an all-empty row.
  ;; ATTEMPT-FUNCTION may step sets in target order before the remaining steps.
  (let ((environment nil) (input-origins nil) (sets nil) (ended nil))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless sets
           (multiple-value-bind (row present origins) (sql-iterator-next input)
             (unless present (return (values nil nil)))
             (setf environment row input-origins origins
                   sets (make-array (length calls) :initial-element nil)
                   ended (make-array (length calls) :element-type 'bit :initial-element 0))))
         (let ((values (make-array (length calls) :initial-element +null+))
               (visited (make-array (length calls) :element-type 'bit :initial-element 0))
               (any nil)
               (*query-row-origins* input-origins))
           (flet ((step-set (index)
                    (check-for-interrupts)
                    (when (and (zerop (aref visited index)) (zerop (aref ended index)))
                      (setf (aref visited index) 1)
                      ;; Argument expressions run only when this target entry is
                      ;; first reached, preserving scalar/SRF target-list order.
                      (unless (aref sets index)
                        (setf (aref sets index)
                              (make-sql-set-function-iterator (nth index calls) environment)))
                      (multiple-value-bind (value present) (funcall (aref sets index))
                        (if present
                            (setf any t (aref values index) value)
                            (setf (aref ended index) 1))))))
             (when attempt-function (funcall attempt-function environment values #'step-set))
             (dotimes (index (length calls)) (step-set index)))
           (if any
               (return (values (concatenate 'vector environment values) t
                               (project-set-row-origins input-origins (length environment) (length values))))
               (setf sets nil ended nil)))))
     (lambda ()
       (setf sets nil ended nil environment nil input-origins nil)
       (close-sql-iterator input)))))

(defun make-project-set-projection-iterator (input calls targets width)
  "Project targets in order on each attempt to expand an input row.

Step each bare set target when reached, creating its iterator and evaluating
its arguments lazily.  Scalar targets also run on the final attempt, even when
all sets are empty and no row is emitted.  Closing the result closes INPUT."
  (let ((attempt-values nil) (attempt-origins nil)
                             (capture-p *datum-origin-capture*))
    (let ((expanded
            (make-project-set-iterator
             input calls
             :attempt-function
             (lambda (environment set-values step-set)
               (setf attempt-values (make-array (length targets))
                     attempt-origins
                     (and capture-p (make-array (length targets) :initial-element nil)))
               (loop for target in targets for index from 0
                     for expression = (getf target :expr)
                     do (if (and (eq (first expression) :col)
                                 (< width (second expression))
                                 (<= (second expression) (+ width (length calls))))
                            (let ((slot (- (second expression) width 1)))
                              (funcall step-set slot)
                              (setf (aref attempt-values index) (aref set-values slot)))
                            (multiple-value-bind (value origin)
                                (if capture-p
                                    (eval-expr-with-origin expression environment *query-row-origins*)
                                    (values (eval-expr expression environment) nil))
                              (setf (aref attempt-values index) value)
                              (when capture-p
                                (setf (aref attempt-origins index) origin)))))))))
      (make-mapping-sql-iterator
       expanded
       (lambda (environment)
         (values (cons attempt-values environment) t attempt-origins))))))

(defun make-sorted-project-set-iterator (node project set-node width calls relations &key source-streamer)
  "Sort source rows before expansion when no sort key depends on a set result.

Cache ordinary targets and sort keys per source row.  Evaluate deferred targets
as expanded rows are pulled; bare set targets use target-list attempt order,
while expressions wrapping set results remain a separate projection.
SOURCE-STREAMER, when supplied, streams the source through the caller's
instrumented push execution; otherwise the sort owns and closes a pull iterator.
Closing the returned iterator releases the buffered sort and its source."
  (let* ((targets (second project))
         (target-count (length targets))
         (deferred
           (map 'vector
                (lambda (target)
                  (or (project-set-expression-dependent-p (getf target :expr) width (length calls) targets)
                      (sort-projection-volatile-p (getf target :expr))))
                targets))
         (bare-p (project-set-projection-targets-p targets width (length calls)))
         (extra nil)
         (order
           (loop for entry in (second node)
                 for expression = (car entry)
                 collect
                 (if (eq (first expression) :sort-output)
                     (progn
                       (setf (aref deferred (1- (second expression))) nil)
                       entry)
                     (progn
                       (push expression extra)
                       (cons (list :sort-output (+ target-count (length extra))
                                   :type (expr-type expression) :typmod -1)
                             (cdr entry))))))
         (cache nil)
         (attempt-values nil)
         (transform
           (lambda (environment)
             (let* ((row (make-sort-project-input-row environment targets deferred))
                    (keys (map 'vector (lambda (expression) (eval-expr expression environment))
                               (reverse extra))))
               (setf (car row) (concatenate 'vector (car row) keys))
               (values row t nil))))
         (source
           (unless source-streamer
             (make-mapping-sql-iterator
              (make-physical-plan-iterator (third set-node) relations) transform)))
         (sorted
           (make-buffered-sql-iterator
            (lambda (emit)
              (stream-select-sort
               (lambda (callback)
                 (if source-streamer
                     (funcall source-streamer
                              (lambda (environment) (funcall callback (funcall transform environment))))
                     (map-sql-iterator source callback)))
               order emit))
            source))
         (input
           (make-mapping-sql-iterator
            sorted
            (lambda (row)
              (unless (= (length (cdr row)) width)
                (error "ProjectSet input width differs from its plan"))
              (setf cache (subseq (car row) 0 target-count))
              (values (cdr row) t nil))))
         (expanded
           (instrument-physical-plan-iterator set-node
                                              (make-project-set-iterator
                                               input calls
                                               :attempt-function
                                               (when bare-p
                                                 (lambda (environment set-values step-set)
                                                   (setf attempt-values (copy-seq cache))
                                                   (loop for target in targets for index from 0
                                                         for expression = (getf target :expr)
                                                         do (cond
                                                              ((and (eq (first expression) :col)
                                                                    (< width (second expression))
                                                                    (<= (second expression) (+ width (length calls))))
                                                               (let ((slot (- (second expression) width 1)))
                                                                 (funcall step-set slot)
                                                                 (setf (aref attempt-values index) (aref set-values slot))))
                                                              ((aref deferred index)
                                                               (setf (aref attempt-values index) (eval-expr expression environment))))))))))
         (output
           (instrument-physical-plan-iterator project
                                              (make-mapping-sql-iterator
                                               expanded
                                               (lambda (environment)
                                                 (values (if bare-p
                                                             (cons attempt-values environment)
                                                             (finish-sort-project-row (cons cache environment) targets deferred))
                                                         t nil))))))
    (if (eq (first node) :top-n)
        (let ((remaining (third node)))
          (make-sql-iterator
           (lambda ()
             (when (plusp remaining)
               (decf remaining)
               (sql-iterator-next output)))
           (lambda () (close-sql-iterator output))))
        output)))

(defun make-lateral-join-iterator (join left right-factory)
  ;; Each left row owns a fresh right iterator and parameter epoch.
  (let ((right nil) (current-left nil) (left-origins nil) (left-present nil) (matched nil)
                    (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless left-present
           (multiple-value-setq (current-left left-present left-origins) (sql-iterator-next left))
           (unless left-present (return (values nil nil)))
           (setf matched nil right (funcall right-factory current-left left-origins)))
         (multiple-value-bind (row present right-origins) (sql-iterator-next right)
           (if present
               (let* ((joined (combine-select-join-environment current-left row join))
                      (origins (and *datum-origin-capture*
                                    (combine-select-join-origins current-left left-origins row right-origins join)))
                      (*query-row-origins* origins))
                 (when (true-p (eval-expr (getf join :qual) joined))
                   (setf matched t)
                   (return (values joined t origins))))
               (progn
                 (close-sql-iterator right)
                 (setf right nil left-present nil)
                 (when (and (not matched) (eq (getf join :type) :left))
                   (return (values (concatenate 'vector current-left null-right) t
                                   (and *datum-origin-capture*
                                        (concatenate 'vector
                                                     (or left-origins (make-array (length current-left) :initial-element nil))
                                                     (make-array (length null-right) :initial-element nil)))))))))))
     (lambda ()
       (close-sql-iterator right)
       (close-sql-iterator left)
       (setf current-left nil left-origins nil)))))

;; The join owns LEFT and a replay store owning RIGHT.  A fresh reader scans
;; RIGHT for each left row; right/full joins make one final pass for unmatched
;; rows.  Closing the join closes the active reader, both inputs and match map.
(defun make-nested-loop-iterator (join left right)
  (let* ((store (make-query-replay-store right))
         (reader nil) (current-left nil) (left-origins nil) (left-present nil)
         (left-done nil) (left-seen nil) (matched nil) (right-phase nil)
         (right-index 0)
         (outer-p (member (getf join :type) '(:right :full)))
         (matches (when outer-p (make-query-match-map)))
         (null-left (make-array (getf join :left-width) :initial-element +null+))
         (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless (or left-present left-done)
           (multiple-value-setq (current-left left-present left-origins) (sql-iterator-next left))
           (unless left-present (setf left-done t))
           (when left-present
             (setf left-seen t matched nil right-index 0 reader (make-query-replay-reader store))))
         (when (and left-done (not outer-p)) (return (values nil nil)))
         (when (and left-done (not right-phase))
           (setf right-phase t right-index 0 reader (make-query-replay-reader store))
           (when left-seen (query-match-map-finish matches)))
         (multiple-value-bind (row present right-origins) (sql-iterator-next reader)
           (when (and matches present)
             (loop while (<= (query-match-map-count matches) right-index)
                   do (query-match-map-append matches)))
           (cond
             (right-phase
              (unless present (return (values nil nil)))
              (let ((index right-index))
                (incf right-index)
                (unless (and left-seen (query-match-map-matched-p matches index))
                  (return (values (concatenate 'vector null-left row) t
                                  (and *datum-origin-capture*
                                       (concatenate 'vector
                                                    (make-array (length null-left) :initial-element nil)
                                                    (or right-origins (make-array (length row) :initial-element nil)))))))))
             (present
              (let* ((index right-index)
                     (joined (combine-select-join-environment current-left row join))
                     (origins (and *datum-origin-capture*
                                   (combine-select-join-origins current-left left-origins row right-origins join)))
                     (*query-row-origins* origins))
                (incf right-index)
                (when (true-p (eval-expr (getf join :qual) joined))
                  (setf matched t)
                  (when matches (query-match-map-mark matches index))
                  (return (values joined t origins)))))
             (t
              (close-sql-iterator reader)
              (setf reader nil left-present nil)
              (when (and (not matched) (member (getf join :type) '(:left :full)))
                (return (values (concatenate 'vector current-left null-right) t
                                (and *datum-origin-capture*
                                     (concatenate 'vector
                                                  (or left-origins (make-array (length current-left) :initial-element nil))
                                                  (make-array (length null-right) :initial-element nil)))))))))))
     (lambda ()
       (close-sql-iterator reader)
       (close-sql-iterator left)
       (close-query-replay-store store)
       (when matches
         (close-query-match-map matches)
         (when (query-match-map-pathname matches)
           (delete-query-temp-file (query-match-map-pathname matches))))
       (setf current-left nil left-origins nil)))))

(defun instrument-physical-plan-iterator (node input)
  "Count and time pulls from INPUT for NODE; closing the wrapper closes INPUT."
  (let* ((metadata (and *cte-active-plan* (gethash node (query-plan-metadata *cte-active-plan*))))
         (started-p nil))
    (if (null metadata) input
        (make-sql-iterator
         (lambda ()
           (let ((started (when *query-plan-instrumentation-p* (get-internal-real-time)))
                 (first-p (not started-p)))
             (when started
               (unless started-p
                 (incf (plan-node-metadata-actual-loops metadata))
                 (setf started-p t)))
             (unwind-protect
                  (multiple-value-bind (row present origins toast-context) (sql-iterator-next input)
                    (when (and started present)
                      (incf (plan-node-metadata-actual-row-count metadata))
                      (setf (plan-node-metadata-actual-rows metadata)
                            (/ (plan-node-metadata-actual-row-count metadata)
                               (plan-node-metadata-actual-loops metadata))))
                    (values row present origins toast-context))
               (when started
                 (let ((elapsed (- (get-internal-real-time) started)))
                   (incf (plan-node-metadata-actual-total-time-ticks metadata) elapsed)
                   (when first-p
                     (incf (plan-node-metadata-actual-startup-time-ticks metadata) elapsed)))))))
         (lambda () (close-sql-iterator input))))))

(defun make-physical-plan-iterator (node relations)
  (instrument-physical-plan-iterator node (%make-physical-plan-iterator node relations)))

(defun make-resumable-query-plan-iterator (plan relations)
  ;; Restore the plan's TOAST demand and instrumentation context for each pull;
  ;; the caller owns the returned iterator and closes it when the plan ends.
  (let* ((physical (query-plan-physical plan))
         (slice-plan (simple-select-toast-slice physical relations))
         (demand (or (simple-select-toast-demand physical relations)
                     (aggregate-toast-demand physical relations))))
    (when (and slice-plan demand)
      (setf demand (cons (car demand)
                         (set-difference (cdr demand)
                                         (mapcar #'cdr (cdr slice-plan))))))
    (let* ((*select-toast-demand* demand)
           (*select-toast-slice-plan* slice-plan)
           (*toast-slice-current* nil)
           (*cte-active-plan* plan)
           (input (make-physical-plan-iterator physical relations)))
      (make-sql-iterator
       (lambda ()
         (let ((*select-toast-demand* demand)
               (*select-toast-slice-plan* slice-plan)
               (*toast-slice-current* nil)
               (*cte-active-plan* plan))
           (multiple-value-bind (row present origins) (sql-iterator-next input)
             (values row present origins))))
       (lambda () (close-sql-iterator input))))))

(defun %make-physical-plan-iterator (node relations)
  (case (first node)
    (:result
     (let ((done nil))
       (make-sql-iterator (lambda () (unless done (setf done t) (values #() t))))))
    ((:seq-scan :index-scan) (make-select-scan-iterator node relations))
    (:cte-scan (make-cte-scan-iterator (physical-scan-relation node relations)))
    (:worktable-scan (make-cte-worktable-iterator (physical-scan-relation node relations)))
    (:recursive-union (make-recursive-union-iterator node relations))
    (:with-ctes (make-with-ctes-iterator (second node) (third node) relations))
    (:append (make-append-sql-iterator (second node) relations))
    (:subquery-scan
     (physical-scan-relation node relations)
     (let* ((frames (cons (cons (if (getf (physical-scan-relation node relations) :lateral-dependent)
                                    *lateral-outer-environment* #())
                                (make-hash-table :test #'eq)) *subquery-outer-frames*))
            (input (let ((*subquery-outer-frames* frames))
                     (when (and *datum-origin-capture*
                                (getf (physical-scan-relation node relations) :lateral-dependent))
                       (setf (gethash +subquery-frame-origins-key+ (cdar frames))
                             (datum-origins-from-spill (datum-origins-for-spill *query-row-origins*))))
                     (make-physical-plan-iterator (fourth node) relations))))
       (make-sql-iterator
        (lambda ()
          (let ((*subquery-outer-frames* frames))
            (multiple-value-bind (row present origins) (sql-iterator-next input)
              (values (car row) present origins))))
        (lambda ()
          (close-sql-iterator input)
          (close-cte-frame-cache (cdar frames))))))
    (:filter
     (make-mapping-sql-iterator
      (make-physical-plan-iterator (third node) relations)
      (lambda (row) (values row (true-p (eval-expr (second node) row)) *query-row-origins*))
      :preserve-toast-context t))
    (:project
     (let* ((deferred (and *query-sort-project*
                           (eq node (car *query-sort-project*))
                           (cdr *query-sort-project*)))
            (set-node (and (null deferred) (project-set-projection-node node)))
            (*query-project-set* (and set-node (cons set-node (second node)))))
       (make-mapping-sql-iterator
        (make-physical-plan-iterator (third node) relations)
        (lambda (row)
          (cond
            (set-node (values row t *query-row-origins*))
            (deferred (values (make-sort-project-input-row row (second node) deferred) t nil))
            (*datum-origin-capture*
             (let ((values (make-array (length (second node))))
                   (origins (make-array (length (second node)) :initial-element nil)))
               (loop for target in (second node) for i from 0
                     do (multiple-value-bind (value origin)
                            (eval-expr-with-origin (getf target :expr) row *query-row-origins*)
                          (setf (aref values i) value (aref origins i) origin)))
               (values (cons values row) t origins)))
            (t (values (make-select-result-row row nil (second node)) t nil)))))))
    (:window-input
     (make-mapping-sql-iterator
      (make-physical-plan-iterator (third node) relations)
      (lambda (row)
        (multiple-value-bind (values origins)
            (window-input-with-origins row (second node) *query-row-origins*)
          (values values t origins)))))
    (:window
     (make-window-plan-iterator (make-physical-plan-iterator (third node) relations) (second node)))
    (:project-set
     (let ((input (make-physical-plan-iterator (third node) relations)))
       (if (and *query-project-set* (eq node (car *query-project-set*)))
           (make-project-set-projection-iterator
            input (second node) (cdr *query-project-set*) (fourth node))
           (make-project-set-iterator input (second node)))))
    (:limit
     (let ((remaining (second node)) (input nil))
       (make-sql-iterator
        (lambda ()
          (when (plusp remaining)
            (unless input (setf input (make-physical-plan-iterator (third node) relations)))
            (decf remaining)
            (sql-iterator-next input)))
        (lambda () (close-sql-iterator input)))))
    (:nested-loop-join
     ;; A parameterized index scan needs a new key and iterator for each left
     ;; row, just as a LATERAL input does (cf. ExecReScanIndexScan).
     (if (or (getf (second node) :lateral-dependent)
             (and (eq (first (fourth node)) :index-scan)
                  (select-scan-path-parameter-expr
                   (physical-scan-path (fourth node) (physical-scan-relation (fourth node) relations)))))
         (make-lateral-join-iterator
          (second node) (make-physical-plan-iterator (third node) relations)
          (lambda (environment origins)
            (let ((*lateral-outer-environment* environment)
                  (*query-row-origins* origins))
              (make-physical-plan-iterator (fourth node) relations))))
         (make-nested-loop-iterator
          (second node) (make-physical-plan-iterator (third node) relations)
          (make-physical-plan-iterator (fourth node) relations))))
    ((:sort :top-n :hash-aggregate :sorted-aggregate :hash-distinct)
     (let ((child (if (eq (first node) :top-n) (fourth node) (third node))))
       ;; If a key needs an SRF value, keep expansion below sort instead of
       ;; moving it above the sort and changing the key's evaluation point.
       (multiple-value-bind (set-node width calls targets) (sort-project-set-info node child)
         (when (and set-node
                    (notany (lambda (entry)
                              (project-set-expression-dependent-p
                               (car entry) width (length calls) targets))
                            (second node)))
           (return-from %make-physical-plan-iterator
             (make-sorted-project-set-iterator node child set-node width calls relations)))))
     (let* ((child (if (member (first node) '(:top-n :hash-aggregate :sorted-aggregate))
                       (fourth node) (third node)))
            (deferred (sort-projection-deferred-slots node child))
            (*query-sort-project* (and deferred (cons child deferred)))
            (input (make-physical-plan-iterator child relations))
            (buffer
              (make-buffered-sql-iterator
               (lambda (emit)
                 (flet ((source (callback) (map-sql-iterator input callback)))
                   (case (first node)
                     (:sort (stream-select-sort #'source (second node) emit))
                     (:top-n (dolist (row (collect-select-top-n #'source (second node) (third node) *datum-origin-capture*))
                               (let ((*query-row-origins* (and *datum-origin-capture* (cdr row))))
                                 (funcall emit (if *datum-origin-capture* (car row) row)))))
                     ((:hash-aggregate :sorted-aggregate)
                      (stream-select-aggregates #'source (second node) (third node) emit))
                     (:hash-distinct
                      (stream-hash-aggregate
                       (lambda (callback) (source (lambda (row) (funcall callback (car row)))))
                       (second node) 0 (lambda (values) (funcall emit (cons values values))))))))
               input)))
       (if deferred
           (make-mapping-sql-iterator
            buffer (lambda (row) (values (finish-sort-project-row row (second child) deferred) t nil)))
           buffer)))
    (otherwise (ereport "0A000" "unsupported resumable producer node ~a" (first node)))))
