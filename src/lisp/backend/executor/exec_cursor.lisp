;;;; exec_cursor.lisp -- resumable physical plans for demand-driven producers.
(in-package #:pg)

(defun make-mapping-sql-iterator (input function &key preserve-toast-context)
  ;; FUNCTION returns the output row, whether to keep it, and its origins.
  ;; The fourth iterator value is a TOAST slice context for the current row.
  ;; Bind it only while evaluating that row; pass it on only when requested.
  (make-sql-iterator
   (lambda ()
     (loop
       (multiple-value-bind (row present origins toast-context presence) (sql-iterator-next input)
         (unless present (return (values nil nil nil nil)))
         ;; A scan's saved pointer belongs only to this input row. Nested
         ;; subqueries must compute their own demand and slice plans.
         (let ((*query-row-origins* (and *datum-origin-capture* origins))
               (*toast-slice-current* toast-context)
               (*query-row-presence* presence)
               (*select-toast-demand* nil)
               (*select-toast-slice-plan* nil))
           (multiple-value-bind (result keep result-origins) (funcall function row)
             (when keep
               (return (values result t result-origins
                               (and preserve-toast-context toast-context) presence))))))))
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

(defun postponable-sort-project-set (node child)
  "Return the ProjectSet, input width and calls when sort keys can run first."
  (when (and (member (first node) '(:sort :top-n))
             (eq (first child) :project) (eq (first (third child)) :project-set))
    (let* ((set-node (third child))
           (width (fourth set-node))
           (calls (second set-node))
           (targets (second child))
           (call-count (length calls)))
      (when (and (integerp width) (not (minusp width)) calls
                 (notany (lambda (entry)
                           (project-set-expression-dependent-p
                            (car entry) width call-count targets))
                         (second node)))
        (values set-node width calls)))))

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
  (let ((tids (make-select-index-tid-iterator table path environment))
        (presence (vector (ensure-select-source-identity relation))))
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
                 (multiple-value-bind (projected-row projected-origins)
                     (project-select-scan-row relations relation row origins)
                   (return (values projected-row t projected-origins nil presence)))))))))
     (lambda () (close-sql-iterator tids)))))

(defun make-select-scan-iterator (node relations)
  ;; Capture the parameter environment and statement snapshot for this scan's
  ;; lifetime; later outer rows must not change its index key or visibility.
  (let* ((relation (physical-scan-relation node relations))
         (table (getf relation :table))
         (presence (vector (ensure-select-source-identity relation)))
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
             (return (values (project-select-scan-row relations relation row nil)
                             t nil nil presence))))
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
                 (multiple-value-bind (projected-row projected-origins)
                     (project-select-scan-row relations relation row origins)
                   (return (values projected-row t projected-origins toast-context presence)))))
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
         (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
           (declare (ignore toast))
           (let ((*query-row-presence* presence))
             (if present
                 (multiple-value-bind (projected result-origins)
                     (project-append-row coercions (car row) origins)
                   (return (values projected t result-origins nil presence)))
                 (progn (close-sql-iterator input) (setf input nil)))))))
     (lambda () (close-sql-iterator input)))))

(defun make-project-set-iterator (input calls &key attempt-function)
  "Keep the current input row until all of its sets are exhausted."
  (let ((width (length calls))
        (environment nil) (input-origins nil) (input-presence nil) (sets nil))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless sets
           (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
             (declare (ignore toast))
             (unless present (return (values nil nil)))
             (setf environment row input-origins origins input-presence presence
                   sets (map 'simple-vector #'identity calls))))
         (let* ((*query-row-origins* input-origins)
                (*query-row-presence* input-presence)
                (values (project-set-next-values environment sets attempt-function)))
           (if values
               (return (values (concatenate 'vector environment values) t
                               (project-set-row-origins input-origins (length environment) width)
                               nil input-presence))
               (setf sets nil)))))
     (lambda ()
       (setf sets nil environment nil input-origins nil)
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

(defmacro make-limit-sql-iterator (count input &key lazy)
  "Validate COUNT on pull and forward the input row and its metadata.
LAZY defers INPUT until the first allowed pull. Expand at the call site so an
existing input needs no lazy-child captures or branch."
  (let ((remaining (gensym "LIMIT-"))
        (iterator (gensym "INPUT-")))
    `(let ((,remaining ,count)
           (,iterator ,(unless lazy input)))
       (make-sql-iterator
        (lambda ()
          (check-select-limit ,remaining)
          (when (plusp ,remaining)
            ,@(when lazy `((unless ,iterator (setf ,iterator ,input))))
            (decf ,remaining)
            (sql-iterator-next ,iterator)))
        (lambda () (close-sql-iterator ,iterator))))))

(defun sorted-project-set-keys (order deferred target-count)
  "Return normalized sort keys and their extra source expressions in SQL order."
  (let ((extra nil) (slot target-count))
    (values
     (loop for entry in order
           for expression = (car entry)
           collect (if (eq (first expression) :sort-output)
                       (progn
                         (setf (aref deferred (1- (second expression))) nil)
                         entry)
                       (progn
                         (push expression extra)
                         (cons (list :sort-output (incf slot)
                                     :type (expr-type expression) :typmod -1)
                               (cdr entry)))))
     (nreverse extra))))

(defun make-sorted-project-set-iterator (node project set-node width calls relations &key source-streamer)
  "Sort source rows before expansion when no sort key depends on a set result.

Cache ordinary targets and their origins per source row.  Source origins travel
through the sort after the target origins, then accompany each expanded input.
Bare set targets use target-list attempt order; wrapped set results remain a
separate projection.  SOURCE-STREAMER, when supplied, passes each source row to
its callback with that row's origins bound.  Otherwise the iterator reads the
source itself.  Closing the result closes the sort and its source."
  (let* ((targets (second project))
         (target-count (length targets))
         (capture-p *datum-origin-capture*)
         (deferred
           (map 'vector
                (lambda (target)
                  (or (project-set-expression-dependent-p
                       (getf target :expr) width (length calls) targets)
                      (sort-projection-volatile-p (getf target :expr))))
                targets))
         (bare-p (project-set-projection-targets-p targets width (length calls))))
    (multiple-value-bind (order extra) (sorted-project-set-keys (second node) deferred target-count)
      (let* ((cache nil)
             (cache-origins nil)
             (attempt-values nil)
             (attempt-origins nil)
             (transform
               (lambda (environment)
                 (multiple-value-bind (row origins)
                     (make-sort-project-input-row environment targets deferred)
                   (let ((keys (map 'vector
                                    (lambda (expression) (eval-expr expression environment))
                                    extra)))
                     (setf (car row) (concatenate 'vector (car row) keys))
                     (values row t origins)))))
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
                                  (lambda (environment)
                                    (multiple-value-bind (row keep origins)
                                        (funcall transform environment)
                                      (declare (ignore keep))
                                      (let ((*query-row-origins* origins))
                                        (funcall callback row)))))
                         (map-sql-iterator source callback)))
                   order emit))
                source))
             (input
               (make-mapping-sql-iterator
                sorted
                (lambda (row)
                  (unless (= (length (cdr row)) width)
                    (error "ProjectSet input width differs from its plan"))
                  (setf cache (subseq (car row) 0 target-count)
                        cache-origins (and capture-p
                                           (subseq *query-row-origins* 0 target-count)))
                  (values (cdr row) t
                          (and capture-p (subseq *query-row-origins* target-count))))))
             (expanded
               (instrument-physical-plan-iterator
                set-node
                (make-project-set-iterator
                 input calls
                 :attempt-function
                 (when bare-p
                   (lambda (environment set-values step-set)
                     (setf attempt-values (copy-seq cache)
                           attempt-origins (and capture-p (copy-seq cache-origins)))
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
                                 (multiple-value-bind (value origin)
                                     (if capture-p
                                         (eval-expr-with-origin expression environment *query-row-origins*)
                                         (values (eval-expr expression environment) nil))
                                   (setf (aref attempt-values index) value)
                                   (when attempt-origins
                                     (setf (aref attempt-origins index) origin)))))))))))
             (output
               (instrument-physical-plan-iterator
                project
                (make-mapping-sql-iterator
                 expanded
                 (lambda (environment)
                   (if bare-p
                       (values (cons attempt-values environment) t attempt-origins)
                       (let ((*query-row-origins*
                               (and capture-p
                                    (concatenate 'vector cache-origins *query-row-origins*))))
                         (multiple-value-bind (row origins)
                             (finish-sort-project-row (cons cache environment) targets deferred)
                           (values row t origins)))))))))
        (if (eq (first node) :top-n)
            (make-limit-sql-iterator (third node) output)
            output)))))

(defun make-lateral-join-iterator (join left right-factory)
  ;; Each left row owns a fresh right iterator and parameter epoch.
  (let ((qual (getf join :qual))
        (right nil) (current-left nil) (left-origins nil) (left-presence nil) (left-present nil) (matched nil)
        (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless left-present
           (multiple-value-bind (row present origins toast presence) (sql-iterator-next left)
             (declare (ignore toast))
             (setf current-left row left-present present
                   left-origins origins left-presence presence))
           (unless left-present (return (values nil nil)))
           (setf matched nil right (funcall right-factory current-left left-origins left-presence)))
         (multiple-value-bind (row present right-origins toast right-presence) (sql-iterator-next right)
           (declare (ignore toast))
           (if present
               (multiple-value-bind (joined origins presence)
                   (qualify-select-join-pair join qual current-left row left-origins right-origins
                                             left-presence right-presence *datum-origin-capture*)
                 (when joined
                   (setf matched t)
                   (return (values joined t origins nil presence))))
               (progn
                 (close-sql-iterator right)
                 (setf right nil left-present nil)
                 (when (and (not matched) (eq (getf join :type) :left))
                   (multiple-value-bind (values origins)
                       (null-extend-select-join-row current-left left-origins null-right nil *datum-origin-capture*)
                     (return (values values t origins nil left-presence)))))))))
     (lambda ()
       (close-sql-iterator right)
       (close-sql-iterator left)
       (setf current-left nil left-origins nil)))))

;; The join owns LEFT and a replay store owning RIGHT.  A fresh reader scans
;; RIGHT for each left row; right/full joins make one final pass for unmatched
;; rows.  Closing the join closes the active reader, both inputs and match map.
(defun make-nested-loop-iterator (join left right)
  (let* ((qual (getf join :qual))
         (outer-p (member (getf join :type) '(:right :full)))
         (spool-memory (nested-loop-spool-memory outer-p))
         (match-memory (nested-loop-match-memory spool-memory))
         (store (make-query-replay-store right spool-memory))
         (reader nil) (current-left nil) (left-origins nil) (left-presence nil) (left-present nil)
         (left-done nil) (left-seen nil) (matched nil) (right-phase nil)
         (right-index 0)
         (matches nil)
         (null-left (make-array (getf join :left-width) :initial-element +null+))
         (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless (or left-present left-done)
           (multiple-value-bind (row present origins toast presence) (sql-iterator-next left)
             (declare (ignore toast))
             (setf current-left row left-present present
                   left-origins origins left-presence presence))
           (unless left-present (setf left-done t))
           (when left-present
             (when (and outer-p (not left-seen))
               (setf matches (make-query-match-map match-memory)))
             (setf left-seen t matched nil right-index 0 reader (make-query-replay-reader store))))
         (when (and left-done (not outer-p)) (return (values nil nil)))
         (when (and left-done (not right-phase))
           (setf right-phase t right-index 0)
           (when left-seen
             (setf reader (make-query-replay-reader store))
             (query-match-map-finish matches)))
         ;; With no left rows, nothing will replay the right input or consult the match map.
         (multiple-value-bind (row present right-origins toast right-presence)
             (sql-iterator-next (if left-seen reader right))
           (declare (ignore toast))
           (cond
             (right-phase
              (unless present
                (unless left-seen (close-sql-iterator right))
                (return (values nil nil)))
              (let ((index right-index))
                (incf right-index)
                (unless (and left-seen (query-match-map-matched-p matches index))
                  (multiple-value-bind (values origins)
                      (null-extend-select-join-row row right-origins null-left t *datum-origin-capture*)
                    (return (values values t origins nil right-presence))))))
             (present
              (when (and matches (= (query-match-map-count matches) right-index))
                (query-match-map-append matches))
              (let ((index right-index))
                (incf right-index)
                (multiple-value-bind (joined origins presence)
                    (qualify-select-join-pair join qual current-left row left-origins right-origins
                                              left-presence right-presence *datum-origin-capture*)
                  (when joined
                    (setf matched t)
                    (when matches (query-match-map-mark matches index))
                    (return (values joined t origins nil presence))))))
             (t
              (close-sql-iterator reader)
              (setf reader nil left-present nil)
              (when (and (not matched) (member (getf join :type) '(:left :full)))
                (multiple-value-bind (values origins)
                    (null-extend-select-join-row current-left left-origins null-right nil *datum-origin-capture*)
                  (return (values values t origins nil left-presence)))))))))
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
                  (multiple-value-bind (row present origins toast-context presence) (sql-iterator-next input)
                    (when (and started present)
                      (incf (plan-node-metadata-actual-row-count metadata))
                      (setf (plan-node-metadata-actual-rows metadata)
                            (/ (plan-node-metadata-actual-row-count metadata)
                               (plan-node-metadata-actual-loops metadata))))
                    (values row present origins toast-context presence))
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
  (multiple-value-bind (demand slice-plan)
      (select-plan-toast-access (query-plan-physical plan) relations)
    (let* ((*select-toast-demand* demand)
           (*select-toast-slice-plan* slice-plan)
           (*toast-slice-current* nil)
           (*cte-active-plan* plan)
           (input (make-physical-plan-iterator (query-plan-physical plan) relations)))
      (make-sql-iterator
       (lambda ()
         (let ((*select-toast-demand* demand)
               (*select-toast-slice-plan* slice-plan)
               (*toast-slice-current* nil)
               (*cte-active-plan* plan))
           (multiple-value-bind (row present origins toast-context presence) (sql-iterator-next input)
             (declare (ignore toast-context))
             (values row present origins nil presence))))
       (lambda () (close-sql-iterator input))))))

(defun make-subquery-scan-iterator (node relations)
  (multiple-value-bind (frame presence)
      (make-physical-subquery-scan-frame node relations *lateral-outer-environment*)
    (let ((frames (cons frame *subquery-outer-frames*))
          (input nil)
          (iterator nil))
      (flet ((close-input (owned)
               (unwind-protect (close-sql-iterator owned)
                 (close-cte-frame-cache (cdr frame)))))
        (unwind-protect
             (let ((*subquery-outer-frames* frames))
               (setf input (make-physical-plan-iterator (fourth node) relations))
               (let ((owned input))
                 (setf iterator
                       (make-sql-iterator
                        (lambda ()
                          (let ((*subquery-outer-frames* frames))
                            (multiple-value-bind (row present origins) (sql-iterator-next owned)
                              (values (car row) present origins nil (and present presence)))))
                        (lambda () (close-input owned))))))
          (unless iterator (close-input input)))))))

(defun make-physical-join-iterator (node relations)
  ;; A parameterized index scan needs a new key and iterator for each left
  ;; row, just as a LATERAL input does (cf. ExecReScanIndexScan).
  (if (physical-join-rescans-inner-p (second node) (fourth node))
      (make-lateral-join-iterator
       (second node) (make-physical-plan-iterator (third node) relations)
       (lambda (environment origins presence)
         (let ((*lateral-outer-environment* environment)
               (*query-row-origins* origins)
               (*query-row-presence* presence))
           (make-physical-plan-iterator (fourth node) relations))))
      (make-nested-loop-iterator
       (second node) (make-physical-plan-iterator (third node) relations)
       (make-physical-plan-iterator (fourth node) relations))))

(defun %make-physical-plan-iterator (node relations)
  (case (first node)
    (:result
     (let ((done nil))
       (make-sql-iterator (lambda () (unless done (setf done t) (values #() t nil nil #()))))))
    ((:seq-scan :index-scan) (make-select-scan-iterator node relations))
    (:cte-scan (make-cte-scan-iterator (physical-scan-relation node relations)))
    (:worktable-scan (make-cte-worktable-iterator (physical-scan-relation node relations)))
    (:recursive-union (make-recursive-union-iterator node relations))
    (:with-ctes (make-with-ctes-iterator (second node) (third node) relations))
    (:append (make-append-sql-iterator (second node) relations))
    (:subquery-scan (make-subquery-scan-iterator node relations))
    (:filter
     (make-mapping-sql-iterator
      (make-physical-plan-iterator (third node) relations)
      (lambda (row) (values row (true-p (eval-expr (second node) row)) *query-row-origins*))
      :preserve-toast-context t))
    (:project
     (let* ((targets (second node))
            (deferred (and *query-sort-project*
                           (eq node (car *query-sort-project*))
                           (cdr *query-sort-project*)))
            (set-node (and (null deferred) (project-set-projection-node node)))
            (*query-project-set* (and set-node (cons set-node targets))))
       (make-mapping-sql-iterator
        (make-physical-plan-iterator (third node) relations)
        (lambda (row)
          (multiple-value-bind (projected origins)
              (project-physical-row row targets deferred set-node)
            (values projected t origins))))))
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
     (make-limit-sql-iterator (second node)
                              (make-physical-plan-iterator (third node) relations) :lazy t))
    (:nested-loop-join (make-physical-join-iterator node relations))
    ((:sort :top-n :hash-aggregate :sorted-aggregate :hash-distinct :sorted-distinct)
     (let ((child (physical-buffered-node-child node)))
       ;; If a key needs an SRF value, keep expansion below sort instead of
       ;; moving it above the sort and changing the key's evaluation point.
       (multiple-value-bind (set-node width calls) (postponable-sort-project-set node child)
         (when set-node
           (return-from %make-physical-plan-iterator
             (make-sorted-project-set-iterator node child set-node width calls relations)))))
     (let* ((child (physical-buffered-node-child node))
            (deferred (sort-projection-deferred-slots node child))
            (*query-sort-project* (and deferred (cons child deferred)))
            (input (make-physical-plan-iterator child relations))
            (buffer
              (make-buffered-sql-iterator
               (lambda (emit)
                 (flet ((source (callback) (map-sql-iterator input callback)))
                   (stream-physical-buffered-node node #'source emit)))
               input)))
       (if deferred
           (make-mapping-sql-iterator
            buffer
            (lambda (row)
              (multiple-value-bind (projected origins)
                  (finish-sort-project-row row (second child) deferred)
                (values projected t origins))))
           buffer)))
    (otherwise (ereport "0A000" "unsupported resumable producer node ~a" (first node)))))
