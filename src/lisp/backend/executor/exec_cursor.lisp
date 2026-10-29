;;;; exec_cursor.lisp -- resumable physical plans for demand-driven producers.
(in-package #:pg)

(defun make-mapping-sql-iterator (input function)
  (make-sql-iterator
   (lambda ()
     (loop
       (multiple-value-bind (row present origins) (sql-iterator-next input)
         (unless present (return (values nil nil nil)))
         (let ((*query-row-origins* (and *datum-origin-capture* origins)))
           (multiple-value-bind (result keep result-origins) (funcall function row)
             (when keep (return (values result t result-origins))))))))
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

(defun make-select-scan-iterator (node relations)
  ;; Capture the parameter environment and statement snapshot for this scan's
  ;; lifetime; later outer rows must not change its index key or visibility.
  (let* ((relation (physical-scan-relation node relations))
         (table (getf relation :table))
         (path (physical-scan-path node relation))
         (environment (copy-query-sort-value *lateral-outer-environment*))
         (source (getf relation :row-source))
         (block 0) (nblocks nil) (rows nil) (initialized nil)
         (snapshot *current-snapshot*)
         (capture-p *datum-origin-capture*))
    ;; Buffer base heap values for the complete key before yielding. The bounded
    ;; store can spill even under a small upper LIMIT; filters and projections
    ;; remain above it and evaluate lazily as rows are consumed.
    (when (eq (select-scan-path-kind path) :index-scan)
      (return-from make-select-scan-iterator
        (make-buffered-sql-iterator
         (lambda (emit)
           (scan-select-index
            table path snapshot
            (lambda (tuple)
              (let ((row (htup-values tuple))
                    (*query-row-origins* (and capture-p
                                              (if (getf relation :join-order-base)
                                                  (make-select-relation-origins relations relation (htup-origins tuple))
                                                  (htup-origins tuple)))))
                ;; Reordered joins still require values in source-order slots.
                (funcall emit (if (getf relation :join-order-base)
                                  (make-select-relation-environment relations relation row)
                                  row))))
            environment)
           ;; A scan can reuse leases held by an outer domain. Detach those too
           ;; before yielding to SQL filters, projections, or mutation.
           (detach-select-index-scans)))))
    (unless (eq (select-scan-path-kind path) :seq-scan)
      (error "A resumable producer requires a SeqScan plan"))
    (make-sql-iterator
     (lambda ()
       (unless initialized
         (setf initialized t)
         (if source (setf rows (funcall source))
             (setf nblocks (buf-nblocks (pg-class-relfilenode table)))))
       (loop
         (when rows
           (let* ((item (pop rows))
                  (row (if (and capture-p (not source)) (car item) item))
                  (origins (and capture-p (not source) (cdr item))))
             (return
               (values (if (getf relation :join-order-base)
                           (make-select-relation-environment relations relation row)
                           row) t
                       (if (getf relation :join-order-base)
                           (make-select-relation-origins relations relation origins)
                           origins)))))
         (when (or source (>= block nblocks)) (return (values nil nil)))
         (let ((page (buf-read (pg-class-relfilenode table) block)))
           (when page
             (heapam-scan-page page block (pg-class-columns table) snapshot
                               (lambda (tuple)
                                 (materialize-select-tuple table tuple snapshot)
                                 (push (if capture-p
                                           (cons (htup-values tuple) (htup-origins tuple))
                                           (htup-values tuple))
                                       rows)))))
         (incf block)
         (setf rows (nreverse rows))))
     (lambda () (setf rows nil)))))

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
         (multiple-value-bind (row present) (sql-iterator-next input)
           (if present
               (let ((values (map 'vector (lambda (expression) (eval-expr expression (car row)))
                                  coercions)))
                 (return (values (cons values values) t)))
               (progn (close-sql-iterator input) (setf input nil))))))
     (lambda () (close-sql-iterator input)))))

(defun make-project-set-iterator (input calls)
  (let ((environment nil) (sets nil))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless sets
           (multiple-value-bind (row present) (sql-iterator-next input)
             (unless present (return (values nil nil)))
             (setf environment row
                   sets (mapcar (lambda (call) (make-sql-set-function-iterator call environment)) calls))))
         (let ((values (make-array (length sets) :initial-element +null+)) (any nil))
           (loop for iterator in sets for index from 0
                 do (multiple-value-bind (value present) (funcall iterator)
                      (when present (setf any t (aref values index) value))))
           (if any (return (values (concatenate 'vector environment values) t))
               (setf sets nil)))))
     (lambda () (setf sets nil environment nil) (close-sql-iterator input)))))

(defun make-lateral-join-iterator (join left right-factory)
  ;; Each left row owns a fresh right iterator and parameter epoch.  Close it
  ;; before advancing the left input, including on early exit or error.
  (let ((right nil) (current-left nil) (left-present nil) (matched nil)
                    (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless left-present
           (multiple-value-setq (current-left left-present) (sql-iterator-next left))
           (unless left-present (return (values nil nil)))
           (setf matched nil right (funcall right-factory current-left)))
         (multiple-value-bind (row present) (sql-iterator-next right)
           (if present
               (let ((joined (combine-select-join-environment current-left row join)))
                 (when (true-p (eval-expr (getf join :qual) joined))
                   (setf matched t)
                   (return (values joined t))))
               (progn
                 (close-sql-iterator right)
                 (setf right nil left-present nil)
                 (when (and (not matched) (eq (getf join :type) :left))
                   (return (values (concatenate 'vector current-left null-right) t))))))))
     (lambda ()
       (close-sql-iterator right)
       (close-sql-iterator left)
       (setf current-left nil)))))

;; The join owns LEFT and a replay store owning RIGHT.  A fresh reader scans
;; RIGHT for each left row; right/full joins make one final pass for unmatched
;; rows.  Closing the join closes the active reader, both inputs and match map.
(defun make-nested-loop-iterator (join left right)
  (let* ((store (make-query-replay-store right))
         (reader nil) (current-left nil) (left-present nil)
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
           (multiple-value-setq (current-left left-present) (sql-iterator-next left))
           (unless left-present (setf left-done t))
           (when left-present
             (setf left-seen t matched nil right-index 0 reader (make-query-replay-reader store))))
         (when (and left-done (not outer-p)) (return (values nil nil)))
         (when (and left-done (not right-phase))
           (setf right-phase t right-index 0 reader (make-query-replay-reader store))
           (when left-seen (query-match-map-finish matches)))
         (multiple-value-bind (row present) (sql-iterator-next reader)
           (when (and matches present)
             (loop while (<= (query-match-map-count matches) right-index)
                   do (query-match-map-append matches)))
           (cond
             (right-phase
              (unless present (return (values nil nil)))
              (let ((index right-index))
                (incf right-index)
                (unless (and left-seen (query-match-map-matched-p matches index))
                  (return (values (concatenate 'vector null-left row) t)))))
             (present
              (let ((index right-index)
                    (joined (combine-select-join-environment current-left row join)))
                (incf right-index)
                (when (true-p (eval-expr (getf join :qual) joined))
                  (setf matched t)
                  (when matches (query-match-map-mark matches index))
                  (return (values joined t)))))
             (t
              (close-sql-iterator reader)
              (setf reader nil left-present nil)
              (when (and (not matched) (member (getf join :type) '(:left :full)))
                (return (values (concatenate 'vector current-left null-right) t))))))))
     (lambda ()
       (close-sql-iterator reader)
       (close-sql-iterator left)
       (close-query-replay-store store)
       (when matches
         (close-query-match-map matches)
         (when (query-match-map-pathname matches)
           (delete-query-temp-file (query-match-map-pathname matches))))
       (setf current-left nil)))))

(defun make-physical-plan-iterator (node relations)
  (let* ((input (%make-physical-plan-iterator node relations))
         (metadata (and *cte-active-plan* (gethash node (query-plan-metadata *cte-active-plan*))))
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
                  (multiple-value-bind (row present origins) (sql-iterator-next input)
                    (when (and started present)
                      (incf (plan-node-metadata-actual-row-count metadata))
                      (setf (plan-node-metadata-actual-rows metadata)
                            (/ (plan-node-metadata-actual-row-count metadata)
                               (plan-node-metadata-actual-loops metadata))))
                    (values row present origins))
               (when started
                 (let ((elapsed (- (get-internal-real-time) started)))
                   (incf (plan-node-metadata-actual-total-time-ticks metadata) elapsed)
                   (when first-p
                     (incf (plan-node-metadata-actual-startup-time-ticks metadata) elapsed)))))))
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
      (lambda (row) (values row (true-p (eval-expr (second node) row)) *query-row-origins*))))
    (:project
     (make-mapping-sql-iterator
      (make-physical-plan-iterator (third node) relations)
      (lambda (row)
        (if *datum-origin-capture*
            (let ((values (make-array (length (second node))))
                  (origins (make-array (length (second node)) :initial-element nil)))
              (loop for target in (second node) for i from 0
                    do (multiple-value-bind (value origin)
                           (eval-expr-with-origin (getf target :expr) row *query-row-origins*)
                         (setf (aref values i) value (aref origins i) origin)))
              (values (cons values row) t origins))
            (values (make-select-result-row row nil (second node)) t nil)))))
    (:window-input
     (make-mapping-sql-iterator
      (make-physical-plan-iterator (third node) relations)
      (lambda (row)
        (values (concatenate 'vector row (map 'vector (lambda (expr) (eval-expr expr row)) (second node))) t))))
    (:window
     (make-window-plan-iterator (make-physical-plan-iterator (third node) relations) (second node)))
    (:project-set
     (make-project-set-iterator (make-physical-plan-iterator (third node) relations) (second node)))
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
          (lambda (environment)
            (let ((*lateral-outer-environment* environment))
              (make-physical-plan-iterator (fourth node) relations))))
         (make-nested-loop-iterator
          (second node) (make-physical-plan-iterator (third node) relations)
          (make-physical-plan-iterator (fourth node) relations))))
    ((:sort :top-n :hash-aggregate :sorted-aggregate :hash-distinct)
     (let* ((child (if (member (first node) '(:top-n :hash-aggregate :sorted-aggregate))
                       (fourth node) (third node)))
            (input (make-physical-plan-iterator child relations)))
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
    (otherwise (ereport "0A000" "unsupported resumable producer node ~a" (first node)))))
