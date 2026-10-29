;;;; exec_cursor_window.lisp -- resumable WindowAgg output.
(in-package #:pg)

(defun make-window-plan-iterator (input stage)
  ;; ROWS prefixes can be returned immediately. RANGE peers and whole
  ;; partitions must be accumulated before their rows can share a result;
  ;; PENDING holds the first row of the next group across iterator calls.
  (let* ((partition (getf stage :partition))
         (order (getf stage :order))
         (keys (append partition (mapcar #'car order)))
         (directions (append (make-list (length partition)) (mapcar #'cdr order)))
         (decorated (make-mapping-sql-iterator
                     input
                     (lambda (row)
                       (let ((padded (make-array (getf stage :width) :initial-element +null+)))
                         (replace padded row :end2 (min (length row) (length padded)))
                         (values (cons (map 'vector (lambda (key) (eval-expr key row)) keys) padded) t
                                 (window-pad-origins *query-row-origins* (length padded)))))))
         (source (if (and keys (getf stage :sort-required t))
                     (make-buffered-sql-iterator
                      (lambda (emit)
                        (stream-select-sort
                         (lambda (callback) (map-sql-iterator decorated callback))
                         (loop for direction in directions for position from 1
                               collect (cons (list :sort-output position) direction))
                         emit))
                      decorated)
                     decorated))
         (functions (getf stage :functions))
         (aggregates (remove-if (lambda (function)
                                  (member (getf function :kind) '(:row-number :rank :dense-rank)))
                                functions))
         (whole-p (and aggregates (eq (second (getf stage :frame)) :unbounded-following)))
         (peers-p (and aggregates (not whole-p) (eq (first (getf stage :frame)) :range)))
         (states nil) (position 0) (rank 1) (dense 1)
         (partition-key nil) (peer-key nil) (pending nil)
         (buffer nil) (reader nil) (results nil))
    (labels
        ((close-buffer ()
           (when reader (close-sql-iterator reader) (setf reader nil))
           (when buffer (close-query-replay-store buffer) (setf buffer nil))
           (setf results nil))
         (take-item ()
           (if pending (values (prog1 pending (setf pending nil)) t)
               (sql-iterator-next source)))
         (row-partition (item) (subseq (car item) 0 (length partition)))
         (row-peer (item) (subseq (car item) (length partition)))
         (advance (item)
           (let* ((environment (cdr item))
                  (new-partition (row-partition item))
                  (new-peer (row-peer item))
                  (same-partition (and (plusp position)
                                       (window-key-equal-p partition-key new-partition)))
                  (same-peer (and same-partition (window-key-equal-p peer-key new-peer))))
             (unless same-partition
               (close-aggregate-transition-states states)
               (setf states (make-aggregate-transition-states aggregates)
                     position 0 rank 1 dense 1 partition-key (copy-query-sort-value new-partition)))
             (incf position)
             (when (> position 1)
               (destructuring-bind (new-rank new-dense)
                   (window-rank-step position rank dense same-peer)
                 (setf rank new-rank dense new-dense)))
             (setf peer-key (copy-query-sort-value new-peer))
             (let ((inputs (window-aggregate-inputs aggregates environment)))
               (step-aggregate-transition-states states aggregates inputs 0))
             (list environment position rank dense)))
         (project (row values)
           (window-project-row (first row) (getf stage :width) functions values
                               (second row) (third row) (fourth row))))
      (make-sql-iterator
       (lambda ()
         (loop
           (when reader
             (multiple-value-bind (row present) (sql-iterator-next reader)
               (when present (return (values (project row results) t))))
             (close-buffer))
           (multiple-value-bind (first present origins) (take-item)
             (unless present (return (values nil nil)))
             (unless (or whole-p peers-p)
               (let ((row (advance first)))
                 (return (values (project row (window-transition-results states)) t
                                 (window-output-origins origins (getf stage :width) functions)))))
             (setf buffer (make-query-replay-store nil))
             (let ((group-partition (row-partition first)) (group-peer (row-peer first)))
               (append-query-replay-row buffer (advance first))
               (loop
                 (multiple-value-bind (item present) (sql-iterator-next source)
                   (unless present (return))
                   (unless (and (window-key-equal-p group-partition (row-partition item))
                                (or whole-p (window-key-equal-p group-peer (row-peer item))))
                     (setf pending item)
                     (return))
                   (append-query-replay-row buffer (advance item)))))
             (query-row-spool-seal (query-replay-store-spool buffer))
             (setf (query-replay-store-eof-p buffer) t
                   results (window-transition-results states)
                   reader (make-query-replay-reader buffer)))))
       (lambda ()
         (close-buffer)
         (close-aggregate-transition-states states)
         (close-sql-iterator source)
         (setf states nil pending nil partition-key nil peer-key nil))))))
