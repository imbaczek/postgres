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
         (transition (make-window-transition)) (results-origins nil) (pending nil)
         (buffer nil) (reader nil) (results nil))
    (labels
        ((close-buffer ()
           (when reader (close-sql-iterator reader) (setf reader nil))
           (when buffer (close-query-replay-store buffer) (setf buffer nil))
           (setf results nil results-origins nil))
         (take-item ()
           (if pending
               (let ((saved (prog1 pending (setf pending nil))))
                 (values (first saved) t (second saved) nil (third saved)))
               (sql-iterator-next source)))
         (row-partition (item) (subseq (car item) 0 (length partition)))
         (row-peer (item) (subseq (car item) (length partition)))
         (advance (item origins presence)
           (let ((*query-row-origins* origins)
                 (*query-row-presence* presence)
                 (environment (cdr item)))
             (multiple-value-bind (position rank dense)
                 (advance-window-transition transition aggregates environment
                                            (row-partition item) (row-peer item))
               (list environment position rank dense))))
         (project (row values)
           (window-project-row (first row) (getf stage :width) functions values
                               (second row) (third row) (fourth row))))
      (make-sql-iterator
       (lambda ()
         (loop
           (when reader
             (multiple-value-bind (row present origins toast presence) (sql-iterator-next reader)
               (declare (ignore toast))
               (when present (return (values (project row results) t
                                             (window-output-origins origins (getf stage :width) functions results-origins)
                                             nil presence))))
             (close-buffer))
           (multiple-value-bind (first present origins toast presence) (take-item)
             (declare (ignore toast))
             (unless present (return (values nil nil)))
             (unless (or whole-p peers-p)
               (let ((row (advance first origins presence)))
                 (return (values (project row (aggregate-transition-results (window-transition-states transition) aggregates nil)) t
                                 (window-output-origins origins (getf stage :width) functions (window-transition-origins transition))
                                 nil presence))))
             (setf buffer (make-query-replay-store nil))
             (let ((group-partition (row-partition first)) (group-peer (row-peer first)))
               (append-query-replay-row buffer (advance first origins presence) origins presence)
               (loop
                 (multiple-value-bind (item present item-origins toast item-presence) (sql-iterator-next source)
                   (declare (ignore toast))
                   (unless present (return))
                   (unless (and (window-key-equal-p group-partition (row-partition item))
                                (or whole-p (window-key-equal-p group-peer (row-peer item))))
                     (setf pending (list item item-origins item-presence))
                     (return))
                   (append-query-replay-row buffer (advance item item-origins item-presence) item-origins item-presence))))
             (query-row-spool-seal (query-replay-store-spool buffer))
             (setf (query-replay-store-eof-p buffer) t
                   results (aggregate-transition-results (window-transition-states transition) aggregates nil)
                   results-origins (let ((origins (window-transition-origins transition)))
                                     (and origins (copy-seq origins)))
                   reader (make-query-replay-reader buffer)))))
       (lambda ()
         (close-buffer)
         (close-aggregate-transition-states (window-transition-states transition))
         (close-sql-iterator source)
         (setf (window-transition-states transition) nil
               (window-transition-partition-key transition) nil
               (window-transition-peer-key transition) nil
               (window-transition-origins transition) nil
               pending nil))))))
