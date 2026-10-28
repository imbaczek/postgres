;;;; exec_window.lisp -- spillable prefix and whole-partition WindowAgg stages.
(in-package #:pg)

(defun window-rank-step (row-position prior-rank prior-dense peer)
  (if peer (list prior-rank prior-dense) (list row-position (1+ prior-dense))))

(defun window-specification-key (descriptor)
  (subquery-semantic-key (list (getf descriptor :partition) (getf descriptor :order)
                               (getf descriptor :frame))))

(defun window-invariant-sort-key-p (expression)
  (case (first expression)
    ((:const :bool :outer-ref) t)
    (:subplan
     (let ((descriptor (second expression)))
       (and (sql-subplan-plan descriptor)
            (member (sql-subplan-kind descriptor) '(:scalar :exists :array))
            (not (member 1 (sql-subplan-depths descriptor))))))
    (:binop (and (window-invariant-sort-key-p (third expression))
                 (window-invariant-sort-key-p (fourth expression))))
    (:unary (window-invariant-sort-key-p (third expression)))
    (:cast (window-invariant-sort-key-p (second expression)))
    (:func (and (not (analyzed-expression-volatile-p expression))
                (every #'window-invariant-sort-key-p (third expression))))
    (otherwise nil)))

(defun window-plan-stages (prepared)
  ;; Share stages by specification; function slots remain independently assigned.
  ;; Omit sorting only when every key is fixed within the query invocation.
  (let ((stages nil)
        (width (+ (getf prepared :window-input-width 0) (length (getf prepared :windows))))
        (base-width (- (getf prepared :window-input-width 0) (length (getf prepared :window-keys)))))
    (loop for descriptor in (getf prepared :windows) for index from 0
          for key = (window-specification-key descriptor)
          for stage = (find key stages :key (lambda (stage) (getf stage :key)) :test #'equal)
          do (unless stage
               (setf stage (list :key key :width width :partition (getf descriptor :partition)
                                 :order (getf descriptor :order) :frame (getf descriptor :frame)
                                 :sort-required
                                 (some (lambda (expression)
                                         (let ((raw (nth (- (second expression) base-width 1)
                                                         (getf prepared :window-keys))))
                                           (not (window-invariant-sort-key-p raw))))
                                       (append (getf descriptor :partition)
                                               (mapcar #'car (getf descriptor :order))))
                                 :functions nil))
               (push stage stages))
             (push (append (list :slot (+ (getf prepared :window-input-width) index))
                           descriptor)
                   (getf stage :functions)))
    (dolist (stage stages)
      (setf (getf stage :functions) (nreverse (getf stage :functions))))
    (nreverse stages)))

(defun window-key-equal-p (left right)
  (and (= (length left) (length right))
       (loop for a across left for b across right always (zerop (order-compare a b)))))

(defun window-array-snapshot (spool)
  ;; Snapshot without consuming transition state: later rows need its full prefix.
  (let ((result (make-array (query-row-spool-count spool))))
    (if (query-row-spool-pathname spool)
        (progn
          (finish-output (query-row-spool-output-stream spool))
          (with-query-temp-input (stream (query-row-spool-pathname spool))
            (dotimes (index (length result))
              (check-for-interrupts)
              (setf (aref result index) (query-spill-read-record stream)))))
        (loop for value in (reverse (query-row-spool-rows spool)) for index from 0
              do (setf (aref result index) (copy-query-sort-value value))))
    (if (zerop (length result)) +null+ result)))

(defun window-transition-results (states)
  (map 'vector (lambda (state)
                 (if (query-row-spool-p state) (window-array-snapshot state)
                     (scalar-aggregate-transition-result state)))
       states))

(defun stream-window-stage (source stage emit)
  ;; Ranking streams independently of the frame. Aggregate ROWS prefixes emit
  ;; per row; RANGE peers and whole partitions buffer until their result is known.
  ;; Owned spools close on stop, error or cancel.
  (with-query-temp-files ()
                         (let* ((partition (getf stage :partition))
                                (order (getf stage :order))
                                (keys (append partition (mapcar #'car order)))
                                (sort-order (append (mapcar (lambda (form) (declare (ignore form)) nil) partition)
                                                    (mapcar #'cdr order)))
                                (functions (getf stage :functions))
                                (aggregates (remove-if (lambda (function)
                                                         (member (getf function :kind) '(:row-number :rank :dense-rank)))
                                                       functions))
                                (width (getf stage :width))
                                (whole-p (and aggregates (eq (second (getf stage :frame)) :unbounded-following)))
                                (peers-p (and aggregates (not whole-p) (eq (first (getf stage :frame)) :range)))
                                (buffer nil) (states nil) (partition-key nil) (peer-key nil)
                                (position 0) (rank 1) (dense 1))
                           (labels
                               ((close-buffer ()
                                  (when buffer
                                    (close-query-row-spool buffer)
                                    (when (query-row-spool-pathname buffer)
                                      (delete-query-temp-file (query-row-spool-pathname buffer)))
                                    (setf buffer nil)))
                                (advance-states (environment)
                                  (let ((inputs (map 'vector
                                                     (lambda (descriptor)
                                                       (if (getf descriptor :input)
                                                           (copy-query-sort-value (eval-expr (getf descriptor :input) environment))
                                                           +null+))
                                                     aggregates)))
                                    (step-aggregate-transition-states states aggregates inputs 0)))
                                (output (environment results row-position row-rank row-dense)
                                  (let ((result (subseq environment 0 width)) (aggregate-index 0))
                                    (dolist (function functions)
                                      (setf (aref result (getf function :slot))
                                            (case (getf function :kind)
                                              (:row-number row-position) (:rank row-rank) (:dense-rank row-dense)
                                              (otherwise (prog1 (copy-query-sort-value (aref results aggregate-index))
                                                           (incf aggregate-index))))))
                                    (funcall emit result)))
                                (flush-buffer ()
                                  (when buffer
                                    (query-row-spool-seal buffer)
                                    (let ((results (window-transition-results states)))
                                      (unwind-protect
                                           (when (eq (map-query-row-spool
                                                      buffer
                                                      (lambda (row index)
                                                        (declare (ignore index))
                                                        (output (first row) results (second row) (third row) (fourth row))))
                                                     :stop)
                                             (return-from stream-window-stage :stop))
                                        (close-buffer)))))
                                (consume (row)
                                  (let* ((environment (cdr row))
                                         (all-keys (car row))
                                         (new-partition (subseq all-keys 0 (length partition)))
                                         (new-peer (subseq all-keys (length partition)))
                                         (same-partition (and (plusp position)
                                                              (window-key-equal-p new-partition partition-key)))
                                         (same-peer (and same-partition (window-key-equal-p new-peer peer-key))))
                                    (check-for-interrupts)
                                    (when (or (and (not same-partition) (plusp position))
                                              (and peers-p (not same-peer)))
                                      (flush-buffer))
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
                                    (advance-states environment)
                                    (if (or whole-p peers-p)
                                        (progn
                                          (unless buffer (setf buffer (make-query-row-spool)))
                                          (query-row-spool-append buffer (list (copy-query-sort-value environment) position rank dense)))
                                        (output environment (window-transition-results states) position rank dense))))
                                (input (callback)
                                  (funcall source
                                           (lambda (environment)
                                             (let* ((padded (make-array width :initial-element +null+))
                                                    (values (map 'vector (lambda (key) (eval-expr key environment)) keys)))
                                               (replace padded environment :end2 (min width (length environment)))
                                               (funcall callback (cons values padded))))))
                                (finish () (flush-buffer)))
                             (unwind-protect
                                  (if (and keys (getf stage :sort-required t))
                                      (stream-select-sort
                                       #'input
                                       (loop for direction in sort-order for index from 1
                                             collect (cons (list :sort-output index) direction))
                                       #'consume :on-complete #'finish)
                                      (progn
                                        (when (eq (input #'consume) :stop) (return-from stream-window-stage :stop))
                                        (finish)))
                               (close-buffer)
                               (close-aggregate-transition-states states))))))
