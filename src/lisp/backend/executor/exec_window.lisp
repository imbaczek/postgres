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
    (if (zerop (length result)) +null+
        (make-sql-array :element-type-id :text :elements result
                        :dimensions (list (length result)) :lower-bounds (list 1)))))

(defun window-transition-results (states)
  (map 'vector (lambda (state)
                 (if (query-row-spool-p state) (window-array-snapshot state)
                     (scalar-aggregate-transition-result state)))
       states))

(defun window-aggregate-inputs (aggregates environment)
  ;; Copy evaluated inputs before transition state can retain them across rows.
  (let ((values (make-array (length aggregates) :initial-element +null+))
        (origins (and *datum-origin-capture* (make-array (length aggregates) :initial-element nil))))
    (loop for descriptor in aggregates for i from 0
          for expression = (getf descriptor :input)
          when expression
            do (multiple-value-bind (value origin)
                   (if *datum-origin-capture*
                       (eval-expr-with-origin expression environment *query-row-origins*)
                       (values (eval-expr expression environment) nil))
                 (setf (aref values i) (copy-query-sort-value value))
                 (when origins (setf (aref origins i) origin))))
    (values values origins)))

(defun window-project-row (environment width functions results position rank dense)
  ;; Copy the outer row vector and each aggregate result; base-column values
  ;; remain shared with ENVIRONMENT and are not deep-copied here.
  (let ((result (subseq environment 0 width)) (aggregate-index 0))
    (dolist (function functions result)
      (setf (aref result (getf function :slot))
            (case (getf function :kind)
              (:row-number position) (:rank rank) (:dense-rank dense)
              (otherwise (prog1 (copy-query-sort-value (aref results aggregate-index))
                           (incf aggregate-index))))))))

(defun window-pad-origins (origins width)
  (when *datum-origin-capture*
    (let ((result (make-array width :initial-element nil)))
      (when origins (replace result origins :end1 (min width (length origins))))
      result)))

(defun window-output-origins (origins width functions &optional transition-origins)
  (let ((result (window-pad-origins origins width)) (aggregate-index 0))
    (when result
      (dolist (function functions)
        (setf (aref result (getf function :slot))
              (if (member (getf function :kind) '(:row-number :rank :dense-rank))
                  nil
                  (prog1 (and transition-origins (aref transition-origins aggregate-index))
                    (incf aggregate-index))))))
    result))

(defun window-input-with-origins (environment expressions origins)
  (let* ((width (length environment))
         (values (make-array (+ width (length expressions))))
         (result-origins (window-pad-origins origins (length values))))
    (replace values environment)
    (loop for expression in expressions for i from width
          do (multiple-value-bind (value origin)
                 (if *datum-origin-capture*
                     (eval-expr-with-origin expression environment origins)
                     (values (eval-expr expression environment) nil))
               (setf (aref values i) value)
               (when result-origins (setf (aref result-origins i) origin))))
    (values values result-origins)))

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
                                (buffer nil) (states nil) (transition-origins nil) (partition-key nil) (peer-key nil)
                                (position 0) (rank 1) (dense 1))
                           (labels
                               ((close-buffer ()
                                  (when buffer
                                    (close-query-row-spool buffer)
                                    (when (query-row-spool-pathname buffer)
                                      (delete-query-temp-file (query-row-spool-pathname buffer)))
                                    (setf buffer nil)))
                                (advance-states (environment)
                                  (multiple-value-bind (inputs input-origins) (window-aggregate-inputs aggregates environment)
                                    (step-aggregate-transition-origins states aggregates inputs input-origins transition-origins 0)
                                    (step-aggregate-transition-states states aggregates inputs 0)))
                                (output (environment results row-position row-rank row-dense)
                                  (let ((*query-row-origins* (window-output-origins *query-row-origins* width functions transition-origins)))
                                    (funcall emit (window-project-row environment width functions results
                                                                      row-position row-rank row-dense))))
                                (flush-buffer ()
                                  (when buffer
                                    (query-row-spool-seal buffer)
                                    (let ((results (window-transition-results states)))
                                      (unwind-protect
                                           (when (eq (map-query-row-spool
                                                      buffer
                                                      (lambda (row index)
                                                        (declare (ignore index))
                                                        (let ((*query-row-origins* (datum-origins-from-spill (fifth row))))
                                                          (output (first row) results (second row) (third row) (fourth row)))))
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
                                            transition-origins (and *datum-origin-capture* (make-array (length aggregates) :initial-element nil))
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
                                          (query-row-spool-append buffer (list (copy-query-sort-value environment) position rank dense
                                                                               (and *datum-origin-capture* (datum-origins-for-spill *query-row-origins*)))))
                                        (output environment (window-transition-results states) position rank dense))))
                                (input (callback)
                                  (funcall source
                                           (lambda (environment)
                                             (let* ((padded (make-array width :initial-element +null+))
                                                    (values (map 'vector (lambda (key) (eval-expr key environment)) keys)))
                                               (replace padded environment :end2 (min width (length environment)))
                                               (let ((*query-row-origins* (window-pad-origins *query-row-origins* width)))
                                                 (funcall callback (cons values padded)))))))
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
