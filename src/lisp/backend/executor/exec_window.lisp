;;;; exec_window.lisp -- spillable prefix and whole-partition WindowAgg stages.
(in-package #:pg)

(defun window-rank-step (row-position prior-rank prior-dense peer)
  (if peer (list prior-rank prior-dense) (list row-position (1+ prior-dense))))

(defun window-specification-key (descriptor)
  (subquery-semantic-key (list (getf descriptor :partition) (getf descriptor :order)
                               (getf descriptor :frame))))

(defun window-invariant-sort-key-p (expression)
  "Classify a key without rescanning its arguments under each enclosing call."
  (labels ((invariant-p (node inside-call-p)
             (case (first node)
               ((:const :bool) t)
               (:outer-ref
                (or (not inside-call-p) (not (analyzed-expression-volatile-p node))))
               (:subplan
                (let ((descriptor (second node)))
                  (and (not inside-call-p)
                       (sql-subplan-plan descriptor)
                       (member (sql-subplan-kind descriptor) '(:scalar :exists :array))
                       (not (member 1 (sql-subplan-depths descriptor))))))
               (:binop (and (invariant-p (third node) inside-call-p)
                            (invariant-p (fourth node) inside-call-p)))
               (:unary (invariant-p (third node) inside-call-p))
               (:cast (invariant-p (second node) inside-call-p))
               (:func (and (not (analyzed-function-volatile-p (second node)))
                           (loop for argument in (third node) always (invariant-p argument t))))
               (otherwise nil))))
    (invariant-p expression nil)))

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

(defstruct window-transition
  states origins partition-key peer-key (position 0) (rank 1) (dense 1))

(defun advance-window-transition (transition aggregates environment partition-key peer-key
                                  &optional flush-group peers-p)
  "Advance one row after emitting any completed group from its prior state."
  (let* ((position (window-transition-position transition))
         (same-partition (and (plusp position)
                              (window-key-equal-p partition-key (window-transition-partition-key transition))))
         (same-peer (and same-partition
                         (window-key-equal-p peer-key (window-transition-peer-key transition)))))
    (when (and flush-group
               (or (and (not same-partition) (plusp position)) (and peers-p (not same-peer))))
      (funcall flush-group))
    (unless same-partition
      (close-aggregate-transition-states (window-transition-states transition))
      (setf (window-transition-states transition) (make-aggregate-transition-states aggregates)
            (window-transition-origins transition)
            (and *datum-origin-capture* (make-array (length aggregates) :initial-element nil))
            (window-transition-position transition) 0
            (window-transition-rank transition) 1
            (window-transition-dense transition) 1
            (window-transition-partition-key transition) (copy-query-sort-value partition-key)))
    (let ((position (incf (window-transition-position transition))))
      (when (> position 1)
        (destructuring-bind (rank dense)
            (window-rank-step position (window-transition-rank transition)
                              (window-transition-dense transition) same-peer)
          (setf (window-transition-rank transition) rank
                (window-transition-dense transition) dense))))
    (setf (window-transition-peer-key transition) (copy-query-sort-value peer-key))
    (let ((states (window-transition-states transition)))
      (multiple-value-bind (inputs input-origins) (window-aggregate-inputs aggregates environment)
        (step-aggregate-transition-origins states aggregates inputs input-origins
                                           (window-transition-origins transition) 0)
        (step-aggregate-transition-states states aggregates inputs 0)))
    (values (window-transition-position transition)
            (window-transition-rank transition) (window-transition-dense transition))))
