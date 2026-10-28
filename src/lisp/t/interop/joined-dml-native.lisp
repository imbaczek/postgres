;;;; Task-owned barriers for a planned index invalidated before DML admission.
(in-package #:pg)

(defvar *joined-dml-race-original* nil)

(defun install-joined-dml-index-race (ready release)
  (when *joined-dml-race-original* (error "Joined DML race hook already installed"))
  (let ((original (symbol-function 'prepare-dml-subqueries)) (armed t))
    (setf *joined-dml-race-original* original
          (symbol-function 'prepare-dml-subqueries)
          (lambda (ast)
            (let ((prepared (funcall original ast)))
              (when (and armed (getf ast :dml-source))
                (setf armed nil)
                (let* ((plan (getf prepared :dml-source-plan))
                       (indexed (some (lambda (node) (eq (first node) :index-scan))
                                      (collect-query-plan-nodes (query-plan-physical plan))))
                       (deadline (+ (get-internal-real-time) (* 30 internal-time-units-per-second))))
                  (with-open-file (stream ready :direction :output :if-exists :supersede)
                    (write indexed :stream stream))
                  (loop until (probe-file release)
                        do (check-for-interrupts)
                           (when (> (get-internal-real-time) deadline)
                             (error "Timed out at joined DML index race barrier"))
                           (sleep 0.01))))
              prepared))))
  t)

(defun remove-joined-dml-index-race ()
  (when *joined-dml-race-original*
    (setf (symbol-function 'prepare-dml-subqueries) *joined-dml-race-original*
          *joined-dml-race-original* nil))
  t)
