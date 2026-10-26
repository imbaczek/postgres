;;;; join-tests.lisp -- nested-loop streaming and outer-join bookkeeping.

(in-package #:pg)

(defun test-streamed-join-stop-and-outer-matches ()
  (let ((original-eval (symbol-function 'eval-expr))
        (qual-evaluations 0)
        (right-visits 0)
        (emitted '()))
    (unwind-protect
         (progn
           (setf (symbol-function 'eval-expr)
                 (lambda (expr env)
                   (when (equal expr '(:bool t))
                     (incf qual-evaluations))
                   (funcall original-eval expr env)))
           (stream-select-join-environments
            (lambda (visit-left)
              (loop for value from 1 to 1000
                    until (eq (funcall visit-left (vector value)) :stop)))
            (lambda (visit-right)
              (loop for value from 1 to 1000
                    do (incf right-visits)
                    until (eq (funcall visit-right (vector value)) :stop)))
            (list :type :cross :qual '(:bool t)
                  :left-width 1 :right-width 1)
            (lambda (environment)
              (push environment emitted)
              :stop))
           (check (= qual-evaluations 1)
                  "streamed nested loop stops after the first accepted pair")
           (check (= right-visits 1)
                  "nested loop stops the inner stream at its first output")
           (check (and (= (length emitted) 1)
                       (= (aref (first emitted) 0) 1)
                       (= (aref (first emitted) 1) 1))
                  "streamed nested loop emits its first pair before stopping")
           (setf emitted '())
           (stream-select-join-environments
            (lambda (visit-left)
              (dolist (row (list (vector 1) (vector 2)))
                (when (eq (funcall visit-left row) :stop)
                  (return :stop))))
            (lambda (visit-right)
              (dolist (row (list (vector 9) (vector 10)))
                (when (eq (funcall visit-right row) :stop)
                  (return :stop))))
            (list :type :full :qual '(:bool nil)
                  :left-width 1 :right-width 1)
            (lambda (environment)
              (push environment emitted)))
           (setf emitted (nreverse emitted))
           (check (= (length emitted) 4)
                  "FULL JOIN emits unmatched rows from both inputs")
           (check (and (= (aref (first emitted) 0) 1)
                       (eq (aref (first emitted) 1) +null+)
                       (= (aref (second emitted) 0) 2)
                       (eq (aref (second emitted) 1) +null+))
                  "FULL JOIN null-extends each unmatched left row")
           (check (and (eq (aref (third emitted) 0) +null+)
                       (= (aref (third emitted) 1) 9)
                       (eq (aref (fourth emitted) 0) +null+)
                       (= (aref (fourth emitted) 1) 10))
                  "FULL JOIN emits unmatched right rows after the left input"))
      (setf (symbol-function 'eval-expr) original-eval))))

(defun test-lazy-join-interpreter-stops-inner-scan ()
  (let* ((left-table (make-pg-class :oid 92001 :name "left_input"
                                    :relfilenode 92011 :columns #()))
         (right-table (make-pg-class :oid 92002 :name "right_input"
                                     :relfilenode 92012 :columns #()))
         (left-path (make-select-scan-path :kind :seq-scan))
         (right-path (make-select-scan-path :kind :seq-scan))
         (join (list :type :cross :qual '(:bool t :type :bool)
                     :left-width 1 :right-width 1))
         (scan-node (lambda (index table)
                      (list :seq-scan index (pg-class-oid table))))
         (registered (list (list :table left-table :path left-path)
                           (list :table right-table :path right-path)))
         (original-scan (symbol-function 'scan-select-path))
         (right-visits 0)
         (left-empty-p nil)
         (rows 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'scan-select-path)
                 (lambda (table relfilenode path snapshot callback
                          &optional environment)
                   (declare (ignore relfilenode path snapshot environment))
                   (cond
                     ((eq table left-table)
                      (unless left-empty-p
                        (funcall callback
                                 (make-htup :values #(1)))))
                     ((eq table right-table)
                      (loop for value from 1 to 1000
                            do (incf right-visits)
                            until (eq (funcall callback
                                               (make-htup
                                                :values (vector value)))
                                      :stop))))))
           (interpret-query-plan
            (%make-query-plan
             :physical
             (list :limit 1
                   (list :nested-loop-join join
                         (funcall scan-node 0 left-table)
                         (funcall scan-node 1 right-table))))
            registered
            (lambda (row) (declare (ignore row)) (incf rows)))
           (check (= rows 1)
                  "plan interpreter returns the first joined row under LIMIT")
           (check (= right-visits 1)
                  "plan interpreter does not consume the full inner scan for LIMIT 1")
           (setf right-visits 0)
           (setf left-empty-p t)
           (interpret-query-plan
            (%make-query-plan
             :physical
             (list :nested-loop-join join
                   (funcall scan-node 0 left-table)
                   (funcall scan-node 1 right-table)))
            (list (list :table left-table :path left-path)
                  (list :table right-table :path right-path))
            (lambda (row) (declare (ignore row)) (incf rows)))
           (check (zerop right-visits)
                  "plan interpreter skips the inner scan when the outer is empty"))
      (setf (symbol-function 'scan-select-path) original-scan))))

(defun test-streamed-join-on-matches-survive-where-and-stop ()
  (let ((original-eval (symbol-function 'eval-expr))
        (emitted '())
        (where-rejected-match '()))
    (unwind-protect
         (progn
           (setf (symbol-function 'eval-expr)
                 (lambda (expr env)
                   (case expr
                     (:join-equal (= (aref env 0) (aref env 1)))
                     (:where-null-left-or-right-two
                      (or (eq (aref env 0) +null+)
                          (eql (aref env 1) 2)))
                     (t (funcall original-eval expr env)))))
           ;; The (1, 1) pair matches ON but fails WHERE. Its right row must
           ;; remain matched so FULL JOIN does not emit it a second time as an
           ;; unmatched right row. Only right value 2 survives as unmatched.
           (stream-select-join-environments
            (lambda (visit-left) (funcall visit-left #(1)))
            (lambda (visit-right)
              (dolist (row (list #(1) #(2)))
                (when (eq (funcall visit-right row) :stop)
                  (return :stop))))
            (list :type :full :qual :join-equal
                  :left-width 1 :right-width 1)
            (lambda (environment)
              (when (true-p
                     (eval-expr :where-null-left-or-right-two environment))
                (push environment where-rejected-match))))
           (check (and (= (length where-rejected-match) 1)
                       (eq (aref (first where-rejected-match) 0) +null+)
                       (= (aref (first where-rejected-match) 1) 2))
                  "FULL JOIN tracks ON matches even when WHERE rejects the pair")

           ;; A consumer that reaches LIMIT on an unmatched left row must stop
           ;; the remaining outer-join work as well.
           (let ((status
                   (stream-select-join-environments
                    (lambda (visit-left)
                      (dolist (row (list #(3) #(4)))
                        (when (eq (funcall visit-left row) :stop)
                          (return :stop))))
                    (lambda (visit-right)
                      (declare (ignore visit-right))
                      (values))
                    (list :type :left :qual '(:bool t)
                          :left-width 1 :right-width 1)
                    (lambda (environment)
                      (push environment emitted)
                      :stop))))
             (check (eq status :stop)
                    "outer-join stop propagates from unmatched-row emission"))
           (check (and (= (length emitted) 1)
                       (= (aref (first emitted) 0) 3)
                       (eq (aref (first emitted) 1) +null+))
                  "stopping after one LEFT JOIN miss skips later left rows"))
      (setf (symbol-function 'eval-expr) original-eval))))

(defun test-select-row-qualification-before-projection ()
  (let ((original-eval (symbol-function 'eval-expr))
        (projection-count 0)
        (environment (vector 7))
        (targets (list (list :expr :project))))
    (unwind-protect
         (progn
           (setf (symbol-function 'eval-expr)
                 (lambda (expr env)
                   (case expr
                     (:accept t)
                     (:reject nil)
                     (:project
                      (incf projection-count)
                      (aref env 0))
                     (t (funcall original-eval expr env)))))
           (check (null (make-select-result-row environment :reject targets))
                  "SELECT row builder discards rows rejected by WHERE")
           (check (zerop projection-count)
                  "SELECT row builder does not project rejected rows")
           (check (null (collect-select-no-from-rows :reject targets))
                  "FROM-less SELECT discards rows rejected by WHERE")
           (check (zerop projection-count)
                  "FROM-less SELECT checks WHERE before projecting targets")
           (let ((row (make-select-result-row environment :accept targets)))
             (check (and row (equalp (car row) #(7)))
                    "SELECT row builder projects qualifying rows")
             (check (eq (cdr row) environment)
                    "SELECT row builder retains the source environment")
             (check (= projection-count 1)
                    "SELECT row builder evaluates projection once"))
           (let ((status
                   (let ((row
                           (make-select-result-row
                            environment :accept targets)))
                     (when row
                       (funcall (lambda (projected-row)
                                  (declare (ignore projected-row))
                                  :stop)
                                row)))))
             (check (eq status :stop)
                    "shared SELECT projection preserves join stop signals")))
      (setf (symbol-function 'eval-expr) original-eval))))
