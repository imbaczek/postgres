;;;; join-tests.lisp -- nested-loop streaming and outer-join bookkeeping.

(in-package #:pg)

(defun test-streamed-join-stop-and-outer-matches ()
  (let ((original-eval (symbol-function 'eval-expr))
        (qual-evaluations 0)
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
            (loop for value from 1 to 1000 collect (vector value))
            (list :type :cross :qual '(:bool t)
                  :left-width 1 :right-width 1)
            (lambda (environment)
              (push environment emitted)
              :stop))
           (check (= qual-evaluations 1)
                  "streamed nested loop stops after the first accepted pair")
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
            (list (vector 9) (vector 10))
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
            (list #(1) #(2))
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
                    nil
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
