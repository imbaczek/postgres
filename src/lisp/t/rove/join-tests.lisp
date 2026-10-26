;;;; join-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-lazy-join-skips-inner-for-empty-outer
    (let ((right-opened 0)
          (emitted 0))
      (stream-select-join-environments
       (lambda (visit-left) (declare (ignore visit-left)) (values))
       (lambda (visit-right)
         (incf right-opened)
         (funcall visit-right #(9)))
       (list :type :cross :qual '(:bool t)
             :left-width 1 :right-width 1)
       (lambda (row) (declare (ignore row)) (incf emitted)))
      (rove:ok (zerop right-opened)
               "empty outer input never opens the inner stream")
      (rove:ok (zerop emitted)
               "empty CROSS JOIN emits no rows")))
