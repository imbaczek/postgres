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

(rove:deftest joins-preserve-source-presence-in-memory-and-on-disk
    (dolist (memory '(1 100000))
      (let ((*query-work-memory-bytes* memory)
            (*query-temp-directory* (uiop:temporary-directory)))
        (dolist (type '(:inner :left :right :full))
          (dolist (match-p '(nil t))
            (with-query-temp-files ()
                                   (let* ((join (list :type type :qual (list :bool match-p)
                                                      :left-width 1 :right-width 1))
                                          (expected (if match-p (list #(11 12))
                                                        (case type
                                                          (:inner nil) (:left (list #(11)))
                                                          (:right (list #(12))) (:full (list #(11) #(12))))))
                                          (callback-presence nil)
                                          (iterator-presence nil))
                                     (flet ((left-stream (callback)
                                              (let ((*query-row-presence* #(11)) (*query-row-origins* nil))
                                                (funcall callback (vector +null+))))
                                            (right-stream (callback)
                                              (let ((*query-row-presence* #(12)) (*query-row-origins* nil))
                                                (funcall callback (vector +null+))))
                                            (input (identity)
                                              (let ((pending t))
                                                (make-sql-iterator
                                                 (lambda ()
                                                   (when pending
                                                     (setf pending nil)
                                                     (values (vector +null+) t nil nil (vector identity))))))))
                                       (stream-select-join-environments
                                        #'left-stream #'right-stream join
                                        (lambda (row)
                                          (rove:ok (every (lambda (value) (eq value +null+)) row))
                                          (push (copy-seq *query-row-presence*) callback-presence)
                                          nil))
                                       (let ((iterator (make-nested-loop-iterator join (input 11) (input 12))))
                                         (unwind-protect
                                              (map-sql-iterator iterator
                                                                (lambda (row)
                                                                  (declare (ignore row))
                                                                  (push (copy-seq *query-row-presence*) iterator-presence)))
                                           (close-sql-iterator iterator))))
                                     (rove:ok (equalp (nreverse callback-presence) expected))
                                     (rove:ok (equalp (nreverse iterator-presence) expected)))))))))
