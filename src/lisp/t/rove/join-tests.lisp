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

(rove:deftest empty-left-join-streams-unmatched-right-without-spilling
    (dolist (type '(:right :full))
      (let ((*query-work-memory-bytes* 1)
            (*query-temp-file-limit-bytes* 0)
            (*datum-origin-capture* t)
            (origin (make-datum-origin :compressed (make-heap-compressed-varlena :payload #(1 2))))
            (*query-temp-directory* (uiop:temporary-directory))
            (join (list :type type :qual '(:bool nil) :left-width 1 :right-width 1))
            (right-next 0)
            (closed 0)
            (pushed nil))
        (stream-select-join-environments
         (lambda (emit) (declare (ignore emit)))
         (lambda (emit) (funcall emit #(5)) (funcall emit #(6)))
         join (lambda (row) (push (aref row 1) pushed)))
        (rove:ok (equal (nreverse pushed) '(5 6)))
        (with-query-temp-files ()
                               (let* ((left (make-sql-iterator (lambda () (values nil nil))))
                                      (right (make-sql-iterator
                                              (lambda ()
                                                (incf right-next)
                                                (when (<= right-next 2)
                                                  (values (vector (+ 4 right-next)) t (vector origin) nil #(12))))
                                              (lambda () (incf closed))))
                                      (iterator (make-nested-loop-iterator join left right)))
                                 (unwind-protect
                                      (loop for expected in '(5 6)
                                            do (multiple-value-bind (row present origins toast presence)
                                                   (sql-iterator-next iterator)
                                                 (declare (ignore toast))
                                                 (rove:ok present)
                                                 (rove:ok (eq (aref row 0) +null+))
                                                 (rove:ok (= (aref row 1) expected))
                                                 (rove:ok (equalp presence #(12)))
                                                 (rove:ok (null (aref origins 0)))
                                                 (rove:ok (eq (aref origins 1) origin))
                                                 (rove:ok (= right-next (- expected 4))))
                                            finally (when (eq type :full)
                                                      (rove:ok (not (nth-value 1 (sql-iterator-next iterator))))
                                                      (rove:ok (= closed 1))
                                                      (rove:ok (not (nth-value 1 (sql-iterator-next iterator))))
                                                      (rove:ok (= right-next 3))))
                                   (close-sql-iterator iterator))
                                 (rove:ok (= closed 1)))))))

(rove:deftest nested-loop-drivers-reserve-work-memory-for-right-matches
    (let* ((right-row (vector (make-string 128 :initial-element #\x)))
           ;; The pull replay index costs sixteen bytes per buffered row.
           (*query-work-memory-bytes* (+ 16 (query-spill-record-size (list right-row nil nil))))
           (*query-temp-file-limit-bytes* 0)
           (*query-temp-directory* (uiop:temporary-directory))
           (*query-row-presence* nil)
           (*datum-origin-capture* nil))
      (dolist (type '(:inner :full))
        (let ((join (list :type type :qual '(:bool t) :left-width 1 :right-width 1))
              (expected (if (eq type :full) "53400" :row)))
          (flet ((outcome (operation)
                   (handler-case (funcall operation)
                     (pg-error (error) (pg-error-sqlstate error))))
                 (single-row (row)
                   (let ((pending t))
                     (make-sql-iterator
                      (lambda ()
                        (when pending
                          (setf pending nil)
                          (values row t)))))))
            (rove:ok
             (equal expected
                    (outcome (lambda ()
                               (block pushed
                                 (stream-select-join-environments
                                  (lambda (emit) (funcall emit #(1)))
                                  (lambda (emit) (funcall emit right-row))
                                  join (lambda (row) (declare (ignore row)) (return-from pushed :row))))))))
            (with-query-temp-files ()
                                   (let ((iterator (make-nested-loop-iterator join (single-row #(1)) (single-row right-row))))
                                     (unwind-protect
                                          (rove:ok
                                           (equal expected (outcome (lambda ()
                                                                      (when (nth-value 1 (sql-iterator-next iterator))
                                                                        :row)))))
                                       (close-sql-iterator iterator)))))))))
