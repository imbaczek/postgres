;;;; join-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-lazy-join-skips-inner-for-empty-outer
    (let ((right-opened 0)
          (emitted 0))
      (let ((iterator
              (make-nested-loop-iterator
               (list :type :cross :qual '(:bool t) :left-width 1 :right-width 1)
               (make-sql-iterator (lambda () (values nil nil)))
               (make-sql-iterator (lambda () (incf right-opened) (values #(9) t))))))
        (unwind-protect
             (map-sql-iterator iterator (lambda (row) (declare (ignore row)) (incf emitted)))
          (close-sql-iterator iterator)))
      (rove:ok (zerop right-opened)
               "empty outer input never opens the inner stream")
      (rove:ok (zerop emitted)
               "empty CROSS JOIN emits no rows")))

(rove:deftest hash-join-preserves-real-origins-and-presence-across-repartition
    (let ((*datum-origin-capture* t)
          (*query-temp-directory* (uiop:temporary-directory)))
      (flet ((input (source start)
               (let ((key start)
                     (presence (vector source)))
                 (make-sql-iterator
                  (lambda ()
                    (when (< key (+ start 40))
                      (let* ((item (prog1 key (incf key)))
                             (compression (make-heap-compressed-varlena :payload (vector source item)))
                             (origin (make-datum-origin :compressed compression)))
                        (values (vector item) t (vector origin) nil presence)))))))
             (payload (origin)
               (and origin (heap-compressed-varlena-payload (datum-origin-compressed origin)))))
        (dolist (build-left-p '(nil t))
          (dolist (budget '(512 65536))
            (with-query-temp-files
              (let* ((left (input 11 0))
                     (right (input 12 1))
                     (seen (make-array 41 :initial-element 0))
                     (stats (make-hash-join-stats))
                     (join (make-hash-join-iterator
                            (if build-left-p right left) (if build-left-p left right)
                            #(0) #(0) #(:int4) :build-left-p build-left-p
                            :preserve-probe-width 1 :preserve-build-width 1 :memory-limit budget :stats stats)))
                (unwind-protect
                     (map-sql-iterator
                      join
                      (lambda (row)
                        (let* ((lhs (aref row 0)) (rhs (aref row 1))
                                                  (key (if (eq lhs +null+) rhs lhs))
                                                  (left-p (< key 40))
                                                  (right-p (plusp key))
                                                  (origins *query-row-origins*))
                          (incf (aref seen key))
                          (rove:ok (eql lhs (if left-p key +null+)))
                          (rove:ok (eql rhs (if right-p key +null+)))
                          (rove:ok (equalp (payload (aref origins 0)) (when left-p (vector 11 key))))
                          (rove:ok (equalp (payload (aref origins 1)) (when right-p (vector 12 key))))
                          (rove:ok (equalp *query-row-presence*
                                           (cond ((and left-p right-p) #(11 12))
                                                 (left-p #(11)) (t #(12))))))))
                  (close-sql-iterator join))
                (rove:ok (every (lambda (count) (= count 1)) seen))
                (rove:ok (if (= budget 512)
                             (> (hash-join-stats-spill-batches stats) 8)
                             (zerop (hash-join-stats-spill-batches stats)))))))))))

(rove:deftest joins-preserve-source-presence-in-memory-and-on-disk
    (dolist (memory '(1 100000))
      (let ((*query-work-memory-bytes* memory)
            (*datum-origin-capture* t)
            (*query-temp-directory* (uiop:temporary-directory)))
        (dolist (type '(:inner :left :right :full))
          (dolist (match-p '(nil t))
            (with-query-temp-files ()
                                   (let ((join (list :type type :qual (list :bool match-p) :left-width 1 :right-width 1))
                                         (expected (if match-p (list #(11 12))
                                                       (case type
                                                         (:inner nil) (:left (list #(11)))
                                                         (:right (list #(12))) (:full (list #(11) #(12))))))
                                         (seen nil))
                                     (flet ((input (identity)
                                              (let ((pending t))
                                                (make-sql-iterator
                                                 (lambda ()
                                                   (when pending
                                                     (setf pending nil)
                                                     (values (vector +null+) t nil nil (vector identity))))))))
                                       (let ((iterator (make-nested-loop-iterator join (input 11) (input 12))))
                                         (unwind-protect
                                              (map-sql-iterator iterator
                                                                (lambda (row)
                                                                  (rove:ok (every (lambda (value) (eq value +null+)) row))
                                                                  (rove:ok (null *query-row-origins*))
                                                                  (push (copy-seq *query-row-presence*) seen)))
                                           (close-sql-iterator iterator))))
                                     (rove:ok (equalp (nreverse seen) expected)))))))))


(rove:deftest empty-left-join-streams-unmatched-right-without-spilling
    (dolist (type '(:right :full))
      (let ((*query-work-memory-bytes* 1)
            (*query-temp-file-limit-bytes* 0)
            (*datum-origin-capture* t)
            (origin (make-datum-origin :compressed (make-heap-compressed-varlena :payload #(1 2))))
            (*query-temp-directory* (uiop:temporary-directory))
            (join (list :type type :qual '(:bool nil) :left-width 1 :right-width 1))
            (right-next 0)
            (closed 0))

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

            (with-query-temp-files ()
                                   (let ((iterator (make-nested-loop-iterator join (single-row #(1)) (single-row right-row))))
                                     (unwind-protect
                                          (rove:ok
                                           (equal expected (outcome (lambda ()
                                                                      (when (nth-value 1 (sql-iterator-next iterator))
                                                                        :row)))))
                                       (close-sql-iterator iterator)))))))))
