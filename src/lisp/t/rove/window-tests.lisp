;;;; window-tests.lisp -- ranking, frames and query-level window analysis.
(in-package #:pg)

(cl-spec:defspec-function window-rank-step
    (:args (row-position (range integer 1 10000))
           (prior-rank (range integer 1 10000))
           (prior-dense (range integer 1 10000))
           (peer boolean))
  (:returns list)
  (:post (equal result (if peer (list prior-rank prior-dense)
                           (list row-position (1+ prior-dense))))))

(cl-spec:defproperty window-ranking-peer-sequence
    ((bits (range integer 0 1048575)) (size (range integer 1 20)))
  (:about window-rank-step)
  (:trials (:normal 300))
  (let ((rank 1) (dense 1) (groups 1) (start 1))
    (loop for position from 2 to size
          for peer = (logbitp (- position 2) bits)
          do (unless peer (incf groups) (setf start position))
             (destructuring-bind (next-rank next-dense)
                 (window-rank-step position rank dense peer)
               (setf rank next-rank dense next-dense))
          always (and (= rank start) (= dense groups)))))

(rove:deftest window-ranking-contracts
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'window-rank-step :trials 300 :seed 42))))
      (rove:ok (eq (getf data :status) :passed) (format nil "~s" data)))
  (unit-check-property 'window-ranking-peer-sequence))

(rove:deftest window-ranking-and-frames
    (dolist (case
                '(("SELECT row_number() OVER (),rank() OVER (),dense_rank() OVER ()" ((1 1 1)))
                  ("SELECT d.x,rank() OVER w,dense_rank() OVER w,sum(d.x) OVER w FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) d WINDOW w AS (ORDER BY d.x) ORDER BY d.x"
                   ((1 1 1 1) (2 2 2 5) (2 2 2 5)))
                  ("SELECT d.x,sum(d.x) OVER (ORDER BY d.x ROWS UNBOUNDED PRECEDING) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) d ORDER BY d.x" ((1 1) (2 3) (3 6)))
                  ("SELECT d.x,sum(d.x) OVER (ORDER BY d.x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (SELECT 1 AS x UNION ALL SELECT 2) d ORDER BY d.x" ((1 3) (2 3)))
                  ("SELECT sum(sum(d.x)) OVER () FROM (SELECT 1 AS x UNION ALL SELECT 2) d GROUP BY d.x" ((3) (3)))
                  ("SELECT row_number() OVER () WHERE false" ())
                  ("SELECT DISTINCT rank() OVER () FROM (SELECT 1 UNION ALL SELECT 2) d" ((1)))
                  ("SELECT (SELECT row_number() OVER ()+d.x) FROM (SELECT 4 AS x) d" ((5)))
                  ("SELECT count(*) OVER (),row_number() OVER () FROM (SELECT count(*) FROM (SELECT 1 WHERE false) q) d" ((1 1)))))
      (rove:ok (equal (second case) (subquery-test-rows (first case))) (first case))))

(rove:deftest window-invalid-contexts
    (dolist (sql '("SELECT 1 WHERE row_number() OVER ()=1"
                   "SELECT sum(row_number() OVER ())"
                   "SELECT sum(rank() OVER ()) OVER ()"
                   "SELECT row_number() OVER (ORDER BY rank() OVER ())"
                   "SELECT 1 GROUP BY row_number() OVER ()"
                   "SELECT rank() OVER (w) WINDOW w AS (ROWS UNBOUNDED PRECEDING)"
                   "SELECT 1 WINDOW w AS (),w AS ()"))
      (rove:ok (handler-case (progn (prepare-select (first (parse-statements sql))) nil)
                 (pg-error (condition) (string= (pg-error-sqlstate condition)
                                                (if (equal sql "SELECT sum(row_number() OVER ())") "42803" "42P20")))) sql)))

(rove:deftest window-array-results-are-owned
    (let ((rows (subquery-test-rows
                 "SELECT array_agg(d.x) OVER (ORDER BY d.x ROWS UNBOUNDED PRECEDING) FROM (SELECT 'a' AS x UNION ALL SELECT 'b' UNION ALL SELECT 'c') d ORDER BY d.x")))
      (rove:ok (equalp rows '((#("a")) (#("a" "b")) (#("a" "b" "c")))))
      (setf (aref (caar (last rows)) 0) "changed")
      (rove:ok (equalp (caar rows) #("a")))))

(rove:deftest window-streaming-limit
    (dolist (frame '("ROWS UNBOUNDED PRECEDING" ""))
      (let* ((visited 0)
             (descriptor (list :kind (if (equal frame "") :row-number :sum)
                               :input '(:const 7 :type :int4 :typmod -1) :type :int8 :slot 0))
             (stage (list :partition nil :order nil :frame '(:rows :current)
                          :width 1 :functions (list descriptor))))
        (stream-window-stage
         (lambda (callback)
           (loop repeat 20
                 do (incf visited)
                 when (eq (funcall callback #()) :stop) do (return :stop)))
         stage (lambda (row) (declare (ignore row)) :stop))
        (rove:ok (= visited 1) "a prefix/ranking consumer stops its source"))))

(rove:deftest window-spilled-array-prefix
    (let* ((directory (merge-pathnames
                       (format nil "pg-window-spill-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-work-memory-bytes* 128))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (rove:ok
              (equalp (subquery-test-rows
                       "SELECT array_agg(d.x) OVER (ORDER BY d.x) FROM (SELECT 'b' AS x UNION ALL SELECT 'a' UNION ALL SELECT 'b') d ORDER BY d.x")
                      '((#("a")) (#("a" "b" "b")) (#("a" "b" "b")))))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty window-generated-prefix-totals
    ((size (range integer 1 12)) (delta (range integer -10 10)) (range-p boolean))
  (:about stream-window-stage)
  (:trials (:normal 150))
  (let* ((source (format nil "~{~a~^ UNION ALL ~}"
                         (loop for index from 1 to size
                               collect (format nil "SELECT ~d AS id,~d AS v" index (* delta index)))))
         (sql (format nil "SELECT d.id,sum(d.v) OVER (ORDER BY d.id ~a UNBOUNDED PRECEDING),row_number() OVER(ORDER BY d.id) FROM (~a) d ORDER BY d.id"
                      (if range-p "RANGE" "ROWS") source))
         (expected (loop for index from 1 to size
                         collect (list index (* delta (/ (* index (1+ index)) 2)) index))))
    (equal (subquery-test-rows sql) expected)))

(rove:deftest window-generated-prefix-totals-check
    (unit-check-property 'window-generated-prefix-totals))

(rove:deftest window-key-sharing-and-probe-cleanup
    (let* ((prepared (prepare-select (first (parse-statements
                                             "SELECT sum(nextval('unused')::int4),rank() OVER(ORDER BY sum(nextval('unused')::int4))")))))
      (rove:ok (= 1 (getf prepared :aggregate-count))))
  (let* ((prepared (prepare-select (first (parse-statements
                                           "SELECT DISTINCT sum(nextval('unused')) OVER(ORDER BY 1) ORDER BY sum(nextval('unused')) OVER(ORDER BY 1)")))))
    (rove:ok (= 1 (length (getf prepared :windows)))))
  (let ((plan (plan-select-query (first (parse-statements "SELECT rank() OVER(ORDER BY 1)")))))
    (rove:ok (search "WindowAgg" (format-query-plan plan)))))

(rove:deftest window-unsupported-features-are-explicit
    (dolist (sql '("SELECT sum(DISTINCT 1) OVER()" "SELECT sum(1) FILTER(WHERE true) OVER()"
                   "SELECT lag(1) OVER()" "SELECT sum(1) OVER(ROWS 1 PRECEDING)"
                   "SELECT rank() OVER(GROUPS UNBOUNDED PRECEDING)"
                   "SELECT rank() OVER(ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW)"))
      (rove:ok (handler-case (progn (prepare-select (first (parse-statements sql))) nil)
                 (pg-error (condition) (string= (pg-error-sqlstate condition) "0A000"))) sql)))
