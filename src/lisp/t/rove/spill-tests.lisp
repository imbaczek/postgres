;;;; spill-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-hash-aggregate-skew-does-not-spill-unnecessarily
    (let ((actual nil))
      (let ((*query-work-memory-bytes* 1))
        (stream-hash-aggregate
         (lambda (callback)
           (dotimes (index 100) (funcall callback #(7))))
         '((:col 1 :type :int4)) 1
         (lambda (row) (push (list (aref row 0) (aref row 1)) actual))))
      (rove:ok (equal actual '((7 100)))
               "a single heavily skewed group stays in one bounded hash state")))

(rove:deftest test-query-temp-fd-scope-macro-expansion-protects-cleanup
    (let* ((expansion (macroexpand-1 '(with-query-temp-fd (fd nil) :body)))
           (scope-form (and (consp expansion) (third expansion)))
           (unwind-form
             (and (consp scope-form)
                  (eq (first scope-form) 'sb-sys:without-interrupts)
                  (second scope-form)))
           (protected-form
             (and (consp unwind-form)
                  (eq (first unwind-form) 'unwind-protect)
                  (second unwind-form)))
           (cleanup-form (and (consp unwind-form) (third unwind-form))))
      (rove:ok (and (consp expansion)
                    (eq (first expansion) 'let)
                    unwind-form)
               "spill fd scope installs the interrupt mask around unwind-protect")
      (rove:ok (and (consp protected-form)
                    (eq (first protected-form) 'progn)
                    (eq (first (second protected-form)) 'setf)
                    (eq (first (third protected-form)) 'sb-sys:with-local-interrupts))
               "spill fd scope binds acquisition before enabling body interrupts")
      (rove:ok (and (consp cleanup-form)
                    (eq (first cleanup-form) 'when)
                    (eq (first (third cleanup-form)) 'query-temp-close-fd))
               "spill fd cleanup stays inside the interrupt-masked unwind-protect")))

(rove:deftest test-query-spill-macros-use-default-memory-limit
    (let ((*query-work-memory-bytes* 37)
          (row-limit-evaluations 0)
          (match-limit-evaluations 0))
      (with-query-row-spool (spool)
        (rove:ok (= (query-row-spool-memory-limit spool) 37)
                 "row-spool macro defaults to the configured work-memory limit"))
      (with-query-match-map (match-map)
        (rove:ok (= (query-match-map-memory-limit match-map) 37)
                 "match-map macro defaults to the configured work-memory limit"))
      (with-query-row-spool (spool (incf row-limit-evaluations))
        (rove:ok (= (query-row-spool-memory-limit spool) 1)
                 "row-spool macro passes an explicit memory limit"))
      (with-query-match-map (match-map (incf match-limit-evaluations))
        (rove:ok (= (query-match-map-memory-limit match-map) 1)
                 "match-map macro passes an explicit memory limit"))
      (rove:ok (= row-limit-evaluations 1)
               "row-spool macro evaluates its memory limit once")
      (rove:ok (= match-limit-evaluations 1)
               "match-map macro evaluates its memory limit once")))

(rove:deftest test-query-temp-file-limit-option-parsing
    (multiple-value-bind (host port pgdata control-port control-socket
                          swank-port swank-address limit)
        (parse-args '("--temp-file-limit-bytes" "4096"))
      (declare (ignore host port pgdata control-port control-socket
                       swank-port swank-address))
      (rove:ok (= limit 4096)
               "server option parses a finite temporary file limit"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-address limit)
      (parse-args '("--temp-file-limit-bytes=-1"))
    (declare (ignore host port pgdata control-port control-socket
                     swank-port swank-address))
    (rove:ok (null limit)
             "server option accepts -1 for unlimited temporary disk space")))
