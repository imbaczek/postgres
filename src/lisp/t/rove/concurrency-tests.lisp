;;;; concurrency-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-transaction-ending-statement-classifier
    (dolist (query '("COMMIT" "END" "ROLLBACK" "ABORT"))
      (rove:ok (some #'transaction-ending-statement-p (parse-statements query))
               (format nil "transaction-ending AST recognized: ~a" query)))
  (let* ((statements (parse-statements "; ; SELECT 1; COMMIT; SELECT 2"))
         (ending-position (position-if #'transaction-ending-statement-p
                                       statements)))
    (rove:ok (= ending-position 1)
             "AST classifier finds a terminal after a nonterminal prefix")
    (rove:ok (= (length statements) 3)
             "the full simple-query message is parsed before dispatch"))
  (rove:ok (simple-query-payload-starts-with-transaction-end-p
            (frontend-message-payload
             (test-query-message "COMMIT; CHECKPOINT")))
           "a transaction-ending first command may enter drain admission")
  (dolist (query (list "/* leading comment */ COMMIT"
                       (format nil "-- leading comment~%ROLLBACK")
                       "; ; COMMIT"))
    (rove:ok (simple-query-payload-starts-with-transaction-end-p
              (frontend-message-payload (test-query-message query)))
             (format nil "SQL trivia before a transaction end is ignored: ~a"
                     query)))
  (rove:ok (not (simple-query-payload-starts-with-transaction-end-p
                 (frontend-message-payload
                  (test-query-message "SELECT 1; COMMIT"))))
           "work before COMMIT cannot enter drain admission")
  (rove:ok (not (simple-query-payload-starts-with-transaction-end-p
                 (frontend-message-payload
                  (test-query-message "SELECT 'COMMIT'"))))
           "a transaction keyword inside a string cannot enter drain admission")
  (rove:ok (handler-case
               (progn (parse-statements "COMMIT; SELECT") nil)
             (pg-error () t)
             (error () t))
           "a syntax error after COMMIT is detected while parsing the full message"))
