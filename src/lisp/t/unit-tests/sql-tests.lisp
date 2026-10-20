;;;; sql-tests.lisp -- SQL evaluation and error cleanup tests.

(in-package #:pg)

(defun test-sql-strict-let-evaluation ()
  (let ((evaluations '()))
    (check (eq +null+
               (sql-strict-let ((left (progn (push :left evaluations) +null+))
                                (right (progn (push :right evaluations) 7)))
                 :body-must-not-run))
           "strict let returns NULL when any binding is NULL")
    (check (equal (nreverse evaluations) '(:left :right))
           "strict let evaluates every binding once before NULL check"))
  (let ((evaluations 0))
    (check (= 9
              (sql-strict-let ((left (progn (incf evaluations) 4))
                               (right (progn (incf evaluations) 5)))
                (+ left right)))
           "strict let returns body result for non-NULL bindings")
    (check (= evaluations 2) "strict let bindings run once each")))

(defun test-using-join-output-metadata ()
  (let* ((left (make-select-column
                :name "k" :expr '(:col 1 :type :varchar :typmod 9
                                  :table-oid 11 :attnum 1)
                :type :varchar :typmod 9 :table-oid 11 :attnum 1))
         (right (make-select-column
                 :name "k" :expr '(:col 1 :type :varchar :typmod 24
                                   :table-oid 22 :attnum 1)
                 :type :varchar :typmod 24 :table-oid 22 :attnum 1))
         (pair (list :left left :right right :type :varchar
                     :typmod (common-using-typmod left right :varchar)))
         (right-output (using-output-column "k" pair :right))
         (full-output (using-output-column "k" pair :full))
         (right-explicit-target
           (analyze-target (list :expr '(:column "k"))
                           (make-select-scope :columns (list right-output))))
         (same-right
           (make-select-column
            :name "k" :expr '(:col 1 :type :varchar :typmod 24
                              :table-oid 22 :attnum 1)
            :type :varchar :typmod 24 :table-oid 22 :attnum 1))
         (same-pair (list :left same-right :right same-right :type :varchar
                          :typmod (common-using-typmod same-right same-right
                                                       :varchar)))
         (same-right-output (using-output-column "k" same-pair :right))
         (same-explicit-target
           (analyze-target (list :expr '(:column "k"))
                           (make-select-scope
                            :columns (list same-right-output))))
         (right-result (target->result-column
                        (select-column-target right-output)))
         (right-explicit-result (target->result-column right-explicit-target))
         (full-result (target->result-column
                       (select-column-target full-output))))
    (check (= -1 (select-column-typmod right-output))
           "RIGHT USING reports the common typmod for differing inputs")
    (check (= -1 (result-column-typmod right-result))
           "RIGHT USING protocol metadata carries the common typmod")
    (check (= -1 (result-column-typmod right-explicit-result))
           "explicit RIGHT USING target carries the common typmod")
    (check (= 0 (getf right-explicit-target :table-oid))
           "relabelled RIGHT USING target has no base-column origin")
    (check (= 22 (getf same-explicit-target :table-oid))
           "unrelabelled RIGHT USING target retains its base-column origin")
    (check (= -1 (result-column-typmod full-result))
           "FULL USING protocol metadata carries the common typmod")
    (let* ((left-int (make-select-column
                      :name "n" :expr '(:col 1 :type :int4 :typmod -1
                                        :table-oid 31 :attnum 1)
                      :type :int4 :typmod -1 :table-oid 31 :attnum 1))
           (right-int (make-select-column
                       :name "n" :expr '(:col 1 :type :int8 :typmod -1
                                         :table-oid 32 :attnum 1)
                       :type :int8 :typmod -1 :table-oid 32 :attnum 1))
           (int-pair (list :left left-int :right right-int
                           :type (common-using-type :int4 :int8 "n")
                           :typmod -1))
           (right-int-output (using-output-column "n" int-pair :right))
           (explicit-int-target
             (analyze-target (list :expr '(:column "n"))
                             (make-select-scope
                              :columns (list right-int-output))))
           (int-result (target->result-column explicit-int-target)))
      (check (eq :int8 (getf explicit-int-target :type))
             "explicit USING target is promoted to the common integer type")
      (check (= +int8oid+ (result-column-type-oid int-result))
             "promoted USING protocol metadata reports bigint"))))

(defun test-error-cleanup-and-fatal-rethrow ()
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (cleanup-count 0)
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (incf cleanup-count))
          (error fatal))
      (pg-error (condition)
        (setf caught condition)))
    (check (= cleanup-count 1) "error cleanup runs before rethrow")
    (check (eq caught fatal) "error cleanup re-signals the original condition"))
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (handler-ran nil)
         (caught nil))
    (handler-case
        (handler-case-nonfatal
         (error fatal)
         (condition
          (setf handler-ran (typep condition 'error))))
      (pg-error (condition)
        (setf caught condition)))
    (check (not handler-ran) "nonfatal handler skips FATAL errors")
    (check (eq caught fatal) "nonfatal handler re-signals the original FATAL"))
  (check (eq :handled
             (handler-case-nonfatal
              (error "test nonfatal")
              (condition
               (if (typep condition 'error) :handled :unexpected))))
         "nonfatal handler handles ordinary errors"))

(defun test-create-table-cleanup-after-fsync-error ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (db (make-database :name "postgres" :oid 1))
         (original-fsync-index (symbol-function 'fsync-index))
         (caught nil))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db)
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-index)
                 (lambda (index)
                   (declare (ignore index))
                   (error "injected final index fsync failure")))
           (handler-case
               (exec-create-table
                nil
                '(:name "cleanup_test"
                  :columns ((:name "value" :type :int4 :typmod -1 :unique t))))
             (error (condition)
               (setf caught condition)))
           (check (typep caught 'error)
                  "CREATE TABLE re-signals final index fsync failure")
           (check (null (gethash "cleanup_test" (database-tables db)))
                  "CREATE TABLE cleanup removes table after final fsync failure"))
      (setf (symbol-function 'fsync-index) original-fsync-index))))

(defun test-relation-build-own-xid ()
  (let ((relfilenode 987653)
        (xid 987654))
    (setf (gethash relfilenode *relation-writers*) (list xid))
    (let ((token (acquire-relation-build-lock relfilenode xid)))
      (check (eq token (gethash relfilenode *relation-builders*))
             "relation build ignores its own writer XID")
      (release-relation-build-lock relfilenode token))
    (remhash relfilenode *relation-writers*)))

(defun test-parse-error-aborts-transaction ()
  (let ((path (merge-pathnames "parse-error.out" *pgdata*)))
    (setf (backend-state *backend*) :in-progress
          (backend-explicit *backend*) t
          (backend-command-id *backend*) 0)
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select from;"))
    (check (eq (backend-state *backend*) :aborted)
           "a parse error aborts an explicit transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select 1;"))
    (check (eq (backend-state *backend*) :aborted)
           "aborted transaction stays aborted")
    (abort-transaction)
    (check (eq (backend-state *backend*) :idle)
           "abort clears the transaction state")))
