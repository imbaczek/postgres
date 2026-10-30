;;;; RETURNING grammar and namespace regressions.
(in-package #:pg)

(rove:deftest returning-parser-targets
    (let ((delete (first (parse-statements
                          "DELETE FROM things AS d WHERE d.id=1 RETURNING d.*,d.id+1 AS next_id")))
          (update (first (parse-statements
                          "UPDATE things AS d SET id=id+1 RETURNING WITH(OLD AS o,NEW AS n) o.id,n.id"))))
      (rove:ok (equal (getf delete :alias) "d"))
      (rove:ok (equal (getf (first (getf delete :returning)) :qualified-star) "d"))
      (rove:ok (= 2 (length (getf delete :returning))))
      (rove:ok (equal (getf update :returning-options) '(:old "o" :new "n")))))

(rove:deftest returning-qualified-star-expansion
    (rove:ok (equal '((7 8))
                    (subquery-test-rows "SELECT d.* FROM(SELECT 7 AS a,8 AS b) d")))
  (rove:ok (equal '((7))
                  (subquery-test-rows "SELECT (SELECT d.*) FROM(SELECT 7 AS a) d"))))

(defun returning-test-table ()
  (make-pg-class :oid 1234 :name "things"
                 :columns (vector (make-pg-column :name "id" :attnum 1 :type :int4 :typmod -1)
                                  (make-pg-column :name "label" :attnum 2 :type :varchar :typmod 16))))

(defun returning-test-targets (sql)
  (let* ((*select-outer-scope* nil)
         (*select-analysis-frame*
           (list :parent nil :descriptors (make-array 0 :adjustable t :fill-pointer 0)
                 :group-keys nil :kind :dml :inside-input nil))
         (*select-aggregate-analysis* *select-analysis-frame*)
         (*select-sublinks-allowed-p* t)
         (*select-sublinks* nil)
         (*select-set-functions-allowed-p* nil))
    (prepare-dml-returning (first (parse-statements sql)) (returning-test-table))))

(rove:deftest returning-current-old-new-namespaces
    (let* ((targets (returning-test-targets
                     "DELETE FROM things AS d RETURNING *,d.id,old.id,new.id"))
           (environment (vector 7 "current" 7 "current" +null+ +null+)))
      (rove:ok (= 5 (length targets)))
      (rove:ok (equal '(7 "current" 7 7 :null)
                      (mapcar (lambda (target) (let ((value (eval-expr (getf target :expr) environment)))
                                                 (if (eq value +null+) :null value))) targets)))
      (rove:ok (= 1234 (getf (first targets) :table-oid)))
      (rove:ok (= 1 (getf (first targets) :attnum))))
  (let* ((targets (returning-test-targets
                   "UPDATE things AS d SET id=1 RETURNING WITH(OLD AS o,NEW AS n) o.id,n.id,d.id"))
         (environment (vector 9 "new" 7 "old" 9 "new")))
    (rove:ok (equal '(7 9 9) (mapcar (lambda (target) (eval-expr (getf target :expr) environment)) targets)))))

(rove:deftest returning-analysis-rejections
    (dolist (case '(("DELETE FROM things RETURNING missing" "42703")
                    ("DELETE FROM things RETURNING old" "0A000")
                    ("DELETE FROM things RETURNING things" "0A000")
                    ("DELETE FROM things RETURNING (new).id" "0A000")
                    ("DELETE FROM things AS d RETURNING things.*" "42P01")
                    ("DELETE FROM things AS d RETURNING WITH(OLD AS d) d.id" "42712")
                    ("DELETE FROM things RETURNING sum(id)" "42803")
                    ("DELETE FROM things RETURNING row_number() OVER()" "42P20")
                    ("DELETE FROM things RETURNING pg_get_publication_tables('p')" "0A000")))
      (let ((caught nil))
        (handler-case (returning-test-targets (first case))
          (pg-error (condition) (setf caught (pg-error-sqlstate condition))))
        (rove:ok (equal caught (second case)) (first case)))))

(defun call-with-returning-portal-test (function &key
                                                   (rows (list #(7 "label") #(8 "second")))
                                                   after-finish)
  (call-with-select-portal-test
   (lambda (stream)
     (let* ((targets (returning-test-targets "UPDATE things SET id=id RETURNING id,label"))
            (prepared (list :returning targets))
            (calls (list 0))
            (names '(prepare-dml-subqueries register-dml-subquery-relations
                     get-current-reader-xid exec-insert exec-update exec-delete))
            (originals (mapcar #'symbol-function names)))
       (unwind-protect
            (flet ((execute (output ast)
                     (rove:ok (eq prepared (cdr *dml-bound-preparation*)))
                     (incf (car calls))
                     (call-with-dml-returning-spool
                      targets
                      (lambda ()
                        (dolist (row rows)
                          (query-row-spool-append *dml-returning-spool* (copy-query-sort-value row)))
                        (finish-dml-command
                         output targets
                         (format nil "~a ~d"
                                 (if (eq (getf ast :type) :insert) "INSERT 0"
                                     (string-upcase (symbol-name (getf ast :type))))
                                 (length rows)))
                        (when after-finish (funcall after-finish))))))
              (setf (symbol-function 'prepare-dml-subqueries)
                    (lambda (ast) (declare (ignore ast)) prepared)
                    (symbol-function 'register-dml-subquery-relations)
                    (lambda (value &optional xid)
                      (declare (ignore xid))
                      (rove:ok (eq value prepared))
                      (values))
                    (symbol-function 'get-current-reader-xid) (lambda () 100)
                    (symbol-function 'exec-insert) #'execute
                    (symbol-function 'exec-update) #'execute
                    (symbol-function 'exec-delete) #'execute)
              (funcall function stream calls))
         (loop for name in names for original in originals
               do (setf (symbol-function name) original)))))))

(rove:deftest returning-extended-metadata-and-formats
    (call-with-returning-portal-test
     (lambda (out calls)
       (extended-apply-operation out '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
       (extended-apply-operation out '(:describe #\S "s") #())
       (extended-apply-operation out '(:bind "p" "s" () () (0 1)) #())
       (extended-apply-operation out '(:describe #\P "p") #())
       (extended-apply-operation out '(:execute "p" 0) #())
       (rove:ok (= 1 (car calls)))
       (let ((frames (select-portal-test-frames out)))
         (rove:ok (equal (mapcar #'car frames) '(#\1 #\t #\T #\2 #\T #\D #\C)))
         (dolist (entry (list (cons (third frames) '(0 0))
                              (cons (fifth frames) '(0 1))))
           (let ((columns
                   (let ((payload (cdar entry)) (position 2))
                     (loop repeat (bytes-to-u16 payload 0)
                           collect
                           (multiple-value-bind (name next)
                               (extended-read-cstring payload position)
                             (setf position (+ next 18))
                             (make-pg-client-column-with-format
                              :name name
                              :type-oid (bytes-to-u32 payload (+ next 6))
                              :format (bytes-to-u16 payload (+ next 16))))))))
             (rove:ok (equal (mapcar #'pg-client-column-name columns) '("id" "label")))
             (rove:ok (equal (mapcar #'pg-client-column-type-oid columns) '(23 1043)))
             (rove:ok (equal (mapcar #'pg-client-column-format columns) (cdr entry)))))
         (rove:ok (equalp (cdr (sixth frames))
                          #(0 2 0 0 0 1 55 0 0 0 5 108 97 98 101 108)))))
     :rows (list #(7 "label"))))

(rove:deftest returning-portal-exact-limit-and-per-fetch-tags
    (dolist (case '(("INSERT INTO things(id) VALUES(1),(2) RETURNING id,label" "INSERT 0")
                    ("UPDATE things SET id=id RETURNING id,label" "UPDATE")
                    ("DELETE FROM things RETURNING id,label" "DELETE")))
      (call-with-returning-portal-test
       (lambda (stream calls)
         (extended-apply-operation stream (list :parse "s" (first case) nil) #())
         (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
         (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                (execution (extended-dml-execution-for portal))
                (owner (extended-dml-execution-owner execution))
                (cid (backend-command-id *backend*)))
           (rove:ok (null (query-execution-owner-snapshot owner)))
           (rove:ok (zerop *active-snapshots*))
           (rove:ok (not (gethash *backend* *backend-used-command-ids*)))
           (extended-apply-operation stream '(:execute "p" 2) #())
           (rove:ok (= 1 (car calls)))
           (rove:ok (= cid (backend-command-id *backend*)))
           (rove:ok (= 1 *active-snapshots*))
           (rove:ok (= cid (snapshot-curcid (query-execution-owner-snapshot owner))))
           (rove:ok (extended-dml-execution-ready-p execution))
           (rove:ok (not (extended-dml-execution-at-end-p execution)))
           (extended-apply-operation stream '(:execute "p" 0) #())
           (extended-apply-operation stream '(:execute "p" -1) #())
           (rove:ok (= 1 (car calls)))
           (rove:ok (= (+ cid 2) (backend-command-id *backend*)))
           (let ((frames (select-portal-test-frames stream)))
             (rove:ok (equal (mapcar #'car frames) '(#\1 #\2 #\D #\D #\s #\C #\C)))
             (dolist (frame (last frames 2))
               (rove:ok (equalp (cdr frame)
                                (string->bytes (format nil "~a 0~c" (second case) (code-char 0)))))))
           (extended-close-portal portal)
           (rove:ok (zerop *active-snapshots*)))))))

(rove:deftest returning-portal-close-before-sync-preserves-command-use
    (call-with-returning-portal-test
     (lambda (stream calls)
       (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
       (extended-apply-operation stream '(:bind "never" "s" () () ()) #())
       (let ((cid (backend-command-id *backend*)))
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= cid (backend-command-id *backend*)))
         (extended-apply-operation stream '(:close #\P "never") #())
         (rove:ok (zerop (car calls)))
         (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
         (extended-apply-operation stream '(:execute "p" 1) #())
         (extended-apply-operation stream '(:flush) #())
         (rove:ok (= cid (backend-command-id *backend*)))
         (extended-apply-operation stream '(:close #\P "p") #())
         (rove:ok (zerop *active-snapshots*))
         (rove:ok (= 1 (car calls)))
         (rove:ok (= cid (gethash *backend* *backend-used-command-ids*)))
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= (1+ cid) (backend-command-id *backend*)))
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= (1+ cid) (backend-command-id *backend*)))))))

(rove:deftest returning-portal-partial-fetch-and-spill-lifetime
    (let ((*query-work-memory-bytes* 1)
          (*query-temp-directory* (uiop:temporary-directory)))
      (call-with-returning-portal-test
       (lambda (stream calls)
         (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
         (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
         (extended-apply-operation stream '(:execute "p" 1) #())
         (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                (execution (extended-dml-execution-for portal))
                (owner (extended-dml-execution-owner execution))
                (store (extended-dml-execution-store execution))
                (path (query-row-spool-pathname (query-replay-store-spool store)))
                (quota (query-temp-cleanup-owner-quota (query-execution-owner-temp-owner owner))))
           (rove:ok (and path (probe-file path)))
           (rove:ok (plusp (query-temp-quota-used-bytes quota)))
           (extended-apply-operation stream '(:execute "p" -2147483648) #())
           (rove:ok (= 1 (car calls)))
           (rove:ok (probe-file path))
           (let ((frames (select-portal-test-frames stream)))
             (rove:ok (equal (mapcar #'car frames) '(#\1 #\2 #\D #\s #\D #\C)))
             (rove:ok (equalp (cdr (car (last frames)))
                              (string->bytes (format nil "UPDATE 1~c" (code-char 0))))))
           (extended-apply-operation stream '(:close #\P "p") #())
           (rove:ok (not (probe-file path)))
           (rove:ok (zerop (query-temp-quota-used-bytes quota))))))))

(rove:deftest returning-portal-handoff-errors-publish-no-rows
    (dolist (failure '(:after-receiver :adoption :reader))
      (let ((*query-work-memory-bytes* 1)
            (*query-temp-directory* (uiop:temporary-directory)))
        (call-with-returning-portal-test
         (lambda (stream calls)
           (declare (ignore calls))
           (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
           (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
           (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                  (owner (extended-dml-execution-owner (extended-dml-execution-for portal)))
                  (quota (query-temp-cleanup-owner-quota (query-execution-owner-temp-owner owner)))
                  (name (case failure (:adoption 'adopt-sealed-query-row-spool) (:reader 'make-query-replay-reader)))
                  (original (when name (symbol-function name)))
                  (close-original (symbol-function 'close-query-row-spool))
                  (closed (make-hash-table :test #'eq)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'close-query-row-spool)
                          (lambda (spool)
                            (incf (gethash spool closed 0))
                            (funcall close-original spool)))
                    (when name
                      (setf (symbol-function name)
                            (lambda (&rest arguments) (declare (ignore arguments)) (error "injected handoff failure"))))
                    (rove:ok
                     (handler-case (progn (extended-apply-operation stream '(:execute "p" 1) #()) nil)
                       (error () (note-query-error) t))))
               (when name (setf (symbol-function name) original))
               (setf (symbol-function 'close-query-row-spool) close-original))
             (rove:ok (= 1 (hash-table-count closed)))
             (rove:ok (every (lambda (count) (= count 1))
                             (loop for count being the hash-values of closed collect count)))
             (rove:ok (query-execution-owner-closed-p owner))
             (rove:ok (zerop *active-snapshots*))
             (rove:ok (zerop (query-temp-quota-used-bytes quota)))
             (rove:ok (eq :aborted (backend-state *backend*)))
             (rove:ok (equal (mapcar #'car (select-portal-test-frames stream)) '(#\1 #\2)))))
         :after-finish (when (eq failure :after-receiver)
                         (lambda () (error "injected outer unwind failure")))))))

(rove:deftest returning-portal-nonlocal-handoff-exits-cannot-reexecute
    (dolist (phase '(:receiver :reader))
      (dolist (exit '(:timeout :throw))
        (flet ((leave ()
                 (if (eq exit :timeout)
                     (error 'sb-ext:timeout)
                     (throw 'returning-test-escape :thrown))))
          (call-with-returning-portal-test
           (lambda (stream calls)
             (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
             (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
             (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                    (execution (extended-dml-execution-for portal))
                    (owner (extended-dml-execution-owner execution))
                    (reader (symbol-function 'make-query-replay-reader))
                    (result nil))
               (unwind-protect
                    (progn
                      (when (eq phase :reader)
                        (setf (symbol-function 'make-query-replay-reader)
                              (lambda (store)
                                (funcall reader store)
                                (leave))))
                      (setf result
                            (handler-case
                                (catch 'returning-test-escape
                                  (extended-apply-operation stream '(:execute "p" 1) #())
                                  :completed)
                              (sb-ext:timeout () :timed-out))))
                 (setf (symbol-function 'make-query-replay-reader) reader))
               (rove:ok (eq result (if (eq exit :timeout) :timed-out :thrown)))
               (rove:ok (= 1 (car calls)))
               (rove:ok (extended-dml-execution-failed-p execution))
               (rove:ok (not (extended-dml-execution-ready-p execution)))
               (rove:ok (query-execution-owner-closed-p owner))
               (rove:ok (zerop *active-snapshots*))
               (rove:ok
                (handler-case (progn (extended-apply-operation stream '(:execute "p" 1) #()) nil)
                  (pg-error (condition) (string= "55000" (pg-error-sqlstate condition)))))
               (rove:ok (= 1 (car calls)))
               (rove:ok (equal (mapcar #'car (select-portal-test-frames stream)) '(#\1 #\2)))))
           :after-finish (when (eq phase :receiver) #'leave))))))

(rove:deftest returning-portal-close-keeps-cleanup-failure-details
    (call-with-returning-portal-test
     (lambda (stream calls)
       (declare (ignore calls))
       (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
       (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
       (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
              (owner (extended-dml-execution-owner (extended-dml-execution-for portal)))
              (fail t))
         (query-execution-owner-add-cleanup
          owner (lambda () (when fail (ereport "58030" "injected portal close failure"))))
         (multiple-value-bind (closed failures) (extended-close-portal portal)
           (rove:ok (not closed))
           (rove:ok
            (find-if (lambda (condition)
                       (and (typep condition 'pg-error)
                            (string= "injected portal close failure" (pg-error-message condition))))
                     failures)))
         (rove:ok (eq :cleanup-pending (query-execution-owner-state owner)))
         (setf fail nil)
         (rove:ok (extended-close-portal portal))
         (rove:ok (query-execution-owner-closed-p owner))))))

(rove:deftest returning-portal-cleanup-fatal-takes-precedence
    (dolist (exit '(:error :timeout :throw :fatal :handled-fatal))
      (call-with-returning-portal-test
       (lambda (stream calls)
         (declare (ignore calls))
         (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
         (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
         (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                (execution (extended-dml-execution-for portal))
                (owner (extended-dml-execution-owner execution))
                (reader (symbol-function 'make-query-replay-reader))
                (original (case exit
                            ((:error :handled-fatal) (make-condition 'simple-error :format-control "original error"))
                            (:timeout (make-condition 'sb-ext:timeout))
                            (:fatal (make-condition 'pg-error :severity "FATAL" :sqlstate "57P01"
                                                              :message "original fatal"))))
                (cleanup-fatal (make-condition 'pg-error :severity "FATAL" :sqlstate "57P01"
                                                         :message "cleanup fatal"))
                (fail t)
                (caught nil))
           (query-execution-owner-add-cleanup
            owner (lambda ()
                    (rove:ok (extended-dml-execution-failed-p execution))
                    (when fail (setf fail nil) (error cleanup-fatal))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'make-query-replay-reader)
                        (lambda (store)
                          (funcall reader store)
                          (when (eq exit :handled-fatal)
                            (handler-case
                                (ereport-fatal "57P01" "handled fatal")
                              (pg-error () nil)))
                          (if (eq exit :throw)
                              (throw 'returning-fatal-test :unexpected-throw)
                              (error original))))
                  (setf caught
                        (handler-case
                            (catch 'returning-fatal-test
                              (extended-apply-operation stream '(:execute "p" 1) #())
                              nil)
                          (pg-error (condition) condition)
                          (error (condition) condition)
                          (sb-ext:timeout (condition) condition))))
             (setf (symbol-function 'make-query-replay-reader) reader))
           (rove:ok (eq caught (if (eq exit :fatal) original cleanup-fatal)))
           (rove:ok (extended-dml-execution-failed-p execution))
           (rove:ok (eq :cleanup-pending (query-execution-owner-state owner)))
           (rove:ok (member owner (query-execution-owner-reload-state) :test #'eq))
           (rove:ok (equal (mapcar #'car (select-portal-test-frames stream)) '(#\1 #\2)))
           (rove:ok (extended-close-portal portal))
           (rove:ok (zerop *active-snapshots*)))))))

(rove:deftest returning-portal-second-interrupt-sees-failed-publication
    (call-with-returning-portal-test
     (lambda (stream calls)
       (declare (ignore calls))
       (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
       (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
       (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
              (execution (extended-dml-execution-for portal))
              (owner (extended-dml-execution-owner execution))
              (reader (symbol-function 'make-query-replay-reader))
              (close (symbol-function 'extended-close-portal))
              (original (make-condition 'sb-ext:timeout))
              (second (make-condition 'sb-ext:timeout))
              (observed nil)
              (caught nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'make-query-replay-reader)
                      (lambda (store) (funcall reader store) (error original))
                      (symbol-function 'extended-close-portal)
                      (lambda (value &optional failed-p)
                        (declare (ignore value failed-p))
                        (sb-thread:interrupt-thread
                         sb-thread:*current-thread*
                         (lambda ()
                           (setf observed (extended-dml-execution-failed-p execution))
                           (error second)))))
                (let ((*error-output* (make-string-output-stream)))
                  (setf caught
                        (handler-case (extended-apply-operation stream '(:execute "p" 1) #())
                          (sb-ext:timeout (condition) condition)))))
           (setf (symbol-function 'make-query-replay-reader) reader
                 (symbol-function 'extended-close-portal) close))
         (rove:ok (eq caught original))
         (rove:ok observed)
         (rove:ok (extended-dml-execution-failed-p execution))
         (rove:ok (member owner (query-execution-owner-reload-state) :test #'eq))
         (rove:ok (extended-close-portal portal))
         (rove:ok (zerop *active-snapshots*))))))

(rove:deftest returning-portal-spill-quota-is-shared-across-portals
    (let ((*query-work-memory-bytes* 1)
          (*query-temp-directory* (uiop:temporary-directory)))
      (call-with-returning-portal-test
       (lambda (stream calls)
         (declare (ignore calls))
         (extended-apply-operation stream '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
         (extended-apply-operation stream '(:bind "one" "s" () () ()) #())
         (extended-apply-operation stream '(:bind "two" "s" () () ()) #())
         (flet ((owner (name)
                  (extended-dml-execution-owner
                   (extended-dml-execution-for
                    (gethash name (extended-query-state-portals *extended-query-state*))))))
           (let* ((one (owner "one")) (two (owner "two"))
                                      (quota (query-temp-cleanup-owner-quota (query-execution-owner-temp-owner one))))
             (rove:ok (eq quota (query-temp-cleanup-owner-quota (query-execution-owner-temp-owner two))))
             (extended-apply-operation stream '(:execute "one" 1) #())
             (rove:ok (plusp (query-temp-quota-used-bytes quota)))
             (let ((*query-temp-file-limit-bytes* (1+ (query-temp-quota-used-bytes quota))))
               (rove:ok
                (handler-case (progn (extended-apply-operation stream '(:execute "two" 1) #()) nil)
                  (pg-error (condition) (string= (pg-error-sqlstate condition) "53400")))))
             (rove:ok (query-execution-owner-closed-p one))
             (rove:ok (query-execution-owner-closed-p two))
             (rove:ok (zerop *active-snapshots*))
             (rove:ok (zerop (query-temp-quota-used-bytes quota)))))))))

(rove:deftest returning-borrowed-spool-is-deferred-and-retained
    (let* ((*query-temp-directory* (uiop:temporary-directory))
           (old-close (symbol-function 'close-query-row-spool))
           (close-count 0)
           (seen nil)
           (path nil)
           (targets (list :target)))
      (unwind-protect
           (progn
             (setf (symbol-function 'close-query-row-spool)
                   (lambda (spool) (incf close-count) (funcall old-close spool)))
             (with-query-temp-files
               (let* ((*dml-returning-spool* (make-query-row-spool 1))
                      (borrowed *dml-returning-spool*)
                      (*dml-returning-receiver*
                        (lambda (spool receiver-targets tag)
                          (setf seen (list spool receiver-targets tag)))))
                 (call-with-dml-returning-spool
                  targets
                  (lambda ()
                    (rove:ok (eq borrowed *dml-returning-spool*))
                    (query-row-spool-append *dml-returning-spool* (vector "large value"))
                    (setf path (query-row-spool-pathname *dml-returning-spool*))
                    (finish-dml-command nil targets "UPDATE 1")))
                 (rove:ok (eq borrowed (first seen)))
                 (rove:ok (eq targets (second seen)))
                 (rove:ok (equal "UPDATE 1" (third seen)))
                 (rove:ok (query-row-spool-sealed-p borrowed))
                 (rove:ok (zerop close-count))
                 (let ((rows nil))
                   (map-query-row-spool borrowed
                                        (lambda (row index)
                                          (declare (ignore index))
                                          (push row rows)))
                   (rove:ok (equalp rows (list (vector "large value")))))
                 (rove:ok (handler-case
                              (progn (call-with-dml-returning-spool targets
                                                                    (lambda () (error "injected")))
                                     nil)
                            (error () t)))
                 (rove:ok (zerop close-count))
                 (let ((*dml-returning-spool* nil))
                   (rove:ok (handler-case (progn (call-with-dml-returning-spool targets (lambda () t)) nil)
                              (error () t))))
                 (let ((*dml-returning-receiver* nil))
                   (call-with-dml-returning-spool targets (lambda () t)))
                 (rove:ok (= close-count 1))
                 (close-query-row-spool borrowed)))
             (rove:ok path)
             (rove:ok (not (probe-file path))))
        (setf (symbol-function 'close-query-row-spool) old-close))))

(rove:deftest returning-deferred-handoff-failure-keeps-caller-ownership
    (let ((*query-temp-directory* (uiop:temporary-directory)))
      (with-query-temp-files
        (dolist (mode '(:error :throw))
          (let* ((spool (make-query-row-spool 1))
                 (*dml-returning-spool* spool)
                 (called nil)
                 (*dml-returning-receiver*
                   (lambda (received targets tag)
                     (declare (ignore targets tag))
                     (setf called (eq received spool))
                     (ecase mode
                       (:error (error "injected receiver failure"))
                       (:throw (throw 'receiver-exit :throw))))))
            (unwind-protect
                 (progn
                   (query-row-spool-append spool (vector "retained"))
                   (rove:ok (eq mode
                                (handler-case
                                    (catch 'receiver-exit
                                      (call-with-dml-returning-spool
                                       (list :target)
                                       (lambda () (finish-dml-command nil (list :target) "DELETE 1")))
                                      nil)
                                  (error () :error))))
                   (rove:ok called)
                   (rove:ok (query-row-spool-sealed-p spool))
                   (let ((rows nil))
                     (map-query-row-spool spool
                                          (lambda (row index)
                                            (declare (ignore index))
                                            (push row rows)))
                     (rove:ok (equalp rows (list (vector "retained"))))))
              (close-query-row-spool spool)))))))

(rove:deftest returning-bind-admits-target-and-readers-together
    (let* ((target (returning-test-table))
           (source (returning-test-table))
           (index (make-pg-index :relfilenode 24))
           (spec (list :table source :path (make-select-scan-path :kind :index-scan :index index)))
           (prepared (list :table target :columns (pg-class-columns target)
                           :insert-source-plan (%make-query-plan :prepared (list :relations (list spec)))))
           (names '(register-relation-accesses ensure-current-table refresh-on-conflict-arbiters
                    revalidate-subquery-relations buf-nblocks))
           (originals (mapcar #'symbol-function names))
           (admissions nil)
           (validated nil))
      (setf (pg-class-relfilenode target) 11
            (pg-class-toast target) (make-pg-toast :relfilenode 12 :index-relfilenode 13)
            (pg-class-relfilenode source) 21
            (pg-class-toast source) (make-pg-toast :relfilenode 22 :index-relfilenode 23))
      (unwind-protect
           (progn
             (setf (symbol-function 'register-relation-accesses)
                   (lambda (writes reads xid)
                     (push (list writes reads xid) admissions))
                   (symbol-function 'ensure-current-table)
                   (lambda (table) (rove:ok (eq table target)))
                   (symbol-function 'refresh-on-conflict-arbiters)
                   (lambda (&rest arguments) (declare (ignore arguments)))
                   (symbol-function 'revalidate-subquery-relations)
                   (lambda (relations)
                     (rove:ok (= 1 (length admissions)))
                     (rove:ok (equal '(22 23) (getf (first relations) :registered-toast-nodes)))
                     (setf validated t))
                   (symbol-function 'buf-nblocks) (lambda (node) (declare (ignore node)) 2))
             (let ((*select-suppress-description* nil))
               (multiple-value-bind (table rel xid relations domain)
                   (register-dml-subquery-relations prepared 77)
                 (declare (ignore relations domain))
                 (rove:ok (eq table target))
                 (rove:ok (= rel 11))
                 (rove:ok (= xid 77))))
             (rove:ok validated)
             (rove:ok (equal admissions '(((11) (12 13 21 22 23 24) 77)))))
        (loop for name in names for original in originals
              do (setf (symbol-function name) original)))))

(rove:deftest returning-extended-drift-precedes-mutation
    (let* ((targets (returning-test-targets "DELETE FROM things RETURNING id,label"))
           (*select-suppress-description* t)
           (*select-expected-result-columns* (mapcar #'target->result-column targets))
           (called nil))
      (setf (result-column-name (first *select-expected-result-columns*)) "old_name")
      (rove:ok
       (handler-case
           (progn
             ;; The absent table also proves validation precedes resource admission.
             (call-with-dml-subqueries
              (list :returning targets)
              (lambda (&rest arguments)
                (declare (ignore arguments))
                (setf called t)))
             nil)
         (pg-error (condition) (string= (pg-error-sqlstate condition) "0A000"))))
      (rove:ok (not called))))

(rove:deftest returning-extended-describe-aborted-transaction
    (let* ((*backend* (make-backend))
           (*extended-query-state* (make-extended-query-state))
           (statement (make-extended-statement
                       :ast (first (parse-statements "DELETE FROM things RETURNING id"))
                       :types #())))
      (set-backend-state *backend* :aborted)
      (setf (gethash "s" (extended-query-state-statements *extended-query-state*)) statement
            (gethash "p" (extended-query-state-portals *extended-query-state*))
            (make-extended-query-portal statement #() t nil #()))
      (dolist (operation '((:describe #\S "s") (:describe #\P "p")))
        (rove:ok
         (handler-case
             (progn (extended-apply-operation nil operation #()) nil)
           (pg-error (condition) (string= (pg-error-sqlstate condition) "25P02")))))))

(defun returning-compatible-column-lists-p (columns expected)
  ;; Format is a Bind choice, independent of the result tuple descriptor.
  (flet ((without-format (column)
           (let ((copy (copy-result-column column)))
             (setf (result-column-format copy) 0)
             copy)))
    (equalp (mapcar #'without-format columns) (mapcar #'without-format expected))))

(defun returning-shape-error-p (condition)
  (and (typep condition 'pg-error) (string= (pg-error-sqlstate condition) "0A000")))

(cl-spec:defgenerator returning-column-shape-arguments ()
  (let* ((columns
           (loop for index below (1+ (random 4))
                 collect (make-result-column
                          :name (format nil "column_~d" index) :table-oid (+ 100 index)
                          :attnum (1+ index) :type-oid 1043 :typlen -1
                          :typmod (+ 4 (random 24)) :format (random 2))))
         (expected (mapcar #'copy-result-column columns)))
    (case (random 8)
      (0 (setf (result-column-name (first expected)) "changed"))
      (1 (incf (result-column-table-oid (first expected))))
      (2 (incf (result-column-attnum (first expected))))
      (3 (setf (result-column-type-oid (first expected)) 25))
      (4 (setf (result-column-typlen (first expected)) 4))
      (5 (incf (result-column-typmod (first expected))))
      (6 (setf expected (rest expected)))
      (7 (setf (result-column-format (first expected))
               (- 1 (result-column-format (first expected))))))
    (list columns expected)))

(cl-spec:defspec-function check-result-column-shape
  "A bound portal rejects a changed descriptor before executing any mutation."
  (:args (columns (type list)) (expected (type list)))
  (:args-generator returning-column-shape-arguments)
  (:capture (compatible (returning-compatible-column-lists-p columns expected)))
  (:cases
   (:same-shape
    (:when compatible)
    (:returns (type list))
    (:post (eq result columns)))
   (:changed-shape
    (:when (not compatible))
    (:signals (satisfies returning-shape-error-p)))))

(cl-spec:defproperty returning-format-does-not-change-shape
    ((format (member 0 1)) (typmod (range integer 4 128)))
  (:about check-result-column-shape extended-select-result-formats)
  (:trials (:normal 100))
  (let* ((column (make-result-column :name "value" :table-oid 1234 :attnum 2
                                     :type-oid 1043 :typlen -1 :typmod typmod))
         (expected (list (copy-result-column column)))
         (columns (list column)))
    (extended-select-result-formats (list format) columns)
    (and (eq columns (check-result-column-shape columns expected))
         (= (result-column-format (first expected)) 0))))

(rove:deftest returning-generated-descriptor-shape
    (let* ((data (cl-spec:result-data
                  (cl-spec:check-function 'check-result-column-shape :trials 100 :seed 42)))
           (passed (eq :passed (getf data :status))))
      (rove:ok passed (if passed "RETURNING descriptor Function Spec passed"
                          (format nil "~s" data))))
  (unit-check-property 'returning-format-does-not-change-shape))
