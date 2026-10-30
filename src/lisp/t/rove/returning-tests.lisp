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

(rove:deftest returning-extended-metadata-and-formats
    (let* ((*backend* (make-backend))
           (*extended-query-state* (make-extended-query-state))
           (path (merge-pathnames
                  (format nil "pg-returning-wire-~d-~d" (sb-posix:getpid) (random 1000000))
                  (uiop:temporary-directory)))
           (old-prepare (symbol-function 'prepare-dml-subqueries))
           (old-execute (symbol-function 'execute-statement))
           (targets (returning-test-targets
                     "UPDATE things SET id=id RETURNING id,label"))
           (frames nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'prepare-dml-subqueries)
                   (lambda (ast) (declare (ignore ast)) (list :returning targets))
                   (symbol-function 'execute-statement)
                   (lambda (stream ast)
                     (declare (ignore ast))
                     (rove:ok *select-suppress-description*)
                     (rove:ok (equalp *select-result-formats* #(0 1)))
                     (with-query-temp-files ()
                                            (let ((*dml-returning-spool* (make-query-row-spool)))
                                              (unwind-protect
                                                   (progn
                                                     (query-row-spool-append *dml-returning-spool* #(7 "label"))
                                                     (finish-dml-command stream targets "UPDATE 1"))
                                                (close-query-row-spool *dml-returning-spool*))))))
             (with-open-file (out path :direction :io :element-type '(unsigned-byte 8)
                                       :if-exists :supersede :if-does-not-exist :create)
               (extended-apply-operation
                out '(:parse "s" "UPDATE things SET id=id RETURNING id,label" ()) #())
               (extended-apply-operation out '(:describe #\S "s") #())
               (extended-apply-operation out '(:bind "p" "s" () () (0 1)) #())
               (extended-apply-operation out '(:describe #\P "p") #())
               (extended-apply-operation out '(:execute "p" 0) #())
               (finish-output out)
               (file-position out 0)
               (loop for (type length payload) = (multiple-value-list (read-message-frame out))
                     while type do (push (cons (code-char type) payload) frames)))
             (setf frames (nreverse frames))
             (rove:ok (equal (mapcar #'car frames) '(#\1 #\t #\T #\2 #\T #\D #\C)))
             (dolist (entry (list (cons (third frames) '(0 0))
                                  (cons (fifth frames) '(0 1))))
               (let ((columns (let ((payload (cdar entry)) (position 2))
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
                              #(0 2 0 0 0 1 55 0 0 0 5 108 97 98 101 108))))
        (setf (symbol-function 'prepare-dml-subqueries) old-prepare
              (symbol-function 'execute-statement) old-execute)
        (when (probe-file path) (delete-file path)))))

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
