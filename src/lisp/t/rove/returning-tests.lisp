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
