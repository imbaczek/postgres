(in-package #:pg)

(rove:deftest insert-select-parser
    (dolist (sql '("INSERT INTO dst SELECT 1"
                   "INSERT INTO dst(id) SELECT 1 UNION ALL SELECT 2 RETURNING id"
                   "INSERT INTO dst AS d SELECT 1 WHERE true RETURNING d.*"
                   "INSERT INTO dst WITH q AS(SELECT 1) SELECT * FROM q"
                   "INSERT INTO dst DEFAULT VALUES RETURNING *"))
      (rove:ok (eq :insert (getf (first (parse-statements sql)) :type)) sql)))

(rove:deftest insert-select-analysis
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 1))
           (table (make-pg-class :oid 9001 :name "dst" :relfilenode 9001
                                 :columns (vector (make-pg-column :name "id" :type :int4 :typmod -1 :attnum 1)
                                                  (make-pg-column :name "label" :type :varchar :typmod 9 :attnum 2)))))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "dst" (database-tables database)) table)
      (dolist (sql '("INSERT INTO dst SELECT '7'"
                     "INSERT INTO dst SELECT 7,'abc'"
                     "INSERT INTO dst SELECT 7 RETURNING old.id,new.id"))
        (let ((prepared (prepare-dml-subqueries (first (parse-statements sql)))))
          (rove:ok (getf prepared :insert-source-plan) sql)
          (rove:ok (null (select-scope-analysis-frame (getf prepared :scope))))))
      (dolist (sql '("INSERT INTO dst(id) SELECT 1,2"
                     "INSERT INTO dst(id,label) SELECT 1"))
        (let ((state nil))
          (handler-case (prepare-dml-subqueries (first (parse-statements sql)))
            (pg-error (error) (setf state (pg-error-sqlstate error))))
          (rove:ok (equal state "42601") sql)))))

(rove:deftest combo-command-id-snapshot-boundaries
    (let* ((xid 987654321) (combo (make-combo-command-id xid 1 3))
                           (at-delete (make-snapshot-struct :xmin xid :xmax (1+ xid) :xip (list xid) :curcid 3))
                           (before-insert (make-snapshot-struct :xmin xid :xmax (1+ xid) :xip (list xid) :curcid 1))
                           (after-delete (make-snapshot-struct :xmin xid :xmax (1+ xid) :xip (list xid) :curcid 4)))
      (unwind-protect
           (progn
             (rove:ok (= combo (make-combo-command-id xid 1 3)))
             (rove:ok (heap-tuple-visible-p xid xid combo at-delete xid +heap-combo-cid+))
             (rove:ok (not (heap-tuple-visible-p xid xid combo before-insert xid +heap-combo-cid+)))
             (rove:ok (not (heap-tuple-visible-p xid xid combo after-delete xid +heap-combo-cid+)))
             (rove:ok (not (heap-tuple-visible-p xid 0 3 at-delete xid))))
        (forget-combo-command-ids xid))
      (rove:ok (null (gethash xid *combo-command-ids*)))))
