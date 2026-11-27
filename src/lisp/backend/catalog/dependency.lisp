;;;; Relation and column identities used by catalog dependencies.
(in-package #:pg)

(defstruct catalog-object-address
  (database-oid nil :read-only t)
  (kind nil :read-only t)
  (object-oid nil :read-only t)
  (sub-id 0 :read-only t))

(define-catalog-codec catalog-object-address make-catalog-object-address
  (:database-oid catalog-object-address-database-oid :database-oid)
  (:kind catalog-object-address-kind :kind)
  (:object-oid catalog-object-address-object-oid :object-oid)
  (:sub-id catalog-object-address-sub-id :sub-id :default 0 :omit-default t))

(defun catalog-relation-address (database object &optional (sub-id 0))
  "Identify a relation or column independently of its name and physical storage."
  (multiple-value-bind (kind oid)
      (etypecase object
        (pg-class (values :table (pg-class-oid object)))
        (pg-index (values :index (pg-index-oid object))))
    (make-catalog-object-address :database-oid (database-oid database)
                                 :kind kind :object-oid oid :sub-id sub-id)))

(defun catalog-index-owner-compatible-p (index table)
  "Check the owning relation and key column dependencies without allocating addresses."
  (and table
       (eql (pg-index-table-oid index) (pg-class-oid table))
       (let ((column-count (length (pg-class-columns table))))
         (every (lambda (attnum)
                  (and (integerp attnum) (<= 1 attnum column-count)))
                (or (pg-index-referenced-columns index) (pg-index-columns index))))))
