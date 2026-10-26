;;;; Shared helpers for migrated unit regressions and their legacy callers.
;;;; Keep PG symbols: macro tests deliberately intercept PG bindings with FLET.
(in-package #:pg)

(defun logical-test-columns ()
  (list (list :attnum 1 :name "id" :type :int4 :typmod -1 :notnull t)
        (list :attnum 2 :name "amount" :type :int8 :typmod -1 :notnull nil)
        (list :attnum 3 :name "label" :type :varchar :typmod 64 :notnull nil)))

(defun logical-test-change (kind database-oid relation-oid
                            &key old-values new-values (key-attnums '(1)))
  (make-logical-change :kind kind :database-oid database-oid
                       :relation-oid relation-oid :relation-name "logical_rows"
                       :columns (logical-test-columns)
                       :key-attnums key-attnums
                       :old-values old-values :new-values new-values))

(defun logical-test-chunk-record (xid index count)
  (let ((event
          (logical-encode-change
           (logical-test-change :insert 7 701 :new-values #(1 2 "chunk")))))
    (make-wal-record :type +wal-record-logical-changes+ :xid xid
                     :payload (logical-encode-chunk index count (list event)))))

(defun test-analyze-catalog-class-image-roundtrip (table)
  "Round-trip TABLE through the same readable catalog image used on disk."
  (let* ((*catalog* (make-catalog))
         (database (make-database :name "postgres" :oid 1)))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash (pg-class-name table) (database-tables database)) table)
    (let* ((image (bytes->string (catalog-image-bytes)))
           (restored
             (with-standard-io-syntax
               (let ((*read-eval* nil))
                 (catalog-from-sexp (read-from-string image)))))
           (restored-database
             (gethash "postgres" (catalog-databases restored))))
      (gethash (pg-class-name table) (database-tables restored-database)))))

(defun test-query-message (query)
  (let* ((query-bytes (string->bytes query))
         (payload (make-array (1+ (length query-bytes))
                              :element-type '(unsigned-byte 8)
                              :initial-element 0)))
    (replace payload query-bytes)
    (make-frontend-message #\Q payload)))

(defun logical-test-capture-row-change (catalog table application-name byte-count)
  (let* ((*catalog* catalog)
         (*backend* (make-backend :xid 9902
                                  :application-name application-name
                                  :database "postgres")))
    (setf (backend-logical-change-bytes *backend*) byte-count)
    (handler-case
        (progn
          (logical-note-row-change :insert table :new-values #(1 "small"))
          (values t (backend-logical-change-bytes *backend*)))
      (error () (values nil (backend-logical-change-bytes *backend*))))))

(defun logical-test-capture-truncate-event (catalog byte-count)
  (let* ((*catalog* catalog)
         (*backend* (make-backend :xid 9903 :database "postgres")))
    (setf (backend-logical-change-bytes *backend*) byte-count)
    (handler-case
        (progn
          (logical-note-transaction-event :truncate
                                          '(:tables ("logical_rows")))
          (values t (backend-logical-change-bytes *backend*)))
      (error ()
        (values nil (backend-logical-change-bytes *backend*))))))
