;;;; Complete WAL catalog history, rather than mutable catalog state, proves drops.
(in-package #:pg-physical-tests)

(defun history-catalog-payload (nodes deletes &optional (identity "history-source"))
  (let* ((pg::*catalog* (pg::make-catalog :cluster-identity identity))
         (database (pg::make-database :name "postgres")))
    (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
    (dolist (node nodes)
      (let ((name (format nil "table_~d" node)))
        (setf (gethash name (pg::database-tables database))
              (pg::make-pg-class :oid node :relfilenode node :name name :columns #()))))
    (pg::wal-encode-catalog-payload (pg::catalog-image-bytes) deletes)))

(defun history-drop-proof (specs)
  (with-physical-store-directory (root)
    (let ((store (pg::physical-store-open root 123 1 :create t))
          (cursor 0))
      (unwind-protect
           (progn
             (dolist (spec specs)
               (destructuring-bind (type payload &optional (xid 0)) spec
                 (let ((raw (pg::wal-encode-record type xid cursor payload)))
                   (pg::physical-store-input store cursor raw)
                   (incf cursor (length raw)))))
             (pg::physical-store-flush store)
             (prog1 (pg::physical-recovery-proven-drops store "history-source")
               (ok (not (probe-file (merge-pathnames "global/pg_catalog.dat" root))))
               (ok (not (probe-file (merge-pathnames "base/" root))))
               (ok (not (probe-file (merge-pathnames "pg_xact" root))))))
        (pg::physical-store-close store)))))

(defun history-batch (type payload &optional (xid 0))
  (list (list pg::+wal-record-batch-start+ (empty-octets))
        (list type payload xid)))

(deftest physical-history-complete-catalog-proves-drop
    (let ((removed (history-drop-proof
                    (history-batch pg::+wal-record-catalog+
                                   (history-catalog-payload '(901) '(900))))))
      (ok (gethash 900 removed))
      (ok (not (gethash 901 removed))))
  (let ((removed (history-drop-proof
                  (append (history-batch pg::+wal-record-catalog+
                                         (history-catalog-payload nil '(900)))
                          (history-batch pg::+wal-record-catalog+
                                         (history-catalog-payload '(900) nil))))))
    (ok (not (gethash 900 removed)))))

(deftest physical-history-chunked-catalog-and-incomplete-tails
    (let* ((payload (history-catalog-payload nil '(900)))
           (split (floor (length payload) 2))
           (first (pg::wal-encode-catalog-chunk-payload (subseq payload 0 split) 0 2))
           (second (pg::wal-encode-catalog-chunk-payload (subseq payload split) 1 2))
           (terminal (pg::wal-encode-catalog-commit-payload payload 2))
           (prefix (list (list pg::+wal-record-batch-start+ (empty-octets))
                         (list pg::+wal-record-catalog-chunk+ first)
                         (list pg::+wal-record-catalog-chunk+ second)))
           (complete (append prefix (list (list pg::+wal-record-catalog-commit+ terminal)))))
      (ok (zerop (hash-table-count (history-drop-proof prefix))))
      (ok (gethash 900 (history-drop-proof complete)))
      (let ((restarted (append prefix (history-batch pg::+wal-record-checkpoint+ (empty-octets))))
            (missing (append (butlast prefix)
                             (list (list pg::+wal-record-catalog-commit+ terminal))))
            (wrong-terminal (append prefix (list (list pg::+wal-record-maintenance+ (empty-octets))))))
        (ok (zerop (hash-table-count (history-drop-proof restarted))))
        (ok (intake-fails-p (lambda () (history-drop-proof missing))))
        (ok (intake-fails-p (lambda () (history-drop-proof wrong-terminal)))))))

(deftest physical-history-invalid-batches-cannot-certify-later-drop
    (let ((drop (history-batch pg::+wal-record-catalog+ (history-catalog-payload nil '(900)))))
      (dolist (invalid
               (list
                (list (list pg::+wal-record-catalog+ (history-catalog-payload nil '(900))))
                (history-batch pg::+wal-record-commit+ #(1) 3)
                (history-batch pg::+wal-record-maintenance+ (empty-octets) 3)
                (history-batch pg::+wal-record-checkpoint+ #(1))
                (history-batch pg::+wal-record-clog-reset+ #(1))
                (history-batch pg::+wal-record-pages+ #(1))
                (history-batch pg::+wal-record-logical-changes+ #(1) 3)
                (history-batch pg::+wal-record-logical-catalog-event+ #(1))))
        (ok (intake-fails-p (lambda () (history-drop-proof (append invalid drop))))))))

(deftest physical-history-rejects-live-delete-and-foreign-catalog
    (dolist (payload (list (history-catalog-payload '(900) '(900))
                           (history-catalog-payload nil '(900) "foreign")
                           (history-catalog-payload nil '(0))))
      (ok (intake-fails-p
           (lambda () (history-drop-proof (history-batch pg::+wal-record-catalog+ payload)))))))

(deftest physical-history-rejects-ambiguous-catalog-keys-and-trailing-forms
    (dolist (text
             (list
              "(:cluster-identity \"history-source\" :next-oid 1000 :databases ()) ()"
              "(:cluster-identity \"history-source\" :next-oid 1000 :databases () :databases ())"
              "(:cluster-identity \"history-source\" :next-oid 1000)"
              "(:cluster-identity \"history-source\" :next-oid 1000 :databases ((:name \"db\" :tables ((:name \"x\" :relfilenode 900 :relfilenode 901)) :indexes ())))"))
      (ok (intake-fails-p
           (lambda ()
             (pg::physical-history-catalog-relfilenodes (pg::string->bytes text) "history-source")))))
  ;; Repeated names cannot hide an older relfilenode through hash-table overwrite.
  (let* ((text "(:cluster-identity \"history-source\" :next-oid 1000 :databases ((:name \"db\" :tables ((:name \"x\" :relfilenode 900) (:name \"x\" :relfilenode 901)) :indexes ())))")
         (payload (pg::wal-encode-catalog-payload (pg::string->bytes text) '(900))))
    (ok (intake-fails-p
         (lambda () (history-drop-proof (history-batch pg::+wal-record-catalog+ payload)))))))

(deftest physical-history-reader-rejects-cycles-without-rejecting-arrays
    (dolist (text '("#1=(:cluster-identity \"history-source\" :next-oid 1000 :databases #1#)"
                    "#1=(#1#)" "#.(error \"must not evaluate\")"))
      (ok (intake-fails-p
           (lambda () (pg::physical-history-read-catalog (pg::string->bytes text))))))
  (ok (equalp #(1 2) (pg::physical-history-read-catalog (pg::string->bytes "#(1 2)"))))
  (ok (string= "abc" (pg::physical-history-read-catalog
                      (pg::string->bytes "#A((3) BASE-CHAR . \"abc\")")))))

(deftest physical-history-base-forks-require-proof-and-minimum-length
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (manifest (list :files '(("base/900" 16 0) ("base/900.vm" 8 0))))
            (removed (make-hash-table :test #'eql)))
        (ok (intake-fails-p (lambda () (pg::physical-recovery-check-base-forks manifest removed))))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (pg::wal-atomic-write-bytes (merge-pathnames "base/900" root)
                                    (make-array 16 :element-type '(unsigned-byte 8) :initial-element 0))
        (pg::wal-atomic-write-bytes (merge-pathnames "base/900.vm" root)
                                    (make-array 7 :element-type '(unsigned-byte 8) :initial-element 0))
        (ok (intake-fails-p (lambda () (pg::physical-recovery-check-base-forks manifest removed))))
        (setf (gethash 900 removed) t)
        (ok (not (intake-fails-p
                  (lambda () (pg::physical-recovery-check-base-forks manifest removed)))))
        (remhash 900 removed)
        (pg::wal-atomic-write-bytes (merge-pathnames "base/900.vm" root)
                                    (make-array 8 :element-type '(unsigned-byte 8) :initial-element 0))
        (ok (not (intake-fails-p
                  (lambda () (pg::physical-recovery-check-base-forks manifest removed))))))))
