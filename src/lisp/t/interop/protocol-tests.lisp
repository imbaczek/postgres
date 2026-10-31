;;;; Independent bytes follow src/backend/replication/logical/proto.c.
(defpackage #:pg-interop-tests
  (:use #:cl #:rove))
(in-package #:pg-interop-tests)

(defun octets (values)
  (make-array (length values) :element-type '(unsigned-byte 8) :initial-contents values))

(defun test-row-equal-p (left right)
  "Compare nested row values with case-sensitive strings.
Preserve EQUALP's numeric comparison while descending into SQL value wrappers."
  (cond ((or (pg::sql-array-p left) (pg::sql-array-p right))
         (and (pg::sql-array-p left) (pg::sql-array-p right)
              (equalp (pg::sql-array-element-type-id left) (pg::sql-array-element-type-id right))
              (equalp (pg::sql-array-dimensions left) (pg::sql-array-dimensions right))
              (equalp (pg::sql-array-lower-bounds left) (pg::sql-array-lower-bounds right))
              (test-row-equal-p (pg::sql-array-elements left) (pg::sql-array-elements right))))
        ((or (pg::sql-record-p left) (pg::sql-record-p right))
         (and (pg::sql-record-p left) (pg::sql-record-p right)
              (test-row-equal-p (pg::sql-record-fields left) (pg::sql-record-fields right))
              (test-row-equal-p (pg::sql-record-values left) (pg::sql-record-values right))))
        ((or (pg::sql-jsonb-p left) (pg::sql-jsonb-p right))
         (and (pg::sql-jsonb-p left) (pg::sql-jsonb-p right)
              (test-row-equal-p (pg::sql-jsonb-value left) (pg::sql-jsonb-value right))))
        ((or (stringp left) (stringp right))
         (and (stringp left) (stringp right) (string= left right)))
        ((or (consp left) (consp right))
         (and (consp left) (consp right)
              (test-row-equal-p (car left) (car right))
              (test-row-equal-p (cdr left) (cdr right))))
        ((or (vectorp left) (vectorp right))
         (and (vectorp left) (vectorp right) (= (length left) (length right))
              (loop for a across left for b across right always (test-row-equal-p a b))))
        (t (equalp left right))))

(deftest row-comparison-preserves-string-case
    (ok (test-row-equal-p (list (vector "A" #(1 "b"))) (list (vector "A" #(1.0 "b")))))
  (ok (not (test-row-equal-p (list (vector "A")) (list (vector "a")))))
  (ok (not (test-row-equal-p (list (vector #(1 "A"))) (list (vector #(1 "a"))))))
  (ok (not (test-row-equal-p (list (vector (text-array '("A"))))
                             (list (vector (text-array '("a")))))))
  (ok (not (test-row-equal-p (pg::make-sql-record :fields '(("x" :text)) :values #("A"))
                             (pg::make-sql-record :fields '(("x" :text)) :values #("a")))))
  (ok (not (test-row-equal-p (pg::make-sql-jsonb '(:object ("k" . "A")))
                             (pg::make-sql-jsonb '(:object ("k" . "a")))))))

(defun text-array (elements)
  "Build an expected one-dimensional SQL text array with PostgreSQL bounds."
  (pg::make-sql-array :element-type-id :text :elements (coerce elements 'vector)
                      :dimensions (when (plusp (length elements)) (list (length elements)))
                      :lower-bounds (when (plusp (length elements)) '(1))))

(defmacro sqlstate-of (&body body)
  "Return a PostgreSQL error's SQLSTATE, NIL on success; propagate other errors."
  `(handler-case (progn ,@body nil)
     (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))

(deftest sqlstate-observation-keeps-error-boundary
    (let ((calls 0))
      (ok (null (sqlstate-of (incf calls))))
      (ok (= 1 calls)))
  (ok (equal "22003" (sqlstate-of (error 'pg::pg-error :sqlstate "22003" :message "out of range"))))
  (ok (handler-case (progn (sqlstate-of (error "unexpected host error")) nil)
        (simple-error () t))))

(defun call-with-standard-slot-fixture (function)
  (let* ((directory (merge-pathnames (format nil "pg-native-slot-~a/" (gensym))
                                     (uiop:temporary-directory)))
         (pg::*pgdata* directory)
         (pg::*backend* (pg::make-backend))
         (pg::*catalog* (pg::make-catalog))
         (pg::*wal-ready* t) (pg::*wal-poisoned* nil)
         (pg::*wal-durable-lsn* 128) (pg::*wal-end-lsn* 128)
         (pg::*next-xid* 10) (pg::*active-xids* '(7))
         (pg::*xid-wrap-pending* nil) (pg::*xid-wrap-failure* nil)
         (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (pg::*wal-replication-slots* (make-hash-table :test #'equal))
         (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (pg::*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
         (pg::*active-snapshots* 0) (pg::*active-snapshot-set* (make-hash-table :test #'eq))
         (pg::*schema-read-lock-count* 0) (pg::*schema-write-lock-waiters* 0)
         (pg::*schema-write-lock-owners* nil)
         (pg::*schema-read-lock-held* nil) (pg::*schema-write-lock-held* nil)
         (pg::*logical-test-failpoint-hook* nil))
    (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*))
          (pg::make-database :name "postgres" :oid 16384)
          (gethash 7 pg::*wal-xid-first-lsn*) 32)
    (pg::wal-ensure-directory-durable directory)
    (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
    (unwind-protect (funcall function)
      (pg::release-backend-transaction-snapshot pg::*backend*)
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(defun prepare-standard-use-backend ()
  (setf (pg::backend-explicit pg::*backend*) t
        (pg::backend-read-only pg::*backend*) t
        (pg::backend-isolation-level pg::*backend*) :repeatable-read)
  (pg::set-backend-state pg::*backend* :in-progress))

(deftest standard-slot-first-durable-image
    (call-with-standard-slot-fixture
     (lambda ()
       (multiple-value-bind (boundary token) (pg::create-standard-logical-slot "native_slot" :nothing)
         (ok (= 128 boundary))
         (ok (null token)))
       (let* ((slot (gethash "native_slot" pg::*wal-replication-slots*))
              (path (pg::wal-replication-slot-path "native_slot"))
              (decoded (pg::wal-read-replication-slot path)))
         (ok (string= "pgoutput" (pg::wal-replication-slot-plugin decoded)))
         (ok (= 128 (pg::wal-replication-slot-confirmed-flush-lsn decoded)))
         (ok (= 32 (pg::wal-replication-slot-restart-lsn decoded)))
         (ok (equal '(32) (gethash 7 (pg::wal-replication-slot-pending-xids slot))))
         (ok (null (pg::wal-replication-slot-active-owner slot))))
       (ok (zerop pg::*active-snapshots*))
       (ok (zerop pg::*schema-read-lock-count*))
       (ok (null (pg::backend-first-snapshot-set pg::*backend*))))))

(deftest slot-create-response-serialization
    (call-with-standard-slot-fixture
     (lambda ()
       (let ((original (symbol-function 'pg::send-create-replication-slot-result)) (sent nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::send-create-replication-slot-result)
                      (lambda (&rest arguments)
                        (ok (sb-thread:holding-mutex-p pg::*logical-slot-lifecycle-lock*))
                        (ok (equal "pgoutput" (fifth arguments)))
                        (setf sent t)
                        (error "injected response failure")))
                (ok
                 (handler-case
                     (progn
                       (pg::create-replication-slot-command nil
                                                            (pg::make-logical-replication-command
                                                             :type :create :slot-name "native_slot"
                                                             :plugin "pgoutput" :snapshot-action
                                                             :nothing))
                       nil)
                   (error nil t))))
           (setf (symbol-function 'pg::send-create-replication-slot-result) original))
         (ok sent)
         (ok (not (sb-thread:holding-mutex-p pg::*logical-slot-lifecycle-lock*)))
         (ok (gethash "native_slot" pg::*wal-replication-slots*))))))

(deftest standard-system-identifier-projection
    (dolist
        (case
            '(("" 14695981039346656037) ("a" 12638187200555641996) ("hello" 11831194018420276491)
              ("λ" 780254166137709982) ("legacy arbitrary identity" 9738095430658919642)))
      (let* ((identity (first case))
             (catalog (pg::make-catalog :cluster-identity identity))
             (decoded (pg::catalog-from-sexp (pg::catalog-to-sexp catalog))))
        (ok (= (second case) (pg::catalog-system-identifier catalog)))
        (ok (= (second case) (pg::catalog-system-identifier decoded)))
        (ok (equal identity (pg::catalog-cluster-identity decoded))))))

(deftest standard-slot-publication-excludes-writers
    (call-with-standard-slot-fixture
     (lambda ()
       (let* ((slots pg::*wal-replication-slots*)
              (attempted (sb-thread:make-semaphore))
              (published (sb-thread:make-semaphore))
              (acquired nil) (writer nil)
              (pg::*logical-test-after-snapshot-capture-hook*
                (lambda (name boundary)
                  (declare (ignore name boundary))
                  (setf writer
                        (sb-thread:make-thread
                         (lambda ()
                           (let ((pg::*wal-replication-slots* slots))
                             (setf acquired (sb-thread:grab-mutex pg::*write-lock* :waitp nil))
                             (when acquired (sb-thread:release-mutex pg::*write-lock*))
                             (sb-thread:signal-semaphore attempted)
                             (sb-thread:wait-on-semaphore published)
                             (pg::with-write-lock
                               (sb-thread:with-mutex (pg::*wal-lock*)
                                 (pg::wal-slot-record-xid-batch-under-wal-lock 11 160)))))
                         :name "standard-slot-post-boundary-writer"))
                  (sb-thread:wait-on-semaphore attempted))))
         (unwind-protect
              (progn
                (pg::create-standard-logical-slot "native_slot" :nothing)
                (ok (null acquired) "writer admission stays closed through slot publication"))
           (sb-thread:signal-semaphore published)
           (when writer (sb-thread:join-thread writer)))
         (ok (equal '(160)
                    (gethash 11 (pg::wal-replication-slot-pending-xids
                                 (gethash "native_slot" slots))))
             "post-boundary XID is retained by the published slot")))))

(deftest standard-slot-parser-rejections
    (dolist
        (case
            '(("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (snapshot 'nothing', snapshot 'use')"
               "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (snapshot 'USE')" "22023")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (snapshot)" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (two_phase '0')" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (two_phase yes)" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (two_phase true)" "0A000")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (failover)" "0A000")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (reserve_wal false)" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (snapshot 'nothing',)" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput ()" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput EXPORT_SNAPSHOT USE_SNAPSHOT" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pg_lisp (snapshot 'nothing')" "0A000")
              ("CREATE_REPLICATION_SLOT s TEMPORARY LOGICAL pgoutput" "0A000")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput;;" "42601")
              ("CREATE_REPLICATION_SLOT s LOGICAL pgoutput (snapshot 'use)" "42601")
              ("CREATE_REPLICATION_SLOT \"s\"\"bad\" LOGICAL pgoutput" "42602")
              ("CREATE_REPLICATION_SLOT 's' LOGICAL pgoutput" "42601")))
      (ok
       (equal (second case) (sqlstate-of (pg::parse-replication-command (first case))))
       (first case))))

(deftest standard-slot-parser-options
    (dolist
        (case
            '(("CREATE_REPLICATION_SLOT Native_Slot LOGICAL pgoutput (SNAPSHOT 'nothing', TWO_PHASE false, FAILOVER false);"
               :nothing "pgoutput")
              ("CREATE_REPLICATION_SLOT \"native_slot\" LOGICAL \"pgoutput\" (SNAPSHOT 'use')" :use
               "pgoutput")
              ("CREATE_REPLICATION_SLOT native_slot LOGICAL pgoutput NOEXPORT_SNAPSHOT" :nothing
               "pgoutput")
              ("CREATE_REPLICATION_SLOT native_slot LOGICAL pgoutput USE_SNAPSHOT" :use "pgoutput")
              ("CREATE_REPLICATION_SLOT native_slot LOGICAL pg_lisp" :export "pg_lisp")))
      (let ((command (pg::parse-replication-command (first case))))
        (ok (equal "native_slot" (pg::logical-replication-command-slot-name command)))
        (ok (eq (second case) (pg::logical-replication-command-snapshot-action command)))
        (ok (equal (third case) (pg::logical-replication-command-plugin command)))))
  (ok (equal "s''napshot" (pg::replication-command-value "'s''''napshot'")))
  (ok (equal "p\"lugin" (pg::replication-command-identifier "\"p\"\"lugin\"")))
  (ok
   (equal "native_slot"
          (pg::logical-replication-command-slot-name
           (pg::parse-replication-command "DROP_REPLICATION_SLOT \"native_slot\"")))))

(deftest standard-use-transaction-owns-snapshot
    (call-with-standard-slot-fixture
     (lambda ()
       (prepare-standard-use-backend)
       (pg::create-standard-logical-slot "native_slot" :use)
       (let* ((registration (pg::backend-transaction-snapshot-registration pg::*backend*))
              (old-slot (gethash "native_slot" pg::*wal-replication-slots*)))
         (ok (pg::snapshot-registration-active-p registration))
         (ok (= 1 pg::*active-snapshots*))
         (ok (= 1 pg::*schema-read-lock-count*))
         (let ((pg::*schema-write-lock-waiters* 1))
           (sb-ext:with-timeout 1
             (pg::with-schema-read-lock (ok (= 1 pg::*schema-read-lock-count*)))))
         (ok (handler-case (pg::with-schema-write-lock nil)
               (pg::pg-error (condition)
                 (string= "25001" (pg::pg-error-sqlstate condition)))))
         (pg::wal-drop-replication-slot "native_slot")
         (ok (pg::snapshot-registration-active-p registration))
         (pg::create-standard-logical-slot "native_slot" :nothing)
         (let* ((new-slot (gethash "native_slot" pg::*wal-replication-slots*)))
           (ok (not (eq old-slot new-slot)))
           (pg::abort-transaction-block)
           (ok (eq new-slot (gethash "native_slot" pg::*wal-replication-slots*)))
           (ok (not (pg::snapshot-registration-active-p registration)))
           (ok (zerop pg::*active-snapshots*))
           (ok (zerop pg::*schema-read-lock-count*))
           (ok (eq :aborted (pg::backend-state pg::*backend*)))
           (ok (pg::backend-explicit pg::*backend*)))))))

(deftest standard-slot-error-cleans-resources
    (dolist (stage '(:before-initial-slot-publication :after-initial-slot-file-publication
                     :after-standard-snapshot-installation))
      (call-with-standard-slot-fixture
       (lambda ()
         (prepare-standard-use-backend)
         (let ((pg::*logical-test-failpoint-hook*
                 (lambda (point &rest details)
                   (declare (ignore details))
                   (when (eq point stage) (error "injected standard-slot failure")))))
           (ok (handler-case (progn (pg::create-standard-logical-slot "native_slot" :use) nil)
                 (error () t))))
         (pg::note-query-error)
         (ok (zerop pg::*active-snapshots*))
         (ok (zerop pg::*schema-read-lock-count*))
         (let ((slot (gethash "native_slot" pg::*wal-replication-slots*)))
           (if (eq stage :before-initial-slot-publication)
               (ok (null slot))
               (progn (ok (= 128 (pg::wal-replication-slot-confirmed-flush-lsn slot)))
                      (ok (null (pg::wal-replication-slot-active-owner slot)))
                      (ok (probe-file (pg::wal-replication-slot-path "native_slot"))))))))))

(deftest standard-slot-uncertain-durability-retains-wal
    (call-with-standard-slot-fixture
     (lambda ()
       (let ((original (symbol-function 'pg::fsync-directory)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::fsync-directory)
                      (lambda (directory)
                        (if (equal directory (pg::wal-replication-slot-directory))
                            (error "injected directory fsync failure")
                            (funcall original directory))))
                (ok (handler-case (progn (pg::create-standard-logical-slot "native_slot" :nothing) nil)
                      (error () t))))
           (setf (symbol-function 'pg::fsync-directory) original)))
       (let* ((slot (gethash "native_slot" pg::*wal-replication-slots*))
              (path (pg::wal-replication-slot-path "native_slot"))
              (decoded (pg::wal-read-replication-slot path)))
         (ok (null (pg::wal-replication-slot-active-owner slot)))
         (ok (= 32 (pg::wal-replication-slot-restart-lsn slot)))
         (ok (= 128 (pg::wal-replication-slot-confirmed-flush-lsn decoded))))
       (ok (zerop pg::*active-snapshots*))
       (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*))))))

(deftest slot-plugin-codec-and-migration
    (let* ((directory (merge-pathnames (format nil "pg-slot-format-~a/" (gensym))
                                       (uiop:temporary-directory)))
           (path (merge-pathnames "codec_slot.slot" directory))
           (pg::*wal-durable-lsn* 1000))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (dolist (plugin '("pg_lisp" "pgoutput"))
               (let* ((confirmed (when (string= plugin "pgoutput") 200))
                      (slot (pg::make-wal-replication-slot
                             :name "codec_slot" :database-oid 16384 :restart-lsn 100
                             :consistent-lsn 200 :confirmed-flush-lsn confirmed :plugin plugin))
                      (bytes (pg::wal-encode-replication-slot slot)))
                 (with-open-file (stream path :direction :output :if-exists :supersede
                                              :element-type '(unsigned-byte 8))
                   (write-sequence bytes stream))
                 (let ((decoded (pg::wal-read-replication-slot path)))
                   (ok (string= plugin (pg::wal-replication-slot-plugin decoded)))
                   (ok (eql confirmed (pg::wal-replication-slot-confirmed-flush-lsn decoded))))))
             (dolist (prefix '("PGSLOT1 codec_slot 100 200"
                               "PGSLOT2 codec_slot 100 200 - reserved"
                               "PGSLOT3 codec_slot 16384 100 200 - reserved"))
               (let ((crc (pg::wal-crc32 (pg::string->bytes prefix))))
                 (with-open-file (stream path :direction :output :if-exists :supersede)
                   (format stream "~a ~8,'0X~%" prefix crc))
                 (let ((decoded (pg::wal-read-replication-slot path)))
                   (ok (string= "pg_lisp" (pg::wal-replication-slot-plugin decoded)))
                   (ok (null (pg::wal-replication-slot-confirmed-flush-lsn decoded))))))
             (dolist (prefix '("PGSLOT4 codec_slot 16384 100 200 200 reserved unknown"
                               "PGSLOT4 codec_slot 16384 100 200 - reserved pgoutput"
                               "PGSLOT4 codec_slot - 100 200 200 reserved pgoutput"))
               (let ((crc (pg::wal-crc32 (pg::string->bytes prefix))))
                 (with-open-file (stream path :direction :output :if-exists :supersede)
                   (format stream "~a ~8,'0X~%" prefix crc))
                 (ok (handler-case (progn (pg::wal-read-replication-slot path) nil)
                       (error () t)))))
             (ok (handler-case
                     (progn (pg::wal-encode-replication-slot
                             (pg::make-wal-replication-slot
                              :name "codec_slot" :plugin "pgoutput" :database-oid 16384
                              :restart-lsn 100 :consistent-lsn 200))
                            nil)
                   (error () t))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(deftest slot-binary-profile-durable-and-fail-closed
    (let* ((directory (merge-pathnames (format nil "pg-slot-profile-~a/" (gensym))
                                       (uiop:temporary-directory)))
           (path (merge-pathnames "codec_slot.slot" directory))
           (pg::*wal-durable-lsn* 1000)
           (slot (pg::make-wal-replication-slot :name "codec_slot" :database-oid 16384
                                                :restart-lsn 100 :consistent-lsn 200)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (ok (eq :text (pg::wal-slot-profile slot)))
             (pg::wal-slot-set-initial-profile slot :binary-bytea)
             (ok (handler-case (progn (pg::wal-slot-set-initial-profile slot :text) nil) (error () t)))
             (with-open-file (stream path :direction :output :if-exists :supersede
                                          :element-type '(unsigned-byte 8))
               (write-sequence (pg::wal-encode-replication-slot slot) stream))
             (ok (eq :binary-bytea (pg::wal-slot-profile (pg::wal-read-replication-slot path))))
             (dolist (prefix '("PGSLOT5 codec_slot 16384 100 200 - reserved pg_lisp unknown"
                               "PGSLOT5 codec_slot 16384 100 200 200 reserved pgoutput binary-bytea"))
               (with-open-file (stream path :direction :output :if-exists :supersede)
                 (format stream "~a ~8,'0X~%" prefix (pg::wal-crc32 (pg::string->bytes prefix))))
               (ok (handler-case (progn (pg::wal-read-replication-slot path) nil) (error () t)))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(deftest slot-binary-profile-survives-ack-and-loss
    (let* ((slot (pg::make-wal-replication-slot :name "binary_slot" :database-oid 16384
                                                :restart-lsn 100 :consistent-lsn 200))
           (old-save (symbol-function 'pg::wal-save-replication-slot))
           (saved nil)
           (pg::*wal-durable-lsn* 1000)
           (pg::*logical-capture-live-slots* nil)
           (pg::*max-slot-wal-keep-bytes* 1)
           (pg::*wal-replication-slots* (make-hash-table :test #'equal)))
      (pg::wal-slot-set-initial-profile slot :binary-bytea)
      (setf (gethash "binary_slot" pg::*wal-replication-slots*) slot)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::wal-save-replication-slot)
                   (lambda (candidate)
                     (push (pg::bytes->string (pg::wal-encode-replication-slot candidate)) saved)))
             (pg::wal-persist-replication-slot-ack-under-lock slot 200)
             (pg::wal-invalidate-lagging-replication-slots-under-wal-lock pg::*max-slot-wal-keep-bytes*)
             (ok (= 2 (length saved)))
             (ok (every (lambda (line) (and (search "PGSLOT5 " line)
                                            (search " binary-bytea " line))) saved))
             (ok (eq :lost (pg::wal-replication-slot-wal-status slot))))
        (setf (symbol-function 'pg::wal-save-replication-slot) old-save))))

(deftest slot-binary-profile-create-parsing
    (let ((command (pg::parse-replication-command
                    "CREATE_REPLICATION_SLOT bin_slot LOGICAL pg_lisp (snapshot 'export', binary_bytea 'true')")))
      (ok (assoc "binary_bytea" (pg::logical-replication-command-options command) :test #'equal))
      (ok (eq :export (pg::logical-replication-command-snapshot-action command))))
  (ok (handler-case
          (progn (pg::parse-replication-command
                  "CREATE_REPLICATION_SLOT bin_slot LOGICAL pgoutput (snapshot 'export', binary_bytea 'true')")
                 nil)
        (pg::pg-error () t))))

(defun call-with-publication-srf-fixture (function)
  (let* ((pg::*backend* (pg::make-backend))
         (pg::*catalog* (pg::make-catalog))
         (database (pg::make-database :name "postgres"))
         (alpha (pg::make-pg-class :oid 201 :name "alpha"
                                   :columns (vector (pg::make-pg-column :name "id" :type :int4 :attnum 1)
                                                    (pg::make-pg-column :name "label" :type :varchar :attnum 2))))
         (beta (pg::make-pg-class :oid 202 :name "beta"
                                  :columns (vector (pg::make-pg-column :name "id" :type :int4 :attnum 1)))))
    (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
          (gethash "alpha" (pg::database-tables database)) alpha
          (gethash "beta" (pg::database-tables database)) beta
          (gethash "p" (pg::database-publications database))
          (pg::make-pg-publication :oid 101 :name "p"
                                   :tables (copy-tree '((:name "beta" :table-oid 202) (:name "alpha" :table-oid 201))))
          (gethash "q" (pg::database-publications database))
          (pg::make-pg-publication :oid 102 :name "q" :tables (copy-tree '((:name "alpha" :table-oid 201))))
          (gethash "all" (pg::database-publications database))
          (pg::make-pg-publication :oid 103 :name "all" :all-tables-p t))
    (funcall function database)))

(deftest discovery-catalogs-follow-stored-objects
    (call-with-publication-srf-fixture
     (lambda (database)
       (setf (gethash "alpha_pkey" (pg::database-indexes database))
             (pg::make-pg-index :oid 203 :name "alpha_pkey" :table-oid 201)
             (gethash "catalog_counter" (pg::database-sequences database))
             (pg::make-pg-sequence :oid 210 :name "catalog_counter"))
       (ok (test-row-equal-p '(#(11 "pg_catalog") #(99 "pg_toast") #(2200 "public"))
                             (derived-query-rows "SELECT oid,nspname FROM pg_catalog.pg_namespace ORDER BY oid")))
       (ok (test-row-equal-p (list (vector "alpha" (char-code #\r))
                                   (vector "alpha_pkey" (char-code #\i))
                                   (vector "beta" (char-code #\r))
                                   (vector "catalog_counter" (char-code #\S)))
                             (derived-query-rows
                              "SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' ORDER BY c.relname")))
       (ok (test-row-equal-p '(#(201)) (derived-query-rows "SELECT oid FROM pg_class WHERE relname='alpha'")))
       (remhash "alpha_pkey" (pg::database-indexes database))
       (ok (null (derived-query-rows "SELECT relname FROM pg_class WHERE relname='alpha_pkey'"))))))

(deftest discovery-sequence-view-uses-publication-selection
    (call-with-publication-srf-fixture
     (lambda (database)
       (let ((publication (gethash "q" (pg::database-publications database))))
         (setf (gethash "catalog_counter" (pg::database-sequences database))
               (pg::make-pg-sequence :oid 210 :name "catalog_counter")
               (pg::pg-publication-sequences publication) :all)
         (ok (test-row-equal-p '(#("q" "public" "catalog_counter"))
                               (derived-query-rows "SELECT pubname,schemaname,sequencename FROM pg_publication_sequences")))
         (setf (gethash "later_counter" (pg::database-sequences database))
               (pg::make-pg-sequence :oid 211 :name "later_counter"))
         (ok (= 2 (length (derived-query-rows "SELECT sequencename FROM pg_publication_sequences"))))
         (remhash "catalog_counter" (pg::database-sequences database))
         (ok (test-row-equal-p '(#("later_counter"))
                               (derived-query-rows "SELECT sequencename FROM pg_publication_sequences")))))))

(deftest discovery-catalog-result-origins
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (dolist (case '(("SELECT relkind FROM pg_class" 1259 18 18)
                       ("SELECT nspname FROM pg_namespace" 2615 2 19)
                       ("SELECT sequencename FROM pg_publication_sequences" 12073 3 19)))
         (let* ((ast (first (pg::parse-statements (first case))))
                (plan (pg::plan-select-query ast))
                (target (first (getf (pg::query-plan-prepared plan) :targets)))
                (column (pg::target->result-column target)))
           (ok (= (second case) (pg::result-column-table-oid column)))
           (ok (= (third case) (pg::result-column-attnum column)))
           (ok (= (fourth case) (pg::result-column-type-oid column))))))))

(deftest catalog-oid-numeric-predicates
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (dolist (sql '("SELECT oid FROM pg_namespace WHERE oid=11"
                      "SELECT oid FROM pg_namespace WHERE 11=oid"
                      "SELECT oid FROM pg_namespace WHERE oid IN (11,12)"
                      "SELECT oid FROM pg_namespace WHERE oid IN (11)"
                      "SELECT oid FROM pg_namespace WHERE oid=11::bigint"))
         (ok (test-row-equal-p '(#(11)) (derived-query-rows sql)) sql))
       (ok (test-row-equal-p '(#(11) #(2200))
                             (derived-query-rows "SELECT oid FROM pg_namespace WHERE oid IN (11,2200) ORDER BY oid")))
       (ok (null (derived-query-rows "SELECT oid FROM pg_namespace WHERE oid=-1"))))))

(deftest catalog-oid-explicit-casts
    (ok (test-row-equal-p '(#(11 4294967295 -1 4294967295 4294967295))
                          (derived-query-rows "SELECT 11::oid,'-1'::oid,'4294967295'::oid::int4,'4294967295'::oid::bigint,(-1::int4)::oid")))
  (dolist (case '(("SELECT (-1::bigint)::oid" "22003")
                  ("SELECT true::oid" "42846")
                  ("SELECT 1::oid+1" "42883")
                  ("SELECT -(1::oid)" "42883")))
    (ok (equal (second case) (sqlstate-of (derived-query-rows (first case))))
        (first case))))

(deftest catalog-slot-lsn-values-are-numeric
    (let* ((row (pg::sql-replication-slot-row
                 (vector "test_slot" nil "F/FFFFFFFF" pg::+null+ 123 "reserved" "pg_lisp" 42)))
           (restart (aref row 2)))
      (ok (= restart #xfffffffff))
      (ok (eq (aref row 3) pg::+null+))
      (ok (string= (aref row 4) "reserved"))
      (ok (= (aref row 5) 123))
      (ok (equal (loop for n from 1 to 23 collect n)
                 (mapcar #'fourth (getf (pg::sql-system-relation-definition "pg_replication_slots") :fields))))
      (ok (= -1 (pg::compare-3way restart (pg::sql-pg-lsn-input "10/0")))))
  (ok (test-row-equal-p '(#(t)) (derived-query-rows "SELECT 'F/FFFFFFFF'::pg_lsn<'10/0'::pg_lsn")))
  (ok (test-row-equal-p '(#("0/00000001")) (derived-query-rows "SELECT '0/1'::pg_lsn::text")))
  (dolist (text '(" 0/1" "+0/1" "0x1/1" "000000000/1" "0/1 " "١/1"))
    (ok (equal "22P02" (sqlstate-of (pg::sql-pg-lsn-input text))))))

(deftest standard-copy-catalog-column-identities
    (let* ((class-fields (getf (pg::sql-system-relation-definition "pg_class") :fields))
           (database-fields (getf (pg::sql-system-relation-definition "pg_database") :fields)))
      (ok (= 28 (fourth (assoc "relispartition" class-fields :test #'string=))))
      (ok (equal '(("oid" :oid t 1) ("datname" :name t 2)) database-fields))))

(deftest star-expansion-keeps-merged-join-expressions
    (ok (test-row-equal-p '(#(1) #(2))
                          (derived-query-rows
                           "SELECT * FROM (SELECT 1 AS id) a FULL JOIN (SELECT 2 AS id) b USING (id) ORDER BY id")))
  (ok (equal "42702" (sqlstate-of (derived-query-rows
                                   "SELECT id,a.id,b.id FROM (SELECT 1 AS id) a FULL JOIN (SELECT 2 AS id) b USING (id) ORDER BY id")))))

(deftest publication-srf-expanded-records
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (ok (test-row-equal-p (list (vector 101 201 #(1 2) pg::+null+)
                                   (vector 101 202 #(1) pg::+null+))
                             (derived-query-rows "SELECT (pg_get_publication_tables('p')).*")))
       (ok (test-row-equal-p (list (vector 9 #(1 2)) (vector 9 #(1)))
                             (derived-query-rows "SELECT 9, (pg_get_publication_tables('p')).attrs")))
       (ok (test-row-equal-p (list #(201 201) (vector 202 pg::+null+))
                             (derived-query-rows "SELECT (pg_get_publication_tables('p')).relid, (pg_get_publication_tables('q')).relid")))
       (ok (= 4 (length (derived-query-rows "SELECT (pg_get_publication_tables('p','p')).*"))))
       (ok (= 1 (length (derived-query-rows "SELECT (pg_get_publication_tables('p')).* LIMIT 1"))))
       (ok (null (derived-query-rows "SELECT (pg_get_publication_tables(VARIADIC NULL)).*")))
       (ok (test-row-equal-p '(#(201) #(202))
                             (derived-query-rows "SELECT (pg_get_publication_tables('all')).relid"))))))

(deftest publication-srf-record-distinct-and-sort
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (let ((directory (merge-pathnames (format nil "pg-record-srf-~a/" (gensym))
                                         (uiop:temporary-directory))))
         (ensure-directories-exist directory)
         (unwind-protect
              (dolist (memory '(1048576 1))
                (dolist (arena '(nil 1048576))
                  (let ((pg::*query-temp-directory* directory)
                        (pg::*query-work-memory-bytes* memory)
                        (pg::*query-sort-arena-limit-bytes* arena))
                    (let* ((rows (derived-query-rows
                                  "SELECT DISTINCT pg_get_publication_tables('p','p') AS r ORDER BY r DESC"))
                           (records (mapcar (lambda (row) (aref row 0)) rows)))
                      (ok (= 2 (length records)))
                      (ok (every #'pg::sql-record-p records))
                      (ok (equal '(202 201)
                                 (mapcar (lambda (record)
                                           (aref (pg::sql-record-values record) 1))
                                         records))))
                    (ok (= 1 (length (derived-query-rows
                                      "SELECT DISTINCT pg_get_publication_tables('p','p') AS r ORDER BY r LIMIT 1"))))
                    (ok (null (uiop:directory-files directory))))))
           (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))))

(deftest publication-srf-order-reuses-expanded-field
    (call-with-publication-srf-fixture
     (lambda (database)
       (setf (gethash "field_calls" (pg::database-sequences database))
             (pg::make-pg-sequence :name "field_calls" :oid 9905))
       (let* ((sql "SELECT (pg_get_publication_tables(nextval('field_calls')::text)).* ORDER BY relid")
              (prepared (pg::query-plan-prepared
                         (pg::plan-select-query (first (pg::parse-statements sql))))))
         (ok (= 1 (length (getf prepared :set-functions))))
         (let* ((order (caar (getf prepared :order)))
                (values (vector 1001 2002 3003 4004))
                (row (cons values #())))
           (ok (= (pg::select-order-value order row) (aref values 1))
               "ORDER reads the already projected relid without reevaluating the SRF"))))))

(deftest nested-spill-scope-preserves-consumer-files
    (let* ((directory (merge-pathnames (format nil "pg-nested-spill-~a/" (gensym))
                                       (uiop:temporary-directory)))
           (pg::*query-temp-directory* directory)
           (quota (pg::make-query-temp-quota 1048576))
           (pg::*query-temp-quota* quota)
           (pathname nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (pg::with-query-temp-files
               (pg::with-query-temp-files
                 (pg::with-query-temp-output (path stream)
                   (setf pathname path)
                   (pg::query-spill-write-record stream #(1 "kept"))))
               (ok (probe-file pathname))
               (with-open-file (stream pathname :external-format :utf-8)
                 (ok (equalp #(1 "kept") (pg::query-spill-read-record stream)))))
             (ok (null (probe-file pathname)))
             (ok (zerop (pg::query-temp-quota-used-bytes quota)))
             (ok (null (uiop:directory-files directory)))
             (ok (handler-case
                     (pg::with-query-temp-files
                       (pg::with-query-temp-files
                         (pg::with-query-temp-output (path stream)
                           (setf pathname path)
                           (pg::query-spill-write-record stream #(2 "error")))
                         (error "consumer failed")))
                   (error () t)))
             (ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(deftest publication-srf-aggregate-input
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (let* ((sql "SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).* FROM pg_catalog.pg_publication WHERE pubname IN ('p')")
              (plan (pg::plan-select-query (first (pg::parse-statements sql))))
              (targets (getf (pg::query-plan-prepared plan) :targets)))
         (ok (= 1 (length (getf (pg::query-plan-prepared plan) :set-functions))))
         (ok (equal '(26 26 22 194)
                    (mapcar (lambda (target) (pg::result-column-type-oid (pg::target->result-column target))) targets)))
         (ok (equal '("pubid" "relid" "attrs" "qual") (mapcar (lambda (target) (getf target :name)) targets)))
         (ok (search "ProjectSet" (pg::format-query-plan plan)))
         (ok (= 2 (length (derived-query-rows sql))))))))

(deftest publication-srf-keeps-volatile-aggregate-identities
    (let* ((sql "SELECT (pg_get_publication_tables(VARIADIC array_agg(nextval('srf_calls'::text)::text))).* FROM (SELECT 1) AS input")
           (ast (first (pg::parse-statements sql)))
           (prepared (pg::query-plan-prepared (pg::plan-select-query ast))))
      (ok (= 4 (length (getf prepared :aggregates))))
      (ok (= 4 (length (getf prepared :set-functions))))))

(deftest publication-srf-refuses-unsupported-requests
    (call-with-publication-srf-fixture
     (lambda (database)
       (dolist (case '(("SELECT (pg_get_publication_tables()).*" "42883")
                       ("SELECT (pg_get_publication_tables('missing')).*" "42704")
                       ("SELECT (pg_get_publication_tables(NULL)).*" "22004")
                       ("SELECT 1 WHERE pg_get_publication_tables('p') IS NULL" "0A000")
                       ("SELECT array_agg(pg_get_publication_tables('p'))" "0A000")))
         (ok (equal (second case) (sqlstate-of (derived-query-rows (first case))))))
       (setf (getf (first (pg::pg-publication-tables
                           (gethash "q" (pg::database-publications database)))) :filter) '(:bool t))
       (ok (equal "0A000" (sqlstate-of (derived-query-rows "SELECT (pg_get_publication_tables('q')).*")))))))

(deftest publication-oids-roundtrip-and-upgrade
    (let* ((catalog (pg::make-catalog :next-oid 500))
           (database (pg::make-database :name "postgres"))
           (first (pg::make-pg-publication :name "a"))
           (second (pg::make-pg-publication :name "b")))
      (setf (gethash "postgres" (pg::catalog-databases catalog)) database
            (gethash "b" (pg::database-publications database)) second
            (gethash "a" (pg::database-publications database)) first)
      (ok (pg::catalog-upgrade-publication-oids catalog))
      (ok (= 500 (pg::pg-publication-oid first)))
      (ok (= 501 (pg::pg-publication-oid second)))
      (ok (= 502 (pg::catalog-next-oid catalog)))
      (ok (null (pg::catalog-upgrade-publication-oids catalog)))
      (ok (= 500 (pg::pg-publication-oid
                  (pg::publication-from-sexp (pg::publication-to-sexp first)))))))

(deftest array-aggregate-spill-and-cleanup
    (let* ((directory (merge-pathnames (format nil "pg-array-agg-~a/" (gensym))
                                       (uiop:temporary-directory)))
           (descriptors '((:kind :count)
                          (:kind :array-agg :input (:col 2 :type :text))))
           (input (list (vector 1 "a") (vector 2 "x") (vector 1 pg::+null+)
                        (vector 2 "y") (vector 1 "b"))))
      (ensure-directories-exist directory)
      (unwind-protect
           (dolist (memory '(1048576 1))
             (let ((pg::*query-temp-directory* directory)
                   (pg::*query-work-memory-bytes* memory)
                   (output nil))
               (pg::stream-sorted-aggregates
                (lambda (callback) (dolist (row input) (funcall callback row)))
                '((:col 1 :type :int4)) descriptors
                (lambda (row) (push row output)))
               (ok (equalp (list (vector 1 3 (text-array (list "a" pg::+null+ "b")))
                                 (vector 2 2 (text-array '("x" "y"))))
                           (nreverse output)))
               (ok (null (uiop:directory-files directory)))
               (let ((count 0))
                 (pg::stream-sorted-aggregates
                  (lambda (callback) (dolist (row input) (funcall callback row)))
                  '((:col 1 :type :int4)) descriptors
                  (lambda (row) (declare (ignore row)) (incf count) :stop))
                 (ok (= 1 count)))
               (ok (null (uiop:directory-files directory)))
               (ok (handler-case
                       (progn
                         (pg::stream-sorted-aggregates
                          (lambda (callback) (dolist (row input) (funcall callback row)))
                          '((:col 1 :type :int4)) descriptors
                          (lambda (row) (declare (ignore row)) (error "consumer failed")))
                         nil)
                     (error () t)))
               (ok (null (uiop:directory-files directory)))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(deftest array-aggregate-scalar-and-empty
    (ok (test-row-equal-p (list (vector (text-array '("x")))) (derived-query-rows "SELECT array_agg('x'::text)")))
  (ok (test-row-equal-p (list (vector (text-array (list pg::+null+))))
                        (derived-query-rows "SELECT array_agg(NULL::text)")))
  (ok (test-row-equal-p (list (vector pg::+null+ 0))
                        (derived-query-rows "SELECT array_agg('x'::text), COUNT(*) WHERE false")))
  (ok (test-row-equal-p '(#("1" 2))
                        (derived-query-rows "SELECT CAST(COUNT(*) AS text), COUNT(*) + COUNT(*)")))
  (ok (test-row-equal-p '(#(3))
                        (derived-query-rows "SELECT length(CAST(array_agg('x'::text) AS text))"))))

(deftest aggregates-in-order-by
    (ok (test-row-equal-p '(#(1)) (derived-query-rows "SELECT 1 ORDER BY array_agg('x'::text)")))
  (ok (test-row-equal-p '(#(1)) (derived-query-rows "SELECT COUNT(*) ORDER BY array_agg('x'::text)")))
  (ok (equal "42P10" (sqlstate-of (derived-query-rows "SELECT DISTINCT 1 ORDER BY COUNT(*)")))))

(deftest distinct-aggregate-order-reuses-selected-values
    (ok (test-row-equal-p '(#(1)) (derived-query-rows "SELECT DISTINCT COUNT(*) ORDER BY COUNT(*)")))
  (dolist (sql '("SELECT DISTINCT array_agg('x'::text) ORDER BY array_agg('x'::text)"
                 "SELECT DISTINCT array_agg('x'::text) ORDER BY pg_catalog.array_agg('x'::text)"))
    (ok (test-row-equal-p (list (vector (text-array '("x")))) (derived-query-rows sql))))
  (ok (test-row-equal-p (list (vector (text-array '("x")) (text-array (list pg::+null+))))
                        (derived-query-rows "SELECT array_agg('x'::text),array_agg(NULL::text)"))))

(deftest array-invalid-operators
    (dolist (sql '("SELECT length(array_agg('x'::text))"
                   "SELECT array_agg('x'::text) + 1"))
      (ok (equal "42883" (sqlstate-of (derived-query-rows sql))))))

(deftest array-aggregate-analysis-errors
    (dolist (case '(("SELECT array_agg('x')" "42725")
                    ("SELECT array_agg(1)" "0A000")
                    ("SELECT array_agg(COUNT(*)::text)" "42803")
                    ("SELECT 1 WHERE array_agg('x'::text) IS NULL" "42803")
                    ("SELECT q.x, array_agg(q.x) FROM (SELECT 'x'::text AS x) q" "42803")))
      (ok (equal (second case) (sqlstate-of (derived-query-rows (first case)))))))

(deftest array-aggregate-result-metadata
    (let* ((plan (pg::plan-select-query (first (pg::parse-statements "SELECT array_agg('x'::text)"))))
           (target (first (getf (pg::query-plan-prepared plan) :targets)))
           (column (pg::target->result-column target)))
      (ok (= 1009 (pg::result-column-type-oid column)))
      (ok (= -1 (pg::result-column-typlen column)))
      (ok (string= "array_agg" (pg::result-column-name column)))
      (ok (find :sorted-aggregate (pg::collect-query-plan-nodes (pg::query-plan-physical plan)) :key #'first))))

(deftest array-text-output-escaping
    (ok (string= "{a,NULL,\"NULL\",\"\",\"a,b\",\"a b\"}"
                 (pg::sql-text-array-output (vector "a" pg::+null+ "NULL" "" "a,b" "a b")))))

(deftest cast-comparison-and-membership-types
    (dolist (sql '("SELECT 'x'::\"char\" IN ('x'::text, 'y'::text)"
                   "SELECT 'x'::\"char\" IN ('x'::\"char\", 'y'::text)"
                   "SELECT 'x'::\"char\" IN ('x'::\"char\", 'y'::\"char\")"
                   "SELECT '1 2'::int2vector IN ('1 2'::int2vector)"
                   "SELECT 'x'::\"char\" = 'x'"
                   "SELECT 'x'::\"char\" = 'x'::text"
                   "SELECT 'x'::\"char\" = 'x'::name"))
      (ok (test-row-equal-p '(#(t)) (derived-query-rows sql))))
  (dolist (sql '("SELECT 'x'::\"char\" > 0"
                 "SELECT '1 2'::int2vector = '1 2'::text"
                 "SELECT 'x'::\"char\" + 1"))
    (ok (equal "42883" (sqlstate-of (derived-query-rows sql)))))
  (ok (equal "42883" (sqlstate-of (derived-query-rows "SELECT 1 IN ('1'::text, '2'::text)")))))

(deftest cast-quoted-identifiers-are-columns
    (dolist (name '("true" "false" "null" "default" "current_user"))
      (ok (equal "42703" (sqlstate-of (derived-query-rows (format nil "SELECT CAST(\"~a\" AS text)" name)))))))

(deftest cast-name-strength
    (dolist (case '(("SELECT '1'::text::int4" "int4")
                    ("SELECT length('x')::text" "length")
                    ("SELECT q.a::text FROM (SELECT 1 AS a) q" "a")))
      (ok (string= (second case)
                   (getf (first (getf (pg::prepare-select
                                       (first (pg::parse-statements (first case)))) :targets))
                         :name)))))

(deftest variadic-call-syntax
    (let* ((ast (first (pg::parse-statements
                        "SELECT pg_catalog.pg_get_publication_tables(VARIADIC array_agg(pubname::text)) FROM pg_catalog.pg_publication")))
           (expression (getf (first (getf ast :targets)) :expr)))
      (ok (getf (cdddr expression) :variadic))
      (ok (string= "pg_catalog" (getf (cdddr expression) :schema)))
      (ok (eq :cast (first (first (third (first (third expression))))))))
  (ok (equal "42601" (sqlstate-of (pg::parse-statements "SELECT f(VARIADIC x, 1)"))))
  ;; C clears the marker when an exact nonvariadic signature matches.
  (ok (test-row-equal-p '(#(1)) (derived-query-rows "SELECT length(VARIADIC 'x')"))))

(deftest cast-unknown-literal-is-analyzed
    (ok (equal "22P02" (sqlstate-of (pg::prepare-select
                                     (first (pg::parse-statements "SELECT 'invalid'::int4 WHERE false")))))))

(deftest expression-cast-syntax-and-values
    (dolist (case '(("SELECT '42'::int4" 42)
                    ("SELECT CAST('42' AS integer)" 42)
                    ("SELECT '0x_FF'::int4" 255)
                    ("SELECT '-0b_100'::bigint" -4)
                    ("SELECT '1_000'::int8" 1000)
                    ("SELECT 'on'::bool" t)
                    ("SELECT false::int4" 0)
                    ("SELECT true::text" "true")
                    ("SELECT 'abcdefgh'::varchar(3)" "abc")
                    ("SELECT 'S'::\"char\"" 83)
                    ("SELECT '-128'::int4::\"char\"::int4" -128)))
      (ok (test-row-equal-p (list (vector (second case))) (derived-query-rows (first case))))))

(deftest expression-cast-errors
    (dolist (case '(("SELECT 9223372036854775808::int8" "22003")
                    ("SELECT '2147483648'::int4" "22003")
                    ("SELECT '9223372036854775808'::int8" "22003")
                    ("SELECT '0x__1'::int4" "22P02")
                    ("SELECT '_1'::int4" "22P02")
                    ("SELECT '2_'::int4" "22P02")
                    ("SELECT 'maybe'::bool" "22P02")
                    ("SELECT '32768'::int2vector" "22003")
                    ("SELECT 128::\"char\"" "22003")
                    ("SELECT true::int8" "42846")
                    ("SELECT 1::int2vector" "42846")
                    ("SELECT 1::char" "0A000")))
      (ok (equal (second case) (sqlstate-of (derived-query-rows (first case)))))))

(deftest expression-cast-result-types
    (dolist (case '(("SELECT 'S'::\"char\"" 18 1 -1 "char")
                    ("SELECT NULL::int2vector" 22 -1 -1 "int2vector")
                    ("SELECT 'x'::text" 25 -1 -1 "text")
                    ("SELECT 'x'::varchar" 1043 -1 -1 "varchar")
                    ("SELECT 'x'::varchar(400)" 1043 -1 404 "varchar")))
      (let* ((target (first (getf (pg::prepare-select
                                   (first (pg::parse-statements (first case)))) :targets)))
             (column (pg::target->result-column target)))
        (ok (= (second case) (pg::result-column-type-oid column)))
        (ok (= (third case) (pg::result-column-typlen column)))
        (ok (= (fourth case) (pg::result-column-typmod column)))
        (ok (string= (fifth case) (pg::result-column-name column)))))
  (ok (test-row-equal-p (list (vector pg::+null+)) (derived-query-rows "SELECT NULL::int2vector"))))

(deftest expression-cast-vector-and-char-io
    (ok (test-row-equal-p '(#(#(1 2 -3))) (derived-query-rows "SELECT '1 2 -3'::int2vector")))
  (ok (test-row-equal-p '(#("1 2 -3")) (derived-query-rows "SELECT '1 2 -3'::int2vector::text")))
  (ok (= 255 (pg::sql-char-input (concatenate 'string (string (code-char 92)) "377"))))
  (ok (string= (concatenate 'string (string (code-char 92)) "303")
               (pg::sql-char-output (pg::sql-char-input "é"))))
  (ok (string= "" (pg::sql-char-output (pg::sql-char-input "")))))

(defun derived-query-rows (sql)
  (let* ((plan (pg::plan-select-query (first (pg::parse-statements sql))))
         (rows nil))
    (pg::interpret-query-plan
     plan (getf (pg::query-plan-prepared plan) :relations)
     (lambda (row) (push (car row) rows)))
    (nreverse rows)))

(defun whole-row-query-rows (sql &optional iterator-p)
  (let* ((plan (pg::plan-select-query (first (pg::parse-statements sql))))
         (relations (getf (pg::query-plan-prepared plan) :relations))
         (rows nil))
    (flet ((emit (row) (push (car row) rows) nil))
      (if iterator-p
          (let ((pg::*query-plan-backends* (make-hash-table :test #'eq)))
            (pg::register-query-plan-backend
             :whole-row-test
             :executor
             (lambda (query registered callback)
               (let ((input (pg::make-resumable-query-plan-iterator query registered)))
                 (unwind-protect (pg::map-sql-iterator input callback)
                   (pg::close-sql-iterator input)))))
            (pg::execute-query-plan plan relations #'emit :backend :whole-row-test))
          (pg::execute-query-plan plan relations #'emit)))
    (nreverse rows)))

(deftest whole-row-json-distinguishes-present-null-fields
    (ok (test-row-equal-p
         '(#("{\"id\":1,\"label\":\"x\"}"))
         (derived-query-rows
          "SELECT to_json(r.*) FROM (SELECT 1 AS id, 'x'::text AS label) r")))
  (ok (test-row-equal-p
       '(#("{\"id\":null,\"label\":null}"))
       (derived-query-rows
        "SELECT to_json(r.*) FROM (SELECT NULL::integer AS id, NULL::text AS label) r")))
  (ok (test-row-equal-p
       '(#(1 "x"))
       (derived-query-rows "SELECT r.* FROM (SELECT 1 AS id, 'x'::text AS label) r"))))

(deftest whole-row-json-survives-correlated-and-lateral-execution
    (dolist (iterator-p '(nil t))
      (dolist (sql '("SELECT to_json(r.*) FROM (SELECT NULL::integer AS id) r"
                     "SELECT (SELECT to_json(r.*)) FROM (SELECT NULL::integer AS id) r"
                     "SELECT (SELECT (SELECT to_json(r.*))) FROM (SELECT NULL::integer AS id) r"
                     "SELECT to_json(r.*) FROM (SELECT NULL::integer AS id) l LEFT JOIN LATERAL (SELECT l.id) r ON true"))
        (ok (test-row-equal-p '(#("{\"id\":null}")) (whole-row-query-rows sql iterator-p))))
      (dolist (sql '("SELECT (SELECT to_json(r.*)) FROM (SELECT 1 AS id) l LEFT JOIN (SELECT NULL::integer AS id) r ON false"
                     "SELECT to_json(r.*) FROM (SELECT 1 AS id) l LEFT JOIN LATERAL (SELECT l.id WHERE false) r ON true"))
        (ok (test-row-equal-p (list (vector pg::+null+)) (whole-row-query-rows sql iterator-p))))))

(deftest json-aggregate-preserves-null-and-whole-row-values
    (dolist (iterator-p '(nil t))
      (dolist (memory '(1 100000))
        (let ((pg::*query-work-memory-bytes* memory)
              (pg::*query-temp-directory* (uiop:temporary-directory)))
          (dolist (case
                      (list
                       (list "SELECT json_agg(r.id) FROM (SELECT 1 AS id WHERE false) r"
                             (list (vector pg::+null+)))
                       (list "SELECT json_agg(r.id) FROM (SELECT NULL::integer AS id) r"
                             '(#("[null]")))
                       (list "SELECT pg_catalog.json_agg(r.id) FROM (SELECT 1 AS id UNION ALL SELECT 2) r"
                             '(#("[1, 2]")))
                       (list "SELECT json_agg(r.*) FROM (SELECT NULL::integer AS id) r"
                             '(#("[{\"id\":null}]")))
                       (list "SELECT json_agg(r.*) FROM (SELECT 1 AS id) l LEFT JOIN (SELECT NULL::integer AS id) r ON false"
                             '(#("[null]")))
                       (list "SELECT json_agg(r.*) FROM (SELECT 1 AS id UNION ALL SELECT 2) r"
                             (list (vector (format nil "[{\"id\":1}, ~% {\"id\":2}]"))))
                       (list "SELECT json_agg(r.x) FROM (SELECT false AS x UNION ALL SELECT true UNION ALL SELECT NULL::boolean) r"
                             '(#("[false, true, null]")))
                       (list "SELECT json_agg(r.x) FROM (SELECT ARRAY[1,2] AS x UNION ALL SELECT ARRAY[3,4]) r"
                             (list (vector (format nil "[[1,2], ~% [3,4]]"))))
                       (list "SELECT json_agg(r.j) FROM (SELECT '{ \"raw\" : 1 }'::json AS j) r"
                             '(#("[{ \"raw\" : 1 }]")))))
            (ok (test-row-equal-p (second case)
                                  (whole-row-query-rows (first case) iterator-p))))))))

(deftest json-aggregate-group-and-window-snapshots
    (dolist (iterator-p '(nil t))
      (dolist (memory '(1 100000))
        (let ((pg::*query-work-memory-bytes* memory)
              (pg::*query-temp-directory* (uiop:temporary-directory)))
          (dolist (case
                      '(("SELECT k,json_agg(v) FROM (SELECT 1 AS k,'a'::text AS v UNION ALL SELECT 2,'b'::text UNION ALL SELECT 1,'c'::text) r GROUP BY k ORDER BY k"
                         (#(1 "[\"a\", \"c\"]") #(2 "[\"b\"]")))
                        ("SELECT v,json_agg(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) r ORDER BY v"
                         (#(1 "[1]") #(2 "[1, 2]") #(3 "[1, 2, 3]")))
                        ("SELECT v,json_agg(v) OVER (ORDER BY v) FROM (SELECT 1 AS v UNION ALL SELECT 1 UNION ALL SELECT 2) r ORDER BY v"
                         (#(1 "[1, 1]") #(1 "[1, 1]") #(2 "[1, 1, 2]")))
                        ("SELECT v,json_agg(v) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (SELECT 1 AS v UNION ALL SELECT 2) r ORDER BY v"
                         (#(1 "[1, 2]") #(2 "[1, 2]")))))
            (ok (test-row-equal-p (second case)
                                  (whole-row-query-rows (first case) iterator-p))))))))

(deftest json-aggregate-rejects-unknown-and-unsupported-call-decorations
    (dolist (case '(("SELECT json_agg('x')" . "42804")
                    ("SELECT json_agg(NULL)" . "42804")
                    ("SELECT json_agg(DISTINCT 1)" . "0A000")
                    ("SELECT json_agg(1 ORDER BY 1)" . "0A000")))
      (ok (equal (cdr case)
                 (handler-case (progn (whole-row-query-rows (car case)) nil)
                   (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))))

(deftest whole-row-analysis-retains-column-precedence-and-slot-identity
    (dolist (iterator-p '(nil t))
      (dolist (case
                  '(("SELECT to_json(r) FROM (SELECT 1 AS id, 'x'::text AS label) r"
                     (#("{\"id\":1,\"label\":\"x\"}")))
                    ("SELECT r FROM (SELECT 7 AS r) r" (#(7)))
                    ("SELECT (SELECT r FROM (SELECT 1 AS id) r) FROM (SELECT 7 AS r) o"
                     (#(7)))
                    ("SELECT r.* FROM (SELECT 1 AS x, 2 AS x) r" (#(1 2)))
                    ("SELECT (r.*) FROM (SELECT 1 AS x, 2 AS x) r" (#(1 2)))
                    ("SELECT to_json((r.*)) FROM (SELECT 1 AS x, 2 AS x) r"
                     (#("{\"x\":1,\"x\":2}")))
                    ("SELECT count(r.*) FROM (SELECT NULL::integer AS id) r" (#(1)))
                    ("SELECT count(r.*) FROM (SELECT 1 AS id) l LEFT JOIN (SELECT NULL::integer AS id) r ON false"
                     (#(0)))))
        (ok (test-row-equal-p (second case) (whole-row-query-rows (first case) iterator-p)))))
  (dolist (sql '("SELECT to_json(r.*) FROM (SELECT 1 AS id) r GROUP BY r.id"
                 "SELECT (SELECT to_json(r.*)) FROM (SELECT 1 AS id) r GROUP BY r.id"))
    (ok (equal "42803"
               (handler-case (progn (whole-row-query-rows sql) nil)
                 (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))))

(deftest whole-row-origin-capture-restores-correlated-presence
    (dolist (iterator-p '(nil t))
      (let ((pg::*datum-origin-capture* t))
        (ok (test-row-equal-p
             '(#("{\"id\":null}"))
             (whole-row-query-rows
              "SELECT to_json((SELECT COALESCE(r.*))) FROM (SELECT NULL::integer AS id) r"
              iterator-p))))))

(deftest whole-row-null-tests-use-composite-semantics
    (dolist (iterator-p '(nil t))
      (dolist (case
                  '(("SELECT (r.*) IS NULL, (r.*) IS NOT NULL FROM (SELECT NULL::integer AS id, NULL::text AS label) r"
                     (#(t nil)))
                    ("SELECT (r.*) IS NULL, (r.*) IS NOT NULL FROM (SELECT 1 AS id, NULL::text AS label) r"
                     (#(nil nil)))
                    ("SELECT (r.*) IS NULL, (r.*) IS NOT NULL FROM (SELECT 1 AS id, 'x'::text AS label) r"
                     (#(nil t)))
                    ("SELECT (r.*) IS NULL, (r.*) IS NOT NULL FROM (SELECT 1 AS id) l LEFT JOIN (SELECT 2 AS id) r ON false"
                     (#(t nil)))
                    ("SELECT (r.*) FROM (SELECT 1 AS id, 'x'::text AS label) r"
                     (#(1 "x")))))
        (ok (test-row-equal-p (second case)
                              (whole-row-query-rows (first case) iterator-p))))))

(deftest whole-row-json-survives-materialization-and-set-expansion
    (dolist (iterator-p '(nil t))
      (dolist (memory '(1 100000))
        (let ((pg::*query-work-memory-bytes* memory)
              (pg::*query-temp-directory* (uiop:temporary-directory)))
          (dolist (case
                      '(("SELECT CASE WHEN random() < 0.5 THEN to_json(r.*) ELSE to_json(r.*) END FROM (SELECT 2 AS id UNION ALL SELECT 1) r ORDER BY id LIMIT 1"
                         (#("{\"id\":1}")))
                        ("WITH r AS MATERIALIZED (SELECT NULL::integer AS id) SELECT to_json(r.*) FROM r"
                         (#("{\"id\":null}")))
                        ("WITH RECURSIVE r(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM r WHERE id < 2) SELECT to_json(r.*) FROM r ORDER BY id"
                         (#("{\"id\":1}") #("{\"id\":2}")))
                        ("SELECT to_json(r.*) FROM (SELECT 1 AS id) r UNION ALL SELECT to_json(r.*) FROM (SELECT 2 AS id) r"
                         (#("{\"id\":1}") #("{\"id\":2}")))
                        ("SELECT to_json(r.*), row_number() OVER () FROM (SELECT NULL::integer AS id) r"
                         (#("{\"id\":null}" 1)))
                        ("SELECT to_json(r.*), sum(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (SELECT 1 AS id UNION ALL SELECT 2) r ORDER BY id"
                         (#("{\"id\":1}" 3) #("{\"id\":2}" 3)))))
            (ok (test-row-equal-p (second case)
                                  (whole-row-query-rows (first case) iterator-p))))))))

(deftest whole-row-json-preserves-null-extension-through-outer-joins
    (dolist (sql '("SELECT to_json(r.*) FROM (SELECT 1 AS id) l
                   LEFT JOIN (SELECT 2 AS id) r ON false"
                   "SELECT to_json(l.*) FROM (SELECT 1 AS id) l
                   RIGHT JOIN (SELECT 2 AS id) r ON false"))
      (ok (test-row-equal-p (list (vector pg::+null+)) (derived-query-rows sql))))
  (ok (test-row-equal-p
       (list (vector "{\"id\":1}" pg::+null+)
             (vector pg::+null+ "{\"id\":2}"))
       (derived-query-rows
        "SELECT to_json(l.*), to_json(r.*) FROM (SELECT 1 AS id) l
           FULL JOIN (SELECT 2 AS id) r ON false ORDER BY coalesce(l.id,r.id)"))))

(deftest source-status-functions-join-real-activity-and-independent-feedback
    (let* ((pg::*backend-registry* (make-hash-table :test #'eql))
           (pg::*wal-sender-registry* (make-hash-table :test #'eq))
           (backend (pg::make-backend :pid 2354 :replication-mode :physical
                                      :application-name "status_join"))
           (ordinary (pg::make-backend :pid 2355 :application-name "sql_only")))
      (setf (gethash 2354 pg::*backend-registry*) backend
            (gethash 2355 pg::*backend-registry*) ordinary)
      (let ((sender (pg::wal-sender-register backend)))
        (unwind-protect
             (progn
               (pg::wal-sender-set-state sender "streaming")
               (pg::wal-sender-note-sent sender 96)
               (pg::wal-sender-note-reply sender 80 64 32 123)
               (ok (test-row-equal-p
                    '(#(2354 "status_join" "async" 80 64 32))
                    (derived-query-rows
                     "SELECT w.pid, application_name, sync_state,
                             pg_catalog.pg_wal_lsn_diff(write_lsn, '0/0')::bigint,
                             pg_catalog.pg_wal_lsn_diff(flush_lsn, '0/0')::bigint,
                             pg_catalog.pg_wal_lsn_diff(replay_lsn, '0/0')::bigint
                      FROM pg_catalog.pg_stat_get_wal_senders() w,
                           pg_catalog.pg_stat_get_activity(w.pid)
                      WHERE w.state = 'streaming'")))
               (dolist (iterator-p '(nil t))
                 (ok (test-row-equal-p
                      '(#("[{\"pid\":2354,\"application_name\":\"status_join\",\"sync_state\":\"async\",\"write_lsn\":80,\"flush_lsn\":64,\"replay_lsn\":32}]"))
                      (whole-row-query-rows
                       "SELECT json_agg(r.*) FROM (
                          SELECT w.pid AS pid, application_name, sync_state,
                                 pg_wal_lsn_diff(write_lsn,'0/0')::bigint AS write_lsn,
                                 pg_wal_lsn_diff(flush_lsn,'0/0')::bigint AS flush_lsn,
                                 pg_wal_lsn_diff(replay_lsn,'0/0')::bigint AS replay_lsn
                          FROM pg_stat_get_wal_senders() w, pg_stat_get_activity(w.pid)
                          WHERE w.state='streaming') r"
                       iterator-p))))
               (ok (test-row-equal-p
                    '(#(2354 "status_join") #(2355 "sql_only"))
                    (derived-query-rows
                     "SELECT pid, application_name FROM pg_stat_get_activity(NULL) ORDER BY pid"))))
          (pg::wal-sender-unregister backend sender)))))

(deftest union-all-rows-and-boundaries
    (dolist (case '(("SELECT 1 UNION ALL SELECT 1" (#(1) #(1)))
                    ("(SELECT 1 LIMIT 1) UNION ALL SELECT 2" (#(1) #(2)))
                    ("SELECT DISTINCT 2 UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY 1 LIMIT 2" (#(1) #(2)))
                    ("SELECT 1 WHERE false UNION ALL SELECT 2" (#(2)))
                    ("SELECT 1 UNION ALL SELECT 2 LIMIT 0" ())
                    ("SELECT * FROM (SELECT 2 AS id UNION ALL SELECT 1) AS u ORDER BY id" (#(1) #(2)))
                    ("(SELECT 2 ORDER BY 1 LIMIT 0) UNION ALL SELECT 1" (#(1)))
                    ("SELECT 2 UNION ALL (SELECT 1 UNION ALL SELECT 3 ORDER BY 1 LIMIT 1) ORDER BY 1" (#(1) #(2)))))
      (ok (test-row-equal-p (second case) (derived-query-rows (first case))) (first case))))

(deftest union-all-types-and-errors
    (ok (test-row-equal-p '(#(1) #(2)) (derived-query-rows "SELECT '1' UNION ALL SELECT 2")))
  (ok (test-row-equal-p '(#(1) #(2)) (derived-query-rows "SELECT 1 UNION SELECT 1 UNION SELECT 2 ORDER BY 1")))
  (ok (test-row-equal-p (list (vector pg::+null+)) (derived-query-rows "SELECT NULL UNION SELECT NULL")))
  (ok (test-row-equal-p '(#("A") #("a")) (derived-query-rows "SELECT 'a' UNION SELECT 'A' ORDER BY 1")))
  (ok (test-row-equal-p (list (vector pg::+null+) #(2))
                        (derived-query-rows "SELECT NULL UNION ALL SELECT 2")))
  (dolist (case '(("SELECT 1 UNION ALL SELECT 1,2" "42601")
                  ("SELECT 1 UNION ALL SELECT true" "42804")
                  ("SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 1" "42804")
                  ("SELECT 'bad' UNION ALL SELECT 1" "22P02")
                  ("SELECT 1 AS x UNION ALL SELECT 2 AS y ORDER BY y" "42703")
                  ("SELECT 1 AS x UNION ALL SELECT 2 ORDER BY x+1" "0A000")
                  ("SELECT 1 INTERSECT SELECT 2" "0A000")
                  ("SELECT 1 EXCEPT SELECT 2" "0A000")))
    (ok (equal (second case) (sqlstate-of (derived-query-rows (first case))))
        (first case))))

(deftest union-all-spill-and-relation-domains
    (call-with-publication-srf-fixture
     (lambda (database)
       (declare (ignore database))
       (let* ((sql "SELECT * FROM (SELECT DISTINCT pubname FROM pg_catalog.pg_publication UNION ALL SELECT pubname FROM pg_catalog.pg_publication) AS u ORDER BY pubname DESC")
              (plan (pg::plan-select-query (first (pg::parse-statements sql))))
              (nodes (pg::collect-query-plan-nodes (pg::query-plan-physical plan)))
              (directory (merge-pathnames (format nil "pg-union-spill-~a/" (gensym))
                                          (uiop:temporary-directory))))
         (ok (equal '(1 2) (mapcar #'second (remove-if-not
                                             (lambda (node) (eq (first node) :seq-scan)) nodes))))
         (ok (search "Append" (pg::format-query-plan plan)))
         (ensure-directories-exist directory)
         (unwind-protect
              (dolist (memory '(1048576 1))
                (let ((pg::*query-temp-directory* directory)
                      (pg::*query-work-memory-bytes* memory))
                  (ok (test-row-equal-p '(#("q") #("q") #("p") #("p") #("all") #("all"))
                                        (derived-query-rows sql)))
                  (ok (null (uiop:directory-files directory)))))
           (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))))

(deftest union-oid-coercion-input
    (dolist (case '(("0" 0) ("010" 8) ("0x10" 16) ("0b101" 5) ("0B1" 1) ("-1" 4294967295)
                    (" -2147483648 " 2147483648) ("4294967295" 4294967295)))
      (ok (= (second case) (pg::sql-oid-input (first case)))))
  (dolist (case '(("١" "22P02") ("0x١" "22P02") ("09" "22P02") ("0b2" "22P02") ("1_0" "22P02")
                  ("4294967296" "22003") ("-2147483649" "22003")))
    (ok (equal (second case) (sqlstate-of (pg::sql-oid-input (first case)))))))

(deftest derived-select-execution
    (dolist (sql '("SELECT q.a FROM (SELECT 7 AS a) AS q"
                   "SELECT a FROM (SELECT 7 AS a)"
                   "SELECT d.a FROM (SELECT q.a FROM (SELECT 7 AS a) q) d"
                   "SELECT DISTINCT q.a FROM (SELECT 7 AS a) q ORDER BY q.a LIMIT 1"))
      (ok (test-row-equal-p '(#(7)) (derived-query-rows sql))))
  (ok (test-row-equal-p '(#(7 8))
                        (derived-query-rows "SELECT q.a, r.b FROM (SELECT 7 AS a) q JOIN (SELECT 8 AS b) r ON q.a < r.b")))
  (ok (test-row-equal-p (list (vector 7 pg::+null+))
                        (derived-query-rows "SELECT q.a, r.b FROM (SELECT 7 AS a) q LEFT JOIN (SELECT 8 AS b) r ON q.a = r.b")))
  (ok (test-row-equal-p '(#(7 8))
                        (derived-query-rows "SELECT a,b FROM (SELECT 7 AS a) CROSS JOIN (SELECT 8 AS b)")))
  (ok (null (derived-query-rows "SELECT q.a FROM (SELECT 7 AS a WHERE false) q"))))

(deftest derived-select-namespace-errors
    (dolist (case '(("SELECT q.a FROM (SELECT 1 AS a, 2 AS a) q" "42702")
                    ("SELECT a FROM (SELECT 1 AS a, 2 AS a) q" "42702")
                    ("SELECT a FROM (SELECT 1 AS a) q CROSS JOIN (SELECT 2 AS a) r" "42702")
                    ("SELECT q.hidden FROM (SELECT 1 AS a) q" "42703")
                    ("SELECT q.a FROM (SELECT 1 AS a) q CROSS JOIN (SELECT q.a) r" "42P01")))
      (ok (equal (second case) (sqlstate-of (pg::plan-select-query (first (pg::parse-statements (first case)))))))))

(deftest derived-select-flat-plan
    (let* ((plan (pg::plan-select-query
                  (first (pg::parse-statements
                          "SELECT q.pubname FROM (SELECT pubname FROM pg_catalog.pg_publication) q"))))
           (relations (getf (pg::query-plan-prepared plan) :relations))
           (nodes (pg::collect-query-plan-nodes (pg::query-plan-physical plan)))
           (target (first (getf (pg::query-plan-prepared plan) :targets))))
      (ok (= 2 (length relations)))
      (ok (find :subquery-scan nodes :key #'first))
      (ok (= 1 (second (find :seq-scan nodes :key #'first))))
      (ok (= 6104 (getf target :table-oid)))
      (ok (= 2 (pg::result-column-attnum (pg::target->result-column target))))
      (ok (every (lambda (node) (gethash node (pg::query-plan-metadata plan))) nodes))
      (ok (search "Subquery Scan on q" (pg::format-query-plan plan)))))

(deftest derived-index-lease-domain-and-fallback
    (let* ((left-table (pg::make-pg-class :oid 10 :relfilenode 11))
           (right-table (pg::make-pg-class :oid 20 :relfilenode 21))
           (left-index (pg::make-pg-index :oid 100 :table-oid 10 :relfilenode 201))
           (right-index (pg::make-pg-index :oid 200 :table-oid 20 :relfilenode 101))
           (left (list :table left-table :path
                       (pg::make-select-scan-path :kind :index-scan :index left-index)))
           (right (list :table right-table :path
                        (pg::make-select-scan-path :kind :index-scan :index right-index)))
           (child-tree '(:nested-loop-join (:type :inner)
                         (:index-scan 0 10 100 nil nil nil)
                         (:index-scan 1 20 200 nil nil nil)))
           (child (pg::%make-query-plan
                   :logical child-tree :physical child-tree
                   :prepared (list :relations (list left right))))
           (source (list :table (pg::make-pg-class :oid 0) :virtual-p t
                         :subquery-plan child :subquery-logical nil :subquery-physical nil))
           (prepared (list :relations (list source)))
           (registered (pg::flatten-select-subplans prepared))
           (tree (pg::select-scan-plan-form 0 source nil t))
           (plan (pg::%make-query-plan :physical tree :prepared prepared)))
      ;; Every child leaf participates in admission, and lock order is independent
      ;; of textual scan order or nesting. Synthetic relations have no heap lease.
      (ok (equal '(11 21 101 201)
                 (sort (pg::select-relation-reader-relfilenodes registered) #'<)))
      (ok (equal '(101 201) (pg::query-plan-index-relfilenodes plan registered)))
      ;; Simulate admission revalidation replacing the selected child index.
      (setf (getf left :path) (pg::make-select-scan-path :kind :seq-scan))
      (pg::reconcile-query-plan-access-paths plan registered)
      (ok (equal '(101) (pg::query-plan-index-relfilenodes plan registered)))
      (ok (= 1 (count :seq-scan (pg::collect-query-plan-nodes tree) :key #'first)))
      ;; Rebasing copied the tree; reconciling the root must not mutate child IR.
      (ok (= 2 (count :index-scan (pg::collect-query-plan-nodes child-tree) :key #'first)))))

(deftest distinct-parser-and-plan
    (let* ((ast (first (pg::parse-statements "SELECT DISTINCT 1 ORDER BY 1 LIMIT 2")))
           (plan (pg::plan-select-query ast)))
      (ok (getf ast :distinct))
      (ok (find :hash-distinct (pg::collect-query-plan-nodes (pg::query-plan-physical plan))
                :key #'first)))
  (ok (equal "0A000" (sqlstate-of (pg::parse-statements "SELECT DISTINCT ON (1) 1")))))

(deftest distinct-executor-projected-values
    (let* ((keys '((:col 1 :type :int4) (:col 2 :type :text)))
           (null pg::+null+)
           (input (list (vector 1 "a") (vector 1 "a")
                        (vector 1 null) (vector 1 null) (vector 2 "a")))
           (output nil))
      (pg::execute-physical-plan-node
       (list :hash-distinct keys :test-input) nil nil
       (lambda (row)
         (ok (eq (car row) (cdr row)))
         (push (coerce (car row) 'list) output))
       (lambda (node callback)
         (declare (ignore node))
         (dolist (values input)
           (funcall callback (cons values #(:irrelevant)))))
       nil)
      (ok (= 3 (length output)))
      (ok (member (list 1 null) output :test #'equal))
      (ok (member '(1 "a") output :test #'equal))
      (ok (member '(2 "a") output :test #'equal))))

(deftest distinct-empty-input
    (let ((count 0))
      (pg::execute-physical-plan-node
       '(:hash-distinct ((:col 1 :type :int4)) :test-input) nil nil
       (lambda (row) (declare (ignore row)) (incf count))
       (lambda (node callback) (declare (ignore node callback))) nil)
      (ok (zerop count))))

(deftest distinct-spill-and-sort-fallback
    (let* ((directory (merge-pathnames
                       (format nil "pg-distinct-~a/" (gensym)) (uiop:temporary-directory)))
           (keys '((:col 1 :type :int4)))
           (input (loop for i below 100 collect
                                        (vector (if (zerop (mod i 17)) pg::+null+ (mod i 23))))))
      (ensure-directories-exist directory)
      (unwind-protect
           (dolist (partitions '(16 1))
             (let ((pg::*query-temp-directory* directory)
                   (pg::*query-work-memory-bytes* 1)
                   (pg::+hash-aggregate-partition-count+ partitions)
                   (output nil))
               (pg::execute-physical-plan-node
                (list :hash-distinct keys :test-input) nil nil
                (lambda (row) (push (aref (car row) 0) output))
                (lambda (node callback)
                  (declare (ignore node))
                  (dolist (values input) (funcall callback (cons values #()))))
                nil)
               (ok (= 24 (length output)))
               (ok (equal (sort (remove pg::+null+ output) #'<)
                          (loop for i below 23 collect i)))
               (ok (= 1 (count pg::+null+ output)))
               (ok (null (uiop:directory-files directory)))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(deftest distinct-vector-content-equality
    (let ((directory (merge-pathnames
                      (format nil "pg-distinct-vector-~a/" (gensym))
                      (uiop:temporary-directory))))
      (ensure-directories-exist directory)
      (unwind-protect
           (dolist (memory '(1048576 1))
             (let ((pg::*query-temp-directory* directory)
                   (pg::*query-work-memory-bytes* memory)
                   (pg::+hash-aggregate-partition-count+ 1)
                   (output nil))
               (pg::execute-physical-plan-node
                '(:hash-distinct ((:col 1 :type :int2vector)) :test-input) nil nil
                (lambda (row) (push (aref (car row) 0) output))
                (lambda (node callback)
                  (declare (ignore node))
                  (dolist (contents '((1 2) (1 2) (1) (1 3) (1 2) (1)))
                    (funcall callback (cons (vector (coerce contents 'vector)) #()))))
                nil)
               (ok (= 3 (length output)))
               (ok (every (lambda (expected) (member expected output :test #'equalp))
                          '(#(1) #(1 2) #(1 3))))
               (ok (null (uiop:directory-files directory)))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(deftest distinct-constant-execution
    (dolist (sql '("SELECT DISTINCT 1 AS value ORDER BY value LIMIT 1"
                   "SELECT DISTINCT 1 ORDER BY 1"))
      (let ((rows nil)
            (plan (pg::plan-select-query (first (pg::parse-statements sql)))))
        (pg::interpret-query-plan plan nil (lambda (row) (push (car row) rows)))
        (ok (equalp rows '(#(1)))))))

(deftest distinct-repeated-aggregate-alias
    (let ((rows nil)
          (plan (pg::plan-select-query
                 (first (pg::parse-statements
                         "SELECT DISTINCT COUNT(*) AS x, COUNT(*) AS x ORDER BY x")))))
      (pg::interpret-query-plan plan nil (lambda (row) (push (car row) rows)))
      (ok (equalp rows '(#(1 1))))))

(deftest distinct-order-must-be-selected
    (let ((table (pg::make-pg-class
                  :name "d" :oid 10000
                  :columns (vector (pg::make-pg-column :name "a" :type :int4 :attnum 1)
                                   (pg::make-pg-column :name "b" :type :int4 :attnum 2)))))
      (let* ((targets (list (pg::analyze-target '(:expr (:column "a") :alias "value") table)))
             (order (pg::analyze-distinct-order
                     '((:expr (:column "value") :desc t)) table targets nil nil)))
        (ok (= 1 (second (caar order))))
        (ok (cdar order))
        (ok (equal "42P10" (sqlstate-of (pg::analyze-distinct-order '((:expr (:column "b"))) table targets nil nil)))))))

(deftest distinct-order-noninteger-constants
    (dolist (sql '("SELECT DISTINCT 'x' ORDER BY 'x'"
                   "SELECT DISTINCT NULL ORDER BY NULL"
                   "SELECT DISTINCT TRUE ORDER BY TRUE"))
      (ok (equal "42601" (sqlstate-of (pg::plan-select-query (first (pg::parse-statements sql))))))))

(deftest name-input-clips-at-utf8-boundary
    (let* ((long (concatenate 'string "a" (make-string 40 :initial-element #\ą)))
           (clipped (pg::sql-name-input long)))
      (ok (= 63 (length (pg::string->bytes clipped))))
      (ok (= 32 (length clipped)))
      (ok (string= clipped (subseq long 0 32)))))

(deftest in-unknown-literal-coercion
    (ok (eq t (scalar-sql-value "SELECT 1 IN ('1')")))
  (ok (eq t (scalar-sql-value "SELECT '1' IN (1, 2)")))
  (ok (eq t (scalar-sql-value "SELECT 1 IN ('01', '2')")))
  (ok (eq t (scalar-sql-value "SELECT true IN ('true', 'false')")))
  (ok (eq pg::+null+ (scalar-sql-value "SELECT 1 NOT IN ('2', NULL)")))
  (ok (equal "22P02" (sqlstate-of (scalar-sql-value "SELECT 1 IN ('invalid')")))))

(deftest publication-result-column-origins
    (let ((table (pg::lookup-sql-system-relation "pg_publication")))
      (loop for column across (pg::pg-class-columns table)
            for expected in '(1 2 3 4 5 6 7 8 9 10 11)
            for target = (pg::analyze-target
                          (list :expr (list :column (pg::pg-column-name column))) table)
            for result = (pg::target->result-column target)
            do (ok (= 6104 (pg::result-column-table-oid result)))
               (ok (= expected (pg::result-column-attnum result))))))

(deftest in-row-items-short-circuit
    (let* ((table (pg::make-pg-class
                   :name "in_rows" :oid 10000
                   :columns (vector (pg::make-pg-column :name "id" :type :int4 :attnum 1))))
           (expr '(:in (:column "id")
                   ((:const 1) (:binop "/" (:column "id") (:const 0))) nil)))
      (ok (eq t (pg::eval-expr (pg::analyze-expr expr table) #(1))))
      (ok (null (pg::eval-expr
                 (pg::analyze-expr (append (butlast expr) '(t)) table) #(1))))))

(deftest publication-catalog-uses-persisted-actions
    (let* ((pg::*backend* (pg::make-backend))
           (database (pg::make-database :name "postgres"))
           (pg::*catalog* (pg::make-catalog)))
      (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
            (gethash "b" (pg::database-publications database))
            (pg::make-pg-publication :oid 112 :name "b" :publish-actions nil)
            (gethash "a" (pg::database-publications database))
            (pg::make-pg-publication :oid 111 :name "a" :publish-actions '(:insert :truncate)))
      (ok (equalp '(#(111 "a" 10 nil nil t nil nil t nil 110) #(112 "b" 10 nil nil nil nil nil nil nil 110))
                  (pg::sql-publication-rows)))
      (let ((catalog (pg::lookup-sql-system-relation "pg_publication")))
        (ok (pg::sql-system-relation-p catalog))
        (ok (= 26 (pg::type-oid
                   (pg::pg-column-type (aref (pg::pg-class-columns catalog) 0))))))))

(defun scalar-sql-value (sql)
  (let* ((ast (first (pg::parse-statements sql)))
         (expr (getf (first (getf ast :targets)) :expr)))
    (pg::eval-expr (pg::analyze-expr expr nil) nil)))

(deftest in-predicate-three-valued-logic
    (dolist (case (list (list "SELECT 2 IN (1, 2, 3)" t)
                        (list "SELECT 4 IN (1, 2, 3)" nil)
                        (list "SELECT 2 NOT IN (1, 2)" nil)
                        (list "SELECT 4 NOT IN (1, 2)" t)
                        (list "SELECT NULL IN (1, 2)" pg::+null+)
                        (list "SELECT 4 IN (1, NULL)" pg::+null+)
                        (list "SELECT 4 NOT IN (1, NULL)" pg::+null+)
                        (list "SELECT 2 IN (NULL, 2)" t)
                        (list "SELECT 'interop_pub' IN ('other', 'interop_pub')" t)))
      (ok (eql (second case) (scalar-sql-value (first case))))))

(deftest dotted-mutation-name-does-not-alias-another-table
    (dolist (sql '("INSERT INTO \"public.rows\" VALUES (1)"
                   "UPDATE \"public.rows\" SET id = 2"
                   "DELETE FROM \"public.rows\""
                   "TRUNCATE TABLE \"public.rows\""
                   "DROP TABLE \"public.rows\""
                   "ALTER TABLE \"public.rows\" ADD UNIQUE (id)"
                   "CREATE TABLE \"public.rows\" (id integer)"))
      (ok (equal "0A000" (sqlstate-of (pg::parse-statements sql))))))

(deftest search-path-set-list-boundaries
    (ok (string= "public, pg_catalog"
                 (getf (first (pg::parse-statements "SET search_path TO public, pg_catalog")) :value)))
  (ok (string= "\"public,pg_catalog\""
               (getf (first (pg::parse-statements "SET search_path TO 'public,pg_catalog'")) :value))))

(deftest search-path-list-is-not-sql-code
    ;; SplitIdentifierString never removes SQL comments from identifier contents.
    (ok (equal '("public/*comment*/" "pg_catalog")
               (pg::search-path-identifiers "public/*comment*/, pg_catalog")))
  (ok (equal '("pg_catalog")
             (pg::parse-sql-search-path "public/*comment*/, pg_catalog")))
  (ok (equal '("public,pg_catalog")
             (pg::search-path-identifiers "\"public,pg_catalog\"")))
  (dolist (value '("public," ",public" "public pg_catalog" "\"unterminated"))
    (ok (equal "22023" (sqlstate-of (pg::search-path-identifiers value))))))

(deftest search-path-setting-is-session-state
    (let ((pg::*backend* (pg::make-backend)))
      (ok (string= "" (pg::set-config-value "search_path" "" nil)))
      (ok (string= "" (pg::session-search-path)))
      (ok (eq pg::+null+ (pg::current-schema-name)))
      (ok (equal "0A000" (sqlstate-of (pg::set-config-value "search_path" "public" t))))
      (ok (string= "" (pg::session-search-path)))
      (ok (string= "\"$user\", public"
                   (pg::set-config-value "search_path" pg::+null+ nil)))))

(deftest no-default-creation-schema
    (let ((pg::*backend* (pg::make-backend)))
      (pg::set-config-value "search_path" "" nil)
      (ok (equal "3F000" (sqlstate-of (pg::ensure-sql-creation-schema))))))

(deftest namespace-does-not-alias-user-tables
    (let* ((pg::*backend* (pg::make-backend))
           (database (pg::make-database :name "postgres"))
           (pg::*catalog* (pg::make-catalog))
           (public-table (pg::make-pg-class :name "pg_replication_slots" :oid 123))
           (literal-table (pg::make-pg-class :name "public.rows" :oid 124)))
      (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
            (gethash "pg_replication_slots" (pg::database-tables database)) public-table
            (gethash "public.rows" (pg::database-tables database)) literal-table)
      (ok (pg::sql-system-relation-p (pg::select-table-or-error "pg_replication_slots")))
      (ok (eq public-table
              (pg::select-table-or-error
               (pg::make-sql-relation-name :schema "public" :name "pg_replication_slots"))))
      (ok (null (pg::select-virtual-row-source public-table)))
      (ok (eq literal-table (pg::select-table-or-error "public.rows")))
      (setf (gethash "search_path" (pg::backend-settings pg::*backend*)) "")
      (ok (pg::sql-system-relation-p (pg::select-table-or-error "pg_replication_slots")))
      (ok (null (pg::lookup-sql-relation "public.rows")))
      (ok (eq literal-table
              (pg::select-table-or-error
               (pg::make-sql-relation-name :schema "public" :name "public.rows"))))
      (setf (gethash "search_path" (pg::backend-settings pg::*backend*)) "public, pg_catalog")
      (ok (eq public-table (pg::select-table-or-error "pg_replication_slots")))))

(deftest qualified-relation-identifiers
    (let* ((ast (first (pg::parse-statements "SELECT r.id FROM public.rows AS r")))
           (reference (getf ast :from)))
      (ok (pg::sql-relation-name-p reference))
      (ok (string= "public" (pg::sql-relation-name-schema reference)))
      (ok (string= "rows" (pg::sql-relation-name-name reference)))
      (ok (string= "r" (getf ast :from-alias))))
  (let ((name (getf (first (pg::parse-statements "SELECT * FROM \"public.rows\"")) :from)))
    (ok (string= "public.rows" name))))

(deftest namespace-search-order
    (let ((pg::*backend* (pg::make-backend)))
      (ok (equal '("pg_catalog" "public") (pg::effective-sql-search-path)))
      (setf (gethash "search_path" (pg::backend-settings pg::*backend*)) "")
      (ok (equal '("pg_catalog") (pg::effective-sql-search-path)))
      (ok (eq pg::+null+ (pg::current-schema-name)))
      (setf (gethash "search_path" (pg::backend-settings pg::*backend*)) "public, pg_catalog")
      (ok (equal '("public" "pg_catalog") (pg::effective-sql-search-path)))
      (setf (gethash "search_path" (pg::backend-settings pg::*backend*)) "\"public,pg_catalog\"")
      (ok (equal '("pg_catalog") (pg::effective-sql-search-path)))))

(deftest catalog-functions-versus-value-keywords
    ;; The C reference's pg_proc includes current_user and session_user as real
    ;; functions, while USER and CURRENT_CATALOG exist only as SQL value syntax.
    (dolist (name '("current_user" "session_user"))
      (ok (string= "postgres"
                   (pg::eval-expr
                    (pg::analyze-expr (list :func name nil :schema "pg_catalog") nil) nil))))
  (dolist (name '("user" "current_catalog"))
    (ok (equal "42883" (sqlstate-of (pg::analyze-expr (list :func name nil :schema "pg_catalog") nil))))))

(deftest replication-command-transaction-context
    (let ((pg::*backend* (pg::make-backend :state :in-progress :explicit t)))
      (ok (equal "25001" (sqlstate-of (pg::validate-replication-command-context
                                       (pg::make-logical-replication-command :type :start)))))
      (setf (pg::backend-state pg::*backend*) :aborted)
      (ok (equal "25P02" (sqlstate-of (pg::validate-replication-command-context
                                       (pg::make-logical-replication-command :type :drop)))))))

(deftest dotted-literal-function-name-is-not-a-schema
    (let* ((ast (first (pg::parse-statements "SELECT \"pg_catalog.version\"()")))
           (expr (getf (first (getf ast :targets)) :expr)))
      (ok (handler-case (progn (pg::analyze-expr expr nil) nil)
            (pg::pg-error () t)))))

(deftest catalog-qualified-functions
    (ok (string= "PostgreSQL 20devel (pg-lisp 0.1)"
                 (pg::eval-expr
                  (pg::analyze-expr '(:func "version" nil :schema "pg_catalog") nil) nil)))
  (ok (handler-case
          (progn (pg::analyze-expr '(:func "version" nil :schema "public") nil) nil)
        (pg::pg-error () t))))

(deftest replication-query-classification
    (dolist (sql '("CREATE_REPLICATION_SLOT s LOGICAL pgoutput EXPORT_SNAPSHOT"
                   "START_REPLICATION SLOT s LOGICAL 0/0"
                   "DROP_REPLICATION_SLOT s"
                   "IDENTIFY_SYSTEM" "TIMELINE_HISTORY 2" "SHOW statement_timeout"))
      (ok (pg::replication-query-payload-p (pg::string->bytes sql))))
  (dolist (sql '("SELECT 1" "BEGIN" "COMMIT" "ROLLBACK"
                 "CREATE_REPLICATION_SLOT_extra" "START_REPLICATIONx"
                 "CREATE TABLE t (id int)" ""))
    (ok (not (pg::replication-query-payload-p (pg::string->bytes sql)))))
  ;; C repl_scanner does not skip SQL comments; database mode uses SQL fallback.
  (ok (not (pg::replication-query-payload-p
            (pg::string->bytes " /* comment */ identify_system"))))
  (ok (not (pg::replication-query-payload-p
            (pg::string->bytes " /* comment */ SHOW statement_timeout"))))
  (ok (not (pg::replication-query-payload-p
            (pg::string->bytes " /* comment */ SELECT 1")))))

(deftest publication-actions-are-a-set
    (ok (equal '(:insert :update :delete)
               (pg::parse-publication-actions "delete, insert, update, insert"))))

(deftest streamed-message-is-not-ordinary
    ;; logicalrep_write_message with xid=91 has an extra uint32, unlike the
    ;; ordinary transactional frame. We do not negotiate streamed transactions.
    (ok (handler-case
            (progn
              (pg::logical-pgoutput-read-message
               (pg::make-logical-subscription-runtime :transaction-open-p t :source-xid 91)
               (octets #(77 0 0 0 91 1 0 0 0 0 0 0 0 42
                         112 103 95 108 105 115 112 47 100 100 108 0
                         0 0 0 1 120)))
              nil)
          (simple-error () t))))

(deftest invalid-publication-options
    (dolist (sql '("CREATE PUBLICATION p FOR TABLE a WITH (publish = 'insert,')"
                   "CREATE PUBLICATION p FOR TABLE a WITH (publish = 'merge')"
                   "CREATE PUBLICATION p FOR TABLE a WITH (publish = 'insert', publish = 'delete')"
                   "CREATE PUBLICATION p FOR TABLE a WITH (publish_via_partition_root = true)"))
      (ok (handler-case (progn (pg::parse-statements sql) nil)
            (pg::pg-error () t)))))

(deftest all-publication-action-subsets-roundtrip
    (dotimes (mask 16)
      (let* ((actions (loop for action in '(:insert :update :delete :truncate)
                            for bit from 0 when (logbitp bit mask) collect action))
             (publication (pg::make-pg-publication :name "p" :publish-actions actions)))
        (ok (equal actions
                   (pg::pg-publication-publish-actions
                    (pg::publication-from-sexp (pg::publication-to-sexp publication))))))))

(deftest explicit-publication-actions
    (let ((ast (first (pg::parse-statements
                       "CREATE PUBLICATION p FOR TABLE a, b WITH (publish = 'insert, update, delete, truncate')"))))
      (ok (equal (getf ast :publish-actions) '(:insert :update :delete :truncate))))
  (let ((ast (first (pg::parse-statements
                     "ALTER PUBLICATION p SET (publish = 'insert, delete')"))))
    (ok (eq (getf ast :action) :set-options))
    (ok (equal (getf ast :publish-actions) '(:insert :delete)))))

(deftest publication-actions-persistence
    (let* ((publication (pg::make-pg-publication :name "p" :publish-actions '(:insert)))
           (encoded (pg::publication-to-sexp publication)))
      (ok (equal '(:insert)
                 (pg::pg-publication-publish-actions (pg::publication-from-sexp encoded))))
      (ok (equal '(:insert :update :delete :truncate)
                 (pg::pg-publication-publish-actions
                  (pg::publication-from-sexp '(:name "legacy")))))
      (ok (null
           (pg::pg-publication-publish-actions
            (pg::publication-from-sexp '(:name "empty" :publish-actions nil)))))))

(deftest reference-version-advertisement
    (ok (string= pg::*server-version* "20devel"))
  (ok (= pg::*server-version-num* 200000))
  (ok (string= (pg::eval-func "version" nil) "PostgreSQL 20devel (pg-lisp 0.1)")))

(deftest malformed-update
    (dolist (bytes '(#(85 0 0 0 7)
                     #(85 0 0 0 7 88)
                     #(85 0 0 0 7 75 0 0)
                     #(85 0 0 0 7 78 0 0 0)))
      (ok (handler-case
              (progn (pg::logical-pgoutput-read-update (octets bytes) 5) nil)
            (simple-error () t)))))

(deftest unsupported-tuple-representations
    (dolist (kind '(98)) ; binary output remains unsupported
      (ok (handler-case
              (progn (pg::logical-pgoutput-read-tuple (octets (list 0 1 kind)) 0) nil)
            (simple-error () t)))))

(deftest unchanged-toast-marker-decoding
    (multiple-value-bind (values end)
        (pg::logical-pgoutput-read-tuple (octets '(0 3 110 117 116 0 0 0 1 120)) 0)
      (ok (equalp values #(nil :unchanged-toast "x")))
      (ok (= end 10)))
  (ok (handler-case
          (progn (pg::logical-source-value-literal 25 :unchanged-toast :pgoutput) nil)
        (simple-error () t))))

(deftest unchanged-toast-update-assignments
    (let* ((id (pg::make-pgoutput-column :name "id" :type-oid 23 :key-p t))
           (body (pg::make-pgoutput-column :name "body" :type-oid 25))
           (note (pg::make-pgoutput-column :name "note" :type-oid 25))
           (relation (pg::make-pgoutput-relation :columns (list id body note) :key-columns (list id))))
      (ok (equal (pg::logical-update-set-clause relation #("7" :unchanged-toast nil))
                 "id = 7, note = NULL"))
      (ok (handler-case
              (progn (pg::logical-update-set-clause relation #(:unchanged-toast :unchanged-toast :unchanged-toast)) nil)
            (simple-error () t)))
      (setf (pg::pgoutput-column-type-oid id) 25)
      (ok (equal (pg::logical-update-set-clause relation #(:unchanged-toast :unchanged-toast :unchanged-toast))
                 "id = id"))
      (ok (handler-case
              (progn (pg::logical-row-key-predicate #(:unchanged-toast) (list id)) nil)
            (simple-error () t)))
      (ok (handler-case
              (progn (pg::logical-row-insert-sql relation #("7" :unchanged-toast nil)) nil)
            (simple-error () t)))))

(deftest unchanged-toast-old-key-and-malformed-update
    (let* ((id (pg::make-pgoutput-column :name "id" :type-oid 25 :key-p t))
           (body (pg::make-pgoutput-column :name "body" :type-oid 25))
           (relation (pg::make-pgoutput-relation :columns (list id body) :key-columns (list id)))
           (payload (octets #(75 0 2 116 0 0 0 1 55 110 78 0 2 117 117))))
      (multiple-value-bind (old new kind) (pg::logical-pgoutput-read-update payload 0)
        (ok (char= kind #\K))
        (ok (equal (pg::logical-row-key-predicate
                    (pg::logical-pgoutput-key-values relation old) (list id)) "id = '7'"))
        (ok (equal (pg::logical-update-set-clause relation new) "id = id")))
      (ok (handler-case
              (progn (pg::logical-pgoutput-read-update (subseq payload 0 14) 0) nil)
            (simple-error () t)))))

(deftest unchanged-toast-marker-context-and-types
    (let* ((id (pg::make-pgoutput-column :name "id" :type-oid 25 :key-p t))
           (number (pg::make-pgoutput-column :name "n" :type-oid 23))
           (body (pg::make-pgoutput-column :name "body" :type-oid 17))
           (relation (pg::make-pgoutput-relation :name "t" :columns (list id number body)
                                                 :key-columns (list id)))
           (runtime (pg::make-logical-subscription-runtime
                     :options (pg::make-logical-subscription-options :tables '("t"))))
           (original (symbol-function 'pg::logical-target-query))
           (queries nil)
           (valid (octets #(75 0 3 116 0 0 0 1 55 110 110
                            78 0 3 117 116 0 0 0 1 49 117)))
           (bad-old (octets #(75 0 3 116 0 0 0 1 55 110 117
                              78 0 3 117 116 0 0 0 1 49 117)))
           (bad-fixed (octets #(75 0 3 116 0 0 0 1 55 110 110
                                78 0 3 117 117 117))))
      (setf (gethash "t" (pg::logical-subscription-runtime-validated-relations runtime)) t)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::logical-target-query)
                   (lambda (target query) (declare (ignore target))
                     (push (if (pg::pg-client-query-plan-p query)
                               (with-output-to-string (stream)
                                 (funcall (pg::pg-client-query-plan-writer query) stream))
                               query)
                           queries)
                     (values nil nil "UPDATE 1")))
             ;; A varlena key may be unchanged when an old K tuple supplies it.
             (pg::logical-apply-update runtime relation valid 0)
             (let ((query (first queries)))
               (ok (pg::pg-client-bound-query-p query))
               (ok (equal (pg::pg-client-bound-query-sql query) "UPDATE t SET n = $1 WHERE id = $2"))
               (ok (equal (mapcar #'car (pg::pg-client-bound-query-parameters query)) '(23 25)))
               (ok (equal (mapcar (lambda (param)
                                    (with-output-to-string (out) (funcall (cdr param) out)))
                                  (pg::pg-client-bound-query-parameters query))
                          '("1" "7"))))
             (setf queries nil)
             ;; Without K, the new tuple supplies the key, so a key marker is unusable.
             (ok (handler-case
                     (progn (pg::logical-apply-update
                             runtime relation (octets #(78 0 3 117 116 0 0 0 1 49 117)) 0)
                            nil)
                   (error () t)))
             (ok (null queries))
             ;; Unselected relations can carry unsupported types and remain ignored.
             (let ((excluded (pg::make-pgoutput-relation :name "excluded"
                                                         :columns (list id number body)
                                                         :key-columns (list id))))
               (setf (pg::pgoutput-column-type-oid body) 999999)
               (ok (null (pg::logical-apply-update runtime excluded bad-fixed 0)))
               (ok (null queries))
               (setf (pg::pgoutput-column-type-oid body) 17))
             (dolist (payload (list bad-old bad-fixed))
               (ok (handler-case (progn (pg::logical-apply-update runtime relation payload 0) nil)
                     (error () t))))
             (ok (handler-case
                     (progn (pg::logical-apply-insert runtime relation
                                                      (octets #(78 0 3 116 0 0 0 1 55 110 117)) 0) nil)
                   (error () t)))
             (ok (handler-case
                     (progn (pg::logical-apply-delete runtime relation
                                                      (octets #(75 0 3 116 0 0 0 1 55 110 117)) 0) nil)
                   (error () t)))
             (ok (handler-case
                     (progn (pg::logical-validate-pgoutput-tuple relation #("7" "1" :unchanged-toast) nil) nil)
                   (error () t)))
             (ok (handler-case
                     (progn (pg::logical-validate-pgoutput-tuple relation #("7" :unchanged-toast "v") t) nil)
                   (error () t)))
             (ok (pg::logical-validate-pgoutput-tuple relation #(:unchanged-toast "1" :unchanged-toast) t))
             (ok (handler-case
                     (progn (pg::logical-row-key-predicate
                             (pg::logical-pgoutput-key-values relation #(:unchanged-toast "1" "v"))
                             (list id)) nil)
                   (error () t))))
        (setf (symbol-function 'pg::logical-target-query) original))))

(deftest invalid-message-flags
    (dolist (flags '(1 2 128))
      (ok (handler-case
              (progn
                (pg::logical-pgoutput-read-message
                 (pg::make-logical-subscription-runtime)
                 (octets (list 77 flags 0 0 0 0 0 0 0 42 120 0 0 0 0 0)))
                nil)
            (simple-error () t)))))

(defun reference-frames ()
  (with-open-file
      (stream (asdf:system-relative-pathname
               "pg-interop-tests" "t/interop/20devel-reference.hex"))
    (loop for line = (read-line stream nil)
          while line
          unless (or (zerop (length line)) (char= (char line 0) #\#))
            collect (octets
                     (loop for offset from 0 below (length line) by 2
                           collect (parse-integer line :start offset :end (+ offset 2)
                                                       :radix 16))))))

(deftest c-generated-reference
    (let* ((frames (reference-frames))
           (updates (remove-if-not (lambda (frame) (= (aref frame 0) 85)) frames))
           (messages (remove-if-not (lambda (frame) (= (aref frame 0) 77)) frames))
           (truncate (find 84 frames :key (lambda (frame) (aref frame 0)))))
      (ok (= 24 (length frames)))
      (ok (= 2 (length updates)))
      (multiple-value-bind (old new) (pg::logical-pgoutput-read-update (first updates) 5)
        (ok (null old))
        (ok (equalp new #("1" "3"))))
      (multiple-value-bind (old new) (pg::logical-pgoutput-read-update (second updates) 5)
        (ok (equalp old #("1" nil)))
        (ok (equalp new #("4" "3"))))
      (ok (equal '(24587) (pg::logical-pgoutput-read-truncate truncate)))
      (ok (equalp truncate (pg::pgoutput-build-truncate '(24587))))
      (ok (= 2 (length messages)))
      (dolist (frame messages)
        (let* ((transactional-p (= (aref frame 1) 1))
               (runtime (pg::make-logical-subscription-runtime
                         :transaction-open-p transactional-p)))
          (multiple-value-bind (prefix lsn content flag)
              (pg::logical-pgoutput-read-message runtime frame)
            (ok (string= prefix "pg_lisp/ddl"))
            (ok (string= content "x"))
            (ok (eql transactional-p flag))
            (ok (equalp
                 frame
                 (pg::pgoutput-build-message
                  (pg::make-logical-change :kind :ddl :event-payload "x")
                  (pg::make-logical-transaction :xid (if flag 1 0) :commit-lsn lsn)))))))))

(deftest nontransactional-message-inside-transaction
    (multiple-value-bind (prefix lsn content transactional-p)
        (pg::logical-pgoutput-read-message
         (pg::make-logical-subscription-runtime :transaction-open-p t)
         (octets #(77 0 0 0 0 0 0 0 0 42 120 0 0 0 0 0)))
      (ok (equal (list prefix lsn content transactional-p) '("x" 42 "" nil)))))

(deftest excluded-full-identity-update
    (let ((runtime (pg::make-logical-subscription-runtime
                    :options (pg::make-logical-subscription-options)))
          (relation (pg::make-pgoutput-relation :name "excluded")))
      (ok (null
           (pg::logical-apply-update
            runtime relation
            (octets #(85 0 0 0 7 79 0 1 116 0 0 0 1 49
                      78 0 1 116 0 0 0 1 50)) 5)))))

(deftest truncate-requires-transaction
    (ok (handler-case
            (progn
              (pg::logical-pgoutput-dispatch
               (pg::make-logical-subscription-runtime)
               (octets #(84 0 0 0 0 0)))
              nil)
          (simple-error () t))))

(deftest native-truncate
    ;; logicalrep_write_truncate: tag, uint32 count, uint8 flags, OIDs.
    (let ((expected #(84 0 0 0 2 0 0 0 0 7 0 0 0 8)))
      (ok (equalp expected (pg::pgoutput-build-truncate '(7 8))))
      (ok (equal '(7 8) (pg::logical-pgoutput-read-truncate (octets expected))))))

(deftest unsupported-truncate-flags
    (dolist (flags '(1 2 3 128))
      (ok (handler-case
              (progn
                (pg::logical-pgoutput-read-truncate (octets (list 84 0 0 0 1 flags 0 0 0 7)))
                nil)
            (simple-error () t)))))

(deftest malformed-truncate
    (dolist (bytes '(#(84) #(84 0 0 0 1 0) #(84 0 0 0 0 0 0)))
      (ok (handler-case
              (progn (pg::logical-pgoutput-read-truncate (octets bytes)) nil)
            (simple-error () t)))))

(deftest update-without-old-key
    ;; U, relid=7, N, two text columns: id=1 and amount=2.
    (multiple-value-bind (old new)
        (pg::logical-pgoutput-read-update
         (octets #(85 0 0 0 7 78 0 2 116 0 0 0 1 49 116 0 0 0 1 50)) 5)
      (ok (null old))
      (ok (equalp new #("1" "2")))))

(deftest update-with-old-key
    ;; C retains relation width in K: non-key attributes are NULL.
    (multiple-value-bind (old new)
        (pg::logical-pgoutput-read-update
         (octets #(85 0 0 0 7 75 0 2 116 0 0 0 1 49 110
                   78 0 2 116 0 0 0 1 51 116 0 0 0 1 50)) 5)
      (ok (equalp old #("1" nil)))
      (ok (equalp new #("3" "2")))))

(deftest row-identity-layout
    (let* ((columns (list (list :attnum 1 :name "id" :type :int4)
                          (list :attnum 2 :name "amount" :type :int4)))
           (change (pg::make-logical-change
                    :kind :delete :relation-oid 7 :columns columns
                    :key-attnums '(1) :old-values #(1 2))))
      (ok (equalp (pg::pgoutput-build-row-change change)
                  #(68 0 0 0 7 75 0 2 116 0 0 0 1 49 110)))))

(deftest key-projection
    (let* ((amount (pg::make-pgoutput-column :name "amount" :type-oid 23))
           (id (pg::make-pgoutput-column :name "id" :type-oid 23 :key-p t))
           (relation (pg::make-pgoutput-relation
                      :columns (list amount id) :key-columns (list id))))
      (ok (equalp (pg::logical-pgoutput-key-values relation #(nil "7")) #("7")))))

(deftest ordinary-transactional-message
    ;; pgoutput_message passes InvalidTransactionId outside streamed blocks.
    (let* ((change (pg::make-logical-change
                    :kind :ddl :database-oid 1 :event-payload "x"))
           (transaction (pg::make-logical-transaction :xid 91 :commit-lsn 42))
           (expected (octets #(77 1 0 0 0 0 0 0 0 42
                               112 103 95 108 105 115 112 47 100 100 108 0
                               0 0 0 1 120))))
      (ok (equalp expected (pg::pgoutput-build-message change transaction)))
      (ok (equal (list "pg_lisp/ddl" 42 "x" t)
                 (multiple-value-list
                  (pg::logical-pgoutput-read-message
                   (pg::make-logical-subscription-runtime
                    :transaction-open-p t :source-xid 91)
                   expected))))))

(deftest standalone-message
    (let* ((change (pg::make-logical-change
                    :kind :ddl :database-oid 1 :event-payload "x"))
           (expected #(77 0 0 0 0 0 0 0 0 42
                       112 103 95 108 105 115 112 47 100 100 108 0
                       0 0 0 1 120)))
      (ok (equalp expected
                  (pg::pgoutput-build-message
                   change (pg::make-logical-transaction :xid 0 :commit-lsn 42))))))
