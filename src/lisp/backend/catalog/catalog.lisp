;;;; catalog.lisp -- pg_class / pg_attribute / pg_index as defstructs,
;;;; persisted to $PGDATA/global/pg_catalog.dat.

(in-package #:pg)

(defstruct pg-column
  name
  type              ; :int4 | :int8 | :varchar
  typmod            ; n+4 for varchar(n), else -1
  attnum
  notnull
  default-sequence-name
  default-expression
  default-sql
  storage)

(defun pg-column-effective-storage (column)
  "Return COLUMN's explicit storage policy or its type default."
  (or (pg-column-storage column) (sql-type-storage-code (pg-column-type column))))

;; Optional metadata owned by one pg-class, modeling the TOAST table and its
;; index identities (as in catalog/toasting.c).  This scaffold does not create
;; relations or implement external-value storage; the OIDs and relfilenodes are
;; distinct identities, and relfrozenxid belongs to the TOAST heap.

(defstruct pg-toast
  oid relfilenode index-oid index-relfilenode relfrozenxid)

(defstruct pg-class
  oid
  name
  relfilenode
  (relfrozenxid +first-normal-xid+)
  columns           ; simple-vector of pg-column, attnum order
  indexes           ; list of index OIDs
  statistics
  checks
  toast)       ; derived ANALYZE estimates; raw samples remain transient

(defstruct pg-index
  oid
  name
  relfilenode
  table-oid
  columns           ; list of attnums
  unique-p
  primary-p
  constraint-p      ; a table constraint depends on this index
  nulls-not-distinct-p
  valid-p)           ; false for legacy indexes that may be incomplete

(defstruct pg-column-statistics
  null-fraction
  distinct-count
  most-common-values)

(defstruct pg-table-statistics
  row-count
  sample-count
  columns)

(defstruct pg-sequence
  oid name increment start-value min-value max-value last-value called-p)

(defstruct pg-publication
  oid name all-tables-p tables sequences generation
  (publish-actions '(:insert :update :delete :truncate)))

(defun pg-index-constraint-owned-p (index)
  "Return whether a table constraint depends on INDEX.

Older catalogs have no CONSTRAINT field, but did persist PRIMARY."
  (or (pg-index-constraint-p index)
      (pg-index-primary-p index)))

(defstruct database
  name
  oid
  (tables (make-hash-table :test #'equal))
  (indexes (make-hash-table :test #'equal))
  (sequences (make-hash-table :test #'equal))
  (publications (make-hash-table :test #'equal))
  (applied-logical-event-ids (make-hash-table :test #'equal)))

(defun new-catalog-cluster-identity ()
  (format nil "~36r-~X"
          (get-universal-time) (random (ash 1 128))))

(defstruct catalog
  (cluster-identity (new-catalog-cluster-identity))
  (next-oid +first-normal-object-id+)
  (databases (make-hash-table :test #'equal)))

(defvar *catalog* nil)

(defun catalog-system-identifier (&optional (catalog *catalog*))
  "Stable protocol systemid: FNV-1a-64 of the durable UTF8 cluster identity.
The projection is part of the protocol contract; preserve its constants and
the original private cluster identity when loading or migrating catalogs."
  (let ((hash #xCBF29CE484222325))
    (loop for octet across (string->bytes (catalog-cluster-identity catalog))
          do (setf hash (logand #xFFFFFFFFFFFFFFFF
                                (* (logxor hash octet) #x100000001B3))))
    hash))

(defvar *catalog-identity-needs-persist-p* nil)
(defvar *catalog-mutex* (sb-thread:make-mutex :name "pg catalog mutex"))
(defvar *catalog-oldest-relfrozenxid* nil)
;; Incremented only after a catalog image has been durably committed.
(defvar *catalog-durable-generation* 0)
(defvar *catalog-save-error-after-durable-p* nil)
(defvar *logical-event-apply-id* nil)

(defvar *logical-source-schema-precondition* nil)

(defvar *logical-event-schema-contract* nil)

(defvar *catalog-logical-event-publication-p* nil)

(declaim (ftype function logical-validate-schema-contract-under-write-lock))

(defvar *logical-event-apply-lock*
  (sb-thread:make-mutex :name "logical event apply mutex"))
(declaim (ftype function logical-sequence-state-event))

;;; ---- serialization ----

(defmacro define-catalog-codec (name constructor &body fields)
  "Define NAME-TO-SEXP and NAME-FROM-SEXP from explicit field mappings.

Each FIELD is (SEXP-KEY ACCESSOR CONSTRUCTOR-KEY &key ENCODE DECODE DEFAULT).
DEFAULT applies only when the serialized key is absent, preserving explicit NIL."
  (let ((to-sexp (intern (format nil "~a-TO-SEXP" name) *package*))
        (from-sexp (intern (format nil "~a-FROM-SEXP" name) *package*)))
    `(progn
       (defun ,to-sexp (object)
         (list
          ,@(loop for field in fields
                  append
                  (destructuring-bind (key accessor constructor-key
                                       &key encode decode default)
                      field
                    (declare (ignore constructor-key decode default))
                    `(,key
                      ,(if encode
                           `(funcall ,encode (,accessor object))
                           `(,accessor object)))))))
       (defun ,from-sexp (sexp)
         (,constructor
          ,@(loop for field in fields
                  append
                  (destructuring-bind (key accessor constructor-key
                                       &key encode decode default)
                      field
                    (declare (ignore accessor encode))
                    `(,constructor-key
                      ,(if decode
                           `(funcall ,decode (getf sexp ,key ,default))
                           `(getf sexp ,key ,default))))))))))

(defun catalog-expression-to-sexp (expression)
  ;; Encode decimals explicitly so persisted expression trees round-trip without
  ;; relying on a Lisp structure reader or losing their display scale.
  (cond ((sql-jsonb-p expression)
         (list :pg-catalog-jsonb (sql-jsonb-output expression)))
        ((sql-timetz-p expression)
         (list :pg-catalog-timetz (sql-timetz-microseconds expression) (sql-timetz-zone-west expression)))
        ((sql-interval-p expression)
         (list :pg-catalog-interval (sql-interval-months expression)
               (sql-interval-days expression) (sql-interval-microseconds expression)))
        ((floatp expression)
         (let ((type (if (typep expression 'single-float) :float4 :float8)))
           (list :pg-catalog-float type (sql-float-bits expression type))))
        ((sql-decimal-p expression)
         (list :pg-catalog-numeric (sql-decimal-coefficient expression) (sql-decimal-scale expression)))
        ((consp expression)
         (cons (catalog-expression-to-sexp (car expression))
               (catalog-expression-to-sexp (cdr expression))))
        (t expression)))

(defun catalog-expression-from-sexp (expression)
  (cond ((and (consp expression) (eq (first expression) :pg-catalog-jsonb))
         (sql-jsonb-input (second expression)))
        ((and (consp expression) (eq (first expression) :pg-catalog-timetz))
         (make-sql-timetz (second expression) (third expression)))
        ((and (consp expression) (eq (first expression) :pg-catalog-interval))
         (make-sql-interval (second expression) (third expression) (fourth expression)))
        ((and (consp expression) (eq (first expression) :pg-catalog-float))
         (sql-float-from-bits (third expression) (second expression)))
        ((and (consp expression) (eq (first expression) :pg-catalog-numeric))
         (make-sql-decimal (second expression) (third expression)))
        ((consp expression)
         (cons (catalog-expression-from-sexp (car expression))
               (catalog-expression-from-sexp (cdr expression))))
        (t expression)))

(define-catalog-codec column make-pg-column
  (:name pg-column-name :name)
  (:type pg-column-type :type)
  (:typmod pg-column-typmod :typmod)
  (:attnum pg-column-attnum :attnum)
  (:notnull pg-column-notnull :notnull)
  (:default-sequence pg-column-default-sequence-name :default-sequence-name)
  (:default-expression pg-column-default-expression :default-expression
                       :encode #'catalog-expression-to-sexp :decode #'catalog-expression-from-sexp)
  (:default-sql pg-column-default-sql :default-sql)
  (:storage pg-column-storage :storage))

(define-catalog-codec sequence make-pg-sequence
  (:oid pg-sequence-oid :oid)
  (:name pg-sequence-name :name)
  (:increment pg-sequence-increment :increment)
  (:start pg-sequence-start-value :start-value)
  (:min pg-sequence-min-value :min-value)
  (:max pg-sequence-max-value :max-value)
  (:last pg-sequence-last-value :last-value)
  (:called pg-sequence-called-p :called-p))

(define-catalog-codec publication make-pg-publication
  (:oid pg-publication-oid :oid)
  (:name pg-publication-name :name)
  (:all-tables pg-publication-all-tables-p :all-tables-p)
  (:tables pg-publication-tables :tables)
  (:sequences pg-publication-sequences :sequences)
  (:generation pg-publication-generation :generation)
  (:publish-actions pg-publication-publish-actions :publish-actions
                    :default '(:insert :update :delete :truncate)))

(define-catalog-codec column-statistics make-pg-column-statistics
  (:null-fraction pg-column-statistics-null-fraction :null-fraction)
  (:distinct-count pg-column-statistics-distinct-count :distinct-count)
  (:most-common-values pg-column-statistics-most-common-values
   :most-common-values :encode #'catalog-expression-to-sexp
                       :decode #'catalog-expression-from-sexp))

(define-catalog-codec table-statistics make-pg-table-statistics
  (:row-count pg-table-statistics-row-count :row-count)
  (:sample-count pg-table-statistics-sample-count :sample-count)
  (:columns pg-table-statistics-columns :columns
            :encode (lambda (columns)
                      (map 'list #'column-statistics-to-sexp columns))
            :decode (lambda (columns)
                      (map 'vector #'column-statistics-from-sexp columns))))

(define-catalog-codec toast make-pg-toast
  (:oid pg-toast-oid :oid)
  (:relfilenode pg-toast-relfilenode :relfilenode)
  (:index-oid pg-toast-index-oid :index-oid)
  (:index-relfilenode pg-toast-index-relfilenode :index-relfilenode)
  (:relfrozenxid pg-toast-relfrozenxid :relfrozenxid))

(define-catalog-codec class make-pg-class
  (:oid pg-class-oid :oid)
  (:name pg-class-name :name)
  (:relfilenode pg-class-relfilenode :relfilenode)
  (:relfrozenxid pg-class-relfrozenxid :relfrozenxid
                 :decode (lambda (xid)
                           (or xid (xid-epoch-start *next-xid*))))
  (:columns pg-class-columns :columns
            :encode (lambda (columns)
                      (loop for col across columns collect (column-to-sexp col)))
            :decode (lambda (columns)
                      (coerce (mapcar #'column-from-sexp columns) 'vector)))
  (:indexes pg-class-indexes :indexes)
  (:toast pg-class-toast :toast
          :encode (lambda (toast) (and toast (toast-to-sexp toast)))
          :decode (lambda (toast) (and toast (toast-from-sexp toast))))
  (:checks pg-class-checks :checks
           :encode #'catalog-expression-to-sexp :decode #'catalog-expression-from-sexp)
  (:statistics pg-class-statistics :statistics
               :encode (lambda (statistics)
                         (and statistics (table-statistics-to-sexp statistics)))
               :decode (lambda (statistics)
                         (and statistics
                              (table-statistics-from-sexp statistics)))))

(define-catalog-codec index make-pg-index
  (:oid pg-index-oid :oid)
  (:name pg-index-name :name)
  (:relfilenode pg-index-relfilenode :relfilenode)
  (:table-oid pg-index-table-oid :table-oid)
  (:columns pg-index-columns :columns)
  (:unique pg-index-unique-p :unique-p)
  (:primary pg-index-primary-p :primary-p)
  (:constraint pg-index-constraint-p :constraint-p)
  (:nulls-not-distinct pg-index-nulls-not-distinct-p :nulls-not-distinct-p)
  (:valid pg-index-valid-p :valid-p))

(defun database-to-sexp (db)
  `(:name ,(database-name db)
    :oid ,(database-oid db)
    :tables ,(loop for c being the hash-values of (database-tables db)
                   collect (class-to-sexp c))
    :indexes ,(loop for i being the hash-values of (database-indexes db)
                    collect (index-to-sexp i))
    :sequences ,(loop for s being the hash-values of (database-sequences db)
                      collect (sequence-to-sexp s))
    :publications ,(loop for p being the hash-values of (database-publications db)
                         collect (publication-to-sexp p))
    :applied-logical-event-ids
    ,(loop for key being the hash-keys of
                                       (database-applied-logical-event-ids db)
             using (hash-value lsn)
           collect (cons key lsn))))

(defun database-from-sexp (s)
  (let ((db (make-database :name (getf s :name) :oid (getf s :oid))))
    (dolist (cs (getf s :tables))
      (let ((c (class-from-sexp cs)))
        (setf (gethash (pg-class-name c) (database-tables db)) c)))
    (dolist (is (getf s :indexes))
      (let ((i (index-from-sexp is)))
        (setf (gethash (pg-index-name i) (database-indexes db)) i)))
    (dolist (ss (getf s :sequences))
      (let ((sequence (sequence-from-sexp ss)))
        ;; Catalogs written before sequence limits were stored only increment
        ;; and start. Match sequence.c:init_params()' direction-sensitive
        ;; int8 defaults when such a catalog is loaded.
        (unless (pg-sequence-min-value sequence)
          (setf (pg-sequence-min-value sequence)
                (if (minusp (pg-sequence-increment sequence))
                    +int8-min+ 1)))
        (unless (pg-sequence-max-value sequence)
          (setf (pg-sequence-max-value sequence)
                (if (minusp (pg-sequence-increment sequence))
                    -1 +int8-max+)))
        (setf (gethash (pg-sequence-name sequence)
                       (database-sequences db)) sequence)))
    (dolist (ps (getf s :publications))
      (let ((publication (publication-from-sexp ps)))
        (setf (gethash (pg-publication-name publication)
                       (database-publications db)) publication)))
    (dolist (entry (getf s :applied-logical-event-ids))
      (if (consp entry)
          (setf (gethash (car entry) (database-applied-logical-event-ids db))
                (cdr entry))
          ;; Accept the short-lived exact-ID list format written during the
          ;; initial logical-event implementation.
          (setf (gethash entry (database-applied-logical-event-ids db)) 0)))
    db))

(defun catalog-to-sexp (cat)
  `(:cluster-identity ,(catalog-cluster-identity cat)
    :next-oid ,(catalog-next-oid cat)
    :databases ,(loop for db being the hash-values of (catalog-databases cat)
                      collect (database-to-sexp db))))

(defun catalog-from-sexp (s)
  (unless (getf s :cluster-identity)
    (setf *catalog-identity-needs-persist-p* t))
  (let ((cat (make-catalog :cluster-identity
                           (or (getf s :cluster-identity)
                               (new-catalog-cluster-identity))
                           :next-oid (getf s :next-oid))))
    (dolist (ds (getf s :databases))
      (let ((db (database-from-sexp ds)))
        (setf (gethash (database-name db) (catalog-databases cat)) db)))
    cat))

(defun catalog-path ()
  (pgdata-path "global/pg_catalog.dat"))

(defun catalog-image-bytes ()
  (string->bytes
   (with-output-to-string (out)
     (with-standard-io-syntax
       (let ((*print-readably* t)
             (*print-pretty* t))
         (prin1 (catalog-to-sexp *catalog*) out)
         (terpri out))))))

(defvar *catalog-rename-function* nil)

(defun write-catalog-image (bytes)
  (when *catalog-logical-event-publication-p*
    (logical-test-failpoint :after-catalog-wal-flush-before-file-publication))
  (with-atomic-output (stream
                       (catalog-path)
                       (pgdata-path "global/pg_catalog.tmp")
                       (pgdata-path "global/")
                       :element-type '(unsigned-byte 8)
                       :rename-function (or *catalog-rename-function*
                                            #'sb-posix:rename))
    ;; Atomic rename keeps the old catalog present until the new image is
    ;; durable, so a crash cannot leave the catalog name missing.
    (write-sequence bytes stream)))

(defun catalog-physical-relfilenodes ()
  "Return catalog-referenced physical nodes, including owned TOAST heap and index.
The legacy WAL inventory supplies table and ordinary index nodes; this adds the
TOAST nodes owned by each table.  The result may contain duplicates."
  (let ((nodes (wal-relation-filennodes)))
    (maphash
     (lambda (name database)
       (declare (ignore name))
       (maphash
        (lambda (name table)
          (declare (ignore name))
          (let ((toast (pg-class-toast table)))
            (when toast
              (push (pg-toast-relfilenode toast) nodes)
              (push (pg-toast-index-relfilenode toast) nodes))))
        (database-tables database)))
     (catalog-databases *catalog*))
    nodes))

(defun catalog-remove-orphan-relations ()
  "Remove unreferenced numeric main and visibility relation files at startup.
Use the complete catalog inventory, including table-owned TOAST nodes.  Forget
each unknown node through storage manager deletion, and sync base/ if any were
removed.  Other names and fork types are not selected by this scan."
  (let ((known (catalog-physical-relfilenodes))
        (base (merge-pathnames "base/" *pgdata*))
        (removed nil))
    (dolist (path (directory (merge-pathnames "*" base)))
      (let ((relfilenode (ignore-errors
                          (and (member (pathname-type path) '(nil "vm") :test #'equal)
                               (parse-integer (pathname-name path))))))
        (when (and relfilenode (not (member relfilenode known :test #'eql)))
          (smgr-forget relfilenode :delete t)
          (setf removed t))))
    (when removed (fsync-directory base)))
  (values))

(defun catalog-compute-oldest-relfrozenxid ()
  "Return the oldest recorded heap horizon across all databases, or NIL.
Include each owned TOAST heap because its tuples can retain transaction IDs
independently of the parent heap.  Like PostgreSQL's vac_update_datfrozenxid,
the minimum covers both ordinary and TOAST relations; this catalog computes a
cluster-wide minimum directly and skips missing horizons."
  (let ((oldest nil))
    (when *catalog*
      (maphash
       (lambda (database-name database)
         (declare (ignore database-name))
         (maphash
          (lambda (table-name table)
            (declare (ignore table-name))
            (dolist (relfrozenxid (list (pg-class-relfrozenxid table)
                                        (and (pg-class-toast table)
                                             (pg-toast-relfrozenxid (pg-class-toast table)))))
              (when (and relfrozenxid
                         (or (null oldest) (< relfrozenxid oldest)))
                (setf oldest relfrozenxid))))
          (database-tables database)))
       (catalog-databases *catalog*)))
    oldest))

(defun catalog-refresh-oldest-relfrozenxid ()
  (setf *catalog-oldest-relfrozenxid*
        (catalog-compute-oldest-relfrozenxid)))

(defun catalog-oldest-relfrozenxid ()
  (or *catalog-oldest-relfrozenxid*
      (catalog-compute-oldest-relfrozenxid)))

(defun save-catalog (&key deferred-relations logical-event on-record-appended
                       (applied-logical-event-id *logical-event-apply-id*))
  (prevent-recovery-write "catalog persistence")
  ;; Do fallible derived-state work before publication. Once WAL commits, the
  ;; generation update and cache assignment below must not signal: callers use
  ;; the generation to distinguish rollback-safe failures from durable ones.
  (let* ((event-key (car applied-logical-event-id))
         (event-lsn (cdr applied-logical-event-id))
         (database (and event-key *backend* (current-database)))
         (previous-event-lsn (and database
                                  (gethash event-key
                                           (database-applied-logical-event-ids
                                            database))))
         (previous-event-present-p
           (and database
                (nth-value 1
                           (gethash event-key
                                    (database-applied-logical-event-ids database)))))
         (wal-generation (wal-catalog-flush-generation)))
    (when database
      (setf (gethash event-key (database-applied-logical-event-ids database))
            event-lsn))
    (handler-case
        (let ((oldest (catalog-compute-oldest-relfrozenxid)))
          (when (and logical-event *logical-event-schema-contract*)
            (logical-validate-schema-contract-under-write-lock
             (getf *logical-event-schema-contract* :schema-after) :after))
          (when logical-event
            (logical-test-failpoint :before-catalog-event-wal-terminal))
          (let ((*catalog-logical-event-publication-p*
                  (not (null logical-event))))
            (wal-commit-catalog
             (catalog-image-bytes) #'write-catalog-image
             :relation-deletes deferred-relations
             :logical-event logical-event
             :on-record-appended on-record-appended))
          (incf *catalog-durable-generation*)
          (setf *catalog-oldest-relfrozenxid* oldest))
      (error (condition)
        (if (> (wal-catalog-flush-generation) wal-generation)
            (setf *catalog-save-error-after-durable-p* t)
            (when database
              (if previous-event-present-p
                  (setf (gethash event-key
                                 (database-applied-logical-event-ids database))
                        previous-event-lsn)
                  (remhash event-key
                           (database-applied-logical-event-ids database)))))
        (error condition)))
    (when logical-event
      ;; The catalog terminal is now durable. Crashing here leaves a replayable
      ;; standalone event, matching the subscriber's target-commit/feedback gap.
      (logical-test-failpoint :after-catalog-event-wal-terminal))
    (values)))

(defun load-catalog ()
  (let ((path (catalog-path)))
    (if (probe-file path)
        (with-open-file (s path :direction :input :if-does-not-exist nil)
          (if s
              (with-standard-io-syntax
                (let ((*read-eval* nil))
                  (catalog-from-sexp (read s nil nil))))
              (make-catalog)))
        (make-catalog))))

(defun allocate-oid ()
  (prog1 (catalog-next-oid *catalog*)
    (incf (catalog-next-oid *catalog*))))

(defun catalog-upgrade-publication-oids (catalog)
  "Assign durable identities to legacy publications before admitting clients."
  (let ((changed-p nil))
    (dolist (database (sort (loop for db being the hash-values of (catalog-databases catalog)
                                  collect db)
                            #'string< :key #'database-name))
      (dolist (publication
               (sort (loop for pub being the hash-values of (database-publications database)
                           collect pub)
                     #'string< :key #'pg-publication-name))
        (unless (pg-publication-oid publication)
          (setf (pg-publication-oid publication) (catalog-next-oid catalog)
                changed-p t)
          (incf (catalog-next-oid catalog)))))
    changed-p))

(defun catalog-init ()
  (setf *catalog-identity-needs-persist-p* nil
        *catalog* (or (load-catalog) (make-catalog)))
  (when (catalog-upgrade-publication-oids *catalog*)
    (setf *catalog-identity-needs-persist-p* t))
  (when *catalog-identity-needs-persist-p*
    ;; Persist generated cluster/publication identities before any client can
    ;; observe them, so restart retains the same publisher incarnation and OIDs.
    (save-catalog)
    (setf *catalog-identity-needs-persist-p* nil))
  (unless (gethash "postgres" (catalog-databases *catalog*))
    (let ((db (make-database :name "postgres" :oid (allocate-oid))))
      (setf (gethash "postgres" (catalog-databases *catalog*)) db)
      (save-catalog)))
  (catalog-refresh-oldest-relfrozenxid)
  (values))

(defun freeze-all-heap-xids (old-epoch
                             &key (fsync-function #'smgr-fsync)
                               after-page-function)
  "Incrementally freeze catalog relations before reusing 32-bit XIDs.

The all-frozen visibility-map bit lets prior VACUUM work skip pages that no
longer contain transaction-status references."
  (let ((boundary (+ (* (1+ old-epoch) +xid-cycle+)
                     +first-normal-xid+)))
    (vacuum-all-relations-for-xid-wraparound
     boundary :fsync-function fsync-function
     :after-page-function after-page-function)))

(defun get-database (name)
  (or (gethash name (catalog-databases *catalog*))
      (let ((db (make-database :name name :oid (allocate-oid))))
        (setf (gethash name (catalog-databases *catalog*)) db)
        db)))

(defun current-database ()
  (get-database (backend-database *backend*)))

(defun strip-schema (name)
  (if (and (> (length name) 7)
           (string= name "public." :end1 7))
      (subseq name 7)
      name))

(defun lookup-table (name)
  (gethash (strip-schema name) (database-tables (current-database))))

(defun ensure-current-table (class)
  "Raise 42P01 when CLASS was dropped or replaced after it was resolved."
  (unless (eq class (lookup-table (pg-class-name class)))
    (ereport "42P01" "relation \"~a\" does not exist" (pg-class-name class)))
  class)

(defun lookup-index (name)
  (gethash (strip-schema name) (database-indexes (current-database))))

(defun lookup-sequence (name)
  (gethash (strip-schema name) (database-sequences (current-database))))

(defun lookup-publication (name)
  (gethash name (database-publications (current-database))))

(defun sequence-value-range-p (value)
  (and (integerp value) (<= +int8-min+ value +int8-max+)))

(defun sequence-default-limits (increment)
  "Return direction-sensitive int8 bounds as sequence.c:init_params() does."
  (if (minusp increment)
      (values +int8-min+ -1)
      (values 1 +int8-max+)))

(defstruct catalog-mutation-snapshot
  database next-oid table-entries index-entries sequence-entries
  publication-entries applied-event-entries table-states index-states
  sequence-states publication-states pending-relation-deletes)

(defun catalog-mutation-hash-entries (table)
  (loop for key being the hash-keys of table using (hash-value value)
        collect (cons key value)))

(defun catalog-mutation-copy-class (class)
  ;; Rollback owns independent column and CHECK trees and TOAST metadata, so a
  ;; failed pre-durable mutation cannot leave partial changes in the table.
  (let ((copy (copy-pg-class class)))
    (setf (pg-class-columns copy)
          (map 'vector (lambda (column)
                         (let ((copy (copy-pg-column column)))
                           (setf (pg-column-default-expression copy)
                                 (copy-tree (pg-column-default-expression column)))
                           copy))
               (pg-class-columns class))
          (pg-class-toast copy) (and (pg-class-toast class)
                                     (copy-pg-toast (pg-class-toast class)))
          (pg-class-indexes copy) (copy-list (pg-class-indexes class))
          (pg-class-checks copy) (copy-tree (pg-class-checks class)))
    copy))

(defun catalog-mutation-copy-index (index)
  (let ((copy (copy-pg-index index)))
    (setf (pg-index-columns copy) (copy-list (pg-index-columns index)))
    copy))

(defun catalog-mutation-copy-publication (publication)
  (let ((copy (copy-pg-publication publication)))
    (setf (pg-publication-tables copy)
          (copy-tree (pg-publication-tables publication))
          (pg-publication-sequences copy)
          (copy-list (pg-publication-sequences publication)))
    copy))

(defun catalog-capture-mutation-snapshot (database)
  (let ((tables (catalog-mutation-hash-entries (database-tables database)))
        (indexes (catalog-mutation-hash-entries (database-indexes database)))
        (sequences (catalog-mutation-hash-entries
                    (database-sequences database)))
        (publications (catalog-mutation-hash-entries
                       (database-publications database))))
    (make-catalog-mutation-snapshot
     :database database
     :next-oid (catalog-next-oid *catalog*)
     :table-entries tables
     :index-entries indexes
     :sequence-entries sequences
     :publication-entries publications
     :applied-event-entries
     (catalog-mutation-hash-entries
      (database-applied-logical-event-ids database))
     :table-states
     (mapcar (lambda (entry)
               (cons (cdr entry) (catalog-mutation-copy-class (cdr entry))))
             tables)
     :index-states
     (mapcar (lambda (entry)
               (cons (cdr entry) (catalog-mutation-copy-index (cdr entry))))
             indexes)
     :sequence-states
     (mapcar (lambda (entry)
               (cons (cdr entry) (copy-pg-sequence (cdr entry))))
             sequences)
     :publication-states
     (mapcar (lambda (entry)
               (cons (cdr entry)
                     (catalog-mutation-copy-publication (cdr entry))))
             publications)
     :pending-relation-deletes
     (sb-thread:with-mutex (*wal-lock*)
       (copy-list *wal-pending-deletes*)))))

(defun catalog-mutation-restore-hash (table entries)
  (clrhash table)
  (dolist (entry entries)
    (setf (gethash (car entry) table) (cdr entry))))

(defun catalog-mutation-restore-class (class saved)
  (setf (pg-class-oid class) (pg-class-oid saved)
        (pg-class-name class) (pg-class-name saved)
        (pg-class-relfilenode class) (pg-class-relfilenode saved)
        (pg-class-relfrozenxid class) (pg-class-relfrozenxid saved)
        (pg-class-toast class) (pg-class-toast saved)
        (pg-class-columns class) (pg-class-columns saved)
        (pg-class-indexes class) (pg-class-indexes saved)
        (pg-class-statistics class) (pg-class-statistics saved)
        (pg-class-checks class) (pg-class-checks saved)))

(defun catalog-mutation-restore-index (index saved)
  (setf (pg-index-oid index) (pg-index-oid saved)
        (pg-index-name index) (pg-index-name saved)
        (pg-index-relfilenode index) (pg-index-relfilenode saved)
        (pg-index-table-oid index) (pg-index-table-oid saved)
        (pg-index-columns index) (pg-index-columns saved)
        (pg-index-unique-p index) (pg-index-unique-p saved)
        (pg-index-primary-p index) (pg-index-primary-p saved)
        (pg-index-constraint-p index) (pg-index-constraint-p saved)
        (pg-index-nulls-not-distinct-p index)
        (pg-index-nulls-not-distinct-p saved)
        (pg-index-valid-p index) (pg-index-valid-p saved)))

(defun catalog-mutation-restore-sequence (sequence saved)
  (setf (pg-sequence-oid sequence) (pg-sequence-oid saved)
        (pg-sequence-name sequence) (pg-sequence-name saved)
        (pg-sequence-increment sequence) (pg-sequence-increment saved)
        (pg-sequence-start-value sequence) (pg-sequence-start-value saved)
        (pg-sequence-min-value sequence) (pg-sequence-min-value saved)
        (pg-sequence-max-value sequence) (pg-sequence-max-value saved)
        (pg-sequence-last-value sequence) (pg-sequence-last-value saved)
        (pg-sequence-called-p sequence) (pg-sequence-called-p saved)))

(defun catalog-mutation-restore-publication (publication saved)
  (setf (pg-publication-oid publication) (pg-publication-oid saved)
        (pg-publication-name publication) (pg-publication-name saved)
        (pg-publication-all-tables-p publication)
        (pg-publication-all-tables-p saved)
        (pg-publication-tables publication) (pg-publication-tables saved)
        (pg-publication-sequences publication)
        (pg-publication-sequences saved)
        (pg-publication-generation publication)
        (pg-publication-generation saved)
        (pg-publication-publish-actions publication) (pg-publication-publish-actions saved)))

(defun catalog-restore-mutation-snapshot (snapshot)
  (let ((database (catalog-mutation-snapshot-database snapshot)))
    (catalog-mutation-restore-hash
     (database-tables database)
     (catalog-mutation-snapshot-table-entries snapshot))
    (catalog-mutation-restore-hash
     (database-indexes database)
     (catalog-mutation-snapshot-index-entries snapshot))
    (catalog-mutation-restore-hash
     (database-sequences database)
     (catalog-mutation-snapshot-sequence-entries snapshot))
    (catalog-mutation-restore-hash
     (database-publications database)
     (catalog-mutation-snapshot-publication-entries snapshot))
    (catalog-mutation-restore-hash
     (database-applied-logical-event-ids database)
     (catalog-mutation-snapshot-applied-event-entries snapshot))
    (dolist (state (catalog-mutation-snapshot-table-states snapshot))
      (catalog-mutation-restore-class (car state) (cdr state)))
    (dolist (state (catalog-mutation-snapshot-index-states snapshot))
      (catalog-mutation-restore-index (car state) (cdr state)))
    (dolist (state (catalog-mutation-snapshot-sequence-states snapshot))
      (catalog-mutation-restore-sequence (car state) (cdr state)))
    (dolist (state (catalog-mutation-snapshot-publication-states snapshot))
      (catalog-mutation-restore-publication (car state) (cdr state)))
    (setf (catalog-next-oid *catalog*)
          (catalog-mutation-snapshot-next-oid snapshot))
    (sb-thread:with-mutex (*wal-lock*)
      (setf *wal-pending-deletes*
            (copy-list
             (catalog-mutation-snapshot-pending-relation-deletes snapshot)))))
  (values))

(defun call-with-catalog-durability-rollback-under-lock (rollback function)
  (let ((*catalog-save-error-after-durable-p* nil)
        (durable-generation *catalog-durable-generation*))
    (handler-case
        (funcall function)
      (error (condition)
        (unless (or *catalog-save-error-after-durable-p*
                    (> *catalog-durable-generation* durable-generation))
          (funcall rollback))
        (error condition)))))

(defun call-with-catalog-mutation-rollback-under-lock (database function)
  ;; tablecmds.c rolls catalog changes back with the surrounding transaction.
  ;; This project has no catalog transaction manager, so snapshot the supported
  ;; catalog maps and pending relfile deletes through the same WAL boundary.
  (let ((snapshot (catalog-capture-mutation-snapshot database)))
    (call-with-catalog-durability-rollback-under-lock
     (lambda () (catalog-restore-mutation-snapshot snapshot))
     function)))

(defmacro with-catalog-mutation-rollback ((database) &body body)
  `(call-with-catalog-mutation-rollback-under-lock
    ,database
    (lambda () ,@body)))

                                        ; errors after the WAL terminal leave its state published.

(defun call-with-catalog-durability-rollback (rollback function)
  "Run a catalog mutation and undo pre-durable failures under the catalog locks.

Holding both locks through the failure decision prevents another catalog writer
from advancing the shared WAL generation between a failed save and rollback."
  (with-write-catalog-locks
    (call-with-catalog-durability-rollback-under-lock rollback function)))

(defun sequence-nextval (name)
  "Advance and durably save NAME before returning its next value."
  (prevent-recovery-write "nextval")
  (when (and *backend* (backend-read-only *backend*))
    (ereport "25006" "cannot execute nextval in a read-only transaction"))
  (when (and *backend* (eq (backend-state *backend*) :in-progress))
    (logical-note-xid-mutation-needs-capture-p (get-current-xid)))
  (with-write-lock
    (let* ((sequence (lookup-sequence name))
           (old-value (and sequence (pg-sequence-last-value sequence)))
           (old-called (and sequence (pg-sequence-called-p sequence))))
      (unless sequence
        (ereport "42P01" "sequence \"~a\" does not exist" name))
      (let ((value (if old-called
                       (+ old-value (pg-sequence-increment sequence))
                       old-value)))
        (unless (<= (pg-sequence-min-value sequence) value
                    (pg-sequence-max-value sequence))
          (ereport "2200H" "sequence \"~a\" has run out" name))
        (setf (pg-sequence-last-value sequence) value
              (pg-sequence-called-p sequence) t)
        (let ((generation (wal-catalog-flush-generation)))
          (handler-case
              (save-catalog :logical-event
                            (logical-sequence-state-event name))
            (error (condition)
              (unless (> (wal-catalog-flush-generation) generation)
                (setf (pg-sequence-last-value sequence) old-value
                      (pg-sequence-called-p sequence) old-called))
              (error condition))))
        (setf (gethash (pg-sequence-oid sequence)
                       (backend-sequence-currvals *backend*)) value)
        value))))

(defun sequence-currval (name)
  (let* ((sequence (lookup-sequence name))
         (value (and sequence
                     (gethash (pg-sequence-oid sequence)
                              (backend-sequence-currvals *backend*)))))
    (unless sequence
      (ereport "42P01" "sequence \"~a\" does not exist" name))
    (unless value
      (ereport "55000"
               "currval of sequence \"~a\" is not yet defined in this session"
               name))
    value))

(defun sequence-setval (name value &optional (called-p t))
  "Set and durably save a sequence value; this operation is not transactional."
  (prevent-recovery-write "setval")
  (when (and *backend* (backend-read-only *backend*))
    (ereport "25006" "cannot execute setval in a read-only transaction"))
  (unless (sequence-value-range-p value)
    (ereport "22003" "sequence value is out of range"))
  (when (and *backend* (eq (backend-state *backend*) :in-progress))
    (logical-note-xid-mutation-needs-capture-p (get-current-xid)))
  (with-write-lock
    (let* ((sequence (lookup-sequence name))
           (old-value (and sequence (pg-sequence-last-value sequence)))
           (old-called (and sequence (pg-sequence-called-p sequence))))
      (unless sequence
        (ereport "42P01" "sequence \"~a\" does not exist" name))
      (unless (<= (pg-sequence-min-value sequence) value
                  (pg-sequence-max-value sequence))
        (ereport "22003"
                 "setval: value ~a is out of bounds for sequence \"~a\" (~a..~a)"
                 value name (pg-sequence-min-value sequence)
                 (pg-sequence-max-value sequence)))
      (setf (pg-sequence-last-value sequence) value
            (pg-sequence-called-p sequence) (not (null called-p)))
      (let ((generation (wal-catalog-flush-generation)))
        (handler-case
            (save-catalog :logical-event
                          (logical-sequence-state-event name))
          (error (condition)
            (unless (> (wal-catalog-flush-generation) generation)
              (setf (pg-sequence-last-value sequence) old-value
                    (pg-sequence-called-p sequence) old-called))
            (error condition))))
      (when called-p
        ;; SetSequence() in src/backend/commands/sequence.c updates the current
        ;; backend's currval cache only for iscalled=true.
        (setf (gethash (pg-sequence-oid sequence)
                       (backend-sequence-currvals *backend*)) value))
      value)))

(defun publication-table-entry (publication table-name)
  (when (pg-publication-all-tables-p publication)
    (return-from publication-table-entry
      (when (gethash table-name (database-tables (current-database)))
        (list :name table-name :filter nil))))
  (let* ((entry (find table-name (pg-publication-tables publication)
                      :key (lambda (candidate) (getf candidate :name))
                      :test #'string=))
         (table (gethash table-name (database-tables (current-database))))
         (entry-oid (and entry (getf entry :table-oid))))
    (when (and entry table
               (or (null entry-oid) (= entry-oid (pg-class-oid table))))
      entry)))

(defun publication-selects-table-p (publication table-name)
  (or (pg-publication-all-tables-p publication)
      (not (null (publication-table-entry publication table-name)))))

(defun publication-selected-table-names (publication db)
  (if (pg-publication-all-tables-p publication)
      (sort (loop for name being the hash-keys of (database-tables db)
                  collect name)
            #'string<)
      (mapcar (lambda (entry) (getf entry :name))
              (remove-if-not
               (lambda (entry)
                 (let* ((table (gethash (getf entry :name)
                                        (database-tables db)))
                        (oid (getf entry :table-oid)))
                   (and table (or (null oid) (= oid (pg-class-oid table))))))
               (pg-publication-tables publication)))))

(defun catalog-expression-sequence-names (expression)
  ;; Stored expressions admit only constant sequence names, making their
  ;; dependencies available to publication and bootstrap inventory.
  (let ((names nil))
    (labels ((walk (node)
               (when (consp node)
                 (when (and (eq (first node) :func)
                            (member (second node) '("nextval" "currval" "setval") :test #'string=))
                   (let ((argument (first (third node))))
                     (when (and (eq (first argument) :const) (stringp (second argument)))
                       (pushnew (strip-schema (second argument)) names :test #'string=))))
                 (walk (car node)) (walk (cdr node)))))
      (walk expression))
    names))

(defun catalog-table-sequence-names (table)
  (remove-duplicates
   (append (loop for column across (pg-class-columns table)
                 append (append (when (pg-column-default-sequence-name column)
                                  (list (pg-column-default-sequence-name column)))
                                (catalog-expression-sequence-names (pg-column-default-expression column))))
           (loop for check in (pg-class-checks table)
                 append (catalog-expression-sequence-names (getf check :expression))))
   :test #'string=))

(defun publication-sequence-names (publication db)
  (let ((selected (if (eq (pg-publication-sequences publication) :all)
                      (loop for name being the hash-keys of (database-sequences db) collect name)
                      (copy-list (pg-publication-sequences publication)))))
    (dolist (table-name (publication-selected-table-names publication db))
      (let ((table (gethash table-name (database-tables db))))
        (when table
          (dolist (name (catalog-table-sequence-names table))
            (pushnew name selected :test #'string=)))))
    (sort selected #'string<)))

(defun catalog-sql-quote-ident (name)
  (with-output-to-string (stream)
    (write-char #\" stream)
    (loop for char across name
          do (when (char= char #\") (write-char #\" stream))
             (write-char char stream))
    (write-char #\" stream)))

(defun catalog-sql-literal (value)
  (with-output-to-string (stream)
    (write-char #\' stream)
    (loop for char across value
          do (when (char= char #\') (write-char #\' stream))
             (write-char char stream))
    (write-char #\' stream)))

(defun catalog-temporal-constant-p (expression)
  "Detect cooked temporal constants whose meaning must survive replay."
  (and (consp expression)
       (or (and (eq (first expression) :const)
                (member (getf (cddr expression) :type)
                        '(:date :time :timetz :timestamp :timestamptz :interval)))
           (and (member (first expression) '(:any :all))
                (consp (fourth expression)) (eq (first (fourth expression)) :scalar-array-const)
                (member (getf (cddddr expression) :element-target)
                        '(:date :time :timetz :timestamp :timestamptz :interval)))
           (catalog-temporal-constant-p (car expression))
           (catalog-temporal-constant-p (cdr expression)))))

(defun catalog-expression-type-sql (type typmod)
  "Render an expression type and its effective typmod for canonical replay."
  (case type
    (:date "date") (:time (if (minusp typmod) "time" (format nil "time(~d)" typmod)))
    (:timetz (format nil "time~a with time zone" (if (minusp typmod) "" (format nil "(~d)" typmod))))
    ((:timestamp :timestamptz)
     (format nil "timestamp~a~a" (if (minusp typmod) "" (format nil "(~d)" typmod))
             (if (eq type :timestamptz) " with time zone" "")))
    (:interval (if (and (not (minusp typmod))
                        (/= (ldb (byte 16 0) typmod) #xffff)
                        (/= (ldb (byte 15 16) typmod) 32767)
                        (not (logbitp 12 (ldb (byte 15 16) typmod))))
                   (format nil "\"interval\"(~d,~d)" (ldb (byte 15 16) typmod) (ldb (byte 16 0) typmod))
                   (format nil "interval~a" (sql-interval-typmod-sql typmod))))
    (:numeric (if (< typmod 4) "numeric"
                  (let ((packed (- typmod 4)))
                    (format nil "numeric(~d,~d)" (ldb (byte 16 16) packed)
                            (- (logxor (logand packed #x7ff) #x400) #x400)))))
    (:varchar (if (< typmod 4) "varchar" (format nil "varchar(~d)" (- typmod 4))))
    (:int2 "smallint") (:int4 "integer") (:int8 "bigint") (:float4 "real")
    (:float8 "double precision") (:bool "boolean") (:text "text") (:json "json") (:jsonb "jsonb") (:bytea "bytea") (:name "name")
    (:oid "oid") (:pg-lsn "pg_lsn") (:bit (if (minusp typmod) "\"bit\"" (format nil "bit(~d)" typmod)))
    (:char "\"char\"") (:int2vector "int2vector") (:text-array "text[]")
    (otherwise (ereport "0A000" "cannot serialize stored expression type ~a" type))))

(defun catalog-temporal-value-text (value type)
  "Format a temporal value in a session-independent representation."
  (case type
    (:date (sql-date-output value)) (:time (sql-time-output value))
    (:timetz (sql-timetz-output value)) (:timestamp (sql-timestamp-output value nil))
    (:timestamptz (sql-timestamp-output value t)) (:interval (sql-interval-output value))
    (otherwise (ereport "0A000" "cannot serialize temporal array type ~a" type))))

(defun catalog-canonical-constant-sql (expression)
  "Render a cooked constant without consulting session format settings."
  (let* ((value (second expression)) (type (getf (cddr expression) :type))
                                     (typmod (or (getf (cddr expression) :typmod) -1))
                                     (type-sql (unless (eq type :pg-node-tree) (catalog-expression-type-sql type typmod))))
    (when (eq type :pg-node-tree)
      (unless (eq value +null+) (ereport "0A000" "cannot serialize pg_node_tree constant"))
      (return-from catalog-canonical-constant-sql "NULL"))
    (if (eq value +null+) (format nil "CAST(NULL AS ~a)" type-sql)
        (let ((text (case type
                      (:date (sql-date-output value)) (:time (sql-time-output value))
                      (:timetz (sql-timetz-output value)) (:timestamp (sql-timestamp-output value nil))
                      (:timestamptz (sql-timestamp-output value t))
                      (:interval (sql-interval-output value))
                      ((:float4 :float8) (sql-float-output value type 3))
                      (:numeric (sql-numeric-output value))
                      (:bool (if value "true" "false"))
                      ((:int2 :int4 :int8 :oid) (princ-to-string value))
                      ((:text :varchar :name) value)
                      (otherwise (value->text value type)))))
          (format nil "CAST(~a AS ~a)" (catalog-sql-literal text) type-sql)))))

(defun catalog-canonical-expression-sql (expression &optional table)
  "Deparse a cooked stored expression without re-evaluating it."
  (labels ((render (node)
             (unless (consp node) (ereport "0A000" "cannot serialize stored expression ~s" node))
             (case (first node)
               (:assignment (render (second node)))
               (:const (catalog-canonical-constant-sql node))
               (:bool (if (second node) "TRUE" "FALSE"))
               (:col (let* ((index (1- (second node)))
                            (columns (and table (pg-class-columns table))))
                       (unless (and columns (<= 0 index) (< index (length columns)))
                         (ereport "0A000" "cannot serialize stored expression column"))
                       (catalog-sql-quote-ident (pg-column-name (aref columns index)))))
               (:cast (format nil "CAST(~a AS ~a)" (render (second node))
                              (catalog-expression-type-sql (getf (cddr node) :type)
                                                           (or (getf (cddr node) :typmod) -1))))
               (:unary (format nil "(~a ~a)" (second node) (render (third node))))
               (:binop (format nil "(~a ~a ~a)" (render (third node)) (second node) (render (fourth node))))
               (:concat (format nil "(~a || ~a)" (render (second node)) (render (third node))))
               ((:and :or) (format nil "(~a ~a ~a)" (render (second node))
                                   (if (eq (first node) :and) "AND" "OR") (render (third node))))
               (:not (format nil "(NOT ~a)" (render (second node))))
               (:is-null (format nil "(~a IS ~aNULL)" (render (second node)) (if (third node) "NOT " "")))
               (:is-bool (format nil "(~a IS ~a~a)" (render (second node))
                                 (if (third node) "NOT " "") (if (fourth node) "TRUE" "FALSE")))
               (:in (format nil "(~a ~aIN (~{~a~^, ~}))" (render (second node))
                            (if (fourth node) "NOT " "") (mapcar #'render (third node))))
               ((:any :all)
                (let* ((array (fourth node))
                       (element-type (getf (cddddr node) :element-target))
                       (array-sql
                         (cond
                           ((and (consp array) (eq (first array) :scalar-array-const))
                            (catalog-sql-literal
                             (sql-text-array-output
                              (map 'vector (lambda (value)
                                             (cond ((eq value +null+) +null+)
                                                   ((member element-type '(:date :time :timetz :timestamp :timestamptz :interval))
                                                    (catalog-temporal-value-text value element-type))
                                                   ((member element-type '(:float4 :float8))
                                                    (sql-float-output value element-type 3))
                                                   (t (value->text value element-type))))
                                   (second array)))))
                           ((and (consp array) (eq (first array) :const) (eq (second array) +null+)
                                 (not (getf (cddr array) :explicit-cast))) "NULL")
                           (t (render array)))))
                  (format nil "(~a ~a ~a (~a))" (render (third node)) (second node)
                          (if (eq (first node) :any) "ANY" "ALL") array-sql)))
               (:array (if (second node)
                           (format nil "ARRAY[~{~a~^, ~}]" (mapcar #'render (second node)))
                           "CAST('{}' AS text[])"))
               (:record-field
                (let* ((record (second node)) (fields (getf (cdddr record) :fields))
                                              (field (nth (third node) fields)))
                  (unless field (ereport "0A000" "cannot serialize stored record field"))
                  (format nil "(~a).~a" (render record) (catalog-sql-quote-ident (first field)))))
               (:array-length (format nil "array_length(~a, ~a)" (render (second node)) (render (third node))))
               ((:func :single-record) (format nil "~a(~{~a~^, ~})" (catalog-sql-quote-ident (second node))
                                               (mapcar #'render (third node))))
               (:coalesce (format nil "COALESCE(~{~a~^, ~})" (mapcar #'render (second node))))
               (:case (format nil "(CASE~{ WHEN ~a THEN ~a~} ELSE ~a END)"
                              (loop for (condition result) in (second node)
                                    append (list (render condition) (render result)))
                              (render (third node))))
               (otherwise (ereport "0A000" "cannot serialize stored expression node ~a" (first node))))))
    (render expression)))

(defun catalog-column-definition-sql (column)
  (with-output-to-string (stream)
    (format stream "~a ~a"
            (catalog-sql-quote-ident (pg-column-name column))
            (case (pg-column-type column)
              (:date "date")
              (:interval (let* ((typmod (pg-column-typmod column))
                                (range (and (not (minusp typmod)) (ldb (byte 15 16) typmod)))
                                (precision (and range (ldb (byte 16 0) typmod))))
                           (if (and range (/= precision #xffff) (/= range 32767) (not (logbitp 12 range)))
                               (format nil "\"interval\"(~d,~d)" range precision)
                               (format nil "interval~a" (sql-interval-typmod-sql typmod)))))
              (:timetz (format nil "time~a with time zone"
                               (if (minusp (pg-column-typmod column)) "" (format nil "(~d)" (pg-column-typmod column)))))
              ((:timestamp :timestamptz)
               (format nil "timestamp~a~a"
                       (if (minusp (pg-column-typmod column)) "" (format nil "(~d)" (pg-column-typmod column)))
                       (if (eq (pg-column-type column) :timestamptz) " with time zone" " without time zone")))
              (:time (if (minusp (pg-column-typmod column)) "time"
                         (format nil "time(~d)" (pg-column-typmod column))))
              (:float4 "real") (:float8 "double precision")
              (:int2 "smallint") (:int4 "integer") (:int8 "bigint")
              (:numeric (let ((typmod (pg-column-typmod column)))
                          (if (< typmod 4) "numeric"
                              (let* ((packed (- typmod 4))
                                     (precision (ldb (byte 16 16) packed))
                                     (scale (- (logxor (logand packed #x7ff) #x400) #x400)))
                                (format nil "numeric(~d,~d)" precision scale)))))
              (:text "text") (:json "json") (:jsonb "jsonb") (:bytea "bytea")
              (:varchar (if (< (pg-column-typmod column) 4) "varchar"
                            (format nil "varchar(~a)" (- (pg-column-typmod column) 4))))
              (otherwise (error "cannot publish unsupported type ~a" (pg-column-type column)))))
    (when (pg-column-notnull column) (write-string " NOT NULL" stream))
    (cond ((pg-column-default-sql column)
           (let* ((expression (pg-column-default-expression column))
                  (sql (if (and expression (catalog-temporal-constant-p expression))
                           (catalog-canonical-expression-sql expression)
                           (pg-column-default-sql column))))
             (format stream " DEFAULT ~a" sql)))
          ((pg-column-default-sequence-name column)
           (format stream " DEFAULT nextval(~a)" (catalog-sql-literal (pg-column-default-sequence-name column)))))))

(defun catalog-table-constraint-sql (index table)
  (let ((columns (mapcar (lambda (attnum)
                           (catalog-sql-quote-ident
                            (pg-column-name
                             (aref (pg-class-columns table) (1- attnum)))))
                         (pg-index-columns index))))
    (format nil "CONSTRAINT ~a ~a~a (~{~a~^, ~})"
            (catalog-sql-quote-ident (pg-index-name index))
            (if (pg-index-primary-p index) "PRIMARY KEY" "UNIQUE")
            (if (and (pg-index-nulls-not-distinct-p index)
                     (not (pg-index-primary-p index)))
                " NULLS NOT DISTINCT" "")
            columns)))

(defun catalog-check-expression-sql (check table)
  (let ((expression (getf check :expression)))
    (if (and expression (catalog-temporal-constant-p expression))
        (catalog-canonical-expression-sql expression table)
        (getf check :sql))))

(defun catalog-table-definition-sql (table)
  (let* ((checks (sort (copy-list (pg-class-checks table)) #'string< :key (lambda (check) (getf check :name))))
         (constraints (mapcar (lambda (index) (catalog-table-constraint-sql index table))
                              (sort (remove-if-not #'pg-index-constraint-owned-p (table-indexes table))
                                    #'string< :key #'pg-index-name)))
         (name (catalog-sql-quote-ident (pg-class-name table)))
         (definitions (append (loop for column across (pg-class-columns table)
                                    collect (catalog-column-definition-sql column))
                              constraints
                              (loop for check in checks when (getf check :validated)
                                    collect (format nil "CONSTRAINT ~a CHECK (~a)"
                                                    (catalog-sql-quote-ident (getf check :name))
                                                    (catalog-check-expression-sql check table))))))
    (with-output-to-string (stream)
      (format stream "CREATE TABLE ~a (~{~a~^, ~})" name definitions)
      ;; CREATE TABLE cannot mark checks NOT VALID. Preserve their state with
      ;; separate ALTERs while the bootstrap target is still empty.
      (dolist (check checks)
        (unless (getf check :validated)
          (format stream "; ALTER TABLE ~a ADD CONSTRAINT ~a CHECK (~a) NOT VALID"
                  name (catalog-sql-quote-ident (getf check :name)) (catalog-check-expression-sql check table))))
      (loop for column across (pg-class-columns table)
            for storage = (pg-column-effective-storage column)
            unless (char= storage (sql-type-storage-code (pg-column-type column)))
              do (format stream "; ALTER TABLE ~a ALTER COLUMN ~a SET STORAGE ~a"
                         name (catalog-sql-quote-ident (pg-column-name column))
                         (case storage (#\p "PLAIN") (#\e "EXTERNAL")
                               (#\x "EXTENDED") (#\m "MAIN")))))))

(defun catalog-standalone-index-definition-sql (index table)
  (format nil "CREATE ~aINDEX IF NOT EXISTS ~a ON ~a (~{~a~^, ~})~a"
          (if (pg-index-unique-p index) "UNIQUE " "")
          (catalog-sql-quote-ident (pg-index-name index))
          (catalog-sql-quote-ident (pg-class-name table))
          (mapcar (lambda (attnum)
                    (catalog-sql-quote-ident
                     (pg-column-name
                      (aref (pg-class-columns table) (1- attnum)))))
                  (pg-index-columns index))
          (if (and (pg-index-unique-p index)
                   (pg-index-nulls-not-distinct-p index))
              " NULLS NOT DISTINCT" "")))

(defun catalog-sequence-definition-sql (sequence)
  (format nil "CREATE SEQUENCE IF NOT EXISTS ~a INCREMENT BY ~d MINVALUE ~d MAXVALUE ~d START WITH ~d"
          (catalog-sql-quote-ident (pg-sequence-name sequence))
          (pg-sequence-increment sequence)
          (pg-sequence-min-value sequence)
          (pg-sequence-max-value sequence)
          (pg-sequence-start-value sequence)))

(defun catalog-sequence-state-data (sequence)
  (list :name (pg-sequence-name sequence)
        :oid (pg-sequence-oid sequence)
        :start (pg-sequence-start-value sequence)
        :increment (pg-sequence-increment sequence)
        :min (pg-sequence-min-value sequence)
        :max (pg-sequence-max-value sequence)
        :last (pg-sequence-last-value sequence)
        :called (pg-sequence-called-p sequence)))

(defun catalog-sequence-bootstrap-entry (sequence)
  (list :definition (catalog-sequence-definition-sql sequence)
        :state (catalog-sequence-state-data sequence)))

(defun catalog-sequence-bootstrap-snapshot (db)
  "Copy sequence definitions and state while the caller holds *WRITE-LOCK*."
  (sort (loop for sequence being the hash-values of (database-sequences db)
              collect (catalog-sequence-bootstrap-entry sequence))
        #'string< :key (lambda (entry) (getf (getf entry :state) :name))))

(defun catalog-replication-bootstrap-data (db &optional snapshot)
  "Return supported schema and sequence state for the imported base snapshot.

Only table columns backed by the implemented int4/int8/varchar types, btree
indexes, and integer sequences are serialized, matching catalog SQL helpers and
the feature boundary in pg_dump.c's schema/data phases. Standalone index entries
carry their owning table, as pg_index.indrelid does in PostgreSQL's catalog."
  (let ((tables (sort (loop for table being the hash-values of
                                                            (database-tables db)
                            collect table)
                      #'string< :key #'pg-class-name))
        (sequences
          (if (and snapshot
                   (snapshot-sequence-state-captured-p snapshot)
                   (= (snapshot-sequence-database-oid snapshot)
                      (database-oid db)))
              (snapshot-sequence-states snapshot)
              (catalog-sequence-bootstrap-snapshot db))))
    (list :database-oid (database-oid db)
          :tables
          (mapcar (lambda (table)
                    (list :name (pg-class-name table)
                          :sql (concatenate
                                'string "CREATE TABLE IF NOT EXISTS "
                                (subseq (catalog-table-definition-sql table)
                                        (length "CREATE TABLE ")))))
                  tables)
          :indexes
          (loop for table in tables
                append (loop for index in (sort (table-indexes table)
                                                #'string<
                                                :key #'pg-index-name)
                             unless (pg-index-constraint-owned-p index)
                               collect
                             ;; pg_index.indrelid records index ownership;
                             ;; preserve that relation before schema filtering.
                               (list :table-name (pg-class-name table)
                                     :name (pg-index-name index)
                                     :sql
                                     (catalog-standalone-index-definition-sql
                                      index table))))
          :sequences sequences)))

(defun publication-definition-data (publication db)
  (list :database-oid (database-oid db)
        :name (pg-publication-name publication)
        :all-tables (pg-publication-all-tables-p publication)
        :generation (pg-publication-generation publication)
        :publish-actions (copy-list (pg-publication-publish-actions publication))
        :tables (if (pg-publication-all-tables-p publication)
                    (mapcar (lambda (name)
                              (list :name name
                                    :table-oid
                                    (pg-class-oid (gethash name
                                                           (database-tables db)))
                                    :filter nil))
                            (publication-selected-table-names publication db))
                    (copy-tree (pg-publication-tables publication)))
        :sequences
        (loop for name in (publication-sequence-names publication db)
              for sequence = (gethash name (database-sequences db))
              when sequence
                collect (list :name name
                              :oid (pg-sequence-oid sequence)
                              :start (pg-sequence-start-value sequence)
                              :increment (pg-sequence-increment sequence)
                              :last (pg-sequence-last-value sequence)
                              :called (pg-sequence-called-p sequence)))))

(defun catalog-readable-data-string (data)
  (with-output-to-string (stream)
    (with-standard-io-syntax
      (let ((*print-readably* t) (*print-pretty* nil))
        (prin1 data stream)))))

(defun find-column (class name)
  (loop for col across (pg-class-columns class)
        when (string= (pg-column-name col) name)
          return col))

(defun find-index-by-oid (oid)
  (loop for idx being the hash-values of (database-indexes (current-database))
        when (= (pg-index-oid idx) oid)
          return idx))

(defun find-table-by-oid (oid)
  (loop for table being the hash-values of (database-tables (current-database))
        when (= (pg-class-oid table) oid)
          return table))

(defun table-indexes (class)
  (loop for oid in (pg-class-indexes class)
        for idx = (find-index-by-oid oid)
        when idx collect idx))

(defun unique-indexes (class)
  (remove-if-not #'pg-index-unique-p (table-indexes class)))

(defun index-types (idx class)
  (loop for attnum in (pg-index-columns idx)
        collect (pg-column-type (aref (pg-class-columns class) (1- attnum)))))

(defun index-values (idx values)
  (loop for attnum in (pg-index-columns idx)
        collect (aref values (1- attnum))))

;;; ---- DDL helpers (caller holds the write lock) ----

(defun check-new-relation-name (db name)
  (when (or (gethash name (database-tables db))
            (gethash name (database-indexes db))
            (gethash name (database-sequences db)))
    (ereport "42P07" "relation \"~a\" already exists" name)))

(defun generate-index-name (db table-name suffix)
  (let ((base (format nil "~a_~a" table-name suffix)))
    (if (or (gethash base (database-tables db))
            (gethash base (database-indexes db)))
        (loop for n from 1
              for candidate = (format nil "~a~a" base n)
              unless (or (gethash candidate (database-tables db))
                         (gethash candidate (database-indexes db)))
                return candidate)
        base)))

(defun generate-unique-index-name (db table-name table attnums)
  "Generate PostgreSQL's default unique-constraint name for ATTNUMS."
  (generate-index-name
   db table-name
   (format nil "~{~a~^_~}_key"
           (mapcar (lambda (attnum)
                     (pg-column-name
                      (aref (pg-class-columns table) (1- attnum))))
                   attnums))))

(defun make-index-object (oid name table cols
                          &key unique primary constraint nulls-not-distinct)
  "Create detached INDEX metadata with a previously reserved object ID."
  (when (some (lambda (attnum)
                (eq (pg-column-type (aref (pg-class-columns table) (1- attnum))) :json))
              cols)
    (ereport "42704" "data type json has no default operator class for access method btree"))
  (make-pg-index :oid oid
                 :name name
                 :relfilenode oid
                 :table-oid (pg-class-oid table)
                 :columns cols
                 :unique-p unique
                 :primary-p primary
                 :constraint-p constraint
                 :nulls-not-distinct-p nulls-not-distinct))

(defun publish-index* (db index table)
  "Install a fully built INDEX in the database and its owning table."
  (check-new-relation-name db (pg-index-name index))
  (unless (= (pg-index-table-oid index) (pg-class-oid table))
    (ereport "XX000" "index \"~a\" belongs to another table"
             (pg-index-name index)))
  (setf (gethash (pg-index-name index) (database-indexes db)) index)
  (pushnew (pg-index-oid index) (pg-class-indexes table) :test #'=)
  index)

(defun create-index* (db name table cols
                      &key unique primary constraint nulls-not-distinct)
  (when (and constraint (find name (pg-class-checks table)
                              :key (lambda (check) (getf check :name)) :test #'string=))
    (ereport "42710" "constraint ~s for relation ~s already exists" name (pg-class-name table)))
  (check-new-relation-name db name)
  (let* ((oid (allocate-oid))
         (idx (make-index-object oid name table cols
                                 :unique unique
                                 :primary primary
                                 :constraint constraint
                                 :nulls-not-distinct nulls-not-distinct)))
    (btree-init oid)
    (publish-index* db idx table)))

(defun rename-index* (db old-name new-name)
  "Rename an index relation, retaining its OID and physical index file."
  (let ((idx (gethash old-name (database-indexes db))))
    (unless idx (ereport "42704" "index ~s does not exist" old-name))
    (unless (string= old-name new-name)
      (let ((table (loop for table being the hash-values of (database-tables db)
                         when (= (pg-class-oid table) (pg-index-table-oid idx)) return table)))
        (when (and (pg-index-constraint-owned-p idx) table
                   (find new-name (pg-class-checks table)
                         :key (lambda (check) (getf check :name)) :test #'string=))
          (ereport "42710" "constraint ~s already exists" new-name)))
      (check-new-relation-name db new-name)
      (remhash old-name (database-indexes db))
      (setf (pg-index-name idx) new-name
            (gethash new-name (database-indexes db)) idx))
    idx))

(defun toast-table-needed-p (columns)
  "Estimate whether COLUMNS need TOAST storage, as heapam_relation_needs_toast_table does.
A toastable column is required; an unknown maximum size or an aligned maximum
heap tuple exceeding the four-tuples-per-page threshold then warrants TOAST."
  (let ((data-length 0)
        (has-toastable nil)
        (unknown nil))
    (loop for column across columns
          for type = (pg-column-type column)
          for typmod = (pg-column-typmod column)
          do (setf data-length (align-up data-length
                                         (if (member type '(:varchar :numeric :text :bytea :json :jsonb))
                                             4 (column-align column))))
             (let ((size
                     (case type
                       (:varchar
                        (setf has-toastable t)
                        (and (>= typmod 4) (+ 4 (* 4 (- typmod 4)))))
                       (:numeric
                        (setf has-toastable t)
                        (and (>= typmod 4)
                             (+ 8 (* 2 (floor (+ (ldb (byte 16 16) (- typmod 4)) 6) 4)))))
                       ((:text :bytea :json :jsonb)
                        (setf has-toastable t)
                        nil)
                       (otherwise (column-fixed-size column)))))
               (if size (incf data-length size) (setf unknown t))))
    (let* ((per-page 4)
           (space (floor (- +blcksz+ (maxalign (+ +size-of-page-header+
                                                  (* per-page +size-of-item-id+))))
                         per-page))
           (threshold (- space (mod space +maxalign+)))
           (tuple-length (+ (maxalign (+ +size-of-heap-tuple-header+
                                         (ceiling (length columns) 8)))
                            (maxalign data-length))))
      (and has-toastable (or unknown (> tuple-length threshold))))))

(defun create-table-toast-storage (table)
  "Create eligible TABLE's owned TOAST heap and index before publishing them.
Until both are initialized and the index is fsynced, this function owns their
cleanup. Once attached, TABLE owns both nodes and catalog save makes that
ownership durable; table rollback or drop handles their eventual removal."
  (when (toast-table-needed-p (pg-class-columns table))
    (let* ((heap-oid (allocate-oid))
           (index-oid (allocate-oid))
           (toast (make-pg-toast :oid heap-oid :relfilenode heap-oid
                                 :index-oid index-oid :index-relfilenode index-oid
                                 :relfrozenxid (next-full-xid)))
           (completed nil))
      (unwind-protect
           (progn
             (smgr-create heap-oid)
             (btree-init index-oid)
             (wal-commit-maintenance)
             (smgr-fsync index-oid)
             (setf (pg-class-toast table) toast
                   completed t))
        (unless completed
          (ignore-errors (wal-discard-relation-pages (list heap-oid index-oid)))
          (ignore-errors (smgr-forget heap-oid :delete t))
          (ignore-errors (smgr-forget index-oid :delete t)))))))

(defconstant +toast-oid-chunk-size+
  (- (let* ((space (floor (- +blcksz+ (maxalign (+ +size-of-page-header+
                                                   (* 4 +size-of-item-id+))))
                          4)))
       (- space (mod space +maxalign+)))
     (maxalign +size-of-heap-tuple-header+) 4 4 4))

(defparameter *toast-chunk-columns*
  (vector (make-pg-column :name "chunk_id" :type :oid :typmod -1 :attnum 1)
          (make-pg-column :name "chunk_seq" :type :int4 :typmod -1 :attnum 2)
          (make-pg-column :name "chunk_data" :type :bytea :typmod -1 :attnum 3 :storage #\p)))

(defun toast-chunk-key (value-id sequence)
  "Encode the unsigned 32-bit OID and signed int4 sequence as an ordered key.
The OID encoding alone is a byte prefix of every key for that value."
  (encode-index-key (list value-id sequence) '(:oid :int4)))

(defvar *toast-next-value-id* 1
  "Next candidate TOAST value ID; the index is authoritative after restart.")

(defconstant +toast-tuple-target+
  (let* ((per-page 4)
         (space (floor (- +blcksz+ (maxalign (+ +size-of-page-header+
                                                (* per-page +size-of-item-id+))))
                       per-page)))
    (- space (mod space +maxalign+)))
  "Default target heap tuple size, based on four tuples per page.")

(defconstant +toast-tuple-target-main+
  (let ((space (- +blcksz+ (maxalign (+ +size-of-page-header+ +size-of-item-id+)))))
    (- space (mod space +maxalign+)))
  "Largest heap tuple target used before externalizing MAIN attributes.")

(defun toast-allocate-value-id-unlocked (toast)
  "Choose an unused nonzero OID in TOAST while its index and *WRITE-LOCK* are held.
Index entries, including dead or aborted entries, reserve their ID until vacuum
removes them. The in-memory candidate is only a hint and need not survive restart."
  (let* ((index-node (pg-toast-index-relfilenode toast))
         (start *toast-next-value-id*)
         (candidate start))
    (unless (typep start '(integer 1 4294967295))
      (error "Invalid TOAST value ID candidate"))
    (when (< (buf-nblocks index-node) 2)
      (ereport "XX001" "TOAST index is missing"))
    (loop
      (setf *toast-next-value-id* (if (= candidate #xffffffff) 1 (1+ candidate)))
      (unless (btree-prefix-entries-unlocked index-node
                                             (encode-index-key (list candidate) '(:oid)))
        (return candidate))
      (setf candidate *toast-next-value-id*)
      (when (= candidate start)
        (ereport "54000" "TOAST value ID space is exhausted")))))

(defun toast-pointer-storage-size (toast pointer)
  "Validate POINTER ownership and size fields and return stored and raw sizes."
  (let* ((info (heap-external-varlena-external-info pointer))
         (size (logand info #x3fffffff))
         (method (ash info -30))
         (raw (heap-external-varlena-raw-size pointer)))
    (unless (and (= (heap-external-varlena-id-width pointer) 4)
                 (= (heap-external-varlena-relation-id pointer) (pg-toast-oid toast))
                 (typep raw '(integer 5 1073741823))
                 (<= 1 size (- raw 4))
                 ;; Only PGLZ (method zero) is currently supported.
                 (zerop method)
                 (or (= size (- raw 4)) (> size 4)))
      (ereport "XX001" "invalid TOAST pointer"))
    (values size (- raw 4) (< size (- raw 4)))))

(defun toast-compressed-payload (bytes)
  "Return a tcinfo-prefixed PGLZ stream for BYTES, or NIL if unsuitable."
  (let ((stream (pglz-compress bytes)))
    (when (and stream (< (+ 4 (length stream)) (length bytes)))
      (let ((payload (make-array (+ 4 (length stream))
                                 :element-type '(unsigned-byte 8))))
        ;; PGLZ's compression method ID is zero in the high two bits.
        (page-set-u32 payload 0 (length bytes))
        (replace payload stream :start1 4)
        payload))))

(defun toast-decompress-payload (payload raw-size)
  "Validate tcinfo and return the RAW-SIZE octets in compressed PAYLOAD."
  (unless (and (> (length payload) 4)
               (= (page-u32 payload 0) raw-size))
    (ereport "XX001" "invalid compressed TOAST header"))
  (pglz-decompress (subseq payload 4) raw-size))

(defun toast-store-value-unlocked (toast bytes value-id xid cid &key compress)
  "Store BYTES as TOAST chunks, optionally using PGLZ, and return a pointer.
The caller holds TOAST's index writer lock and then *WRITE-LOCK*. VALUE-ID is an
unused nonzero 32-bit ID, or NIL to allocate one. XID and CID identify the
inserting transaction and command. BYTES contains 1 through 1073741819 octets;
chunks are copied. Abort the transaction after any partial write."
  (check-type bytes (vector (unsigned-byte 8)))
  (unless (and (<= 1 (length bytes) 1073741819)
               (or (null value-id) (typep value-id '(integer 1 4294967295))))
    (error "Invalid TOAST value or ID"))
  (unless value-id (setf value-id (toast-allocate-value-id-unlocked toast)))
  (let* ((raw-size (length bytes))
         (payload (or (and compress (toast-compressed-payload bytes)) bytes))
         (index-node (pg-toast-index-relfilenode toast))
         (heap-node (pg-toast-relfilenode toast)))
    (when (< (buf-nblocks index-node) 2)
      (ereport "XX001" "TOAST index is missing"))
    (when (btree-prefix-entries-unlocked index-node
                                         (encode-index-key (list value-id) '(:oid)))
      (error "TOAST value ID is already in use"))
    (loop for start from 0 below (length payload) by +toast-oid-chunk-size+
          for sequence from 0
          for end = (min (length payload) (+ start +toast-oid-chunk-size+))
          do (multiple-value-bind (block offset)
                 (heapam-insert heap-node *toast-chunk-columns*
                                (vector value-id sequence (subseq payload start end)) xid cid)
               (btree-insert-unlocked index-node (toast-chunk-key value-id sequence)
                                      block offset)))
    (make-heap-external-varlena :raw-size (+ 4 raw-size)
                                :external-info (length payload) :value-id value-id
                                :relation-id (pg-toast-oid toast) :id-width 4)))

(defun toast-plan-tuple (values columns &key (external-p t))
  "Plan compression and externalization phases according to column storage.
Return physical values, selected indexes, and the subset selected compressed.
Selected values contain placeholder pointers that the caller must replace.
VALUES remains unchanged; EXTERNAL-P says whether the relation has a TOAST table."
  (let* ((physical (copy-seq values))
         (attempted (make-array (length columns) :initial-element nil))
         (selected nil)
         (compressed-selected nil)
         (cached nil)
         (header (maxalign (+ +size-of-heap-tuple-header+ (ceiling (length columns) 8))))
         (max-data (- +toast-tuple-target+ header)))
    (labels ((layout ()
               (multiple-value-bind (size offsets encoded mask)
                   (heap-tuple-layout physical columns cached)
                 (declare (ignore offsets mask))
                 (setf cached encoded)
                 size))
             (size-at (i)
               (let* ((value (aref physical i))
                      (size (length (aref cached i))))
                 (+ size (if (and (not (heap-compressed-varlena-p value))
                                  (<= size 126)) 1 4))))
             (candidate (main-p compress-p)
               (let ((best nil) (best-size (maxalign (+ 4 +external-varlena-fixed-size+))))
                 (loop for i below (length columns)
                       for value = (aref physical i)
                       for column = (aref columns i)
                       for type = (pg-column-type column)
                       for storage = (pg-column-effective-storage column)
                       when (and (member type '(:varchar :text :json :jsonb :bytea :numeric))
                                 (if main-p (char= storage #\m) (find storage "xe"))
                                 (not (eq value +null+))
                                 (not (heap-external-varlena-p value))
                                 (or (not compress-p)
                                     (and (not (aref attempted i))
                                          (not (heap-compressed-varlena-p value)))))
                         do (let ((size (size-at i)))
                              (when (> size best-size)
                                (setf best i best-size size))))
                 best))
             (compress-value (index)
               (setf (aref attempted index) t)
               (let* ((raw (aref cached index))
                      (compressed (unless (char= (pg-column-effective-storage (aref columns index)) #\e)
                                    (toast-compressed-payload raw))))
                 (when (and compressed (< (+ 4 (length compressed)) (- (length raw) 2)))
                   (setf (aref physical index) (make-heap-compressed-varlena :payload compressed)
                         (aref cached index) nil))))
             (externalize (index)
               (push index selected)
               (when (heap-compressed-varlena-p (aref physical index))
                 (push index compressed-selected))
               (setf (aref physical index)
                     (make-heap-external-varlena :raw-size 5 :external-info 1
                                                 :value-id 1 :relation-id 1 :id-width 4))))
      ;; Phase 1: EXTENDED compression, with immediate externalization of an
      ;; individual value that still exceeds the normal data target.
      (loop while (> (layout) +toast-tuple-target+)
            for best = (candidate nil t)
            while best
            do (compress-value best)
               (layout)
               (when (and external-p (> (size-at best) max-data))
                 (externalize best)))
      ;; Phase 2: move remaining EXTENDED values to TOAST.
      (when external-p
        (loop while (> (layout) +toast-tuple-target+)
              for best = (candidate nil nil)
              while best do (externalize best)))
      ;; Phase 3: try compression of MAIN values.
      (loop while (> (layout) +toast-tuple-target+)
            for best = (candidate t t)
            while best do (compress-value best))
      ;; Phase 4: move MAIN values only above the page-derived MAIN target.
      (when external-p
        (loop while (> (layout) +toast-tuple-target-main+)
              for best = (candidate t nil)
              while best do (externalize best)))
      (values physical (nreverse selected) compressed-selected))))

(defun toast-externalize-tuple-unlocked (toast values columns xid cid &optional reuse)
  "Return a physical tuple while keeping VALUES usable for SQL and replication.
REUSE holds external pointers from the claimed old tuple for unchanged columns.
The caller holds the TOAST index and write locks and must abort after errors."
  (let ((planning-values (copy-seq values)))
    (when reuse
      (loop for pointer across reuse for i from 0
            when pointer do (setf (aref planning-values i) pointer)))
    (multiple-value-bind (physical indexes compressed-indexes)
        (toast-plan-tuple planning-values columns :external-p (not (null toast)))
      (when indexes
        (let ((raw-payloads (nth-value 2 (heap-tuple-layout values columns))))
          (dolist (index indexes)
            (setf (aref physical index)
                  (toast-store-value-unlocked toast (aref raw-payloads index)
                                              nil xid cid :compress (member index compressed-indexes))))))
      physical)))

(defun toast-fetch-value (toast pointer snapshot &key (index-reader #'call-with-btree-relation-read-lock))
  "Return the uncompressed bytes named by POINTER in the owning TOAST storage.
SNAPSHOT must remain active for the entire call, protecting the parent value's
lifetime; chunk visibility is parent-driven, not evaluated against SNAPSHOT.
INDEX-READER is called with the index relfilenode and a zero-argument function;
it must call that function under a read lease on the index and return its result.
The lease covers reading and assembling chunks. Only matching 32-bit relation
pointers with raw sizes from 1 to 1073741819 are accepted; missing,
misordered, or malformed chunks signal corruption."
  (unless (and snapshot
               (sb-thread:with-mutex (*snapshot-count-lock*)
                 (gethash snapshot *active-snapshot-set*)))
    (error "TOAST fetch requires an active snapshot"))
  (let* ((size (toast-pointer-storage-size toast pointer))
         (id (heap-external-varlena-value-id pointer))
         (index-node (pg-toast-index-relfilenode toast))
         (heap-node (pg-toast-relfilenode toast)))
    (when (< (buf-nblocks index-node) 2)
      (ereport "XX001" "TOAST index is missing"))
    (funcall index-reader
             index-node
             (lambda ()
               (let* ((payload (make-array size :element-type '(unsigned-byte 8)))
                      (expected 0)
                      (total (ceiling size +toast-oid-chunk-size+))
                      (raw-size (- (heap-external-varlena-raw-size pointer) 4)))
                 (btree-map-prefix-entries-unlocked
                  index-node (encode-index-key (list id) '(:oid))
                  (lambda (entry)
                    (let ((tuple (heapam-get-tuple heap-node (second entry) (third entry)
                                                   *toast-chunk-columns* t)))
                      (when tuple
                        (let* ((values (htup-values tuple))
                               (chunk-id (aref values 0))
                               (sequence (aref values 1))
                               (data (aref values 2))
                               (expected-size (if (= expected (1- total))
                                                  (- size (* expected +toast-oid-chunk-size+))
                                                  +toast-oid-chunk-size+)))
                          (unless (and (= chunk-id id)
                                       (equalp (first entry) (toast-chunk-key chunk-id sequence)))
                            (ereport "XX001" "TOAST index points to the wrong chunk"))
                          (unless (and (eql sequence expected) (< expected total)
                                       (vectorp data)
                                       (every (lambda (octet) (typep octet '(unsigned-byte 8))) data)
                                       (= (length data) expected-size))
                            (ereport "XX001" "invalid TOAST chunk ~a" sequence))
                          (replace payload data :start1 (* expected +toast-oid-chunk-size+))
                          (incf expected))))))
                 (unless (= expected total)
                   (ereport "XX001" "missing TOAST chunk ~a" expected))
                 (if (< size raw-size)
                     (toast-decompress-payload payload raw-size)
                     payload))))))

(defun toast-fetch-value-slice (toast pointer snapshot start length
                                &key (index-reader #'call-with-btree-relation-read-lock))
  "Fetch up to LENGTH bytes from START, clamped to the logical value.
SNAPSHOT must protect the owning parent throughout the call. Uncompressed
values read only intersecting chunk keys under one index lease; compressed
values use the full validated fetch. Corruption in unrequested chunks is not
checked by a partial fetch."
  (unless (and (integerp start) (>= start 0) (integerp length) (>= length 0))
    (error "TOAST slice offset and length must be nonnegative integers"))
  (unless (and snapshot
               (sb-thread:with-mutex (*snapshot-count-lock*)
                 (gethash snapshot *active-snapshot-set*)))
    (error "TOAST fetch requires an active snapshot"))
  (multiple-value-bind (size raw-size compressed-p) (toast-pointer-storage-size toast pointer)
    (let* ((id (heap-external-varlena-value-id pointer))
           (index-node (pg-toast-index-relfilenode toast))
           (heap-node (pg-toast-relfilenode toast))
           (begin (min start raw-size))
           (end (min raw-size (+ begin length)))
           (result nil))
      (unless (typep id '(integer 1 4294967295))
        (ereport "XX001" "invalid TOAST pointer"))
      (when (< (buf-nblocks index-node) 2)
        (ereport "XX001" "TOAST index is missing"))
      (when (= begin end)
        (return-from toast-fetch-value-slice (make-array 0 :element-type '(unsigned-byte 8))))
      (when compressed-p
        (return-from toast-fetch-value-slice
          (subseq (toast-fetch-value toast pointer snapshot :index-reader index-reader) begin end)))
      (setf result (make-array (- end begin) :element-type '(unsigned-byte 8)))
      (funcall index-reader index-node
               (lambda ()
                 (loop for sequence from (floor begin +toast-oid-chunk-size+)
                         to (floor (1- end) +toast-oid-chunk-size+)
                       for key = (toast-chunk-key id sequence)
                       for tids = (btree-search-unlocked index-node key)
                       do (let* ((tuples (loop for tid in tids
                                               for tuple = (heapam-get-tuple heap-node (car tid) (cdr tid)
                                                                             *toast-chunk-columns* t)
                                               when tuple collect tuple))
                                 (tuple (first tuples)))
                            (unless (= (length tuples) 1)
                              (ereport "XX001" "missing or duplicate TOAST chunk"))
                            (let* ((values (htup-values tuple))
                                   (bytes (aref values 2))
                                   (chunk-start (* sequence +toast-oid-chunk-size+))
                                   (expected (min +toast-oid-chunk-size+ (- size chunk-start))))
                              (unless (and (eql (aref values 0) id)
                                           (eql (aref values 1) sequence)
                                           (typep bytes '(vector (unsigned-byte 8)))
                                           (= (length bytes) expected))
                                (ereport "XX001" "invalid TOAST chunk"))
                              (let ((from (max begin chunk-start))
                                    (to (min end (+ chunk-start expected))))
                                (replace result bytes :start1 (- from begin)
                                                      :start2 (- from chunk-start)
                                                      :end2 (- to chunk-start))))))))
      result)))

(defun toast-delete-value-unlocked (toast pointer xid cid)
  "Mark the chunks referenced by POINTER deleted by XID and CID.
The caller must own the parent tuple and hold the TOAST index writer lock,
then the global write lock, throughout this call.  Readers must keep the
parent tuple's snapshot active while fetching its external value; that snapshot
protects the value's lifetime even after deletion.

Validate the complete chunk sequence before marking any tuple.  An existing
aborted XMAX is replaced; this XID's own XMAX is left unchanged, making a
repeat deletion idempotent.  Any other nonzero XMAX signals a conflict.  If an
error occurs after marking begins, the caller must abort the transaction so a
partial deletion cannot commit."
  (let* ((size (toast-pointer-storage-size toast pointer))
         (id (heap-external-varlena-value-id pointer))
         (index-node (pg-toast-index-relfilenode toast))
         (heap-node (pg-toast-relfilenode toast))
         (chunks nil)
         (tids nil))
    (when (< (buf-nblocks index-node) 2)
      (ereport "XX001" "TOAST index is missing"))
    (dolist (entry (btree-prefix-entries-unlocked
                    index-node (encode-index-key (list id) '(:oid))))
      (let ((tuple (heapam-get-tuple heap-node (second entry) (third entry)
                                     *toast-chunk-columns* t)))
        (when tuple
          (let* ((values (htup-values tuple))
                 (chunk-id (aref values 0))
                 (sequence (aref values 1))
                 (old-xmax (htup-xmax tuple)))
            (unless (and (= chunk-id id)
                         (equalp (first entry) (toast-chunk-key chunk-id sequence)))
              (ereport "XX001" "TOAST index points to the wrong chunk"))
            (unless (or (zerop old-xmax) (= old-xmax xid) (xid-aborted-p old-xmax))
              (error "TOAST chunk is already being deleted"))
            (push (cons sequence (aref values 2)) chunks)
            (unless (= old-xmax xid) (push (cons (second entry) (third entry)) tids))))))
    (toast-assemble-chunks (nreverse chunks) size +toast-oid-chunk-size+ :validate-only t)
    (dolist (tid tids)
      (unless (heapam-set-xmax heap-node (car tid) (cdr tid) xid cid)
        (ereport "XX001" "TOAST chunk disappeared during deletion")))
    (values)))

(defun create-table* (db name columns)
  "COLUMNS is a list of plists with :name :type :typmod :notnull :primary,
:unique, and :nulls-not-distinct. Returns the new pg-class."
  (check-new-relation-name db name)
  (dolist (def columns)
    (let ((sequence-name (getf def :default-sequence-name)))
      (when sequence-name
        (unless (gethash sequence-name (database-sequences db))
          (ereport "42P01" "sequence \"~a\" does not exist" sequence-name))
        (unless (member (getf def :type) '(:int4 :int8))
          (ereport "42804" "sequence default requires an integer column")))))
  (let* ((oid (allocate-oid))
         (cols (coerce
                (loop for def in columns
                      for i from 1
                      collect (make-pg-column
                               :name (getf def :name)
                               :type (getf def :type)
                               :typmod (getf def :typmod)
                               :attnum i
                               :notnull (getf def :notnull)
                               :default-sequence-name
                               (getf def :default-sequence-name)))
                'vector))
         (class (make-pg-class :oid oid
                               :name name
                               :relfilenode oid
                               :relfrozenxid (next-full-xid)
                               :columns cols
                               :indexes '())))
    (setf (gethash name (database-tables db)) class)
    (handler-case
        (progn
          (smgr-create (pg-class-relfilenode class))
          (create-table-toast-storage class)
          ;; Per-column PRIMARY KEY / UNIQUE.
          (loop for def in columns
                for col across cols
                do (cond
                     ((getf def :primary)
                      (setf (pg-column-notnull col) t)
                      (setf (pg-index-valid-p
                             (create-index*
                              db
                              (or (getf def :primary-name) (generate-index-name db name "pkey"))
                              class (list (pg-column-attnum col))
                              :unique t :primary t :constraint t))
                            t))
                     ((getf def :unique)
                      (setf (pg-index-valid-p
                             (create-index*
                              db
                              (or (getf def :unique-name)
                                  (generate-index-name db name
                                                       (format nil "~a_key" (pg-column-name col))))
                              class (list (pg-column-attnum col))
                              :unique t
                              :constraint t
                              :nulls-not-distinct
                              (getf def :nulls-not-distinct)))
                            t))))
          class)
      (error (e)
        ;; A column constraint can fail after the table and earlier indexes
        ;; have been installed.  Roll all of them back as one CREATE TABLE.
        (when (eq class (gethash name (database-tables db)))
          (drop-table* db name))
        (error e)))))

(defun drop-table* (db name)
  "Remove NAME and its owned metadata, deferring physical relation deletion.
The owned TOAST heap and its index are deleted with the table, just as the
internal dependency registered by PostgreSQL's create_toast_table makes the
TOAST relation follow its parent.  Deferred deletion lets the surrounding WAL
and catalog durability machinery handle commit or rollback."
  (let ((class (gethash name (database-tables db))))
    (unless class
      (ereport "42P01" "table \"~a\" does not exist" name))
    (dolist (oid (pg-class-indexes class))
      (let ((idx (find-index-by-oid oid)))
        (when idx
          (remhash (pg-index-name idx) (database-indexes db))
          (wal-defer-relation-delete (pg-index-relfilenode idx)))))
    (maphash
     (lambda (publication-name publication)
       (declare (ignore publication-name))
       (unless (pg-publication-all-tables-p publication)
         (let ((remaining
                 (remove name (pg-publication-tables publication)
                         :key (lambda (entry) (getf entry :name))
                         :test #'string=)))
           (unless (= (length remaining)
                      (length (pg-publication-tables publication)))
             (setf (pg-publication-tables publication) remaining)
             (incf (pg-publication-generation publication))))))
     (database-publications db))
    (let ((toast (pg-class-toast class)))
      (when toast
        (wal-defer-relation-delete (pg-toast-relfilenode toast))
        (wal-defer-relation-delete (pg-toast-index-relfilenode toast))))
    (remhash name (database-tables db))
    (wal-defer-relation-delete (pg-class-relfilenode class))
    (values)))

(defun drop-index* (db name)
  (let ((idx (gethash name (database-indexes db))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" name))
    (remhash name (database-indexes db))
    (let ((class (loop for c being the hash-values of (database-tables db)
                       when (= (pg-class-oid c) (pg-index-table-oid idx))
                         return c)))
      (when class
        (setf (pg-class-indexes class)
              (delete (pg-index-oid idx) (pg-class-indexes class)))))
    (wal-defer-relation-delete (pg-index-relfilenode idx))
    (values)))
