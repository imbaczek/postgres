;;;; Primary WAL catalog publication and deferred relation deletion.

(in-package #:pg)

(defun wal-append-catalog-chunks (payload count)
  (let ((chunk-size (- +wal-segment-size+ +wal-header-size+
                       +wal-catalog-chunk-header-size+)))
    (loop for index from 0 below count
          for start = (* index chunk-size)
          for stop = (min (length payload) (+ start chunk-size))
          for chunk = (subseq payload start stop)
          do (wal-append-record
              +wal-record-catalog-chunk+ 0
              (wal-encode-catalog-chunk-payload chunk index count))))
  (values))

(defun wal-defer-relation-delete (relfilenode)
  (when *wal-ready*
    (sb-thread:with-mutex (*wal-lock*)
      (pushnew relfilenode *wal-pending-deletes* :test #'eql)))
  (values))

(defun wal-catalog-flush-generation ()
  "Return the number of catalog WAL commits known to have reached durable WAL."
  (sb-thread:with-mutex (*wal-lock*)
    *wal-catalog-flush-generation*))

(defun wal-live-logical-slot-for-database-p (slot database-oid)
  (and (wal-logical-slot-p slot)
       (eql database-oid (wal-replication-slot-database-oid slot))
       (not (eq (wal-replication-slot-wal-status slot) :lost))))

(defun wal-logical-catalog-event-needed-under-lock-p (database-oid)
  "Whether a live slot or in-flight snapshot needs DATABASE-OID catalog events.

Call while holding *WAL-LOCK*. Provisional interests close the gap between
snapshot capture under *WRITE-LOCK* and durable slot publication."
  (or (wal-logical-capture-required-under-lock-p)
      (loop for interested-database being the hash-values of
            *wal-logical-catalog-event-interests*
              thereis (= database-oid interested-database))
      (loop for slot being the hash-values of *wal-replication-slots*
              thereis (wal-live-logical-slot-for-database-p slot database-oid))))

(defun wal-transactional-ddl-mode-under-lock (database-oid)
  "Classify durable and provisional interests before staging or publishing DDL."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (loop for oid being the hash-values of *wal-logical-catalog-event-interests*
                thereis (= oid database-oid))
    (return-from wal-transactional-ddl-mode-under-lock :incompatible))
  (let ((capable-p nil))
    (loop for slot being the hash-values of *wal-replication-slots*
          when (wal-live-logical-slot-for-database-p slot database-oid)
            do (unless (wal-transactional-ddl-slot-plugin-p (wal-replication-slot-plugin slot))
                 (return-from wal-transactional-ddl-mode-under-lock :incompatible))
               (setf capable-p t))
    (cond (capable-p :transactional)
          ((wal-logical-capture-required-under-lock-p) :incompatible))))

(defun wal-register-logical-catalog-event-interest-under-writer-lock
    (registration database-oid)
  "Protect sequence/DDL events after a snapshot boundary until slot publish."
  ;; WAL loads before the snapshot structure is defined, so keep validation
  ;; independent of its generated predicates. Callers pass the active
  ;; registration created by snapshot capture.
  (unless (and registration
               (integerp database-oid) (<= 1 database-oid #xFFFFFFFF))
    (error "logical catalog event interest requires a snapshot and database"))
  ;; The capture caller still holds *WRITE-LOCK*: taking *WAL-LOCK* here follows
  ;; the write -> WAL order and makes the interest visible before writers resume.
  (sb-thread:with-mutex (*wal-lock*)
    (setf (gethash registration *wal-logical-catalog-event-interests*)
          database-oid))
  registration)

(defun wal-release-logical-catalog-event-interest (registration)
  (when registration
    (sb-thread:with-mutex (*wal-lock*)
      (remhash registration *wal-logical-catalog-event-interests*)))
  (values))

(defun wal-prepare-logical-catalog-event-under-lock (event)
  "Validate EVENT and return it only if a slot or in-flight snapshot needs it.
Call with the WAL lock held, before beginning publication."
  (when event
    (when (< (length event) 4)
      (wal-invalid-input "invalid WAL logical catalog event envelope"))
    (when (wal-logical-catalog-event-needed-under-lock-p (wal-u32 event 0))
      (when (< (length event) 8)
        (wal-invalid-input "invalid WAL logical catalog event envelope"))
      (when (> (+ +wal-header-size+ (length event)) +wal-segment-size+)
        (wal-invalid-input "WAL logical catalog event exceeds the segment size"))
      event)))

(defun wal-commit-catalog (catalog-bytes write-function
                           &key relation-deletes logical-event
                             on-record-appended on-durable)
  "Durably publish a full catalog image and any deferred relation drops.
With WAL ready, return the exact end LSN of the terminal catalog record after
local apply. ON-RECORD-APPENDED runs after that record is appended; ON-DURABLE
receives its end LSN after the flush and before local page, catalog and drop
application. An incomplete publication fail-stops, retaining the first error.
Without WAL, write the catalog, call ON-DURABLE with NIL, and return no values."
  (unless *wal-ready*
    (sb-thread:with-mutex (*wal-lock*) (wal-require-no-pending-storage))
    (funcall write-function catalog-bytes)
    (sb-thread:with-mutex (*wal-lock*)
      (incf *wal-catalog-flush-generation*))
    (when on-durable (funcall on-durable nil))
    (return-from wal-commit-catalog (values)))
  (let ((commit-lsn nil))
    (with-wal-publication-fence (begin-publication)
      (wal-settle-page-publications-under-lock)
      (wal-require-no-pending-storage)
      (let* ((logical-event (wal-prepare-logical-catalog-event-under-lock logical-event))
             (deletes (sort (remove-duplicates
                             (append (copy-list *wal-pending-deletes*)
                                     (copy-list relation-deletes))
                             :test #'eql)
                            #'<))
             (keys (wal-primary-page-keys deletes))
             (catalog-payload (wal-encode-catalog-payload catalog-bytes deletes))
             (chunked-p (> (+ +wal-header-size+ (length catalog-payload)) +wal-segment-size+))
             (chunk-count (when chunked-p (wal-catalog-chunk-count catalog-payload)))
             (commit-payload (when chunked-p
                               (wal-encode-catalog-commit-payload catalog-payload chunk-count))))
        (wal-primary-preflight-page-keys keys)
        (with-wal-primary-page-spool (spool)
          (begin-publication)
          (wal-primary-record-page-xids keys (wal-append-batch-start))
          (wal-primary-append-pages keys spool)
          (wal-primary-seal-page-spool spool)
          (when chunked-p
            (wal-append-catalog-chunks catalog-payload chunk-count)
            (when *wal-output-stream* (finish-output *wal-output-stream*))
            (wal-test-pause :after-catalog-chunks))
          (when logical-event
            (wal-append-record +wal-record-logical-catalog-event+ 0 logical-event))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-before-vacuum-full-catalog-record-appended-hook*)
            (funcall *wal-test-before-vacuum-full-catalog-record-appended-hook*))
          (setf commit-lsn
                (if chunked-p
                    (wal-append-record +wal-record-catalog-commit+ 0 commit-payload)
                    (wal-append-record +wal-record-catalog+ 0 catalog-payload)))
          (when on-record-appended (funcall on-record-appended))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-pause-at*
                     (string= *wal-test-pause-at* "after-vacuum-full-catalog-wal-append"))
            (finish-output *wal-output-stream*)
            (fsync-stream *wal-output-stream*)
            (when *wal-new-segments*
              (fsync-directory (wal-directory))
              (setf *wal-new-segments* '()))
            (wal-test-pause :after-vacuum-full-catalog-wal-append))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-before-vacuum-full-catalog-flush-hook*)
            (funcall *wal-test-before-vacuum-full-catalog-flush-hook*))
          (wal-flush)
          (incf *wal-catalog-flush-generation*)
          (when on-durable (funcall on-durable commit-lsn))
          (wal-test-pause :after-catalog-wal-flush)
          (when *wal-vacuum-full-publication*
            (wal-test-pause :after-vacuum-full-catalog-wal-flush))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-after-vacuum-full-catalog-flush-hook*)
            (funcall *wal-test-after-vacuum-full-catalog-flush-hook*))
          (wal-primary-apply-page-spool spool)
          (funcall write-function catalog-bytes)
          (dolist (relfilenode deletes) (smgr-forget relfilenode :delete t))
          (setf *wal-pending-deletes*
                (set-difference *wal-pending-deletes* deletes :test #'eql))
          (when deletes (fsync-directory (merge-pathnames "base/" *pgdata*)))
          (wal-clear-page-cache)
          (setf *wal-pending-btree-split* nil))))
    commit-lsn))
