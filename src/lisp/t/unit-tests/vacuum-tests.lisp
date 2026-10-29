;;;; vacuum-tests.lisp -- VACUUM and B-tree tests.

(in-package #:pg)

(defun test-vacuum-current-table-index-set-lock-guard ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (db (make-database :name "postgres" :oid 992030))
         (table (make-pg-class :oid 992031 :name "vac_guard"
                               :relfilenode 992032 :indexes '(992033)))
         (index (make-pg-index :oid 992033 :name "vac_guard_idx"
                               :relfilenode 992034 :table-oid 992031))
         (replacement-index (make-pg-index :oid 992033 :name "vac_guard_idx"
                                           :relfilenode 992035
                                           :table-oid 992031))
         (replacement-table (copy-pg-class table))
         (table-evaluations 0)
         (index-set-evaluations 0)
         (table-check-locked-p nil)
         (index-check-locked-p nil)
         (body-ran nil)
         (original-ensure (symbol-function 'ensure-current-table))
         (original-same-set (symbol-function 'same-index-object-set-p)))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db
          (gethash (pg-class-name table) (database-tables db)) table
          (gethash (pg-index-name index) (database-indexes db)) index)
    (flet ((ensure-current-table (candidate)
             (setf table-check-locked-p
                   (and (sb-thread:holding-mutex-p *write-lock*)
                        (sb-thread:holding-mutex-p *catalog-lock*)))
             (funcall original-ensure candidate))
           (same-index-object-set-p (left right)
             (setf index-check-locked-p
                   (and (sb-thread:holding-mutex-p *write-lock*)
                        (sb-thread:holding-mutex-p *catalog-lock*)))
             (funcall original-same-set left right)))
      (check (eq (with-current-table-index-set
                     ((progn (incf table-evaluations) table)
                      (progn (incf index-set-evaluations) (list index)))
                   (setf body-ran t)
                   (check (and (sb-thread:holding-mutex-p *write-lock*)
                               (sb-thread:holding-mutex-p *catalog-lock*))
                          "VACUUM FULL catalog body runs under ordered catalog locks")
                   :guarded)
                 :guarded)
             "VACUUM FULL current-index guard preserves the body result")
      (check (and (= table-evaluations 1)
                  (= index-set-evaluations 1)
                  table-check-locked-p
                  index-check-locked-p
                  body-ran)
             "VACUUM FULL captures inputs once and validates under catalog locks")
      (let ((body-ran nil)
            (caught nil))
        (handler-case
            (with-current-table-index-set (table (list replacement-index))
              (setf body-ran t))
          (pg-error (condition) (setf caught condition)))
        (check (and caught (string= (pg-error-sqlstate caught) "40001"))
               "VACUUM FULL rejects replacement index metadata with serialization failure")
        (check (not body-ran)
               "VACUUM FULL does not mutate catalogs after index-set validation fails")
        (check (and (not (sb-thread:holding-mutex-p *write-lock*))
                    (not (sb-thread:holding-mutex-p *catalog-lock*)))
               "VACUUM FULL releases catalog locks after index-set validation fails"))
      (setf (gethash (pg-class-name table) (database-tables db))
            replacement-table)
      (let ((body-ran nil)
            (caught nil))
        (handler-case
            (with-current-table-index-set (table (list index))
              (setf body-ran t))
          (pg-error (condition) (setf caught condition)))
        (check (and caught (string= (pg-error-sqlstate caught) "42P01"))
               "VACUUM FULL rejects a stale table object under the catalog lock")
        (check (not body-ran)
               "VACUUM FULL does not mutate catalogs after table revalidation fails")
        (check (and (not (sb-thread:holding-mutex-p *write-lock*))
                    (not (sb-thread:holding-mutex-p *catalog-lock*)))
               "VACUUM FULL releases catalog locks after table validation fails")))))

(defun test-vacuum-xid-normalization-invariant ()
  (let* ((xmin 992001)
         (xmax 992002)
         (cutoff 992010)
         (offset 64)
         (aborted-xmin 992003)
         (committed-xmax 992004)
         (page (make-page))
         (raw-xmin (xid32-of xmin))
         (raw-xmax (xid32-of xmax))
         (local-xmin xmin)
         (local-xmax xmax)
         (changed nil))
    (clog-set xmin +clog-committed+)
    (clog-set xmax +clog-aborted+)
    (clog-set aborted-xmin +clog-aborted+)
    (clog-set committed-xmax +clog-committed+)
    (check (= (vacuum-normalized-xmin raw-xmin xmin cutoff) +frozen-xid+)
           "plain and FULL VACUUM share the committed-XMIN freeze rule")
    (check (= (vacuum-normalized-xmax raw-xmax xmax) +invalid-xid+)
           "plain and FULL VACUUM share the aborted-XMAX cleanup rule")
    (check (= (vacuum-normalized-xmin (xid32-of xmin) xmin xmin) xmin)
           "XMIN at the freeze cutoff is not frozen")
    (check (= (vacuum-normalized-xmin (xid32-of aborted-xmin)
                                      aborted-xmin cutoff)
              aborted-xmin)
           "aborted XMIN is not frozen")
    (check (= (vacuum-normalized-xmax (xid32-of committed-xmax)
                                      committed-xmax)
              committed-xmax)
           "committed XMAX is retained for visibility")
    (vacuum-normalize-tuple-xids
     page offset raw-xmin local-xmin raw-xmax local-xmax cutoff changed)
    (check (and changed
                (= raw-xmin +frozen-xid+)
                (= local-xmin +frozen-xid+)
                (= raw-xmax +invalid-xid+)
                (= local-xmax +invalid-xid+))
           "tuple-XID macro updates page source values and local copies together")
    (check (and (= (page-u32 page (+ offset +heap-tuple-xmin-off+))
                   +frozen-xid+)
                (= (page-u32 page (+ offset +heap-tuple-xmax-off+))
                   +invalid-xid+))
           "tuple-XID macro writes normalized XIDs to the tuple page")
    (check (= (vacuum-normalized-xmin (xid32-of +frozen-xid+)
                                      +frozen-xid+ cutoff)
              +frozen-xid+)
           "normalization preserves tuples already carrying the frozen XID")
    (check (= (vacuum-normalized-xmax 0 0) 0)
           "normalization preserves tuples without a deleting XID")))

(defun vacuum-full-test-read-file (path)
  (with-open-file (stream path :direction :input
                               :element-type '(unsigned-byte 8))
    (let ((bytes (make-array (file-length stream)
                             :element-type '(unsigned-byte 8))))
      (read-sequence bytes stream)
      bytes)))

(defun vacuum-full-test-write-relation-marker (relfilenode)
  (let ((path (relation-file-path relfilenode)))
    (ensure-directories-exist path)
    (with-open-file (stream path :direction :output
                                 :if-exists :supersede
                                 :if-does-not-exist :create
                                 :element-type '(unsigned-byte 8))
      (write-byte #x5A stream))))

(defun call-with-vacuum-full-publish-test-fixture (function)
  (let* ((test-pgdata (merge-pathnames "vacuum-full-publish-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*catalog* (make-catalog))
         (*catalog-oldest-relfrozenxid* nil)
         (*catalog-durable-generation* 0)
         (*backend* (make-backend))
         (*wal-ready* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (*wal-dirty-store* nil)
         (*wal-page-batch* nil)
         (*wal-page-batch-xids* nil)
         (*wal-pending-deletes* '())
         (*wal-pending-btree-split* nil)
         (*wal-catalog-flush-generation* 0)
         (*wal-vacuum-full-publication* nil)
         (*wal-test-before-vacuum-full-catalog-record-appended-hook* nil)
         (*wal-test-before-vacuum-full-catalog-flush-hook* nil)
         (*wal-test-after-vacuum-full-catalog-flush-hook* nil)
         (*wal-test-pause-at* nil)
         (*wal-test-pause-relfilenode* nil)
         (*wal-test-pause-fired* nil)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-poisoned* nil)
         (*relation-streams* (make-hash-table :test #'equal))
         (*catalog-rename-function* nil)
         (db (make-database :name "postgres" :oid 991000))
         (table (make-pg-class :oid 991001 :name "vac_full_fault"
                               :relfilenode 991002
                               :relfrozenxid +first-normal-xid+
                               :columns #() :indexes '(991003)
                               :toast (make-pg-toast :oid 991008 :relfilenode 991009
                                                     :index-oid 991010 :index-relfilenode 991011
                                                     :relfrozenxid +first-normal-xid+)))
         (index (make-pg-index :oid 991003 :name "vac_full_fault_idx"
                               :relfilenode 991004 :table-oid 991001
                               :columns '(1) :valid-p t))
         (new-table nil)
         (new-index nil)
         (new-relfilenodes '(991005 991006 991012 991013))
         (old-relfilenodes '(991002 991004 991009 991011))
         (original-fast-shutdown (symbol-function 'request-fast-shutdown)))
    (ignore-errors
     (uiop:delete-directory-tree test-pgdata :validate t
                                             :if-does-not-exist :ignore))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db
          (gethash (pg-class-name table) (database-tables db)) table
          (gethash (pg-index-name index) (database-indexes db)) index
          new-table (copy-pg-class table)
          new-index (copy-pg-index index)
          (pg-class-relfilenode new-table) (first new-relfilenodes)
          (pg-index-relfilenode new-index) (second new-relfilenodes)
          (pg-class-toast new-table) (copy-pg-toast (pg-class-toast table))
          (pg-toast-relfilenode (pg-class-toast new-table)) (third new-relfilenodes)
          (pg-toast-index-relfilenode (pg-class-toast new-table)) (fourth new-relfilenodes))
    (dolist (relfilenode (append old-relfilenodes new-relfilenodes))
      (vacuum-full-test-write-relation-marker relfilenode))
    (unwind-protect
         (progn
           ;; WAL failures normally request a postmaster fast shutdown. Keep the
           ;; unit process alive so the publication state can be inspected.
           (setf (symbol-function 'request-fast-shutdown)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (values)))
           (wal-init)
           (write-catalog-image (catalog-image-bytes))
           (funcall function db table index new-table new-index
                    old-relfilenodes new-relfilenodes
                    (vacuum-full-test-read-file (catalog-path))))
      (setf (symbol-function 'request-fast-shutdown)
            original-fast-shutdown)
      (sb-thread:with-mutex (*wal-lock*) (wal-clear-page-cache))
      (check (null *wal-dirty-store*) "VACUUM FULL fixture releases all dirty-page ownership")
      (smgr-close-all)
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun check-vacuum-full-publish-durable-state
    (db new-table new-index new-relfilenodes old-relfilenodes old-catalog-image)
  (check (eq (gethash (pg-class-name new-table) (database-tables db)) new-table)
         "durable VACUUM FULL error keeps replacement heap metadata")
  (check (eq (gethash (pg-index-name new-index) (database-indexes db)) new-index)
         "durable VACUUM FULL error keeps replacement index metadata")
  (check (every (lambda (relfilenode)
                  (probe-file (relation-file-path relfilenode)))
                new-relfilenodes)
         "durable VACUUM FULL error preserves replacement relation files")
  (check (every (lambda (relfilenode)
                  (not (probe-file (relation-file-path relfilenode))))
                old-relfilenodes)
         "durable VACUUM FULL error retires old relation files")
  (check (equalp (vacuum-full-test-read-file (catalog-path))
                 old-catalog-image)
         "failed catalog replacement leaves the previous catalog pathname intact"))

(defun vacuum-full-test-recover-appended-catalog-record
    (new-relfilenodes old-relfilenodes)
  (when *wal-output-stream*
    (close *wal-output-stream*)
    (setf *wal-output-stream* nil))
  (smgr-close-all)
  (setf *catalog* (make-catalog)
        *catalog-oldest-relfrozenxid* nil
        *wal-catalog-flush-generation* 0)
  (wal-init)
  (wal-recover)
  (setf *catalog* (load-catalog))
  (let* ((db (gethash "postgres" (catalog-databases *catalog*)))
         (table (gethash "vac_full_fault" (database-tables db)))
         (index (gethash "vac_full_fault_idx" (database-indexes db))))
    (check (= (pg-class-relfilenode table) (first new-relfilenodes))
           "restart replays a complete pre-flush catalog record")
    (check (= (pg-index-relfilenode index) (second new-relfilenodes))
           "restart replays the pre-flush replacement index metadata")
    (check (every (lambda (relfilenode)
                    (probe-file (relation-file-path relfilenode)))
                  new-relfilenodes)
           "restart retains files referenced by the replayed catalog")
    (check (every (lambda (relfilenode)
                    (not (probe-file (relation-file-path relfilenode))))
                  old-relfilenodes)
           "restart applies deferred deletion from the replayed catalog")))

(defun vacuum-full-test-recover-unappended-catalog-record
    (new-relfilenodes old-relfilenodes old-catalog-image
     sentinel-relfilenode sentinel-original-image)
  (when *wal-output-stream*
    (close *wal-output-stream*)
    (setf *wal-output-stream* nil))
  (smgr-close-all)
  (setf *catalog* (make-catalog)
        *catalog-oldest-relfrozenxid* nil
        *wal-catalog-flush-generation* 0)
  (wal-init)
  (wal-recover)
  (setf *catalog* (load-catalog))
  (let* ((db (gethash "postgres" (catalog-databases *catalog*)))
         (table (gethash "vac_full_fault" (database-tables db)))
         (index (gethash "vac_full_fault_idx" (database-indexes db))))
    (check (= (pg-class-relfilenode table) (first old-relfilenodes))
           "restart ignores a catalog batch without its terminal record")
    (check (= (pg-index-relfilenode index) (second old-relfilenodes))
           "restart keeps the original index metadata without a terminal record")
    (check (every (lambda (relfilenode)
                    (probe-file (relation-file-path relfilenode)))
                  old-relfilenodes)
           "restart retains original relation files without a terminal record")
    (check (every (lambda (relfilenode)
                    (not (probe-file (relation-file-path relfilenode))))
                  new-relfilenodes)
           "restart leaves unpublished replacement files absent")
    (check (equalp (vacuum-full-test-read-file (catalog-path))
                   old-catalog-image)
           "restart retains the original catalog image without a terminal record")
    (check (equalp (vacuum-full-test-read-file
                    (relation-file-path sentinel-relfilenode))
                   sentinel-original-image)
           "restart discards staged page images from an unterminated catalog batch")))

(defun vacuum-full-publish-fault-message (fault-point)
  (ecase fault-point
    (:before-record "injected before catalog WAL record")
    (:before-flush "injected before catalog WAL flush")
    (:after-flush "injected after catalog WAL flush")
    (:rename "injected during catalog-file replacement")))

(defun install-vacuum-full-publish-fault (fault-point sentinel-relfilenode inject)
  (ecase fault-point
    (:before-record
     ;; Include a complete page-image record in the unterminated catalog batch.
     (vacuum-full-test-write-relation-marker sentinel-relfilenode)
     (let ((original-image
             (vacuum-full-test-read-file
              (relation-file-path sentinel-relfilenode))))
       (let ((page (make-page)))
         (setf (aref page 0) #xA5)
         (buf-write sentinel-relfilenode 0 page))
       (setf *wal-test-before-vacuum-full-catalog-record-appended-hook*
             (lambda ()
               (finish-output *wal-output-stream*)
               (fsync-stream *wal-output-stream*)
               (when *wal-new-segments*
                 (fsync-directory (wal-directory))
                 (setf *wal-new-segments* '()))
               (funcall inject "injected before catalog WAL record")))
       original-image))
    (:before-flush
     (setf *wal-test-before-vacuum-full-catalog-flush-hook*
           (lambda ()
             ;; Leave the terminal record visible while durable-LSN still points
             ;; to the prior WAL boundary, as restart must do after a crash here.
             (finish-output *wal-output-stream*)
             (fsync-stream *wal-output-stream*)
             (when *wal-new-segments*
               (fsync-directory (wal-directory))
               (setf *wal-new-segments* '()))
             (funcall inject "injected before catalog WAL flush"))))
    (:after-flush
     (setf *wal-test-after-vacuum-full-catalog-flush-hook*
           (lambda () (funcall inject "injected after catalog WAL flush"))))
    (:rename
     (setf *catalog-rename-function*
           (lambda (&rest arguments)
             (declare (ignore arguments))
             (funcall inject "injected during catalog-file replacement"))))))

(defun check-vacuum-full-publish-fault-result
    (fault-point caught injected-condition preserve-replacements cleanup-ran
     db table index new-table new-index new-relfilenodes old-relfilenodes
     old-catalog-image sentinel-relfilenode sentinel-original-image)
  (check (and caught
              (eq caught injected-condition)
              (search (vacuum-full-publish-fault-message fault-point)
                      (princ-to-string caught)))
         (format nil "VACUUM FULL re-signals the ~a failure" fault-point))
  (check (= *wal-catalog-flush-generation*
            (if (member fault-point '(:before-record :before-flush)) 0 1))
         (format nil "~a fault lands on the expected side of WAL flush" fault-point))
  (cond
    ((eq fault-point :before-record)
     (check (not preserve-replacements)
            "failure before record append leaves replacement cleanup enabled")
     (check cleanup-ran
            "failure before record append cleans replacement files")
     (check (eq (gethash (pg-class-name table) (database-tables db)) table)
            "failure before record append restores original heap metadata")
     (check (eq (gethash (pg-index-name index) (database-indexes db)) index)
            "failure before record append restores original index metadata")
     (check (every (lambda (relfilenode)
                     (not (probe-file (relation-file-path relfilenode))))
                   new-relfilenodes)
            "failure before record append removes detached replacement files")
     (check (equalp (vacuum-full-test-read-file (catalog-path)) old-catalog-image)
            "failure before record append leaves the old catalog image intact")
     (vacuum-full-test-recover-unappended-catalog-record
      new-relfilenodes old-relfilenodes old-catalog-image
      sentinel-relfilenode sentinel-original-image))
    ((eq fault-point :before-flush)
     (check preserve-replacements
            "appended catalog record protects replacement files before flush")
     (check (not cleanup-ran)
            "pre-flush failure skips cleanup of replayable replacement files")
     (check (eq (gethash (pg-class-name new-table) (database-tables db)) new-table)
            "pre-flush failure keeps replacement heap metadata in memory")
     (check (eq (gethash (pg-index-name new-index)
                         (database-indexes db)) new-index)
            "pre-flush failure keeps replacement index metadata in memory")
     (check (every (lambda (relfilenode)
                     (probe-file (relation-file-path relfilenode)))
                   new-relfilenodes)
            "pre-flush failure retains replacement files referenced by replay")
     (check (every (lambda (relfilenode)
                     (probe-file (relation-file-path relfilenode)))
                   old-relfilenodes)
            "pre-flush failure defers old-file deletion until recovery")
     (vacuum-full-test-recover-appended-catalog-record
      new-relfilenodes old-relfilenodes))
    (t
     (check preserve-replacements
            "post-flush failure preserves replacement files")
     (check (not cleanup-ran)
            "post-flush failure skips unpublished-file cleanup")
     (check-vacuum-full-publish-durable-state
      db new-table new-index new-relfilenodes old-relfilenodes
      old-catalog-image))))

(defun run-vacuum-full-publish-fault-test (fault-point)
  (call-with-vacuum-full-publish-test-fixture
   (lambda (db table index new-table new-index
            old-relfilenodes new-relfilenodes old-catalog-image)
     (let ((preserve-replacements nil)
           (cleanup-ran nil)
           (caught nil)
           (injected-condition nil)
           (sentinel-relfilenode 991007)
           (sentinel-original-image nil))
       (flet ((inject (message)
                (setf injected-condition
                      (make-condition 'simple-error
                                      :format-control message
                                      :format-arguments nil))
                (error injected-condition)))
         (let ((original-image
                 (install-vacuum-full-publish-fault
                  fault-point sentinel-relfilenode #'inject)))
           (when (eq fault-point :before-record)
             (setf sentinel-original-image original-image)))
         (handler-case
             (with-error-cleanup
                 (condition
                  (unless preserve-replacements
                    (setf cleanup-ran t)
                    (ignore-errors
                     (wal-discard-relation-pages new-relfilenodes))
                    (dolist (relfilenode new-relfilenodes)
                      (ignore-errors (smgr-forget relfilenode :delete t)))))
               (vacuum-full-publish
                db table (list index) new-table (list new-index)
                :on-wal-appended
                (lambda () (setf preserve-replacements t))))
           (error (condition) (setf caught condition)))
         (check-vacuum-full-publish-fault-result
          fault-point caught injected-condition preserve-replacements cleanup-ran
          db table index new-table new-index new-relfilenodes old-relfilenodes
          old-catalog-image sentinel-relfilenode sentinel-original-image))))))

(defun test-vacuum-full-publish-failure-boundaries ()
  (run-vacuum-full-publish-fault-test :before-record)
  (run-vacuum-full-publish-fault-test :before-flush)
  (run-vacuum-full-publish-fault-test :after-flush)
  (run-vacuum-full-publish-fault-test :rename))

(defun test-vacuum-page-retry-rebuilds-proposal ()
  (let ((*wal-ready* nil)
        (*wal-page-cache* (make-hash-table :test #'equal))
        (*wal-page-batch* nil)
        (*wal-test-pause-at* nil)
        (read-count 0)
        (proposal-count 0)
        (publication-results '())
        (page-values '())
        (baseline-values '())
        (relation-evaluations 0)
        (block-evaluations 0)
        (accepted-value nil)
        (original-buf-read (symbol-function 'buf-read)))
    (unwind-protect
         (progn
           (smgr-forget 992022 :delete t)
           (setf (symbol-function 'buf-read)
                 (lambda (relfilenode block &optional fork)
                   (declare (ignore fork))
                   (check (= relfilenode 992022)
                          "VACUUM page retry captures its relation")
                   (check (= block 3)
                          "VACUUM page retry captures its block")
                   (incf read-count)
                   (case read-count
                     (1 (vector :page 1))
                     (2 (vector :page 2))
                     ((3 4) (vector :page 2))
                     (otherwise
                      (error "unexpected VACUUM page read ~a" read-count)))))
           (let* ((relfilenode (progn
                                 (incf relation-evaluations)
                                 992022))
                  (block (progn (incf block-evaluations) 3)))
             (with-vacuum-page-retry (page source-page relfilenode block)
               (check (and (= relfilenode 992022) (= block 3))
                      "VACUUM page retry exposes its captured page address")
               (push (aref page 1) page-values)
               (push (aref source-page 1) baseline-values)
               (incf proposal-count)
               (setf (aref page 1) (1+ (aref page 1)))
               (let ((published
                       (try-vacuum-page-publication
                        relfilenode block source-page
                        :page page :write-page-p nil)))
                 (push published publication-results)
                 (when published
                   (setf accepted-value (aref page 1))
                   t))))
           (check (equal (reverse page-values) '(1 2))
                  "a failed page compare recomputes from a fresh page")
           (check (equal (reverse baseline-values) '(1 2))
                  "each VACUUM retry captures a fresh unchanged baseline")
           (check (equal (reverse publication-results) '(nil t))
                  "VACUUM retry discards a proposal after a failed page compare")
           (check (and (= read-count 4) (= proposal-count 2)
                       (= relation-evaluations 1) (= block-evaluations 1)
                       (= accepted-value 3))
                  "VACUUM page retry reevaluates its proposal, not its relation")
           (setf (symbol-function 'buf-read)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   nil))
           (let ((relfilenode 992022)
                 (block 3))
             (check (handler-case
                        (progn
                          (with-vacuum-page-retry
                              (page source-page relfilenode block)
                            (values page source-page relfilenode block))
                          nil)
                      (error (condition)
                        (search "disappeared during VACUUM"
                                (princ-to-string condition))))
                    "VACUUM page retry errors when its page disappears")))
      (setf (symbol-function 'buf-read) original-buf-read)
      (smgr-forget 992022 :delete t))))

(defun test-vacuum-page-publication-is-atomic ()
  (let* ((*wal-ready* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (*wal-dirty-store* nil)
         (*wal-page-batch* nil)
         (*wal-page-batch-xids* nil)
         (*wal-test-pause-at* nil)
         (relfilenode 918273657)
         (heap-block 0)
         (heap-page (page-init (make-page)))
         (changed-page nil)
         (original-visibilitymap-clear
           (symbol-function 'visibilitymap-clear))
         (staged-heap-write-seen nil)
         (injected-error nil))
    (smgr-forget relfilenode :delete t)
    (ensure-directories-exist (relation-file-path relfilenode))
    (setf (aref heap-page 100) #x11
          changed-page (copy-seq heap-page))
    (unwind-protect
         (progn
           (smgr-write relfilenode heap-block heap-page)
           (visibilitymap-set-status
            relfilenode heap-block
            (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
           (let ((*wal-ready* t))
             (setf (aref changed-page 100) #x22
                   (symbol-function 'visibilitymap-clear)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (let ((staged-page
                             (and *wal-page-batch*
                                  (gethash
                                   (wal-page-key relfilenode heap-block)
                                   *wal-page-batch*))))
                       (setf staged-heap-write-seen
                             (and staged-page
                                  (= (aref (wal-dirty-read-reference
                                            (wal-page-key relfilenode heap-block) staged-page) 100)
                                     #x22))))
                     (error "injected failure before visibility-map clear")))
             (unwind-protect
                  (progn
                    (setf injected-error
                          (handler-case
                              (progn
                                (try-vacuum-page-publication
                                 relfilenode heap-block heap-page
                                 :page changed-page :write-page-p t)
                                nil)
                            (error (condition)
                              (search "injected failure before visibility-map clear"
                                      (princ-to-string condition)))))
                    (check injected-error
                           "VACUUM page publication propagates a VM-clear failure")
                    (check staged-heap-write-seen
                           "fault is injected after the heap page is staged")
                    (check (not (nth-value
                                 1 (gethash (wal-page-key relfilenode heap-block)
                                            *wal-page-cache*)))
                           "failed VACUUM publication exposes no staged heap page")
                    (check (not (nth-value
                                 1 (gethash (wal-page-key relfilenode 0 :visibility)
                                            *wal-page-cache*)))
                           "failed VACUUM publication exposes no staged VM page")
                    (check (equalp (buf-read relfilenode heap-block) heap-page)
                           "failed VACUUM publication preserves the original heap page")
                    (check (= (visibilitymap-status relfilenode heap-block)
                              (logior +vm-all-visible-bit+
                                      +vm-all-frozen-bit+))
                           "failed VACUUM publication preserves visibility bits"))
               (setf (symbol-function 'visibilitymap-clear)
                     original-visibilitymap-clear))
             (check (try-vacuum-page-publication
                     relfilenode heap-block heap-page
                     :page changed-page :write-page-p t)
                    "stable heap page accepts a VACUUM publication")
             (check (and (nth-value
                          1 (gethash (wal-page-key relfilenode heap-block)
                                     *wal-page-cache*))
                         (nth-value
                          1 (gethash (wal-page-key relfilenode 0 :visibility)
                                     *wal-page-cache*)))
                    "successful VACUUM publication exposes both pages together")
             (check (= (aref (buf-read relfilenode heap-block) 100) #x22)
                    "successful VACUUM publication exposes the heap edit")
             (check (zerop (visibilitymap-status relfilenode heap-block))
                    "successful VACUUM publication clears visibility bits")))
      (setf (symbol-function 'visibilitymap-clear)
            original-visibilitymap-clear)
      (sb-thread:with-mutex (*wal-lock*) (wal-clear-page-cache))
      (check (null *wal-dirty-store*) "VACUUM fixture releases all dirty-page ownership")
      (smgr-forget relfilenode :delete t))))

(defun test-vacuum-page-read-releases-global-write-lock ()
  (let* ((*wal-ready* nil)
         (*heap-fsms* (make-hash-table :test #'eql))
         (heap-relfilenode 918273658)
         (other-relfilenode 918273659)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (test-pgdata *pgdata*)
         (test-fsms *heap-fsms*)
         (vacuum-backend (make-backend))
         (writer-backend (make-backend))
         (test-lock (sb-thread:make-mutex :name "vacuum copied-page test"))
         (test-cv (sb-thread:make-waitqueue :name "vacuum copied-page cv"))
         (page-read-paused nil)
         (resume-scan nil)
         (writer-done nil)
         (vacuum-done nil)
         (writer-error nil)
         (vacuum-error nil)
         (threads '())
         (original-buf-read (symbol-function 'buf-read)))
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget other-relfilenode :delete t)
    (clog-set 8210 +clog-committed+)
    (clog-set 8211 +clog-committed+)
    (unwind-protect
         (progn
           (heapam-insert heap-relfilenode columns #(1) 8210 0)
           ;; Keep FSM initialization out of the paused scan read.
           (heap-fsm-for-relation heap-relfilenode)
           (setf (symbol-function 'buf-read)
                 (lambda (relfilenode block &optional (fork :main))
                   (let ((page (funcall original-buf-read relfilenode block fork)))
                     (when (and page
                                (eq fork :main)
                                (= relfilenode heap-relfilenode)
                                (zerop block)
                                (sb-thread:with-mutex (test-lock)
                                  (unless page-read-paused
                                    (setf page-read-paused t)
                                    (sb-thread:condition-broadcast test-cv)
                                    t)))
                       (sb-thread:with-mutex (test-lock)
                         (loop until resume-scan
                               do (sb-thread:condition-wait test-cv test-lock))))
                     page)))
           (push
            (sb-thread:make-thread
             (lambda ()
               (let ((*pgdata* test-pgdata)
                     (*heap-fsms* test-fsms)
                     (*wal-ready* nil)
                     (*backend* vacuum-backend))
                 (handler-case
                     (heap-vacuum-relation heap-relfilenode nil 8212 8100)
                   (error (condition) (setf vacuum-error condition))))
               (sb-thread:with-mutex (test-lock)
                 (setf vacuum-done t)
                 (sb-thread:condition-broadcast test-cv)))
             :name "vacuum copied-page scan")
            threads)
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) page-read-paused)))
                  "VACUUM reaches a private copied-page read")
           (push
            (sb-thread:make-thread
             (lambda ()
               (let ((*pgdata* test-pgdata)
                     (*heap-fsms* test-fsms)
                     (*wal-ready* nil)
                     (*backend* writer-backend))
                 (handler-case
                     (with-write-lock
                       (heapam-insert other-relfilenode columns #(2) 8211 0))
                   (error (condition) (setf writer-error condition))))
               (sb-thread:with-mutex (test-lock)
                 (setf writer-done t)
                 (sb-thread:condition-broadcast test-cv)))
             :name "unrelated heap writer during vacuum")
            threads)
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) writer-done))
                   1.0)
                  "an unrelated heap INSERT progresses during VACUUM page work")
           (sb-thread:with-mutex (test-lock)
             (setf resume-scan t)
             (sb-thread:condition-broadcast test-cv))
           (dolist (thread threads)
             (sb-thread:join-thread thread :timeout 3))
           (check (null writer-error)
                  (format nil "the unrelated heap INSERT finishes without an error: ~a"
                          writer-error))
           (check (null vacuum-error)
                  (format nil "VACUUM completes after its copied page is released: ~a"
                          vacuum-error))
           (check vacuum-done "VACUUM worker terminates"))
      (sb-thread:with-mutex (test-lock)
        (setf resume-scan t)
        (sb-thread:condition-broadcast test-cv))
      (dolist (thread threads)
        (ignore-errors (sb-thread:join-thread thread :timeout 3)))
      (setf (symbol-function 'buf-read) original-buf-read)
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget other-relfilenode :delete t))))

(defun test-vacuum-reuses-tid-clears-index-and-vm ()
  (let* ((*heap-fsms* (make-hash-table :test #'eql))
         (*wal-ready* nil)
         (heap-relfilenode 918273655)
         (index-relfilenode 918273656)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (old-key (encode-index-key '(1) '(:int4)))
         (new-key (encode-index-key '(2) '(:int4)))
         (old-tid nil))
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget index-relfilenode :delete t)
    (clog-set 8200 +clog-committed+)
    (clog-set 8201 +clog-committed+)
    (unwind-protect
         (progn
           (btree-init index-relfilenode)
           (setf old-tid
                 (multiple-value-list
                  (heapam-insert heap-relfilenode columns #(1) 8200 0)))
           (btree-insert index-relfilenode old-key (first old-tid)
                         (second old-tid))
           (with-write-lock
             (let ((page (buf-read heap-relfilenode (first old-tid))))
               (multiple-value-bind (tuple-off flags len)
                   (page-get-lp page (second old-tid))
                 (declare (ignore flags len))
                 (page-set-u32 page (+ tuple-off +heap-tuple-xmax-off+) 8201))
               (buf-write heap-relfilenode (first old-tid) page)))
           (heap-vacuum-relation heap-relfilenode (list index-relfilenode)
                                 8202 8100)
           (check (= (visibilitymap-status heap-relfilenode (first old-tid))
                     (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
                  "VACUUM marks the reclaimed empty page all-visible and all-frozen")
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode (first old-tid))
                            (second old-tid))
             (declare (ignore off len))
             (check (= flags +lp-unused+)
                    "VACUUM makes a deleted tuple TID reusable"))
           (visibilitymap-set-status
            heap-relfilenode (first old-tid)
            (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
           (check (= (visibilitymap-status heap-relfilenode (first old-tid))
                     (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
                  "fixture marks the reusable page all-visible and all-frozen")
           (multiple-value-bind (new-block new-off)
               (heapam-insert heap-relfilenode columns #(2) 8202 0)
             (btree-insert index-relfilenode new-key new-block new-off)
             (check (equal (list new-block new-off) old-tid)
                    "INSERT reuses the vacuumed heap TID")
             (check (null (btree-search index-relfilenode old-key))
                    "reused TID has no stale entry under its old index key")
             (check (equal (btree-search index-relfilenode new-key)
                           (list (cons new-block new-off)))
                    "new index key points to the reused TID")
             (check (zerop (visibilitymap-status heap-relfilenode new-block))
                    "INSERT clears all-visible and all-frozen VM bits")))
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget index-relfilenode :delete t))))

(defun test-vacuum-snapshot-horizon ()
  ;; Port the permanent-table horizon case from
  ;; src/test/isolation/specs/horizons.spec. The Lisp executor currently keeps
  ;; snapshots per statement, so register the older snapshot directly here.
  (let* ((*backend* (make-backend))
         (*next-xid* 7303)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-ready* nil)
         (heap-relfilenode 918273651)
         (index-relfilenode 918273652)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (key (encode-index-key '(1) '(:int4)))
         (snapshot nil)
         (registration (make-snapshot-registration))
         (page (page-init (make-page)))
         (offset (page-add-item
                  page (form-heap-tuple #(1) columns 7301 0 0 0 1)))
         (snapshot-active nil)
         (index-cleanup-before-heap-reuse nil))
    ;; The snapshot starts after the insert but before the committed delete.
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget index-relfilenode :delete t)
    (clog-set 7301 +clog-committed+)
    (unwind-protect
         (progn
           (ensure-directories-exist (relation-file-path heap-relfilenode))
           (check (= (buf-extend heap-relfilenode page) 0)
                  "vacuum horizon fixture creates heap block zero")
           (btree-init index-relfilenode)
           (btree-insert index-relfilenode key 0 offset)
           (setf snapshot-active t
                 snapshot (make-snapshot 1 :register registration)
                 *next-xid* 7304)
           (with-write-lock
             (let ((heap-page (buf-read heap-relfilenode 0)))
               (multiple-value-bind (tuple-off flags len)
                   (page-get-lp heap-page offset)
                 (declare (ignore flags len))
                 (page-set-u32 heap-page
                               (+ tuple-off +heap-tuple-xmax-off+) 7303)
                 (buf-write heap-relfilenode 0 heap-page))))
           (clog-set 7303 +clog-committed+)
           (check (= (snapshot-xmax snapshot) 7303)
                  "snapshot treats the later delete XID as in the future")
           (check (= (oldest-xmin) 7303)
                  "registered old snapshot holds the vacuum removal horizon")
           (heap-vacuum-relation heap-relfilenode (list index-relfilenode)
                                 (oldest-xmin) 7300)
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode 0) offset)
             (declare (ignore off len))
             (check (= flags +lp-normal+)
                    "vacuum preserves a deleted tuple visible to the old snapshot"))
           (let ((tuple (heapam-get-tuple heap-relfilenode 0 offset columns)))
             (check (and tuple
                         (heap-tuple-visible-p
                          (htup-xmin tuple) (htup-xmax tuple) (htup-cid tuple)
                          snapshot nil))
                    "old snapshot can still see the tuple after vacuum")
             (check (equal (btree-search index-relfilenode key)
                           (list (cons 0 offset)))
                    "vacuum retains the tuple's index TID behind the old horizon"))
           (release-snapshot-registration registration)
           (setf snapshot-active nil)
           (check (= (oldest-xmin) 7304)
                  "releasing the old snapshot advances the removal horizon")
           (let ((original-btree-vacuum-delete-tids
                   (symbol-function 'btree-vacuum-delete-tids)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'btree-vacuum-delete-tids)
                          (lambda (index-relfilenode tids)
                            (funcall original-btree-vacuum-delete-tids
                                     index-relfilenode tids)
                            (multiple-value-bind (off flags len)
                                (page-get-lp (buf-read heap-relfilenode 0) offset)
                              (declare (ignore off len))
                              (setf index-cleanup-before-heap-reuse
                                    (and (null (btree-search
                                                index-relfilenode key))
                                         (= flags +lp-dead+))))))
                    (heap-vacuum-relation heap-relfilenode
                                          (list index-relfilenode)
                                          (oldest-xmin) 7300))
               (setf (symbol-function 'btree-vacuum-delete-tids)
                     original-btree-vacuum-delete-tids)))
           (check index-cleanup-before-heap-reuse
                  "vacuum removes the index TID while its heap slot is still dead")
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode 0) offset)
             (declare (ignore off len))
             (check (= flags +lp-unused+)
                    "vacuum makes the dead heap slot reusable after horizon advance"))
           (check (null (btree-search index-relfilenode key))
                  "vacuum removes dead index TIDs before heap-slot reuse")
           (let* ((fsm (gethash heap-relfilenode *heap-fsms*))
                  (recorded-space (and fsm
                                       (gethash 0 (heap-fsm-page-space fsm))))
                  (page-space
                    (heap-page-insert-capacity (buf-read heap-relfilenode 0))))
             (check (and recorded-space (= recorded-space page-space))
                    "vacuum records reclaimed heap space in the FSM")))
      (when snapshot-active
        (release-snapshot-registration registration))
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget index-relfilenode :delete t))))

(defun test-btree-duplicate-keys ()
  ;; Insert many entries with one key so the leaves holding them must split.
  ;; Repeatedly updating an indexed value produces exactly this shape: a long
  ;; run of index entries with equal keys spread over several leaves.
  (let* ((rel 987654)
         (key (encode-index-key (list 42) (list :int4)))
         (n 2000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel key 0 i))
    ;; Internal separators do not include heap TIDs, so an equal key with a
    ;; lower TID can land in a later leaf. PostgreSQL's heapkeyspace routing
    ;; in nbtinsert.c keeps this ordering global; our cursor must preserve the
    ;; tree's own physical traversal instead of using TID as a global anchor.
    (btree-insert rel key 1 1)
    (let ((found (btree-search rel key)))
      (check (= (length found) (1+ n))
             "duplicate keys are found across leaf splits")
      (check (= (length (remove-duplicates found :test #'equal)) (1+ n))
             "every duplicate-key TID is distinct")
      (let ((cursor-tids '())
            (visited 0))
        (btree-scan-equal rel key (lambda (tid) (push tid cursor-tids)))
        (check (equal (nreverse cursor-tids) found)
               "duplicate-key cursor streams every leaf TID in index order")
        (check (eq (btree-scan-equal
                    rel key
                    (lambda (tid)
                      (declare (ignore tid))
                      (check (= (length (btree-search rel key)) (1+ n))
                             "a cursor callback may re-enter the same index as a reader")
                      :stop))
                   :stop)
               "nested index reads do not deadlock the duplicate-key cursor")
        (check (eq (btree-scan-equal rel key
                                     (lambda (tid)
                                       (declare (ignore tid))
                                       (incf visited)
                                       :stop))
                   :stop)
               "duplicate-key cursor reports early termination")
        (check (= visited 1)
               "duplicate-key cursor reads no candidate after early stop"))
      (check (null (btree-search rel (encode-index-key (list 43) (list :int4))))
             "an absent key is not found")
      (btree-vacuum-delete-tids
       rel (append (loop for i below (/ n 2) collect (cons 0 i))
                   '((1 . 1))))
      (let ((remaining (btree-search rel key)))
        (check (= (length remaining) (/ n 2))
               "B-tree VACUUM removes dead TIDs across duplicate-key leaves")
        (check (every (lambda (tid) (>= (cdr tid) (/ n 2))) remaining)
               "B-tree VACUUM preserves every remaining duplicate-key TID")))))

(defun test-btree-relation-lock-macro ()
  (let* ((relfilenode 987657)
         (state (btree-relation-lock relfilenode))
         (evaluations 0)
         (writer-owned-p nil)
         (nested-reader-p nil))
    (with-btree-relation-lock ((progn (incf evaluations) relfilenode))
      (setf writer-owned-p
            (eq sb-thread:*current-thread*
                (btree-relation-lock-state-writer-owner state))
            nested-reader-p
            (eq (call-with-btree-relation-read-lock relfilenode
                                                    (lambda () :read))
                :read)))
    (check (and (= evaluations 1) writer-owned-p nested-reader-p)
           "B-tree relation-lock macro evaluates once and permits a nested read")
    (check (and (null (btree-relation-lock-state-writer-owner state))
                (zerop (btree-relation-lock-state-reader-count state)))
           "B-tree relation-lock macro releases ownership after its body")
    (btree-forget relfilenode)))

(defun test-btree-relation-lock-set-macro ()
  (let* ((low-relfilenode 987661)
         (high-relfilenode 987662)
         (low-state (btree-relation-lock low-relfilenode))
         (high-state (btree-relation-lock high-relfilenode))
         (evaluations 0)
         (inside-p nil))
    (with-btree-relation-locks
        ((progn
           (incf evaluations)
           (list high-relfilenode low-relfilenode high-relfilenode)))
      (setf inside-p
            (and (eq (btree-relation-lock-state-writer-owner low-state)
                     sb-thread:*current-thread*)
                 (eq (btree-relation-lock-state-writer-owner high-state)
                     sb-thread:*current-thread*)
                 (= (btree-relation-lock-state-writer-depth low-state) 1)
                 (= (btree-relation-lock-state-writer-depth high-state) 1))))
    (check (and (= evaluations 1) inside-p)
           "B-tree writer-lock set evaluates once and deduplicates relations")
    (check (and (null (btree-relation-lock-state-writer-owner low-state))
                (null (btree-relation-lock-state-writer-owner high-state)))
           "B-tree writer-lock set releases each relation after its body")
    (btree-forget low-relfilenode)
    (btree-forget high-relfilenode)))

(defun test-btree-writer-interrupt-cleans-waiter ()
  (let* ((relfilenode 987660)
         (state (btree-relation-lock relfilenode))
         (original-wait (symbol-function 'btree-relation-condition-wait))
         (writer-interrupted-p nil))
    (unwind-protect
         (progn
           ;; Keep a read lease while the writer reaches its wait, then force
           ;; that wait to unwind so waiter cleanup is exercised.
           (setf (symbol-function 'btree-relation-condition-wait)
                 (lambda (state)
                   (declare (ignore state))
                   (error "forced B-tree writer wait interrupt")))
           (call-with-btree-relation-read-lock
            relfilenode
            (lambda ()
              (let ((writer
                      (sb-thread:make-thread
                       (lambda ()
                         (handler-case
                             (call-with-btree-relation-write-lock relfilenode
                                                                  (lambda () :writer))
                           (error ()
                             (setf writer-interrupted-p t)))))))
                (sb-thread:join-thread writer))))
           (check writer-interrupted-p
                  "an interrupted B-tree writer exits its condition wait")
           (check (zerop (btree-relation-lock-state-waiting-writers state))
                  "an interrupted B-tree writer unregisters its waiter")
           (check (eq (call-with-btree-relation-read-lock
                       relfilenode (lambda () :reader))
                      :reader)
                  "readers proceed after an interrupted writer releases its waiter"))
      (setf (symbol-function 'btree-relation-condition-wait) original-wait)
      (btree-forget relfilenode))))

(defun test-btree-read-lock-set-macro ()
  (let* ((low-relfilenode 987658)
         (high-relfilenode 987659)
         (low-state (btree-relation-lock low-relfilenode))
         (high-state (btree-relation-lock high-relfilenode))
         (inside-p nil))
    (with-btree-relation-read-locks
        ((list high-relfilenode low-relfilenode high-relfilenode))
      (setf inside-p
            (and (= (btree-relation-lock-state-reader-count low-state) 1)
                 (= (btree-relation-lock-state-reader-count high-state) 1)
                 (= (gethash sb-thread:*current-thread*
                             (btree-relation-lock-state-reader-owners low-state))
                    1)
                 (= (gethash sb-thread:*current-thread*
                             (btree-relation-lock-state-reader-owners high-state))
                    1)))
      (check inside-p
             "B-tree read-lock set deduplicates and holds every requested lease")
      (check (eq (call-with-btree-relation-read-lock
                  low-relfilenode (lambda () :nested-read))
                 :nested-read)
             "B-tree read-lock sets permit nested same-relation scans"))
    (check (and (zerop (btree-relation-lock-state-reader-count low-state))
                (zerop (btree-relation-lock-state-reader-count high-state)))
           "B-tree read-lock set releases every lease after its body")
    (btree-forget low-relfilenode)
    (btree-forget high-relfilenode)))

(defun test-btree-mixed-duplicate-keys ()
  ;; Interleave two keys so splits produce equal separators for both, which
  ;; stresses the key-range order that search assumes.
  (let* ((rel 987655)
         (ka (encode-index-key (list 1) (list :int4)))
         (kb (encode-index-key (list 2) (list :int4)))
         (n 3000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel (if (evenp i) ka kb) 0 i))
    (let ((fa (btree-search rel ka))
          (fb (btree-search rel kb)))
      (check (= (length fa) (/ n 2)) "even entries found (mixed keys)")
      (check (= (length fb) (/ n 2)) "odd entries found (mixed keys)")
      (check (every (lambda (tid) (evenp (cdr tid))) fa)
             "key A search returned only key-A TIDs")
      (check (every (lambda (tid) (oddp (cdr tid))) fb)
             "key B search returned only key-B TIDs"))))

(defun test-btree-vacuum-bounded-split-retries ()
  (let* ((rel 918273654)
         (dead-key (encode-index-key '(4242) '(:int4)))
         (pass-count 0)
         (split-pass-count 0)
         (inserted 0))
    (smgr-forget rel :delete t)
    (unwind-protect
         (progn
           (btree-init rel)
           (dotimes (i 600)
             (with-write-lock
               (btree-insert rel (encode-index-key (list i) '(:int4)) 0 i)))
           (with-write-lock
             (btree-insert rel dead-key 90000 1))
           (let ((initial-generation (btree-split-generation rel)))
             (btree-vacuum-delete-tids
              rel '((90000 . 1))
              :after-page-function
              (lambda (index-relfilenode block)
                (declare (ignore index-relfilenode))
                (when (zerop block)
                  (incf pass-count)
                  (let ((generation (btree-split-generation rel)))
                    (dotimes (i 700)
                      (with-write-lock
                        (btree-insert
                         rel (encode-index-key (list (+ 100000 inserted i))
                                               '(:int4))
                         91000 i)))
                    (incf inserted 700)
                    (when (> (btree-split-generation rel) generation)
                      (incf split-pass-count))))))
             (check (= pass-count (1+ +btree-vacuum-max-split-retries+))
                    "B-tree VACUUM bounds retries before its exclusive final pass")
             (check (= split-pass-count pass-count)
                    "each optimistic cleanup pass overlaps a real B-tree split")
             (check (> (btree-split-generation rel) initial-generation)
                    "interleaved inserts advance the split generation")
             (check (null (btree-search rel dead-key))
                    "the final stable pass removes the dead TID")))
      (smgr-forget rel :delete t))
    (check (not (nth-value 1 (gethash rel *btree-split-generations*)))
           "permanently forgetting an index clears its split generation")))
