;;;; vacuum.lisp -- table-level VACUUM horizons and catalog updates.

(in-package #:pg)

(defun current-table-index-relfilenodes (table)
  (mapcar #'pg-index-relfilenode (table-indexes table)))

(defun call-with-revalidated-relation-locks
    (state-provider requested-relations acquire-lock release-lock
     valid-state-p function)
  "Retry FUNCTION until its captured relation state is protected by locks.

STATE-PROVIDER returns catalog state before and after locking.
REQUESTED-RELATIONS derives the initial lock set; VALID-STATE-P verifies that
the current state is covered. Acquired locks are always released, including
when revalidation or FUNCTION signals an error."
  (loop
    for captured = (funcall state-provider)
    for requested = (funcall requested-relations captured)
    do (multiple-value-bind (locked-relations token)
           (funcall acquire-lock requested)
         (unwind-protect
              (let ((current (funcall state-provider)))
                (when (funcall valid-state-p
                               captured current locked-relations)
                  (return-from call-with-revalidated-relation-locks
                    (funcall function current))))
           (funcall release-lock locked-relations token)))))

(defun call-with-table-maintenance-locks
    (table index-provider verify-function function)
  "Hold the heap and current index maintenance locks around FUNCTION."
  (call-with-revalidated-relation-locks
   (lambda ()
     (with-write-lock
       (list (pg-class-relfilenode table) (funcall index-provider))))
   (lambda (state) (cons (first state) (second state)))
   #'acquire-relation-maintenance-locks
   #'release-relation-maintenance-locks
   (lambda (captured current locked-relations)
     (and (= (first captured) (first current))
          (every (lambda (relfilenode)
                   (member relfilenode locked-relations :test #'=))
                 (cons (first current) (second current)))))
   (lambda (current)
     (when verify-function
       (with-write-lock (funcall verify-function)))
     (funcall function (second current)))))

(defun vacuum-catalog-relation
    (table index-provider verify-function
     &key forced-oldest-xmin forced-freeze-cutoff after-page-function
       fsync-function)
  "Vacuum one catalog relation and durably advance its relfrozenxid."
  (call-with-table-maintenance-locks
   table index-provider verify-function
   (lambda (index-relfilenodes)
     (let* ((next-xid (next-full-xid))
            ;; C's PROC_IN_VACUUM flag keeps the vacuum statement's own
            ;; registered snapshot from holding back its removal horizon.
            (oldest-xmin (or forced-oldest-xmin
                             (oldest-xmin *current-snapshot*)))
            (old-relfrozenxid (or (pg-class-relfrozenxid table)
                                  +first-normal-xid+))
            (age (max 0 (- next-xid old-relfrozenxid)))
            (freeze-target
              (if forced-freeze-cutoff
                  forced-freeze-cutoff
                  (if (>= age +vacuum-aggressive-freeze-age+)
                      next-xid
                      (max +first-normal-xid+
                           (- next-xid +vacuum-freeze-min-age+)))))
            (freeze-cutoff (min oldest-xmin freeze-target)))
       (heap-vacuum-relation-locked
        (pg-class-relfilenode table) index-relfilenodes
        oldest-xmin freeze-cutoff
        :after-page-function after-page-function)
       (with-write-catalog-locks
         (when verify-function (funcall verify-function))
         (setf (pg-class-relfrozenxid table)
               (max old-relfrozenxid freeze-cutoff))
         (save-catalog))
       (when fsync-function
         (funcall fsync-function (pg-class-relfilenode table)))
       (pg-class-relfrozenxid table)))))

(defun vacuum-table (table)
  "Run a plain manual VACUUM for TABLE in the current database."
  (vacuum-catalog-relation
   table
   (lambda () (current-table-index-relfilenodes table))
   (lambda () (ensure-current-table table))))

(defun same-index-object-set-p (left right)
  "Compare index membership and metadata identity without depending on order."
  (and (= (length left) (length right))
       (= (length left) (length (remove-duplicates left :test #'eq)))
       (= (length right) (length (remove-duplicates right :test #'eq)))
       (every (lambda (index)
                (member index right :test #'eq))
              left)))

(defun call-with-table-rewrite-locks (table function)
  "Hold exclusive build gates for TABLE and its exact current index set."
  (call-with-revalidated-relation-locks
   (lambda ()
     (with-write-lock
       (ensure-current-table table)
       (let ((indexes (table-indexes table)))
         (list (pg-class-relfilenode table)
               indexes
               (mapcar #'pg-index-relfilenode indexes)))))
   (lambda (state) (cons (first state) (third state)))
   (lambda (requested)
     (let ((locked-relations
             (sort (remove-duplicates (copy-list requested) :test #'=) #'<)))
       (values locked-relations
               (acquire-relation-build-locks
                locked-relations
                (and *backend* (backend-xid *backend*))
                t))))
   #'release-relation-build-locks
   (lambda (captured current locked-relations)
     (let ((indexes (second captured))
           (index-relfilenodes (third captured))
           (current-indexes (second current))
           (current-relfilenodes (third current)))
       (and (= (first captured) (first current))
            (= (length indexes) (length current-indexes))
            (same-index-object-set-p indexes current-indexes)
            (= (length index-relfilenodes)
               (length current-relfilenodes))
            (every (lambda (relfilenode)
                     (member relfilenode locked-relations :test #'=))
                   (cons (first current) current-relfilenodes)))))
   (lambda (current)
     (funcall function (second current)))))

(defun vacuum-full-copy-tuple (source-table source-relfilenode
                               target-relfilenode block offnum oldest-xmin
                               freeze-cutoff)
  (let ((tuple (heapam-get-tuple
                source-relfilenode block offnum
                (pg-class-columns source-table))))
    (when (and tuple
               (not (tuple-prunable-p (htup-xmin tuple)
                                      (htup-xmax tuple) oldest-xmin)))
      (let* ((xmin (htup-xmin tuple))
             (raw-xmax (htup-xmax tuple))
             (copy-xmin (if (and (>= xmin +first-normal-xid+)
                                 (< xmin freeze-cutoff)
                                 (xid-committed-p xmin))
                            +frozen-xid+
                            xmin))
             (copy-xmax (if (and (not (zerop raw-xmax))
                                 (xid-aborted-p (htup-xmax tuple)))
                            +invalid-xid+
                            raw-xmax)))
        (multiple-value-bind (target-block target-offnum)
            (heapam-insert target-relfilenode
                           (pg-class-columns source-table)
                           (htup-values tuple) copy-xmin (htup-cid tuple))
          (unless (zerop copy-xmax)
            (let ((page (buf-read target-relfilenode target-block)))
              (multiple-value-bind (offset flags len)
                  (page-get-lp page target-offnum)
                (declare (ignore flags len))
                (page-set-u32 page (+ offset +heap-tuple-xmax-off+)
                              (xid32-of copy-xmax)))
              (with-wal-page-batch
                (buf-write target-relfilenode target-block page)
                (visibilitymap-clear target-relfilenode target-block))
              (mark-dirty target-relfilenode))))))))

(defun vacuum-full-copy-heap (table target-relfilenode oldest-xmin
                              freeze-cutoff)
  "Copy every non-prunable tuple version to compact detached storage."
  (let* ((source-relfilenode (pg-class-relfilenode table))
         (nblocks (buf-nblocks source-relfilenode)))
    (dotimes (block nblocks)
      (check-for-interrupts)
      (let ((page (buf-read source-relfilenode block)))
        (when page
          (loop for offnum from 1 to (page-lp-count page)
                do (multiple-value-bind (off flags len)
                       (page-get-lp page offnum)
                     (declare (ignore off len))
                     (when (= flags +lp-normal+)
                       (vacuum-full-copy-tuple
                        table source-relfilenode target-relfilenode
                        block offnum oldest-xmin freeze-cutoff)))))))
    (buf-nblocks target-relfilenode)))

(defun vacuum-full-freeze-cutoff (table oldest-xmin next-xid)
  (let* ((old-relfrozenxid (or (pg-class-relfrozenxid table)
                               +first-normal-xid+))
         (age (max 0 (- next-xid old-relfrozenxid)))
         (freeze-target (if (>= age +vacuum-aggressive-freeze-age+)
                            next-xid
                            (max +first-normal-xid+
                                 (- next-xid +vacuum-freeze-min-age+)))))
    (min oldest-xmin freeze-target)))

(defun vacuum-full-reserve-storage (table indexes new-table new-indexes)
  "Reserve detached relfilenodes durably while the relation gates are held."
  (with-write-catalog-locks
    (ensure-current-table table)
    (unless (same-index-object-set-p indexes (table-indexes table))
      (ereport "40001" "relation indexes changed during VACUUM FULL"))
    (setf (pg-class-relfilenode new-table) (allocate-oid))
    (setf (pg-class-indexes new-table)
          (copy-list (pg-class-indexes table)))
    (loop for old-index in indexes
          for new-index in new-indexes
          do (setf (pg-index-relfilenode new-index) (allocate-oid)))
    (save-catalog))
  (values))

(defun vacuum-full-publish (db table indexes new-table new-indexes)
  "Atomically publish replacement metadata and defer old-file deletion."
  (let ((old-relfilenodes
          (cons (pg-class-relfilenode table)
                (mapcar #'pg-index-relfilenode indexes)))
        (published nil))
    (with-write-catalog-locks
      (ensure-current-table table)
      (unless (same-index-object-set-p indexes (table-indexes table))
        (ereport "40001" "relation indexes changed during VACUUM FULL"))
      (let ((old-flush-generation (wal-catalog-flush-generation)))
        (setf (gethash (pg-class-name new-table) (database-tables db))
              new-table)
        (dolist (new-index new-indexes)
          (setf (gethash (pg-index-name new-index) (database-indexes db))
                new-index))
        (handler-case
            (progn
              (save-catalog :deferred-relations old-relfilenodes)
              (setf published t))
          (error (condition)
            (if (> (wal-catalog-flush-generation) old-flush-generation)
                (progn
                  ;; The catalog WAL record is durable.  Keep the replacement
                  ;; metadata and storage even if applying it failed locally.
                  (setf published t)
                  (ignore-errors (catalog-refresh-oldest-relfrozenxid))
                  (dolist (relfilenode old-relfilenodes)
                    (ignore-errors (smgr-forget relfilenode :delete t)))
                  published)
                (progn
                  ;; No durable catalog record owns the replacement files.
                  (setf (gethash (pg-class-name table) (database-tables db))
                        table)
                  (dolist (index indexes)
                    (setf (gethash (pg-index-name index)
                                   (database-indexes db))
                          index))
                  (error condition)))))))
    published))

(defun vacuum-full-table (table)
  "Rewrite TABLE and all its indexes under exclusive relation build gates."
  (let ((db (with-write-lock
              (ensure-current-table table)
              (current-database))))
    (call-with-table-rewrite-locks
     table
     (lambda (indexes)
       (let* ((*current-snapshot* (refresh-statement-snapshot))
              ;; Keep FULL's original registered snapshot in the removal
              ;; horizon. PostgreSQL does not set PROC_IN_VACUUM for FULL,
              ;; since index expressions may consult other relations.
              (oldest-xmin (oldest-xmin))
              (freeze-cutoff
                (vacuum-full-freeze-cutoff table oldest-xmin (next-full-xid)))
              (new-table (copy-pg-class table))
              (new-indexes (mapcar #'copy-pg-index indexes))
              (new-relfilenodes nil)
              (published nil))
         (setf (pg-class-indexes new-table)
               (copy-list (pg-class-indexes table)))
         (handler-case
             (progn
               (vacuum-full-reserve-storage
                table indexes new-table new-indexes)
               (setf new-relfilenodes
                     (cons (pg-class-relfilenode new-table)
                           (mapcar #'pg-index-relfilenode new-indexes)))
               (let* ((new-heap-relfilenode
                        (pg-class-relfilenode new-table))
                      (nblocks
                        (vacuum-full-copy-heap
                         table new-heap-relfilenode oldest-xmin freeze-cutoff)))
                 (heap-vacuum-update-visibility-map
                  new-heap-relfilenode nblocks oldest-xmin freeze-cutoff)
                 (loop for old-index in indexes
                       for new-index in new-indexes
                       for was-valid = (pg-index-valid-p old-index)
                       do (btree-init (pg-index-relfilenode new-index))
                          (build-index-entries
                           new-index new-table
                           :unique (and was-valid
                                        (pg-index-unique-p old-index))
                           :name (pg-index-name old-index))
                          (setf (pg-index-valid-p new-index) was-valid))
                 (wal-commit-maintenance)
                 (dolist (new-index new-indexes)
                   (fsync-index new-index))
                 (smgr-fsync new-heap-relfilenode)
                 (smgr-fsync new-heap-relfilenode :visibility)
                 (setf (pg-class-relfrozenxid new-table)
                       (max (or (pg-class-relfrozenxid table)
                                +first-normal-xid+)
                            freeze-cutoff)))
               (setf published
                     (vacuum-full-publish
                      db table indexes new-table new-indexes)))
           (error (condition)
             (unless published
               (when new-relfilenodes
                 (wal-discard-relation-pages new-relfilenodes)
                 (dolist (relfilenode new-relfilenodes)
                   (smgr-forget relfilenode :delete t))))
             (error condition))))))))

(defun vacuum-all-relations-for-xid-wraparound
    (boundary &key fsync-function after-page-function)
  "Freeze each catalog relation to BOUNDARY, skipping all-frozen VM pages."
  (maphash
   (lambda (database-name database)
     (declare (ignore database-name))
     (maphash
      (lambda (table-name table)
        (declare (ignore table-name))
        (let ((index-provider
                (lambda ()
                  (loop for oid in (pg-class-indexes table)
                        for index = (gethash oid (database-indexes database))
                        when index collect (pg-index-relfilenode index))))
              (verify-function
                (lambda ()
                  (unless (eq table
                              (gethash (pg-class-name table)
                                       (database-tables database)))
                    (ereport "42P01" "relation \"~a\" does not exist"
                             (pg-class-name table))))))
          (vacuum-catalog-relation
           table index-provider verify-function
           :forced-oldest-xmin boundary
           :forced-freeze-cutoff boundary
           :after-page-function after-page-function
           :fsync-function fsync-function)))
      (database-tables database)))
   (catalog-databases *catalog*))
  (values))
