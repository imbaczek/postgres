;;;; vacuum.lisp -- table-level VACUUM horizons and catalog updates.

(in-package #:pg)

(defun current-table-index-relfilenodes (table)
  (mapcar #'pg-index-relfilenode (table-indexes table)))

(defun vacuum-compute-freeze-cutoff
    (old-relfrozenxid oldest-xmin next-xid &key forced-freeze-cutoff)
  "Choose the freeze cutoff from the caller's horizon and age policy."
  (let* ((old-relfrozenxid (or old-relfrozenxid +first-normal-xid+))
         (age (max 0 (- next-xid old-relfrozenxid)))
         (freeze-target
           (or forced-freeze-cutoff
               (if (>= age +vacuum-aggressive-freeze-age+)
                   next-xid
                   (max +first-normal-xid+
                        (- next-xid +vacuum-freeze-min-age+))))))
    (min oldest-xmin freeze-target)))

(defun call-with-revalidated-relation-locks
    (state-provider requested-relations acquire-lock release-lock
     valid-state-p function)
  "Retry FUNCTION until its captured relation state is protected by locks.

STATE-PROVIDER returns catalog state before and after locking.
REQUESTED-RELATIONS derives the initial lock set; VALID-STATE-P verifies that
the current state is covered. Acquired locks are always released, including
when revalidation or FUNCTION signals an error. ACQUIRE-LOCK accepts the
requested relations and an ownership callback, published atomically with gates."
  (loop
    for captured = (funcall state-provider)
    for requested = (funcall requested-relations captured)
    do (with-acquired-relation-locks
           (locked-relations token
                             (lambda (publish) (funcall acquire-lock requested publish))
                             release-lock)
         (let ((current (funcall state-provider)))
           (when (funcall valid-state-p captured current locked-relations)
             (return-from call-with-revalidated-relation-locks
               (funcall function current)))))))

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
            (freeze-cutoff
              (vacuum-compute-freeze-cutoff
               old-relfrozenxid oldest-xmin next-xid
               :forced-freeze-cutoff forced-freeze-cutoff)))
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

(defun vacuum-toast-relation
    (table verify-function
     &key forced-oldest-xmin forced-freeze-cutoff after-page-function fsync-function)
  "Vacuum TABLE's owned TOAST heap and index, advancing its own freeze horizon.
Capture the ownership and relfilenodes under the write lock, then lock the parent,
TOAST heap, and index and revalidate them before work and catalog publication.
The parent lock protects ownership while TOAST is processed, as in vacuum.c's
parent session lock.  A pre-durable catalog save failure restores the old TOAST horizon; the
optional fsync runs after publication.  Forced horizons and callbacks are passed
to the TOAST pass independently of the main relation."
  (let (toast parent-node heap-node index-node)
    (with-write-lock
      (when verify-function (funcall verify-function))
      (setf toast (pg-class-toast table))
      (when toast
        (setf parent-node (pg-class-relfilenode table)
              heap-node (pg-toast-relfilenode toast)
              index-node (pg-toast-index-relfilenode toast))))
    (when toast
      (labels ((verify-ownership ()
                 (when verify-function (funcall verify-function))
                 (unless (and (eq toast (pg-class-toast table))
                              (= parent-node (pg-class-relfilenode table))
                              (= heap-node (pg-toast-relfilenode toast))
                              (= index-node (pg-toast-index-relfilenode toast)))
                   (ereport "55000" "TOAST relation changed while waiting for lock"))))
        (with-relation-maintenance-locks ((list parent-node heap-node index-node))
          (with-write-lock (verify-ownership))
          (let* ((oldest-xmin (or forced-oldest-xmin (oldest-xmin *current-snapshot*)))
                 (old-xid (or (pg-toast-relfrozenxid toast) +first-normal-xid+))
                 (cutoff (vacuum-compute-freeze-cutoff
                          old-xid oldest-xmin (next-full-xid)
                          :forced-freeze-cutoff forced-freeze-cutoff)))
            (heap-vacuum-relation-locked heap-node (list index-node)
                                         oldest-xmin cutoff
                                         :after-page-function after-page-function)
            (with-write-catalog-locks
              (verify-ownership)
              (call-with-catalog-durability-rollback-under-lock
               (lambda () (setf (pg-toast-relfrozenxid toast) old-xid))
               (lambda ()
                 (setf (pg-toast-relfrozenxid toast) (max old-xid cutoff))
                 (save-catalog))))
            (when fsync-function (funcall fsync-function heap-node))
            (pg-toast-relfrozenxid toast)))))))

(defun vacuum-table (table)
  "Run a plain manual VACUUM for TABLE, then its owned TOAST relation.
Each relation computes and publishes its own freeze horizon."
  (let ((verify (lambda () (ensure-current-table table))))
    (prog1 (vacuum-catalog-relation
            table (lambda () (current-table-index-relfilenodes table)) verify)
      (vacuum-toast-relation table verify))))

(defun same-index-object-set-p (left right)
  "Compare index membership and metadata identity without depending on order."
  (and (= (length left) (length right))
       (= (length left) (length (remove-duplicates left :test #'eq)))
       (= (length right) (length (remove-duplicates right :test #'eq)))
       (every (lambda (index)
                (member index right :test #'eq))
              left)))

(defmacro with-current-table-index-set ((table indexes) &body body)
  "Run BODY under catalog locks after validating TABLE and its index objects.

The caller holds the table rewrite gates while the current catalog entry and
its exact index object set are checked and updated. PostgreSQL's C VACUUM FULL
holds an AccessExclusiveLock across the rewrite; this catalog guard keeps this
implementation's replacement metadata transition atomic as well."
  (let ((table-value (gensym "TABLE-"))
        (indexes-value (gensym "INDEXES-")))
    `(let* ((,table-value ,table)
            (,indexes-value ,indexes))
       (with-write-catalog-locks
         (ensure-current-table ,table-value)
         (unless (same-index-object-set-p
                  ,indexes-value (table-indexes ,table-value))
           (ereport "40001" "relation indexes changed during VACUUM FULL"))
         ,@body))))

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
   (lambda (requested publish)
     (acquire-relation-build-locks
      requested (and *backend* (backend-xid *backend*)) t publish))
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
             (xmax (htup-xmax tuple))
             (copy-xmin
               (vacuum-normalized-xmin (xid32-of xmin) xmin freeze-cutoff))
             (copy-xmax
               (vacuum-normalized-xmax (xid32-of xmax) xmax)))
        (multiple-value-bind (target-block target-offnum)
            (heapam-insert target-relfilenode
                           (pg-class-columns source-table)
                           (toast-plan-tuple (htup-values tuple)
                                             (pg-class-columns source-table) :external-p nil)
                           copy-xmin (htup-cid tuple))
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
  (vacuum-compute-freeze-cutoff
   (pg-class-relfrozenxid table) oldest-xmin next-xid))

(defun vacuum-full-reserve-storage (table indexes new-table new-indexes)
  "Reserve detached relfilenodes durably while the relation gates are held."
  (with-current-table-index-set (table indexes)
    (setf (pg-class-relfilenode new-table) (allocate-oid))
    (setf (pg-class-indexes new-table)
          (copy-list (pg-class-indexes table)))
    (loop for old-index in indexes
          for new-index in new-indexes
          do (setf (pg-index-relfilenode new-index) (allocate-oid)))
    (save-catalog))
  (values))

(defun vacuum-full-publish (db table indexes new-table new-indexes
                            &key on-wal-appended)
  "Publish replacement metadata, retaining files once its WAL record is complete."
  (let ((old-relfilenodes
          (cons (pg-class-relfilenode table)
                (mapcar #'pg-index-relfilenode indexes))))
    (with-current-table-index-set (table indexes)
      (let ((old-flush-generation (wal-catalog-flush-generation))
            (wal-appended nil))
        (setf (gethash (pg-class-name new-table) (database-tables db))
              new-table)
        (dolist (new-index new-indexes)
          (setf (gethash (pg-index-name new-index) (database-indexes db))
                new-index))
        (handler-case
            (let ((*wal-vacuum-full-publication* t))
              (save-catalog
               :deferred-relations old-relfilenodes
               :on-record-appended
               (lambda ()
                 (setf wal-appended t)
                 (when on-wal-appended
                   (funcall on-wal-appended)))))
          (error (condition)
            (cond
              ((> (wal-catalog-flush-generation) old-flush-generation)
               ;; Recovery will replay the durable catalog. Retire the old
               ;; files best-effort without replacing the original error.
               (ignore-errors (catalog-refresh-oldest-relfrozenxid))
               (dolist (relfilenode old-relfilenodes)
                 (ignore-errors (smgr-forget relfilenode :delete t)))
               (error condition))
              (t
               (unless wal-appended
                 ;; No complete terminal catalog record can own the
                 ;; replacement files, so restoring the old metadata is safe.
                 (setf (gethash (pg-class-name table) (database-tables db))
                       table)
                 (dolist (index indexes)
                   (setf (gethash (pg-index-name index)
                                  (database-indexes db))
                         index)))
               ;; If WAL append completed, recovery may still replay it. Keep
               ;; replacement metadata and both file sets in that case.
               (error condition)))))))
    (values)))

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
              (keep-replacement-files nil))
         (setf (pg-class-indexes new-table)
               (copy-list (pg-class-indexes table)))
         (with-error-cleanup
             (condition
              (unless keep-replacement-files
                (when new-relfilenodes
                  (ignore-errors
                   (wal-discard-relation-pages new-relfilenodes))
                  (dolist (relfilenode new-relfilenodes)
                    (ignore-errors
                     (smgr-forget relfilenode :delete t))))))
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
           (vacuum-full-publish
            db table indexes new-table new-indexes
            :on-wal-appended
            (lambda () (setf keep-replacement-files t)))))))))

(defun vacuum-all-relations-for-xid-wraparound
    (boundary &key fsync-function after-page-function)
  "Freeze each catalog relation and its owned TOAST relation to BOUNDARY.
Both passes use the forced horizon and callbacks, skipping all-frozen VM pages;
each publishes its own horizon after its heap pass."
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
           :fsync-function fsync-function)
          (vacuum-toast-relation
           table verify-function :forced-oldest-xmin boundary
           :forced-freeze-cutoff boundary :after-page-function after-page-function
           :fsync-function fsync-function)))
      (database-tables database)))
   (catalog-databases *catalog*))
  (values))
