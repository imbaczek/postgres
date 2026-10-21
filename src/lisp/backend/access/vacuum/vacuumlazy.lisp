;;;; vacuumlazy.lisp -- manual lazy heap and index cleanup.

(in-package #:pg)

(defconstant +vacuum-freeze-min-age+ 50000000)
(defconstant +vacuum-aggressive-freeze-age+ 150000000)
(defconstant +vacuum-max-dead-tids+ 8192)
(defconstant +vacuum-max-pages-per-batch+ 8192)

(defmacro with-vacuum-heap-page ((page source-page relfilenode block) &body body)
  "Bind a private PAGE copy and its unchanged SOURCE-PAGE baseline.

The RELFILENODE and BLOCK variables are captured once for BODY."
  (let ((relfilenode-value (gensym "RELFILENODE-"))
        (block-value (gensym "BLOCK-"))
        (page-value (gensym "PAGE-")))
    `(let* ((,relfilenode-value ,relfilenode)
            (,block-value ,block))
       (let ((,page-value (buf-read ,relfilenode-value ,block-value)))
         (when ,page-value
           (let ((,page ,page-value)
                 (,source-page (copy-seq ,page-value))
                 (,relfilenode ,relfilenode-value)
                 (,block ,block-value))
             ,@body))))))

(defun try-vacuum-page-publication
    (relfilenode block source-page &key page write-page-p
                                     (visibility-status nil
                                                        visibility-status-p))
  "Publish a heap/VM edit only if SOURCE-PAGE is still the current page.

Heap and VM publication share one WAL batch.  The global write lock protects
the compare and publication from concurrent heap writers."
  (with-write-lock
    (let ((current-page (buf-read relfilenode block)))
      (when (and current-page (equalp current-page source-page))
        (with-wal-page-batch
          (when write-page-p
            (buf-write relfilenode block page))
          (if visibility-status-p
              (if (zerop visibility-status)
                  (visibilitymap-clear relfilenode block)
                  (visibilitymap-set-status relfilenode block
                                            visibility-status))
              (visibilitymap-clear relfilenode block)))
        t))))

(defun vacuum-normalized-xmin (raw-xmin xmin freeze-cutoff)
  "Return the frozen XMIN value when its committed XID is past the cutoff."
  (if (and (>= raw-xmin +first-normal-xid+)
           (< xmin freeze-cutoff)
           (xid-committed-p xmin))
      +frozen-xid+
      xmin))

(defun vacuum-normalized-xmax (raw-xmax xmax)
  "Return invalid XMAX for an aborted deleting transaction."
  (if (and (not (zerop raw-xmax))
           (xid-aborted-p xmax))
      +invalid-xid+
      xmax))

(defmacro vacuum-normalize-tuple-xids
    (page offset raw-xmin xmin raw-xmax xmax freeze-cutoff changed-p)
  "Normalize tuple XIDs and their local copies.

All arguments are lexical variables; CHANGED-P is set on an update."
  (let ((normalized-xmin (gensym "NORMALIZED-XMIN-"))
        (normalized-xmax (gensym "NORMALIZED-XMAX-")))
    `(let ((,normalized-xmin
             (vacuum-normalized-xmin ,raw-xmin ,xmin ,freeze-cutoff))
           (,normalized-xmax
             (vacuum-normalized-xmax ,raw-xmax ,xmax)))
       (unless (= ,normalized-xmin ,xmin)
         (page-set-u32 ,page (+ ,offset +heap-tuple-xmin-off+)
                       ,normalized-xmin)
         (setf ,raw-xmin ,normalized-xmin
               ,xmin ,normalized-xmin
               ,changed-p t))
       (unless (= ,normalized-xmax ,xmax)
         (page-set-u32 ,page (+ ,offset +heap-tuple-xmax-off+)
                       ,normalized-xmax)
         (setf ,raw-xmax ,normalized-xmax
               ,xmax ,normalized-xmax
               ,changed-p t)))))

(defun heap-vacuum-scan-page (relfilenode block oldest-xmin freeze-cutoff)
  "Mark removable tuples LP_DEAD and freeze old surviving tuple XIDs.

If a concurrent writer changes the page, discard every proposal and rescan."
  (loop
    (check-for-interrupts)
    (let ((published-result nil))
      (with-vacuum-heap-page (page source-page relfilenode block)
        (let ((dead-tids '())
              (changed nil))
          (loop for offnum from 1 to (page-lp-count page)
                do (multiple-value-bind (off flags len)
                       (page-get-lp page offnum)
                     (case flags
                       (#.+lp-dead+
                        (push (cons block offnum) dead-tids))
                       (#.+lp-normal+
                        (when (< len +size-of-heap-tuple-header+)
                          (ereport "XX001" "heap tuple header is truncated"))
                        (let* ((raw-xmin
                                 (page-u32 page (+ off +heap-tuple-xmin-off+)))
                               (raw-xmax
                                 (page-u32 page (+ off +heap-tuple-xmax-off+)))
                               (xmin (full-xid-from-32 raw-xmin))
                               (xmax (full-xid-from-32 raw-xmax)))
                          (if (tuple-prunable-p xmin xmax oldest-xmin)
                              (progn
                                (page-mark-dead page offnum)
                                (setf changed t)
                                (push (cons block offnum) dead-tids))
                              (vacuum-normalize-tuple-xids
                               page off raw-xmin xmin raw-xmax xmax freeze-cutoff
                               changed)))))))
          (when (or (not changed)
                    (try-vacuum-page-publication
                     relfilenode block source-page
                     :page page :write-page-p t))
            (setf published-result
                  (list (nreverse dead-tids) changed)))))
      (when published-result
        (return (values-list published-result))))))

(defun heap-vacuum-remove-dead-tids (relfilenode tids fsm)
  "Make LP_DEAD slots reusable only after every index has removed their TIDs."
  (let ((by-block (make-hash-table :test #'eql)))
    (dolist (tid tids)
      (push (cdr tid) (gethash (car tid) by-block)))
    (maphash
     (lambda (block offsets)
       (check-for-interrupts)
       (loop
         (check-for-interrupts)
         (let ((finished nil)
               (free-space nil))
           (with-vacuum-heap-page (page source-page relfilenode block)
             (let ((changed nil))
               (dolist (offnum offsets)
                 (when (and (<= 1 offnum (page-lp-count page))
                            (multiple-value-bind (off flags len)
                                (page-get-lp page offnum)
                              (declare (ignore off len))
                              (= flags +lp-dead+)))
                   (page-mark-unused page offnum)
                   (setf changed t)))
               (if changed
                   (progn
                     (page-compact page)
                     (when (try-vacuum-page-publication
                            relfilenode block source-page
                            :page page :write-page-p t)
                       (setf free-space (heap-page-insert-capacity page)
                             finished t)))
                   (setf finished t))))
           (when finished
             ;; Keep FSM synchronization outside WAL/page locks.  A stale FSM
             ;; entry is advisory and heapam-insert rechecks the page.
             (when free-space
               (heap-fsm-record-page fsm block free-space))
             (return)))))
     by-block))
  (values))

(defun heap-vacuum-finish-batch (relfilenode indexes tids fsm)
  ;; LP_DEAD is durable before index cleanup. Heap slots become reusable only
  ;; after every index's deletion WAL has reached its maintenance terminal.
  (wal-commit-maintenance)
  (when tids
    (wal-test-pause :after-vacuum-dead-tids))
  (when tids
    (loop for index-relfilenode in indexes
          for index-number from 0
          do (btree-vacuum-delete-tids index-relfilenode tids)
             (wal-commit-maintenance)
             (when (zerop index-number)
               (wal-test-pause :after-vacuum-index-cleanup)))
    ;; At this boundary every index deletion is durable, but heap line pointers
    ;; are still LP_DEAD and cannot yet be reused by an INSERT.
    (wal-test-pause :after-vacuum-indexes-before-reuse)
    (heap-vacuum-remove-dead-tids relfilenode tids fsm)
    (wal-commit-maintenance))
  (values))

(defun heap-vacuum-update-visibility-map
    (relfilenode nblocks oldest-xmin freeze-cutoff)
  "Freeze eligible tuples and publish VM bits with a per-page compare-and-set."
  (let ((pages-in-batch 0)
        (batch-dirty nil))
    (dotimes (block nblocks)
      (check-for-interrupts)
      (unless (= (visibilitymap-status relfilenode block)
                 (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
        (loop
          with published = nil
          until published
          do (check-for-interrupts)
             (with-vacuum-heap-page (page source-page relfilenode block)
               (let ((visible t)
                     (all-frozen t)
                     (tuple-changed nil))
                 (loop for offnum from 1 to (page-lp-count page)
                       do (multiple-value-bind (off flags len)
                              (page-get-lp page offnum)
                            (case flags
                              (#.+lp-dead+
                               (setf visible nil all-frozen nil))
                              (#.+lp-normal+
                               (when (< len +size-of-heap-tuple-header+)
                                 (ereport "XX001" "heap tuple header is truncated"))
                               (let* ((raw-xmin
                                        (page-u32
                                         page (+ off +heap-tuple-xmin-off+)))
                                      (raw-xmax
                                        (page-u32
                                         page (+ off +heap-tuple-xmax-off+)))
                                      (xmin (full-xid-from-32 raw-xmin))
                                      (xmax (full-xid-from-32 raw-xmax)))
                                 (vacuum-normalize-tuple-xids
                                  page off raw-xmin xmin raw-xmax xmax
                                  freeze-cutoff tuple-changed)
                                 (unless (and (or (= xmin +bootstrap-xid+)
                                                  (= xmin +frozen-xid+)
                                                  (and (>= raw-xmin
                                                           +first-normal-xid+)
                                                       (< xmin oldest-xmin)
                                                       (xid-committed-p xmin)))
                                              (or (zerop raw-xmax)
                                                  (xid-aborted-p xmax)))
                                   (setf visible nil))
                                 (unless (or (= xmin +bootstrap-xid+)
                                             (= xmin +frozen-xid+))
                                   (setf all-frozen nil))
                                 (unless (zerop raw-xmax)
                                   (setf all-frozen nil)))))))
                 (let* ((status (if visible
                                    (if all-frozen
                                        (logior +vm-all-visible-bit+
                                                +vm-all-frozen-bit+)
                                        +vm-all-visible-bit+)
                                    0))
                        (old-status (visibilitymap-status relfilenode block)))
                   (cond
                     ((and (not tuple-changed) (= old-status status))
                      (setf published t))
                     ((try-vacuum-page-publication
                       relfilenode block source-page
                       :page page :write-page-p tuple-changed
                       :visibility-status status)
                      (setf batch-dirty t
                            published t))))))))
      (incf pages-in-batch)
      (when (>= pages-in-batch +vacuum-max-pages-per-batch+)
        (when batch-dirty
          (wal-commit-maintenance))
        (setf pages-in-batch 0
              batch-dirty nil)))
    (when batch-dirty
      (wal-commit-maintenance)))
  (values))

(defun heap-vacuum-relation-locked
    (relfilenode index-relfilenodes oldest-xmin freeze-cutoff
     &key after-page-function)
  "Lazy-vacuum one heap while its heap and index maintenance locks are held."
  (let* ((fsm (heap-fsm-for-relation relfilenode))
         (nblocks (buf-nblocks relfilenode))
         (pending-tids '())
         (pending-count 0)
         (pages-in-batch 0)
         (batch-dirty nil))
    (dotimes (block nblocks)
      (check-for-interrupts)
      (unless (logtest +vm-all-frozen-bit+
                       (visibilitymap-status relfilenode block))
        (multiple-value-bind (tids changed)
            (heap-vacuum-scan-page relfilenode block oldest-xmin freeze-cutoff)
          (when tids
            (setf pending-tids (nconc pending-tids tids)
                  pending-count (+ pending-count (length tids))
                  batch-dirty t))
          (when changed (setf batch-dirty t))))
      (when after-page-function
        (funcall after-page-function relfilenode block))
      (incf pages-in-batch)
      (when (or (>= pending-count +vacuum-max-dead-tids+)
                (>= pages-in-batch +vacuum-max-pages-per-batch+))
        (when batch-dirty
          (heap-vacuum-finish-batch relfilenode index-relfilenodes
                                    pending-tids fsm))
        (setf pending-tids '()
              pending-count 0
              pages-in-batch 0
              batch-dirty nil)))
    (when batch-dirty
      (heap-vacuum-finish-batch relfilenode index-relfilenodes
                                pending-tids fsm))
    (heap-vacuum-update-visibility-map
     relfilenode nblocks oldest-xmin freeze-cutoff))
  (values))

(defun heap-vacuum-relation
    (relfilenode index-relfilenodes oldest-xmin freeze-cutoff
     &key after-page-function)
  "Acquire maintenance locks and lazy-vacuum one heap relation."
  (with-relation-maintenance-locks
      ((cons relfilenode index-relfilenodes))
    (heap-vacuum-relation-locked relfilenode index-relfilenodes
                                 oldest-xmin freeze-cutoff
                                 :after-page-function after-page-function)))
