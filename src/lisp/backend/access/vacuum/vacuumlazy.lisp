;;;; vacuumlazy.lisp -- manual lazy heap and index cleanup.

(in-package #:pg)

(defconstant +vacuum-freeze-min-age+ 50000000)
(defconstant +vacuum-aggressive-freeze-age+ 150000000)
(defconstant +vacuum-max-dead-tids+ 8192)
(defconstant +vacuum-max-pages-per-batch+ 8192)

(defun heap-vacuum-scan-page (relfilenode block oldest-xmin freeze-cutoff)
  "Mark removable tuples LP_DEAD and freeze old surviving tuple XIDs."
  (let ((dead-tids '())
        (changed nil)
        (has-dead nil))
    (with-write-lock
      (let ((page (buf-read relfilenode block)))
        (when page
          (loop for offnum from 1 to (page-lp-count page)
                do (multiple-value-bind (off flags len)
                       (page-get-lp page offnum)
                     (case flags
                       (#.+lp-dead+
                        (setf has-dead t)
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
                                (setf changed t has-dead t)
                                (push (cons block offnum) dead-tids))
                              (progn
                                (when (and (>= raw-xmin +first-normal-xid+)
                                           (< xmin freeze-cutoff)
                                           (xid-committed-p xmin))
                                  (page-set-u32
                                   page (+ off +heap-tuple-xmin-off+)
                                   +frozen-xid+)
                                  (setf changed t))
                                (when (and (not (zerop raw-xmax))
                                           (xid-aborted-p xmax))
                                  (page-set-u32
                                   page (+ off +heap-tuple-xmax-off+)
                                   +invalid-xid+)
                                  (setf changed t)))))))))
          (when (or changed has-dead)
            (with-wal-page-batch
              (when changed
                (buf-write relfilenode block page))
              (visibilitymap-clear relfilenode block))))))
    (values (nreverse dead-tids) changed)))

(defun heap-vacuum-remove-dead-tids (relfilenode tids fsm)
  "Make LP_DEAD slots reusable only after every index has removed their TIDs."
  (let ((by-block (make-hash-table :test #'eql)))
    (dolist (tid tids)
      (push (cdr tid) (gethash (car tid) by-block)))
    (maphash
     (lambda (block offsets)
       (check-for-interrupts)
       (with-write-lock
         (let ((page (buf-read relfilenode block)))
           (when page
             (let ((changed nil))
               (dolist (offnum offsets)
                 (when (and (<= 1 offnum (page-lp-count page))
                            (multiple-value-bind (off flags len)
                                (page-get-lp page offnum)
                              (declare (ignore off len))
                              (= flags +lp-dead+)))
                   (page-mark-unused page offnum)
                   (setf changed t)))
               (when changed
                 (page-compact page)
                 (with-wal-page-batch
                   (buf-write relfilenode block page)
                   (visibilitymap-clear relfilenode block))
                 (heap-fsm-record-page fsm block
                                       (heap-page-insert-capacity page))))))))
     by-block))
  (values))

(defun heap-vacuum-finish-batch (relfilenode indexes tids fsm)
  ;; LP_DEAD is durable before index cleanup. Heap slots become reusable only
  ;; after every index's deletion WAL has reached its maintenance terminal.
  (wal-commit-maintenance)
  (when tids
    (dolist (index-relfilenode indexes)
      (btree-vacuum-delete-tids index-relfilenode tids))
    (wal-commit-maintenance)
    (heap-vacuum-remove-dead-tids relfilenode tids fsm)
    (wal-commit-maintenance))
  (values))

(defun heap-vacuum-update-visibility-map
    (relfilenode nblocks oldest-xmin freeze-cutoff)
  "Publish VM bits only after checking each heap page under the DML write lock."
  (let ((pages-in-batch 0)
        (batch-dirty nil))
    (dotimes (block nblocks)
      (check-for-interrupts)
      (unless (= (visibilitymap-status relfilenode block)
                 (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
        (with-write-lock
          (let ((page (buf-read relfilenode block)))
            (when page
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
                                       (page-u32 page
                                                 (+ off +heap-tuple-xmin-off+)))
                                     (raw-xmax
                                       (page-u32 page
                                                 (+ off +heap-tuple-xmax-off+)))
                                     (xmin (full-xid-from-32 raw-xmin))
                                     (xmax (full-xid-from-32 raw-xmax)))
                                (when (and (>= raw-xmin +first-normal-xid+)
                                           (< xmin freeze-cutoff)
                                           (xid-committed-p xmin))
                                  (page-set-u32
                                   page (+ off +heap-tuple-xmin-off+)
                                   +frozen-xid+)
                                  (setf raw-xmin +frozen-xid+
                                        xmin +frozen-xid+
                                        tuple-changed t))
                                (when (and (not (zerop raw-xmax))
                                           (xid-aborted-p xmax))
                                  (page-set-u32
                                   page (+ off +heap-tuple-xmax-off+)
                                   +invalid-xid+)
                                  (setf raw-xmax +invalid-xid+
                                        xmax +invalid-xid+
                                        tuple-changed t))
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
                  (when (or tuple-changed (/= old-status status))
                    (with-wal-page-batch
                      (when tuple-changed
                        (buf-write relfilenode block page))
                      (if (zerop status)
                          (visibilitymap-clear relfilenode block)
                          (visibilitymap-set-status relfilenode block status)))
                    (setf batch-dirty t))))))))
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
  (let* ((fsm (with-write-lock
                (heap-fsm-for-relation relfilenode)))
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
