;; physical-row-replay.lisp
;;
;; Apply a conservative contiguous prefix of row WAL between statements while
;; admission is closed. Idle repeatable-read snapshots and their storage remain
;; in place; batches that cannot be proved safe wait for those readers to end.

(in-package #:pg)

(defun physical-row-replay-reader-horizon ()
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (let ((horizon 0) (registered 0) (covered nil))
      (maphash (lambda (snapshot present)
                 (when present
                   (incf registered)
                   (if (snapshot-source-xids-complete-p snapshot)
                       (push snapshot covered)
                       (setf horizon (max horizon (snapshot-xmax snapshot))))))
               *active-snapshot-set*)
      (when (and (plusp *active-snapshots*) (zerop registered))
        (error "active recovery snapshots lack visibility information"))
      (values horizon (nreverse covered)))))

(defun physical-row-replay-allowed-p (record horizon)
  ;; Stop at the first record outside the certified row replay prefix.
  ;; Empty allocation-reservation batches are allowed; the decoder validates
  ;; that they have the required empty shape.
  (let ((type (wal-record-type record)) (xid (wal-record-xid record)))
    (cond
      ((= type +wal-record-commit+)
       (and (typep xid '(integer 3 18446744073709551615))
            (>= (xid32-of xid) 3) (>= xid horizon)))
      (t (member type (list +wal-record-batch-start+ +wal-record-pages+
                            +wal-record-logical-changes+ +wal-record-xid-reservation+
                            +wal-record-running-xacts+))))))

(defun physical-row-replay-check-covered-commit (record snapshots)
  (when (= (wal-record-type record) +wal-record-commit+)
    (let ((xid (wal-record-xid record)))
      (dolist (snapshot snapshots)
        (unless (and (snapshot-source-xids-complete-p snapshot)
                     (or (>= xid (snapshot-xmax snapshot))
                         (member xid (snapshot-xip snapshot) :test #'=)))
          (error "source COMMIT ~d at LSN ~d violates retained snapshot coverage (xmax ~d)"
                 xid (wal-record-end record) (snapshot-xmax snapshot))))))
  (values))

(defun physical-row-replay-images-allowed-p (images horizon)
  ;; Unknown, maintenance and contributor-free images cannot pass a reader.
  (every (lambda (image)
           (let ((xids (wal-page-image-xids image)))
             (and (eq :row (wal-page-image-provenance image)) xids
                  (every (lambda (xid)
                           (and (typep xid '(integer 3 18446744073709551615))
                                (>= (xid32-of xid) 3) (>= xid horizon)))
                         xids))))
         images))

(defun physical-row-replay-file-blocks (node fork)
  ;; Check the regular fork before extension. An absent visibility fork starts
  ;; at zero blocks; a missing main fork is an error.
  (handler-case
      (multiple-value-bind (stream size)
          (open-bounded-regular-file (relation-file-path node fork) (* +blcksz+ (ash 1 32)))
        (unwind-protect
             (progn
               (unless (zerop (mod size +blcksz+)) (error "unaligned row replay relation"))
               (/ size +blcksz+))
          (close stream)))
    (sb-posix:syscall-error (condition)
      (if (and (eq fork :visibility) (= sb-posix:enoent (sb-posix:syscall-errno condition)))
          0
          (error condition)))))

(defun physical-row-replay-preflight-images (images)
  ;; Validate the whole batch before applying any image: known relation,
  ;; allowed fork, no duplicate pages and only contiguous extension.
  (let ((nodes (wal-relation-filennodes))
        (sizes (make-hash-table :test #'equal))
        (seen (make-hash-table :test #'equal)))
    (dolist (image images)
      (let* ((node (wal-page-image-relfilenode image)) (fork (wal-page-image-fork image))
                                                       (block (wal-page-image-block image)) (key (list node fork))
                                                       (page-key (list node fork block)))
        (unless (and (member node nodes :test #'eql) (member fork '(:main :visibility))
                     (typep block '(unsigned-byte 32)) (not (gethash page-key seen)))
          (error "unknown or duplicate row replay page"))
        (setf (gethash page-key seen) t)
        (unless (gethash (list node :main) sizes)
          (setf (gethash (list node :main) sizes) (physical-row-replay-file-blocks node :main)))
        (multiple-value-bind (size present) (gethash key sizes)
          (unless present (setf size (physical-row-replay-file-blocks node fork)))
          (when (> block size) (error "row replay page skips a relation block"))
          (setf (gethash key sizes) (max size (1+ block)))))))
  (values))

;;; The reader-safe replay path uses the same durable terminal publication as
;;; full replay; prospective per-record membership is not visible before it.

(defun physical-row-replay-publish-terminal (record next-xid reservation source-xids)
  ;; Publish progress only after redo; restart can repeat an unpublished batch.
  (when *physical-replay-test-hook*
    (funcall *physical-replay-test-hook* :after-redo record))
  (physical-replay-publish-progress record next-xid reservation source-xids)
  (wal-clear-page-cache)
  (when *physical-replay-test-hook*
    (funcall *physical-replay-test-hook* :after-progress record)))

(defun physical-row-replay-prefix (horizon &optional covered)
  ;; Publish each safe terminal in order, and defer at the first unsafe batch.
  (with-physical-replay-staging ()
    (let ((next-xid *next-xid*)
          (source-xids (copy-tree (getf *physical-recovery-state* :source-xids)))
          (reservation (copy-list (getf *physical-recovery-state* :source-reservation))))
      (physical-store-map-durable-records
       *physical-receive-store*
       (lambda (record raw)
         (declare (ignore raw))
         (unless (physical-row-replay-allowed-p record horizon)
           (return-from physical-row-replay-prefix :deferred))
         (when *physical-replay-test-hook*
           (funcall *physical-replay-test-hook* :before-record record))
         (setf next-xid (physical-replay-observed-next-xid record next-xid)
               reservation (physical-source-reservation-next reservation record next-xid)
               source-xids (physical-source-xids-next source-xids record next-xid))
         (if (wal-terminal-record-type-p (wal-record-type record))
             (sb-thread:with-mutex (*wal-lock*)
               (physical-row-replay-check-covered-commit record covered)
               (unless (physical-row-replay-images-allowed-p *wal-recovery-page-images* horizon)
                 (return-from physical-row-replay-prefix :deferred))
               (physical-row-replay-preflight-images (reverse *wal-recovery-page-images*))
               (wal-replay-record record)
               (physical-row-replay-publish-terminal record next-xid reservation source-xids))
             (wal-replay-record record)))
       :start-lsn *last-replay-lsn*)))
  *last-replay-lsn*)

(defun physical-replay-with-readers ()
  ;; Run between statements under closed admission, retaining idle snapshots and
  ;; their storage state. Active SQL has already drained from the gate.
  (unless (and (zerop *live-operation-depth*)
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*) (not *swank-listener-enabled-p*)
               *clog-stream* (output-stream-p *clog-stream*))
    (error "row replay requires owned quiescence and writable recovery CLOG"))
  (check-physical-recovery-work)
  (let ((finished nil))
    (unwind-protect
         (multiple-value-bind (horizon covered) (physical-row-replay-reader-horizon)
           (prog1 (physical-row-replay-prefix horizon covered)
             (setf finished t)))
      (unless finished (setf *physical-recovery-failed-p* t)))))
