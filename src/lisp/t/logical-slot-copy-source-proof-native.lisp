(in-package #:pg)

(defun logical-source-proof-test-parts (owner context input)
  (let* ((record (logical-slot-copy-install-input-record owner context input))
         (proof (logical-slot-copy-install-input-proof input))
         (certificate (logical-slot-copy-install-input-certificate input))
         (state (logical-slot-copy-committed-proof-state proof)))
    (assert (eq :active (logical-slot-copy-install-input-state-phase record)))
    (assert (= (logical-slot-copy-install-input-state-prefix record)
               (length (getf certificate :slots))))
    (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
      (assert (equal metadata (logical-slot-copy-committed-proof-metadata proof)))
      (assert (equalp prefix (logical-slot-copy-committed-proof-prefix proof)))
      (assert (eql generation (getf certificate :promotion-generation)))
      (let* ((path (physical-restartpoint-generation-path (getf metadata :lsn) generation))
             (receipt (list :metadata (copy-tree metadata)
                            :generation-size (sb-posix:stat-size (slot-state-lstat path))
                            :catalog-size (length catalog)
                            :catalog-digest (logical-slot-copy-import-digest catalog)
                            :next-oid (getf (physical-history-read-catalog catalog) :next-oid)
                            :local-floor (load-next-xid))))
        (logical-slot-copy-install-check-input owner context input)
        (list :installation (copy-seq (logical-slot-copy-install-input-image input))
              :base-proof (logical-slot-copy-read-file
                           context "logical-slot-copy.base-verified" +logical-slot-copy-base-proof-maximum-bytes+)
              :descriptor (logical-slot-copy-descriptor-encode
                           (logical-slot-copy-committed-proof-descriptor proof))
              :evidence (logical-slot-copy-import-evidence-encode
                         (logical-slot-copy-committed-proof-evidence proof))
              :fact (physical-logical-encode-fact (logical-slot-copy-install-input-fact input))
              :restartpoint (logical-slot-copy-source-receipt-image receipt)
              :prefix (copy-seq prefix)
              :child-control (logical-slot-copy-read-file
                              context "global/pg_wal_control" (1+ +wal-control-maximum-bytes+)))))))

(defun logical-source-proof-test-rejected-p (function)
  (handler-case (progn (funcall function) nil) (error () t)))

(defun logical-source-proof-test-check (owner context input)
  (let* ((parts (logical-source-proof-test-parts owner context input))
         (image (logical-slot-copy-source-proof-encode parts))
         (original-read (symbol-function 'physical-restartpoint-read))
         (facts nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-restartpoint-read)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (error "permanent pure checker attempted a forbidden R read")))
           (setf facts (logical-slot-copy-check-source-proof image)))
      (setf (symbol-function 'physical-restartpoint-read) original-read))
    (assert (equal (getf facts :fact) (logical-slot-copy-install-input-fact input)))
    (assert (eq :pending (getf (getf facts :certificate) :phase)))
    (assert (equalp (getf facts :plan) (logical-slot-copy-install-input-plan input)))
    (assert (not (eq (getf facts :prefix) (getf parts :prefix))))
    (dolist (mode '(:fact :journal :child-control :receipt))
      (let ((bad (logical-slot-copy-source-proof-decode image)))
        (ecase mode
          (:fact
           (let ((fact (physical-logical-decode-fact (getf bad :fact))))
             (incf (getf fact :through))
             (setf (getf bad :fact) (physical-logical-encode-fact fact))))
          (:journal
           (let ((journal (logical-slot-copy-install-decode (getf bad :installation))))
             (setf (getf journal :evidence-digest) (logxor 1 (getf journal :evidence-digest))
                   (getf bad :installation) (logical-slot-copy-install-encode journal))))
          (:child-control
           (setf (aref (getf bad :child-control) 0)
                 (logxor 1 (aref (getf bad :child-control) 0))))
          (:receipt
           (let ((receipt (logical-slot-copy-source-receipt-decode (getf bad :restartpoint))))
             (incf (getf receipt :generation-size))
             (setf (getf bad :restartpoint) (logical-slot-copy-source-receipt-image receipt)))))
        (assert (logical-source-proof-test-rejected-p
                 (lambda ()
                   (logical-slot-copy-check-source-proof
                    (logical-slot-copy-source-proof-encode bad)))))))
    (format t "~&SOURCE-PROOF-BINDING-PASS original-fact=~s bytes=~d rows=~d~%"
            (not (null (getf (getf facts :control) :logical-fact)))
            (length image) (length (getf (getf facts :certificate) :slots)))
    (finish-output)))

(defun logical-source-proof-test-run ()
  (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
    (unless (slot-state-lstat (merge-pathnames "logical-slot-copy.install" *pgdata*))
      (assert (eq :active (logical-slot-copy-install-fresh-active))))
    (call-with-logical-slot-copy-install-cold-input #'logical-source-proof-test-check)
    (assert (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
    (assert (and (null *physical-recovery-state*) (null *physical-receive-store*)
                 (null *catalog*) (null *clog-stream*) (null *wal-output-stream*)
                 (not *wal-ready*) (not *physical-recovery-failed-p*)
                 (zerop (hash-table-count *wal-replication-slots*))))))
