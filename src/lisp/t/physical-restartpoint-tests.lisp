;;;; Restartpoint generations bind durable data to a validated receive suffix.
(in-package #:pg-physical-tests)

(defun call-with-restartpoint-fixture (function)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (pg::*physical-recovery-state*
             (list :system-id 123 :timeline 1 :manifest-digest 77 :end 32 :checkpoint 32
                   :cluster-identity "history-source" :required-applied 96 :observed-next-xid 50))
           (pg::*last-replay-lsn* 96) (pg::*next-xid* 50) (pg::*xid-batch-end* 50)
           (pg::*wal-checkpoint-lsn* 32) (pg::*catalog* nil) (pg::*clog-stream* nil)
           (pg::*relation-streams* (make-hash-table :test #'equal))
           (pg::*physical-replay-seen-relfilenodes* (make-hash-table :test #'eql))
           (spool (merge-pathnames "spool/" root)))
      (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
      (pg::wal-ensure-directory-durable (merge-pathnames "global/" root))
      (pg::wal-ensure-directory-durable spool)
      (setf (gethash 900 pg::*physical-replay-seen-relfilenodes*) t)
      (multiple-value-bind (deletes bytes) (pg::wal-decode-catalog-payload
                                            (history-catalog-payload '(900) nil))
        (declare (ignore deletes))
        (pg::wal-atomic-write-bytes (pg::catalog-path) bytes))
      (pg::wal-atomic-write-bytes (merge-pathnames "base/900" root)
                                  (make-array 32 :element-type '(unsigned-byte 8) :initial-element 0))
      (let ((store (pg::physical-store-open spool 123 1 :create t :start-lsn 32)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-input store 64
                                         (legacy-checkpoint-record 64))
               (pg::physical-store-flush store)
               (funcall function root store))
          (pg::physical-store-close store))))))

(defmacro with-restartpoint-fixture ((root store) &body body)
  `(call-with-restartpoint-fixture (lambda (,root ,store) ,@body)))

(deftest restartpoint-generation-roundtrip-and-suffix-start
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (multiple-value-bind (metadata bytes)
          (pg::physical-restartpoint-read pg::*physical-recovery-state*)
        (ok (= 96 (getf metadata :lsn)))
        (ok (= 32 (getf metadata :base)))
        (ok (equal '(("base/900" 32)) (getf metadata :forks)))
        (ok (equal '(900) (getf metadata :seen)))
        (ok (equalp bytes (pg::recovery-read-bytes (pg::catalog-path)))))
      (setf pg::*last-replay-lsn* 32)
      (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 96 pg::*wal-checkpoint-lsn*))
      (ok (gethash 900 pg::*physical-replay-seen-relfilenodes*))))

(deftest restartpoint-new-pointer-with-old-floor-is-valid
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-store-test-hook*
              (lambda (point ignored)
                (declare (ignore ignored))
                (when (eq point :after-restartpoint-publication) (error "simulated stop")))))
        (ok (intake-fails-p
             (lambda () (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)))))
      (ok (= 32 (pg::physical-store-floor-lsn store)))
      (pg::physical-store-close store)
      (let ((reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1)))
        (unwind-protect
             (ok (= 96 (pg::physical-restartpoint-prepare-storage reopened)))
          (pg::physical-store-close reopened)))))

(deftest restartpoint-nonlocal-publication-exit-fences-recovery
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store)
            (pg::*physical-recovery-failed-p* nil)
            (pg::*physical-restartpoint-test-hook*
              (lambda (point)
                (when (eq point :after-generation) (throw 'interrupted :interrupted)))))
        (ok (eq :interrupted (catch 'interrupted (pg::physical-recovery-restartpoint))))
        (ok pg::*physical-recovery-failed-p*)
        (ok (not (probe-file (pg::physical-restartpoint-pointer-path))))
        (ok (= 32 (pg::physical-store-floor-lsn store)))
        (ok (intake-fails-p (lambda () (pg::check-recovery-available)))))))

(deftest restartpoint-damaged-surviving-fork-fails-before-catalog-restore
    (dolist (damage '(:missing :truncated))
      (with-restartpoint-fixture (root store)
        (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
        (let ((path (merge-pathnames "base/900" root))
              (disk-bytes (pg::string->bytes "damaged catalog must remain unchanged")))
          (ecase damage
            (:missing (delete-file path))
            (:truncated (pg::wal-atomic-write-bytes path #(0))))
          (pg::wal-atomic-write-bytes (pg::catalog-path) disk-bytes)
          (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
          (ok (equalp disk-bytes (pg::recovery-read-bytes (pg::catalog-path))))))))

(deftest restartpoint-complete-post-snapshot-drop-exempts-missing-fork
    (with-restartpoint-fixture (root store)
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (pg::physical-store-input store 96 (sample-physical-record 96))
      (let* ((raw (pg::wal-encode-record pg::+wal-record-catalog+ 0 128
                                         (history-catalog-payload nil '(900))))
             (finish (+ 128 (length raw))))
        (pg::physical-store-input store 128 raw)
        (pg::physical-store-flush store)
        (setf (getf pg::*physical-recovery-state* :required-applied) finish)
        (delete-file (merge-pathnames "base/900" root))
        (ok (= 96 (pg::physical-restartpoint-prepare-storage store))))))

(deftest restartpoint-pointer-corruption-never-falls-back
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (let ((pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path))))
        (setf (aref pointer 16) (logxor 1 (aref pointer 16)))
        (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer))
      (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
      (delete-file (pg::physical-restartpoint-pointer-path))
      (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))))

(deftest restartpoint-missing-authoritative-generation-cannot-fall-back
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (let* ((pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path)))
             (path (pg::physical-restartpoint-generation-path
                    (pg::wal-u64 pointer 8) (pg::wal-u64 pointer 16))))
        (delete-file path)
        (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store)))))))

(deftest restartpoint-metadata-must-cover-live-forks-and-history
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((bytes (pg::recovery-read-bytes (pg::catalog-path)))
             (metadata (pg::physical-restartpoint-metadata
                        (pg::physical-store-prefix-digest store 96) bytes)))
        (dolist (key '(:forks :seen))
          (let ((bad (copy-list metadata)))
            (setf (getf bad key) nil)
            (ok (intake-fails-p
                 (lambda () (pg::physical-restartpoint-check-metadata
                             bad bytes pg::*physical-recovery-state*))))))
        (let ((image (pg::physical-restartpoint-encode metadata bytes)))
          (setf (aref image (1- (length image))) (logxor 1 (aref image (1- (length image)))))
          (ok (intake-fails-p (lambda () (pg::physical-restartpoint-decode image))))))))

(deftest restartpoint-startup-progress-cannot-lower-required-boundary
    (with-restartpoint-fixture (root store)
      (declare (ignore root store))
      (setf (getf pg::*physical-recovery-state* :required-applied) 256
            (getf pg::*physical-recovery-state* :observed-next-xid) 100)
      (pg::physical-recovery-save-progress 96 50)
      (let ((control (pg::physical-recovery-read-control pg::*pgdata*)))
        (ok (= 256 (getf control :applied)))
        (ok (= 100 (getf control :next-xid))))))

(deftest restartpoint-retained-suffix-keeps-receive-and-apply-distinct
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (pg::physical-store-input store 96 (sample-physical-record 96))
      (pg::physical-store-input
       store 128 (pg::wal-encode-record pg::+wal-record-commit+ 42 128
                                        (pg::wal-encode-commit-timestamp 1)))
      (pg::physical-store-input store 168 (sample-physical-record 168))
      (pg::physical-store-flush store)
      (setf (getf pg::*physical-recovery-state* :required-applied) 168)
      (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 200 (pg::physical-store-flushed-lsn store)))
      (setf (getf pg::*physical-recovery-state* :required-applied) 169)
      (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))))

(deftest physical-store-v2-control-migrates-without-changing-bootstrap
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store))
          (pg::physical-store-close store)))
      (let* ((path (merge-pathnames "receive.control" root))
             (v3 (pg::recovery-read-bytes path))
             (v2 (make-array 52 :element-type '(unsigned-byte 8) :initial-element 0)))
        (replace v2 v3 :end2 48)
        (setf (aref v2 7) 50)
        (pg::wal-set-u32 v2 48 (pg::wal-crc32 v2))
        (pg::wal-atomic-write-bytes path v2))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 0 (pg::physical-store-base-lsn store) (pg::physical-store-floor-lsn store)))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (ok (= 68 (length (pg::recovery-read-bytes (merge-pathnames "receive.control" root))))))
          (pg::physical-store-close store)))))

(cl-spec:defproperty restartpoint-scheduling-is-translation-invariant
    ((floor (range integer 0 4611686018427387903))
     (offset (range integer 0 4611686018427387903))
     (progress (range integer 0 1000000))
     (elapsed (range integer 0 600))
     (budget (range integer 1 1000000))
     (timeout (range integer 1 600)))
  (:about pg::physical-restartpoint-due-p)
  (:trials (:normal 200))
  (let* ((now (* elapsed internal-time-units-per-second))
         (due (pg::physical-restartpoint-due-p (+ floor progress) floor now 0 timeout budget))
         (shifted (pg::physical-restartpoint-due-p
                   (+ floor offset progress) (+ floor offset) now 0 timeout budget)))
    (and (eql due shifted)
         (or (not due)
             (pg::physical-restartpoint-due-p
              (+ floor progress budget) floor now 0 timeout budget))
         (not (pg::physical-restartpoint-due-p floor floor now 0 timeout budget)))))

(deftest generated-restartpoint-schedule
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'restartpoint-scheduling-is-translation-invariant :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest restartpoint-scheduler-uses-applied-progress-and-time
    (let ((second internal-time-units-per-second))
      (ok (not (pg::physical-restartpoint-due-p 32 32 (* 100 second) 0 1 64)))
      (ok (not (pg::physical-restartpoint-due-p 64 32 second 0 300 64)))
      (ok (pg::physical-restartpoint-due-p 96 32 second 0 300 64))
      (ok (pg::physical-restartpoint-due-p 64 32 (* 300 second) 0 300 64))
      (ok (not (pg::physical-restartpoint-due-p 64 32 (* 299 second) 0 300 64)))))

(deftest restartpoint-scheduler-ignores-unapplied-receive-backlog
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store)
            (pg::*last-replay-lsn* 32)
            (pg::*physical-restartpoint-last-time* 0)
            (pg::*physical-restartpoint-timeout* 0)
            (pg::*physical-restartpoint-wal-bytes* 1))
        (ok (not (pg::physical-restartpoint-needed-p)))
        (ok (eq :not-due (pg::physical-recovery-restartpoint :automatic t)))
        (ok (= 32 (pg::physical-store-floor-lsn store)))
        (setf pg::*last-replay-lsn* 96)
        (ok (= 96 (pg::physical-recovery-restartpoint :automatic t)))
        (ok (= 96 (pg::physical-store-floor-lsn store)))
        (ok (not (pg::physical-restartpoint-needed-p))))))

(deftest recovery-checkpoint-runs-within-sql-admission
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store)
            (pg::*backend* (pg::make-backend :state :in-progress)))
        (ok (null (recovery-state-of
                   (lambda () (pg::recovery-check-statement '(:type :checkpoint))))))
        (pg::with-live-operation
          (ok (= 96 (pg::physical-recovery-checkpoint))))
        (ok (= 96 (pg::physical-store-floor-lsn store))))))

(deftest recovery-checkpoint-core-rejects-unadmitted-callers
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store))
        (ok (intake-fails-p (lambda () (pg::physical-recovery-restartpoint-core))))
        (ok (= 32 (pg::physical-store-floor-lsn store))))))

(deftest recovery-checkpoint-static-base-is-unchanged
    (let ((pg::*physical-recovery-state* (sample-recovery-manifest))
          (pg::*physical-receive-store* nil))
      (pg::with-live-operation
        (ok (eq :unchanged (pg::physical-recovery-checkpoint))))))
