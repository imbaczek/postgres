(in-package #:pg-physical-tests)

(defun call-with-adoption-clean-primary-fixture (function)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (pg::*catalog* (pg::make-catalog :cluster-identity "adoption-primary"))
           (snapshot (sample-checkpoint-snapshot :oldest-active-xid 42))
           (metadata (pg::make-wal-control-metadata
                      (pg::catalog-system-identifier) pg::+db-shutdowned+ 100))
           (control (pg::wal-encode-control-image 152 152 snapshot :metadata metadata)))
      (sb-posix:chmod (namestring root) #o700)
      (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*))
            (pg::make-database :name "postgres" :oid 5))
      (ensure-directories-exist (pg::catalog-path))
      (pg::wal-atomic-write-bytes (pg::catalog-path) (pg::catalog-image-bytes))
      (pg::wal-atomic-write-bytes (merge-pathnames ".pg_lisp.lock" root)
                                  (make-array 0 :element-type '(unsigned-byte 8)))
      (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                  (pg::string->bytes (format nil "16~%")))
      (pg::wal-atomic-write-bytes (pg::control-path) (pg::string->bytes (format nil "1003~%")))
      (pg::wal-atomic-write-bytes (pg::wal-control-path) control)
      (pg::wal-ensure-directory-durable (pg::wal-directory))
      (pg::wal-atomic-write-bytes
       (pg::wal-segment-path 0 1 :legacy)
       (concatenate '(vector (unsigned-byte 8))
                    (make-array 32 :element-type '(unsigned-byte 8) :initial-element 0)
                    (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 32
                                           (pg::wal-encode-checkpoint-snapshot snapshot))))
      (funcall function root snapshot metadata))))

(deftest adoption-clean-inspection-preserves-actual-xid-and-allocation-capacity
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore snapshot metadata))
       (let ((before (mapcar #'lineage-test-bytes
                             (list (pg::catalog-path) (pg::control-path) (pg::wal-control-path)
                                   (pg::wal-segment-path 0 1 :legacy)))))
         (pg::call-with-physical-adoption-owner
          root
          (lambda (owner)
            (let ((state (pg::physical-adoption-inspect-primary owner)))
              (ok (= 152 (getf state :end)))
              (ok (= 32 (getf state :checkpoint-start)))
              (ok (= 42 (getf state :next-xid)))
              (ok (= 1003 (getf state :allocation-next-xid)))
              (ok (eq :legacy (getf state :namespace))))))
         (ok (equalp before (mapcar #'lineage-test-bytes
                                    (list (pg::catalog-path) (pg::control-path) (pg::wal-control-path)
                                          (pg::wal-segment-path 0 1 :legacy)))))))))

(deftest adoption-primary-proof-requires-the-durable-source-origin
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore metadata))
       (let* ((origin (pg::make-physical-export-origin
                       :system-id (pg::catalog-system-identifier) :timeline 1
                       :base 32 :manifest-digest 77 :proven-lsn 32
                       :prefix-digest #xCBF29CE484222325))
              (identity (pg::physical-export-origin-identity origin))
              (path (pg::physical-export-origin-path origin))
              (raw (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 32
                                          (pg::wal-encode-checkpoint-snapshot snapshot))))
         (pg::call-with-physical-adoption-owner
          root
          (lambda (owner)
            (let ((state (pg::physical-adoption-inspect-primary owner)))
              (ok (intake-fails-p
                   (lambda () (pg::physical-adoption-prove-primary-origin state identity))))
              (ok (not (probe-file path)))
              (pg::wal-ensure-directory-durable (pg::physical-export-origin-directory))
              (pg::wal-atomic-write-bytes path (pg::physical-export-origin-encode origin))
              (let ((before (lineage-test-bytes path))
                    (proof (pg::physical-adoption-prove-primary-origin state identity)))
                (ok (= 152 (getf proof :end)))
                (ok (= (pg::physical-store-update-digest #xCBF29CE484222325 raw)
                       (getf proof :end-digest)))
                (ok (equal identity (getf proof :identity)))
                (ok (= 32 (getf proof :proven-lsn)))
                (ok (equalp before (lineage-test-bytes path)))))))))))

(deftest adoption-primary-proof-rejects-foreign-origin-and-future-progress
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore snapshot metadata))
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (let ((state (pg::physical-adoption-inspect-primary owner)))
            (dolist (change '(:system :timeline :future))
              (let ((origin (pg::make-physical-export-origin
                             :system-id (pg::catalog-system-identifier) :timeline 1
                             :base 32 :manifest-digest 77 :proven-lsn 32
                             :prefix-digest #xCBF29CE484222325)))
                (case change
                  (:system (incf (pg::physical-export-origin-system-id origin)))
                  (:timeline (setf (pg::physical-export-origin-timeline origin) 2))
                  (:future (setf (pg::physical-export-origin-proven-lsn origin) 184)))
                (let ((path (pg::physical-export-origin-path origin)))
                  (pg::wal-ensure-directory-durable (pg::physical-export-origin-directory))
                  (pg::wal-atomic-write-bytes path (pg::physical-export-origin-encode origin))
                  (let ((before (lineage-test-bytes path)))
                    (ok (intake-fails-p
                         (lambda ()
                           (pg::physical-adoption-prove-primary-origin
                            state (pg::physical-export-origin-identity origin)))))
                    (ok (equalp before (lineage-test-bytes path)))))))))))))

(deftest adoption-clean-inspection-refuses-dirty-future-and-mismatched-checkpoints
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (let ((path (pg::wal-control-path)))
         (dolist (change '(:dirty :future :foreign :snapshot :active))
           (let ((copy (copy-tree metadata))
                 (checkpoint (copy-tree snapshot))
                 (end 152))
             (case change
               (:dirty (setf (getf copy :db-state) pg::+db-in-production+))
               (:future (setf end 184))
               (:foreign (incf (getf copy :system-id)))
               (:snapshot (incf (getf checkpoint :next-xid))
                (setf (getf checkpoint :oldest-active-xid) (getf checkpoint :next-xid)))
               (:active (setf (getf checkpoint :oldest-active-xid) 17)))
             (pg::wal-atomic-write-bytes
              path (pg::wal-encode-control-image 152 end checkpoint :metadata copy))
             (let ((before (lineage-test-bytes path)))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::call-with-physical-adoption-owner root #'pg::physical-adoption-inspect-primary))))
               (ok (equalp before (lineage-test-bytes path))))))))))

(deftest adoption-offline-owner-never-creates-or-repairs-ownership
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (let ((lock (merge-pathnames ".pg_lisp.lock" root)))
        (ok (intake-fails-p
             (lambda () (pg::call-with-physical-adoption-owner root #'identity))))
        (ok (not (probe-file lock)))
        (pg::wal-atomic-write-bytes lock (make-array 0 :element-type '(unsigned-byte 8)))
        (sb-posix:chmod (namestring lock) #o640)
        (let ((escaped nil))
          (pg::call-with-physical-adoption-owner
           root
           (lambda (owner)
             (setf escaped owner)
             (ok (eq owner (pg::physical-adoption-check-owner owner)))
             (ok (intake-fails-p
                  (lambda () (pg::call-with-physical-adoption-owner root #'identity))))))
          (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner escaped)))))
        (ok (= #o640 (logand #o777 (sb-posix:stat-mode (sb-posix:stat lock)))))
        (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))))

(deftest adoption-offline-owner-releases-on-interrupt-and-nonlocal-exit
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (pg::wal-atomic-write-bytes
       (merge-pathnames ".pg_lisp.lock" root) (make-array 0 :element-type '(unsigned-byte 8)))
      (ok (eq :left (catch 'leave-adoption
                      (pg::call-with-physical-adoption-owner
                       root (lambda (owner) (declare (ignore owner))
                              (throw 'leave-adoption :left))))))
      (ok (intake-fails-p
           (lambda ()
             (pg::call-with-physical-adoption-owner
              root (lambda (owner)
                     (declare (ignore owner))
                     (sb-thread:interrupt-thread
                      sb-thread:*current-thread* (lambda () (error "offline owner interrupt"))))))))
      (ok (pg::call-with-physical-adoption-owner
           root (lambda (owner) (eq owner (pg::physical-adoption-check-owner owner)))))))

(deftest adoption-offline-owner-refuses-pid-files-and-replaced-locks
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (let* ((lock (merge-pathnames ".pg_lisp.lock" root))
             (pid (merge-pathnames "postmaster.pid" root))
             (empty (make-array 0 :element-type '(unsigned-byte 8))))
        (pg::wal-atomic-write-bytes lock empty)
        (pg::wal-atomic-write-bytes pid (pg::string->bytes "123"))
        (ok (intake-fails-p
             (lambda () (pg::call-with-physical-adoption-owner root #'identity))))
        (ok (equalp (pg::string->bytes "123") (lineage-test-bytes pid)))
        (delete-file pid)
        (pg::call-with-physical-adoption-owner
         root
         (lambda (owner)
           (pg::wal-atomic-write-bytes lock empty)
           (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner)))))))))

(deftest adoption-offline-owner-refuses-symlink-and-hardlinked-locks
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (let ((lock (merge-pathnames ".pg_lisp.lock" root))
            (outside (merge-pathnames "other" root))
            (empty (make-array 0 :element-type '(unsigned-byte 8))))
        (pg::wal-atomic-write-bytes outside empty)
        (sb-posix:symlink (namestring outside) (namestring lock))
        (ok (intake-fails-p
             (lambda () (pg::call-with-physical-adoption-owner root #'identity))))
        (sb-posix:unlink (namestring lock))
        (sb-posix:link (namestring outside) (namestring lock))
        (ok (intake-fails-p
             (lambda () (pg::call-with-physical-adoption-owner root #'identity))))
        (ok (equalp empty (lineage-test-bytes outside))))))

(deftest adoption-origin-suffix-uses-stored-commitment-and-only-required-wal
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (history (pg::timeline-parse-history 1 ""))
             (segment pg::+wal-segment-size+)
             (raw (lineage-test-record segment 33))
             (origin (pg::make-physical-export-origin
                      :system-id 9 :timeline 1 :base 32 :manifest-digest 7
                      :proven-lsn segment :prefix-digest 1234))
             (path (lineage-test-file 1 1 raw))
             (expected (pg::physical-store-update-digest 1234 raw)))
        (ok (= expected (pg::physical-adoption-hash-origin-suffix origin (+ segment 33) history)))
        (ok (not (probe-file (pg::wal-segment-path 0 1 :timeline))))
        (ok (equalp raw (lineage-test-bytes path)))
        (ok (= 1234 (pg::physical-export-origin-prefix-digest origin)))
        (ok (= 1234 (pg::physical-adoption-hash-origin-suffix origin segment history)))
        (ok (intake-fails-p
             (lambda () (pg::physical-adoption-hash-origin-suffix origin (1- segment) history))))
        (ok (intake-fails-p
             (lambda () (pg::physical-adoption-hash-origin-suffix origin (+ segment 32) history))))
        (ok (intake-fails-p
             (lambda () (pg::physical-adoption-hash-origin-suffix
                         origin (+ segment 33) (lineage-test-history segment)))))
        (ok (equalp raw (lineage-test-bytes path))))))

(deftest adoption-origin-suffix-fails-after-a-valid-record-without-publication
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (history (pg::timeline-parse-history 1 ""))
             (good (lineage-test-record 32 33))
             (bad (lineage-test-record 65 33))
             (origin (pg::make-physical-export-origin
                      :system-id 9 :timeline 1 :base 32 :manifest-digest 7
                      :proven-lsn 32 :prefix-digest #xCBF29CE484222325))
             (before (pg::copy-physical-export-origin origin)))
        (setf (aref bad 28) (logxor 1 (aref bad 28)))
        (let* ((path (lineage-test-file 1 0 (make-array 32 :element-type '(unsigned-byte 8)
                                                           :initial-element 0) good bad))
               (bytes (lineage-test-bytes path)))
          (ok (intake-fails-p
               (lambda () (pg::physical-adoption-hash-origin-suffix origin 98 history))))
          (ok (equalp before origin))
          (ok (equalp bytes (lineage-test-bytes path)))))))
