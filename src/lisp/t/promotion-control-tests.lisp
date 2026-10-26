;;;; Pending promotion control remains recovery state; no child authority is inferred.
(in-package #:pg-physical-tests)

(defun pending-test-control (&rest options)
  (apply #'pg::physical-recovery-control-image 3 123 1 77 32 96 50
         (append options (list :state-time 12345 :promotion-child 2
                               :promotion-generation 987 :promotion-source-namespace :legacy))))

(deftest pending-promotion-control-binds-source-child-and-exact-generation
    (dolist (namespace '(:legacy :timeline))
      (let* ((bytes (pending-test-control :promotion-source-namespace namespace))
             (control (pg::physical-recovery-decode-control bytes)))
        (ok (= 88 (length bytes)))
        (ok (= 4 (getf control :control-version)))
        (ok (= 3 (getf control :phase)))
        (ok (= 1 (getf control :timeline)))
        (ok (= 2 (getf control :promotion-child)))
        (ok (= 987 (getf control :promotion-generation)))
        (ok (eq namespace (getf control :promotion-source-namespace)))
        (ok (= 32 (getf control :checkpoint)))
        (ok (= 96 (getf control :applied)))
        (ok (= 50 (getf control :next-xid)))
        (ok (= pg::+db-in-archive-recovery+ (getf control :db-state)))))
  (let ((ordinary (pg::physical-recovery-control-image 2 123 1 77 32 96 50 :state-time 12345)))
    (ok (= 72 (length ordinary)))
    (ok (= 3 (getf (pg::physical-recovery-decode-control ordinary) :control-version)))))

(deftest pending-control-preserves-exhausted-visibility-without-reserving-child-xids
    (let* ((bytes (pg::physical-recovery-control-image
                   3 123 1 77 32 96 18446744073709551616 :state-time 12345
                   :promotion-child 2 :promotion-generation 0 :promotion-source-namespace :legacy))
           (control (pg::physical-recovery-decode-control bytes)))
      (ok (zerop (pg::wal-u64 bytes 48)))
      (ok (logbitp 31 (pg::wal-u32 bytes 8)))
      (ok (= 18446744073709551616 (getf control :next-xid)))
      (ok (zerop (getf control :promotion-generation)))))

(deftest pending-control-rejects-invalid-schema-and-noncanonical-exhaustion
    (dolist (options '((:promotion-child 1) (:promotion-child 4294967296)
                       (:promotion-generation nil) (:promotion-generation 18446744073709551616)
                       (:promotion-source-namespace nil) (:promotion-source-namespace :guessed)
                       (:db-state 2)))
      (ok (intake-fails-p (lambda () (apply #'pending-test-control options)))))
  (dolist (change '((8 2) (8 1027) (68 1) (72 2)))
    (let ((bytes (pending-test-control)))
      (pg::wal-set-u32 bytes (first change) (second change))
      (pg::wal-set-u32 bytes 84 0)
      (pg::wal-set-u32 bytes 84 (pg::wal-crc32 bytes))
      (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bytes))))))
  (let ((bytes (pending-test-control)))
    (pg::wal-set-u32 bytes 8 #x80000003)
    (pg::wal-set-u32 bytes 84 0)
    (pg::wal-set-u32 bytes 84 (pg::wal-crc32 bytes))
    (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bytes))))))

(deftest pending-control-crc-covers-every-new-identity-and-boundary-field
    (dolist (offset '(8 12 16 24 32 40 48 56 60 68 72 76 83))
      (let ((bytes (pending-test-control)))
        (setf (aref bytes offset) (logxor 1 (aref bytes offset)))
        (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bytes)))))))

(deftest pending-control-cannot-be-rewritten-as-ordinary-progress
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (bytes (pending-test-control))
                                 (state (pg::physical-recovery-decode-control bytes)) (writes 0)
                                 (pg::*physical-recovery-control-test-hook*
                                   (lambda (&rest ignored) (declare (ignore ignored)) (incf writes))))
        (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bytes)
        (dolist (phase '(1 2 3))
          (ok (intake-fails-p (lambda () (pg::physical-recovery-write-control state phase 96 50)))))
        (let ((ordinary (pg::physical-recovery-decode-control
                         (pg::physical-recovery-control-image 2 123 1 77 32 96 50))))
          (ok (intake-fails-p (lambda () (pg::physical-recovery-write-control ordinary 3 96 50))))
          (setf (getf ordinary :promotion-child) 2)
          (ok (intake-fails-p (lambda () (pg::physical-recovery-write-control ordinary 2 96 50)))))
        (ok (zerop writes))
        (ok (equalp bytes (lineage-test-bytes (pg::physical-recovery-control-path)))))))

(deftest pending-control-startup-stops-before-provenance-rewrite-or-storage-mutation
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*physical-recovery-state* nil)
                                 (pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1)
                                 (writes 0)
                                 (pg::*physical-recovery-control-test-hook*
                                   (lambda (&rest ignored) (declare (ignore ignored)) (incf writes)))
                                 (files (list
                                         (list "recovery.required" (pg::string->bytes (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%")))
                                         (list "physical-recovery.control" (pending-test-control))
                                         (list "global/pg_wal_control" (pg::wal-encode-control-image 32 96 nil))
                                         (list "global/pg_control" #(3 4 5))
                                         (list "pg_wal/0000000000000000.wal" #(7 8 9))
                                         (list "global/catalog" #(12 13))
                                         (list "pg_wal/000000020000000000000000.wal" #(19 20)))))
        ;; Deliberately omit a base manifest: the pending guard must run first.
        (dolist (entry files)
          (pg::wal-atomic-write-bytes (merge-pathnames (first entry) root) (second entry)))
        (ok (handler-case (progn (pg::physical-recovery-prepare) nil)
              (error (condition) (search "pending native promotion" (princ-to-string condition)))))
        (ok (zerop writes))
        (ok (null pg::*physical-recovery-state*))
        (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
        (dolist (entry files)
          (ok (equalp (second entry) (lineage-test-bytes (merge-pathnames (first entry) root))))))))

(deftest pending-control-keeps-recovery-exclusion-and-refuses-worker-admission
    (let ((pg::*physical-recovery-state* (pg::physical-recovery-decode-control (pending-test-control)))
          (pg::*physical-recovery-failed-p* nil))
      (ok (pg::recovery-in-progress-p))
      (ok (equal "25006" (recovery-state-of (lambda () (pg::prevent-recovery-write "test")))))
      (ok (equal "57P03" (recovery-state-of #'pg::check-recovery-available)))
      (ok (equal "57P03" (recovery-state-of #'pg::check-physical-recovery-worker-start)))))

(cl-spec:defproperty pending-promotion-identities-roundtrip-without-committing-child
    ((child (range integer 2 4294967295)) (generation (range integer 0 18446744073709551615))
                                          (next (range integer 3 18446744073709551616)) (kind (range integer 0 1)))
  (:about pg::physical-recovery-control-image)
  (:trials (:normal 200))
  (let* ((namespace (nth kind '(:legacy :timeline)))
         (bytes (pg::physical-recovery-control-image
                 3 123 1 77 32 96 next :state-time 12345 :promotion-child child
                 :promotion-generation generation :promotion-source-namespace namespace))
         (control (pg::physical-recovery-decode-control bytes)))
    (and (= 3 (getf control :phase)) (= 1 (getf control :timeline))
         (= child (getf control :promotion-child))
         (= generation (getf control :promotion-generation))
         (= next (getf control :next-xid))
         (eq namespace (getf control :promotion-source-namespace)))))

(deftest generated-pending-promotion-control-identities
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'pending-promotion-identities-roundtrip-without-committing-child :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
