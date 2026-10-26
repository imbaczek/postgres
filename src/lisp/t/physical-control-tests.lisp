;;;; Versioned native recovery state; C DBState semantics, not C disk layout.
(in-package #:pg-physical-tests)

(defun legacy-recovery-control (bytes)
  (let ((legacy (make-array 60 :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace legacy bytes :end1 56 :end2 56)
    (replace legacy #(80 71 82 83 84 65 84 69))
    (pg::wal-set-u32 legacy 56 (pg::wal-crc32 legacy))
    legacy))

(deftest recovery-control-v2-roundtrip-and-legacy-read
    (dolist (phase '(1 2))
      (dolist (state '(2 5))
        (let* ((applied (if (= phase 1) 32 96))
               (bytes (pg::physical-recovery-control-image
                       phase 123 1 456 32 applied 1003 :db-state state :state-time 123456))
               (control (pg::physical-recovery-decode-control bytes)))
          (ok (= 72 (length bytes)))
          (ok (= 3 (getf control :control-version)))
          (let ((v2 (copy-seq bytes)))
            (setf (aref v2 7) 50)
            (pg::wal-set-u32 v2 68 0)
            (pg::wal-set-u32 v2 68 (pg::wal-crc32 v2))
            (let ((old (pg::physical-recovery-decode-control v2)))
              (ok (= 2 (getf old :control-version)))
              (ok (= 1003 (getf old :next-xid)))
              (ok (= state (getf old :db-state)))))
          (ok (= phase (getf control :phase)))
          (ok (= state (getf control :db-state)))
          (ok (= 123456 (getf control :state-time)))
          (ok (= applied (getf control :applied)))
          (let ((legacy (pg::physical-recovery-decode-control (legacy-recovery-control bytes))))
            (ok (= 1 (getf legacy :control-version)))
            (ok (null (getf legacy :db-state)))
            (ok (null (getf legacy :state-time)))
            (ok (= applied (getf legacy :applied))))))))

(deftest recovery-control-v3-rejects-ambiguous-exhaustion-flags
    (dolist (mutation '(hidden-value missing-flag unknown-phase-bit))
      (let ((bytes (pg::physical-recovery-control-image
                    2 123 1 456 32 96 18446744073709551616)))
        (ecase mutation
          (hidden-value (pg::wal-set-u64 bytes 48 3))
          (missing-flag (pg::wal-set-u32 bytes 8 2))
          (unknown-phase-bit (pg::wal-set-u32 bytes 8 #xc0000002)))
        (pg::wal-set-u32 bytes 68 0)
        (pg::wal-set-u32 bytes 68 (pg::wal-crc32 bytes))
        (ok (handler-case (progn (pg::physical-recovery-decode-control bytes) nil)
              (error () t))))))

(deftest recovery-control-rejects-invalid-state-and-corruption
    (dolist (arguments '((0 123 1 0 32 32 1003 :db-state 2)
                         (1 123 1 456 32 32 1003 :db-state 6)
                         (2 123 1 456 32 96 1003 :state-time -9223372036854775809)
                         (2 123 1 456 32 96 1003 :state-time 9223372036854775808)))
      (ok (intake-fails-p (lambda () (apply #'pg::physical-recovery-control-image arguments)))))
  (let ((bytes (pg::physical-recovery-control-image 2 123 1 456 32 96 1003 :state-time 1)))
    (loop for offset below (length bytes)
          do (let ((bad (copy-seq bytes)))
               (setf (aref bad offset) (logxor 1 (aref bad offset)))
               (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bad))))))
    ;; Even a CRC-valid unsupported primary state is not a recovery control.
    (pg::wal-set-u32 bytes 56 6)
    (pg::wal-set-u32 bytes 68 0)
    (pg::wal-set-u32 bytes 68 (pg::wal-crc32 bytes))
    (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bytes))))))

(cl-spec:defproperty recovery-control-roundtrip-preserves-clean-and-active-state
    ((identifier (range integer 0 18446744073709551615))
     (position (range integer 0 18446744073709551615))
     (time (range integer -9223372036854775808 9223372036854775807))
     (state-bit (range integer 0 1)))
  (:about pg::physical-recovery-decode-control)
  (:trials (:normal 200))
  (let* ((applied (+ 32 (mod position 18446744073709551584)))
         (next-xid (+ 3 (mod position 18446744073709551613)))
         (state (if (zerop state-bit) 2 5))
         (bytes (pg::physical-recovery-control-image
                 2 identifier 1 identifier 32 applied next-xid
                 :db-state state :state-time time))
         (decoded (pg::physical-recovery-decode-control bytes)))
    (and (= identifier (getf decoded :system-id) (getf decoded :manifest-digest))
         (= applied (getf decoded :applied))
         (= next-xid (getf decoded :next-xid))
         (= state (getf decoded :db-state))
         (= time (getf decoded :state-time)))))

(deftest generated-recovery-control-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'recovery-control-roundtrip-preserves-clean-and-active-state :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest recovery-control-state-transitions-never-lower-progress
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (state (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                          :checkpoint 32 :required-applied 256 :observed-next-xid 100))
             (pg::*physical-recovery-state* state))
        (pg::physical-recovery-write-control state 2 256 100 :db-state 2)
        (ok (= 2 (getf (pg::physical-recovery-read-control root) :db-state)))
        (pg::physical-recovery-save-progress 128 80)
        (let ((control (pg::physical-recovery-read-control root)))
          (ok (= 5 (getf control :db-state)))
          (ok (= 256 (getf control :applied)))
          (ok (= 100 (getf control :next-xid))))
        (ok (= 5 (getf pg::*physical-recovery-state* :db-state))))))

(deftest recovery-control-returned-phase-matches-durable-transition
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (before (list :phase 1 :system-id 123 :timeline 1 :manifest-digest 456
                           :checkpoint 32 :required-applied 32 :observed-next-xid 100))
             (after (pg::physical-recovery-write-control before 2 96 100)))
        (ok (= 1 (getf before :phase)))
        (ok (null (getf before :db-state)))
        (ok (= 2 (getf after :phase)))
        (ok (= 5 (getf after :db-state)))
        (ok (= 2 (getf (pg::physical-recovery-read-control root) :phase))))))

(deftest recovery-control-cannot-mark-incomplete-reconstruction-clean
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (state (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                          :checkpoint 32 :required-applied 256 :observed-next-xid 100)))
        (pg::physical-recovery-write-control state 2 96 50)
        (let ((before (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
          (ok (intake-fails-p
               (lambda () (pg::physical-recovery-write-control state 2 96 50 :db-state 2))))
          (ok (equalp before
                      (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))))
        (ok (= 5 (getf (pg::physical-recovery-read-control root) :db-state))))))

(deftest recovery-control-time-matches-signed-c-pg-time-t
    (dolist (time '(-9223372036854775808 -1 0 1 9223372036854775807))
      (let* ((bytes (pg::physical-recovery-control-image
                     2 123 1 456 32 96 1003 :state-time time))
             (decoded (pg::physical-recovery-decode-control bytes)))
        (ok (= time (getf decoded :state-time)))
        (when (= time -1)
          (ok (every (lambda (byte) (= byte 255)) (subseq bytes 60 68)))))))
