(in-package #:pg-physical-tests)

(defun export-origin-test-value (&key (base 32) (proven 64) (digest 99))
  (pg::make-physical-export-origin :system-id 123 :timeline 1 :base base
                                   :manifest-digest 456 :proven-lsn proven :prefix-digest digest))

(deftest export-origin-codec-binds-history-and-rejects-corruption
    (let* ((origin (export-origin-test-value))
           (bytes (pg::physical-export-origin-encode origin)))
      (ok (equalp origin (pg::physical-export-origin-decode bytes)))
      (loop for offset below (length bytes)
            do (let ((damaged (copy-seq bytes)))
                 (setf (aref damaged offset) (logxor 1 (aref damaged offset)))
                 (ok (intake-fails-p (lambda () (pg::physical-export-origin-decode damaged))))))
      (loop for size below (length bytes)
            do (ok (intake-fails-p (lambda () (pg::physical-export-origin-decode (subseq bytes 0 size))))))
      (ok (intake-fails-p
           (lambda ()
             (pg::physical-export-origin-decode
              (concatenate '(vector (unsigned-byte 8)) bytes #(10))))))))

(deftest export-origin-codec-rejects-impossible-historical-commitments
    (dolist (origin (list (export-origin-test-value :base 0)
                          (export-origin-test-value :proven 31)
                          (export-origin-test-value :proven 32 :digest 0)))
      (ok (intake-fails-p (lambda () (pg::physical-export-origin-encode origin)))))
  (let ((origin (export-origin-test-value :proven 32 :digest #xCBF29CE484222325)))
    (ok (equalp origin
                (pg::physical-export-origin-decode (pg::physical-export-origin-encode origin))))))

(cl-spec:defproperty export-origin-codec-roundtrips
    ((system-id (range integer 0 18446744073709551615))
     (timeline (range integer 1 4294967295))
     (base (range integer 32 18446744073709551615))
     (advance (range integer 0 18446744073709551615))
     (manifest (range integer 0 18446744073709551615))
     (digest (range integer 0 18446744073709551615)))
  (:about pg::physical-export-origin-encode)
  (:trials (:normal 200))
  (let* ((proven (min 18446744073709551615 (+ base advance)))
         (origin (pg::make-physical-export-origin
                  :system-id system-id :timeline timeline :base base :manifest-digest manifest
                  :proven-lsn proven :prefix-digest (if (= base proven) #xCBF29CE484222325 digest))))
    (equalp origin (pg::physical-export-origin-decode (pg::physical-export-origin-encode origin)))))

(deftest generated-export-origin-codec-roundtrip
    (let ((data (cl-spec:result-data (cl-spec:run-property 'export-origin-codec-roundtrips :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun call-with-export-origin-fixture (function)
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root)
          (pg::*catalog* (pg::make-catalog :cluster-identity "export-origin-fixture"))
          (pg::*backend* (pg::make-backend :pid 999 :thread sb-thread:*current-thread*))
          (pg::*wal-lock* (sb-thread:make-mutex :name "export origin WAL fixture"))
          (pg::*wal-temp-cleanup-domain* nil)
          (pg::*wal-storage-incarnation* (gensym "ORIGIN-FIXTURE-"))
          (pg::*wal-segment-namespace* :legacy)
          (pg::*wal-insert-timeline* 1)
          (pg::*wal-durable-lsn* 64)
          (pg::*wal-poisoned* nil)
          (pg::*physical-recovery-state* nil)
          (pg::*physical-export-origins* (make-hash-table :test #'equal))
          (pg::*physical-export-origin-reservations* (make-hash-table :test #'eq))
          (pg::*physical-export-origin-limit* 2)
          (pg::*physical-export-origin-test-hook* nil)
          (pg::*physical-source-pins* (make-hash-table :test #'equal))
          (pg::*physical-source-senders* (make-hash-table :test #'equal))
          (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
          (pg::*wal-replication-slots* (make-hash-table :test #'equal))
          (pg::*pm-requested-mode* pg::+no-shutdown+))
      (funcall function root))))

(defun export-origin-fixture-value ()
  (let ((origin (export-origin-test-value :proven 32 :digest #xCBF29CE484222325)))
    (setf (pg::physical-export-origin-system-id origin) (pg::catalog-system-identifier))
    origin))

(defun export-origin-fixture-install (token origin)
  (pg::with-live-operation-quiescence
    (let ((pg::*wal-checkpoint-lsn* (pg::physical-export-origin-base origin))
          (pg::*wal-durable-lsn* (pg::physical-export-origin-base origin)))
      (pg::physical-export-origin-note-image
       token (list :system-id (pg::physical-export-origin-system-id origin)
                   :timeline (pg::physical-export-origin-timeline origin)
                   :checkpoint (pg::physical-export-origin-base origin)
                   :end (pg::physical-export-origin-base origin))
       (pg::physical-export-origin-manifest-digest origin))))
  (pg::physical-export-origin-initialize token origin))

(deftest export-origin-capacity-and-cancellation-do-not-own-wal-or-slots
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*physical-export-origin-limit* 1))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token)
            (declare (ignore token))
            (ok (intake-fails-p #'pg::physical-export-origin-reserve))
            (ok (not pg::*wal-poisoned*))))
         (ok (zerop (hash-table-count pg::*physical-export-origin-reservations*)))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-export-origin-reservation
                 (lambda (token) (declare (ignore token)) (error "cancelled export"))))))
         (ok (zerop (hash-table-count pg::*physical-export-origin-reservations*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))
         (ok (zerop (hash-table-count pg::*wal-replication-slots*)))))))

(deftest export-origin-publication-is-durable-before-memory-and-forget-reuses-capacity
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((origin (export-origin-fixture-value))
              (identity (pg::physical-export-origin-identity origin))
              (observed nil)
              (pg::*physical-export-origin-limit* 1)
              (pg::*physical-export-origin-test-hook*
                (lambda (point value)
                  (when (eq point :after-save)
                    (setf observed
                          (and (equalp value (pg::physical-export-origin-read
                                              (pg::physical-export-origin-path value)))
                               (zerop (hash-table-count pg::*physical-export-origins*))))))))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token)
            (export-origin-fixture-install token origin)
            (ok observed)
            (ok (intake-fails-p (lambda () (pg::physical-export-origin-forget identity))))
            (ok (not pg::*wal-poisoned*))))
         (ok (= 1 (length (pg::physical-export-origin-list))))
         (ok (intake-fails-p #'pg::physical-export-origin-reserve))
         (pg::physical-export-origin-forget identity)
         (ok (not (probe-file (pg::physical-export-origin-path origin))))
         (ok (null (pg::physical-export-origin-list)))
         (pg::call-with-physical-export-origin-reservation (lambda (token) (declare (ignore token)) t))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))))))

(deftest export-origin-cold-load-keeps-history-without-old-wal-and-never-resets-progress
    (call-with-export-origin-fixture
     (lambda (root)
       (let* ((origin (export-origin-fixture-value))
              (identity (pg::physical-export-origin-identity origin))
              (advanced (pg::copy-physical-export-origin origin)))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token)
            (export-origin-fixture-install token origin)
            ;; Model a durable proof publication; this fixture owns no WAL files.
            (setf (pg::physical-export-origin-proven-lsn advanced) 4096
                  (pg::physical-export-origin-prefix-digest advanced) 987)
            (let ((pg::*wal-durable-lsn* 4096))
              (pg::with-wal-critical
                (pg::physical-export-origin-save advanced)
                (setf (pg::physical-export-origin-entry-origin
                       (gethash identity pg::*physical-export-origins*)) advanced))
              (ok (= 4096 (pg::physical-export-origin-proven-lsn
                           (pg::physical-export-origin-initialize token origin)))))))
         (let ((pg::*physical-recovery-state* (list :phase 2)))
           (pg::physical-export-origin-load))
         (ok (not (probe-file (merge-pathnames "pg_wal/" root))))
         (ok (= 4096 (pg::physical-export-origin-proven-lsn
                      (first (pg::physical-export-origin-list)))))))))

(deftest export-origin-cold-load-rejects-foreign-files-without-publishing-a-partial-registry
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((origin (export-origin-fixture-value)))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (let ((registry pg::*physical-export-origins*)
               (foreign (pg::copy-physical-export-origin origin)))
           (incf (pg::physical-export-origin-system-id foreign))
           (pg::with-wal-critical (pg::physical-export-origin-save foreign))
           (ok (intake-fails-p #'pg::physical-export-origin-load))
           (ok (eq registry pg::*physical-export-origins*))
           (ok (= 1 (hash-table-count registry))))))))

(deftest export-origin-nonlocal-publication-exit-fences-and-releases-capacity
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((origin (export-origin-fixture-value))
              (pg::*physical-export-origin-test-hook*
                (lambda (point value)
                  (declare (ignore value))
                  (when (eq point :after-save) (throw 'origin-cut :interrupted)))))
         (ok (intake-fails-p
              (lambda ()
                (catch 'origin-cut
                  (pg::call-with-physical-export-origin-reservation
                   (lambda (token) (export-origin-fixture-install token origin)))))))
         (ok pg::*wal-poisoned*)
         (ok (zerop (hash-table-count pg::*physical-export-origin-reservations*)))
         (ok (null (pg::physical-export-origin-list)))
         (ok (equalp origin (pg::physical-export-origin-read (pg::physical-export-origin-path origin))))))))

(deftest export-origin-reservation-acquisition-interrupt-cannot-leak-capacity
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*physical-export-origin-test-hook*
               (lambda (point value)
                 (declare (ignore value))
                 (when (eq point :after-reserve)
                   (sb-thread:interrupt-thread sb-thread:*current-thread*
                                               (lambda () (error "reservation acquisition interrupted")))))))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-export-origin-reservation
                 (lambda (token) (declare (ignore token)) t)))))
         (ok (zerop (hash-table-count pg::*physical-export-origin-reservations*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest export-origin-forget-rejects-recovery-without-changing-durable-history
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((origin (export-origin-fixture-value))
              (identity (pg::physical-export-origin-identity origin)))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (let ((pg::*physical-recovery-state* (list :phase 2)))
           (ok (intake-fails-p (lambda () (pg::physical-export-origin-forget identity)))))
         (ok (not pg::*wal-poisoned*))
         (ok (= 1 (length (pg::physical-export-origin-list))))
         (ok (equalp origin (pg::physical-export-origin-read (pg::physical-export-origin-path origin))))))))

(defun call-with-export-origin-sender (origin function)
  (let* ((name "origin_follow")
         (pin (pg::make-physical-follow-pin
               :name name :system-id (pg::physical-export-origin-system-id origin) :timeline 1
               :manifest-digest (pg::physical-export-origin-manifest-digest origin)
               :consistent-lsn 32 :restart-lsn 32 :confirmed-lsn 32 :prefix-digest 0
               :stage :ready
               :anchor (list :base-timeline 1 :base 32
                             :manifest-digest (pg::physical-export-origin-manifest-digest origin))))
         (owner (pg::make-physical-sender
                 :name name :pin pin :backend pg::*backend*
                 :system-id (pg::physical-export-origin-system-id origin) :timeline 1 :namespace :legacy)))
    (setf (gethash name pg::*physical-source-pins*) pin
          (gethash name pg::*physical-source-senders*) owner
          (gethash pin pg::*wal-snapshot-retention-pins*) 32)
    (unwind-protect (funcall function name owner pin)
      (pg::physical-source-release-sender name owner)
      (remhash name pg::*physical-source-pins*)
      (remhash pin pg::*wal-snapshot-retention-pins*))))

(defun export-origin-test-record (cursor)
  (let ((raw (pg::wal-encode-record pg::+wal-record-maintenance+ 0 cursor #())))
    (values (pg::wal-decode-record raw cursor) raw)))

(defun export-origin-test-capture (name owner endpoint)
  (sb-thread:with-mutex (pg::*wal-lock*)
    (pg::physical-export-origin-start-proof name owner 32 endpoint)))

(defun export-origin-test-publish (proof name owner endpoint)
  (pg::with-wal-publication-fence (begin)
    (pg::physical-export-origin-publish-proof-under-lock proof name owner endpoint #'begin)))

(deftest export-origin-proof-hashes-from-genuine-history-instead-of-carried-seed
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((origin (export-origin-fixture-value))
              (pg::*wal-durable-lsn* 96))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (multiple-value-bind (record-one raw-one) (export-origin-test-record 32)
           (multiple-value-bind (record-two raw-two) (export-origin-test-record 64)
             (let* ((middle (pg::copy-physical-export-origin origin))
                    (middle-digest (pg::physical-store-update-digest #xCBF29CE484222325 raw-one))
                    (expected (pg::physical-store-update-digest middle-digest raw-two)))
               (setf (pg::physical-export-origin-proven-lsn middle) 64
                     (pg::physical-export-origin-prefix-digest middle) middle-digest)
               (pg::with-wal-critical
                 (pg::physical-export-origin-save middle)
                 (setf (pg::physical-export-origin-entry-origin
                        (gethash (pg::physical-export-origin-identity origin) pg::*physical-export-origins*))
                       middle))
               (call-with-export-origin-sender
                origin
                (lambda (name owner pin)
                  (ok (zerop (pg::physical-source-pin-prefix-digest pin)))
                  (let ((proof (export-origin-test-capture name owner 96)))
                    (pg::physical-export-origin-proof-record proof 32 record-one raw-one)
                    (pg::physical-export-origin-proof-record proof 64 record-two raw-two)
                    (let ((pg::*physical-export-origin-test-hook*
                            (lambda (point value)
                              (when (eq point :after-progress-publication)
                                (ok (= 96 (pg::physical-export-origin-proven-lsn
                                           (pg::physical-export-origin-read
                                            (pg::physical-export-origin-path value)))))
                                (ok (= 32 (pg::physical-source-pin-confirmed-lsn pin)))))))
                      (export-origin-test-publish proof name owner 96))
                    (let ((saved (first (pg::physical-export-origin-list))))
                      (ok (= 96 (pg::physical-export-origin-proven-lsn saved)))
                      (ok (= expected (pg::physical-export-origin-prefix-digest saved))))))))))))))

(deftest export-origin-proof-rejects-incomplete-and-stale-generations
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((origin (export-origin-fixture-value))
              (identity (pg::physical-export-origin-identity origin))
              (pg::*wal-durable-lsn* 64))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (call-with-export-origin-sender
          origin
          (lambda (name owner pin)
            (declare (ignore pin))
            (ok (intake-fails-p (lambda () (pg::physical-export-origin-forget identity))))
            (ok (not pg::*wal-poisoned*))
            (let ((proof (export-origin-test-capture name owner 64)))
              (ok (intake-fails-p (lambda () (export-origin-test-publish proof name owner 64))))
              (multiple-value-bind (record raw) (export-origin-test-record 32)
                (pg::physical-export-origin-proof-record proof 32 record raw))
              (let ((pg::*wal-storage-incarnation* (gensym "REPLACED-")))
                (ok (intake-fails-p (lambda () (export-origin-test-publish proof name owner 64)))))
              (setf (gethash identity pg::*physical-export-origins*)
                    (pg::make-physical-export-origin-entry :origin (pg::copy-physical-export-origin origin)))
              (ok (intake-fails-p (lambda () (export-origin-test-publish proof name owner 64))))
              (ok (not pg::*wal-poisoned*))
              (ok (equalp origin (pg::physical-export-origin-read
                                  (pg::physical-export-origin-path origin)))))))))))

(deftest export-origin-authority-cannot-be-created-by-a-follow-pin-or-rebound-image
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((origin (export-origin-fixture-value)))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token)
            (ok (intake-fails-p (lambda () (pg::physical-export-origin-initialize token origin))))
            (ok (null (pg::physical-export-origin-list)))
            (export-origin-fixture-install token origin)
            (let ((changed (pg::copy-physical-export-origin origin)))
              (setf (pg::physical-export-origin-manifest-digest changed) 999)
              (ok (intake-fails-p (lambda () (pg::physical-export-origin-initialize token changed)))))))
         (call-with-export-origin-sender
          origin
          (lambda (name owner pin)
            (let ((saved (pg::physical-follow-pin-anchor pin)))
              (unwind-protect
                   (progn
                     (setf (pg::physical-follow-pin-anchor pin)
                           (list :base-timeline 1 :base 32 :manifest-digest 999))
                     (ok (null (export-origin-test-capture name owner 64))))
                (setf (pg::physical-follow-pin-anchor pin) saved)))
            (clrhash pg::*physical-export-origins*)
            (ok (null (export-origin-test-capture name owner 64)))
            (ok (not pg::*wal-poisoned*))))))))

(deftest export-origin-concurrent-proofs-never-regress-or-repeat-a-durable-publication
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((origin (export-origin-fixture-value)) (pg::*wal-durable-lsn* 96))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (call-with-export-origin-sender
          origin
          (lambda (name owner pin)
            (declare (ignore pin))
            (let ((short (export-origin-test-capture name owner 64))
                  (long (export-origin-test-capture name owner 96))
                  (writes 0))
              (multiple-value-bind (record raw) (export-origin-test-record 32)
                (pg::physical-export-origin-proof-record short 32 record raw)
                (pg::physical-export-origin-proof-record long 32 record raw))
              (multiple-value-bind (record raw) (export-origin-test-record 64)
                (pg::physical-export-origin-proof-record long 64 record raw))
              (let ((pg::*physical-export-origin-test-hook*
                      (lambda (point value) (declare (ignore value))
                        (when (eq point :before-save) (incf writes)))))
                (export-origin-test-publish long name owner 96)
                (export-origin-test-publish short name owner 64)
                (export-origin-test-publish long name owner 96))
              (ok (= 1 writes))
              (ok (= 96 (pg::physical-export-origin-proven-lsn
                         (first (pg::physical-export-origin-list))))))))))))

(deftest export-origin-proof-leaves-unproved-seeds-and-old-timelines-inert
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((origin (export-origin-fixture-value)) (pg::*wal-durable-lsn* 96))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (call-with-export-origin-sender
          origin
          (lambda (name owner pin)
            (setf (pg::physical-source-pin-confirmed-lsn pin) 64
                  (pg::physical-source-pin-restart-lsn pin) 64)
            (pg::with-wal-critical
              (ok (null (pg::physical-export-origin-start-proof name owner 64 96))))
            (setf (pg::physical-source-pin-timeline pin) 2
                  (pg::physical-sender-timeline owner) 2
                  (pg::physical-sender-namespace owner) :timeline)
            (let ((pg::*wal-insert-timeline* 2) (pg::*wal-segment-namespace* :timeline))
              (pg::with-wal-critical
                (ok (null (pg::physical-export-origin-start-proof name owner 64 96)))))
            (ok (= 32 (pg::physical-export-origin-proven-lsn
                       (first (pg::physical-export-origin-list)))))
            (ok (not pg::*wal-poisoned*))))))))

(deftest export-origin-proof-rejects-an-interior-record-floor
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((origin (export-origin-fixture-value))
              (invalid (pg::copy-physical-export-origin origin))
              (pg::*wal-durable-lsn* 64))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (setf (pg::physical-export-origin-proven-lsn invalid) 48
               (pg::physical-export-origin-prefix-digest invalid) 777)
         (pg::with-wal-critical
           (pg::physical-export-origin-save invalid)
           (setf (pg::physical-export-origin-entry-origin
                  (gethash (pg::physical-export-origin-identity origin) pg::*physical-export-origins*))
                 invalid))
         (call-with-export-origin-sender
          origin
          (lambda (name owner pin)
            (declare (ignore pin))
            (let ((proof (export-origin-test-capture name owner 64)))
              (multiple-value-bind (record raw) (export-origin-test-record 32)
                (ok (intake-fails-p
                     (lambda () (pg::physical-export-origin-proof-record proof 32 record raw)))))
              (ok (intake-fails-p (lambda () (export-origin-test-publish proof name owner 64))))
              (ok (not pg::*wal-poisoned*)))))))))

(deftest export-origin-progress-publication-cut-keeps-the-private-floor-retained
    (call-with-export-origin-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((origin (export-origin-fixture-value)) (pg::*wal-durable-lsn* 64))
         (pg::call-with-physical-export-origin-reservation
          (lambda (token) (export-origin-fixture-install token origin)))
         (call-with-export-origin-sender
          origin
          (lambda (name owner pin)
            (let ((proof (export-origin-test-capture name owner 64)))
              (multiple-value-bind (record raw) (export-origin-test-record 32)
                (pg::physical-export-origin-proof-record proof 32 record raw))
              (let ((pg::*physical-export-origin-test-hook*
                      (lambda (point value) (declare (ignore value))
                        (when (eq point :after-save) (throw 'progress-cut :interrupted)))))
                (ok (intake-fails-p
                     (lambda () (catch 'progress-cut (export-origin-test-publish proof name owner 64))))))
              (ok pg::*wal-poisoned*)
              (ok (= 32 (pg::physical-source-pin-confirmed-lsn pin)))
              (ok (= 64 (pg::physical-export-origin-proven-lsn
                         (pg::physical-export-origin-read (pg::physical-export-origin-path origin))))))))))))
