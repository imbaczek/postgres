(in-package #:pg-physical-tests)

(defun backup-lease-authority-error-p (function)
  (handler-case (progn (funcall function) nil)
    (error (condition) (search "backup lease" (princ-to-string condition)))))

(defun call-with-backup-lease-authority-fixture (function)
  (call-with-backup-lease-store-fixture
   (lambda (root)
     (sb-posix:chmod (namestring root) #o700)
     (let* ((snapshot (sample-checkpoint-snapshot :timeline 2 :prev-timeline 1))
            (pg::*wal-checkpoint-snapshot* snapshot)
            (pg::*wal-checkpoint-lsn* 152)
            (pg::*wal-durable-lsn* 152)
            (pg::*wal-end-lsn* 152)
            (pg::*wal-ready* t)
            (pg::*wal-segment-namespace* :timeline)
            (pg::*wal-replication-slots* (make-hash-table :test #'equal))
            (pg::*physical-source-test-hook* nil)
            (pg::*physical-export-origins* (make-hash-table :test #'equal))
            (pg::*physical-export-origin-reservations* (make-hash-table :test #'eq))
            (pg::*physical-export-origin-test-hook* nil))
       (pg::wal-ensure-directory-durable (pg::wal-directory))
       (with-open-file (stream (pg::wal-segment-path 0) :direction :output
                                                        :if-exists :error :element-type '(unsigned-byte 8))
         (write-sequence (make-array 32 :element-type '(unsigned-byte 8) :initial-element 0) stream)
         (write-sequence (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 32
                                                (pg::wal-encode-checkpoint-snapshot snapshot)) stream)
         (pg::fsync-stream stream))
       (funcall function root)))))

(defun backup-lease-authority-prepare (&optional ready request (digest 991))
  (let* ((requested (backup-lease-test-record))
         (request (pg::physical-backup-lease-request-copy
                   (or request (getf requested :request)))))
    (setf (getf requested :request) request)
    (let ((capturing (copy-list requested)))
      (setf (getf capturing :phase) :capturing)
      (pg::physical-backup-lease-publish nil requested)
      (pg::physical-backup-lease-publish requested capturing)
      (let ((pin (pg::with-live-operation-quiescence
                   (pg::physical-backup-lease-create-pin request))))
        (when ready
          (pg::physical-source-pin-bind-manifest pin digest)
          (let ((next (copy-list capturing)))
            (setf (getf next :phase) :ready
                  (getf next :base) (list :checkpoint-start 32 :base 152 :manifest-digest digest)
                  capturing (pg::physical-backup-lease-publish capturing next))))
        (values capturing pin)))))

(cl-spec:defproperty backup-lease-indexed-names-deny-ordinary-pin-lanes
    ((phase (range integer 0 5)))
  (:about pg::physical-backup-lease-check-ordinary-pin)
  (:trials (:normal 100))
  (let* ((record (backup-lease-test-record))
         (selected (nth phase '(:requested :capturing :ready :interrupted :closing :closed)))
         (name (backup-lease-store-test-name record))
         (pg::*physical-backup-lease-records* (make-hash-table :test #'equal))
         (pg::*wal-lock* (sb-thread:make-mutex :name "lease role property")))
    (setf (getf record :phase) selected)
    (when (eq selected :ready) (setf (getf record :base) (getf (backup-lease-test-record :ready) :base)))
    (when (member selected '(:closing :closed)) (setf (getf record :outcome) :aborted))
    (setf (gethash name pg::*physical-backup-lease-records*) (pg::physical-backup-lease-copy record))
    (sb-thread:with-mutex (pg::*wal-lock*)
      (and (backup-lease-authority-error-p
            (lambda () (pg::physical-backup-lease-check-ordinary-pin name)))
           (null (pg::physical-backup-lease-check-ordinary-pin "ordinary"))))))

(deftest generated-backup-lease-private-role-boundaries
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-indexed-names-deny-ordinary-pin-lanes
                                         :seed 91 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-name-creation-requires-capturing-owner-and-quiescence
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((requested (backup-lease-test-record))
              (capturing (backup-lease-test-record :capturing))
              (request (getf requested :request))
              (name (getf request :name)))
         (pg::physical-backup-lease-publish nil requested)
         (ok (backup-lease-authority-error-p (lambda () (pg::physical-source-pin-create name))))
         (ok (backup-lease-authority-error-p (lambda () (pg::physical-backup-lease-create-pin request))))
         (pg::with-live-operation-quiescence
           (ok (backup-lease-authority-error-p (lambda () (pg::physical-backup-lease-create-pin request)))))
         (pg::physical-backup-lease-publish requested capturing)
         (let ((wrong (pg::physical-backup-lease-request-copy request)))
           (setf (getf wrong :token) (make-string 64 :initial-element #\b))
           (pg::with-live-operation-quiescence
             (ok (backup-lease-authority-error-p (lambda () (pg::physical-backup-lease-create-pin wrong))))))
         (let ((pin (pg::with-live-operation-quiescence
                      (pg::physical-backup-lease-create-pin request))))
           (ok (eq pin (gethash name pg::*physical-source-pins*)))
           (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-closed-name-denies-bootstrap-and-follow-reuse
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((closing (backup-lease-test-record))
              (name (backup-lease-store-test-name closing)))
         (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
         (pg::physical-backup-lease-publish nil closing)
         (let ((closed (copy-tree closing)))
           (setf (getf closed :phase) :closed)
           (pg::physical-backup-lease-publish closing closed))
         (ok (backup-lease-authority-error-p (lambda () (pg::physical-source-pin-create name))))
         (ok (backup-lease-authority-error-p
              (lambda () (pg::physical-source-follow-reserve name (follow-anchor-request 123 456)))))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-pattern-alone-does-not-restrict-an-ordinary-pin
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((request (backup-lease-test-request))
              (name (getf request :name))
              (pin (pg::physical-source-pin-create name)))
         (ok (eq pin (gethash name pg::*physical-source-pins*)))
         (pg::physical-source-pin-release name)
         (ok (null (gethash name pg::*physical-source-pins*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-and-ordinary-pin-creation-fence-nonlocal-save-exits
    (dolist (kind '(:lease :ordinary))
      (dolist (point '(:before-save :after-save))
        (call-with-backup-lease-authority-fixture
         (lambda (root)
           (declare (ignore root))
           (let* ((requested (backup-lease-test-record))
                  (capturing (backup-lease-test-record :capturing))
                  (request (getf requested :request))
                  (name (if (eq kind :lease) (getf request :name) "ordinary_cut"))
                  (seen nil))
             (when (eq kind :lease)
               (pg::physical-backup-lease-publish nil requested)
               (pg::physical-backup-lease-publish requested capturing))
             (let ((pg::*physical-source-test-hook*
                     (lambda (actual pin)
                       (declare (ignore pin))
                       (when (eq actual point)
                         (setf seen t)
                         (throw 'lease-pin-cut :cut)))))
               (ok (intake-fails-p
                    (lambda ()
                      (catch 'lease-pin-cut
                        (if (eq kind :lease)
                            (pg::with-live-operation-quiescence
                              (pg::physical-backup-lease-create-pin request))
                            (pg::physical-source-pin-create name)))))))
             (ok seen)
             (ok pg::*wal-poisoned*)
             (ok (null (gethash name pg::*physical-source-pins*)))
             (let ((pg::*wal-poisoned* nil))
               (pg::physical-backup-lease-load-records)
               (pg::physical-source-pin-load)
               (let ((pin (gethash name pg::*physical-source-pins*)))
                 (if (eq point :after-save)
                     (ok (and pin (= 32 (gethash pin pg::*wal-snapshot-retention-pins*))))
                     (ok (null pin))))
               (when (eq kind :lease)
                 (ok (eq :interrupted
                         (getf (gethash name pg::*physical-backup-lease-records*) :phase)))))))))))

(deftest backup-lease-pin-rejects-native-start-and-feedback-before-claims
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (let* ((name (backup-lease-store-test-name record))
                (path (pg::physical-source-pin-path name))
                (before (lineage-test-bytes path))
                (owner (gensym "UNEXPECTED-SENDER-"))
                (command (pg::parse-replication-command
                          (format nil "START_REPLICATION PHYSICAL 0/00000098 (native_format '~a', proof_pin '~a', system_id '~d', timeline '2', base_lsn '0/00000098', manifest_digest '991', prefix_digest '~d')"
                                  pg::+physical-native-format+ name (pg::catalog-system-identifier)
                                  #xCBF29CE484222325))))
           (ok (backup-lease-authority-error-p (lambda () (pg::physical-source-acquire-sender command))))
           (ok (zerop (hash-table-count pg::*physical-source-senders*)))
           (setf (gethash name pg::*physical-source-senders*) owner)
           (ok (backup-lease-authority-error-p
                (lambda () (pg::physical-source-confirm-receipt name owner 152 #xCBF29CE484222325))))
           (remhash name pg::*physical-source-senders*)
           (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
           (ok (equalp before (lineage-test-bytes path)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-pin-rejects-generic-local-and-standby-retirement
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (let* ((name (backup-lease-store-test-name record))
                (path (pg::physical-source-pin-path name))
                (before (lineage-test-bytes path))
                (request (list :version 1 :system-id (pg::catalog-system-identifier)
                               :source-timeline 2 :base 152 :manifest-digest 991)))
           (ok (backup-lease-authority-error-p (lambda () (pg::physical-source-pin-release name))))
           (let ((pg::*physical-recovery-state* (list :phase 2))
                 (pg::*physical-source-pins* (make-hash-table :test #'equal)))
             (ok (backup-lease-authority-error-p
                  (lambda () (pg::physical-source-bootstrap-release name request)))))
           (ok (equalp before (lineage-test-bytes path)))
           (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-checked-unlink-requires-durable-closing-and-exact-request
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (let* ((request (getf record :request))
                (name (getf request :name))
                (path (pg::physical-source-pin-path name))
                (closing (copy-tree record)))
           (pg::with-write-lock
             (pg::with-wal-critical
               (ok (backup-lease-authority-error-p
                    (lambda () (pg::physical-source-pin-unlink pin request))))))
           (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
           (pg::physical-backup-lease-publish record closing)
           (let ((wrong (pg::physical-backup-lease-request-copy request)))
             (setf (getf wrong :token) (make-string 64 :initial-element #\b))
             (pg::with-write-lock
               (pg::with-wal-critical
                 (ok (backup-lease-authority-error-p
                      (lambda () (pg::physical-source-pin-unlink pin wrong)))))))
           (ok (probe-file path))
           (pg::with-write-lock
             (pg::with-wal-publication-fence (begin-publication)
               (pg::physical-backup-lease-check-pin-unlink pin request)
               (begin-publication)
               (pg::physical-source-pin-unlink pin request)))
           (ok (not (probe-file path)))
           (ok (null (gethash name pg::*physical-source-pins*)))
           (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-checked-unlink-allows-disk-only-state-only-on-standby
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (let* ((request (getf record :request))
                (name (getf request :name))
                (path (pg::physical-source-pin-path name))
                (before (lineage-test-bytes path))
                (closing (copy-tree record)))
           (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
           (pg::physical-backup-lease-publish record closing)
           (remhash name pg::*physical-source-pins*)
           (pg::with-write-lock
             (pg::with-wal-critical
               (ok (backup-lease-authority-error-p
                    (lambda () (pg::physical-source-pin-unlink pin request))))))
           (ok (equalp before (lineage-test-bytes path)))
           (let ((pg::*physical-recovery-state* (list :phase 2)))
             (pg::with-write-lock
               (pg::with-wal-publication-fence (begin-publication)
                 (pg::physical-backup-lease-check-pin-unlink pin request)
                 (begin-publication)
                 (pg::physical-source-pin-unlink pin request))))
           (ok (not (probe-file path)))
           (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-keeps-origin-evidence-owned-until-closed
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (let* ((origin (pg::make-physical-export-origin
                         :system-id (pg::catalog-system-identifier) :timeline 2
                         :base 152 :manifest-digest 991 :proven-lsn 152
                         :prefix-digest #xCBF29CE484222325))
                (identity (pg::physical-export-origin-identity origin))
                (request (getf record :request))
                (closing (copy-tree record)))
           (pg::with-wal-critical
             (pg::physical-export-origin-save origin)
             (setf (gethash identity pg::*physical-export-origins*)
                   (pg::make-physical-export-origin-entry :origin origin)))
           (ok (intake-fails-p (lambda () (pg::physical-export-origin-forget identity))))
           (ok (probe-file (pg::physical-export-origin-path origin)))
           (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
           (pg::physical-backup-lease-publish record closing)
           (pg::with-write-lock
             (pg::with-wal-publication-fence (begin-publication)
               (pg::physical-backup-lease-check-pin-unlink pin request)
               (begin-publication)
               (pg::physical-source-pin-unlink pin request)))
           (let ((closed (copy-tree closing)))
             (setf (getf closed :phase) :closed)
             (pg::physical-backup-lease-publish closing closed))
           (pg::physical-export-origin-forget identity)
           (ok (not (probe-file (pg::physical-export-origin-path origin))))
           (ok (not pg::*wal-poisoned*)))))))
