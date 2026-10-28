(in-package #:pg-physical-tests)

(defun call-with-follow-release-proof (function)
  (call-with-follow-anchor-source
   (lambda (root request eor)
     (declare (ignore eor))
     (let ((pin (pg::physical-source-follow-ready "release_follow" request)))
       (funcall function root request pin)))))

(deftest follow-release-directory-sync-failure-fences-with-retention-intact
    (call-with-follow-release-proof
     (lambda (root request pin)
       (declare (ignore root))
       (let ((original (symbol-function 'pg::fsync-directory)) (failed nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::fsync-directory)
                      (lambda (directory)
                        (if (equal directory (pg::physical-source-pin-directory))
                            (error "injected follow-pin directory sync failure")
                            (funcall original directory))))
                (setf failed (handler-case
                                 (progn (pg::physical-source-follow-release
                                         "release_follow" (pg::physical-follow-anchor-digest request)) nil)
                               (error () t))))
           (setf (symbol-function 'pg::fsync-directory) original))
         (ok failed)
         (ok pg::*wal-poisoned*)
         (ok (eq pin (gethash "release_follow" pg::*physical-source-pins*)))
         (ok (gethash pin pg::*wal-snapshot-retention-pins*))
         (ok (null (probe-file (pg::physical-source-pin-path "release_follow"))))))))

(deftest follow-release-sigkill-cold-retry-keeps-retirement-idempotent
    (dolist (point '(:before-release-unlink :after-release-unlink :after-release-directory-fsync))
      (call-with-follow-release-proof
       (lambda (root request pin)
         (declare (ignore pin))
         (let* ((digest (format nil "~d" (pg::physical-follow-anchor-digest request)))
                (script (namestring (asdf:system-relative-pathname "pg" "t/follow-release-crash-child.lisp")))
                (argv (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script" script)))
           (multiple-value-bind (output errors status)
               (uiop:run-program (append argv (list (namestring root) (symbol-name point) digest))
                                 :output :string :error-output :string :ignore-error-status t)
             (ok (not (zerop status)))
             (ok (probe-file (merge-pathnames "release-crash.marker" root))
                 (format nil "~a: ~a ~a" point output errors)))
           (ok (eq (not (null (probe-file (pg::physical-source-pin-path "release_follow"))))
                   (eq point :before-release-unlink)))
           (multiple-value-bind (output errors status)
               (uiop:run-program (append argv (list (namestring root) "RETRY" digest))
                                 :output :string :error-output :string :ignore-error-status t)
             (ok (zerop status) (format nil "~a ~a" output errors))
             (ok (search "follow release cold retry PASS" output))))))))

(cl-spec:defproperty follow-release-anchor-checksum-is-canonical-and-proof-sensitive
    ((prefix (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615)))
  (:about pg::physical-follow-anchor-digest)
  (:trials (:normal 200))
  (let* ((anchor (follow-anchor-request prefix overlap))
         (digest (pg::physical-follow-anchor-digest anchor))
         (reordered (loop for (key . value) in (reverse (loop for (key value) on anchor by #'cddr
                                                              collect (cons key value)))
                          append (list key value))))
    (and (= digest (pg::physical-follow-anchor-digest reordered))
         (progn
           (setf (getf reordered :receive-digest) (logxor prefix 1))
           (/= digest (pg::physical-follow-anchor-digest reordered))))))

(deftest generated-follow-release-anchor-checksum-is-canonical-and-proof-sensitive
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-release-anchor-checksum-is-canonical-and-proof-sensitive :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest follow-release-can-retire-a-lost-ready-pin-without-its-wal
    (call-with-follow-release-proof
     (lambda (root request pin)
       (declare (ignore root))
       (setf (pg::physical-source-pin-lost-p pin) t)
       (pg::physical-source-pin-save pin)
       (delete-file (pg::wal-segment-path 0))
       (ok (eq :released (pg::physical-source-follow-release
                          "release_follow" (pg::physical-follow-anchor-digest request))))
       (ng pg::*wal-poisoned*))))

(deftest follow-release-refuses-nonready-or-disagreeing-durable-metadata
    (dolist (kind '(:bootstrap :retaining :registry-missing :file-missing :disk-proof :disk-progress))
      (call-with-follow-release-proof
       (lambda (root request pin)
         (declare (ignore root))
         (let ((digest (pg::physical-follow-anchor-digest request))
               (path (pg::physical-source-pin-path "release_follow")))
           (ecase kind
             (:bootstrap
              (let ((other (pg::make-physical-source-pin
                            :name "release_follow" :system-id (getf request :system-id)
                            :timeline 2 :manifest-digest 77 :consistent-lsn 64
                            :restart-lsn 64 :confirmed-lsn 64 :prefix-digest 123)))
                (setf (gethash "release_follow" pg::*physical-source-pins*) other)
                (pg::physical-source-pin-save other)))
             (:retaining
              (setf (pg::physical-follow-pin-stage pin) :retaining
                    (pg::physical-source-pin-confirmed-lsn pin) nil
                    (pg::physical-source-pin-restart-lsn pin) (getf request :overlap-start))
              (pg::physical-source-pin-save pin))
             (:registry-missing (remhash "release_follow" pg::*physical-source-pins*))
             (:file-missing (delete-file path))
             ((:disk-proof :disk-progress)
              (let ((other (pg::physical-follow-pin-decode (lineage-test-bytes path))))
                (if (eq kind :disk-proof)
                    (progn
                      (incf (getf (pg::physical-follow-pin-anchor other) :receive-digest))
                      (incf (pg::physical-source-pin-prefix-digest other)))
                    (setf (pg::physical-source-pin-confirmed-lsn other) 120
                          (pg::physical-source-pin-restart-lsn other) 120
                          (pg::physical-source-pin-prefix-digest other) 999))
                (pg::physical-source-pin-save other))))
           (let ((before (when (probe-file path) (lineage-test-bytes path))))
             (ok (signals (pg::physical-source-follow-release "release_follow" digest) 'error)
                 (symbol-name kind))
             (ok (equalp before (when (probe-file path) (lineage-test-bytes path)))))
           (ng pg::*wal-poisoned*))))))

(deftest follow-release-and-logical-drop-keep-their-own-namespaces
    (dolist (operation '(:release :drop))
      (call-with-follow-release-proof
       (lambda (root request pin)
         (declare (ignore root))
         (let* ((pg::*wal-replication-slots* (make-hash-table :test #'equal))
                (slot (pg::make-wal-replication-slot
                       :name "release_follow" :database-oid 1 :plugin "pgoutput"
                       :restart-lsn 64 :consistent-lsn 64 :confirmed-flush-lsn 64))
                (path (pg::wal-replication-slot-path "release_follow")))
           (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
           (pg::wal-save-replication-slot slot)
           (setf (gethash "release_follow" pg::*wal-replication-slots*) slot)
           (let ((before (lineage-test-bytes path)))
             (if (eq operation :release)
                 (progn
                   (pg::physical-source-follow-release "release_follow" (pg::physical-follow-anchor-digest request))
                   (ok (eq slot (gethash "release_follow" pg::*wal-replication-slots*)))
                   (ok (equalp before (lineage-test-bytes path))))
                 (progn
                   (pg::wal-drop-replication-slot "release_follow")
                   (ok (null (probe-file path)))
                   (ok (eq pin (gethash "release_follow" pg::*physical-source-pins*)))
                   (ok (probe-file (pg::physical-source-pin-path "release_follow")))))))))))

(deftest follow-release-retains-memory-ownership-until-directory-sync
    (call-with-follow-release-proof
     (lambda (root request pin)
       (declare (ignore root))
       (let* ((events nil)
              (pg::*physical-source-test-hook*
                (lambda (point selected)
                  (when (member point '(:after-release-unlink :after-release-directory-fsync))
                    (push (list point (eq pin selected)
                                (eq pin (gethash "release_follow" pg::*physical-source-pins*))
                                (not (null (gethash pin pg::*wal-snapshot-retention-pins*)))
                                (not (null (probe-file (pg::physical-source-pin-path "release_follow"))))) events)))))
         (pg::physical-source-follow-release "release_follow" (pg::physical-follow-anchor-digest request))
         (ok (equal (reverse events) '((:after-release-unlink t t t nil)
                                       (:after-release-directory-fsync t t t nil))))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))))))

(defun follow-release-query (digest &optional (name "release_follow"))
  (format nil "RELEASE_FOLLOW SLOT ~a (native_format '~a', anchor_digest '~d')"
          name pg::+physical-native-format+ digest))

(deftest follow-release-wire-parser-validates-negotiation-and-classification
    (let* ((digest (pg::physical-follow-anchor-digest (follow-anchor-request 123 456)))
           (query (follow-release-query digest))
           (command (pg::parse-replication-command query)))
      (ok (eq :physical-release (pg::logical-replication-command-type command)))
      (ok (equal "release_follow" (pg::logical-replication-command-slot-name command)))
      (ok (= digest (pg::physical-stream-u64 (pg::physical-stream-option command "anchor_digest"))))
      (ok (pg::replication-query-payload-p
           (pg::string->bytes (concatenate 'string query (string (code-char 0))))))
      (dolist (case (list (list "native_format '[^']+'" "native_format 'wrong'" "0A000")
                          (list ", anchor_digest '[^']+'" "" "0A000")
                          (list "anchor_digest '[^']+'" "anchor_digest '-1'" "22023")
                          (list "anchor_digest '[^']+'" "anchor_digest '18446744073709551616'" "22023")
                          (list "\\)$" ", anchor_digest '1')" "0A000")
                          (list "\\)$" ", unknown '1')" "0A000")))
        (destructuring-bind (pattern replacement state) case
          (ok (equal state (recovery-state-of
                            (lambda () (pg::parse-replication-command
                                        (cl-ppcre:regex-replace pattern query replacement))))))))))

(deftest follow-release-wire-preserves-transaction-and-recovery-guards
    (let* ((command (pg::parse-replication-command (follow-release-query 123)))
           (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
           (pg::*backend* (pg::make-backend :explicit t :state :in-progress)))
      (ok (equal "25001" (recovery-state-of (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-explicit pg::*backend*) nil pg::*physical-recovery-state* (list :phase 2))
      (ok (equal "25006" (recovery-state-of (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-state pg::*backend*) :aborted)
      (ok (equal "25P02" (recovery-state-of (lambda () (pg::validate-replication-command-context command)))))))

(deftest follow-release-client-requires-only-command-complete-and-idle-ready
    (dolist (kind '(:success :row :tag :duplicate :early-ready))
      (call-with-physical-client-wire
       (lambda (client request)
         (if (eq kind :success)
             (progn
               (ok (pg::pg-client-native-release client "release_follow" 123))
               (with-open-file (in request :element-type '(unsigned-byte 8))
                 (multiple-value-bind (type payload) (pg::pg-client-frame in)
                   (ok (eql #\Q type))
                   (ok (eq :physical-release
                           (pg::logical-replication-command-type
                            (pg::parse-replication-command (pg::pg-client-cstring payload 0))))))))
             (ok (physical-client-test-protocol-error-p
                  (lambda () (pg::pg-client-native-release client "release_follow" 123))))))
       (lambda (out)
         (cond ((eq kind :row) (pg::physical-stream-switch-result out 2 96))
               (t
                (unless (eq kind :early-ready)
                  (pg::send-command-complete out (if (eq kind :tag) "DROP_REPLICATION_SLOT" "RELEASE_FOLLOW")))
                (when (eq kind :duplicate) (pg::send-command-complete out "RELEASE_FOLLOW"))
                (pg::send-ready-for-query out #\I)))))))

(deftest follow-release-client-drains-error-completion-before-retry
    (call-with-physical-client-wire
     (lambda (client request)
       (declare (ignore request))
       (ok (equal "55000" (handler-case
                              (progn (pg::pg-client-native-release client "release_follow" 123) nil)
                            (pg::pg-client-error (condition) (pg::pg-client-error-sqlstate condition)))))
       (ok (pg::pg-client-native-release client "release_follow" 123)))
     (lambda (out)
       (physical-client-test-error-response out :valid)
       (pg::send-command-complete out "RELEASE_FOLLOW")
       (pg::send-ready-for-query out #\I))))

(deftest follow-release-retires-only-the-matching-ready-pin-and-retries-absence
    (call-with-follow-release-proof
     (lambda (root request pin)
       (declare (ignore root))
       (let ((digest (pg::physical-follow-anchor-digest request)))
         (ok (eq :released (pg::physical-source-follow-release "release_follow" digest)))
         (ok (null (gethash "release_follow" pg::*physical-source-pins*)))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (null (probe-file (pg::physical-source-pin-path "release_follow"))))
         (ok (eq :absent (pg::physical-source-follow-release "release_follow" digest)))
         (ng pg::*wal-poisoned*)))))

(deftest follow-release-checksum-does-not-bind-mutable-confirmation-or-loss
    (dolist (lost '(nil t))
      (call-with-follow-release-proof
       (lambda (root request pin)
         (declare (ignore root))
         (let ((digest (pg::physical-follow-anchor-digest request))
               (end (+ (getf request :receive-lsn) (length (getf request :eor-octets)))))
           (pg::with-write-lock
             (pg::with-wal-critical
               (setf (pg::physical-source-pin-confirmed-lsn pin) end
                     (pg::physical-source-pin-restart-lsn pin) end
                     (pg::physical-source-pin-prefix-digest pin) 999
                     (pg::physical-source-pin-lost-p pin) lost)
               (pg::physical-source-pin-save pin)))
           (ok (= digest (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
           (ok (eq :released (pg::physical-source-follow-release "release_follow" digest))))))))

(deftest follow-release-refuses-wrong-anchor-and-active-sender-without-poison
    (dolist (kind '(:anchor :sender))
      (call-with-follow-release-proof
       (lambda (root request pin)
         (declare (ignore root))
         (let* ((digest (pg::physical-follow-anchor-digest request))
                (before (lineage-test-bytes (pg::physical-source-pin-path "release_follow"))))
           (if (eq kind :anchor) (setf digest (logxor digest 1))
               (setf (gethash "release_follow" pg::*physical-source-senders*) :active))
           (ok (signals (pg::physical-source-follow-release "release_follow" digest) 'error))
           (ok (eq pin (gethash "release_follow" pg::*physical-source-pins*)))
           (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path "release_follow"))))
           (ng pg::*wal-poisoned*))))))
