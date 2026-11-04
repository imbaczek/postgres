(in-package #:pg-physical-tests)

(defun backup-lease-wire-request (&optional (identity "lease wire"))
  (let ((request (backup-lease-test-request)))
    (setf (getf request :cluster-identity) identity
          (getf request :system-id) (pg::physical-store-update-digest
                                     #xCBF29CE484222325 (pg::string->bytes identity)))
    (pg::physical-backup-lease-request-copy request)))

(defun backup-lease-wire-record (kind request)
  (when (zerop kind) (return-from backup-lease-wire-record nil))
  (let ((record (backup-lease-test-record (if (member kind '(3 4 5 7)) :ready :requested))))
    (setf (getf record :request) request
          (getf record :phase) (nth kind '(nil :requested :capturing :ready :interrupted :closing :closed :closed)))
    (when (member kind '(5 6 7))
      (setf (getf record :outcome) (if (= kind 7) :handed-off :aborted)))
    (when (= kind 7)
      (setf (getf record :successor) (list :name "wire_successor" :anchor-digest 37)))
    (pg::physical-backup-lease-copy record)))

(defun backup-lease-wire-replace (text before after)
  (let ((position (search before text)))
    (assert position)
    (concatenate 'string (subseq text 0 position) after (subseq text (+ position (length before))))))

(cl-spec:defproperty backup-lease-wire-preserves-the-original-request-and-state
    ((kind (range integer 0 7)) (identity-choice (range integer 0 5)) (verb-choice (range integer 0 2)))
  (:about pg::parse-native-backup-lease-command pg::pg-client-native-backup-lease-record)
  (:trials (:normal 100))
  (let* ((identity (nth identity-choice
                        (list "wire" "wire'quoted" "wire\\literal" "λź日本"
                              (make-string 300 :initial-element #\λ) (format nil "wire~%line"))))
         (request (backup-lease-wire-request identity))
         (verb (nth verb-choice '("CAPTURE_BACKUP_NATIVE" "BACKUP_LEASE_STATUS" "BACKUP_LEASE_ABORT")))
         (record (backup-lease-wire-record kind request))
         (query (pg::pg-client-native-backup-lease-query verb request))
         (command (pg::parse-replication-command query))
         (row (pg::physical-backup-lease-result-row request record)))
    (and (equal request (pg::physical-backup-lease-command-request command))
         (pg::replication-query-payload-p (pg::string->bytes query))
         (equal record (pg::pg-client-native-backup-lease-record (coerce row 'vector) request))
         (not (search (getf request :token) (prin1-to-string row))))))

(deftest generated-backup-lease-wire-request-and-state-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-wire-preserves-the-original-request-and-state
                                         :seed 93 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-wire-rejects-missing-duplicate-and-nonliteral-ownership
    (let* ((request (backup-lease-wire-request))
           (token (getf request :token))
           (name (getf request :name))
           (query (pg::pg-client-native-backup-lease-query "BACKUP_LEASE_STATUS" request)))
      (dolist (bad (list "BACKUP_LEASE_STATUS"
                         "BACKUP_LEASE_STATUS ()"
                         (backup-lease-wire-replace query "lease_token" "unknown_option")
                         (backup-lease-wire-replace query "native_format '" "native_format 'wrong")
                         (concatenate 'string (subseq query 0 (1- (length query))) ", lease_token '" token "')")
                         (backup-lease-wire-replace query (format nil "'~a'" name) name)
                         (backup-lease-wire-replace query (format nil "'~a'" name)
                                                    (format nil "'~a'" (string-upcase name)))
                         (backup-lease-wire-replace query "source_timeline '" "source_timeline '-")
                         (backup-lease-wire-replace query "system_id '" "system_id '-")))
        (let ((condition (handler-case (progn (pg::parse-replication-command bad) nil)
                           (error (condition) condition))))
          (ok condition)
          (ok (not (search token (princ-to-string condition))))))))

(deftest backup-lease-wire-builder-refuses-an-embedded-query-terminator
    (let ((request (backup-lease-wire-request (format nil "wire~cidentity" (code-char 0)))))
      (ok (intake-fails-p
           (lambda () (pg::pg-client-native-backup-lease-query "BACKUP_LEASE_STATUS" request))))))

(deftest backup-lease-wire-client-rejects-incoherent-state-and-different-request
    (let* ((request (backup-lease-wire-request))
           (ready (coerce (pg::physical-backup-lease-result-row
                           request (backup-lease-wire-record 3 request)) 'vector)))
      (dolist (mutation '((0 "other_lease") (1 "unknown") (1 "capturing") (1 "typo")
                          (2 "aborted") (3 nil) (4 nil) (5 nil) (5 "-1") (6 "unexpected_successor")))
        (let ((row (copy-seq ready)))
          (setf (aref row (first mutation)) (second mutation))
          (ok (physical-client-test-protocol-error-p
               (lambda () (pg::pg-client-native-backup-lease-record row request))))))
      (let ((row (coerce (pg::physical-backup-lease-result-row
                          request (backup-lease-wire-record 7 request)) 'vector)))
        (setf (aref row 6) (getf request :name))
        (ok (physical-client-test-protocol-error-p
             (lambda () (pg::pg-client-native-backup-lease-record row request)))))))

(deftest backup-lease-wire-status-uses-a-strict-result-and-does-not-return-the-token
    (let* ((request (backup-lease-wire-request))
           (record (backup-lease-wire-record 3 request)))
      (call-with-physical-client-wire
       (lambda (client request-file)
         (declare (ignore request-file))
         (ok (equal record (pg::pg-client-native-backup-lease-status client request))))
       (lambda (output)
         (physical-client-test-result output pg::*physical-backup-lease-result-schema*
                                      (list (pg::physical-backup-lease-result-row request record))
                                      "BACKUP_LEASE_STATUS")))))

(deftest backup-lease-wire-client-binds-ready-status-to-the-received-image
    (let* ((request (backup-lease-wire-request))
           (record (backup-lease-wire-record 3 request))
           (base (getf record :base))
           (digest (getf base :manifest-digest))
           (manifest (list :version 3 :source-pin (getf request :name)
                           :cluster-identity (getf request :cluster-identity)
                           :system-id (getf request :system-id) :timeline (getf request :timeline)
                           :checkpoint-start (getf base :checkpoint-start)
                           :checkpoint (getf base :base) :end (getf base :base))))
      (ok (pg::physical-backup-lease-check-received-status manifest digest request record))
      (let ((reordered (loop for key in '(:timeline :cluster-identity :system-id :token :name :version)
                             append (list key (getf request key)))))
        (ok (pg::physical-backup-lease-check-received-status manifest digest reordered record)))
      (dolist (key '(:source-pin :cluster-identity :system-id :timeline :checkpoint-start :end))
        (let ((changed (copy-list manifest)))
          (setf (getf changed key) (if (stringp (getf changed key)) "different" (1+ (getf changed key))))
          (ok (physical-client-test-protocol-error-p
               (lambda () (pg::physical-backup-lease-check-received-status changed digest request record))))))
      (ok (physical-client-test-protocol-error-p
           (lambda () (pg::physical-backup-lease-check-received-status manifest (1+ digest) request record))))
      (let ((interrupted (copy-tree record)))
        (setf (getf interrupted :phase) :interrupted)
        (ok (handler-case
                (progn (pg::physical-backup-lease-check-received-status manifest digest request interrupted) nil)
              (pg::pg-client-error (condition) (equal "55000" (pg::pg-client-error-sqlstate condition))))))))

(deftest backup-lease-wire-commands-require-a-physical-sender-and-no-transaction
    (let ((pg::*wal-sender-registry* (make-hash-table :test #'eq))
          (pg::*physical-recovery-state* nil))
      (dolist (verb '("CAPTURE_BACKUP_NATIVE" "BACKUP_LEASE_STATUS" "BACKUP_LEASE_ABORT" "BACKUP_LEASE_HANDOFF"))
        (let ((command (pg::parse-replication-command
                        (pg::pg-client-native-backup-lease-query
                         verb (backup-lease-wire-request)
                         (when (equal verb "BACKUP_LEASE_HANDOFF")
                           (list :name "wire_successor" :anchor-digest 37))))))
          (dolist (backend (list nil (pg::make-backend :pid 177 :replication-mode :database)
                                 (pg::make-backend :pid 178 :replication-mode :physical)))
            (let ((pg::*backend* backend))
              (ok (intake-fails-p (lambda () (pg::physical-backup-lease-wire-preflight command))))))
          (let ((pg::*backend* (pg::make-backend :pid 179 :replication-mode :physical :explicit t)))
            (ok (intake-fails-p (lambda () (pg::validate-replication-command-context command)))))))))

(deftest backup-lease-handoff-wire-checks-admitted-connection-state
    (let ((pg::*wal-sender-registry* (make-hash-table :test #'eq))
          (pg::*sync-replication-state* (pg::make-sync-replication-state :source-id 42))
          (pg::*physical-recovery-failed-p* nil)
          (pg::*backend* (pg::make-backend :pid 181 :replication-mode :physical))
          (command (pg::parse-replication-command
                    (pg::pg-client-native-backup-lease-query
                     "BACKUP_LEASE_HANDOFF" (backup-lease-wire-request)
                     (list :name "wire_successor" :anchor-digest 37)))))
      (pg::wal-sender-register pg::*backend*)
      (unwind-protect
           (dolist (case '(:transaction :aborted :standby))
             (let ((pg::*physical-recovery-state* (when (eq case :standby) (list :phase 2))))
               (setf (pg::backend-explicit pg::*backend*) (eq case :transaction)
                     (pg::backend-state pg::*backend*) (if (eq case :aborted) :aborted :idle))
               (ok (equal (ecase case
                            (:transaction "25001") (:aborted "25P02") (:standby "55000"))
                          (handler-case
                              (progn (pg::physical-backup-lease-wire-preflight command) nil)
                            (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))
               (ok (equal (ecase case
                            (:transaction "25001") (:aborted "25P02") (:standby "25006"))
                          (handler-case
                              (progn (pg::validate-replication-command-context command) nil)
                            (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))))
        (pg::wal-sender-unregister pg::*backend*))))

(cl-spec:defproperty backup-lease-handoff-wire-preserves-successor
    ((digest (range integer 0 18446744073709551615))
     (identity-choice (range integer 0 2)))
  (:about pg::parse-native-backup-lease-command pg::pg-client-native-backup-lease-query)
  (:trials (:normal 80))
  (let* ((request (backup-lease-wire-request
                   (nth identity-choice '("handoff" "handoff'quoted" "handoff\\literal"))))
         (successor (list :name "wire_successor" :anchor-digest digest))
         (query (pg::pg-client-native-backup-lease-query "BACKUP_LEASE_HANDOFF" request successor))
         (command (pg::parse-replication-command query)))
    (and (eq :physical-lease-handoff (pg::logical-replication-command-type command))
         (equal request (pg::physical-backup-lease-command-request command))
         (equal successor (cdr (assoc "successor" (pg::logical-replication-command-options command)
                                      :test #'equal)))
         (pg::replication-query-payload-p (pg::string->bytes query)))))

(deftest generated-backup-lease-handoff-wire-successor-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-handoff-wire-preserves-successor
                                         :seed 103 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-handoff-client-requires-the-exact-closed-successor
    (let* ((request (backup-lease-wire-request))
           (closed (backup-lease-wire-record 7 request))
           (successor (getf closed :successor)))
      (call-with-physical-client-wire
       (lambda (client request-file)
         (declare (ignore request-file))
         (ok (equal closed (pg::pg-client-native-backup-lease-handoff client request successor))))
       (lambda (output)
         (physical-client-test-result output pg::*physical-backup-lease-result-schema*
                                      (list (pg::physical-backup-lease-result-row request closed))
                                      "BACKUP_LEASE_HANDOFF")))
      (dolist (kind '(0 3 6 7 8))
        (let ((record (backup-lease-wire-record (min kind 7) request)))
          (case kind
            (7 (incf (getf (getf record :successor) :anchor-digest)))
            (8 (setf (getf record :phase) :closing)))
          (call-with-physical-client-wire
           (lambda (client request-file)
             (declare (ignore request-file))
             (ok (physical-client-test-protocol-error-p
                  (lambda () (pg::pg-client-native-backup-lease-handoff client request successor)))))
           (lambda (output)
             (physical-client-test-result output pg::*physical-backup-lease-result-schema*
                                          (list (pg::physical-backup-lease-result-row request record))
                                          "BACKUP_LEASE_HANDOFF")))))))

(deftest backup-lease-handoff-wire-executes-the-source-transition
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*wal-sender-registry* (make-hash-table :test #'eq))
             (pg::*sync-replication-state* (pg::make-sync-replication-state :source-id 42)))
         (setf (pg::backend-replication-mode pg::*backend*) :physical)
         (pg::wal-sender-register pg::*backend*)
         (unwind-protect
              (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
                (declare (ignore bootstrap))
                (backup-lease-startup-save-origin record)
                (let* ((pin (backup-lease-startup-prepare-successor record))
                       (request (getf record :request))
                       (successor (list :name (pg::physical-source-pin-name pin) :anchor-digest
                                        (pg::physical-follow-anchor-digest
                                         (pg::physical-follow-pin-anchor pin))))
                       (command (pg::parse-replication-command
                                 (pg::pg-client-native-backup-lease-query
                                  "BACKUP_LEASE_HANDOFF" request successor))))
                  (call-with-physical-client-wire
                   (lambda (client request-file)
                     (declare (ignore request-file))
                     (let ((closed (pg::pg-client-native-backup-lease-handoff client request successor)))
                       (ok (eq :closed (getf closed :phase)))
                       (ok (eq :handed-off (getf closed :outcome)))
                       (ok (equal successor (getf closed :successor)))))
                   (lambda (output) (pg::execute-native-backup-lease-command output command)))
                  (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
                  (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))))
           (pg::wal-sender-unregister pg::*backend*))))))

(deftest backup-lease-handoff-wire-rejects-ambiguous-successor-options
    (let* ((request (backup-lease-wire-request))
           (successor (list :name "wire_successor" :anchor-digest 37))
           (query (pg::pg-client-native-backup-lease-query "BACKUP_LEASE_HANDOFF" request successor)))
      (dolist (bad (list
                    (backup-lease-wire-replace query ", successor_name 'wire_successor'" "")
                    (backup-lease-wire-replace query ", successor_anchor_digest '37'" "")
                    (backup-lease-wire-replace query "successor_name" "unknown_option")
                    (backup-lease-wire-replace query "'wire_successor'" "wire_successor")
                    (backup-lease-wire-replace query "'wire_successor'" "'bad-name'")
                    (backup-lease-wire-replace query "'wire_successor'" (format nil "'~a'" (getf request :name)))
                    (backup-lease-wire-replace query "'37'" "'-1'")
                    (backup-lease-wire-replace query "'37'" "'18446744073709551616'")
                    (concatenate 'string (subseq query 0 (1- (length query)))
                                 ", successor_name 'duplicate')")))
        (let ((condition (handler-case (progn (pg::parse-replication-command bad) nil)
                           (error (condition) condition))))
          (ok condition)
          (ok (not (search (getf request :token) (princ-to-string condition))))))
      (ok (intake-fails-p
           (lambda () (pg::pg-client-native-backup-lease-query "BACKUP_LEASE_HANDOFF" request))))
      (ok (intake-fails-p
           (lambda () (pg::pg-client-native-backup-lease-query "BACKUP_LEASE_STATUS" request successor))))))

(cl-spec:defproperty backup-lease-allocation-wire-preserves-the-exact-intent
    ((generation (range integer 1 16)) (digest (range integer 0 18446744073709551615))
                                       (identity-choice (range integer 0 3)) (verb-choice (range integer 0 1))
                                       (writer (range integer 2 4)))
  (:about pg::parse-native-backup-lease-command pg::pg-client-native-backup-lease-follow-query)
  (:trials (:normal 100))
  (let* ((request (backup-lease-wire-request
                   (nth identity-choice '("allocation" "allocation'quoted" "allocation\\literal" "λź日本"))))
         (row (backup-lease-allocation-test-record :live generation digest))
         (coordinates (getf row :coordinates))
         (verb (nth verb-choice '("BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW"))))
    (setf (getf row :request) request
          (getf coordinates :system-id) (getf request :system-id)
          (getf coordinates :base-timeline) (getf request :timeline)
          (getf coordinates :source-timeline) writer)
    (let* ((query (pg::pg-client-native-backup-lease-follow-query
                   verb request generation (getf row :name) coordinates))
           (command (pg::parse-replication-command query)))
      (and (eq (if (zerop verb-choice) :physical-lease-prepare-follow :physical-lease-cancel-follow)
               (pg::logical-replication-command-type command))
           (equal (pg::physical-backup-lease-allocation-copy row)
                  (pg::physical-backup-lease-command-allocation command))
           (pg::replication-query-payload-p (pg::string->bytes query))))))

(deftest generated-backup-lease-allocation-wire-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-allocation-wire-preserves-the-exact-intent
                                         :seed 113 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-allocation-wire-rejects-unbound-and-ambiguous-intents
    (let* ((row (backup-lease-allocation-test-record))
           (request (getf row :request))
           (query (pg::pg-client-native-backup-lease-follow-query
                   "BACKUP_LEASE_PREPARE_FOLLOW" request 1 (getf row :name) (getf row :coordinates))))
      (dolist (bad (list
                    (backup-lease-wire-replace query ", allocation_generation '1'" "")
                    (backup-lease-wire-replace query "allocation_generation '1'" "allocation_generation '0'")
                    (backup-lease-wire-replace query "allocation_generation '1'" "allocation_generation '17'")
                    (backup-lease-wire-replace query "allocation_generation '1'" "allocation_generation 1")
                    (backup-lease-wire-replace query "allocation_name" "unknown_option")
                    (backup-lease-wire-replace query "'allocated_follow'" "allocated_follow")
                    (backup-lease-wire-replace query "follow_system_id '" "follow_system_id '-")
                    (backup-lease-wire-replace query "follow_base_timeline '2'" "follow_base_timeline '3'")
                    (backup-lease-wire-replace query "follow_source_timeline '2'" "follow_source_timeline '0'")
                    (concatenate 'string (subseq query 0 (1- (length query))) ", allocation_generation '1')")
                    (concatenate 'string (subseq query 0 (1- (length query))) ", follow_proof_kind 'unknown')")))
        (let ((condition (handler-case (progn (pg::parse-replication-command bad) nil)
                           (error (condition) condition))))
          (ok condition)
          (ok (not (search (getf request :token) (princ-to-string condition))))))))

(defun call-with-backup-lease-allocation-wire-fixture (function)
  (call-with-backup-lease-allocation-authority-fixture
   (lambda (root parent row)
     (let ((pg::*wal-sender-registry* (make-hash-table :test #'eq))
           (pg::*sync-replication-state* (pg::make-sync-replication-state :source-id 42)))
       (setf (pg::backend-replication-mode pg::*backend*) :physical)
       (pg::wal-sender-register pg::*backend*)
       (unwind-protect (funcall function root parent row)
         (pg::wal-sender-unregister pg::*backend*))))))

(defun backup-lease-allocation-wire-command (verb row)
  (pg::parse-replication-command
   (pg::pg-client-native-backup-lease-follow-query
    verb (getf row :request) (getf row :generation) (getf row :name) (getf row :coordinates))))

(defun backup-lease-allocation-wire-rpc (row prepare)
  (call-with-physical-client-wire
   (lambda (client request-file)
     (declare (ignore request-file))
     (funcall (if prepare #'pg::pg-client-native-backup-lease-follow-prepare
                  #'pg::pg-client-native-backup-lease-follow-cancel)
              client (getf row :request) (getf row :generation) (getf row :name) (getf row :coordinates)))
   (lambda (output)
     (pg::execute-native-backup-lease-command
      output (backup-lease-allocation-wire-command
              (if prepare "BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW") row)))))

(deftest backup-lease-allocation-wire-executes-prepare-retry-and-cancel
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let ((first (backup-lease-allocation-wire-rpc row t)))
         (ok (pg::physical-follow-request-matches-anchor-p (getf row :coordinates) first))
         (ok (equal first (backup-lease-allocation-wire-rpc row t)))
         (ok (equal parent (backup-lease-allocation-wire-rpc row nil)))
         (ok (eq :canceled (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
         (ok (intake-fails-p (lambda () (backup-lease-allocation-wire-rpc row t))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-wire-rechecks-cancellation-before-reply
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (let ((original (symbol-function 'pg::physical-backup-lease-follow-prepare))
             (injected 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-backup-lease-follow-prepare)
                      (lambda (&rest args)
                        (prog1 (apply original args)
                          (incf injected)
                          (apply #'pg::physical-backup-lease-follow-cancel args))))
                (ok (intake-fails-p (lambda () (backup-lease-allocation-wire-rpc row t))))
                (ok (= 1 injected))
                (ok (eq :canceled
                        (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
                (ok (not pg::*wal-poisoned*)))
           (setf (symbol-function 'pg::physical-backup-lease-follow-prepare) original))))))

(deftest backup-lease-allocation-wire-keeps-selected-handoff-on-cancel
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((anchor (backup-lease-allocation-wire-rpc row t))
              (successor (list :name (getf row :name) :anchor-digest
                               (pg::physical-follow-anchor-digest anchor)))
              (closed (pg::physical-backup-lease-handoff (getf parent :request) successor)))
         (ok (equal closed (backup-lease-allocation-wire-rpc row nil)))
         (ok (eq :live (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
         (ok (gethash (getf row :name) pg::*physical-source-pins*))
         (ok (equal anchor (backup-lease-allocation-wire-rpc row t)))))))

(deftest backup-lease-allocation-wire-checks-admission-and-transaction-state
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (dolist (verb '("BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW"))
         (let ((command (backup-lease-allocation-wire-command verb row)))
           (dolist (fault '(:transaction :aborted :standby :unadmitted :database))
             (let ((pg::*physical-recovery-state* (when (eq fault :standby) (list :phase 2)))
                   (pg::*wal-sender-registry* (if (eq fault :unadmitted)
                                                  (make-hash-table :test #'eq) pg::*wal-sender-registry*)))
               (setf (pg::backend-explicit pg::*backend*) (eq fault :transaction)
                     (pg::backend-state pg::*backend*) (if (eq fault :aborted) :aborted :idle)
                     (pg::backend-replication-mode pg::*backend*) (if (eq fault :database) :database :physical))
               (let ((expected (case fault
                                 (:transaction "25001") (:aborted "25P02")
                                 (:standby (when (equal verb "BACKUP_LEASE_PREPARE_FOLLOW") "55000"))
                                 (otherwise "55000"))))
                 (ok (equal expected
                            (handler-case (progn (pg::physical-backup-lease-wire-preflight command) nil)
                              (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))))
           (setf (pg::backend-explicit pg::*backend*) nil
                 (pg::backend-state pg::*backend*) :idle
                 (pg::backend-replication-mode pg::*backend*) :physical))))))

(deftest backup-lease-allocation-wire-cancel-on-standby-needs-no-old-wal
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (backup-lease-allocation-wire-rpc row t)
       (delete-file (pg::wal-segment-path 0))
       (let ((pg::*physical-recovery-state* (list :phase 2)))
         (ok (equal parent (backup-lease-allocation-wire-rpc row nil))))
       (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
       (ok (not pg::*wal-poisoned*)))))

(deftest backup-lease-allocation-wire-terminal-unknown-cancel-does-not-create-a-row
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let ((closed (pg::physical-backup-lease-abort (getf parent :request))))
         (ok (equal closed (backup-lease-allocation-wire-rpc row nil)))
         (ok (null (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))
         (ok (not (probe-file (pg::physical-backup-lease-allocation-path row))))))))

(deftest backup-lease-allocation-client-rejects-changed-result-identities
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent intent)
       (declare (ignore root))
       (let* ((request (getf intent :request))
              (name (getf intent :name))
              (coordinates (getf intent :coordinates))
              (pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare intent))
              (prepared (pg::physical-backup-lease-follow-prepare-snapshot intent pin))
              (canceled (append (pg::physical-backup-lease-result-row request parent) (list "1" name))))
         (dolist (prepare '(nil t))
           (let ((schema (if prepare
                             (append '(("lease_name" 25) ("allocation_generation" 20))
                                     (physical-client-test-prepare-schema))
                             (append pg::*physical-backup-lease-result-schema*
                                     '(("allocation_generation" 20) ("allocation_name" 25)))))
                 (tag (if prepare "BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW")))
             (dolist (mutation (if prepare
                                   '((0 "other_lease") (1 "2") (1 "-1") (2 "other_pin") (3 "0")
                                     (4 "3") (9 "0/00000099") (10 "0") (13 "bad history") (14 "2"))
                                   '((0 "other_lease") (1 "unknown") (8 "2") (8 "-1") (9 "other_pin"))))
               (let ((row (copy-list (if prepare prepared canceled))))
                 (setf (nth (first mutation) row) (second mutation))
                 (call-with-physical-client-wire
                  (lambda (client request-file)
                    (declare (ignore request-file))
                    (ok (physical-client-test-protocol-error-p
                         (lambda ()
                           (funcall (if prepare #'pg::pg-client-native-backup-lease-follow-prepare
                                        #'pg::pg-client-native-backup-lease-follow-cancel)
                                    client request 1 name coordinates)))))
                  (lambda (output) (physical-client-test-result output schema (list row) tag)))))))))))

(deftest backup-lease-allocation-wire-retained-proof-kind-roundtrip
    (let* ((intent (backup-lease-allocation-test-record))
           (coordinates (getf intent :coordinates)))
      (setf (getf coordinates :source-timeline) 3
            (getf coordinates :kind) :retained-timeline)
      (dolist (verb '("BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW"))
        (let ((command (backup-lease-allocation-wire-command verb intent)))
          (ok (equal (pg::physical-backup-lease-allocation-copy intent)
                     (pg::physical-backup-lease-command-allocation command)))))))

(deftest backup-lease-allocation-client-freezes-inputs-before-io
    (dolist (prepare '(nil t))
      (dolist (fault (if prepare '(:name :request :coordinates) '(:name :request)))
        (call-with-backup-lease-allocation-wire-fixture
         (lambda (root parent intent)
           (declare (ignore root))
           (let* ((before (pg::physical-backup-lease-allocation-copy intent))
                  (request (getf intent :request))
                  (name (getf intent :name))
                  (coordinates (getf intent :coordinates))
                  (pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare intent))
                  (response (if prepare
                                (pg::physical-backup-lease-follow-prepare-snapshot intent pin)
                                (append (pg::physical-backup-lease-result-row request parent) (list "1" name))))
                  (changed-lease (copy-seq (getf request :name)))
                  (original (symbol-function 'pg::physical-client-query))
                  (injected 0))
             (setf (char changed-lease (1- (length changed-lease))) #\e)
             (call-with-physical-client-wire
              (lambda (client request-file)
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::physical-client-query)
                             (lambda (actual query schema tag &key (maximum 4096) command-only)
                               (incf injected)
                               (ecase fault
                                 (:name (setf (char name 0) #\z))
                                 (:request (setf (getf request :name) changed-lease))
                                 (:coordinates (incf (getf coordinates :receive-digest))))
                               (funcall original actual query schema tag
                                        :maximum maximum :command-only command-only)))
                       (ok (physical-client-test-protocol-error-p
                            (lambda ()
                              (funcall (if prepare #'pg::pg-client-native-backup-lease-follow-prepare
                                           #'pg::pg-client-native-backup-lease-follow-cancel)
                                       client request 1 name coordinates)))))
                  (setf (symbol-function 'pg::physical-client-query) original))
                (ok (= 1 injected))
                (with-open-file (in request-file :element-type '(unsigned-byte 8))
                  (multiple-value-bind (kind payload) (pg::pg-client-frame in)
                    (ok (eql #\Q kind))
                    (ok (equal before
                               (pg::physical-backup-lease-command-allocation
                                (pg::parse-replication-command (pg::pg-client-cstring payload 0))))))))
              (lambda (output)
                (let ((row (copy-list response)))
                  (ecase fault
                    (:name (setf (nth (if prepare 2 9) row) "zllocated_follow"))
                    (:request (setf (first row) changed-lease))
                    (:coordinates (setf (nth 10 row) (format nil "~d" (1+ (getf coordinates :receive-digest))))))
                  (physical-client-test-result
                   output (if prepare (append '(("lease_name" 25) ("allocation_generation" 20))
                                              (physical-client-test-prepare-schema))
                              (append pg::*physical-backup-lease-result-schema*
                                      '(("allocation_generation" 20) ("allocation_name" 25))))
                   (list row) (if prepare "BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW")))))))))))

(deftest backup-lease-allocation-wire-settles-selected-closing-on-cancel
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((anchor (backup-lease-allocation-wire-rpc row t))
              (closing (copy-tree parent))
              (name (getf row :name)))
         (setf (getf closing :phase) :closing
               (getf closing :outcome) :handed-off
               (getf closing :successor)
               (list :name name :anchor-digest (pg::physical-follow-anchor-digest anchor)))
         (pg::physical-backup-lease-publish parent closing)
         (ok (equal anchor (backup-lease-allocation-wire-rpc row t)))
         (let ((result (backup-lease-allocation-wire-rpc row nil)))
           (ok (eq :closed (getf result :phase)))
           (ok (eq :handed-off (getf result :outcome)))
           (ok (equal (getf closing :successor) (getf result :successor))))
         (ok (eq :live (getf (gethash name pg::*physical-backup-lease-allocations*) :state)))
         (ok (gethash name pg::*physical-source-pins*))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-wire-snapshot-requires-the-exact-returned-pin
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (let* ((pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))
              (other (copy-structure pin)))
         (ok (intake-fails-p
              (lambda () (pg::physical-backup-lease-follow-prepare-snapshot row other))))
         (ok (not pg::*wal-poisoned*))
         (ok (pg::physical-backup-lease-follow-prepare-snapshot row pin))))))

(deftest backup-lease-allocation-wire-rejects-foreign-credentials-and-rebound-intents
    (call-with-backup-lease-allocation-wire-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (backup-lease-allocation-wire-rpc row t)
       (let* ((name (getf row :name))
              (pin (gethash name pg::*physical-source-pins*))
              (before (lineage-test-bytes (pg::physical-backup-lease-allocation-path row))))
         (dolist (verb '("BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW"))
           (dolist (fault '(:token :generation :coordinates))
             (let ((bad (pg::physical-backup-lease-allocation-copy row)))
               (ecase fault
                 (:token (setf (char (getf (getf bad :request) :token) 0) #\b))
                 (:generation (incf (getf bad :generation)))
                 (:coordinates (incf (getf (getf bad :coordinates) :manifest-digest))))
               (let ((condition
                       (handler-case
                           (with-open-file (out (merge-pathnames "rejected-wire" pg::*pgdata*)
                                                :direction :output :if-exists :supersede
                                                :element-type '(unsigned-byte 8))
                             (pg::execute-native-backup-lease-command
                              out (backup-lease-allocation-wire-command verb bad))
                             nil)
                         (pg::pg-error (condition) condition))))
                 (ok condition)
                 (ok (equal "55000" (pg::pg-error-sqlstate condition)))
                 (ok (not (search (getf (getf bad :request) :token) (princ-to-string condition)))))
               (ok (equalp before (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
             (ok (eq pin (gethash name pg::*physical-source-pins*)))
             (ok (not pg::*wal-poisoned*))))))))
