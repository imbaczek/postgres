(in-package #:pg-physical-tests)

(defun native-test-start-query (&optional (cursor 64) (base 32) (identity 123))
  (format nil "START_REPLICATION PHYSICAL ~a (proof_pin 'copy', native_format 'pg_lisp_wal_7', system_id '~d', timeline '1', base_lsn '~a', prefix_digest '14695981039346656037')"
          (pg::wal-format-lsn cursor) identity (pg::wal-format-lsn base)))

(cl-spec:defproperty native-start-keeps-public-slot-and-proof-identities-separate
    ((public-id (range integer 0 1024))
     (proof-id (range integer 0 1024))
     (named (range integer 0 1))
     (shared (range integer 0 1)))
  (:about pg::parse-native-physical-start)
  (:trials (:normal 200))
  (let* ((public (format nil "slot_~d" public-id))
         (proof (if (zerop shared) (format nil "proof_~d" proof-id) public))
         (query (format nil
                        "START_REPLICATION ~aPHYSICAL 0/40 (native_format '~a', proof_pin '~a', system_id '123', timeline '1', base_lsn '0/20', prefix_digest '14695981039346656037')"
                        (if (zerop named) "" (format nil "SLOT ~a " public))
                        pg::+physical-native-format+ proof))
         (command (pg::parse-replication-command query)))
    (and (equal (unless (zerop named) public) (pg::logical-replication-command-slot-name command))
         (equal proof (pg::physical-stream-proof-pin command))
         (= 64 (pg::logical-replication-command-requested-lsn command)))))

(deftest generated-native-start-identities-stay-separate
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'native-start-keeps-public-slot-and-proof-identities-separate :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest native-start-requires-one-explicit-private-proof-pin
    (let ((old "START_REPLICATION SLOT copy PHYSICAL 0/40 (native_format 'pg_lisp_wal_7', system_id '123', timeline '1', base_lsn '0/20', prefix_digest '14695981039346656037')"))
      (ok (equal "0A000" (recovery-state-of (lambda () (pg::parse-replication-command old))))))
  (dolist (options '("proof_pin 'first', proof_pin 'second'"
                     "proof_pin 'Invalid-Proof'"
                     "proof_pin ''"))
    (let ((query (format nil
                         "START_REPLICATION PHYSICAL 0/40 (native_format '~a', system_id '123', timeline '1', base_lsn '0/20', prefix_digest '14695981039346656037', ~a)"
                         pg::+physical-native-format+ options)))
      (ok (intake-fails-p (lambda () (pg::parse-replication-command query)))))))

(defun call-with-public-stream-fixture (function &key (public-name "public_slot") (proof-name "private_proof") temporary)
  (call-with-physical-source-fixture
   (lambda ()
     (let* ((pg::*physical-recovery-state* nil)
            (pg::*physical-recovery-failed-p* nil)
            (pg::*wal-ready* t)
            (pg::*wal-insert-timeline* 1) (pg::*wal-segment-namespace* :legacy)
            (pg::*physical-source-senders* (make-hash-table :test #'equal))
            (pg::*backend* (pg::make-backend :pid 3456))
            (base pg::*wal-checkpoint-lsn*)
            (pg::*wal-durable-lsn* (+ base 64))
            (pg::*wal-end-lsn* pg::*wal-durable-lsn*)
            (public (pg::wal-create-physical-replication-slot public-name :temporary temporary))
            (pin (pg::physical-source-pin-create proof-name)))
       (pg::wal-atomic-write-bytes
        (pg::wal-segment-path (floor base pg::+wal-segment-size+))
        (concatenate '(vector (unsigned-byte 8))
                     (pg::wal-encode-record pg::+wal-record-maintenance+ 0 base #())
                     (pg::wal-encode-record pg::+wal-record-maintenance+ 0 (+ base 32) #())))
       (flet ((command (&optional (cursor (or (pg::physical-source-pin-confirmed-lsn pin) base)))
                (pg::parse-replication-command
                 (format nil
                         "START_REPLICATION SLOT ~a PHYSICAL ~a (native_format '~a', proof_pin '~a', system_id '~d', timeline '1', base_lsn '~a', prefix_digest '~d')"
                         public-name (pg::wal-format-lsn cursor) pg::+physical-native-format+ proof-name
                         (pg::catalog-system-identifier) (pg::wal-format-lsn base)
                         (pg::physical-source-pin-prefix-digest pin)))))
         (funcall function #'command public pin base))))))

(deftest named-native-stream-claims-both-identities-and-keeps-durable-state
    (dolist (shared '(nil t))
      (call-with-public-stream-fixture
       (lambda (command public pin base)
         (declare (ignore base))
         (let ((owner nil))
           (pg::call-with-native-physical-sender
            (funcall command)
            (lambda (context limit next)
              (declare (ignore limit next))
              (setf owner context)
              (ok (eq context (pg::wal-replication-slot-active-owner public)))
              (ok (eq pg::*backend* (pg::wal-replication-slot-active-owner-backend public)))
              (ok (eq context (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*)))
              (ok (null (pg::wal-replication-slot-restart-lsn public)))))
           (ok (null (pg::wal-replication-slot-active-owner public)))
           (ok (null (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*)))
           (ok (eq public (gethash "public_slot" pg::*wal-replication-slots*)))
           (ok (eq pin (gethash (pg::physical-source-pin-name pin) pg::*physical-source-pins*)))
           (ok (probe-file (pg::wal-replication-slot-path "public_slot")))
           (ok (probe-file (pg::physical-source-pin-path (pg::physical-source-pin-name pin))))
           (ok owner)))
       :proof-name (if shared "public_slot" "private_proof"))))

(deftest named-native-stream-rejects-public-slot-errors-without-claiming-proof
    (dolist (kind '(:missing :logical :lost :busy))
      (call-with-public-stream-fixture
       (lambda (command public pin base)
         (declare (ignore base))
         (ecase kind
           (:missing (remhash "public_slot" pg::*wal-replication-slots*))
           (:logical (setf (pg::wal-replication-slot-kind public) :logical))
           (:lost (setf (pg::wal-replication-slot-wal-status public) :lost))
           (:busy (setf (pg::wal-replication-slot-active-owner public) :another-owner)))
         (ok (equal (ecase kind (:missing "42704") ((:logical :lost) "55000") (:busy "55006"))
                    (recovery-state-of
                     (lambda ()
                       (pg::call-with-native-physical-sender
                        (funcall command) (lambda (&rest values) (declare (ignore values))))))))
         (ok (null (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*)))
         (ok (eq (if (eq kind :busy) :another-owner nil)
                 (pg::wal-replication-slot-active-owner public)))))))

(deftest named-native-stream-installs-cleanup-before-either-claim
    (dolist (cut '(:after-native-private-sender-claim :after-native-public-sender-claim))
      (call-with-public-stream-fixture
       (lambda (command public pin base)
         (declare (ignore base))
         (let ((pg::*logical-test-failpoint-hook*
                 (lambda (point &rest details)
                   (declare (ignore details))
                   (when (eq point cut) (throw 'abort-claim :aborted)))))
           (ok (eq :aborted
                   (catch 'abort-claim
                     (pg::call-with-native-physical-sender
                      (funcall command) (lambda (&rest values) (declare (ignore values))))))))
         (ok (null (pg::wal-replication-slot-active-owner public)))
         (ok (null (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest named-native-stream-first-real-feedback-reserves-only-flush
    (call-with-public-stream-fixture
     (lambda (command public pin base)
       (let ((query (funcall command)))
         (pg::call-with-native-physical-sender
          query
          (lambda (owner limit next)
            (declare (ignore limit next))
            (pg::physical-source-validate-resume query owner)
            (pg::physical-source-confirm-receipt
             (pg::physical-source-pin-name pin) owner base (pg::physical-source-pin-prefix-digest pin))
            (ok (null (pg::wal-replication-slot-restart-lsn public)))
            (pg::physical-source-confirm-status
             query owner (list :write (+ base 64) :flush (+ base 32) :apply base))
            (ok (= (+ base 32) (pg::wal-replication-slot-restart-lsn public)))
            (ok (= (+ base 32) (pg::physical-source-pin-confirmed-lsn pin)))))
         (ok (= (+ base 32)
                (pg::wal-replication-slot-restart-lsn
                 (pg::wal-read-replication-slot (pg::wal-replication-slot-path "public_slot")))))))))

(deftest named-native-stream-can-start-before-and-feedback-backward-from-public-floor
    (call-with-public-stream-fixture
     (lambda (command public pin base)
       (let ((first (funcall command)))
         (pg::call-with-native-physical-sender
          first
          (lambda (owner limit next)
            (declare (ignore limit next))
            (pg::physical-source-confirm-status
             first owner (list :write (+ base 64) :flush (+ base 32) :apply base)))))
       (pg::wal-advance-physical-replication-slot "public_slot" (+ base 64))
       (let ((second (funcall command)))
         (pg::call-with-native-physical-sender
          second
          (lambda (owner limit next)
            (declare (ignore limit next))
            (pg::physical-source-validate-resume second owner)
            (ok (= (+ base 64) (pg::wal-replication-slot-restart-lsn public)))
            (pg::physical-source-confirm-status
             second owner (list :write (+ base 64) :flush (+ base 32) :apply base))
            (ok (= (+ base 32) (pg::physical-source-pin-confirmed-lsn pin)))
            (ok (= (+ base 32) (pg::wal-replication-slot-restart-lsn public))))))
       (ok (= (+ base 32)
              (pg::wal-replication-slot-restart-lsn
               (pg::wal-read-replication-slot (pg::wal-replication-slot-path "public_slot"))))))))

(deftest named-native-temporary-stream-restores-the-creator-claim-on-release
    (call-with-public-stream-fixture
     (lambda (command public pin base)
       (declare (ignore pin base))
       (let ((creator pg::*backend*))
         (ok (eq creator (pg::wal-replication-slot-active-owner public)))
         (pg::call-with-native-physical-sender
          (funcall command)
          (lambda (owner limit next)
            (declare (ignore limit next))
            (ok (eq owner (pg::wal-replication-slot-active-owner public)))
            (ok (not (eq creator owner)))))
         (ok (eq creator (pg::wal-replication-slot-active-owner public)))
         (ok (eq creator (pg::wal-replication-slot-active-owner-backend public)))
         (ok (plusp (pg::wal-replication-slot-inactive-since public)))
         (ok (not (eq pg::+null+ (aref (pg::sql-public-replication-slot-row public) 16))))
         (ok (eq public (gethash "public_slot" pg::*wal-replication-slots*)))
         (ok (probe-file (pg::wal-replication-slot-path "public_slot")))))
     :temporary t))

(deftest named-native-stream-revalidates-both-generations-before-read-and-feedback
    (dolist (change '(:public :private :owner :backend :caller :lost :timeline :namespace))
      (call-with-public-stream-fixture
       (lambda (command public pin base)
         (let ((query (funcall command)))
           (pg::call-with-native-physical-sender
            query
            (lambda (owner limit next)
              (declare (ignore limit next))
              (ecase change
                (:public (setf (gethash "public_slot" pg::*wal-replication-slots*)
                               (pg::copy-wal-replication-slot public)))
                (:private (setf (gethash (pg::physical-source-pin-name pin) pg::*physical-source-pins*)
                                (pg::copy-physical-source-pin pin)))
                (:owner (setf (pg::wal-replication-slot-active-owner public) :replacement))
                (:backend (setf (pg::wal-replication-slot-active-owner-backend public)
                                (pg::make-backend :pid 9999)))
                (:caller (setf pg::*backend* (pg::make-backend :pid 9999)))
                (:lost (setf (pg::wal-replication-slot-wal-status public) :lost))
                (:timeline (incf pg::*wal-insert-timeline*))
                (:namespace (setf pg::*wal-segment-namespace* :timeline)))
              (ok (equal "55000"
                         (recovery-state-of (lambda () (pg::physical-source-read-next
                                                        (pg::physical-source-pin-name pin) owner base)))))
              (ok (intake-fails-p
                   (lambda () (pg::physical-source-confirm-status
                               query owner (list :write base :flush base :apply base)))))
              (ok (not pg::*wal-poisoned*))
              (when (eq change :caller)
                (setf pg::*backend* (pg::physical-sender-backend owner)))))
           (when (eq change :public)
             (let ((replacement (gethash "public_slot" pg::*wal-replication-slots*)))
               (ok (pg::wal-replication-slot-active-owner replacement)))))))))

(deftest named-native-public-flush-publication-fences-all-interrupted-cuts
    (dolist (backward '(nil t))
      (dolist (cut '(:before-save :after-save))
        (dolist (exit '(:error :throw :timeout))
          (call-with-public-stream-fixture
           (lambda (command public pin base)
             (let* ((save (symbol-function 'pg::wal-save-replication-slot))
                    (old (if backward (+ base 64) base))
                    (new (+ base 32))
                    (pg::*pm-requested-mode* pg::+no-shutdown+)
                    (query (funcall command))
                    (interrupted nil))
               (setf (pg::wal-replication-slot-restart-lsn public) old
                     (pg::wal-replication-slot-restart-timeline public) 1)
               (funcall save public)
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::wal-save-replication-slot)
                            (lambda (candidate)
                              (ok (<= (pg::wal-replication-slot-restart-lsn public) (min old new)))
                              (when (eq cut :after-save) (funcall save candidate))
                              (ecase exit
                                (:error (error "injected public feedback save failure"))
                                (:throw (throw 'abort-public-save :aborted))
                                (:timeout (error 'sb-ext:timeout :seconds 1)))))
                      (handler-case
                          (setf interrupted
                                (catch 'abort-public-save
                                  (pg::call-with-native-physical-sender
                                   query
                                   (lambda (owner limit next)
                                     (declare (ignore limit next))
                                     (pg::physical-source-confirm-status
                                      query owner (list :write (+ base 64) :flush new :apply base))))))
                        (error () (setf interrupted :error))
                        (sb-ext:timeout () (setf interrupted :timeout)))
                      (ok interrupted)
                      (ok pg::*wal-poisoned*)
                      (ok (= pg::+fast-shutdown+ pg::*pm-requested-mode*))
                      (ok (= (min old new) (pg::wal-replication-slot-restart-lsn public)))
                      (ok (null (pg::wal-replication-slot-active-owner public)))
                      (ok (null (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*)))
                      (ok (probe-file (pg::physical-source-pin-path (pg::physical-source-pin-name pin))))
                      (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
                      (ok (= (if (eq cut :after-save) new old)
                             (pg::wal-replication-slot-restart-lsn
                              (pg::wal-read-replication-slot (pg::wal-replication-slot-path "public_slot")))))
                      (ok (intake-fails-p
                           (lambda () (pg::physical-source-pin-release (pg::physical-source-pin-name pin))))))
                 (setf (symbol-function 'pg::wal-save-replication-slot) save)))))))))

(deftest named-native-public-feedback-fences-real-atomic-file-publication-cuts
    (dolist (backward '(nil t))
      (dolist (cut '(:slot-file-before-fsync :slot-file-after-fsync :slot-file-before-rename
                     :slot-file-after-rename :slot-file-before-directory-fsync
                     :slot-file-after-directory-fsync-before-memory-publication))
        (dolist (exit '(:error :throw :timeout))
          (call-with-public-stream-fixture
           (lambda (command public pin base)
             (let* ((old (if backward (+ base 64) base))
                    (new (+ base 32))
                    (query (funcall command))
                    (original-shutdown (symbol-function 'pg::request-fast-shutdown))
                    (requests nil)
                    (pg::*pm-requested-mode* pg::+no-shutdown+))
               (setf (pg::wal-replication-slot-restart-lsn public) old
                     (pg::wal-replication-slot-restart-timeline public) 1)
               (pg::wal-save-replication-slot public)
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::request-fast-shutdown)
                            (lambda (&rest args)
                              (push (and pg::*wal-poisoned*
                                         (not (sb-thread:holding-mutex-p pg::*wal-lock*))
                                         (not (sb-thread:holding-mutex-p pg::*write-lock*))) requests)
                              (apply original-shutdown args)))
                      (let ((pg::*logical-test-failpoint-hook*
                              (lambda (point &rest details)
                                (declare (ignore details))
                                (when (eq point cut)
                                  (ok (sb-thread:holding-mutex-p pg::*wal-lock*))
                                  (ok (= (min old new) (pg::wal-replication-slot-restart-lsn public)))
                                  (ecase exit
                                    (:error (error "public feedback atomic cut"))
                                    (:throw (throw 'atomic-feedback-cut :aborted))
                                    (:timeout (error 'sb-ext:timeout :seconds 1)))))))
                        (handler-case
                            (catch 'atomic-feedback-cut
                              (pg::call-with-native-physical-sender
                               query
                               (lambda (owner limit next)
                                 (declare (ignore limit next))
                                 (pg::physical-source-confirm-status
                                  query owner (list :write (+ base 64) :flush new :apply base)))))
                          (sb-ext:timeout () nil)
                          (error () nil)))
                      (ok pg::*wal-poisoned*)
                      (ok requests)
                      (ok (every #'identity requests))
                      (ok (= pg::+fast-shutdown+ pg::*pm-requested-mode*))
                      (ok (= (min old new) (pg::wal-replication-slot-restart-lsn public)))
                      (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
                      (ok (null (pg::wal-replication-slot-active-owner public)))
                      (ok (null (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*)))
                      (ok (= (if (member cut '(:slot-file-after-rename
                                               :slot-file-before-directory-fsync
                                               :slot-file-after-directory-fsync-before-memory-publication))
                                 new old)
                             (pg::wal-replication-slot-restart-lsn
                              (pg::wal-read-replication-slot (pg::wal-replication-slot-path "public_slot")))))
                      (ok (intake-fails-p
                           (lambda () (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*))))
                      (ok (probe-file (pg::wal-segment-path (1- (floor base pg::+wal-segment-size+))))))
                 (setf (symbol-function 'pg::request-fast-shutdown) original-shutdown)))))))))

(deftest native-default-receiver-start-uses-private-proof-only
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t :start-lsn 32))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :system-id 123 :timeline 1 :end 32))
             (runtime (pg::make-physical-follow-runtime :pin "copy")))
        (unwind-protect
             (let ((command (pg::parse-replication-command (pg::physical-follow-start-query runtime 32))))
               (ok (null (pg::logical-replication-command-slot-name command)))
               (ok (equal "copy" (pg::physical-stream-proof-pin command))))
          (pg::physical-store-close store)))))

(deftest native-named-receiver-start-keeps-public-slot-separate-from-proof
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t :start-lsn 32))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :system-id 123 :timeline 1 :end 32))
             (config (pg::physical-follow-with-controller
                      '(1 "127.0.0.1" 5432 "private_proof")
                      '(:version 1 :enabled t :preparing nil :cleanup nil :public-slot "public_slot")))
             (runtime (pg::make-physical-follow-runtime :pin "private_proof" :config config))
             (pg::*physical-follow-runtime* runtime)
             (pg::*physical-follow-statistics* nil))
        (unwind-protect
             (progn
               (let ((command (pg::parse-replication-command (pg::physical-follow-start-query runtime 32))))
                 (ok (equal "public_slot" (pg::logical-replication-command-slot-name command)))
                 (ok (equal "private_proof" (pg::physical-stream-proof-pin command))))
               (pg::physical-follow-monitor-start runtime 32)
               (ok (equal "public_slot" (aref (pg::sql-record-values (pg::wal-receiver-result)) 11))))
          (pg::physical-store-close store)))))

(deftest native-retarget-slot-control-dispatch-keeps-literal-arguments-outside-admission
    (let ((original (symbol-function 'pg::physical-follow-retarget-conninfo))
          (seen nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-retarget-conninfo)
                   (lambda (&rest arguments)
                     (setf seen (list pg::*live-operation-depth* arguments))
                     :started))
             (dolist (slot '(nil "public_slot"))
               (setf seen nil)
               (with-output-to-string (out)
                 (pg::control-eval out
                                   (list 'pg::physical-follow-retarget-conninfo "host=localhost"
                                         :prefix "follow" :fallback "receiver" :slot slot)))
               (ok (zerop (first seen)))
               (ok (equal slot (getf (rest (second seen)) :slot))))
             (dolist (form '((pg::physical-follow-retarget-conninfo "host=localhost" :slot (progn "public_slot"))
                             (pg::physical-follow-retarget-conninfo "host=localhost" :slot "one" :slot "two")))
               (setf seen nil)
               (with-output-to-string (out) (pg::control-eval out form))
               (ok (plusp (first seen)))))
        (setf (symbol-function 'pg::physical-follow-retarget-conninfo) original))))

(deftest unnamed-native-stream-keeps-same-spelled-public-slot-unclaimed
    (call-with-physical-source-fixture
     (lambda ()
       (let* ((pg::*physical-recovery-state* nil)
              (pg::*physical-recovery-failed-p* nil)
              (pg::*wal-insert-timeline* 1)
              (pg::*wal-segment-namespace* :legacy)
              (pg::*physical-source-senders* (make-hash-table :test #'equal))
              (public (pg::wal-create-physical-replication-slot "copy"))
              (pin (pg::physical-source-pin-create "copy"))
              (base (pg::physical-source-pin-consistent-lsn pin))
              (command
                (pg::parse-replication-command
                 (format nil
                         "START_REPLICATION PHYSICAL ~a (native_format '~a', proof_pin 'copy', system_id '~d', timeline '1', base_lsn '~a', prefix_digest '~d')"
                         (pg::wal-format-lsn base) pg::+physical-native-format+
                         (pg::catalog-system-identifier) (pg::wal-format-lsn base)
                         (pg::physical-source-pin-prefix-digest pin))))
              (owner (pg::physical-source-acquire-sender command)))
         (unwind-protect
              (progn
                (ok (eq owner (gethash "copy" pg::*physical-source-senders*)))
                (ok (eq public (gethash "copy" pg::*wal-replication-slots*)))
                (ok (null (pg::wal-replication-slot-active-owner public)))
                (ok (null (pg::wal-replication-slot-restart-lsn public))))
           (pg::physical-source-release-sender "copy" owner))
         (ok (null (gethash "copy" pg::*physical-source-senders*)))
         (ok (eq pin (gethash "copy" pg::*physical-source-pins*)))
         (ok (eq public (gethash "copy" pg::*wal-replication-slots*)))
         (ok (probe-file (pg::wal-replication-slot-path "copy")))))))

(deftest native-physical-start-requires-format-and-base
    (let ((command (pg::parse-replication-command (native-test-start-query))))
      (ok (eq :physical-start (pg::logical-replication-command-type command)))
      (ok (= 64 (pg::logical-replication-command-requested-lsn command)))
      (ok (equal "pg_lisp_wal_7" (pg::physical-stream-option command "native_format"))))
  (dolist (query '("START_REPLICATION PHYSICAL 0/0"
                   "START_REPLICATION SLOT copy PHYSICAL 0/0"
                   "START_REPLICATION PHYSICAL 0/0 (proof_pin 'copy', native_format 'other')"
                   "START_REPLICATION PHYSICAL 0/0 (proof_pin 'copy', native_format 'pg_lisp_wal_7', system_id '123', timeline '2', base_lsn '0/0')"
                   "START_REPLICATION PHYSICAL 0/0 (proof_pin 'copy', native_format 'pg_lisp_wal_7', system_id '123', timeline '1', timeline '1')"
                   "START_REPLICATION PHYSICAL 0/0 (proof_pin 'copy', native_format 'pg_lisp_wal_6', system_id '123', timeline '1', base_lsn '0/0', prefix_digest '14695981039346656037')"
                   "START_REPLICATION PHYSICAL 0/0 (proof_pin 'copy', native_format 'pg_lisp_wal_5', system_id '123', timeline '1', base_lsn '0/0', prefix_digest '14695981039346656037')"))
    (ok (intake-fails-p (lambda () (pg::parse-replication-command query))))))

(deftest native-physical-start-rejects-transaction-blocks
    (let ((pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*backend* (pg::make-backend :explicit t :state :in-progress)))
      (ok (equal "25001"
                 (recovery-state-of
                  (lambda ()
                    (pg::validate-replication-command-context
                     (pg::parse-replication-command (native-test-start-query)))))))))

(deftest native-physical-feedback-separates-positions
    (let ((previous (list :write 64 :flush 64 :apply 32))
          (next (list :write 96 :flush 80 :apply 64)))
      (ok (eq next (pg::physical-stream-validate-status next previous 96 32)))
      (dolist (bad (list (list :write 97 :flush 80 :apply 64)
                         (list :write 96 :flush 97 :apply 64)
                         (list :write 96 :flush 80 :apply 81)
                         (list :write 96 :flush 63 :apply 32)
                         (list :write 96 :flush 80 :apply 31)))
        (ok (intake-fails-p
             (lambda () (pg::physical-stream-validate-status bad previous 96 32)))))))

(deftest native-receiver-batches-write-but-flushes-on-keepalive
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :end 0))
             (pg::*physical-recovery-failed-p* nil)
             (pg::*physical-receive-flush-bytes* 1024))
        (unwind-protect
             (labels ((frame (start)
                        (let ((payload (pg::make-payload)) (raw (sample-physical-record start)))
                          (pg::payload-push-byte payload (char-code #\w))
                          (pg::pg-client-push-u64 payload start)
                          (pg::pg-client-push-u64 payload (+ start (length raw)))
                          (pg::pg-client-push-u64 payload 0)
                          (pg::payload-push payload raw)
                          payload)))
               (pg::physical-follow-process-frame (frame 0))
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (let ((keepalive (pg::make-payload)))
                 (pg::payload-push-byte keepalive (char-code #\k))
                 (pg::pg-client-push-u64 keepalive 999999)
                 (pg::pg-client-push-u64 keepalive 0)
                 (pg::payload-push-byte keepalive 1)
                 (pg::physical-follow-process-frame keepalive))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (ok (= 32 (getf pg::*physical-recovery-state* :receive-lsn)))
               (let ((pg::*physical-receive-flush-bytes* 16))
                 (pg::physical-follow-process-frame (frame 32)))
               (ok (= 64 (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))))

(deftest native-receiver-retries-sbcl-timeout-without-escaping-thread
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :end 0))
             (pg::*physical-recovery-failed-p* nil)
             (pg::*shutdown* pg::+no-shutdown+)
             (runtime (pg::make-physical-follow-runtime))
             (original (symbol-function 'pg::physical-follow-session))
             (attempts 0) (escaped nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-follow-session)
                     (lambda (state cursor)
                       (declare (ignore cursor))
                       (if (= (incf attempts) 1)
                           (error 'sb-ext:timeout :seconds 10)
                           (setf (pg::physical-follow-runtime-stopped-p state) t))))
               (handler-case (pg::physical-follow-receiver-loop runtime)
                 (sb-ext:timeout () (setf escaped t)))
               (ok (not escaped))
               (ok (= attempts 2))
               (ok (pg::physical-follow-runtime-last-error runtime)))
          (setf (symbol-function 'pg::physical-follow-session) original)
          (pg::physical-store-close store)))))

(deftest native-follow-config-roundtrip-and-damage
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (config (list 1 "127.0.0.1" 5874 "copy")))
        (pg::physical-follow-save-config config)
        (ok (equal config (pg::physical-follow-read-config)))
        (with-open-file (stream (pg::physical-follow-config-path) :direction :io :if-exists :overwrite)
          (file-position stream 5)
          (write-char #\X stream))
        (ok (intake-fails-p #'pg::physical-follow-read-config))))
  (dolist (config '((1 "" 5874 "copy") (1 "host" 0 "copy")
                    (1 "host" 70000 "copy") (1 "host" 5874 "bad pin")))
    (ok (intake-fails-p (lambda () (pg::physical-follow-check-config config))))))

(deftest native-physical-frame-shape-is-bounded
    (dolist (payload (list (empty-octets) #(119) #(107 0) #(120)))
      (ok (intake-fails-p (lambda () (pg::physical-follow-process-frame payload))))))

(deftest native-source-proves-resume-prefix-and-preserves-corrupt-wal
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*catalog* (pg::make-catalog))
             (pg::*physical-recovery-state* nil)
             (pg::*physical-source-pins* (make-hash-table :test #'equal))
             (pg::*physical-source-senders* (make-hash-table :test #'equal))
             (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
             (pg::*wal-checkpoint-lsn* 64) (pg::*wal-durable-lsn* 128)
             (pg::*wal-poisoned* nil)
             (records (list (sample-physical-record 0)
                            (legacy-checkpoint-record 32)
                            (sample-physical-record 64)
                            (pg::wal-encode-record pg::+wal-record-maintenance+ 0 96 (empty-octets)))))
        (pg::wal-ensure-directory-durable (pg::wal-directory))
        (with-open-file (stream (pg::wal-segment-path 0) :direction :output
                                                         :element-type '(unsigned-byte 8))
          (dolist (raw records) (write-sequence raw stream)))
        (pg::physical-source-pin-create "copy")
        (let* ((command (pg::parse-replication-command
                         (native-test-start-query 128 64 (pg::catalog-system-identifier))))
               (digest (reduce #'pg::physical-store-update-digest (nthcdr 2 records)
                               :initial-value #xCBF29CE484222325))
               (owner (pg::physical-source-acquire-sender command)))
          (unwind-protect
               (progn
                 (setf (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command)
                                   :test #'equal)) (format nil "~d" digest))
                 (ok (= 128 (pg::physical-source-validate-resume command owner)))
                 (setf (pg::logical-replication-command-requested-lsn command) 65)
                 (ok (intake-fails-p (lambda () (pg::physical-source-validate-resume command owner))))
                 (setf (pg::logical-replication-command-requested-lsn command) 128
                       (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command)
                                   :test #'equal)) "0")
                 (ok (intake-fails-p (lambda () (pg::physical-source-validate-resume command owner))))
                 (ok (intake-fails-p (lambda () (pg::physical-source-pin-release "copy"))))
                 (ok (not pg::*wal-poisoned*))
                 (with-open-file (stream (pg::wal-segment-path 0) :direction :io :if-exists :overwrite
                                                                  :element-type '(unsigned-byte 8))
                   (file-position stream 92)
                   (write-byte 0 stream))
                 (ok (intake-fails-p (lambda () (pg::physical-source-read-next "copy" owner 64))))
                 (with-open-file (stream (pg::wal-segment-path 0) :element-type '(unsigned-byte 8))
                   (ok (= 128 (file-length stream)))))
            (pg::physical-source-release-sender "copy" owner)))
        (pg::physical-source-pin-release "copy"))))
