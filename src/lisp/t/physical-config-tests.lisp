;;;; Literal configuration input and command-line precedence against the C contracts.
(in-package #:pg-physical-tests)

(defun write-startup-config (path text)
  (ensure-directories-exist path)
  (with-open-file (stream path :direction :output :if-exists :supersede :external-format :utf-8)
    (write-string text stream)))

(deftest c-logical-subscriber-startup-format-settings-remain-supported
    (let ((pg::*backend* (pg::make-backend)))
      (pg::session-apply-startup-options
       '(("options" . "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3")))
      (ok (equal "ISO, MDY" (pg::sql-setting-value "datestyle")))
      (pg::session-set-extra-setting "datestyle" "ISO , MDY")
      (ok (equal "ISO, MDY" (pg::sql-setting-value "datestyle")))
      (ok (equal "postgres" (pg::sql-setting-value "intervalstyle")))
      (ok (equal "3" (pg::sql-setting-value "extra_float_digits")))
      (ok (intake-fails-p
           (lambda () (pg::session-set-extra-setting "extra_float_digits" "4"))))))

(deftest application-name-normalization-clips-before-byte-escaping
    (ok (equal "plain" (pg::session-normalize-application-name "plain")))
  (ok (equal "a\\x0a\\xc3\\xa9" (pg::session-normalize-application-name (format nil "a~%é"))))
  (ok (equal (make-string 63 :initial-element #\x)
             (pg::session-normalize-application-name (make-string 100 :initial-element #\x))))
  (ok (equal (make-string 62 :initial-element #\x)
             (pg::session-normalize-application-name
              (concatenate 'string (make-string 62 :initial-element #\x) "é"))))
  (ok (= 248 (length (pg::session-normalize-application-name
                      (make-string 32 :initial-element #\é))))))

(deftest application-name-setting-keeps-slot-reset-and-transaction-state-together
    (let ((pg::*backend* (pg::make-backend)) (output (make-broadcast-stream)))
      (pg::session-apply-startup-options
       '(("options" . "-c application_name=option") ("application_name" . "startup")))
      (ok (equal "startup" (pg::sql-setting-value "application_name")))
      (ok (equal "startup" (pg::backend-application-name pg::*backend*)))
      (pg::command-begin output nil)
      (pg::session-set-application-name "session")
      (pg::session-set-application-name "local1" t)
      (pg::session-set-application-name "local2" t)
      (ok (equal "local2" (pg::backend-application-name pg::*backend*)))
      (pg::command-commit output)
      (ok (equal "session" (pg::sql-setting-value "application_name")))
      (ok (equal "session" (pg::backend-application-name pg::*backend*)))
      (pg::command-begin output nil)
      (pg::session-set-application-name "local" t)
      (pg::session-set-application-name "wins")
      (pg::command-commit output)
      (ok (equal "wins" (pg::sql-setting-value "application_name")))
      (pg::command-begin output nil)
      (pg::session-set-application-name "aborted")
      (pg::abort-transaction-block)
      (ok (equal "wins" (pg::backend-application-name pg::*backend*)))
      (pg::command-rollback output)
      (ok (equal "wins" (pg::sql-setting-value "application_name")))
      (pg::session-set-application-name :default)
      (ok (equal "startup" (pg::backend-application-name pg::*backend*)))
      (ok (equal "client" (aref (pg::session-application-name-settings-row) 5)))
      (pg::session-set-application-name "reset-all")
      (pg::session-reset-all-settings)
      (ok (equal "startup" (pg::backend-application-name pg::*backend*)))
      (ok (getf (first (pg::parse-statements "SET LOCAL application_name='local'")) :local-p))
      (ok (intake-fails-p (lambda () (pg::parse-statements "SET LOCAL search_path=public"))))))

(deftest application-name-activity-slot-clips-the-expanded-guc
    (let ((pg::*backend* (pg::make-backend)))
      (pg::session-set-application-name (make-string 32 :initial-element #\é))
      (ok (= 248 (length (pg::sql-setting-value "application_name"))))
      (ok (= 63 (length (pg::backend-application-name pg::*backend*))))
      (ok (equal (subseq (pg::sql-setting-value "application_name") 0 63)
                 (pg::backend-application-name pg::*backend*)))))

(deftest application-name-finalization-survives-pending-reset-boundary-interrupt
    (dolist (commit-p '(t nil))
      (let ((pg::*backend* (pg::make-backend))
            (original (symbol-function 'pg::reset-backend-transaction))
            (caught nil))
        (pg::session-set-application-name "base")
        (pg::command-begin (make-broadcast-stream) nil)
        (pg::session-set-application-name "session")
        (pg::session-set-application-name "local" t)
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::reset-backend-transaction)
                     (lambda (backend)
                       (funcall original backend)
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (error "application-name reset boundary")))))
               (handler-case
                   (if commit-p (pg::commit-transaction) (pg::abort-transaction))
                 (simple-error (condition)
                   (setf caught (equal "application-name reset boundary" (princ-to-string condition)))))
               (ok caught)
               (ok (eq :idle (pg::backend-state pg::*backend*)))
               (ok (equal (if commit-p "session" "base")
                          (pg::backend-application-name pg::*backend*)))
               (ok (null (gethash :application-name-local (pg::backend-settings pg::*backend*))))
               (when commit-p
                 (ok (null (pg::backend-settings-transaction-start pg::*backend*)))))
          (setf (symbol-function 'pg::reset-backend-transaction) original)))))

(deftest implicit-query-block-whitelist-excludes-nontransactional-operations
    (ok (pg::implicit-query-block-statements-p
         (pg::parse-statements "SET LOCAL application_name='packet'; SELECT current_setting('application_name')")))
  (dolist (sql '("SELECT 1" "CREATE TABLE x (id integer); INSERT INTO x VALUES (1)"
                 "BEGIN; SELECT 1" "TRUNCATE x; SELECT 1" "CHECKPOINT; SELECT 1"
                 "VACUUM x; SELECT 1" "INSERT INTO x VALUES (9); SET TRANSACTION READ ONLY; SELECT 1"
                 "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED; SELECT 1"))
    (ok (not (pg::implicit-query-block-statements-p (pg::parse-statements sql))) sql)))

(deftest implicit-query-block-withholds-final-command-complete-on-commit-error
    (let ((pg::*backend* (pg::make-backend))
          (write-message (symbol-function 'pg::write-message))
          (commit (symbol-function 'pg::commit-transaction))
          (messages nil) (caught nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::write-message)
                   (lambda (stream type payload)
                     (declare (ignore stream payload))
                     (push type messages))
                   (symbol-function 'pg::commit-transaction)
                   (lambda () (error "injected packet commit failure")))
             (handler-case
                 (pg::execute-simple-query-statements
                  (make-broadcast-stream)
                  (pg::parse-statements "SET application_name='uncommitted'; SHOW application_name"))
               (simple-error (condition)
                 (setf caught (equal "injected packet commit failure" (princ-to-string condition)))))
             (ok caught)
             (ok (= 1 (count #\C messages)))
             (ok (member #\D messages))
             (ok (eq :idle (pg::backend-state pg::*backend*)))
             (ok (null (pg::backend-implicit-query-block pg::*backend*)))
             (ok (equal "" (pg::backend-application-name pg::*backend*)))
             (ok (null (pg::backend-settings-transaction-start pg::*backend*))))
        (setf (symbol-function 'pg::write-message) write-message
              (symbol-function 'pg::commit-transaction) commit))))

(deftest implicit-query-block-restores-all-settings-before-cleanup-interrupt
    (let ((pg::*backend* (pg::make-backend))
          (reset (symbol-function 'pg::reset-backend-transaction))
          (execute (symbol-function 'pg::execute-query-statements))
          (caught nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::execute-query-statements)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (setf (gethash "timezone" (pg::backend-settings pg::*backend*)) "Europe/Warsaw")
                     (error "packet body failure"))
                   (symbol-function 'pg::reset-backend-transaction)
                   (lambda (backend)
                     (funcall reset backend)
                     (sb-thread:interrupt-thread
                      sb-thread:*current-thread*
                      (lambda () (error "packet cleanup boundary")))))
             (handler-case
                 (pg::execute-simple-query-statements
                  (make-broadcast-stream) (pg::parse-statements "SHOW TimeZone; SHOW application_name"))
               (simple-error (condition)
                 (setf caught (equal "packet cleanup boundary" (princ-to-string condition)))))
             (ok caught)
             (ok (equal "UTC" (pg::sql-setting-value "timezone")))
             (ok (eq :idle (pg::backend-state pg::*backend*)))
             (ok (null (pg::backend-implicit-query-block pg::*backend*)))
             (ok (null (pg::backend-settings-transaction-start pg::*backend*))))
        (setf (symbol-function 'pg::reset-backend-transaction) reset
              (symbol-function 'pg::execute-query-statements) execute))))

(cl-spec:defproperty application-name-preserves-printable-ascii
    ((bits (range integer 0 4611686018427387903)) (size (range integer 0 63)))
  (:about pg::session-normalize-application-name)
  (:trials (:normal 200))
  (let ((name (map 'string (lambda (index) (code-char (+ 32 (mod (ash bits (- (mod index 50))) 95))))
                   (loop for index below size collect index))))
    (equal name (pg::session-normalize-application-name name))))

(deftest generated-application-name-printable-ascii
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'application-name-preserves-printable-ascii :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest session-startup-options-and-reset-preserve-client-baseline
    (let ((pg::*backend* (pg::make-backend)))
      (ok (equal '("-c" "custom.value=two words" "-cother.value=a\\b" "'literal'")
                 (pg::split-session-options "-c custom.value=two\\ words -cother.value=a\\\\b 'literal'")))
      (pg::session-apply-startup-options
       '(("options" . "-c statement_timeout=2s -c pg_stat_statements.track=none")))
      (ok (equal "2s" (pg::sql-setting-value "statement_timeout")))
      (ok (equal "none" (pg::sql-setting-value "pg_stat_statements.track")))
      (pg::session-set-extra-setting "test.unset" :default)
      (ok (equal "" (pg::sql-setting-value "test.unset")))
      (ok (equal "client" (aref (pg::session-timeout-settings-row) 5)))
      (pg::session-apply-startup-options '(("Statement_Timeout" . "2s")))
      (ok (equal "client" (aref (pg::session-timeout-settings-row) 5)))
      (pg::session-set-extra-setting "statement_timeout" "40ms")
      (ok (equal "40ms" (pg::sql-setting-value "statement_timeout")))
      (pg::session-set-extra-setting "statement_timeout" :default)
      (ok (equal "2s" (pg::sql-setting-value "statement_timeout")))
      (pg::session-set-extra-setting "statement_timeout" "100ms")
      (pg::session-reset-all-settings)
      (ok (equal "2s" (pg::sql-setting-value "statement_timeout")))
      (ok (intake-fails-p (lambda () (pg::session-set-extra-setting "statement_timeout" "-1"))))
      (ok (intake-fails-p (lambda () (pg::session-apply-startup-options '(("options" . "-c port=5434"))))))))

(cl-spec:defproperty session-options-roundtrip-backslash-escaped-tokens
    ((bits (range integer 0 4611686018427387903)) (size (range integer 1 40)))
  (:about pg::split-session-options)
  (:trials (:normal 200))
  (let* ((alphabet (map 'string #'code-char '(32 9 10 34 39 92 65 61 228)))
         (word (map 'string (lambda (index)
                              (char alphabet (mod (ash bits (- (mod index 50))) (length alphabet))))
                    (loop for index below size collect index)))
         (encoded (with-output-to-string (stream)
                    (loop for character across word
                          do (when (or (pg::sql-array-space-p character) (char= character #\\))
                               (write-char #\\ stream))
                             (write-char character stream)))))
    (equal (list "-c" word) (pg::split-session-options (concatenate 'string "-c " encoded)))))

(deftest generated-session-option-token-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'session-options-roundtrip-backslash-escaped-tokens :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest statement-timeout-safe-point-clears-expired-deadline
    (let ((pg::*backend* (pg::make-backend)))
      (setf (pg::backend-statement-deadline pg::*backend*) (1- (get-internal-real-time)))
      (ok (intake-fails-p #'pg::check-for-interrupts))
      (ok (null (pg::backend-statement-deadline pg::*backend*)))
      (pg::session-set-extra-setting "statement_timeout" "20ms")
      (pg::arm-statement-timeout)
      (ok (plusp (pg::backend-statement-deadline pg::*backend*)))
      (pg::clear-statement-timeout)
      (ok (null (pg::backend-statement-deadline pg::*backend*)))))

(deftest btree-lock-wait-observes-statement-timeout-without-leaking-ownership
    (let ((result nil) (thread nil) (relation 987654321))
      (pg::call-with-btree-relation-write-lock
       relation
       (lambda ()
         (setf thread
               (sb-thread:make-thread
                (lambda ()
                  (let ((pg::*backend* (pg::make-backend)))
                    (pg::session-set-extra-setting "statement_timeout" "40ms")
                    (pg::arm-statement-timeout)
                    (setf result
                          (intake-fails-p
                           (lambda ()
                             (pg::call-with-btree-relation-read-lock relation (lambda () nil)))))))))
         (sb-thread:join-thread thread :timeout 3 :default :timeout)
         (ok result)))
      (pg::call-with-btree-relation-write-lock relation (lambda () (ok t)))))

(deftest sql-setting-and-lower-functions-follow-c-semantics
    (let ((pg::*startup-server-settings* (pg::startup-default-settings "/tmp/data/")))
      (ok (equal "5min" (pg::eval-func "current_setting" '("CHECKPOINT_TIMEOUT"))))
      (ok (eq pg::+null+ (pg::eval-func "current_setting" '("missing.setting" t))))
      (ok (eq pg::+null+ (pg::eval-func "current_setting" (list pg::+null+))))
      (ok (eq pg::+null+ (pg::eval-func "current_setting" (list "port" pg::+null+))))
      (ok (intake-fails-p (lambda () (pg::eval-func "current_setting" '("missing.setting")))))
      ;; formatting.c uses ASCII-only lower for the supported C/POSIX collation.
      (ok (equal "port ÄΣ" (pg::eval-func "lower" '("PORT ÄΣ"))))
      (ok (eq pg::+null+ (pg::eval-func "lower" (list pg::+null+))))))

(deftest scalar-array-all-matches-c-empty-and-null-identities
    (flet ((value (sql)
             (let* ((ast (first (pg::parse-statements sql)))
                    (expression (getf (first (getf ast :targets)) :expr)))
               (pg::eval-expr (pg::analyze-expr expression nil) nil))))
      (ok (eq t (value "SELECT 1 = ANY('{1,2}')")))
      (ok (intake-fails-p (lambda () (value "SELECT 1 = ANY('{1,bad}')"))))
      (ok (intake-fails-p
           (lambda () (value "SELECT CASE WHEN false THEN 1 = ANY('{bad}') ELSE true END"))))
      (ok (intake-fails-p (lambda () (value "SELECT 1 = ANY('{1}'::text[])"))))
      (let ((long-name (make-string 64 :initial-element #\a)))
        (ok (eq t (value (format nil "SELECT '~a'::name = ANY('{~a}')"
                                 long-name long-name)))))
      (ok (eq t (value "SELECT 'r'::\"char\" = ANY('{rel}')")))
      (ok (eq t (value "SELECT 'port' != ALL(ARRAY['host','timeout'])")))
      (ok (eq nil (value "SELECT 'port' != ALL(ARRAY['host','port'])")))
      (ok (eq pg::+null+ (value "SELECT 'port' != ALL(ARRAY['host',NULL])")))
      (ok (eq nil (value "SELECT 'port' != ALL(ARRAY[NULL,'port'])")))
      (ok (eq t (value "SELECT NULL = ALL('{}'::text[])")))
      (ok (eq nil (value "SELECT NULL = ANY('{}'::text[])")))
      (ok (eq pg::+null+ (value "SELECT 'port' = ALL(NULL)")))
      (ok (eq pg::+null+ (value "SELECT 'port' = ANY(NULL)")))
      (ok (eq pg::+null+ (value "SELECT current_setting('missing', 'true')")))))

(cl-spec:defproperty text-array-input-roundtrips-output
    ((bits (range integer 0 4611686018427387903)) (size (range integer 0 40)))
  (:about pg::sql-text-array-input)
  (:trials (:normal 200))
  (let* ((alphabet (map 'string #'code-char '(32 9 10 34 92 123 125 44 78 85 76 65 228 931)))
         (text (map 'string (lambda (index)
                              (char alphabet (mod (ash bits (- (mod index 50)))
                                                  (length alphabet))))
                    (loop for index below size collect index)))
         (array (vector pg::+null+ "" "NULL" text)))
    (equalp array (pg::sql-text-array-input (pg::sql-text-array-output array)))))

(deftest generated-text-array-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'text-array-input-roundtrips-output :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest text-array-input-matches-token-escaping-and-rejects-malformed-values
    (ok (equalp (vector "a b" " c " "NULL" "NULL" pg::+null+)
                (pg::sql-text-array-input "{ a b ,\" c \",\"NULL\",N\\ULL,NULL}")))
  (dolist (text '("{,}" "{a,}" "{\"a\"b}" "{a\"b}" "{a" "{}trailing"))
    (ok (intake-fails-p (lambda () (pg::sql-text-array-input text))) text)))

(deftest max-locks-reload-preserves-installed-capacity-until-restart
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*installed-resource-parameters* nil)
             (pg::*managed-worker-slots* nil)
             (pg::*managed-worker-registration-closed-p* nil)
             (pg::*relation-lock-object-limit* nil) (pg::*relation-lock-owner-limit* nil))
        (write-startup-config configuration (format nil "max_locks_per_transaction=137~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (let ((installed (pg::startup-install-resource-capacity pg::*startup-server-settings*)))
          (write-startup-config configuration (format nil "max_locks_per_transaction=149~%"))
          (pg::startup-reload-settings)
          (let ((entry (gethash "max_locks_per_transaction" pg::*startup-server-settings*)))
            (ok (= 137 (getf entry :value)))
            (ok (= 149 (getf entry :desired-value)))
            (ok (getf entry :pending-restart)))
          (ok (eq installed pg::*installed-resource-parameters*))
          (ok (= 137 (pg::resource-parameters-locks installed)))
          (ok (= (* 137 136) pg::*relation-lock-object-limit*))
          (ok (= (* 2 137 136) pg::*relation-lock-owner-limit*))
          (write-startup-config configuration (format nil "max_locks_per_transaction=137~%"))
          (pg::startup-reload-settings)
          (ok (not (getf (gethash "max_locks_per_transaction" pg::*startup-server-settings*)
                         :pending-restart)))))))

(deftest max-locks-setting-sizes-the-shared-pool-with-c-bounds
    (let ((settings (pg::startup-default-settings "/tmp/lock-setting-test/")))
      (ok (= 128 (pg::startup-setting-value settings "max_locks_per_transaction")))
      (ok (= 10 (pg::startup-normalize-setting "max_locks_per_transaction" "10")))
      (ok (= 2147483647 (pg::startup-normalize-setting "max_locks_per_transaction" "2147483647")))
      (dolist (value '("9" "2147483648" "-1"))
        (ok (intake-fails-p
             (lambda () (pg::startup-normalize-setting "max_locks_per_transaction" value)))))
      (setf (gethash "max_locks_per_transaction" settings) (list :value 137 :source :command-line))
      (multiple-value-bind (parameters objects owners) (pg::startup-resource-capacity-plan settings)
        (ok (= 137 (pg::resource-parameters-locks parameters)))
        (ok (= (* 137 136) objects))
        (ok (= (* 2 objects) owners)))
      (ok (equal "55P02" (recovery-state-of
                          (lambda () (pg::startup-check-session-setting "max_locks_per_transaction")))))))

(deftest max-workers-setting-matches-c-bounds-and-startup-context
    (let ((settings (pg::startup-default-settings "/tmp/worker-setting-test/")))
      (ok (equal 8 (pg::startup-setting-value settings "max_worker_processes")))
      (ok (= 0 (pg::startup-normalize-setting "max_worker_processes" "0")))
      (ok (= 262143 (pg::startup-normalize-setting "max_worker_processes" "262143")))
      (dolist (value '("-1" "262144"))
        (ok (intake-fails-p
             (lambda () (pg::startup-normalize-setting "max_worker_processes" value)))))
      (ok (equal "55P02"
                 (recovery-state-of
                  (lambda () (pg::startup-check-session-setting "max_worker_processes")))))
      (setf (gethash "max_worker_processes" settings) (list :value 262143 :source :command-line))
      (ok (intake-fails-p (lambda () (pg::startup-resource-capacity-plan settings))))))

(deftest max-workers-reload-preserves-registrations-and-capacity-until-restart
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*installed-resource-parameters* nil)
             (pg::*managed-worker-slots* nil)
             (pg::*managed-worker-registration-closed-p* nil)
             (pg::*relation-lock-object-limit* nil) (pg::*relation-lock-owner-limit* nil))
        (write-startup-config configuration (format nil "max_worker_processes=1~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (let* ((installed (pg::startup-install-resource-capacity pg::*startup-server-settings*))
               (slots pg::*managed-worker-slots*)
               (handle (pg::managed-worker-register (lambda () :reserved))))
          (unwind-protect
               (progn
                 (write-startup-config configuration (format nil "max_worker_processes=2~%"))
                 (pg::startup-reload-settings)
                 (let ((entry (gethash "max_worker_processes" pg::*startup-server-settings*)))
                   (ok (= 1 (getf entry :value)))
                   (ok (= 2 (getf entry :desired-value)))
                   (ok (getf entry :pending-restart)))
                 (let ((row (find "max_worker_processes" (pg::startup-settings-rows)
                                  :key (lambda (row) (aref row 0)) :test #'equal)))
                   (ok (equal "1" (pg::startup-setting-show-value "max_worker_processes")))
                   (ok (equal "1" (aref row 1)))
                   (ok (equal "postmaster" (aref row 3)))
                   (ok (equal "integer" (aref row 4)))
                   (ok (aref row 9)))
                 (ok (eq installed pg::*installed-resource-parameters*))
                 (ok (eq slots pg::*managed-worker-slots*))
                 (ok (= 1 (length slots)))
                 (ok (eq :registered (pg::managed-worker-status handle)))
                 (ok (null (pg::managed-worker-register (lambda () :overflow))))
                 (ok (= (* 128 129) pg::*relation-lock-object-limit*))
                 (let ((pg::*installed-resource-parameters* nil)
                       (pg::*managed-worker-slots* nil))
                   (let* ((settings (pg::startup-load-settings
                                     pg::*startup-native-inputs* pg::*startup-command-overrides*))
                          (restarted (pg::startup-install-resource-capacity settings)))
                     (ok (= 2 (pg::resource-parameters-workers restarted)))
                     (ok (= 2 (length pg::*managed-worker-slots*))))))
            (pg::managed-worker-request-stop handle))))))

(cl-spec:defproperty worker-setting-sizes-installed-resource-plan
    ((workers (range integer 0 512)))
  (:about pg::startup-resource-capacity-plan)
  (:trials (:normal 200))
  (let ((settings (pg::startup-default-settings "/tmp/worker-plan-test/")))
    (setf (gethash "max_worker_processes" settings)
          (list :value workers :source :command-line))
    (multiple-value-bind (parameters objects owners)
        (pg::startup-resource-capacity-plan settings)
      (and (= workers (pg::resource-parameters-workers parameters))
           (= objects (* 128 (+ 100 workers 10 16 2)))
           (= owners (* 2 objects))))))

(deftest generated-worker-setting-resource-plan
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'worker-setting-sizes-installed-resource-plan :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest disabled-feature-defaults-match-c-metadata-and-context
    (let ((settings (pg::startup-default-settings "/tmp/disabled-features/")))
      (ok (equal 0 (pg::startup-setting-value settings "max_prepared_transactions")))
      (ok (assoc "track_commit_timestamp" pg::*startup-setting-specs* :test #'equal))
      (ok (not (pg::startup-setting-value settings "track_commit_timestamp")))
      (ok (= 262143 (pg::startup-normalize-setting "max_prepared_transactions" "262143")))
      (dolist (value '("-1" "262144"))
        (ok (intake-fails-p
             (lambda () (pg::startup-normalize-setting "max_prepared_transactions" value)))))
      (dolist (name '("recovery_target" "recovery_target_lsn" "recovery_target_name"
                      "recovery_target_time" "recovery_target_xid"))
        (ok (equal "" (pg::startup-setting-value settings name)))
        (ok (equal "" (pg::startup-normalize-setting name ""))))
      (dolist (name '("max_prepared_transactions" "track_commit_timestamp" "recovery_target"
                      "recovery_target_lsn" "recovery_target_name" "recovery_target_time"
                      "recovery_target_xid"))
        (ok (equal "55P02"
                   (recovery-state-of
                    (lambda () (pg::startup-check-session-setting name))))))))

(deftest disabled-features-check-final-values-after-configuration-precedence
    (with-physical-store-directory (root)
      (let ((configuration (merge-pathnames "postgresql.conf" root))
            (included (merge-pathnames "included.conf" root)))
        (dolist (setting '(("max_prepared_transactions" "1" "0")
                           ("track_commit_timestamp" "on" "off")
                           ("recovery_target" "immediate" "")
                           ("recovery_target_lsn" "0/10" "")
                           ("recovery_target_name" "stop_here" "")
                           ("recovery_target_time" "2026-10-30 00:00:00+00" "")
                           ("recovery_target_xid" "42" "")))
          (destructuring-bind (name enabled disabled) setting
            (write-startup-config included (format nil "~a='~a'~%" name enabled))
            (write-startup-config configuration
                                  (format nil "include='included.conf'~%~a='~a'~%" name disabled))
            (multiple-value-bind (native overrides)
                (pg::startup-cli-inputs (list "-D" (namestring root)))
              (let ((settings (pg::startup-load-settings native overrides)))
                (ok (equal (pg::startup-normalize-setting name disabled)
                           (pg::startup-setting-value settings name))))
              (write-startup-config configuration (format nil "include='included.conf'~%"))
              (ok (intake-fails-p
                   (lambda () (pg::startup-load-settings native overrides))))
              (let ((auto (merge-pathnames "postgresql.auto.conf" root)))
                (write-startup-config auto (format nil "~a='~a'~%" name disabled))
                (ok (equal (pg::startup-normalize-setting name disabled)
                           (pg::startup-setting-value (pg::startup-load-settings native overrides) name)))
                (write-startup-config auto (format nil "~a='~a'~%" name enabled))
                (multiple-value-bind (command-native command-overrides)
                    (pg::startup-cli-inputs (list "-D" (namestring root)
                                                  "-c" (format nil "~a=~a" name disabled)))
                  (ok (equal (pg::startup-normalize-setting name disabled)
                             (pg::startup-setting-value
                              (pg::startup-load-settings command-native command-overrides) name))))
                (delete-file auto))))))))

(deftest unsupported-feature-reload-preserves-the-entire-installed-configuration
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*installed-resource-parameters* nil)
             (pg::*managed-worker-slots* nil)
             (pg::*managed-worker-registration-closed-p* nil)
             (pg::*relation-lock-object-limit* nil) (pg::*relation-lock-owner-limit* nil))
        (write-startup-config configuration (format nil "checkpoint_timeout=90~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (pg::startup-install-resource-capacity pg::*startup-server-settings*)
        (let ((active pg::*startup-server-settings*)
              (installed pg::*installed-resource-parameters*)
              (slots pg::*managed-worker-slots*))
          (dolist (assignment '("max_prepared_transactions=1"
                                "track_commit_timestamp=on"
                                "recovery_target='immediate'"
                                "recovery_target_lsn='0/10'"
                                "recovery_target_name='stop_here'"
                                "recovery_target_time='2026-10-30 00:00:00+00'"
                                "recovery_target_xid='42'"))
            (write-startup-config configuration (format nil "checkpoint_timeout=120~%~a~%" assignment))
            (ok (intake-fails-p #'pg::startup-reload-settings))
            (ok (eq active pg::*startup-server-settings*))
            (ok (eq installed pg::*installed-resource-parameters*))
            (ok (eq slots pg::*managed-worker-slots*))
            (ok (= 90 (pg::startup-setting-value active "checkpoint_timeout")))
            (ok (zerop (pg::resource-parameters-prepared installed))))))))

(deftest direct-resource-planning-cannot-enable-unsupported-features
    (dolist (setting '(("max_prepared_transactions" . "1")
                       ("track_commit_timestamp" . "on")
                       ("recovery_target" . "immediate")
                       ("recovery_target_lsn" . "0/10")
                       ("recovery_target_name" . "stop_here")
                       ("recovery_target_time" . "2026-10-30 00:00:00+00")
                       ("recovery_target_xid" . "42")))
      (let ((settings (pg::startup-default-settings "/tmp/direct-feature-plan/")))
        (setf (gethash (car setting) settings)
              (list :value (pg::startup-normalize-setting (car setting) (cdr setting))
                    :source :command-line))
        (ok (intake-fails-p (lambda () (pg::startup-resource-capacity-plan settings)))))))

(deftest wal-settings-match-c-enum-aliases-defaults-and-context
    (let* ((pg::*startup-server-settings* (pg::startup-default-settings "/tmp/wal-settings/"))
           (pg::*startup-native-inputs* (make-hash-table :test #'equal)))
      (ok (equal "replica" (pg::startup-setting-value pg::*startup-server-settings* "wal_level")))
      (ok (not (pg::startup-setting-value pg::*startup-server-settings* "wal_log_hints")))
      (dolist (input '("replica" "REPLICA" "archive" "hot_standby"))
        (ok (equal "replica" (pg::startup-normalize-setting "wal_level" input))))
      (ok (equal "logical" (pg::startup-normalize-setting "wal_level" "LoGiCaL")))
      (ok (pg::startup-normalize-setting "wal_log_hints" "on"))
      (dolist (input '("" "archivejunk" "invalid"))
        (ok (intake-fails-p (lambda () (pg::startup-normalize-setting "wal_level" input)))))
      (let ((row (find "wal_level" (pg::startup-settings-rows)
                       :key (lambda (row) (aref row 0)) :test #'equal)))
        (progn (ok (equal "enum" (aref row 4)))
               (ok (equalp #("minimal" "replica" "logical")
                           (pg::sql-array-elements (aref row 6))))))
      (dolist (name '("wal_level" "wal_log_hints"))
        (ok (equal "55P02"
                   (recovery-state-of
                    (lambda () (pg::startup-check-session-setting name))))))
      (setf (gethash "wal_level" pg::*startup-server-settings*)
            (list :value "minimal" :source :command-line))
      (ok (intake-fails-p
           (lambda () (pg::startup-resource-capacity-plan pg::*startup-server-settings*))))))

(deftest startup-settings-catalog-keeps-base-units-and-active-provenance
    (let* ((pg::*startup-server-settings* (pg::startup-default-settings "/tmp/data/"))
           (pg::*startup-native-inputs* (make-hash-table :test #'equal)))
      (setf (gethash "checkpoint_timeout" pg::*startup-server-settings*)
            (list :value 90 :source :file :sourcefile "/tmp/include.conf" :sourceline 7)
            (gethash "port" pg::*startup-server-settings*)
            (list :value 5433 :source :command-line :pending-restart t :desired-value 5434))
      (let* ((rows (pg::startup-settings-rows))
             (timeout (find "checkpoint_timeout" rows :key (lambda (row) (aref row 0)) :test #'equal))
             (port (find "port" rows :key (lambda (row) (aref row 0)) :test #'equal)))
        (ok (equalp timeout (vector "checkpoint_timeout" "90" "s" "sighup" "integer"
                                    "configuration file" pg::+null+ "/tmp/include.conf" 7 nil)))
        (ok (equal "5433" (aref port 1)))
        (ok (eq pg::+null+ (aref port 7)))
        (ok (aref port 9))
        (ok (equal "90s" (pg::startup-setting-show-value "checkpoint_timeout")))
        (ok (equal '(1 2 3 7 8 9 12 15 16 17)
                   (mapcar #'fourth pg::+sql-settings-fields+))))))

(deftest startup-removing-file-postmaster-default-still-needs-restart
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*physical-restartpoint-timeout* 300)
             (pg::*physical-restartpoint-wal-bytes* 1073741824)
             (pg::*max-slot-wal-keep-bytes* 1073741824)
             (pg::*wal-slot-retention-policy* nil)
             (pg::*max-replication-slots* 10))
        (write-startup-config configuration (format nil "port=5433~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (write-startup-config configuration "")
        (pg::startup-reload-settings)
        (let ((entry (gethash "port" pg::*startup-server-settings*)))
          (ok (= 5433 (getf entry :value)))
          (ok (eq :file (getf entry :source)))
          (ok (getf entry :pending-restart)))
        (write-startup-config configuration (format nil "port=5433~%"))
        (pg::startup-reload-settings)
        (ok (not (getf (gethash "port" pg::*startup-server-settings*) :pending-restart))))))

(deftest startup-arguments-consume-values-and-reject-unknown-input
    (multiple-value-bind (native overrides)
        (pg::startup-cli-inputs '("-D" "--directory-is-a-value" "-p5455" "-cport=5456"))
      (ok (equal "--directory-is-a-value" (gethash "pgdata" native)))
      (ok (equal "5456" (gethash "port" overrides))))
  (dolist (args '(("--typo=1") ("--bad" "value") ("unexpected") ("-p") ("-c" "port")))
    (ok (intake-fails-p (lambda () (pg::startup-cli-inputs args)))))
  (multiple-value-bind (native overrides)
      (pg::startup-cli-inputs '("-c" "config_file=first" "--config-file=second"))
    (declare (ignore overrides))
    (ok (equal "second" (gethash "config_file" native)))))

(deftest startup-config-output-distinguishes-settings-from-the-legacy-control-port
    (dolist (args '(("-C" "port") ("-Cport")))
      (multiple-value-bind (native overrides) (pg::startup-cli-inputs args)
        (declare (ignore overrides))
        (ok (equal "port" (gethash "config-output" native)))
        (ok (null (gethash "control-port" native)))))
  (multiple-value-bind (native overrides) (pg::startup-cli-inputs '("-C" "6433"))
    (declare (ignore overrides))
    (ok (equal "6433" (gethash "control-port" native)))
    (ok (null (gethash "config-output" native))))
  (dolist (args '(("-C" "port" "-C" "6433") ("-Cport" "-C6433")))
    (multiple-value-bind (native overrides) (pg::startup-cli-inputs args)
      (declare (ignore overrides))
      (ok (equal "6433" (gethash "control-port" native)))
      (ok (null (gethash "config-output" native)))))
  (dolist (args '(("-C" "6433" "-C" "port") ("-C6433" "-Cport")))
    (multiple-value-bind (native overrides) (pg::startup-cli-inputs args)
      (declare (ignore overrides))
      (ok (equal "port" (gethash "config-output" native)))
      (ok (null (gethash "control-port" native)))))
  (multiple-value-bind (native overrides) (pg::startup-cli-inputs '("--control-port" "6434"))
    (declare (ignore overrides))
    (ok (equal "6434" (gethash "control-port" native)))))

(deftest startup-config-output-uses-file-auto-and-command-line-precedence
    (with-physical-store-directory (root)
      (let* ((configuration (merge-pathnames "postgresql.conf" root))
             (auto (merge-pathnames "postgresql.auto.conf" root))
             (pg::*startup-server-settings* :unchanged)
             (pg::*pgdata* :unchanged)
             (args (list "-D" (namestring root))))
        (write-startup-config configuration (format nil "port=5540~%checkpoint_timeout='90s'~%"))
        (write-startup-config auto (format nil "port=5541~%"))
        (ok (equal "5541" (pg::startup-configuration-output (append args '("-C" "port")))))
        (ok (equal "5542" (pg::startup-configuration-output
                           (append args '("-c" "port=5542" "-C" "PORT")))))
        (ok (equal "90" (pg::startup-configuration-output
                         (append args '("-C" "checkpoint_timeout")))))
        (ok (equal "" (pg::startup-configuration-output (append args '("-C" "restore_command")))))
        (ok (equal (string-right-trim "/" (namestring root))
                   (pg::startup-configuration-output (append args '("-C" "data_directory")))))
        (ok (intake-fails-p
             (lambda () (pg::startup-configuration-output (append args '("-C" "unknown_setting"))))))
        (ok (eq :unchanged pg::*startup-server-settings*))
        (ok (eq :unchanged pg::*pgdata*))
        (ok (null (pg::startup-configuration-output (append args '("--control-port" "6433")))))
        (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))))

(deftest startup-config-output-does-not-create-a-missing-data-directory
    (with-physical-store-directory (root)
      (let* ((missing (merge-pathnames "absent/" root))
             (args (list "-D" (namestring missing) "-C" "port")))
        (ok (intake-fails-p (lambda () (pg::startup-configuration-output args))))
        (ok (not (probe-file missing))))))

(cl-spec:defproperty signed-binary-prefix-preserves-int32-values
    ((number (range integer -2147483648 2147483647)))
  (:about pg::startup-numeric-prefix)
  (:trials (:normal 200))
  (multiple-value-bind (parsed unit)
      (pg::startup-numeric-prefix (format nil "~:[0b~;-0b~]~b" (minusp number) (abs number)))
    (and (= parsed number) (string= unit ""))))

(deftest generated-signed-binary-prefix-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'signed-binary-prefix-preserves-int32-values :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest signed-integer-gucs-accept-c23-binary-prefixes
    (dolist (case '(("max_connections" "0b1100100" 100)
                    ("max_connections" "0b10000000" 128)
                    ("max_connections" "+0B1010" 10)
                    ("max_slot_wal_keep_size" "-0b1" -1)
                    ("checkpoint_timeout" "0b11110s" 30)
                    ("max_wal_size" "0b10GB" 2048)))
      (destructuring-bind (name text expected) case
        (ok (= expected (pg::startup-normalize-setting name text)))))
  (dolist (text (list "0b" "0b2" "0b10e1" "0b10.5"
                      (concatenate 'string "0b" (make-string 80 :initial-element #\1))))
    (ok (intake-fails-p
         (lambda () (pg::startup-normalize-setting "max_connections" text))))))

(deftest startup-integer-conversion-matches-pinned-c-rounding
    (dolist (case '(("port" "0x1538" 5432) ("port" "012470" 5432)
                    ("port" "5432.5" 5432) ("port" "5433.5" 5434)
                    ("port" "1e3" 1000) ("checkpoint_timeout" "30.5001s" 30)
                    ("checkpoint_timeout" "1.5min" 90)
                    ("max_wal_size" "1.4998MB" 2) ("max_wal_size" "1.1GB" 1126)
                    ("max_slot_wal_keep_size" "-1" -1)))
      (destructuring-bind (name text expected) case
        (ok (= expected (pg::startup-normalize-setting name text)))))
  (dolist (case '(("port" "08") ("port" "65536") ("checkpoint_timeout" "29s")
                  ("max_wal_size" "1MB") ("max_wal_size" "1gb") ("port" "NaN")))
    (destructuring-bind (name text) case
      (ok (intake-fails-p (lambda () (pg::startup-normalize-setting name text)))))))

(deftest cluster-name-clips-startup-bytes-before-escaping-nonprintable-bytes
    (dolist (case (list (list "" "") (list "research" "research")
                        (list (make-string 70 :initial-element #\a)
                              (make-string 63 :initial-element #\a))
                        (list (concatenate 'string (make-string 62 :initial-element #\a) "é")
                              (concatenate 'string (make-string 62 :initial-element #\a) "\\xc3"))
                        (list (concatenate 'string (make-string 61 :initial-element #\a) "é")
                              (concatenate 'string (make-string 61 :initial-element #\a) "\\xc3\\xa9"))
                        (list (string #\Newline) "\\x0a")))
      (destructuring-bind (input expected) case
        (ok (equal expected (pg::startup-normalize-setting "cluster_name" input)))))
  (ok (eq :postmaster (second (assoc "cluster_name" pg::*startup-setting-specs* :test #'equal)))) )

(cl-spec:defproperty cluster-name-preserves-printable-ascii-within-name-bound
    ((size (range integer 0 100)) (byte (range integer 32 126)))
  (:about pg::startup-normalize-setting)
  (:trials (:normal 200))
  (let ((input (make-string size :initial-element (code-char byte))))
    (equal (subseq input 0 (min size 63))
           (pg::startup-normalize-setting "cluster_name" input))))

(deftest generated-cluster-name-ascii-normalization
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'cluster-name-preserves-printable-ascii-within-name-bound
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest startup-config-literals-and-escapes-do-not-use-the-lisp-reader
    (multiple-value-bind (name value)
        (pg::startup-config-line "listen_addresses = '\\06127.0.0.1' # comment")
      (ok (equal "listen_addresses" name))
      (ok (equal "127.0.0.1" (pg::startup-input-text value))))
  (multiple-value-bind (name value)
      (pg::startup-config-line "data_directory 'a''b\\\\c#literal'")
    (declare (ignore name))
    (ok (equal "a'b\\c#literal" (pg::startup-input-text value))))
  (multiple-value-bind (name value)
      (pg::startup-config-line "data_directory = '#.(error \"not Lisp\")'")
    (declare (ignore name))
    (ok (equal "#.(error \"not Lisp\")" (pg::startup-input-text value))))
  (dolist (line '("port =" "port = 1 2" "port = 'unterminated"
                  "listen_addresses = *" "listen_addresses = 127.0.0.1"))
    (ok (intake-fails-p (lambda () (pg::startup-config-line line))))))

(deftest startup-config-octal-escapes-preserve-utf8-octets
    (multiple-value-bind (name value)
        (pg::startup-config-line "cluster_name='\\303\\251'")
      (ok (equal "cluster_name" name))
      (ok (equal "é" (pg::startup-input-text value))))
  (with-physical-store-directory (root)
    (let ((configuration (merge-pathnames "postgresql.conf" root))
          (table (make-hash-table :test #'equal)))
      (write-startup-config configuration "cluster_name='\\303\\251'")
      (pg::startup-read-config configuration table)
      (ok (equal "\\xc3\\xa9" (pg::startup-setting-value table "cluster_name"))))))

(deftest startup-config-octal-consumption-wraps-bytes-and-uses-c-string-nul-boundary
    (multiple-value-bind (name value) (pg::startup-config-line "cluster_name='\\7779'")
      (declare (ignore name))
      (ok (typep value '(vector (unsigned-byte 8))))
      (ok (equalp #(255 57) value))
      (ok (equal "\\xff9" (pg::startup-normalize-setting "cluster_name" value))))
  (multiple-value-bind (name value) (pg::startup-config-line "cluster_name='\\000'")
    (declare (ignore name))
    (ok (zerop (length value)))
    (ok (equal "" (pg::startup-normalize-setting "cluster_name" value)))))

(deftest startup-config-unicode-includes-resolve-literal-and-octal-bytes
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "postgresql.conf" root)))
        (write-startup-config (merge-pathnames "é.conf" root) "cluster_name='included'")
        (dolist (line '("include='é.conf'" "include='\\303\\251.conf'"))
          (write-startup-config path line)
          (let ((table (make-hash-table :test #'equal)))
            (pg::startup-read-config path table)
            (ok (equal "included" (pg::startup-setting-value table "cluster_name")))))
        (write-startup-config path "include='\\377'")
        (ok (intake-fails-p
             (lambda () (pg::startup-read-config path (make-hash-table :test #'equal))))))))

(deftest startup-includes-auto-conf-and-cli-have-ordered-precedence
    (with-physical-store-directory (root)
      (let* ((configuration (merge-pathnames "postgresql.conf" root))
             (included (merge-pathnames "extra" root))
             (directory (merge-pathnames "conf.d/" root))
             (data (merge-pathnames "data/" root)))
        (ensure-directories-exist (merge-pathnames "placeholder" data))
        (write-startup-config included (format nil "port = 5455~%"))
        (write-startup-config (merge-pathnames "10.conf" directory) (format nil "port = 5456~%"))
        (write-startup-config (merge-pathnames "20.conf" directory) (format nil "port = 5457~%"))
        (write-startup-config (merge-pathnames ".hidden.conf" directory) "not_valid")
        (write-startup-config (merge-pathnames "ignored.txt" directory) "not_valid")
        (write-startup-config configuration
                              (format nil "port=5454~%include='extra'~%include_dir='conf.d'~%data_directory='~a'~%port=5458~%"
                                      (namestring data)))
        (write-startup-config (merge-pathnames "postgresql.auto.conf" data) (format nil "port=5459~%"))
        (multiple-value-bind (native overrides)
            (pg::startup-cli-inputs (list "-D" (namestring root) "--port=5460"))
          (let ((settings (pg::startup-load-settings native overrides)))
            (ok (= 5460 (pg::startup-setting-value settings "port")))
            (ok (eq :command-line (getf (gethash "port" settings) :source))))
          (remhash "port" overrides)
          (let ((settings (pg::startup-load-settings native overrides)))
            (ok (= 5459 (pg::startup-setting-value settings "port")))
            (ok (equal (namestring (merge-pathnames "postgresql.auto.conf" data))
                       (getf (gethash "port" settings) :sourcefile))))))))

(deftest startup-config-symlink-includes-stay-relative-to-the-config-name
    (with-physical-store-directory (root)
      (let* ((configuration (merge-pathnames "postgresql.conf" root))
             (real (merge-pathnames "shared/actual.conf" root))
             (table (make-hash-table :test #'equal)))
        (write-startup-config real (format nil "include='extra'~%"))
        (write-startup-config (merge-pathnames "extra" root) (format nil "port=5455~%"))
        (write-startup-config (merge-pathnames "shared/extra" root) (format nil "port=9999~%"))
        (sb-posix:symlink (namestring real) (namestring configuration))
        (pg::startup-read-config configuration table)
        (ok (= 5455 (pg::startup-setting-value table "port"))))))

(deftest startup-config-fails-closed-on-missing-unsupported-or-cyclic-input
    (with-physical-store-directory (root)
      (let ((configuration (merge-pathnames "postgresql.conf" root))
            (table (make-hash-table :test #'equal)))
        (ok (intake-fails-p (lambda () (pg::startup-read-config configuration table))))
        (write-startup-config configuration (format nil "include='postgresql.conf'~%"))
        (ok (intake-fails-p (lambda () (pg::startup-read-config configuration table))))
        (write-startup-config configuration (format nil "unsupported_future_setting='must-not-be-ignored'~%"))
        (ok (intake-fails-p (lambda () (pg::startup-read-config configuration table))))
        (write-startup-config configuration (format nil "include_if_exists='missing'~%port=5455~%"))
        (pg::startup-read-config configuration table)
        (ok (= 5455 (pg::startup-setting-value table "port"))))))

(deftest startup-reload-keeps-postmaster-settings-and-applies-valid-sighup-values
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*physical-restartpoint-timeout* 300)
             (pg::*physical-restartpoint-wal-bytes* 1073741824)
             (pg::*max-slot-wal-keep-bytes* 1073741824)
             (pg::*wal-slot-retention-policy* nil)
             (pg::*max-replication-slots* 10))
        (write-startup-config configuration (format nil "port=5454~%cluster_name='original cluster'~%checkpoint_timeout='30s'~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (write-startup-config configuration (format nil "port=5455~%cluster_name='replacement cluster'~%checkpoint_timeout='90s'~%"))
        (pg::startup-reload-settings)
        (ok (= 5454 (pg::startup-setting-value pg::*startup-server-settings* "port")))
        (ok (getf (gethash "port" pg::*startup-server-settings*) :pending-restart))
        (ok (= 90 pg::*physical-restartpoint-timeout*))
        (ok (equal "original cluster" (pg::startup-setting-show-value "cluster_name")))
        (ok (equal "replacement cluster"
                   (pg::startup-configuration-output
                    (list "-D" (namestring root) "-C" "cluster_name"))))
        (let ((row (pg::startup-settings-row
                    "cluster_name" (gethash "cluster_name" pg::*startup-server-settings*))))
          (ok (equal "original cluster" (aref row 1)))
          (ok (equal "postmaster" (aref row 3)))
          (ok (aref row 9)))
        (let ((installed pg::*startup-server-settings*))
          (write-startup-config configuration
                                (format nil "checkpoint_timeout='120s'~%data_directory='\\377'~%"))
          (ok (intake-fails-p (lambda () (pg::startup-reload-settings))))
          (ok (eq installed pg::*startup-server-settings*))
          (ok (= 90 pg::*physical-restartpoint-timeout*)))
        (write-startup-config configuration (format nil "checkpoint_timeout='120s'~%unknown_parameter=1~%"))
        (ok (intake-fails-p (lambda () (pg::startup-reload-settings))))
        (ok (= 90 pg::*physical-restartpoint-timeout*))
        (ok (= 5454 (pg::startup-setting-value pg::*startup-server-settings* "port"))))))

(deftest wal-keep-reload-installs-only-valid-settings-and-exposes-provenance
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*wal-keep-size-mb* 0)
             (pg::*physical-restartpoint-timeout* 300)
             (pg::*physical-restartpoint-wal-bytes* 1073741824)
             (pg::*max-slot-wal-keep-bytes* 1073741824)
             (pg::*wal-slot-retention-policy* nil)
             (pg::*max-replication-slots* 10)
             (pg::*max-connections* 100) (pg::*max-wal-senders* 10))
        (write-startup-config configuration (format nil "wal_keep_size='1GB'~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (ok (= 1024 pg::*wal-keep-size-mb*))
        (ok (equal "1GB" (pg::startup-setting-show-value "wal_keep_size")))
        (ok (equal "1024" (pg::startup-configuration-output
                           (list "-D" (namestring root) "-C" "wal_keep_size"))))
        (write-startup-config configuration (format nil "wal_keep_size='17MB'~%"))
        (pg::startup-reload-settings)
        (ok (= 17 pg::*wal-keep-size-mb*))
        (let* ((entry (gethash "wal_keep_size" pg::*startup-server-settings*))
               (row (pg::startup-settings-row "wal_keep_size" entry)))
          (ok (equal "17" (aref row 1)))
          (ok (equal "MB" (aref row 2)))
          (ok (equal "sighup" (aref row 3)))
          (ok (equal "integer" (aref row 4)))
          (ok (not (getf entry :pending-restart))))
        (ok (intake-fails-p (lambda () (pg::startup-check-session-setting "wal_keep_size"))))
        (write-startup-config configuration (format nil "wal_keep_size=-1~%"))
        (ok (intake-fails-p (lambda () (pg::startup-reload-settings))))
        (ok (= 17 pg::*wal-keep-size-mb*))
        (write-startup-config configuration "")
        (pg::startup-reload-settings)
        (ok (= 0 pg::*wal-keep-size-mb*)))))
