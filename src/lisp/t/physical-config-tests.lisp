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
        (ok (equalp timeout #("checkpoint_timeout" "90" "s" "sighup" "integer"
                              "configuration file" "/tmp/include.conf" 7 nil)))
        (ok (equal "5433" (aref port 1)))
        (ok (eq pg::+null+ (aref port 6)))
        (ok (aref port 8))
        (ok (equal "90s" (pg::startup-setting-show-value "checkpoint_timeout")))
        (ok (equal '(1 2 3 7 8 9 15 16 17)
                   (mapcar #'fourth pg::+sql-settings-fields+))))))

(deftest startup-removing-file-postmaster-default-still-needs-restart
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (configuration (merge-pathnames "postgresql.conf" root))
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*physical-restartpoint-timeout* 300)
             (pg::*physical-restartpoint-wal-bytes* 1073741824)
             (pg::*max-slot-wal-keep-bytes* 1073741824))
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

(deftest startup-config-literals-and-escapes-do-not-use-the-lisp-reader
    (multiple-value-bind (name value)
        (pg::startup-config-line "listen_addresses = '\\06127.0.0.1' # comment")
      (ok (equal "listen_addresses" name))
      (ok (equal "127.0.0.1" value)))
  (multiple-value-bind (name value)
      (pg::startup-config-line "data_directory 'a''b\\\\c#literal'")
    (declare (ignore name))
    (ok (equal "a'b\\c#literal" value)))
  (multiple-value-bind (name value)
      (pg::startup-config-line "data_directory = '#.(error \"not Lisp\")'")
    (declare (ignore name))
    (ok (equal "#.(error \"not Lisp\")" value)))
  (dolist (line '("port =" "port = 1 2" "port = 'unterminated"
                  "listen_addresses = *" "listen_addresses = 127.0.0.1"))
    (ok (intake-fails-p (lambda () (pg::startup-config-line line))))))

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
        (write-startup-config configuration (format nil "hba_file='must-not-be-ignored'~%"))
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
             (pg::*max-slot-wal-keep-bytes* 1073741824))
        (write-startup-config configuration (format nil "port=5454~%checkpoint_timeout='30s'~%"))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (write-startup-config configuration (format nil "port=5455~%checkpoint_timeout='90s'~%"))
        (pg::startup-reload-settings)
        (ok (= 5454 (pg::startup-setting-value pg::*startup-server-settings* "port")))
        (ok (getf (gethash "port" pg::*startup-server-settings*) :pending-restart))
        (ok (= 90 pg::*physical-restartpoint-timeout*))
        (write-startup-config configuration (format nil "checkpoint_timeout='120s'~%unknown_parameter=1~%"))
        (ok (intake-fails-p (lambda () (pg::startup-reload-settings))))
        (ok (= 90 pg::*physical-restartpoint-timeout*))
        (ok (= 5454 (pg::startup-setting-value pg::*startup-server-settings* "port"))))))
