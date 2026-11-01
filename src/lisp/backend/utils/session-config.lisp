;;;; Session settings, transaction undo and client parameter reporting.
;;;; Transaction cleanup changes in-memory state; wire reports are sent later.

;;;; Session startup options and USERSET values; no extension execution is implied.
(in-package #:pg)

(defun session-custom-setting-p (name)
  (let ((parts (uiop:split-string name :separator ".")))
    (and (> (length parts) 1)
         (every (lambda (part)
                  (and (plusp (length part))
                       (or (alpha-char-p (char part 0)) (char= (char part 0) #\_))
                       (every (lambda (character)
                                (or (alphanumericp character) (find character "_$")))
                              part)))
                parts))))

(defparameter *session-format-defaults*
  '(("datestyle" . "ISO, MDY") ("intervalstyle" . "postgres") ("extra_float_digits" . 1) ("bytea_output" . "hex")))

(defun session-integer-setting (name value minimum maximum unit)
  (handler-case
      (startup-parse-integer-setting
       (list name :user :integer "0" minimum maximum unit) (princ-to-string value))
    (error () (ereport "22023" "invalid value for parameter ~s: ~s" name value))))

(defun session-normalize-datestyle (value)
  "Apply C's independent DateStyle and date-order settings."
  (labels ((parts (setting)
             (let* ((text (or setting "ISO, MDY"))
                    (comma (position #\, text))
                    (style (if comma (subseq text 0 comma) text))
                    (order (cond ((search "DMY" text :test #'char-equal) "DMY")
                                 ((search "YMD" text :test #'char-equal) "YMD")
                                 (t "MDY"))))
               (values style order))))
    (multiple-value-bind (style order)
        (parts (and *backend* (gethash "datestyle" (backend-settings *backend*))))
      (let ((have-style nil) (have-order nil))
        (dolist (part (search-path-identifiers value "DateStyle"))
          (let* ((upper (string-upcase part))
                 (new-style (cond ((string= upper "ISO") "ISO")
                                  ((string= upper "SQL") "SQL")
                                  ((and (>= (length upper) 8) (string= upper "POSTGRES" :end1 8)) "Postgres")
                                  ((string= upper "GERMAN") "German")))
                 (new-order (cond ((or (string= upper "MDY") (string= upper "US")
                                       (and (>= (length upper) 7) (string= upper "NONEURO" :end1 7))) "MDY")
                                  ((or (string= upper "DMY")
                                       (and (>= (length upper) 4) (string= upper "EURO" :end1 4))) "DMY")
                                  ((string= upper "YMD") "YMD"))))
            (cond (new-style
                   (when (and have-style (not (string= style new-style)))
                     (ereport "22023" "conflicting DateStyle specifications"))
                   (setf style new-style have-style t)
                   (when (and (string= style "German") (not have-order)) (setf order "DMY")))
                  (new-order
                   (when (and have-order (not (string= order new-order)))
                     (ereport "22023" "conflicting DateStyle specifications"))
                   (setf order new-order have-order t))
                  ((string= upper "DEFAULT")
                   (multiple-value-bind (default-style default-order)
                       (parts (and *backend* (gethash "datestyle" (backend-settings-reset *backend*))))
                     (unless have-style (setf style default-style))
                     (unless have-order (setf order default-order))))
                  (t (ereport "22023" "invalid value for parameter DateStyle: ~s" value)))))
        (format nil "~a, ~a" style order)))))

(defun session-normalize-extra-setting (name value)
  (cond
    ((string= name "statement_timeout")
     (session-integer-setting name value 0 2147483647 :milliseconds))
    ((string= name "extra_float_digits") (session-integer-setting name value -15 3 nil))
    ((string= name "bytea_output")
     (unless (member value '("hex" "escape") :test #'string-equal)
       (ereport "22023" "invalid value for parameter \"bytea_output\": \"~a\"" value))
     (string-downcase value))
    ((string= name "datestyle") (session-normalize-datestyle value))
    ((string= name "intervalstyle")
     (unless (string-equal value "postgres")
       (ereport "0A000" "only postgres IntervalStyle is supported"))
     "postgres")
    (t (princ-to-string value))))

(defun session-extra-setting-p (name)
  (or (string= name "application_name") (string= name "statement_timeout")
      (string= name "synchronous_commit")
      (assoc name *session-format-defaults* :test #'string=)
      (session-custom-setting-p name)))

(defun session-normalize-application-name (value)
  "Clip VALUE at 63 UTF-8 bytes without splitting a character, then escape bytes
outside printable ASCII. This differs from postmaster cluster_name byte clipping.
Return the escaped value, whether clipping occurred, and the clipped input text."
  (let* ((text (princ-to-string value))
         (clipped (sql-name-input text)))
    (values
     (guc-clean-ascii-octets (string->bytes clipped))
     (not (equal text clipped)) clipped)))

(defun session-publish-application-name ()
  "Copy the setting to the 63-byte activity slot without changing its GUC value."
  (let ((value (gethash "application_name" (backend-settings *backend*) "")))
    (setf (backend-application-name *backend*) (sql-name-input value))
    value))

(defun session-report-parameter (stream name value)
  "Report a parameter only when its value differs from the last value sent to this connection."
  (when (and *backend* (not *session-setting-reports-suppressed-p*))
    (let ((reported (backend-reported-settings *backend*)))
      (unless (equal value (gethash name reported))
        (send-parameter-status stream name value)
        (setf (gethash name reported) value)))))

(defun session-report-parameters (stream)
  "Emit changed parameters before ReadyForQuery, using connection-wide wire history.
Search-path reporting is an existing native extension."
  (when *backend*
    (session-report-parameter stream "DateStyle" (session-extra-setting-value "datestyle"))
    (session-report-parameter stream "TimeZone"
                              (gethash "timezone" (backend-settings *backend*) "UTC"))
    (session-report-parameter stream "search_path" (session-search-path))
    (session-report-parameter stream "application_name"
                              (gethash "application_name" (backend-settings *backend*) ""))))

(defun session-normalize-synchronous-commit (value)
  "Normalize supported synchronous_commit values.
ON and its true aliases select remote flush. LOCAL keeps local durability;
OFF and its false aliases request asynchronous local commit."
  (let ((text (string-downcase (princ-to-string value))))
    (cond
      ((member text '("on" "true" "yes" "1") :test #'string=) "on")
      ((member text '("local" "remote_write" "remote_apply") :test #'string=) text)
      ((member text '("off" "false" "no" "0") :test #'string=)
       "off")
      (t (ereport "22023" "invalid value for parameter synchronous_commit")))))

(defun session-capture-raw-binding (settings name)
  "Capture value and source with independent presence flags.
Treat the four-field snapshot as immutable; transaction undo copies are shallow."
  (multiple-value-bind (value present-p) (gethash name settings)
    (multiple-value-bind (source source-present-p) (gethash (list :source name) settings)
      (list present-p value source-present-p source))))

(defun session-install-raw-binding (settings name binding)
  "Restore or remove value and source independently, preserving absence."
  (destructuring-bind (present-p value source-present-p source) binding
    (if present-p (setf (gethash name settings) value) (remhash name settings))
    (if source-present-p
        (setf (gethash (list :source name) settings) source)
        (remhash (list :source name) settings)))
  binding)

(defun session-remember-settings ()
  "Save the transaction's settings once so abort can restore the whole settings table."
  (when (and (eq (backend-state *backend*) :in-progress)
             (null (backend-settings-transaction-start *backend*)))
    (setf (backend-settings-transaction-start *backend*)
          (copy-session-settings (backend-settings *backend*)))))

(defun session-assign-setting-binding (name value reset-p local-p)
  "Assign a validated setting with interrupts masked.
The first LOCAL captures the prior binding once; ordinary SET clears that marker.
Snapshot, marker, value and source updates remain in the same masked section."
  (let ((settings (backend-settings *backend*))
        (local-key (list :local name)))
    (sb-sys:without-interrupts
      (session-remember-settings)
      (if local-p
          (unless (nth-value 1 (gethash local-key settings))
            (setf (gethash local-key settings) (session-capture-raw-binding settings name)))
          (remhash local-key settings))
      (session-install-raw-binding
       settings name
       (if reset-p
           (session-capture-raw-binding (backend-settings-reset *backend*) name)
           (list t value t "session"))))
    value))

(defun session-set-synchronous-commit (value &optional local-p)
  "Set or reset the session binding, optionally for this transaction.
Preserve absence as well as value so LOCAL, RESET and abort can restore fallback."
  (let* ((reset-p (or (eq value :default) (eq value +null+)))
         (text (unless reset-p (session-normalize-synchronous-commit value))))
    (session-assign-setting-binding "synchronous_commit" text reset-p local-p)))

(defun session-synchronous-commit-value (&optional (configuration nil supplied-p))
  "Return the session override or the installed default.
Do not copy the default into the session; without an override require a root
and a default entry. When CONFIGURATION is supplied, use that root for fallback."
  (when *backend*
    (multiple-value-bind (value present-p)
        (gethash "synchronous_commit" (backend-settings *backend*))
      (when present-p (return-from session-synchronous-commit-value value))))
  (let* ((root (if supplied-p configuration (startup-require-configuration)))
         (entry (and root (gethash "synchronous_commit" (startup-configuration-settings root)))))
    (unless entry (error "synchronous_commit has no installed default"))
    (getf entry :value)))

(defun session-synchronous-commit-policy ()
  "Capture and normalize the setting once as local durability and remote wait.
LOCAL requests flush without remote waiting; ON, REMOTE_WRITE and REMOTE_APPLY
request flush and the corresponding remote boundary. OFF requests asynchronous
local commit without a remote wait."
  (let* ((value (session-normalize-synchronous-commit (session-synchronous-commit-value)))
         (remote (cond ((string= value "on") :flush)
                       ((string= value "remote_write") :write)
                       ((string= value "remote_apply") :apply)
                       ((string= value "local") nil))))
    (%make-commit-policy (if (string= value "off") :async :flush) remote)))

(defun session-synchronous-commit-wait-mode ()
  "Return the remote component of the captured policy for existing callers."
  (commit-policy-remote-wait (session-synchronous-commit-policy)))

(defun session-synchronous-commit-settings-row (configuration)
  "Build one effective user enum row with its session or installed provenance."
  (multiple-value-bind (value present-p)
      (if *backend*
          (gethash "synchronous_commit" (backend-settings *backend*))
          (values nil nil))
    (let ((entry (if present-p
                     (list :value value :source :default)
                     (and configuration
                          (gethash "synchronous_commit"
                                   (startup-configuration-settings configuration))))))
      (when entry
        (let ((row (startup-settings-row "synchronous_commit" entry configuration)))
          (when present-p
            (setf (aref row 5)
                  (gethash '(:source "synchronous_commit") (backend-settings *backend*) "session")))
          row)))))

(defun session-application-name-value (value)
  "Normalize VALUE and send its truncation notice without assigning a setting."
  (multiple-value-bind (text truncated-p clipped)
      (session-normalize-application-name value)
    (when (and truncated-p (backend-stream *backend*))
      (send-notice (backend-stream *backend*)
                   (format nil "identifier ~c~a~c will be truncated to ~c~a~c"
                           #\" value #\" #\" clipped #\")
                   "NOTICE" "42622"))
    text))

(defun session-set-application-name (value &optional local-p)
  "Set the application name and its source, retaining transaction undo state.
Repeated LOCAL assignments preserve the binding to restore at commit.
LOCAL-P is used only within a transaction."
  (let* ((reset-p (or (eq value :default) (eq value +null+)))
         (text (unless reset-p (session-application-name-value value))))
    (sb-sys:without-interrupts
      (session-assign-setting-binding "application_name" text reset-p local-p)
      (session-publish-application-name))))

(defun session-finish-transaction-settings (commit-p)
  "Restore supported LOCAL bindings at commit or transaction-start bindings on abort.
This changes in-memory state only; client reporting occurs later."
  (let ((settings (backend-settings *backend*))
        (start (backend-settings-transaction-start *backend*)))
    (sb-sys:without-interrupts
      (dolist (name *session-local-setting-names*)
        (let* ((key (list :local name))
               (restore (if commit-p
                            (gethash key settings)
                            (when start (session-capture-raw-binding start name)))))
          (when restore (session-install-raw-binding settings name restore))
          (remhash key settings)))
      (session-publish-application-name))))

(defun session-application-name-settings-row ()
  "Build the supported pg_settings fields for application_name."
  (vector "application_name" (if *backend* (sql-setting-value "application_name") "")
          +null+ "user" "string"
          (if *backend*
              (gethash '(:source "application_name") (backend-settings *backend*) "default")
              "default")
          +null+ +null+ +null+ nil))

(defun session-set-extra-setting (name value)
  "Set or reset a session setting, saving the transaction's settings before changes.
The saved settings allow an abort to restore the values in effect at transaction start."
  (when (string= name "application_name")
    (return-from session-set-extra-setting (session-set-application-name value)))
  (when (string= name "synchronous_commit")
    (session-set-synchronous-commit value)
    (return-from session-set-extra-setting (session-synchronous-commit-value)))
  (let* ((settings (backend-settings *backend*))
         (reset-p (or (eq value :default) (eq value +null+)))
         (reset (backend-settings-reset *backend*)))
    (session-remember-settings)
    (when (session-custom-setting-p name)
      (unless (nth-value 1 (gethash name reset))
        (setf (gethash name reset) "")))
    (if reset-p
        (multiple-value-bind (baseline present-p) (gethash name reset)
          (if present-p (setf (gethash name settings) baseline) (remhash name settings))
          (if (gethash (list :source name) reset)
              (setf (gethash (list :source name) settings) (gethash (list :source name) reset))
              (remhash (list :source name) settings)))
        (progn
          (setf (gethash name settings)
                (session-normalize-extra-setting name value)
                (gethash (list :source name) settings) "session")))
    (session-extra-setting-value name)))

(defun session-extra-setting-value (name)
  (when (and *backend* (session-extra-setting-p name))
    (multiple-value-bind (value present-p) (gethash name (backend-settings *backend*))
      (cond
        ((string= name "synchronous_commit") (values (session-synchronous-commit-value) t))
        ((string= name "application_name") (values (or value "") t))
        ((string= name "statement_timeout")
         (let ((milliseconds (or value 0)))
           (values (if (zerop (mod milliseconds 1000))
                       (startup-format-unit (/ milliseconds 1000) :seconds)
                       (format nil "~dms" milliseconds))
                   t)))
        ((assoc name *session-format-defaults* :test #'string=)
         (values (princ-to-string (if present-p value
                                      (cdr (assoc name *session-format-defaults* :test #'string=)))) t))
        (present-p (values value t))
        ((nth-value 1 (gethash name (backend-settings-reset *backend*)))
         (values (gethash name (backend-settings-reset *backend*)) t))))))

(defun split-session-options (text)
  "Match postinit.c:pg_split_opts: backslash escaping, with literal quotes."
  (let ((position 0) (length (length text)) (arguments nil))
    (loop
      (loop while (and (< position length) (sql-array-space-p (char text position)))
            do (incf position))
      (when (= position length) (return (nreverse arguments)))
      (let ((escaped nil))
        (push (with-output-to-string (stream)
                (loop while (< position length)
                      for character = (char text position)
                      do (when (and (not escaped) (sql-array-space-p character)) (return))
                         (if (and (not escaped) (char= character #\\))
                             (setf escaped t)
                             (progn (write-char character stream) (setf escaped nil)))
                         (incf position)))
              arguments)))))

(defun session-option-assignments (text)
  (let ((arguments (split-session-options text)) (assignments nil))
    (loop while arguments
          for option = (pop arguments)
          for assignment = (cond
                             ((string= option "-c") (pop arguments))
                             ((uiop:string-prefix-p "-c" option) (subseq option 2))
                             ((uiop:string-prefix-p "--" option) (subseq option 2))
                             (t (ereport "42601" "invalid startup option: ~s" option)))
          do (unless (and assignment (position #\= assignment))
               (ereport "42601" "startup option requires name=value"))
             (let* ((equal (position #\= assignment))
                    (name (substitute #\_ #\- (string-downcase (subseq assignment 0 equal)))))
               (push (cons name (subseq assignment (1+ equal))) assignments)))
    (nreverse assignments)))

(defun session-apply-startup-options (params)
  "Apply client startup settings and retain them as the session reset baseline."
  (handler-case
      (let ((options (startup-last-parameter params "options"))
            (*session-setting-reports-suppressed-p* t))
        ;; -c options precede additional startup GUC fields, as in postinit.c.
        (dolist (assignment
                 (append (when options (session-option-assignments options))
                         (remove-if
                          (lambda (pair)
                            (member (car pair) '("user" "database" "replication"
                                                 "options")
                                    :test #'string=))
                          params)))
          (let ((name (string-downcase (car assignment))))
            (exec-set (make-broadcast-stream) (list :name name :value (cdr assignment)))
            (when (session-extra-setting-p name)
              (setf (gethash (list :source name) (backend-settings *backend*)) "client"))))
        (setf (backend-settings-reset *backend*)
              (copy-session-settings (backend-settings *backend*))))
    (pg-error (condition)
      (ereport-fatal (pg-error-sqlstate condition) "~a" (pg-error-message condition)))))

(defun session-reset-all-settings ()
  (session-remember-settings)
  (setf (backend-settings *backend*) (copy-session-settings (backend-settings-reset *backend*)))
  (session-publish-application-name))

(defun session-timeout-settings-row ()
  (let* ((settings (and *backend* (backend-settings *backend*)))
         (value (if settings (gethash "statement_timeout" settings 0) 0))
         (source (if settings (gethash '(:source "statement_timeout") settings "default") "default")))
    (vector "statement_timeout" (princ-to-string value) "ms" "user" "integer"
            source +null+ +null+ +null+ nil)))
