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
  '(("datestyle" . "ISO, MDY") ("intervalstyle" . "postgres") ("extra_float_digits" . 1)))

(defun session-integer-setting (name value minimum maximum unit)
  (handler-case
      (startup-parse-integer-setting
       (list name :user :integer "0" minimum maximum unit) (princ-to-string value))
    (error () (ereport "22023" "invalid value for parameter ~s: ~s" name value))))

(defun session-normalize-extra-setting (name value)
  (cond
    ((string= name "statement_timeout")
     (session-integer-setting name value 0 2147483647 :milliseconds))
    ((string= name "extra_float_digits") (session-integer-setting name value -15 3 nil))
    ((string= name "datestyle")
     (unless (every (lambda (part) (member part '("ISO" "MDY") :test #'string-equal))
                    (search-path-identifiers value "DateStyle"))
       (ereport "0A000" "only ISO, MDY DateStyle is supported"))
     "ISO, MDY")
    ((string= name "intervalstyle")
     (unless (string-equal value "postgres")
       (ereport "0A000" "only postgres IntervalStyle is supported"))
     "postgres")
    (t (princ-to-string value))))

(defun session-extra-setting-p (name)
  (or (string= name "application_name") (string= name "statement_timeout")
      (assoc name *session-format-defaults* :test #'string=)
      (session-custom-setting-p name)))

(defun session-normalize-application-name (value)
  (let* ((text (princ-to-string value))
         (clipped (sql-name-input text)))
    (values
     (with-output-to-string (output)
       (loop for byte across (string->bytes clipped)
             do (if (<= 32 byte 126)
                    (write-char (code-char byte) output)
                    (format output "\\x~(~2,'0x~)" byte))))
     (not (equal text clipped)) clipped)))

(defun session-publish-application-name ()
  (let ((value (gethash "application_name" (backend-settings *backend*) "")))
    (setf (backend-application-name *backend*) (sql-name-input value))
    value))

(defun session-report-parameter (stream name value)
  (when (and *backend* (not *session-setting-reports-suppressed-p*))
    (let ((reported (backend-reported-settings *backend*)))
      (unless (equal value (gethash name reported))
        (send-parameter-status stream name value)
        (setf (gethash name reported) value)))))

(defun session-report-parameters (stream)
  (when *backend*
    (session-report-parameter stream "TimeZone"
                              (gethash "timezone" (backend-settings *backend*) "UTC"))
    (session-report-parameter stream "search_path" (session-search-path))
    (session-report-parameter stream "application_name"
                              (gethash "application_name" (backend-settings *backend*) ""))))

(defun session-remember-settings ()
  (when (and (eq (backend-state *backend*) :in-progress)
             (null (backend-settings-transaction-start *backend*)))
    (setf (backend-settings-transaction-start *backend*)
          (copy-session-settings (backend-settings *backend*)))))

(defun session-application-name-value (value)
  (multiple-value-bind (text truncated-p clipped)
      (session-normalize-application-name value)
    (when (and truncated-p (backend-stream *backend*))
      (send-notice (backend-stream *backend*)
                   (format nil "identifier ~c~a~c will be truncated to ~c~a~c"
                           #\" value #\" #\" clipped #\")
                   "NOTICE" "42622"))
    text))

(defun session-set-application-name (value &optional local-p)
  (let* ((settings (backend-settings *backend*))
         (reset (backend-settings-reset *backend*))
         (reset-p (or (eq value :default) (eq value +null+)))
         (text (if reset-p
                   (gethash "application_name" reset "")
                   (session-application-name-value value))))
    (session-remember-settings)
    (if local-p
        (unless (gethash :application-name-local settings)
          (setf (gethash :application-name-local settings)
                (list (gethash "application_name" settings "")
                      (gethash '(:source "application_name") settings "default"))))
        (remhash :application-name-local settings))
    (setf (gethash "application_name" settings) text
          (gethash '(:source "application_name") settings)
          (if reset-p (gethash '(:source "application_name") reset "default") "session"))
    (session-publish-application-name)))

(defun session-finish-application-name (commit-p)
  (let* ((settings (backend-settings *backend*))
         (start (backend-settings-transaction-start *backend*))
         (restore (if commit-p
                      (gethash :application-name-local settings)
                      (when start
                        (list (gethash "application_name" start "")
                              (gethash '(:source "application_name") start "default"))))))
    (when restore
      (setf (gethash "application_name" settings) (first restore)
            (gethash '(:source "application_name") settings) (second restore)))
    (remhash :application-name-local settings)
    (session-publish-application-name)))

(defun session-application-name-settings-row ()
  (vector "application_name" (if *backend* (sql-setting-value "application_name") "")
          +null+ "user" "string"
          (if *backend*
              (gethash '(:source "application_name") (backend-settings *backend*) "default")
              "default")
          +null+ +null+ nil))

(defun session-set-extra-setting (name value)
  (when (string= name "application_name")
    (return-from session-set-extra-setting (session-set-application-name value)))
  (let* ((settings (backend-settings *backend*))
         (reset-p (or (eq value :default) (eq value +null+)))
         (reset (backend-settings-reset *backend*)))
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
  (handler-case
      (let ((options (cdr (assoc "options" params :test #'string=)))
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
            source +null+ +null+ nil)))
