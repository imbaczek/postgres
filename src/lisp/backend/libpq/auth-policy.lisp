;;; Authentication policy publication and connection admission.
;;;
;;; HBA rules and their source text are installed as one generation.  Startup
;;; and reload validate a candidate before making it visible to connections.
(in-package #:pg)

(defvar *authentication-policy* nil)

(defstruct authentication-policy
  "One installed rule generation and the source texts retained for backup."
  (rules nil :read-only t)
  (hba-text "" :read-only t) (ident-text "" :read-only t)
  (hba-path nil :read-only t) (ident-path nil :read-only t))

(defun authentication-copy-rule (rule)
  "Copy one rule, including selector text, source text and address bytes."
  (flet ((tokens (items)
           (mapcar (lambda (token)
                     (%make-hba-token (copy-seq (hba-token-text token)) (hba-token-quoted-p token)))
                   items)))
    (make-hba-rule :transport (hba-rule-transport rule)
                   :databases (tokens (hba-rule-databases rule))
                   :users (tokens (hba-rule-users rule))
                   :network (when (hba-rule-network rule) (copy-seq (hba-rule-network rule)))
                   :mask (when (hba-rule-mask rule) (copy-seq (hba-rule-mask rule)))
                   :method (hba-rule-method rule)
                   :source (copy-seq (hba-rule-source rule)) :line (hba-rule-line rule))))

(defun authentication-text-within-bound-p (text)
  "Accept only valid Unicode text whose UTF-8 encoding is at most 1 MiB."
  (and (stringp text) (<= (length text) 1048576)
       (loop with bytes = 0
             for character across text
             for code = (char-code character)
             do (when (or (> code #x10ffff) (<= #xd800 code #xdfff))
                  (return nil))
                (incf bytes (cond ((< code #x80) 1) ((< code #x800) 2)
                                  ((< code #x10000) 3) (t 4)))
             when (> bytes 1048576) do (return nil)
               finally (return t))))

(defun authentication-complete-policy-p (policy)
  "Check policy completeness and text bounds without canonical validation.
Copying a complete policy validates and parses its retained text."
  (and (authentication-policy-p policy)
       (authentication-policy-rules policy)
       (authentication-text-within-bound-p (authentication-policy-hba-text policy))
       (plusp (length (authentication-policy-hba-text policy)))
       (authentication-text-within-bound-p (authentication-policy-ident-text policy))
       (pathnamep (authentication-policy-hba-path policy))
       (pathnamep (authentication-policy-ident-path policy))
       t))

(defun authentication-policy-settings-p (policy settings)
  "Require a complete policy whose retained paths match the candidate settings.
This checks retained provenance, not current file contents."
  (and (authentication-complete-policy-p policy)
       (loop for (name path) in
                             (list (list "hba_file" (authentication-policy-hba-path policy))
                                   (list "ident_file" (authentication-policy-ident-path policy)))
             always (let ((configured (startup-setting-value settings name)))
                      (and configured (equal (pathname configured) path))))))

(defun authentication-copy-policy (policy)
  "Own the policy data. Complete policies derive rules from retained HBA text,
which is also the source copied into a physical backup. Validate retained ident
text and reparse HBA text rather than trusting a supplied rule tree. Incomplete
component fixtures retain their explicit rule trees."
  (when policy
    (let* ((complete (authentication-complete-policy-p policy))
           (hba-text (copy-seq (authentication-policy-hba-text policy)))
           (ident-text (copy-seq (authentication-policy-ident-text policy)))
           (hba-path (authentication-policy-hba-path policy))
           (ident-path (authentication-policy-ident-path policy)))
      (when complete (ident-validate-text ident-text ident-path))
      (make-authentication-policy
       :rules (if complete
                  (hba-parse-text hba-text :source (namestring hba-path))
                  (mapcar #'authentication-copy-rule (authentication-policy-rules policy)))
       :hba-text hba-text :ident-text ident-text
       :hba-path hba-path :ident-path ident-path))))

;; During descriptor-scoped image verification, read policy through the held
;; image directory; ordinary startup continues to use its configuration reader.

(defun authentication-read-text (path)
  "Read at most 1 MiB from a bounded regular file and decode it as UTF-8."
  (labels ((read-text (stream)
             (let* ((buffer (make-string 1048577))
                    (count (read-sequence buffer stream)))
               (when (> count 1048576) (error "authentication input exceeds 1 MiB: ~a" path))
               (sb-ext:octets-to-string
                (map '(vector (unsigned-byte 8)) #'char-code (subseq buffer 0 count))
                :external-format :utf-8))))
    (if *physical-image-context*
        (with-physical-image-input (stream path :external-format :latin-1)
          (when (> (physical-image-stream-length stream) 1048576)
            (error "authentication input exceeds 1 MiB: ~a" path))
          (read-text stream))
        (with-open-stream (stream (startup-open-config path 1048576))
          (read-text stream)))))

(defun hba-read-file (path)
  "Return parsed HBA rules and their source text as two values."
  (let ((text (authentication-read-text path)))
    (values (hba-parse-text text :source (namestring path)) text)))

(defun ident-validate-text (text source)
  "Validate retained ident text and reject unsupported mappings."
  (when (or (not (authentication-text-within-bound-p text)) (find (code-char 0) text))
    (error "invalid identity mapping text: ~a" source))
  (dolist (entry (hba-logical-lines text :source (namestring source)))
    (when (hba-tokenize-line (cdr entry))
      (error "~a:~d: identity mappings are unsupported" source (car entry))))
  text)

(defun ident-read-file (path)
  "Accept an empty or comment-only ident file; identity mappings are unsupported."
  (ident-validate-text (authentication-read-text path) path))

(defun authentication-read-policy (settings)
  "Build a candidate from the resolved HBA and ident paths.
An HBA error rejects the candidate. An unreadable or unsupported ident file is
logged and treated as empty, as ident mappings are not used by this subset."
  (let ((hba (pathname (startup-setting-value settings "hba_file")))
        (ident (pathname (startup-setting-value settings "ident_file"))))
    (multiple-value-bind (rules text) (hba-read-file hba)
      (let ((ident-text
              (handler-case (ident-read-file ident)
                (error (condition)
                  (admin-log "could not load identity mappings: ~a" condition)
                  ""))))
        (make-authentication-policy :rules rules :hba-text text :ident-text ident-text
                                    :hba-path hba :ident-path ident)))))

(defparameter *authentication-marker-text* "pg-lisp authentication initialized
")

(defparameter *authentication-default-hba*
  (format nil "host all all 127.0.0.1/32 trust~%host replication all 127.0.0.1/32 trust~%host all all ::1/128 trust~%host replication all ::1/128 trust~%"))

(declaim (ftype function recovery-read-manifest recovery-check-manifest-schema))

(defun authentication-marker-path (directory)
  (merge-pathnames "authentication.initialized" directory))

(defun authentication-entry-exists-p (path)
  (handler-case (progn (sb-posix:lstat (namestring path)) t)
    (sb-posix:syscall-error (condition)
      (if (= sb-posix:enoent (sb-posix:syscall-errno condition)) nil (error condition)))))

(defun authentication-check-marker (directory)
  (let* ((expected (string->bytes *authentication-marker-text*))
         (actual (read-bounded-regular-file (authentication-marker-path directory)
                                            (length expected) :minimum (length expected))))
    (unless (equalp expected actual) (error "invalid authentication initialization marker"))
    t))

(defun authentication-owned-default-p (settings name filename)
  (let ((entry (gethash name settings)))
    (and (getf entry :implicit-path)
         (equal (getf entry :value)
                (namestring (startup-absolute-config-path (merge-pathnames filename *pgdata*)))))))

(defun authentication-check-backup-policy (directory)
  "Require the marker and both canonical policy files when the backup manifest
requires them. Validate the files before allowing startup to create defaults."
  (when (authentication-entry-exists-p (merge-pathnames "physical-backup.manifest" directory))
    (let ((manifest (recovery-check-manifest-schema (recovery-read-manifest directory))))
      (when (find-if #'bootstrap-scram-path-p (getf manifest :files) :key #'first)
        (let ((*pgdata* directory))
          (read-bootstrap-scram-verifier)))
      (when (eql 1 (getf manifest :auth-policy-required))
        (authentication-check-marker directory)
        (hba-read-file (merge-pathnames "pg_hba.conf" directory))
        (ident-read-file (merge-pathnames "pg_ident.conf" directory))
        t))))

(defun authentication-preflight-startup (settings)
  "Check existing policy and backup requirements before creating defaults.
An unmarked target with recovery.required but no manifest cannot acquire fresh
defaults; a marked target must retain a valid marker and readable HBA file."
  (authentication-check-backup-policy *pgdata*)
  (let ((marked (authentication-entry-exists-p (authentication-marker-path *pgdata*)))
        (hba (pathname (startup-setting-value settings "hba_file"))))
    (when (and (not marked)
               (authentication-entry-exists-p (merge-pathnames "recovery.required" *pgdata*))
               (not (authentication-entry-exists-p
                     (merge-pathnames "physical-backup.manifest" *pgdata*))))
      (error "incomplete recovery image cannot initialize authentication defaults"))
    (when marked (authentication-check-marker *pgdata*))
    (unless (and (not marked)
                 (authentication-owned-default-p settings "hba_file" "pg_hba.conf")
                 (not (authentication-entry-exists-p hba)))
      (hba-read-file hba)))
  t)

(defvar *authentication-publication-test-hook* nil)

(defun authentication-publication-test-point (point path)
  (when *authentication-publication-test-hook*
    (funcall *authentication-publication-test-hook* point path)))

(defun authentication-create-file (path text)
  "Publish TEXT without replacing an existing path. Sync the temporary inode before
linking it, then remove the temporary name and sync the directory. On unwinding,
clean up owned descriptors and temporary names. A published final name remains
if a later operation fails."
  (let ((bytes (string->bytes text))
        (directory (uiop:pathname-directory-pathname path))
        (fd nil) (temporary nil) (stream nil) (installed nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (multiple-value-setq (fd temporary)
               (open-exclusive-temporary-file (merge-pathnames ".authentication-XXXXXX" directory))))
           (sb-sys:without-interrupts
             (setf stream (sb-sys:make-fd-stream fd :input nil :output t
                                                    :element-type '(unsigned-byte 8) :auto-close t)
                   fd nil))
           (write-sequence bytes stream)
           (authentication-publication-test-point :after-file-write path)
           (fsync-stream stream)
           (authentication-publication-test-point :after-file-fsync path)
           (setf installed
                 (handler-case (progn (sb-posix:link (namestring temporary) (namestring path)) t)
                   (sb-posix:syscall-error (condition)
                     (if (= sb-posix:eexist (sb-posix:syscall-errno condition))
                         nil (error condition)))))
           (when installed
             (authentication-publication-test-point :after-publish path)
             (fsync-stream stream))
           (sb-posix:unlink (namestring temporary))
           (setf temporary nil)
           (fsync-directory directory)
           (when installed (authentication-publication-test-point :after-directory-fsync path))
           installed)
      (when stream (ignore-errors (close stream)))
      (when fd (ignore-errors (sb-posix:close fd)))
      (when temporary (ignore-errors (sb-posix:unlink (namestring temporary)))))))

(defun authentication-prepare-startup (settings)
  "Initialize implicit PGDATA defaults only while owning the data directory.
Never replace existing files; validate the candidate and publish the marker last.
A marked cluster must not silently recreate a missing HBA file."
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               (equal (truename *pgdata*) *postmaster-pid-directory*))
    (error "authentication initialization requires owned PGDATA"))
  (authentication-preflight-startup settings)
  (let ((marked (authentication-entry-exists-p (authentication-marker-path *pgdata*))))
    (unless marked
      (when (authentication-owned-default-p settings "hba_file" "pg_hba.conf")
        (authentication-create-file (merge-pathnames "pg_hba.conf" *pgdata*)
                                    *authentication-default-hba*))
      (when (authentication-owned-default-p settings "ident_file" "pg_ident.conf")
        (authentication-create-file (merge-pathnames "pg_ident.conf" *pgdata*) "")))
    (let ((policy (authentication-read-policy settings)))
      (unless marked
        (authentication-create-file (authentication-marker-path *pgdata*) *authentication-marker-text*))
      (authentication-check-marker *pgdata*)
      (fsync-directory *pgdata*)
      policy)))

(defun authentication-capture-policy ()
  "Return authentication from a captured installed root for a backup.
Require an installed policy. Root capture ends before the backup writes files;
the backup caller also holds reload exclusion and operation quiescence."
  (let ((policy (startup-configuration-authentication (startup-require-configuration))))
    (unless (and (authentication-policy-p policy) (authentication-policy-rules policy))
      (error "physical backup requires an installed authentication policy"))
    policy))

(defun authentication-write-backup-policy (policy target)
  "Copy the captured generation's text and marker into the backup target.
Each canonical adjunct must be created successfully; existing paths fail."
  (unless (and (authentication-policy-p policy) (authentication-policy-rules policy))
    (error "invalid physical backup authentication policy"))
  (dolist (entry (list (cons "pg_hba.conf" (authentication-policy-hba-text policy))
                       (cons "pg_ident.conf" (authentication-policy-ident-text policy))
                       (cons "authentication.initialized" *authentication-marker-text*)))
    (unless (authentication-create-file (merge-pathnames (car entry) target) (cdr entry))
      (error "physical backup target already contains authentication input: ~a" (car entry))))
  (authentication-check-marker target)
  t)

(defun startup-last-parameter (params name)
  "Return the last occurrence of NAME in a startup packet."
  (cdr (find name params :key #'car :test #'string= :from-end t)))

(defun authentication-clip-name (name)
  "Limit a startup name to 63 UTF-8 bytes. A slice splitting a code point fails
with 22021; arbitrary invalid UTF-8 names are not supported."
  (handler-case
      (let ((bytes (string->bytes name)))
        (sb-ext:octets-to-string bytes :end (min 63 (length bytes)) :external-format :utf-8))
    (error ()
      (ereport-fatal "22021" "startup name is not valid UTF-8 after byte truncation"))))

(defun authentication-startup-identities (params replication)
  "Resolve startup user and database for HBA matching from the final parameters.
A missing database defaults to the user; physical replication has no database."
  (let ((user (startup-last-parameter params "user"))
        (database (startup-last-parameter params "database")))
    (unless (and user (plusp (length user)))
      (ereport-fatal "28000" "no PostgreSQL user name specified in startup packet"))
    (unless (and database (plusp (length database))) (setf database user))
    (values (authentication-clip-name user)
            (if (eq replication :physical) "" (authentication-clip-name database)))))

(sb-alien:define-alien-routine ("inet_ntop" authentication-inet-ntop) sb-alien:c-string
  (family sb-alien:int) (address (* (sb-alien:unsigned 8)))
  (buffer (* (sb-alien:unsigned 8))) (size (sb-alien:unsigned 32)))

(defun authentication-peer-text (peer)
  (let* ((address (coerce peer '(vector (unsigned-byte 8))))
         (family (ecase (length address) (4 2) (16 10)))
         (buffer (make-array 46 :element-type '(unsigned-byte 8) :initial-element 0)))
    (sb-sys:with-pinned-objects (address buffer)
      (or (authentication-inet-ntop
           family (sb-alien:sap-alien (sb-sys:vector-sap address) (* (sb-alien:unsigned 8)))
           (sb-alien:sap-alien (sb-sys:vector-sap buffer) (* (sb-alien:unsigned 8))) 46)
          (error "could not format numeric client address")))))

(defun authentication-authorize (policy user database peer replication)
  "Select the first matching HBA rule for the peer and startup identities.
Return T for trust or :SCRAM for a password exchange. A reject or missing
rule signals a fatal error."
  (unless policy (ereport-fatal "28000" "authentication policy is not initialized"))
  (let ((method (hba-match-rules (authentication-policy-rules policy) user database peer
                                 :replication replication)))
    (case method
      (:trust (return-from authentication-authorize t))
      (:scram (return-from authentication-authorize :scram)))
    (let ((host (authentication-peer-text peer)))
      (ecase method
        (:reject
         (if (eq replication :physical)
             (ereport-fatal "28000" "pg_hba.conf rejects replication connection for host \"~a\", user \"~a\", no encryption"
                            host user)
             (ereport-fatal "28000" "pg_hba.conf rejects connection for host \"~a\", user \"~a\", database \"~a\", no encryption"
                            host user database)))
        (:implicit-reject
         (if (eq replication :physical)
             (ereport-fatal "28000" "no pg_hba.conf entry for replication connection from host \"~a\", user \"~a\", no encryption"
                            host user)
             (ereport-fatal "28000" "no pg_hba.conf entry for host \"~a\", user \"~a\", database \"~a\", no encryption"
                            host user database)))))))

(defun bootstrap-scram-hex (text size)
  (unless (= (length text) (* 2 size))
    (error "invalid bootstrap SCRAM verifier"))
  (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
    (dotimes (index size bytes)
      (let* ((offset (* 2 index))
             (high (digit-char-p (char text offset) 16))
             (low (digit-char-p (char text (1+ offset)) 16)))
        (unless (and high low) (error "invalid bootstrap SCRAM verifier"))
        (setf (aref bytes index) (+ (* 16 high) low))))))

(defconstant +bootstrap-scram-maximum-bytes+ 255)

(defun bootstrap-scram-path-p (name)
  (equal name "global/pg_bootstrap_scram"))

(defun read-bootstrap-scram-stream (stream)
  (let* ((stat (sb-posix:fstat (sb-sys:fd-stream-fd stream)))
         (mode (sb-posix:stat-mode stat))
         (buffer (make-string (1+ +bootstrap-scram-maximum-bytes+))))
    (unless (and (sb-posix:s-isreg mode) (zerop (logand mode #o077))
                 (= (sb-posix:stat-uid stat) (sb-posix:geteuid)))
      (error "invalid bootstrap SCRAM verifier file"))
    (let ((count (read-sequence buffer stream)))
      (unless (and (< 1 count (length buffer))
                   (char= (char buffer (1- count)) #\Newline)
                   (loop for i below count always (< (char-code (char buffer i)) 128)))
        (error "invalid bootstrap SCRAM verifier file"))
      (let ((fields (uiop:split-string (subseq buffer 0 (1- count)) :separator " ")))
        (unless (= 5 (length fields)) (error "invalid bootstrap SCRAM verifier"))
        (destructuring-bind (tag rounds salt stored server) fields
          (unless (and (string= tag "PGSCRAM1") (plusp (length rounds)) (every #'digit-char-p rounds))
            (error "invalid bootstrap SCRAM verifier"))
          (let ((iterations (parse-integer rounds)))
            (unless (<= 1 iterations #x7fffffff)
              (error "invalid bootstrap SCRAM iteration count"))
            (values (list (bootstrap-scram-hex salt 16) iterations
                          (bootstrap-scram-hex stored 32) (bootstrap-scram-hex server 32))
                    (subseq buffer 0 count))))))))

(defun read-bootstrap-scram-verifier ()
  "Load the private bootstrap verifier and return its validated file text as a second value."
  (let ((path (pgdata-path "global/pg_bootstrap_scram")))
    (if *physical-image-context*
        (with-physical-image-input (stream path :external-format :latin-1)
          (read-bootstrap-scram-stream stream))
        (sb-sys:without-interrupts
          (let ((fd nil) (stream nil))
            (unwind-protect
                 (progn
                   (setf fd (sb-posix:open (namestring path)
                                           (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                                   sb-posix:o-nonblock +postmaster-o-cloexec+))
                         stream (sb-sys:make-fd-stream fd :input t :element-type 'character
                                                          :external-format :latin-1))
                   (sb-sys:with-local-interrupts (read-bootstrap-scram-stream stream)))
              (if stream (close stream) (when fd (sb-posix:close fd)))))))))

(defun authentication-committed-role (user)
  (and *catalog* (gethash user (catalog-roles *catalog*))))

(defun role-can-rewind-native-p (role)
  (and role (pg-role-login-p role) (pg-role-native-rewind-p role)
       (every (lambda (signature)
                (member (car signature) (pg-role-function-grants role)))
              *rewind-function-grant-signatures*)))

(defun authentication-check-role-admission (user replication)
  "Enforce persisted LOGIN and REPLICATION attributes before admitting a connection."
  (let ((role (or (authentication-committed-role user)
                  (and (string= user "postgres") (catalog-bootstrap-role)))))
    (unless role
      (ereport-fatal "28000" "role ~s does not exist" user))
    (when (not (pg-role-login-p role))
      (ereport-fatal "28000" "role is not permitted to log in"))
    (when (and replication
               (not (pg-role-replication-p role))
               (not (and (eq replication :physical) (role-can-rewind-native-p role))))
      (ereport-fatal "42501" "permission denied to start WAL sender"))
    (when (and (not (string= user "postgres"))
               (not (or (eq replication :physical)
                        (and (null replication) (pg-role-function-grants role)))))
      (ereport-fatal "42501" "role has no supported SQL or physical replication access"))))

(defun rewind-restricted-sql-session-p (&optional (backend *backend*))
  (and backend (null (backend-replication-mode backend))
       (not (string= (backend-startup-user backend) "postgres"))))

(defun rewind-sql-expression-p (expression)
  (when (consp expression)
    (case (first expression)
      ((:const :bool :parameter :column :qualified-column) t)
      (:func
       (and (member (second expression)
                    '("current_user" "session_user" "pg_read_binary_file" "pg_stat_file" "pg_ls_dir"
                      "pg_is_in_recovery" "pg_control_checkpoint" "pg_current_wal_lsn"
                      "pg_walfile_name" "substr" "octet_length") :test #'string=)
            (member (getf (cdddr expression) :schema) '(nil "pg_catalog") :test #'equal)
            (notany #'identity
                    (loop for (key value) on (cdddr expression) by #'cddr
                          unless (eq key :schema) collect value))
            (every #'rewind-sql-expression-p (third expression))))
      (:binop (and (member (second expression) '("=" "||") :test #'string=)
                   (every #'rewind-sql-expression-p (cddr expression))))
      (:and (every #'rewind-sql-expression-p (rest expression)))
      (:not (rewind-sql-expression-p (second expression)))
      (:cast (and (member (third expression) '(:bit :int4 :int8 :text :bool))
                  (rewind-sql-expression-p (second expression))))
      (:record-field (rewind-sql-expression-p (second expression))))))

(defun rewind-sql-from-p (from)
  (or (null from)
      (and (sql-relation-name-p from)
           (equal (sql-relation-name-schema from) "pg_catalog")
           (equal (sql-relation-name-name from) "pg_roles"))
      (and (sql-derived-relation-p from)
           (let* ((query (sql-derived-relation-query from))
                  (targets (getf query :targets))
                  (expression (getf (first targets) :expr)))
             (and (equal (loop for (key value) on query by #'cddr when value collect key)
                         '(:type :lateral :from-function :targets))
                  (= (length targets) 1)
                  (eq (first expression) :from-function)
                  (rewind-sql-expression-p (second expression)))))))

(defun rewind-slot-name-list-p (expression)
  "Accept the bound parameter or the literal slot-name array sent by Patroni."
  (or (equal expression '(:parameter 1))
      (and (consp expression) (eq (first expression) :array-literal)
           (every (lambda (item)
                    (and (consp item) (eq (first item) :const)
                         (stringp (second item)) (null (cddr item))))
                  (second expression))
           (null (cddr expression)))))

(defun rewind-slot-query-matches-p (statement template allow-literal-array)
  "Compare the fixed query AST, allowing only its slot-name array to vary."
  (cond
    ((equal template '(:parameter 1))
     (if allow-literal-array (rewind-slot-name-list-p statement)
         (equal statement template)))
    ((consp template)
     (and (consp statement)
          (rewind-slot-query-matches-p (car statement) (car template) allow-literal-array)
          (rewind-slot-query-matches-p (cdr statement) (cdr template) allow-literal-array)))
    ((sql-derived-relation-p template)
     (and (sql-derived-relation-p statement)
          (equal (sql-derived-relation-query statement) (sql-derived-relation-query template))))
    ((sql-relation-name-p template)
     (and (sql-relation-name-p statement)
          (equal (sql-relation-name-schema statement) (sql-relation-name-schema template))
          (equal (sql-relation-name-name statement) (sql-relation-name-name template))))
    (t (equal statement template))))

(defun patroni-slot-query-shape-p (statement &optional allow-literal-array)
  "Match the fixed stock Patroni slot query."
  (some (lambda (template) (rewind-slot-query-matches-p statement template allow-literal-array))
        (load-time-value
         (append
          (parse-statements
           "SELECT slot_name, catalog_xmin FROM pg_catalog.pg_get_replication_slots()
               WHERE NOT pg_catalog.pg_is_in_recovery() AND slot_name = ANY($1)")
          (loop for filter in '("" "NOT failover AND ")
                append
                (parse-statements
                 (format nil
                         "SELECT slot_name, slot_type, datname, plugin, catalog_xmin,
                                    pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint,
                                    pg_catalog.pg_read_binary_file('pg_replslot/' || slot_name || '/state')
                             FROM pg_catalog.pg_get_replication_slots()
                             JOIN pg_catalog.pg_database ON datoid = oid
                             WHERE ~aNOT pg_catalog.pg_is_in_recovery()
                               AND slot_name = ANY($1)" filter)))) t)))

(defun rewind-patroni-slot-query-p (statement)
  "Allow Patroni's read-only slot queries for a login authorized for native rewind."
  (and (role-can-rewind-native-p (authentication-committed-role (backend-startup-user *backend*)))
       (patroni-slot-query-shape-p statement t)))

(defun check-rewind-sql-statements (statements)
  (when (rewind-restricted-sql-session-p)
    (dolist (statement statements)
      (unless (or (member (getf statement :type) '(:begin :commit :rollback))
                  (rewind-patroni-slot-query-p statement)
                  (and (eq (getf statement :type) :select)
                       (loop for (key value) on statement by #'cddr
                             always (or (null value)
                                        (member key '(:type :targets :from :from-alias :where))))
                       (rewind-sql-from-p (getf statement :from))
                       (every (lambda (target)
                                (or (getf target :star)
                                    (rewind-sql-expression-p (getf target :expr))))
                              (getf statement :targets))
                       (or (null (getf statement :where))
                           (rewind-sql-expression-p (getf statement :where)))))
        (ereport "42501" "statement is outside the rewind SQL scope")))))

(defun authenticate-role-scram (stream user)
  (let* ((role (authentication-committed-role user))
         (verifier (cond (role (and (pg-role-login-p role) (pg-role-verifier role)))
                         ((string= user "postgres")
                          (handler-case (read-bootstrap-scram-verifier)
                            (pg-error (condition) (error condition))
                            (error () nil))))))
    (unless verifier (ereport-fatal "28P01" "password authentication failed"))
    (sb-ext:with-timeout 10
      (apply #'scram-authenticate-stream stream verifier))))
