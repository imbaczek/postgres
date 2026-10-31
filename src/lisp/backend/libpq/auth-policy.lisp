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

(defun authentication-copy-policy (policy)
  (when policy
    (make-authentication-policy
     :rules (mapcar #'authentication-copy-rule (authentication-policy-rules policy))
     :hba-text (copy-seq (authentication-policy-hba-text policy))
     :ident-text (copy-seq (authentication-policy-ident-text policy))
     :hba-path (authentication-policy-hba-path policy)
     :ident-path (authentication-policy-ident-path policy))))

(defun authentication-read-text (path)
  "Read at most 1 MiB from a bounded regular file and decode it as UTF-8."
  (with-open-stream (stream (startup-open-config path 1048576))
    (let* ((buffer (make-string 1048577))
           (count (read-sequence buffer stream)))
      (when (> count 1048576) (error "authentication input exceeds 1 MiB: ~a" path))
      (sb-ext:octets-to-string
       (map '(vector (unsigned-byte 8)) #'char-code (subseq buffer 0 count))
       :external-format :utf-8))))

(defun hba-read-file (path)
  "Return parsed HBA rules and their source text as two values."
  (let ((text (authentication-read-text path)))
    (values (hba-parse-text text :source (namestring path)) text)))

(defun ident-read-file (path)
  "Accept an empty or comment-only ident file; identity mappings are unsupported."
  (let ((text (authentication-read-text path)))
    (when (find (code-char 0) text) (error "NUL in identity mapping file: ~a" path))
    (dolist (entry (hba-logical-lines text :source (namestring path)))
      (when (hba-tokenize-line (cdr entry))
        (error "~a:~d: identity mappings are unsupported" path (car entry))))
    text))

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
  "Return the installed generation for a backup, not uninstalled disk edits.
The backup caller holds reload exclusion and operation quiescence."
  (let ((policy *authentication-policy*))
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
  "Apply the installed first-match HBA policy to the numeric peer and startup names.
Trust admits the connection; reject and no match signal distinct fatal errors."
  (unless policy (ereport-fatal "28000" "authentication policy is not initialized"))
  (let ((method (hba-match-rules (authentication-policy-rules policy) user database peer
                                 :replication replication)))
    (when (eq method :trust) (return-from authentication-authorize t))
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
