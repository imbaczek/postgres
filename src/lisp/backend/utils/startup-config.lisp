;;;; Startup configuration inputs and typed settings used by the native server.
(in-package #:pg)

(declaim (special *authentication-policy*))

(declaim (ftype function authentication-read-policy))

(defvar *startup-server-settings* nil)

(defvar *startup-native-inputs* nil)

(defvar *startup-command-overrides* nil)

(defstruct startup-configuration
  ;; Each root has a fresh identity and stores only its predecessor's identity.
  ;; Inputs and authentication are owned copies; desired may reuse an unchanged
  ;; generation. Resources are validated, not the installed live capacity.
  (identity (gensym "CONFIGURATION-") :read-only t)
  (predecessor nil :read-only t)
  (settings nil :read-only t) (native nil :read-only t) (overrides nil :read-only t)
  (authentication nil :read-only t) (desired nil :read-only t)
  (slot-policy nil :read-only t) (resources nil :read-only t)
  (timeout 0 :read-only t) (wal-bytes 0 :read-only t) (keep-mb 0 :read-only t)
  (hot-standby nil :read-only t) (wal-level nil :read-only t)
  (max-slots 0 :read-only t) (max-connections 0 :read-only t) (max-senders 0 :read-only t))

(defvar *startup-installed-configuration* nil)

(defvar *startup-configuration-lock* (sb-thread:make-mutex :name "installed configuration"))

(defun startup-capture-configuration ()
  "Capture the installed root, or NIL if configuration is uninstalled.
Hold the root mutex only for the pointer read; callers use the root after release."
  (sb-thread:with-mutex (*startup-configuration-lock*)
    *startup-installed-configuration*))

(defun startup-require-configuration ()
  "Return the installed root or signal that configuration is uninstalled."
  (or (startup-capture-configuration)
      (error "server configuration is not installed")))

(defun startup-retention-inputs ()
  "Return policy and keep size from one required installed configuration.
A missing root is an error; there is no fallback to globals."
  (let ((configuration (startup-require-configuration)))
    (values (startup-configuration-slot-policy configuration)
            (startup-configuration-keep-mb configuration))))

(defun startup-copy-input (value)
  "Copy mutable storage in configuration input trees.
Reject unsupported objects without printing their contents."
  (typecase value
    (hash-table
     (let ((copy (make-hash-table :test (hash-table-test value))))
       (maphash (lambda (key item)
                  (setf (gethash (startup-copy-input key) copy) (startup-copy-input item)))
                value)
       copy))
    (cons (cons (startup-copy-input (car value)) (startup-copy-input (cdr value))))
    (vector
     (let ((copy (copy-seq value)))
       (dotimes (index (length copy) copy)
         (setf (aref copy index) (startup-copy-input (aref copy index))))))
    ((or null symbol number character pathname) value)
    (t (error "unsupported configuration input object"))))

(declaim (ftype function authentication-copy-policy))

(defun startup-prepare-installation-from-base (settings native overrides installed policy supplied-p)
  "Prepare owned inputs, authentication, desired follow state and validated values.
Reload derives inputs and predecessor from the same captured base; default
authentication also uses that base. An explicitly supplied NIL policy differs
from an omitted policy. Resolve slot retention once here.
Resource capacity installation remains a startup step."
  (let* ((predecessor (when installed (startup-configuration-identity installed)))
         (owned-settings (startup-copy-input settings))
         (owned-native (startup-copy-input native))
         (owned-overrides (startup-copy-input overrides))
         (authentication (authentication-copy-policy
                          (if supplied-p policy
                              (when (and installed (startup-configuration-authentication installed))
                                (authentication-read-policy owned-settings)))))
         (resources (startup-check-connection-limits owned-settings))
         (slot-policy (startup-effective-slot-policy owned-settings owned-native))
         (desired (physical-follow-plan-settings owned-settings)))
    (make-startup-configuration
     :predecessor predecessor :settings owned-settings :native owned-native
     :overrides owned-overrides :authentication authentication :desired desired
     :resources resources :slot-policy slot-policy
     :timeout (startup-setting-value owned-settings "checkpoint_timeout")
     :keep-mb (startup-setting-value owned-settings "wal_keep_size")
     :wal-bytes (* 1024 1024 (startup-setting-value owned-settings "max_wal_size"))
     :hot-standby (startup-setting-value owned-settings "hot_standby")
     :wal-level (if (equal "logical" (startup-setting-value owned-settings "wal_level")) :logical :replica)
     :max-slots (startup-setting-value owned-settings "max_replication_slots")
     :max-connections (startup-setting-value owned-settings "max_connections")
     :max-senders (startup-setting-value owned-settings "max_wal_senders"))))

(defun startup-prepare-installation (settings native overrides &optional (policy nil supplied-p))
  "Prepare an installation against one captured configuration generation."
  (startup-prepare-installation-from-base
   settings native overrides (startup-capture-configuration) policy supplied-p))

(declaim (special *managed-worker-slots* *managed-worker-lock*
                  *managed-worker-registration-closed-p*))

(declaim (special *physical-restartpoint-timeout* *physical-restartpoint-wal-bytes*))

(declaim (ftype function directory-pathname parse-port-arg parse-query-temp-file-limit-bytes
                parse-query-sort-arena-limit-bytes parse-slot-wal-keep-bytes
                parse-bind-address-arg))

(defparameter *startup-recovery-target-settings*
  '("recovery_target" "recovery_target_lsn" "recovery_target_name"
    "recovery_target_time" "recovery_target_xid")
  "Target selectors that must remain empty until targeted recovery is implemented.")

(defparameter *startup-setting-specs*
  '(("port" :postmaster :integer "5433" 1 65535 nil)
    ("listen_addresses" :postmaster :string "127.0.0.1")
    ("data_directory" :postmaster :string nil)
    ("hba_file" :postmaster :string nil)
    ("ident_file" :postmaster :string nil)
    ("cluster_name" :postmaster :string "")
    ("restore_command" :sighup :string "")
    ("primary_conninfo" :sighup :conninfo "")
    ("primary_slot_name" :sighup :slot "")
    ("recovery_target_timeline" :postmaster :timeline "latest")
    ("hot_standby" :postmaster :boolean "on")
    ("wal_level" :postmaster :wal-level "replica")
    ("wal_log_hints" :postmaster :boolean "off")
    ("max_connections" :postmaster :integer "100" 1 262143 nil)
    ("max_wal_senders" :postmaster :integer "10" 0 262143 nil)
    ("max_worker_processes" :postmaster :integer "8" 0 262143 nil)
    ("max_prepared_transactions" :postmaster :integer "0" 0 262143 nil)
    ("track_commit_timestamp" :postmaster :boolean "off")
    ("recovery_target" :postmaster :string "")
    ("recovery_target_lsn" :postmaster :string "")
    ("recovery_target_name" :postmaster :string "")
    ("recovery_target_time" :postmaster :string "")
    ("recovery_target_xid" :postmaster :string "")
    ("max_locks_per_transaction" :postmaster :integer "128" 10 2147483647 nil)
    ("reserved_connections" :postmaster :integer "0" 0 262143 nil)
    ("superuser_reserved_connections" :postmaster :integer "3" 0 262143 nil)
    ("max_replication_slots" :postmaster :integer "10" 0 262143 nil)
    ("checkpoint_timeout" :sighup :integer "300" 30 86400 :seconds)
    ("max_wal_size" :sighup :integer "1024" 2 2147483647 :megabytes)
    ("wal_keep_size" :sighup :integer "0" 0 2147483647 :megabytes)
    ("max_slot_wal_keep_size" :sighup :integer "-1" -1 2147483647 :megabytes)))

(sb-alien:define-alien-routine ("strtol" startup-strtol) sb-alien:long
  (text (* sb-alien:char)) (end (* (* sb-alien:char))) (base sb-alien:int))

(sb-alien:define-alien-routine ("strtod" startup-strtod) sb-alien:double
  (text (* sb-alien:char)) (end (* (* sb-alien:char))))

(sb-alien:define-alien-routine ("__errno_location" startup-errno-location) (* sb-alien:int))

(sb-alien:define-alien-routine ("strtoull" startup-strtoull) sb-alien:unsigned-long-long
  (text (* sb-alien:char)) (end (* (* sb-alien:char))) (base sb-alien:int))

(defun startup-recovery-timeline-number (begin end)
  "Parse an unsigned base-zero value with the libc behavior used by C.
Use the C23 entry point when available, otherwise the older strtoull."
  (let ((address (sb-sys:find-foreign-symbol-address "__isoc23_strtoull")))
    (setf (sb-alien:deref (startup-errno-location)) 0)
    (if address
        (sb-alien:alien-funcall
         (sb-alien:sap-alien
          (sb-sys:int-sap address)
          (function sb-alien:unsigned-long-long
                    (* sb-alien:char) (* (* sb-alien:char)) sb-alien:int))
         begin end 0)
        (startup-strtoull begin end 0))))

(defun startup-parse-recovery-timeline (text)
  "Parse exact current or latest, or a complete nonzero 32-bit timeline ID.
Numeric input uses unsigned base-zero libc parsing; units and integer GUC
rounding do not apply."
  (when (find (code-char 0) text) (error "NUL in recovery_target_timeline"))
  (cond ((string= text "current") :current)
        ((string= text "latest") :latest)
        (t
         (let ((bytes (concatenate '(vector (unsigned-byte 8)) (string->bytes text) #(0))))
           (sb-sys:with-pinned-objects (bytes)
             (sb-alien:with-alien ((end (* sb-alien:char)))
               (let* ((begin (sb-alien:sap-alien (sb-sys:vector-sap bytes) (* sb-alien:char)))
                      (errno (startup-errno-location)))
                 (let* ((timeline (startup-recovery-timeline-number begin (sb-alien:addr end)))
                        (offset (- (sb-sys:sap-int (sb-alien:alien-sap end))
                                   (sb-sys:sap-int (sb-sys:vector-sap bytes)))))
                   (unless (and (zerop (sb-alien:deref errno))
                                (zerop (aref bytes offset))
                                (<= 1 timeline 4294967295))
                     (error "invalid recovery_target_timeline ~s" text))
                   timeline))))))))

(defun startup-integer-number (begin end)
  "Parse a signed base-zero integer using C23 strtol when available, otherwise strtol.
Clear errno after symbol lookup so lookup errors do not contaminate conversion.
Recovery timeline parsing uses its separate unsigned conversion."
  (let ((address (sb-sys:find-foreign-symbol-address "__isoc23_strtol")))
    (setf (sb-alien:deref (startup-errno-location)) 0)
    (if address
        (sb-alien:alien-funcall
         (sb-alien:sap-alien
          (sb-sys:int-sap address)
          (function sb-alien:long (* sb-alien:char) (* (* sb-alien:char)) sb-alien:int))
         begin end 0)
        (startup-strtol begin end 0))))

(defun startup-numeric-prefix (text)
  "Use C numeric conversions as in guc.c:parse_int, without Lisp READ.
Try the signed integer path first; overflow or a fractional or exponent suffix
uses strtod. Return the remaining unit for the caller's conversion and rounding."
  (when (find (code-char 0) text) (error "NUL in configuration value"))
  (let ((bytes (concatenate '(vector (unsigned-byte 8)) (string->bytes text) #(0))))
    (sb-sys:with-pinned-objects (bytes)
      (sb-alien:with-alien ((end (* sb-alien:char)))
        (let* ((begin (sb-alien:sap-alien (sb-sys:vector-sap bytes) (* sb-alien:char)))
               (errno (startup-errno-location))
               (number nil)
               (offset nil))
          (setf (sb-alien:deref errno) 0
                number (startup-integer-number begin (sb-alien:addr end))
                offset (- (sb-sys:sap-int (sb-alien:alien-sap end))
                          (sb-sys:sap-int (sb-sys:vector-sap bytes))))
          (when (or (= (sb-alien:deref errno) sb-posix:erange)
                    (find (code-char (aref bytes offset)) ".eE"))
            (setf (sb-alien:deref errno) 0
                  number (startup-strtod begin (sb-alien:addr end))
                  offset (- (sb-sys:sap-int (sb-alien:alien-sap end))
                            (sb-sys:sap-int (sb-sys:vector-sap bytes)))))
          (unless (and (plusp offset) (zerop (sb-alien:deref errno)))
            (error "invalid numeric configuration value ~s" text))
          (setf number (coerce number 'double-float))
          (when (or (sb-ext:float-nan-p number) (sb-ext:float-infinity-p number))
            (error "nonfinite configuration value"))
          (values number (string-trim '(#\Space #\Tab #\Newline #\Return #\Page)
                                      (bytes->string bytes :start offset :end (1- (length bytes))))))))))

(defun startup-parse-integer-setting (spec text)
  "Match parse_int rounding, including conversion via the next smaller unit."
  (multiple-value-bind (number unit) (startup-numeric-prefix text)
    (unless (zerop (length unit))
      (let* ((table (ecase (seventh spec)
                      (:milliseconds '(("d" 86400000d0) ("h" 3600000d0) ("min" 60000d0)
                                       ("s" 1000d0) ("ms" 1d0) ("us" 0.001d0)))
                      (:seconds '(("d" 86400d0) ("h" 3600d0) ("min" 60d0)
                                  ("s" 1d0) ("ms" 0.001d0) ("us" 0.000001d0)))
                      (:megabytes '(("TB" 1048576d0) ("GB" 1024d0) ("MB" 1d0)
                                    ("kB" 0.0009765625d0) ("B" 0.00000095367431640625d0)))
                      ((nil) nil)))
             (position (position unit table :key #'first :test #'string=)))
        (unless position (error "invalid unit in configuration parameter ~a: ~s" (first spec) text))
        (setf number (* number (second (nth position table))))
        (when (< (1+ position) (length table))
          (let ((smaller (second (nth (1+ position) table))))
            (setf number (* (round (/ number smaller)) smaller))))))
    (let ((integer (round number)))
      (unless (<= (fifth spec) integer (sixth spec))
        (error "configuration parameter ~a out of range: ~s" (first spec) text))
      integer)))

(defun startup-parse-boolean-setting (text)
  "Parse a GUC Boolean, rejecting edge ASCII whitespace unlike SQL casts."
  (when (or (zerop (length text))
            (member (char-code (char text 0)) '(9 10 11 12 13 32))
            (member (char-code (char text (1- (length text)))) '(9 10 11 12 13 32)))
    (error "invalid boolean configuration value ~s" text))
  (sql-cast-boolean-input text))

(declaim (ftype function physical-follow-configured-slot physical-follow-plan-settings
                physical-follow-call-with-desired-install))

(defun startup-render-conninfo (bytes)
  (handler-case
      (let ((text (bytes->string bytes)))
        (if (equalp bytes (string->bytes text)) text (copy-seq bytes)))
    (error () (copy-seq bytes))))

(defun startup-setting-text (name value)
  (case (third (assoc name *startup-setting-specs* :test #'string=))
    (:boolean (if value "on" "off"))
    (:conninfo (startup-render-conninfo value))
    (t (princ-to-string value))))

(defun guc-clean-ascii-octets (octets)
  "Render printable ASCII directly and escape each other byte as hexadecimal."
  (with-output-to-string (output)
    (loop for byte across octets
          do (if (<= 32 byte 126)
                 (write-char (code-char byte) output)
                 (format output "\\x~(~2,'0x~)" byte)))))

(defun startup-input-octets (input)
  "Encode text as UTF-8 or copy already parsed octets without reencoding."
  (etypecase input
    (string (string->bytes input))
    ((vector (unsigned-byte 8)) (copy-seq input))))

(defun startup-input-text (input)
  "Decode input octets as UTF-8 for ordinary settings and include paths."
  (bytes->string (startup-input-octets input)))

(defun startup-parse-wal-level (text)
  "Parse WAL level names, mapping the deprecated archive and hot_standby aliases
to replica. Feature admission separately rejects minimal."
  (let ((value (string-downcase text)))
    (cond ((member value '("minimal" "replica" "logical") :test #'string=) value)
          ((member value '("archive" "hot_standby") :test #'string=) "replica")
          (t (error "invalid value for configuration parameter wal_level: ~s" text)))))

(defun startup-normalize-setting (name input)
  "Normalize a startup setting from text or literal octets.
For cluster_name, postmaster SQL_ASCII clips 63 raw bytes before ASCII cleaning,
even with a UTF-8 database. Conninfo retains its input octets; other settings
decode their input as UTF-8.
Timeline input is validated but its original spelling is retained."
  (let ((spec (assoc name *startup-setting-specs* :test #'string=))
        (octets (startup-input-octets input)))
    (unless spec (error "unrecognized configuration parameter ~s" name))
    (when (find 0 octets) (error "NUL in configuration parameter ~a" name))
    (when (string= name "cluster_name")
      ;; Postmaster GUC parsing uses SQL_ASCII, even for a UTF-8 cluster.
      (return-from startup-normalize-setting
        (guc-clean-ascii-octets (subseq octets 0 (min 63 (length octets))))))
    (when (eq :conninfo (third spec))
      (return-from startup-normalize-setting octets))
    (let ((text (startup-input-text octets)))
      (when (eq :slot (third spec))
        (physical-follow-configured-slot text)
        (return-from startup-normalize-setting text))
      (ecase (third spec)
        (:integer (startup-parse-integer-setting spec text))
        (:wal-level (startup-parse-wal-level text))
        (:boolean (startup-parse-boolean-setting text))
        (:timeline (startup-parse-recovery-timeline text) text)
        (:string
         (when (and (string= name "restore_command") (not (equal text "")))
           (error "nonempty restore_command requires unimplemented archive recovery"))
         (unless (or (member name '("restore_command" "hba_file" "ident_file") :test #'string=)
                     (member name *startup-recovery-target-settings* :test #'string=)
                     (plusp (length text)))
           (error "invalid empty configuration parameter ~a" name))
         (when (and (string= name "listen_addresses") (find #\, text))
           (error "multiple listen_addresses are not implemented"))
         text)))))

(defun startup-config-name-p (text)
  (and (plusp (length text))
       (or (alpha-char-p (char text 0)) (char= (char text 0) #\_))
       (every (lambda (char) (or (alphanumericp char) (find char "_."))) text)))

(defun startup-config-escape (line position)
  "DeescapeQuotedString's octal and single-character escapes, without READ."
  (let* ((char (char line position))
         (digit (digit-char-p char 8)))
    (if digit
        (let ((value digit) (next (1+ position)))
          (loop repeat 2 while (and (< next (length line)) (digit-char-p (char line next) 8))
                do (setf value (+ (* value 8) (digit-char-p (char line next) 8)))
                   (incf next))
          (values (code-char (logand value #xff)) next))
        (values (case char
                  (#\n #\Newline) (#\r #\Return) (#\t #\Tab)
                  (#\b #\Backspace) (#\f #\Page) (t char))
                (1+ position)))))

(defun startup-config-bare-value-p (text)
  "Recognize exactly one INTEGER, REAL, ID or UNQUOTED_STRING lexer token."
  (labels ((letter-p (char)
             (or (find char "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
                 (>= (char-code char) 128)))
           (ascii-letter-p (char)
             (find char "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"))
           (digits (position radix)
             (loop while (and (< position (length text)) (digit-char-p (char text position) radix))
                   do (incf position) finally (return position))))
    (when (zerop (length text)) (return-from startup-config-bare-value-p nil))
    (or (and (letter-p (char text 0))
             (every (lambda (char) (or (letter-p char) (digit-char-p char)
                                       (find char "-._:/"))) text))
        (let* ((start (if (find (char text 0) "+-") 1 0))
               (integer-end (digits start 10))
               (hex-end (when (and (<= (+ start 3) (length text))
                                   (string= "0x" text :start2 start :end2 (+ start 2))
                                   (digit-char-p (char text (+ start 2)) 16))
                          (digits (+ start 2) 16))))
          (or (and (> (or hex-end integer-end) start)
                   (every #'ascii-letter-p (subseq text (or hex-end integer-end))))
              (when (and (< integer-end (length text)) (char= (char text integer-end) #\.))
                (let ((end (digits (1+ integer-end) 10)))
                  (when (and (< end (length text)) (find (char text end) "eE"))
                    (incf end)
                    (when (and (< end (length text)) (find (char text end) "+-")) (incf end))
                    (let ((next (digits end 10)))
                      (when (= end next) (return-from startup-config-bare-value-p nil))
                      (setf end next)))
                  (= end (length text)))))))))

(defun startup-absolute-config-path (path)
  "Canonicalize lexically, like AbsoluteConfigLocation, without following symlinks."
  (let ((absolute (namestring (merge-pathnames path (uiop:getcwd))))
        (parts nil))
    (dolist (part (uiop:split-string absolute :separator '(#\/)))
      (cond ((string= part "..") (pop parts))
            ((member part '("" ".") :test #'string=))
            (t (push part parts))))
    (pathname (format nil "/~{~a~^/~}" (nreverse parts)))))

(defun startup-config-value (line position)
  "Parse a Latin-1 byte view into literal octets, without the Lisp reader.
Octal escapes produce raw bytes; retain only the prefix before the first NUL,
as in a C string. Do not reencode the byte view as UTF-8."
  (let ((length (length line)) (index position))
    (labels ((space-p (char) (find char '(#\Space #\Tab #\Return)))
             (skip () (loop while (and (< index length) (space-p (char line index))) do (incf index))))
      (skip)
      (when (or (= index length) (char= (char line index) #\#))
        (error "missing configuration value"))
      (let ((value
              (if (char= (char line index) #\')
                  (progn
                    (incf index)
                    (with-output-to-string (out)
                      (loop
                        (when (= index length) (error "unterminated configuration string"))
                        (let ((char (char line index)))
                          (incf index)
                          (cond
                            ((char= char #\')
                             (if (and (< index length) (char= (char line index) #\'))
                                 (progn (write-char char out) (incf index))
                                 (return)))
                            ((char= char #\\)
                             (when (= index length) (error "unterminated configuration escape"))
                             (multiple-value-bind (escaped next) (startup-config-escape line index)
                               (setf index next)
                               (write-char escaped out)))
                            (t (write-char char out)))))))
                  (let ((start index))
                    (loop while (and (< index length) (not (space-p (char line index)))
                                     (not (char= (char line index) #\#))) do (incf index))
                    (let ((bare (subseq line start index)))
                      (unless (startup-config-bare-value-p bare)
                        (error "invalid unquoted configuration token ~s" bare))
                      bare)))))
        (skip)
        (unless (or (= index length) (char= (char line index) #\#))
          (error "trailing configuration tokens"))
        ;; Deescaped literals enter C's GUC layer as NUL-terminated strings.
        (map '(vector (unsigned-byte 8)) #'char-code
             (subseq value 0 (position (code-char 0) value)))))))

(defun startup-config-line (line)
  (when (find (code-char 0) line) (error "literal NUL in configuration input"))
  (let* ((text (string-left-trim '(#\Space #\Tab #\Return) line))
         (end (position-if (lambda (char) (or (find char '(#\Space #\Tab #\Return))
                                              (char= char #\=))) text)))
    (unless (or (zerop (length text)) (char= (char text 0) #\#))
      (let* ((name (string-downcase (subseq text 0 end)))
             (position (or end (length text))))
        (unless (startup-config-name-p name) (error "invalid configuration name"))
        (loop while (and (< position (length text))
                         (find (char text position) '(#\Space #\Tab #\Return))) do (incf position))
        (when (and (< position (length text)) (char= (char text position) #\=)) (incf position))
        (values name (startup-config-value text position))))))

(defun startup-include-path (value parent)
  (merge-pathnames (startup-input-text value) (uiop:pathname-directory-pathname parent)))

(defun startup-open-config (path &optional (maximum (* 16 1024 1024)))
  "Open a bounded regular config file as a Latin-1 byte view.
Follow symlinks, but reject special files without blocking the reload gate."
  (let* ((fd (sb-posix:open (namestring path)
                            (logior sb-posix:o-rdonly sb-posix:o-nonblock +postmaster-o-cloexec+)))
         (success nil))
    (unwind-protect
         (let ((stat (sb-posix:fstat fd)))
           (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                        (<= 0 (sb-posix:stat-size stat) maximum))
             (error "configuration input must be a regular file of at most ~d bytes" maximum))
           (prog1 (sb-sys:make-fd-stream fd :input t :element-type 'character
                                            :external-format :latin-1 :auto-close t)
             (setf success t)))
      (unless success (sb-posix:close fd)))))

(defun startup-apply-config-line (name value path number table depth)
  (cond
    ((member name '("include" "include_if_exists") :test #'string=)
     (startup-read-config (startup-include-path value path) table (1+ depth)
                          (string= name "include_if_exists")))
    ((string= name "include_dir")
     (let ((directory (uiop:ensure-directory-pathname (startup-include-path value path))))
       (unless (uiop:directory-exists-p directory)
         (error "configuration include directory does not exist"))
       (dolist (file (sort (uiop:directory-files directory) #'string< :key #'file-namestring))
         (when (and (equal (pathname-type file) "conf")
                    (not (char= (char (file-namestring file) 0) #\.)))
           (startup-read-config file table (1+ depth))))))
    (t
     (setf (gethash name table)
           (list :value (startup-normalize-setting name value)
                 :source :file :sourcefile (namestring path) :sourceline number)))))

(defun startup-read-config (path table &optional (depth 0) optional)
  "Apply includes in lexical order, retaining each assignment's file and line."
  (when (> depth 10) (error "configuration include depth exceeds 10"))
  (unless (probe-file path)
    (if optional (return-from startup-read-config table)
        (error "configuration file does not exist: ~a" path)))
  (setf path (startup-absolute-config-path path))
  (with-open-stream (stream (startup-open-config path))
    (loop for line = (read-line stream nil nil) while line for number from 1
          do (handler-case
                 (multiple-value-bind (name value) (startup-config-line line)
                   (when name
                     (startup-apply-config-line name value path number table depth)))
               (error (condition)
                 (error "~a:~d: ~a" path number condition)))))
  table)

(defparameter *startup-native-options*
  '(("-D" . "pgdata") ("--pgdata" . "pgdata")
    ("--config-file" . "config_file")
    ("--control-port" . "control-port")
    ("--control-socket" . "control-socket")
    ("--swank-port" . "swank-port") ("--swank-bind-address" . "swank-bind-address")
    ("--temp-file-limit-bytes" . "temp-file-limit-bytes")
    ("--sort-arena-limit-bytes" . "sort-arena-limit-bytes")
    ("--max-slot-wal-keep-bytes" . "max-slot-wal-keep-bytes")))

(defun startup-set-command-setting (settings assignment)
  (let ((equals (position #\= assignment)))
    (unless (and equals (plusp equals)) (error "-c requires name=value"))
    (let ((name (substitute #\_ #\- (string-downcase (subseq assignment 0 equals)))))
      (unless (or (string= name "config_file")
                  (assoc name *startup-setting-specs* :test #'string=))
        (error "unrecognized configuration parameter ~s" name))
      (setf (gethash name settings) (subseq assignment (1+ equals))))))

(defun startup-cli-inputs (argv)
  "Consume values once and reject unknown flags or positional arguments.
For -C, values containing only decimal digits select the native control port;
other values request configuration output. The last -C selects between these uses."
  (let ((native (make-hash-table :test #'equal))
        (settings (make-hash-table :test #'equal)))
    (loop while argv
          for argument = (pop argv)
          for equals = (and (uiop:string-prefix-p "--" argument) (position #\= argument))
          for attached = (and (> (length argument) 2)
                              (member (subseq argument 0 2) '("-D" "-p" "-h" "-c" "-C") :test #'string=))
          for option = (cond (attached (subseq argument 0 2))
                             (equals (subseq argument 0 equals)) (t argument))
          for key = (cdr (assoc option *startup-native-options* :test #'string=))
          do (labels ((value ()
                        (cond (attached (subseq argument 2))
                              (equals (subseq argument (1+ equals)))
                              (argv (pop argv))
                              (t (error "option ~a requires a value" option)))))
               (cond
                 ((string= option "--")
                  (when argv (error "unexpected positional server arguments"))
                  (return))
                 ((string= option "-C")
                  (let* ((text (value))
                         (numeric (and (plusp (length text))
                                       (every (lambda (char) (find char "0123456789")) text))))
                    (remhash (if numeric "config-output" "control-port") native)
                    (setf (gethash (if numeric "control-port" "config-output") native) text)))
                 (key (setf (gethash key (if (string= key "config_file") settings native)) (value)))
                 ((string= option "-c") (startup-set-command-setting settings (value)))
                 ((member option '("-p" "--port" "-h" "--host") :test #'string=)
                  (setf (gethash (if (member option '("-p" "--port") :test #'string=)
                                     "port" "listen_addresses") settings) (value)))
                 ((and equals (uiop:string-prefix-p "--" argument))
                  (startup-set-command-setting settings (subseq argument 2)))
                 (t (error "unrecognized server option or argument ~s" argument)))))
    (when (gethash "config_file" settings)
      (setf (gethash "config_file" native) (gethash "config_file" settings))
      (remhash "config_file" settings))
    (values native settings)))

(defun startup-setting-value (settings name)
  (getf (gethash name settings) :value))

(defun startup-recovery-timeline-goal ()
  "Return the active cold-start timeline goal, defaulting to latest.
A reload may record a different desired value, but does not change this goal."
  (startup-parse-recovery-timeline
   (or (when *startup-server-settings*
         (startup-setting-value *startup-server-settings* "recovery_target_timeline"))
       "latest")))

(defun startup-default-settings (directory)
  (let ((settings (make-hash-table :test #'equal)))
    (dolist (spec *startup-setting-specs*)
      (setf (gethash (first spec) settings)
            (list :value (cond ((string= (first spec) "data_directory") directory)
                               ((member (first spec) '("hba_file" "ident_file") :test #'string=) nil)
                               (t (startup-normalize-setting (first spec) (fourth spec))))
                  :source :default)))
    settings))

(defun startup-check-feature-support (settings)
  "Reject effective settings that enable unsupported features.
Minimal WAL generation is unavailable. Prepared transactions must be zero,
commit timestamp tracking off, and each listed recovery target empty.
Return SETTINGS after validation."
  (unless (member (startup-setting-value settings "wal_level") '("replica" "logical") :test #'equal)
    (error "wal_level=minimal requires unimplemented minimal WAL generation"))
  (when (plusp (startup-setting-value settings "max_prepared_transactions"))
    (error "positive max_prepared_transactions requires unimplemented prepared transactions"))
  (when (startup-setting-value settings "track_commit_timestamp")
    (error "track_commit_timestamp=on requires unimplemented commit timestamp tracking"))
  (dolist (name *startup-recovery-target-settings*)
    (unless (equal "" (startup-setting-value settings name))
      (error "nonempty ~a requires unimplemented targeted recovery" name)))
  settings)

(defun startup-resolve-auth-files (settings directory)
  "Resolve implicit HBA and ident defaults under DIRECTORY, the original config
directory. Explicit relative paths resolve against the invocation directory.
Record whether each path was implicit."
  (dolist (pair '(("hba_file" . "pg_hba.conf") ("ident_file" . "pg_ident.conf")))
    (let* ((name (car pair))
           (value (startup-setting-value settings name)))
      (unless (or value directory)
        (error "~a requires an explicit path or a configuration directory" name))
      (setf (gethash name settings)
            (list :value (namestring
                          (startup-absolute-config-path
                           (if value value (merge-pathnames (cdr pair) directory))))
                  :source :override :implicit-path (null value)))))
  settings)

(defun startup-load-settings (native overrides &optional reload-directory)
  "Read primary/includes, then auto.conf, then command-line overrides.
Check feature support after applying all sources, so later overrides take precedence.
Resolve authentication paths after all settings sources have been applied."
  (let* ((directory (namestring (directory-pathname
                                 (or (gethash "pgdata" native) (uiop:getenv "PGDATA") "./pgdata"))))
         (configuration-directory
           (when (or (gethash "pgdata" native) (uiop:getenv "PGDATA")
                     (not (gethash "config_file" native)))
             directory))
         (explicit (gethash "config_file" native))
         (path (if explicit (startup-absolute-config-path explicit)
                   (merge-pathnames "postgresql.conf" directory)))
         (settings (startup-default-settings directory)))
    (when (or explicit (probe-file path))
      (startup-read-config path settings))
    (when (gethash "data_directory" overrides)
      (setf (gethash "data_directory" settings)
            (list :value (startup-normalize-setting "data_directory"
                                                    (gethash "data_directory" overrides))
                  :source :command-line)))
    (setf directory (namestring (directory-pathname (startup-setting-value settings "data_directory"))))
    (let* ((auto-path (merge-pathnames "postgresql.auto.conf" (or reload-directory directory)))
           (auto-settings (make-hash-table :test #'equal)))
      (when (probe-file auto-path)
        (startup-read-config auto-path auto-settings)
        (when (gethash "data_directory" auto-settings)
          (error "data_directory is not allowed in postgresql.auto.conf"))
        (maphash (lambda (name entry) (setf (gethash name settings) entry)) auto-settings)))
    (maphash (lambda (name value)
               (setf (gethash name settings)
                     (list :value (startup-normalize-setting name value) :source :command-line)))
             overrides)
    ;; SelectConfigFiles fixes both location GUCs with PGC_S_OVERRIDE.
    (setf (getf (gethash "data_directory" settings) :value) directory
          (getf (gethash "data_directory" settings) :source) :override)
    (setf (gethash "config_file" settings)
          (list :value (namestring (startup-absolute-config-path path)) :source :override))
    (startup-resolve-auth-files settings configuration-directory)
    (startup-check-feature-support settings)))

(defun startup-native-limit (native key environment parser fallback)
  (let ((value (or (gethash key native) (uiop:getenv environment))))
    (if value (funcall parser value) fallback)))

(defun startup-effective-slot-limit (settings native)
  "Select the explicit native byte cap or the standard limit; NIL means unlimited."
  (wal-slot-retention-policy-limit (startup-effective-slot-policy settings native)))

(defun startup-effective-slot-policy (settings native)
  "Choose exact-byte mode only for an explicit native option or environment value."
  (let ((bytes (or (gethash "max-slot-wal-keep-bytes" native)
                   (uiop:getenv "PG_MAX_SLOT_WAL_KEEP_BYTES")))
        (megabytes (startup-setting-value settings "max_slot_wal_keep_size")))
    (make-wal-slot-retention-policy
     :mode (if bytes :bytes :segments)
     :limit (if bytes (parse-slot-wal-keep-bytes bytes)
                (unless (= megabytes -1) (* megabytes 1024 1024)))
     :max-wal-bytes (* 1024 1024 (startup-setting-value settings "max_wal_size")))))

(defun startup-argument-values (argv)
  (multiple-value-bind (native overrides) (startup-cli-inputs argv)
    (let* ((settings (startup-load-settings native overrides))
           (host (startup-setting-value settings "listen_addresses"))
           (port (startup-setting-value settings "port"))
           (control-port (gethash "control-port" native))
           (swank-port (gethash "swank-port" native)))
      (values (if (string= host "*") "0.0.0.0" host)
              port (startup-setting-value settings "data_directory")
              (when control-port (parse-port-arg "--control-port" control-port))
              (gethash "control-socket" native)
              (when swank-port (parse-port-arg "--swank-port" swank-port))
              (parse-bind-address-arg "--swank-bind-address"
                                      (or (gethash "swank-bind-address" native) "127.0.0.1"))
              (startup-native-limit native "temp-file-limit-bytes" "PG_QUERY_TEMP_FILE_LIMIT_BYTES"
                                    #'parse-query-temp-file-limit-bytes *query-temp-file-limit-bytes*)
              (startup-native-limit native "sort-arena-limit-bytes" "PG_QUERY_SORT_ARENA_LIMIT_BYTES"
                                    #'parse-query-sort-arena-limit-bytes *query-sort-arena-limit-bytes*)
              (startup-effective-slot-limit settings native)
              settings native overrides))))

(defun startup-check-connection-limits (settings)
  "Check reserved connection slots and plan the process and lock capacities."
  (let ((connections (startup-setting-value settings "max_connections"))
        (reserved (startup-setting-value settings "reserved_connections"))
        (superuser (startup-setting-value settings "superuser_reserved_connections")))
    (unless (< (+ reserved superuser) connections)
      (error "reserved_connections plus superuser_reserved_connections must be less than max_connections"))
    (startup-resource-capacity-plan settings)))

(defun startup-resource-capacity-plan (settings)
  "Plan the installed tuple and relation lock capacities from startup settings.
All five fields use active settings. Prepared transactions are accepted only
when the validated setting is zero."
  (startup-check-feature-support settings)
  (let ((parameters (make-resource-parameters
                     (startup-setting-value settings "max_connections")
                     (startup-setting-value settings "max_worker_processes")
                     (startup-setting-value settings "max_wal_senders")
                     (startup-setting-value settings "max_prepared_transactions")
                     (startup-setting-value settings "max_locks_per_transaction"))))
    (multiple-value-bind (objects owners) (resource-lock-capacities parameters)
      (values parameters objects owners))))

(defun startup-install-resource-capacity (settings)
  "Install the tuple, relation limits and managed-worker slots once at startup.
Check existing relation registrations before publishing under the relation and
worker locks. Auxiliary services have separate lifecycles and do not consume
these slots."
  (multiple-value-bind (parameters objects owners) (startup-resource-capacity-plan settings)
    (let ((slots (make-array (resource-parameters-workers parameters) :initial-element nil)))
      (sb-thread:with-mutex (*relation-lock-mutex*)
        (sb-thread:with-mutex (*managed-worker-lock*)
          (when (or *installed-resource-parameters* *managed-worker-slots*)
            (error "resource capacity is already installed; restart is required"))
          (when (shutdown-in-progress-p)
            (error "cannot install resource capacity during shutdown"))
          (multiple-value-bind (used-objects used-owners) (relation-resource-footprint-under-lock)
            (unless (and (<= used-objects objects) (<= used-owners owners))
              (error "installed resource capacity cannot cover existing relation registrations")))
          (sb-sys:without-interrupts
            (setf *relation-lock-object-limit* objects
                  *relation-lock-owner-limit* owners
                  *managed-worker-slots* slots
                  *managed-worker-registration-closed-p* nil
                  *installed-resource-parameters* parameters)))))
    parameters))

(defun startup-check-installation-predecessor (candidate)
  "Reject a candidate whose predecessor is no longer the installed identity."
  (let ((installed (startup-capture-configuration)))
    (unless (eq (startup-configuration-predecessor candidate)
                (when installed (startup-configuration-identity installed)))
      (error "configuration generation changed before installation")))
  (values))

(defun startup-publish-configuration (candidate)
  "Publish prepared values and the root under the root mutex.
Publication still uses live-operation quiescence. Migrated readers capture the
root; retention and restartpoint readers still use globals."
  (sb-thread:with-mutex (*startup-configuration-lock*)
    (setf *authentication-policy* (startup-configuration-authentication candidate)
          *startup-server-settings* (startup-configuration-settings candidate)
          *startup-native-inputs* (startup-configuration-native candidate)
          *startup-command-overrides* (startup-configuration-overrides candidate)
          *enable-hot-standby* (startup-configuration-hot-standby candidate)
          *wal-level* (startup-configuration-wal-level candidate)
          *wal-keep-size-mb* (startup-configuration-keep-mb candidate)
          *physical-restartpoint-timeout* (startup-configuration-timeout candidate)
          *physical-restartpoint-wal-bytes* (startup-configuration-wal-bytes candidate)
          *max-slot-wal-keep-bytes* (wal-slot-retention-policy-limit (startup-configuration-slot-policy candidate))
          *wal-slot-retention-policy* (startup-configuration-slot-policy candidate)
          *max-replication-slots* (startup-configuration-max-slots candidate)
          *max-connections* (startup-configuration-max-connections candidate)
          *max-wal-senders* (startup-configuration-max-senders candidate)
          *startup-installed-configuration* candidate))
  (values))

(defun startup-commit-installation (candidate)
  "Check and publish CANDIDATE with its desired follow generation."
  (check-type candidate startup-configuration)
  (physical-follow-call-with-desired-install
   (startup-configuration-desired candidate)
   (lambda () (startup-publish-configuration candidate))
   (lambda () (startup-check-installation-predecessor candidate)))
  candidate)

(defun startup-install-settings (settings native overrides &optional (policy nil supplied-p))
  "Prepare and install owned settings under quiescence; return the original SETTINGS."
  (startup-commit-installation
   (if supplied-p
       (startup-prepare-installation settings native overrides policy)
       (startup-prepare-installation settings native overrides)))
  settings)

(defun startup-reload-settings ()
  "Validate a fresh input tree before replacing active values under quiescence.
Postmaster settings retain their active values and record pending restart.  A
setting added to a warm registry uses its boot default as the old value.
Reject an unsupported effective candidate before publishing any reload changes.
Capture the base once before file I/O and use it for both inputs and predecessor."
  (let ((base (startup-capture-configuration)))
    (when base
      (let* ((native (startup-configuration-native base))
             (overrides (startup-configuration-overrides base))
             (settings (startup-configuration-settings base))
             (candidate (startup-load-settings native overrides *pgdata*))
             (defaults (startup-default-settings (namestring *pgdata*))))
        (dolist (spec *startup-setting-specs*)
          (let* ((name (first spec)) (old (or (gethash name settings) (gethash name defaults)))
                                     (new (gethash name candidate)))
            ;; SelectConfigFiles locks the resolved data path above file priority.
            (when (string= name "data_directory")
              (setf new (list :value (getf old :value) :source :override)
                    (gethash name candidate) new))
            (when (and (eq (second spec) :postmaster)
                       (or (not (equal (getf old :value) (getf new :value)))
                           ;; C marks removal even when the file held the boot value.
                           (and (eq (getf old :source) :file)
                                (eq (getf new :source) :default))))
              (setf (gethash name candidate)
                    (list* :pending-restart t :desired-value (getf new :value) (copy-list old)))
              (admin-log "configuration parameter ~a requires restart" name))))
        (startup-commit-installation
         (startup-prepare-installation-from-base candidate native overrides base nil nil))
        candidate))))

(defun startup-format-unit (value kind)
  (if (<= value 0) (princ-to-string value)
      (let ((units (ecase kind
                     (:seconds '(("d" 86400) ("h" 3600) ("min" 60) ("s" 1)))
                     (:bytes '(("TB" 1099511627776) ("GB" 1073741824) ("MB" 1048576)
                               ("kB" 1024) ("B" 1))))))
        (loop for (unit multiplier) in units
              when (zerop (mod value multiplier))
                return (format nil "~d~a" (/ value multiplier) unit)))))

(defun startup-check-session-setting (name)
  "Enforce guc.c's POSTMASTER/SIGHUP contexts for SQL SET and RESET."
  (let ((spec (assoc name *startup-setting-specs* :test #'string=)))
    (when (or spec (string= name "config_file"))
      (ereport "55P02" "parameter ~s cannot be changed~a"
               name (if (or (string= name "config_file")
                            (eq (second spec) :postmaster))
                        " without restarting the server" " now")))))

(defun startup-settings-row (name entry &optional (configuration (startup-capture-configuration)))
  "Expose actual installed settings in guc_funcs.c column units and provenance.
WAL level enum values are a text array of canonical names; aliases are hidden.
Other supported setting rows have NULL enum values. CONFIGURATION supplies the
slot policy and provenance from the same root as ENTRY, without rereading the
environment."
  (let* ((spec (assoc name *startup-setting-specs* :test #'string=))
         (source (getf entry :source))
         (unit (case (seventh spec) (:seconds "s") (:megabytes "MB") (t +null+)))
         (value (getf entry :value)))
    ;; The native byte option predates GUC support and can specify partial MB.
    ;; Preserve its true value, unit and provenance instead of rounding it away.
    (when (and configuration (string= name "max_slot_wal_keep_size"))
      (let ((policy (startup-configuration-slot-policy configuration)))
        (when (eq :bytes (wal-slot-retention-policy-mode policy))
          (setf value (or (wal-slot-retention-policy-limit policy) -1)
                unit "B"
                source (if (gethash "max-slot-wal-keep-bytes"
                                    (startup-configuration-native configuration))
                           :command-line :environment)))))
    (vector name (startup-setting-text name value) unit
            (if spec (string-downcase (symbol-name (second spec))) "postmaster")
            (case (third spec) (:integer "integer") (:boolean "bool") (:wal-level "enum") (t "string"))
            (ecase source
              (:default "default") (:file "configuration file") (:override "override")
              (:command-line "command line") (:environment "environment variable"))
            (if (eq (third spec) :wal-level) (make-sql-array :element-type-id :text
                                                             :elements (vector "minimal" "replica" "logical")
                                                             :dimensions '(3) :lower-bounds '(1)) +null+)
            (if (eq source :file) (or (getf entry :sourcefile) +null+) +null+)
            (if (eq source :file) (or (getf entry :sourceline) +null+) +null+)
            (not (null (getf entry :pending-restart))))))

(defun startup-settings-rows ()
  "Snapshot supported installed server and session GUCs; omit custom placeholders.
Use one root for all server rows. Without an installed root, return session rows."
  (let* ((configuration (startup-capture-configuration))
         (settings (when configuration (startup-configuration-settings configuration))))
    (append
     (when settings
       (loop for name in (sort (loop for name being the hash-keys of settings collect name) #'string<)
             collect (startup-settings-row name (gethash name settings) configuration)))
     (list (session-timeout-settings-row) (session-application-name-settings-row)))))

(defun startup-setting-show-value (name)
  "Read NAME from one installed root; return no match when none is installed."
  (let ((configuration (startup-capture-configuration)))
    (when configuration
      (let ((entry (gethash name (startup-configuration-settings configuration))))
        (when entry
          (let ((value (getf entry :value)))
            (values (case (intern (string-upcase name) :keyword)
                      (:checkpoint_timeout (startup-format-unit value :seconds))
                      ((:max_wal_size :wal_keep_size)
                       (startup-format-unit (* value 1024 1024) :bytes))
                      (:max_slot_wal_keep_size
                       (let ((limit (wal-slot-retention-policy-limit
                                     (startup-configuration-slot-policy configuration))))
                         (if limit (startup-format-unit limit :bytes) "-1")))
                      (t (startup-setting-text name value)))
                    t)))))))

(defun startup-configuration-output (argv)
  "Return the requested -C value in base units using normal configuration precedence.
Require an existing data directory. Resolve names case-insensitively without
installing settings, taking PGDATA ownership or starting listeners."
  (multiple-value-bind (native overrides) (startup-cli-inputs argv)
    (let ((name (gethash "config-output" native)))
      (when name
        (let* ((settings (startup-load-settings native overrides))
               (entry (gethash (string-downcase name) settings)))
          (unless (uiop:directory-exists-p (startup-setting-value settings "data_directory"))
            (error "configuration output requires an existing data directory"))
          (unless entry (error "unrecognized configuration parameter ~s" name))
          (let ((value (getf entry :value)))
            (if (and (string-equal name "data_directory") (> (length value) 1))
                (string-right-trim "/" value)
                (startup-setting-text (string-downcase name) value))))))))

(defun startup-print-configuration-value (value)
  (finish-output *standard-output*)
  (let ((bytes (etypecase value
                 (string (string->bytes value))
                 ((vector (unsigned-byte 8)) value)))
        (output (sb-sys:make-fd-stream 1 :output t :element-type '(unsigned-byte 8)
                                         :buffering :none :auto-close nil)))
    (write-sequence bytes output)
    (write-byte 10 output)
    (finish-output output)))

(defun startup-description-rows ()
  "Build the native registry subset in C --describe-config field order.
As in C's zero-initialized reset fields, booleans report FALSE and integers 0;
strings and enums use boot values. These are not runtime or -C defaults."
  (loop for (name context kind boot minimum maximum) in *startup-setting-specs*
        for type = (case kind
                     (:integer "INTEGER") (:boolean "BOOLEAN") (:wal-level "ENUM")
                     (t "STRING"))
        for default = (case kind
                        ;; C GucInfoMain builds the registry without initializing reset fields.
                        (:boolean "FALSE")
                        (:integer "0")
                        (t (or boot "")))
        collect (list name (string-downcase (symbol-name context))
                      "Native server settings" type default
                      (if (eq kind :integer) (princ-to-string minimum) "")
                      (if (eq kind :integer) (princ-to-string maximum) "")
                      "" "")))

(defun startup-print-description (&optional (output *standard-output*))
  "Print nine tab-separated fields per supported native server setting.
The group is Native server settings and both description fields are empty."
  (dolist (row (startup-description-rows))
    (loop for field in row
          for first = t then nil
          unless first do (write-char #\Tab output)
            do (write-string field output))
    (terpri output)))

(defun startup-handle-meta-request (argv)
  "Print and exit for version or configuration requests before server startup.
A first argument of --describe-config prints the registry and exits before startup."
  (when (or (equal argv '("--version")) (equal argv '("-V")))
    (format t "pg-lisp (PostgreSQL) ~a~%" *server-version*)
    (finish-output)
    (sb-ext:exit :code 0))
  (when (equal (first argv) "--describe-config")
    (startup-print-description)
    (finish-output)
    (sb-ext:exit :code 0))
  (let ((value (startup-configuration-output argv)))
    (when value
      (startup-print-configuration-value value)
      (sb-ext:exit :code 0))))
