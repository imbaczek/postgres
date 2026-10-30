;;;; Startup configuration inputs and typed settings used by the native server.
(in-package #:pg)

(defvar *startup-server-settings* nil)

(defvar *startup-native-inputs* nil)

(defvar *startup-command-overrides* nil)

(declaim (special *managed-worker-slots* *managed-worker-lock*
                  *managed-worker-registration-closed-p*))

(declaim (special *physical-restartpoint-timeout* *physical-restartpoint-wal-bytes*))

(declaim (ftype function directory-pathname parse-port-arg parse-query-temp-file-limit-bytes
                parse-query-sort-arena-limit-bytes parse-slot-wal-keep-bytes
                parse-bind-address-arg))

(defparameter *startup-setting-specs*
  '(("port" :postmaster :integer "5433" 1 65535 nil)
    ("listen_addresses" :postmaster :string "127.0.0.1")
    ("data_directory" :postmaster :string nil)
    ("cluster_name" :postmaster :string "")
    ("restore_command" :sighup :string "")
    ("primary_conninfo" :sighup :conninfo "")
    ("primary_slot_name" :sighup :slot "")
    ("recovery_target_timeline" :postmaster :timeline "latest")
    ("hot_standby" :postmaster :boolean "on")
    ("max_connections" :postmaster :integer "100" 1 262143 nil)
    ("max_wal_senders" :postmaster :integer "10" 0 262143 nil)
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
        (:boolean (startup-parse-boolean-setting text))
        (:timeline (startup-parse-recovery-timeline text) text)
        (:string
         (when (and (string= name "restore_command") (not (equal text "")))
           (error "nonempty restore_command requires unimplemented archive recovery"))
         (unless (or (string= name "restore_command") (plusp (length text)))
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
  (let* ((absolute (merge-pathnames path (uiop:getcwd)))
         (parts nil))
    (dolist (part (rest (pathname-directory absolute)))
      (cond ((or (member part '(:up :back)) (equal part "..")) (pop parts))
            ((member part '("" ".") :test #'equal))
            (t (push part parts))))
    (make-pathname :directory (cons :absolute (nreverse parts)) :defaults absolute)))

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

(defun startup-open-config (path)
  "Open a bounded regular config file as a Latin-1 byte view.
Follow symlinks, but reject special files without blocking the reload gate."
  (let* ((fd (sb-posix:open (namestring path)
                            (logior sb-posix:o-rdonly sb-posix:o-nonblock +postmaster-o-cloexec+)))
         (success nil))
    (unwind-protect
         (let ((stat (sb-posix:fstat fd)))
           (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                        (<= (sb-posix:stat-size stat) (* 16 1024 1024)))
             (error "configuration input must be a regular file of at most 16 MiB"))
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
            (list :value (if (string= (first spec) "data_directory")
                             directory (startup-normalize-setting (first spec) (fourth spec)))
                  :source :default)))
    settings))

(defun startup-load-settings (native overrides &optional reload-directory)
  "Read primary/includes, then auto.conf, then command-line overrides."
  (let* ((directory (namestring (directory-pathname
                                 (or (gethash "pgdata" native) (uiop:getenv "PGDATA") "./pgdata"))))
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
    settings))

(defun startup-native-limit (native key environment parser fallback)
  (let ((value (or (gethash key native) (uiop:getenv environment))))
    (if value (funcall parser value) fallback)))

(defun startup-effective-slot-limit (settings native)
  "Select the explicit native byte cap or the standard limit; NIL means unlimited."
  (let ((bytes (or (gethash "max-slot-wal-keep-bytes" native)
                   (uiop:getenv "PG_MAX_SLOT_WAL_KEEP_BYTES")))
        (megabytes (startup-setting-value settings "max_slot_wal_keep_size")))
    (if bytes (parse-slot-wal-keep-bytes bytes)
        (unless (= megabytes -1) (* megabytes 1024 1024)))))

(defun startup-effective-slot-policy (settings native)
  "Choose exact-byte mode only for an explicit native option or environment value."
  (make-wal-slot-retention-policy
   :mode (if (or (gethash "max-slot-wal-keep-bytes" native)
                 (uiop:getenv "PG_MAX_SLOT_WAL_KEEP_BYTES")) :bytes :segments)
   :limit (startup-effective-slot-limit settings native)
   :max-wal-bytes (* 1024 1024 (startup-setting-value settings "max_wal_size"))))

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
Connections, WAL senders and locks per transaction use active settings; worker
processes and prepared transactions use internal values 8 and 0. The lock setting
has default 128 and range 10 through INT_MAX, and requires a restart to change."
  (let ((parameters (make-resource-parameters
                     (startup-setting-value settings "max_connections") 8
                     (startup-setting-value settings "max_wal_senders") 0
                     (startup-setting-value settings "max_locks_per_transaction"))))
    (multiple-value-bind (objects owners) (resource-lock-capacities parameters)
      (values parameters objects owners))))

(defun startup-install-resource-capacity (settings)
  "Install the tuple and relation limits once under the relation lock.
Check existing registrations fit before publishing either limit."
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

(defun startup-install-settings (settings native overrides)
  "Validate and install settings, including session quotas before listen.
Install wal_keep_size in MB on successful reload; postmaster settings such as
hot_standby retain their active values until restart. Settings and the desired
follow generation are published together under live-operation quiescence."
  (startup-check-connection-limits settings)
  (let ((timeout (startup-setting-value settings "checkpoint_timeout"))
        (wal-bytes (* 1024 1024 (startup-setting-value settings "max_wal_size")))
        (slot-policy (startup-effective-slot-policy settings native))
        (desired (physical-follow-plan-settings settings)))
    (physical-follow-call-with-desired-install desired
                                               (lambda ()
                                                 (setf *startup-server-settings* settings
                                                       *startup-native-inputs* native *startup-command-overrides* overrides
                                                       *enable-hot-standby* (startup-setting-value settings "hot_standby")
                                                       *wal-keep-size-mb* (startup-setting-value settings "wal_keep_size")
                                                       *physical-restartpoint-timeout* timeout
                                                       *physical-restartpoint-wal-bytes* wal-bytes
                                                       *max-slot-wal-keep-bytes* (wal-slot-retention-policy-limit slot-policy)
                                                       *wal-slot-retention-policy* slot-policy
                                                       *max-replication-slots* (startup-setting-value settings "max_replication_slots")
                                                       *max-connections* (startup-setting-value settings "max_connections")
                                                       *max-wal-senders* (startup-setting-value settings "max_wal_senders")))
                                               ))
  settings)

(defun startup-reload-settings ()
  "Validate a fresh input tree before replacing active values under quiescence.
Postmaster settings retain their active values and record pending restart.  A
setting added to a warm registry uses its boot default as the old value."
  (when *startup-server-settings*
    (let ((candidate (startup-load-settings *startup-native-inputs* *startup-command-overrides* *pgdata*))
          (defaults (startup-default-settings (namestring *pgdata*))))
      (dolist (spec *startup-setting-specs*)
        (let* ((name (first spec)) (old (or (gethash name *startup-server-settings*) (gethash name defaults)))
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
      (startup-install-settings candidate *startup-native-inputs* *startup-command-overrides*))))

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

(defun startup-settings-row (name entry)
  "Expose actual installed settings in guc_funcs.c column units and provenance."
  (let* ((spec (assoc name *startup-setting-specs* :test #'string=))
         (source (getf entry :source))
         (unit (case (seventh spec) (:seconds "s") (:megabytes "MB") (t +null+)))
         (value (getf entry :value)))
    ;; The native byte option predates GUC support and can specify partial MB.
    ;; Preserve its true value, unit and provenance instead of rounding it away.
    (when (string= name "max_slot_wal_keep_size")
      (let ((command (and *startup-native-inputs*
                          (gethash "max-slot-wal-keep-bytes" *startup-native-inputs*)))
            (environment (uiop:getenv "PG_MAX_SLOT_WAL_KEEP_BYTES")))
        (when (or command environment)
          (setf value (or *max-slot-wal-keep-bytes* -1)
                unit "B" source (if command :command-line :environment)))))
    (vector name (startup-setting-text name value) unit
            (if spec (string-downcase (symbol-name (second spec))) "postmaster")
            (case (third spec) (:integer "integer") (:boolean "bool") (t "string"))
            (ecase source
              (:default "default") (:file "configuration file") (:override "override")
              (:command-line "command line") (:environment "environment variable"))
            (if (eq source :file) (or (getf entry :sourcefile) +null+) +null+)
            (if (eq source :file) (or (getf entry :sourceline) +null+) +null+)
            (not (null (getf entry :pending-restart))))))

(defun startup-settings-rows ()
  "Snapshot supported installed server and session GUCs; omit custom placeholders."
  (let ((settings *startup-server-settings*))
    (append
     (when settings
       (loop for name in (sort (loop for name being the hash-keys of settings collect name) #'string<)
             collect (startup-settings-row name (gethash name settings))))
     (list (session-timeout-settings-row) (session-application-name-settings-row)))))

(defun startup-setting-show-value (name)
  (when *startup-server-settings*
    (let ((entry (gethash name *startup-server-settings*)))
      (when entry
        (let ((value (getf entry :value)))
          (values (case (intern (string-upcase name) :keyword)
                    (:checkpoint_timeout (startup-format-unit value :seconds))
                    ((:max_wal_size :wal_keep_size)
                     (startup-format-unit (* value 1024 1024) :bytes))
                    (:max_slot_wal_keep_size
                     (if *max-slot-wal-keep-bytes* (startup-format-unit *max-slot-wal-keep-bytes* :bytes) "-1"))
                    (t (startup-setting-text name value)))
                  t))))))

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

(defun startup-handle-meta-request (argv)
  "Print and exit for version or configuration requests before server startup."
  (when (or (equal argv '("--version")) (equal argv '("-V")))
    (format t "pg-lisp (PostgreSQL) ~a~%" *server-version*)
    (finish-output)
    (sb-ext:exit :code 0))
  (let ((value (startup-configuration-output argv)))
    (when value
      (startup-print-configuration-value value)
      (sb-ext:exit :code 0))))
