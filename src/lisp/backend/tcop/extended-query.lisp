;;;; Extended query protocol state and bounded frame parsing.

(in-package #:pg)

(defstruct extended-statement ast types)

(defstruct extended-portal statement values (executed-p nil))

(defstruct extended-select-portal-metadata columns formats snapshot registration)

(defvar *extended-select-portal-metadata* (make-hash-table :test #'eq :weakness :key))

(defvar *extended-select-portal-metadata-lock* (sb-thread:make-mutex :name "extended SELECT portal metadata"))

(defun extended-select-portal-info (portal)
  (sb-thread:with-mutex (*extended-select-portal-metadata-lock*)
    (gethash portal *extended-select-portal-metadata*)))

(defun extended-portal-select-p (portal) (not (null (extended-select-portal-info portal))))

(defun extended-portal-result-columns (portal)
  (let ((info (extended-select-portal-info portal))) (and info (extended-select-portal-metadata-columns info))))

(defun extended-portal-result-formats (portal)
  (let ((info (extended-select-portal-info portal))) (and info (extended-select-portal-metadata-formats info))))

(defun make-extended-query-portal (statement values select-p columns formats)
  (let ((portal (make-extended-portal :statement statement :values values)))
    (when select-p
      (sb-thread:with-mutex (*extended-select-portal-metadata-lock*)
        (setf (gethash portal *extended-select-portal-metadata*)
              (make-extended-select-portal-metadata :columns columns :formats formats))))
    portal))

(defun call-with-extended-bind-spool (input size function)
  "Read SIZE bytes into a checksummed anonymous spool, scoped to FUNCTION."
  (unless (<= 0 size (- +max-query-message-size+ 4))
    (malformed-protocol-message "invalid Bind message length"))
  (let ((owner (make-logical-wire-field-owner)))
    (unwind-protect
         (let ((output (logical-wire-field-open owner))
               (buffer (make-array 65536 :element-type '(unsigned-byte 8)))
               (remaining size))
           (multiple-value-bind (sink metrics) (logical-encoding-sink size output)
             (loop while (plusp remaining) do
               (let* ((count (min remaining (length buffer)))
                      (end (read-sequence buffer input :end count)))
                 (when (zerop end)
                   (error "connection ended during Bind body"))
                 (funcall sink (if (= end (length buffer)) buffer (subseq buffer 0 end)))
                 (decf remaining end)))
             (finish-output output)
             (unless (file-position output 0)
               (error "cannot seek Bind spool"))
             (multiple-value-bind (written crc) (funcall metrics)
               (unless (= written size) (error "invalid Bind spool size"))
               (funcall function output crc))))
      (close-logical-wire-field-owner owner))))

(defstruct extended-bind-spool stream size crc)

(defun extended-verify-bind-spool (spool)
  "Verify the complete raw Bind before using its metadata or values."
  (let ((stream (extended-bind-spool-stream spool))
        (remaining (extended-bind-spool-size spool))
        (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
    (unless (file-position stream 0) (error "cannot seek Bind spool"))
    (multiple-value-bind (sink metrics) (logical-encoding-sink remaining)
      (loop while (plusp remaining) do
        (let* ((count (min remaining (length buffer)))
               (end (read-sequence buffer stream :end count)))
          (when (zerop end) (error "truncated Bind spool"))
          (funcall sink (if (= end (length buffer)) buffer (subseq buffer 0 end)))
          (decf remaining end)))
      (multiple-value-bind (size crc) (funcall metrics)
        (unless (and (= size (extended-bind-spool-size spool))
                     (= crc (extended-bind-spool-crc spool)))
          (error "Bind spool checksum mismatch")))))
  (values))

(defun extended-parse-bind-spool (spool)
  "Parse a complete spooled Bind without materializing its parameter fields."
  (let ((stream (extended-bind-spool-stream spool))
        (remaining (extended-bind-spool-size spool)))
    (unless (file-position stream 0) (error "cannot seek Bind spool"))
    (labels ((next-octet ()
               (when (zerop remaining) (malformed-protocol-message "truncated extended query message"))
               (let ((value (read-byte stream nil nil)))
                 (unless value (error "truncated Bind spool"))
                 (decf remaining)
                 value))
             (uint (count)
               (loop repeat count for value = (next-octet)
                     for result = value then (logior (ash result 8) value)
                     finally (return result)))
             (cstring ()
               (let ((bytes (make-array 32 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)))
                 (loop for value = (next-octet) until (zerop value) do (vector-push-extend value bytes))
                 (bytes->string bytes)))
             (field ()
               (let ((size (uint 4)))
                 (if (= size #xffffffff) nil
                     (progn
                       (unless (<= size remaining) (malformed-protocol-message "truncated extended query message"))
                       (let ((offset (file-position stream)))
                         (unless (file-position stream (+ offset size)) (error "cannot seek Bind spool"))
                         (decf remaining size)
                         (cons offset size)))))))
      (let* ((portal (cstring)) (statement (cstring))
                                (formats (loop repeat (uint 2) collect (uint 2)))
                                (values (loop repeat (uint 2) collect (field)))
                                (results (loop repeat (uint 2) collect (uint 2))))
        (unless (zerop remaining) (malformed-protocol-message "extended query message has trailing bytes"))
        (list :bind portal statement formats values results)))))

(defun extended-bind-spool-field (spool range)
  "Materialize only the requested parameter from SPOOL."
  (let* ((stream (extended-bind-spool-stream spool))
         (size (cdr range))
         (bytes (make-array size :element-type '(unsigned-byte 8))))
    (unless (file-position stream (car range)) (error "cannot seek Bind spool field"))
    (loop for position = 0 then next
          while (< position size)
          for next = (read-sequence bytes stream :start position)
          do (when (= next position) (error "truncated Bind spool field")))
    bytes))

(defstruct extended-query-state
  (statements (make-hash-table :test #'equal))
  (portals (make-hash-table :test #'equal))
  (ignore-until-sync nil)
  (in-cycle-p nil)
  (statement-deadline nil)
  (executed-in-cycle-p nil)
  (implicit-p nil))

(defvar *extended-query-state* nil)

(defvar *extended-query-state-registry* (make-hash-table :test #'eq :weakness :key))

(defvar *extended-query-state-registry-lock* (sb-thread:make-mutex :name "extended query state"))

(defun extended-query-state-for-backend (backend)
  (unless backend (error "extended query requires a backend"))
  (sb-thread:with-mutex (*extended-query-state-registry-lock*)
    (or (gethash backend *extended-query-state-registry*)
        (setf (gethash backend *extended-query-state-registry*) (make-extended-query-state)))))

(defun extended-release-portal (portal)
  "Release the SELECT portal's registered snapshot before discarding it."
  (when portal
    (let ((info (extended-select-portal-info portal)))
      (when info
        (sb-sys:without-interrupts
          (let ((registration (extended-select-portal-metadata-registration info)))
            (when registration
              (release-snapshot-registration registration)
              (setf (extended-select-portal-metadata-registration info) nil
                    (extended-select-portal-metadata-snapshot info) nil)))))))
  (values))

(defun extended-remove-portal (state name)
  (sb-sys:without-interrupts
    (extended-release-portal (gethash name (extended-query-state-portals state)))
    (remhash name (extended-query-state-portals state)))
  (values))

(defun extended-clear-portals (state)
  (sb-sys:without-interrupts
    (maphash (lambda (name portal)
               (declare (ignore name))
               (extended-release-portal portal))
             (extended-query-state-portals state))
    (clrhash (extended-query-state-portals state)))
  (values))

(defun extended-install-portal (state name portal)
  "Publish a SELECT portal with its Bind-time snapshot and command ID.
The registration protects its horizon from vacuum until portal cleanup.  Release
it if installation fails; replacing a portal releases the old registration."
  (let ((info (extended-select-portal-info portal)))
    (if info
        (with-statement-snapshot (snapshot #'statement-start)
          (let ((registration (make-snapshot-registration))
                (retained (copy-snapshot snapshot))
                (installed nil))
            (unwind-protect
                 (sb-sys:without-interrupts
                   (sb-thread:with-mutex (*xid-lock*)
                     (register-active-snapshot retained registration))
                   (extended-remove-portal state name)
                   (setf (extended-select-portal-metadata-snapshot info) retained
                         (extended-select-portal-metadata-registration info) registration
                         (gethash name (extended-query-state-portals state)) portal
                         installed t))
              (unless installed (release-snapshot-registration registration)))))
        (progn
          (extended-remove-portal state name)
          (setf (gethash name (extended-query-state-portals state)) portal))))
  portal)

(defun extended-backend-holds-snapshot-p (backend)
  "Tell wraparound whether BACKEND must run to retire its own portal pins."
  (sb-thread:with-mutex (*extended-query-state-registry-lock*)
    (let ((state (gethash backend *extended-query-state-registry*)))
      (when state
        (loop for portal being the hash-values of (extended-query-state-portals state)
              for info = (extended-select-portal-info portal)
                thereis (and info (extended-select-portal-metadata-registration info)))))))

(defun extended-clear-portals-for-backend (backend)
  (when backend
    (sb-thread:with-mutex (*extended-query-state-registry-lock*)
      (let ((state (gethash backend *extended-query-state-registry*)))
        (when state
          (extended-clear-portals state)
          (setf (extended-query-state-executed-in-cycle-p state) nil)))))
  (values))

(defun forget-extended-query-state (backend)
  (when backend
    (sb-thread:with-mutex (*extended-query-state-registry-lock*)
      (let ((state (gethash backend *extended-query-state-registry*)))
        (when state (extended-clear-portals state)))
      (remhash backend *extended-query-state-registry*)))
  (values))

(defun extended-read-cstring (payload position)
  (multiple-value-bind (value end) (pg-client-cstring payload position)
    (values value end)))

(defun extended-type-from-oid (oid)
  (let ((entry (find oid +sql-expression-types+ :key #'second)))
    (unless entry (ereport "0A000" "unsupported parameter type OID ~a" oid))
    (first entry)))

(defun extended-parse-frame (type payload)
  "Parse the complete frame before changing prepared or portal state."
  (let ((position 0) (length (length payload)))
    (labels ((need (n) (unless (<= (+ position n) length) (malformed-protocol-message "truncated extended query message")))
             (u16 () (need 2) (prog1 (bytes-to-u16 payload position) (incf position 2)))
             (u32 () (need 4) (prog1 (bytes-to-u32 payload position) (incf position 4)))
             (cstring () (multiple-value-bind (value next) (extended-read-cstring payload position)
                           (setf position next) value))
             (done () (unless (= position length) (malformed-protocol-message "extended query message has trailing bytes"))))
      (case type
        (#\P (let* ((name (cstring)) (query (cstring)) (count (u16))
                                     (oids (loop repeat count collect (u32))))
               (done) (list :parse name query oids)))
        (#\B (let* ((portal (cstring)) (statement (cstring))
                                       (formats (loop repeat (u16) collect (u16)))
                                       (values (loop repeat (u16)
                                                     collect (let ((size (u32)))
                                                               (if (= size #xffffffff) nil
                                                                   (progn (need size)
                                                                          (prog1 (cons position size) (incf position size)))))))
                                       (results (loop repeat (u16) collect (u16))))
               (done) (list :bind portal statement formats values results)))
        (#\E (let ((portal (cstring)) (limit (u32)))
               (done) (list :execute portal limit)))
        ((#\C #\D) (need 1) (let ((kind (code-char (aref payload position))))
                              (incf position)
                              (let ((name (cstring)))
                                (done) (list (if (char= type #\C) :close :describe) kind name))))
        (#\S (done) '(:sync))
        (#\H (done) '(:flush))
        (otherwise (ereport "0A000" "unsupported extended query message"))))))

(defun extended-parameter-format (formats index count)
  (case (length formats)
    (0 0)
    (1 (aref formats 0))
    (otherwise (unless (= (length formats) count)
                 (ereport "08P01" "invalid parameter format count"))
     (aref formats index))))

(defun extended-context-sensitive-literal-p (tree)
  "Identify string literals whose parse-time coercion cannot yet be retained."
  (let ((pending (list tree)))
    (loop while pending
          for item = (pop pending)
          when (consp item)
            do (when (and (eq (first item) :const) (consp (rest item))
                          (stringp (second item)))
                 (return-from extended-context-sensitive-literal-p t))
               (push (cdr item) pending)
               (push (car item) pending))
    nil))

(defun extended-select-result-columns (ast &optional register-p)
  "Plan result columns and optionally retain relation readers for the transaction."
  (let* ((prepared (query-plan-prepared (plan-select-query ast)))
         (relations (getf prepared :relations))
         (columns (mapcar #'target->result-column (getf prepared :targets))))
    (when (and register-p relations)
      (with-registered-select-relations (registered relations)
        (declare (ignore registered))))
    columns))

(defvar *extended-statement-signatures* (make-hash-table :test #'eq :weakness :key))

(defvar *extended-statement-signatures-lock* (sb-thread:make-mutex :name "extended statement signatures"))

(defun extended-result-signature (columns)
  "Match C equalRowTypes for supported scalar result attributes."
  (mapcar (lambda (column) (list (result-column-name column)
                                 (result-column-type-oid column)
                                 (result-column-typmod column)))
          columns))

(defun extended-check-result-signature (statement columns)
  (multiple-value-bind (signature present)
      (sb-thread:with-mutex (*extended-statement-signatures-lock*)
        (gethash statement *extended-statement-signatures*))
    ;; Statements prepared by an older live-reload image have no stored signature.
    (when (and present (not (equal signature (extended-result-signature columns))))
      (ereport "0A000" "cached plan must not change result type")))
  columns)

(defun extended-binary-type-p (type)
  (member type '(:text :varchar :bytea :json :jsonb :numeric)))

(defun extended-binary-numeric-output (value)
  "Produce numeric_send's network-order base-10000 representation."
  (let ((special (case value (:numeric-nan #xc000)
                       (:numeric-positive-infinity #xd000)
                       (:numeric-negative-infinity #xf000))))
    (if special
        (concatenate '(vector (unsigned-byte 8))
                     (u16-to-bytes 0) (u16-to-bytes 0)
                     (u16-to-bytes special) (u16-to-bytes (if (= special #xc000) 0 32)))
        (let* ((packed (sql-numeric-to-packed-octets value))
               (header (page-u16 packed 4))
               (short (= (logand header #xc000) #x8000))
               (offset (if short 6 8))
               (weight (if short
                           (let ((w (logand header #x7f))) (if (logbitp 6 w) (- w 128) w))
                           (let ((w (page-u16 packed 6))) (if (logbitp 15 w) (- w 65536) w))))
               (sign (if (if short (logbitp 13 header) (logbitp 14 header)) #x4000 0))
               (count (/ (- (length packed) offset) 2))
               (result (make-array (+ 8 (* 2 count)) :element-type '(unsigned-byte 8))))
          (replace result (u16-to-bytes count))
          (replace result (u16-to-bytes (logand weight #xffff)) :start1 2)
          (replace result (u16-to-bytes sign) :start1 4)
          (replace result (u16-to-bytes (sql-numeric-scale value)) :start1 6)
          (dotimes (i count)
            (replace result (u16-to-bytes (page-u16 packed (+ offset (* 2 i))))
                     :start1 (+ 8 (* 2 i))))
          result))))

(defun extended-binary-numeric-input (bytes parameter-number)
  "Read numeric_recv's base-10000 representation, truncating to display scale."
  (labels ((u16 (offset)
             (when (< (length bytes) (+ offset 2))
               (ereport "08P01" "insufficient data left in message"))
             (+ (ash (aref bytes offset) 8) (aref bytes (1+ offset)))))
    (let* ((count (u16 0)) (encoded-weight (u16 2))
                           (weight (if (logbitp 15 encoded-weight) (- encoded-weight 65536) encoded-weight))
                           (sign (u16 4))
                           (scale (progn
                                    (unless (member sign '(#x0000 #x4000 #xc000 #xd000 #xf000))
                                      (ereport "22P03" "invalid sign in external \"numeric\" value"))
                                    (u16 6))))
      (when (> scale #x3fff) (ereport "22P03" "invalid scale in external \"numeric\" value"))
      (let ((acc 0))
        (dotimes (i count)
          (let ((digit (u16 (+ 8 (* 2 i)))))
            (when (>= digit 10000) (ereport "22P03" "invalid digit in external \"numeric\" value"))
            (setf acc (+ (* acc 10000) digit))))
        (unless (= (length bytes) (+ 8 (* 2 count)))
          (ereport "22P03" "incorrect binary data format in bind parameter ~d" parameter-number))
        (case sign
          (#xc000 :numeric-nan) (#xd000 :numeric-positive-infinity) (#xf000 :numeric-negative-infinity)
          (otherwise
           (let* ((exponent (+ scale (* 4 (- (1+ weight) count))))
                  (coefficient (if (minusp exponent)
                                   (floor acc (expt 10 (- exponent)))
                                   (* acc (expt 10 exponent)))))
             (when (and (not (zerop coefficient))
                        (> (- (length (write-to-string coefficient :base 10)) scale) 131072))
               (ereport "22003" "value overflows numeric format"))
             (make-sql-decimal (if (= sign #x4000) (- coefficient) coefficient) scale))))))))

(defun extended-binary-input (bytes type parameter-number)
  (case type
    (:bytea bytes)
    (:numeric (extended-binary-numeric-input bytes parameter-number))
    (otherwise
     (let* ((start (if (eq type :jsonb)
                       (progn
                         (when (zerop (length bytes)) (ereport "08P01" "insufficient data left in message"))
                         (unless (= (aref bytes 0) 1)
                           (ereport "XX000" "unsupported jsonb version number ~d" (aref bytes 0)))
                         1)
                       0))
            (text (handler-case (sb-ext:octets-to-string bytes :start start :external-format :utf-8)
                    (sb-int:character-decoding-error () (ereport "22021" "invalid UTF-8 in binary parameter")))))
       (when (position (code-char 0) text) (ereport "22021" "null byte in binary parameter"))
       (sql-cast-value text :text type -1)))))

(defun extended-select-result-formats (formats columns)
  (let ((count (length columns)))
    (unless (member (length formats) (list 0 1 count))
      (ereport "08P01" "invalid result format count"))
    (let ((result (make-array count)) (format-vector (coerce formats 'vector)))
      (loop for column in columns for index from 0
            for format = (case (length formats) (0 0) (1 (first formats)) (otherwise (aref format-vector index)))
            do (unless (or (= format 0) (and (= format 1) (extended-binary-type-p (extended-type-from-oid (result-column-type-oid column)))))
                 (ereport "0A000" "unsupported binary result format"))
               (setf (aref result index) format
                     (result-column-format column) format))
      result)))

(defun extended-apply-operation (stream operation payload)
  (let ((state *extended-query-state*))
    (case (first operation)
      (:parse
       (destructuring-bind (kind name sql oids) operation
         (declare (ignore kind))
         (when (eq (backend-state *backend*) :idle)
           (sb-sys:without-interrupts
             (begin-extended-implicit-query-block)
             (setf (extended-query-state-implicit-p state) t)))
         (when (zerop (length name))
           (remhash "" (extended-query-state-statements state)))
         (when (and (plusp (length name)) (gethash name (extended-query-state-statements state)))
           (ereport "42P05" "prepared statement already exists"))
         (let* ((statements (parse-statements sql))
                (types (map 'vector #'extended-type-from-oid oids)))
           (unless (and (= (length statements) 1)
                        (or (and (member (getf (first statements) :type) '(:insert :update :delete))
                                 (null (getf (first statements) :returning)))
                            (eq (getf (first statements) :type) :select)))
             (ereport "0A000" "extended query supports DML or SELECT"))
           (when (extended-context-sensitive-literal-p (first statements))
             (ereport "0A000" "string literals in extended statements are not supported; use parameters"))
           (when (eq (backend-state *backend*) :aborted)
             (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
           ;; Parse establishes the repeatable-read horizon; planned relation
           ;; readers remain registered until transaction end.
           (with-statement-snapshot (snapshot #'statement-start)
             (let* ((*query-parameter-types* types)
                    (signature (if (eq (getf (first statements) :type) :select)
                                   (extended-result-signature (extended-select-result-columns (first statements) t))
                                   (progn (prepare-dml-subqueries (first statements)) nil))))
               (let ((statement (make-extended-statement :ast (first statements) :types types)))
                 (sb-thread:with-mutex (*extended-statement-signatures-lock*)
                   (setf (gethash statement *extended-statement-signatures*) signature))
                 (setf (gethash name (extended-query-state-statements state)) statement))))
           (write-message stream #\1 #()))))
      (:bind
       (destructuring-bind (kind portal name formats ranges results) operation
         (declare (ignore kind))
         (let ((statement (gethash name (extended-query-state-statements state))))
           (unless statement (ereport "26000" "prepared statement does not exist"))
           (when (eq (backend-state *backend*) :aborted)
             (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
           (when (eq (backend-state *backend*) :idle)
             (sb-sys:without-interrupts
               (begin-extended-implicit-query-block)
               (setf (extended-query-state-implicit-p state) t)))
           (when (and (plusp (length portal)) (gethash portal (extended-query-state-portals state)))
             (ereport "42P03" "portal already exists"))
           (unless (= (length ranges) (length (extended-statement-types statement)))
             (ereport "08P01" "bind parameter count mismatch"))
           (unless (and (member (length formats) (list 0 1 (length ranges)))
                        (every (lambda (format) (member format '(0 1))) formats))
             (ereport "08P01" "invalid parameter formats"))
           (let* ((select-p (eq (getf (extended-statement-ast statement) :type) :select))
                  (columns (when select-p
                             (let ((*query-parameter-types* (extended-statement-types statement)))
                               (extended-check-result-signature
                                statement (extended-select-result-columns (extended-statement-ast statement) t)))))
                  (result-formats (when select-p (extended-select-result-formats results columns)))
                  (types (extended-statement-types statement))
                  (format-vector (coerce formats 'vector))
                  (values (make-array (length ranges))))
             (unless (or select-p (null results) (and (= (length results) 1) (= (first results) 0)))
               (ereport "0A000" "binary result format is not supported"))
             (loop for range in ranges for index from 0
                   for type = (aref types index)
                   for format = (extended-parameter-format format-vector index (length values))
                   do (when (and (= format 1) (not (extended-binary-type-p type)))
                        (ereport "0A000" "unsupported binary parameter format"))
                      (setf (aref values index)
                            (if (null range) +null+
                                (let* ((spooled (extended-bind-spool-p payload))
                                       (bytes (if spooled (extended-bind-spool-field payload range) payload))
                                       (start (if spooled 0 (car range)))
                                       (end (+ start (cdr range))))
                                  (if (= format 0)
                                      (progn
                                        (when (position 0 bytes :start start :end end)
                                          (ereport "22021" "null byte in text parameter"))
                                        (sql-cast-value (handler-case (sb-ext:octets-to-string bytes :start start :end end :external-format :utf-8)
                                                          (sb-int:character-decoding-error ()
                                                            (ereport "22021" "invalid UTF-8 in text parameter")))
                                                        :text type -1))
                                      (extended-binary-input (if spooled bytes (subseq bytes start end)) type (1+ index)))))))
             (when (extended-bind-spool-p payload) (extended-verify-bind-spool payload))
             (extended-install-portal
              state portal
              (make-extended-query-portal statement values select-p columns result-formats))
             (write-message stream #\2 #())))))
      (:execute
       (destructuring-bind (kind name limit) operation
         (declare (ignore kind))
         (unless (zerop limit) (ereport "0A000" "limited portal execution is not supported"))
         (let ((portal (gethash name (extended-query-state-portals state))))
           (unless portal (ereport "34000" "portal does not exist"))
           (when (extended-portal-executed-p portal) (ereport "55000" "portal already executed"))
           (when (eq (backend-state *backend*) :idle)
             (begin-extended-implicit-query-block)
             (setf (extended-query-state-implicit-p state) t))
           (let ((*query-parameter-types* (extended-statement-types (extended-portal-statement portal)))
                 (*query-parameter-values* (extended-portal-values portal))
                 (*select-result-formats* (extended-portal-result-formats portal))
                 (*select-expected-result-columns* (extended-portal-result-columns portal))
                 (*select-suppress-description* (extended-portal-select-p portal))
                 (*statement-snapshot-override*
                   (let ((info (extended-select-portal-info portal)))
                     (and info (extended-select-portal-metadata-snapshot info)))))
             (execute-statement stream (extended-statement-ast (extended-portal-statement portal))))
           (setf (extended-portal-executed-p portal) t
                 (extended-query-state-executed-in-cycle-p state) t))))
      (:describe
       (destructuring-bind (kind target name) operation
         (declare (ignore kind))
         (multiple-value-bind (columns select-p)
             (case target
               (#\S (let ((statement (gethash name (extended-query-state-statements state))))
                      (unless statement (ereport "26000" "prepared statement does not exist"))
                      (when (and (eq (backend-state *backend*) :aborted)
                                 (eq (getf (extended-statement-ast statement) :type) :select))
                        (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
                      (let ((response (make-payload)))
                        (payload-push-u16 response (length (extended-statement-types statement)))
                        (loop for type across (extended-statement-types statement)
                              do (payload-push-u32 response (type-oid type)))
                        (write-message stream #\t response))
                      (when (eq (getf (extended-statement-ast statement) :type) :select)
                        (let ((*query-parameter-types* (extended-statement-types statement)))
                          (values (extended-check-result-signature
                                   statement (extended-select-result-columns (extended-statement-ast statement))) t)))))
               (#\P (let ((portal (gethash name (extended-query-state-portals state))))
                      (unless portal (ereport "34000" "portal does not exist"))
                      (when (and (eq (backend-state *backend*) :aborted) (extended-portal-select-p portal))
                        (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
                      (values (extended-portal-result-columns portal) (extended-portal-select-p portal))))
               (otherwise (ereport "08P01" "invalid describe target")))
           (if select-p (send-row-description stream columns) (write-message stream #\n #())))))
      (:close
       (destructuring-bind (kind target name) operation
         (declare (ignore kind))
         (case target
           (#\S (remhash name (extended-query-state-statements state)))
           (#\P (extended-remove-portal state name))
           (otherwise (ereport "08P01" "invalid close target")))
         (write-message stream #\3 #())))
      (:sync
       (when (extended-query-state-implicit-p state)
         (when (eq (backend-state *backend*) :in-progress) (commit-transaction))
         (setf (extended-query-state-implicit-p state) nil)
         (extended-clear-portals state))
       (when (eq (backend-state *backend*) :idle)
         (extended-clear-portals state))
       (setf (extended-query-state-ignore-until-sync state) nil
             (extended-query-state-in-cycle-p state) nil
             (extended-query-state-executed-in-cycle-p state) nil)
       (send-ready-for-query stream (transaction-status-char)))
      (:flush (force-output stream)))))
