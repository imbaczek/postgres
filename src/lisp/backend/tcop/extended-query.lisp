;;;; Extended query protocol state and bounded frame parsing.

(in-package #:pg)

(defstruct extended-statement ast types)

(defstruct extended-portal statement values (executed-p nil))

(defstruct extended-select-portal-metadata columns formats)

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

(defun extended-clear-portals-for-backend (backend)
  (when backend
    (sb-thread:with-mutex (*extended-query-state-registry-lock*)
      (let ((state (gethash backend *extended-query-state-registry*)))
        (when state
          (clrhash (extended-query-state-portals state))
          (setf (extended-query-state-executed-in-cycle-p state) nil)))))
  (values))

(defun forget-extended-query-state (backend)
  (when backend
    (sb-thread:with-mutex (*extended-query-state-registry-lock*)
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

(defun extended-copy-select-p (ast)
  "Accept only the plain table scan needed by initial copy."
  (and (eq (getf ast :type) :select)
       (equal (getf ast :targets) '((:star t)))
       (stringp (getf ast :from))
       (null (getf ast :from-alias))
       (every (lambda (key) (null (getf ast key)))
              '(:joins :where :group-by :having :distinct :windows :order-by :limit :offset :with :with-recursive :set-operation))))

(defun extended-select-result-columns (ast)
  (mapcar #'target->result-column (getf (query-plan-prepared (plan-select-query ast)) :targets)))

(defun extended-select-result-formats (formats columns)
  (let ((count (length columns)))
    (unless (member (length formats) (list 0 1 count))
      (ereport "08P01" "invalid result format count"))
    (let ((result (make-array count)) (format-vector (coerce formats 'vector)))
      (loop for column in columns for index from 0
            for format = (case (length formats) (0 0) (1 (first formats)) (otherwise (aref format-vector index)))
            do (unless (or (= format 0) (and (= format 1) (= (result-column-type-oid column) 17)))
                 (ereport "0A000" "binary result format is supported only for bytea"))
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
                            (and (zerop (length types)) (extended-copy-select-p (first statements)))))
             (ereport "0A000" "extended query supports DML or a plain table SELECT"))
           (when (extended-context-sensitive-literal-p (first statements))
             (ereport "0A000" "string literals in extended statements are not supported; use parameters"))
           (when (eq (backend-state *backend*) :aborted)
             (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
           (let ((*query-parameter-types* types))
             (if (extended-copy-select-p (first statements))
                 (extended-select-result-columns (first statements))
                 (prepare-dml-subqueries (first statements))))
           (setf (gethash name (extended-query-state-statements state))
                 (make-extended-statement :ast (first statements) :types types))
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
           (let* ((select-p (extended-copy-select-p (extended-statement-ast statement)))
                  (columns (when select-p (extended-select-result-columns (extended-statement-ast statement))))
                  (result-formats (when select-p (extended-select-result-formats results columns)))
                  (types (extended-statement-types statement))
                  (format-vector (coerce formats 'vector))
                  (values (make-array (length ranges))))
             (unless (or select-p (null results) (and (= (length results) 1) (= (first results) 0)))
               (ereport "0A000" "binary result format is not supported"))
             (loop for range in ranges for index from 0
                   for type = (aref types index)
                   for format = (extended-parameter-format format-vector index (length values))
                   do (when (and (= format 1) (not (eq type :bytea)))
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
                                        (sql-cast-value (sb-ext:octets-to-string bytes :start start :end end
                                                                                       :external-format :utf-8)
                                                        :text type -1))
                                      (if spooled bytes (subseq bytes start end)))))))
             (when (extended-bind-spool-p payload) (extended-verify-bind-spool payload))
             (setf (gethash portal (extended-query-state-portals state))
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
                 (*select-suppress-description* (extended-portal-select-p portal)))
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
                                 (extended-copy-select-p (extended-statement-ast statement)))
                        (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
                      (let ((response (make-payload)))
                        (payload-push-u16 response (length (extended-statement-types statement)))
                        (loop for type across (extended-statement-types statement)
                              do (payload-push-u32 response (type-oid type)))
                        (write-message stream #\t response))
                      (when (extended-copy-select-p (extended-statement-ast statement))
                        (values (extended-select-result-columns (extended-statement-ast statement)) t))))
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
           (#\P (remhash name (extended-query-state-portals state)))
           (otherwise (ereport "08P01" "invalid close target")))
         (write-message stream #\3 #())))
      (:sync
       (when (extended-query-state-implicit-p state)
         (when (eq (backend-state *backend*) :in-progress) (commit-transaction))
         (setf (extended-query-state-implicit-p state) nil)
         (clrhash (extended-query-state-portals state)))
       (when (eq (backend-state *backend*) :idle)
         (clrhash (extended-query-state-portals state)))
       (setf (extended-query-state-ignore-until-sync state) nil
             (extended-query-state-in-cycle-p state) nil
             (extended-query-state-executed-in-cycle-p state) nil)
       (send-ready-for-query stream (transaction-status-char)))
      (:flush (force-output stream)))))
