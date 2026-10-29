;;;; client.lisp -- small PostgreSQL v3 frontend for the replication worker.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-bsd-sockets))

(define-condition pg-client-error (error)
  ((sqlstate :initarg :sqlstate :initform "XX000"
             :reader pg-client-error-sqlstate)
   (message :initarg :message :reader pg-client-error-message))
  (:report (lambda (condition stream)
             (format stream "PostgreSQL ~a: ~a"
                     (pg-client-error-sqlstate condition)
                     (pg-client-error-message condition)))))

(defstruct pg-client socket stream)
(defstruct pg-client-column name type-oid (typmod -1))

(defun pg-client-cstring (bytes offset)
  (let ((end (position 0 bytes :start offset)))
    (unless end
      (error "unterminated PostgreSQL frontend string"))
    (values (bytes->string bytes :start offset :end end) (1+ end))))

(defun pg-client-u64 (bytes offset)
  (unless (<= (+ offset 8) (length bytes))
    (error "truncated PostgreSQL 64-bit field"))
  (loop with value = 0
        for i from offset below (+ offset 8)
        do (setf value (logior (ash value 8) (aref bytes i)))
        finally (return value)))

(defun pg-client-i64 (bytes offset)
  (let ((value (pg-client-u64 bytes offset)))
    (if (logbitp 63 value) (- value #x10000000000000000) value)))

(defstruct logical-wire-cursor stream remaining)

(defun logical-wire-byte (cursor)
  (when (zerop (logical-wire-cursor-remaining cursor))
    (error "truncated logical replication frame"))
  (let ((byte (read-byte (logical-wire-cursor-stream cursor) nil nil)))
    (unless byte (error "logical replication connection ended during a frame"))
    (decf (logical-wire-cursor-remaining cursor))
    byte))

(defun logical-wire-uint (cursor size)
  (loop repeat size for value = (logical-wire-byte cursor)
        for result = value then (logior (ash result 8) value)
        finally (return result)))

(defun logical-wire-octets (cursor size)
  (unless (<= size (logical-wire-cursor-remaining cursor))
    (error "truncated logical replication value"))
  (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
    (loop for position = 0 then next
          while (< position size)
          for next = (read-sequence bytes (logical-wire-cursor-stream cursor) :start position)
          do (when (= next position) (error "logical replication connection ended during a value"))
             (decf (logical-wire-cursor-remaining cursor) (- next position)))
    bytes))

(defun logical-wire-write-text (cursor size output &optional octet-consumer)
  "Validate SIZE UTF-8 bytes, writing decoded characters to OUTPUT when supplied.
OCTET-CONSUMER, when supplied, receives each bounded input block."
  (unless (<= size (logical-wire-cursor-remaining cursor))
    (error "truncated logical replication value"))
  (let ((remaining size) (needed 0) (code 0) (minimum 0))
    (loop while (plusp remaining) do
      (let* ((count (min remaining 65536))
             (bytes (logical-wire-octets cursor count)))
        (decf remaining count)
        (loop for byte across bytes do
          (if (zerop needed)
              (cond
                ((< byte #x80) (when output (write-char (code-char byte) output)))
                ((<= #xc2 byte #xdf) (setf needed 1 code (logand byte #x1f) minimum #x80))
                ((<= #xe0 byte #xef) (setf needed 2 code (logand byte #x0f) minimum #x800))
                ((<= #xf0 byte #xf4) (setf needed 3 code (logand byte #x07) minimum #x10000))
                (t (error "invalid UTF-8 in logical replication value")))
              (progn
                (unless (<= #x80 byte #xbf)
                  (error "invalid UTF-8 in logical replication value"))
                (setf code (logior (ash code 6) (logand byte #x3f)))
                (decf needed)
                (when (zerop needed)
                  (unless (and (<= minimum code #x10ffff) (not (<= #xd800 code #xdfff)))
                    (error "invalid UTF-8 in logical replication value"))
                  (when output (write-char (code-char code) output))))))
        (when octet-consumer (funcall octet-consumer bytes))))
    (unless (zerop needed) (error "truncated UTF-8 in logical replication value"))))

(defun logical-wire-text (cursor size)
  "Decode a bounded UTF-8 field without retaining its complete octet vector."
  (with-output-to-string (output)
    (logical-wire-write-text cursor size output)))

(defparameter *logical-wire-resident-field-budget* (* 4 1024 1024)
  "Conservative budget for retained field strings per receiver scope.")

(defstruct (logical-wire-field-budget (:constructor make-logical-wire-field-budget
                                          (&optional (limit *logical-wire-resident-field-budget*))))
  limit (used 0))

(defstruct logical-wire-field-owner stream (size 0) (active-p t)
           (budget (make-logical-wire-field-budget)) (resident-bytes 0))

(defstruct logical-wire-field owner offset size crc)

(defparameter *logical-wire-field-spill-threshold* (* 1024 1024))

(defun close-logical-wire-field-owner (owner)
  "Expire OWNER and close its anonymous file once, including on nonlocal exits."
  (when owner
    (sb-sys:without-interrupts
      (when (logical-wire-field-owner-active-p owner)
        (setf (logical-wire-field-owner-active-p owner) nil)
        (decf (logical-wire-field-budget-used (logical-wire-field-owner-budget owner))
              (logical-wire-field-owner-resident-bytes owner))
        (setf (logical-wire-field-owner-resident-bytes owner) 0)
        (when (logical-wire-field-owner-stream owner)
          (close (logical-wire-field-owner-stream owner) :abort t)))))
  (values))

(defun logical-wire-field-open (owner)
  "Open an anonymous, process-owned scratch file for OWNER."
  (unless (logical-wire-field-owner-active-p owner)
    (error "logical field owner is closed"))
  (or (logical-wire-field-owner-stream owner)
      (sb-sys:without-interrupts
        (loop repeat 100
              for path = (merge-pathnames
                          (format nil "pg-logical-field-~d-~d"
                                  (sb-posix:getpid) (random most-positive-fixnum))
                          (uiop:temporary-directory))
              do (let ((fd nil) (stream nil) (published nil))
                   (unwind-protect
                        (progn
                          (setf fd (handler-case
                                       (sb-posix:open (namestring path)
                                                      (logior sb-posix:o-rdwr sb-posix:o-creat
                                                              sb-posix:o-excl sb-posix:o-nofollow
                                                              +postmaster-o-cloexec+) #o600)
                                     (sb-posix:syscall-error () nil)))
                          (when fd
                            (sb-posix:unlink (namestring path))
                            (setf stream (sb-sys:make-fd-stream
                                          fd :input t :output t
                                          :element-type '(unsigned-byte 8)))
                            (setf (logical-wire-field-owner-stream owner) stream
                                  published t)
                            (return stream)))
                     (unless published
                       (when fd
                         (if stream (close stream :abort t) (sb-posix:close fd))
                         (ignore-errors (sb-posix:unlink (namestring path)))))))
              finally (error "could not create logical receiver scratch file")))))

(defun logical-wire-field-capture (owner cursor size)
  "Validate and spool one field in OWNER, publishing only a complete slice."
  (unless (and (logical-wire-field-owner-active-p owner)
               (<= size (logical-wire-cursor-remaining cursor))
               (<= (+ size (logical-wire-field-owner-size owner)) +max-query-message-size+))
    (error "invalid logical receiver field length"))
  (let* ((stream (logical-wire-field-open owner))
         (offset (logical-wire-field-owner-size owner)))
    (unless (file-position stream offset) (error "cannot seek logical receiver scratch file"))
    (multiple-value-bind (sink metrics) (logical-encoding-sink size stream)
      (logical-wire-write-text cursor size nil sink)
      (multiple-value-bind (written crc) (funcall metrics)
        (unless (= written size) (error "invalid logical receiver field size"))
        (setf (logical-wire-field-owner-size owner) (+ offset size))
        (make-logical-wire-field :owner owner :offset offset :size size :crc crc)))))

(defun logical-wire-reserve-field (owner size)
  "Reserve a conservative resident charge before decoding one field."
  (sb-sys:without-interrupts
    (unless (logical-wire-field-owner-active-p owner)
      (error "logical field owner is closed"))
    (let* ((budget (logical-wire-field-owner-budget owner))
           (charge (+ 64 (* 4 size))))
      (when (<= (+ (logical-wire-field-budget-used budget) charge)
                (logical-wire-field-budget-limit budget))
        (incf (logical-wire-field-budget-used budget) charge)
        (incf (logical-wire-field-owner-resident-bytes owner) charge)
        t))))

(defun logical-wire-received-field (cursor size owner type-oid)
  "Return a string or a scoped slice for one text-format field."
  (declare (ignore type-oid))
  (cond
    ((null owner) (logical-wire-text cursor size))
    ((or (> size *logical-wire-field-spill-threshold*)
         (not (logical-wire-reserve-field owner size)))
     (logical-wire-field-capture owner cursor size))
    (t (logical-wire-text cursor size))))

(defun logical-wire-field-visit (field output)
  "Verify FIELD and optionally write its decoded characters to OUTPUT."
  (let* ((owner (logical-wire-field-owner field))
         (stream (and (logical-wire-field-owner-active-p owner)
                      (logical-wire-field-owner-stream owner)))
         (size (logical-wire-field-size field)))
    (unless (and stream (<= (+ (logical-wire-field-offset field) size)
                            (logical-wire-field-owner-size owner)))
      (error "logical receiver field has expired"))
    (finish-output stream)
    (unless (file-position stream (logical-wire-field-offset field))
      (error "cannot seek logical receiver field"))
    (multiple-value-bind (sink metrics) (logical-encoding-sink size)
      (logical-wire-write-text (make-logical-wire-cursor :stream stream :remaining size)
                               size output sink)
      (multiple-value-bind (read-size crc) (funcall metrics)
        (unless (and (= read-size size) (= crc (logical-wire-field-crc field)))
          (error "logical receiver field checksum mismatch")))))
  (values))

(defun pg-client-frame (stream)
  (multiple-value-bind (type length-bytes payload) (read-message-frame stream)
    (unless (and type length-bytes payload)
      (error "PostgreSQL connection ended during a message"))
    (values (code-char type) payload)))

(defun pg-client-error-from-payload (payload)
  (let ((position 0)
        (sqlstate "XX000")
        (message "server returned an error"))
    (loop while (< position (length payload))
          for code = (aref payload position)
          do (incf position)
             (when (zerop code) (return))
             (multiple-value-bind (value next)
                 (pg-client-cstring payload position)
               (setf position next)
               (cond
                 ((= code (char-code #\C)) (setf sqlstate value))
                 ((= code (char-code #\M)) (setf message value)))))
    (make-condition 'pg-client-error :sqlstate sqlstate :message message)))

(defun pg-client-connect (host port database &key (user "postgres")
                                               application-name replication-p options)
  (let* ((socket (make-instance 'sb-bsd-sockets:inet-socket
                                :type :stream :protocol :tcp))
         (stream nil)
         (client nil))
    (unwind-protect
         (progn
           (sb-bsd-sockets:socket-connect
            socket (sb-bsd-sockets:host-ent-address
                    (sb-bsd-sockets:get-host-by-name host)) port)
           (setf stream
                 (sb-bsd-sockets:socket-make-stream
                  socket :input t :output t
                  :element-type '(unsigned-byte 8) :buffering :none)
                 client (make-pg-client :socket socket :stream stream))
           (pg-client-send-startup stream user database application-name replication-p options)
           (pg-client-read-startup stream)
           (setf socket nil stream nil)
           client)
      (when stream (ignore-errors (close stream :abort t)))
      (when socket (ignore-errors (sb-bsd-sockets:socket-close socket))))))

(defun pg-client-send-startup (stream user database application-name replication-p &optional options)
  (let ((body (make-payload)))
    (payload-push-u32 body +protocol-version-3-0+)
    (payload-push-cstring body "user")
    (payload-push-cstring body user)
    (payload-push-cstring body "database")
    (payload-push-cstring body database)
    (when application-name
      (payload-push-cstring body "application_name")
      (payload-push-cstring body application-name))
    (when options
      (payload-push-cstring body "options")
      (payload-push-cstring body options))
    (when replication-p
      (payload-push-cstring body "replication")
      (payload-push-cstring body "database"))
    (vector-push-extend 0 body)
    (write-sequence (u32-to-bytes (+ 4 (length body))) stream)
    (write-sequence body stream)
    (force-output stream)))

(defun pg-client-read-startup (stream)
  (loop
    (multiple-value-bind (type payload) (pg-client-frame stream)
      (case type
        (#\R
         (unless (and (= (length payload) 4) (zerop (bytes-to-u32 payload 0)))
           (error "unsupported PostgreSQL authentication request")))
        (#\E (error (pg-client-error-from-payload payload)))
        (#\Z (return))
        ((#\S #\K #\N) nil)
        (otherwise (error "unexpected startup response ~s" type))))))

(defun pg-client-close (client)
  (when client
    (ignore-errors (close (pg-client-stream client) :abort t))
    (ignore-errors (sb-bsd-sockets:socket-close (pg-client-socket client))))
  (values))

(defstruct pg-client-query-plan writer)

(defclass pg-client-query-output-stream (logical-utf8-output-stream) ())

(defmethod sb-gray:stream-write-char :before ((stream pg-client-query-output-stream) character)
  (declare (ignore stream))
  (when (char= character (code-char 0)) (error "SQL query contains a null byte")))

(defun pg-client-query-plan-size (plan)
  "Validate the complete producer and return its UTF-8 byte length."
  (let ((counter (make-instance 'pg-client-query-output-stream :buffer nil)))
    (funcall (pg-client-query-plan-writer plan) counter)
    (logical-flush-utf8-output counter)
    (let ((size (logical-utf8-size counter)))
      (when (> (+ size 5) +max-query-message-size+)
        (error "SQL query exceeds PostgreSQL message length"))
      size)))

(defun pg-client-send-query-plan (client plan)
  "Preflight and emit a deterministic query producer without a full SQL string."
  (let* ((writer (pg-client-query-plan-writer plan)))
    (let* ((size (pg-client-query-plan-size plan))
           (length (+ size 5)) (stream (pg-client-stream client))
           (started nil) (complete nil) (written 0))
      (unwind-protect
           (progn
             (setf started t)
             (write-byte (char-code #\Q) stream)
             (write-sequence (u32-to-bytes length) stream)
             (let ((output (make-instance 'pg-client-query-output-stream
                                          :buffer (lambda (bytes)
                                                    (incf written (length bytes))
                                                    (when (> written size)
                                                      (error "SQL query changed while sending"))
                                                    (write-sequence bytes stream)))))
               (funcall writer output)
               (logical-flush-utf8-output output))
             (unless (= written size) (error "SQL query changed while sending"))
             (write-byte 0 stream)
             (force-output stream)
             (setf complete t))
        (when (and started (not complete)) (pg-client-close client))))))

(defun pg-client-send-query (client query)
  (when (find (code-char 0) query)
    (error "SQL query contains a null byte"))
  (let* ((size (loop for start from 0 below (length query) by 4096
                     sum (length (string->bytes
                                  (subseq query start (min (length query) (+ start 4096)))))))
         (frame-length (+ size 5))
         (stream (pg-client-stream client))
         (started nil)
         (complete nil))
    (unless (<= frame-length #x7fffffff)
      (error "SQL query exceeds PostgreSQL message length"))
    (unwind-protect
         (progn
           (setf started t)
           (write-byte (char-code #\Q) stream)
           (write-sequence (u32-to-bytes frame-length) stream)
           (loop for start from 0 below (length query) by 4096
                 for bytes = (string->bytes
                              (subseq query start (min (length query) (+ start 4096))))
                 sum (length bytes) into written
                 do (write-sequence bytes stream)
                 finally (unless (= written size)
                           (error "SQL query changed while sending")))
           (write-byte 0 stream)
           (force-output stream)
           (setf complete t))
      (when (and started (not complete))
        (pg-client-close client)))))

(defun pg-client-read-column-description (payload)
  (let ((position 2)
        (count (bytes-to-u16 payload 0))
        (columns '()))
    (dotimes (i count)
      (multiple-value-bind (name next) (pg-client-cstring payload position)
        (setf position (+ next 6)) ; table OID and attribute number
        (let ((type-oid (bytes-to-u32 payload position))
              (typmod (logical-u32-signed
                       (bytes-to-u32 payload (+ position 6)))))
          (incf position 4)
          (incf position 8) ; typlen, typmod, format
          (push (make-pg-client-column :name name :type-oid type-oid
                                       :typmod typmod)
                columns))))
    (unless (= position (length payload))
      (error "RowDescription has trailing bytes"))
    (nreverse columns)))

(defun pg-client-read-data-row (payload columns)
  (let ((position 2)
        (count (bytes-to-u16 payload 0))
        (values '()))
    (unless (= count (length columns))
      (error "DataRow width does not match RowDescription"))
    (dotimes (i count)
      (let ((size (bytes-to-u32 payload position)))
        (incf position 4)
        (if (= size #xFFFFFFFF)
            (push nil values)
            (progn
              (unless (<= (+ position size) (length payload))
                (error "truncated DataRow value"))
              (push (bytes->string payload :start position :end (+ position size))
                    values)
              (incf position size)))))
    (unless (= position (length payload))
      (error "DataRow has trailing bytes"))
    (coerce (nreverse values) 'vector)))

(defun pg-client-streamed-query-frame (client columns &optional field-owner)
  "Read a complete query frame, decoding DataRow fields without a frame copy."
  (let ((stream (pg-client-stream client)) (complete nil))
    (unwind-protect
         (let* ((type (read-byte stream nil nil))
                (header (make-logical-wire-cursor :stream stream :remaining 4))
                (length (logical-wire-uint header 4)))
           (unless type (error "PostgreSQL connection ended during a message"))
           (unless (<= 4 length +max-query-message-size+)
             (error "invalid PostgreSQL message length"))
           (let* ((cursor (make-logical-wire-cursor :stream stream :remaining (- length 4)))
                  (decoded-p (= type (char-code #\D)))
                  (payload
                    (if decoded-p
                        (progn
                          (unless columns (error "DataRow arrived without RowDescription"))
                          (let ((count (logical-wire-uint cursor 2)))
                            (unless (= count (length columns))
                              (error "DataRow width does not match RowDescription"))
                            (coerce (loop for column in columns
                                          for size = (logical-wire-uint cursor 4)
                                          collect (if (= size #xffffffff) nil
                                                      (logical-wire-received-field cursor size field-owner
                                                                                   (pg-client-column-type-oid column))))
                                    'vector)))
                        (logical-wire-octets cursor (logical-wire-cursor-remaining cursor)))))
             (unless (zerop (logical-wire-cursor-remaining cursor))
               (error "DataRow has trailing bytes"))
             (setf complete t)
             (values (code-char type) payload decoded-p)))
      (unless complete (pg-client-close client)))))

(defun pg-client-query (client query &key on-row on-row-with-owner field-budget)
  "Send one simple query and return ROWS, COLUMNS, and the completion tag.

ON-ROW receives each streamed row. ON-ROW-WITH-OWNER receives a row, columns,
and its temporary field owner; returning :RETAIN transfers that owner's lifetime
to the callback. Otherwise the owner expires as soon as the callback returns."
  (when (and on-row on-row-with-owner)
    (error "only one row callback may be supplied"))
  (if (pg-client-query-plan-p query)
      (pg-client-send-query-plan client query)
      (pg-client-send-query client query))
  (let ((columns nil) (rows '()) (tag nil) (server-error nil))
    (loop
      (let ((owner (and on-row-with-owner (if field-budget
                                              (make-logical-wire-field-owner :budget field-budget)
                                              (make-logical-wire-field-owner))))
            (transferred nil))
        (sb-sys:without-interrupts
          (unwind-protect
               (sb-sys:with-local-interrupts
                 (multiple-value-bind (type payload decoded-p)
                     (if (or on-row on-row-with-owner)
                         (pg-client-streamed-query-frame client columns owner)
                         (pg-client-frame (pg-client-stream client)))
                   (case type
                     (#\T (setf columns (pg-client-read-column-description payload)))
                     (#\D
                      (unless columns (error "DataRow arrived without RowDescription"))
                      (let ((row (if decoded-p payload (pg-client-read-data-row payload columns))))
                        (cond
                          (on-row-with-owner
                           (when (eq :retain (funcall on-row-with-owner row columns owner))
                             (setf transferred t)))
                          (on-row (funcall on-row row columns))
                          (t (push row rows)))))
                     (#\C
                      (multiple-value-bind (value end) (pg-client-cstring payload 0)
                        (declare (ignore end))
                        (setf tag value)))
                     (#\E (setf server-error (pg-client-error-from-payload payload)))
                     ((#\N #\S) nil)
                     (#\Z
                      (when server-error (error server-error))
                      (return-from pg-client-query (values (nreverse rows) columns tag)))
                     (otherwise (error "unexpected simple-query response ~s" type)))))
            (unless transferred (close-logical-wire-field-owner owner))))))))

(defun pg-client-start-replication (client query)
  (pg-client-send-query client query)
  (loop
    (multiple-value-bind (type payload) (pg-client-frame (pg-client-stream client))
      (case type
        (#\W
         (unless (equalp payload #(0 0 0))
           (error "unsupported CopyBothResponse format"))
         (return))
        (#\E (error (pg-client-error-from-payload payload)))
        (#\N nil)
        (#\Z (error "replication command ended without CopyBothResponse"))
        (otherwise (error "unexpected replication startup response ~s" type))))))

(defun pg-client-replication-frame (client)
  (pg-client-frame (pg-client-stream client)))

(defun pg-client-push-u64 (payload value)
  (unless (and (integerp value) (<= 0 value #xFFFFFFFFFFFFFFFF))
    (error "PostgreSQL client LSN is out of range"))
  (loop for shift downfrom 56 to 0 by 8
        do (payload-push-byte payload (logand (ash value (- shift)) #xFF)))
  payload)

(defun pg-client-send-standby-status (client lsn)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\r))
    (pg-client-push-u64 payload lsn)
    (pg-client-push-u64 payload lsn)
    (pg-client-push-u64 payload lsn)
    (pg-client-push-u64 payload 0)
    (payload-push-byte payload 0)
    (write-message (pg-client-stream client) #\d payload)))
