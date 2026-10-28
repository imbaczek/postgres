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
               (case code
                 (#.(char-code #\C) (setf sqlstate value))
                 (#.(char-code #\M) (setf message value)))))
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

(defun pg-client-send-query (client query)
  (when (find (code-char 0) query)
    (error "SQL query contains a null byte"))
  (let ((payload (make-payload)))
    (payload-push payload (string->bytes query))
    (vector-push-extend 0 payload)
    (write-message (pg-client-stream client) #\Q payload)))

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

(defun pg-client-query (client query &key on-row)
  "Send one simple query and return ROWS, COLUMNS, and the completion tag.

When ON-ROW is supplied, call it for each streamed row without retaining the
result set. This is used by the initial copy so its memory bound is the worker's
8 MiB apply chunk rather than the complete source table."
  (pg-client-send-query client query)
  (let ((columns nil)
        (rows '())
        (tag nil)
        (server-error nil))
    (loop
      (multiple-value-bind (type payload) (pg-client-frame (pg-client-stream client))
        (case type
          (#\T (setf columns (pg-client-read-column-description payload)))
          (#\D
           (unless columns (error "DataRow arrived without RowDescription"))
           (let ((row (pg-client-read-data-row payload columns)))
             (if on-row
                 (funcall on-row row columns)
                 (push row rows))))
          (#\C
           (multiple-value-bind (value end) (pg-client-cstring payload 0)
             (declare (ignore end))
             (setf tag value)))
          (#\E (setf server-error (pg-client-error-from-payload payload)))
          ((#\N #\S) nil)
          (#\Z
           (when server-error (error server-error))
           (return (values (nreverse rows) columns tag)))
          (otherwise
           (error "unexpected simple-query response ~s" type)))))))

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
