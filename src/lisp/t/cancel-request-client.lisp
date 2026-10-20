;;;; cancel-request-client.lisp -- exercise PostgreSQL's out-of-band cancel wire packet.

(require :asdf)
(pushnew (truename (merge-pathnames #p"../"
                                    (uiop:pathname-directory-pathname
                                     *load-truename*)))
         asdf:*central-registry* :test #'equal)
(asdf:load-system "pg")

(in-package #:pg)

(defun cancel-test-concat-octets (&rest parts)
  (let* ((size (reduce #'+ parts :key #'length :initial-value 0))
         (bytes (make-array size :element-type '(unsigned-byte 8)))
         (offset 0))
    (dolist (part parts)
      (replace bytes part :start1 offset)
      (incf offset (length part)))
    bytes))

(defun cancel-test-open-connection (port)
  (let ((socket (make-instance 'sb-bsd-sockets:inet-socket
                               :type :stream :protocol :tcp)))
    (sb-bsd-sockets:socket-connect
     socket (sb-bsd-sockets:make-inet-address "127.0.0.1") port)
    (values socket
            (sb-bsd-sockets:socket-make-stream
             socket :input t :output t
             :element-type '(unsigned-byte 8) :buffering :full))))

(defun cancel-test-read-message (stream)
  (let ((type (read-byte stream nil nil)))
    (unless type
      (error "server closed the connection before ReadyForQuery"))
    (multiple-value-bind (length-bytes got) (read-exact-bytes stream 4)
      (unless length-bytes
        (error "incomplete backend message length after ~d bytes" got))
      (let ((length (bytes-to-u32 length-bytes 0)))
        (when (< length 4)
          (error "backend sent invalid message length ~d" length))
        (multiple-value-bind (payload payload-got)
            (read-exact-bytes stream (- length 4))
          (unless payload
            (error "incomplete backend message body after ~d bytes" payload-got))
          (values (code-char type) payload))))))

(defun cancel-test-read-until-ready (stream)
  (loop with messages = '()
        do (multiple-value-bind (type payload)
               (cancel-test-read-message stream)
             (push (cons type payload) messages)
             (when (char= type #\Z)
               (return (nreverse messages))))))

(defun cancel-test-startup (stream)
  (let* ((body (cancel-test-concat-octets
                (u32-to-bytes 196608)
                (string->bytes "user") #(0)
                (string->bytes "postgres") #(0)
                (string->bytes "database") #(0)
                (string->bytes "postgres") #(0)
                #(0)))
         (packet (cancel-test-concat-octets
                  (u32-to-bytes (+ 4 (length body))) body)))
    (write-sequence packet stream)
    (force-output stream)
    (let* ((messages (cancel-test-read-until-ready stream))
           (key-message (find #\K messages :key #'car)))
      (unless (and key-message (= (length (cdr key-message)) 8))
        (error "server did not send valid BackendKeyData"))
      (values (bytes-to-u32 (cdr key-message) 0)
              (bytes-to-u32 (cdr key-message) 4)))))

(defun cancel-test-send-cancel (port pid key)
  (multiple-value-bind (socket stream) (cancel-test-open-connection port)
    (unwind-protect
         (progn
           (write-sequence
            (cancel-test-concat-octets
             (u32-to-bytes 16)
             (u32-to-bytes +cancel-request-code+)
             (u32-to-bytes pid)
             (u32-to-bytes key))
            stream)
           (force-output stream))
      (ignore-errors (close stream))
      (ignore-errors (sb-bsd-sockets:socket-close socket)))))

(defun cancel-test-control-eval (port form)
  (let* ((socket (make-instance 'sb-bsd-sockets:inet-socket
                                :type :stream :protocol :tcp))
         (stream nil))
    (unwind-protect
         (progn
           (sb-bsd-sockets:socket-connect
            socket (sb-bsd-sockets:make-inet-address "127.0.0.1") port)
           (setf stream (sb-bsd-sockets:socket-make-stream
                         socket :input t :output t
                         :element-type 'character :buffering :line))
           (write-line form stream)
           (write-line "(quit)" stream)
           (force-output stream)
           (with-output-to-string (out)
             (loop for line = (read-line stream nil :eof)
                   until (eq line :eof)
                   do (write-line line out))))
      (when stream (ignore-errors (close stream)))
      (ignore-errors (sb-bsd-sockets:socket-close socket)))))

(defun cancel-test-wait-for-xid (port pid)
  (let ((reply
          (cancel-test-control-eval
           port
           (format nil
                   "(loop repeat 100 for backend = (lookup-backend ~d) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))"
                   pid))))
    (unless (search ":WAITING" reply)
      (error "backend ~d did not enter a transaction-ID wait: ~a" pid reply))))

(defun cancel-test-send-query (stream query)
  (write-message stream #\Q
                 (cancel-test-concat-octets (string->bytes query) #(0))))

(defun cancel-test-assert-not-cancelled (stream description)
  (sleep 0.3)
  (when (listen stream)
    (error "~a unexpectedly cancelled the backend" description)))

(defun cancel-test-main ()
  (let* ((port (parse-integer (sb-ext:posix-getenv "CANCEL_TEST_PORT")))
         (control-port
           (parse-integer (sb-ext:posix-getenv "CANCEL_TEST_CONTROL_PORT")))
         (socket nil)
         (stream nil))
    (unwind-protect
         (progn
           (multiple-value-setq (socket stream)
             (cancel-test-open-connection port))
           (multiple-value-bind (pid key) (cancel-test-startup stream)
             ;; The shutdown test holds this unique key in another transaction.
             (cancel-test-send-query stream "insert into t values (1);")
             (cancel-test-wait-for-xid control-port pid)
             (cancel-test-send-cancel port pid (logxor key 1))
             (cancel-test-assert-not-cancelled stream "wrong cancel key")
             (cancel-test-send-cancel port #xFFFFFFFF key)
             (cancel-test-assert-not-cancelled stream "wrong cancel PID")
             ;; The control REPL confirmed the backend is inside wait-for-xid,
             ;; so one valid out-of-band request must interrupt this statement.
             (cancel-test-send-cancel port pid key)
             (loop repeat 50 until (listen stream) do (sleep 0.1))
             (unless (listen stream)
               (error "valid CancelRequest for PID ~d did not interrupt the blocked INSERT; backend state: ~a"
                      pid
                      (cancel-test-control-eval
                       control-port
                       (format nil
                               "(let ((backend (lookup-backend ~d))) (and backend (list (backend-executing backend) (backend-cancel-pending backend) (backend-waiting-xid backend))))"
                               pid))))
             (let* ((messages (cancel-test-read-until-ready stream))
                    (error-message (find #\E messages :key #'car)))
               (unless (and error-message
                            (search "57014" (bytes->string (cdr error-message)))
                            (search "canceling statement"
                                    (bytes->string (cdr error-message))))
                 (error "valid CancelRequest did not return SQLSTATE 57014")))
             (cancel-test-send-query stream "select 99;")
             (let* ((messages (cancel-test-read-until-ready stream))
                    (row (find #\D messages :key #'car)))
               (unless (and row
                            (= (bytes-to-u16 (cdr row) 0) 1)
                            (= (bytes-to-u32 (cdr row) 2) 2)
                            (string= (bytes->string (cdr row) :start 6 :end 8)
                                     "99"))
                 (error "backend did not survive CancelRequest"))))
           (format t "cancel-request-client: PASS~%"))
      (when stream (ignore-errors (close stream)))
      (when socket (ignore-errors (sb-bsd-sockets:socket-close socket))))))

(cancel-test-main)
