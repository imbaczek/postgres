(require :sb-bsd-sockets)

(let* ((port (parse-integer (sb-ext:posix-getenv "STALLED_PORT")))
       (ready-file (sb-ext:posix-getenv "STALLED_READY"))
       (socket (make-instance 'sb-bsd-sockets:inet-socket
                              :type :stream
                              :protocol :tcp))
       (stream nil))
  (unwind-protect
       (progn
         (sb-bsd-sockets:socket-connect
          socket (sb-bsd-sockets:make-inet-address "127.0.0.1") port)
         (setf stream
               (sb-bsd-sockets:socket-make-stream
                socket :input t :output t
                :element-type '(unsigned-byte 8) :buffering :none))
         ;; Send PostgreSQL's SSLRequest and wait for the server's "N" reply.
         ;; This confirms the Lisp backend accepted the socket and is reading
         ;; the startup protocol before the test stalls the connection.
         (dolist (byte '(0 0 0 8 4 210 22 47))
           (write-byte byte stream))
         (force-output stream)
         (unless (= (read-byte stream) (char-code #\N))
           (error "server did not reject SSLRequest"))
         (with-open-file (ready ready-file
                                :direction :output
                                :if-exists :supersede)
           (write-line "accepted" ready))
         (sleep 30))
    (when stream
      (ignore-errors (close stream :abort t)))
    (ignore-errors (sb-bsd-sockets:socket-close socket))))
