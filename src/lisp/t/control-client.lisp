;;;; control-client.lisp -- test helper: talk to the control-plane REPL.
;;;;
;;;; Usage: control-client.lisp tcp HOST:PORT  |  control-client.lisp unix PATH
;;;; Reads forms from stdin (one per line), sends them to the control REPL,
;;;; and prints everything the server sends back until it closes the socket.
;;;; Invoked by t/control-repl.sh with SBCL's --script option.

(require :sb-bsd-sockets)

(defun read-stdin-text ()
  (with-output-to-string (out)
    (loop for line = (read-line *standard-input* nil :eof)
          until (eq line :eof)
          do (write-line line out))))

(defun connect-socket (mode addr)
  (if (string= mode "unix")
      (let ((s (make-instance 'sb-bsd-sockets:local-socket
                              :type :stream :protocol 0)))
        (sb-bsd-sockets:socket-connect s addr)
        s)
      (let* ((colon (position #\: addr :from-end t))
             (host (subseq addr 0 colon))
             (port (parse-integer addr :start (1+ colon)))
             (s (make-instance 'sb-bsd-sockets:inet-socket
                               :type :stream :protocol :tcp)))
        (sb-bsd-sockets:socket-connect s (sb-bsd-sockets:make-inet-address host) port)
        s)))

(let* ((args (cdr sb-ext:*posix-argv*))
       (mode (first args))
       (addr (second args)))
  (unless (and mode addr)
    (format *error-output* "usage: control-client.lisp tcp HOST:PORT | unix PATH~%")
    (sb-ext:exit :code 2))
  (let* ((payload (read-stdin-text))
         (sock (connect-socket mode addr))
         (stream (sb-bsd-sockets:socket-make-stream
                  sock :input t :output t
                  :element-type 'character :buffering :line)))
    (write-string payload stream)
    (finish-output stream)
    (loop for line = (read-line stream nil :eof)
          until (eq line :eof)
          do (write-line line))
    (finish-output)
    (ignore-errors (close stream))
    (ignore-errors (sb-bsd-sockets:socket-close sock))
    (sb-ext:exit :code 0)))
