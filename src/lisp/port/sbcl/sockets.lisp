;;;; sockets.lisp -- SBCL socket plumbing for the backend.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-bsd-sockets))

(defun make-listener (host port)
  (let ((s (make-instance 'sb-bsd-sockets:inet-socket
                          :type :stream :protocol :tcp)))
    (setf (sb-bsd-sockets:sockopt-reuse-address s) t)
    (sb-bsd-sockets:socket-bind s (sb-bsd-sockets:make-inet-address host) port)
    (sb-bsd-sockets:socket-listen s 16)
    s))

(defun accept-connection (listener)
  (sb-bsd-sockets:socket-accept listener))

(defun socket->stream (sock)
  (sb-bsd-sockets:socket-make-stream
   sock
   :input t :output t
   :element-type '(unsigned-byte 8)
   :buffering :full))

(defun close-connection (sock stream)
  (when stream
    (ignore-errors (finish-output stream))
    (ignore-errors (close stream)))
  (ignore-errors (sb-bsd-sockets:socket-close sock)))
