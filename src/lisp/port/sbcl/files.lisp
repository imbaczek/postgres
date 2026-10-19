;;;; files.lisp -- durable page I/O over SBCL file streams.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-posix))

(defun open-relation-file (pathname)
  (ensure-directories-exist pathname)
  (open pathname
        :direction :io
        :element-type '(unsigned-byte 8)
        :if-exists :overwrite
        :if-does-not-exist :create))

(defun relation-nblocks (stream)
  (let ((len (file-length stream)))
    (floor len +blcksz+)))

(defun read-page (stream block)
  "Return the page vector for BLOCK, or NIL if the block does not exist."
  (let ((buf (make-array +blcksz+ :element-type '(unsigned-byte 8)
                                  :initial-element 0)))
    (file-position stream (* block +blcksz+))
    (let ((n (read-sequence buf stream)))
      (if (< n +blcksz+)
          nil
          buf))))

(defun write-page (stream block page)
  (file-position stream (* block +blcksz+))
  (write-sequence page stream)
  (finish-output stream)
  (values))

(defun fsync-stream (stream)
  (finish-output stream)
  (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
