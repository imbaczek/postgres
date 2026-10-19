;;;; pqcomm.lisp -- frontend/backend message framing.

(in-package #:pg)

(defstruct (frontend-message (:constructor make-frontend-message (type payload)))
  type
  payload)

;;; ---- byte helpers (network/big-endian, per the PostgreSQL wire protocol) ----

(defun bytes-to-u16 (b off)
  (logior (ash (aref b off) 8)
          (aref b (1+ off))))

(defun bytes-to-u32 (b off)
  (logior (ash (aref b off) 24)
          (ash (aref b (+ off 1)) 16)
          (ash (aref b (+ off 2)) 8)
          (aref b (+ off 3))))

(defun u16-to-bytes (v)
  (let ((b (make-array 2 :element-type '(unsigned-byte 8))))
    (setf (aref b 0) (logand (ash v -8) #xFF)
          (aref b 1) (logand v #xFF))
    b))

(defun u32-to-bytes (v)
  (let ((b (make-array 4 :element-type '(unsigned-byte 8))))
    (setf (aref b 0) (logand (ash v -24) #xFF)
          (aref b 1) (logand (ash v -16) #xFF)
          (aref b 2) (logand (ash v -8) #xFF)
          (aref b 3) (logand v #xFF))
    b))

(defun string->bytes (string)
  (sb-ext:string-to-octets string :external-format :utf-8))

(defun bytes->string (bytes &key (start 0) (end (length bytes)))
  (sb-ext:octets-to-string (subseq bytes start end) :external-format :utf-8))

;;; ---- reading ----

(defun read-exact-bytes (stream n)
  "Read exactly N bytes. Return NIL on EOF."
  (when (zerop n)
    (return-from read-exact-bytes (make-array 0 :element-type '(unsigned-byte 8))))
  (let ((buf (make-array n :element-type '(unsigned-byte 8))))
    (let ((got (read-sequence buf stream)))
      (when (< got n)
        (return-from read-exact-bytes nil))
      buf)))

(defun read-startup-packet (stream)
  "Read one untyped startup packet body. Return NIL on EOF."
  (let ((lenb (read-exact-bytes stream 4)))
    (when (null lenb)
      (return-from read-startup-packet nil))
    (let* ((len (bytes-to-u32 lenb 0)))
      (when (< len 4)
        (return-from read-startup-packet nil))
      (read-exact-bytes stream (- len 4)))))

(defun read-message (stream)
  "Read one typed frontend message. Return NIL on EOF."
  (let ((type-byte (read-byte stream nil nil)))
    (when (null type-byte)
      (return-from read-message nil))
    (let* ((lenb (read-exact-bytes stream 4))
           (len (and lenb (bytes-to-u32 lenb 0)))
           (payload (and lenb (>= len 4) (read-exact-bytes stream (- len 4)))))
      (when (or (null lenb) (null payload))
        (return-from read-message nil))
      (make-frontend-message (code-char type-byte) payload))))

;;; ---- writing ----

(defun write-message (stream type payload)
  (write-byte (char-code type) stream)
  (write-sequence (u32-to-bytes (+ 4 (length payload))) stream)
  (write-sequence payload stream)
  (force-output stream))

(defun make-payload ()
  (make-array 0 :adjustable t :fill-pointer 0
                :element-type '(unsigned-byte 8)))

(defun payload-push (p bytes)
  (loop for b across bytes do (vector-push-extend b p))
  p)

(defun payload-push-u16 (p v) (payload-push p (u16-to-bytes v)) p)
(defun payload-push-u32 (p v) (payload-push p (u32-to-bytes v)) p)
(defun payload-push-byte (p v) (vector-push-extend v p) p)
(defun payload-push-string (p s) (payload-push p (string->bytes s)) p)

(defun payload-push-cstring (p s)
  (payload-push-string p s)
  (vector-push-extend 0 p)
  p)
