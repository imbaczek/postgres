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
  "Read exactly N bytes. Return bytes and N, or NIL and the count on EOF."
  (when (zerop n)
    (return-from read-exact-bytes
      (values (make-array 0 :element-type '(unsigned-byte 8)) 0)))
  (let ((buf (make-array n :element-type '(unsigned-byte 8))))
    (let ((got (read-sequence buf stream)))
      (when (< got n)
        (return-from read-exact-bytes (values nil got)))
      (values buf got))))

(defun malformed-protocol-message (message)
  (ereport-fatal "08P01" message))

(defun read-startup-packet (stream &optional (operation-wrapper #'funcall)
                            &rest options)
  "Read an untyped startup packet and optionally classify its request code.

BEFORE-BODY runs after the request code is read and before the rest of the
packet. A packet whose code equals BYPASS-CODE skips BEFORE-BODY and returns T
as a second value. Blocking reads stay outside OPERATION-WRAPPER."
  (let ((before-body (getf options :before-body))
        (bypass-code (getf options :bypass-code)))
    (multiple-value-bind (lenb got) (read-exact-bytes stream 4)
      (unless lenb
        (if (zerop got)
            (return-from read-startup-packet nil)
            (funcall operation-wrapper
                     (lambda ()
                       (malformed-protocol-message
                        "incomplete startup packet length")))))
      (let ((len (bytes-to-u32 lenb 0)))
        (unless (<= 8 len +max-startup-message-size+)
          (funcall operation-wrapper
                   (lambda ()
                     (if (< len 8)
                         (malformed-protocol-message
                          "startup packet length is too short")
                         (malformed-protocol-message
                          "startup packet exceeds the size limit")))))
        (multiple-value-bind (code-bytes code-got) (read-exact-bytes stream 4)
          (unless code-bytes
            (funcall operation-wrapper
                     (lambda ()
                       (if (zerop code-got)
                           (malformed-protocol-message
                            "incomplete startup packet body")
                           (malformed-protocol-message
                            "truncated startup packet body")))))
          (let ((bypass-p (and bypass-code
                               (= (bytes-to-u32 code-bytes 0) bypass-code))))
            (when (and before-body (not bypass-p))
              (funcall before-body))
            (multiple-value-bind (tail tail-got)
                (read-exact-bytes stream (- len 8))
              (unless tail
                (funcall operation-wrapper
                         (lambda ()
                           (if (zerop tail-got)
                               (malformed-protocol-message
                                "incomplete startup packet body")
                               (malformed-protocol-message
                                "truncated startup packet body")))))
              (values (concatenate '(vector (unsigned-byte 8)) code-bytes tail)
                      bypass-p))))))))

(defun read-message-frame (stream &optional (maximum +max-query-message-size+))
  "Read one bounded raw frame without validating or constructing a message.

Frontend decoding belongs under live-operation admission; client readers
validate their own response schema. Only the allocation bound is checked here.
MAXIMUM bounds the length word value, including its four bytes; its default
retains the frontend limit."
  (let ((type-byte (read-byte stream nil nil)))
    (when (null type-byte)
      (return-from read-message-frame (values nil nil nil)))
    (multiple-value-bind (length-bytes got) (read-exact-bytes stream 4)
      (declare (ignore got))
      (unless length-bytes
        (return-from read-message-frame (values type-byte nil nil)))
      (let* ((length (logior (ash (aref length-bytes 0) 24)
                             (ash (aref length-bytes 1) 16)
                             (ash (aref length-bytes 2) 8)
                             (aref length-bytes 3))))
        (if (<= 4 length maximum)
            (multiple-value-bind (payload body-got)
                (read-exact-bytes stream (- length 4))
              (declare (ignore body-got))
              (values type-byte length-bytes payload))
            ;; Invalid lengths are returned for admitted validation without
            ;; allocating or attempting to consume an unbounded body.
            (values type-byte length-bytes nil))))))

(defun decode-frontend-message (type-byte length-bytes payload)
  "Validate raw frame fields and construct a frontend message.

Production callers run this inside the live-operation gate."
  (labels ((reject (message)
             (malformed-protocol-message message)))
    (unless length-bytes
      (reject "incomplete frontend message length"))
    (let ((length (bytes-to-u32 length-bytes 0)))
      (when (< length 4)
        (reject "frontend message length is too short"))
      (when (> length +max-query-message-size+)
        (reject "frontend message exceeds the size limit"))
      (when (and (= type-byte (char-code #\X)) (/= length 4))
        (reject "terminate message must not contain a payload"))
      (unless (and payload (= (length payload) (- length 4)))
        (reject "incomplete frontend message body"))
      (when (= type-byte (char-code #\Q))
        (let ((terminator (position 0 payload)))
          (unless (and terminator
                       (= terminator (1- (length payload))))
            (reject
             "simple query message must contain one trailing null byte"))))
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
