(in-package #:pg)

(defun logical-slot-copy-literal-image (magic maximum value label)
  "Frame a caller-validated bounded literal with magic, length, CRC and newline."
  (let* ((body (string->bytes (physical-bootstrap-manifest-text value)))
         (header (metadata-sized-literal-header magic (length body) (wal-crc32 body))))
    (unless (<= (+ (length header) (length body) 1) maximum)
      (error "~a exceeds its bound" label))
    (concatenate '(vector (unsigned-byte 8)) header body #(10))))

(defun logical-slot-copy-literal-decode (bytes magic maximum encode label)
  "Decode a framed literal and use ENCODE to validate its schema and canonical bytes."
  (multiple-value-bind (start end)
      (metadata-sized-literal-span bytes magic maximum maximum label
                                   :maximum-size-digits (length (format nil "~d" maximum)))
    (unless (= (1+ end) (length bytes))
      (error "~a length or checksum mismatch" label))
    (let ((value (recovery-parse-manifest-text (bytes->string bytes :start start :end end))))
      (unless (equalp bytes (funcall encode value))
        (error "noncanonical ~a" label))
      value)))
