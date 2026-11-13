(in-package #:pg)

(defun logical-slot-copy-literal-image (magic maximum value label)
  "Frame a caller-validated bounded literal with magic, length, CRC and newline."
  (let* ((body (string->bytes (physical-bootstrap-manifest-text value)))
         (header (string->bytes
                  (format nil "~a ~d ~8,'0X~%" magic (length body) (wal-crc32 body)))))
    (unless (<= (+ (length header) (length body) 1) maximum)
      (error "~a exceeds its bound" label))
    (concatenate '(vector (unsigned-byte 8)) header body #(10))))

(defun logical-slot-copy-literal-decode (bytes magic maximum encode label)
  "Decode bounded literal framing and require a canonical re-encoding.
ENCODE validates the literal schema as well as reproducing its frame."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= 1 (length bytes) maximum))
    (error "invalid ~a bytes" label))
  (let* ((newline (position 10 bytes))
         (parts (and newline
                     (uiop:split-string (bytes->string (subseq bytes 0 newline)) :separator " "))))
    (unless (and (= 3 (length parts)) (equal magic (first parts))
                 (<= 1 (length (second parts)) (length (format nil "~d" maximum)))
                 (every #'digit-char-p (second parts))
                 (= 8 (length (third parts)))
                 (every (lambda (char) (find char "0123456789ABCDEF")) (third parts)))
      (error "invalid ~a header" label))
    (let* ((start (1+ newline)) (size (parse-integer (second parts))) (end (+ start size)))
      (unless (and (= (1+ end) (length bytes)) (= 10 (aref bytes end))
                   (= (parse-integer (third parts) :radix 16)
                      (wal-crc32 (subseq bytes start end))))
        (error "~a length or checksum mismatch" label))
      (let ((value (recovery-parse-manifest-text (bytes->string (subseq bytes start end)))))
        (unless (equalp bytes (funcall encode value))
          (error "noncanonical ~a" label))
        value))))
