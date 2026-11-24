(in-package #:pg.metadata-literal)

(defun physical-bootstrap-manifest-text (manifest)
  "Print stable eligible literal trees directly, producing an owned string.
Standard I/O syntax with readably NIL avoids SBCL's readable BASE-CHAR #A arrays.
Ineligible trees use the historical copier fallback; depth or cycle rejection
only declines the fast path and does not add a new cycle error."
  (when (%metadata-literal-tree-p manifest 0)
    (return-from physical-bootstrap-manifest-text
      (with-standard-io-syntax
        (let ((*print-readably* nil))
          (prin1-to-string manifest)))))
  (labels ((literal (value)
             (typecase value
               (string (map 'string #'identity value))
               (cons (mapcar #'literal value))
               (t value))))
    (with-standard-io-syntax (prin1-to-string (literal manifest)))))

(defun recovery-check-literal-text (text)
  "Accept only the literal list/string/number/keyword grammar emitted by our manifest."
  (let ((quoted nil) (escaped nil) (depth 0))
    (loop for char across text
          do (cond
               (quoted
                (cond (escaped (setf escaped nil))
                      ((char= char #\\) (setf escaped t))
                      ((char= char #\") (setf quoted nil))))
               ((char= char #\") (setf quoted t))
               ((char= char #\() (when (> (incf depth) 16) (error "recovery manifest too deep")))
               ((char= char #\)) (when (minusp (decf depth)) (error "invalid recovery manifest")))
               ((or (find char " :+-" :test #'char=)
                    (find char '(#\Tab #\Newline #\Return #\Page))
                    (and (char>= char #\A) (char<= char #\Z))
                    (and (char>= char #\0) (char<= char #\9))))
               (t (error "nonliteral syntax in recovery manifest"))))
    (when (or quoted escaped (not (zerop depth))) (error "incomplete recovery manifest"))))

(defun recovery-parse-manifest-text (text)
  "Read only the bounded literal grammar, with reader evaluation disabled."
  (recovery-check-literal-text text)
  (with-standard-io-syntax
    (let ((*read-eval* nil) (*package* (find-package "CL-USER")))
      (multiple-value-bind (manifest position) (read-from-string text)
        (unless (every (lambda (char) (find char '(#\Space #\Tab #\Newline #\Return)))
                       (subseq text position))
          (error "trailing recovery manifest data"))
        manifest))))

(defun metadata-literal-envelope-image (magic value maximum label)
  "Encode a canonical literal in a magic, CRC and terminal-newline envelope."
  (let* ((body (string->bytes (physical-bootstrap-manifest-text value)))
         (header (string->bytes (format nil "~a ~8,'0X~%" magic (wal-crc32 body)))))
    (unless (<= (+ (length header) (length body) 1) maximum)
      (error "~a exceeds its record bound" label))
    (concatenate '(vector (unsigned-byte 8)) header body #(10))))

(defun metadata-literal-envelope-read (magic bytes maximum label)
  "Read a checksummed bounded literal, leaving schema and canonicality to the caller."
  (unless (typep bytes '(vector (unsigned-byte 8)))
    (error "~a requires an octet vector" label))
  (unless (<= (length bytes) maximum)
    (error "~a exceeds its record bound" label))
  (let* ((newline (position 10 bytes))
         (header (when newline (bytes->string bytes :end newline)))
         (prefix (concatenate 'string magic " ")))
    (unless (and header (= (length header) (+ (length prefix) 8))
                 (uiop:string-prefix-p prefix header))
      (error "invalid ~a envelope" label))
    (let* ((start (1+ newline))
           (end (1- (length bytes)))
           (crc (parse-integer header :start (length prefix) :radix 16)))
      (unless (and (<= start end) (= 10 (aref bytes end))
                   (= crc (wal-crc32 bytes start end)))
        (error "~a checksum mismatch" label))
      (recovery-parse-manifest-text (bytes->string bytes :start start)))))

(defun metadata-literal-envelope-decode (magic bytes maximum normalize label)
  "Validate framing, apply NORMALIZE to the literal and require canonical bytes."
  (let ((record (funcall normalize (metadata-literal-envelope-read magic bytes maximum label))))
    (unless (equalp bytes (metadata-literal-envelope-image magic record maximum label))
      (error "noncanonical ~a record" label))
    record))

(defun metadata-sized-literal-header (magic size crc)
  "Encode the canonical sized header for a literal or journal body."
  (string->bytes (format nil "~a ~d ~8,'0X~%" magic size crc)))

(defun metadata-sized-literal-image (magic maximum value label)
  "Frame a caller-validated bounded literal with magic, length, CRC and newline."
  (let* ((body (string->bytes (physical-bootstrap-manifest-text value)))
         (header (metadata-sized-literal-header magic (length body) (wal-crc32 body))))
    (unless (<= (+ (length header) (length body) 1) maximum)
      (error "~a exceeds its bound" label))
    (concatenate '(vector (unsigned-byte 8)) header body #(10))))

(defun metadata-sized-literal-span (bytes magic maximum maximum-body label
                                    &key maximum-header maximum-size-digits)
  "Validate a sized header and body checksum, returning body start, end and CRC.
The caller validates the literal schema and any bytes after the body's newline."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= (length bytes) maximum))
    (error "invalid or oversized ~a" label))
  (let* ((limit (if maximum-header (min maximum-header (length bytes)) (length bytes)))
         (newline (position 10 bytes :end limit))
         (parts (and newline (uiop:split-string (bytes->string bytes :end newline) :separator " "))))
    (unless (and (= 3 (length parts)) (equal magic (first parts))
                 (plusp (length (second parts)))
                 (or (null maximum-size-digits) (<= (length (second parts)) maximum-size-digits))
                 (every #'digit-char-p (second parts)) (= 8 (length (third parts))))
      (error "invalid ~a header" label))
    (let* ((size (parse-integer (second parts)))
           (crc (parse-integer (third parts) :radix 16))
           (start (1+ newline))
           (end (+ start size)))
      (unless (and (<= 1 size maximum-body)
                   (< end (length bytes)) (= 10 (aref bytes end))
                   (= crc (wal-crc32 bytes start end))
                   (not (mismatch bytes (metadata-sized-literal-header magic size crc) :end1 start)))
        (error "~a length or checksum mismatch" label))
      (values start end crc))))
