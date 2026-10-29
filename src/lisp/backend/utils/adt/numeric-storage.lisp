;;;; PostgreSQL packed finite Numeric representation (numeric.c:make_result).
(in-package #:pg)

(defun sql-numeric-to-packed-octets (value)
  "Encode a finite SQL numeric as a complete little-endian packed datum.
Use PostgreSQL's four-byte varlena header; special numeric values are rejected."
  (unless (or (integerp value) (sql-decimal-p value))
    (error "Packed numeric requires a finite value"))
  (unless (typep (sql-numeric-scale value) '(integer 0 16383))
    (error "Invalid packed numeric scale"))
  (let* ((scale (sql-numeric-scale value))
         (coefficient (sql-numeric-coefficient value))
         (number (* (abs coefficient) (expt 10 (mod (- scale) 4))))
         (fraction-groups (ceiling scale 4))
         (digits nil))
    (when (plusp number)
      (let* ((text (write-to-string number :base 10 :radix nil))
             (first-width (or (and (plusp (mod (length text) 4))
                                   (mod (length text) 4))
                              4)))
        (push (parse-integer text :end first-width) digits)
        (loop for start from first-width below (length text) by 4
              do (push (parse-integer text :start start :end (+ start 4)) digits))
        (setf digits (nreverse digits))))
    (let ((weight (if digits (- (length digits) fraction-groups 1) 0)))
      (setf digits (nreverse (member-if-not #'zerop (nreverse digits))))
      (unless (<= -32768 weight 32767)
        (error "Packed numeric weight out of range"))
      (let* ((short (and (<= scale 63) (<= -64 weight 63)))
             (header-size (if short 6 8))
             (size (+ header-size (* 2 (length digits))))
             (bytes (make-array size :element-type '(unsigned-byte 8)
                                     :initial-element 0))
             (negative (and digits (minusp coefficient))))
        (page-set-u32 bytes 0 (ash size 2))
        (if short
            (page-set-u16 bytes 4
                          (logior #x8000 (if negative #x2000 0)
                                  (ash scale 7) (logand weight #x7f)))
            (progn
              (page-set-u16 bytes 4 (logior (if negative #x4000 0) scale))
              (page-set-u16 bytes 6 (logand weight #xffff))))
        (loop for digit in digits for offset from header-size by 2
              do (page-set-u16 bytes offset digit))
        bytes))))

(defun sql-numeric-from-packed-octets (bytes)
  "Decode a complete little-endian finite Numeric with a four-byte header.
Check lengths, digit range, and precision; reject external, short-varlena and
special values. Noncanonical zero digits and weights are accepted."
  (check-type bytes (vector (unsigned-byte 8)))
  (labels ((bad () (ereport "XX001" "invalid packed numeric")))
    (unless (and (>= (length bytes) 6)
                 (= (page-u32 bytes 0) (ash (length bytes) 2)))
      (bad))
    (let* ((header (page-u16 bytes 4))
           (kind (logand header #xc000))
           (short (= kind #x8000))
           (header-size (if short 6 8)))
      (unless (and (member kind '(#x0000 #x4000 #x8000))
                   (>= (length bytes) header-size)
                   (evenp (- (length bytes) header-size)))
        (bad))
      (let* ((negative (if short (logbitp 13 header) (= kind #x4000)))
             (scale (if short (ldb (byte 6 7) header) (logand header #x3fff)))
             (encoded-weight (if short (logand header #x7f) (page-u16 bytes 6)))
             (weight (if short
                         (if (logbitp 6 encoded-weight) (- encoded-weight 128) encoded-weight)
                         (if (logbitp 15 encoded-weight) (- encoded-weight 65536) encoded-weight)))
             (count (/ (- (length bytes) header-size) 2))
             (accumulator 0))
        (dotimes (i count)
          (let ((digit (page-u16 bytes (+ header-size (* i 2)))))
            (unless (< digit 10000) (bad))
            (setf accumulator (+ (* accumulator 10000) digit))))
        (let* ((decimal-exponent (+ scale (* 4 (- (1+ weight) count))))
               (coefficient
                 (if (minusp decimal-exponent)
                     (multiple-value-bind (q remainder)
                         (floor accumulator (expt 10 (- decimal-exponent)))
                       (unless (zerop remainder) (bad))
                       q)
                     (* accumulator (expt 10 decimal-exponent)))))
          (make-sql-decimal (if negative (- coefficient) coefficient) scale))))))

(defun sql-numeric-to-varlena-payload (value)
  "Encode numeric VALUE without its outer varlena header, including specials."
  (let ((special (case value
                   (:numeric-nan #xc000)
                   (:numeric-positive-infinity #xd000)
                   (:numeric-negative-infinity #xf000))))
    (if special
        (let ((bytes (make-array 2 :element-type '(unsigned-byte 8))))
          (page-set-u16 bytes 0 special)
          bytes)
        (subseq (sql-numeric-to-packed-octets value) 4))))

(defun sql-numeric-from-varlena-payload (payload)
  "Decode finite or special numeric PAYLOAD without an outer varlena header."
  (check-type payload (vector (unsigned-byte 8)))
  (when (< (length payload) 2) (ereport "XX001" "invalid packed numeric"))
  (let ((header (page-u16 payload 0)))
    (when (= (logand header #xc000) #xc000)
      (unless (= (length payload) 2) (ereport "XX001" "invalid packed numeric"))
      (return-from sql-numeric-from-varlena-payload
        (case header
          (#xc000 :numeric-nan)
          (#xd000 :numeric-positive-infinity)
          (#xf000 :numeric-negative-infinity)
          (otherwise (ereport "XX001" "invalid packed numeric"))))))
  (let ((bytes (make-array (+ 4 (length payload))
                           :element-type '(unsigned-byte 8))))
    (page-set-u32 bytes 0 (ash (length bytes) 2))
    (replace bytes payload :start1 4)
    (sql-numeric-from-packed-octets bytes)))
