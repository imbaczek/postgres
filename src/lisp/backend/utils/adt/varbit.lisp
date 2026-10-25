;;;; varbit.c fixed-bit expression input and explicit casts.
(in-package #:pg)

(defconstant +sql-bit-max-typmod+ (* 10485760 8))
(defconstant +sql-bit-max-input-length+ #x7ffffff8)

(declaim (ftype function sql-cast-integer-input))

(defun sql-bit-validate-typmod (length)
  (when (< length 1)
    (ereport "22023" "length for type bit must be at least 1"))
  (when (> length +sql-bit-max-typmod+)
    (ereport "22023" "length for type bit cannot exceed ~d" +sql-bit-max-typmod+))
  length)

(defun sql-bit-input (text &optional (typmod -1))
  "Match bit_in; a supplied input typmod requires exact length before digit checks."
  (let* ((prefix (and (plusp (length text)) (char text 0)))
         (hex-p (and prefix (find prefix "xX")))
         (offset (if (and prefix (find prefix "bBxX")) 1 0))
         (size (- (length text) offset))
         (bit-length (* size (if hex-p 4 1))))
    (when (> bit-length +sql-bit-max-input-length+)
      (ereport "54000" "bit string length exceeds the maximum allowed (~d)" +sql-bit-max-input-length+))
    (when (and (plusp typmod) (/= bit-length typmod))
      (ereport "22026" "bit string length ~d does not match type bit(~d)" bit-length typmod))
    (let ((result (make-string bit-length :initial-element #\0)))
      (loop for position from offset below (length text)
            for character = (char text position)
            for index from 0
            do (if hex-p
                   (let ((digit (and (< (char-code character) 128) (digit-char-p character 16))))
                     (unless digit (ereport "22P02" "\"~a\" is not a valid hexadecimal digit" character))
                     (dotimes (bit 4)
                       (when (logbitp (- 3 bit) digit)
                         (setf (char result (+ (* index 4) bit)) #\1))))
                   (progn
                     (unless (find character "01")
                       (ereport "22P02" "\"~a\" is not a valid binary digit" character))
                     (setf (char result index) character))))
      result)))

(defun sql-bit-resize (bits typmod)
  "Explicit bit casts preserve leftmost bits and zero-pad on the right."
  (if (or (<= typmod 0) (= typmod (length bits))) bits
      (let ((result (make-string typmod :initial-element #\0)))
        (replace result bits :end2 (min typmod (length bits)))
        result)))

(defun sql-integer-to-bit (value typmod)
  "Keep low bits and sign-extend at the left, matching bitfromint4/bitfromint8."
  (let* ((size (if (<= 1 typmod +sql-bit-max-input-length+) typmod 1))
         (result (make-string size :initial-element #\0)))
    (dotimes (index size result)
      (when (logbitp index value)
        (setf (char result (- size index 1)) #\1)))))

(defun sql-bit-to-integer (bits width)
  "Interpret the bit pattern at destination width, not the string's own width."
  (when (> (length bits) width)
    (ereport "22003" "~a out of range" (if (= width 32) "integer" "bigint")))
  (let ((value (if (zerop (length bits)) 0 (parse-integer bits :radix 2))))
    (if (logbitp (1- width) value) (- value (ash 1 width)) value)))
