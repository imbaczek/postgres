;;;; float.lisp -- IEEE floating point input and output helpers.
(in-package #:pg)

(defun sql-float-bits (value type)
  (if (eq type :float4)
      (logand #xffffffff (sb-kernel:single-float-bits value))
      (logand #xffffffffffffffff (sb-kernel:double-float-bits value))))

(defun sql-float-from-bits (bits type)
  (if (eq type :float4)
      (sb-kernel:make-single-float (if (logbitp 31 bits) (- bits #x100000000) bits))
      (let* ((high (ldb (byte 32 32) bits))
             (signed-high (if (logbitp 31 high) (- high #x100000000) high)))
        (sb-kernel:make-double-float signed-high (ldb (byte 32 0) bits)))))

(defun sql-float-index-bits (value type)
  "Canonicalize SQL-equal NaNs and signed zeros and return sortable bits."
  (let* ((width (if (eq type :float4) 32 64))
         (sign (ash 1 (1- width)))
         (mask (1- (ash 1 width)))
         (bits (cond ((sb-ext:float-nan-p value)
                      (if (= width 32) #x7fc00000 #x7ff8000000000000))
                     ((zerop value) 0)
                     (t (sql-float-bits value type)))))
    (if (logbitp (1- width) bits) (logxor bits mask) (logxor bits sign))))

(defun sql-float-group-key (value)
  "Return a stable key for PostgreSQL equality, including NaNs and signed zero."
  (let ((type (if (typep value 'single-float) :float4 :float8)))
    (list type (sql-float-index-bits value type))))

(defun sql-float-to-numeric (value type)
  "Match float4_numeric/float8_numeric's six or fifteen significant digits."
  (cond ((sb-ext:float-nan-p value) :numeric-nan)
        ((sb-ext:float-infinity-p value)
         (if (minusp value) :numeric-negative-infinity :numeric-positive-infinity))
        ((zerop value) (make-sql-decimal 0 0))
        (t
         (let* ((number (rational value))
                (absolute (abs number))
                (exponent (- (length (format nil "~d" (numerator absolute)))
                             (length (format nil "~d" (denominator absolute)))))
                (precision (if (eq type :float4) 6 15)))
           (when (< absolute (expt 10 exponent)) (decf exponent))
           (let* ((scale (- (1- precision) exponent))
                  (coefficient (round (* number (expt 10 scale)))))
             (when (minusp scale)
               (setf coefficient (* coefficient (expt 10 (- scale))) scale 0))
             (loop while (and (plusp scale) (zerop (rem coefficient 10)))
                   do (setf coefficient (truncate coefficient 10)) (decf scale))
             (make-sql-decimal coefficient scale))))))

(defun sql-float-from-number (value type)
  "Convert a finite integer, numeric, or float to the requested IEEE width."
  (let* ((one (if (eq type :float4) 1.0f0 1.0d0))
         (number (if (sql-decimal-p value) (sql-numeric-rational value) value))
         (result (handler-case
                     (sb-int:with-float-traps-masked (:overflow :underflow :inexact :invalid)
                       (float number one))
                   (arithmetic-error () (ereport "22003" "floating-point value out of range")))))
    (when (and (or (not (floatp number))
                   (not (or (sb-ext:float-infinity-p number) (sb-ext:float-nan-p number))))
               (or (sb-ext:float-infinity-p result)
                   (and (not (zerop number)) (zerop result))))
      (ereport "22003" "floating-point value out of range"))
    result))

(defun sql-float-hex-input (text type)
  "Read the hexadecimal significand and binary exponent accepted by strtod."
  (let* ((length (length text)) (position 0) (sign 1) (coefficient 0)
                                (fraction-digits 0) (digit-count 0) (point nil) (exponent 0))
    (labels ((peek () (when (< position length) (char text position)))
             (bad () (ereport "22P02" "invalid input syntax for type ~a: ~s" type text))
             (out-of-range ()
               (ereport "22003" "\"~a\" is out of range for type ~a"
                        (subseq text 0 position)
                        (if (eq type :float4) "real" "double precision"))))
      (when (and (peek) (find (peek) "+-"))
        (when (char= (peek) #\-) (setf sign -1)) (incf position))
      (unless (and (< (+ position 1) length) (char= (char text position) #\0)
                   (char-equal (char text (1+ position)) #\x)) (bad))
      (incf position 2)
      (loop for character = (peek)
            while character
            for digit = (digit-char-p character 16)
            do (cond (digit (setf coefficient (+ (* coefficient 16) digit))
                            (incf digit-count) (when point (incf fraction-digits)) (incf position))
                     ((and (char= character #\.) (not point)) (setf point t) (incf position))
                     (t (return))))
      (when (zerop digit-count) (bad))
      (when (and (peek) (find (peek) "pP"))
        (let ((exponent-start position))
          (incf position)
          (let ((exponent-sign 1) (count 0))
            (when (and (peek) (find (peek) "+-"))
              (when (char= (peek) #\-) (setf exponent-sign -1)) (incf position))
            (loop for digit = (and (peek) (digit-char-p (peek) 10)) while digit
                  do (setf exponent (min 1000000 (+ (* exponent 10) digit)))
                     (incf count) (incf position))
            ;; strtod leaves an incomplete exponent in the unconsumed suffix.
            (if (zerop count)
                (setf position exponent-start exponent 0)
                (setf exponent (* exponent-sign exponent))))))
      ;; strtod reports range failure before rejecting an unconsumed suffix.
      (decf exponent (* 4 fraction-digits))
      (when (and (not (zerop coefficient))
                 (not (<= -1200 (+ exponent (integer-length coefficient)) 1100)))
        (out-of-range))
      (let ((value (if (zerop coefficient) 0 (* sign coefficient (expt 2 exponent)))))
        (let ((result (handler-case (sql-float-from-number value type)
                        (pg-error (condition)
                          (if (string= (pg-error-sqlstate condition) "22003")
                              (out-of-range) (error condition))))))
          (unless (= position length) (bad))
          (if (and (zerop coefficient) (minusp sign)) (- result) result))))))

(defun sql-float-decimal-input (text type)
  "Read decimal float syntax without applying numeric's scale limits."
  (let ((length (length text)) (position 0) (sign 1) (coefficient 0)
                               (fraction-digits 0) (digit-count 0) (point nil) (exponent 0))
    (labels ((peek () (when (< position length) (char text position)))
             (bad () (ereport "22P02" "invalid input syntax for type ~a: ~s" type text))
             (out-of-range ()
               (ereport "22003" "\"~a\" is out of range for type ~a"
                        (subseq text 0 position)
                        (if (eq type :float4) "real" "double precision"))))
      (when (and (peek) (find (peek) "+-"))
        (when (char= (peek) #\-) (setf sign -1)) (incf position))
      (loop for character = (peek) while character
            for digit = (digit-char-p character 10)
            do (cond (digit (setf coefficient (+ (* coefficient 10) digit))
                            (incf digit-count) (when point (incf fraction-digits)) (incf position))
                     ((and (char= character #\.) (not point)) (setf point t) (incf position))
                     (t (return))))
      (when (zerop digit-count) (bad))
      (when (and (peek) (find (peek) "eE"))
        (let ((exponent-start position))
          (incf position)
          (let ((exponent-sign 1) (count 0))
            (when (and (peek) (find (peek) "+-"))
              (when (char= (peek) #\-) (setf exponent-sign -1)) (incf position))
            (loop for digit = (and (peek) (digit-char-p (peek) 10)) while digit
                  do (setf exponent (min 1000000 (+ (* exponent 10) digit)))
                     (incf count) (incf position))
            ;; strtod leaves an incomplete exponent in the unconsumed suffix.
            (if (zerop count)
                (setf position exponent-start exponent 0)
                (setf exponent (* exponent-sign exponent))))))
      ;; strtod reports range failure before rejecting an unconsumed suffix.
      (decf exponent fraction-digits)
      (when (and (not (zerop coefficient))
                 (not (<= -400 (+ exponent (length (format nil "~d" coefficient))) 400)))
        (out-of-range))
      (let* ((value (if (zerop coefficient) 0 (* sign coefficient (expt 10 exponent))))
             (result (handler-case (sql-float-from-number value type)
                       (pg-error (condition)
                         (if (string= (pg-error-sqlstate condition) "22003")
                             (out-of-range) (error condition))))))
        (unless (= position length) (bad))
        (if (and (zerop coefficient) (minusp sign)) (- result) result)))))

(defun sql-float-input (text type)
  "Read a PostgreSQL float value directly at TYPE's precision.
As in float4in_internal and float8in_internal, a range failure in the recognized
number precedes rejection of trailing junk and quotes only that number; syntax
errors quote the original input, including whitespace. Hex input follows the
behavior of the supported C peer's libc and is not portable across all hosts."
  (handler-case
      (let* ((input (string-trim (mapcar #'code-char '(9 10 11 12 13 32)) text))
             (lower (string-downcase input))
             (single-p (eq type :float4)))
        (cond ((member lower '("nan" "+nan" "-nan") :test #'string=)
               (if single-p (sb-kernel:make-single-float #x7fc00000)
                   (sb-kernel:make-double-float #x7ff80000 0)))
              ((member lower '("infinity" "+infinity" "inf" "+inf") :test #'string=)
               (if single-p sb-ext:single-float-positive-infinity sb-ext:double-float-positive-infinity))
              ((member lower '("-infinity" "-inf") :test #'string=)
               (if single-p sb-ext:single-float-negative-infinity sb-ext:double-float-negative-infinity))
              (t
               (let ((start (if (and (plusp (length lower)) (find (char lower 0) "+-")) 1 0)))
                 (when (and (<= (+ start 2) (length lower))
                            (string= lower "0x" :start1 start :end1 (+ start 2)))
                   (return-from sql-float-input (sql-float-hex-input input type))))
               (sql-float-decimal-input input type))))
    (pg-error (condition)
      ;; float4in/float8in preserve original invalid input, but range
      ;; errors quote only the number, without surrounding whitespace.
      (let ((name (if (eq type :float4) "real" "double precision")))
        (cond ((string= (pg-error-sqlstate condition) "22P02")
               (ereport "22P02" "invalid input syntax for type ~a: \"~a\"" name text))
              (t (error condition)))))))

(defun sql-float-significant-digits (value precision)
  "Return decimal point position and rounded significant digits, as %g does."
  (let* ((number (abs (rational value)))
         (exponent (- (length (format nil "~d" (numerator number)))
                      (length (format nil "~d" (denominator number)))))
         (precision (max 1 precision)))
    (when (< number (expt 10 exponent)) (decf exponent))
    (let* ((scale (- (1- precision) exponent))
           (rounded (round (* number (expt 10 scale)))))
      (when (>= rounded (expt 10 precision))
        (setf rounded (truncate rounded 10))
        (incf exponent))
      (loop while (zerop (rem rounded 10)) do (setf rounded (truncate rounded 10)))
      (values (1+ exponent) (format nil "~d" rounded)))))

(defun sql-float-output (value type &optional (extra-digits 1))
  "Format an IEEE value with PostgreSQL's default shortest output mode."
  (cond ((sb-ext:float-nan-p value) "NaN")
        ((sb-ext:float-infinity-p value) (if (minusp value) "-Infinity" "Infinity"))
        ((zerop value) (if (minusp (float-sign value)) "-0" "0"))
        (t
         (multiple-value-bind (position digits)
             (if (plusp extra-digits) (sb-impl::flonum-to-digits (abs value))
                 (sql-float-significant-digits value (+ extra-digits (if (eq type :float4) 6 15))))
           (let* ((exponent (1- position))
                  (scientific (or (< exponent -4)
                                  (>= exponent (if (plusp extra-digits)
                                                   (if (eq type :float4) 6 15)
                                                   (max 1 (+ extra-digits (if (eq type :float4) 6 15)))))))
                  (sign (if (minusp value) "-" ""))
                  (length (length digits)))
             (if scientific
                 (format nil "~a~a~ae~a~2,'0d" sign (subseq digits 0 1)
                         (if (= length 1) "" (concatenate 'string "." (subseq digits 1)))
                         (if (minusp exponent) "-" "+") (abs exponent))
                 (concatenate 'string sign
                              (cond ((<= position 0)
                                     (concatenate 'string "0." (make-string (- position) :initial-element #\0) digits))
                                    ((>= position length)
                                     (concatenate 'string digits (make-string (- position length) :initial-element #\0)))
                                    (t (concatenate 'string (subseq digits 0 position) "." (subseq digits position)))))))))))

(defun sql-float-compare (left right)
  "Use PostgreSQL's NaN ordering and equality for IEEE floats."
  (cond ((sb-ext:float-nan-p left) (if (sb-ext:float-nan-p right) 0 1))
        ((sb-ext:float-nan-p right) -1)
        ((< left right) -1) ((> left right) 1) (t 0)))

(sb-alien:define-alien-routine ("acos" sql-libm-acos) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("asin" sql-libm-asin) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("atan" sql-libm-atan) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("atan2" sql-libm-atan2) sb-alien:double (y sb-alien:double) (x sb-alien:double))

(sb-alien:define-alien-routine ("sin" sql-libm-sin) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("cos" sql-libm-cos) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("tan" sql-libm-tan) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("sinh" sql-libm-sinh) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("cosh" sql-libm-cosh) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("tanh" sql-libm-tanh) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("asinh" sql-libm-asinh) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("acosh" sql-libm-acosh) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("atanh" sql-libm-atanh) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("cbrt" sql-libm-cbrt) sb-alien:double (x sb-alien:double))

(sb-alien:define-alien-routine ("fmod" sql-libm-fmod) sb-alien:double (x sb-alien:double) (y sb-alien:double))

(defun sql-sind-q1 (x)
  (if (<= x 30d0)
      (/ (/ (sql-libm-sin (* x (/ pi 180d0)))
            (sql-libm-sin (* 30d0 (/ pi 180d0)))) 2d0)
      (- 1d0 (/ (/ (- 1d0 (sql-libm-cos (* (- 90d0 x) (/ pi 180d0))))
                   (- 1d0 (sql-libm-cos (* 60d0 (/ pi 180d0))))) 2d0))))

(defun sql-cosd-q1 (x)
  (if (<= x 60d0)
      (- 1d0 (/ (/ (- 1d0 (sql-libm-cos (* x (/ pi 180d0))))
                   (- 1d0 (sql-libm-cos (* 60d0 (/ pi 180d0))))) 2d0))
      (/ (/ (sql-libm-sin (* (- 90d0 x) (/ pi 180d0)))
            (sql-libm-sin (* 30d0 (/ pi 180d0)))) 2d0)))

(defun sql-asind-q1 (x)
  (if (<= x 0.5d0)
      (* (/ (sql-libm-asin x) (sql-libm-asin 0.5d0)) 30d0)
      (- 90d0 (* (/ (sql-libm-acos x) (sql-libm-acos 0.5d0)) 60d0))))

(defun sql-acosd-q1 (x)
  (if (<= x 0.5d0)
      (- 90d0 (* (/ (sql-libm-asin x) (sql-libm-asin 0.5d0)) 30d0))
      (* (/ (sql-libm-acos x) (sql-libm-acos 0.5d0)) 60d0)))

(defun sql-float-trig (name args)
  "Evaluate float.c's trigonometric and hyperbolic functions using libm.
ARGS contains float8 arguments in SQL order; callers handle NULLs.  NaN
propagates, invalid domains signal 22003, and poles can return infinity.
Degree functions reduce angles to the first quadrant and use the normalized
special-angle formulas from float.c, preserving its exact results at those angles."
  (let ((x (first args)) (y (second args)))
    (when (or (sb-ext:float-nan-p x) (and y (sb-ext:float-nan-p y)))
      (return-from sql-float-trig (sql-float-from-bits #x7ff8000000000000 :float8)))
    (when (and (member name '("asin" "acos" "asind" "acosd" "atanh") :test #'string=)
               (or (< x -1d0) (> x 1d0)))
      (ereport "22003" "input is out of range"))
    (when (and (string= name "acosh") (< x 1d0))
      (ereport "22003" "input is out of range"))
    (when (and (member name '("sin" "cos" "tan" "cot" "sind" "cosd" "tand" "cotd") :test #'string=)
               (sb-ext:float-infinity-p x))
      (ereport "22003" "input is out of range"))
    (sb-int:with-float-traps-masked (:invalid :inexact :underflow :overflow :divide-by-zero)
      (cond
        ((string= name "asin") (sql-libm-asin x))
        ((string= name "acos") (sql-libm-acos x))
        ((string= name "atan") (sql-libm-atan x))
        ((string= name "atan2") (sql-libm-atan2 x y))
        ((string= name "sin") (sql-libm-sin x))
        ((string= name "cos") (sql-libm-cos x))
        ((string= name "tan") (sql-libm-tan x))
        ((string= name "cot") (/ 1d0 (sql-libm-tan x)))
        ((string= name "sinh") (sql-libm-sinh x))
        ((string= name "cosh") (sql-libm-cosh x))
        ((string= name "tanh") (sql-libm-tanh x))
        ((string= name "asinh") (sql-libm-asinh x))
        ((string= name "acosh") (sql-libm-acosh x))
        ((string= name "atanh") (sql-libm-atanh x))
        ((member name '("cbrt" "dcbrt") :test #'string=) (sql-libm-cbrt x))
        ((string= name "degrees") (sql-float-arithmetic "/" x (/ pi 180d0)))
        ((string= name "radians") (sql-float-arithmetic "*" x (/ pi 180d0)))
        ((string= name "asind") (if (>= x 0d0) (sql-asind-q1 x) (- (sql-asind-q1 (- x)))))
        ((string= name "acosd") (if (>= x 0d0) (sql-acosd-q1 x) (+ 90d0 (sql-asind-q1 (- x)))))
        ((string= name "atand") (* (/ (sql-libm-atan x) (sql-libm-atan 1d0)) 45d0))
        ((string= name "atan2d") (* (/ (sql-libm-atan2 x y) (sql-libm-atan 1d0)) 45d0))
        (t
         (let ((angle (sql-libm-fmod x 360d0)) (sign 1d0))
           (when (< angle 0d0)
             (setf angle (- angle))
             (unless (string= name "cosd") (setf sign (- sign))))
           (when (> angle 180d0)
             (setf angle (- 360d0 angle))
             (unless (string= name "cosd") (setf sign (- sign))))
           (when (> angle 90d0)
             (setf angle (- 180d0 angle))
             (unless (string= name "sind") (setf sign (- sign))))
           (let* ((s (sql-sind-q1 angle))
                  (c (sql-cosd-q1 angle))
                  (result (* sign
                             (cond ((string= name "sind") s)
                                   ((string= name "cosd") c)
                                   ((string= name "tand")
                                    (/ (/ s c) (/ (sql-sind-q1 45d0) (sql-cosd-q1 45d0))))
                                   (t (/ (/ c s) (/ (sql-cosd-q1 45d0) (sql-sind-q1 45d0))))))))
             (if (and (member name '("tand" "cotd") :test #'string=) (zerop result))
                 0d0 result))))))))

(defun sql-float-sqrt (value)
  "Return the float8 square root, rejecting negative values as float.c does.
NaN and positive infinity pass through, and negative zero retains its sign."
  (when (and (not (sb-ext:float-nan-p value)) (minusp value))
    (ereport "2201F" "cannot take square root of a negative number"))
  (sb-int:with-float-traps-masked (:invalid :inexact :underflow :overflow)
    (sqrt value)))

(defun sql-float-exp (value)
  "Evaluate float8 exponential using float.c's special-value handling.
NaN and positive infinity pass through; negative infinity returns zero.  For
finite inputs, an infinite or zero result signals overflow or underflow."
  (when (sb-ext:float-nan-p value) (return-from sql-float-exp value))
  (when (sb-ext:float-infinity-p value)
    (return-from sql-float-exp (if (minusp value) 0d0 value)))
  (let ((result (sb-int:with-float-traps-masked (:overflow :underflow :inexact)
                  (exp value))))
    (cond ((sb-ext:float-infinity-p result) (ereport "22003" "value out of range: overflow"))
          ((zerop result) (ereport "22003" "value out of range: underflow"))
          (t result))))

(defun sql-float-power (base exponent)
  "Compute float8 power following float.c's dpow special and domain rules.
NaNs, infinities and signed zeros are handled explicitly; finite overflow and
underflow signal SQLSTATE 22003, and invalid power arguments signal 2201F."
  (when (sb-ext:float-nan-p base)
    (return-from sql-float-power (if (and (not (sb-ext:float-nan-p exponent))
                                          (zerop exponent)) 1d0 base)))
  (when (sb-ext:float-nan-p exponent)
    (return-from sql-float-power (if (= base 1d0) 1d0 exponent)))
  (when (and (zerop base) (minusp exponent))
    (ereport "2201F" "zero raised to a negative power is undefined"))
  (when (and (minusp base) (not (sb-ext:float-infinity-p exponent))
             (not (integerp (rational exponent))))
    (ereport "2201F" "a negative number raised to a non-integer power yields a complex result"))
  (let* ((inf (sql-float-from-bits #x7ff0000000000000 :float8))
         (negative-odd (and (minusp (float-sign base))
                            (not (sb-ext:float-infinity-p exponent))
                            (integerp (rational exponent))
                            (oddp (rational exponent)))))
    (when (sb-ext:float-infinity-p exponent)
      (return-from sql-float-power
        (cond ((= (abs base) 1d0) 1d0)
              ((eq (> (abs base) 1d0) (plusp exponent)) inf)
              (t 0d0))))
    (when (sb-ext:float-infinity-p base)
      (return-from sql-float-power
        (if (plusp exponent)
            (if negative-odd (- inf) inf)
            (if (zerop exponent) 1d0 (if negative-odd -0d0 0d0)))))
    (when (zerop base)
      (return-from sql-float-power
        (if (zerop exponent) 1d0 (if negative-odd -0d0 0d0))))
    (let ((result (sb-int:with-float-traps-masked (:invalid :overflow :underflow :inexact)
                    (expt (abs base) exponent))))
      (when (sb-ext:float-infinity-p result) (ereport "22003" "value out of range: overflow"))
      (when (zerop result) (ereport "22003" "value out of range: underflow"))
      (if negative-odd (- result) result))))

(defun sql-float-ln (value)
  "Evaluate the float8 natural logarithm with SQL domain errors."
  (when (sb-ext:float-nan-p value) (return-from sql-float-ln value))
  (when (zerop value) (ereport "2201E" "cannot take logarithm of zero"))
  (when (minusp value) (ereport "2201E" "cannot take logarithm of a negative number"))
  (sb-int:with-float-traps-masked (:inexact :underflow :overflow)
    (log value)))

(defun sql-float-log10 (value)
  "Evaluate the float8 base-ten logarithm with SQL domain errors."
  (when (sb-ext:float-nan-p value) (return-from sql-float-log10 value))
  (when (zerop value) (ereport "2201E" "cannot take logarithm of zero"))
  (when (minusp value) (ereport "2201E" "cannot take logarithm of a negative number"))
  (sb-int:with-float-traps-masked (:inexact :underflow :overflow)
    (sb-kernel:%log10 value)))

(defun sql-float-basic-function (name value)
  "Apply float.c's unary rounding and sign functions.
Round uses ties to even, and zero results retain the input's sign.  Like
float.c, sign returns zero for NaN; abs clears the sign bit even for NaN."
  (when (string= name "abs")
    (let* ((type (if (typep value 'single-float) :float4 :float8))
           (sign-bit (if (eq type :float4) 31 63)))
      (return-from sql-float-basic-function
        (sql-float-from-bits (logandc2 (sql-float-bits value type) (ash 1 sign-bit)) type))))
  (when (string= name "sign")
    (return-from sql-float-basic-function
      (cond ((sb-ext:float-nan-p value) 0d0)
            ((plusp value) 1d0)
            ((minusp value) -1d0)
            (t 0d0))))
  (when (or (sb-ext:float-nan-p value) (sb-ext:float-infinity-p value) (zerop value))
    (return-from sql-float-basic-function value))
  (let ((result (cond ((string= name "round") (round value))
                      ((string= name "trunc") (truncate value))
                      ((member name '("ceil" "ceiling") :test #'string=) (ceiling value))
                      ((string= name "floor") (floor value))
                      (t (ereport "XX000" "unknown float function ~a" name)))))
    (if (zerop result)
        (if (minusp value) -0d0 0d0)
        (coerce result 'double-float))))

(defun sql-float-arithmetic (operator left right)
  "Apply an IEEE operation and PostgreSQL's overflow and underflow checks."
  (when (and (string= operator "/") (not (sb-ext:float-nan-p right))
             (zerop right) (not (sb-ext:float-nan-p left)))
    (ereport "22012" "division by zero"))
  (let* ((left-special (or (sb-ext:float-nan-p left) (sb-ext:float-infinity-p left)))
         (right-special (or (sb-ext:float-nan-p right) (sb-ext:float-infinity-p right)))
         (result (sb-int:with-float-traps-masked (:overflow :underflow :invalid :divide-by-zero :inexact)
                   (cond ((string= operator "+") (+ left right))
                         ((string= operator "-") (- left right))
                         ((string= operator "*") (* left right))
                         ((string= operator "/") (/ left right))
                         (t (ereport "42883" "unsupported float operator ~a" operator))))))
    (when (and (sb-ext:float-infinity-p result) (not left-special) (not right-special))
      (ereport "22003" "value out of range: overflow"))
    (when (and (not (sb-ext:float-nan-p result)) (zerop result)
               (not left-special) (not (zerop left))
               (or (and (string= operator "*") (not right-special) (not (zerop right)))
                   (and (string= operator "/") (not right-special))))
      (ereport "22003" "value out of range: underflow"))
    result))
