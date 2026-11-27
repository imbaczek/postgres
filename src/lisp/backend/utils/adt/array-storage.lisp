;;;; array-storage.lisp -- PostgreSQL ArrayType payloads for heap and logical rows.

(in-package #:pg)

(defconstant +max-varlena-array-elements+ (floor +max-query-message-size+ 8)
  "Limit the resident element vector for these arrays to the query frame budget.")

(defvar *sql-varlena-array-element-reserver* nil
  "Optional aggregate element-budget callback, called before array allocations.")

(defun sql-varlena-array-element-octets (value type)
  (ecase type
    (:bytea (check-type value (vector (unsigned-byte 8))) value)
    (:json (sql-json-input value) (string->bytes value))
    ((:text :varchar) (string->bytes value))
    (:jsonb (sql-jsonb-to-octets value))))

(defun sql-array-write-fixed-element (value type bytes offset)
  (ecase type
    (:bool (setf (aref bytes offset) (if value 1 0)))
    (:int2 (page-set-u16 bytes offset (ldb (byte 16 0) value)))
    (:int4 (page-set-u32 bytes offset (ldb (byte 32 0) value)))
    (:int8 (page-set-u64 bytes offset (ldb (byte 64 0) value)))
    (:uuid (sql-uuid-copy-octets value bytes offset))))

(defun sql-array-read-fixed-element (bytes offset type)
  (ecase type
    (:bool (not (zerop (aref bytes offset))))
    (:int2 (- (logxor (page-u16 bytes offset) #x8000) #x8000))
    (:int4 (heap-read-int4 bytes offset))
    (:int8 (- (logxor (page-u64 bytes offset) #x8000000000000000) #x8000000000000000))
    (:uuid (sql-uuid-from-octets bytes offset))))

(defun sql-varlena-array-encode (array element-type)
  "Encode an ArrayType payload, excluding its outer four-byte varlena header."
  (unless (and (sql-array-p array) (eq (sql-array-element-type-id array) element-type))
    (error "invalid array element type"))
  (let* ((dimensions (sql-array-dimensions array))
         (bounds (sql-array-lower-bounds array))
         (items (sql-array-elements array))
         (ndim (length dimensions))
         (count (length items))
         (fixed-width (sql-array-fixed-element-width element-type))
         (has-null nil)
         (data-start 0)
         (cursor 0)
         (encoded nil))
    (when (> count +max-varlena-array-elements+)
      (ereport "54000" "array element count exceeds supported maximum"))
    (unless (and (<= 0 ndim 6) (= ndim (length bounds))
                 (if (zerop ndim) (zerop count)
                     (and (or (every (lambda (dimension) (typep dimension '(integer 1 #x7fffffff))) dimensions)
                              (and (eq element-type :int2) (equal dimensions '(0)) (equal bounds '(0))))
                          (= count (reduce #'* dimensions :initial-value 1))))
                 (loop for lower in bounds for dimension in dimensions
                       always (and (typep lower '(signed-byte 32))
                                   (<= (+ lower dimension -1) #x7fffffff))))
      (error "invalid array dimensions"))
    (when *sql-varlena-array-element-reserver*
      (funcall *sql-varlena-array-element-reserver* count))
    (setf has-null (loop for item across items for index from 0
                         do (when (zerop (mod index 4096)) (check-for-interrupts))
                           thereis (eq item +null+))
          data-start (maxalign (+ 16 (* 8 ndim) (if has-null (ceiling count 8) 0)))
          cursor (- data-start 4)
          encoded (unless fixed-width (make-array count :initial-element nil)))
    (loop for item across items for index from 0
          do (when (zerop (mod index 4096)) (check-for-interrupts))
          unless (eq item +null+) do
            (if fixed-width
                (incf cursor fixed-width)
                (let ((bytes (sql-varlena-array-element-octets item element-type)))
                  (setf (aref encoded index) bytes)
                  (incf cursor (align-up (+ 4 (length bytes)) 4))))
            (when (> (+ cursor 4) #x3fffffff)
              (ereport "54000" "array value exceeds maximum size")))
    (let ((result (make-array cursor :element-type '(unsigned-byte 8) :initial-element 0)))
      (page-set-u32 result 0 ndim)
      (page-set-u32 result 4 (if has-null data-start 0))
      (page-set-u32 result 8 (sql-varlena-array-element-oid element-type))
      (loop for dimension in dimensions for i from 0
            do (page-set-u32 result (+ 12 (* i 4)) dimension))
      (loop for lower in bounds for i from 0
            do (page-set-u32 result (+ 12 (* ndim 4) (* i 4)) (logand lower #xffffffff)))
      (let ((position (- data-start 4)) (bitmap (+ 12 (* 8 ndim))))
        (loop for item across items for index from 0
              do (when (zerop (mod index 4096)) (check-for-interrupts))
              unless (eq item +null+) do
                (when has-null
                  (setf (aref result (+ bitmap (floor index 8)))
                        (logior (aref result (+ bitmap (floor index 8))) (ash 1 (mod index 8)))))
                (if fixed-width
                    (progn
                      (sql-array-write-fixed-element item element-type result position)
                      (incf position fixed-width))
                    (let ((bytes (aref encoded index)))
                      (setf position (align-up position 4))
                      (page-set-u32 result position (ash (+ 4 (length bytes)) 2))
                      (replace result bytes :start1 (+ position 4))
                      (incf position (align-up (+ 4 (length bytes)) 4))))))
      result)))

(defun sql-array-read-varlena-element (bytes position limit type)
  "Read one aligned array datum and return the next physical offset."
  (flet ((bad () (ereport "XX001" "invalid stored array")))
    (when (>= position limit) (bad))
    (let* ((first (aref bytes position))
           (short-p (logbitp 0 first))
           (header-size (if short-p 1 4))
           (size (if short-p (ash first -1)
                     (progn
                       (when (> (+ position 4) limit) (bad))
                       (let ((header (page-u32 bytes position)))
                         (unless (zerop (logand header 3)) (bad))
                         (ash header -2)))))
           (next (+ position (align-up size 4))))
      (when (or (< size header-size) (= first 1) (> next limit))
        (bad))
      (let ((payload (subseq bytes (+ position header-size) (+ position size))))
        (values
         (ecase type
           (:bytea payload)
           (:jsonb (sql-jsonb-from-octets payload))
           ((:text :varchar :json)
            (handler-case
                (let ((text (bytes->string payload)))
                  (when (eq type :json) (sql-json-input text))
                  text)
              (sb-int:character-decoding-error () (bad))
              (pg-error (condition)
                (let ((state (pg-error-sqlstate condition)))
                  (if (or (and (>= (length state) 2) (string= "22" state :end2 2))
                          (string= "54001" state))
                      (bad)
                      (error condition)))))))
         next)))))

(defun sql-varlena-array-decode (bytes element-type)
  "Decode a PostgreSQL ArrayType payload, validating all bounds and elements."
  (labels ((bad () (ereport "XX001" "invalid stored array")))
    (unless (and (typep bytes '(vector (unsigned-byte 8))) (>= (length bytes) 12))
      (bad))
    (let* ((length (length bytes))
           (ndim (page-u32 bytes 0))
           (offset (page-u32 bytes 4))
           (oid (page-u32 bytes 8))
           (fixed-width (sql-array-fixed-element-width element-type)))
      (unless (and (<= ndim 6) (= oid (sql-varlena-array-element-oid element-type))
                   (<= (+ 12 (* ndim 8)) length))
        (bad))
      (let ((dimensions nil) (bounds nil) (count (if (zerop ndim) 0 1)))
        (dotimes (i ndim)
          (let* ((dimension (page-u32 bytes (+ 12 (* i 4))))
                 (raw-bound (page-u32 bytes (+ 12 (* ndim 4) (* i 4))))
                 (lower (if (logbitp 31 raw-bound) (- raw-bound #x100000000) raw-bound)))
            (unless (and (or (<= 1 dimension #x7fffffff)
                             (and (eq element-type :int2) (= ndim 1) (zerop dimension) (zerop lower)))
                         (<= (+ lower dimension -1) #x7fffffff))
              (bad))
            (setf count (* count dimension))
            (push dimension dimensions)
            (push lower bounds)))
        (let* ((has-null (plusp offset))
               (header (+ 16 (* ndim 8)))
               (minimum (maxalign (+ header (if has-null (ceiling count 8) 0))))
               (start (if has-null offset (maxalign header)))
               (bitmap (+ 12 (* ndim 8))))
          (unless (and (>= start minimum) (zerop (mod start 8)) (<= start (+ length 4)))
            (bad))
          (when (and (not has-null) (> count (floor (- length (- start 4)) (or fixed-width 4))))
            (bad))
          (when (> count +max-varlena-array-elements+)
            (ereport "54000" "array element count exceeds supported maximum"))
          ;; Reject impossible counts before allocating the element vector.
          (let ((non-null-count
                  (if has-null
                      (loop for index below (ceiling count 8)
                            for remaining = (- count (* index 8))
                            for byte = (aref bytes (+ bitmap index))
                            do (when (zerop (logand index 4095))
                                 (check-for-interrupts))
                            sum (logcount (if (< remaining 8)
                                              (logand byte (1- (ash 1 remaining))) byte)))
                      count)))
            (let ((available (- length (- start 4))))
              (unless (if fixed-width
                          (= available (* non-null-count fixed-width))
                          (>= available (* non-null-count 4)))
                (bad))))
          (when *sql-varlena-array-element-reserver*
            (funcall *sql-varlena-array-element-reserver* count))
          (let ((values (make-array count :initial-element +null+))
                (position (- start 4)))
            (dotimes (i count)
              (when (zerop (logand i 4095)) (check-for-interrupts))
              (when (or (not has-null)
                        (logbitp (mod i 8) (aref bytes (+ bitmap (floor i 8)))))
                (multiple-value-bind (value next)
                    (if fixed-width
                        (values (sql-array-read-fixed-element bytes position element-type)
                                (+ position fixed-width))
                        (sql-array-read-varlena-element bytes position length element-type))
                  (setf (aref values i) value position next))))
            (unless (= position length) (bad))
            (make-sql-array :element-type-id element-type :elements values
                            :dimensions (nreverse dimensions) :lower-bounds (nreverse bounds))))))))
