;;;; uuid.lisp -- SQL UUID values and their PostgreSQL input/output forms.

(in-package #:pg)

(defconstant +sql-uuid-size+ 16)

(defstruct (sql-uuid (:constructor %make-sql-uuid (bytes)) (:copier nil))
  "A UUID owns its octets. Keep the backing vector private and copy across buffers."
  (bytes (error "UUID octets required")
   :type (simple-array (unsigned-byte 8) (16)) :read-only t))

(defun sql-uuid-from-octets (source &optional (start 0))
  "Read one UUID from SOURCE without retaining the input buffer."
  (unless (<= 0 start (+ start +sql-uuid-size+) (length source))
    (error "invalid UUID byte range"))
  (%make-sql-uuid
   (if (typep source '(simple-array (unsigned-byte 8) (*)))
       (subseq source start (+ start +sql-uuid-size+))
       (let ((bytes (make-array +sql-uuid-size+ :element-type '(unsigned-byte 8))))
         (replace bytes source :start2 start :end2 (+ start +sql-uuid-size+))
         bytes))))

(defun sql-uuid-copy-octets (value target &optional (start 0))
  "Copy VALUE to writable octets in TARGET at START."
  (unless (<= 0 start (+ start +sql-uuid-size+) (length target))
    (error "invalid UUID byte range"))
  (replace target (sql-uuid-bytes value) :start1 start))

(defun sql-uuid-from-bytea (value)
  (unless (= (length value) +sql-uuid-size+)
    (ereport "22P03" "invalid input length for type uuid"))
  (sql-uuid-from-octets value))

(defun sql-uuid-to-bytea (value)
  (copy-seq (sql-uuid-bytes value)))

(defun sql-uuid-hex-value (character)
  (cond ((char<= #\0 character #\9) (- (char-code character) (char-code #\0)))
        ((char<= #\a character #\f) (+ 10 (- (char-code character) (char-code #\a))))
        ((char<= #\A character #\F) (+ 10 (- (char-code character) (char-code #\A))))))

(defun sql-uuid-input (source)
  "Parse 32 ASCII hex digits with optional braces and four-digit separators."
  (let* ((length (length source))
         (braced (and (plusp length) (char= (char source 0) #\{)))
         (cursor (if braced 1 0))
         (bytes (make-array +sql-uuid-size+ :element-type '(unsigned-byte 8))))
    (flet ((invalid () (ereport "22P02" "invalid input syntax for type uuid: ~s" source)))
      (dotimes (i +sql-uuid-size+)
        (unless (<= (+ cursor 2) length) (invalid))
        (let ((high (sql-uuid-hex-value (char source cursor)))
              (low (sql-uuid-hex-value (char source (1+ cursor)))))
          (unless (and high low) (invalid))
          (setf (aref bytes i) (+ (ash high 4) low)))
        (incf cursor 2)
        (when (and (oddp i) (< i (1- +sql-uuid-size+)) (< cursor length)
                   (char= (char source cursor) #\-))
          (incf cursor)))
      (when braced
        (unless (and (< cursor length) (char= (char source cursor) #\})) (invalid))
        (incf cursor))
      (unless (= cursor length) (invalid)))
    (%make-sql-uuid bytes)))

(defun sql-uuid-output (value)
  "Return the canonical lowercase 8-4-4-4-12 spelling."
  (let ((output (make-string 36))
        (cursor 0)
        (hex "0123456789abcdef"))
    (loop for octet across (sql-uuid-bytes value)
          for i from 0
          do (when (member i '(4 6 8 10))
               (setf (char output cursor) #\-)
               (incf cursor))
             (setf (char output cursor) (char hex (ash octet -4))
                   (char output (1+ cursor)) (char hex (logand octet 15)))
             (incf cursor 2))
    output))

(defun sql-uuid-compare (left right)
  "Compare UUIDs in unsigned octet order."
  (declare (optimize (speed 3) (safety 1)))
  (let* ((a (sql-uuid-bytes left)) (b (sql-uuid-bytes right))
                                   (difference (- (aref a 0) (aref b 0))))
    (unless (zerop difference)
      (return-from sql-uuid-compare (if (minusp difference) -1 1)))
    (let ((start #-(and sbcl 64-bit) 1
                 #+(and sbcl 64-bit)
                 ;; Word equality skips shared octets independently of endianness.
                 (cond ((/= (sb-kernel:%vector-raw-bits a 0)
                            (sb-kernel:%vector-raw-bits b 0)) 1)
                       ((/= (sb-kernel:%vector-raw-bits a 1)
                            (sb-kernel:%vector-raw-bits b 1)) 8)
                       (t (return-from sql-uuid-compare 0)))))
      (loop for i of-type fixnum from start below 16
            for diff = (- (aref a i) (aref b i))
            unless (zerop diff) return (if (minusp diff) -1 1)
              finally (return 0)))))

(defun sql-uuid-hash-state (value seed)
  "Return the high and low words of hash_bytes_extended for a 16-byte UUID.
PostgreSQL exposes the hash values to SQL, so a Lisp hash is insufficient."
  (let* ((bytes (sql-uuid-bytes value))
         (a (+ #x9e3779b9 +sql-uuid-size+ 3923095))
         (b a)
         (c a))
    (flet ((rotate (word count)
             (logior (ldb (byte 32 0) (ash word count)) (ash word (- count 32))))
           (word-at (offset)
             #+big-endian
             (loop for index from offset below (+ offset 4)
                   for word = (aref bytes index) then (logior (ash word 8) (aref bytes index))
                   finally (return word))
             #-big-endian
             (page-u32 bytes offset)))
      (macrolet ((add (place word) `(setf ,place (ldb (byte 32 0) (+ ,place ,word))))
                 (mix-step (target origin next count)
                   `(progn
                      (setf ,target (logxor (ldb (byte 32 0) (- ,target ,origin))
                                            (rotate ,origin ,count)))
                      (add ,origin ,next)))
                 (mix ()
                   '(progn (mix-step a c b 4) (mix-step b a c 6) (mix-step c b a 8)
                     (mix-step a c b 16) (mix-step b a c 19) (mix-step c b a 4)))
                 (final-step (target origin count)
                   `(setf ,target (ldb (byte 32 0) (- (logxor ,target ,origin)
                                                      (rotate ,origin ,count))))))
        (unless (zerop seed)
          (add a (ldb (byte 32 32) seed))
          (add b (ldb (byte 32 0) seed))
          (mix))
        (add a (word-at 0))
        (add b (word-at 4))
        (add c (word-at 8))
        (mix)
        (add a (word-at 12))
        (final-step c b 14)
        (final-step a c 11)
        (final-step b a 25)
        (final-step c b 16)
        (final-step a c 4)
        (final-step b a 14)
        (final-step c b 24)
        (values b c)))))

(defun sql-uuid-hash (value)
  (let ((word (nth-value 1 (sql-uuid-hash-state value 0))))
    (- (logxor word #x80000000) #x80000000)))

(defun sql-uuid-hash-extended (value seed)
  (multiple-value-bind (high low) (sql-uuid-hash-state value seed)
    (let ((word (logior (ash high 32) low)))
      (- (logxor word #x8000000000000000) #x8000000000000000))))
