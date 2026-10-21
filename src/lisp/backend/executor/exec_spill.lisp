;;;; exec_spill.lisp -- bounded-memory executor spill files and row stores.

(in-package #:pg)

(defparameter *query-work-memory-bytes* (* 4 1024 1024)
  "Approximate memory budget shared by one executor spool or sort operation.")

(defvar *query-temp-directory* nil)
(defvar *query-temp-files* nil)
(defvar *query-temp-file-scope-active-p* nil)
(defvar *query-temp-file-sequence* 0)
(defvar *query-temp-file-lock*
  (sb-thread:make-mutex :name "pg query temporary file names"))
(defparameter +query-spill-eof+ (gensym "QUERY-SPILL-EOF-"))

(defun unlink-query-temp-file (pathname)
  (delete-file pathname))

(defmacro with-query-temp-files (&body body)
  "Delete every query-local spill file, including files from failed execution."
  `(let ((*query-temp-files* nil)
         (*query-temp-file-scope-active-p* t))
     (unwind-protect
          (progn ,@body)
       (dolist (pathname *query-temp-files*)
         (ignore-errors (unlink-query-temp-file pathname))))))

(defun query-temp-file-pathname ()
  (let* ((sequence
           (sb-thread:with-mutex (*query-temp-file-lock*)
             (incf *query-temp-file-sequence*)))
         (directory
           (uiop:ensure-directory-pathname
            (pathname (or *query-temp-directory*
                          (uiop:temporary-directory)))))
         (name (format nil "pg-query-~36r-~36r-~36r.tmp"
                       (get-universal-time) (sb-posix:getpid) sequence)))
    (merge-pathnames name directory)))

(defun register-query-temp-file (pathname)
  (unless *query-temp-file-scope-active-p*
    (error "Query spill files must be created inside WITH-QUERY-TEMP-FILES"))
  (push pathname *query-temp-files*)
  pathname)

(defun open-and-register-query-temp-file (pathname open-arguments)
  "Open PATHNAME and publish it to scope cleanup as one interrupt-safe step."
  (let ((stream nil)
        (registered-p nil))
    (unwind-protect
         (sb-sys:without-interrupts
           (setf stream (apply #'open pathname open-arguments))
           (when stream
             (register-query-temp-file pathname)
             (setf registered-p t))
           stream)
      (when (and stream (not registered-p))
        (ignore-errors (close stream :abort t))
        (ignore-errors (delete-file pathname))))))

(defun open-query-temp-output ()
  (unless *query-temp-file-scope-active-p*
    (error "Query spill files must be created inside WITH-QUERY-TEMP-FILES"))
  (loop repeat 100
        for pathname = (query-temp-file-pathname)
        for stream = (open-and-register-query-temp-file
                      pathname
                      '(:direction :output :if-exists nil
                        :if-does-not-exist :create :external-format :utf-8))
        when stream
          do (return-from open-query-temp-output (values pathname stream)))
  (error "Could not create a unique query spill file"))

(defun open-query-temp-io ()
  (unless *query-temp-file-scope-active-p*
    (error "Query spill files must be created inside WITH-QUERY-TEMP-FILES"))
  (loop repeat 100
        for pathname = (query-temp-file-pathname)
        for stream = (open-and-register-query-temp-file
                      pathname
                      '(:direction :io :element-type (unsigned-byte 8)
                        :if-exists nil :if-does-not-exist :create))
        when stream
          do (return-from open-query-temp-io (values pathname stream)))
  (error "Could not create a unique query spill file"))

(defun query-spill-value-size (value)
  "Estimate retained row memory without building its printed representation."
  (cond
    ((stringp value)
     (+ 40 (* 4 (length value))))
    ((and (vectorp value) (typep value '(vector (unsigned-byte 8))))
     (+ 64 (length value)))
    ((vectorp value)
     (+ 64 (loop for element across value
                 sum (query-spill-value-size element))))
    ((consp value)
     (+ 48 (query-spill-value-size (car value))
        (query-spill-value-size (cdr value))))
    ((symbolp value)
     (+ 32 (* 2 (length (symbol-name value)))))
    ((integerp value)
     (+ 16 (ceiling (integer-length value) 8)))
    ((rationalp value)
     (+ 32 (query-spill-value-size (numerator value))
        (query-spill-value-size (denominator value))))
    ((floatp value) 32)
    ((complexp value)
     (+ 32 (query-spill-value-size (realpart value))
        (query-spill-value-size (imagpart value))))
    ((characterp value) 8)
    (t 64)))

(defun query-spill-decode (value)
  (cond
    ((and (consp value) (eq (first value) :pg-query-spill-vector))
     (let* ((element-type (second value))
            (elements (third value)))
       (if (equal (array-element-type elements) element-type)
           (progn
             (dotimes (index (length elements))
               (setf (aref elements index)
                     (query-spill-decode (aref elements index))))
             elements)
           (make-array (length elements)
                       :element-type element-type
                       :initial-contents
                       (map 'vector #'query-spill-decode elements)))))
    ((consp value)
     (cons (query-spill-decode (car value))
           (query-spill-decode (cdr value))))
    (t value)))

(defun query-spill-record-size (value)
  (+ 64 (query-spill-value-size value)))

(defun query-spill-write-value (stream value)
  (cond
    ((stringp value)
     (write value :stream stream))
    ((vectorp value)
     (write-string "(:pg-query-spill-vector " stream)
     (write (array-element-type value) :stream stream)
     (write-char #\Space stream)
     (if (typep value '(vector (unsigned-byte 8)))
         (progn
           (write-string "#U8(" stream)
           (loop with digits = "0123456789ABCDEF"
                 for byte across value
                 do (write-char (char digits (ash byte -4)) stream)
                    (write-char (char digits (logand byte #x0f)) stream))
           (write-char #\) stream))
         (progn
           (write-string "#(" stream)
           (loop for element across value
                 for first-p = t then nil
                 do (unless first-p (write-char #\Space stream))
                    (query-spill-write-value stream element))
           (write-char #\) stream)))
     (write-char #\) stream))
    ((consp value)
     (write-char #\( stream)
     (loop for tail = value then (cdr tail)
           for first-p = t then nil
           do (unless first-p (write-char #\Space stream))
              (query-spill-write-value stream (car tail))
           when (not (consp (cdr tail)))
             do (unless (null (cdr tail))
                  (write-string " . " stream)
                  (query-spill-write-value stream (cdr tail)))
                (return))
     (write-char #\) stream))
    (t
     (write value :stream stream))))

(defun query-spill-hex-digit (character)
  (position (char-upcase character) "0123456789ABCDEF"))

(defun read-query-spill-byte-vector (stream subchar argument)
  (declare (ignore subchar argument))
  (unless (char= (read-char stream) #\8)
    (error "Malformed query spill byte vector"))
  (unless (char= (read-char stream) #\()
    (error "Malformed query spill byte vector"))
  (let ((bytes (make-array 0 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
        (high-nibble nil))
    (loop for character = (read-char stream nil nil)
          do (cond
               ((null character)
                (error "Unterminated query spill byte vector"))
               ((char= character #\))
                (when high-nibble
                  (error "Odd number of hex digits in query spill byte vector"))
                (return))
               ((find character '(#\Space #\Tab #\Newline #\Return)))
               ((null high-nibble)
                (setf high-nibble (query-spill-hex-digit character))
                (unless high-nibble
                  (error "Invalid hex digit in query spill byte vector")))
               (t
                (let ((low-nibble (query-spill-hex-digit character)))
                  (unless low-nibble
                    (error "Invalid hex digit in query spill byte vector"))
                  (vector-push-extend (+ (ash high-nibble 4) low-nibble) bytes)
                  (setf high-nibble nil)))))
    bytes))

(defparameter *query-spill-readtable*
  (let ((readtable (copy-readtable nil)))
    (set-dispatch-macro-character #\# #\u
                                  #'read-query-spill-byte-vector readtable)
    (set-dispatch-macro-character #\# #\U
                                  #'read-query-spill-byte-vector readtable)
    readtable))

(defun query-spill-write-record (stream value)
  (let ((*print-pretty* nil)
        (*print-readably* t)
        (*print-circle* t)
        (*print-length* nil)
        (*print-level* nil)
        (*print-base* 10)
        (*print-radix* nil)
        (*package* (find-package '#:pg)))
    (query-spill-write-value stream value)
    (terpri stream)))

(defun query-spill-read-record (stream)
  (let ((*read-eval* nil)
        (*readtable* *query-spill-readtable*)
        (*package* (find-package '#:pg)))
    (let ((value (read stream nil +query-spill-eof+)))
      (if (eq value +query-spill-eof+)
          +query-spill-eof+
          (query-spill-decode value)))))

(defstruct (query-row-spool (:constructor %make-query-row-spool))
  (rows nil)
  (count 0)
  (memory-bytes 0)
  (memory-limit *query-work-memory-bytes*)
  pathname
  output-stream
  sealed-p)

(defun make-query-row-spool (&optional (memory-limit *query-work-memory-bytes*))
  (%make-query-row-spool :memory-limit (max 1 memory-limit)))

(defun query-row-spool-spill (spool)
  (unless (query-row-spool-output-stream spool)
    (multiple-value-bind (pathname stream) (open-query-temp-output)
      (setf (query-row-spool-pathname spool) pathname
            (query-row-spool-output-stream spool) stream)
      (dolist (row (nreverse (query-row-spool-rows spool)))
        (query-spill-write-record stream row))
      (setf (query-row-spool-rows spool) nil
            (query-row-spool-memory-bytes spool) 0)))
  spool)

(defun query-row-spool-append (spool row)
  (when (query-row-spool-sealed-p spool)
    (error "Cannot append to a sealed query row spool"))
  (let ((row-bytes (query-spill-record-size row)))
    (when (and (null (query-row-spool-output-stream spool))
               (> (+ row-bytes (query-row-spool-memory-bytes spool))
                  (query-row-spool-memory-limit spool)))
      (query-row-spool-spill spool))
    (if (query-row-spool-output-stream spool)
        (query-spill-write-record (query-row-spool-output-stream spool) row)
        (progn
          (push row (query-row-spool-rows spool))
          (incf (query-row-spool-memory-bytes spool) row-bytes)))
    (prog1 (query-row-spool-count spool)
      (incf (query-row-spool-count spool)))))

(defun query-row-spool-seal (spool)
  (unless (query-row-spool-sealed-p spool)
    (when (query-row-spool-output-stream spool)
      (close (query-row-spool-output-stream spool))
      (setf (query-row-spool-output-stream spool) nil))
    (setf (query-row-spool-sealed-p spool) t))
  spool)

(defun close-query-row-spool (spool)
  (when (query-row-spool-output-stream spool)
    (ignore-errors (close (query-row-spool-output-stream spool)))
    (setf (query-row-spool-output-stream spool) nil))
  (values))

(defmacro with-query-row-spool ((spool &optional memory-limit) &body body)
  `(let ((,spool (make-query-row-spool ,memory-limit)))
     (unwind-protect
          (progn ,@body)
       (close-query-row-spool ,spool))))

(defun map-query-row-spool (spool callback)
  (unless (query-row-spool-sealed-p spool)
    (error "Cannot read an unsealed query row spool"))
  (if (query-row-spool-pathname spool)
      (with-open-file (stream (query-row-spool-pathname spool)
                              :direction :input :external-format :utf-8)
        (loop for index from 0
              for row = (query-spill-read-record stream)
              until (eq row +query-spill-eof+)
              do (when (eq (funcall callback row index) :stop)
                   (return-from map-query-row-spool :stop)))
        (values))
      (loop for row in (reverse (query-row-spool-rows spool))
            for index from 0
            do (when (eq (funcall callback row index) :stop)
                 (return-from map-query-row-spool :stop))
            finally (return (values)))))

(defstruct (query-match-map (:constructor %make-query-match-map))
  (bytes (make-array 0 :element-type '(unsigned-byte 8)
                       :adjustable t :fill-pointer 0))
  (count 0)
  (memory-limit 1)
  pathname
  io-stream
  (finished-p nil))

(defun make-query-match-map (&optional (memory-limit *query-work-memory-bytes*))
  (%make-query-match-map :memory-limit (max 1 memory-limit)))

(defun query-match-map-spill (match-map)
  (unless (query-match-map-io-stream match-map)
    (multiple-value-bind (pathname stream) (open-query-temp-io)
      (setf (query-match-map-pathname match-map) pathname
            (query-match-map-io-stream match-map) stream)
      (loop for byte across (query-match-map-bytes match-map)
            do (write-byte byte stream))
      (setf (query-match-map-bytes match-map)
            (make-array 0 :element-type '(unsigned-byte 8)
                          :adjustable t :fill-pointer 0))))
  match-map)

(defun query-match-map-append (match-map)
  (when (query-match-map-finished-p match-map)
    (error "Cannot append to a finished query match map"))
  (when (and (null (query-match-map-io-stream match-map))
             (>= (fill-pointer (query-match-map-bytes match-map))
                 (query-match-map-memory-limit match-map)))
    (query-match-map-spill match-map))
  (if (query-match-map-io-stream match-map)
      (progn
        (file-position (query-match-map-io-stream match-map)
                       (query-match-map-count match-map))
        (write-byte 0 (query-match-map-io-stream match-map)))
      (vector-push-extend 0 (query-match-map-bytes match-map)))
  (prog1 (query-match-map-count match-map)
    (incf (query-match-map-count match-map))))

(defun query-match-map-mark (match-map index)
  (unless (<= 0 index (1- (query-match-map-count match-map)))
    (error "Query match-map index is out of bounds: ~a" index))
  (if (query-match-map-io-stream match-map)
      (progn
        (file-position (query-match-map-io-stream match-map) index)
        (write-byte 1 (query-match-map-io-stream match-map)))
      (setf (aref (query-match-map-bytes match-map) index) 1))
  (values))

(defun query-match-map-finish (match-map)
  (when (query-match-map-io-stream match-map)
    (finish-output (query-match-map-io-stream match-map)))
  (setf (query-match-map-finished-p match-map) t)
  match-map)

(defun query-match-map-matched-p (match-map index)
  (unless (query-match-map-finished-p match-map)
    (error "Cannot read an unfinished query match map"))
  (unless (<= 0 index (1- (query-match-map-count match-map)))
    (error "Query match-map index is out of bounds: ~a" index))
  (= (if (query-match-map-io-stream match-map)
         (progn
           (file-position (query-match-map-io-stream match-map) index)
           (read-byte (query-match-map-io-stream match-map)))
         (aref (query-match-map-bytes match-map) index))
     1))

(defun close-query-match-map (match-map)
  (when (query-match-map-io-stream match-map)
    (ignore-errors (close (query-match-map-io-stream match-map)))
    (setf (query-match-map-io-stream match-map) nil))
  (values))

(defmacro with-query-match-map ((match-map &optional memory-limit) &body body)
  `(let ((,match-map (make-query-match-map ,memory-limit)))
     (unwind-protect
          (progn ,@body)
       (close-query-match-map ,match-map))))
