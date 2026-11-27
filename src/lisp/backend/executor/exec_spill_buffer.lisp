;;; Small UTF-8 buffers for the hash join's temporary replay files.
(in-package #:pg)

(defconstant +hash-spill-buffer-bytes+ 512)

(defstruct hash-spill-state
  (buffer (make-array +hash-spill-buffer-bytes+ :element-type '(unsigned-byte 8))
   :type (simple-array (unsigned-byte 8) (*)))
  (cursor 0 :type fixnum)
  (position 0 :type (unsigned-byte 64))
  (end 0 :type fixnum)
  (width 0 :type (integer 0 4))
  (unread nil :type (or null character)))

(defclass hash-spill-stream ()
  ((fd :initform nil :accessor hash-spill-fd)
   (state :initform (make-hash-spill-state) :reader hash-spill-state)))

(defclass hash-spill-input (hash-spill-stream sb-gray:fundamental-character-input-stream) ())

(defclass hash-spill-output (hash-spill-stream sb-gray:fundamental-character-output-stream)
  ((quota-writer :initform nil :accessor hash-spill-quota-writer)))

(defclass hash-spill-match-stream
    (hash-spill-stream sb-gray:fundamental-binary-input-stream sb-gray:fundamental-binary-output-stream)
  ())

(defun open-hash-spill-stream (path direction)
  "Use an executor-owned buffer instead of a native file stream's fixed allocation.
Allocate the stream before opening the descriptor so acquisition can transfer directly."
  (let* ((stream (make-instance (ecase direction
                                  (:input 'hash-spill-input)
                                  (:output 'hash-spill-output)
                                  (:io 'hash-spill-match-stream))))
         (creates-p (not (eq direction :input)))
         (flags (ecase direction
                  (:input sb-posix:o-rdonly)
                  (:output sb-posix:o-wronly)
                  (:io sb-posix:o-rdwr))))
    (handler-case
        (setf (hash-spill-fd stream)
              (port-open-file path
                              (if creates-p (logior flags sb-posix:o-creat sb-posix:o-excl) flags)
                              #o600))
      (sb-posix:syscall-error (condition)
        (unless (and creates-p (= (sb-posix:syscall-errno condition) sb-posix:eexist))
          (error condition))))
    (and (hash-spill-fd stream) stream)))

(defconstant +hash-spill-stream-charge+ (+ +hash-spill-buffer-bytes+ 160)
  "Reserve the payload and its stream, state, and vector headers.")

(defun hash-spill-system-io (operation fd buffer start count)
  (sb-sys:with-pinned-objects (buffer)
    (loop
      (handler-case
          (return (funcall operation fd (sb-sys:sap+ (sb-sys:vector-sap buffer) start) count))
        (sb-posix:syscall-error (condition)
          (unless (= (sb-posix:syscall-errno condition) sb-posix:eintr)
            (error condition)))))))

(defun write-hash-spill-buffer (stream state count)
  (loop with buffer = (hash-spill-state-buffer state)
        for start = 0 then (+ start written)
        while (< start count)
        for written = (progn (check-for-interrupts)
                             (hash-spill-system-io #'sb-posix:write (hash-spill-fd stream)
                                                   buffer start (- count start)))
        when (zerop written) do (error "Short write to hash spill file")))

(defun hash-spill-seek (stream position)
  (let ((fd (hash-spill-fd stream)))
    (case position
      (:start (sb-posix:lseek fd 0 sb-posix:seek-set))
      (:end (sb-posix:lseek fd 0 sb-posix:seek-end))
      (otherwise (sb-posix:lseek fd position sb-posix:seek-set)))))

(defmethod sb-gray:stream-write-sequence ((stream hash-spill-match-stream) sequence
                                          &optional (start 0) end)
  (let* ((state (hash-spill-state stream))
         (buffer (hash-spill-state-buffer state))
         (end (or end (length sequence))))
    (loop for offset from start below end by (length buffer)
          for count = (min (length buffer) (- end offset))
          do (replace buffer sequence :end1 count :start2 offset :end2 (+ offset count))
             (write-hash-spill-buffer stream state count)
             (incf (hash-spill-state-position state) count)))
  sequence)

(defmethod stream-element-type ((stream hash-spill-match-stream))
  '(unsigned-byte 8))

(defmethod sb-gray:stream-read-byte ((stream hash-spill-match-stream))
  (let ((state (hash-spill-state stream)))
    (let ((buffer (hash-spill-state-buffer state)))
      (if (zerop (hash-spill-system-io #'sb-posix:read (hash-spill-fd stream) buffer 0 1))
          :eof
          (progn (incf (hash-spill-state-position state)) (aref buffer 0))))))

(defmethod sb-gray:stream-write-byte ((stream hash-spill-match-stream) byte)
  (let ((state (hash-spill-state stream)))
    (let ((buffer (hash-spill-state-buffer state)))
      (setf (aref buffer 0) byte)
      (unless (= 1 (hash-spill-system-io #'sb-posix:write (hash-spill-fd stream) buffer 0 1))
        (error "Short write to hash spill match map"))
      (incf (hash-spill-state-position state)))
    byte))

(defmethod sb-gray:stream-finish-output ((stream hash-spill-match-stream))
  nil)

(defmethod sb-gray:stream-file-position ((stream hash-spill-match-stream) &optional position)
  (let ((state (hash-spill-state stream)))
    (if position
        (progn
          (unless (eql position (hash-spill-state-position state))
            (setf (hash-spill-state-position state) (hash-spill-seek stream position)))
          t)
        (hash-spill-state-position state))))

(defmethod open-stream-p ((stream hash-spill-stream))
  (not (null (hash-spill-fd stream))))

(defmethod close ((stream hash-spill-stream) &key abort)
  (when (hash-spill-fd stream)
    (unwind-protect
         (when (and (not abort) (output-stream-p stream))
           (finish-output stream))
      (sb-sys:without-interrupts
        (let ((fd (hash-spill-fd stream)))
          ;; On Linux an interrupted close still releases the descriptor.
          (setf (hash-spill-fd stream) nil)
          (sb-posix:close fd)))))
  t)

(defmethod sb-gray:stream-finish-output ((stream hash-spill-output))
  (let ((state (hash-spill-state stream)))
    (write-hash-spill-buffer stream state (hash-spill-state-cursor state))
    (setf (hash-spill-state-cursor state) 0)
    nil))

(declaim (inline hash-spill-write-octet hash-spill-read-octet))

(defun hash-spill-write-octet (stream state octet)
  (when (= (hash-spill-state-cursor state) +hash-spill-buffer-bytes+)
    (sb-gray:stream-finish-output stream))
  (setf (aref (hash-spill-state-buffer state) (hash-spill-state-cursor state)) octet)
  (incf (hash-spill-state-cursor state)))

(defun hash-spill-encode-character (stream state character)
  (let ((code (char-code character)))
    (cond
      ((< code #x80) (hash-spill-write-octet stream state code))
      (t
       (let ((width (cond ((< code #x800) 2) ((< code #x10000) 3) (t 4))))
         (when (<= #xd800 code #xdfff)
           (error "Invalid UTF-8 character in hash spill file"))
         (hash-spill-write-octet stream state
                                 (logior (aref #(#xc0 #xe0 #xf0) (- width 2))
                                         (ash code (* -6 (1- width)))))
         (loop for shift from (* 6 (- width 2)) downto 0 by 6
               do (hash-spill-write-octet stream state (logior #x80 (ldb (byte 6 shift) code)))))))))

(defmethod sb-gray:stream-write-char ((stream hash-spill-output) character)
  (let ((state (hash-spill-state stream)))
    (hash-spill-encode-character stream state character)
    character))

(defmethod sb-gray:stream-write-string ((stream hash-spill-output) string &optional (start 0) end)
  (let ((state (hash-spill-state stream)))
    (loop for index from start below (or end (length string))
          do (hash-spill-encode-character stream state (char string index)))
    string))

(defun hash-spill-read-octet (stream state)
  (when (= (hash-spill-state-cursor state) (hash-spill-state-end state))
    (check-for-interrupts)
    (incf (hash-spill-state-position state) (hash-spill-state-end state))
    (setf (hash-spill-state-cursor state) 0
          (hash-spill-state-end state)
          (hash-spill-system-io #'sb-posix:read (hash-spill-fd stream)
                                (hash-spill-state-buffer state) 0 +hash-spill-buffer-bytes+)))
  (unless (zerop (hash-spill-state-end state))
    (prog1 (aref (hash-spill-state-buffer state) (hash-spill-state-cursor state))
      (incf (hash-spill-state-cursor state)))))

(defun write-hash-spill-u32 (stream state value)
  (declare (type (unsigned-byte 32) value))
  (dotimes (index 4)
    (hash-spill-write-octet stream state (ldb (byte 8 (* index 8)) value))))

(defun read-hash-spill-u32 (stream state)
  (let ((value 0))
    (declare (type (unsigned-byte 32) value))
    (dotimes (index 4 value)
      (let ((octet (hash-spill-read-octet stream state)))
        (unless octet (error "Truncated binary hash spill record"))
        (setf value (logior value (ash octet (* index 8))))))))

(defun hash-spill-decode-character (stream state first)
  (let* ((width (cond ((< first #x80) 1) ((<= #xc2 first #xdf) 2)
                      ((<= #xe0 first #xef) 3) ((<= #xf0 first #xf4) 4)
                      (t (error "Invalid UTF-8 in hash spill file"))))
         (code (if (= width 1) first (ldb (byte (- 7 width) 0) first))))
    (dotimes (index (1- width))
      (let ((octet (hash-spill-read-octet stream state)))
        (unless (and octet (<= #x80 octet #xbf))
          (error "Invalid UTF-8 in hash spill file"))
        (setf code (logior (ash code 6) (logand octet #x3f)))))
    (unless (and (<= (aref #(0 #x80 #x800 #x10000) (1- width)) code #x10ffff)
                 (not (<= #xd800 code #xdfff)))
      (error "Invalid UTF-8 in hash spill file"))
    (setf (hash-spill-state-width state) width)
    (code-char code)))

(defmethod sb-gray:stream-read-char ((stream hash-spill-input))
  (let ((state (hash-spill-state stream)))
    (let ((unread (hash-spill-state-unread state)))
      (if unread
          (progn (setf (hash-spill-state-unread state) nil) unread)
          (let ((first (hash-spill-read-octet stream state)))
            (cond ((null first) :eof)
                  ((< first #x80)
                   (setf (hash-spill-state-width state) 1)
                   (code-char first))
                  (t (hash-spill-decode-character stream state first))))))))

(defmethod sb-gray:stream-unread-char ((stream hash-spill-input) character)
  (let ((state (hash-spill-state stream)))
    (setf (hash-spill-state-unread state) character)
    nil))

(defmethod sb-gray:stream-file-position ((stream hash-spill-input) &optional position)
  (let ((state (hash-spill-state stream)))
    (if position
        (let ((offset (hash-spill-seek stream position)))
          (setf (hash-spill-state-position state) offset
                (hash-spill-state-cursor state) 0 (hash-spill-state-end state) 0
                (hash-spill-state-unread state) nil)
          t)
        (- (+ (hash-spill-state-position state) (hash-spill-state-cursor state))
           (if (hash-spill-state-unread state) (hash-spill-state-width state) 0)))))
