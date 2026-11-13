;;; Allocation-free ASCII span kernels. The caller validates the simple octet
;;; vector and fixnum bounds and keeps inputs stable throughout both calls.
(in-package #:pg)

(declaim (optimize (speed 3) (safety 0) (debug 0)))

(defun %octet-span-ascii-p (bytes start end)
  "Test a stable, validated simple octet span for ASCII without allocation."
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end))
  (loop for index fixnum from start below end
        always (< (aref bytes index) 128)))

(defun %octet-span-copy-ascii (bytes start end output)
  "Copy a stable, validated ASCII span without allocation.
OUTPUT is a preallocated simple CHARACTER array of exactly END - START elements."
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array character (*)) output)
           (type fixnum start end))
  (loop for index fixnum from start below end
        for destination fixnum from 0
        do (setf (aref output destination) (code-char (aref bytes index))))
  output)

(defun %string-span-utf8-size (string start end)
  "Count UTF-8 bytes without allocation, or return NIL for an invalid scalar.
The caller supplies a stable string and valid fixnum span of at most 4096 characters.
WITH-ARRAY-DATA resolves displaced storage; BASE-CHAR data uses ASCII width."
  (declare (type string string)
           (type fixnum start end))
  (sb-kernel:with-array-data ((data string) (begin start) (finish end))
    (if (typep data 'simple-base-string)
        (- finish begin)
        (locally
            (declare (type (simple-array character (*)) data))
          (let ((size 0))
            (declare (type fixnum size))
            (loop for index fixnum from begin below finish
                  for code fixnum = (char-code (schar data index))
                  do (incf size
                           (cond ((< code #x80) 1)
                                 ((< code #x800) 2)
                                 ((<= #xd800 code #xdfff)
                                  (return-from %string-span-utf8-size nil))
                                 ((< code #x10000) 3)
                                 ((<= code #x10ffff) 4)
                                 (t (return-from %string-span-utf8-size nil)))))
            size)))))
