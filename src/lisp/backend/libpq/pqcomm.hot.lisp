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
