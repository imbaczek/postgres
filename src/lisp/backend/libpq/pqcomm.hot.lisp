(in-package #:pg)

(declaim (optimize (speed 3) (safety 0) (debug 0)))

(defun %octet-span-ascii-p (bytes start end)
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end))
  (loop for index fixnum from start below end
        always (< (aref bytes index) 128)))

(defun %octet-span-copy-ascii (bytes start end output)
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array character (*)) output)
           (type fixnum start end))
  (loop for index fixnum from start below end
        for destination fixnum from 0
        do (setf (aref output destination) (code-char (aref bytes index))))
  output)
