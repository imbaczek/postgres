;;; Allocation-free eligibility checks for stable metadata literal trees.
;;; DEPTH is a private argument in the range 0 through 64.
(in-package #:pg)

(declaim (ftype (function (t (integer 0 64)) boolean)
                %metadata-literal-tree-p %metadata-literal-list-p))

(defun %metadata-literal-tree-p (value depth)
  "Check stable integers, strings, NIL, T, keywords and proper literal lists.
The depth cap declines the fast path without allocating."
  (declare (optimize (speed 3) (safety 1) (debug 0))
           (type (integer 0 64) depth))
  (and (< depth 64)
       (cond
         ((or (integerp value) (stringp value)) t)
         ((symbolp value) (or (null value) (eq value t) (keywordp value)))
         ((consp value) (%metadata-literal-list-p value (1+ depth)))
         (t nil))))

(defun %metadata-literal-list-p (value depth)
  "Check a stable proper literal list without allocation.
A cdr cycle declines the fast path."
  (declare (optimize (speed 3) (safety 1) (debug 0))
           (type (integer 0 64) depth))
  (let ((slow value) (fast value))
    (loop
      (dotimes (step 2)
        (unless (consp fast)
          (return-from %metadata-literal-list-p (null fast)))
        (unless (%metadata-literal-tree-p (car fast) depth)
          (return-from %metadata-literal-list-p nil))
        (setf fast (cdr fast)))
      (setf slow (cdr slow))
      (when (eq slow fast) (return nil)))))

(defun %physical-bootstrap-write-manifest-envelope (payload crc output)
  "Write the manifest envelope into OUTPUT and return it.
PAYLOAD is a simple octet vector of UTF-8 bytes and CRC is their checksum.
OUTPUT must be a distinct simple octet vector of exactly
(+ (length PAYLOAD) 28) bytes.
Write the 18-byte magic, eight hexadecimal CRC digits, a newline, PAYLOAD,
and a final newline.  Valid calls allocate no heap memory."
  (declare (optimize (speed 3) (safety 3))
           (type (simple-array (unsigned-byte 8) (*)) payload output)
           (type (unsigned-byte 32) crc))
  (loop for index below 18
        do (setf (aref output index) (char-code (char "PGPHYSICALBACKUP1 " index))))
  (dotimes (offset 8)
    (let ((digit (ldb (byte 4 (* 4 (- 7 offset))) crc)))
      (setf (aref output (+ 18 offset)) (+ digit (if (< digit 10) 48 55)))))
  (setf (aref output 26) 10)
  (replace output payload :start1 27)
  (setf (aref output (+ 27 (length payload))) 10)
  output)
