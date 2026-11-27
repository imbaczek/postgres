;;; Disposable-server prelude for the hash join holdout benchmark.
(in-package #:pg)

;;; Keep the roadmap's plans and hash operators, restoring the earlier scan,
;;; checksum and text-spill paths for a controlled performance comparison.
(defun single-int4-scan-layout-p (columns)
  (declare (ignore columns))
  nil)

(defun make-fixed-hash-count-iterator (aggregate relations)
  (declare (ignore aggregate relations))
  nil)

(defun wal-page-checksum (page relfilenode block &optional (fork :main))
  (let* ((main-p (eq fork :main))
         (identity-size (if main-p 8 12))
         (copy (make-array (+ identity-size (length page))
                           :element-type '(unsigned-byte 8) :initial-element 0)))
    (page-set-u32 copy 0 relfilenode)
    (unless main-p (page-set-u32 copy 4 (wal-page-fork-id fork)))
    (page-set-u32 copy (- identity-size 4) block)
    (replace copy page :start1 identity-size)
    (page-set-u16 copy (+ identity-size +pd-checksum-off+) 0)
    (let ((checksum (logand (wal-crc32 copy) #xffff)))
      (if (<= checksum 3) (+ checksum 4) checksum))))

(defun wal-dirty-checksum (key page)
  (let ((bytes (make-array (+ 12 +blcksz+) :element-type '(unsigned-byte 8))))
    (wal-set-u32 bytes 0 (first key))
    (wal-set-u32 bytes 4 (wal-page-fork-id (second key)))
    (wal-set-u32 bytes 8 (third key))
    (replace bytes page :start1 12)
    (wal-crc32 bytes)))

(defun query-spill-write-record (stream value)
  (let* ((pathname (query-temp-path-for-stream stream))
         (output (if pathname
                     (make-instance 'query-temp-limited-character-stream :stream stream :pathname pathname)
                     stream)))
    (let ((*print-pretty* nil) (*print-readably* t) (*print-circle* t)
                               (*print-length* nil) (*print-level* nil) (*print-base* 10)
                               (*print-radix* nil) (*package* (find-package '#:pg)))
      (query-spill-write-value output value)
      (terpri output))))

(defun query-spill-read-record (stream)
  (call-with-counted-query-spill-read stream
                                      (lambda () (query-spill-read-text-record stream))))

(let ((original (symbol-function 'hash-join-key-slots)))
  (setf (symbol-function 'hash-join-key-slots)
        (lambda (join) (values (funcall original join) nil))))
