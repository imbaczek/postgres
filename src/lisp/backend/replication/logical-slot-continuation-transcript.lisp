(in-package #:pg)

(defun logical-slot-continuation-tail-frame (record raw owner carrier kind original)
  (unless (and (typep raw '(vector (unsigned-byte 8))) (<= 1 (length raw) +wal-segment-size+)
               (typep owner '(integer 1 4294967295)) (typep carrier '(integer 1 4294967295))
               (typep original '(unsigned-byte 64))
               (<= original (wal-record-start record)) (< (wal-record-start record) (wal-record-end record))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (= (length raw) (- (wal-record-end record) (wal-record-start record))))
    (error "invalid continuation transcript coordinates"))
  (let ((frame (make-array (+ 40 (length raw)) :element-type '(unsigned-byte 8) :initial-element 0)))
    (wal-set-u64 frame 0 original)
    (wal-set-u64 frame 8 (wal-record-start record))
    (wal-set-u64 frame 16 (wal-record-end record))
    (wal-set-u32 frame 24 owner)
    (wal-set-u32 frame 28 carrier)
    (setf (aref frame 32) (ecase kind (:ordinary 0) (:end-of-recovery 1)))
    (wal-set-u32 frame 36 (length raw))
    (replace frame raw :start1 40)
    frame))

(defun call-with-logical-slot-continuation-transcript (function)
  ;; Use a private child of the process temporary directory.  Attempt cleanup
  ;; on normal or nonlocal exit; failed cleanup retains ownership for retry.
  ;; Scratch grows with the WAL interval and uses the query temporary-file
  ;; quota; an I/O failure leaves startup fenced.
  (let* ((*query-temp-directory* nil)
         (spool (make-logical-spool)))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf (query-temp-cleanup-owner-directory (logical-spool-owner spool))
                   (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp
                     (namestring (merge-pathnames "pg-continuation-proof.XXXXXX"
                                                  (uiop:temporary-directory))))))
             (sb-sys:with-local-interrupts (funcall function spool)))
        (close-logical-spool spool)))))
