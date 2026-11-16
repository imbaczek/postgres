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

(defun call-with-logical-slot-continuation-transcript-reader (spool start end function)
  (unless (and (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64))
               (<= start end) (functionp function) (logical-spool-sealed-p spool))
    (error "continuation transcript reader requires a sealed bounded interval"))
  (let ((active t) (busy nil) (failed nil) (thread sb-thread:*current-thread*))
    (labels ((map-records (visit from)
               (unless (and active (eq thread sb-thread:*current-thread*))
                 (error "continuation transcript reader ended or changed thread"))
               (unless (and (functionp visit) (eql start from))
                 (error "continuation transcript reader requires its exact starting point"))
               (when (or busy failed)
                 (error "continuation transcript reader is busy or failed"))
               (let ((complete nil) (position start))
                 (setf busy t)
                 (unwind-protect
                      (multiple-value-prog1
                          (call-with-logical-spool-reader
                           spool (+ 40 +wal-segment-size+)
                           (lambda (next)
                             (loop for frame = (funcall next) while frame
                                   do (unless (and (>= (length frame) 41)
                                                   (= position (wal-u64 frame 0))
                                                   (<= position (wal-u64 frame 8))
                                                   (< (wal-u64 frame 8) (wal-u64 frame 16))
                                                   (<= (wal-u64 frame 16) end)
                                                   (= (wal-u32 frame 36) (- (length frame) 40)
                                                      (- (wal-u64 frame 16) (wal-u64 frame 8)))
                                                   (plusp (wal-u32 frame 24)) (plusp (wal-u32 frame 28))
                                                   (member (aref frame 32) '(0 1))
                                                   (every #'zerop (subseq frame 33 36)))
                                        (error "invalid continuation transcript frame"))
                                      (let* ((raw (subseq frame 40))
                                             (record (wal-decode-record raw (wal-u64 frame 8))))
                                        (unless (and (= (wal-record-end record) (wal-u64 frame 16))
                                                     (eql (= (aref frame 32) 1)
                                                          (= (wal-record-type record) +wal-record-end-of-recovery+)))
                                          (error "continuation transcript record differs from its frame"))
                                        (funcall visit record raw)
                                        (setf position (wal-record-end record))))
                             (unless (= position end)
                               (error "continuation transcript did not reach its captured endpoint"))))
                        (setf complete t))
                   (setf busy nil)
                   (unless complete (setf failed t))))))
      (unwind-protect (funcall function #'map-records)
        (setf active nil)))))

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
