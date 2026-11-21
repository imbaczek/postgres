(in-package #:pg)

(defun physical-install-read-progress (bytes position body-crc last-step label &key allow-tail)
  "Return the completed step, valid end position and partial-step flag.
ALLOW-TAIL leaves bytes after the final complete step for the caller to check."
  (let ((scratch (make-array 32 :element-type '(unsigned-byte 8)))
        (step 0))
    (loop while (and (< step last-step) (< position (length bytes)))
          do (let* ((next-size (%physical-install-write-progress body-crc (1+ step) scratch))
                    (count (min next-size (- (length bytes) position))))
               (when (mismatch bytes scratch :start1 position :end1 (+ position count) :end2 count)
                 (error "invalid ~a progress" label))
               (when (< count next-size)
                 (return-from physical-install-read-progress (values step position t)))
               (incf position count)
               (incf step)))
    (when (and (not allow-tail) (< position (length bytes)))
      (error "unexpected data after final ~a progress" label))
    (values step position nil)))
