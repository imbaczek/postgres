(in-package #:pg)

(defun physical-install-read-progress (bytes position body-crc last-step label)
  "Return the completed step, valid end position and partial-tail flag.
BYTES holds a validated plan followed by its progress, beginning at POSITION.
Only a prefix of the next expected line may follow the last complete line."
  (let ((scratch (make-array 32 :element-type '(unsigned-byte 8)))
        (step 0))
    (loop while (< position (length bytes))
          do (when (= step last-step)
               (error "unexpected data after final ~a progress" label))
             (let* ((next-size (%physical-install-write-progress body-crc (1+ step) scratch))
                    (count (min next-size (- (length bytes) position))))
               (when (mismatch bytes scratch :start1 position :end1 (+ position count) :end2 count)
                 (error "invalid ~a progress" label))
               (when (< count next-size)
                 (return-from physical-install-read-progress (values step position t)))
               (incf position count)
               (incf step)))
    (values step position nil)))
