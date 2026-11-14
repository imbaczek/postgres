(in-package #:pg)

(defun logical-install-allocation-sample ()
  "Run a cold installation sample and restore the captured namespace on exit.
Private staging orphans may remain; later samples do not adopt them."
  (let ((root nil) (image nil))
    (unwind-protect
         (let ((*logical-slot-copy-install-publication-test-hook*
                 (lambda (point publisher)
                   (declare (ignore point))
                   (unless root
                     (setf root (logical-install-journal-test-root publisher)
                           image (copy-seq (logical-slot-copy-install-publisher-image publisher)))))))
           (logical-slot-copy-install-fresh-active))
      (when root (unwind-protect (logical-install-exchange-test-restore root image)
                   (sb-posix:close root))))))

(defun logical-install-allocation-profile ()
  "Profile a cold startup installation after a warmup sample.
ALLOC-TOTAL includes namespace restoration.  Function counts, bytes, and times
are inclusive and overlap for nested calls; they must not be summed.  Restore
the original function definitions on exit, including on failure."
  (assert (logical-install-handoff-test-unopened-p))
  (let ((symbols '(physical-image-file-facts logical-slot-copy-install-file-identity
                   logical-slot-copy-import-state-image logical-slot-copy-install-input-view
                   logical-slot-copy-install-check-input-seal logical-slot-copy-import-image
                   logical-slot-copy-import-evidence-encode logical-slot-copy-descriptor-encode
                   logical-slot-copy-install-read-layout logical-slot-copy-install-check-row-receipt
                   logical-slot-copy-check-committed-source logical-slot-copy-install-row-durable-facts))
        (originals nil) (stats nil))
    (logical-install-allocation-sample)
    (sb-ext:gc :full t)
    (unwind-protect
         (progn
           (dolist (symbol symbols)
             (let ((original (symbol-function symbol))
                   (counter (make-array 3 :initial-element 0)))
               (push (cons symbol original) originals)
               (push (cons symbol counter) stats)
               (setf (symbol-function symbol)
                     (lambda (&rest arguments)
                       (declare (dynamic-extent arguments))
                       (let ((bytes (sb-ext:get-bytes-consed)) (start (get-internal-real-time)))
                         (unwind-protect (apply original arguments)
                           (incf (aref counter 0))
                           (incf (aref counter 1) (- (sb-ext:get-bytes-consed) bytes))
                           (incf (aref counter 2) (- (get-internal-real-time) start))))))))
           (let ((bytes (sb-ext:get-bytes-consed)) (start (get-internal-real-time)))
             (logical-install-allocation-sample)
             (format t "~&ALLOC-TOTAL bytes=~d seconds=~,6f~%"
                     (- (sb-ext:get-bytes-consed) bytes)
                     (/ (- (get-internal-real-time) start) (float internal-time-units-per-second 1d0)))))
      (sb-sys:without-interrupts
        (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry)))))
    (dolist (entry (reverse stats))
      (let ((counter (cdr entry)))
        (format t "~&ALLOC-FUNCTION ~a calls=~d bytes=~d seconds=~,6f~%"
                (car entry) (aref counter 0) (aref counter 1)
                (/ (aref counter 2) (float internal-time-units-per-second 1d0)))))
    (finish-output)))
