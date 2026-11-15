(in-package #:pg)

(defun call-with-logical-slot-continuation-sidecar (owner context facts guard function)
  (logical-slot-copy-check-input owner context)
  (unless (assoc "logical_slot_wal" (physical-image-context-children context) :test #'equal)
    (logical-slot-copy-open-input-sidecar owner context))
  (let* ((descriptor (getf facts :descriptor))
         (parent (cdr (assoc "logical_slot_wal" (physical-image-context-children context) :test #'equal)))
         (directory-facts (physical-image-file-facts parent))
         (active t) (thread sb-thread:*current-thread*))
    (logical-slot-copy-input-sidecar-inventory context descriptor)
    (multiple-value-bind (observations index)
        (logical-slot-copy-observe-pieces context descriptor nil (make-hash-table :test #'equal))
      (labels ((check-directory ()
                 (unless (and active (eq thread sb-thread:*current-thread*)
                              (equal directory-facts (physical-image-file-facts parent))
                              (equal directory-facts
                                     (physical-image-file-facts
                                      (physical-image-context-root-fd context) "logical_slot_wal")))
                   (error "continuation sidecar lost its live named directory"))
                 t)
               (check-files ()
                 (check-directory)
                 (logical-slot-copy-input-sidecar-inventory context descriptor)
                 (dolist (row observations)
                   (unless (equal (fourth row)
                                  (physical-image-file-facts
                                   parent (file-namestring (first row))))
                     (error "continuation sidecar piece changed during its source scope")))
                 t)
               (check-reader ()
                 (unless (funcall guard) (error "continuation sidecar lost its storage scope"))
                 (check-directory)))
        (unwind-protect
             (progn
               (check-files)
               (multiple-value-prog1
                   (funcall function
                            (logical-slot-copy-input-piece-reader parent index #'check-reader)
                            #'check-directory #'check-files)
                 (check-files)))
          (sb-sys:without-interrupts (setf active nil)))))))
