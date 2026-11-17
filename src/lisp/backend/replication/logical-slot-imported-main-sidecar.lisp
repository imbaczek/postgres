(in-package #:pg)

(defun call-with-logical-slot-imported-startup-sidecar (root image root-check function)
  ;; Check the original immutable sidecar without opening WAL or minting a source.
  ;; The callback's check closure accepts :DIRECTORY-FACTS to return a copy of
  ;; the directory facts after the same checks; it is valid only in this scope.
  (physical-adoption-check-startup-quiescence)
  (funcall root-check)
  (let* ((facts (logical-slot-copy-check-source-proof (copy-seq image)))
         (descriptor (getf facts :descriptor))
         (named-root (namestring (uiop:ensure-directory-pathname *pgdata*)))
         (root-facts (physical-image-file-facts root))
         (active nil) (failed nil) (thread sb-thread:*current-thread*))
    (logical-slot-imported-startup-check-certificate-root facts root-facts)
    (call-with-logical-slot-copy-source-proof-fd
     root "logical_slot_wal" t
     (lambda (sidecar sidecar-facts)
       (call-with-physical-image-owned-context
        root t
        (lambda (context)
          (physical-image-duplicate-child context "logical_slot_wal" sidecar)
          (logical-slot-copy-input-sidecar-inventory context descriptor)
          (multiple-value-bind (observations index)
              (logical-slot-copy-observe-pieces
               context descriptor nil (make-hash-table :test #'equal))
            (declare (ignore index))
            (setf observations (nreverse observations))
            (labels ((check (&optional projection)
                       (unless (and active (eq thread sb-thread:*current-thread*))
                         (error "imported sidecar observation ended or changed thread"))
                       (when failed (error "imported sidecar observation already failed"))
                       (handler-case
                           (progn
                             (physical-adoption-check-startup-quiescence)
                             (funcall root-check)
                             (unless (and (equal named-root
                                                 (namestring
                                                  (uiop:ensure-directory-pathname *pgdata*)))
                                          (equal root-facts (physical-image-file-facts root))
                                          (equal root-facts
                                                 (physical-image-file-facts -100 named-root))
                                          (equal sidecar-facts (physical-image-file-facts sidecar))
                                          (equal sidecar-facts
                                                 (physical-image-file-facts
                                                  root "logical_slot_wal")))
                               (error "imported sidecar observation changed its named inputs"))
                             (logical-slot-copy-input-sidecar-inventory context descriptor)
                             (dolist (row observations)
                               (multiple-value-bind (parent leaf)
                                   (physical-image-parent context (first row))
                                 (unless (and (= (first (fourth row)) (first sidecar-facts))
                                              (= (ninth (fourth row)) (ninth sidecar-facts))
                                              (equal (fourth row)
                                                     (physical-image-file-facts parent leaf)))
                                   (error
                                    "imported sidecar piece changed after digest verification"))))
                             (ecase projection
                               ((nil) t)
                               (:directory-facts (logical-slot-copy-clone-facts sidecar-facts))))
                         (error (condition) (setf failed t) (error condition)))))
              (unwind-protect
                   (progn
                     (setf active t)
                     (check)
                     (multiple-value-prog1
                         (funcall function (logical-slot-copy-clone-facts observations) #'check)
                       (check)))
                (setf active nil))))))))))
