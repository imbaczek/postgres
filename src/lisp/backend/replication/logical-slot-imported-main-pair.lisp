(in-package #:pg)

(defun logical-slot-imported-startup-check-certificate-root (facts root-facts)
  (let ((certificate (getf facts :certificate)))
    (unless (and (eql (getf certificate :device) (first root-facts))
                 (eql (getf certificate :inode) (second root-facts)))
      (error "imported startup proof belongs to a different PGDATA root")))
  t)

(defun logical-slot-imported-startup-pair-file-facts (held)
  ;; Expose detached relative names and full facts, never held descriptors.
  (logical-slot-copy-clone-facts
   (mapcar (lambda (row)
             (let ((name (second row)))
               (list (if (member name '("journal" "active") :test #'equal)
                         (concatenate 'string "logical-slot-copy.install/" name)
                         name)
                     (fourth row))))
           (reverse held))))

(defun call-with-logical-slot-imported-startup-pair (root root-check function)
  ;; Read the permanent pair before WAL opens. ROOT-CHECK retains real ownership.
  ;; The callback receives copies and a scoped check, never startup permission.
  (physical-adoption-check-startup-quiescence)
  (funcall root-check)
  (let ((root-facts (physical-image-file-facts root))
        (named-root (namestring (uiop:ensure-directory-pathname *pgdata*)))
        (held nil) (private-facts nil) (private-image nil)
        (active nil) (failed nil) (thread sb-thread:*current-thread*))
    (labels ((check-root ()
               (unless (eq thread sb-thread:*current-thread*)
                 (error "imported startup observation changed thread"))
               (physical-adoption-check-startup-quiescence)
               (funcall root-check)
               (unless (and (equal named-root
                                   (namestring (uiop:ensure-directory-pathname *pgdata*)))
                            (equal root-facts (physical-image-file-facts root))
                            (equal root-facts (physical-image-file-facts -100 named-root)))
                 (error "imported startup observation changed its named root")))
             (hold (parent name directory-p bytes next)
               (check-root)
               (call-with-logical-slot-copy-source-proof-fd
                parent name directory-p
                (lambda (fd facts)
                  (let ((record (list parent name fd facts (and bytes (copy-seq bytes)))))
                    (sb-sys:without-interrupts
                      (unwind-protect
                           (progn
                             (push record held)
                             (sb-sys:with-local-interrupts
                               (when bytes
                                 (logical-slot-imported-check-held-file parent name fd facts bytes))
                               (funcall next fd)))
                        (when (eq record (first held)) (pop held))))))))
             (check (&optional projection)
               (unless (and active (eq thread sb-thread:*current-thread*))
                 (error "imported startup observation ended or changed thread"))
               (when failed (error "imported startup observation already failed"))
               (handler-case
                   (progn
                     (check-root)
                     (logical-slot-imported-check-file-anchors
                      root named-root root-facts held :canonical t)
                     (logical-slot-imported-startup-check-certificate-root private-facts root-facts)
                     (ecase projection
                       ((nil) t)
                       (:file-facts (logical-slot-imported-startup-pair-file-facts held))))
                 (error (condition)
                   (setf failed t)
                   (error condition)))))
      (check-root)
      (multiple-value-prog1
          (call-with-logical-slot-imported-canonical-files
           root #'hold
           (lambda (facts image)
             (setf (fifth (first held)) (copy-seq image)
                   private-facts facts private-image (copy-seq image))
             (logical-slot-imported-startup-check-certificate-root facts root-facts))
           (lambda ()
             (unwind-protect
                  (progn
                    (setf active t)
                    (check)
                    (multiple-value-prog1
                        (funcall function (copy-seq private-image)
                                 (logical-slot-copy-check-source-proof (copy-seq private-image))
                                 #'check)
                      (check)))
               (setf active nil))))
        (check-root)))))
