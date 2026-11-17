(in-package #:pg)

(defun logical-slot-imported-startup-recovery-profile (root)
  ;; This names-only classification supplies no recovery or startup authority.
  (let* ((facts
           (mapcar (lambda (name) (slot-state-lstat (slot-state-fd-path root name)))
                   '("physical-recovery.control" "physical-backup.manifest" "recovery.required")))
         (present (count-if #'identity facts)))
    (unless (every (lambda (entry) (or (null entry) (sb-posix:s-isreg (sb-posix:stat-mode entry))))
                   facts)
      (error "imported startup recovery provenance has a noncanonical file kind"))
    (case present
      (0 :retired)
      (3 :original)
      (otherwise (error "imported startup recovery provenance is only partly retired")))))

(defun logical-slot-imported-startup-history-rows (image profile)
  (let* ((parts (logical-slot-copy-source-proof-decode image))
         (facts (logical-slot-copy-check-source-proof image))
         (evidence (getf facts :evidence))
         (rows (list (list "logical-slot-copy.descriptor" (getf parts :descriptor))
                     (list "logical-slot-copy.base-verified" (getf parts :base-proof))
                     (list "logical-slot-copy.evidence" (getf parts :evidence)))))
    (values
     (append rows
             (ecase profile
               (:retired nil)
               (:original
                (list (list "physical-backup.manifest" (getf evidence :manifest))
                      (list "physical-recovery.control" (getf evidence :control))
                      (list "recovery.required" (physical-recovery-marker-bytes))))))
     facts)))

(defun call-with-logical-slot-imported-startup-history (root image root-check function)
  ;; Hold historical copies as evidence. The complete recovery trio is historical,
  ;; not a live recovery request; the retired profile permits a later checkpoint.
  (physical-adoption-check-startup-quiescence)
  (funcall root-check)
  (let* ((profile (logical-slot-imported-startup-recovery-profile root))
         (root-facts (physical-image-file-facts root))
         (named-root (namestring (uiop:ensure-directory-pathname *pgdata*)))
         (active nil) (failed nil) (thread sb-thread:*current-thread*))
    (multiple-value-bind (rows facts)
        (logical-slot-imported-startup-history-rows (copy-seq image) profile)
      (labels ((check-root ()
                 (unless (eq thread sb-thread:*current-thread*)
                   (error "imported historical observation changed thread"))
                 (physical-adoption-check-startup-quiescence)
                 (funcall root-check)
                 (unless (and (equal named-root
                                     (namestring (uiop:ensure-directory-pathname *pgdata*)))
                              (equal root-facts (physical-image-file-facts root))
                              (equal root-facts (physical-image-file-facts -100 named-root))
                              (logical-slot-copy-imported-startup-candidate-p)
                              (eq profile (logical-slot-imported-startup-recovery-profile root)))
                   (error "imported historical observation changed its root or recovery profile"))
                 (logical-slot-imported-startup-check-certificate-root facts root-facts))
               (check (held)
                 (unless (and active (eq thread sb-thread:*current-thread*))
                   (error "imported historical observation ended or changed thread"))
                 (when failed (error "imported historical observation already failed"))
                 (handler-case
                     (progn
                       (check-root)
                       (logical-slot-imported-check-file-anchors
                        root named-root root-facts held :canonical t)
                       t)
                   (error (condition) (setf failed t) (error condition))))
               (visit (remaining held)
                 (check-root)
                 (if remaining
                     (destructuring-bind (name bytes) (first remaining)
                       (call-with-logical-slot-copy-source-proof-fd
                        root name nil
                        (lambda (fd file-facts)
                          (logical-slot-imported-check-held-file root name fd file-facts bytes)
                          (multiple-value-prog1
                              (visit (rest remaining)
                                     (cons (list root name fd file-facts bytes) held))
                            (logical-slot-imported-check-held-file
                             root name fd file-facts bytes)))))
                     (unwind-protect
                          (progn
                            (setf active t)
                            (check held)
                            (multiple-value-prog1
                                (funcall function profile
                                         (mapcar (lambda (entry)
                                                   (list (copy-seq (second entry))
                                                         (copy-tree (fourth entry))
                                                         (copy-seq (fifth entry))))
                                                 (reverse held))
                                         (lambda () (check held)))
                              (check held)))
                       (setf active nil)))))
        (check-root)
        (multiple-value-prog1 (visit rows nil)
          (check-root))))))
