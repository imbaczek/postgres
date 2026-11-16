(in-package #:pg)

(defun logical-slot-continuation-cold-decode-selected (bytes)
  (if (and (<= 16 (length bytes))
           (equalp (subseq bytes 0 16) #(80 71 76 79 71 67 79 78 84 83 78 65 80 49 0 0)))
      (values :stable (logical-slot-continuation-decode bytes))
      (values :prepared (logical-slot-continuation-prepared-decode bytes))))

(defun logical-slot-continuation-cold-classify (kind model rows binding certificate host)
  (ecase kind
    (:stable
     (logical-slot-continuation-check-selected-snapshot model binding certificate host rows (sixth host))
     (values model :stable))
    (:prepared
     (dolist (key '(:old :new))
       (let ((snapshot (getf model key)))
         (logical-slot-continuation-check-selected-snapshot
          snapshot binding certificate host (getf snapshot :slots) (sixth host))))
     (let ((selection (logical-slot-continuation-prepared-classify model rows)))
       (when (eq selection :blocked)
         (error "cold continuation complete inventory matches neither old nor new"))
       (values (getf model selection) selection)))))

(defun call-with-logical-slot-continuation-cold-selected-files (storage function)
  (let* ((directory (logical-slot-continuation-storage-call storage :directory))
         (names (logical-slot-continuation-selector-names storage))
         (root-facts (physical-image-file-facts directory)))
    (unless (member "selector" names :test #'string=)
      (error "cold continuation has no canonical selected selector"))
    (call-with-logical-slot-copy-source-proof-fd
     directory "selector" nil
     (lambda (fd facts)
       (let* ((image (logical-slot-copy-import-control-read-fd
                      fd (logical-slot-continuation-prepared-maximum-bytes)))
              (ledger (list "selector" (copy-list facts) image)))
         (multiple-value-bind (kind model) (logical-slot-continuation-cold-decode-selected image)
           (labels ((with-temporary (temporary temp-fd)
                      (flet ((check ()
                               (logical-slot-continuation-storage-check storage)
                               (unless (and (equal names (logical-slot-continuation-selector-names storage))
                                            (equal root-facts (physical-image-file-facts directory)))
                                 (error "cold continuation selected directory changed"))
                               (logical-slot-imported-check-held-file directory "selector" fd facts image)
                               (when temporary
                                 (logical-slot-imported-check-held-file
                                  directory "selector.tmp" temp-fd (second temporary) (third temporary)))))
                        (check)
                        (multiple-value-prog1 (funcall function kind model ledger temporary #'check)
                          (check)))))
             (if (member "selector.tmp" names :test #'string=)
                 (call-with-logical-slot-copy-source-proof-fd
                  directory "selector.tmp" nil
                  (lambda (temp-fd temp-facts)
                    (with-temporary
                        (list "selector.tmp" (copy-list temp-facts)
                              (logical-slot-copy-import-control-read-fd
                               temp-fd (logical-slot-continuation-prepared-maximum-bytes)))
                      temp-fd)))
                 (with-temporary nil nil)))))))))
