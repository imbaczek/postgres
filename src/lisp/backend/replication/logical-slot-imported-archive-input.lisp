(in-package #:pg)

;; Open the named archive through the held directory.  The reader bounds each
;; object and verifies its content ID; it performs no WAL or live owner checks,
;; so retained-root validation can call it under the archive mutex.

(defun call-with-logical-slot-imported-archive-input (context function)
  (let ((root (physical-image-context-root-fd context)))
    (call-with-logical-slot-imported-archive-directory
     root
     (lambda (fd facts)
       (let* ((input (physical-image-fd-path fd))
              (directory (physical-retained-root-directory input))
              (identity (logical-slot-copy-source-proof-directory-identity facts))
              (thread sb-thread:*current-thread*) (mutex *wal-lock*) (active t))
         (labels ((check ()
                    (unless (and active (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*))
                      (error "imported archive input scope ended or changed execution"))
                    (physical-image-check-context context)
                    (unless (and (equal identity (logical-slot-copy-source-proof-directory-identity
                                                  (physical-image-file-facts fd)))
                                 (equal identity (logical-slot-copy-source-proof-directory-identity
                                                  (physical-image-file-facts root "pg_physical_retained")))
                                 (equal directory (truename input)))
                      (error "imported archive input lost its named directory"))
                    t)
                  (read-object (selected kind id maximum)
                    (check)
                    (unless (and (or (equal selected directory) (equal selected input))
                                 (typep id '(unsigned-byte 64))
                                 (eql maximum (ecase kind
                                                (:inventory +physical-retained-inventory-limit+)
                                                ((:extent :overlap) +physical-retained-image-limit+))))
                      (error "imported archive input request escaped its fixed bounds"))
                    (let ((bytes
                            (call-with-logical-slot-copy-source-proof-fd
                             fd (file-namestring (physical-retained-object-path input kind id)) nil
                             (lambda (object observed)
                               (declare (ignore observed))
                               (logical-slot-copy-import-control-read-fd object maximum)))))
                      (unless (= id (logical-slot-copy-import-digest bytes))
                        (error "imported archive input differs from its content ID"))
                      (check)
                      bytes)))
           (unwind-protect
                (progn (check)
                       (multiple-value-prog1 (funcall function directory input #'read-object #'check
                                                      (physical-retained-archive-key fd))
                         (check)))
             (setf active nil))))))))
