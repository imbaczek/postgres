(in-package #:pg)

(defun logical-slot-copy-source-proof-read-held (owner context register)
  (let* ((root (physical-image-context-root-fd context))
         (root-facts (physical-image-file-facts root))
         (named-root (namestring (physical-adoption-owner-directory owner))))
    (call-with-logical-slot-copy-source-proof-fd
     root "logical-slot-copy.source-proof" nil
     (lambda (proof-fd proof-facts)
       (let* ((image (logical-slot-copy-import-control-read-fd
                      proof-fd (logical-slot-copy-source-proof-maximum-bytes)))
              (facts (logical-slot-copy-check-source-proof image))
              (certificate (getf facts :certificate)))
         (unless (and (= (first root-facts) (getf certificate :device))
                      (= (second root-facts) (getf certificate :inode)))
           (error "permanent source proof belongs to a different PGDATA root"))
         (call-with-logical-slot-copy-source-proof-fd
          root "pg_physical_retained" t
          (lambda (archive-fd archive-facts)
            (let ((input (physical-image-fd-path archive-fd))
                  (directory (physical-retained-root-directory
                              (merge-pathnames "pg_physical_retained/"
                                               (physical-adoption-owner-directory owner))))
                  (failed nil) (active t))
              (labels
                  ((check ()
                     (unless (and active (not failed))
                       (error "permanent source reader scope ended or failed"))
                     (physical-directory-check-owner-common owner)
                     (logical-slot-copy-check-postmaster-owner
                      owner (logical-slot-copy-source-reader-owner-ownership owner))
                     (physical-image-check-context context)
                     (unless (and (equal root-facts (physical-image-file-facts root))
                                  (equal root-facts (physical-image-file-facts -100 named-root))
                                  (equal proof-facts (physical-image-file-facts proof-fd))
                                  (equal proof-facts
                                         (physical-image-file-facts root "logical-slot-copy.source-proof"))
                                  (equal archive-facts (physical-image-file-facts archive-fd))
                                  (equal archive-facts
                                         (physical-image-file-facts root "pg_physical_retained"))
                                  (not (slot-state-lstat
                                        (slot-state-fd-path root "logical-slot-copy.source-proof.tmp"))))
                       (error "permanent source reader lost its named anchors")))
                   (read-object (selected kind id maximum)
                     (let ((complete nil))
                       (unwind-protect
                            (multiple-value-prog1
                                (progn
                                  (check)
                                  (unless (and (equal selected input)
                                               (= maximum (ecase kind
                                                            (:inventory +physical-retained-inventory-limit+)
                                                            ((:extent :overlap) +physical-retained-image-limit+))))
                                    (error "permanent source object reader escaped its fixed input"))
                                  (let* ((path (physical-retained-object-path input kind id))
                                         (bytes (call-with-logical-slot-copy-source-proof-fd
                                                 archive-fd (file-namestring path) nil
                                                 (lambda (fd observed)
                                                   (declare (ignore observed))
                                                   (logical-slot-copy-import-control-read-fd fd maximum)))))
                                    (unless (= id (logical-slot-copy-import-digest bytes))
                                      (error "permanent source archive object differs from its content ID"))
                                    (check)
                                    bytes))
                              (setf complete t))
                         (unless complete
                           (setf failed t (physical-adoption-owner-active-p owner) nil)))))
                   (prove ()
                     (physical-directory-check-owner owner)
                     (check)
                     (unless (equalp image (logical-slot-copy-import-control-read-fd
                                            proof-fd (length image)))
                       (error "permanent source canonical bytes changed"))
                     (logical-slot-copy-source-proof-scan-archive facts input #'read-object)
                     (logical-slot-copy-source-proof-check-archive-prefix facts input #'read-object)
                     (check)
                     (physical-directory-check-owner owner)))
                (unwind-protect
                     (progn
                       (check)
                       (let* ((reference (getf certificate :retained-reference))
                              (identity (physical-promotion-retained-identity
                                         (getf certificate :system-id) reference))
                              (history (logical-slot-copy-source-proof-parent-history facts)))
                         (funcall register directory input identity (getf reference :root-id)
                                  history #'read-object))
                       (prove)
                       (when *logical-slot-copy-verification-test-hook*
                         (funcall *logical-slot-copy-verification-test-hook*
                                  :after-source-proof-files-close nil))
                       (let ((*logical-slot-copy-verification-test-hook* nil)) (prove))
                       facts)
                  (setf active nil)))))))))))

(defun logical-slot-copy-read-source-proof ()
  (logical-slot-copy-install-check-input-entry)
  (call-with-logical-slot-copy-install-exclusion
   (lambda ()
     (call-with-logical-slot-copy-source-reader-owner
      (lambda (owner)
        (call-with-logical-slot-copy-source-reader-reference
         (lambda (register)
           (call-with-physical-directory-root
            owner
            (lambda (borrowed path)
              (declare (ignore path))
              (call-with-physical-image-owned-context
               borrowed t
               (lambda (context)
                 (let ((*pgdata* (physical-image-context-root-path context))
                       (*physical-image-context* context))
                   (logical-slot-copy-source-proof-read-held owner context register)))))))))))))
