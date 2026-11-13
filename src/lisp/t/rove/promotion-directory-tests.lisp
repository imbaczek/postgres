(in-package #:pg)

(defun call-with-unit-promotion-directory (function)
  (let* ((root (uiop:ensure-directory-pathname
                (sb-posix:mkdtemp
                 (namestring (merge-pathnames "promotion-directory-XXXXXX"
                                              (uiop:temporary-directory))))))
         (fd nil))
    (unwind-protect
         (progn
           (dolist (name '("base" "global" "pg_wal"))
             (sb-posix:mkdir (namestring (merge-pathnames name root)) #o700))
           (setf fd (physical-backup-open-directory root))
           (call-with-physical-backup-image-directory
            fd (lambda (path)
                 (let ((*pgdata* path))
                   (funcall function *physical-image-context* root)))))
      (when fd (sb-posix:close fd))
      (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty held-promotion-prefix-keeps-exact-parent-bytes
    ((offset (range integer 0 256)) (salt (range integer 0 255)))
  (:about wal-install-timeline-prefix)
  (:trials (:normal 100))
  (call-with-unit-promotion-directory
   (lambda (context root)
     (declare (ignore root))
     (let* ((fd (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
            (prefix (make-array offset :element-type '(unsigned-byte 8))))
       (dotimes (i offset) (setf (aref prefix i) (mod (+ i salt) 256)))
       (wal-install-timeline-prefix 1 2 offset prefix :timeline fd)
       (equalp prefix (read-bounded-regular-file (wal-segment-path 0 2 :timeline)
                                                 +wal-segment-size+))))))

(rove:deftest held-promotion-prefix-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'held-promotion-prefix-keeps-exact-parent-bytes :seed 7520))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest held-promotion-directory-rejects-unbound-or-retired-descriptors
    (call-with-unit-promotion-directory
     (lambda (context root)
       (let* ((fd (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
              (path (physical-image-fd-path fd)))
         (rove:ok (equal path (wal-ensure-directory-durable path fd)))
         (rove:ok (rove:signals
                   (wal-ensure-directory-durable (merge-pathnames "absent/" root) fd) 'error))
         (rove:ok (not (probe-file (merge-pathnames "absent/" root))))
         (let ((*physical-image-context* nil))
           (rove:ok (rove:signals (wal-ensure-directory-durable path fd) 'error)))
         (unwind-protect
              (progn
                (setf (physical-image-context-active context) nil)
                (rove:ok (rove:signals (wal-ensure-directory-durable path fd) 'error)))
           (setf (physical-image-context-active context) t))))))

(rove:deftest held-promotion-history-is-exclusive-and-rejects-directory-substitution
    (call-with-unit-promotion-directory
     (lambda (context root)
       (let* ((fd (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
              (path (timeline-history-path 2 *pgdata*))
              (bytes (timeline-encode-history 2 1 64 "test" nil))
              (named (merge-pathnames "pg_wal" root))
              (parked (merge-pathnames "parked" root)))
         (timeline-publish-history-bytes path bytes :directory-fd fd)
         (rove:ok (equalp bytes (read-bounded-regular-file path 4096)))
         (rove:ok (rove:signals (timeline-publish-history-bytes path bytes :directory-fd fd) 'error))
         (unwind-protect
              (progn
                (sb-posix:rename (namestring named) (namestring parked))
                (sb-posix:mkdir (namestring named) #o700)
                (rove:ok
                 (rove:signals
                  (wal-install-timeline-prefix 1 3 0 (make-array 0 :element-type '(unsigned-byte 8))
                                               :timeline fd) 'error))
                (rove:ok (null (directory-entry-names (merge-pathnames "pg_wal/" root))))
                (rove:ok (not (probe-file (merge-pathnames
                                           (wal-segment-filename 0 3 :timeline)
                                           (merge-pathnames "parked/" root))))))
           (sb-posix:rmdir (namestring named))
           (sb-posix:rename (namestring parked) (namestring named)))))))

(rove:deftest held-promotion-publication-stays-on-the-open-directory-after-a-late-swap
    (dolist (kind '(:prefix :history))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (let* ((fd (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (named (merge-pathnames "pg_wal" root))
                (parked (merge-pathnames "parked" root))
                (moved nil)
                (bytes (if (eq kind :prefix)
                           (make-array 64 :element-type '(unsigned-byte 8) :initial-element 17)
                           (timeline-encode-history 2 1 64 "test" nil)))
                (leaf (if (eq kind :prefix)
                          (wal-segment-filename 0 2 :timeline) (timeline-history-filename 2))))
           (flet ((swap (point)
                    (when (and (not moved)
                               (eq point (if (eq kind :prefix) :after-prefix-fsync :after-file-fsync)))
                      (sb-posix:rename (namestring named) (namestring parked))
                      (sb-posix:mkdir (namestring named) #o700)
                      (setf moved t))))
             (unwind-protect
                  (let ((*wal-timeline-test-hook* #'swap)
                        (*timeline-history-test-hook* #'swap))
                    (if (eq kind :prefix)
                        (wal-install-timeline-prefix 1 2 64 bytes :timeline fd)
                        (timeline-publish-history-bytes (timeline-history-path 2 *pgdata*) bytes
                                                        :directory-fd fd))
                    (rove:ok moved)
                    (rove:ok (null (directory-entry-names (merge-pathnames "pg_wal/" root))))
                    (rove:ok (equalp bytes (read-bounded-regular-file
                                            (merge-pathnames leaf (merge-pathnames "parked/" root)) 4096))))
               (when moved
                 (sb-posix:rmdir (namestring named))
                 (sb-posix:rename (namestring parked) (namestring named))))))))))
