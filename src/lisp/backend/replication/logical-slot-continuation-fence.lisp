(in-package #:pg)

(defvar *logical-slot-continuation-fenced-roots*
  (make-hash-table :test #'equal :synchronized t))

(defun logical-slot-continuation-gc-fenced-p (directory)
  ;; Once the feature is seen, the fence stays closed for this PGDATA
  ;; device, inode and mount, including after empty restoration.  There is no
  ;; opening path yet.
  (let ((root (uiop:ensure-directory-pathname directory)))
    (when (slot-state-lstat root)
      (let* ((facts (physical-image-file-facts -100 (namestring (truename root))))
             (key (list (first facts) (second facts) (ninth facts))))
        (or (gethash key *logical-slot-continuation-fenced-roots*)
            (when (or (slot-state-lstat (merge-pathnames "logical-slot-copy.source-proof" root))
                      (slot-state-lstat (merge-pathnames "pg_logical_continuation" root)))
              (setf (gethash key *logical-slot-continuation-fenced-roots*) t)))))))