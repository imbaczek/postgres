(in-package #:pg-physical-tests)

(defun call-with-physical-image-copy-fixture (function &key version-bytes)
  (call-with-physical-image-read-fixture
   (lambda (source source-fd)
     (with-physical-store-directory (target)
       (sb-posix:chmod (namestring target) #o700)
       (let ((target-fd (pg::physical-backup-open-directory target)))
         (unwind-protect
              (pg::call-with-physical-backup-image-directory
               source-fd
               (lambda (source-root)
                 (declare (ignore source-root))
                 (let ((source-context pg::*physical-image-context*))
                   (pg::call-with-physical-backup-receive-directory
                    target-fd
                    (lambda (target-root)
                      (funcall function source-context pg::*physical-image-context*
                               source target target-root))))))
           (sb-posix:close target-fd)))))
   :version-bytes version-bytes))

(deftest physical-image-copy-uses-explicit-scopes-and-independent-inodes
    (call-with-physical-image-copy-fixture
     (lambda (source-context target-context source target target-root)
       (dolist (entry '(("PG_VERSION" 17) ("global/pg_control" 42)))
         (let* ((name (first entry))
                (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element (second entry)))
                (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
                (from (merge-pathnames name source))
                (to (merge-pathnames name target)))
           (ok (equal (list name 1 digest)
                      (pg::physical-image-copy-file source-context target-context name 1 digest)))
           (ok (eq target-context pg::*physical-image-context*))
           (pg::with-physical-image-input
               (input (pg::pgdata-resolve-path name target-root) :element-type '(unsigned-byte 8))
             (ok (= (second entry) (read-byte input))))
           (ok (equalp (lineage-test-bytes from) (lineage-test-bytes to)))
           (ok (/= (sb-posix:stat-ino (sb-posix:lstat (namestring from)))
                   (sb-posix:stat-ino (sb-posix:lstat (namestring to)))))))
       (ok (intake-fails-p
            (lambda ()
              (pg::physical-image-copy-file target-context source-context "PG_VERSION" 1 0)))))))

(cl-spec:defproperty physical-image-copy-preserves-bytes-without-sharing-inodes
    ((size (range integer 0 131080)) (salt (range integer 0 255)))
  (:about pg::physical-image-copy-file)
  (:trials (:normal 40))
  (let ((bytes (make-array size :element-type '(unsigned-byte 8)))
        (valid nil))
    (loop for index below size do (setf (aref bytes index) (mod (+ index salt) 251)))
    (call-with-physical-image-copy-fixture
     (lambda (source-context target-context source target target-root)
       (declare (ignore target-root))
       (let* ((from (merge-pathnames "PG_VERSION" source))
              (to (merge-pathnames "PG_VERSION" target))
              (inode (sb-posix:stat-ino (sb-posix:lstat (namestring from))))
              (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
         (setf valid
               (and (equal (list "PG_VERSION" size digest)
                           (pg::physical-image-copy-file
                            source-context target-context "PG_VERSION" size digest))
                    (equalp bytes (lineage-test-bytes from))
                    (equalp bytes (lineage-test-bytes to))
                    (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring from))))
                    (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring to))))))))
     :version-bytes bytes)
    valid))

(deftest generated-physical-image-copy-content-and-inode-independence
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-image-copy-preserves-bytes-without-sharing-inodes
                                       :seed 96 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest physical-image-copy-handles-chunk-boundaries-and-checksum-or-length-errors
    (dolist (size '(0 1 65535 65536 65537 131079))
      (let ((bytes (make-array size :element-type '(unsigned-byte 8) :initial-element 73)))
        (call-with-physical-image-copy-fixture
         (lambda (source-context target-context source target target-root)
           (declare (ignore source target-root))
           (let ((digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
             (ok (equal (list "PG_VERSION" size digest)
                        (pg::physical-image-copy-file
                         source-context target-context "PG_VERSION" size digest)))
             (ok (equalp bytes (lineage-test-bytes (merge-pathnames "PG_VERSION" target))))))
         :version-bytes bytes)))
  (dolist (kind '(:length :checksum))
    (call-with-physical-image-copy-fixture
     (lambda (source-context target-context source target target-root)
       (declare (ignore target-root))
       (let* ((from (merge-pathnames "PG_VERSION" source))
              (to (merge-pathnames "PG_VERSION" target))
              (bytes (lineage-test-bytes from))
              (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
         (ok (intake-fails-p
              (lambda ()
                (pg::physical-image-copy-file
                 source-context target-context "PG_VERSION"
                 (if (eq kind :length) 2 1)
                 (if (eq kind :checksum) (logxor 1 digest) digest)))))
         (ok (equalp bytes (lineage-test-bytes from)))
         (if (eq kind :length)
             (ok (not (probe-file to)))
             (ok (equalp bytes (lineage-test-bytes to)))))))))
