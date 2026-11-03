(in-package #:pg-physical-tests)

(deftest reseed-attempt-writer-never-creates-intent-in-a-replaced-root
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let* ((candidate (copy-tree record))
              (moved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) ".moved/")))
              (check (symbol-function 'pg::physical-reseed-attempt-check-owner))
              (fired nil))
         (setf (getf candidate :phase) :aborting)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-attempt-check-owner)
                      (lambda (actual value)
                        (prog1 (funcall check actual value)
                          (unless fired
                            (setf fired t)
                            (rename-file root moved)
                            (sb-posix:mkdir (namestring root) #o700)))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-write-temporary owner candidate))))
                (ok fired)
                (ok (null (pg::directory-entry-names root))))
           (setf (symbol-function 'pg::physical-reseed-attempt-check-owner) check)
           (when fired
             (uiop:delete-directory-tree root :validate t)
             (rename-file moved root)))
         (ok (equal record (pg::physical-reseed-attempt-read owner)))))))

(deftest reseed-attempt-promotion-never-renames-entries-in-a-replaced-root
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let* ((candidate (copy-tree record))
              (moved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) ".moved/")))
              (check (symbol-function 'pg::physical-reseed-attempt-check-previous))
              (fired nil)
              (final (pg::physical-reseed-attempt-path root))
              (temp (pg::physical-reseed-attempt-temporary-path root)))
         (setf (getf candidate :phase) :aborting)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-attempt-check-previous)
                      (lambda (actual previous identity)
                        (prog1 (funcall check actual previous identity)
                          (unless fired
                            (setf fired t)
                            (rename-file root moved)
                            (sb-posix:mkdir (namestring root) #o700)
                            (with-open-file (output final :direction :output) (write-string "foreign final" output))
                            (with-open-file (output temp :direction :output) (write-string "foreign temporary" output))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-publish owner candidate :previous record))))
                (ok fired)
                (ok (equalp (pg::string->bytes "foreign final") (lineage-test-bytes final)))
                (ok (probe-file temp)))
           (setf (symbol-function 'pg::physical-reseed-attempt-check-previous) check)
           (when fired
             (uiop:delete-directory-tree root :validate t)
             (rename-file moved root)))
         (ok (eq owner (pg::physical-reseed-check-owner owner)))))))

(deftest reseed-attempt-raw-reader-keeps-borrowed-root-and-fixed-name-boundary
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (declare (ignore owner))
       (let ((fd (pg::physical-backup-open-directory root)))
         (unwind-protect
              (progn
                (ok (equal record (pg::physical-reseed-attempt-read-at fd "physical-reseed.attempt")))
                (ok (equal record (pg::physical-reseed-attempt-read-at fd "physical-reseed.attempt" :stabilize t)))
                (dolist (name '("../physical-reseed.attempt" "postmaster.pid" "physical-reseed.install" ""))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-read-at fd name)))))
                (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))
           (sb-posix:close fd))))))

(deftest reseed-attempt-raw-reader-rejects-unsafe-or-malformed-metadata
    (dolist (kind '(:symlink :fifo :directory :public :hardlink :empty :invalid))
      (call-with-reseed-inventory-fixture
       (lambda (root owner record)
         (declare (ignore owner record))
         (let* ((path (pg::physical-reseed-attempt-path root))
                (saved (merge-pathnames "saved-private-attempt" root))
                (fd (pg::physical-backup-open-directory root)))
           (sb-posix:rename (namestring path) (namestring saved))
           (unwind-protect
                (progn
                  (ecase kind
                    (:symlink (sb-posix:symlink (namestring saved) (namestring path)))
                    (:fifo (sb-posix:mkfifo (namestring path) #o600))
                    (:directory (sb-posix:mkdir (namestring path) #o700))
                    (:hardlink (sb-posix:link (namestring saved) (namestring path)))
                    ((:public :empty :invalid)
                     (with-open-file (output path :direction :output)
                       (unless (eq kind :empty) (write-string "malformed intent" output)))
                     (sb-posix:chmod (namestring path) (if (eq kind :public) #o644 #o600))))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-read-at fd "physical-reseed.attempt"))))
                  (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))
             (sb-posix:close fd)
             (if (eq kind :directory) (sb-posix:rmdir (namestring path)) (sb-posix:unlink (namestring path)))
             (sb-posix:rename (namestring saved) (namestring path))))))))
