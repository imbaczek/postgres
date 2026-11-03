(in-package #:pg-physical-tests)

(defun call-with-reseed-inventory-fixture (function)
  (call-with-reseed-owner-directory
   (lambda (root)
     (pg::call-with-physical-reseed-owner
      root
      (lambda (owner)
        (let ((record (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner))))
          (funcall function root owner record)))))))

(defun reseed-inventory-test-row (name &optional (mode #o100600))
  (list name 1 7 mode 2 1000 13 20 30))

(cl-spec:defproperty reseed-inventory-keeps-hardlink-names-and-allows-rename-ctime
    ((inode (range integer 1 18446744073709551615))
     (ctime (range integer -9223372036854775808 9223372036854775807)))
  (:about pg::physical-reseed-install-old-copy pg::physical-reseed-install-old-row-matches-p)
  (:trials (:normal 100))
  (let* ((a (reseed-inventory-test-row "a"))
         (b (reseed-inventory-test-row "b"))
         (current (copy-tree a)))
    (setf (third a) inode (third b) inode (third current) inode (ninth current) ctime)
    (let ((rows (pg::physical-reseed-install-old-copy (list a b) 1)))
      (and (equal rows (list a b))
           (not (eq (caar rows) (first a)))
           (pg::physical-reseed-install-old-row-matches-p a current)))))

(deftest generated-reseed-inventory-hardlinks-and-rename-ctime
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-inventory-keeps-hardlink-names-and-allows-rename-ctime
                                       :seed 97 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest reseed-inventory-codec-refuses-unsafe-or-ambiguous-membership
    (let ((a (reseed-inventory-test-row "a")) (b (reseed-inventory-test-row "b")))
      (ok (equal (list a b) (pg::physical-reseed-install-old-copy (list a b) 1)))
      (ok (null (pg::physical-reseed-install-old-copy nil 1)))
      (dolist (rows (list (list b a) (list a a) (list (butlast a)) (list (append a '(0)))))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-copy rows 1)))))
      (dolist (name (list "" "." ".." "base/child" "/absolute" ".pg_lisp.lock"
                          "physical-reseed.attempt" "physical-reseed.attempt.tmp"
                          "physical-reseed.install" "physical-reseed.install.tmp"
                          (string (code-char 0))))
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-install-old-copy (list (reseed-inventory-test-row name)) 1)))))
      (dolist (change '((1 2) (2 0) (3 0) (4 0) (5 -1) (6 9223372036854775808) (7 39614081257132168796771975168)))
        (let ((bad (copy-tree a)))
          (setf (nth (first change) bad) (second change))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-copy (list bad) 1))))))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-install-old-copy (list a) 1 :forbidden '((1 7))))))))

(deftest reseed-inventory-cross-step-matches-preserve-membership-not-rename-metadata
    (dolist (mode '(#o100600 #o120777 #o010600 #o140600 #o020600 #o060600 #o040700))
      (let* ((before (reseed-inventory-test-row "opaque" mode)) (after (copy-tree before)))
        (incf (ninth after))
        (ok (pg::physical-reseed-install-old-row-matches-p before after))
        (dolist (index '(0 1 2 3 5))
          (let ((changed (copy-tree after)))
            (setf (nth index changed) (if (zerop index) "another" (1+ (nth index changed))))
            (ok (not (pg::physical-reseed-install-old-row-matches-p before changed)))))
        (dolist (index '(4 6 7))
          (let ((changed (copy-tree after)))
            (incf (nth index changed))
            (ok (eq (not (null (sb-posix:s-isdir mode)))
                    (not (null (pg::physical-reseed-install-old-row-matches-p before changed))))))))))

(deftest reseed-inventory-snapshot-preserves-opaque-files-links-and-unreadable-subtrees
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let ((file (merge-pathnames "dirty" root)) (dir (merge-pathnames "base/" root))
                                                   (fifo (merge-pathnames "socket-like-fifo" root)))
         (with-open-file (output file :direction :output :element-type '(unsigned-byte 8))
           (write-sequence #(0 255 128 17) output))
         (sb-posix:chmod (namestring file) 0)
         (sb-posix:link (namestring file) (namestring (merge-pathnames "dirty-alias" root)))
         (sb-posix:mkdir (namestring dir) #o700)
         (with-open-file (output (merge-pathnames "invalid-control" dir) :direction :output)
           (write-string "not a database and must never be parsed" output))
         (sb-posix:chmod (namestring dir) 0)
         (sb-posix:symlink "/does-not-exist/outside" (namestring (merge-pathnames "pg_wal" root)))
         (sb-posix:mkfifo (namestring fifo) #o600)
         (unwind-protect
              (let* ((rows (pg::physical-reseed-install-old-snapshot owner record))
                     (names (mapcar #'first rows))
                     (expected '("base" "dirty" "dirty-alias" "pg_wal" "socket-like-fifo")))
                (ok (equal expected names))
                (ok (= (third (assoc "dirty" rows :test #'equal))
                       (third (assoc "dirty-alias" rows :test #'equal))))
                (ok (sb-posix:s-islnk (fourth (assoc "pg_wal" rows :test #'equal))))
                (ok (sb-posix:s-isfifo (fourth (assoc "socket-like-fifo" rows :test #'equal))))
                (ok (equal rows (pg::physical-reseed-install-old-snapshot owner record)))
                (ok (equal record (pg::physical-reseed-attempt-read owner)))
                (ok (not (probe-file (pg::physical-reseed-install-path root)))))
           (sb-posix:chmod (namestring dir) #o700)
           (sb-posix:chmod (namestring file) #o600))))))

(deftest reseed-inventory-snapshot-rejects-management-hardlinks-without-following-symlinks
    (dolist (name '(".pg_lisp.lock" "physical-reseed.attempt"))
      (call-with-reseed-inventory-fixture
       (lambda (root owner record)
         (let ((alias (merge-pathnames "alias" root)) (source (merge-pathnames name root)))
           (sb-posix:link (namestring source) (namestring alias))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner record))))
           (sb-posix:unlink (namestring alias))
           (sb-posix:symlink name (namestring alias))
           (ok (equal '("alias") (mapcar #'first (pg::physical-reseed-install-old-snapshot owner record)))))))))

(deftest reseed-inventory-snapshot-refuses-concurrent-namespace-or-content-changes
    (dolist (change '(:add :remove :replace :write))
      (call-with-reseed-inventory-fixture
       (lambda (root owner record)
         (let ((file (merge-pathnames "dirty" root)))
           (with-open-file (output file :direction :output) (write-string "old" output))
           (let* ((pg::*physical-reseed-install-inventory-test-hook*
                    (lambda (point directory)
                      (declare (ignore directory))
                      (when (eq point :after-inventory)
                        (ecase change
                          (:add (with-open-file (output (merge-pathnames "new" root) :direction :output)
                                  (write-string "new" output)))
                          (:remove (sb-posix:unlink (namestring file)))
                          (:replace
                           (rename-file file (merge-pathnames "old-renamed" root))
                           (with-open-file (output file :direction :output) (write-string "new" output)))
                          (:write (with-open-file (output file :direction :output :if-exists :overwrite)
                                    (write-string "changed-length" output)))))))
                  (message (handler-case
                               (progn (pg::physical-reseed-install-old-snapshot owner record) "accepted")
                             (error (condition) (princ-to-string condition)))))
             (ok (search "reseed old inventory changed" message) (format nil "~s ~a" change message))
             (ok (equal record (pg::physical-reseed-attempt-read owner)))))))))

(deftest reseed-inventory-snapshot-rechecks-owner-mount-and-attempt
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (with-open-file (output (merge-pathnames "dirty" root) :direction :output) (write-string "old" output))
       (let ((original (symbol-function 'pg::physical-adoption-statx)) (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-adoption-statx)
                      (lambda (fd name flags mask buffer)
                        (let ((result (funcall original fd name flags mask buffer)))
                          (when (equal name "dirty")
                            (incf calls)
                            (setf (sb-sys:sap-ref-64 (sb-alien:alien-sap buffer) #x90) 999999))
                          result)))
                (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner record))))
                (ok (plusp calls)))
           (setf (symbol-function 'pg::physical-adoption-statx) original)))
       (let ((pg::*physical-reseed-install-inventory-test-hook*
               (lambda (point directory)
                 (declare (ignore directory))
                 (when (eq point :after-inventory)
                   (let ((next (copy-tree record)))
                     (setf (getf next :phase) :aborting)
                     (pg::physical-reseed-attempt-publish owner next :previous record))))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner record))))
         (ok (eq :aborting (getf (pg::physical-reseed-attempt-read owner) :phase)))))))

(deftest reseed-inventory-rejects-unrepresentable-filenames-before-publication
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (flet ((python (code)
                (let ((process (sb-ext:run-program "/usr/bin/python3"
                                                   (list "-c" code (namestring root)) :wait t)))
                  (unwind-protect
                       (ok (zerop (sb-ext:process-exit-code process)))
                    (sb-ext:process-close process)))))
         (python "import os,sys; os.close(os.open(os.fsencode(sys.argv[1])+b'/'+bytes([255]),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600))")
         (unwind-protect
              (progn
                (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner record))))
                (ok (equal record (pg::physical-reseed-attempt-read owner)))
                (ok (not (probe-file (pg::physical-reseed-install-path root)))))
           (python "import os,sys; os.unlink(os.fsencode(sys.argv[1])+b'/'+bytes([255]))"))))))

(deftest reseed-inventory-rejects-replaced-root-without-damaging-owner
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let* ((moved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) ".moved/")))
              (changed nil)
              (pg::*physical-reseed-install-inventory-test-hook*
                (lambda (point directory)
                  (declare (ignore directory))
                  (when (eq point :after-inventory)
                    (rename-file root moved)
                    (setf changed t)
                    (sb-posix:mkdir (namestring root) #o700)))))
         (unwind-protect
              (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner record))))
           (when changed
             (sb-posix:rmdir (string-right-trim "/" (namestring root)))
             (rename-file moved root)))
         (ok (eq owner (pg::physical-reseed-check-owner owner)))
         (ok (equal record (pg::physical-reseed-attempt-read owner)))))))

(deftest reseed-inventory-keeps-original-stopped-pid-bytes-and-inode
    (call-with-reaped-reseed-pid
     (lambda (dead)
       (call-with-reseed-owner-directory
        (lambda (root)
          (let* ((path (reseed-test-write-pid root (- dead)))
                 (bytes (lineage-test-bytes path))
                 (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (let* ((record (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))
                      (rows (pg::physical-reseed-install-old-snapshot owner record)))
                 (ok (equal '("postmaster.pid") (mapcar #'first rows)))
                 (ok (= inode (third (first rows))))
                 (ok (equalp bytes (lineage-test-bytes path)))
                 (ok (eq owner (pg::physical-reseed-check-owner owner))))))))))))

(deftest reseed-inventory-cross-step-comparison-accepts-real-hardlink-rename
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (with-physical-store-directory (quarantine)
         (sb-posix:chmod (namestring quarantine) #o700)
         (let ((a (merge-pathnames "a" root)) (b (merge-pathnames "b" root))
                                              (root-fd nil) (quarantine-fd nil))
           (with-open-file (output a :direction :output) (write-string "opaque old bytes" output))
           (sb-posix:link (namestring a) (namestring b))
           (let ((rows (pg::physical-reseed-install-old-snapshot owner record)))
             (unwind-protect
                  (progn
                    (setf root-fd (pg::physical-backup-open-directory root)
                          quarantine-fd (pg::physical-backup-open-directory quarantine))
                    (let ((mount (pg::physical-adoption-install-mount-id nil :directory-fd root-fd)))
                      (sleep 0.002)
                      (sb-posix:rename (namestring a) (namestring (merge-pathnames "a" quarantine)))
                      (let ((current-a (pg::physical-reseed-install-old-observe quarantine-fd "a" mount))
                            (current-b (pg::physical-reseed-install-old-observe root-fd "b" mount)))
                        (ok (pg::physical-reseed-install-old-row-matches-p (first rows) current-a))
                        (ok (pg::physical-reseed-install-old-row-matches-p (second rows) current-b))
                        (ok (= (third current-a) (third current-b)))
                        (ok (/= (ninth (first rows)) (ninth current-a)))
                        (ok (= (ninth current-a) (ninth current-b))))))
               (when root-fd (sb-posix:close root-fd))
               (when quarantine-fd (sb-posix:close quarantine-fd)))))))))

(deftest reseed-inventory-directory-enumeration-enforces-entry-and-encoded-name-bounds
    (with-physical-store-directory (root)
      (dolist (name '("a" "bb" "é"))
        (with-open-file (output (merge-pathnames name root) :direction :output)
          (write-string "opaque" output)))
      (ok (equal '("a" "bb" "é")
                 (sort (pg::directory-entry-names root :maximum-entries 3 :maximum-name-bytes 5) #'string<)))
      (dolist (arguments '((:maximum-entries 0) (:maximum-entries 2) (:maximum-name-bytes 4)))
        (let ((message (handler-case (progn (apply #'pg::directory-entry-names root arguments) "accepted")
                         (error (condition) (princ-to-string condition)))))
          (ok (search "directory inventory exceeds" message))))
      (dolist (arguments '((:maximum-entries -1) (:maximum-name-bytes -1) (:maximum-entries :invalid)))
        (ok (intake-fails-p (lambda () (apply #'pg::directory-entry-names root arguments)))))
      (ok (= 3 (length (pg::directory-entry-names root))))))

(deftest reseed-inventory-observations-match-independent-posix-stat-fields
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let ((path (merge-pathnames "opaque" root)))
         (with-open-file (output path :direction :output) (write-string "old bytes" output))
         (sb-posix:chmod (namestring path) #o641)
         (let ((stat (sb-posix:lstat (namestring path)))
               (row (first (pg::physical-reseed-install-old-snapshot owner record))))
           (ok (= (sb-posix:stat-dev stat) (second row)))
           (ok (= (sb-posix:stat-ino stat) (third row)))
           (ok (= (sb-posix:stat-mode stat) (fourth row)))
           (ok (= (sb-posix:stat-nlink stat) (fifth row)))
           (ok (= (sb-posix:stat-uid stat) (sixth row)))
           (ok (= (sb-posix:stat-size stat) (seventh row)))
           (ok (= (sb-posix:stat-mtime stat) (floor (eighth row) 1000000000)))
           (ok (= (sb-posix:stat-ctime stat) (floor (ninth row) 1000000000))))))))
