(in-package #:pg-physical-tests)

(defun call-with-physical-image-read-fixture (function &key version-bytes)
  (with-physical-store-directory (root)
    (sb-posix:chmod (namestring root) #o700)
    (let ((fd (pg::physical-backup-open-directory root)))
      (unwind-protect
           (progn
             (pg::call-with-physical-backup-receive-directory
              fd
              (lambda (anchored)
                (dolist (entry '(("PG_VERSION" 17) ("global/pg_control" 42)))
                  (pg::call-with-physical-image-output
                   anchored (first entry) (lambda (output)
                                            (if (and version-bytes (equal (first entry) "PG_VERSION"))
                                                (write-sequence version-bytes output)
                                                (write-byte (second entry) output)))))))
             (funcall function root fd))
        (sb-posix:close fd)))))

(deftest physical-image-read-scope-opens-existing-files-without-output-authority
    (call-with-physical-image-read-fixture
     (lambda (root fd)
       (let* ((path (merge-pathnames "global/pg_control" root))
              (bytes (lineage-test-bytes path))
              (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
              (entered-output nil)
              (created nil)
              (pg::*physical-image-test-hook*
                (lambda (point name)
                  (declare (ignore name))
                  (when (eq point :after-mkdir) (setf created t)))))
         (multiple-value-bind (first second)
             (pg::call-with-physical-backup-image-directory
              fd
              (lambda (anchored)
                (ok (pg::physical-image-read-context-p pg::*physical-image-context*))
                (let ((message
                        (handler-case
                            (progn (pg::physical-image-open-child pg::*physical-image-context* "global")
                                   "directory creation accepted")
                          (error (condition) (princ-to-string condition)))))
                  (ok (search "read scope cannot create directories" message)))
                (dolist (entry '(("PG_VERSION" 17) ("global/pg_control" 42)))
                  (pg::with-physical-image-input
                      (input (pg::pgdata-resolve-path (first entry) anchored)
                             :element-type '(unsigned-byte 8))
                    (ok (= (second entry) (read-byte input)))
                    (ok (eq :eof (read-byte input nil :eof)))))
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-physical-image-output
                        anchored "unexpected" (lambda (output)
                                                (declare (ignore output))
                                                (setf entered-output t))))))
                (values :read 42)))
           (ok (eq :read first))
           (ok (= 42 second)))
         (ok (not entered-output))
         (ok (not created))
         (ok (not (probe-file (merge-pathnames "unexpected" root))))
         (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
         (ok (equalp bytes (lineage-test-bytes path)))
         (ok (sb-posix:fstat fd))))))

(deftest physical-image-input-at-preserves-values-and-borrowed-parent-across-exits
    (call-with-physical-image-read-fixture
     (lambda (root parent)
       (declare (ignore root))
       (let ((owned nil))
         (multiple-value-bind (first second)
             (pg::call-with-physical-image-input-at
              parent "PG_VERSION"
              (lambda (input)
                (setf owned (sb-sys:fd-stream-fd input))
                (values (read-byte input) :read))
              :element-type '(unsigned-byte 8))
           (ok (= 17 first))
           (ok (eq :read second)))
         (ok (intake-fails-p (lambda () (sb-posix:fstat owned))))
         (ok (sb-posix:fstat parent))
         (ok (eq :cut
                 (catch 'input-at-cut
                   (pg::call-with-physical-image-input-at
                    parent "PG_VERSION"
                    (lambda (input)
                      (setf owned (sb-sys:fd-stream-fd input))
                      (throw 'input-at-cut :cut))
                    :element-type '(unsigned-byte 8)))))
         (ok (intake-fails-p (lambda () (sb-posix:fstat owned))))
         (ok (sb-posix:fstat parent))
         (ok (equal '(:missing 42)
                    (multiple-value-list
                     (pg::call-with-physical-image-input-at
                      parent "missing" (lambda (input) (values (if input :present :missing) 42))
                      :if-does-not-exist nil))))
         (ok (sb-posix:fstat parent))))))

(deftest physical-image-input-at-refuses-path-components-and-nonregular-files
    (call-with-physical-image-read-fixture
     (lambda (root parent)
       (let ((called nil))
         (flet ((refuses (leaf)
                  (intake-fails-p
                   (lambda ()
                     (pg::call-with-physical-image-input-at
                      parent leaf
                      (lambda (input) (declare (ignore input)) (setf called t)))))))
           (dolist (leaf (append '("" "." ".." "global/pg_control" "global/../PG_VERSION")
                                 (list (string (code-char 0)))))
             (ok (refuses leaf)))
           (sb-posix:symlink "PG_VERSION" (namestring (merge-pathnames "input-link" root)))
           (sb-posix:mkfifo (namestring (merge-pathnames "input-fifo" root)) #o600)
           (dolist (leaf '("input-link" "input-fifo" "global")) (ok (refuses leaf)))
           (sb-posix:link (namestring (merge-pathnames "PG_VERSION" root))
                          (namestring (merge-pathnames "input-hardlink" root)))
           (ok (refuses "input-hardlink"))
           (ok (not called))
           (ok (sb-posix:fstat parent)))))))

(deftest physical-image-read-scope-closes-owned-descriptors-on-nonlocal-exit
    (call-with-physical-image-read-fixture
     (lambda (root fd)
       (declare (ignore root))
       (let ((owned nil) (context nil))
         (ok (eq :cut
                 (catch 'read-scope-cut
                   (pg::call-with-physical-backup-image-directory
                    fd
                    (lambda (anchored)
                      (declare (ignore anchored))
                      (setf context pg::*physical-image-context*
                            owned (cons (pg::physical-image-context-root-fd context)
                                        (mapcar #'cdr (pg::physical-image-context-children context))))
                      (throw 'read-scope-cut :cut))))))
         (ok (= 4 (length owned)))
         (dolist (owned-fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat owned-fd)))))
         (ok (intake-fails-p (lambda () (pg::physical-image-check-context context))))
         (ok (sb-posix:fstat fd))))))

(deftest physical-image-read-scope-keeps-root-and-child-reads-anchored-after-replacement
    (dolist (kind '(:root :child))
      (call-with-physical-image-read-fixture
       (lambda (root fd)
         (with-physical-store-directory (outside)
           (let* ((from (if (eq kind :root) root (merge-pathnames "global/" root)))
                  (saved (if (eq kind :root)
                             (uiop:ensure-directory-pathname
                              (format nil "~a.reading/" (string-right-trim "/" (namestring root))))
                             (merge-pathnames "global-held/" root)))
                  (moved nil)
                  (read-complete nil)
                  (owned nil))
             (dolist (name '("PG_VERSION" "pg_control" "global/pg_control"))
               (ensure-directories-exist (merge-pathnames name outside))
               (with-open-file (output (merge-pathnames name outside) :direction :output
                                                                      :element-type '(unsigned-byte 8))
                 (write-byte 99 output)))
             (unwind-protect
                  (progn
                    (ok (intake-fails-p
                         (lambda ()
                           (pg::call-with-physical-backup-image-directory
                            fd
                            (lambda (anchored)
                              (let ((context pg::*physical-image-context*))
                                (setf owned (cons (pg::physical-image-context-root-fd context)
                                                  (mapcar #'cdr (pg::physical-image-context-children context)))))
                              (rename-file from saved)
                              (setf moved t)
                              (sb-posix:symlink (namestring outside)
                                                (string-right-trim "/" (namestring from)))
                              (dolist (entry '(("PG_VERSION" 17) ("global/pg_control" 42)))
                                (pg::with-physical-image-input
                                    (input (pg::pgdata-resolve-path (first entry) anchored)
                                           :element-type '(unsigned-byte 8))
                                  (ok (= (second entry) (read-byte input)))))
                              (setf read-complete t)))
                           (pg::physical-image-check-target root fd))))
                    (ok read-complete)
                    (ok (= 4 (length owned)))
                    (dolist (owned-fd owned)
                      (ok (intake-fails-p (lambda () (sb-posix:fstat owned-fd)))))
                    (ok (sb-posix:fstat fd))
                    (dolist (name '("PG_VERSION" "pg_control" "global/pg_control"))
                      (ok (equalp #(99) (lineage-test-bytes (merge-pathnames name outside))))))
               (when moved
                 (when (pg::physical-adoption-path-present-p from)
                   (sb-posix:unlink (string-right-trim "/" (namestring from))))
                 (rename-file saved from)))))))))

(deftest physical-image-read-scope-rejects-missing-or-unsafe-existing-child-directories
    (dolist (kind '(:missing :symlink :file :fifo :public :special-mode))
      (call-with-physical-image-read-fixture
       (lambda (root fd)
         (let ((path (merge-pathnames "global/" root))
               (saved (merge-pathnames "global-held/" root))
               (entered nil))
           (rename-file path saved)
           (unwind-protect
                (progn
                  (ecase kind
                    (:missing nil)
                    (:symlink (sb-posix:symlink (namestring saved)
                                                (string-right-trim "/" (namestring path))))
                    (:file (with-open-file (output (string-right-trim "/" (namestring path))
                                                   :direction :output)
                             (write-string "not a directory" output)))
                    (:fifo (sb-posix:mkfifo (string-right-trim "/" (namestring path)) #o600))
                    (:public (sb-posix:mkdir (namestring path) #o755))
                    (:special-mode (sb-posix:mkdir (namestring path) #o700)
                     (sb-posix:chmod (namestring path) #o2700)))
                  (ok (intake-fails-p
                       (lambda ()
                         (pg::call-with-physical-backup-image-directory
                          fd (lambda (anchored) (declare (ignore anchored)) (setf entered t))))))
                  (ok (not entered))
                  (ok (sb-posix:fstat fd)))
             (unless (eq kind :missing)
               (if (member kind '(:public :special-mode))
                   (sb-posix:rmdir (namestring path))
                   (sb-posix:unlink (string-right-trim "/" (namestring path)))))
             (rename-file saved path)))))))

