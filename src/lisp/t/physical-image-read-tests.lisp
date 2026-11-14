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

(defun call-with-image-cleanup-held-scope (borrowed function)
  (pg::call-with-physical-backup-image-directory
   borrowed
   (lambda (path)
     (declare (ignore path))
     (let ((outer pg::*physical-image-context*))
       (pg::call-with-physical-reseed-follow-held-base-directory
        (pg::physical-image-context-root-fd outer)
        (cdr (assoc "base" (pg::physical-image-context-children outer) :test #'equal))
        (cdr (assoc "global" (pg::physical-image-context-children outer) :test #'equal))
        (cdr (assoc "pg_wal" (pg::physical-image-context-children outer) :test #'equal))
        function (lambda () (pg::physical-image-check-children outer)))))))

(deftest physical-image-cleanup-ignores-mutated-descriptor-observations
    (dolist (case '((:root nil) (:children nil) (:entry nil) (:extra nil)
                    (:root t) (:children t) (:entry t) (:extra t)))
      (call-with-physical-image-read-fixture
       (lambda (root borrowed)
         (declare (ignore root))
         (let ((kind (first case))
               (scope (if (second case) #'call-with-image-cleanup-held-scope
                          #'pg::call-with-physical-backup-image-directory))
               (foreign (sb-posix:fcntl borrowed 1030 0))
               (owned nil) (context nil))
           (unwind-protect
                (progn
                  (ok (eq :body-cut
                          (catch 'image-cleanup-cut
                            (funcall scope
                                     borrowed
                                     (lambda (path)
                                       (declare (ignore path))
                                       (setf context pg::*physical-image-context*
                                             owned (cons (pg::physical-image-context-root-fd context)
                                                         (mapcar #'cdr (pg::physical-image-context-children context))))
                                       (ecase kind
                                         (:root (setf (pg::physical-image-context-root-fd context) foreign))
                                         (:children (setf (pg::physical-image-context-children context) nil))
                                         (:entry (setf (cdar (pg::physical-image-context-children context)) foreign))
                                         (:extra (push (cons "foreign" foreign) (pg::physical-image-context-children context))))
                                       (throw 'image-cleanup-cut :body-cut))))))
                  (ok (not (pg::physical-image-context-active context)))
                  (ok (not (intake-fails-p (lambda () (sb-posix:fstat foreign)))))
                  (ok (not (intake-fails-p (lambda () (sb-posix:fstat borrowed)))))
                  (dolist (fd owned)
                    (ok (intake-fails-p (lambda () (sb-posix:fstat fd))))))
             (dolist (fd (cons foreign owned))
               (ignore-errors (sb-posix:close fd)))))))))

(deftest physical-image-cleanup-drains-after-close-errors-and-preserves-primary-exit
    (dolist (case '((:normal 0 nil) (:normal 1 nil) (:normal 2 nil) (:normal 3 nil)
                    (:throw 1 nil) (:error 1 nil) (:normal 1 t) (:throw 2 t) (:error 0 t)))
      (call-with-physical-image-read-fixture
       (lambda (root borrowed)
         (declare (ignore root))
         (let* ((mode (first case))
                (scope (if (third case) #'call-with-image-cleanup-held-scope
                           #'pg::call-with-physical-backup-image-directory))
                (original (symbol-function 'sb-posix:close))
                (foreign (sb-posix:fcntl borrowed 1030 0))
                (owned nil) (counts nil) (injected nil) (reused nil)
                (body-error (make-condition 'simple-error :format-control "primary image failure"))
                (close-error (make-condition 'simple-error :format-control "image close failure")))
           (unwind-protect
                (let ((result
                        (handler-case
                            (catch 'image-primary-cut
                              (funcall scope
                                       borrowed
                                       (lambda (path)
                                         (declare (ignore path))
                                         (setf owned (cons (pg::physical-image-context-root-fd pg::*physical-image-context*)
                                                           (mapcar #'cdr (pg::physical-image-context-children pg::*physical-image-context*))))
                                         (setf (symbol-function 'sb-posix:close)
                                               (lambda (fd)
                                                 (when (member fd owned) (push fd counts))
                                                 (prog1 (funcall original fd)
                                                   (when (and (eql fd (nth (second case) owned)) (not injected))
                                                     (setf injected t reused fd)
                                                     (sb-posix:dup2 foreign fd)
                                                     (error close-error)))))
                                         (ecase mode
                                           (:normal (values :one :two))
                                           (:throw (throw 'image-primary-cut :body-cut))
                                           (:error (error body-error)))))
                              :returned)
                          (error (condition) condition))))
                  (ok injected)
                  (ok (eq result (ecase mode (:normal close-error) (:throw :body-cut) (:error body-error))))
                  (dolist (fd owned)
                    (ok (= 1 (count fd counts)))
                    (unless (eql fd reused)
                      (ok (intake-fails-p (lambda () (sb-posix:fstat fd))))))
                  (ok (and reused (not (intake-fails-p (lambda () (sb-posix:fstat reused))))))
                  (ok (not (intake-fails-p (lambda () (sb-posix:fstat foreign)))))
                  (ok (not (intake-fails-p (lambda () (sb-posix:fstat borrowed))))))
             (setf (symbol-function 'sb-posix:close) original)
             (dolist (fd (cons foreign owned))
               (ignore-errors (funcall original fd)))))))))

(deftest physical-image-owned-context-rejects-copies-expiry-and-shortened-inventory
    (call-with-physical-image-read-fixture
     (lambda (root borrowed)
       (declare (ignore root))
       (let ((saved nil) (owned nil))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-backup-image-directory
                 borrowed
                 (lambda (path)
                   (declare (ignore path))
                   (setf saved pg::*physical-image-context*
                         owned (cons (pg::physical-image-context-root-fd saved)
                                     (mapcar #'cdr (pg::physical-image-context-children saved))))
                   (let ((copy (copy-structure saved)))
                     (ok (intake-fails-p (lambda () (pg::physical-image-check-context copy))))
                     (ok (intake-fails-p
                          (lambda () (pg::physical-image-register-child copy "foreign" borrowed)))))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-image-register-child saved "foreign" borrowed))))
                   (setf (pg::physical-image-context-children saved) nil))))))
         (dolist (fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
         (setf (pg::physical-image-context-active saved) t)
         (ok (intake-fails-p (lambda () (pg::physical-image-check-context saved))))
         (ok (intake-fails-p
              (lambda () (pg::physical-image-register-child saved "foreign" borrowed))))
         (ok (not (intake-fails-p (lambda () (sb-posix:fstat borrowed)))))))))

(deftest physical-image-cleanup-drains-after-a-close-nonlocal-exit
    (call-with-physical-image-read-fixture
     (lambda (root borrowed)
       (declare (ignore root))
       (let ((original (symbol-function 'sb-posix:close))
             (foreign (sb-posix:fcntl borrowed 1030 0))
             (owned nil) (counts nil) (reused nil) (saved nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'sb-posix:close)
                      (lambda (fd)
                        (when (member fd owned) (push fd counts))
                        (prog1 (funcall original fd)
                          (when (and (member fd owned) (null reused))
                            (setf reused fd)
                            (sb-posix:dup2 foreign fd)
                            (ok (not (pg::physical-image-context-active saved)))
                            (throw 'image-close-cut :close-cut)))))
                (ok (eq :close-cut
                        (catch 'image-close-cut
                          (pg::call-with-physical-backup-image-directory
                           borrowed
                           (lambda (path)
                             (declare (ignore path))
                             (setf saved pg::*physical-image-context*
                                   owned (cons (pg::physical-image-context-root-fd saved)
                                               (mapcar #'cdr (pg::physical-image-context-children saved))))))
                          :returned)))
                (dolist (fd owned)
                  (ok (= 1 (count fd counts)))
                  (unless (eql fd reused)
                    (ok (intake-fails-p (lambda () (sb-posix:fstat fd))))))
                (ok (and reused (not (intake-fails-p (lambda () (sb-posix:fstat reused))))))
                (ok (not (intake-fails-p (lambda () (sb-posix:fstat borrowed))))))
           (setf (symbol-function 'sb-posix:close) original)
           (dolist (fd (cons foreign owned))
             (ignore-errors (funcall original fd))))))))

(deftest physical-image-cleanup-retains-acquisitions-before-validation-failure
    (dolist (cut '(1 2 3 4))
      (call-with-physical-image-read-fixture
       (lambda (root borrowed)
         (declare (ignore root))
         (let ((original (symbol-function 'pg::physical-image-check-directory))
               (acquired nil) (entered nil) (calls 0) (injected nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-image-check-directory)
                        (lambda (fd &optional device)
                          (prog1 (funcall original fd device)
                            (pushnew fd acquired)
                            (when (= cut (incf calls))
                              (setf injected t)
                              (error "image validation cut")))))
                  (ok (intake-fails-p
                       (lambda ()
                         (pg::call-with-physical-backup-image-directory
                          borrowed
                          (lambda (path) (declare (ignore path)) (setf entered t))))))
                  (ok injected)
                  (ok (not entered))
                  (ok (= cut (length acquired)))
                  (dolist (fd acquired)
                    (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
                  (ok (not (intake-fails-p (lambda () (sb-posix:fstat borrowed))))))
             (setf (symbol-function 'pg::physical-image-check-directory) original)))))))

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
                    (:public (sb-posix:mkdir (namestring path) #o755)
                     (sb-posix:chmod (namestring path) #o755))
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

