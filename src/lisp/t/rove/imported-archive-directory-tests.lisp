(in-package #:pg)

(defun call-with-imported-archive-directory-test (function)
  (let* ((root (uiop:ensure-directory-pathname
                (sb-posix:mkdtemp
                 (namestring (merge-pathnames "pg-archive-directory.XXXXXX" (uiop:temporary-directory))))))
         (directory (merge-pathnames "pg_physical_retained/" root))
         (fd nil))
    (ensure-directories-exist directory)
    (sb-posix:chmod (namestring directory) #o700)
    (unwind-protect
         (progn
           (setf fd (sb-posix:open (namestring root) (logior sb-posix:o-rdonly sb-posix:o-directory)))
           (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (funcall function directory fd)
             (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))
      (when fd (sb-posix:close fd))
      (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(rove:deftest imported-archive-directory-allows-unrelated-publication-only-in-explicit-mode
    (dolist (strict '(nil t))
      (call-with-imported-archive-directory-test
       (lambda (directory parent)
         (let ((entered nil))
           (flet ((publish (fd facts)
                    (setf entered t)
                    (with-open-file (out (merge-pathnames "unselected-object" directory)
                                         :direction :output :if-exists :error :if-does-not-exist :create)
                      (write-string "new immutable object" out))
                    (rove:ok (not (equal facts (physical-image-file-facts fd))))
                    (rove:ok (equal (logical-slot-copy-source-proof-directory-identity facts)
                                    (logical-slot-copy-source-proof-directory-identity
                                     (physical-image-file-facts fd))))))
             (if strict
                 (rove:ok (rove:signals
                           (call-with-logical-slot-copy-source-proof-fd
                            parent "pg_physical_retained" t #'publish) 'error))
                 (rove:ok (call-with-logical-slot-imported-archive-directory parent #'publish))))
           (rove:ok entered))))))

(rove:deftest imported-archive-directory-rejects-substitution-and-permission-change
    (dolist (kind '(:replacement :symlink :permission))
      (call-with-imported-archive-directory-test
       (lambda (directory parent)
         (let ((saved (merge-pathnames "archive-original/" (uiop:pathname-parent-directory-pathname directory)))
               (entered nil) (changed nil)
               (named (string-right-trim "/" (namestring directory))))
           (unwind-protect
                (rove:ok
                 (rove:signals
                  (call-with-logical-slot-imported-archive-directory
                   parent
                   (lambda (fd facts)
                     (declare (ignore fd facts))
                     (setf entered t)
                     (if (eq kind :permission)
                         (sb-posix:chmod (namestring directory) #o750)
                         (progn
                           (sb-posix:rename named (string-right-trim "/" (namestring saved)))
                           (if (eq kind :symlink)
                               (sb-posix:symlink (string-right-trim "/" (namestring saved)) named)
                               (sb-posix:mkdir named #o700))))
                     (setf changed t)))
                  'error))
             (if (eq kind :permission)
                 (sb-posix:chmod (namestring directory) #o700)
                 (when (probe-file saved)
                   (if (eq kind :symlink)
                       (when (slot-state-lstat named) (sb-posix:unlink named))
                       (when (slot-state-lstat named) (sb-posix:rmdir named)))
                   (sb-posix:rename (string-right-trim "/" (namestring saved)) named))))
           (rove:ok entered)
           (rove:ok changed))))))

(rove:deftest imported-archive-directory-mode-cannot-relax-file-checks
    (call-with-imported-archive-directory-test
     (lambda (directory parent)
       (let* ((path (merge-pathnames "immutable-proof" (uiop:pathname-parent-directory-pathname directory)))
              (entered nil))
         (with-open-file (out path :direction :output :if-exists :error :if-does-not-exist :create)
           (write-string "proof" out))
         (sb-posix:chmod (namestring path) #o600)
         (rove:ok (rove:signals
                   (call-with-logical-slot-copy-source-proof-fd
                    parent "immutable-proof" nil
                    (lambda (fd facts) (declare (ignore fd facts)) (setf entered t))
                    :mutable-directory t)
                   'error))
         (rove:ok (not entered))
         (rove:ok
          (rove:signals
           (call-with-logical-slot-copy-source-proof-fd
            parent "immutable-proof" nil
            (lambda (fd facts)
              (declare (ignore fd facts))
              (setf entered t)
              (with-open-file (out path :direction :output :if-exists :append)
                (write-char #\! out))))
           'error))
         (rove:ok entered)))))
