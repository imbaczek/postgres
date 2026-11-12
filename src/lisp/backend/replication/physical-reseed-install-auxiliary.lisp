;;;-------------------------------------------------------------------------
;;;
;;; physical-reseed-install-auxiliary.lisp
;;;   Hold and check a guarded physical reseed auxiliary directory.
;;;
;;; The scope holds the target root and auxiliary directory descriptors, a
;;; directory lock, and the startup lock.  It checks both guard files and
;;; their named identities throughout the callback.  These facts support a
;;; future installation publisher and do not publish a journal or move files.
;;;
;;;-------------------------------------------------------------------------
(in-package #:pg)

(defparameter *physical-reseed-install-guard-text* "offline reseed auxiliary directory")

(defvar *physical-reseed-install-test-hook* nil)

(defun physical-reseed-install-test-point (point directory)
  (when *physical-reseed-install-test-hook*
    (funcall *physical-reseed-install-test-hook* point directory)))

(defun physical-reseed-install-guard-bytes (name)
  (cond
    ((equal name ".pg_lisp.lock") (make-array 0 :element-type '(unsigned-byte 8)))
    ((equal name "physical-reseed.install") (string->bytes *physical-reseed-install-guard-text*))
    (t (error "unexpected reseed auxiliary guard name"))))

;; Create or open a guard without following symlinks.  Hold the startup lock
;; for the scope and sync each newly written guard before returning control.
(defun call-with-physical-reseed-install-guard (directory-fd name prepare function)
  (let ((fd nil) (stream nil) (bytes (physical-reseed-install-guard-bytes name)))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       directory-fd name
                       (logior (if prepare (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl)
                                   sb-posix:o-rdonly)
                               sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) #o600)))
           (when (minusp fd)
             (setf fd nil)
             (error "cannot open reseed auxiliary guard: errno ~d" (sb-alien:get-errno)))
           (when (equal name ".pg_lisp.lock")
             (unless (zerop (physical-store-flock fd 6))
               (error "reseed auxiliary startup lock is already owned")))
           (sb-sys:without-interrupts
             (setf stream (sb-sys:make-fd-stream fd :input t :output prepare :element-type '(unsigned-byte 8))))
           (when prepare
             (sb-posix:fchmod fd #o600)
             (write-sequence bytes stream)
             (fsync-stream stream))
           (funcall function stream))
      (if stream (close stream) (when fd (sb-posix:close fd))))))

;; Check private metadata, exact contents, mount and the identities of both
;; the held file and its name.  Recheck after reading to detect replacement.
(defun physical-reseed-install-guard-facts (directory-fd stream name device mount)
  (let* ((fd (sb-sys:fd-stream-fd stream))
         (path (merge-pathnames name (physical-image-fd-path directory-fd)))
         (bytes (physical-reseed-install-guard-bytes name))
         (before (physical-reseed-file-facts (sb-posix:fstat fd))))
    (unless (and (= device (first before))
                 (= #o100600 (third before)) (= 1 (fourth before))
                 (= (sb-posix:geteuid) (fifth before)) (= (length bytes) (sixth before))
                 (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                 (equal before (physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
      (error "reseed auxiliary guard identity or private metadata changed"))
    (file-position stream 0)
    (let ((actual (make-array (length bytes) :element-type '(unsigned-byte 8))))
      (unless (and (= (length bytes) (read-sequence actual stream))
                   (eq :eof (read-byte stream nil :eof))
                   (equalp bytes actual)
                   (equal before (physical-reseed-file-facts (sb-posix:fstat fd)))
                   (equal before (physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
        (error "reseed auxiliary guard bytes or identity changed")))
    (second before)))

(defun physical-reseed-install-auxiliary-base-facts (owner root-fd directory fd)
  (physical-directory-check-owner-common owner)
  (physical-image-check-target directory fd)
  (let ((stat (physical-image-check-directory fd (physical-adoption-owner-device owner))))
    (unless (= (physical-adoption-install-mount-id nil :directory-fd root-fd)
               (physical-adoption-install-mount-id nil :directory-fd fd))
      (error "reseed auxiliary must share the owned target mount"))
    (physical-reseed-attempt-stage-copy
     (list :directory (namestring directory) :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))
     (physical-reseed-attempt-local-evidence owner))))

;; Collect owned copies of the directory and guard identities after checking
;; the held root and auxiliary directory.  Reject aliases of the owned root
;; and its lock.
(defun physical-reseed-install-auxiliary-facts (owner root-fd directory fd lock fence)
  (let* ((local (physical-reseed-attempt-local-evidence owner))
         (base (physical-reseed-install-auxiliary-base-facts owner root-fd directory fd))
         (device (getf base :device))
         (mount (physical-adoption-install-mount-id nil :directory-fd fd))
         (identities (make-hash-table :test #'equal)))
    (dolist (inode (list (physical-adoption-owner-inode owner) (physical-adoption-owner-lock-inode owner)))
      (physical-reseed-install-register device inode device identities))
    (physical-reseed-install-auxiliary-copy
     (append base
             (list :lock-inode (physical-reseed-install-guard-facts fd lock ".pg_lisp.lock" device mount)
                   :fence-inode (physical-reseed-install-guard-facts fd fence "physical-reseed.install" device mount)))
     local identities)))

(defmacro physical-reseed-check-directory-flock (fd active message)
  ;; The probe permits local interrupts, so check ACTIVE again after it returns.
  ;; A failed observation latches the scope inactive before signaling the error.
  `(sb-sys:without-interrupts
     (unless (and (sb-sys:with-local-interrupts
                    (handler-case-noninterrupt (physical-file-holds-exclusive-flock-p ,fd)
                                               (condition nil)))
                  ,active)
       (setf ,active nil)
       (error ,message))))

;; Prepare guards under ordinary stopped ownership or reopen saved guard facts.
;; Preparation checks the full owner role after each test point and before the
;; callback.  It syncs the guard files, auxiliary directory and naming ancestors
;; before the callback can use the directory.
;;
;; The callback receives the directory, copied facts, held directory descriptor
;; and a check function.  The check is valid only in this scope and under the
;; owner's thread and lifetime rules.  It checks common root ownership and the
;; saved directory and guard facts.  Reopening leaves role authority with the
;; caller.  The post-callback check also uses common ownership, allowing a future
;; publisher to supersede the ordinary role during the callback.
;;
;; Guard files remain in place on a nonlocal exit.  Their removal and eventual
;; publication of an installation journal belong to later work.
(defun call-with-physical-reseed-install-auxiliary (owner directory function &key prepare empty expected)
  (unless (physical-reseed-owner-p owner) (error "reseed auxiliary requires stopped directory ownership"))
  (when prepare
    (unless (eq (type-of owner) 'physical-reseed-owner)
      (error "creating reseed auxiliary guards requires ordinary stopped ownership"))
    (physical-reseed-check-owner owner))
  (unless (or prepare expected) (error "reopening reseed auxiliary requires its saved identity"))
  (let* ((directory (truename (uiop:ensure-directory-pathname directory)))
         (local (physical-reseed-attempt-local-evidence owner))
         (expected (when expected
                     (if prepare (physical-reseed-attempt-stage-copy expected local)
                         (physical-reseed-install-auxiliary-copy expected local (make-hash-table :test #'equal))))))
    (call-with-physical-directory-root
     owner
     (lambda (root-fd root)
       (declare (ignore root))
       (let ((fd nil) (active nil))
         (unwind-protect
              (progn
                (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
                (unless (zerop (physical-store-flock fd 6)) (error "reseed auxiliary directory is already owned"))
                (let ((base (physical-reseed-install-auxiliary-base-facts owner root-fd directory fd)))
                  (when (and prepare expected (not (equal base expected)))
                    (error "reseed auxiliary differs from its prepared identity")))
                (when prepare
                  (when (and empty (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t :maximum-entries 1))
                    (error "reseed quarantine must start empty"))
                  (dolist (name '(".pg_lisp.lock" "physical-reseed.install"))
                    (physical-adoption-require-absent
                     (merge-pathnames name (physical-image-fd-path fd)) "reseed auxiliary already has management intent")))
                (physical-reseed-install-test-point :after-auxiliary-open directory)
                (when prepare (physical-reseed-check-owner owner))
                (physical-reseed-install-auxiliary-base-facts owner root-fd directory fd)
                (call-with-physical-reseed-install-guard
                 fd ".pg_lisp.lock" prepare
                 (lambda (lock)
                   (physical-reseed-install-test-point :after-auxiliary-lock directory)
                   (when prepare (physical-reseed-check-owner owner))
                   (call-with-physical-reseed-install-guard
                    fd "physical-reseed.install" prepare
                    (lambda (fence)
                      (let ((facts (physical-reseed-install-auxiliary-facts owner root-fd directory fd lock fence)))
                        (when (and expected (not prepare) (not (equal expected facts)))
                          (error "reseed auxiliary differs from its saved guards"))
                        (when prepare
                          (sb-posix:fsync fd)
                          (physical-adoption-install-sync-ancestors directory))
                        (setf active t)
                        (flet ((check ()
                                 (unless active (error "reseed auxiliary scope has ended"))
                                 (physical-reseed-check-directory-flock
                                  fd active "reseed auxiliary directory lost its exclusive lock")
                                 (let ((actual (physical-reseed-install-auxiliary-facts owner root-fd directory fd lock fence)))
                                   (unless (equal facts actual) (error "reseed auxiliary identity changed"))
                                   (physical-file-check-exclusive-flock lock)
                                   actual)))
                          (physical-reseed-install-test-point :after-auxiliary-sync directory)
                          (when prepare (physical-reseed-check-owner owner))
                          (check)
                          (multiple-value-prog1
                              (funcall function directory
                                       (physical-reseed-install-auxiliary-copy facts local (make-hash-table :test #'equal))
                                       fd #'check)
                            (check)))))))))
           (setf active nil)
           (when fd (sb-posix:close fd))))))))
