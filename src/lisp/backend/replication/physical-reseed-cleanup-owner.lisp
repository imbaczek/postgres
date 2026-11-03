(in-package #:pg)

(defun print-physical-reseed-cleanup-owner (owner stream depth)
  (declare (ignore depth))
  (print-unreadable-object (owner stream :type t :identity t)))

(defstruct (physical-reseed-cleanup-owner (:include physical-reseed-owner)
                                          (:print-function print-physical-reseed-cleanup-owner))
  attempt)

(defun physical-reseed-cleanup-check-intent (directory)
  (physical-adoption-require-absent
   (physical-reseed-install-path directory)
   "reseed cleanup ownership cannot reconcile a final install journal")
  (unless (physical-adoption-path-present-p (physical-reseed-install-temporary-path directory))
    (error "reseed cleanup ownership requires an interrupted initial install intent")))

(defun physical-reseed-cleanup-check-binding (owner actual)
  (let ((saved (physical-reseed-cleanup-owner-attempt owner)))
    (unless (and (every (lambda (key) (equal (getf saved key) (getf actual key)))
                        *physical-reseed-attempt-identity-keys*)
                 (member (getf actual :phase)
                         (case (getf saved :phase)
                           (:premanifest '(:premanifest :aborting :aborted))
                           (:aborting '(:aborting :aborted))
                           (:aborted '(:aborted))))
                 (equal (getf actual :local) (physical-reseed-attempt-local-evidence owner)))
      (error "reseed cleanup attempt identity or phase changed")))
  owner)

(defun physical-reseed-cleanup-pid-evidence (record)
  (let ((pid (getf (getf record :local) :pid)))
    (when pid
      (list :pid (getf pid :pid) :identity (copy-list (getf pid :identity))
            :bytes (physical-libpq-datum-octets (getf pid :bytes) 16384)))))

(defun physical-reseed-cleanup-initial-arguments (directory)
  (physical-reseed-cleanup-check-intent directory)
  (let ((fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (let* ((record (physical-reseed-attempt-read-at fd "physical-reseed.attempt"))
                  (local (getf record :local))
                  (root (sb-posix:fstat fd)))
             (unless (and (equal (getf local :directory) (namestring directory))
                          (= (getf local :device) (sb-posix:stat-dev root))
                          (= (getf local :inode) (sb-posix:stat-ino root)))
               (error "reseed cleanup seed belongs to another root"))
             (list :attempt record :old-pid (physical-reseed-cleanup-pid-evidence record))))
      (when fd (sb-posix:close fd)))))

(defmethod physical-adoption-check-owner-role ((owner physical-reseed-cleanup-owner))
  (physical-directory-check-owner-common owner)
  (physical-reseed-cleanup-check-intent (physical-adoption-owner-directory owner))
  (physical-reseed-check-original-pid owner)
  (physical-reseed-cleanup-check-binding
   owner (physical-reseed-attempt-read-owned owner "physical-reseed.attempt"))
  owner)

(defun call-with-physical-reseed-cleanup-owner (directory function)
  (call-with-physical-adoption-directory-owner
   directory function :constructor #'make-physical-reseed-cleanup-owner
   :initial-arguments #'physical-reseed-cleanup-initial-arguments))
