(in-package #:pg)

;; The saved attempt contains source credentials and a bearer token.  Use an
;; opaque printer to keep these fields out of routine owner output.

(defun print-physical-reseed-cleanup-owner (owner stream depth)
  (declare (ignore depth))
  (print-unreadable-object (owner stream :type t :identity t)))

;; Reconcile the saved source request after an initial install temporary entry
;; supersedes ordinary stopped ownership.  Advance the attempt's abort phases
;; while retaining the install and startup fences.

(defstruct (physical-reseed-cleanup-owner (:include physical-reseed-owner)
                                          (:print-function print-physical-reseed-cleanup-owner))
  attempt)

;; Accept any entry at the temporary install name by checking its presence
;; only.  A final install name prevents this cleanup role from opening.

;; Cleanup ownership also refuses follow intent on initial entry and recheck.
;; Its specialized role check cannot rely on the base owner's method.

(defun physical-reseed-cleanup-check-intent (directory)
  (physical-directory-check-reseed-follow-intent directory)
  (physical-adoption-require-absent
   (physical-reseed-install-path directory)
   "reseed cleanup ownership cannot reconcile a final install journal")
  (unless (physical-adoption-path-present-p (physical-reseed-install-temporary-path directory))
    (error "reseed cleanup ownership requires an interrupted initial install intent")))

;; Keep the original request, source and local identities fixed.  The attempt
;; may advance through abort phases but may not move backward or be rebound.

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

;; Read the saved attempt and PID evidence while the factory holds the flock.
;; Bind the seed to the opened root before constructing the cleanup owner.

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

;; Recheck the root, install names, original PID and attempt on each use.
;; Source cleanup checks the saved attempt independently of image health.
;; A partial or malformed attempt temporary remains fenced until explicitly
;; reconciled.  A complete forward temporary can use ordinary recovery.

(defmethod physical-adoption-check-owner-role ((owner physical-reseed-cleanup-owner))
  (physical-directory-check-owner-common owner)
  (physical-reseed-cleanup-check-intent (physical-adoption-owner-directory owner))
  (physical-reseed-check-original-pid owner)
  (physical-reseed-cleanup-check-binding
   owner (physical-reseed-attempt-read-owned owner "physical-reseed.attempt"))
  owner)

;; Source STATUS and ABORT use this checked owner.  Abort intent must be
;; durable before ABORT, and ABORTED needs the source's closed abort receipt.

(defun call-with-physical-reseed-cleanup-owner (directory function)
  (call-with-physical-adoption-directory-owner
   directory function :constructor #'make-physical-reseed-cleanup-owner
   :initial-arguments #'physical-reseed-cleanup-initial-arguments))
