(in-package #:pg)

;;; A narrow, thread-bound owner for checking the installed target and reading
;;; follow intent.  It does not publish or recover a journal, issue RPCs, or
;;; activate follow.  The inherited opaque printer protects saved credentials.

(defstruct (physical-reseed-follow-owner
            (:include physical-reseed-install-owner)
            (:print-function print-physical-reseed-install-owner))
  binding)

(defun physical-reseed-follow-base-at (root-fd plan)
  (call-with-physical-backup-image-directory
   root-fd
   (lambda (directory)
     (multiple-value-bind (manifest digest) (physical-reseed-image-base-state directory)
       (unless (and (equal manifest (getf plan :manifest)) (= digest (getf plan :manifest-digest)))
         (error "target follow base differs from its install plan"))
       (let ((*pgdata* directory))
         (physical-reseed-follow-base-copy
          (list :manifest-digest digest
                :checkpoint-start (physical-manifest-checkpoint-start manifest)
                :recovery-control
                (physical-libpq-octets-datum
                 (recovery-read-bytes (physical-recovery-control-path directory) :maximum 196))
                :wal-control
                (physical-libpq-octets-datum
                 (recovery-read-bytes (wal-control-path) :maximum (1+ +wal-control-maximum-bytes+)))
                :auth-files
                (loop for name in '("authentication.initialized" "pg_hba.conf" "pg_ident.conf")
                      collect (subseq (physical-adoption-stage-file-state name :maximum 1048576) 0 3)))
          (getf (getf plan :attempt) :request)))))))

(defun physical-reseed-follow-check-binding (owner record)
  (let ((binding (physical-reseed-follow-owner-binding owner)))
    (unless (every (lambda (key) (equal (getf binding key) (getf record key)))
                   '(:version :attempt :install :base))
      (error "target follow journal rebound its immutable installed image")))
  record)

;; Bind the original attempt in the install plan, the complete terminal install
;; reference and installed base evidence.  The actual attempt may have a new
;; inode and a forward abort phase; its immutable identity is checked separately.

(defun physical-reseed-follow-initial-arguments (directory)
  (let* ((arguments (physical-reseed-install-seed-arguments directory))
         (plan (getf arguments :plan))
         (fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (let* ((state (physical-reseed-install-read-at fd "physical-reseed.install" :stabilize t))
                  (reference (physical-reseed-follow-terminal-reference state))
                  (base (physical-reseed-follow-base-at fd plan)))
             (unless (equal plan (getf state :plan))
               (error "target follow predecessor changed during ownership acquisition"))
             (append arguments
                     (list :binding
                           (physical-reseed-follow-copy
                            (list :version 1 :attempt (getf plan :attempt) :install reference
                                  :base base :generations nil))))))
      (when fd (sb-posix:close fd)))))

;; Keep role validation finite: check the root, predecessor, current attempt,
;; PID and canonical final binding without recursively proving the whole image.
;; A temporary is admitted only as reconciliation evidence, not as readable state.

(defmethod physical-adoption-check-owner-role ((owner physical-reseed-follow-owner))
  (physical-directory-check-owner-common owner)
  (call-with-physical-directory-root
   owner
   (lambda (fd directory)
     (physical-adoption-require-absent
      (physical-reseed-install-temporary-path directory) "target follow requires a settled install journal")
     (physical-adoption-require-absent
      (physical-reseed-attempt-temporary-path directory) "target follow requires a settled original attempt")
     (let* ((state (physical-reseed-install-read-at fd "physical-reseed.install"))
            (plan (physical-reseed-install-owner-plan owner))
            (attempt (physical-reseed-attempt-read-at fd "physical-reseed.attempt")))
       (unless (and (equal plan (getf state :plan))
                    (equal (physical-reseed-follow-terminal-reference state)
                           (getf (physical-reseed-follow-owner-binding owner) :install)))
         (error "target follow terminal predecessor changed"))
       (physical-reseed-cleanup-check-binding owner attempt)
       (physical-reseed-install-check-pid owner fd plan (getf state :step))
       (when (physical-reseed-follow-entry fd "physical-reseed.follow")
         (physical-reseed-follow-check-binding owner (physical-reseed-follow-read-at fd "physical-reseed.follow")))
       (physical-reseed-follow-entry fd "physical-reseed.follow.tmp" t)
       (setf (physical-reseed-cleanup-owner-attempt owner) attempt))))
  owner)

(defmethod physical-reseed-install-extra-entries ((owner physical-reseed-follow-owner) root-fd)
  (physical-directory-check-owner-common owner)
  (let ((stat (sb-posix:fstat root-fd)))
    (unless (and (= (physical-adoption-owner-device owner) (sb-posix:stat-dev stat))
                 (= (physical-adoption-owner-inode owner) (sb-posix:stat-ino stat)))
      (error "target follow namespace descriptor differs from its owner")))
  (loop for name in '("physical-reseed.follow" "physical-reseed.follow.tmp")
        for row = (nth-value 1 (physical-reseed-follow-entry root-fd name t))
        when row collect (cons name row)))

;; Hold the namespace descriptors while proving the terminal image and base.
;; Repeat the proof around FUNCTION; the supplied check is valid only in this
;; scope and does not grant publication or network authority.

(defun call-with-physical-reseed-follow-image (owner function)
  (unless (physical-reseed-follow-owner-p owner) (error "target follow ownership is required"))
  (call-with-physical-reseed-install-namespace
   owner
   (lambda (root-fd incoming-fd quarantine-fd state prefix check)
     (declare (ignore incoming-fd quarantine-fd))
     (unless (and (= prefix (1- (physical-reseed-install-last-step (getf state :plan))))
                  (equal (physical-reseed-follow-terminal-reference state)
                         (getf (physical-reseed-follow-owner-binding owner) :install)))
       (error "target follow installation is not terminal"))
     (flet ((verify ()
              (funcall check)
              (physical-reseed-install-check-image root-fd (getf state :plan) check owner)
              (unless (equal (physical-reseed-follow-base-at root-fd (getf state :plan))
                             (getf (physical-reseed-follow-owner-binding owner) :base))
                (error "target follow installed base evidence changed"))
              (funcall check)))
       (verify)
       (multiple-value-prog1 (funcall function root-fd #'verify)
         (verify))))))

;; Genesis is a detached empty record, not permission to publish or prepare.
;; Both fixed follow names must be absent before it can be returned.

(defun physical-reseed-follow-initial-record (owner)
  (call-with-physical-reseed-follow-image
   owner
   (lambda (root-fd check)
     (declare (ignore check))
     (physical-adoption-require-absent
      (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root-fd))
      "target follow temporary requires reconciliation")
     (physical-adoption-require-absent
      (merge-pathnames "physical-reseed.follow" (physical-image-fd-path root-fd))
      "target follow journal already exists")
     (physical-reseed-follow-copy (physical-reseed-follow-owner-binding owner)))))

;; Any temporary follow entry fences ordinary reads, even if its metadata is
;; acceptable to the owner role.  Reconciliation is a separate future operation.

(defun physical-reseed-follow-read (owner)
  (call-with-physical-reseed-follow-image
   owner
   (lambda (root-fd check)
     (declare (ignore check))
     (physical-adoption-require-absent
      (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root-fd))
      "target follow temporary requires reconciliation")
     (when (physical-reseed-follow-entry root-fd "physical-reseed.follow")
       (physical-reseed-follow-copy
        (physical-reseed-follow-check-binding owner (physical-reseed-follow-read-at root-fd "physical-reseed.follow" :stabilize t)))))))

;; Acquire the stopped root lock for FUNCTION and revoke the owner on exit.
;; Finish the initial image scope before FUNCTION so its operations can take
;; their own workspace locks.  Recheck the image after a normal return.

(defun call-with-physical-reseed-follow-owner (directory function)
  (call-with-physical-adoption-directory-owner
   directory
   (lambda (owner)
     (flet ((verify ()
              (call-with-physical-reseed-follow-image
               owner (lambda (root-fd check)
                       (declare (ignore root-fd check)) t))))
       (verify)
       (multiple-value-prog1 (funcall function owner)
         (verify))))
   :constructor #'make-physical-reseed-follow-owner
   :initial-arguments #'physical-reseed-follow-initial-arguments))
