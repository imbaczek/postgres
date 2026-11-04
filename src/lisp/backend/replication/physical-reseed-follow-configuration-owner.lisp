(in-package #:pg)

;;; Own configuration work against a stopped target. The owner checks the
;;; installed image; operation scopes separately check the workspace.

;; Dedicated owner for building and staged configuration.  Its inherited opaque
;; printer keeps the saved source credentials private.
(defstruct (physical-reseed-follow-configuration-owner
            (:include physical-reseed-follow-owner)
            (:print-function print-physical-reseed-install-owner)))

(defmethod physical-reseed-follow-check-configuration-role
    ((owner physical-reseed-follow-configuration-owner) record)
  (let ((configuration (getf record :configuration)))
    (when (and configuration (not (member (getf configuration :phase) '(:building :staged :installing :inert))))
      (error "configuration ownership does not admit this phase")))
  record)

(defun physical-reseed-follow-configuration-initial-arguments (directory)
  (let ((fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (let ((snapshot (physical-reseed-follow-snapshot-at fd)))
             (unless (getf (getf snapshot :record) :configuration)
               (return-from physical-reseed-follow-configuration-initial-arguments
                 (physical-reseed-follow-initial-arguments directory)))
             (let* ((arguments (physical-reseed-install-seed-arguments directory))
                    (state (physical-reseed-install-read-at fd "physical-reseed.install"))
                    (record (physical-reseed-follow-copy (getf snapshot :record)))
                    (plan (getf arguments :plan)))
               (unless (and (equal plan (getf state :plan))
                            (equal (getf record :attempt) (getf plan :attempt))
                            (equal (getf record :install) (physical-reseed-follow-terminal-reference state)))
                 (error "configuration seed differs from the actual terminal installation"))
               (physical-reseed-follow-check-snapshot fd snapshot)
               (append arguments (list :binding (physical-reseed-follow-core-projection record))))))
      (when fd (sb-posix:close fd)))))

;; Check the original installed image before and after FUNCTION.  Workspace
;; evidence is established by the individual operations, not this factory.
(defun call-with-physical-reseed-follow-configuration-owner (directory function)
  (call-with-physical-adoption-directory-owner
   directory
   (lambda (owner)
     (flet ((verify ()
              (call-with-physical-reseed-follow-image
               owner (lambda (fd check) (declare (ignore fd check)) t))))
       (verify)
       (multiple-value-prog1 (funcall function owner) (verify))))
   :constructor #'make-physical-reseed-follow-configuration-owner
   :initial-arguments #'physical-reseed-follow-configuration-initial-arguments))

(defun physical-reseed-follow-configuration-workspace-facts (owner root-fd directory fd)
  (let* ((facts (physical-reseed-install-auxiliary-base-facts owner root-fd directory fd))
         (plan (physical-reseed-install-owner-plan owner))
         (attempt (getf plan :attempt))
         (protected (list (getf attempt :local) (getf attempt :stage)
                          (getf plan :incoming) (getf plan :quarantine))))
    (unless (= (physical-adoption-install-mount-id directory)
               (physical-adoption-install-mount-id nil :directory-fd fd))
      (error "configuration workspace path changed its mount"))
    (dolist (other protected)
      (let ((path (pathname (getf other :directory))))
        (when (or (uiop:subpathp directory path) (uiop:subpathp path directory)
                  (and (= (getf facts :device) (getf other :device))
                       (= (getf facts :inode) (getf other :inode))))
          (error "configuration workspace overlaps a protected directory"))))
    facts))

;; Hold and check the named workspace while calling FUNCTION with its facts,
;; descriptor and check callback.  These are valid only in this scope and thread.
(defun call-with-physical-reseed-follow-workspace (owner root-fd directory function check)
  (let ((directory (truename (uiop:ensure-directory-pathname directory)))
        (fd nil) (active nil))
    (unwind-protect
         (progn
           (funcall check)
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (unless (zerop (physical-store-flock fd 6))
             (error "configuration workspace is already owned"))
           (let ((facts (physical-reseed-follow-configuration-workspace-facts owner root-fd directory fd)))
             (setf active t)
             (flet ((verify ()
                      (unless active (error "configuration workspace scope has ended"))
                      (funcall check)
                      (unless (equal facts
                                     (physical-reseed-follow-configuration-workspace-facts owner root-fd directory fd))
                        (error "configuration workspace identity changed"))))
               (verify)
               (multiple-value-prog1 (funcall function facts fd #'verify)
                 (verify)))))
      (setf active nil)
      (when fd (sb-posix:close fd)))))

(defun call-with-physical-reseed-follow-empty-workspace (owner root-fd directory function check)
  (call-with-physical-reseed-follow-workspace
   owner root-fd directory
   (lambda (facts fd workspace-check)
     (flet ((verify ()
              (funcall workspace-check)
              (when (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t :maximum-entries 1)
                (error "configuration workspace must be empty"))))
       (verify)
       (multiple-value-prog1 (funcall function facts fd #'verify) (verify))))
   check))

(defun physical-reseed-follow-configuration-building-record (record workspace images binding time)
  (let* ((core (physical-reseed-follow-core-copy (physical-reseed-follow-core-projection record)))
         (generation (car (last (getf core :generations)))))
    (physical-reseed-follow-copy
     (append core
             (list :configuration
                   (list :phase :building :generation (getf generation :generation)
                         :anchor-digest (physical-follow-anchor-digest (getf binding :anchor))
                         :time time :workspace workspace :directories nil
                         :images (physical-reseed-follow-configuration-image-specifications images core)
                         :entries nil :step 0))))))

;; Publish building intent for an empty workspace and deterministic images.
;; Require the current attempt to stay active and unchanged. Check the exact
;; follow predecessor before publishing its replacement.
(defun physical-reseed-follow-begin-configuration (owner workspace time)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "beginning configuration requires dedicated ownership"))
  (unless (typep time '(signed-byte 64)) (error "invalid inert follow image time"))
  (physical-reseed-follow-check-idle owner)
  (call-with-physical-reseed-follow-store-proof
   owner
   (lambda (root-fd check)
     (let* ((previous (physical-reseed-follow-snapshot-at root-fd))
            (record (getf previous :record)))
       (unless record (error "configuration requires a durable proved follow record"))
       (physical-reseed-follow-check-binding owner record)
       (multiple-value-bind (images binding)
           (physical-reseed-follow-configuration-images-at
            owner root-fd (physical-reseed-follow-core-projection record) time)
         (call-with-physical-reseed-follow-empty-workspace
          owner root-fd workspace
          (lambda (facts workspace-fd workspace-check)
            (let ((candidate (physical-reseed-follow-configuration-building-record record facts images binding time)))
              (physical-reseed-follow-check-transition record candidate)
              (physical-reseed-follow-check-snapshot root-fd previous)
              (if (getf record :configuration)
                  (progn
                    (unless (equal candidate record) (error "configuration building intent differs"))
                    (values (physical-reseed-follow-copy record) previous))
                  (progn
                    (sb-posix:fsync workspace-fd)
                    (physical-adoption-install-sync-ancestors (pathname (getf facts :directory)))
                    (funcall workspace-check)
                    (physical-reseed-follow-write-intent
                     owner root-fd candidate (physical-reseed-follow-image candidate) previous workspace-check)))))
          check))))))

(defun call-with-physical-reseed-follow-building-evidence (owner root-fd record function check)
  (let* ((configuration (getf record :configuration))
         (time (getf configuration :time)))
    (unless (eq :building (getf configuration :phase))
      (error "configuration recovery requires building intent"))
    (multiple-value-bind (images binding)
        (physical-reseed-follow-configuration-images-at
         owner root-fd (physical-reseed-follow-core-projection record) time)
      (call-with-physical-reseed-follow-empty-workspace
       owner root-fd (pathname (getf (getf configuration :workspace) :directory))
       (lambda (facts workspace-fd workspace-check)
         (let ((expected (physical-reseed-follow-configuration-building-record record facts images binding time)))
           (unless (equal expected record) (error "configuration building evidence differs"))
           (funcall function workspace-fd workspace-check)))
       check))))

(defgeneric physical-reseed-follow-configuration-recovery-dispatch (owner))

;; Reconcile configuration publication with its exact predecessor and workspace
;; evidence.  A stable proved final without configuration can also be returned.
;; The current attempt must remain active; partial or unexplained temporaries
;; remain unchanged and fenced.
(defun physical-reseed-follow-recover-configuration (owner)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "configuration recovery requires dedicated ownership"))
  (physical-reseed-follow-check-idle owner)
  (physical-reseed-follow-configuration-recovery-dispatch owner))

(defmethod physical-reseed-follow-configuration-recovery-dispatch ((owner physical-reseed-follow-owner))
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "configuration recovery requires dedicated ownership"))
  (physical-reseed-follow-check-idle owner)
  (call-with-physical-reseed-follow-store-proof
   owner
   (lambda (root-fd check)
     (let* ((previous (physical-reseed-follow-snapshot-at root-fd))
            (record (getf previous :record))
            (temporary (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t)))
       (unless record (error "configuration recovery requires its durable predecessor"))
       (physical-reseed-follow-check-binding owner record)
       (if temporary
           (let ((candidate (physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")))
             (unless (or (and (null (getf record :configuration))
                              (eq :building (getf (getf candidate :configuration) :phase)))
                         (and (eq :building (getf (getf record :configuration) :phase))
                              (eq :staged (getf (getf candidate :configuration) :phase))))
               (error "configuration recovery requires its exact supported predecessor"))
             (physical-reseed-follow-check-transition record candidate)
             (call-with-physical-reseed-follow-configuration-recovery-evidence
              owner root-fd candidate
              (lambda (workspace-fd workspace-check)
                (declare (ignore workspace-fd))
                (let ((fd nil))
                  (unwind-protect
                       (progn
                         (funcall workspace-check)
                         (physical-reseed-follow-check-snapshot root-fd previous)
                         (sb-sys:without-interrupts
                           (setf fd (physical-image-openat
                                     root-fd "physical-reseed.follow.tmp"
                                     (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                             +postmaster-o-cloexec+) 0)))
                         (when (minusp fd) (setf fd nil) (error "cannot open configuration recovery temporary"))
                         (unless (equal temporary (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                           (error "configuration recovery temporary changed"))
                         (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                         (physical-reseed-follow-promote-intent
                          owner root-fd fd (subseq temporary 0 5) candidate previous workspace-check))
                    (when fd (sb-posix:close fd)))))
              check))
           (flet ((stabilize ()
                    (funcall check)
                    (physical-reseed-follow-check-stable-record-at owner root-fd record)
                    (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                    (physical-reseed-follow-read-at root-fd "physical-reseed.follow" :stabilize t)
                    (physical-reseed-follow-check-snapshot root-fd previous)
                    (values (physical-reseed-follow-copy record) previous)))
             (if (getf record :configuration)
                 (call-with-physical-reseed-follow-configuration-stable-evidence
                  owner root-fd record
                  (lambda (workspace-fd workspace-check)
                    (declare (ignore workspace-fd))
                    (funcall workspace-check)
                    (stabilize))
                  check)
                 (stabilize))))))))
