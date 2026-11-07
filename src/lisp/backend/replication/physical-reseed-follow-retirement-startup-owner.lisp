(in-package #:pg)

(defstruct (physical-reseed-follow-retirement-startup-owner
            (:conc-name retirement-startup-))
  ;; Borrow the published lock and PID stream on the creating thread.  Keep
  ;; root, mount and PID facts with a copy of the trusted handover evidence.
  directory root-facts mount lock lock-fd lock-facts
  pid-stream pid-inode pid-state thread active-p evidence busy-p uncertain-p)

(defmethod print-object ((owner physical-reseed-follow-retirement-startup-owner) stream)
  (print-unreadable-object (owner stream :type t :identity t)))

(defun physical-reseed-follow-retirement-check-startup-owner (owner)
  ;; Keep the published lock, PID and root check independent of the full
  ;; observation proof.  Reasserting flock checks current exclusivity and can
  ;; reacquire a released lock.
  (unless (and (physical-reseed-follow-retirement-startup-owner-p owner)
               (retirement-startup-active-p owner)
               (eq (retirement-startup-thread owner) sb-thread:*current-thread*))
    (error "retirement startup owner ended or changed thread"))
  (when (getf (getf (getf (retirement-startup-evidence owner) :snapshot) :record) :cancellation)
    (error "target cancellation fences retirement startup"))
  (physical-adoption-check-startup-quiescence)
  (let* ((directory (retirement-startup-directory owner))
         (lock (retirement-startup-lock owner))
         (lock-fd (retirement-startup-lock-fd owner))
         (pid-stream (retirement-startup-pid-stream owner))
         (pid (retirement-startup-pid-state owner))
         (root (sb-posix:lstat (namestring directory)))
         (lock-stat (sb-posix:fstat lock-fd)))
    (unless (and pid (eq lock *postmaster-data-lock*) (open-stream-p lock)
                 (= lock-fd (sb-sys:fd-stream-fd lock))
                 (equal directory *postmaster-pid-directory*)
                 (equal directory (uiop:ensure-directory-pathname (truename *pgdata*)))
                 (sb-posix:s-isdir (sb-posix:stat-mode root))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid root))
                 (zerop (logand #o077 (sb-posix:stat-mode root)))
                 (equal (retirement-startup-root-facts owner)
                        (subseq (physical-reseed-file-facts root) 0 5))
                 (sb-posix:s-isreg (sb-posix:stat-mode lock-stat))
                 (= 1 (sb-posix:stat-nlink lock-stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid lock-stat))
                 (zerop (logand #o077 (sb-posix:stat-mode lock-stat)))
                 (equal (retirement-startup-lock-facts owner)
                        (physical-reseed-file-facts lock-stat))
                 (equal (retirement-startup-lock-facts owner)
                        (physical-reseed-file-facts
                         (sb-posix:lstat (namestring (merge-pathnames ".pg_lisp.lock" directory)))))
                 (= (retirement-startup-mount owner)
                    (physical-adoption-install-mount-id directory)
                    (physical-adoption-install-mount-id nil :directory-fd lock-fd))
                 (eq pid-stream *postmaster-pid-stream*) (open-stream-p pid-stream)
                 (eql (retirement-startup-pid-inode owner) *postmaster-pid-inode*)
                 (postmaster-pid-owned-p)
                 (= (retirement-startup-mount owner)
                    (physical-adoption-install-mount-id nil :directory-fd (sb-sys:fd-stream-fd pid-stream))
                    (physical-adoption-install-mount-id (merge-pathnames "postmaster.pid" directory)))
                 (= (getf pid :pid) (sb-posix:getpid))
                 (equal (getf pid :identity)
                        (physical-reseed-file-facts (sb-posix:fstat (sb-sys:fd-stream-fd pid-stream))))
                 (zerop (physical-store-flock lock-fd 6)))
      (error "retirement startup lost its published postmaster context"))
    (let ((current (physical-reseed-read-pid directory)))
      (unless (and current (= (getf pid :pid) (getf current :pid))
                   (equal (getf pid :identity) (getf current :identity))
                   (equalp (getf pid :bytes) (getf current :bytes)))
        (error "retirement startup PID evidence changed"))))
  owner)

(defun physical-reseed-follow-retirement-check-startup-idle (owner)
  ;; Require a live owner with no operation in progress or uncertain outcome.
  (physical-reseed-follow-retirement-check-startup-owner owner)
  (when (or (retirement-startup-busy-p owner) (retirement-startup-uncertain-p owner))
    (error "retirement startup owner is busy or uncertain"))
  owner)

(defun call-with-physical-reseed-follow-retirement-operation (owner function &optional final-check failure-cleanup)
  ;; Keep the owner busy through the operation and its final guard.  Run the
  ;; optional final check only after successful return and guard validation.
  ;; On failure, defer interrupts while cleanup runs before clearing busy.
  (physical-reseed-follow-retirement-check-startup-idle owner)
  (let ((completed nil))
    (unwind-protect
         (progn
           (setf (retirement-startup-busy-p owner) t)
           (physical-reseed-follow-test-point
            :after-retirement-operation-busy
            (merge-pathnames "physical-reseed.follow" (retirement-startup-directory owner)))
           (flet ((check ()
                    (physical-reseed-follow-retirement-check-startup-owner owner)
                    (unless (retirement-startup-busy-p owner)
                      (error "retirement operation lost its busy scope"))
                    (when (retirement-startup-uncertain-p owner)
                      (error "retirement operation requires durable reconciliation"))))
             (check)
             (multiple-value-prog1 (funcall function)
               (check)
               (when final-check (funcall final-check))
               (setf completed t))))
      (sb-sys:without-interrupts
        (unwind-protect
             (when (and (not completed) failure-cleanup) (funcall failure-cleanup))
          (setf (retirement-startup-busy-p owner) nil))))))

(defun call-with-physical-reseed-follow-retirement-startup-root (owner function)
  ;; Open and close the root descriptor around the callback.  VERIFY is valid
  ;; only within this scope and uses the owner's thread-bound guard.
  (physical-reseed-follow-retirement-check-startup-owner owner)
  (let ((fd nil) (active nil) (directory (retirement-startup-directory owner)))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-backup-open-directory directory)))
           (setf active t)
           (flet ((verify ()
                    (unless active (error "retirement startup root scope ended"))
                    (physical-reseed-follow-retirement-check-startup-owner owner)
                    (physical-image-check-target directory fd)
                    (unless (= (retirement-startup-mount owner)
                               (physical-adoption-install-mount-id nil :directory-fd fd))
                      (error "retirement startup root crossed mounts"))
                    t))
             (verify)
             (multiple-value-prog1 (funcall function fd #'verify) (verify))))
      (setf active nil)
      (when fd (sb-posix:close fd)))))

(defun call-with-physical-reseed-follow-retirement-startup-observation (owner function)
  ;; Select the owned-PID reader from the saved journal evidence.  Compare a
  ;; fresh matching projection before calling FUNCTION, under the borrowed
  ;; lock, PID and root checks.  Capture from private copies while all ten
  ;; roles are held, before the final verifier.  Audit after the root closes.
  (let ((stamp nil))
    (multiple-value-prog1
        (call-with-physical-reseed-follow-retirement-startup-root
         owner
         (lambda (root check)
           (let* ((evidence (retirement-startup-evidence owner))
                  (retired (getf evidence :journal)))
             (unless evidence (error "retirement startup historical evidence has been consumed"))
             (flet ((observe (descriptors state verify)
                      (unless (equal evidence
                                     (if retired
                                         (physical-reseed-follow-retired-startup-projection descriptors state)
                                         (physical-reseed-follow-startup-projection descriptors state)))
                        (error "retirement startup differs from its PID handover evidence"))
                      (funcall verify)
                      (let ((private-descriptors (copy-tree descriptors))
                            (names (physical-adoption-attempt-value-copy (getf (getf state :layout) :names))))
                        (multiple-value-prog1 (funcall function descriptors state verify)
                          (setf stamp (physical-reseed-follow-retirement-role-stamp private-descriptors names))))))
               (if retired
                   (call-with-physical-reseed-follow-retired-owned-pid-observation root check #'observe)
                   (call-with-physical-reseed-follow-retirement-owned-pid-observation
                    root (getf evidence :snapshot) check #'observe))))))
      (physical-reseed-follow-retirement-audit-stamp
       stamp (lambda () (physical-reseed-follow-retirement-check-startup-owner owner))))))

(defun call-with-physical-reseed-follow-retirement-startup-owner (expected function)
  ;; Bind trusted handover evidence to published postmaster ownership.
  ;; Require the branch and journal geometry to agree before saving the
  ;; evidence.  Reprove it before the callback and revoke the owner on exit.
  (when (getf (getf (getf (getf expected :evidence) :snapshot) :record) :cancellation)
    (error "target cancellation fences retirement startup"))
  (unless (and (case (getf expected :branch)
                 (:retirement (null (getf (getf expected :evidence) :journal)))
                 (:retired (member (getf (getf (getf expected :evidence) :journal) :geometry)
                                   '(:alias :receipt))))
               (getf expected :evidence)
               *postmaster-data-lock* (open-stream-p *postmaster-data-lock*)
               *postmaster-pid-stream* (open-stream-p *postmaster-pid-stream*))
    (error "retirement startup requires published postmaster ownership and handover evidence"))
  (let* ((directory (uiop:ensure-directory-pathname (truename *pgdata*)))
         (lock-fd (sb-sys:fd-stream-fd *postmaster-data-lock*))
         (owner
           (make-physical-reseed-follow-retirement-startup-owner
            :directory directory
            :root-facts (subseq (physical-reseed-file-facts (sb-posix:lstat (namestring directory))) 0 5)
            :mount (physical-adoption-install-mount-id directory)
            :lock *postmaster-data-lock* :lock-fd lock-fd
            :lock-facts (physical-reseed-file-facts (sb-posix:fstat lock-fd))
            :pid-stream *postmaster-pid-stream* :pid-inode *postmaster-pid-inode*
            :pid-state (physical-reseed-read-pid directory)
            :thread sb-thread:*current-thread* :active-p t
            :evidence (physical-adoption-attempt-value-copy (getf expected :evidence)))))
    (unwind-protect
         (progn
           (physical-reseed-follow-retirement-check-startup-idle owner)
           (call-with-physical-reseed-follow-retirement-startup-observation
            owner (lambda (descriptors state verify)
                    (declare (ignore descriptors state))
                    (funcall verify)))
           (multiple-value-prog1 (funcall function owner)
             (physical-reseed-follow-retirement-check-startup-idle owner)))
      (setf (retirement-startup-active-p owner) nil))))
