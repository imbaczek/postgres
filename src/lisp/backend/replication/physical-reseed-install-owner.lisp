(in-package #:pg)

;; Install journal ownership for source cleanup and recovery checks.  An install
;; owner can perform source STATUS and ABORT through the cleanup role.  It has
;; no authority to move files or use an image.
;;
;; Require a private, bounded, single-link journal and return its stat facts.
;; Empty files are accepted only while the publisher holds a new generation.
(defun physical-reseed-install-file-identity (stat &optional allow-empty)
  (unless (and (= #o100600 (sb-posix:stat-mode stat))
               (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
               (= 1 (sb-posix:stat-nlink stat))
               (<= (if allow-empty 0 1) (sb-posix:stat-size stat) +physical-reseed-install-maximum-bytes+))
    (error "reseed install journal is not a bounded private single-link regular file"))
  (physical-reseed-file-facts stat))

;; Read a named journal through a held descriptor.  Check its mount before
;; reading and compare named and descriptor stat facts around the read.  Bind
;; the decoded plan to this journal generation and root.  Stabilization also
;; syncs file and directory.  A malformed or changed final journal fails closed.
;; Return the decoded state, file identity and digest of the same bytes.
(defun physical-reseed-install-read-at (root-fd name &key stabilize)
  (unless (member name '("physical-reseed.install" "physical-reseed.install.tmp") :test #'equal)
    (error "unexpected reseed install journal name"))
  (let ((fd nil) (stream nil)
                 (path (merge-pathnames name (physical-image-fd-path root-fd))))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       root-fd name (logior (if stabilize sb-posix:o-rdwr sb-posix:o-rdonly)
                                            sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) 0)))
           (when (minusp fd)
             (setf fd nil) (error "cannot open reseed install journal: errno ~d" (sb-alien:get-errno)))
           (let* ((before (physical-reseed-install-file-identity (sb-posix:fstat fd)))
                  (size (sixth before)) (bytes (make-array size :element-type '(unsigned-byte 8))))
             (unless (and (equal before (physical-reseed-install-file-identity (sb-posix:lstat (namestring path))))
                          (= (physical-adoption-install-mount-id nil :directory-fd root-fd)
                             (physical-adoption-install-mount-id nil :directory-fd fd)))
               (error "reseed install journal changed before reading or crosses mounts"))
             (sb-sys:without-interrupts
               (setf stream (sb-sys:make-fd-stream fd :input t :output stabilize :element-type '(unsigned-byte 8))))
             (when stabilize (fsync-stream stream) (sb-posix:fsync root-fd))
             (physical-reseed-install-test-point :after-journal-open path)
             (unless (and (= size (read-sequence bytes stream))
                          (eq :eof (read-byte stream nil :eof))
                          (equal before (physical-reseed-install-file-identity (sb-posix:fstat fd)))
                          (equal before (physical-reseed-install-file-identity (sb-posix:lstat (namestring path)))))
               (error "reseed install journal changed while reading"))
             (let* ((state (physical-reseed-install-decode bytes))
                    (plan (getf state :plan)) (local (getf (getf plan :attempt) :local))
                    (journal (getf plan :journal)) (root (sb-posix:fstat root-fd)))
               (unless (and (= (getf journal :device) (first before)) (= (getf journal :inode) (second before))
                            (= (getf local :device) (sb-posix:stat-dev root))
                            (= (getf local :inode) (sb-posix:stat-ino root)))
                 (error "reseed install journal generation or root binding differs"))
               (append state (list :identity before :digest
                                   (physical-store-update-digest #xCBF29CE484222325 bytes))))))
      (if stream (close stream) (when fd (sb-posix:close fd))))))

;; Keep the owner's plan and attempt evidence out of printed representations.
(defun print-physical-reseed-install-owner (owner stream depth)
  (declare (ignore depth))
  (print-unreadable-object (owner stream :type t :identity t)))

(defstruct (physical-reseed-install-owner (:include physical-reseed-cleanup-owner)
                                          (:print-function print-physical-reseed-install-owner))
  ;; Private copy of the recovered plan, fixed for this ownership scope.
  plan
  ;; Greatest validated journal step seen by this owner.
  (step 0))

;; Borrow ROOT-FD and hold the quarantine directory descriptor and two input
;; streams for its management files.  Compare the saved guard facts on both
;; sides of the callback.  The held PGDATA lock serializes this read-only scope.
(defun call-with-physical-reseed-install-quarantine-pid (owner root-fd fact function)
  (let ((directory (pathname (getf fact :directory))) (fd nil) (lock nil) (fence nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-backup-open-directory directory)))
           (physical-reseed-install-auxiliary-base-facts owner root-fd directory fd)
           (sb-sys:without-interrupts
             (setf lock (open-bounded-regular-file (merge-pathnames ".pg_lisp.lock" (physical-image-fd-path fd)) 0))
             (setf fence (open-bounded-regular-file (merge-pathnames "physical-reseed.install" (physical-image-fd-path fd)) 128)))
           (flet ((check ()
                    (unless (equal fact (physical-reseed-install-auxiliary-facts owner root-fd directory fd lock fence))
                      (error "reseed PID quarantine identity changed"))))
             (check)
             (multiple-value-prog1 (funcall function fd)
               (check))))
      (when fence (close fence))
      (when lock (close lock))
      (when fd (sb-posix:close fd)))))

;; Read PID evidence only while its named statx observation remains stable.
;; A moved entry may have a different ctime after rename.
(defun physical-reseed-install-read-pid-at (fd expected &optional moved)
  (when (physical-adoption-path-present-p (merge-pathnames "postmaster.pid" (physical-image-fd-path fd)))
    (let* ((mount (physical-adoption-install-mount-id nil :directory-fd fd))
           (before (physical-reseed-install-old-observe fd "postmaster.pid" mount))
           (current (physical-reseed-read-pid (physical-image-fd-path fd))))
      (unless (and (equal before (physical-reseed-install-old-observe fd "postmaster.pid" mount))
                   (or (null expected)
                       (if moved (equal (butlast before) (butlast expected)) (equal before expected))))
        (error "reseed PID stat evidence changed"))
      current)))

;; A live process or an indeterminate process state prevents cleanup.
(defun physical-reseed-install-check-dead-pid (old)
  (unless (eq :dead (physical-reseed-process-state (getf old :pid)))
    (error "reseed original process is live or its state is unknown")))

;; Check the original PID at its expected location and require it to be dead.
;; The quarantine location is permitted one move ahead of the recorded step,
;; since a crash can occur between a rename and its progress record.  Compare
;; moved identity and bytes while allowing rename to change ctime, and check
;; that no second root PID appears.
(defun physical-reseed-install-check-pid (owner root-fd plan step)
  (let* ((old (physical-reseed-owner-old-pid owner))
         (pid-row (assoc "postmaster.pid" (getf plan :old) :test #'equal))
         (current (physical-reseed-install-read-pid-at root-fd pid-row)))
    (cond
      ((null old)
       (when (or current (physical-reseed-install-read-pid-at root-fd nil))
         (error "reseed installation gained unexpected root PID metadata")))
      (t
       (let ((ordinal (1+ (position "postmaster.pid" (getf plan :old) :key #'first :test #'equal))))
         (if (and (< step ordinal) current)
             (progn
               (unless (equalp old current) (error "reseed original root PID evidence changed"))
               (physical-reseed-install-check-dead-pid old))
             (progn
               (when (or (< step (1- ordinal)) current)
                 (error "reseed original PID location disagrees with progress"))
               (call-with-physical-reseed-install-quarantine-pid
                owner root-fd (getf plan :quarantine)
                (lambda (quarantine-fd)
                  (let ((moved (physical-reseed-install-read-pid-at quarantine-fd pid-row t)))
                    (unless (and moved (= (getf old :pid) (getf moved :pid))
                                 (equal (subseq (getf old :identity) 0 7) (subseq (getf moved :identity) 0 7))
                                 (equalp (getf old :bytes) (getf moved :bytes)))
                      (error "reseed quarantined PID evidence changed"))
                    (physical-reseed-install-check-dead-pid old)
                    (when (physical-reseed-install-read-pid-at root-fd nil)
                      (error "reseed installation gained a second root PID"))))))))))
    owner))

;; Check follow intent before reading the install plan.  The specialized owner
;; repeats this check on use because its role method bypasses the base method.

(defun physical-reseed-install-initial-arguments (directory)
  (physical-directory-check-reseed-follow-intent directory)
  (physical-reseed-install-seed-arguments directory))

;; Recover the plan only when no temporary intent remains and the final journal
;; binds to the current attempt and directory.
(defun physical-reseed-install-seed-arguments (directory)
  (physical-adoption-require-absent (physical-reseed-install-temporary-path directory)
                                    "reseed install temporary requires reconciliation")
  (let ((fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (let* ((state (physical-reseed-install-read-at fd "physical-reseed.install"))
                  (plan (getf state :plan)) (original (getf plan :attempt))
                  (attempt (physical-reseed-attempt-read-at fd "physical-reseed.attempt")))
             (unless (and (equal (getf (getf original :local) :directory) (namestring directory))
                          (every (lambda (key) (equal (getf original key) (getf attempt key)))
                                 *physical-reseed-attempt-identity-keys*))
               (error "reseed install journal belongs to another attempt"))
             (list :plan (physical-reseed-install-copy plan) :step (getf state :step)
                   :attempt (physical-reseed-attempt-copy attempt)
                   :old-pid (physical-reseed-cleanup-pid-evidence original))))
      (when fd (sb-posix:close fd)))))

;; Recheck the fixed plan, attempt binding, PID evidence, and nondecreasing
;; journal progress whenever install ownership is used.
(defmethod physical-adoption-check-owner-role ((owner physical-reseed-install-owner))
  (physical-directory-check-owner-common owner)
  (physical-directory-check-reseed-follow-intent (physical-adoption-owner-directory owner))
  (physical-adoption-require-absent
   (physical-reseed-install-temporary-path (physical-adoption-owner-directory owner))
   "reseed install temporary requires reconciliation")
  (call-with-physical-directory-root
   owner
   (lambda (fd directory)
     (declare (ignore directory))
     (let* ((state (physical-reseed-install-read-at fd "physical-reseed.install"))
            (plan (physical-reseed-install-owner-plan owner))
            (attempt (physical-reseed-attempt-read-at fd "physical-reseed.attempt")))
       (unless (and (equal plan (getf state :plan))
                    (>= (getf state :step) (physical-reseed-install-owner-step owner))
                    (every (lambda (key) (equal (getf attempt key) (getf (getf plan :attempt) key)))
                           *physical-reseed-attempt-identity-keys*))
         (error "reseed install plan, attempt binding or observed progress changed"))
       (physical-reseed-cleanup-check-binding owner attempt)
       (physical-reseed-install-check-pid owner fd plan (getf state :step))
       (setf (physical-reseed-install-owner-step owner) (getf state :step)
             (physical-reseed-cleanup-owner-attempt owner) attempt))))
  owner)

;; Acquire stopped directory ownership with the recovered install evidence.
(defun call-with-physical-reseed-install-owner (directory function)
  (call-with-physical-adoption-directory-owner
   directory function :constructor #'make-physical-reseed-install-owner
   :initial-arguments #'physical-reseed-install-initial-arguments))

;; Refuse follow intent before choosing any older owner; a future follow owner
;; needs its own narrowly scoped authority to reconcile these names.

;; Choose the final install journal first.  Failure to read or validate it is an
;; error and never falls back to cleanup or ordinary ownership.  A temporary
;; journal alone selects cleanup ownership.
(defun call-with-physical-reseed-operation-owner (directory function)
  (physical-directory-check-reseed-follow-intent directory)
  (cond
    ((physical-adoption-path-present-p (physical-reseed-install-path directory))
     (call-with-physical-reseed-install-owner directory function))
    ((physical-adoption-path-present-p (physical-reseed-install-temporary-path directory))
     (call-with-physical-reseed-cleanup-owner directory function))
    (t (call-with-physical-reseed-owner directory function))))
