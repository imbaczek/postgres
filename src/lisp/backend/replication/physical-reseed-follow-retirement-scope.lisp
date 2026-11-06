(in-package #:pg)

(defun physical-reseed-follow-retirement-directory-check (root expected fd)
  ;; Check the held directory against its canonical name, recorded identity
  ;; and root mount.
  (let* ((directory (pathname (getf expected :directory)))
         (parent (physical-image-check-directory root))
         (stat (physical-image-check-directory fd (sb-posix:stat-dev parent)))
         (mount (physical-adoption-install-mount-id nil :directory-fd root)))
    (physical-image-check-target directory fd)
    (unless (and (equal (namestring directory) (namestring (truename directory)))
                 (= (getf expected :device) (sb-posix:stat-dev stat))
                 (= (getf expected :inode) (sb-posix:stat-ino stat))
                 (not (= (sb-posix:stat-ino parent) (sb-posix:stat-ino stat)))
                 (= mount (physical-adoption-install-mount-id nil :directory-fd fd)
                    (physical-adoption-install-mount-id directory)))
      (error "retirement directory lost its canonical recorded binding")))
  t)

(defun physical-reseed-follow-retirement-guard-observation (fd expected lock fence)
  ;; Capture stable facts for both held guard streams and their recorded
  ;; inodes so the directory scope can recheck them.
  (let ((mount (physical-adoption-install-mount-id nil :directory-fd fd)))
    (loop for (name stream key) in (list (list ".pg_lisp.lock" lock :lock-inode)
                                         (list "physical-reseed.install" fence :fence-inode))
          collect
          (let ((row (physical-reseed-install-old-observe fd (copy-seq name) mount))
                (inode (physical-reseed-follow-configuration-guard-facts fd stream name)))
            (unless (and (= inode (getf expected key))
                         (equal row (physical-reseed-install-old-observe fd name mount)))
              (error "retirement guard differs from its recorded inode or stable facts"))
            row))))

(defun call-with-physical-reseed-follow-retirement-directory (root value function parent-check)
  ;; Borrow ROOT and hold the named directory and both guard streams around
  ;; the callback.  VERIFY is valid only on this thread within the scope.
  (let ((expected (physical-adoption-attempt-value-copy value))
        (fd nil) (active nil) (thread sb-thread:*current-thread*))
    (unwind-protect
         (progn
           (funcall parent-check)
           (sb-sys:without-interrupts
             (setf fd (physical-backup-open-directory (pathname (getf expected :directory)))))
           (physical-reseed-follow-retirement-directory-check root expected fd)
           (unless (zerop (physical-store-flock fd 6)) (error "retirement directory is already owned"))
           (call-with-physical-reseed-install-guard
            fd ".pg_lisp.lock" nil
            (lambda (lock)
              (call-with-physical-reseed-install-guard
               fd "physical-reseed.install" nil
               (lambda (fence)
                 (let ((guards (physical-reseed-follow-retirement-guard-observation fd expected lock fence)))
                   (flet ((verify ()
                            (unless (and active (eq thread sb-thread:*current-thread*))
                              (error "retirement directory scope ended or changed thread"))
                            (funcall parent-check)
                            (physical-reseed-follow-retirement-directory-check root expected fd)
                            (unless (equal guards (physical-reseed-follow-retirement-guard-observation fd expected lock fence))
                              (error "retirement guards changed during their held scope"))))
                     (unwind-protect
                          (progn
                            (setf active t)
                            (verify)
                            (multiple-value-prog1 (funcall function fd #'verify) (verify)))
                       (setf active nil)))))))))
      (setf active nil)
      (when fd (sb-posix:close fd)))))

(defun physical-reseed-follow-retirement-check-workspace (record plan)
  ;; Reject workspace overlap with either installation auxiliary before
  ;; acquiring the remaining directories.
  (let* ((workspace (getf (getf record :configuration) :workspace))
         (directory (pathname (getf workspace :directory))))
    (dolist (other (list (getf plan :incoming) (getf plan :quarantine)))
      (let ((path (pathname (getf other :directory))))
        (when (or (uiop:subpathp directory path) (uiop:subpathp path directory)
                  (and (= (getf workspace :device) (getf other :device))
                       (= (getf workspace :inode) (getf other :inode))))
          (error "retirement workspace overlaps an installation auxiliary")))))
  t)

(defun call-with-physical-reseed-follow-retirement-auxiliaries (root plan function check)
  ;; Hold incoming and quarantine together and chain their checks around the
  ;; callback.
  (call-with-physical-reseed-follow-retirement-directory
   root (getf plan :incoming)
   (lambda (incoming incoming-check)
     (call-with-physical-reseed-follow-retirement-directory
      root (getf plan :quarantine)
      (lambda (quarantine quarantine-check)
        (funcall function incoming quarantine quarantine-check))
      incoming-check))
   check))

(defun physical-reseed-follow-retirement-check-receive (descriptors store record base binding check)
  ;; Check the exact activated receive tuple and retained prefix, then validate
  ;; the active follow state against the held base directories.
  (funcall check)
  (physical-store-check-live store)
  (physical-store-check-complete-input store)
  (unless (equal (getf binding :after) (physical-store-control-tuple store))
    (error "retirement receive tuple differs from the exact activated selection"))
  (physical-store-read-retained-control store (getf binding :after))
  (physical-store-validate-prefix store)
  (call-with-physical-reseed-follow-held-base-directory
   (physical-reseed-follow-configuration-role-fd descriptors :root)
   (physical-reseed-follow-configuration-role-fd descriptors :base)
   (physical-reseed-follow-configuration-role-fd descriptors :global)
   (physical-reseed-follow-configuration-role-fd descriptors :wal)
   (lambda (directory)
     (let ((*pgdata* directory))
       (physical-follow-check-active-state
        store base (physical-reseed-follow-activation-config record binding))))
   check)
  (funcall check)
  t)

(defun physical-reseed-follow-retirement-root-check (root record parent-check)
  ;; Check the borrowed root against the recorded identity and mount after
  ;; startup quiescence and the caller's parent check.
  (physical-adoption-check-startup-quiescence)
  (funcall parent-check)
  (let ((local (getf (getf record :attempt) :local))
        (stat (physical-image-check-directory root)))
    (physical-image-check-target (pathname (getf local :directory)) root)
    (unless (and (= (getf local :device) (sb-posix:stat-dev stat))
                 (= (getf local :inode) (sb-posix:stat-ino stat))
                 (= (physical-adoption-install-mount-id (pathname (getf local :directory)))
                    (physical-adoption-install-mount-id nil :directory-fd root)))
      (error "retirement root differs from its recorded named descriptor")))
  t)

(defun call-with-physical-reseed-follow-retirement-directories (root value function check)
  ;; The provisional plan read accepts the checked pending install alias while
  ;; these directory guards are held.
  ;; Borrow ROOT and hold the other nine directory descriptors and guards for
  ;; the callback.  BASE and BINDING are fresh internal facts.  Copy them
  ;; before saving them as operation evidence.
  (let* ((record (physical-reseed-follow-copy value))
         (configuration (getf record :configuration))
         (directories (getf configuration :directories))
         (workspace (append (getf configuration :workspace)
                            (list :lock-inode (getf directories :lock-inode)
                                  :fence-inode (getf directories :fence-inode)))))
    (call-with-physical-reseed-follow-retirement-directory
     root workspace
     (lambda (workspace-fd workspace-check)
       (call-with-physical-reseed-follow-fixed-configuration-directories
        workspace-fd record
        (lambda (staged saved fixed-check)
          (let ((plan (physical-reseed-follow-retirement-provisional-plan-at root saved record fixed-check)))
            (physical-reseed-follow-retirement-check-workspace record plan)
            (call-with-physical-reseed-follow-retirement-auxiliaries
             root plan
             (lambda (incoming quarantine auxiliary-check)
               (call-with-physical-reseed-follow-base-directories
                root plan
                (lambda (base-fds base-check)
                  (let ((descriptors (append (list (cons :root root) (cons :workspace workspace-fd)
                                                   (cons :staged staged) (cons :saved saved)
                                                   (cons :incoming incoming) (cons :quarantine quarantine))
                                             base-fds)))
                    (multiple-value-bind (base control binding model images row)
                        (physical-reseed-follow-configuration-base-proof
                         root saved base-fds plan record (getf configuration :step) base-check)
                      (declare (ignore control model images row))
                      (call-with-physical-reseed-follow-installation-receive
                       descriptors record binding (getf configuration :step)
                       (lambda (held) (funcall function held base binding base-check))
                       base-check))))
                auxiliary-check))
             fixed-check)))
        workspace-check))
     check)))

(defun call-with-physical-reseed-follow-retirement-store (descriptors base function check)
  ;; Open the receive store, preserving its tail and using its existing lock.
  ;; Normal process-local retained-root restoration still applies.  ON-OPEN
  ;; captures the store for cleanup if opening exits nonlocally.
  ;; VERIFY checks this thread, the parent scope and store liveness.  It is
  ;; revoked before the store closes.  Callers supply phase-specific proof.
  (let ((store nil) (active nil) (thread sb-thread:*current-thread*))
    (unwind-protect
         (progn
           (funcall check)
           (physical-store-open
            (physical-image-fd-path (physical-reseed-follow-configuration-role-fd descriptors :receive))
            (getf base :system-id) (getf base :timeline) :start-lsn (getf base :end)
            :discard-tail nil :create-lock nil
            :retained-identity (physical-recovery-retained-identity base)
            :retained-directory (merge-pathnames
                                 "pg_physical_retained/"
                                 (physical-image-fd-path (physical-reseed-follow-configuration-role-fd descriptors :root)))
            :on-open (lambda (opened) (setf store opened)))
           (setf active t)
           (flet ((verify ()
                    (unless (and active (eq thread sb-thread:*current-thread*))
                      (error "retirement store scope ended or changed thread"))
                    (funcall check)
                    (physical-store-check-live store)
                    t))
             (verify)
             (multiple-value-prog1 (funcall function store #'verify) (verify))))
      (setf active nil)
      (when store (physical-store-close store)))))

(defun call-with-physical-reseed-follow-retirement-resources (root value parent-check function)
  ;; Borrow the caller's root and hold the remaining resources around the
  ;; callback.  Its verifier covers resource lifetime and liveness.  The caller
  ;; freezes the fresh base and binding facts and proves the relevant phase.
  (let ((record (physical-reseed-follow-copy value)))
    (flet ((root-check ()
             (physical-reseed-follow-retirement-root-check root record parent-check)))
      (root-check)
      (call-with-physical-reseed-follow-retirement-directories
       root record
       (lambda (descriptors base binding check)
         (call-with-physical-reseed-follow-retirement-store
          descriptors base
          (lambda (store verify) (funcall function (copy-tree descriptors) store base binding verify))
          check))
       #'root-check))))

(defun physical-reseed-follow-retirement-named-row (directory name mount)
  (let* ((trimmed (string-right-trim "/" directory))
         (path (if (equal name ".")
                   (if (plusp (length trimmed)) trimmed "/.")
                   (namestring (merge-pathnames name (pathname directory)))))
         (row (physical-reseed-install-old-observe +physical-adoption-at-fdcwd+ path mount)))
    (cons name (rest row))))

(defun physical-reseed-follow-retirement-path-fact (path)
  ;; The cleanup stamp supplements an already authenticated full proof.  Its
  ;; ancestor rows retain identity, type, owner and mount, but omit volatile
  ;; metadata that sibling changes can affect.  Role and child rows stay exact.
  (let* ((mount (physical-adoption-install-mount-id path))
         (row (physical-reseed-install-old-observe +physical-adoption-at-fdcwd+ path mount)))
    (unless (= #o040000 (logand #o170000 (fourth row)))
      (error "retirement observation changed during resource cleanup"))
    (list path mount (list (second row) (third row) (fourth row) (sixth row)))))

(defun physical-reseed-follow-retirement-path-facts (directory)
  (let ((parts (pathname-directory (pathname directory))))
    (loop for end from 1 to (length parts)
          for prefix = (namestring (make-pathname :directory (subseq parts 0 end) :name nil :type nil))
          for trimmed = (string-right-trim "/" prefix)
          collect (physical-reseed-follow-retirement-path-fact
                   (if (plusp (length trimmed)) trimmed "/.")))))

(defun physical-reseed-follow-retirement-role-stamp (descriptors names)
  (unless (= 10 (length descriptors))
    (error "retirement cleanup requires exactly ten held directory roles"))
  (let ((paths nil)
        (roles nil))
    (dolist (role '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive))
      (let* ((fd (physical-reseed-follow-configuration-role-fd descriptors role))
             (directory (namestring (uiop:ensure-directory-pathname (truename (physical-image-fd-path fd)))))
             (mount (physical-adoption-install-mount-id nil :directory-fd fd))
             (row (physical-reseed-install-old-observe fd "." mount))
             (entries (loop for name in (getf names role)
                            collect (physical-reseed-install-old-observe fd name mount))))
        (unless (and (equal row (physical-reseed-install-old-observe fd "." mount))
                     (equal row (physical-reseed-follow-retirement-named-row directory "." mount)))
          (error "retirement observation changed during resource cleanup"))
        (dolist (binding (physical-reseed-follow-retirement-path-facts directory))
          (pushnew binding paths :test #'equal))
        (push (list role directory mount row entries) roles)))
    (list :paths (stable-sort paths #'string< :key #'first) :roles (nreverse roles))))

(defun physical-reseed-follow-retirement-check-stamp (stamp)
  (unless (and (getf stamp :paths)
               (equal (mapcar #'first (getf stamp :roles))
                      '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive)))
    (error "retirement cleanup requires a complete private role stamp"))
  (dolist (binding (getf stamp :paths))
    (unless (equal binding (physical-reseed-follow-retirement-path-fact (first binding)))
      (error "retirement observation changed during resource cleanup")))
  (dolist (entry (getf stamp :roles))
    (destructuring-bind (role directory mount row entries) entry
      (declare (ignore role))
      (unless (and (equal row (physical-reseed-follow-retirement-named-row directory "." mount))
                   (every (lambda (file)
                            (equal file (physical-reseed-follow-retirement-named-row directory (first file) mount)))
                          entries)
                   (equal row (physical-reseed-follow-retirement-named-row directory "." mount)))
        (error "retirement observation changed during resource cleanup"))))
  t)

(defun physical-reseed-follow-retirement-audit-stamp (stamp parent-check)
  ;; Named statx observations open no new teardown scope.  They compare
  ;; metadata under cooperating locks at the statx observation boundary.
  ;; The parent check may itself close a PID descriptor, so finish with a pure
  ;; stamp check after its last invocation.
  (funcall parent-check)
  (physical-reseed-follow-retirement-check-stamp stamp)
  (funcall parent-check)
  (physical-reseed-follow-retirement-check-stamp stamp))

(defun call-with-physical-reseed-follow-retirement-receive-core
    (descriptors snapshot base binding function check layout-reader)
  ;; Repeat the same layout proof around active receive validation.
  ;; Full root metadata must remain exact.  Creating and removing an entry
  ;; invalidates this observation, so recovery requires a fresh scope.
  ;; The callback gets copies.  Revoke its thread-bound VERIFY before the
  ;; store scope cleans up.
  (let* ((record (physical-reseed-follow-copy (getf snapshot :record)))
         (thread sb-thread:*current-thread*) (active nil) (state nil))
    (funcall check)
    (setf state (funcall layout-reader descriptors check))
    (call-with-physical-reseed-follow-retirement-store
     descriptors base
     (lambda (store store-check)
       (declare (ignore store-check))
       (unwind-protect
            (progn
              (setf active t)
              (flet ((verify ()
                       (unless (and active (eq thread sb-thread:*current-thread*))
                         (error "retirement observation scope ended or changed thread"))
                       (funcall check)
                       (unless (equal state (funcall layout-reader descriptors check))
                         (error "retirement layout changed during its observation scope"))
                       (physical-reseed-follow-retirement-check-receive descriptors store record base binding check)
                       (unless (equal state (funcall layout-reader descriptors check))
                         (error "retirement layout changed around active receive validation"))
                       t))
                (verify)
                (multiple-value-prog1
                    (funcall function (copy-tree descriptors) (physical-adoption-attempt-value-copy state) #'verify)
                  (verify))))
         (setf active nil)))
     check)))

(defun call-with-physical-reseed-follow-retirement-receive-at
    (descriptors snapshot base binding function check extra-reader candidate &optional pid-evidence)
  (call-with-physical-reseed-follow-retirement-receive-core
   descriptors snapshot base binding function check
   (lambda (held verify)
     (physical-reseed-follow-retirement-layout-state-at
      held snapshot verify extra-reader candidate pid-evidence))))

(defun call-with-physical-reseed-follow-retirement-observation-core
    (root snapshot parent-check function extra-reader layout-reader)
  ;; Root checks use EXTRA-READER to keep the source facts stable.
  ;; LAYOUT-READER supplies the complete proof within the held receive scope.
  ;; Borrow ROOT and its parent ownership from the caller.  Hold the other nine
  ;; directory descriptors, guard streams and local store for the callback.
  ;; Capture the stamp from private copies while all ten roles are held, before
  ;; the mandatory final verifier.  Audit it after the owned resources close.
  (let* ((snapshot (physical-adoption-attempt-value-copy snapshot))
         (record (physical-reseed-follow-copy (getf snapshot :record)))
         (stamp nil))
    (flet ((root-check ()
             (physical-reseed-follow-retirement-root-check root record parent-check)
             (funcall extra-reader root)))
      (root-check)
      (multiple-value-prog1
          (call-with-physical-reseed-follow-retirement-directories
           root record
           (lambda (descriptors base binding check)
             (call-with-physical-reseed-follow-retirement-receive-core
              descriptors snapshot base binding
              (lambda (held state verify)
                (let ((private-descriptors (copy-tree held))
                      (names (physical-adoption-attempt-value-copy (getf (getf state :layout) :names))))
                  (multiple-value-prog1 (funcall function held state verify)
                    (setf stamp (physical-reseed-follow-retirement-role-stamp private-descriptors names)))))
              check layout-reader))
           #'root-check)
        (physical-reseed-follow-retirement-audit-stamp stamp #'root-check)))))

(defun call-with-physical-reseed-follow-retirement-observation-at
    (root snapshot parent-check function extra-reader candidate &optional pid-evidence)
  (let ((snapshot (physical-adoption-attempt-value-copy snapshot)))
    (call-with-physical-reseed-follow-retirement-observation-core
     root snapshot parent-check function extra-reader
     (lambda (descriptors check)
       (physical-reseed-follow-retirement-layout-state-at
        descriptors snapshot check extra-reader candidate pid-evidence)))))

(defun call-with-physical-reseed-follow-retirement-receive
    (descriptors snapshot base binding function check)
  ;; Use the strict extra-entry policy without a temporary candidate.
  (call-with-physical-reseed-follow-retirement-receive-at
   descriptors snapshot base binding function check
   (lambda (root) (physical-reseed-follow-retirement-extra-at root snapshot))
   nil))

(defun call-with-physical-reseed-follow-retirement-observation (root snapshot parent-check function)
  ;; Use the strict policy that refuses a temporary journal.
  (let ((snapshot (physical-adoption-attempt-value-copy snapshot)))
    (call-with-physical-reseed-follow-retirement-observation-at
     root snapshot parent-check function
     (lambda (fd) (physical-reseed-follow-retirement-extra-at fd snapshot))
     nil)))

(defun call-with-physical-reseed-follow-retirement-temporary-observation (root snapshot parent-check function)
  ;; Hold the temporary or its absence while proving the final layout and
  ;; optional next candidate.  The callback receives copied facts and a check
  ;; valid only in this scope.  It grants no mutation or startup authority.
  (let ((snapshot (physical-adoption-attempt-value-copy snapshot)))
    (call-with-physical-reseed-follow-retirement-next-temporary
     root snapshot parent-check
     (lambda (temporary verify)
       (flet ((extra (fd)
                (funcall verify)
                (append (physical-reseed-follow-retirement-final-extra-at fd snapshot)
                        (when temporary
                          (list (cons (copy-seq "physical-reseed.follow.tmp")
                                      (physical-adoption-attempt-value-copy (getf temporary :row))))))))
         (call-with-physical-reseed-follow-retirement-observation-at
          root snapshot parent-check function #'extra (getf temporary :record)))))))
