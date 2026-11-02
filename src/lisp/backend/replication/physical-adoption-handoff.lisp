;;; Publish the empty receive spool for an installed adoption.
;;;
;;; With offline ownership, prepare a separate guarded workspace and create an
;;; empty spool at the proved stage end.  Sync its tree and ancestors before
;;; publishing the immutable handoff intent in PGDATA.  The intent authorizes
;;; one no-replace rename of the spool into PGDATA; sync both parents.
;;;
;;; Resume validates the intent and either spool location before proceeding.
;;; A pre-intent interruption can leave a guarded workspace orphan; retry
;;; preparation with a new empty workspace.  After intent publication use
;;; handoff resume, since installation resume does not accept this namespace.
;;; Ready means the spool is published, not that startup is admitted.

(in-package #:pg)

(defvar *physical-adoption-handoff-write-limit* nil)

(defun physical-adoption-handoff-create-workspace (owner installation directory)
  (let* ((root (physical-adoption-owner-directory owner))
         (plan (getf installation :plan))
         (fact (physical-adoption-install-directory-fact directory))
         (mount (physical-adoption-install-check-roles owner plan)))
    (unless (and (= (getf fact :device) (physical-adoption-owner-device owner))
                 (= (getf fact :mount) mount))
      (error "adoption handoff workspace crosses a filesystem or mount"))
    (dolist (role '(:root :stage :workspace))
      (let ((other (physical-adoption-install-directory plan role)))
        (when (or (uiop:subpathp directory other) (uiop:subpathp other directory))
          (error "adoption handoff workspace overlaps protected data"))))
    (physical-adoption-install-prepare-auxiliary directory)
    (let* ((spool (merge-pathnames "spool/" directory))
           (stage (getf (getf plan :attempt) :stage)))
      (sb-posix:mkdir (namestring spool) #o700)
      (let ((store (physical-store-open spool (getf stage :system-id) (getf stage :timeline)
                                        :create t :start-lsn (getf stage :end))))
        (physical-store-close store))
      (sb-posix:chmod (namestring (merge-pathnames "receive.control" spool)) #o600)
      (physical-adoption-install-test-point :after-handoff-spool-create spool)
      (unless (= (getf fact :mount) (physical-adoption-install-mount-id root))
        (error "adoption root mount changed during handoff preparation"))
      (values (physical-adoption-install-persistent-fact
               (physical-adoption-install-auxiliary-fact directory) :directory t)
              (physical-adoption-attempt-fields-copy
               (physical-adoption-install-directory-fact spool) '(:device :inode))
              (physical-adoption-install-snapshot spool)))))

(defun physical-adoption-handoff-check-prepared (owner value)
  (let* ((installation (physical-adoption-install-read owner))
         (workspace (physical-adoption-handoff-workspace value))
         (pending (merge-pathnames "intent.pending" workspace)))
    (physical-adoption-install-validate owner installation)
    (physical-adoption-handoff-check-installation owner value)
    (unless (and (physical-adoption-install-names-p
                  workspace '(".pg_lisp.lock" "intent.pending" "physical-adoption.install" "spool"))
                 (equal value (physical-adoption-handoff-read-file owner pending)))
      (error "adoption pending handoff intent or workspace changed"))
    (physical-adoption-handoff-check-spool owner value :prepared)
    (physical-adoption-install-validate owner installation)
    value))

(defun physical-adoption-handoff-write-pending (owner value)
  (let* ((workspace (physical-adoption-handoff-workspace value))
         (path (merge-pathnames "intent.pending" workspace))
         (identity (getf value :intent-file))
         (image (physical-adoption-handoff-image value))
         (fd (sb-posix:open (namestring path)
                            (logior sb-posix:o-rdwr sb-posix:o-nofollow +postmaster-o-cloexec+))))
    (unwind-protect
         (let ((stat (sb-posix:fstat fd)))
           (unless (and (zerop (sb-posix:stat-size stat))
                        (sb-posix:s-isreg (sb-posix:stat-mode stat))
                        (= #o600 (logand #o7777 (sb-posix:stat-mode stat)))
                        (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                        (= 1 (sb-posix:stat-nlink stat))
                        (= (getf identity :device) (sb-posix:stat-dev stat))
                        (= (getf identity :inode) (sb-posix:stat-ino stat)))
             (error "adoption handoff scratch identity changed before writing"))
           (physical-adoption-install-write-bytes
            fd image 0 path :limit *physical-adoption-handoff-write-limit*
            :point :after-handoff-intent-write-chunk)
           (sb-posix:fsync fd)
           (physical-adoption-install-test-point :after-handoff-intent-file-fsync path))
      (sb-posix:close fd))
    (physical-adoption-handoff-check-prepared owner value)
    (physical-adoption-install-sync-tree
     workspace (physical-adoption-install-snapshot workspace :exclude *physical-adoption-install-retained-names*))
    (physical-adoption-install-sync-ancestors workspace)
    (physical-adoption-handoff-check-prepared owner value)))

(defun physical-adoption-handoff-sync-parents (owner value)
  (dolist (pair (list (cons :after-handoff-workspace-fsync (physical-adoption-handoff-workspace value))
                      (cons :after-handoff-root-fsync (physical-adoption-owner-directory owner))))
    (fsync-directory (cdr pair))
    (physical-adoption-install-test-point (car pair) (cdr pair))))

(defun physical-adoption-handoff-publish (owner value)
  (let* ((workspace (physical-adoption-handoff-workspace value))
         (pending (merge-pathnames "intent.pending" workspace))
         (target (physical-adoption-handoff-path (physical-adoption-owner-directory owner))))
    (physical-adoption-install-test-point :before-handoff-intent-rename target)
    (physical-adoption-handoff-check-prepared owner value)
    (physical-adoption-install-rename pending target)
    (physical-adoption-install-test-point :after-handoff-intent-rename target)
    (physical-adoption-handoff-sync-parents owner value)
    (unless (eq :prepared (physical-adoption-handoff-validate owner value))
      (error "adoption handoff changed during intent publication"))
    value))

(defun physical-adoption-handoff-prepare (owner directory)
  "Prepare and publish handoff intent under an existing offline OWNER.
DIRECTORY must be a new empty private workspace, disjoint from the protected
installation directories and on the same filesystem and mount."
  (let* ((directory (uiop:ensure-directory-pathname directory))
         (root (physical-adoption-owner-directory owner))
         (installation (physical-adoption-install-read owner))
         (plan (getf installation :plan)))
    (physical-adoption-install-validate owner installation)
    (unless (and (= (getf installation :step) (physical-adoption-install-last-step plan))
                 (eq :installed (getf (physical-adoption-attempt-read owner) :phase)))
      (error "adoption handoff requires a completed installed attempt"))
    (physical-adoption-require-absent (physical-adoption-handoff-path root)
                                      "adoption handoff intent already exists")
    (multiple-value-bind (workspace spool entries)
        (physical-adoption-handoff-create-workspace owner installation directory)
      (let ((value (physical-adoption-handoff-copy
                    (list :version 1 :installation plan
                          :attempt-file (physical-adoption-handoff-file-fact
                                         (physical-adoption-attempt-path root)
                                         +physical-adoption-attempt-maximum-bytes+)
                          :journal-file (physical-adoption-handoff-file-fact
                                         (physical-adoption-install-path root)
                                         +physical-adoption-install-maximum-bytes+)
                          :workspace workspace :spool spool :spool-entries entries
                          :intent-file (physical-adoption-allocate-private directory "intent.pending")))))
        (physical-adoption-handoff-write-pending owner value)
        (physical-adoption-handoff-publish owner value)))))

(defun physical-adoption-handoff-resume (owner)
  "Resume a published handoff under an existing offline OWNER.
Return the intent and :READY once the empty receive spool is published.
This does not admit startup or perform follow configuration."
  (let* ((value (physical-adoption-handoff-read owner))
         (root (physical-adoption-owner-directory owner))
         (location (physical-adoption-handoff-validate owner value))
         (path (physical-adoption-handoff-path root)))
    (physical-adoption-install-sync-tree
     root (list (list "physical-adoption.handoff"
                      (physical-adoption-install-tree
                       root "physical-adoption.handoff" (physical-adoption-owner-device owner)
                       (physical-adoption-install-mount-id root)))))
    (physical-adoption-handoff-sync-parents owner value)
    (unless (eq location (physical-adoption-handoff-validate owner value))
      (error "adoption handoff advanced before its planned move"))
    (when (eq location :prepared)
      (physical-adoption-install-test-point :before-handoff-spool-rename path)
      (unless (eq :prepared (physical-adoption-handoff-validate owner value))
        (error "adoption handoff advanced before receive spool publication"))
      (physical-adoption-install-rename
       (physical-adoption-handoff-spool owner value :prepared)
       (physical-adoption-handoff-spool owner value :ready))
      (physical-adoption-install-test-point :after-handoff-spool-rename path)
      (physical-adoption-handoff-sync-parents owner value))
    (unless (eq :ready (physical-adoption-handoff-validate owner value))
      (error "adoption handoff did not publish its empty receive spool"))
    (values value :ready)))

(defun physical-adoption-handoff-start (owner directory)
  "Prepare and publish the empty receive spool under offline OWNER.
DIRECTORY is a new empty private workspace for this handoff."
  (physical-adoption-handoff-prepare owner directory)
  (physical-adoption-handoff-resume owner))
