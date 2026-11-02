;;; Validate and prepare the offline installation namespace.
;;;
;;; The workspace is private, empty and disjoint from PGDATA and stage.
;;; Before publishing the plan, preparation syncs the original tree and the
;;; stage, as well as directory ancestors, so the plan does not outrun the
;;; bytes and names it protects.  Resume rechecks identities, contents and
;;; exact names against the recorded state or one move ahead.  A missing
;;; journal does not clear an installing attempt's startup fence.

(in-package #:pg)

(defun physical-adoption-install-directory (plan role)
  (ecase role
    (:root (pathname (getf (getf (getf plan :attempt) :local) :directory)))
    (:stage (pathname (getf (getf (getf plan :attempt) :stage) :directory)))
    (:workspace (pathname (getf (getf plan :workspace) :directory)))
    (:incoming (merge-pathnames "incoming/" (physical-adoption-install-directory plan :workspace)))
    (:quarantine (merge-pathnames "quarantine/" (physical-adoption-install-directory plan :workspace)))))

(defun physical-adoption-install-persistent-fact (fact &key directory)
  (physical-adoption-attempt-fields-copy
   fact (append (when directory '(:directory)) '(:device :inode :lock-inode :fence-inode))))

(defun physical-adoption-install-names-p (directory names)
  (equal (sort (copy-list names) #'string<) (physical-adoption-install-entry-names directory)))

(defun physical-adoption-install-check-roles (owner plan &key (journal-present t))
  (physical-adoption-attempt-check-owner owner (getf plan :attempt))
  (let* ((root (physical-adoption-install-directory plan :root))
         (root-fact (physical-adoption-install-directory-fact root))
         (mount (getf root-fact :mount))
         (stage (getf (getf plan :attempt) :stage))
         (stage-fact (physical-adoption-install-directory-fact
                      (physical-adoption-install-directory plan :stage))))
    (unless (and (= (getf stage :device) (getf stage-fact :device))
                 (= (getf stage :inode) (getf stage-fact :inode))
                 (= mount (getf stage-fact :mount)))
      (error "adoption stage directory lost its installation identity"))
    (dolist (role '(:workspace :incoming :quarantine))
      (let ((actual (physical-adoption-install-auxiliary-fact
                     (physical-adoption-install-directory plan role))))
        (unless (and (equal (getf plan role)
                            (physical-adoption-install-persistent-fact actual :directory (eq role :workspace)))
                     (= mount (getf actual :mount)))
          (error "adoption installation directory or fence changed"))))
    (dolist (name (append '(".pg_lisp.lock" "physical-adoption.attempt")
                          (when journal-present '("physical-adoption.install"))))
      (unless (= mount (physical-adoption-install-mount-id (merge-pathnames name root)))
        (error "adoption installation management file crosses a mount")))
    mount))

(defun physical-adoption-install-coordinates (plan step)
  (let* ((old (length (getf plan :old))) (new (length (getf plan :new)))
                                         (moves (min step (+ old new))))
    (values (min moves old) (max 0 (- moves old)))))

(defun physical-adoption-install-root-entries (plan evacuated installed)
  (sort (append (subseq (getf plan :old) evacuated) (subseq (getf plan :new) 0 installed))
        #'string< :key #'first))

(defun physical-adoption-install-top-p (directory entry mount)
  (handler-case
      (let* ((row (first (second entry)))
             (path (physical-adoption-install-entry-path directory (first entry)))
             (stat (sb-posix:lstat (namestring path))))
        (and (physical-adoption-install-row-identity-p row stat)
             (= mount (physical-adoption-install-mount-id path))))
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition)) (error condition)))))

(defun physical-adoption-install-namespace-p (plan step mount)
  (multiple-value-bind (evacuated installed) (physical-adoption-install-coordinates plan step)
    (let ((root (physical-adoption-install-directory plan :root))
          (incoming (physical-adoption-install-directory plan :incoming))
          (quarantine (physical-adoption-install-directory plan :quarantine)))
      (and (physical-adoption-install-names-p
            root (append (copy-list *physical-adoption-install-retained-names*)
                         (mapcar #'first (physical-adoption-install-root-entries plan evacuated installed))))
           (physical-adoption-install-names-p
            incoming (append '(".pg_lisp.lock" "physical-adoption.install")
                             (mapcar #'first (subseq (getf plan :new) installed))))
           (physical-adoption-install-names-p
            quarantine (append '(".pg_lisp.lock" "physical-adoption.install")
                               (mapcar #'first (subseq (getf plan :old) 0 evacuated))))
           (loop for entry in (getf plan :old) for index from 0
                 always (physical-adoption-install-top-p (if (< index evacuated) quarantine root) entry mount))
           (loop for entry in (getf plan :new) for index from 0
                 always (physical-adoption-install-top-p (if (< index installed) root incoming) entry mount))))))

(defun physical-adoption-install-plan-inodes (plan)
  (append
   (list (getf (getf (getf plan :attempt) :local) :inode)
         (getf (getf (getf plan :attempt) :local) :lock-inode)
         (getf (getf (getf plan :attempt) :stage) :inode))
   (loop for role in '(:workspace :incoming :quarantine :journal)
         append (loop for key in '(:inode :lock-inode :fence-inode)
                      for value = (getf (getf plan role) key) when value collect value))
   (loop for key in '(:old :stage-tree :new)
         append (loop for entry in (getf plan key) append (mapcar #'fifth (second entry))))))

(defun physical-adoption-install-check-scratch (owner state attempt observed)
  (let* ((plan (getf state :plan))
         (workspace (physical-adoption-install-directory plan :workspace))
         (scratch (merge-pathnames "attempt.pending" workspace))
         (names (physical-adoption-install-entry-names workspace))
         (present (member "attempt.pending" names :test #'equal))
         (step (getf state :step))
         (last (physical-adoption-install-last-step plan)))
    (unless (physical-adoption-install-names-p
             workspace (append '(".pg_lisp.lock" "physical-adoption.install" "incoming" "quarantine")
                               (when present '("attempt.pending"))))
      (error "unexpected entry in adoption installation workspace"))
    (when present
      (unless (or (and (zerop step) (zerop observed) (eq :proved (getf attempt :phase)))
                  (and (= step last) (eq :installing (getf attempt :phase))))
        (error "adoption attempt scratch is outside its publication window"))
      (let* ((stat (sb-posix:lstat (namestring scratch)))
             (root (physical-adoption-owner-directory owner))
             (attempt-inode (sb-posix:stat-ino (sb-posix:lstat (namestring (physical-adoption-attempt-path root))))))
        (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                     (= #o600 (logand #o7777 (sb-posix:stat-mode stat)))
                     (= 1 (sb-posix:stat-nlink stat)) (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                     (= (getf (getf plan :workspace) :device) (sb-posix:stat-dev stat))
                     (= (physical-adoption-install-mount-id root) (physical-adoption-install-mount-id scratch))
                     (<= (sb-posix:stat-size stat) +physical-adoption-attempt-maximum-bytes+)
                     (/= attempt-inode (sb-posix:stat-ino stat))
                     (not (member (sb-posix:stat-ino stat) (physical-adoption-install-plan-inodes plan))))
          (error "adoption attempt scratch lost its private disposable identity"))))
    present))

(defun physical-adoption-install-check-content (plan observed)
  (multiple-value-bind (evacuated installed) (physical-adoption-install-coordinates plan observed)
    (dolist (pair (list (list :root (physical-adoption-install-root-entries plan evacuated installed))
                        (list :incoming (subseq (getf plan :new) installed))
                        (list :quarantine (subseq (getf plan :old) 0 evacuated))))
      (unless (equal (second pair)
                     (physical-adoption-install-snapshot
                      (physical-adoption-install-directory plan (first pair))
                      :exclude *physical-adoption-install-retained-names*))
        (error "adoption installation object contents changed")))
    (unless (equal (getf plan :stage-tree)
                   (physical-adoption-install-snapshot (physical-adoption-install-directory plan :stage)))
      (error "adoption canonical stage changed during installation"))))

(defun physical-adoption-install-validate (owner state)
  (unless (equal state (physical-adoption-install-read owner))
    (error "adoption installation journal changed"))
  (let* ((plan (getf state :plan))
         (attempt (physical-adoption-attempt-read owner))
         (normalized (copy-list attempt))
         (mount (physical-adoption-install-check-roles owner plan))
         (step (getf state :step))
         (moves (1- (physical-adoption-install-last-step plan)))
         (observed
           (cond ((physical-adoption-install-namespace-p plan step mount) (min step moves))
                 ((and (< step moves) (physical-adoption-install-namespace-p plan (1+ step) mount)) (1+ step))
                 (t (error "adoption installation namespace is not its recorded state or one move ahead")))))
    (setf (getf normalized :phase) :proved)
    (unless (and (equal normalized (getf plan :attempt))
                 (case (getf attempt :phase)
                   (:proved (and (zerop step) (zerop observed) (not (getf state :partial))))
                   (:installing t)
                   (:installed (= step (1+ moves)))))
      (error "adoption attempt phase does not authorize its installation journal"))
    (when (and (getf state :partial) (/= observed (min (1+ step) moves)))
      (error "partial adoption progress has no corresponding completed operation"))
    (physical-adoption-install-check-scratch owner state attempt observed)
    (physical-adoption-install-check-content plan observed)
    (when (= observed moves)
      (physical-adoption-install-check-image
       (physical-adoption-install-directory plan :root) (getf plan :attempt) :installed t))
    (unless (and (= mount (physical-adoption-install-check-roles owner plan))
                 (physical-adoption-install-namespace-p plan observed mount)
                 (equal attempt (physical-adoption-attempt-read owner))
                 (equal state (physical-adoption-install-read owner)))
      (error "adoption installation context changed during verification"))
    (physical-adoption-install-check-scratch owner state attempt observed)
    observed))

(defun physical-adoption-install-validate-prepared (owner plan)
  (let* ((root (physical-adoption-owner-directory owner))
         (workspace (physical-adoption-install-directory plan :workspace))
         (incoming (physical-adoption-install-directory plan :incoming))
         (quarantine (physical-adoption-install-directory plan :quarantine))
         (pending (merge-pathnames "journal.pending" workspace))
         (mount (physical-adoption-install-check-roles owner plan :journal-present nil))
         (stat (sb-posix:lstat (namestring pending))))
    (physical-adoption-check-no-install root)
    (unless (and (equal (getf plan :attempt) (physical-adoption-attempt-read owner))
                 (physical-adoption-install-names-p
                  root (append '(".pg_lisp.lock" "physical-adoption.attempt") (mapcar #'first (getf plan :old))))
                 (physical-adoption-install-names-p
                  workspace '(".pg_lisp.lock" "physical-adoption.install" "incoming" "quarantine" "journal.pending"))
                 (physical-adoption-install-names-p quarantine '(".pg_lisp.lock" "physical-adoption.install"))
                 (physical-adoption-install-names-p
                  incoming (append '(".pg_lisp.lock" "physical-adoption.install") (mapcar #'first (getf plan :new))))
                 (physical-adoption-install-journal-stat-p stat plan)
                 (= mount (physical-adoption-install-mount-id pending)))
      (error "adoption installation preparation changed before publication"))
    (physical-adoption-attempt-validate-context owner (getf plan :attempt))
    (physical-adoption-install-check-content plan 0)
    (physical-adoption-install-check-image incoming (getf plan :attempt) :auxiliary t)
    (when (plusp (sb-posix:stat-size stat))
      (let ((pending-state (physical-adoption-install-decode
                            (read-bounded-regular-file pending +physical-adoption-install-maximum-bytes+))))
        (unless (and (equal plan (getf pending-state :plan))
                     (zerop (getf pending-state :step)) (not (getf pending-state :partial)))
          (error "adoption pending journal differs from its prepared plan"))))
    (physical-adoption-check-no-install root)
    t))

(defun physical-adoption-allocate-private (directory name)
  "Create a private scratch inode without replacing or following an entry."
  (let* ((path (physical-adoption-install-entry-path directory name))
         (fd (sb-posix:open (namestring path)
                            (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                    sb-posix:o-nofollow +postmaster-o-cloexec+) #o600)))
    (unwind-protect
         (progn
           (sb-posix:fchmod fd #o600)
           (sb-posix:fsync fd)
           (let ((stat (sb-posix:fstat fd)))
             (list :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))))
      (sb-posix:close fd))))

(defun physical-adoption-install-allocate-journal (workspace)
  (physical-adoption-allocate-private workspace "journal.pending"))

(defun physical-adoption-install-prepare (owner directory &key client)
  (let* ((root (physical-adoption-owner-directory (physical-adoption-check-owner owner)))
         (record (physical-adoption-attempt-read owner))
         (stage (pathname (getf (getf record :stage) :directory)))
         (workspace (uiop:ensure-directory-pathname directory))
         (workspace-fact (physical-adoption-install-directory-fact workspace))
         (root-fact (physical-adoption-install-directory-fact root))
         (incoming (merge-pathnames "incoming/" workspace))
         (quarantine (merge-pathnames "quarantine/" workspace)))
    (physical-adoption-check-no-install root)
    (unless (and (eq :proved (getf record :phase))
                 (null (physical-adoption-install-entry-names workspace))
                 (= (getf root-fact :device) (getf workspace-fact :device))
                 (= (getf root-fact :mount) (getf workspace-fact :mount))
                 (not (or (uiop:subpathp workspace root) (uiop:subpathp root workspace)
                          (uiop:subpathp workspace stage) (uiop:subpathp stage workspace))))
      (error "adoption installation needs a proved attempt and separate empty private workspace"))
    (physical-adoption-attempt-validate-context owner record)
    (dolist (phase '(:installing :installed))
      (let ((candidate (copy-list record)))
        (setf (getf candidate :phase) phase)
        (physical-adoption-attempt-image candidate)))
    (setf workspace-fact (physical-adoption-install-prepare-auxiliary workspace))
    (dolist (path (list incoming quarantine))
      (sb-posix:mkdir (namestring path) #o700)
      (sb-posix:chmod (namestring path) #o700)
      (physical-adoption-install-prepare-auxiliary path))
    (fsync-directory workspace)
    (let* ((stage-tree (physical-adoption-install-snapshot stage))
           (new (physical-adoption-install-clone stage incoming stage-tree)))
      (physical-adoption-install-check-image incoming record :auxiliary t)
      (unless (equal record (physical-adoption-attempt-resume-proof owner :client client))
        (error "adoption proved attempt changed during installation preparation"))
      (let ((old (physical-adoption-install-snapshot root :exclude *physical-adoption-install-retained-names*)))
        (physical-adoption-install-sync-tree root old)
        (physical-adoption-install-sync-tree stage stage-tree)
        (dolist (path (list root stage workspace))
          (physical-adoption-install-sync-ancestors path))
        (let ((plan
                (physical-adoption-install-copy
                 (list :version 1 :attempt record
                       :workspace (physical-adoption-install-persistent-fact workspace-fact :directory t)
                       :incoming (physical-adoption-install-persistent-fact
                                  (physical-adoption-install-auxiliary-fact incoming))
                       :quarantine (physical-adoption-install-persistent-fact
                                    (physical-adoption-install-auxiliary-fact quarantine))
                       :journal (physical-adoption-install-allocate-journal workspace)
                       :old old
                       :stage-tree stage-tree :new new))))
          (physical-adoption-install-publish-journal owner plan))))))
