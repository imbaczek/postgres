;; Verify the follow journal against the offline directory state.
;; A state must match the recorded move prefix or be exactly one move ahead.
;; The latter is the rename completed before its progress record became durable.
;; Identity, contents and directory context are checked before resuming.
(in-package #:pg)

(defun physical-adoption-follow-directory (value role)
  (let* ((record (physical-adoption-follow-record value))
         (root (pathname (getf (getf record :local) :directory)))
         (workspace (pathname (getf (getf value :workspace) :directory))))
    (ecase role
      (:root root)
      (:wal (merge-pathnames "pg_wal/" root))
      (:spool (merge-pathnames "pg_physical_receive/" root))
      (:workspace workspace)
      (:staged (merge-pathnames "staged/" workspace))
      (:saved (merge-pathnames "saved/" workspace)))))

(defun physical-adoption-follow-read-file (owner path)
  (physical-adoption-check-owner owner)
  (let ((before (sb-posix:lstat (namestring path))))
    (unless (and (sb-posix:s-isreg (sb-posix:stat-mode before))
                 (= #o600 (logand #o7777 (sb-posix:stat-mode before)))
                 (= 1 (sb-posix:stat-nlink before))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid before)))
      (error "adoption follow journal is not a private owned regular file"))
    (let* ((state (physical-adoption-follow-decode
                   (read-bounded-regular-file path +physical-adoption-follow-maximum-bytes+)))
           (plan (getf state :plan)) (identity (getf plan :journal)))
      (physical-adoption-attempt-check-owner owner (physical-adoption-follow-record plan))
      (unless (and (= (getf identity :device) (sb-posix:stat-dev before))
                   (= (getf identity :inode) (sb-posix:stat-ino before))
                   (= (getf state :file-size) (sb-posix:stat-size before))
                   (equal (physical-adoption-stage-stat-identity before)
                          (physical-adoption-stage-stat-identity (sb-posix:lstat (namestring path)))))
        (error "adoption follow journal identity changed"))
      state)))

(defun physical-adoption-follow-read (owner)
  (physical-adoption-follow-read-file
   owner (physical-adoption-follow-path (physical-adoption-owner-directory owner))))

(defun physical-adoption-follow-check-workspaces (owner value mount)
  (let ((handoff (getf value :handoff))
        (root (physical-adoption-owner-directory owner)))
    (unless (and (= mount (physical-adoption-handoff-check-workspace owner handoff))
                 (physical-adoption-install-names-p
                  (physical-adoption-handoff-workspace handoff)
                  '(".pg_lisp.lock" "physical-adoption.install")))
      (error "adoption follow handoff workspace changed"))
    (let ((workspace (physical-adoption-install-auxiliary-fact
                      (physical-adoption-follow-directory value :workspace))))
      (unless (and (equal (getf value :workspace)
                          (physical-adoption-install-persistent-fact workspace :directory t))
                   (= mount (getf workspace :mount)))
        (error "adoption follow workspace or guard changed")))
    (dolist (role '(:staged :saved :spool :wal))
      (let* ((directory (physical-adoption-follow-directory value role))
             (fact (if (eq role :wal)
                       (let* ((stat (sb-posix:lstat (namestring directory)))
                              (row (first (second (assoc "pg_wal"
                                                         (getf (getf handoff :installation) :new)
                                                         :test #'equal)))))
                         (unless (and (equal directory (truename directory))
                                      (physical-adoption-install-top-p root (list "pg_wal" (list row)) mount))
                           (error "adoption follow WAL directory changed"))
                         (list :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat)
                               :mount (physical-adoption-install-mount-id directory)))
                       (physical-adoption-install-directory-fact directory)))
             (expected
               (case role
                 ((:staged :saved) (getf value role))
                 (:spool (getf handoff :spool))
                 (:wal
                  (let ((row (first (second (assoc "pg_wal"
                                                   (getf (getf handoff :installation) :new)
                                                   :test #'equal)))))
                    (list :device (fourth row) :inode (fifth row)))))))
        (unless (and (= (getf expected :device) (getf fact :device))
                     (= (getf expected :inode) (getf fact :inode))
                     (= mount (getf fact :mount)))
          (error "adoption follow directory identity or mount changed"))))
    mount))

(defun physical-adoption-follow-check-context (owner value)
  (let* ((handoff (getf value :handoff))
         (root (physical-adoption-owner-directory owner))
         (mount (physical-adoption-check-preserved-installation owner handoff))
         (actual (physical-adoption-handoff-read owner)))
    (unless (and (equal handoff actual)
                 (equal (getf value :handoff-file)
                        (physical-adoption-handoff-file-fact
                         (physical-adoption-handoff-path root) +physical-adoption-handoff-maximum-bytes+))
                 (= mount (physical-adoption-install-mount-id (physical-adoption-handoff-path root))))
      (error "adoption follow lost its preserved handoff authority"))
    (physical-adoption-follow-check-workspaces owner value mount)))

(defun physical-adoption-follow-position-entries (positions role)
  (sort (loop for (where name row) in positions when (eq where role)
              collect (list name (list (cons name (copy-list (rest row))))))
        #'string< :key #'first))

(defun physical-adoption-follow-wal-rows (value positions)
  (let ((old (second (assoc "pg_wal" (getf (getf (getf value :handoff) :installation) :new) :test #'equal))))
    (sort (append (copy-list old)
                  (loop for (role name row) in positions when (eq role :wal)
                        collect (cons (concatenate 'string "pg_wal/" name) (copy-list (rest row)))))
          #'string< :key #'first)))

(defun physical-adoption-follow-root-names (value positions &optional owner)
  (append (remove "physical-recovery.control"
                  (mapcar #'first (getf (getf (getf value :handoff) :installation) :new)) :test #'equal)
          (copy-list *physical-adoption-install-retained-names*)
          (list "physical-adoption.handoff" "physical-adoption.follow" "pg_physical_receive")
          (physical-adoption-owner-extra-root-names owner)
          (loop for (role name) in positions when (eq role :root) collect name)))

(defun physical-adoption-follow-position-namespace-p (value positions mount)
  (let* ((wal-rows (physical-adoption-follow-wal-rows value positions))
         (wal-names
           (remove-duplicates
            (loop for row in wal-rows unless (equal "pg_wal" (first row))
                  collect (let* ((tail (subseq (first row) 7)) (slash (position #\/ tail)))
                            (subseq tail 0 slash)))
            :test #'equal)))
    (and (physical-adoption-install-names-p (physical-adoption-follow-directory value :workspace)
                                            '(".pg_lisp.lock" "physical-adoption.install" "saved" "staged"))
         (loop for role in '(:staged :saved)
               always (physical-adoption-install-names-p
                       (physical-adoption-follow-directory value role)
                       (mapcar #'first (physical-adoption-follow-position-entries positions role))))
         (physical-adoption-install-names-p
          (physical-adoption-follow-directory value :spool)
          (cons "receive.lock" (mapcar #'first (physical-adoption-follow-position-entries positions :spool))))
         (physical-adoption-install-names-p (physical-adoption-follow-directory value :wal) wal-names)
         (loop for (role name row) in positions
               always (physical-adoption-install-top-p
                       (physical-adoption-follow-directory value role) (list name (list row)) mount)))))

(defun physical-adoption-follow-namespace-p (value step mount &optional owner)
  (let ((positions (physical-adoption-follow-positions value step)))
    (and (physical-adoption-install-names-p
          (physical-adoption-follow-directory value :root)
          (physical-adoption-follow-root-names value positions owner))
         (physical-adoption-follow-position-namespace-p value positions mount))))

(defun physical-adoption-follow-check-content (owner value step mount)
  (let* ((positions (physical-adoption-follow-positions value step))
         (installation (getf (getf value :handoff) :installation))
         (root (physical-adoption-owner-directory owner))
         (device (physical-adoption-owner-device owner))
         (budget (list 100000 16777216)))
    (dolist (entry (getf installation :new))
      (unless (member (first entry) '("physical-recovery.control" "pg_wal") :test #'equal)
        (unless (equal (second entry)
                       (physical-adoption-install-tree root (first entry) device mount budget))
          (error "adoption follow changed an immutable installed image object"))))
    (unless (equal (physical-adoption-follow-wal-rows value positions)
                   (physical-adoption-install-tree root "pg_wal" device mount budget))
      (error "adoption follow changed original WAL or has unexplained history entries"))
    (dolist (role '(:staged :saved :spool))
      (let ((expected (physical-adoption-follow-position-entries positions role)))
        (when (eq role :spool)
          (push (copy-tree (assoc "receive.lock" (getf (getf value :handoff) :spool-entries) :test #'equal)) expected)
          (setf expected (sort expected #'string< :key #'first)))
        (unless (equal expected (physical-adoption-install-snapshot
                                 (physical-adoption-follow-directory value role)))
          (error "adoption follow metadata image or saved original changed"))))
    (dolist (entry (physical-adoption-follow-position-entries positions :root))
      (unless (equal (second entry) (physical-adoption-install-tree root (first entry) device mount budget))
        (error "adoption follow published metadata differs from its planned image")))))

(defun physical-adoption-follow-check-active (owner value)
  (let* ((*pgdata* (physical-adoption-owner-directory owner))
         (record (physical-adoption-follow-record value))
         (binding (physical-adoption-follow-planned-binding record))
         (expected (physical-adoption-follow-config record binding))
         (config (physical-follow-read-config)))
    (setf (getf (fifth expected) :phase) :active)
    (unless (equal expected config) (error "adoption follow active config differs from its proved plan"))
    (multiple-value-bind (base control) (physical-recovery-verify-base-state)
      (declare (ignore control))
      (let ((store (physical-store-open
                    (physical-adoption-follow-directory value :spool) (getf base :system-id)
                    (getf base :timeline) :start-lsn (getf base :end) :discard-tail nil :create-lock nil
                    :retained-identity (physical-recovery-retained-identity base))))
        (unwind-protect
             (physical-follow-check-active-state store base config :policy nil)
          (physical-store-close store))))))

;; Accept the recorded prefix or one completed but unrecorded move.
;; Recheck the context and journal after inspecting names and contents.
(defun physical-adoption-follow-validate (owner state)
  (unless (equal state (physical-adoption-follow-read owner))
    (error "adoption follow journal changed"))
  (let* ((value (getf state :plan))
         (mount (physical-adoption-follow-check-context owner value))
         (step (getf state :step))
         (last (length (physical-adoption-follow-moves value)))
         (observed
           (cond ((physical-adoption-follow-namespace-p value step mount owner) step)
                 ((and (< step last) (physical-adoption-follow-namespace-p value (1+ step) mount owner)) (1+ step))
                 (t (error "adoption follow namespace is not its recorded prefix or one move ahead")))))
    (when (and (getf state :partial) (/= observed (1+ step)))
      (error "partial adoption follow progress has no completed move"))
    (physical-adoption-follow-check-content owner value observed mount)
    (when (= observed last) (physical-adoption-follow-check-active owner value))
    (unless (and (= mount (physical-adoption-follow-check-context owner value))
                 (= mount (physical-adoption-install-mount-id
                           (physical-adoption-follow-path (physical-adoption-owner-directory owner))))
                 (physical-adoption-follow-namespace-p value observed mount owner)
                 (equal state (physical-adoption-follow-read owner)))
      (error "adoption follow context changed during verification"))
    observed))
