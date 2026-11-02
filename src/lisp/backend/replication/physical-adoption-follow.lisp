;; A is physical-adoption.attempt, J is physical-adoption.install, H is
;; physical-adoption.handoff, and F is physical-adoption.follow.  W3 is the
;; caller's fresh third workspace.

;; Finish an offline adoption handoff by preparing follow metadata.
;; W3 is private workspace for staged images and saved originals.  Publishing F
;; makes its fixed plan authoritative; resume performs its no-replace renames
;; and records each durable prefix.  A failure before F is published leaves a
;; private W3; retry with a fresh workspace path.
;;
;; The terminal state is active-fenced.  A, J, H and F, the quarantine and the
;; source pin remain in place.  This code does not start network, redo or
;; workers, retire startup artifacts, or coordinate the next phase.
(in-package #:pg)

;; Verify the installed base and empty receive spool before preparing images.
(defun physical-adoption-follow-preflight (owner handoff)
  (unless (eq :ready (physical-adoption-handoff-validate owner handoff))
    (error "adoption follow requires a published empty receive spool"))
  (let* ((*pgdata* (physical-adoption-owner-directory owner))
         (record (getf (getf handoff :installation) :attempt)))
    (multiple-value-bind (base control) (physical-recovery-verify-base-state)
      (let ((store (physical-store-open
                    (merge-pathnames "pg_physical_receive/" *pgdata*)
                    (getf base :system-id) (getf base :timeline) :start-lsn (getf base :end)
                    :discard-tail nil :create-lock nil
                    :retained-identity (physical-recovery-retained-identity base))))
        (unwind-protect
             (physical-adoption-follow-make-binding record base control store)
          (physical-store-close store))))))

(defun physical-adoption-follow-create-workspace (owner handoff directory)
  (let* ((fact (physical-adoption-install-directory-fact directory))
         (root (physical-adoption-owner-directory owner))
         (installation (getf handoff :installation))
         (mount (physical-adoption-install-mount-id root)))
    (unless (and (= (getf fact :device) (physical-adoption-owner-device owner))
                 (= (getf fact :mount) mount))
      (error "adoption follow workspace crosses a filesystem or mount"))
    (dolist (other (append (loop for role in '(:root :stage :workspace)
                                 collect (physical-adoption-install-directory installation role))
                           (list (physical-adoption-handoff-workspace handoff))))
      (when (or (uiop:subpathp directory other) (uiop:subpathp other directory))
        (error "adoption follow workspace overlaps protected data")))
    (physical-adoption-install-prepare-auxiliary directory)
    (dolist (name '("staged/" "saved/"))
      (sb-posix:mkdir (namestring (merge-pathnames name directory)) #o700))
    (values (physical-adoption-install-persistent-fact
             (physical-adoption-install-auxiliary-fact directory) :directory t)
            (physical-adoption-attempt-fields-copy
             (physical-adoption-install-directory-fact (merge-pathnames "staged/" directory))
             '(:device :inode))
            (physical-adoption-attempt-fields-copy
             (physical-adoption-install-directory-fact (merge-pathnames "saved/" directory))
             '(:device :inode)))))

(defun physical-adoption-follow-stage-images (handoff binding directory time)
  (let* ((record (getf (getf handoff :installation) :attempt))
         (images (physical-adoption-follow-images record binding time))
         (staged (merge-pathnames "staged/" directory)))
    (dolist (image images)
      (let* ((path (merge-pathnames (car image) staged))
             (identity (physical-adoption-allocate-private staged (car image))))
        (physical-adoption-write-empty-private
         path identity (cdr image) :limit *physical-adoption-follow-stage-write-limit*
         :point :after-follow-stage-write-chunk :after-sync :after-follow-stage-file-fsync)))
    (values (physical-adoption-install-snapshot staged) nil)))

(defun physical-adoption-follow-check-prepared (owner value)
  (let* ((handoff (getf value :handoff))
         (workspace (physical-adoption-follow-directory value :workspace))
         (path (merge-pathnames "journal.pending" workspace))
         (state (physical-adoption-follow-read-file owner path)))
    (unless (and (eq :ready (physical-adoption-handoff-validate owner handoff))
                 (equal value (getf state :plan)) (zerop (getf state :step)) (not (getf state :partial))
                 (physical-adoption-install-names-p
                  workspace '(".pg_lisp.lock" "journal.pending" "physical-adoption.install" "saved" "staged"))
                 (equal (getf value :entries)
                        (physical-adoption-install-snapshot (physical-adoption-follow-directory value :staged)))
                 (null (physical-adoption-install-snapshot (physical-adoption-follow-directory value :saved))))
      (error "adoption follow prepared metadata or preserved handoff changed"))
    (physical-adoption-follow-check-context owner value)
    (physical-adoption-handoff-validate owner handoff)
    value))

;; Publish F only after the private workspace and its images are durable.
(defun physical-adoption-follow-publish-journal (owner value)
  (let* ((workspace (physical-adoption-follow-directory value :workspace))
         (pending (merge-pathnames "journal.pending" workspace))
         (target (physical-adoption-follow-path (physical-adoption-owner-directory owner))))
    (physical-adoption-install-test-point :before-follow-journal-rename target)
    (physical-adoption-follow-check-prepared owner value)
    (physical-adoption-install-rename pending target)
    (physical-adoption-install-test-point :after-follow-journal-rename target)
    (physical-adoption-follow-sync-authority owner value)
    (let ((state (physical-adoption-follow-read owner)))
      (unless (and (equal value (getf state :plan)) (zerop (getf state :step))
                   (zerop (physical-adoption-follow-validate owner state)))
        (error "adoption follow initial journal readback differs"))
      state)))

(defun physical-adoption-follow-prepare (owner directory)
  (let* ((directory (uiop:ensure-directory-pathname directory))
         (root (physical-adoption-owner-directory owner))
         (handoff (physical-adoption-handoff-read owner))
         (binding (physical-adoption-follow-preflight owner handoff))
         (time (- (get-universal-time) 2208988800)))
    (physical-adoption-require-absent (physical-adoption-follow-path root)
                                      "adoption follow journal already exists")
    (multiple-value-bind (workspace staged saved)
        (physical-adoption-follow-create-workspace owner handoff directory)
      (multiple-value-bind (entries reuse)
          (physical-adoption-follow-stage-images handoff binding directory time)
        (let* ((journal (physical-adoption-allocate-private directory "journal.pending"))
               (value (physical-adoption-follow-copy
                       (list :version 1 :handoff handoff
                             :handoff-file (physical-adoption-handoff-file-fact
                                            (physical-adoption-handoff-path root)
                                            +physical-adoption-handoff-maximum-bytes+)
                             :workspace workspace :staged staged :saved saved :journal journal
                             :time time :entries entries :reuse-wal reuse))))
          (physical-adoption-write-empty-private
           (merge-pathnames "journal.pending" directory) journal (physical-adoption-follow-image value 0)
           :limit *physical-adoption-follow-write-limit* :point :after-follow-journal-write-chunk
           :after-sync :after-follow-journal-file-fsync)
          (physical-adoption-follow-check-prepared owner value)
          (physical-adoption-install-sync-tree
           directory (physical-adoption-install-snapshot
                      directory :exclude *physical-adoption-install-retained-names*))
          (physical-adoption-install-sync-ancestors directory)
          (physical-adoption-follow-check-prepared owner value)
          (physical-adoption-follow-publish-journal owner value))))))

;; Reconcile one move ahead, then advance the fixed plan to active-fenced.
(defun physical-adoption-follow-resume (owner)
  (let* ((state (physical-adoption-follow-read owner))
         (value (getf state :plan))
         (moves (physical-adoption-follow-moves value)))
    (physical-adoption-follow-validate owner state)
    (physical-adoption-follow-sync-journal owner state)
    (loop
      for step = (getf state :step)
      for observed = (physical-adoption-follow-validate owner state)
      do (cond
           ((= step (length moves))
            (return (values state :active-fenced)))
           ((> observed step)
            (setf state (physical-adoption-follow-append-step owner state)))
           (t
            (destructuring-bind (from name to target) (nth step moves)
              (let ((source (physical-adoption-install-entry-path
                             (physical-adoption-follow-directory value from) name))
                    (destination (physical-adoption-install-entry-path
                                  (physical-adoption-follow-directory value to) target)))
                (physical-adoption-install-test-point :before-follow-move source)
                (unless (= step (physical-adoption-follow-validate owner state))
                  (error "adoption follow advanced before its planned move"))
                (physical-adoption-install-rename source destination)
                (physical-adoption-install-test-point :after-follow-move destination)
                (setf state (physical-adoption-follow-append-step owner state)))))))))

(defun physical-adoption-follow-start (owner directory)
  (physical-adoption-follow-prepare owner directory)
  (physical-adoption-follow-resume owner))
