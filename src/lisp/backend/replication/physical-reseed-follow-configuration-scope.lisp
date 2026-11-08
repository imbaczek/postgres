(in-package #:pg)

;; The context belongs to one active thread and store operation.  Its opaque
;; printer keeps credentials private.  It holds borrowed descriptors and proof
;; state only for the scope; reading through it grants no mutation authority.
(defstruct (physical-reseed-follow-installation-context
            (:constructor %make-physical-reseed-follow-installation-context)
            (:print-function physical-libpq-print-private-object))
  owner thread active-p descriptors plan record install-state attempt snapshot prefix
  base binding model images store check)

(defun physical-reseed-follow-installation-context (owner)
  (let ((context *physical-reseed-follow-installation-scope*))
    (unless (and (physical-reseed-follow-installation-context-p context)
                 (physical-reseed-follow-installation-context-active-p context)
                 (eq owner (physical-reseed-follow-installation-context-owner context))
                 (eq sb-thread:*current-thread* (physical-reseed-follow-installation-context-thread context))
                 (physical-reseed-follow-owner-busy-p owner))
      (error "installation operation requires its active thread-bound store scope"))
    context))

(defun call-with-physical-reseed-follow-installation-workspace (owner root-fd record function check)
  (let* ((configuration (getf record :configuration))
         (expected (getf configuration :directories))
         (workspace (getf configuration :workspace)))
    (call-with-physical-reseed-follow-workspace
     owner root-fd (pathname (getf workspace :directory))
     (lambda (facts fd workspace-check)
       (unless (equal workspace facts) (error "installation workspace differs from its saved identity"))
       (call-with-physical-reseed-follow-configuration-guard
        owner fd ".pg_lisp.lock"
        (lambda (lock-inode lock-check)
          (unless (= lock-inode (getf expected :lock-inode)) (error "installation workspace lock changed"))
          (call-with-physical-reseed-follow-configuration-guard
           owner fd "physical-reseed.install"
           (lambda (fence-inode fence-check)
             (unless (= fence-inode (getf expected :fence-inode)) (error "installation workspace fence changed"))
             (call-with-physical-reseed-follow-fixed-configuration-directories
              fd record
              (lambda (staged saved fixed-check) (funcall function fd staged saved fixed-check))
              fence-check))
           lock-check))
        workspace-check))
     check)))

(defun call-with-physical-reseed-follow-installation-receive
    (descriptors record binding prefix function check)
  (let* ((positions (physical-reseed-follow-configuration-positions record binding prefix))
         (position (assoc :receive-directory positions))
         (parent (physical-reseed-follow-configuration-role-fd descriptors (second position)))
         (leaf (third position))
         (expected (getf (getf (getf record :configuration) :directories) :receive))
         (mount (physical-adoption-install-mount-id nil :directory-fd parent))
         (fd nil))
    (unwind-protect
         (progn
           (funcall check)
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       parent leaf (logior sb-posix:o-rdonly sb-posix:o-directory
                                           sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
           (when (minusp fd) (setf fd nil) (error "cannot open the prescribed receive directory"))
           (let ((stat (physical-image-check-directory fd)))
             (unless (and (= (getf expected :device) (sb-posix:stat-dev stat))
                          (= (getf expected :inode) (sb-posix:stat-ino stat))
                          (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
               (error "installation receive directory differs from its saved identity")))
           (funcall check)
           (funcall function (append descriptors (list (cons :receive fd)))))
      (when fd (sb-posix:close fd)))))

(defun physical-reseed-follow-installation-guard (context)
  (unless (and (physical-reseed-follow-installation-context-active-p context)
               (eq sb-thread:*current-thread* (physical-reseed-follow-installation-context-thread context)))
    (error "installation image scope has ended or changed threads"))
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-installation-context-descriptors context) :root)))
    (funcall (physical-reseed-follow-installation-context-check context))
    (physical-reseed-check-owner owner)
    (physical-reseed-follow-check-snapshot root (physical-reseed-follow-installation-context-snapshot context))
    (unless (and (equal (physical-reseed-follow-installation-context-plan context)
                        (physical-reseed-install-owner-plan owner))
                 (equal (physical-reseed-follow-installation-context-install-state context)
                        (physical-reseed-install-read-at root "physical-reseed.install")))
      (error "installation predecessor changed during the mapped operation"))
    (physical-reseed-follow-check-attempt-snapshot-at
     root (physical-reseed-follow-installation-context-attempt context)))
  t)

;; Check the receive store against the derived tuple at every prefix.  At the
;; complete prefix also prove the actual pending state against the base and store.
(defun physical-reseed-follow-installation-check-receive (context check)
  (let* ((store (physical-reseed-follow-installation-context-store context))
         (binding (physical-reseed-follow-installation-context-binding context))
         (record (physical-reseed-follow-installation-context-record context))
         (base (physical-reseed-follow-installation-context-base context))
         (descriptors (physical-reseed-follow-installation-context-descriptors context))
         (maximum (length (physical-reseed-follow-configuration-moves record binding))))
    (physical-store-check-held-lock store)
    (physical-store-check-complete-input store)
    (unless (equal (physical-store-control-tuple store) (getf binding :after))
      (error "installation receive store differs from the exact derived tuple"))
    (physical-store-read-retained-control store (getf binding :after))
    (physical-store-validate-prefix store)
    (when (= maximum (physical-reseed-follow-installation-context-prefix context))
      (call-with-physical-reseed-follow-held-base-directory
       (physical-reseed-follow-configuration-role-fd descriptors :root)
       (physical-reseed-follow-configuration-role-fd descriptors :base)
       (physical-reseed-follow-configuration-role-fd descriptors :global)
       (physical-reseed-follow-configuration-role-fd descriptors :wal)
       (lambda (directory)
         (let ((*pgdata* directory)
               (config (physical-reseed-follow-pending-config record binding)))
           (multiple-value-bind (candidate position control)
               (physical-follow-check-pending-state store base config)
             (declare (ignore candidate control))
             (unless (eq :after position) (error "installed pending state has the old receive tuple")))))
       check))
    (funcall check)))

(defun physical-reseed-follow-installation-verify (context)
  (physical-reseed-follow-installation-guard context)
  (let* ((descriptors (physical-reseed-follow-installation-context-descriptors context))
         (owner (physical-reseed-follow-installation-context-owner context))
         (record (physical-reseed-follow-installation-context-record context))
         (plan (physical-reseed-follow-installation-context-plan context))
         (prefix (physical-reseed-follow-installation-context-prefix context))
         (binding (physical-reseed-follow-installation-context-binding context))
         (images (physical-reseed-follow-installation-context-images context))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (base-fds (loop for role in '(:base :global :wal)
                         collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role)))))
    (flet ((guard () (physical-reseed-follow-installation-guard context)))
      (let ((layout (physical-reseed-follow-installation-layout-state
                     owner descriptors record binding images prefix #'guard)))
        (multiple-value-bind (base control actual-binding model actual-images row)
            (physical-reseed-follow-configuration-base-proof root saved base-fds plan record prefix #'guard)
          (declare (ignore control row))
          (unless (and (equalp base (physical-reseed-follow-installation-context-base context))
                       (equal binding actual-binding) (equalp images actual-images)
                       (equal (physical-store-control-tuple model)
                              (physical-store-control-tuple (physical-reseed-follow-installation-context-model context))))
            (error "mapped installation base facts changed")))
        (physical-reseed-follow-installation-check-receive context #'guard)
        (unless (equal layout (physical-reseed-follow-installation-layout-state
                               owner descriptors record binding images prefix #'guard))
          (error "mapped installation layout changed around the complete base proof"))
        (guard))))
  t)

(defun call-with-physical-reseed-follow-installation-context
    (owner descriptors plan record install-state attempt snapshot prefix check function)
  (multiple-value-bind (base control binding model images row)
      (physical-reseed-follow-configuration-base-proof
       (physical-reseed-follow-configuration-role-fd descriptors :root)
       (physical-reseed-follow-configuration-role-fd descriptors :saved)
       (loop for role in '(:base :global :wal)
             collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role)))
       plan record prefix check)
    (declare (ignore control row))
    (call-with-physical-reseed-follow-installation-receive
     descriptors record binding prefix
     (lambda (held)
       (let ((store nil) (context nil))
         (unwind-protect
              (progn
                (physical-store-open
                 (physical-image-fd-path (physical-reseed-follow-configuration-role-fd held :receive))
                 (getf base :system-id) (getf base :timeline) :start-lsn (getf base :end)
                 :discard-tail nil :create-lock nil
                 :retained-identity (physical-recovery-retained-identity base)
                 :on-open (lambda (opened) (setf store opened)))
                (setf context (%make-physical-reseed-follow-installation-context
                               :owner owner :thread sb-thread:*current-thread* :active-p t
                               :descriptors held :plan plan :record record :install-state install-state
                               :attempt attempt :snapshot snapshot :prefix prefix :base base :binding binding
                               :model model :images images :store store :check check))
                (physical-reseed-follow-installation-verify context)
                (let ((*physical-reseed-follow-installation-scope* context))
                  (flet ((verify () (physical-reseed-follow-installation-verify context)))
                    (multiple-value-prog1
                        (funcall function (physical-reseed-follow-configuration-role-fd held :root) #'verify)
                      (verify)))))
           (when context (setf (physical-reseed-follow-installation-context-active-p context) nil))
           (when store (physical-store-close store)))))
     check)))

(defun call-with-physical-reseed-follow-mapped-predecessor (owner function)
  ;; Hold nine directory roles and check the journal, attempt and terminal
  ;; install predecessor.  The context adds the receive role for full proof.
  (call-with-physical-reseed-install-directories
   owner
   (lambda (root incoming quarantine value-plan root-check)
     (let* ((snapshot (physical-reseed-follow-snapshot-at root))
            (record (physical-reseed-follow-copy (getf snapshot :record)))
            (attempt (physical-reseed-follow-attempt-snapshot-at root))
            (install-state (physical-reseed-install-read-at root "physical-reseed.install")))
       (multiple-value-bind (plan checked)
           (physical-reseed-follow-configuration-install-plan value-plan record)
         (setf record checked)
         (physical-reseed-follow-check-binding owner record)
         (unless (and (equal plan (getf install-state :plan))
                      (equal (getf record :install) (physical-reseed-follow-terminal-reference install-state)))
           (error "mapped image does not match the actual terminal predecessor"))
         (flet ((journal-check ()
                  (funcall root-check)
                  (physical-reseed-check-owner owner)
                  (physical-reseed-follow-check-attempt-snapshot-at root attempt)
                  (unless (equal install-state (physical-reseed-install-read-at root "physical-reseed.install"))
                    (error "mapped image predecessor changed"))))
           (journal-check)
           (physical-reseed-follow-check-snapshot root snapshot)
           (call-with-physical-reseed-follow-installation-workspace
            owner root record
            (lambda (workspace staged saved workspace-check)
              (call-with-physical-reseed-follow-base-directories
               root plan
               (lambda (base-fds base-check)
                 (physical-reseed-follow-check-snapshot root snapshot)
                 (let ((descriptors (append (list (cons :root root) (cons :incoming incoming)
                                                  (cons :quarantine quarantine) (cons :workspace workspace)
                                                  (cons :staged staged) (cons :saved saved)) base-fds)))
                   (funcall function descriptors plan record install-state attempt snapshot base-check)))
               workspace-check))
            #'journal-check)))))))

;; Hold root, incoming, quarantine, workspace, staged, saved, base, global, WAL
;; and receive descriptors for one mapped proof.  Keep journal and attempt checks
;; separate from the full layout proof so they cannot recurse into this scope.
(defun %call-with-physical-reseed-follow-mapped-image (owner function)
  ;; Derive the installation prefix before constructing the context and its
  ;; receive descriptor.  The callback runs under the complete mapped proof.
  (when *physical-reseed-follow-installation-scope* (error "mapped installation scope is already active"))
  (let ((*physical-reseed-follow-installation-scope* :acquiring))
    (call-with-physical-reseed-follow-mapped-predecessor
     owner
     (lambda (descriptors plan record install-state attempt snapshot check)
       (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
              (extra (physical-reseed-install-extra-entries owner root))
              (prefix (physical-reseed-follow-configuration-initial-prefix
                       root (physical-reseed-follow-configuration-role-fd descriptors :workspace)
                       (physical-reseed-follow-configuration-role-fd descriptors :staged)
                       (physical-reseed-follow-configuration-role-fd descriptors :saved) plan record extra)))
         (call-with-physical-reseed-follow-installation-context
          owner descriptors plan record install-state attempt snapshot prefix check function))))))

(defun call-with-physical-reseed-follow-mapped-image (owner function)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "mapped image requires dedicated configuration ownership"))
  (%call-with-physical-reseed-follow-mapped-image owner function))

(defmethod physical-reseed-follow-image-dispatch
    ((owner physical-reseed-follow-configuration-owner) function)
  (let ((phase (call-with-physical-directory-root
                owner
                (lambda (fd directory)
                  (declare (ignore directory))
                  (when (physical-reseed-follow-entry fd "physical-reseed.follow")
                    (getf (getf (physical-reseed-follow-read-at fd "physical-reseed.follow") :configuration) :phase))))))
    (if (or *physical-reseed-follow-installation-request* (member phase '(:installing :inert)))
        (call-with-physical-reseed-follow-mapped-image owner function)
        (call-next-method))))
