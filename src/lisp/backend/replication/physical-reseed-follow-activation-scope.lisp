(in-package #:pg)

;;; Check a settled target through held directories and journal snapshots.
;;; Keep activation layout and receive proofs in one thread-bound scope.

(defstruct (physical-reseed-follow-activation-context
            (:constructor %make-physical-reseed-follow-activation-context)
            (:print-function physical-libpq-print-private-object))
  ;; Thread-bound scope with borrowed descriptors and store.  Its private
  ;; printer protects credentials, and the context is revoked on exit.
  owner thread active-p descriptors plan record install-state attempt snapshot prefix
  base binding model images store check output-entry durable-output)

(defun call-with-physical-reseed-follow-activation-operation (owner function)
  ;; The store operation callback returns a record and snapshot.  A failure
  ;; leaves the owner uncertain and requires it to be closed and reopened.
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "activation operation requires dedicated configuration ownership"))
  (physical-reseed-follow-check-idle owner)
  (when (or *physical-reseed-follow-activation-request* *physical-reseed-follow-installation-request*)
    (error "activation request is already active"))
  (let ((*physical-reseed-follow-activation-request* owner) (completed nil))
    (unwind-protect
         (multiple-value-prog1 (call-with-physical-reseed-follow-store owner function)
           (setf completed t))
      (unless completed (setf (physical-reseed-follow-owner-uncertain-p owner) t)))))

(defun physical-reseed-follow-activation-context (owner)
  ;; Only the owning thread may use the context during its busy store scope.
  (let ((context *physical-reseed-follow-activation-scope*))
    (unless (and (physical-reseed-follow-activation-context-p context)
                 (physical-reseed-follow-activation-context-active-p context)
                 (eq owner (physical-reseed-follow-activation-context-owner context))
                 (eq sb-thread:*current-thread* (physical-reseed-follow-activation-context-thread context))
                 (physical-reseed-follow-owner-busy-p owner))
      (error "activation operation requires its active thread-bound store scope"))
    context))

(defun physical-reseed-follow-activation-guard (context)
  ;; Recheck the held scope and journal predecessors without recursively
  ;; running the complete layout and receive proof.
  (unless (and (physical-reseed-follow-activation-context-active-p context)
               (eq sb-thread:*current-thread* (physical-reseed-follow-activation-context-thread context)))
    (error "activation image scope has ended or changed threads"))
  (let* ((owner (physical-reseed-follow-activation-context-owner context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-activation-context-descriptors context) :root)))
    (funcall (physical-reseed-follow-activation-context-check context))
    (physical-reseed-check-owner owner)
    (physical-reseed-follow-check-snapshot root (physical-reseed-follow-activation-context-snapshot context))
    (unless (and (equal (physical-reseed-follow-activation-context-record context)
                        (getf (physical-reseed-follow-activation-context-snapshot context) :record))
                 (equal (physical-reseed-follow-activation-context-plan context)
                        (physical-reseed-install-owner-plan owner))
                 (equal (physical-reseed-follow-activation-context-install-state context)
                        (physical-reseed-install-read-at root "physical-reseed.install")))
      (error "installation predecessor changed during the mapped operation"))
    (physical-reseed-follow-check-attempt-snapshot-at
     root (physical-reseed-follow-activation-context-attempt context)))
  t)

(defun physical-reseed-follow-activation-initial-prefix (owner descriptors record binding check)
  ;; Identify one of the three prefixes from stable directory names, then
  ;; check it against recorded activation progress.  Full proof follows.
  (funcall check)
  (let* ((plan (physical-reseed-install-owner-plan owner))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (extra (physical-reseed-install-extra-entries owner root))
         (roles '(:root :workspace :staged :saved)))
    (flet ((names ()
             (loop for role in roles
                   append (list role (physical-reseed-install-directory-names
                                      (physical-reseed-follow-configuration-role-fd descriptors role))))))
      (let* ((actual (names))
             (matches
               (loop for prefix from 0 to 2
                     for expected = (physical-reseed-follow-activation-inventories
                                     plan record binding prefix extra)
                     when (equal actual (loop for role in roles append (list role (getf expected role))))
                       collect prefix)))
        (unless (and (= 1 (length matches)) (equal actual (names)))
          (error "activation names disagree with their unique initial prefix"))
        (physical-reseed-follow-activation-check-prefix record (first matches))
        (funcall check)
        (first matches)))))

(defun physical-reseed-follow-activation-observe (owner descriptors record binding images check)
  ;; Absent and Building intent inspect output at prefix zero.  Later phases
  ;; identify a prefix and prove the corresponding layout.
  (let ((phase (getf (getf record :activation) :phase)))
    (if (member phase '(nil :building))
        (progn
          (physical-reseed-follow-activation-check-prefix record 0)
          (values 0 (physical-reseed-follow-activation-output-state
                     owner descriptors record binding images check)))
        (let ((prefix (physical-reseed-follow-activation-initial-prefix
                       owner descriptors record binding check)))
          (values prefix (physical-reseed-follow-activation-layout-state
                          owner descriptors record binding images prefix check))))))

(defun physical-reseed-follow-activation-layout (context check)
  ;; Recheck the prefix and keep any Building output entry fixed in scope.
  (let* ((record (physical-reseed-follow-activation-context-record context))
         (prefix (physical-reseed-follow-activation-context-prefix context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (owner (physical-reseed-follow-activation-context-owner context))
         (binding (physical-reseed-follow-activation-context-binding context))
         (images (physical-reseed-follow-activation-context-images context)))
    (physical-reseed-follow-activation-check-prefix record prefix)
    (if (member (getf (getf record :activation) :phase) '(nil :building))
        (let ((output (physical-reseed-follow-activation-output-state
                       owner descriptors record binding images check)))
          (unless (equal (getf output :entry)
                         (physical-reseed-follow-activation-context-output-entry context))
            (error "activation output changed during its bound scope"))
          output)
        (physical-reseed-follow-activation-layout-state
         owner descriptors record binding images prefix check))))

(defun physical-reseed-follow-activation-read-saved (context)
  ;; Read and decode fresh saved pending bytes on each call.  Check the
  ;; captured file identity and guard the read on both sides.
  (physical-reseed-follow-activation-guard context)
  (let* ((record (physical-reseed-follow-activation-context-record context))
         (config (physical-reseed-follow-pending-config
                  record (physical-reseed-follow-activation-context-binding context)))
         (saved (physical-reseed-follow-configuration-role-fd
                 (physical-reseed-follow-activation-context-descriptors context) :saved))
         (expected (first (second (assoc "follow.pending"
                                         (getf (getf record :configuration) :entries) :test #'equal)))))
    (multiple-value-bind (row bytes)
        (physical-reseed-follow-configuration-file-row
         saved "follow.pending" "follow.pending" (physical-follow-config-image config))
      (unless (equal expected row) (error "saved pending configuration changed its captured identity"))
      (physical-reseed-follow-activation-guard context)
      (physical-follow-decode-config bytes))))

(defun physical-reseed-follow-activation-check-receive (context check)
  ;; Prefix 0 checks pending control in root, prefix 1 reads saved pending
  ;; control, and prefix 2 checks active control.  All require the settled
  ;; receive tuple and validated retained prefix.
  (let* ((store (physical-reseed-follow-activation-context-store context))
         (binding (physical-reseed-follow-activation-context-binding context))
         (record (physical-reseed-follow-activation-context-record context))
         (base (physical-reseed-follow-activation-context-base context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (prefix (physical-reseed-follow-activation-context-prefix context)))
    (physical-store-check-live store)
    (physical-store-check-complete-input store)
    (unless (equal (physical-store-control-tuple store) (getf binding :after))
      (error "activation receive store differs from its exact settled tuple"))
    (physical-store-read-retained-control store (getf binding :after))
    (physical-store-validate-prefix store)
    (call-with-physical-reseed-follow-held-base-directory
     (physical-reseed-follow-configuration-role-fd descriptors :root)
     (physical-reseed-follow-configuration-role-fd descriptors :base)
     (physical-reseed-follow-configuration-role-fd descriptors :global)
     (physical-reseed-follow-configuration-role-fd descriptors :wal)
     (lambda (directory)
       (let ((*pgdata* directory))
         (if (= prefix 2)
             (physical-follow-check-active-state
              store base (physical-reseed-follow-activation-config record binding))
             (multiple-value-bind (candidate position control)
                 (if (zerop prefix)
                     (physical-follow-check-pending-state
                      store base (physical-reseed-follow-pending-config record binding))
                     (physical-follow-check-pending-state-core
                      store base (physical-reseed-follow-pending-config record binding)
                      (lambda () (physical-reseed-follow-activation-read-saved context))))
               (declare (ignore candidate control))
               (unless (eq :after position)
                 (error "activation pending state has the old receive tuple"))))))
     check)
    (funcall check)))

(defun physical-reseed-follow-activation-verify (context)
  ;; Prove the layout around the base and receive checks.  The smaller guard
  ;; is available to each check without recursive proof.
  (physical-reseed-follow-activation-guard context)
  (let* ((descriptors (physical-reseed-follow-activation-context-descriptors context))
         (record (physical-reseed-follow-activation-context-record context))
         (plan (physical-reseed-follow-activation-context-plan context))
         (binding (physical-reseed-follow-activation-context-binding context))
         (images (physical-reseed-follow-activation-context-images context))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (base-fds (loop for role in '(:base :global :wal)
                         collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role)))))
    (flet ((guard () (physical-reseed-follow-activation-guard context)))
      (let ((layout (physical-reseed-follow-activation-layout context #'guard)))
        (multiple-value-bind (base control actual-binding model actual-images row)
            (physical-reseed-follow-configuration-base-proof root saved base-fds plan record (getf (getf record :configuration) :step) #'guard)
          (declare (ignore control row))
          (unless (and (equalp base (physical-reseed-follow-activation-context-base context))
                       (equal binding actual-binding) (equalp images actual-images)
                       (equal (physical-store-control-tuple model)
                              (physical-store-control-tuple (physical-reseed-follow-activation-context-model context))))
            (error "mapped activation base facts changed")))
        (physical-reseed-follow-activation-check-receive context #'guard)
        (unless (equal layout (physical-reseed-follow-activation-layout context #'guard))
          (error "mapped activation layout changed around the complete base proof"))
        (guard))))
  t)

(defun call-with-physical-reseed-follow-activation-context
    (owner descriptors plan record install-state attempt snapshot check function)
  ;; Add the held receive directory to the nine predecessor roles.  Open the
  ;; store and prove the context around the callback, then revoke and close it.
  (let ((terminal (getf (getf record :configuration) :step)))
    (multiple-value-bind (base control binding model images row)
        (physical-reseed-follow-configuration-base-proof
         (physical-reseed-follow-configuration-role-fd descriptors :root)
         (physical-reseed-follow-configuration-role-fd descriptors :saved)
         (loop for role in '(:base :global :wal)
               collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role)))
         plan record terminal check)
      (declare (ignore control row))
      (physical-reseed-follow-activation-config record binding)
      (call-with-physical-reseed-follow-installation-receive
       descriptors record binding terminal
       (lambda (held)
         (multiple-value-bind (prefix observation)
             (physical-reseed-follow-activation-observe owner held record binding images check)
           (let ((store nil) (context nil))
             (unwind-protect
                  (progn
                    (setf store (physical-store-open
                                 (physical-image-fd-path (physical-reseed-follow-configuration-role-fd held :receive))
                                 (getf base :system-id) (getf base :timeline) :start-lsn (getf base :end)
                                 :discard-tail nil :create-lock nil
                                 :retained-identity (physical-recovery-retained-identity base))
                          context (%make-physical-reseed-follow-activation-context
                                   :owner owner :thread sb-thread:*current-thread* :active-p t
                                   :descriptors held :plan plan :record record :install-state install-state
                                   :attempt attempt :snapshot snapshot :prefix prefix :base base :binding binding
                                   :model model :images images :store store :check check
                                   :output-entry (getf observation :entry)))
                    (physical-reseed-follow-activation-verify context)
                    (let ((*physical-reseed-follow-activation-scope* context))
                      (flet ((verify () (physical-reseed-follow-activation-verify context)))
                        (multiple-value-prog1
                            (funcall function (physical-reseed-follow-configuration-role-fd held :root) #'verify)
                          (verify)))))
               (when context (setf (physical-reseed-follow-activation-context-active-p context) nil))
               (when store (physical-store-close store))))))
       check))))

(defun call-with-physical-reseed-follow-activation-image (owner function)
  ;; The raw image callback may return its own values.  Its descriptors and
  ;; check are valid only within the mapped proof.
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "mapped image requires dedicated configuration ownership"))
  (when *physical-reseed-follow-activation-scope* (error "mapped activation scope is already active"))
  (let ((*physical-reseed-follow-activation-scope* :acquiring))
    (call-with-physical-reseed-follow-mapped-predecessor
     owner
     (lambda (descriptors plan record install-state attempt snapshot check)
       (call-with-physical-reseed-follow-activation-context
        owner descriptors plan record install-state attempt snapshot check function)))))

(defmethod physical-reseed-follow-image-dispatch :around
    ((owner physical-reseed-follow-configuration-owner) function)
  ;; Route activation intent and explicit requests through the activation
  ;; proof.  Installation and activation scopes cannot overlap.
  (when (and *physical-reseed-follow-activation-request*
             (not (eq owner *physical-reseed-follow-activation-request*)))
    (error "activation request belongs to a different owner"))
  (let ((activation
          (call-with-physical-directory-root
           owner (lambda (fd directory)
                   (declare (ignore directory))
                   (when (physical-reseed-follow-entry fd "physical-reseed.follow")
                     (getf (physical-reseed-follow-read-at fd "physical-reseed.follow") :activation))))))
    (if (or activation *physical-reseed-follow-activation-request*)
        (progn
          (when (or *physical-reseed-follow-installation-scope*
                    *physical-reseed-follow-installation-request*)
            (error "activation cannot enter an installation operation"))
          (call-with-physical-reseed-follow-activation-image owner function))
        (call-next-method))))
