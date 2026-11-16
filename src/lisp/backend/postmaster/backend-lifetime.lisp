(in-package #:pg)

(declaim (special *backend-registry* *backend-registry-lock*))

;; Track backend lifetimes independently of reusable OS thread IDs.  These weak
;; indices do not keep an otherwise unreachable backend or token alive.
(defvar *backend-lifetimes*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

(defvar *backend-lifetime-seals*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

(defvar *backend-lifetime-revocations*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

;; The token names its backend and carries a private state closure.  The seal
;; table independently binds the token to that exact closure.
(defstruct (backend-lifetime (:constructor %make-backend-lifetime))
  (backend nil :read-only t)
  (state nil :read-only t))

;; Test the token's immutable backend association.  This does not check that
;; the lifetime is live or grants authority.
(defun backend-lifetime-names-backend-p (token backend)
  (and (backend-lifetime-p token)
       (eq backend (backend-lifetime-backend token))))

;; Enroll only the registered backend on its own thread and OS thread ID.
;; A DEFVAR reload does not replace an older unsynchronized registry, so refuse
;; enrollment in that case.  The closure retains the expected identity facts.
(defun backend-lifetime-enroll-under-registry-lock (backend)
  (assert (sb-thread:holding-mutex-p *backend-registry-lock*))
  (when (and (sb-ext:hash-table-synchronized-p *backend-registry*)
             (eq backend (gethash (backend-pid backend) *backend-registry*))
             (eq (backend-thread backend) sb-thread:*current-thread*)
             (eql (backend-pid backend)
                  (sb-thread:thread-os-tid sb-thread:*current-thread*)))
    (let* ((registry *backend-registry*)
           (lifetimes *backend-lifetimes*)
           (revocations *backend-lifetime-revocations*)
           (pid (backend-pid backend))
           (thread (backend-thread backend))
           (active t)
           (token nil))
      (setf token
            (%make-backend-lifetime
             :backend backend
             :state
             (lambda (candidate action)
               (unless (eq candidate token)
                 (error "copied backend lifetime"))
               (ecase action
                 (:check
                  (unless
                      (and active
                           (eq registry *backend-registry*)
                           (eq lifetimes *backend-lifetimes*)
                           (eq revocations *backend-lifetime-revocations*)
                           (not (gethash token revocations))
                           (eq token (gethash backend lifetimes))
                           (eq backend (gethash pid registry))
                           (eql pid (backend-pid backend))
                           (eq thread (backend-thread backend))
                           (sb-thread:thread-alive-p thread)
                           (handler-case
                               (eql pid (sb-thread:thread-os-tid thread))
                             (error () nil)))
                    (setf active nil)
                    (error "stale backend lifetime"))
                  token)))))
      (setf (gethash token *backend-lifetime-seals*) (backend-lifetime-state token)
            (gethash backend *backend-lifetimes*) token))))

;; Look up the independently sealed state while holding WAL exclusion.
(defun backend-lifetime-sealed-state-under-wal-lock (token)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (backend-lifetime-p token)
    (let ((state (backend-lifetime-state token)))
      (when (and (functionp state) (eq state (gethash token *backend-lifetime-seals*)))
        state))))

;; Checks may be made by an observing thread.  The closure checks the owner's
;; registry binding, thread, OS ID and liveness.  A copied token is rejected
;; before it can poison the genuine token's state.
;;
;; Remember seal loss in the weak revocation table so restoring a seal cannot
;; revive a token after a failed check.
(defun backend-lifetime-check-under-wal-lock (token)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (unless (backend-lifetime-p token)
      (error "missing backend lifetime"))
    (when (gethash token *backend-lifetime-revocations*)
      (error "revoked backend lifetime"))
    (let ((state (backend-lifetime-sealed-state-under-wal-lock token)))
      (unless state
        (setf (gethash token *backend-lifetime-revocations*) t)
        (error "unissued backend lifetime"))
      (funcall state token :check))))

;; Only the owning thread may capture its backend's current lifetime.
(defun backend-lifetime-capture-under-wal-lock (backend)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (eq (backend-thread backend) sb-thread:*current-thread*)
    (error "backend lifetime capture requires its owning thread"))
  (let ((token (gethash backend *backend-lifetimes*)))
    (unless (backend-lifetime-names-backend-p token backend)
      (error "backend lifetime capture changed its registered owner"))
    (backend-lifetime-check-under-wal-lock token)))

;; Revoke every token for this backend before removing its current binding.
;; Lock the seal table while scanning: enrollment writes it under the registry
;; lock, independently of the WAL lock.  Weak revocations prevent revival.
(defun backend-lifetime-retire-under-wal-lock (backend)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (let ((tokens nil))
      ;; Ignore a foreign alias; the seal scan also finds this backend's tokens.
      (let ((current (gethash backend *backend-lifetimes*)))
        (when (backend-lifetime-names-backend-p current backend)
          (push current tokens)))
      (sb-ext:with-locked-hash-table (*backend-lifetime-seals*)
        (maphash (lambda (token state)
                   (declare (ignore state))
                   (when (backend-lifetime-names-backend-p token backend)
                     (pushnew token tokens :test #'eq)))
                 *backend-lifetime-seals*))
      (dolist (token tokens)
        (setf (gethash token *backend-lifetime-revocations*) t)
        (remhash token *backend-lifetime-seals*))
      (remhash backend *backend-lifetimes*)))
  (values))
