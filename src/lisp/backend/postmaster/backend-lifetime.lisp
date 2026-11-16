(in-package #:pg)

(declaim (special *backend-registry* *backend-registry-lock*))

(defvar *backend-lifetimes*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

(defvar *backend-lifetime-seals*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

(defvar *backend-lifetime-revocations*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

(defstruct (backend-lifetime (:constructor %make-backend-lifetime))
  (backend nil :read-only t)
  (state nil :read-only t))

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

(defun backend-lifetime-sealed-state-under-wal-lock (token)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (backend-lifetime-p token)
    (let ((state (backend-lifetime-state token)))
      (when (and (functionp state) (eq state (gethash token *backend-lifetime-seals*)))
        state))))

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

(defun backend-lifetime-capture-under-wal-lock (backend)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (eq (backend-thread backend) sb-thread:*current-thread*)
    (error "backend lifetime capture requires its owning thread"))
  (backend-lifetime-check-under-wal-lock
   (gethash backend *backend-lifetimes*)))

(defun backend-lifetime-retire-under-wal-lock (backend)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (let ((tokens nil))
      (let ((current (gethash backend *backend-lifetimes*)))
        (when current (push current tokens)))
      (sb-ext:with-locked-hash-table (*backend-lifetime-seals*)
        (maphash (lambda (token state)
                   (declare (ignore state))
                   (when (and (backend-lifetime-p token)
                              (eq backend (backend-lifetime-backend token)))
                     (pushnew token tokens :test #'eq)))
                 *backend-lifetime-seals*))
      (dolist (token tokens)
        (setf (gethash token *backend-lifetime-revocations*) t)
        (remhash token *backend-lifetime-seals*))
      (remhash backend *backend-lifetimes*)))
  (values))
