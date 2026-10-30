(in-package #:pg)

(defstruct (managed-worker-handle
            (:constructor %make-managed-worker-handle (slot generation completion)))
  (slot 0 :read-only t)
  (generation 0 :read-only t)
  (completion nil :read-only t))

(defvar *managed-worker-slots* nil)

(defvar *managed-worker-registration-closed-p* nil)

(defvar *managed-worker-generation* 0)
(defvar *managed-worker-lock* (sb-thread:make-mutex :name "managed background workers"))

(defun managed-worker-entry-under-lock (handle)
  (unless (sb-thread:holding-mutex-p *managed-worker-lock*)
    (error "managed worker registry requires exclusion"))
  (check-type handle managed-worker-handle)
  (let ((slot (managed-worker-handle-slot handle)))
    (when (and *managed-worker-slots* (< slot (length *managed-worker-slots*)))
      (let ((entry (aref *managed-worker-slots* slot)))
        (when (and entry (= (managed-worker-handle-generation handle) (getf entry :generation)))
          entry)))))

(defun managed-worker-register (function &key (name "native background worker")
                                           database parallel (restart :never))
  (unless (and (functionp function) (stringp name) (<= 1 (length name) 128))
    (error "invalid managed background worker"))
  (when (or database parallel (not (eq restart :never)))
    (error "managed workers support only one-shot jobs without a database context"))
  (sb-thread:with-mutex (*managed-worker-lock*)
    (unless (and (vectorp *managed-worker-slots*)
                 (resource-parameters-valid-p *installed-resource-parameters*)
                 (= (length *managed-worker-slots*)
                    (resource-parameters-workers *installed-resource-parameters*)))
      (error "managed worker capacity is not installed"))
    (when (or *managed-worker-registration-closed-p* (shutdown-in-progress-p))
      (return-from managed-worker-register nil))
    (let ((slot (position nil *managed-worker-slots*)))
      (when slot
        (let* ((generation (incf *managed-worker-generation*))
               (handle (%make-managed-worker-handle slot generation (sb-thread:make-semaphore)))
               (entry (list :generation generation :state :registered :function function
                            :name (copy-seq name) :thread nil :stop-requested nil :handle handle)))
          (setf (aref *managed-worker-slots* slot) entry)
          handle)))))

(defun managed-worker-status (handle)
  (sb-thread:with-mutex (*managed-worker-lock*)
    (let ((entry (managed-worker-entry-under-lock handle)))
      (if entry (getf entry :state) :stopped))))

(defun managed-worker-request-stop (handle)
  (sb-thread:with-mutex (*managed-worker-lock*)
    (let ((entry (managed-worker-entry-under-lock handle)))
      (when entry
        (if (eq (getf entry :state) :registered)
            (managed-worker-release-under-lock handle)
            (sb-sys:without-interrupts
              (setf (getf entry :stop-requested) t (getf entry :state) :stopping)))
        t))))

(defvar *current-managed-worker* nil)

(defvar *managed-worker-test-hook* nil)

(defun managed-worker-release-under-lock (handle)
  (unless (sb-thread:holding-mutex-p *managed-worker-lock*)
    (error "managed worker release requires exclusion"))
  (when (managed-worker-entry-under-lock handle)
    (sb-sys:without-interrupts
      (setf (aref *managed-worker-slots* (managed-worker-handle-slot handle)) nil)
      (sb-thread:signal-semaphore (managed-worker-handle-completion handle)))
    t))

(defun managed-worker-complete (handle)
  (sb-thread:with-mutex (*managed-worker-lock*)
    (let ((entry (managed-worker-entry-under-lock handle)))
      (when entry
        (when (and (getf entry :thread)
                   (not (eq (getf entry :thread) sb-thread:*current-thread*)))
          (error "another thread still owns this managed worker"))
        (managed-worker-release-under-lock handle)))))

(defun managed-worker-stop-requested-p (&optional (handle *current-managed-worker*))
  (unless handle (error "no current managed worker"))
  (sb-thread:with-mutex (*managed-worker-lock*)
    (let ((entry (managed-worker-entry-under-lock handle)))
      (or (null entry) (getf entry :stop-requested) (shutdown-in-progress-p)))))

(defun managed-worker-main (handle gate slots installed)
  (let ((*managed-worker-slots* slots)
        (*installed-resource-parameters* installed)
        (*current-managed-worker* handle))
    (unwind-protect
         (progn
           (sb-thread:wait-on-semaphore gate)
           (let ((function
                   (sb-thread:with-mutex (*managed-worker-lock*)
                     (let ((entry (managed-worker-entry-under-lock handle)))
                       (when (and entry (not (getf entry :stop-requested))
                                  (not (shutdown-in-progress-p)))
                         (setf (getf entry :state) :running)
                         (getf entry :function))))))
             (when function
               (handler-case (funcall function)
                 (sb-ext:timeout (condition)
                   (admin-log "managed background worker timed out: ~a" condition))
                 (error (condition) (admin-log "managed background worker failed: ~a" condition))))))
      (managed-worker-complete handle))))

(defun managed-worker-start (handle)
  (let ((gate nil) (thread nil) (claimed nil) (published nil) (name nil)
                   (slots *managed-worker-slots*) (installed *installed-resource-parameters*))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*managed-worker-lock*)
             (let ((entry (managed-worker-entry-under-lock handle)))
               (unless (and entry (eq (getf entry :state) :registered))
                 (return-from managed-worker-start nil))
               (sb-sys:without-interrupts
                 (setf claimed t name (getf entry :name) (getf entry :state) :starting))))
           (sb-sys:without-interrupts
             (when *managed-worker-test-hook*
               (funcall *managed-worker-test-hook* :before-gate-create))
             (setf gate (sb-thread:make-semaphore))
             (when *managed-worker-test-hook* (funcall *managed-worker-test-hook* :before-thread-create))
             (setf thread
                   (sb-thread:make-thread
                    (lambda () (managed-worker-main handle gate slots installed))
                    :name name))
             (when *managed-worker-test-hook* (funcall *managed-worker-test-hook* :after-thread-create))
             (sb-thread:with-mutex (*managed-worker-lock*)
               (let ((entry (managed-worker-entry-under-lock handle)))
                 (unless entry (error "managed worker lost its reservation before publication"))
                 (setf (getf entry :thread) thread published t)))
             (sb-thread:signal-semaphore gate))
           handle)
      (when (and claimed (not published))
        (if thread
            (progn (managed-worker-request-stop handle) (sb-thread:signal-semaphore gate))
            (managed-worker-complete handle))))))

(defun managed-worker-wait (handle &key (timeout 5))
  (when (or (plusp *live-operation-depth*)
            (sb-thread:holding-mutex-p *managed-worker-lock*))
    (error "managed worker wait requires no admission or registry lock"))
  (sb-thread:with-mutex (*managed-worker-lock*)
    (let ((entry (managed-worker-entry-under-lock handle)))
      (when (and entry (eq (getf entry :thread) sb-thread:*current-thread*))
        (error "managed worker cannot wait for itself"))))
  (when (eq :stopped (managed-worker-status handle)) (return-from managed-worker-wait t))
  (when (sb-thread:wait-on-semaphore (managed-worker-handle-completion handle) :timeout timeout)
    (sb-thread:signal-semaphore (managed-worker-handle-completion handle))
    t))

(defun start-managed-background-worker (function &rest options)
  (let ((handle nil) (accepted nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf handle (apply #'managed-worker-register function options)))
           (when handle
             (when *managed-worker-test-hook*
               (funcall *managed-worker-test-hook* :after-registration))
             (sb-sys:without-interrupts
               (setf accepted (not (null (managed-worker-start handle)))))
             (when accepted handle)))
      (when (and handle (not accepted)) (managed-worker-request-stop handle)))))

(defun managed-worker-request-shutdown ()
  (let ((handles
          (sb-thread:with-mutex (*managed-worker-lock*)
            (setf *managed-worker-registration-closed-p* t)
            (when *managed-worker-slots*
              (loop for entry across *managed-worker-slots*
                    when entry collect (getf entry :handle))))))
    (dolist (handle handles) (managed-worker-request-stop handle))
    handles))

(defun managed-worker-shutdown ()
  (when (plusp *live-operation-depth*)
    (error "managed worker shutdown requires no operation admission"))
  (dolist (handle (managed-worker-request-shutdown))
    (unless (managed-worker-wait handle :timeout 5)
      (error "managed background worker did not stop before storage shutdown")))
  (values))
