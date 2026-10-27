;;;; control.lisp -- out-of-band control plane: hot reload and a Lisp REPL.
;;;;
;;;; The backend normally speaks the PostgreSQL wire protocol.  This file adds
;;;; a second, non-SQL interface for developers and operators:
;;;;
;;;;   * SIGHUP recompiles and reloads the "pg" system in the running image,
;;;;     the same way PostgreSQL reloads configuration.
;;;;   * An optional REPL is exposed on a loopback TCP port and/or a Unix
;;;;     domain socket, enabled with --control-port / --control-socket.
;;;;
;;;; The REPL evaluates arbitrary Lisp, so it is off unless explicitly asked
;;;; for; the TCP variant is always bound to 127.0.0.1 and the Unix socket is
;;;; protected by filesystem permissions.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-posix))

;;; ---- in-image reload ----

;; The process's real stdout/stderr, captured at startup so that a reload
;; triggered from a REPL connection can send ASDF's chatter to the server log
;; instead of the client.
(defvar *server-output* nil)
(defvar *server-error* nil)

(defun capture-server-streams ()
  (setf *server-output* *standard-output*
        *server-error* *error-output*))

;; Serialize SIGHUP reloads and explicit (reload) calls so they cannot run
;; concurrently and interleave a half-compiled image.
(defvar *reload-run-lock* (sb-thread:make-mutex :name "pg reload run lock"))

;; A reload closes admission to SQL operations and waits for current queries to
;; leave before redefining functions used by backend threads.
(defvar *live-operation-lock*
  (sb-thread:make-mutex :name "pg live operation lock"))
(defvar *live-operation-cv*
  (sb-thread:make-waitqueue :name "pg live operation condition"))
(defvar *live-operation-admission-open* t)

(defvar *live-operation-terminal-shutdown* nil)

(defvar *live-operation-drain-open* nil)
(defvar *live-operation-count* 0)
(defvar *live-operation-backend-counts* (make-hash-table :test #'eq))
(defvar *live-operation-depth* 0)

(defstruct live-operation-token
  "Ownership of one outer operation, whether active or parked.

BACKEND and THREAD identify the original owner.  SNAPSHOT-OWNERS records the
statement registrations captured while parked."
  backend thread lane
  (state :pending)
  snapshot-owners)

(defvar *live-operation-current-token* nil)

(defvar *live-operation-parked-p* nil)

(defvar *live-operation-parked* (make-hash-table :test #'eq)
  "Parked operation tokens, protected by *LIVE-OPERATION-LOCK*.")

(defvar *live-operation-park-generation* 0)

(defvar *live-operation-parking-test-hook* nil)

(defvar *live-operation-current-lane* nil)
(defvar *live-operation-check-cancel-p* nil)
(defvar *live-operation-quiescence-owner* nil)
(defvar *live-operation-quiescence-owner-thread* nil)
(defvar *live-operation-drain-backends* nil)
(defvar *live-operation-cleanup-backends* nil)
(defvar *live-operation-admission-wait-hook* nil)
(defvar *live-operation-pending-cancel-requests* nil)
(defvar *live-operation-backend-query-tokens* (make-hash-table :test #'eq))
(defvar *live-operation-cancel-request-admission-hook* nil)
(defvar *live-operation-before-open-hook* nil)

(declaim (ftype function list-backends))
(declaim (ftype function lookup-backend cancel-key-matches-p))
(declaim (special *backend*))

(defvar *reload-requested* nil)

(defun request-reload (&rest signal-info)
  "Record a reload request and return immediately.

Signal handlers run in whichever thread the OS delivered the signal to, so
this must not take a lock (the signal may land in the reloader itself). ASDF
must not run inside a handler either; the reloader thread polls the flag."
  (declare (ignore signal-info))
  (setf *reload-requested* t))

(defun live-operation-lock-owned-p ()
  (eq (sb-thread:mutex-owner *live-operation-lock*)
      sb-thread:*current-thread*))

(defun call-with-live-operation-lock (function)
  "Call FUNCTION while owning the admission mutex, even across interrupts."
  (let ((acquired-p nil))
    (sb-sys:without-interrupts
      (sb-thread:grab-mutex *live-operation-lock*)
      (setf acquired-p t)
      (unwind-protect
           (sb-sys:allow-with-interrupts (funcall function))
        (when (and acquired-p (live-operation-lock-owned-p))
          (sb-thread:release-mutex *live-operation-lock*))))))

(defun call-with-live-operation-lock-if-needed (function)
  "Call FUNCTION with the admission mutex, preserving any existing ownership."
  (sb-sys:without-interrupts
    (let ((already-owned-p (live-operation-lock-owned-p)))
      (unless already-owned-p
        (sb-thread:grab-mutex *live-operation-lock*))
      (unwind-protect
           (funcall function)
        (unless already-owned-p
          (when (live-operation-lock-owned-p)
            (sb-thread:release-mutex *live-operation-lock*))))))
  (values))

(defun adopt-legacy-live-operation-quiescence ()
  "Adopt a closed gate when this code is loaded by an older reload function."
  (let ((reload-owner (sb-thread:mutex-owner *reload-run-lock*)))
    (when reload-owner
      (call-with-live-operation-lock
       (lambda ()
         (when (and (not *live-operation-admission-open*)
                    (null *live-operation-quiescence-owner*))
           (setf *live-operation-quiescence-owner*
                 (gensym "LEGACY-LIVE-OPERATION-OWNER-")
                 *live-operation-quiescence-owner-thread* reload-owner
                 *live-operation-drain-open* nil
                 *live-operation-drain-backends* nil
                 *live-operation-cleanup-backends* nil))))))
  (values))

;; During the first in-place reload, the old RELOAD function may have closed
;; admission before the ownership fields above existed. It still owns the
;; reload mutex while ASDF loads this file, so attach that gate to its thread;
;; its old no-argument OPEN call can then release it safely.
(eval-when (:load-toplevel)
  (adopt-legacy-live-operation-quiescence))

(defun live-operation-admitted-p (drain-p)
  "Return the lane admitted now, evaluating DRAIN-P under the gate lock."
  (cond
    (*live-operation-terminal-shutdown* nil)
    (*live-operation-admission-open* :normal)
    ((and *live-operation-drain-open*
          (if (functionp drain-p) (funcall drain-p) drain-p))
     :drain)
    (t nil)))

(defun wait-for-live-operation-admission (drain-p &optional backend)
  (when *live-operation-parked-p*
    (error "parked callbacks cannot enter live operations"))
  (call-with-live-operation-lock
   (lambda ()
     (loop do (when (and *live-operation-check-cancel-p*
                         backend
                         (backend-cancel-pending backend))
                (check-for-interrupts))
           until (live-operation-admitted-p drain-p)
           do (when *live-operation-admission-wait-hook*
                (funcall *live-operation-admission-wait-hook*))
              (sb-thread:condition-wait *live-operation-cv*
                                        *live-operation-lock*))))
  (values))

(defun enter-live-operation-locked (lane backend)
  (when *live-operation-parked-p*
    (error "parked callbacks cannot enter live operations"))
  (sb-sys:without-interrupts
    (let ((next-count (1+ *live-operation-count*))
          (next-backend-count
            (when backend (1+ (gethash backend *live-operation-backend-counts* 0)))))
      (when backend
        (setf (gethash backend *live-operation-backend-counts*) next-backend-count))
      (setf *live-operation-count* next-count)))
  lane)

(defun try-enter-live-operation (drain-p &optional backend)
  "Count an operation if its admission lane remains open; never wait here."
  (call-with-live-operation-lock
   (lambda ()
     (sb-sys:without-interrupts
       (when (and *live-operation-check-cancel-p*
                  backend
                  (backend-cancel-pending backend))
         (check-for-interrupts))
       (let ((lane (live-operation-admitted-p drain-p)))
         (when lane
           (enter-live-operation-locked lane backend)))))))

(defun remove-live-operation-credit-under-lock (backend)
  (unless (plusp *live-operation-count*)
    (error "live operation count underflow"))
  (when backend
    (let ((count (gethash backend *live-operation-backend-counts*)))
      (unless (and count (plusp count))
        (error "live operation backend count underflow"))
      (if (= count 1)
          (remhash backend *live-operation-backend-counts*)
          (setf (gethash backend *live-operation-backend-counts*) (1- count)))))
  (decf *live-operation-count*)
  (when (zerop *live-operation-count*)
    (sb-thread:condition-broadcast *live-operation-cv*))
  (values))

(defun release-live-operation-credit (backend &optional token)
  (let ((finished-p nil)
        (lock-acquired-p nil)
        (pending-interrupt nil))
    (loop until finished-p
          do (progn
               (handler-case
                   (sb-sys:without-interrupts
                     (when (sb-thread:grab-mutex *live-operation-lock* :waitp nil)
                       (setf lock-acquired-p t)
                       (unwind-protect
                            (progn
                              (when token
                                (unless (and (eq :active (live-operation-token-state token))
                                             (eq backend (live-operation-token-backend token)))
                                  (error "live operation cleanup token mismatch")))
                              (remove-live-operation-credit-under-lock backend)
                              (when token (setf (live-operation-token-state token) :released))
                              (setf finished-p t))
                         (sb-thread:release-mutex *live-operation-lock*))))
                 (error (condition)
                   (if lock-acquired-p (error condition)
                       (setf pending-interrupt condition))))
               (unless finished-p
                 (handler-case (sb-thread:thread-yield)
                   (error (condition) (setf pending-interrupt condition))))))
    (when pending-interrupt (error pending-interrupt)))
  (values))

(defun live-operation-parked-count ()
  (call-with-live-operation-lock
   (lambda () (hash-table-count *live-operation-parked*))))

(defun live-operation-parked-owners-under-quiescence ()
  "Copy parked ownership for the caller that owns closed, drained admission.

Validate each parked thread while holding the admission lock."
  (call-with-live-operation-lock
   (lambda ()
     (unless (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
                  (not *live-operation-drain-open*)
                  (zerop *live-operation-count*))
       (error "parked ownership census requires closed admission"))
     (loop for token being the hash-keys of *live-operation-parked*
           do (unless (and (eq :parked (live-operation-token-state token))
                           (sb-thread:thread-alive-p (live-operation-token-thread token)))
                (error "invalid parked operation owner"))
           collect (list token (live-operation-token-backend token)
                         (live-operation-token-thread token)
                         (copy-tree (live-operation-token-snapshot-owners token)))))))

(defun live-operation-parking-test-point (point token)
  (when *live-operation-parking-test-hook*
    (funcall *live-operation-parking-test-hook* point token)))

(defun capture-live-operation-snapshot-owners ()
  "Capture active enclosing registration tokens, generations and snapshots.

Hold the snapshot lock while reading their lifecycle state."
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (loop for registration in (remove-duplicates *statement-snapshot-registrations* :test #'eq)
          do (unless (snapshot-registration-p registration)
               (error "invalid enclosing statement snapshot registration"))
             (when (snapshot-registration-releasing-p registration)
               (error "cannot park during snapshot release"))
          when (snapshot-registration-active-p registration)
            collect (list registration
                          (snapshot-registration-lifecycle-generation registration)
                          (snapshot-registration-snapshot registration)))))

(defun park-live-operation-under-lock (token owners)
  "Transfer the current operation's active credit to TOKEN under the gate lock.

The caller masks interrupts.  The operation retains one parked owner until its
credit is restored."
  (unless (and (eq token *live-operation-current-token*)
               (eq (live-operation-token-thread token) sb-thread:*current-thread*)
               (eq (live-operation-token-state token) :active)
               (plusp *live-operation-count*)
               (let ((backend (live-operation-token-backend token)))
                 (or (null backend) (plusp (gethash backend *live-operation-backend-counts* 0)))))
    (error "parking requires the current active admission token"))
  (let ((published nil))
    (unwind-protect
         (progn
           (setf (gethash token *live-operation-parked*) t)
           (remove-live-operation-credit-under-lock (live-operation-token-backend token))
           (setf (live-operation-token-snapshot-owners token) owners
                 (live-operation-token-state token) :parked
                 published t)
           (incf *live-operation-park-generation*)
           (sb-thread:condition-broadcast *live-operation-cv*))
      (unless published (remhash token *live-operation-parked*)))))

(defun live-operation-resume-lane ()
  (if *live-operation-terminal-shutdown*
      (when (null *live-operation-quiescence-owner*) :terminal-cleanup)
      (live-operation-admitted-p t)))

(defun restore-live-operation-token (token)
  "Restore TOKEN's active credit before allowing its stack to unwind.

Defer interrupts while waiting for a resume lane.  Terminal cleanup waits for
any foreign quiescence owner to release the gate, then restores the credit
before signaling FATAL."
  (sb-sys:without-interrupts
    (let ((restored nil) (pending nil) (lane nil))
      (labels ((defer (condition)
                 (when (or (null pending) (fatal-error-p condition))
                   (setf pending condition))))
        (loop until restored
              do (handler-case
                     (sb-sys:allow-with-interrupts
                       (call-with-live-operation-lock
                        (lambda ()
                          (loop until (setf lane (live-operation-resume-lane))
                                do (sb-thread:condition-wait *live-operation-cv* *live-operation-lock*))
                          (sb-sys:without-interrupts
                            (unless (and (eq :parked (live-operation-token-state token))
                                         (gethash token *live-operation-parked*)
                                         (eq (live-operation-token-thread token) sb-thread:*current-thread*))
                              (error "parked admission ownership mismatch"))
                            (enter-live-operation-locked lane (live-operation-token-backend token))
                            (remhash token *live-operation-parked*)
                            (setf (live-operation-token-state token) :active
                                  (live-operation-token-lane token) lane
                                  (live-operation-token-snapshot-owners token) nil
                                  restored t)
                            (incf *live-operation-park-generation*)
                            (sb-thread:condition-broadcast *live-operation-cv*)))))
                   (pg-error (condition) (defer condition))
                   (sb-ext:timeout (condition) (defer condition)))))
      (setf *live-operation-current-lane* lane)
      (cond ((and pending (fatal-error-p pending)) (error pending))
            ((eq lane :terminal-cleanup)
             (ereport-fatal "57P01" "terminating connection due to administrator command"))
            (pending (error pending)))))
  (values))

(defun call-with-live-operation-parked (function)
  "Call FUNCTION while the current operation is parked, then restore its credit.

Detach owned SELECT index scans before capturing snapshots and parking admission.
The caller must make other suspended resources safe across concurrent work.  The
callback must not enter operation admission or access storage.  Cleanup restores
ownership on every exit."
  (let ((token *live-operation-current-token*))
    (unless (and (live-operation-token-p token) (plusp *live-operation-depth*)
                 (not *live-operation-parked-p*)
                 (eq :active (live-operation-token-state token)))
      (error "parking requires admitted work"))
    (when (live-operation-lock-owned-p)
      (error "cannot park while owning the admission mutex"))
    (detach-select-index-scans)
    (let ((owners (capture-live-operation-snapshot-owners)))
      (live-operation-parking-test-point :before-park token)
      (sb-sys:without-interrupts
        (unwind-protect
             (progn
               (call-with-live-operation-lock-if-needed
                (lambda () (park-live-operation-under-lock token owners)))
               (sb-sys:allow-with-interrupts
                 (let ((*live-operation-depth* 0) (*live-operation-parked-p* t))
                   (live-operation-parking-test-point :after-park token)
                   (funcall function))))
          (when (eq (live-operation-token-state token) :parked)
            (unwind-protect
                 (live-operation-parking-test-point :before-restore token)
              (restore-live-operation-token token))
            (unwind-protect
                 (live-operation-parking-test-point :after-restore token)
              (sb-sys:allow-with-interrupts
                (sb-sys:with-local-interrupts (values))))))))))

(defun leave-live-operation (&optional backend token)
  (unwind-protect
       (when (and token (eq :parked (live-operation-token-state token)))
         (restore-live-operation-token token))
    (release-live-operation-credit backend token)))

(defun finish-live-operation-drain ()
  ;; Called with *LIVE-OPERATION-LOCK* held. Drain work may finish an active
  ;; transaction when it is already needed to release an operation in flight,
  ;; but an idle transaction must not hold reload open while waiting for its
  ;; client to send another query.
  (loop while (plusp *live-operation-count*)
        do (sb-thread:condition-wait *live-operation-cv*
                                     *live-operation-lock*))
  ;; Close the drain lane atomically with observing an empty operation set.
  ;; Otherwise new drain work could overlap ASDF's function redefinitions.
  (setf *live-operation-drain-open* nil)
  (values))

(defun snapshot-live-operation-backends ()
  "Capture all cleanup targets and the narrower set eligible for transaction drain."
  (let ((drain-backends (make-hash-table :test #'eq))
        (cleanup-backends (make-hash-table :test #'eq)))
    (when (fboundp 'list-backends)
      (dolist (backend (list-backends))
        (setf (gethash backend cleanup-backends) t)
        (when (member (backend-state backend) '(:in-progress :aborted))
          (setf (gethash backend drain-backends) t))))
    (maphash (lambda (backend count)
               (when (plusp count)
                 (setf (gethash backend cleanup-backends) t
                       (gethash backend drain-backends) t)))
             *live-operation-backend-counts*)
    (values (loop for backend being the hash-keys of drain-backends collect backend)
            (loop for backend being the hash-keys of cleanup-backends collect backend))))

(defun capture-live-operation-cancel-target (body)
  "Capture the active query named by BODY while owning the admission lock."
  (let* ((pid (bytes-to-u32 body 4))
         (key (bytes-to-u32 body 8))
         (backend (lookup-backend pid)))
    (when (and backend
               (integerp (backend-cancel-key backend))
               (cancel-key-matches-p key (backend-cancel-key backend))
               (backend-executing backend))
      (let ((query-token (gethash backend *live-operation-backend-query-tokens*)))
        (when query-token
          (cons backend query-token))))))

(defun try-enter-live-operation-cancel-request (body backend)
  "Atomically admit or defer BODY while owning the admission lock."
  (when *live-operation-parked-p*
    (error "parked callbacks cannot admit or defer cancel requests"))
  (call-with-live-operation-lock
   (lambda ()
     (when *live-operation-cancel-request-admission-hook*
       (funcall *live-operation-cancel-request-admission-hook*))
     (let ((lane (live-operation-admitted-p t)))
       (cond
         (lane
          (enter-live-operation-locked lane backend))
         ((and (not *live-operation-admission-open*)
               (not *live-operation-drain-open*))
          (let ((target (capture-live-operation-cancel-target body)))
            (when target
              (push target *live-operation-pending-cancel-requests*))
            (if target :queued :ignored)))
         (t nil))))))

(defun apply-pending-live-operation-cancel-requests ()
  "Apply deferred requests only to the query active when each arrived."
  (let ((requests (nreverse *live-operation-pending-cancel-requests*)))
    (setf *live-operation-pending-cancel-requests* nil)
    (dolist (target requests)
      ;; Query start/end and request capture use this lock too. The token check
      ;; prevents an idle cancel from leaking into a later command on this PID.
      (ignore-errors
       (let ((backend (car target))
             (query-token (cdr target)))
         (when (and (eq query-token
                        (gethash backend *live-operation-backend-query-tokens*))
                    (backend-executing backend))
           (setf (backend-cancel-pending backend) t)
           (sb-thread:condition-broadcast *live-operation-cv*))))))
  (values))

(defun restore-live-operation-admission (owner)
  (call-with-live-operation-lock-if-needed
   (lambda ()
     (when (eq *live-operation-quiescence-owner* owner)
       (apply-pending-live-operation-cancel-requests)
       (setf *live-operation-quiescence-owner* nil
             *live-operation-quiescence-owner-thread* nil
             *live-operation-drain-backends* nil
             *live-operation-cleanup-backends* nil
             *live-operation-drain-open* nil
             *live-operation-admission-open* (not *live-operation-terminal-shutdown*))
       (sb-thread:condition-broadcast *live-operation-cv*)))))

(defun fence-live-operation-shutdown ()
  "Close admission and wait for both active and parked operations to drain.

The primary finalizer separately holds reload exclusion."
  (when *live-operation-parked-p*
    (error "parked callbacks cannot start a shutdown fence"))
  (when (plusp *live-operation-depth*)
    (error "shutdown fence requires an unadmitted caller"))
  (call-with-live-operation-lock
   (lambda ()
     (when (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
                (plusp (hash-table-count *live-operation-parked*)))
       (error "release owned quiescence before draining parked shutdown cleanup"))
     (setf *live-operation-terminal-shutdown* t
           *live-operation-admission-open* nil
           *live-operation-drain-open* nil)
     (sb-thread:condition-broadcast *live-operation-cv*)
     (loop while (or (plusp *live-operation-count*)
                     (plusp (hash-table-count *live-operation-parked*)))
           do (sb-thread:condition-wait *live-operation-cv* *live-operation-lock*))))
  (values))

(defun close-live-operation-admission (&optional requested-owner)
  (when (plusp *live-operation-depth*)
    (error "cannot reload from inside a live operation"))
  ;; The optional legacy call shape supports an already-running reloader whose
  ;; compiled macro expansion predates explicit gate-owner tokens.
  (let ((owner (or requested-owner
                   (gensym "LIVE-OPERATION-QUIESCENCE-OWNER-"))))
    (call-with-live-operation-lock
     (lambda ()
       (loop while *live-operation-quiescence-owner*
             do (when (eq *live-operation-quiescence-owner-thread*
                          sb-thread:*current-thread*)
                  (error "nested live-operation quiescence is not allowed"))
                (sb-thread:condition-wait *live-operation-cv*
                                          *live-operation-lock*))
       ;; Transaction-state setters and operation counters use this lock. Capture
       ;; both open transactions and sessions already executing admitted work.
       (multiple-value-bind (drain-backends cleanup-backends)
           (snapshot-live-operation-backends)
         (setf *live-operation-drain-backends* drain-backends
               *live-operation-cleanup-backends* cleanup-backends))
       (let ((finished-p nil))
         ;; Do not allow an asynchronous interrupt between claiming the gate and
         ;; installing the cleanup that releases it.
         (sb-sys:without-interrupts
           (setf *live-operation-quiescence-owner* owner
                 *live-operation-quiescence-owner-thread* sb-thread:*current-thread*
                 *live-operation-admission-open* nil
                 *live-operation-drain-open* t)
           (unwind-protect
                (sb-sys:allow-with-interrupts
                  (finish-live-operation-drain)
                  (setf finished-p t))
             ;; CONDITION-WAIT may unwind with the mutex held or released. Restore
             ;; the gate only after reacquiring it and confirming our ownership.
             (unless finished-p
               (restore-live-operation-admission owner)))))))
    (values)))

(defun open-live-operation-admission (&optional requested-owner)
  (when *live-operation-before-open-hook*
    (funcall *live-operation-before-open-hook*))
  (call-with-live-operation-lock
   (lambda ()
     ;; A failed nested close must not release a different invocation's gate.
     (when (if requested-owner
               (eq *live-operation-quiescence-owner* requested-owner)
               (eq *live-operation-quiescence-owner-thread*
                   sb-thread:*current-thread*))
       (apply-pending-live-operation-cancel-requests)
       (sb-sys:without-interrupts
         (setf *live-operation-quiescence-owner* nil
               *live-operation-quiescence-owner-thread* nil
               *live-operation-drain-backends* nil
               *live-operation-cleanup-backends* nil
               *live-operation-drain-open* nil
               *live-operation-admission-open* (not *live-operation-terminal-shutdown*))
         (sb-thread:condition-broadcast *live-operation-cv*)))))
  (values))

(defmacro with-live-operation-internal ((drain-p &optional check-cancel-p) &body body)
  (let ((outermost (gensym "OUTERMOST-"))
        (admitted (gensym "ADMITTED-"))
        (backend (gensym "BACKEND-"))
        (token (gensym "TOKEN-"))
        (drain-test (gensym "DRAIN-TEST-"))
        (operation-block (gensym "LIVE-OPERATION-")))
    `(let* ((,outermost (zerop *live-operation-depth*))
            (,backend *backend*)
            (,token (when ,outermost
                      (make-live-operation-token :backend ,backend :thread sb-thread:*current-thread*)))
            (,drain-test ,(when drain-p `(lambda () ,drain-p)))
            (,admitted nil)
            (*live-operation-check-cancel-p* ,check-cancel-p))
       (if ,outermost
           (block ,operation-block
             (loop
               (wait-for-live-operation-admission ,drain-test ,backend)
               (sb-sys:without-interrupts
                 (setf ,admitted (try-enter-live-operation ,drain-test ,backend))
                 (when ,admitted
                   (setf (live-operation-token-state ,token) :active
                         (live-operation-token-lane ,token) ,admitted)
                   (return-from ,operation-block
                     (unwind-protect
                          (sb-sys:allow-with-interrupts
                            (let ((*live-operation-depth* (1+ *live-operation-depth*))
                                  (*live-operation-current-lane* ,admitted)
                                  (*live-operation-current-token* ,token))
                              (progn ,@body)))
                       (leave-live-operation ,backend ,token)))))))
           (let ((*live-operation-depth* (1+ *live-operation-depth*)))
             (progn ,@body))))))

(defmacro with-live-operation (&body body)
  "Run BODY while protecting its function definitions from a live reload."
  `(with-live-operation-internal (nil) ,@body))

(defmacro with-live-operation-shutdown-cleanup (&body body)
  "Run fatal connection cleanup without waiting during backend-terminating shutdown.

Use only for sending a fatal response and releasing connection resources; this
lane must not run query or storage work."
  `(if (and (fboundp 'shutdown-terminates-backends-p)
            (shutdown-terminates-backends-p))
       (progn ,@body)
       (with-live-operation-captured-backend-cleanup ,@body)))

(defmacro with-live-operation-transaction-drain (&body body)
  "Run work from a backend transaction that is already in progress."
  `(with-live-operation-internal
       ((and *backend*
             (member *backend* *live-operation-drain-backends* :test #'eq)
             (member (backend-state *backend*) '(:in-progress :aborted))))
     ,@body))

(defmacro with-live-operation-cancel-request ((request) &body body)
  "Count CancelRequest work during drain, deferring only an active query target."
  (let ((request-value (gensym "CANCEL-REQUEST-"))
        (backend (gensym "BACKEND-"))
        (decision (gensym "DECISION-"))
        (outermost (gensym "OUTERMOST-"))
        (block-name (gensym "CANCEL-REQUEST-BLOCK-")))
    `(let ((,request-value ,request)
           (,backend *backend*)
           (,outermost (zerop *live-operation-depth*)))
       (if (not ,outermost)
           (let ((*live-operation-depth* (1+ *live-operation-depth*))
                 (*live-operation-check-cancel-p* nil))
             (progn ,@body))
           (block ,block-name
             (loop
               (let ((,decision nil))
                 ;; Admission and deferral are one locked decision. If admission
                 ;; reopens, this request either enters first or queues its exact
                 ;; query target before any awakened waiter can acquire the lock.
                 (sb-sys:without-interrupts
                   (let ((*live-operation-check-cancel-p* nil))
                     (setf ,decision
                           (try-enter-live-operation-cancel-request
                            ,request-value ,backend)))
                   (cond
                     ((member ,decision '(:queued :ignored))
                      (return-from ,block-name ,decision))
                     (,decision
                      (return-from ,block-name
                        (unwind-protect
                             (sb-sys:allow-with-interrupts
                               (let ((*live-operation-depth* 1)
                                     (*live-operation-current-lane* ,decision)
                                     (*live-operation-check-cancel-p* nil))
                                 (progn ,@body)))
                          (leave-live-operation ,backend)))))))))))))

(defmacro with-live-operation-captured-backend-cleanup (&body body)
  "Run backend bookkeeping for a session connected when reload closed admission."
  `(with-live-operation-internal
       ((and *backend*
             (or (member *backend* *live-operation-cleanup-backends* :test #'eq)
                 (member *backend* *live-operation-drain-backends* :test #'eq))))
     ,@body))

(defmacro with-live-operation-backend-shutdown-cleanup ((backend) &body body)
  "Bind BACKEND while admitting its shutdown and teardown cleanup."
  (let ((backend-value (gensym "BACKEND-")))
    `(let* ((,backend-value ,backend)
            (*backend* ,backend-value))
       (with-live-operation-shutdown-cleanup ,@body))))

(defmacro with-live-operation-query-state-cleanup (&body body)
  "Clear query admission state without waiting during backend-terminating shutdown."
  `(if (and (fboundp 'shutdown-terminates-backends-p)
            (shutdown-terminates-backends-p))
       (progn ,@body)
       (with-live-operation-captured-backend-cleanup ,@body)))

(defmacro with-live-operation-query-dispatch (drain-command-p &body body)
  "Admit transaction-ending commands from captured transactions during drain."
  `(with-live-operation-internal
       ((and ,drain-command-p
             *backend*
             (member *backend* *live-operation-drain-backends* :test #'eq)
             (member (backend-state *backend*) '(:in-progress :aborted)))
        t)
     ,@body))

(defmacro with-live-operation-quiescence (&body body)
  "Run BODY after existing live operations drain and before new ones enter."
  (let ((owner (gensym "LIVE-OPERATION-QUIESCENCE-")))
    `(let ((,owner (gensym "LIVE-OPERATION-QUIESCENCE-OWNER-")))
       (unwind-protect
            (progn
              (close-live-operation-admission ,owner)
              ,@body)
         (open-live-operation-admission ,owner)))))

(defun try-call-with-live-operation-reload (function)
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (let ((generation
              (call-with-live-operation-lock
               (lambda ()
                 (when (plusp (hash-table-count *live-operation-parked*))
                   *live-operation-park-generation*)))))
        (if generation
            (values nil nil generation)
            (values t (multiple-value-list (funcall function)) nil))))))

(defun call-with-live-operation-reload (function)
  "Run FUNCTION under reload exclusion after active and parked work has drained.

If parked owners remain after admission drains, reopen the gate and release the
reload lock before waiting for a change and retrying."
  (when (or (plusp *live-operation-depth*) *live-operation-parked-p*)
    (error "reload exclusion requires an outside caller"))
  (loop
    (multiple-value-bind (finished values generation)
        (try-call-with-live-operation-reload function)
      (when finished (return (values-list values)))
      (call-with-live-operation-lock
       (lambda ()
         (loop while (and (= generation *live-operation-park-generation*)
                          (plusp (hash-table-count *live-operation-parked*)))
               do (sb-thread:condition-wait *live-operation-cv* *live-operation-lock*)))))))

(defun reload ()
  (when (or (plusp *live-operation-depth*) *live-operation-parked-p*)
    (request-reload)
    (return-from reload :queued))
  (format t "~&pg-lisp: reloading code...~%")
  (finish-output)
  (let ((ok
          (call-with-live-operation-reload
           (lambda ()
             (handler-case
                 (let ((*standard-output* (or *server-output* *standard-output*))
                       (*error-output* (or *server-error* *error-output*))
                       (*compile-verbose* nil) (*load-verbose* nil))
                   (asdf:load-system "pg" :force t)
                   (startup-reload-settings)
                   t)
               (error (condition)
                 (format *error-output* "~&pg-lisp: reload failed: ~a~%" condition)
                 (finish-output *error-output*)
                 nil))))))
    (format t "~&pg-lisp: reload ~:[failed (see server log)~;complete~]~%" ok)
    (finish-output)
    ok))

(defun reload-loop ()
  (loop
    (if (sb-ext:compare-and-swap *reload-requested* t nil)
        (reload)
        (sleep 0.05))))

(defun enable-reload ()
  "Install the SIGHUP handler and start the reloader thread."
  (sb-sys:enable-interrupt sb-unix:sighup #'request-reload)
  (sb-thread:make-thread #'reload-loop :name "pg reloader")
  (values))

;;; ---- control REPL ----

;; A symbol, not a gensym, so the tag survives a reload: a connection that is
;; already blocked on CATCH keeps seeing the QUIT that the redefined function
;; throws.
(defvar *control-quit-tag* 'control-repl-quit)

(defun quit ()
  "Close the current control REPL connection."
  (throw *control-quit-tag* nil))

(defvar *control-socket-path* nil)

(defun cleanup-control-socket ()
  "Remove the Unix control socket, if one was created."
  (when *control-socket-path*
    (ignore-errors (delete-file *control-socket-path*))
    (setf *control-socket-path* nil)))

(defun enable-cleanup ()
  "Arrange for the Unix control socket to be removed on exit.

SIGTERM/SIGINT/SIGQUIT are handled by the shutdown machinery in
postmaster.lisp; each shutdown mode exits through SB-EXT:EXIT and runs this
hook."
  (pushnew 'cleanup-control-socket sb-ext:*exit-hooks*)
  (values))

(defun private-control-socket-path (path)
  "Resolve PATH inside a directory owned by this user with mode 0700 or stricter."
  (let* ((path (pathname path))
         (directory (truename
                     (merge-pathnames
                      (make-pathname :name nil :type nil :defaults path)
                      *default-pathname-defaults*)))
         (status (sb-posix:stat (namestring directory)))
         (mode (sb-posix:stat-mode status)))
    (unless (and (= (logand mode sb-posix:s-ifmt) sb-posix:s-ifdir)
                 (= (sb-posix:stat-uid status) (sb-posix:getuid))
                 (= (logand mode #o700) #o700)
                 (zerop (logand mode #o077)))
      (error "control socket directory must be owned by this user and mode 0700: ~a"
             directory))
    (make-pathname :directory (pathname-directory directory)
                   :name (pathname-name path)
                   :type (pathname-type path)
                   :version nil)))

(defun make-local-listener (path)
  "Return a listening AF_UNIX stream socket bound to PATH.

The socket node and its owner-only parent directory are both restricted:
this listener evaluates arbitrary Lisp."
  (let ((s (make-instance 'sb-bsd-sockets:local-socket
                          :type :stream :protocol 0))
        (bound nil))
    (handler-case
        (progn
          (sb-bsd-sockets:socket-bind s (namestring path))
          (setf bound t)
          (sb-posix:chmod (namestring path) #o700)
          (sb-bsd-sockets:socket-listen s 16)
          s)
      (error (e)
        (ignore-errors (sb-bsd-sockets:socket-close s))
        (when bound
          (ignore-errors (delete-file path)))
        (error "cannot create protected control socket ~a: ~a" path e)))))

(defun control-accept-loop (listener name)
  (loop
    (let ((sock (accept-connection-or-nil listener name)))
      (when sock
        (sb-thread:make-thread
         (lambda () (control-serve sock))
         :name "pg control connection")))))

(declaim (ftype function physical-base-backup))

(declaim (ftype function physical-recovery-select-history))

(defun control-eval (stream form)
  "Evaluate FORM, printing its values (or an error) to STREAM."
  (handler-case
      (format stream "~&~{~s~^~%~}~%"
              (multiple-value-list
               (cond
                 ((and (consp form) (eq (first form) 'reload) (null (rest form)))
                  (reload))
                 ((and (consp form) (eq (first form) 'physical-base-backup)
                       (= (length form) 3) (every #'stringp (rest form)))
                  ;; Literal arguments only: do not evaluate arbitrary argument
                  ;; expressions outside live admission. The backup owns its gate.
                  (physical-base-backup (second form) (third form)))
                 ((and (consp form)
                       (member (first form) '(physical-recovery-enable-streaming physical-recovery-replay physical-recovery-restartpoint physical-recovery-prepare-shutdown physical-recovery-promote physical-follow-stop wal-migrate-timeline-namespace))
                       (null (rest form)))
                  (funcall (first form)))
                 ((and (consp form) (eq (first form) 'physical-recovery-select-history)
                       (= (length form) 4) (integerp (second form)) (stringp (third form))
                       (listp (fourth form)) (= (length (fourth form)) +wal-end-of-recovery-record-size+)
                       (every (lambda (byte) (typep byte '(unsigned-byte 8))) (fourth form)))
                  (physical-recovery-select-history
                   (second form) (third form) (coerce (fourth form) '(vector (unsigned-byte 8)))))
                 ((and (consp form) (eq (first form) 'physical-follow-start)
                       (= (length form) 4) (stringp (second form))
                       (integerp (third form)) (stringp (fourth form)))
                  (physical-follow-start (second form) (third form) (fourth form)))
                 (t (with-live-operation (eval form))))))
    (error (e)
      (format stream "~&;; error: ~a~%" e))))

(defun read-control-form (stream)
  "Read one form, with reader evaluation disabled.

The binding must cover READ only: EVAL may run code that reads source (a
reload, say), which legitimately uses #. for compile-time constants."
  (let ((*read-eval* nil))
    (read stream nil :eof)))

(defun control-read-eval-loop (stream)
  "Read forms from STREAM until EOF, printing each result."
  (loop
    (format stream "~&pg> ")
    (finish-output stream)
    (let ((form (handler-case (read-control-form stream)
                  (error (e)
                    (format stream "~&;; read error: ~a~%" e)
                    :skip))))
      (cond
        ((eq form :eof) (return))
        ((eq form :skip))
        (t (control-eval stream form))))))

(defun control-serve (sock)
  "Read-eval-print loop for one control connection."
  (let ((stream nil))
    (unwind-protect
         (handler-case
             (progn
               (setf stream (sb-bsd-sockets:socket-make-stream
                             sock :input t :output t
                             :element-type 'character :buffering :line))
               (let ((*standard-output* stream)
                     (*error-output* stream)
                     (*trace-output* stream)
                     (*query-io* stream)
                     (*package* (find-package '#:pg)))
                 (format stream "~&pg-lisp control REPL~%server ~a, package ~a~%~
                                 (reload) reloads code, (quit) disconnects.~%"
                         *server-version* (package-name *package*))
                 (finish-output stream)
                 (catch *control-quit-tag*
                   (control-read-eval-loop stream))
                 (format stream "~&bye~%")))
           ;; A client that vanished shows up as a stream error; anything else
           ;; is a bug worth logging.
           (stream-error () nil)
           (error (e)
             (format *error-output* "~&pg-lisp: control connection error: ~a~%"
                     e)
             nil))
      (when stream
        (ignore-errors (finish-output stream))
        (ignore-errors (close stream)))
      (ignore-errors (sb-bsd-sockets:socket-close sock)))))

(defun start-control-plane (&key port socket-path)
  "Start the control REPL on loopback TCP PORT and/or Unix SOCKET-PATH.

Both are optional; with neither, no control plane is started."
  (when socket-path
    (setf socket-path (private-control-socket-path socket-path)))
  (let ((local-listener nil)
        (tcp-listener nil)
        (local-path (and socket-path (pathname socket-path)))
        (started nil))
    (unwind-protect
         (progn
           ;; Bind and secure every requested endpoint before accepting on any
           ;; of them. A local-socket permission failure must not leave TCP REPL
           ;; threads serving while startup unwinds.
           (when local-path
             (setf local-listener (make-local-listener local-path)))
           (when port
             (setf tcp-listener (make-listener "127.0.0.1" port)))
           (when local-listener
             (setf *control-socket-path* (namestring local-path)))
           (when (or port local-path)
             (enable-cleanup))
           (when local-listener
             (sb-thread:make-thread
              (lambda () (control-accept-loop local-listener "control unix"))
              :name "pg control unix")
             (format t "~&pg-lisp: control REPL on ~a~%"
                     (namestring local-path)))
           (when tcp-listener
             (sb-thread:make-thread
              (lambda () (control-accept-loop tcp-listener "control tcp"))
              :name "pg control tcp")
             (format t "~&pg-lisp: control REPL on 127.0.0.1:~a~%" port))
           (when (or port local-path)
             (finish-output))
           (setf started t)
           (values))
      (unless started
        (setf *control-socket-path* nil)
        (when local-listener
          (ignore-errors (sb-bsd-sockets:socket-close local-listener)))
        (when local-listener
          (ignore-errors (delete-file local-path)))
        (when tcp-listener
          (ignore-errors (sb-bsd-sockets:socket-close tcp-listener)))))))

(defvar *swank-listener-enabled-p* nil
  "The optional debugger evaluates outside normal operation admission.")

(defun start-swank-listener (port bind-address)
  "Start the optional Swank listener on BIND-ADDRESS and PORT."
  (when port
    (let* ((package (find-package "SWANK"))
           (create-server (and package (find-symbol "CREATE-SERVER" package)))
           (global-debugger (and package (find-symbol "*GLOBAL-DEBUGGER*" package))))
      (unless (and create-server (fboundp create-server))
        (error "pinned Swank system is not loaded"))
      ;; A Swank connection can still debug its own requests, but must not
      ;; redirect unhandled errors from SQL or control threads to Emacs.
      (when global-debugger
        (set global-debugger nil))
      (handler-case
          (funcall create-server :port port
                                 :interface bind-address
                                 :style :spawn
                                 :dont-close t)
        (error (e)
          (error "cannot start Swank on ~a:~a: ~a" bind-address port e)))
      (setf *swank-listener-enabled-p* t)
      (format t "~&pg-lisp: Swank listening on ~a:~a~%" bind-address port)
      (finish-output)))
  (values))
