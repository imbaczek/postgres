;;;; logical-retention.lisp -- runtime cleanup after slot invalidation.

(in-package #:pg)

(defun wal-invalidate-lagging-replication-slots-under-wal-lock ()
  "Durably lose over-limit slots before checkpoint recycling removes their WAL."
  (when (integerp *max-slot-wal-keep-bytes*)
    (maphash
     (lambda (name slot)
       (declare (ignore name))
       (when (and (eq (wal-replication-slot-wal-status slot) :reserved)
                  (wal-replication-slot-restart-lsn slot)
                  (> (- *wal-durable-lsn*
                        (wal-replication-slot-restart-lsn slot))
                     *max-slot-wal-keep-bytes*))
         (let ((candidate (wal-copy-replication-slot-with-profile slot)))
           (setf (wal-replication-slot-wal-status candidate) :lost
                 (wal-replication-slot-pending-xids candidate)
                 (make-hash-table :test #'eql))
           ;; Like InvalidatePossiblyObsoleteSlot() in slot.c, persist loss
           ;; before dropping the live retention pin.
           (wal-save-replication-slot candidate)
           (setf (wal-replication-slot-wal-status slot) :lost
                 (wal-replication-slot-pending-xids slot)
                 (wal-replication-slot-pending-xids candidate))
           (when (and (wal-logical-slot-p slot) *wal-replication-slot-loss-collector*)
             (push slot (car *wal-replication-slot-loss-collector*))))))
     *wal-replication-slots*))
  (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
  (physical-source-invalidate-lagging-pins-under-wal-lock)
  (values))

(defun wal-lost-replication-slots ()
  "Return current lost generations so a later maintenance pass can retry cleanup."
  (sb-thread:with-mutex (*wal-lock*)
    (loop for slot being the hash-values of *wal-replication-slots*
          when (and (wal-logical-slot-p slot) (eq (wal-replication-slot-wal-status slot) :lost))
            collect slot)))

(defun call-with-invalidated-slot-cleanup (function)
  "Run WAL maintenance and retire lost-slot consumers after its locks drain."
  (let ((*wal-replication-slot-loss-collector* (list nil)))
    (unwind-protect
         (funcall function)
      ;; Re-scan durable runtime state: an interrupted cleanup may have lost its
      ;; local collector after marking a slot lost but before token adoption.
      (cleanup-invalidated-logical-replication-state
       (remove-duplicates
        (append (nreverse (car *wal-replication-slot-loss-collector*))
                (wal-lost-replication-slots))
        :test #'eq)))))

(defun wal-checkpoint ()
  "Write cached pages and then clean up any slots lost by this checkpoint."
  (prevent-recovery-primary-operation "CHECKPOINT")
  (call-with-invalidated-slot-cleanup #'wal-checkpoint-core)
  (values))

(defun wal-remove-checkpointed-segments (checkpoint-lsn)
  (call-with-invalidated-slot-cleanup
   (lambda ()
     (with-wal-critical
       (wal-remove-checkpointed-segments-under-wal-lock checkpoint-lsn))))
  (values))

(defun detach-exported-logical-snapshots-for-slots-under-registry-lock (slots)
  "Transfer matching exports to retryable cleanup while the registry is held."
  (let ((entries nil))
    (sb-sys:without-interrupts
      (labels ((collect-matching (token state)
                 (when (member (exported-logical-snapshot-slot-object state)
                               slots :test #'eq)
                   (pushnew (cons token state) entries
                            :test (lambda (left right)
                                    (eq (cdr left) (cdr right)))))))
        (maphash #'collect-matching *exported-logical-snapshots*)
        (maphash #'collect-matching *expired-exported-logical-snapshots*))
      (dolist (entry entries)
        (let ((token (car entry))
              (state (cdr entry)))
          (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
            (setf (exported-logical-snapshot-expired-p state) t))
          (when (eq state (gethash token *exported-logical-snapshots*))
            (remhash token *exported-logical-snapshots*))
          (setf (gethash token *expired-exported-logical-snapshots*) state))))
    (mapcar #'cdr entries)))

(defun wal-drop-replication-slot (name)
  "Durably remove NAME and adopt its exported snapshots for retryable cleanup."
  (unless (wal-valid-replication-slot-name-p name)
    (error "invalid replication slot name ~s" name))
  (let ((removed-slot nil))
    ;; Registry -> WAL matches snapshot publication. Adopt the exact slot's
    ;; exports before releasing either lock, so a dropped slot is never the only
    ;; remaining owner of cleanup work.
    (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
      (sb-sys:without-interrupts
        (sb-thread:with-mutex (*wal-lock*)
          (let ((slot (gethash name *wal-replication-slots*)))
            (unless slot
              (ereport "42704" "replication slot \"~a\" does not exist" name))
            (when (wal-replication-slot-active-owner slot)
              (ereport "55006" "replication slot \"~a\" is active" name))
            (wal-delete-replication-slot-file name)
            (setf removed-slot slot)
            (remhash name *wal-replication-slots*)
            (wal-refresh-logical-capture-slot-mirror-under-wal-lock)))
        (when removed-slot
          (detach-exported-logical-snapshots-for-slots-under-registry-lock
           (list removed-slot))
          (logical-test-failpoint :after-logical-slot-drop-detach name))))
    (when removed-slot
      (cleanup-invalidated-logical-replication-state (list removed-slot))))
  (values))

(defvar *logical-slot-owner-termination-lock*
  (sb-thread:make-mutex :name "logical slot owner termination requests"))

(defvar *logical-slot-owner-termination-requests*
  (make-hash-table :test #'eq))

(defun wal-release-replication-slot-owner (name owner)
  "Release OWNER's runtime claim and backend association."
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*wal-lock*)
      (let ((slot (gethash name *wal-replication-slots*)))
        (when (and slot (eq owner (wal-replication-slot-active-owner slot)))
          (setf (wal-replication-slot-active-owner slot) nil
                (wal-replication-slot-active-owner-backend slot) nil)
          (sb-thread:with-mutex (*logical-slot-owner-termination-lock*)
            (remhash slot *logical-slot-owner-termination-requests*))))))
  (values))

(defun call-with-logical-replication-slot (name function &key allow-initial-copy)
  "Hold an exclusive runtime claim and remember its backend for invalidation."
  (let ((owner (gensym "LOGICAL-SLOT-"))
        (slot nil))
    (unwind-protect
         (progn
           ;; Establish cleanup before the owner becomes visible. Publication is
           ;; interrupt-atomic so invalidation sees both owner fields or neither.
           (sb-sys:without-interrupts
             (sb-thread:with-mutex (*wal-lock*)
               (setf slot (gethash name *wal-replication-slots*))
               (unless slot
                 (ereport "42704" "replication slot \"~a\" does not exist" name))
               (unless (wal-replication-slot-database-oid slot)
                 (ereport "55000" "replication slot has no database binding"))
               (when (eq (wal-replication-slot-wal-status slot) :lost)
                 (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
               (unless (or (wal-replication-slot-confirmed-flush-lsn slot)
                           allow-initial-copy)
                 (ereport "55000" "replication slot initial copy is not acknowledged"))
               (when (wal-replication-slot-active-owner slot)
                 (ereport "55006" "replication slot \"~a\" is active" name))
               (when (and *backend*
                          (/= (database-oid (current-database))
                              (wal-replication-slot-database-oid slot)))
                 (ereport "55000" "replication slot belongs to another database"))
               (setf (wal-replication-slot-active-owner slot) owner
                     (wal-replication-slot-active-owner-backend slot) *backend*)))
           (logical-test-failpoint :after-logical-slot-owner-publication name)
           (let ((*wal-logical-slot-owner* owner)
                 (*wal-logical-slot-read-progress*
                   (or (wal-replication-slot-confirmed-flush-lsn slot)
                       (wal-replication-slot-consistent-lsn slot))))
             (funcall function slot)))
      ;; Safe even if claim validation failed or the slot was replaced: release
      ;; only clears the exact owner token published above.
      (wal-release-replication-slot-owner name owner))))

(defun register-exported-logical-snapshot (state)
  "Publish STATE only while its exact slot is still durably reserved."
  (let ((token (exported-logical-snapshot-token state))
        (slot-name (exported-logical-snapshot-slot-name state))
        (slot-object (exported-logical-snapshot-slot-object state)))
    (unless slot-object
      (error "exported logical snapshot has no slot identity"))
    ;; Registry -> WAL is intentional. Invalidation releases WAL before it
    ;; enters the registry, so token publication cannot race a lost generation.
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
        (when (or (gethash token *exported-logical-snapshots*)
                  (gethash token *expired-exported-logical-snapshots*)
                  (gethash token *transaction-exported-snapshots*))
          (error "duplicate exported logical snapshot token"))
        (sb-thread:with-mutex (*wal-lock*)
          (unless (and (eq slot-object (gethash slot-name *wal-replication-slots*))
                       (eq (wal-replication-slot-wal-status slot-object) :reserved))
            (ereport "55000"
                     "replication slot was lost before snapshot publication"))
          (setf (gethash token *exported-logical-snapshots*) state))))
    state))

(defun remove-exported-logical-snapshot-token (token)
  "Move TOKEN to the pending-cleanup registry before releasing its owner."
  (let ((state nil))
    (unwind-protect
         (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
           (setf state (or (gethash token *exported-logical-snapshots*)
                           (gethash token *expired-exported-logical-snapshots*)))
           (when (and state (gethash token *exported-logical-snapshots*))
             ;; Imports hold this registry lock through importer publication.
             ;; Expire and transfer ownership before a new importer can look up
             ;; the token or an interrupt can abandon cleanup.
             (sb-sys:without-interrupts
               (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
                 (setf (exported-logical-snapshot-expired-p state) t))
               (remhash token *exported-logical-snapshots*)
               (setf (gethash token *expired-exported-logical-snapshots*) state))))
      (when state
        (finish-expired-exported-logical-snapshot state)))
    state))

(defun release-exported-logical-snapshot-for-slot (slot-name &optional slot-object)
  "Retire only the matching slot generation's exported snapshot."
  (let ((tokens nil))
    (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
      (maphash
       (lambda (token state)
         (when (and (string= slot-name
                             (exported-logical-snapshot-slot-name state))
                    (or (null slot-object)
                        (eq slot-object
                            (exported-logical-snapshot-slot-object state))))
           (push token tokens)))
       *exported-logical-snapshots*))
    (dolist (token tokens)
      (remove-exported-logical-snapshot-token token)))
  (values))

(defun detach-exported-logical-snapshots-for-slots (slots)
  "Expire matching tokens and transfer them to retryable pending cleanup."
  (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
    (detach-exported-logical-snapshots-for-slots-under-registry-lock slots)))

(defun copy-exported-logical-snapshot-importers (state)
  (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
    (copy-list (exported-logical-snapshot-importers state))))

(defvar *logical-test-invalidated-backend-hook* nil)

(defun request-invalidated-backend-termination (backend)
  (if *logical-test-invalidated-backend-hook*
      (funcall *logical-test-invalidated-backend-hook* backend)
      (let ((socket (backend-socket backend)))
        ;; A peer may have stopped reading. Break socket I/O before delivering
        ;; the single graceful termination, like slot.c does outside shared locks.
        (when socket
          (ignore-errors (sb-bsd-sockets:socket-shutdown socket :direction :io)))
        (ignore-errors (terminate-backend backend))))
  (values))

(defun wait-for-invalidated-backend (backend &optional (timeout 5))
  (let ((thread (backend-thread backend)))
    (when (and thread
               (not (eq thread sb-thread:*current-thread*))
               (sb-thread:thread-alive-p thread))
      ;; Do not asynchronously abort an unwind already releasing MVCC and schema
      ;; ownership. A timed-out backend retains its pins until its cleanup ends.
      (handler-case
          (sb-thread:join-thread thread :timeout timeout)
        (sb-thread:join-thread-error () nil))))
  (values))

(defun terminate-invalidated-backend (backend &optional (timeout 5))
  (request-invalidated-backend-termination backend)
  (wait-for-invalidated-backend backend timeout))

(defun request-lost-slot-owner-termination (slot backend)
  "Send one graceful termination to this current lost slot generation's owner."
  (let ((claimed nil))
    (logical-test-failpoint
     :before-lost-slot-owner-termination-claim slot backend)
    (sb-sys:without-interrupts
      ;; Cleanup can hold a stale owner/backend pair captured before another
      ;; pass let the owner release. Revalidate under WAL, then claim in WAL ->
      ;; termination-registry order, matching owner release's lock order.
      (sb-thread:with-mutex (*wal-lock*)
        (when (and (eq slot
                       (gethash (wal-replication-slot-name slot)
                                *wal-replication-slots*))
                   (eq (wal-replication-slot-wal-status slot) :lost)
                   (eq backend
                       (wal-replication-slot-active-owner-backend slot)))
          (sb-thread:with-mutex (*logical-slot-owner-termination-lock*)
            (unless (gethash slot *logical-slot-owner-termination-requests*)
              (setf (gethash slot *logical-slot-owner-termination-requests*)
                    backend
                    claimed t)))))
      ;; Signal after both locks are released, but before interrupts resume.
      (when claimed
        (request-invalidated-backend-termination backend)))
    claimed))

(defun logical-pending-expired-snapshot-states ()
  (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
    (loop for state being the hash-values
            of *expired-exported-logical-snapshots*
          collect state)))

(defun request-exported-snapshot-importer-termination (state)
  (let ((requested-backends nil))
    (dolist (backend (copy-exported-logical-snapshot-importers state))
      (unless (eq backend *backend*)
        ;; Record each backend separately before its one graceful request. Keep
        ;; claim and request uninterruptible, but release the registry lock
        ;; before touching its socket or thread.
        (sb-sys:without-interrupts
          (let ((claimed nil))
            (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
              (unless (member
                       backend
                       (exported-logical-snapshot-termination-requested-backends
                        state)
                       :test #'eq)
                (push backend
                      (exported-logical-snapshot-termination-requested-backends
                       state))
                (setf claimed t)))
            (when claimed
              (request-invalidated-backend-termination backend)
              (push backend requested-backends))))))
    (nreverse requested-backends)))

(defun cleanup-invalidated-logical-replication-state (slots)
  "Revoke lost slot tokens and stop consumers after WAL locks are released.

PostgreSQL's slot.c invalidation path terminates the active walsender outside
shared locks. SnapBuildClearExportedSnapshot() similarly retires the exported
snapshot only after its importer transaction has ended."
  (let* ((new-states (detach-exported-logical-snapshots-for-slots slots))
         (pending-states (logical-pending-expired-snapshot-states))
         (states (remove-duplicates (append new-states pending-states) :test #'eq))
         (importer-backends
           (loop for state in states
                 append (copy-exported-logical-snapshot-importers state)))
         (owner-requests
           (loop for slot in slots
                 for backend = (wal-replication-slot-active-owner-backend slot)
                 when (and backend
                           (not (member backend importer-backends :test #'eq)))
                   collect (cons slot backend)))
         (owner-backends (mapcar #'cdr owner-requests))
         (newly-requested-owners
           (loop for (slot . backend) in owner-requests
                 when (request-lost-slot-owner-termination slot backend)
                   collect backend))
         (newly-requested-importers
           (loop for state in states
                 append (request-exported-snapshot-importer-termination state)))
         (backends (remove-duplicates
                    (append owner-backends importer-backends)
                    :test #'eq))
         (current-backend-p (member *backend* backends :test #'eq)))
    (unwind-protect
         (progn
           (dolist (backend backends)
             (unless (eq backend *backend*)
               (when (or (member backend newly-requested-owners :test #'eq)
                         (member backend newly-requested-importers :test #'eq)
                         (member backend owner-backends :test #'eq))
                 (wait-for-invalidated-backend backend))))
           (dolist (state states)
             (finish-expired-exported-logical-snapshot state))
           ;; Self termination is last: its transaction unwind releases the
           ;; imported MVCC pin and completes deferred schema cleanup.
           (when current-backend-p
             (terminate-backend *backend*)))
      ;; Pending registry entries keep ownership if cleanup is interrupted.
      (dolist (state states)
        (ignore-errors (finish-expired-exported-logical-snapshot state)))))
  (values))
