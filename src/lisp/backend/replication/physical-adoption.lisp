;;;; Read-only prerequisites for inspecting a stopped primary and its origin.
;;;; These checks do not perform adoption, remote proof, or installation.
(in-package #:pg)

(defun physical-adoption-attempt-path (&optional (directory *pgdata*))
  "Return the fixed local adoption attempt fence path."
  (merge-pathnames "physical-adoption.attempt" directory))

(defun physical-adoption-install-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-adoption.install" directory))

(defun physical-adoption-require-absent (path message)
  (handler-case
      (progn (sb-posix:lstat (namestring path)) (error message))
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
        (error condition))))
  (values))

(defun physical-adoption-check-no-install (directory)
  (physical-adoption-require-absent
   (physical-adoption-install-path directory)
   "offline adoption installation requires journal reconciliation"))

(defun physical-adoption-check-startup ()
  "Refuse startup on any attempt or installation journal entry or inspection error."
  (physical-adoption-check-no-install *pgdata*)
  (physical-adoption-require-absent
   (physical-adoption-attempt-path)
   "pending offline adoption attempt requires reconciliation before startup"))

(defstruct physical-adoption-owner
  "Thread-bound ownership of an existing offline PGDATA and its dedicated lock.
This context is invalid after its enclosing call exits."
  directory device inode lock lock-device lock-inode thread active-p)

(defun physical-adoption-check-owner (owner)
  "Recheck the active owner and the identities of its directory and lock inode."
  (unless (and (physical-adoption-owner-p owner)
               (physical-adoption-owner-active-p owner)
               (eq sb-thread:*current-thread* (physical-adoption-owner-thread owner))
               (open-stream-p (physical-adoption-owner-lock owner)))
    (error "offline adoption no longer owns its data directory"))
  (let* ((directory (physical-adoption-owner-directory owner))
         (stat (sb-posix:lstat (namestring directory)))
         (lock (sb-posix:lstat (namestring (merge-pathnames ".pg_lisp.lock" directory)))))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                 (zerop (logand #o077 (sb-posix:stat-mode stat)))
                 (= (physical-adoption-owner-device owner) (sb-posix:stat-dev stat))
                 (= (physical-adoption-owner-inode owner) (sb-posix:stat-ino stat))
                 (sb-posix:s-isreg (sb-posix:stat-mode lock))
                 (= 1 (sb-posix:stat-nlink lock))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid lock))
                 (= (physical-adoption-owner-lock-device owner) (sb-posix:stat-dev lock))
                 (= (physical-adoption-owner-lock-inode owner) (sb-posix:stat-ino lock)))
      (error "offline adoption data directory or lock inode changed")))
  owner)

(defun call-with-physical-adoption-owner (directory function)
  "Call FUNCTION with exclusive ownership of an existing private PGDATA lock.
Reject a PID file; invalidate the thread-bound owner and close the lock on exit.
The caller must keep inspected files immutable throughout the operation."
  (let* ((directory (uiop:ensure-directory-pathname (truename directory)))
         (stat (sb-posix:lstat (namestring directory))))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                 (zerop (logand #o077 (sb-posix:stat-mode stat))))
      (error "offline adoption requires an owned private data directory"))
    (sb-sys:without-interrupts
      (multiple-value-bind (lock size)
          (open-bounded-regular-file (merge-pathnames ".pg_lisp.lock" directory) 0)
        (declare (ignore size))
        (let ((owner nil))
          (unwind-protect
               (let ((locked (sb-posix:fstat (sb-sys:fd-stream-fd lock))))
                 (unless (and (= 1 (sb-posix:stat-nlink locked))
                              (= (sb-posix:geteuid) (sb-posix:stat-uid locked))
                              (zerop (physical-store-flock (sb-sys:fd-stream-fd lock) 6)))
                   (error "offline adoption could not own the dedicated PGDATA lock"))
                 (setf owner (make-physical-adoption-owner
                              :directory directory :device (sb-posix:stat-dev stat)
                              :inode (sb-posix:stat-ino stat) :lock lock
                              :lock-device (sb-posix:stat-dev locked)
                              :lock-inode (sb-posix:stat-ino locked)
                              :thread sb-thread:*current-thread* :active-p t))
                 (physical-adoption-check-owner owner)
                 (when (native-control-optional-bytes (merge-pathnames "postmaster.pid" directory))
                   (error "offline adoption requires a stopped primary without a PID file"))
                 (sb-sys:with-local-interrupts (funcall function owner)))
            (when owner (setf (physical-adoption-owner-active-p owner) nil))
            (close lock)))))))

(defun physical-adoption-inspect-primary (owner)
  "Inspect a stopped primary without creating, repairing, or replaying files.
Require DB_SHUTDOWNED and checkpoint = durable endpoint E, then verify the
checkpoint record, resources, catalog identity and allocator capacity. Return
actual next XID separately from the allocator's next value. Mutable facts must
be revalidated before any future installation."
  (physical-adoption-check-owner owner)
  (let* ((*pgdata* (physical-adoption-owner-directory owner))
         (*physical-recovery-state* nil)
         (*physical-recovery-failed-p* nil)
         (*catalog-identity-needs-persist-p* nil)
         (*catalog* (load-catalog))
         (capture (native-control-capture)))
    (when (or (second capture) (physical-recovery-read-signal-mode)
              (probe-file (xid-wrap-marker-path)))
      (error "offline adoption requires an ordinary clean primary"))
    (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace metadata)
        (wal-decode-control-line (bytes->string (first capture)))
      (unless (and (typep checkpoint '(integer 1))
                   (= checkpoint durable)
                   (= insertion checkpoint-timeline)
                   (= +db-shutdowned+ (getf metadata :db-state -1))
                   (wal-checkpoint-snapshot-valid-p snapshot)
                   (= insertion (getf snapshot :timeline))
                   (= checkpoint (getf snapshot :end))
                   (or (zerop (getf snapshot :oldest-active-xid))
                       (= (getf snapshot :oldest-active-xid) (getf snapshot :next-xid)))
                   (not *catalog-identity-needs-persist-p*)
                   (gethash "postgres" (catalog-databases *catalog*))
                   (eql (getf metadata :system-id) (catalog-system-identifier)))
        (error "offline adoption requires matching clean checkpoint and catalog identity"))
      (let* ((*wal-checkpoint-lsn* checkpoint)
             (*wal-durable-lsn* durable)
             (*wal-checkpoint-snapshot* snapshot)
             (*wal-insert-timeline* insertion)
             (*wal-checkpoint-timeline* checkpoint-timeline)
             (*wal-segment-namespace* namespace)
             (record (wal-read-checkpoint-record (getf snapshot :start) checkpoint))
             (allocation (load-next-xid))
             (history (timeline-read-history insertion)))
        (unless (and (equal snapshot (wal-checkpoint-record-snapshot record))
                     (= allocation (1+ (getf snapshot :xid-reserved-through)))
                     (<= (getf snapshot :next-xid) allocation)
                     (equalp (read-bounded-regular-file (merge-pathnames "PG_VERSION" *pgdata*) 3)
                             (string->bytes (format nil "16~%"))))
          (error "offline adoption checkpoint bytes or allocator capacity differ from control"))
        (physical-recovery-check-checkpoint-resources snapshot metadata)
        (native-control-facts capture)
        (physical-adoption-check-owner owner)
        (unless (equalp capture (native-control-capture))
          (error "offline adoption primary control changed during inspection"))
        (list :owner owner :system-id (catalog-system-identifier)
              :cluster-identity (catalog-cluster-identity *catalog*)
              :timeline insertion :namespace namespace :end checkpoint
              :checkpoint-start (getf snapshot :start) :snapshot snapshot :metadata metadata
              :next-xid (getf snapshot :next-xid) :allocation-next-xid allocation
              :history history :capture capture :catalog *catalog*)))))

(defun physical-adoption-hash-origin-suffix (origin end history &key (namespace :timeline))
  "Extend the origin digest through END using the caller's immutable WAL files.
Bounds and timeline arithmetic are checked here; this alone establishes neither
durability nor authority of ORIGIN. The caller must verify both independently."
  (physical-export-origin-check origin)
  (timeline-check-lineage history)
  (unless (and (typep end '(unsigned-byte 64))
               (<= (physical-export-origin-proven-lsn origin) end)
               (= (physical-export-origin-timeline origin)
                  (timeline-history-entry-timeline (first history))))
    (error "adoption origin suffix differs from its original timeline or endpoint"))
  (let ((digest (physical-export-origin-prefix-digest origin)))
    (multiple-value-bind (cursor files complete)
        (wal-scan-lineage
         (lambda (record owner carrier raw)
           (declare (ignore record owner carrier))
           (setf digest (physical-store-update-digest digest raw)))
         history (physical-export-origin-proven-lsn origin) end
         :namespace namespace :include-raw t)
      (declare (ignore files))
      (unless (and complete (= cursor end))
        (error "adoption origin suffix did not reach the exact endpoint"))
      digest)))

(defun physical-adoption-prove-primary-origin (state identity)
  "Read the source's durable origin and hash its WAL suffix to clean endpoint E.
Reinspect the primary, require the same system and original timeline, and recheck
control and origin after hashing. This evidence grants no installation or writer
authority; mutable facts need revalidation before a future installation."
  (let* ((owner (getf state :owner))
         (*pgdata* (physical-adoption-owner-directory (physical-adoption-check-owner owner)))
         (fresh (physical-adoption-inspect-primary owner)))
    (unless (and (equalp (getf state :capture) (getf fresh :capture))
                 (= (getf state :system-id) (getf fresh :system-id))
                 (= (getf state :allocation-next-xid) (getf fresh :allocation-next-xid)))
      (error "offline adoption primary state changed before its origin proof"))
    (destructuring-bind (system timeline base manifest) identity
      (let* ((key (make-physical-export-origin
                   :system-id system :timeline timeline :base base :manifest-digest manifest
                   :proven-lsn base :prefix-digest #xCBF29CE484222325))
             (path (physical-export-origin-path key))
             (origin (physical-export-origin-read path))
             (end (getf fresh :end)))
        (unless (and (= system (getf fresh :system-id))
                     (= timeline (getf fresh :timeline))
                     (<= (physical-export-origin-proven-lsn origin) end))
          (error "offline adoption origin belongs to another primary or exceeds its clean endpoint"))
        (let ((digest (physical-adoption-hash-origin-suffix
                       origin end (getf fresh :history) :namespace (getf fresh :namespace))))
          (physical-adoption-check-owner owner)
          (unless (and (equalp (getf fresh :capture) (native-control-capture))
                       (equalp origin (physical-export-origin-read path)))
            (error "offline adoption origin or primary control changed during proof"))
          (list :identity (physical-export-origin-identity origin)
                :proven-lsn (physical-export-origin-proven-lsn origin)
                :prefix-digest (physical-export-origin-prefix-digest origin)
                :end end :end-digest digest))))))
