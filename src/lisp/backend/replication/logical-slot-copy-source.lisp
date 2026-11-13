(in-package #:pg)

(defvar *logical-slot-copy-restoration-failure* nil)

(declaim (ftype function logical-slot-copy-validate-startup))

(defun logical-slot-copy-check-startup ()
  (logical-slot-copy-validate-startup))

(defun logical-slot-copy-check-identity (gate-owner ownership)
  (check-live-operation-startup-quiescence gate-owner)
  (postmaster-check-ownership ownership)
  (prevent-recovery-primary-operation "logical slot base copy")
  (when *swank-listener-enabled-p*
    (error "logical slot base copy requires Swank to be off"))
  (when (or (not (active-xids-empty-p)) *xid-wrap-pending*
            (probe-file (xid-wrap-marker-path)))
    (error "logical slot base copy requires no active transaction IDs or pending XID wrap"))
  (when (physical-recovery-reader-conflict-p)
    (error "logical slot base copy requires no retained readers or idle transactions"))
  (let ((system-id (physical-slot-startup-system-id)))
    (sb-thread:with-mutex (*wal-lock*)
      (let ((fields (multiple-value-list (wal-read-control-fields))))
        (unless (and *catalog* (not *catalog-identity-needs-persist-p*)
                     (eql system-id (catalog-system-identifier))
                     (eql system-id (getf *wal-control-metadata* :system-id))
                     (equal fields
                            (list *wal-checkpoint-lsn* *wal-durable-lsn* *wal-checkpoint-snapshot*
                                  *wal-insert-timeline* *wal-checkpoint-timeline*
                                  *wal-segment-namespace* *wal-control-metadata*)))
          (error "logical slot base copy requires matching durable primary identity"))))
    system-id))

(defun logical-slot-copy-source-preflight (gate-owner ownership)
  (let ((system-id (logical-slot-copy-check-identity gate-owner ownership)))
    (sb-thread:with-mutex (*wal-lock*)
      (unless (and (primary-wal-writer-available-under-lock-p)
                   (not (eq sb-thread:*current-thread*
                            (primary-wal-writer-thread *primary-wal-writer*))))
        (error "logical slot base copy requires an owned healthy primary writer"))
      (values *primary-wal-writer* system-id))))

(defun logical-slot-copy-check-paused-writer (writer system-id gate-owner ownership)
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (unless (eq writer *primary-wal-writer*)
      (error "logical slot copy replaced its paused writer"))
    (let ((failure (primary-wal-writer-failure writer)))
      (when failure
        (error (if (typep failure 'condition) failure
                   (make-condition 'simple-error :format-control "primary WAL writer failed: ~s"
                                                 :format-arguments (list failure)))))))
  (check-primary-storage-startup-context gate-owner ownership)
  (unless (eql system-id (logical-slot-copy-check-identity gate-owner ownership))
    (error "logical slot copy source identity changed"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (validate-primary-wal-writer-storage-under-lock (primary-wal-writer-epoch writer)))
  (values))

(defun logical-slot-copy-restore-writer (writer system-id gate-owner ownership primary)
  (sb-sys:without-interrupts
    (let ((completed nil) (failure nil))
      (flet ((remember (condition) (unless failure (setf failure condition))))
        (unwind-protect
             (handler-bind ((error #'remember) (sb-ext:timeout #'remember))
               (unless (eq writer *primary-wal-writer*)
                 (error "logical slot copy replaced its paused writer"))
               (stop-primary-wal-writer)
               (logical-slot-copy-check-paused-writer writer system-id gate-owner ownership)
               (start-primary-wal-writer)
               (setf completed t))
          (unless completed
            (let ((first (or primary failure
                             (make-condition 'simple-error
                                             :format-control "logical slot copy writer restoration exited before completion"))))
              (sb-thread:with-mutex (*primary-wal-writer-lock*)
                (unless *logical-slot-copy-restoration-failure*
                  (setf *logical-slot-copy-restoration-failure*
                        (list :primary first :restoration failure))))
              (fence-live-operation-shutdown)
              (wal-fail-stop
               (or (car *wal-fail-stop-cause-cell*)
                   (and *backend* (backend-wal-failure *backend*))
                   (sb-thread:with-mutex (*primary-wal-writer-lock*)
                     (let ((writer-failure (primary-wal-writer-failure writer)))
                       (and (typep writer-failure 'condition) writer-failure)))
                   failure first))))))))
  (values))

(defun call-with-logical-slot-copy-writer-paused (function)
  (check-primary-wal-writer-join-context)
  (call-with-live-operation-startup-quiescence
   (lambda (gate-owner)
     (call-with-postmaster-ownership
      (lambda (ownership)
        (multiple-value-bind (writer system-id)
            (logical-slot-copy-source-preflight gate-owner ownership)
          (let ((primary nil)
                (*wal-fail-stop-cause-cell* (or *wal-fail-stop-cause-cell* (list nil))))
            (flet ((remember (condition) (unless primary (setf primary condition))))
              (sb-sys:without-interrupts
                (unwind-protect
                     (sb-sys:with-local-interrupts
                       (handler-bind ((error #'remember) (sb-ext:timeout #'remember))
                         (stop-primary-wal-writer)
                         (logical-slot-copy-check-paused-writer writer system-id gate-owner ownership)
                         (multiple-value-prog1 (funcall function gate-owner ownership)
                           (logical-slot-copy-check-paused-writer writer system-id gate-owner ownership))))
                  (logical-slot-copy-restore-writer writer system-id gate-owner ownership primary)))))))))))

(defun logical-slot-copy-open-pin-directory (root &key create)
  (when (and create
             (not (zerop (physical-image-mkdirat root "pg_physical_pins" #o700)))
             (/= sb-posix:eexist (sb-alien:get-errno)))
    (error "cannot prepare logical slot copy pin directory"))
  (let ((fd nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       root "pg_physical_pins"
                       (logior sb-posix:o-rdonly sb-posix:o-directory
                               sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open logical slot copy pin directory"))
             (let ((held (physical-image-check-directory fd (sb-posix:stat-dev (sb-posix:fstat root))))
                   (named (physical-image-file-facts root "pg_physical_pins")))
               (unless (and (= (sb-posix:stat-dev held) (first named))
                            (= (sb-posix:stat-ino held) (second named)))
                 (error "logical slot copy pin directory changed")))
             (when create (sb-posix:fsync fd) (sb-posix:fsync root))
             (prog1 fd (setf fd nil)))
        (when fd (sb-posix:close fd))))))

(defun call-with-logical-slot-copy-source-directory (gate-owner ownership function &key prepare-pins)
  (check-primary-storage-startup-context gate-owner ownership)
  (let ((fd nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-backup-open-directory (postmaster-ownership-directory ownership)))
             (let ((stat (physical-image-check-directory fd)))
               (unless (and (= (postmaster-ownership-device ownership) (sb-posix:stat-dev stat))
                            (= (postmaster-ownership-inode ownership) (sb-posix:stat-ino stat)))
                 (error "logical slot copy source directory changed")))
             (when prepare-pins
               (sb-posix:close (logical-slot-copy-open-pin-directory fd :create t)))
             (sb-sys:with-local-interrupts
               (call-with-physical-backup-image-directory
                fd
                (lambda (root)
                  (when prepare-pins
                    (sb-sys:without-interrupts
                      (push (cons "pg_physical_pins"
                                  (logical-slot-copy-open-pin-directory
                                   (physical-image-context-root-fd *physical-image-context*)))
                            (physical-image-context-children *physical-image-context*))))
                  (let ((*pgdata* root))
                    (check-primary-storage-startup-context gate-owner ownership)
                    (multiple-value-prog1 (funcall function *physical-image-context*)
                      (check-primary-storage-startup-context gate-owner ownership)))))))
        (when fd (sb-posix:close fd))))))
