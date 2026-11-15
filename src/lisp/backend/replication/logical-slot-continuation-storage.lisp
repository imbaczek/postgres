(in-package #:pg)

(defstruct (logical-slot-continuation-startup-storage
            (:constructor %make-logical-slot-continuation-startup-storage)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

(defvar *logical-slot-continuation-startup-storages* nil)

(defvar *logical-slot-continuation-storage-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defun logical-slot-continuation-storage-call (storage operation &optional argument)
  (let ((state (and (logical-slot-continuation-startup-storage-p storage)
                    (gethash storage *logical-slot-continuation-storage-identities*))))
    (unless state (error "continuation storage has no registered startup publisher"))
    (funcall state storage operation argument)))

(defun logical-slot-continuation-storage-check (storage)
  (logical-slot-continuation-storage-call storage :check))

(defun logical-slot-continuation-prepare-startup-directory (gate ownership)
  ;; Under the startup gate and postmaster ownership, create the fixed 0700
  ;; child and sync it and PGDATA before capturing the host owner and proof
  ;; binder.  Existing entries are checked and preserved; contents are not
  ;; classified or removed here.
  (declare (notinline sb-posix:close))
  (check-primary-storage-startup-context gate ownership)
  (let ((root nil) (complete nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf root (physical-backup-open-directory (postmaster-ownership-directory ownership)))
             (let ((facts (physical-image-file-facts root)))
               (unless (and (= (first facts) (postmaster-ownership-device ownership))
                            (= (second facts) (postmaster-ownership-inode ownership))
                            (equal facts (physical-image-file-facts
                                          -100 (namestring (postmaster-ownership-directory ownership)))))
                 (error "continuation startup directory differs from owned PGDATA")))
             (check-primary-storage-startup-context gate ownership)
             (unless (zerop (physical-image-mkdirat root "pg_logical_continuation" #o700))
               (unless (= sb-posix:eexist (sb-alien:get-errno))
                 (error "cannot create continuation directory: errno ~d" (sb-alien:get-errno))))
             (sb-sys:with-local-interrupts
               (call-with-logical-slot-copy-source-proof-fd
                root "pg_logical_continuation" t
                (lambda (directory facts)
                  (declare (ignore facts))
                  (check-primary-storage-startup-context gate ownership)
                  (sb-posix:fsync directory)
                  (sb-posix:fsync root)
                  (check-primary-storage-startup-context gate ownership))
                :mutable-directory t))
             (setf complete t))
        (when root
          (handler-case (sb-posix:close root)
            (error (condition)
              (if complete (error condition)
                  (report-query-temp-cleanup-failure "continuation root" condition)))))))))

(defun call-with-logical-slot-continuation-wal-directory (root function)
  (call-with-logical-slot-copy-source-proof-fd
   root "pg_wal" t
   (lambda (directory facts)
     (let ((active t) (thread sb-thread:*current-thread*))
       (flet ((check ()
                (unless (and active (eq thread sb-thread:*current-thread*)
                             (equal facts (physical-image-file-facts directory))
                             (equal facts (physical-image-file-facts root "pg_wal")))
                  (error "continuation WAL directory lost its live named binding"))))
         (unwind-protect
              (progn
                (check)
                (multiple-value-prog1 (funcall function directory #'check) (check)))
           (sb-sys:without-interrupts (setf active nil))))))))

(defun call-with-logical-slot-continuation-startup-storage-context
    (owner context function)
  ;; Weak EQ identity dispatch and dynamic registration limit this capability
  ;; to its active thread and checked host, proof, context and named child.
  ;; A failed operation or nonlocal exit during it, including an interrupt,
  ;; makes failure sticky. :WITH-ORIGIN includes its callback in the operation.
  ;; Copied objects and foreign-thread calls are rejected before that state
  ;; changes. Cleanup revokes registration; the proof scope closes the held FD.
  ;;
  ;; :DIRECTORY returns a borrowed FD for this scope; :BINDING returns copied
  ;; canonical proof bytes.  :HOST returns a detached actual control image,
  ;; system ID, history and end.  :WITH-ORIGIN runs its callback inside the
  ;; actual origin proof and root-reader lease.  :FACTS rechecks and decodes
  ;; the canonical proof.  :WITH-LOCAL-WAL holds the named pg_wal directory
  ;; for its callback and supplies a directory binding check.
  (unless (logical-slot-imported-host-startup-owner-p owner)
    (error "continuation startup storage requires a typed host startup owner"))
  (call-with-logical-slot-imported-proof-files
   owner context
   (lambda (facts check under-lock)
     (let ((root (physical-image-context-root-fd context)))
       (call-with-logical-slot-copy-source-proof-fd
        root "pg_logical_continuation" t
        (lambda (directory directory-facts)
          (let ((storage nil) (active t) (failed nil) (wal-check nil)
                              (thread sb-thread:*current-thread*)
                              (identity (logical-slot-copy-source-proof-directory-identity directory-facts)))
            (labels ((state (candidate operation &optional argument)
                       (unless (and (eq storage candidate) (eq thread sb-thread:*current-thread*))
                         (error "continuation startup publisher changed identity or thread"))
                       (unless (and active (not failed))
                         (error "continuation startup publisher ended or failed"))
                       (let ((complete nil))
                         (sb-sys:without-interrupts
                           (unwind-protect
                                (multiple-value-prog1
                                    (sb-sys:with-local-interrupts
                                      (unless (member storage *logical-slot-continuation-startup-storages* :test #'eq)
                                        (error "continuation startup publisher lost its registration"))
                                      (if (eq operation :check-under-lock)
                                          (funcall under-lock)
                                          (funcall check))
                                      (unless (and
                                               (equal identity
                                                      (logical-slot-copy-source-proof-directory-identity
                                                       (physical-image-file-facts directory)))
                                               (equal identity
                                                      (logical-slot-copy-source-proof-directory-identity
                                                       (physical-image-file-facts root "pg_logical_continuation"))))
                                        (error "continuation startup publisher lost its named directory"))
                                      (when wal-check (funcall wal-check))
                                      (ecase operation
                                        (:check t)
                                        (:check-under-lock t)
                                        (:with-local-wal
                                            (unless (functionp argument)
                                              (error "continuation local WAL requires a callback"))
                                          (call-with-logical-slot-continuation-wal-directory
                                           root
                                           (lambda (directory directory-check)
                                             (let ((prior wal-check))
                                               (unwind-protect
                                                    (progn
                                                      (setf wal-check (lambda () (when prior (funcall prior))
                                                                        (funcall directory-check)))
                                                      (funcall argument directory directory-check))
                                                 (sb-sys:without-interrupts (setf wal-check prior)))))))
                                        (:with-sidecar
                                            (unless (functionp argument)
                                              (error "continuation sidecar requires a callback"))
                                          (call-with-logical-slot-continuation-sidecar
                                           owner context
                                           (logical-slot-copy-check-source-proof (first (funcall check t)))
                                           (lambda () (state storage :check-under-lock))
                                           (lambda (reader directory-check file-check)
                                             (let ((prior wal-check))
                                               (unwind-protect
                                                    (progn
                                                      (setf wal-check (lambda () (when prior (funcall prior))
                                                                        (funcall directory-check)))
                                                      (funcall argument reader file-check))
                                                 (sb-sys:without-interrupts (setf wal-check prior)))))))
                                        (:directory directory)
                                        (:binding (first (funcall check t)))
                                        (:facts (logical-slot-copy-check-source-proof (first (funcall check t))))
                                        (:host
                                         (sb-thread:with-mutex (*wal-lock*)
                                           (logical-slot-continuation-copy
                                            (logical-slot-imported-host-startup-image))))
                                        (:with-origin
                                            (unless (functionp argument)
                                              (error "continuation origin proof requires a callback"))
                                          (call-with-logical-slot-imported-host-origin
                                           owner context argument))))
                                  (setf complete t))
                             (unless complete (setf failed t)))))))
              (setf storage (%make-logical-slot-continuation-startup-storage :state #'state))
              (let ((*logical-slot-continuation-startup-storages*
                      (cons storage *logical-slot-continuation-startup-storages*)))
                (sb-sys:without-interrupts
                  (unwind-protect
                       (progn
                         (setf (gethash storage *logical-slot-continuation-storage-identities*) #'state)
                         (sb-sys:with-local-interrupts
                           (logical-slot-continuation-storage-check storage)
                           (multiple-value-prog1 (funcall function storage facts)
                             (logical-slot-continuation-storage-check storage))))
                    (setf active nil)
                    (remhash storage *logical-slot-continuation-storage-identities*)))))))
        :mutable-directory t)))))

(defun call-with-logical-slot-continuation-startup-storage (gate ownership function)
  ;; Current control is observed read-only. The origin operation restores
  ;; and holds the independent origin proof; continuation and admission
  ;; still require their own checks.
  (logical-slot-continuation-prepare-startup-directory gate ownership)
  (call-with-logical-slot-imported-host-startup-owner
   gate ownership
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (root path)
        (declare (ignore path))
        (call-with-physical-image-owned-context
         root t
         (lambda (context)
           (call-with-logical-slot-continuation-startup-storage-context
            owner context function))))))))
