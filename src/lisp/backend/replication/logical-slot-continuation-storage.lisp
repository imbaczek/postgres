(in-package #:pg)

(defstruct (logical-slot-continuation-startup-storage
            (:constructor %make-logical-slot-continuation-startup-storage)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

(defvar *logical-slot-continuation-startup-storages* nil)

(defvar *logical-slot-continuation-storage-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defun logical-slot-continuation-storage-call (storage operation)
  (let ((state (and (logical-slot-continuation-startup-storage-p storage)
                    (gethash storage *logical-slot-continuation-storage-identities*))))
    (unless state (error "continuation storage has no registered startup publisher"))
    (funcall state storage operation)))

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

(defun call-with-logical-slot-continuation-startup-storage-context
    (owner context function)
  ;; Weak EQ identity dispatch and dynamic registration limit this capability
  ;; to its active thread and checked host, proof, context and named child.
  ;; A failed check or nonlocal exit during a check makes failure sticky,
  ;; including an interrupt.  Copied or foreign-thread objects are rejected
  ;; before that state is changed.  Cleanup revokes registration and the source-proof
  ;; scope closes the held FD.
  ;;
  ;; :DIRECTORY returns a borrowed FD for this scope; :BINDING returns copied
  ;; canonical proof bytes.
  (unless (logical-slot-imported-host-startup-owner-p owner)
    (error "continuation startup storage requires a typed host startup owner"))
  (call-with-logical-slot-imported-proof-files
   owner context
   (lambda (facts check under-lock)
     (declare (ignore under-lock))
     (let ((root (physical-image-context-root-fd context)))
       (call-with-logical-slot-copy-source-proof-fd
        root "pg_logical_continuation" t
        (lambda (directory directory-facts)
          (let ((storage nil) (active t) (failed nil)
                              (thread sb-thread:*current-thread*)
                              (identity (logical-slot-copy-source-proof-directory-identity directory-facts)))
            (labels ((state (candidate operation)
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
                                      (funcall check)
                                      (unless (and
                                               (equal identity
                                                      (logical-slot-copy-source-proof-directory-identity
                                                       (physical-image-file-facts directory)))
                                               (equal identity
                                                      (logical-slot-copy-source-proof-directory-identity
                                                       (physical-image-file-facts root "pg_logical_continuation"))))
                                        (error "continuation startup publisher lost its named directory"))
                                      (ecase operation
                                        (:check t)
                                        (:directory directory)
                                        (:binding (first (funcall check t)))))
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
  ;; This is a storage capability for future selector I/O.  It does not
  ;; publish a snapshot, selector or control transition, lease or restore
  ;; roots, reserve a tail, prove records, admit work, run GC, or establish
  ;; readiness.  Current control is observed read-only; no offline adoption
  ;; authority is granted.
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
