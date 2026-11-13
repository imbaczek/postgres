(in-package #:pg)

(defstruct (logical-slot-copy-import-publication-owner
            (:include logical-slot-copy-import-owner)
            (:constructor %make-logical-slot-copy-import-publication-owner)
            (:print-function physical-libpq-print-private-object))
  pending-fd
  (pending-name "logical-slot-copy.import.tmp"))

(defmethod physical-adoption-check-owner-role
    ((owner logical-slot-copy-import-publication-owner))
  (call-next-method)
  (unless (= 3 (logical-slot-copy-import-owner-phase owner))
    (error "pending import publication requires phase 3"))
  owner)

(defmethod physical-adoption-owner-extra-root-names
    ((owner logical-slot-copy-import-publication-owner))
  (append (call-next-method)
          (loop for name in '("logical-slot-copy.import" "logical-slot-copy.import.tmp")
                when (slot-state-lstat
                      (merge-pathnames name (physical-adoption-owner-directory owner)))
                  collect name)))

(defun logical-slot-copy-import-read-certificate (context)
  (when (slot-state-lstat
         (merge-pathnames "logical-slot-copy.import" (physical-image-context-root-path context)))
    (multiple-value-bind (bytes facts)
        (logical-slot-copy-read-file context "logical-slot-copy.import"
                                     +logical-slot-copy-import-maximum-bytes+)
      (values (logical-slot-copy-import-decode bytes) bytes facts))))

(defun logical-slot-copy-import-private-fd-p (fd)
  (let ((stat (sb-posix:fstat fd)))
    (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
         (= 1 (sb-posix:stat-nlink stat))
         (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
         (= #o600 (logand #o7777 (sb-posix:stat-mode stat))))))

(defun logical-slot-copy-check-import-publication (owner context)
  (unless (logical-slot-copy-import-publication-owner-p owner)
    (error "pending import write requires its publication owner"))
  (logical-slot-copy-check-input owner context)
  (let ((fd (logical-slot-copy-import-publication-owner-pending-fd owner))
        (name (logical-slot-copy-import-publication-owner-pending-name owner))
        (root (physical-image-context-root-fd context))
        (temporary (slot-state-lstat
                    (merge-pathnames "logical-slot-copy.import.tmp"
                                     (physical-image-context-root-path context)))))
    (when (and temporary (or (null fd) (not (equal name "logical-slot-copy.import.tmp"))))
      (error "unexpected pending import temporary file"))
    (when fd
      (unless (and (logical-slot-copy-import-private-fd-p fd)
                   (equal (physical-image-file-facts fd) (physical-image-file-facts root name)))
        (error "pending import publication inode changed"))))
  t)

(defun logical-slot-copy-import-check-publication-plan (owner context attempt store tuple)
  (logical-slot-copy-check-import-publication owner context)
  (logical-slot-copy-import-check-pending-plan owner context attempt)
  (unless (equal tuple (physical-store-control-tuple store))
    (error "logical import receive tuple changed before publication"))
  (physical-store-read-retained-control store tuple)
  t)

(defun logical-slot-copy-import-check-certificate-bytes (context bytes)
  (let ((actual (nth-value 1 (logical-slot-copy-import-read-certificate context))))
    (unless (equalp bytes actual)
      (error "pending import certificate changed during publication"))))

(defun logical-slot-copy-import-sync-pending (owner context attempt store tuple bytes)
  (let ((root (physical-image-context-root-fd context)))
    (logical-slot-copy-import-check-publication-plan owner context attempt store tuple)
    (logical-slot-copy-import-check-certificate-bytes context bytes)
    (call-with-physical-image-input-at
     root "logical-slot-copy.import"
     (lambda (stream)
       (let ((fd (sb-sys:fd-stream-fd stream)))
         (unless (equal (physical-image-file-facts fd)
                        (physical-image-file-facts root "logical-slot-copy.import"))
           (error "pending import certificate changed before sync"))
         (sb-posix:fsync fd)
         (logical-slot-copy-verification-test-point :before-import-pending-root-fsync context)
         (logical-slot-copy-import-check-publication-plan owner context attempt store tuple)
         (sb-posix:fsync root)
         (logical-slot-copy-verification-test-point :after-import-pending-root-fsync context)
         (logical-slot-copy-import-check-certificate-bytes context bytes)
         (unless (equal (physical-image-file-facts fd)
                        (physical-image-file-facts root "logical-slot-copy.import"))
           (error "pending import certificate failed final inode check"))))
     :element-type '(unsigned-byte 8))
    (physical-slot-check-standby-receive
     store (logical-slot-copy-import-pending-plan-state attempt)
     (getf (logical-slot-copy-import-pending-plan-value attempt) :fork))
    (logical-slot-copy-import-check-publication-plan owner context attempt store tuple)
    bytes))

(defun logical-slot-copy-import-write-pending (owner context attempt store tuple existing)
  (let* ((bytes (logical-slot-copy-import-image
                 (logical-slot-copy-import-pending-plan-value attempt)))
         (root (physical-image-context-root-fd context))
         (pending "logical-slot-copy.import.tmp") (name "logical-slot-copy.import")
         (fd nil) (stream nil))
    (logical-slot-copy-import-check-publication-plan owner context attempt store tuple)
    (when existing
      (unless (equalp bytes existing) (error "pending import retry bytes differ"))
      (return-from logical-slot-copy-import-write-pending
        (logical-slot-copy-import-sync-pending owner context attempt store tuple bytes)))
    (when (logical-slot-copy-import-read-certificate context)
      (error "pending import certificate appeared before exclusive publication"))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       root pending (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                            sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
             (when (minusp fd) (setf fd nil) (error "cannot create pending import certificate"))
             (setf (logical-slot-copy-import-publication-owner-pending-fd owner) fd
                   stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)))
             (sb-sys:with-local-interrupts
               (write-sequence bytes stream)
               (force-output stream)
               (logical-slot-copy-verification-test-point :before-import-pending-file-fsync context)
               (logical-slot-copy-import-check-publication-plan owner context attempt store tuple)
               (sb-posix:fsync fd)
               (logical-slot-copy-verification-test-point :after-import-pending-file-fsync context)
               (logical-slot-copy-verification-test-point :before-import-pending-rename context)
               (logical-slot-copy-import-check-publication-plan owner context attempt store tuple)
               (when (logical-slot-copy-import-read-certificate context)
                 (error "pending import certificate appeared before rename"))
               (sb-sys:without-interrupts
                 (unless (zerop (physical-adoption-renameat2
                                 root pending root name +physical-adoption-rename-noreplace+))
                   (error "cannot publish exclusive pending import certificate"))
                 (setf (logical-slot-copy-import-publication-owner-pending-name owner) name))
               (logical-slot-copy-verification-test-point :after-import-pending-rename context)
               (logical-slot-copy-import-sync-pending owner context attempt store tuple bytes)))
        (unwind-protect
             (if stream (close stream) (when fd (sb-posix:close fd)))
          (setf (logical-slot-copy-import-publication-owner-pending-fd owner) nil))))))

(defun logical-slot-copy-publish-import-pending ()
  (call-with-logical-slot-copy-import-input-kind
   #'%make-logical-slot-copy-import-publication-owner
   (lambda (owner context)
     (logical-slot-copy-check-import-publication owner context)
     (multiple-value-bind (existing old-bytes) (logical-slot-copy-import-read-certificate context)
       (let* ((attempt (logical-slot-copy-import-build-pending owner context existing))
              (state (logical-slot-copy-import-pending-plan-state attempt))
              (*pgdata* (physical-image-context-root-path context))
              (*physical-image-context* nil))
         (call-with-logical-slot-copy-import-store
          owner state
          (lambda (store)
            (physical-follow-call-with-receive-root
             store
             (lambda ()
               (let ((tuple (physical-store-control-tuple store)))
                 (physical-slot-check-standby-receive
                  store state (getf (logical-slot-copy-import-pending-plan-value attempt) :fork))
                 (logical-slot-copy-import-decode
                  (logical-slot-copy-import-write-pending
                   owner context attempt store tuple old-bytes))))))))))))
