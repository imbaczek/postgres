(in-package #:pg)

(defstruct (logical-slot-copy-import-publication-owner
            (:include logical-slot-copy-import-owner)
            (:constructor %make-logical-slot-copy-import-publication-owner)
            (:print-function physical-libpq-print-private-object))
  "Typed phase 3 publication owner, subject to the base import exclusions.
Saved evidence facts are required and rechecked before certificate publication."
  pending-fd
  (pending-name "logical-slot-copy.import.tmp")
  evidence-ready)

(defmethod physical-adoption-check-owner-role
    ((owner logical-slot-copy-import-publication-owner))
  (call-next-method)
  (unless (= 3 (logical-slot-copy-import-owner-phase owner))
    (error "pending import publication requires phase 3"))
  owner)

(defmethod physical-adoption-owner-extra-root-names
    ((owner logical-slot-copy-import-publication-owner))
  (append (call-next-method)
          (loop for name in '("logical-slot-copy.import" "logical-slot-copy.import.tmp"
                              "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp")
                when (slot-state-lstat
                      (merge-pathnames name (physical-adoption-owner-directory owner)))
                  collect name)))

(defun logical-slot-copy-import-file-spec (kind)
  "Return fixed names, bound and decoder for an import artifact kind."
  (ecase kind
    (:pending (values "logical-slot-copy.import" "logical-slot-copy.import.tmp"
                      +logical-slot-copy-import-maximum-bytes+ #'logical-slot-copy-import-decode))
    (:evidence (values "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp"
                       +logical-slot-copy-evidence-maximum-bytes+
                       #'logical-slot-copy-import-evidence-decode))))

(defun logical-slot-copy-import-read-artifact (context kind)
  "Read a private regular artifact and its file facts under caller-owned CONTEXT."
  (multiple-value-bind (name temporary maximum decode) (logical-slot-copy-import-file-spec kind)
    (declare (ignore temporary))
    (when (slot-state-lstat (merge-pathnames name (physical-image-context-root-path context)))
      (multiple-value-bind (bytes facts) (logical-slot-copy-read-file context name maximum)
        (unless (and (sb-posix:s-isreg (third facts)) (= 1 (fourth facts))
                     (= (sb-posix:geteuid) (fifth facts))
                     (= #o600 (logand #o7777 (third facts))))
          (error "logical import metadata is not a private regular file"))
        (values (funcall decode bytes) bytes facts)))))

(defun logical-slot-copy-import-file-test-point (kind point context)
  "Select the publication test point for KIND and POINT."
  (logical-slot-copy-verification-test-point
   (or (cdr (assoc point
                   (ecase kind
                     (:pending '((:before-file-fsync . :before-import-pending-file-fsync)
                                 (:after-file-fsync . :after-import-pending-file-fsync)
                                 (:before-rename . :before-import-pending-rename)
                                 (:after-rename . :after-import-pending-rename)
                                 (:before-root-fsync . :before-import-pending-root-fsync)
                                 (:after-root-fsync . :after-import-pending-root-fsync)))
                     (:evidence '((:before-file-fsync . :before-import-evidence-file-fsync)
                                  (:after-file-fsync . :after-import-evidence-file-fsync)
                                  (:before-rename . :before-import-evidence-rename)
                                  (:after-rename . :after-import-evidence-rename)
                                  (:before-root-fsync . :before-import-evidence-root-fsync)
                                  (:after-root-fsync . :after-import-evidence-root-fsync))))))
       (error "unknown logical import publication point"))
   context))

(defun logical-slot-copy-import-check-evidence-ready (owner context attempt)
  "Require the synced evidence bytes and file facts captured by this owner.
Rewrites are rejected even when the bytes are unchanged."
  (multiple-value-bind (value bytes facts) (logical-slot-copy-import-read-artifact context :evidence)
    (declare (ignore value))
    (unless (and (logical-slot-copy-import-publication-owner-evidence-ready owner)
                 (equal facts (logical-slot-copy-import-publication-owner-evidence-ready owner))
                 (equalp bytes (logical-slot-copy-import-pending-plan-evidence attempt)))
      (error "pending import requires its synced unchanged evidence"))))

(defun logical-slot-copy-import-read-certificate (context)
  (logical-slot-copy-import-read-artifact context :pending))

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
        (root (physical-image-context-root-fd context)))
    (dolist (temporary '("logical-slot-copy.import.tmp" "logical-slot-copy.evidence.tmp"))
      (when (and (slot-state-lstat
                  (merge-pathnames temporary (physical-image-context-root-path context)))
                 (or (null fd) (not (equal name temporary))))
        (error (if (equal temporary "logical-slot-copy.import.tmp")
                   "unexpected pending import temporary file"
                   "unexpected import evidence temporary file"))))
    (when fd
      (unless (and (logical-slot-copy-import-private-fd-p fd)
                   (equal (physical-image-file-facts fd) (physical-image-file-facts root name)))
        (error "logical import publication inode changed"))))
  t)

(defun logical-slot-copy-import-check-publication-plan (owner context attempt store tuple kind)
  "Recheck source and evidence binding, receive tuple and owner before publication.
Certificate writes also require the pinned evidence bytes and file facts."
  (logical-slot-copy-check-import-publication owner context)
  (logical-slot-copy-import-check-pending-plan owner context attempt)
  (logical-slot-copy-import-evidence-check-certificate
   (logical-slot-copy-import-evidence-decode (logical-slot-copy-import-pending-plan-evidence attempt))
   (logical-slot-copy-import-pending-plan-value attempt)
   (logical-slot-copy-import-pending-plan-descriptor attempt)
   (logical-slot-copy-import-owner-existing-proof owner))
  (unless (equal tuple (physical-store-control-tuple store))
    (error "logical import receive tuple changed before publication"))
  (physical-store-read-retained-control store tuple)
  (ecase kind
    (:evidence nil)
    (:pending (logical-slot-copy-import-check-evidence-ready owner context attempt)))
  t)

(defun logical-slot-copy-import-check-file-bytes (context kind bytes)
  "Require the named artifact to retain the expected bytes."
  (let ((actual (nth-value 1 (logical-slot-copy-import-read-artifact context kind))))
    (unless (equalp bytes actual)
      (error "logical import metadata changed during publication"))))

(defun logical-slot-copy-import-sync-file (owner context attempt store tuple kind bytes)
  "Recheck and sync the artifact and root, then read back bytes and file identity."
  (let ((root (physical-image-context-root-fd context))
        (name (logical-slot-copy-import-file-spec kind)))
    (logical-slot-copy-import-check-publication-plan owner context attempt store tuple kind)
    (logical-slot-copy-import-check-file-bytes context kind bytes)
    (call-with-physical-image-input-at
     root name
     (lambda (stream)
       (let ((fd (sb-sys:fd-stream-fd stream)))
         (unless (and (logical-slot-copy-import-private-fd-p fd)
                      (equal (physical-image-file-facts fd) (physical-image-file-facts root name)))
           (error "logical import metadata changed before sync"))
         (sb-posix:fsync fd)
         (logical-slot-copy-import-file-test-point kind :before-root-fsync context)
         (logical-slot-copy-import-check-publication-plan owner context attempt store tuple kind)
         (sb-posix:fsync root)
         (logical-slot-copy-import-file-test-point kind :after-root-fsync context)
         (logical-slot-copy-import-check-file-bytes context kind bytes)
         (unless (equal (physical-image-file-facts fd) (physical-image-file-facts root name))
           (error "logical import metadata failed final inode check"))))
     :element-type '(unsigned-byte 8))
    (physical-slot-check-standby-receive
     store (logical-slot-copy-import-pending-plan-state attempt)
     (getf (logical-slot-copy-import-pending-plan-value attempt) :fork))
    (logical-slot-copy-import-check-publication-plan owner context attempt store tuple kind)
    bytes))

(defun logical-slot-copy-import-write-file (owner context attempt store tuple kind existing)
  "Publish or resync an artifact under the owned scope with exclusive rename.
Check the plan around file and root fsync and readback; stale temporaries fail."
  (let* ((bytes (ecase kind
                  (:pending (logical-slot-copy-import-image
                             (logical-slot-copy-import-pending-plan-value attempt)))
                  (:evidence (logical-slot-copy-import-pending-plan-evidence attempt))))
         (root (physical-image-context-root-fd context))
         (fd nil) (stream nil))
    (multiple-value-bind (name temporary) (logical-slot-copy-import-file-spec kind)
      (logical-slot-copy-import-check-publication-plan owner context attempt store tuple kind)
      (when existing
        (unless (equalp bytes existing) (error "logical import retry bytes differ"))
        (return-from logical-slot-copy-import-write-file
          (logical-slot-copy-import-sync-file owner context attempt store tuple kind bytes)))
      (when (logical-slot-copy-import-read-artifact context kind)
        (error "logical import metadata appeared before exclusive publication"))
      (sb-sys:without-interrupts
        (unwind-protect
             (progn
               (setf fd (physical-image-openat
                         root temporary (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                                sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
               (when (minusp fd) (setf fd nil) (error "cannot create logical import metadata"))
               (setf (logical-slot-copy-import-publication-owner-pending-fd owner) fd
                     (logical-slot-copy-import-publication-owner-pending-name owner) temporary
                     stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)))
               (sb-sys:with-local-interrupts
                 (write-sequence bytes stream)
                 (force-output stream)
                 (logical-slot-copy-import-file-test-point kind :before-file-fsync context)
                 (logical-slot-copy-import-check-publication-plan owner context attempt store tuple kind)
                 (sb-posix:fsync fd)
                 (logical-slot-copy-import-file-test-point kind :after-file-fsync context)
                 (logical-slot-copy-import-file-test-point kind :before-rename context)
                 (logical-slot-copy-import-check-publication-plan owner context attempt store tuple kind)
                 (when (logical-slot-copy-import-read-artifact context kind)
                   (error "logical import metadata appeared before rename"))
                 (sb-sys:without-interrupts
                   (unless (zerop (physical-adoption-renameat2
                                   root temporary root name +physical-adoption-rename-noreplace+))
                     (error "cannot publish exclusive logical import metadata"))
                   (setf (logical-slot-copy-import-publication-owner-pending-name owner) name))
                 (logical-slot-copy-import-file-test-point kind :after-rename context)
                 (logical-slot-copy-import-sync-file owner context attempt store tuple kind bytes)))
          (unwind-protect
               (if stream (close stream) (when fd (sb-posix:close fd)))
            (setf (logical-slot-copy-import-publication-owner-pending-fd owner) nil)))))))

(defun logical-slot-copy-publish-import-pending ()
  "Verify actual source proof and publish only a pending import certificate.
Sync and read back permanent evidence before writing the certificate. An existing
certificate without evidence is refused. This zero-argument entry grants no
admission and writes no PGSLOT7 or child WAL; ordinary fences remain in place."
  (call-with-logical-slot-copy-import-input-kind
   #'%make-logical-slot-copy-import-publication-owner
   (lambda (owner context)
     (logical-slot-copy-check-import-publication owner context)
     (multiple-value-bind (existing old-bytes) (logical-slot-copy-import-read-certificate context)
       (let* ((old-evidence (nth-value 1 (logical-slot-copy-import-read-artifact context :evidence)))
              (attempt (progn
                         (when (and existing (null old-evidence))
                           (error "pending import certificate has no permanent evidence"))
                         (logical-slot-copy-import-build-pending owner context existing)))
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
                 (logical-slot-copy-import-write-file
                  owner context attempt store tuple :evidence old-evidence)
                 (setf (logical-slot-copy-import-publication-owner-evidence-ready owner)
                       (nth-value 2 (logical-slot-copy-import-read-artifact context :evidence)))
                 (logical-slot-copy-import-decode
                  (logical-slot-copy-import-write-file
                   owner context attempt store tuple :pending old-bytes))))))))))))
