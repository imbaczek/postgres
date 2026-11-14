(in-package #:pg)

(defun logical-slot-copy-source-proof-check-file (context name &key expected absent)
  (unless (member name '("logical-slot-copy.source-proof" "logical-slot-copy.source-proof.tmp")
                  :test #'equal)
    (error "source proof publication requires a fixed name"))
  (let* ((root (physical-image-context-root-fd context))
         (present (slot-state-lstat (slot-state-fd-path root name))))
    (when absent
      (when present (error "source proof publication expected an absent name"))
      (return-from logical-slot-copy-source-proof-check-file nil))
    (unless present (error "source proof publication lost a fixed file"))
    (let ((facts (physical-image-file-facts root name)))
      (unless (and (logical-slot-copy-import-control-private-facts-p
                    facts (physical-image-file-facts root))
                   (<= 0 (sixth facts) (logical-slot-copy-source-proof-maximum-bytes))
                   (or (null expected) (equal expected facts)))
        (error "source proof publication file binding changed"))
      facts)))

(defun logical-slot-copy-source-proof-check-open-file (context name fd identity)
  ;; Match the held descriptor to the fixed name and its device, inode and mount.
  (let ((held (physical-image-file-facts fd)))
    (unless (and (equal identity (logical-slot-copy-install-file-identity held))
                 (equal held (logical-slot-copy-source-proof-check-file context name)))
      (error "source proof publication lost an open file binding"))
    held))

(defun logical-slot-copy-source-proof-check-image-fd (owner context input attempt fd)
  (declare (notinline sb-posix:read))
  (logical-slot-copy-source-proof-check-preparation owner context input attempt :source nil)
  (let* ((record (logical-slot-copy-source-proof-preparation-record owner context input attempt))
         (expected (second record))
         (bytes (logical-slot-copy-import-control-read-fd fd (length expected)))
         (eof (make-array 1 :element-type '(unsigned-byte 8))))
    (unless (equalp expected bytes) (error "source proof publication bytes differ from its seal"))
    (sb-sys:with-pinned-objects (eof)
      (unless (zerop (sb-posix:read fd (sb-sys:vector-sap eof) 1))
        (error "source proof publication has trailing bytes")))
    (physical-image-file-facts fd)))

(defun logical-slot-copy-source-proof-read-canonical (owner context input attempt &optional expected)
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (let* ((root (physical-image-context-root-fd context))
            (name "logical-slot-copy.source-proof"))
       (if (slot-state-lstat (slot-state-fd-path root name))
           (call-with-logical-slot-copy-source-proof-fd
            root name nil
            (lambda (fd facts)
              (when (and expected (not (equal expected facts)))
                (error "canonical source proof inode changed"))
              (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)))
           (when expected (error "canonical source proof disappeared")))))))

(defun logical-slot-copy-source-proof-remove-temporary (owner context input attempt canonical)
  ;; Check the canonical file first.  Discard a private orphan of size zero
  ;; through the maximum, without adopting its contents; sync the root after
  ;; unlinking it.
  (declare (notinline sb-posix:fsync))
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (let* ((root (physical-image-context-root-fd context))
            (name "logical-slot-copy.source-proof.tmp"))
       (if canonical
           (logical-slot-copy-source-proof-read-canonical owner context input attempt canonical)
           (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof" :absent t))
       (when (slot-state-lstat (slot-state-fd-path root name))
         (let ((facts (call-with-logical-slot-copy-source-proof-fd
                       root name nil
                       (lambda (fd facts)
                         (declare (ignore fd))
                         (unless (<= 0 (sixth facts) (logical-slot-copy-source-proof-maximum-bytes))
                           (error "orphan source proof temporary is oversized"))
                         facts))))
           (logical-slot-copy-source-proof-publication-point
            owner context input attempt :before-source-proof-orphan-unlink)
           (logical-slot-copy-source-proof-check-file context name :expected facts)
           (if canonical
               (logical-slot-copy-source-proof-read-canonical owner context input attempt canonical)
               (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof" :absent t))
           (unless (zerop (logical-slot-copy-unlinkat root name 0))
             (error "cannot remove reserved source proof temporary"))
           (logical-slot-copy-source-proof-check-file context name :absent t)
           (sb-posix:fsync root)
           (logical-slot-copy-source-proof-publication-point
            owner context input attempt :after-source-proof-orphan-unlink)
           t))))
   :source t))

(defun logical-slot-copy-source-proof-write-temporary (owner context input attempt)
  ;; Create a fresh exclusive temporary.  Pop ownership before close and never
  ;; close or retry a descriptor whose identity has been replaced.
  (declare (notinline sb-posix:write sb-posix:fsync sb-posix:close))
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (let* ((root (physical-image-context-root-fd context))
            (name "logical-slot-copy.source-proof.tmp")
            (image (second (logical-slot-copy-source-proof-preparation-record owner context input attempt)))
            (fd nil) (identity nil) (complete nil))
       (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof" :absent t)
       (logical-slot-copy-source-proof-check-file context name :absent t)
       (unwind-protect
            (progn
              (sb-sys:without-interrupts
                (setf fd (physical-image-openat
                          root name (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                            sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+)
                          #o600))
                (when (minusp fd) (setf fd nil) (error "cannot create source proof temporary")))
              (setf identity (logical-slot-copy-install-file-identity (physical-image-file-facts fd)))
              (logical-slot-copy-source-proof-check-open-file context name fd identity)
              (logical-slot-copy-source-proof-publication-point
               owner context input attempt :after-source-proof-temp-create)
              (sb-sys:with-pinned-objects (image)
                (loop with position = 0 while (< position (length image))
                      for count = (min 65536 (- (length image) position)
                                       (or *logical-slot-copy-source-proof-publication-write-limit* 65536))
                      do (unless (plusp count) (error "invalid source proof write bound"))
                         (logical-slot-copy-source-proof-check-open-file context name fd identity)
                         (let ((written (sb-posix:write
                                         fd (sb-sys:sap+ (sb-sys:vector-sap image) position) count)))
                           (unless (plusp written) (error "short source proof write"))
                           (incf position written))
                         (logical-slot-copy-source-proof-publication-point
                          owner context input attempt :after-source-proof-temp-write :source nil)))
              (logical-slot-copy-source-proof-check-open-file context name fd identity)
              (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)
              (logical-slot-copy-source-proof-check-open-file context name fd identity)
              (sb-posix:fsync fd)
              (logical-slot-copy-source-proof-publication-point
               owner context input attempt :after-source-proof-temp-sync)
              (logical-slot-copy-source-proof-check-open-file context name fd identity)
              (let ((facts (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)))
                (unless (and (equal identity (logical-slot-copy-install-file-identity facts))
                             (equal facts (logical-slot-copy-source-proof-check-file context name)))
                  (error "source proof temporary lost its final binding"))
                (setf complete t)
                facts))
         (when fd
           (let ((owned fd))
             (sb-sys:without-interrupts
               (setf fd nil)
               (handler-case
                   (progn
                     (when (and identity
                                (not (equal identity
                                            (logical-slot-copy-install-file-identity
                                             (physical-image-file-facts owned)))))
                       (error "source proof temporary descriptor was replaced"))
                     (sb-posix:close owned))
                 (error (condition)
                   (if complete (error condition)
                       (report-query-temp-cleanup-failure "source proof temporary" condition))))))))))
   :source t))

(defun logical-slot-copy-source-proof-install-temporary (owner context input attempt temporary)
  ;; Install without replacement, then bind and read back the installed inode.
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (let ((root (physical-image-context-root-fd context)))
       (logical-slot-copy-source-proof-publication-point
        owner context input attempt :before-source-proof-install)
       (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof" :absent t)
       (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof.tmp" :expected temporary)
       (unless (zerop (physical-adoption-renameat2
                       root "logical-slot-copy.source-proof.tmp"
                       root "logical-slot-copy.source-proof" +physical-adoption-rename-noreplace+))
         (error "cannot install source proof exclusively"))
       (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof.tmp" :absent t)
       (let ((facts (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof")))
         (unless (equal (logical-slot-copy-install-file-identity temporary)
                        (logical-slot-copy-install-file-identity facts))
           (error "source proof installation moved a different inode"))
         (logical-slot-copy-source-proof-publication-point
          owner context input attempt :after-source-proof-install)
         (logical-slot-copy-source-proof-read-canonical owner context input attempt facts))))
   :source t))

(defun logical-slot-copy-source-proof-sync-canonical (owner context input attempt canonical)
  ;; Read back and sync the installed file and its root before accepting it.
  (declare (notinline sb-posix:fsync))
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (let ((root (physical-image-context-root-fd context)))
       (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof.tmp" :absent t)
       (call-with-logical-slot-copy-source-proof-fd
        root "logical-slot-copy.source-proof" nil
        (lambda (fd facts)
          (unless (equal canonical facts) (error "canonical source proof changed before sync"))
          (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)
          (sb-posix:fsync fd)
          (logical-slot-copy-source-proof-publication-point
           owner context input attempt :after-source-proof-file-sync)
          (sb-posix:fsync root)
          (logical-slot-copy-source-proof-publication-point
           owner context input attempt :after-source-proof-root-sync)
          (logical-slot-copy-source-proof-check-open-file
           context "logical-slot-copy.source-proof" fd (logical-slot-copy-install-file-identity canonical))
          (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof.tmp" :absent t)
          (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)))))
   :source t))
