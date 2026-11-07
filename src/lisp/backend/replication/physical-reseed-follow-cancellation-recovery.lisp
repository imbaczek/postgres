(in-package #:pg)

(declaim (ftype function physical-reseed-follow-cancellation-owner-root))

(defstruct (physical-reseed-follow-cleaned-recovery
            (:constructor %make-physical-reseed-follow-cleaned-recovery)
            (:copier nil)
            (:print-function physical-libpq-print-private-object))
  (owner nil :read-only t) (thread nil :read-only t) (root nil :read-only t)
  (snapshot nil :read-only t) (bytes nil :read-only t))

(defun physical-reseed-follow-cleaned-recovery-snapshot-at (root-fd)
  (let* ((snapshot (physical-reseed-follow-cancellation-snapshot-at root-fd))
         (before (physical-reseed-follow-cancellation-preconfiguration-record
                  (getf (getf snapshot :follow) :record))))
    (unless (member (getf (getf before :cancellation) :phase) '(:pending :source-closed))
      (error "cleanup recovery requires its pending or source-closed predecessor"))
    (multiple-value-bind (identity row)
        (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp")
      (unless identity (error "cleanup recovery requires a complete temporary"))
      (multiple-value-bind (candidate actual)
          (physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")
        (let* ((group (getf candidate :cancellation))
               (plan (when (eq :cleaned (getf group :phase))
                       (physical-reseed-follow-cancellation-ack-plan before (getf group :receipt)))))
          (unless (and plan (equal identity actual)
                       (equal candidate (physical-reseed-follow-cancellation-cleaned-candidate before plan))
                       (equal row (nth-value 1 (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp"))))
            (error "cleanup recovery temporary differs from its exact acknowledged successor"))
          (physical-adoption-attempt-value-copy
           (append snapshot
                   (list :temporary
                         (list :record candidate :identity identity :row row
                               :mount (physical-adoption-install-mount-id nil :directory-fd root-fd))))))))))

(defun physical-reseed-follow-observe-cleaned-recovery (owner)
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "cleanup recovery capture requires active operation ownership"))
  (let ((root nil)
        (snapshot nil))
    (setf snapshot
          (call-with-physical-directory-root
           owner
           (lambda (root-fd directory)
             (declare (ignore directory))
             (let ((stat (sb-posix:fstat root-fd)))
               (setf root (list (copy-seq (namestring (physical-adoption-owner-directory owner)))
                                (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
                                (physical-adoption-install-mount-id nil :directory-fd root-fd))))
             (let ((current
                     (call-with-physical-reseed-follow-image
                      owner
                      (lambda (fd check)
                        (let ((value (physical-reseed-follow-cleaned-recovery-snapshot-at fd)))
                          (physical-reseed-follow-cancellation-local-proof
                           owner fd (getf (getf value :follow) :record))
                          (physical-reseed-follow-cancellation-local-proof
                           owner fd (getf (getf value :temporary) :record))
                          (physical-reseed-follow-read-at fd "physical-reseed.follow" :stabilize t)
                          (funcall check)
                          (unless (equal value (physical-reseed-follow-cleaned-recovery-snapshot-at fd))
                            (error "cleanup recovery image changed during capture"))
                          value)))))
               (physical-reseed-follow-check-snapshot root-fd (getf current :follow))
               (unless (equal current (physical-reseed-follow-cleaned-recovery-snapshot-at root-fd))
                 (error "cleanup recovery changed while releasing image resources"))
               current))))
    (unless (equal root (physical-reseed-follow-cancellation-owner-root owner))
      (error "cleanup recovery changed its owned directory"))
    (values snapshot root)))

(defun physical-reseed-follow-capture-cleaned-recovery (owner)
  (multiple-value-bind (snapshot root) (physical-reseed-follow-observe-cleaned-recovery owner)
    (%make-physical-reseed-follow-cleaned-recovery
     :owner owner :thread sb-thread:*current-thread*
     :root (physical-adoption-attempt-value-copy root)
     :snapshot (physical-adoption-attempt-value-copy snapshot)
     :bytes (physical-reseed-follow-image (getf (getf snapshot :temporary) :record)))))

(defun physical-reseed-follow-cleaned-recovery-network-snapshot (owner context &optional expected)
  (unless (and (physical-reseed-follow-cleaned-recovery-p context)
               (eq owner (physical-reseed-follow-cleaned-recovery-owner context))
               (eq sb-thread:*current-thread* (physical-reseed-follow-cleaned-recovery-thread context)))
    (error "cleanup recovery changed owner, thread or context"))
  (multiple-value-bind (snapshot root) (physical-reseed-follow-observe-cleaned-recovery owner)
    (unless (and (equal root (physical-reseed-follow-cleaned-recovery-root context))
                 (equal snapshot (physical-reseed-follow-cleaned-recovery-snapshot context))
                 (or (null expected) (equal expected snapshot))
                 (equalp (physical-reseed-follow-cleaned-recovery-bytes context)
                         (physical-reseed-follow-image (getf (getf snapshot :temporary) :record))))
      (error "cleanup recovery changed its captured predecessor or temporary"))
    (physical-adoption-attempt-value-copy snapshot)))

(defun physical-reseed-follow-check-cleaned-recovery-temporary-at (root-fd temporary candidate)
  (unless (and (equal candidate (getf temporary :record))
               (equal temporary
                      (getf (physical-reseed-follow-cleaned-recovery-snapshot-at root-fd) :temporary)))
    (error "cleanup recovery temporary changed before promotion")))

(defun physical-reseed-follow-open-cleaned-recovery-temporary (root-fd temporary candidate)
  (declare (notinline sb-posix:close))
  (let ((fd nil) (complete nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       root-fd "physical-reseed.follow.tmp"
                       (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                               +postmaster-o-cloexec+) 0)))
           (when (minusp fd) (setf fd nil) (error "cannot open cleanup recovery temporary"))
           (unless (and (equal (getf temporary :identity)
                               (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                        (= (getf temporary :mount) (physical-adoption-install-mount-id nil :directory-fd fd)))
             (error "cleanup recovery descriptor differs from its captured temporary"))
           (physical-reseed-follow-check-cleaned-recovery-temporary-at root-fd temporary candidate)
           (setf complete t)
           fd)
      (unless complete (when fd (sb-posix:close fd))))))
