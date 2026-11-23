;;;; Native prefix preparation for C's XLogInitNewTimeline sequence.
(in-package #:pg)

(defvar *wal-timeline-test-hook* nil)

(defun wal-timeline-test-point (point)
  (when *wal-timeline-test-hook* (funcall *wal-timeline-test-hook* point)))

(defun wal-copy-timeline-prefix (source destination count)
  "Copy exactly COUNT bytes; never synthesize missing source bytes."
  (let ((buffer (make-array (min count 65536) :element-type '(unsigned-byte 8))))
    (loop with remaining = count
          while (plusp remaining)
          for length = (min remaining (length buffer))
          do (unless (= length (read-sequence buffer source :end length))
               (error "short parent WAL prefix read"))
             (write-sequence buffer destination :end length)
             (decf remaining length))))

(defun wal-stage-timeline-prefix (source-timeline fork &key (namespace *wal-segment-namespace*) (prefix nil prefix-p) directory-fd observer)
  "Prepare C XLogInitNewTimeline's prefix in a unique, synced temporary inode.
The caller pins immutable source files and has already verified FORK as a durable
applied terminal with cluster/history provenance. This copies bytes, not proof.
Return a temporary pathname owned by the caller, which must install or delete it.
No canonical child file, history, WAL control or runtime identity is published.
With PREFIX explicitly supplied, stage those independently authenticated bytes
instead of reading a parent file; require its exact fork-offset length.
Native variable-length segments end at the fork offset; unlike C, no zero tail
is written. At an exact segment boundary no parent file is needed.
DIRECTORY-FD, when supplied, is the caller-held WAL directory descriptor."
  (declare (notinline open-exclusive-temporary-file sb-posix:unlink))
  (unless (and (typep fork '(unsigned-byte 64))
               (typep source-timeline '(integer 1 4294967295))
               (member namespace '(:legacy :timeline))
               (or (eq namespace :timeline) (= source-timeline 1)))
    (error "invalid parent WAL prefix identity"))
  (multiple-value-bind (segment offset) (wal-segment-and-offset fork)
    (when (and prefix-p
               (not (and (typep prefix '(vector (unsigned-byte 8))) (= offset (length prefix)))))
      (error "authenticated timeline prefix has the wrong length or type"))
    (let ((source nil) (temporary nil) (fd nil) (destination nil) (finished nil))
      (sb-sys:without-interrupts
        (unwind-protect
             (progn
               (when (and (not prefix-p) (plusp offset))
                 (multiple-value-bind (stream timeline size)
                     (wal-lineage-open-segment
                      segment (list (make-timeline-history-entry :timeline source-timeline :begin 0))
                      0 namespace)
                   (declare (ignore timeline))
                   (setf source stream)
                   (unless (<= offset size) (error "parent WAL prefix is incomplete"))))
               (wal-ensure-directory-durable (wal-directory) directory-fd)
               (multiple-value-setq (fd temporary)
                 (open-exclusive-temporary-file (merge-pathnames ".timeline-XXXXXX" (wal-directory))))
               (wal-staging-observe observer :created :prefix temporary fd)
               (setf destination (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8))
                     fd nil)
               (sb-sys:with-local-interrupts
                 (when source
                   (wal-copy-timeline-prefix source destination offset)
                   (close source)
                   (setf source nil))
                 (when prefix-p (write-sequence prefix destination))
                 (finish-output destination)
                 (wal-timeline-test-point :before-prefix-fsync)
                 (sb-sys:without-interrupts
                   (fsync-stream destination)
                   (wal-staging-observe observer :written :prefix temporary
                                        (sb-sys:fd-stream-fd destination)))
                 (wal-timeline-test-point :after-prefix-fsync)
                 (close destination)
                 (setf destination nil)
                 (fsync-directory (wal-directory))
                 (wal-timeline-test-point :after-prefix-directory-fsync))
               (setf finished t)
               (pathname temporary))
          (when source (ignore-errors (close source)))
          (when destination (ignore-errors (close destination)))
          (when fd (ignore-errors (sb-posix:close fd)))
          (when (and temporary (not finished))
            (ignore-errors (sb-posix:unlink (namestring temporary)))))))))

(defun wal-archive-promotion-partial-name (source-timeline fork)
  (multiple-value-bind (segment offset) (wal-segment-and-offset fork)
    (when (plusp offset)
      (concatenate 'string (wal-segment-filename segment source-timeline :timeline) ".partial"))))

(defun wal-archive-install-source (directory-fd name bytes check-directory)
  "Sync archive source bytes and publish them without replacing a name."
  (let ((template (merge-pathnames ".archive-source-XXXXXX" (physical-image-fd-path directory-fd)))
        (fd nil) (temporary nil) (stream nil) (installed nil))
    (unwind-protect
         (progn
           (multiple-value-bind (descriptor path) (open-exclusive-temporary-file template)
             (setf fd descriptor temporary (file-namestring path)))
           (setf stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)))
           (write-sequence bytes stream)
           (finish-output stream)
           (sb-posix:fsync fd)
           (unless (equal (physical-image-file-facts fd)
                          (physical-image-file-facts directory-fd temporary))
             (error "archive source temporary changed"))
           (funcall check-directory directory-fd)
           (slot-state-rename-no-replace directory-fd temporary directory-fd name)
           (setf installed t)
           (sb-posix:fsync directory-fd)
           (wal-archive-check-source directory-fd name fd))
      (unwind-protect
           (when (and temporary (not installed)
                      (ignore-errors (equal (physical-image-file-facts fd)
                                            (physical-image-file-facts directory-fd temporary))))
             (port-unlinkat directory-fd temporary 0))
        (cond (stream (close stream :abort t))
              (fd (sb-posix:close fd)))))))

(defun wal-archive-stage-source (directory-fd name bytes check-directory)
  "Stabilize an identical staged source on retry, or install a new one."
  (sb-sys:without-interrupts
    (funcall check-directory directory-fd)
    (let ((path (merge-pathnames name (physical-image-fd-path directory-fd))))
      (if (wal-namespace-file-stat path t)
          (unless (equalp bytes (wal-archive-read-source-bytes directory-fd name :sync t))
            (error "existing archive source differs: ~a" name))
          (wal-archive-install-source directory-fd name bytes check-directory)))
    (funcall check-directory directory-fd)
    name))

(defun wal-archive-stage-promotion-partial (source-timeline fork prefix)
  "Stage the verified fork prefix under its timeline-qualified archive name."
  (let ((name (wal-archive-promotion-partial-name source-timeline fork)))
    (when name
      (unless (and (typep prefix '(vector (unsigned-byte 8)))
                   (= (length prefix) (mod fork +wal-segment-size+)))
        (error "invalid promotion archive prefix"))
      (with-wal-archive-directory (wal-fd)
        (wal-archive-stage-source wal-fd name prefix #'wal-archive-check-wal-directory)
        (when (>= (length prefix) +wal-header-size+)
          (wal-archive-stage-source
           wal-fd (wal-archive-segment-proof-name name)
           (wal-encode-archive-segment-proof
            (catalog-system-identifier) source-timeline
            (floor fork +wal-segment-size+) :timeline fork prefix)
           #'wal-archive-check-wal-directory))
        name))))

;; Prefix staging can install several destination paths during rollover, so an
;; observer's ledger must distinguish paths.  The scratch stream is closed before
;; installation; install callbacks therefore have no borrowed descriptor.
(defun wal-install-timeline-prefix (source-timeline child fork prefix namespace &optional directory-fd observer)
  "Force-install owned uncommitted bytes; caller proves intent and excludes history.
The caller fences uncertain publication and must never use this after history.
DIRECTORY-FD is an optional borrowed WAL directory descriptor."
  (declare (notinline sb-posix:rename sb-posix:unlink))
  (let ((destination (wal-segment-path (floor fork +wal-segment-size+) child :timeline))
        (temporary nil))
    (wal-namespace-file-stat destination t)
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf temporary (wal-stage-timeline-prefix source-timeline fork
                                                        :namespace namespace :prefix prefix
                                                        :directory-fd directory-fd :observer observer))
             (sb-sys:with-local-interrupts
               (when (wal-namespace-file-stat destination t)
                 (sb-sys:without-interrupts
                   (wal-staging-observe observer :before-unlink :prefix destination nil)
                   (sb-posix:unlink (namestring destination))
                   (wal-staging-observe observer :unlinked :prefix destination nil)))
               (wal-timeline-test-point :after-child-unlink)
               (fsync-directory (wal-directory))
               (wal-timeline-test-point :after-child-unlink-directory-fsync)
               (sb-sys:without-interrupts
                 (wal-staging-observe observer :before-install :prefix temporary nil destination)
                 (sb-posix:rename (namestring temporary) (namestring destination))
                 (let ((installed temporary))
                   (setf temporary nil)
                   (wal-staging-observe observer :installed :prefix installed nil destination)))
               (wal-timeline-test-point :after-child-rename)
               (fsync-directory (wal-directory))
               (wal-timeline-test-point :after-child-directory-fsync)
               destination))
        (when temporary (ignore-errors (sb-posix:unlink (namestring temporary))))))))
