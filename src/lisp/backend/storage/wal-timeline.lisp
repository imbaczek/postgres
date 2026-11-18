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

(defun wal-archive-promotion-partial-name (source-timeline fork namespace)
  (multiple-value-bind (segment offset) (wal-segment-and-offset fork)
    (when (plusp offset)
      (concatenate 'string (wal-segment-filename segment source-timeline namespace) ".partial"))))

(defun wal-archive-write-promotion-partial (wal-fd name prefix)
  "Sync the verified fork prefix and install it without replacing an existing file."
  (let ((template (merge-pathnames ".archive-partial-XXXXXX" (physical-image-fd-path wal-fd)))
        (fd nil) (temporary nil) (stream nil) (installed nil))
    (unwind-protect
         (progn
           (multiple-value-bind (descriptor path) (open-exclusive-temporary-file template)
             (setf fd descriptor temporary (file-namestring path)))
           (setf stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)))
           (write-sequence prefix stream)
           (finish-output stream)
           (sb-posix:fsync fd)
           (unless (equal (physical-image-file-facts fd)
                          (physical-image-file-facts wal-fd temporary))
             (error "promotion archive temporary changed"))
           (wal-archive-check-wal-directory wal-fd)
           (slot-state-rename-no-replace wal-fd temporary wal-fd name)
           (setf installed t)
           (sb-posix:fsync wal-fd))
      (unwind-protect
           (when (and temporary (not installed)
                      (ignore-errors (equal (physical-image-file-facts fd)
                                            (physical-image-file-facts wal-fd temporary))))
             (port-unlinkat wal-fd temporary 0))
        (cond (stream (close stream :abort t))
              (fd (sb-posix:close fd)))))))

(defun wal-archive-stage-promotion-partial (source-timeline fork prefix namespace)
  "Stage the verified fork prefix as an archive partial when needed.
On retry, accept an existing partial only when its bytes match."
  (let ((name (wal-archive-promotion-partial-name source-timeline fork namespace)))
    (when name
      (unless (and (typep prefix '(vector (unsigned-byte 8)))
                   (= (length prefix) (mod fork +wal-segment-size+)))
        (error "invalid promotion archive prefix"))
      (sb-sys:without-interrupts
        (let ((wal-fd (wal-archive-open-wal-directory)))
          (unwind-protect
               (let ((path (merge-pathnames name (physical-image-fd-path wal-fd))))
                 (wal-archive-check-wal-directory wal-fd)
                 (if (wal-namespace-file-stat path t)
                     (unless (equalp prefix
                                     (wal-archive-read-source-bytes wal-fd name :sync t))
                       (error "existing promotion archive partial differs: ~a" name))
                     (wal-archive-write-promotion-partial wal-fd name prefix)))
            (sb-posix:close wal-fd))))
      name)))

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
