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
