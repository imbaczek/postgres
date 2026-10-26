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

(defun wal-stage-timeline-prefix (source-timeline fork &key (namespace *wal-segment-namespace*))
  "Prepare C XLogInitNewTimeline's prefix in a unique, synced temporary inode.
The caller pins immutable source files and has already verified FORK as a durable
applied terminal with cluster/history provenance. This copies bytes, not proof.
Return a temporary pathname owned by the caller, which must install or delete it.
No canonical child file, history, WAL control or runtime identity is published.
Native variable-length segments end at the fork offset; unlike C, no zero tail
is written. At an exact segment boundary no parent file is needed."
  (unless (and (typep fork '(unsigned-byte 64))
               (typep source-timeline '(integer 1 4294967295))
               (member namespace '(:legacy :timeline))
               (or (eq namespace :timeline) (= source-timeline 1)))
    (error "invalid parent WAL prefix identity"))
  (multiple-value-bind (segment offset) (wal-segment-and-offset fork)
    (let ((source nil) (temporary nil) (fd nil) (destination nil) (finished nil))
      (unwind-protect
           (progn
             (when (plusp offset)
               (multiple-value-bind (stream timeline size)
                   (wal-lineage-open-segment
                    segment (list (make-timeline-history-entry :timeline source-timeline :begin 0))
                    0 namespace)
                 (declare (ignore timeline))
                 (setf source stream)
                 (unless (<= offset size) (error "parent WAL prefix is incomplete"))))
             (wal-ensure-directory-durable (wal-directory))
             (multiple-value-setq (fd temporary)
               (sb-posix:mkstemp (namestring (merge-pathnames ".timeline-XXXXXX" (wal-directory)))))
             (setf destination (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8))
                   fd nil)
             (when source
               (wal-copy-timeline-prefix source destination offset)
               (close source)
               (setf source nil))
             (finish-output destination)
             (wal-timeline-test-point :before-prefix-fsync)
             (fsync-stream destination)
             (wal-timeline-test-point :after-prefix-fsync)
             (close destination)
             (setf destination nil)
             (fsync-directory (wal-directory))
             (wal-timeline-test-point :after-prefix-directory-fsync)
             (setf finished t)
             (pathname temporary))
        (when source (ignore-errors (close source)))
        (when destination (ignore-errors (close destination)))
        (when fd (ignore-errors (sb-posix:close fd)))
        (when (and temporary (not finished))
          (ignore-errors (sb-posix:unlink temporary)))))))
