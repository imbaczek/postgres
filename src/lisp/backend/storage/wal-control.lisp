;;;; Primary WAL control reads and durable publication.
(in-package #:pg)

(defun wal-read-control-fields ()
  "Read one bounded regular control image, returning NIL only when absent.
Reject extra lines before decoding its CRC and checkpoint metadata."
  (let ((path (wal-control-path)))
    (handler-case
        (with-input-from-string
            (stream (bytes->string (read-bounded-regular-file path (1+ +wal-control-maximum-bytes+) :minimum 1)))
          (let ((line (read-line stream nil nil)))
            (unless (and line (<= (length line) +wal-control-maximum-bytes+) (null (read-line stream nil nil)))
              (error "WAL control file ~a is corrupt" path))
            ;; Keep asynchronous interrupts out of the bounded decode and its
            ;; error converter; they can run again once the handler is gone.
            (sb-sys:without-interrupts
              (handler-case-noninterrupt (wal-decode-control-line line)
                                         (condition
                                          (error "WAL control file ~a is corrupt: ~a" path condition))))))
      (sb-posix:syscall-error (condition)
        (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
          (error condition))))))

(defun wal-save-control-fields (checkpoint-lsn durable-lsn)
  "Save WAL positions and retain any known primary identity and lifecycle state.
Refresh its modification time. Update memory only after atomic publication."
  (let ((metadata (when *wal-control-metadata*
                    (wal-refresh-control-metadata
                     *wal-control-metadata* (getf *wal-control-metadata* :db-state)))))
    (wal-atomic-write-bytes
     (wal-control-path)
     (wal-encode-control-image checkpoint-lsn durable-lsn *wal-checkpoint-snapshot*
                               :timeline *wal-insert-timeline*
                               :checkpoint-timeline *wal-checkpoint-timeline*
                               :namespace *wal-segment-namespace* :metadata metadata))
    (setf *wal-control-metadata* metadata))
  (values))

(defun wal-save-durable-lsn (lsn)
  (when (< lsn *wal-durable-lsn*)
    (error "WAL durable LSN cannot move backwards from ~a to ~a"
           *wal-durable-lsn* lsn))
  (wal-save-control-fields *wal-checkpoint-lsn* lsn)
  (setf *wal-durable-lsn* lsn)
  (values))

(defun wal-save-checkpoint-control (lsn &optional snapshot clog-extent)
  (when (> lsn *wal-durable-lsn*)
    (error "WAL checkpoint LSN ~a is past durable LSN ~a" lsn *wal-durable-lsn*))
  ;; WAL-FLUSH retains the old copy. Publish a candidate only after page/CLOG sync.
  (let ((metadata
          (let ((*wal-checkpoint-snapshot* snapshot)
                (*wal-checkpoint-timeline* (if snapshot (getf snapshot :timeline) *wal-insert-timeline*))
                (*wal-control-metadata* (copy-list *wal-control-metadata*)))
            (when *wal-control-metadata*
              (if clog-extent
                  (setf (getf *wal-control-metadata* :checkpoint-clog-bytes) clog-extent)
                  (remf *wal-control-metadata* :checkpoint-clog-bytes)))
            (wal-save-control-fields lsn *wal-durable-lsn*)
            *wal-control-metadata*)))
    (setf *wal-control-metadata* metadata))
  (setf *wal-checkpoint-lsn* lsn
        *wal-checkpoint-snapshot* (copy-list snapshot)
        *wal-checkpoint-timeline* (if snapshot (getf snapshot :timeline) *wal-insert-timeline*))
  (values))
