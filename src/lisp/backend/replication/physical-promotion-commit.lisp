;;;; Child checkpoint/control is the irreversible native promotion commit point.
;;;; A pending promotion still belongs to the parent until child control is saved.
;;;; Build one fixed sequence: end-of-recovery, batch start, then checkpoint.
;;;; The plan uses the selected restartpoint's catalog and reservation proof;
;;;; retries must produce the same bytes, including timestamps and counters.
;;;;
;;;; The caller owns PGDATA and holds promotion exclusion with admission closed.
;;;; Check existing child files before restoring data. Before history publication,
;;;; abandoned child staging may be replaced. After history publication, accept
;;;; only the planned files and bounded incomplete records.
;;;;
;;;; Sync WAL, relation data, CLOG and catalog before replacing PGWAL6 control.
;;;; That replacement commits the child. A later failure must restart from child
;;;; control, never restore the parent generation. See physical-promotion-primary.
;;;; This native checkpoint bridge is slower than C's lightweight promotion path;
;;;; compare StartupXLOG and PerformRecoveryXLogAction in access/transam/xlog.c.

(in-package #:pg)

(defun physical-promotion-checkpoint-plan (state next-oid local-floor)
  "Build the child WAL plan without changing files or runtime state.
STATE is the verified pending request. NEXT-OID comes from its saved catalog;
LOCAL-FLOOR is the persisted allocation floor. Keep actual replay visibility
separate from the capacity that the new primary must reserve.

Return a plist with :FORK, :CHILD, :SNAPSHOT, :END and :FRAMES. Each frame is
a list of its start LSN and encoded bytes. Each record fits entirely within
one segment; rotation leaves an EOF gap rather than inventing padding records."
  (unless (and (eql 3 (getf state :phase)) (typep next-oid '(unsigned-byte 64))
               (physical-source-reservation-valid-p (getf state :source-reservation))
               (getf (getf state :source-reservation) :complete)
               (typep local-floor '(integer 3 18446744073709551616)))
    (error "child checkpoint lacks allocation proof"))
  (multiple-value-bind (eor eor-start) (physical-promotion-eor-image state)
    (let* ((batch-start (wal-record-start-after (+ eor-start (length eor)) +wal-header-size+))
           (start (wal-record-start-after (+ batch-start +wal-header-size+) (wal-checkpoint-record-size)))
           (end (+ start (wal-checkpoint-record-size)))
           (next (canonical-next-full-xid
                  (max (getf state :next-xid) local-floor
                       (1+ (getf (getf state :source-reservation) :bound)))))
           (snapshot (list :version 2 :start start :end end :redo start
                           :next-xid next :xid-reserved-through (1- next) :next-oid next-oid
                           :time (getf state :state-time) :timeline (getf state :promotion-child)
                           :prev-timeline (getf state :timeline) :oldest-active-xid next)))
      (unless (and (<= end #xffffffffffffffff) (wal-checkpoint-snapshot-valid-p snapshot))
        (error "invalid child checkpoint plan"))
      (list :fork (getf state :applied) :child (getf state :promotion-child)
            :snapshot snapshot :end end
            :frames (list (list eor-start eor)
                          (list batch-start (wal-encode-record +wal-record-batch-start+ 0 batch-start #()))
                          (list start (wal-encode-record +wal-record-checkpoint+ 0 start
                                                         (wal-encode-checkpoint-snapshot snapshot))))))))

(defun physical-promotion-checkpoint-files (plan prefix)
  "Return the complete expected child files as (segment bytes) pairs.
PREFIX is the authenticated parent prefix ending at the fork. Include it
unchanged, append only planned records, and leave rotation gaps at EOF.
The returned byte vectors describe the whole allowed extent of each file."
  (let ((files (make-hash-table :test #'eql)) (fork (getf plan :fork)))
    (unless (= (length prefix) (mod fork +wal-segment-size+)) (error "invalid child checkpoint prefix"))
    (setf (gethash (floor fork +wal-segment-size+) files) prefix)
    (dolist (frame (getf plan :frames))
      (destructuring-bind (start bytes) frame
        (let* ((segment (floor start +wal-segment-size+))
               (prior (gethash segment files (make-array 0 :element-type '(unsigned-byte 8)))))
          (unless (= (length prior) (mod start +wal-segment-size+)) (error "invalid checkpoint rotation"))
          (setf (gethash segment files) (concatenate '(vector (unsigned-byte 8)) prior bytes)))))
    (sort (loop for segment being the hash-keys of files collect (list segment (gethash segment files)))
          #'< :key #'first)))

(defun physical-promotion-current-checkpoint-plan (state)
  "Read the exact saved catalog and local XID floor, then build the child plan.
Do not use mutable runtime catalog counters to choose retry bytes."
  (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
    (declare (ignore metadata prefix generation))
    (values (physical-promotion-checkpoint-plan
             state (getf (physical-history-read-catalog catalog) :next-oid) (load-next-xid))
            catalog)))

(defun physical-promotion-checkpoint-artifacts (state prefix &key allow-uninstalled)
  "Check child files without changing them, before restoring the catalog.
Before history exists, ALLOW-UNINSTALLED permits replacement from the saved
prefix. Once history exists, every child segment must belong to the plan.
Reject a foreign complete record, an oversized file or checkpoint staging
without its exact EOR. Bounded torn records may be rewritten by the publisher.
Return the verified plan; signal an error for contradictory artifacts."
  (let* ((plan (physical-promotion-current-checkpoint-plan state))
         (files (physical-promotion-checkpoint-files plan prefix))
         (child (getf state :promotion-child))
         (history-p (wal-namespace-file-stat (timeline-history-path child *pgdata*) t))
         (eor (first (getf plan :frames)))
         (after-eor-p nil) (eor-exact-p nil))
    (dolist (segment (wal-discover-segments child :timeline))
      (wal-namespace-file-stat (wal-segment-path segment child :timeline))
      (when (and history-p (not (assoc segment files)))
        (error "published child history has an unexplained WAL segment")))
    (dolist (entry files)
      (destructuring-bind (segment expected) entry
        (let* ((path (wal-segment-path segment child :timeline))
               (stat (wal-namespace-file-stat path t)))
          (unless (or (and allow-uninstalled (not history-p))
                      (and (not history-p)
                           (/= segment (floor (getf plan :fork) +wal-segment-size+))))
            (let* ((eor-segment (floor (first eor) +wal-segment-size+))
                   (bytes (if stat (physical-promotion-read-file path)
                              (if (> segment eor-segment)
                                  (make-array 0 :element-type '(unsigned-byte 8))
                                  (error "pending checkpoint lost durable prefix/EOR file"))))
                   (prefix-size (if (= segment (floor (getf plan :fork) +wal-segment-size+))
                                    (length prefix) 0)))
              (unless (and (<= prefix-size (length bytes) (length expected))
                           (equalp prefix (if (plusp prefix-size) (subseq bytes 0 prefix-size) prefix)))
                (error "child checkpoint prefix or extent differs"))
              (dolist (frame (getf plan :frames))
                (destructuring-bind (start record) frame
                  (when (= segment (floor start +wal-segment-size+))
                    (let* ((offset (mod start +wal-segment-size+))
                           (available (max 0 (min (length record) (- (length bytes) offset))))
                           (raw (subseq bytes (min offset (length bytes)))))
                      (when (and (plusp available)
                                 (not (equalp record (subseq raw 0 (min (length raw) (length record)))))
                                 (physical-promotion-valid-tail-frame-p raw))
                        (error "child checkpoint contains a different complete record"))
                      (when (eq frame eor)
                        (setf eor-exact-p (and (= available (length record))
                                               (equalp record (subseq raw 0 (length record))))))
                      (when (and (not (eq frame eor)) (plusp available))
                        (setf after-eor-p t)))))))))))
    (when (and after-eor-p (not eor-exact-p))
      (error "child checkpoint staging lost its durable EOR"))
    plan))

(defun physical-promotion-open-staging-file (path)
  "Open a regular child file for reading and writing, or create it exclusively.
Return its stream and size. The caller closes the stream. Do not follow a
symlink or replace an existing special file."
  (if (wal-namespace-file-stat path t)
      (physical-promotion-open-file path t)
      (let ((fd (sb-posix:open (namestring path)
                               (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                       sb-posix:o-nofollow +postmaster-o-cloexec+) #o600)))
        (handler-case
            (values (sb-sys:make-fd-stream fd :input t :output t :element-type '(unsigned-byte 8)) 0)
          (error (condition) (sb-posix:close fd) (error condition))))))

(defun physical-promotion-stage-frame (child frame)
  "Write and sync one planned frame, preserving any exact existing bytes.
Earlier preflight must have checked the file extent and all staged records.
Never create a hole or rewrite an exact EOR beneath a later checkpoint.
Close the stream on every exit; the enclosing publisher fences failures."
  (destructuring-bind (start record) frame
    (let* ((path (wal-segment-path (floor start +wal-segment-size+) child :timeline))
           (offset (mod start +wal-segment-size+)))
      (multiple-value-bind (stream size) (physical-promotion-open-staging-file path)
        (unwind-protect
             (let ((prior (make-array (min (length record) (max 0 (- size offset)))
                                      :element-type '(unsigned-byte 8))))
               (unless (<= offset size) (error "checkpoint staging would create an unexplained hole"))
               (file-position stream offset)
               (unless (= (length prior) (read-sequence prior stream)) (error "short checkpoint staging read"))
               (unless (equalp prior record)
                 (file-position stream offset)
                 (write-sequence record stream))
               (physical-promotion-test-point
                (ecase (wal-u16 record 6)
                  (13 :after-child-eor-write) (6 :after-child-batch-write) (4 :after-child-checkpoint-write)))
               (wal-sync-output-bytes stream t))
          (close stream))))))

(defun physical-promotion-remove-child-orphans (state files)
  "Remove abandoned selected-child segments outside FILES before history exists.
Preflight all orphan names as regular files before unlinking any of them.
Published history makes an unexpected segment an error, not disposable staging.
Parent segments are never selected. Sync the WAL directory after removals."
  (let* ((child (getf state :promotion-child))
         (segments (wal-discover-segments child :timeline))
         (orphans (remove-if (lambda (segment) (assoc segment files)) segments)))
    (when (and orphans (wal-namespace-file-stat (timeline-history-path child *pgdata*) t))
      (error "published child history has an unexplained WAL segment"))
    (dolist (segment orphans) (wal-namespace-file-stat (wal-segment-path segment child :timeline)))
    (dolist (segment orphans)
      (sb-posix:unlink (namestring (wal-segment-path segment child :timeline)))
      (physical-promotion-test-point :after-child-orphan-unlink))
    (when orphans (fsync-directory (wal-directory)))))

(defun physical-promotion-save-child-control (plan)
  "Atomically commit the planned child checkpoint and durable end in PGWAL6.
The caller must already have synced all referenced WAL and database files.
Once this image survives, startup must recover the child even if later cleanup
fails. Do not restore parent control on an error after the rename.
Use crash-recovery state until primary initialization and trigger retirement
finish; a durable child is not yet a production-ready postmaster."
  (let ((snapshot (getf plan :snapshot)) (child (getf plan :child)) (end (getf plan :end)))
    (with-atomic-output
        (stream (wal-control-path) (merge-pathnames "global/pg_wal_control.tmp" *pgdata*)
                (merge-pathnames "global/" *pgdata*) :element-type '(unsigned-byte 8)
                :rename-function (lambda (from to)
                                   (sb-posix:rename from to)
                                   (physical-promotion-test-point :after-child-control-rename))
                :fsync-directory-function
                (lambda (directory) (fsync-directory directory)
                  (physical-promotion-test-point :after-child-control-directory-fsync)))
      (write-sequence (wal-encode-control-image end end snapshot :timeline child
                                                                 :checkpoint-timeline child :namespace :timeline
                                                                 :metadata
                                                                 (make-wal-control-metadata
                                                                  (getf *physical-recovery-state* :system-id)
                                                                  +db-in-crash-recovery+))
                      stream))))

(defun physical-promotion-publish-child-control ()
  "Commit a pending promotion while writer admission remains closed.
Require owned promotion exclusion, the exact saved generation and an empty
application write cache. Validate staging, sync history and planned WAL, then
sync database files before publishing child control. Return the checkpoint end.

This function does not initialize the primary or clear recovery exclusion.
An error or nonlocal exit after publication starts fences this process; restart
must determine from durable control whether parent or child is authoritative."
  (with-physical-promotion-exclusion (3)
    (let ((state *physical-recovery-state*) (finished nil))
      (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
        (declare (ignore catalog))
        (physical-promotion-check-pending state metadata prefix generation :allow-history t)
        (let* ((plan (physical-promotion-checkpoint-artifacts state prefix))
               (files (physical-promotion-checkpoint-files plan prefix))
               (child (getf state :promotion-child))
               (history-p (wal-namespace-file-stat (timeline-history-path child *pgdata*) t)))
          (unless (and (zerop (hash-table-count *wal-page-cache*)) (null *wal-page-batch*)
                       (null *active-xids*))
            (error "child checkpoint requires an empty application write cache"))
          (unwind-protect
               (progn
                 (physical-promotion-remove-child-orphans state files)
                 (unless history-p
                   (dolist (entry (rest files))
                     (wal-install-timeline-prefix (getf state :timeline) child
                                                  (* (first entry) +wal-segment-size+)
                                                  (make-array 0 :element-type '(unsigned-byte 8))
                                                  (getf state :promotion-source-namespace))))
                 (physical-promotion-sync-history state)
                 (physical-promotion-test-point :after-child-history-sync)
                 (dolist (frame (getf plan :frames))
                   (physical-promotion-stage-frame child frame)
                   (when (eq frame (first (getf plan :frames)))
                     (physical-promotion-test-point :after-child-eor-flush)))
                 (physical-promotion-test-point :after-child-checkpoint-flush)
                 (smgr-fsync-all) (clog-fsync)
                 (with-open-file (stream (catalog-path) :element-type '(unsigned-byte 8)) (fsync-stream stream))
                 (dolist (directory (list *pgdata* (merge-pathnames "base/" *pgdata*)
                                          (merge-pathnames "global/" *pgdata*)))
                   (fsync-directory directory))
                 (physical-promotion-test-point :after-child-data-sync)
                 (physical-promotion-save-child-control plan)
                 (setf finished t)
                 (getf plan :end))
            (unless finished (setf *physical-recovery-failed-p* t))))))))
