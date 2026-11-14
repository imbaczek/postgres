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

;;;; The sequence above applies to legacy pending control. Frozen FPW intent
;;;; may add a batch start and type 17 change between EOR and the checkpoint
;;;; batch when the source policy is unknown or differs. Native EOR first proves
;;;; child lineage; C calls UpdateFullPageWrites before PerformRecoveryXLogAction.
;;;; Writer admission stays closed until child control is durable.
(in-package #:pg)

(defun physical-promotion-policy-prefix
    (state cursor source-key intent-key checkpoint-key copy-fact fact-at-p record-type encode)
  "Plan a policy change and checkpoint field from frozen intent and source evidence.
Unknown source status requires a change; absent intent emits no change or field.
Return frames, the next cursor and checkpoint fields without publishing them."
  (let ((source (funcall copy-fact (getf state source-key))))
    (when (and source
               (not (funcall fact-at-p source (getf state :applied)
                             (physical-recovery-replay-timeline state))))
      (error "promotion ~s source differs from its frozen frontier" intent-key))
    (unless (get-properties state (list intent-key))
      (return-from physical-promotion-policy-prefix (values nil cursor nil)))
    (let* ((desired (getf state intent-key))
           (fields (list checkpoint-key desired)))
      (unless (member desired '(nil t)) (error "invalid frozen promotion policy ~s" intent-key))
      (when (and source (eql desired (getf source :enabled)))
        (return-from physical-promotion-policy-prefix (values nil cursor fields)))
      (let* ((payload (funcall encode desired))
             (batch (wal-record-start-after cursor +wal-header-size+))
             (start (wal-record-start-after (+ batch +wal-header-size+)
                                            (+ +wal-header-size+ (length payload))))
             (bytes (wal-encode-record record-type 0 start payload)))
        (values (list (list batch (wal-encode-record +wal-record-batch-start+ 0 batch #()))
                      (list start bytes))
                (+ start (length bytes)) fields)))))

(defun physical-promotion-fpw-prefix (state cursor)
  "Plan an FPW change when frozen local policy differs from the source fact.
Unknown source state requires a change; absent legacy intent emits none."
  (physical-promotion-policy-prefix
   state cursor :source-fpw-fact :promotion-full-page-writes :full-page-writes
   #'physical-fpw-copy-fact #'physical-fpw-fact-at-p
   +wal-record-full-page-writes-change+ #'wal-encode-full-page-writes-change))

(defun physical-promotion-logical-prefix (state cursor)
  "Plan the child logical status and checkpoint fields from frozen evidence.
An unchanged ON status retains the inherited pair, including unknown. A change
to ON writes a child terminal and uses its end and child timeline as the pair."
  (multiple-value-bind (frames next fields)
      (physical-promotion-policy-prefix
       state cursor :source-logical-fact :promotion-logical-decoding-enabled :logical-decoding-enabled
       #'physical-logical-copy-fact #'physical-logical-fact-at-p
       +wal-record-logical-decoding-status-change+ #'wal-encode-logical-decoding-status-change)
    (let ((source (getf state :source-logical-fact)))
      (values frames next
              (append fields
                      (when (getf fields :logical-decoding-enabled)
                        (cond
                          (frames (list :logical-on-start-lsn next
                                        :logical-on-start-timeline (getf state :promotion-child)))
                          ((getf source :logical-on-start-lsn)
                           (list :logical-on-start-lsn (getf source :logical-on-start-lsn)
                                 :logical-on-start-timeline (getf source :logical-on-start-timeline))))))))))

(defun physical-promotion-checkpoint-plan (state next-oid local-floor)
  "Build the child WAL plan without changing files or runtime state.
STATE is the verified pending request. NEXT-OID comes from its saved catalog;
LOCAL-FLOOR is the persisted allocation floor. Keep actual replay visibility
separate from the capacity that the new primary must reserve.

Return a plist with :FORK, :CHILD, :SNAPSHOT, :END and :FRAMES. Each frame is
a list of its start LSN and encoded bytes. Each record fits entirely within
one segment; rotation leaves an EOF gap rather than inventing padding records.
Copy the source checkpoint's historical commit timestamp XID pair before size
planning. Local configuration does not establish facts about that checkpoint;
an absent pair remains unknown. The frozen source facts select the exact size
and enter the snapshot, so placement and retries use the same captured facts.
Semantic records are ordered EOR, optional FPW change, optional logical status
change, checkpoint. Each batched record has a preceding batch-start frame."
  (unless (and (eql 3 (getf state :phase)) (typep next-oid '(unsigned-byte 64))
               (physical-source-reservation-valid-p (getf state :source-reservation))
               (getf (getf state :source-reservation) :complete)
               (typep local-floor '(integer 3 18446744073709551616)))
    (error "child checkpoint lacks allocation proof"))
  (multiple-value-bind (eor eor-start) (physical-promotion-eor-image state)
    (multiple-value-bind (fpw-prefix fpw-cursor fpw-fields)
        (physical-promotion-fpw-prefix state (+ eor-start (length eor)))
      (multiple-value-bind (logical-prefix cursor logical-fields)
          (physical-promotion-logical-prefix state fpw-cursor)
        (let* ((checkpoint-facts
                 (append (copy-tree (getf state :source-resource-fact))
                         (wal-copy-checkpoint-commit-ts-fields (getf state :source-checkpoint))
                         fpw-fields logical-fields))
               (record-size (wal-checkpoint-record-size checkpoint-facts))
               (batch-start (wal-record-start-after cursor +wal-header-size+))
               (start (wal-record-start-after (+ batch-start +wal-header-size+) record-size))
               (end (+ start record-size))
               (next (canonical-next-full-xid
                      (max (getf state :next-xid) local-floor
                           (1+ (getf (getf state :source-reservation) :bound)))))
               (snapshot (list :version 2 :start start :end end :redo start
                               :next-xid next :xid-reserved-through (1- next) :next-oid next-oid
                               :time (getf state :state-time) :timeline (getf state :promotion-child)
                               :prev-timeline (physical-recovery-replay-timeline state) :oldest-active-xid next)))
          (setf snapshot (append snapshot checkpoint-facts))
          (unless (and (<= end #xffffffffffffffff) (wal-checkpoint-snapshot-valid-p snapshot))
            (error "invalid child checkpoint plan"))
          (list :fork (getf state :applied) :child (getf state :promotion-child)
                :snapshot snapshot :end end
                :frames (append (list (list eor-start eor)) fpw-prefix logical-prefix
                                (list (list batch-start (wal-encode-record +wal-record-batch-start+ 0 batch-start #()))
                                      (list start (wal-encode-record +wal-record-checkpoint+ 0 start
                                                                     (wal-encode-checkpoint-snapshot snapshot)))))))))))

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
  "Build the child plan from the saved catalog and local XID floor.

Return the plan and the saved catalog bytes as a second value.  Do not use
mutable runtime catalog counters to choose retry bytes."
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
                  (13 :after-child-eor-write) (6 :after-child-batch-write)
                  (17 :after-child-fpw-write) (18 :after-child-logical-write)
                  (4 :after-child-checkpoint-write)))
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

(defun physical-promotion-sync-child-data (&optional context)
  "Sync child database files and directories before control publication.
With CONTEXT, use its checked held descriptors for directory sync."
  (smgr-fsync-all)
  (clog-fsync)
  (if context
      (call-with-physical-image-input
       (catalog-path) (lambda (stream) (fsync-stream stream))
       :element-type '(unsigned-byte 8))
      (with-open-file (stream (catalog-path) :element-type '(unsigned-byte 8))
        (fsync-stream stream)))
  (if context
      (progn
        (physical-image-check-children context)
        (dolist (fd (list (physical-image-context-root-fd context)
                          (cdr (assoc "base" (physical-image-context-children context) :test #'equal))
                          (cdr (assoc "global" (physical-image-context-children context) :test #'equal))))
          (sb-posix:fsync fd)))
      (dolist (directory (list *pgdata* (merge-pathnames "base/" *pgdata*)
                               (merge-pathnames "global/" *pgdata*)))
        (fsync-directory directory))))

(defun physical-promotion-check-installed-control (parent fd)
  "Check that the installed control name still binds the held descriptor."
  (unless (equal (physical-image-file-facts fd)
                 (physical-image-file-facts parent "pg_wal_control"))
    (error "child control inode changed after rename")))

(defun physical-promotion-write-child-control-at (context bytes check)
  "Write and read back a private read-write temporary under the held global FD.
The caller retains CONTEXT and supplies CHECK for pending, final source and
committed proof. Rename, then sync the held file and directory. Leave an
interrupted temporary in place for reconciliation."
  (physical-image-check-children context)
  (let ((parent (cdr (assoc "global" (physical-image-context-children context) :test #'equal)))
        (fd nil) (stream nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (funcall check nil)
             (setf fd (physical-image-openat
                       parent "pg_wal_control.tmp"
                       (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                               sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
             (when (minusp fd) (setf fd nil) (error "cannot create exclusive child control temporary"))
             (setf stream (sb-sys:make-fd-stream fd :input t :output t :element-type '(unsigned-byte 8)))
             (sb-sys:with-local-interrupts
               (write-sequence bytes stream)
               (force-output stream)
               (physical-promotion-test-point :before-child-control-file-fsync)
               (funcall check nil)
               (sb-posix:fsync fd)
               (physical-promotion-test-point :after-child-control-file-fsync)
               (physical-promotion-test-point :before-child-control-rename)
               (funcall check :final)
               (physical-image-check-children context)
               (let ((stat (sb-posix:fstat fd)))
                 (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                              (= #o600 (logand #o7777 (sb-posix:stat-mode stat)))
                              (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                              (= 1 (sb-posix:stat-nlink stat))
                              (= (length bytes) (sb-posix:stat-size stat))
                              (equal (physical-image-file-facts fd)
                                     (physical-image-file-facts parent "pg_wal_control.tmp")))
                   (error "child control temporary changed before rename")))
               (file-position stream 0)
               (let ((actual (make-array (length bytes) :element-type '(unsigned-byte 8))))
                 (unless (and (= (length bytes) (read-sequence actual stream)) (equalp bytes actual))
                   (error "child control temporary bytes differ before rename")))
               (unless (zerop (port-renameat parent "pg_wal_control.tmp" parent "pg_wal_control"))
                 (error "cannot publish child control"))
               (physical-promotion-test-point :after-child-control-rename)
               (funcall check t)
               (physical-promotion-check-installed-control parent fd)
               (sb-posix:fsync fd)
               (sb-posix:fsync parent)
               (physical-promotion-test-point :after-child-control-directory-fsync)
               (funcall check t)
               (physical-promotion-check-installed-control parent fd)))
        (if stream (close stream) (when fd (sb-posix:close fd)))))))

(defun physical-promotion-child-control-image (plan &optional (time (- (get-universal-time) 2208988800)))
  "Build child control bytes; private imports supply frozen Unix seconds for TIME."
  (let* ((snapshot (getf plan :snapshot))
         (metadata (make-wal-control-metadata (getf *physical-recovery-state* :system-id)
                                              +db-in-crash-recovery+ time))
         (reference (getf *physical-recovery-state* :retained-reference)))
    (setf metadata (append metadata (wal-copy-control-resource-fields snapshot)))
    (when reference (setf (getf metadata :retained-reference) (copy-list reference)))
    (wal-encode-control-image (getf plan :end) (getf plan :end) snapshot
                              :timeline (getf plan :child) :checkpoint-timeline (getf plan :child)
                              :namespace :timeline :metadata metadata)))

(defun physical-promotion-save-child-control (plan &key context check)
  "Atomically commit the planned child checkpoint and durable end in PGWAL6.
The caller must already have synced all referenced WAL and database files.
Once this image survives, startup must recover the child even if later cleanup
fails. Do not restore parent control on an error after the rename.
Carry any retained reference into child control at its fixed parent fork.
Use crash-recovery state until primary initialization and trigger retirement
finish; a durable child is not yet a production-ready postmaster."
  (let ((bytes (if context
                   (physical-promotion-child-control-image plan (getf (getf plan :snapshot) :time))
                   (physical-promotion-child-control-image plan))))
    (if context
        (physical-promotion-write-child-control-at context bytes check)
        (with-atomic-output
            (stream (wal-control-path) (merge-pathnames "global/pg_wal_control.tmp" *pgdata*)
                    (merge-pathnames "global/" *pgdata*) :element-type '(unsigned-byte 8)
                    :rename-function (lambda (from to)
                                       (sb-posix:rename from to)
                                       (physical-promotion-test-point :after-child-control-rename))
                    :fsync-directory-function
                    (lambda (directory) (fsync-directory directory)
                      (physical-promotion-test-point :after-child-control-directory-fsync)))
          (write-sequence bytes stream)))))

(declaim (ftype function logical-slot-copy-import-commit-parent
                logical-slot-copy-import-check-child-commit))

(defun physical-promotion-stage-child-checkpoint
    (state plan prefix files history-p check sync-data &key context install-prefix)
  (let ((child (getf state :promotion-child))
        (directory-fd (when context
                        (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))))
    (physical-promotion-remove-child-orphans state files)
    (unless history-p
      (when install-prefix
        (funcall check nil)
        (wal-install-timeline-prefix (physical-recovery-replay-timeline state) child
                                     (getf plan :fork) prefix
                                     (physical-promotion-parent-namespace state) directory-fd))
      (dolist (entry (rest files))
        (funcall check nil)
        (wal-install-timeline-prefix (physical-recovery-replay-timeline state) child
                                     (* (first entry) +wal-segment-size+)
                                     (make-array 0 :element-type '(unsigned-byte 8))
                                     (physical-promotion-parent-namespace state) directory-fd)))
    (funcall check nil)
    (physical-promotion-sync-history state directory-fd)
    (physical-promotion-test-point :after-child-history-sync)
    (dolist (frame (getf plan :frames))
      (funcall check nil)
      (physical-promotion-stage-frame child frame)
      (when (eq frame (first (getf plan :frames)))
        (physical-promotion-test-point :after-child-eor-flush)))
    (physical-promotion-test-point :after-child-checkpoint-flush)
    (funcall check :staged)
    (funcall sync-data)
    (physical-promotion-test-point :after-child-data-sync)
    (funcall check :staged)
    (getf plan :end)))

(defun physical-promotion-publish-child-control-core (&optional owner context attempt)
  "Publish the checked child plan under promotion exclusion with admission closed.
An import owner supplies repeated proofs and held directories. Import callers
bind *PGDATA* to CONTEXT's root and make CONTEXT the current image. Failure after
publication starts fences recovery; the ordinary path keeps its existing fence."
  (unless (and (sb-thread:holding-mutex-p *physical-follow-command-lock*)
               (sb-thread:holding-mutex-p *reload-run-lock*)
               (sb-thread:holding-mutex-p *physical-restartpoint-start-lock*)
               (sb-thread:holding-mutex-p *timeline-history-lock*)
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (zerop *live-operation-depth*) (not *live-operation-drain-open*))
    (error "child publication requires owned promotion exclusion"))
  (let ((parent (if owner
                    (logical-slot-copy-import-commit-parent owner context attempt)
                    (progn
                      (when (or context attempt) (error "child publication has no import owner"))
                      (logical-slot-copy-check-promotion)
                      nil)))
        (state *physical-recovery-state*) (finished nil))
    (labels ((check (&optional committed)
               (when owner
                 (logical-slot-copy-import-check-child-commit
                  owner context attempt
                  (case committed
                    ((nil :staged :final t) committed)
                    (otherwise (error "unknown child publication proof phase")))))))
      (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
        (declare (ignore catalog))
        (physical-promotion-check-pending state metadata prefix generation
                                          :allow-history t :slot-parent parent)
        (physical-promotion-ensure-pending-retained state metadata)
        (let* ((plan (physical-promotion-checkpoint-artifacts state prefix :allow-uninstalled (not (null owner))))
               (files (physical-promotion-checkpoint-files plan prefix))
               (history-p (wal-namespace-file-stat
                           (timeline-history-path (getf state :promotion-child) *pgdata*) t)))
          (unless (and (zerop (hash-table-count *wal-page-cache*)) (null *wal-dirty-store*)
                       (null *wal-page-batch*) (null *active-xids*))
            (error "child checkpoint requires an empty application write cache"))
          (check)
          (unwind-protect
               (progn
                 (physical-promotion-stage-child-checkpoint
                  state plan prefix files history-p #'check (lambda () (physical-promotion-sync-child-data context))
                  :context context :install-prefix (not (null owner)))
                 (physical-promotion-save-child-control
                  plan :context context :check
                  (lambda (&optional committed) (check (or committed :staged))))
                 (check t)
                 (setf finished t)
                 (getf plan :end))
            (unless finished (setf *physical-recovery-failed-p* t))))))))

(defun physical-promotion-publish-child-control ()
  "Commit a pending promotion while writer admission remains closed.
Require owned promotion exclusion, the exact saved generation and an empty
application write cache. Validate staging, sync history and planned WAL, then
sync database files before publishing child control. Return the checkpoint end.

This function does not initialize the primary or clear recovery exclusion.
An error or nonlocal exit after publication starts fences this process; restart
must determine from durable control whether parent or child is authoritative."
  (with-physical-promotion-exclusion (3)
    (physical-promotion-publish-child-control-core)))
