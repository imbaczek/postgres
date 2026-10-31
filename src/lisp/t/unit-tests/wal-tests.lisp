;;;; wal-tests.lisp -- write-ahead log tests.

(in-package #:pg)

(defun wal-test-encode-versioned-record (type xid start payload version)
  (let ((record (wal-encode-record type xid start payload)))
    (wal-set-u16 record 4 version)
    (wal-set-u32 record 28 0)
    (wal-set-u32 record 28 (wal-crc32 record))
    record))

(defun test-wal-v3-maintenance-record-replay ()
  ;; WAL v3 already used maintenance terminals; v4 only extends page images.
  ;; Exercise complete headers, checksums, decoding, and terminal replay, like
  ;; PostgreSQL's xlogreader/recovery path must do across a format upgrade.
  (let* ((*wal-checkpoint-lsn* 0)
         (*wal-recovery-group-active* nil)
         (*wal-recovery-page-images* '())
         (*wal-recovery-catalog-chunks* '())
         (*wal-recovery-catalog-chunk-count* nil)
         (*wal-recovery-catalog-next-chunk* 0)
         (empty (make-array 0 :element-type '(unsigned-byte 8)))
         (batch-bytes
           (wal-test-encode-versioned-record
            +wal-record-batch-start+ 0 0 empty +wal-version-v3+))
         (terminal-bytes
           (wal-test-encode-versioned-record
            +wal-record-maintenance+ 0 +wal-header-size+ empty +wal-version-v3+))
         (batch (wal-decode-record batch-bytes 0))
         (terminal
           (wal-decode-record terminal-bytes +wal-header-size+)))
    (wal-replay-record batch)
    (wal-replay-record terminal)
    (check (and (= (wal-record-version terminal) +wal-version-v3+)
                (= (wal-record-type terminal) +wal-record-maintenance+)
                (not *wal-recovery-group-active*))
           "recovery decodes and replays a complete v3 maintenance terminal")))

(defun wal-test-append-committed-xid-batch (xid &optional (contributors (list xid)))
  (let* ((batch-start (wal-append-batch-start))
         (page (page-init (make-page)))
         (image (make-wal-page-image :relfilenode (+ 7400 xid) :block 0
                                     :page page :xids contributors)))
    (dolist (contributor contributors)
      (wal-slot-record-xid-batch-under-wal-lock contributor batch-start))
    (wal-append-page-images (list image))
    (let ((commit-end
            (wal-append-record +wal-record-commit+ xid
                               (make-array 0 :element-type '(unsigned-byte 8)))))
      (wal-slot-record-xid-commit-under-wal-lock xid commit-end)
      (values batch-start commit-end))))

(defun test-wal-replication-slot-acknowledgment-progress ()
  (let* ((test-pgdata (merge-pathnames "wal-slot-ack-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*wal-ready* t)
         (*max-slot-wal-keep-bytes* nil)
         (*clog-stream* nil)
         (*active-xids* '(7201 7202))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (original-scan (symbol-function 'wal-scan))
         (scan-starts '())
         (slot (make-wal-replication-slot :name "ack_slot"
                                          :restart-lsn 0 :consistent-lsn 0
                                          :confirmed-flush-lsn 0))
         (first-start nil)
         (first-end nil)
         (second-end nil))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (setf (gethash "ack_slot" *wal-replication-slots*) slot)
    (wal-save-replication-slot slot)
    (setf (symbol-function 'wal-scan)
          (lambda (function &optional scan-from-lsn)
            (push scan-from-lsn scan-starts)
            (funcall original-scan function scan-from-lsn)))
    (unwind-protect
         (progn
           (multiple-value-bind (start end)
               (wal-test-append-committed-xid-batch 7201 '(7201 7202))
             (check (zerop start) "first transaction starts at retained WAL origin")
             (setf first-start start first-end end))
           (wal-flush)
           (check (handler-case
                      (progn
                        (wal-acknowledge-replication-slot "ack_slot"
                                                          (1- first-end))
                        nil)
                    (error () t))
                  "slot acknowledgment rejects an LSN inside a WAL record")
           (wal-acknowledge-replication-slot "ack_slot" first-end)
           (check (and (= (wal-replication-slot-confirmed-flush-lsn slot) first-end)
                       (= (wal-replication-slot-restart-lsn slot) first-start))
                  "slot acknowledgment advances while an older transaction pins WAL")
           (multiple-value-bind (start end)
               (wal-test-append-committed-xid-batch 7202)
             (declare (ignore start))
             (setf second-end end))
           (wal-flush)
           (let ((record-ends '())
                 (scanned-end nil))
             (setf scanned-end
                   (wal-scan (lambda (record)
                               (push (wal-record-end record) record-ends)
                               (when (= (wal-record-end record) first-end)
                                 :stop))
                             first-start)
                   record-ends (nreverse record-ends))
             (check (= scanned-end first-end)
                    "WAL scan returns the target LSN after an early stop")
             (check (= (car (last record-ends)) first-end)
                    "WAL scan does not visit records after its stop boundary"))
           (setf *wal-replication-slots* (make-hash-table :test #'equal))
           (wal-load-replication-slots)
           (setf slot (gethash "ack_slot" *wal-replication-slots*))
           (check (and (= (wal-replication-slot-confirmed-flush-lsn slot) first-end)
                       (equal (gethash 7202
                                       (wal-replication-slot-pending-xids slot))
                              (cons first-start second-end)))
                  "startup rebuilds unacknowledged transaction progress from v4 WAL")
           (check (and (= (wal-replication-slot-confirmed-flush-lsn slot) first-end)
                       (= (wal-replication-slot-restart-lsn slot) first-start))
                  "an older active transaction pins restart WAL behind confirmed progress")
           (check (handler-case
                      (progn
                        (wal-acknowledge-replication-slot "ack_slot"
                                                          (1- first-end))
                        nil)
                    (error () t))
                  "slot acknowledgment cannot move backwards")
           (setf scan-starts '())
           (wal-acknowledge-replication-slot "ack_slot" second-end)
           (check (and (= (wal-replication-slot-confirmed-flush-lsn slot) second-end)
                       (= (wal-replication-slot-restart-lsn slot) second-end)
                       (zerop (hash-table-count
                               (wal-replication-slot-pending-xids slot))))
                  "acknowledging the next batch releases its transaction WAL pin")
           (check (equal scan-starts (list first-end))
                  "slot acknowledgment scans forward from confirmed progress"))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (setf (symbol-function 'wal-scan) original-scan)
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-replication-slot-retention-cap-invalidates-pin ()
  (let* ((test-pgdata (merge-pathnames "wal-slot-retention-cap-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) 100))
         (*wal-poisoned* nil)
         (*max-slot-wal-keep-bytes* 32)
         (*startup-installed-configuration*
           (let ((native (make-hash-table :test #'equal))
                 (*physical-follow-desired* nil) (*physical-recovery-state* nil))
             (setf (gethash "max-slot-wal-keep-bytes" native) "32")
             (startup-prepare-installation-from-base
              (startup-default-settings test-pgdata) native (make-hash-table :test #'equal)
              nil nil t)))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (slot (make-wal-replication-slot :name "lag_slot" :restart-lsn 0
                                          :consistent-lsn *wal-durable-lsn*)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (unwind-protect
         (progn
           (dolist (segment '(0 1 2))
             (with-open-file (stream (wal-segment-path segment)
                                     :direction :output
                                     :element-type '(unsigned-byte 8)
                                     :if-exists :supersede
                                     :if-does-not-exist :create)
               (write-byte 0 stream)))
           (setf (gethash "lag_slot" *wal-replication-slots*) slot)
           (wal-save-replication-slot slot)
           (wal-remove-checkpointed-segments *wal-durable-lsn*)
           (let ((row (first (wal-replication-slot-rows))))
             (check (and (eq (wal-replication-slot-wal-status slot) :lost)
                         (equal (aref row 5) "lost")
                         (zerop (aref row 4)))
                    "over-limit slot is invalidated and reports released retention"))
           (check (and (not (probe-file (wal-segment-path 0)))
                       (not (probe-file (wal-segment-path 1)))
                       (probe-file (wal-segment-path 2)))
                  "checkpoint releases old WAL after persisting slot invalidation"))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-snapshot-boundary-retains-active-xid-pages ()
  (let* ((*backend* (make-backend :xid 4101))
         (*next-xid* 5000)
         (*active-xids* '(4101 4102))
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-end-lsn* 900)
         (*wal-durable-lsn* 900)
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (registration (make-snapshot-registration)))
    ;; A page batch can contain writes by several transactions. Keep each
    ;; active contributor's first batch start, even if later batches include it
    ;; again. This parallels the transaction state retained by PostgreSQL's
    ;; SnapBuild machinery until a commit record closes that XID.
    (wal-record-page-xids-at-batch-start
     (list (make-wal-page-image :xids '(4101 4999))) 250)
    (wal-record-page-xids-at-batch-start
     (list (make-wal-page-image :xids '(4101 4102))) 350)
    (check (= (gethash 4101 *wal-xid-first-lsn*) 250)
           "WAL retention keeps the first batch containing an active XID")
    (check (= (gethash 4102 *wal-xid-first-lsn*) 350)
           "WAL retention records each active page contributor")
    (check (not (gethash 4999 *wal-xid-first-lsn*))
           "WAL retention ignores contributors that have already left the active set")
    (multiple-value-bind (snapshot boundary-lsn restart-lsn)
        (with-write-lock
          (wal-capture-snapshot-boundary-under-writer-lock 7 registration))
      (check (and (= boundary-lsn 900) (= restart-lsn 250))
             "snapshot export pairs the durable boundary with its oldest active-XID WAL")
      (check (= (gethash registration *wal-snapshot-retention-pins*) 250)
             "registered snapshot pins its returned WAL restart position")
      (check (and (= (snapshot-xmax snapshot) 5000)
                  (equal (snapshot-xip snapshot) '(4101 4102)))
             "snapshot boundary captures the active XID horizon under the WAL lock"))
    (release-xid 4101)
    (check (not (gethash 4101 *wal-xid-first-lsn*))
           "ending an XID releases its retained WAL position")
    (check (= (gethash registration *wal-snapshot-retention-pins*) 250)
           "snapshot restart WAL remains pinned after an XID ends")
    (release-snapshot-registration registration)
    (check (and (zerop *active-snapshots*)
                (not (gethash registration *wal-snapshot-retention-pins*)))
           "releasing the snapshot also releases its WAL retention pin")))

(defun call-with-wal-snapshot-release-race-state (function)
  (let* ((test-lock (sb-thread:make-mutex :name "snapshot boundary race test"))
         (test-cv (sb-thread:make-waitqueue :name "snapshot boundary race test"))
         (registration (make-snapshot-registration))
         (saved-active-snapshots *active-snapshots*)
         (saved-active-snapshot-set *active-snapshot-set*)
         (saved-active-xids *active-xids*)
         (saved-next-xid *next-xid*)
         (saved-wal-ready *wal-ready*)
         (saved-wal-poisoned *wal-poisoned*)
         (saved-wal-end-lsn *wal-end-lsn*)
         (saved-wal-durable-lsn *wal-durable-lsn*)
         (saved-wal-xid-first-lsn *wal-xid-first-lsn*)
         (saved-wal-snapshot-retention-pins *wal-snapshot-retention-pins*)
         (saved-hook *wal-test-after-snapshot-boundary-registration-hook*)
         (saved-release-hook *wal-test-after-snapshot-release-mark-hook*))
    ;; The capture and release workers share process-global snapshot state.
    (setf *active-snapshots* 0
          *active-snapshot-set* (make-hash-table :test #'eq)
          *active-xids* nil
          *next-xid* 9000
          *wal-ready* t
          *wal-poisoned* nil
          *wal-end-lsn* 800
          *wal-durable-lsn* 800
          *wal-xid-first-lsn* (make-hash-table :test #'eql)
          *wal-snapshot-retention-pins* (make-hash-table :test #'eq)
          *wal-test-after-snapshot-boundary-registration-hook* nil
          *wal-test-after-snapshot-release-mark-hook* nil)
    (unwind-protect
         (funcall function test-lock test-cv registration)
      (setf *wal-test-after-snapshot-boundary-registration-hook* nil
            *wal-test-after-snapshot-release-mark-hook* nil)
      (release-snapshot-registration registration)
      (setf *active-snapshots* saved-active-snapshots
            *active-snapshot-set* saved-active-snapshot-set
            *active-xids* saved-active-xids
            *next-xid* saved-next-xid
            *wal-ready* saved-wal-ready
            *wal-poisoned* saved-wal-poisoned
            *wal-end-lsn* saved-wal-end-lsn
            *wal-durable-lsn* saved-wal-durable-lsn
            *wal-xid-first-lsn* saved-wal-xid-first-lsn
            *wal-snapshot-retention-pins* saved-wal-snapshot-retention-pins
            *wal-test-after-snapshot-boundary-registration-hook* saved-hook
            *wal-test-after-snapshot-release-mark-hook* saved-release-hook))))

(defun test-wal-snapshot-release-race-before-pin-publication ()
  (call-with-wal-snapshot-release-race-state
   (lambda (test-lock test-cv registration)
     (let ((hook-entered nil)
           (resume-capture nil)
           (capture-error nil)
           (capture-result nil)
           (worker nil))
       (setf *wal-test-after-snapshot-boundary-registration-hook*
             (lambda (token)
               (declare (ignore token))
               (sb-thread:with-mutex (test-lock)
                 (setf hook-entered t)
                 (sb-thread:condition-broadcast test-cv)
                 (loop until resume-capture
                       do (sb-thread:condition-wait test-cv test-lock)))))
       (unwind-protect
            (progn
              (setf worker
                    (sb-thread:make-thread
                     (lambda ()
                       (handler-case
                           (progn
                             (with-write-lock
                               (wal-capture-snapshot-boundary-under-writer-lock
                                0 registration))
                             (setf capture-result :returned))
                         (error (condition)
                           (setf capture-error (princ-to-string condition)))))
                     :name "snapshot boundary release race"))
              (let ((entered
                      (test-wait-until
                       (lambda ()
                         (sb-thread:with-mutex (test-lock) hook-entered))
                       2.0)))
                (check entered "snapshot boundary test pauses after registration")
                (when entered
                  (release-snapshot-registration registration)
                  (check (and (zerop *active-snapshots*)
                              (not (snapshot-registration-active-p registration)))
                         "concurrent release clears the registered snapshot before pin publication"))
                (sb-thread:with-mutex (test-lock)
                  (setf resume-capture t)
                  (sb-thread:condition-broadcast test-cv)))
              (sb-thread:join-thread worker)
              (setf worker nil)
              (check (and (null capture-result)
                          capture-error
                          (search "snapshot registration was released" capture-error))
                     "boundary capture rejects a registration released before pin publication")
              (check (not (gethash registration *wal-snapshot-retention-pins*))
                     "a released registration cannot leave a WAL retention pin")
              (check (zerop *active-snapshots*)
                     "snapshot release racing boundary capture keeps the active count balanced"))
         (sb-thread:with-mutex (test-lock)
           (setf resume-capture t)
           (sb-thread:condition-broadcast test-cv))
         (when worker (sb-thread:join-thread worker)))))))

(defun test-wal-snapshot-release-race-before-reuse ()
  (call-with-wal-snapshot-release-race-state
   (lambda (test-lock test-cv registration)
     (let ((first-release-entered nil)
           (second-release-entered nil)
           (resume-first-release nil)
           (hook-calls 0)
           (first-error nil)
           (second-error nil)
           (old-generation nil)
           (old-pin nil)
           (old-state
             (make-exported-logical-snapshot :importers (list :old-importer)))
           (new-state nil)
           (first-worker nil)
           (second-worker nil))
       (with-write-lock
         (wal-capture-snapshot-boundary-under-writer-lock 0 registration))
       (setf old-generation
             (snapshot-registration-lifecycle-generation registration)
             old-pin (snapshot-registration-wal-retention-lsn registration)
             *wal-test-after-snapshot-release-mark-hook*
             (lambda (token)
               (declare (ignore token))
               (sb-thread:with-mutex (test-lock)
                 (incf hook-calls)
                 (if (= hook-calls 1)
                     (progn
                       (setf first-release-entered t)
                       (sb-thread:condition-broadcast test-cv)
                       (loop until resume-first-release
                             do (sb-thread:condition-wait test-cv test-lock)))
                     (setf second-release-entered t))
                 (sb-thread:condition-broadcast test-cv))))
       (register-exported-logical-snapshot-importer
        old-state registration :old-importer)
       (setf first-worker
             (sb-thread:make-thread
              (lambda ()
                (handler-case
                    (release-snapshot-registration registration)
                  (error (condition)
                    (setf first-error (princ-to-string condition)))))
              :name "first snapshot release"))
       (unwind-protect
            (progn
              (check (test-wait-until
                      (lambda ()
                        (sb-thread:with-mutex (test-lock) first-release-entered))
                      2.0)
                     "first releaser pauses after capturing its generation")
              ;; Hold WAL while the retry captures the same old generation;
              ;; then the first releaser stays paused while the retry retires it.
              (sb-thread:with-mutex (*wal-lock*)
                (setf second-worker
                      (sb-thread:make-thread
                       (lambda ()
                         (handler-case
                             (release-snapshot-registration registration)
                           (error (condition)
                             (setf second-error (princ-to-string condition)))))
                       :name "retry snapshot release"))
                (check (test-wait-until
                        (lambda ()
                          (sb-thread:with-mutex (test-lock)
                            second-release-entered))
                        2.0)
                       "retry captures the old generation while waiting for WAL"))
              (when second-worker
                (sb-thread:join-thread second-worker)
                (setf second-worker nil))
              (check (null second-error) "retry completes the old release")
              (check (and (not (gethash registration
                                        *wal-snapshot-retention-pins*))
                          (not (snapshot-registration-releasing-p registration))
                          (zerop *active-snapshots*))
                     "retry clears the old pin before registration reuse")
              (check (and (null (snapshot-registration-exported-state registration))
                          (null (exported-logical-snapshot-importers old-state)))
                     "release detaches the old importer before allowing reuse")
              (with-write-lock
                (wal-capture-snapshot-boundary-under-writer-lock 0 registration))
              (setf new-state
                    (make-exported-logical-snapshot :importers '()))
              (register-exported-logical-snapshot-importer
               new-state registration :new-importer)
              (check (and (> (snapshot-registration-lifecycle-generation
                              registration)
                             old-generation)
                          (= (snapshot-registration-wal-retention-lsn registration)
                             old-pin))
                     "reuse advances the generation even when the new pin has the same LSN")
              (sb-thread:with-mutex (test-lock)
                (setf resume-first-release t)
                (sb-thread:condition-broadcast test-cv))
              (sb-thread:join-thread first-worker)
              (setf first-worker nil)
              (check (null first-error) "delayed old releaser completes harmlessly")
              (check (and (snapshot-registration-active-p registration)
                          (= (gethash registration
                                      *wal-snapshot-retention-pins*)
                             old-pin)
                          (= *active-snapshots* 1))
                     "delayed old release preserves the new snapshot's WAL pin")
              (check (and (eq (snapshot-registration-exported-state registration)
                              new-state)
                          (equal (exported-logical-snapshot-importers new-state)
                                 '(:new-importer)))
                     "delayed old cleanup preserves the new importer association"))
         (sb-thread:with-mutex (test-lock)
           (setf resume-first-release t)
           (sb-thread:condition-broadcast test-cv))
         (when first-worker (sb-thread:join-thread first-worker))
         (when second-worker (sb-thread:join-thread second-worker)))))))

(defun test-wal-chunked-catalog-record ()
  (let* ((test-pgdata (merge-pathnames "wal-catalog-chunks-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-ready* t)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-pending-deletes* '())
         (*wal-pending-btree-split* nil)
         (*wal-test-pause-at* nil)
         (catalog-bytes
           (make-array (+ (- +wal-segment-size+ +wal-header-size+) 1)
                       :element-type '(unsigned-byte 8)
                       :initial-element 65))
         (catalog-written nil)
         (record-types '()))
    ;; Remove artifacts left by an interrupted prior run that reused the same
    ;; process-local randomized test directory.
    (ignore-errors
     (uiop:delete-directory-tree test-pgdata :validate t
                                             :if-does-not-exist :ignore))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (wal-commit-catalog
            catalog-bytes
            (lambda (bytes)
              (setf catalog-written (equalp bytes catalog-bytes))))
           (check catalog-written
                  "oversized catalog image is published after WAL flush")
           (wal-scan (lambda (record)
                       (push (wal-record-type record) record-types)))
           (setf record-types (nreverse record-types))
           (check (and (= (count +wal-record-catalog-chunk+ record-types) 2)
                       (= (car (last record-types))
                          +wal-record-catalog-commit+))
                  "oversized catalog image uses chunks and a terminal record")
           (let ((*wal-recovery-group-active* nil)
                 (*wal-recovery-page-images* '())
                 (*wal-recovery-catalog-chunks* '())
                 (*wal-recovery-catalog-chunk-count* nil)
                 (*wal-recovery-catalog-next-chunk* 0))
             (wal-scan #'wal-replay-record))
           (with-open-file (stream (merge-pathnames "global/pg_catalog.dat"
                                                    *pgdata*)
                                   :direction :input
                                   :element-type '(unsigned-byte 8))
             (let ((recovered (make-array (file-length stream)
                                          :element-type '(unsigned-byte 8))))
               (read-sequence recovered stream)
               (check (equalp recovered catalog-bytes)
                      "chunked catalog WAL reassembles during recovery")))
           (let ((*wal-recovery-catalog-chunks*
                   (list (make-array 1 :element-type '(unsigned-byte 8))))
                 (*wal-recovery-catalog-chunk-count* 1)
                 (*wal-recovery-catalog-next-chunk* 1))
             (check (handler-case
                        (progn
                          (wal-reassemble-catalog-chunks 1 #xffffffffffffffff)
                          nil)
                      (error () t))
                    "catalog chunk length mismatch fails before allocation")))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-catalog-terminal-validation-precedes-page-redo ()
  (let* ((test-pgdata
           (merge-pathnames "wal-invalid-catalog-terminal-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*relation-streams* (make-hash-table :test #'equal))
         (*wal-ready* t)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (relfilenode 424242)
         (page-before (page-init (make-page)))
         (page-after (let ((page (page-init (make-page))))
                       (setf (aref page 100) #x5A)
                       page))
         (images (list (make-wal-page-image :relfilenode relfilenode
                                            :block 0 :page page-after)))
         (catalog-chunk
           (make-array 1 :element-type '(unsigned-byte 8) :initial-element 65))
         (batch-bytes
           (wal-encode-record +wal-record-batch-start+ 0 0 #()))
         (batch-record (wal-decode-record batch-bytes 0))
         (pages-start (wal-record-end batch-record))
         (pages-end (+ pages-start +wal-header-size+
                       (wal-page-payload-length images)))
         (pages-payload (wal-encode-page-images images pages-end))
         (pages-bytes
           (wal-encode-record +wal-record-pages+ 0 pages-start pages-payload))
         (pages-record (wal-decode-record pages-bytes pages-start))
         (chunk-start (wal-record-end pages-record))
         (chunk-payload (wal-encode-catalog-chunk-payload
                         catalog-chunk 0 1))
         (chunk-bytes
           (wal-encode-record +wal-record-catalog-chunk+ 0 chunk-start
                              chunk-payload))
         (chunk-record (wal-decode-record chunk-bytes chunk-start))
         (terminal-start (wal-record-end chunk-record))
         (terminal-payload
           (wal-encode-catalog-commit-payload
            (make-array 2 :element-type '(unsigned-byte 8) :initial-element 65)
            1))
         (terminal-bytes
           (wal-encode-record +wal-record-catalog-commit+ 0 terminal-start
                              terminal-payload))
         (terminal-record (wal-decode-record terminal-bytes terminal-start))
         (end-lsn (wal-record-end terminal-record)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (merge-pathnames "base/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (ensure-directories-exist (relation-file-path relfilenode))
    (smgr-write relfilenode 0 page-before)
    (with-open-file (stream (wal-segment-path 0)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (dolist (bytes (list batch-bytes pages-bytes chunk-bytes terminal-bytes))
        (write-sequence bytes stream)))
    (setf *wal-durable-lsn* end-lsn)
    (wal-save-control-fields 0 end-lsn)
    (unwind-protect
         (progn
           ;; Each record was decoded after encoding, so its CRC is valid. The
           ;; terminal declares two catalog bytes although its only chunk has
           ;; one; recovery must reject it before writing the staged page.
           (let ((error-message
                   (handler-case
                       (progn (wal-recover) nil)
                     (error (condition) (princ-to-string condition)))))
             (check (and error-message
                         (search "catalog chunks do not match their declared length"
                                 error-message))
                    "malformed chunked catalog terminal fails recovery")
             (check (equalp (smgr-read relfilenode 0) page-before)
                    "invalid catalog terminal leaves relation pages unchanged")))
      (smgr-close-all)
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun wal-test-legacy-checkpoint-record (start)
  (let ((bytes (wal-encode-record +wal-record-checkpoint+ 0 start #())))
    (wal-set-u16 bytes 4 5)
    (wal-set-u32 bytes 28 0)
    (wal-set-u32 bytes 28 (wal-crc32 bytes))
    bytes))

(defun test-wal-segment-rotation ()
  (let* ((test-pgdata (merge-pathnames "wal-segment-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*wal-checkpoint-snapshot* nil)
         (*next-xid* 3) (*xid-batch-end* 3) (*active-xids* nil)
         (checkpoint-snapshot nil)
         (directory-synced-on-rotation nil)
         (original-fsync-directory (symbol-function 'fsync-directory)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-directory)
                 (lambda (directory)
                   (when (eql *wal-output-segment* 0)
                     (setf directory-synced-on-rotation t))
                   (funcall original-fsync-directory directory)))
           (wal-append-record
            +wal-record-commit+ 42
            (make-array (- +wal-segment-size+ +wal-header-size+)
                        :element-type '(unsigned-byte 8)
                        :initial-element 0))
           (setf checkpoint-snapshot
                 (wal-capture-checkpoint-snapshot (wal-next-record-start (wal-checkpoint-record-size))))
           (wal-append-record +wal-record-checkpoint+ 0
                              (wal-encode-checkpoint-snapshot checkpoint-snapshot))
           (check directory-synced-on-rotation
                  "WAL rotation syncs segment names before opening the next segment")
           (wal-flush)
           (let ((records 0))
             (multiple-value-bind (end-lsn segments)
                 (wal-scan (lambda (record)
                             (declare (ignore record))
                             (incf records)))
               (check (= records 2) "WAL scanner reads records across a segment boundary")
               (check (= end-lsn *wal-end-lsn*) "WAL scan ends at the last segment LSN")
               (check (equal segments '(0 1)) "WAL scan discovers contiguous segments")
               (wal-save-checkpoint-control end-lsn checkpoint-snapshot)
               (wal-remove-checkpointed-segments end-lsn)
               (check (not (probe-file (wal-segment-path 0)))
                      "checkpoint recycling removes older WAL segments")
               (check (probe-file (wal-segment-path 1))
                      "checkpoint recycling retains the checkpoint segment")
               (multiple-value-bind (retained-end retained-segments)
                   (wal-scan (lambda (record) (declare (ignore record))))
                 (check (= retained-end end-lsn)
                        "WAL recovery scans from the retained checkpoint segment")
                 (check (equal retained-segments '(1))
                        "WAL recovery accepts a retained checkpoint suffix"))))
           (setf (symbol-function 'fsync-directory) original-fsync-directory)
           (when *wal-output-stream*
             (ignore-errors (close *wal-output-stream*)))))))

(defun test-wal-checkpoint-retains-active-xid-segments ()
  (let* ((test-pgdata (merge-pathnames "wal-active-xid-retention-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*active-xids* '(6401 6402))
         (*next-xid* 6403)
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-ready* t)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) 100))
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*wal-poisoned* nil)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (checkpoint-lsn (+ (* 2 +wal-segment-size+) 100))
         (registration (make-snapshot-registration)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (dolist (segment '(0 1 2))
             (with-open-file (stream (wal-segment-path segment)
                                     :direction :output
                                     :element-type '(unsigned-byte 8)
                                     :if-exists :supersede
                                     :if-does-not-exist :create)
               (write-byte 0 stream)))
           (wal-record-page-xids-at-batch-start
            (list (make-wal-page-image :xids '(6401)))
            (+ +wal-segment-size+ +wal-header-size+))
           (wal-record-page-xids-at-batch-start
            (list (make-wal-page-image :xids '(6402))) +wal-header-size+)
           (with-write-lock
             (wal-capture-snapshot-boundary-under-writer-lock 0 registration))
           (release-xid 6402)
           (check (not (gethash 6402 *wal-xid-first-lsn*))
                  "completed XID no longer owns the active-WAL pin")
           (wal-remove-checkpointed-segments checkpoint-lsn)
           (check (every (lambda (segment)
                           (probe-file (wal-segment-path segment)))
                         '(0 1 2))
                  "checkpoint retains snapshot WAL after its XID ends")
           (release-snapshot-registration registration)
           (wal-remove-checkpointed-segments checkpoint-lsn)
           (check (and (not (probe-file (wal-segment-path 0)))
                       (probe-file (wal-segment-path 1))
                       (probe-file (wal-segment-path 2)))
                  "checkpoint advances after the snapshot releases its WAL pin")
           (release-xid 6401)
           (wal-remove-checkpointed-segments checkpoint-lsn)
           (check (and (not (probe-file (wal-segment-path 1)))
                       (probe-file (wal-segment-path 2)))
                  "checkpoint retains the checkpoint segment after XID WAL is released"))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-replication-slot-durable-retention ()
  (let* ((test-pgdata (merge-pathnames "wal-replication-slot-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*active-xids* '(6401))
         (*next-xid* 6402)
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) 100))
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*logical-capture-xid-modes* (make-hash-table :test #'eql))
         (*logical-capture-live-slots* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (checkpoint-lsn *wal-durable-lsn*)
         (registration (make-snapshot-registration))
         (slot-owner (gensym "COPY-SLOT-"))
         (snapshot nil)
         (boundary-lsn nil)
         (restart-lsn nil)
         (slot nil))
    (setf (gethash 6401 *wal-xid-first-lsn*) 100)
    (check (and (wal-valid-replication-slot-name-p "copy_slot")
                (not (wal-valid-replication-slot-name-p "Copy_slot")))
           "replication slot names follow PostgreSQL's lowercase name rules")
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (unwind-protect
         (progn
           (dolist (segment '(0 1 2))
             (with-open-file (stream (wal-segment-path segment)
                                     :direction :output
                                     :element-type '(unsigned-byte 8)
                                     :if-exists :supersede
                                     :if-does-not-exist :create)
               (write-byte 0 stream)))
           (let ((*backend* (make-backend)))
             (call-with-logical-capture-armed
              (lambda ()
                (with-write-lock
                  (multiple-value-setq (snapshot boundary-lsn restart-lsn slot)
                    (wal-create-replication-slot-under-writer-lock
                     "copy_slot" 7 registration slot-owner))))))
           (check (and snapshot (= boundary-lsn checkpoint-lsn)
                       (= restart-lsn 100)
                       (= (wal-replication-slot-consistent-lsn slot)
                          checkpoint-lsn))
                  "slot creation pairs a snapshot boundary with the oldest active-XID WAL")
           (check (and (probe-file (wal-replication-slot-path "copy_slot"))
                       (eq slot (gethash "copy_slot" *wal-replication-slots*)))
                  "slot reserves a WAL pin and finishes durable publication")
           ;; This fixture has no transaction records, so finalize its
           ;; consistent point before exercising startup metadata loading.
           (check (handler-case
                      (progn
                        (wal-acknowledge-replication-slot
                         "copy_slot" boundary-lsn)
                        nil)
                    (error () t))
                  "an unconfirmed slot rejects unowned low-level acknowledgment")
           (check (handler-case
                      (progn
                        (wal-finalize-replication-slot-copy
                         "copy_slot" boundary-lsn (gensym "WRONG-OWNER-"))
                        nil)
                    (error () t))
                  "initial finalization rejects a different slot owner")
           (check (null (wal-replication-slot-confirmed-flush-lsn slot))
                  "a rejected owner cannot publish the initial boundary")
           (setf (wal-replication-slot-wal-status slot) :lost)
           (check (handler-case
                      (progn
                        (wal-finalize-replication-slot-copy
                         "copy_slot" boundary-lsn slot-owner)
                        nil)
                    (error () t))
                  "initial finalization rejects a slot whose WAL was lost")
           (check (null (wal-replication-slot-confirmed-flush-lsn slot))
                  "lost-WAL finalization leaves the slot unconfirmed")
           (setf (wal-replication-slot-wal-status slot) :reserved)
           (wal-finalize-replication-slot-copy
            "copy_slot" boundary-lsn slot-owner)
           (wal-release-replication-slot-owner "copy_slot" slot-owner)
           ;; A restart reconstructs slot pins from durable metadata before
           ;; checkpoint recycling is allowed to run.
           (setf *wal-replication-slots* (make-hash-table :test #'equal))
           (wal-load-replication-slots)
           (check (= (wal-replication-slot-restart-lsn
                      (gethash "copy_slot" *wal-replication-slots*))
                     100)
                  "startup reloads the durable replication slot restart point")
           (release-xid 6401)
           (release-snapshot-registration registration)
           (wal-remove-checkpointed-segments checkpoint-lsn)
           (check (every (lambda (segment)
                           (probe-file (wal-segment-path segment)))
                         '(0 1 2))
                  "checkpoint retains slot WAL after its captured XID and snapshot end")
           (wal-drop-replication-slot "copy_slot")
           (wal-remove-checkpointed-segments checkpoint-lsn)
           (check (and (not (probe-file (wal-segment-path 0)))
                       (not (probe-file (wal-segment-path 1)))
                       (probe-file (wal-segment-path 2))
                       (not (probe-file (wal-replication-slot-path "copy_slot"))))
                  "dropping a slot durably releases its WAL retention point"))
      (when (snapshot-registration-active-p registration)
        (release-snapshot-registration registration))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-with-exported-snapshot-scope ()
  (let* ((test-pgdata (merge-pathnames "exported-snapshot-scope-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*catalog* (make-catalog))
         (*backend* (make-backend))
         (database (make-database :name "postgres" :oid 1))
         (*active-xids* nil)
         (*next-xid* 9000)
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 1000)
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (snapshot nil)
         (start-lsn nil))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) database)
    (unwind-protect
         (progn
           (multiple-value-setq (snapshot start-lsn)
             (with-exported-snapshot (captured captured-start
                                               :slot-name "base_copy")
               (check (and (eq (backend-state *backend*) :in-progress)
                           (backend-explicit *backend*)
                           (eq (backend-isolation-level *backend*)
                               :repeatable-read)
                           (backend-read-only *backend*))
                      "export scope owns a read-only repeatable-read transaction")
               (check (and (eq captured
                               (backend-transaction-snapshot *backend*))
                           (eq captured
                               (snapshot-registration-snapshot
                                (backend-transaction-snapshot-registration
                                 *backend*)))
                           (eq captured *current-snapshot*))
                      "export scope installs the registered snapshot for copy reads")
               (check (sb-thread:with-mutex (*schema-lock-mutex*)
                        (plusp *schema-read-lock-count*))
                      "export scope holds the schema read gate")
               (check (handler-case
                          (progn (commit-transaction) nil)
                        (error () t))
                      "callers cannot end the retained transaction inside the scope")
               (check (handler-case
                          (progn
                            (with-schema-write-lock t)
                            nil)
                        (error () t))
                      "schema changes cannot upgrade the exported read gate")
               (check (handler-case
                          (progn (wal-drop-replication-slot "base_copy") nil)
                        (error () t))
                      "an active exported-copy slot cannot be dropped")
               (check (gethash "base_copy" *wal-replication-slots*)
                      "a rejected active-slot drop retains its WAL pin")
               (values captured captured-start)))
           (check (and snapshot (= start-lsn 1000)
                       (eq (backend-state *backend*) :idle)
                       (null (backend-transaction-snapshot *backend*))
                       (null (wal-replication-slot-active-owner
                              (gethash "base_copy" *wal-replication-slots*)))
                       (zerop *active-snapshots*))
                  "export scope releases the snapshot and slot claim after the copy")
           (check (gethash "base_copy" *wal-replication-slots*)
                  "successful export leaves a durable WAL slot behind")
           (wal-drop-replication-slot "base_copy")
           (check (null (gethash "base_copy" *wal-replication-slots*))
                  "caller can release the completed export slot")
           (check (handler-case
                      (progn
                        (with-exported-snapshot (failed-snapshot failed-start
                                                                 :slot-name "failed_copy")
                          (declare (ignore failed-snapshot failed-start))
                          (error "injected initial-copy failure"))
                        nil)
                    (error () t))
                  "copy errors propagate out of the export scope")
           (check (and (eq (backend-state *backend*) :idle)
                       (zerop *active-snapshots*)
                       (null (wal-replication-slot-active-owner
                              (gethash "failed_copy" *wal-replication-slots*))))
                  "failed copies release their snapshot and active slot claim")
           (wal-drop-replication-slot "failed_copy"))
      (when (not (eq (backend-state *backend*) :idle))
        (ignore-errors (abort-transaction)))
      (when (gethash "base_copy" *wal-replication-slots*)
        (ignore-errors (wal-drop-replication-slot "base_copy")))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-exported-snapshot-wraparound-admission ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (database (make-database :name "postgres" :oid 1))
         (*xid-wrap-pending* t)
         (*xid-wrap-failure* nil)
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*schema-read-lock-count* 0)
         (*schema-write-lock-owners* nil)
         (*schema-write-lock-waiters* 0)
         (*wal-ready* nil)
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (original-wait (symbol-function 'condition-wait-with-timeout))
         (wait-called nil))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) database)
    (unwind-protect
         (progn
           (setf (symbol-function 'condition-wait-with-timeout)
                 (lambda (waitqueue mutex)
                   (declare (ignore waitqueue mutex))
                   (setf wait-called t)
                   (error "injected wraparound gate wait")))
           (check (handler-case
                      (progn
                        (with-exported-snapshot (snapshot start-lsn
                                                          :slot-name "pending_copy")
                          (declare (ignore snapshot start-lsn)))
                        nil)
                    (error () t))
                  "snapshot export waits at the pending wraparound gate")
           (check wait-called
                  "export checks wraparound admission before touching WAL")
           (check (and (eq (backend-state *backend*) :idle)
                       (not (backend-wraparound-waiting *backend*))
                       (zerop *active-snapshots*)
                       (zerop *schema-read-lock-count*)
                       (zerop (hash-table-count *wal-replication-slots*)))
                  "a blocked export publishes no snapshot, lock, or slot"))
      (setf (symbol-function 'condition-wait-with-timeout) original-wait))))

(defun test-exported-snapshot-wraparound-race ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (database (make-database :name "postgres" :oid 1))
         (*xid-wrap-pending* nil)
         (*xid-wrap-failure* nil)
         (*active-xids* '())
         (*next-xid* 7000)
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*schema-read-lock-count* 0)
         (*schema-write-lock-owners* nil)
         (*schema-write-lock-waiters* 0)
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-durable-lsn* 1000)
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-test-after-snapshot-boundary-registration-hook* nil)
         (preflight-called nil)
         (wait-called nil)
         (wait-held-gate nil)
         (capture-held-gate nil)
         (publish-held-gate nil)
         (publish-saw-active-pin nil)
         (ack-called nil)
         (ack-after-snapshot-release nil)
         (finalizer-owner-held nil)
         (original-prepare
           (symbol-function 'prepare-xid-wraparound-for-statement))
         (original-wait (symbol-function 'condition-wait-with-timeout))
         (original-publish
           (symbol-function 'wal-publish-captured-replication-slot))
         (original-finalize
           (symbol-function 'wal-finalize-replication-slot-copy)))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) database)
    (unwind-protect
         (progn
           ;; Model a coordinator arriving immediately after the preflight
           ;; check, before the export has registered its retained snapshot.
           (setf (symbol-function 'prepare-xid-wraparound-for-statement)
                 (lambda (backend)
                   (declare (ignore backend))
                   (setf preflight-called t
                         *xid-wrap-pending* t)))
           (setf (symbol-function 'condition-wait-with-timeout)
                 (lambda (waitqueue mutex)
                   (declare (ignorable mutex))
                   (if (eq waitqueue *xid-wrap-cv*)
                       (progn
                         (setf wait-called t
                               wait-held-gate
                               (sb-thread:holding-mutex-p *xid-wrap-lock*)
                               *xid-wrap-pending* nil)
                         t)
                       (funcall original-wait waitqueue mutex))))
           (setf *wal-test-after-snapshot-boundary-registration-hook*
                 (lambda (registration)
                   (setf capture-held-gate
                         (and (sb-thread:holding-mutex-p *xid-wrap-lock*)
                              (snapshot-registration-active-p registration)
                              (not *xid-wrap-pending*)))))
           (setf (symbol-function 'wal-publish-captured-replication-slot)
                 (lambda (name boundary-lsn restart-lsn registration
                          &optional active-owner active-xids database-oid)
                   (declare (ignore active-xids database-oid))
                   (setf publish-held-gate
                         (sb-thread:holding-mutex-p *xid-wrap-lock*)
                         publish-saw-active-pin
                         (and (snapshot-registration-active-p registration)
                              (nth-value 1
                                         (gethash registration
                                                  *wal-snapshot-retention-pins*))))
                   (let ((slot
                           (make-wal-replication-slot
                            :name name :consistent-lsn boundary-lsn
                            :restart-lsn restart-lsn :active-owner active-owner)))
                     (setf (gethash name *wal-replication-slots*) slot)
                     slot)))
           (setf (symbol-function 'wal-finalize-replication-slot-copy)
                 (lambda (name acknowledged-lsn owner)
                   (let ((slot (gethash name *wal-replication-slots*)))
                     (setf ack-called t
                           finalizer-owner-held
                           (eq owner (wal-replication-slot-active-owner slot))
                           ack-after-snapshot-release
                           (and (= acknowledged-lsn 1000)
                                (eq (backend-state *backend*) :idle)
                                (zerop *active-snapshots*))))))
           (with-exported-snapshot (snapshot start-lsn
                                             :slot-name "race_copy")
             (check (and (eq snapshot
                             (backend-transaction-snapshot *backend*))
                         (= start-lsn 1000))
                    "export uses the snapshot and boundary captured after the gate opens"))
           (check (and preflight-called wait-called wait-held-gate)
                  "export rechecks a newly pending wraparound gate atomically")
           (check capture-held-gate
                  "snapshot registration occurs while wraparound admission is locked")
           (check (and publish-saw-active-pin (not publish-held-gate))
                  "slot publication follows gate release while the snapshot pins WAL")
           (check (and ack-called ack-after-snapshot-release
                       finalizer-owner-held)
                  "initial-copy finalization follows the copy and retains its slot owner")
           (check (and (eq (backend-state *backend*) :idle)
                       (zerop *active-snapshots*)
                       (zerop *schema-read-lock-count*)
                       (null (backend-transaction-snapshot *backend*)))
                  "export cleanup releases its registered snapshot and schema gate"))
      (setf (symbol-function 'prepare-xid-wraparound-for-statement)
            original-prepare
            (symbol-function 'condition-wait-with-timeout) original-wait
            (symbol-function 'wal-publish-captured-replication-slot)
            original-publish
            (symbol-function 'wal-finalize-replication-slot-copy)
            original-finalize)
      (when (not (eq (backend-state *backend*) :idle))
        (ignore-errors (abort-transaction))))))

(defun test-wal-replication-slot-fsync-failure-keeps-pin ()
  (let* ((test-pgdata (merge-pathnames "wal-replication-slot-fsync-test/"
                                       *pgdata*))
         (*pgdata* test-pgdata)
         (*active-xids* '(6411))
         (*next-xid* 6412)
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) 100))
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*logical-capture-xid-modes* (make-hash-table :test #'eql))
         (*logical-capture-live-slots* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (checkpoint-lsn *wal-durable-lsn*)
         (registration (make-snapshot-registration))
         (original-fsync-directory (symbol-function 'fsync-directory))
         (failed nil))
    (setf (gethash 6411 *wal-xid-first-lsn*) 100)
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (dolist (segment '(0 1 2))
      (with-open-file (stream (wal-segment-path segment)
                              :direction :output
                              :element-type '(unsigned-byte 8)
                              :if-exists :supersede
                              :if-does-not-exist :create)
        (write-byte 0 stream)))
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-directory)
                 (lambda (directory)
                   (if (equal directory (wal-replication-slot-directory))
                       (error "injected slot directory fsync failure")
                       (funcall original-fsync-directory directory))))
           (setf failed
                 (handler-case
                     (progn
                       (let ((*backend* (make-backend)))
                         (call-with-logical-capture-armed
                          (lambda ()
                            (with-write-lock
                              (wal-create-replication-slot-under-writer-lock
                               "fsync_slot" 7 registration)))))
                       nil)
                   (error () t)))
           (setf (symbol-function 'fsync-directory) original-fsync-directory)
           (check (and failed
                       (probe-file (wal-replication-slot-path "fsync_slot")))
                  "directory fsync failure occurs after the slot file rename")
           (check (gethash "fsync_slot" *wal-replication-slots*)
                  "a slot file that may survive restart keeps its WAL pin")
           (release-xid 6411)
           (release-snapshot-registration registration)
           (wal-remove-checkpointed-segments checkpoint-lsn)
           (check (every (lambda (segment)
                           (probe-file (wal-segment-path segment)))
                         '(0 1 2))
                  "checkpoint retains WAL after slot directory fsync failure"))
      (setf (symbol-function 'fsync-directory) original-fsync-directory)
      (when (snapshot-registration-active-p registration)
        (release-snapshot-registration registration))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-drop-fsync-retry-keeps-pin ()
  (let* ((test-pgdata (merge-pathnames "wal-replication-slot-drop-test/"
                                       *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-durable-lsn* 1000)
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-poisoned* nil)
         (slot (make-wal-replication-slot
                :name "drop_slot" :restart-lsn 500 :consistent-lsn 1000))
         (original-fsync-directory (symbol-function 'fsync-directory)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (wal-save-replication-slot slot)
    (setf (gethash "drop_slot" *wal-replication-slots*) slot)
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-directory)
                 (lambda (directory)
                   (if (equal directory (wal-replication-slot-directory))
                       (error "injected slot directory fsync failure")
                       (funcall original-fsync-directory directory))))
           (check (handler-case
                      (progn (wal-drop-replication-slot "drop_slot") nil)
                    (error () t))
                  "slot drop surfaces a directory fsync failure")
           (setf (symbol-function 'fsync-directory) original-fsync-directory)
           (check (and (gethash "drop_slot" *wal-replication-slots*)
                       (not (probe-file (wal-replication-slot-path "drop_slot"))))
                  "failed directory fsync retains the in-memory WAL pin")
           (wal-drop-replication-slot "drop_slot")
           (check (null (gethash "drop_slot" *wal-replication-slots*))
                  "retry syncs an earlier unlink before releasing its WAL pin")
           (check (not *wal-poisoned*)
                  "slot metadata fsync failures do not poison WAL records"))
      (setf (symbol-function 'fsync-directory) original-fsync-directory)
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-replication-slot-load-rejects-missing-wal ()
  (let* ((test-pgdata (merge-pathnames "wal-replication-slot-missing-wal-test/"
                                       *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) 100))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (slot (make-wal-replication-slot
                :name "missing_wal" :restart-lsn 100
                :consistent-lsn *wal-durable-lsn*)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (unwind-protect
         (progn
           (wal-save-replication-slot slot)
           (dolist (segment '(0 2))
             (with-open-file (stream (wal-segment-path segment)
                                     :direction :output
                                     :element-type '(unsigned-byte 8)
                                     :if-exists :supersede
                                     :if-does-not-exist :create)
               (write-byte 0 stream)))
           (check (handler-case
                      (progn (wal-load-replication-slots) nil)
                    (error () t))
                  "startup refuses a slot whose required WAL segment is missing"))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-checkpoint-skips-obsolete-prefix ()
  (let* ((test-pgdata (merge-pathnames "wal-checkpoint-scan-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* (+ (* 2 +wal-segment-size+)
                                  (* 2 +wal-header-size+)))
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+)
                               (* 3 +wal-header-size+)))
         (prefix (wal-encode-record +wal-record-commit+ 1 0 #()))
         (same-segment-prefix
           (wal-encode-record +wal-record-commit+ 3
                              (* 2 +wal-segment-size+) #()))
         (checkpoint-start (+ (* 2 +wal-segment-size+) +wal-header-size+))
         (checkpoint (wal-test-legacy-checkpoint-record checkpoint-start))
         (post-checkpoint (wal-encode-record
                           +wal-record-commit+ 2
                           (+ checkpoint-start +wal-header-size+) #()))
         (obsolete-bytes (copy-seq prefix))
         (same-segment-obsolete-bytes (copy-seq same-segment-prefix)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    ;; The gap at segment 1 and damage in segment 0 are both before the durable
    ;; checkpoint, so neither should prevent scanning the retained suffix.
    (setf (aref obsolete-bytes 28) (logxor (aref obsolete-bytes 28) #xFF))
    (setf (aref same-segment-obsolete-bytes 28)
          (logxor (aref same-segment-obsolete-bytes 28) #xFF))
    (with-open-file (stream (wal-segment-path 0)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-sequence obsolete-bytes stream))
    (with-open-file (stream (wal-segment-path 2)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-sequence same-segment-obsolete-bytes stream)
      (write-sequence checkpoint stream)
      (write-sequence post-checkpoint stream))
    (multiple-value-bind (end-lsn segments)
        (wal-scan (lambda (record) (declare (ignore record))))
      (check (= end-lsn *wal-durable-lsn*)
             "checkpoint scan advances through the retained WAL suffix")
      (check (equal segments '(2))
             "checkpoint scan ignores obsolete segments and gaps"))
    ;; The checkpoint itself and all records after it remain subject to normal
    ;; integrity checks.
    (with-open-file (stream (wal-segment-path 2)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :overwrite)
      (file-position stream (+ +wal-header-size+ 28))
      (write-byte (logxor (aref checkpoint 28) #xFF) stream))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "WAL checksum mismatch" (princ-to-string condition))))
           "checkpoint scan validates the checkpoint record")
    (with-open-file (stream (wal-segment-path 2)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :overwrite)
      (file-position stream +wal-header-size+)
      (write-sequence checkpoint stream)
      (file-position stream (+ (* 2 +wal-header-size+) 28))
      (write-byte (logxor (aref post-checkpoint 28) #xFF) stream))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "WAL checksum mismatch" (princ-to-string condition))))
           "checkpoint scan validates records after the checkpoint")))

(defun test-wal-missing-interior-segment ()
  (let* ((test-pgdata (merge-pathnames "wal-gap-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* +wal-header-size+)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) +wal-header-size+)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (dolist (segment '(0 1 2))
      (let* ((start (* segment +wal-segment-size+))
             (type (if (zerop segment)
                       +wal-record-checkpoint+
                       +wal-record-commit+))
             (record (if (= type +wal-record-checkpoint+)
                         (wal-test-legacy-checkpoint-record start)
                         (wal-encode-record
                          type 0 start
                          ;; Fill segment 1 so the record in segment 2 is
                          ;; a legal successor before segment 1 is removed.
                          (if (= segment 1)
                              (make-array (- +wal-segment-size+ +wal-header-size+)
                                          :element-type '(unsigned-byte 8) :initial-element 0)
                              #())))))
        (with-open-file (stream (wal-segment-path segment)
                                :direction :output
                                :element-type '(unsigned-byte 8)
                                :if-exists :supersede
                                :if-does-not-exist :create)
          (write-sequence record stream))))
    (multiple-value-bind (end-lsn segments)
        (wal-scan (lambda (record) (declare (ignore record))))
      (check (= end-lsn *wal-durable-lsn*)
             "WAL scan reads a complete retained segment chain")
      (check (equal segments '(0 1 2))
             "WAL scan sees all segments before a missing-segment check"))
    (delete-file (wal-segment-path 1))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "missing WAL segment" (princ-to-string condition))))
           "WAL recovery rejects a missing interior segment")))
