;;;; logical-tests.lisp -- logical row-change codec and WAL stream tests.

(in-package #:pg)

(defun logical-test-columns ()
  (list (list :attnum 1 :name "id" :type :int4 :typmod -1 :notnull t)
        (list :attnum 2 :name "amount" :type :int8 :typmod -1 :notnull nil)
        (list :attnum 3 :name "label" :type :varchar :typmod 64 :notnull nil)))

(defun logical-test-change (kind database-oid relation-oid
                            &key old-values new-values)
  (make-logical-change :kind kind :database-oid database-oid
                       :relation-oid relation-oid :relation-name "logical_rows"
                       :columns (logical-test-columns)
                       :old-values old-values :new-values new-values))

(defun test-logical-row-change-codec ()
  ;; These are the executor-boundary equivalents of DecodeInsert,
  ;; DecodeUpdate, and DecodeDelete in src/backend/replication/logical/decode.c.
  (let* ((old-row (vector 10 -90 "old"))
         (new-row (vector 10 120 +null+))
         (events
           (list (logical-test-change :insert 7 701 :new-values new-row)
                 (logical-test-change :update 7 701 :old-values old-row
                                                    :new-values new-row)
                 (logical-test-change :delete 7 701 :old-values old-row))))
    (dolist (event events)
      (let* ((encoded (logical-encode-change event))
             (decoded (logical-decode-change encoded)))
        (check (and (eq (logical-change-kind decoded)
                        (logical-change-kind event))
                    (= (logical-change-database-oid decoded) 7)
                    (= (logical-change-relation-oid decoded) 701)
                    (equal (logical-change-columns decoded)
                           (logical-change-columns event))
                    (equalp (logical-change-old-values decoded)
                            (logical-change-old-values event))
                    (equalp (logical-change-new-values decoded)
                            (logical-change-new-values event)))
               "logical row image and schema round-trip")))
    (let* ((encoded (logical-encode-change (first events)))
           (with-trailing-byte
               (concatenate '(vector (unsigned-byte 8)) encoded #(0))))
      (check (handler-case
                 (progn (logical-decode-change with-trailing-byte) nil)
               (error () t))
             "logical row payload rejects trailing bytes"))
    (let* ((long-name (make-string 70000 :initial-element #\r))
           (event (logical-test-change :insert 7 701
                                       :new-values #(11 12 "long name"))))
      (setf (logical-change-relation-name event) long-name)
      (check (string= long-name
                      (logical-change-relation-name
                       (logical-decode-change (logical-encode-change event))))
             "logical schema names use lengths large enough for valid identifiers"))))

(defun logical-test-append-batch (xid changes &optional commit-p)
  ;; Like reorderbuffer.c, the reader must not expose queued changes until a
  ;; transaction's commit record closes the WAL batch.
  (wal-append-batch-start)
  (when changes
    (wal-append-logical-changes xid (mapcar #'logical-encode-change changes)))
  (when commit-p
    (wal-append-record +wal-record-commit+ xid #())))

(defun logical-test-with-wal-storage (function)
  (let* ((test-pgdata
           (merge-pathnames (format nil "logical-stream-~a/" (random 100000000))
                            *pgdata*))
         (*pgdata* test-pgdata)
         (*backend* nil)
         (*wal-ready* t)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*clog-stream* nil)
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*max-slot-wal-keep-bytes* nil))
    (ignore-errors
     (uiop:delete-directory-tree test-pgdata :validate t
                                             :if-does-not-exist :ignore))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (unwind-protect
         (funcall function)
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (when *clog-stream*
        (ignore-errors (close *clog-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-logical-wal-reader-commit-order-and-slot-ack ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((slot (make-wal-replication-slot
                   :name "logical_slot" :database-oid 7 :restart-lsn 0
                   :consistent-lsn 0 :confirmed-flush-lsn 0))
            (other-slot (make-wal-replication-slot
                         :name "other_slot" :database-oid 7 :restart-lsn 0
                         :consistent-lsn 0 :confirmed-flush-lsn 0))
            (lost-slot (make-wal-replication-slot
                        :name "lost_slot" :database-oid 7 :restart-lsn 0
                        :consistent-lsn 0 :confirmed-flush-lsn 0
                        :wal-status :lost))
            (first-end nil)
            (later-end nil))
       (setf (gethash "logical_slot" *wal-replication-slots*) slot
             (gethash "other_slot" *wal-replication-slots*) other-slot
             (gethash "lost_slot" *wal-replication-slots*) lost-slot)
       ;; An unterminated batch before a later batch mirrors a crash tail that
       ;; wal-replay-record discards when it sees the next batch-start record.
       (logical-test-append-batch
        900 (list (logical-test-change :insert 7 701
                                       :new-values #(90 0 "aborted"))))
       (logical-test-append-batch
        901 (list (logical-test-change :insert 7 701
                                       :new-values #(10 1 "first"))
                  (logical-test-change :update 7 701
                                       :old-values #(10 1 "first")
                                       :new-values #(10 2 "second")))
        t)
       (setf first-end *wal-end-lsn*)
       ;; A slot is database-scoped even though the physical WAL is shared.
       (logical-test-append-batch
        902 (list (logical-test-change :insert 8 801
                                       :new-values #(20 2 "other db")))
        t)
       (logical-test-append-batch
        903 (list (logical-test-change :delete 7 701
                                       :old-values #(30 3 "third")))
        t)
       (setf later-end *wal-end-lsn*)
       ;; The scan's durable horizon may also end in an incomplete crash tail.
       (logical-test-append-batch
        904 (list (logical-test-change :insert 7 701
                                       :new-values #(40 4 "uncommitted"))))
       (wal-flush)
       (wal-recover)
       (check (handler-case
                  (progn
                    (call-with-logical-replication-slot "lost_slot"
                                                        (lambda (owned-slot) (declare (ignore owned-slot))))
                    nil)
                (error () t))
              "logical readers cannot acquire a slot whose WAL was lost")
       (check (handler-case
                  (progn (wal-logical-slot-read-transactions "logical_slot") nil)
                (error () t))
              "logical slot reads require an explicit active owner")
       (check (handler-case
                  (progn (wal-logical-slot-acknowledge "logical_slot" first-end)
                         nil)
                (error () t))
              "logical acknowledgment requires ownership of its slot")
       (call-with-logical-replication-slot
        "logical_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (let ((bounded-transactions
                  (wal-logical-slot-read-transactions
                   "logical_slot" :max-transactions 1)))
            (check (equal (mapcar #'logical-transaction-xid
                                  bounded-transactions)
                          '(901))
                   "logical reader stops at its transaction-count boundary"))
          (let ((bounded-transactions
                  (wal-logical-slot-read-transactions
                   "logical_slot" :max-transactions 8 :max-bytes 1)))
            (check (equal (mapcar #'logical-transaction-xid
                                  bounded-transactions)
                          '(901))
                   "byte budget returns one complete oversized transaction"))
          (check (handler-case
                     (progn
                       (wal-logical-slot-acknowledge "logical_slot" later-end)
                       nil)
                   (error () t))
                 "slot cannot acknowledge a transaction beyond read progress")
          (check (handler-case
                     (progn
                       (wal-logical-slot-acknowledge "logical_slot" "invalid")
                       nil)
                   (error () t))
                 "logical acknowledgment rejects non-integer LSNs")
          (let ((transactions (wal-logical-slot-read-transactions "logical_slot")))
            (check (equal (mapcar #'logical-transaction-xid transactions)
                          '(901 903))
                   "reader drops crash tails and filters other databases")
            (check (< (logical-transaction-commit-lsn (first transactions))
                      (logical-transaction-commit-lsn (second transactions)))
                   "logical transactions are emitted in durable commit order")
            (check (equal (mapcar #'logical-change-kind
                                  (logical-transaction-changes
                                   (first transactions)))
                          '(:insert :update))
                   "row changes retain executor order within a transaction")
            (check (handler-case
                       (progn
                         (wal-logical-slot-acknowledge
                          "other_slot"
                          (logical-transaction-commit-lsn (first transactions)))
                         nil)
                     (error () t))
                   "a reader cannot acknowledge a different slot")
            (wal-logical-slot-acknowledge
             "logical_slot"
             (logical-transaction-commit-lsn (first transactions)))
            (check (= (wal-replication-slot-confirmed-flush-lsn slot) first-end)
                   "slot acknowledgment persists the applied commit boundary")
            (multiple-value-bind (transactions progress-lsn)
                (wal-logical-slot-read-transactions
                 "logical_slot" :max-scan-bytes 1)
              (check (null transactions)
                     "bounded scan can make progress without row transactions")
              (check (> progress-lsn first-end)
                     "empty read returns its last complete terminal LSN")
              (wal-logical-slot-acknowledge "logical_slot" progress-lsn))
            (check (equal (mapcar #'logical-transaction-xid
                                  (wal-logical-slot-read-transactions
                                   "logical_slot"))
                          '(903))
                   "a second read resumes after the durable acknowledgment"))))))))

(defun logical-test-commit-boundary-batch (xid)
  (with-wal-critical
    (let ((batch-start (wal-append-batch-start)))
      ;; Model an earlier page batch from XID before a logical slot captures its
      ;; snapshot; the slot must retain this active XID through the later commit.
      (wal-record-page-xids-at-batch-start
       (list (make-wal-page-image :xids (list xid))) batch-start)
      (wal-append-record +wal-record-maintenance+ 0 #())
      (wal-flush)
      batch-start)))

(defun logical-test-commit-insert (xid table values)
  (let* ((*backend* (make-backend :xid xid))
         (*active-xids* (cons xid *active-xids*)))
    (setf (backend-database *backend*) "postgres"
          (backend-state *backend*) :in-progress)
    (logical-note-row-change :insert table :new-values values)
    (commit-transaction)))

(defun logical-test-abort-insert (xid table)
  (let* ((*backend* (make-backend :xid xid))
         (*active-xids* (cons xid *active-xids*))
         (wal-end-before *wal-end-lsn*))
    (setf (backend-database *backend*) "postgres"
          (backend-state *backend*) :in-progress)
    (logical-note-row-change :insert table :new-values #(58 7 "aborted"))
    (abort-transaction)
    (check (null (backend-logical-changes *backend*))
           "rollback discards its transaction-local logical queue")
    (check (= *wal-end-lsn* wal-end-before)
           "rollback emits no logical WAL records")))

(defun test-logical-snapshot-handoff-active-xid ()
  ;; SnapBuildProcessRunningXacts and CreateInitDecodingContext establish a
  ;; consistent point while concurrent transactions remain in progress. This
  ;; test captures a row before that point, commits its XID afterward, and reads
  ;; it only from the shared snapshot/stream boundary.
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (database (make-database :name "postgres" :oid 7))
            (table
              (make-pg-class
               :oid 701 :name "logical_rows"
               :columns
               (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                       :attnum 1 :notnull t)
                       (make-pg-column :name "amount" :type :int8 :typmod -1
                                       :attnum 2 :notnull nil)
                       (make-pg-column :name "label" :type :varchar :typmod 64
                                       :attnum 3 :notnull nil))))
            (xid 9501)
            (*backend* (make-backend :xid xid))
            (*active-xids* (list xid))
            (*next-xid* (1+ xid))
            (*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (registration (make-snapshot-registration))
            (owner (gensym "SNAPSHOT-HANDOFF-"))
            (during-copy-xid (1+ xid))
            (boundary-lsn nil)
            (restart-lsn nil)
            (snapshot nil)
            (slot nil)
            (ack-lsn nil))
       (setf (backend-database *backend*) "postgres"
             (backend-state *backend*) :in-progress
             (gethash "postgres" (catalog-databases *catalog*)) database
             (gethash "logical_rows" (database-tables database)) table)
       (logical-note-row-change :insert table :new-values #(55 4 "across boundary"))
       (logical-test-commit-boundary-batch xid)
       (multiple-value-setq (snapshot boundary-lsn restart-lsn slot)
         (with-write-lock
           (wal-create-replication-slot-under-writer-lock
            "handoff_slot" 0 registration owner 7)))
       (check (member xid (snapshot-xip snapshot) :test #'=)
              "snapshot contains the active writer")
       (check (< restart-lsn boundary-lsn)
              (format nil "slot retains active writer WAL (restart=~a boundary=~a xid-first=~a)"
                      restart-lsn boundary-lsn (gethash xid *wal-xid-first-lsn*)))
       (check (null (wal-replication-slot-confirmed-flush-lsn slot))
              "new slot awaits initial-copy acknowledgment")
       (check (not (member during-copy-xid (snapshot-xip snapshot) :test #'=))
              "transaction started after the snapshot is outside the copy")
       (logical-test-commit-insert
        during-copy-xid table #(56 5 "committed during copy"))
       ;; A second statement in the original transaction remains attached to
       ;; its XID even though it runs after the snapshot boundary.
       (logical-note-row-change :insert table :new-values #(57 6 "second statement"))
       (release-snapshot-registration registration)
       (wal-release-replication-slot-owner "handoff_slot" owner)
       (wal-acknowledge-replication-slot "handoff_slot" boundary-lsn)
       (commit-transaction)
       (check (and (null (backend-logical-changes *backend*))
                   (not (member xid *active-xids* :test #'=)))
              "real transaction commit publishes and retires its queued row change")
       (logical-test-abort-insert (1+ during-copy-xid) table)
       ;; Startup recovery validates complete v5 row records before the slot
       ;; reader resumes from its durable acknowledgment.
       (wal-recover)
       (call-with-logical-replication-slot
        "handoff_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (let* ((transactions
                   (wal-logical-slot-read-transactions "handoff_slot"))
                 (transaction (second transactions))
                 (change (first (logical-transaction-changes transaction))))
            (check (and (= (length transactions) 2)
                        (equal (mapcar #'logical-transaction-xid transactions)
                               (list during-copy-xid xid))
                        (> (logical-transaction-commit-lsn transaction)
                           boundary-lsn))
                   "copy-time and active-at-snapshot transactions stream in commit order")
            (check (and (eq (logical-change-kind change) :insert)
                        (equalp (logical-change-new-values change)
                                #(55 4 "across boundary")))
                   "executor-captured row reaches the reader through real commit WAL")
            (check (equalp (mapcar #'logical-change-new-values
                                   (logical-transaction-changes transaction))
                           '(#(55 4 "across boundary")
                             #(57 6 "second statement")))
                   "multi-statement row changes retain their transaction order")
            (setf ack-lsn
                  (logical-transaction-commit-lsn (second transactions)))
            (wal-logical-slot-acknowledge "handoff_slot" ack-lsn))))
       ;; Slot files persist subscriber progress across restart. The resumed
       ;; reader must start after the last durable acknowledgment.
       (wal-load-replication-slots)
       (setf slot (gethash "handoff_slot" *wal-replication-slots*))
       (check (and (= (wal-replication-slot-database-oid slot) 7)
                   (= (wal-replication-slot-confirmed-flush-lsn slot) ack-lsn))
              "restart reloads the database binding and logical acknowledgment")
       (call-with-logical-replication-slot
        "handoff_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (check (null (wal-logical-slot-read-transactions "handoff_slot"))
                 "restart does not replay acknowledged transactions")))))))

(defun test-wal-pgslot3-database-field-validation ()
  (let* ((test-directory
           (merge-pathnames (format nil "logical-slot-file-~a/" (random 100000000))
                            *pgdata*))
         (valid-path (merge-pathnames "valid_slot.slot" test-directory))
         (invalid-path (merge-pathnames "invalid_slot.slot" test-directory))
         (unbound (make-wal-replication-slot
                   :name "valid_slot" :restart-lsn 0 :consistent-lsn 0
                   :wal-status :reserved))
         (invalid-prefix
           (format nil "PGSLOT3 invalid_slot not-an-oid 0 0 - reserved")))
    (wal-ensure-directory-durable test-directory)
    (unwind-protect
         (progn
           (with-open-file (stream valid-path :direction :output
                                              :element-type '(unsigned-byte 8)
                                              :if-exists :supersede
                                              :if-does-not-exist :create)
             (write-sequence (wal-encode-replication-slot unbound) stream))
           (check (null (wal-replication-slot-database-oid
                         (wal-read-replication-slot valid-path)))
                  "PGSLOT3 preserves a valid legacy database-unbound slot")
           (with-open-file (stream invalid-path :direction :output
                                                :element-type '(unsigned-byte 8)
                                                :if-exists :supersede
                                                :if-does-not-exist :create)
             (write-sequence
              (string->bytes
               (format nil "~a ~8,'0X~%" invalid-prefix
                       (wal-crc32 (string->bytes invalid-prefix))))
              stream))
           (check (handler-case
                      (progn (wal-read-replication-slot invalid-path) nil)
                    (error () t))
                  "PGSLOT3 rejects malformed database OIDs with a valid CRC"))
      (ignore-errors
       (uiop:delete-directory-tree test-directory :validate t
                                                  :if-does-not-exist :ignore)))))
