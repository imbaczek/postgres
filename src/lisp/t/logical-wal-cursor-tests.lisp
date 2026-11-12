(in-package #:pg-physical-tests)

(cl-spec:defproperty logical-wal-carrier-follows-segment-last-byte
    ((segment (range integer 0 1099511627774))
     (offset (range integer 0 16777215)))
  (:about pg::logical-wal-segment-carrier)
  (:trials (:normal 100))
  (let* ((size pg::+wal-segment-size+) (fork (+ (* segment size) offset))
                                       (history (lineage-test-history fork)))
    (and (= 2 (pg::logical-wal-segment-carrier segment history))
         (or (zerop segment) (= 1 (pg::logical-wal-segment-carrier (1- segment) history)))
         (= 2 (pg::logical-wal-segment-carrier (1+ segment) history)))))

(deftest generated-logical-wal-carrier-selection
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-wal-carrier-follows-segment-last-byte :seed 745))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 100 (getf data :trials)))))

(cl-spec:defproperty logical-wal-record-length-preserves-native-bounds
    ((offset (range integer 0 16777215))
     (length (range integer 32 16777216))
     (margin (range integer -1 1)))
  (:about pg::logical-wal-check-record-length)
  (:trials (:normal 100))
  (let ((header (make-array pg::+wal-header-size+ :element-type '(unsigned-byte 8) :initial-element 0))
        (size (+ offset length)))
    (pg::wal-set-u32 header 8 length)
    (let ((result (handler-case
                      (pg::logical-wal-check-record-length header 0 offset size (+ size margin))
                    (error () nil))))
      (if (and (<= size pg::+wal-segment-size+) (not (minusp margin)))
          (eql length result)
          (null result)))))

(deftest generated-logical-wal-record-length-bounds
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-wal-record-length-preserves-native-bounds :seed 747))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 100 (getf data :trials)))))

(defun cursor-test-call (history start end function &key (owner 1) (role :prefix-end)
                                                      (guard (lambda () t)))
  (pg::call-with-logical-wal-cursor history :timeline start end owner role guard function))

(defun cursor-test-collect (cursor)
  (loop for values = (multiple-value-list (pg::logical-wal-cursor-next cursor))
        while (first values)
        collect (destructuring-bind (record bytes owner carrier kind) values
                  (list (pg::wal-record-start record) (pg::wal-record-end record)
                        owner carrier kind bytes))))

(deftest logical-wal-cursor-selects-newest-carrier-across-same-segment-forks
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (a (lineage-test-record 0))
             (b (pg::wal-encode-end-of-recovery-record 33 0 2 1 1))
             (fork (+ 33 (length b)))
             (c (pg::wal-encode-end-of-recovery-record fork 1 3 2 1))
             (end (+ fork (length c)))
             (history (list (pg::make-timeline-history-entry :timeline 3 :begin fork)
                            (pg::make-timeline-history-entry :timeline 2 :begin 33 :end fork)
                            (pg::make-timeline-history-entry :timeline 1 :begin 0 :end 33)))
             (path (lineage-test-file 3 0 a b c))
             (original (lineage-test-bytes path)))
        (lineage-test-file 1 0 a (lineage-test-record 33))
        (lineage-test-file 2 0 a b (lineage-test-record fork))
        (cursor-test-call
         history 0 end
         (lambda (cursor)
           (let ((seen (cursor-test-collect cursor)))
             (ok (equal '((1 3 :ordinary) (2 3 :end-of-recovery) (3 3 :end-of-recovery))
                        (mapcar (lambda (row) (subseq row 2 5)) seen)))
             (ok (every #'equalp (list a b c) (mapcar #'sixth seen)))
             (ok (= end (pg::logical-wal-cursor-position cursor))))))
        (ok (equalp original (lineage-test-bytes path))))))

(deftest logical-wal-cursor-never-falls-back-to-an-older-carrier
    (dolist (failure '(:missing :corrupt :short :symlink :directory :fifo))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (a (lineage-test-record 0))
               (b (pg::wal-encode-end-of-recovery-record 33 0 2 1 1))
               (end (+ 33 (length b)))
               (parent (lineage-test-file 1 0 a b))
               (child (pg::wal-segment-path 0 2 :timeline)))
          (ecase failure
            (:missing)
            (:corrupt
             (let ((bytes (concatenate '(vector (unsigned-byte 8)) a b)))
               (setf (aref bytes 32) (logxor 1 (aref bytes 32)))
               (pg::wal-atomic-write-bytes child bytes)))
            (:short (pg::wal-atomic-write-bytes child (subseq a 0 31)))
            (:symlink (sb-posix:symlink (namestring parent) (namestring child)))
            (:directory (ensure-directories-exist (uiop:ensure-directory-pathname child)))
            (:fifo (sb-posix:mkfifo (namestring child) #o600)))
          (ok (signals
               (cursor-test-call (lineage-test-history 33) 0 end
                                 (lambda (cursor) (pg::logical-wal-cursor-next cursor)))
               'error)
              (format nil "selected ~a carrier cannot fall back" failure))))))

(deftest logical-wal-cursor-preserves-prefix-and-record-start-roles-at-exact-fork
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (fork pg::+wal-segment-size+)
                                 (raw (pg::wal-encode-end-of-recovery-record fork 0 2 1 1))
                                 (history (lineage-test-history fork)))
        (lineage-test-file 2 1 raw)
        (dolist (pair '((1 :prefix-end) (2 :record-start)))
          (cursor-test-call
           history fork (+ fork (length raw))
           (lambda (cursor)
             (multiple-value-bind (record bytes owner carrier kind) (pg::logical-wal-cursor-next cursor)
               (ok (= fork (pg::wal-record-start record)))
               (ok (equalp raw bytes))
               (ok (= 2 owner carrier))
               (ok (eq :end-of-recovery kind))))
           :owner (first pair) :role (second pair)))
        (ok (signals (cursor-test-call history fork fork #'identity :owner 1 :role :record-start) 'error)))))

(deftest logical-wal-cursor-reads-ancestor-segments-and-checks-rotated-eor
    (dolist (rotated '(nil t))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+)
                                   (first-size (- size 40))
                                   (a (lineage-test-record 0 first-size))
                                   (b (unless rotated (lineage-test-record size 96)))
                                   (fork (if rotated first-size (+ size 96)))
                                   (start (if rotated size fork))
                                   (eor (pg::wal-encode-end-of-recovery-record start 0 2 1 1))
                                   (history (lineage-test-history fork))
                                   (end (+ start (length eor))))
          (if rotated
              (progn (lineage-test-file 2 0 a) (lineage-test-file 2 1 eor))
              (progn (lineage-test-file 1 0 a) (lineage-test-file 2 1 b eor)))
          (cursor-test-call
           history 0 end
           (lambda (cursor)
             (let ((seen (cursor-test-collect cursor)))
               (ok (equal (if rotated '(2 2) '(1 2 2)) (mapcar #'fourth seen)))
               (ok (equal (if rotated '(1 2) '(1 1 2)) (mapcar #'third seen)))
               (ok (eq :end-of-recovery (fifth (car (last seen))))))))))))

(deftest logical-wal-cursor-rejects-illegal-gaps-and-empty-destinations
    (dolist (failure '(:fits :empty :fork :past-end))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+)
                                   (a (lineage-test-record 0))
                                   (history (if (eq failure :fork) (lineage-test-history 100)
                                                (pg::timeline-parse-history 1 "")))
                                   (carrier (if (eq failure :fork) 2 1)))
          (lineage-test-file carrier 0 a)
          (lineage-test-file carrier 1
                             (if (eq failure :empty) (make-array 0 :element-type '(unsigned-byte 8))
                                 (lineage-test-record size)))
          (ok (signals
               (cursor-test-call history 33 (if (eq failure :past-end) size (+ size 33))
                                 (lambda (cursor) (pg::logical-wal-cursor-next cursor)))
               'error))))))

(deftest logical-wal-cursor-validates-eor-history-and-batch-isolation
    (dolist (failure '(:parent :child :late :batch :missing))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (a (if (eq failure :batch)
                      (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0
                                             (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0))
                      (lineage-test-record 0)))
               (fork (if (eq failure :late) 32 33))
               (child (if (eq failure :parent) 3 2))
               (raw (if (eq failure :missing) (lineage-test-record 33)
                        (pg::wal-encode-end-of-recovery-record
                         33 0 (if (member failure '(:child :parent)) 3 2)
                         (if (eq failure :parent) 2 1) 1))))
          (lineage-test-file child 0 a raw)
          (ok (signals
               (cursor-test-call (lineage-test-history fork child)
                                 (if (eq failure :late) 33 0) (+ 33 (length raw))
                                 (lambda (cursor) (cursor-test-collect cursor))
                                 :owner (if (eq failure :late) 2 1))
               'error))))))

(deftest logical-wal-cursor-enforces-frozen-horizon-and-scope-lifetime
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (a (lineage-test-record 0))
                                 (b (lineage-test-record 33)) (escaped nil))
        (lineage-test-file 1 0 a b)
        (cursor-test-call (pg::timeline-parse-history 1 "") 0 33
                          (lambda (cursor)
                            (setf escaped cursor)
                            (ok (pg::logical-wal-cursor-next cursor))
                            (ok (null (pg::logical-wal-cursor-next cursor)))))
        (ok (signals (pg::logical-wal-cursor-next escaped) 'error))
        (ok (signals
             (cursor-test-call (pg::timeline-parse-history 1 "") 0 34
                               (lambda (cursor) (cursor-test-collect cursor)))
             'error)))))

(deftest logical-wal-cursor-rechecks-guard-after-decode-before-progress
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (allowed t) (calls 0)
                                 (decode (symbol-function 'pg::wal-decode-record)))
        (lineage-test-file 1 0 (lineage-test-record 0))
        (cursor-test-call
         (pg::timeline-parse-history 1 "") 0 33
         (lambda (cursor)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-decode-record)
                        (lambda (&rest arguments)
                          (assert (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                          (prog1 (apply decode arguments) (setf allowed nil))))
                  (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
                  (ok (zerop (pg::logical-wal-cursor-position cursor))))
             (setf (symbol-function 'pg::wal-decode-record) decode)))
         :guard (lambda ()
                  (assert (sb-thread:holding-mutex-p pg::*wal-lock*))
                  (incf calls)
                  allowed))
        (ok (= 3 calls)))))

(deftest logical-wal-cursor-rejects-unproven-guards-and-cross-thread-use
    (let ((history (pg::timeline-parse-history 1 "")))
      (dolist (guard (list nil (lambda () nil)))
        (ok (signals (cursor-test-call history 0 0 #'identity :guard guard) 'error)))
      (dolist (segment '(-1 1099511627776 1.5 nil))
        (ok (signals (pg::logical-wal-segment-carrier segment history) 'error))))
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root))
      (lineage-test-file 1 0 (lineage-test-record 0))
      (cursor-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (cursor)
         (let ((thread (sb-thread:make-thread
                        (lambda ()
                          (handler-case (pg::logical-wal-cursor-next cursor)
                            (error () :rejected))))))
           (ok (eq :rejected (sb-thread:join-thread thread)))
           (ok (signals
                (let ((pg::*wal-lock* (sb-thread:make-mutex)))
                  (pg::logical-wal-cursor-next cursor))
                'error))
           (ok (zerop (pg::logical-wal-cursor-position cursor)))
           (ok (pg::logical-wal-cursor-next cursor))))))))

(deftest logical-wal-cursor-rejects-nested-and-incomplete-batches
    (dolist (nested '(nil t))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (empty (make-array 0 :element-type '(unsigned-byte 8)))
               (a (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 empty))
               (b (if nested (pg::wal-encode-record pg::+wal-record-batch-start+ 0 32 empty) empty)))
          (lineage-test-file 1 0 a b)
          (ok (signals
               (cursor-test-call (pg::timeline-parse-history 1 "") 0 (+ (length a) (length b))
                                 (lambda (cursor) (cursor-test-collect cursor)))
               'error))))))

(deftest logical-wal-slot-start-proof-distinguishes-feedback-and-restart-roles
    (let* ((pending (make-hash-table :test #'eql))
           (slot (pg::make-wal-replication-slot
                  :name "cursor-proof" :plugin "pgoutput"
                  :restart-lsn 64 :restart-timeline 1
                  :confirmed-flush-lsn 97 :confirmed-timeline 1
                  :pending-xids pending)))
      (ok (not (pg::logical-slot-wal-start-proven-p slot 97 1 :prefix-end))
          "exact standard feedback need not be a record boundary")
      (ok (pg::logical-slot-wal-start-proven-p slot 64 1 :prefix-end)
          "a consumed restart floor is a prefix boundary")
      (ok (not (pg::logical-slot-wal-start-proven-p slot 64 1 :record-start)))
      (ok (not (pg::logical-slot-wal-start-proven-p slot 64 2 :prefix-end)))
      (setf (gethash 7 pending) (cons 64 128))
      (ok (pg::logical-slot-wal-start-proven-p slot 64 1 :record-start))
      (ok (not (pg::logical-slot-wal-start-proven-p slot 64 1 :prefix-end))
          "a pending restart is a record start, even at the same numeric position")
      (clrhash pending)
      (setf (pg::wal-replication-slot-restart-lsn slot) 0)
      (ok (pg::logical-slot-wal-start-proven-p slot 0 1 :record-start))
      (ok (not (pg::logical-slot-wal-start-proven-p slot 0 1 :prefix-end)))
      (setf (pg::wal-replication-slot-plugin slot) "pg_lisp")
      (ok (pg::logical-slot-wal-start-proven-p slot 97 1 :prefix-end)
          "private terminal acknowledgments retain their boundary proof")))

(deftest logical-wal-replay-scan-pins-the-anchor-before-entering-the-reader
    (let* ((pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (pg::*wal-replication-slots* (make-hash-table :test #'equal))
           (pg::*wal-poisoned* nil) (pg::*wal-durable-lsn* 128)
           (owner (list :owner)) (pg::*wal-logical-slot-owner* owner)
           (slot (pg::make-wal-replication-slot
                  :name "replay-pin" :database-oid 7 :restart-lsn 32
                  :consistent-lsn 32 :confirmed-flush-lsn 64 :active-owner owner))
           (entered nil) (escaped nil))
      (setf (gethash "replay-pin" pg::*wal-replication-slots*) slot)
      (pg::call-with-wal-retained-slot-scan
       "replay-pin" 96 128
       (lambda (scan requested)
         (setf entered t escaped scan)
         (ok (= requested 96))
         (ok (= 32 (pg::wal-retained-slot-scan-start scan)))
         (ok (= 32 (pg::wal-retained-slot-scan-cursor scan)))
         (ok (= 32 (gethash (pg::wal-retained-slot-scan-pin scan)
                            pg::*wal-snapshot-retention-pins*)))
         (setf pg::*wal-durable-lsn* 256)
         (ok (= 128 (pg::wal-retained-slot-scan-end scan))))
       :replay-from-restart t)
      (ok entered)
      (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))
      (let ((called nil)
            (message nil))
        (setf message
              (handler-case
                  (pg::call-with-logical-slot-wal-cursor-from-scan
                   escaped 1 :prefix-end (lambda (cursor) (declare (ignore cursor)) (setf called t)))
                (error (condition) (princ-to-string condition))))
        (ok (and message (search "changed or lost retained WAL" message)))
        (ok (not called)))
      (let ((unwound nil))
        (catch 'reader-exit
          (pg::call-with-wal-retained-slot-scan
           "replay-pin" nil 128
           (lambda (scan requested)
             (ok (= requested 64))
             (ok (= 32 (pg::wal-retained-slot-scan-start scan)))
             (setf unwound t)
             (throw 'reader-exit nil))
           :replay-from-restart t))
        (ok unwound)
        (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*))))))

(deftest logical-wal-reader-policy-hides-tails-and-restarts-abandoned-batches
    (dolist (nested '(nil t))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (empty (make-array 0 :element-type '(unsigned-byte 8)))
               (a (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 empty))
               (b (if nested (pg::wal-encode-record pg::+wal-record-batch-start+ 0 32 empty) empty))
               (end (+ (length a) (length b))))
          (lineage-test-file 1 0 a b)
          (pg::call-with-logical-wal-cursor
           (pg::timeline-parse-history 1 "") :timeline 0 end 1 :prefix-end (lambda () t)
           (lambda (cursor)
             (ok (= (if nested 2 1) (length (cursor-test-collect cursor))))
             (ok (= end (pg::logical-wal-cursor-position cursor)))
             (ok (pg::logical-wal-cursor-batch-open cursor)))
           :batch-policy :logical-reader)))))

(deftest logical-wal-reader-policy-still-rejects-eor-in-an-open-batch
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (a (pg::wal-encode-record
                 pg::+wal-record-batch-start+ 0 0
                 (make-array 0 :element-type '(unsigned-byte 8))))
             (b (pg::wal-encode-end-of-recovery-record 32 0 2 1 1)))
        (lineage-test-file 2 0 a b)
        (pg::call-with-logical-wal-cursor
         (lineage-test-history 32) :timeline 0 (+ 32 (length b))
         1 :prefix-end (lambda () t)
         (lambda (cursor)
           (ok (pg::logical-wal-cursor-next cursor))
           (let ((message (handler-case (progn (pg::logical-wal-cursor-next cursor) nil)
                            (error (condition) (princ-to-string condition)))))
             (ok (and message (search "end-of-recovery disagrees" message)))
             (ok (= 32 (pg::logical-wal-cursor-position cursor)))))
         :batch-policy :logical-reader))))

(deftest logical-wal-replay-reader-does-not-charge-suppressed-batches
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (empty (make-array 0 :element-type '(unsigned-byte 8)))
             (history (pg::timeline-parse-history 1 ""))
             (scan (pg::make-wal-retained-slot-scan :start 0 :end 192 :database 7)))
        (apply #'lineage-test-file 1 0
               (loop for start from 0 below 192 by 32
                     collect (pg::wal-encode-record
                              (if (zerop (mod start 64)) pg::+wal-record-batch-start+
                                  pg::+wal-record-maintenance+) 0 start empty)))
        (dolist (case '((64 nil) (65 t)))
          (pg::call-with-logical-wal-cursor
           history :timeline 0 192 1 :prefix-end (lambda () t)
           (lambda (cursor)
             (multiple-value-bind (transactions progress exhausted)
                 (pg::wal-logical-slot-read-transactions-from-scan
                  scan 1 1 1 :cursor cursor :output-start (first case) :allow-filter (second case))
               (ok (null transactions))
               (ok (= 128 progress) "suppressed replay does not consume the scan budget")
               (ok (not exhausted))))
           :batch-policy :logical-reader))
        (pg::call-with-logical-wal-cursor
         history :timeline 0 192 1 :prefix-end (lambda () t)
         (lambda (cursor)
           (let ((message
                   (handler-case
                       (pg::wal-logical-slot-read-transactions-from-scan
                        scan 1 1 1 :cursor cursor :output-start 65)
                     (error (condition) (princ-to-string condition)))))
             (ok (and message (search "not a completed logical WAL boundary" message)))))
         :batch-policy :logical-reader))))

(deftest logical-wal-replay-reader-does-not-turn-eor-into-ack-progress
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (empty (make-array 0 :element-type '(unsigned-byte 8)))
             (a (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 empty))
             (b (pg::wal-encode-record pg::+wal-record-maintenance+ 0 32 empty))
             (eor (pg::wal-encode-end-of-recovery-record 64 0 2 1 1))
             (end (+ 64 (length eor)))
             (scan (pg::make-wal-retained-slot-scan :start 0 :end end :database 7)))
        (lineage-test-file 2 0 a b eor)
        (pg::call-with-logical-wal-cursor
         (lineage-test-history 64) :timeline 0 end 1 :prefix-end (lambda () t)
         (lambda (cursor)
           (multiple-value-bind (transactions progress exhausted)
               (pg::wal-logical-slot-read-transactions-from-scan
                scan 1 1024 1024 :cursor cursor :output-start 0)
             (ok (null transactions))
             (ok (= 64 progress))
             (ok exhausted)
             (ok (= end (pg::logical-wal-cursor-position cursor)))))
         :batch-policy :logical-reader))))

(defun call-with-reader-row-batches (function &key invalid-first)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (empty (make-array 0 :element-type '(unsigned-byte 8)))
           (columns '((:attnum 1 :name "id" :type :int4 :typmod -1 :notnull t)))
           (position 0) (first-end nil) (event-bytes nil)
           (records
             (loop for xid from 7 to 8 append
                                       (let* ((marker (pg::wal-encode-record pg::+wal-record-batch-start+ 0 position empty))
                                              (event (pg::logical-encode-change
                                                      (pg::make-logical-change
                                                       :kind :insert :database-oid 7 :relation-oid 701
                                                       :relation-name "replayed_rows" :columns columns
                                                       :key-attnums '(1) :new-values (vector xid))))
                                              (payload (pg::logical-encode-chunk 0 1 (list event)))
                                              (chunk (pg::wal-encode-record
                                                      pg::+wal-record-logical-changes+ xid (+ position (length marker)) payload))
                                              (commit (pg::wal-encode-record
                                                       pg::+wal-record-commit+ xid (+ position (length marker) (length chunk))
                                                       (if (and invalid-first (= xid 7))
                                                           (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)
                                                           (pg::wal-encode-commit-timestamp xid)))))
                                         (incf position (+ (length marker) (length chunk) (length commit)))
                                         (when (= xid 7)
                                           (setf first-end position
                                                 event-bytes (nth-value 3 (pg::wal-decode-logical-change-chunk payload))))
                                         (list marker chunk commit))))
           (scan (pg::make-wal-retained-slot-scan :start 0 :end position :database 7)))
      (apply #'lineage-test-file 1 0 records)
      (funcall function scan first-end position event-bytes))))

(deftest logical-wal-replay-reader-validates-and-closes-suppressed-row-spools
    (dolist (invalid '(nil t))
      (call-with-reader-row-batches
       (lambda (scan first-end end event-bytes)
         (declare (ignore event-bytes))
         (pg::call-with-logical-reader-spools
          (lambda ()
            (pg::call-with-logical-wal-cursor
             (pg::timeline-parse-history 1 "") :timeline 0 end 1 :prefix-end (lambda () t)
             (lambda (cursor)
               (if invalid
                   (let ((message
                           (handler-case
                               (pg::wal-logical-slot-read-transactions-from-scan
                                scan 1 1 1 :cursor cursor :output-start first-end)
                             (error (condition) (princ-to-string condition)))))
                     (ok (and message (search "commit record has an invalid payload" message)))
                     (ok (= 1 (length (car pg::*logical-reader-spool-scope*))))
                     (ok (every #'pg::logical-spool-closed-p (car pg::*logical-reader-spool-scope*))))
                   (multiple-value-bind (transactions progress exhausted)
                       (pg::wal-logical-slot-read-transactions-from-scan
                        scan 1 1 1 :cursor cursor :output-start first-end)
                     (ok (= 1 (length transactions)))
                     (ok (= 8 (pg::logical-transaction-xid (first transactions))))
                     (ok (= end progress))
                     (ok (not exhausted))
                     (let ((spools (car pg::*logical-reader-spool-scope*)))
                       (ok (= 2 (length spools)))
                       (ok (= 1 (count-if #'pg::logical-spool-closed-p spools)))
                       (ok (not (pg::logical-spool-closed-p
                                 (pg::logical-transaction-spool (first transactions)))))))))
             :batch-policy :logical-reader))))
       :invalid-first invalid)))

(deftest logical-wal-reader-reports-only-consumer-finished-terminal-proof
    (dolist (case '(:empty :open-tail :filter-tail :finished-tail))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (empty (make-array 0 :element-type '(unsigned-byte 8)))
               (records
                 (unless (eq case :empty)
                   (if (eq case :finished-tail)
                       (list (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 empty)
                             (pg::wal-encode-record pg::+wal-record-maintenance+ 0 32 empty)
                             (pg::wal-encode-record pg::+wal-record-batch-start+ 0 64 empty))
                       (list (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 empty)))))
               (end (reduce #'+ records :key #'length :initial-value 0))
               (requested (if (eq case :filter-tail) 1 0))
               (scan (pg::make-wal-retained-slot-scan :start 0 :end end :database 7)))
          (when records (apply #'lineage-test-file 1 0 records))
          (pg::call-with-logical-wal-cursor
           (pg::timeline-parse-history 1 "") :timeline 0 end 1 :prefix-end (lambda () t)
           (lambda (cursor)
             (multiple-value-bind (transactions progress exhausted finished)
                 (pg::wal-logical-slot-read-transactions-from-scan
                  scan 10 1024 1024 :cursor cursor :output-start requested
                  :allow-filter (eq case :filter-tail))
               (ok (null transactions))
               (ok exhausted)
               (if (eq case :finished-tail)
                   (progn (ok (= 64 progress)) (ok (= 64 finished))
                          (ok (= 96 (pg::logical-wal-cursor-position cursor))))
                   (progn (ok (= requested progress)) (ok (null finished))))))
           :batch-policy :logical-reader)))))

(deftest logical-wal-reader-keeps-terminal-proof-behind-limit-lookahead
    (call-with-reader-row-batches
     (lambda (scan first-end end event-bytes)
       (pg::call-with-logical-reader-spools
        (lambda ()
          (pg::call-with-logical-wal-cursor
           (pg::timeline-parse-history 1 "") :timeline 0 end 1 :prefix-end (lambda () t)
           (lambda (cursor)
             (multiple-value-bind (transactions progress exhausted finished)
                 (pg::wal-logical-slot-read-transactions-from-scan
                  scan 10 (1+ event-bytes) 1048576 :cursor cursor)
               (ok (= 1 (length transactions)))
               (ok (= 7 (pg::logical-transaction-xid (first transactions))))
               (ok (= first-end progress finished))
               (ok (not exhausted))
               (ok (= end (pg::logical-wal-cursor-position cursor)))
               (ok (< finished (pg::logical-wal-cursor-position cursor)))
               (ok (= 1 (count-if #'pg::logical-spool-closed-p
                                  (car pg::*logical-reader-spool-scope*))))))
           :batch-policy :logical-reader))))))

(deftest logical-wal-continuation-propagates-fatal-and-latched-wal-errors
    (let* ((slot (pg::make-wal-replication-slot :name "interrupt-proof"))
           (source (pg::%make-logical-wal-source :slot slot))
           (pg::*wal-logical-slot-read-continuation*
             (pg::%make-logical-wal-continuation :source source :position 64 :owner 1))
           (original (symbol-function 'pg::logical-wal-check-source))
           (attempts 0))
      (unwind-protect
           (dolist (kind '(:ordinary :fatal :wal-failure))
             (let* ((condition (make-condition 'pg::pg-error
                                               :severity (if (eq kind :fatal) "FATAL" "ERROR")
                                               :sqlstate (if (eq kind :fatal) "57P01" "XX000")))
                    (pg::*backend* (pg::make-backend
                                    :wal-failure (when (eq kind :wal-failure) condition)))
                    (proof nil) (caught nil))
               (setf (symbol-function 'pg::logical-wal-check-source)
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (incf attempts) (error condition)))
               (handler-case
                   (setf proof (sb-thread:with-mutex (pg::*wal-lock*)
                                 (pg::logical-wal-reusable-continuation slot 64 64)))
                 (error (error) (setf caught error)))
               (ok (null proof))
               (if (eq kind :ordinary)
                   (ok (null caught) "ordinary validation errors remain cache misses")
                   (ok (eq condition caught) "fatal conditions propagate unchanged"))))
        (setf (symbol-function 'pg::logical-wal-check-source) original))
      (ok (= 3 attempts))))
