;;;; protocol.lisp -- logical replication commands and pgoutput v1 framing.

(in-package #:pg)

(defstruct logical-replication-command
  type slot-name plugin export-snapshot-p requested-lsn protocol-version)

(defparameter +logical-replication-type-oids+
  '((:int4 . 23) (:int8 . 20) (:varchar . 1043)))

(defun replication-command-words (query)
  (let ((query (string-right-trim '(#\Space #\Tab #\Newline #\Return #\;)
                                  query))
        (words '())
        (position 0))
    (loop while (< position (length query))
          do (loop while (and (< position (length query))
                              (find (char query position)
                                    '(#\Space #\Tab #\Newline #\Return)))
                   do (incf position))
             (when (< position (length query))
               (let ((start position)
                     (char (char query position)))
                 (cond
                   ((find char '(#\( #\)))
                    (incf position)
                    (push (subseq query start position) words))
                   ((char= char #\')
                    (incf position)
                    (loop while (and (< position (length query))
                                     (not (char= (char query position) #\')))
                          do (incf position))
                    (unless (< position (length query))
                      (ereport "42601" "unterminated replication command option"))
                    (incf position)
                    (push (subseq query start position) words))
                   (t
                    (loop while (and (< position (length query))
                                     (not (find (char query position)
                                                '(#\Space #\Tab #\Newline #\Return
                                                  #\( #\) #\'))))
                          do (incf position))
                    (push (subseq query start position) words))))))
    (nreverse words)))

(defun parse-replication-lsn (text)
  (let ((slash (position #\/ text)))
    (unless (and slash
                 (plusp slash)
                 (< slash (1- (length text)))
                 (not (position #\/ text :start (1+ slash))))
      (ereport "22P02" "invalid replication LSN \"~a\"" text))
    (handler-case
        (let ((high (parse-integer text :end slash :radix 16 :junk-allowed nil))
              (low (parse-integer text :start (1+ slash) :radix 16
                                       :junk-allowed nil)))
          (unless (and (<= 0 high #xFFFFFFFF) (<= 0 low #xFFFFFFFF))
            (ereport "22P02" "replication LSN is out of range"))
          (+ (ash high 32) low))
      (error ()
        (ereport "22P02" "invalid replication LSN \"~a\"" text)))))

(defun parse-replication-command (query)
  (let* ((words (replication-command-words query))
         (verb (and words (string-upcase (first words)))))
    (cond
      ((equal verb "CREATE_REPLICATION_SLOT")
       (unless (and (= (length words) 5)
                    (string-equal (third words) "LOGICAL")
                    (string-equal (fourth words) "pgoutput")
                    (string-equal (fifth words) "EXPORT_SNAPSHOT"))
         (ereport "0A000"
                  "supported syntax is CREATE_REPLICATION_SLOT name LOGICAL pgoutput EXPORT_SNAPSHOT"))
       (unless (wal-valid-replication-slot-name-p (second words))
         (ereport "42602" "invalid replication slot name \"~a\""
                  (second words)))
       (make-logical-replication-command
        :type :create :slot-name (second words) :plugin "pgoutput"
        :export-snapshot-p t))
      ((equal verb "DROP_REPLICATION_SLOT")
       (unless (= (length words) 2)
         (ereport "0A000" "supported syntax is DROP_REPLICATION_SLOT name"))
       (unless (wal-valid-replication-slot-name-p (second words))
         (ereport "42602" "invalid replication slot name \"~a\""
                  (second words)))
       (make-logical-replication-command :type :drop :slot-name (second words)))
      ((equal verb "START_REPLICATION")
       (unless (and (<= 5 (length words))
                    (string-equal (second words) "SLOT")
                    (wal-valid-replication-slot-name-p (third words))
                    (string-equal (fourth words) "LOGICAL"))
         (ereport "0A000"
                  "supported syntax is START_REPLICATION SLOT name LOGICAL lsn"))
       (let ((protocol-version 2))
         (when (> (length words) 5)
           (unless (equal words
                          (append (subseq words 0 5)
                                  '("(" "proto_version" "'2'" ")")))
             (ereport "0A000" "only pgoutput protocol version 2 is supported")))
         (make-logical-replication-command
          :type :start :slot-name (third words)
          :requested-lsn (parse-replication-lsn (fifth words))
          :protocol-version protocol-version)))
      (t
       (ereport "42601" "unsupported replication command")))))

(defun replication-result-columns ()
  (mapcar (lambda (name)
            (make-result-column :name name :type-oid +textoid+ :typlen -1))
          '("slot_name" "consistent_point" "snapshot_name" "output_plugin")))

(defun send-create-replication-slot-result (stream name boundary token)
  (send-row-description stream (replication-result-columns))
  (send-data-row-text
   stream (list name (wal-format-lsn boundary) token "pgoutput"))
  (send-command-complete stream "CREATE_REPLICATION_SLOT")
  (send-ready-for-query stream #\I))

(defun replication-push-u64 (payload value)
  (unless (and (integerp value) (<= 0 value #xFFFFFFFFFFFFFFFF))
    (error "replication protocol integer is out of range: ~a" value))
  (loop for shift downfrom 56 to 0 by 8
        do (payload-push-byte payload (logand (ash value (- shift)) #xFF)))
  payload)

(defun replication-push-i64 (payload value)
  (replication-push-u64 payload (logand value #xFFFFFFFFFFFFFFFF)))

(defun replication-read-u64 (bytes offset)
  (unless (<= (+ offset 8) (length bytes))
    (ereport-fatal "08P01" "truncated logical replication protocol value"))
  (loop with value = 0
        for position from offset below (+ offset 8)
        do (setf value (logior (ash value 8) (aref bytes position)))
        finally (return value)))

(defun replication-standby-status (stream)
  (multiple-value-bind (type length-bytes payload) (read-message-frame stream)
    (declare (ignore length-bytes))
    (when (null type)
      (return-from replication-standby-status nil))
    (unless (and (= type (char-code #\d))
                 (= (length payload) 34)
                 (= (aref payload 0) (char-code #\r)))
      (ereport-fatal "08P01" "expected a standby status update in CopyBoth mode"))
    (list :write (replication-read-u64 payload 1)
          :flush (replication-read-u64 payload 9)
          :apply (replication-read-u64 payload 17))))

(defun send-copy-both-response (stream)
  ;; CopyBothResponse: text format and zero result columns.
  (write-message stream #\W #(0 0 0)))

(defun send-replication-copy-data (stream payload)
  (write-message stream #\d payload))

(defun send-logical-xlog-data (stream wal-start wal-end plugin-data)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\w))
    (replication-push-u64 payload wal-start)
    (replication-push-u64 payload wal-end)
    (replication-push-i64 payload 0)
    (payload-push payload plugin-data)
    (send-replication-copy-data stream payload)))

(defun send-logical-keepalive (stream wal-end)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\k))
    (replication-push-u64 payload wal-end)
    (replication-push-i64 payload 0)
    (payload-push-byte payload 1)
    (send-replication-copy-data stream payload)))

(defun pgoutput-type-oid (type)
  (or (cdr (assoc type +logical-replication-type-oids+))
      (error "unsupported pgoutput type ~a" type)))

(defun pgoutput-build-begin (transaction)
  ;; Compare with pgoutput_send_begin() and logicalrep_write_begin() in C.
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\B))
    (replication-push-u64 payload (logical-transaction-commit-lsn transaction))
    (replication-push-i64 payload 0)
    (payload-push-u32 payload (logical-transaction-xid transaction))
    payload))

(defun pgoutput-build-commit (transaction)
  ;; The source commit record is the transaction end LSN in this WAL format.
  (let ((lsn (logical-transaction-commit-lsn transaction))
        (payload (make-payload)))
    (payload-push-byte payload (char-code #\C))
    (payload-push-byte payload 0)
    (replication-push-u64 payload lsn)
    (replication-push-u64 payload lsn)
    (replication-push-i64 payload 0)
    payload))

(defun pgoutput-build-relation (change)
  ;; pgoutput.c sends Relation before row changes; KEY flags follow the selected
  ;; unique replica identity captured with each immutable WAL row image.
  (let ((payload (make-payload))
        (columns (logical-change-columns change))
        (key-attnums (logical-change-key-attnums change)))
    (payload-push-byte payload (char-code #\R))
    (payload-push-u32 payload (logical-change-relation-oid change))
    (payload-push-cstring payload "public")
    (payload-push-cstring payload (logical-change-relation-name change))
    (payload-push-byte payload (char-code #\d))
    (payload-push-u16 payload (length columns))
    (dolist (column columns)
      (payload-push-byte
       payload (if (member (getf column :attnum) key-attnums :test #'=) 1 0))
      (payload-push-cstring payload (getf column :name))
      (payload-push-u32 payload (pgoutput-type-oid (getf column :type)))
      (payload-push-u32 payload (logand (getf column :typmod) #xFFFFFFFF)))
    payload))

(defun pgoutput-value-text (value type)
  (if (eq value +null+)
      nil
      (case type
        ((:int4 :int8) (princ-to-string value))
        (:varchar value)
        (otherwise (error "unsupported pgoutput value type ~a" type)))))

(defun pgoutput-build-tuple (columns values &optional attnums)
  (let* ((selected
           (if attnums
               (remove-if-not
                (lambda (column)
                  (member (getf column :attnum) attnums :test #'=))
                columns)
               columns))
         (payload (make-payload)))
    (unless (= (length values) (length columns))
      (error "logical row image no longer matches its relation schema"))
    (payload-push-u16 payload (length selected))
    (dolist (column selected)
      (let* ((value (aref values (1- (getf column :attnum))))
             (text (pgoutput-value-text value (getf column :type))))
        (if text
            (let ((bytes (string->bytes text)))
              (payload-push-byte payload (char-code #\t))
              (payload-push-u32 payload (length bytes))
              (payload-push payload bytes))
            (payload-push-byte payload (char-code #\n)))))
    payload))

(defun pgoutput-build-row-change (change)
  (let* ((payload (make-payload))
         (keys (logical-change-key-attnums change))
         (relation-oid (logical-change-relation-oid change))
         (columns (logical-change-columns change)))
    (case (logical-change-kind change)
      (:insert
       (payload-push-byte payload (char-code #\I))
       (payload-push-u32 payload relation-oid)
       (payload-push-byte payload (char-code #\N))
       (payload-push payload
                     (pgoutput-build-tuple
                      columns (logical-change-new-values change))))
      (:update
       (payload-push-byte payload (char-code #\U))
       (payload-push-u32 payload relation-oid)
       (when (logical-change-old-values change)
         (payload-push-byte payload
                            (char-code (if keys #\K #\O)))
         (payload-push payload
                       (pgoutput-build-tuple
                        columns (logical-change-old-values change)
                        (and keys keys))))
       (payload-push-byte payload (char-code #\N))
       (payload-push payload
                     (pgoutput-build-tuple
                      columns (logical-change-new-values change))))
      (:delete
       (payload-push-byte payload (char-code #\D))
       (payload-push-u32 payload relation-oid)
       (payload-push-byte payload (char-code (if keys #\K #\O)))
       (payload-push payload
                     (pgoutput-build-tuple
                      columns (logical-change-old-values change)
                      (and keys keys))))
      (otherwise (error "unsupported logical change kind")))
    payload))

(defun pgoutput-message-prefix (change)
  (case (logical-change-kind change)
    (:truncate "pg_lisp/truncate")
    (:ddl "pg_lisp/ddl")
    (:sequence-state "pg_lisp/sequence-state")
    (otherwise (error "logical change kind has no Message prefix"))))

(defun pgoutput-build-message (change transaction)
  ;; logicalrep_write_message() in src/backend/replication/logical/proto.c
  ;; includes XID only for transactional messages, then flags, LSN, prefix,
  ;; and sized content. Version 2 enables that XID field for this worker.
  (let* ((payload (make-payload))
         (transactional-p (plusp (logical-transaction-xid transaction)))
         (content (string->bytes (logical-change-event-payload change)))
         (prefix (pgoutput-message-prefix change)))
    (payload-push-byte payload (char-code #\M))
    (when transactional-p
      (payload-push-u32 payload (logical-transaction-xid transaction)))
    (payload-push-byte payload (if transactional-p 1 0))
    (replication-push-u64 payload (logical-transaction-commit-lsn transaction))
    (payload-push-cstring payload prefix)
    (payload-push-u32 payload (length content))
    (payload-push payload content)))

(defun send-logical-transaction (stream transaction wal-end)
  ;; Use one XLogData frame per pgoutput message like the C logical output
  ;; plugin's prepare/write callback path.
  (let ((changes (logical-transaction-changes transaction)))
    (if (zerop (logical-transaction-xid transaction))
        (progn
          (unless (and (= (length changes) 1)
                       (member (logical-change-kind (first changes))
                               '(:ddl :sequence-state)))
            (error "catalog logical transaction must contain one standalone event"))
          (send-logical-xlog-data
           stream (logical-transaction-commit-lsn transaction) wal-end
           (pgoutput-build-message (first changes) transaction)))
        (progn
          (send-logical-xlog-data
           stream (logical-transaction-commit-lsn transaction) wal-end
           (pgoutput-build-begin transaction))
          (dolist (change changes)
            (if (eq (logical-change-kind change) :truncate)
                (send-logical-xlog-data
                 stream (logical-transaction-commit-lsn transaction) wal-end
                 (pgoutput-build-message change transaction))
                (progn
                  (send-logical-xlog-data
                   stream (logical-transaction-commit-lsn transaction) wal-end
                   (pgoutput-build-relation change))
                  (send-logical-xlog-data
                   stream (logical-transaction-commit-lsn transaction) wal-end
                   (pgoutput-build-row-change change)))))
          (send-logical-xlog-data
           stream (logical-transaction-commit-lsn transaction) wal-end
           (pgoutput-build-commit transaction))))))

(defun replication-status-position (status)
  (let ((write-lsn (getf status :write))
        (flush-lsn (getf status :flush))
        (apply-lsn (getf status :apply)))
    (unless (and (= write-lsn flush-lsn) (= flush-lsn apply-lsn))
      (ereport "22023"
               "subscriber write, flush, and apply LSNs must match in this worker"))
    flush-lsn))

(defun finalize-initial-replication-copy (slot-name requested-lsn status)
  (let ((position (replication-status-position status)))
    (unless (= position requested-lsn)
      (ereport "22023"
               "initial copy-complete status must equal the consistent LSN"))
    (wal-finalize-replication-slot-copy
     slot-name position *wal-logical-slot-owner*)
    ;; The imported source transaction has ended by the time the subscriber
    ;; sends this status, so snapmgr's exported registration can now be retired.
    (release-exported-logical-snapshot-for-slot slot-name)
    (setf *wal-logical-slot-read-progress* position)
    position))

(defun validate-stream-status (slot-name sent-lsn status)
  (let ((position (replication-status-position status)))
    (unless (<= (wal-replication-slot-confirmed-flush-lsn
                 (gethash slot-name *wal-replication-slots*))
                position sent-lsn)
      (ereport "22023" "standby status is outside sent logical replication progress"))
    (wal-logical-slot-acknowledge slot-name position)
    position))

(defun end-logical-copy-mode (stream)
  (write-message stream #\c (make-payload))
  (send-command-complete stream "START_REPLICATION")
  (send-ready-for-query stream #\I))

(defun start-logical-replication (stream slot-name requested-lsn)
  (let ((copy-mode-p nil))
    (handler-case
        (progn
          (wal-logical-slot-validate-start-position slot-name requested-lsn)
          (call-with-logical-replication-slot
           slot-name
           (lambda (slot)
             (let ((initial-copy-p
                     (null (wal-replication-slot-confirmed-flush-lsn slot))))
               (wal-logical-slot-validate-start-position
                slot-name requested-lsn)
               (send-copy-both-response stream)
               (setf copy-mode-p t)
               (let ((first-status (replication-standby-status stream)))
                 (unless first-status
                   (return-from start-logical-replication nil))
                 (if initial-copy-p
                     (finalize-initial-replication-copy
                      slot-name requested-lsn first-status)
                     (let ((position (replication-status-position first-status)))
                       (unless (= position requested-lsn)
                         (ereport "22023"
                                  "first resumed standby status must equal the requested LSN"))
                       (wal-logical-slot-accept-resume-position
                        slot-name requested-lsn))))
               (stream-logical-replication-loop stream slot-name requested-lsn)))
           :allow-initial-copy t)
          (when copy-mode-p (end-logical-copy-mode stream)))
      (pg-error (condition)
        (if copy-mode-p
            (progn
              (send-error-response stream condition)
              (end-logical-copy-mode stream))
            (progn
              (send-error-response stream condition)
              (send-ready-for-query stream #\I)))))))

(defun stream-logical-replication-loop (stream slot-name initial-lsn)
  (let ((sent-lsn initial-lsn))
    (loop
      (multiple-value-bind (transactions progress-lsn)
          (wal-logical-slot-read-transactions
           slot-name :max-transactions 1)
        (if transactions
            (let* ((transaction (first transactions))
                   (commit-lsn (logical-transaction-commit-lsn transaction)))
              (send-logical-transaction stream transaction progress-lsn)
              (setf sent-lsn commit-lsn)
              (let ((status (replication-standby-status stream)))
                (unless status (return))
                (setf sent-lsn
                      (validate-stream-status slot-name sent-lsn status))))
            (progn
              (when (> progress-lsn sent-lsn)
                (setf sent-lsn progress-lsn))
              (send-logical-keepalive stream sent-lsn)
              (let ((status (replication-standby-status stream)))
                (unless status (return))
                (validate-stream-status slot-name sent-lsn status)
                (sleep 0.05))))))))

(defun create-replication-slot-command (stream command)
  (multiple-value-bind (boundary token)
      (sb-thread:with-mutex (*logical-slot-lifecycle-lock*)
        (create-exported-logical-snapshot-slot
         (logical-replication-command-slot-name command)))
    (send-create-replication-slot-result
     stream (logical-replication-command-slot-name command) boundary token)))

(defun drop-replication-slot-command (stream command)
  (let ((name (logical-replication-command-slot-name command)))
    ;; Serialize slot removal with CREATE's slot/token publication so dropping
    ;; an old generation cannot release a newly recreated generation by name.
    (sb-thread:with-mutex (*logical-slot-lifecycle-lock*)
      (wal-drop-replication-slot name)
      (release-exported-logical-snapshot-for-slot name))
    (send-command-complete stream "DROP_REPLICATION_SLOT")
    (send-ready-for-query stream #\I)))

(defun replication-frame-query-string (type-byte length-bytes payload)
  (let* ((message (decode-frontend-message type-byte length-bytes payload))
         (query-payload (frontend-message-payload message))
         (end (or (position 0 query-payload) (length query-payload))))
    (bytes->string query-payload :start 0 :end end)))

(defun handle-replication-query-frame (stream type-byte length-bytes payload)
  (with-backend-query-execution (:backend *backend* :cleanup :query-state)
    (handler-case
        (let* ((query
                 (with-live-operation
                   (replication-frame-query-string
                    type-byte length-bytes payload)))
               (command (with-live-operation
                          (parse-replication-command query))))
          (case (logical-replication-command-type command)
            (:create
             (with-live-operation
               (create-replication-slot-command stream command)))
            (:drop
             (with-live-operation
               (drop-replication-slot-command stream command)))
            (:start
             (start-logical-replication
              stream (logical-replication-command-slot-name command)
              (logical-replication-command-requested-lsn command)))))
      (pg-error (condition)
        (if (fatal-error-p condition)
            (error condition)
            (progn
              (send-error-response stream condition)
              (send-ready-for-query stream #\I)))))))
