;;;; protocol.lisp -- logical replication commands and pgoutput v1 framing.

(in-package #:pg)

(defstruct logical-replication-command
  type slot-name plugin snapshot-action requested-lsn protocol-version options)

(defparameter +logical-replication-type-oids+
  '((:int2 . 21) (:int4 . 23) (:int8 . 20) (:varchar . 1043) (:text . 25) (:numeric . 1700) (:float4 . 700) (:float8 . 701) (:date . 1082) (:time . 1083) (:timestamp . 1114) (:timestamptz . 1184) (:interval . 1186) (:timetz . 1266)))

(defun replication-command-words (query)
  "Tokenize repl_scanner-style quoted words, C whitespace and command punctuation."
  (let ((words nil) (position 0) (length (length query)))
    (loop while (< position length)
          for character = (char query position)
          do (cond
               ((sql-array-space-p character) (incf position))
               ((find character '(#\( #\) #\, #\;))
                (push (string character) words) (incf position))
               ((find character '(#\' #\"))
                (let ((start position))
                  (setf position (nth-value 1 (scan-quoted-text query position character)))
                  (push (subseq query start position) words)))
               (t
                (let ((start position))
                  (loop while (and (< position length)
                                   (not (sql-array-space-p (char query position)))
                                   (not (find (char query position) '(#\( #\) #\, #\; #\' #\"))))
                        do (incf position))
                  (push (subseq query start position) words)))))
    (setf words (nreverse words))
    (when (equal (car (last words)) ";") (setf words (butlast words)))
    (when (member ";" words :test #'equal)
      (ereport "42601" "expected a single replication command"))
    words))

(defun replication-command-identifier (word)
  "Decode an IDENT with C's ASCII folding and UTF8-safe name clipping."
  (unless (and word (plusp (length word)))
    (ereport "42601" "expected a replication identifier"))
  (if (char= (char word 0) #\")
      (let ((value (scan-quoted-text word 0 #\")))
        (when (zerop (length value))
          (ereport "42601" "zero-length delimited identifier"))
        (sql-name-input value))
      (labels ((letter-p (character)
                 (or (find character "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
                     (>= (char-code character) 128))))
        (unless (and (letter-p (char word 0))
                     (every (lambda (character)
                              (or (letter-p character) (find character "0123456789$")))
                            word))
          (ereport "42601" "invalid replication identifier"))
        (sql-name-input
         (map 'string (lambda (character)
                        (if (<= 65 (char-code character) 90)
                            (code-char (+ 32 (char-code character))) character))
              word)))))

(defun replication-command-value (word)
  (cond
    ((null word) nil)
    ((char= (char word 0) #\') (scan-quoted-text word 0 #\'))
    ((every (lambda (character) (find character "0123456789")) word) word)
    (t (replication-command-identifier word))))

(defun parse-replication-generic-options (words)
  "Read the nonempty comma-separated generic_option_list in repl_gram.y."
  (unless (and (equal (first words) "(") (equal (car (last words)) ")"))
    (ereport "42601" "expected parenthesized replication options"))
  (let ((remaining (butlast (rest words))) (options nil))
    (unless remaining (ereport "42601" "expected a replication option"))
    (loop
      (let* ((name (replication-command-identifier (pop remaining)))
             (value (unless (or (null remaining) (equal (first remaining) ","))
                      (pop remaining))))
        (when value (replication-command-value value))
        (push (cons name value) options))
      (unless remaining (return))
      (unless (and (equal (pop remaining) ",") remaining)
        (ereport "42601" "expected another comma-separated replication option")))
    (nreverse options)))

(defun replication-option-boolean (name word)
  "Match defGetBoolean, including the distinction between UCONST and strings."
  (cond
    ((null word) t)
    ((every (lambda (character) (find character "0123456789")) word)
     (let ((value (parse-integer word)))
       (case value
         (0 nil) (1 t)
         (otherwise (ereport "42601" "~a requires a Boolean value" name)))))
    (t
     (let ((value (replication-command-value word)))
       (cond ((member value '("true" "on") :test #'string-equal) t)
             ((member value '("false" "off") :test #'string-equal) nil)
             (t (ereport "42601" "~a requires a Boolean value" name)))))))

(defun parse-create-slot-options (words)
  "Normalize legacy spelling and apply parseCreateReplSlotOptions rules."
  (let ((options
          (if (equal (first words) "(")
              (parse-replication-generic-options words)
              (loop for word in words
                    collect (cond
                              ((string-equal word "EXPORT_SNAPSHOT")
                               (cons "snapshot" "'export'"))
                              ((string-equal word "NOEXPORT_SNAPSHOT")
                               (cons "snapshot" "'nothing'"))
                              ((string-equal word "USE_SNAPSHOT")
                               (cons "snapshot" "'use'"))
                              ((string-equal word "TWO_PHASE") (cons "two_phase" nil))
                              ((string-equal word "RESERVE_WAL") (cons "reserve_wal" nil))
                              (t (ereport "42601" "invalid replication slot option"))))))
        (seen nil) (snapshot :export))
    (dolist (option options)
      (let ((name (car option)) (word (cdr option)))
        (when (member name seen :test #'equal)
          (ereport "42601" "conflicting or redundant options"))
        (push name seen)
        (cond
          ((equal name "snapshot")
           (unless word (ereport "42601" "snapshot requires a parameter"))
           (let ((value (replication-command-value word)))
             (setf snapshot
                   (cond ((equal value "export") :export)
                         ((equal value "nothing") :nothing)
                         ((equal value "use") :use)
                         (t (ereport "22023" "unrecognized snapshot action"))))))
          ((member name '("two_phase" "failover") :test #'equal)
           (when (replication-option-boolean name word)
             (ereport "0A000" "~a slots are not supported" name)))
          ((equal name "reserve_wal")
           (ereport "42601" "reserve_wal is only valid for physical slots"))
          (t (ereport "42601" "unrecognized replication slot option ~a" name)))))
    snapshot))

(defun parse-create-replication-slot (words)
  (let* ((slot-name (replication-command-identifier (second words)))
         (remaining (cddr words)))
    (when (string-equal (first remaining) "TEMPORARY")
      (ereport "0A000" "temporary replication slots are not supported"))
    (unless (and (string-equal (first remaining) "LOGICAL") (second remaining))
      (ereport "42601" "expected LOGICAL and an output plugin"))
    (unless (wal-valid-replication-slot-name-p slot-name)
      (ereport "42602" "invalid replication slot name ~s" slot-name))
    (let* ((plugin (replication-command-identifier (second remaining)))
           (snapshot (parse-create-slot-options (cddr remaining))))
      (unless (member plugin '("pgoutput" "pg_lisp") :test #'equal)
        (ereport "0A000" "unsupported output plugin ~s" plugin))
      (when (and (equal plugin "pg_lisp") (not (eq snapshot :export)))
        (ereport "0A000" "pg_lisp slots require an exported snapshot"))
      (make-logical-replication-command
       :type :create :slot-name slot-name :plugin plugin :snapshot-action snapshot))))

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

(declaim (ftype function parse-native-physical-start start-native-physical-replication))

(defun parse-timeline-history-command (words)
  (unless (and (= (length words) 2) (plusp (length (second words)))
               (every (lambda (character) (find character "0123456789")) (second words)))
    (ereport "42601" "expected TIMELINE_HISTORY positive_timeline"))
  (let ((timeline (parse-integer (second words))))
    (unless (<= 1 timeline #xffffffff) (ereport "42601" "invalid timeline ~a" timeline))
    (make-logical-replication-command
     :type :timeline-history :options (list (cons "timeline" timeline)))))

(defun parse-replication-command (query)
  (let* ((words (replication-command-words query))
         (verb (and words (string-upcase (first words)))))
    (cond
      ((equal verb "IDENTIFY_SYSTEM")
       (unless (= (length words) 1)
         (ereport "42601" "IDENTIFY_SYSTEM does not accept options"))
       (make-logical-replication-command :type :identify))
      ((equal verb "SHOW")
       (unless (= (length words) 2) (ereport "42601" "expected SHOW setting"))
       (make-logical-replication-command
        :type :show :options (list (cons "name" (replication-command-identifier (second words))))))
      ((equal verb "TIMELINE_HISTORY") (parse-timeline-history-command words))
      ((equal verb "CREATE_REPLICATION_SLOT") (parse-create-replication-slot words))
      ((equal verb "DROP_REPLICATION_SLOT")
       (unless (or (= (length words) 2)
                   (and (= (length words) 3) (string-equal (third words) "WAIT")))
         (ereport "42601" "expected DROP_REPLICATION_SLOT name [WAIT]"))
       (let ((name (replication-command-identifier (second words))))
         (unless (wal-valid-replication-slot-name-p name)
           (ereport "42602" "invalid replication slot name ~s" name))
         (make-logical-replication-command :type :drop :slot-name name
                                           :options (when (third words)
                                                      (list (cons "wait" t))))))
      ((and (equal verb "START_REPLICATION")
            (not (and (>= (length words) 4) (string-equal (fourth words) "LOGICAL"))))
       (parse-native-physical-start words))
      ((equal verb "START_REPLICATION")
       (unless (and (<= 5 (length words))
                    (string-equal (second words) "SLOT")
                    (string-equal (fourth words) "LOGICAL"))
         (ereport "42601" "expected START_REPLICATION SLOT name LOGICAL lsn"))
       (let ((name (replication-command-identifier (third words))))
         (unless (wal-valid-replication-slot-name-p name)
           (ereport "42602" "invalid replication slot name ~s" name))
         (multiple-value-bind (version options)
             (parse-start-replication-options (nthcdr 5 words))
           (make-logical-replication-command
            :type :start :slot-name name
            :requested-lsn (parse-replication-lsn (fifth words))
            :protocol-version version :options options))))
      (t (ereport "42601" "unsupported replication command")))))

(defun replication-result-columns ()
  (mapcar (lambda (name)
            (make-result-column :name name :type-oid +textoid+ :typlen -1))
          '("slot_name" "consistent_point" "snapshot_name" "output_plugin")))

(defun identify-replication-system (stream)
  "Return system identity and a WAL position paired with its timeline.
On a standby, receipt ahead of replay is reported only on the replay timeline."
  (send-row-description
   stream (list (make-result-column :name "systemid" :type-oid +textoid+ :typlen -1)
                (make-result-column :name "timeline" :type-oid +int8oid+ :typlen 8)
                (make-result-column :name "xlogpos" :type-oid +textoid+ :typlen -1)
                (make-result-column :name "dbname" :type-oid +textoid+ :typlen -1)))
  (multiple-value-bind (flushed timeline)
      (if (recovery-in-progress-p) (physical-recovery-identify-position)
          (sb-thread:with-mutex (*wal-lock*) (values *wal-durable-lsn* *wal-insert-timeline*)))
    (send-data-row-text
     stream (list (format nil "~d" (catalog-system-identifier)) (format nil "~d" timeline)
                  (wal-format-lsn flushed)
                  (unless (eq (backend-replication-mode *backend*) :physical)
                    (backend-database *backend*)))))
  (send-command-complete stream "IDENTIFY_SYSTEM")
  (send-ready-for-query stream (transaction-status-char)))

(defun send-create-replication-slot-result (stream name boundary token &optional (plugin "pg_lisp"))
  (send-row-description stream (replication-result-columns))
  (send-data-row-text
   stream (list name (wal-format-lsn boundary) token plugin))
  (send-command-complete stream "CREATE_REPLICATION_SLOT")
  (send-ready-for-query stream (transaction-status-char)))

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
    (replication-push-i64 payload (postgres-current-timestamp))
    (payload-push payload plugin-data)
    (send-replication-copy-data stream payload)))

(defun send-logical-keepalive (stream wal-end)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\k))
    (replication-push-u64 payload wal-end)
    (replication-push-i64 payload (postgres-current-timestamp))
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
        ((:int2 :int4 :int8) (princ-to-string value))
        ((:varchar :text) value)
        (:numeric (sql-numeric-output value))
        (:date (sql-date-output value))
        (:time (sql-time-output value))
        (:interval (sql-interval-output value))
        (:timetz (sql-timetz-output value))
        (:timestamp (sql-timestamp-output value nil))
        (:timestamptz (sql-timestamp-output value t))
        ((:float4 :float8) (sql-float-output value type))
        (otherwise (error "unsupported pgoutput value type ~a" type)))))

(defun pgoutput-build-tuple (columns values &optional key-attnums)
  ;; logicalrep_write_tuple() preserves relation width for old key images.
  ;; ReorderBuffer's identity tuple has NULL in non-key attributes.
  (let ((payload (make-payload)))
    (unless (= (length values) (length columns))
      (error "logical row image no longer matches its relation schema"))
    (payload-push-u16 payload (length columns))
    (dolist (column columns)
      (let* ((attnum (getf column :attnum))
             (value (if (or (null key-attnums) (member attnum key-attnums))
                        (aref values (1- attnum))
                        +null+))
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
    (:progress "pg_lisp/progress")
    (otherwise (error "logical change kind has no Message prefix"))))

(defun pgoutput-build-truncate (relation-oids)
  "Encode a plain nonstreamed TRUNCATE as logicalrep_write_truncate does."
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\T))
    (payload-push-u32 payload (length relation-oids))
    (payload-push-byte payload 0) ; neither CASCADE nor RESTART IDENTITY
    (dolist (oid relation-oids)
      (payload-push-u32 payload oid))
    payload))

(defun pgoutput-build-message (change transaction)
  ;; pgoutput_message() passes InvalidTransactionId outside streamed blocks.
  ;; An ordinary transactional Message has flags first, even with protocol v2.
  ;; See src/backend/replication/logical/proto.c:logicalrep_write_message.
  (let* ((payload (make-payload))
         (transactional-p (plusp (logical-transaction-xid transaction)))
         (content (string->bytes (logical-change-event-payload change)))
         (prefix (pgoutput-message-prefix change)))
    (payload-push-byte payload (char-code #\M))
    (payload-push-byte payload (if transactional-p 1 0))
    (replication-push-u64 payload (logical-transaction-commit-lsn transaction))
    (payload-push-cstring payload prefix)
    (payload-push-u32 payload (length content))
    (payload-push payload content)))

(defun pgoutput-build-safe-progress-message (lsn)
  "Encode a publisher-verified scan frontier as a nontransactional Message.

This private prefix distinguishes scanned-safe progress from the standard
keepalive's WAL-end field, which may include changes not yet applied."
  (pgoutput-build-message
   (make-logical-change :kind :progress :event-payload "")
   (make-logical-transaction :xid 0 :commit-lsn lsn)))

(defun send-logical-progress (stream lsn)
  "Send LSN only after the logical reader scanned through that safe boundary."
  (send-logical-xlog-data
   stream lsn lsn (pgoutput-build-safe-progress-message lsn)))

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

(defun finalize-initial-replication-copy (slot-name slot-object requested-lsn status)
  (let ((position (replication-status-position status))
        (finalized nil))
    (unless (= position requested-lsn)
      (ereport "22023"
               "initial copy-complete status must equal the consistent LSN"))
    ;; Keep the exported snapshot registered on validation and pre-finalization
    ;; failures. Once the slot terminal is durable, every exit path can retire it.
    (unwind-protect
         (progn
           (logical-test-failpoint
            :before-copy-slot-finalization slot-name position)
           (sb-sys:without-interrupts
             (wal-finalize-replication-slot-copy
              slot-name position *wal-logical-slot-owner*)
             (setf finalized t))
           ;; This process-crash point is now inside the release scope.
           (logical-test-failpoint
            :after-copy-slot-finalization-before-snapshot-release
            slot-name position))
      (when finalized
        ;; The imported source transaction has ended by the time the subscriber
        ;; sends this status, so snapmgr's exported registration can be retired.
        (release-exported-logical-snapshot-for-slot slot-name slot-object)))
    (when finalized
      (logical-test-failpoint
       :after-snapshot-schema-lease-release slot-name position)
      (setf *wal-logical-slot-read-progress* position))
    position))

(defun validate-stream-status (slot-name sent-lsn status)
  (let ((position (replication-status-position status)))
    (unless (<= (wal-replication-slot-confirmed-flush-lsn
                 (gethash slot-name *wal-replication-slots*))
                position sent-lsn)
      (ereport "22023" "standby status is outside sent logical replication progress"))
    (logical-test-failpoint :before-slot-acknowledgment slot-name position)
    (wal-logical-slot-acknowledge slot-name position)
    ;; LogicalConfirmReceivedLocation() makes the persisted consumer horizon
    ;; visible only after the slot file is durable.
    (logical-test-failpoint :after-slot-acknowledgment slot-name position)
    position))

(defun end-logical-copy-mode (stream)
  (write-message stream #\c (make-payload))
  (send-command-complete stream "START_REPLICATION")
  (send-ready-for-query stream #\I))

(defun start-logical-replication (stream slot-name requested-lsn &optional command)
  (when (and command
             (sb-thread:with-mutex (*wal-lock*)
               (let ((slot (gethash slot-name *wal-replication-slots*)))
                 (and slot (equal (wal-replication-slot-plugin slot) "pgoutput")))))
    (return-from start-logical-replication
      (start-standard-logical-replication stream command)))
  (when (and command
             (assoc "publication_names" (logical-replication-command-options command)
                    :test #'equal))
    (ereport "0A000" "publication options require a pgoutput slot"))
  (let ((copy-mode-p nil))
    (handler-case-nonfatal
     (progn
       (wal-logical-slot-validate-start-position slot-name requested-lsn)
       (call-with-logical-replication-slot
        slot-name
        (lambda (slot)
          (unless (equal (wal-replication-slot-plugin slot) "pg_lisp")
            (ereport "0A000" "standard pgoutput streaming is not implemented"))
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
                   slot-name slot requested-lsn first-status)
                  (let ((position (replication-status-position first-status)))
                    (unless (= position requested-lsn)
                      (ereport "22023"
                               "first resumed standby status must equal the requested LSN"))
                    (wal-logical-slot-accept-resume-position
                     slot-name requested-lsn))))
            (stream-logical-replication-loop stream slot-name requested-lsn)))
        :allow-initial-copy t)
       (when copy-mode-p (end-logical-copy-mode stream)))
     (condition
      (if copy-mode-p
          (progn
            (send-error-response stream condition)
            (end-logical-copy-mode stream))
          (progn
            (send-error-response stream condition)
            (send-ready-for-query stream #\I)))))))

(defun logical-scan-budget-limit (override failpoint action)
  "Return the scan budget, permitting a narrow value only for crash tests."
  (if (null override)
      +max-logical-read-scan-bytes+
      (progn
        (unless (and (string-equal (or failpoint "")
                                   "after-scan-progress-before-feedback")
                     (string-equal (or action "") "crash"))
          (error "test scan budget requires the safe-progress crash failpoint"))
        (let ((limit (ignore-errors
                      (parse-integer override :junk-allowed nil))))
          (unless (and limit (<= 1 limit +max-logical-read-scan-bytes+))
            (error "PG_LISP_TEST_LOGICAL_SCAN_BYTES must be between 1 and ~d"
                   +max-logical-read-scan-bytes+))
          limit))))

(defun logical-stream-scan-budget-bytes ()
  (logical-scan-budget-limit
   (sb-ext:posix-getenv "PG_LISP_TEST_LOGICAL_SCAN_BYTES")
   (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT")
   (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_ACTION")))

(defun stream-logical-replication-loop (stream slot-name initial-lsn)
  (let ((sent-lsn initial-lsn))
    (loop
      (multiple-value-bind (transactions progress-lsn)
          (wal-logical-slot-read-transactions
           slot-name
           :max-transactions 1
           :max-scan-bytes (logical-stream-scan-budget-bytes))
        (if transactions
            (let* ((transaction (first transactions))
                   (commit-lsn (logical-transaction-commit-lsn transaction)))
              (send-logical-transaction stream transaction progress-lsn)
              (setf sent-lsn commit-lsn)
              (let ((status (replication-standby-status stream)))
                (unless status (return))
                (setf sent-lsn
                      (validate-stream-status slot-name sent-lsn status))))
            (if (> progress-lsn sent-lsn)
                (progn
                  ;; Advance only through a bounded scan that found no
                  ;; subscriber-visible event, never through keepalive WAL-END.
                  (send-logical-progress stream progress-lsn)
                  (setf sent-lsn progress-lsn)
                  (let ((status (replication-standby-status stream)))
                    (unless status (return))
                    (setf sent-lsn
                          (validate-stream-status slot-name sent-lsn status))))
                (progn
                  (send-logical-keepalive stream sent-lsn)
                  (let ((status (replication-standby-status stream)))
                    (unless status (return))
                    (validate-stream-status slot-name sent-lsn status)
                    (sleep 0.05)))))))))

(defun create-replication-slot-command (stream command)
  (let ((name (logical-replication-command-slot-name command))
        (plugin (logical-replication-command-plugin command))
        (snapshot (logical-replication-command-snapshot-action command)))
    ;; Serialize DROP until the success row is sent, as C retains the slot's
    ;; active owner through do_tup_output. Sending failure still unlocks.
    (sb-thread:with-mutex (*logical-slot-lifecycle-lock*)
      (multiple-value-bind (boundary token)
          (if (equal plugin "pg_lisp")
              (create-exported-logical-snapshot-slot name)
              (create-standard-logical-slot name snapshot))
        (send-create-replication-slot-result stream name boundary token plugin)))))

(defun drop-replication-slot-command (stream command)
  (let ((name (logical-replication-command-slot-name command))
        (wait-p (assoc "wait" (logical-replication-command-options command)
                       :test #'equal)))
    (loop
      (check-for-interrupts)
      (when (handler-case
                (with-live-operation
                  ;; Never retain the lifecycle lock or reload admission while
                  ;; WAIT sleeps for an active sender to release its slot.
                  (sb-thread:with-mutex (*logical-slot-lifecycle-lock*)
                    (wal-drop-replication-slot name)
                    (release-exported-logical-snapshot-for-slot name))
                  t)
              (pg-error (condition)
                (unless (and wait-p (equal "55006" (pg-error-sqlstate condition)))
                  (error condition))
                nil))
        (return))
      (sleep 0.01))
    (with-live-operation
      (send-command-complete stream "DROP_REPLICATION_SLOT")
      (send-ready-for-query stream (transaction-status-char)))))

(defun replication-frame-query-string (type-byte length-bytes payload)
  (let* ((message (decode-frontend-message type-byte length-bytes payload))
         (query-payload (frontend-message-payload message))
         (end (or (position 0 query-payload) (length query-payload))))
    (bytes->string query-payload :start 0 :end end)))

(defun validate-replication-command-context (command)
  "Enforce transaction and recovery restrictions before touching replication state."
  (when (eq (backend-state *backend*) :aborted)
    (ereport "25P02"
             "current transaction is aborted, commands ignored until end of transaction block"))
  (check-recovery-available)
  (when (and (eq (backend-replication-mode *backend*) :physical)
             (member (logical-replication-command-type command) '(:create :start)))
    (ereport "55000" "logical decoding requires a database connection"))
  ;; All non-read-only commands retain their recovery write guard.
  (unless (member (logical-replication-command-type command) '(:identify :timeline-history :show))
    (prevent-recovery-write "logical replication state change"))
  (when (and (eq (logical-replication-command-type command) :create)
             (eq (logical-replication-command-snapshot-action command) :export)
             (backend-explicit *backend*))
    (ereport "25001" "EXPORT_SNAPSHOT cannot run inside a transaction block"))
  (when (and (member (logical-replication-command-type command) '(:start :physical-start :timeline-history))
             (backend-explicit *backend*))
    (ereport "25001" "~a cannot run inside a transaction block"
             (if (eq (logical-replication-command-type command) :timeline-history)
                 "TIMELINE_HISTORY" "START_REPLICATION"))))

(defun handle-replication-query-frame (stream type-byte length-bytes payload)
  (with-backend-query-execution (:backend *backend* :cleanup :query-state)
    (handler-case
        (let* ((query
                 (with-live-operation
                   (replication-frame-query-string
                    type-byte length-bytes payload)))
               (command (with-live-operation
                          (progn
                            (when (and (eq (backend-replication-mode *backend*) :physical)
                                       (not (replication-query-payload-p payload)))
                              (ereport "0A000" "cannot execute SQL in a physical replication connection"))
                            (parse-replication-command query)))))
          (with-live-operation (validate-replication-command-context command))
          (case (logical-replication-command-type command)
            (:identify
             (with-live-operation (identify-replication-system stream)))
            (:show
             (with-live-operation
               (exec-show stream (list :name (cdr (assoc "name" (logical-replication-command-options command)
                                                         :test #'string=))))
               (send-ready-for-query stream (transaction-status-char))))
            (:timeline-history
             (with-live-operation
               (send-timeline-history stream
                                      (cdr (assoc "timeline" (logical-replication-command-options command)
                                                  :test #'string=)))))
            (:create
             (with-live-operation
               (create-replication-slot-command stream command)))
            (:drop (drop-replication-slot-command stream command))
            (:physical-start (start-native-physical-replication stream command))
            (:start
             (start-logical-replication
              stream (logical-replication-command-slot-name command)
              (logical-replication-command-requested-lsn command) command))))
      (pg-error (condition)
        (if (fatal-error-p condition)
            (error condition)
            (with-live-operation-captured-backend-cleanup
              (note-query-error)
              (send-error-response stream condition)
              (send-ready-for-query stream (transaction-status-char))))))))
