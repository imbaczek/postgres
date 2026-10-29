;;;; logical-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-row-decoders-reject-malformed-before-apply
    (let ((insert (symbol-function 'logical-apply-insert-tuple))
          (update (symbol-function 'logical-apply-update-tuples))
          (delete (symbol-function 'logical-apply-delete-tuple))
          (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-apply-insert-tuple)
                   (lambda (&rest args) (declare (ignore args)) (incf calls))
                   (symbol-function 'logical-apply-update-tuples)
                   (lambda (&rest args) (declare (ignore args)) (incf calls))
                   (symbol-function 'logical-apply-delete-tuple)
                   (lambda (&rest args) (declare (ignore args)) (incf calls)))
             (dolist (entry (list (list #'logical-apply-insert #(78 0 0 1))
                                  (list #'logical-apply-update #(78 0 0 1))
                                  (list #'logical-apply-delete #(75 0 0 1))
                                  (list #'logical-apply-insert #(78 0))
                                  (list #'logical-apply-update #(78 0))
                                  (list #'logical-apply-update #(75 0 0 78 0))
                                  (list #'logical-apply-update #(75 0 0 78 0 0 1))
                                  (list #'logical-apply-delete #(75 0))))
               (rove:ok (handler-case
                            (progn (funcall (first entry) nil nil (second entry) 0) nil)
                          (error () t))))
             (rove:ok (zerop calls))
             (logical-apply-insert nil nil #(78 0 0) 0)
             (logical-apply-update nil nil #(78 0 0) 0)
             (logical-apply-delete nil nil #(75 0 0) 0)
             (rove:ok (= calls 3)))
        (setf (symbol-function 'logical-apply-insert-tuple) insert
              (symbol-function 'logical-apply-update-tuples) update
              (symbol-function 'logical-apply-delete-tuple) delete))))

(rove:deftest test-logical-row-change-codec
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
          (rove:ok (and (eq (logical-change-kind decoded)
                            (logical-change-kind event))
                        (= (logical-change-database-oid decoded) 7)
                        (= (logical-change-relation-oid decoded) 701)
                        (equal (logical-change-key-attnums decoded)
                               (logical-change-key-attnums event))
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
        (rove:ok (handler-case
                     (progn (logical-decode-change with-trailing-byte) nil)
                   (error () t))
                 "logical row payload rejects trailing bytes"))
      (let* ((long-name (make-string 70000 :initial-element #\r))
             (event (logical-test-change :insert 7 701
                                         :new-values #(11 12 "long name"))))
        (setf (logical-change-relation-name event) long-name)
        (rove:ok (string= long-name
                          (logical-change-relation-name
                           (logical-decode-change (logical-encode-change event))))
                 "logical schema names use lengths large enough for valid identifiers"))))

(rove:deftest test-logical-unchanged-toast-marker
    (let* ((event (logical-test-change :update 7 701 :old-values (vector 1 +null+ +null+)
                                                     :new-values (vector 1 2 +logical-unchanged-toast+)))
           (encoded (logical-encode-change event))
           (decoded (logical-decode-change encoded))
           (payload (pgoutput-build-row-change decoded)))
      (rove:ok (= 5 (aref encoded 1)))
      (rove:ok (eq +logical-unchanged-toast+ (aref (logical-change-new-values decoded) 2)))
      (multiple-value-bind (old new kind) (logical-pgoutput-read-update payload 5)
        (declare (ignore old kind))
        (rove:ok (eq :unchanged-toast (aref new 2))))
      (let ((*logical-validation-only* t))
        (rove:ok (eq +logical-unchanged-toast+
                     (aref (logical-change-new-values (logical-decode-change encoded)) 2))))
      (dolist (invalid (list
                        (logical-test-change :insert 7 701 :new-values (vector 1 2 +logical-unchanged-toast+))
                        (logical-test-change :update 7 701 :old-values (vector 1 2 +logical-unchanged-toast+)
                                                           :new-values (vector 1 2 "v"))
                        (logical-test-change :update 7 701 :key-attnums '(3)
                                                           :old-values (vector 1 2 "v")
                                                           :new-values (vector 1 2 +logical-unchanged-toast+))
                        (logical-test-change :update 7 701 :old-values (vector 1 2 "v")
                                                           :new-values (vector 1 +logical-unchanged-toast+ "v"))))
        (rove:ok (handler-case (progn (logical-encode-change invalid) nil) (error () t)))
        (rove:ok (handler-case (progn (pgoutput-build-row-change invalid) nil) (error () t))))
      (let* ((old-null (logical-test-change :update 7 701 :old-values (vector 1 2 +null+)
                                                          :new-values (vector 1 2 "v")))
             (old-text (logical-test-change :update 7 701 :old-values (vector 1 2 "x")
                                                          :new-values (vector 1 2 "v")))
             (bad (logical-encode-change old-null))
             (offset (mismatch bad (logical-encode-change old-text))))
        (setf (aref bad offset) 18)
        (dolist (validation-only '(nil t))
          (let ((*logical-validation-only* validation-only))
            (rove:ok (handler-case (progn (logical-decode-change bad) nil) (error () t))))))
      (rove:ok (handler-case
                   (progn (logical-read-value (make-logical-cursor :bytes #(18)) :int4) nil)
                 (error () t)))))

(rove:deftest test-logical-row-column-numbers-are-canonical
    (let* ((change (logical-test-change :update 7 701 :old-values (vector 1 2 +null+)
                                                      :new-values (vector 1 2 +logical-unchanged-toast+)))
           (encoded (logical-encode-change change))
           (cursor (make-logical-cursor :bytes encoded))
           (offsets nil))
      ;; Locate attribute numbers using the actual codec, rather than assuming
      ;; relation or column-name lengths.
      (logical-read-u8 cursor) (logical-read-u8 cursor) (logical-read-u8 cursor)
      (logical-read-u32 cursor)
      (rove:ok (= 0 (logical-read-u8 cursor)))
      (logical-read-u32 cursor) (logical-read-string-u32 cursor)
      (rove:ok (= 3 (logical-read-u16 cursor)))
      (dotimes (i 3)
        (push (logical-cursor-offset cursor) offsets)
        (logical-read-u16 cursor) (logical-read-string-u32 cursor)
        (logical-read-u8 cursor) (logical-read-u32 cursor) (logical-read-u8 cursor))
      (setf offsets (nreverse offsets))
      ;; The first two columns are swapped, duplicated, or gapped. In
      ;; particular the marker must never be interpreted against another attnum.
      (dolist (numbers '((2 1 3) (1 1 3) (1 3 4)))
        (let ((bad (copy-seq encoded))
              (bad-change (copy-logical-change change)))
          (setf (logical-change-columns bad-change) (copy-tree (logical-change-columns change)))
          (loop for number in numbers for offset in offsets
                for column in (logical-change-columns bad-change)
                do (page-set-u16 bad offset number)
                   (setf (getf column :attnum) number))
          (rove:ok (handler-case (progn (logical-encode-change bad-change) nil) (error () t)))
          (rove:ok (handler-case (progn (pgoutput-build-row-change bad-change) nil) (error () t)))
          (rove:ok (handler-case (progn (logical-decode-change bad) nil) (error () t)))
          (rove:ok (handler-case
                       (progn (logical-validate-change-from-cursor (make-logical-cursor :bytes bad)) nil)
                     (error () t)))
          (rove:ok (handler-case
                       (progn (logical-recovery-assembler-append
                               (make-logical-recovery-assembler 65536) (length bad) 0 bad) nil)
                     (error () t)))))))

(rove:deftest test-logical-capture-reused-toast-images
    (let* ((pointer (make-heap-external-varlena :raw-size 5004 :external-info 5000
                                                :value-id 5 :relation-id 7 :id-width 4))
           (old (vector 1 "old-key" "large"))
           (new (vector 2 "old-key" "large"))
           (reuse (vector nil pointer pointer)))
      (multiple-value-bind (old-image new-image)
          (logical-capture-row-images :update '(1 2) old new reuse)
        (rove:ok (equalp old-image (vector 1 "old-key" +null+)))
        (rove:ok (equalp new-image (vector 2 "old-key" +logical-unchanged-toast+)))
        (rove:ok (equalp old (vector 1 "old-key" "large")))
        (rove:ok (equalp new (vector 2 "old-key" "large"))))
      (multiple-value-bind (old-image new-image)
          (logical-capture-row-images :update nil old new reuse)
        (rove:ok (equalp old-image old))
        (rove:ok (equalp new-image new)))
      (multiple-value-bind (old-image new-image)
          (logical-capture-row-images :delete '(1) old nil nil)
        (rove:ok (equalp old-image (vector 1 +null+ +null+)))
        (rove:ok (null new-image)))))

(rove:deftest test-logical-copy-target-query-frame-boundary
    (let* ((within (make-string (- +max-query-message-size+ 5) :initial-element #\a))
           (over (concatenate 'string within "a")))
      (rove:ok (eq within (logical-copy-check-query-size within)))
      (rove:ok (handler-case (progn (logical-copy-check-query-size over) nil)
                 (error () t)))
      (setf (char within 0) #\ż)
      (rove:ok (handler-case (progn (logical-copy-check-query-size within) nil)
                 (error () t)))))

(rove:deftest test-logical-bounded-utf8-and-frame-view
    (let* ((value (concatenate 'string (make-string 4095 :initial-element #\a)
                               (string (code-char #x1f642)) "ż"))
           (payload (make-array 28 :element-type '(unsigned-byte 8) :initial-element 0))
           (original (symbol-function 'logical-pgoutput-dispatch))
           (seen nil))
      (rove:ok (= (logical-utf8-size-bounded value) (length (string->bytes value))))
      (setf (aref payload 0) (char-code #\w)
            (aref payload 25) (char-code #\X)
            (aref payload 26) 7
            (aref payload 27) 9)
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-pgoutput-dispatch)
                   (lambda (runtime view)
                     (declare (ignore runtime))
                     (multiple-value-bind (base offset) (array-displacement view)
                       (setf seen (and (eq base payload) (= offset 25)
                                       (equalp view #(88 7 9)))))))
             (logical-stream-frame nil payload)
             (rove:ok seen))
        (setf (symbol-function 'logical-pgoutput-dispatch) original))))

(rove:deftest test-client-streamed-query-frame
    (let* ((path (merge-pathnames (format nil "pg-query-frame-~d-~d" (sb-posix:getpid) (random 1000000))
                                  (uiop:temporary-directory)))
           (query (concatenate 'string (make-string 4095 :initial-element #\a) "🙂ż'\\"))
           (encoded (string->bytes query))
           (expected (make-payload)))
      (unwind-protect
           (progn
             (payload-push-byte expected (char-code #\Q))
             (payload-push expected (u32-to-bytes (+ 5 (length encoded))))
             (payload-push expected encoded)
             (payload-push-byte expected 0)
             (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                          :if-exists :supersede :if-does-not-exist :create)
               (pg-client-send-query (make-pg-client :stream stream) query))
             (with-open-file (stream path :element-type '(unsigned-byte 8))
               (let ((actual (make-array (file-length stream) :element-type '(unsigned-byte 8))))
                 (rove:ok (= (read-sequence actual stream) (length actual)))
                 (rove:ok (equalp actual expected)))))
        (when (probe-file path) (delete-file path)))))

(rove:deftest test-client-streamed-query-failure-closes-connection
    (let* ((path (merge-pathnames (format nil "pg-query-fail-~d-~d" (sb-posix:getpid) (random 1000000))
                                  (uiop:temporary-directory)))
           (original (symbol-function 'string->bytes))
           (calls 0)
           (stream nil))
      (unwind-protect
           (progn
             (setf stream (open path :direction :output :element-type '(unsigned-byte 8)
                                     :if-exists :supersede :if-does-not-exist :create))
             (let ((client (make-pg-client :stream stream)))
               (rove:ok (handler-case (progn (pg-client-send-query client (string (code-char 0))) nil)
                          (error () t)))
               (rove:ok (and (open-stream-p stream) (zerop (file-position stream))))
               (setf (symbol-function 'string->bytes)
                     (lambda (value)
                       (incf calls)
                       (if (= calls 2) (error "injected query write failure")
                           (funcall original value))))
               (rove:ok (handler-case (progn (pg-client-send-query client "abc") nil)
                          (error () t)))
               (rove:ok (not (open-stream-p stream)))))
        (setf (symbol-function 'string->bytes) original)
        (when (and stream (open-stream-p stream)) (close stream :abort t))
        (when (probe-file path) (delete-file path)))))

(rove:deftest test-logical-copy-frame-counts-sql-escaping
    (let* ((column (make-pg-client-column :name "payload" :type-oid 25))
           (row (vector (make-string (floor +max-query-message-size+ 2) :initial-element #\')))
           (query (logical-copy-insert-query "t" (list column) row)))
      (rove:ok (handler-case (progn (logical-copy-check-query-size query) nil)
                 (error () t))
               "doubling quotes cannot bypass the actual target-frame limit")))

(rove:deftest test-logical-copy-oversize-query-rolls-back
    (let ((original (symbol-function 'logical-target-query))
          (queries nil)
          (column (make-pg-client-column :name "payload" :type-oid 25)))
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-target-query)
                   (lambda (target query)
                     (declare (ignore target))
                     (push query queries)))
             (rove:ok
              (handler-case
                  (progn
                    (logical-copy-target-chunk
                     :target "t" (list column)
                     (list (vector (make-string (floor +max-query-message-size+ 2)
                                                :initial-element #\'))))
                    nil)
                (error () t)))
             (rove:ok (equal (nreverse queries) '("BEGIN" "ROLLBACK"))
                      "frame rejection rolls back before sending the oversized INSERT"))
        (setf (symbol-function 'logical-target-query) original))))

(rove:deftest test-logical-text-row-change
    (let* ((value (format nil "~a~a" "Zażółć " (make-string 3000 :initial-element #\x)))
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :text
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let ((decoded (logical-decode-change (logical-encode-change event))))
        (rove:ok (eq :text (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL retains the distinct text type")
        (rove:ok (equal value (aref (logical-change-new-values decoded) 2))
                 "logical WAL retains long UTF-8 text"))
      (rove:ok (= 25 (pgoutput-type-oid :text)) "pgoutput advertises text OID 25")
      (rove:ok (logical-type-supported-p 25) "subscriber accepts text OID 25")))

(rove:deftest test-logical-bytea-row-change
    (let* ((value (make-array 256 :element-type '(unsigned-byte 8)
                                  :initial-contents (loop for i below 256 collect i)))
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :bytea
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let ((decoded (logical-decode-change (logical-encode-change event))))
        (rove:ok (eq :bytea (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL preserves bytea type")
        (rove:ok (equalp value (aref (logical-change-new-values decoded) 2))
                 "logical WAL preserves arbitrary octets"))
      (rove:ok (= 17 (pgoutput-type-oid :bytea)) "pgoutput advertises bytea OID 17")
      (rove:ok (logical-type-supported-p 17) "subscriber accepts bytea OID 17")
      (rove:ok (equal (sql-bytea-output value) (pgoutput-value-text value :bytea))
               "pgoutput uses text-format bytea output")))

(rove:deftest test-logical-json-row-change
    (let* ((value "{ \"a\": 1, \"a\": [null, true] }")
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :json
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let ((decoded (logical-decode-change (logical-encode-change event))))
        (rove:ok (eq :json (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL preserves JSON type")
        (rove:ok (string= value (aref (logical-change-new-values decoded) 2))
                 "logical WAL preserves JSON source text"))
      (rove:ok (= 114 (pgoutput-type-oid :json)) "pgoutput advertises JSON OID 114")
      (rove:ok (logical-type-supported-p 114) "subscriber accepts JSON OID 114")))

(rove:deftest test-logical-jsonb-row-change
    (let* ((value (sql-jsonb-input "{\"b\":[null,1.00], \"a\":true}"))
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :jsonb
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let* ((decoded (logical-decode-change (logical-encode-change event)))
             (restored (aref (logical-change-new-values decoded) 2)))
        (rove:ok (eq :jsonb (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL preserves JSONB type")
        (rove:ok (zerop (sql-jsonb-compare value restored)) "logical WAL preserves JSONB value"))
      (rove:ok (= 3802 (pgoutput-type-oid :jsonb)) "pgoutput advertises JSONB OID 3802")
      (rove:ok (logical-type-supported-p 3802) "subscriber accepts JSONB OID 3802")))

(rove:deftest test-logical-pgoutput-v2-codec
    ;; Compare the v2 message shapes with pgoutput.c's Relation/Begin/Commit and
    ;; logicalrep_write_insert/update/delete in src/backend/replication/.
    (let* ((lsn #x0102030405060708)
           (insert (logical-test-change :insert 7 701 :new-values #(10 20 "oak")))
           (update (logical-test-change :update 7 701
                                        :old-values #(10 20 "oak")
                                        :new-values #(10 30 "elm")))
           (delete (logical-test-change :delete 7 701
                                        :old-values #(10 30 "elm")))
           (transaction (make-logical-transaction
                         :xid 91 :commit-lsn lsn
                         :changes (list insert update delete)))
           (relation (pgoutput-build-relation insert))
           (insert-payload (pgoutput-build-row-change insert))
           (update-payload (pgoutput-build-row-change update))
           (delete-payload (pgoutput-build-row-change delete))
           (begin (pgoutput-build-begin transaction))
           (commit (pgoutput-build-commit transaction)))
      (rove:ok (and (= (aref relation 0) (char-code #\R))
                    (= (bytes-to-u32 relation 1) 701)
                    (= (aref insert-payload 0) (char-code #\I))
                    (= (bytes-to-u32 insert-payload 1) 701))
               "pgoutput Relation and INSERT carry the source relation OID")
      (multiple-value-bind (tuple end)
          (logical-pgoutput-read-tuple insert-payload 6)
        (rove:ok (and (= (aref insert-payload 5) (char-code #\N))
                      (= end (length insert-payload))
                      (equalp tuple #("10" "20" "oak")))
                 "pgoutput INSERT encodes a complete text-format tuple"))
      (multiple-value-bind (key end)
          (logical-pgoutput-read-tuple update-payload 6)
        (rove:ok (and (= (aref update-payload 5) (char-code #\K))
                      (= (aref update-payload end) (char-code #\N))
                      (equalp key #("10" nil nil)))
                 "pgoutput UPDATE retains relation width in its old key tuple"))
      (multiple-value-bind (key end)
          (logical-pgoutput-read-tuple delete-payload 6)
        (rove:ok (and (= (aref delete-payload 5) (char-code #\K))
                      (= end (length delete-payload))
                      (equalp key #("10" nil nil)))
                 "pgoutput DELETE retains relation width in its old key tuple"))
      (rove:ok (string= (logical-row-key-predicate
                         #(nil) (list (make-pgoutput-column :name "id" :type-oid 23)))
                        "id IS NULL")
               "nullable NULLS NOT DISTINCT replica keys generate NULL-safe predicates")
      (rove:ok (and (= (aref begin 0) (char-code #\B))
                    (= (pg-client-u64 begin 1) lsn)
                    (= (bytes-to-u32 begin 17) 91)
                    (= (length commit) 26)
                    (= (aref commit 0) (char-code #\C))
                    (= (pg-client-u64 commit 2) lsn)
                    (= (pg-client-u64 commit 10) lsn))
               "pgoutput Begin and Commit retain their C wire field order and LSNs")
      (let* ((catalog-change
               (make-logical-change :kind :ddl :database-oid 7
                                    :event-payload "(:database-oid 7 :ast (:type :create-table))"))
             (standalone
               (pgoutput-build-message
                catalog-change
                (make-logical-transaction :xid 0 :commit-lsn lsn
                                          :changes (list catalog-change))))
             (truncate
               (make-logical-change :kind :truncate :database-oid 7
                                    :event-payload "(:tables (\"logical_rows\"))"))
             (transactional
               (pgoutput-build-message truncate transaction)))
        (rove:ok (and (= (aref standalone 0) (char-code #\M))
                      (= (aref standalone 1) 0)
                      (= (pg-client-u64 standalone 2) lsn))
                 "pgoutput v2 emits standalone DDL Message without an XID")
        (rove:ok (and (= (aref transactional 0) (char-code #\M))
                      (= (aref transactional 1) 1)
                      (= (pg-client-u64 transactional 2) lsn))
                 "ordinary transactional Message has flags and LSN without an XID")
        (multiple-value-bind (prefix event-lsn content)
            (logical-pgoutput-read-message
             (make-logical-subscription-runtime) standalone)
          (rove:ok (and (string= prefix "pg_lisp/ddl") (= event-lsn lsn)
                        (string= content (logical-change-event-payload catalog-change)))
                   "subscriber decodes standalone v2 Message fields"))
        (multiple-value-bind (prefix event-lsn content)
            (logical-pgoutput-read-message
             (make-logical-subscription-runtime
              :transaction-open-p t :source-xid 91)
             transactional)
          (rove:ok (and (string= prefix "pg_lisp/truncate") (= event-lsn lsn)
                        (string= content (logical-change-event-payload truncate)))
                   "subscriber validates ordinary transactional Message flags and content")))
      (rove:ok (eq :create
                   (logical-replication-command-type
                    (parse-replication-command
                     "CREATE_REPLICATION_SLOT unit_slot LOGICAL pgoutput EXPORT_SNAPSHOT")))
               "logical slot command parser accepts the supported pgoutput syntax")
      (rove:ok (eq :start
                   (logical-replication-command-type
                    (parse-replication-command
                     "START_REPLICATION SLOT unit_slot LOGICAL 0/0 (proto_version '2')")))
               "logical slot command parser accepts the worker's pgoutput v2 start command")
      (rove:ok (handler-case
                   (progn
                     (parse-replication-command
                      "START_REPLICATION SLOT unit_slot LOGICAL 0/0 (proto_version '1')")
                     nil)
                 (error () t))
               "logical slot command parser rejects pgoutput v1 for v2 Message support")))

(rove:deftest test-pgoutput-streamed-row-matches-builder
    (let* ((columns '((:attnum 1 :name "id" :type :int4 :typmod -1)
                      (:attnum 2 :name "b" :type :bytea :typmod -1)
                      (:attnum 3 :name "t" :type :text :typmod -1)
                      (:attnum 4 :name "j" :type :jsonb :typmod -1)))
           (bytes (make-array 33000 :element-type '(unsigned-byte 8)))
           (text (concatenate 'string (make-string 4095 :initial-element #\a) "🙂é"))
           (jsonb (sql-jsonb-input "{\"z\":[null,1.20],\"a\":\"🙂\"}"))
           (new (vector 7 bytes text jsonb))
           (old (vector 7 +null+ "old" jsonb)))
      (dotimes (i (length bytes)) (setf (aref bytes i) (mod i 256)))
      (dolist (change
               (list (make-logical-change :kind :insert :database-oid 7 :relation-oid 701
                                          :relation-name "items" :columns columns :key-attnums '(1)
                                          :new-values new)
                     (make-logical-change :kind :update :database-oid 7 :relation-oid 701
                                          :relation-name "items" :columns columns :key-attnums '(1)
                                          :old-values old :new-values (vector 7 +logical-unchanged-toast+ text jsonb))
                     (make-logical-change :kind :update :database-oid 7 :relation-oid 701
                                          :relation-name "items" :columns columns :old-values old :new-values new)
                     (make-logical-change :kind :delete :database-oid 7 :relation-oid 701
                                          :relation-name "items" :columns columns :key-attnums '(1)
                                          :old-values old)
                     (make-logical-change :kind :delete :database-oid 7 :relation-oid 701
                                          :relation-name "items" :columns columns :old-values old)))
        (let ((actual (make-payload)) (largest 0))
          (pgoutput-emit-row-change
           change (lambda (part)
                    (if (integerp part)
                        (payload-push-byte actual part)
                        (progn (setf largest (max largest (length part)))
                               (payload-push actual part)))))
          (rove:ok (equalp actual (pgoutput-build-row-change change)))
          (rove:ok (= (length actual) (pgoutput-row-change-size change)))
          (rove:ok (<= largest 65536))))))

(rove:deftest test-pgoutput-buffered-sink-boundaries
    (dolist (length '(0 65535 65536 65537 131073))
      (let ((source (make-array length :element-type '(unsigned-byte 8) :initial-element 97))
            (actual (make-payload)) (largest 0))
        (call-with-pgoutput-buffered-sink
         length
         (lambda (bytes count)
           (setf largest (max largest count))
           (loop for i below count do (payload-push-byte actual (aref bytes i))))
         (lambda (sink) (funcall sink source)))
        (rove:ok (equalp actual source))
        (rove:ok (<= largest 65536))))
  (dolist (length '(1 3))
    (rove:ok
     (handler-case
         (progn
           (call-with-pgoutput-buffered-sink length (lambda (bytes count) (declare (ignore bytes count)))
                                             (lambda (sink) (funcall sink #(1 2))))
           nil)
       (error (condition) (search "measured size" (princ-to-string condition)))))))

(rove:deftest test-private-streamed-row-failure-closes-frame
    (let* ((path (merge-pathnames
                  (format nil "pg-row-frame-~d-~d" (sb-posix:getpid) (random 1000000))
                  (uiop:temporary-directory)))
           (change (logical-test-change :insert 7 701 :new-values #(1 2 "value")))
           (original (symbol-function 'call-with-pgoutput-buffered-sink))
           (stream nil))
      (unwind-protect
           (progn
             (setf stream (open path :direction :output :element-type '(unsigned-byte 8)
                                     :if-exists :supersede :if-does-not-exist :create)
                   (symbol-function 'call-with-pgoutput-buffered-sink)
                   (lambda (&rest args) (declare (ignore args)) (error "injected row write failure")))
             (rove:ok (handler-case
                          (progn (send-logical-row-xlog-data stream 10 20 change) nil)
                        (error (condition) (search "injected row write failure" (princ-to-string condition)))))
             (rove:ok (not (open-stream-p stream))))
        (setf (symbol-function 'call-with-pgoutput-buffered-sink) original)
        (when (and stream (open-stream-p stream)) (close stream :abort t))
        (when (probe-file path) (delete-file path)))))

(rove:deftest test-private-streamed-row-frame-matches-builder
    (let* ((path (merge-pathnames
                  (format nil "pg-row-frame-ok-~d-~d" (sb-posix:getpid) (random 1000000))
                  (uiop:temporary-directory)))
           (change (logical-test-change :insert 7 701 :new-values #(1 2 "hé🙂")))
           (original (symbol-function 'postgres-current-timestamp))
           (envelope (pgoutput-xlog-envelope 10 20 30))
           (row (pgoutput-build-row-change change))
           (expected (make-payload)))
      (unwind-protect
           (progn
             (payload-push-byte expected (char-code #\d))
             (payload-push expected (u32-to-bytes (+ 4 (length envelope) (length row))))
             (payload-push expected envelope)
             (payload-push expected row)
             (setf (symbol-function 'postgres-current-timestamp) (lambda () 30))
             (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                          :if-exists :supersede :if-does-not-exist :create)
               (send-logical-row-xlog-data stream 10 20 change))
             (with-open-file (stream path :element-type '(unsigned-byte 8))
               (let ((actual (make-array (file-length stream) :element-type '(unsigned-byte 8))))
                 (rove:ok (= (length actual) (read-sequence actual stream)))
                 (rove:ok (equalp actual expected)))))
        (setf (symbol-function 'postgres-current-timestamp) original)
        (when (probe-file path) (delete-file path)))))

(rove:deftest test-logical-catalog-event-codec-and-parser
    (let* ((ast (list :type :alter-sequence :name "counter" :restart 19))
           (bytes (logical-encode-catalog-event 7 ast))
           (change (logical-decode-catalog-event bytes))
           (envelope (logical-read-data-string
                      (logical-change-event-payload change)))
           (sql (format nil "APPLY LOGICAL EVENT ~a 12345 ~a"
                        (logical-sql-literal "source:7:slot")
                        (logical-sql-literal
                         (catalog-readable-data-string envelope))))
           (apply-ast (first (parse-statements sql))))
      (rove:ok (and (eq (logical-change-kind change) :ddl)
                    (= (logical-change-database-oid change) 7)
                    (= (getf envelope :database-oid) 7)
                    (equal (getf envelope :ast) ast))
               "catalog event preserves its source database and supported DDL AST")
      (rove:ok (and (eq (getf apply-ast :type) :apply-logical-event)
                    (string= (getf apply-ast :event-key) "source:7:slot")
                    (= (getf apply-ast :event-lsn) 12345)
                    (string= (getf apply-ast :payload)
                             (catalog-readable-data-string envelope)))
               "private APPLY LOGICAL EVENT SQL parses the idempotent event envelope")
      (rove:ok (handler-case
                   (progn (logical-read-data-string "(:type :ddl) (:type :drop-table)") nil)
                 (error () t))
               "logical event parser rejects trailing S-expression payloads")
      (rove:ok (handler-case
                   (progn (logical-read-data-string "#.(error \"unsafe\")") nil)
                 (error () t))
               "logical event parser disables reader evaluation")))

(rove:deftest test-logical-bootstrap-private-index-projection
    (let* ((progress-name "pg_lisp_subscription_progress")
           (config-name "pg_lisp_subscription_config")
           (ordinary-name "logical_guard_rows")
           (progress-source-index "publisher_progress_source_lsn_idx")
           (config-source-index "publisher_config_source_port_idx")
           (ordinary-index "logical_guard_rows_label_idx")
           (ordinary-lookalike-index
             "pg_lisp_subscription_progress_label_idx")
           (source
             (list :database-oid 7
                   :tables
                   (list (list :name progress-name
                               :sql "CREATE TABLE pg_lisp_subscription_progress (source_lsn varchar)")
                         (list :name config-name
                               :sql "CREATE TABLE pg_lisp_subscription_config (source_port integer)")
                         (list :name ordinary-name
                               :sql "CREATE TABLE logical_guard_rows (label varchar)"))
                   :indexes
                   (list (list :table-name progress-name
                               :name progress-source-index
                               :sql "CREATE INDEX publisher_progress_source_lsn_idx ON pg_lisp_subscription_progress (source_lsn)")
                         (list :table-name config-name
                               :name config-source-index
                               :sql "CREATE INDEX publisher_config_source_port_idx ON pg_lisp_subscription_config (source_port)")
                         (list :table-name ordinary-name
                               :name ordinary-index
                               :sql "CREATE INDEX logical_guard_rows_label_idx ON logical_guard_rows (label)")
                         (list :table-name ordinary-name
                               :name ordinary-lookalike-index
                               :sql "CREATE INDEX pg_lisp_subscription_progress_label_idx ON logical_guard_rows (label)"))
                   :sequences nil))
           (target
             (list :database-oid 7
                   :tables
                   (list (list :name progress-name
                               :sql "CREATE TABLE pg_lisp_subscription_progress (source_slot varchar PRIMARY KEY, source_lsn varchar, state varchar NOT NULL, snapshot_token varchar)")
                         (list :name config-name
                               :sql "CREATE TABLE pg_lisp_subscription_config (source_slot varchar PRIMARY KEY, source_host varchar NOT NULL, source_port integer NOT NULL, source_database varchar NOT NULL, table_allowlist varchar NOT NULL)")
                         (list :name ordinary-name
                               :sql "CREATE TABLE logical_guard_rows (label varchar)"))
                   :indexes
                   (list (list :table-name progress-name
                               :name "pg_lisp_subscription_progress_pkey"
                               :sql "CREATE UNIQUE INDEX pg_lisp_subscription_progress_pkey ON pg_lisp_subscription_progress (source_slot)")
                         (list :table-name config-name
                               :name "pg_lisp_subscription_config_pkey"
                               :sql "CREATE UNIQUE INDEX pg_lisp_subscription_config_pkey ON pg_lisp_subscription_config (source_slot)")
                         (list :table-name ordinary-name
                               :name ordinary-index
                               :sql "CREATE INDEX logical_guard_rows_label_idx ON logical_guard_rows (label)")
                         (list :table-name ordinary-name
                               :name ordinary-lookalike-index
                               :sql "CREATE INDEX pg_lisp_subscription_progress_label_idx ON logical_guard_rows (label)"))
                   :sequences nil))
           (source-view (logical-replication-bootstrap-view source))
           (target-view (logical-replication-bootstrap-view target))
           (source-contract (logical-bootstrap-schema-contract source-view))
           (target-contract (logical-bootstrap-schema-contract target-view)))
      (rove:ok (equal (mapcar (lambda (entry) (getf entry :name))
                              (getf source-view :tables))
                      (list ordinary-name))
               "bootstrap projection omits both subscriber-private tables")
      (rove:ok (equal (mapcar (lambda (entry) (getf entry :name))
                              (getf source-view :indexes))
                      (list ordinary-index ordinary-lookalike-index))
               "bootstrap projection filters private indexes by owning table")
      (rove:ok (and (equal source-contract target-contract)
                    (logical-schema-contract-subset-compatible-p
                     source-contract target-contract))
               "private table and index differences are outside the schema contract")
      (let ((private-source (copy-tree source))
            (private-target (copy-tree target)))
        (setf (getf private-source :tables)
              (subseq (getf private-source :tables) 0 2)
              (getf private-source :indexes)
              (subseq (getf private-source :indexes) 0 2)
              (getf private-target :tables)
              (subseq (getf private-target :tables) 0 2)
              (getf private-target :indexes)
              (subseq (getf private-target :indexes) 0 2))
        (rove:ok (handler-case
                     (progn
                       (logical-validate-existing-replication-bootstrap
                        nil nil
                        (logical-replication-bootstrap-view private-source)
                        (logical-replication-bootstrap-view private-target)
                        (make-logical-subscription-options))
                       t)
                   (error () nil))
                 "private table schema differences bypass copy-schema validation"))
      (dolist (malformed-index
               (list '(:name "missing_owner_idx" :sql "CREATE INDEX missing_owner_idx")
                     '(:table-name 7 :name "non_string_owner_idx"
                       :sql "CREATE INDEX non_string_owner_idx")
                     '(:table-name "missing_table" :name "unknown_owner_idx"
                       :sql "CREATE INDEX unknown_owner_idx")
                     '(:table-name "logical_guard_rows" :name "duplicate_owner_idx"
                       :sql "CREATE INDEX duplicate_owner_idx"
                       :table-name "pg_lisp_subscription_progress")))
        (let ((malformed (copy-tree source)))
          (setf (getf malformed :indexes) (list malformed-index))
          (rove:ok (handler-case
                       (progn (logical-replication-bootstrap-view malformed) nil)
                     (error () t))
                   "malformed or unknown bootstrap index owners are rejected")))))

(rove:deftest test-logical-private-metadata-ddl-targets
    (let* ((database (make-database :name "postgres" :oid 7))
           (progress-name (first +logical-private-schema-table-names+))
           (config-name (second +logical-private-schema-table-names+))
           (ordinary-name "logical_guard_rows")
           (progress-index-name "pg_lisp_subscription_progress_pkey")
           (config-index-name "pg_lisp_subscription_config_pkey")
           (ordinary-index-name "logical_guard_rows_pkey")
           (qualified-progress-name (concatenate 'string "public." progress-name))
           (qualified-config-name (concatenate 'string "public." config-name))
           (qualified-progress-index-name
             (concatenate 'string "public." progress-index-name))
           (progress-table
             (make-pg-class :oid 701 :name progress-name :relfilenode 1701))
           (config-table
             (make-pg-class :oid 702 :name config-name :relfilenode 1702))
           (ordinary-table
             (make-pg-class :oid 703 :name ordinary-name :relfilenode 1703))
           (cases nil))
      (setf (gethash progress-name (database-tables database)) progress-table
            (gethash config-name (database-tables database)) config-table
            (gethash ordinary-name (database-tables database)) ordinary-table
            (gethash progress-index-name (database-indexes database))
            (make-pg-index :oid 801 :name progress-index-name
                           :relfilenode 1801 :table-oid 701)
            (gethash config-index-name (database-indexes database))
            (make-pg-index :oid 802 :name config-index-name
                           :relfilenode 1802 :table-oid 702)
            (gethash ordinary-index-name (database-indexes database))
            (make-pg-index :oid 803 :name ordinary-index-name
                           :relfilenode 1803 :table-oid 703))
      (setf cases
            (list
             (list (list :type :create-table :name progress-name :if-not-exists t)
                   progress-name)
             ;; The parser has already stripped one public prefix here; the
             ;; executor's lookup-table strips another before resolving the name.
             (list (list :type :create-table :name qualified-progress-name
                         :if-not-exists t)
                   progress-name)
             (list (list :type :drop-table
                         :names (list ordinary-name config-name) :if-exists t)
                   config-name)
             (list (list :type :drop-table
                         :names (list qualified-progress-name) :if-exists t)
                   nil)
             (list (list :type :alter-table :table progress-name :actions nil)
                   progress-name)
             (list (list :type :alter-table :table qualified-progress-name
                         :actions nil)
                   progress-name)
             (list (list :type :create-index :table config-name
                         :name "config_extra_idx")
                   config-name)
             (list (list :type :create-index :table qualified-config-name
                         :name "qualified_config_idx")
                   config-name)
             (list (list :type :create-index :table ordinary-name
                         :name progress-index-name)
                   progress-name)
             (list (list :type :create-index :table ordinary-name
                         :name qualified-progress-index-name)
                   progress-name)
             (list (list :type :drop-index
                         :names (list ordinary-index-name config-index-name))
                   config-name)
             (list (list :type :drop-index
                         :names (list qualified-progress-index-name))
                   nil)
             (list (list :type :alter-index :name progress-index-name
                         :new-name "renamed_progress_idx")
                   progress-name)
             (list (list :type :alter-index :name qualified-progress-index-name
                         :new-name "renamed_progress_idx")
                   progress-name)
             (list (list :type :create-table :name ordinary-name) nil)
             (list (list :type :drop-table :names (list ordinary-name)) nil)
             (list (list :type :alter-table :table ordinary-name :actions nil) nil)
             (list (list :type :create-index :table ordinary-name
                         :name "ordinary_extra_idx")
                   nil)
             (list (list :type :drop-index :names (list ordinary-index-name)) nil)
             (list (list :type :alter-sequence :name "ordinary_seq" :increment 2)
                   nil)))
      (dolist (case cases)
        (rove:ok (equal (logical-private-metadata-ddl-target-name
                         database (first case))
                        (second case))
                 "replicated DDL guard classifies protected table and index targets"))))

(rove:deftest test-logical-scan-budget-override
    (rove:ok (= (logical-scan-budget-limit nil nil nil)
                +max-logical-read-scan-bytes+)
             "logical reader uses the production scan budget by default")
  (rove:ok (= (logical-scan-budget-limit
               "1" "after-scan-progress-before-feedback" "crash")
              1)
           "safe-progress crash tests can narrow the scan budget")
  (dolist (value '("0" "not-a-number"))
    (rove:ok (handler-case
                 (progn
                   (logical-scan-budget-limit
                    value "after-scan-progress-before-feedback" "crash")
                   nil)
               (error () t))
             "logical scan budget rejects invalid values"))
  (rove:ok (handler-case
               (progn
                 (logical-scan-budget-limit
                  (write-to-string (1+ +max-logical-read-scan-bytes+))
                  "after-scan-progress-before-feedback" "crash")
                 nil)
             (error () t))
           "logical scan budget cannot exceed the production limit")
  (dolist (gate '(("after-target-commit-before-feedback" "crash")
                  ("after-scan-progress-before-feedback" "error")))
    (rove:ok (handler-case
                 (progn (apply #'logical-scan-budget-limit "1" gate) nil)
               (error () t))
             "logical scan budget requires its dedicated crash failpoint")))

(rove:deftest test-create-sequence-parser-bounds
    (let* ((explicit
             (first (parse-statements
                     "CREATE SEQUENCE bounded_counter INCREMENT BY -2 MINVALUE -90 MAXVALUE 5 START WITH 4")))
           (default-descending
             (first (parse-statements
                     "CREATE SEQUENCE descending_counter INCREMENT BY -2"))))
      (rove:ok (and (= (getf explicit :increment) -2)
                    (= (getf explicit :min) -90)
                    (= (getf explicit :max) 5)
                    (= (getf explicit :start) 4))
               "CREATE SEQUENCE parses explicit int8 minimum and maximum bounds")
      (rove:ok (and (= (getf default-descending :min) +int8-min+)
                    (= (getf default-descending :max) -1)
                    (= (getf default-descending :start) -1))
               "descending sequence defaults follow sequence.c init_params")
      (rove:ok (handler-case
                   (progn
                     (parse-statements
                      "CREATE SEQUENCE invalid_counter MINVALUE 20 MAXVALUE 10")
                     nil)
                 (error () t))
               "CREATE SEQUENCE rejects bounds that do not contain its start")))

(rove:deftest test-logical-subscription-argument-parser
    (let ((options
            (parse-logical-subscription-args
             '("--slot" "test_slot" "--source-port" "5434"
               "--target-port" "5435" "--table" "first_rows"
               "--table" "second_rows"))))
      (rove:ok (and (= (logical-subscription-options-source-port options) 5434)
                    (= (logical-subscription-options-target-port options) 5435)
                    (equal (logical-subscription-options-tables options)
                           '("first_rows" "second_rows")))
               "subscriber CLI consumes option values and retains repeated tables")
      (rove:ok (handler-case
                   (progn
                     (parse-logical-subscription-args
                      '("--slot" "test_slot" "--table"))
                     nil)
                 (error () t))
               "subscriber CLI rejects an option with no value")
      (dolist (table +logical-private-schema-table-names+)
        (rove:ok (handler-case
                     (progn
                       (parse-logical-subscription-args
                        (list "--slot" "test_slot" "--table" table))
                       nil)
                   (error () t))
                 "subscriber CLI rejects private metadata tables in --table"))))

(rove:deftest test-logical-subscription-resume-config
    (let* ((options (make-logical-subscription-options
                     :source-host "publisher.example" :source-port 5440
                     :source-database "publisher_db"
                     :source-identity "cluster-a"
                     :tables '("z_rows" "a_rows")))
           (config (list :source-host "publisher.example"
                         :source-identity "cluster-a"
                         :source-port 5440
                         :source-database "publisher_db"
                         :table-allowlist "a_rows,z_rows")))
      (rove:ok (string= (fourth (logical-subscription-config-values options))
                        "a_rows,z_rows")
               "subscriber config canonicalizes allowlist ordering")
      (rove:ok (null (logical-validate-resume-config options '(:state "ready") config))
               "matching source and allowlist permit a safe resume")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config
                      (make-logical-subscription-options
                       :source-host "other.example" :source-port 5440
                       :source-database "publisher_db" :tables '("z_rows" "a_rows"))
                      '(:state "ready") config)
                     nil)
                 (error () t))
               "subscriber config rejects changing the source endpoint")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config
                      (make-logical-subscription-options
                       :source-host "publisher.example" :source-port 5440
                       :source-database "publisher_db" :tables '("a_rows"))
                      '(:state "ready") config)
                     nil)
                 (error () t))
               "subscriber config rejects changing the table allowlist")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config options '(:state "ready") nil)
                     nil)
                 (error () t))
               "legacy progress without a saved config cannot resume unsafely")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config
                      (make-logical-subscription-options
                       :source-host "publisher.example" :source-port 5440
                       :source-database "publisher_db"
                       :source-identity "replacement-cluster"
                       :tables '("z_rows" "a_rows"))
                      '(:state "ready") config)
                     nil)
                 (error () t))
               "a replacement publisher incarnation cannot reuse the old event marker")))

(rove:deftest test-logical-spool-spills-and-cleans-up
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-spool-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 3)
           (first (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(1 2)))
           (second (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(3 4 5)))
           (spool (make-logical-spool)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (logical-spool-append spool first)
             (rove:ok (null (logical-spool-path spool)))
             (logical-spool-append spool second)
             (rove:ok (probe-file (logical-spool-path spool)))
             (rove:ok (null (logical-spool-events spool)))
             (logical-spool-seal spool)
             (rove:ok (equalp (logical-spool-events-in-order spool) (list first second)))
             (close-logical-spool spool)
             (close-logical-spool spool)
             (rove:ok (null (uiop:directory-files directory))))
        (close-logical-spool spool)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-rejects-quota-and-corruption
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-spool-fault-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (event (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(1 2))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (let* ((*query-temp-file-limit-bytes* 9)
                    (spool (make-logical-spool)))
               (unwind-protect
                    (progn
                      (rove:ok (handler-case (progn (logical-spool-append spool event) nil)
                                 (pg-error (condition) (string= (pg-error-sqlstate condition) "53400"))))
                      (rove:ok (logical-spool-failed-p spool))
                      (rove:ok (handler-case (progn (logical-spool-seal spool) nil)
                                 (error () t))))
                 (close-logical-spool spool)))
             (let ((*query-temp-file-limit-bytes* nil)
                   (spool (make-logical-spool)))
               (unwind-protect
                    (progn
                      (logical-spool-append spool event)
                      (logical-spool-seal spool)
                      (file-position (logical-spool-stream spool) 8)
                      (write-byte 99 (logical-spool-stream spool))
                      (finish-output (logical-spool-stream spool))
                      (rove:ok (handler-case (progn (logical-spool-events-in-order spool) nil)
                                 (error () t))))
                 (close-logical-spool spool)))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-outlives-query-and-ends-with-transaction
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-xact-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*backend* (make-backend))
           (event (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (with-query-temp-files
               (logical-capture-encoded-event *backend* event 3))
             (let ((spool (backend-logical-spool *backend*)))
               (rove:ok (probe-file (logical-spool-path spool)))
               (logical-spool-seal spool)
               (rove:ok (equalp (logical-spool-events-in-order spool) (list event)))
               (reset-backend-transaction *backend*)
               (rove:ok (null (backend-logical-spool *backend*)))
               (rove:ok (null (probe-file (logical-spool-path spool))))))
        (when (backend-logical-spool *backend*) (reset-backend-transaction *backend*))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-cleanup-failure-retains-owner
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-cleanup-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*query-temp-pending-cleanups* nil)
           (*backend* (make-backend))
           (event (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (original (symbol-function 'unlink-query-temp-file)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (logical-capture-encoded-event *backend* event 3)
             (let ((spool (backend-logical-spool *backend*)))
               (logical-spool-seal spool)
               (call-with-logical-spool-owner
                spool (lambda () (finish-query-temp-stream (logical-spool-stream spool) t))))
             (setf (symbol-function 'unlink-query-temp-file)
                   (lambda (path) (declare (ignore path)) (error "injected unlink failure")))
             (reset-backend-transaction *backend*)
             (rove:ok (null (backend-logical-spool *backend*)))
             (rove:ok (= 1 (getf (query-temp-pending-cleanup-status) :records)))
             (rove:ok (= 1 (length (uiop:directory-files directory))))
             (setf (symbol-function 'unlink-query-temp-file) original)
             (retry-pending-query-temp-cleanup)
             (rove:ok (zerop (getf (query-temp-pending-cleanup-status) :records)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'unlink-query-temp-file) original)
        (retry-pending-query-temp-cleanup)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-wal-plan-boundaries
    (let* ((*logical-capture-memory-bytes* (* 64 1024 1024))
           (legacy-limit (- +wal-segment-size+ +wal-header-size+
                            +logical-change-chunk-header-size+ 4))
           (small (make-array 1 :element-type '(unsigned-byte 8) :initial-element 1))
           (exact (make-array legacy-limit :element-type '(unsigned-byte 8) :initial-element 2))
           (larger (make-array (1+ legacy-limit) :element-type '(unsigned-byte 8) :initial-element 3)))
      (let ((spool (make-logical-spool)))
        (unwind-protect
             (progn
               (logical-spool-append spool exact)
               (let ((plan (logical-preflight-spool spool)))
                 (rove:ok (eq (logical-spool-wal-plan-mode plan) :legacy))
                 (rove:ok (= (logical-spool-wal-plan-count plan) 1))))
          (close-logical-spool spool)))
      (let ((spool (make-logical-spool)))
        (unwind-protect
             (progn
               (logical-spool-append spool small)
               (logical-spool-append spool larger)
               (logical-spool-append spool small)
               (let ((plan (logical-preflight-spool spool)))
                 (rove:ok (eq (logical-spool-wal-plan-mode plan) :fragment))
                 (rove:ok (= (logical-spool-wal-plan-count plan) 4))))
          (close-logical-spool spool)))
      (let ((spool (make-logical-spool)))
        (unwind-protect
             (progn
               (logical-spool-append spool small)
               (logical-spool-append spool exact)
               (let ((plan (logical-preflight-spool spool)))
                 (rove:ok (eq (logical-spool-wal-plan-mode plan) :legacy))
                 (rove:ok (= (logical-spool-wal-plan-count plan) 2))))
          (close-logical-spool spool)))))

(rove:deftest test-logical-encoder-stream-matches-vector
    (let* ((text (concatenate 'string (make-string 4095 :initial-element #\a) "🙂é"))
           (change (logical-test-change :update 7 701 :old-values (vector 1 2 text)
                                                      :new-values (vector 3 4 text)))
           (expected (logical-encode-change change))
           (actual (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)))
      (logical-encode-change change
                             (lambda (part)
                               (if (integerp part)
                                   (vector-push-extend part actual)
                                   (loop for byte across part do (vector-push-extend byte actual)))))
      (rove:ok (equalp actual expected))
      (rove:ok (equalp (logical-change-new-values (logical-decode-change actual))
                       (vector 3 4 text)))))

(rove:deftest test-logical-spool-append-change-boundaries
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-encode-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (change (logical-test-change :insert 7 701 :new-values (vector 1 2 "hello🙂")))
           (expected (logical-encode-change change))
           (size (length expected)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (multiple-value-bind (sink metrics) (logical-encoding-sink size)
               (logical-encode-change change sink)
               (multiple-value-bind (actual-size crc) (funcall metrics)
                 (rove:ok (= actual-size size))
                 (rove:ok (= crc (wal-crc32 expected)))))
             (let ((spool (make-logical-spool)))
               (unwind-protect
                    (progn
                      (rove:ok (= (logical-spool-append-change spool change size) size))
                      (rove:ok (probe-file (logical-spool-path spool)))
                      (logical-spool-seal spool)
                      (rove:ok (equalp (logical-spool-events-in-order spool) (list expected))))
                 (close-logical-spool spool)))
             (let ((spool (make-logical-spool)))
               (unwind-protect
                    (progn
                      (rove:ok (handler-case
                                   (progn (logical-spool-append-change spool change (1- size)) nil)
                                 (pg-error (condition) (string= (pg-error-sqlstate condition) "54000"))))
                      (rove:ok (logical-spool-failed-p spool))
                      (rove:ok (zerop (logical-spool-count spool))))
                 (close-logical-spool spool)))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-encoder-stream-bytea-and-jsonb
    (dolist (case (list (list :bytea (make-array 256 :element-type '(unsigned-byte 8)
                                                     :initial-contents (loop for i below 256 collect i)))
                        (list :jsonb (sql-jsonb-input "{\"a\":[null,\"🙂\",1.00]}"))))
      (let* ((event (logical-test-change :insert 7 701 :new-values (vector 1 2 (second case))))
             (actual (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)))
        (setf (getf (third (logical-change-columns event)) :type) (first case)
              (getf (third (logical-change-columns event)) :typmod) -1)
        (let ((expected (logical-encode-change event)))
          (logical-encode-change event
                                 (lambda (part)
                                   (if (integerp part)
                                       (vector-push-extend part actual)
                                       (loop for byte across part do (vector-push-extend byte actual)))))
          (rove:ok (equalp expected actual))))))

(rove:deftest test-logical-jsonb-bounded-utf8-output
    (let* ((large (concatenate 'string (make-string 4095 :initial-element #\a)
                               "🙂" (make-string 5000 :initial-element #\Newline)))
           (value (sql-jsonb-input
                   (with-output-to-string (out)
                     (write-string "{\"nested\":[1.00," out)
                     (sql-jsonb-write-string large out)
                     (write-string "]}" out))))
           (expected (sb-ext:string-to-octets (sql-jsonb-output value) :external-format :utf-8))
           (actual (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0))
           (largest 0))
      (logical-write-jsonb-u32
       (lambda (part)
         (if (integerp part)
             (vector-push-extend part actual)
             (progn (setf largest (max largest (length part)))
                    (loop for byte across part do (vector-push-extend byte actual)))))
       value)
      (rove:ok (= (length expected)
                  (loop for i below 4 sum (ash (aref actual i) (* 8 i)))))
      (rove:ok (equalp expected (subseq actual 4)))
      (rove:ok (<= largest (* 4096 4)))
      (rove:ok (> (length expected) largest))))

(rove:deftest test-logical-spool-detects-change-between-passes
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-mutation-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (change (logical-test-change :insert 7 701 :new-values (vector 1 2 (copy-seq "hello"))))
           (*logical-spool-test-after-measure*
             (lambda (item) (setf (char (aref (logical-change-new-values item) 2) 0) #\H)))
           (spool (make-logical-spool)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (rove:ok (handler-case (progn (logical-spool-append-change spool change 10000) nil)
                        (error (condition)
                          (search "logical event changed during encoding" (princ-to-string condition)))))
             (rove:ok (probe-file (logical-spool-path spool)))
             (rove:ok (char= (char (aref (logical-change-new-values change) 2) 0) #\H))
             (rove:ok (logical-spool-failed-p spool))
             (rove:ok (zerop (logical-spool-count spool)))
             (rove:ok (zerop (logical-spool-bytes spool)))
             (rove:ok (handler-case (progn (logical-spool-seal spool) nil) (error () t)))
             (close-logical-spool spool)
             (rove:ok (null (uiop:directory-files directory))))
        (close-logical-spool spool)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-bounded-block-reader
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-blocks-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (event (make-array 1000 :element-type '(unsigned-byte 8)
                                   :initial-contents (loop for i below 1000 collect (mod i 256))))
           (second (make-array 513 :element-type '(unsigned-byte 8) :initial-element 42))
           (spool (make-logical-spool))
           (actual (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0))
           (starts '()) (ends 0) (maximum 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (logical-spool-append spool event)
             (logical-spool-append spool second)
             (logical-spool-seal spool)
             (logical-spool-map-event-blocks
              spool (lambda (size) (push size starts))
              (lambda (bytes start end)
                (setf maximum (max maximum (- end start)))
                (loop for i from start below end do (vector-push-extend (aref bytes i) actual)))
              (lambda () (incf ends)) :block-size 127)
             (rove:ok (equal (nreverse starts) '(1000 513)))
             (rove:ok (= ends 2))
             (rove:ok (<= maximum 127))
             (rove:ok (equalp actual (concatenate '(vector (unsigned-byte 8)) event second)))
             (file-position (logical-spool-stream spool) 18)
             (write-byte 99 (logical-spool-stream spool))
             (finish-output (logical-spool-stream spool))
             (rove:ok (handler-case
                          (progn (logical-spool-map-event-blocks spool (lambda (size) (declare (ignore size)))
                                                                 (lambda (bytes start end) (declare (ignore bytes start end)))
                                                                 (lambda ()) :block-size 127) nil)
                        (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
             (rove:ok (logical-spool-failed-p spool)))
        (close-logical-spool spool)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-cursors-verify-and-expire
    (let* ((directory (merge-pathnames
                       (format nil "pg-cursor-visit-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (text-value (make-string 200000 :initial-element #\x))
           (first (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values (vector 1 2 text-value))))
           (second (logical-encode-change
                    (logical-test-change :insert 7 701 :new-values #(3 4 "tail"))))
           (spool (make-logical-spool))
           (values nil) (retained nil) (nested nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (logical-spool-append spool first)
             (logical-spool-append spool second)
             (logical-spool-seal spool)
             (logical-spool-map-event-cursors
              spool
              (lambda (cursor)
                (rove:ok (streamp (logical-cursor-stream cursor)))
                (setf retained cursor)
                (push (logical-change-new-values (logical-decode-change-from-cursor cursor)) values)
                (setf nested
                      (handler-case
                          (progn (logical-spool-map-events spool #'identity) nil)
                        (error (condition) (search "already being visited" (princ-to-string condition)))))
                (rove:ok (handler-case (progn (close-logical-spool spool) nil)
                           (error (condition) (search "already being visited" (princ-to-string condition)))))))
             (rove:ok nested)
             (rove:ok (equalp (nreverse values) (list (vector 1 2 text-value) #(3 4 "tail"))))
             (rove:ok (handler-case (progn (logical-read-u8 retained) nil) (error () t)))
             (rove:ok (not (logical-spool-failed-p spool))))
        (close-logical-spool spool)
        (rove:ok (null (uiop:directory-files directory)))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-cursor-corruption-and-bounds
    (let* ((directory (merge-pathnames
                       (format nil "pg-cursor-fault-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (event (logical-encode-change
                   (logical-test-change :insert 7 701
                                        :new-values (vector 1 2 (make-string 200000 :initial-element #\x)))))
           (spool (make-logical-spool)) (calls 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (logical-spool-append spool event)
             (logical-spool-seal spool)
             (let ((stream (logical-spool-stream spool)))
               (file-position stream (+ 8 100000))
               (write-byte 99 stream)
               (finish-output stream))
             (rove:ok
              (handler-case
                  (progn (logical-spool-map-event-cursors
                          spool (lambda (cursor) (declare (ignore cursor)) (incf calls))) nil)
                (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
             (rove:ok (zerop calls))
             (rove:ok (logical-spool-failed-p spool))
             (close-logical-spool spool)
             (setf spool (make-logical-spool))
             (logical-spool-append spool event)
             (logical-spool-seal spool)
             (rove:ok
              (handler-case
                  (progn (logical-spool-map-event-cursors spool (lambda (cursor) (logical-read-u8 cursor))) nil)
                (error (condition) (search "did not consume" (princ-to-string condition)))))
             (rove:ok (logical-spool-failed-p spool)))
        (close-logical-spool spool)
        (rove:ok (null (uiop:directory-files directory)))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-spool-memory-cursor-bounds
    (let* ((*logical-capture-memory-bytes* 1024)
           (spool (make-logical-spool))
           (events nil) (retained nil))
      (unwind-protect
           (progn
             (logical-spool-append spool (make-array 2 :element-type '(unsigned-byte 8)
                                                       :initial-contents '(1 2)))
             (logical-spool-append spool (make-array 1 :element-type '(unsigned-byte 8)
                                                       :initial-contents '(3)))
             (logical-spool-seal spool)
             (logical-spool-map-event-cursors
              spool
              (lambda (cursor)
                (setf retained cursor)
                (let* ((length (logical-cursor-limit cursor))
                       (bytes (logical-read-octets cursor length)))
                  (rove:ok (handler-case (progn (logical-read-u8 cursor) nil) (error () t)))
                  (push bytes events))))
             (rove:ok (equalp (nreverse events) (list #(1 2) #(3))))
             (rove:ok (handler-case (progn (logical-read-u8 retained) nil) (error () t))))
        (close-logical-spool spool))))

(rove:deftest test-logical-spool-concurrent-visitor-rejected
    (let* ((*logical-capture-memory-bytes* 1024)
           (spool (make-logical-spool))
           (entered (sb-thread:make-semaphore :count 0))
           (release (sb-thread:make-semaphore :count 0))
           (thread nil))
      (unwind-protect
           (progn
             (logical-spool-append spool (make-array 1 :element-type '(unsigned-byte 8)
                                                       :initial-contents '(7)))
             (logical-spool-seal spool)
             (setf thread
                   (sb-thread:make-thread
                    (lambda ()
                      (logical-spool-map-event-cursors
                       spool
                       (lambda (cursor)
                         (sb-thread:signal-semaphore entered)
                         (sb-thread:wait-on-semaphore release)
                         (logical-read-u8 cursor))))))
             (rove:ok (sb-thread:wait-on-semaphore entered :timeout 5))
             (rove:ok (handler-case (progn (logical-spool-map-events spool #'identity) nil)
                        (error (condition) (search "already being visited" (princ-to-string condition)))))
             (sb-thread:signal-semaphore release)
             (sb-thread:join-thread thread)
             (setf thread nil)
             (rove:ok (not (logical-spool-failed-p spool))))
        (when thread
          (sb-thread:signal-semaphore release)
          (sb-thread:join-thread thread))
        (close-logical-spool spool))))

(rove:deftest test-logical-spool-cursor-callback-remains-interruptible
    (let* ((*logical-capture-memory-bytes* 1024)
           (spool (make-logical-spool))
           (entered (sb-thread:make-semaphore :count 0))
           (release (sb-thread:make-semaphore :count 0))
           (thread nil) (retained nil))
      (unwind-protect
           (progn
             (logical-spool-append spool (make-array 1 :element-type '(unsigned-byte 8)
                                                       :initial-contents '(7)))
             (logical-spool-seal spool)
             (setf thread
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (progn
                            (logical-spool-map-event-cursors
                             spool
                             (lambda (cursor)
                               (setf retained cursor)
                               (rove:ok sb-sys:*interrupts-enabled*)
                               (sb-thread:signal-semaphore entered)
                               (sb-thread:wait-on-semaphore release)
                               (logical-read-u8 cursor)))
                            :completed)
                        (error (condition)
                          (if (search "injected visitor interrupt" (princ-to-string condition))
                              :interrupted :unexpected))))))
             (rove:ok (sb-thread:wait-on-semaphore entered :timeout 5))
             (sb-thread:interrupt-thread thread (lambda () (error "injected visitor interrupt")))
             (let ((result (sb-thread:join-thread thread :timeout 2 :default :timed-out)))
               (when (eq result :timed-out)
                 (sb-thread:signal-semaphore release)
                 (sb-thread:join-thread thread))
               (setf thread nil)
               (rove:ok (eq :interrupted result)))
             (rove:ok (logical-spool-failed-p spool))
             (rove:ok (handler-case (progn (logical-read-u8 retained) nil) (error () t))))
        (when thread
          (sb-thread:signal-semaphore release)
          (sb-thread:join-thread thread))
        (close-logical-spool spool))))

(rove:deftest test-logical-spool-cursor-respects-outer-interrupt-mask
    (let* ((*logical-capture-memory-bytes* 1024)
           (spool (make-logical-spool))
           (was-masked nil))
      (unwind-protect
           (progn
             (logical-spool-append spool (make-array 1 :element-type '(unsigned-byte 8)
                                                       :initial-contents '(7)))
             (logical-spool-seal spool)
             (sb-sys:without-interrupts
               (logical-spool-map-event-cursors
                spool (lambda (cursor)
                        (setf was-masked (not sb-sys:*interrupts-enabled*))
                        (logical-read-u8 cursor))))
             (rove:ok was-masked))
        (close-logical-spool spool))))

(rove:deftest test-logical-spool-cursor-framing-and-unwind
    (let* ((directory (merge-pathnames
                       (format nil "pg-cursor-frame-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (event (make-array 1024 :element-type '(unsigned-byte 8) :initial-element 7)))
      (ensure-directories-exist directory)
      (unwind-protect
           (dolist (mode '(:header :payload :length :trailing :seek :unwind))
             (let ((spool (make-logical-spool)) (calls 0) (retained nil))
               (unwind-protect
                    (progn
                      (logical-spool-append spool event)
                      (logical-spool-seal spool)
                      (let ((stream (logical-spool-stream spool)))
                        (case mode
                          (:header (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) 4))
                          (:payload (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) (+ 8 1023)))
                          (:length (file-position stream 0)
                           (write-byte 255 stream)
                           (write-byte 255 stream)
                           (write-byte 255 stream)
                           (write-byte 127 stream)
                           (finish-output stream))
                          (:trailing (file-position stream (+ 8 1024))
                           (write-byte 9 stream)
                           (finish-output stream))))
                      (if (eq mode :unwind)
                          (rove:ok
                           (eq :escaped
                               (block escaped
                                 (logical-spool-map-event-cursors
                                  spool (lambda (cursor)
                                          (setf retained cursor)
                                          (return-from escaped :escaped)))
                                 :not-escaped)))
                          (rove:ok
                           (handler-case
                               (progn
                                 (logical-spool-map-event-cursors
                                  spool (lambda (cursor)
                                          (incf calls)
                                          (setf retained cursor)
                                          (logical-read-octets cursor 1024)
                                          (when (eq mode :seek)
                                            (file-position (logical-cursor-stream cursor) 0))))
                                 nil)
                             (error () t))))
                      (rove:ok (logical-spool-failed-p spool))
                      (when (member mode '(:header :payload :length))
                        (rove:ok (zerop calls)))
                      (when retained
                        (rove:ok (handler-case (progn (logical-read-u8 retained) nil) (error () t)))))
                 (close-logical-spool spool))))
        (rove:ok (null (uiop:directory-files directory)))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-fragment-payload-from-bounded-buffer
    (let* ((event (make-array 32 :element-type '(unsigned-byte 8)
                                 :initial-contents (loop for i below 32 collect i)))
           (fragment (make-logical-fragment :event event :start 3 :end 19))
           (whole (logical-encode-fragment 1 3 fragment))
           (bounded (logical-encode-fragment-bytes 1 3 32 3 (subseq event 3 19))))
      (rove:ok (equalp whole bounded))
      (multiple-value-bind (index count total offset bytes) (logical-decode-fragment bounded)
        (rove:ok (equal (list index count total offset) '(1 3 32 3)))
        (rove:ok (equalp bytes (subseq event 3 19))))))

(rove:deftest test-logical-change-stream-backed-cursor
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-cursor-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (change (logical-test-change :update 7 701 :old-values #(1 2 "old🙂")
                                                      :new-values #(1 3 "new🙂")))
           (event (logical-encode-change change))
           (spool (make-logical-spool)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (logical-spool-append spool event)
             (logical-spool-seal spool)
             (file-position (logical-spool-stream spool) 8)
             (let* ((cursor (make-logical-cursor :stream (logical-spool-stream spool)
                                                 :size (length event)))
                    (decoded (logical-decode-change-from-cursor cursor)))
               (rove:ok (eq (logical-change-kind decoded) :update))
               (rove:ok (equalp (logical-change-old-values decoded) #(1 2 "old🙂")))
               (rove:ok (equalp (logical-change-new-values decoded) #(1 3 "new🙂")))
               (rove:ok (= (logical-cursor-offset cursor) (length event))))
             (file-position (logical-spool-stream spool) 8)
             (rove:ok (handler-case
                          (progn (logical-decode-change-from-cursor
                                  (make-logical-cursor :stream (logical-spool-stream spool)
                                                       :size (1- (length event)))) nil)
                        (error () t))))
        (close-logical-spool spool)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-stream-cursor-codec-equivalence
    (let* ((directory (merge-pathnames
                       (format nil "pg-logical-cursor-cases-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (spool (make-logical-spool))
           (octets (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(0 128 255)))
           (binary (logical-test-change :insert 7 701 :new-values (vector -7 -9 octets)))
           (jsonb (logical-test-change :insert 7 702 :new-values (vector 4 5 (sql-jsonb-input "{\"a\":[1,true]}"))))
           (truncate (make-logical-change :kind :truncate :database-oid 7 :event-payload "(:tables (\"x\"))"))
           (events nil))
      (setf (getf (third (logical-change-columns binary)) :type) :bytea
            (getf (third (logical-change-columns binary)) :typmod) -1
            (getf (third (logical-change-columns jsonb)) :type) :jsonb
            (getf (third (logical-change-columns jsonb)) :typmod) -1
            events (mapcar #'logical-encode-change (list binary jsonb truncate)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (dolist (event events) (logical-spool-append spool event))
             (logical-spool-seal spool)
             (file-position (logical-spool-stream spool) 0)
             (dolist (event events)
               (let* ((size (logical-spool-read-u32 (logical-spool-stream spool)))
                      (checksum (logical-spool-read-u32 (logical-spool-stream spool)))
                      (cursor (make-logical-cursor :stream (logical-spool-stream spool) :size size))
                      (decoded (logical-decode-change-from-cursor cursor)))
                 (rove:ok (= checksum (wal-crc32 event)))
                 (rove:ok (equalp (logical-encode-change decoded) event))))
             (let ((stream (logical-spool-stream spool)))
               (rove:ok (handler-case
                            (progn (logical-read-u8 (make-logical-cursor :stream stream :size 1)) nil)
                          (error () t)))))
        (close-logical-spool spool)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-validation-utf8-matches-sbcl
    (dolist (bytes '((0) (65 195 169) (240 159 153 130) (192 128) (224 128 128)
                     (237 160 128) (244 144 128 128) (245 128 128 128)
                     (226 130) (128) (255) (239 191 191)))
      (let* ((vector (make-array (length bytes) :element-type '(unsigned-byte 8) :initial-contents bytes))
             (reference (handler-case (stringp (sb-ext:octets-to-string vector :external-format :utf-8))
                          (error () nil)))
             (actual (handler-case (progn (logical-validate-utf8 (make-logical-cursor :bytes vector)
                                                                 (length vector)) t)
                       (error () nil))))
        (rove:ok (eq actual reference)))))

(rove:deftest test-logical-validation-discarded-fields
    (dolist (case (list (list :text (concatenate 'string "ą🙂" (make-string 70000 :initial-element #\x)))
                        (list :bytea (make-array 70000 :element-type '(unsigned-byte 8) :initial-element 255))))
      (let* ((event (logical-test-change :insert 7 701 :new-values (vector 1 2 (second case)))))
        (setf (getf (third (logical-change-columns event)) :type) (first case)
              (getf (third (logical-change-columns event)) :typmod) -1)
        (let* ((bytes (logical-encode-change event))
               (cursor (make-logical-cursor :bytes bytes)))
          (logical-validate-change-from-cursor cursor)
          (rove:ok (= (logical-cursor-offset cursor) (length bytes)))
          (rove:ok (equalp (logical-change-new-values (logical-decode-change bytes))
                           (logical-change-new-values event)))))))

(rove:deftest test-logical-recovery-validates-fragmented-values
    (let* ((change (logical-test-change :insert 7 701 :new-values #(1 2 "abc")))
           (good (logical-encode-change change))
           (bad-utf8 (copy-seq good))
           (bad-tag (copy-seq good))
           (truncated (subseq good 0 (1- (length good))))
           (binary (logical-test-change :insert 7 701
                                        :new-values (vector 1 2 (make-array 3 :element-type '(unsigned-byte 8)
                                                                              :initial-contents '(1 2 3)))))
           (truncated-binary nil))
      (setf (getf (third (logical-change-columns binary)) :type) :bytea
            (getf (third (logical-change-columns binary)) :typmod) -1)
      (let ((encoded (logical-encode-change binary)))
        (setf truncated-binary (subseq encoded 0 (1- (length encoded)))))
      (setf (aref bad-utf8 (1- (length bad-utf8))) #xff
            (aref bad-tag (- (length bad-tag) 8)) 15)
      (flet ((accepts (event)
               (let* ((*wal-recovery-group-active* t)
                      (*wal-recovery-logical-xid* nil)
                      (*wal-recovery-logical-chunk-count* nil)
                      (*wal-recovery-logical-next-chunk* 0)
                      (*wal-recovery-logical-bytes* 0)
                      (fragment (make-logical-fragment :event event :start 0 :end (length event)))
                      (record (make-wal-record :type +wal-record-logical-changes+ :xid 55
                                               :payload (logical-encode-fragment 0 1 fragment))))
                 (handler-case (progn (wal-recovery-accept-logical-change-chunk record)
                                      (wal-recovery-finish-logical-change-batch 55) t)
                   (error () nil)))))
        (rove:ok (accepts good))
        (rove:ok (not (accepts bad-utf8)))
        (rove:ok (not (accepts bad-tag)))
        (rove:ok (not (accepts truncated)))
        (rove:ok (not (accepts truncated-binary))))))

(rove:deftest test-logical-recovery-spool-lifetime
    (let* ((directory (merge-pathnames
                       (format nil "pg-recovery-spool-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-temp-file-limit-bytes* 0)
           (*logical-capture-memory-bytes* 1)
           (event (logical-encode-change (logical-test-change :insert 7 701 :new-values #(1 2 "recover🙂"))))
           (middle (floor (length event) 2))
           (first (make-logical-fragment :event event :start 0 :end middle))
           (second (make-logical-fragment :event event :start middle :end (length event)))
           (one (make-wal-record :type +wal-record-logical-changes+ :xid 91
                                 :payload (logical-encode-fragment 0 2 first)))
           (two (make-wal-record :type +wal-record-logical-changes+ :xid 91
                                 :payload (logical-encode-fragment 1 2 second))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (with-wal-replay-staging ()
               (setf *wal-recovery-group-active* t)
               (wal-recovery-accept-logical-change-chunk one)
               (let ((assembler (logical-fragment-batch-assembler *wal-recovery-logical-xid*)))
                 (rove:ok (null (logical-recovery-assembler-buffer assembler)))
                 (rove:ok (logical-recovery-assembler-spool assembler)))
               (rove:ok (= 1 (length (uiop:directory-files directory))))
               (wal-recovery-accept-logical-change-chunk two)
               (rove:ok (null (uiop:directory-files directory)))
               (wal-recovery-finish-logical-change-batch 91))
             (with-wal-replay-staging ()
               (setf *wal-recovery-group-active* t)
               (wal-recovery-accept-logical-change-chunk one)
               (rove:ok (= 1 (length (uiop:directory-files directory)))))
             (rove:ok (null (uiop:directory-files directory)))
             (with-wal-replay-staging ()
               (setf *wal-recovery-group-active* t)
               (wal-recovery-accept-logical-change-chunk one)
               (wal-recovery-reset-logical-change-batch)
               (rove:ok (null (uiop:directory-files directory))))
             (rove:ok (handler-case
                          (with-wal-replay-staging ()
                            (setf *wal-recovery-group-active* t)
                            (wal-recovery-accept-logical-change-chunk one)
                            (wal-recovery-accept-logical-change-chunk one)
                            nil)
                        (error () t)))
             (rove:ok (null (uiop:directory-files directory)))
             (let* ((bad (copy-seq event)))
               (setf (aref bad (1- (length bad))) #xff)
               (let ((bad-two (make-wal-record
                               :type +wal-record-logical-changes+ :xid 91
                               :payload (logical-encode-fragment
                                         1 2 (make-logical-fragment :event bad :start middle :end (length bad))))))
                 (rove:ok (handler-case
                              (with-wal-replay-staging ()
                                (setf *wal-recovery-group-active* t)
                                (wal-recovery-accept-logical-change-chunk one)
                                (wal-recovery-accept-logical-change-chunk bad-two)
                                nil)
                            (error () t)))))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-slot-spooled-fragment-lifetime
    (let* ((directory (merge-pathnames
                       (format nil "pg-slot-spool-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (*logical-validation-only* t)
           (original #(1 2 "slot🙂"))
           (event (logical-encode-change (logical-test-change :insert 7 701 :new-values original)))
           (middle (floor (length event) 2))
           (one (make-wal-record :type +wal-record-logical-changes+ :xid 91
                                 :payload (logical-encode-fragment 0 2
                                                                   (make-logical-fragment :event event :start 0 :end middle))))
           (two (make-wal-record :type +wal-record-logical-changes+ :xid 91
                                 :payload (logical-encode-fragment 1 2
                                                                   (make-logical-fragment :event event :start middle :end (length event)))))
           (state (make-logical-reader-state :database-oid 7)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (wal-logical-reader-start-batch state)
             (wal-logical-reader-add-chunk state one)
             (rove:ok (= 1 (length (uiop:directory-files directory))))
             (wal-logical-reader-reset-batch state nil)
             (rove:ok (null (uiop:directory-files directory)))
             (wal-logical-reader-start-batch state)
             (wal-logical-reader-add-chunk state one)
             (wal-logical-reader-add-chunk state two)
             (rove:ok (null (uiop:directory-files directory)))
             (let ((change (first (logical-reader-state-changes state))))
               (wal-logical-reader-reset-batch state nil)
               (rove:ok (equalp (logical-change-new-values change) original))))
        (wal-logical-reader-reset-batch state nil)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-scoped-committed-transaction-spool
    (let* ((directory (merge-pathnames
                       (format nil "pg-slot-iterator-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (*query-temp-file-limit-bytes* 0)
           (*logical-validation-only* t)
           (state (make-logical-reader-state :database-oid 7))
           (first (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "first"))))
           (second (logical-encode-change
                    (logical-test-change :insert 7 701 :new-values #(3 4 "second"))))
           (chunk (make-wal-record :type +wal-record-logical-changes+ :xid 88
                                   :payload (logical-encode-chunk 0 1 (list first second))))
           (commit (make-wal-record :type +wal-record-commit+ :xid 88 :start 90 :end 100
                                    :payload (wal-encode-commit-timestamp 0)))
           (transaction nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (call-with-logical-reader-spools
              (lambda ()
                (wal-logical-reader-start-batch state)
                (wal-logical-reader-add-chunk state chunk)
                (rove:ok (null (logical-reader-state-changes state)))
                (rove:ok (null (logical-reader-state-transactions state)))
                (rove:ok (= 1 (length (uiop:directory-files directory))))
                (wal-logical-reader-finish-batch state commit)
                (setf transaction (first (logical-reader-state-transactions state)))
                (let ((values nil))
                  (logical-transaction-map-changes
                   transaction (lambda (change) (push (logical-change-new-values change) values)))
                  (rove:ok (equalp (nreverse values) (list #(1 2 "first") #(3 4 "second")))))))
             (rove:ok (null (uiop:directory-files directory)))
             (rove:ok (handler-case
                          (progn (logical-transaction-map-changes transaction #'identity) nil)
                        (error () t))))
        (wal-logical-reader-reset-batch state nil)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-scoped-spool-abort-and-invalid-terminal-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-slot-abort-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (state (make-logical-reader-state :database-oid 7))
           (event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "value"))))
           (chunk (make-wal-record :type +wal-record-logical-changes+ :xid 88
                                   :payload (logical-encode-chunk 0 1 (list event))))
           (bad-commit (make-wal-record :type +wal-record-commit+ :xid 89 :end 100)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (rove:ok
              (handler-case
                  (call-with-logical-reader-spools
                   (lambda ()
                     (wal-logical-reader-start-batch state)
                     (wal-logical-reader-add-chunk state chunk)
                     (wal-logical-reader-finish-batch state bad-commit)
                     nil))
                (error () t)))
             (rove:ok (null (logical-reader-state-transactions state)))
             (rove:ok (null (uiop:directory-files directory)))
             (wal-logical-reader-reset-batch state nil)
             (rove:ok
              (handler-case
                  (call-with-logical-reader-spools
                   (lambda ()
                     (wal-logical-reader-start-batch state)
                     (wal-logical-reader-add-chunk state chunk)
                     (error "injected callback failure")))
                (error () t)))
             (rove:ok (null (uiop:directory-files directory))))
        (wal-logical-reader-reset-batch state nil)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-standard-scoped-spool-preflight-and-interruption
    (let* ((directory (merge-pathnames
                       (format nil "pg-slot-send-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (state (make-logical-reader-state :database-oid 7))
           (stream (make-standard-stream :emit-floor 0 :last-send-time (get-internal-real-time)))
           (original (symbol-function 'standard-xlog-data))
           (change (logical-test-change :insert 7 701 :new-values #(1 2 "value")))
           (frames nil) (fail-relation nil))
      (setf (logical-change-publication-selection change)
            '((:relation-oid 701 :publications ((:name "p" :oid 100 :generation 1 :unsupported nil)))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'standard-xlog-data)
                   (lambda (ignored payload &optional (end 0))
                     (declare (ignore ignored))
                     (push (list (code-char (aref payload 0)) end) frames)
                     (not (and fail-relation (= (aref payload 0) (char-code #\R))))))
             (call-with-logical-reader-spools
              (lambda ()
                (logical-reader-store-change state change)
                (let* ((spool (logical-reader-state-spool state))
                       (transaction (make-logical-transaction :xid 88 :commit-record-lsn 90
                                                              :commit-lsn 100 :commit-timestamp 0
                                                              :spool spool)))
                  (logical-spool-seal spool)
                  (rove:ok (standard-send-scoped-transaction stream transaction '("other")))
                  (rove:ok (null frames))
                  (setf fail-relation t)
                  (rove:ok (null (standard-send-scoped-transaction stream transaction '("p"))))
                  (rove:ok (equal (nreverse frames) (list (list #\B 0) (list #\R 0))))
                  (rove:ok (zerop (standard-stream-pending-count stream))))))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'standard-xlog-data) original)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-standard-scoped-preflight-checks-later-events
    (let* ((state (make-standard-stream :emit-floor 0))
           (original (symbol-function 'standard-xlog-data))
           (first (logical-test-change :insert 7 701 :new-values #(1 2 "first")))
           (second (logical-test-change :insert 7 701 :new-values #(3 4 "second")))
           (frames nil))
      (setf (logical-change-publication-selection first)
            '((:relation-oid 701 :publications ((:name "p" :oid 100 :generation 1 :unsupported nil)))))
      (unwind-protect
           (progn
             (setf (symbol-function 'standard-xlog-data)
                   (lambda (ignored payload &optional (end 0))
                     (declare (ignore ignored payload end))
                     (setf frames t) t))
             (call-with-logical-reader-spools
              (lambda ()
                (let ((reader (make-logical-reader-state :database-oid 7)))
                  (logical-reader-store-change reader first)
                  (logical-reader-store-change reader second)
                  (let ((spool (logical-reader-state-spool reader)))
                    (logical-spool-seal spool)
                    (rove:ok (handler-case
                                 (progn
                                   (standard-send-scoped-transaction
                                    state (make-logical-transaction :xid 88 :spool spool
                                                                    :commit-record-lsn 90
                                                                    :commit-lsn 100 :commit-timestamp 0)
                                    '("p"))
                                   nil)
                               (pg-error (condition)
                                 (string= (pg-error-sqlstate condition) "0A000"))))
                    (rove:ok (null frames)))))))
        (setf (symbol-function 'standard-xlog-data) original))))

(rove:deftest test-standard-streaming-preflights-later-row-before-begin
    (let* ((state (make-standard-stream :emit-floor 0))
           (original (symbol-function 'standard-xlog-data))
           (first (logical-test-change :insert 7 701 :new-values #(1 2 "first")))
           (second (logical-test-change :insert 7 701 :new-values #(3 4 "second")))
           (selection '((:relation-oid 701 :publications ((:name "p" :oid 100 :generation 1 :unsupported nil)))))
           (sent nil))
      (setf (logical-change-publication-selection first) selection
            (logical-change-publication-selection second) selection
            (logical-change-new-values second) #(3))
      (unwind-protect
           (progn
             (setf (symbol-function 'standard-xlog-data)
                   (lambda (&rest args) (declare (ignore args)) (setf sent t) t))
             (rove:ok
              (handler-case
                  (progn
                    (standard-send-scoped-transaction
                     state (make-logical-transaction :xid 88 :commit-record-lsn 90
                                                     :commit-lsn 100 :commit-timestamp 0
                                                     :changes (list first second)) '("p"))
                    nil)
                (error (condition) (search "logical row width" (princ-to-string condition)))))
             (rove:ok (null sent)))
        (setf (symbol-function 'standard-xlog-data) original))))

(rove:deftest test-standard-scoped-corrupt-later-event-before-begin
    (let* ((directory (merge-pathnames
                       (format nil "pg-slot-corrupt-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (state (make-standard-stream :emit-floor 0))
           (original (symbol-function 'standard-xlog-data))
           (change (logical-test-change :insert 7 701 :new-values #(1 2 "value")))
           (frames nil))
      (setf (logical-change-publication-selection change)
            '((:relation-oid 701 :publications ((:name "p" :oid 100 :generation 1 :unsupported nil)))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'standard-xlog-data)
                   (lambda (ignored payload &optional (end 0))
                     (declare (ignore ignored payload end))
                     (setf frames t) t))
             (call-with-logical-reader-spools
              (lambda ()
                (let ((reader (make-logical-reader-state :database-oid 7)))
                  (logical-reader-store-change reader change)
                  (logical-reader-store-change reader change)
                  (let* ((spool (logical-reader-state-spool reader))
                         (file (logical-spool-stream spool))
                         (second-start (+ 16 (length (logical-encode-change change)))))
                    (logical-spool-seal spool)
                    ;; Change a text byte, leaving the encoded event structurally valid.
                    (file-position file (+ second-start (1- (length (logical-encode-change change)))))
                    (write-byte 99 file)
                    (finish-output file)
                    (rove:ok (handler-case
                                 (progn
                                   (standard-send-scoped-transaction
                                    state (make-logical-transaction :xid 88 :spool spool
                                                                    :commit-record-lsn 90
                                                                    :commit-lsn 100 :commit-timestamp 0)
                                    '("p"))
                                   nil)
                               (error (condition)
                                 (search "checksum mismatch" (princ-to-string condition)))))
                    (rove:ok (null frames))))))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'standard-xlog-data) original)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-slot-scan-spool-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-slot-scan-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (*wal-replication-slots* (make-hash-table :test #'equal))
           (*wal-logical-slot-owner* (list :owner))
           (*wal-logical-slot-read-progress* nil)
           (*wal-poisoned* nil)
           (*wal-durable-lsn* 100)
           (slot (make-wal-replication-slot :name "scan" :database-oid 7
                                            :restart-lsn 0 :consistent-lsn 0
                                            :confirmed-flush-lsn 0
                                            :active-owner *wal-logical-slot-owner*))
           (event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "scan"))))
           (middle (floor (length event) 2))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #()))
           (fragment (make-wal-record :type +wal-record-logical-changes+ :xid 91
                                      :payload (logical-encode-fragment
                                                0 2 (make-logical-fragment
                                                     :event event :start 0 :end middle))))
           (original-scan (symbol-function 'wal-scan))
           (fail-scan nil)
           (saw-spool nil))
      (setf (gethash "scan" *wal-replication-slots*) slot)
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-scan)
                   (lambda (function &rest options)
                     (declare (ignore options))
                     (funcall function start)
                     (funcall function fragment)
                     (setf saw-spool (= 1 (length (uiop:directory-files directory))))
                     (when fail-scan (error "injected scan failure"))))
             (multiple-value-bind (transactions progress)
                 (wal-logical-slot-read-transactions "scan")
               (rove:ok saw-spool)
               (rove:ok (null transactions))
               (rove:ok (= progress 0))
               (rove:ok (null (uiop:directory-files directory))))
             (setf saw-spool nil fail-scan t)
             (rove:ok (handler-case (progn (wal-logical-slot-read-transactions "scan") nil)
                        (error (condition)
                          (search "injected scan failure" (princ-to-string condition)))))
             (rove:ok saw-spool)
             (rove:ok (null (uiop:directory-files directory)))
             (rove:ok (= 0 (wal-replication-slot-confirmed-flush-lsn slot))))
        (setf (symbol-function 'wal-scan) original-scan)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-logical-fragment-assembly
    (let* ((first (make-array 10 :element-type '(unsigned-byte 8)
                                 :initial-contents '(0 1 2 3 4 5 6 7 8 9)))
           (second (make-array 2 :element-type '(unsigned-byte 8)
                                 :initial-contents '(10 11)))
           (plan (logical-plan-event-fragments (list first second) 4 12))
           (state (make-logical-fragment-assembler 12))
           (complete '()))
      (rove:ok (= (length plan) 4))
      (dolist (fragment plan)
        (let* ((event (logical-fragment-event fragment))
               (start (logical-fragment-start fragment))
               (end (logical-fragment-end fragment))
               (result (logical-assemble-event-fragment state (length event) start
                                                        (subseq event start end))))
          (when result (push result complete))))
      (logical-finish-fragment-assembly state)
      (rove:ok (equalp (nreverse complete) (list first second)))
      (rove:ok (= (logical-fragment-assembler-bytes state) 12))
      (rove:ok (handler-case (progn (logical-plan-event-fragments (list first second) 4 11) nil)
                 (error () t))))
  (flet ((bad (total offset octets)
           (handler-case
               (progn (logical-assemble-event-fragment
                       (make-logical-fragment-assembler 10) total offset
                       (coerce octets '(vector (unsigned-byte 8))))
                      nil)
             (error () t))))
    (rove:ok (bad 4 1 #(1)))
    (rove:ok (bad 11 0 #(1)))
    (rove:ok (bad 2 0 #()))
    (rove:ok (bad 2 1 #(1 2))))
  (let ((state (make-logical-fragment-assembler 10)))
    (logical-assemble-event-fragment state 4 0 (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(1 2)))
    (rove:ok (handler-case (progn (logical-assemble-event-fragment state 4 3 (make-array 1 :element-type '(unsigned-byte 8) :initial-element 3)) nil) (error () t)))
    (rove:ok (handler-case (progn (logical-assemble-event-fragment state 5 2 (make-array 1 :element-type '(unsigned-byte 8) :initial-element 3)) nil) (error () t)))
    (rove:ok (handler-case (progn (logical-finish-fragment-assembly state) nil) (error () t)))))

(rove:deftest test-logical-fragment-limits-and-failure-state
    (let* ((bytes (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(7 8)))
           (state (make-logical-fragment-assembler 3)))
      (rove:ok (equalp (logical-assemble-event-fragment state 2 0 bytes) bytes))
      (rove:ok (= (logical-fragment-assembler-bytes state) 2))
      (rove:ok (handler-case
                   (progn (logical-assemble-event-fragment state 2 0 bytes) nil)
                 (error () t)))
      (rove:ok (= (logical-fragment-assembler-bytes state) 2))
      (rove:ok (null (logical-fragment-assembler-buffer state)))
      (rove:ok (handler-case
                   (progn (logical-assemble-event-fragment state 1 0 #(999)) nil)
                 (error () t)))
      (rove:ok (= (logical-fragment-assembler-bytes state) 2)))
  (dolist (limits '((0 10) (-1 10) (1 0) (1 -1)))
    (rove:ok (handler-case
                 (progn (logical-plan-event-fragments '() (first limits) (second limits)) nil)
               (error () t)))))

(rove:deftest test-logical-fragment-payload-roundtrip
    (let* ((event (make-array 9 :element-type '(unsigned-byte 8)
                                :initial-contents '(0 1 2 3 4 5 6 7 8)))
           (plan (logical-plan-event-fragments (list event) 4 9))
           (state (make-logical-fragment-assembler 9))
           (completed nil))
      (loop for fragment in plan for index from 0
            for payload = (logical-encode-fragment index (length plan) fragment)
            do (multiple-value-bind (actual-index count total offset bytes)
                   (logical-decode-fragment payload)
                 (rove:ok (= actual-index index))
                 (rove:ok (= count (length plan)))
                 (setf completed (or (logical-assemble-event-fragment state total offset bytes)
                                     completed))))
      (logical-finish-fragment-assembly state)
      (rove:ok (equalp event completed))
      (let* ((payload (logical-encode-fragment 0 3 (first plan)))
             (trailing (concatenate '(vector (unsigned-byte 8)) payload #(0))))
        (rove:ok (handler-case (progn (logical-decode-fragment trailing) nil)
                   (error () t)))
        (setf (aref payload 0) 0)
        (rove:ok (handler-case (progn (logical-decode-fragment payload) nil)
                   (error () t))))))

(rove:deftest test-logical-fragment-rejects-truncated-payloads
    (let* ((event (make-array 5 :element-type '(unsigned-byte 8) :initial-element 42))
           (fragment (first (logical-plan-event-fragments (list event) 4 5)))
           (payload (logical-encode-fragment 0 2 fragment)))
      (loop for length below (length payload)
            do (rove:ok (handler-case
                            (progn (logical-decode-fragment (subseq payload 0 length)) nil)
                          (error () t)))))
  (let* ((event (make-array 5 :element-type '(unsigned-byte 8) :initial-element 42))
         (fragment (first (logical-plan-event-fragments (list event) 4 5))))
    (dolist (field-offset '(8 12 20))
      (let ((payload (logical-encode-fragment 0 2 fragment)))
        (fill payload 0 :start field-offset :end (+ field-offset 4))
        (rove:ok (handler-case (progn (logical-decode-fragment payload) nil)
                   (error () t)))))))

(rove:deftest test-logical-fragment-batch-validation
    (let* ((event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "fragment"))))
           (plan (logical-plan-event-fragments (list event) 9 10000))
           (payloads (loop for fragment in plan for index from 0
                           collect (logical-encode-fragment index (length plan) fragment)))
           (batch (make-logical-fragment-batch 10000)))
      (rove:ok (handler-case
                   (progn (logical-fragment-batch-accept batch 5 (second payloads)) nil)
                 (error () t)))
      (logical-fragment-batch-accept batch 5 (first payloads))
      (rove:ok (handler-case (progn (logical-fragment-batch-finish batch 5) nil)
                 (error () t)))
      (rove:ok (handler-case
                   (progn (logical-fragment-batch-accept batch 6 (second payloads)) nil)
                 (error () t)))
      (let ((change nil))
        (dolist (payload (rest payloads))
          (let ((result (logical-fragment-batch-accept batch 5 payload)))
            (when result (setf change result))))
        (rove:ok (eq (logical-change-kind change) :insert))
        (rove:ok (= (logical-change-relation-oid change) 701)))
      (rove:ok (handler-case (progn (logical-fragment-batch-finish batch 6) nil)
                 (error () t)))
      (logical-fragment-batch-finish batch 5)))

(rove:deftest test-logical-fragments-recovery-and-slot-reader
    (let* ((event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "fragment"))))
           (plan (logical-plan-event-fragments (list event) 9 10000))
           (records (loop for fragment in plan for index from 0
                          collect (make-wal-record :type +wal-record-logical-changes+ :xid 88
                                                   :payload (logical-encode-fragment index (length plan) fragment))))
           (state (make-logical-reader-state :database-oid 7))
           (*wal-recovery-group-active* t)
           (*wal-recovery-logical-xid* nil)
           (*wal-recovery-logical-chunk-count* nil)
           (*wal-recovery-logical-next-chunk* 0)
           (*wal-recovery-logical-bytes* 0))
      (wal-logical-reader-start-batch state)
      (wal-recovery-accept-logical-change-chunk (first records))
      (wal-logical-reader-add-chunk state (first records))
      (rove:ok (handler-case (progn (wal-recovery-finish-logical-change-batch 88) nil)
                 (error () t)))
      (dolist (record (rest records))
        (wal-recovery-accept-logical-change-chunk record)
        (wal-logical-reader-add-chunk state record))
      (wal-recovery-finish-logical-change-batch 88)
      (rove:ok (= (length (logical-reader-state-changes state)) 1))
      (rove:ok (= (logical-reader-state-change-bytes state) (length event)))
      (logical-fragment-batch-finish (logical-reader-state-fragment-batch state) 88)
      (wal-logical-reader-reset-batch state nil)
      (rove:ok (null (logical-reader-state-fragment-batch state)))))

(rove:deftest test-logical-change-chunk-validation
    (let* ((event
             (logical-encode-change
              (logical-test-change :insert 7 701 :new-values #(1 2 "chunk"))))
           (change-bytes (length event))
           (first-chunk
             (make-wal-record :type +wal-record-logical-changes+ :xid 88
                              :payload (logical-encode-chunk 0 2 (list event)))))
      (labels ((rejects (record xid count index bytes)
                 (handler-case
                     (progn
                       (wal-advance-logical-change-chunk
                        record xid count index bytes)
                       nil)
                   (error () t))))
        (multiple-value-bind (xid count next-index bytes changes)
            (wal-advance-logical-change-chunk first-chunk nil nil 0 0)
          (rove:ok (and (= xid 88) (= count 2) (= next-index 1)
                        (= bytes change-bytes) (= (length changes) 1))
                   "logical chunk progress records the first complete chunk"))
        (rove:ok (rejects (logical-test-chunk-record 88 1 2) nil nil 0 0)
                 "logical chunks must start at index zero")
        (rove:ok (rejects (logical-test-chunk-record 0 0 2) nil nil 0 0)
                 "logical chunks require a positive transaction ID")
        (rove:ok (rejects (logical-test-chunk-record 89 1 2) 88 2 1 change-bytes)
                 "logical chunks cannot change transaction ID")
        (rove:ok (rejects (logical-test-chunk-record 88 0 2) 88 2 1 change-bytes)
                 "logical chunks must be sequential")
        (rove:ok (rejects (logical-test-chunk-record 88 1 3) 88 2 1 change-bytes)
                 "logical chunk count cannot change within a transaction")
        (rove:ok (rejects (logical-test-chunk-record 88 1 2) 88 2 1
                          +max-logical-wal-transaction-bytes+)
                 "logical chunk progress enforces the 65 MiB transaction cap")
        (let ((state (make-logical-reader-state :database-oid 7 :batch-open-p t
                                                :next-chunk 0))
              (commit (make-wal-record :type +wal-record-commit+ :xid 88 :end 100)))
          (wal-logical-reader-add-chunk state first-chunk)
          (rove:ok (handler-case
                       (progn (wal-logical-reader-finish-batch state commit) nil)
                     (error () t))
                   "logical commit terminals require every declared chunk")))))

(rove:deftest test-logical-copy-chunk-limit-override
    (rove:ok (= (logical-copy-chunk-limit nil nil nil)
                +logical-copy-chunk-bytes+)
             "initial-copy chunks use the production limit by default")
  (rove:ok (= (logical-copy-chunk-limit
               "300" "after-copy-chunk-commit" "crash")
              300)
           "copy-chunk crash tests can use a smaller chunk limit")
  (dolist (arguments '(("0" "after-copy-chunk-commit" "crash")
                       ("not-a-number" "after-copy-chunk-commit" "crash")
                       ("300" "after-target-commit-before-feedback" "crash")
                       ("300" "after-copy-chunk-commit" "error")))
    (rove:ok (handler-case
                 (progn (apply #'logical-copy-chunk-limit arguments) nil)
               (error () t))
             "initial-copy chunk override rejects invalid value or failpoint"))
  (rove:ok (handler-case
               (progn
                 (logical-copy-chunk-limit
                  (write-to-string (1+ +logical-copy-chunk-bytes+))
                  "after-copy-chunk-commit" "crash")
                 nil)
             (error () t))
           "initial-copy chunk override cannot exceed production limit"))

(rove:deftest test-logical-replication-bootstrap-definitions
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (db (make-database :name "postgres" :oid 7))
           (table
             (make-pg-class
              :oid 301 :name "bootstrap_rows" :relfilenode 1301
              :columns
              (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                      :attnum 1 :notnull t)
                      (make-pg-column :name "amount" :type :int8 :typmod -1
                                      :attnum 2 :notnull nil
                                      :default-sequence-name "bootstrap_seq"))
              :indexes '(401 402)))
           (primary
             (make-pg-index :oid 401 :name "bootstrap_rows_pkey"
                            :relfilenode 1401 :table-oid 301 :columns '(1)
                            :unique-p t :primary-p t :constraint-p t :valid-p t))
           (standalone
             (make-pg-index :oid 402 :name "bootstrap_rows_amount_idx"
                            :relfilenode 1402 :table-oid 301 :columns '(2)
                            :unique-p t :nulls-not-distinct-p t :valid-p t))
           (sequence
             (make-pg-sequence :oid 501 :name "bootstrap_seq" :increment -2
                               :start-value -1 :min-value +int8-min+
                               :max-value -1 :last-value -9 :called-p t)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) db
            (gethash "bootstrap_rows" (database-tables db)) table
            (gethash "bootstrap_rows_pkey" (database-indexes db)) primary
            (gethash "bootstrap_rows_amount_idx" (database-indexes db)) standalone
            (gethash "bootstrap_seq" (database-sequences db)) sequence)
      (let* ((bootstrap (catalog-replication-bootstrap-data db))
             (sequence-snapshot
               (make-snapshot-struct
                :sequence-database-oid 7 :sequence-state-captured-p t
                :sequence-states (getf bootstrap :sequences)))
             (table-sql (getf (first (getf bootstrap :tables)) :sql))
             (index-entry (first (getf bootstrap :indexes)))
             (sequence-entry (first (getf bootstrap :sequences))))
        (rove:ok (and (search "CREATE TABLE IF NOT EXISTS" table-sql)
                      (search "DEFAULT nextval('bootstrap_seq')" table-sql)
                      (search "CONSTRAINT \"bootstrap_rows_pkey\" PRIMARY KEY" table-sql)
                      (not (search "bootstrap_rows_amount_idx" table-sql)))
                 "bootstrap table definition retains defaults and only constraint indexes")
        (rove:ok (and (string= (getf index-entry :name) "bootstrap_rows_amount_idx")
                      (string= (getf index-entry :table-name) "bootstrap_rows")
                      (search "CREATE UNIQUE INDEX IF NOT EXISTS" (getf index-entry :sql))
                      (search "NULLS NOT DISTINCT" (getf index-entry :sql)))
                 "bootstrap emits standalone btree indexes as separate SQL")
        (rove:ok (and (search (format nil "INCREMENT BY -2 MINVALUE ~d MAXVALUE -1 START WITH -1"
                                      +int8-min+)
                              (getf sequence-entry :definition))
                      (= (getf (getf sequence-entry :state) :last) -9)
                      (getf (getf sequence-entry :state) :called))
                 "bootstrap preserves sequence increment, bounds, start, and state")
        ;; The exported snapshot keeps its sequence state from the slot boundary,
        ;; even though PostgreSQL sequences are not MVCC-visible.
        (setf (pg-sequence-last-value sequence) -11)
        (let* ((captured-bootstrap
                 (catalog-replication-bootstrap-data db sequence-snapshot))
               (captured-sequence
                 (first (getf captured-bootstrap :sequences))))
          (rove:ok (= (getf (getf captured-sequence :state) :last) -9)
                   "bootstrap uses the sequence state captured with the slot boundary")))))

(rove:deftest test-logical-bootstrap-constraint-order-stable
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 7))
           (table
             (make-pg-class
              :oid 801 :name "orders" :relfilenode 1801
              :columns
              (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                      :attnum 1 :notnull t)
                      (make-pg-column :name "account_id" :type :int4 :typmod -1
                                      :attnum 2 :notnull t))
              :indexes '(901 902)))
           (primary
             (make-pg-index :oid 901 :name "orders_pkey"
                            :relfilenode 1901 :table-oid 801 :columns '(1)
                            :unique-p t :primary-p t :constraint-p t :valid-p t))
           (unique
             (make-pg-index :oid 902 :name "orders_account_key"
                            :relfilenode 1902 :table-oid 801 :columns '(2)
                            :unique-p t :constraint-p t :valid-p t)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "orders" (database-tables database)) table
            (gethash "orders_pkey" (database-indexes database)) primary
            (gethash "orders_account_key" (database-indexes database)) unique)
      (let* ((primary-first (catalog-table-definition-sql table))
             (account-position
               (search "CONSTRAINT \"orders_account_key\" UNIQUE" primary-first))
             (primary-position
               (search "CONSTRAINT \"orders_pkey\" PRIMARY KEY" primary-first)))
        (setf (pg-class-indexes table) '(902 901))
        (rove:ok (string= primary-first (catalog-table-definition-sql table))
                 "schema contracts serialize multiple constraints independently of catalog order")
        (rove:ok (and account-position primary-position
                      (< account-position primary-position))
                 "table constraints serialize in stable name order"))))

(rove:deftest test-logical-schema-contract-strictness
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (db (make-database :name "postgres" :oid 7))
           (table
             (make-pg-class
              :oid 701 :name "contract_rows" :relfilenode 1701
              :columns
              (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                      :attnum 1 :notnull t))))
           (sequence
             (make-pg-sequence :oid 702 :name "contract_seq" :start-value 1
                               :increment 1 :min-value 1 :max-value +int8-max+
                               :last-value 4 :called-p t)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) db
            (gethash "contract_rows" (database-tables db)) table
            (gethash "contract_seq" (database-sequences db)) sequence)
      (let* ((before (logical-current-schema-contract))
             (empty '(:tables () :indexes () :sequences ()))
             (event-bytes
               (let ((*logical-source-schema-precondition* before))
                 (logical-catalog-event-for-ast
                  '(:type :alter-table :name "contract_rows" :actions ()))))
             (event (logical-decode-catalog-event event-bytes))
             (envelope
               (logical-validate-ddl-event-envelope
                (logical-read-data-string
                 (logical-change-event-payload event)))))
        (rove:ok (logical-schema-contract-valid-p before)
                 "supported catalog objects produce a valid schema contract")
        (rove:ok (logical-schema-contract-subset-compatible-p before empty)
                 "an empty subscriber schema can be bootstrapped")
        (rove:ok (and (equal (getf envelope :schema-before) before)
                      (equal (getf envelope :schema-after) before)
                      (eql (getf envelope :schema-contract-version) 1))
                 "DDL events carry versioned before and after schema contracts")
        (logical-validate-schema-contract before :before)
        ;; Sequence values are not schema, as nextval() is non-MVCC state.
        (setf (pg-sequence-last-value sequence) 99)
        (rove:ok (equal before (logical-current-schema-contract))
                 "sequence advancement does not invalidate schema compatibility")
        (let ((target-with-extra
                (list :tables (append (copy-tree (getf before :tables))
                                      '((:name "local_only" :sql "CREATE TABLE local_only ()")))
                      :indexes (copy-tree (getf before :indexes))
                      :sequences (copy-tree (getf before :sequences)))))
          (rove:ok (not (logical-schema-contract-subset-compatible-p
                         before target-with-extra))
                   "an extra target table is treated as schema drift"))
        (rove:ok (not (logical-schema-contract-valid-p
                       (append (copy-tree before) '(:unexpected t))))
                 "schema contracts reject unknown or duplicate top-level keys")
        (let ((duplicate-objects (copy-tree before)))
          (setf (getf duplicate-objects :tables)
                (append (copy-tree (getf duplicate-objects :tables))
                        (copy-tree (getf duplicate-objects :tables))))
          (rove:ok (not (logical-schema-contract-valid-p duplicate-objects))
                   "schema contracts reject duplicate object names"))
        (let ((sequence-drift (copy-tree before)))
          (setf (getf (first (getf sequence-drift :sequences)) :definition)
                "CREATE SEQUENCE contract_seq INCREMENT BY 2")
          (rove:ok (not (logical-schema-contract-subset-compatible-p
                         before sequence-drift))
                   "sequence definition drift is rejected"))
        (let ((source-with-index (copy-tree before))
              (target-with-index-drift (copy-tree before)))
          (setf (getf source-with-index :indexes)
                '((:name "contract_idx" :sql "CREATE INDEX contract_idx"))
                (getf target-with-index-drift :indexes)
                '((:name "contract_idx" :sql "CREATE UNIQUE INDEX contract_idx")))
          (rove:ok (not (logical-schema-contract-subset-compatible-p
                         source-with-index target-with-index-drift))
                   "index definition drift is rejected"))
        (let ((malformed-envelope (copy-tree envelope)))
          (setf (getf malformed-envelope :ast)
                '(:type :drop-table :names ("one")
                  :type :drop-sequence :names ("two")))
          (rove:ok (handler-case
                       (progn
                         (logical-validate-ddl-event-envelope malformed-envelope)
                         nil)
                     (error () t))
                   "DDL envelopes reject malformed AST plists"))
        (setf (pg-column-notnull (aref (pg-class-columns table) 0)) nil)
        (rove:ok (not (equal before (logical-current-schema-contract)))
                 "column nullability changes are part of the schema contract")
        (rove:ok (handler-case
                     (progn (logical-validate-schema-contract before :before) nil)
                   (error () t))
                 "strict schema validation rejects target drift"))))

(rove:deftest test-logical-schema-contract-column-shape-drift
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 7))
           (columns
             (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                     :attnum 1 :notnull t)
                     (make-pg-column :name "label" :type :varchar :typmod 20
                                     :attnum 2 :notnull nil)
                     (make-pg-column :name "amount" :type :int8 :typmod -1
                                     :attnum 3 :notnull nil)))
           (table (make-pg-class :oid 703 :name "contract_rows"
                                 :relfilenode 1703 :columns columns)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "contract_rows" (database-tables database)) table)
      (let ((source-contract (logical-current-schema-contract)))
        (setf (pg-column-type (aref columns 1)) :int4)
        (rove:ok (not (logical-schema-contract-subset-compatible-p
                       source-contract (logical-current-schema-contract)))
                 "schema contract rejects column type drift")
        (setf (pg-column-type (aref columns 1)) :varchar
              (pg-column-typmod (aref columns 1)) 36)
        (rove:ok (not (logical-schema-contract-subset-compatible-p
                       source-contract (logical-current-schema-contract)))
                 "schema contract rejects varchar typmod drift")
        (setf (pg-column-typmod (aref columns 1)) 20
              (pg-class-columns table)
              (vector (aref columns 1) (aref columns 0) (aref columns 2)))
        (rove:ok (not (logical-schema-contract-subset-compatible-p
                       source-contract (logical-current-schema-contract)))
                 "schema contract rejects column order drift"))))

(rove:deftest test-logical-capture-application-headroom
    (let* ((catalog (make-catalog))
           (database (make-database :name "postgres" :oid 7))
           (table
             (make-pg-class
              :oid 702 :name "logical_rows"
              :columns (vector (make-pg-column :name "id" :type :int4
                                               :typmod -1 :attnum 1 :notnull t)
                               (make-pg-column :name "label" :type :varchar
                                               :typmod -1 :attnum 2 :notnull nil)))))
      (setf (gethash "postgres" (catalog-databases catalog)) database
            (gethash "logical_rows" (database-tables database)) table)
      (multiple-value-bind (sample-accepted event-bytes)
          (logical-test-capture-row-change catalog table "postgres" 0)
        (rove:ok sample-accepted
                 "a small source change fits the ordinary transaction budget")
        (labels ((check-boundary (application limit label)
                   (dolist (delta '(-1 0 1))
                     (let* ((expected (+ limit delta))
                            (initial (- expected event-bytes)))
                       (multiple-value-bind (accepted total)
                           (logical-test-capture-row-change
                            catalog table application initial)
                         (rove:ok (= (if accepted total (+ initial event-bytes)) expected)
                                  (format nil "~a accounts for encoded event bytes" label))
                         (rove:ok (eq accepted (<= expected limit))
                                  (format nil "~a accepts through its inclusive cap" label)))))))
          (check-boundary "postgres" +max-logical-transaction-bytes+
                          "ordinary source transaction")
          (check-boundary "pg-lisp-subscribe-apply"
                          +max-logical-wal-transaction-bytes+
                          "subscriber apply transaction")))))

(rove:deftest test-logical-truncate-capture-headroom
    (let* ((catalog (make-catalog))
           (database (make-database :name "postgres" :oid 7)))
      (setf (gethash "postgres" (catalog-databases catalog)) database)
      (multiple-value-bind (sample-accepted event-bytes)
          (logical-test-capture-truncate-event catalog 0)
        (rove:ok sample-accepted
                 "a small TRUNCATE utility event fits the source transaction budget")
        (dolist (delta '(-1 0 1))
          (let* ((expected (+ +max-logical-transaction-bytes+ delta))
                 (initial (- expected event-bytes)))
            (multiple-value-bind (accepted total)
                (logical-test-capture-truncate-event catalog initial)
              (rove:ok (= (if accepted total (+ initial event-bytes)) expected)
                       "TRUNCATE capture accounts for encoded utility-event bytes")
              (rove:ok (eq accepted
                           (<= expected +max-logical-transaction-bytes+))
                       "TRUNCATE accepts through the inclusive 64 MiB cap")))))))

(rove:deftest test-logical-catalog-terminal-validation
    (let ((catalog-event (make-logical-change :kind :ddl :database-oid 7))
          (row-change (logical-test-change :insert 7 701 :new-values #(1 2 "a")))
          (commit (make-wal-record :type +wal-record-commit+ :xid 9 :end 100))
          (catalog-terminal
            (make-wal-record :type +wal-record-catalog+ :xid 0 :end 100)))
      (rove:ok (handler-case
                   (progn
                     (wal-logical-reader-finish-batch
                      (make-logical-reader-state :database-oid 7 :batch-open-p t
                                                 :changes (list catalog-event))
                      commit)
                     nil)
                 (error () t))
               "catalog events cannot become visible at a row commit terminal")
      (rove:ok (handler-case
                   (progn
                     (wal-logical-reader-finish-batch
                      (make-logical-reader-state :database-oid 7 :batch-open-p t
                                                 :changes (list row-change))
                      catalog-terminal)
                     nil)
                 (error () t))
               "row changes cannot become visible at a catalog terminal")
      (rove:ok (handler-case
                   (progn
                     (let ((*wal-recovery-logical-catalog-event-p* t)
                           (*wal-recovery-logical-chunk-count* nil))
                       (wal-recovery-validate-logical-terminal :commit 9))
                     nil)
                 (error () t))
               "WAL recovery rejects a catalog event followed by a row commit")
      (rove:ok (handler-case
                   (progn
                     (let ((*wal-recovery-logical-catalog-event-p* nil)
                           (*wal-recovery-logical-chunk-count* 1))
                       (wal-recovery-validate-logical-terminal :catalog))
                     nil)
                 (error () t))
               "WAL recovery rejects row changes followed by a catalog terminal")))

(rove:deftest test-wal-logical-catalog-event-slot-gating
    (let* ((*wal-replication-slots* (make-hash-table :test #'equal))
           (*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
           (slot (make-wal-replication-slot :name "catalog_event_slot"
                                            :database-oid 7)))
      (sb-thread:with-mutex (*wal-lock*)
        (rove:ok (not (wal-logical-catalog-event-needed-under-lock-p 7))
                 "catalog state before logical slot creation needs no stream event")
        (setf (gethash "catalog_event_slot" *wal-replication-slots*) slot)
        (rove:ok (wal-logical-catalog-event-needed-under-lock-p 7)
                 "catalog changes are recorded for a live database slot")
        (rove:ok (not (wal-logical-catalog-event-needed-under-lock-p 8))
                 "catalog events are scoped to the slot database")
        (setf (wal-replication-slot-wal-status slot) :lost)
        (rove:ok (not (wal-logical-catalog-event-needed-under-lock-p 7))
                 "a lost slot no longer requires catalog stream events"))))

(rove:deftest test-logical-wal-recovery-byte-boundaries
    (let* ((record (logical-test-chunk-record 9902 1 2))
           (change-bytes
             (nth-value 3
                        (wal-decode-logical-change-chunk
                         (wal-record-payload record))))
           (limit +max-logical-wal-transaction-bytes+)
           (expected-xid 9902)
           (expected-count 2)
           (expected-index 1))
      (labels ((accepted-total-p (prior-bytes expected-total)
                 (multiple-value-bind (xid count next-index total)
                     (wal-advance-logical-change-chunk
                      record expected-xid expected-count expected-index prior-bytes)
                   (and (= xid expected-xid)
                        (= count expected-count)
                        (= next-index (1+ expected-index))
                        (= total expected-total))))
               (rejected-p (prior-bytes)
                 (handler-case
                     (progn
                       (wal-advance-logical-change-chunk
                        record expected-xid expected-count expected-index prior-bytes)
                       nil)
                   (error () t))))
        (rove:ok (accepted-total-p (- limit change-bytes 1) (1- limit))
                 "WAL recovery accepts 65 MiB minus one byte")
        (rove:ok (accepted-total-p (- limit change-bytes) limit)
                 "WAL recovery accepts exactly 65 MiB")
        (rove:ok (rejected-p (1+ (- limit change-bytes)))
                 "WAL recovery rejects one byte above 65 MiB"))))

(rove:deftest test-unpinned-snapshot-release-preserves-wal-pin-map
    ;; Snapshot release follows snapmgr.c:AtEOXact_Snapshot for the MVCC
    ;; registration; only a published WAL pin may be removed under *wal-lock*.
    ;; This impossible registration/map mismatch is an adversarial sentinel that
    ;; catches accidental pin-map mutation in the unpinned path.
    (let* ((*active-snapshots* 0)
           (*active-snapshot-set* (make-hash-table :test #'eq))
           (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (registration (make-snapshot-registration))
           (snapshot (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
           (sentinel :test-only-unpublished-pin))
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*xid-lock*)
               (register-active-snapshot snapshot registration))
             (sb-thread:with-mutex (*wal-lock*)
               (setf (gethash registration *wal-snapshot-retention-pins*) sentinel))
             (rove:ok (null (snapshot-registration-wal-retention-lsn registration))
                      "test pin-map sentinel has no published registration LSN")
             (release-snapshot-registration registration)
             (rove:ok (and (zerop *active-snapshots*)
                           (not (snapshot-registration-releasing-p registration)))
                      "unpinned release retires the active snapshot registration")
             (rove:ok (eq sentinel
                          (sb-thread:with-mutex (*wal-lock*)
                            (gethash registration *wal-snapshot-retention-pins*)))
                      "unpinned release does not mutate WAL pin state without *wal-lock*"))
        (release-snapshot-registration registration)
        (sb-thread:with-mutex (*wal-lock*)
          (remhash registration *wal-snapshot-retention-pins*)))))
