;;;; logical.lisp -- transactional row-change records and slot reads.

(in-package #:pg)

(defconstant +max-logical-transaction-bytes+ (* 64 1024 1024))
(defconstant +max-logical-apply-headroom-bytes+ (* 1024 1024))
(defconstant +max-logical-wal-transaction-bytes+
  (+ +max-logical-transaction-bytes+ +max-logical-apply-headroom-bytes+))

;; These bounds apply to one encoded event; transaction spools are quota controlled.
(defconstant +max-logical-event-bytes+ (* 128 1024 1024))

(defconstant +max-logical-wal-event-bytes+ (+ +max-logical-event-bytes+ +max-logical-apply-headroom-bytes+))

(defconstant +max-unscoped-logical-materialization-bytes+ +max-logical-wal-transaction-bytes+)

(defconstant +max-logical-read-transactions+ 128)
(defconstant +max-logical-read-bytes+ (* 8 1024 1024))
(defconstant +max-logical-read-scan-bytes+ (* 16 1024 1024))
(defconstant +logical-change-chunk-header-size+ 12)

(declaim (ftype function call-with-logical-replication-slot))

(defvar *wal-logical-slot-read-progress* nil)

(defconstant +logical-unchanged-toast+ :logical-unchanged-toast
  "Internal row-codec marker for an unchanged external non-key value.")

(defstruct logical-change
  kind database-oid relation-oid relation-name columns key-attnums
  old-values new-values event-payload (publication-selection :unknown))

(defstruct logical-transaction
  xid commit-lsn commit-record-lsn commit-timestamp changes spool)

(defstruct logical-cursor
  "Bounded logical event reader over octets or a positioned binary stream.
Discard the cursor after a read error; the stream may have advanced."
  bytes stream size (offset 0) expected-crc)

(defstruct logical-event-lease cursor start (active-p t) (visiting-p nil))

(defstruct logical-event-slice lease offset length type checksum)

(defvar *logical-event-lease* nil)

(defun logical-event-slice-visit (slice function &key (check-crc t))
  "Visit a slice in bounded blocks while its event callback owns the cursor."
  (let* ((lease (logical-event-slice-lease slice))
         (cursor (logical-event-lease-cursor lease))
         (stream (logical-cursor-stream cursor))
         (offset (logical-event-slice-offset slice))
         (length (logical-event-slice-length slice))
         (saved (and stream (file-position stream)))
         (crc #xffffffff))
    (unless (and (logical-event-lease-active-p lease) (not (logical-event-lease-visiting-p lease))
                 (<= (+ offset length) (logical-cursor-limit cursor)))
      (error "logical event slice is expired or busy"))
    (sb-sys:without-interrupts
      (setf (logical-event-lease-visiting-p lease) t)
      (unwind-protect
           (sb-sys:with-local-interrupts
             (progn
               (when stream
                 (unless (and saved (file-position stream (+ (logical-event-lease-start lease) offset)))
                   (error "cannot seek logical event slice")))
               (loop for position from offset below (+ offset length) by 65536
                     for count = (min 65536 (- (+ offset length) position))
                     for bytes = (if stream
                                     (let ((buffer (make-array count :element-type '(unsigned-byte 8))))
                                       (unless (= (read-sequence buffer stream) count)
                                         (error "truncated logical event slice"))
                                       buffer)
                                     (subseq (logical-cursor-bytes cursor) position (+ position count)))
                     do (loop for byte across bytes
                              do (setf crc (logxor (ash crc -8)
                                                   (aref *wal-crc32-table*
                                                         (logand (logxor crc byte) #xff)))))
                        (funcall function bytes))
               (let ((checksum (logxor crc #xffffffff)))
                 (when (and check-crc (/= checksum (logical-event-slice-checksum slice)))
                   (error "logical event slice checksum mismatch"))
                 checksum)))
        (unwind-protect
             (when (and stream saved)
               (unless (file-position stream saved) (error "cannot restore logical event position")))
          (setf (logical-event-lease-visiting-p lease) nil))))))

(defun logical-make-event-slice (cursor length type)
  (let* ((offset (logical-cursor-offset cursor))
         (slice (make-logical-event-slice :lease *logical-event-lease*
                                          :offset offset :length length :type type)))
    (logical-cursor-require cursor length)
    (setf (logical-event-slice-checksum slice)
          (logical-event-slice-visit slice (lambda (bytes) (declare (ignore bytes))) :check-crc nil))
    (if (member type '(:text :varchar :json))
        (logical-validate-utf8 cursor length)
        (logical-skip-value-octets cursor length))
    (logical-event-slice-visit slice (lambda (bytes) (declare (ignore bytes))))
    slice))

(defstruct logical-reader-state
  database-oid batch-open-p xid chunk-count next-chunk fragment-batch
  (change-bytes 0) (changes '()) (transactions '()) spool catalog-p row-p)

(defvar *logical-reader-spool-scope* nil)

(declaim (notinline logical-spool-bytes))

(declaim (special *query-temp-file-limit-bytes*))

(defun call-with-logical-reader-spools (function)
  "Own all slot-reader spools until FUNCTION returns or unwinds."
  (let ((*logical-reader-spool-scope* (list nil)))
    (unwind-protect (funcall function)
      (dolist (spool (car *logical-reader-spool-scope*))
        (close-logical-spool spool)))))

(defun logical-reader-owned-spool (state)
  (unless *logical-reader-spool-scope* (error "logical reader has no spool scope"))
  (or (logical-reader-state-spool state)
      (sb-sys:without-interrupts
        (let* ((*query-temp-file-limit-bytes* nil)
               (spool (make-logical-spool)))
          (setf (logical-reader-state-spool state) spool)
          (push spool (car *logical-reader-spool-scope*))
          spool))))

(defun logical-reader-store-validated-event (state change event)
  "Store original bytes after validating their complete decoded metadata."
  (when (= (logical-change-database-oid change) (logical-reader-state-database-oid state))
    (logical-spool-append (logical-reader-owned-spool state) event))
  (values))

(declaim (special *logical-validation-only*))

(defun logical-reader-store-fragment-cursor (state cursor expected-crc)
  "Validate a completed event, then retain its original framed bytes when selected."
  (let ((size (logical-cursor-limit cursor)))
    (multiple-value-bind (sink metrics) (logical-encoding-sink size)
      (loop while (< (logical-cursor-offset cursor) size)
            do (funcall sink (logical-read-octets cursor
                                                  (min 65536 (- size (logical-cursor-offset cursor))))))
      (multiple-value-bind (count checksum) (funcall metrics)
        (unless (and (= count size) (= checksum expected-crc))
          (error "logical event checksum mismatch"))))
    (when (logical-cursor-stream cursor)
      (unless (file-position (logical-cursor-stream cursor) 0)
        (error "cannot rewind logical event spool")))
    (setf (logical-cursor-offset cursor) 0)
    (let* ((*logical-validation-only* t)
           (change (logical-decode-change-from-cursor cursor)))
      (unless (member (logical-change-kind change) '(:insert :update :delete :truncate))
        (error "catalog event appears in a transactional row-change record"))
      (when (logical-cursor-stream cursor)
        (unless (file-position (logical-cursor-stream cursor) 0)
          (error "cannot rewind logical event spool")))
      (setf (logical-cursor-offset cursor) 0)
      (multiple-value-bind (sink metrics) (logical-encoding-sink size)
        (loop while (< (logical-cursor-offset cursor) size)
              do (funcall sink (logical-read-octets cursor
                                                    (min 65536 (- size (logical-cursor-offset cursor))))))
        (multiple-value-bind (count checksum) (funcall metrics)
          (unless (and (= count size) (= checksum expected-crc))
            (error "logical event checksum mismatch"))))
      (setf (logical-reader-state-row-p state) t)
      (when (= (logical-change-database-oid change) (logical-reader-state-database-oid state))
        (when (logical-cursor-stream cursor)
          (unless (file-position (logical-cursor-stream cursor) 0)
            (error "cannot rewind logical event spool")))
        (setf (logical-cursor-offset cursor) 0)
        (logical-spool-append-cursor (logical-reader-owned-spool state) cursor expected-crc))))
  (values))

(defun logical-reader-store-change (state change)
  (if *logical-reader-spool-scope*
      (when (= (logical-change-database-oid change) (logical-reader-state-database-oid state))
        (logical-spool-append-change (logical-reader-owned-spool state)
                                     change +max-logical-wal-event-bytes+))
      (progn
        (when (> (logical-reader-state-change-bytes state) +max-unscoped-logical-materialization-bytes+)
          (error "unscoped logical reader exceeds its materialization limit"))
        (setf (logical-reader-state-changes state)
              (nconc (logical-reader-state-changes state) (list change)))))
  (values))

(defun logical-transaction-map-changes (transaction function &key slices)
  "Visit each committed change while its owning reader scope is active."
  (if (logical-transaction-spool transaction)
      (logical-spool-map-event-cursors
       (logical-transaction-spool transaction)
       (lambda (cursor)
         (let* ((*logical-validation-only* nil)
                (lease (and slices (make-logical-event-lease
                                    :cursor cursor
                                    :start (if (logical-cursor-stream cursor)
                                               (file-position (logical-cursor-stream cursor))
                                               0))))
                (*logical-event-lease* lease))
           (declare (special *logical-validation-only*))
           (unwind-protect
                (let ((change (logical-decode-change-from-cursor cursor)))
                  (when lease
                    (logical-event-slice-visit
                     (make-logical-event-slice :lease lease :offset 0
                                               :length (logical-cursor-limit cursor)
                                               :checksum (logical-cursor-expected-crc cursor))
                     (lambda (bytes) (declare (ignore bytes)))))
                  (funcall function change))
             (when lease (setf (logical-event-lease-active-p lease) nil))))))
      (dolist (change (logical-transaction-changes transaction))
        (funcall function change)))
  (values))

(defun logical-transaction-map-send-changes (transaction function)
  (logical-transaction-map-changes transaction function :slices t))

(declaim (notinline logical-fragment-assembler-bytes logical-fragment-batch-p
                    logical-fragment-batch-xid logical-fragment-batch-count
                    logical-fragment-batch-next logical-fragment-batch-assembler))

(declaim (notinline logical-recovery-assembler-bytes))

(declaim (notinline logical-fragment-batch-validate-only))

(defun logical-type-code (type)
  "Map a column type to its logical schema code; text, varchar and json are distinct."
  (case type
    (:int4 1)
    (:int8 2)
    (:varchar 3)
    (:numeric 4)
    (:int2 5)
    (:float4 6)
    (:float8 7)
    (:date 8)
    (:time 9)
    (:timestamp 10)
    (:timestamptz 11)
    (:interval 12)
    (:timetz 13)
    (:text 14)
    (:bytea 15)
    (:json 16)
    (:jsonb 17)
    (otherwise (error "logical replication does not support type ~a" type))))

(defun logical-type-from-code (code)
  (case code
    (1 :int4)
    (2 :int8)
    (3 :varchar)
    (4 :numeric)
    (5 :int2)
    (6 :float4)
    (7 :float8)
    (8 :date)
    (9 :time)
    (10 :timestamp)
    (11 :timestamptz)
    (12 :interval)
    (13 :timetz)
    (14 :text)
    (15 :bytea)
    (16 :json)
    (17 :jsonb)
    (otherwise (error "invalid logical column type code ~a" code))))

(defun logical-kind-code (kind)
  (case kind
    (:insert 1)
    (:update 2)
    (:delete 3)
    (:truncate 4)
    (:ddl 5)
    (:sequence-state 6)
    (otherwise (error "invalid logical row change kind ~a" kind))))

(defun logical-kind-from-code (code)
  (case code
    (1 :insert)
    (2 :update)
    (3 :delete)
    (4 :truncate)
    (5 :ddl)
    (6 :sequence-state)
    (otherwise (error "invalid logical row change code ~a" code))))

(defun logical-write-u8 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFF))
    (error "logical WAL byte is out of range: ~a" value))
  (if (functionp buffer)
      (funcall buffer value)
      (vector-push-extend value buffer)))

(defun logical-write-u16 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFFFF))
    (error "logical WAL 16-bit value is out of range: ~a" value))
  (dotimes (shift 2)
    (logical-write-u8 buffer (logand (ash value (* -8 shift)) #xFF))))

(defun logical-write-u32 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFFFFFFFF))
    (error "logical WAL 32-bit value is out of range: ~a" value))
  (dotimes (shift 4)
    (logical-write-u8 buffer (logand (ash value (* -8 shift)) #xFF))))

(defun logical-write-u64 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFFFFFFFFFFFFFFFF))
    (error "logical WAL 64-bit value is out of range: ~a" value))
  (dotimes (shift 8)
    (logical-write-u8 buffer (logand (ash value (* -8 shift)) #xFF))))

(defun logical-write-octets (buffer octets)
  (if (functionp buffer)
      (funcall buffer octets)
      (loop for byte across octets do (vector-push-extend byte buffer)))
  buffer)

(defun logical-write-string-u32 (buffer string)
  "Encode UTF-8 in bounded blocks, retaining the existing u32 byte length."
  (let ((size (loop for start from 0 below (length string) by 4096
                    for end = (min (length string) (+ start 4096))
                    sum (length (sb-ext:string-to-octets string :external-format :utf-8
                                                                :start start :end end)))))
    (logical-write-u32 buffer size)
    (loop for start from 0 below (length string) by 4096
          for end = (min (length string) (+ start 4096))
          do (logical-write-octets buffer
                                   (sb-ext:string-to-octets string :external-format :utf-8
                                                                   :start start :end end)))))

(defclass logical-utf8-output-stream (sb-gray:fundamental-character-output-stream)
  ((buffer :initarg :buffer :reader logical-utf8-buffer)
   (characters :initform (make-array 4096 :element-type 'character :fill-pointer 0)
               :reader logical-utf8-characters)
   (size :initform 0 :accessor logical-utf8-size)))

(defun logical-flush-utf8-output (stream)
  (let ((characters (logical-utf8-characters stream)))
    (unless (zerop (length characters))
      (let ((octets (sb-ext:string-to-octets characters :external-format :utf-8)))
        (incf (logical-utf8-size stream) (length octets))
        (when (logical-utf8-buffer stream)
          (logical-write-octets (logical-utf8-buffer stream) octets)))
      (setf (fill-pointer characters) 0))))

(defmethod sb-gray:stream-write-char ((stream logical-utf8-output-stream) character)
  (let ((characters (logical-utf8-characters stream)))
    (vector-push character characters)
    (when (= (length characters) (array-total-size characters))
      (logical-flush-utf8-output stream)))
  character)

(defun logical-write-jsonb-u32 (buffer value)
  "Count canonical UTF-8 bytes, then emit them in bounded blocks."
  (let ((counter (make-instance 'logical-utf8-output-stream :buffer nil)))
    (sql-jsonb-write value counter)
    (logical-flush-utf8-output counter)
    (logical-write-u32 buffer (logical-utf8-size counter))
    (let ((writer (make-instance 'logical-utf8-output-stream :buffer buffer)))
      (sql-jsonb-write value writer)
      (logical-flush-utf8-output writer))))

(defun logical-write-value (buffer type value)
  "Encode a value; text, varchar and json share UTF-8 tag 3. Their schema codes
remain distinct. Bytea uses tag 15 followed by a u32 length and raw octets."
  (cond
    ((eq value +logical-unchanged-toast+) (logical-write-u8 buffer 18))
    ((eq value +null+)
     (logical-write-u8 buffer 0))
    ((eq type :timetz)
     (logical-write-u8 buffer 13)
     (logical-write-u64 buffer (logand (sql-timetz-microseconds value) #xffffffffffffffff))
     (logical-write-u32 buffer (logand (sql-timetz-zone-west value) #xffffffff)))
    ((eq type :interval)
     (logical-write-u8 buffer 12)
     (logical-write-u32 buffer (logand (sql-interval-months value) #xffffffff))
     (logical-write-u32 buffer (logand (sql-interval-days value) #xffffffff))
     (logical-write-u64 buffer (logand (sql-interval-microseconds value) #xffffffffffffffff)))
    ((member type '(:timestamp :timestamptz))
     (logical-write-u8 buffer (if (eq type :timestamp) 10 11))
     (logical-write-u64 buffer (logand value #xffffffffffffffff)))
    ((eq type :time)
     (logical-write-u8 buffer 9)
     (logical-write-u64 buffer (logand value #xffffffffffffffff)))
    ((eq type :date)
     (logical-write-u8 buffer 8)
     (logical-write-u32 buffer (logand value #xffffffff)))
    ((eq type :float4)
     (logical-write-u8 buffer 6)
     (logical-write-u32 buffer (sql-float-bits value type)))
    ((eq type :float8)
     (logical-write-u8 buffer 7)
     (logical-write-u64 buffer (sql-float-bits value type)))
    ((eq type :int2)
     (unless (and (integerp value) (<= -32768 value 32767))
       (error "invalid int2 logical row value ~s" value))
     (logical-write-u8 buffer 5)
     (logical-write-u16 buffer (logand value #xffff)))
    ((eq type :int4)
     (unless (and (integerp value) (<= +int4-min+ value +int4-max+))
       (error "invalid int4 logical row value ~s" value))
     (logical-write-u8 buffer 1)
     (logical-write-u32 buffer (logand value #xFFFFFFFF)))
    ((eq type :int8)
     (unless (and (integerp value) (<= +int8-min+ value +int8-max+))
       (error "invalid int8 logical row value ~s" value))
     (logical-write-u8 buffer 2)
     (logical-write-u64 buffer (logand value #xFFFFFFFFFFFFFFFF)))
    ((eq type :numeric)
     (unless (sql-numeric-p value) (error "invalid numeric logical row value ~s" value))
     (logical-write-u8 buffer 4)
     (logical-write-string-u32 buffer (sql-numeric-output value)))
    ((eq type :jsonb)
     (unless (sql-jsonb-p value) (error "invalid jsonb logical row value"))
     (logical-write-u8 buffer 17)
     (logical-write-jsonb-u32 buffer value))
    ((eq type :bytea)
     (check-type value (vector (unsigned-byte 8)))
     (logical-write-u8 buffer 15)
     (logical-write-u32 buffer (length value))
     (logical-write-octets buffer value))
    ((member type '(:varchar :text :json))
     (unless (stringp value)
       (error "invalid varchar logical row value ~s" value))
     (logical-write-u8 buffer 3)
     (logical-write-string-u32 buffer value))
    (t (error "logical replication does not support type ~a" type))))

(defun logical-write-row (buffer columns values)
  (unless (and values (= (length columns) (length values)))
    (error "logical row width does not match its relation description"))
  (loop for column in columns
        for value across values
        do (logical-write-value buffer (getf column :type) value)))

(defun logical-identity-index-usable-p (index table)
  (let ((attnums (pg-index-columns index)))
    (and (pg-index-valid-p index)
         (pg-index-unique-p index)
         attnums
         (every (lambda (attnum)
                  (<= 1 attnum (length (pg-class-columns table))))
                attnums)
         (or (pg-index-primary-p index)
             (pg-index-nulls-not-distinct-p index)
             (every (lambda (attnum)
                      (pg-column-notnull
                       (aref (pg-class-columns table) (1- attnum))))
                    attnums)))))

(defun logical-table-replica-identity (table)
  "Choose the primary or otherwise unambiguous unique index for pgoutput K."
  (let* ((indexes (table-indexes table))
         (primary (find-if (lambda (index)
                             (and (pg-index-primary-p index)
                                  (logical-identity-index-usable-p index table)))
                           indexes))
         (index (or primary
                    (find-if (lambda (candidate)
                               (logical-identity-index-usable-p candidate table))
                             indexes))))
    (and index (copy-list (pg-index-columns index)))))

(defun logical-capture-publications (tables kind)
  "Freeze selection at the successful mutation's writer-lock linearization point."
  (unless (sb-thread:holding-mutex-p *write-lock*)
    (error "historic publication capture requires the writer lock"))
  (unless (member kind '(:insert :update :delete :truncate))
    (error "unsupported historic publication operation"))
  (sb-thread:with-mutex (*catalog-mutex*)
    (loop for table in tables
          collect
          (list :relation-oid (pg-class-oid table)
                :publications
                (sort
                 (loop for publication being the hash-values of (database-publications (current-database))
                       for entry = (publication-table-entry publication (pg-class-name table))
                       when (and entry (member kind (pg-publication-publish-actions publication)))
                         collect (list :name (copy-seq (pg-publication-name publication))
                                       :oid (pg-publication-oid publication)
                                       :generation (or (pg-publication-generation publication) 0)
                                       :unsupported (not (null (getf entry :filter)))))
                 #'string< :key (lambda (entry) (getf entry :name)))))))

(defun logical-publication-entry-valid-p (entry)
  (and (logical-plist-has-exact-keys-p entry '(:name :oid :generation :unsupported))
       (let ((name (getf entry :name)) (oid (getf entry :oid)) (generation (getf entry :generation)))
         (and (stringp name) (not (find (code-char 0) name))
              (<= 1 (length (string->bytes name)) 63)
              (integerp oid) (< 0 oid #x100000000)
              (integerp generation) (<= 0 generation #xffffffffffffffff)
              (member (getf entry :unsupported) '(nil t))))))

(defun logical-validate-publication-selection (selection)
  (unless (and (listp selection) (<= (length selection) #xffff))
    (error "invalid historic publication selection"))
  (let ((oids nil))
    (dolist (relation selection)
      (unless (logical-plist-has-exact-keys-p relation '(:relation-oid :publications))
        (error "invalid historic publication relation"))
      (let ((oid (getf relation :relation-oid)) (publications (getf relation :publications)))
        (unless (and (integerp oid) (< 0 oid #x100000000) (not (member oid oids))
                     (listp publications) (<= (length publications) #xffff)
                     (every #'logical-publication-entry-valid-p publications)
                     (= (length publications)
                        (length (remove-duplicates publications :key (lambda (entry) (getf entry :name))
                                                                :test #'equal)))
                     (= (length publications)
                        (length (remove-duplicates publications :key (lambda (entry) (getf entry :oid))))))
          (error "invalid or duplicate historic publication metadata"))
        (push oid oids))))
  selection)

(defun logical-write-publication-selection (buffer selection)
  (logical-validate-publication-selection selection)
  (logical-write-u16 buffer (length selection))
  (dolist (relation selection)
    (logical-write-u32 buffer (getf relation :relation-oid))
    (logical-write-u16 buffer (length (getf relation :publications)))
    (dolist (publication (getf relation :publications))
      (logical-write-string-u32 buffer (getf publication :name))
      (logical-write-u32 buffer (getf publication :oid))
      (logical-write-u64 buffer (getf publication :generation))
      (logical-write-u8 buffer (if (getf publication :unsupported) 1 0)))))

(defun logical-validate-row-columns (columns)
  "Require the relation descriptor to use physical slot order exactly."
  (unless (and (listp columns)
               (<= (length columns) #xffff)
               (loop for column in columns for attnum from 1
                     always (eql (getf column :attnum) attnum)))
    (error "logical row change has noncanonical column numbers"))
  columns)

(defun logical-validate-unchanged-toast (change)
  "Reject unchanged markers outside non-key varlena UPDATE new images."
  (let ((kind (logical-change-kind change))
        (keys (logical-change-key-attnums change)))
    (when (member kind '(:insert :update :delete))
      (logical-validate-row-columns (logical-change-columns change)))
    (loop for row in (list (logical-change-old-values change) (logical-change-new-values change))
          for new-p in '(nil t)
          when row
            do (unless (= (length row) (length (logical-change-columns change)))
                 (error "logical row width does not match its relation description"))
               (loop for value across row for column in (logical-change-columns change)
                     when (eq value +logical-unchanged-toast+)
                       do (unless (and (eq kind :update) new-p keys
                                       (not (member (getf column :attnum) keys))
                                       (member (getf column :type) '(:text :varchar :bytea :json :jsonb :numeric)))
                            (error "invalid unchanged TOAST marker")))))
  change)

(defun logical-encode-change (change &optional (buffer (make-array 0 :element-type '(unsigned-byte 8)
                                                                     :adjustable t :fill-pointer 0)))
  (logical-validate-change-publications change)
  (logical-validate-unchanged-toast change)
  (let ((columns (logical-change-columns change))
        (key-attnums (logical-change-key-attnums change)))
    (when (member (logical-change-kind change)
                  '(:truncate :ddl :sequence-state))
      (logical-write-u8 buffer 0)
      (logical-write-u8 buffer (if (and (eq (logical-change-kind change) :truncate)
                                        (not (eq (logical-change-publication-selection change) :unknown))) 4 2))
      (logical-write-u8 buffer (logical-kind-code (logical-change-kind change)))
      (logical-write-u32 buffer (logical-change-database-oid change))
      (when (and (eq (logical-change-kind change) :truncate)
                 (not (eq (logical-change-publication-selection change) :unknown)))
        (logical-write-publication-selection buffer (logical-change-publication-selection change)))
      (logical-write-string-u32 buffer (logical-change-event-payload change))
      (return-from logical-encode-change buffer))
    ;; Row format 5 carries a publication-selection presence flag and supports
    ;; the unchanged external value marker.
    (logical-write-u8 buffer 0)
    (logical-write-u8 buffer 5)
    (logical-write-u8 buffer (logical-kind-code (logical-change-kind change)))
    (logical-write-u32 buffer (logical-change-database-oid change))
    (logical-write-u8 buffer (if (eq (logical-change-publication-selection change) :unknown) 0 1))
    (unless (eq (logical-change-publication-selection change) :unknown)
      (logical-write-publication-selection buffer (logical-change-publication-selection change)))
    (logical-write-u32 buffer (logical-change-relation-oid change))
    (logical-write-string-u32 buffer (logical-change-relation-name change))
    (logical-write-u16 buffer (length columns))
    (dolist (column columns)
      (logical-write-u16 buffer (getf column :attnum))
      (logical-write-string-u32 buffer (getf column :name))
      (logical-write-u8 buffer (logical-type-code (getf column :type)))
      (logical-write-u32 buffer (logand (getf column :typmod) #xFFFFFFFF))
      (logical-write-u8 buffer (if (getf column :notnull) 1 0)))
    (logical-write-u16 buffer (length key-attnums))
    (dolist (attnum key-attnums)
      (logical-write-u16 buffer attnum))
    (dolist (values (list (logical-change-old-values change)
                          (logical-change-new-values change)))
      (logical-write-u8 buffer (if values 1 0))
      (when values (logical-write-row buffer columns values)))
    (unless (case (logical-change-kind change)
              (:insert (and (null (logical-change-old-values change))
                            (logical-change-new-values change)))
              (:update (and (logical-change-old-values change)
                            (logical-change-new-values change)))
              (:delete (and (logical-change-old-values change)
                            (null (logical-change-new-values change)))))
      (error "logical row change has inconsistent old/new images"))
    buffer))

(defun logical-cursor-limit (cursor)
  "Return the known byte bound of a vector- or stream-backed cursor."
  (if (logical-cursor-stream cursor)
      (logical-cursor-size cursor)
      (length (logical-cursor-bytes cursor))))

(defun logical-cursor-require (cursor count)
  (unless (and (integerp count) (not (minusp count))
               (<= (+ (logical-cursor-offset cursor) count) (logical-cursor-limit cursor)))
    (error "truncated logical row-change payload")))

(defun logical-read-u8 (cursor)
  (logical-cursor-require cursor 1)
  (let ((byte (if (logical-cursor-stream cursor)
                  (read-byte (logical-cursor-stream cursor) nil nil)
                  (aref (logical-cursor-bytes cursor) (logical-cursor-offset cursor)))))
    (unless byte (error "truncated logical row-change payload"))
    (incf (logical-cursor-offset cursor))
    byte))

(defun logical-read-u16 (cursor)
  (logical-cursor-require cursor 2)
  (if (logical-cursor-stream cursor)
      (loop for shift below 2 sum (ash (logical-read-u8 cursor) (* 8 shift)))
      (prog1 (wal-u16 (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
        (incf (logical-cursor-offset cursor) 2))))

(defun logical-read-u32 (cursor)
  (logical-cursor-require cursor 4)
  (if (logical-cursor-stream cursor)
      (loop for shift below 4 sum (ash (logical-read-u8 cursor) (* 8 shift)))
      (prog1 (wal-u32 (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
        (incf (logical-cursor-offset cursor) 4))))

(defun logical-read-u64 (cursor)
  (logical-cursor-require cursor 8)
  (if (logical-cursor-stream cursor)
      (loop for shift below 8 sum (ash (logical-read-u8 cursor) (* 8 shift)))
      (prog1 (wal-u64 (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
        (incf (logical-cursor-offset cursor) 8))))

(defun logical-read-octets (cursor count)
  (logical-cursor-require cursor count)
  (let ((bytes (if (logical-cursor-stream cursor)
                   (let ((result (make-array count :element-type '(unsigned-byte 8))))
                     (unless (= (read-sequence result (logical-cursor-stream cursor)) count)
                       (error "truncated logical row-change payload"))
                     result)
                   (subseq (logical-cursor-bytes cursor) (logical-cursor-offset cursor)
                           (+ (logical-cursor-offset cursor) count)))))
    (incf (logical-cursor-offset cursor) count)
    bytes))

(defun logical-read-string-u32 (cursor)
  (sb-ext:octets-to-string (logical-read-octets cursor (logical-read-u32 cursor))
                           :external-format :utf-8))

(defun logical-u32-signed (value)
  (if (logbitp 31 value) (- value #x100000000) value))

(defun logical-u64-signed (value)
  (if (logbitp 63 value) (- value #x10000000000000000) value))

(defvar *logical-validation-only* nil)

(defun logical-validate-utf8 (cursor count)
  "Consume COUNT UTF-8 bytes, rejecting overlong, surrogate and out-of-range forms."
  (logical-cursor-require cursor count)
  (let ((end (+ (logical-cursor-offset cursor) count)))
    (labels ((next-byte ()
               (when (>= (logical-cursor-offset cursor) end)
                 (error "truncated UTF-8 in logical row-change payload"))
               (logical-read-u8 cursor))
             (continuation (minimum maximum)
               (let ((byte (next-byte)))
                 (unless (<= minimum byte maximum)
                   (error "invalid UTF-8 in logical row-change payload")))))
      (loop while (< (logical-cursor-offset cursor) end)
            for lead = (next-byte)
            do (cond
                 ((<= lead #x7f))
                 ((<= #xc2 lead #xdf) (continuation #x80 #xbf))
                 ((<= #xe0 lead #xef)
                  (continuation (if (= lead #xe0) #xa0 #x80)
                                (if (= lead #xed) #x9f #xbf))
                  (continuation #x80 #xbf))
                 ((<= #xf0 lead #xf4)
                  (continuation (if (= lead #xf0) #x90 #x80)
                                (if (= lead #xf4) #x8f #xbf))
                  (continuation #x80 #xbf)
                  (continuation #x80 #xbf))
                 (t (error "invalid UTF-8 in logical row-change payload"))))))
  (values))

(defun logical-skip-value-octets (cursor count)
  "Consume a length-prefixed binary value without retaining its bytes."
  (logical-cursor-require cursor count)
  (loop while (plusp count)
        for size = (min count 65536)
        do (logical-read-octets cursor size)
           (decf count size))
  (values))

(defun logical-jsonb-canonical-simple-p (cursor length)
  "Recognize canonical JSONB strings and simple arrays without materializing them.
Leave CURSOR unchanged. Objects and numbers use the ordinary parser."
  (let* ((offset (logical-cursor-offset cursor))
         (stream (logical-cursor-stream cursor))
         (position (and stream (file-position stream)))
         (end (+ offset length)))
    (unwind-protect
         (catch 'logical-jsonb-noncanonical
           (progn
             (logical-cursor-require cursor length)
             (logical-validate-utf8 cursor length)
             (when stream (unless (file-position stream position) (error "cannot rewind JSONB")))
             (setf (logical-cursor-offset cursor) offset)
             (labels ((take-byte ()
                        (when (>= (logical-cursor-offset cursor) end)
                          (throw 'logical-jsonb-noncanonical nil))
                        (logical-read-u8 cursor))
                      (expect (byte)
                        (unless (= (take-byte) byte) (throw 'logical-jsonb-noncanonical nil)))
                      (hex (byte)
                        (cond ((<= 48 byte 57) (- byte 48))
                              ((<= 97 byte 102) (+ 10 (- byte 97)))
                              (t (throw 'logical-jsonb-noncanonical nil))))
                      (string-value ()
                        (loop for byte = (take-byte)
                              do (cond
                                   ((= byte 34) (return))
                                   ((< byte 32) (throw 'logical-jsonb-noncanonical nil))
                                   ((= byte 92)
                                    (let ((escape (take-byte)))
                                      (cond
                                        ((member escape '(34 92 98 102 110 114 116)) nil)
                                        ((= escape 117)
                                         (expect 48) (expect 48)
                                         (let ((code (+ (* 16 (hex (take-byte))) (hex (take-byte)))))
                                           (unless (and (<= 1 code 31)
                                                        (not (member code '(8 9 10 12 13))))
                                             (throw 'logical-jsonb-noncanonical nil))))
                                        (t (throw 'logical-jsonb-noncanonical nil))))))))
                      (word (text)
                        (loop for character across text do (expect (char-code character))))
                      (value (first depth)
                        (when (> depth 512) (throw 'logical-jsonb-noncanonical nil))
                        (case first
                          (34 (string-value))
                          (116 (word "rue"))
                          (102 (word "alse"))
                          (110 (word "ull"))
                          (91 (let ((next (take-byte)))
                                (unless (= next 93)
                                  (loop
                                    (value next (1+ depth))
                                    (case (take-byte)
                                      (93 (return))
                                      (44 (expect 32) (setf next (take-byte)))
                                      (otherwise (throw 'logical-jsonb-noncanonical nil)))))))
                          (otherwise (throw 'logical-jsonb-noncanonical nil)))))
               (value (take-byte) 0)
               (= (logical-cursor-offset cursor) end)))
           )
      (when stream
        (unless (file-position stream position) (error "cannot restore JSONB cursor")))
      (setf (logical-cursor-offset cursor) offset))))

(defun logical-read-value (cursor type)
  "Decode a value, accepting tag 3 for text, varchar or json and tag 15 for
length-prefixed bytea octets; reject mismatched column types. JSON text is not
revalidated by this codec."
  (let ((tag (logical-read-u8 cursor)))
    (case tag
      (18 (unless (member type '(:varchar :text :bytea :json :jsonb :numeric))
            (error "logical unchanged TOAST tag does not match its column type"))
       +logical-unchanged-toast+)
      (0 +null+)
      (1 (unless (eq type :int4)
           (error "logical value tag does not match its column type"))
       (logical-u32-signed (logical-read-u32 cursor)))
      (2 (unless (eq type :int8)
           (error "logical value tag does not match its column type"))
       (logical-u64-signed (logical-read-u64 cursor)))
      (3 (unless (member type '(:varchar :text :json))
           (error "logical value tag does not match its column type"))
       (let ((length (logical-read-u32 cursor)))
         (cond (*logical-validation-only* (logical-validate-utf8 cursor length) "")
               ((and *logical-event-lease* (eq cursor (logical-event-lease-cursor *logical-event-lease*)))
                (logical-make-event-slice cursor length type))
               (t (sb-ext:octets-to-string (logical-read-octets cursor length)
                                           :external-format :utf-8)))))
      (4 (unless (eq type :numeric)
           (error "logical value tag does not match its column type"))
       (sql-numeric-input (logical-read-string-u32 cursor)))
      (5 (unless (eq type :int2)
           (error "logical value tag does not match its column type"))
       (let ((value (logical-read-u16 cursor)))
         (if (logbitp 15 value) (- value #x10000) value)))
      (6 (unless (eq type :float4) (error "logical value tag does not match its column type"))
       (sql-float-from-bits (logical-read-u32 cursor) type))
      (7 (unless (eq type :float8) (error "logical value tag does not match its column type"))
       (sql-float-from-bits (logical-read-u64 cursor) type))
      (8 (unless (eq type :date) (error "logical value tag does not match its column type"))
       (logical-u32-signed (logical-read-u32 cursor)))
      (9 (unless (eq type :time) (error "logical value tag does not match its column type"))
       (logical-u64-signed (logical-read-u64 cursor)))
      (10 (unless (eq type :timestamp) (error "logical value tag does not match its column type"))
       (logical-u64-signed (logical-read-u64 cursor)))
      (11 (unless (eq type :timestamptz) (error "logical value tag does not match its column type"))
       (logical-u64-signed (logical-read-u64 cursor)))
      (12 (unless (eq type :interval) (error "logical value tag does not match its column type"))
       (make-sql-interval (logical-u32-signed (logical-read-u32 cursor))
                          (logical-u32-signed (logical-read-u32 cursor))
                          (logical-u64-signed (logical-read-u64 cursor))))
      (17 (unless (eq type :jsonb)
            (error "logical value tag does not match its column type"))
       (let ((length (logical-read-u32 cursor)))
         (if (and (>= length 65536)
                  (or *logical-validation-only*
                      (and *logical-event-lease*
                           (eq cursor (logical-event-lease-cursor *logical-event-lease*))))
                  (logical-jsonb-canonical-simple-p cursor length))
             (if *logical-validation-only*
                 (progn (logical-skip-value-octets cursor length) (make-sql-jsonb ""))
                 (logical-make-event-slice cursor length :jsonb))
             (sql-jsonb-input
              (sb-ext:octets-to-string (logical-read-octets cursor length)
                                       :external-format :utf-8)))))
      (15 (unless (eq type :bytea)
            (error "logical value tag does not match its column type"))
       (let ((length (logical-read-u32 cursor)))
         (cond (*logical-validation-only* (logical-skip-value-octets cursor length) #())
               ((and *logical-event-lease* (eq cursor (logical-event-lease-cursor *logical-event-lease*)))
                (logical-make-event-slice cursor length type))
               (t (logical-read-octets cursor length)))))
      (13 (unless (eq type :timetz) (error "logical value tag does not match its column type"))
       (make-sql-timetz (logical-u64-signed (logical-read-u64 cursor))
                        (logical-u32-signed (logical-read-u32 cursor))))
      (otherwise (error "invalid logical value tag ~a" tag)))))

(defun logical-read-row (cursor columns)
  (coerce (loop for column in columns
                collect (logical-read-value cursor (getf column :type)))
          'vector))

(defun logical-read-publication-selection (cursor)
  (logical-validate-publication-selection
   (loop repeat (logical-read-u16 cursor)
         collect
         (list :relation-oid (logical-read-u32 cursor)
               :publications
               (loop repeat (logical-read-u16 cursor)
                     collect (list :name (logical-read-string-u32 cursor)
                                   :oid (logical-read-u32 cursor)
                                   :generation (logical-read-u64 cursor)
                                   :unsupported
                                   (case (logical-read-u8 cursor)
                                     (0 nil) (1 t)
                                     (otherwise (error "invalid historic publication flags")))))))))

(defun logical-validate-change-publications (change)
  (let ((selection (logical-change-publication-selection change)))
    (unless (eq selection :unknown)
      (logical-validate-publication-selection selection)
      (let ((expected
              (case (logical-change-kind change)
                ((:insert :update :delete) (list (logical-change-relation-oid change)))
                (:truncate (getf (logical-read-data-string (logical-change-event-payload change))
                                 :relation-oids))
                (otherwise (error "historic selection on unsupported logical operation")))))
        (unless (equal (sort (copy-list expected) #'<)
                       (sort (mapcar (lambda (entry) (getf entry :relation-oid)) selection) #'<))
          (error "historic publication relation identities do not match the change")))))
  change)

(defun logical-decode-change-from-cursor (cursor)
  (let* ((marker (logical-read-u8 cursor))
         (event-version (if (zerop marker) (logical-read-u8 cursor) 0))
         (kind (logical-kind-from-code
                (if (zerop marker) (logical-read-u8 cursor) marker)))
         (database-oid (logical-read-u32 cursor))
         (selection (case event-version
                      (4 (logical-read-publication-selection cursor))
                      (5 (case (logical-read-u8 cursor)
                           (0 :unknown)
                           (1 (logical-read-publication-selection cursor))
                           (otherwise (error "invalid logical publication-selection flag"))))
                      (otherwise :unknown))))
    (when (member event-version '(2 4))
      (unless (if (= event-version 4) (eq kind :truncate)
                  (member kind '(:truncate :ddl :sequence-state)))
        (error "invalid logical catalog-event kind ~a" kind))
      (let ((payload (logical-read-string-u32 cursor)))
        (unless (= (logical-cursor-offset cursor) (logical-cursor-limit cursor))
          (error "logical event payload has trailing bytes"))
        (return-from logical-decode-change-from-cursor
          (logical-validate-change-publications
           (make-logical-change :kind kind :database-oid database-oid
                                :publication-selection selection :event-payload payload)))))
    (unless (= event-version 5)
      (error "unsupported logical row-change event version"))
    (let* ((relation-oid (logical-read-u32 cursor))
           (relation-name (logical-read-string-u32 cursor))
           (column-count (logical-read-u16 cursor))
           (columns
             (logical-validate-row-columns
              (loop repeat column-count
                    collect (list :attnum (logical-read-u16 cursor)
                                  :name (logical-read-string-u32 cursor)
                                  :type (logical-type-from-code
                                         (logical-read-u8 cursor))
                                  :typmod (logical-u32-signed
                                           (logical-read-u32 cursor))
                                  :notnull (case (logical-read-u8 cursor)
                                             (0 nil)
                                             (1 t)
                                             (otherwise
                                              (error "invalid logical column flags")))))))
           (key-attnums (loop repeat (logical-read-u16 cursor)
                              collect (logical-read-u16 cursor)))
           (rows
             (loop repeat 2
                   collect (case (logical-read-u8 cursor)
                             (0 nil)
                             (1 (logical-read-row cursor columns))
                             (otherwise (error "invalid logical row-image flag")))))
           (change (make-logical-change :kind kind
                                        :database-oid database-oid
                                        :relation-oid relation-oid
                                        :relation-name relation-name
                                        :columns columns
                                        :key-attnums key-attnums
                                        :old-values (first rows)
                                        :new-values (second rows)
                                        :publication-selection selection)))
      (unless (= (logical-cursor-offset cursor) (logical-cursor-limit cursor))
        (error "logical row-change payload has trailing bytes"))
      (unless (and (<= (length key-attnums) (length columns))
                   (= (length key-attnums) (length (remove-duplicates key-attnums)))
                   (every (lambda (attnum)
                            (find attnum columns :key (lambda (column)
                                                        (getf column :attnum))
                                                 :test #'=))
                          key-attnums))
        (error "logical row change has invalid replica-identity columns"))
      (unless (case kind
                (:insert (and (null (logical-change-old-values change))
                              (logical-change-new-values change)))
                (:update (and (logical-change-old-values change)
                              (logical-change-new-values change)))
                (:delete (and (logical-change-old-values change)
                              (null (logical-change-new-values change)))))
        (error "logical row change has inconsistent old/new images"))
      (logical-validate-unchanged-toast change)
      (logical-validate-change-publications change))))

(defun logical-decode-change (bytes)
  "Decode a complete vector-backed logical WAL event."
  (logical-decode-change-from-cursor (make-logical-cursor :bytes bytes)))

(defun logical-validate-change-from-cursor (cursor)
  "Validate an event while discarding text and binary values during recovery.
Other types retain their normal semantic checks and may allocate their values."
  (let* ((*logical-validation-only* t)
         (change (logical-decode-change-from-cursor cursor)))
    (unless (member (logical-change-kind change) '(:insert :update :delete :truncate))
      (error "catalog event appears in a transactional row-change record")))
  (values))

(defun logical-encode-catalog-event (database-oid ast)
  (let* ((text (catalog-readable-data-string
                (list :database-oid database-oid :ast ast)))
         (change (make-logical-change
                  :kind :ddl :database-oid database-oid
                  :event-payload text)))
    (logical-wrap-catalog-event database-oid
                                (logical-encode-change change))))

(defun logical-wrap-catalog-event (database-oid event-bytes)
  "Prefix an event with the database and byte length required by wal.lisp."
  (let ((buffer (make-array 0 :element-type '(unsigned-byte 8)
                              :adjustable t :fill-pointer 0)))
    (logical-write-u32 buffer database-oid)
    (logical-write-u32 buffer (length event-bytes))
    (logical-write-octets buffer event-bytes)))

(defun logical-read-data-string (text)
  "Read one data-only S-expression, rejecting evaluation and trailing forms."
  (let ((eof (gensym "LOGICAL-EVENT-EOF-")))
    (with-input-from-string (stream text)
      (with-standard-io-syntax
        (let* ((*read-eval* nil)
               (value (read stream nil eof)))
          (when (eq value eof)
            (error "logical event payload is empty"))
          (unless (eq (read stream nil eof) eof)
            (error "logical event payload has trailing data"))
          value)))))

(defun logical-decode-catalog-event (bytes)
  (let ((cursor (make-logical-cursor :bytes bytes)))
    (let* ((database-oid (logical-read-u32 cursor))
           (event-bytes (logical-read-octets cursor (logical-read-u32 cursor))))
      (unless (= (logical-cursor-offset cursor) (length bytes))
        (error "WAL logical catalog event has trailing bytes"))
      (let ((change (logical-decode-change event-bytes)))
        (unless (= database-oid (logical-change-database-oid change))
          (error "WAL logical catalog event database does not match its payload"))
        (unless (member (logical-change-kind change) '(:ddl :sequence-state))
          (error "WAL catalog event has invalid kind ~a"
                 (logical-change-kind change)))
        change))))

(defun logical-sequence-state-event (name)
  "Encode the durable state update emitted by sequence.c-style operations."
  (let ((sequence (lookup-sequence name)))
    (unless sequence
      (ereport "42P01" "sequence \"~a\" does not exist" name))
    (logical-wrap-catalog-event
     (database-oid (current-database))
     (logical-encode-change
      (make-logical-change
       :kind :sequence-state :database-oid (database-oid (current-database))
       :event-payload
       (catalog-readable-data-string
        (list :database-oid (database-oid (current-database))
              :name name :oid (pg-sequence-oid sequence)
              :definition (catalog-sequence-definition-sql sequence)
              :start (pg-sequence-start-value sequence)
              :increment (pg-sequence-increment sequence)
              :min (pg-sequence-min-value sequence)
              :max (pg-sequence-max-value sequence)
              :last (pg-sequence-last-value sequence)
              :called (pg-sequence-called-p sequence))))))))

(defun logical-capture-encoded-event (backend encoded total)
  "Own one freshly encoded event through BACKEND's transaction lifetime."
  (let ((spool (sb-sys:without-interrupts
                 (or (backend-logical-spool backend)
                     (setf (backend-logical-spool backend) (make-logical-spool))))))
    (logical-spool-append spool encoded)
    (setf (backend-logical-change-bytes backend) total))
  (values))

(defun logical-capture-change (backend change limit &key after-measure)
  "Encode CHANGE into BACKEND's spool within its per-event budget."
  (let ((spool (sb-sys:without-interrupts
                 (or (backend-logical-spool backend)
                     (setf (backend-logical-spool backend) (make-logical-spool))))))
    (let ((size (logical-spool-append-change
                 spool change limit :after-measure after-measure)))
      (incf (backend-logical-change-bytes backend) size)))
  (values))

(defun logical-note-transaction-event (kind payload &key (publication-selection :unknown))
  "Capture a transactional utility event such as TRUNCATE before XID commit."
  (when (and *backend* (backend-xid *backend*)
             (logical-note-xid-mutation-needs-capture-p (backend-xid *backend*)))
    (unless (eq kind :truncate)
      (error "unsupported logical transaction event ~a" kind))
    (let* ((text (with-output-to-string (stream)
                   (with-standard-io-syntax
                     (let ((*print-readably* t) (*print-pretty* nil))
                       (prin1 payload stream)))))
           (change (make-logical-change
                    :kind kind :publication-selection publication-selection
                    :database-oid (database-oid (current-database))
                    :event-payload text)))
      (logical-capture-change *backend* change +max-logical-transaction-bytes+)
      change)))

(defun logical-relation-columns (table)
  "Copy the immutable relation descriptor used by both row and TRUNCATE WAL."
  (loop for column across (pg-class-columns table)
        collect (list :attnum (pg-column-attnum column)
                      :name (copy-seq (pg-column-name column))
                      :type (pg-column-type column)
                      :typmod (pg-column-typmod column)
                      :notnull (pg-column-notnull column))))

(defun logical-relation-description (table)
  (list :oid (pg-class-oid table) :name (copy-seq (pg-class-name table))
        :columns (logical-relation-columns table)
        :key-attnums (copy-list (logical-table-replica-identity table))))

(defun logical-capture-row-images (kind keys old-values new-values toast-reuse)
  "Copy SQL images, keeping identity fields and marking proven external reuse."
  (let ((old-image (and old-values (copy-seq old-values)))
        (new-image (and new-values (copy-seq new-values))))
    (when (and old-image keys)
      (loop for i below (length old-image)
            unless (member (1+ i) keys) do (setf (aref old-image i) +null+)))
    (when (and (eq kind :update) keys toast-reuse new-image)
      (unless (= (length toast-reuse) (length new-image))
        (error "logical TOAST reuse width does not match row"))
      (loop for pointer across toast-reuse for i from 0
            when (and pointer (not (member (1+ i) keys)))
              do (unless (heap-external-varlena-p pointer)
                   (error "invalid logical TOAST reuse pointer"))
                 (setf (aref new-image i) +logical-unchanged-toast+)))
    (values old-image new-image)))

(declaim (ftype function logical-validate-large-row-change))

(defun logical-note-row-change (kind table &key old-values new-values toast-reuse (publication-selection :unknown))
  "Capture a successful executor row operation for its current transaction.

PostgreSQL's logical decode path queues heap changes in DecodeInsert,
DecodeUpdate, and DecodeDelete; this subset already has decoded values in the
executor, so capture the same operation before transaction commit."
  (when (and *backend* (backend-xid *backend*)
             (logical-note-xid-mutation-needs-capture-p (backend-xid *backend*)))
    (logical-test-failpoint :before-logical-row-encode kind (pg-class-name table))
    (let* ((columns
             (logical-relation-columns table))
           (key-attnums (logical-table-replica-identity table))
           (old-image nil)
           (new-image nil)
           (change (make-logical-change
                    :kind kind
                    :database-oid (database-oid (current-database))
                    :relation-oid (pg-class-oid table)
                    :relation-name (copy-seq (pg-class-name table))
                    :columns columns
                    :key-attnums key-attnums
                    :publication-selection publication-selection
                    :old-values old-image
                    :new-values new-image))
           (limit (+ +max-logical-event-bytes+
                     (if (string= (or (backend-application-name *backend*) "")
                                  "pg-lisp-subscribe-apply")
                         +max-logical-apply-headroom-bytes+
                         0))))
      (multiple-value-setq (old-image new-image)
        (logical-capture-row-images kind key-attnums old-values new-values toast-reuse))
      (setf (logical-change-old-values change) old-image
            (logical-change-new-values change) new-image)
      (logical-capture-change
       *backend* change limit
       :after-measure (lambda (size)
                        (when (> size +max-logical-transaction-bytes+)
                          (logical-validate-large-row-change change new-values))))))
  (values))

(defun logical-encode-chunk (index count events)
  (let ((buffer (make-array 0 :element-type '(unsigned-byte 8)
                              :adjustable t :fill-pointer 0)))
    (logical-write-u32 buffer index)
    (logical-write-u32 buffer count)
    (logical-write-u32 buffer (length events))
    (dolist (event events)
      (logical-write-u32 buffer (length event))
      (logical-write-octets buffer event))
    buffer))

(defun logical-split-encoded-changes (events)
  (let ((payload-limit (- +wal-segment-size+ +wal-header-size+
                          +logical-change-chunk-header-size+))
        (chunks '())
        (chunk '())
        (chunk-bytes 0))
    (dolist (event events)
      (let ((entry-bytes (+ 4 (length event))))
        (when (> entry-bytes payload-limit)
          (error "one logical row change exceeds the maximum WAL record size"))
        (when (and chunk (> (+ chunk-bytes entry-bytes) payload-limit))
          (push (nreverse chunk) chunks)
          (setf chunk nil chunk-bytes 0))
        (push event chunk)
        (incf chunk-bytes entry-bytes)))
    (when chunk (push (nreverse chunk) chunks))
    (nreverse chunks)))

(defun wal-append-logical-changes (xid encoded-changes)
  "Append ordered, bounded chunks that remain invisible until XID commits."
  (when encoded-changes
    (unless (and (integerp xid) (plusp xid))
      (error "logical row changes require a transaction ID"))
    (unless (listp encoded-changes)
      (logical-append-spool-wal xid encoded-changes)
      (return-from wal-append-logical-changes (values)))
    (let* ((events encoded-changes)
           (event-bytes (reduce #'+ events :key #'length :initial-value 0)))
      (when (> event-bytes +max-logical-wal-transaction-bytes+)
        (error "logical transaction exceeds its WAL change limit"))
      (let ((legacy-limit (- +wal-segment-size+ +wal-header-size+
                             +logical-change-chunk-header-size+ 4)))
        (if (some (lambda (event) (> (length event) legacy-limit)) events)
            (let* ((fragments (logical-plan-event-fragments
                               events (- +wal-segment-size+ +wal-header-size+
                                         +logical-fragment-header-size+)
                               +max-logical-wal-transaction-bytes+))
                   (count (length fragments)))
              (loop for fragment in fragments for index from 0
                    do (wal-append-record +wal-record-logical-changes+ xid
                                          (logical-encode-fragment index count fragment))))
            (let* ((chunks (logical-split-encoded-changes events))
                   (chunk-count (length chunks)))
              (loop for chunk in chunks for index from 0
                    do (wal-append-record +wal-record-logical-changes+ xid
                                          (logical-encode-chunk index chunk-count chunk))))))))
  (values))

(defun wal-decode-logical-change-chunk (payload &optional raw-events-p)
  (let* ((cursor (make-logical-cursor :bytes payload))
         (index (logical-read-u32 cursor))
         (count (logical-read-u32 cursor))
         (event-count (logical-read-u32 cursor))
         (change-bytes 0)
         (changes '()) (events '()))
    (unless (and (plusp count) (< index count) (plusp event-count))
      (error "invalid logical WAL chunk header"))
    (loop repeat event-count
          for length = (logical-read-u32 cursor)
          for event = (progn
                        (unless (<= 1 length +max-logical-wal-event-bytes+)
                          (error "invalid logical WAL event length"))
                        (logical-read-octets cursor length))
          do (incf change-bytes length)
             (let ((*logical-validation-only* (and raw-events-p t)))
               (push (logical-decode-change event) changes))
             (when raw-events-p (push event events)))
    (unless (= (logical-cursor-offset cursor) (length payload))
      (error "logical WAL chunk has trailing bytes"))
    (values index count (nreverse changes) change-bytes (nreverse events))))

(defun wal-recovery-reset-logical-change-batch ()
  (when (and (logical-fragment-batch-p *wal-recovery-logical-xid*)
             (logical-fragment-batch-validate-only *wal-recovery-logical-xid*)
             (logical-fragment-batch-assembler *wal-recovery-logical-xid*))
    (logical-close-recovery-assembler
     (logical-fragment-batch-assembler *wal-recovery-logical-xid*)))
  (setf *wal-recovery-logical-xid* nil
        *wal-recovery-logical-chunk-count* nil
        *wal-recovery-logical-next-chunk* 0
        *wal-recovery-logical-bytes* 0)
  (values))

(defun wal-advance-logical-change-chunk
    (record expected-xid expected-count expected-index change-bytes &optional raw-events-p)
  "Validate one chunk and return its updated transaction progress."
  (multiple-value-bind (index count changes event-bytes events)
      (wal-decode-logical-change-chunk (wal-record-payload record) raw-events-p)
    (let ((xid (wal-record-xid record)))
      (unless (plusp xid)
        (error "logical row-change record has no transaction ID"))
      (unless expected-count
        (unless (and (null expected-xid)
                     (zerop expected-index)
                     (zerop change-bytes)
                     (zerop index))
          (error "logical WAL chunks do not start at index zero"))
        (setf expected-xid xid
              expected-count count))
      (unless (and (= xid expected-xid)
                   (= count expected-count)
                   (= index expected-index))
        (error "logical WAL chunks are out of order or change transaction"))
      (let ((next-index (1+ expected-index))
            (next-bytes (+ change-bytes event-bytes)))
        (values expected-xid expected-count next-index next-bytes changes events)))))

(defun wal-recovery-accept-logical-change-chunk (record)
  (unless *wal-recovery-group-active*
    (error "logical row changes appear outside a WAL batch"))
  (if (logical-fragment-payload-p (wal-record-payload record))
      (let ((batch (or *wal-recovery-logical-xid*
                       (make-logical-fragment-batch +max-logical-wal-event-bytes+))))
        (unless (logical-fragment-batch-p batch)
          (error "logical WAL batch mixes fragment and legacy records"))
        ;; The staging cleanup must own BATCH before it can acquire a file.
        (setf *wal-recovery-logical-xid* batch)
        (logical-fragment-batch-accept batch (wal-record-xid record) (wal-record-payload record)
                                       :validate-only t)
        (setf *wal-recovery-logical-chunk-count* (logical-fragment-batch-count batch)
              *wal-recovery-logical-next-chunk* (logical-fragment-batch-next batch)
              *wal-recovery-logical-bytes*
              (logical-recovery-assembler-bytes (logical-fragment-batch-assembler batch))))
      (progn
        (when (logical-fragment-batch-p *wal-recovery-logical-xid*)
          (error "logical WAL batch mixes fragment and legacy records"))
        (multiple-value-bind (xid count next-index change-bytes)
            (wal-advance-logical-change-chunk
             record *wal-recovery-logical-xid* *wal-recovery-logical-chunk-count*
             *wal-recovery-logical-next-chunk* *wal-recovery-logical-bytes*)
          (setf *wal-recovery-logical-xid* xid
                *wal-recovery-logical-chunk-count* count
                *wal-recovery-logical-next-chunk* next-index
                *wal-recovery-logical-bytes* change-bytes))))
  (values))

(defun wal-recovery-finish-logical-change-batch (xid)
  (when *wal-recovery-logical-chunk-count*
    (if (logical-fragment-batch-p *wal-recovery-logical-xid*)
        (logical-fragment-batch-finish *wal-recovery-logical-xid* xid)
        (unless (and (= xid *wal-recovery-logical-xid*)
                     (= *wal-recovery-logical-next-chunk*
                        *wal-recovery-logical-chunk-count*))
          (error "logical WAL chunks do not match their commit record"))))
  (wal-recovery-reset-logical-change-batch))

(defun wal-logical-reader-reset-batch (state batch-open-p)
  (let ((batch (logical-reader-state-fragment-batch state)))
    (when (and batch (logical-fragment-batch-assembler batch))
      (logical-close-recovery-assembler (logical-fragment-batch-assembler batch))))
  (when (logical-reader-state-spool state)
    (close-logical-spool (logical-reader-state-spool state)))
  (setf (logical-reader-state-spool state) nil
        (logical-reader-state-catalog-p state) nil
        (logical-reader-state-row-p state) nil
        (logical-reader-state-batch-open-p state) batch-open-p
        (logical-reader-state-fragment-batch state) nil
        (logical-reader-state-xid state) nil
        (logical-reader-state-chunk-count state) nil
        (logical-reader-state-next-chunk state) 0
        (logical-reader-state-change-bytes state) 0
        (logical-reader-state-changes state) '())
  (values))

(defun wal-logical-reader-start-batch (state)
  ;; Recovery treats an unterminated group as a crash tail and starts staging
  ;; anew at the next batch marker. Match that rule so later commits remain
  ;; readable after an interrupted earlier commit.
  (wal-logical-reader-reset-batch state t))

(defun wal-logical-reader-add-chunk (state record)
  (unless (logical-reader-state-batch-open-p state)
    (error "logical WAL changes appear outside a batch"))
  (when (logical-reader-state-catalog-p state)
    (error "row changes follow a logical catalog event in the same WAL batch"))
  (if (logical-fragment-payload-p (wal-record-payload record))
      (progn
        (when (and (logical-reader-state-chunk-count state)
                   (null (logical-reader-state-fragment-batch state)))
          (error "logical WAL batch mixes fragment and legacy records"))
        (let* ((batch (or (logical-reader-state-fragment-batch state)
                          (make-logical-fragment-batch +max-logical-wal-event-bytes+)))
               (change (progn
                         ;; Publish ownership before acceptance can acquire a file.
                         (setf (logical-reader-state-fragment-batch state) batch)
                         (logical-fragment-batch-accept batch (wal-record-xid record)
                                                        (wal-record-payload record)
                                                        :on-complete
                                                        (when *logical-reader-spool-scope*
                                                          (lambda (cursor crc)
                                                            (logical-reader-store-fragment-cursor state cursor crc)))))))
          (setf (logical-reader-state-fragment-batch state) batch
                (logical-reader-state-xid state) (logical-fragment-batch-xid batch)
                (logical-reader-state-chunk-count state) (logical-fragment-batch-count batch)
                (logical-reader-state-next-chunk state) (logical-fragment-batch-next batch)
                (logical-reader-state-change-bytes state)
                (logical-recovery-assembler-bytes (logical-fragment-batch-assembler batch)))
          (when change
            (setf (logical-reader-state-row-p state) t)
            (logical-reader-store-change state change))))
      (progn
        (when (logical-reader-state-fragment-batch state)
          (error "logical WAL batch mixes fragment and legacy records"))
        (multiple-value-bind (xid count next-index change-bytes changes events)
            (let ((*logical-validation-only* nil))
              (wal-advance-logical-change-chunk
               record (logical-reader-state-xid state) (logical-reader-state-chunk-count state)
               (logical-reader-state-next-chunk state) (logical-reader-state-change-bytes state)
               (not (null *logical-reader-spool-scope*))))
          (setf (logical-reader-state-xid state) xid
                (logical-reader-state-chunk-count state) count
                (logical-reader-state-next-chunk state) next-index
                (logical-reader-state-change-bytes state) change-bytes)
          (unless (every (lambda (change)
                           (member (logical-change-kind change) '(:insert :update :delete :truncate)))
                         changes)
            (error "catalog event appears in a transactional row-change record"))
          (loop for change in changes
                for event in (if events events (make-list (length changes))) do
                  (setf (logical-reader-state-row-p state) t)
                  (if events (logical-reader-store-validated-event state change event)
                      (logical-reader-store-change state change))))))
  (values))

(defun wal-logical-reader-add-catalog-event (state record)
  (unless (logical-reader-state-batch-open-p state)
    (error "logical catalog event appears outside a WAL batch"))
  (when (or (logical-reader-state-chunk-count state)
            (logical-reader-state-catalog-p state)
            (logical-reader-state-row-p state))
    (error "logical catalog event is mixed with other changes in a WAL batch"))
  (unless (zerop (wal-record-xid record))
    (error "logical catalog event must not carry a transaction ID"))
  (let* ((bytes (wal-record-payload record))
         (change (logical-decode-catalog-event bytes)))
    (setf (logical-reader-state-catalog-p state) t)
    (when (= (logical-change-database-oid change)
             (logical-reader-state-database-oid state))
      (let ((next-bytes (+ (logical-reader-state-change-bytes state)
                           (length bytes))))
        (when (> next-bytes +max-logical-wal-transaction-bytes+)
          (error "logical catalog event exceeds the 65 MiB transaction limit"))
        (setf (logical-reader-state-change-bytes state) next-bytes)
        (logical-reader-store-change state change))))
  (values))

(defun wal-logical-reader-finish-batch (state record)
  (unless (logical-reader-state-batch-open-p state)
    (error "logical WAL terminal appears outside a batch"))
  (let* ((record-type (wal-record-type record))
         (catalog-terminal-p
           (member record-type (list +wal-record-catalog+
                                     +wal-record-catalog-commit+)))
         (changes
           (remove-if-not
            (lambda (change)
              (= (logical-change-database-oid change)
                 (logical-reader-state-database-oid state)))
            (logical-reader-state-changes state)))
         (spool (logical-reader-state-spool state))
         (has-changes (or changes spool))
         (catalog-event-p (or (logical-reader-state-catalog-p state)
                              (some (lambda (change) (member (logical-change-kind change)
                                                             '(:ddl :sequence-state))) changes)))
         (row-change-p (or (logical-reader-state-row-p state)
                           (some (lambda (change) (member (logical-change-kind change)
                                                          '(:insert :update :delete :truncate))) changes))))
    (when (= record-type +wal-record-running-xacts+)
      (unless *wal-recovery-reservation-batch-clean-p*
        (error "logical reader running snapshot requires an empty metadata batch"))
      (wal-running-xacts-record-info record))
    (when (= record-type +wal-record-xid-reservation+)
      (unless *wal-recovery-reservation-batch-clean-p*
        (error "logical reader reservation is not the sole record after its batch marker"))
      (wal-xid-reservation-record-bound record))
    (cond
      ((= record-type +wal-record-commit+)
       (when (logical-reader-state-fragment-batch state)
         (logical-fragment-batch-finish (logical-reader-state-fragment-batch state)
                                        (wal-record-xid record)))
       (when catalog-event-p
         (error "logical catalog events are attached to a row commit"))
       (when (logical-reader-state-chunk-count state)
         (unless (and (= (wal-record-xid record)
                         (logical-reader-state-xid state))
                      (= (logical-reader-state-next-chunk state)
                         (logical-reader-state-chunk-count state)))
           (error "logical WAL chunks do not match their commit record")))
       (when has-changes
         (when spool (logical-spool-seal spool))
         (push (make-logical-transaction :spool spool :xid (wal-record-xid record)
                                         :commit-timestamp
                                         (wal-decode-commit-timestamp (wal-record-payload record))
                                         :commit-record-lsn (wal-record-start record)
                                         :commit-lsn (wal-record-end record)
                                         :changes changes)
               (logical-reader-state-transactions state))
         (setf (logical-reader-state-spool state) nil)))
      (catalog-terminal-p
       (when (or (logical-reader-state-chunk-count state) row-change-p)
         (error "logical row changes are attached to a catalog WAL batch"))
       (when has-changes
         (when spool (logical-spool-seal spool))
         (push (make-logical-transaction :spool spool :xid 0
                                         :commit-record-lsn (wal-record-start record)
                                         :commit-lsn (wal-record-end record)
                                         :changes changes)
               (logical-reader-state-transactions state))
         (setf (logical-reader-state-spool state) nil)))
      ((or (logical-reader-state-chunk-count state)
           (logical-reader-state-catalog-p state)
           (logical-reader-state-row-p state))
       (error "logical changes are attached to an unsupported WAL terminal")))
    (wal-logical-reader-reset-batch state nil)))

(defun wal-logical-reader-has-database-changes-p (state)
  (or (logical-reader-state-spool state)
      (some (lambda (change)
              (= (logical-change-database-oid change)
                 (logical-reader-state-database-oid state)))
            (logical-reader-state-changes state))))

(defun wal-logical-reader-validate-limits
    (max-transactions max-bytes max-scan-bytes)
  (unless (and (integerp max-transactions) (plusp max-transactions))
    (ereport "22023" "logical read transaction limit must be positive"))
  (unless (and (integerp max-bytes) (plusp max-bytes))
    (ereport "22023" "logical read byte limit must be positive"))
  (unless (and (integerp max-scan-bytes) (plusp max-scan-bytes))
    (ereport "22023" "logical WAL scan byte limit must be positive"))
  (values))

(defun wal-logical-reader-return (transactions progress-lsn)
  (when (and *wal-logical-slot-owner*
             (integerp progress-lsn))
    (setf *wal-logical-slot-read-progress*
          (max (or *wal-logical-slot-read-progress* progress-lsn)
               progress-lsn)))
  (values transactions progress-lsn))

(defun wal-acknowledge-standard-replication-slot
    (name acknowledged-lsn restart-floor sent-lsn)
  "Persist exact standard feedback while retaining an aligned decoding cursor.
RESTART-FLOOR is a complete batch boundary at or below the reported flush,
tracked by the bounded sender. SENT-LSN never includes an unfinished frame."
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let ((slot (gethash name *wal-replication-slots*)))
      (unless slot
        (ereport "42704" "replication slot ~s does not exist" name))
      (unless (and *wal-logical-slot-owner*
                   (eq *wal-logical-slot-owner*
                       (wal-replication-slot-active-owner slot)))
        (ereport "55006" "replication slot is not owned by this reader"))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot has lost its WAL"))
      (unless (equal (wal-replication-slot-plugin slot) "pgoutput")
        (ereport "55000" "standard feedback requires a pgoutput slot"))
      (unless (and (integerp acknowledged-lsn)
                   (<= 0 acknowledged-lsn #xFFFFFFFFFFFFFFFF))
        (ereport "22023" "invalid standard feedback position"))
      (when (<= acknowledged-lsn (wal-replication-slot-confirmed-flush-lsn slot))
        (return-from wal-acknowledge-standard-replication-slot slot))
      (unless (and (integerp acknowledged-lsn) (integerp restart-floor)
                   (integerp sent-lsn) (integerp *wal-logical-slot-read-progress*)
                   (<= 0 acknowledged-lsn sent-lsn
                       *wal-logical-slot-read-progress* *wal-durable-lsn*)
                   (<= (wal-replication-slot-restart-lsn slot)
                       restart-floor acknowledged-lsn))
        (ereport "22023" "standard feedback is outside safe replication progress"))
      (wal-persist-replication-slot-ack-under-lock
       slot acknowledged-lsn restart-floor))))

(defun wal-logical-slot-validate-start-position (name requested-lsn)
  "Validate the requested initial or resumed stream boundary for NAME."
  (unless (and (integerp requested-lsn)
               (<= 0 requested-lsn #xFFFFFFFFFFFFFFFF))
    (ereport "22023" "invalid logical replication start LSN"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let ((slot (gethash name *wal-replication-slots*)))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
      (unless (wal-replication-slot-database-oid slot)
        (ereport "55000" "logical replication slot has no database binding"))
      (if (null (wal-replication-slot-confirmed-flush-lsn slot))
          (unless (= requested-lsn (wal-replication-slot-consistent-lsn slot))
            (ereport "22023"
                     "initial logical replication must start at the consistent LSN"))
          (let ((confirmed
                  (wal-replication-slot-confirmed-flush-lsn slot)))
            (unless (<= confirmed requested-lsn *wal-durable-lsn*)
              (ereport "22023"
                       "logical replication start LSN is outside retained durable WAL"))
            (when (and (> requested-lsn confirmed)
                       (not (wal-slot-terminal-ack-p slot requested-lsn)))
              (ereport "22023"
                       "logical replication start LSN must be a terminal WAL batch")))))
    (values)))

(defun wal-logical-slot-accept-resume-position (name requested-lsn)
  "Confirm a target's durable marker as the first status on a resumed stream."
  (unless *wal-logical-slot-owner*
    (ereport "55000" "logical replication slot is not owned by this reader"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let* ((slot (gethash name *wal-replication-slots*))
           (confirmed (and slot
                           (wal-replication-slot-confirmed-flush-lsn slot))))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (unless (eq (wal-replication-slot-active-owner slot)
                  *wal-logical-slot-owner*)
        (ereport "55006" "logical replication slot is not owned by this reader"))
      (unless confirmed
        (ereport "55000" "initial copy requires its consistent-point handshake"))
      (unless (and (integerp requested-lsn)
                   (<= confirmed requested-lsn *wal-durable-lsn*))
        (ereport "22023" "resume status is outside confirmed durable WAL"))
      (when (> requested-lsn confirmed)
        (unless (wal-slot-terminal-ack-p slot requested-lsn)
          (ereport "22023" "resume status must name a terminal WAL batch"))
        (wal-persist-replication-slot-ack-under-lock slot requested-lsn))
      (setf *wal-logical-slot-read-progress* requested-lsn)
      requested-lsn)))

(defstruct wal-retained-slot-scan
  name slot owner database plugin timeline namespace start end cursor pin)

(defun wal-retained-slot-scan-validate-under-lock (scan)
  (wal-check-healthy)
  (let ((slot (gethash (wal-retained-slot-scan-name scan) *wal-replication-slots*)))
    (unless (and (eq slot (wal-retained-slot-scan-slot scan))
                 (eq (wal-replication-slot-active-owner slot) (wal-retained-slot-scan-owner scan))
                 (eq (wal-replication-slot-wal-status slot) :reserved)
                 (eql (wal-replication-slot-database-oid slot) (wal-retained-slot-scan-database scan))
                 (equal (wal-replication-slot-plugin slot) (wal-retained-slot-scan-plugin scan))
                 (eql *wal-insert-timeline* (wal-retained-slot-scan-timeline scan))
                 (eq *wal-segment-namespace* (wal-retained-slot-scan-namespace scan))
                 (eql (gethash (wal-retained-slot-scan-pin scan) *wal-snapshot-retention-pins*)
                      (wal-retained-slot-scan-start scan)))
      (error "logical replication slot changed or lost retained WAL during scan"))))

(defun call-with-wal-retained-slot-scan (name from-lsn upto-lsn function)
  "Capture a durable slot horizon and pin its retained start for FUNCTION."
  (let ((scan nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (sb-thread:with-mutex (*wal-lock*)
               (wal-check-healthy)
               (let* ((slot (gethash name *wal-replication-slots*))
                      (owner (and slot (wal-replication-slot-active-owner slot)))
                      (start (and slot (or from-lsn (wal-replication-slot-confirmed-flush-lsn slot)
                                           (wal-replication-slot-consistent-lsn slot))))
                      (end (or upto-lsn *wal-durable-lsn*)))
                 (unless slot (ereport "42704" "replication slot does not exist"))
                 (unless (and *wal-logical-slot-owner* (eq owner *wal-logical-slot-owner*))
                   (ereport "55000" "logical replication slot is not owned by this reader"))
                 (unless (eq (wal-replication-slot-wal-status slot) :reserved)
                   (ereport "55000" "logical replication slot has lost its WAL"))
                 (unless (wal-replication-slot-confirmed-flush-lsn slot)
                   (ereport "55000" "logical replication slot initial copy is not acknowledged"))
                 (unless (and (integerp (wal-replication-slot-database-oid slot)) start)
                   (ereport "55000" "logical replication slot is not initialized"))
                 (unless (and (integerp start) (>= start (wal-replication-slot-restart-lsn slot)))
                   (ereport "22023" "logical replication cursor is outside retained WAL"))
                 (unless (and (integerp end) (<= start end *wal-durable-lsn*))
                   (ereport "22023" "logical replication read end is outside durable WAL"))
                 (let ((pin (gensym "LOGICAL-SCAN-PIN-")))
                   (setf scan (make-wal-retained-slot-scan
                               :name name :slot slot :owner owner
                               :database (wal-replication-slot-database-oid slot)
                               :plugin (copy-seq (wal-replication-slot-plugin slot))
                               :timeline *wal-insert-timeline* :namespace *wal-segment-namespace*
                               :start start :end end :cursor start :pin pin))
                   (setf (gethash pin *wal-snapshot-retention-pins*) start)))))
           (funcall function scan))
      (when scan
        (sb-sys:without-interrupts
          (sb-thread:with-mutex (*wal-lock*)
            (remhash (wal-retained-slot-scan-pin scan) *wal-snapshot-retention-pins*)))))))

(defun wal-retained-slot-scan-next (scan)
  "Copy one retained durable record under the lock, then validate it outside."
  (let ((bytes nil) (record-lsn nil))
    (sb-thread:with-mutex (*wal-lock*)
      (wal-retained-slot-scan-validate-under-lock scan)
      (let ((cursor (wal-retained-slot-scan-cursor scan))
            (end (wal-retained-slot-scan-end scan))
            (gap-remaining nil))
        (when (= cursor end) (return-from wal-retained-slot-scan-next nil))
        (when (> cursor end) (error "retained WAL scan passed its durable horizon"))
        (loop
          for segment = (floor cursor +wal-segment-size+)
          for offset = (mod cursor +wal-segment-size+)
          for path = (wal-segment-path segment (wal-retained-slot-scan-timeline scan)
                                       (wal-retained-slot-scan-namespace scan))
          do (let ((stream nil) (size nil))
               (unwind-protect
                    (progn
                      (sb-sys:without-interrupts
                        (multiple-value-setq (stream size)
                          (open-bounded-regular-file path +wal-segment-size+)))
                      (cond
                        ((> offset size) (error "retained WAL scan offset exceeds segment"))
                        ((= offset size)
                         (let ((next (* (1+ segment) +wal-segment-size+)))
                           (when (or gap-remaining (zerop offset))
                             (error "empty retained WAL segment"))
                           (unless (<= next end) (error "retained WAL scan stops before its horizon"))
                           (setf gap-remaining (- +wal-segment-size+ offset)
                                 cursor next)))
                        (t
                         (when (< (- size offset) +wal-header-size+)
                           (error "incomplete retained WAL header"))
                         (unless (file-position stream offset) (error "cannot seek retained WAL"))
                         (let ((header (make-array +wal-header-size+ :element-type '(unsigned-byte 8))))
                           (unless (= (read-sequence header stream) +wal-header-size+)
                             (error "cannot read retained WAL header"))
                           (let ((length (wal-u32 header 8)))
                             (when (and gap-remaining (<= length gap-remaining))
                               (error "invalid retained WAL segment rotation gap"))
                             (unless (and (<= +wal-header-size+ length +wal-segment-size+)
                                          (<= (+ offset length) size +wal-segment-size+)
                                          (<= (+ cursor length) end))
                               (error "invalid or incomplete retained WAL record"))
                             (setf bytes (make-array length :element-type '(unsigned-byte 8))
                                   record-lsn cursor)
                             (unless (file-position stream offset) (error "cannot seek retained WAL"))
                             (unless (= (read-sequence bytes stream) length)
                               (error "cannot read retained WAL record")))))))
                 (when stream (close stream))))
          when bytes do (return))))
    (let ((record (wal-decode-record bytes record-lsn)))
      (wal-require-ordinary-record-type (wal-record-type record))
      (setf (wal-retained-slot-scan-cursor scan) (wal-record-end record))
      record)))

(defun wal-logical-slot-read-transactions-from-scan (scan max-transactions max-bytes max-scan-bytes)
  (let ((start-lsn (wal-retained-slot-scan-start scan))
        (end-lsn (wal-retained-slot-scan-end scan)))
    (when (= start-lsn end-lsn)
      (return-from wal-logical-slot-read-transactions-from-scan
        (values '() start-lsn)))
    (let ((*wal-recovery-reservation-batch-clean-p* nil)
          (state (make-logical-reader-state
                  :database-oid (wal-retained-slot-scan-database scan)))
          (transaction-count 0)
          (change-bytes 0)
          (progress-lsn start-lsn))
      (unwind-protect
           (progn
             ;; Read from the captured durable horizon. The retention pin protects
             ;; segments while each record is copied under the WAL lock.
             (loop for record = (wal-retained-slot-scan-next scan)
                   while record do
                     (wal-note-recovery-batch-record record)
                     (let ((record-type (wal-record-type record)))
                       (cond
                         ((= record-type +wal-record-batch-start+)
                          (wal-logical-reader-start-batch state))
                         ((= record-type +wal-record-logical-changes+)
                          (wal-logical-reader-add-chunk state record))
                         ((= record-type +wal-record-logical-catalog-event+)
                          (wal-logical-reader-add-catalog-event state record))
                         (t
                          (when (wal-terminal-record-type-p record-type)
                            (let* ((has-changes
                                     (and (or (= record-type +wal-record-commit+)
                                              (= record-type +wal-record-catalog+)
                                              (= record-type +wal-record-catalog-commit+))
                                          (wal-logical-reader-has-database-changes-p
                                           state)))
                                   (batch-bytes
                                     (logical-reader-state-change-bytes state)))
                              (when (and has-changes
                                         (plusp transaction-count)
                                         (or (>= transaction-count max-transactions)
                                             (> (+ change-bytes batch-bytes) max-bytes)))
                                (return-from wal-logical-slot-read-transactions-from-scan
                                  (values
                                   (nreverse
                                    (logical-reader-state-transactions state))
                                   progress-lsn)))
                              (wal-logical-reader-finish-batch state record)
                              (setf progress-lsn (wal-record-end record))
                              (when has-changes
                                (incf transaction-count)
                                (incf change-bytes batch-bytes))
                              (when (or (>= transaction-count max-transactions)
                                        (>= change-bytes max-bytes)
                                        (>= (- progress-lsn start-lsn) max-scan-bytes))
                                (return-from wal-logical-slot-read-transactions-from-scan
                                  (values
                                   (nreverse
                                    (logical-reader-state-transactions state))
                                   progress-lsn)))))))))
             ;; An open batch at the durable scan horizon is an incomplete commit
             ;; tail. Its changes stay invisible, as in DecodeCommit/reorderbuffer.
             (values
              (nreverse (logical-reader-state-transactions state)) progress-lsn))
        (wal-logical-reader-reset-batch state nil)))))

(defun wal-logical-slot-read-transactions
    (name &key upto-lsn from-lsn
            (max-transactions +max-logical-read-transactions+)
            (max-bytes +max-logical-read-bytes+)
            (max-scan-bytes +max-logical-read-scan-bytes+))
  "Read committed transactions at a pinned, fixed durable horizon."
  (wal-logical-reader-validate-limits max-transactions max-bytes max-scan-bytes)
  (call-with-wal-retained-slot-scan
   name from-lsn upto-lsn
   (lambda (scan)
     (multiple-value-bind (transactions progress)
         (wal-logical-slot-read-transactions-from-scan scan max-transactions max-bytes max-scan-bytes)
       (sb-thread:with-mutex (*wal-lock*)
         (wal-retained-slot-scan-validate-under-lock scan)
         (wal-logical-reader-return transactions progress))))))

(defun wal-logical-slot-acknowledge (name acknowledged-lsn)
  "Acknowledge scanned WAL after all preceding row changes are durable."
  (unless *wal-logical-slot-owner*
    (ereport "55000" "logical replication slot is not owned by this reader"))
  (unless (and (integerp acknowledged-lsn)
               (integerp *wal-logical-slot-read-progress*)
               (<= acknowledged-lsn *wal-logical-slot-read-progress*))
    (ereport "22023" "logical acknowledgment is past the reader's progress LSN"))
  (wal-acknowledge-replication-slot
   name acknowledged-lsn *wal-logical-slot-owner*))
