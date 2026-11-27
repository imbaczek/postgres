;;;; heapam.lisp -- heap tuples on pages: form, deform, insert, delete-mark,
;;;; scan, lazy pruning.

(in-package #:pg)

;; pg-column is defined by the later catalog component in pg.asd.
(declaim (notinline pg-column-type pg-column-missing-value))

(declaim (ftype function sql-varlena-array-encode sql-varlena-array-decode))

(defvar *datum-origin-capture* nil
  "When true, retain detached compression provenance for query projection.")

(defvar *query-row-origins* nil
  "Per-slot compression provenance for the row currently emitted.")

(defvar *query-row-presence* nil)

(defstruct datum-origin compressed external-p descriptor)

(defstruct htup
  block
  offnum
  ctid-block
  ctid-offnum
  xmin
  xmax
  cid
  infomask
  values
  physical-compressed
  origins)

;;; The PostgreSQL FSM is persisted in a separate relation fork. Keep the MVP's
;;; page-space candidates in memory and rebuild them once per relation after a
;;; restart. Buckets are lower bounds, so a candidate is never reported as
;;; having more space than it actually does.
(defconstant +heap-fsm-category-count+ 256)
(defconstant +heap-fsm-category-size+ (/ +blcksz+ +heap-fsm-category-count+))

(defstruct heap-fsm
  (buckets (make-hash-table :test #'eql))
  (page-space (make-hash-table :test #'eql))
  (nonempty-categories (make-array +heap-fsm-category-count+
                                   :initial-element 0)))

(defvar *heap-fsms* (make-hash-table :test #'eql))
(defvar *heap-fsm-lock*
  (sb-thread:make-mutex :name "pg heap FSM"))

(defun heap-fsm-category (space)
  (min (1- +heap-fsm-category-count+)
       (floor (max 0 space) +heap-fsm-category-size+)))

(defun heap-page-insert-capacity (page)
  "Return free bytes plus one line pointer when LP_UNUSED can be reused."
  (+ (page-free-space page)
     (if (page-first-unused-slot page) +size-of-item-id+ 0)))

(defun heap-fsm-forget (relfilenode)
  (sb-thread:with-mutex (*heap-fsm-lock*)
    (remhash relfilenode *heap-fsms*))
  (values))

(defun heap-fsm-record-page-unlocked (fsm block space)
  (multiple-value-bind (old-space present-p)
      (gethash block (heap-fsm-page-space fsm))
    (when present-p
      (let* ((pages (gethash old-space (heap-fsm-buckets fsm)))
             (remaining (delete block pages :test #'eql)))
        (if remaining
            (setf (gethash old-space (heap-fsm-buckets fsm)) remaining)
            (progn
              (remhash old-space (heap-fsm-buckets fsm))
              (decf (aref (heap-fsm-nonempty-categories fsm)
                          (heap-fsm-category old-space))))))
      (remhash block (heap-fsm-page-space fsm))))
  (when (plusp space)
    (multiple-value-bind (pages present-p)
        (gethash space (heap-fsm-buckets fsm))
      (unless present-p
        (incf (aref (heap-fsm-nonempty-categories fsm)
                    (heap-fsm-category space))))
      (push block pages)
      (setf (gethash space (heap-fsm-buckets fsm)) pages
            (gethash block (heap-fsm-page-space fsm)) space)))
  (values))

(defun heap-fsm-record-page (fsm block space)
  "Record BLOCK's current free space, removing its previous category first."
  (sb-thread:with-mutex (*heap-fsm-lock*)
    (heap-fsm-record-page-unlocked fsm block space)))

(defun heap-fsm-take-page (fsm space-needed)
  "Return and remove a candidate page with at least SPACE-NEEDED bytes."
  (sb-thread:with-mutex (*heap-fsm-lock*)
    (let* ((minimum-space (max 0 space-needed))
           (minimum-category (floor minimum-space +heap-fsm-category-size+)))
      (loop for category from minimum-category below +heap-fsm-category-count+
            when (plusp (aref (heap-fsm-nonempty-categories fsm) category))
              do (loop for space from (if (= category minimum-category)
                                          minimum-space
                                          (* category +heap-fsm-category-size+))
                         to (min +blcksz+
                                 (1- (* (1+ category) +heap-fsm-category-size+)))
                       for candidates = (gethash space (heap-fsm-buckets fsm))
                       when candidates
                         do (let ((block (pop candidates)))
                              (if candidates
                                  (setf (gethash space (heap-fsm-buckets fsm))
                                        candidates)
                                  (progn
                                    (remhash space (heap-fsm-buckets fsm))
                                    (decf (aref (heap-fsm-nonempty-categories fsm)
                                                category))))
                              (remhash block (heap-fsm-page-space fsm))
                              (return-from heap-fsm-take-page block)))))))

(defun heap-fsm-for-relation (relfilenode)
  (or (sb-thread:with-mutex (*heap-fsm-lock*)
        (gethash relfilenode *heap-fsms*))
      (let ((fsm (make-heap-fsm)))
        ;; Build a private candidate without holding either the FSM lock or the
        ;; global write lock across relation page reads.  Concurrent builders
        ;; install only one candidate; all other callers use that shared FSM.
        (dotimes (block (buf-nblocks relfilenode))
          (let ((page (buf-read relfilenode block)))
            (when page
              (heap-fsm-record-page-unlocked
               fsm block (heap-page-insert-capacity page)))))
        (sb-thread:with-mutex (*heap-fsm-lock*)
          (or (gethash relfilenode *heap-fsms*)
              (setf (gethash relfilenode *heap-fsms*) fsm))))))

(defmacro define-heap-array-types (&rest elements)
  "Project the supported array element types into the heap codec's layout tables."
  (let ((entries
          (loop for element in elements
                for array = (or (sql-array-type element)
                                (error "no SQL array type for ~a" element))
                collect (list element array (type-oid element)
                              (sql-expression-type-length element)
                              (ecase (sql-type-alignment-code array) (#\i 4) (#\d 8))))))
    `(progn
       (defun heap-varlena-array-element-type (type)
         (case type
           ,@(loop for (element array) in entries collect (list array element))))
       (defun heap-varlena-alignment (type)
         (case type
           ,@(loop for (nil array nil nil alignment) in entries
                   when (/= alignment 4) collect (list array alignment))
           (otherwise 4)))
       (defun sql-array-fixed-element-width (type)
         (case type
           ,@(loop for (element nil nil width) in entries
                   when (plusp width) collect (list element width))))
       (defun sql-varlena-array-element-oid (type)
         (ecase type
           ,@(loop for (element nil oid) in entries collect (list element oid)))))))

(define-heap-array-types :bytea :json :jsonb :bool :int2 :int4 :int8 :text :varchar :uuid)

(defun heap-varlena-type-p (type)
  "Recognize the variable-width types implemented by the physical heap codec."
  (or (member type '(:varchar :text :json :jsonb :bytea :numeric))
      (heap-varlena-array-element-type type)))

(defun heap-decode-varlena-payload (payload type)
  "Interpret already validated inline or detoasted bytes for a stored type."
  (case type
    (:bytea payload)
    (:jsonb (sql-jsonb-from-octets payload))
    (:numeric (sql-numeric-from-varlena-payload payload))
    ((:varchar :text :json) (bytes->string payload))
    (otherwise
     (let ((element (heap-varlena-array-element-type type)))
       (unless element (ereport "XX001" "stored value has unsupported type"))
       (sql-varlena-array-decode payload element)))))

(defmacro define-heap-column-layout (types)
  "Compile registry metadata for the heap codec's fixed-width scalar types."
  `(progn
     (defun column-align (col)
       (let ((type (pg-column-type col)))
         (case type
           ,@(loop for type in types
                   collect (list type (ecase (sql-type-alignment-code type)
                                        (#\c 1) (#\s 2) (#\i 4) (#\d 8))))
           (otherwise
            (if (heap-varlena-type-p type)
                (heap-varlena-alignment type)
                1)))))
     (defun column-fixed-size (col)
       (case (pg-column-type col)
         ,@(loop for type in types collect (list type (sql-expression-type-length type)))))))

(define-heap-column-layout
    (:bool :uuid :int2 :int4 :int8 :oid :float4 :float8 :date :time :timetz :timestamp :timestamptz :interval))

(defconstant +vartag-ondisk-oid+ 18)

(defconstant +vartag-ondisk-oid8+ 4)

(defconstant +external-varlena-fixed-size+ 14)

;; Physical on-disk TOAST descriptor (varatt_external_oid / _oid8 in
;; varatt.h), not the value it references. RAW-SIZE is the original size
;; including the four-byte header; EXTERNAL-INFO holds the saved size without
;; that header in its low 30 bits and the compression method in its high two.
;; VALUE-ID identifies the value in the TOAST table, RELATION-ID identifies the
;; table, and ID-WIDTH selects a 4- or 8-byte value ID. These are copied scalar
;; fields; the descriptor owns neither TOAST storage nor materialized data.
;; The codec preserves the fields as opaque bits and checks their widths, not
;; whether sizes, compression method, or IDs describe a valid TOAST value.

(defstruct heap-external-varlena
  raw-size external-info value-id relation-id id-width)

;; PAYLOAD is the PostgreSQL tcinfo word followed by a PGLZ stream.
(defstruct heap-compressed-varlena payload)

(defun heap-tuple-header-size (values)
  "Reserve a null bitmap only when the tuple actually contains SQL NULL."
  (maxalign (+ +size-of-heap-tuple-header+
               (if (find +null+ values :test #'eq) (ceiling (length values) 8) 0))))

(defun heap-varlena-header-size (value data column)
  "Return the header bytes to add to encoded DATA. An external descriptor carries its header."
  (cond ((heap-external-varlena-p value) 0)
        ((and (not (heap-compressed-varlena-p value))
              (not (char= (pg-column-effective-storage column) #\p))
              (<= (length data) 126))
         1)
        (t 4)))

(defun heap-tuple-layout (values columns &optional cached-varlena)
  "Compute the exact physical layout used to form a heap tuple.
Return size, attribute offsets, encoded varlena data and infomask.
CACHED-VARLENA, when supplied, must correspond to the same columns and unchanged
inline values at each cached index. Clear an entry when its inline value or type
changes; external descriptors are always encoded afresh. The cache vector is
copied, but its payloads are not: inline bytea payloads may alias VALUES and
must remain stable while the layout is used."
  (let* ((natts (length columns))
         (hoff (heap-tuple-header-size values))
         (offsets (make-array natts :initial-element nil))
         (varlena-data (if cached-varlena (copy-seq cached-varlena)
                           (make-array natts :initial-element nil)))
         (cursor hoff)
         (infomask 0)
         (has-null nil))
    (loop for i from 0 below natts
          for v = (aref values i)
          for col = (aref columns i)
          do (cond
               ((eq v +null+) (setf has-null t))
               (t
                (let* ((type (pg-column-type col))
                       (varlena-p (heap-varlena-type-p type)))
                  (when varlena-p
                    (setf (aref varlena-data i)
                          (if (heap-external-varlena-p v)
                              (heap-encode-external-varlena v)
                              (or (and (heap-compressed-varlena-p v) (heap-compressed-varlena-payload v))
                                  (aref varlena-data i)
                                  (case type
                                    (:bytea (check-type v (vector (unsigned-byte 8))) v)
                                    (:jsonb (sql-jsonb-to-octets v))
                                    (:numeric (sql-numeric-to-varlena-payload v))
                                    (otherwise
                                     (let ((element (heap-varlena-array-element-type type)))
                                       (if element (sql-varlena-array-encode v element)
                                           (string->bytes v)))))))))
                  (let* ((data (aref varlena-data i))
                         (header-size (and varlena-p (heap-varlena-header-size v data col))))
                    (when (or (null header-size) (= header-size 4))
                      (setf cursor (align-up cursor (column-align col))))
                    (setf (aref offsets i) cursor)
                    (incf cursor (if header-size (+ header-size (length data))
                                     (column-fixed-size col))))))))
    (when has-null (setf infomask (logior infomask +heap-hasnull+)))
    (when (some #'heap-external-varlena-p values)
      (setf infomask (logior infomask +heap-hasexternal+)))
    (when (some (lambda (column) (heap-varlena-type-p (pg-column-type column))) columns)
      (setf infomask (logior infomask +heap-hasvarwidth+)))
    (values cursor offsets varlena-data infomask)))

(defun form-heap-tuple (values columns xmin xmax cid block off &optional return-varlena-p)
  "Build a HeapTupleHeaderData + null bitmap + attribute data byte vector.
Inline :bytea values are octet vectors copied into the returned tuple; callers
must keep them stable during this call. :varchar, :text, :json, :jsonb, :bytea and :numeric may instead be
physical external descriptors, written unaligned with HEAP_HASEXTERNAL set.
This codec does not create or fetch TOAST data. RETURN-VARLENA-P also returns
the encoded varlena payloads for reuse within this row."
  (multiple-value-bind (cursor offsets varlena-data infomask)
      (heap-tuple-layout values columns)
    (let ((natts (length columns))
          (hoff (heap-tuple-header-size values))
          (bytes (make-array cursor :element-type '(unsigned-byte 8)
                                    :initial-element 0)))
      ;; HeapTupleHeaderData stores 32-bit TransactionIds. The allocator keeps
      ;; full XIDs and advances the epoch when these low 32 bits wrap.
      (page-set-u32 bytes +heap-tuple-xmin-off+ (xid32-of xmin))
      (page-set-u32 bytes +heap-tuple-xmax-off+ (xid32-of xmax))
      (page-set-u32 bytes +heap-tuple-cid-off+ cid)
      (page-set-u32 bytes +heap-tuple-ctid-block-off+ block)
      (page-set-u16 bytes +heap-tuple-ctid-offnum-off+ off)
      (page-set-u16 bytes +heap-tuple-natts-off+ natts)
      (page-set-u16 bytes +heap-tuple-infomask-off+ infomask)
      (setf (aref bytes +heap-tuple-hoff-off+) hoff)
      ;; Null bitmap.
      (loop for i from 0 below natts
            when (eq (aref values i) +null+)
              do (let ((byte-index (+ +size-of-heap-tuple-header+
                                      (floor i 8)))
                       (bit (mod i 8)))
                   (setf (aref bytes byte-index)
                         (logior (aref bytes byte-index) (ash 1 bit)))))
      ;; Attribute data.
      (loop for i from 0 below natts
            for v = (aref values i)
            for off = (aref offsets i)
            for col = (aref columns i)
            do (unless (eq v +null+)
                 (case (pg-column-type col)
                   (:timetz
                    (page-set-u64 bytes off (logand (sql-timetz-microseconds v) #xffffffffffffffff))
                    (page-set-u32 bytes (+ off 8) (logand (sql-timetz-zone-west v) #xffffffff)))
                   (:interval
                    (page-set-u64 bytes off (logand (sql-interval-microseconds v) #xffffffffffffffff))
                    (page-set-u32 bytes (+ off 8) (logand (sql-interval-days v) #xffffffff))
                    (page-set-u32 bytes (+ off 12) (logand (sql-interval-months v) #xffffffff)))
                   (:float4 (page-set-u32 bytes off (sql-float-bits v :float4)))
                   (:float8 (page-set-u64 bytes off (sql-float-bits v :float8)))
                   (:bool (setf (aref bytes off) (if v 1 0)))
                   (:uuid (sql-uuid-copy-octets v bytes off))
                   (:int2
                    (page-set-u16 bytes off (logand v #xffff)))
                   (:oid
                    (check-type v (unsigned-byte 32))
                    (page-set-u32 bytes off v))
                   ((:int4 :date)
                    (page-set-u32 bytes off (logand v #xFFFFFFFF)))
                   ((:int8 :time :timestamp :timestamptz)
                    (page-set-u64 bytes off (logand v #xFFFFFFFFFFFFFFFF)))
                   (otherwise
                    (when (heap-varlena-type-p (pg-column-type col))
                      (let ((data (aref varlena-data i)))
                        (ecase (heap-varlena-header-size v data col)
                          (0 (replace bytes data :start1 off))
                          (1
                           (setf (aref bytes off) (logior (ash (1+ (length data)) 1) 1))
                           (replace bytes data :start1 (1+ off)))
                          (4
                           (page-set-u32 bytes off (logior (ash (+ 4 (length data)) 2)
                                                           (if (heap-compressed-varlena-p v) 2 0)))
                           (replace bytes data :start1 (+ off 4))))))))))
      (if return-varlena-p (values bytes varlena-data) bytes))))

(defun heap-encode-external-varlena (pointer)
  "Return a fresh little-endian physical descriptor, validating field widths.
The external header and tag precede unaligned fields; no TOAST semantic
validation, allocation, or materialization is performed."
  (let* ((width (heap-external-varlena-id-width pointer))
         (raw (heap-external-varlena-raw-size pointer))
         (info (heap-external-varlena-external-info pointer))
         (id (heap-external-varlena-value-id pointer))
         (relation (heap-external-varlena-relation-id pointer)))
    (unless (and (member width '(4 8))
                 (typep raw '(unsigned-byte 32))
                 (typep info '(unsigned-byte 32))
                 (typep relation '(unsigned-byte 32))
                 (integerp id) (<= 0 id) (< id (ash 1 (* 8 width))))
      (error "Invalid external varlena fields"))
    (let ((bytes (make-array (+ +external-varlena-fixed-size+ width) :element-type '(unsigned-byte 8)
                                                                     :initial-element 0)))
      (setf (aref bytes 0) 1
            (aref bytes 1) (if (= width 4) +vartag-ondisk-oid+ +vartag-ondisk-oid8+))
      (page-set-u32 bytes 2 raw)
      (page-set-u32 bytes 6 info)
      (if (= width 4)
          (page-set-u32 bytes 10 id)
          (page-set-u64 bytes 10 id))
      (page-set-u32 bytes (+ 10 width) relation)
      bytes)))

(defun heap-decode-external-varlena (bytes offset &optional (limit (length bytes)))
  "Decode an on-disk descriptor at OFFSET and return it and its exclusive end.
Check the header, supported tag, and bounds; preserve field bits without
validating TOAST semantics. The result does not retain or own BYTES."
  (unless (and (integerp offset) (<= 0 offset)
               (<= (+ offset 2) limit)
               (= (aref bytes offset) 1))
    (error "Invalid external varlena header"))
  (let* ((tag (aref bytes (1+ offset)))
         (width (cond ((= tag +vartag-ondisk-oid+) 4)
                      ((= tag +vartag-ondisk-oid8+) 8)
                      (t (error "Unsupported external varlena tag"))))
         (end (+ offset +external-varlena-fixed-size+ width)))
    (when (> end limit)
      (error "Truncated external varlena"))
    (values (make-heap-external-varlena
             :raw-size (page-u32 bytes (+ offset 2))
             :external-info (page-u32 bytes (+ offset 6))
             :value-id (if (= width 4) (page-u32 bytes (+ offset 10))
                           (page-u64 bytes (+ offset 10)))
             :relation-id (page-u32 bytes (+ offset 10 width))
             :id-width width)
            end)))

(defun toast-assemble-chunks (chunks size chunk-size &key (offset 0) (length (- size offset))
                                                       validate-only)
  "Validate the selected TOAST chunks and return a fresh copy of the requested slice.
CHUNKS contains ordered (sequence . octet-vector) pairs covering the slice.
VALIDATE-ONLY performs the same bounds, sequence, octet and length checks,
returns NIL, and does not allocate the result vector."
  (unless (and (typep size '(integer 0 1073741823))
               (typep chunk-size '(integer 1 *))
               (typep offset '(integer 0 *))
               (typep length '(integer 0 *))
               (<= (+ offset length) size))
    (error "Invalid TOAST slice bounds"))
  (unless (and (listp chunks)
               (or (null chunks)
                   (handler-case (list-length chunks) (type-error () nil))))
    (ereport "XX001" "invalid TOAST chunk list"))
  (let* ((total (ceiling size chunk-size))
         (first (floor offset chunk-size))
         (last (if (zerop length) (1- first) (floor (1- (+ offset length)) chunk-size)))
         (expected first))
    (dolist (chunk chunks)
      (unless (consp chunk)
        (ereport "XX001" "invalid TOAST chunk"))
      (let* ((sequence (car chunk))
             (data (cdr chunk))
             (expected-size (if (= expected (1- total))
                                (- size (* expected chunk-size))
                                chunk-size)))
        (unless (and (eql sequence expected) (<= expected last)
                     (vectorp data)
                     (every (lambda (octet) (typep octet '(unsigned-byte 8))) data)
                     (= (length data) expected-size))
          (ereport "XX001" "invalid TOAST chunk ~a" sequence))
        (incf expected)))
    (unless (= expected (1+ last))
      (ereport "XX001" "missing TOAST chunk ~a" expected))
    (unless validate-only
      (let ((result (make-array length :element-type '(unsigned-byte 8))))
        (dolist (chunk chunks)
          (let* ((chunk-start (* (car chunk) chunk-size))
                 (copy-start (max offset chunk-start))
                 (copy-end (min (+ offset length) (+ chunk-start (length (cdr chunk))))))
            (replace result (cdr chunk) :start1 (- copy-start offset)
                                        :start2 (- copy-start chunk-start)
                                        :end2 (- copy-end chunk-start))))
        result))))

(defun heap-inline-varlena-range (bytes offset &optional (limit (length bytes)))
  "Return inclusive start and exclusive end offsets into BYTES, without copying.
Accept only little-endian inline short or uncompressed four-byte headers;
reject external and compressed datums and lengths outside BYTES."
  (when (>= offset limit)
    (error "Truncated varlena header"))
  (let* ((header (aref bytes offset))
         (shortp (oddp header)))
    (when (or (= header 1) (and (not shortp) (logbitp 1 header)))
      (error "Unsupported external or compressed varlena datum"))
    (unless (or shortp (<= (+ offset 4) limit))
      (error "Truncated varlena header"))
    (let* ((header-size (if shortp 1 4))
           (total (if shortp (ash header -1) (ash (page-u32 bytes offset) -2)))
           (end (+ offset total)))
      (unless (and (>= total header-size) (<= end limit))
        (error "Invalid varlena length"))
      (values (+ offset header-size) end))))

(defun heap-decode-inline-compressed (bytes offset &optional (limit (length bytes)))
  "Decode one aligned inline PGLZ datum and return its bytes and physical end."
  (unless (<= (+ offset 8) limit)
    (ereport "XX001" "truncated compressed varlena"))
  (let* ((header (page-u32 bytes offset))
         (total (ash header -2))
         (end (+ offset total))
         (info (page-u32 bytes (+ offset 4)))
         (raw-size (logand info #x3fffffff)))
    (unless (and (= (logand header 3) 2) (> total 8)
                 (<= end limit) (zerop (ash info -30))
                 (> raw-size 0))
      (ereport "XX001" "invalid compressed varlena"))
    (values (pglz-decompress bytes raw-size :start (+ offset 8) :end end) end)))

(defun heap-inline-varlena-bytes (bytes offset &optional (limit (length bytes)))
  "Return a fresh uncompressed payload and the physical end of an inline datum."
  (if (= (logand (aref bytes offset) 3) 2)
      (heap-decode-inline-compressed bytes offset limit)
      (multiple-value-bind (start end) (heap-inline-varlena-range bytes offset limit)
        (values (subseq bytes start end) end))))

(defun heap-varlena-physical-end-and-raw-size (bytes offset &optional (limit (length bytes)))
  "Return an inline or external datum's physical end and uncompressed datum size."
  (cond ((= (aref bytes offset) 1)
         (multiple-value-bind (pointer end) (heap-decode-external-varlena bytes offset limit)
           (values end (heap-external-varlena-raw-size pointer))))
        ((= (logand (aref bytes offset) 3) 2)
         (unless (<= (+ offset 8) limit) (ereport "XX001" "truncated compressed varlena"))
         (let* ((total (ash (page-u32 bytes offset) -2))
                (info (page-u32 bytes (+ offset 4)))
                (end (+ offset total)))
           (unless (and (> total 8) (<= end limit) (zerop (ash info -30))
                        (> (logand info #x3fffffff) 0))
             (ereport "XX001" "invalid compressed varlena"))
           (values end (+ 4 (logand info #x3fffffff)))))
        (t (multiple-value-bind (start end) (heap-inline-varlena-range bytes offset limit)
             (values end (+ 4 (- end start)))))))

(defmacro heap-decode-case (type cursor limit &body clauses)
  "Decode fixed-width values and advance by the width declared in the SQL type registry."
  `(case ,type
     ,@(loop for (key . forms) in clauses
             collect
             (if (eq key 'otherwise)
                 (cons key forms)
                 (let* ((types (if (listp key) key (list key)))
                        (width (sql-expression-type-length (first types))))
                   (assert (and (plusp width)
                                (every (lambda (item) (= width (sql-expression-type-length item))) types)))
                   `(,key
                     (when (> (+ ,cursor ,width) ,limit)
                       (ereport "XX001" "truncated heap attribute"))
                     ,@forms
                     (incf ,cursor ,width)))))))

(declaim (inline heap-tuple-data-offset heap-read-int4))

(defun heap-tuple-data-offset (bytes start limit natts infomask)
  (let ((cursor (+ start (aref bytes (+ start +heap-tuple-hoff-off+)))))
    (unless (<= (+ start +size-of-heap-tuple-header+) cursor limit)
      (ereport "XX001" "invalid heap tuple header length"))
    (when (and (logtest infomask +heap-hasnull+)
               (> (+ start +size-of-heap-tuple-header+ (ceiling natts 8)) cursor))
      (ereport "XX001" "truncated heap null bitmap"))
    cursor))

(defun heap-read-int4 (bytes offset)
  (let ((value (page-u32 bytes offset)))
    (if (logbitp 31 value) (- value #x100000000) value)))

(declaim (inline heap-read-int8))

(defun heap-read-int8 (bytes offset)
  (let ((bits (page-u64 bytes offset)))
    (if (logbitp 63 bits) (- bits #x10000000000000000) bits)))

(declaim (inline single-fixed-heap-tuple-offset))

(declaim (inline single-heap-attribute-cursor))

(defun single-heap-attribute-cursor (page start limit infomask)
  "Return the data cursor of a non-NULL attribute in a singleton tuple."
  (unless (<= 0 start (+ start +size-of-heap-tuple-header+) limit (length page))
    (ereport "XX001" "invalid heap tuple bounds"))
  (let ((cursor (heap-tuple-data-offset page start limit 1 infomask)))
    (unless (and (logtest infomask +heap-hasnull+)
                 (logbitp 0 (aref page (+ start +size-of-heap-tuple-header+))))
      cursor)))

(defun single-fixed-heap-tuple-offset (bytes start limit infomask width alignment)
  "Return the offset of a non-NULL fixed-width attribute in a single-column tuple."
  (let ((cursor (single-heap-attribute-cursor bytes start limit infomask)))
    (when cursor
      (let ((offset (+ start (align-up (- cursor start) alignment))))
        (unless (<= (+ offset width) limit)
          (ereport "XX001" "truncated heap attribute"))
        offset))))

(defun single-int4-heap-tuple-value (bytes start limit infomask)
  "Decode a single int4 SQL value, including NULL."
  (let ((offset (single-fixed-heap-tuple-offset bytes start limit infomask 4 4)))
    (if offset (heap-read-int4 bytes offset) +null+)))

(defun deform-heap-tuple (bytes columns &key demand max-varlena-size wide-value
                                          (start 0) (limit (length bytes)))
  "Return values, XIDs, command ID, infomask, and successor TID.
Inline compressed attributes are decompressed, and inline :bytea values are
fresh octet vectors independent of BYTES. External :varchar, :text, :json,
:jsonb, :bytea and :numeric attributes remain physical descriptors; TOAST fetching and
external decompression are deferred. START and LIMIT give the inclusive start and
exclusive end of the tuple. Use the physical attribute count to locate its
bitmap and decode the descriptor prefix. Attributes added after tuple
formation use the descriptor's missing values. Decoded values do not retain
the input page."
  (unless (<= 0 start (+ start +size-of-heap-tuple-header+) limit (length bytes))
    (ereport "XX001" "invalid heap tuple bounds"))
  (let* ((natts (length columns))
         (physical-natts (page-u16 bytes (+ start +heap-tuple-natts-off+)))
         (xmin (page-u32 bytes (+ start +heap-tuple-xmin-off+)))
         (xmax (page-u32 bytes (+ start +heap-tuple-xmax-off+)))
         (cid (page-u32 bytes (+ start +heap-tuple-cid-off+)))
         (ctid-block (page-u32 bytes (+ start +heap-tuple-ctid-block-off+)))
         (ctid-offnum (page-u16 bytes (+ start +heap-tuple-ctid-offnum-off+)))
         (infomask (page-u16 bytes (+ start +heap-tuple-infomask-off+)))
         (values (make-array natts :initial-element +null+))
         (physical-compressed nil)
         (cursor (heap-tuple-data-offset bytes start limit physical-natts infomask)))
    (loop for i from 0 below (min natts physical-natts)
          for col = (aref columns i)
          do (let ((is-null
                     (and (logtest infomask +heap-hasnull+)
                          (logbitp (mod i 8)
                                   (aref bytes (+ start +size-of-heap-tuple-header+
                                                  (floor i 8)))))))
               (unless is-null
                 (let* ((type (pg-column-type col))
                        (varlena-p (heap-varlena-type-p type)))
                   (unless (and varlena-p (< cursor limit) (not (zerop (aref bytes cursor))))
                     (setf cursor (+ start (align-up (- cursor start) (column-align col)))))
                   (heap-decode-case type cursor limit
                     (:timetz
                      (setf (aref values i)
                            (make-sql-timetz (heap-read-int8 bytes cursor)
                                             (heap-read-int4 bytes (+ cursor 8)))))
                     (:interval
                      (setf (aref values i)
                            (make-sql-interval (heap-read-int4 bytes (+ cursor 12))
                                               (heap-read-int4 bytes (+ cursor 8))
                                               (heap-read-int8 bytes cursor))))
                     (:float4
                      (setf (aref values i) (sql-float-from-bits (page-u32 bytes cursor) :float4)))
                     (:float8
                      (setf (aref values i) (sql-float-from-bits (page-u64 bytes cursor) :float8)))
                     (:bool (setf (aref values i) (not (zerop (aref bytes cursor)))))
                     (:uuid (setf (aref values i) (sql-uuid-from-octets bytes cursor)))
                     (:int2
                      (let ((v (page-u16 bytes cursor)))
                        (when (>= v #x8000) (decf v #x10000))
                        (setf (aref values i) v)))
                     (:oid
                      (setf (aref values i) (page-u32 bytes cursor)))
                     ((:int4 :date)
                      (setf (aref values i) (heap-read-int4 bytes cursor)))
                     ((:int8 :time :timestamp :timestamptz)
                      (setf (aref values i) (heap-read-int8 bytes cursor)))
                     (otherwise
                      (when varlena-p
                        (when (>= cursor limit) (ereport "XX001" "truncated heap attribute"))
                        (if (and (null demand) (null max-varlena-size)
                                 (member type '(:text :varchar :json :numeric))
                                 (/= 1 (aref bytes cursor))
                                 (/= 2 (logand 3 (aref bytes cursor))))
                            (multiple-value-bind (start end) (heap-inline-varlena-range bytes cursor limit)
                              (setf (aref values i)
                                    (if (eq type :numeric)
                                        (sql-numeric-from-varlena-payload bytes :start start :end end)
                                        (bytes->string bytes :start start :end end))
                                    cursor end))
                            (multiple-value-bind (physical-end raw-size)
                                (heap-varlena-physical-end-and-raw-size bytes cursor limit)
                              (cond ((and demand (/= i demand)) (setf cursor physical-end))
                                    ((and max-varlena-size (> raw-size max-varlena-size))
                                     (setf (aref values i) wide-value cursor physical-end))
                                    (t (if (= (aref bytes cursor) 1)
                                           (multiple-value-bind (pointer end)
                                               (heap-decode-external-varlena bytes cursor limit)
                                             (setf (aref values i) pointer cursor end))
                                           (multiple-value-bind (payload end)
                                               (heap-inline-varlena-bytes bytes cursor limit)
                                             (when (= (logand (aref bytes cursor) 3) 2)
                                               (unless physical-compressed
                                                 (setf physical-compressed (make-array natts :initial-element nil)))
                                               (setf (aref physical-compressed i)
                                                     (make-heap-compressed-varlena :payload (subseq bytes (+ cursor 4) end))))
                                             (setf (aref values i) (heap-decode-varlena-payload payload type)
                                                   cursor end))))))))))))))
    (loop for i from physical-natts below natts
          do (setf (aref values i) (pg-column-missing-value (aref columns i))))
    (values values xmin xmax cid infomask ctid-block ctid-offnum physical-compressed)))

(defun heapam-deform-page-item (page block offnum off len columns &optional toast-visibility-p)
  "Deform a live line pointer in an already acquired page."
  (when (< len +size-of-heap-tuple-header+)
    (ereport "XX001" "invalid heap tuple bounds"))
  (when (logtest +heap-xmin-invalid+ (page-u16 page (+ off +heap-tuple-infomask-off+)))
    (return-from heapam-deform-page-item nil))
  (let ((xmin (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmin-off+)))))
    ;; An abandoned ALTER TABLE may have written a tuple in a different shape.
    (unless (if toast-visibility-p
                (not (heap-toast-tuple-visible-p
                      xmin (page-u16 page (+ off +heap-tuple-infomask-off+))))
                (and (> xmin +frozen-xid+)
                     (= (clog-get xmin) +clog-aborted+)))
      (multiple-value-bind (values raw-xmin xmax cid infomask
                            ctid-block ctid-offnum physical-compressed)
          (deform-heap-tuple page columns :start off :limit (+ off len))
        (declare (ignore raw-xmin))
        (make-htup :block block :offnum offnum
                   :ctid-block ctid-block :ctid-offnum ctid-offnum
                   :xmin xmin :xmax (full-xid-from-32 xmax) :cid cid
                   :infomask infomask :values values :physical-compressed physical-compressed)))))

(defun heapam-get-tuple (relfilenode block offnum columns &optional toast-visibility-p)
  "Fetch and deform the live tuple at BLOCK and OFFNUM, or return NIL if absent.
When TOAST-VISIBILITY-P is true, apply the parent-driven TOAST insertion
visibility predicate to the tuple header before decoding its attributes; return
NIL if it fails. This is not an MVCC snapshot check: the caller must establish
visibility and protect the lifetime of the owning parent separately."
  (with-buf-read-page (page relfilenode block)
    (when page
      (with-live-page-item (page offnum off len)
        (heapam-deform-page-item page block offnum off len columns toast-visibility-p)))))

(defvar *heap-savepoint-undoing-p* nil)

(defun heap-savepoint-recording-p ()
  (and *backend* (backend-savepoints *backend*) (not *heap-savepoint-undoing-p*)))

(defmacro publish-heap-page (relfilenode block page &optional owned-p)
  "Stage a heap page and its visibility-map clear in the same WAL publication."
  (let ((node (gensym "RELFILENODE-"))
        (number (gensym "BLOCK-")))
    `(let ((,node ,relfilenode)
           (,number ,block))
       (with-wal-page-batch
         (buf-write ,node ,number ,page ,@(when owned-p '(:main :take-ownership)))
         (visibilitymap-clear ,node ,number)))))

(defun publish-heap-insert-page (relfilenode block page offsets xmin)
  "Publish a page and record one offset or a newest-first list of offsets."
  (flet ((publish ()
           (publish-heap-page relfilenode block page t)))
    (if (heap-savepoint-recording-p)
        (sb-sys:without-interrupts
          (let* ((xid (xid32-of xmin))
                 (prior (backend-heap-undo-log *backend*))
                 (journal
                   (etypecase offsets
                     (integer (cons (list :insert relfilenode block offsets xid) prior))
                     (list (nconc (loop for offnum in offsets
                                        collect (list :insert relfilenode block offnum xid))
                                  prior)))))
            (publish)
            (setf (backend-heap-undo-log *backend*) journal)))
        (publish)))
  (mark-dirty relfilenode))

(defun heap-page-add-tuple (page tuple block allow-reuse)
  "Add TUPLE and associate its header with the stable line pointer."
  (multiple-value-bind (offnum reused-p) (page-add-item page tuple :allow-reuse allow-reuse)
    (when offnum
      (let ((offset (page-get-lp page offnum)))
        (page-set-u32 page (+ offset +heap-tuple-ctid-block-off+) block)
        (page-set-u16 page (+ offset +heap-tuple-ctid-offnum-off+) offnum)))
    (values offnum reused-p)))

(defun heapam-insert (relfilenode columns values xmin cid)
  "Insert a tuple; return (values block offnum)."
  (let* ((tuple (form-heap-tuple values columns xmin 0 cid 0 0))
         (fsm (heap-fsm-for-relation relfilenode))
         (required (+ +size-of-item-id+ (maxalign (length tuple)))))
    (labels ((add-to-page (page block allow-reuse)
               (multiple-value-bind (offnum reused-p)
                   (heap-page-add-tuple page tuple block allow-reuse)
                 (when offnum
                   (let ((capacity (if reused-p (heap-page-insert-capacity page)
                                       (page-free-space page))))
                     (publish-heap-insert-page relfilenode block page offnum xmin)
                     (heap-fsm-record-page fsm block capacity)))
                 offnum)))
      (loop for block = (heap-fsm-take-page fsm required)
            while block
            for page = (buf-read relfilenode block)
            when page
              do (let ((offnum (add-to-page page block t)))
                   (if offnum
                       (return-from heapam-insert (values block offnum))
                       ;; FSM entries are advisory, like PostgreSQL's. Refresh
                       ;; one if an intervening page change made it stale.
                       (heap-fsm-record-page fsm block
                                             (heap-page-insert-capacity page)))))
      ;; Tuple TIDs remain stable for concurrent update-chain rechecks. If the
      ;; FSM has no reusable page with enough space, extend the relation.
      (let* ((block (buf-nblocks relfilenode))
             (page (page-init (make-page)))
             (offnum (add-to-page page block nil)))
        (unless offnum
          (ereport "54000" "row is too large for a heap page"))
        (values block offnum)))))

(defun heapam-insert-formed-tuples (relfilenode tuples xmin)
  "Insert a bounded list of preformed tuples and return their TIDs in input order.
The caller holds the write lock. Each page remains private until its heap and
visibility-map changes publish together. As in C heap_multi_insert, the page
copy and publication are shared across rows, while each row gets a stable TID."
  (when tuples
    (let ((fsm (heap-fsm-for-relation relfilenode))
          (tids nil))
      (flet ((fill-page (page block allow-reuse)
               (let ((offsets nil))
                 (loop while tuples
                       for tuple = (first tuples)
                       while (or allow-reuse
                                 (<= (+ +size-of-item-id+ (maxalign (length tuple)))
                                     (page-free-space page)))
                       for offnum = (heap-page-add-tuple page tuple block allow-reuse)
                       while offnum
                       do (pop tuples)
                          (push offnum offsets)
                          (push (cons block offnum) tids))
                 (let ((capacity (heap-page-insert-capacity page)))
                   (when offsets
                     (publish-heap-insert-page relfilenode block page offsets xmin))
                   (heap-fsm-record-page fsm block capacity))
                 offsets)))
        (loop while tuples
              for required = (+ +size-of-item-id+ (maxalign (length (first tuples))))
              for block = (heap-fsm-take-page fsm required)
              do (if block
                     (let ((page (buf-read relfilenode block)))
                       (when page (fill-page page block t)))
                     (let ((new-block (buf-nblocks relfilenode))
                           (page (page-init (make-page))))
                       (unless (fill-page page new-block nil)
                         (ereport "54000" "row is too large for a heap page"))))))
      (nreverse tids))))

(defun heap-savepoint-header-entry (relfilenode block offnum page offset)
  (list :header relfilenode block offnum
        (page-u32 page (+ offset +heap-tuple-xmin-off+))
        (page-u32 page (+ offset +heap-tuple-xmax-off+))
        (page-u32 page (+ offset +heap-tuple-cid-off+))
        (page-u16 page (+ offset +heap-tuple-infomask-off+))
        (page-u32 page (+ offset +heap-tuple-ctid-block-off+))
        (page-u16 page (+ offset +heap-tuple-ctid-offnum-off+))))

(defun note-heap-savepoint-header (relfilenode block offnum page offset)
  (when (heap-savepoint-recording-p)
    (push (heap-savepoint-header-entry relfilenode block offnum page offset)
          (backend-heap-undo-log *backend*))))

(defmacro with-heap-tuple-edit ((page offset relfilenode block offnum &optional owned-p)
                                &body body)
  "Mutate one live tuple and publish its page after a complete edit.
With OWNED-P, BODY must keep the private page local to the edit."
  (let ((relfilenode-value (gensym "RELFILENODE-"))
        (block-value (gensym "BLOCK-"))
        (offnum-value (gensym "OFFNUM-"))
        (page-value (gensym "PAGE-"))
        (tuple-offset (gensym "TUPLE-OFFSET-"))
        (tuple-length (gensym "TUPLE-LENGTH-")))
    `(let* ((,relfilenode-value ,relfilenode)
            (,block-value ,block)
            (,offnum-value ,offnum)
            (,page-value (buf-read ,relfilenode-value ,block-value)))
       (when ,page-value
         (with-live-page-item (,page-value ,offnum-value
                                           ,tuple-offset ,tuple-length)
           (when (< ,tuple-length +size-of-heap-tuple-header+)
             (ereport "XX001" "heap tuple header is truncated"))
           (let ((,page ,page-value)
                 (,offset ,tuple-offset))
             (note-heap-savepoint-header ,relfilenode-value ,block-value ,offnum-value ,page ,offset)
             (multiple-value-prog1
                 (progn ,@body)
               (publish-heap-page ,relfilenode-value ,block-value ,page-value ,owned-p)
               (mark-dirty ,relfilenode-value))))))))

(defun write-heap-delete-header (page offset xmax cid)
  "Set a private tuple header's xmax and command ID before page publication."
  ;; Preserve an own insertion's cmin alongside cmax in the existing CID field.
  ;; The infomask distinguishes the transaction-local combo ID from an ordinary CID.
  (let* ((mask (page-u16 page (+ offset +heap-tuple-infomask-off+)))
         (own-insert (= (page-u32 page (+ offset +heap-tuple-xmin-off+)) (xid32-of xmax)))
         (old-cid (page-u32 page (+ offset +heap-tuple-cid-off+)))
         (cmin (if (and own-insert (logtest +heap-combo-cid+ mask))
                   (combo-command-id-values xmax old-cid) old-cid))
         (stored (if own-insert (make-combo-command-id xmax cmin cid) cid)))
    (page-set-u32 page (+ offset +heap-tuple-xmax-off+) (xid32-of xmax))
    (page-set-u32 page (+ offset +heap-tuple-cid-off+) stored)
    (page-set-u16 page (+ offset +heap-tuple-infomask-off+)
                  (if own-insert (logior mask +heap-combo-cid+)
                      (logandc2 mask +heap-combo-cid+)))
    t))

(defun heapam-set-xmax (relfilenode block offnum xmax cid)
  (with-heap-tuple-edit (page offset relfilenode block offnum :take-ownership)
    (write-heap-delete-header page offset xmax cid)))

(defun heap-page-item-visible-p (page offset snapshot &optional committed-xid-cache)
  (heap-tuple-visible-p
   (full-xid-from-32 (page-u32 page (+ offset +heap-tuple-xmin-off+)))
   (full-xid-from-32 (page-u32 page (+ offset +heap-tuple-xmax-off+)))
   (page-u32 page (+ offset +heap-tuple-cid-off+))
   snapshot (and *backend* (backend-xid *backend*))
   (page-u16 page (+ offset +heap-tuple-infomask-off+)) committed-xid-cache))

(defun heapam-delete-page-candidates (relfilenode block candidates xid cid)
  "Delete qualified same-page CANDIDATES in one publication, or return NIL.
The statement retains the snapshot that protects candidate TIDs from reuse.
Its writer admission and logical capture decision must precede this call."
  (with-write-lock
    (let ((page (buf-read relfilenode block))
          (items nil)
          (committed-xid-cache (list nil)))
      (unless page (return-from heapam-delete-page-candidates nil))
      (dolist (candidate candidates)
        (let ((offnum (htup-offnum candidate)))
          (unless (with-live-page-item (page offnum offset length)
                    (unless (and (>= length +size-of-heap-tuple-header+)
                                 (<= (+ offset length) (length page)))
                      (ereport "XX001" "invalid heap tuple bounds"))
                    ;; Leave conflicts, including an aborted xmax, to the scalar
                    ;; path. No candidate is changed if any one needs rechecking.
                    (when (and (zerop (page-u32 page (+ offset +heap-tuple-xmax-off+)))
                               (not (on-conflict-row-lock-owner relfilenode block offnum xid))
                               (heap-page-item-visible-p page offset *current-snapshot*
                                                         committed-xid-cache))
                      (push (cons offnum offset) items)))
            (return-from heapam-delete-page-candidates nil))))
      (let* ((recording (heap-savepoint-recording-p))
             (journal (and recording (backend-heap-undo-log *backend*))))
        (dolist (item (nreverse items))
          (when recording
            (push (heap-savepoint-header-entry relfilenode block (car item) page (cdr item))
                  journal))
          (write-heap-delete-header page (cdr item) xid cid))
        (sb-sys:without-interrupts
          (publish-heap-page relfilenode block page t)
          (when recording (setf (backend-heap-undo-log *backend*) journal))
          (mark-dirty relfilenode)))
      t)))

(defun heapam-set-ctid-unlocked (relfilenode block offnum ctid-block ctid-offnum)
  (unless (sb-thread:holding-mutex-p *write-lock*)
    (error "heap CTID mutation requires the writer lock"))
  (with-heap-tuple-edit (page offset relfilenode block offnum :take-ownership)
    (page-set-u32 page (+ offset +heap-tuple-ctid-block-off+) ctid-block)
    (page-set-u16 page (+ offset +heap-tuple-ctid-offnum-off+) ctid-offnum)
    t))

(defun heapam-set-ctid (relfilenode block offnum ctid-block ctid-offnum)
  (with-write-lock
    (heapam-set-ctid-unlocked relfilenode block offnum ctid-block ctid-offnum)))

(defun undo-heap-savepoint-entry (entry xid)
  "Undo a tuple in place without resetting unrelated edits on the same page."
  (destructuring-bind (kind node block offnum xmin &optional xmax old-cid mask ctid-block ctid-offnum) entry
    (unless
        (with-heap-tuple-edit (page offset node block offnum :take-ownership)
          (unless (= xmin (page-u32 page (+ offset +heap-tuple-xmin-off+)))
            (ereport "XX001" "savepoint rollback lost its heap tuple version"))
          (ecase kind
            (:insert
             ;; Index entries stay valid physical pointers to a dead tuple.
             (let* ((cmin (page-u32 page (+ offset +heap-tuple-cid-off+)))
                    (stored (make-combo-command-id xid cmin cmin)))
               (page-set-u32 page (+ offset +heap-tuple-xmax-off+) (xid32-of xid))
               (page-set-u32 page (+ offset +heap-tuple-cid-off+) stored)
               (page-set-u16 page (+ offset +heap-tuple-infomask-off+)
                             (logior +heap-combo-cid+ +heap-xmin-invalid+
                                     (logandc2 (page-u16 page (+ offset +heap-tuple-infomask-off+))
                                               +heap-xmin-committed+)))))
            (:header
             (page-set-u32 page (+ offset +heap-tuple-xmax-off+) xmax)
             (page-set-u32 page (+ offset +heap-tuple-cid-off+) old-cid)
             (page-set-u16 page (+ offset +heap-tuple-infomask-off+) mask)
             (page-set-u32 page (+ offset +heap-tuple-ctid-block-off+) ctid-block)
             (page-set-u16 page (+ offset +heap-tuple-ctid-offnum-off+) ctid-offnum)))
          t)
      (ereport "XX001" "savepoint rollback lost its heap tuple"))))

(defun undo-heap-to-savepoint (checkpoint)
  "Replay the undo journal back to CHECKPOINT under the heap writer lock."
  (let ((*heap-savepoint-undoing-p* t)
        (xid (backend-xid *backend*)))
    (with-write-lock
      (loop until (eq (backend-heap-undo-log *backend*) checkpoint)
            do (let ((entry (first (backend-heap-undo-log *backend*))))
                 (unless entry (error "savepoint checkpoint is outside the heap journal"))
                 (undo-heap-savepoint-entry entry xid)
                 (pop (backend-heap-undo-log *backend*))))))
  (values))

(defun fresh-delete-snapshot (fallback)
  (if *backend*
      (make-snapshot (backend-command-id *backend*))
      fallback))

(defun repeatable-read-conflict-p (xid)
  "Whether XID committed after the current transaction snapshot was taken."
  (let ((snapshot (and *backend*
                       (eq (backend-isolation-level *backend*) :repeatable-read)
                       (backend-transaction-snapshot *backend*))))
    (and snapshot
         (or (>= xid (snapshot-xmax snapshot))
             (member xid (snapshot-xip snapshot) :test #'=)))))

(defun heap-tuple-version-key (tuple)
  ;; A retained statement snapshot prevents TID reuse and thus ABA. UPDATE changes TID;
  ;; aborted xmax changes and VACUUM freezing do not change the row version.
  (list (htup-block tuple) (htup-offnum tuple)))

(defun heap-delete-committed-action (tuple block offnum)
  "Classify a committed xmax under the writer lock before following an UPDATE."
  (let ((updated-p (or (/= (htup-ctid-block tuple) block)
                       (/= (htup-ctid-offnum tuple) offnum))))
    (cond
      ((repeatable-read-conflict-p (htup-xmax tuple))
       (list (if updated-p :serialization-update-conflict :serialization-delete-conflict)
             (htup-xmax tuple)))
      ((and updated-p (not (and *backend* (eq (backend-isolation-level *backend*) :repeatable-read))))
       (list :follow (htup-ctid-block tuple) (htup-ctid-offnum tuple) (htup-xmax tuple)))
      (t :skip))))

(defun heapam-delete-tid (relfilenode columns block offnum xid cid
                          &optional (matches-p (constantly t)) on-delete
                            outside-lock-recheck-p qualified-tuple before-claim)
  "Delete the latest qualifying version of TID, waiting for a writer.

Under READ COMMITTED, follow a committed update's ctid chain and evaluate
MATCHES-P against the refreshed row before claiming it. Under REPEATABLE READ,
return a serialization-conflict status when the row changed after the retained
snapshot. Return T, the tuple whose xmax was set, its physical location, and a
status keyword. ON-DELETE runs under the successful mutation\'s writer lock;
when supplied, its result is returned as a sixth value. BEFORE-CLAIM runs
outside that lock on the qualified version, which is rechecked before writing
xmax. It requires OUTSIDE-LOCK-RECHECK-P."
  ;; OUTSIDE-LOCK-RECHECK-P allows :QUALIFY to evaluate without the writer lock;
  ;; QUALIFIED-TUPLE identifies an already qualified version for TID revalidation.
  (when before-claim (assert outside-lock-recheck-p))
  (register-relation-writer relfilenode xid)
  (let ((target-block block)
        (target-offnum offnum)
        (snapshot *current-snapshot*)
        (expected-xmin nil)
        (qualified-version (and qualified-tuple (heap-tuple-version-key qualified-tuple)))
        (prepared-version nil)
        (visited nil))
    (loop
      (when (and visited (gethash (cons target-block target-offnum) visited))
        (ereport "XX001" "heap tuple update chain contains a cycle"))
      (let ((action
              (with-write-lock
                (let* ((tuple (heapam-get-tuple relfilenode target-block
                                                target-offnum columns))
                       (lock-owner (on-conflict-row-lock-owner
                                    relfilenode target-block target-offnum xid)))
                  (cond
                    ((null tuple) :skip)
                    (lock-owner (list :wait lock-owner))
                    ((and expected-xmin
                          (/= (htup-xmin tuple) expected-xmin))
                     :skip)
                    ((= (htup-xmax tuple) xid)
                     (list :done tuple (when on-delete (funcall on-delete))))
                    ((and (not (zerop (htup-xmax tuple)))
                          (xid-active-p (htup-xmax tuple))
                          (/= (htup-xmax tuple) xid))
                     (list :wait (htup-xmax tuple)))
                    ((and (not (zerop (htup-xmax tuple)))
                          (xid-committed-p (htup-xmax tuple)))
                     (heap-delete-committed-action tuple target-block target-offnum))
                    ((not (heap-tuple-visible-p
                           (htup-xmin tuple) (htup-xmax tuple) (htup-cid tuple)
                           snapshot
                           (and *backend* (backend-xid *backend*)) (htup-infomask tuple)))
                     :skip)
                    ((or (zerop (htup-xmax tuple))
                         (xid-aborted-p (htup-xmax tuple)))
                     (cond
                       ((and outside-lock-recheck-p
                             (not (equal qualified-version (heap-tuple-version-key tuple))))
                        (list :qualify tuple))
                       ((and before-claim (not (equal prepared-version qualified-version)))
                        (list :prepare-claim tuple))
                       ((or outside-lock-recheck-p (funcall matches-p tuple))
                        (if (heapam-set-xmax relfilenode target-block target-offnum xid cid)
                            (list :done tuple (when on-delete (funcall on-delete)))
                            :skip))
                       (t :skip)))
                    (t :skip))))))
        (case (if (consp action) (car action) action)
          (:done (return (if on-delete
                             (values t (second action) target-block target-offnum :ok (third action))
                             (values t (second action) target-block target-offnum :ok))))
          (:skip (return (values nil nil nil nil :skip)))
          (:qualify
           (let ((tuple (second action)))
             (setf qualified-version (heap-tuple-version-key tuple))
             (unless (funcall matches-p tuple)
               (return (values nil nil nil nil :skip)))
             (when before-claim
               (funcall before-claim tuple)
               (setf prepared-version qualified-version))))
          (:prepare-claim
           (funcall before-claim (second action))
           (setf prepared-version qualified-version))
          (:serialization-update-conflict
           (return (values nil nil nil nil :serialization-update-conflict)))
          (:serialization-delete-conflict
           (return (values nil nil nil nil :serialization-delete-conflict)))
          (:wait
           (wait-for-xid (second action) :recheck-on-wakeup-p t)
           ;; READ COMMITTED rechecks a row after its conflicting writer ends.
           ;; REPEATABLE READ retains its original horizon and reports a write
           ;; conflict if the waited-for transaction committed the row.
           (when (or (null *backend*)
                     (eq (backend-isolation-level *backend*) :read-committed))
             (setf snapshot (fresh-delete-snapshot snapshot))
             (when *backend*
               (setf (backend-snapshot *backend*) snapshot))))
          (:follow
           (unless visited
             (setf visited (make-hash-table :test #'equal)))
           (setf (gethash (cons target-block target-offnum) visited) t)
           (setf target-block (second action)
                 target-offnum (third action)
                 expected-xmin (fourth action)
                 snapshot (fresh-delete-snapshot snapshot))
           (when *backend*
             (setf (backend-snapshot *backend*) snapshot))))))))

(defun heapam-prune-page (relfilenode block)
  "Keep heap TIDs stable until VACUUM can establish a removal horizon."
  (declare (ignore relfilenode block))
  0)

(defun htup-visible-p (tuple snapshot)
  (heap-tuple-visible-p (htup-xmin tuple)
                        (htup-xmax tuple)
                        (htup-cid tuple)
                        snapshot
                        (and *backend* (backend-xid *backend*)) (htup-infomask tuple)))

(defun heapam-page-tid-visible-p (page offnum snapshot)
  "Check a line pointer's MVCC visibility without deforming its key."
  (when page
    (with-live-page-item (page offnum offset length)
      (unless (and (>= length +size-of-heap-tuple-header+)
                   (<= (+ offset length) (length page)))
        (ereport "XX001" "invalid heap tuple bounds"))
      (heap-page-item-visible-p page offset snapshot))))

(defun heapam-tid-visible-p (relfilenode block offnum snapshot)
  "Check MVCC visibility when the index key is sufficient to identify the row."
  (with-buf-read-page (page relfilenode block)
    (heapam-page-tid-visible-p page offnum snapshot)))

(defun heap-single-inline-varlena-matches-p (matches page start limit infomask)
  "Call MATCHES on an inline varlena payload without decoding the row.
Return true for external or compressed values so ordinary deformation qualifies them."
  (let ((cursor (single-heap-attribute-cursor page start limit infomask)))
    (when cursor
      (let ((offset (if (and (< cursor limit) (not (zerop (aref page cursor))))
                        cursor
                        (+ start (align-up (- cursor start) 4)))))
        (when (>= offset limit) (ereport "XX001" "truncated heap attribute"))
        (let ((header (aref page offset)))
          (or (= header 1) (= (logand header 3) 2)
              (multiple-value-bind (begin end) (heap-inline-varlena-range page offset limit)
                (funcall matches page begin end))))))))

(defun heapam-scan-page (page block columns snapshot callback &optional prefilter demand values-filter raw-filter)
  ;; Check visibility from the tuple header before copying or deforming attributes.
  ;; Like page_collect_tuples in src/backend/access/heap/heapam.c, only visible
  ;; tuples proceed through the scan; invisible tuple payloads are never decoded.
  ;; Reuse a positive CLOG result for tuples with the same full XID.
  ;; Never cache an in-progress result. The cache dies with this page visit,
  ;; and snapshot checks still run for every tuple.
  (let ((committed-xid-cache (list nil)))
    (do-live-page-items (page offnum off len)
      (let ((xmin (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmin-off+))))
            (xmax (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmax-off+))))
            (cid (page-u32 page (+ off +heap-tuple-cid-off+)))
            (infomask (page-u16 page (+ off +heap-tuple-infomask-off+))))
        (when (and (heap-tuple-visible-p xmin xmax cid snapshot
                                         (and *backend* (backend-xid *backend*)) infomask
                                         committed-xid-cache)
                   (or (null raw-filter)
                       (funcall raw-filter page off (+ off len) infomask))
                   (or (null prefilter)
                       (funcall prefilter (deform-heap-tuple page columns :demand demand
                                                                          :start off :limit (+ off len)))))
          ;; Deform within this line pointer's bounds while the page is held.
          ;; The resulting values can outlive the page and its buffer.
          (multiple-value-bind (values raw-xmin raw-xmax raw-cid raw-infomask
                                ctid-block ctid-offnum physical-compressed)
              (deform-heap-tuple page columns :start off :limit (+ off len))
            (declare (ignore raw-xmin raw-xmax raw-cid raw-infomask))
            (when (or (null values-filter) (funcall values-filter values))
              (let ((tuple (make-htup :block block :offnum offnum
                                      :ctid-block ctid-block :ctid-offnum ctid-offnum
                                      :xmin xmin :xmax xmax :cid cid :infomask infomask
                                      :values values :physical-compressed physical-compressed)))
                (when (eq (funcall callback tuple) :stop)
                  (return-from heapam-scan-page :stop))))))))))

(defun heapam-scan (relfilenode columns snapshot callback &key prefilter demand values-filter raw-filter)
  "Call CALLBACK with each visible HTUP. CALLBACK may return :stop.
PREFILTER receives values from a narrow deformation using zero-based DEMAND.
Only tuples passing PREFILTER are fully deformed. VALUES-FILTER tests the full
value vector before constructing an HTUP. RAW-FILTER can reject a tuple before
any deformation using its page, bounds, and infomask."
  (let ((nblocks (buf-nblocks relfilenode))
        (handle (make-buf-page-handle)))
    (dotimes (block nblocks)
      (unwind-protect
           (let ((page (buf-acquire-read-page handle relfilenode block)))
             (when (and page (eq (heapam-scan-page page block columns snapshot callback prefilter demand values-filter raw-filter)
                                 :stop))
               (return)))
        (sb-sys:without-interrupts (buf-release-page handle)))))
  (values))
