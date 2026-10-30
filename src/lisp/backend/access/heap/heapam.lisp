;;;; heapam.lisp -- heap tuples on pages: form, deform, insert, delete-mark,
;;;; scan, lazy pruning.

(in-package #:pg)

;; pg-column is defined by the later catalog component in pg.asd.
(declaim (notinline pg-column-type))

(declaim (ftype function sql-varlena-array-encode sql-varlena-array-decode))

(defvar *datum-origin-capture* nil
  "When true, retain detached compression provenance for query projection.")

(defvar *query-row-origins* nil
  "Per-slot compression provenance for the row currently emitted.")

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
     (if (loop for offnum from 1 to (page-lp-count page)
                 thereis (multiple-value-bind (off flags len)
                             (page-get-lp page offnum)
                           (declare (ignore off len))
                           (= flags +lp-unused+)))
         +size-of-item-id+
         0)))

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

(defun heap-varlena-array-element-type (type)
  "Return the element type for the supported physical varlena arrays."
  (case type (:bytea-array :bytea) (:json-array :json) (:jsonb-array :jsonb)))

(defun column-align (col)
  (case (pg-column-type col)
    ((:int8 :float8 :time :timestamp :timestamptz :interval :timetz) 8)
    (:int2 2)
    ((:int4 :oid :numeric :varchar :text :json :jsonb :bytea :bytea-array :json-array :jsonb-array :float4 :date) 4)
    (t 1)))

(defun column-fixed-size (col)
  (case (pg-column-type col)
    (:interval 16)
    (:timetz 12)
    (:int2 2)
    ((:int4 :oid :float4 :date) 4)
    ((:int8 :float8 :time :timestamp :timestamptz) 8)
    (t nil)))

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
                (let ((al (column-align col)))
                  (when (or (heap-varlena-array-element-type (pg-column-type col))
                            (member (pg-column-type col) '(:varchar :text :json :jsonb :bytea :numeric)))
                    (setf (aref varlena-data i)
                          (if (heap-external-varlena-p v)
                              (heap-encode-external-varlena v)
                              (or (and (heap-compressed-varlena-p v) (heap-compressed-varlena-payload v))
                                  (aref varlena-data i)
                                  (case (pg-column-type col)
                                    (:bytea (check-type v (vector (unsigned-byte 8))) v)
                                    (:jsonb (sql-jsonb-to-octets v))
                                    (:numeric (sql-numeric-to-varlena-payload v))
                                    ((:bytea-array :json-array :jsonb-array)
                                     (sql-varlena-array-encode v (heap-varlena-array-element-type (pg-column-type col))))
                                    (otherwise (string->bytes v)))))))
                  (unless (and (or (heap-varlena-array-element-type (pg-column-type col))
                                   (member (pg-column-type col) '(:varchar :text :json :jsonb :bytea :numeric)))
                               (or (heap-external-varlena-p v)
                                   (and (not (heap-compressed-varlena-p v))
                                        (not (char= (pg-column-effective-storage col) #\p))
                                        (<= (length (aref varlena-data i)) 126))))
                    (setf cursor (align-up cursor al)))
                  (setf (aref offsets i) cursor)
                  (incf cursor
                        (case (pg-column-type col)
                          ((:varchar :text :json :jsonb :bytea :numeric :bytea-array :json-array :jsonb-array)
                           (let ((size (length (aref varlena-data i))))
                             (if (heap-external-varlena-p v) size
                                 (+ size (if (and (not (heap-compressed-varlena-p v))
                                                  (not (char= (pg-column-effective-storage col) #\p))
                                                  (<= size 126)) 1 4)))))
                          (otherwise (column-fixed-size col))))))))
    (when has-null (setf infomask (logior infomask +heap-hasnull+)))
    (when (some #'heap-external-varlena-p values)
      (setf infomask (logior infomask +heap-hasexternal+)))
    (when (some (lambda (c) (or (heap-varlena-array-element-type (pg-column-type c))
                                (member (pg-column-type c) '(:varchar :text :json :jsonb :bytea :numeric)))) columns)
      (setf infomask (logior infomask +heap-hasvarwidth+)))
    (values cursor offsets varlena-data infomask)))

(defun form-heap-tuple (values columns xmin xmax cid block off)
  "Build a HeapTupleHeaderData + null bitmap + attribute data byte vector.
Inline :bytea values are octet vectors copied into the returned tuple; callers
must keep them stable during this call. :varchar, :text, :json, :jsonb, :bytea and :numeric may instead be
physical external descriptors, written unaligned with HEAP_HASEXTERNAL set.
This codec does not create or fetch TOAST data."
  (let* ((natts (length columns))
         (hoff (heap-tuple-header-size values))
         (layout (multiple-value-list (heap-tuple-layout values columns)))
         (cursor (first layout))
         (offsets (second layout))
         (varlena-data (third layout))
         (infomask (fourth layout)))
    (let ((bytes (make-array cursor :element-type '(unsigned-byte 8)
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
                   (:int2
                    (page-set-u16 bytes off (logand v #xffff)))
                   (:oid
                    (check-type v (unsigned-byte 32))
                    (page-set-u32 bytes off v))
                   ((:int4 :date)
                    (page-set-u32 bytes off (logand v #xFFFFFFFF)))
                   ((:int8 :time :timestamp :timestamptz)
                    (page-set-u64 bytes off (logand v #xFFFFFFFFFFFFFFFF)))
                   ((:varchar :text :json :jsonb :bytea :numeric :bytea-array :json-array :jsonb-array)
                    (let ((data (aref varlena-data i)))
                      (if (heap-external-varlena-p v)
                          (replace bytes data :start1 off)
                          (if (and (not (heap-compressed-varlena-p v))
                                   (not (char= (pg-column-effective-storage col) #\p))
                                   (<= (length data) 126))
                              (progn
                                (setf (aref bytes off)
                                      (logior (ash (1+ (length data)) 1) 1))
                                (replace bytes data :start1 (1+ off)))
                              (progn
                                (page-set-u32 bytes off (logior (ash (+ 4 (length data)) 2)
                                                                (if (heap-compressed-varlena-p v) 2 0)))
                                (replace bytes data :start1 (+ off 4))))))))))
      bytes)))

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

(defun heap-decode-external-varlena (bytes offset)
  "Decode an on-disk descriptor at OFFSET and return it and its exclusive end.
Check the header, supported tag, and bounds; preserve field bits without
validating TOAST semantics. The result does not retain or own BYTES."
  (unless (and (integerp offset) (<= 0 offset)
               (<= (+ offset 2) (length bytes))
               (= (aref bytes offset) 1))
    (error "Invalid external varlena header"))
  (let* ((tag (aref bytes (1+ offset)))
         (width (cond ((= tag +vartag-ondisk-oid+) 4)
                      ((= tag +vartag-ondisk-oid8+) 8)
                      (t (error "Unsupported external varlena tag"))))
         (end (+ offset +external-varlena-fixed-size+ width)))
    (when (> end (length bytes))
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

(defun heap-inline-varlena-range (bytes offset)
  "Return inclusive start and exclusive end offsets into BYTES, without copying.
Accept only little-endian inline short or uncompressed four-byte headers;
reject external and compressed datums and lengths outside BYTES."
  (when (>= offset (length bytes))
    (error "Truncated varlena header"))
  (let* ((header (aref bytes offset))
         (shortp (oddp header)))
    (when (or (= header 1) (and (not shortp) (logbitp 1 header)))
      (error "Unsupported external or compressed varlena datum"))
    (unless (or shortp (<= (+ offset 4) (length bytes)))
      (error "Truncated varlena header"))
    (let* ((header-size (if shortp 1 4))
           (total (if shortp (ash header -1) (ash (page-u32 bytes offset) -2)))
           (end (+ offset total)))
      (unless (and (>= total header-size) (<= end (length bytes)))
        (error "Invalid varlena length"))
      (values (+ offset header-size) end))))

(defun heap-decode-inline-compressed (bytes offset)
  "Decode one aligned inline PGLZ datum and return its bytes and physical end."
  (unless (<= (+ offset 8) (length bytes))
    (ereport "XX001" "truncated compressed varlena"))
  (let* ((header (page-u32 bytes offset))
         (total (ash header -2))
         (end (+ offset total))
         (info (page-u32 bytes (+ offset 4)))
         (raw-size (logand info #x3fffffff)))
    (unless (and (= (logand header 3) 2) (> total 8)
                 (<= end (length bytes)) (zerop (ash info -30))
                 (> raw-size 0))
      (ereport "XX001" "invalid compressed varlena"))
    (values (pglz-decompress bytes raw-size :start (+ offset 8) :end end) end)))

(defun heap-inline-varlena-bytes (bytes offset)
  "Return a fresh uncompressed payload and the physical end of an inline datum."
  (if (= (logand (aref bytes offset) 3) 2)
      (heap-decode-inline-compressed bytes offset)
      (multiple-value-bind (start end) (heap-inline-varlena-range bytes offset)
        (values (subseq bytes start end) end))))

(defun heap-varlena-physical-end-and-raw-size (bytes offset)
  "Return an inline or external datum's physical end and uncompressed datum size."
  (cond ((= (aref bytes offset) 1)
         (multiple-value-bind (pointer end) (heap-decode-external-varlena bytes offset)
           (values end (heap-external-varlena-raw-size pointer))))
        ((= (logand (aref bytes offset) 3) 2)
         (unless (<= (+ offset 8) (length bytes)) (ereport "XX001" "truncated compressed varlena"))
         (let* ((total (ash (page-u32 bytes offset) -2))
                (info (page-u32 bytes (+ offset 4)))
                (end (+ offset total)))
           (unless (and (> total 8) (<= end (length bytes)) (zerop (ash info -30))
                        (> (logand info #x3fffffff) 0))
             (ereport "XX001" "invalid compressed varlena"))
           (values end (+ 4 (logand info #x3fffffff)))))
        (t (multiple-value-bind (start end) (heap-inline-varlena-range bytes offset)
             (values end (+ 4 (- end start)))))))

(defun deform-heap-tuple (bytes columns &key demand max-varlena-size wide-value)
  "Return values, XIDs, command ID, infomask, and successor TID.
Inline compressed attributes are decompressed, and inline :bytea values are
fresh octet vectors independent of BYTES. External :varchar, :text, :json,
:jsonb, :bytea and :numeric attributes remain physical descriptors; TOAST fetching and
external decompression are deferred."
  (let* ((natts (length columns))
         (xmin (page-u32 bytes +heap-tuple-xmin-off+))
         (xmax (page-u32 bytes +heap-tuple-xmax-off+))
         (cid (page-u32 bytes +heap-tuple-cid-off+))
         (ctid-block (page-u32 bytes +heap-tuple-ctid-block-off+))
         (ctid-offnum (page-u16 bytes +heap-tuple-ctid-offnum-off+))
         (infomask (page-u16 bytes +heap-tuple-infomask-off+))
         (hoff (aref bytes +heap-tuple-hoff-off+))
         (values (make-array natts :initial-element +null+))
         (physical-compressed nil)
         (cursor hoff))
    (loop for i from 0 below natts
          for col = (aref columns i)
          do (let ((is-null
                     (and (logtest infomask +heap-hasnull+)
                          (logbitp (mod i 8)
                                   (aref bytes (+ +size-of-heap-tuple-header+
                                                  (floor i 8)))))))
               (unless is-null
                 (let ((al (column-align col)))
                   (unless (and (or (heap-varlena-array-element-type (pg-column-type col))
                                    (member (pg-column-type col) '(:varchar :text :json :jsonb :bytea :numeric)))
                                (not (zerop (aref bytes cursor))))
                     (setf cursor (align-up cursor al)))
                   (case (pg-column-type col)
                     (:timetz
                      (let ((micros (page-u64 bytes cursor)) (zone (page-u32 bytes (+ cursor 8))))
                        (when (logbitp 63 micros) (decf micros #x10000000000000000))
                        (when (logbitp 31 zone) (decf zone #x100000000))
                        (setf (aref values i) (make-sql-timetz micros zone)))
                      (incf cursor 12))
                     (:interval
                      (let ((micros (page-u64 bytes cursor)) (days (page-u32 bytes (+ cursor 8)))
                                                             (months (page-u32 bytes (+ cursor 12))))
                        (when (logbitp 63 micros) (decf micros #x10000000000000000))
                        (when (logbitp 31 days) (decf days #x100000000))
                        (when (logbitp 31 months) (decf months #x100000000))
                        (setf (aref values i) (make-sql-interval months days micros)))
                      (incf cursor 16))
                     (:float4
                      (setf (aref values i) (sql-float-from-bits (page-u32 bytes cursor) :float4))
                      (incf cursor 4))
                     (:float8
                      (setf (aref values i) (sql-float-from-bits (page-u64 bytes cursor) :float8))
                      (incf cursor 8))
                     (:int2
                      (let ((v (page-u16 bytes cursor)))
                        (when (>= v #x8000) (decf v #x10000))
                        (setf (aref values i) v))
                      (incf cursor 2))
                     (:oid
                      (setf (aref values i) (page-u32 bytes cursor))
                      (incf cursor 4))
                     ((:int4 :date)
                      (let ((v (page-u32 bytes cursor)))
                        (when (>= v #x80000000)
                          (decf v #x100000000))
                        (setf (aref values i) v))
                      (incf cursor 4))
                     ((:int8 :time :timestamp :timestamptz)
                      (let ((v (page-u64 bytes cursor)))
                        (when (>= v #x8000000000000000)
                          (decf v #x10000000000000000))
                        (setf (aref values i) v))
                      (incf cursor 8))
                     ((:varchar :text :json :jsonb :bytea :numeric :bytea-array :json-array :jsonb-array)
                      (multiple-value-bind (physical-end raw-size)
                          (heap-varlena-physical-end-and-raw-size bytes cursor)
                        (cond ((and demand (/= i demand)) (setf cursor physical-end))
                              ((and max-varlena-size (> raw-size max-varlena-size))
                               (setf (aref values i) wide-value cursor physical-end))
                              (t (if (= (aref bytes cursor) 1)
                                     (multiple-value-bind (pointer end)
                                         (heap-decode-external-varlena bytes cursor)
                                       (setf (aref values i) pointer cursor end))
                                     (multiple-value-bind (payload end)
                                         (heap-inline-varlena-bytes bytes cursor)
                                       (when (= (logand (aref bytes cursor) 3) 2)
                                         (unless physical-compressed
                                           (setf physical-compressed (make-array natts :initial-element nil)))
                                         (setf (aref physical-compressed i)
                                               (make-heap-compressed-varlena :payload (subseq bytes (+ cursor 4) end))))
                                       (setf (aref values i) (case (pg-column-type col)
                                                               (:bytea payload)
                                                               (:jsonb (sql-jsonb-from-octets payload))
                                                               (:numeric (sql-numeric-from-varlena-payload payload))
                                                               ((:bytea-array :json-array :jsonb-array)
                                                                (sql-varlena-array-decode payload (heap-varlena-array-element-type (pg-column-type col))))
                                                               (otherwise (bytes->string payload)))
                                             cursor end))))))))))))
    (values values xmin xmax cid infomask ctid-block ctid-offnum physical-compressed)))

(defun heapam-get-tuple (relfilenode block offnum columns &optional toast-visibility-p)
  "Fetch and deform the live tuple at BLOCK and OFFNUM, or return NIL if absent.
When TOAST-VISIBILITY-P is true, apply the parent-driven TOAST insertion
visibility predicate to the tuple header before decoding its attributes; return
NIL if it fails. This is not an MVCC snapshot check: the caller must establish
visibility and protect the lifetime of the owning parent separately. With the
option false, no visibility check is performed."
  (let ((page (buf-read relfilenode block)))
    (when page
      (with-live-page-item (page offnum off len)
        (when (and toast-visibility-p
                   (not (heap-toast-tuple-visible-p
                         (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmin-off+)))
                         (page-u16 page (+ off +heap-tuple-infomask-off+)))))
          (return-from heapam-get-tuple nil))
        (let ((bytes (subseq page off (+ off len))))
          (multiple-value-bind (values xmin xmax cid infomask
                                ctid-block ctid-offnum physical-compressed)
              (deform-heap-tuple bytes columns)
            (make-htup :block block :offnum offnum
                       :ctid-block ctid-block :ctid-offnum ctid-offnum
                       :xmin (full-xid-from-32 xmin)
                       :xmax (full-xid-from-32 xmax) :cid cid
                       :infomask infomask :values values :physical-compressed physical-compressed)))))))

(defun heapam-insert (relfilenode columns values xmin cid)
  "Insert a tuple; return (values block offnum)."
  (let* ((tuple (form-heap-tuple values columns xmin 0 cid 0 0))
         (fsm (heap-fsm-for-relation relfilenode))
         (required (+ +size-of-item-id+ (maxalign (length tuple)))))
    (labels ((add-to-page (page block)
               (let ((offnum (page-add-item page tuple :allow-reuse t)))
                 (when offnum
                   (multiple-value-bind (toff tflags tlen)
                       (page-get-lp page offnum)
                     (declare (ignore tflags tlen))
                     (page-set-u32 page (+ toff +heap-tuple-ctid-block-off+)
                                   block)
                     (page-set-u16 page (+ toff +heap-tuple-ctid-offnum-off+)
                                   offnum))
                   (with-wal-page-batch
                     (buf-write relfilenode block page)
                     (visibilitymap-clear relfilenode block))
                   (mark-dirty relfilenode)
                   (heap-fsm-record-page fsm block
                                         (heap-page-insert-capacity page)))
                 offnum)))
      (loop for block = (heap-fsm-take-page fsm required)
            while block
            for page = (buf-read relfilenode block)
            when page
              do (let ((offnum (add-to-page page block)))
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
             (offnum (page-add-item page tuple :allow-reuse nil)))
        (unless offnum
          (ereport "54000" "row is too large for a heap page"))
        (multiple-value-bind (toff tflags tlen) (page-get-lp page offnum)
          (declare (ignore tflags tlen))
          (page-set-u32 page (+ toff +heap-tuple-ctid-block-off+) block)
          (page-set-u16 page (+ toff +heap-tuple-ctid-offnum-off+) offnum))
        (with-wal-page-batch
          (buf-write relfilenode block page)
          (visibilitymap-clear relfilenode block))
        (mark-dirty relfilenode)
        (heap-fsm-record-page fsm block
                              (heap-page-insert-capacity page))
        (values block offnum)))))

(defmacro with-heap-tuple-edit ((page offset relfilenode block offnum) &body body)
  "Mutate one live tuple and publish its page after a complete edit."
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
             (multiple-value-prog1
                 (progn ,@body)
               (with-wal-page-batch
                 (buf-write ,relfilenode-value ,block-value ,page-value)
                 (visibilitymap-clear ,relfilenode-value ,block-value))
               (mark-dirty ,relfilenode-value))))))))

(defun heapam-set-xmax (relfilenode block offnum xmax cid)
  ;; Preserve an own insertion's cmin alongside cmax in the existing CID field.
  ;; The infomask distinguishes the transaction-local combo ID from an ordinary CID.
  (with-heap-tuple-edit (page offset relfilenode block offnum)
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
      t)))

(defun heapam-set-ctid-unlocked (relfilenode block offnum ctid-block ctid-offnum)
  (unless (sb-thread:holding-mutex-p *write-lock*)
    (error "heap CTID mutation requires the writer lock"))
  (with-heap-tuple-edit (page offset relfilenode block offnum)
    (page-set-u32 page (+ offset +heap-tuple-ctid-block-off+) ctid-block)
    (page-set-u16 page (+ offset +heap-tuple-ctid-offnum-off+) ctid-offnum)
    t))

(defun heapam-set-ctid (relfilenode block offnum ctid-block ctid-offnum)
  (with-write-lock
    (heapam-set-ctid-unlocked relfilenode block offnum ctid-block ctid-offnum)))

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

(defun heapam-delete-tid (relfilenode columns block offnum xid cid
                          &optional (matches-p (constantly t)) on-delete
                            outside-lock-recheck-p qualified-tuple)
  "Delete the latest qualifying version of TID, waiting for a writer.

Under READ COMMITTED, follow a committed update's ctid chain and evaluate
MATCHES-P against the refreshed row before claiming it. Under REPEATABLE READ,
return a serialization-conflict status when the row changed after the retained
snapshot. Return T, the tuple whose xmax was set, its physical location, and a
status keyword. ON-DELETE runs under the successful mutation\'s writer lock;
when supplied, its result is returned as a sixth value."
  ;; OUTSIDE-LOCK-RECHECK-P allows :QUALIFY to evaluate without the writer lock;
  ;; QUALIFIED-TUPLE identifies an already qualified version for TID revalidation.
  (register-relation-writer relfilenode xid)
  (let ((target-block block)
        (target-offnum offnum)
        (snapshot *current-snapshot*)
        (expected-xmin nil)
        (qualified-version (and qualified-tuple (heap-tuple-version-key qualified-tuple)))
        (visited (make-hash-table :test #'equal)))
    (loop
      (let ((key (cons target-block target-offnum)))
        (when (gethash key visited)
          (ereport "XX001" "heap tuple update chain contains a cycle"))
        (setf (gethash key visited) t))
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
                     (let ((updated-p
                             (or (/= (htup-ctid-block tuple) target-block)
                                 (/= (htup-ctid-offnum tuple) target-offnum))))
                       (cond
                         ((repeatable-read-conflict-p (htup-xmax tuple))
                          (list (if updated-p
                                    :serialization-update-conflict
                                    :serialization-delete-conflict)
                                (htup-xmax tuple)))
                         (updated-p
                          (if (and *backend*
                                   (eq (backend-isolation-level *backend*)
                                       :repeatable-read))
                              :skip
                              (list :follow (htup-ctid-block tuple)
                                    (htup-ctid-offnum tuple)
                                    (htup-xmax tuple))))
                         (t :skip))))
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
             (remhash (cons target-block target-offnum) visited)))
          (:serialization-update-conflict
           (return (values nil nil nil nil :serialization-update-conflict)))
          (:serialization-delete-conflict
           (return (values nil nil nil nil :serialization-delete-conflict)))
          (:wait
           (wait-for-xid (second action))
           ;; Revisit this same TID after the writer ends; it is not a chain loop.
           (remhash (cons target-block target-offnum) visited)
           ;; READ COMMITTED rechecks a row after its conflicting writer ends.
           ;; REPEATABLE READ retains its original horizon and reports a write
           ;; conflict if the waited-for transaction committed the row.
           (when (or (null *backend*)
                     (eq (backend-isolation-level *backend*) :read-committed))
             (setf snapshot (fresh-delete-snapshot snapshot))
             (when *backend*
               (setf (backend-snapshot *backend*) snapshot))))
          (:follow
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

(defun heapam-scan-page (page block columns snapshot callback)
  ;; Check visibility from the tuple header before copying or deforming attributes.
  ;; Like page_collect_tuples in src/backend/access/heap/heapam.c, only visible
  ;; tuples proceed through the scan; invisible tuple payloads are never decoded.
  (do-live-page-items (page offnum off len)
    (let ((xmin (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmin-off+))))
          (xmax (full-xid-from-32 (page-u32 page (+ off +heap-tuple-xmax-off+))))
          (cid (page-u32 page (+ off +heap-tuple-cid-off+)))
          (infomask (page-u16 page (+ off +heap-tuple-infomask-off+))))
      (when (heap-tuple-visible-p xmin xmax cid snapshot
                                  (and *backend* (backend-xid *backend*)) infomask)
        (multiple-value-bind (values raw-xmin raw-xmax raw-cid raw-infomask
                              ctid-block ctid-offnum physical-compressed)
            (deform-heap-tuple (subseq page off (+ off len)) columns)
          (declare (ignore raw-xmin raw-xmax raw-cid raw-infomask))
          (let ((tuple (make-htup :block block :offnum offnum
                                  :ctid-block ctid-block :ctid-offnum ctid-offnum
                                  :xmin xmin :xmax xmax :cid cid :infomask infomask
                                  :values values :physical-compressed physical-compressed)))
            (when (eq (funcall callback tuple) :stop)
              (return-from heapam-scan-page :stop))))))))

(defun heapam-scan (relfilenode columns snapshot callback)
  "Call CALLBACK with each visible HTUP. CALLBACK may return :stop."
  (let ((nblocks (buf-nblocks relfilenode)))
    (dotimes (block nblocks)
      (let ((page (buf-read relfilenode block)))
        (when page
          (when (eq (heapam-scan-page page block columns snapshot callback)
                    :stop)
            (return))))))
  (values))
