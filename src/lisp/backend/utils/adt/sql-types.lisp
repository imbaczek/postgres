;;;; SQL type identities shared by analysis, catalogs, and protocol framing.

(in-package #:pg.sql-types)

(defparameter +sql-expression-types+
  '((:jsonb 3802 -1) (:json 114 -1) (:bytea 17 -1) (:bool 16 1) (:timestamptz 1184 8) (:char 18 1) (:name 19 64) (:int8 20 8)
    (:int2 21 2) (:int2vector 22 -1) (:int4 23 4) (:text 25 -1) (:varchar 1043 -1)
    (:bytea-array 1001 -1 :bytea)
    (:json-array 199 -1 :json)
    (:jsonb-array 3807 -1 :jsonb)
    (:text-array 1009 -1 :text)
    (:int2-array 1005 -1 :int2)
    (:int2vector-array 1006 -1 :int2vector)
    (:int4-array 1007 -1 :int4)
    (:int8-array 1016 -1 :int8)
    (:numeric-array 1231 -1 :numeric)
    (:float4-array 1021 -1 :float4)
    (:float8-array 1022 -1 :float8)
    (:bool-array 1000 -1 :bool)
    (:char-array 1002 -1 :char)
    (:name-array 1003 -1 :name)
    (:varchar-array 1015 -1 :varchar)
    (:oid-array 1028 -1 :oid)
    (:date-array 1182 -1 :date)
    (:time-array 1183 -1 :time)
    (:timetz-array 1270 -1 :timetz)
    (:timestamp-array 1115 -1 :timestamp)
    (:timestamptz-array 1185 -1 :timestamptz)
    (:interval-array 1187 -1 :interval)
    (:pg-lsn-array 3221 -1 :pg-lsn)
    (:bit-array 1561 -1 :bit) (:oid 26 4) (:oid8 6437 8) (:xid 28 4) (:pg-node-tree 194 -1) (:record 2249 -1)
    (:void 2278 4) (:inet 869 -1) (:pg-lsn 3220 8) (:numeric 1700 -1) (:float4 700 4)
    (:float8 701 8) (:date 1082 4) (:time 1083 8) (:timetz 1266 12)
    (:timestamp 1114 8) (:interval 1186 16) (:bit 1560 -1))
  "Supported SQL types as (identifier OID length &optional element-type).")

(defparameter +sql-array-types+
  (loop for (type nil nil element-type) in +sql-expression-types+
        when element-type collect (cons element-type type)))

(defun sql-array-type (element-type)
  (cdr (assoc element-type +sql-array-types+)))

(defparameter *sql-element-types-by-array*
  (let ((elements (make-hash-table :test #'eq)))
    (dolist (pair +sql-array-types+ elements)
      (setf (gethash (cdr pair) elements) (car pair)))))

(defun sql-array-element-type (array-type)
  (values (gethash array-type *sql-element-types-by-array*)))

(defun sql-expression-type-description (type)
  (or (assoc type +sql-expression-types+)
      (ereport "0A000" "unsupported expression type ~a" type)))

(defun sql-expression-type-length (type)
  (third (sql-expression-type-description type)))

(defun type-oid (type)
  (second (sql-expression-type-description type)))

(defparameter *sql-type-identifiers-by-oid*
  (let ((identifiers (make-hash-table :test #'eql)))
    (dolist (entry +sql-expression-types+ identifiers)
      (setf (gethash (second entry) identifiers) (first entry)))))

(defun sql-type-from-oid (oid)
  "Return the supported SQL type identifier for OID, or NIL."
  (values (gethash oid *sql-type-identifiers-by-oid*)))

(defun sql-type-storage-code (type)
  "Return PostgreSQL's default typstorage code for a supported TYPE."
  (cond ((member type '(:numeric :inet)) #\m)
        ((or (sql-array-element-type type)
             (member type '(:text :varchar :bytea :json :jsonb :bit :pg-node-tree :record)))
         #\x)
        (t #\p)))

(defun sql-type-catalog-name (type)
  "Return PostgreSQL's internal typname for a supported Lisp type identifier."
  (let ((element (sql-array-element-type type)))
    (if element
        (concatenate 'string "_" (sql-type-catalog-name element))
        (substitute #\_ #\- (string-downcase (symbol-name type))))))

(defparameter *sql-type-display-names*
  (let ((names (make-hash-table :test #'eq)))
    (labels ((display-name (type)
               (let ((element (sql-array-element-type type)))
                 (if element
                     (concatenate 'string (display-name element) "[]")
                     (case type
                       (:int2 "smallint")
                       (:int4 "integer")
                       (:int8 "bigint")
                       (:time "time without time zone")
                       (:timetz "time with time zone")
                       (:timestamp "timestamp without time zone")
                       (:timestamptz "timestamp with time zone")
                       (:float4 "real")
                       (:float8 "double precision")
                       (:varchar "character varying")
                       (:bool "boolean")
                       (:char "\"char\"")
                       ;; Keep the existing diagnostic fallback for catalog XIDs.
                       (:xid "text")
                       (otherwise (sql-type-catalog-name type)))))))
      (dolist (entry +sql-expression-types+ names)
        (let ((type (first entry)))
          (setf (gethash type names) (display-name type)))))))

(defun sql-type-display-name (type)
  "Return a read-only type name without a typmod, or text for an unclassified expression."
  (values (gethash type *sql-type-display-names* "text")))

(defun sql-type-alignment-code (type)
  "Return the catalog typalign code. Arrays use at least integer alignment."
  (let ((element (sql-array-element-type type)))
    (cond
      (element (if (char= (sql-type-alignment-code element) #\d) #\d #\i))
      ((member type '(:bool :char :name)) #\c)
      ((eq type :int2) #\s)
      ((member type '(:int8 :oid8 :float8 :time :timetz :timestamp :timestamptz
                      :interval :pg-lsn :record)) #\d)
      (t #\i))))

(defun sql-numeric-typmod (precision scale)
  "Validate and pack a declared numeric precision and signed scale."
  (unless (<= 1 precision 1000)
    (ereport "22023" "NUMERIC precision ~d must be between 1 and 1000" precision))
  (unless (<= -1000 scale 1000)
    (ereport "22023" "NUMERIC scale ~d must be between -1000 and 1000" scale))
  (+ 4 (logior (ash precision 16) (logand scale #x7ff))))

(defun sql-numeric-typmod-components (typmod)
  "Return the precision and signed scale of a specified numeric typmod."
  (let ((packed (- typmod 4)))
    (values (ldb (byte 16 16) packed)
            (- (logxor (logand packed #x7ff) #x400) #x400))))

(defparameter *sql-interval-ranges*
  '((4 :year) (2 :month) (6 :year :month)
    (8 :day) (1024 :hour) (2048 :minute) (4096 :second)
    (1032 :day :hour) (3080 :day :minute) (7176 :day :second)
    (3072 :hour :minute) (7168 :hour :second) (6144 :minute :second)
    (#x7fff))
  "Packed interval masks and their optional starting and ending SQL fields.")

(defun sql-interval-range-mask (start end)
  "Return the mask for START and optional END, or NIL for an unsupported range."
  (loop for (mask first last) in *sql-interval-ranges*
        when (and (eq start first) (eq end last)) return mask))

(defun sql-interval-range-fields (mask)
  "Return the read-only range descriptor for MASK, or NIL."
  (assoc mask *sql-interval-ranges*))

(defun sql-interval-typmod (range precision)
  "Pack validated interval range and precision fields as in timestamp.h."
  (logior (ash (logand range #x7fff) 16) (logand precision #xffff)))

(defun sql-interval-typmod-range (typmod)
  (ldb (byte 15 16) typmod))

(defun sql-interval-typmod-components (typmod)
  "Return the range and precision fields of a specified interval typmod."
  (values (sql-interval-typmod-range typmod) (ldb (byte 16 0) typmod)))

(defun sql-varchar-typmod (length)
  "Encode a declared varchar character count as a PostgreSQL type modifier."
  (when (< length 1)
    (ereport "22023" "length for type varchar must be at least 1"))
  (when (> length 10485760)
    (ereport "22023" "length for type varchar cannot exceed 10485760"))
  (+ length 4))

(defun constrain-varchar-value (value typmod explicit-p)
  (let ((maximum (- typmod 4)))
    (if (and (>= maximum 0) (or explicit-p (> (length value) maximum)))
        (progn
          (when (and (not explicit-p)
                     (position-if-not (lambda (character) (char= character #\Space)) value :start maximum))
            (ereport "22001" "value too long for type character varying(~a)" maximum))
          (subseq value 0 (min maximum (length value))))
        value)))

(defun sql-varchar-cast-value (value typmod)
  "Truncate an explicit cast to the varchar character limit."
  (constrain-varchar-value value typmod t))

(defun sql-varchar-assignment-value (value typmod)
  "Apply the assignment length rule, allowing only spaces beyond the limit."
  (constrain-varchar-value value typmod nil))
