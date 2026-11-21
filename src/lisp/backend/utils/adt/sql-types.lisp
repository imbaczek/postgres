;;;; SQL type identities shared by analysis, catalogs, and protocol framing.

(in-package #:pg.sql-types)

(defparameter +sql-expression-types+
  '((:jsonb 3802 -1) (:json 114 -1) (:bytea 17 -1) (:bool 16 1) (:timestamptz 1184 8) (:char 18 1) (:name 19 64) (:int8 20 8)
    (:int2 21 2) (:int2vector 22 -1) (:int4 23 4) (:text 25 -1) (:varchar 1043 -1)
    (:bytea-array 1001 -1) (:json-array 199 -1) (:jsonb-array 3807 -1)
    (:text-array 1009 -1) (:int2-array 1005 -1) (:int2vector-array 1006 -1) (:int4-array 1007 -1) (:int8-array 1016 -1)
    (:numeric-array 1231 -1) (:float4-array 1021 -1) (:float8-array 1022 -1)
    (:bool-array 1000 -1) (:char-array 1002 -1) (:name-array 1003 -1)
    (:varchar-array 1015 -1) (:oid-array 1028 -1) (:date-array 1182 -1)
    (:time-array 1183 -1) (:timetz-array 1270 -1) (:timestamp-array 1115 -1)
    (:timestamptz-array 1185 -1) (:interval-array 1187 -1) (:pg-lsn-array 3221 -1)
    (:bit-array 1561 -1) (:oid 26 4) (:oid8 6437 8) (:xid 28 4) (:pg-node-tree 194 -1) (:record 2249 -1)
    (:void 2278 4) (:inet 869 -1) (:pg-lsn 3220 8) (:numeric 1700 -1) (:float4 700 4)
    (:float8 701 8) (:date 1082 4) (:time 1083 8) (:timetz 1266 12)
    (:timestamp 1114 8) (:interval 1186 16) (:bit 1560 -1)))

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
