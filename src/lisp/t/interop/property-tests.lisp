;;;; Generated checks supplement the independent bytes and C peer tests.
(in-package #:pg-interop-tests)

(cl-spec:defgenerator generate-name-text ()
  (let ((alphabet #(#\a #\ą #\€ #\😀)))
    (coerce (loop repeat (random 100)
                  collect (aref alphabet (random (length alphabet))))
            'string)))

(cl-spec:defspec name-text (type string) (:generator generate-name-text))

;; C name.c:namein/pg_mbcliplen: longest whole-character prefix in 63 UTF8 bytes.
(cl-spec:defspec-function pg::sql-name-input
    (:args (text name-text))
  (:returns (type string))
  (:post (<= (length (pg::string->bytes result)) 63)
         (<= (length result) (length text))
         (string= result text :end2 (length result))
         (or (= (length result) (length text))
             (> (length (pg::string->bytes
                         (subseq text 0 (1+ (length result))))) 63))))

(cl-spec:defproperty name-clipping-is-idempotent ((text name-text))
  (:about pg::sql-name-input)
  (:trials (:normal 200))
  (let ((clipped (pg::sql-name-input text)))
    (equal clipped (pg::sql-name-input clipped))))

;; C proto.c uses signed int64 commit timestamps; the private WAL is little endian.
(cl-spec:defspec-function pg::wal-encode-commit-timestamp
    (:args (timestamp (range integer -9223372036854775808 9223372036854775807)))
  (:returns (type (simple-array (unsigned-byte 8) (8)))))

(cl-spec:defproperty commit-timestamps-roundtrip
    ((timestamp (range integer -9223372036854775808 9223372036854775807)))
  (:about pg::wal-encode-commit-timestamp pg::wal-decode-commit-timestamp)
  (:trials (:normal 200))
  (= timestamp
     (pg::wal-decode-commit-timestamp (pg::wal-encode-commit-timestamp timestamp))))

;; A C source may start at its confirmed floor and must retain the target marker.
(cl-spec:defproperty resume-requires-both-source-floors
    ((confirmed (range integer 0 18446744073709551615))
     (restart (range integer 0 18446744073709551615))
     (marker (range integer 0 18446744073709551615)))
  (:about pg::logical-standard-resume-slot-safe-p)
  (:trials (:normal 200))
  (let ((row (vector "pgoutput" "logical" "t" "f" "f"
                     (format nil "~X/~8,'0X" (ash confirmed -32) (ldb (byte 32 0) confirmed))
                     (format nil "~X/~8,'0X" (ash restart -32) (ldb (byte 32 0) restart)) nil "f" "f")))
    (eql (<= (max confirmed restart) marker)
         (pg::logical-standard-resume-slot-safe-p row marker))))

(cl-spec:defproperty identity-keys-exclude-include-tail
    ((rotation (range integer 0 5)) (count (range integer 1 6)))
  (:about pg::logical-standard-key-attnums)
  (:trials (:normal 200))
  (let* ((attributes (loop for index below 6 collect (1+ (mod (+ index rotation) 6))))
         (prefix (subseq attributes 0 count))
         (base (format nil "~{~d~^ ~}" prefix))
         (with-include (format nil "~{~d~^ ~}" (append prefix '(99 100))))
         (expected (sort (copy-list prefix) #'<)))
    (and (equal expected (pg::logical-standard-key-attnums base count))
         (equal expected (pg::logical-standard-key-attnums with-include count)))))

(defun check-generated-contract (name)
  "Run a Function Spec reproducibly, retaining its evidence on failure."
  (let ((data (cl-spec:result-data (cl-spec:check-function name :trials 200 :seed 42))))
    (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun check-generated-property (name)
  "Run a Property reproducibly, retaining its evidence on failure."
  (let ((data (cl-spec:result-data (cl-spec:run-property name :seed 42))))
    (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest generated-name-clipping
    (check-generated-contract 'pg::sql-name-input)
  (check-generated-property 'name-clipping-is-idempotent))

(deftest generated-commit-timestamps
    (check-generated-contract 'pg::wal-encode-commit-timestamp)
  (check-generated-property 'commit-timestamps-roundtrip))

(deftest generated-resume-floors
    (check-generated-property 'resume-requires-both-source-floors))

(deftest generated-identity-keys
    (check-generated-property 'identity-keys-exclude-include-tail))
