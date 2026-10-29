;;;; PostgreSQL-generated JsonbContainer payload fixtures.
(in-package #:pg)

(defun jsonb-test-unhex (text)
  (let ((bytes (make-array (/ (length text) 2) :element-type '(unsigned-byte 8))))
    (dotimes (i (length bytes) bytes)
      (setf (aref bytes i) (parse-integer text :start (* 2 i) :end (+ (* 2 i) 2) :radix 16)))))

(rove:deftest test-jsonb-binary-postgresql-fixtures
    (dolist (fixture '(("null" "01000050000000c0")
                       ("true" "01000050000000b0")
                       ("1.23" "010000500a0000902800000000810100fc08")
                       ("\"abc\"" "0100005003000080616263")
                       ("[]" "00000040")
                       ("{}" "00000020")
                       ("[\"x\",1.23,false,null]"
                        "04000040010000800d0000100000002000000040780000002800000000810100fc08")
                       ("{\"b\":[1,2],\"aa\":{\"q\":true}}"
                        "0200002001000080020000001d0000500d000050626161000200004008000090080000102000000000800100200000000080020001000020010000800000003071")))
      (let* ((value (sql-jsonb-input (first fixture)))
             (expected (jsonb-test-unhex (second fixture)))
             (encoded (sql-jsonb-to-octets value)))
        (rove:ok (equalp expected encoded))
        (rove:ok (string= (sql-jsonb-output value)
                          (sql-jsonb-output (sql-jsonb-from-octets expected)))))))

(rove:deftest test-jsonb-concat-storage-spill-and-logical
    (dolist (fixture '(("{\"z\":0,\"a\":{\"x\":1},\"é\":null}"
                        "{\"a\":{\"y\":2.00},\"é\":\"雪\"}"
                        "{\"a\": {\"y\": 2.00}, \"z\": 0, \"é\": \"雪\"}"
                        "{\"a\": {\"x\": 1}, \"z\": 0, \"é\": null}"
                        "{\"a\": {\"y\": 2.00}, \"é\": \"雪\"}")
                       ("[null,\"雪\",1.20]" "{\"a\":false}"
                        "[null, \"雪\", 1.20, {\"a\": false}]"
                        "[null, \"雪\", 1.20]" "{\"a\": false}")
                       ("null" "1.00" "[null, 1.00]" "null" "1.00")))
      (destructuring-bind (left-text right-text expected left-expected right-expected) fixture
        (let* ((left (sql-jsonb-input left-text))
               (right (sql-jsonb-input right-text))
               (value (sql-jsonb-concat left right))
               (stored (sql-jsonb-from-octets (sql-jsonb-to-octets value)))
               (spill (with-output-to-string (out) (query-spill-write-value out value)))
               (spilled (let ((*read-eval* nil)) (query-spill-decode (read-from-string spill))))
               (event (logical-test-change :insert 7 701 :new-values (vector 1 2 value))))
          (setf (getf (third (logical-change-columns event)) :type) :jsonb
                (getf (third (logical-change-columns event)) :typmod) -1)
          (let* ((encoded (logical-encode-change event))
                 (streamed (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0))
                 (decoded (logical-decode-change encoded)))
            (logical-encode-change event
                                   (lambda (part)
                                     (if (integerp part) (vector-push-extend part streamed)
                                         (loop for byte across part do (vector-push-extend byte streamed)))))
            (rove:ok (equalp encoded streamed))
            (dolist (actual (list value stored spilled (aref (logical-change-new-values decoded) 2)))
              (rove:ok (string= expected (sql-jsonb-output actual)) expected))
            (let* ((directory (merge-pathnames
                               (format nil "pg-jsonb-spill-~d-~d/" (sb-posix:getpid) (random 1000000))
                               (uiop:temporary-directory)))
                   (*query-temp-directory* directory))
              (ensure-directories-exist directory)
              (unwind-protect
                   (with-query-temp-files
                     (with-query-temp-output (pathname out)
                       (query-spill-write-record out value)
                       (finish-output out)
                       (with-query-temp-input (in pathname)
                         (rove:ok (string= expected (sql-jsonb-output (query-spill-read-record in)))))))
                (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore)))
            (rove:ok (string= left-expected (sql-jsonb-output left)))
            (rove:ok (string= right-expected (sql-jsonb-output right))))))))

(rove:deftest test-jsonb-binary-corrupt-framing
    (dolist (hex '("" "00000000" "01000050000000d0" "0100004001000080"
                   "01000050010000b0" "0100004001000090ff"
                   "010000500100008000" "0100005001000080ff"))
      (rove:ok (handler-case
                   (progn (sql-jsonb-from-octets (jsonb-test-unhex hex)) nil)
                 (error () t)))))

(rove:deftest test-jsonb-binary-offset-stride
    (let* ((json (format nil "[~{~a~^,~}]" (loop for i below 40 collect i)))
           (value (sql-jsonb-input json))
           (bytes (sql-jsonb-to-octets value)))
      ;; These entries match jsonb_agg(generate_series(0,39)) in PostgreSQL.
      (rove:ok (= (page-u32 bytes 4) #x90000006))
      (rove:ok (= (page-u32 bytes (+ 4 (* 32 4))) #x90000108))
      (rove:ok (string= (sql-jsonb-output value)
                        (sql-jsonb-output (sql-jsonb-from-octets bytes))))
      ;; Readers accept offset entries at other indices too.
      (page-set-u32 bytes 8 #x90000010)
      (rove:ok (string= (sql-jsonb-output value)
                        (sql-jsonb-output (sql-jsonb-from-octets bytes))))))

(rove:deftest test-jsonb-binary-depth-limit
    (let ((value :json-null))
      (dotimes (i 512) (setf value (vector value)))
      (let ((bytes (sql-jsonb-to-octets (make-sql-jsonb value))))
        (rove:ok (sql-jsonb-p (sql-jsonb-from-octets bytes))))
      (setf value (vector value))
      (let ((bytes (sql-jsonb-to-octets (make-sql-jsonb value))))
        (rove:ok (handler-case (progn (sql-jsonb-from-octets bytes) nil)
                   (error () t))))))
