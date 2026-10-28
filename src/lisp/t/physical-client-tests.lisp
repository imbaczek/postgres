(in-package #:pg-physical-tests)

(defun call-with-physical-client-wire (function writer)
  (with-physical-store-directory (root)
    (let ((response (merge-pathnames "response.wire" root))
          (request (merge-pathnames "request.wire" root)))
      (with-open-file (out response :direction :output :element-type '(unsigned-byte 8))
        (funcall writer out))
      (with-open-file (in response :element-type '(unsigned-byte 8))
        (with-open-file (out request :direction :output :element-type '(unsigned-byte 8))
          (funcall function (pg::make-pg-client :stream (make-two-way-stream in out)) request))))))

(defun physical-client-test-result (out schema rows tag)
  (pg::send-row-description
   out (mapcar (lambda (entry)
                 (pg::make-result-column :name (first entry) :type-oid (second entry)
                                         :typlen (if (= (second entry) pg::+int8oid+) 8 -1)))
               schema))
  (dolist (row rows) (pg::send-data-row-text out row))
  (pg::send-command-complete out tag)
  (pg::send-ready-for-query out #\I))

(defun physical-client-test-prepare-schema ()
  (loop for name in '("slot_name" "system_id" "base_timeline" "base_lsn" "manifest_digest"
                      "source_timeline" "receive_timeline" "receive_lsn" "prefix_digest"
                      "overlap_start" "overlap_digest" "history" "next_tli" "next_tli_startpos" "eor_hex")
        for index from 0 collect (list name (if (member index '(2 5 6 12)) pg::+int8oid+ pg::+textoid+))))

(defun physical-client-test-prepare-row ()
  (let ((anchor (follow-anchor-request 123 456)))
    (list "bound" "42" "1" "0/00000040" "77" "2" "1" "0/00000060" "123"
          "0/00000000" "456" (getf anchor :history-text) "2" "0/00000060"
          (format nil "~(~{~2,'0X~}~)" (getf anchor :eor-octets)))))

(defun physical-client-test-protocol-error-p (function)
  (handler-case (progn (funcall function) nil)
    (pg::pg-client-error (condition) (equal "08P01" (pg::pg-client-error-sqlstate condition)))))

(defun physical-client-test-error-response (out kind &optional (status #\I))
  (let ((payload (pg::make-payload)))
    (if (eq kind :unterminated) (pg::payload-push payload #(67 53))
        (progn
          (pg::payload-push-byte payload (char-code #\C))
          (pg::payload-push-cstring payload "55000")
          (pg::payload-push-byte payload (char-code #\M))
          (pg::payload-push-cstring payload "retained pin is unavailable")
          (pg::payload-push-byte payload 0)))
    (when (eq kind :trailing) (pg::payload-push-byte payload 99))
    (pg::write-message out #\E (if (eq kind :missing-end) (subseq payload 0 (1- (length payload))) payload)))
  (pg::send-ready-for-query out status))

(deftest physical-client-source-error-drains-ready-before-reusing-connection
    (dolist (mode '(:start :prepare :finish))
      (let* ((expected (follow-anchor-request 123 456))
             (coordinates (follow-anchor-receiver-request expected)))
        (call-with-physical-client-wire
         (lambda (client request)
           (declare (ignore request))
           (flet ((command ()
                    (ecase mode
                      (:start (pg::pg-client-native-start client "START_REPLICATION probe"))
                      (:prepare (pg::pg-client-native-prepare client "bound" coordinates))
                      (:finish (pg::pg-client-native-finish client 2 96)))))
             (ok (equal "55000" (handler-case (progn (command) nil)
                                  (pg::pg-client-error (condition) (pg::pg-client-error-sqlstate condition)))))
             (ok (equal (if (eq mode :prepare) expected '(:switch 2 96))
                        (if (eq mode :finish)
                            (pg::pg-client-native-start client "START_REPLICATION retry")
                            (command)))
                 (symbol-name mode))))
         (lambda (out)
           (physical-client-test-error-response out :valid)
           (if (eq mode :prepare)
               (physical-client-test-result out (physical-client-test-prepare-schema)
                                            (list (physical-client-test-prepare-row)) "PREPARE_FOLLOW")
               (pg::physical-stream-switch-result out 2 96)))))))

(deftest physical-client-error-status-accepts-known-transaction-states-only
    (dolist (status '(#\I #\T #\E #\X))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (let ((state (handler-case (progn (pg::pg-client-native-start client "START_REPLICATION probe") nil)
                        (pg::pg-client-error (condition) (pg::pg-client-error-sqlstate condition)))))
           (ok (equal (if (char= status #\X) "08P01" "55000") state))))
       (lambda (out) (physical-client-test-error-response out :valid status)))))

(deftest physical-client-source-errors-preserve-sqlstate-and-reject-malformed-errors
    (dolist (kind '(:valid :unterminated :trailing :missing-end))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (ok (equal (if (eq kind :valid) "55000" "08P01")
                    (handler-case (progn (pg::pg-client-native-start client "START_REPLICATION probe") nil)
                      (pg::pg-client-error (condition) (pg::pg-client-error-sqlstate condition))))
             (symbol-name kind)))
       (lambda (out) (physical-client-test-error-response out kind)))))

(deftest physical-client-rejects-binary-row-description
    (call-with-physical-client-wire
     (lambda (client request)
       (declare (ignore request))
       (ok (physical-client-test-protocol-error-p
            (lambda () (pg::pg-client-native-start client "START_REPLICATION probe")))))
     (lambda (out)
       (let ((payload (pg::make-payload)))
         (pg::payload-push-u16 payload 2)
         (loop for (name oid) in (list (list "next_tli" pg::+int8oid+) (list "next_tli_startpos" pg::+textoid+))
               do (pg::payload-push-cstring payload name)
                  (pg::payload-push-u32 payload 0) (pg::payload-push-u16 payload 0)
                  (pg::payload-push-u32 payload oid)
                  (pg::payload-push-u16 payload (if (= oid pg::+int8oid+) 8 65535))
                  (pg::payload-push-u32 payload #xFFFFFFFF)
                  (pg::payload-push-u16 payload 1))
         (pg::write-message out #\T payload)))))

(defun physical-client-test-history (size)
  (let* ((prefix (getf (follow-anchor-request 123 456) :history-text))
         (text (make-string size :initial-element #\x)))
    (replace text prefix)
    (loop for start from (length prefix) below size by 512
          do (setf (char text start) #\#)
             (setf (char text (min (1- size) (+ start 511))) #\Newline))
    text))

(deftest physical-client-reads-a-maximum-size-history-row-with-framing-overhead
    (let ((text (physical-client-test-history pg::+physical-store-history-maximum-bytes+)))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (let ((bytes (pg::pg-client-native-history client 2)))
           (ok (= pg::+physical-store-history-maximum-bytes+ (length bytes)))
           (ok (equalp (pg::string->bytes text) bytes))))
       (lambda (out)
         (physical-client-test-result out (list (list "filename" pg::+textoid+) (list "content" pg::+textoid+))
                                      (list (list "00000002.history" text)) "TIMELINE_HISTORY")))))

(deftest physical-client-rejects-wrong-history-filename-and-excess-content
    (dolist (kind '(:filename :size :lineage))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (ok (physical-client-test-protocol-error-p (lambda () (pg::pg-client-native-history client 2)))
             (symbol-name kind)))
       (lambda (out)
         (physical-client-test-result
          out (list (list "filename" pg::+textoid+) (list "content" pg::+textoid+))
          (list (list (if (eq kind :filename) "00000003.history" "00000002.history")
                      (case kind
                        (:size (physical-client-test-history (1+ pg::+physical-store-history-maximum-bytes+)))
                        (:lineage (format nil "2~c0/00000060~%" #\Tab))
                        (t (getf (follow-anchor-request 123 456) :history-text)))))
          "TIMELINE_HISTORY")))))

(deftest physical-client-rejects-malformed-switch-results-as-protocol-errors
    (dolist (kind '(:name :type :width :missing :duplicate :null :tag :ready :description :cstring :data :timeline :fork))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (ok (physical-client-test-protocol-error-p
              (lambda () (pg::pg-client-native-start client "START_REPLICATION probe"))) (symbol-name kind)))
       (lambda (out)
         (let ((schema (list (list "next_tli" pg::+int8oid+) (list "next_tli_startpos" pg::+textoid+)))
               (row (list "2" "0/00000060")))
           (ecase kind
             (:name (setf (first (first schema)) "unexpected"))
             (:type (setf (second (first schema)) pg::+textoid+))
             (:width (setf schema (cdr schema)))
             ((:missing :duplicate :tag :ready :description :cstring :data) nil)
             (:null (setf (first row) nil))
             (:timeline (setf (first row) "1"))
             (:fork (setf (second row) "not-an-lsn")))
           (cond
             ((eq kind :description) (pg::write-message out #\T #(0 2 120 0)))
             ((eq kind :cstring) (pg::write-message out #\T #(0 2 120)))
             (t
              (pg::send-row-description
               out (mapcar (lambda (entry)
                             (pg::make-result-column :name (first entry) :type-oid (second entry)
                                                     :typlen (if (= (second entry) pg::+int8oid+) 8 -1))) schema))
              (unless (eq kind :missing)
                (if (eq kind :data) (pg::write-message out #\D #(0 2 0))
                    (pg::send-data-row-text out row)))
              (when (eq kind :duplicate) (pg::send-data-row-text out row))
              (pg::send-command-complete out (if (eq kind :tag) "SELECT 1" "START_STREAMING"))
              (pg::send-ready-for-query out (if (eq kind :ready) #\T #\I)))))))))

(deftest physical-client-prepare-rejects-different-coordinates-or-source-proof
    (dolist (kind '(:pin :coordinate :history :next :fork :hex :crc :oversize-history))
      (let* ((anchor (follow-anchor-request 123 456)) (coordinates (follow-anchor-receiver-request anchor)))
        (call-with-physical-client-wire
         (lambda (client request)
           (declare (ignore request))
           (ok (physical-client-test-protocol-error-p
                (lambda () (pg::pg-client-native-prepare client "bound" coordinates))) (symbol-name kind)))
         (lambda (out)
           (let ((row (physical-client-test-prepare-row)))
             (ecase kind
               (:pin (setf (first row) "other_pin"))
               (:coordinate (setf (nth 8 row) "124"))
               (:history (setf (nth 11 row) (store-history-text 1 95)))
               (:oversize-history (setf (nth 11 row) (physical-client-test-history
                                                      (1+ pg::+physical-store-history-maximum-bytes+))))
               (:next (setf (nth 12 row) "3"))
               (:fork (setf (nth 13 row) "0/00000061"))
               (:hex (setf (nth 14 row) "zz"))
               (:crc (setf (char (nth 14 row) 20) #\F)))
             (physical-client-test-result out (physical-client-test-prepare-schema) (list row) "PREPARE_FOLLOW")))))))

(deftest physical-client-finish-rejects-a-different-prepared-switch
    (dolist (expected '((3 96) (2 95)))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (ok (physical-client-test-protocol-error-p
              (lambda () (apply #'pg::pg-client-native-finish client expected)))))
       (lambda (out) (pg::physical-stream-switch-result out 2 96)))))

(deftest physical-client-frame-bound-rejects-before-consuming-oversized-body
    (call-with-physical-client-wire
     (lambda (client request)
       (declare (ignore request))
       (ok (physical-client-test-protocol-error-p (lambda () (pg::pg-client-native-frame client 4096))))
       (ok (= 99 (read-byte (pg::pg-client-stream client)))))
     (lambda (out)
       (write-byte (char-code #\D) out) (write-sequence (pg::u32-to-bytes 4097) out)
       (write-byte 99 out))))

(deftest physical-client-prepares-and-checks-source-owned-anchor
    (let* ((anchor (follow-anchor-request 123 456)) (coordinates (follow-anchor-receiver-request anchor)))
      (call-with-physical-client-wire
       (lambda (client request)
         (ok (equal anchor (pg::pg-client-native-prepare client "bound" coordinates)))
         (with-open-file (in request :element-type '(unsigned-byte 8))
           (multiple-value-bind (kind payload) (pg::pg-client-frame in)
             (ok (eql kind #\Q))
             (let ((command (pg::parse-replication-command (pg::pg-client-cstring payload 0))))
               (ok (equal coordinates (pg::physical-follow-prepare-request command)))))))
       (lambda (out)
         (physical-client-test-result out (physical-client-test-prepare-schema)
                                      (list (physical-client-test-prepare-row)) "PREPARE_FOLLOW")))))

(deftest physical-client-fetches-exact-immediate-child-history
    (let ((text (getf (follow-anchor-request 123 456) :history-text)))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (ok (equalp (pg::string->bytes text) (pg::pg-client-native-history client 2))))
       (lambda (out)
         (physical-client-test-result out (list (list "filename" pg::+textoid+) (list "content" pg::+textoid+))
                                      (list (list "00000002.history" text)) "TIMELINE_HISTORY")))))

(deftest physical-client-finishes-parent-copy-with-checked-switch
    (call-with-physical-client-wire
     (lambda (client request)
       (ok (equal '(:switch 2 96) (pg::pg-client-native-finish client 2 96)))
       (with-open-file (in request :element-type '(unsigned-byte 8))
         (multiple-value-bind (kind payload) (pg::pg-client-frame in)
           (ok (eql kind #\c))
           (ok (equalp payload #())))))
     (lambda (out) (pg::physical-stream-switch-result out 2 96))))

(deftest physical-client-custom-bound-keeps-the-default-frontend-bound
    (call-with-physical-client-wire
     (lambda (client request)
       (declare (ignore request))
       (multiple-value-bind (type length-bytes payload) (pg::read-message-frame (pg::pg-client-stream client))
         (ok (= (char-code #\D) type))
         (ok (= (1+ pg::+max-query-message-size+) (pg::bytes-to-u32 length-bytes 0)))
         (ok (null payload)))
       (ok (= 99 (read-byte (pg::pg-client-stream client)))))
     (lambda (out)
       (write-byte (char-code #\D) out)
       (write-sequence (pg::u32-to-bytes (1+ pg::+max-query-message-size+)) out)
       (write-byte 99 out))))

(cl-spec:defproperty physical-client-prepared-prefix-digests-keep-full-width
    ((prefix (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615)))
  (:about pg::pg-client-native-prepare)
  (:trials (:normal 200))
  (let ((expected (follow-anchor-request prefix overlap)) (received nil))
    (call-with-physical-client-wire
     (lambda (client request)
       (declare (ignore request))
       (setf received (pg::pg-client-native-prepare client "bound" (follow-anchor-receiver-request expected))))
     (lambda (out)
       (let ((row (physical-client-test-prepare-row)))
         (setf (nth 8 row) (format nil "~d" prefix) (nth 10 row) (format nil "~d" overlap))
         (physical-client-test-result out (physical-client-test-prepare-schema) (list row) "PREPARE_FOLLOW"))))
    (equal expected received)))

(deftest generated-physical-client-prepared-prefix-digests-keep-full-width
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-client-prepared-prefix-digests-keep-full-width :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest physical-client-start-distinguishes-copy-from-exact-fork-result
    (dolist (switch '(nil t))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (ok (equal (if switch '(:switch 2 96) :copy-both)
                    (pg::pg-client-native-start client "START_REPLICATION probe"))))
       (lambda (out)
         (if switch (pg::physical-stream-switch-result out 2 96)
             (pg::write-message out #\W #(0 0 0)))))))
