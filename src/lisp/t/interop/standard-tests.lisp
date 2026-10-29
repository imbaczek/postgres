(in-package #:pg-interop-tests)

(defun call-with-owned-standard-stream (function)
  (call-with-standard-slot-fixture
   (lambda ()
     (pg::create-standard-logical-slot "native_slot" :nothing)
     (pg::call-with-logical-replication-slot
      "native_slot"
      (lambda (slot)
        (let ((pg::*wal-durable-lsn* 256)
              (pg::*wal-logical-slot-read-progress* 256))
          (funcall function
                   (pg::make-standard-stream
                    :slot-name "native_slot" :slot-object slot :emit-floor 256
                    :scan-cursor 128 :safe-frontier 128 :reported-flush 128
                    :restart-floor 32)
                   slot)))))))

(defun standard-test-feedback (write flush apply &optional reply)
  (let ((payload (pg::make-payload)))
    (pg::payload-push-byte payload (char-code #\r))
    (pg::replication-push-u64 payload write)
    (pg::replication-push-u64 payload flush)
    (pg::replication-push-u64 payload apply)
    (pg::replication-push-i64 payload 0)
    (pg::payload-push-byte payload (if reply 1 0))
    payload))

(deftest standard-feedback-resume-and-unaligned-confirmation
    (call-with-owned-standard-stream
     (lambda (state slot)
       (pg::standard-accept-feedback state (standard-test-feedback 230 200 240 t))
       (ok (= 128 (pg::wal-replication-slot-confirmed-flush-lsn slot)))
       (ok (pg::standard-stream-reply-p state))
       (pg::standard-note-frontier state 160)
       (pg::standard-acknowledge-flush state)
       (ok (= 160 (pg::wal-replication-slot-confirmed-flush-lsn slot))
           "a lost-feedback resume claim confirms only the safely scanned prefix")
       (ok (= 32 (pg::wal-replication-slot-restart-lsn slot))
           "the pre-boundary active XID still pins its earlier WAL")
       (clrhash (pg::wal-replication-slot-pending-xids slot))
       (pg::standard-note-frontier state 224)
       (pg::standard-acknowledge-flush state)
       (ok (= 200 (pg::wal-replication-slot-confirmed-flush-lsn slot)))
       (ok (= 160 (pg::wal-replication-slot-restart-lsn slot))
           "an interior flush retains an aligned restart floor")
       (let ((decoded (pg::wal-read-replication-slot
                       (pg::wal-replication-slot-path "native_slot"))))
         (ok (= 200 (pg::wal-replication-slot-confirmed-flush-lsn decoded)))
         (ok (= 160 (pg::wal-replication-slot-restart-lsn decoded))))
       (pg::standard-accept-feedback state (standard-test-feedback 0 0 0))
       (ok (= 200 (pg::wal-replication-slot-confirmed-flush-lsn slot)))
       (ok (equal '(224) (pg::standard-stream-pending-boundaries state)))
       (ok (equal "22023" (sqlstate-of (pg::standard-accept-feedback
                                        state (standard-test-feedback 300 300 300))))))))

(deftest standard-feedback-preserves-private-ack-rules
    (call-with-owned-standard-stream
     (lambda (state slot)
       (declare (ignore state))
       (ok (eq slot (pg::wal-acknowledge-standard-replication-slot
                     "native_slot" 0 0 0)))
       (ok (equal "22023" (sqlstate-of (pg::wal-acknowledge-standard-replication-slot
                                        "native_slot" 160 128 150))))
       (let ((pg::*wal-logical-slot-owner* nil))
         (ok (equal "55006" (sqlstate-of (pg::wal-acknowledge-standard-replication-slot
                                          "native_slot" 160 128 192))))))))

(deftest standard-start-negotiation
    (dolist (version '(1 2 3 4))
      (let* ((command
               (pg::parse-replication-command
                (format nil "START_REPLICATION SLOT \"native_slot\" LOGICAL 0/00000000 (proto_version '~d', streaming 'off', binary 'false', two_phase 'false', origin 'any', publication_names '\"Pub One\", second')" version))))
        (ok (= version (pg::logical-replication-command-protocol-version command)))
        (ok (equal '("Pub One" "second") (pg::standard-publication-names command)))))
  (dolist (options '("proto_version '4', publication_names 'p', streaming 'on'"
                     "proto_version '4', publication_names 'p', streaming 'parallel'"
                     "proto_version '4', publication_names 'p', binary 'true'"
                     "proto_version '4', publication_names 'p', messages 'true'"
                     "proto_version '4', publication_names 'p', two_phase 'true'"
                     "proto_version '4', publication_names 'p', origin 'none'"
                     "proto_version '4', proto_version '4', publication_names 'p'"
                     "publication_names 'p'"
                     "proto_version '4', publication_names p"))
    (ok (handler-case
            (progn (pg::parse-replication-command
                    (format nil "START_REPLICATION SLOT s LOGICAL 0/0 (~a)" options)) nil)
          (pg::pg-error () t)) options)))

(deftest standard-commit-positions-and-replay-floor
    (let* ((transaction (pg::make-logical-transaction
                         :xid 7 :commit-record-lsn 160 :commit-lsn 192 :commit-timestamp 0))
           (begin (pg::pgoutput-standard-begin transaction))
           (commit (pg::pgoutput-standard-commit transaction)))
      (ok (equalp (octets '(66 0 0 0 0 0 0 0 160 0 0 0 0 0 0 0 0 0 0 0 7)) begin))
      (ok (equalp (octets '(67 0 0 0 0 0 0 0 0 160 0 0 0 0 0 0 0 192 0 0 0 0 0 0 0 0))
                  commit))
      (ok (pg::standard-transaction-visible-p transaction 0))
      (ok (pg::standard-transaction-visible-p transaction 160))
      (ok (pg::standard-transaction-visible-p transaction 161))
      (ok (pg::standard-transaction-visible-p transaction 170)
          "a flush inside the commit record does not certify the transaction")
      (ok (not (pg::standard-transaction-visible-p transaction 192)))
      (ok (not (pg::standard-transaction-visible-p transaction 256)))))

(deftest standard-source-commit-timestamps
    (dolist (timestamp '(0 1 -1 726000000000001 9223372036854775807 -9223372036854775808))
      (ok (= timestamp
             (pg::wal-decode-commit-timestamp (pg::wal-encode-commit-timestamp timestamp)))))
  (ok (null (pg::wal-decode-commit-timestamp #())))
  (ok (equalp (octets '(8 7 6 5 4 3 2 1))
              (pg::wal-encode-commit-timestamp #x0102030405060708)))
  (ok (handler-case (progn (pg::wal-decode-commit-timestamp #(1 2 3)) nil)
        (error () t)))
  (let* ((timestamp #x0102030405060708)
         (transaction (pg::make-logical-transaction
                       :xid 7 :commit-record-lsn 160 :commit-lsn 192
                       :commit-timestamp timestamp)))
    (ok (= timestamp (pg::replication-read-u64
                      (pg::pgoutput-standard-begin transaction) 9)))
    (ok (= timestamp (pg::replication-read-u64
                      (pg::pgoutput-standard-commit transaction) 18))))
  (ok (equal "0A000" (sqlstate-of (pg::pgoutput-standard-begin
                                   (pg::make-logical-transaction :xid 1 :commit-record-lsn 160
                                                                 :commit-lsn 192))))))

(defun call-with-standard-socket-pair (function)
  (let ((listener (pg::make-listener "127.0.0.1" 0))
        (client (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp))
        (server nil) (input nil) (output nil))
    (unwind-protect
         (progn
           (multiple-value-bind (address port) (sb-bsd-sockets:socket-name listener)
             (declare (ignore address))
             (sb-bsd-sockets:socket-connect
              client (sb-bsd-sockets:make-inet-address "127.0.0.1") port))
           (setf server (pg::accept-connection listener)
                 output (pg::socket->stream server)
                 input (pg::socket->stream client))
           (funcall function (pg::make-standard-stream :stream output :socket server) input))
      (pg::close-connection server output)
      (pg::close-connection client input)
      (ignore-errors (sb-bsd-sockets:socket-close listener)))))

(deftest standard-copydone-finishes-current-partial-frame
    (call-with-standard-socket-pair
     (lambda (state input)
       (let ((original (symbol-function 'pg::standard-poll-feedback))
             (polls 0)
             (payload (make-array 100 :element-type '(unsigned-byte 8) :initial-element 90)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::standard-poll-feedback)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (when (= (incf polls) 2)
                          (setf (pg::standard-stream-stop-reason state) :done))))
                (ok (pg::standard-write-frame state #\d payload))
                (ok (eq :done (pg::standard-stream-stop-reason state)))
                (ok (not (pg::standard-stream-raw-partial-p state)))
                (ok (pg::standard-write-frame state #\c #() :finish-copy t))
                (sb-ext:with-timeout 2
                  (multiple-value-bind (kind bytes) (pg::pg-client-frame input)
                    (ok (char= kind #\d))
                    (ok (equalp payload bytes)))
                  (multiple-value-bind (kind bytes) (pg::pg-client-frame input)
                    (ok (char= kind #\c))
                    (ok (zerop (length bytes))))))
           (setf (symbol-function 'pg::standard-poll-feedback) original))))))

(deftest standard-cancel-during-partial-frame-closes-transport
    (call-with-standard-socket-pair
     (lambda (state input)
       (let ((original (symbol-function 'pg::standard-poll-feedback))
             (polls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::standard-poll-feedback)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (when (= (incf polls) 2)
                          (pg::ereport "57014" "injected cancellation"))))
                (ok (equal "57014" (sqlstate-of (pg::standard-write-frame
                                                 state #\d (make-array 100 :element-type '(unsigned-byte 8))))))
                (ok (not (open-stream-p (pg::standard-stream-stream state))))
                (sb-ext:with-timeout 2
                  (ok (equalp (octets '(100 0 0 0 104)) (pg::read-exact-bytes input 5)))
                  (ok (null (read-byte input nil nil))
                      "no ErrorResponse is appended inside the incomplete frame")))
           (setf (symbol-function 'pg::standard-poll-feedback) original))))))

(deftest standard-streamed-frame-finishes-after-copydone
    (call-with-standard-socket-pair
     (lambda (state input)
       (let ((original (symbol-function 'pg::standard-poll-feedback))
             (polls 0)
             (payload (make-array 100 :element-type '(unsigned-byte 8) :initial-element 90)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::standard-poll-feedback)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (when (= (incf polls) 2)
                          (setf (pg::standard-stream-stop-reason state) :done))))
                (ok (pg::standard-write-streamed-frame
                     state #\d (length payload) (lambda (sink) (funcall sink payload))))
                (ok (eq :done (pg::standard-stream-stop-reason state)))
                (ok (not (pg::standard-stream-raw-partial-p state)))
                (sb-ext:with-timeout 2
                  (multiple-value-bind (kind bytes) (pg::pg-client-frame input)
                    (ok (char= kind #\d))
                    (ok (equalp bytes payload)))))
           (setf (symbol-function 'pg::standard-poll-feedback) original))))))

(deftest standard-streamed-frame-cancel-closes-partial-transport
    (call-with-standard-socket-pair
     (lambda (state input)
       (let ((original (symbol-function 'pg::standard-poll-feedback)) (polls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::standard-poll-feedback)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (when (= (incf polls) 2)
                          (pg::ereport "57014" "injected cancellation"))))
                (ok (equal "57014"
                           (sqlstate-of
                            (pg::standard-write-streamed-frame
                             state #\d 100 (lambda (sink) (funcall sink (make-array 100 :element-type '(unsigned-byte 8))))))))
                (ok (not (open-stream-p (pg::standard-stream-stream state))))
                (sb-ext:with-timeout 2
                  (ok (equalp (octets '(100 0 0 0 104)) (pg::read-exact-bytes input 5)))
                  (ok (null (read-byte input nil nil)))))
           (setf (symbol-function 'pg::standard-poll-feedback) original))))))

(deftest standard-streamed-frame-retries-short-sends
    (call-with-standard-socket-pair
     (lambda (state input)
       (let ((original (symbol-function 'pg::standard-send-socket-chunk))
             (poll (symbol-function 'pg::standard-poll-feedback))
             (calls 0)
             (payload (make-array 20 :element-type '(unsigned-byte 8) :initial-element 91)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::standard-poll-feedback) (lambda (state) (declare (ignore state)))
                      (symbol-function 'pg::standard-send-socket-chunk)
                      (lambda (socket bytes)
                        (incf calls)
                        (if (= calls 1) nil
                            (funcall original socket (subseq bytes 0 1)))))
                (ok (pg::standard-write-streamed-frame
                     state #\d (length payload) (lambda (sink) (funcall sink payload))))
                (ok (> calls 20))
                (sb-ext:with-timeout 2
                  (multiple-value-bind (kind bytes) (pg::pg-client-frame input)
                    (ok (char= kind #\d))
                    (ok (equalp bytes payload)))))
           (setf (symbol-function 'pg::standard-send-socket-chunk) original
                 (symbol-function 'pg::standard-poll-feedback) poll))))))

(deftest subscriber-reads-fragmented-socket-frame
    (call-with-standard-socket-pair
     (lambda (state input)
       (let* ((payload (pg::make-payload))
              (frame (pg::make-payload))
              (runtime (pg::make-logical-subscription-runtime
                        :options (pg::make-logical-subscription-options) :transaction-open-p t))
              (original (symbol-function 'pg::logical-pgoutput-apply-operation))
              (seen nil) (writer nil))
         (unwind-protect
              (progn
                (pg::payload-push-byte payload (char-code #\w))
                (dotimes (i 24) (pg::payload-push-byte payload 0))
                (pg::payload-push payload #(73 0 0 0 7 78 0 1 116 0 0 0 4 240 159 153 130))
                (pg::payload-push-byte frame (char-code #\d))
                (pg::payload-push frame (pg::u32-to-bytes (+ 4 (length payload))))
                (pg::payload-push frame payload)
                (setf (symbol-function 'pg::logical-pgoutput-apply-operation)
                      (lambda (receiver operation) (declare (ignore receiver)) (setf seen operation))
                      writer (sb-thread:make-thread
                              (lambda ()
                                (loop for byte across frame do
                                  (write-byte byte (pg::standard-stream-stream state))
                                  (force-output (pg::standard-stream-stream state))
                                  (sleep 0.001)))))
                (sb-ext:with-timeout 5
                  (pg::logical-read-stream-operation runtime (pg::make-pg-client :stream input)))
                (sb-thread:join-thread writer)
                (setf writer nil)
                (ok (equalp seen (list #\I 7 (vector "🙂")))))
           (unwind-protect
                (when writer (sb-thread:join-thread writer))
             (setf (symbol-function 'pg::logical-pgoutput-apply-operation) original)))))))

(deftest standard-streamed-row-matches-existing-frame
    (call-with-standard-socket-pair
     (lambda (state input)
       (let* ((poll (symbol-function 'pg::standard-poll-feedback))
              (clock (symbol-function 'pg::postgres-current-timestamp))
              (change (pg::make-logical-change
                       :kind :insert :database-oid 7 :relation-oid 701 :relation-name "items"
                       :columns '((:attnum 1 :name "id" :type :int4 :typmod -1)
                                  (:attnum 2 :name "value" :type :text :typmod -1))
                       :new-values #(7 "hé🙂")))
              (expected (pg::pgoutput-xlog-envelope 0 0 30)))
         (unwind-protect
              (progn
                (pg::payload-push expected (pg::pgoutput-build-row-change change))
                (setf (symbol-function 'pg::standard-poll-feedback) (lambda (state) (declare (ignore state)))
                      (symbol-function 'pg::postgres-current-timestamp) (lambda () 30))
                (ok (pg::standard-xlog-row state change))
                (sb-ext:with-timeout 2
                  (multiple-value-bind (kind bytes) (pg::pg-client-frame input)
                    (ok (char= kind #\d))
                    (ok (equalp bytes expected)))))
           (setf (symbol-function 'pg::standard-poll-feedback) poll
                 (symbol-function 'pg::postgres-current-timestamp) clock))))))

(deftest standard-streamed-frame-stop-before-header
    (call-with-standard-socket-pair
     (lambda (state input)
       (declare (ignore input))
       (let ((original (symbol-function 'pg::standard-poll-feedback)) (produced nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::standard-poll-feedback)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (setf (pg::standard-stream-stop-reason state) :done)))
                (ok (not (pg::standard-write-streamed-frame
                          state #\d 1 (lambda (sink) (setf produced t) (funcall sink 1)))))
                (ok (not produced))
                (ok (not (pg::standard-stream-raw-partial-p state))))
           (setf (symbol-function 'pg::standard-poll-feedback) original))))))

(deftest standard-timeout-is-a-disconnect
    (call-with-owned-standard-stream
     (lambda (state slot)
       (declare (ignore slot))
       (setf (pg::standard-stream-last-reply-time state)
             (- (get-internal-real-time) (* 61 internal-time-units-per-second)))
       (ok (handler-case (progn (pg::standard-stream-check state) nil)
             (pg::standard-replication-timeout () t))))))

(deftest standard-truncate-announces-relation-before-first-change
    (let* ((relation (pg::make-logical-change
                      :relation-oid 10 :relation-name "a" :key-attnums '(1)
                      :columns '((:attnum 1 :name "id" :type :int4 :typmod -1))))
           (transaction (pg::make-logical-transaction :xid 7 :commit-record-lsn 160
                                                      :commit-lsn 192 :commit-timestamp 0))
           (state (pg::make-standard-stream :emit-floor 0))
           (original (symbol-function 'pg::standard-xlog-data))
           (frames nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::standard-xlog-data)
                   (lambda (ignored payload &optional (end 0))
                     (declare (ignore ignored))
                     (push (list (code-char (aref payload 0)) end) frames)
                     t))
             (ok (pg::standard-send-transaction
                  state transaction (list (cons :truncate (list relation)))))
             (ok (equal '((#\B 0) (#\R 0) (#\T 0) (#\C 192)) (nreverse frames))))
        (setf (symbol-function 'pg::standard-xlog-data) original))))

(deftest standard-native-truncate-selection
    (let* ((selection '("p"))
           (data '(:tables ("a" "b") :relation-oids (10 11)
                   :relations ((:oid 10 :name "a" :key-attnums (1)
                                :columns ((:attnum 1 :name "id" :type :int4 :typmod -1)))
                               (:oid 11 :name "b" :key-attnums (1)
                                :columns ((:attnum 1 :name "id" :type :int4 :typmod -1))))))
           (change (pg::make-logical-change
                    :kind :truncate
                    :publication-selection '((:relation-oid 10 :publications ((:name "p" :oid 100 :generation 1 :unsupported nil)))
                                             (:relation-oid 11 :publications nil))
                    :event-payload (pg::catalog-readable-data-string data)))
           (transaction (pg::make-logical-transaction
                         :xid 1 :commit-record-lsn 160 :commit-lsn 192
                         :changes (list change))))
      (let ((changes (pg::standard-transaction-changes transaction selection)))
        (ok (eq :truncate (caar changes)))
        (ok (equal '(10) (mapcar #'pg::logical-change-relation-oid (cdar changes))))
        (ok (equal "a" (pg::logical-change-relation-name (second (first changes))))))
      (setf data (copy-tree data))
      (setf (getf (first (getf data :relations)) :key-attnums) nil
            (pg::logical-change-event-payload change) (pg::catalog-readable-data-string data))
      (ok (null (pg::logical-change-key-attnums
                 (second (first (pg::standard-transaction-changes transaction selection))))))
      (setf (pg::logical-change-event-payload change) "(:TABLES (\"a\"))")
      (ok (equal "0A000" (sqlstate-of (pg::standard-transaction-changes transaction selection))))))

(deftest standard-fragmented-feedback-and-copydone-boundary
    (call-with-owned-standard-stream
     (lambda (state slot)
       (declare (ignore slot))
       (let* ((path (merge-pathnames "fragmented-feedback" pg::*pgdata*))
              (feedback (standard-test-feedback 0 0 0))
              (available 0)
              (original (symbol-function 'pg::standard-input-ready-p)))
         (with-open-file (output path :direction :output :element-type '(unsigned-byte 8))
           (write-byte (char-code #\d) output)
           (write-sequence (pg::u32-to-bytes 38) output)
           (write-sequence feedback output)
           (write-byte (char-code #\c) output)
           (write-sequence (pg::u32-to-bytes 4) output)
           (write-byte (char-code #\Q) output))
         (with-open-file (input path :element-type '(unsigned-byte 8))
           (setf (pg::standard-stream-stream state) input)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::standard-input-ready-p)
                        (lambda (ignored)
                          (declare (ignore ignored))
                          (< (file-position input) available)))
                  (loop for exposed from 1 below 39
                        do (setf available exposed)
                           (ok (null (pg::standard-read-feedback-frame state)))
                           (ok (= exposed (pg::standard-stream-input-count state))))
                  (setf available 39)
                  (ok (pg::standard-read-feedback-frame state))
                  (ok (zerop (pg::standard-stream-input-count state)))
                  (setf available 45)
                  (ok (pg::standard-read-feedback-frame state))
                  (ok (eq :done (pg::standard-stream-stop-reason state)))
                  (ok (null (pg::standard-read-feedback-frame state)))
                  (ok (= 44 (file-position input)))
                  (ok (= (char-code #\Q) (read-byte input))))
             (setf (symbol-function 'pg::standard-input-ready-p) original)))))))
