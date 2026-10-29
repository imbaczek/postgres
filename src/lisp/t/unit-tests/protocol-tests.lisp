;;;; protocol-tests.lisp -- frontend protocol tests.

(in-package #:pg)

(defun test-malformed-packet-p (path bytes reader &optional expected-message)
  (with-open-file (stream path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create
                          :element-type '(unsigned-byte 8))
    (write-sequence bytes stream))
  (handler-case
      (with-open-file (stream path
                              :direction :input
                              :element-type '(unsigned-byte 8))
        (funcall reader stream)
        nil)
    (pg-error (condition)
      (and expected-message
           (string= (pg-error-sqlstate condition) "08P01")
           (string= (pg-error-message condition) expected-message)))
    (error () (null expected-message))))

(defun test-concat-octets (&rest parts)
  (let* ((size (reduce #'+ parts :key #'length :initial-value 0))
         (bytes (make-array size :element-type '(unsigned-byte 8)))
         (offset 0))
    (dolist (part parts)
      (replace bytes part :start1 offset)
      (incf offset (length part)))
    bytes))

(defun test-startup-packet-size (path packet-length)
  (with-open-file (stream path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create
                          :element-type '(unsigned-byte 8))
    (write-sequence (u32-to-bytes packet-length) stream)
    (write-sequence
     (make-array (- packet-length 4)
                 :element-type '(unsigned-byte 8)
                 :initial-element 0)
     stream))
  (with-open-file (stream path
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((body (read-startup-packet stream)))
      (and body (= (length body) +max-startup-packet-length+)))))

(defun test-logical-ddl-response-deferral ()
  ;; postgres.c:exec_simple_query completes the transaction before EndCommand.
  (let ((original-write-message (symbol-function 'write-message))
        (original-commit-transaction (symbol-function 'commit-transaction))
        (write-count 0)
        (write-types '())
        (commit-failed-p t)
        (commit-complete-p nil)
        (all-writes-after-commit-and-unlock-p t)
        (responses nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'write-message)
                 (lambda (stream type payload)
                   (declare (ignore stream payload))
                   (let ((acquired
                           (sb-thread:grab-mutex
                            *logical-ddl-event-order-lock* :waitp nil)))
                     (setf all-writes-after-commit-and-unlock-p
                           (and all-writes-after-commit-and-unlock-p
                                (not *logical-ddl-event-order-active-p*)
                                commit-complete-p
                                acquired))
                     (when acquired
                       (sb-thread:release-mutex
                        *logical-ddl-event-order-lock*))
                     (incf write-count)
                     (push type write-types)
                     (values)))
                 (symbol-function 'commit-transaction)
                 (lambda ()
                   (when commit-failed-p
                     (error "injected DDL commit failure"))
                   (setf commit-complete-p t)))
           (multiple-value-bind (result queued)
               (call-with-logical-ddl-event-order
                (lambda ()
                  (send-notice nil "queued DDL notice")
                  (send-command-complete nil "CREATE TABLE")
                  (check (zerop write-count)
                         "DDL responses remain queued while schema events are ordered")
                  :ddl-result))
             (check (eq result :ddl-result)
                    "DDL ordering returns the body result")
             (setf responses queued))
           (check (equal (mapcar #'first responses)
                         '(:notice :command-complete))
                  "DDL response queue preserves notice and completion order")
           (check (zerop write-count)
                  "responses stay deferred after unlocking until COMMIT succeeds")
           (handler-case
               (commit-ddl-and-flush-responses responses)
             (error () nil))
           (check (zerop write-count)
                  "failed DDL COMMIT sends no success or deferred notice response")
           (setf commit-failed-p nil)
           (commit-ddl-and-flush-responses responses)
           (check (and (= write-count 2)
                       (equal (mapcar #'char-code (nreverse write-types))
                              '(78 67))
                       all-writes-after-commit-and-unlock-p)
                  "successful DDL COMMIT flushes replies outside the event-order lock"))
      (setf (symbol-function 'write-message) original-write-message
            (symbol-function 'commit-transaction)
            original-commit-transaction))))

(defun test-protocol-packet-limits ()
  (let ((path (merge-pathnames "malformed-packet.out" *pgdata*)))
    (check (test-startup-packet-size path +max-startup-message-size+)
           "startup packets accept PostgreSQL's maximum body length")
    (check (test-malformed-packet-p
            path (u32-to-bytes 7) #'read-startup-packet
            "startup packet length is too short")
           "undersized startup packets are rejected")
    (check (test-malformed-packet-p
            path (u32-to-bytes (1+ +max-startup-message-size+))
            #'read-startup-packet "startup packet exceeds the size limit")
           "oversized startup packets are rejected before allocation")
    (check (test-malformed-packet-p
            path (u32-to-bytes 8) #'read-startup-packet
            "incomplete startup packet body")
           "truncated startup packet bodies are rejected")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes 3))
            #'test-read-frontend-message "frontend message length is too short")
           "undersized query packets are rejected")
    (check (test-malformed-packet-p
            path (vector (char-code #\Q))
            #'test-read-frontend-message "incomplete frontend message length")
           "truncated query packet lengths are rejected after admission")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes
                                      (1+ +max-query-message-size+)))
            #'test-read-frontend-message "frontend message exceeds the size limit")
           "oversized query packets are rejected before allocation")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes 5))
            #'test-read-frontend-message "incomplete frontend message body")
           "truncated query packet bodies are rejected")))

(defun test-protocol-decoding-enters-live-operation ()
  (let* ((backend (make-backend))
         (test-lock (sb-thread:make-mutex :name "test protocol admission lock"))
         (admission-waited nil)
         (decoder-ran nil)
         (decoder-live-count 0)
         (decoder-lane nil)
         (handler-ran nil)
         (worker-error nil)
         (worker nil)
         (original-decoder (symbol-function 'decode-frontend-message))
         (original-handler (symbol-function 'extended-apply-operation)))
    (unwind-protect
         (progn
           (setf (symbol-function 'decode-frontend-message)
                 (lambda (type-byte length-bytes payload)
                   (let ((message (funcall original-decoder
                                           type-byte length-bytes payload)))
                     (sb-thread:with-mutex (test-lock)
                       (setf decoder-ran t
                             decoder-live-count *live-operation-count*
                             decoder-lane *live-operation-current-lane*))
                     message))
                 (symbol-function 'extended-apply-operation)
                 (lambda (stream operation payload)
                   (declare (ignore stream operation payload))
                   (sb-thread:with-mutex (test-lock)
                     (setf handler-ran t))))
           (let ((*backend* backend))
             (with-live-operation-quiescence
               (setf worker
                     (sb-thread:make-thread
                      (lambda ()
                        (let ((*backend* backend)
                              (*live-operation-admission-wait-hook*
                                (lambda ()
                                  (sb-thread:with-mutex (test-lock)
                                    (setf admission-waited t)))))
                          (handler-case
                              (dispatch-frontend-frame
                               nil (char-code #\D) (u32-to-bytes 6)
                               (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(83 0)))
                            (error (condition)
                              (sb-thread:with-mutex (test-lock)
                                (setf worker-error condition))))))
                      :name "test admitted protocol decode"))
               (check (test-wait-until
                       (lambda ()
                         (sb-thread:with-mutex (test-lock) admission-waited)))
                      "protocol dispatch waits at closed reload admission")
               (check (not (sb-thread:with-mutex (test-lock) decoder-ran))
                      "protocol validation and object construction wait for admission")
               (check (not (sb-thread:with-mutex (test-lock) handler-ran))
                      "protocol handling waits for admission")))
           (check (and worker
                       (eq (sb-thread:join-thread worker :timeout 2) :continue))
                  "raw protocol dispatch resumes after reload admission opens")
           (check (and (sb-thread:with-mutex (test-lock) decoder-ran)
                       (plusp (sb-thread:with-mutex (test-lock)
                                decoder-live-count))
                       (eq (sb-thread:with-mutex (test-lock) decoder-lane)
                           :normal))
                  "frontend validation and construction run inside an admitted operation")
           (check (and (sb-thread:with-mutex (test-lock) handler-ran)
                       (null worker-error))
                  "the decoded message is dispatched after admission"))
      (when (and worker (sb-thread:thread-alive-p worker))
        (ignore-errors
         (sb-thread:interrupt-thread
          worker (lambda () (error "protocol admission test timed out"))))
        (ignore-errors (sb-thread:join-thread worker :timeout 2)))
      (setf (symbol-function 'decode-frontend-message) original-decoder
            (symbol-function 'extended-apply-operation) original-handler)
      (test-reset-live-operation-admission))))

(defun test-malformed-protocol-frames-drain-to-fatal-error ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend)))
    (labels ((expect-fatal-frame-error (type-byte length-bytes payload label)
               (let ((worker
                       (sb-thread:make-thread
                        (lambda ()
                          (let ((*backend* backend)
                                (*live-operation-admission-open* nil)
                                (*live-operation-drain-open* t)
                                (*live-operation-drain-backends* (list backend))
                                (*live-operation-cleanup-backends* (list backend))
                                (*live-operation-count* 0)
                                (*live-operation-backend-counts*
                                  (make-hash-table :test #'eq)))
                            (handler-case
                                (dispatch-frontend-frame
                                 nil type-byte length-bytes payload)
                              (error (condition) condition))))
                        :name "test malformed frame drain")))
                 (let ((result (sb-thread:join-thread worker :timeout 2)))
                   (check (and (typep result 'pg-error)
                               (fatal-error-p result)
                               (string= (pg-error-sqlstate result) "08P01"))
                          label)
                   (when (sb-thread:thread-alive-p worker)
                     (ignore-errors
                      (sb-thread:interrupt-thread
                       worker (lambda () (error "malformed frame drain timed out"))))
                     (ignore-errors (sb-thread:join-thread worker :timeout 2)))))))
      (set-backend-state backend :in-progress)
      (unwind-protect
           (progn
             (expect-fatal-frame-error
              (char-code #\Q) (u32-to-bytes 3) nil
              "a malformed query frame reaches fatal validation during drain")
             (expect-fatal-frame-error
              (char-code #\D) (u32-to-bytes 5) nil
              "a malformed non-query frame reaches fatal validation during drain")
             (expect-fatal-frame-error
              (char-code #\X) (u32-to-bytes 5) #(0)
              "a malformed Terminate frame reaches fatal validation during drain"))
        (set-backend-state backend old-state)
        (test-reset-live-operation-admission)))))

(defun test-terminate-frame-drains-active-backend ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend)))
    (set-backend-state backend :in-progress)
    (unwind-protect
         (let ((worker
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* backend)
                          (*live-operation-admission-open* nil)
                          (*live-operation-drain-open* t)
                          (*live-operation-drain-backends* (list backend))
                          (*live-operation-cleanup-backends* (list backend))
                          (*live-operation-count* 0)
                          (*live-operation-backend-counts*
                            (make-hash-table :test #'eq)))
                      (dispatch-frontend-frame
                       nil (char-code #\X) (u32-to-bytes 4)
                       (make-array 0 :element-type '(unsigned-byte 8)))))
                  :name "test Terminate frame drain")))
           (let ((result (sb-thread:join-thread worker :timeout 2)))
             (check (eq result :terminate)
                    "an active backend can dispatch Terminate during reload drain")
             (when (sb-thread:thread-alive-p worker)
               (ignore-errors
                (sb-thread:interrupt-thread
                 worker (lambda () (error "Terminate frame drain timed out"))))
               (ignore-errors (sb-thread:join-thread worker :timeout 2)))))
      (set-backend-state backend old-state)
      (test-reset-live-operation-admission))))
