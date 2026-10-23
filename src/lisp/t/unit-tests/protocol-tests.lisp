;;;; protocol-tests.lisp -- frontend protocol tests.

(in-package #:pg)

(defun test-swank-arguments ()
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-port=4512" "--swank-bind-address=0.0.0.0"))
    (declare (ignore pgdata control-port control-socket))
    (check (string= host "127.0.0.1") "default SQL host is unchanged")
    (check (= port 5433) "default SQL port is unchanged")
    (check (= swank-port 4512) "Swank port accepts --option=value")
    (check (string= swank-bind-address "0.0.0.0")
           "Swank bind address accepts --option=value"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-bind-address" "0.0.0.0"))
    (declare (ignore host port pgdata control-port control-socket))
    (check (null swank-port) "Swank bind address alone does not enable listener")
    (check (string= swank-bind-address "0.0.0.0")
           "Swank bind address is preserved without a listener"))
  (check (handler-case
             (progn (parse-args '("--swank-port=")) nil)
           (error () t))
         "empty Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-port")) nil)
           (error () t))
         "missing Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-port" "65536")) nil)
           (error () t))
         "out-of-range Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-bind-address=")) nil)
           (error () t))
         "empty Swank bind address is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-bind-address")) nil)
           (error () t))
         "missing Swank bind address is rejected"))

(defun test-replication-slot-retention-option-parsing ()
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port bind-address temp-limit arena-limit slot-limit)
      (parse-args '("--max-slot-wal-keep-bytes=4096"))
    (declare (ignore host port pgdata control-port control-socket swank-port
                     bind-address temp-limit arena-limit))
    (check (= slot-limit 4096)
           "slot retention option accepts a finite byte cap"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port bind-address temp-limit arena-limit slot-limit)
      (parse-args '("--max-slot-wal-keep-bytes" "-1"))
    (declare (ignore host port pgdata control-port control-socket swank-port
                     bind-address temp-limit arena-limit))
    (check (null slot-limit)
           "slot retention option uses -1 to disable the cap"))
  (check (handler-case
             (progn (parse-args '("--max-slot-wal-keep-bytes" "-2")) nil)
           (error () t))
         "slot retention option rejects negative values other than -1"))

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
         (original-handler (symbol-function 'handle-unsupported-message)))
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
                 (symbol-function 'handle-unsupported-message)
                 (lambda (stream message)
                   (declare (ignore stream message))
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
                               nil (char-code #\D) (u32-to-bytes 5) #(0))
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
            (symbol-function 'handle-unsupported-message) original-handler)
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
