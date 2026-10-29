(in-package #:pg-physical-tests)

(cl-spec:defproperty physical-libpq-headers-preserve-unsigned-identities
    ((session (range integer 1 #xffffffffffffffff))
     (request (range integer 1 #xffffffffffffffff))
     (size (range integer 0 1048576)))
  (:about pg::physical-libpq-header)
  (:trials (:normal 200))
  (let ((header (pg::physical-libpq-header session request #\T size)))
    (multiple-value-bind (kind actual)
        (pg::physical-libpq-check-header header session request '(#\T) size)
      (and (= 25 (length header)) (char= kind #\T) (= size actual)))))

(deftest generated-physical-libpq-header-identities
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-libpq-headers-preserve-unsigned-identities :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest physical-libpq-header-rejects-stale-malformed-and-oversized-responses
    (dolist (change '(:magic :session :request :kind :length :short :trailing :global-bound))
      (let ((header (pg::physical-libpq-header 42 1 #\T 4)))
        (ecase change
          (:global-bound
           (setf header (pg::physical-libpq-header 42 1 #\T #xffffffff)))
          (:magic (setf (aref header 0) 0))
          (:session (setf (aref header 11) 43))
          (:request (setf (aref header 19) 2))
          (:kind (setf (aref header 20) (char-code #\D)))
          (:length (setf (aref header 24) 5))
          (:short (setf header (subseq header 0 24)))
          (:trailing (setf header (concatenate '(vector (unsigned-byte 8)) header #(0)))))
        (ok (intake-fails-p
             (lambda ()
               (pg::physical-libpq-check-header header 42 1 '(#\T)
                                                (if (eq change :global-bound) #xffffffff 4))))))))

(deftest physical-libpq-profile-copies-input-and-keeps-secrets-out-of-printing
    (let* ((original (copy-seq "host=localhost password=private-fixture-marker"))
           (fallback (copy-seq "walreceiver"))
           (profile (pg::make-physical-libpq-profile original :fallback fallback))
           (expected (copy-seq original)))
      (setf (char original 0) #\X (char fallback 0) #\X)
      (ok (equal expected (pg::physical-libpq-profile-conninfo profile)))
      (ok (equal "walreceiver" (pg::physical-libpq-profile-fallback profile)))
      (setf (char (pg::physical-libpq-profile-conninfo profile) 0) #\Y)
      (ok (equal expected (pg::physical-libpq-profile-conninfo profile)))
      (ok (not (search "private-fixture-marker" (princ-to-string profile))))
      (ok (intake-fails-p
           (lambda () (pg::make-physical-libpq-profile (format nil "host=x~c" (code-char 0))))))))

(defun libpq-test-string (payload text)
  (let ((bytes (pg::string->bytes text)))
    (pg::payload-push-u32 payload (length bytes))
    (loop for byte across bytes do (vector-push-extend byte payload))))

(defun libpq-test-result-body (&key (name "systemid") (oid 25) (format 0) (value "42")
                                 (tag "IDENTIFY_SYSTEM") (status 0))
  (let ((payload (pg::make-payload)))
    (vector-push-extend status payload)
    (libpq-test-string payload tag)
    (pg::payload-push-u32 payload 1)
    (pg::payload-push-u32 payload 1)
    (libpq-test-string payload name)
    (dolist (number (list 0 0 oid -1 -1 format))
      (pg::payload-push-u32 payload (mod number #x100000000)))
    (if (eq value pg::+null+)
        (pg::payload-push-u32 payload #xffffffff)
        (libpq-test-string payload value))
    payload))

(deftest physical-libpq-result-retains-typed-single-row-validation
    (let ((result (pg::physical-libpq-decode-result #\T (libpq-test-result-body))))
      (ok (equalp #("42") (pg::physical-libpq-check-result result '(("systemid" 25)) "IDENTIFY_SYSTEM"))))
  (dolist (body (list (libpq-test-result-body :name "wrong")
                      (libpq-test-result-body :oid 20)
                      (libpq-test-result-body :format 1)
                      (libpq-test-result-body :tag "wrong")
                      (libpq-test-result-body :value pg::+null+)
                      (libpq-test-result-body :status 2)))
    (ok (intake-fails-p
         (lambda ()
           (pg::physical-libpq-check-result
            (pg::physical-libpq-decode-result #\T body) '(("systemid" 25)) "IDENTIFY_SYSTEM"))))))

(deftest physical-libpq-result-rejects-truncation-and-trailing-bytes
    (let ((body (libpq-test-result-body)))
      (dolist (bad (list (subseq body 0 (1- (length body)))
                         (concatenate '(vector (unsigned-byte 8)) body #(0))))
        (ok (intake-fails-p (lambda () (pg::physical-libpq-decode-result #\T bad)))))))

(defun call-with-libpq-test-pipe (function)
  (let* ((process (sb-ext:run-program "/bin/cat" nil :use-posix-spawn t :wait nil
                                                     :input :stream :output :stream :error :stream))
         (channel (pg::%make-physical-libpq-channel :process process :session 42)))
    (unwind-protect
         (sb-ext:with-timeout 5 (funcall function channel))
      (close (sb-ext:process-input process))
      (sb-ext:process-wait process)
      (sb-ext:process-close process))))

(deftest physical-libpq-pipe-preserves-binary-bodies
    (call-with-libpq-test-pipe
     (lambda (channel)
       (let* ((body (make-array 256 :element-type '(unsigned-byte 8)
                                    :initial-contents (loop for byte below 256 collect byte)))
              (request (pg::physical-libpq-write-request channel #\D body)))
         (multiple-value-bind (kind received)
             (pg::physical-libpq-read-reply channel request '(#\D) 256)
           (ok (char= kind #\D))
           (ok (equalp body received)))))))

(deftest physical-libpq-pipe-rejects-body-before-reading-it
    (call-with-libpq-test-pipe
     (lambda (channel)
       (let ((stream (sb-ext:process-input (pg::physical-libpq-channel-process channel))))
         (write-sequence (pg::physical-libpq-header 42 0 #\D 1024) stream)
         (finish-output stream)
         (ok (intake-fails-p
              (lambda () (pg::physical-libpq-read-reply channel 0 '(#\D) 10))))))))

(deftest physical-libpq-pipe-preserves-checked-error-sqlstate
    (call-with-libpq-test-pipe
     (lambda (channel)
       (let ((body (pg::make-payload)))
         (loop for byte across (pg::string->bytes "28P01")
               do (vector-push-extend byte body))
         (libpq-test-string body "libpq connection failed")
         (let ((request (pg::physical-libpq-write-request channel #\! body)))
           (handler-case
               (progn (pg::physical-libpq-read-reply channel request '(#\O) 0)
                      (fail "expected checked helper error"))
             (pg::pg-client-error (condition)
               (ok (equal "28P01" (pg::pg-client-error-sqlstate condition)))
               (ok (equal "libpq connection failed" (pg::pg-client-error-message condition))))))))))

(defun call-with-owned-libpq-test-process (program arguments function)
  (let ((channel (pg::physical-libpq-reserve-channel)))
    (unwind-protect
         (progn
           (setf (pg::physical-libpq-channel-process channel)
                 (sb-ext:run-program program arguments :use-posix-spawn t :wait nil
                                                       :input :stream :output :stream :error :stream))
           (let ((fd (pg::physical-libpq-pidfd-open
                      (sb-ext:process-pid (pg::physical-libpq-channel-process channel)) 0)))
             (assert (not (minusp fd)))
             (setf (pg::physical-libpq-channel-pidfd channel) fd))
           (funcall function channel))
      (pg::physical-libpq-close channel))))

(deftest physical-libpq-terminal-child-does-not-acquire-reused-pid
    (let ((channel (pg::physical-libpq-reserve-channel))
          (original (symbol-function 'pg::physical-libpq-pidfd-open))
          (calls 0))
      (unwind-protect
           (progn
             (setf (pg::physical-libpq-channel-process channel)
                   (sb-ext:run-program "/bin/true" nil :wait t))
             (setf (symbol-function 'pg::physical-libpq-pidfd-open)
                   (lambda (pid flags)
                     (declare (ignore pid flags))
                     (incf calls)
                     -1))
             (ok (intake-fails-p
                  (lambda () (pg::physical-libpq-acquire-process-handle channel))))
             (ok (zerop calls))
             (pg::physical-libpq-close channel)
             (ok (pg::physical-libpq-channel-closed channel)))
        (setf (symbol-function 'pg::physical-libpq-pidfd-open) original)
        (pg::physical-libpq-close channel))))

(deftest physical-libpq-pidfd-failure-kills-owned-child-before-reaping
    (let ((channel (pg::physical-libpq-reserve-channel))
          (original (symbol-function 'pg::physical-libpq-pidfd-open)))
      (unwind-protect
           (progn
             (setf (pg::physical-libpq-channel-process channel)
                   (sb-ext:run-program "/bin/sleep" '("30") :use-posix-spawn t
                                                            :wait nil :input :stream :output :stream :error :stream))
             (setf (symbol-function 'pg::physical-libpq-pidfd-open)
                   (lambda (pid flags) (declare (ignore pid flags)) -1))
             (ok (intake-fails-p
                  (lambda () (pg::physical-libpq-acquire-process-handle channel))))
             (pg::physical-libpq-close channel)
             (ok (member (sb-ext:process-status (pg::physical-libpq-channel-process channel))
                         '(:exited :signaled)))
             (ok (pg::physical-libpq-channel-closed channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*))))
        (setf (symbol-function 'pg::physical-libpq-pidfd-open) original)
        (pg::physical-libpq-close channel))))

(deftest physical-libpq-capacity-is-returned-only-after-close
    (let ((first (pg::physical-libpq-reserve-channel))
          (second nil))
      (unwind-protect
           (progn
             (setf second (pg::physical-libpq-reserve-channel))
             (ok (intake-fails-p #'pg::physical-libpq-reserve-channel))
             (pg::physical-libpq-close second)
             (setf second (pg::physical-libpq-reserve-channel))
             (ok second))
        (when second (pg::physical-libpq-close second))
        (pg::physical-libpq-close first))))

(deftest physical-libpq-deadline-kills-and-reaps-owned-helper
    (call-with-owned-libpq-test-process
     "/bin/sleep" '("30")
     (lambda (channel)
       (let ((timed-out nil))
         (handler-case
             (pg::physical-libpq-exchange channel #\X (pg::make-payload) '(#\A) 0 :timeout 0.05)
           (sb-ext:timeout () (setf timed-out t)))
         (ok timed-out)
         (ok (member (sb-ext:process-status (pg::physical-libpq-channel-process channel))
                     '(:exited :signaled)))
         (ok (null (pg::physical-libpq-channel-pidfd channel)))
         (ok (not (gethash channel pg::*physical-libpq-channels*)))))))

(deftest physical-libpq-controller-can-stop-but-cannot-own-io
    (call-with-owned-libpq-test-process
     "/bin/cat" nil
     (lambda (channel)
       (let ((rejected nil))
         (sb-thread:join-thread
          (sb-thread:make-thread
           (lambda ()
             (setf rejected (intake-fails-p
                             (lambda () (pg::physical-libpq-require-owner channel))))
             (pg::physical-libpq-stop channel))))
         (ok rejected)
         (ok (pg::physical-libpq-channel-stopped channel))))))

(deftest physical-libpq-query-validation-failure-closes-channel
    (let ((original (symbol-function 'pg::physical-libpq-exchange))
          (channel (pg::physical-libpq-reserve-channel)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-exchange)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (values #\T (libpq-test-result-body :name "wrong"))))
             (ok (intake-fails-p
                  (lambda ()
                    (pg::physical-libpq-query channel "IDENTIFY_SYSTEM"
                                              '(("systemid" 25)) "IDENTIFY_SYSTEM"))))
             (ok (pg::physical-libpq-channel-stopped channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*))))
        (setf (symbol-function 'pg::physical-libpq-exchange) original)
        (pg::physical-libpq-close channel))))

(defun libpq-test-switch-body ()
  (let ((body (pg::make-payload)))
    (vector-push-extend 0 body)
    (libpq-test-string body "START_STREAMING")
    (pg::payload-push-u32 body 2)
    (pg::payload-push-u32 body 1)
    (dolist (column '(("next_tli" 20 8) ("next_tli_startpos" 25 -1)))
      (libpq-test-string body (first column))
      (dolist (number (list 0 0 (second column) (third column) -1 0))
        (pg::payload-push-u32 body (mod number #x100000000))))
    (libpq-test-string body "2")
    (libpq-test-string body "0/400")
    body))

(deftest physical-libpq-finish-requires-prepared-child-and-fork
    (let ((original (symbol-function 'pg::physical-libpq-exchange))
          (channel (pg::physical-libpq-reserve-channel)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-exchange)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (values #\T (libpq-test-switch-body))))
             (ok (equal (list :switch 2 1024) (pg::physical-libpq-finish channel 2 1024)))
             (ok (intake-fails-p
                  (lambda () (pg::physical-libpq-finish channel 3 1024))))
             (ok (pg::physical-libpq-channel-stopped channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*))))
        (setf (symbol-function 'pg::physical-libpq-exchange) original)
        (pg::physical-libpq-close channel))))
