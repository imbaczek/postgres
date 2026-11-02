(in-package #:pg)

(defvar *shutdown-streaming-original-hook* nil)

(defvar *shutdown-streaming-parked-thread* nil)

(defvar *shutdown-streaming-second-thread* nil)

(defun shutdown-streaming-write-marker (directory name text)
  (let ((temporary (merge-pathnames (concatenate 'string name ".tmp") directory))
        (path (merge-pathnames name directory)))
    (with-open-file (output temporary :direction :output :if-exists :supersede)
      (write-string text output))
    (sb-posix:rename temporary path))
  (values))

(defun shutdown-streaming-install-parked-worker (directory)
  (let ((original *live-operation-before-open-hook*))
    (setf *live-operation-before-open-hook*
          (lambda ()
            (when original (funcall original))
            (when (and *shutdown-finalizing-p*
                       (call-with-live-operation-lock
                        (lambda () (plusp (hash-table-count *live-operation-parked*)))))
              (setf *live-operation-before-open-hook* original)
              (shutdown-streaming-write-marker directory "parked-drain-gate" "waiting")))))
  (setf *shutdown-streaming-parked-thread*
        (sb-thread:make-thread
         (lambda ()
           (handler-case
               (let ((*backend* nil))
                 (with-live-operation
                   (handler-case
                       (progn
                         (call-with-live-operation-parked
                          (lambda ()
                            (shutdown-streaming-write-marker directory "parked-ready" "parked")
                            (loop repeat 1500
                                  when (probe-file (merge-pathnames "release-parked" directory)) return t
                                    do (when (and (call-with-live-operation-lock
                                                   (lambda () *live-operation-terminal-shutdown*))
                                                  (not (probe-file (merge-pathnames "parked-drain-gate" directory))))
                                         (shutdown-streaming-write-marker directory "parked-drain-gate" "terminal"))
                                       (sleep 0.01)
                                  finally (error "parked shutdown worker timed out"))))
                         (shutdown-streaming-write-marker directory "parked-resumed" "resumed"))
                     (pg-error (condition)
                       (if (and (call-with-live-operation-lock
                                 (lambda () *live-operation-terminal-shutdown*))
                                (string= "57P01" (pg-error-sqlstate condition))
                                (fatal-error-p condition))
                           (shutdown-streaming-write-marker directory "parked-terminal" "retired")
                           (error condition))))))
             (error (condition)
               (shutdown-streaming-write-marker directory "parked-error" (princ-to-string condition)))))
         :name "parked shutdown regression worker"))
  (values))

(defun shutdown-streaming-install-feedback-barrier (directory baseline)
  (setf *shutdown-streaming-original-hook* *physical-stream-test-hook*)
  (let ((held nil))
    (setf *physical-stream-test-hook*
          (lambda (point &rest arguments)
            (when *shutdown-streaming-original-hook*
              (apply *shutdown-streaming-original-hook* point arguments))
            (when (and (eq point :before-feedback) (not held))
              (multiple-value-bind (written flushed)
                  (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
                    (values (physical-store-written-lsn *physical-receive-store*)
                            (physical-store-flushed-lsn *physical-receive-store*)))
                (when (> flushed baseline)
                  (assert (zerop *live-operation-depth*))
                  (setf held t)
                  (shutdown-streaming-write-marker directory "flush-held"
                                                   (format nil "~d ~d~%" written flushed))
                  (loop repeat 1500
                        when (probe-file (merge-pathnames "release-feedback" directory)) return t
                          do (sleep 0.01)
                        finally (error "shutdown feedback barrier timed out"))))))))
  (values))

(defun shutdown-streaming-remove-feedback-barrier ()
  (setf *physical-stream-test-hook* *shutdown-streaming-original-hook*
        *shutdown-streaming-original-hook* nil)
  (values))

(defun shutdown-streaming-install-checkpoint-cut (directory)
  (setf *native-control-test-hook*
        (lambda (point)
          (when (eq point :after-primary-shutdown-checkpoint)
            (setf *native-control-test-hook* nil)
            (shutdown-streaming-write-marker directory "checkpoint-cut" "checkpoint")
            (loop repeat 1500
                  when (probe-file (merge-pathnames "release-checkpoint-cut" directory)) return t
                    do (sleep 0.01)
                  finally (error "shutdown checkpoint cut timed out")))))
  (values))

(defun shutdown-streaming-record-outcomes (directory)
  (let ((original *native-control-test-hook*))
    (setf *native-control-test-hook*
          (lambda (point)
            (when original (funcall original point))
            (when (eq point :after-primary-shutdown-clean)
              (shutdown-streaming-write-marker
               directory "sender-outcomes"
               (format nil "~{~a~^ ~}"
                       (mapcar (lambda (token)
                                 (symbol-name (wal-sender-stream-token-outcome token)))
                               (wal-sender-shutdown-plan-tokens *wal-sender-shutdown-plan*))))))))
  (values))

(defun shutdown-streaming-second-parameters (directory)
  (let* ((runtime (copy-physical-follow-runtime *physical-follow-runtime*))
         (cursor (physical-store-flushed-lsn *physical-receive-store*)))
    (setf (physical-follow-runtime-pin runtime) "second_shutdown")
    (shutdown-streaming-write-marker directory "second-query"
                                     (physical-follow-start-query runtime cursor))
    (shutdown-streaming-write-marker
     directory "second-parameters"
     (format nil "~d ~d ~d" (getf *physical-recovery-state* :end)
             (getf *physical-recovery-state* :manifest-digest) cursor)))
  (values))

(defun shutdown-streaming-second-feedback (client written baseline base)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\r))
    (dolist (position (list written baseline base))
      (pg-client-push-u64 payload position))
    (pg-client-push-u64 payload (postgres-current-timestamp))
    (payload-push-byte payload 0)
    (physical-client-feedback client payload)))

(defun shutdown-streaming-start-second-client (directory port base baseline)
  (when (and *shutdown-streaming-second-thread*
             (sb-thread:thread-alive-p *shutdown-streaming-second-thread*))
    (error "secondary shutdown test client is already running"))
  (setf *shutdown-streaming-second-thread*
        (sb-thread:make-thread
         (lambda ()
           (let ((client nil) (written baseline))
             (handler-case
                 (unwind-protect
                      (progn
                        (setf client (pg-client-connect "127.0.0.1" port "postgres"
                                                        :replication-p t
                                                        :application-name "shutdown_second"))
                        (with-open-file (input (merge-pathnames "second-query" directory))
                          (pg-client-start-replication client (read-line input)))
                        (loop
                          (multiple-value-bind (kind payload)
                              (sb-ext:with-timeout 10 (pg-client-replication-frame client))
                            (when (eql kind #\E) (error (pg-client-error-from-payload payload)))
                            (unless (eql kind #\d) (error "secondary test stream ended early"))
                            (when (eql (aref payload 0) (char-code #\w))
                              (setf written (+ (pg-client-u64 payload 1) (- (length payload) 25))))
                            (shutdown-streaming-second-feedback client written baseline base)
                            (shutdown-streaming-write-marker directory "second-ready" "ready")
                            (when (> written baseline)
                              (shutdown-streaming-write-marker directory "second-final" (format nil "~d" written))
                              (loop repeat 1500
                                    when (probe-file (merge-pathnames "release-second" directory)) return t
                                      do (sleep 0.01)
                                    finally (error "secondary test disconnect timed out"))
                              (return)))))
                   (when client (pg-client-close client)))
               (error (condition)
                 (shutdown-streaming-write-marker directory "second-error" (princ-to-string condition))))
             (shutdown-streaming-write-marker directory "second-closed" "closed")))
         :name "secondary shutdown physical stream"))
  (values))
