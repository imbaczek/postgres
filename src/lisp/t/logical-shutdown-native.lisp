(in-package #:pg)

(load (merge-pathnames "shutdown-streaming-native.lisp" *load-truename*))

(defun logical-shutdown-native-record (directory name)
  (multiple-value-bind (rows streams stage)
      (call-with-wal-sender-state-lock
       (lambda ()
         (values
          (loop for token in (and *wal-sender-shutdown-plan*
                                  (wal-sender-shutdown-plan-tokens *wal-sender-shutdown-plan*))
                for owner = (wal-sender-stream-token-owner token)
                when (eq :logical (wal-sender-stream-token-kind token))
                  collect (list (logical-shutdown-owner-name owner)
                                (wal-sender-stream-token-kind token)
                                (wal-sender-stream-token-state token)
                                (if (wal-sender-stream-token-stopped-p token) 1 0)
                                (or (wal-sender-stream-token-limit token) -1)
                                (or (wal-sender-stream-token-target token) -1)
                                (or (wal-sender-stream-token-outcome token) :none)
                                (logical-shutdown-owner-slot owner)))
          (hash-table-count *wal-sender-streams*)
          (if *wal-sender-shutdown-plan*
              (wal-sender-shutdown-plan-stage *wal-sender-shutdown-plan*) :none))))
    (multiple-value-bind (storage slots)
        (sb-thread:with-mutex (*wal-lock*)
          (values
           (list (getf *wal-control-metadata* :db-state) *wal-end-lsn* *wal-durable-lsn*
                 *wal-checkpoint-lsn* (if *wal-pending-page-publication* 1 0)
                 (loop for slot being the hash-values of *wal-replication-slots*
                       count (wal-replication-slot-active-owner slot)))
           (loop for row in rows
                 for slot = (eighth row)
                 for disk = (wal-read-replication-slot (wal-replication-slot-path (first row)))
                 collect (append (subseq row 0 7)
                                 (list (or (wal-replication-slot-confirmed-flush-lsn slot) -1)
                                       (if (wal-replication-slot-active-owner slot) 1 0)
                                       (or (wal-replication-slot-confirmed-flush-lsn disk) -1))))))
      (let ((clients (sb-thread:with-mutex (*backend-registry-lock*)
                       (list *n-connections* *n-backends*)))
            (senders (sb-thread:with-mutex (*wal-sender-lock*)
                       (hash-table-count *wal-sender-registry*))))
        (when (string= name "clean")
          (assert (= +db-shutdowned+ (first storage)))
          (assert (every #'zerop (append clients (list senders streams (sixth storage)))))
          (assert (every (lambda (row) (and (eq :released (third row)) (zerop (ninth row))
                                            (= (eighth row) (tenth row)))) slots)))
        (shutdown-streaming-write-marker
         directory name
         (with-output-to-string (output)
           (format output "~{~a~^ ~}~%"
                   (append (subseq storage 0 5) clients
                           (list senders streams (sixth storage) stage)))
           (dolist (row slots) (format output "~{~a~^ ~}~%" row)))))))
  (values))

(defun logical-shutdown-native-install (directory)
  (let ((original *native-control-test-hook*))
    (setf *native-control-test-hook*
          (lambda (point)
            (when original (funcall original point))
            (case point
              (:after-primary-shutdown-dirty (logical-shutdown-native-record directory "dirty"))
              (:after-primary-shutdown-checkpoint (logical-shutdown-native-record directory "checkpoint"))
              (:after-primary-shutdown-clean (logical-shutdown-native-record directory "clean"))))))
  (let ((send (symbol-function 'standard-send-socket-chunk))
        (blocked nil) (block-recorded nil) (partial-recorded nil) (drain-recorded nil)
        (buffers-restored nil))
    (setf (symbol-function 'standard-send-socket-chunk)
          (lambda (socket bytes)
            (let ((count (funcall send socket bytes)))
              (when (and (or (null count) (zerop count))
                         (probe-file (merge-pathnames "arm-output" directory)))
                (setf blocked t))
              count)))
    (dolist (symbol '(standard-poll-feedback logical-stream-poll))
      (let ((original (symbol-function symbol)))
        (setf (symbol-function symbol)
              (lambda (state)
                (multiple-value-prog1 (funcall original state)
                  (assert (zerop *live-operation-depth*))
                  (when (and blocked (not block-recorded))
                    (setf block-recorded t)
                    (shutdown-streaming-write-marker directory "backpressure" "socket-send returned EAGAIN"))
                  (when (and (not partial-recorded)
                             (probe-file (merge-pathnames "arm-partial" directory))
                             (< 0 (standard-stream-input-count state) 39))
                    (setf partial-recorded t)
                    (shutdown-streaming-write-marker
                     directory "partial" (format nil "~d" (standard-stream-input-count state))))
                  (when (and (not buffers-restored)
                             (probe-file (merge-pathnames "release-output" directory)))
                    (setf (sb-bsd-sockets:sockopt-send-buffer (standard-stream-socket state)) 262144
                          buffers-restored t))
                  (when (and (not drain-recorded)
                             (call-with-wal-sender-state-lock
                              (lambda () (and *wal-sender-shutdown-plan*
                                              (eq :draining (wal-sender-shutdown-plan-stage
                                                             *wal-sender-shutdown-plan*))))))
                    (setf drain-recorded t)
                    (logical-shutdown-native-record directory "draining"))))))))
  (values))

(defun logical-shutdown-native-small-output ()
  (let ((sockets
          (call-with-wal-sender-state-lock
           (lambda ()
             (loop for token being the hash-values of *wal-sender-streams*
                   when (eq :logical (wal-sender-stream-token-kind token))
                     collect (backend-socket (wal-sender-stream-token-backend token)))))))
    (assert (= 1 (length sockets)))
    (setf (sb-bsd-sockets:sockopt-send-buffer (first sockets)) 4096))
  (values))

(defun logical-shutdown-native-pause-writer (directory)
  (assert (null *primary-wal-writer-test-hook*))
  (setf *primary-wal-writer-test-hook*
        (lambda (point writer)
          (when (eq point :before-cycle)
            (assert (zerop *live-operation-depth*))
            (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
            (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
            (setf *primary-wal-writer-test-hook* nil)
            (shutdown-streaming-write-marker directory "writer-ready" "paused")
            (loop with deadline = (+ (get-internal-real-time) (* 30 internal-time-units-per-second))
                  with stop-recorded = nil
                  until (probe-file (merge-pathnames "release-writer" directory))
                  do (when (and (not stop-recorded) (primary-wal-writer-stopping-p writer))
                       (setf stop-recorded t)
                       (shutdown-streaming-write-marker directory "writer-stopping" "stop requested"))
                     (when (>= (get-internal-real-time) deadline)
                       (error "logical shutdown writer barrier timed out"))
                     (sleep 0.01)))))
  (values))

(defun logical-shutdown-native-record-async (directory)
  (let ((facts
          (sb-thread:with-mutex (*wal-lock*)
            (assert *wal-pending-page-publication*)
            (assert (> *wal-end-lsn* *wal-durable-lsn*))
            (list *wal-end-lsn* *wal-durable-lsn*))))
    (shutdown-streaming-write-marker directory "async" (format nil "~{~d~^ ~}" facts)))
  (values))

(defun logical-shutdown-native-install-failure (directory)
  (let ((pause (symbol-function 'wal-test-pause))
        (fail-stop (symbol-function 'wal-fail-stop))
        (failure (make-condition 'simple-error :format-control "owned incomplete logical batch"))
        (reported nil))
    (setf (symbol-function 'wal-test-pause)
          (lambda (point)
            (when (eq point :after-page-images) (error failure))
            (funcall pause point))
          (symbol-function 'wal-fail-stop)
          (lambda (condition)
            (when (and (eq condition failure) (not reported))
              (assert *wal-poisoned*)
              (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
              (setf reported t)
              (shutdown-streaming-write-marker directory "failed-batch" (princ-to-string condition)))
            (funcall fail-stop condition))))
  (values))
