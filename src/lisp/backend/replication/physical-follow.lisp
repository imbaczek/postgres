;;;; Local receiver/replayer workers. No election, promotion or routing policy.
(in-package #:pg)

(define-condition physical-follow-terminal-error (simple-error) ())
(defstruct physical-follow-runtime host port pin
           (stopped-p nil) (connected-p nil) last-error client receiver-thread replay-thread)
(defvar *physical-follow-runtime* nil)
(defvar *physical-follow-lock* (sb-thread:make-mutex :name "native physical follow lifecycle"))

(defvar *physical-follow-command-lock* (sb-thread:make-mutex :name "native physical follow commands"))

(defvar *physical-follow-statistics* nil
  "Session-tagged receiver telemetry, protected by *PHYSICAL-FOLLOW-LOCK*.")

(defvar *physical-follow-last-end-lsn* 0
  "C's latestWalEnd survives receiver exit/reconnect within a server process.")

(defun physical-follow-connection-description (runtime)
  "Describe the successful native plaintext connection, without libpq-only defaults."
  (let ((host (with-output-to-string (stream)
                (write-char (code-char 39) stream)
                (loop for character across (physical-follow-runtime-host runtime)
                      do (when (member (char-code character) '(39 92))
                           (write-char (code-char 92) stream))
                         (write-char character stream))
                (write-char (code-char 39) stream))))
    (format nil "host=~a port=~d user=postgres dbname=postgres application_name=pg_lisp_physical_receiver replication=database sslmode=disable"
            host (physical-follow-runtime-port runtime))))

(defun physical-follow-monitor-start (runtime cursor)
  "Publish C's connecting state with stale sender details cleared."
  (let ((now (postgres-current-timestamp)))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf *physical-follow-statistics*
            (list :runtime runtime :pid (sb-thread:thread-os-tid sb-thread:*current-thread*)
                  :receive-start-lsn cursor :receive-start-tli 1
                  :last-send-time now :last-receipt-time now
                  :latest-end-lsn *physical-follow-last-end-lsn*
                  :latest-end-time now)
            (physical-follow-runtime-connected-p runtime) nil))))

(defun physical-follow-monitor-connected (runtime)
  "Publish sender details only after native streaming is established."
  (sb-thread:with-mutex (*physical-follow-lock*)
    (when (eq runtime (getf *physical-follow-statistics* :runtime))
      (setf (getf *physical-follow-statistics* :host) (physical-follow-runtime-host runtime)
            (getf *physical-follow-statistics* :port) (physical-follow-runtime-port runtime)
            (getf *physical-follow-statistics* :conninfo) (physical-follow-connection-description runtime)
            (physical-follow-runtime-connected-p runtime) t
            (physical-follow-runtime-last-error runtime) nil))))

(defun physical-follow-monitor-message (runtime end send-time)
  "Match ProcessWalSndrMessage without using source end as local progress."
  (let ((now (postgres-current-timestamp)))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (when (eq runtime (getf *physical-follow-statistics* :runtime))
        (when (< (getf *physical-follow-statistics* :latest-end-lsn) end)
          (setf (getf *physical-follow-statistics* :latest-end-time) send-time))
        (setf *physical-follow-last-end-lsn* end
              (getf *physical-follow-statistics* :latest-end-lsn) end
              (getf *physical-follow-statistics* :last-send-time) send-time
              (getf *physical-follow-statistics* :last-receipt-time) now)))))

(defun physical-follow-monitor-stop (runtime)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (setf (physical-follow-runtime-connected-p runtime) nil)
    (when (eq runtime (getf *physical-follow-statistics* :runtime))
      (setf *physical-follow-statistics* nil))))

(defun physical-follow-statistics ()
  "Copy receiver metadata and real spool write/flush; never infer a live receiver from a spool."
  (let ((store *physical-receive-store*))
    (when store
      ;; Do not wait for a store mutex while holding the lifecycle lock.
      (sb-thread:with-mutex ((physical-store-mutex store))
        (sb-thread:with-mutex (*physical-follow-lock*)
          (let* ((runtime *physical-follow-runtime*)
                 (statistics *physical-follow-statistics*))
            (when (and runtime (eq runtime (getf statistics :runtime)))
              (append (copy-list statistics)
                      (list :status (cond ((physical-follow-runtime-stopped-p runtime) "stopping")
                                          ((physical-follow-runtime-connected-p runtime) "streaming")
                                          (t "connecting"))
                            :written-lsn (physical-store-written-lsn store)
                            :flushed-lsn (physical-store-flushed-lsn store)
                            :received-tli (physical-store-timeline store))))))))))

(defparameter *physical-receive-flush-bytes* (* 1024 1024)
  "Batch native flushes while busy; a keepalive flushes all completed records.")

(defun physical-follow-reject (message)
  (error 'physical-follow-terminal-error :format-control "~a" :format-arguments (list message)))

(defun physical-follow-config-path ()
  (merge-pathnames "physical-follow.control" *pgdata*))

(defun physical-follow-check-config (config)
  (unless (and (listp config) (= 4 (length config)) (eql 1 (first config))
               (stringp (second config)) (<= 1 (length (second config)) 255)
               (notany (lambda (char) (< (char-code char) 32)) (second config))
               (typep (third config) '(integer 1 65535))
               (stringp (fourth config)) (wal-valid-replication-slot-name-p (fourth config)))
    (error "invalid native physical follow configuration"))
  config)

(defun physical-follow-save-config (config)
  (physical-follow-check-config config)
  (let* ((text (physical-bootstrap-manifest-text config))
         (crc (wal-crc32 (string->bytes text))))
    (wal-atomic-write-bytes (physical-follow-config-path)
                            (string->bytes (format nil "PGPHYSICALFOLLOW1 ~8,'0X~%~a~%" crc text)))))

(defun physical-follow-read-config ()
  (let* ((bytes (recovery-read-bytes (physical-follow-config-path) :maximum 4096))
         (text (bytes->string bytes))
         (newline (position #\Newline text))
         (prefix "PGPHYSICALFOLLOW1 "))
    (unless (and newline (= newline (+ (length prefix) 8))
                 (uiop:string-prefix-p prefix text))
      (error "corrupt native physical follow envelope"))
    (let* ((body (subseq text (1+ newline)))
           (crc (parse-integer text :start (length prefix) :end newline :radix 16)))
      ;; The final newline is outside the checksummed literal, like base manifests.
      (unless (and (plusp (length body)) (char= #\Newline (char body (1- (length body))))
                   (= crc (wal-crc32 (string->bytes (subseq body 0 (1- (length body)))))))
        (error "native physical follow checksum mismatch"))
      (physical-follow-check-config (recovery-parse-manifest-text body)))))

(defun physical-follow-feedback (client)
  "Snapshot independent write/flush/apply, then perform socket I/O outside admission."
  (when *physical-stream-test-hook* (funcall *physical-stream-test-hook* :before-feedback))
  (let ((payload (with-live-operation
                   (let ((store *physical-receive-store*) (result (make-payload)))
                     (check-recovery-available)
                     (sb-thread:with-mutex ((physical-store-mutex store))
                       (physical-store-check-live store)
                       (payload-push-byte result (char-code #\r))
                       (pg-client-push-u64 result (physical-store-written-lsn store))
                       (pg-client-push-u64 result (physical-store-flushed-lsn store))
                       (pg-client-push-u64 result *last-replay-lsn*)
                       (pg-client-push-u64 result (postgres-current-timestamp))
                       (payload-push-byte result 0))
                     result))))
    (sb-ext:with-timeout 10
      (write-message (pg-client-stream client) #\d payload))))

(defun physical-follow-process-frame (payload &optional runtime)
  (unless (and payload (plusp (length payload))) (physical-follow-reject "empty native physical frame"))
  (case (code-char (aref payload 0))
    (#\w
     (unless (<= 26 (length payload) (+ 25 65536))
       (physical-follow-reject "invalid native XLogData size"))
     (let ((start (pg-client-u64 payload 1)) (end (pg-client-u64 payload 9))
                                             (bytes (subseq payload 25)))
       (unless (<= (+ start (length bytes)) end #xffffffffffffffff)
         (physical-follow-reject "native XLogData exceeds durable source end"))
       (when runtime
         (physical-follow-monitor-message runtime end (pg-client-i64 payload 17)))
       (physical-recovery-receive start bytes)
       ;; Match walreceiver's write-then-flush cycle rather than forcing a
       ;; manifest fsync for every tiny physical record. Feedback still exposes
       ;; written versus durable flush independently.
       (when (>= (- (physical-store-written-lsn *physical-receive-store*)
                    (physical-store-flushed-lsn *physical-receive-store*))
                 *physical-receive-flush-bytes*)
         (physical-recovery-flush))))
    (#\k
     (unless (and (= (length payload) 18) (member (aref payload 17) '(0 1)))
       (physical-follow-reject "invalid native keepalive"))
     (when runtime
       (physical-follow-monitor-message runtime (pg-client-u64 payload 1) (pg-client-i64 payload 9)))
     ;; Caught-up traffic must become durable even below the batching threshold.
     ;; The source WAL-end hint is never substituted for the local write cursor.
     (physical-recovery-flush))
    (otherwise (physical-follow-reject "unsupported native physical message"))))

(defun physical-follow-start-query (runtime cursor)
  (with-live-operation
    (format nil
            "START_REPLICATION SLOT ~a PHYSICAL ~a (native_format '~a', system_id '~d', timeline '1', base_lsn '~a', prefix_digest '~d')"
            (physical-follow-runtime-pin runtime) (wal-format-lsn cursor)
            +physical-native-format+ (getf *physical-recovery-state* :system-id)
            (wal-format-lsn (getf *physical-recovery-state* :end))
            (physical-store-flushed-digest *physical-receive-store*))))

(defun physical-follow-check-identity (rows)
  (unless (and (= (length rows) 1) (= (length (first rows)) 4)
               (equal (aref (first rows) 0)
                      (format nil "~d" (getf *physical-recovery-state* :system-id)))
               (equal (aref (first rows) 1) "1"))
    (physical-follow-reject "source identity or timeline differs from the native recovery base")))

(defun physical-follow-session (runtime cursor)
  (let ((client nil))
    (unwind-protect
         (progn
           (physical-follow-monitor-start runtime cursor)
           (setf client (sb-ext:with-timeout 10
                          (pg-client-connect (physical-follow-runtime-host runtime)
                                             (physical-follow-runtime-port runtime) "postgres"
                                             :application-name "pg_lisp_physical_receiver"
                                             :replication-p t)))
           ;; Match libpq connectNoDelay before stop-and-wait status traffic.
           (setf (sb-bsd-sockets:sockopt-tcp-nodelay (pg-client-socket client)) t)
           (sb-thread:with-mutex (*physical-follow-lock*)
             (when (physical-follow-runtime-stopped-p runtime)
               (return-from physical-follow-session))
             (setf (physical-follow-runtime-client runtime) client))
           ;; Waiting for replay admission is not a network deadline and must
           ;; never be interrupted by the transport timer.
           (let ((query (physical-follow-start-query runtime cursor)))
             (sb-ext:with-timeout 10
               (physical-follow-check-identity (pg-client-query client "IDENTIFY_SYSTEM"))
               (pg-client-start-replication client query)))
           (physical-follow-monitor-connected runtime)
           (loop until (physical-follow-runtime-stopped-p runtime)
                 do (multiple-value-bind (type payload)
                        (sb-ext:with-timeout 10 (pg-client-replication-frame client))
                      (case type
                        (#\d (with-live-operation (physical-follow-process-frame payload runtime)))
                        (#\E (error (pg-client-error-from-payload payload)))
                        (otherwise (physical-follow-reject "unexpected native replication frame"))))
                    (physical-follow-feedback client)))
      (physical-follow-monitor-stop runtime)
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when (eq client (physical-follow-runtime-client runtime))
          (setf (physical-follow-runtime-client runtime) nil)))
      (pg-client-close client))))

(defun physical-follow-terminal-p (condition)
  (or (typep condition 'physical-follow-terminal-error)
      *physical-recovery-failed-p*
      (and *physical-receive-store* (physical-store-failed-p *physical-receive-store*))
      (and (typep condition 'pg-client-error)
           (not (member (pg-client-error-sqlstate condition) '("57P01" "57P03" "53300" "55006")
                        :test #'equal)))))

(defun physical-follow-note-error (runtime condition &optional terminal)
  (setf (physical-follow-runtime-last-error runtime) (princ-to-string condition))
  (when terminal (setf (physical-follow-runtime-stopped-p runtime) t))
  (admin-log "native physical receiver ~a: ~a" (if terminal "stopped" "retrying")
             (physical-follow-runtime-last-error runtime)))

(defun physical-follow-receiver-loop (runtime)
  (loop until (or (physical-follow-runtime-stopped-p runtime) (shutdown-in-progress-p))
        do (handler-case
               (let ((cursor (with-live-operation
                               (check-recovery-available)
                               (physical-store-reset-unflushed *physical-receive-store*))))
                 (physical-follow-session runtime cursor))
             ((or error sb-ext:timeout) (condition)
               (unless (physical-follow-runtime-stopped-p runtime)
                 (physical-follow-note-error runtime condition (physical-follow-terminal-p condition)))))
           (unless (physical-follow-runtime-stopped-p runtime) (sleep 0.2))))

(defun physical-follow-replay-loop (runtime)
  (loop until (or (physical-follow-runtime-stopped-p runtime) (shutdown-in-progress-p))
        do (handler-case
               (when (and (not (sb-thread:with-mutex (*physical-replay-pause-lock*)
                                 *physical-replay-paused-p*))
                          (< *last-replay-lsn* (physical-store-flushed-lsn *physical-receive-store*)))
                 (physical-recovery-replay))
             (error (condition)
               (physical-follow-note-error runtime condition t)))
           (sleep 0.05)))

(defun physical-follow-launch (config)
  "Called with the command mutex held; publish only a complete worker pair."
  (physical-follow-check-config config)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (when *physical-follow-runtime* (error "native physical follow already has a runtime; stop it first"))
    (let ((runtime (make-physical-follow-runtime :host (second config) :port (third config)
                                                 :pin (fourth config)))
          (ready nil) (success nil))
      (unwind-protect
           (flet ((run (function)
                    (loop until ready do (sleep 0.01))
                    (unless (physical-follow-runtime-stopped-p runtime)
                      (funcall function runtime))))
             (setf (physical-follow-runtime-receiver-thread runtime)
                   (sb-thread:make-thread (lambda () (run #'physical-follow-receiver-loop))
                                          :name "native physical WAL receiver")
                   (physical-follow-runtime-replay-thread runtime)
                   (sb-thread:make-thread (lambda () (run #'physical-follow-replay-loop))
                                          :name "native physical WAL replay")
                   *physical-follow-runtime* runtime success t ready t)
             :started)
        (unless success
          ;; No worker can touch storage before the complete pair is published.
          (setf (physical-follow-runtime-stopped-p runtime) t ready t))))))

(defun physical-follow-start (host port pin)
  "Persist a native source endpoint and run receipt/replay; never authorize a writer."
  (when (plusp *live-operation-depth*) (error "follow start requires its own operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (let ((config (physical-follow-check-config (list 1 host port pin))))
      (when *physical-follow-runtime* (error "stop the existing native physical follow runtime first"))
      (when (eq :deferred (physical-recovery-enable-streaming))
        (return-from physical-follow-start :deferred))
      (with-live-operation (physical-follow-save-config config))
      (physical-follow-launch config))))

(defun physical-follow-stop ()
  "Disable automatic receipt durably; keep recovery role, spool and applied data."
  (when (plusp *live-operation-depth*) (error "follow stop must join outside operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (with-live-operation
      (when (probe-file (physical-follow-config-path))
        (delete-file (physical-follow-config-path))
        (fsync-directory *pgdata*)))
    (let ((runtime *physical-follow-runtime*))
      (when runtime
        (setf (physical-follow-runtime-stopped-p runtime) t)
        (let ((client (sb-thread:with-mutex (*physical-follow-lock*)
                        (physical-follow-runtime-client runtime))))
          (when client
            (ignore-errors
             (sb-bsd-sockets:socket-shutdown (pg-client-socket client) :direction :io))))
        (let ((threads (list (physical-follow-runtime-receiver-thread runtime)
                             (physical-follow-runtime-replay-thread runtime))))
          (dolist (thread threads)
            (when thread (sb-thread:join-thread thread :timeout 15 :default :timeout)))
          (when (some (lambda (thread) (and thread (sb-thread:thread-alive-p thread))) threads)
            (error "native physical workers have not stopped")))
        (sb-thread:with-mutex (*physical-follow-lock*) (setf *physical-follow-runtime* nil)))))
  :stopped)

(defun physical-follow-autostart ()
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (when (probe-file (physical-follow-config-path))
      (unless (and (recovery-in-progress-p) *physical-receive-store*)
        (error "native follow configuration requires streaming recovery"))
      (unless *physical-follow-runtime*
        (physical-follow-launch (physical-follow-read-config))))))
