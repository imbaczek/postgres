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
                  :receive-start-lsn cursor :receive-start-tli (getf *physical-recovery-state* :timeline)
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
                            :received-tli (physical-store-prefix-timeline store (physical-store-flushed-lsn store)))))))))))

(defparameter *physical-receive-flush-bytes* (* 1024 1024)
  "Batch native flushes while busy; a keepalive flushes all completed records.")

(defun physical-follow-reject (message)
  (error 'physical-follow-terminal-error :format-control "~a" :format-arguments (list message)))

(defun physical-follow-config-path ()
  (merge-pathnames "physical-follow.control" *pgdata*))

(defconstant +physical-follow-config-maximum-bytes+
  (+ (* 4 +physical-store-history-maximum-bytes+) 65536)
  "Maximum config envelope size before parsing, including bounded histories.")

(defparameter *physical-follow-binding-keys*
  '(:version :phase :anchor :child-history :before :after :applied :required :recovery)
  "Exact keys of the optional version 1 binding.")

(defun physical-follow-binding-pair-p (pair)
  "Recognize a bounded (LSN timeline) pair."
  (and (listp pair) (= 2 (length pair))
       (typep (first pair) '(unsigned-byte 64))
       (typep (second pair) '(integer 1 4294967295))))

(defun physical-follow-binding-reference-p (timeline digest)
  "Recognize an absent or complete selected-history reference."
  (or (and (null timeline) (null digest))
      (and (typep timeline '(integer 2 4294967295))
           (typep digest '(unsigned-byte 64)))))

(defun physical-follow-binding-control-check (tuple)
  "Check the representation and internal bounds of a receive-control tuple.
Use PHYSICAL-STORE-CONTROL-TUPLE's field order; no durable file is read here."
  (unless (and (listp tuple) (= 11 (length tuple))
               (every (lambda (index) (typep (nth index tuple) '(unsigned-byte 64)))
                      '(0 2 3 4 5 6 9))
               (typep (second tuple) '(integer 1 4294967295))
               (<= (third tuple) (sixth tuple) (fourth tuple))
               (or (/= (third tuple) (sixth tuple)) (= (seventh tuple) #xCBF29CE484222325))
               (physical-follow-binding-reference-p (eighth tuple) (ninth tuple))
               (or (null (nth 10 tuple)) (typep (nth 10 tuple) '(unsigned-byte 64))))
    (error "invalid follow receive-control witness"))
  tuple)

(defun physical-follow-binding-recovery-check (witness before applied)
  "Check the named phase-2 recovery witness against the saved tuple and applied pair.
Identity, checkpoint, observed XID, old history reference and restartpoint token
still require comparison with durable recovery state."
  (physical-follow-check-plist
   witness '(:phase :cluster-identity :checkpoint :observed-next-xid
             :history-target :history-digest :restartpoint))
  (let ((identity (getf witness :cluster-identity))
        (checkpoint (getf witness :checkpoint))
        (observed (getf witness :observed-next-xid))
        (point (getf witness :restartpoint)))
    (unless (and (eql 2 (getf witness :phase))
                 (stringp identity) (<= 1 (length identity) 4096)
                 (typep checkpoint '(unsigned-byte 64)) (<= checkpoint (third before))
                 (typep observed '(integer 3 18446744073709551615))
                 (physical-follow-binding-reference-p (getf witness :history-target)
                                                      (getf witness :history-digest))
                 (if point
                     (and (listp point) (= 2 (length point))
                          (every (lambda (value) (typep value '(unsigned-byte 64))) point)
                          (<= (sixth before) (first point) (first applied)))
                     (= (sixth before) (third before))))
      (error "invalid follow recovery witness"))))

(defun physical-follow-binding-selection-digest (anchor child history before digest)
  "Derive the candidate selection digest from the anchor and immediate child history."
  (physical-store-update-digest
   #xCBF29CE484222325
   (physical-store-history-image
    (list :version 1 :system-id (getf anchor :system-id)
          :base-timeline (getf anchor :base-timeline) :base (getf anchor :base)
          :target child :history-text history :fork-digest digest
          :previous-digest (ninth before) :eor-octets (getf anchor :eor-octets)))))

(defun physical-follow-binding-transition-check (anchor text before after)
  "Check the catch-up or rewind plan and its immediate child history.
The source history can extend beyond that child. Preserve the base, floor,
manifest and root fields, and validate the proposed selection digest."
  (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
    (let* ((child (timeline-check-end-of-recovery eor (getf anchor :receive-timeline) history))
           (child-history (timeline-check-lineage (timeline-parse-history child text))))
      (unless (and (physical-store-history-extends-p child-history history)
                   (= (or (eighth before) (second before)) (getf anchor :receive-timeline))
                   (= (min (fourth before) fork) (getf anchor :receive-lsn))
                   (every (lambda (index) (eql (nth index before) (nth index after))) '(0 1 2 5 6 9 10)))
        (error "follow binding changes immutable provenance or source ancestry"))
      (if (equal before after)
          (unless (and (<= (fourth before) fork)
                       (= (fifth before) (getf anchor :receive-digest)))
            (error "follow binding retained an unapplied divergent tail"))
          (unless (and (<= fork (fourth before)) (= (fourth after) fork)
                       (= (fifth after) (getf anchor :receive-digest))
                       (eql (eighth after) child)
                       (eql (ninth after)
                            (physical-follow-binding-selection-digest anchor child text before (fifth after))))
            (error "follow binding has a foreign post-transition selection")))
      child-history)))

(defun physical-follow-binding-check (binding)
  "Validate the shape and internal coherence of a version 1 follow binding.
The anchor, immediate child history, before and after control tuples, applied and
required pairs and recovery witness describe a plan, not durable proof.
Neither binding phase is admitted by the current receiver."
  (physical-follow-check-plist binding *physical-follow-binding-keys*)
  (let* ((anchor (getf binding :anchor)) (text (getf binding :child-history))
                                         (before (physical-follow-binding-control-check (getf binding :before)))
                                         (after (physical-follow-binding-control-check (getf binding :after)))
                                         (applied (getf binding :applied)) (required (getf binding :required))
                                         (witness (getf binding :recovery)))
    (unless (and (eql 1 (getf binding :version)) (member (getf binding :phase) '(:pending :active))
                 (stringp text) (<= (length (string->bytes text)) +physical-store-history-maximum-bytes+)
                 (stringp (getf anchor :history-text))
                 (<= (length (string->bytes (getf anchor :history-text))) +physical-store-history-maximum-bytes+)
                 (physical-follow-binding-pair-p applied) (physical-follow-binding-pair-p required)
                 (= (first before) (getf anchor :system-id))
                 (= (second before) (getf anchor :base-timeline))
                 (= (third before) (getf anchor :base))
                 (= (nth 9 before) (getf anchor :manifest-digest))
                 (<= (third before) (first required) (first applied) (getf anchor :receive-lsn))
                 (<= (sixth before) (first applied))
                 (<= (sixth before) (getf anchor :receive-lsn)))
      (error "invalid native physical follow binding"))
    (let ((history (physical-follow-binding-transition-check anchor text before after)))
      (dolist (pair (list applied required))
        (unless (and (timeline-prefix-boundary-p (second pair) (first pair) history)
                     (= (second pair) (if (= (first pair) (getf anchor :base))
                                          (getf anchor :base-timeline)
                                          (timeline-at-point (1- (first pair)) history))))
          (error "follow recovery frontier has a foreign timeline"))))
    (physical-follow-binding-recovery-check witness before applied)
    (when (getf witness :history-target)
      (unless (<= (getf witness :history-target) (getf anchor :receive-timeline))
        (error "follow recovery history witness is ahead of the old receive selection"))))
  binding)

(defun physical-follow-check-config (config)
  "Check a version 1 bootstrap config or its optional fifth binding field.
A valid binding does not authorize starting a bound receiver."
  (unless (and (listp config) (member (length config) '(4 5)) (eql 1 (first config))
               (stringp (second config)) (<= 1 (length (second config)) 255)
               (notany (lambda (char) (< (char-code char) 32)) (second config))
               (typep (third config) '(integer 1 65535))
               (stringp (fourth config)) (wal-valid-replication-slot-name-p (fourth config)))
    (error "invalid native physical follow configuration"))
  (when (= 5 (length config)) (physical-follow-binding-check (fifth config)))
  config)

(defun physical-follow-config-image (config)
  "Encode a checked config in the existing bounded PGPHYSICALFOLLOW1 envelope."
  (physical-follow-check-config config)
  (let* ((text (physical-bootstrap-manifest-text config))
         (crc (wal-crc32 (string->bytes text)))
         (bytes (string->bytes (format nil "PGPHYSICALFOLLOW1 ~8,'0X~%~a~%" crc text))))
    (unless (<= (length bytes) +physical-follow-config-maximum-bytes+)
      (error "native physical follow configuration exceeds its reopen bound"))
    bytes))

(defun physical-follow-save-config (config)
  "Atomically write a checked config image."
  (wal-atomic-write-bytes (physical-follow-config-path) (physical-follow-config-image config)))

(defun physical-follow-decode-config (bytes)
  "Check the bounded envelope and checksum, then validate its literal payload.
Validation checks coherence, not durable state or source proof."
  (unless (<= (length bytes) +physical-follow-config-maximum-bytes+)
    (error "native physical follow configuration exceeds its reopen bound"))
  (let* ((text (bytes->string bytes)) (newline (position #\Newline text))
                                      (prefix "PGPHYSICALFOLLOW1 "))
    (unless (and newline (= newline (+ (length prefix) 8)) (uiop:string-prefix-p prefix text))
      (error "corrupt native physical follow envelope"))
    (let* ((body (subseq text (1+ newline)))
           (crc (parse-integer text :start (length prefix) :end newline :radix 16)))
      (unless (and (plusp (length body)) (char= #\Newline (char body (1- (length body))))
                   (= crc (wal-crc32 (string->bytes (subseq body 0 (1- (length body)))))))
        (error "native physical follow checksum mismatch"))
      (physical-follow-check-config (recovery-parse-manifest-text body)))))

(defun physical-follow-read-config ()
  "Read the whole bounded config file before decoding it."
  (physical-follow-decode-config
   (recovery-read-bytes (physical-follow-config-path) :maximum +physical-follow-config-maximum-bytes+)))

(defun physical-follow-require-bootstrap-config (config)
  "Reject either binding phase until reconciliation and bound receiving exist."
  (physical-follow-check-config config)
  (when (= 5 (length config))
    (error "native follow binding requires reconciliation and bound receiver support"))
  config)

(defun physical-follow-check-startup-config ()
  "Reject a saved binding before startup can mutate storage or begin redo."
  (when (probe-file (physical-follow-config-path))
    (physical-follow-require-bootstrap-config (physical-follow-read-config))))

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
  "Build a native stream request for the saved timeline, base and receive prefix.
Include the verified base manifest digest when it is available."
  (with-live-operation
    (format nil
            "START_REPLICATION SLOT ~a PHYSICAL ~a (native_format '~a', system_id '~d', timeline '~d', base_lsn '~a', prefix_digest '~d'~a)"
            (physical-follow-runtime-pin runtime) (wal-format-lsn cursor)
            +physical-native-format+ (getf *physical-recovery-state* :system-id)
            (getf *physical-recovery-state* :timeline)
            (wal-format-lsn (getf *physical-recovery-state* :end))
            (physical-store-flushed-digest *physical-receive-store*)
            (if (getf *physical-recovery-state* :manifest-digest)
                (format nil ", manifest_digest '~d'" (getf *physical-recovery-state* :manifest-digest))
                ""))))

(defun physical-follow-check-identity (rows)
  (unless (and (= (length rows) 1) (= (length (first rows)) 4)
               (equal (aref (first rows) 0)
                      (format nil "~d" (getf *physical-recovery-state* :system-id)))
               (equal (aref (first rows) 1)
                      (format nil "~d" (getf *physical-recovery-state* :timeline))))
    (physical-follow-reject "source identity or timeline differs from the native recovery base")))

(defun physical-follow-flush-on-exit (runtime)
  "Flush complete writes before receiver exit; record fatal cleanup errors without masking an unwind."
  (with-live-operation
    (when *physical-recovery-failed-p*
      (return-from physical-follow-flush-on-exit :failed))
    (handler-case
        (let ((store *physical-receive-store*))
          (unless store (error "receiver exit lost its native receive spool"))
          (multiple-value-bind (failed written flushed)
              (sb-thread:with-mutex ((physical-store-mutex store))
                (values (or (physical-store-failed-p store) (physical-store-closed-p store))
                        (physical-store-written-lsn store) (physical-store-flushed-lsn store)))
            (cond (failed :failed)
                  ((= written flushed) flushed)
                  (t (physical-recovery-flush)))))
      (error (condition)
        ;; C's WAL fsync failure is PANIC. Keep this native recovery image
        ;; closed until restart, even if a stop request is pending.
        (setf *physical-recovery-failed-p* t)
        (physical-follow-note-error runtime condition t)
        :failed))))

(defun physical-follow-check-exit ()
  "A joined receiver must not turn poisoned receive storage into a successful stop."
  (with-live-operation
    (check-recovery-available)
    (unless *physical-receive-store* (error "receiver exit lost its native receive spool"))
    (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
      (physical-store-check-live *physical-receive-store*))))

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
      (unwind-protect
           (physical-follow-flush-on-exit runtime)
        (physical-follow-monitor-stop runtime)
        (sb-thread:with-mutex (*physical-follow-lock*)
          (when (eq client (physical-follow-runtime-client runtime))
            (setf (physical-follow-runtime-client runtime) nil)))
        (pg-client-close client)))))

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
               (let ((terminal (physical-follow-terminal-p condition)))
                 (when (or (not (physical-follow-runtime-stopped-p runtime))
                           (and terminal (not *physical-recovery-failed-p*)))
                   (physical-follow-note-error runtime condition terminal)))))
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
  "Launch a four-field bootstrap config with the command mutex held.
Reject bindings before creating workers; publish only a complete worker pair."
  (check-physical-recovery-worker-start)
  (physical-follow-require-bootstrap-config config)
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
  "Persist a four-field bootstrap endpoint and run receipt and replay.
Reject a saved binding before replacing its config; never authorize a writer."
  (when (plusp *live-operation-depth*) (error "follow start requires its own operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (check-physical-recovery-worker-start)
    (physical-follow-check-startup-config)
    (let ((config (physical-follow-check-config (list 1 host port pin))))
      (when *physical-follow-runtime* (error "stop the existing native physical follow runtime first"))
      (when (eq :deferred (physical-recovery-enable-streaming))
        (return-from physical-follow-start :deferred))
      (with-live-operation (physical-follow-save-config config))
      (physical-follow-launch config))))

(defun physical-follow-quiesce-runtime ()
  "Caller owns the command mutex, outside admission; preserve durable follow configuration."
  (when (plusp *live-operation-depth*) (error "follow quiescence must join outside admission"))
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
      ;; Older live stack frames may retain their pre-flush cleanup.
      (physical-follow-flush-on-exit runtime)
      (physical-follow-check-exit)
      (sb-thread:with-mutex (*physical-follow-lock*) (setf *physical-follow-runtime* nil))))
  :stopped)

(defun physical-follow-stop ()
  "Disable bootstrap receipt durably; keep recovery role, spool and applied data.
Reject a saved binding before deleting its config or stopping workers."
  (when (plusp *live-operation-depth*) (error "follow stop must join outside operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (with-live-operation
      (check-physical-recovery-work)
      (physical-follow-check-startup-config)
      (when (probe-file (physical-follow-config-path))
        (delete-file (physical-follow-config-path))
        (fsync-directory *pgdata*)))
    (physical-follow-quiesce-runtime)))

(defun physical-recovery-select-history (target history-text eor-bytes)
  "Select receive history under follow-command, reload and quiescence exclusion.
Follow must already be stopped. The caller must independently verify remote
system identity, exact prefix and retention before invoking this private control.
A failure after selection work begins fences recovery; this does not admit a network source."
  (when (plusp *live-operation-depth*) (error "history selection requires its own operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (check-physical-recovery-worker-start)
    (when *physical-follow-runtime* (error "stop native follow before selecting receive history"))
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (check-physical-recovery-worker-start)
        (unless (and *physical-receive-store* (eql 2 (getf *physical-recovery-state* :phase)))
          (error "history selection requires streaming recovery"))
        (let ((history (timeline-check-lineage (timeline-parse-history target history-text)))
              (finished nil))
          (unless (physical-store-history-extends-p
                   (timeline-read-history (getf *physical-recovery-state* :timeline)) history)
            (error "receive history changes immutable base ancestry"))
          (unwind-protect
               (progn
                 (physical-store-select-history *physical-receive-store* target history-text eor-bytes)
                 (physical-recovery-publish-selected-history *physical-receive-store*)
                 (let ((candidate (physical-recovery-attach-selected-store
                                   *physical-recovery-state* *physical-receive-store*)))
                   (setf *physical-recovery-state*
                         (physical-recovery-write-control candidate 2 *last-replay-lsn* *next-xid*)))
                 (setf finished t)
                 target)
            (unless finished (setf *physical-recovery-failed-p* t))))))))

(defun physical-follow-autostart ()
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (check-physical-recovery-worker-start)
    (when (probe-file (physical-follow-config-path))
      (unless (and (recovery-in-progress-p) *physical-receive-store*)
        (error "native follow configuration requires streaming recovery"))
      (unless *physical-follow-runtime*
        (physical-follow-launch (physical-follow-read-config))))))
