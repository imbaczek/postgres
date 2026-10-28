;;;; Private owned-server helpers for a manual selected-history replay experiment.
(in-package #:pg)

(defun selected-replay-export (directory)
  (with-write-lock
    (with-wal-critical
      (let* ((timeline *wal-insert-timeline*) (history (timeline-read-history timeline))
                                              (fork (timeline-history-entry-end (second history))) (first t))
        (wal-atomic-write-bytes (merge-pathnames "history.txt" directory) (timeline-history-bytes timeline))
        (let ((prefix (make-array (mod fork +wal-segment-size+) :element-type '(unsigned-byte 8))))
          (when (plusp (length prefix))
            (with-open-file (in (wal-segment-path (floor fork +wal-segment-size+) timeline :timeline)
                                :element-type '(unsigned-byte 8))
              (unless (= (length prefix) (read-sequence prefix in)) (error "short selected source prefix"))))
          (wal-atomic-write-bytes (merge-pathnames "source-prefix.bin" directory) prefix))
        (with-open-file (out (merge-pathnames "identity.txt" directory) :direction :output :if-exists :supersede)
          (format out "~d ~d ~d ~d~%" (catalog-system-identifier) timeline fork *wal-durable-lsn*)
          (fsync-stream out))
        (with-open-file (out (merge-pathnames "frames.bin" directory) :direction :output
                                                                      :if-exists :supersede :element-type '(unsigned-byte 8))
          (wal-scan-lineage
           (lambda (record owner file-timeline)
             (declare (ignore owner))
             (when first
               (unless (= (wal-record-type record) +wal-record-end-of-recovery+)
                 (error "selected source prefix does not begin with EOR")))
             (let* ((start (wal-record-start record)) (length (- (wal-record-end record) start))
                                                      (bytes (make-array length :element-type '(unsigned-byte 8)))
                                                      (header (make-array 12 :element-type '(unsigned-byte 8) :initial-element 0)))
               (with-open-file (in (wal-segment-path (floor start +wal-segment-size+) file-timeline :timeline)
                                   :element-type '(unsigned-byte 8))
                 (file-position in (mod start +wal-segment-size+))
                 (unless (= length (read-sequence bytes in)) (error "short selected source record")))
               (when first
                 (wal-atomic-write-bytes (merge-pathnames "eor.bin" directory) bytes)
                 (setf first nil))
               (wal-set-u64 header 0 start) (wal-set-u32 header 8 length)
               (write-sequence header out) (write-sequence bytes out)))
           history fork *wal-durable-lsn* :namespace :timeline)
          (when first (error "selected source has no child WAL"))
          (fsync-stream out))
        (fsync-directory directory)
        (list :timeline timeline :fork fork :end *wal-durable-lsn*)))))

(defun selected-replay-export-parent-prefix (directory)
  (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
    (let ((prefix (physical-recovery-wal-prefix-under-lock *physical-receive-store* *last-replay-lsn*)))
      (unless prefix (error "selected receiver lacks an authenticated fork prefix"))
      (wal-atomic-write-bytes (merge-pathnames "receiver-prefix.bin" directory) prefix)
      (length prefix))))

(defun selected-replay-inject (directory &optional eor-only)
  (with-open-file (in (merge-pathnames "frames.bin" directory) :element-type '(unsigned-byte 8))
    (loop with header = (make-array 12 :element-type '(unsigned-byte 8))
          for count = (read-sequence header in)
          while (plusp count)
          do (unless (= count 12) (error "short selected test frame header"))
             (let* ((start (wal-u64 header 0)) (length (wal-u32 header 8))
                                               (bytes (when (<= +wal-header-size+ length +wal-segment-size+)
                                                        (make-array length :element-type '(unsigned-byte 8)))))
               (unless (and bytes (= length (read-sequence bytes in))) (error "short selected test frame"))
               (when (>= start (physical-store-written-lsn *physical-receive-store*))
                 (physical-recovery-receive start bytes))
               (when eor-only (return)))))
  (physical-recovery-flush))

(defun selected-replay-inject-eor-after-worker-join (directory)
  (setf *physical-promotion-test-hook*
        (lambda (point)
          (when (eq point :after-online-worker-join)
            (setf *physical-promotion-test-hook* nil)
            (with-live-operation
              (selected-replay-inject directory t))))))

(defun selected-replay-pause-interleaving (name)
  (let ((paused *physical-replay-paused-p*) (stopping *physical-promotion-stopping-p*)
                                            (thread nil))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*physical-replay-pause-lock*)
             (setf thread
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (progn (physical-replay-pause-command name) :accepted)
                        (error (condition) (princ-to-string condition))))
                    :name "selected replay pause interleaving"))
             (loop with deadline = (+ (get-internal-real-time) (* 5 internal-time-units-per-second))
                   until (eq *physical-replay-pause-lock* (sb-thread::thread-waiting-for thread))
                   do (when (> (get-internal-real-time) deadline)
                        (error "pause command did not reach the mutex"))
                      (sleep 0.001))
             (setf *physical-promotion-stopping-p* t))
           (let ((result (sb-thread:join-thread thread :timeout 5 :default :timeout)))
             (unless (and (stringp result) (search "standby promotion is ongoing" result :test #'char-equal))
               (error "pause command crossed promotion gate: ~s" result)))
           (assert (eql paused *physical-replay-paused-p*))
           t)
      (sb-thread:with-mutex (*physical-replay-pause-lock*)
        (setf *physical-promotion-stopping-p* stopping *physical-replay-paused-p* paused))
      (when (and thread (sb-thread:thread-alive-p thread))
        (sb-thread:join-thread thread :timeout 5 :default :timeout)))))

(defun selected-replay-export-follow-request (directory source)
  (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
    (let* ((state *physical-recovery-state*) (store *physical-receive-store*)
                                             (cursor (physical-store-flushed-lsn store))
                                             (prefix (physical-recovery-wal-prefix-under-lock store cursor))
                                             (request
                                               (list :version 1 :system-id (getf state :system-id)
                                                     :base-timeline (getf state :timeline) :base (getf state :end)
                                                     :manifest-digest (getf state :manifest-digest) :source-timeline source
                                                     :receive-timeline (physical-store-prefix-timeline store cursor)
                                                     :receive-lsn cursor :receive-digest (physical-store-flushed-digest store)
                                                     :overlap-start (- cursor (length prefix))
                                                     :overlap-digest (physical-store-update-digest #xCBF29CE484222325 prefix)
                                                     :history-text (bytes->string (read-bounded-regular-file
                                                                                   (merge-pathnames "history.txt" directory) (* 16 1024 1024)))
                                                     :eor-octets (coerce (read-bounded-regular-file
                                                                          (merge-pathnames "eor.bin" directory) +wal-end-of-recovery-record-size+)
                                                                         'list))))
      (unless (and prefix (plusp (length prefix)) (= cursor *last-replay-lsn*))
        (error "native anchor fixture requires a nonempty applied fork overlap"))
      (physical-follow-anchor-check request)
      (wal-atomic-write-bytes (merge-pathnames "follow-request.txt" directory)
                              (string->bytes (physical-bootstrap-manifest-text request)))
      (with-open-file (out (merge-pathnames "follow-summary.txt" directory)
                           :direction :output :if-exists :supersede)
        (format out "~d ~d ~d ~d~%" (getf request :system-id) (getf request :base)
                (getf request :manifest-digest) (getf request :receive-digest))
        (fsync-stream out))
      (fsync-directory directory)
      t)))

(defun selected-replay-follow-request (directory)
  (recovery-parse-manifest-text
   (bytes->string (read-bounded-regular-file (merge-pathnames "follow-request.txt" directory)
                                             (physical-follow-pin-image-limit)))))

(defun selected-replay-follow-pin (directory)
  (let* ((request (selected-replay-follow-request directory))
         (pin (physical-source-follow-ready "selected_follow" request)))
    (assert (and (physical-follow-pin-ready-p pin)
                 (= (getf request :receive-lsn) (physical-source-pin-confirmed-lsn pin))
                 (= (getf request :receive-digest) (physical-source-pin-prefix-digest pin))
                 (< (getf request :base) (getf request :receive-lsn) *wal-checkpoint-lsn*)
                 (equal request (physical-follow-pin-anchor pin))
                 (= (physical-source-pin-confirmed-lsn pin)
                    (gethash pin *wal-snapshot-retention-pins*))))
    t))

(defun selected-replay-follow-behind-pin (directory)
  (let* ((request (selected-replay-follow-request directory))
         (base (getf request :base))
         (origin (getf request :overlap-start))
         (prefix (read-bounded-regular-file (merge-pathnames "source-prefix.bin" directory)
                                            +wal-segment-size+)))
    (unless (< origin base (getf request :receive-lsn))
      (error "behind-fork wire fixture needs its base in the fork segment"))
    (setf (getf request :receive-lsn) base
          (getf request :receive-timeline) (getf request :base-timeline)
          (getf request :receive-digest) #xCBF29CE484222325
          (getf request :overlap-digest)
          (physical-store-update-digest #xCBF29CE484222325 (subseq prefix 0 (- base origin))))
    (physical-source-follow-ready "selected_behind" request)
    t))

(defun selected-replay-bad-follow-overlap (directory)
  (let ((request (selected-replay-follow-request directory)) (rejected nil))
    (setf (getf request :overlap-digest) (logxor 1 (getf request :overlap-digest)))
    (handler-case (physical-source-follow-ready "selected_bad" request)
      (wal-input-validation-error () (setf rejected t)))
    (assert rejected)
    (let ((pin (gethash "selected_bad" *physical-source-pins*)))
      (assert (and pin (not (physical-source-pin-stream-ready-p pin))
                   (null (physical-source-pin-confirmed-lsn pin)) (not *wal-poisoned*))))
    (physical-source-pin-release "selected_bad")
    t))

(defun selected-replay-install-follow-cut (directory point)
  (setf *physical-source-test-hook*
        (lambda (stage pin)
          (when (and (eq stage point) (string= "selected_follow" (physical-source-pin-name pin)))
            (with-open-file (out (merge-pathnames "follow-cut-held" directory)
                                 :direction :output :if-exists :supersede)
              (format out "~d~%" (sb-posix:getpid)) (fsync-stream out))
            (loop until (probe-file (merge-pathnames "follow-cut-release" directory))
                  do (sleep 0.01))))))

(defvar *selected-replay-follow-result* nil)

(defun selected-replay-start-follow-pin (directory)
  (setf *selected-replay-follow-result* nil)
  (sb-thread:make-thread
   (lambda ()
     (setf *selected-replay-follow-result*
           (handler-case (selected-replay-follow-pin directory)
             (error (condition) (princ-to-string condition)))))
   :name "selected follow pin experiment")
  t)

(defun selected-replay-native-client-start-query (slot request cursor digest)
  (format nil
          "START_REPLICATION SLOT ~a PHYSICAL ~a (native_format '~a', mode 'follow', system_id '~d', timeline '~d', base_lsn '~a', prefix_digest '~d', manifest_digest '~d')"
          slot (wal-format-lsn cursor) +physical-native-format+ (getf request :system-id)
          (getf request :receive-timeline) (wal-format-lsn (getf request :base))
          digest (getf request :manifest-digest)))

(defun selected-replay-native-client-feedback (client store base)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\r))
    (pg-client-push-u64 payload (physical-store-written-lsn store))
    (pg-client-push-u64 payload (physical-store-flushed-lsn store))
    (pg-client-push-u64 payload base)
    (pg-client-push-u64 payload (postgres-current-timestamp))
    (payload-push-byte payload 0)
    (sb-ext:with-timeout 10 (write-message (pg-client-stream client) #\d payload))))

(defun selected-replay-native-client-parent (client store base fork)
  (loop
    (multiple-value-bind (type payload)
        (sb-ext:with-timeout 10 (pg-client-native-frame client (+ 65536 29)))
      (case type
        (#\d
         (case (code-char (aref payload 0))
           (#\w (physical-store-input store (pg-client-u64 payload 1) (subseq payload 25)))
           (#\k (assert (= 18 (length payload))))
           (otherwise (error "unexpected native parent payload"))))
        (#\c
         (assert (zerop (length payload)))
         (physical-store-flush store)
         (assert (= fork (physical-store-flushed-lsn store)))
         (selected-replay-native-client-feedback client store base)
         (return (sb-ext:with-timeout 10 (pg-client-native-finish client 2 fork))))
        (#\E (pg-client-native-source-error payload))
        (otherwise (error "unexpected native parent frame"))))
    (physical-store-flush store)
    (selected-replay-native-client-feedback client store base)))

(defun selected-replay-native-client-reject-release (client slot digest expected-state)
  (assert
   (equal expected-state
          (sb-ext:with-timeout 10
            (handler-case (progn (pg-client-native-release client slot digest) nil)
              (pg-client-error (condition) (pg-client-error-sqlstate condition)))))))

(defun selected-replay-probe-native-client (directory port)
  (let* ((request (selected-replay-follow-request directory))
         (coordinates (loop for (key value) on request by #'cddr
                            unless (member key '(:history-text :eor-octets)) append (list key value)))
         (base (getf request :base)) (fork (getf request :receive-lsn))
         (client (sb-ext:with-timeout 10
                   (pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
         (store nil))
    (unwind-protect
         (progn
           (assert (equal request (sb-ext:with-timeout 10 (pg-client-native-prepare client "wire_follow" coordinates))))
           (assert (equalp (string->bytes (getf request :history-text))
                           (sb-ext:with-timeout 10 (pg-client-native-history client 2))))
           (assert (equal (list :switch 2 fork)
                          (sb-ext:with-timeout 10
                            (pg-client-native-start
                             client (selected-replay-native-client-start-query
                                     "wire_follow" request fork (getf request :receive-digest))))))
           (let* ((prefix (read-bounded-regular-file (merge-pathnames "source-prefix.bin" directory)
                                                     +wal-segment-size+))
                  (behind (copy-tree coordinates))
                  (behind-anchor nil)
                  (spool (merge-pathnames "native-client-receive/" directory)))
             (setf (getf behind :receive-lsn) base
                   (getf behind :receive-digest) #xCBF29CE484222325
                   (getf behind :overlap-digest)
                   (physical-store-update-digest
                    #xCBF29CE484222325 (subseq prefix 0 (- base (getf request :overlap-start)))))
             (setf behind-anchor
                   (sb-ext:with-timeout 10 (pg-client-native-prepare client "native_client_parent" behind)))
             (wal-ensure-directory-durable spool)
             (setf store (physical-store-open spool (getf request :system-id) 1 :create t :start-lsn base))
             (assert (eq :copy-both
                         (sb-ext:with-timeout 10
                           (pg-client-native-start
                            client (selected-replay-native-client-start-query
                                    "native_client_parent" behind base #xCBF29CE484222325)))))
             (let ((other (sb-ext:with-timeout 10
                            (pg-client-connect "127.0.0.1" port "postgres" :replication-p t))))
               (unwind-protect
                    (selected-replay-native-client-reject-release
                     other "native_client_parent" (physical-follow-anchor-digest behind-anchor) "55006")
                 (pg-client-close other)))
             (assert (equal (list :switch 2 fork)
                            (selected-replay-native-client-parent client store base fork)))
             (assert (= (getf request :receive-digest) (physical-store-flushed-digest store)))
             (let ((digest (physical-follow-anchor-digest behind-anchor)))
               (selected-replay-native-client-reject-release client "native_client_parent" (logxor 1 digest) "55000")
               (assert (sb-ext:with-timeout 10 (pg-client-native-release client "native_client_parent" digest)))
               (assert (sb-ext:with-timeout 10 (pg-client-native-release client "native_client_parent" digest)))))
           (assert (not *wal-poisoned*))
           (assert (not (gethash "native_client_parent" *physical-source-pins*)))
           t)
      (pg-client-close client)
      (when store (physical-store-close store)))))
