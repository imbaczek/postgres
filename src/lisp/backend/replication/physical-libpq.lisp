(in-package #:pg)

;; The helper owns libpq in an isolated child. This transport is not yet wired
;; into the physical receiver; Lisp remains responsible for WAL validation,
;; durability, and deciding when feedback and frontend CopyDone may be sent.
;; The largest COPY payload admits stock 128 KiB WAL data plus its 25-byte
;; header (and the smaller 65561-byte native payload).
(defconstant +physical-libpq-copy-maximum+ (+ (* 128 1024) 25))

(defun physical-libpq-header (session request kind size)
  (check-type session (unsigned-byte 64))
  (check-type request (unsigned-byte 64))
  (check-type size (unsigned-byte 32))
  (let ((payload (make-payload)))
    (loop for byte across #(80 71 76 81) do (vector-push-extend byte payload))
    (pg-client-push-u64 payload session)
    (pg-client-push-u64 payload request)
    (vector-push-extend (char-code kind) payload)
    (payload-push-u32 payload size)
    payload))

(defun physical-libpq-check-header (header session request kinds maximum)
  (unless (and (= 25 (length header))
               (equalp (subseq header 0 4) #(80 71 76 81))
               (= session (pg-client-u64 header 4))
               (= request (pg-client-u64 header 12))
               (member (code-char (aref header 20)) kinds)
               (<= (bytes-to-u32 header 21)
                   (min maximum +physical-client-proof-frame-maximum+)))
    (pg-client-native-reject "invalid libpq helper response header"))
  (values (code-char (aref header 20)) (bytes-to-u32 header 21)))

(defstruct (physical-libpq-profile
            (:constructor %make-physical-libpq-profile (conninfo fallback))
            (:conc-name %physical-libpq-profile-)
            (:print-function physical-libpq-print-profile))
  "Copied connection settings; printing the profile does not expose secrets.
FALLBACK supplies libpq's fallback application name. Libpq resolves service and
environment settings for each connection attempt. Effective multihost connections
are unsupported by the helper."
  (conninfo "" :type string :read-only t)
  (fallback "" :type string :read-only t))

(defun physical-libpq-print-profile (profile stream depth)
  (declare (ignore depth))
  (print-unreadable-object (profile stream :type t :identity t)))

(defun make-physical-libpq-profile (conninfo &key (fallback "walreceiver"))
  (unless (and (stringp conninfo) (stringp fallback))
    (error "invalid libpq connection profile"))
  (let ((saved-conninfo (copy-seq conninfo))
        (saved-fallback (copy-seq fallback)))
    (unless (handler-case
                (and (not (find (code-char 0) saved-conninfo))
                     (not (find (code-char 0) saved-fallback))
                     (<= (+ 8 (length (string->bytes saved-conninfo))
                            (length (string->bytes saved-fallback))) 1048576))
              (error () nil))
      (error "invalid libpq connection profile"))
    (%make-physical-libpq-profile saved-conninfo saved-fallback)))

(defun physical-libpq-profile-conninfo (profile)
  (copy-seq (%physical-libpq-profile-conninfo profile)))

(defun physical-libpq-profile-fallback (profile)
  (copy-seq (%physical-libpq-profile-fallback profile)))

(defstruct physical-libpq-result
  "A bounded helper result, including metadata to check against the expected schema."
  kind status tag columns rows)

(defun physical-libpq-decode-result (kind body)
  (unless (member kind '(#\T #\C))
    (pg-client-native-reject "unexpected libpq result kind"))
  (let ((position 0))
    (labels ((take-bytes (count)
               (unless (<= 0 count (- (length body) position))
                 (pg-client-native-reject "truncated libpq result"))
               (prog1 (subseq body position (+ position count))
                 (incf position count)))
             (number () (bytes-to-u32 (take-bytes 4) 0))
             (text ()
               (handler-case (bytes->string (take-bytes (number)))
                 (error () (pg-client-native-reject "invalid libpq result text")))))
      (let* ((status (aref (take-bytes 1) 0))
             (tag (text))
             (fields (number))
             (count (number)))
        (unless (and (<= fields 64) (<= count 1))
          (pg-client-native-reject "libpq result exceeds row or column bound"))
        (let* ((columns (loop repeat fields
                              collect (cons (text) (loop repeat 6 collect (number)))))
               (rows (loop repeat count
                           collect (coerce
                                    (loop repeat fields
                                          for size = (number)
                                          collect (if (= size #xffffffff) +null+
                                                      (handler-case
                                                          (bytes->string (take-bytes size))
                                                        (error ()
                                                          (pg-client-native-reject
                                                           "invalid libpq row text")))))
                                    'vector))))
          (unless (= position (length body))
            (pg-client-native-reject "libpq result has trailing bytes"))
          (make-physical-libpq-result :kind kind :status status :tag tag
                                      :columns columns :rows rows))))))

(defun physical-libpq-check-result (result schema tag &key command-only)
  (unless (and (zerop (physical-libpq-result-status result))
               (equal tag (physical-libpq-result-tag result))
               (char= (physical-libpq-result-kind result) (if command-only #\C #\T))
               (= (length schema) (length (physical-libpq-result-columns result)))
               (= (length (physical-libpq-result-rows result)) (if command-only 0 1)))
    (pg-client-native-reject "unexpected libpq result shape or completion"))
  (loop for expected in schema
        for actual in (physical-libpq-result-columns result)
        unless (equal actual
                      (list (first expected) 0 0 (second expected)
                            (if (= (second expected) +int8oid+) 8 #xffffffff)
                            #xffffffff 0))
          do (pg-client-native-reject "unexpected libpq column metadata"))
  (let ((row (first (physical-libpq-result-rows result))))
    (unless (every #'stringp row)
      (pg-client-native-reject "libpq result contains a null field"))
    row))

(defstruct (physical-libpq-channel (:constructor %make-physical-libpq-channel))
  "One child and its request stream. The owning transport API performs I/O and
close; a controller may stop it using PIDFD. Session and request identities bind each
reply to its exchange. A stopped channel cannot be reused; failed exchanges close it."
  process
  pidfd
  owner
  (session 0 :type (unsigned-byte 64))
  (request 0 :type (unsigned-byte 64))
  (stopped nil)
  (closed nil)
  (lock (sb-thread:make-mutex :name "libpq channel")))

(defun physical-libpq-read-exactly (stream size)
  (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
    (loop with position = 0
          while (< position size)
          for next = (read-sequence bytes stream :start position)
          do (when (= next position)
               (pg-client-native-reject "libpq helper closed its response pipe"))
             (setf position next))
    bytes))

(defun physical-libpq-read-reply (channel request kinds maximum)
  (let* ((stream (sb-ext:process-output (physical-libpq-channel-process channel)))
         (header (physical-libpq-read-exactly stream 25)))
    (multiple-value-bind (kind size)
        (physical-libpq-check-header header (physical-libpq-channel-session channel)
                                     request (cons #\! kinds) (max maximum 512))
      (unless (<= size (if (char= kind #\!) 512 maximum))
        (pg-client-native-reject "libpq helper response exceeds its command bound"))
      (let ((body (physical-libpq-read-exactly stream size)))
        (when (physical-libpq-channel-stopped channel)
          (error "libpq helper operation was stopped"))
        (when (char= kind #\!)
          (unless (and (>= size 9) (= (- size 9) (bytes-to-u32 body 5))
                       (every (lambda (byte) (or (<= 48 byte 57) (<= 65 byte 90)))
                              (subseq body 0 5))
                       (every (lambda (byte) (<= 32 byte 126)) (subseq body 9)))
            (pg-client-native-reject "invalid libpq helper error"))
          (error 'pg-client-error :sqlstate (bytes->string (subseq body 0 5))
                                  :message (bytes->string (subseq body 9))))
        (values kind body)))))

(defun physical-libpq-write-request (channel kind body)
  (unless (<= (length body) 1048576)
    (error "libpq helper request exceeds its bound"))
  (when (physical-libpq-channel-stopped channel)
    (error "libpq helper operation was stopped"))
  (when (= (physical-libpq-channel-request channel) #xffffffffffffffff)
    (error "libpq helper request identity exhausted"))
  (let* ((request (incf (physical-libpq-channel-request channel)))
         (stream (sb-ext:process-input (physical-libpq-channel-process channel))))
    (write-sequence (physical-libpq-header (physical-libpq-channel-session channel)
                                           request kind (length body)) stream)
    (write-sequence body stream)
    (finish-output stream)
    request))

(sb-alien:define-alien-routine ("pidfd_open" physical-libpq-pidfd-open) sb-alien:int
  (pid sb-alien:int) (flags sb-alien:unsigned-int))

(sb-alien:define-alien-routine ("pidfd_send_signal" physical-libpq-pidfd-signal) sb-alien:int
  (fd sb-alien:int) (signal sb-alien:int) (info sb-alien:system-area-pointer)
  (flags sb-alien:unsigned-int))

(defvar *physical-libpq-channels* (make-hash-table :test #'eq))

(defvar *physical-libpq-channels-lock*
  (sb-thread:make-mutex :name "libpq helper capacity"))

(defun physical-libpq-reserve-channel ()
  (sb-thread:with-mutex (*physical-libpq-channels-lock*)
    (when (>= (hash-table-count *physical-libpq-channels*) 2)
      (error "libpq helper capacity exhausted"))
    (let ((channel (%make-physical-libpq-channel :owner sb-thread:*current-thread*)))
      (setf (gethash channel *physical-libpq-channels*) t)
      channel)))

(defun physical-libpq-require-owner (channel)
  (unless (eq (physical-libpq-channel-owner channel) sb-thread:*current-thread*)
    (error "libpq helper operation requires its owning thread")))

(defun physical-libpq-stop (channel)
  "Mark CHANNEL stopped and signal its child through the stable process handle."
  (sb-thread:with-mutex ((physical-libpq-channel-lock channel))
    (setf (physical-libpq-channel-stopped channel) t)
    (when (physical-libpq-channel-pidfd channel)
      (unless (zerop (physical-libpq-pidfd-signal
                      (physical-libpq-channel-pidfd channel) 9 (sb-sys:int-sap 0) 0))
        (unless (= sb-posix:esrch (sb-alien:get-errno))
          (error "could not signal owned libpq helper")))))
  (values))

(defun physical-libpq-close (channel)
  "Stop and reap CHANNEL from its owner. Repeated close is harmless.
Cleanup runs without interrupts. The capacity slot is retained if child death
cannot be established, so another child cannot silently exceed the limit."
  (physical-libpq-require-owner channel)
  (sb-sys:without-interrupts
    (unless (physical-libpq-channel-closed channel)
      (physical-libpq-stop channel)
      (let ((process (physical-libpq-channel-process channel)))
        (when process
          (loop repeat 200
                until (member (sb-ext:process-status process) '(:exited :signaled))
                do (sleep 0.01))
          (unless (member (sb-ext:process-status process) '(:exited :signaled))
            (error "owned libpq helper has not exited"))
          (sb-ext:process-wait process)
          (sb-ext:process-close process))
        (sb-thread:with-mutex ((physical-libpq-channel-lock channel))
          (when (physical-libpq-channel-pidfd channel)
            (sb-posix:close (physical-libpq-channel-pidfd channel))
            (setf (physical-libpq-channel-pidfd channel) nil)))
        (sb-thread:with-mutex (*physical-libpq-channels-lock*)
          (remhash channel *physical-libpq-channels*))
        (setf (physical-libpq-channel-closed channel) t))))
  (values))

(defun physical-libpq-check-success (kind body)
  (unless (case kind
            (#\H (= 16 (length body)))
            ((#\O #\A #\F #\I) (zerop (length body)))
            (#\B (equalp body #(0 0 0 0 0)))
            (#\D (<= 1 (length body) +physical-libpq-copy-maximum+))
            ((#\T #\C) t)
            (otherwise nil))
    (pg-client-native-reject "invalid libpq helper success payload")))

(defmacro with-physical-libpq-operation ((channel) &body body)
  (let ((owned (gensym "CHANNEL")) (complete (gensym "COMPLETE")))
    `(let ((,owned ,channel) (,complete nil))
       (physical-libpq-require-owner ,owned)
       (unwind-protect
            (multiple-value-prog1 (progn ,@body)
              (setf ,complete t))
         (unless ,complete (physical-libpq-close ,owned))))))

(defun physical-libpq-exchange (channel kind body kinds maximum &key (timeout 12))
  "Send one bounded request and validate its matching typed reply.
On failure or timeout, close the channel instead of reusing a partial exchange."
  (with-physical-libpq-operation (channel)
    (sb-ext:with-timeout timeout
      (let ((request (physical-libpq-write-request channel kind body)))
        (multiple-value-bind (reply bytes)
            (physical-libpq-read-reply channel request kinds maximum)
          (physical-libpq-check-success reply bytes)
          (values reply bytes))))))

(declaim (ftype function random-cancel-key))

(defun physical-libpq-process-arguments (pid)
  (with-open-file (stream (format nil "/proc/~d/cmdline" pid)
                          :element-type '(unsigned-byte 8))
    (let ((bytes (make-payload)))
      (loop for byte = (read-byte stream nil nil)
            while byte
            do (when (>= (length bytes) 16384)
                 (error "libpq helper argument inventory exceeds its bound"))
               (vector-push-extend byte bytes))
      (bytes->string bytes))))

(defun physical-libpq-acquire-process-handle (channel)
  "Acquire a pidfd while SBCL's process reaper lock protects the child identity."
  (let ((process (physical-libpq-channel-process channel))
        (failure nil))
    (sb-int:with-system-mutex (sb-impl::*active-processes-lock*)
      (if (and (member process sb-impl::*active-processes* :test #'eq)
               (not (sb-impl::process-closed-p process))
               (member (sb-impl::process-%status process) '(:running :stopped)))
          (let ((fd (physical-libpq-pidfd-open (sb-ext:process-pid process) 0)))
            (if (minusp fd)
                (progn
                  (sb-unix:unix-kill (sb-ext:process-pid process) 9)
                  (setf failure "could not open libpq helper process handle"))
                (setf (physical-libpq-channel-pidfd channel) fd)))
          (setf failure "libpq helper exited before process handle acquisition")))
    (when failure (error failure))))

(defun physical-libpq-open-channel (helper &key on-spawn)
  "Reserve one of two slots, spawn HELPER, and verify its identity and hello.
Ownership assumes RUN-PROGRAM returns normally. This project installs no process
status hooks; an unrelated hook that throws before RUN-PROGRAM returns is not
supported. A failed open attempts to close the reserved channel."
  (let* ((path (namestring (truename helper)))
         (parent (format nil "~d" (sb-posix:getpid)))
         (session (loop for value = (logior (ash (random-cancel-key) 32)
                                            (random-cancel-key))
                        when (plusp value) return value))
         (nonce (format nil "~16,'0x" session)))
    (sb-sys:without-interrupts
      (let ((channel (physical-libpq-reserve-channel))
            (complete nil))
        (setf (physical-libpq-channel-session channel) session)
        (unwind-protect
             (progn
               (setf (physical-libpq-channel-process channel)
                     (sb-ext:run-program path (list parent nonce) :use-posix-spawn t
                                                                  :wait nil :input :stream :output :stream :error :stream))
               (physical-libpq-acquire-process-handle channel)
               (sb-sys:with-local-interrupts
                 (sb-ext:with-timeout 5
                   (let ((pid (sb-ext:process-pid (physical-libpq-channel-process channel))))
                     (when on-spawn (funcall on-spawn channel))
                     (multiple-value-bind (kind body)
                         (physical-libpq-read-reply channel 0 '(#\H) 16)
                       (physical-libpq-check-success kind body)
                       (unless (equal (physical-libpq-process-arguments pid)
                                      (format nil "~a~c~a~c~a~c"
                                              path (code-char 0) parent (code-char 0)
                                              nonce (code-char 0)))
                         (error "libpq helper process identity differs"))
                       (unless (and (= pid (bytes-to-u32 body 0))
                                    (= (sb-posix:getpid) (bytes-to-u32 body 4))
                                    (>= (bytes-to-u32 body 8) 170000)
                                    (= (* 256 1024 1024) (bytes-to-u32 body 12)))
                         (pg-client-native-reject "libpq helper handshake differs"))))))
               (setf complete t)
               channel)
          (unless complete
            (physical-libpq-close channel)))))))

(defun physical-libpq-push-text (payload text)
  (when (find (code-char 0) text)
    (error "libpq request text contains a null character"))
  (let ((bytes (string->bytes text)))
    (payload-push-u32 payload (length bytes))
    (loop for byte across bytes do (vector-push-extend byte payload)))
  payload)

(defun physical-libpq-connect (helper profile &key on-spawn)
  (let ((channel (physical-libpq-open-channel helper :on-spawn on-spawn)))
    (with-physical-libpq-operation (channel)
      (let ((body (make-payload)))
        (physical-libpq-push-text body (%physical-libpq-profile-conninfo profile))
        (physical-libpq-push-text body (%physical-libpq-profile-fallback profile))
        (physical-libpq-exchange channel #\O body '(#\O) 0)
        channel))))

(defun physical-libpq-query (channel query schema tag &key (maximum 4096) command-only)
  "Run a command and require its bounded result to match SCHEMA and TAG."
  (with-physical-libpq-operation (channel)
    (let ((body (make-payload)))
      (payload-push-u32 body maximum)
      (physical-libpq-push-text body query)
      (multiple-value-bind (kind bytes)
          (physical-libpq-exchange channel #\Q body '(#\T #\C) maximum)
        (physical-libpq-check-result (physical-libpq-decode-result kind bytes)
                                     schema tag :command-only command-only)))))

(defun physical-libpq-switch-result (kind bytes)
  (let* ((row (physical-libpq-check-result
               (physical-libpq-decode-result kind bytes)
               '(("next_tli" 20) ("next_tli_startpos" 25)) "START_STREAMING"))
         (next (pg-client-native-u64 (aref row 0) t))
         (fork (pg-client-native-lsn (aref row 1))))
    (unless (> next 1)
      (pg-client-native-reject "native switch lacks a child timeline"))
    (list :switch next fork)))

(defun physical-libpq-start (channel query)
  "Start streaming, returning :COPY-BOTH or the validated timeline switch."
  (with-physical-libpq-operation (channel)
    (let ((body (make-payload)))
      (payload-push-u32 body 4096)
      (physical-libpq-push-text body query)
      (multiple-value-bind (kind bytes)
          (physical-libpq-exchange channel #\Q body '(#\B #\T) 4096)
        (if (char= kind #\B) :copy-both
            (physical-libpq-switch-result kind bytes))))))

(defun physical-libpq-receive (channel &key (wait-ms 1000) (maximum +physical-libpq-copy-maximum+))
  "Receive COPY data, server completion, or an idle indication within the bounds."
  (unless (and (typep wait-ms '(integer 0 10000))
               (typep maximum '(integer 1 131097)))
    (error "invalid libpq copy receive bound"))
  (let ((body (make-payload)))
    (payload-push-u32 body maximum)
    (payload-push-u32 body wait-ms)
    (physical-libpq-exchange channel #\R body '(#\D #\F #\I) maximum
                             :timeout (+ 12 (/ wait-ms 1000)))))

(defun physical-libpq-feedback (channel body)
  "Send feedback chosen by the caller from its own WAL and durability state."
  (unless (<= 1 (length body) 65532)
    (error "invalid libpq feedback length"))
  (physical-libpq-exchange channel #\S body '(#\A) 0)
  (values))

(defun physical-libpq-finish (channel next fork)
  "Send frontend CopyDone and verify the resulting switch against NEXT and FORK.
The caller must first flush local receipt and send truthful durability feedback."
  (with-physical-libpq-operation (channel)
    (let ((body (make-payload)))
      (payload-push-u32 body 4096)
      (multiple-value-bind (kind bytes)
          (physical-libpq-exchange channel #\E body '(#\T) 4096)
        (let ((result (physical-libpq-switch-result kind bytes)))
          (unless (equal result (list :switch next fork))
            (pg-client-native-reject "native switch differs from preparation"))
          result)))))
