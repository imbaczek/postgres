;;;; PostgreSQL's SASL startup exchange for SCRAM-SHA-256.
(in-package #:pg)

(defconstant +scram-max-frame-bytes+ 1024)

(sb-alien:define-alien-routine ("EVP_EncodeBlock" scram-crypto-encode64) sb-alien:int
  (output sb-alien:system-area-pointer) (input sb-alien:system-area-pointer)
  (length sb-alien:int))

(sb-alien:define-alien-routine ("EVP_DecodeBlock" scram-crypto-decode64) sb-alien:int
  (output sb-alien:system-area-pointer) (input sb-alien:system-area-pointer)
  (length sb-alien:int))

(sb-alien:define-alien-routine ("RAND_bytes" scram-crypto-random) sb-alien:int
  (output sb-alien:system-area-pointer) (length sb-alien:int))

(defun scram-encode64 (bytes)
  (check-type bytes (simple-array (unsigned-byte 8) (*)))
  (when (> (length bytes) (* 3 (floor #x7fffffff 4)))
    (error "SCRAM encoding input is too large"))
  (ensure-scram-crypto)
  (let* ((size (* 4 (ceiling (length bytes) 3)))
         (output (make-array (1+ size) :element-type '(unsigned-byte 8))))
    (sb-sys:with-pinned-objects (bytes output)
      (unless (= size (scram-crypto-encode64
                       (sb-sys:vector-sap output) (sb-sys:vector-sap bytes) (length bytes)))
        (error "could not encode SCRAM bytes")))
    (bytes->string output :end size)))

(defun scram-decode64 (text size)
  "Decode a canonically padded base64 field of an expected size."
  (let ((encoded-size (* 4 (ceiling size 3))))
    (when (and (= encoded-size (length text))
               (every (lambda (character) (< (char-code character) 128)) text))
      (ensure-scram-crypto)
      (let ((input (string->bytes text))
            (output (make-array (* 3 (ceiling size 3)) :element-type '(unsigned-byte 8))))
        (sb-sys:with-pinned-objects (input output)
          (when (= (length output)
                   (scram-crypto-decode64
                    (sb-sys:vector-sap output) (sb-sys:vector-sap input) encoded-size))
            (let ((decoded (subseq output 0 size)))
              (when (string= text (scram-encode64 decoded)) decoded))))))))

(defun scram-decode-proof (text)
  (scram-decode64 text +scram-key-bytes+))

(defvar *scram-libpq-loaded-p* nil)

(sb-alien:define-alien-routine ("PQconnectStart" scram-pq-connect-start) sb-alien:system-area-pointer
  (connection-options sb-alien:c-string))

(sb-alien:define-alien-routine ("PQencryptPasswordConn" scram-pq-encrypt-password) sb-alien:system-area-pointer
  (connection sb-alien:system-area-pointer)
  (password sb-alien:system-area-pointer)
  (user sb-alien:c-string)
  (algorithm sb-alien:c-string))

(sb-alien:define-alien-routine ("PQfinish" scram-pq-finish) sb-alien:void
  (connection sb-alien:system-area-pointer))

(sb-alien:define-alien-routine ("PQfreemem" scram-pq-free) sb-alien:void
  (pointer sb-alien:system-area-pointer))

(defun ensure-scram-libpq ()
  (sb-thread:with-mutex (*scram-crypto-lock*)
    (unless *scram-libpq-loaded-p*
      (sb-alien:load-shared-object "libpq.so.5")
      (setf *scram-libpq-loaded-p* t))))

(defun scram-decode-libpq-verifier (text)
  (let ((fields (uiop:split-string text :separator "$:")))
    (when (= 5 (length fields))
      (destructuring-bind (method rounds salt stored server) fields
        (when (and (string= method "SCRAM-SHA-256")
                   (<= 1 (length rounds) 10) (every #'digit-char-p rounds))
          (let ((iterations (parse-integer rounds))
                (salt-bytes (scram-decode64 salt 16))
                (stored-key (scram-decode-proof stored))
                (server-key (scram-decode-proof server)))
            (when (and (<= 1 iterations #x7fffffff) salt-bytes stored-key server-key)
              (list salt-bytes iterations stored-key server-key))))))))

(defun scram-read-libpq-verifier (pointer)
  (let ((text (make-string 256)))
    (unwind-protect
         (loop for index below (length text)
               for byte = (sb-sys:sap-ref-8 pointer index)
               when (zerop byte)
                 do (return (scram-decode-libpq-verifier (subseq text 0 index)))
               do (setf (char text index) (code-char byte)))
      (fill text #\Space))))

(defun scram-build-role-verifier (password)
  "Use libpq's PostgreSQL password normalization and random salt without connecting."
  (when (zerop (length password))
    (return-from scram-build-role-verifier nil))
  (when (find (code-char 0) password)
    (ereport "22023" "role password contains a zero byte"))
  (ensure-scram-libpq)
  (let ((bytes (sb-ext:string-to-octets password :external-format :utf-8 :null-terminate t))
        (connection (sb-sys:int-sap 0))
        (secret (sb-sys:int-sap 0)))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             ;; An unknown connection option gives libpq a handle without opening a socket.
             (setf connection (scram-pq-connect-start "pg_lisp_offline=true"))
             (unless (zerop (sb-sys:sap-int connection))
               (sb-sys:with-pinned-objects (bytes)
                 (setf secret (scram-pq-encrypt-password connection (sb-sys:vector-sap bytes)
                                                         "" "scram-sha-256"))))
             (or (and (not (zerop (sb-sys:sap-int secret)))
                      (scram-read-libpq-verifier secret))
                 (error "could not generate role SCRAM verifier")))
        (fill bytes 0)
        (unless (zerop (sb-sys:sap-int secret)) (scram-pq-free secret))
        (unless (zerop (sb-sys:sap-int connection)) (scram-pq-finish connection))))))

(defun scram-random-nonce ()
  (ensure-scram-crypto)
  (let ((bytes (make-array 18 :element-type '(unsigned-byte 8))))
    (sb-sys:with-pinned-objects (bytes)
      (unless (= 1 (scram-crypto-random (sb-sys:vector-sap bytes) (length bytes)))
        (error "could not generate a SCRAM nonce")))
    (scram-encode64 bytes)))

(defun scram-wire-error ()
  (ereport-fatal "08P01" "malformed SCRAM authentication message"))

(defun scram-read-response (stream)
  (multiple-value-bind (type length payload) (read-message-frame stream +scram-max-frame-bytes+ (char-code #\p))
    (declare (ignore length))
    (unless (and (eql type (char-code #\p)) payload)
      (scram-wire-error))
    payload))

(defun scram-send-request (stream code &optional data)
  (with-backend-message (stream #\R payload)
    (payload-push-u32 payload code)
    (if (= code 10)
        (progn
          (payload-push-cstring payload "SCRAM-SHA-256")
          (payload-push-byte payload 0))
        (payload-push-string payload data))))

(defun scram-wire-text (bytes &optional (start 0) (end (length bytes)))
  (when (position 0 bytes :start start :end end)
    (scram-wire-error))
  (handler-case (bytes->string bytes :start start :end end)
    (error () (scram-wire-error))))

(defun scram-read-client-first (stream)
  (let* ((payload (scram-read-response stream))
         (terminator (position 0 payload))
         (offset (when terminator (+ terminator 5))))
    (unless (and offset (<= offset (length payload))
                 (string= "SCRAM-SHA-256" (scram-wire-text payload 0 terminator)))
      (scram-wire-error))
    (let ((size (bytes-to-u32 payload (1+ terminator))))
      (cond
        ((= size #xffffffff)
         (unless (= offset (length payload)) (scram-wire-error))
         (scram-send-request stream 11 "")
         (scram-wire-text (scram-read-response stream)))
        ((and (plusp size) (= size (- (length payload) offset)))
         (scram-wire-text payload offset))
        (t (scram-wire-error))))))

(defun scram-scan-extensions (message separator &key find-proof)
  "Check optional attributes and optionally return the first proof separator."
  (loop for start = separator then (position #\, message :start (+ start 3))
        while start
        do (unless (and (< (+ start 2) (length message))
                        (let ((attribute (char message (1+ start))))
                          (or (char<= #\A attribute #\Z) (char<= #\a attribute #\z)))
                        (char= #\= (char message (+ start 2))))
             (scram-wire-error))
           (when (and find-proof (char= #\p (char message (1+ start))))
             (return start))))

(defun scram-parse-client-first (message)
  (when (and (<= 4 (length message)) (member (char message 0) '(#\n #\y))
             (string= message ",a=" :start1 1 :end1 4))
    (ereport-fatal "0A000" "SCRAM authorization identities are unsupported"))
  (when (and (<= 5 (length message)) (member (char message 0) '(#\n #\y))
             (string= message ",,m=" :start1 1 :end1 5))
    (ereport-fatal "0A000" "SCRAM mandatory extensions are unsupported"))
  (unless (and (<= 7 (length message))
               (member (char message 0) '(#\n #\y))
               (string= message ",,n=" :start1 1 :end1 5))
    (scram-wire-error))
  (let* ((separator (position #\, message :start 5))
         (nonce-start (when separator (+ separator 3)))
         (nonce-end (when nonce-start (and (<= nonce-start (length message))
                                           (position #\, message :start nonce-start)))))
    (unless (and nonce-start (< nonce-start (length message))
                 (string= message "r=" :start1 (1+ separator) :end1 nonce-start)
                 (< nonce-start (or nonce-end (length message)))
                 (every (lambda (character) (<= 33 (char-code character) 126))
                        (subseq message nonce-start nonce-end)))
      (scram-wire-error))
    (scram-scan-extensions message nonce-end)
    (values (subseq message 3) (subseq message nonce-start nonce-end)
            (if (char= (char message 0) #\n) "biws" "eSws"))))

(defun scram-parse-client-final (message channel-binding nonce)
  (let* ((prefix (format nil "c=~a,r=~a" channel-binding nonce))
         (end (length prefix)))
    (unless (and (< end (length message))
                 (string= prefix message :end2 end)
                 (char= #\, (char message end)))
      (scram-wire-error))
    (let ((proof-position (scram-scan-extensions message end :find-proof t)))
      (unless proof-position (scram-wire-error))
      (values (subseq message 0 proof-position)
              (or (scram-decode-proof (subseq message (+ proof-position 3)))
                  (scram-wire-error))))))

(defun scram-authenticate-stream (stream salt iterations stored-key server-key)
  "Send the SCRAM challenge and verify the client's proof.
The caller supplies a valid verifier and owns the network timeout. On success
send the final signature. The caller sends AuthenticationOk after admission."
  (scram-send-request stream 10)
  (multiple-value-bind (client-first client-nonce channel-binding)
      (scram-parse-client-first (scram-read-client-first stream))
    (let* ((nonce (concatenate 'string client-nonce (scram-random-nonce)))
           (challenge (format nil "r=~a,s=~a,i=~d" nonce (scram-encode64 salt) iterations)))
      (scram-send-request stream 11 challenge)
      (multiple-value-bind (client-final proof)
          (scram-parse-client-final (scram-wire-text (scram-read-response stream)) channel-binding nonce)
        (let* ((auth-message (string->bytes (format nil "~a,~a,~a" client-first challenge client-final)))
               (signature (scram-verify-client-proof stored-key server-key auth-message proof)))
          (unless signature (ereport-fatal "28P01" "password authentication failed"))
          (scram-send-request stream 12 (concatenate 'string "v=" (scram-encode64 signature)))
          t)))))
