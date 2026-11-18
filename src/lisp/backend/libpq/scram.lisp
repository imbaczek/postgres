;;;; SCRAM-SHA-256 keys and proofs for PostgreSQL authentication.
(in-package #:pg)

(defconstant +scram-key-bytes+ 32)

(deftype scram-key ()
  `(simple-array (unsigned-byte 8) (,+scram-key-bytes+)))

(defvar *scram-crypto-loaded-p* nil)
(defvar *scram-crypto-lock* (sb-thread:make-mutex :name "SCRAM crypto provider"))

(defun ensure-scram-crypto ()
  (sb-thread:with-mutex (*scram-crypto-lock*)
    (unless *scram-crypto-loaded-p*
      (sb-alien:load-shared-object "libcrypto.so.3")
      (setf *scram-crypto-loaded-p* t))))

(sb-alien:define-alien-routine ("EVP_sha256" scram-evp-sha256) sb-alien:system-area-pointer)

(sb-alien:define-alien-routine ("SHA256" scram-crypto-sha256) sb-alien:system-area-pointer
  (input sb-alien:system-area-pointer)
  (length sb-alien:unsigned-long)
  (output sb-alien:system-area-pointer))

(sb-alien:define-alien-routine ("HMAC" scram-crypto-hmac) sb-alien:system-area-pointer
  (digest sb-alien:system-area-pointer)
  (key sb-alien:system-area-pointer)
  (key-length sb-alien:int)
  (input sb-alien:system-area-pointer)
  (input-length sb-alien:unsigned-long)
  (output sb-alien:system-area-pointer)
  (output-length (* sb-alien:unsigned-int)))

(sb-alien:define-alien-routine ("PKCS5_PBKDF2_HMAC" scram-crypto-pbkdf2) sb-alien:int
  (password sb-alien:system-area-pointer)
  (password-length sb-alien:int)
  (salt sb-alien:system-area-pointer)
  (salt-length sb-alien:int)
  (iterations sb-alien:int)
  (digest sb-alien:system-area-pointer)
  (output-length sb-alien:int)
  (output sb-alien:system-area-pointer))

(sb-alien:define-alien-routine ("CRYPTO_memcmp" scram-crypto-compare) sb-alien:int
  (first sb-alien:system-area-pointer)
  (second sb-alien:system-area-pointer)
  (length sb-alien:unsigned-long))

(defun scram-key-buffer ()
  (make-array +scram-key-bytes+ :element-type '(unsigned-byte 8) :initial-element 0))

(defun scram-sha256 (input)
  (check-type input (simple-array (unsigned-byte 8) (*)))
  (let ((output (scram-key-buffer)))
    (sb-sys:with-pinned-objects (input output)
      (when (zerop (sb-sys:sap-int
                    (scram-crypto-sha256 (sb-sys:vector-sap input) (length input)
                                         (sb-sys:vector-sap output))))
        (error "could not calculate SCRAM digest")))
    output))

(defun scram-hmac-sha256 (key input)
  (check-type key scram-key)
  (check-type input (simple-array (unsigned-byte 8) (*)))
  (let ((output (scram-key-buffer)))
    (sb-sys:with-pinned-objects (key input output)
      (sb-alien:with-alien ((length sb-alien:unsigned-int))
        (when (or (zerop (sb-sys:sap-int
                          (scram-crypto-hmac
                           (scram-evp-sha256) (sb-sys:vector-sap key) (length key)
                           (sb-sys:vector-sap input) (length input) (sb-sys:vector-sap output)
                           (sb-alien:addr length))))
                  (/= length +scram-key-bytes+))
          (error "could not calculate SCRAM HMAC"))))
    output))

(defun scram-derive-keys (password salt iterations)
  "Return the stored key and server key for a normalized password and a salt.
The caller owns the password and salt buffers and the returned keys."
  (unless (typep password '(simple-array (unsigned-byte 8) (*)))
    (error "invalid SCRAM password buffer"))
  (check-type salt (simple-array (unsigned-byte 8) (*)))
  (check-type iterations (integer 1 2147483647))
  (unless (and (<= 1 (length password) #x7fffffff)
               (<= 1 (length salt) #x7fffffff))
    (error "invalid SCRAM password or salt length"))
  (ensure-scram-crypto)
  (let ((salted (scram-key-buffer))
        (client-key nil))
    (unwind-protect
         (progn
           (sb-sys:with-pinned-objects (password salt salted)
             (unless (= 1 (scram-crypto-pbkdf2
                           (sb-sys:vector-sap password) (length password)
                           (sb-sys:vector-sap salt) (length salt) iterations
                           (scram-evp-sha256) (length salted) (sb-sys:vector-sap salted)))
               (error "could not derive SCRAM keys")))
           ;; Derive the client and server keys with separate labels.
           ;; Store the hash of the client key so it cannot act as a proof.
           (setf client-key (scram-hmac-sha256 salted (string->bytes "Client Key")))
           (values (scram-sha256 client-key)
                   (scram-hmac-sha256 salted (string->bytes "Server Key"))))
      (fill salted 0)
      (when client-key (fill client-key 0)))))

(defun scram-verify-client-proof (stored-key server-key auth-message proof)
  "Return the server signature for a valid SCRAM client proof, or NIL."
  (unless (and (typep stored-key 'scram-key)
               (typep server-key 'scram-key))
    (error "invalid SCRAM key buffer"))
  (unless (typep proof 'scram-key)
    (return-from scram-verify-client-proof nil))
  (ensure-scram-crypto)
  (let ((client-key (scram-hmac-sha256 stored-key auth-message)))
    (unwind-protect
         (progn
           ;; Recover the client key from the proof and compare its hash
           ;; with the stored key in constant time.
           (dotimes (index +scram-key-bytes+)
             (setf (aref client-key index) (logxor (aref client-key index) (aref proof index))))
           (let ((computed (scram-sha256 client-key)))
             (sb-sys:with-pinned-objects (stored-key computed)
               (when (zerop (scram-crypto-compare
                             (sb-sys:vector-sap stored-key) (sb-sys:vector-sap computed)
                             +scram-key-bytes+))
                 (scram-hmac-sha256 server-key auth-message)))))
      (fill client-key 0))))
