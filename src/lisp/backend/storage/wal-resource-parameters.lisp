(in-package #:pg)

(defconstant +resource-parameters-size+ 32)

(defstruct (resource-parameters
            (:constructor %make-resource-parameters (connections workers senders prepared locks)))
  "Immutable five-field resource tuple, constructed explicitly with checked values.
NIL means unknown and is not a valid tuple. This codec does not supply defaults
for legacy data or publish resource requirements. A consumer must associate a
tuple with its applied LSN and timeline, not use later received values for an
older replay seed."
  (connections 1 :read-only t)
  (workers 0 :read-only t)
  (senders 0 :read-only t)
  (prepared 0 :read-only t)
  (locks 10 :read-only t))

(defun resource-parameters-values (parameters)
  (list (resource-parameters-connections parameters)
        (resource-parameters-workers parameters)
        (resource-parameters-senders parameters)
        (resource-parameters-prepared parameters)
        (resource-parameters-locks parameters)))

(defun resource-parameters-valid-p (parameters)
  "Check field ranges and minimum feasible process count.
The minimum includes one autovacuum slot and two special processes; local
capacity calculation checks the actual autovacuum slot count separately."
  (and (resource-parameters-p parameters)
       (typep (resource-parameters-connections parameters) '(integer 1 262143))
       (every (lambda (value) (typep value '(integer 0 262143)))
              (list (resource-parameters-workers parameters)
                    (resource-parameters-senders parameters)
                    (resource-parameters-prepared parameters)))
       (typep (resource-parameters-locks parameters) '(integer 10 2147483647))
       (<= (+ (resource-parameters-connections parameters)
              (resource-parameters-workers parameters)
              (resource-parameters-senders parameters) 1 2)
           262143)))

(defun make-resource-parameters (connections workers senders prepared locks)
  "Construct a checked tuple from all five explicit values."
  (let ((parameters (%make-resource-parameters connections workers senders prepared locks)))
    (unless (resource-parameters-valid-p parameters)
      (error "invalid resource parameters"))
    parameters))

(defun resource-parameters-from-values (values)
  "Validate five explicit values and reconstruct an immutable tuple."
  (unless (and (listp values) (= 5 (length values)))
    (error "resource parameters require five explicit values"))
  (apply #'make-resource-parameters values))

(defun wal-encode-resource-parameters (parameters)
  "Encode the native 32-byte PGRES001 tuple with five little-endian words.
The reserved final word is zero. This is not C physical WAL and has no CRC;
the containing format must provide integrity protection."
  (unless (resource-parameters-valid-p parameters)
    (error "invalid resource parameters"))
  (let ((bytes (make-array +resource-parameters-size+
                           :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 82 69 83 48 48 49))
    (loop for value in (resource-parameters-values parameters)
          for offset from 8 by 4
          do (wal-set-u32 bytes offset value))
    bytes))

(defun wal-decode-resource-parameters (bytes)
  "Decode and validate the complete native tuple; reject malformed input.
Unknown requirements are represented by NIL outside this codec."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (= (length bytes) +resource-parameters-size+)
               (equalp (subseq bytes 0 8) #(80 71 82 69 83 48 48 49))
               (zerop (wal-u32 bytes 28)))
    (error "invalid resource parameter encoding"))
  (apply #'make-resource-parameters
         (loop for offset from 8 below 28 by 4 collect (wal-u32 bytes offset))))

(defun resource-parameters-shortfalls (local required)
  "Return local values below the required fields, in C parameter check order.
Compare the supplied tuple as-is, so a later requirement may decrease. Unknown
or invalid tuples are errors rather than an empty shortfall list."
  (unless (and (resource-parameters-valid-p local) (resource-parameters-valid-p required))
    (error "resource parameter requirements are unknown or invalid"))
  (loop for name in '("max_connections" "max_worker_processes" "max_wal_senders"
                      "max_prepared_transactions" "max_locks_per_transaction")
        for current in (resource-parameters-values local)
        for minimum in (resource-parameters-values required)
        when (< current minimum) collect (list (copy-seq name) current minimum)))

(defun resource-lock-capacities (parameters &key (autovacuum-worker-slots 16))
  "Calculate local object and owner capacities using actual autovacuum slots.
MaxBackends excludes prepared transactions; N includes them and the owner
capacity is twice N. The default autovacuum slot count is 16."
  (unless (and (resource-parameters-valid-p parameters)
               (typep autovacuum-worker-slots '(integer 1 262143)))
    (error "invalid resource capacity inputs"))
  (let* ((backends (+ (resource-parameters-connections parameters)
                      (resource-parameters-workers parameters)
                      (resource-parameters-senders parameters)
                      autovacuum-worker-slots 2))
         (objects (* (resource-parameters-locks parameters)
                     (+ backends (resource-parameters-prepared parameters)))))
    (when (> backends 262143) (error "too many server processes configured"))
    (values objects (* 2 objects))))

(defun wal-resource-parameters-hex (parameters)
  "Render the fixed native tuple as hexadecimal for control metadata."
  (with-output-to-string (stream)
    (loop for byte across (wal-encode-resource-parameters parameters)
          do (format stream "~2,'0X" byte))))

(defun wal-resource-parameters-from-hex (text)
  "Decode exactly one hexadecimal tuple and validate its reserved word."
  (unless (and (stringp text) (= (length text) (* 2 +resource-parameters-size+))
               (every (lambda (character) (digit-char-p character 16)) text))
    (error "invalid resource parameter hex encoding"))
  (let ((bytes (make-array +resource-parameters-size+ :element-type '(unsigned-byte 8))))
    (loop for index below (length bytes)
          do (setf (aref bytes index)
                   (parse-integer text :start (* index 2) :end (+ (* index 2) 2) :radix 16)))
    (wal-decode-resource-parameters bytes)))
