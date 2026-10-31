(in-package #:pg)

(defconstant +resource-parameters-size+ 32)

(defstruct (resource-parameters
            (:constructor %make-resource-parameters (connections workers senders prepared locks)))
  "Immutable five-field resource tuple, constructed explicitly with checked values.
NIL means unknown and is not a valid tuple. This codec does not supply defaults
for legacy data. A consumer must associate a tuple with its applied LSN and
timeline, not use later received values for an
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

(defun wal-parameter-change-record-info (record)
  "Validate a native version 8 parameter terminal and decode its tuple.
This record is not PostgreSQL C physical WAL."
  (unless (and (= (wal-record-type record) +wal-record-parameter-change+)
               (>= (wal-record-version record) 8)
               (zerop (wal-record-xid record))
               (typep (wal-record-start record) '(unsigned-byte 64))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (= (wal-record-end record)
                  (+ (wal-record-start record) +wal-header-size+
                     (length (wal-record-payload record))))
               (> (wal-record-end record) (wal-record-start record))
               (= (floor (wal-record-start record) +wal-segment-size+)
                  (floor (1- (wal-record-end record)) +wal-segment-size+)))
    (error "invalid native parameter change record"))
  (wal-decode-resource-parameters (wal-record-payload record)))

(declaim (ftype function wal-save-control-fields wal-read-control-fields
                wal-control-metadata-valid-p wal-control-resource-parameters))

(defvar *wal-resource-parameters-test-hook* nil
  "Optional test hook at resource WAL and control publication boundaries.")

(defun wal-resource-parameters-test-point (point end)
  "Call the test hook with the publication point and record end."
  (when *wal-resource-parameters-test-hook*
    (funcall *wal-resource-parameters-test-hook* point end)))

(defun wal-publish-resource-parameters-under-lock (parameters end timeline)
  "Save a durable record's tuple and coordinates under the WAL lock.
Read back the complete control image before installing the new memory copy."
  (unless (and (wal-control-metadata-valid-p *wal-control-metadata*)
               (resource-parameters-valid-p parameters)
               (typep end '(integer 1 18446744073709551615))
               (<= end *wal-durable-lsn*)
               (eql timeline *wal-insert-timeline*))
    (error "invalid primary resource publication"))
  (let ((published
          (let ((*wal-control-metadata*
                  (append
                   (loop for (key value) on *wal-control-metadata* by #'cddr
                         unless (member key '(:resource-parameters :resource-lsn :resource-timeline))
                           append (list key value))
                   (list :resource-parameters (resource-parameters-values parameters)
                         :resource-lsn end :resource-timeline timeline))))
            (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*)
            (let ((actual (multiple-value-list (wal-read-control-fields))))
              (unless (equal actual
                             (list *wal-checkpoint-lsn* *wal-durable-lsn*
                                   *wal-checkpoint-snapshot* *wal-insert-timeline*
                                   *wal-checkpoint-timeline* *wal-segment-namespace*
                                   *wal-control-metadata*))
                (error "resource control readback differs from published image"))
              (seventh actual)))))
    (setf *wal-control-metadata* published))
  (values))

(defun wal-report-resource-parameters (parameters)
  "Report a primary tuple, returning its durable record end.
Preflight and lock waits remain interruptible unless the caller masked interrupts;
unchanged values need no publication. Mask interrupts from append through flush,
control publication and readback. An incomplete publication poisons WAL under the
lock, and protected cleanup requests fail-stop after the lock unwinds."
  (prevent-recovery-primary-operation "primary resource reporting")
  (unless (resource-parameters-valid-p parameters) (error "invalid resource parameters"))
  (let ((started nil) (completed nil) (failure nil) (result nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (sb-sys:with-local-interrupts
             (sb-thread:with-mutex (*wal-lock*)
               (wal-check-healthy)
               (unless (and *wal-ready* (wal-control-metadata-valid-p *wal-control-metadata*))
                 (error "primary control identity is not initialized"))
               (if (equal (resource-parameters-values parameters)
                          (getf *wal-control-metadata* :resource-parameters))
                   (setf result (getf *wal-control-metadata* :resource-lsn))
                   (sb-sys:without-interrupts
                     (unwind-protect
                          (handler-case
                              (progn
                                (setf started t)
                                (wal-append-batch-start)
                                (wal-append-record +wal-record-parameter-change+ 0
                                                   (wal-encode-resource-parameters parameters))
                                (wal-flush)
                                (wal-resource-parameters-test-point :after-wal-flush *wal-end-lsn*)
                                (wal-publish-resource-parameters-under-lock
                                 parameters *wal-end-lsn* *wal-insert-timeline*)
                                (wal-resource-parameters-test-point :after-control-save *wal-end-lsn*)
                                (setf result *wal-end-lsn* completed t))
                            (error (condition) (setf failure condition)))
                       (unless completed (setf *wal-poisoned* t)))))))
        (cond
          (failure (wal-fail-stop failure))
          ((and started (not completed))
           (when (fboundp 'request-fast-shutdown)
             (ignore-errors (funcall (symbol-function 'request-fast-shutdown))))))))
    result))

(defun wal-recover-resource-parameters (record)
  "Publish the latest replayed primary tuple under WAL exclusion.
An equal position must match control; older records do not replace it. Later
values replace earlier ones even when they decrease."
  (let* ((parameters (wal-parameter-change-record-info record))
         (end (wal-record-end record))
         (prior (getf *wal-control-metadata* :resource-lsn)))
    (cond
      ((or (null prior) (> end prior))
       (wal-publish-resource-parameters-under-lock parameters end *wal-insert-timeline*))
      ((= end prior)
       (unless (and (eql *wal-insert-timeline* (getf *wal-control-metadata* :resource-timeline))
                    (equal (resource-parameters-values parameters)
                           (getf *wal-control-metadata* :resource-parameters)))
         (error "resource WAL differs from control at its published position")))))
  (values))

(defconstant +wal-resource-fact-size+ 48)

(declaim (ftype function wal-control-resource-fields-valid-p wal-control-resource-parameters))

(defun wal-resource-fact-valid-p (fact)
  "Check the canonical tuple, record end LSN and origin timeline together."
  (and (listp fact) (= 6 (length fact))
       (equal (loop for (key value) on fact by #'cddr collect key)
              '(:resource-parameters :resource-lsn :resource-timeline))
       (wal-control-resource-fields-valid-p fact)))

(defun wal-encode-resource-fact (fact)
  "Encode a positioned fact in 48 bytes: tuple, end LSN, timeline and zero reserved word."
  (unless (wal-resource-fact-valid-p fact) (error "invalid positioned resource fact"))
  (let ((bytes (make-array +wal-resource-fact-size+ :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes (wal-encode-resource-parameters (wal-control-resource-parameters fact)))
    (wal-set-u64 bytes 32 (getf fact :resource-lsn))
    (wal-set-u32 bytes 40 (getf fact :resource-timeline))
    bytes))

(defun wal-decode-resource-fact (bytes)
  "Decode exactly one positioned fact, rejecting a nonzero reserved word."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (= (length bytes) +wal-resource-fact-size+)
               (zerop (wal-u32 bytes 44)))
    (error "invalid positioned resource fact encoding"))
  (let ((fact (list :resource-parameters
                    (resource-parameters-values (wal-decode-resource-parameters (subseq bytes 0 32)))
                    :resource-lsn (wal-u64 bytes 32) :resource-timeline (wal-u32 bytes 40))))
    (unless (wal-resource-fact-valid-p fact) (error "invalid positioned resource fact"))
    fact))

(defun resource-recovery-admission (local fact hot-standby)
  "Compare installed capacity with a positioned source fact without changing state.
Return :DISABLED when hot standby is off, :UNKNOWN when the fact is absent, or
:READY or :INSUFFICIENT with the ordered list of shortfalls."
  (unless (resource-parameters-valid-p local) (error "invalid installed resource parameters"))
  (when (and fact (not (wal-resource-fact-valid-p fact)))
    (error "invalid recovery resource fact"))
  (cond
    ((not hot-standby) :disabled)
    ((null fact) :unknown)
    (t
     (let ((shortfalls (resource-parameters-shortfalls local (wal-control-resource-parameters fact))))
       (values (if shortfalls :insufficient :ready) shortfalls)))))
