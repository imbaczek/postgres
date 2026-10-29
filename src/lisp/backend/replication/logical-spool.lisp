;;;; Transaction-owned storage for encoded logical events.
(in-package #:pg)

(defparameter *logical-capture-memory-bytes* (* 4 1024 1024))

(defvar *logical-spool-test-after-measure* nil)

(defstruct (logical-spool (:constructor %make-logical-spool))
  owner (events '()) (bytes 0) (count 0) path stream sealed-p closed-p failed-p)

(defun make-logical-spool ()
  "Create an owner whose lifetime is the transaction, not a query scope."
  (let* ((quota (make-query-temp-quota *query-temp-file-limit-bytes*))
         (owner (make-query-temp-scope-owner quota)))
    (setf (query-temp-cleanup-owner-temporary-directory owner) *query-temp-directory*)
    (%make-logical-spool :owner owner)))

(defun call-with-logical-spool-owner (spool function)
  "Rebind the temporary-file machinery to SPOOL's persistent cleanup owner."
  (let* ((owner (logical-spool-owner spool))
         (*query-temp-cleanup-owner* owner)
         (*query-temp-file-scope-active-p* t)
         (*query-temp-quota* (query-temp-cleanup-owner-quota owner))
         (*query-temp-directory* (query-temp-cleanup-owner-temporary-directory owner))
         (*query-temp-scope-directory* (query-temp-cleanup-owner-directory owner))
         (*query-temp-files* nil))
    (unwind-protect (funcall function)
      (setf (query-temp-cleanup-owner-directory owner) *query-temp-scope-directory*))))

(defun logical-spool-write-event (spool event)
  "Write one length- and CRC-framed event, reserving quota before file I/O."
  (let ((length (length event)) (path (logical-spool-path spool))
                                (stream (logical-spool-stream spool)))
    (unless (<= 1 length #xffffffff) (error "invalid logical spool event length"))
    (query-temp-check-space path (+ 8 length))
    ;; Reserve before I/O so partial writes stay charged until cleanup.
    (query-temp-account-space path (+ 8 length))
    (dolist (word (list length (wal-crc32 event)))
      (dotimes (shift 4)
        (write-byte (logand (ash word (* -8 shift)) #xff) stream)))
    (write-sequence event stream)))

(defun logical-spool-ensure-file (spool)
  "Move buffered events to a transaction-owned file if needed."
  (unless (logical-spool-stream spool)
    (sb-sys:without-interrupts
      (multiple-value-bind (path stream) (open-query-temp-io)
        (setf (logical-spool-path spool) path
              (logical-spool-stream spool) stream)))
    (dolist (prior (reverse (logical-spool-events spool)))
      (logical-spool-write-event spool prior))
    (setf (logical-spool-events spool) nil))
  (values))

(defun logical-encoding-sink (limit &optional stream)
  "Return a sink and a metrics closure for bounded counting, CRC and optional output."
  (let ((size 0) (crc #xffffffff))
    (values
     (lambda (part)
       (let ((length (if (integerp part) 1 (length part))))
         (when (> (+ size length) limit)
           (ereport "54000" "logical change stream transaction exceeds the 64 MiB limit"))
         (incf size length)
         (flet ((step-byte (byte)
                  (setf crc (logxor (ash crc -8)
                                    (aref *wal-crc32-table* (logand (logxor crc byte) #xff))))))
           (if (integerp part)
               (progn (step-byte part) (when stream (write-byte part stream)))
               (progn (loop for byte across part do (step-byte byte))
                      (when stream (write-sequence part stream)))))))
     (lambda () (values size (logxor crc #xffffffff))))))

(defun logical-spool-append (spool event)
  "Take ownership of a fresh EVENT, spilling once the memory limit is reached.
The caller must not mutate EVENT after passing it to this function."
  (when (or (logical-spool-sealed-p spool) (logical-spool-closed-p spool)
            (logical-spool-failed-p spool))
    (error "logical spool is not writable"))
  (unless (and (typep event '(vector (unsigned-byte 8))) (plusp (length event)))
    (error "invalid logical spool event"))
  (let ((complete nil))
    (unwind-protect
         (progn
           (call-with-logical-spool-owner
            spool
            (lambda ()
              (when (and (null (logical-spool-stream spool))
                         (> (+ (logical-spool-bytes spool) (length event))
                            *logical-capture-memory-bytes*))
                (logical-spool-ensure-file spool))
              (if (logical-spool-stream spool)
                  (logical-spool-write-event spool event)
                  (push event (logical-spool-events spool)))
              (incf (logical-spool-bytes spool) (length event))
              (incf (logical-spool-count spool))))
           (setf complete t))
      (unless complete (setf (logical-spool-failed-p spool) t))))
  (values))

(defun logical-spool-append-change (spool change remaining)
  "Encode CHANGE into SPOOL without retaining a large encoded event vector."
  (when (or (logical-spool-sealed-p spool) (logical-spool-closed-p spool)
            (logical-spool-failed-p spool))
    (error "logical spool is not writable"))
  (let ((complete nil))
    (unwind-protect
         (multiple-value-bind (counter metrics) (logical-encoding-sink remaining)
           (logical-encode-change change counter)
           (multiple-value-bind (size checksum) (funcall metrics)
             (unless (<= 1 size #xffffffff) (error "invalid logical spool event length"))
             (when *logical-spool-test-after-measure*
               (funcall *logical-spool-test-after-measure* change))
             (if (and (null (logical-spool-stream spool))
                      (<= (+ (logical-spool-bytes spool) size) *logical-capture-memory-bytes*))
                 (let ((event (logical-encode-change change)))
                   (unless (and (= (length event) size) (= (wal-crc32 event) checksum))
                     (error "logical event changed during encoding"))
                   (logical-spool-append spool event))
                 (call-with-logical-spool-owner
                  spool
                  (lambda ()
                    (logical-spool-ensure-file spool)
                    (let ((path (logical-spool-path spool)) (stream (logical-spool-stream spool)))
                      (query-temp-check-space path (+ 8 size))
                      (query-temp-account-space path (+ 8 size))
                      (dolist (word (list size checksum))
                        (dotimes (shift 4)
                          (write-byte (logand (ash word (* -8 shift)) #xff) stream)))
                      (multiple-value-bind (writer written) (logical-encoding-sink size stream)
                        (logical-encode-change change writer)
                        (multiple-value-bind (actual-size actual-checksum) (funcall written)
                          (unless (and (= actual-size size) (= actual-checksum checksum))
                            (error "logical event changed during encoding"))))
                      (incf (logical-spool-bytes spool) size)
                      (incf (logical-spool-count spool))))))
             (setf complete t)
             size))
      (unless complete (setf (logical-spool-failed-p spool) t)))))

(defun logical-spool-seal (spool)
  "Flush completed events before a caller enters the WAL critical section."
  (when (or (logical-spool-closed-p spool) (logical-spool-failed-p spool))
    (error "logical spool cannot be sealed"))
  (unless (logical-spool-sealed-p spool)
    (let ((complete nil))
      (unwind-protect
           (progn
             (when (logical-spool-stream spool)
               (finish-output (logical-spool-stream spool)))
             (setf (logical-spool-sealed-p spool) t
                   complete t))
        (unless complete (setf (logical-spool-failed-p spool) t)))))
  (values))

(defun logical-spool-read-u32 (stream)
  (loop for shift below 4
        for byte = (read-byte stream nil nil)
        do (unless byte (error "truncated logical spool header"))
        sum (ash byte (* 8 shift))))

(defun %logical-spool-map-events (spool function)
  "Read and validate each event, retaining only one spilled event at a time."
  (unless (and (logical-spool-sealed-p spool) (not (logical-spool-closed-p spool)))
    (error "logical spool is not readable"))
  (if (null (logical-spool-stream spool))
      (dolist (event (reverse (logical-spool-events spool))) (funcall function event))
      (let ((stream (logical-spool-stream spool)) (total 0))
        (unless (file-position stream 0) (error "cannot seek logical spool"))
        (dotimes (index (logical-spool-count spool))
          (let ((length (logical-spool-read-u32 stream))
                (checksum (logical-spool-read-u32 stream)))
            (unless (and (plusp length) (<= (+ total length) (logical-spool-bytes spool)))
              (error "invalid logical spool event length"))
            (let ((event (make-array length :element-type '(unsigned-byte 8))))
              (unless (= (read-sequence event stream) length)
                (error "truncated logical spool event"))
              (unless (= (wal-crc32 event) checksum)
                (error "logical spool checksum mismatch"))
              (incf total length)
              (funcall function event))))
        (unless (and (= total (logical-spool-bytes spool))
                     (null (read-byte stream nil nil)))
          (error "invalid logical spool contents"))))
  (values))

(defun logical-spool-map-events (spool function)
  "Visit verified events in order, permanently rejecting this spool after an error."
  (when (logical-spool-failed-p spool) (error "logical spool has failed"))
  (let ((complete nil))
    (unwind-protect
         (progn (%logical-spool-map-events spool function) (setf complete t))
      (unless complete (setf (logical-spool-failed-p spool) t))))
  (values))

(defun logical-spool-events-in-order (spool)
  "Materialize verified events for callers that explicitly require a list."
  (let ((events '()))
    (logical-spool-map-events spool (lambda (event) (push event events)))
    (nreverse events)))

(defun logical-spool-map-event-blocks (spool start-function block-function end-function
                                       &key (block-size 65536))
  "Validate events and visit bounded byte ranges without constructing an event vector.
BLOCK-FUNCTION must consume its byte range before returning; file buffers are reused."
  (when (or (logical-spool-failed-p spool) (not (logical-spool-sealed-p spool))
            (logical-spool-closed-p spool) (not (and (integerp block-size) (plusp block-size))))
    (error "logical spool is not readable"))
  (let ((complete nil) (stream (logical-spool-stream spool)))
    (unwind-protect
         (progn
           (if (null stream)
               (dolist (event (reverse (logical-spool-events spool)))
                 (funcall start-function (length event))
                 (loop for start from 0 below (length event) by block-size
                       for end = (min (length event) (+ start block-size))
                       do (funcall block-function event start end))
                 (funcall end-function))
               (let ((buffer (make-array block-size :element-type '(unsigned-byte 8)))
                     (total 0))
                 (unless (file-position stream 0) (error "cannot seek logical spool"))
                 (dotimes (index (logical-spool-count spool))
                   (let* ((length (logical-spool-read-u32 stream))
                          (expected (logical-spool-read-u32 stream))
                          (crc #xffffffff))
                     (unless (and (plusp length) (<= (+ total length) (logical-spool-bytes spool)))
                       (error "invalid logical spool event length"))
                     (funcall start-function length)
                     (loop for remaining = length then (- remaining n)
                           while (plusp remaining)
                           for n = (min remaining block-size)
                           do (unless (= (read-sequence buffer stream :end n) n)
                                (error "truncated logical spool event"))
                              (loop for i below n for byte = (aref buffer i)
                                    do (setf crc (logxor (ash crc -8)
                                                         (aref *wal-crc32-table*
                                                               (logand (logxor crc byte) #xff)))))
                              (funcall block-function buffer 0 n))
                     (unless (= (logxor crc #xffffffff) expected)
                       (error "logical spool checksum mismatch"))
                     (incf total length)
                     (funcall end-function)))
                 (unless (and (= total (logical-spool-bytes spool)) (null (read-byte stream nil nil)))
                   (error "invalid logical spool contents"))))
           (setf complete t))
      (unless complete (setf (logical-spool-failed-p spool) t))))
  (values))

(defstruct logical-spool-wal-plan spool mode count)

(defun logical-preflight-spool (spool)
  "Validate SPOOL and plan bounded WAL records before publishing a batch."
  (logical-spool-seal spool)
  (when (> (logical-spool-bytes spool) +max-logical-wal-transaction-bytes+)
    (error "logical transaction exceeds its WAL change limit"))
  (let ((payload-limit (- +wal-segment-size+ +wal-header-size+
                          +logical-change-chunk-header-size+))
        (fragment-limit (- +wal-segment-size+ +wal-header-size+
                           +logical-fragment-header-size+))
        (chunk-bytes 0) (chunks 0) (fragments 0) (fragment-p nil))
    (logical-spool-map-event-blocks
     spool
     (lambda (size)
       (let ((entry (+ 4 size)))
         (incf fragments (ceiling size fragment-limit))
         (when (> entry payload-limit) (setf fragment-p t))
         (when (and (plusp chunk-bytes) (> (+ chunk-bytes entry) payload-limit))
           (incf chunks)
           (setf chunk-bytes 0))
         (incf chunk-bytes entry)))
     (lambda (buffer start end) (declare (ignore buffer start end)))
     (lambda ()))
    (when (plusp chunk-bytes) (incf chunks))
    (let ((count (if fragment-p fragments chunks)))
      (unless (<= 1 count #xffffffff) (error "invalid logical WAL chunk count"))
      (make-logical-spool-wal-plan :spool spool
                                   :mode (if fragment-p :fragment :legacy)
                                   :count count))))

(defun logical-append-spool-wal (xid plan)
  "Append a preflighted spool with bounded event and WAL record buffers.
A second read failure is a WAL critical-section failure, like other WAL I/O."
  (let ((spool (logical-spool-wal-plan-spool plan))
        (count (logical-spool-wal-plan-count plan))
        (index 0))
    (ecase (logical-spool-wal-plan-mode plan)
      (:fragment
       (let ((limit (- +wal-segment-size+ +wal-header-size+ +logical-fragment-header-size+))
             (total 0) (offset 0) (used 0) (fragment nil))
         (logical-spool-map-event-blocks
          spool
          (lambda (size) (setf total size offset 0 used 0 fragment nil))
          (lambda (buffer start end)
            (loop while (< start end)
                  do (unless fragment
                       (setf fragment (make-array (min limit (- total offset))
                                                  :element-type '(unsigned-byte 8))
                             used 0))
                     (let ((n (min (- end start) (- (length fragment) used))))
                       (replace fragment buffer :start1 used :start2 start :end2 (+ start n))
                       (incf used n)
                       (incf start n))
                     (when (= used (length fragment))
                       (wal-append-record +wal-record-logical-changes+ xid
                                          (logical-encode-fragment-bytes index count total offset fragment))
                       (incf index)
                       (incf offset used)
                       (setf fragment nil))))
          (lambda ()
            (unless (and (= offset total) (null fragment))
              (error "incomplete logical WAL fragment"))))))
      (:legacy
       (let ((limit (- +wal-segment-size+ +wal-header-size+ +logical-change-chunk-header-size+))
             (bytes 0) (events '()) (current nil) (offset 0))
         (flet ((flush-chunk ()
                  (when events
                    (wal-append-record +wal-record-logical-changes+ xid
                                       (logical-encode-chunk index count (nreverse events)))
                    (incf index)
                    (setf events nil bytes 0))))
           (logical-spool-map-event-blocks
            spool
            (lambda (size)
              (let ((entry (+ 4 size)))
                (when (> entry limit) (error "logical WAL spool plan changed during commit"))
                (when (and events (> (+ bytes entry) limit)) (flush-chunk))
                (incf bytes entry)
                (setf current (make-array size :element-type '(unsigned-byte 8)) offset 0)))
            (lambda (buffer start end)
              (replace current buffer :start1 offset :start2 start :end2 end)
              (incf offset (- end start)))
            (lambda ()
              (unless (= offset (length current)) (error "incomplete logical WAL event"))
              (push current events)
              (setf current nil)))
           (flush-chunk)))))
    (unless (= index count) (error "logical WAL spool plan changed during commit")))
  (values))

(defun close-logical-spool (spool)
  "Transfer cleanup ownership before forgetting any transaction resources."
  (sb-sys:without-interrupts
    (unless (logical-spool-closed-p spool)
      (call-with-logical-spool-owner
       spool (lambda () (finish-query-temp-scope)))
      (setf (logical-spool-closed-p spool) t
            (logical-spool-events spool) nil
            (logical-spool-stream spool) nil)))
  (values))
