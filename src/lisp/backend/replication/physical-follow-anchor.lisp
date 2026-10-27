(in-package #:pg)

(defun physical-follow-pin-check-live (pin)
  (wal-check-healthy)
  (when (recovery-in-progress-p)
    (wal-invalid-input "physical follow anchor source has entered recovery"))
  (unless (and (eq pin (gethash (physical-source-pin-name pin) *physical-source-pins*))
               (not (physical-source-pin-lost-p pin)))
    (wal-invalid-input "physical follow retention is lost or replaced; reseed is required"))
  (physical-follow-pin-check-source pin))

(defun physical-follow-pin-check-source-eor (pin)
  (multiple-value-bind (history fork expected)
      (physical-follow-anchor-check (physical-follow-pin-anchor pin))
    (declare (ignore fork))
    (let* ((start (wal-record-start expected))
           (segment (floor start +wal-segment-size+))
           (offset (mod start +wal-segment-size+))
           (raw (coerce (getf (physical-follow-pin-anchor pin) :eor-octets)
                        '(vector (unsigned-byte 8))))
           (bytes (make-array (length raw) :element-type '(unsigned-byte 8))))
      (unless (<= (wal-record-end expected) *wal-durable-lsn*)
        (wal-invalid-input "physical follow end-of-recovery is not durable"))
      (multiple-value-bind (stream timeline size)
          (wal-lineage-open-segment segment history 0 :timeline)
        (unwind-protect
             (progn
               (unless (= timeline (physical-source-pin-timeline pin))
                 (wal-invalid-input "physical follow end-of-recovery lacks a source WAL extent"))
               (unless (<= (+ offset (length bytes))
                           (wal-lineage-effective-size size segment timeline history))
                 (wal-invalid-input "physical follow source lacks its end-of-recovery bytes"))
               (file-position stream offset)
               (unless (and (= (length bytes) (read-sequence bytes stream)) (equalp raw bytes))
                 (wal-invalid-input "physical follow end-of-recovery differs from source WAL")))
          (close stream))))))

(defun physical-source-follow-reserve (name request)
  (unless (wal-valid-replication-slot-name-p name)
    (wal-invalid-input "invalid physical follow pin name"))
  (let* ((name (copy-seq name))
         (anchor (physical-follow-anchor-copy request))
         (candidate
           (make-physical-follow-pin
            :name (copy-seq name) :system-id (getf anchor :system-id)
            :timeline (getf anchor :source-timeline) :manifest-digest (getf anchor :manifest-digest)
            :consistent-lsn (getf anchor :receive-lsn) :restart-lsn (getf anchor :overlap-start)
            :prefix-digest (getf anchor :receive-digest) :anchor anchor)))
    (physical-follow-pin-encode candidate)
    (with-live-operation
      (prevent-recovery-primary-operation "physical follow anchor reservation")
      (with-write-lock
        (with-wal-critical
          (let ((existing (gethash name *physical-source-pins*)))
            (when existing
              (unless (and (physical-follow-pin-p existing)
                           (equal anchor (physical-follow-pin-anchor existing)))
                (wal-invalid-input "physical follow pin name is already bound to another request"))
              (physical-follow-pin-check-live existing)
              (return-from physical-source-follow-reserve existing)))
          (when (probe-file (physical-source-pin-path name))
            (wal-invalid-input "physical follow pin file is unregistered; restart is required"))
          (handler-case
              (progn
                (physical-follow-pin-check-source candidate)
                (physical-follow-pin-check-retained-wal candidate)
                (physical-follow-pin-check-source-eor candidate))
            (wal-input-validation-error (condition) (error condition))
            (error (condition)
              (wal-invalid-input "physical follow source WAL is unavailable; reseed is required: ~a"
                                 condition)))
          (wal-ensure-directory-durable (physical-source-pin-directory))
          ;; This provisional memory reservation grants no stream or confirmation.
          ;; A failed save fences WAL; retention stays conservative until restart.
          (setf (gethash name *physical-source-pins*) candidate
                (gethash candidate *wal-snapshot-retention-pins*)
                (physical-source-pin-restart-lsn candidate))
          (physical-source-pin-save candidate)
          (physical-source-pin-test-point :after-follow-retention candidate)
          candidate)))))

(defun physical-follow-pin-proof-digest (pin)
  (with-live-operation
    (let* ((request (physical-follow-pin-anchor pin))
           (start (getf request :overlap-start)) (end (getf request :receive-lsn))
           (segment (floor start +wal-segment-size+))
           (history (physical-follow-pin-history pin))
           (stream nil) (file-timeline nil) (size nil))
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*wal-lock*)
               (physical-follow-pin-check-live pin)
               (when (physical-follow-pin-ready-p pin)
                 (return-from physical-follow-pin-proof-digest (values nil t)))
               (multiple-value-setq (stream file-timeline size)
                 (wal-lineage-open-segment segment history 0 :timeline)))
             (unless (= file-timeline (physical-source-pin-timeline pin))
               (wal-invalid-input "physical follow overlap lacks a complete source WAL extent"))
             (let ((reached
                     (wal-lineage-scan-segment
                      (lambda (record owner file) (declare (ignore record owner file)))
                      stream segment (wal-lineage-effective-size size segment file-timeline history)
                      0 end history file-timeline nil)))
               (unless (= reached end)
                 (wal-invalid-input "physical follow overlap does not end at a source record boundary")))
             (file-position stream 0)
             (loop with remaining = (- end start)
                   with buffer = (make-array (min 65536 remaining) :element-type '(unsigned-byte 8))
                   with digest = #xCBF29CE484222325
                   while (plusp remaining)
                   for count = (min remaining (length buffer))
                   do (unless (= count (read-sequence buffer stream :end count))
                        (wal-invalid-input "physical follow overlap is truncated"))
                      (setf digest
                            (physical-store-update-digest
                             digest (if (= count (length buffer)) buffer (subseq buffer 0 count))))
                      (decf remaining count)
                   finally (return digest)))
        (when stream (close stream))))))

(defun physical-follow-pin-check-suffix (pin)
  (let* ((history (physical-follow-pin-history pin))
         (cursor (physical-source-pin-consistent-lsn pin))
         (end (with-live-operation
                (sb-thread:with-mutex (*wal-lock*)
                  (physical-follow-pin-check-live pin)
                  *wal-durable-lsn*)))
         (gap nil)
         (logical-cursor cursor))
    (loop while (< cursor end)
          do (with-live-operation
               (multiple-value-bind (segment offset) (wal-segment-and-offset cursor)
                 (let ((stream nil) (timeline nil) (size nil))
                   (unwind-protect
                        (progn
                          (sb-thread:with-mutex (*wal-lock*)
                            (physical-follow-pin-check-live pin)
                            (when (physical-follow-pin-ready-p pin)
                              (return-from physical-follow-pin-check-suffix t))
                            (multiple-value-setq (stream timeline size)
                              (wal-lineage-open-segment segment history 0 :timeline)))
                          (unless (= timeline (physical-source-pin-timeline pin))
                            (wal-invalid-input "physical follow suffix lacks a complete source WAL extent"))
                          (setf cursor
                                (wal-lineage-scan-segment
                                 (lambda (record owner file)
                                   (declare (ignore owner file))
                                   (when (= (wal-record-type record) +wal-record-end-of-recovery+)
                                     (let* ((fields (wal-end-of-recovery-record-info record))
                                            (parent (getf fields :prev-timeline))
                                            (fork (timeline-history-entry-end
                                                   (find parent history
                                                         :key #'timeline-history-entry-timeline))))
                                       (unless (and fork (= logical-cursor fork))
                                         (wal-invalid-input "physical follow suffix does not reach its fork"))
                                       (timeline-check-end-of-recovery record parent history)))
                                   (setf logical-cursor (wal-record-end record)))
                                 stream segment
                                 (wal-lineage-effective-size size segment timeline history)
                                 offset end history timeline gap)
                                gap nil)
                          (when (< cursor end)
                            (let ((next (* (1+ segment) +wal-segment-size+)))
                              (when (< cursor next)
                                (when (<= end next)
                                  (wal-invalid-input "physical follow suffix ends in a rotation gap"))
                                (setf gap cursor cursor next)))))
                     (when stream (close stream)))))))
    t))

(defun physical-source-follow-ready (name request)
  (let ((pin (physical-source-follow-reserve name request)))
    (unless (physical-follow-pin-ready-p pin)
      (multiple-value-bind (digest already-ready) (physical-follow-pin-proof-digest pin)
        (unless (or already-ready
                    (= digest (getf (physical-follow-pin-anchor pin) :overlap-digest)))
          (wal-invalid-input "physical follow overlap differs from retained source bytes"))
        (physical-follow-pin-check-suffix pin)
        (physical-source-pin-test-point :after-follow-proof pin))
      (with-live-operation
        (with-write-lock
          (with-wal-critical
            (physical-follow-pin-check-live pin)
            (unless (physical-follow-pin-ready-p pin)
              (let* ((candidate (copy-physical-source-pin pin))
                     (cursor (physical-source-pin-consistent-lsn pin)))
                (setf (physical-follow-pin-stage candidate) :ready
                      (physical-source-pin-confirmed-lsn candidate) cursor
                      (physical-source-pin-restart-lsn candidate) cursor)
                (physical-follow-pin-check-fields candidate)
                (physical-source-pin-save candidate)
                (setf (physical-follow-pin-stage pin) :ready
                      (physical-source-pin-confirmed-lsn pin) cursor
                      (physical-source-pin-restart-lsn pin) cursor
                      (gethash pin *wal-snapshot-retention-pins*) cursor)
                (physical-source-pin-test-point :after-follow-ready pin)))))))
    pin))
