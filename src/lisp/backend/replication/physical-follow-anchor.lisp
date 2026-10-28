(in-package #:pg)

(defun physical-follow-pin-check-live (pin)
  "Require a healthy primary and the same live, retained pin under the WAL lock."
  (wal-check-healthy)
  (when (recovery-in-progress-p)
    (wal-invalid-input "physical follow anchor source has entered recovery"))
  (unless (and (eq pin (gethash (physical-source-pin-name pin) *physical-source-pins*))
               (not (physical-source-pin-lost-p pin)))
    (wal-invalid-input "physical follow retention is lost or replaced; reseed is required"))
  (physical-follow-pin-check-source pin))

(defun physical-follow-pin-check-source-eor (pin)
  "Compare the requested end-of-recovery bytes with the selected source lineage."
  (multiple-value-bind (history fork expected)
      (physical-follow-anchor-check (physical-follow-pin-anchor pin))
    (declare (ignore history fork))
    (let* ((start (wal-record-start expected))
           (segment (floor start +wal-segment-size+))
           (offset (mod start +wal-segment-size+))
           (raw (coerce (getf (physical-follow-pin-anchor pin) :eor-octets)
                        '(vector (unsigned-byte 8)))))
      (unless (<= (wal-record-end expected) *wal-durable-lsn*)
        (wal-invalid-input "physical follow end-of-recovery is not durable"))
      (let ((bytes (physical-follow-source-segment pin segment start)))
        (unless (and (<= (+ offset (length raw)) (length bytes))
                     (equalp raw (subseq bytes offset (+ offset (length raw)))))
          (wal-invalid-input "physical follow end-of-recovery differs from source WAL"))))))

(defun physical-source-follow-reserve (name request)
  "Reserve source WAL for an anchor under admission, write and WAL exclusion.

Validate the request before entering the WAL critical section. A malformed
request does not poison healthy WAL. Retention is installed before saving the
pin; an ambiguous save fences the source and keeps retention until restart."
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
  "Hash the nonempty retained overlap independently of the carried receive seed."
  (with-live-operation
    (let* ((request (physical-follow-pin-anchor pin))
           (start (getf request :overlap-start)) (end (getf request :receive-lsn))
           (segment (floor start +wal-segment-size+))
           (history (physical-follow-pin-history pin))
           (bytes nil) (file-timeline nil))
      (sb-thread:with-mutex (*wal-lock*)
        (physical-follow-pin-check-live pin)
        (when (physical-follow-pin-ready-p pin)
          (return-from physical-follow-pin-proof-digest (values nil t)))
        (multiple-value-setq (bytes file-timeline)
          (physical-follow-source-segment pin segment end)))
      (let ((reached
              (wal-lineage-scan-segment
               (lambda (record owner file) (declare (ignore record owner file)))
               bytes segment (length bytes) 0 end history file-timeline nil)))
        (unless (= reached end)
          (wal-invalid-input "physical follow overlap does not end at a source record boundary")))
      (physical-store-update-digest
       #xCBF29CE484222325 (subseq bytes 0 (- end start))))))

(defun physical-follow-pin-check-suffix (pin)
  "Validate selected ancestry and child WAL through a captured durable endpoint."
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
                 (let ((bytes nil) (timeline nil))
                   (sb-thread:with-mutex (*wal-lock*)
                     (physical-follow-pin-check-live pin)
                     (when (physical-follow-pin-ready-p pin)
                       (return-from physical-follow-pin-check-suffix t))
                     (multiple-value-setq (bytes timeline)
                       (physical-follow-source-segment pin segment cursor)))
                   (setf cursor
                         (wal-lineage-scan-segment
                          (lambda (record owner file)
                            (declare (ignore owner file))
                            (when (= (wal-record-type record) +wal-record-end-of-recovery+)
                              (let* ((fields (wal-end-of-recovery-record-info record))
                                     (parent (getf fields :prev-timeline))
                                     (fork (timeline-history-entry-end
                                            (find parent history :key #'timeline-history-entry-timeline))))
                                (unless (and fork (= logical-cursor fork))
                                  (wal-invalid-input "physical follow suffix does not reach its fork"))
                                (timeline-check-end-of-recovery record parent history)))
                            (setf logical-cursor (wal-record-end record)))
                          bytes segment (length bytes) offset end history timeline gap)
                         gap nil)
                   (when (< cursor end)
                     (let ((next (* (1+ segment) +wal-segment-size+)))
                       (when (< cursor next)
                         (when (<= end next)
                           (wal-invalid-input "physical follow suffix ends in a rotation gap"))
                         (setf gap cursor cursor next))))))))
    t))

(defun physical-source-follow-ready (name request)
  "Reserve and prove an anchor, then durably mark its pin ready.

READY is eligibility for a future follow path, not network admission. Save
before publishing the ready state in memory; a save failure fences the source."
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
