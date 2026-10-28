(in-package #:pg)

(defvar *physical-follow-aborted-attempts* (make-hash-table :test #'equal))

(defvar *physical-follow-aborted-attempt-limit* 65536)

(defun physical-follow-check-attempt-name (name)
  "Reject a canceled attempt name under WAL exclusion.
Names canceled by either release form are single-use until source restart."
  (when (gethash name *physical-follow-aborted-attempts*)
    (wal-invalid-input "physical follow attempt was canceled; use a new pin name")))

(defun physical-follow-abort-attempt-name (name)
  "Fence delayed reservations under the WAL lock without evicting older names.
The process-local set defaults to a limit of 65536 names. A new name at the limit fails
before acknowledgment, unlink or retention release. Restart ends all earlier
requests, so the set need not survive it."
  (unless (gethash name *physical-follow-aborted-attempts*)
    (unless (< (hash-table-count *physical-follow-aborted-attempts*)
               *physical-follow-aborted-attempt-limit*)
      (wal-invalid-input "physical follow canceled-attempt limit reached; source restart is required"))
    (setf (gethash (copy-seq name) *physical-follow-aborted-attempts*) t)))

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
          (physical-follow-check-attempt-name name)
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
  "Hash the nonempty physical overlap independently of the carried receive digest.
Neither the source archive endpoint digest nor its base overlap substitutes for
the receiver's digest or the independently compared overlap."
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
  "Validate selected ancestry and child WAL through a captured durable endpoint.
Use the source root and live segments independently of the receiver's base;
require complete records and the end-of-recovery transition at the fork."
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

(defun physical-source-follow-anchor (request)
  "Build an anchor from receiver coordinates and the current source's history.
Read the exact end-of-recovery record for the receiver's immediate next timeline
under WAL exclusion. The provisional pin is only a read context; it is never
registered or serialized. Reservation and proof revalidate retention afterward."
  (setf request (physical-follow-request-copy request))
  (with-live-operation
    (prevent-recovery-primary-operation "physical follow source descriptor")
    (sb-thread:with-mutex (*wal-lock*)
      (wal-check-healthy)
      (unless (and (= (getf request :system-id) (catalog-system-identifier))
                   (= (getf request :source-timeline) *wal-insert-timeline*)
                   (> *wal-insert-timeline* 1))
        (wal-invalid-input "physical follow descriptor belongs to another source"))
      (let* ((text (bytes->string (timeline-history-bytes *wal-insert-timeline*)))
             (history (timeline-check-lineage (timeline-parse-history *wal-insert-timeline* text)))
             (position (position (getf request :receive-timeline) history
                                 :key #'timeline-history-entry-timeline))
             (entry (and position (nth position history)))
             (fork (and entry (timeline-history-entry-end entry))))
        (unless (and fork position (plusp position)
                     (plusp (getf request :receive-lsn))
                     (<= (getf request :receive-lsn) fork))
          (wal-invalid-input "physical follow receiver is outside source ancestry"))
        (let* ((anchor (append (copy-list request) (list :history-text text)))
               (pin (make-physical-follow-pin
                     :system-id (getf request :system-id) :timeline *wal-insert-timeline*
                     :consistent-lsn (getf request :receive-lsn) :anchor anchor))
               (start (wal-record-start-after fork +wal-end-of-recovery-record-size+))
               (offset (mod start +wal-segment-size+))
               (bytes (physical-follow-source-segment pin (floor start +wal-segment-size+) start))
               (end (+ offset +wal-end-of-recovery-record-size+)))
          (unless (and (<= end (length bytes))
                       (<= (+ start +wal-end-of-recovery-record-size+) *wal-durable-lsn*))
            (wal-invalid-input "physical follow source end-of-recovery is unavailable"))
          (setf (getf anchor :eor-octets) (coerce (subseq bytes offset end) 'list))
          (physical-follow-anchor-copy anchor))))))

(defun physical-source-follow-existing-anchor (name request)
  "Return a copy of the live saved anchor for an exact request retry.
Reject a name bound to different coordinates or a pin that has lost retention.
The existing pin keeps any confirmation already advanced."
  (with-live-operation
    (prevent-recovery-primary-operation "physical follow preparation")
    (sb-thread:with-mutex (*wal-lock*)
      (wal-check-healthy)
      (physical-follow-check-attempt-name name)
      (let ((pin (gethash name *physical-source-pins*)))
        (when pin
          (unless (and (physical-follow-pin-p pin)
                       (loop for (key value) on request by #'cddr
                             always (equal value (getf (physical-follow-pin-anchor pin) key))))
            (wal-invalid-input "physical follow pin name is already bound to another request"))
          (physical-follow-pin-check-live pin)
          (physical-follow-anchor-copy (physical-follow-pin-anchor pin)))))))

(defun physical-source-follow-prepare (name request)
  "Prepare a ready source pin from receiver coordinates.
Reuse an exact live anchor when present. After descriptor capture fails, retry
the lookup for a concurrent preparation. If there is no match, translate a generic
capture failure to invalid input only while WAL is healthy; preserve PostgreSQL
and WAL validation errors and poisoned WAL failures. Reservation and proof
revalidate retention after capture and remain outside that error mapping, so an
ambiguous READY save is not reported as a reseed request."
  (unless (wal-valid-replication-slot-name-p name)
    (wal-invalid-input "invalid physical follow pin name"))
  (setf request (physical-follow-request-copy request))
  (let ((anchor
          (or (physical-source-follow-existing-anchor name request)
              (handler-case (physical-source-follow-anchor request)
                (error (condition)
                  (or (physical-source-follow-existing-anchor name request)
                      (if (or *wal-poisoned* (typep condition 'pg-error)
                              (typep condition 'wal-input-validation-error))
                          (error condition)
                          (wal-invalid-input
                           "physical follow source WAL is unavailable; reseed is required: ~a"
                           condition))))))))
    (physical-source-pin-test-point :after-follow-anchor-capture nil)
    (physical-source-follow-ready name anchor)))

(defun physical-source-follow-release-checked (name expected-digest request)
  "Retire a matching follow pin under WAL exclusion, including on a standby.
An exact request can retire retaining, ready or lost pins without old WAL; a
checksum requires a ready pin. Neither form authenticates the endpoint. Both
fence an absent or released attempt name against delayed preparation. Uncertain
I/O fences WAL; a mismatch or busy sender does not. A new name at the canceled-name
limit fails before acknowledgment, unlink or retention release."
  (check-recovery-available)
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid physical follow pin name"))
  (if request
      (setf request (physical-follow-request-copy request))
      (unless (typep expected-digest '(unsigned-byte 64))
        (ereport "22023" "invalid physical follow anchor digest")))
  (let ((result
          (with-write-lock
            (with-wal-critical
              (when (and request (/= (getf request :system-id) (catalog-system-identifier)))
                (wal-invalid-input "physical follow cleanup belongs to another source"))
              (let ((registered (gethash name *physical-source-pins*)))
                (when (and registered (not (physical-follow-pin-p registered)))
                  (wal-invalid-input "follow cleanup cannot retire a bootstrap pin")))
              (if (gethash name *physical-source-senders*) :busy
                  (multiple-value-bind (pin disk) (physical-source-retirement-pins name)
                    (cond
                      ((null pin)
                       (physical-follow-abort-attempt-name name)
                       (when (uiop:directory-exists-p (physical-source-pin-directory))
                         (fsync-directory (physical-source-pin-directory)))
                       :absent)
                      (t
                       (unless (and (physical-follow-pin-p pin) (physical-follow-pin-p disk)
                                    (or request (physical-follow-pin-ready-p pin))
                                    (if request
                                        (loop for (key value) on request by #'cddr
                                              always (equal value (getf (physical-follow-pin-anchor pin) key)))
                                        (= expected-digest
                                           (physical-follow-anchor-digest (physical-follow-pin-anchor pin)))))
                         (wal-invalid-input "physical follow release differs from its immutable identity"))
                       (physical-follow-abort-attempt-name name)
                       (physical-source-pin-unlink pin)
                       :released))))))))
    (when (eq result :busy) (ereport "55006" "physical follow pin has an active sender"))
    result))

(defun physical-source-follow-abort (name request)
  "Cancel an attempt using its exact saved receiver coordinates.
A retaining or ready pin, including a lost pin, needs no old WAL for release."
  (physical-source-follow-release-checked name nil request))

(defun physical-source-follow-release (name expected-digest)
  "Release a ready follow pin by its canonical immutable anchor checksum."
  (physical-source-follow-release-checked name expected-digest nil))

(declaim (ftype function physical-follow-bootstrap-request-copy))

(defun physical-source-bootstrap-release (name request)
  "Retire a matching bootstrap pin under WAL exclusion, including on a standby.
Check the immutable tuple and durable pin; reject a busy sender or follow pin.
The tuple is not authentication. Use attempt-specific backup names while
cleanup is outstanding, since an absent bootstrap name is not fenced."
  (check-recovery-available)
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid physical bootstrap pin name"))
  (setf request (physical-follow-bootstrap-request-copy request))
  (let ((result
          (with-write-lock
            (with-wal-critical
              (unless (= (getf request :system-id) (catalog-system-identifier))
                (wal-invalid-input "bootstrap retirement belongs to another source"))
              (if (gethash name *physical-source-senders*) :busy
                  (multiple-value-bind (pin disk) (physical-source-retirement-pins name)
                    (cond
                      ((null pin)
                       (when (uiop:directory-exists-p (physical-source-pin-directory))
                         (fsync-directory (physical-source-pin-directory)))
                       :absent)
                      (t
                       (when (or (physical-follow-pin-p pin) (physical-follow-pin-p disk))
                         (wal-invalid-input "bootstrap release cannot delete a follow pin"))
                       (unless (and (= (getf request :source-timeline) (physical-source-pin-timeline pin))
                                    (= (getf request :base) (physical-source-pin-consistent-lsn pin))
                                    (eql (getf request :manifest-digest) (physical-source-pin-manifest-digest pin)))
                         (wal-invalid-input "bootstrap release differs from its immutable identity"))
                       (physical-source-pin-unlink pin)
                       :released))))))))
    (when (eq result :busy) (ereport "55006" "physical bootstrap pin has an active sender"))
    result))
