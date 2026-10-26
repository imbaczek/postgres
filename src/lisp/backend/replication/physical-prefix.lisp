;;;; Preserve authenticated final-segment bytes before receive-floor advancement.
(in-package #:pg)

(defun physical-recovery-base-wal-prefix ()
  "Recheck the immutable manifest-bound base prefix; never read a sparse spool hole."
  (let* ((state *physical-recovery-state*) (base (getf state :end))
                                           (namespace (getf state :wal-namespace :legacy))
                                           (segment (floor base +wal-segment-size+)) (offset (mod base +wal-segment-size+))
                                           (name (concatenate 'string "pg_wal/" (wal-segment-filename segment 1 namespace)))
                                           (entry (find name (getf state :files) :key #'first :test #'equal)))
    (when (zerop offset)
      (return-from physical-recovery-base-wal-prefix (make-array 0 :element-type '(unsigned-byte 8))))
    (unless (and entry (= offset (second entry)))
      (error "base manifest lacks its exact final WAL prefix"))
    (multiple-value-bind (stream timeline size)
        (wal-lineage-open-segment segment
                                  (list (make-timeline-history-entry :timeline 1 :begin 0))
                                  0 namespace)
      (declare (ignore timeline))
      (unwind-protect
           (let ((bytes (make-array offset :element-type '(unsigned-byte 8))))
             (unless (and (= size offset) (= offset (read-sequence bytes stream))
                          (= (third entry) (physical-store-update-digest #xCBF29CE484222325 bytes)))
               (error "base WAL prefix differs from immutable provenance"))
             bytes)
        (close stream)))))

(defun physical-recovery-prefix-anchor (store cursor)
  "Return verified bytes covering segment start through FLOOR, or NIL if unavailable."
  (let* ((state *physical-recovery-state*) (floor (physical-store-floor-lsn store))
                                           (segment (floor cursor +wal-segment-size+))
                                           (anchor (getf state :wal-prefix-bytes)) (lsn (getf state :wal-prefix-lsn)))
    (cond
      ((and anchor (typep lsn '(unsigned-byte 64)) (<= floor lsn cursor)
            (= segment (floor lsn +wal-segment-size+))
            (= (length anchor) (mod lsn +wal-segment-size+)))
       anchor)
      ((= floor (getf state :end)) (physical-recovery-base-wal-prefix))
      (t nil))))

(defun physical-recovery-wal-prefix-under-lock (store cursor)
  "Reconstruct a complete final-segment prefix from authenticated anchors and records.
The caller owns admission/quiescence and STORE's mutex at an applied terminal.
NIL means a legacy compacted image lacks its pre-floor proof; #() is known empty.
No raw pre-floor spool bytes are trusted, and no data or receive state is changed."
  (let* ((state *physical-recovery-state*) (floor (physical-store-floor-lsn store))
                                           (segment (floor cursor +wal-segment-size+)) (offset (mod cursor +wal-segment-size+))
                                           (same-segment (= segment (floor floor +wal-segment-size+)))
                                           (covered (if same-segment (mod floor +wal-segment-size+) 0))
                                           (prefix (make-array offset :element-type '(unsigned-byte 8)))
                                           (anchor (when (plusp covered) (physical-recovery-prefix-anchor store cursor)))
                                           (available (or (zerop covered) anchor)))
    (unless (and (typep cursor '(unsigned-byte 64))
                 (<= floor cursor (physical-store-flushed-lsn store))
                 (= (physical-store-system-id store) (getf state :system-id))
                 (= (physical-store-timeline store) (getf state :timeline))
                 (= (physical-store-base-lsn store) (getf state :end)))
      (error "WAL prefix does not belong to current recovery progress"))
    (when anchor (replace prefix anchor :end1 covered :end2 covered))
    (let ((digest
            (nth-value 1
                       (physical-store-validate-prefix
                        store :digest-cursor cursor
                        :on-record
                        (lambda (record raw)
                          (when (and (= segment (floor (wal-record-start record) +wal-segment-size+))
                                     (<= (wal-record-end record) cursor))
                            (unless (= covered (mod (wal-record-start record) +wal-segment-size+))
                              (error "noncontiguous applied WAL prefix"))
                            (when available (replace prefix raw :start1 covered))
                            (incf covered (length raw))))))))
      (unless (and digest (= covered offset))
        (error "applied WAL prefix lacks an exact durable boundary")))
    (when available prefix)))

(defun physical-restartpoint-check-wal-prefix (metadata bytes)
  "Bind an optional captured prefix to the selected generation's exact boundary."
  (let ((digest (getf metadata :wal-prefix-digest)))
    (unless (if (eql 4 (getf metadata :version))
                (if digest
                    (and (typep digest '(unsigned-byte 64))
                         (typep bytes '(vector (unsigned-byte 8)))
                         (= (length bytes) (mod (getf metadata :lsn) +wal-segment-size+))
                         (= digest (physical-store-update-digest #xCBF29CE484222325 bytes)))
                    (null bytes))
                (null bytes))
      (error "invalid restartpoint WAL prefix proof")))
  bytes)
