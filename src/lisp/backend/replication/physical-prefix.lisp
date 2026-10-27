;;;; Preserve authenticated final-segment bytes before receive-floor advancement.
(in-package #:pg)

(defun physical-recovery-read-base-overlap (state directory)
  (let* ((base (getf state :end)) (timeline (getf state :timeline))
                                  (namespace (getf state :wal-namespace :legacy)))
    (unless (and (typep base '(integer 32 18446744073709551615))
                 (typep timeline '(integer 1 4294967295))
                 (member namespace '(:legacy :timeline))
                 (or (= timeline 1) (eq namespace :timeline)))
      (error "invalid immutable base overlap coordinates"))
    (let* ((segment (floor (1- base) +wal-segment-size+))
           (size (1+ (mod (1- base) +wal-segment-size+)))
           (name (concatenate 'string "pg_wal/" (wal-segment-filename segment timeline namespace)))
           (entry (find name (getf state :files) :key #'first :test #'equal)))
      (unless (and (listp entry) (= 3 (length entry))
                   (eql size (second entry)) (typep (third entry) '(unsigned-byte 64)))
        (error "base manifest lacks its exact overlap WAL file"))
      (let ((bytes (read-bounded-regular-file (merge-pathnames name directory) size :minimum size)))
        (unless (= (third entry) (physical-store-update-digest #xCBF29CE484222325 bytes))
          (error "base WAL overlap differs from immutable provenance"))
        (values bytes (copy-tree entry))))))

(defun physical-recovery-base-wal-prefix ()
  "Recheck the immutable manifest-bound base prefix; never read a sparse spool hole."
  (let ((base (getf *physical-recovery-state* :end)))
    (if (zerop (mod base +wal-segment-size+))
        (make-array 0 :element-type '(unsigned-byte 8))
        (physical-recovery-read-base-overlap *physical-recovery-state* *pgdata*))))

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
