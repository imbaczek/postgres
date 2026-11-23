;;;; Native WAL page-image payloads and location-bound page checksums.
(in-package #:pg)

(defstruct wal-page-image
  ;; XIDS lists actual writer FullXIDs, excluding internal markers 0 and 1.
  ;; Provenance describes the whole image: :unknown, :row, or :maintenance.
  relfilenode (fork :main) block page (xids '()) (provenance :unknown))

(defun wal-page-fork-id (fork)
  (case fork
    (:main 0)
    (:visibility 1)
    (otherwise (error "unknown relation fork ~a" fork))))

(defun wal-page-fork-from-id (fork-id)
  (case fork-id
    (0 :main)
    (1 :visibility)
    (otherwise (error "unknown WAL relation fork id ~a" fork-id))))

(defun wal-page-checksum (page relfilenode block &optional (fork :main))
  "Return a location-bound nonzero CRC16 for PAGE, reserving legacy values."
  (let* ((main-p (eq fork :main))
         (identity-size (if main-p 8 12))
         (copy (make-array (+ identity-size (length page))
                           :element-type '(unsigned-byte 8)
                           :initial-element 0)))
    (page-set-u32 copy 0 relfilenode)
    (if main-p
        (page-set-u32 copy 4 block)
        (progn
          (page-set-u32 copy 4 (wal-page-fork-id fork))
          (page-set-u32 copy 8 block)))
    (replace copy page :start1 identity-size)
    (page-set-u16 copy (+ identity-size +pd-checksum-off+) 0)
    (let ((checksum (logand (wal-crc32 copy) #xFFFF)))
      (if (<= checksum 3) (+ checksum 4) checksum))))

(defun wal-stamp-page (page lsn relfilenode block &optional (fork :main))
  (page-set-u64 page +pd-lsn-off+ lsn)
  (page-set-u16 page +pd-checksum-off+ 0)
  (page-set-u16 page +pd-checksum-off+
                (wal-page-checksum page relfilenode block fork))
  page)

(defun wal-verify-page (page relfilenode block &optional (fork :main))
  (let ((lsn (page-u64 page +pd-lsn-off+))
        (checksum (page-u16 page +pd-checksum-off+)))
    (if (zerop lsn)
        ;; Unstamped pages have no WAL LSN or checksum.
        ;; Persisted WAL pages always carry a nonzero LSN and must have a CRC.
        (zerop checksum)
        (= checksum (wal-page-checksum page relfilenode block fork)))))

(defun wal-page-image-encoded-size (image)
  (+ 12 +blcksz+ 4 (* 8 (length (wal-page-image-xids image)))))

(defun wal-page-payload-length (images)
  (+ 4 (loop for image in images sum (wal-page-image-encoded-size image))))

(defun wal-encode-page-images (images lsn)
  ;; The contributor count uses low 30 bits; high bits encode provenance:
  ;; 00 unknown, 10 row, 11 maintenance. 01 is reserved and rejected.
  (let ((payload (make-array (wal-page-payload-length images)
                             :element-type '(unsigned-byte 8)
                             :initial-element 0))
        (offset 4))
    (wal-set-u32 payload 0 (length images))
    (dolist (image images)
      (wal-stamp-page (wal-page-image-page image) lsn
                      (wal-page-image-relfilenode image)
                      (wal-page-image-block image)
                      (wal-page-image-fork image))
      (wal-set-u32 payload offset (wal-page-image-relfilenode image))
      (wal-set-u32 payload (+ offset 4)
                   (wal-page-fork-id (wal-page-image-fork image)))
      (wal-set-u32 payload (+ offset 8) (wal-page-image-block image))
      (replace payload (wal-page-image-page image) :start1 (+ offset 12))
      (let* ((xids (wal-page-image-xids image))
             (xid-count-offset (+ offset 12 +blcksz+))
             (xid-offset (+ xid-count-offset 4)))
        (unless (<= (length xids) #x3fffffff) (error "too many page contributors"))
        (wal-set-u32 payload xid-count-offset
                     (logior (length xids)
                             (ecase (wal-page-image-provenance image)
                               (:unknown 0) (:row #x80000000) (:maintenance #xc0000000))))
        (loop for xid in xids
              do (wal-set-u64 payload xid-offset xid)
                 (incf xid-offset 8))
        (setf offset xid-offset)))
    payload))

(defun wal-decode-page-images (payload lsn)
  (when (< (length payload) 4)
    (error "WAL page-image record is too short"))
  (let ((offset 4)
        (images nil))
    (loop repeat (wal-u32 payload 0)
          do (let* ((page-offset (+ offset 12))
                    (page-end (+ page-offset +blcksz+))
                    (xids-start (+ page-end 4)))
               (unless (<= page-end (length payload))
                 (error "WAL page-image record has a truncated page"))
               (unless (<= xids-start (length payload))
                 (error "WAL page-image record has a truncated contributor count"))
               (let* ((relfilenode (wal-u32 payload offset))
                      (fork (wal-page-fork-from-id (wal-u32 payload (+ offset 4))))
                      (block (wal-u32 payload (+ offset 8)))
                      (tagged-count (wal-u32 payload page-end))
                      (flags (ldb (byte 2 30) tagged-count))
                      (xids-end (+ xids-start (* (logand tagged-count #x3fffffff) 8))))
                 (when (= flags 1)
                   (error "invalid page provenance flags"))
                 (unless (<= xids-end (length payload))
                   (error "WAL page-image record has truncated contributors"))
                 (let ((page (subseq payload page-offset page-end)))
                   (unless (and (= (page-u64 page +pd-lsn-off+) lsn)
                                (wal-verify-page page relfilenode block fork))
                     (error "WAL page image has an invalid LSN or checksum"))
                   (push (make-wal-page-image
                          :relfilenode relfilenode :fork fork :block block :page page
                          :xids (loop for xid-offset from xids-start below xids-end by 8
                                      collect (wal-u64 payload xid-offset))
                          :provenance (ecase flags (0 :unknown) (2 :row) (3 :maintenance)))
                         images))
                 (setf offset xids-end))))
    (unless (= offset (length payload))
      (error "WAL page-image record has an invalid length"))
    (nreverse images)))
