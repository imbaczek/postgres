(in-package #:pg)

(defconstant +physical-retained-metadata-limit+ (* 1024 1024))
(defconstant +physical-retained-image-limit+ (+ +wal-segment-size+ +physical-retained-metadata-limit+ 20))
(defparameter *physical-retained-extent-keys*
  '(:version :system-id :base-timeline :base :manifest-digest :history-target
    :history-digest :history-text :start :end :start-digest :end-digest
    :segment :file-timeline :prefix-digest))

(defun physical-retained-extent-history (metadata)
  "Validate extent metadata and return its parsed lineage."
  (physical-follow-check-plist metadata *physical-retained-extent-keys*)
  (unless (and (eql 1 (getf metadata :version))
               (every (lambda (key) (typep (getf metadata key) '(unsigned-byte 64)))
                      '(:system-id :base :manifest-digest :start :end :start-digest
                        :end-digest :segment :prefix-digest))
               (every (lambda (key) (typep (getf metadata key) '(integer 1 4294967295)))
                      '(:base-timeline :history-target :file-timeline))
               (<= (getf metadata :base) (getf metadata :start))
               (< (getf metadata :start) (getf metadata :end))
               (if (= (getf metadata :base-timeline) (getf metadata :history-target))
                   (null (getf metadata :history-digest))
                   (typep (getf metadata :history-digest) '(unsigned-byte 64)))
               (stringp (getf metadata :history-text))
               (<= (length (getf metadata :history-text)) +physical-retained-metadata-limit+))
    (error "invalid retained WAL extent metadata"))
  (let ((history (timeline-check-lineage
                  (timeline-parse-history (getf metadata :history-target)
                                          (getf metadata :history-text)))))
    (unless (and (timeline-prefix-boundary-p (getf metadata :base-timeline)
                                             (getf metadata :base) history)
                 (find (getf metadata :file-timeline) history
                       :key #'timeline-history-entry-timeline))
      (error "retained WAL extent has foreign base or file timeline"))
    history))

(defun physical-retained-extent-check-record (record previous prior history file-timeline)
  "Check record placement and require an EOR at each timeline change."
  (let* ((owner (timeline-at-point (wal-record-start record) history))
         (entry (find owner history :key #'timeline-history-entry-timeline))
         (fork (timeline-history-entry-end entry)))
    (unless (and (>= file-timeline owner)
                 (or (null fork) (<= (wal-record-end record) fork)))
      (error "retained WAL record crosses history or uses an ancestor file"))
    (if (= owner prior)
        (wal-require-ordinary-record-type (wal-record-type record))
        (let* ((fields (wal-end-of-recovery-record-info record))
               (parent (getf fields :prev-timeline))
               (parent-entry (find parent history :key #'timeline-history-entry-timeline)))
          (unless (and (= parent prior) parent-entry
                       (eql previous (timeline-history-entry-end parent-entry)))
            (error "retained WAL end-of-recovery lacks its exact fork"))
          (timeline-check-end-of-recovery record parent history)))
    owner))

(defun physical-retained-extent-scan (metadata bytes history &optional digest-cursor)
  "Validate records against HISTORY and return the digest at DIGEST-CURSOR.

Hash record bytes in order, excluding legal rotation gaps.  Each owner change
requires an EOR. NIL means DIGEST-CURSOR is not a known boundary. The starting
digest is an input, not independently proved here."
  (let* ((origin (* (getf metadata :segment) +wal-segment-size+))
         (cursor (getf metadata :start)) (offset (max 0 (- cursor origin)))
         (digest (getf metadata :start-digest))
         (timeline (if (= cursor (getf metadata :base)) (getf metadata :base-timeline)
                       (timeline-at-point (1- cursor) history)))
         (found (and (eql cursor digest-cursor) digest)))
    (loop while (< offset (length bytes))
          do (unless (<= (+ offset +wal-header-size+) (length bytes))
               (error "truncated retained WAL header"))
             (let ((size (wal-u32 bytes (+ offset 8))))
               (unless (<= +wal-header-size+ size (- (length bytes) offset))
                 (error "truncated retained WAL record"))
               (let* ((raw (subseq bytes offset (+ offset size)))
                      (record (wal-decode-record raw (+ origin offset))))
                 (when (< cursor (wal-record-start record))
                   (wal-lineage-check-gap cursor record history))
                 (setf timeline (physical-retained-extent-check-record
                                 record cursor timeline history (getf metadata :file-timeline)))
                 (setf digest (physical-store-update-digest digest raw)
                       cursor (wal-record-end record))
                 (when (eql cursor digest-cursor) (setf found digest))
                 (incf offset size))))
    (unless (and (= cursor (getf metadata :end))
                 (= digest (getf metadata :end-digest)))
      (error "retained WAL extent lacks its exact end or receive digest"))
    found))

(defun physical-retained-extent-check (metadata bytes &key digest-cursor)
  "Check one segment image and its logical extent against its metadata.
Return METADATA and the requested boundary digest, or NIL when not found.

Bytes before START are auxiliary prefix material, not coverage.  Checksums and
the initial digest do not prove the image against a base or receive history."
  (let* ((history (physical-retained-extent-history metadata))
         (segment (getf metadata :segment))
         (origin (* segment +wal-segment-size+))
         (start (getf metadata :start)) (end (getf metadata :end)))
    (unless (and (typep bytes '(vector (unsigned-byte 8)))
                 (< 0 (length bytes) (1+ +wal-segment-size+))
                 (= (+ origin (length bytes)) end)
                 (= segment (floor (1- end) +wal-segment-size+))
                 (or (<= origin start end)
                     (and (plusp segment)
                          (= (floor start +wal-segment-size+) (1- segment))))
                 (= (getf metadata :prefix-digest)
                    (physical-store-update-digest #xCBF29CE484222325 bytes))
                 (or (/= start (getf metadata :base))
                     (= (getf metadata :start-digest) #xCBF29CE484222325)))
      (error "retained WAL extent bytes disagree with metadata"))
    (values metadata (physical-retained-extent-scan metadata bytes history digest-cursor))))

(defun physical-retained-extent-encode (metadata bytes)
  "Validate and encode one extent in a bounded checksum envelope."
  (physical-retained-extent-check metadata bytes)
  (let* ((text (string->bytes (physical-bootstrap-manifest-text metadata)))
         (size (+ 20 (length text) (length bytes))))
    (unless (<= (length text) +physical-retained-metadata-limit+)
      (error "retained WAL extent metadata exceeds its bound"))
    (let ((image (make-array size :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace image #(80 71 82 69 88 84 48 49))
      (wal-set-u32 image 8 (length text))
      (wal-set-u32 image 12 (length bytes))
      (replace image text :start1 16)
      (replace image bytes :start1 (+ 16 (length text)))
      (wal-set-u32 image (- size 4) (wal-crc32 image))
      image)))

(defun physical-retained-extent-decode (image)
  "Decode and validate an extent, returning fresh metadata and byte storage.

The checksum detects accidental corruption; it does not authenticate the image."
  (unless (and (typep image '(vector (unsigned-byte 8)))
               (<= 20 (length image) +physical-retained-image-limit+)
               (equalp (subseq image 0 8) #(80 71 82 69 88 84 48 49)))
    (error "invalid retained WAL extent envelope"))
  (let* ((metadata-size (wal-u32 image 8)) (data-size (wal-u32 image 12))
                                           (crc-offset (- (length image) 4)) (copy (copy-seq image)))
    (unless (and (<= metadata-size +physical-retained-metadata-limit+)
                 (<= data-size +wal-segment-size+)
                 (= (+ 20 metadata-size data-size) (length image)))
      (error "retained WAL extent length mismatch"))
    (wal-set-u32 copy crc-offset 0)
    (unless (= (wal-u32 image crc-offset) (wal-crc32 copy))
      (error "retained WAL extent checksum mismatch"))
    (let ((metadata (recovery-parse-manifest-text
                     (bytes->string (subseq image 16 (+ 16 metadata-size)))))
          (bytes (subseq image (+ 16 metadata-size) crc-offset)))
      (physical-retained-extent-check metadata bytes)
      (values metadata bytes))))

(defconstant +physical-retained-plan-maximum-bytes+ (* 64 1024 1024))

(defconstant +physical-retained-plan-maximum-images+ 1024)

(defun physical-retained-wal-check-budget (images &optional (maximum +physical-retained-plan-maximum-bytes+))
  "Check the candidate image count and byte budget before decoding."
  (loop with total = 0
        for image in images for count from 1
        do (unless (and (<= count +physical-retained-plan-maximum-images+)
                        (typep image '(vector (unsigned-byte 8))))
             (error "retained WAL plan image budget or type is invalid"))
           (incf total (length image))
           (when (> total maximum)
             (error "retained WAL plan exceeds its byte budget"))
        finally (return total)))

(defun physical-retained-wal-plan (images identity history start end)
  "Plan exact record-boundary coverage from a preselected batch of images.

Reject holes, overlaps, inconsistent shared prefixes and mismatched lineage.
The caller owns IMAGES during the call; decoded data and the returned plan are
fresh copies.  A future inventory must bind starting digests and prefixes to
base, restartpoint or receive evidence and select candidates before loading
them.  This plan grants no retention or serving authority."
  (physical-retained-wal-check-budget images)
  (physical-follow-check-plist identity '(:system-id :base-timeline :base :manifest-digest))
  (timeline-check-lineage history)
  (unless (and (typep start '(unsigned-byte 64))
               (typep end '(unsigned-byte 64)) (< start end))
    (error "invalid retained WAL coverage request"))
  (let* ((extents
           (sort (loop for image in images
                       collect (multiple-value-bind (metadata bytes)
                                   (physical-retained-extent-decode image)
                                 (list metadata bytes)))
                 #'< :key (lambda (item) (getf (first item) :start))))
         (previous nil) (cursor start) (digest nil) (plan nil)
         (prefixes (make-hash-table :test #'eql)))
    (dolist (item extents)
      (let ((metadata (first item)))
        (unless (and (every (lambda (key) (eql (getf identity key) (getf metadata key)))
                            '(:system-id :base-timeline :base :manifest-digest))
                     (physical-store-history-extends-p
                      (physical-retained-extent-history metadata) history))
          (error "retained WAL coverage has foreign identity or history"))
        (physical-retained-extent-scan metadata (second item) history)
        (let* ((segment (getf metadata :segment)) (bytes (second item))
                                                  (prior (gethash segment prefixes)))
          (when (and prior
                     (let ((size (min (length prior) (length bytes))))
                       (mismatch prior bytes :end1 size :end2 size)))
            (error "retained WAL extents disagree about their shared segment prefix"))
          (when (or (null prior) (> (length bytes) (length prior)))
            (setf (gethash segment prefixes) bytes)))
        (when previous
          (unless (<= (getf previous :end) (getf metadata :start))
            (error "retained WAL inventory contains overlapping extents"))
          (when (= (getf previous :end) (getf metadata :start))
            (unless (= (getf previous :end-digest) (getf metadata :start-digest))
              (error "retained WAL inventory has discontinuous receive digests"))))
        (setf previous metadata)))
    (dolist (item extents)
      (when (= cursor end) (return))
      (destructuring-bind (metadata bytes) item
        (let ((left (getf metadata :start)) (right (getf metadata :end)))
          (when (and (<= left cursor) (< cursor right))
            (let* ((limit (min right end))
                   (from (nth-value 1 (physical-retained-extent-check metadata bytes :digest-cursor cursor)))
                   (through (nth-value 1 (physical-retained-extent-check metadata bytes :digest-cursor limit))))
              (unless (and from through (or (null digest) (= digest from)))
                (error "retained WAL request lacks exact matching record boundaries"))
              (push (list :metadata metadata :bytes bytes :start cursor :end limit
                          :start-digest from :end-digest through) plan)
              (setf cursor limit digest through))))))
    (unless (= cursor end) (error "retained WAL inventory has a coverage hole"))
    (nreverse plan)))
