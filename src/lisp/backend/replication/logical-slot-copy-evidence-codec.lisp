;;; Encode and decode the bounded permanent evidence envelope.

;;; Permanent import evidence checks describe recorded facts, not authority.
(in-package #:pg)

(defconstant +logical-slot-copy-evidence-snapshot-maximum+ 16384)

(defconstant +logical-slot-copy-evidence-maximum-bytes+
  (+ 24 20 (* 3 16 1024 1024)
     +logical-slot-copy-evidence-record-maximum+ +physical-recovery-control-maximum-bytes+
     28 +logical-slot-copy-evidence-snapshot-maximum+
     (* 7 (+ 12 +logical-slot-copy-evidence-record-maximum+))))

(defun logical-slot-copy-import-evidence-parts (value)
  "Validate bounded raw evidence parts and the exact child frame plan.
The roughly 48 MiB bound includes three large blobs without hex expansion."
  (unless (and (= 12 (logical-slot-copy-descriptor-list-count value 12))
               (equal (loop for key in value by #'cddr collect key)
                      '(:manifest :checkpoint :control :selected-history :child-history :plan)))
    (error "invalid logical import evidence fields"))
  (let* ((blobs (loop for key in '(:manifest :checkpoint :control :selected-history :child-history)
                      collect (getf value key)))
         (plan (logical-slot-copy-evidence-check-plan (getf value :plan)))
         (snapshot (string->bytes (physical-bootstrap-manifest-text (getf plan :snapshot))))
         (size (+ 24 20 28 (length snapshot)
                  (loop for bytes in blobs
                        for minimum in '(1 1 1 0 1)
                        for maximum in (list (* 16 1024 1024)
                                             +logical-slot-copy-evidence-record-maximum+
                                             +physical-recovery-control-maximum-bytes+
                                             +physical-store-history-maximum-bytes+
                                             +physical-store-history-maximum-bytes+)
                        sum (logical-slot-copy-descriptor-blob-size bytes minimum maximum))
                  (loop for (start bytes) in (getf plan :frames)
                        sum (+ 12 (length bytes))))))
    (unless (<= size +logical-slot-copy-evidence-maximum-bytes+)
      (error "logical import evidence exceeds its encoded bound"))
    (values blobs plan snapshot size)))

(defun logical-slot-copy-import-evidence-encode (value)
  "Encode observational evidence; CRC covers all bytes except its four-byte trailer."
  (multiple-value-bind (blobs plan snapshot size) (logical-slot-copy-import-evidence-parts value)
    (let ((bytes (make-array size :element-type '(unsigned-byte 8) :initial-element 0))
          (position 20))
      (replace bytes #(80 71 76 79 71 73 77 80 79 82 84 68 65 84 65 49))
      (wal-set-u32 bytes 16 (- size 24))
      (labels ((u32 (number) (wal-set-u32 bytes position number) (incf position 4))
               (u64 (number) (wal-set-u64 bytes position number) (incf position 8))
               (blob (data)
                 (u32 (length data)) (replace bytes data :start1 position)
                 (incf position (length data))))
        (dolist (data blobs) (blob data))
        (u64 (getf plan :fork)) (u32 (getf plan :child)) (u64 (getf plan :end))
        (blob snapshot)
        (u32 (length (getf plan :frames)))
        (dolist (frame (getf plan :frames))
          (u64 (first frame)) (blob (second frame))))
      (unless (= position (- size 4)) (error "logical import evidence size changed"))
      (wal-set-u32 bytes position (wal-crc32 bytes 0 position))
      bytes)))

(defun logical-slot-copy-import-evidence-decode (bytes)
  "Decode bounded, checksummed canonical evidence without granting authority."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= 24 (length bytes) +logical-slot-copy-evidence-maximum-bytes+)
               (equalp (subseq bytes 0 16) #(80 71 76 79 71 73 77 80 79 82 84 68 65 84 65 49))
               (= (wal-u32 bytes 16) (- (length bytes) 24))
               (= (wal-u32 bytes (- (length bytes) 4))
                  (wal-crc32 bytes 0 (- (length bytes) 4))))
    (error "invalid logical import evidence envelope"))
  (let ((position 20) (end (- (length bytes) 4)))
    (labels ((take (count)
               (unless (<= count (- end position)) (error "truncated logical import evidence"))
               (prog1 position (incf position count)))
             (u32 () (wal-u32 bytes (take 4)))
             (u64 () (wal-u64 bytes (take 8)))
             (blob (minimum maximum)
               (let ((size (u32)))
                 (unless (<= minimum size maximum) (error "oversized logical import evidence blob"))
                 (subseq bytes (take size) position))))
      (let* ((manifest (blob 1 (* 16 1024 1024)))
             (checkpoint (blob 1 +logical-slot-copy-evidence-record-maximum+))
             (control (blob 1 +physical-recovery-control-maximum-bytes+))
             (selected (blob 0 +physical-store-history-maximum-bytes+))
             (history (blob 1 +physical-store-history-maximum-bytes+))
             (fork (u64)) (child (u32)) (child-end (u64))
             (snapshot (recovery-parse-manifest-text
                        (bytes->string (blob 1 +logical-slot-copy-evidence-snapshot-maximum+))))
             (count (u32)))
        (unless (member count '(3 5 7)) (error "invalid logical import evidence frame count"))
        (let* ((frames (loop repeat count
                             collect (let ((start (u64)))
                                       (list start (blob +wal-header-size+
                                                         +logical-slot-copy-evidence-record-maximum+)))))
               (value (list :manifest manifest :checkpoint checkpoint :control control
                            :selected-history selected :child-history history
                            :plan (list :fork fork :child child :snapshot snapshot
                                        :end child-end :frames frames))))
          (unless (= position end) (error "trailing logical import evidence data"))
          (unless (equalp bytes (logical-slot-copy-import-evidence-encode value))
            (error "noncanonical logical import evidence"))
          value)))))
