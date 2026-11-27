;;; Bounded hash join for consumers that only need the cardinality.
(in-package #:pg)

(defconstant +int4-hash-slot-count+ 4096)
(defconstant +int4-hash-load-limit+ (floor (* +int4-hash-slot-count+ 2) 3))
(defconstant +int4-hash-batch-size+ 256)

(defstruct int4-hash-spill-part
  path stream state (count 0 :type fixnum) single-key (distinct-p nil))

(declaim (inline int4-hash-word int4-hash-partition-slot int4-hash-store-key))

(defun int4-hash-word (key)
  ;; Odd multiplication is a permutation on the 32-bit key domain. Split on
  ;; high bits first so consecutive keys do not concentrate in one batch.
  (declare (type (signed-byte 32) key))
  (ldb (byte 32 0) (* key #x9e3779b1)))

(defun int4-hash-partition-slot (key shift mask)
  (declare (type (signed-byte 32) key) (type fixnum shift mask))
  (logand (ash (int4-hash-word key) (- shift)) mask))

(defun int4-hash-store-key (bytes offset key)
  "Store a little-endian key with statically typed octet writes in the hot loop.
The generic page store accepts vectors with arbitrary element types."
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum offset) (type (signed-byte 32) key))
  (setf (aref bytes offset) (ldb (byte 8 0) key)
        (aref bytes (+ offset 1)) (ldb (byte 8 8) key)
        (aref bytes (+ offset 2)) (ldb (byte 8 16) key)
        (aref bytes (+ offset 3)) (ldb (byte 8 24) key)))

(defun open-int4-hash-spill-part ()
  (multiple-value-bind (path stream) (open-query-temp-output nil t)
    (make-int4-hash-spill-part :path path :stream stream :state (hash-spill-state stream))))

(defun flush-int4-hash-spill-part (part)
  (let* ((state (int4-hash-spill-part-state part))
         (bytes (hash-spill-state-cursor state))
         (path (int4-hash-spill-part-path part))
         (stream (int4-hash-spill-part-stream part)))
    (when (plusp bytes)
      (query-temp-check-space path bytes)
      (write-hash-spill-buffer stream state bytes)
      (setf (hash-spill-state-cursor state) 0)
      (query-temp-account-space path bytes stream))))

(defun append-int4-hash-spill-key (part key)
  (declare (type (signed-byte 32) key))
  (let ((state (int4-hash-spill-part-state part)))
    (when (= (hash-spill-state-cursor state) +hash-spill-buffer-bytes+)
      (flush-int4-hash-spill-part part))
    (int4-hash-store-key (hash-spill-state-buffer state) (hash-spill-state-cursor state) key)
    (incf (hash-spill-state-cursor state) 4))
  (cond
    ((zerop (int4-hash-spill-part-count part)) (setf (int4-hash-spill-part-single-key part) key))
    ((not (eql key (int4-hash-spill-part-single-key part)))
     (setf (int4-hash-spill-part-distinct-p part) t)))
  (incf (int4-hash-spill-part-count part)))

(defun seal-int4-hash-spill-parts (parts)
  (loop for part across parts
        when part
          do (flush-int4-hash-spill-part part)
             (finish-query-temp-stream (int4-hash-spill-part-stream part) t)
             (setf (int4-hash-spill-part-stream part) nil
                   (int4-hash-spill-part-state part) nil))
  parts)

(defun map-int4-hash-spill-keys (part consume)
  (declare (dynamic-extent consume))
  (when part
    (with-query-temp-input (stream (int4-hash-spill-part-path part) nil t)
      (let* ((buffer (hash-spill-state-buffer (hash-spill-state stream)))
             (fd (hash-spill-fd stream))
             (size (length buffer))
             (remaining (int4-hash-spill-part-count part)))
        (declare (type fixnum size remaining))
        (loop while (plusp remaining)
              for keys fixnum = (min remaining (floor size 4))
              for wanted fixnum = (* keys 4)
              do (check-for-interrupts)
                 (loop for start fixnum = 0 then (+ start read)
                       while (< start wanted)
                       for read fixnum = (hash-spill-system-io #'sb-posix:read fd buffer start (- wanted start))
                       when (zerop read) do (error "Truncated integer hash partition"))
                 (when *statement-resource-usage*
                   (incf (statement-resource-usage-spill-read-bytes *statement-resource-usage*) wanted))
                 (loop for offset fixnum from 0 below wanted by 4 do
                   (funcall consume (heap-read-int4 buffer offset)))
                 (decf remaining keys))))))

(defun route-int4-hash-spill-key (parts build-parts shift mask key)
  (declare (type simple-vector parts))
  (let ((index (int4-hash-partition-slot key shift mask)))
    (when (or (not build-parts) (svref build-parts index))
      (let ((part (or (svref parts index)
                      (setf (svref parts index) (open-int4-hash-spill-part)))))
        (append-int4-hash-spill-key part key)))))

(defun spill-int4-hash-table-keys (table snapshot keys bits present &optional observer)
  (let* ((fanout (ash 1 bits))
         (parts (make-array fanout :initial-element nil))
         (shift (- 32 bits))
         (mask (1- fanout)))
    (flet ((scan ()
             (let ((rows
                     (map-visible-int4-heap-key-batches
                      table snapshot keys
                      (lambda (batch count)
                        (declare (type (simple-array (signed-byte 32) (*)) batch) (type fixnum count))
                        (dotimes (index count)
                          (route-int4-hash-spill-key parts present shift mask (aref batch index)))))))
               (values (seal-int4-hash-spill-parts parts) rows))))
      (declare (dynamic-extent #'scan))
      (if observer (funcall observer #'scan) (scan)))))

(defun split-int4-hash-part (part shift fanout present)
  (let ((parts (make-array fanout :initial-element nil))
        (mask (1- fanout)))
    (map-int4-hash-spill-keys part
                              (lambda (key)
                                (route-int4-hash-spill-key parts present shift mask key)))
    (seal-int4-hash-spill-parts parts)
    (delete-query-temp-file (int4-hash-spill-part-path part))
    parts))

(declaim (inline int4-hash-frequency-slot))

(defun int4-hash-frequency-slot (keys counts key)
  (declare (optimize (speed 3) (safety 1))
           (type (simple-array (signed-byte 32) (*)) keys)
           (type (simple-array (unsigned-byte 64) (*)) counts)
           (type (signed-byte 32) key))
  (loop with mask fixnum = (1- +int4-hash-slot-count+)
        with hash = (int4-hash-word key)
        for slot fixnum = (logand (logxor hash (ash hash -12) (ash hash -24)) mask)
          then (logand (1+ slot) mask)
        when (or (zerop (aref counts slot)) (= (aref keys slot) key))
          return slot))

(defun count-int4-hash-leaf (build probe keys counts)
  ;; A single distinct key needs no frequency table, even under heavy skew.
  (if (int4-hash-spill-part-distinct-p build)
      (let ((count 0))
        (declare (type (simple-array (signed-byte 32) (*)) keys)
                 (type (simple-array (unsigned-byte 64) (*)) counts))
        (fill counts 0)
        (map-int4-hash-spill-keys
         build (lambda (key)
                 (let ((slot (int4-hash-frequency-slot keys counts key)))
                   (setf (aref keys slot) key)
                   (incf (aref counts slot)))))
        (map-int4-hash-spill-keys
         probe (lambda (key)
                 (incf count (aref counts (int4-hash-frequency-slot keys counts key)))))
        count)
      (let ((key (int4-hash-spill-part-single-key build))
            (matches 0))
        (map-int4-hash-spill-keys probe (lambda (candidate) (when (= key candidate) (incf matches))))
        (* matches (int4-hash-spill-part-count build)))))

(defun count-int4-hash-partitions (builds probes shift keys counts stats)
  (let ((count 0))
    (map nil
         (lambda (build probe)
           (when stats
             (incf (hash-join-stats-spill-batches stats) (if (or build probe) 1 0)))
           (cond
             ((and build probe (int4-hash-spill-part-distinct-p build)
                   (> (int4-hash-spill-part-count build) +int4-hash-load-limit+))
              (unless (plusp shift) (error "Integer hash partition exhausted its distinct key bits"))
              (let* ((bits (min shift 4
                                (integer-length (1- (ceiling (int4-hash-spill-part-count build)
                                                             +int4-hash-load-limit+)))))
                     (next (- shift bits))
                     (fanout (ash 1 bits))
                     (child-builds (split-int4-hash-part build next fanout nil))
                     (child-probes (split-int4-hash-part probe next fanout child-builds)))
                (incf count (count-int4-hash-partitions child-builds child-probes next keys counts stats))))
             (t
              (when (and build probe)
                (let ((matches (count-int4-hash-leaf build probe keys counts)))
                  (when stats
                    (incf (hash-join-stats-build-blocks stats))
                    (incf (hash-join-stats-key-probes stats) (int4-hash-spill-part-count probe)))
                  (incf count matches)))
              (when build (delete-query-temp-file (int4-hash-spill-part-path build)))
              (when probe (delete-query-temp-file (int4-hash-spill-part-path probe))))))
         builds probes)
    count))

(defun count-int4-heap-hash-join (build probe snapshot &key stats observe-build observe-probe)
  "Count matches within the minimum SQL work_mem using compact key partitions.
Scan one input at a time, then reserve 48 KiB for the frequency table. A
repartition opens at most 16 charged writers and one reader beside that table."
  (with-query-temp-files ()
                         (let* ((bits 5)
                                (batch (make-array +int4-hash-batch-size+ :element-type '(signed-byte 32)))
                                (builds (spill-int4-hash-table-keys build snapshot batch bits nil observe-build))
                                (probes (spill-int4-hash-table-keys probe snapshot batch bits builds observe-probe))
                                (keys (make-array +int4-hash-slot-count+ :element-type '(signed-byte 32)))
                                (counts (make-array +int4-hash-slot-count+ :element-type '(unsigned-byte 64))))
                           (when stats
                             (unless (hash-join-stats-key-probes stats) (setf (hash-join-stats-key-probes stats) 0))
                             (setf (hash-join-stats-peak-build-bytes stats) (* +int4-hash-slot-count+ 12)))
                           (count-int4-hash-partitions builds probes (- 32 bits) keys counts stats))))
