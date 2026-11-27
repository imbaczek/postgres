;;; Bounded hash join for consumers that only need the cardinality.
(in-package #:pg)

(defconstant +int4-hash-slot-count+ 4096)
(defconstant +int4-hash-load-limit+ (floor (* +int4-hash-slot-count+ 2) 3))
(defconstant +int4-hash-batch-size+ 256)

(defstruct int4-hash-spill-part
  path stream state (count 0 :type fixnum) single-key single-key-high single-key-third (distinct-p nil))

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

(defun map-fixed-hash-spill-buffers (part width consume &optional read-buffer)
  (declare (dynamic-extent consume))
  (when part
    (with-query-temp-input (stream (int4-hash-spill-part-path part) nil t)
      (let* ((buffer (or read-buffer (hash-spill-state-buffer (hash-spill-state stream))))
             (fd (hash-spill-fd stream))
             (size (length buffer))
             (remaining (int4-hash-spill-part-count part)))
        (declare (type fixnum size remaining width))
        (loop while (plusp remaining)
              for keys fixnum = (min remaining (floor size width))
              for wanted fixnum = (* keys width)
              do (check-for-interrupts)
                 (loop for start fixnum = 0 then (+ start read)
                       while (< start wanted)
                       for read fixnum = (hash-spill-system-io #'sb-posix:read fd buffer start (- wanted start))
                       when (zerop read) do (error "Truncated fixed-width hash partition"))
                 (when *statement-resource-usage*
                   (incf (statement-resource-usage-spill-read-bytes *statement-resource-usage*) wanted))
                 (funcall consume buffer wanted)
                 (decf remaining keys))))))

(defun map-int4-hash-spill-keys (part consume)
  (declare (dynamic-extent consume))
  (map-fixed-hash-spill-buffers
   part 4 (lambda (buffer wanted)
            (loop for offset fixnum from 0 below wanted by 4
                  do (funcall consume (heap-read-int4 buffer offset))))))

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

(defun count-fixed-hash-partitions (builds probes shift load-limit split leaf stats &optional refine-shift)
  (let ((count 0))
    (map nil
         (lambda (build probe)
           (when stats
             (incf (hash-join-stats-spill-batches stats) (if (or build probe) 1 0)))
           (cond
             ((and build probe (int4-hash-spill-part-distinct-p build)
                   (> (int4-hash-spill-part-count build) load-limit))
              (unless (plusp shift) (error "Fixed-width hash partition exhausted its distinct key bits"))
              (let* ((bits (min (1+ (mod (1- shift) 32)) 4
                                (integer-length (1- (ceiling (int4-hash-spill-part-count build)
                                                             load-limit)))))
                     (next (- shift bits))
                     (fanout (ash 1 bits))
                     (child-builds (funcall split build next fanout nil))
                     (child-probes (funcall split probe next fanout child-builds)))
                (when refine-shift
                  (let* ((index (position-if #'identity child-builds))
                         (only (and index (svref child-builds index))))
                    (when (and only (svref child-probes index) (int4-hash-spill-part-distinct-p only)
                               (> (int4-hash-spill-part-count only) load-limit)
                               (= 1 (count-if #'identity child-builds)))
                      (setf next (min next (funcall refine-shift only))))))
                (incf count (count-fixed-hash-partitions child-builds child-probes next load-limit
                                                         split leaf stats refine-shift))))
             (t
              (when (and build probe)
                (let ((matches (funcall leaf build probe)))
                  (when stats
                    (incf (hash-join-stats-build-blocks stats))
                    (incf (hash-join-stats-key-probes stats) (int4-hash-spill-part-count probe)))
                  (incf count matches)))
              (when build (delete-query-temp-file (int4-hash-spill-part-path build)))
              (when probe (delete-query-temp-file (int4-hash-spill-part-path probe))))))
         builds probes)
    count))

(defun append-int8-hash-spill-key (part low high)
  (declare (type (signed-byte 32) low high))
  (let ((state (int4-hash-spill-part-state part)))
    (when (= (hash-spill-state-cursor state) +hash-spill-buffer-bytes+)
      (flush-int4-hash-spill-part part))
    (let ((offset (hash-spill-state-cursor state)) (buffer (hash-spill-state-buffer state)))
      (int4-hash-store-key buffer offset low)
      (int4-hash-store-key buffer (+ offset 4) high)
      (incf (hash-spill-state-cursor state) 8)))
  (cond
    ((zerop (int4-hash-spill-part-count part))
     (setf (int4-hash-spill-part-single-key part) low
           (int4-hash-spill-part-single-key-high part) high))
    ((or (not (eql low (int4-hash-spill-part-single-key part)))
         (not (eql high (int4-hash-spill-part-single-key-high part))))
     (setf (int4-hash-spill-part-distinct-p part) t)))
  (incf (int4-hash-spill-part-count part)))

(defun map-int8-hash-spill-keys (part consume)
  (declare (dynamic-extent consume))
  (map-fixed-hash-spill-buffers
   part 8 (lambda (buffer wanted)
            (loop for offset fixnum from 0 below wanted by 8
                  do (funcall consume (heap-read-int4 buffer offset)
                              (heap-read-int4 buffer (+ offset 4)))))))

(declaim (inline int8-hash-word))

(defun int8-hash-word (low high)
  "Permute all 64 input bits for routing and probing."
  (declare (optimize (speed 3) (safety 1))
           (type (signed-byte 32) low high))
  (let ((word (logior (ldb (byte 32 0) low)
                      (ash (ldb (byte 32 0) high) 32))))
    (declare (type (unsigned-byte 64) word))
    (setf word (ldb (byte 64 0) (* (logxor word (ash word -30)) #xbf58476d1ce4e5b9))
          word (ldb (byte 64 0) (* (logxor word (ash word -27)) #x94d049bb133111eb)))
    (logxor word (ash word -31))))

(defun route-int8-hash-spill-key (parts build-parts shift mask low high)
  (declare (type simple-vector parts)
           (type (integer 0 63) shift) (type fixnum mask)
           (type (signed-byte 32) low high))
  (let ((index (logand (ash (int8-hash-word low high) (- shift)) mask)))
    (when (or (not build-parts) (svref build-parts index))
      (let ((part (or (svref parts index)
                      (setf (svref parts index) (open-int4-hash-spill-part)))))
        (append-int8-hash-spill-key part low high)))))

(defun spill-int8-hash-table-keys (table snapshot keys bits present observer)
  (let* ((fanout (ash 1 bits))
         (parts (make-array fanout :initial-element nil))
         (shift (- 64 bits))
         (mask (1- fanout)))
    (flet ((scan ()
             (let ((rows
                     (map-visible-int8-heap-key-batches
                      table snapshot keys
                      (lambda (batch count)
                        (declare (type (simple-array (signed-byte 32) (*)) batch) (type fixnum count))
                        (loop for index fixnum from 0 below count by 2
                              do (route-int8-hash-spill-key parts present shift mask
                                                            (aref batch index) (aref batch (1+ index))))))))
               (values (seal-int4-hash-spill-parts parts) rows))))
      (declare (dynamic-extent #'scan))
      (if observer (funcall observer #'scan) (scan)))))

(defun split-int8-hash-part (part shift fanout present)
  (let ((parts (make-array fanout :initial-element nil))
        (mask (1- fanout)))
    (map-int8-hash-spill-keys part
                              (lambda (low high)
                                (route-int8-hash-spill-key parts present shift mask low high)))
    (seal-int4-hash-spill-parts parts)
    (delete-query-temp-file (int4-hash-spill-part-path part))
    parts))

(declaim (inline int8-hash-frequency-slot))

(defun int8-hash-frequency-slot (lows highs counts low high)
  (declare (optimize (speed 3) (safety 1))
           (type (simple-array (signed-byte 32) (*)) lows highs)
           (type (simple-array (unsigned-byte 16) (*)) counts)
           (type (signed-byte 32) low high))
  (loop with mask fixnum = (1- +int4-hash-slot-count+)
        for slot fixnum = (logand (int8-hash-word low high) mask)
          then (logand (1+ slot) mask)
        when (or (zerop (aref counts slot))
                 (and (= (aref lows slot) low) (= (aref highs slot) high)))
          return slot))

(defun count-int8-hash-leaf (build probe lows highs counts)
  (if (int4-hash-spill-part-distinct-p build)
      (let ((count 0))
        (declare (type (simple-array (signed-byte 32) (*)) lows highs)
                 (type (simple-array (unsigned-byte 16) (*)) counts))
        (fill counts 0)
        (map-int8-hash-spill-keys
         build (lambda (low high)
                 (let ((slot (int8-hash-frequency-slot lows highs counts low high)))
                   (setf (aref lows slot) low (aref highs slot) high)
                   (incf (aref counts slot)))))
        (map-int8-hash-spill-keys
         probe (lambda (low high)
                 (incf count (aref counts (int8-hash-frequency-slot lows highs counts low high)))))
        count)
      (let ((low (int4-hash-spill-part-single-key build))
            (high (int4-hash-spill-part-single-key-high build))
            (matches 0))
        (map-int8-hash-spill-keys probe (lambda (l h) (when (and (= l low) (= h high)) (incf matches))))
        (* matches (int4-hash-spill-part-count build)))))

(defun count-int8-heap-hash-join (build probe snapshot &key stats observe-build observe-probe)
  "Use a 40 KiB paired-word frequency table under the minimum SQL work_mem.
A mixed-key leaf has at most the load-limit row count, so 16-bit frequencies
suffice. Partitions with a single distinct key multiply full counts directly."
  (with-query-temp-files ()
                         (let* ((bits 5)
                                (batch (make-array (* 2 +int4-hash-batch-size+) :element-type '(signed-byte 32)))
                                (builds (spill-int8-hash-table-keys build snapshot batch bits nil observe-build))
                                (probes (spill-int8-hash-table-keys probe snapshot batch bits builds observe-probe))
                                (lows (make-array +int4-hash-slot-count+ :element-type '(signed-byte 32)))
                                (highs (make-array +int4-hash-slot-count+ :element-type '(signed-byte 32)))
                                (counts (make-array +int4-hash-slot-count+ :element-type '(unsigned-byte 16))))
                           (when stats
                             (unless (hash-join-stats-key-probes stats) (setf (hash-join-stats-key-probes stats) 0))
                             (setf (hash-join-stats-peak-build-bytes stats) (* +int4-hash-slot-count+ 10)))
                           (count-fixed-hash-partitions builds probes (- 64 bits) +int4-hash-load-limit+
                                                        #'split-int8-hash-part
                                                        (lambda (build probe)
                                                          (count-int8-hash-leaf build probe lows highs counts))
                                                        stats))))

(defun append-interval-hash-spill-key (part low days high)
  (declare (type (signed-byte 32) low days high))
  (let ((state (int4-hash-spill-part-state part)))
    (when (> (+ (hash-spill-state-cursor state) 10) +hash-spill-buffer-bytes+)
      (flush-int4-hash-spill-part part))
    (let ((offset (hash-spill-state-cursor state)) (buffer (hash-spill-state-buffer state)))
      (int4-hash-store-key buffer offset low)
      (int4-hash-store-key buffer (+ offset 4) days)
      (setf (aref buffer (+ offset 8)) (ldb (byte 8 0) high)
            (aref buffer (+ offset 9)) (ldb (byte 8 8) high))
      (incf (hash-spill-state-cursor state) 10)))
  (cond
    ((zerop (int4-hash-spill-part-count part))
     (setf (int4-hash-spill-part-single-key part) low
           (int4-hash-spill-part-single-key-high part) days
           (int4-hash-spill-part-single-key-third part) high))
    ((and (not (int4-hash-spill-part-distinct-p part))
          (or (/= low (int4-hash-spill-part-single-key part))
              (/= days (int4-hash-spill-part-single-key-high part))
              (/= high (int4-hash-spill-part-single-key-third part))))
     (setf (int4-hash-spill-part-distinct-p part) t)))
  (incf (int4-hash-spill-part-count part)))

(defun map-interval-hash-spill-keys (part read-buffer consume)
  (declare (dynamic-extent consume))
  (map-fixed-hash-spill-buffers
   part 10 (lambda (buffer wanted)
             (declare (type (simple-array (unsigned-byte 8) (*)) buffer))
             (loop for offset fixnum from 0 below wanted by 10
                   for low = (heap-read-int4 buffer offset)
                   for days = (heap-read-int4 buffer (+ offset 4))
                   for high = (- (aref buffer (+ offset 9))
                                 (if (logbitp 7 (aref buffer (+ offset 9))) 256 0))
                   do (funcall consume low days
                               (logior (aref buffer (+ offset 8)) (ash high 8)))))
   read-buffer))

(defun route-interval-hash-spill-key (parts build-parts shift mask low days high)
  (declare (type simple-vector parts)
           (type (integer 0 95) shift) (type fixnum mask)
           (type (signed-byte 32) low days high))
  ;; Given HIGH, this 64-bit permutation plus HIGH is injective.
  (let ((index (logand (if (>= shift 32)
                           (ash (int8-hash-word low (logxor days high)) (- 32 shift))
                           (ash high (- shift)))
                       mask)))
    (when (or (not build-parts) (svref build-parts index))
      (let ((part (or (svref parts index)
                      (setf (svref parts index) (open-int4-hash-spill-part)))))
        (append-interval-hash-spill-key part low days high)))))

(defun spill-interval-hash-table-keys (table snapshot keys bits present observer)
  (let* ((fanout (ash 1 bits))
         (parts (make-array fanout :initial-element nil))
         (shift (- 96 bits))
         (mask (1- fanout)))
    (flet ((scan ()
             (let ((rows
                     (map-visible-fixed-heap-key-batches
                      table snapshot keys
                      (lambda (batch count)
                        (declare (type (simple-array (signed-byte 32) (*)) batch) (type fixnum count))
                        (loop for index fixnum from 0 below count by 3
                              do (route-interval-hash-spill-key
                                  parts present shift mask (aref batch index)
                                  (aref batch (+ index 1)) (aref batch (+ index 2)))))
                      #'map-visible-interval-page-key-batches)))
               (values (seal-int4-hash-spill-parts parts) rows))))
      (declare (dynamic-extent #'scan))
      (if observer (funcall observer #'scan) (scan)))))

(defun split-interval-hash-part (part shift fanout present read-buffer)
  (let ((parts (make-array fanout :initial-element nil))
        (mask (1- fanout)))
    (map-interval-hash-spill-keys part read-buffer
                                  (lambda (low days high)
                                    (route-interval-hash-spill-key parts present shift mask low days high)))
    (seal-int4-hash-spill-parts parts)
    (delete-query-temp-file (int4-hash-spill-part-path part))
    parts))

(defun interval-hash-shift-limit (part read-buffer)
  "Find the first useful split after all build keys followed one partition."
  (let* ((first-high (int4-hash-spill-part-single-key-third part))
         (first-hash (int8-hash-word (int4-hash-spill-part-single-key part)
                                     (logxor (int4-hash-spill-part-single-key-high part) first-high)))
         (varying-hash 0)
         (varying-high 0))
    (declare (type (signed-byte 32) first-high)
             (type (unsigned-byte 64) first-hash varying-hash)
             (type (unsigned-byte 32) varying-high))
    (map-interval-hash-spill-keys
     part read-buffer (lambda (low days high)
                        (declare (type (signed-byte 32) low days high))
                        (setf varying-hash (logior varying-hash (logxor first-hash (int8-hash-word low (logxor days high))))
                              varying-high (logior varying-high (ldb (byte 32 0) (logxor first-high high))))))
    (if (zerop varying-hash)
        (integer-length varying-high)
        (+ 32 (integer-length varying-hash)))))

(declaim (inline interval-hash-frequency-slot))

(defun interval-hash-frequency-slot (lows day-words highs counts low days high)
  (declare (optimize (speed 3) (safety 1))
           (type (simple-array (signed-byte 32) (*)) lows day-words)
           (type (simple-array (signed-byte 16) (*)) highs)
           (type (simple-array (unsigned-byte 16) (*)) counts)
           (type (signed-byte 32) low days high))
  (loop with mask fixnum = (1- (length counts))
        for slot fixnum = (logand (int8-hash-word low (logxor days high)) mask)
          then (logand (1+ slot) mask)
        when (or (zerop (aref counts slot))
                 (and (= (aref lows slot) low) (= (aref day-words slot) days)
                      (= (aref highs slot) high)))
          return slot))

(defun count-interval-hash-leaf (build probe lows day-words highs counts read-buffer)
  (if (int4-hash-spill-part-distinct-p build)
      (let ((count 0))
        (declare (type (simple-array (signed-byte 32) (*)) lows day-words)
                 (type (simple-array (signed-byte 16) (*)) highs)
                 (type (simple-array (unsigned-byte 16) (*)) counts))
        (fill counts 0)
        (map-interval-hash-spill-keys
         build read-buffer (lambda (low days high)
                             (let ((slot (interval-hash-frequency-slot lows day-words highs counts low days high)))
                               (setf (aref lows slot) low (aref day-words slot) days (aref highs slot) high)
                               (incf (aref counts slot)))))
        (map-interval-hash-spill-keys
         probe read-buffer (lambda (low days high)
                             (incf count (aref counts (interval-hash-frequency-slot lows day-words highs counts low days high)))))
        count)
      (let ((low (int4-hash-spill-part-single-key build))
            (days (int4-hash-spill-part-single-key-high build))
            (high (int4-hash-spill-part-single-key-third build))
            (matches 0))
        (map-interval-hash-spill-keys probe read-buffer (lambda (l d h)
                                                          (when (and (= l low) (= d days) (= h high))
                                                            (incf matches))))
        (* matches (int4-hash-spill-part-count build)))))

(defun count-interval-heap-hash-join (build probe snapshot &key stats observe-build observe-probe)
  "Count with 10-byte canonical interval keys and a 48 KiB frequency table.
The high word combines at most five remainder bits with the upper day
bits of the canonical span, so a signed 16-bit array holds it exactly."
  (with-query-temp-files ()
                         (let* ((bits 6)
                                (slots +int4-hash-slot-count+)
                                (batch (make-array (* 3 +int4-hash-batch-size+) :element-type '(signed-byte 32)))
                                (builds (spill-interval-hash-table-keys build snapshot batch bits nil observe-build))
                                (probes (spill-interval-hash-table-keys probe snapshot batch bits builds observe-probe))
                                (lows (make-array slots :element-type '(signed-byte 32)))
                                (day-words (make-array slots :element-type '(signed-byte 32)))
                                (highs (make-array slots :element-type '(signed-byte 16)))
                                (counts (make-array slots :element-type '(unsigned-byte 16)))
                                ;; The 48 KiB table and at most sixteen 672-byte split writers coexist.
                                (read-buffer (make-array 4096 :element-type '(unsigned-byte 8))))
                           (when stats
                             (unless (hash-join-stats-key-probes stats) (setf (hash-join-stats-key-probes stats) 0))
                             (setf (hash-join-stats-peak-build-bytes stats) (* slots 12)))
                           (count-fixed-hash-partitions builds probes (- 96 bits) +int4-hash-load-limit+
                                                        (lambda (part shift fanout present)
                                                          (split-interval-hash-part part shift fanout present read-buffer))
                                                        (lambda (build probe)
                                                          (count-interval-hash-leaf build probe lows day-words highs counts read-buffer))
                                                        stats (lambda (part) (interval-hash-shift-limit part read-buffer))))))

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
                           (count-fixed-hash-partitions builds probes (- 32 bits) +int4-hash-load-limit+
                                                        #'split-int4-hash-part
                                                        (lambda (build probe)
                                                          (count-int4-hash-leaf build probe keys counts))
                                                        stats))))
