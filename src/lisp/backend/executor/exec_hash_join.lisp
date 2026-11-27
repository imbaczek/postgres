;;;; Serial hash join over the executor's resumable row iterators.

(in-package #:pg)

(defun hash-join-key-type-p (type)
  "Whether SQL equality on TYPE has a corresponding value hash."
  (member type '(:int2 :int4 :int8 :date :time :timestamp :timestamptz :text :varchar :numeric)))

(defun hash-join-row-hash (row slots types)
  "Hash the indexed components without allocating an intermediate key."
  (let ((hash 0))
    (loop for slot across slots
          for type across types
          for value = (aref row slot)
          do (when (eq value +null+) (return-from hash-join-row-hash nil))
             (setf hash (logand #x3fffffff (logxor (* hash 33) (if (eq type :numeric)
                                                                   (analyze-statistics-value-hash value)
                                                                   (sxhash value))))))
    hash))

(defun hash-join-keys-equal-p (left right left-slots right-slots types)
  "Recheck every component using SQL equality for the admitted type."
  (loop for left-slot across left-slots
        for right-slot across right-slots
        for type across types
        for lhs = (aref left left-slot)
        for rhs = (aref right right-slot)
        always (and (not (eq lhs +null+)) (not (eq rhs +null+))
                    (case type
                      ((:text :varchar) (string= lhs rhs))
                      (:numeric (zerop (sql-numeric-compare lhs rhs)))
                      (otherwise (= lhs rhs))))))

(defstruct hash-join-row
  values origins presence matched-p)

(defconstant +hash-join-batch-count+ 8)

(defconstant +hash-join-max-partition-bits+ 18
  "Bound depth-first repartitioning by the number of selected hash bits.")

(defun hash-join-row-charge (row origins presence)
  (+ +hash-join-entry-overhead-bytes+ (query-spill-record-size row) (* 8 (+ (length origins) (length presence)))))

(defun retain-hash-join-row (row origins presence &optional owned-p)
  (make-hash-join-row :values (if owned-p row (copy-seq row))
                      :origins (if owned-p origins (copy-seq origins))
                      :presence (if owned-p presence (copy-seq presence))))

(defstruct hash-join-partition
  (store (make-query-replay-store nil 0 t))
  (charge 0)
  first-hash
  (varying-bits 0))

(defstruct hash-join-batch
  build probe used-bits)

(defun hash-join-partition-index (hash bits)
  "Pack the selected hash bits, in low-to-high order, into a partition index."
  (let ((index 0))
    (loop for position from 0 while (plusp bits)
          for bit = (logand bits (- bits))
          do (unless (zerop (logand hash bit)) (setf index (logior index (ash 1 position))))
             (setf bits (logxor bits bit)))
    index))

(defun append-hash-join-partition (partitions index hash row origins presence build-p)
  (let ((partition (or (aref partitions index)
                       (setf (aref partitions index) (make-hash-join-partition)))))
    (when build-p
      (incf (hash-join-partition-charge partition) (hash-join-row-charge row origins presence))
      (when hash
        (let ((first-hash (hash-join-partition-first-hash partition)))
          (if first-hash
              (setf (hash-join-partition-varying-bits partition)
                    (logior (hash-join-partition-varying-bits partition) (logxor first-hash hash)))
              (setf (hash-join-partition-first-hash partition) hash)))))
    (append-query-replay-row (hash-join-partition-store partition) row origins presence)))

(defconstant +hash-join-spill-filter-bits+ 8192)

(defconstant +hash-join-spill-filter-bytes+
  (+ (/ +hash-join-spill-filter-bits+ 8) 32)
  "Reserve the bit vector payload and space for its header and alignment.")

(defconstant +hash-join-writer-reserve+ (* +hash-join-batch-count+ (+ +hash-spill-stream-charge+ 96))
  "Include the cached quota writer owned by each partition output.")

(defun hash-join-charge-buffers-p (memory-limit)
  ;; Tiny internal test budgets still exercise the requested row cap.
  (>= memory-limit (* 2 +hash-join-writer-reserve+)))

(defun hash-join-spill-filter-p (memory-limit preserve-probe-p)
  (and (not preserve-probe-p) (>= memory-limit 16384)))

(defun hash-join-initial-build-memory (memory-limit preserve-probe-p)
  "Reserve space for writers that can coexist with the initial table."
  (- memory-limit
     (if (hash-join-spill-filter-p memory-limit preserve-probe-p) +hash-join-spill-filter-bytes+ 0)
     (if (hash-join-charge-buffers-p memory-limit) +hash-join-writer-reserve+ 0)))

(defun hash-join-spill-filter-index (hash)
  (logand (1- +hash-join-spill-filter-bits+) (logxor hash (ash hash -13) (ash hash -26))))

(defun record-hash-join-spill-hash (filter hash)
  (when (and filter hash)
    (setf (sbit filter (hash-join-spill-filter-index hash)) 1))
  hash)

(defun seal-hash-join-partitions (partitions)
  (loop for partition across partitions
        when partition
          do (let ((store (hash-join-partition-store partition)))
               (query-row-spool-seal (query-replay-store-spool store))
               (setf (query-replay-store-eof-p store) t))))

(defun partition-hash-join-input (input partitions slots types route build-p)
  "Consume and seal one side of a partition pass. ROUTE returns NIL to discard a row."
  (loop
    (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
      (declare (ignore toast))
      (unless present (return))
      (let* ((hash (hash-join-row-hash row slots types))
             (index (funcall route hash)))
        (when index
          (append-hash-join-partition partitions index hash row origins presence build-p)))))
  (close-sql-iterator input)
  (seal-hash-join-partitions partitions))

(defun close-hash-join-partition (partition)
  (when partition (close-query-replay-store (hash-join-partition-store partition))))

(defun release-hash-join-partitions (partitions &optional (index 0))
  (when (< index (length partitions))
    (unwind-protect (close-hash-join-partition (aref partitions index))
      (release-hash-join-partitions partitions (1+ index)))))

(defun close-hash-join-batch (batch)
  (unwind-protect (close-hash-join-partition (hash-join-batch-build batch))
    (close-hash-join-partition (hash-join-batch-probe batch))))

(defun close-hash-join-batches (batches)
  (when batches
    (unwind-protect (close-hash-join-batch (first batches))
      (close-hash-join-batches (rest batches)))))

(defun collect-hash-join-batches (used-bits writer adopt)
  "Keep partition stores covered by cleanup until ADOPT takes ownership."
  (let ((build (make-array +hash-join-batch-count+ :initial-element nil))
        (probe (make-array +hash-join-batch-count+ :initial-element nil))
        (completed-p nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (sb-sys:with-local-interrupts (funcall writer build probe))
             (funcall adopt
                      (loop for lhs across build for rhs across probe
                            when (or lhs rhs)
                              collect (make-hash-join-batch :build lhs :probe rhs :used-bits used-bits)))
             (setf completed-p t))
        (unless completed-p
          (unwind-protect (release-hash-join-partitions build)
            (release-hash-join-partitions probe)))))))

(defun hash-join-output-presence (left right cache)
  "Reuse the last immutable source identity vector, including across spill replay."
  (when (and (simple-vector-p left) (simple-vector-p right))
    (let ((previous (car cache))
          (split (length left)))
      (if (and previous
               (= (length previous) (+ split (length right)))
               (loop for i below split always (eql (svref left i) (svref previous i)))
               (loop for i below (length right)
                     always (eql (svref right i) (svref previous (+ split i)))))
          previous
          (setf (car cache) (combine-query-row-presence left right))))))

(defun emit-hash-join-row (left left-origins left-presence entry residual build-left-p presence-cache)
  (let ((right (hash-join-row-values entry))
        (right-origins (hash-join-row-origins entry))
        (right-presence (hash-join-row-presence entry)))
    (when build-left-p
      (rotatef left right)
      (rotatef left-origins right-origins)
      (rotatef left-presence right-presence))
    (let* ((row (concatenate 'simple-vector left right))
           (origins (when *datum-origin-capture*
                      (let ((result (make-array (length row) :initial-element nil)))
                        (when left-origins (replace result left-origins))
                        (when right-origins (replace result right-origins :start1 (length left)))
                        result)))
           (presence (hash-join-output-presence left-presence right-presence presence-cache))
           (*query-row-origins* origins)
           (*query-row-presence* presence))
      (when (or (null residual) (eq t (funcall residual row)))
        (values row t origins nil presence)))))

(defun spill-hash-join-build-table (buckets partitions filter)
  (maphash (lambda (hash entries)
             (record-hash-join-spill-hash filter hash)
             (let ((index (logand (or hash 0) (1- +hash-join-batch-count+))))
               (dolist (entry entries)
                 (check-for-interrupts)
                 (append-hash-join-partition partitions index hash (hash-join-row-values entry)
                                             (hash-join-row-origins entry) (hash-join-row-presence entry) t))))
           buckets))

(defun feed-hash-join-build (input slots types add-row spill-inputs keep-nulls-p)
  "Consume the initial build until exhaustion or until it spills."
  (loop
    (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
      (declare (ignore toast))
      (unless present
        (close-sql-iterator input)
        (return :probe))
      (let ((hash (hash-join-row-hash row slots types)))
        (when (and (or hash keep-nulls-p) (not (funcall add-row hash row origins presence)))
          (funcall spill-inputs hash row origins presence)
          (return :load-batch))))))

(defun load-hash-join-build-block (reader pending slots types add-row full-p)
  "Return the first unconsumed row if it does not fit the current build block."
  (loop
    (multiple-value-bind (row present origins toast presence)
        (if pending
            (values (hash-join-row-values pending) t (hash-join-row-origins pending)
                    nil (hash-join-row-presence pending))
            (sql-iterator-next reader))
      (declare (ignore toast))
      (unless present (return nil))
      (unless (funcall add-row (hash-join-row-hash row slots types) row origins presence)
        (return (or pending (retain-hash-join-row row origins presence t))))
      (setf pending nil)
      (when (funcall full-p) (return nil)))))

(defun release-hash-join-match-map (map)
  (when map
    (close-query-match-map map)
    (when (query-match-map-pathname map)
      (delete-query-temp-file (query-match-map-pathname map)))))

(defun spill-hash-join-inputs (buckets build-batches probe-batches hash row origins presence
                               build build-slots keep-build-p probe probe-slots keep-probe-p types filter-p)
  ;; The negative filter is used only for non-preserved probes.
  (let ((filter (when filter-p
                  (make-array +hash-join-spill-filter-bits+ :element-type 'bit :initial-element 0)))
        (mask (1- +hash-join-batch-count+)))
    (spill-hash-join-build-table buckets build-batches filter)
    (clrhash buckets)
    (record-hash-join-spill-hash filter hash)
    (append-hash-join-partition build-batches (logand (or hash 0) mask) hash row origins presence t)
    (partition-hash-join-input build build-batches build-slots types
                               (lambda (key)
                                 (when (or key keep-build-p)
                                   (record-hash-join-spill-hash filter key)
                                   (logand (or key 0) mask)))
                               t)
    (partition-hash-join-input probe probe-batches probe-slots types
                               (lambda (key)
                                 (let ((index (logand (or key 0) mask)))
                                   (when (or keep-probe-p
                                             (and key (aref build-batches index)
                                                  (or (null filter)
                                                      (= 1 (sbit filter (hash-join-spill-filter-index key))))))
                                     index)))
                               nil)))

(defun probe-hash-join-entry (row origins presence entry probe-slots build-slots types residual
                              build-left-p null-probe stats presence-cache)
  (when stats (incf (hash-join-stats-candidates stats)))
  (when (hash-join-keys-equal-p row (hash-join-row-values entry) probe-slots build-slots types)
    (multiple-value-bind (joined present joined-origins toast joined-presence)
        (emit-hash-join-row row origins presence entry residual build-left-p presence-cache)
      (when present
        (when null-probe (setf (hash-join-row-matched-p entry) t))
        (values joined t joined-origins toast joined-presence)))))

(defun record-hash-join-spill-batches (stats batches)
  (when stats (incf (hash-join-stats-spill-batches stats) (length batches))))

(defun hash-join-repartition-bits (batch budget)
  "Choose up to three unused varying bits before opening the batch for replay."
  (let* ((build (hash-join-batch-build batch))
         (used (hash-join-batch-used-bits batch))
         (remaining (- +hash-join-max-partition-bits+ (logcount used))))
    (when (and build (hash-join-batch-probe batch)
               (> (hash-join-partition-charge build) budget)
               (plusp remaining)
               ;; With a hard quota, bounded replay avoids a second live copy.
               (not (and *query-temp-quota* (query-temp-quota-limit *query-temp-quota*))))
      (let ((available (logandc2 (hash-join-partition-varying-bits build) used))
            (needed (integer-length (1- (ceiling (hash-join-partition-charge build) (max 1 budget)))))
            (selected 0))
        (dotimes (i (min 3 remaining needed))
          (let ((lowest (logand available (- available))))
            (setf selected (logior selected lowest) available (logxor available lowest))))
        (unless (zerop selected) selected)))))

(defun repartition-hash-join-side (parent partitions slots types route build-p)
  (let ((reader (make-query-replay-reader (hash-join-partition-store parent))))
    (unwind-protect (partition-hash-join-input reader partitions slots types route build-p)
      (close-sql-iterator reader)))
  (close-hash-join-partition parent))

(defun repartition-hash-join-batch (batch bits build-slots probe-slots types preserve-probe-p adopt)
  (collect-hash-join-batches
   (logior bits (hash-join-batch-used-bits batch))
   (lambda (build probe)
     (repartition-hash-join-side
      (hash-join-batch-build batch) build build-slots types
      (lambda (hash) (hash-join-partition-index (or hash 0) bits)) t)
     (repartition-hash-join-side
      (hash-join-batch-probe batch) probe probe-slots types
      (lambda (hash)
        (let ((index (hash-join-partition-index (or hash 0) bits)))
          (when (or preserve-probe-p (and hash (aref build index))) index)))
      nil))
   adopt))

(defstruct hash-join-replay
  batches build-reader pending match-map)

(defun advance-hash-join-replay (replay)
  (close-hash-join-batch (first (hash-join-replay-batches replay)))
  (pop (hash-join-replay-batches replay)))

(defun prepare-hash-join-replay (replay memory-limit build-slots probe-slots types preserve-probe-p stats)
  (loop for batches = (hash-join-replay-batches replay)
        while batches
        for bits = (hash-join-repartition-bits (first batches) memory-limit)
        while bits
        do (repartition-hash-join-batch
            (first batches) bits build-slots probe-slots types preserve-probe-p
            (lambda (children)
              (record-hash-join-spill-batches stats children)
              (setf (hash-join-replay-batches replay) (nconc children (rest batches)))))))

(defun load-hash-join-replay (replay build-limit match-map-limit build-slots probe-slots types preserve-build-p
                              stats add-row full-p nonempty-p)
  "Load the next replay block, or open the final unmatched pass for its probe side."
  (loop
    (unless (hash-join-replay-build-reader replay)
      (prepare-hash-join-replay replay build-limit build-slots probe-slots types match-map-limit stats))
    (let ((batch (first (hash-join-replay-batches replay))))
      (unless batch (return (values :done nil)))
      (let* ((build (hash-join-batch-build batch))
             (probe (hash-join-batch-probe batch))
             (probe-store (and probe (hash-join-partition-store probe))))
        (when (and build (or probe-store preserve-build-p))
          (unless (hash-join-replay-build-reader replay)
            (setf (hash-join-replay-build-reader replay)
                  (make-query-replay-reader (hash-join-partition-store build))))
          (setf (hash-join-replay-pending replay)
                (load-hash-join-build-block (hash-join-replay-build-reader replay)
                                            (hash-join-replay-pending replay) build-slots types add-row full-p))
          (when (funcall nonempty-p)
            (when stats (incf (hash-join-stats-build-blocks stats)))
            (when (and match-map-limit probe-store (null (hash-join-replay-match-map replay)))
              (setf (hash-join-replay-match-map replay) (make-query-match-map match-map-limit t
                                                                              (query-row-spool-count (query-replay-store-spool probe-store)))))
            (return (if probe-store
                        (values :probe (make-query-replay-reader probe-store))
                        (values :unmatched-build nil)))))
        (close-sql-iterator (hash-join-replay-build-reader replay))
        (setf (hash-join-replay-build-reader replay) nil)
        (when (and match-map-limit probe-store)
          (let ((map (hash-join-replay-match-map replay)))
            (when map (query-match-map-finish map)))
          (return (values :unmatched (make-query-replay-reader probe-store))))
        (advance-hash-join-replay replay)))))

(defun close-hash-join-resources (probe build replay)
  (unwind-protect (close-sql-iterator probe)
    (unwind-protect (close-sql-iterator build)
      (when replay
        (unwind-protect (release-hash-join-match-map (hash-join-replay-match-map replay))
          (close-hash-join-batches (hash-join-replay-batches replay)))))))

(defun make-hash-join-iterator (probe build probe-slots build-slots types
                                &key residual preserve-probe-width preserve-build-width build-left-p stats (memory-limit (effective-query-work-memory-bytes)))
  "Join equal resolved key types. The iterator owns both children and all spill files."
  (unless (and (= (length probe-slots) (length build-slots) (length types))
               (plusp (length types)) (every #'hash-join-key-type-p types))
    (error "invalid hash join key definition"))
  (let* ((filter-p (hash-join-spill-filter-p memory-limit preserve-probe-width))
         (charge-buffers-p (hash-join-charge-buffers-p memory-limit))
         (build-limit (hash-join-initial-build-memory memory-limit preserve-probe-width))
         (match-map-limit (when preserve-probe-width (max 1 (floor memory-limit 8))))
         (buckets nil) (phase :build) (charged 0) (replay nil)
         (active-probe probe) (unmatched-build nil) (presence-cache (list nil))
         (null-build (when preserve-probe-width
                       (make-hash-join-row :values (make-array preserve-probe-width :initial-element +null+)
                                           :presence #())))
         (null-probe (when preserve-build-width (make-array preserve-build-width :initial-element +null+)))
         (probe-row nil) (probe-origins nil) (probe-presence nil) (chain nil) (probe-index -1) (matched-p nil))
    (labels ((reset-table ()
               (unless buckets (setf buckets (make-hash-table :test #'eql)))
               (clrhash buckets)
               (setf charged 0 unmatched-build nil))
             (add-row (hash row origins presence)
               (let ((bytes (hash-join-row-charge row origins presence)))
                 ;; Replay can process a single oversized tuple as its own block.
                 (when (or (<= (+ charged bytes) build-limit)
                           (and (eq phase :load-batch) (zerop charged)))
                   (incf charged bytes)
                   (when stats (setf (hash-join-stats-peak-build-bytes stats)
                                     (max charged (hash-join-stats-peak-build-bytes stats))))
                   (let ((entry (retain-hash-join-row row origins presence (eq phase :load-batch))))
                     (push entry (gethash hash buckets))
                     (when null-probe (push entry unmatched-build)))
                   t)))
             (spill-inputs (hash row origins presence)
               (setf unmatched-build nil)
               (collect-hash-join-batches
                (1- +hash-join-batch-count+)
                (lambda (build-batches probe-batches)
                  (spill-hash-join-inputs buckets build-batches probe-batches hash row origins presence
                                          build build-slots preserve-build-width probe probe-slots
                                          preserve-probe-width types filter-p))
                (lambda (batches)
                  (setf replay (make-hash-join-replay :batches batches))
                  (record-hash-join-spill-batches stats batches)))
               ;; CLRHASH drops the entries but retains the initial table's capacity.
               ;; Size the replay table from its own build blocks.
               (setf buckets nil active-probe nil phase :load-batch
                     build-limit (- memory-limit (or match-map-limit 0)
                                    (if charge-buffers-p
                                        (* (if match-map-limit 3 2) +hash-spill-stream-charge+)
                                        0))))
             (build-table ()
               (reset-table)
               (setf phase (feed-hash-join-build build build-slots types #'add-row #'spill-inputs preserve-build-width))
               (when (and stats (eq phase :probe) (plusp charged)) (incf (hash-join-stats-build-blocks stats))))
             (load-batch ()
               (reset-table)
               (multiple-value-setq (phase active-probe)
                 (load-hash-join-replay replay build-limit match-map-limit build-slots probe-slots types
                                        preserve-build-width stats #'add-row
                                        (lambda () (>= charged build-limit)) (lambda () (plusp charged))))
               (setf probe-index -1))
             (read-probe ()
               (multiple-value-bind (row present origins toast presence) (sql-iterator-next active-probe)
                 (declare (ignore toast))
                 (setf probe-row row probe-origins origins probe-presence presence matched-p nil)
                 (if present
                     (progn (incf probe-index) t)
                     (progn (close-sql-iterator active-probe) (setf active-probe nil)))))
             (next-row ()
               (loop
                 (check-for-interrupts)
                 (let ((match-map (and replay (hash-join-replay-match-map replay))))
                   (ecase phase
                     (:build (build-table))
                     (:load-batch (load-batch))
                     (:done (return))
                     (:unmatched-build
                      (let ((entry (pop unmatched-build)))
                        (cond ((null entry) (setf phase (if replay :load-batch :done)))
                              ((not (hash-join-row-matched-p entry))
                               (return (emit-hash-join-row null-probe nil #() entry nil build-left-p presence-cache))))))
                     (:unmatched
                      (if (read-probe)
                          (unless (and match-map (query-match-map-matched-p match-map probe-index))
                            (return (emit-hash-join-row probe-row probe-origins probe-presence null-build nil build-left-p presence-cache)))
                          (progn
                            (release-hash-join-match-map match-map)
                            (setf (hash-join-replay-match-map replay) nil phase :load-batch)
                            (advance-hash-join-replay replay))))
                     (:probe
                      (cond (chain
                             (multiple-value-bind (row present origins toast presence)
                                 (probe-hash-join-entry probe-row probe-origins probe-presence (pop chain)
                                                        probe-slots build-slots types residual build-left-p
                                                        null-probe stats presence-cache)
                               (when present
                                 (when (and match-map (not matched-p)) (query-match-map-mark match-map probe-index))
                                 (setf matched-p t)
                                 (return (values row t origins toast presence)))))
                            ((and null-build (not replay) probe-row (not matched-p))
                             (setf matched-p t)
                             (return (emit-hash-join-row probe-row probe-origins probe-presence null-build nil build-left-p presence-cache)))
                            ((read-probe)
                             (when (and match-map (= probe-index (query-match-map-count match-map)))
                               (query-match-map-append match-map))
                             (let ((hash (hash-join-row-hash probe-row probe-slots types)))
                               (setf chain (when hash (gethash hash buckets)))))
                            (t (setf phase (cond (null-probe :unmatched-build)
                                                 (replay :load-batch)
                                                 (t :done))))))))))
             (close-join ()
               (setf phase :done buckets nil chain nil unmatched-build nil
                     probe-row nil probe-origins nil probe-presence nil)
               (unwind-protect (close-hash-join-resources probe build replay)
                 (setf replay nil active-probe nil))))
      (make-sql-iterator #'next-row #'close-join))))
