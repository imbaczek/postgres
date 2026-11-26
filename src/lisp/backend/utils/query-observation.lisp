;;;; query-observation.lisp -- bounded cumulative statement measurements.

(in-package #:pg)

(defstruct statement-phase-statistics
  (count 0 :type integer)
  (total 0.0d0 :type double-float)
  (minimum 0.0d0 :type double-float)
  (maximum 0.0d0 :type double-float)
  (mean 0.0d0 :type double-float)
  (m2 0.0d0 :type double-float)
  (extrema-empty-p t))

(defun record-statement-phase-time (statistics milliseconds)
  "Accumulate a successful phase using the Welford recurrence used by pg_stat_statements."
  (declare (type double-float milliseconds))
  (let* ((count (incf (statement-phase-statistics-count statistics)))
         (previous-mean (statement-phase-statistics-mean statistics))
         (delta (- milliseconds previous-mean))
         (mean (+ previous-mean (/ delta count)))
         (empty-p (statement-phase-statistics-extrema-empty-p statistics)))
    (incf (statement-phase-statistics-total statistics) milliseconds)
    (incf (statement-phase-statistics-m2 statistics) (* delta (- milliseconds mean)))
    (setf (statement-phase-statistics-mean statistics) mean
          (statement-phase-statistics-minimum statistics)
          (if empty-p milliseconds
              (min milliseconds (statement-phase-statistics-minimum statistics)))
          (statement-phase-statistics-maximum statistics)
          (if empty-p milliseconds
              (max milliseconds (statement-phase-statistics-maximum statistics)))
          (statement-phase-statistics-extrema-empty-p statistics) nil)))

(defstruct statement-resource-usage
  (relation-reads 0 :type integer)
  (clean-cache-hits 0 :type integer)
  (relation-writes 0 :type integer)
  (spill-read-bytes 0 :type integer)
  (spill-written-bytes 0 :type integer)
  (wal-records 0 :type integer)
  (wal-bytes 0 :type integer))

(defun add-statement-resource-usage (total usage)
  (incf (statement-resource-usage-relation-reads total)
        (statement-resource-usage-relation-reads usage))
  (incf (statement-resource-usage-clean-cache-hits total)
        (statement-resource-usage-clean-cache-hits usage))
  (incf (statement-resource-usage-relation-writes total)
        (statement-resource-usage-relation-writes usage))
  (incf (statement-resource-usage-spill-read-bytes total)
        (statement-resource-usage-spill-read-bytes usage))
  (incf (statement-resource-usage-spill-written-bytes total)
        (statement-resource-usage-spill-written-bytes usage))
  (incf (statement-resource-usage-wal-records total) (statement-resource-usage-wal-records usage))
  (incf (statement-resource-usage-wal-bytes total) (statement-resource-usage-wal-bytes usage))
  (values))

(defstruct statement-statistics-entry
  key text stats-since minmax-stats-since
  (text-bytes 0 :type integer)
  (planning (make-statement-phase-statistics))
  (execution (make-statement-phase-statistics))
  (rows 0 :type integer)
  resources
  (execution-errors 0 :type integer)
  (execution-cancellations 0 :type integer)
  next)

(defstruct statement-statistics-shard
  (mutex (sb-thread:make-mutex :name "statement statistics"))
  (entries (make-hash-table :test #'equal))
  (capacity 0 :type integer)
  (text-budget 0 :type integer)
  (text-bytes 0 :type integer)
  (evictions 0 :type integer)
  oldest newest
  ;; Only shard zero publishes the reset time for the store.
  reset-time)

(defun make-statement-statistics-store (capacity text-budget &key (shards 16))
  "Divide the entry cap and text budget among independent FIFO shards."
  (check-type capacity (integer 1))
  (check-type text-budget (integer 0))
  (check-type shards (integer 1))
  (let* ((count (min capacity shards))
         (store (make-array count)))
    (multiple-value-bind (entries extra-entries) (floor capacity count)
      (multiple-value-bind (bytes extra-bytes) (floor text-budget count)
        (dotimes (index count store)
          (setf (aref store index)
                (make-statement-statistics-shard
                 :capacity (+ entries (if (< index extra-entries) 1 0))
                 :text-budget (+ bytes (if (< index extra-bytes) 1 0))
                 :reset-time (when (zerop index)
                               (sql-current-local-microseconds nil nil)))))))))

(defun unlink-statement-statistics-entry (shard entry previous)
  "Remove ENTRY from the hash and FIFO while the caller owns the masked shard lock."
  (let ((next (statement-statistics-entry-next entry)))
    (remhash (statement-statistics-entry-key entry) (statement-statistics-shard-entries shard))
    (decf (statement-statistics-shard-text-bytes shard) (statement-statistics-entry-text-bytes entry))
    (if previous
        (setf (statement-statistics-entry-next previous) next)
        (setf (statement-statistics-shard-oldest shard) next))
    (unless next
      (setf (statement-statistics-shard-newest shard) previous))))

(defun evict-statement-statistics-entry (shard)
  (unlink-statement-statistics-entry shard (statement-statistics-shard-oldest shard) nil)
  (incf (statement-statistics-shard-evictions shard)))

(defun insert-statement-statistics-entry (shard key text text-bytes)
  (let ((entries (statement-statistics-shard-entries shard)))
    (loop while (or (>= (hash-table-count entries) (statement-statistics-shard-capacity shard))
                    (> (+ (statement-statistics-shard-text-bytes shard) text-bytes)
                       (statement-statistics-shard-text-budget shard)))
          do (evict-statement-statistics-entry shard))
    (let* ((now (sql-current-local-microseconds nil nil))
           (entry (make-statement-statistics-entry :key (copy-list key)
                                                   :text text :text-bytes text-bytes
                                                   :stats-since now :minmax-stats-since now))
           (tail (statement-statistics-shard-newest shard)))
      (if tail
          (setf (statement-statistics-entry-next tail) entry)
          (setf (statement-statistics-shard-oldest shard) entry))
      (setf (statement-statistics-shard-newest shard) entry
            (gethash (statement-statistics-entry-key entry) entries) entry)
      (incf (statement-statistics-shard-text-bytes shard) text-bytes)
      entry)))

(defun store-statement-statistics-phase (store key phase milliseconds rows text &optional resources)
  "Store a phase measurement or execution failure under immutable scalar key components.
TEXT is bounded representative text or a producer scoped to this statement.
The producer runs outside the shard lock. Only its rendered string can enter the store."
  (check-type phase (member :planning :execution :error :canceled))
  (check-type milliseconds double-float)
  (check-type rows (integer 0))
  (let ((shard (aref store (mod (sxhash key) (length store)))))
    (loop
      (block update
        (sb-thread:with-mutex ((statement-statistics-shard-mutex shard))
          (sb-sys:without-interrupts
            (let ((entry (gethash key (statement-statistics-shard-entries shard))))
              (unless entry
                (let ((budget (statement-statistics-shard-text-budget shard)))
                  (when (and (plusp budget) (functionp text))
                    (return-from update))
                  (let ((text-bytes (if (stringp text) (* 4 (length text)) 0)))
                    (setf entry
                          (if (and (stringp text) (<= text-bytes budget))
                              (insert-statement-statistics-entry shard key (copy-seq text) text-bytes)
                              (insert-statement-statistics-entry shard key nil 0))))))
              (ecase phase
                (:planning
                 (record-statement-phase-time (statement-statistics-entry-planning entry) milliseconds))
                (:error (incf (statement-statistics-entry-execution-errors entry)))
                (:canceled (incf (statement-statistics-entry-execution-cancellations entry)))
                (:execution
                 (record-statement-phase-time (statement-statistics-entry-execution entry) milliseconds)
                 (incf (statement-statistics-entry-rows entry) rows)
                 (when resources
                   (if (statement-statistics-entry-resources entry)
                       (add-statement-resource-usage (statement-statistics-entry-resources entry) resources)
                       (setf (statement-statistics-entry-resources entry)
                             (copy-statement-resource-usage resources))))))
              (return-from store-statement-statistics-phase (values))))))
      (setf text (funcall text))
      (check-type text (or null string)))))

(defun snapshot-statement-statistics-entry (entry showtext)
  (let ((snapshot (copy-statement-statistics-entry entry)))
    (setf (statement-statistics-entry-key snapshot) (copy-list (statement-statistics-entry-key entry))
          (statement-statistics-entry-text snapshot)
          (when showtext (copy-seq (statement-statistics-entry-text entry)))
          (statement-statistics-entry-planning snapshot)
          (copy-statement-phase-statistics (statement-statistics-entry-planning entry))
          (statement-statistics-entry-execution snapshot)
          (copy-statement-phase-statistics (statement-statistics-entry-execution entry))
          (statement-statistics-entry-resources snapshot)
          (when (statement-statistics-entry-resources entry)
            (copy-statement-resource-usage (statement-statistics-entry-resources entry)))
          (statement-statistics-entry-next snapshot) nil)
    snapshot))

(defun snapshot-statement-statistics (store &optional (showtext t))
  "Copy snapshots under individual shard locks for formatting outside the locks."
  (loop for shard across store
        nconc (sb-thread:with-mutex ((statement-statistics-shard-mutex shard))
                (loop for entry being the hash-values of (statement-statistics-shard-entries shard)
                      collect (snapshot-statement-statistics-entry entry showtext)))))

(defun call-with-statement-statistics-locks (store function)
  "Acquire every shard in one order for store-wide metadata operations."
  (declare (dynamic-extent function))
  (labels ((lock-shards (index)
             (if (= index (length store))
                 (funcall function)
                 (sb-thread:with-mutex ((statement-statistics-shard-mutex (aref store index)))
                   (lock-shards (1+ index))))))
    (lock-shards 0)))

(defun reset-statement-phase-extrema (phase)
  (setf (statement-phase-statistics-minimum phase) 0.0d0
        (statement-phase-statistics-maximum phase) 0.0d0
        (statement-phase-statistics-extrema-empty-p phase) t))

(defun reset-statement-statistics-shard (shard userid dbid queryid extrema-only-p reset-time)
  "Clear matching entries while preserving the FIFO order of all survivors."
  (let ((previous nil))
    (loop for entry = (statement-statistics-shard-oldest shard) then next
          while entry
          for next = (statement-statistics-entry-next entry)
          for (owner database nil id) = (statement-statistics-entry-key entry)
          do (cond
               ((and (or (zerop userid) (= userid owner))
                     (or (zerop dbid) (= dbid database))
                     (or (zerop queryid) (= queryid id)))
                (if extrema-only-p
                    (progn
                      (reset-statement-phase-extrema (statement-statistics-entry-planning entry))
                      (reset-statement-phase-extrema (statement-statistics-entry-execution entry))
                      (setf (statement-statistics-entry-minmax-stats-since entry) reset-time))
                    (unlink-statement-statistics-entry shard entry previous)))
               (t (setf previous entry))))))

(defun reset-statement-statistics (store &optional clock (userid 0) (dbid 0) (queryid 0) extrema-only-p)
  "Reset matching groups at one point. Zero is a wildcard for an identity field."
  (call-with-statement-statistics-locks
   store
   (lambda ()
     (sb-sys:without-interrupts
       (let ((reset-time (if clock (funcall clock) (sql-current-local-microseconds nil nil))))
         (loop for shard across store
               do (reset-statement-statistics-shard shard userid dbid queryid extrema-only-p reset-time))
         (when (and (not extrema-only-p)
                    (every (lambda (shard) (null (statement-statistics-shard-oldest shard))) store))
           (loop for shard across store do (setf (statement-statistics-shard-evictions shard) 0))
           (setf (statement-statistics-shard-reset-time (aref store 0)) reset-time))
         reset-time)))))

(defun statement-statistics-info (store)
  "Return eviction count and reset time from the same store generation."
  (call-with-statement-statistics-locks
   store
   (lambda ()
     (values (loop for shard across store sum (statement-statistics-shard-evictions shard))
             (statement-statistics-shard-reset-time (aref store 0))))))

(defvar *statement-statistics-store* nil
  "The server's cumulative statement store, or NIL when collection is disabled.")

(defvar *active-statement-observation* nil
  "The observation whose execution phase is currently running on this backend.")

(defvar *statement-nested-p* nil
  "True while an untracked utility runs nested SQL statements.")

(defvar *statement-observation-clock* #'get-internal-real-time
  "Monotonic tick source for statement phase timing.")

(defvar *statement-resource-usage* nil
  "Resource accumulator for the active execution, bound per backend thread.")

(defstruct statement-observation
  (store nil :read-only t)
  (key nil :read-only t)
  (text nil :read-only t)
  (execution-ticks 0 :type integer)
  (executed-p nil)
  (rows 0 :type integer)
  (resources (make-statement-resource-usage) :read-only t)
  terminal-status)

(defun statement-observation-milliseconds (ticks)
  (* ticks (/ 1000.0d0 internal-time-units-per-second)))

(defmacro with-statement-planning (((analyzed analysis) (query-id identity-form)
                                    (results planning) ticks track-planning-p)
                                   &body body)
  "Bind planning results and elapsed ticks, excluding work to fingerprint the analysis."
  (let ((start (gensym "ANALYSIS-START"))
        (end (gensym "ANALYSIS-END"))
        (planning-start (gensym "PLANNING-START")))
    `(let* ((,start (when ,track-planning-p (funcall *statement-observation-clock*)))
            (,analyzed ,analysis)
            (,end (when ,start (funcall *statement-observation-clock*)))
            (,query-id ,identity-form)
            (,planning-start (when ,start (funcall *statement-observation-clock*))))
       (multiple-value-bind ,results ,planning
         (let ((,ticks (when ,start
                         (+ (- ,end ,start)
                            (- (funcall *statement-observation-clock*) ,planning-start)))))
           ,@body)))))

(defun call-with-statement-execution-slice (observation function)
  "Run one active slice of a single backend-owned observation.
The caller records rows and owns terminal completion. An unwind marks the
execution failed, so later retirement cannot publish a successful execution."
  (declare (dynamic-extent function))
  (if (null observation)
      (funcall function)
      (let ((*active-statement-observation* observation)
            (*statement-resource-usage* (statement-observation-resources observation))
            (started (funcall *statement-observation-clock*))
            (status :failed))
        (setf (statement-observation-executed-p observation) t)
        (unwind-protect
             (handler-bind ((error (lambda (condition)
                                     (setf status
                                           (if (and (typep condition 'pg-error)
                                                    (string= (pg-error-sqlstate condition) "57014"))
                                               :canceled :error)))))
               (multiple-value-prog1 (funcall function)
                 (setf status :success)))
          (incf (statement-observation-execution-ticks observation)
                (- (funcall *statement-observation-clock*) started))
          (unless (eq status :success)
            (finish-statement-observation observation status))))))

(defun finish-statement-observation (observation status)
  "Publish the executed observation's terminal outcome once."
  (sb-sys:without-interrupts
    (when (and observation (null (statement-observation-terminal-status observation)))
      (setf (statement-observation-terminal-status observation) status)
      (when (statement-observation-executed-p observation)
        (let ((store (statement-observation-store observation))
              (key (statement-observation-key observation))
              (text (statement-observation-text observation)))
          (case status
            (:success
             (store-statement-statistics-phase
              store key :execution
              (statement-observation-milliseconds (statement-observation-execution-ticks observation))
              (statement-observation-rows observation) text
              (statement-observation-resources observation)))
            ((:error :canceled)
             (store-statement-statistics-phase store key status 0.0d0 0 text)))))))
  (values))
