;;;; catalog-tests.lisp -- catalog and index ordering tests.

(in-package #:pg)

(defun test-catalog-mutation-rollback-macro ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend :database "postgres"))
         (database (make-database :name "postgres" :oid 7))
         (table
           (make-pg-class
            :oid 701 :name "rollback_rows" :relfilenode 1701
            :columns
            (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                    :attnum 1 :notnull t))))
         (original-next-oid (catalog-next-oid *catalog*)))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash "rollback_rows" (database-tables database)) table)
    (let ((failed nil))
      (handler-case
          (with-write-catalog-locks
            (with-catalog-mutation-rollback (database)
              (setf (pg-column-notnull (aref (pg-class-columns table) 0)) nil
                    (gethash "transient" (database-tables database))
                    (make-pg-class :oid 702 :name "transient" :columns #()))
              (incf (catalog-next-oid *catalog*))
              (error "injected pre-durable catalog failure")))
        (error () (setf failed t)))
      (check (and failed
                  (eq table (gethash "rollback_rows" (database-tables database)))
                  (not (gethash "transient" (database-tables database)))
                  (pg-column-notnull (aref (pg-class-columns table) 0))
                  (= (catalog-next-oid *catalog*) original-next-oid))
             "catalog mutation macro restores object state, catalog maps, and OID allocation"))
    (let ((failed nil))
      (handler-case
          (with-write-catalog-locks
            (with-catalog-mutation-rollback (database)
              (setf (pg-column-notnull (aref (pg-class-columns table) 0)) nil
                    *catalog-save-error-after-durable-p* t)
              (error "injected post-durable catalog failure")))
        (error () (setf failed t)))
      (check (and failed
                  (not (pg-column-notnull (aref (pg-class-columns table) 0))))
             "catalog mutation macro keeps state after the durable WAL boundary"))
    ;; Exercise the generation path through save-catalog itself, after its
    ;; catalog terminal has crossed the WAL boundary like RecordTransactionCommit()
    ;; in xact.c. This guards against a rollback after a durable catalog image.
    (logical-test-with-wal-storage
     (lambda ()
       (let* ((*catalog* (make-catalog))
              (*backend* (make-backend :database "postgres"))
              (*catalog-durable-generation* 0)
              (*wal-catalog-flush-generation* 0)
              (database (make-database :name "postgres" :oid 7))
              (table
                (make-pg-class
                 :oid 701 :name "rollback_rows" :relfilenode 1701
                 :columns
                 (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                         :attnum 1 :notnull t))))
              (event (logical-encode-catalog-event
                      7 '(:type :create-table :name "rollback_event")))
              (generation *catalog-durable-generation*)
              (caught nil))
         (setf (gethash "postgres" (catalog-databases *catalog*)) database
               (gethash "rollback_rows" (database-tables database)) table
               (gethash :catalog-rollback-test
                        *wal-logical-catalog-event-interests*) 7)
         (let ((*logical-test-failpoint-hook*
                 (lambda (point &rest details)
                   (declare (ignore details))
                   (when (eq point :after-catalog-event-wal-terminal)
                     (error "injected failure after durable catalog terminal")))))
           (handler-case
               (with-write-catalog-locks
                 (with-catalog-mutation-rollback (database)
                   ;; Rollback would restore NOT NULL, so a false result below
                   ;; proves the post-terminal mutation stayed published.
                   (setf (pg-column-notnull
                          (aref (pg-class-columns table) 0)) nil)
                   (save-catalog :logical-event event)))
             (error () (setf caught t))))
         (check (and caught
                     (> *catalog-durable-generation* generation)
                     (plusp *wal-durable-lsn*)
                     (not (pg-column-notnull (aref (pg-class-columns table) 0))))
                "catalog mutation macro keeps state after save-catalog advances the durable generation"))))))

(defun test-with-table-ddl-evaluates-lock-inputs-once ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend :database "postgres"))
         (*relation-builders* (make-hash-table :test #'eql))
         (*relation-reader-blockers* (make-hash-table :test #'eql))
         (*relation-build-groups* (make-hash-table :test #'eq))
         (database (make-database :name "postgres" :oid 7))
         (table (make-pg-class :oid 810 :name "macro_rows"
                               :relfilenode 810 :columns #()))
         (index (make-pg-index :oid 820 :name "macro_rows_idx"
                               :relfilenode 820 :table-oid 810))
         (table-evaluations 0)
         (index-evaluations 0)
         (wait-evaluations 0)
         (locked-relations nil)
         (reader-blocked-p nil)
         (body-ran nil))
    (setf (gethash "postgres" (catalog-databases *catalog*)) database
          (gethash "macro_rows" (database-tables database)) table)
    (let ((result
            (with-table-ddl
                ((progn (incf table-evaluations) table)
                 :indexes (progn (incf index-evaluations) (list index))
                 :wait-readers (progn (incf wait-evaluations) t))
              (maphash
               (lambda (token entry)
                 (declare (ignore token))
                 (setf locked-relations (first entry)
                       reader-blocked-p (second entry)))
               *relation-build-groups*)
              (setf body-ran t)
              table)))
      (check (eq result table)
             "table DDL macro returns its body result")
      (check (and (= table-evaluations 1)
                  (= index-evaluations 1)
                  (= wait-evaluations 1))
             "table DDL macro evaluates table, index, and wait forms once")
      (check (and (equal locked-relations '(810 820))
                  reader-blocked-p
                  body-ran)
             "table DDL macro locks its table and indexes before the body"))
    (let ((rejected nil)
          (stale-body-ran nil))
      (handler-case
          (with-table-ddl
              ((progn (remhash "macro_rows" (database-tables database)) table))
            (setf stale-body-ran t))
        (error () (setf rejected t)))
      (check (and rejected (not stale-body-ran))
             "table DDL macro revalidates a stale table before catalog mutation"))))

(defun test-analyze-column-statistics-and-costing ()
  (let* ((sample (vector #(1) #(1) (vector +null+) #(2) #(3) #(3)))
         (column (analyze-column-statistics sample 0 6))
         (table (make-pg-class
                 :oid 57 :name "analyzed" :relfilenode 5701 :columns #()
                 :statistics
                 (make-pg-table-statistics
                  :row-count 100000 :sample-count 6 :columns (vector column))))
         (seq (make-select-scan-path :kind :seq-scan))
         (index (make-pg-index :oid 58 :name "analyzed_idx"
                               :relfilenode 5702 :table-oid 57))
         (index-path (make-select-scan-path
                      :kind :index-scan :index index :selectivity 0.001d0))
         (original-buf-nblocks (symbol-function 'buf-nblocks)))
    (check (= (pg-column-statistics-distinct-count column) 3)
           "ANALYZE estimates distinct non-NULL sample values")
    (check (< (abs (- (pg-column-statistics-null-fraction column)
                      (/ 1.0d0 6)))
              1.0d-9)
           "ANALYZE records sampled NULL fraction")
    (check (= (cdr (assoc 1 (pg-column-statistics-most-common-values column)))
              (/ 2.0d0 6))
           "ANALYZE stores MCV frequencies over the full sample")
    (check (= (planner-equality-selectivity table 1 1) (/ 2.0d0 6))
           "planner uses an MCV frequency for equality selectivity")
    (unwind-protect
         (progn
           (setf (symbol-function 'buf-nblocks)
                 (lambda (relfilenode)
                   (case relfilenode (5701 1000) (5702 20) (t 1))))
           (check (eq (choose-cheapest-select-path table (list seq index-path))
                      index-path)
                  "cost model chooses a selective index scan over SeqScan"))
      (setf (symbol-function 'buf-nblocks) original-buf-nblocks))))

(defun call-with-test-analyze-pages (table rows function)
  "Run FUNCTION with an isolated snapshot and heap pages containing ROWS.
Install buffer reads for these physical tuples for the duration of the call."
  (let ((pages (make-array 0 :adjustable t :fill-pointer 0))
        (columns (pg-class-columns table))
        (relfilenode (pg-class-relfilenode table))
        (original-nblocks (symbol-function 'buf-nblocks))
        (original-read (symbol-function 'buf-read))
        (*backend* nil)
        (*next-xid* 3))
    (dolist (row rows)
      (let ((bytes (form-heap-tuple row columns 1 0 0 0 1)))
        (unless (and (plusp (length pages))
                     (page-add-item (aref pages (1- (length pages))) bytes))
          (let ((page (page-init (make-page))))
            (assert (page-add-item page bytes))
            (vector-push-extend page pages)))))
    (unwind-protect
         (progn
           (setf (symbol-function 'buf-nblocks)
                 (lambda (rel)
                   (assert (= rel relfilenode))
                   (length pages))
                 (symbol-function 'buf-read)
                 (lambda (rel block)
                   (assert (= rel relfilenode))
                   (aref pages block)))
           (funcall function (make-snapshot-struct :xmin 2 :xmax 3 :curcid 1) pages))
      (setf (symbol-function 'buf-nblocks) original-nblocks
            (symbol-function 'buf-read) original-read))))

(defun test-analyze-byte-vector-sample-retention ()
  (let* ((columns (vector (make-pg-column :name "id" :type :int4)
                          (make-pg-column :name "payload" :type :bytea)))
         (table (make-pg-class :oid 5902 :name "byte_sample"
                               :relfilenode 5902 :columns columns))
         (pointer (make-heap-external-varlena :raw-size (+ 4 (* 128 1024))
                                              :external-info (* 128 1024)
                                              :value-id 1 :relation-id 1 :id-width 4)))
    ;; The external value is wide logically, but its heap tuple holds a pointer.
    (call-with-test-analyze-pages
     table (loop for i below 1000 collect (vector i pointer))
     (lambda (snapshot pages)
       (declare (ignore pages))
       (multiple-value-bind (row-count sample)
           (analyze-sample-visible-rows table 5902 snapshot)
         (check (= row-count 1000) "ANALYZE counts rows with large external byte vectors")
         (check (= (length sample) +analyze-sample-size+)
                "external logical width does not reduce the physical tuple reservoir")
         (check (<= (loop for bytes across sample sum (length bytes))
                    +analyze-sample-memory-bytes+)
                "physical tuple copies stay within the sample memory budget")
         (check (every (lambda (bytes)
                         (eq (analyze-physical-column-value table bytes 1 snapshot)
                             +analyze-wide-value+))
                       sample)
                "wide external byte vectors are recognized without fetching TOAST")
         (check (every (lambda (bytes)
                         (integerp (analyze-physical-column-value table bytes 0 snapshot)))
                       sample)
                "narrow columns remain available beside wide external values"))))))

(defun test-analyze-byte-vector-reservoir-width-and-oversize ()
  (let* ((columns (vector (make-pg-column :name "v" :type :bytea)))
         (table (make-pg-class :oid 5903 :name "byte_sample_edges"
                               :relfilenode 5903 :columns columns))
         (*analyze-random-function* (constantly 0)))
    (call-with-test-analyze-pages
     table (loop for index below 600
                 collect (vector (make-array (if (< index 300) 16 4096)
                                             :element-type '(unsigned-byte 8)
                                             :initial-element (mod index 256))))
     (lambda (snapshot pages)
       (declare (ignore pages))
       (multiple-value-bind (row-count sample)
           (analyze-sample-visible-rows table 5903 snapshot)
         (check (= row-count 600) "ANALYZE counts rows after tuple width grows")
         (check (= (length sample) +analyze-sample-size+)
                "larger physical tuples retain the configured reservoir count")
         (check (eq (analyze-physical-column-value table (aref sample 0) 0 snapshot)
                    +analyze-wide-value+)
                "later wide byte values can replace earlier narrow values")
         (check (<= (loop for bytes across sample sum (length bytes))
                    +analyze-sample-memory-bytes+)
                "mixed-width physical samples stay within the memory budget"))))
    ;; An oversized logical value still fits in the physical sample as a pointer.
    (let* ((pointer (make-heap-external-varlena
                     :raw-size (+ 5 +analyze-sample-memory-bytes+)
                     :external-info (1+ +analyze-sample-memory-bytes+)
                     :value-id 1 :relation-id 1 :id-width 4))
           (small (make-array 3 :element-type '(unsigned-byte 8)
                                :initial-contents '(4 5 6))))
      (call-with-test-analyze-pages
       table (list (vector pointer) (vector small))
       (lambda (snapshot pages)
         (declare (ignore pages))
         (multiple-value-bind (row-count sample)
             (analyze-sample-visible-rows table 5903 snapshot)
           (check (and (= row-count 2) (= (length sample) 2))
                  "ANALYZE retains physical tuples even when logical values exceed its budget")
           (check (eq (analyze-physical-column-value table (aref sample 0) 0 snapshot)
                      +analyze-wide-value+)
                  "oversized external values use the wide-value marker")
           (check (equalp (analyze-physical-column-value table (aref sample 1) 0 snapshot)
                          small)
                  "a narrow byte value after an oversized value is still decoded"))))
      (call-with-test-analyze-pages
       table (list (vector pointer) (vector pointer))
       (lambda (snapshot pages)
         (declare (ignore pages))
         (let ((stats (analyze-table-statistics table 5903 snapshot)))
           (check (and (= (pg-table-statistics-row-count stats) 2)
                       (= (pg-table-statistics-sample-count stats) 2))
                  "all-wide rows still contribute to ANALYZE row and sample counts")
           (check (null (pg-column-statistics-most-common-values
                         (aref (pg-table-statistics-columns stats) 0)))
                  "wide values are excluded from the decoded MCV list")))))
    (call-with-test-analyze-pages
     table nil
     (lambda (snapshot pages)
       (check (zerop (length pages)) "the empty fixture has no heap blocks")
       (multiple-value-bind (row-count sample)
           (analyze-sample-visible-rows table 5903 snapshot)
         (check (and (zerop row-count) (zerop (length sample)))
                "ANALYZE handles a relation with no visible rows"))))))

(defun test-analyze-reservoir-sample-is-bounded ()
  (let* ((columns (vector (make-pg-column :name "id" :type :int4 :attnum 1)))
         (table (make-pg-class :oid 59 :name "sampled" :relfilenode 5901 :columns columns))
         (*analyze-random-function* (constantly 0)))
    (call-with-test-analyze-pages
     table (loop for value below 1000 collect (vector value))
     (lambda (snapshot pages)
       (check (> (length pages) 1) "the reservoir fixture spans multiple heap pages")
       (multiple-value-bind (row-count sample)
           (analyze-sample-visible-rows table 5901 snapshot)
         (check (= row-count 1000) "ANALYZE counts visible rows while sampling")
         (check (= (length sample) +analyze-sample-size+)
                "ANALYZE reservoir remains at its configured sample bound")
         (check (every (lambda (bytes)
                         (let ((value (analyze-physical-column-value table bytes 0 snapshot)))
                           (and (integerp value) (<= 0 value 999))))
                       sample)
                "ANALYZE sample contains valid physical tuple copies")
         (check (= (analyze-physical-column-value table (aref sample 0) 0 snapshot) 999)
                "the full reservoir still considers later rows")
         ;; Samples must own their tuple bytes after the source pages change.
         (loop for page across pages do (fill page 0))
         (check (= (analyze-physical-column-value table (aref sample 0) 0 snapshot) 999)
                "sampled tuples remain owned after source pages change"))))
    (call-with-test-analyze-pages
     table (loop for value below 10 collect (vector value))
     (lambda (snapshot pages)
       (let ((page (aref pages 0)))
         (multiple-value-bind (offset flags length) (page-get-lp page 1)
           (declare (ignore flags length))
           (page-set-u32 page (+ offset +heap-tuple-xmax-off+) 1))
         (page-mark-dead page 2)
         (page-mark-unused page 3))
       (multiple-value-bind (row-count sample)
           (analyze-sample-visible-rows table 5901 snapshot)
         (check (and (= row-count 7) (= (length sample) 7))
                "ANALYZE excludes deleted tuples and dead or unused line pointers")
         (check (equal (loop for bytes across sample
                             collect (analyze-physical-column-value table bytes 0 snapshot))
                       '(3 4 5 6 7 8 9))
                "physical sampling applies snapshot visibility before admission"))))))

(defun test-analyze-publish-restores-on-save-failure ()
  (let* ((table (make-pg-class :oid 60 :name "stats_failure"
                               :relfilenode 6001 :columns #()))
         (previous (make-pg-table-statistics :row-count 1))
         (candidate (make-pg-table-statistics :row-count 2))
         (original-save (symbol-function 'save-catalog))
         (original-ensure (symbol-function 'ensure-current-table))
         (caught nil))
    (setf (pg-class-statistics table) previous)
    (unwind-protect
         (progn
           (setf (symbol-function 'save-catalog)
                 (lambda () (error "injected catalog save failure"))
                 (symbol-function 'ensure-current-table)
                 (lambda (current)
                   (declare (ignore current))
                   t))
           (handler-case (publish-analyze-statistics table candidate)
             (error (condition) (setf caught condition)))
           (check (typep caught 'error)
                  "ANALYZE reports a catalog save failure")
           (check (eq (pg-class-statistics table) previous)
                  "ANALYZE restores in-memory statistics when persistence fails"))
      (setf (symbol-function 'save-catalog) original-save
            (symbol-function 'ensure-current-table) original-ensure))))

(defun test-analyze-publish-keeps-durable-stats-on-late-failure ()
  (let* ((table (make-pg-class :oid 62 :name "stats_durable_failure"
                               :relfilenode 6201 :columns #()))
         (previous (make-pg-table-statistics :row-count 1))
         (candidate (make-pg-table-statistics :row-count 2))
         (original-save (symbol-function 'save-catalog))
         (original-ensure (symbol-function 'ensure-current-table))
         (*wal-catalog-flush-generation* (wal-catalog-flush-generation))
         (caught nil))
    (setf (pg-class-statistics table) previous)
    (unwind-protect
         (progn
           (setf (symbol-function 'save-catalog)
                 (lambda ()
                   (incf *wal-catalog-flush-generation*)
                   (error "injected post-WAL catalog failure"))
                 (symbol-function 'ensure-current-table)
                 (lambda (current)
                   (declare (ignore current))
                   t))
           (handler-case (publish-analyze-statistics table candidate)
             (error (condition) (setf caught condition)))
           (check (typep caught 'error)
                  "ANALYZE reports a catalog failure after durable WAL")
           (check (eq (pg-class-statistics table) candidate)
                  "ANALYZE keeps statistics that are already present in durable WAL"))
      (setf (symbol-function 'save-catalog) original-save
            (symbol-function 'ensure-current-table) original-ensure))))

(defun test-nested-loop-seq-cost-models-replayable-inner ()
  (let* ((table (make-pg-class
                 :oid 61 :name "spooled_inner" :relfilenode 6101 :columns #()
                 :statistics (make-pg-table-statistics :row-count 100)))
         (path (make-select-scan-path :kind :seq-scan))
         (base-cost (select-scan-path-cost table path))
         (repeated-cost (nested-loop-inner-path-cost table path 5))
         (index (make-pg-index :oid 6102 :relfilenode 6102
                               :table-oid 61 :columns '(1)))
         (cached-index-path
           (make-select-scan-path :kind :index-scan :index index
                                  :selectivity 0.1d0))
         (parameterized-index-path
           (make-select-scan-path :kind :index-scan :index index
                                  :parameter-expr '(:col 1) :selectivity 0.1d0)))
    (check (= repeated-cost (+ base-cost 4.0d0))
           "SeqScan inner costing scans once and charges replay per later outer row")
    (check (< repeated-cost (* 5 base-cost))
           "replayable SeqScan costing is below a full rescan per outer row")
    (check (< (nested-loop-inner-path-cost table cached-index-path 5)
              (nested-loop-inner-path-cost table parameterized-index-path 5))
           "unparameterized IndexScan is spooled while parameterized scans rescan")
    (let ((*query-work-memory-bytes* 1))
      (check (= (select-path-spool-spill-page-cost table path) 1.0d0)
             "nested-loop costing estimates the disk pages for a spill")
      (check (= (- (nested-loop-inner-path-cost table path 5) base-cost) 9.0d0)
             "nested-loop costing charges the initial spill write and later rereads"))))
