;;;; Extended query protocol state and bounded frame parsing.

(in-package #:pg)

(defstruct extended-statement ast types activity-text (result-signature nil :read-only t))

(defstruct extended-portal statement values (executed-p nil)
           (returns-rows-p nil :read-only t)
           result-columns result-formats)

(defstruct extended-select-execution owner plan targets iterator observation queryid (at-end-p nil) (failed-p nil))

(defstruct extended-dml-execution owner prepared targets spool store iterator observation queryid
           (ready-p nil) (at-end-p nil) (failed-p nil))

(defvar *extended-dml-executions* (make-hash-table :test #'eq :weakness :key))

(defvar *extended-select-executions* (make-hash-table :test #'eq :weakness :key))

(defvar *extended-execute-suspended-p* nil)

(defvar *extended-portal-executions-lock* (sb-thread:make-mutex :name "extended portal executions"))

(defun extended-portal-select-p (portal)
  (eq (getf (extended-statement-ast (extended-portal-statement portal)) :type) :select))

(defun call-with-extended-bind-spool (input size function)
  "Read SIZE bytes into a checksummed anonymous spool, scoped to FUNCTION."
  (unless (<= 0 size (- +max-query-message-size+ 4))
    (malformed-protocol-message "invalid Bind message length"))
  (let ((owner (make-logical-wire-field-owner)))
    (unwind-protect
         (let ((output (logical-wire-field-open owner))
               (buffer (make-array 65536 :element-type '(unsigned-byte 8)))
               (remaining size))
           (multiple-value-bind (sink metrics) (logical-encoding-sink size output)
             (loop while (plusp remaining) do
               (let* ((count (min remaining (length buffer)))
                      (end (with-backend-client-read-wait
                             (read-sequence buffer input :end count))))
                 (when (zerop end)
                   (error "connection ended during Bind body"))
                 (funcall sink (if (= end (length buffer)) buffer (subseq buffer 0 end)))
                 (decf remaining end)))
             (finish-output output)
             (unless (file-position output 0)
               (error "cannot seek Bind spool"))
             (multiple-value-bind (written crc) (funcall metrics)
               (unless (= written size) (error "invalid Bind spool size"))
               (funcall function output crc))))
      (close-logical-wire-field-owner owner))))

(defstruct extended-bind-spool stream size crc)

(defun extended-verify-bind-spool (spool)
  "Verify the complete raw Bind before using its metadata or values."
  (let ((stream (extended-bind-spool-stream spool))
        (remaining (extended-bind-spool-size spool))
        (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
    (unless (file-position stream 0) (error "cannot seek Bind spool"))
    (multiple-value-bind (sink metrics) (logical-encoding-sink remaining)
      (loop while (plusp remaining) do
        (let* ((count (min remaining (length buffer)))
               (end (read-sequence buffer stream :end count)))
          (when (zerop end) (error "truncated Bind spool"))
          (funcall sink (if (= end (length buffer)) buffer (subseq buffer 0 end)))
          (decf remaining end)))
      (multiple-value-bind (size crc) (funcall metrics)
        (unless (and (= size (extended-bind-spool-size spool))
                     (= crc (extended-bind-spool-crc spool)))
          (error "Bind spool checksum mismatch")))))
  (values))

(defun extended-parse-bind-spool (spool)
  "Parse a complete spooled Bind without materializing its parameter fields."
  (let ((stream (extended-bind-spool-stream spool))
        (remaining (extended-bind-spool-size spool)))
    (unless (file-position stream 0) (error "cannot seek Bind spool"))
    (labels ((next-octet ()
               (when (zerop remaining) (malformed-protocol-message "truncated extended query message"))
               (let ((value (read-byte stream nil nil)))
                 (unless value (error "truncated Bind spool"))
                 (decf remaining)
                 value))
             (uint (count)
               (loop repeat count for value = (next-octet)
                     for result = value then (logior (ash result 8) value)
                     finally (return result)))
             (cstring ()
               (let ((bytes (make-array 32 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)))
                 (loop for value = (next-octet) until (zerop value) do (vector-push-extend value bytes))
                 (bytes->string bytes)))
             (field ()
               (let ((size (uint 4)))
                 (if (= size #xffffffff) nil
                     (progn
                       (unless (<= size remaining) (malformed-protocol-message "truncated extended query message"))
                       (let ((offset (file-position stream)))
                         (unless (file-position stream (+ offset size)) (error "cannot seek Bind spool"))
                         (decf remaining size)
                         (cons offset size)))))))
      (let* ((portal (cstring)) (statement (cstring))
                                (formats (loop repeat (uint 2) collect (uint 2)))
                                (values (loop repeat (uint 2) collect (field)))
                                (results (loop repeat (uint 2) collect (uint 2))))
        (unless (zerop remaining) (malformed-protocol-message "extended query message has trailing bytes"))
        (list :bind portal statement formats values results)))))

(defun extended-bind-spool-field (spool range)
  "Materialize only the requested parameter from SPOOL."
  (let* ((stream (extended-bind-spool-stream spool))
         (size (cdr range))
         (bytes (make-array size :element-type '(unsigned-byte 8))))
    (unless (file-position stream (car range)) (error "cannot seek Bind spool field"))
    (loop for position = 0 then next
          while (< position size)
          for next = (read-sequence bytes stream :start position)
          do (when (= next position) (error "truncated Bind spool field")))
    bytes))

(defstruct extended-query-state
  (statements (make-hash-table :test #'equal))
  (portals (make-hash-table :test #'equal))
  (ignore-until-sync nil)
  (in-cycle-p nil)
  (statement-deadline nil)
  (executed-in-cycle-p nil)
  (implicit-p nil))

(defvar *extended-query-state* nil)

(defvar *extended-query-state-registry* (make-hash-table :test #'eq :weakness :key))

(defvar *extended-query-state-registry-lock* (sb-thread:make-mutex :name "extended query state"))

(defun extended-query-state-for-backend (backend)
  (unless backend (error "extended query requires a backend"))
  (sb-thread:with-mutex (*extended-query-state-registry-lock*)
    (or (gethash backend *extended-query-state-registry*)
        (setf (gethash backend *extended-query-state-registry*) (make-extended-query-state)))))

(defun extended-backend-holds-snapshot-p (backend)
  "Tell wraparound whether BACKEND must run to retire its portal pins."
  (some (lambda (owner)
          (let ((registration (query-execution-owner-snapshot-registration owner)))
            (and (eq backend (query-execution-owner-backend owner))
                 (or (snapshot-registration-active-p registration)
                     (snapshot-registration-releasing-p registration)))))
        (query-execution-owner-reload-state)))

(defun extended-select-execution-for (portal)
  (sb-thread:with-mutex (*extended-portal-executions-lock*)
    (gethash portal *extended-select-executions*)))

(defun extended-dml-execution-for (portal)
  (sb-thread:with-mutex (*extended-portal-executions-lock*)
    (gethash portal *extended-dml-executions*)))

(defun invalidate-extended-dml-execution (execution)
  (setf (extended-dml-execution-failed-p execution) t
        (extended-dml-execution-observation execution) nil
        (extended-dml-execution-prepared execution) nil
        (extended-dml-execution-targets execution) nil
        (extended-dml-execution-iterator execution) nil
        (extended-dml-execution-store execution) nil
        (extended-dml-execution-spool execution) nil)
  (values))

(defun extended-close-portal (portal &optional failed-p discard-observation-p)
  ;; Failed cleanup keeps its owner reachable for retry. A failed transaction
  ;; may retain portal identity after executable state has been closed.
  (let ((execution (extended-select-execution-for portal)))
    (when execution
      (when failed-p (setf (extended-select-execution-failed-p execution) t))
      (multiple-value-bind (closed failures)
          (close-query-execution-owner (extended-select-execution-owner execution))
        (unless closed (return-from extended-close-portal (values nil failures))))
      (finish-statement-observation
       (extended-select-execution-observation execution)
       (if (or discard-observation-p (extended-select-execution-failed-p execution))
           :failed :success))
      (setf (extended-select-execution-observation execution) nil
            (extended-select-execution-plan execution) nil
            (extended-select-execution-targets execution) nil
            (extended-select-execution-iterator execution) nil)
      (unless failed-p
        (sb-thread:with-mutex (*extended-portal-executions-lock*)
          (remhash portal *extended-select-executions*)))))
  (let ((execution (extended-dml-execution-for portal)))
    (when execution
      (when failed-p (setf (extended-dml-execution-failed-p execution) t))
      (multiple-value-bind (closed failures)
          (close-query-execution-owner (extended-dml-execution-owner execution))
        (unless closed (return-from extended-close-portal (values nil failures))))
      (invalidate-extended-dml-execution execution)
      (unless failed-p
        (sb-thread:with-mutex (*extended-portal-executions-lock*)
          (remhash portal *extended-dml-executions*)))))
  t)

(defun extended-drop-portal (state name)
  (let ((portal (gethash name (extended-query-state-portals state))))
    (when portal
      (remhash name (extended-query-state-portals state))
      (unless (extended-close-portal portal)
        (ereport "58030" "portal cleanup remains incomplete"))))
  (values))

(defun capture-savepoint-portals ()
  (when *extended-query-state*
    (loop for portal being the hash-values of (extended-query-state-portals *extended-query-state*)
          collect portal)))

(defun restore-savepoint-portals (checkpoint)
  (when *extended-query-state*
    (let* ((portals (extended-query-state-portals *extended-query-state*))
           (discard
             (loop for name being the hash-keys of portals using (hash-value portal)
                   unless (member portal checkpoint :test #'eq) collect name)))
      (dolist (name discard)
        (let ((portal (gethash name portals)))
          (unless (extended-close-portal portal nil t)
            (ereport "58030" "portal cleanup remains incomplete"))
          (remhash name portals))))))

(defun make-extended-portal-execution-owner (portal &key suppress-datum-origins)
  "Build the bindings retained by the portal's resumable executor."
  (make-query-execution-owner
   :bindings
   (list* (cons '*query-parameter-types* (extended-statement-types (extended-portal-statement portal)))
          (cons '*query-parameter-values* (extended-portal-values portal))
          (cons '*select-result-formats* (extended-portal-result-formats portal))
          (cons '*select-expected-result-columns* (extended-portal-result-columns portal))
          (cons '*select-suppress-description* t)
          (when suppress-datum-origins (list (cons '*datum-origin-capture* nil))))))

(defmacro with-extended-portal-initialization ((portal execution registry) &body body)
  "Register the execution before acquisition so cleanup can retire a partial Bind."
  (let ((actual-portal (gensym "PORTAL-"))
        (complete (gensym "COMPLETE-")))
    `(let ((,actual-portal ,portal)
           (,complete nil))
       (unwind-protect
            (progn
              (sb-thread:with-mutex (*extended-portal-executions-lock*)
                (setf (gethash ,actual-portal ,registry) ,execution))
              (multiple-value-prog1 (progn ,@body)
                (setf ,complete t)))
         (unless ,complete (extended-close-portal ,actual-portal))))))

(defun prepare-observed-select-result (ast execution)
  "Fingerprint the analyzed query before planning can rewrite it."
  (multiple-value-bind (store toplevel) (statement-tracking-store)
    (unless (and *backend* *statement-statistics-store*)
      (return-from prepare-observed-select-result (prepare-select-result ast)))
    (with-statement-planning ((prepared (prepare-select ast))
                              (query-id (statement-select-fingerprint prepared))
                              ((plan targets columns) (prepare-select-result ast prepared))
                              ticks (and store (statement-track-planning-p)))
      (setf (extended-select-execution-queryid execution) query-id
            (extended-select-execution-observation execution)
            (begin-planned-statement-observation
             store toplevel query-id ticks (getf ast :normalized-statement-text)))
      (values plan targets columns))))

(defun extended-initialize-select-portal (portal &key snapshot result-format-codes)
  "Prepare a SELECT portal and return its result columns.
The owner retains the plan, relations and snapshot until the portal closes.
SNAPSHOT is the active simple-query snapshot. Otherwise start the snapshot
for an extended-protocol portal and validate its result against Parse."
  (let* ((statement (extended-portal-statement portal))
         (owner (make-extended-portal-execution-owner portal :suppress-datum-origins t))
         (execution (make-extended-select-execution :owner owner)))
    (with-extended-portal-initialization (portal execution *extended-select-executions*)
      (if snapshot
          (query-execution-owner-pin-snapshot owner snapshot)
          (recovery-check-statement (extended-statement-ast statement)))
      (call-with-query-execution-owner
       owner
       (lambda ()
         (multiple-value-bind (plan targets columns)
             (prepare-observed-select-result (extended-statement-ast statement) execution)
           (setf (extended-select-execution-plan execution) plan
                 (extended-select-execution-targets execution) targets)
           (call-with-admitted-select-plan
            plan (lambda (relations)
                   (setf (query-execution-owner-binding owner '*subquery-registered-relations*)
                         relations)
                   (unless snapshot
                     ;; Wait for recipe-changing writers before taking the RC snapshot.
                     (query-execution-owner-acquire-snapshot
                      owner (lambda (registration) (statement-start registration nil)))
                     (extended-check-result-signature statement columns)
                     (let ((formats (extended-select-result-formats result-format-codes columns)))
                       (setf (extended-portal-result-columns portal) columns
                             (extended-portal-result-formats portal) formats
                             (query-execution-owner-binding owner '*select-result-formats*) formats)))
                   (retain-query-plan-executor-tables plan relations)))
           columns))))))

(defun extended-send-portal-rows (stream iterator targets limit &key values-only-p batch-p)
  ;; Observe EOF only through a pull allowed by LIMIT, even for a sealed store.
  (let ((count 0)
        (frame-buffer nil))
    (loop while (or (<= limit 0) (< count limit))
          do (multiple-value-bind (row present) (sql-iterator-next iterator)
               (unless present
                 (when (and batch-p (> count 1))
                   (let ((complete nil))
                     (sb-sys:without-interrupts
                       (unwind-protect
                            (sb-sys:with-local-interrupts
                              (with-backend-client-write-wait (stream) (force-output stream))
                              (setf complete t))
                         (unless complete (ignore-errors (close stream :abort t)))))))
                 (return-from extended-send-portal-rows (values count t)))
               (when (and batch-p (= count 1))
                 (setf frame-buffer (make-array +select-data-row-buffer-size+
                                                :element-type '(unsigned-byte 8))))
               (let ((*select-toast-demand* nil)
                     (*select-toast-slice-plan* nil)
                     (*toast-slice-current* nil))
                 (send-select-row stream (if values-only-p (cons row row) row) targets
                                  (or (not batch-p) (zerop count)) frame-buffer))
               (incf count)))
    (values count nil)))

(defun run-select-portal-slice (stream execution limit)
  "Send SELECT rows and return the count and whether the portal reached the end.
A nonpositive LIMIT requests all remaining rows. Keep the iterator in the
portal owner so a bounded Execute can resume the same plan and snapshot.
Detach active index scans before a suspended execution releases its leases.
On completion or error, close the iterators while those leases are still held."
  (let ((count 0))
    (unless execution (ereport "55000" "portal has no initialized SELECT execution"))
    (when (extended-select-execution-failed-p execution)
      (ereport "55000" "portal cannot be run"))
    (unless (extended-select-execution-at-end-p execution)
      (call-with-query-execution-owner
       (extended-select-execution-owner execution)
       (lambda ()
         (let ((plan (extended-select-execution-plan execution))
               (relations *subquery-registered-relations*)
               (targets (extended-select-execution-targets execution))
               (iterator (extended-select-execution-iterator execution)))
           (call-with-select-index-lease-domain
            (query-plan-index-relfilenodes plan relations)
            (lambda ()
              (let ((batch-p (and (<= limit 0) (null iterator)))
                    (*iterator-index-scan-domain-p* t)
                    (suspend-p nil))
                (unwind-protect
                     (progn
                       (unless iterator
                         (setf iterator (make-resumable-query-plan-iterator plan relations :projected-only-p t)
                               (extended-select-execution-iterator execution) iterator))
                       (multiple-value-bind (delivered at-end)
                           (extended-send-portal-rows stream iterator targets limit :batch-p batch-p)
                         (setf count delivered
                               (extended-select-execution-at-end-p execution) at-end)
                         (unless at-end
                           (detach-select-index-scans)
                           (setf suspend-p t))))
                  (unless suspend-p
                    (sb-sys:without-interrupts
                      (unwind-protect
                           (sb-sys:with-local-interrupts
                             (unwind-protect (close-sql-iterator iterator)
                               ;; A shared CTE producer can outlive its last result reader.
                               (dolist (open (loop for open being the hash-keys of *sql-open-iterators*
                                                   collect open))
                                 (close-sql-iterator open))))
                        ;; Never try to spool a cursor after its group has unwound.
                        ;; The iterator and temp owners still own any unfinished cleanup.
                        (setf *select-index-scans*
                              (delete-if #'select-index-scan-borrowed-lease-p *select-index-scans*))))
                    (setf (extended-select-execution-iterator execution) nil))))))))))
    (values count (extended-select-execution-at-end-p execution))))

(defun run-select-portal (stream portal limit)
  "Account only the active slice, leaving a suspended observation with its portal."
  (let* ((execution (extended-select-execution-for portal))
         (observation (and execution
                           (not (extended-select-execution-at-end-p execution))
                           (extended-select-execution-observation execution))))
    (if (null observation)
        (run-select-portal-slice stream execution limit)
        (multiple-value-bind (count at-end)
            (call-with-statement-execution-slice
             observation (lambda () (run-select-portal-slice stream execution limit)))
          (incf (statement-observation-rows observation) count)
          (values count at-end)))))

(defun execute-select-portal (stream portal limit)
  (multiple-value-bind (count at-end) (run-select-portal stream portal limit)
    (if at-end
        (progn
          (let ((execution (extended-select-execution-for portal)))
            (when execution
              (finish-statement-observation
               (extended-select-execution-observation execution) :success)))
          (send-command-complete stream (format nil "SELECT ~d" count))
          t)
        (progn
          (setf *extended-execute-suspended-p* t)
          (write-message stream #\s #())
          nil))))

(defun exec-select (stream ast)
  "Run a simple-query SELECT through a transient portal.
Send its row description before execution and close it before reporting completion."
  (let ((portal (make-extended-portal :statement (make-extended-statement :ast ast)))
        (count nil)
        (original-fatal nil))
    (unwind-protect
         (handler-case
             (progn
               (send-row-description stream
                                     (extended-initialize-select-portal portal :snapshot *current-snapshot*))
               (setf count (run-select-portal stream portal 0)))
           (pg-error (condition)
             (when (fatal-error-p condition) (setf original-fatal condition))
             (error condition)))
      (handler-case
          (unless (or (extended-close-portal portal) (null count))
            (ereport "58030" "portal cleanup remains incomplete"))
        (error (condition)
          (error (or original-fatal condition)))))
    (send-command-complete stream (format nil "SELECT ~d" count))))

(defun extended-initialize-dml-portal (portal)
  (let* ((statement (extended-portal-statement portal))
         (owner (make-extended-portal-execution-owner portal))
         (execution (make-extended-dml-execution :owner owner)))
    (with-extended-portal-initialization (portal execution *extended-dml-executions*)
      (call-with-query-execution-owner
       owner
       (lambda ()
         (unless (recovery-in-progress-p)
           (prepare-xid-wraparound-for-statement *backend*))
         ;; Disallow isolation changes after Bind has admitted the query.
         (setf (backend-first-snapshot-set *backend*) t)
         (multiple-value-bind (store toplevel) (statement-tracking-store)
           (multiple-value-bind (prepared observation queryid)
               (prepare-observed-dml (extended-statement-ast statement) store toplevel)
             (setf (extended-dml-execution-prepared execution) prepared
                   (extended-dml-execution-targets execution) (getf prepared :returning)
                   (extended-dml-execution-observation execution) observation
                   (extended-dml-execution-queryid execution) queryid)
             (register-dml-subquery-relations prepared (get-current-reader-xid)))))))))

(defun extended-materialize-dml-portal (stream portal execution)
  ;; Run mutations once and retain a sealed result for later pulls. The receiver
  ;; pins the statement snapshot while its original registration is still live;
  ;; spool adoption and publication wait until statement execution returns.
  (let ((owner (extended-dml-execution-owner execution))
        (resource nil)
        (closer #'close-query-row-spool)
        (received nil)
        (targets nil)
        (complete nil)
        (original-fatal nil))
    (unwind-protect
         (handler-case
             (progn
               (call-with-query-execution-owner
                owner
                (lambda ()
                  ;; The closer owns a mutable resource cell before any acquisition.
                  ;; Terminal invalidation may discard every runtime slot independently.
                  (query-execution-owner-add-cleanup
                   owner (lambda () (when resource (funcall closer resource))))
                  (sb-sys:without-interrupts
                    (setf resource (make-query-row-spool)
                          (extended-dml-execution-spool execution) resource
                          *dml-returning-spool* resource))
                  (let* ((statement (extended-portal-statement portal))
                         (*statement-command-start-advance-p* nil)
                         (*dml-bound-preparation*
                           (cons (extended-statement-ast statement)
                                 (extended-dml-execution-prepared execution)))
                         (*dml-returning-receiver*
                           (lambda (spool result-targets result-tag)
                             (declare (ignore result-tag))
                             (when (or received (not (eq spool resource)))
                               (error "unexpected deferred RETURNING receiver"))
                             ;; Copy/register while the active statement token still pins
                             ;; this snapshot. Publication waits for the outer unwind.
                             (query-execution-owner-pin-snapshot owner *current-snapshot*)
                             (setf received t targets result-targets))))
                    (execute-statement stream (extended-statement-ast statement)))
                  (unless received (error "RETURNING execution did not produce a sealed spool"))
                  (sb-sys:without-interrupts
                    (let ((store (adopt-sealed-query-row-spool resource)))
                      (setf resource store
                            closer #'close-query-replay-store
                            (extended-dml-execution-store execution) store
                            (extended-dml-execution-spool execution) nil
                            *dml-returning-spool* nil)
                      (setf (extended-dml-execution-iterator execution)
                            (make-query-replay-reader store)
                            (extended-dml-execution-targets execution) targets
                            (extended-dml-execution-ready-p execution) t)))))
               (setf complete t))
           (pg-error (condition)
             (when (fatal-error-p condition) (setf original-fatal condition))
             (error condition)))
      (sb-sys:without-interrupts
        (unless complete
          ;; Publish failure before another interrupt can preempt cleanup.
          ;; If cleanup also signals FATAL, preserve an original FATAL.
          (setf (extended-dml-execution-failed-p execution) t)
          (flet ((report-cleanup (condition)
                   (handler-case
                       (format *error-output* "~&incomplete RETURNING cleanup: ~a~%" condition)
                     (error (failure)
                       (when (fatal-error-p failure) (error (or original-fatal failure))))
                     (sb-ext:timeout () nil))))
            (handler-case
                (multiple-value-bind (closed failures)
                    (sb-sys:with-local-interrupts (extended-close-portal portal t))
                  (unless closed (report-cleanup failures)))
              (error (condition)
                (if (fatal-error-p condition)
                    (error (or original-fatal condition))
                    (report-cleanup condition)))
              (sb-ext:timeout (condition) (report-cleanup condition))))))))
  (values))

(defun call-with-extended-dml-observation (execution function)
  "Measure one mutation run, retaining only its query identity afterward."
  (declare (dynamic-extent function))
  (let ((*dml-statement-observation* (extended-dml-execution-observation execution)))
    (multiple-value-prog1 (call-with-statement-execution-slice *dml-statement-observation* function)
      (finish-statement-observation *dml-statement-observation* :success)
      (setf (extended-dml-execution-observation execution) nil))))

(defun extended-execute-dml-portal (stream portal limit)
  ;; The first Execute materializes the statement; subsequent calls only read
  ;; its store. As with SELECT, an exact row limit suspends without an EOF probe.
  (let ((execution (extended-dml-execution-for portal))
        (count 0))
    (unless execution (ereport "55000" "portal has no initialized RETURNING execution"))
    (when (extended-dml-execution-failed-p execution)
      (ereport "55000" "portal cannot be run"))
    (unless (extended-dml-execution-ready-p execution)
      (call-with-extended-dml-observation
       execution (lambda () (extended-materialize-dml-portal stream portal execution))))
    (unless (extended-dml-execution-at-end-p execution)
      (call-with-query-execution-owner
       (extended-dml-execution-owner execution)
       (lambda ()
         (multiple-value-bind (delivered at-end)
             (extended-send-portal-rows
              stream (extended-dml-execution-iterator execution)
              (extended-dml-execution-targets execution) limit :values-only-p t)
           (setf count delivered
                 (extended-dml-execution-at-end-p execution) at-end)))))
    (if (extended-dml-execution-at-end-p execution)
        (progn
          (send-command-complete
           stream
           (case (getf (extended-statement-ast (extended-portal-statement portal)) :type)
             (:insert (format nil "INSERT 0 ~d" count))
             (:update (format nil "UPDATE ~d" count))
             (:delete (format nil "DELETE ~d" count))))
          t)
        (progn
          (setf *extended-execute-suspended-p* t)
          (write-message stream #\s #())
          nil))))

(defun extended-clear-portals-for-backend (backend &key discard-observations-p)
  (when backend
    (let* ((state
             (sb-thread:with-mutex (*extended-query-state-registry-lock*)
               (or (gethash backend *extended-query-state-registry*)
                   (and (eq backend *backend*) *extended-query-state*))))
           (portals (when state
                      (loop for portal being the hash-values of
                                                             (extended-query-state-portals state) collect portal)))
           (fatal nil))
      (when state
        (unless *preserve-failed-extended-portals*
          (clrhash (extended-query-state-portals state)))
        (setf (extended-query-state-executed-in-cycle-p state) nil))
      (dolist (portal portals)
        (handler-case (extended-close-portal portal *preserve-failed-extended-portals* discard-observations-p)
          (pg-error (condition)
            (when (and (fatal-error-p condition) (null fatal)) (setf fatal condition)))))
      (let ((pending
              (handler-case (close-query-execution-owners-for-backend backend)
                (pg-error (condition)
                  (when (and (fatal-error-p condition) (null fatal)) (setf fatal condition))
                  (remove-if-not
                   (lambda (owner) (eq backend (query-execution-owner-backend owner)))
                   (query-execution-owner-reload-state))))))
        (when fatal (error fatal))
        pending))))

(defun forget-extended-query-state (backend)
  (when backend
    (clear-backend-command-id-used backend)
    (let ((state
            (sb-thread:with-mutex (*extended-query-state-registry-lock*)
              (gethash backend *extended-query-state-registry*))))
      ;; No frontend execution can resume after this terminal boundary.
      (when state
        (loop for portal being the hash-values of (extended-query-state-portals state)
              for execution = (extended-select-execution-for portal)
              when execution
                do (setf (extended-select-execution-failed-p execution) t
                         (extended-select-execution-plan execution) nil
                         (extended-select-execution-targets execution) nil
                         (extended-select-execution-iterator execution) nil))
        (loop for portal being the hash-values of (extended-query-state-portals state)
              for execution = (extended-dml-execution-for portal)
              when execution do (invalidate-extended-dml-execution execution))
        (clrhash (extended-query-state-portals state)))
      (retire-query-execution-owners-for-backend backend)
      (sb-thread:with-mutex (*extended-query-state-registry-lock*)
        (remhash backend *extended-query-state-registry*))))
  (values))

(defun extended-read-cstring (payload position)
  (multiple-value-bind (value end) (pg-client-cstring payload position)
    (values value end)))

(defun extended-type-from-oid (oid)
  (or (sql-type-from-oid oid)
      (ereport "0A000" "unsupported parameter type OID ~a" oid)))

(defun extended-parse-frame (type payload)
  "Parse the complete frame before changing prepared or portal state."
  (let ((position 0) (length (length payload)))
    (labels ((need (n) (unless (<= (+ position n) length) (malformed-protocol-message "truncated extended query message")))
             (u16 () (need 2) (prog1 (bytes-to-u16 payload position) (incf position 2)))
             (u32 () (need 4) (prog1 (bytes-to-u32 payload position) (incf position 4)))
             (cstring () (multiple-value-bind (value next) (extended-read-cstring payload position)
                           (setf position next) value))
             (done () (unless (= position length) (malformed-protocol-message "extended query message has trailing bytes"))))
      (case type
        (#\P (let* ((name (cstring)) (query (cstring)) (count (u16))
                                     (oids (loop repeat count collect (u32))))
               (done) (list :parse name query oids)))
        (#\B (let* ((portal (cstring)) (statement (cstring))
                                       (formats (loop repeat (u16) collect (u16)))
                                       (values (loop repeat (u16)
                                                     collect (let ((size (u32)))
                                                               (if (= size #xffffffff) nil
                                                                   (progn (need size)
                                                                          (prog1 (cons position size) (incf position size)))))))
                                       (results (loop repeat (u16) collect (u16))))
               (done) (list :bind portal statement formats values results)))
        (#\E (let ((portal (cstring)) (limit (let ((value (u32))) (if (logbitp 31 value) (- value #x100000000) value))))
               (done) (list :execute portal limit)))
        ((#\C #\D) (need 1) (let ((kind (code-char (aref payload position))))
                              (incf position)
                              (let ((name (cstring)))
                                (done) (list (if (char= type #\C) :close :describe) kind name))))
        (#\S (done) '(:sync))
        (#\H (done) '(:flush))
        (otherwise (ereport "0A000" "unsupported extended query message"))))))

(defun extended-parameter-format (formats index count)
  (case (length formats)
    (0 0)
    (1 (aref formats 0))
    (otherwise (unless (= (length formats) count)
                 (ereport "08P01" "invalid parameter format count"))
     (aref formats index))))

(defun extended-context-sensitive-literal-p (tree)
  "Identify string literals whose parse-time coercion cannot yet be retained."
  (let ((pending (list tree)))
    (loop while pending
          for item = (pop pending)
          when (consp item)
            do (when (and (eq (first item) :const) (consp (rest item))
                          (stringp (second item)))
                 (return-from extended-context-sensitive-literal-p t))
               (push (cdr item) pending)
               (push (car item) pending))
    nil))

(defun extended-select-result-columns (ast &optional register-p)
  "Plan result columns and optionally retain relation readers for the transaction."
  (multiple-value-bind (plan targets columns) (prepare-select-result ast)
    (declare (ignore targets))
    (let ((relations (getf (query-plan-prepared plan) :relations)))
      (when (and register-p relations)
        (with-registered-select-relations (registered relations)
          (declare (ignore registered)))))
    columns))

(defun extended-statement-returns-rows-p (statement)
  (let ((ast (extended-statement-ast statement)))
    (or (eq (getf ast :type) :select) (not (null (getf ast :returning))))))

(defun extended-statement-result-columns (ast &optional register-p)
  ;; Prepare DML even without RETURNING so Parse validates the statement. For
  ;; RETURNING, derive result columns through the same analysis used by execution.
  (case (getf ast :type)
    (:select (extended-select-result-columns ast register-p))
    (:do nil)
    (:copy-from (copy-from-target ast) nil)
    (otherwise
     (mapcar #'target->result-column (getf (prepare-dml-subqueries ast) :returning)))))

(defun extended-result-signature (columns)
  "Match C equalRowTypes for supported scalar result attributes."
  (mapcar (lambda (column) (list (result-column-name column)
                                 (result-column-type-oid column)
                                 (result-column-typmod column)))
          columns))

(defun extended-check-result-signature (statement columns)
  (unless (equal (extended-statement-result-signature statement)
                 (extended-result-signature columns))
    (ereport "0A000" "cached plan must not change result type"))
  columns)

(defun extended-binary-type-p (type)
  (member type '(:bool :uuid :text :varchar :bytea :json :jsonb :numeric)))

(defun extended-binary-numeric-output (value)
  "Produce numeric_send's network-order base-10000 representation."
  (let ((special (case value (:numeric-nan #xc000)
                       (:numeric-positive-infinity #xd000)
                       (:numeric-negative-infinity #xf000))))
    (if special
        (concatenate '(vector (unsigned-byte 8))
                     (u16-to-bytes 0) (u16-to-bytes 0)
                     (u16-to-bytes special) (u16-to-bytes (if (= special #xc000) 0 32)))
        (let* ((packed (sql-numeric-to-packed-octets value))
               (header (page-u16 packed 4))
               (short (= (logand header #xc000) #x8000))
               (offset (if short 6 8))
               (weight (if short
                           (let ((w (logand header #x7f))) (if (logbitp 6 w) (- w 128) w))
                           (let ((w (page-u16 packed 6))) (if (logbitp 15 w) (- w 65536) w))))
               (sign (if (if short (logbitp 13 header) (logbitp 14 header)) #x4000 0))
               (count (/ (- (length packed) offset) 2))
               (result (make-array (+ 8 (* 2 count)) :element-type '(unsigned-byte 8))))
          (replace result (u16-to-bytes count))
          (replace result (u16-to-bytes (logand weight #xffff)) :start1 2)
          (replace result (u16-to-bytes sign) :start1 4)
          (replace result (u16-to-bytes (sql-numeric-scale value)) :start1 6)
          (dotimes (i count)
            (replace result (u16-to-bytes (page-u16 packed (+ offset (* 2 i))))
                     :start1 (+ 8 (* 2 i))))
          result))))

(defun extended-binary-numeric-input (bytes parameter-number)
  "Read numeric_recv's base-10000 representation, truncating to display scale."
  (labels ((u16 (offset)
             (when (< (length bytes) (+ offset 2))
               (ereport "08P01" "insufficient data left in message"))
             (+ (ash (aref bytes offset) 8) (aref bytes (1+ offset)))))
    (let* ((count (u16 0)) (encoded-weight (u16 2))
                           (weight (if (logbitp 15 encoded-weight) (- encoded-weight 65536) encoded-weight))
                           (sign (u16 4))
                           (scale (progn
                                    (unless (member sign '(#x0000 #x4000 #xc000 #xd000 #xf000))
                                      (ereport "22P03" "invalid sign in external \"numeric\" value"))
                                    (u16 6))))
      (when (> scale #x3fff) (ereport "22P03" "invalid scale in external \"numeric\" value"))
      (let ((acc 0))
        (dotimes (i count)
          (let ((digit (u16 (+ 8 (* 2 i)))))
            (when (>= digit 10000) (ereport "22P03" "invalid digit in external \"numeric\" value"))
            (setf acc (+ (* acc 10000) digit))))
        (unless (= (length bytes) (+ 8 (* 2 count)))
          (ereport "22P03" "incorrect binary data format in bind parameter ~d" parameter-number))
        (case sign
          (#xc000 :numeric-nan) (#xd000 :numeric-positive-infinity) (#xf000 :numeric-negative-infinity)
          (otherwise
           (let* ((exponent (+ scale (* 4 (- (1+ weight) count))))
                  (coefficient (if (minusp exponent)
                                   (floor acc (expt 10 (- exponent)))
                                   (* acc (expt 10 exponent)))))
             (when (and (not (zerop coefficient))
                        (> (- (length (write-to-string coefficient :base 10)) scale) 131072))
               (ereport "22003" "value overflows numeric format"))
             (make-sql-decimal (if (= sign #x4000) (- coefficient) coefficient) scale))))))))

(defun extended-binary-input (bytes type parameter-number)
  (case type
    (:bool
     (when (zerop (length bytes))
       (ereport "08P01" "no data left in message"))
     (when (> (length bytes) 1)
       (ereport "22P03" "incorrect binary data format in bind parameter ~d" parameter-number))
     (not (zerop (aref bytes 0))))
    (:uuid
     (when (< (length bytes) +sql-uuid-size+)
       (ereport "08P01" "insufficient data left in message"))
     (when (> (length bytes) +sql-uuid-size+)
       (ereport "22P03" "incorrect binary data format in bind parameter ~d" parameter-number))
     (sql-uuid-from-octets bytes))
    (:bytea bytes)
    (:numeric (extended-binary-numeric-input bytes parameter-number))
    (otherwise
     (let* ((start (if (eq type :jsonb)
                       (progn
                         (when (zerop (length bytes)) (ereport "08P01" "insufficient data left in message"))
                         (unless (= (aref bytes 0) 1)
                           (ereport "XX000" "unsupported jsonb version number ~d" (aref bytes 0)))
                         1)
                       0))
            (text (handler-case (sb-ext:octets-to-string bytes :start start :external-format :utf-8)
                    (sb-int:character-decoding-error () (ereport "22021" "invalid UTF-8 in binary parameter")))))
       (when (position (code-char 0) text) (ereport "22021" "null byte in binary parameter"))
       (sql-cast-value text :text type -1)))))

(defun extended-select-result-formats (formats columns)
  (let ((count (length columns)))
    (unless (member (length formats) (list 0 1 count))
      (ereport "08P01" "invalid result format count"))
    (let ((result (make-array count)) (format-vector (coerce formats 'vector)))
      (loop for column in columns for index from 0
            for format = (case (length formats) (0 0) (1 (first formats)) (otherwise (aref format-vector index)))
            do (unless (or (= format 0) (and (= format 1) (extended-binary-type-p (extended-type-from-oid (result-column-type-oid column)))))
                 (ereport "0A000" "unsupported binary result format"))
               (setf (aref result index) format
                     (result-column-format column) format))
      result)))

(defun extended-initial-parameter-types (ast oids)
  "Allocate slots for supplied OIDs and parameters whose OIDs were omitted."
  (let ((count (length oids)))
    (labels ((visit (node)
               (when (consp node)
                 (if (eq (first node) :parameter)
                     (let ((index (second node)))
                       (unless (<= 1 index #xffff)
                         (ereport "42P02" "there is no parameter $~a" index))
                       (setf count (max count index)))
                     (loop for tail = node then (cdr tail)
                           while (consp tail) do (visit (car tail)))))))
      (visit ast))
    (let ((types (make-array count :initial-element nil)))
      (loop for oid in oids for slot from 0
            unless (zerop oid) do (setf (svref types slot) (extended-type-from-oid oid)))
      types)))

(defun extended-apply-parse (stream state operation)
  (destructuring-bind (kind name sql oids) operation
    (declare (ignore kind))
    (publish-backend-activity-query *backend* sql)
    (when (zerop (length name))
      (remhash "" (extended-query-state-statements state)))
    (when (and (plusp (length name)) (gethash name (extended-query-state-statements state)))
      (ereport "42P05" "prepared statement already exists"))
    (let* ((statements (parse-statements sql :collect-statement-text-p *statement-statistics-store*))
           (ast (first statements))
           (concurrent (concurrent-index-ddl-p ast))
           (slot-query-p (and (= (length statements) 1)
                              (patroni-slot-query-shape-p (first statements))))
           (types (extended-initial-parameter-types ast oids)))
      (check-rewind-sql-statements statements)
      (unless (and (= (length statements) 1)
                   (or concurrent (member (getf ast :type) '(:insert :update :delete :select :do :copy-from))))
        (ereport "0A000" "unsupported statement in extended query"))
      (when (and (not concurrent) (eq (backend-state *backend*) :idle))
        (sb-sys:without-interrupts
          (begin-extended-implicit-query-block)
          (setf (extended-query-state-implicit-p state) t)))
      (when (and (not slot-query-p)
                 (extended-context-sensitive-literal-p (first statements)))
        (ereport "0A000" "string literals in extended statements are not supported; use parameters"))
      (when (eq (backend-state *backend*) :aborted)
        (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
      ;; Parse establishes the repeatable-read horizon; planned relation
      ;; readers remain registered until transaction end.
      (let* ((*query-parameter-types* types)
             (signature
               (unless (or concurrent (eq (getf ast :type) :do))
                 (with-statement-snapshot
                     (snapshot (lambda (registration) (statement-start registration nil)))
                   (extended-result-signature (extended-statement-result-columns ast t)))))
             (statement (make-extended-statement :ast ast :types types :result-signature signature
                                                 :activity-text (subseq sql 0 (min 1024 (length sql))))))
        (let ((unknown (position nil types)))
          (when unknown
            (report-unresolved-query-parameter (1+ unknown))))
        (setf (gethash name (extended-query-state-statements state)) statement))
      (unless (eq (backend-state *backend*) :idle)
        (incf (backend-command-id *backend*)))
      (write-message stream #\1 #()))))

(defun extended-apply-bind (stream state operation payload)
  (destructuring-bind (kind portal name formats ranges results) operation
    (declare (ignore kind))
    (let* ((statement (gethash name (extended-query-state-statements state)))
           (concurrent (and statement (concurrent-index-ddl-p (extended-statement-ast statement)))))
      (unless statement (ereport "26000" "prepared statement does not exist"))
      (publish-backend-activity-query *backend* (extended-statement-activity-text statement))
      (when (eq (backend-state *backend*) :aborted)
        (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
      (when (and (not concurrent) (eq (backend-state *backend*) :idle))
        (sb-sys:without-interrupts
          (begin-extended-implicit-query-block)
          (setf (extended-query-state-implicit-p state) t)))
      (when (and (plusp (length portal)) (gethash portal (extended-query-state-portals state)))
        (ereport "42P03" "portal already exists"))
      (when (zerop (length portal)) (extended-drop-portal state ""))
      (unless (= (length ranges) (length (extended-statement-types statement)))
        (ereport "08P01" "bind parameter count mismatch"))
      (unless (and (member (length formats) (list 0 1 (length ranges)))
                   (every (lambda (format) (member format '(0 1))) formats))
        (ereport "08P01" "invalid parameter formats"))
      ;; Bind admits relation readers before taking a SELECT snapshot.
      ;; Admit the RR horizon first so those readers cannot block TRUNCATE
      ;; while waiting for its schema writer lease. DML in RC still snapshots
      ;; at Execute.
      (when (and (not concurrent)
                 (eq (backend-isolation-level *backend*) :repeatable-read)
                 (not (eq (getf (extended-statement-ast statement) :type) :do)))
        (unless (recovery-in-progress-p)
          (prepare-xid-wraparound-for-statement *backend*))
        (ensure-transaction-snapshot *backend* (backend-command-id *backend*))
        (setf (backend-first-snapshot-set *backend*) t))
      (let* ((returns-rows-p (extended-statement-returns-rows-p statement))
             (select-p (eq (getf (extended-statement-ast statement) :type) :select))
             (columns (when (and returns-rows-p (not select-p))
                        (let ((*query-parameter-types* (extended-statement-types statement)))
                          (extended-check-result-signature
                           statement (extended-statement-result-columns (extended-statement-ast statement) t)))))
             (result-formats (when (and returns-rows-p (not select-p)) (extended-select-result-formats results columns)))
             (types (extended-statement-types statement))
             (format-vector (coerce formats 'vector))
             (values (make-array (length ranges))))
        (unless (or returns-rows-p (null results) (and (= (length results) 1) (= (first results) 0)))
          (ereport "0A000" "binary result format is not supported"))
        (loop for range in ranges for index from 0
              for type = (aref types index)
              for format = (extended-parameter-format format-vector index (length values))
              do (when (and (= format 1) (not (extended-binary-type-p type)))
                   (ereport "0A000" "unsupported binary parameter format"))
                 (setf (aref values index)
                       (if (null range) +null+
                           (let* ((spooled (extended-bind-spool-p payload))
                                  (bytes (if spooled (extended-bind-spool-field payload range) payload))
                                  (start (if spooled 0 (car range)))
                                  (end (+ start (cdr range))))
                             (if (= format 0)
                                 (progn
                                   (when (position 0 bytes :start start :end end)
                                     (ereport "22021" "null byte in text parameter"))
                                   (sql-cast-value (handler-case (sb-ext:octets-to-string bytes :start start :end end :external-format :utf-8)
                                                     (sb-int:character-decoding-error ()
                                                       (ereport "22021" "invalid UTF-8 in text parameter")))
                                                   :text type -1))
                                 (extended-binary-input (if spooled bytes (subseq bytes start end)) type (1+ index)))))))
        (when (extended-bind-spool-p payload) (extended-verify-bind-spool payload))
        (let ((bound (make-extended-portal :statement statement :values values
                                           :returns-rows-p returns-rows-p
                                           :result-columns columns :result-formats result-formats)))
          (cond (select-p
                 (extended-initialize-select-portal bound :result-format-codes results))
                ((member (getf (extended-statement-ast statement) :type)
                         '(:insert :update :delete))
                 (extended-initialize-dml-portal bound)))
          (sb-sys:without-interrupts
            (setf (gethash portal (extended-query-state-portals state)) bound))
          (write-message stream #\2 #()))))))

(defun extended-execute-concurrent-index (stream state ast)
  "Finish a standalone utility immediately, preserving the surrounding Sync."
  (when (and (extended-query-state-implicit-p state)
             (not (extended-query-state-executed-in-cycle-p state))
             (not (backend-explicit *backend*)))
    ;; Parse and Bind can establish a planning horizon. With no prior Execute
    ;; this is not an implicit pipeline transaction for utility admission.
    (commit-transaction)
    (setf (extended-query-state-implicit-p state) nil))
  (let ((*deferred-command-completes* nil))
    (let ((*defer-command-complete-p* t))
      (execute-concurrent-index stream ast))
    (extended-clear-portals-for-backend *backend*)
    (dolist (response (nreverse *deferred-command-completes*))
      (apply #'send-command-complete response)))
  (clear-statement-timeout)
  t)

(defun publish-extended-portal-activity (portal)
  (let* ((statement (extended-portal-statement portal))
         (queryid
           (cond
             ((extended-portal-select-p portal)
              (let ((execution (extended-select-execution-for portal)))
                (when execution (extended-select-execution-queryid execution))))
             ((member (getf (extended-statement-ast statement) :type) '(:insert :update :delete))
              (let ((execution (extended-dml-execution-for portal)))
                (when execution (extended-dml-execution-queryid execution)))))))
    (publish-backend-activity-query *backend* (extended-statement-activity-text statement) queryid)))

(defun extended-apply-execute (stream state operation)
  (destructuring-bind (kind name limit) operation
    (declare (ignore kind))
    (setf *extended-execute-suspended-p* nil)
    (let ((portal (gethash name (extended-query-state-portals state))))
      (unless portal (ereport "34000" "portal \"~a\" does not exist" name))
      (publish-extended-portal-activity portal)
      (when (eq (backend-state *backend*) :aborted)
        (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
      (when (rewind-restricted-sql-session-p)
        (check-rewind-sql-statements (list (extended-statement-ast (extended-portal-statement portal)))))
      (let ((ast (extended-statement-ast (extended-portal-statement portal))))
        (recovery-check-statement ast)
        (when (concurrent-index-ddl-p ast)
          (return-from extended-apply-execute
            (extended-execute-concurrent-index stream state ast))))
      (when (eq (backend-state *backend*) :idle)
        (begin-extended-implicit-query-block)
        (setf (extended-query-state-implicit-p state) t))
      (let ((completed
              (cond
                ((extended-portal-select-p portal)
                 (execute-select-portal stream portal limit))
                ((extended-portal-returns-rows-p portal)
                 (extended-execute-dml-portal stream portal limit))
                (t
                 (when (extended-portal-executed-p portal)
                   (ereport "55000" "portal already executed"))
                 (let* ((statement (extended-portal-statement portal))
                        (ast (extended-statement-ast statement))
                        (*statement-command-start-advance-p* nil))
                   (if (member (getf ast :type) '(:do :copy-from))
                       (let ((*query-parameter-types* (extended-statement-types statement))
                             (*query-parameter-values* (extended-portal-values portal)))
                         (execute-statement stream ast))
                       (let ((execution (extended-dml-execution-for portal)))
                         (unless execution
                           (ereport "55000" "portal has no initialized DML execution"))
                         (call-with-query-execution-owner
                          (extended-dml-execution-owner execution)
                          (lambda ()
                            (let ((*dml-bound-preparation*
                                    (cons ast (extended-dml-execution-prepared execution))))
                              (call-with-extended-dml-observation
                               execution (lambda () (execute-statement stream ast)))))))))
                 t))))
        (setf (extended-portal-executed-p portal) t
              (extended-query-state-executed-in-cycle-p state) t)
        (when completed
          (incf (backend-command-id *backend*))
          (clear-statement-timeout))
        completed))))

(defun extended-apply-describe (stream state operation)
  (destructuring-bind (kind target name) operation
    (declare (ignore kind))
    (multiple-value-bind (columns returns-rows-p)
        (case target
          (#\S (let ((statement (gethash name (extended-query-state-statements state))))
                 (unless statement (ereport "26000" "prepared statement does not exist"))
                 (when (and (eq (backend-state *backend*) :aborted)
                            (extended-statement-returns-rows-p statement))
                   (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
                 (let ((response (make-payload)))
                   (payload-push-u16 response (length (extended-statement-types statement)))
                   (loop for type across (extended-statement-types statement)
                         do (payload-push-u32 response (type-oid type)))
                   (write-message stream #\t response))
                 (when (extended-statement-returns-rows-p statement)
                   (let ((*query-parameter-types* (extended-statement-types statement)))
                     (values (extended-check-result-signature
                              statement (extended-statement-result-columns (extended-statement-ast statement))) t)))))
          (#\P (let ((portal (gethash name (extended-query-state-portals state))))
                 (unless portal (ereport "34000" "portal \"~a\" does not exist" name))
                 (when (and (eq (backend-state *backend*) :aborted) (extended-portal-returns-rows-p portal))
                   (ereport "25P02" "current transaction is aborted, commands ignored until end of transaction block"))
                 (values (extended-portal-result-columns portal) (extended-portal-returns-rows-p portal))))
          (otherwise (ereport "08P01" "invalid describe target")))
      (if returns-rows-p (send-row-description stream columns) (write-message stream #\n #())))))

(defun extended-apply-operation (stream operation payload)
  (let ((state *extended-query-state*))
    (case (first operation)
      (:parse (extended-apply-parse stream state operation))
      (:bind (extended-apply-bind stream state operation payload))
      (:execute (extended-apply-execute stream state operation))
      (:describe (extended-apply-describe stream state operation))
      (:close
       (destructuring-bind (kind target name) operation
         (declare (ignore kind))
         (case target
           (#\S (remhash name (extended-query-state-statements state)))
           (#\P (extended-drop-portal state name))
           (otherwise (ereport "08P01" "invalid close target")))
         (write-message stream #\3 #())))
      (:sync
       ;; Explicit transactions advance a used command ID even if its portal
       ;; remains suspended or was closed. Flush does not end the command.
       (when (and (not (extended-query-state-implicit-p state))
                  (eq (backend-state *backend*) :in-progress))
         (advance-used-backend-command-id))
       (when (extended-query-state-implicit-p state)
         (when (eq (backend-state *backend*) :in-progress) (commit-transaction))
         (setf (extended-query-state-implicit-p state) nil)
         (extended-clear-portals-for-backend *backend*))
       (when (eq (backend-state *backend*) :idle)
         (extended-clear-portals-for-backend *backend*))
       (setf (extended-query-state-ignore-until-sync state) nil
             (extended-query-state-in-cycle-p state) nil
             (extended-query-state-executed-in-cycle-p state) nil)
       (send-ready-for-query stream (transaction-status-char)))
      (:flush (force-output stream)))))
