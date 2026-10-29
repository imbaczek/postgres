;;;; logical-tests.lisp -- logical row-change codec and WAL stream tests.

(in-package #:pg)

(defun test-logical-keepalive-does-not-advance-progress ()
  (let* ((runtime
           (make-logical-subscription-runtime
            :source-client :source :current-lsn 100))
         (original-send (symbol-function 'pg-client-send-standby-status))
         (sent nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg-client-send-standby-status)
                 (lambda (client lsn)
                   (setf sent (list client lsn))))
           (logical-progress-keepalive runtime 500)
           (check (= (logical-subscription-runtime-current-lsn runtime) 100)
                  "publisher keepalive WAL end does not advance subscriber progress")
           (check (equal sent '(:source 100))
                  "keepalive feedback acknowledges only the durable apply position"))
      (setf (symbol-function 'pg-client-send-standby-status) original-send))))

(defun test-logical-slot-file-failpoint-atomicity ()
  (let* ((stem (format nil "pg-logical-failpoint-~d-~d"
                       (sb-posix:getpid) (get-internal-real-time)))
         (target (merge-pathnames (make-pathname :name stem :type "slot")
                                  #p"/tmp/"))
         (temporary (make-pathname :name stem :type "tmp" :defaults target))
         (directory (make-pathname :name nil :type nil :defaults target))
         (stages '((:slot-file-before-write . "old")
                   (:slot-file-after-write . "old")
                   (:slot-file-before-fsync . "old")
                   (:slot-file-after-fsync . "old")
                   (:slot-file-before-rename . "old")
                   (:slot-file-after-rename . "new")
                   (:slot-file-before-directory-fsync . "new")
                   (:slot-file-after-directory-fsync-before-memory-publication
                    . "new"))))
    (unwind-protect
         (dolist (stage stages)
           (with-open-file (stream target :direction :output
                                          :if-exists :supersede
                                          :if-does-not-exist :create)
             (write-string "old" stream))
           (let* ((point (car stage))
                  (*logical-test-failpoint-hook*
                    (lambda (actual &rest details)
                      (declare (ignore details))
                      (when (eq actual point)
                        (error "injected slot file failure"))))
                  (failed
                    (handler-case
                        (progn
                          (atomic-replace-file
                           target temporary directory
                           (lambda (stream) (write-string "new" stream)))
                          nil)
                      (error () t)))
                  (actual
                    (with-open-file (stream target :direction :input)
                      (let ((text (make-string (file-length stream))))
                        (read-sequence text stream)
                        text))))
             (check failed (format nil "~a failpoint fired" point))
             (check (string= actual (cdr stage))
                    (format nil "~a preserves the valid old or new slot" point))
             (check (not (probe-file temporary))
                    (format nil "~a removes its temporary file" point))))
      (ignore-errors (delete-file target))
      (ignore-errors (delete-file temporary)))))

(defun test-logical-test-failpoint-lsn-threshold ()
  (check (= (logical-test-failpoint-lsn-value "1/00000005")
            #x100000005)
         "logical failpoint LSN parsing preserves both 32-bit halves")
  (check (logical-test-failpoint-position-after-p nil nil)
         "failpoints retain hit-count behavior when no LSN threshold is set")
  (check (not (logical-test-failpoint-position-after-p
               (list "slot" #x100000005) "1/00000005"))
         "an acknowledgment at the threshold does not trigger the failpoint")
  (check (logical-test-failpoint-position-after-p
          (list "slot" #x100000006) "1/00000005")
         "an acknowledgment after the threshold can trigger the failpoint")
  (check (not (logical-test-failpoint-position-after-p
               (list "slot") "1/00000005"))
         "an LSN-filtered failpoint ignores calls without a position")
  (let ((guard-file (merge-pathnames "logical-failpoint-guard" *pgdata*)))
    (check (logical-test-failpoint-guard-open-p nil)
           "a failpoint without a guard file remains eligible")
    (check (not (logical-test-failpoint-guard-open-p guard-file))
           "a guarded failpoint waits while the target marker is absent")
    (unwind-protect
         (progn
           (with-open-file (stream guard-file
                                   :direction :output
                                   :if-exists :supersede
                                   :if-does-not-exist :create)
             (write-line "target committed" stream))
           (check (logical-test-failpoint-guard-open-p guard-file)
                  "a guarded failpoint becomes eligible after the target marker"))
      (ignore-errors (delete-file guard-file)))))

(declaim (ftype (function (function) *) logical-test-with-wal-storage))

(defun test-logical-snapshot-capture-to-slot-publication-interest ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (*backend* (make-backend :database "postgres"))
            (*active-xids* nil)
            (*next-xid* 3)
            (*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (database (make-database :name "postgres" :oid 7))
            (sequence
              (make-pg-sequence :oid 701 :name "gap_counter" :start-value 10
                                :increment 1 :min-value 1 :max-value +int8-max+
                                :last-value 10 :called-p t))
            (boundary-lsn nil)
            (token nil)
            (event-p nil)
            (*logical-test-after-snapshot-capture-hook*
              (lambda (slot-name captured-lsn)
                (declare (ignore slot-name))
                (setf boundary-lsn captured-lsn)
                (sb-thread:with-mutex (*wal-lock*)
                  (check (wal-logical-catalog-event-needed-under-lock-p 7)
                         "captured snapshot reserves catalog events before slot publish"))
                ;; This is the concurrent sequence.c operation that can run
                ;; after snapshot capture releases *WRITE-LOCK* but before slot
                ;; publication makes the durable slot visible.
                (sequence-nextval "gap_counter"))))
       (setf (gethash "postgres" (catalog-databases *catalog*)) database
             (gethash "gap_counter" (database-sequences database)) sequence)
       (unwind-protect
            (progn
              (multiple-value-setq (boundary-lsn token)
                (create-exported-logical-snapshot-slot "gap_slot"))
              (let* ((exported (gethash token *exported-logical-snapshots*))
                     (captured-state
                       (getf (first
                              (snapshot-sequence-states
                               (exported-logical-snapshot-snapshot exported)))
                             :state)))
                (let* ((registration
                         (exported-logical-snapshot-registration exported))
                       (slot (exported-logical-snapshot-slot-object exported)))
                  (check (and (snapshot-registration-active-p registration)
                              (not (nth-value
                                    1
                                    (gethash registration
                                             *wal-snapshot-retention-pins*))))
                         "durable slot publication drops only the provisional WAL pin")
                  (check (and (eq slot (gethash "gap_slot" *wal-replication-slots*))
                              (eq (wal-replication-slot-wal-status slot) :reserved))
                         "the durable slot retains the restart horizon after pin handoff"))
                (check (and (= (getf captured-state :last) 10)
                            (getf captured-state :called)
                            (= (pg-sequence-last-value sequence) 11))
                       "copy retains boundary sequence state before an interleaved nextval")
                (wal-scan
                 (lambda (record)
                   (when (= (wal-record-type record)
                            +wal-record-logical-catalog-event+)
                     (let ((change
                             (logical-decode-catalog-event
                              (wal-record-payload record))))
                       (when (and (> (wal-record-end record) boundary-lsn)
                                  (eq (logical-change-kind change) :sequence-state))
                         (setf event-p t)))))
                 boundary-lsn)
                (check event-p
                       "sequence advance between capture and publication is streamable")))
         (when token
           (remove-exported-logical-snapshot-token token))
         (ignore-errors (wal-drop-replication-slot "gap_slot")))))))

(defun test-logical-retention-reload-preserves-checkpoint-delegate ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((source (asdf:system-relative-pathname
                     "pg" "backend/replication/logical-retention.lisp"))
            (original-core (symbol-function 'wal-checkpoint-core))
            (original-cleanup
              (symbol-function 'call-with-invalidated-slot-cleanup))
            (original-wrapper (symbol-function 'wal-checkpoint))
            (first-core-calls 0)
            (second-core-calls 0)
            (cleanup-calls 0)
            (first-core (lambda () (incf first-core-calls)))
            (second-core (lambda () (incf second-core-calls)))
            (test-cleanup
              (lambda (function)
                (incf cleanup-calls)
                (funcall function))))
       (unwind-protect
            (progn
              ;; Load the public wrapper, then model a WAL reload that replaces
              ;; the named physical implementation without touching that wrapper.
              (load source)
              (setf (symbol-function 'wal-checkpoint-core) first-core
                    (symbol-function 'call-with-invalidated-slot-cleanup)
                    test-cleanup)
              (let ((wrapper (symbol-function 'wal-checkpoint)))
                (wal-checkpoint)
                (check (and (= first-core-calls 1)
                            (= cleanup-calls 1))
                       "retention wrapper delegates checkpoint work through slot cleanup")
                (setf (symbol-function 'wal-checkpoint-core) second-core)
                (check (eq wrapper (symbol-function 'wal-checkpoint))
                       "WAL core reload preserves the public cleanup wrapper")
                (wal-checkpoint)
                (check (and (= second-core-calls 1)
                            (= cleanup-calls 2))
                       "the wrapper cleans up and dispatches to the reloaded WAL core"))
              ;; A retention-only reload must reinstall the cleanup wrapper.
              (load source)
              (setf (symbol-function 'wal-checkpoint-core) second-core
                    (symbol-function 'call-with-invalidated-slot-cleanup)
                    test-cleanup)
              (wal-checkpoint)
              (check (and (= second-core-calls 2)
                          (= cleanup-calls 3))
                     "retention-only reload preserves checkpoint cleanup"))
         (setf (symbol-function 'wal-checkpoint-core) original-core
               (symbol-function 'call-with-invalidated-slot-cleanup)
               original-cleanup
               (symbol-function 'wal-checkpoint) original-wrapper))))))

(defun test-logical-invalidated-backend-cleanup-is-graceful ()
  (let* ((gate-lock (sb-thread:make-mutex :name "logical termination cleanup"))
         (gate-cv (sb-thread:make-waitqueue :name "logical termination cleanup"))
         (termination-requested nil)
         (cleanup-started nil)
         (cleanup-finished nil)
         (allow-cleanup nil)
         (backend (make-backend))
         (original-terminate (symbol-function 'terminate-backend))
         (termination-requests 0)
         (thread nil))
    (setf thread
          (sb-thread:make-thread
           (lambda ()
             (sb-thread:with-mutex (gate-lock)
               (loop until termination-requested
                     do (sb-thread:condition-wait gate-cv gate-lock)))
             (unwind-protect
                  (values)
               (sb-thread:with-mutex (gate-lock)
                 (setf cleanup-started t)
                 (sb-thread:condition-broadcast gate-cv)
                 (loop until allow-cleanup
                       do (sb-thread:condition-wait gate-cv gate-lock))
                 (setf cleanup-finished t))))))
    (setf (backend-thread backend) thread)
    (unwind-protect
         (progn
           ;; Model one graceful termination request. The live backend remains
           ;; inside its unwind cleanup until the test releases the gate.
           (setf (symbol-function 'terminate-backend)
                 (lambda (requested-backend)
                   (incf termination-requests)
                   (check (eq requested-backend backend)
                          "invalidation targets its registered backend")
                   (sb-thread:with-mutex (gate-lock)
                     (setf termination-requested t)
                     (sb-thread:condition-broadcast gate-cv))))
           (terminate-invalidated-backend backend 0.01)
           (check (= termination-requests 1)
                  "bounded backend cleanup sends only one termination request")
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (gate-lock) cleanup-started))
                   2.0)
                  "invalidated backend enters its transaction cleanup")
           (check (sb-thread:thread-alive-p thread)
                  "cleanup remains alive after the bounded graceful wait")
           (sb-thread:with-mutex (gate-lock)
             (setf allow-cleanup t)
             (sb-thread:condition-broadcast gate-cv))
           (sb-thread:join-thread thread :timeout 2)
           (check cleanup-finished
                  "the invalidated backend completes cleanup before exit"))
      (setf (symbol-function 'terminate-backend) original-terminate)
      (sb-thread:with-mutex (gate-lock)
        (setf allow-cleanup t)
        (sb-thread:condition-broadcast gate-cv))
      (when (and thread (sb-thread:thread-alive-p thread))
        (sb-thread:join-thread thread :timeout 2)))))

(defun test-logical-importer-termination-retry-is-per-backend ()
  (let* ((gate-lock (sb-thread:make-mutex :name "logical importer retry"))
         (gate-cv (sb-thread:make-waitqueue :name "logical importer retry"))
         (termination-requested nil)
         (cleanup-started nil)
         (allow-cleanup nil)
         (importer-a (make-backend))
         (importer-b (make-backend))
         (state (make-exported-logical-snapshot))
         (thread nil)
         (sent nil))
    (setf (exported-logical-snapshot-importers state)
          (list importer-a importer-b)
          thread
          (sb-thread:make-thread
           (lambda ()
             (sb-thread:with-mutex (gate-lock)
               (loop until termination-requested
                     do (sb-thread:condition-wait gate-cv gate-lock)))
             (unwind-protect
                  (values)
               (sb-thread:with-mutex (gate-lock)
                 (setf cleanup-started t)
                 (sb-thread:condition-broadcast gate-cv)
                 (loop until allow-cleanup
                       do (sb-thread:condition-wait gate-cv gate-lock)))))))
    (setf (backend-thread importer-a) thread)
    (unwind-protect
         (let ((*logical-test-invalidated-backend-hook*
                 (lambda (backend)
                   (push backend sent)
                   (when (eq backend importer-a)
                     (sb-thread:with-mutex (gate-lock)
                       (setf termination-requested t)
                       (sb-thread:condition-broadcast gate-cv))
                     (throw 'after-first-importer-request :interrupted)))))
           (check (eq (catch 'after-first-importer-request
                        (request-exported-snapshot-importer-termination state)
                        :completed)
                      :interrupted)
                  "first importer request is interrupted before the next importer")
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (gate-lock) cleanup-started))
                   2.0)
                  "first importer has entered transaction cleanup")
           (check (sb-thread:thread-alive-p thread)
                  "first importer remains inside its cleanup while retry runs")
           (check (equal (request-exported-snapshot-importer-termination state)
                         (list importer-b))
                  "retry requests only the importer not yet signaled")
           (check (equal (nreverse sent) (list importer-a importer-b))
                  "each importer receives exactly one graceful termination"))
      (sb-thread:with-mutex (gate-lock)
        (setf allow-cleanup t)
        (sb-thread:condition-broadcast gate-cv))
      (when (and thread (sb-thread:thread-alive-p thread))
        (sb-thread:join-thread thread :timeout 2)))))

(defun test-logical-token-publication-failure-preserves-other-schema-lease ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (*backend* (make-backend :database "postgres"))
            (*active-xids* nil)
            (*next-xid* 3)
            (*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (*exported-logical-snapshots* (make-hash-table :test #'equal))
            (*expired-exported-logical-snapshots* (make-hash-table :test #'equal))
            (*schema-read-lock-count* 0)
            (database (make-database :name "postgres" :oid 7))
            (baseline-schema-readers nil)
            (caught nil))
       (setf (gethash "postgres" (catalog-databases *catalog*)) database)
       ;; A separate lease detects an accidental double release when token
       ;; ownership moves into the export registry and its caller unwinds.
       (acquire-schema-read-lock (lambda () nil))
       (setf baseline-schema-readers *schema-read-lock-count*)
       (let ((*logical-test-failpoint-hook*
               (lambda (point &rest details)
                 (declare (ignore details))
                 (when (eq point :after-snapshot-token-publication)
                   (error "injected failure after token publication")))))
         (handler-case
             (create-exported-logical-snapshot-slot "token-publication-fault")
           (error () (setf caught t))))
       (check caught "token-publication failpoint interrupts export return")
       (check (and (= *schema-read-lock-count* baseline-schema-readers)
                   (plusp baseline-schema-readers))
              "export cleanup releases its lease without releasing another export")
       (check (and (zerop *active-snapshots*)
                   (zerop (hash-table-count *exported-logical-snapshots*))
                   (zerop (hash-table-count *expired-exported-logical-snapshots*)))
              "token-publication failure retires its MVCC and registry ownership")
       (check (null (gethash "token-publication-fault" *wal-replication-slots*))
              "token-publication failure removes its unconfirmed source slot")
       (release-schema-read-lock)))))

(defun logical-test-append-batch (xid changes &optional commit-p)
  ;; Like reorderbuffer.c, the reader must not expose queued changes until a
  ;; transaction's commit record closes the WAL batch.
  (wal-append-batch-start)
  (when changes
    (wal-append-logical-changes xid (mapcar #'logical-encode-change changes)))
  (when commit-p
    (wal-append-record +wal-record-commit+ xid #())))

(defun logical-test-append-catalog-event (database-oid ast)
  ;; Match wal-commit-catalog's event record followed by a catalog terminal.
  (let ((empty (make-array 0 :element-type '(unsigned-byte 8))))
    (wal-append-batch-start)
    (wal-append-record +wal-record-logical-catalog-event+ 0
                       (logical-encode-catalog-event database-oid ast))
    (wal-append-record +wal-record-catalog+ 0
                       (wal-encode-catalog-payload empty nil))))

(defun logical-test-with-wal-storage (function)
  (let* ((test-pgdata
           (merge-pathnames (format nil "logical-stream-~a/" (random 100000000))
                            *pgdata*))
         (*pgdata* test-pgdata)
         (*backend* nil)
         (*wal-ready* t)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*clog-stream* nil)
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
         (*max-slot-wal-keep-bytes* nil))
    (ignore-errors
     (uiop:delete-directory-tree test-pgdata :validate t
                                             :if-does-not-exist :ignore))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (unwind-protect
         (funcall function)
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (when *clog-stream*
        (ignore-errors (close *clog-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun logical-test-sized-catalog-event (size)
  (let* ((ast '(:type :alter-table :name "size_probe" :actions ()))
         (schema '(:tables () :indexes () :sequences ()))
         (envelope (list :schema-contract-version 1
                         :database-oid 7
                         :ast ast
                         :schema-before schema
                         :schema-after schema
                         :padding ""))
         (base-payload (catalog-readable-data-string envelope))
         (base-event
           (logical-wrap-catalog-event
            7
            (logical-encode-change
             (make-logical-change :kind :ddl :database-oid 7
                                  :event-payload base-payload))))
         (padding-size (- size (length base-event))))
    (unless (and (logical-supported-ddl-ast-p ast)
                 (logical-schema-contract-valid-p schema))
      (error "catalog event fixture must carry a supported AST and schema"))
    (when (minusp padding-size)
      (error "catalog event size ~d is below its valid envelope size" size))
    (setf (getf envelope :padding)
          (make-string padding-size :initial-element #\x))
    (let* ((payload (catalog-readable-data-string envelope))
           (event
             (logical-wrap-catalog-event
              7
              (logical-encode-change
               (make-logical-change :kind :ddl :database-oid 7
                                    :event-payload payload))))
           (decoded (logical-decode-catalog-event event))
           (decoded-envelope
             (logical-read-data-string
              (logical-change-event-payload decoded))))
      (logical-validate-ddl-event-envelope decoded-envelope)
      (unless (= (length event) size)
        (error "catalog event fixture has size ~d, expected ~d"
               (length event) size))
      event)))

(defun test-logical-catalog-event-size-boundaries ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-pending-deletes* nil)
            (slot (make-wal-replication-slot
                   :name "catalog_size_slot" :database-oid 7
                   :restart-lsn 0 :consistent-lsn 0
                   :confirmed-flush-lsn 0))
            (limit (- +wal-segment-size+ +wal-header-size+))
            (catalog-bytes (catalog-image-bytes))
            (event-sizes '()))
       (ensure-directories-exist
        (merge-pathnames "global/pg_catalog.dat" *pgdata*))
       (setf (gethash "catalog_size_slot" *wal-replication-slots*) slot)
       (labels ((append-event (size)
                  (let ((start *wal-end-lsn*))
                    (handler-case
                        (progn
                          (wal-commit-catalog
                           catalog-bytes
                           (lambda (bytes) (declare (ignore bytes)))
                           :logical-event
                           (logical-test-sized-catalog-event size))
                          (wal-flush)
                          (values t start *wal-end-lsn*))
                      (error ()
                        (values nil start *wal-end-lsn*)))))
                (scan-event-sizes ()
                  (let ((sizes '()))
                    (wal-scan
                     (lambda (record)
                       (when (= (wal-record-type record)
                                +wal-record-logical-catalog-event+)
                         (push (length (wal-record-payload record)) sizes))))
                    (nreverse sizes))))
         (dolist (delta '(-1 0))
           (let ((size (+ limit delta)))
             (multiple-value-bind (accepted start end) (append-event size)
               (check accepted
                      (format nil "catalog event accepts ~a payload bytes" size))
               (check (> end start)
                      "an accepted catalog event advances durable WAL")
               (push size event-sizes))))
         (let ((before *wal-end-lsn*))
           (multiple-value-bind (accepted start end)
               (append-event (1+ limit))
             (check (not accepted)
                    "catalog event rejects one byte beyond a WAL segment")
             (check (and (= start before) (= end before))
                    "oversize catalog event is rejected before WAL append")
             (check (not *wal-poisoned*)
                    "oversize catalog input leaves the WAL healthy")))
         (multiple-value-bind (accepted start end) (append-event 1024)
           (check accepted
                  "WAL accepts a valid catalog event after oversize rejection")
           (check (> end start)
                  "WAL remains appendable after oversize rejection")
           (push 1024 event-sizes))
         (let ((expected (nreverse event-sizes)))
           (check (equal (scan-event-sizes) expected)
                  "only valid complete catalog events are present before recovery")
           ;; Recovery validates the length-prefixed logical.c event envelope
           ;; before applying each catalog terminal, as wal-replay-record does.
           (wal-recover)
           (check (equal (scan-event-sizes) expected)
                  "valid segment-boundary catalog events survive WAL recovery")
           (remhash "catalog_size_slot" *wal-replication-slots*)
           (let ((before *wal-end-lsn*))
             (wal-commit-catalog
              catalog-bytes
              (lambda (bytes) (declare (ignore bytes)))
              :logical-event
              (logical-test-sized-catalog-event (1+ limit)))
             (wal-flush)
             (check (> *wal-end-lsn* before)
                    "catalog commits when no slot needs an oversized event")
             (check (equal (scan-event-sizes) expected)
                    "oversized catalog events are filtered when no slot retains them")
             (check (not *wal-poisoned*)
                    "filtering an oversized unneeded event leaves WAL healthy"))))))))

(defun logical-test-reader-scan-boundary (boundary)
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((limit +max-logical-read-scan-bytes+)
            (start (if (eq boundary :above) +wal-header-size+ 0))
            (expected-distance (ecase boundary
                                 (:below (1- limit))
                                 (:at limit)
                                 (:above (1+ limit))))
            (slot (make-wal-replication-slot
                   :name "scan_boundary_slot" :database-oid 7
                   :restart-lsn start :consistent-lsn start
                   :confirmed-flush-lsn start)))
       (check (= limit +wal-segment-size+)
              "the scan-boundary fixture aligns with a WAL segment")
       (ecase boundary
         ((:below :at)
          (wal-append-batch-start)
          (wal-append-record
           +wal-record-maintenance+ 0
           (make-array (- expected-distance (* 2 +wal-header-size+))
                       :element-type '(unsigned-byte 8) :initial-element 0)))
         (:above
          ;; Start just after a record, complete one batch before the segment
          ;; end, then force the next terminal to rotate and end one byte beyond
          ;; the scan cap. Like XLogReadRecord(), the reader returns only a full
          ;; terminal boundary and never stops inside a record.
          (wal-append-record +wal-record-maintenance+ 0 #())
          (wal-append-batch-start)
          (wal-append-record
           +wal-record-maintenance+ 0
           (make-array (- +wal-segment-size+ (* 5 +wal-header-size+))
                       :element-type '(unsigned-byte 8) :initial-element 0))
          (wal-append-batch-start)
          (wal-append-record
           +wal-record-maintenance+ 0
           (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0))))
       (wal-flush)
       (setf (gethash "scan_boundary_slot" *wal-replication-slots*) slot)
       (call-with-logical-replication-slot
        "scan_boundary_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (multiple-value-bind (transactions progress)
              (wal-logical-slot-read-transactions "scan_boundary_slot")
            (check (null transactions)
                   "scan-cap progress does not manufacture a transaction")
            (check (= (- progress start) expected-distance)
                   (format nil "~a scan cap returns its complete terminal frontier"
                           boundary)))))))))

(defun test-logical-reader-scan-byte-boundaries ()
  (dolist (boundary '(:below :at :above))
    (logical-test-reader-scan-boundary boundary)))

(defun logical-test-assert-owned-reader-progress (slot first-end later-end)
  (call-with-logical-replication-slot
   "logical_slot"
   (lambda (owned-slot)
     (declare (ignore owned-slot))
     (let ((bounded-transactions
             (wal-logical-slot-read-transactions
              "logical_slot" :max-transactions 1)))
       (check (equal (mapcar #'logical-transaction-xid bounded-transactions)
                     '(901))
              "logical reader stops at its transaction-count boundary"))
     (let ((bounded-transactions
             (wal-logical-slot-read-transactions
              "logical_slot" :max-transactions 8 :max-bytes 1)))
       (check (equal (mapcar #'logical-transaction-xid bounded-transactions)
                     '(901))
              "byte budget returns one complete oversized transaction"))
     (check (handler-case
                (progn
                  (wal-logical-slot-acknowledge "logical_slot" later-end)
                  nil)
              (error () t))
            "slot cannot acknowledge a transaction beyond read progress")
     (check (handler-case
                (progn
                  (wal-logical-slot-acknowledge "logical_slot" "invalid")
                  nil)
              (error () t))
            "logical acknowledgment rejects non-integer LSNs")
     (let ((transactions (wal-logical-slot-read-transactions "logical_slot")))
       (check (equal (mapcar #'logical-transaction-xid transactions) '(901 903))
              "reader drops crash tails and filters other databases")
       (check (< (logical-transaction-commit-lsn (first transactions))
                 (logical-transaction-commit-lsn (second transactions)))
              "logical transactions are emitted in durable commit order")
       (check (equal (mapcar #'logical-change-kind
                             (logical-transaction-changes (first transactions)))
                     '(:insert :update))
              "row changes retain executor order within a transaction")
       (check (handler-case
                  (progn
                    (wal-logical-slot-acknowledge
                     "other_slot"
                     (logical-transaction-commit-lsn (first transactions)))
                    nil)
                (error () t))
              "a reader cannot acknowledge a different slot")
       (wal-logical-slot-acknowledge
        "logical_slot"
        (logical-transaction-commit-lsn (first transactions)))
       (check (= (wal-replication-slot-confirmed-flush-lsn slot) first-end)
              "slot acknowledgment persists the applied commit boundary")
       (multiple-value-bind (transactions progress-lsn)
           (wal-logical-slot-read-transactions
            "logical_slot" :max-scan-bytes 1)
         (check (null transactions)
                "bounded scan can make progress without row transactions")
         (check (> progress-lsn first-end)
                "empty read returns its last complete terminal LSN")
         (wal-logical-slot-acknowledge "logical_slot" progress-lsn))
       (check (equal (mapcar #'logical-transaction-xid
                             (wal-logical-slot-read-transactions "logical_slot"))
                     '(903))
              "a second read resumes after the durable acknowledgment")))))

(defun test-logical-wal-reader-commit-order-and-slot-ack ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((slot (make-wal-replication-slot
                   :name "logical_slot" :database-oid 7 :restart-lsn 0
                   :consistent-lsn 0 :confirmed-flush-lsn 0))
            (other-slot (make-wal-replication-slot
                         :name "other_slot" :database-oid 7 :restart-lsn 0
                         :consistent-lsn 0 :confirmed-flush-lsn 0))
            (lost-slot (make-wal-replication-slot
                        :name "lost_slot" :database-oid 7 :restart-lsn 0
                        :consistent-lsn 0 :confirmed-flush-lsn 0
                        :wal-status :lost))
            (first-end nil)
            (later-end nil))
       (setf (gethash "logical_slot" *wal-replication-slots*) slot
             (gethash "other_slot" *wal-replication-slots*) other-slot
             (gethash "lost_slot" *wal-replication-slots*) lost-slot)
       ;; An unterminated batch before a later batch mirrors a crash tail that
       ;; wal-replay-record discards when it sees the next batch-start record.
       (logical-test-append-batch
        900 (list (logical-test-change :insert 7 701
                                       :new-values #(90 0 "aborted"))))
       (logical-test-append-batch
        901 (list (logical-test-change :insert 7 701
                                       :new-values #(10 1 "first"))
                  (logical-test-change :update 7 701
                                       :old-values #(10 1 "first")
                                       :new-values #(10 2 "second")))
        t)
       (setf first-end *wal-end-lsn*)
       ;; A slot is database-scoped even though the physical WAL is shared.
       (logical-test-append-batch
        902 (list (logical-test-change :insert 8 801
                                       :new-values #(20 2 "other db")))
        t)
       (logical-test-append-batch
        903 (list (logical-test-change :delete 7 701
                                       :old-values #(30 3 "third")))
        t)
       (setf later-end *wal-end-lsn*)
       ;; The scan's durable horizon may also end in an incomplete crash tail.
       (logical-test-append-batch
        904 (list (logical-test-change :insert 7 701
                                       :new-values #(40 4 "uncommitted"))))
       (wal-flush)
       (wal-recover)
       (check (handler-case
                  (progn
                    (call-with-logical-replication-slot "lost_slot"
                                                        (lambda (owned-slot) (declare (ignore owned-slot))))
                    nil)
                (error () t))
              "logical readers cannot acquire a slot whose WAL was lost")
       (check (handler-case
                  (progn (wal-logical-slot-read-transactions "logical_slot") nil)
                (error () t))
              "logical slot reads require an explicit active owner")
       (check (handler-case
                  (progn (wal-logical-slot-acknowledge "logical_slot" first-end)
                         nil)
                (error () t))
              "logical acknowledgment requires ownership of its slot")
       (logical-test-assert-owned-reader-progress slot first-end later-end)))))

(defun logical-test-large-change (id payload)
  (make-logical-change
   :kind :insert :database-oid 7 :relation-oid 702
   :relation-name "wide_logical_rows"
   :columns (list (list :attnum 1 :name "id" :type :int4
                        :typmod -1 :notnull t)
                  (list :attnum 2 :name "payload" :type :varchar
                        :typmod -1 :notnull nil))
   :key-attnums '(1) :new-values (vector id payload)))

(defun logical-test-large-change-of-size (id database-oid byte-count)
  "Build an encoded row-change fixture of BYTE-COUNT bytes, independent of SQL input limits."
  (let* ((template (logical-test-large-change id ""))
         (template-bytes (length (logical-encode-change template)))
         (payload-bytes (- byte-count template-bytes)))
    (unless (and (integerp payload-bytes) (not (minusp payload-bytes)))
      (error "requested logical change size is smaller than its fixed metadata"))
    (let ((change
            (logical-test-large-change
             id (make-string payload-bytes :initial-element #\x))))
      (setf (logical-change-database-oid change) database-oid)
      (unless (= (length (logical-encode-change change)) byte-count)
        (error "logical test change did not encode to its requested size"))
      change)))

(defun test-logical-safe-progress-reaches-ddl-after-scan-budget ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (slot (make-wal-replication-slot
                   :name "scan_progress_slot" :database-oid 7
                   :restart-lsn 0 :consistent-lsn 0 :confirmed-flush-lsn 0))
            (runtime
              (make-logical-subscription-runtime
               :options
               (make-logical-subscription-options :slot-name "scan_progress_slot")
               :target :target :source-client :source :current-lsn 0))
            (symbols '(logical-persist-progress
                       pg-client-send-standby-status
                       replication-standby-status
                       send-logical-progress
                       send-logical-transaction
                       send-logical-keepalive))
            (originals (mapcar (lambda (symbol)
                                 (cons symbol (symbol-function symbol)))
                               symbols))
            (persisted '())
            (durable-marker nil)
            (actions '())
            (feedback nil)
            (pending-statuses 0)
            (progress-messages 0)
            (keepalives 0)
            (delivered nil)
            (crashed nil)
            (crash-actions nil))
       (setf (gethash "scan_progress_slot" *wal-replication-slots*) slot
             (symbol-function 'logical-persist-progress)
             (lambda (client slot-name lsn state token &rest options)
               (declare (ignore client state token options))
               ;; This models the separately durable target progress row. A
               ;; restarted runtime below is rebuilt only from this value.
               (setf durable-marker lsn)
               (push (cons slot-name lsn) persisted)
               (push :persist actions))
             (symbol-function 'pg-client-send-standby-status)
             (lambda (client lsn)
               (declare (ignore client))
               (setf feedback lsn)
               (incf pending-statuses)
               (push :feedback actions))
             (symbol-function 'replication-standby-status)
             (lambda (stream)
               (declare (ignore stream))
               (when (plusp pending-statuses)
                 (decf pending-statuses)
                 (push :feedback actions)
                 (list :write feedback :flush feedback :apply feedback)))
             (symbol-function 'send-logical-progress)
             (lambda (stream lsn)
               (declare (ignore stream))
               (incf progress-messages)
               (logical-pgoutput-dispatch
                runtime (pgoutput-build-safe-progress-message lsn)))
             (symbol-function 'send-logical-transaction)
             (lambda (stream transaction wal-end)
               (declare (ignore stream wal-end))
               (setf delivered transaction
                     feedback (logical-transaction-commit-lsn transaction))
               ;; Model the subscriber COMMIT followed by its feedback.
               (push :target-commit actions)
               (incf pending-statuses))
             (symbol-function 'send-logical-keepalive)
             (lambda (stream lsn)
               (declare (ignore stream lsn))
               (incf keepalives)))
       (unwind-protect
            (progn
              ;; Three 8 MiB transactions from another database exceed the
              ;; reader's 16 MiB scan budget before the relevant DDL event.
              ;; This follows reorderbuffer.c's bounded transaction batches.
              (dotimes (index 3)
                (logical-test-append-batch
                 (+ 810 index)
                 (list (logical-test-large-change-of-size
                        index 8 (* 8 1024 1024)))
                 t))
              (logical-test-append-catalog-event
               7 '(:type :alter-sequence :name "counter" :restart 19))
              (wal-flush)
              ;; Crash after the target commits safe progress but before it sends
              ;; feedback. This is the durable-marker/ack gap in worker.c.
              (let ((*logical-test-failpoint-hook*
                      (lambda (point &rest details)
                        (declare (ignore details))
                        (when (and (eq point :after-scan-progress-before-feedback)
                                   (not crashed))
                          (setf crashed t)
                          (throw 'scan-progress-crash :crashed)))))
                (let ((result
                        (catch 'scan-progress-crash
                          (call-with-logical-replication-slot
                           "scan_progress_slot"
                           (lambda (owned-slot)
                             (declare (ignore owned-slot))
                             (stream-logical-replication-loop
                              :stream "scan_progress_slot" 0))))))
                  (setf crash-actions (reverse actions))
                  (check (and crashed (eq result :crashed))
                         "scan progress failpoint interrupts before feedback")
                  (check (and (= (length persisted) 1)
                              durable-marker
                              (string= durable-marker (cdar persisted))
                              (null feedback)
                              (zerop pending-statuses)
                              (zerop
                               (wal-replication-slot-confirmed-flush-lsn slot)))
                         "safe target marker survives while publisher confirmation stays behind")
                  (check (equal crash-actions '(:persist))
                         "the interrupted scan persisted before sending no feedback")))
              ;; Resume using the target's already durable marker. START_REPLICATION
              ;; accepts that terminal position, then the reader reaches the DDL.
              (let ((restart-lsn (parse-replication-lsn durable-marker)))
                (setf runtime
                      (make-logical-subscription-runtime
                       :options
                       (make-logical-subscription-options
                        :slot-name "scan_progress_slot")
                       :target :target :source-client :source
                       :current-lsn restart-lsn)
                      actions nil)
                (call-with-logical-replication-slot
                 "scan_progress_slot"
                 (lambda (owned-slot)
                   (declare (ignore owned-slot))
                   (wal-logical-slot-accept-resume-position
                    "scan_progress_slot" restart-lsn)
                   (stream-logical-replication-loop
                    :stream "scan_progress_slot" restart-lsn))))
              (check (plusp progress-messages)
                     "publisher sends safe progress after the scan budget")
              (check (= progress-messages (length persisted))
                     "subscriber durably records each sent scan frontier")
              (let* ((ordered-actions (reverse actions))
                     (persist-position (position :persist ordered-actions))
                     (first-feedback (position :feedback ordered-actions))
                     (target-commit (position :target-commit ordered-actions))
                     (last-feedback
                       (position :feedback ordered-actions :from-end t)))
                (check (and persist-position first-feedback
                            (< persist-position first-feedback))
                       "restarted subscriber persists scan progress before feedback")
                (check (and target-commit last-feedback
                            (< target-commit last-feedback))
                       "restarted subscriber commits the pending DDL before feedback"))
              (check (and delivered
                          (zerop (logical-transaction-xid delivered))
                          (eq (logical-change-kind
                               (first (logical-transaction-changes delivered)))
                              :ddl))
                     "publisher loop reaches the pending DDL after scan progress")
              (check (= (wal-replication-slot-confirmed-flush-lsn slot)
                        (logical-transaction-commit-lsn delivered))
                     "publisher acknowledgment advances from the durable resume marker through DDL")
              (check (= keepalives 1)
                     "an idle keepalive does not carry additional scan progress"))
         (dolist (entry originals)
           (setf (symbol-function (car entry)) (cdr entry))))))))

(defun test-logical-copy-chunk-byte-boundaries ()
  (let* ((originals
           (mapcar (lambda (symbol)
                     (cons symbol (symbol-function symbol)))
                   '(logical-validate-copy-schema
                     pg-client-query
                     logical-copy-target-chunk)))
         (source-rows '())
         (chunk-sizes '())
         (chunk-ids '()))
    (setf (symbol-function 'logical-validate-copy-schema)
          (lambda (source target table)
            (declare (ignore source target table))
            '("id" "payload"))
          (symbol-function 'pg-client-query)
          (lambda (client query &key on-row-with-owner field-budget)
            (declare (ignore field-budget))
            (declare (ignore client query))
            (dolist (row source-rows)
              (funcall on-row-with-owner row nil nil)))
          (symbol-function 'logical-copy-target-chunk)
          (lambda (target table columns rows)
            (declare (ignore target table columns))
            (push (loop for row in rows sum (logical-copy-row-size row))
                  chunk-sizes)
            (push (mapcar (lambda (row) (aref row 0)) (reverse rows)) chunk-ids)))
    (unwind-protect
         (flet ((run-copy (bytes)
                  (setf source-rows
                        (list (vector "1" (make-string (- bytes 65)
                                                       :initial-element #\x)))
                        chunk-sizes '())
                  (handler-case
                      (progn
                        (logical-copy-source-table :source :target "copy_rows")
                        (values t (nreverse chunk-sizes)))
                    (error ()
                      (values nil (nreverse chunk-sizes))))))
           (dolist (delta '(-1 0))
             (let ((expected (+ +logical-copy-chunk-bytes+ delta)))
               (multiple-value-bind (copied-p sizes) (run-copy expected)
                 (check (and copied-p (equal sizes (list expected)))
                        "initial copy accepts a row below or exactly at 8 MiB"))))
           (multiple-value-bind (copied-p sizes)
               (run-copy (1+ +logical-copy-chunk-bytes+))
             (check (and copied-p (equal sizes (list (1+ +logical-copy-chunk-bytes+))))
                    "initial copy accepts an oversized row as a singleton"))
           (setf source-rows
                 (list (vector "1" "small")
                       (vector "2" (make-string +logical-copy-chunk-bytes+ :initial-element #\x))
                       (vector "3" "small")
                       (vector "4" (make-string +logical-copy-chunk-bytes+ :initial-element #\y)))
                 chunk-sizes '() chunk-ids '())
           (logical-copy-source-table :source :target "copy_rows")
           (check (equal (nreverse chunk-sizes)
                         (mapcar #'logical-copy-row-size source-rows))
                  "large rows flush separately")
           (check (equal (nreverse chunk-ids) '(("1") ("2") ("3") ("4")))
                  "small and large singleton chunks preserve source order"))
      (dolist (entry originals)
        (setf (symbol-function (car entry)) (cdr entry))))))

(defun test-logical-reader-transaction-count-boundaries ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (slot (make-wal-replication-slot
                   :name "count_limit_slot" :database-oid 7 :restart-lsn 0
                   :consistent-lsn 0 :confirmed-flush-lsn 0))
            (below-end nil)
            (at-end nil))
       (setf (gethash "count_limit_slot" *wal-replication-slots*) slot)
       (dotimes (index 129)
         (logical-test-append-batch
          (+ 9200 index)
          (list (logical-test-change
                 :insert 7 701 :new-values (vector index index "row")))
          t)
         (when (= index 126) (setf below-end *wal-end-lsn*))
         (when (= index 127) (setf at-end *wal-end-lsn*)))
       (wal-flush)
       (call-with-logical-replication-slot
        "count_limit_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (multiple-value-bind (below progress)
              (wal-logical-slot-read-transactions
               "count_limit_slot" :upto-lsn below-end)
            (check (and (= (length below) 127) (= progress below-end))
                   "reader returns just below its 128-transaction cap"))
          (multiple-value-bind (at progress)
              (wal-logical-slot-read-transactions "count_limit_slot")
            (check (and (= (length at) 128) (= progress at-end))
                   "reader returns exactly 128 committed transactions"))
          (wal-logical-slot-acknowledge "count_limit_slot" at-end)
          (multiple-value-bind (above progress)
              (wal-logical-slot-read-transactions "count_limit_slot")
            (check (and (= (length above) 1)
                        (= (logical-transaction-xid (first above)) 9328)
                        (> progress at-end))
                   "the transaction above the read cap remains for the next batch"))))))))

(defun test-logical-reader-change-byte-boundaries ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (below-size (1- +max-logical-read-bytes+))
            (at-size +max-logical-read-bytes+)
            (above-size (1+ +max-logical-read-bytes+))
            (below-end nil)
            (at-end nil)
            (above-end nil))
       (logical-test-append-batch
        9401 (list (logical-test-large-change-of-size 1 7 below-size)) t)
       (setf below-end *wal-end-lsn*)
       (logical-test-append-batch
        9402 (list (logical-test-large-change-of-size 2 7 at-size)) t)
       (setf at-end *wal-end-lsn*)
       (logical-test-append-batch
        9403 (list (logical-test-large-change-of-size 3 7 above-size)) t)
       (setf above-end *wal-end-lsn*)
       (setf (gethash "bytes_below_slot" *wal-replication-slots*)
             (make-wal-replication-slot
              :name "bytes_below_slot" :database-oid 7 :restart-lsn 0
              :consistent-lsn 0 :confirmed-flush-lsn 0)
             (gethash "bytes_at_slot" *wal-replication-slots*)
             (make-wal-replication-slot
              :name "bytes_at_slot" :database-oid 7 :restart-lsn 0
              :consistent-lsn 0 :confirmed-flush-lsn below-end)
             (gethash "bytes_above_slot" *wal-replication-slots*)
             (make-wal-replication-slot
              :name "bytes_above_slot" :database-oid 7 :restart-lsn 0
              :consistent-lsn 0 :confirmed-flush-lsn at-end))
       (wal-flush)
       (labels ((read-batch-bytes (slot-name upto)
                  (call-with-logical-replication-slot
                   slot-name
                   (lambda (owned-slot)
                     (declare (ignore owned-slot))
                     (multiple-value-bind (transactions progress)
                         (wal-logical-slot-read-transactions
                          slot-name :upto-lsn upto)
                       (values
                        (length transactions) progress
                        (if transactions
                            (reduce #'+
                                    (logical-transaction-changes
                                     (first transactions))
                                    :key (lambda (change)
                                           (length
                                            (logical-encode-change change))))
                            0)))))))
         (multiple-value-bind (count progress bytes)
             (read-batch-bytes "bytes_below_slot" below-end)
           (check (and (= count 1) (= progress below-end)
                       (= bytes below-size))
                  "reader accepts a transaction one byte below 8 MiB"))
         (multiple-value-bind (count progress bytes)
             (read-batch-bytes "bytes_at_slot" at-end)
           (check (and (= count 1) (= progress at-end) (= bytes at-size))
                  "reader accepts a transaction exactly at 8 MiB"))
         (multiple-value-bind (count progress bytes)
             (read-batch-bytes "bytes_above_slot" above-end)
           (check (and (= count 1) (= progress above-end)
                       (= bytes above-size))
                  "reader returns one whole transaction above 8 MiB")))))))

(defun test-logical-wal-apply-headroom-recovery ()
  ;; Exercise the inclusive recovery limit across multiple WAL chunks, each
  ;; smaller than one segment, as DecodeCommit/reorderbuffer do.
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((change-count 7)
            (limit +max-logical-wal-transaction-bytes+)
            (base-size (floor limit change-count))
            (remainder (mod limit change-count))
            (changes
              (loop for id from 1 to change-count
                    for size = (+ base-size (if (<= id remainder) 1 0))
                    collect (logical-test-large-change-of-size id 7 size)))
            (change-bytes
              (reduce #'+ changes
                      :key (lambda (change)
                             (length (logical-encode-change change)))
                      :initial-value 0))
            (slot (make-wal-replication-slot
                   :name "headroom_slot" :database-oid 7 :restart-lsn 0
                   :consistent-lsn 0 :confirmed-flush-lsn 0)))
       (check (= change-bytes limit)
              "WAL recovery fixture reaches exactly 65 MiB of row changes")
       (setf (gethash "headroom_slot" *wal-replication-slots*) slot)
       (logical-test-append-batch 9901 changes t)
       (setf changes nil)
       (wal-flush)
       (wal-recover)
       (call-with-logical-replication-slot
        "headroom_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (let* ((transactions
                   (wal-logical-slot-read-transactions
                    "headroom_slot" :max-bytes (* 8 1024 1024)))
                 (transaction (first transactions))
                 (recovered-bytes
                   (and transaction
                        (reduce #'+ (logical-transaction-changes transaction)
                                :key (lambda (change)
                                       (length (logical-encode-change change)))
                                :initial-value 0))))
            (check (and (= (length transactions) 1)
                        (= (length (logical-transaction-changes transaction))
                           change-count)
                        (= recovered-bytes limit))
                   "WAL recovery and the logical reader preserve one exact-cap commit"))))))))

(defun logical-test-commit-boundary-batch (xid)
  (with-wal-critical
    (let ((batch-start (wal-append-batch-start)))
      ;; Model an earlier page batch from XID before a logical slot captures its
      ;; snapshot; the slot must retain this active XID through the later commit.
      (wal-record-page-xids-at-batch-start
       (list (make-wal-page-image :xids (list xid))) batch-start)
      (wal-append-record +wal-record-maintenance+ 0 #())
      (wal-flush)
      batch-start)))

(defun logical-test-commit-insert (xid table values)
  (let* ((*backend* (make-backend :xid xid))
         (*active-xids* (cons xid *active-xids*)))
    ;; Model allocation as well as registration: every active XID precedes NEXT.
    (sb-thread:with-mutex (*xid-lock*)
      (setf *next-xid* (max *next-xid* (advance-full-xid xid))))
    (setf (backend-database *backend*) "postgres"
          (backend-state *backend*) :in-progress)
    (logical-note-row-change :insert table :new-values values)
    (commit-transaction)))

(defun logical-test-abort-insert (xid table)
  (let* ((*backend* (make-backend :xid xid))
         (*active-xids* (cons xid *active-xids*))
         (wal-end-before *wal-end-lsn*))
    (setf (backend-database *backend*) "postgres"
          (backend-state *backend*) :in-progress)
    (logical-note-row-change :insert table :new-values #(58 7 "aborted"))
    (abort-transaction)
    (check (null (backend-logical-changes *backend*))
           "rollback discards its transaction-local logical queue")
    (check (= *wal-end-lsn* wal-end-before)
           "rollback emits no logical WAL records")))

(defun test-logical-snapshot-handoff-active-xid ()
  ;; SnapBuildProcessRunningXacts and CreateInitDecodingContext establish a
  ;; consistent point while concurrent transactions remain in progress. This
  ;; test captures a row before that point, commits its XID afterward, and reads
  ;; it only from the shared snapshot/stream boundary.
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (database (make-database :name "postgres" :oid 7))
            (table
              (make-pg-class
               :oid 701 :name "logical_rows"
               :columns
               (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                       :attnum 1 :notnull t)
                       (make-pg-column :name "amount" :type :int8 :typmod -1
                                       :attnum 2 :notnull nil)
                       (make-pg-column :name "label" :type :varchar :typmod 64
                                       :attnum 3 :notnull nil))))
            (xid 9501)
            (*backend* (make-backend :xid xid))
            (*active-xids* (list xid))
            (*next-xid* (1+ xid))
            (*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (registration (make-snapshot-registration))
            (owner (gensym "SNAPSHOT-HANDOFF-"))
            (during-copy-xid (1+ xid))
            (boundary-lsn nil)
            (restart-lsn nil)
            (snapshot nil)
            (slot nil)
            (ack-lsn nil))
       (setf (backend-database *backend*) "postgres"
             (backend-state *backend*) :in-progress
             (gethash "postgres" (catalog-databases *catalog*)) database
             (gethash "logical_rows" (database-tables database)) table)
       (logical-note-row-change :insert table :new-values #(55 4 "across boundary"))
       (logical-test-commit-boundary-batch xid)
       (multiple-value-setq (snapshot boundary-lsn restart-lsn slot)
         (with-write-lock
           (wal-create-replication-slot-under-writer-lock
            "handoff_slot" 0 registration owner 7)))
       (check (member xid (snapshot-xip snapshot) :test #'=)
              "snapshot contains the active writer")
       (check (< restart-lsn boundary-lsn)
              (format nil "slot retains active writer WAL (restart=~a boundary=~a xid-first=~a)"
                      restart-lsn boundary-lsn (gethash xid *wal-xid-first-lsn*)))
       (check (null (wal-replication-slot-confirmed-flush-lsn slot))
              "new slot awaits initial-copy acknowledgment")
       (check (not (member during-copy-xid (snapshot-xip snapshot) :test #'=))
              "transaction started after the snapshot is outside the copy")
       (logical-test-commit-insert
        during-copy-xid table #(56 5 "committed during copy"))
       ;; A second statement in the original transaction remains attached to
       ;; its XID even though it runs after the snapshot boundary.
       (logical-note-row-change :insert table :new-values #(57 6 "second statement"))
       (release-snapshot-registration registration)
       (wal-finalize-replication-slot-copy "handoff_slot" boundary-lsn owner)
       (wal-release-replication-slot-owner "handoff_slot" owner)
       (commit-transaction)
       (check (and (null (backend-logical-changes *backend*))
                   (not (member xid *active-xids* :test #'=)))
              "real transaction commit publishes and retires its queued row change")
       (logical-test-abort-insert (1+ during-copy-xid) table)
       ;; Startup recovery validates complete v5 row records before the slot
       ;; reader resumes from its durable acknowledgment.
       (wal-recover)
       (call-with-logical-replication-slot
        "handoff_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (let* ((transactions
                   (wal-logical-slot-read-transactions "handoff_slot"))
                 (transaction (second transactions))
                 (change (first (logical-transaction-changes transaction))))
            (check (and (= (length transactions) 2)
                        (equal (mapcar #'logical-transaction-xid transactions)
                               (list during-copy-xid xid))
                        (> (logical-transaction-commit-lsn transaction)
                           boundary-lsn))
                   "copy-time and active-at-snapshot transactions stream in commit order")
            (check (and (eq (logical-change-kind change) :insert)
                        (equalp (logical-change-new-values change)
                                #(55 4 "across boundary")))
                   "executor-captured row reaches the reader through real commit WAL")
            (check (equalp (mapcar #'logical-change-new-values
                                   (logical-transaction-changes transaction))
                           '(#(55 4 "across boundary")
                             #(57 6 "second statement")))
                   "multi-statement row changes retain their transaction order")
            (setf ack-lsn
                  (logical-transaction-commit-lsn (second transactions)))
            (wal-logical-slot-acknowledge "handoff_slot" ack-lsn))))
       ;; Slot files persist subscriber progress across restart. The resumed
       ;; reader must start after the last durable acknowledgment.
       (wal-load-replication-slots)
       (setf slot (gethash "handoff_slot" *wal-replication-slots*))
       (check (and (= (wal-replication-slot-database-oid slot) 7)
                   (= (wal-replication-slot-confirmed-flush-lsn slot) ack-lsn))
              "restart reloads the database binding and logical acknowledgment")
       (call-with-logical-replication-slot
        "handoff_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (check (null (wal-logical-slot-read-transactions "handoff_slot"))
                 "restart does not replay acknowledged transactions")))))))

(defun test-logical-export-cleanup-releases-after-claim-error ()
  ;; ExportSnapshot() in src/backend/utils/time/snapmgr.c keeps an exported
  ;; snapshot alive until transaction cleanup. A failed importer finalizer
  ;; must likewise finish releasing both its MVCC registration and schema lease.
  (let* ((*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*schema-read-lock-count* 0)
         (snapshot (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
         (registration (make-snapshot-registration))
         (state (make-exported-logical-snapshot
                 :registration registration :schema-read-lock-held-p t
                 :expired-p t))
         (baseline-schema-readers nil)
         (caught nil)
         (claim-observed nil))
    (register-active-snapshot snapshot registration)
    ;; Keep an unrelated lease alive while cleanup releases the export's lease.
    (acquire-schema-read-lock (lambda () nil))
    (setf baseline-schema-readers *schema-read-lock-count*)
    (acquire-schema-read-lock (lambda () nil))
    (let ((*logical-test-after-export-cleanup-claim-hook*
            (lambda (claimed-state)
              (setf claim-observed
                    (and (exported-logical-snapshot-source-registration-released-p
                          claimed-state)
                         (exported-logical-snapshot-cleanup-complete-p
                          claimed-state)))
              (error "injected finalizer failure after cleanup claim"))))
      (handler-case
          (finish-expired-exported-logical-snapshot state)
        (error () (setf caught t))))
    (check (and caught claim-observed)
           "snapshot finalizer failure occurs after both cleanup claims")
    (check (and (not (snapshot-registration-active-p registration))
                (zerop *active-snapshots*))
           "finalizer cleanup releases the exported MVCC registration")
    (check (and (= *schema-read-lock-count* baseline-schema-readers)
                (not (exported-logical-snapshot-schema-read-lock-held-p state)))
           "finalizer cleanup releases the schema lease after registration failure")
    (check (and (exported-logical-snapshot-cleanup-complete-p state)
                (null (finish-expired-exported-logical-snapshot state)))
           "a retried snapshot finalizer is idempotent")))

(defun test-logical-export-cleanup-retries-wal-pin-release ()
  ;; AtEOXact_Snapshot() in snapmgr.c releases transaction-owned snapshots. If
  ;; WAL-pin removal fails after MVCC release, the exported finalizer must keep
  ;; its token discoverable so a later maintenance pass can finish cleanup.
  (let* ((*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*schema-read-lock-count* 0)
         (*exported-logical-snapshots* (make-hash-table :test #'equal))
         (*expired-exported-logical-snapshots* (make-hash-table :test #'equal))
         (token "wal-pin-retry")
         (snapshot (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
         (registration (make-snapshot-registration))
         (state (make-exported-logical-snapshot
                 :token token :registration registration
                 :schema-read-lock-held-p t :expired-p t))
         (injected-count 0)
         (caught nil))
    (register-active-snapshot snapshot registration)
    (acquire-schema-read-lock (lambda () nil))
    (setf (snapshot-registration-wal-retention-lsn registration) 42
          (gethash registration *wal-snapshot-retention-pins*) 42
          (gethash token *expired-exported-logical-snapshots*) state)
    (let ((*wal-test-after-snapshot-release-mark-hook*
            (lambda (ignored-registration)
              (declare (ignore ignored-registration))
              (when (< injected-count 2)
                (incf injected-count)
                (error "injected failure after snapshot release claim")))))
      (handler-case
          (cleanup-invalidated-logical-replication-state nil)
        (error () (setf caught t))))
    (check (and caught (= injected-count 2)
                (snapshot-registration-releasing-p registration)
                (= (snapshot-registration-wal-retention-lsn registration) 42)
                (= (gethash registration *wal-snapshot-retention-pins*) 42)
                (eq state (gethash token *expired-exported-logical-snapshots*)))
           "failed finalization stays published for a later cleanup pass")
    (cleanup-invalidated-logical-replication-state nil)
    (check (and (null (gethash registration *wal-snapshot-retention-pins*))
                (null (snapshot-registration-wal-retention-lsn registration))
                (not (snapshot-registration-releasing-p registration)))
           "pending-registry retry removes the provisional WAL pin")
    (check (and (zerop *active-snapshots*)
                (zerop *schema-read-lock-count*)
                (exported-logical-snapshot-cleanup-complete-p state)
                (null (gethash token *expired-exported-logical-snapshots*)))
           "pending-registry retry completes MVCC and schema cleanup exactly once")))

(defun test-unpinned-snapshot-release-ignores-stale-reuse-pin ()
  ;; A retry may finish an unpinned release while its original caller is
  ;; paused, after which the registration can be reused and pinned again.
  (let* ((baseline-active-snapshots *active-snapshots*)
         (registration (make-snapshot-registration))
         (old-snapshot
           (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
         (new-snapshot
           (make-snapshot-struct :xmin 20 :xmax 30 :xip '() :curcid 1))
         (gate-lock (sb-thread:make-mutex :name "stale snapshot release test"))
         (gate-cv (sb-thread:make-waitqueue :name "stale snapshot release test"))
         (release-paused nil)
         (continue-release nil)
         (stale-release-error nil)
         (release-thread nil)
         (new-pin 42))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*xid-lock*)
             (register-active-snapshot old-snapshot registration))
           (setf release-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*wal-test-after-snapshot-release-mark-hook*
                            (lambda (ignored-registration)
                              (declare (ignore ignored-registration))
                              (sb-thread:with-mutex (gate-lock)
                                (setf release-paused t)
                                (sb-thread:condition-broadcast gate-cv)
                                (loop until continue-release
                                      do (sb-thread:condition-wait
                                          gate-cv gate-lock))))))
                      (handler-case
                          (release-snapshot-registration registration)
                        (error (condition)
                          (setf stale-release-error condition)))))))
           (sb-thread:with-mutex (gate-lock)
             (loop until release-paused
                   do (sb-thread:condition-wait gate-cv gate-lock)))
           ;; A concurrent retry finishes the same unpinned generation.
           (release-snapshot-registration registration)
           ;; Reuse the token and publish a pin as boundary capture does.
           (sb-thread:with-mutex (*xid-lock*)
             (register-active-snapshot new-snapshot registration))
           (sb-thread:with-mutex (*wal-lock*)
             (sb-thread:with-mutex (*snapshot-count-lock*)
               (setf (gethash registration *wal-snapshot-retention-pins*) new-pin
                     (snapshot-registration-wal-retention-lsn registration)
                     new-pin)))
           (sb-thread:with-mutex (gate-lock)
             (setf continue-release t)
             (sb-thread:condition-broadcast gate-cv))
           (sb-thread:join-thread release-thread)
           (check (null stale-release-error)
                  "stale unpinned release ignores a reused generation's WAL pin")
           (check (snapshot-registration-active-p registration)
                  "stale release preserves the reused registration")
           (check (not (snapshot-registration-releasing-p registration))
                  "stale release preserves the new release state")
           (check (eql (snapshot-registration-wal-retention-lsn registration)
                       new-pin)
                  "stale release preserves the new registration's pin LSN")
           (check (= *active-snapshots* (1+ baseline-active-snapshots))
                  "stale release preserves the new active snapshot count")
           (check (eq new-pin
                      (sb-thread:with-mutex (*wal-lock*)
                        (gethash registration *wal-snapshot-retention-pins*)))
                  "stale release preserves the new WAL retention map entry"))
      (when release-thread
        (sb-thread:with-mutex (gate-lock)
          (setf continue-release t)
          (sb-thread:condition-broadcast gate-cv))
        (sb-thread:join-thread release-thread))
      (when (or (snapshot-registration-active-p registration)
                (snapshot-registration-releasing-p registration))
        (release-snapshot-registration registration))
      (sb-thread:with-mutex (*wal-lock*)
        (remhash registration *wal-snapshot-retention-pins*))
      (check (= *active-snapshots* baseline-active-snapshots)
             "stale-release regression restores the active snapshot count"))))

(defun test-logical-snapshot-import-locks-token-through-pin ()
  (let* ((catalog (make-catalog))
         (database (make-database :name "postgres" :oid 7))
         (token (new-exported-logical-snapshot-token))
         (source-snapshot
           (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
         (source-registration (make-snapshot-registration))
         (state (make-exported-logical-snapshot
                 :slot-name "snapshot_race" :token token :database-oid 7
                 :snapshot source-snapshot :registration source-registration))
         (import-backend
           (make-backend :database "postgres" :state :in-progress :explicit t
                         :isolation-level :repeatable-read :read-only t))
         (gate-lock (sb-thread:make-mutex :name "logical snapshot import test"))
         (gate-cv (sb-thread:make-waitqueue :name "logical snapshot import test"))
         (lookup-seen nil)
         (finalizer-started nil)
         (finalizer-done nil)
         (allow-import nil)
         (import-error nil)
         (finalizer-error nil)
         (importer nil)
         (finalizer nil)
         (baseline-snapshots *active-snapshots*))
    (setf (gethash "postgres" (catalog-databases catalog)) database)
    (sb-thread:with-mutex (*xid-lock*)
      (register-active-snapshot source-snapshot source-registration))
    (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
      (setf (gethash token *exported-logical-snapshots*) state))
    (unwind-protect
         (progn
           (setf importer
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*catalog* catalog)
                          (*backend* import-backend)
                          (*logical-test-after-snapshot-token-lookup-hook*
                            (lambda (ignored-state)
                              (declare (ignore ignored-state))
                              (sb-thread:with-mutex (gate-lock)
                                (setf lookup-seen t)
                                (sb-thread:condition-broadcast gate-cv)
                                (loop until allow-import
                                      do (sb-thread:condition-wait
                                          gate-cv gate-lock))))))
                      (handler-case
                          (import-exported-logical-snapshot token)
                        (error (condition)
                          (setf import-error condition)))))))
           (sb-thread:with-mutex (gate-lock)
             (loop until lookup-seen
                   do (sb-thread:condition-wait gate-cv gate-lock)))
           (setf finalizer
                 (sb-thread:make-thread
                  (lambda ()
                    (sb-thread:with-mutex (gate-lock)
                      (setf finalizer-started t)
                      (sb-thread:condition-broadcast gate-cv))
                    (handler-case
                        (remove-exported-logical-snapshot-token token)
                      (error (condition)
                        (setf finalizer-error condition)))
                    (sb-thread:with-mutex (gate-lock)
                      (setf finalizer-done t)
                      (sb-thread:condition-broadcast gate-cv)))))
           (sb-thread:with-mutex (gate-lock)
             (loop until finalizer-started
                   do (sb-thread:condition-wait gate-cv gate-lock)))
           (sleep 0.02)
           (sb-thread:with-mutex (gate-lock)
             (check (not finalizer-done)
                    "snapshot finalizer waits while import publishes its pin")
             (setf allow-import t)
             (sb-thread:condition-broadcast gate-cv))
           (sb-thread:join-thread importer)
           (sb-thread:join-thread finalizer)
           (check (null import-error) "snapshot import completes during finalization")
           (check (null finalizer-error) "snapshot finalization completes after import")
           (check (null (gethash token *exported-logical-snapshots*))
                  "copy completion removes the exact imported snapshot token")
           (check (snapshot-registration-active-p
                   (backend-transaction-snapshot-registration import-backend))
                  "imported transaction retains an independent snapshot pin")
           (check (= *active-snapshots* (1+ baseline-snapshots))
                  "finalization releases only the exported snapshot registration"))
      (sb-thread:with-mutex (gate-lock)
        (setf allow-import t)
        (sb-thread:condition-broadcast gate-cv))
      (when (and importer (sb-thread:thread-alive-p importer))
        (sb-thread:join-thread importer))
      (when (and finalizer (sb-thread:thread-alive-p finalizer))
        (sb-thread:join-thread finalizer))
      (when (gethash token *exported-logical-snapshots*)
        (ignore-errors (remove-exported-logical-snapshot-token token)))
      (when (snapshot-registration-active-p source-registration)
        (release-snapshot-registration source-registration))
      (let ((import-registration
              (backend-transaction-snapshot-registration import-backend)))
        (when (and import-registration
                   (snapshot-registration-active-p import-registration))
          (release-snapshot-registration import-registration))))))

(defun test-logical-slot-owner-publication-failure-cleans-owner ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*backend* nil)
            (slot (make-wal-replication-slot
                   :name "owner-fault" :database-oid 7
                   :restart-lsn 0 :consistent-lsn 0
                   :confirmed-flush-lsn 0 :wal-status :reserved))
            (callback-ran nil)
            (*logical-test-failpoint-hook*
              (lambda (point &rest details)
                (declare (ignore details))
                (when (eq point :after-logical-slot-owner-publication)
                  (error "injected failure after slot owner publication")))))
       (setf (gethash "owner-fault" *wal-replication-slots*) slot)
       (check (handler-case
                  (progn
                    (call-with-logical-replication-slot
                     "owner-fault"
                     (lambda (owned-slot)
                       (declare (ignore owned-slot))
                       (setf callback-ran t)))
                    nil)
                (error () t))
              "owner publication failpoint interrupts the slot callback")
       (check (not callback-ran)
              "slot callback does not run after owner publication failure")
       (check (and (null (wal-replication-slot-active-owner slot))
                   (null (wal-replication-slot-active-owner-backend slot)))
              "owner publication failure releases its exact runtime claim")))))

(defun test-logical-lost-slot-cleans-importers-by-generation ()
  ;; slot.c invalidates durable retention before terminating consumers. Cleanup
  ;; must match the old slot object so a recreated same-name slot keeps its token.
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*exported-logical-snapshots* (make-hash-table :test #'equal))
            (*expired-exported-logical-snapshots*
              (make-hash-table :test #'equal))
            (baseline-schema-readers *schema-read-lock-count*)
            (old-slot (make-wal-replication-slot
                       :name "snapshot_race" :restart-lsn 1
                       :consistent-lsn 2 :database-oid 7 :wal-status :lost))
            (new-slot (make-wal-replication-slot
                       :name "snapshot_race" :restart-lsn 30
                       :consistent-lsn 32 :database-oid 7 :wal-status :reserved))
            (old-token "old-generation-token")
            (new-token "new-generation-token")
            (source-snapshot
              (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
            (source-registration (make-snapshot-registration))
            (import-snapshot
              (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
            (import-registration (make-snapshot-registration))
            (import-backend (make-backend :database "postgres"))
            (old-state
              (make-exported-logical-snapshot
               :slot-name "snapshot_race" :slot-object old-slot :token old-token
               :database-oid 7 :snapshot source-snapshot
               :registration source-registration))
            (new-state
              (make-exported-logical-snapshot
               :slot-name "snapshot_race" :slot-object new-slot :token new-token
               :database-oid 7))
            (terminated-backends nil)
            (*logical-test-invalidated-backend-hook*
              (lambda (backend)
                (push backend terminated-backends)
                (check (not (eq sb-thread:*current-thread*
                                (sb-thread:mutex-owner
                                 *exported-logical-snapshot-importer-lock*)))
                       "importer termination runs outside the importer mutex")
                (check (null (gethash old-token *exported-logical-snapshots*))
                       "invalidation removes the token before stopping its importer")
                (release-snapshot-registration import-registration))))
       (setf (gethash "snapshot_race" *wal-replication-slots*) new-slot)
       (sb-thread:with-mutex (*xid-lock*)
         (register-active-snapshot source-snapshot source-registration)
         (register-active-snapshot import-snapshot import-registration))
       (acquire-schema-read-lock
        (lambda ()
          (setf (exported-logical-snapshot-schema-read-lock-held-p old-state) t)))
       (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
         (setf (gethash old-token *exported-logical-snapshots*) old-state
               (gethash new-token *exported-logical-snapshots*) new-state))
       (register-exported-logical-snapshot-importer
        old-state import-registration import-backend)
       (cleanup-invalidated-logical-replication-state (list old-slot))
       (check (equal terminated-backends (list import-backend))
              "lost slot cleanup terminates each tracked importer")
       (check (eq new-state (gethash new-token *exported-logical-snapshots*))
              "old cleanup leaves a recreated slot generation untouched")
       (check (and (not (snapshot-registration-active-p source-registration))
                   (not (snapshot-registration-active-p import-registration)))
              "source and imported MVCC pins drain after consumer termination")
       (check (= *schema-read-lock-count* baseline-schema-readers)
              "lost slot cleanup releases the exported schema lease")))))

(defun test-logical-drop-adopts-snapshot-before-cleanup ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*exported-logical-snapshots* (make-hash-table :test #'equal))
            (*expired-exported-logical-snapshots*
              (make-hash-table :test #'equal))
            (slot (make-wal-replication-slot
                   :name "drop_export" :restart-lsn 0 :consistent-lsn 0
                   :database-oid 7 :wal-status :reserved))
            (token "drop-export-token")
            (state (make-exported-logical-snapshot
                    :slot-name "drop_export" :slot-object slot :token token
                    :database-oid 7))
            (caught nil))
       (wal-save-replication-slot slot)
       (setf (gethash "drop_export" *wal-replication-slots*) slot
             (gethash token *exported-logical-snapshots*) state)
       (let ((*logical-test-failpoint-hook*
               (lambda (point &rest details)
                 (declare (ignore details))
                 (when (eq point :after-logical-slot-drop-detach)
                   (error "injected failure after slot and token ownership transfer")))))
         (handler-case
             (wal-drop-replication-slot "drop_export")
           (error () (setf caught t))))
       (check (and caught
                   (null (gethash "drop_export" *wal-replication-slots*))
                   (null (gethash token *exported-logical-snapshots*))
                   (eq state (gethash token *expired-exported-logical-snapshots*)))
              "interrupted DROP keeps the exact snapshot in pending cleanup")
       (cleanup-invalidated-logical-replication-state nil)
       (check (and (null (gethash token *expired-exported-logical-snapshots*))
                   (exported-logical-snapshot-cleanup-complete-p state))
              "a later cleanup pass finishes a dropped slot's snapshot release")))))

(defun test-logical-maintenance-rediscovers-lost-slot-owners ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*logical-slot-owner-termination-requests*
              (make-hash-table :test #'eq))
            (owner (make-backend))
            (slot (make-wal-replication-slot
                   :name "rescan_lost" :restart-lsn 0 :consistent-lsn 0
                   :database-oid 7 :wal-status :lost
                   :active-owner-backend owner))
            (requests nil)
            (*logical-test-invalidated-backend-hook*
              (lambda (backend) (push backend requests))))
       (setf (gethash "rescan_lost" *wal-replication-slots*) slot)
       ;; This models a prior maintenance thread exiting before it could carry
       ;; its local loss collector into cleanup. The next pass must rediscover
       ;; the already durable :lost slot from runtime state.
       (call-with-invalidated-slot-cleanup #'values)
       (check (equal requests (list owner))
              "maintenance rediscovers an owner of an already-lost slot")
       (call-with-invalidated-slot-cleanup #'values)
       (check (equal requests (list owner))
              "maintenance retry does not re-signal an owner already unwinding")))))

(defun test-logical-owner-termination-revalidates-captured-slot ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((slot-table *wal-replication-slots*)
            (termination-requests (make-hash-table :test #'eq))
            (owner (make-backend))
            (owner-token (gensym "OWNER-"))
            (slot (make-wal-replication-slot
                   :name "owner-race" :restart-lsn 0 :consistent-lsn 0
                   :database-oid 7 :wal-status :lost
                   :active-owner owner-token
                   :active-owner-backend owner))
            (gate-lock (sb-thread:make-mutex :name "owner request revalidation"))
            (gate-cv (sb-thread:make-waitqueue :name "owner request revalidation"))
            (captured-pair nil)
            (resume-cleanup nil)
            (request-count 0)
            (cleanup-error nil)
            (cleanup-thread nil)
            (backend-hook
              (lambda (backend)
                (when (eq backend owner)
                  (incf request-count))))
            (*wal-replication-slots* slot-table)
            (*logical-slot-owner-termination-requests* termination-requests)
            (*logical-test-invalidated-backend-hook* backend-hook))
       (setf (gethash "owner-race" *wal-replication-slots*) slot
             cleanup-thread
             (sb-thread:make-thread
              (lambda ()
                ;; SBCL threads do not inherit these dynamic registries. Keep
                ;; the stale pair while the first cleanup releases its owner.
                (let ((*wal-replication-slots* slot-table)
                      (*logical-slot-owner-termination-requests*
                        termination-requests)
                      (*logical-test-invalidated-backend-hook* backend-hook))
                  (let ((captured-slot slot)
                        (captured-backend
                          (wal-replication-slot-active-owner-backend slot)))
                    (sb-thread:with-mutex (gate-lock)
                      (setf captured-pair (cons captured-slot captured-backend))
                      (sb-thread:condition-broadcast gate-cv)
                      (loop until resume-cleanup
                            do (sb-thread:condition-wait gate-cv gate-lock)))
                    (handler-case
                        (request-lost-slot-owner-termination
                         captured-slot captured-backend)
                      (error (condition)
                        (setf cleanup-error condition))))))))
       (unwind-protect
            (progn
              (check (test-wait-until
                      (lambda ()
                        (sb-thread:with-mutex (gate-lock) captured-pair))
                      2.0)
                     (format nil
                             "second cleanup captures the old owner/backend pair (captured=~s alive=~s error=~s)"
                             captured-pair
                             (and cleanup-thread
                                  (sb-thread:thread-alive-p cleanup-thread))
                             cleanup-error))
              (check (and (eq (car captured-pair) slot)
                          (eq (cdr captured-pair) owner))
                     "captured cleanup pair names the current slot owner")
              (check (request-lost-slot-owner-termination slot owner)
                     "first cleanup sends one termination to the current owner")
              (wal-release-replication-slot-owner "owner-race" owner-token)
              (sb-thread:with-mutex (gate-lock)
                (setf resume-cleanup t)
                (sb-thread:condition-broadcast gate-cv))
              (sb-thread:join-thread cleanup-thread :timeout 2)
              (check (null cleanup-error)
                     "retry completes after the slot owner releases")
              (check (= request-count 1)
                     "stale cleanup pair cannot signal an owner after release"))
         (sb-thread:with-mutex (gate-lock)
           (setf resume-cleanup t)
           (sb-thread:condition-broadcast gate-cv))
         (when (and cleanup-thread (sb-thread:thread-alive-p cleanup-thread))
           (sb-thread:join-thread cleanup-thread :timeout 2)))))))

(defun test-wal-pgslot3-database-field-validation ()
  (let* ((test-directory
           (merge-pathnames (format nil "logical-slot-file-~a/" (random 100000000))
                            *pgdata*))
         (valid-path (merge-pathnames "valid_slot.slot" test-directory))
         (invalid-path (merge-pathnames "invalid_slot.slot" test-directory))
         (unbound (make-wal-replication-slot
                   :name "valid_slot" :restart-lsn 0 :consistent-lsn 0
                   :wal-status :reserved))
         (invalid-prefix
           (format nil "PGSLOT3 invalid_slot not-an-oid 0 0 - reserved")))
    (wal-ensure-directory-durable test-directory)
    (unwind-protect
         (progn
           (with-open-file (stream valid-path :direction :output
                                              :element-type '(unsigned-byte 8)
                                              :if-exists :supersede
                                              :if-does-not-exist :create)
             (write-sequence (wal-encode-replication-slot unbound) stream))
           (check (null (wal-replication-slot-database-oid
                         (wal-read-replication-slot valid-path)))
                  "PGSLOT3 preserves a valid legacy database-unbound slot")
           (with-open-file (stream invalid-path :direction :output
                                                :element-type '(unsigned-byte 8)
                                                :if-exists :supersede
                                                :if-does-not-exist :create)
             (write-sequence
              (string->bytes
               (format nil "~a ~8,'0X~%" invalid-prefix
                       (wal-crc32 (string->bytes invalid-prefix))))
              stream))
           (check (handler-case
                      (progn (wal-read-replication-slot invalid-path) nil)
                    (error () t))
                  "PGSLOT3 rejects malformed database OIDs with a valid CRC"))
      (ignore-errors
       (uiop:delete-directory-tree test-directory :validate t
                                                  :if-does-not-exist :ignore)))))
