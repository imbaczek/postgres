;;;; Local restartpoint generations and durable publication, independent of source pins.
(in-package #:pg)

(declaim (special *physical-follow-command-lock*))

(defvar *physical-restartpoint-test-hook* nil)

(defvar *physical-restartpoint-timeout* 300
  "Seconds between native standby restartpoints when replay has advanced.")

(defvar *physical-restartpoint-wal-bytes* (* 1024 1024 1024)
  "Applied native WAL distance that requests a standby restartpoint.")

(defvar *physical-restartpoint-last-time* 0)

(defvar *physical-restartpoint-worker* nil)

(defvar *physical-restartpoint-start-lock* (sb-thread:make-mutex :name "standby restartpoint worker start"))

(defun physical-restartpoint-test-point (point)
  (when *physical-restartpoint-test-hook*
    (funcall *physical-restartpoint-test-hook* point)))

(defun physical-restartpoint-directory ()
  (merge-pathnames "pg_physical_restartpoints/" *pgdata*))

(defun physical-restartpoint-pointer-path ()
  (merge-pathnames "physical-restartpoint.control" *pgdata*))

(defun physical-restartpoint-generation-path (lsn digest)
  (merge-pathnames (format nil "~16,'0X-~16,'0X.rp" lsn digest)
                   (physical-restartpoint-directory)))

(defun physical-restartpoint-encode (metadata catalog-bytes)
  (let* ((text (string->bytes (physical-bootstrap-manifest-text metadata)))
         (length (+ 32 (length text) (length catalog-bytes))))
    (unless (and (<= 1 (length text) (* 16 1024 1024))
                 (plusp (length catalog-bytes)) (<= length (* 1024 1024 1024)))
      (error "physical restartpoint exceeds its readable generation bounds"))
    (let ((bytes (make-array length :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace bytes #(80 71 82 80 71 69 78 49))
      (wal-set-u32 bytes 8 (length text))
      (wal-set-u64 bytes 12 (length catalog-bytes))
      (replace bytes text :start1 32)
      (replace bytes catalog-bytes :start1 (+ 32 (length text)))
      (wal-set-u32 bytes 24 (wal-crc32 bytes))
      bytes)))

(defun physical-restartpoint-decode (bytes)
  (unless (>= (length bytes) 32) (error "short physical restartpoint generation"))
  (let ((crc (wal-u32 bytes 24))
        (text-length (wal-u32 bytes 8))
        (catalog-length (wal-u64 bytes 12)))
    (setf bytes (copy-seq bytes))
    (wal-set-u32 bytes 24 0)
    (unless (and (equalp (subseq bytes 0 8) #(80 71 82 80 71 69 78 49))
                 (zerop (wal-u32 bytes 20)) (zerop (wal-u32 bytes 28))
                 (= crc (wal-crc32 bytes))
                 (<= 1 text-length (* 16 1024 1024))
                 (plusp catalog-length)
                 (= (length bytes) (+ 32 text-length catalog-length)))
      (error "invalid physical restartpoint generation"))
    (values (recovery-parse-manifest-text (bytes->string bytes :start 32 :end (+ 32 text-length)))
            (subseq bytes (+ 32 text-length)))))

(defun physical-restartpoint-check-metadata (metadata catalog-bytes state)
  (physical-history-check-plist
   metadata (append '(:version :system-id :cluster-identity :timeline :manifest-digest
                      :base :lsn :prefix-digest :next-xid :forks :seen)
                    (when (eql 2 (getf metadata :version)) '(:source-checkpoint))))
  (unless (and (if (eql 2 (getf metadata :version)) (= (length metadata) 24)
                   (and (eql 1 (getf metadata :version)) (= (length metadata) 22)))
               (eql (getf metadata :system-id) (getf state :system-id))
               (equal (getf metadata :cluster-identity) (getf state :cluster-identity))
               (eql (getf metadata :timeline) (getf state :timeline))
               (eql (getf metadata :manifest-digest) (getf state :manifest-digest))
               (eql (getf metadata :base) (getf state :end))
               (every (lambda (key) (typep (getf metadata key) '(unsigned-byte 64)))
                      '(:lsn :prefix-digest :next-xid))
               (<= (getf state :end) (getf metadata :lsn))
               (<= 3 (getf metadata :next-xid)))
    (error "foreign or malformed physical restartpoint metadata"))
  (let ((snapshot (getf metadata :source-checkpoint)))
    (when snapshot
      (unless (and (wal-checkpoint-snapshot-valid-p snapshot)
                   (<= (getf state :end) (getf snapshot :end) (getf metadata :lsn)))
        (error "restartpoint source checkpoint lies outside its applied history"))))
  (let ((names (make-hash-table :test #'equal))
        (seen (make-hash-table :test #'eql))
        (live (physical-history-catalog-relfilenodes catalog-bytes (getf state :cluster-identity))))
    (dolist (node (getf metadata :seen))
      (unless (and (typep node '(integer 1 4294967295)) (not (gethash node seen)))
        (error "invalid restartpoint relation history"))
      (setf (gethash node seen) t))
    (dolist (entry (getf metadata :forks))
      (unless (and (listp entry) (= (length entry) 2)
                   (recovery-manifest-path-p (first entry))
                   (uiop:string-prefix-p "base/" (first entry))
                   (typep (second entry) '(unsigned-byte 64))
                   (not (gethash (first entry) names))
                   (gethash (parse-integer (first entry) :start 5 :junk-allowed t) live))
        (error "invalid restartpoint fork inventory"))
      (setf (gethash (first entry) names) t))
    (maphash (lambda (node present)
               (declare (ignore present))
               (unless (and (gethash node seen) (gethash (format nil "base/~d" node) names))
                 (error "restartpoint omits a live relation fork or its history")))
             live))
  metadata)

(defun physical-restartpoint-fork-inventory (catalog-bytes)
  (let ((live (physical-history-catalog-relfilenodes
               catalog-bytes (getf *physical-recovery-state* :cluster-identity)))
        (forks nil))
    (maphash
     (lambda (node present)
       (declare (ignore present))
       (dolist (name (list (format nil "base/~d" node) (format nil "base/~d.vm" node)))
         (let ((path (merge-pathnames name *pgdata*)))
           (when (or (probe-file path) (not (uiop:string-suffix-p name ".vm")))
             (with-open-file (stream path :element-type '(unsigned-byte 8))
               (push (list name (file-length stream)) forks))))))
     live)
    (sort forks #'string< :key #'first)))

(defun physical-restartpoint-metadata (digest catalog-bytes)
  (let* ((state *physical-recovery-state*)
         (forks (physical-restartpoint-fork-inventory catalog-bytes))
         (seen (loop for node being the hash-keys of *physical-replay-seen-relfilenodes* collect node)))
    (dolist (entry forks)
      (pushnew (parse-integer (first entry) :start 5 :junk-allowed t) seen))
    (list :version 2 :system-id (getf state :system-id)
          :cluster-identity (getf state :cluster-identity) :timeline (getf state :timeline)
          :manifest-digest (getf state :manifest-digest) :base (getf state :end)
          :lsn *last-replay-lsn* :prefix-digest digest :next-xid *next-xid*
          :forks forks :seen (sort seen #'<)
          :source-checkpoint (copy-list (getf state :source-checkpoint)))))

(defun physical-restartpoint-publish (digest)
  "Called at a complete applied terminal with replay/admission and store locks held."
  (smgr-fsync-all)
  (clog-fsync)
  (with-open-file (stream (catalog-path) :element-type '(unsigned-byte 8)) (fsync-stream stream))
  (fsync-directory (merge-pathnames "base/" *pgdata*))
  (fsync-directory (merge-pathnames "global/" *pgdata*))
  (let* ((catalog-bytes (recovery-read-bytes (catalog-path) :maximum (* 1024 1024 1024)))
         (metadata (physical-restartpoint-metadata digest catalog-bytes))
         (bytes (physical-restartpoint-encode metadata catalog-bytes))
         (hash (physical-store-update-digest #xCBF29CE484222325 bytes))
         (path (physical-restartpoint-generation-path *last-replay-lsn* hash))
         (pointer (make-array 28 :element-type '(unsigned-byte 8) :initial-element 0)))
    (physical-restartpoint-check-metadata metadata catalog-bytes *physical-recovery-state*)
    (wal-ensure-directory-durable (physical-restartpoint-directory))
    ;; Never overwrite a generation named by a pointer, including retry orphans.
    (if (probe-file path)
        (unless (equalp bytes (recovery-read-bytes path :maximum (* 1024 1024 1024)))
          (error "restartpoint generation name collision"))
        (wal-atomic-write-bytes path bytes))
    (physical-restartpoint-test-point :after-generation)
    (replace pointer #(80 71 82 80 80 84 82 49))
    (wal-set-u64 pointer 8 *last-replay-lsn*)
    (wal-set-u64 pointer 16 hash)
    (wal-set-u32 pointer 24 (wal-crc32 pointer))
    (wal-atomic-write-bytes (physical-restartpoint-pointer-path) pointer)
    (physical-restartpoint-test-point :after-pointer)
    (setf (getf *physical-recovery-state* :restartpoint-lsn) *last-replay-lsn*)
    path))

(defun physical-restartpoint-read (state)
  "The durable pointer is authoritative; corruption never selects an older image."
  (unless (probe-file (physical-restartpoint-pointer-path))
    (return-from physical-restartpoint-read (values nil nil)))
  (let ((pointer (recovery-read-bytes (physical-restartpoint-pointer-path) :maximum 28)))
    (unless (= (length pointer) 28) (error "short restartpoint pointer"))
    (let ((crc (wal-u32 pointer 24)))
      (wal-set-u32 pointer 24 0)
      (unless (and (equalp (subseq pointer 0 8) #(80 71 82 80 80 84 82 49))
                   (= crc (wal-crc32 pointer)))
        (error "corrupt restartpoint pointer")))
    (let* ((lsn (wal-u64 pointer 8))
           (digest (wal-u64 pointer 16))
           (bytes (recovery-read-bytes (physical-restartpoint-generation-path lsn digest)
                                       :maximum (* 1024 1024 1024))))
      (unless (= digest (physical-store-update-digest #xCBF29CE484222325 bytes))
        (error "restartpoint generation checksum mismatch"))
      (multiple-value-bind (metadata catalog-bytes) (physical-restartpoint-decode bytes)
        (physical-restartpoint-check-metadata metadata catalog-bytes state)
        (unless (= lsn (getf metadata :lsn)) (error "restartpoint pointer boundary mismatch"))
        (values metadata catalog-bytes)))))

(defun physical-restartpoint-prepare-storage (store)
  "Validate provenance, suffix and surviving forks before restoring an R catalog."
  (let* ((state *physical-recovery-state*)
         (identity (getf state :cluster-identity))
         (required (getf state :required-applied)))
    (multiple-value-bind (metadata catalog-bytes) (physical-restartpoint-read state)
      (let ((start (if metadata (getf metadata :lsn) (getf state :end))))
        (unless (and (<= (physical-store-floor-lsn store) start required
                         (physical-store-flushed-lsn store))
                     (or metadata (= start (physical-store-base-lsn store))))
          (error "receive floor lacks its required published restartpoint"))
        (when metadata
          (unless (and (= (getf metadata :prefix-digest) (physical-store-prefix-digest store start))
                       (<= (getf metadata :next-xid) (getf state :observed-next-xid)))
            (error "restartpoint differs from its durable receive prefix")))
        (let ((removed (physical-recovery-proven-drops
                        store identity :start-lsn start :required-lsn required)))
          (physical-recovery-check-base-forks
           (if metadata (list :files (getf metadata :forks)) state) removed))
        ;; Every check above precedes mutation. Later failure still leaves the
        ;; pointer and suffix sufficient for repeating this restoration.
        (if metadata
            (progn
              (wal-atomic-write-bytes (catalog-path) catalog-bytes)
              (setf *catalog* (load-catalog)
                    *physical-replay-seen-relfilenodes* (make-hash-table :test #'eql))
              (dolist (node (getf metadata :seen))
                (setf (gethash node *physical-replay-seen-relfilenodes*) t))
              (setf *next-xid* (getf metadata :next-xid)
                    *xid-batch-end* *next-xid*
                    (getf state :restartpoint-lsn) start
                    (getf *physical-recovery-state* :source-checkpoint)
                    (copy-list (getf metadata :source-checkpoint))))
            (physical-recovery-reset-replay-history))
        (setf *last-replay-lsn* start *wal-checkpoint-lsn* start)
        start))))

(defun physical-restartpoint-remove-old-generations ()
  "Called only after pointer, floor and spool deletion have been synced."
  (let* ((pointer (recovery-read-bytes (physical-restartpoint-pointer-path) :maximum 28))
         (keep (physical-restartpoint-generation-path (wal-u64 pointer 8) (wal-u64 pointer 16))))
    (dolist (path (directory (merge-pathnames "*.rp" (physical-restartpoint-directory))))
      (unless (equal (pathname-name path) (pathname-name keep)) (delete-file path)))
    (fsync-directory (physical-restartpoint-directory))))

(defun physical-restartpoint-due-p (applied floor now last-time timeout wal-bytes)
  "Schedule only applied progress; a receive backlog cannot become a restartpoint."
  (and (> applied floor)
       (or (>= (- applied floor) wal-bytes)
           (>= (- now last-time) (* timeout internal-time-units-per-second)))))

(defun physical-restartpoint-needed-p (&optional (now (get-internal-real-time)))
  "Call with admission or quiescence; inspect floor and apply under the store lock."
  (when (and *physical-receive-store* (not *physical-recovery-failed-p*))
    (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
      (physical-store-check-live *physical-receive-store*)
      (physical-restartpoint-due-p
       *last-replay-lsn* (physical-store-floor-lsn *physical-receive-store*)
       now *physical-restartpoint-last-time*
       *physical-restartpoint-timeout* *physical-restartpoint-wal-bytes*))))

(defun physical-recovery-restartpoint-core ()
  "Publish while admission pins replay, or while this thread owns quiescence.
Never acquire the reload mutex here: a replayer may own it while draining SQL."
  (unless (or (plusp *live-operation-depth*)
              (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
                   (not *live-operation-drain-open*)))
    (error "restartpoint publication requires live admission or owned quiescence"))
  (check-physical-recovery-work)
  (unless (and (recovery-in-progress-p) *physical-receive-store*)
    (error "restartpoint requires streaming recovery"))
  (when *swank-listener-enabled-p* (error "restartpoint requires Swank to be off"))
  (let ((finished nil))
    (unwind-protect
         (let ((lsn (physical-store-compact *physical-receive-store* *last-replay-lsn*
                                            #'physical-restartpoint-publish)))
           (physical-restartpoint-remove-old-generations)
           (setf *physical-restartpoint-last-time* (get-internal-real-time)
                 finished t)
           lsn)
      (unless finished (setf *physical-recovery-failed-p* t)))))

(defun physical-recovery-checkpoint ()
  "CHECKPOINT in recovery is a restartpoint request, including within read transactions."
  (check-physical-recovery-work)
  (if *physical-receive-store*
      (physical-recovery-restartpoint-core)
      ;; An immutable base has no newer replay boundary to checkpoint.
      :unchanged))

(defun physical-recovery-restartpoint (&key automatic)
  "Publish a local durable terminal and compact its receive spool; never promote."
  (check-physical-recovery-work)
  (unless (and (recovery-in-progress-p) *physical-receive-store*)
    (error "restartpoint requires streaming recovery"))
  (when (plusp *live-operation-depth*) (error "restartpoint requires its own admission gate"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (check-physical-recovery-work)
      (when (and automatic (not (physical-restartpoint-needed-p)))
        (return-from physical-recovery-restartpoint :not-due))
      (when (physical-recovery-reader-conflict-p)
        (return-from physical-recovery-restartpoint :deferred))
      (physical-recovery-restartpoint-core))))

(defun physical-restartpoint-worker-step ()
  "Keep the running worker's iteration replaceable by a live reload."
  ;; A previous loop frame may survive reload. Exit only this registered
  ;; worker, synchronously before entering admission or doing storage work.
  (when (and *physical-shutdown-state*
             (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
               (eq sb-thread:*current-thread* *physical-restartpoint-worker*)))
    (sb-thread:return-from-thread :stopped))
  (handler-case
      (when (with-live-operation (physical-restartpoint-needed-p))
        (physical-recovery-restartpoint :automatic t))
    (error (condition)
      (setf *physical-recovery-failed-p* t)
      (admin-log "native standby restartpoint failed: ~a" condition)
      :failed)))

(defun physical-restartpoint-worker-loop ()
  (loop until (or *physical-shutdown-state* (shutdown-in-progress-p) *physical-recovery-failed-p*)
        for result = (physical-restartpoint-worker-step)
        do (sleep (if (eq result :deferred) 1 0.2))))

(defun physical-restartpoint-start-worker ()
  "Start one local maintenance worker for streaming recovery, independent of following."
  (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
    (check-physical-recovery-worker-start)
    (when (and (recovery-in-progress-p) *physical-receive-store*
               (not (and *physical-restartpoint-worker*
                         (sb-thread:thread-alive-p *physical-restartpoint-worker*))))
      (setf *physical-restartpoint-last-time* (get-internal-real-time)
            *physical-restartpoint-worker*
            (sb-thread:make-thread #'physical-restartpoint-worker-loop
                                   :name "native standby restartpoints")))
    *physical-restartpoint-worker*))

(defun physical-restartpoint-join-for-shutdown ()
  "Join outside admission and the reload lock; never interrupt publication."
  (let ((worker (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                  *physical-restartpoint-worker*)))
    (when worker
      (sb-thread:join-thread worker :timeout 15 :default :timeout)
      (when (sb-thread:thread-alive-p worker)
        (error "native standby restartpoint worker has not stopped"))
      (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
        (when (eq worker *physical-restartpoint-worker*)
          (setf *physical-restartpoint-worker* nil))))))

(defun physical-recovery-prepare-shutdown ()
  "Quiesce native recovery and publish its applied terminal; do not exit the process."
  (when (plusp *live-operation-depth*) (error "standby shutdown requires its own admission gate"))
  (check-recovery-available)
  (unless (recovery-in-progress-p)
    (ereport "55000" "standby shutdown preparation requires recovery"))
  (when *swank-listener-enabled-p* (error "standby shutdown requires Swank to be off"))
  (sb-thread:with-mutex (*physical-shutdown-lock*)
    (sb-thread:with-mutex (*physical-follow-command-lock*)
      (check-recovery-available)
      (when (eq *physical-shutdown-state* :prepared)
        (return-from physical-recovery-prepare-shutdown
          (or *last-replay-lsn* (getf *physical-recovery-state* :end))))
      (let ((finished nil) (terminal nil))
        (unwind-protect
             (progn
               (with-live-operation
                 (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                   (setf *physical-shutdown-state* :stopping)))
               ;; Exit flush needs admission; replay/maintenance may own reload.
               ;; Join all of them before taking either gate.
               (physical-follow-quiesce-runtime)
               (physical-restartpoint-join-for-shutdown)
               (sb-thread:with-mutex (*reload-run-lock*)
                 (with-live-operation-quiescence
                   (check-recovery-available)
                   (when *physical-receive-store*
                     (when (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
                             (physical-store-check-live *physical-receive-store*)
                             (< (physical-store-flushed-lsn *physical-receive-store*)
                                (physical-store-written-lsn *physical-receive-store*)))
                       (physical-recovery-flush)))
                   (setf terminal (if *physical-receive-store*
                                      (physical-recovery-restartpoint-core)
                                      (or *last-replay-lsn* (getf *physical-recovery-state* :end))))
                   ;; C records clean recovery even when no checkpoint advanced.
                   ;; Publish only after durable data/pointer/floor and while gated.
                   (setf *physical-recovery-state*
                         (physical-recovery-write-control
                          *physical-recovery-state* (getf *physical-recovery-state* :phase)
                          terminal *next-xid* :db-state +db-shutdowned-in-recovery+))
                   (setf *physical-shutdown-state* :prepared)))
               ;; A failure while reopening admission is a failed preparation too.
               (setf finished t)
               terminal)
          (unless finished
            (setf *physical-shutdown-state* :failed
                  *physical-recovery-failed-p* t)))))))
