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

(defun physical-restartpoint-encode (metadata catalog-bytes &optional prefix-bytes)
  (physical-restartpoint-check-wal-prefix metadata prefix-bytes)
  (let* ((text (string->bytes (physical-bootstrap-manifest-text metadata)))
         (length (+ 32 (length text) (length catalog-bytes) (length prefix-bytes))))
    (unless (and (<= 1 (length text) (* 16 1024 1024))
                 (plusp (length catalog-bytes)) (<= length (* 1024 1024 1024)))
      (error "physical restartpoint exceeds its readable generation bounds"))
    (let ((bytes (make-array length :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace bytes (if (eql 4 (getf metadata :version))
                         #(80 71 82 80 71 69 78 50) #(80 71 82 80 71 69 78 49)))
      (wal-set-u32 bytes 8 (length text))
      (wal-set-u64 bytes 12 (length catalog-bytes))
      (wal-set-u32 bytes 20 (length prefix-bytes))
      (replace bytes text :start1 32)
      (replace bytes catalog-bytes :start1 (+ 32 (length text)))
      (when prefix-bytes (replace bytes prefix-bytes :start1 (+ 32 (length text) (length catalog-bytes))))
      (wal-set-u32 bytes 24 (wal-crc32 bytes))
      bytes)))

(defun physical-restartpoint-decode (bytes)
  (unless (>= (length bytes) 32) (error "short physical restartpoint generation"))
  (let ((crc (wal-u32 bytes 24)) (text-length (wal-u32 bytes 8))
                                 (catalog-length (wal-u64 bytes 12)) (prefix-length (wal-u32 bytes 20))
                                 (new (equalp (subseq bytes 0 8) #(80 71 82 80 71 69 78 50))))
    (setf bytes (copy-seq bytes))
    (wal-set-u32 bytes 24 0)
    (unless (and (or new (equalp (subseq bytes 0 8) #(80 71 82 80 71 69 78 49)))
                 (or new (zerop prefix-length))
                 (< prefix-length +wal-segment-size+)
                 (zerop (wal-u32 bytes 28)) (= crc (wal-crc32 bytes))
                 (<= 1 text-length (* 16 1024 1024)) (plusp catalog-length)
                 (= (length bytes) (+ 32 text-length catalog-length prefix-length)))
      (error "invalid physical restartpoint generation"))
    (let* ((metadata (recovery-parse-manifest-text
                      (bytes->string bytes :start 32 :end (+ 32 text-length))))
           (catalog-end (+ 32 text-length catalog-length))
           (prefix (when (and new (getf metadata :wal-prefix-digest)) (subseq bytes catalog-end))))
      (unless (eq new (eql 4 (getf metadata :version)))
        (error "restartpoint envelope and metadata versions disagree"))
      (when (and new (null (getf metadata :wal-prefix-digest)) (plusp prefix-length))
        (error "unavailable restartpoint prefix has stored bytes"))
      (physical-restartpoint-check-wal-prefix metadata prefix)
      (values metadata (subseq bytes (+ 32 text-length) catalog-end) prefix))))

;;; Membership is optional in a generation.  When present, it must describe
;;; the same applied LSN and actual next XID as the enclosing generation, and
;;; its anchor cannot precede the base.  Source identity belongs to that
;;; enclosing generation and is checked with its other provenance.

(defun physical-restartpoint-check-source-xids (metadata)
  (let ((source (getf metadata :source-xids)))
    (when source
      (running-xacts-check-state source)
      (unless (and (= (getf source :through) (getf metadata :lsn))
                   (= (getf source :next-xid) (getf metadata :next-xid))
                   (<= (getf metadata :base) (getf source :anchor)))
        (error "restartpoint source membership differs from its replay boundary"))))
  (values))

(defun physical-restartpoint-check-source-resources (metadata)
  "Validate an optional version 4 source fact at the restartpoint's replay position."
  (when (loop for key in metadata by #'cddr thereis (eq key :source-resource-fact))
    (let ((fact (getf metadata :source-resource-fact))
          (timeline (getf metadata :replay-timeline (getf metadata :timeline))))
      (unless (and (eql 4 (getf metadata :version)) (wal-resource-fact-valid-p fact)
                   (<= (getf fact :resource-lsn) (getf metadata :lsn))
                   (<= (getf fact :resource-timeline) timeline))
        (error "invalid restartpoint source resource fact"))))
  (values))

(defun physical-restartpoint-resource-seed (metadata state store start)
  "Recover the fact at START from the restartpoint, checkpoint or retained WAL.
Prove the separately saved required fact at its own recovery boundary before use."
  (let* ((checkpoint (if metadata (getf metadata :source-checkpoint) (getf state :source-checkpoint)))
         (timeline (if metadata (getf metadata :replay-timeline (getf metadata :timeline))
                       (getf state :timeline)))
         (base (getf state :end))
         (base-fact (getf state :base-source-resource-fact))
         (fact (if metadata (getf metadata :source-resource-fact) base-fact))
         (required (getf state :required-applied))
         (minimum (getf state :required-resource-fact)))
    (when (and (null fact) checkpoint
               (= (getf checkpoint :end) start) (= (getf checkpoint :timeline) timeline))
      (setf fact (wal-copy-control-resource-fields checkpoint)))
    (when (and metadata (null fact))
      (cond
        ((and checkpoint (getf checkpoint :resource-parameters)
              (<= (physical-store-floor-lsn store) (getf checkpoint :end) start))
         (setf fact (physical-recovery-scan-resource-fact
                     store (getf checkpoint :end) start (wal-copy-control-resource-fields checkpoint) state)))
        ((<= (physical-store-floor-lsn store) base)
         (setf fact (physical-recovery-scan-resource-fact store base start base-fact state)))))
    (when fact
      (unless (<= (getf fact :resource-lsn) start)
        (error "restartpoint resources are newer than its replay seed"))
      (timeline-check-resource-origin fact (physical-recovery-resource-history store state)))
    (when minimum
      (if (< start required)
          (unless (equal minimum (physical-recovery-scan-resource-fact store start required fact state))
            (error "required resource fact cannot be reconstructed; take a fresh base backup"))
          (let ((candidate (copy-list state)))
            (unless fact
              (error "restartpoint lacks required resource proof; take a fresh base backup"))
            (setf (getf candidate :source-resource-fact) fact
                  (getf candidate :replay-timeline) timeline)
            (physical-recovery-control-frontier candidate start))))
    (wal-copy-control-resource-fields fact)))

(defun physical-restartpoint-history-fields-valid-p (metadata)
  "Check the optional selected-history reference and replay timeline in metadata.
Older metadata without these fields uses the base timeline."
  (let* ((base (getf metadata :timeline)) (target (getf metadata :history-target))
                                          (digest (getf metadata :history-digest))
                                          (replay (getf metadata :replay-timeline base))
                                          (source-xids (loop for key in metadata by #'cddr thereis (eq key :source-xids)))
                                          (resources (loop for key in metadata by #'cddr thereis (eq key :source-resource-fact))))
    (and (= (length metadata) (+ 28 (if source-xids 2 0) (if resources 2 0) (if target 6 0)))
         (physical-recovery-selection-fields-valid-p 2 3 base replay target digest)
         (or (not target)
             (every (lambda (key) (loop for name in metadata by #'cddr thereis (eq key name)))
                    '(:replay-timeline :history-target :history-digest))))))

(defun physical-restartpoint-check-metadata (metadata catalog-bytes state &optional prefix-bytes)
  (physical-history-check-plist
   metadata (append '(:version :system-id :cluster-identity :timeline :manifest-digest
                      :base :lsn :prefix-digest :next-xid :forks :seen)
                    (when (member (getf metadata :version) '(2 3 4)) '(:source-checkpoint))
                    (when (member (getf metadata :version) '(3 4)) '(:source-reservation))
                    (when (eql 4 (getf metadata :version)) '(:wal-prefix-digest))))
  (unless (and (case (getf metadata :version)
                 (1 (= (length metadata) 22)) (2 (= (length metadata) 24))
                 (3 (= (length metadata) 26))
                 (4 (physical-restartpoint-history-fields-valid-p metadata))
                 (otherwise nil))
               (eql (getf metadata :system-id) (getf state :system-id))
               (equal (getf metadata :cluster-identity) (getf state :cluster-identity))
               (eql (getf metadata :timeline) (getf state :timeline))
               (eql (getf metadata :manifest-digest) (getf state :manifest-digest))
               (eql (getf metadata :base) (getf state :end))
               (every (lambda (key) (typep (getf metadata key) '(unsigned-byte 64)))
                      '(:lsn :prefix-digest))
               (<= (getf state :end) (getf metadata :lsn))
               (typep (getf metadata :next-xid)
                      (if (member (getf metadata :version) '(3 4))
                          '(integer 3 18446744073709551616)
                          '(integer 3 18446744073709551615))))
    (error "foreign or malformed physical restartpoint metadata"))
  (physical-restartpoint-check-source-xids metadata)
  (physical-restartpoint-check-source-resources metadata)
  (let ((reservation (getf metadata :source-reservation)))
    (when reservation
      (unless (and (physical-source-reservation-valid-p reservation)
                   (= (getf reservation :through) (getf metadata :lsn))
                   (or (not (getf reservation :complete))
                       (<= (getf metadata :next-xid) (1+ (getf reservation :bound)))))
        (error "invalid restartpoint source allocation frontier"))))
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
  (physical-restartpoint-check-wal-prefix metadata prefix-bytes)
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

;;; Save a private copy of the applied membership with its replay boundary.
;;; A missing field means coverage is unknown, not that no XIDs are running.

(defun physical-restartpoint-metadata (digest catalog-bytes &optional prefix-bytes)
  (let* ((state *physical-recovery-state*)
         (forks (physical-restartpoint-fork-inventory catalog-bytes))
         (seen (loop for node being the hash-keys of *physical-replay-seen-relfilenodes* collect node)))
    (dolist (entry forks)
      (pushnew (parse-integer (first entry) :start 5 :junk-allowed t) seen))
    (let ((metadata
            (list :version 4 :system-id (getf state :system-id)
                  :cluster-identity (getf state :cluster-identity) :timeline (getf state :timeline)
                  :manifest-digest (getf state :manifest-digest) :base (getf state :end)
                  :lsn *last-replay-lsn* :prefix-digest digest :next-xid *next-xid*
                  :forks forks :seen (sort seen #'<)
                  :source-checkpoint (copy-tree (getf state :source-checkpoint))
                  :source-reservation (copy-list (getf state :source-reservation))
                  :wal-prefix-digest (when prefix-bytes
                                       (physical-store-update-digest #xCBF29CE484222325 prefix-bytes)))))
      (when (getf state :source-resource-fact)
        (setf metadata (append metadata
                               (list :source-resource-fact
                                     (wal-copy-control-resource-fields (getf state :source-resource-fact))))))
      (when (getf state :source-xids)
        (setf metadata (append metadata (list :source-xids (copy-tree (getf state :source-xids))))))
      (when (getf state :history-target)
        (setf metadata (append metadata
                               (list :replay-timeline (physical-recovery-replay-timeline state)
                                     :history-target (getf state :history-target)
                                     :history-digest (getf state :history-digest)))))
      metadata)))

(defun physical-restartpoint-publish (digest)
  "Called at a complete applied terminal with replay/admission and store locks held."
  (smgr-fsync-all)
  (clog-fsync)
  (with-open-file (stream (catalog-path) :element-type '(unsigned-byte 8)) (fsync-stream stream))
  (fsync-directory (merge-pathnames "base/" *pgdata*))
  (fsync-directory (merge-pathnames "global/" *pgdata*))
  (let* ((catalog-bytes (recovery-read-bytes (catalog-path) :maximum (* 1024 1024 1024)))
         (prefix (physical-recovery-wal-prefix-under-lock *physical-receive-store* *last-replay-lsn*))
         (metadata (physical-restartpoint-metadata digest catalog-bytes prefix))
         (bytes (physical-restartpoint-encode metadata catalog-bytes prefix))
         (hash (physical-store-update-digest #xCBF29CE484222325 bytes))
         (path (physical-restartpoint-generation-path *last-replay-lsn* hash))
         (pointer (make-array 28 :element-type '(unsigned-byte 8) :initial-element 0)))
    (physical-restartpoint-check-metadata metadata catalog-bytes *physical-recovery-state* prefix)
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
    (setf (getf *physical-recovery-state* :restartpoint-lsn) *last-replay-lsn*
          (getf *physical-recovery-state* :wal-prefix-lsn) *last-replay-lsn*
          (getf *physical-recovery-state* :wal-prefix-bytes) prefix)
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
      (multiple-value-bind (metadata catalog-bytes prefix) (physical-restartpoint-decode bytes)
        (physical-restartpoint-check-metadata metadata catalog-bytes state prefix)
        (unless (= lsn (getf metadata :lsn)) (error "restartpoint pointer boundary mismatch"))
        (values metadata catalog-bytes prefix digest)))))

;;; Restore membership from the selected, validated generation without CLOG
;;; filtering: on-disk CLOG can be ahead of that replay boundary.  Missing
;;; membership and legacy fallback leave coverage unknown.  The restored list
;;; is copied so recovery owns it independently of the decoded generation.

(defun physical-restartpoint-prepare-storage (store)
  "Validate provenance, suffix and surviving forks before restoring an R catalog."
  (let* ((state *physical-recovery-state*)
         (identity (getf state :cluster-identity))
         (required (getf state :required-applied)))
    (multiple-value-bind (metadata catalog-bytes prefix) (physical-restartpoint-read state)
      ;; Legacy generations stored capacity as visibility. A complete retained
      ;; prefix can reconstruct actual counters; compacted legacy images cannot.
      (when (and metadata (< (getf metadata :version) 3))
        (unless (<= (physical-store-floor-lsn store) (getf state :end))
          (error "legacy restartpoint lost actual XID history; reseed required"))
        (setf metadata nil catalog-bytes nil))
      (physical-recovery-policy-check-seed state store metadata)
      (let ((start (if metadata (getf metadata :lsn) (getf state :end)))
            (resource-seed nil))
        (unless (and (<= (physical-store-floor-lsn store) start (physical-store-flushed-lsn store))
                     (or metadata (= start (physical-store-base-lsn store))))
          (error "receive floor lacks its required published restartpoint"))
        (when metadata
          (physical-recovery-check-history-reference store (getf metadata :history-target)
                                                     (getf metadata :history-digest))
          (physical-recovery-check-store-point
           store start (getf metadata :replay-timeline (getf metadata :timeline)))
          (unless (= (getf metadata :prefix-digest) (physical-store-prefix-digest store start))
            (error "restartpoint differs from its durable receive prefix")))
        (setf resource-seed (physical-restartpoint-resource-seed metadata state store start))
        (multiple-value-bind (removed terminal reset-forks)
            (physical-recovery-proven-drops
             store identity :start-lsn start :required-lsn (max required start)
             :policy (getf state :timeline-policy))
          (declare (ignore terminal))
          (physical-recovery-check-base-forks
           (if metadata (list :files (getf metadata :forks)) state) removed reset-forks))
        (setf (getf *physical-recovery-state* :wal-prefix-lsn) (and metadata start)
              (getf *physical-recovery-state* :wal-prefix-bytes) (and metadata prefix))
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
                    (getf *physical-recovery-state* :restartpoint-lsn) start
                    (getf *physical-recovery-state* :source-checkpoint)
                    (copy-tree (getf metadata :source-checkpoint))
                    (getf *physical-recovery-state* :source-reservation)
                    (copy-list (getf metadata :source-reservation))
                    (getf *physical-recovery-state* :source-xids)
                    (copy-tree (getf metadata :source-xids))))
            (progn
              (physical-recovery-reset-replay-history)
              ;; Latest control visibility belongs to REQUIRED, not the older base.
              (setf (getf *physical-recovery-state* :source-xids) nil
                    *next-xid* (canonical-next-full-xid (getf state :next-xid))
                    *xid-batch-end* *next-xid*)))
        (when (or resource-seed (getf *physical-recovery-state* :source-resource-fact))
          (setf (getf *physical-recovery-state* :source-resource-fact)
                (wal-copy-control-resource-fields resource-seed)))
        (setf (getf *physical-recovery-state* :replay-timeline)
              (if metadata (getf metadata :replay-timeline (getf metadata :timeline))
                  (getf state :timeline)))
        (when (> start required)
          (when resource-seed
            (setf (getf *physical-recovery-state* :required-resource-fact)
                  (wal-copy-control-resource-fields resource-seed)))
          (setf (getf *physical-recovery-state* :required-applied) start
                (getf *physical-recovery-state* :required-timeline) (physical-recovery-replay-timeline)
                (getf *physical-recovery-state* :observed-next-xid)
                (max (or (getf state :observed-next-xid) 3) *next-xid*)))
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
    (let ((configuration (startup-require-configuration)))
      (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
        (physical-store-check-live *physical-receive-store*)
        (physical-restartpoint-due-p
         *last-replay-lsn* (physical-store-floor-lsn *physical-receive-store*)
         now *physical-restartpoint-last-time*
         (startup-configuration-timeout configuration)
         (startup-configuration-wal-bytes configuration))))))

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
  (when (and (or *physical-shutdown-state* *physical-promotion-stopping-p*)
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
  (loop until (or *physical-shutdown-state* *physical-promotion-stopping-p*
                  (shutdown-in-progress-p) *physical-recovery-failed-p*)
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
  "Cancel transitions, quiesce recovery and publish its applied terminal.
Join workers before taking reload or admission gates; do not exit the process."
  (when (plusp *live-operation-depth*) (error "standby shutdown requires its own admission gate"))
  (check-recovery-available)
  (unless (recovery-in-progress-p)
    (ereport "55000" "standby shutdown preparation requires recovery"))
  (when *swank-listener-enabled-p* (error "standby shutdown requires Swank to be off"))
  (physical-follow-call-with-controller-blocked
   (lambda ()
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
                     *physical-recovery-failed-p* t)))))))))
