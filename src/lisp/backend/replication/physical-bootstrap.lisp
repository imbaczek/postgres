;;;; Coherent native base copy under local operation quiescence.
(in-package #:pg)

(defvar *physical-bootstrap-test-hook* nil)

(defun physical-bootstrap-test-point (point target)
  (when *physical-bootstrap-test-hook*
    (funcall *physical-bootstrap-test-hook* point target)))

(declaim (ftype function physical-promotion-startup-primary-p physical-promotion-startup-pending-p))

(defun physical-bootstrap-standby-control-p ()
  "Return true when existing recovery control requires ordinary standby preparation.
Malformed control is an error, not permission to fall back to primary startup."
  (when (wal-namespace-file-stat (physical-recovery-control-path) t)
    (member (getf (physical-recovery-read-control *pgdata*) :phase) '(0 1 2))))

(defun physical-recovery-signal-found-p (name)
  "Check NAME for a recovery request, following symlinks as stat does.
Contents and size do not matter. Once stat succeeds, an open or fsync failure
does not cancel the request. Open nonblocking and fsync only regular files."
  (let ((path (namestring (merge-pathnames name *pgdata*))))
    (handler-case (sb-posix:stat path)
      (sb-posix:syscall-error () (return-from physical-recovery-signal-found-p nil)))
    (handler-case
        (let ((fd (sb-posix:open path
                                 (logior sb-posix:o-rdwr sb-posix:o-nonblock
                                         +postmaster-o-cloexec+))))
          (unwind-protect
               (when (sb-posix:s-isreg (sb-posix:stat-mode (sb-posix:fstat fd)))
                 (sb-posix:fsync fd))
            (sb-posix:close fd)))
      (sb-posix:syscall-error () nil))
    t))

(defun physical-recovery-read-signal-mode ()
  "Read both signal names once at cold startup; standby takes precedence."
  (let ((standby (physical-recovery-signal-found-p "standby.signal"))
        (archive (physical-recovery-signal-found-p "recovery.signal")))
    (cond (standby :standby) (archive :archive))))

(defun physical-recovery-signal-retirement-stat (name)
  "Inspect the signal name itself before retirement, rejecting directories.
A missing name is harmless; other lstat failures stop cleanup."
  (handler-case
      (let ((stat (sb-posix:lstat (namestring (merge-pathnames name *pgdata*)))))
        (when (sb-posix:s-isdir (sb-posix:stat-mode stat))
          (error "recovery signal cannot be retired as a file: ~a" name))
        stat)
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
        (error condition)))))

(defun physical-recovery-unlink-signal (name)
  "Unlink the signal name if present, leaving any symlink target untouched."
  (when (physical-recovery-signal-retirement-stat name)
    (sb-posix:unlink (namestring (merge-pathnames name *pgdata*)))))

(defun physical-bootstrap-check-startup ()
  "Choose recovery or primary startup before listeners or ordinary storage setup.
Recovery artifacts without recovery control are an error: pending control is
retired last, so their presence cannot safely select primary startup. Nonpending
standby control requires recovery validation. A pending request is reconciled
against parent or committed child control. Return :PRIMARY-READY when pending
completion already initialized storage. Invalid proof is an error. Resolve the
active timeline goal before recovery preparation or redo."
  (let ((signal-mode (physical-recovery-read-signal-mode))
        (timeline-goal (startup-recovery-timeline-goal)))
    (when (and (not (wal-namespace-file-stat (physical-recovery-control-path) t))
               (or signal-mode
                   (wal-namespace-file-stat (merge-pathnames "recovery.required" *pgdata*) t)
                   (wal-namespace-file-stat (merge-pathnames "physical-backup.manifest" *pgdata*) t)
                   (and (wal-namespace-file-stat (physical-follow-config-path) t)
                        (not (physical-follow-cleanup-only-p (physical-follow-read-config))))
                   (wal-namespace-file-stat (merge-pathnames "physical-restartpoint.control" *pgdata*) t)))
      (error "recovery metadata is incomplete: recovery control is missing"))
    (cond
      ((physical-bootstrap-standby-control-p) (physical-recovery-prepare :signal-mode signal-mode :timeline-goal timeline-goal) (values))
      ((physical-promotion-startup-primary-p) (values))
      ((physical-promotion-startup-pending-p) :primary-ready)
      (t
       (if (or (probe-file (merge-pathnames "recovery.required" *pgdata*))
               (probe-file (merge-pathnames "physical-backup.manifest" *pgdata*))
               (probe-file (merge-pathnames "physical-recovery.control" *pgdata*)))
           (physical-recovery-prepare :signal-mode signal-mode :timeline-goal timeline-goal)
           (when (recovery-in-progress-p)
             (error "cannot clear recovery role by removing its metadata")))
       (values)))))

(declaim (special *postmaster-data-lock*))

(defvar *physical-backup-stage-lock* (sb-thread:make-mutex :name "native backup stages"))

;; A source owns only the container named for its current device, inode,
;; system identifier and canonical path digest.  The owner record also checks
;; the full path, so startup does not sweep an old reseed or relocated cluster.
(defun physical-backup-stage-container (&optional (catalog *catalog*))
  (unless (and *postmaster-data-lock* (open-stream-p *postmaster-data-lock*))
    (error "native backup stages require data directory ownership"))
  (let* ((data (truename *pgdata*))
         (parts (pathname-directory data))
         (stat (sb-posix:stat (namestring data))))
    (unless (and catalog (stringp (car (last parts))))
      (error "native backup stages require durable cluster identity"))
    (merge-pathnames (format nil ".pg-native-backups-~x-~x-~16,'0x-~16,'0x/"
                             (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
                             (catalog-system-identifier catalog)
                             (physical-store-update-digest #xCBF29CE484222325
                                                           (string->bytes (namestring data))))
                     (make-pathname :directory (butlast parts) :defaults data))))

(defun physical-backup-stage-owner-bytes (catalog)
  (let* ((data (truename *pgdata*)) (stat (sb-posix:stat (namestring data))))
    (string->bytes (format nil "PGNATIVEBACKUPSTAGES1~%~d ~d ~d~%~a~%"
                           (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
                           (catalog-system-identifier catalog) (namestring data)))))

(defun physical-backup-open-directory (path)
  (sb-posix:open (string-right-trim "/" (namestring path))
                 (logior sb-posix:o-rdonly sb-posix:o-directory
                         sb-posix:o-nofollow +postmaster-o-cloexec+)))

(defun physical-backup-check-stage-directory (fd)
  (let ((stat (sb-posix:fstat fd)))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:stat-uid stat) (sb-posix:geteuid))
                 (= #o700 (logand #o777 (sb-posix:stat-mode stat))))
      (error "native backup stage directory is not private"))))

(defun physical-backup-open-stage-container (catalog &key create)
  (let* ((root (physical-backup-stage-container catalog))
         (owner (physical-backup-stage-owner-bytes catalog))
         (created nil))
    (unless (query-temp-lstat root)
      (unless create (return-from physical-backup-open-stage-container nil))
      (sb-posix:mkdir (string-right-trim "/" (namestring root)) #o700)
      (setf created t))
    (let ((fd (physical-backup-open-directory root)) (success nil))
      (unwind-protect
           (progn
             (physical-backup-check-stage-directory fd)
             (let ((marker (pathname (query-temp-fd-child-path fd ".owner"))))
               (when created
                 (with-open-file (stream marker :direction :output :if-exists :error
                                                :element-type '(unsigned-byte 8))
                   (sb-posix:fchmod (sb-sys:fd-stream-fd stream) #o600)
                   (write-sequence owner stream)
                   (fsync-stream stream))
                 (sb-posix:fsync fd)
                 (fsync-directory (uiop:pathname-parent-directory-pathname root)))
               (multiple-value-bind (stream size) (open-bounded-regular-file marker 16384)
                 (unwind-protect
                      (let* ((stat (sb-posix:fstat (sb-sys:fd-stream-fd stream)))
                             (bytes (make-array size :element-type '(unsigned-byte 8))))
                        (unless (and (= (sb-posix:stat-uid stat) (sb-posix:geteuid))
                                     (= 1 (sb-posix:stat-nlink stat))
                                     (= #o600 (logand #o777 (sb-posix:stat-mode stat)))
                                     (= size (read-sequence bytes stream))
                                     (equalp owner bytes))
                          (error "native backup stage owner does not match PGDATA")))
                   (close stream))))
             (setf success t)
             (values fd root))
        (unless success (sb-posix:close fd))))))

(defun physical-backup-stage-name-p (name)
  (and (= 12 (length name)) (uiop:string-prefix-p "stage." name)
       (every (lambda (char) (or (and (char<= #\a char #\z)) (and (char<= #\A char #\Z))
                                 (and (char<= #\0 char #\9))))
              (subseq name 6))))

;; Remove children through directory descriptors, without following symlinks.
;; Callers validate the owner and stage directory before allowing removal.
(defun physical-backup-remove-stage-tree (parent name)
  (let* ((path (query-temp-fd-child-path parent name))
         (stat (sb-posix:lstat path)))
    (if (sb-posix:s-isdir (sb-posix:stat-mode stat))
        (let ((fd (physical-backup-open-directory (pathname path))))
          (unwind-protect
               (multiple-value-bind (names complete) (query-temp-directory-entry-names fd)
                 (unless complete (error "native backup stage enumeration failed"))
                 (dolist (child names) (physical-backup-remove-stage-tree fd child)))
            (sb-posix:close fd))
          (sb-posix:rmdir path))
        (sb-posix:unlink path))))

(defun physical-backup-cleanup-stages ()
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (when (probe-file (catalog-path))
      (let* ((*catalog-identity-needs-persist-p* nil)
             (catalog (load-catalog)))
        (when *catalog-identity-needs-persist-p*
          (return-from physical-backup-cleanup-stages nil))
        (multiple-value-bind (fd root) (physical-backup-open-stage-container catalog)
          (declare (ignore root))
          (when fd
            (unwind-protect
                 (multiple-value-bind (names complete) (query-temp-directory-entry-names fd)
                   (unless complete (error "native backup stage container enumeration failed"))
                   (dolist (name names)
                     (when (physical-backup-stage-name-p name)
                       (let ((child (physical-backup-open-directory
                                     (pathname (query-temp-fd-child-path fd name)))))
                         (unwind-protect (physical-backup-check-stage-directory child)
                           (sb-posix:close child)))
                       (physical-backup-remove-stage-tree fd name)))
                   (sb-posix:fsync fd))
              (sb-posix:close fd))))))))

(defun physical-backup-create-stage ()
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (multiple-value-bind (fd root) (physical-backup-open-stage-container *catalog* :create t)
      (unwind-protect
           (uiop:ensure-directory-pathname
            (sb-posix:mkdtemp (namestring (merge-pathnames "stage.XXXXXX" root))))
        (sb-posix:close fd)))))

(defun physical-backup-delete-stage (stage)
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (multiple-value-bind (fd root) (physical-backup-open-stage-container *catalog*)
      (unless fd (error "native backup stage container disappeared"))
      (unwind-protect
           (let ((name (car (last (pathname-directory stage)))))
             (unless (and (physical-backup-stage-name-p name)
                          (equal (truename root)
                                 (truename (uiop:pathname-parent-directory-pathname stage))))
               (error "native backup stage cleanup has a foreign path"))
             (physical-backup-remove-stage-tree fd name)
             (sb-posix:fsync fd))
        (sb-posix:close fd)))))

(defun physical-bootstrap-target (directory)
  "Require an existing empty target outside the source directory hierarchy."
  (let ((target (truename (uiop:ensure-directory-pathname directory)))
        (source (truename *pgdata*)))
    (when (or (uiop:subpathp target source) (uiop:subpathp source target)
              (uiop:directory-files target) (uiop:subdirectories target))
      (error "physical backup target must be empty and separate from source"))
    target))

(defun physical-recovery-marker-bytes ()
  "Canonical recovery exclusion marker shared by creation and both startup paths."
  (string->bytes (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%")))

(defun physical-bootstrap-mark-target (target)
  "Persist the recovery guard before placing any database files in TARGET."
  (fsync-directory (uiop:pathname-parent-directory-pathname target))
  (with-open-file (stream (merge-pathnames "recovery.required" target)
                          :direction :output :if-exists :error :if-does-not-exist :create)
    (write-string (bytes->string (physical-recovery-marker-bytes)) stream)
    (fsync-stream stream))
  (fsync-directory target))

(defun physical-bootstrap-history-inventory (timeline)
  "Return the selected timeline history and available ancestor histories for a backup.
Validate the selected lineage before constructing the inventory."
  (let ((history (timeline-check-lineage (timeline-read-history timeline))))
    (sort (loop for entry in history
                for ancestor = (timeline-history-entry-timeline entry)
                when (and (> ancestor 1)
                          (or (= ancestor timeline)
                              (probe-file (timeline-history-path ancestor *pgdata*))))
                  collect (concatenate 'string "pg_wal/" (timeline-history-filename ancestor)))
          #'string<)))

(defun physical-bootstrap-inventory (&optional (checkpoint-start (wal-checkpoint-record-start)))
  "Return only durable cluster state and catalog-referenced relation forks."
  (let ((files (list "PG_VERSION" "pg_xact" "global/pg_control"
                     "global/pg_catalog.dat" "global/pg_wal_control")))
    (dolist (relfilenode (remove-duplicates (catalog-physical-relfilenodes)))
      (push (format nil "base/~d" relfilenode) files)
      (when (probe-file (relation-file-path relfilenode :visibility))
        (push (format nil "base/~d.vm" relfilenode) files)))
    (let* ((restart checkpoint-start)
           (last (and (plusp *wal-durable-lsn*) (floor (1- *wal-durable-lsn*) +wal-segment-size+))))
      (when last
        (loop for segment from (floor restart +wal-segment-size+) to last
              do (push (concatenate 'string "pg_wal/" (file-namestring (wal-segment-path segment))) files))))
    (sort (append files (physical-bootstrap-history-inventory *wal-insert-timeline*)) #'string<)))

(defun physical-bootstrap-derived-wal-control (target)
  "Derive target control when the source has a historical archive reference.
Validate child control and require a checkpoint beyond the old fork.  Remove only
the old reference from the target, preserving positions, snapshot, timelines,
namespace and other metadata.  Atomically write and read back the complete tuple,
then hash the actual target bytes.  Leave source control unchanged.  With no
reference, return NIL so the ordinary copy preserves source bytes exactly.
The new base receives its own archive; the old primary archive is not copied."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace metadata)
      (wal-read-control-fields)
    (let ((reference (getf metadata :retained-reference)))
      (when reference
        (wal-require-supported-timelines insertion checkpoint-timeline)
        (unless (and snapshot (> checkpoint (getf reference :end)))
          (error "base backup checkpoint does not supersede its historical archive"))
        (let ((copied (copy-list metadata))
              (path (merge-pathnames "global/pg_wal_control" target)))
          (remf copied :retained-reference)
          (let ((bytes (wal-encode-control-image
                        checkpoint durable snapshot :timeline insertion
                        :checkpoint-timeline checkpoint-timeline :namespace namespace :metadata copied)))
            (wal-ensure-directory-durable (uiop:pathname-directory-pathname path))
            (wal-atomic-write-bytes path bytes)
            (let ((actual (recovery-read-bytes path :maximum (1+ +wal-control-maximum-bytes+))))
              (unless (and (equalp bytes actual)
                           (equal (multiple-value-list (wal-decode-control-line (bytes->string actual)))
                                  (list checkpoint durable snapshot insertion checkpoint-timeline namespace copied)))
                (error "base backup control readback differs from its new checkpoint"))
              (list "global/pg_wal_control" (length actual)
                    (physical-store-update-digest #xCBF29CE484222325 actual)))))))))

(defun physical-bootstrap-copy-file (relative target &key limit)
  "Copy and fsync a stable source file or exact prefix, returning its length and checksum."
  (when (equal relative "global/pg_wal_control")
    (let ((derived (physical-bootstrap-derived-wal-control target)))
      (when derived
        (physical-bootstrap-test-point :after-file target)
        (return-from physical-bootstrap-copy-file derived))))
  (let* ((source (merge-pathnames relative *pgdata*))
         (destination (merge-pathnames relative target))
         (parent (uiop:pathname-directory-pathname destination))
         (digest #xCBF29CE484222325)
         (length 0)
         (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
    (wal-ensure-directory-durable parent)
    (with-open-file (input source :element-type '(unsigned-byte 8))
      (with-atomic-output
          (output destination (make-pathname :type "tmp" :defaults destination)
                  parent :element-type '(unsigned-byte 8))
        (loop for count = (read-sequence buffer input
                                         :end (if limit (min (length buffer) (- limit length))
                                                  (length buffer)))
              while (plusp count)
              do (write-sequence buffer output :end count)
                 (incf length count)
                 (setf digest (physical-store-update-digest digest (subseq buffer 0 count))))
        (when (and limit (/= length limit))
          (error "physical base backup file ~a is shorter than its durable prefix" relative))))
    (physical-bootstrap-test-point :after-file target)
    (list relative length digest)))

(defun physical-bootstrap-manifest-text (manifest)
  "Print portable literal strings rather than SBCL's readable BASE-CHAR #A arrays."
  (labels ((literal (value)
             (typecase value
               (string (map 'string #'identity value))
               (cons (mapcar #'literal value))
               (t value))))
    (with-standard-io-syntax (prin1-to-string (literal manifest)))))

(declaim (ftype function physical-recovery-check-checkpoint-resources))

(defun physical-bootstrap-check-export-slot-under-wal-lock (slot name checkpoint-start)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "native backup export validation requires WAL exclusion"))
  (wal-check-healthy)
  (unless (and (not (recovery-in-progress-p))
               (eq slot (gethash name *wal-replication-slots*))
               (physical-slot-state-valid-p slot)
               (equal name (wal-replication-slot-name slot))
               (physical-slot-owned-by-backend-p slot *backend*)
               (eq *backend* (wal-replication-slot-active-owner slot))
               (eq *backend* (wal-replication-slot-active-owner-backend slot))
               (eq :reserved (wal-replication-slot-wal-status slot))
               (= (catalog-system-identifier) (wal-replication-slot-system-id slot))
               (eql *wal-insert-timeline* (wal-replication-slot-restart-timeline slot))
               (typep (wal-replication-slot-restart-lsn slot) '(integer 1 18446744073709551615))
               (<= (wal-replication-slot-restart-lsn slot) checkpoint-start *wal-durable-lsn*))
    (wal-invalid-input "native backup export lost its owned WAL reservation"))
  slot)

(defun physical-bootstrap-check-export-slot (slot name checkpoint-start)
  (unless (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
    (error "physical export validation requires operation quiescence ownership"))
  (sb-thread:with-mutex (*wal-lock*)
    (physical-bootstrap-check-export-slot-under-wal-lock slot name checkpoint-start)))

(defun physical-bootstrap-copy-under-quiescence (target pin-name &key export-slot)
  "Checkpoint and capture one complete native image while mutation admission is closed.
Check checkpoint resource facts against control before creating the source pin.
Capture the active authentication generation and write its canonical adjuncts
outside the immutable file inventory. New manifests require these adjuncts."
  (unless (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
    (error "physical base copy requires operation quiescence ownership"))
  ;; An admitted control operation could enable Swank while this caller drains.
  (when *swank-listener-enabled-p*
    (error "gated physical base backup requires the optional Swank listener to be off"))
  (when (or (not (active-xids-empty-p)) *xid-wrap-pending*
            (probe-file (xid-wrap-marker-path)))
    (error "physical base backup requires no active transaction IDs or pending XID wrap"))
  (let ((policy (authentication-capture-policy)))
    (wal-checkpoint-core)
    (physical-recovery-check-checkpoint-resources *wal-checkpoint-snapshot* *wal-control-metadata*)
    (let ((pin (unless export-slot (physical-source-pin-create pin-name))))
      (when export-slot
        (physical-bootstrap-check-export-slot export-slot pin-name (wal-checkpoint-record-start)))
      ;; Local backup failure retains its private pin for operator inspection.
      ;; Remote export owns a temporary slot and drops it in its outer cleanup.
      (physical-bootstrap-mark-target target)
      (physical-recovery-mark-target target)
      (wal-ensure-directory-durable (merge-pathnames "base/" target))
      (authentication-write-backup-policy policy target)
      (physical-bootstrap-test-point :after-checkpoint target)
      (let* ((checkpoint *wal-checkpoint-lsn*)
             (end *wal-durable-lsn*)
             (system-id (catalog-system-identifier))
             (last-segment (and (plusp end) (concatenate 'string "pg_wal/"
                                                         (file-namestring (wal-segment-path
                                                                           (floor (1- end) +wal-segment-size+))))))
             (files (mapcar (lambda (relative)
                              (physical-bootstrap-copy-file
                               relative target
                               :limit (when (equal relative last-segment)
                                        (1+ (mod (1- end) +wal-segment-size+)))))
                            (physical-bootstrap-inventory)))
             (manifest (list :version 3 :system-id system-id
                             :cluster-identity (catalog-cluster-identity *catalog*) :timeline *wal-insert-timeline*
                             :checkpoint checkpoint :checkpoint-start (wal-checkpoint-record-start checkpoint)
                             :end end :next-xid *next-xid*
                             :source-pin (if export-slot (wal-replication-slot-name export-slot)
                                             (physical-source-pin-name pin)) :files files :auth-policy-required 1)))
        (physical-bootstrap-test-point :before-manifest target)
        (let* ((*print-readably* t)
               (text (physical-bootstrap-manifest-text manifest))
               (bytes (string->bytes text))
               (crc (wal-crc32 bytes))
               (image (string->bytes (format nil "PGPHYSICALBACKUP1 ~8,'0X~%~a~%" crc text))))
          (when pin
            (physical-source-pin-bind-manifest pin (physical-store-update-digest #xCBF29CE484222325 image)))
          (physical-bootstrap-test-point :after-pin-binding target)
          (wal-atomic-write-bytes (merge-pathnames "physical-backup.manifest" target) image))
        (fsync-directory target)
        (physical-bootstrap-test-point :after-manifest target)
        manifest))))

(defun physical-base-backup (directory pin-name)
  "Create a recovery-required native base image while SQL mutations are quiescent.
Invoke as a direct control form with two literal strings, or outside a live
operation. Idle transactions with assigned XIDs cause refusal; new operations
wait until the copy completes. This initial copy pauses the source for its
duration. Cleanup of any deferred slot recycling runs after reload exclusion and
quiescence end. It is not the final online base-backup protocol."
  (prevent-recovery-primary-operation "native base backup")
  (when *swank-listener-enabled-p*
    (error "gated physical base backup requires the optional Swank listener to be off"))
  (when (plusp *live-operation-depth*)
    (error "physical base backup must start outside a live operation"))
  (unless (wal-valid-replication-slot-name-p pin-name)
    (error "invalid physical base backup pin name"))
  (call-with-invalidated-slot-cleanup
   (lambda ()
     (sb-thread:with-mutex (*reload-run-lock*)
       (with-live-operation-quiescence
         (physical-bootstrap-copy-under-quiescence
          (physical-bootstrap-target directory) pin-name))))))

(declaim (ftype function call-with-physical-export-origin-reservation
                physical-export-origin-note-image physical-export-origin-record-backup))

(declaim (ftype function physical-export-origin-capture-backup))

;; Reserve a temporary export slot before quiescing mutations.  Copy and sync
;; the image while quiescent, then drop that slot even on failure.  The local
;; bootstrap path's private pin is separate and is not retired by this export.
(defun physical-export-base-backup (directory name)
  (unless *backend* (error "native backup export requires a backend owner"))
  (when (plusp *live-operation-depth*)
    (error "native backup export must start outside a live operation"))
  (unless (wal-valid-replication-slot-name-p name)
    (error "invalid native backup export slot name"))
  (call-with-physical-export-origin-reservation
   (lambda (origin-reservation)
     (call-with-invalidated-slot-cleanup
      (lambda ()
        (sb-thread:with-mutex (*reload-run-lock*)
          (let ((target (with-live-operation
                          (prevent-recovery-primary-operation "native backup export")
                          (when *swank-listener-enabled-p*
                            (error "native backup export requires Swank to be off"))
                          (physical-bootstrap-target directory))))
            (sb-sys:without-interrupts
              (let ((slot nil))
                (unwind-protect
                     (progn
                       (setf slot (with-live-operation
                                    (wal-create-physical-replication-slot
                                     name :immediately-reserve t :temporary t)))
                       (sb-sys:with-local-interrupts
                         (let ((manifest
                                 (with-live-operation-quiescence
                                   (prevent-recovery-primary-operation "native backup export")
                                   (physical-export-origin-capture-backup
                                    target (physical-bootstrap-copy-under-quiescence
                                            target name :export-slot slot)
                                    origin-reservation))))
                           (physical-export-origin-record-backup target manifest slot origin-reservation)
                           manifest)))
                  (when slot (wal-drop-replication-slot name slot))))))))))))

(defun physical-backup-bundle-path-p (name timeline)
  (or (member name '("physical-backup.manifest" "recovery.required" "physical-recovery.control"
                     "pg_hba.conf" "pg_ident.conf" "authentication.initialized") :test #'equal)
      (recovery-manifest-path-p name timeline)))

(defun physical-backup-send-file (directory entry timeline emit)
  (destructuring-bind (name size expected-digest) entry
    (unless (and (physical-backup-bundle-path-p name timeline)
                 (typep size '(unsigned-byte 64))
                 (typep expected-digest '(unsigned-byte 64)))
      (error "invalid native backup bundle file"))
    (let ((path (string->bytes name))
          (digest #xCBF29CE484222325)
          (buffer (make-array 65535 :element-type '(unsigned-byte 8))))
      (unless (<= 1 (length path) 512) (error "native backup bundle path exceeds its bound"))
      (multiple-value-bind (input actual-size)
          (open-bounded-regular-file (merge-pathnames name directory) size)
        (unwind-protect
             (progn
               (unless (= size actual-size) (error "native backup bundle file length changed"))
               (let ((header (make-payload)))
                 (payload-push-byte header (char-code #\F))
                 (payload-push-u32 header (length path))
                 (loop for byte across path do (payload-push-byte header byte))
                 (replication-push-u64 header size)
                 (replication-push-u64 header expected-digest)
                 (funcall emit header))
               (loop with remaining = size
                     while (plusp remaining)
                     for count = (read-sequence buffer input :end (min remaining (length buffer)))
                     do (when (zerop count) (error "native backup bundle file was truncated"))
                        (let ((bytes (subseq buffer 0 count))
                              (payload (make-payload)))
                          (setf digest (physical-store-update-digest digest bytes))
                          (payload-push-byte payload (char-code #\D))
                          (loop for byte across bytes do (payload-push-byte payload byte))
                          (funcall emit payload))
                        (decf remaining count))
               (unless (and (eq :eof (read-byte input nil :eof)) (= digest expected-digest))
                 (error "native backup bundle file changed"))
               (let ((end (make-payload)))
                 (payload-push-byte end (char-code #\E))
                 (funcall emit end)))
          (close input))))))

(defun physical-backup-metadata-bounds ()
  '(("physical-backup.manifest" 16777216)
    ("recovery.required" 1024)
    ("physical-recovery.control" 196)
    ("pg_hba.conf" 1048576)
    ("pg_ident.conf" 1048576)
    ("authentication.initialized" 1024)))

(defun physical-backup-bundle-entries (directory manifest)
  (recovery-check-manifest-schema manifest)
  (unless (equal manifest (recovery-read-manifest directory))
    (error "native backup bundle manifest changed"))
  (append
   (loop for (name maximum) in
                            (physical-backup-metadata-bounds)
         for bytes = (read-bounded-regular-file (merge-pathnames name directory) maximum)
         collect (list name (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes)))
   (getf manifest :files)))

;; Send B (bundle header), F/D/E (file header, data and end), then Z
;; (count and checksum).  The checksum covers all preceding payloads, including
;; file checksums; it detects corruption and is not an authentication mechanism.
(defun physical-backup-send-bundle (directory manifest emit)
  (let* ((entries (physical-backup-bundle-entries directory manifest))
         (digest #xCBF29CE484222325))
    (flet ((send (payload)
             (setf digest (physical-store-update-digest digest payload))
             (funcall emit payload)))
      (let ((header (make-payload)))
        (payload-push-byte header (char-code #\B))
        (loop for byte across (string->bytes "PGNATIVEBACKUP1") do (payload-push-byte header byte))
        (payload-push-u32 header (length entries))
        (send header))
      (dolist (entry entries)
        (physical-backup-send-file directory entry (getf manifest :timeline) #'send)))
    (let ((end (make-payload)))
      (payload-push-byte end (char-code #\Z))
      (payload-push-u32 end (length entries))
      (replication-push-u64 end digest)
      (funcall emit end))))

(defun physical-backup-read-frame (next)
  (let ((frame (funcall next)))
    (unless (and (typep frame '(vector (unsigned-byte 8))) (<= 1 (length frame) 65536))
      (error "invalid or truncated native backup bundle frame"))
    frame))

(defun physical-backup-file-header (header name maximum &optional expected)
  (unless (and (>= (length header) 21) (= (aref header 0) (char-code #\F)))
    (error "native backup bundle lacks its expected file"))
  (let ((length (bytes-to-u32 header 1)))
    (unless (and (<= 1 length 512) (= (length header) (+ 21 length))
                 (equalp (string->bytes name) (subseq header 5 (+ 5 length))))
      (error "native backup bundle file name or header is invalid"))
    (let ((size (replication-read-u64 header (+ 5 length)))
          (digest (replication-read-u64 header (+ 13 length))))
      (unless (and (<= size maximum)
                   (or (null expected) (and (= size (second expected)) (= digest (third expected)))))
        (error "native backup bundle file disagrees with its manifest or bound"))
      (values size digest))))

(defun physical-backup-receive-file (directory name maximum next &key expected (timeline 1))
  (unless (physical-backup-bundle-path-p name timeline)
    (error "invalid native backup destination path"))
  (multiple-value-bind (size expected-digest)
      (physical-backup-file-header (physical-backup-read-frame next) name maximum expected)
    (let* ((path (merge-pathnames name directory))
           (parent (uiop:pathname-directory-pathname path))
           (digest #xCBF29CE484222325))
      (wal-ensure-directory-durable parent)
      (with-open-file (output path :direction :output :if-exists :error
                                   :if-does-not-exist :create :element-type '(unsigned-byte 8))
        (loop with remaining = size
              while (plusp remaining)
              for frame = (physical-backup-read-frame next)
              for count = (1- (length frame))
              do (unless (and (= (aref frame 0) (char-code #\D)) (<= 1 count remaining))
                   (error "invalid native backup file data extent"))
                 (let ((bytes (subseq frame 1)))
                   (setf digest (physical-store-update-digest digest bytes))
                   (write-sequence bytes output))
                 (decf remaining count))
        (let ((end (physical-backup-read-frame next)))
          (unless (and (= (length end) 1) (= (aref end 0) (char-code #\E))
                       (= expected-digest digest))
            (error "native backup file end or checksum is invalid")))
        (fsync-stream output))
      (fsync-directory parent)
      (list name size digest))))

;; Bound the file count and metadata sizes before receiving the manifest and
;; its inventory.  Check order, paths, sizes and checksums, then verify the
;; immutable recovery image and copied authentication policy before publication.
(defun physical-backup-receive-bundle (directory next)
  (let ((digest #xCBF29CE484222325)
        (magic (string->bytes "PGNATIVEBACKUP1")))
    (flet ((receive-frame ()
             (let ((frame (physical-backup-read-frame next)))
               (setf digest (physical-store-update-digest digest frame))
               frame)))
      (let* ((header (receive-frame))
             (count-offset (1+ (length magic))))
        (unless (and (= (length header) (+ count-offset 4))
                     (= (aref header 0) (char-code #\B))
                     (equalp magic (subseq header 1 count-offset)))
          (error "invalid native backup bundle header"))
        (let ((count (bytes-to-u32 header count-offset)))
          (unless (<= 6 count 1000000) (error "invalid native backup bundle file count"))
          (physical-backup-receive-file directory "physical-backup.manifest" 16777216 #'receive-frame)
          (let ((manifest (recovery-check-manifest-schema (recovery-read-manifest directory))))
            (unless (and (eql 3 (getf manifest :version))
                         (= count (+ 6 (length (getf manifest :files)))))
              (error "native backup bundle inventory disagrees with its manifest"))
            (dolist (entry (rest (physical-backup-metadata-bounds)))
              (physical-backup-receive-file directory (first entry) (second entry) #'receive-frame))
            (dolist (entry (getf manifest :files))
              (physical-backup-receive-file directory (first entry) (second entry) #'receive-frame
                                            :expected entry :timeline (getf manifest :timeline)))
            (let ((end (physical-backup-read-frame next)))
              (unless (and (= 13 (length end)) (= (aref end 0) (char-code #\Z))
                           (= count (bytes-to-u32 end 1)) (= digest (replication-read-u64 end 5)))
                (error "native backup bundle completion or checksum is invalid")))
            (let ((*pgdata* directory))
              (unless (zerop (getf (physical-recovery-read-control directory) :phase))
                (error "native backup bundle is not an immutable base image"))
              (physical-recovery-verify-base-state)
              (authentication-check-backup-policy directory))
            manifest))))))

(defun physical-backup-destination (directory)
  (let* ((target (directory-pathname (namestring (pathname directory))))
         (parts (pathname-directory target))
         (leaf (car (last parts))))
    (unless (and (stringp leaf) (not (wild-pathname-p target)))
      (error "invalid native backup destination"))
    (let* ((parent (truename (make-pathname :directory (butlast parts) :defaults target)))
           (resolved (merge-pathnames (make-pathname :directory (list :relative leaf)) parent)))
      (when (directory-entry-names resolved)
        (error "native backup destination is not empty"))
      (values resolved parent))))

(defun physical-backup-next-copy-frame (channel)
  (loop
    (multiple-value-bind (kind bytes)
        (physical-libpq-receive-copy-out channel "BASE_BACKUP_NATIVE" :maximum 65536)
      (case kind
        (#\D (return bytes))
        (#\C (error "native backup COPY completed before its bundle"))
        (#\I nil)))))

(defun physical-backup-finish-copy (channel)
  (loop
    (multiple-value-bind (kind bytes)
        (physical-libpq-receive-copy-out channel "BASE_BACKUP_NATIVE" :maximum 65536)
      (declare (ignore bytes))
      (case kind
        (#\C (return t))
        (#\D (error "native backup COPY has trailing bundle data"))
        (#\I nil)))))

(declaim (special +physical-native-format+))

;; Receive into a private sibling directory.  Publish only after the complete
;; CopyOut and image verification, by rename followed by parent directory fsync.
;; Normal errors and interrupts remove the stage; SIGKILL can leave it behind.
(defun physical-base-backup-remote (directory conninfo &key (helper (physical-libpq-helper-path)))
  (multiple-value-bind (target parent) (physical-backup-destination directory)
    (let ((stage nil) (channel nil))
      (unwind-protect
           (progn
             (sb-sys:without-interrupts
               (setf stage (uiop:ensure-directory-pathname
                            (sb-posix:mkdtemp
                             (namestring (merge-pathnames ".pg-native-backup.XXXXXX" parent))))))
             (setf channel
                   (physical-libpq-connect helper
                                           (make-physical-libpq-profile conninfo :mode :physical
                                                                                 :fallback "pg_basebackup")
                                           :on-spawn (lambda (spawned) (setf channel spawned))))
             (physical-libpq-start-copy-out
              channel (format nil "BASE_BACKUP_NATIVE (native_format '~a')" +physical-native-format+))
             (let ((manifest (physical-backup-receive-bundle
                              stage (lambda () (physical-backup-next-copy-frame channel)))))
               (physical-backup-finish-copy channel)
               (physical-libpq-close channel)
               (setf channel nil)
               (wal-atomic-write-bytes (merge-pathnames "postgresql.conf" stage)
                                       (make-array 0 :element-type '(unsigned-byte 8)))
               (fsync-directory stage)
               (physical-backup-destination target)
               (sb-posix:rename (string-right-trim "/" (namestring stage))
                                (string-right-trim "/" (namestring target)))
               (fsync-directory parent)
               (setf stage nil)
               manifest))
        (unwind-protect
             (when channel (physical-libpq-close channel))
          (when stage (uiop:delete-directory-tree stage :validate t :if-does-not-exist :ignore)))))))

;; Parse the pg_basebackup options used by the native clone wrapper, preserving
;; the supplied conninfo.  Unsupported options fail instead of being ignored.

(defun native-basebackup-options (arguments)
  (let ((directory nil) (conninfo "") (method "stream"))
    (labels ((value ()
               (or (pop arguments) (error "native base backup option requires a value")))
             (long-value (argument prefix)
               (subseq argument (length prefix))))
      (loop while arguments for argument = (pop arguments)
            do (cond
                 ((member argument '("-D" "--pgdata") :test #'equal)
                  (setf directory (value)))
                 ((uiop:string-prefix-p "--pgdata=" argument)
                  (setf directory (long-value argument "--pgdata=")))
                 ((member argument '("-d" "--dbname") :test #'equal)
                  (setf conninfo (value)))
                 ((uiop:string-prefix-p "--dbname=" argument)
                  (setf conninfo (long-value argument "--dbname=")))
                 ((member argument '("-X" "--wal-method" "--xlog-method") :test #'equal)
                  (setf method (value)))
                 ((uiop:string-prefix-p "--wal-method=" argument)
                  (setf method (long-value argument "--wal-method=")))
                 ((member argument '("-w" "--no-password" "-v" "--verbose") :test #'equal) nil)
                 (t (error "unsupported native base backup option")))))
    (unless (and directory (plusp (length directory)) (equal method "stream"))
      (error "native base backup requires --pgdata and WAL method stream"))
    (values directory conninfo)))

(defun native-basebackup-main (arguments)
  (handler-case
      (multiple-value-bind (directory conninfo) (native-basebackup-options arguments)
        (physical-base-backup-remote directory conninfo)
        0)
    (error (condition)
      (format *error-output* "pg_basebackup: ~a~%" condition)
      1)))
