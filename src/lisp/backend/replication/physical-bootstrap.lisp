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

(declaim (ftype function logical-slot-copy-check-startup))

(defun physical-bootstrap-check-startup ()
  "Choose recovery or primary startup before listeners or ordinary storage setup.
Recovery artifacts without recovery control are an error: pending control is
retired last, so their presence cannot safely select primary startup. Nonpending
standby control requires recovery validation. A pending request is reconciled
against parent or committed child control. Return :PRIMARY-READY when pending
completion already initialized storage. Invalid proof is an error. Resolve the
active timeline goal before recovery preparation or redo."
  (logical-slot-copy-check-startup)
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
(defun physical-backup-remove-stage-tree (parent name &key mount expected)
  (let ((path (query-temp-fd-child-path parent name))
        (observed (when mount (physical-image-file-facts parent name))))
    (flet ((check (actual)
             (let ((original (or expected observed)))
               (unless (and (= mount (ninth actual))
                            (equal (subseq original 0 3) (subseq actual 0 3))
                            (= (fifth original) (fifth actual)))
                 (error "native backup stage cleanup found a different object or mount")))))
      (when observed (check observed))
      (let ((stat (sb-posix:lstat path)))
        (if (sb-posix:s-isdir (sb-posix:stat-mode stat))
            (let ((fd (physical-backup-open-directory (pathname path))))
              (unwind-protect
                   (progn
                     (when observed (check (physical-image-file-facts fd)))
                     (multiple-value-bind (names complete) (query-temp-directory-entry-names fd)
                       (unless complete (error "native backup stage enumeration failed"))
                       (dolist (child names)
                         (physical-backup-remove-stage-tree fd child :mount mount)))
                     (when observed (check (physical-image-file-facts parent name)))
                     (sb-posix:rmdir path))
                (sb-posix:close fd)))
            (progn
              (when observed (check (physical-image-file-facts parent name)))
              (sb-posix:unlink path)))))))

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

(defun physical-backup-delete-stage (stage &key missing-ok)
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (multiple-value-bind (fd root) (physical-backup-open-stage-container *catalog*)
      (unless fd (error "native backup stage container disappeared"))
      (unwind-protect
           (let ((name (car (last (pathname-directory stage)))))
             (unless (and (physical-backup-stage-name-p name)
                          (equal (truename root)
                                 (truename (uiop:pathname-parent-directory-pathname stage))))
               (error "native backup stage cleanup has a foreign path"))
             (when (or (not missing-ok)
                       (query-temp-lstat (pathname (query-temp-fd-child-path fd name))))
               (physical-backup-remove-stage-tree fd name)
               (sb-posix:fsync fd)))
        (sb-posix:close fd)))))

(defvar *physical-rewind-stage-readers* (make-hash-table :test #'equal))

(defvar *physical-rewind-stage-retirements* (make-hash-table :test #'equal))

(defvar *physical-rewind-stage-count-limit* 16)

(defvar *physical-rewind-stage-byte-limit* (* 16 1024 1024 1024))

(defun physical-rewind-stage-name (lease-name)
  (unless (wal-valid-replication-slot-name-p lease-name)
    (error "invalid rewind stage lease name"))
  (concatenate 'string "rewind." lease-name))

(defun physical-rewind-stage-lease-name (name)
  (when (uiop:string-prefix-p "rewind." name)
    (let ((lease-name (subseq name 7)))
      (when (wal-valid-replication-slot-name-p lease-name) lease-name))))

(defun physical-rewind-stage-names (container)
  (multiple-value-bind (names complete) (query-temp-directory-entry-names container)
    (unless complete (error "rewind stage enumeration failed"))
    (remove-if-not #'physical-rewind-stage-lease-name names)))

(defun physical-rewind-open-stage-directory (container name)
  (let ((child (physical-backup-open-directory
                (pathname (query-temp-fd-child-path container name)))))
    (handler-case (progn (physical-backup-check-stage-directory child) child)
      (error (condition)
        (sb-posix:close child)
        (error condition)))))

(defun physical-rewind-retain-stage (stage lease-name entries)
  (when (> (reduce #'+ entries :key #'second) *physical-rewind-stage-byte-limit*)
    (error "rewind image exceeds retained stage capacity"))
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (multiple-value-bind (container root) (physical-backup-open-stage-container *catalog*)
      (unless container (error "rewind stage container disappeared"))
      (unwind-protect
           (let* ((name (car (last (pathname-directory stage))))
                  (retained (physical-rewind-stage-name lease-name))
                  (from (query-temp-fd-child-path container name))
                  (to (query-temp-fd-child-path container retained)))
             (unless (and (physical-backup-stage-name-p name)
                          (equal root (uiop:pathname-parent-directory-pathname stage))
                          (not (query-temp-lstat (pathname to)))
                          (< (length (physical-rewind-stage-names container))
                             *physical-rewind-stage-count-limit*))
               (error "rewind stage cannot be retained in its source workspace"))
             (let ((child (physical-rewind-open-stage-directory container name)))
               (unwind-protect
                    (progn
                      (sb-posix:fsync child)
                      (sb-posix:rename from to)
                      (sb-posix:fsync container)
                      (setf (gethash lease-name *physical-rewind-stage-readers*) 1)
                      (merge-pathnames (concatenate 'string retained "/") root))
                 (sb-posix:close child))))
        (sb-posix:close container)))))

(defun physical-rewind-remove-retained-stage (container name)
  (when (query-temp-lstat (pathname (query-temp-fd-child-path container name)))
    (sb-posix:close (physical-rewind-open-stage-directory container name))
    (physical-backup-remove-stage-tree container name)
    (sb-posix:fsync container)))

(defun physical-rewind-remove-stage-under-lock (lease-name)
  (multiple-value-bind (container root) (physical-backup-open-stage-container *catalog*)
    (declare (ignore root))
    (when container
      (unwind-protect
           (physical-rewind-remove-retained-stage container (physical-rewind-stage-name lease-name))
        (sb-posix:close container)))))

(defun physical-rewind-retire-stage (record)
  (when (member (getf record :phase) '(:interrupted :closing :closed))
    (let ((lease-name (getf (getf record :request) :name)))
      (sb-thread:with-mutex (*physical-backup-stage-lock*)
        (if (gethash lease-name *physical-rewind-stage-readers*)
            (setf (gethash lease-name *physical-rewind-stage-retirements*) t)
            (physical-rewind-remove-stage-under-lock lease-name))))))

(defun physical-rewind-release-stage (lease-name)
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (let ((readers (gethash lease-name *physical-rewind-stage-readers*)))
      (unless (and readers (plusp readers))
        (error "rewind stage reader is not registered"))
      (if (> readers 1)
          (decf (gethash lease-name *physical-rewind-stage-readers*))
          (progn
            (remhash lease-name *physical-rewind-stage-readers*)
            (when (remhash lease-name *physical-rewind-stage-retirements*)
              (physical-rewind-remove-stage-under-lock lease-name)))))))

(defun physical-rewind-pin-retained-stage (lease-name)
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (when (gethash lease-name *physical-rewind-stage-retirements*)
      (error "retained rewind stage is retiring"))
    (multiple-value-bind (container root) (physical-backup-open-stage-container *catalog*)
      (declare (ignore root))
      (unless container (error "retained rewind stage is unavailable"))
      (unwind-protect
           (let ((fd (physical-rewind-open-stage-directory container (physical-rewind-stage-name lease-name))))
             (incf (gethash lease-name *physical-rewind-stage-readers* 0))
             fd)
        (sb-posix:close container)))))

(defun call-with-physical-rewind-retained-stage (lease-name function &key check-lease)
  (let ((fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-rewind-pin-retained-stage lease-name)))
           (when check-lease (funcall check-lease))
           (call-with-physical-image-directory fd function :primary))
      (when fd
        (unwind-protect (sb-posix:close fd)
          (physical-rewind-release-stage lease-name))))))

(defun physical-rewind-cleanup-stages-at-startup (lease-records)
  (sb-thread:with-mutex (*physical-backup-stage-lock*)
    (multiple-value-bind (container root) (physical-backup-open-stage-container *catalog*)
      (declare (ignore root))
      (when container
        (unwind-protect
             (dolist (name (physical-rewind-stage-names container))
               (let ((record (gethash (physical-rewind-stage-lease-name name) lease-records)))
                 (unless (and (eq :ready (getf record :phase)) (getf record :rewind-seal-digest))
                   (physical-rewind-remove-retained-stage container name))))
          (sb-posix:close container))))))

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

(defun physical-bootstrap-relation-inventory ()
  (loop for relfilenode in (remove-duplicates (catalog-physical-relfilenodes))
        collect (format nil "base/~d" relfilenode)
        when (probe-file (relation-file-path relfilenode :visibility))
          collect (format nil "base/~d.vm" relfilenode)))

(defun physical-bootstrap-inventory (&optional (checkpoint-start (wal-checkpoint-record-start)))
  "Return only durable cluster state and catalog-referenced relation forks."
  (let ((files (append (physical-bootstrap-relation-inventory)
                       (list "PG_VERSION" "pg_xact" "global/pg_control"
                             "global/pg_catalog.dat" "global/pg_wal_control"))))
    (when (authentication-entry-exists-p (pgdata-path "global/pg_bootstrap_scram"))
      (push "global/pg_bootstrap_scram" files))
    (let* ((restart checkpoint-start)
           (last (and (plusp *wal-durable-lsn*) (floor (1- *wal-durable-lsn*) +wal-segment-size+))))
      (when last
        (loop for segment from (floor restart +wal-segment-size+) to last
              do (push (concatenate 'string "pg_wal/" (file-namestring (wal-segment-path segment))) files))))
    (sort (append files (physical-bootstrap-history-inventory *wal-insert-timeline*)) #'string<)))

(defun physical-bootstrap-derived-wal-image ()
  "Return a control image and decoded tuple when the base must drop local metadata."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace metadata)
      (wal-read-control-fields)
    (let ((reference (getf metadata :retained-reference)))
      (when reference
        (wal-require-supported-timelines insertion checkpoint-timeline)
        (unless (and snapshot (> checkpoint (getf reference :end)))
          (error "base backup checkpoint does not supersede its historical archive"))))
    (multiple-value-bind (copied derived-p) (wal-control-base-metadata metadata)
      (when derived-p
        (values (wal-encode-control-image
                 checkpoint durable snapshot :timeline insertion
                 :checkpoint-timeline checkpoint-timeline :namespace namespace :metadata copied)
                (list checkpoint durable snapshot insertion checkpoint-timeline namespace copied))))))

(defun physical-bootstrap-derived-wal-control (target)
  "Derive target control when source control contains local-only metadata.
Validate an archive reference against the checkpoint, then remove it and the
checksum qualification from the copied control. Preserve the other control facts,
write and read back the tuple, and hash the target bytes. Return NIL when no
derivation is needed. The source control remains unchanged."
  (multiple-value-bind (bytes expected) (physical-bootstrap-derived-wal-image)
    (when bytes
      (let ((path (merge-pathnames "global/pg_wal_control" target)))
        (wal-ensure-directory-durable (uiop:pathname-directory-pathname path))
        (wal-atomic-write-bytes path bytes)
        (let ((actual (recovery-read-bytes path :maximum (1+ +wal-control-maximum-bytes+))))
          (unless (and (equalp bytes actual)
                       (equal (multiple-value-list (wal-decode-control-line (bytes->string actual))) expected))
            (error "base backup control readback differs from its new checkpoint"))
          (list "global/pg_wal_control" (length actual)
                (physical-store-update-digest #xCBF29CE484222325 actual)))))))

(defun physical-backup-private-file-p (name)
  (or (bootstrap-scram-path-p name)
      (equal name "global/pg_catalog.dat")))

(defun physical-bootstrap-copy-file (relative target &key limit)
  "Copy and fsync a stable source file or exact prefix, returning its length and checksum.
Each read writes and hashes the same bytes.  A requested prefix must be complete."
  (when (bootstrap-scram-path-p relative)
    (multiple-value-bind (verifier text) (read-bootstrap-scram-verifier)
      (declare (ignore verifier))
      (let ((destination (merge-pathnames relative target))
            (bytes (string->bytes text)))
        (wal-ensure-directory-durable (uiop:pathname-directory-pathname destination))
        (unless (authentication-create-file destination text)
          (error "bootstrap verifier already exists in backup target"))
        (physical-bootstrap-test-point :after-file target)
        (return-from physical-bootstrap-copy-file
          (list relative (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))))))
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
                  parent :element-type '(unsigned-byte 8)
                  :mode (when (physical-backup-private-file-p relative) #o600))
        (loop for count = (read-sequence buffer input
                                         :end (if limit (min (length buffer) (- limit length))
                                                  (length buffer)))
              while (plusp count)
              do (write-sequence buffer output :end count)
                 (incf length count)
                 (setf digest (physical-store-update-digest-span digest buffer 0 count)))
        (when (and limit (/= length limit))
          (error "physical base backup file ~a is shorter than its durable prefix" relative))))
    (physical-bootstrap-test-point :after-file target)
    (list relative length digest)))

(defun physical-bootstrap-manifest-text (manifest)
  "Print stable eligible literal trees directly, producing an owned string.
Standard I/O syntax with readably NIL avoids SBCL's readable BASE-CHAR #A arrays.
Ineligible trees use the historical copier fallback; depth or cycle rejection
only declines the fast path and does not add a new cycle error."
  (when (%metadata-literal-tree-p manifest 0)
    (return-from physical-bootstrap-manifest-text
      (with-standard-io-syntax
        (let ((*print-readably* nil))
          (prin1-to-string manifest)))))
  (labels ((literal (value)
             (typecase value
               (string (map 'string #'identity value))
               (cons (mapcar #'literal value))
               (t value))))
    (with-standard-io-syntax (prin1-to-string (literal manifest)))))

(defun physical-bootstrap-manifest-image (manifest)
  "Return an owned manifest image with its CRC envelope.
Encode the canonical text as UTF-8 once and calculate the CRC over those bytes."
  (let* ((payload (string->bytes (physical-bootstrap-manifest-text manifest)))
         (crc (wal-crc32 payload))
         (image (make-array (+ 28 (length payload)) :element-type '(unsigned-byte 8))))
    (%physical-bootstrap-write-manifest-envelope payload crc image)))

(declaim (ftype function physical-recovery-check-checkpoint-resources))

(defun physical-bootstrap-check-export-slot-under-wal-lock (slot name checkpoint-start)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "native backup export validation requires WAL exclusion"))
  (wal-check-healthy)
  (wal-require-usable-replication-slot slot)
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

(declaim (ftype function physical-backup-lease-create-pin))

(defun physical-bootstrap-copy-under-quiescence (target pin-name &key export-slot lease-request)
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
  (when (or (and export-slot lease-request)
            (and lease-request (not (equal pin-name (getf lease-request :name)))))
    (wal-invalid-input "physical backup has inconsistent retention owners"))
  (let ((policy (authentication-capture-policy)))
    (wal-checkpoint-core)
    (physical-recovery-check-checkpoint-resources *wal-checkpoint-snapshot* *wal-control-metadata*)
    (let ((pin (unless export-slot
                 (if lease-request (physical-backup-lease-create-pin lease-request)
                     (physical-source-pin-create pin-name)))))
      (when export-slot
        (physical-bootstrap-check-export-slot export-slot pin-name (wal-checkpoint-record-start)))
      ;; Local backup failure retains its private pin for operator inspection.
      ;; Leased copies keep their pin after capture; checked lease retirement removes it.
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
        (let ((image (physical-bootstrap-manifest-image manifest)))
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
  "Produce and verify an export under a temporary WAL slot and origin reservation.
The slot is dropped on every exit; a verified origin remains after cancellation."
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

(defun physical-backup-reuse-path-p (name)
  (and (stringp name) (uiop:string-prefix-p "base/" name)
       (recovery-manifest-path-p name)))

(defun rewind-readable-file-p (name)
  ;; Catalog, WAL and configuration files can contain credentials. Native
  ;; rewind transfers its sealed image over the physical replication path.
  (or (member name '("PG_VERSION" "global/pg_control" "pg_xact") :test #'equal)
      (physical-backup-reuse-path-p name)))

(defun require-rewind-function-grant (function)
  (let* ((user (backend-startup-user *backend*))
         (role (authentication-committed-role user)))
    (unless (if role
                (or (pg-role-superuser-p role) (member function (pg-role-function-grants role)))
                (string= user "postgres"))
      (ereport "42501" "permission denied for rewind file function"))))

(defun call-with-rewind-file-image (function)
  (let ((fd nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (sb-posix:open
                       (string-right-trim "/" (namestring *pgdata*))
                       (logior sb-posix:o-rdonly sb-posix:o-directory
                               sb-posix:o-nofollow +postmaster-o-cloexec+)))
             (sb-sys:with-local-interrupts
               (call-with-physical-image-directory fd function :primary)))
        (when fd (sb-posix:close fd))))))

(defun rewind-readable-directory-p (name)
  (member name '("." "base" "global") :test #'equal))

(defun rewind-image-directory-fd (name)
  (if (string= name ".")
      (physical-image-context-root-fd *physical-image-context*)
      (cdr (assoc name (physical-image-context-children *physical-image-context*) :test #'equal))))

(defun sql-rewind-stat-file (name missing-ok)
  (require-rewind-function-grant :stat-file)
  (when (or (eq name +null+) (eq missing-ok +null+)) (return-from sql-rewind-stat-file +null+))
  (unless (or (rewind-readable-directory-p name) (rewind-readable-file-p name))
    (ereport "42501" "file is outside the rewind read scope"))
  (let ((stat
          (call-with-rewind-file-image
           (lambda (directory)
             (if (rewind-readable-directory-p name)
                 (sb-posix:fstat (rewind-image-directory-fd name))
                 (with-physical-image-input (stream (merge-pathnames name directory)
                                                    :element-type '(unsigned-byte 8)
                                                    :if-does-not-exist nil)
                   (when stream (sb-posix:fstat (sb-sys:fd-stream-fd stream)))))))))
    (unless stat
      (if (eq missing-ok t) (return-from sql-rewind-stat-file +null+)
          (ereport "58P01" "rewind source file does not exist")))
    (flet ((timestamp (seconds) (* (- seconds 946684800) 1000000)))
      (make-sql-record :fields +rewind-stat-file-result-fields+
                       :values (vector (sb-posix:stat-size stat)
                                       (timestamp (sb-posix:stat-atime stat))
                                       (timestamp (sb-posix:stat-mtime stat))
                                       (timestamp (sb-posix:stat-ctime stat))
                                       +null+
                                       (not (null (sb-posix:s-isdir (sb-posix:stat-mode stat)))))))))

(defun make-rewind-directory-result-iterator (name missing-ok include-dot-dirs)
  (require-rewind-function-grant :ls-dir)
  (when (member +null+ (list name missing-ok include-dot-dirs))
    (return-from make-rewind-directory-result-iterator (lambda () (values nil nil))))
  (unless (rewind-readable-directory-p name)
    (ereport "42501" "directory is outside the rewind read scope"))
  (let ((entries
          (call-with-rewind-file-image
           (lambda (root)
             (declare (ignore root))
             (let* ((root-p (string= name "."))
                    (path (physical-image-fd-path (rewind-image-directory-fd name)))
                    (names (directory-entry-names path :follow-symlinks t
                                                       :maximum-entries 100000
                                                       :maximum-name-bytes (* 16 1024 1024))))
               (nconc (when (eq include-dot-dirs t) (list "." ".."))
                      (remove-if-not
                       (lambda (leaf)
                         (if root-p
                             (or (rewind-readable-directory-p leaf) (rewind-readable-file-p leaf))
                             (rewind-readable-file-p (concatenate 'string name "/" leaf))))
                       names)))))))
    (lambda ()
      (when entries
        (values (pop entries) t)))))

(defun sql-rewind-read-binary-file (name &optional (offset 0 ranged-p) length missing-ok)
  (require-rewind-function-grant (if ranged-p :read-binary-file-range :read-binary-file))
  (when (member +null+ (list name offset length missing-ok))
    (return-from sql-rewind-read-binary-file +null+))
  (unless (rewind-readable-file-p name)
    (ereport "42501" "file is outside the rewind read scope"))
  (when (and ranged-p (minusp length))
    (ereport "22023" "requested length cannot be negative"))
  (call-with-rewind-file-image
   (lambda (directory)
     (with-physical-image-input (stream (merge-pathnames name directory)
                                        :element-type '(unsigned-byte 8) :if-does-not-exist nil)
       (unless stream
         (if missing-ok (return-from sql-rewind-read-binary-file +null+)
             (ereport "58P01" "rewind source file does not exist")))
       (let* ((size (sb-posix:stat-size (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
              (start (if (minusp offset) (+ size offset) offset))
              (count (min (or length size) (max 0 (- size start)))))
         (when (minusp start)
           (ereport "22023" "requested offset precedes the start of the file"))
         (when (> count (* 16 1024 1024))
           (ereport "54000" "rewind file read exceeds the 16 MiB limit"))
         (let ((result (make-array count :element-type '(unsigned-byte 8))))
           (unless (or (zerop count) (file-position stream start))
             (ereport "58030" "could not seek rewind source file"))
           (let ((read (read-sequence result stream)))
             (if (= read count) result (subseq result 0 read)))))))))

(defun physical-backup-file-header-image (entry tag)
  (destructuring-bind (name size digest) entry
    (let ((path (string->bytes name))
          (header (make-payload)))
      (unless (<= 1 (length path) 512) (error "native backup bundle path exceeds its bound"))
      (payload-push-byte header (char-code tag))
      (payload-push-u32 header (length path))
      (loop for byte across path do (payload-push-byte header byte))
      (replication-push-u64 header size)
      (replication-push-u64 header digest)
      header)))

(defun physical-backup-page-reference (offset digest)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\P))
    (replication-push-u64 payload offset)
    (replication-push-u64 payload digest)
    payload))

(defun physical-backup-file-data (buffer count)
  (let ((payload (make-array (1+ count) :element-type '(unsigned-byte 8))))
    (setf (aref payload 0) (char-code #\D))
    (replace payload buffer :start1 1 :end2 count)
    payload))

(defun physical-backup-send-file (directory entry timeline emit &key pages)
  "Send a checked bundle file as a header, data frames and an end frame.
Compare each offered page digest with the source page before sending a reference.
Send source bytes on mismatch. Hash all source bytes and check the complete file
length and digest before sending the end frame."
  (destructuring-bind (name size expected-digest) entry
    (unless (and (physical-backup-bundle-path-p name timeline)
                 (typep size '(unsigned-byte 64))
                 (typep expected-digest '(unsigned-byte 64)))
      (error "invalid native backup bundle file"))
    (let ((header (physical-backup-file-header-image entry #\F))
          (digest #xCBF29CE484222325)
          (buffer (make-array 65535 :element-type '(unsigned-byte 8))))
      (multiple-value-bind (input actual-size)
          (open-bounded-regular-file (pgdata-resolve-path name directory) size)
        (unwind-protect
             (progn
               (unless (= size actual-size) (error "native backup bundle file length changed"))
               (funcall emit header)
               (loop with remaining = size
                     with offsets = (when pages
                                      (sort (loop for offset being the hash-keys of pages collect offset) #'<))
                     while (plusp remaining)
                     for offset = (- size remaining)
                     for offered = (when (eql offset (first offsets))
                                     (gethash (pop offsets) pages))
                     for extent = (cond (offered +blcksz+)
                                        (offsets (- (first offsets) offset))
                                        (t (length buffer)))
                     for count = (read-sequence buffer input :end (min remaining extent (length buffer)))
                     do (when (zerop count) (error "native backup bundle file was truncated"))
                        (setf digest (physical-store-update-digest-span digest buffer 0 count))
                        (let ((hash (and offered (= count +blcksz+)
                                         (physical-store-update-digest-span
                                          #xCBF29CE484222325 buffer 0 count))))
                          (funcall emit (if (and hash (= offered hash))
                                            (physical-backup-page-reference offset hash)
                                            (physical-backup-file-data buffer count))))
                        (decf remaining count))
               (unless (and (eq :eof (read-byte input nil :eof)) (= digest expected-digest))
                 (error "native backup bundle file changed"))
               (let ((end (make-payload)))
                 (payload-push-byte end (char-code #\E))
                 (funcall emit end)))
          (close input))))))

(defun physical-backup-metadata-bounds ()
  "Return metadata size limits, including the shared recovery control maximum."
  `(("physical-backup.manifest" 16777216)
    ("recovery.required" 1024)
    ("physical-recovery.control" ,+physical-recovery-control-maximum-bytes+)
    ("pg_hba.conf" 1048576)
    ("pg_ident.conf" 1048576)
    ("authentication.initialized" 1024)))

(defun physical-backup-bundle-entries (directory manifest &key manifest-digest)
  (recovery-check-manifest-schema manifest)
  (when (get-properties manifest '(:logical-slot-copy-id))
    (error "ordinary native backup bundles cannot carry logical slot copy adjuncts"))
  (append
   (loop for (name maximum) in (physical-backup-metadata-bounds)
         for bytes = (read-bounded-regular-file (merge-pathnames name directory) maximum)
         for digest = (if (equal name "physical-backup.manifest")
                          (multiple-value-bind (actual hash) (recovery-decode-manifest bytes)
                            (unless (and (equal manifest actual)
                                         (or (null manifest-digest) (= manifest-digest hash)))
                              (error "native backup bundle manifest changed"))
                            hash)
                          (physical-store-update-digest #xCBF29CE484222325 bytes))
         collect (list name (length bytes) digest))
   (getf manifest :files)))

;; Send B (bundle header), F/D/E (file header, data and end), then Z
;; (count and checksum).  The checksum covers all preceding payloads, including
;; file checksums; it detects corruption and is not an authentication mechanism.
(defun physical-backup-send-bundle (directory manifest emit &key reuse expected-entries pages)
  (let* ((entries (physical-backup-bundle-entries directory manifest))
         (digest #xCBF29CE484222325))
    (when (and expected-entries (not (equal expected-entries entries)))
      (error "native backup bundle changed after capture verification"))
    (flet ((send (payload)
             (setf digest (physical-store-update-digest digest payload))
             (funcall emit payload)))
      (let ((header (make-payload)))
        (payload-push-byte header (char-code #\B))
        (loop for byte across (string->bytes "PGNATIVEBACKUP1") do (payload-push-byte header byte))
        (payload-push-u32 header (length entries))
        (send header))
      (dolist (entry entries)
        (if (and reuse (physical-backup-reuse-path-p (first entry)) (funcall reuse entry))
            (send (physical-backup-file-header-image entry #\R))
            (physical-backup-send-file directory entry (getf manifest :timeline) #'send
                                       :pages (and pages (gethash (first entry) pages))))))
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

(defun physical-backup-file-header (header name maximum &optional expected (tag #\F))
  (unless (and (>= (length header) 21) (= (aref header 0) (char-code tag)))
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

;; Consume exactly the declared data extent and its end frame while checking
;; the digest; the caller owns the output stream and its durability.

(defun physical-backup-read-page-reference (frame offset remaining reader)
  "Read an offered local page and verify its length and digest against the reference."
  (unless (and reader (= (length frame) 17) (zerop (mod offset +blcksz+))
               (<= +blcksz+ remaining) (= offset (replication-read-u64 frame 1)))
    (error "invalid native backup page reference"))
  (let* ((digest (replication-read-u64 frame 9))
         (page (funcall reader offset digest)))
    (unless (and (typep page '(vector (unsigned-byte 8)))
                 (= (length page) +blcksz+)
                 (= digest (physical-store-update-digest #xCBF29CE484222325 page)))
      (error "native backup page reference disagrees with the target"))
    page))

(defun physical-backup-receive-file-content (size expected-digest next output &optional relative page-reader)
  "Receive file data and return its digest after checking the end frame.
Validate each data extent or page reference before writing it. Hash the assembled
bytes against the sealed file digest. The caller checks the header and owns output."
  (let ((digest #xCBF29CE484222325))
    (loop with remaining = size
          while (plusp remaining)
          for frame = (physical-backup-read-frame next)
          do (multiple-value-bind (bytes start)
                 (case (code-char (aref frame 0))
                   (#\D (values frame 1))
                   (#\P (values (physical-backup-read-page-reference
                                 frame (- size remaining) remaining page-reader) 0))
                   (otherwise (error "invalid native backup file data extent")))
               (let ((count (- (length bytes) start)))
                 (unless (<= 1 count remaining) (error "invalid native backup file data extent"))
                 (setf digest (physical-store-update-digest-span digest bytes start (length bytes)))
                 (write-sequence bytes output :start start)
                 (when (and relative *physical-image-test-hook* (< count remaining))
                   (finish-output output)
                   (funcall *physical-image-test-hook* :after-partial-receive relative))
                 (decf remaining count))))
    (let ((end (physical-backup-read-frame next)))
      (unless (and (= (length end) 1) (= (aref end 0) (char-code #\E))
                   (= expected-digest digest))
        (error "native backup file end or checksum is invalid")))
    digest))

;; Descriptor-scoped receipt creates relative to held directories; the legacy
;; path retains its existing pathname-based file creation and sync behavior.

(defun call-with-private-backup-file-output (path function)
  (sb-sys:without-interrupts
    (let ((fd nil) (stream nil))
      (unwind-protect
           (progn
             (setf fd (sb-posix:open (namestring path)
                                     (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                             sb-posix:o-nofollow +postmaster-o-cloexec+)
                                     #o600))
             (sb-posix:fchmod fd #o600)
             (setf stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)))
             (sb-sys:with-local-interrupts (funcall function stream)))
        (if stream (close stream) (when fd (sb-posix:close fd)))))))

(defun physical-backup-receive-file (directory name maximum next &key expected (timeline 1) reuse read-page)
  (unless (physical-backup-bundle-path-p name timeline)
    (error "invalid native backup destination path"))
  (let ((header (physical-backup-read-frame next)))
    (when (= (aref header 0) (char-code #\R))
      (unless (and reuse expected (physical-backup-reuse-path-p name))
        (error "unexpected native backup relation reuse"))
      (physical-image-require-output-context directory)
      (physical-backup-file-header header name maximum expected #\R)
      (funcall reuse expected)
      (return-from physical-backup-receive-file expected))
    (multiple-value-bind (size expected-digest)
        (physical-backup-file-header header name maximum expected)
      (let ((digest
              (if *physical-image-context*
                  (call-with-physical-image-output
                   directory name
                   (lambda (output)
                     (physical-backup-receive-file-content
                      size expected-digest next output name
                      (when (and expected read-page (physical-backup-reuse-path-p name))
                        (lambda (offset digest) (funcall read-page name offset digest))))))
                  (let* ((path (merge-pathnames name directory))
                         (parent (uiop:pathname-directory-pathname path)))
                    (wal-ensure-directory-durable parent)
                    (flet ((receive (output)
                             (multiple-value-prog1
                                 (physical-backup-receive-file-content size expected-digest next output)
                               (fsync-stream output))))
                      (prog1 (if (physical-backup-private-file-p name)
                                 (call-with-private-backup-file-output path #'receive)
                                 (with-open-file (output path :direction :output :if-exists :error
                                                              :if-does-not-exist :create :element-type '(unsigned-byte 8))
                                   (receive output)))
                        (fsync-directory parent)))))))
        (list name size digest)))))

;; The descriptor route checks all received names, including metadata, against
;; the manifest.  This snapshot is not a substitute for an installation check.

;; Reseed installation mode requires a descriptor scope and admits only the two
;; auxiliary guard names at the root.  Child inventories remain strict.
;; A future caller must check guard contents and identities before and after
;; image verification.
(defun physical-backup-check-received-inventory (directory manifest &key reseed-installation)
  (unless (member reseed-installation '(nil t)) (error "invalid reseed installation inventory mode"))
  (when (and reseed-installation (null *physical-image-context*))
    (error "guarded reseed inventory requires its descriptor scope"))
  (when *physical-image-context*
    (physical-image-check-inventory
     directory (append (mapcar #'first (physical-backup-metadata-bounds))
                       (mapcar #'first (getf manifest :files))
                       (when reseed-installation '(".pg_lisp.lock" "physical-reseed.install"))))))

;; Bound the file count and metadata sizes before receiving the manifest and
;; its inventory.  Check order, paths, sizes and checksums, then verify the
;; immutable recovery image and copied authentication policy before publication.
(defun physical-backup-receive-bundle (directory next &key reuse read-page)
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
            (when (get-properties manifest '(:logical-slot-copy-id))
              (error "ordinary native backup bundles cannot receive logical slot copy adjuncts"))
            (unless (and (eql 3 (getf manifest :version))
                         (= count (+ 6 (length (getf manifest :files)))))
              (error "native backup bundle inventory disagrees with its manifest"))
            (dolist (entry (rest (physical-backup-metadata-bounds)))
              (physical-backup-receive-file directory (first entry) (second entry) #'receive-frame))
            (dolist (entry (getf manifest :files))
              (physical-backup-receive-file directory (first entry) (second entry) #'receive-frame
                                            :expected entry :timeline (getf manifest :timeline)
                                            :reuse reuse :read-page read-page))
            (let ((end (physical-backup-read-frame next)))
              (unless (and (= 13 (length end)) (= (aref end 0) (char-code #\Z))
                           (= count (bytes-to-u32 end 1)) (= digest (replication-read-u64 end 5)))
                (error "native backup bundle completion or checksum is invalid")))
            (let ((*pgdata* directory))
              (unless (zerop (getf (physical-recovery-read-control directory) :phase))
                (error "native backup bundle is not an immutable base image"))
              (physical-recovery-verify-base-state)
              (authentication-check-backup-policy directory)
              (physical-backup-check-received-inventory directory manifest))
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

(defun physical-backup-next-copy-frame (channel &optional (tag "BASE_BACKUP_NATIVE"))
  (loop
    (multiple-value-bind (kind bytes)
        (physical-libpq-receive-copy-out channel tag :maximum 65536)
      (case kind
        (#\D (return bytes))
        (#\C (error "native backup COPY completed before its bundle"))
        (#\I nil)))))

(defun physical-backup-finish-copy (channel &optional (tag "BASE_BACKUP_NATIVE"))
  (loop
    (multiple-value-bind (kind bytes)
        (physical-libpq-receive-copy-out channel tag :maximum 65536)
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
