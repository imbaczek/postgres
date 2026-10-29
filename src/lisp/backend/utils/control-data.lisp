;;;; Native control inspection and primary lifecycle publication.
;;;;
;;;; The server records primary state in pg_wal_control. A standby instead uses
;;;; its recovery control and the checkpoint in its selected restartpoint.
;;;; A committed child control takes precedence over retired recovery files.
;;;;
;;;; Inspection never starts storage or repairs files. It validates a stable
;;;; set of controls and reports the native facts with pg_controldata's labels.
;;;; See C pg_controldata.c and xlog.c for the output and lifecycle ordering.

(in-package #:pg)

(defvar *native-control-test-hook* nil)

(defun native-control-test-point (point)
  "Call the optional test hook at a control publication boundary."
  (when *native-control-test-hook* (funcall *native-control-test-hook* point)))

(defun wal-mark-primary-startup ()
  "Mark an existing primary unclean before storage startup can change files.
Retain its checkpoint and identity. Legacy controls have no lifecycle state;
leave that state unknown until successful initialization publishes production."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-id namespace metadata)
      (wal-read-control-fields)
    (when metadata
      (wal-require-supported-timelines insertion checkpoint-id)
      (wal-atomic-write-bytes
       (wal-control-path)
       (wal-encode-control-image
        checkpoint durable snapshot :timeline insertion :checkpoint-timeline checkpoint-id
        :namespace namespace
        :metadata (wal-refresh-control-metadata metadata +db-in-crash-recovery+)))))
  (native-control-test-point :after-primary-startup-dirty)
  (values))

(defun wal-set-primary-state (state)
  "Publish STATE with the initialized catalog's system identifier.
Hold the WAL lock across publication. Install the in-memory metadata only after
the durable control write succeeds; an existing identity must match the catalog."
  (prevent-recovery-primary-operation "primary control publication")
  (unless (and *wal-ready* *catalog*) (error "primary storage has not been initialized"))
  (let ((system-id (catalog-system-identifier)))
    (when (and *wal-control-metadata*
               (/= system-id (getf *wal-control-metadata* :system-id)))
      (error "primary control and catalog system identifiers differ"))
    (with-wal-critical
      (let ((*wal-control-metadata* (if *wal-control-metadata*
                                        (wal-refresh-control-metadata *wal-control-metadata* state)
                                        (make-wal-control-metadata system-id state))))
        (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*))
      (setf *wal-control-metadata*
            (nth-value 6 (wal-read-control-fields)))))
  (values))

(defun wal-note-primary-ready ()
  "Publish production after primary initialization and promotion cleanup.
The caller must finish those steps before calling this function."
  (unless (and *wal-control-metadata*
               (= +db-in-production+ (getf *wal-control-metadata* :db-state))
               (= (catalog-system-identifier) (getf *wal-control-metadata* :system-id)))
    (wal-set-primary-state +db-in-production+))
  (values))

(defun prepare-primary-shutdown ()
  "Finish smart or fast shutdown in the owning postmaster, outside admission.
Clients must already be gone. Serialize with reload, close admission permanently,
retire abandoned transactions, then sync a checkpoint before recording clean
shutdown. An unexpected active slot owner leaves recycling pending and prevents
clean shutdown publication. An error keeps admission closed. Do not roll back a
possibly published clean state."
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               *shutdown-finalizing-p* (zerop *live-operation-depth*))
    (error "primary shutdown requires the owning unadmitted postmaster"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (fence-live-operation-shutdown)
    (wal-set-primary-state +db-shutdowning+)
    (native-control-test-point :after-primary-shutdown-dirty)
    (postmaster-retire-abandoned-backends)
    (wal-checkpoint)
    (sb-thread:with-mutex (*wal-lock*)
      (when *wal-slot-recycling-pending*
        (error "primary shutdown left an active replication slot owner")))
    (native-control-test-point :after-primary-shutdown-checkpoint)
    (wal-set-primary-state +db-shutdowned+)
    (native-control-test-point :after-primary-shutdown-clean))
  (values))

(defun native-control-optional-bytes (path)
  "Return bounded regular-file bytes, or NIL only when PATH is absent.
A present file with invalid type, size or access permissions is an error."
  (handler-case (physical-promotion-read-file path)
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition)) (error condition)))))

(defun native-control-recovery-role-p (control insertion)
  "Return true when CONTROL still selects recovery on INSERTION's timeline.
Phases 0 through 2 are ordinary recovery; phase 3 remains pending only while
insertion is on the recorded parent timeline."
  (and control
       (or (member (getf control :phase) '(0 1 2))
           (and (eql 3 (getf control :phase))
                (eql insertion (getf control :timeline))))))

(defun native-control-capture ()
  "Read the controls that select the current native role and checkpoint.
Keep their bytes for the stability check. Capture recovery artifacts for an
ordinary standby, including one on a child timeline, or a pending promotion.
With no recovery control, capture artifacts so incomplete recovery is rejected.
A committed child with pending control ignores retired recovery artifacts.
Return WAL control, recovery control, marker, manifest, restartpoint pointer,
current child history and follow control, in that order."
  (let* ((wal (native-control-optional-bytes (wal-control-path)))
         (insertion (and wal (nth-value 3 (wal-decode-control-line (bytes->string wal)))))
         (recovery (native-control-optional-bytes (physical-recovery-control-path)))
         (control (when recovery (physical-recovery-decode-control recovery)))
         (pending (native-control-recovery-role-p control insertion))
         (parent (or pending (null control) (and insertion (= insertion 1)))))
    (unless wal (error "native WAL control file is missing"))
    (list wal recovery
          (when parent (native-control-optional-bytes (merge-pathnames "recovery.required" *pgdata*)))
          (when parent (native-control-optional-bytes (merge-pathnames "physical-backup.manifest" *pgdata*)))
          (when parent
            (native-control-optional-bytes (physical-restartpoint-pointer-path)))
          (when (> insertion 1)
            (native-control-optional-bytes (timeline-history-path insertion *pgdata*)))
          (when parent
            (native-control-optional-bytes (merge-pathnames "physical-follow.control" *pgdata*))))))

(defun native-control-catalog-system-id ()
  "Recover a legacy primary's identifier from its saved catalog identity.
Read the catalog without initialization or reader evaluation. Its size bound
matches the supported restartpoint generation bound rather than a WAL segment."
  (let* ((bytes (read-bounded-regular-file (catalog-path) (ash 1 30) :minimum 1))
         (image (physical-history-read-catalog bytes))
         (identity (getf image :cluster-identity)))
    (unless (and (stringp identity) (plusp (length identity)))
      (error "native catalog lacks a durable system identity"))
    (catalog-system-identifier (make-catalog :cluster-identity identity))))

(defun native-control-manifest-file (manifest name maximum)
  "Read NAME from the immutable base after checking its manifest entry.
Reject a missing entry or a length or digest mismatch; bound the read by MAXIMUM."
  (let* ((entry (find name (getf manifest :files) :key #'first :test #'equal))
         (bytes (read-bounded-regular-file (merge-pathnames name *pgdata*) maximum)))
    (unless (and entry (= (second entry) (length bytes))
                 (= (third entry) (physical-store-update-digest #xCBF29CE484222325 bytes)))
      (error "native base file differs from its manifest: ~a" name))
    bytes))

(defun native-control-check-recovery-base (control manifest snapshot)
  "Check that the immutable base control and SNAPSHOT agree with MANIFEST.
For a child base, also authenticate its history file and check child control
against that history. An ordinary base cannot carry an old primary's retained
archive reference; pending promotion may retain one."
  (let ((bytes (native-control-manifest-file manifest "global/pg_wal_control" 513)))
    (multiple-value-bind (checkpoint durable actual insertion checkpoint-timeline namespace metadata)
        (wal-decode-control-line (bytes->string bytes))
      (unless (and (= checkpoint (getf manifest :checkpoint))
                   (= durable (getf manifest :end))
                   (= insertion checkpoint-timeline (getf manifest :timeline))
                   (or (= insertion 1) (eq namespace :timeline))
                   (equal snapshot actual)
                   (= (physical-manifest-checkpoint-start manifest)
                      (if actual (getf actual :start) (- checkpoint +wal-header-size+)))
                   (or (eql 3 (getf control :phase)) (null (getf metadata :retained-reference))))
        (error "native recovery control differs from its immutable base checkpoint"))
      (when (> insertion 1)
        (native-control-manifest-file
         manifest (concatenate 'string "pg_wal/" (timeline-history-filename insertion))
         (* 16 1024 1024))
        (physical-promotion-check-child-control insertion checkpoint-timeline))))
  (values))

(defun native-control-recovery-checkpoint (control snapshot)
  "Select the authenticated source checkpoint for recovery control.
Ordinary recovery may use the base copy. Pending promotion requires its exact
version 4 generation, applied position and next XID, and validates any retained
archive without registering it or changing files."
  (multiple-value-bind (manifest digest) (recovery-read-manifest *pgdata*)
    (recovery-check-manifest-schema manifest)
    (unless (and (every (lambda (key) (eql (getf control key) (getf manifest key)))
                        '(:system-id :timeline :checkpoint))
                 (or (eql 0 (getf control :phase)) (= digest (getf control :manifest-digest))))
      (error "recovery control differs from its base manifest"))
    (native-control-check-recovery-base control manifest snapshot)
    (setf (getf manifest :manifest-digest) digest)
    (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read manifest)
      (declare (ignore catalog prefix))
      (when (eql 3 (getf control :phase))
        (unless (and metadata (eql 4 (getf metadata :version))
                     (eql generation (getf control :promotion-generation))
                     (eql (getf metadata :lsn) (getf control :applied))
                     (eql (getf metadata :next-xid) (getf control :next-xid)))
          (error "pending control differs from its selected restartpoint"))
        (physical-promotion-check-pending-retained control metadata))
      (when (and metadata (> (getf metadata :lsn) (getf control :applied)))
        (error "restartpoint is ahead of durable recovery control"))
      (or (getf metadata :source-checkpoint) snapshot))))

(defun native-control-facts (capture)
  "Validate CAPTURE and return the facts used by native pg_controldata.
Captured recovery artifacts without recovery control are an error. A standby
uses recovery control for its state and applied floor, even on a child timeline.
Phase 0 has not yet bound the manifest digest. A phase-3 request remains pending
when insertion equals its parent timeline. A committed child must agree with its
history and ignores retired recovery files. Report checkpoint starts, though WAL
control stores ends."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-id namespace metadata)
      (wal-decode-control-line (bytes->string (first capture)))
    (declare (ignore durable namespace))
    (let* ((control (when (second capture) (physical-recovery-decode-control (second capture))))
           (system-id (or (getf metadata :system-id)
                          (getf control :system-id)
                          (native-control-catalog-system-id)))
           (state (getf metadata :db-state))
           (time (getf metadata :state-time))
           (recovery-lsn 0) (recovery-timeline 0))
      (when (and (null control)
                 (or (third capture) (fourth capture) (fifth capture) (seventh capture)))
        (error "native recovery metadata is incomplete: recovery control is missing"))
      (cond
        ((and (or (> insertion 1) (getf metadata :retained-reference))
              (not (native-control-recovery-role-p control insertion)))
         (multiple-value-bind (checked history)
             (physical-promotion-check-child-control insertion checkpoint-id)
           (declare (ignore checked))
           (physical-promotion-check-retiring-control snapshot history system-id)))
        (control
         (unless (and (third capture)
                      (equalp (third capture) (physical-recovery-marker-bytes)))
           (error "recovery control lacks its required marker"))
         (when (and metadata (/= system-id (getf control :system-id)))
           (error "recovery and base control system identifiers differ"))
         (setf snapshot (native-control-recovery-checkpoint control snapshot)
               state (getf control :db-state) time (getf control :state-time)
               system-id (getf control :system-id)
               recovery-lsn (getf control :applied)
               recovery-timeline (getf control :applied-timeline (getf control :timeline)))))
      (list :system-id system-id :db-state state :state-time time
            :checkpoint (if snapshot (getf snapshot :start)
                            (max 0 (- checkpoint +wal-header-size+)))
            :checkpoint-timeline (if snapshot (getf snapshot :timeline) checkpoint-id) :snapshot snapshot
            :recovery-lsn recovery-lsn :recovery-timeline recovery-timeline))))

(defun native-control-read (directory)
  "Read coherent control facts without starting storage or changing PGDATA.
Compare role controls and child history before and after validation, retrying
when publication changed them. Immutable generations are bound by the selected
pointer and digest. Phase 0 may leave the control's manifest digest unbound;
the base files are still checked against the manifest read from disk."
  (let ((*pgdata* (uiop:ensure-directory-pathname (truename directory))))
    (loop repeat 8
          for capture = (native-control-capture)
          do (handler-case
                 (let ((facts (native-control-facts capture)))
                   (when (equalp capture (native-control-capture))
                     (return-from native-control-read facts)))
               (error (condition)
                 (when (equalp capture (native-control-capture)) (error condition))))
             (sleep 0.01))
    (error "native control files changed throughout inspection")))

(defun native-control-time-text (unix-time)
  "Format Unix seconds in local time, or return ??? for an unsupported time."
  (handler-case
      (multiple-value-bind (second minute hour day month year weekday)
          (decode-universal-time (+ unix-time 2208988800))
        (format nil "~a ~a ~2d ~2,'0d:~2,'0d:~2,'0d ~4d"
                (aref #("Mon" "Tue" "Wed" "Thu" "Fri" "Sat" "Sun") weekday)
                (aref #("Jan" "Feb" "Mar" "Apr" "May" "Jun" "Jul" "Aug" "Sep" "Oct" "Nov" "Dec") (1- month))
                day hour minute second year))
    (error () "???")))

(defun native-control-data-rows (facts)
  "Return supported native facts under C pg_controldata's field names.
Omit fields the native format does not establish, including rewind settings
and checksum versions. A legacy primary's unknown lifecycle stays unknown."
  (let* ((snapshot (getf facts :snapshot))
         (timeline (or (getf snapshot :timeline) (getf facts :checkpoint-timeline)))
         (redo (or (getf snapshot :redo) (getf facts :checkpoint)))
         (state (getf facts :db-state)) (rows nil))
    (flet ((add (name value) (push (cons name (princ-to-string value)) rows)))
      (add "Database system identifier" (getf facts :system-id))
      (add "Database cluster state"
           (if (typep state '(integer 0 6))
               (aref #("starting up" "shut down" "shut down in recovery"
                       "shutting down" "in crash recovery" "in archive recovery"
                       "in production") state)
               "unrecognized status code"))
      (when (getf facts :state-time)
        (add "pg_control last modified" (native-control-time-text (getf facts :state-time))))
      (add "Latest checkpoint location" (wal-format-lsn (getf facts :checkpoint)))
      (add "Latest checkpoint's REDO location" (wal-format-lsn redo))
      (add "Latest checkpoint's REDO WAL file"
           (subseq (wal-segment-filename (floor redo +wal-segment-size+) timeline :timeline) 0 24))
      (add "Latest checkpoint's TimeLineID" timeline)
      (add "Latest checkpoint's PrevTimeLineID" (or (getf snapshot :prev-timeline) timeline))
      (when snapshot
        (let ((next (getf snapshot :next-xid)))
          (add "Latest checkpoint's NextXID"
               (if (= next #x10000000000000000) "exhausted"
                   (format nil "~d:~d" (ldb (byte 32 32) next) (ldb (byte 32 0) next)))))
        (when (getf snapshot :next-oid)
          (add "Latest checkpoint's NextOID" (getf snapshot :next-oid)))
        (add "Latest checkpoint's oldestActiveXID"
             (ldb (byte 32 0) (getf snapshot :oldest-active-xid)))
        (add "Time of latest checkpoint" (native-control-time-text (getf snapshot :time))))
      (add "Minimum recovery ending location" (wal-format-lsn (getf facts :recovery-lsn)))
      (add "Min recovery ending loc's timeline" (getf facts :recovery-timeline))
      (add "Database block size" +blcksz+)
      (add "Bytes per WAL segment" +wal-segment-size+))
    (nreverse rows)))

(defun native-controldata-directory (arguments)
  "Resolve the supported -D options, positional directory and PGDATA fallback."
  (let ((directory nil) (operands nil) (options t))
    (loop while arguments
          for argument = (pop arguments)
          do (cond
               ((and options (string= argument "--")) (setf options nil))
               ((and options (member argument '("-D" "--pgdata") :test #'string=))
                (unless arguments (error "~a requires a data directory" argument))
                (setf directory (pop arguments)))
               ((and options (uiop:string-prefix-p "--pgdata=" argument))
                (setf directory (subseq argument 9)))
               ((and options (uiop:string-prefix-p "-D" argument))
                (setf directory (subseq argument 2)))
               ((and options (plusp (length argument)) (char= #\- (char argument 0)))
                (error "unrecognized option ~a" argument))
               (t (push argument operands))))
    (when operands
      (when (or directory (> (length operands) 1)) (error "too many command-line arguments"))
      (setf directory (first operands)))
    (or directory (uiop:getenv "PGDATA") (error "no data directory specified"))))

(defun native-controldata-main (arguments &optional (output *standard-output*) (errors *error-output*))
  "Print native control data and return a process exit code.
Inspection must not initialize or repair the cluster."
  (handler-case
      (cond
        ((member (first arguments) '("--help" "-?") :test #'equal)
         (format output "Usage: pg_controldata [OPTION] [DATADIR]~%~
                         Options: -D, --pgdata=DATADIR; -V, --version; -?, --help~%")
         0)
        ((member (first arguments) '("--version" "-V") :test #'equal)
         (format output "pg_controldata (pg-lisp native) ~a~%" *server-version*)
         0)
        (t
         (let ((rows (native-control-data-rows
                      (native-control-read (native-controldata-directory arguments)))))
           (dolist (row rows) (format output "~a:~vt~a~%" (car row) 38 (cdr row))))
         0))
    (error (condition)
      (format errors "pg_controldata: ~a~%" condition)
      1)))
