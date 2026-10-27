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
shutdown. An error keeps admission closed. Do not roll back a possibly
published clean state."
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               *shutdown-finalizing-p* (zerop *live-operation-depth*))
    (error "primary shutdown requires the owning unadmitted postmaster"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (fence-live-operation-shutdown)
    (wal-set-primary-state +db-shutdowning+)
    (native-control-test-point :after-primary-shutdown-dirty)
    (postmaster-retire-abandoned-backends)
    (wal-checkpoint)
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

(defun native-control-capture ()
  "Read the controls that select the current native role and checkpoint.
Keep their bytes for the stability check. A committed child no longer depends
on the old base manifest or restartpoint pointer."
  (let* ((wal (native-control-optional-bytes (wal-control-path)))
         (insertion (and wal (nth-value 3 (wal-decode-control-line (bytes->string wal)))))
         (recovery (native-control-optional-bytes (physical-recovery-control-path)))
         (parent (and insertion (= insertion 1))))
    (unless wal (error "native WAL control file is missing"))
    (list wal recovery
          (when parent (native-control-optional-bytes (merge-pathnames "recovery.required" *pgdata*)))
          (when parent (native-control-optional-bytes (merge-pathnames "physical-backup.manifest" *pgdata*)))
          (when (and parent recovery)
            (native-control-optional-bytes (physical-restartpoint-pointer-path))))))

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

(defun native-control-recovery-checkpoint (control snapshot)
  "Select the latest authenticated source checkpoint for a standby.
Verify the base manifest and selected generation, and reject a restartpoint
ahead of durable applied progress. Use the base copy when no newer copy exists."
  (multiple-value-bind (manifest digest) (recovery-read-manifest *pgdata*)
    (recovery-check-manifest-schema manifest)
    (unless (and (every (lambda (key) (eql (getf control key) (getf manifest key)))
                        '(:system-id :timeline :checkpoint))
                 (= digest (getf control :manifest-digest)))
      (error "recovery control differs from its base manifest"))
    (setf (getf manifest :manifest-digest) digest)
    (let ((metadata (physical-restartpoint-read manifest)))
      (when (and metadata (> (getf metadata :lsn) (getf control :applied)))
        (error "restartpoint is ahead of durable recovery control"))
      (or (getf metadata :source-checkpoint) snapshot))))

(defun native-control-facts (capture)
  "Validate CAPTURE and return the facts used by native pg_controldata.
A verified child control wins over obsolete recovery metadata. A standby takes
its state and applied floor from recovery control. Report checkpoint starts,
although native WAL control stores checkpoint ends."
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
      (cond
        ((> insertion 1)
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
               recovery-timeline (getf control :timeline)))
        ((or (third capture) (fourth capture))
         (error "native recovery metadata is incomplete")))
      (list :system-id system-id :db-state state :state-time time
            :checkpoint (if snapshot (getf snapshot :start)
                            (max 0 (- checkpoint +wal-header-size+)))
            :checkpoint-timeline checkpoint-id :snapshot snapshot
            :recovery-lsn recovery-lsn :recovery-timeline recovery-timeline))))

(defun native-control-read (directory)
  "Read coherent control facts without starting storage or changing PGDATA.
Compare controls before and after validation, retrying when publication changed
them. Immutable generations are bound by the selected pointer and digest."
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
