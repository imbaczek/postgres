;;;; Cluster-owned recovery role; session settings never grant write authority.
(in-package #:pg)

(defconstant +db-startup+ 0)

(defconstant +db-shutdowned+ 1)

(defconstant +db-shutdowned-in-recovery+ 2)

(defconstant +db-shutdowning+ 3)

(defconstant +db-in-crash-recovery+ 4)

(defconstant +db-in-archive-recovery+ 5)

(defconstant +db-in-production+ 6)

(defvar *enable-hot-standby* t)

(defvar *physical-recovery-state* nil
  "Verified native recovery image metadata, preserved across code reloads.")

(defvar *physical-receive-store* nil)

(defvar *physical-replay-paused-p* nil
  "Process-local replay pause; preserved across reload, cleared by process restart.")

(defvar *physical-replay-pause-lock*
  (sb-thread:make-mutex :name "native replay pause"))

(declaim (ftype function physical-replay-pause-command))

(defvar *physical-replay-seen-relfilenodes* nil)

(defvar *physical-replay-restarting-p* nil)

(declaim (ftype function startup-reload-settings session-apply-startup-options session-extra-setting-p
                session-set-extra-setting session-extra-setting-value session-reset-all-settings
                session-timeout-settings-row))

(declaim (ftype function startup-setting-show-value startup-check-session-setting sql-setting-value))

(declaim (ftype function physical-recovery-checkpoint physical-restartpoint-start-worker))

(declaim (ftype function physical-restartpoint-prepare-storage physical-recovery-restartpoint))

(declaim (ftype function physical-recovery-proven-drops physical-recovery-check-base-forks))

(declaim (ftype function physical-recovery-reset-replay-history))

(declaim (ftype function physical-recovery-check-streaming-base
                physical-recovery-open-receive-store physical-replay-from
                physical-recovery-check-live-files))

(defvar *last-replay-lsn* nil
  "Last verified applied WAL boundary; NIL before any recovery has applied an image.")

(defvar *physical-recovery-failed-p* nil
  "A failed replay never admits further SQL until process restart.")

(define-condition physical-promotion-shutdown (error) ())

(defvar *physical-promotion-stopping-p* nil)

(defvar *physical-promotion-requested-p* nil)

(defvar *physical-promotion-worker* nil)

(declaim (ftype function pg-promote))

(defvar *physical-shutdown-state* nil
  "Process-local native standby shutdown phase; reset only by process restart.")

(defvar *physical-shutdown-lock* (sb-thread:make-mutex :name "native standby shutdown"))

(defun check-physical-recovery-work ()
  (check-recovery-available)
  (when (member *physical-shutdown-state* '(:prepared :failed))
    (ereport "55000" "native standby shutdown is prepared; restart is required")))

(define-condition physical-recovery-worker-shutdown (pg-error) ()
  (:default-initargs :sqlstate "55000"
                     :message "cannot start native recovery workers during shutdown"))

(defun check-physical-recovery-worker-start ()
  (check-recovery-available)
  (when (or *physical-shutdown-state* *physical-promotion-stopping-p*
            (shutdown-in-progress-p))
    (error 'physical-recovery-worker-shutdown)))

(defun check-recovery-available ()
  (when (eql 3 (getf *physical-recovery-state* :phase))
    (ereport "57P03" "pending native promotion requires reconciliation"))
  (when *physical-recovery-failed-p*
    (ereport "57P03" "physical recovery failed; restart is required")))

(defun recovery-in-progress-p ()
  (not (null *physical-recovery-state*)))

(defun prevent-recovery-write (operation)
  "Reject application mutations independently of session or transaction settings."
  (when (recovery-in-progress-p)
    (ereport "25006" "cannot execute ~a during recovery" operation)))

(defun prevent-recovery-primary-operation (operation)
  (when (recovery-in-progress-p)
    (ereport "55000" "~a cannot be executed during recovery" operation)))

(defun recovery-check-statement (ast)
  "Admit the supported read-only SQL subset while a physical image is in recovery."
  (check-recovery-available)
  (when (and (recovery-in-progress-p)
             (not (member (getf ast :type)
                          '(:select :copy-to :explain :begin :commit :rollback :set :reset :show :checkpoint))))
    (prevent-recovery-write (getf ast :type))))

(defun recovery-check-read-write ()
  (when (recovery-in-progress-p)
    (ereport "0A000" "cannot set transaction read-write mode during recovery")))
