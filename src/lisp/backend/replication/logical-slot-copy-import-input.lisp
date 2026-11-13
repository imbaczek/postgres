;;; Thread-owned, read-only import input scope. This sibling owner is not a
;;; cold publisher and grants neither admission nor publication.
(in-package #:pg)

(defstruct (logical-slot-copy-import-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-copy-import-owner))
  "Owner valid within its thread and input scope.
Construction captures the live recovery state and store, or NIL in cold mode."
  (ownership nil :read-only t)
  (mode nil :read-only t)
  (phase nil :read-only t)
  (state nil :read-only t)
  (store nil :read-only t)
  existing-proof)

(defun logical-slot-copy-check-import-locks ()
  "Require import exclusion and stopped workers at the worker join boundary."
  (unless (and (zerop *live-operation-depth*)
               (sb-thread:holding-mutex-p *physical-follow-command-lock*)
               (sb-thread:holding-mutex-p *reload-run-lock*)
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*)
               (sb-thread:holding-mutex-p *physical-restartpoint-start-lock*)
               (sb-thread:holding-mutex-p *timeline-history-lock*)
               (null *physical-follow-runtime*) (null *physical-restartpoint-worker*)
               (null *physical-shutdown-state*) (not (shutdown-in-progress-p))
               (not *physical-recovery-failed-p*) (not *wal-poisoned*)
               (not *swank-listener-enabled-p*)
               (or (null *physical-receive-store*)
                   (and (physical-store-p *physical-receive-store*)
                        (not (sb-thread:holding-mutex-p
                              (physical-store-mutex *physical-receive-store*))))))
    (error "logical import input requires owned import exclusion and stopped workers"))
  t)

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-import-owner))
  (logical-slot-copy-check-import-locks)
  (unless (member (logical-slot-copy-import-owner-phase owner) '(2 3))
    (error "logical import input requires streaming or pending recovery"))
  (ecase (logical-slot-copy-import-owner-mode owner)
    (:cold
     (physical-adoption-check-startup-quiescence)
     (unless (and (= 3 (logical-slot-copy-import-owner-phase owner))
                  (null (logical-slot-copy-import-owner-state owner))
                  (null (logical-slot-copy-import-owner-store owner))
                  (null *physical-recovery-state*) (null *catalog*)
                  (null *wal-output-stream*) (null *clog-stream*)
                  (zerop *n-backends*) (zerop (hash-table-count *relation-streams*)))
       (error "cold logical import input requires unopened storage")))
    (:live
     (unless (and *wal-ready*
                  (eq *physical-recovery-state* (logical-slot-copy-import-owner-state owner))
                  (eq *physical-receive-store* (logical-slot-copy-import-owner-store owner))
                  (physical-store-p *physical-receive-store*)
                  (eql (getf *physical-recovery-state* :phase)
                       (logical-slot-copy-import-owner-phase owner)))
       (error "live logical import input lost its captured recovery state or store"))
     (physical-store-check-live *physical-receive-store*)))
  (logical-slot-copy-check-postmaster-owner
   owner (logical-slot-copy-import-owner-ownership owner))
  (physical-adoption-check-startup)
  owner)

(defmethod physical-adoption-owner-extra-root-names ((owner logical-slot-copy-import-owner))
  (physical-directory-check-owner owner)
  (append (list "postmaster.pid")
          (when (logical-slot-copy-import-owner-existing-proof owner)
            (list "logical-slot-copy.base-verified"))))

(defun call-with-logical-slot-copy-import-input (function)
  "Run FUNCTION in a sibling read-only import input scope.
This grants neither publication nor admission. Live mode borrows the caller's
store; cold mode requires unopened storage globals. Public fences still apply."
  (logical-slot-copy-check-import-locks)
  (let ((mode (if *wal-ready* :live :cold))
        (state *physical-recovery-state*)
        (store *physical-receive-store*))
    (call-with-logical-slot-copy-owned-input
     (lambda (&rest arguments)
       (let* ((ownership (getf arguments :ownership))
              (control (physical-recovery-read-control
                        (postmaster-ownership-directory ownership))))
         (apply #'%make-logical-slot-copy-import-owner
                :mode mode :phase (getf control :phase) :state state :store store arguments)))
     (lambda (owner context proof)
       (unless proof (error "logical import input requires base verification receipt"))
       (setf (logical-slot-copy-import-owner-existing-proof owner) proof)
       (funcall function owner context)))))

(defun logical-slot-copy-check-import-receipt (owner context)
  "Check the bound receipt for the captured import phase and its recovery state."
  (unless (and (logical-slot-copy-import-owner-p owner)
               (logical-slot-copy-import-owner-existing-proof owner))
    (error "logical import receipt requires its owned input scope"))
  (logical-slot-copy-check-bound-receipt
   owner context (logical-slot-copy-import-owner-existing-proof owner)
   (list (logical-slot-copy-import-owner-phase owner))
   (lambda (control)
     (if (= 3 (getf control :phase))
         (let ((*physical-image-context* nil))
           (physical-promotion-read-pending-state))
         (physical-recovery-verify-base-state)))))

