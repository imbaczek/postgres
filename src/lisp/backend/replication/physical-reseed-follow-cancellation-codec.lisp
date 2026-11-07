;; Encode cancellation phases and preserve their frozen predecessors.

(in-package #:pg)

;; Cancellation also records a closed source receipt and a cleanup
;; acknowledgment.  The request, installed base, generation history, and any
;; existing terminal receipt remain frozen.  A handoff receipt must select the
;; recorded generation and match its known digest; without an anchor proof it
;; remains structurally valid but unproved.
;;
;; Pending may advance to source-closed, or directly to cleaned for parent
;; abort.  Source-closed may advance to cleaned with the same receipt.  The
;; cleaned acknowledgment is parent-abort for an abort and selected-release
;; for a handoff.  Repeating an unchanged record is allowed.
;;
;; These are pure schemas and transitions.  A source-closed receipt observes
;; the source; a cleaned acknowledgment needs the caller's proof of the typed
;; operation.  Neither form grants authority to issue RPCs or write a journal.
;; The common record constructor copies and checks each candidate and its
;; transition; the wrappers select the desired cancellation phase.

(defun physical-reseed-follow-cancellation-copy (value &optional predecessor)
  (physical-reseed-install-check-literal value)
  (let ((phase (getf value :phase)))
    (unless (member phase '(:pending :source-closed :cleaned))
      (error "invalid target follow cancellation phase"))
    (physical-follow-check-plist
     value (ecase phase
             (:pending '(:phase))
             (:source-closed '(:phase :receipt))
             (:cleaned '(:phase :receipt :ack))))
    (when (eq :pending phase)
      (return-from physical-reseed-follow-cancellation-copy (list :phase :pending)))
    (unless predecessor
      (error "source cleanup receipt requires its frozen predecessor"))
    (multiple-value-bind (receipt classification)
        (physical-reseed-follow-cancellation-source-observation-copy
         predecessor (getf value :receipt))
      (unless (and receipt (eq :closed (getf receipt :phase))
                   (member classification '(:aborting :handed-off :unproved)))
        (error "source cleanup requires a bound closed parent receipt"))
      (let ((group (list :phase phase :receipt receipt)))
        (when (eq :cleaned phase)
          (let ((ack (getf value :ack)))
            (unless (eq ack (if (eq :aborted (getf receipt :outcome))
                                :parent-abort :selected-release))
              (error "source cleanup acknowledgment differs from its closed outcome"))
            (setf group (append group (list :ack ack)))))
        group))))

(defun physical-reseed-follow-check-forward-record (record)
  ;; Every cancellation phase fences preparation, settlement, activation and
  ;; other forward follow operations.  Recovery has its own admission path.
  (when (getf record :cancellation)
    (error "target cancellation fences ordinary follow operations"))
  record)

(defun physical-reseed-follow-cancellation-check-transition (before after)
  (let* ((old (getf before :cancellation))
         (new (getf after :cancellation))
         (old-phase (getf old :phase))
         (new-phase (getf new :phase)))
    (unless (and new
                 (every (lambda (key) (equal (getf before key) (getf after key)))
                        '(:version :attempt :install :base :generations :configuration
                          :settlement :activation :retirement))
                 (or (equal old new)
                     (and (null old) (eq :pending new-phase))
                     (and (eq :pending old-phase)
                          (or (eq :source-closed new-phase)
                              (and (eq :cleaned new-phase)
                                   (eq :parent-abort (getf new :ack)))))
                     (and (eq :source-closed old-phase) (eq :cleaned new-phase)
                          (equal (getf old :receipt) (getf new :receipt)))))
      (error "target follow cancellation cannot disappear or change its frozen predecessors"))
    t))

(defun physical-reseed-follow-cancellation-record (value &optional group)
  (let* ((before (physical-reseed-follow-copy value))
         (candidate (copy-list before)))
    (setf (getf candidate :cancellation)
          (or group (getf before :cancellation) (list :phase :pending)))
    (setf candidate (physical-reseed-follow-copy candidate))
    (physical-reseed-follow-check-transition before candidate)
    candidate))

(defun physical-reseed-follow-cancellation-begin-record (value)
  (physical-reseed-follow-cancellation-record value))

(defun physical-reseed-follow-cancellation-source-closed-record (value receipt)
  (physical-reseed-follow-cancellation-record value (list :phase :source-closed :receipt receipt)))

(defun physical-reseed-follow-cancellation-cleaned-record (value receipt acknowledgment)
  (physical-reseed-follow-cancellation-record
   value (list :phase :cleaned :receipt receipt :ack acknowledgment)))
