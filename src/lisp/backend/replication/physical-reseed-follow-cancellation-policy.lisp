;;; Choose source actions and cleanup acknowledgments from the saved cancellation
;;; and a bound source observation.  Unresolved generation debt remains deferred.

(in-package #:pg)

;; Plan only the cases for which this operation can confirm cleanup.  A parent
;; abort with generation history keeps that debt for later reconciliation.

(defun physical-reseed-follow-cancellation-ack-plan (value observation)
  (let* ((record (physical-reseed-follow-cancellation-preconfiguration-record value))
         (group (getf record :cancellation))
         (phase (getf group :phase))
         (generations (getf record :generations)))
    (unless (member phase '(:pending :source-closed))
      (error "cleanup operation evidence requires pending or source-closed cancellation"))
    (multiple-value-bind (receipt classification)
        (physical-reseed-follow-cancellation-source-observation-copy record observation)
      (unless (and receipt (eq :closed (getf receipt :phase)))
        (error "cleanup operation evidence requires a bound closed receipt"))
      (when (and (eq phase :source-closed) (not (equal receipt (getf group :receipt))))
        (error "cleanup operation receipt differs from the durable source-closed receipt"))
      (cond
        ((and (eq classification :aborting) (null generations))
         (list :operation :abort :ack :parent-abort :receipt receipt :batch nil :successor nil))
        ((and (member classification '(:handed-off :unproved))
              (eq phase :source-closed) (= 1 (length generations)))
         (list :operation :release :ack :selected-release :receipt receipt :batch nil
               :successor (physical-backup-lease-successor-copy
                           (getf receipt :successor) (getf receipt :request))))))))

(defun physical-reseed-follow-cancellation-source-action (value observation)
  (let* ((record (physical-reseed-follow-cancellation-preconfiguration-record value))
         (group (getf record :cancellation))
         (phase (getf group :phase)))
    (ecase phase
      (:cleaned (values :complete (physical-backup-lease-copy (getf group :receipt))))
      (:source-closed
       (let ((receipt (getf group :receipt)))
         (when (and observation (not (equal receipt observation)))
           (error "source observation differs from the durable closed receipt"))
         (let ((plan (physical-reseed-follow-cancellation-ack-plan record receipt)))
           (values (case (getf plan :operation) (:abort :abort) (:release :release) (otherwise :defer))
                   (physical-backup-lease-copy receipt)))))
      (:pending
       (multiple-value-bind (receipt classification)
           (physical-reseed-follow-cancellation-source-observation-copy record observation)
         (values
          (case classification
            ((:unknown :unbound) :unresolved)
            (:open :abort)
            (:aborting
             (if (and (eq :closed (getf receipt :phase)) (getf record :generations)) :defer :abort))
            ((:handed-off :unproved) :cleanup-selected))
          receipt))))))

(defun physical-reseed-follow-cancellation-source-closed-candidate (value observation)
  (let* ((before (physical-reseed-follow-cancellation-preconfiguration-record value))
         (observation (physical-backup-lease-copy observation)))
    (case (getf (getf before :cancellation) :phase)
      (:pending (physical-reseed-follow-cancellation-source-closed-record before observation))
      (:source-closed
       (unless (equal observation (getf (getf before :cancellation) :receipt))
         (error "source observation differs from the saved closed receipt"))
       before)
      (otherwise (error "source observation requires pending or source-closed cancellation")))))

(defun physical-reseed-follow-cancellation-confirm-operation-result (record plan result)
  (let* ((receipt (physical-backup-lease-copy result))
         (confirmed (physical-reseed-follow-cancellation-ack-plan record receipt)))
    (unless (and confirmed (equal plan confirmed)
                 (equal receipt (getf plan :receipt)))
      (error "cleanup operation did not return its exact closed receipt"))
    confirmed))

(defun physical-reseed-follow-cancellation-cleaned-candidate (value &optional plan)
  (let* ((before (physical-reseed-follow-cancellation-preconfiguration-record value))
         (confirmed (or plan
                        (physical-reseed-follow-cancellation-ack-plan
                         before (getf (getf before :cancellation) :receipt)))))
    (unless confirmed (error "cleanup publication retains unresolved generation debt"))
    (physical-reseed-follow-cancellation-confirm-operation-result
     before confirmed (getf confirmed :receipt))
    (physical-reseed-follow-cancellation-cleaned-record
     before (getf confirmed :receipt) (getf confirmed :ack))))
