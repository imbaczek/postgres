(in-package #:pg)

(defstruct (logical-slot-copy-install-prepare-owner
            (:include logical-slot-copy-committed-owner)
            (:constructor %make-logical-slot-copy-install-prepare-owner)
            (:print-function physical-libpq-print-private-object))
  "Committed input owner for a scoped pending installation preparation.")

(defstruct (logical-slot-copy-install-preparation
            (:constructor %make-logical-slot-copy-install-preparation)
            (:print-function physical-libpq-print-private-object))
  "Sealed pending attempt bound to its owner, context, store and proof."
  (owner nil :read-only t) (context nil :read-only t) (store nil :read-only t)
  (certificate nil :read-only t) (plan nil :read-only t)
  (fact nil :read-only t) (history nil :read-only t) (proof nil :read-only t))

(defvar *logical-slot-copy-install-preparations* nil)

(defun logical-slot-copy-install-preparation-image (attempt)
  "Make an independent image of the attempt's proof and inputs."
  (let ((proof (logical-slot-copy-install-preparation-proof attempt)))
    (list
     (logical-slot-copy-import-image (logical-slot-copy-install-preparation-certificate attempt))
     (logical-slot-copy-import-evidence-encode (logical-slot-copy-committed-proof-evidence proof))
     (logical-slot-copy-descriptor-encode (logical-slot-copy-committed-proof-descriptor proof))
     (logical-slot-copy-import-state-image
      (list (logical-slot-copy-install-preparation-plan attempt)
            (logical-slot-copy-install-preparation-fact attempt)
            (logical-slot-copy-install-preparation-history attempt)
            (logical-slot-copy-committed-proof-manifest proof)
            (logical-slot-copy-committed-proof-control proof)
            (logical-slot-copy-committed-proof-state proof)
            (logical-slot-copy-committed-proof-metadata proof)
            (logical-slot-copy-committed-proof-prefix proof)
            (logical-slot-copy-committed-proof-generation proof)
            (logical-slot-copy-committed-proof-tuple proof)
            (logical-slot-copy-committed-proof-observations proof))))))

(defun logical-slot-copy-install-preparation-record (owner context attempt)
  "Require the registered attempt and its matching owner and context."
  (let ((record (assoc attempt *logical-slot-copy-install-preparations* :test #'eq)))
    (unless (and record (logical-slot-copy-install-prepare-owner-p owner)
                 (logical-slot-copy-install-preparation-p attempt)
                 (eq owner (logical-slot-copy-install-preparation-owner attempt))
                 (eq context (logical-slot-copy-install-preparation-context attempt)))
      (error "logical installation preparation requires its registered owner and attempt"))
    record))

(defun logical-slot-copy-install-recheck-source (context attempt)
  "Recheck the committed source against the detached proof."
  (let ((proof (logical-slot-copy-install-preparation-proof attempt)))
    (multiple-value-bind (fact history tuple)
        (logical-slot-copy-check-committed-source
         context (logical-slot-copy-install-preparation-certificate attempt)
         (logical-slot-copy-committed-proof-evidence proof)
         (logical-slot-copy-committed-proof-descriptor proof)
         (logical-slot-copy-committed-proof-manifest proof)
         (logical-slot-copy-committed-proof-control proof)
         (logical-slot-copy-install-preparation-plan attempt)
         (logical-slot-copy-committed-proof-state proof)
         (logical-slot-copy-committed-proof-metadata proof)
         (logical-slot-copy-committed-proof-prefix proof)
         (logical-slot-copy-committed-proof-generation proof)
         (logical-slot-copy-install-preparation-store attempt))
      (unless (and (logical-slot-copy-committed-image-equal-p
                    fact (logical-slot-copy-install-preparation-fact attempt))
                   (logical-slot-copy-committed-image-equal-p
                    history (logical-slot-copy-install-preparation-history attempt))
                   (equal tuple (logical-slot-copy-committed-proof-tuple proof)))
        (error "logical installation preparation source proof changed")))))

(defun logical-slot-copy-install-check-preparation (owner context attempt &key source)
  "Check the registered attempt, held input, receive tuple and capacity.
With SOURCE, also recheck the full committed source proof."
  (let* ((record (logical-slot-copy-install-preparation-record owner context attempt))
         (proof (logical-slot-copy-install-preparation-proof attempt))
         (store (logical-slot-copy-install-preparation-store attempt)))
    (funcall (third record) :check)
    (logical-slot-copy-check-input owner context)
    (unless (logical-slot-copy-committed-image-equal-p
             (second record) (logical-slot-copy-install-preparation-image attempt))
      (error "logical installation preparation image changed"))
    (physical-store-check-live store)
    (unless (equal (logical-slot-copy-committed-proof-tuple proof) (physical-store-control-tuple store))
      (error "logical installation preparation receive tuple changed"))
    (physical-store-read-retained-control store (logical-slot-copy-committed-proof-tuple proof))
    (logical-slot-copy-input-check-observations
     owner context (logical-slot-copy-committed-proof-observations proof))
    (logical-slot-copy-import-check-capacity
     owner context (list :slots (getf (logical-slot-copy-install-preparation-certificate attempt) :slots)))
    (when source (logical-slot-copy-install-recheck-source context attempt))
    t))

(defun logical-slot-copy-install-call-with-write (owner context attempt function)
  "Recheck the pending proof around a trusted filesystem write.
Arm failure handling before calling FUNCTION.  FUNCTION must perform the full
operation and its checks without masking errors.  Any error or nonlocal exit
before the post-write check completes fails the attempt."
  (let ((record (logical-slot-copy-install-preparation-record owner context attempt))
        (completed nil))
    (unwind-protect
         (progn
           (logical-slot-copy-install-check-preparation owner context attempt :source t)
           (funcall (third record) :arm)
           (multiple-value-prog1 (funcall function)
             (logical-slot-copy-install-check-preparation owner context attempt :source t)
             (setf completed t)))
      (unless completed (funcall (third record) :fail)))))

(defun call-with-logical-slot-copy-install-preparation (function)
  "Call FUNCTION with a registered, pending-only preparation attempt.
The attempt is revoked on exit.  After a write is armed, an error or incomplete
scope latches recovery failure, including failure during the final source
recheck.  End this scope before exchanging an active certificate; that needs
an owner which can check active state.  This scope does not build files, move
journals, activate slots or admit them."
  (let ((started nil) (failed nil) (finished nil))
    (labels ((control (action)
               (ecase action
                 (:arm (setf started t))
                 (:fail (setf failed t))
                 (:check (when failed (error "logical installation preparation already failed"))))))
      (unwind-protect
           (multiple-value-prog1
               (handler-bind ((error (lambda (condition)
                                       (declare (ignore condition))
                                       (when started (setf failed t)))))
                 (call-with-logical-slot-copy-committed-source-kind
                  #'%make-logical-slot-copy-install-prepare-owner
                  (lambda (owner context store certificate plan fact history proof)
                    (logical-slot-copy-install-active-image certificate)
                    (let* ((attempt
                             (%make-logical-slot-copy-install-preparation
                              :owner owner :context context :store store
                              :certificate (logical-slot-copy-import-decode
                                            (logical-slot-copy-import-image certificate))
                              :plan (logical-slot-copy-import-state-image plan)
                              :fact (logical-slot-copy-import-state-image fact)
                              :history (logical-slot-copy-import-state-image history) :proof proof))
                           (record (list attempt (logical-slot-copy-install-preparation-image attempt) #'control))
                           (*logical-slot-copy-install-preparations*
                             (cons record *logical-slot-copy-install-preparations*)))
                      (logical-slot-copy-install-check-preparation owner context attempt)
                      (multiple-value-prog1 (funcall function owner context attempt)
                        (logical-slot-copy-install-check-preparation owner context attempt))))))
             (unless failed (setf finished t)))
        (when (and started (not finished)) (setf *physical-recovery-failed-p* t))))))
