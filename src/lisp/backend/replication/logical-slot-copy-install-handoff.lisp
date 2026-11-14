(in-package #:pg)

;;; The handoff survives preparation's descriptor scope only within the outer
;;; operation.  It is minted after source and stage postchecks.  Publishing the
;;; fixed journal and moving canonical slots require a separate publisher.
(defstruct (logical-slot-copy-install-operation
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-copy-install-operation)
            (:print-function physical-libpq-print-private-object))
  "Thread-bound operation owner; it does not authorize offline adoption."
  (ownership nil :read-only t))

(defstruct (logical-slot-copy-install-operation-state
            (:constructor %make-logical-slot-copy-install-operation-state)
            (:print-function physical-libpq-print-private-object))
  "Scoped operation evidence, including the root, exclusion state, and handoffs."
  (operation nil :read-only t) (thread nil :read-only t)
  (root-fd nil :read-only t) (root-identity nil :read-only t)
  (mode nil :read-only t) (state nil :read-only t) (store nil :read-only t)
  (locks nil :read-only t) (gate nil :read-only t)
  handoffs failed)

(defstruct (logical-slot-copy-install-handoff
            (:constructor %make-logical-slot-copy-install-handoff)
            (:print-function physical-libpq-print-private-object))
  "Opaque handle valid only in its creating operation."
  (operation nil :read-only t))

(defstruct (logical-slot-copy-install-handoff-record
            (:constructor %make-logical-slot-copy-install-handoff-record)
            (:print-function physical-libpq-print-private-object))
  "Private image and availability state for one handoff."
  (handoff nil :read-only t) (image nil :read-only t) state)

(defvar *logical-slot-copy-install-operations* nil)
(defvar *logical-slot-copy-install-handoff-test-hook* nil)

(defun logical-slot-copy-install-handoff-point (point operation handoff)
  (when *logical-slot-copy-install-handoff-test-hook*
    (funcall *logical-slot-copy-install-handoff-test-hook* point operation handoff)))

(defun logical-slot-copy-install-lock-image ()
  (list *physical-follow-command-lock* *reload-run-lock* *physical-follow-generation-lock*
        *physical-restartpoint-start-lock* *timeline-history-lock*
        *live-operation-lock* *live-operation-cv*))

(defun logical-slot-copy-install-operation-record (operation)
  "Find the active operation by identity and require its owning thread."
  (let ((record (find operation *logical-slot-copy-install-operations*
                      :key #'logical-slot-copy-install-operation-state-operation :test #'eq)))
    (unless (and record (logical-slot-copy-install-operation-p operation)
                 (not (logical-slot-copy-install-operation-state-failed record))
                 (physical-adoption-owner-active-p operation)
                 (eq sb-thread:*current-thread* (logical-slot-copy-install-operation-state-thread record)))
      (error "logical installation operation scope ended or changed thread"))
    record))

(defun logical-slot-copy-install-check-operation-mode (record)
  "Require the captured cold or live mode and its corresponding state."
  (ecase (logical-slot-copy-install-operation-state-mode record)
    (:cold
     (physical-adoption-check-startup-quiescence)
     (unless (and (not *wal-ready*) (null *physical-recovery-state*) (null *physical-receive-store*)
                  (null *catalog*) (null *wal-output-stream*) (null *clog-stream*)
                  (zerop *n-backends*) (zerop (hash-table-count *relation-streams*))
                  (null (logical-slot-copy-install-operation-state-state record))
                  (null (logical-slot-copy-install-operation-state-store record)))
       (error "cold logical installation operation requires unopened storage")))
    (:live
     (unless (and *wal-ready*
                  (eq *physical-recovery-state* (logical-slot-copy-install-operation-state-state record))
                  (eq *physical-receive-store* (logical-slot-copy-install-operation-state-store record))
                  (physical-store-p *physical-receive-store*)
                  (eql 3 (getf *physical-recovery-state* :phase)))
       (error "live logical installation operation changed its pending state or store"))
     (physical-store-check-live *physical-receive-store*))))

(defun logical-slot-copy-install-check-operation (operation)
  "Recheck ownership, locks, quiescence gate, root binding, and mode.
A failed check revokes the operation."
  (let ((record (logical-slot-copy-install-operation-record operation)) (completed nil))
    (unwind-protect
         (progn
           (unless (equal (logical-slot-copy-install-operation-state-locks record)
                          (logical-slot-copy-install-lock-image))
             (error "logical installation operation exclusion identities changed"))
           (logical-slot-copy-check-import-locks)
           (check-live-operation-startup-quiescence
            (logical-slot-copy-install-operation-state-gate record))
           (unless (sb-thread:holding-mutex-p *physical-follow-generation-lock*)
             (error "logical installation operation requires the generation fence"))
           (physical-directory-check-owner-common operation)
           (logical-slot-copy-check-postmaster-owner
            operation (logical-slot-copy-install-operation-ownership operation))
           (let ((held (physical-image-file-facts (logical-slot-copy-install-operation-state-root-fd record)))
                 (named (physical-image-file-facts -100 (namestring (physical-adoption-owner-directory operation))))
                 (expected (logical-slot-copy-install-operation-state-root-identity record)))
             (unless (and (equal expected (logical-slot-copy-install-file-identity held))
                          (equal expected (logical-slot-copy-install-file-identity named))
                          (sb-posix:s-isdir (third held)) (= (sb-posix:geteuid) (fifth held))
                          (= #o700 (logand #o7777 (third held))))
               (error "logical installation operation root binding changed")))
           (physical-directory-check-owner-common operation)
           (logical-slot-copy-check-import-locks)
           (logical-slot-copy-install-check-operation-mode record)
           (check-live-operation-startup-quiescence
            (logical-slot-copy-install-operation-state-gate record))
           (setf completed t)
           operation)
      (unless completed
        (setf (logical-slot-copy-install-operation-state-failed record) t
              (physical-adoption-owner-active-p operation) nil)))))

(defmethod physical-adoption-check-owner-role ((operation logical-slot-copy-install-operation))
  (logical-slot-copy-install-check-operation operation))

(defmethod physical-adoption-check-owner ((operation logical-slot-copy-install-operation))
  (declare (ignore operation))
  (error "logical installation operation does not authorize offline adoption"))

(defun logical-slot-copy-install-check-operation-input (operation context)
  "Require the preparation context to hold the operation's root."
  (logical-slot-copy-install-check-operation operation)
  (physical-image-check-context context)
  (unless (equal (logical-slot-copy-install-operation-state-root-identity
                  (logical-slot-copy-install-operation-record operation))
                 (logical-slot-copy-install-file-identity
                  (physical-image-file-facts (physical-image-context-root-fd context))))
    (error "logical installation preparation differs from its operation root")))

(defun logical-slot-copy-install-handoff-record (operation handoff)
  "Find an available handoff by identity in its active operation."
  (logical-slot-copy-install-check-operation operation)
  (let ((record (find handoff
                      (logical-slot-copy-install-operation-state-handoffs
                       (logical-slot-copy-install-operation-record operation))
                      :key #'logical-slot-copy-install-handoff-record-handoff :test #'eq)))
    (unless (and record (logical-slot-copy-install-handoff-p handoff)
                 (eq operation (logical-slot-copy-install-handoff-operation handoff))
                 (eq :available (logical-slot-copy-install-handoff-record-state record)))
      (error "logical installation handoff is not available in this operation"))
    record))

(defun logical-slot-copy-install-handoff-image (operation handoff)
  "Return a defensive copy of the available handoff image."
  (copy-seq (logical-slot-copy-install-handoff-record-image
             (logical-slot-copy-install-handoff-record operation handoff))))

(defun logical-slot-copy-install-consume-handoff (operation handoff)
  "Spend the handoff once and return defensive bytes.
These bytes do not authorize I/O or publication.  A future publisher must
reprove its typed role, source, and stage under fresh ownership."
  (let ((image (logical-slot-copy-install-handoff-image operation handoff)))
    (sb-sys:without-interrupts
      (let ((record (logical-slot-copy-install-handoff-record operation handoff)))
        (setf (logical-slot-copy-install-handoff-record-state record) :spent)))
    image))

(defun logical-slot-copy-install-create-handoff (operation mark-started)
  "Stage the installation and mint a handoff after preparation postchecks.
Recheck the operation and decode the image before registering the handoff."
  (let ((image
          (call-with-logical-slot-copy-install-preparation
           (lambda (owner context attempt)
             (logical-slot-copy-install-check-operation-input operation context)
             (logical-slot-copy-install-handoff-point :before-stage operation nil)
             (funcall mark-started)
             (let ((image (call-with-logical-slot-copy-install-staged
                           owner context attempt
                           (lambda (stage) (copy-seq (logical-slot-copy-install-stage-image stage))))))
               (logical-slot-copy-install-handoff-point :after-stage operation nil)
               image)))))
    (logical-slot-copy-install-handoff-point :after-preparation operation nil)
    (logical-slot-copy-install-check-operation operation)
    (logical-slot-copy-install-decode image)
    (let* ((handoff (%make-logical-slot-copy-install-handoff :operation operation))
           (record (%make-logical-slot-copy-install-handoff-record
                    :handoff handoff :image (copy-seq image) :state :available)))
      (push record (logical-slot-copy-install-operation-state-handoffs
                    (logical-slot-copy-install-operation-record operation)))
      (logical-slot-copy-install-handoff-point :after-mint operation handoff)
      (logical-slot-copy-install-check-operation operation)
      handoff)))

(defun call-with-logical-slot-copy-install-operation-under-locks (function)
  "Run the scoped operation under the caller's exclusion locks.
Revoke every handoff and the owner on exit.  After staging starts, failure
through root close and the final postmaster ownership check fences recovery."
  (when *logical-slot-copy-install-operations*
    (error "logical installation operation is already active"))
  (logical-slot-copy-check-import-locks)
  (let ((started nil) (finished nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-postmaster-ownership
              (lambda (ownership)
                (let ((operation (apply #'%make-logical-slot-copy-install-operation
                                        (logical-slot-copy-owner-arguments ownership))))
                  (unwind-protect
                       (call-with-physical-directory-root
                        operation
                        (lambda (fd path)
                          (declare (ignore path))
                          (let* ((record (%make-logical-slot-copy-install-operation-state
                                          :operation operation :thread sb-thread:*current-thread*
                                          :root-fd fd :root-identity (logical-slot-copy-install-file-identity
                                                                      (physical-image-file-facts fd))
                                          :mode (if *wal-ready* :live :cold)
                                          :state *physical-recovery-state* :store *physical-receive-store*
                                          :locks (logical-slot-copy-install-lock-image)
                                          :gate *live-operation-quiescence-owner*))
                                 (*logical-slot-copy-install-operations*
                                   (cons record *logical-slot-copy-install-operations*)))
                            (unwind-protect
                                 (progn
                                   (logical-slot-copy-install-check-operation operation)
                                   (let ((handoff (logical-slot-copy-install-create-handoff
                                                   operation (lambda () (setf started t)))))
                                     (multiple-value-prog1 (funcall function operation handoff)
                                       (logical-slot-copy-install-check-operation operation))))
                              (dolist (handoff-record (logical-slot-copy-install-operation-state-handoffs record))
                                (setf (logical-slot-copy-install-handoff-record-state handoff-record) :revoked))))))
                    (setf (physical-adoption-owner-active-p operation) nil)))))
           (setf finished t))
      (when (and started (not finished)) (setf *physical-recovery-failed-p* t)))))

(defun call-with-logical-slot-copy-install-handoff (function)
  "Hold the locks and quiescence gate while calling FUNCTION with a handoff."
  (when *logical-slot-copy-install-operations*
    (error "logical installation operation is already active"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-follow-generation-lock*)
          (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
            (sb-thread:with-mutex (*timeline-history-lock*)
              (call-with-logical-slot-copy-install-operation-under-locks function))))))))
