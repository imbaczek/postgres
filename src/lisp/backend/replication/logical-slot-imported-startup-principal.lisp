(in-package #:pg)

(declaim (ftype function logical-slot-imported-history-equal-p))

(defvar *logical-slot-imported-startup-captures* nil)

(defun call-with-logical-slot-imported-startup-capture (function)
  "Register a one-use capture bound to one typed source and principal."
  (let ((capture nil) (source nil) (principal nil) (active t)
                      (thread sb-thread:*current-thread*) (mutex *wal-lock*))
    (flet ((state (operation candidate observed &optional installed)
             (unless (and active (eq candidate capture)
                          (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*))
               (error "imported startup capture changed identity, thread or WAL mutex"))
             (if (eq operation :bind)
                 (progn
                   (unless (and (null source) (logical-slot-imported-startup-source-p observed)
                                (eq capture (logical-slot-imported-startup-source-capture observed))
                                (member observed *logical-slot-startup-sources* :test #'eq))
                     (error "imported startup capture cannot bind or replace this source"))
                   (setf source observed))
                 (progn
                   (unless (eq source observed)
                     (error "imported startup capture belongs to another source"))
                   (ecase operation
                     (:identity t)
                     (:principal principal)
                     (:install
                      (unless (and (null principal)
                                   (logical-slot-imported-startup-principal-p installed)
                                   (eq source (logical-slot-imported-startup-principal-source installed)))
                        (error "imported startup capture cannot replace its principal"))
                      (setf principal installed)))))))
      (setf capture (%make-logical-slot-imported-startup-capture :state #'state))
      (let ((*logical-slot-imported-startup-captures*
              (cons capture *logical-slot-imported-startup-captures*)))
        (unwind-protect (funcall function capture)
          (setf active nil))))))

(defun logical-slot-imported-startup-capture-call (source operation &optional principal)
  "Retrieve the fixed principal even if registration was lost.
Its protected check then detects the loss and latches failure; other operations
require the registered capture."
  (unless (logical-slot-imported-startup-source-p source)
    (error "imported startup principal requires its typed capture source"))
  (let ((capture (logical-slot-imported-startup-source-capture source)))
    (unless (and (logical-slot-imported-startup-capture-p capture)
                 (or (eq operation :principal)
                     (member capture *logical-slot-imported-startup-captures* :test #'eq)))
      (error "imported startup source lost its registered capture"))
    (funcall (logical-slot-imported-startup-capture-state capture)
             operation capture source principal)))

(defvar *logical-slot-imported-startup-principals* nil)

(defun logical-slot-imported-startup-source-fields (source)
  ;; Include capability identity in the principal's frozen source fields.
  (list (logical-slot-startup-source-inventory source)
        (logical-slot-imported-startup-source-capture source)
        (logical-slot-startup-source-gate-owner source)
        (logical-slot-startup-source-ownership source)
        (logical-slot-startup-source-system-id source)
        (logical-slot-startup-source-timeline source)
        (logical-slot-startup-source-namespace source)
        (logical-slot-startup-source-end source)
        (logical-slot-startup-source-incarnation source)
        (logical-slot-startup-source-wal-level source)
        (logical-slot-startup-source-registry source)
        (logical-slot-startup-source-maximum source)
        (logical-slot-startup-source-policy source)
        (logical-slot-startup-source-thread source)
        (logical-slot-startup-source-mutex source)))

(defun logical-slot-imported-startup-bounded-entries (source)
  "Bound the entry list before traversing or copying it."
  (let ((entries (logical-slot-startup-source-entries source)))
    (logical-slot-copy-descriptor-list-count entries (logical-slot-startup-source-maximum source))
    entries))

(defun logical-slot-imported-startup-freeze-entries (source)
  "Copy candidate evidence independently, excluding derived pending XIDs."
  (loop for entry in (logical-slot-imported-startup-bounded-entries source)
        for slot = (logical-slot-startup-entry-slot entry)
        collect (list entry slot
                      (copy-seq (logical-slot-startup-entry-bytes entry))
                      (copy-seq (logical-slot-startup-entry-image entry))
                      (logical-slot-startup-candidate-image slot)
                      (wal-slot-profile slot)
                      (wal-replication-slot-logical-provenance slot))))

(defun logical-slot-imported-startup-entries-equal-p (expected observed)
  (loop with remaining = observed
        for row in expected
        always
        (and (consp remaining)
             (destructuring-bind (entry slot bytes image encoded profile provenance) row
               (and (eq entry (pop remaining))
                    (eq slot (logical-slot-startup-entry-slot entry))
                    (equalp bytes (logical-slot-startup-entry-bytes entry))
                    (equalp image (logical-slot-startup-entry-image entry))
                    (equalp encoded (logical-slot-startup-candidate-image slot))
                    (eq profile (logical-slot-startup-entry-profile entry))
                    (eq profile (wal-slot-profile slot))
                    (eq provenance (logical-slot-startup-entry-provenance entry))
                    (eq provenance (wal-replication-slot-logical-provenance slot)))))
        finally (return (null remaining))))

(defun logical-slot-imported-startup-freeze-source (source)
  "Capture an independent source and candidate snapshot under WAL exclusion."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let* ((entries (logical-slot-imported-startup-freeze-entries source))
         (bytes (logical-slot-startup-source-history-bytes source))
         (history-bytes (and bytes (copy-seq bytes))))
    (logical-slot-startup-check-source source)
    (list :fields (logical-slot-imported-startup-source-fields source)
          :root (logical-wal-current-root-identity)
          :control (copy-seq (logical-slot-startup-source-control source))
          :history-bytes history-bytes
          :history (timeline-check-lineage
                    (timeline-parse-history
                     (logical-slot-startup-source-timeline source)
                     (if history-bytes (bytes->string history-bytes) "")))
          :entries entries)))

(defun logical-slot-imported-startup-snapshot-equal-p (snapshot source)
  (and (logical-slot-imported-startup-entries-equal-p
        (getf snapshot :entries) (logical-slot-startup-source-entries source))
       (equal (getf snapshot :fields) (logical-slot-imported-startup-source-fields source))
       (eq (logical-slot-startup-source-registry source) *wal-replication-slots*)
       (equal (getf snapshot :root) (logical-slot-startup-source-root source))
       (equalp (getf snapshot :control) (logical-slot-startup-source-control source))
       (equalp (getf snapshot :history-bytes) (logical-slot-startup-source-history-bytes source))
       (logical-slot-imported-history-equal-p
        (getf snapshot :history) (logical-slot-startup-source-history source))))

(defun call-with-logical-slot-imported-startup-principal-check (principal source function)
  "Check identity before sticky failure handling, so copies cannot revoke the genuine source."
  (unless (logical-slot-imported-startup-principal-p principal)
    (error "imported startup source requires its typed principal"))
  (let ((state (logical-slot-imported-startup-principal-state principal)))
    (funcall state :identity principal source)
    (sb-sys:without-interrupts
      (let ((complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (funcall state :check principal source)
                   (unless (and (eq source (logical-slot-imported-startup-principal-source principal))
                                (member principal *logical-slot-imported-startup-principals* :test #'eq))
                     (error "imported startup principal is not registered for this source"))
                   (funcall function))
               (setf complete t))
          (unless complete
            (funcall state :fail principal source)
            (setf (logical-slot-startup-source-active-p source) nil)))))))

(defun logical-slot-imported-startup-principal-under-lock (source)
  "Check the private principal and current source under WAL exclusion.
The source check reads current control, system identity and history as needed;
it does not reread the slot inventory."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((principal (logical-slot-imported-startup-capture-call source :principal)))
    (unless principal
      (error "imported startup source has no registered principal"))
    (call-with-logical-slot-imported-startup-principal-check
     principal source
     (lambda ()
       (logical-slot-startup-check-source source)
       principal))))

(defun call-with-logical-slot-imported-startup-principal (source function)
  "Bind a private snapshot to SOURCE and check actual inventory at entry and exit.
This scope alone does not establish imported slot eligibility or readiness."
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "imported startup principal acquisition requires the WAL mutex to be released"))
  (logical-slot-imported-startup-capture-call source :identity)
  (when (logical-slot-imported-startup-capture-call source :principal)
    (error "imported startup capture already has its principal"))
  (logical-slot-startup-check-authority source)
  (unless (some (lambda (entry)
                  (wal-replication-slot-child-owner (logical-slot-startup-entry-slot entry)))
                (logical-slot-imported-startup-bounded-entries source))
    (error "imported startup principal requires a child-owned candidate"))
  (let ((snapshot (sb-thread:with-mutex (*wal-lock*)
                    (logical-slot-imported-startup-freeze-source source)))
        (principal nil) (active t) (failed nil)
        (thread sb-thread:*current-thread*) (mutex *wal-lock*))
    (flet ((state (operation candidate observed)
             (unless (and (eq candidate principal) (eq observed source)
                          (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*))
               (error "imported startup principal changed identity, thread or WAL mutex"))
             (ecase operation
               (:identity t)
               (:check
                (unless (and active (not failed)
                             (member principal *logical-slot-imported-startup-principals* :test #'eq)
                             (member (logical-slot-imported-startup-source-capture source)
                                     *logical-slot-imported-startup-captures* :test #'eq)
                             (logical-slot-imported-startup-snapshot-equal-p snapshot source))
                  (error "imported startup principal ended, failed or changed")))
               (:fail (setf failed t))
               (:release (setf active nil)))))
      (setf principal (%make-logical-slot-imported-startup-principal :source source :state #'state))
      (logical-slot-imported-startup-capture-call source :install principal)
      (let ((*logical-slot-imported-startup-principals*
              (cons principal *logical-slot-imported-startup-principals*)))
        (unwind-protect
             (progn
               (logical-slot-startup-check-inventory source)
               (sb-thread:with-mutex (*wal-lock*)
                 (logical-slot-imported-startup-principal-under-lock source))
               (multiple-value-prog1 (funcall function source)
                 (logical-slot-startup-check-inventory source)
                 (sb-thread:with-mutex (*wal-lock*)
                   (logical-slot-imported-startup-principal-under-lock source))))
          (state :release principal source))))))
