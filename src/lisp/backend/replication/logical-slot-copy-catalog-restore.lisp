(in-package #:pg)

(defun logical-slot-copy-catalog-open-restartpoints (context)
  (declare (notinline physical-image-openat sb-posix:close))
  (physical-image-check-context context)
  (let ((root (physical-image-context-root-fd context)) (fd nil))
    (unless (assoc "pg_physical_restartpoints" (physical-image-context-children context) :test #'equal)
      (sb-sys:without-interrupts
        (unwind-protect
             (progn
               (setf fd (physical-image-openat
                         root "pg_physical_restartpoints"
                         (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow
                                 +postmaster-o-cloexec+) 0))
               (when (minusp fd) (setf fd nil) (error "cannot open selected restartpoint directory"))
               (physical-image-check-directory fd (first (physical-image-file-facts root)))
               (unless (= (ninth (physical-image-file-facts fd)) (ninth (physical-image-file-facts root)))
                 (error "restartpoint directory crosses its owned mount"))
               (physical-image-register-child context "pg_physical_restartpoints" fd)
               (setf fd nil))
          (when fd (sb-posix:close fd)))))
    (let ((directory (cdr (assoc "pg_physical_restartpoints"
                                 (physical-image-context-children context) :test #'equal))))
      (physical-image-check-children context)
      (values directory (physical-image-file-facts directory)))))

(defun logical-slot-copy-catalog-observe (context name maximum)
  (multiple-value-bind (parent leaf) (physical-image-parent context name)
    (let ((facts (physical-image-file-facts parent leaf)))
      (unless (and (logical-slot-copy-import-control-private-facts-p
                    facts (physical-image-file-facts (physical-image-context-root-fd context)))
                   (<= 0 (sixth facts) maximum))
        (error "catalog restoration witness is not a bounded private file"))
      (let ((row (logical-slot-copy-file-row context name (sixth facts))))
        (unless (equal facts (fourth row)) (error "catalog restoration witness changed"))
        row))))

(defun logical-slot-copy-catalog-artifacts (binding owner context)
  (logical-slot-copy-check-input owner context)
  (let* ((state (getf binding :state))
         (child (getf state :promotion-child))
         (history (timeline-history-filename child))
         (directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
         (*physical-image-context* context))
    (physical-promotion-check-history state t)
    (unless (logical-slot-copy-committed-image-equal-p
             (getf binding :plan)
             (physical-promotion-checkpoint-artifacts state (getf binding :prefix) :allow-uninstalled t))
      (error "catalog restoration child artifact plan changed"))
    (physical-promotion-check-pending-slots
     state (cons (physical-adoption-owner-directory owner) (physical-image-context-root-fd context)))
    (append
     (when (slot-state-lstat (slot-state-fd-path directory history))
       (list (logical-slot-copy-catalog-observe context (concatenate 'string "pg_wal/" history)
                                                (* 16 1024 1024))))
     (loop for segment in (wal-discover-segments child :timeline)
           collect (logical-slot-copy-catalog-observe
                    context (concatenate 'string "pg_wal/" (wal-segment-filename segment child :timeline))
                    +wal-segment-size+)))))

(defun logical-slot-copy-catalog-read-binding (owner context proof)
  ;; Bind parent D control and the actual selected R catalog (R = F), including
  ;; the pointer, generation, metadata and restartpoint file witnesses.
  (logical-slot-copy-import-control-fences context)
  (let ((global (cdr (assoc "global" (physical-image-context-children context) :test #'equal))))
    (unless (logical-slot-copy-import-control-private-facts-p
             (physical-image-file-facts global "pg_catalog.dat") (physical-image-file-facts global))
      (error "catalog restoration target is not private and regular")))
  (multiple-value-bind (restartpoints restartpoint-facts)
      (logical-slot-copy-catalog-open-restartpoints context)
    (multiple-value-bind (certificate certificate-bytes certificate-facts)
        (logical-slot-copy-import-read-artifact context :pending)
      (unless (and certificate (eq :pending (getf certificate :phase)))
        (error "catalog restoration requires its original pending certificate"))
      (multiple-value-bind (bound evidence descriptor manifest control plan status observations)
          (logical-slot-copy-read-preserved-inputs owner context proof certificate :parent)
        (declare (ignore bound status))
        (multiple-value-bind (global global-facts global-children phase)
            (logical-slot-copy-catalog-global-image context manifest)
          (multiple-value-bind (state metadata prefix generation)
              (logical-slot-copy-committed-state manifest control descriptor)
            (multiple-value-bind (actual catalog actual-prefix actual-generation)
                (physical-restartpoint-read state)
              (unless (and (equal metadata actual) (equalp prefix actual-prefix)
                           (eql generation actual-generation) (plusp (length catalog)))
                (error "catalog restoration selected restartpoint changed"))
              (let* ((name (format nil "pg_physical_restartpoints/~16,'0X-~16,'0X.rp"
                                   (getf metadata :lsn) generation))
                     (restartpoint-observations
                       (list (logical-slot-copy-catalog-observe context "physical-restartpoint.control" 28)
                             (logical-slot-copy-catalog-observe context name (* 1024 1024 1024))))
                     (inputs (cons (list "logical-slot-copy.import" (length certificate-bytes)
                                         (logical-slot-copy-import-digest certificate-bytes) certificate-facts)
                                   observations)))
                (multiple-value-bind (old-bytes old-facts)
                    (logical-slot-copy-read-file context "global/pg_catalog.dat" (* 1024 1024 1024))
                  (unless (equal old-facts (second (assoc "pg_catalog.dat" global-children :test #'equal)))
                    (error "catalog restoration target changed during observation"))
                  (values
                   (list :certificate certificate :evidence evidence :descriptor descriptor
                         :manifest manifest :control control :plan plan :state state
                         :metadata metadata :prefix prefix :generation generation :catalog catalog
                         :inputs inputs :restartpoint-observations restartpoint-observations
                         :restartpoints restartpoints :restartpoint-directory-facts restartpoint-facts
                         :global-facts global-facts :global-children global-children
                         :old-bytes old-bytes :old-facts old-facts)
                   global phase))))))))))

(defun logical-slot-copy-catalog-call-with-source (owner context proof mark-started function)
  ;; Hold the retained store and receive-root lease across the callback and
  ;; its final checks.  The internal restore accepts this attempt so a future
  ;; publisher can use it within an existing lease.
  (multiple-value-bind (binding global phase)
      (logical-slot-copy-catalog-read-binding owner context proof)
    (call-with-logical-slot-copy-committed-store
     owner (getf binding :state)
     (lambda (store)
       (physical-promotion-ensure-pending-retained
        (getf binding :state) (getf binding :metadata) store)
       (physical-follow-call-with-receive-root
        store
        (lambda ()
          (let ((attempt nil) (source-active t) (complete nil) (failed nil))
            (setf (getf binding :source-proof)
                  (multiple-value-list
                   (logical-slot-copy-check-retained-source
                    (getf binding :certificate) (getf binding :evidence) (getf binding :descriptor)
                    (getf binding :manifest) (getf binding :control) (getf binding :plan)
                    (getf binding :state) (getf binding :metadata) (getf binding :prefix)
                    (getf binding :generation) store
                    (lambda () (logical-slot-copy-catalog-artifacts binding owner context)))))
            (setf attempt
                  (%make-logical-slot-copy-catalog-attempt
                   :owner owner :context context :binding binding :global global
                   :root (physical-image-context-root-fd context) :mark-started mark-started
                   :file-operation (logical-slot-copy-catalog-file-operations (lambda () attempt))
                   :failure-operation
                   (lambda (operation subject)
                     (when (eq subject attempt)
                       (ecase operation
                         (:failedp failed)
                         (:revoke
                          (setf failed t
                                (logical-slot-copy-catalog-attempt-failed attempt) t
                                (physical-adoption-owner-active-p owner) nil)))))
                   :phase phase :target-facts (getf binding :old-facts) :active t
                   :source-check
                   (lambda ()
                     (unless source-active
                       (logical-slot-copy-catalog-revoke attempt)
                       (error "catalog source checker expired"))
                     (logical-slot-copy-catalog-check-source attempt store))))
            (setf (logical-slot-copy-catalog-attempt-seal attempt)
                  (logical-slot-copy-import-state-image (logical-slot-copy-catalog-image attempt)))
            (logical-slot-copy-catalog-seal-operation attempt)
            (let ((*logical-slot-copy-catalog-attempts*
                    (acons owner attempt *logical-slot-copy-catalog-attempts*)))
              (unwind-protect
                   (progn
                     (logical-slot-copy-catalog-open-one attempt nil)
                     (when (eq phase :residue) (logical-slot-copy-catalog-open-one attempt t))
                     (logical-slot-copy-catalog-check attempt)
                     (multiple-value-prog1 (funcall function attempt)
                       (unless (and (member (logical-slot-copy-catalog-attempt-phase attempt) '(:installed :synced))
                                    (logical-slot-copy-catalog-attempt-file-synced attempt)
                                    (logical-slot-copy-catalog-attempt-directory-synced attempt))
                         (error "catalog restoration did not complete durability"))
                       (logical-slot-copy-catalog-point :after-cold-catalog-execute attempt)
                       (logical-slot-copy-catalog-check attempt)
                       (setf source-active nil)
                       (logical-slot-copy-catalog-check attempt :source nil)
                       (setf complete t)))
                (setf source-active nil
                      (logical-slot-copy-catalog-attempt-active attempt) nil)
                (unless complete (logical-slot-copy-catalog-revoke attempt))
                (logical-slot-copy-catalog-close attempt))))))))))

(defun logical-slot-copy-import-restore-parent-catalog ()
  ;; Restore only the selected parent catalog under cold exclusion.  This does
  ;; not install runtime globals, publish child control, admit writers or retire
  ;; the copy.  Failure after mutation starts fences recovery.
  (when *logical-slot-copy-catalog-attempts*
    (error "catalog restoration is already active"))
  (logical-slot-copy-install-check-input-entry)
  (when *wal-ready* (error "catalog restoration requires unopened storage"))
  (let ((started nil) (complete nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-logical-slot-copy-install-exclusion
              (lambda ()
                (call-with-logical-slot-copy-owned-input
                 (lambda (&rest arguments)
                   (apply #'%make-logical-slot-copy-catalog-owner
                          :mode :cold :state nil :store nil arguments))
                 (lambda (owner context proof)
                   (let ((*pgdata* (physical-image-context-root-path context))
                         (*physical-image-context* nil))
                     (logical-slot-copy-catalog-call-with-source
                      owner context proof (lambda () (setf started t))
                      #'logical-slot-copy-catalog-restore))))))
           (setf complete t))
      (when (and started (not complete)) (setf *physical-recovery-failed-p* t)))))
