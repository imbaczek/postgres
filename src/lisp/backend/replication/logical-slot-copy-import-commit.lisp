;;; Commit the verified child control under a live import owner.
;;; Slot activation and writer admission remain separate.
(in-package #:pg)

(defstruct (logical-slot-copy-import-commit-owner
            (:include logical-slot-copy-import-publication-owner)
            (:constructor %make-logical-slot-copy-import-commit-owner)
            (:print-function physical-libpq-print-private-object))
  "Live commit owner with publication exclusions and generation fencing.")

(defstruct (logical-slot-copy-import-commit-attempt
            (:constructor %make-logical-slot-copy-import-commit-attempt)
            (:print-function physical-libpq-print-private-object))
  "Saved plan, certificate, evidence facts and receive tuple for rechecking."
  (plan nil :read-only t)
  (certificate nil :read-only t)
  (certificate-facts nil :read-only t)
  (evidence-facts nil :read-only t)
  (tuple nil :read-only t))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-import-commit-owner))
  (call-next-method)
  (unless (and (eq :live (logical-slot-copy-import-owner-mode owner))
               (sb-thread:holding-mutex-p *physical-follow-generation-lock*))
    (error "logical child commit requires live generation exclusion"))
  owner)

(defun logical-slot-copy-import-check-committed-files (context attempt &optional (committed t))
  "Compare exact child WAL inventory and bytes, history and, when committed, control."
  (let* ((plan (logical-slot-copy-import-commit-attempt-plan attempt))
         (child-plan (logical-slot-copy-import-pending-plan-child-plan plan))
         (evidence (logical-slot-copy-import-evidence-decode
                    (logical-slot-copy-import-pending-plan-evidence plan)))
         (files (physical-promotion-checkpoint-files
                 child-plan (logical-slot-copy-import-pending-plan-prefix plan))))
    (unless (equal (mapcar #'first files)
                   (wal-discover-segments (getf child-plan :child) :timeline))
      (error "import child segment inventory differs from its exact plan"))
    (when committed
      (unless (equalp (physical-promotion-child-control-image child-plan (getf (getf child-plan :snapshot) :time))
                      (logical-slot-copy-read-file context "global/pg_wal_control"
                                                   (1+ +wal-control-maximum-bytes+)))
        (error "committed import child control differs from its exact plan")))
    (unless (equalp (getf evidence :child-history)
                    (physical-promotion-read-file
                     (timeline-history-path (getf child-plan :child) *pgdata*)))
      (error "committed import child history differs from its exact plan"))
    (dolist (entry files)
      (unless (equalp (second entry)
                      (physical-promotion-read-file
                       (wal-segment-path (first entry) (getf child-plan :child) :timeline)))
        (error "committed import child WAL differs from its exact plan")))))

(defun logical-slot-copy-import-check-child-file (context name maximum)
  "Check an existing child artifact is private, bounded and bound to its name."
  (multiple-value-bind (parent leaf) (physical-image-parent context name)
    (call-with-physical-image-input-at
     parent leaf
     (lambda (stream)
       (when stream
         (let ((fd (sb-sys:fd-stream-fd stream)))
           (unless (and (logical-slot-copy-import-private-fd-p fd)
                        (<= (sb-posix:stat-size (sb-posix:fstat fd)) maximum)
                        (equal (physical-image-file-facts fd) (physical-image-file-facts parent leaf)))
             (error "logical import child artifact is not a private bound file")))))
     :element-type '(unsigned-byte 8) :if-does-not-exist nil)))

(defun logical-slot-copy-import-check-child-staging (context plan)
  "Check the staged child history and WAL file ownership."
  (let ((child (getf (logical-slot-copy-import-pending-plan-child-plan plan) :child)))
    (logical-slot-copy-import-check-child-file
     context (concatenate 'string "pg_wal/" (timeline-history-filename child)) (* 16 1024 1024))
    (dolist (segment (wal-discover-segments child :timeline))
      (logical-slot-copy-import-check-child-file
       context (concatenate 'string "pg_wal/" (wal-segment-filename segment child :timeline))
       +wal-segment-size+))))

(defun logical-slot-copy-import-check-child-commit (owner context attempt committed)
  "Recheck certificate, evidence, receive tuple and child artifacts by proof phase.
NIL checks pending, :STAGED checks child bytes, :FINAL repeats actual D to F
source proof before control rename, and T checks committed control and bytes."
  (unless (and (logical-slot-copy-import-commit-owner-p owner)
               (logical-slot-copy-import-commit-attempt-p attempt))
    (error "child commit requires its typed owner and saved attempt"))
  (logical-slot-copy-check-input owner context)
  (let ((plan (logical-slot-copy-import-commit-attempt-plan attempt)))
    (logical-slot-copy-import-check-child-staging context plan)
    (multiple-value-bind (value bytes facts) (logical-slot-copy-import-read-artifact context :pending)
      (unless (and (equal value (logical-slot-copy-import-pending-plan-value plan))
                   (equalp bytes (logical-slot-copy-import-commit-attempt-certificate attempt))
                   (equal facts (logical-slot-copy-import-commit-attempt-certificate-facts attempt)))
        (error "child commit pending certificate changed")))
    (multiple-value-bind (value bytes facts) (logical-slot-copy-import-read-artifact context :evidence)
      (declare (ignore value))
      (unless (and (equalp bytes (logical-slot-copy-import-pending-plan-evidence plan))
                   (equal facts (logical-slot-copy-import-commit-attempt-evidence-facts attempt)))
        (error "child commit permanent evidence changed")))
    (unless (equal (logical-slot-copy-import-commit-attempt-tuple attempt)
                   (physical-store-control-tuple (logical-slot-copy-import-owner-store owner)))
      (error "child commit receive tuple changed"))
    (physical-store-read-retained-control
     (logical-slot-copy-import-owner-store owner)
     (logical-slot-copy-import-commit-attempt-tuple attempt))
    (if (eq committed t)
        (progn
          (unless (equalp (logical-slot-copy-import-pending-plan-control plan)
                          (logical-slot-copy-read-file context "physical-recovery.control"
                                                       +physical-recovery-control-maximum-bytes+))
            (error "child commit pending recovery control changed"))
          (logical-slot-copy-import-check-committed-files context attempt))
        (progn
          (unless (member committed '(nil :staged :final)) (error "unknown child commit proof phase"))
          (logical-slot-copy-import-check-pending-plan owner context plan)
          (case committed
            (:staged (logical-slot-copy-import-check-committed-files context attempt nil))
            (:final
             (let ((fresh (logical-slot-copy-import-build-pending
                           owner context (logical-slot-copy-import-pending-plan-value plan))))
               (unless (equalp (logical-slot-copy-import-pending-plan-evidence fresh)
                               (logical-slot-copy-import-pending-plan-evidence plan))
                 (error "child commit actual source proof changed before rename")))
             (logical-slot-copy-import-check-child-commit owner context attempt :staged))))))
  t)

(defun logical-slot-copy-import-commit-parent (owner context attempt)
  "Check pending proof and return the caller-held named root and descriptor."
  (logical-slot-copy-import-check-child-commit owner context attempt nil)
  (cons (physical-adoption-owner-directory owner) (physical-image-context-root-fd context)))

(defun logical-slot-copy-import-commit-child-control ()
  "Commit the planned live child control under the typed owner and generation fence.
Failure after starting through owner exit fences recovery. Leave interrupted
control temporaries for reconciliation. This grants no admission and installs
no PGSLOT7 slots."
  (physical-promotion-check-lifecycle-context
   3 :preserve-readers *physical-promotion-preserve-readers-p*)
  (unless (sb-thread:holding-mutex-p *physical-follow-generation-lock*)
    (error "logical child commit requires the generation fence"))
  (let ((started nil) (finished nil))
    (unwind-protect
         (multiple-value-prog1
             (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
               (when *physical-restartpoint-worker*
                 (error "logical child commit requires a joined restartpoint worker"))
               (sb-thread:with-mutex (*timeline-history-lock*)
                 (call-with-logical-slot-copy-import-input-kind
                  #'%make-logical-slot-copy-import-commit-owner
                  (lambda (owner context)
                    (multiple-value-bind (existing bytes facts)
                        (logical-slot-copy-import-read-artifact context :pending)
                      (unless (and existing (eq :pending (getf existing :phase)))
                        (error "logical child commit requires a pending certificate"))
                      (let* ((plan (logical-slot-copy-import-build-pending owner context existing))
                             (attempt (%make-logical-slot-copy-import-commit-attempt
                                       :plan plan :certificate bytes :certificate-facts facts
                                       :evidence-facts
                                       (nth-value 2 (logical-slot-copy-import-read-artifact context :evidence))
                                       :tuple (physical-store-control-tuple
                                               (logical-slot-copy-import-owner-store owner))))
                             (*pgdata* (physical-image-context-root-path context)))
                        (physical-follow-call-with-receive-root
                         (logical-slot-copy-import-owner-store owner)
                         (lambda ()
                           (let ((*physical-image-context* context))
                             (logical-slot-copy-import-check-child-commit owner context attempt nil)
                             (setf started t)
                             (multiple-value-prog1
                                 (physical-promotion-publish-child-control-core owner context attempt)
                               (logical-slot-copy-verification-test-point
                                :after-import-child-control context)))))))))))
           (setf finished t))
      (when (and started (not finished)) (setf *physical-recovery-failed-p* t)))))
