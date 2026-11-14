(in-package #:pg)

;; Publish the exact CF child from canonical parent D, or verify and sync an
;; already published child.  Catalog restoration and selected data sync precede
;; the control rename.  The checks on both sides of each boundary preserve the
;; source and WAL evidence through publication and durability.
(defun logical-slot-copy-cold-control-file (attempt operation)
  (funcall (logical-slot-copy-cold-control-attempt-operations attempt) :file attempt operation))

(defun logical-slot-copy-cold-control-point (point attempt)
  (logical-slot-copy-cold-control-record attempt)
  (logical-slot-copy-verification-test-point point (logical-slot-copy-cold-control-attempt-context attempt))
  (logical-slot-copy-cold-control-record attempt))

(defun logical-slot-copy-cold-control-restore-catalog (attempt runtime store)
  (logical-slot-copy-cold-control-check-metadata attempt runtime)
  (unless (eq :control (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
    (let* ((owner (logical-slot-copy-cold-control-attempt-owner attempt))
           (context (logical-slot-copy-cold-control-attempt-context attempt))
           (binding (logical-slot-copy-cold-control-attempt-binding attempt))
           (*pgdata* (physical-image-context-root-path context))
           (*physical-image-context* nil)
           (target nil))
      (let ((proof (logical-slot-copy-read-file context "logical-slot-copy.base-verified"
                                                +logical-slot-copy-base-proof-maximum-bytes+)))
        (multiple-value-bind (catalog-binding global phase)
            (logical-slot-copy-catalog-read-binding owner context proof)
          (logical-slot-copy-catalog-call-with-bound-source
           owner context catalog-binding global phase store
           (logical-slot-copy-cold-control-attempt-mark-started attempt)
           (lambda (catalog-attempt)
             (logical-slot-copy-catalog-restore catalog-attempt)
             (setf target (copy-list (logical-slot-copy-catalog-attempt-target-facts catalog-attempt)))))))
      (unless target (error "cold catalog restoration lost its authenticated target"))
      (setf (logical-slot-copy-cold-control-runtime-catalog-bytes runtime) (getf binding :catalog)
            (logical-slot-copy-cold-control-runtime-catalog-facts runtime) target
            (logical-slot-copy-cold-control-runtime-temporary-kind runtime) :absent
            (logical-slot-copy-cold-control-runtime-temporary-facts runtime) nil
            (logical-slot-copy-cold-control-runtime-temporary-bytes runtime) nil)
      (logical-slot-copy-cold-control-advance-directory attempt runtime)))
  (logical-slot-copy-cold-control-check-metadata attempt runtime))

(defun logical-slot-copy-cold-control-artifacts (attempt runtime)
  (logical-slot-copy-cold-control-check-metadata attempt runtime)
  (let* ((context (logical-slot-copy-cold-control-attempt-context attempt))
         (binding (logical-slot-copy-cold-control-attempt-binding attempt))
         (checker (logical-slot-copy-cold-control-runtime-wal-checker runtime))
         (snapshot (logical-slot-copy-cold-control-runtime-wal-snapshot runtime))
         (*physical-image-context* context))
    (when checker (funcall checker nil))
    (when snapshot (logical-slot-copy-cold-wal-check-snapshot context snapshot 4096))
    (let ((rows
            (if (logical-slot-copy-cold-control-runtime-child-complete runtime)
                (logical-slot-copy-check-committed-child
                 context (getf binding :evidence) (getf binding :plan) (getf binding :prefix)
                 (getf binding :control)
                 :include-control (eq :child (logical-slot-copy-cold-control-runtime-phase runtime)))
                (logical-slot-copy-catalog-artifacts binding
                                                     (logical-slot-copy-cold-control-attempt-owner attempt) context))))
      (when checker (funcall checker nil))
      (when snapshot (logical-slot-copy-cold-wal-check-snapshot context snapshot 4096))
      (logical-slot-copy-cold-control-check-metadata attempt runtime)
      rows)))

(defun logical-slot-copy-cold-control-publish-target (attempt runtime)
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (unless (eq :parent (logical-slot-copy-cold-control-runtime-phase runtime))
    (error "cold target creation requires parent D"))
  (logical-slot-copy-cold-control-point :before-cold-control-create attempt)
  (logical-slot-copy-cold-control-file attempt :create)
  (logical-slot-copy-cold-control-point :after-cold-control-create attempt)
  (logical-slot-copy-cold-control-file attempt :write)
  (logical-slot-copy-cold-control-point :after-cold-control-write attempt)
  (logical-slot-copy-cold-control-file attempt :sync-old)
  (logical-slot-copy-cold-control-point :after-cold-control-old-sync attempt)
  (logical-slot-copy-cold-control-file attempt :sync-temp)
  (logical-slot-copy-cold-control-point :after-cold-control-temp-sync attempt)
  (logical-slot-copy-cold-control-point :before-cold-control-rename attempt)
  (let ((*logical-slot-copy-verification-test-hook* nil))
    (logical-slot-copy-cold-control-file attempt :rename))
  (logical-slot-copy-cold-control-point :after-cold-control-rename attempt)
  (logical-slot-copy-cold-control-file attempt :sync-installed)
  (logical-slot-copy-cold-control-point :after-cold-control-file-sync attempt)
  (logical-slot-copy-cold-control-file attempt :sync-directory)
  (logical-slot-copy-cold-control-point :after-cold-control-directory-sync attempt))

(defun logical-slot-copy-cold-control-check-finished (attempt runtime)
  (logical-slot-copy-cold-control-check-metadata attempt runtime)
  (unless (and (eq :child (logical-slot-copy-cold-control-runtime-phase runtime))
               (logical-slot-copy-cold-control-runtime-child-complete runtime)
               (logical-slot-copy-cold-control-runtime-file-synced runtime)
               (logical-slot-copy-cold-control-runtime-directory-synced runtime)
               (equalp (getf (logical-slot-copy-cold-control-attempt-binding attempt) :catalog)
                       (logical-slot-copy-cold-control-runtime-catalog-bytes runtime)))
    (error "cold control publication did not finish durability"))
  t)

(defun logical-slot-copy-cold-control-readonly-wal (attempt runtime)
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (let* ((context (logical-slot-copy-cold-control-attempt-context attempt))
         (b (logical-slot-copy-cold-control-attempt-binding attempt))
         (ledger
           (logical-slot-copy-cold-wal-initialize
            context (getf b :state) (getf b :plan) (getf b :prefix) t nil 4096
            (lambda (&rest arguments) (declare (ignore arguments))
              (error "read-only cold WAL observation cannot acquire writer witnesses"))
            (* 6 +wal-segment-size+))))
    (setf (logical-slot-copy-cold-wal-ledger-schedule ledger) nil)
    (setf (logical-slot-copy-cold-control-runtime-wal-snapshot runtime)
          (logical-slot-copy-cold-wal-snapshot ledger))))

;; Execute one publication attempt.  Parent D restores the selected catalog,
;; stages and verifies the child, syncs selected data, then publishes control;
;; an existing child is rechecked and synced.  Return :published or :synced
;; only after the corresponding durability and source checks succeed.
(defun logical-slot-copy-cold-control-execute (attempt runtime store)
  (with-logical-slot-copy-cold-control-guard (attempt)
    (logical-slot-copy-cold-control-runtime-record attempt runtime)
    (logical-slot-copy-cold-control-file attempt :open-control)
    (if (eq :child (logical-slot-copy-cold-control-runtime-phase runtime))
        (progn
          (logical-slot-copy-cold-control-readonly-wal attempt runtime)
          (logical-slot-copy-cold-control-file attempt :sync-installed)
          (logical-slot-copy-cold-control-file attempt :sync-directory)
          (logical-slot-copy-cold-control-check-finished attempt runtime)
          (let ((*logical-slot-copy-verification-test-hook* nil))
            (funcall (logical-slot-copy-cold-control-attempt-source-check attempt)))
          :synced)
        (progn
          (logical-slot-copy-cold-control-restore-catalog attempt runtime store)
          (when (eq :control (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
            (logical-slot-copy-cold-control-file attempt :open-residue)
            (logical-slot-copy-cold-control-point :before-cold-control-residue-unlink attempt)
            (logical-slot-copy-cold-control-file attempt :remove-residue)
            (logical-slot-copy-cold-control-point :after-cold-control-residue-unlink attempt)
            (logical-slot-copy-cold-control-file attempt :close-temp)
            (logical-slot-copy-cold-control-file attempt :sync-directory))
          (let* ((b (logical-slot-copy-cold-control-attempt-binding attempt))
                 (context (logical-slot-copy-cold-control-attempt-context attempt))
                 (state (getf b :state)) (plan (getf b :plan)) (prefix (getf b :prefix))
                 (files (physical-promotion-checkpoint-files plan prefix))
                 (history-p (not (null (wal-namespace-file-stat
                                        (timeline-history-path (getf plan :child) *pgdata*) t)))))
            (call-with-logical-slot-copy-cold-wal-ledger
             context state plan prefix
             (lambda () (logical-slot-copy-cold-control-check-metadata attempt runtime))
             (lambda (observer verify)
               (setf (logical-slot-copy-cold-control-runtime-wal-checker runtime) verify)
               (funcall (logical-slot-copy-cold-control-attempt-mark-started attempt))
               (logical-slot-copy-cold-wal-stage-checkpoint
                state plan prefix files history-p
                (lambda (stage)
                  (funcall verify stage)
                  (when stage (setf (logical-slot-copy-cold-control-runtime-child-complete runtime) t))
                  (funcall (logical-slot-copy-cold-control-attempt-source-check attempt)))
                (lambda () (logical-slot-copy-cold-control-sync-data attempt runtime))
                :context context :install-prefix t :observer observer)
               (logical-slot-copy-cold-control-publish-target attempt runtime)
               (logical-slot-copy-cold-control-check-finished attempt runtime)
               (let ((*logical-slot-copy-verification-test-hook* nil))
                 (funcall (logical-slot-copy-cold-control-attempt-source-check attempt)))
               (setf (logical-slot-copy-cold-control-runtime-wal-snapshot runtime) (funcall verify :snapshot)
                     (logical-slot-copy-cold-control-runtime-wal-checker runtime) nil))
             :history-p history-p :install-prefix t :maximum 4096))
          :published))))
