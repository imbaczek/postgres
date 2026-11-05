(in-package #:pg)

;;; Publish activation intent, move local controls, and recover journal writes.
;;; Building binds the intended active image before output creation.

(defun physical-reseed-follow-activation-require-attempt (context)
  ;; Effects require the exact active original attempt.  Read-only scope
  ;; checks can still inspect an attempt that has advanced to abort.
  (unless (and (physical-reseed-follow-activation-context-active-p context)
               (eq sb-thread:*current-thread* (physical-reseed-follow-activation-context-thread context)))
    (error "activation attempt check requires its active thread-bound scope"))
  (let* ((owner (physical-reseed-follow-activation-context-owner context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (snapshot (physical-reseed-follow-activation-context-attempt context))
         (record (getf snapshot :record)))
    (funcall (physical-reseed-follow-activation-context-check context))
    (physical-reseed-check-owner owner)
    (physical-reseed-follow-check-attempt-snapshot-at root snapshot)
    (unless (and (eq :premanifest (getf record :phase))
                 (equal record (getf (physical-reseed-follow-activation-context-record context) :attempt)))
      (error "activation requires the exact active original attempt")))
  t)

(defstruct (physical-reseed-follow-activation-authority
            (:constructor %make-physical-reseed-follow-activation-authority)
            (:print-function physical-libpq-print-private-object))
  ;; This thread-bound private witness retains the original snapshot, anchor
  ;; and candidate.  PUBLISHED starts NIL and records one adopted snapshot.
  ;; The opaque printer protects private values.
  owner context thread active-p snapshot anchor candidate published)

(defun physical-reseed-follow-activation-authority (context)
  ;; Require the live authority belonging to this context, owner and thread.
  (let ((authority *physical-reseed-follow-activation-authority*))
    (unless (and (physical-reseed-follow-activation-authority-p authority)
                 (physical-reseed-follow-activation-authority-active-p authority)
                 (eq sb-thread:*current-thread* (physical-reseed-follow-activation-authority-thread authority))
                 (eq context (physical-reseed-follow-activation-authority-context authority))
                 (eq (physical-reseed-follow-activation-context-owner context)
                     (physical-reseed-follow-activation-authority-owner authority))
                 (eq context (physical-reseed-follow-activation-context
                              (physical-reseed-follow-activation-authority-owner authority))))
      (error "activation intent requires its live thread-bound source authority"))
    authority))

(defun physical-reseed-follow-activation-local-facts (context)
  ;; Capture detached local facts from the live context for comparison across
  ;; source observation and publication.
  (let* ((owner (physical-reseed-follow-activation-context-owner context))
         (record (physical-reseed-follow-activation-context-record context))
         (activation (getf record :activation))
         (generations (getf record :generations)))
    (unless (and (eq context (physical-reseed-follow-activation-context owner))
                 (member (getf activation :phase) '(:staged :activating))
                 (zerop (getf activation :step))
                 (zerop (physical-reseed-follow-activation-context-prefix context))
                 (eq :inert (getf (getf record :configuration) :phase))
                 (eq :handed-off (getf (getf record :settlement) :phase))
                 (= 1 (length generations))
                 (eq :proved (getf (first generations) :phase))
                 (null (getf record :retirement)))
      (error "activation source facts require settled prefix zero"))
    (physical-reseed-follow-activation-require-attempt context)
    (let* ((layout (physical-reseed-follow-activation-layout
                    context (lambda () (physical-reseed-follow-activation-guard context))))
           (active (getf layout :active)))
      (unless (and (equal "follow.active" (first active))
                   (equal "follow.active" (third active))
                   (= (second active)
                      (physical-reseed-follow-configuration-role-fd
                       (physical-reseed-follow-activation-context-descriptors context) :staged)))
        (error "activation source observation lost its staged active file"))
      (physical-adoption-attempt-value-copy
       (list :follow (physical-reseed-follow-activation-context-snapshot context)
             :attempt (physical-reseed-follow-activation-context-attempt context)
             :install (physical-reseed-follow-activation-context-install-state context)
             :plan (physical-reseed-follow-activation-context-plan context)
             :active-entry (getf activation :entry)
             :active-row (fourth active))))))

(defun physical-reseed-follow-activation-local-snapshot (context &optional expected)
  ;; Run the full image proof between captures and compare both with any
  ;; expected detached facts.
  (let ((current (physical-reseed-follow-activation-local-facts context)))
    (physical-reseed-follow-activation-verify context)
    (unless (and (equal current (physical-reseed-follow-activation-local-facts context))
                 (or (null expected) (equal expected current)))
      (error "activation predecessors changed during source observation"))
    current))

(defun physical-reseed-follow-activation-network-snapshot (owner &optional expected)
  ;; Capture detached journal, attempt, install, plan and active-file facts
  ;; under the mapped proof.  Repeat the capture around the check so the caller
  ;; can compare it after network work.  Return copied values after releasing
  ;; the held descriptors.  Keep this tuple private because the original
  ;; attempt contains frozen source credentials.
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "activation source observation requires an active owner operation"))
  (call-with-physical-reseed-follow-image
   owner
   (lambda (root check)
     (declare (ignore check))
     (let ((context (physical-reseed-follow-activation-context owner)))
       (unless (eq :staged (getf (getf (physical-reseed-follow-activation-context-record context) :activation) :phase))
         (error "activation source observation requires one settled Staged generation"))
       (flet ((temporary-absent ()
                (physical-adoption-require-absent
                 (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
                 "activation temporary requires local recovery before source observation")))
         (temporary-absent)
         (multiple-value-prog1
             (physical-reseed-follow-activation-local-snapshot context expected)
           (temporary-absent)))))))

(defun physical-reseed-follow-activation-initial-temporary (context)
  ;; Check the complete temporary and its stable named identity before it can
  ;; be used in source-authorized recovery.
  (physical-reseed-follow-activation-guard context)
  (physical-reseed-follow-activation-require-attempt context)
  (let* ((root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-activation-context-descriptors context) :root))
         (name "physical-reseed.follow.tmp")
         (before (nth-value 1 (physical-reseed-follow-entry root name t))))
    (unless before (error "initial activation recovery requires its complete temporary"))
    (multiple-value-bind (record identity) (physical-reseed-follow-read-at root name)
      (unless (and (equal record (physical-reseed-follow-activation-intent-record
                                  (physical-reseed-follow-activation-context-record context)))
                   (equal before (nth-value 1 (physical-reseed-follow-entry root name t))))
        (error "initial activation temporary differs from its exact next intent"))
      (physical-reseed-follow-activation-guard context)
      (physical-adoption-attempt-value-copy
       (list :record record :identity identity :row before)))))

(defun physical-reseed-follow-activation-recovery-network-snapshot (owner &optional expected)
  ;; Pair detached local facts with the exact decoded next intent and stable
  ;; temporary metadata.  Recheck the temporary around the image proof.
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "activation source observation requires an active owner operation"))
  (call-with-physical-reseed-follow-image
   owner
   (lambda (root check)
     (declare (ignore root))
     (let* ((context (physical-reseed-follow-activation-context owner))
            (temporary (physical-reseed-follow-activation-initial-temporary context))
            (current (physical-reseed-follow-activation-local-snapshot context))
            (snapshot (append current (list :temporary temporary))))
       (funcall check)
       (unless (and (equal temporary (physical-reseed-follow-activation-initial-temporary context))
                    (or (null expected) (equal expected snapshot)))
         (error "initial activation recovery predecessors changed during source observation"))
       snapshot))))

(defun call-with-physical-reseed-follow-activation-observation (owner function)
  ;; Keep the owner busy through client close and the final snapshot check.
  ;; This observation scope preserves the uncertainty state.
  (physical-reseed-follow-check-idle owner)
  (setf (physical-reseed-follow-owner-busy-p owner) t)
  (unwind-protect (funcall function)
    (setf (physical-reseed-follow-owner-busy-p owner) nil)))

(defun physical-reseed-follow-probe-activation-source (owner &key helper recovery)
  ;; Retry the exact selected allocation through the original frozen route
  ;; and credentials.  Recheck local facts around the exchange and after client
  ;; close, keeping the owner busy until cleanup finishes.  Local validation is
  ;; read-only, so failures preserve owner certainty.  The source observation
  ;; can become stale as the source changes.  A fresh observation is needed
  ;; before publishing the initial Activating step zero intent.
  (call-with-physical-reseed-follow-activation-observation owner
                                                           (lambda ()
                                                             (let* ((observe (if recovery #'physical-reseed-follow-activation-recovery-network-snapshot
                                                                                 #'physical-reseed-follow-activation-network-snapshot))
                                                                    (snapshot (funcall observe owner))
                                                                    (record (getf (getf snapshot :follow) :record))
                                                                    (attempt (getf record :attempt))
                                                                    (source (physical-follow-source-copy (getf attempt :source)))
                                                                    (request (physical-backup-lease-request-copy (getf attempt :request)))
                                                                    (generation (first (getf record :generations)))
                                                                    (expected (physical-reseed-follow-generation-anchor generation))
                                                                    (client nil) (anchor nil))
                                                               (flet ((check () (funcall observe owner snapshot)))
                                                                 (unwind-protect
                                                                      (let ((frozen (nth-value 1 (physical-follow-source-profile source))))
                                                                        (setf client
                                                                              (physical-libpq-connect-frozen
                                                                               (or helper (physical-libpq-helper-path :on-spawn (lambda (actor) (setf client actor))))
                                                                               frozen :on-spawn (lambda (actor) (setf client actor))))
                                                                        (physical-adoption-attempt-check-client client source)
                                                                        (check)
                                                                        (let ((position (physical-reseed-follow-observe-source client source generation)))
                                                                          (check)
                                                                          (setf anchor
                                                                                (physical-follow-anchor-copy
                                                                                 (pg-client-native-backup-lease-follow-prepare
                                                                                  client (physical-backup-lease-request-copy request)
                                                                                  (getf generation :generation) (copy-seq (getf generation :name))
                                                                                  (physical-follow-request-copy (getf generation :coordinates)))))
                                                                          (check)
                                                                          (physical-adoption-attempt-check-client client source)
                                                                          (unless (equal expected anchor)
                                                                            (error "selected source allocation differs from its exact recorded anchor"))
                                                                          (physical-reseed-follow-observe-source client source generation :minimum position)
                                                                          (check)))
                                                                   (when client (physical-client-close client)))
                                                                 (check)
                                                                 (values anchor snapshot))))))

(defun physical-reseed-follow-activation-staged-record (value entry)
  ;; Construct and check the Staged successor without changing disk state.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation)))
    (unless (and (eq :building (getf activation :phase)) (zerop (getf activation :step)))
      (error "staged activation requires Building at step zero"))
    (setf (getf activation :phase) :staged
          (getf activation :entry) (physical-adoption-attempt-value-copy entry))
    (let ((candidate (physical-reseed-follow-copy record)))
      (physical-reseed-follow-check-transition value candidate)
      candidate)))

(defun physical-reseed-follow-activation-intent-record (value)
  ;; Construct the initial transition as a checked value.  Publication
  ;; requires the separate source authority held by the operation.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation)))
    (unless (and (eq :staged (getf activation :phase))
                 (zerop (getf activation :step))
                 (null (getf record :retirement)))
      (error "activation intent requires its exact Staged predecessor"))
    (setf (getf activation :phase) :activating)
    (let ((candidate (physical-reseed-follow-copy record)))
      (physical-reseed-follow-check-transition value candidate)
      candidate)))

(defun physical-reseed-follow-activation-progress-record (value)
  ;; Copy and check the next Activating step, then the Active state at step 2.
  ;; Retirement must be absent.  Callers must prove the required filesystem
  ;; state is durable and hold publication authority before writing this value.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation)))
    (unless (and (eq :activating (getf activation :phase))
                 (null (getf record :retirement)))
      (error "activation progress requires an Activating predecessor"))
    (if (< (getf activation :step) 2)
        (incf (getf activation :step))
        (setf (getf activation :phase) :active))
    (let ((candidate (physical-reseed-follow-copy record)))
      (physical-reseed-follow-check-transition value candidate)
      candidate)))

(defvar *physical-reseed-follow-activation-progress* nil)

(defstruct (physical-reseed-follow-activation-progress
            (:constructor %make-physical-reseed-follow-activation-progress)
            (:print-function physical-libpq-print-private-object))
  ;; A private, thread-bound witness for one progress publication.  It holds
  ;; the predecessor, snapshot, candidate, synced prefix and layout facts.
  ;; PUBLISHED records the single adopted snapshot before the scope ends.
  ;; The context owns the borrowed descriptors and the printer keeps private
  ;; credentials out of output.
  context thread active-p before snapshot candidate prefix facts published)

(defun physical-reseed-follow-activation-progress (context)
  ;; Require the live witness for this context and thread.
  (let ((proof *physical-reseed-follow-activation-progress*))
    (unless (and (physical-reseed-follow-activation-progress-p proof)
                 (physical-reseed-follow-activation-progress-active-p proof)
                 (eq context (physical-reseed-follow-activation-progress-context proof))
                 (eq sb-thread:*current-thread*
                     (physical-reseed-follow-activation-progress-thread proof))
                 (eq context
                     (physical-reseed-follow-activation-context
                      (physical-reseed-follow-activation-context-owner context))))
      (error "activation progress requires its live thread-bound durability proof"))
    proof))

(defun physical-reseed-follow-activation-progress-observe (context)
  ;; Capture the layout facts that must remain fixed during publication.  Root
  ;; directory size and times, and the two journal names, can change as the
  ;; journal is written and adopted.  Other captured facts remain checked.
  (let* ((layout (physical-reseed-follow-activation-layout
                  context (lambda () (physical-reseed-follow-activation-guard context))))
         (names (physical-adoption-attempt-value-copy (getf layout :names))))
    (setf (getf names :root)
          (set-difference (getf names :root)
                          '("physical-reseed.follow" "physical-reseed.follow.tmp") :test #'equal))
    (physical-adoption-attempt-value-copy
     (list :attempt (physical-reseed-follow-activation-context-attempt context)
           :install (physical-reseed-follow-activation-context-install-state context)
           :plan (physical-reseed-follow-activation-context-plan context)
           :prefix (physical-reseed-follow-activation-context-prefix context)
           :new (getf layout :new) :images (getf layout :images)
           :active (getf layout :active) :old (getf layout :old)
           :directories (loop for row in (getf layout :directories)
                              collect (if (eq :root (first row)) (subseq row 0 7) row))
           :names names))))

(defun physical-reseed-follow-activation-check-progress-candidate (context value)
  ;; Require the exact successor frozen by the live progress witness.
  (let* ((proof (physical-reseed-follow-activation-progress context))
         (before (physical-reseed-follow-activation-progress-before proof))
         (candidate (physical-reseed-follow-copy value))
         (expected (physical-reseed-follow-activation-progress-record before)))
    (unless (and (equal candidate expected)
                 (equal candidate (physical-reseed-follow-activation-progress-candidate proof))
                 (equal (physical-reseed-follow-activation-context-record context)
                        (if (physical-reseed-follow-activation-progress-published proof) candidate before)))
      (error "activation progress differs from its frozen exact next transition"))
    candidate))

(defun physical-reseed-follow-activation-check-progress (context)
  ;; Recheck the candidate, attempt, prefix, journal snapshot and synced
  ;; layout facts before and after publication.
  (let* ((proof (physical-reseed-follow-activation-progress context))
         (published (physical-reseed-follow-activation-progress-published proof)))
    (physical-reseed-follow-activation-require-attempt context)
    (physical-reseed-follow-activation-check-progress-candidate
     context (physical-reseed-follow-activation-progress-candidate proof))
    (unless (and (= (physical-reseed-follow-activation-context-prefix context)
                    (physical-reseed-follow-activation-progress-prefix proof))
                 (equal (physical-reseed-follow-activation-context-snapshot context)
                        (or published (physical-reseed-follow-activation-progress-snapshot proof)))
                 (equal (physical-reseed-follow-activation-progress-observe context)
                        (physical-reseed-follow-activation-progress-facts proof)))
      (error "activation progress lost its synced local facts"))
    t))

(defun call-with-physical-reseed-follow-activation-progress (context check function &key recovery)
  ;; The initial source authority scope must end before local progress begins.
  ;; Sync the next move or its one-ahead layout, freeze the resulting facts,
  ;; and revoke the thread-bound witness after the callback.
  (when (or *physical-reseed-follow-activation-progress*
            *physical-reseed-follow-activation-authority*)
    (error "activation progress overlaps an active publication authority"))
  (let* ((before (physical-reseed-follow-copy
                  (physical-reseed-follow-activation-context-record context)))
         (candidate (physical-reseed-follow-activation-progress-record before))
         (step (getf (getf before :activation) :step))
         (prefix (min 2 (1+ step)))
         (snapshot (physical-adoption-attempt-value-copy
                    (physical-reseed-follow-activation-context-snapshot context))))
    (when (and recovery (/= prefix (physical-reseed-follow-activation-context-prefix context)))
      (error "activation recovery requires the move already at its candidate prefix"))
    (if (< step 2)
        (physical-reseed-follow-activation-next-move context check)
        (funcall check))
    (funcall check)
    (let* ((proof (%make-physical-reseed-follow-activation-progress
                   :context context :thread sb-thread:*current-thread* :active-p t
                   :before before :snapshot snapshot :candidate candidate :prefix prefix
                   :facts (physical-reseed-follow-activation-progress-observe context)))
           (*physical-reseed-follow-activation-progress* proof))
      (unwind-protect
           (progn
             (physical-reseed-follow-activation-check-progress context)
             (funcall function candidate))
        (setf (physical-reseed-follow-activation-progress-active-p proof) nil)))))

(defun physical-reseed-follow-activation-intent-candidate (context value)
  ;; Check the frozen pure successor against the original snapshot and anchor.
  ;; The context may hold the predecessor or the adopted successor.
  (let* ((authority (physical-reseed-follow-activation-authority context))
         (snapshot (physical-reseed-follow-activation-authority-snapshot authority))
         (before (getf (getf snapshot :follow) :record))
         (candidate (physical-reseed-follow-copy value))
         (expected (physical-reseed-follow-activation-intent-record before))
         (current (physical-reseed-follow-activation-context-record context)))
    (unless (and (equal candidate expected)
                 (equal candidate (physical-reseed-follow-activation-authority-candidate authority))
                 (equal (physical-reseed-follow-activation-authority-anchor authority)
                        (physical-reseed-follow-generation-anchor (first (getf before :generations))))
                 (or (equal current before)
                     (and (physical-reseed-follow-activation-authority-published authority)
                          (equal current candidate))))
      (error "activation intent differs from its frozen source-authorized transition"))
    candidate))

(defun physical-reseed-follow-activation-authority-check (context)
  ;; Recheck the copied facts and candidate.  Recovery checks its temporary
  ;; before rename and requires its absence after publication.
  (let* ((authority (physical-reseed-follow-activation-authority context))
         (expected (physical-adoption-attempt-value-copy
                    (physical-reseed-follow-activation-authority-snapshot authority)))
         (temporary (getf expected :temporary))
         (published (physical-reseed-follow-activation-authority-published authority)))
    (physical-reseed-follow-activation-intent-candidate
     context (physical-reseed-follow-activation-authority-candidate authority))
    (remf expected :temporary)
    (if published
        (let ((root (physical-reseed-follow-configuration-role-fd
                     (physical-reseed-follow-activation-context-descriptors context) :root)))
          (setf (getf expected :follow) published)
          (physical-adoption-require-absent
           (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
           "published activation retained an unexpected temporary"))
        (when temporary
          (unless (equal temporary (physical-reseed-follow-activation-initial-temporary context))
            (error "source-authorized activation temporary changed"))))
    (unless (equal expected (physical-reseed-follow-activation-local-facts context))
      (error "source-authorized activation predecessors changed")))
  t)

(defun call-with-physical-reseed-follow-activation-authority (context anchor snapshot function)
  ;; Bind the copied snapshot, canonical anchor and candidate to this owner,
  ;; context and thread.  Revoke the private authority when the callback exits.
  (when *physical-reseed-follow-activation-authority*
    (error "activation source authority is already active"))
  (let ((local (physical-adoption-attempt-value-copy snapshot)))
    (remf local :temporary)
    (physical-reseed-follow-activation-local-snapshot context local))
  (let* ((record (getf (getf snapshot :follow) :record))
         (authority (%make-physical-reseed-follow-activation-authority
                     :owner (physical-reseed-follow-activation-context-owner context)
                     :context context :thread sb-thread:*current-thread* :active-p t
                     :snapshot (physical-adoption-attempt-value-copy snapshot)
                     :anchor (physical-follow-anchor-copy anchor)
                     :candidate (physical-reseed-follow-activation-intent-record record)))
         (*physical-reseed-follow-activation-authority* authority))
    (unwind-protect
         (progn
           (physical-reseed-follow-activation-authority-check context)
           (funcall function authority))
      (setf (physical-reseed-follow-activation-authority-active-p authority) nil))))

(defun physical-reseed-follow-activation-building-candidate (context value)
  ;; Accept only the first Building record, bound to the derived image and
  ;; an absent output at prefix zero.
  (let* ((before (physical-reseed-follow-activation-context-record context))
         (candidate (physical-reseed-follow-copy value))
         (binding (physical-reseed-follow-activation-context-binding context))
         (bytes (physical-follow-config-image
                 (physical-reseed-follow-activation-config before binding)))
         (expected (list :phase :building :step 0
                         :image (list (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))
                         :entry nil)))
    (physical-reseed-follow-check-transition before candidate)
    (unless (and (null (getf before :activation))
                 (null (getf before :retirement)) (null (getf candidate :retirement))
                 (zerop (physical-reseed-follow-activation-context-prefix context))
                 (null (physical-reseed-follow-activation-context-output-entry context))
                 (equal expected (getf candidate :activation)))
      (error "activation publication requires the exact first Building image and absent output"))
    candidate))

(defun physical-reseed-follow-activation-check-durable-output (context candidate)
  ;; Match the candidate and current named metadata to the output witness
  ;; captured after synchronization in this scope.
  (let* ((descriptors (physical-reseed-follow-activation-context-descriptors context))
         (parent (physical-reseed-follow-configuration-role-fd descriptors :staged))
         (proof (physical-reseed-follow-activation-context-durable-output context))
         (entry (getf (getf candidate :activation) :entry))
         (mount (physical-adoption-install-mount-id nil :directory-fd parent)))
    (unless (and proof entry
                 (equal entry (getf proof :entry))
                 (equal entry (physical-reseed-follow-activation-context-output-entry context))
                 (equal (getf proof :row)
                        (physical-reseed-install-old-observe parent "follow.active" mount)))
      (error "staged activation lacks its unchanged durable output proof")))
  t)

(defun physical-reseed-follow-activation-staged-candidate (context value &optional (durable t))
  ;; Check the exact Building successor.  Recovery first checks its shape,
  ;; then establishes and checks the durability witness before publication.
  (let* ((before (physical-reseed-follow-activation-context-record context))
         (candidate (physical-reseed-follow-copy value))
         (entry (physical-reseed-follow-activation-context-output-entry context)))
    (unless (and entry (zerop (physical-reseed-follow-activation-context-prefix context))
                 (null (getf before :retirement)) (null (getf candidate :retirement))
                 (equal candidate (physical-reseed-follow-activation-staged-record before entry)))
      (error "staged activation differs from its exact Building predecessor and output"))
    (when durable (physical-reseed-follow-activation-check-durable-output context candidate))
    candidate))

(defun physical-reseed-follow-activation-publication-scope-check (owner context candidate)
  ;; Check exclusive candidate binding and the exact active original attempt.
  ;; This boundary check is smaller than the full image proof.
  (unless (and (eq context (physical-reseed-follow-activation-context owner))
               (equal candidate *physical-reseed-follow-activation-candidate*)
               (not (or *physical-reseed-follow-installation-scope*
                        *physical-reseed-follow-installation-candidate*
                        *physical-reseed-follow-settlement-candidate*
                        *physical-reseed-follow-terminal-settlement-candidate*)))
    (error "activation publication lacks its exclusive frozen candidate"))
  (physical-reseed-follow-activation-require-attempt context)
  t)

(defun physical-reseed-follow-activation-publication-check (owner context candidate)
  ;; Initial Activating intent requires source authority.  Staged and later
  ;; progress use their respective local durability proofs.
  (physical-reseed-follow-activation-publication-scope-check owner context candidate)
  (case (getf (getf candidate :activation) :phase)
    (:staged (physical-reseed-follow-activation-check-durable-output context candidate))
    (:activating
     (if (zerop (getf (getf candidate :activation) :step))
         (physical-reseed-follow-activation-authority-check context)
         (physical-reseed-follow-activation-check-progress context)))
    (:active (physical-reseed-follow-activation-check-progress context)))
  t)

(defmethod physical-reseed-follow-configuration-publication-candidate
    ((owner physical-reseed-follow-configuration-owner)
     (context physical-reseed-follow-activation-context) candidate)
  ;; Check the operation and validate the exact successor for its phase.
  (physical-reseed-follow-activation-publication-check owner context candidate)
  (case (getf (getf candidate :activation) :phase)
    (:building (physical-reseed-follow-activation-building-candidate context candidate))
    (:staged (physical-reseed-follow-activation-staged-candidate context candidate))
    (:activating
     (if (zerop (getf (getf candidate :activation) :step))
         (physical-reseed-follow-activation-intent-candidate context candidate)
         (physical-reseed-follow-activation-check-progress-candidate context candidate)))
    (:active (physical-reseed-follow-activation-check-progress-candidate context candidate))
    (otherwise (error "unsupported activation publication phase"))))

(defmethod physical-reseed-follow-published-snapshot :around
    ((owner physical-reseed-follow-configuration-owner) root-fd candidate snapshot previous)
  ;; Adopt the checked snapshot once at the rename boundary before later
  ;; guards run, then recheck the corresponding authority or progress proof.
  (unless *physical-reseed-follow-activation-scope*
    (return-from physical-reseed-follow-published-snapshot (call-next-method)))
  (let* ((context (physical-reseed-follow-activation-context owner))
         (activation (getf candidate :activation))
         (phase (getf activation :phase))
         (intent (and (eq :activating phase) (zerop (getf activation :step))))
         (progress (or (eq :active phase)
                       (and (eq :activating phase) (plusp (getf activation :step))))))
    (when (or intent progress)
      (physical-reseed-follow-activation-publication-scope-check owner context candidate))
    (unless (and (equal candidate
                        (cond (intent (physical-reseed-follow-activation-intent-candidate context candidate))
                              (progress
                               (physical-reseed-follow-activation-check-progress-candidate context candidate))
                              (t (physical-reseed-follow-configuration-publication-candidate
                                  owner context candidate))))
                 (= root-fd (physical-reseed-follow-configuration-role-fd
                             (physical-reseed-follow-activation-context-descriptors context) :root))
                 (equal previous (physical-reseed-follow-activation-context-snapshot context))
                 (equal (physical-reseed-follow-activation-context-record context) (getf previous :record))
                 (equal candidate (getf snapshot :record)))
      (error "published activation differs from its frozen candidate or predecessor"))
    (when intent
      (let ((authority (physical-reseed-follow-activation-authority context)))
        (unless (and (null (physical-reseed-follow-activation-authority-published authority))
                     (equal previous
                            (getf (physical-reseed-follow-activation-authority-snapshot authority) :follow)))
          (error "source authority already adopted a journal or changed its predecessor"))
        (setf (physical-reseed-follow-activation-authority-published authority)
              (physical-adoption-attempt-value-copy snapshot))))
    (when progress
      (let ((proof (physical-reseed-follow-activation-progress context)))
        (unless (and (null (physical-reseed-follow-activation-progress-published proof))
                     (equal previous (physical-reseed-follow-activation-progress-snapshot proof)))
          (error "activation progress already adopted a journal or changed its predecessor"))
        (setf (physical-reseed-follow-activation-progress-published proof)
              (physical-adoption-attempt-value-copy snapshot))))
    (setf (physical-reseed-follow-activation-context-record context) (physical-reseed-follow-copy candidate)
          (physical-reseed-follow-activation-context-snapshot context) snapshot)
    (when intent (physical-reseed-follow-activation-authority-check context))
    (when progress (physical-reseed-follow-activation-check-progress context)))
  t)

(defun physical-reseed-follow-activation-publish (context value check)
  ;; Check the candidate before creating the temporary.  Building reserves
  ;; future capacity, and Staged uses that reservation.  Keep checking the
  ;; frozen kind and attempt during the write.
  (let* ((candidate (physical-reseed-follow-copy value))
         (*physical-reseed-follow-activation-candidate* (physical-reseed-follow-copy candidate))
         (owner (physical-reseed-follow-activation-context-owner context))
         (previous (physical-reseed-follow-activation-context-snapshot context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-activation-context-descriptors context) :root)))
    (funcall check)
    (physical-reseed-follow-configuration-publication-candidate owner context candidate)
    (when (eq :building (getf (getf candidate :activation) :phase))
      (physical-reseed-follow-check-future-capacity candidate))
    (flet ((guard ()
             (funcall check)
             (physical-reseed-follow-activation-publication-check owner context candidate)))
      (multiple-value-bind (record snapshot)
          (physical-reseed-follow-write-intent
           owner root candidate (physical-reseed-follow-image candidate) previous #'guard)
        (unless (and (equal record (physical-reseed-follow-activation-context-record context))
                     (equal snapshot (physical-reseed-follow-activation-context-snapshot context)))
          (error "activation publication did not adopt its canonical readback"))
        (values record snapshot)))))

(defun physical-reseed-follow-activation-publish-building (context value check)
  ;; Restrict this entry point to the first Building candidate.
  (physical-reseed-follow-activation-publish
   context (physical-reseed-follow-activation-building-candidate context value) check))

(defun physical-reseed-follow-begin-activation (owner)
  ;; Publish the first Building intent or return the existing Building record.
  ;; An existing temporary must be recovered explicitly.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (record (physical-reseed-follow-copy (physical-reseed-follow-activation-context-record context)))
            (activation (getf record :activation)))
       (funcall check)
       (physical-reseed-follow-activation-require-attempt context)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "activation temporary requires explicit recovery")
       (if activation
           (progn
             (unless (eq :building (getf activation :phase))
               (error "activation has already advanced beyond Building"))
             (values record (physical-reseed-follow-activation-context-snapshot context)))
           (let* ((bytes (physical-follow-config-image
                          (physical-reseed-follow-activation-config
                           record (physical-reseed-follow-activation-context-binding context))))
                  (image (list (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))))
             (setf (getf record :activation)
                   (list :phase :building :step 0 :image image :entry nil))
             (physical-reseed-follow-activation-publish-building context record check)))))))

(defun physical-reseed-follow-activation-existing-intent (owner)
  ;; Require the active original attempt when retrying a committed initial
  ;; intent offline.
  (call-with-physical-reseed-follow-activation-observation owner
                                                           (lambda ()
                                                             (call-with-physical-reseed-follow-image
                                                              owner
                                                              (lambda (root check)
                                                                (let* ((context (physical-reseed-follow-activation-context owner))
                                                                       (record (physical-reseed-follow-activation-context-record context))
                                                                       (activation (getf record :activation)))
                                                                  (funcall check)
                                                                  (physical-reseed-follow-activation-require-attempt context)
                                                                  (physical-adoption-require-absent
                                                                   (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
                                                                   "activation temporary requires explicit recovery")
                                                                  (when (and (eq :activating (getf activation :phase))
                                                                             (zerop (getf activation :step)))
                                                                    (physical-reseed-follow-copy record))))))))

(defun physical-reseed-follow-start-activation (owner &key helper)
  ;; Return an already committed initial intent offline.  A new one requires
  ;; fresh source observation and a checked publication scope.  The observation
  ;; can become stale, and START must later recheck floors.
  (or (physical-reseed-follow-activation-existing-intent owner)
      (multiple-value-bind (anchor snapshot)
          (physical-reseed-follow-probe-activation-source owner :helper helper)
        (call-with-physical-reseed-follow-activation-operation
         owner
         (lambda (root check)
           (let ((context (physical-reseed-follow-activation-context owner)))
             (physical-adoption-require-absent
              (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
              "activation temporary requires explicit recovery")
             (call-with-physical-reseed-follow-activation-authority
              context anchor snapshot
              (lambda (authority)
                (physical-reseed-follow-activation-publish
                 context (physical-reseed-follow-activation-authority-candidate authority) check)))))))))

(defun physical-reseed-follow-recover-local-activation (owner)
  ;; Hold and preflight the temporary through resynchronization and promotion.
  ;; Progress recovery accepts the exact next record at its moved prefix.
  ;; Building and Staged retain their local recovery checks.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (previous (physical-reseed-follow-activation-context-snapshot context)))
       (multiple-value-bind (temporary row)
           (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
         (if temporary
             (let ((fd nil))
               (unwind-protect
                    (progn
                      (sb-sys:without-interrupts
                        (setf fd (physical-image-openat
                                  root "physical-reseed.follow.tmp"
                                  (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                          +postmaster-o-cloexec+) 0)))
                      (when (minusp fd)
                        (setf fd nil)
                        (error "cannot open activation recovery temporary"))
                      (let* ((candidate (physical-reseed-follow-read-at root "physical-reseed.follow.tmp"))
                             (*physical-reseed-follow-activation-candidate*
                               (physical-reseed-follow-copy candidate)))
                        (labels ((preflight ()
                                   (funcall check)
                                   (physical-reseed-follow-activation-require-attempt context)
                                   (unless (and
                                            (equal temporary
                                                   (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                                            (= (physical-adoption-install-mount-id nil :directory-fd root)
                                               (physical-adoption-install-mount-id nil :directory-fd fd))
                                            (equal row (nth-value 1 (physical-reseed-follow-entry
                                                                     root "physical-reseed.follow.tmp")))
                                            (equal candidate (physical-reseed-follow-read-at
                                                              root "physical-reseed.follow.tmp"))
                                            (equal row (nth-value 1 (physical-reseed-follow-entry
                                                                     root "physical-reseed.follow.tmp")))
                                            (equal temporary
                                                   (physical-reseed-follow-file-identity (sb-posix:fstat fd))))
                                     (error "activation recovery temporary changed during preflight")))
                                 (promote ()
                                   (preflight)
                                   (physical-reseed-follow-configuration-publication-candidate
                                    owner context candidate)
                                   (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                                   (physical-reseed-follow-promote-intent
                                    owner root fd (subseq temporary 0 5) candidate previous
                                    (lambda ()
                                      (funcall check)
                                      (when (equal previous
                                                   (physical-reseed-follow-activation-context-snapshot context))
                                        (preflight))
                                      (physical-reseed-follow-activation-publication-check owner context candidate)))))
                          (preflight)
                          (if (eq :activating
                                  (getf (getf (physical-reseed-follow-activation-context-record context)
                                              :activation) :phase))
                              (progn
                                (unless (equal candidate
                                               (physical-reseed-follow-activation-progress-record
                                                (physical-reseed-follow-activation-context-record context)))
                                  (error "activation recovery temporary is not the exact next progress record"))
                                (call-with-physical-reseed-follow-activation-progress
                                 context #'preflight
                                 (lambda (expected)
                                   (unless (equal candidate expected)
                                     (error "activation recovery candidate changed during synchronization"))
                                   (promote))
                                 :recovery t))
                              (progn
                                (physical-reseed-follow-activation-prepare-recovery context candidate #'preflight)
                                (promote))))))
                 (when fd (sb-posix:close fd))))
             (progn
               (setf (physical-reseed-follow-owner-uncertain-p owner) t)
               (physical-reseed-follow-read-at root "physical-reseed.follow" :stabilize t)
               (funcall check)
               (values (physical-reseed-follow-copy
                        (physical-reseed-follow-activation-context-record context))
                       (physical-reseed-follow-activation-context-snapshot context)))))))))

(defun physical-reseed-follow-activation-source-recovery-p (owner)
  ;; Enter the activation image directly so recovery can inspect the journal
  ;; before the first Building final exists.  Staged with a temporary needs
  ;; the source-authorized recovery path.
  (call-with-physical-reseed-follow-activation-observation owner
                                                           (lambda ()
                                                             (call-with-physical-reseed-follow-activation-image
                                                              owner
                                                              (lambda (root check)
                                                                (declare (ignore check))
                                                                (let ((context (physical-reseed-follow-activation-context owner)))
                                                                  (and (eq :staged (getf (getf (physical-reseed-follow-activation-context-record context)
                                                                                               :activation) :phase))
                                                                       (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
                                                                       t)))))))

(defun physical-reseed-follow-recover-activation-intent (owner helper)
  ;; Reobserve the source before promoting a complete initial Activating
  ;; temporary.  Bind its checked identity to the promotion.
  (multiple-value-bind (anchor snapshot)
      (physical-reseed-follow-probe-activation-source owner :helper helper :recovery t)
    (call-with-physical-reseed-follow-activation-operation
     owner
     (lambda (root check)
       (let ((context (physical-reseed-follow-activation-context owner)))
         (call-with-physical-reseed-follow-activation-authority
          context anchor snapshot
          (lambda (authority)
            (let* ((candidate (physical-reseed-follow-copy
                               (physical-reseed-follow-activation-authority-candidate authority)))
                   (*physical-reseed-follow-activation-candidate* (physical-reseed-follow-copy candidate))
                   (previous (getf snapshot :follow))
                   (temporary (getf snapshot :temporary))
                   (fd nil))
              (physical-reseed-follow-configuration-publication-candidate owner context candidate)
              (unwind-protect
                   (progn
                     (funcall check)
                     (physical-reseed-follow-activation-authority-check context)
                     (sb-sys:without-interrupts
                       (setf fd (physical-image-openat
                                 root "physical-reseed.follow.tmp"
                                 (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                         +postmaster-o-cloexec+) 0)))
                     (when (minusp fd) (setf fd nil) (error "cannot open initial activation recovery temporary"))
                     (unless (equal (getf temporary :identity)
                                    (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                       (error "initial activation recovery temporary changed before open"))
                     (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                     (physical-reseed-follow-promote-intent
                      owner root fd (subseq (getf temporary :identity) 0 5) candidate previous
                      (lambda ()
                        (funcall check)
                        (physical-reseed-follow-activation-publication-check owner context candidate))))
                (when fd (sb-posix:close fd)))))))))))

(defun physical-reseed-follow-recover-activation (owner &key helper)
  ;; Fence the owner if recovery fails after its idle check.
  ;; A complete initial Activating/0 temporary needs a fresh source observation.
  ;; Building, Staged and later progress candidates recover locally.
  (physical-reseed-follow-check-idle owner)
  (let ((completed nil))
    (unwind-protect
         (multiple-value-prog1
             (if (physical-reseed-follow-activation-source-recovery-p owner)
                 (physical-reseed-follow-recover-activation-intent owner helper)
                 (physical-reseed-follow-recover-local-activation owner))
           (setf completed t))
      (unless completed (setf (physical-reseed-follow-owner-uncertain-p owner) t)))))

(defun physical-reseed-follow-activation-output-guard (context)
  ;; Guard output work while the layout may be partial.  The complete proof
  ;; runs again after the output has been checked and adopted.
  (physical-reseed-follow-activation-guard context)
  (physical-reseed-follow-activation-require-attempt context))

(defun physical-reseed-follow-activation-sync-output (context entry check)
  ;; Hold the same file descriptor through synchronization and identity checks.
  ;; Sync the file and its directories, then bind the checked entry and named
  ;; metadata as the scope's durability witness.  Recheck after adoption.
  (declare (notinline sb-posix:fsync))
  (let* ((owner (physical-reseed-follow-activation-context-owner context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (record (physical-reseed-follow-activation-context-record context))
         (binding (physical-reseed-follow-activation-context-binding context))
         (parent (physical-reseed-follow-configuration-role-fd descriptors :staged))
         (workspace (physical-reseed-follow-configuration-role-fd descriptors :workspace))
         (path (merge-pathnames "follow.active" (physical-image-fd-path parent)))
         (bytes (physical-follow-config-image (physical-reseed-follow-activation-config record binding))))
    (unless (and entry (eq :building (getf (getf record :activation) :phase))
                 (zerop (physical-reseed-follow-activation-context-prefix context)))
      (error "output synchronization requires the exact Building file"))
    (physical-reseed-follow-activation-output-guard context)
    (call-with-physical-image-input-at
     parent "follow.active"
     (lambda (stream)
       (let* ((fd (sb-sys:fd-stream-fd stream))
              (fixed (subseq (physical-reseed-follow-file-identity (sb-posix:fstat fd) t) 0 5)))
         (labels ((held ()
                    (physical-reseed-follow-activation-output-guard context)
                    (physical-reseed-follow-configuration-output-check parent fd path fixed)
                    (unless (equal entry (physical-reseed-follow-configuration-file-row
                                          parent "follow.active" "follow.active" bytes))
                      (error "active output differs from its captured file"))
                    (physical-reseed-follow-configuration-output-check parent fd path fixed))
                  (point (event)
                    (held)
                    (physical-reseed-follow-test-point event path)
                    (held)))
           (held)
           (setf (physical-reseed-follow-owner-uncertain-p owner) t)
           (point :before-activation-output-fsync)
           (sb-posix:fsync fd)
           (point :after-activation-output-fsync)
           (point :before-activation-staged-fsync)
           (sb-posix:fsync parent)
           (point :after-activation-staged-fsync)
           (point :before-activation-workspace-fsync)
           (sb-posix:fsync workspace)
           (point :after-activation-workspace-fsync)
           (point :before-activation-ancestors-fsync)
           (physical-adoption-install-sync-ancestors
            (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
           (point :after-activation-ancestors-fsync)
           (let* ((observed (physical-reseed-follow-activation-output-state
                             owner descriptors record binding
                             (physical-reseed-follow-activation-context-images context)
                             (lambda () (physical-reseed-follow-activation-output-guard context))))
                  (actual (getf observed :entry))
                  (mount (physical-adoption-install-mount-id nil :directory-fd parent)))
             (unless (equal entry actual) (error "durable active output changed before adoption"))
             (held)
             (setf (physical-reseed-follow-activation-context-output-entry context)
                   (physical-adoption-attempt-value-copy actual)
                   (physical-reseed-follow-activation-context-durable-output context)
                   (list :entry (physical-adoption-attempt-value-copy actual)
                         :row (physical-reseed-install-old-observe parent "follow.active" mount)))
             (funcall check)
             (held)
             actual))))
     :element-type '(unsigned-byte 8))))

(defun physical-reseed-follow-stage-activation (owner)
  ;; Create or check the active image, make it durable, and publish Staged.
  ;; This establishes local durability.  Current source checks are separate.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (record (physical-reseed-follow-activation-context-record context))
            (phase (getf (getf record :activation) :phase)))
       (funcall check)
       (physical-reseed-follow-activation-require-attempt context)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "activation temporary requires explicit recovery")
       (unless (member phase '(:building :staged))
         (error "activation staging requires Building or Staged intent"))
       (if (eq phase :staged)
           (values (physical-reseed-follow-copy record)
                   (physical-reseed-follow-activation-context-snapshot context))
           (let* ((descriptors (physical-reseed-follow-activation-context-descriptors context))
                  (parent (physical-reseed-follow-configuration-role-fd descriptors :staged))
                  (bytes (physical-follow-config-image
                          (physical-reseed-follow-activation-config
                           record (physical-reseed-follow-activation-context-binding context))))
                  (entry (physical-reseed-follow-configuration-write-file
                          owner parent "follow.active" "follow.active" bytes
                          (lambda () (physical-reseed-follow-activation-output-guard context)) :prepare t)))
             (physical-reseed-follow-activation-sync-output context entry check)
             (physical-reseed-follow-activation-publish
              context (physical-reseed-follow-activation-staged-record record entry) check)))))))

(defun physical-reseed-follow-activation-prepare-recovery (context candidate check)
  ;; Validate a Building or Staged transition before any recovery fsync.  For Staged,
  ;; reattach the output witness and check it before promoting the temporary.
  (case (getf (getf candidate :activation) :phase)
    (:building (physical-reseed-follow-activation-building-candidate context candidate))
    (:staged
     (physical-reseed-follow-activation-staged-candidate context candidate nil)
     (physical-reseed-follow-activation-sync-output
      context (getf (getf candidate :activation) :entry) check)
     (physical-reseed-follow-activation-staged-candidate context candidate))
    (otherwise (error "unsupported activation recovery transition"))))

(defun physical-reseed-follow-activation-move-file-check
    (context stream parent leaf logical bytes expected row)
  ;; Check the held file and its current name against the captured identity,
  ;; metadata and canonical bytes throughout the move.
  (physical-reseed-follow-activation-guard context)
  (physical-reseed-follow-activation-require-attempt context)
  (let* ((fd (sb-sys:fd-stream-fd stream))
         (mount (physical-adoption-install-mount-id nil :directory-fd parent))
         (actual (make-array (length bytes) :element-type '(unsigned-byte 8))))
    (unless (and (equal row (physical-reseed-install-old-observe parent leaf mount))
                 (equal (physical-reseed-install-row-file-facts row)
                        (physical-reseed-file-facts (sb-posix:fstat fd)))
                 (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                 (file-position stream 0)
                 (= (length bytes) (read-sequence actual stream))
                 (eq :eof (read-byte stream nil :eof))
                 (equalp bytes actual)
                 (equal expected
                        (physical-reseed-follow-configuration-file-row parent leaf logical bytes))
                 (equal row (physical-reseed-install-old-observe parent leaf mount))
                 (equal (physical-reseed-install-row-file-facts row)
                        (physical-reseed-file-facts (sb-posix:fstat fd))))
      (error "activation move file changed its held identity, metadata or canonical bytes")))
  (physical-reseed-follow-activation-guard context)
  t)

(defun physical-reseed-follow-activation-move-point (context check guard event)
  ;; Run the scope and held-file guards around each move fault point.
  (funcall check)
  (funcall guard)
  (physical-reseed-follow-test-point
   event (physical-image-fd-path
          (physical-reseed-follow-configuration-role-fd
           (physical-reseed-follow-activation-context-descriptors context) :root)))
  (funcall check)
  (funcall guard))

(defun physical-reseed-follow-activation-next-move (context check)
  ;; Keep the file descriptor open through the move and sync both directories.
  ;; Rename may change ctime while the other recorded file facts remain fixed.
  ;; A one-ahead layout is resynced without repeating the rename.
  (declare (notinline sb-posix:fsync physical-adoption-renameat2))
  (unless (eq context
              (physical-reseed-follow-activation-context
               (physical-reseed-follow-activation-context-owner context)))
    (error "activation move requires its current context"))
  (let* ((record (physical-reseed-follow-activation-context-record context))
         (activation (getf record :activation))
         (step (getf activation :step))
         (prefix (physical-reseed-follow-activation-context-prefix context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (binding (physical-reseed-follow-activation-context-binding context))
         (owner (physical-reseed-follow-activation-context-owner context)))
    (unless (and (eq :activating (getf activation :phase))
                 (< step 2) (member prefix (list step (1+ step))))
      (error "activation move requires its recorded step or one-ahead layout"))
    (funcall check)
    (physical-reseed-follow-activation-require-attempt context)
    (destructuring-bind (logical from-role from-leaf to-role to-leaf)
        (nth step (physical-reseed-follow-activation-moves record binding))
      (let* ((from (physical-reseed-follow-configuration-role-fd descriptors from-role))
             (to (physical-reseed-follow-configuration-role-fd descriptors to-role))
             (parent (if (= prefix step) from to))
             (leaf (if (= prefix step) from-leaf to-leaf))
             (bytes (if (zerop step)
                        (cdr (assoc logical (physical-reseed-follow-activation-context-images context)
                                    :test #'equal))
                        (physical-follow-config-image
                         (physical-reseed-follow-activation-config record binding))))
             (expected (if (zerop step)
                           (first (second (assoc logical (getf (getf record :configuration) :entries)
                                                 :test #'equal)))
                           (getf activation :entry)))
             (mount (physical-adoption-install-mount-id nil :directory-fd parent))
             (row (physical-reseed-install-old-observe parent leaf mount)))
        (call-with-physical-image-input-at
         parent leaf
         (lambda (stream)
           (flet ((guard ()
                    (physical-reseed-follow-activation-move-file-check
                     context stream parent leaf logical bytes expected row)))
             (flet ((point (event)
                      (physical-reseed-follow-activation-move-point context check #'guard event)))
               (guard)
               (setf (physical-reseed-follow-owner-uncertain-p owner) t)
               (point :before-activation-move-file-fsync)
               (sb-posix:fsync (sb-sys:fd-stream-fd stream))
               (point :after-activation-move-file-fsync)
               (when (= prefix step)
                 (point :before-activation-move)
                 (sb-sys:without-interrupts
                   (unless (zerop (physical-adoption-renameat2
                                   from from-leaf to to-leaf +physical-adoption-rename-noreplace+))
                     (error "cannot move activation file: errno ~d" (sb-alien:get-errno)))
                   (setf parent to leaf to-leaf
                         (physical-reseed-follow-activation-context-prefix context) (1+ step)))
                 (let ((after (physical-reseed-install-old-observe parent leaf mount)))
                   (unless (equal (subseq row 1 8) (subseq after 1 8))
                     (error "activation rename changed immutable file metadata"))
                   (setf row after))
                 (point :after-activation-move))
               (point :before-activation-moved-file-fsync)
               (sb-posix:fsync (sb-sys:fd-stream-fd stream))
               (point :after-activation-moved-file-fsync)
               (point :before-activation-source-directory-fsync)
               (sb-posix:fsync from)
               (point :after-activation-source-directory-fsync)
               (point :before-activation-destination-directory-fsync)
               (sb-posix:fsync to)
               (point :after-activation-destination-directory-fsync)
               (funcall check)
               (guard)
               (values (1+ step)
                       (physical-reseed-follow-activation-layout
                        context (lambda () (physical-reseed-follow-activation-guard context)))))))
         :element-type '(unsigned-byte 8))))))

(defun physical-reseed-follow-advance-activation (owner)
  ;; Advance one durable activation transition per call.  A checked Active
  ;; record is returned again on retry.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (record (physical-reseed-follow-activation-context-record context)))
       (physical-reseed-follow-activation-require-attempt context)
       (when (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
         (error "activation temporary requires recovery before advancement"))
       (if (eq :active (getf (getf record :activation) :phase))
           (progn
             (funcall check)
             (values (physical-reseed-follow-copy record)
                     (physical-reseed-follow-activation-context-snapshot context)))
           (call-with-physical-reseed-follow-activation-progress
            context check
            (lambda (candidate)
              (physical-reseed-follow-activation-publish context candidate check))))))))
