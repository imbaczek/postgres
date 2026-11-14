;;; Read-only, owner-bound import preflight for live phase 2 or live/cold
;;; phase 3. D is the copy base end and F the selected fork. The result is
;;; observational; promotion remains fenced until import publication is complete.
;;; PostgreSQL's src/backend/replication/logical/slotsync.c is a reference for
;;; retaining a first invalidation cause and leaving ineligible slots pending.
(in-package #:pg)

(defun logical-slot-copy-import-disposition (slot fact history fork parent)
  "Classify the source slot at F, preserving its first terminal cause.
Unknown or ahead positions defer; proved ON coverage must match its frozen origin."
  (unless (and (logical-slot-state-valid-p slot)
               (timeline-prefix-boundary-p parent fork history))
    (error "invalid logical import source positions"))
  (when fact (physical-logical-check-fact fact history fork parent))
  (let ((provenance (wal-replication-slot-logical-provenance slot)))
    (when (or (null provenance)
              (> (wal-replication-slot-restart-lsn slot) fork)
              (> (wal-replication-slot-consistent-lsn slot) fork)
              (and (wal-replication-slot-confirmed-flush-lsn slot)
                   (> (wal-replication-slot-confirmed-flush-lsn slot) fork)))
      (return-from logical-slot-copy-import-disposition :deferred))
    (logical-slot-check-primary-history slot history)
    (let ((cause (logical-slot-effective-invalidation-cause slot)))
      (unless (eq :none cause)
        (return-from logical-slot-copy-import-disposition cause)))
    (case (logical-slot-on-coverage
           (physical-logical-state-from-fact fact) history
           (wal-replication-slot-consistent-lsn slot)
           (logical-slot-provenance-birth-timeline provenance) :prefix-end fork parent)
      (:insufficient :wal-level-insufficient)
      (:unknown :deferred)
      (:proved-on
       (let ((origin (logical-slot-provenance-on-start-lsn provenance)))
         (cond
           ((null origin) :deferred)
           ((and (= origin (getf fact :logical-on-start-lsn))
                 (= (logical-slot-provenance-on-start-timeline provenance)
                    (getf fact :logical-on-start-timeline)))
            :eligible)
           (t (error "logical import ON origin differs from its copied source"))))))))

(defun logical-slot-copy-import-base-state (manifest descriptor control)
  "Build the explicit D seed from the descriptor checkpoint and base evidence.
D is the copy base end; the pending source checkpoint at F is distinct."
  (let* ((base (copy-tree manifest))
         (snapshot (third (logical-slot-copy-control-fields
                           (logical-slot-copy-descriptor-base-control descriptor))))
         (history (timeline-parse-history
                   (getf manifest :timeline)
                   (bytes->string (logical-slot-copy-descriptor-history descriptor))))
         (logical (physical-logical-base-fact manifest snapshot history))
         (fpw (physical-fpw-fact-from-state
               (wal-fpw-state-from-checkpoint snapshot (make-symbol "IMPORT-FPW"))
               (getf manifest :end) (getf manifest :timeline))))
    (setf (getf base :source-checkpoint) snapshot
          (getf base :source-reservation) (physical-recovery-initial-reservation manifest)
          (getf base :base-source-resource-fact) (wal-copy-control-resource-fields snapshot)
          (getf base :base-source-fpw-fact) fpw
          (getf base :base-source-logical-fact) logical)
    (dolist (pair '((:required-resource-fact :resource-fact)
                    (:required-fpw-fact :fpw-fact)
                    (:required-logical-fact :logical-fact)))
      (when (getf control (second pair))
        (setf (getf base (first pair)) (copy-tree (getf control (second pair))))))
    base))

(defun logical-slot-copy-import-scan (store state manifest descriptor control)
  "Check D through durable flush and derive logical evidence at clean F.
Capture the receive tuple and independent state image for later rechecking."
  (let* ((tuple (physical-store-control-tuple store))
         (state-image (logical-slot-copy-import-state-image state))
         (candidate (physical-follow-copy-checked-store store))
         (index (physical-follow-controller-index candidate))
         (fork (getf control :applied))
         (parent (getf control :applied-timeline (getf control :timeline)))
         (base (logical-slot-copy-import-base-state manifest descriptor control))
         (seed (list (getf base :end) (getf base :timeline) (getf base :next-xid)
                     (getf base :source-checkpoint) (getf base :source-reservation) nil
                     (getf base :base-source-resource-fact)
                     (getf base :base-source-fpw-fact) (getf base :base-source-logical-fact))))
    (physical-slot-check-standby-receive store state fork)
    (let* ((result (physical-follow-scan-replay-from-seed
                    candidate base (list fork parent) (list fork parent)
                    (getf control :next-xid) seed
                    (lambda (function start)
                      (physical-follow-map-active-prefix candidate index function start))
                    t))
           (fact (getf result :applied-logical-fact))
           (history (physical-store-retained-history candidate)))
      (unless (equal tuple (physical-store-control-tuple store))
        (error "logical import receive selection changed during source scan"))
      (values fact history tuple state-image))))

(defun logical-slot-copy-import-recovery-plan (owner control)
  "Select the live restartpoint or the pending promotion state and metadata."
  (if (= 3 (getf control :phase))
      (physical-promotion-read-pending-state)
      (let ((state (logical-slot-copy-import-owner-state owner)))
        (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
          (declare (ignore catalog))
          (values state metadata prefix generation)))))

(defun logical-slot-copy-import-state-image (value)
  "Copy recovery lists, strings, vectors and timeline entries to detect alias changes."
  (typecase value
    (timeline-history-entry (copy-timeline-history-entry value))
    (cons (cons (logical-slot-copy-import-state-image (car value))
                (logical-slot-copy-import-state-image (cdr value))))
    (string (copy-seq value))
    (vector
     (let ((copy (copy-seq value)))
       (when (equal t (array-element-type copy))
         (dotimes (index (length copy))
           (setf (aref copy index)
                 (logical-slot-copy-import-state-image (aref value index)))))
       copy))
    (t value)))

(defun logical-slot-copy-import-recheck-plan (owner control state state-image metadata prefix generation)
  "Recheck selected state, its independent image, metadata, prefix and generation."
  (multiple-value-bind (current current-metadata current-prefix current-generation)
      (logical-slot-copy-import-recovery-plan owner control)
    (unless (and (if (= 2 (getf control :phase)) (eq state current) (equalp state current))
                 (equalp state-image (logical-slot-copy-import-state-image current))
                 (equalp metadata current-metadata) (equalp prefix current-prefix)
                 (eql generation current-generation))
      (error "logical import selected restartpoint changed during source preparation"))))

(defun call-with-logical-slot-copy-import-store (owner state function)
  "Borrow the caller's live store, or open and close a cold pending store."
  (if (eq :live (logical-slot-copy-import-owner-mode owner))
      (funcall function (logical-slot-copy-import-owner-store owner))
      (let ((store nil))
        (unwind-protect
             (progn
               (physical-promotion-open-pending-store
                state :on-open (lambda (opened) (setf store opened)))
               (funcall function store))
          (when store (physical-store-close store))))))

(defun logical-slot-copy-import-check-progress
    (owner context state metadata prefix generation store)
  "Check selected progress under the owned root.
In phase 3, also check full checkpoint artifacts before the final source replay."
  (if (= 3 (getf state :phase))
      (physical-promotion-validate-cold-state
       state metadata prefix generation store #'physical-promotion-checkpoint-artifacts
       (cons (physical-adoption-owner-directory owner)
             (physical-image-context-root-fd context)))
      (progn
        (unless (and (eq state (logical-slot-copy-import-owner-state owner))
                     (eq store (logical-slot-copy-import-owner-store owner)))
          (error "logical import lost its captured live recovery state"))
        (physical-promotion-check-progress state metadata prefix generation))))

(defun logical-slot-copy-import-source-rows (descriptor fact history fork parent)
  "Return detached source bytes and classifications without changing slot profiles."
  (loop for (name bytes) in (logical-slot-copy-descriptor-slots descriptor)
        for slot = (logical-slot-state-decode bytes name)
        for provenance = (wal-replication-slot-logical-provenance slot)
        collect (list (copy-seq name)
                      (logical-slot-generation-token
                       (logical-slot-provenance-generation provenance))
                      (physical-store-update-digest #xCBF29CE484222325 bytes)
                      (bytes->string bytes)
                      (logical-slot-copy-import-disposition slot fact history fork parent))))

(defun logical-slot-copy-read-import-source
    (owner context store descriptor observations manifest control state metadata prefix generation)
  "Scan the retained source once and return detached classifications at F.
Recheck the selected plan, receive tuple, input observations, and sidecars."
  (multiple-value-bind (fact history tuple state-image)
      (logical-slot-copy-import-scan store state manifest descriptor control)
    (let* ((fork (getf control :applied))
           (parent (getf control :applied-timeline (getf control :timeline)))
           (rows (logical-slot-copy-import-source-rows descriptor fact history fork parent))
           (control-bytes (logical-slot-copy-read-file
                           context "physical-recovery.control"
                           +physical-recovery-control-maximum-bytes+))
           (value
             (list :copy-id (copy-seq (logical-slot-copy-descriptor-copy-id descriptor))
                   :device (physical-adoption-owner-device owner)
                   :inode (physical-adoption-owner-inode owner)
                   :manifest-digest (getf control :manifest-digest)
                   :descriptor-digest (third (find "logical-slot-copy.descriptor"
                                                   observations :key #'first :test #'equal))
                   :base-proof-digest (third (find "logical-slot-copy.base-verified"
                                                   observations :key #'first :test #'equal))
                   :system-id (getf manifest :system-id)
                   :base-end (getf manifest :end) :base-timeline (getf manifest :timeline)
                   :selected-history (mapcar (lambda (entry)
                                               (list (timeline-history-entry-timeline entry)
                                                     (timeline-history-entry-begin entry)
                                                     (timeline-history-entry-end entry)))
                                             history)
                   :fork fork :parent-timeline parent :phase (getf control :phase)
                   :control-bytes control-bytes :logical-fact fact
                   :eligible-interest (not (null (find :eligible rows :key #'fifth)))
                   :slots rows)))
      (when (and (= 3 (getf control :phase)) (getf value :eligible-interest)
                 (not (getf control :promotion-logical-decoding-enabled)))
        (error "pending promotion disabled eligible copied logical slots"))
      (logical-slot-copy-verification-test-point :after-import-source-scan context)
      (physical-slot-check-standby-receive store state fork)
      (unless (equal tuple (physical-store-control-tuple store))
        (error "logical import receive selection changed after source preparation"))
      (logical-slot-copy-import-recheck-plan
       owner control state state-image metadata prefix generation)
      (logical-slot-copy-input-check-observations owner context observations)
      (dolist (row observations)
        (unless (equal row (logical-slot-copy-file-row context (first row)))
          (error "logical import input bytes changed during source preparation")))
      (logical-slot-copy-input-sidecar-inventory context descriptor)
      (logical-slot-copy-check-input owner context)
      value)))

(defun call-with-logical-slot-copy-import-source-state (owner context function)
  "Bind the checked receipt and selected progress to a store and receive-root lease.
Call FUNCTION while that store and lease remain held."
  (multiple-value-bind (descriptor observations manifest control)
      (logical-slot-copy-check-import-receipt owner context)
    (let ((*pgdata* (physical-image-context-root-path context))
          (*physical-image-context* nil))
      (multiple-value-bind (state metadata prefix generation)
          (logical-slot-copy-import-recovery-plan owner control)
        (call-with-logical-slot-copy-import-store
         owner state
         (lambda (store)
           (logical-slot-copy-import-check-progress
            owner context state metadata prefix generation store)
           (physical-follow-call-with-receive-root
            store
            (lambda ()
              (funcall function store descriptor observations manifest control
                       state metadata prefix generation)))))))))

(defun logical-slot-copy-prepare-import-source (owner context)
  "Prepare detached observational source data under the owned read-only scope.
Check receipt, selected progress, retained receive root and final input observations.
This grants no publication or admission; public promotion remains fenced."
  (call-with-logical-slot-copy-import-source-state
   owner context
   (lambda (store descriptor observations manifest control state metadata prefix generation)
     (logical-slot-copy-read-import-source
      owner context store descriptor observations manifest control state metadata prefix generation))))
