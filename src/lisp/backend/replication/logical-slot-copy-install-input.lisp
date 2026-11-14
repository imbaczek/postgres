(in-package #:pg)

;;; Check certificate inputs independently of the certificate's installation phase.
;;; Live operation still requires recovery phase 3; cold operation requires unopened storage.
;;; This read scope grants no publication, adoption, or admission authority.
(defun logical-slot-copy-install-input-physical-facts (layout)
  (remove-if-not (lambda (row) (= 5 (length row))) (getf layout :observations)))

(defun logical-slot-copy-install-check-input-layout (owner input record &key (physical-proof t))
  "Check monotone phase and prefix and unchanged physical slot witnesses.
When requested, also check physical slot WAL coverage under the source lease."
  (let* ((operation (logical-slot-copy-install-input-state-operation record))
         (layout (logical-slot-copy-install-read-layout
                  operation (logical-slot-copy-install-input-state-image record)))
         (phase (getf layout :phase)) (prefix (getf layout :prefix))
         (previous (logical-slot-copy-install-input-state-phase record))
         (old-prefix (logical-slot-copy-install-input-state-prefix record))
         (physical (getf layout :physical))
         (mode (logical-slot-copy-committed-owner-mode owner))
         (slots (logical-slot-copy-install-physical-registry
                 mode physical (mapcar #'first (getf (logical-slot-copy-install-input-certificate input) :slots))))
         (rank '((:private . 0) (:pending . 1) (:active . 2))))
    (unless (and (>= (cdr (assoc phase rank)) (cdr (assoc previous rank))) (>= prefix old-prefix)
                 (or (logical-slot-copy-install-input-state-ready record)
                     (and (eq phase previous) (= prefix old-prefix))))
      (error "logical installation input layout moved backwards or changed before proof"))
    (unless (and (equalp (logical-slot-copy-install-input-state-physical-images record)
                         (mapcar #'physical-slot-encode physical))
                 (logical-slot-copy-committed-image-equal-p
                  (logical-slot-copy-install-input-state-physical-facts record)
                  (logical-slot-copy-install-input-physical-facts layout)))
      (error "logical installation physical inventory changed during its scope"))
    (when (eq mode :live)
      (logical-slot-copy-install-check-physical-witnesses
       (logical-slot-copy-install-input-state-physical-witnesses record) slots))
    (when physical-proof
      (physical-promotion-check-slot-prefix
       slots (logical-slot-copy-committed-proof-state (logical-slot-copy-install-input-proof input)) :committed t))
    layout))

(defun logical-slot-copy-install-check-input-observations (owner context input)
  "Recheck the retained receive tuple and all stable input and child observations."
  (let* ((proof (logical-slot-copy-install-input-proof input))
         (store (logical-slot-copy-install-input-store input))
         (tuple (logical-slot-copy-committed-proof-tuple proof))
         (observations (logical-slot-copy-committed-proof-observations proof)))
    (physical-store-check-live store)
    (unless (equal tuple (physical-store-control-tuple store))
      (error "logical installation input receive tuple changed"))
    (physical-store-read-retained-control store tuple)
    (logical-slot-copy-input-check-observations owner context observations)
    (dolist (row observations)
      (unless (equal row (logical-slot-copy-file-row context (first row)))
        (error "logical installation stable input or child bytes changed")))))

(defun logical-slot-copy-install-recheck-input-source (context input)
  "Reprove the actual retained source and child under the reader lease."
  (let ((proof (logical-slot-copy-install-input-proof input)))
    (multiple-value-bind (fact history tuple)
        (logical-slot-copy-check-committed-source
         context (logical-slot-copy-install-input-certificate input)
         (logical-slot-copy-committed-proof-evidence proof)
         (logical-slot-copy-committed-proof-descriptor proof)
         (logical-slot-copy-committed-proof-manifest proof)
         (logical-slot-copy-committed-proof-control proof)
         (logical-slot-copy-install-input-plan input)
         (logical-slot-copy-committed-proof-state proof)
         (logical-slot-copy-committed-proof-metadata proof)
         (logical-slot-copy-committed-proof-prefix proof)
         (logical-slot-copy-committed-proof-generation proof)
         (logical-slot-copy-install-input-store input))
      (unless (and (logical-slot-copy-committed-image-equal-p fact (logical-slot-copy-install-input-fact input))
                   (logical-slot-copy-committed-image-equal-p history (logical-slot-copy-install-input-history input))
                   (equal tuple (logical-slot-copy-committed-proof-tuple proof)))
        (error "logical installation source proof changed")))))

(defun logical-slot-copy-install-check-input (owner context input &key (source t))
  "Recheck the seal, observations, layout, and by default the full source proof.
A failed check revokes the scope.  The initial call may omit the source recheck
because the enclosing source proof has just completed it under the lease."
  (let ((record (logical-slot-copy-install-input-record owner context input)) (completed nil))
    (unwind-protect
         (progn
           (logical-slot-copy-install-check-input-seal owner context input record)
           (logical-slot-copy-install-check-input-observations owner context input)
           (let ((before (logical-slot-copy-install-check-input-layout owner input record)))
             (when source (logical-slot-copy-install-recheck-input-source context input))
             (logical-slot-copy-install-check-input-seal owner context input record)
             (logical-slot-copy-install-check-input-observations owner context input)
             (let ((after (logical-slot-copy-install-check-input-layout owner input record)))
               (unless (and (eq (getf before :phase) (getf after :phase))
                            (= (getf before :prefix) (getf after :prefix)))
                 (error "logical installation layout changed during its input proof"))
               (setf (logical-slot-copy-install-input-state-phase record) (getf after :phase)
                     (logical-slot-copy-install-input-state-prefix record) (getf after :prefix))))
           (setf completed t)
           t)
      (unless completed (logical-slot-copy-install-revoke-input record)))))

(defun logical-slot-copy-install-bind-input-layout (record)
  "Bind a fresh private handoff or a cold fixed journal and capture the layout."
  (let* ((operation (logical-slot-copy-install-input-state-operation record))
         (outer (logical-slot-copy-install-operation-record operation))
         (image (logical-slot-copy-install-input-state-image record))
         (layout (logical-slot-copy-install-read-layout operation image))
         (value (logical-slot-copy-install-decode image))
         (mode (logical-slot-copy-install-operation-state-mode outer))
         (physical (getf layout :physical))
         (slots (logical-slot-copy-install-physical-registry
                 mode physical (mapcar #'first (getf value :entries)))))
    (ecase (logical-slot-copy-install-input-state-origin record)
      (:fresh
       (unless (and (eq :private (getf layout :phase))
                    (equalp image (logical-slot-copy-install-handoff-image
                                   operation (logical-slot-copy-install-input-state-handoff record))))
         (error "fresh installation input requires its private handoff")))
      (:cold
       (unless (and (eq mode :cold) (member (getf layout :phase) '(:pending :active)))
         (error "cold installation input requires its fixed journal"))))
    (setf (logical-slot-copy-install-input-state-phase record) (getf layout :phase)
          (logical-slot-copy-install-input-state-prefix record) (getf layout :prefix)
          (logical-slot-copy-install-input-state-physical-images record) (mapcar #'physical-slot-encode physical)
          (logical-slot-copy-install-input-state-physical-facts record)
          (logical-slot-copy-import-state-image (logical-slot-copy-install-input-physical-facts layout))
          (logical-slot-copy-install-input-state-physical-witnesses record)
          (when (eq mode :live) (logical-slot-copy-install-physical-witnesses slots)))
    (logical-slot-copy-import-decode (getf value :certificate))))

(defun logical-slot-copy-install-check-stable-inputs (owner context observations)
  "Recheck all saved stable input and child observations without replaying source WAL."
  (logical-slot-copy-input-check-observations owner context observations)
  (dolist (row observations)
    (unless (equal row (logical-slot-copy-file-row context (first row)))
      (error "logical installation stable input changed after source proof"))))

(defun call-with-logical-slot-copy-install-input-state (record function)
  "Prove source and layout, then call FUNCTION with a scoped input capsule.
The proof covers actual restartpoint and WAL from D through F under the retained
root lease.  After the cold store closes, recheck all stable and child facts,
the seal, exact phase and prefix, and physical parity without replaying source WAL.
The scope must end before registry or global engine activation."
  (when *logical-slot-copy-install-inputs* (error "logical installation input is already active"))
  (let* ((operation (logical-slot-copy-install-input-state-operation record))
         (outer (logical-slot-copy-install-operation-record operation))
         (*logical-slot-copy-install-inputs* (list record)) (completed nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-logical-slot-copy-owned-input
              (lambda (&rest arguments)
                (let ((owner (apply #'%make-logical-slot-copy-install-input-owner
                                    :operation operation
                                    :mode (logical-slot-copy-install-operation-state-mode outer)
                                    :state (logical-slot-copy-install-operation-state-state outer)
                                    :store (logical-slot-copy-install-operation-state-store outer) arguments)))
                  (setf (logical-slot-copy-install-input-state-owner record) owner)
                  owner))
              (lambda (owner context receipt)
                (logical-slot-copy-install-check-operation-input operation context)
                (setf (logical-slot-copy-install-input-state-context record) context
                      (logical-slot-copy-install-input-state-context-fd record) (physical-image-context-root-fd context)
                      (logical-slot-copy-install-input-state-context-children record)
                      (mapcar #'copy-list (physical-image-context-children context)))
                (let ((certificate (logical-slot-copy-install-bind-input-layout record)))
                  (multiple-value-bind (bound evidence descriptor manifest control plan status observations)
                      (logical-slot-copy-read-committed-inputs owner context receipt certificate)
                    (declare (ignore status))
                    (unless (= (getf (logical-slot-copy-install-decode
                                      (logical-slot-copy-install-input-state-image record)) :evidence-digest)
                               (logical-slot-copy-import-digest (logical-slot-copy-import-evidence-encode evidence)))
                      (error "logical installation journal evidence digest differs"))
                    (multiple-value-prog1
                        (call-with-logical-slot-copy-committed-proof
                         owner context bound evidence descriptor manifest control plan observations
                         (lambda (same-owner same-context store pending child-plan fact history proof)
                           (unless (and (eq owner same-owner) (eq context same-context))
                             (error "logical installation source scope changed its owner"))
                           (let ((input (%make-logical-slot-copy-install-input
                                         :owner owner :context context :store store
                                         :image (copy-seq (logical-slot-copy-install-input-state-image record))
                                         :certificate (logical-slot-copy-import-decode (logical-slot-copy-import-image pending))
                                         :plan (logical-slot-copy-import-state-image child-plan)
                                         :fact (logical-slot-copy-import-state-image fact)
                                         :history (logical-slot-copy-import-state-image history) :proof proof)))
                             (setf (logical-slot-copy-install-input-state-input record) input
                                   (logical-slot-copy-install-input-state-seal record)
                                   (logical-slot-copy-import-state-image (logical-slot-copy-install-input-view input)))
                             (logical-slot-copy-install-check-input owner context input :source nil)
                             (setf (logical-slot-copy-install-input-state-ready record) t)
                             (multiple-value-prog1 (funcall function owner context input)
                               (logical-slot-copy-install-check-input owner context input)))))
                      (let ((input (logical-slot-copy-install-input-state-input record)))
                        (logical-slot-copy-install-check-stable-inputs
                         owner context (logical-slot-copy-committed-proof-observations
                                        (logical-slot-copy-install-input-proof input)))
                        (logical-slot-copy-install-check-input-seal owner context input record)
                        ;; The cold store and its reader lease have ended.
                        (let ((layout (logical-slot-copy-install-check-input-layout
                                       owner input record :physical-proof nil)))
                          (unless (and (eq (getf layout :phase)
                                           (logical-slot-copy-install-input-state-phase record))
                                       (= (getf layout :prefix)
                                          (logical-slot-copy-install-input-state-prefix record)))
                            (error "logical installation layout changed after its source scope")))))))))
           (setf completed t))
      (unless completed (logical-slot-copy-install-revoke-input record))
      (when (logical-slot-copy-install-input-state-owner record)
        (setf (physical-adoption-owner-active-p (logical-slot-copy-install-input-state-owner record)) nil)))))
