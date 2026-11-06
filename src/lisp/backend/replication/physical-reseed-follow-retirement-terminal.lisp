;;; Publish the terminal retirement record under the held startup owner.
(in-package #:pg)

(defstruct (physical-reseed-follow-retirement-terminal-witness (:conc-name retirement-terminal-))
  ;; Bind one private terminal publication to its intent, thread and proof.
  intent thread active-p binding proof phase final-snapshot)

(defmethod print-object ((witness physical-reseed-follow-retirement-terminal-witness) stream)
  (physical-libpq-print-private-object witness stream 0))

(defun physical-reseed-follow-retirement-terminal-check-outer (witness &optional phases)
  ;; Require the live thread-bound witness and the exact Retiring step 2 to
  ;; Retired step 2 transition at frozen prefix 2 with no alias.
  (unless (and (physical-reseed-follow-retirement-terminal-witness-p witness)
               (retirement-terminal-active-p witness)
               (eq (retirement-terminal-thread witness) sb-thread:*current-thread*))
    (error "retirement terminal witness ended or changed thread"))
  (let* ((intent (retirement-terminal-intent witness))
         (before (getf (getf (retirement-intent-snapshot intent) :record) :retirement))
         (after (getf (retirement-intent-candidate intent) :retirement)))
    (unless (and (equal (retirement-terminal-binding witness)
                        (physical-reseed-follow-retirement-intent-binding intent))
                 (eq :retiring (getf before :phase)) (eql 2 (getf before :step))
                 (eq :retired (getf after :phase)) (eql 2 (getf after :step))
                 (eql 2 (getf (retirement-terminal-proof witness) :prefix))
                 (null (getf (retirement-terminal-proof witness) :location)))
      (error "retirement terminal changed its binding or terminal boundary")))
  (when (and phases (not (member (retirement-terminal-phase witness) phases)))
    (error "retirement terminal requires phase ~s" phases))
  t)

(defun physical-reseed-follow-retirement-terminal-check (witness)
  ;; Check the frozen proof during publication and the full durable proof
  ;; after adoption.
  (physical-reseed-follow-retirement-terminal-check-outer witness '(:ready :checking :publishing :done))
  (let ((intent (retirement-terminal-intent witness)))
    (unless (equal (retirement-terminal-proof witness) (retirement-intent-proof intent))
      (error "retirement terminal changed its frozen proof"))
    (if (eq :done (retirement-terminal-phase witness))
        (progn
          (unless (equal (retirement-terminal-final-snapshot witness) (retirement-intent-final-snapshot intent))
            (error "retirement terminal changed its returned snapshot"))
          (physical-reseed-follow-retirement-intent-check-durable-proof
           intent (lambda () (physical-reseed-follow-retirement-terminal-check-outer witness '(:done)))))
        (physical-reseed-follow-retirement-intent-check intent)))
  (physical-reseed-follow-retirement-terminal-check-outer witness '(:ready :checking :publishing :done))
  t)

(defmethod physical-reseed-follow-retirement-authorize-progress
    ((authority physical-reseed-follow-retirement-terminal-witness) witness)
  ;; Consume this typed authority once before further checks and callbacks.
  ;; Latch the effect and owner uncertainty before the first filesystem effect.
  (physical-reseed-follow-retirement-terminal-check-outer authority '(:ready))
  (unless (and (eq witness (retirement-terminal-intent authority))
               (retirement-intent-publication-claimed-p witness))
    (error "retirement terminal authority requires its claimed publisher"))
  (sb-sys:without-interrupts
    (unless (eq :ready (retirement-terminal-phase authority))
      (error "retirement terminal authority already claimed"))
    (setf (retirement-terminal-phase authority) :checking))
  (let ((record (getf (retirement-intent-snapshot witness) :record)))
    (unless (equal (retirement-intent-candidate witness)
                   (physical-reseed-follow-retirement-next-record record (getf (getf record :retirement) :attempt-file)))
      (error "retirement terminal changed its exact next candidate")))
  (physical-reseed-follow-retirement-terminal-check authority)
  (let ((owner (retirement-intent-owner witness)))
    (sb-sys:without-interrupts
      (unless (and (eq :checking (retirement-terminal-phase authority))
                   (not (retirement-intent-effect-p witness))
                   (not (retirement-startup-uncertain-p owner)))
        (error "retirement terminal requires a fresh operation"))
      (setf (retirement-terminal-phase authority) :publishing
            (retirement-intent-effect-p witness) t
            (retirement-startup-uncertain-p owner) t)))
  (physical-reseed-follow-retirement-terminal-check authority)
  t)

(defun physical-reseed-follow-retirement-terminal-run (witness)
  ;; Publish the exact candidate and retain the returned snapshot for the
  ;; terminal checks after adoption.
  (physical-reseed-follow-retirement-terminal-check-outer witness '(:ready))
  (let ((intent (retirement-terminal-intent witness)))
    (multiple-value-bind (record snapshot)
        (physical-reseed-follow-retirement-intent-publish-held intent witness)
      (unless (and (equal record (retirement-intent-candidate intent))
                   (equal snapshot (retirement-intent-final-snapshot intent)))
        (error "retirement terminal returned another publication"))
      (let ((copy (physical-adoption-attempt-value-copy snapshot)))
        (sb-sys:without-interrupts
          (setf (retirement-terminal-final-snapshot witness) copy
                (retirement-terminal-phase witness) :done)))
      (physical-reseed-follow-retirement-terminal-check witness)
      (values record snapshot))))

(defun call-with-physical-reseed-follow-retirement-terminal-context (owner expected function)
  ;; Prove prefix 2 and bind a private witness to the intent.  A normal return
  ;; requires done and a final check.  Revoke the witness on every exit.
  ;; The base context rechecks the adopted state after temporary cleanup.
  (call-with-physical-reseed-follow-retirement-intent-base-context
   owner expected
   (lambda (snapshot)
     (let* ((record (getf snapshot :record))
            (retirement (getf record :retirement)))
       (unless (and (eq :retiring (getf retirement :phase)) (eql 2 (getf retirement :step)))
         (error "retirement terminal context requires Retiring2"))
       (physical-reseed-follow-retirement-next-record record (getf retirement :attempt-file))))
   (lambda (intent)
     (let ((proof (physical-reseed-follow-retirement-prefix-proof-at
                   (retirement-intent-descriptors intent) (retirement-intent-store intent)
                   (getf (retirement-intent-snapshot intent) :record) 2
                   (lambda () (physical-reseed-follow-retirement-intent-check-scope intent))
                   (lambda (root) (physical-reseed-follow-retirement-intent-extra intent root)))))
       (dolist (field '(:prefix :location :layout :install :attempt))
         (unless (equal (getf expected field) (getf proof field))
           (error "retirement terminal changed after its startup observation")))
       (setf (retirement-intent-proof intent) (physical-adoption-attempt-value-copy proof))
       (physical-reseed-follow-retirement-intent-check intent)
       (let ((witness (make-physical-reseed-follow-retirement-terminal-witness
                       :intent intent :thread sb-thread:*current-thread* :active-p t
                       :binding (physical-adoption-attempt-value-copy
                                 (physical-reseed-follow-retirement-intent-binding intent))
                       :proof (physical-adoption-attempt-value-copy proof) :phase :ready)))
         (unwind-protect
              (multiple-value-prog1 (funcall function witness)
                (physical-reseed-follow-retirement-terminal-check-outer witness '(:done))
                (physical-reseed-follow-retirement-terminal-check witness))
           (setf (retirement-terminal-active-p witness) nil)))))))

(defun physical-reseed-follow-retirement-publish-terminal (owner)
  ;; Publish from Retiring step 2 or return a stable Retired step 2 retry.
  ;; The shared publisher can recover an exact complete temporary.  Clear
  ;; uncertainty after the context and its cleanup return normally.
  ;; Stable retries return copies without filesystem effects.
  (call-with-physical-reseed-follow-retirement-operation
   owner
   (lambda ()
     (let* ((expected
              (physical-reseed-follow-retirement-intent-startup-expected owner))
            (record (getf (getf expected :snapshot) :record))
            (retirement (getf record :retirement)))
       (unless (and (member (getf retirement :phase) '(:retiring :retired))
                    (eql 2 (getf retirement :step))
                    (eql 2 (getf expected :prefix)) (null (getf expected :location)))
         (error "terminal publication requires Retiring2 or completed Retired2"))
       (when (and *physical-reseed-follow-write-limit*
                  (not (and (integerp *physical-reseed-follow-write-limit*)
                            (plusp *physical-reseed-follow-write-limit*))))
         (error "invalid retirement intent write limit"))
       (if (eq :retired (getf retirement :phase))
           (progn
             (when (getf expected :temporary-file)
               (error "terminal publication retry requires stable Retired2"))
             (values (physical-reseed-follow-copy record)
                     (physical-adoption-attempt-value-copy (getf expected :snapshot))))
           (multiple-value-prog1
               (call-with-physical-reseed-follow-retirement-terminal-context
                owner expected #'physical-reseed-follow-retirement-terminal-run)
             (setf (retirement-startup-uncertain-p owner) nil)))))))
