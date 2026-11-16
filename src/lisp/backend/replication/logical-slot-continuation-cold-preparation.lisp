(in-package #:pg)

;; Prepare an imported slot registry without publishing it. A receipt survives
;; the proof's closed descriptors, but later effects need a new protected check.
;; Only the owned entry point registers a receipt; copying it grants no authority.

(defstruct (logical-slot-continuation-cold-preparation
            (:constructor %make-logical-slot-continuation-cold-preparation)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

(defvar *logical-slot-continuation-cold-preparations*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-preparation-scopes* nil)

(defun logical-slot-continuation-cold-preparation-call (preparation operation)
  (unless (and (logical-slot-continuation-cold-preparation-p preparation)
               (member preparation *logical-slot-continuation-cold-preparation-scopes* :test #'eq))
    (error "cold preparation requires its scoped registered receipt"))
  (sb-sys:without-interrupts
    (let ((state (logical-slot-continuation-cold-preparation-state preparation)))
      (unless (and (functionp state)
                   (eq state (gethash preparation *logical-slot-continuation-cold-preparations*)))
        (when (functionp state) (funcall state :lost preparation))
        (error "cold preparation lost its registered receipt"))
      (sb-sys:with-local-interrupts (funcall state operation preparation)))))

;; Retain the original candidate identities and freeze their registry image,
;; including reconstructed pending transactions, for later comparison.

;; Build the complete candidate registry.  Restore physical slots with a
;; startup inactivity time; child logical slots are reconstructed separately.
(defun logical-slot-continuation-cold-prepare-registry (source)
  (logical-slot-startup-check-inventory source)
  (let ((candidates (mapcar #'logical-slot-startup-entry-slot
                            (logical-slot-startup-source-entries source)))
        (registry (make-hash-table :test #'equal))
        (inactive-since (postgres-current-timestamp)))
    (unless (every (lambda (slot)
                     (and (not (wal-replication-slot-temporary-p slot))
                          (or (wal-physical-slot-p slot) (wal-logical-slot-p slot))))
                   candidates)
      (error "cold continuation preparation has an unsupported candidate"))
    (logical-slot-startup-prepare-known-candidates candidates #'identity)
    (dolist (slot candidates)
      (when (some #'identity (subseq (logical-slot-publication-slot-transients slot) 0 6))
        (error "cold continuation candidate is active or uncertain"))
      (when (wal-physical-slot-p slot)
        (physical-slot-restore slot inactive-since))
      (wal-check-replication-slot-capacity-for-count
       (hash-table-count registry) (logical-slot-startup-source-maximum source))
      (setf (gethash (copy-seq (wal-replication-slot-name slot)) registry) slot))
    (logical-slot-startup-check-inventory source)
    (values registry (logical-slot-publication-registry-image registry))))

(defun logical-slot-continuation-cold-preparation-fail (failure mutex)
  (when failure
    (if (sb-thread:holding-mutex-p mutex)
        (funcall failure)
        (sb-thread:with-mutex (mutex) (funcall failure)))))

;; Complete reconstruction and close its source scopes and descriptors before
;; returning the saved data.  The failure cell carries cleanup across that close
;; and receipt creation, so an uncertain failure is retained.

(defun logical-slot-continuation-cold-prepare-data (gate ownership &optional failure-cell)
  (let ((data nil) (failure nil) (complete nil) (mutex *wal-lock*))
    (sb-sys:without-interrupts
      (unwind-protect
           (multiple-value-prog1
               (sb-sys:with-local-interrupts
                 (setf data
                       (call-with-logical-slot-continuation-cold-reconstruction
                        gate ownership
                        (lambda (source observation reservation check proof)
                          (sb-thread:with-mutex (*wal-lock*)
                            (setf failure
                                  (logical-slot-continuation-cold-reservation-call
                                   reservation :failure-cleanup))
                            (when failure-cell
                              (sb-sys:without-interrupts
                                (setf (car failure-cell) failure (cdr failure-cell) *wal-lock*))))
                          (multiple-value-bind (registry image)
                              (logical-slot-continuation-cold-prepare-registry source)
                            (funcall check)
                            (sb-thread:with-mutex (*wal-lock*)
                              (logical-slot-startup-check-source source)
                              (list :registry registry :image image
                                    :proof (logical-slot-continuation-copy proof)
                                    :observation (logical-slot-continuation-copy observation)
                                    :reservation reservation :failure failure
                                    :host (logical-slot-imported-host-startup-image)
                                    :mirrors (logical-slot-publication-mirrors)
                                    :incarnation (logical-slot-startup-source-incarnation source)
                                    :maximum (logical-slot-startup-source-maximum source)
                                    :wal-level (logical-slot-startup-source-wal-level source)
                                    :namespace (logical-slot-startup-source-namespace source)
                                    :binding (logical-slot-continuation-cold-reservation-call reservation :binding)
                                    :origin (logical-slot-continuation-cold-reservation-call reservation :origin)
                                    :mutex *wal-lock* :thread sb-thread:*current-thread*))))
                        :policy t :receipt t))
                 (logical-slot-publication-check-registry (getf data :registry) (getf data :image))
                 data)
             (setf complete t))
        (unless complete (logical-slot-continuation-cold-preparation-fail failure mutex))))))

(defun logical-slot-continuation-cold-preparation-check-data (data gate ownership)
  (check-primary-storage-startup-context gate ownership)
  (sb-thread:with-mutex (*wal-lock*)
    (unless (and (eq (getf data :mutex) *wal-lock*)
                 (eq (getf data :thread) sb-thread:*current-thread*)
                 (eq (getf data :incarnation) *wal-storage-incarnation*)
                 (eq (getf data :namespace) *wal-segment-namespace*)
                 (eq (getf data :wal-level) *wal-level*)
                 (eql (getf data :maximum) *max-replication-slots*)
                 (equal (getf data :mirrors) (logical-slot-publication-mirrors))
                 (equalp (getf data :host) (logical-slot-imported-host-startup-image)))
      (error "cold preparation changed its startup host or configuration"))
    (let ((reservation (getf data :reservation)))
      (logical-slot-continuation-cold-reservation-call reservation :check)
      (unless (and (equalp (getf data :binding)
                           (logical-slot-continuation-cold-reservation-call reservation :binding))
                   (eq (getf data :origin)
                       (logical-slot-continuation-cold-reservation-call reservation :origin)))
        (error "cold preparation changed its original permanent source")))
    (logical-slot-publication-check-registry (getf data :registry) (getf data :image)))
  t)

;; A fresh preparation derives a new physical inactivity time.  Ignore that
;; transient when comparing the two preparations, while checking the other state.
(defun logical-slot-continuation-cold-preparation-semantic-image (image)
  (mapcar (lambda (row)
            (let ((transients (copy-list (eighth row))))
              (when (wal-physical-slot-p (second row))
                (setf (seventh transients) 0))
              (list (first row) (third row) (fourth row) (seventh row) transients)))
          image))

;; A claim makes a fresh full proof in closed scopes and compares the host,
;; configuration, observation, source, semantic registry image and F..L witness.

(defun logical-slot-continuation-cold-preparation-reprove (data gate ownership)
  (logical-slot-continuation-cold-preparation-check-data data gate ownership)
  (let ((current (logical-slot-continuation-cold-prepare-data gate ownership)))
    (unless (and (eq (getf data :reservation) (getf current :reservation))
                 (equalp (getf data :observation) (getf current :observation))
                 (equalp (getf data :host) (getf current :host))
                 (equalp (getf data :proof) (getf current :proof))
                 (equalp (getf data :binding) (getf current :binding))
                 (eq (getf data :origin) (getf current :origin))
                 (every (lambda (key) (eql (getf data key) (getf current key)))
                        '(:incarnation :maximum :wal-level :namespace :mutex :thread))
                 (equal (getf data :mirrors) (getf current :mirrors))
                 (equalp (logical-slot-continuation-cold-preparation-semantic-image (getf data :image))
                         (logical-slot-continuation-cold-preparation-semantic-image (getf current :image))))
      (error "cold preparation differs from its fresh closed-scope proof")))
  (logical-slot-continuation-cold-preparation-check-data data gate ownership))

;; Register a receipt only after the source scopes have closed.  Identity and
;; thread rejection precede the masked owner failure latch and cleanup.  A claim
;; is not effect or publication authority: a later effect needs fresh protected
;; descriptors and its own commit guard.

(defun call-with-logical-slot-continuation-cold-preparation (gate ownership function)
  (unless (functionp function) (error "cold preparation requires a callback"))
  (let ((data nil) (mutex *wal-lock*) (thread sb-thread:*current-thread*)
                   (failure (cons nil nil))
                   (preparation nil) (active t) (failed nil) (busy nil) (claimed nil) (complete nil))
    (labels ((state (operation candidate)
               (unless (and active (eq preparation candidate)
                            (eq mutex *wal-lock*) (eq thread sb-thread:*current-thread*))
                 (error "cold preparation ended or changed identity, thread or WAL mutex"))
               (sb-sys:without-interrupts
                 (when (eq operation :lost)
                   (setf failed t)
                   (error "cold preparation lost its registered receipt"))
                 (when (and (eq operation :claim) claimed)
                   (setf failed t)
                   (error "cold preparation was already claimed"))
                 (unless (and (not failed) (not busy)
                              (eq #'state (gethash preparation *logical-slot-continuation-cold-preparations*)))
                   (setf failed t)
                   (error "cold preparation failed, is busy or lost its registration"))
                 (let ((done nil))
                   (setf busy t)
                   (unwind-protect
                        (multiple-value-prog1
                            (sb-sys:with-local-interrupts
                              (logical-slot-continuation-cold-preparation-check-data data gate ownership)
                              (multiple-value-prog1
                                  (ecase operation
                                    (:check t)
                                    (:registry (getf data :registry))
                                    (:observation (logical-slot-continuation-copy (getf data :observation)))
                                    (:proof (logical-slot-continuation-copy (getf data :proof)))
                                    (:reservation (getf data :reservation))
                                    (:claim
                                     (logical-slot-continuation-cold-preparation-reprove data gate ownership)
                                     (when failed (error "cold preparation failed during reproof"))
                                     (setf claimed t)))
                                (unless (and (not failed)
                                             (eq #'state (gethash preparation *logical-slot-continuation-cold-preparations*)))
                                  (error "cold preparation failed or lost registration during its check"))))
                          (setf done t))
                     (setf busy nil)
                     (unless done (setf failed t)))))))
      (sb-sys:without-interrupts
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (setf data (logical-slot-continuation-cold-prepare-data gate ownership failure)
                         mutex (getf data :mutex) thread (getf data :thread))
                   (logical-slot-continuation-cold-preparation-check-data data gate ownership)
                   (setf preparation (%make-logical-slot-continuation-cold-preparation :state #'state))
                   (let ((*logical-slot-continuation-cold-preparation-scopes*
                           (cons preparation *logical-slot-continuation-cold-preparation-scopes*)))
                     (sb-sys:without-interrupts
                       (setf (gethash preparation *logical-slot-continuation-cold-preparations*) #'state))
                     (multiple-value-prog1 (funcall function preparation)
                       (state :check preparation))))
               (setf complete t))
          (setf active nil)
          (when preparation (remhash preparation *logical-slot-continuation-cold-preparations*))
          (unless complete (logical-slot-continuation-cold-preparation-fail (car failure) (cdr failure))))))))
