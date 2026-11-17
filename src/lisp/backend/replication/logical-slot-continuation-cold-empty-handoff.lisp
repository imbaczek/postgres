(in-package #:pg)

(declaim (ftype function logical-slot-continuation-cold-authority-maps
                logical-slot-continuation-cold-map-image
                logical-slot-continuation-cold-map-matches-p
                logical-slot-continuation-cold-copy-map))

(defstruct (logical-slot-continuation-cold-empty-handoff
            (:constructor %make-logical-slot-continuation-cold-empty-handoff)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t)
  (thread sb-thread:*current-thread* :read-only t))

(defvar *logical-slot-continuation-cold-empty-handoffs*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-empty-denied*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-empty-test-hook* nil)

(defun logical-slot-continuation-cold-empty-check-registration (data)
  (unless (and (eq (getf data :handoff-map) *logical-slot-continuation-cold-empty-handoffs*)
               (eq (getf data :denial-map) *logical-slot-continuation-cold-empty-denied*)
               (eq (getf data :dispatcher)
                   (gethash (getf data :token) *logical-slot-continuation-cold-empty-handoffs*))
               (not (gethash (getf data :token) *logical-slot-continuation-cold-empty-denied*)))
    (error "empty startup handoff lost its registration"))
  t)

(defun logical-slot-continuation-cold-empty-point (point data)
  (when *logical-slot-continuation-cold-empty-test-hook*
    (funcall *logical-slot-continuation-cold-empty-test-hook*
             point (getf data :token) (getf data :reservation))))

(defun logical-slot-continuation-cold-empty-check-maps (data)
  (when (getf data :maps)
    (let ((current (logical-slot-continuation-cold-authority-maps)))
      (unless (and (loop for map across current for saved across (getf data :maps)
                         for image across (getf data :images)
                         always (and (eq map saved)
                                     (logical-slot-continuation-cold-map-matches-p map image)))
                   (eq *wal-snapshot-retention-pins* (getf data :pin-table))
                   (logical-slot-continuation-cold-map-matches-p
                    *wal-snapshot-retention-pins* (getf data :pins)))
        (error "empty startup handoff authority maps or pins changed"))))
  t)

(defun logical-slot-continuation-cold-empty-check (data)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (logical-slot-continuation-cold-empty-check-registration data)
  (logical-slot-continuation-cold-reservation-call (getf data :reservation) :check)
  (logical-slot-continuation-cold-empty-check-maps data)
  (when (getf data :new)
    (logical-slot-publication-check-registry (getf data :new) (getf data :new-image)))
  (unless (getf data :closed)
    (dolist (owner (getf data :owners))
      (logical-slot-imported-host-startup-under-lock owner)))
  t)

(defun logical-slot-continuation-cold-empty-bind (reservation token control registry)
  (logical-slot-continuation-cold-empty-call token :bind (list reservation control registry)))

(defun logical-slot-continuation-cold-empty-bind-data (data reservation control registry)
  (unless (and (eq reservation (getf data :reservation)) (eq registry (getf data :old))
               (eq :scoped (getf data :phase)) (null (getf data :control))
               (eq :reserved (svref control 0)) (null (svref control 1)))
    (error "empty startup handoff cannot bind this reservation"))
  (setf (getf data :control) control (svref control 1) (getf data :token))
  control)

(defun logical-slot-continuation-cold-empty-commit (data)
  (assert (and (sb-thread:holding-mutex-p *wal-lock*)
               (sb-thread:holding-mutex-p *xid-lock*)
               (not sb-sys:*interrupts-enabled*)))
  (unless (and (getf data :active) (not (getf data :failed))
               (eq :installing (getf data :phase))
               (eq sb-thread:*current-thread* (getf data :thread)))
    (error "empty startup handoff has no prepared atomic installer"))
  (dolist (install (getf data :owner-commits)) (funcall install))
  (let ((control (getf data :control)))
    (setf *logical-slot-continuation-cold-registries* (svref (getf data :next-maps) 4)
          (svref control 2) (getf data :new)
          (svref control 4) (getf data :new-image)
          (svref control 0) :empty-published
          (getf data :maps) (getf data :next-maps)
          (getf data :images) (getf data :next-images)
          (getf data :phase) :published))
  t)

(defun logical-slot-continuation-cold-zero-child-plan-p (plan)
  (and (eq :stable (getf plan :kind)) (getf plan :selector)
       (null (getf (getf plan :snapshot) :slots))
       (null (getf plan :children)) (null (getf plan :actions))
       (null (getf plan :temporary))))

(defun logical-slot-continuation-cold-empty-check-source (data source)
  (logical-slot-continuation-cold-empty-check data)
  (logical-slot-startup-check-source source)
  (unless (and (eq :scoped (getf data :phase))
               (or (null (getf data :source)) (eq source (getf data :source)))
               (not (logical-slot-imported-startup-source-p source))
               (eq (logical-slot-startup-source-gate-owner source) (getf data :gate))
               (eq (logical-slot-startup-source-ownership source) (getf data :ownership))
               (eq (logical-slot-startup-source-registry source) (getf data :old))
               (eq *wal-replication-slots* (getf data :old))
               (zerop (hash-table-count (getf data :old)))
               (equalp
                (sort (mapcar (lambda (entry)
                                (let ((slot (logical-slot-startup-entry-slot entry)))
                                  (when (or (wal-replication-slot-child-owner slot)
                                            (and (wal-physical-slot-p slot)
                                                 (wal-replication-slot-temporary-p slot)))
                                    (error "zero-child source contains a child or temporary slot"))
                                  (list (wal-replication-slot-name slot)
                                        (logical-slot-startup-entry-bytes entry))))
                              (logical-slot-startup-source-entries source))
                      #'string< :key #'first)
                (getf data :rows)))
    (error "zero-child startup requires its exact checked ordinary source"))
  (setf (getf data :source) source)
  t)

(defun logical-slot-continuation-cold-empty-prepare (data source restored)
  (logical-slot-continuation-cold-empty-check-source data source)
  (unless (and (hash-table-p restored)
               (loop for name being the hash-keys of restored using (hash-value slot)
                     always (and (not (wal-replication-slot-child-owner slot))
                                 (some (lambda (entry)
                                         (and (equal name (wal-replication-slot-name
                                                           (logical-slot-startup-entry-slot entry)))
                                              (eq slot (logical-slot-startup-entry-slot entry))))
                                       (logical-slot-startup-source-entries source))))
               (not (eq restored (getf data :old))))
    (error "zero-child startup replacement differs from its checked source"))
  (logical-slot-continuation-cold-reservation-call
   (getf data :reservation) :prepare-empty (getf data :token))
  (setf (getf data :new-image) (logical-slot-publication-registry-image restored)
        (getf data :new) restored
        (getf data :owners) (copy-list *logical-slot-imported-host-startup-owners*)
        (getf data :owner-commits)
        (mapcar (lambda (owner)
                  (funcall (logical-slot-imported-host-startup-owner-state owner)
                           :prepare-empty-registry owner (getf data :token)))
                (getf data :owners)))
  (let* ((maps (logical-slot-continuation-cold-authority-maps))
         (next (copy-seq maps))
         (registry-map (logical-slot-continuation-cold-copy-map (svref maps 4))))
    (remhash (getf data :old) registry-map)
    (setf (gethash restored registry-map) (getf data :reservation)
          (svref next 4) registry-map
          (getf data :new) restored
          (getf data :maps) maps
          (getf data :images) (map 'vector #'logical-slot-continuation-cold-map-image maps)
          (getf data :next-maps) next
          (getf data :next-images) (map 'vector #'logical-slot-continuation-cold-map-image next)
          (getf data :pin-table) *wal-snapshot-retention-pins*
          (getf data :pins) (logical-slot-continuation-cold-map-image *wal-snapshot-retention-pins*)
          (getf data :phase) :prepared))
  (logical-slot-continuation-cold-empty-point :prepared data)
  (lambda () (logical-slot-continuation-cold-empty-commit data)))

(defun logical-slot-continuation-cold-empty-call (token operation &optional value)
  (sb-sys:without-interrupts
    (let* ((typed (logical-slot-continuation-cold-empty-handoff-p token))
           (state (and typed (gethash token *logical-slot-continuation-cold-empty-handoffs*)))
           (registered (and (functionp state)
                            (eq state (logical-slot-continuation-cold-empty-handoff-state token)))))
      (when (and typed (not registered)
                 (eq sb-thread:*current-thread* (logical-slot-continuation-cold-empty-handoff-thread token)))
        (setf (gethash token *logical-slot-continuation-cold-empty-denied*) t))
      (unless (and registered (not (gethash token *logical-slot-continuation-cold-empty-denied*)))
        (error "empty startup handoff is not registered or observed registration loss"))
      (sb-sys:with-local-interrupts (funcall state operation token value)))))

(declaim (ftype function logical-slot-continuation-cold-empty-retire-data))

(defun logical-slot-continuation-cold-empty-dispatch (data operation value)
  (with-logical-slot-continuation-observation-check
      ((getf data :active) (getf data :failed) (getf data :thread) "empty startup handoff")
    (assert (sb-thread:holding-mutex-p *wal-lock*))
    (ecase operation
      (:check (logical-slot-continuation-cold-empty-check data))
      (:authorize-source (logical-slot-continuation-cold-empty-check-source data value))
      (:owner-registry
       (logical-slot-continuation-list value 4)
       (unless (and (eq :scoped (getf data :phase)) (getf data :new)
                    (typep (first value) 'logical-slot-imported-host-startup-owner)
                    (member (first value) *logical-slot-imported-host-startup-owners* :test #'eq)
                    (eq (second value) (getf data :gate))
                    (eq (third value) (getf data :ownership))
                    (eq (fourth value) (getf data :old))
                    (logical-slot-publication-check-registry (getf data :new) (getf data :new-image)))
         (error "empty handoff cannot replace this imported observer registry"))
       (getf data :new))
      (:bind
       (logical-slot-continuation-list value 3)
       (logical-slot-continuation-cold-empty-bind-data data (first value) (second value) (third value)))
      (:prepare
       (logical-slot-continuation-list value 2)
       (logical-slot-continuation-cold-empty-prepare data (first value) (second value)))
      (:retire
       (logical-slot-continuation-list value 2)
       (logical-slot-continuation-cold-empty-retire-data data (first value) (second value)))
      (:before-commit
       (logical-slot-continuation-cold-empty-check data)
       (unless (eq :prepared (getf data :phase)) (error "empty handoff is not prepared"))
       (logical-slot-continuation-cold-empty-point :before-commit data)
       (logical-slot-continuation-cold-empty-check data)
       (unless (and (equal *logical-slot-imported-host-startup-owners* (getf data :owners))
                    (eq *wal-replication-slots* (getf data :old))
                    (zerop (hash-table-count (getf data :old)))
                    (logical-slot-publication-check-registry (getf data :new) (getf data :new-image)))
         (error "empty startup tables changed before publication"))
       (unless (loop for map across (getf data :next-maps)
                     for image across (getf data :next-images)
                     always (logical-slot-continuation-cold-map-matches-p map image))
         (error "empty startup prospective maps changed before publication"))
       (setf (getf data :phase) :installing))
      (:after-commit
       (logical-slot-continuation-cold-empty-check data)
       (unless (eq :published (getf data :phase)) (error "empty handoff is not published"))
       (logical-slot-continuation-cold-empty-point :after-commit data))
      (:before-retirement
       (logical-slot-continuation-cold-empty-check data)
       (logical-slot-continuation-cold-empty-point :before-retirement data)))))

(defun logical-slot-continuation-cold-empty-retire (reservation token control)
  (logical-slot-continuation-cold-empty-call token :retire (list reservation control)))

(defun logical-slot-continuation-cold-empty-retire-data (data reservation control)
  (logical-slot-continuation-cold-empty-check data)
  (unless (and (getf data :closed) (eq :published (getf data :phase))
               (eq reservation (getf data :reservation)) (eq control (getf data :control))
               (eq (getf data :token) (svref control 1)) (eq :empty-published (svref control 0))
               (eq *wal-replication-slots* (getf data :new))
               (logical-slot-publication-check-registry *wal-replication-slots* (getf data :new-image))
               (logical-slot-continuation-gc-fenced-p *pgdata*))
    (error "empty startup handoff has no closed publication receipt"))
  (let ((roots (logical-slot-continuation-cold-copy-map *logical-slot-continuation-cold-reservations*))
        (registries (logical-slot-continuation-cold-copy-map *logical-slot-continuation-cold-registries*)))
    (remhash (getf data :root) roots)
    (remhash (getf data :new) registries)
    (sb-sys:without-interrupts
      (setf *logical-slot-continuation-cold-reservations* roots
            *logical-slot-continuation-cold-registries* registries
            (svref control 0) :empty-released
            (getf data :phase) :released
            (getf data :active) nil)
      (remhash reservation *wal-snapshot-retention-pins*)
      (remhash reservation *logical-slot-continuation-cold-identities*)
      (remhash (getf data :token) *logical-slot-continuation-cold-empty-handoffs*)))
  t)

(defun logical-slot-continuation-cold-empty-revoke (data)
  (sb-sys:without-interrupts
    (setf (getf data :active) nil)
    (when (getf data :token)
      (remhash (getf data :token) *logical-slot-continuation-cold-empty-handoffs*)))
  nil)

(defun logical-slot-continuation-cold-empty-record-closed (data)
  (with-logical-slot-continuation-observation-check
      ((getf data :active) (getf data :failed) (getf data :thread) "empty startup close receipt")
    (logical-slot-continuation-cold-empty-check-registration data)
    (unless (and (eq :published (getf data :phase)) (not (getf data :closed))
                 (every (lambda (owner) (not (physical-adoption-owner-active-p owner)))
                        (getf data :owners)))
      (error "empty startup handoff still has live or already closed scopes"))
    (setf (getf data :closed) t)))

(defun call-with-logical-slot-continuation-cold-empty-handoff (gate ownership function)
  (unless (functionp function) (error "empty startup handoff requires a callback"))
  (let ((token nil) (closed nil) (abandon nil) (complete nil))
    (unwind-protect
         (progn
           (%call-with-logical-slot-continuation-cold-cleanup-inputs
            gate ownership
            (lambda (plan reservation check)
              (unless (logical-slot-continuation-cold-zero-child-plan-p plan)
                (error "empty startup handoff requires a private checked stable empty plan"))
              (let ((data (list :token nil :dispatcher nil :active t :failed nil
                                :thread sb-thread:*current-thread*
                                :gate gate :ownership ownership :reservation reservation
                                :root nil :old nil :new nil :new-image nil :source nil
                                :rows (logical-slot-continuation-copy (getf plan :retained))
                                :phase :scoped :closed nil :control nil
                                :maps nil :images nil :next-maps nil :next-images nil
                                :pin-table nil :pins nil :owners nil :owner-commits nil
                                :handoff-map *logical-slot-continuation-cold-empty-handoffs*
                                :denial-map *logical-slot-continuation-cold-empty-denied*))
                    (state nil))
                (setf state
                      (lambda (operation object value)
                        (unless (eq object token)
                          (error "empty startup handoff changed identity"))
                        (with-logical-slot-continuation-observation-check
                            ((getf data :active) (getf data :failed) (getf data :thread) "empty startup handoff")
                          (logical-slot-continuation-cold-empty-check-registration data)
                          (logical-slot-continuation-cold-empty-dispatch data operation value)))
                      token (%make-logical-slot-continuation-cold-empty-handoff :state state)
                      (getf data :token) token
                      (getf data :dispatcher) state
                      abandon (lambda () (logical-slot-continuation-cold-empty-revoke data))
                      closed
                      (lambda ()
                        (unless (and (getf data :active) (not (getf data :failed))
                                     (eq :published (getf data :phase))
                                     (eq sb-thread:*current-thread* (getf data :thread)))
                          (error "empty startup handoff cannot record closed scopes"))
                        (logical-slot-continuation-cold-empty-record-closed data)))
                (sb-thread:with-mutex (*wal-lock*)
                  (logical-slot-continuation-cold-reservation-call reservation :check)
                  (setf (getf data :root) (logical-slot-continuation-cold-reservation-call reservation :root)
                        (getf data :old) *wal-replication-slots*
                        (gethash token *logical-slot-continuation-cold-empty-handoffs*) state))
                (logical-slot-continuation-cold-empty-point :minted data)
                (funcall function token reservation check
                         (mapcar (lambda (row)
                                   (logical-slot-startup-entry-from-image (second row) (first row)))
                                 (getf data :rows))))))
           (setf complete t)
           (values token closed abandon))
      (unless complete (when abandon (funcall abandon))))))
