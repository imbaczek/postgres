(in-package #:pg)

(defvar *logical-slot-continuation-cold-commits*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defun logical-slot-continuation-cold-commit-call (plan operation owner)
  (unless (logical-slot-continuation-cold-commit-p plan)
    (error "cold commit requires its registered plan"))
  (let* ((state (logical-slot-continuation-cold-commit-state plan))
         (binding (gethash plan *logical-slot-continuation-cold-commits*)))
    (unless (and (functionp state) binding (eq state (car binding)))
      (when (functionp state) (funcall state :lost plan owner))
      (error "cold commit lost its registered plan"))
    (funcall state operation plan owner)))

(defun logical-slot-continuation-cold-authority-maps ()
  (vector *logical-slot-continuation-protections*
          *logical-slot-continuation-protection-registries*
          *logical-slot-continuation-protection-identities*
          *logical-slot-continuation-cold-reservations*
          *logical-slot-continuation-cold-registries*))

(defun logical-slot-continuation-cold-map-image (table)
  (loop for key being the hash-keys of table using (hash-value value)
        collect (cons (if (member (hash-table-test table) '(eq eql)) key
                          (logical-slot-continuation-copy key))
                      value)))

(defun logical-slot-continuation-cold-map-matches-p (table image)
  (and (= (hash-table-count table) (length image))
       (every (lambda (row)
                (multiple-value-bind (value present) (gethash (car row) table)
                  (and present (eq value (cdr row)))))
              image)))

(defun logical-slot-continuation-cold-copy-map (table)
  (let ((copy (make-hash-table
               :test (hash-table-test table)
               :size (max 16 (* 2 (1+ (hash-table-count table))))
               :weakness (sb-ext:hash-table-weakness table)
               :synchronized (sb-ext:hash-table-synchronized-p table))))
    (maphash (lambda (key value) (setf (gethash key copy) value)) table)
    copy))

(defun logical-slot-continuation-cold-commit-prepare-maps (data)
  (let* ((original (logical-slot-continuation-cold-authority-maps))
         (copies (map 'vector #'logical-slot-continuation-cold-copy-map original))
         (root (getf data :root)) (registry (getf data :registry))
         (old-registry (first (getf data :mirrors)))
         (reservation (getf data :reservation)) (group (getf data :group)))
    (unless (and (null (gethash root (svref original 0)))
                 (null (gethash registry (svref original 1)))
                 (eq reservation (gethash root (svref original 3)))
                 (eq reservation (gethash old-registry (svref original 4))))
      (error "cold commit changed its original authority anchors"))
    (setf (gethash (copy-list root) (svref copies 0)) group
          (gethash registry (svref copies 1)) group
          (gethash group (svref copies 2)) (logical-slot-continuation-protection-state group))
    (remhash root (svref copies 3))
    (remhash old-registry (svref copies 4))
    (values original (map 'vector #'logical-slot-continuation-cold-map-image original)
            copies (map 'vector #'logical-slot-continuation-cold-map-image copies))))

(defun logical-slot-continuation-cold-commit-check-maps (data published)
  (let* ((expected (getf data (if published :new-maps :original-maps)))
         (images (getf data (if published :new-map-images :original-map-images)))
         (current (logical-slot-continuation-cold-authority-maps)))
    (unless (and expected
                 (loop for table across current for saved across expected for image across images
                       always (and (eq table saved)
                                   (logical-slot-continuation-cold-map-matches-p table image)))
                 (loop for table across (getf data :new-maps)
                       for image across (getf data :new-map-images)
                       always (logical-slot-continuation-cold-map-matches-p table image)))
      (error "cold commit authority maps changed"))))

(defun logical-slot-continuation-cold-commit-postcheck (data)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (eq :published (svref (getf data :group-control) 0))
               (eq :published (svref (getf data :completion-control) 0))
               (eq :published (svref (getf data :reservation-control) 0))
               (eq (getf data :registry) *wal-replication-slots*))
    (error "cold commit changed its published phases or registry"))
  (logical-slot-continuation-cold-commit-check-maps data t)
  (logical-slot-publication-check-registry (getf data :registry) (getf data :image))
  (logical-slot-continuation-protection-call (getf data :group) :check)
  (logical-slot-continuation-cold-reservation-call (getf data :reservation) :check)
  t)

(defun logical-slot-continuation-cold-bind-handoff (reservation plan control)
  (let* ((binding (gethash plan *logical-slot-continuation-cold-commits*))
         (data (and binding (cdr binding))) (owner (and data (getf data :owner))))
    (logical-slot-continuation-cold-commit-call plan :identity owner)
    (unless (and (eq reservation (getf data :reservation))
                 (null (getf data :reservation-control))
                 (eq :claimed (svref (getf data :completion-control) 0))
                 (eq :staged (svref (getf data :group-control) 0)))
      (error "cold commit cannot bind this reservation handoff"))
    (logical-slot-continuation-cold-completion-call (getf data :completion) :check)
    (let ((group (getf data :group)))
      (funcall (logical-slot-continuation-protection-state group) :identity group)
      (unless (and (equal (getf data :root) (logical-slot-continuation-protection-root-identity group))
                   (equalp (getf data :binding)
                           (funcall (logical-slot-continuation-protection-state group) :binding group))
                   (equalp (getf data :selected-image)
                           (funcall (logical-slot-continuation-protection-state group) :image group))
                   (eq (getf data :group-failure)
                       (funcall (logical-slot-continuation-protection-state group) :failure-cleanup group)))
        (error "cold commit changed its prepared group")))
    (sb-sys:without-interrupts
      (setf (svref control 1) (getf data :group)
            (svref control 2) (getf data :registry)
            (svref control 3) (getf data :group-failure)
            (getf data :reservation-control) control))
    control))

(defun logical-slot-continuation-cold-commit-check (data)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (logical-slot-continuation-cold-completion-call (getf data :completion) :check)
  (if (eq :published (svref (getf data :group-control) 0))
      (progn
        (funcall (getf data :guard))
        (logical-slot-continuation-cold-commit-postcheck data))
      (progn
        (logical-slot-continuation-cold-commit-check-maps data nil)
        (unless (and (eq :claimed (svref (getf data :completion-control) 0))
                     (eq :reserved (svref (getf data :reservation-control) 0))
                     (eq (getf data :group) (svref (getf data :reservation-control) 1))
                     (eq (getf data :registry) (svref (getf data :reservation-control) 2))
                     (eq (getf data :group-failure) (svref (getf data :reservation-control) 3)))
          (error "cold commit changed its staged handoff"))
        (funcall (getf data :group-check))
        (logical-slot-publication-check-registry (getf data :registry) (getf data :image))
        (funcall (getf data :guard))
        (logical-slot-continuation-cold-reservation-call (getf data :reservation) :check))))

(defun call-with-logical-slot-continuation-cold-commit
    (owner completion guard function failure-cell)
  (unless (and (functionp guard) (functionp function) (consp failure-cell)
               (functionp (car failure-cell)) (eq (cdr failure-cell) *wal-lock*))
    (error "cold commit requires final resources and an outer failure receipt"))
  (let* ((data (logical-slot-continuation-cold-completion-call completion :claim))
         (completion-control (cdr (gethash completion *logical-slot-continuation-cold-completions*)))
         (selected (third (getf (getf data :observation) :selector)))
         (registry (getf data :registry))
         (mutex *wal-lock*) (thread sb-thread:*current-thread*)
         (group nil) (group-control nil) (group-failure nil) (group-check nil)
         (handoff nil) (original nil) (copies nil) (plan nil)
         (active t) (busy nil) (failed nil) (complete nil) (cleanup nil))
    (sb-thread:with-mutex (*wal-lock*)
      (multiple-value-setq (group group-control group-failure group-check)
        (logical-slot-continuation-build-protection-under-lock
         registry selected (getf data :binding) (getf data :root)
         (getf data :origin) (getf data :namespace) :staged)))
    (let ((reservation-cleanup (car failure-cell)))
      (setf cleanup (lambda ()
                      (unwind-protect (funcall group-failure)
                        (funcall reservation-cleanup)))))
    (setf data (append (list :owner owner :completion completion :completion-control completion-control
                             :group group :group-control group-control :group-failure group-failure
                             :group-check group-check :guard guard :selected-image selected
                             :reservation-control nil :original-maps nil :original-map-images nil
                             :new-maps nil :new-map-images nil)
                       data))
    (labels ((state (operation candidate actual-owner)
               (unless (and active (eq candidate plan) (eq actual-owner owner)
                            (eq mutex *wal-lock*) (eq thread sb-thread:*current-thread*))
                 (error "cold commit ended or changed owner, identity, mutex or thread"))
               (sb-sys:without-interrupts
                 (when (eq operation :lost) (setf failed t) (error "cold commit lost its registration"))
                 (when (or failed busy) (setf failed t) (error "cold commit failed or is busy"))
                 (if (eq operation :identity) t
                     (let ((done nil))
                       (setf busy t)
                       (unwind-protect
                            (multiple-value-prog1
                                (sb-sys:with-local-interrupts
                                  (unless (and (eq group (getf data :group))
                                               (eq group-control (getf data :group-control))
                                               (eq completion-control (getf data :completion-control))
                                               (eq handoff (getf data :reservation-control))
                                               (eq original (getf data :original-maps))
                                               (eq copies (getf data :new-maps)))
                                    (error "cold commit changed its private state cells"))
                                  (logical-slot-continuation-cold-commit-check data)
                                  (when failed (error "cold commit failed during its guard"))
                                  (ecase operation (:check t) (:data data)))
                              (setf done t))
                         (setf busy nil)
                         (unless done (setf failed t))))))))
      (sb-sys:without-interrupts
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (setf plan (%make-logical-slot-continuation-cold-commit :state #'state))
                   (setf (gethash plan *logical-slot-continuation-cold-commits*) (cons #'state data))
                   (setf (car failure-cell) cleanup)
                   (sb-thread:with-mutex (*wal-lock*)
                     (funcall guard)
                     (setf handoff
                           (logical-slot-continuation-cold-reservation-call
                            (getf data :reservation) :prepare-handoff plan))
                     (logical-slot-continuation-cold-publication-point
                      :before-group-pin completion group registry)
                     (setf (gethash group *wal-snapshot-retention-pins*)
                           (logical-slot-continuation-protection-fork group))
                     (logical-slot-continuation-cold-publication-point
                      :after-group-pin completion group registry)
                     (multiple-value-bind (old old-images new new-images)
                         (logical-slot-continuation-cold-commit-prepare-maps data)
                       (setf original old copies new
                             (getf data :original-maps) old (getf data :original-map-images) old-images
                             (getf data :new-maps) new (getf data :new-map-images) new-images))
                     (logical-slot-continuation-cold-publication-point
                      :maps-ready completion group registry)
                     (state :check plan owner))
                   (multiple-value-prog1 (funcall function plan)
                     (sb-thread:with-mutex (*wal-lock*) (state :check plan owner))))
               (setf complete t))
          (setf active nil)
          (when plan (remhash plan *logical-slot-continuation-cold-commits*))
          (unless complete (logical-slot-continuation-cold-preparation-fail cleanup mutex)))))))
