(in-package #:pg)

(defun physical-reseed-follow-activation-predecessor (record)
  ;; Validate only the core, configuration and settlement.  Later groups
  ;; must not become part of their own predecessor.
  (physical-reseed-follow-copy
   (append (physical-reseed-follow-core-projection record)
           (list :configuration (getf record :configuration)
                 :settlement (getf record :settlement)))))

(defun physical-reseed-follow-activation-protected-identities (record)
  (let* ((configuration (getf record :configuration))
         (device (getf (getf (getf record :attempt) :local) :device))
         (identities (physical-reseed-follow-configuration-protected-identities record)))
    (physical-adoption-install-fact-copy (getf configuration :workspace) device identities :directory t)
    (physical-reseed-follow-configuration-directories-copy
     (getf configuration :directories) device identities)
    (physical-reseed-follow-configuration-entries-copy
     (getf configuration :entries) (getf configuration :images) device identities)
    identities))

(defun physical-reseed-follow-activation-entry-copy (value record image)
  (let* ((device (getf (getf (getf record :attempt) :local) :device))
         (identities (physical-reseed-follow-activation-protected-identities record))
         (entries (physical-adoption-install-trees-copy
                   (list (list "follow.active" (list value))) device identities))
         (entry (first (second (first entries)))))
    (unless (and (eq :file (second entry)) (= #o600 (third entry))
                 (= (first image) (sixth entry)) (= (second image) (seventh entry)))
      (error "activation entry differs from its private active image"))
    entry))

(defun %physical-reseed-follow-activation-copy (value record)
  (physical-follow-check-plist value '(:phase :step :image :entry))
  (let* ((phase (getf value :phase)) (step (getf value :step))
                                     (image (getf value :image)) (entry (getf value :entry)))
    (unless (and (eq :inert (getf (getf record :configuration) :phase))
                 (eq :handed-off (getf (getf record :settlement) :phase))
                 (listp image) (= 2 (length image))
                 (typep (first image) '(integer 1 *))
                 (<= (first image) +physical-follow-config-maximum-bytes+)
                 (typep (second image) '(unsigned-byte 64))
                 (typep step '(integer 0 2))
                 (case phase
                   (:building (and (zerop step) (null entry)))
                   (:staged (and (zerop step) entry))
                   (:activating entry)
                   (:active (and (= step 2) entry))))
      (error "activation differs from its settled inert predecessor or image phase"))
    (list :phase phase :step step :image (copy-list image)
          :entry (when entry (physical-reseed-follow-activation-entry-copy entry record image)))))

(defun physical-reseed-follow-activation-copy (value record)
  ;; The image is (byte-count digest); entry describes the private active
  ;; file.  Building has no entry, staged has one, and activating advances
  ;; through steps 0 to 2 before active at step 2.
  ;; These are structural checks, not evidence that a file was published.
  (physical-reseed-install-check-literal (list value record))
  (physical-follow-check-plist value '(:phase :step :image :entry))
  (%physical-reseed-follow-activation-copy value (physical-reseed-follow-activation-predecessor record)))

(defun physical-reseed-follow-activation-check-transition (before after)
  ;; Freeze the predecessor and image, then freeze the entry once staged.
  ;; Each transition advances at most one activation step.
  (let ((old (getf before :activation)) (new (getf after :activation)))
    (unless (every (lambda (key) (equal (getf before key) (getf after key)))
                   '(:version :attempt :install :base :generations :configuration :settlement))
      (error "activation freezes its historical predecessors"))
    (unless
        (and new
             (cond
               ((null old) (eq :building (getf new :phase)))
               ((equal old new) t)
               ((equal (getf old :image) (getf new :image))
                (let ((old-phase (getf old :phase)) (new-phase (getf new :phase))
                                                    (old-step (getf old :step)) (new-step (getf new :step)))
                  (or (and (eq old-phase :building) (eq new-phase :staged))
                      (and (equal (getf old :entry) (getf new :entry))
                           (or (and (eq old-phase :staged) (eq new-phase :activating)
                                    (zerop new-step))
                               (and (eq old-phase :activating)
                                    (or (and (eq new-phase :activating)
                                             (= new-step (1+ old-step)))
                                        (and (= old-step new-step 2) (eq new-phase :active)))))))))))
      (error "activation cannot regress, skip a prefix, rebind or disappear"))
    t))
