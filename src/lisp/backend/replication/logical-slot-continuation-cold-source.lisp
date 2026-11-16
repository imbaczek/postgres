(in-package #:pg)

(defun logical-slot-continuation-cold-source-matches-p
    (source gate ownership entries snapshot horizon root)
  (and (logical-slot-imported-startup-source-p source)
       (eq gate (logical-slot-startup-source-gate-owner source))
       (eq ownership (logical-slot-startup-source-ownership source))
       (equal root (logical-slot-startup-source-root source))
       (eql horizon (logical-slot-startup-source-end source))
       (eql (getf snapshot :system-id) (logical-slot-startup-source-system-id source))
       (eql (getf snapshot :history-target) (logical-slot-startup-source-timeline source))
       (eq :timeline (logical-slot-startup-source-namespace source))
       (equalp (getf snapshot :history) (logical-slot-startup-source-history-bytes source))
       (equal entries (logical-slot-startup-source-entries source))
       (member source *logical-slot-startup-sources* :test #'eq)))

(defun call-with-logical-slot-continuation-cold-source
    (gate ownership storage reservation function)
  ;; Turn the actual cold observation into a scoped imported source.  The
  ;; caller retains startup ownership; the capability only checks inventory.
  (call-with-logical-slot-continuation-cold-observation
   storage reservation
   (lambda (observation actual recheck)
     (unless (eq actual reservation) (error "cold startup inventory changed its reservation"))
     (let* ((snapshot (logical-slot-continuation-copy (getf observation :snapshot)))
            (horizon (getf observation :horizon))
            (entries (mapcar (lambda (row) (logical-slot-startup-entry-from-image (second row) (first row)))
                             (getf observation :rows)))
            (root (logical-wal-current-root-identity))
            (inventory nil) (bound nil) (frozen nil)
            (active t) (failed nil) (thread sb-thread:*current-thread*) (mutex *wal-lock*))
       (funcall recheck)
       (labels ((state (operation candidate source)
                  ;; Reject copies and foreign callers before sticky failure can
                  ;; revoke the genuine source. Expired checks do no disk I/O.
                  (unless (and (eq inventory candidate) (eq thread sb-thread:*current-thread*)
                               (eq mutex *wal-lock*))
                    (error "cold startup inventory changed identity, thread or WAL mutex"))
                  (when (eq operation :preflight)
                    (unless (and active (not failed) (null bound) (null source))
                      (error "cold startup inventory cannot be reused"))
                    (return-from state t))
                  (unless (if (eq operation :bind) (null bound) (eq source bound))
                    (error "cold startup inventory belongs to another source"))
                  (sb-sys:without-interrupts
                    (let ((complete nil))
                      (unwind-protect
                           (multiple-value-prog1
                               (sb-sys:with-local-interrupts
                                 (unless (and active (not failed)
                                              (eq #'state (gethash inventory *logical-slot-startup-inventories*))
                                              (logical-slot-startup-source-active-p source)
                                              (eq inventory (logical-slot-startup-source-inventory source))
                                              (logical-slot-continuation-cold-source-matches-p
                                               source gate ownership entries snapshot horizon root))
                                   (error "cold startup inventory ended, failed or changed its source"))
                                 (ecase operation
                                   (:bind (setf bound source
                                                frozen (logical-slot-imported-startup-freeze-entries source)))
                                   ((:check :observe)
                                    (unless (logical-slot-imported-startup-entries-equal-p
                                             frozen (logical-slot-startup-source-entries source))
                                      (error "cold startup inventory changed its captured candidates"))
                                    (when (eq operation :observe) (funcall recheck))))
                                 (unless (eq #'state (gethash inventory *logical-slot-startup-inventories*))
                                   (error "cold startup inventory lost its registration"))
                                 t)
                             (setf complete t))
                        (unless complete
                          (setf failed t (logical-slot-startup-source-active-p source) nil)))))))
         (setf inventory (%make-logical-slot-startup-inventory :state #'state))
         (sb-sys:without-interrupts
           (unwind-protect
                (progn
                  (setf (gethash inventory *logical-slot-startup-inventories*) #'state)
                  (sb-sys:with-local-interrupts
                    (call-with-logical-slot-imported-startup-capture
                     (lambda (capture)
                       (%call-with-logical-slot-startup-source
                        gate ownership entries
                        (lambda (source)
                          (call-with-logical-slot-imported-startup-principal
                           source (lambda (checked) (funcall function checked observation reservation recheck))))
                        capture inventory)))))
             (setf active nil)
             (remhash inventory *logical-slot-startup-inventories*))))))))

;; The first child EOR can follow a legal segment gap.  Keep the original fork
;; F as the selection boundary even when the EOR starts after it.

(defun logical-slot-continuation-cold-select-child (candidate record raw fork parent child history)
  (unless (and (= (wal-record-start record) (logical-wal-eor-position fork))
               (= (wal-record-type record) +wal-record-end-of-recovery+))
    (error "cold continuation does not start with its selected child EOR"))
  (setf (physical-store-selection candidate)
        (list :history history :target child :parent parent :fork fork
              :eor-bytes raw :previous (physical-store-selection candidate))))

;; Replay the genuine archive from D through F to seed physical replay from F
;; through L.  This checks child initialization and returns the completed policy
;; for slot lifetime checks; it grants no admission or publication authority.

(defun logical-slot-continuation-cold-policy (facts snapshot horizon archive map-records)
  (let* ((certificate (getf facts :certificate))
         (fork (getf certificate :fork))
         (parent (getf certificate :parent-timeline))
         (child (first (getf snapshot :origin)))
         (history (logical-slot-continuation-history
                   (getf snapshot :history-target) (getf snapshot :history))))
    (multiple-value-bind (prefix completed)
        (logical-slot-copy-source-proof-scan-archive
         facts (logical-wal-archive-input archive) (logical-wal-archive-reader archive))
      (let* ((base (logical-slot-copy-import-base-state
                    (getf facts :manifest) (getf facts :descriptor) (getf facts :control)
                    :reservation (getf prefix :reservation)))
             (next (getf prefix :next-xid))
             (candidate (%make-physical-store
                         :system-id (getf certificate :system-id) :timeline parent
                         :base-lsn fork :floor-lsn fork :flushed-lsn horizon
                         :selection (list :history (logical-slot-copy-source-proof-parent-history facts)
                                          :target parent)))
             (seed (list fork parent next (getf completed :source-checkpoint)
                         (getf prefix :reservation) (getf prefix :source-xids)
                         (getf prefix :applied-resource-fact) (getf prefix :applied-fpw-fact)
                         (getf completed :logical-fact)))
             (seen-eor nil))
        (setf (getf base :required-resource-fact) (getf prefix :applied-resource-fact)
              (getf base :required-fpw-fact) (getf prefix :applied-fpw-fact)
              (getf base :required-logical-fact) (getf completed :logical-fact))
        (multiple-value-bind (result final)
            (physical-follow-scan-replay-from-seed-core
             candidate base (list fork parent) (list fork parent) next seed
             (lambda (visit start)
               (funcall map-records
                        (lambda (record raw)
                          (unless seen-eor
                            (logical-slot-continuation-cold-select-child
                             candidate record raw fork parent child history)
                            (setf seen-eor t))
                          (funcall visit record raw))
                        start))
             t)
          (unless (and seen-eor (null (getf result :disposition))
                       (= child (getf result :timeline))
                       (= child (getf (getf final :source-checkpoint) :timeline 0))
                       (getf final :logical-fact))
            (error "cold continuation lacks completed child initialization and logical policy"))
          (physical-logical-check-fact
           (getf final :logical-fact) history (getf result :terminal) child)
          (list :horizon horizon :result result :completed final))))))

;; :POLICY requests the completed replay policy as an additional callback
;; argument.  Reprove it and the live source before leaving this scope.

(defun call-with-logical-slot-continuation-cold-source-readers
    (storage source observation archive check-source function &key policy receipt)
  ;; S is the selected sealed end and L the observed durable horizon (S <= L).
  ;; Each reserved slot reads continuously from its start through L, carrying
  ;; pending transactions across any D, F and S seams it crosses. Reprove
  ;; before returning outward.
  (let* ((facts (logical-slot-continuation-storage-call storage :facts))
         (snapshot (logical-slot-continuation-copy (getf observation :snapshot)))
         (horizon (getf observation :horizon)) (tail-witness nil))
    (flet ((boundaries ()
             (logical-slot-startup-check-live source)
             (funcall check-source)
             (logical-slot-continuation-runtime-tail-observer
              (second (getf snapshot :origin)) (first (getf snapshot :sealed-end))
              (logical-slot-continuation-prove-parent-boundaries
               storage facts archive
               (logical-slot-continuation-saved-boundaries snapshot (getf facts :certificate))
               :batch-policy :logical-reader)
              :logical-reader)))
      (call-with-logical-slot-continuation-local-tail-source
       storage snapshot
       (lambda (prove check-tail local-reader local-guard &optional map-records)
         (setf tail-witness (nth-value 1 (funcall prove)))
         (logical-slot-continuation-storage-call
          storage :with-sidecar
          (lambda (sidecar-reader check-files)
            (let ((active t) (failed nil) (thread sb-thread:*current-thread*)
                             (proved-policy (when policy
                                              (list* :tail tail-witness
                                                     (logical-slot-continuation-cold-policy
                                                      facts snapshot horizon archive map-records)))))
              (macrolet ((in-scope (&body body)
                           (let ((complete (gensym "COMPLETE")))
                             `(progn
                                (unless (and active (not failed) (eq thread sb-thread:*current-thread*))
                                  (error "cold startup readers ended, failed or changed thread"))
                                (sb-sys:without-interrupts
                                  (let ((,complete nil))
                                    (unwind-protect
                                         (multiple-value-prog1
                                             (sb-sys:with-local-interrupts (progn ,@body))
                                           (setf ,complete t))
                                      (unless ,complete (setf failed t)))))))))
                (labels ((check ()
                           (in-scope
                            (logical-slot-startup-check-inventory source)
                            (funcall check-files)
                            (funcall check-tail)))
                         (guard ()
                           (in-scope
                            (logical-slot-imported-startup-principal-under-lock source)
                            (funcall local-guard)))
                         (read-slot (slot callback)
                           (in-scope
                            (check)
                            (unless (and (find slot (logical-slot-startup-source-entries source)
                                               :key #'logical-slot-startup-entry-slot :test #'eq)
                                         (logical-slot-continuation-selected-child-p slot snapshot))
                              (error "cold reader requires its exact startup candidate"))
                            (multiple-value-prog1
                                (call-with-logical-slot-continuation-lanes
                                 facts snapshot slot archive sidecar-reader local-reader #'guard callback
                                 :end horizon :batch-policy :logical-reader)
                              (check)))))
                  (unwind-protect
                       (progn
                         (check)
                         (multiple-value-prog1
                             (apply function #'read-slot #'check
                                    (when policy (list (logical-slot-continuation-copy proved-policy))))
                           (check)
                           (setf tail-witness (nth-value 1 (funcall prove)))
                           (when (and policy
                                      (not (equal proved-policy
                                                  (list* :tail tail-witness
                                                         (logical-slot-continuation-cold-policy
                                                          facts snapshot horizon archive map-records)))))
                             (error "cold continuation policy changed during reconstruction"))
                           (check)))
                    (sb-sys:without-interrupts (setf active nil)))))))))
       :boundaries #'boundaries :end horizon :batch-policy :logical-reader :require-closed nil :transcript policy :witness receipt))))

;; :POLICY also checks each saved slot's lifetime against the proved policy.

;; :RECEIPT requires :POLICY and passes the proof as a fifth callback argument.
;; Without :RECEIPT the callback keeps its four-argument interface.

(defun logical-slot-continuation-cold-reconstruct-ordinary (source slot)
  (when (and (wal-logical-slot-p slot)
             (eq :reserved (wal-replication-slot-wal-status slot)))
    (when (wal-replication-slot-child-owner slot)
      (error "ordinary reconstruction cannot use a child-owned candidate"))
    (setf (wal-replication-slot-pending-xids slot)
          (if (logical-slot-startup-known-p slot)
              (progn
                (logical-slot-startup-check-eligibility source slot)
                (logical-slot-startup-derive-pending source slot))
              (logical-slot-startup-derive-legacy-pending source slot))))
  slot)

(defun call-with-logical-slot-continuation-cold-reconstruction (gate ownership function &key policy receipt)
  ;; Reconstruct every saved reserved candidate privately through the actual
  ;; durable horizon L before the callback. This scope does not publish slots
  ;; or grant admission.
  (when (and receipt (not policy)) (error "cold reconstruction receipt requires its policy proof"))
  (call-with-logical-slot-continuation-cold-context
   gate ownership
   (lambda (storage reservation archive)
     (call-with-logical-slot-continuation-cold-source
      gate ownership storage reservation
      (lambda (source observation actual check-source)
        (call-with-logical-slot-continuation-cold-source-readers
         storage source observation archive check-source
         (lambda (read-slot check &optional proved-policy)
           (dolist (entry (logical-slot-startup-source-entries source))
             (let ((slot (logical-slot-startup-entry-slot entry)))
               (if (logical-slot-continuation-selected-child-p
                    slot (getf observation :snapshot))
                   (when (eq :reserved (wal-replication-slot-wal-status slot))
                     (funcall read-slot slot
                              (lambda (cursor seams)
                                (declare (ignore seams))
                                (setf (wal-replication-slot-pending-xids slot)
                                      (logical-slot-startup-derive-pending-with-cursor source slot cursor)))))
                   (logical-slot-continuation-cold-reconstruct-ordinary source slot))))
           (funcall check)
           (when policy
             (let* ((snapshot (getf observation :snapshot))
                    (history (logical-slot-continuation-history
                              (getf snapshot :history-target) (getf snapshot :history)))
                    (state (physical-logical-state-from-fact
                            (getf (getf proved-policy :completed) :logical-fact))))
               (dolist (entry (logical-slot-startup-source-entries source))
                 (when (logical-slot-continuation-selected-child-p
                        (logical-slot-startup-entry-slot entry) snapshot)
                   (logical-slot-copy-check-slot-lifetime
                    (logical-slot-startup-entry-slot entry) state history (getf observation :horizon))))))
           (apply function source observation actual check
                  (when receipt (list (logical-slot-continuation-copy proved-policy)))))
         :policy policy :receipt receipt))))))
