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

(defun call-with-logical-slot-continuation-cold-source-readers
    (storage source observation archive check-source function)
  (let* ((facts (logical-slot-continuation-storage-call storage :facts))
         (snapshot (logical-slot-continuation-copy (getf observation :snapshot)))
         (horizon (getf observation :horizon)))
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
       (lambda (prove check-tail local-reader local-guard)
         (funcall prove)
         (logical-slot-continuation-storage-call
          storage :with-sidecar
          (lambda (sidecar-reader check-files)
            (let ((active t) (failed nil) (thread sb-thread:*current-thread*))
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
                            (unless (find slot (logical-slot-startup-source-entries source)
                                          :key #'logical-slot-startup-entry-slot :test #'eq)
                              (error "cold reader requires its exact startup candidate"))
                            (multiple-value-prog1
                                (call-with-logical-slot-continuation-lanes
                                 facts snapshot slot archive sidecar-reader local-reader #'guard callback
                                 :end horizon :batch-policy :logical-reader)
                              (check)))))
                  (unwind-protect
                       (progn
                         (check)
                         (multiple-value-prog1 (funcall function #'read-slot #'check)
                           (check)
                           (funcall prove)
                           (check)))
                    (sb-sys:without-interrupts (setf active nil)))))))))
       :boundaries #'boundaries :end horizon :batch-policy :logical-reader :require-closed nil))))

(defun call-with-logical-slot-continuation-cold-reconstruction (gate ownership function)
  (call-with-logical-slot-continuation-cold-context
   gate ownership
   (lambda (storage reservation archive)
     (call-with-logical-slot-continuation-cold-source
      gate ownership storage reservation
      (lambda (source observation actual check-source)
        (call-with-logical-slot-continuation-cold-source-readers
         storage source observation archive check-source
         (lambda (read-slot check)
           (dolist (entry (logical-slot-startup-source-entries source))
             (let ((slot (logical-slot-startup-entry-slot entry)))
               (when (eq :reserved (wal-replication-slot-wal-status slot))
                 (funcall read-slot slot
                          (lambda (cursor seams)
                            (declare (ignore seams))
                            (setf (wal-replication-slot-pending-xids slot)
                                  (logical-slot-startup-derive-pending-with-cursor source slot cursor)))))))
           (funcall check)
           (funcall function source observation actual check))))))))
