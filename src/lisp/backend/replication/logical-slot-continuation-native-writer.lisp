(in-package #:pg)

(defun logical-slot-continuation-writer-after-registry-image (before slot candidate)
  (mapcar
   (lambda (row)
     (if (eq slot (second row))
         (list (first row) slot (logical-slot-startup-candidate-image candidate)
               (wal-slot-profile candidate) (wal-replication-slot-logical-provenance candidate)
               (wal-replication-slot-pending-xids candidate)
               (logical-slot-publication-pending-image (wal-replication-slot-pending-xids candidate))
               (logical-slot-publication-slot-transients candidate))
         row))
   before))

(defun logical-slot-continuation-writer-make-context (proof group record candidate ledger facts flags)
  (let* ((slot (wal-retained-slot-scan-slot (logical-slot-continuation-native-proof-scan proof)))
         (name (getf record :name)) (before (logical-slot-publication-registry-image *wal-replication-slots*)))
    (%make-logical-slot-continuation-writer-context
     :proof proof :group group :record record :candidate candidate :ledger ledger
     :target-row (find name ledger :key #'first :test #'string=)
     :slot slot :name name :directory (logical-slot-continuation-native-proof-directory proof)
     :slot-root (logical-slot-continuation-native-proof-slot-root proof)
     :target-directory (logical-slot-continuation-native-proof-target-directory proof)
     :old-slot-image (second (find name (getf (getf record :old) :slots) :key #'first :test #'string=))
     :new-slot-image (second (find name (getf (getf record :new) :slots) :key #'first :test #'string=))
     :old-image (copy-seq (logical-slot-continuation-native-proof-old-image proof))
     :prepared-image (logical-slot-continuation-prepared-encode record)
     :new-image (logical-slot-continuation-encode (getf record :new))
     :selected-image (copy-seq (logical-slot-continuation-native-proof-old-image proof))
     :selected-facts (copy-list facts) :registry *wal-replication-slots* :before before
     :after (logical-slot-continuation-writer-after-registry-image before slot candidate)
     :mirrors (logical-slot-publication-mirrors) :flags flags)))

(defun logical-slot-continuation-native-writer-from-proof (proof control)
  (logical-slot-continuation-writer-control-call control :alive)
  (multiple-value-bind (group record candidate ledger added facts)
      (logical-slot-continuation-ensure-native-protection-under-lock proof)
    (declare (ignore added))
    (let ((flags (logical-slot-continuation-writer-control-call control :attach group)))
      (unless record (return-from logical-slot-continuation-native-writer-from-proof candidate))
      (logical-slot-continuation-native-proof-claim-under-lock proof)
      (let* ((context (logical-slot-continuation-writer-make-context proof group record candidate ledger facts flags))
             (writer nil) (active t) (thread sb-thread:*current-thread*) (mutex *wal-lock*))
        (labels ((state (operation object &optional first second)
                   (unless (and (eq object writer) (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*)
                                (sb-thread:holding-mutex-p mutex))
                     (error "continuation writer changed identity, thread or WAL mutex"))
                   (cond
                     ((eq operation :fail) (setf (logical-slot-continuation-writer-context-failed context) t))
                     ((eq operation :expire) (setf active nil))
                     (t
                      (unless active (error "continuation writer ended"))
                      (when (and (logical-slot-continuation-writer-context-failed context)
                                 (not (member operation '(:identity :cancel-ready))))
                        (error "continuation writer failed"))
                      (ecase operation
                        (:identity t)
                        (:context (unless (eq first context) (error "continuation writer context was copied")) t)
                        (:cancel-ready
                         (unless (and (eq first group) (not (car flags)))
                           (error "continuation writer cancellation attempted effects"))
                         t)
                        (:bind-check
                         (unless (and (eq first group)
                                      (eq :registered (logical-slot-continuation-writer-context-phase context))
                                      (null (logical-slot-continuation-writer-context-cell context))
                                      (equalp second (logical-slot-continuation-writer-context-old-image context)))
                           (error "continuation writer cannot claim this group image"))
                         t)
                        (:bind-cell
                         (unless (and (eq first group)
                                      (eq :registered (logical-slot-continuation-writer-context-phase context))
                                      (null (logical-slot-continuation-writer-context-cell context))
                                      (logical-slot-continuation-protection-call group :owns-cell (cons writer second)))
                           (error "continuation writer cannot bind this selection cell"))
                         (setf (logical-slot-continuation-writer-context-cell context) second
                               (logical-slot-continuation-writer-context-old-selection context) (car second)
                               (logical-slot-continuation-writer-context-phase context) :begun))
                        (:phase (logical-slot-continuation-writer-context-phase context))
                        (:guard (logical-slot-continuation-writer-guard context))
                        (:receipt
                         (unless (member (logical-slot-continuation-writer-context-phase context)
                                         '(:publication-ready :published))
                           (error "continuation writer has no publication receipt yet"))
                         (logical-slot-continuation-writer-context-receipt context))
                        (:run (logical-slot-continuation-writer-run context))
                        (:publish (logical-slot-continuation-writer-publish context first)))))))
          (setf writer (%make-logical-slot-continuation-writer :state #'state)
                (logical-slot-continuation-writer-context-writer context) writer
                (logical-slot-continuation-writer-context-receipt context)
                (%make-logical-slot-continuation-writer-receipt :writer writer))
          (let ((*logical-slot-continuation-writers* (cons writer *logical-slot-continuation-writers*)))
            (sb-sys:without-interrupts
              (unwind-protect
                   (progn
                     (setf (gethash writer *logical-slot-continuation-writer-identities*) #'state)
                     (sb-sys:with-local-interrupts
                       (logical-slot-continuation-writer-point context :registered)
                       (multiple-value-prog1 (logical-slot-continuation-writer-call writer :run)
                         (unless (eq :published (logical-slot-continuation-writer-call writer :phase))
                           (error "continuation writer returned before publication")))))
                (when (and (not (car flags)) (gethash writer *logical-slot-continuation-writer-identities*))
                  (handler-case (logical-slot-continuation-protection-call group :cancel writer)
                    (error (condition)
                      (logical-slot-continuation-writer-control-call control :poison)
                      (report-query-temp-cleanup-failure "continuation writer cancellation" condition))))
                (setf active nil)
                (remhash (logical-slot-continuation-writer-context-receipt context)
                         *logical-slot-continuation-writer-receipts*)
                (remhash writer *logical-slot-continuation-writer-identities*)))))))))

(defun logical-slot-continuation-native-acknowledge (name target &optional expected-owner)
  (unless (and (wal-valid-replication-slot-name-p name) (typep target '(unsigned-byte 64)))
    (ereport "22023" "invalid imported native acknowledgment"))
  (when (or (sb-thread:holding-mutex-p *wal-lock*)
            (sb-thread:holding-mutex-p *logical-slot-continuation-writer-lock*))
    (error "continuation writer lifetime must start outside WAL and cannot be reentered"))
  (sb-thread:with-mutex (*logical-slot-continuation-writer-lock*)
    (let ((control nil) (active t) (flags (cons nil nil)) (group nil) (failure nil) (complete nil)
                        (thread sb-thread:*current-thread*) (mutex *wal-lock*))
      (labels ((state (operation object value)
                 (unless (and (eq object control) active (eq thread sb-thread:*current-thread*)
                              (sb-thread:holding-mutex-p *logical-slot-continuation-writer-lock*))
                   (error "continuation writer outer lifetime ended or changed thread"))
                 (ecase operation
                   (:alive t)
                   (:attach
                    (assert (and (eq mutex *wal-lock*) (sb-thread:holding-mutex-p mutex)))
                    (when (and group (not (eq group value))) (error "continuation writer changed its group"))
                    (setf group value
                          failure (logical-slot-continuation-protection-call group :failure-cleanup))
                    flags)
                   (:poison (when failure (funcall failure))))))
        (setf control (%make-logical-slot-continuation-writer-control :state #'state))
        (let ((*logical-slot-continuation-writer-controls*
                (cons control *logical-slot-continuation-writer-controls*)))
          (sb-sys:without-interrupts
            (unwind-protect
                 (progn
                   (setf (gethash control *logical-slot-continuation-writer-control-identities*) #'state)
                   (multiple-value-prog1
                       (sb-sys:with-local-interrupts
                         (when *logical-slot-continuation-writer-test-hook*
                           (funcall *logical-slot-continuation-writer-test-hook* :lifetime-acquired nil))
                         (multiple-value-prog1
                             (call-with-logical-slot-continuation-native-proof
                              name target
                              (lambda (proof)
                                (sb-thread:with-mutex (*wal-lock*)
                                  (logical-slot-continuation-native-writer-from-proof proof control)))
                              expected-owner)
                           (when *logical-slot-continuation-writer-test-hook*
                             (funcall *logical-slot-continuation-writer-test-hook* :after-proof-return nil))))
                     (setf complete t)))
              (unless (or complete (not (car flags)))
                (when failure
                  (if (sb-thread:holding-mutex-p mutex)
                      (funcall failure)
                      (sb-thread:with-mutex (mutex) (funcall failure)))))
              (setf active nil)
              (remhash control *logical-slot-continuation-writer-control-identities*))))))))
