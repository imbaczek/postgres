(in-package #:pg)

(defun call-with-logical-slot-continuation-cold-observation (storage reservation function)
  ;; Lend a read-only classification and recheck callback within this thread.
  ;; Scope and sticky failure checks precede I/O, so an expired callback does
  ;; no I/O.  The closed-row ledgers are reobserved before the scope ends.
  (let ((facts (logical-slot-continuation-storage-call storage :facts))
        (binding (logical-slot-continuation-storage-call storage :binding))
        (host (logical-slot-continuation-storage-call storage :host)))
    (call-with-logical-slot-continuation-cold-selected-files
     storage
     (lambda (kind model selector temporary check-selector)
       (logical-slot-continuation-storage-call
        storage :with-cold-slot-root
        (lambda (directory check-directory)
          (let ((active t) (failed nil) (thread sb-thread:*current-thread*))
            (macrolet ((in-scope (&body body)
                         (let ((complete (gensym "COMPLETE")))
                           `(progn
                              (unless (and active (eq thread sb-thread:*current-thread*))
                                (error "cold continuation observation expired or changed thread"))
                              (when failed (error "cold continuation observation failed"))
                              (sb-sys:without-interrupts
                                (let ((,complete nil))
                                  (unwind-protect
                                       (multiple-value-prog1
                                           (sb-sys:with-local-interrupts (progn ,@body))
                                         (setf ,complete t))
                                    (unless ,complete (setf failed t)))))))))
              (labels ((check ()
                         (in-scope
                          (funcall check-directory)
                          (funcall check-selector)
                          (sb-thread:with-mutex (*wal-lock*)
                            (logical-slot-continuation-cold-reservation-call reservation :check)))))
                (unwind-protect
                     (multiple-value-bind (rows ledger)
                         (logical-slot-continuation-cold-read-inventory
                          directory #'check (when (eq kind :prepared) model))
                       (multiple-value-bind (snapshot choice)
                           (logical-slot-continuation-cold-classify
                            kind model rows binding (getf facts :certificate) host)
                         (flet ((recheck ()
                                  (in-scope
                                   (check)
                                   (multiple-value-bind (current observed)
                                       (logical-slot-continuation-cold-read-inventory
                                        directory #'check (when (eq kind :prepared) model))
                                     (unless (and (equalp rows current) (equalp ledger observed))
                                       (error "cold continuation observation changed before its scope ended")))
                                   (check))))
                           (multiple-value-prog1
                               (funcall function
                                        (logical-slot-continuation-copy
                                         (list :kind kind :choice choice :snapshot snapshot
                                               :selector selector :temporary temporary :rows rows
                                               :ledger ledger :horizon (sixth host)))
                                        reservation #'recheck)
                             (recheck)))))
                  (sb-sys:without-interrupts (setf active nil))))))))))))

(defun call-with-logical-slot-continuation-cold-context (gate ownership function)
  ;; Hold the actual startup owner and restored origin around the reservation
  ;; and observation.  Failure, including late close, retains the fork pin;
  ;; this scope does not open automatic loading, ACK or WAL retirement.
  (unless (functionp function) (error "cold continuation requires a callback"))
  (let ((failure (list nil)) (complete nil) (mutex *wal-lock*))
    (sb-sys:without-interrupts
      (unwind-protect
           (multiple-value-prog1
               (sb-sys:with-local-interrupts
                 (call-with-logical-slot-continuation-startup-storage
                  gate ownership
                  (lambda (storage facts)
                    (declare (ignore facts))
                    (logical-slot-continuation-storage-call
                     storage :with-origin
                     (lambda (archive)
                       (let ((reservation (logical-slot-continuation-ensure-cold-reservation storage failure)))
                         (when *logical-slot-continuation-cold-test-hook*
                           (funcall *logical-slot-continuation-cold-test-hook* :reservation-returned reservation))
                         (when *logical-slot-continuation-cold-test-hook*
                           (funcall *logical-slot-continuation-cold-test-hook* :protected reservation))
                         (multiple-value-prog1
                             (funcall function storage reservation archive)
                           (when *logical-slot-continuation-cold-test-hook*
                             (funcall *logical-slot-continuation-cold-test-hook* :after-observation reservation)))))))))
             (progn
               (when *logical-slot-continuation-cold-test-hook*
                 (funcall *logical-slot-continuation-cold-test-hook* :after-proof-return nil))
               (setf complete t)))
        (unless (or complete (null (car failure)))
          (if (sb-thread:holding-mutex-p mutex)
              (funcall (car failure))
              (sb-thread:with-mutex (mutex) (funcall (car failure)))))))))

(defun call-with-logical-slot-continuation-cold-state (gate ownership function)
  (unless (functionp function) (error "cold continuation requires a callback"))
  (call-with-logical-slot-continuation-cold-context
   gate ownership
   (lambda (storage reservation archive)
     (declare (ignore archive))
     (call-with-logical-slot-continuation-cold-observation storage reservation function))))
