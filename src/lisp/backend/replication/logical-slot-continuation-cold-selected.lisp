(in-package #:pg)

(defun logical-slot-continuation-cold-decode-selected (bytes)
  ;; Decode the selected bytes as either a stable snapshot or PREPARED record.
  (if (and (<= 16 (length bytes))
           (equalp (subseq bytes 0 16) #(80 71 76 79 71 67 79 78 84 83 78 65 80 49 0 0)))
      (values :stable (logical-slot-continuation-decode bytes))
      (values :prepared (logical-slot-continuation-prepared-decode bytes))))

;; Classify selected child rows against stable or prepared snapshots.  This
;; helper does not partition or validate the ordinary rows of a full inventory.
(defun logical-slot-continuation-cold-classify-child-rows
    (kind model children binding certificate host)
  (ecase kind
    (:stable
     (logical-slot-continuation-check-selected-snapshot
      model binding certificate host children (sixth host))
     (values model :stable))
    (:prepared
     (dolist (key '(:old :new))
       (let ((snapshot (getf model key)))
         (logical-slot-continuation-check-selected-snapshot
          snapshot binding certificate host (getf snapshot :slots) (sixth host))))
     (let ((selection (logical-slot-continuation-prepared-classify model children)))
       (when (eq selection :blocked)
         (error "cold continuation complete inventory matches neither old nor new"))
       (values (getf model selection) selection)))))

;; Classify the selected generation against the child rows only.  Partitioning
;; also validates the ordinary rows in the complete disk inventory.
(defun logical-slot-continuation-cold-classify (kind model rows binding certificate host)
  (logical-slot-continuation-cold-classify-child-rows
   kind model (logical-slot-continuation-partition-rows rows certificate host)
   binding certificate host))

;; Observe exact selector presence or absence under a held private root.  A
;; selector is decoded as a canonical stable or PREPARED image.  A held
;; selector.tmp is unselected evidence, not bootstrap proof; prefix
;; authorization is a later step.
;; Keep original ledgers private and give the callback copies.  Its check
;; enforces owner thread, expiry and sticky failure before I/O.  The scope
;; expires before descriptor close; the owned source-proof FD primitive closes
;; and validates its return, including late close failure.  This observation
;; grants no plan, cleanup, publication or source authority.
(defun call-with-logical-slot-continuation-cold-selector-files
    (directory guard function &key require-selected)
  (unless (and (functionp guard) (functionp function) (typep require-selected 'boolean))
    (error "cold selector observation requires a guard, callback and selection policy"))
  (funcall guard)
  (let ((root-facts (physical-image-file-facts directory))
        (names nil))
    (unless (and (sb-posix:s-isdir (third root-facts))
                 (= #o700 (logand #o7777 (third root-facts)))
                 (= (fifth root-facts) (sb-posix:geteuid)))
      (error "cold selector observation requires a private directory"))
    (setf names (logical-slot-continuation-selector-file-names directory))
    (when (and require-selected (not (member "selector" names :test #'string=)))
      (error "cold continuation has no canonical selected selector"))
    (labels ((check-directory ()
               (funcall guard)
               (unless (and (equal root-facts (physical-image-file-facts directory))
                            (equal names (logical-slot-continuation-selector-file-names directory)))
                 (error "cold continuation selected directory changed")))
             (with-file (name consume)
               (check-directory)
               (multiple-value-prog1
                   (if (member name names :test #'string=)
                       (call-with-logical-slot-copy-source-proof-fd
                        directory name nil
                        (lambda (fd facts)
                          (let ((ledger
                                  (list (copy-seq name) (copy-list facts)
                                        (logical-slot-copy-import-control-read-fd
                                         fd (logical-slot-continuation-prepared-maximum-bytes)))))
                            (check-directory)
                            (funcall consume fd ledger))))
                       (funcall consume nil nil))
                 (check-directory)))
             (invoke (kind model fd selected temp-fd temporary)
               (let ((active t) (failed nil) (thread sb-thread:*current-thread*))
                 (labels ((check ()
                            (unless (and active (eq thread sb-thread:*current-thread*))
                              (error "cold selector observation expired or changed thread"))
                            (when failed (error "cold selector observation failed"))
                            (sb-sys:without-interrupts
                              (let ((complete nil))
                                (unwind-protect
                                     (multiple-value-prog1
                                         (sb-sys:with-local-interrupts
                                           (check-directory)
                                           (dolist (entry (list (list fd selected)
                                                                (list temp-fd temporary)))
                                             (when (second entry)
                                               (logical-slot-imported-check-held-file
                                                directory (first (second entry)) (first entry)
                                                (second (second entry)) (third (second entry)))))
                                           t)
                                       (setf complete t))
                                  (unless complete (setf failed t)))))))
                   (unwind-protect
                        (progn
                          (check)
                          (multiple-value-prog1
                              (funcall function kind (logical-slot-continuation-copy model)
                                       (logical-slot-continuation-copy selected)
                                       (logical-slot-continuation-copy temporary) #'check)
                            (check)))
                     (sb-sys:without-interrupts (setf active nil)))))))
      (with-file
        "selector"
        (lambda (fd selected)
          (multiple-value-bind (kind model)
              (if selected (logical-slot-continuation-cold-decode-selected (third selected))
                  (values :absent nil))
            (with-file "selector.tmp"
              (lambda (temp-fd temporary)
                (invoke kind model fd selected temp-fd temporary)))))))))

;; Supply the storage directory and its guard to the direct observer.
(defun call-with-logical-slot-continuation-cold-selector-observation
    (storage function &key require-selected)
  (call-with-logical-slot-continuation-cold-selector-files
   (logical-slot-continuation-storage-call storage :directory)
   (lambda () (logical-slot-continuation-storage-check storage))
   function :require-selected require-selected))

;; Preserve the existing selected-only interface: absence is an error.
(defun call-with-logical-slot-continuation-cold-selected-files (storage function)
  (call-with-logical-slot-continuation-cold-selector-observation
   storage function :require-selected t))
