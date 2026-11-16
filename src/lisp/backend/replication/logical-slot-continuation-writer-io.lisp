(in-package #:pg)

(defun logical-slot-continuation-writer-base-check (context)
  ;; Keep the registry, mirrors and group's private selection cell stable.
  ;; Before publication its car is the old selection and cdr is the writer;
  ;; afterwards the car is the new image and the cdr is clear.
  (with-continuation-writer-context
      (context writer proof group registry before after mirrors cell old-selection new-image phase)
    (logical-slot-continuation-native-proof-check-under-lock proof)
    (logical-slot-continuation-protection-call group :check)
    (logical-slot-publication-check-registry registry (if (eq phase :published) after before))
    (unless (and (equal mirrors (logical-slot-publication-mirrors))
                 (consp cell)
                 (if (eq phase :published)
                     (and (eq (car cell) new-image) (null (cdr cell)))
                     (and (eq (car cell) old-selection) (eq (cdr cell) writer))))
      (error "continuation writer changed its live state or selection cell"))
    t))

(defun logical-slot-continuation-writer-check-selected (context)
  (with-continuation-writer-context (context directory selected-facts selected-image)
    (unless (equal '("selector") (logical-slot-copy-import-control-names directory 3))
      (error "continuation writer requires its sole selected image"))
    (call-with-logical-slot-copy-source-proof-fd
     directory "selector" nil
     (lambda (fd facts)
       (unless (and (equal selected-facts facts)
                    (equalp selected-image
                            (logical-slot-copy-import-control-read-fd
                             fd (logical-slot-continuation-prepared-maximum-bytes))))
         (error "continuation writer selected inode or bytes changed"))))))

(defun logical-slot-continuation-writer-guard (context)
  ;; Guard unchanged members and the selected inode through PREPARED and P7N.
  ;; An active replacement checks its own named file and exact inode receipt.
  (logical-slot-continuation-writer-base-check context)
  (with-continuation-writer-context
      (context slot-root ledger name target-row slot-resource selector-resource)
    (unless (equal (mapcar #'first ledger) (logical-slot-continuation-slot-inventory-names slot-root))
      (error "continuation writer complete inventory names changed"))
    (dolist (row ledger)
      (unless (and slot-resource (string= name (first row)))
        (let ((expected (if (string= name (first row)) target-row row)))
          (unless (equalp expected
                          (logical-slot-continuation-read-slot-inventory-row
                           slot-root (first row)
                           (lambda () (logical-slot-continuation-writer-base-check context))))
            (error "continuation writer inventory inode or bytes changed")))))
    (unless selector-resource (logical-slot-continuation-writer-check-selected context)))
  (logical-slot-continuation-writer-base-check context))

;; Compare the child rows with the new selector and the full disk ledger with
;; the expected replacement, including the ordinary physical slots.
(defun logical-slot-continuation-writer-check-new-inventory (context)
  (with-continuation-writer-context (context proof slot-root record ledger name target-row)
    (multiple-value-bind (rows observed)
        (logical-slot-continuation-read-slot-inventory
         slot-root (lambda () (logical-slot-continuation-writer-base-check context)))
      (unless (and (eq :new
                       (logical-slot-continuation-prepared-classify
                        record
                        (logical-slot-continuation-native-proof-partition-under-lock
                         proof rows)))
                   (equalp observed
                           (mapcar (lambda (row) (if (string= name (first row)) target-row row)) ledger)))
        (error "continuation writer did not preserve its exact new inventory")))))

(defun logical-slot-continuation-writer-recheck-before-intent (context operation)
  ;; Recheck the fresh plan, pending state and exact selector inode before intent.
  (with-continuation-writer-context
      (context proof prepared-image candidate ledger selected-facts)
    (multiple-value-bind (fresh next observed facts)
        (logical-slot-continuation-native-preflight-under-lock proof)
      (unless (and fresh
                   (equalp prepared-image (logical-slot-continuation-prepared-encode fresh))
                   (equalp ledger observed) (equal selected-facts facts)
                   (equal selected-facts (funcall operation :receipt))
                   (equal (logical-slot-publication-pending-image
                           (wal-replication-slot-pending-xids candidate))
                          (logical-slot-publication-pending-image
                           (wal-replication-slot-pending-xids next))))
        (error "continuation writer preflight changed before intent")))))

(defun logical-slot-continuation-writer-replace-selector (context)
  ;; Install PREPARED first and the stable new selector after P7N is durable.
  ;; Each replacement scope closes before the next phase begins.
  (with-continuation-writer-context
      (context directory selector-resource selected-image selected-facts
               prepared-image new-image phase flags)
    (let* ((first (eq phase :begun))
           (next (ecase phase (:begun prepared-image) (:p7-durable new-image)))
           (next-phase (if first :prepared-durable :stable-durable)))
      (sb-sys:without-interrupts
        (unwind-protect
             (progn
               (sb-sys:with-local-interrupts
                 (call-with-logical-slot-continuation-file-replacement
                  directory :selector selected-image next
                  (lambda () (logical-slot-continuation-writer-guard context))
                  (lambda (operation)
                    (setf selector-resource operation)
                    (unless (equal selected-facts (funcall operation :receipt))
                      (error "continuation writer selector was substituted before replacement"))
                    (when first
                      (logical-slot-continuation-writer-recheck-before-intent context operation)
                      (logical-slot-continuation-writer-point context :before-intent)
                      (logical-slot-continuation-writer-guard context)
                      (sb-sys:without-interrupts
                        (setf (car flags) t phase :intent-attempted)))
                    (funcall operation :prepare)
                    (logical-slot-continuation-writer-point context
                                                            (if first :prepared-temporary :stable-temporary))
                    (funcall operation :replace)
                    (logical-slot-continuation-writer-point context
                                                            (if first :prepared-selected :stable-selected))
                    (funcall operation :sync)
                    (setf selected-facts (funcall operation :receipt) selected-image next))))
               (setf selector-resource nil phase next-phase))
          (setf selector-resource nil))))
    (logical-slot-continuation-writer-guard context)
    (logical-slot-continuation-writer-point context phase)))

(defun logical-slot-continuation-writer-replace-slot (context)
  ;; Install and sync P7N, including the outer slot root, before advancing.
  (declare (notinline sb-posix:fsync))
  (with-continuation-writer-context
      (context target-directory slot-root slot-resource target-row name
               old-slot-image new-slot-image phase)
    (unless (eq phase :prepared-durable) (error "continuation writer requires durable PREPARED"))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (sb-sys:with-local-interrupts
               (call-with-logical-slot-continuation-file-replacement
                target-directory :slot-state old-slot-image new-slot-image
                (lambda () (logical-slot-continuation-writer-guard context))
                (lambda (operation)
                  (setf slot-resource operation)
                  (unless (and (equal (second target-row) (physical-image-file-facts target-directory))
                               (equal (third target-row) (funcall operation :receipt)))
                    (error "continuation writer target was substituted before replacement"))
                  (funcall operation :prepare)
                  (logical-slot-continuation-writer-point context :p7-temporary)
                  (funcall operation :replace)
                  (logical-slot-continuation-writer-point context :p7-selected)
                  (funcall operation :sync)
                  (setf target-row (list name (copy-list (physical-image-file-facts target-directory))
                                         (funcall operation :receipt) (copy-seq new-slot-image))))))
             (setf slot-resource nil phase :p7-installed))
        (setf slot-resource nil)))
    (logical-slot-continuation-writer-guard context)
    (logical-slot-continuation-writer-point context :before-slot-root-sync)
    (sb-posix:fsync slot-root)
    (logical-slot-continuation-writer-guard context)
    (logical-slot-continuation-writer-check-new-inventory context)
    (setf phase :p7-durable)
    (logical-slot-continuation-writer-point context :p7-durable)))

(defun logical-slot-continuation-writer-publish (context receipt)
  ;; Publish live state only after all replacement scopes have closed and the
  ;; new inventory and candidate have been checked again.
  (with-continuation-writer-context
      (context writer group slot candidate new-slot-image after cell new-image flags phase)
    (unless (and (eq phase :publication-ready)
                 (eq receipt (logical-slot-continuation-writer-context-receipt context))
                 (eq writer (gethash receipt *logical-slot-continuation-writer-receipts*)))
      (error "continuation writer publication is not ready or was consumed"))
    (logical-slot-continuation-writer-point context :before-live-publication)
    (logical-slot-continuation-writer-guard context)
    (logical-slot-continuation-writer-check-new-inventory context)
    (logical-slot-check-update-under-lock slot candidate)
    (let ((row (find (wal-replication-slot-name slot) after :key #'first :test #'string=)))
      (unless (and (equalp new-slot-image (logical-slot-startup-candidate-image candidate))
                   (eq (sixth row) (wal-replication-slot-pending-xids candidate))
                   (equal (seventh row)
                          (logical-slot-publication-pending-image
                           (wal-replication-slot-pending-xids candidate)))
                   (equal (eighth row) (logical-slot-publication-slot-transients candidate)))
        (error "continuation writer candidate changed before publication")))
    (sb-sys:without-interrupts
      (setf (wal-replication-slot-confirmed-flush-lsn slot) (wal-replication-slot-confirmed-flush-lsn candidate)
            (wal-replication-slot-restart-lsn slot) (wal-replication-slot-restart-lsn candidate)
            (wal-replication-slot-confirmed-timeline slot) (wal-replication-slot-confirmed-timeline candidate)
            (wal-replication-slot-restart-timeline slot) (wal-replication-slot-restart-timeline candidate)
            (wal-replication-slot-pending-xids slot) (wal-replication-slot-pending-xids candidate)
            (wal-replication-slot-invalidation-cause slot) (wal-replication-slot-invalidation-cause candidate)
            (wal-replication-slot-wal-status slot) (wal-replication-slot-wal-status candidate)
            (car cell) new-image (cdr cell) nil phase :published (cdr flags) t))
    slot))

(defun logical-slot-continuation-writer-run (context)
  ;; Complete the three durable replacements in order before live publication.
  (with-continuation-writer-context (context writer receipt group phase slot)
    (unless (eq phase :registered) (error "continuation writer was already started"))
    (logical-slot-continuation-protection-call group :begin writer)
    (logical-slot-continuation-writer-guard context)
    (logical-slot-continuation-writer-point context :begun)
    (logical-slot-continuation-writer-replace-selector context)
    (logical-slot-continuation-writer-replace-slot context)
    (logical-slot-continuation-writer-replace-selector context)
    (logical-slot-continuation-writer-check-new-inventory context)
    (logical-slot-continuation-writer-guard context)
    (setf phase :publication-ready
          (gethash receipt *logical-slot-continuation-writer-receipts*) writer)
    (logical-slot-continuation-writer-publish-under-lock writer receipt)
    (logical-slot-continuation-writer-point context :published)
    (logical-slot-continuation-writer-guard context)
    slot))
