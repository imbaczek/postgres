(in-package #:pg)

(defun logical-slot-continuation-cold-plan-facts-key (facts root directory-p)
  (logical-slot-continuation-list facts 9)
  (unless (and (every #'integerp facts)
               (typep (third facts) '(unsigned-byte 16))
               (typep (fourth facts) '(unsigned-byte 32))
               (typep (fifth facts) '(unsigned-byte 32))
               (typep (sixth facts) '(unsigned-byte 64))
               (every (lambda (value) (typep value '(unsigned-byte 64)))
                      (list (first facts) (second facts) (ninth facts)))
               (= (first facts) (first root)) (= (ninth facts) (ninth root))
               (= (fifth facts) (fifth root)) (plusp (fourth facts))
               (not (minusp (sixth facts)))
               (if directory-p
                   (and (sb-posix:s-isdir (third facts)) (= #o700 (logand #o7777 (third facts))))
                   (and (sb-posix:s-isreg (third facts)) (= #o600 (logand #o7777 (third facts)))
                        (= 1 (fourth facts)))))
    (error "cold cleanup resource is not private on its observed root"))
  (list (first facts) (second facts) (ninth facts)))

;; Check the complete captured inventory before classifying it.  Require private
;; resources on the observed root, distinct identities, bounded namespaces and
;; exact file sizes; a name or inode must not be claimed twice.
(defun logical-slot-continuation-cold-check-plan-ledger (rows root selector temporary)
  (logical-slot-continuation-list root 9)
  (logical-slot-copy-descriptor-list-count rows (* 2 +logical-slot-continuation-obligation-limit+))
  (let ((resources (make-hash-table :test #'equal)) (names (make-hash-table :test #'equal))
                                                    (canonical 0) (staging 0))
    (labels ((claim (facts directory-p)
               (let ((key (logical-slot-continuation-cold-plan-facts-key facts root directory-p)))
                 (when (gethash key resources) (error "cold cleanup resources alias one another"))
                 (setf (gethash key resources) t)))
             (file (row maximum)
               (logical-slot-continuation-list row 3)
               (claim (second row) nil)
               (logical-slot-copy-descriptor-blob-size (third row) 0 maximum)
               (unless (= (length (third row)) (sixth (second row)))
                 (error "cold cleanup bytes differ from their captured file size"))))
      (claim root t)
      (loop for ledger in (list selector temporary) for name in '("selector" "selector.tmp")
            when ledger do
              (file ledger (logical-slot-continuation-prepared-maximum-bytes))
              (unless (equal name (first ledger)) (error "cold cleanup selector ledger has another name")))
      (dolist (row rows)
        (logical-slot-continuation-list row 10)
        (logical-slot-continuation-plist row '(:name :base :staging :directory :files))
        (multiple-value-bind (base stage)
            (logical-slot-continuation-cold-entry-base (getf row :name))
          (unless (and (equal base (getf row :base)) (eq stage (getf row :staging))
                       (not (gethash (getf row :name) names)))
            (error "cold cleanup row has a repeated or inconsistent name"))
          (setf (gethash (getf row :name) names) t)
          (if stage (incf staging) (incf canonical)))
        (claim (getf row :directory) t)
        (logical-slot-copy-descriptor-list-count (getf row :files) 2)
        (let ((files (mapcar #'first (getf row :files))))
          (unless (and (every (lambda (name) (member name '("state" "state.tmp") :test #'equal)) files)
                       (equal files (sort (remove-duplicates (copy-list files) :test #'equal) #'string<)))
            (error "cold cleanup row has unexpected or repeated files")))
        (dolist (entry (getf row :files)) (file entry +slot-state-maximum-bytes+)))
      (unless (and (<= canonical +logical-slot-continuation-obligation-limit+)
                   (<= staging +logical-slot-continuation-obligation-limit+))
        (error "cold cleanup inventory exceeds its independent namespace bounds"))))
  t)

;; Bind the decoded selection back to the captured selector bytes and validate
;; its child state before using it to classify the inventory.
(defun logical-slot-continuation-cold-check-plan-selection (kind model selector binding certificate host)
  (ecase kind
    (:absent (when (or model selector) (error "absent selector carries a selected image")))
    ((:stable :prepared)
     (unless selector (error "cold cleanup selection has no captured canonical selector"))
     (multiple-value-bind (decoded-kind decoded)
         (logical-slot-continuation-cold-decode-selected (third selector))
       (unless (and (eq decoded-kind kind) (equalp decoded model))
         (error "cold cleanup selection differs from its captured image")))
     (let ((snapshot (if (eq kind :prepared) (getf model :old) model)))
       (logical-slot-continuation-cold-check-child-selection
        kind model (getf snapshot :slots) binding certificate host))))
  t)

;; Certificate names remain protected even when absent.  A present child needs
;; canonical state; only the PREPARED target may have an exact new-state prefix.
(defun logical-slot-continuation-cold-plan-children (rows domain kind model)
  (let ((children nil))
    (dolist (row rows)
      (when (assoc (getf row :base) domain :test #'string=)
        (when (getf row :staging) (error "cold cleanup staging name is protected by its certificate"))
        (let* ((name (getf row :name)) (files (getf row :files))
                                       (state (assoc "state" files :test #'string=))
                                       (temporary (assoc "state.tmp" files :test #'string=)))
          (unless state (error "certified child has no canonical state"))
          (when temporary
            (unless (and (eq kind :prepared) (equal name (getf model :name)))
              (error "certified child has unowned temporary state"))
            (logical-slot-continuation-cold-check-prefix
             (third temporary)
             (second (assoc name (getf (getf model :new) :slots) :test #'string=))))
          (push (list name (third state)) children))))
    (nreverse children)))

;; An ordinary or staged name cannot hide a child image or reuse its logical
;; provenance, even when its pathname is outside the certified domain.
(defun logical-slot-continuation-cold-plan-reject-child-alias (bytes domain)
  (when (logical-slot-state-image-p bytes)
    (let ((slot (handler-case-noninterrupt
                 (let ((name (second (wal-split-fields (bytes->string bytes)))))
                   (logical-slot-state-decode bytes name))
                 (condition nil))))
      (when slot
        (let ((provenance (wal-replication-slot-logical-provenance slot)))
          (when (or (wal-replication-slot-child-owner slot)
                    (and provenance
                         (some (lambda (entry)
                                 (let ((child (wal-replication-slot-logical-provenance (second entry))))
                                   (and (= (logical-slot-provenance-system-id provenance)
                                           (logical-slot-provenance-system-id child))
                                        (= (logical-slot-provenance-generation provenance)
                                           (logical-slot-provenance-generation child)))))
                               domain)))
            (error "cold cleanup ordinary or staged bytes carry a child identity"))))))
  t)

(defun logical-slot-continuation-cold-plan-action (operation row &optional retained)
  (list :operation operation :name (getf row :name) :before row
        :after (if retained
                   (list :name (getf row :name)
                         :directory-identity
                         (logical-slot-copy-source-proof-directory-identity (getf row :directory))
                         :files (list retained))
                   :absent)))

;; Validate a temporary physical image before retirement, without requiring
;; local system ID, history or WAL eligibility, as in RestoreSlotFromDisk.

;; Plan staging removal, scratch removal and temporary physical slot retirement
;; in that order.  Keep the complete state of other ordinary slots.  PostgreSQL
;; slot.c startup removes staging directories and RestoreSlotFromDisk removes
;; state.tmp; this planner records actions and does not perform those operations.
(defun logical-slot-continuation-cold-plan-ordinary (rows domain)
  (let ((staging nil) (scratch nil) (temporary nil) (retained nil))
    (dolist (row (sort (copy-list rows) #'string< :key (lambda (row) (getf row :name))))
      (unless (assoc (getf row :base) domain :test #'string=)
        (dolist (file (getf row :files))
          (logical-slot-continuation-cold-plan-reject-child-alias (third file) domain))
        (if (getf row :staging)
            (push (logical-slot-continuation-cold-plan-action :remove-staging row) staging)
            (let* ((name (getf row :name)) (files (getf row :files))
                                           (state (assoc "state" files :test #'string=)))
              (unless state (error "ordinary canonical slot has no complete state"))
              (let ((slot (wal-decode-replication-slot-image (third state) name)))
                (if (and (wal-physical-slot-p slot) (wal-replication-slot-temporary-p slot))
                    (progn
                      (unless (equalp (third state) (physical-slot-encode slot))
                        (error "cold cleanup temporary physical image is not canonical"))
                      (push (logical-slot-continuation-cold-plan-action :retire-temporary row) temporary))
                    (progn
                      (push (list name (third state)) retained)
                      (when (assoc "state.tmp" files :test #'string=)
                        (push (logical-slot-continuation-cold-plan-action :remove-scratch row state) scratch)))))))))
    (values (append (nreverse staging) (nreverse scratch) (nreverse temporary)) (nreverse retained))))

;; Construct a copied plan only after the selector, certificate and complete
;; inventory agree.  An absent selector's temporary must prefix the derived
;; bootstrap image.  The plan itself grants no cleanup or publication authority.
(defun logical-slot-continuation-cold-build-cleanup-plan
    (kind model selector temporary rows root binding certificate host)
  (let ((domain (logical-slot-continuation-cold-certified-domain binding certificate host)))
    (logical-slot-continuation-cold-check-plan-ledger rows root selector temporary)
    (logical-slot-continuation-cold-check-plan-selection kind model selector binding certificate host)
    (let ((children (logical-slot-continuation-cold-plan-children rows domain kind model)))
      (multiple-value-bind (snapshot choice)
          (logical-slot-continuation-cold-check-child-selection
           kind model children binding certificate host)
        (when (and (eq kind :absent) temporary)
          (logical-slot-continuation-cold-check-prefix
           (third temporary) (logical-slot-continuation-encode snapshot)))
        (multiple-value-bind (actions ordinary)
            (logical-slot-continuation-cold-plan-ordinary rows domain)
          (logical-slot-continuation-copy
           (list :kind kind :choice choice :snapshot snapshot :selector selector :temporary temporary
                 :root root :ledger rows :children children :protected (mapcar #'first domain)
                 :retained (sort (append (copy-list children) ordinary) #'string< :key #'first)
                 :actions actions)))))))
