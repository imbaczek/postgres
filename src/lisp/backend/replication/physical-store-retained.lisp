;;;; Check the immutable inventory named by receive control before tail cleanup.
;;;; The verified base identity comes from recovery, not from receive control.
;;;; Full coverage checks do not restore deletion ownership or select a new root.

;;;; Reopen restores local ownership after those checks, before tail cleanup.
;;;; Control I/O runs under store ownership and outside the archive mutex.

(in-package #:pg)

(defun physical-store-check-retained-identity (store)
  "Validate and copy the supplied immutable identity against STORE's base."
  (let ((identity (physical-retained-root-copy-identity (physical-store-retained-identity store))))
    (unless (and (= (getf identity :system-id) (physical-store-system-id store))
                 (= (getf identity :base-timeline) (physical-store-timeline store))
                 (= (getf identity :base) (physical-store-base-lsn store)))
      (error "receive inventory identity differs from its store"))
    (setf (physical-store-retained-identity store) identity)))

(defun physical-store-retained-directory (store)
  "Return the directory for STORE's retained inventory objects."
  (or (physical-store-retained-directory-value store)
      (physical-store-path store "retained/")))

(defun physical-store-check-retained-coverage (store entries)
  "Require a contiguous digest chain from the base and fixed seed to the floor."
  (let ((cursor (physical-store-base-lsn store)) (digest #xCBF29CE484222325))
    (dolist (entry entries)
      (let ((metadata (third entry)))
        (unless (and (= cursor (getf metadata :start))
                     (= digest (getf metadata :start-digest)))
          (error "receive retained inventory has a hole or digest discontinuity"))
        (setf cursor (getf metadata :end) digest (getf metadata :end-digest))))
    (unless (and (= cursor (physical-store-floor-lsn store))
                 (= digest (physical-store-floor-digest store)))
      (error "receive floor differs from its retained inventory endpoint"))))

(defvar *physical-store-retained-owners* (make-hash-table :test #'equal))

(defun physical-store-retained-owner (store)
  "Return the stable local control owner for the canonical receive directory."
  (let ((key (namestring (physical-retained-root-directory (physical-store-directory store)))))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (or (gethash key *physical-store-retained-owners*)
          (setf (gethash key *physical-store-retained-owners*) (gensym "RECEIVE-CONTROL"))))))

(defun physical-store-retained-history (store)
  "Return the parsed selected history, its target and its text."
  (let* ((selection (physical-store-selection store))
         (target (physical-store-selected-timeline store))
         (text (if selection (getf selection :history-text)
                   (if (= target 1) "" (bytes->string (timeline-history-bytes target))))))
    (values (timeline-check-lineage (timeline-parse-history target text)) target text)))

(defstruct (physical-receive-control
            (:constructor physical-receive-control-from-fields
                (system-id timeline base flush flush-digest floor floor-digest
                 history-target history-digest manifest-digest root)))
  "Immutable receive-control snapshot. Convert to a list only at witness boundaries."
  (system-id nil :read-only t)
  (timeline nil :read-only t)
  (base nil :read-only t)
  (flush nil :read-only t)
  (flush-digest nil :read-only t)
  (floor nil :read-only t)
  (floor-digest nil :read-only t)
  (history-target nil :read-only t)
  (history-digest nil :read-only t)
  (manifest-digest nil :read-only t)
  (root nil :read-only t))

(defun physical-store-control-snapshot (store &key
                                                (floor (physical-store-floor-lsn store))
                                                (floor-digest (physical-store-floor-digest store))
                                                (root (physical-store-retained-root store)))
  "Capture all fields used to check receive-control readback."
  (physical-receive-control-from-fields
   (physical-store-system-id store) (physical-store-timeline store)
   (physical-store-base-lsn store) (physical-store-flushed-lsn store)
   (physical-store-flushed-digest store) floor floor-digest
   (getf (physical-store-selection store) :target)
   (getf (physical-store-selection store) :digest)
   (getf (physical-store-retained-identity store) :manifest-digest) root))

(defun physical-receive-control-to-tuple (control)
  "Serialize CONTROL in the persisted receive witness field order."
  (list (physical-receive-control-system-id control) (physical-receive-control-timeline control)
        (physical-receive-control-base control) (physical-receive-control-flush control)
        (physical-receive-control-flush-digest control) (physical-receive-control-floor control)
        (physical-receive-control-floor-digest control) (physical-receive-control-history-target control)
        (physical-receive-control-history-digest control) (physical-receive-control-manifest-digest control)
        (physical-receive-control-root control)))

(defun physical-receive-control-same-identity-p (left right)
  (and (eql (physical-receive-control-system-id left) (physical-receive-control-system-id right))
       (eql (physical-receive-control-timeline left) (physical-receive-control-timeline right))
       (eql (physical-receive-control-base left) (physical-receive-control-base right))
       (eql (physical-receive-control-history-target left) (physical-receive-control-history-target right))
       (eql (physical-receive-control-history-digest left) (physical-receive-control-history-digest right))
       (eql (physical-receive-control-manifest-digest left) (physical-receive-control-manifest-digest right))))

(defun physical-receive-control-same-progress-p (left right)
  (and (eql (physical-receive-control-flush left) (physical-receive-control-flush right))
       (eql (physical-receive-control-flush-digest left) (physical-receive-control-flush-digest right))
       (eql (physical-receive-control-floor left) (physical-receive-control-floor right))
       (eql (physical-receive-control-floor-digest left) (physical-receive-control-floor-digest right))
       (eql (physical-receive-control-root left) (physical-receive-control-root right))))

(defun physical-receive-control-equal-p (left right)
  (and (physical-receive-control-same-identity-p left right)
       (physical-receive-control-same-progress-p left right)))

(defun physical-store-control-tuple (store &rest options)
  "Build the persisted receive witness list for STORE."
  (physical-receive-control-to-tuple (apply #'physical-store-control-snapshot store options)))

(defun physical-receive-control-matches-p (snapshot expected)
  "Compare SNAPSHOT with an in-memory snapshot or a persisted witness list."
  (if (physical-receive-control-p expected)
      (physical-receive-control-equal-p snapshot expected)
      (equal (physical-receive-control-to-tuple snapshot) expected)))

(defun physical-store-read-retained-control (store expected)
  "Read control under the store owner and check every field in EXPECTED.
EXPECTED can be a snapshot or a persisted witness list.

The root update calls this outside the archive mutex."
  (let ((probe (%make-physical-store :directory (physical-store-directory store)
                                     :system-id (physical-store-system-id store)
                                     :timeline (physical-store-timeline store)
                                     :retained-identity (physical-store-retained-identity store)
                                     :retained-directory-value (physical-store-retained-directory store))))
    (physical-store-load-control probe)
    (let ((current (physical-store-control-snapshot probe)))
      (unless (physical-receive-control-matches-p current expected)
        (error "receive control readback differs from its complete expected tuple")))
    (values (physical-store-retained-root probe) (physical-store-selection probe))))

(defun physical-store-restore-retained-reference (store)
  "Restore or reconcile local root ownership from the store's durable control."
  (when (and (physical-store-retained-identity store)
             (uiop:directory-exists-p (physical-store-retained-directory store)))
    (physical-retained-root-reconcile
     (physical-store-retained-owner store) (physical-store-retained-directory store)
     (physical-store-retained-identity store) (physical-store-retained-history store)
     (lambda () (physical-store-read-retained-control store (physical-store-control-snapshot store))))))

(defun physical-store-check-retained-root (store)
  "Validate the referenced inventory and every image against the selected history.

Require coverage from the base to the receive floor with the recorded digest,
then restore or reconcile the process-local reference. This preserves the root selected by durable control."
  (when (physical-store-retained-root store)
    (physical-store-check-retained-identity store)
    (let* ((history (or (getf (physical-store-selection store) :history)
                        (timeline-read-history (physical-store-timeline store))))
           (index (physical-retained-inventory-load
                   (physical-store-retained-directory store) (physical-store-retained-root store)
                   (physical-store-retained-identity store) history)))
      (physical-store-check-retained-coverage store (getf index :entries))))
  (physical-store-restore-retained-reference store)
  t)
