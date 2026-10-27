;;;; Check the immutable inventory named by receive control before tail cleanup.
;;;; The verified base identity comes from recovery, not from receive control.
;;;; Full coverage checks do not restore deletion ownership or select a new root.

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
  (let ((key (namestring (physical-retained-root-directory (physical-store-directory store)))))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (or (gethash key *physical-store-retained-owners*)
          (setf (gethash key *physical-store-retained-owners*) (gensym "RECEIVE-CONTROL"))))))

(defun physical-store-retained-history (store)
  (let* ((selection (physical-store-selection store))
         (target (physical-store-selected-timeline store))
         (text (if selection (getf selection :history-text)
                   (if (= target 1) "" (bytes->string (timeline-history-bytes target))))))
    (values (timeline-check-lineage (timeline-parse-history target text)) target text)))

(defun physical-store-control-tuple (store &key
                                             (floor (physical-store-floor-lsn store))
                                             (floor-digest (physical-store-floor-digest store))
                                             (root (physical-store-retained-root store)))
  (list (physical-store-system-id store) (physical-store-timeline store)
        (physical-store-base-lsn store) (physical-store-flushed-lsn store)
        (physical-store-flushed-digest store) floor floor-digest
        (getf (physical-store-selection store) :target)
        (getf (physical-store-selection store) :digest)
        (getf (physical-store-retained-identity store) :manifest-digest) root))

(defun physical-store-read-retained-control (store expected)
  (let ((probe (%make-physical-store :directory (physical-store-directory store)
                                     :system-id (physical-store-system-id store)
                                     :timeline (physical-store-timeline store)
                                     :retained-identity (physical-store-retained-identity store)
                                     :retained-directory-value (physical-store-retained-directory store))))
    (physical-store-load-control probe)
    (unless (equal expected (physical-store-control-tuple probe))
      (error "receive control readback differs from its complete expected tuple"))
    (physical-store-retained-root probe)))

(defun physical-store-restore-retained-reference (store)
  (when (and (physical-store-retained-identity store)
             (uiop:directory-exists-p (physical-store-retained-directory store)))
    (physical-retained-root-reconcile
     (physical-store-retained-owner store) (physical-store-retained-directory store)
     (physical-store-retained-identity store) (physical-store-retained-history store)
     (lambda () (physical-store-read-retained-control store (physical-store-control-tuple store))))))

(defun physical-store-check-retained-root (store)
  "Validate the referenced inventory and every image against the selected history.

Require its coverage to reach the receive floor with the recorded digest. This
checks the reference; it does not restore deletion ownership or select a new root."
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
