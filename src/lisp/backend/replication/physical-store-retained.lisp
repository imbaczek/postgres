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
  (physical-store-path store "retained/"))

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
  t)
