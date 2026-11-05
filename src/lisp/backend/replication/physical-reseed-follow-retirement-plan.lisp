(in-package #:pg)

(defun physical-reseed-follow-retirement-check-prefix (value prefix)
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation))
         (retirement (getf record :retirement))
         (phase (getf retirement :phase))
         (step (getf retirement :step)))
    (unless (and (eq :active (getf activation :phase)) (= 2 (getf activation :step))
                 (typep prefix '(integer 0 2))
                 (case phase
                   ((nil) (zerop prefix))
                   (:retiring (or (= prefix step) (= prefix (min 2 (1+ step)))))
                   (:retired (= prefix 2))))
      (error "observed retirement prefix disagrees with recorded progress"))
    prefix))

(defun physical-reseed-follow-retirement-plan-record (value binding)
  (let* ((record (physical-reseed-follow-activation-plan-record value binding))
         (activation (getf record :activation)))
    (unless (and (eq :active (getf activation :phase)) (= 2 (getf activation :step)))
      (error "retirement plan requires completed activation"))
    record))

(defun physical-reseed-follow-retirement-moves (value binding)
  (physical-reseed-follow-retirement-plan-record value binding)
  (physical-adoption-attempt-value-copy
   '((:install-journal :root "physical-reseed.install" :saved "physical-reseed.install")
     (:attempt-journal :root "physical-reseed.attempt" :saved "physical-reseed.attempt"))))

(defun physical-reseed-follow-retirement-positions (value binding prefix)
  (let ((record (physical-reseed-follow-retirement-plan-record value binding)))
    (unless (typep prefix '(integer 0 2)) (error "invalid retirement move prefix"))
    (physical-reseed-follow-apply-position-moves
     (append (physical-reseed-follow-activation-positions record binding 2)
             (list (list :install-journal :root (copy-seq "physical-reseed.install"))
                   (list :attempt-journal :root (copy-seq "physical-reseed.attempt"))))
     (subseq (physical-reseed-follow-retirement-moves record binding) 0 prefix)
     "retirement move lacks a unique source or empty destination")))

(defun physical-reseed-follow-retirement-inventories (value-plan value binding prefix extra)
  (let ((record (physical-reseed-follow-retirement-plan-record value binding)))
    (unless (typep prefix '(integer 0 2)) (error "invalid retirement inventory prefix"))
    (physical-reseed-follow-configuration-sort-inventories
     (physical-reseed-follow-apply-inventory-moves
      (physical-reseed-follow-activation-inventories value-plan record binding 2 extra)
      (subseq (physical-reseed-follow-retirement-moves record binding) 0 prefix)
      "retirement inventory move lacks a unique source or empty destination"))))
