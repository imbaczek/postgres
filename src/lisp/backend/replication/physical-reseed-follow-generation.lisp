(in-package #:pg)

;;; Structural validation of saved generation coordinates and source history.
;;; The enclosing journal contains the original attempt's credentials; do not
;;; print journal records.  A later owner must verify evidence and authority
;;; before any source operation.

;; Timeline 1 has empty history; a child needs at least one byte.  A valid
;; parsed prefix need not list every ancestor, so the owner must compare the
;; complete saved history with the actual source history.

(defun physical-reseed-follow-generation-history (coordinates text)
  (unless (and (stringp text) (<= (length text) +physical-store-history-maximum-bytes+)
               (<= (if (= 1 (getf coordinates :source-timeline)) 0 1)
                   (length (string->bytes text)) +physical-store-history-maximum-bytes+)
               (or (/= 1 (getf coordinates :source-timeline)) (string= text "")))
    (error "invalid target follow source history"))
  (let ((history (timeline-check-lineage
                  (timeline-parse-history (getf coordinates :source-timeline) text))))
    (unless (and (timeline-prefix-boundary-p
                  (getf coordinates :base-timeline) (getf coordinates :base) history)
                 (timeline-prefix-boundary-p
                  (getf coordinates :receive-timeline) (getf coordinates :receive-lsn) history))
      (error "target follow coordinates are outside saved source history")))
  (copy-seq text))

;; Reattach the shared history to the kind-specific proof and return a detached
;; anchor.  Its request coordinates must match this generation exactly.

(defun physical-reseed-follow-generation-anchor (record)
  (physical-reseed-install-check-literal record)
  (when (getf record :anchor)
    (let* ((coordinates (physical-follow-request-copy (getf record :coordinates)))
           (kind (physical-follow-request-kind coordinates))
           (keys (if (eq kind :descendant) *physical-follow-anchor-keys*
                     *physical-follow-same-timeline-anchor-keys*))
           (history (physical-reseed-follow-generation-history
                     coordinates (getf record :source-history)))
           (anchor (physical-follow-anchor-copy
                    (physical-adoption-attempt-with-history (getf record :anchor) keys history))))
      (unless (physical-follow-request-matches-anchor-p coordinates anchor)
        (error "target follow proof differs from saved coordinates"))
      anchor)))

(defun physical-reseed-follow-generation-copy (record request)
  (physical-reseed-install-check-literal (list record request))
  (physical-follow-check-plist record '(:generation :name :coordinates :source-history :phase :anchor))
  (unless (and (member (getf record :phase) '(:preparing :proved :canceling :canceled))
               (case (getf record :phase)
                 (:preparing (null (getf record :anchor)))
                 (:proved (not (null (getf record :anchor))))
                 (otherwise t)))
    (error "invalid target follow generation phase or proof"))
  (let* ((allocation (physical-backup-lease-follow-intent
                      request (getf record :generation) (getf record :name)
                      (getf record :coordinates) :live))
         (coordinates (getf allocation :coordinates))
         (history (physical-reseed-follow-generation-history coordinates (getf record :source-history)))
         (anchor (physical-reseed-follow-generation-anchor record)))
    (list :generation (getf allocation :generation) :name (getf allocation :name)
          :coordinates coordinates :source-history history :phase (getf record :phase)
          :anchor (when anchor
                    (physical-adoption-attempt-fields-copy
                     anchor (remove :history-text
                                    (if (eq :descendant (physical-follow-anchor-kind anchor))
                                        *physical-follow-anchor-keys*
                                        *physical-follow-same-timeline-anchor-keys*)))))))

;; Identity, coordinates and history never change.  Only the forward phase
;; transitions are allowed; once captured, a proof is retained through cancel.

(defun physical-reseed-follow-generation-check-transition (previous candidate request)
  (let* ((before (physical-reseed-follow-generation-copy previous request))
         (after (physical-reseed-follow-generation-copy candidate request))
         (old (getf before :phase)) (new (getf after :phase)))
    (unless (and (every (lambda (key) (equal (getf before key) (getf after key)))
                        '(:generation :name :coordinates :source-history))
                 (or (eq old new)
                     (member (list old new)
                             '((:preparing :proved) (:preparing :canceling)
                               (:proved :canceling) (:canceling :canceled))
                             :test #'equal))
                 (or (and (eq old :preparing) (eq new :proved))
                     (equal (getf before :anchor) (getf after :anchor))))
      (error "target follow generation cannot rebind, reverse or lose its proof"))
    t))
