(in-package #:pg)

;;; Local follow store checks.  Public intent publication accepts only empty
;;; genesis and PREPARING; preparation publishes PROVED after source and local
;;; checks.  A historical record does not grant current source authority.

(defvar *physical-reseed-follow-write-limit* nil)

(defun physical-reseed-follow-intent-copy (record &optional previous)
  (let* ((copy (physical-reseed-follow-copy record))
         (generations (getf copy :generations)))
    (unless (and (null (getf copy :cancellation)) (<= (length generations) 1)
                 (every (lambda (row) (eq :preparing (getf row :phase))) generations))
      (error "target follow intent publication cannot certify a remote outcome"))
    (when previous
      (physical-reseed-follow-intent-copy previous)
      (physical-reseed-follow-check-transition previous copy))
    copy))

;; Check the immutable owner binding and prove the proposed ancestry and WAL
;; overlap against the installed base.  This is local evidence, not an RPC or
;; proof that a source has accepted the intent.

(defun physical-reseed-follow-check-base-intent (record base directory)
  (let* ((generation (first (getf record :generations)))
         (coordinates (getf generation :coordinates))
         (history (timeline-check-lineage
                   (timeline-parse-history (getf coordinates :source-timeline)
                                           (getf generation :source-history))))
         (previous (timeline-check-lineage (timeline-read-history (getf base :timeline) directory))))
    (unless (physical-store-history-extends-p previous history)
      (error "target follow intent changes installed timeline ancestry"))
    (multiple-value-bind (bytes entry) (physical-recovery-read-base-overlap base directory)
      (unless (and (= (getf coordinates :overlap-start) (- (getf base :end) (length bytes)))
                   (= (getf coordinates :overlap-digest) (third entry)))
        (error "target follow intent differs from its installed WAL overlap"))))
  t)

(defun physical-reseed-follow-check-local-intent (owner root-fd record)
  (physical-reseed-follow-check-binding owner record)
  (let ((generation (first (getf record :generations))) (base nil) (control nil))
    (when generation
      (call-with-physical-backup-image-directory
       root-fd
       (lambda (directory)
         (let ((*pgdata* directory))
           (multiple-value-bind (verified raw) (physical-recovery-verify-base-state)
             (setf base verified control raw))
           (physical-reseed-follow-check-base-intent record base directory)))))
    (values record base control)))

(defun physical-reseed-follow-snapshot-at (root-fd)
  (multiple-value-bind (identity row) (physical-reseed-follow-entry root-fd "physical-reseed.follow")
    (when identity
      (multiple-value-bind (record actual) (physical-reseed-follow-read-at root-fd "physical-reseed.follow")
        (unless (and (equal identity actual)
                     (equal row (nth-value 1 (physical-reseed-follow-entry root-fd "physical-reseed.follow"))))
          (error "target follow final changed while capturing its generation"))
        (list :record record :identity identity :row row)))))

(defun physical-reseed-follow-check-snapshot (root-fd expected)
  (unless (equal expected (physical-reseed-follow-snapshot-at root-fd))
    (error "target follow final generation changed"))
  expected)

(defun physical-reseed-follow-check-final-metadata (root-fd expected)
  (multiple-value-bind (identity row) (physical-reseed-follow-entry root-fd "physical-reseed.follow")
    (unless (and (equal identity (getf expected :identity)) (equal row (getf expected :row)))
      (error "target follow final metadata changed after image verification")))
  (when (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t)
    (error "target follow temporary remains after publication"))
  t)

;; Stable history can be inspected after an abort, but a mutation requires the
;; actual current attempt to remain active.
(defun physical-reseed-follow-check-active-attempt (owner)
  (unless (eq :premanifest (getf (physical-reseed-cleanup-owner-attempt owner) :phase))
    (error "target follow intent cannot advance an aborting attempt"))
  owner)

(defun physical-reseed-follow-store-fd-check (root-fd fd name fixed)
  (let ((facts (physical-reseed-follow-file-identity (sb-posix:fstat fd) t)))
    (unless (and (equal fixed (subseq facts 0 5))
                 (equal facts (physical-reseed-follow-entry root-fd name t))
                 (= (physical-adoption-install-mount-id nil :directory-fd root-fd)
                    (physical-adoption-install-mount-id nil :directory-fd fd)))
      (error "target follow temporary descriptor changed or lost its name"))
    facts))

;; Capture both the actual attempt record and its file facts, not just the
;; immutable attempt saved in the install plan.

(defun physical-reseed-follow-attempt-snapshot-at (root-fd)
  (let ((mount (physical-adoption-install-mount-id nil :directory-fd root-fd)))
    (list :record (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt")
          :row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount))))

(defun physical-reseed-follow-check-attempt-snapshot-at (root-fd expected)
  (unless (equal expected (physical-reseed-follow-attempt-snapshot-at root-fd))
    (error "target follow original attempt changed during publication"))
  expected)

;; Block public reentry throughout the store operation.  Recheck the exact
;; attempt after the image post-check and final metadata check, before clearing
;; uncertainty.  An incomplete mutation requires closing and reopening the owner.

(defun call-with-physical-reseed-follow-held-store (owner function)
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "target follow held store requires an active operation"))
  (let ((result
          (call-with-physical-directory-root
           owner
           (lambda (root-fd directory)
             (declare (ignore directory))
             (let ((attempt (physical-reseed-follow-attempt-snapshot-at root-fd)))
               (flet ((attempt-check ()
                        (physical-reseed-follow-check-attempt-snapshot-at root-fd attempt)))
                 (multiple-value-bind (record snapshot)
                     (call-with-physical-reseed-follow-image
                      owner
                      (lambda (image-fd image-check)
                        (flet ((check () (funcall image-check) (attempt-check)))
                          (check)
                          (funcall function image-fd #'check))))
                   (attempt-check)
                   (physical-reseed-follow-check-final-metadata root-fd snapshot)
                   (attempt-check)
                   record)))))))
    result))

;; The busy flag also covers mapped installation operations.  An incomplete
;; mutation remains uncertain even though the busy flag is cleared on exit.
(defun call-with-physical-reseed-follow-store (owner function)
  (physical-reseed-follow-check-idle owner)
  (setf (physical-reseed-follow-owner-busy-p owner) t)
  (unwind-protect
       (multiple-value-prog1
           (call-with-physical-reseed-follow-held-store
            owner
            (lambda (root-fd check)
              (flet ((forward-check ()
                       (physical-reseed-follow-check-forward-record
                        (getf (physical-reseed-follow-snapshot-at root-fd) :record))
                       (funcall check)))
                (forward-check)
                (multiple-value-prog1 (funcall function root-fd #'forward-check)
                  (forward-check)))))
         (setf (physical-reseed-follow-owner-uncertain-p owner) nil))
    (setf (physical-reseed-follow-owner-busy-p owner) nil)))

;; Pin the exact current attempt and its file facts around the full image and
;; namespace checks.  Publication also requires the attempt to remain active.

(defun call-with-physical-reseed-follow-attempt-proof (owner function)
  (call-with-physical-reseed-follow-store owner function))

(defun call-with-physical-reseed-follow-store-proof (owner function)
  (call-with-physical-reseed-follow-attempt-proof
   owner
   (lambda (root-fd attempt-check)
     (flet ((check ()
              (funcall attempt-check)
              (physical-reseed-follow-check-active-attempt owner)))
       (check)
       (funcall function root-fd #'check)))))
