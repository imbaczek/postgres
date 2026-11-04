(in-package #:pg)

(defvar *physical-reseed-follow-write-limit* nil)

(defun physical-reseed-follow-intent-copy (record &optional previous)
  (let* ((copy (physical-reseed-follow-copy record))
         (generations (getf copy :generations)))
    (unless (and (<= (length generations) 1)
                 (every (lambda (row) (eq :preparing (getf row :phase))) generations))
      (error "target follow intent publication cannot certify a remote outcome"))
    (when previous
      (physical-reseed-follow-intent-copy previous)
      (physical-reseed-follow-check-transition previous copy))
    copy))

(defun physical-reseed-follow-check-local-intent (owner root-fd record)
  (physical-reseed-follow-check-binding owner record)
  (let ((generation (first (getf record :generations))))
    (when generation
      (call-with-physical-backup-image-directory
       root-fd
       (lambda (directory)
         (let* ((*pgdata* directory)
                (base (physical-recovery-verify-base-state))
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
               (error "target follow intent differs from its installed WAL overlap"))))))))
  record)

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

(defun call-with-physical-reseed-follow-store (owner function)
  (physical-reseed-follow-check-idle owner)
  (setf (physical-reseed-follow-owner-busy-p owner) t)
  (unwind-protect
       (let ((result
               (call-with-physical-directory-root
                owner
                (lambda (root-fd directory)
                  (declare (ignore directory))
                  (multiple-value-bind (record snapshot)
                      (call-with-physical-reseed-follow-image owner function)
                    (physical-reseed-follow-check-final-metadata root-fd snapshot)
                    record)))))
         (setf (physical-reseed-follow-owner-uncertain-p owner) nil)
         result)
    (setf (physical-reseed-follow-owner-busy-p owner) nil)))

(defun call-with-physical-reseed-follow-store-proof (owner function)
  (call-with-physical-reseed-follow-store
   owner
   (lambda (root-fd image-check)
     (let* ((mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
            (attempt (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt"))
            (row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount)))
       (flet ((check ()
                (funcall image-check)
                (physical-reseed-follow-check-active-attempt owner)
                (unless (and (equal attempt (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt"))
                             (equal row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount)))
                  (error "target follow original attempt changed during publication"))))
         (check)
         (funcall function root-fd #'check))))))
