;;; Capture and recheck local follow, attempt and install evidence around network
;;; work while the cancellation owner is busy.

(in-package #:pg)

(defun physical-reseed-follow-cancellation-snapshot-at (root-fd)
  (let ((mount (physical-adoption-install-mount-id nil :directory-fd root-fd)))
    (physical-adoption-attempt-value-copy
     (list :follow (physical-reseed-follow-snapshot-at root-fd)
           :attempt (physical-reseed-follow-attempt-snapshot-at root-fd)
           :install (list :record (physical-reseed-install-read-at root-fd "physical-reseed.install")
                          :row (physical-reseed-install-old-observe root-fd "physical-reseed.install" mount))))))

;; Require a stable cancellation and no temporary before network work.  Check
;; the same local evidence again after releasing the image resources.

(defun physical-reseed-follow-cancellation-network-snapshot (owner &optional expected)
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "cancellation network observation requires an active owner operation"))
  (call-with-physical-directory-root
   owner
   (lambda (root-fd directory)
     (declare (ignore directory))
     (let ((snapshot
             (call-with-physical-reseed-follow-image
              owner
              (lambda (fd check)
                (physical-adoption-require-absent
                 (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path fd))
                 "cancellation temporary requires recovery before network work")
                (let* ((current (physical-reseed-follow-cancellation-snapshot-at fd))
                       (record (getf (getf current :follow) :record)))
                  (unless (getf record :cancellation)
                    (error "remote cleanup requires durable cancellation intent"))
                  (physical-reseed-follow-cancellation-local-proof owner fd record)
                  (physical-reseed-follow-read-at fd "physical-reseed.follow" :stabilize t)
                  (when (and expected (not (equal expected current)))
                    (error "cancellation predecessor changed during network work"))
                  (funcall check)
                  (unless (equal current (physical-reseed-follow-cancellation-snapshot-at fd))
                    (error "cancellation evidence changed during local verification"))
                  current)))))
       (physical-reseed-follow-check-final-metadata root-fd (getf snapshot :follow))
       (unless (equal snapshot (physical-reseed-follow-cancellation-snapshot-at root-fd))
         (error "cancellation evidence changed while releasing image resources"))
       snapshot))))
