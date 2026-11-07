;;; Record a structural source-closed observation and recover its publication.
;;; Completion of source cleanup requires separate operation evidence.

(in-package #:pg)

(defun physical-reseed-follow-record-cancellation-source-closed (owner observation)
  ;; Record the structurally checked source observation for a pending owner.
  ;; Cleanup completion requires separate evidence of the source operation.
  ;; A matching source-closed retry synchronizes the saved record.  Busy remains
  ;; held through finalization, and uncertain publication requires recovery.
  (let ((observation (physical-backup-lease-copy observation)))
    (physical-reseed-follow-check-idle-state owner)
    (setf (physical-reseed-follow-owner-busy-p owner) t)
    (unwind-protect
         (let ((expected
                 (call-with-physical-directory-root
                  owner
                  (lambda (root-fd directory)
                    (declare (ignore directory))
                    (let ((snapshot (physical-reseed-follow-snapshot-at root-fd)))
                      (unless snapshot (error "source observation requires a durable cancellation predecessor"))
                      (physical-reseed-follow-cancellation-preconfiguration-record (getf snapshot :record))
                      snapshot)))))
           (let* ((before (getf expected :record))
                  (phase (getf (getf before :cancellation) :phase))
                  (candidate
                    (case phase
                      (:pending (physical-reseed-follow-cancellation-source-closed-record before observation))
                      (:source-closed
                       (unless (equal observation (getf (getf before :cancellation) :receipt))
                         (error "source observation differs from the saved closed receipt"))
                       (physical-reseed-follow-copy before))
                      (otherwise (error "source observation requires pending or source-closed cancellation")))))
             (physical-reseed-follow-cancellation-check-cleanup-capacity candidate)
             (call-with-physical-reseed-follow-cancellation-finalization
              owner
              (lambda (root-fd check)
                (physical-reseed-follow-check-snapshot root-fd expected)
                (when (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t)
                  (error "source cleanup temporary requires explicit recovery"))
                (physical-reseed-follow-cancellation-local-proof owner root-fd candidate)
                (funcall check)
                (if (eq phase :source-closed)
                    (physical-reseed-follow-cancellation-stable owner root-fd expected check)
                    (let ((bytes (physical-reseed-follow-image candidate)))
                      (call-with-physical-reseed-follow-cancellation-cleanup-authority
                       owner root-fd candidate expected bytes :write check
                       (lambda (verify)
                         (physical-reseed-follow-write-intent
                          owner root-fd candidate bytes expected verify)))))))))
      (sb-sys:without-interrupts
        (setf (physical-reseed-follow-owner-busy-p owner) nil)))))

(defun physical-reseed-follow-cancellation-recover-source-closed
    (owner root-fd previous temporary check)
  ;; Recheck the exact successor and temporary identity before promotion.
  ;; Hold the descriptor through promotion.  Keep uncertainty latched through
  ;; finalization.
  (declare (notinline sb-posix:close))
  (multiple-value-bind (candidate identity)
      (physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")
    (unless (equal temporary identity)
      (error "source-closed recovery temporary changed"))
    (physical-reseed-follow-cancellation-cleanup-candidate candidate previous)
    (physical-reseed-follow-cancellation-local-proof owner root-fd candidate)
    (physical-reseed-follow-cancellation-check-cleanup-capacity candidate)
    (funcall check)
    (physical-reseed-follow-check-snapshot root-fd previous)
    (let ((fd nil) (bytes (physical-reseed-follow-image candidate)))
      (unwind-protect
           (progn
             (setf (physical-reseed-follow-owner-uncertain-p owner) t)
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         root-fd "physical-reseed.follow.tmp"
                         (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                 +postmaster-o-cloexec+) 0)))
             (when (minusp fd) (setf fd nil) (error "cannot open source-closed recovery temporary"))
             (unless (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
               (error "source-closed recovery descriptor changed"))
             (call-with-physical-reseed-follow-cancellation-cleanup-authority
              owner root-fd candidate previous bytes :recover check
              (lambda (verify)
                (physical-reseed-follow-promote-intent
                 owner root-fd fd (subseq identity 0 5) candidate previous verify))
              fd identity))
        (when fd (sb-posix:close fd))))))
