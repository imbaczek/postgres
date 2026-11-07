;;; Record a structural source-closed observation and recover its publication.
;;; Completion of source cleanup requires separate operation evidence.

(in-package #:pg)

(defun physical-reseed-follow-publish-cancellation-source-closed-held (owner expected candidate evidence-check)
  (let* ((previous (getf expected :follow))
         (phase (getf (getf (getf previous :record) :cancellation) :phase))
         (checked (physical-reseed-follow-cancellation-source-closed-candidate
                   (getf previous :record) (getf (getf candidate :cancellation) :receipt)))
         (published nil))
    (unless (equal candidate checked)
      (error "source-closed publication differs from its exact candidate"))
    (let ((result
            (call-with-physical-reseed-follow-cancellation-finalization
             owner
             (lambda (root-fd store-check)
               (flet ((check () (funcall store-check) (funcall evidence-check root-fd)))
                 (physical-reseed-follow-check-snapshot root-fd previous)
                 (when (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t)
                   (error "source cleanup temporary requires explicit recovery"))
                 (physical-reseed-follow-cancellation-local-proof owner root-fd candidate)
                 (check)
                 (multiple-value-bind (record final)
                     (if (eq phase :source-closed)
                         (physical-reseed-follow-cancellation-stable owner root-fd previous #'check)
                         (let ((bytes (physical-reseed-follow-image candidate)))
                           (call-with-physical-reseed-follow-cancellation-cleanup-authority
                            owner root-fd candidate previous bytes :write #'check
                            (lambda (verify)
                              (physical-reseed-follow-write-intent
                               owner root-fd candidate bytes previous verify)))))
                   (setf published (physical-adoption-attempt-value-copy final))
                   (values record final)))))))
      (values result published))))

(defun physical-reseed-follow-record-cancellation-source-closed-held (operation expected observation)
  (physical-reseed-follow-cancellation-operation-call operation :source-closed expected observation))

(defun physical-reseed-follow-record-cancellation-source-closed (owner observation)
  ;; Record the structurally checked source observation for a pending owner.
  ;; Cleanup completion requires separate evidence of the source operation.
  ;; A matching source-closed retry synchronizes the saved record.  Busy remains
  ;; held through finalization, and uncertain publication requires recovery.
  (let ((observation (physical-backup-lease-copy observation)))
    (call-with-physical-reseed-follow-cancellation-operation
     owner
     (lambda (operation)
       (nth-value 0
                  (physical-reseed-follow-record-cancellation-source-closed-held
                   operation
                   (physical-reseed-follow-cancellation-operation-call operation :snapshot)
                   observation))))))

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

(defun physical-reseed-follow-record-cancellation-cleaned-held (operation expected evidence)
  (physical-reseed-follow-cancellation-operation-call operation :cleaned expected evidence))
