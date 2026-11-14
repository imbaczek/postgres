(in-package #:pg)

;;; Keep source observations and replay under one cold receive-root lease.
;;; The lease is observational and does not authorize installation writes.
(defun logical-slot-copy-import-source-parent-observation (context descriptor)
  "Require exact parent D control and return its file observation."
  (multiple-value-bind (bytes facts)
      (logical-slot-copy-read-file context "global/pg_wal_control" (1+ +wal-control-maximum-bytes+))
    (unless (equalp bytes (logical-slot-copy-descriptor-base-control descriptor))
      (error "leased import source requires its exact parent control"))
    (list "global/pg_wal_control" (length bytes) (logical-slot-copy-import-digest bytes) facts)))

(defun call-with-logical-slot-copy-import-source (owner context function)
  "Call FUNCTION with source data and a zero-argument checker under a cold lease.
The cold phase 3 scope holds the same store and receive-root lease throughout.
The checker compares a finite cloned seal and observations, then repeats the
D through F source replay; it runs again after FUNCTION returns.  A failed or
expired checker cannot be reused.  After the initial input check, failure,
including store close failure, revokes the owner.  This scope grants no mutation, write, or
admission authority.  A write owner must check the specific temporary file
and the held global directory's entries before changing files."
  (unless (and (logical-slot-copy-import-owner-p owner)
               (eq :cold (logical-slot-copy-import-owner-mode owner))
               (= 3 (logical-slot-copy-import-owner-phase owner))
               (functionp function))
    (error "leased import source requires a cold phase 3 input owner and callback"))
  (logical-slot-copy-check-input owner context)
  (let ((finished nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-logical-slot-copy-import-source-state
              owner context
              (lambda (store descriptor observations manifest control state metadata prefix generation)
                (let* ((source (logical-slot-copy-read-import-source
                                owner context store descriptor observations manifest control
                                state metadata prefix generation))
                       (observations (cons (logical-slot-copy-import-source-parent-observation context descriptor)
                                           observations))
                       (tuple (copy-tree (physical-store-control-tuple store)))
                       (descriptor-image (logical-slot-copy-descriptor-encode descriptor))
                       (image (logical-slot-copy-import-state-image
                               (list source state metadata prefix generation manifest control observations)))
                       (active t) (failed nil))
                  (labels ((check ()
                             (unless (and active (not failed))
                               (error "leased import source proof ended or failed"))
                             (let ((complete nil))
                               (unwind-protect
                                    (progn
                                      (logical-slot-copy-check-input owner context)
                                      (unless (and (logical-slot-copy-committed-image-equal-p
                                                    image (list source state metadata prefix generation
                                                                manifest control observations))
                                                   (equal tuple (physical-store-control-tuple store))
                                                   (equalp descriptor-image
                                                           (logical-slot-copy-descriptor-encode descriptor)))
                                        (error "leased import source binding was modified"))
                                      (logical-slot-copy-import-check-progress
                                       owner context state metadata prefix generation store)
                                      (unless (logical-slot-copy-committed-image-equal-p
                                               (first image)
                                               (logical-slot-copy-read-import-source
                                                owner context store descriptor observations manifest control
                                                state metadata prefix generation))
                                        (error "leased import source changed during its scope"))
                                      (setf complete t))
                                 (unless complete
                                   (setf failed t (physical-adoption-owner-active-p owner) nil))))))
                    (unwind-protect
                         (multiple-value-prog1 (funcall function source #'check)
                           (check))
                      (setf active nil))))))
           (setf finished t))
      (unless finished (setf (physical-adoption-owner-active-p owner) nil)))))
