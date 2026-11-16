(in-package #:pg)

(defun logical-slot-continuation-cold-reservation-check-host
    (root pins incarnation child origin)
  (unless (and (eq pins *wal-snapshot-retention-pins*)
               (eq incarnation *wal-storage-incarnation*)
               (eql child *wal-insert-timeline*) (eq :timeline *wal-segment-namespace*))
    (error "cold reservation lost its storage or fork pin"))
  (let ((facts (physical-image-file-facts
                -100 (namestring (uiop:ensure-directory-pathname *pgdata*)))))
    (unless (equal root (list (first facts) (second facts) (ninth facts)))
      (error "cold reservation lost its original PGDATA root")))
  (logical-slot-imported-host-origin-call origin :check-root))

;; Keep the original fork pin until the replacement group has been published
;; and startup notification has completed.  The reservation and group overlap
;; during the handoff so either can be marked uncertain on failure.
(defun logical-slot-continuation-build-cold-reservation-under-lock
    (root binding origin fork child)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((pins *wal-snapshot-retention-pins*) (incarnation *wal-storage-incarnation*)
                                             (registry *wal-replication-slots*) (mutex *wal-lock*)
                                             (private-root (copy-list root)) (private-binding (copy-seq binding))
                                             (reservation nil) (uncertain nil) (control (vector :reserved nil nil nil)))
    (labels ((fail ()
               (assert (sb-thread:holding-mutex-p mutex))
               (sb-sys:without-interrupts
                 (setf uncertain t)
                 (when (svref control 3) (funcall (svref control 3))))
               t)
             (check ()
               (when uncertain (error "cold continuation reservation is uncertain"))
               (logical-slot-continuation-cold-reservation-check-host
                private-root pins incarnation child origin)
               (ecase (svref control 0)
                 (:reserved
                  (unless (and (eq registry *wal-replication-slots*)
                               (zerop (hash-table-count registry))
                               (eql fork (gethash reservation pins)))
                    (error "cold reservation lost its unpublished registry or fork pin")))
                 ((:published :released)
                  (let ((group (svref control 1)) (replacement (svref control 2)))
                    (unless (and (eq replacement *wal-replication-slots*)
                                 (eq group (gethash private-root *logical-slot-continuation-protections*))
                                 (eq group (gethash replacement
                                                    *logical-slot-continuation-protection-registries*))
                                 (eql fork (gethash group pins))
                                 (if (eq :published (svref control 0))
                                     (eql fork (gethash reservation pins))
                                     (null (gethash reservation pins))))
                      (error "cold reservation lost its published group handoff"))
                    (logical-slot-continuation-protection-call group :check))))
               (when uncertain (error "cold reservation failed during its check"))
               t)
             (state (operation object &optional value)
               (assert (and (eq mutex *wal-lock*) (sb-thread:holding-mutex-p mutex)))
               (unless (and (eq object reservation)
                            (if (eq :reserved (svref control 0))
                                (and (eq reservation
                                         (gethash private-root *logical-slot-continuation-cold-reservations*))
                                     (eq reservation
                                         (gethash registry *logical-slot-continuation-cold-registries*)))
                                (member (svref control 0) '(:published :released))))
                 (error "cold reservation changed its identity or root registration"))
               (ecase operation
                 (:identity t)
                 (:uncertain uncertain)
                 (:failure-cleanup #'fail)
                 (:binding (copy-seq private-binding))
                 (:origin origin)
                 (:fork fork)
                 (:check (check))
                 (:require-published
                  (when (eq :reserved (svref control 0))
                    (ereport "55000" "continuation cold reconciliation has not published its group"))
                  (check))
                 (:prepare-handoff
                  (check)
                  (unless (and (eq :reserved (svref control 0)) (null (svref control 1)))
                    (error "cold reservation already prepared a handoff"))
                  (logical-slot-continuation-cold-bind-handoff reservation value control))
                 (:retire-handoff
                  (check)
                  (unless (and (eq :published (svref control 0)) (not uncertain)
                               (not (logical-slot-continuation-protection-call
                                     (svref control 1) :uncertain)))
                    (error "cold reservation cannot retire its fork pin"))
                  (sb-sys:without-interrupts
                    (remhash reservation pins)
                    (setf (svref control 0) :released))
                  t))))
      (setf reservation (%make-logical-slot-continuation-cold-reservation :state #'state))
      (values reservation #'fail))))
