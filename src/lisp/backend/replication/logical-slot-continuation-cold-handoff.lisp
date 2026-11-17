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

(declaim (ftype function logical-slot-continuation-cold-empty-bind
                logical-slot-continuation-cold-empty-retire))

(defun logical-slot-continuation-cold-check-group-handoff (root pins fork reservation control)
  (let ((group (svref control 1)) (replacement (svref control 2)))
    (unless (and (eq replacement *wal-replication-slots*)
                 (eq group (gethash root *logical-slot-continuation-protections*))
                 (eq group (gethash replacement *logical-slot-continuation-protection-registries*))
                 (eql fork (gethash group pins))
                 (if (eq :published (svref control 0))
                     (eql fork (gethash reservation pins))
                     (null (gethash reservation pins))))
      (error "cold reservation lost its published group handoff"))
    (logical-slot-continuation-protection-call group :check)))

;; Keep the original fork pin until the replacement group has been published
;; and startup notification has completed.  The reservation and group overlap
;; during the handoff so either can be marked uncertain on failure.
(defun logical-slot-continuation-build-cold-reservation-under-lock
    (root binding origin fork child &optional (host-timeline child))
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((pins *wal-snapshot-retention-pins*) (incarnation *wal-storage-incarnation*)
                                             (registry *wal-replication-slots*) (mutex *wal-lock*)
                                             (private-root (copy-list root)) (private-binding (copy-seq binding))
                                             (reservation nil) (uncertain nil) (control (vector :reserved nil nil nil nil)))
    (labels ((fail ()
               (assert (sb-thread:holding-mutex-p mutex))
               (sb-sys:without-interrupts
                 (setf uncertain t)
                 (when (svref control 3) (funcall (svref control 3))))
               t)
             (check ()
               (when uncertain (error "cold continuation reservation is uncertain"))
               (logical-slot-continuation-cold-reservation-check-host
                private-root pins incarnation host-timeline origin)
               (ecase (svref control 0)
                 (:reserved
                  (unless (and (eq registry *wal-replication-slots*)
                               (zerop (hash-table-count registry))
                               (eql fork (gethash reservation pins)))
                    (error "cold reservation lost its unpublished registry or fork pin")))
                 (:empty-published
                  (unless (and (eq (svref control 2) *wal-replication-slots*)
                               (logical-slot-publication-check-registry *wal-replication-slots* (svref control 4))
                               (eql fork (gethash reservation pins)))
                    (error "cold reservation lost its empty publication or fork pin")))
                 ((:published :released)
                  (logical-slot-continuation-cold-check-group-handoff
                   private-root pins fork reservation control)))
               (when uncertain (error "cold reservation failed during its check"))
               t)
             (state (operation object &optional value)
               (assert (and (eq mutex *wal-lock*) (sb-thread:holding-mutex-p mutex)))
               (unless (and (eq object reservation)
                            (if (member (svref control 0) '(:reserved :empty-published))
                                (and (eq reservation
                                         (gethash private-root *logical-slot-continuation-cold-reservations*))
                                     (eq reservation
                                         (gethash (if (eq :reserved (svref control 0)) registry
                                                      (svref control 2))
                                                  *logical-slot-continuation-cold-registries*)))
                                (member (svref control 0) '(:published :released))))
                 (error "cold reservation changed its identity or root registration"))
               (ecase operation
                 (:identity t)
                 (:uncertain uncertain)
                 (:failure-cleanup #'fail)
                 (:root (copy-list private-root))
                 (:binding (copy-seq private-binding))
                 (:origin origin)
                 (:fork fork)
                 (:check (check))
                 (:require-published
                  (when (member (svref control 0) '(:reserved :empty-published))
                    (ereport "55000" "continuation cold reconciliation has not published its group"))
                  (check))
                 (:prepare-handoff
                  (check)
                  (unless (eql child host-timeline)
                    (error "cold C group handoff requires the original child host"))
                  (unless (and (eq :reserved (svref control 0)) (null (svref control 1)))
                    (error "cold reservation already prepared a handoff"))
                  (logical-slot-continuation-cold-bind-handoff reservation value control))
                 (:prepare-empty
                  (check)
                  (unless (and (eq :reserved (svref control 0)) (null (svref control 1)))
                    (error "cold reservation already prepared an empty handoff"))
                  (logical-slot-continuation-cold-empty-bind reservation value control registry))
                 (:retire-empty
                  (check)
                  (unless (eq :empty-published (svref control 0))
                    (error "cold reservation has no empty publication to retire"))
                  (logical-slot-continuation-cold-empty-retire reservation value control))
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
