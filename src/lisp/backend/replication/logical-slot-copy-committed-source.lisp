(in-package #:pg)

(defun call-with-logical-slot-copy-committed-store (owner state function)
  (unless (logical-slot-copy-committed-owner-p owner)
    (error "committed receive input requires its sibling owner"))
  (if (eq :live (logical-slot-copy-committed-owner-mode owner))
      (funcall function (logical-slot-copy-committed-owner-store owner))
      (let ((store nil))
        (unwind-protect
             (progn
               (physical-promotion-open-pending-store
                state :on-open (lambda (opened) (setf store opened)))
               (funcall function store))
          (when store (physical-store-close store))))))

(defun logical-slot-copy-committed-state (manifest control descriptor)
  (unless (and (eql 4 (getf control :control-version)) (eql 3 (getf control :phase)))
    (error "committed source reconstruction requires preserved v4 phase 3"))
  (let* ((fields (logical-slot-copy-control-fields
                  (logical-slot-copy-descriptor-base-control descriptor)))
         (snapshot (third fields))
         (namespace (sixth fields)))
    (unless (eq namespace (getf control :promotion-source-namespace))
      (error "committed source namespace differs from its preserved D control"))
    ;; The input owner already binds actual child control to the exact saved plan.
    ;; All other base rows retain the ordinary streaming metadata checks.
    (let ((base (copy-list manifest)))
      (setf (getf base :files)
            (remove "global/pg_wal_control" (getf manifest :files) :key #'first :test #'equal))
      (physical-recovery-check-streaming-base base control))
    (physical-promotion-reconstruct-pending-state manifest control snapshot namespace)))

(defun logical-slot-copy-check-committed-progress (state metadata prefix generation store)
  (let ((*physical-recovery-state* state) (*physical-receive-store* store)
                                          (*last-replay-lsn* (getf state :applied)) (*next-xid* (getf state :next-xid))
                                          (*wal-segment-namespace* (getf state :wal-namespace))
                                          (*wal-insert-timeline* (getf state :timeline))
                                          (*wal-checkpoint-timeline* (getf state :timeline)))
    (physical-promotion-check-progress state metadata prefix generation 3)
    (physical-recovery-check-base-forks
     (list :files (getf metadata :forks)) (make-hash-table :test #'eql))))

(defun logical-slot-copy-check-committed-child (context evidence plan prefix control)
  (let* ((files (physical-promotion-checkpoint-files plan prefix))
         (child (getf plan :child))
         (*physical-recovery-state* control)
         (observations nil))
    (unless (equal (mapcar #'first files) (wal-discover-segments child :timeline))
      (error "committed import child segment inventory differs from its plan"))
    (flet ((check (name expected)
             (multiple-value-bind (bytes facts)
                 (logical-slot-copy-read-file context name (length expected))
               (unless (equalp expected bytes)
                 (error "committed import child bytes differ from their source-authenticated plan"))
               (push (list name (length bytes) (logical-slot-copy-import-digest bytes) facts)
                     observations))))
      (check (concatenate 'string "pg_wal/"
                          (file-namestring (timeline-history-path child *pgdata*)))
             (getf evidence :child-history))
      (check "global/pg_wal_control"
             (physical-promotion-child-control-image plan (getf (getf plan :snapshot) :time)))
      (dolist (entry files)
        (check (concatenate 'string "pg_wal/"
                            (wal-segment-filename (first entry) child :timeline))
               (second entry))))
    observations))

(defun logical-slot-copy-recheck-committed-state
    (manifest control descriptor state metadata prefix generation)
  (multiple-value-bind (actual-state actual-metadata actual-prefix actual-generation)
      (logical-slot-copy-committed-state manifest control descriptor)
    (unless (logical-slot-copy-committed-image-equal-p (logical-slot-copy-import-state-image
                                                        (list state metadata prefix generation))
                                                       (logical-slot-copy-import-state-image
                                                        (list actual-state actual-metadata actual-prefix actual-generation)))
      (error "committed import selected restartpoint changed"))))

(defun logical-slot-copy-check-committed-source
    (context certificate evidence descriptor manifest control plan state metadata prefix generation store)
  (logical-slot-copy-check-committed-progress state metadata prefix generation store)
  (unless (and (equalp (getf evidence :selected-history)
                       (logical-slot-copy-import-selected-history state))
               (equalp (getf evidence :child-history) (physical-promotion-history-image state))
               (logical-slot-copy-committed-image-equal-p plan (physical-promotion-current-checkpoint-plan state)))
    (error "committed import source history or child plan differs from actual restartpoint"))
  (multiple-value-bind (fact history tuple state-image)
      (logical-slot-copy-import-scan store state manifest descriptor control)
    (let ((observed (copy-list control)))
      (when (and (getf control :logical-fact) (not (equal fact (getf control :logical-fact))))
        (error "committed import source logical fact differs from its frozen control"))
      (setf (getf observed :logical-fact) fact)
      (unless (eq :control-fact-checked
                  (logical-slot-copy-evidence-check-rows certificate descriptor observed history))
        (error "committed import source logical replay did not resolve slot dispositions"))
      (when (and (find :eligible (getf certificate :slots) :key #'fifth)
                 (not (getf control :promotion-logical-decoding-enabled)))
        (error "committed import disabled eligible logical interest"))
      (let ((child-observations
              (logical-slot-copy-check-committed-child context evidence plan prefix control)))
        (unless (and (equal tuple (physical-store-control-tuple store))
                     (logical-slot-copy-committed-image-equal-p state-image (logical-slot-copy-import-state-image state)))
          (error "committed import source changed during verification"))
        (logical-slot-copy-check-committed-progress state metadata prefix generation store)
        (logical-slot-copy-recheck-committed-state
         manifest control descriptor state metadata prefix generation)
        (values fact history tuple child-observations)))))

(defun call-with-logical-slot-copy-committed-source (function)
  (call-with-logical-slot-copy-committed-input
   (lambda (owner context certificate evidence descriptor manifest control plan status)
     (declare (ignore status))
     (let ((*pgdata* (physical-image-context-root-path context))
           (*physical-image-context* nil))
       (multiple-value-bind (state metadata prefix generation)
           (logical-slot-copy-committed-state manifest control descriptor)
         (call-with-logical-slot-copy-committed-store
          owner state
          (lambda (store)
            (physical-promotion-ensure-pending-retained state metadata store)
            (physical-follow-call-with-receive-root
             store
             (lambda ()
               (multiple-value-bind (fact history tuple child-observations)
                   (logical-slot-copy-check-committed-source
                    context certificate evidence descriptor manifest control plan
                    state metadata prefix generation store)
                 (let ((image (logical-slot-copy-import-state-image
                               (list certificate evidence manifest control plan state metadata prefix fact history))))
                   (multiple-value-prog1
                       (funcall function owner context store certificate plan fact history)
                     (unless (and (equal tuple (physical-store-control-tuple store))
                                  (logical-slot-copy-committed-image-equal-p image (logical-slot-copy-import-state-image
                                                                                    (list certificate evidence manifest control
                                                                                          plan state metadata prefix fact history))))
                       (error "committed import source binding was modified"))
                     (logical-slot-copy-input-check-observations owner context child-observations)
                     (dolist (row child-observations)
                       (unless (equal row (logical-slot-copy-file-row context (first row)))
                         (error "committed import child file changed during its scope")))
                     (logical-slot-copy-check-committed-source
                      context certificate evidence descriptor manifest control plan
                      state metadata prefix generation store)))))))))))))
