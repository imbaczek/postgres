(in-package #:pg)

(declaim (ftype function physical-recovery-check-store-point))

(declaim (ftype function physical-follow-call-with-offline-store physical-restartpoint-read))

(defun physical-recovery-policy-history (state store)
  (timeline-check-lineage
   (or (and store (getf (physical-store-selection store) :history))
       (timeline-read-history (getf state :timeline)))))

(defun physical-recovery-policy-target-history (target history)
  (let ((tail (member target history :key #'timeline-history-entry-timeline)))
    (if tail
        (cons (make-timeline-history-entry
               :timeline target :begin (timeline-history-entry-begin (first tail)) :end nil)
              (mapcar #'copy-timeline-history-entry (rest tail)))
        (timeline-check-lineage (timeline-read-history target)))))

(defun physical-recovery-policy-check-pair (pair history)
  (unless (and (typep (first pair) '(unsigned-byte 64))
               (typep (second pair) '(integer 1 4294967295))
               (timeline-prefix-boundary-p (second pair) (first pair) history)
               (or (zerop (first pair))
                   (= (second pair) (timeline-at-point (1- (first pair)) history))))
    (error "recovery timeline target excludes a required position"))
  pair)

(defun physical-recovery-policy-protected-prefix (history frontier)
  (timeline-check-lineage history)
  (loop for entry in (reverse history)
        when (< (timeline-history-entry-begin entry) frontier)
          collect (list (timeline-history-entry-timeline entry)
                        (timeline-history-entry-begin entry)
                        (min frontier (or (timeline-history-entry-end entry) frontier)))))

(defun physical-recovery-resolve-timeline-policy (state store metadata goal)
  (unless (or (member goal '(:latest :current))
              (typep goal '(integer 1 4294967295)))
    (error "invalid recovery timeline goal"))
  (let* ((base (list (getf state :end) (getf state :timeline)))
         (required (list (getf state :required-applied (first base))
                         (getf state :required-timeline (second base))))
         (usable (and metadata (>= (getf metadata :version) 3) metadata))
         (seed (if usable
                   (list (getf usable :lsn) (getf usable :replay-timeline (getf usable :timeline)))
                   base))
         (history (physical-recovery-policy-history state store)))
    (dolist (pair (list base required seed))
      (physical-recovery-policy-check-pair pair history)
      (when store (physical-recovery-check-store-point store (first pair) (second pair))))
    (let* ((anchor (max (second required) (second seed)))
           (target (cond ((eq goal :latest) nil) ((eq goal :current) anchor) (t goal)))
           (target-history (when target (physical-recovery-policy-target-history target history))))
      (when target
        (dolist (pair (list base required seed))
          (physical-recovery-policy-check-pair pair target-history))
        (let ((frontier (max (first base) (first required) (first seed))))
          (unless (equal (physical-recovery-policy-protected-prefix history frontier)
                         (physical-recovery-policy-protected-prefix target-history frontier))
            (error "recovery timeline target rewrites durable ancestry"))))
      (list :goal (if (integerp goal) :numeric goal) :target target :startup-anchor anchor
            :history target-history :original-required required
            :original-observed-next-xid (getf state :observed-next-xid)
            :startup-seed seed))))

(defun physical-recovery-prepare-timeline-policy (state goal)
  (if (= 2 (getf state :phase))
      (physical-follow-call-with-offline-store
       state (lambda (store)
               (multiple-value-bind (metadata catalog prefix digest) (physical-restartpoint-read state)
                 (declare (ignore catalog prefix digest))
                 (physical-recovery-resolve-timeline-policy state store metadata goal))))
      (physical-recovery-resolve-timeline-policy state nil nil goal)))

(defun physical-recovery-policy-check-seed (state store metadata)
  (let ((policy (getf state :timeline-policy)))
    (when (getf policy :target)
      (dolist (pair (list (list (getf state :required-applied)
                                (getf state :required-timeline (getf state :timeline)))
                          (if metadata
                              (list (getf metadata :lsn)
                                    (getf metadata :replay-timeline (getf metadata :timeline)))
                              (list (getf state :end) (getf state :timeline)))))
        (physical-recovery-check-store-point store (first pair) (second pair))
        (physical-recovery-policy-check-pair pair (getf policy :history))))))

(defun physical-recovery-policy-record-permitted-p (store record policy)
  (or (null (getf policy :target))
      (let* ((history (getf policy :history))
             (selection (physical-store-selection store))
             (owner (if selection
                        (timeline-at-point (wal-record-start record) (getf selection :history))
                        (physical-store-timeline store)))
             (expected (timeline-at-point (wal-record-start record) history))
             (entry (find owner history :key #'timeline-history-entry-timeline)))
        (and (= owner expected) entry
             (or (null (timeline-history-entry-end entry))
                 (<= (wal-record-end record) (timeline-history-entry-end entry)))))))

(defun physical-recovery-policy-record-disposition (store record policy)
  (if (physical-recovery-policy-record-permitted-p store record policy)
      :permitted
      (let* ((history (getf (physical-store-selection store) :history))
             (entry (find (getf policy :target) history :key #'timeline-history-entry-timeline)))
        (if (and entry (timeline-history-entry-end entry)
                 (>= (wal-record-start record) (timeline-history-entry-end entry)))
            :timeline-target-reached
            :timeline-history-unavailable))))

(defun physical-recovery-policy-fence-current-p (fence store policy)
  (and fence
       (eq policy (getf fence :policy))
       (eq store (getf fence :store))
       (eq (physical-store-selection store) (getf fence :selection))
       (eql *last-replay-lsn* (getf fence :applied))))

(defun physical-recovery-note-policy-exhaustion (result store)
  (let ((policy (getf *physical-recovery-state* :timeline-policy))
        (old (getf *physical-recovery-state* :timeline-replay-fence)))
    (if (member result '(:timeline-target-reached :timeline-history-unavailable))
        (progn
          (when (and (eq result :timeline-history-unavailable)
                     (not (and (eq result (getf old :reason))
                               (physical-recovery-policy-fence-current-p old store policy))))
            (admin-log "recovery is waiting for timeline ~d; received history follows another branch"
                       (getf policy :target)))
          (setf (getf *physical-recovery-state* :timeline-replay-fence)
                (list :policy policy :store store :selection (physical-store-selection store)
                      :applied *last-replay-lsn* :reason result)))
        (remf *physical-recovery-state* :timeline-replay-fence)))
  result)

(defun physical-recovery-replay-pending-p ()
  (with-live-operation
    (let* ((store *physical-receive-store*)
           (policy (getf *physical-recovery-state* :timeline-policy))
           (fence (getf *physical-recovery-state* :timeline-replay-fence)))
      (and store (< *last-replay-lsn* (physical-store-flushed-lsn store))
           (not (physical-recovery-policy-fence-current-p fence store policy))))))

(defun physical-recovery-map-permitted-records (store function &key
                                                                 (start-lsn (physical-store-floor-lsn store))
                                                                 (policy (getf *physical-recovery-state* :timeline-policy)))
  (block traversal
    (physical-store-map-durable-records
     store (lambda (record raw)
             (let ((disposition (physical-recovery-policy-record-disposition store record policy)))
               (unless (eq disposition :permitted)
                 (return-from traversal disposition)))
             (funcall function record raw))
     :start-lsn start-lsn)))
