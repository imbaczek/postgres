(in-package #:pg)

(defun logical-slot-continuation-boundary-match-p
    (request record owner kind original fork terminal)
  ;; A changed restart must be a batch or EOR start, or a completed terminal
  ;; end.  A changed acknowledgement must be a completed terminal end; the
  ;; checkpoint is not an acknowledgement.
  (and (eql owner (third request))
       (ecase (first request)
         (:confirmed
          (and terminal (eq :prefix-end (fourth request))
               (= (second request) (wal-record-end record))))
         (:restart
          (or (and (eq :record-start (fourth request))
                   (or (= (wal-record-type record) +wal-record-batch-start+)
                       (eq kind :end-of-recovery))
                   (= (second request) (wal-record-start record)))
              (and terminal (eq :prefix-end (fourth request))
                   (= (second request) (wal-record-end record)))
              (and fork (= (second request) fork) (= original fork)
                   (eq :record-start (fourth request)) (eq kind :end-of-recovery)
                   (= (wal-record-start record) (logical-wal-eor-position fork))))))))

(defun logical-slot-continuation-boundary-observer (requests &key fork checkpoint)
  (logical-slot-copy-descriptor-list-count requests (* 2 +logical-slot-continuation-obligation-limit+))
  (when fork (unless (typep fork '(unsigned-byte 64)) (error "invalid continuation fork")))
  (when checkpoint
    (logical-slot-continuation-list checkpoint 2)
    (unless (typep (first checkpoint) '(unsigned-byte 64)) (error "invalid continuation checkpoint"))
    (logical-slot-copy-descriptor-blob-size (second checkpoint) +wal-header-size+ +wal-segment-size+)
    (setf checkpoint (list (first checkpoint) (copy-seq (second checkpoint)))))
  (let ((pending (make-hash-table :test #'eql))
        (batch-open nil) (clean nil) (seen nil) (failed nil))
    (dolist (request requests)
      (logical-slot-continuation-list request 4)
      (unless (and (member (first request) '(:restart :confirmed))
                   (or (eq :restart (first request)) (eq :prefix-end (fourth request))))
        (error "invalid continuation saved-position obligation"))
      (logical-slot-continuation-position-shape (rest request))
      (pushnew (copy-list request) (gethash (second request) pending) :test #'equal))
    (labels ((observe (record raw owner carrier kind original)
               (declare (ignore carrier))
               (when failed (error "continuation boundary proof has failed"))
               (let ((complete nil) (terminal nil) (type (wal-record-type record)))
                 (unwind-protect
                      (progn
                        (ecase kind
                          (:end-of-recovery
                           (unless (and (= type +wal-record-end-of-recovery+) (not batch-open))
                             (error "continuation EOR interrupts an ordinary batch"))
                           (setf clean nil))
                          (:ordinary
                           (when (= type +wal-record-end-of-recovery+)
                             (error "continuation EOR was classified as ordinary"))
                           (let ((*wal-recovery-reservation-batch-clean-p* clean))
                             (wal-note-recovery-batch-record record)
                             (cond
                               ((= type +wal-record-batch-start+)
                                (when batch-open (error "nested continuation batch"))
                                (setf batch-open t))
                               ((and (not batch-open) (not seen) checkpoint
                                     (= type +wal-record-checkpoint+)
                                     (= (first checkpoint) (wal-record-start record))
                                     (equalp raw (second checkpoint)))
                                nil)
                               ((not batch-open) (error "continuation record appears outside a batch"))
                               ((wal-terminal-record-type-p type)
                                (when (wal-empty-metadata-terminal-type-p type)
                                  (wal-validate-empty-metadata-terminal
                                   record *wal-recovery-reservation-batch-clean-p*))
                                (setf terminal t batch-open nil
                                      *wal-recovery-reservation-batch-clean-p* nil)))
                             (setf clean *wal-recovery-reservation-batch-clean-p*))))
                        (dolist (position (remove-duplicates
                                           (list (wal-record-start record) (wal-record-end record)
                                                 (and fork (= original fork) fork))
                                           :test #'eql))
                          (when position
                            (let ((remaining
                                    (remove-if
                                     (lambda (request)
                                       (logical-slot-continuation-boundary-match-p
                                        request record owner kind original fork terminal))
                                     (gethash position pending))))
                              (if remaining (setf (gethash position pending) remaining)
                                  (remhash position pending)))))
                        (setf seen t complete t))
                   (unless complete (setf failed t)))))
             (finish ()
               (unless (and (not failed) (not batch-open) (zerop (hash-table-count pending)))
                 (setf failed t)
                 (error "continuation saved position has no completed record-boundary proof"))
               t))
      (values #'observe #'finish))))

(defun logical-slot-continuation-saved-boundaries (snapshot certificate)
  (logical-slot-continuation-check snapshot)
  (let ((history (logical-slot-continuation-history
                  (getf snapshot :history-target) (getf snapshot :history)))
        (requests nil))
    (dolist (row (getf snapshot :slots))
      (let* ((slot (logical-slot-state-decode (second row) (first row)))
             (source (find (first row) (getf certificate :slots) :key #'first :test #'equal)))
        (unless (and source (fourth source)) (error "saved continuation boundary has no certified origin"))
        (let* ((baseline (logical-slot-state-decode (string->bytes (fourth source)) (first row)))
               (restart (wal-replication-slot-restart-lsn slot))
               (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
               (confirmed-owner (wal-replication-slot-confirmed-timeline slot)))
          (logical-slot-imported-check-state slot baseline history (first (getf snapshot :sealed-end)))
          (when (eq :reserved (wal-replication-slot-wal-status slot))
            (unless (and (= restart (wal-replication-slot-restart-lsn baseline))
                         (eql (wal-replication-slot-restart-timeline slot)
                              (wal-replication-slot-restart-timeline baseline)))
              (multiple-value-bind (owner role) (logical-slot-startup-start-position slot history)
                (pushnew (list :restart restart owner role) requests :test #'equal)))
            (when (and confirmed
                       (not (and (eql confirmed (wal-replication-slot-confirmed-flush-lsn baseline))
                                 (eql confirmed-owner (wal-replication-slot-confirmed-timeline baseline))))
                       (not (and (= confirmed (wal-replication-slot-consistent-lsn baseline))
                                 (eql confirmed-owner
                                      (logical-slot-provenance-birth-timeline
                                       (wal-replication-slot-logical-provenance baseline))))))
              (pushnew (list :confirmed confirmed confirmed-owner :prefix-end) requests :test #'equal))))))
    (nreverse requests)))

(defun logical-slot-continuation-prove-parent-boundaries (storage facts archive requests)
  ;; Parent requests use the retained origin archive; child requests are
  ;; returned for the local tail proof.  Requests before the archive base
  ;; are rejected until a separate retained sidecar proof exists.
  (setf facts (logical-slot-continuation-storage-call storage :facts))
  (let* ((certificate (getf facts :certificate))
         (fork (getf certificate :fork)) (child (getf certificate :child-timeline))
         (parent-requests nil) (child-requests nil))
    (dolist (request requests)
      (if (or (< (second request) fork)
              (and (= (second request) fork) (/= (third request) child)))
          (push request parent-requests)
          (push request child-requests)))
    (when parent-requests
      (let* ((base (getf (logical-wal-archive-reference archive) :base))
             (history (logical-wal-archive-history archive)))
        (when (some (lambda (request) (< (second request) base)) parent-requests)
          (error "advanced saved continuation position requires its retained sidecar proof"))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer
             parent-requests
             :checkpoint (list (physical-manifest-checkpoint-start (getf facts :manifest))
                               (getf (getf facts :evidence) :checkpoint)))
          (flet ((guard ()
                   (logical-slot-continuation-storage-call storage :check-under-lock)
                   (logical-wal-archive-check archive)))
            (call-with-logical-wal-cursor
             history :timeline base fork (timeline-at-point base history) :record-start #'guard
             (lambda (cursor)
               (loop for original = (logical-wal-cursor-position cursor)
                     do (multiple-value-bind (record raw owner carrier kind) (logical-wal-cursor-next cursor)
                          (unless record (return))
                          (funcall observe record raw owner carrier kind original)))
               (unless (= fork (logical-wal-cursor-position cursor))
                 (error "continuation parent boundary proof did not reach exact F"))
               (funcall finish))
             :archive archive :archive-only t :batch-policy :logical-reader)))))
    (nreverse child-requests)))

(defun logical-slot-continuation-active-boundaries (storage facts snapshot archive)
  (setf facts (logical-slot-continuation-storage-call storage :facts))
  (let ((requests (logical-slot-continuation-saved-boundaries snapshot (getf facts :certificate))))
    (logical-slot-continuation-boundary-observer
     (logical-slot-continuation-prove-parent-boundaries storage facts archive requests)
     :fork (second (getf snapshot :origin)))))
