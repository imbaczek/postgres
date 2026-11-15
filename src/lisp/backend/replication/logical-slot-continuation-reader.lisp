(in-package #:pg)

(defun call-with-logical-slot-continuation-lanes
    (facts snapshot slot archive sidecar-reader local-reader guard function
     &key (end (first (getf snapshot :sealed-end))) position local-provider (batch-policy :logical-reader))
  ;; D is the manifest end (the checkpoint end), not the archive base or
  ;; checkpoint start.  Read before D from the sidecar, D through F from the
  ;; original parent archive only, and F through the sealed end from local
  ;; original-child WAL.  The archive lane has no local fallback.
  (let* ((history (logical-slot-continuation-history
                   (getf snapshot :history-target) (getf snapshot :history)))
         (parent-history (logical-slot-copy-source-proof-parent-history facts))
         (base (getf (getf facts :manifest) :end))
         (fork (getf (getf facts :certificate) :fork))
         (start (if position (first position) (wal-replication-slot-restart-lsn slot)))
         (specifications nil))
    (unless (and (integerp start) (integerp end) (<= 0 start end))
      (error "invalid imported reader interval"))
    (multiple-value-bind (owner role) (if position
                                          (progn (logical-slot-continuation-list position 3)
                                                 (values (second position) (third position)))
                                          (logical-slot-startup-start-position slot history))
      (flet ((add-lane (lineage begin stop start-owner start-role archive reader &optional provider)
               (when (< begin stop)
                 (push (list lineage begin stop start-owner start-role archive reader provider) specifications))))
        (add-lane parent-history start (min base end) owner role nil sidecar-reader)
        (let ((begin (max start base)) (stop (min fork end)))
          (add-lane parent-history begin stop
                    (if (= begin start) owner (timeline-at-point (1- begin) parent-history))
                    (if (= begin start) role :prefix-end) archive nil))
        (let ((begin (max start fork)))
          (add-lane history begin end
                    (if (= begin fork) (getf snapshot :history-target) owner)
                    (if (= begin fork) :record-start role) nil local-reader local-provider)))
      (labels ((open-lanes (remaining cursors)
                 (if (null remaining)
                     (if cursors
                         (call-with-logical-wal-lane-provider
                          (nreverse cursors) guard
                          (lambda (provider)
                            (call-with-logical-wal-cursor
                             history :timeline start end owner role guard
                             (lambda (cursor) (funcall function cursor (list base fork end)))
                             :record-reader provider :batch-policy batch-policy)))
                         (call-with-logical-wal-cursor
                          history :timeline start end owner role guard
                          (lambda (cursor) (funcall function cursor (list base fork end)))
                          :segment-reader local-reader :batch-policy batch-policy))
                     (destructuring-bind (lineage begin stop start-owner start-role lane-archive reader provider)
                         (first remaining)
                       (call-with-logical-wal-cursor
                        lineage :timeline begin stop start-owner start-role guard
                        (lambda (cursor) (open-lanes (rest remaining) (cons cursor cursors)))
                        :archive lane-archive :archive-only (not (null lane-archive))
                        :segment-reader reader :record-reader provider :batch-policy batch-policy)))))
        (open-lanes (nreverse specifications) nil)))))

(defun call-with-logical-slot-continuation-storage-readers (storage facts function &optional finish)
  ;; Keep startup storage, canonical proof, selector FD, origin root lease,
  ;; local tail proof and sidecar live for the multislot callback.  The scope
  ;; checks guard, check and read-slot with a failure latch and postcheck.
  ;; Callbacks are trusted internal code and can access raw data; this scope
  ;; does not sandbox them.  Current startup supports the original child only.
  ;; Atomic startup publication is available to the original-child startup
  ;; library.  Automatic loading, admission, runtime scan and ACK remain
  ;; separate work; the GC fence remains closed.
  (logical-slot-continuation-storage-call
   storage :with-origin
   (lambda (archive)
     (unless (equal '("selector") (logical-slot-continuation-selector-names storage))
       (error "composed startup reader requires an unambiguous stable selector"))
     (call-with-logical-slot-continuation-selector
      storage
      (lambda (snapshot fd selector-facts)
        (let ((image (logical-slot-continuation-encode snapshot))
              (directory (logical-slot-continuation-storage-call storage :directory))
              (source-facts (logical-slot-continuation-storage-call storage :facts)))
          (logical-slot-continuation-check-active-startup storage facts snapshot)
          (call-with-logical-slot-continuation-local-tail-source
           storage snapshot
           (lambda (prove check-tail local-reader local-guard)
             (funcall prove)
             (logical-slot-continuation-storage-call
              storage :with-sidecar
              (lambda (sidecar-reader check-files)
                (let ((active t) (failed nil) (thread sb-thread:*current-thread*))
                  (macrolet ((in-scope (&body body)
                               (let ((complete (gensym "COMPLETE")))
                                 `(progn
                                    (unless (eq thread sb-thread:*current-thread*)
                                      (error "composed startup reader changed thread"))
                                    (unless (and active (not failed))
                                      (error "composed startup reader ended or failed"))
                                    (sb-sys:without-interrupts
                                      (let ((,complete nil))
                                        (unwind-protect
                                             (multiple-value-prog1
                                                 (sb-sys:with-local-interrupts (progn ,@body))
                                               (setf ,complete t))
                                          (unless ,complete (setf failed t)))))))))
                    (labels ((guard ()
                               (in-scope
                                (logical-slot-continuation-storage-call storage :check-under-lock)
                                (funcall local-guard)
                                (unless (and (equal selector-facts (physical-image-file-facts fd))
                                             (equal selector-facts (physical-image-file-facts directory "selector"))
                                             (equal '("selector")
                                                    (logical-slot-copy-import-control-names directory 3)))
                                  (error "composed source lost its stable selected selector"))
                                t))
                             (check ()
                               (in-scope
                                (unless (equal '("selector") (logical-slot-continuation-selector-names storage))
                                  (error "composed source has an ambiguous selector"))
                                (logical-slot-continuation-check-active-startup storage nil snapshot)
                                (logical-slot-imported-check-held-file directory "selector" fd selector-facts image)
                                (funcall check-files)
                                (funcall check-tail)))
                             (read-slot (name callback)
                               (in-scope
                                (check)
                                (let* ((row (find name (getf snapshot :slots) :key #'first :test #'equal))
                                       (slot (and row (logical-slot-state-decode (second row) name))))
                                  (unless (and slot (eq :reserved (wal-replication-slot-wal-status slot)))
                                    (error "composed startup reader requires its saved reserved slot"))
                                  (multiple-value-prog1
                                      (call-with-logical-slot-continuation-lanes
                                       source-facts snapshot slot archive sidecar-reader local-reader #'guard callback)
                                    (check))))))
                      (unwind-protect
                           (progn
                             (check)
                             (let ((results (multiple-value-list (funcall function #'read-slot #'check))))
                               (check)
                               (funcall prove)
                               (check)
                               (multiple-value-prog1
                                   (if finish
                                       (funcall finish (first results) snapshot #'check #'guard)
                                       (values-list results))
                                 (check))))
                        (sb-sys:without-interrupts (setf active nil)))))))))
           :boundaries (lambda ()
                         (logical-slot-continuation-active-boundaries storage nil snapshot archive)))))))))

(defun call-with-logical-slot-continuation-startup-readers (gate ownership function)
  (call-with-logical-slot-continuation-startup-storage
   gate ownership
   (lambda (storage facts)
     (call-with-logical-slot-continuation-storage-readers storage facts function))))

(defun call-with-logical-slot-continuation-startup-cursor (gate ownership name function)
  (call-with-logical-slot-continuation-startup-readers
   gate ownership
   (lambda (read-slot check)
     (funcall read-slot name (lambda (cursor seams) (funcall function cursor check seams))))))
