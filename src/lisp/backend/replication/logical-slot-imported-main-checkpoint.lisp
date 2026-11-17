(in-package #:pg)

(defun logical-slot-imported-startup-check-child-binding (facts fields history)
  ;; Bind the validated child checkpoint and history to the imported origin.
  ;; The position in this lineage is not proof that all earlier WAL is retained.
  (let ((certificate (getf facts :certificate)))
    (unless (and (eql (getf certificate :system-id)
                      (getf (seventh fields) :system-id))
                 (<= (getf certificate :child-end) (second fields))
                 (timeline-prefix-boundary-p (getf certificate :child-timeline)
                                             (getf certificate :child-end) history))
      (error "current child checkpoint differs from the imported identity or floor"))
    (logical-slot-imported-check-origin-history
     (getf certificate :child-timeline) (getf certificate :parent-timeline)
     (getf certificate :fork) (getf (getf facts :evidence) :child-history) history))
  t)

(defun logical-slot-imported-startup-check-checkpoint-inputs (held)
  (dolist (row held)
    (destructuring-bind (parent name fd facts bytes) row
      (if bytes
          (logical-slot-imported-check-held-file parent name fd facts bytes)
          (unless (and (equal facts (physical-image-file-facts fd))
                       (equal facts (physical-image-file-facts parent name)))
            (error "imported checkpoint directory changed")))))
  t)

(defun call-with-logical-slot-imported-startup-checkpoint (root image root-check function)
  ;; Observe current primary control before WAL opens, under startup quiescence.
  ;; ROOT and ROOT-CHECK identify the held PGDATA root; ROOT-CHECK must reject
  ;; lost authority.  Keep the named inputs open through FUNCTION, which receives
  ;; copied checkpoint facts and a check closure.  The closure is valid only on
  ;; this thread during the callback and latches failures.  Checks before and after
  ;; the callback reject changed inputs or authority.  This observation grants no
  ;; startup admission and does not prove a complete retained WAL archive.
  (physical-adoption-check-startup-quiescence)
  (funcall root-check)
  (let* ((facts (logical-slot-copy-check-source-proof (copy-seq image)))
         (root-facts (physical-image-file-facts root))
         (named-root (copy-seq (namestring (uiop:ensure-directory-pathname *pgdata*))))
         (thread sb-thread:*current-thread*) (held nil) (active nil) (failed nil)
         (fields nil) (control-image nil) (history-image nil) (record-image nil)
         (wal-directory nil))
    (labels ((check-root ()
               (unless (eq thread sb-thread:*current-thread*)
                 (error "imported checkpoint observation changed thread"))
               (physical-adoption-check-startup-quiescence)
               (funcall root-check)
               (unless (and (equal named-root
                                   (namestring (uiop:ensure-directory-pathname *pgdata*)))
                            (equal root-facts (physical-image-file-facts root))
                            (equal root-facts (physical-image-file-facts -100 named-root)))
                 (error "imported checkpoint observation changed its named root"))
               (logical-slot-imported-startup-check-certificate-root facts root-facts))
             (check ()
               (unless (and active (eq thread sb-thread:*current-thread*))
                 (error "imported checkpoint observation ended or changed thread"))
               (when failed (error "imported checkpoint observation already failed"))
               (handler-case
                   (progn
                     (check-root)
                     (logical-slot-imported-startup-check-checkpoint-inputs held)
                     t)
                 (error (condition) (setf failed t) (error condition))))
             (hold (parent name directory-p maximum next)
               (check-root)
               (call-with-logical-slot-copy-source-proof-fd
                parent name directory-p
                (lambda (fd file-facts)
                  (let* ((bytes (and maximum
                                     (logical-slot-copy-import-control-read-fd fd maximum)))
                         (row (list parent (copy-seq name) fd file-facts bytes)))
                    (when bytes
                      (logical-slot-imported-check-held-file parent name fd file-facts bytes))
                    (sb-sys:without-interrupts
                      (unwind-protect
                           (progn
                             (push row held)
                             (sb-sys:with-local-interrupts (funcall next fd bytes)))
                        (when (eq row (first held)) (pop held))))))))
             (with-history (path consume)
               (hold root "pg_wal" t nil
                     (lambda (directory bytes)
                       (declare (ignore bytes))
                       (setf wal-directory directory)
                       (hold directory (file-namestring path) nil
                             +physical-store-history-maximum-bytes+
                             (lambda (fd image)
                               (declare (ignore fd))
                               (setf history-image image)
                               (funcall consume image))))))
             (with-record (path snapshot consume)
               (hold wal-directory (file-namestring path) nil +wal-segment-size+
                     (lambda (fd bytes)
                       (declare (ignore fd))
                       (let ((offset (mod (getf snapshot :start) +wal-segment-size+))
                             (count (wal-checkpoint-record-size snapshot)))
                         (unless (<= (+ offset count) (length bytes))
                           (error "child checkpoint is truncated"))
                         (setf record-image (subseq bytes offset (+ offset count)))
                         (funcall consume (copy-seq record-image))))))
             (finish (snapshot history metadata)
               (logical-slot-imported-startup-check-child-binding facts fields history)
               (unwind-protect
                    (progn
                      (setf active t)
                      (check)
                      (multiple-value-prog1
                          (funcall
                           function
                           (logical-slot-copy-clone-facts
                            (list :system-id (getf metadata :system-id)
                                  :timeline (getf snapshot :timeline) :durable (second fields)
                                  :control control-image :history history-image :checkpoint record-image
                                  :root-facts root-facts
                                  :inputs (mapcar (lambda (row) (list (second row) (fourth row)))
                                                  (reverse held))))
                           #'check)
                        (check)))
                 (setf active nil))))
      (check-root)
      (multiple-value-prog1
          (hold root "global" t nil
                (lambda (global bytes)
                  (declare (ignore bytes))
                  (hold global "pg_wal_control" nil (1+ +wal-control-maximum-bytes+)
                        (lambda (fd bytes)
                          (declare (ignore fd))
                          (setf control-image bytes
                                fields (logical-slot-copy-control-fields bytes))
                          (%physical-promotion-check-child-checkpoint
                           (fourth fields) (fifth fields) (lambda () (values-list fields))
                           #'with-history #'with-record #'finish)))))
        (check-root)))))
