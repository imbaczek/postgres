(in-package #:pg)

;; Both archive histories are the frozen original parent history for [D,F).
;; The record also keeps the original child history for descendant and seam
;; checks.  Do not attach this parent archive to a child or current local-first
;; cursor.  A future provider must read the origin archive alone and prove
;; continuation separately.

(defun call-with-logical-slot-imported-origin-archive
    (record facts check under-lock directory input reader anchors function)
  (multiple-value-bind (token reference identity parent-history)
      (sb-thread:with-mutex (*wal-lock*)
        (logical-slot-imported-host-origin-call record :selection))
    (let ((archive nil) (active t) (thread sb-thread:*current-thread*) (mutex *wal-lock*))
      (labels ((guard ()
                 (assert (sb-thread:holding-mutex-p *wal-lock*))
                 (unless (and active (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*)
                              (logical-slot-copy-committed-image-equal-p
                               reference (logical-wal-archive-reference archive))
                              (logical-slot-copy-committed-image-equal-p
                               identity (logical-wal-archive-identity archive))
                              (logical-slot-imported-history-equal-p parent-history (logical-wal-archive-history archive))
                              (logical-slot-imported-history-equal-p
                               parent-history (logical-wal-archive-parent-history archive)))
                   (error "imported origin archive lost its private selection"))
                 (funcall under-lock)
                 (funcall anchors)
                 (logical-slot-imported-host-origin-call record :check-root))
               (check-binding ()
                 (let ((binding (funcall check t)))
                   (sb-thread:with-mutex (*wal-lock*)
                     (logical-slot-imported-host-origin-call record :binding binding)
                     (guard)))))
        (setf archive (%make-logical-wal-archive
                       :directory directory :input input :reader reader
                       :identity (copy-tree identity) :reference (copy-tree reference)
                       :history (mapcar #'copy-timeline-history-entry parent-history)
                       :parent-history (mapcar #'copy-timeline-history-entry parent-history)
                       :minimum-end (getf reference :end) :guard #'guard))
        (unwind-protect
             (call-with-physical-retained-root-reader
              (lambda ()
                (check-binding)
                (let ((index (logical-wal-archive-read-index archive)))
                  (sb-thread:with-mutex (*wal-lock*)
                    (guard)
                    (setf (logical-wal-archive-index archive) index)))
                (logical-slot-copy-source-proof-scan-archive facts input reader)
                (logical-slot-copy-source-proof-check-archive-prefix facts input reader)
                (check-binding)
                (multiple-value-prog1 (funcall function archive)
                  (check-binding)))
              token directory identity (getf reference :root-id))
          (sb-sys:without-interrupts
            (setf active nil (logical-wal-archive-index archive) nil)
            (logical-wal-archive-clear-cache archive)))))))

;; Reconcile before allowing startup GC or readiness.  This is a private
;; restoration path; production integration and admission are not yet present.
;; Live checks bracket the pure D..F replay, whose simulated globals and raw FD
;; reader must not invoke WAL or live owner checks inside replay.

(defun call-with-logical-slot-imported-host-origin (owner context function)
  (unless (logical-slot-imported-host-startup-owner-p owner)
    (error "host origin restoration requires its typed startup observation"))
  (call-with-logical-slot-imported-proof-files
   owner context
   (lambda (ignored check under-lock)
     (declare (ignore ignored))
     (call-with-logical-slot-imported-archive-input
      context
      (lambda (directory input reader anchors)
        (let* ((binding (funcall check t))
               (facts (logical-slot-copy-check-source-proof (first binding)))
               (record (logical-slot-imported-host-origin-record owner context directory binding)))
          (multiple-value-bind (token reference identity parent-history)
              (sb-thread:with-mutex (*wal-lock*)
                (logical-slot-imported-host-origin-call record :selection))
            (physical-retained-root-reconcile
             token directory identity parent-history
             (lambda ()
               (let ((observed (funcall check t)))
                 (funcall anchors)
                 (sb-thread:with-mutex (*wal-lock*)
                   (logical-slot-imported-host-origin-call record :binding observed))
                 (getf reference :root-id)))
             :reader reader))
          (call-with-logical-slot-imported-origin-archive
           record facts check under-lock directory input reader anchors function)))))))
