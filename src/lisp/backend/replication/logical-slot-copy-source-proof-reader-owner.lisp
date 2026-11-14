(in-package #:pg)

(defstruct (logical-slot-copy-source-reader-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-copy-source-reader-owner)
            (:print-function physical-libpq-print-private-object))
  (ownership nil :read-only t))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-source-reader-owner))
  (logical-slot-copy-check-import-locks)
  (physical-adoption-check-startup-quiescence)
  (unless (and (sb-thread:holding-mutex-p *physical-follow-generation-lock*)
               (null *physical-recovery-state*) (null *catalog*) (null *wal-output-stream*)
               (null *clog-stream*) (zerop *n-backends*)
               (zerop (hash-table-count *relation-streams*)))
    (error "permanent source reader requires unopened cold storage"))
  (logical-slot-copy-check-postmaster-owner
   owner (logical-slot-copy-source-reader-owner-ownership owner))
  (physical-adoption-check-startup)
  owner)

(defmethod physical-adoption-check-owner ((owner logical-slot-copy-source-reader-owner))
  (error "permanent source reader does not authorize offline adoption"))

(defun call-with-logical-slot-copy-source-reader-owner (function)
  ;; This cold owner permits observation, not offline adoption or promotion.
  (call-with-postmaster-ownership
   (lambda (ownership)
     (let ((owner (apply #'%make-logical-slot-copy-source-reader-owner
                         (logical-slot-copy-owner-arguments ownership))))
       (unwind-protect
            (progn (physical-directory-check-owner owner) (funcall function owner))
         (setf (physical-adoption-owner-active-p owner) nil))))))

(defun call-with-logical-slot-copy-source-reader-reference (function)
  ;; Protect the archive root only for this scope.  DIRECTORY is the canonical
  ;; registry key; INPUT and READER use the held archive FD for actual I/O.
  (let ((token (gensym "SOURCE-PROOF-READER")) (reference nil)
                                               (registered nil) (active t) (complete nil))
    (labels ((register (directory input identity id history reader)
               (unless (and active (not registered))
                 (error "permanent source reference scope ended or was already registered"))
               (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                 (let ((payload (physical-retained-inventory-load
                                 input id identity history :reader reader)))
                   (setf reference
                         (physical-retained-root-make-reference
                          directory identity id (getf payload :entries) (getf payload :base-overlap)))
                   (sb-sys:without-interrupts
                     (physical-retained-root-install token reference)
                     (setf registered t))))))
      (unwind-protect
           (multiple-value-prog1 (funcall function #'register) (setf complete t))
        (setf active nil)
        (when registered
          (let ((changed nil))
            (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
              (if (eq reference (gethash token *physical-retained-root-references*))
                  (physical-retained-root-install token nil)
                  (setf changed t)))
            (when changed
              (if complete
                  (error "permanent source reader reference changed during cleanup")
                  (report-query-temp-cleanup-failure
                   "permanent source reader reference"
                   (make-condition 'simple-error :format-control "reader reference changed"))))))))))
