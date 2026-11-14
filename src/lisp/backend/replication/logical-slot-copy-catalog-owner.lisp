(in-package #:pg)

(defstruct (logical-slot-copy-catalog-owner
            (:include logical-slot-copy-committed-owner)
            (:constructor %make-logical-slot-copy-catalog-owner)
            (:print-function physical-libpq-print-private-object)))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-catalog-owner))
  (call-next-method)
  (unless (and (eq :cold (logical-slot-copy-committed-owner-mode owner))
               (not *wal-ready*) (null *physical-recovery-state*) (null *physical-receive-store*)
               (null *catalog*) (null *clog-stream*) (null *wal-output-stream*)
               (zerop (hash-table-count *relation-streams*)))
    (error "catalog restoration requires unopened cold storage"))
  owner)

(defstruct (logical-slot-copy-catalog-attempt
            (:constructor %make-logical-slot-copy-catalog-attempt)
            (:print-function physical-libpq-print-private-object))
  (owner nil :read-only t) (context nil :read-only t)
  (binding nil :read-only t) (root nil :read-only t) (global nil :read-only t)
  (source-check nil :read-only t) (mark-started nil :read-only t)
  (file-operation nil :read-only t) (failure-operation nil :read-only t)
  seal operation-seal phase old-fd temp-fd temp-facts target-facts global-changed
  temp-synced old-synced file-synced directory-synced active failed)

(defvar *logical-slot-copy-catalog-attempts* nil)

(declaim (ftype function logical-slot-copy-catalog-artifacts))

(declaim (ftype function logical-slot-copy-catalog-check-files))

(defun logical-slot-copy-catalog-image (attempt)
  (let ((binding (logical-slot-copy-catalog-attempt-binding attempt)))
    (list binding (logical-slot-copy-descriptor-encode (getf binding :descriptor))
          (logical-slot-copy-catalog-attempt-source-check attempt)
          (logical-slot-copy-catalog-attempt-mark-started attempt)
          (logical-slot-copy-catalog-attempt-file-operation attempt)
          (logical-slot-copy-catalog-attempt-failure-operation attempt))))

(defun logical-slot-copy-catalog-operation-image (attempt)
  (list (logical-slot-copy-catalog-attempt-phase attempt)
        (logical-slot-copy-catalog-attempt-old-fd attempt)
        (logical-slot-copy-catalog-attempt-temp-fd attempt)
        (logical-slot-copy-catalog-attempt-temp-facts attempt)
        (logical-slot-copy-catalog-attempt-target-facts attempt)
        (logical-slot-copy-catalog-attempt-global-changed attempt)
        (logical-slot-copy-catalog-attempt-temp-synced attempt)
        (logical-slot-copy-catalog-attempt-old-synced attempt)
        (logical-slot-copy-catalog-attempt-file-synced attempt)
        (logical-slot-copy-catalog-attempt-directory-synced attempt)))

(defun logical-slot-copy-catalog-seal-operation (attempt)
  (setf (logical-slot-copy-catalog-attempt-operation-seal attempt)
        (logical-slot-copy-import-state-image (logical-slot-copy-catalog-operation-image attempt))))

(defun logical-slot-copy-catalog-record (attempt)
  (unless (and (logical-slot-copy-catalog-attempt-p attempt)
               (eq attempt (cdr (assoc (logical-slot-copy-catalog-attempt-owner attempt)
                                       *logical-slot-copy-catalog-attempts* :test #'eq)))
               (logical-slot-copy-catalog-attempt-active attempt)
               (not (logical-slot-copy-catalog-attempt-failed attempt))
               (functionp (logical-slot-copy-catalog-attempt-failure-operation attempt))
               (not (funcall (logical-slot-copy-catalog-attempt-failure-operation attempt) :failedp attempt)))
    (error "catalog restoration scope ended or failed"))
  (let ((owner (logical-slot-copy-catalog-attempt-owner attempt))
        (context (logical-slot-copy-catalog-attempt-context attempt)))
    (logical-slot-copy-check-input owner context)
    (unless (and (eql (logical-slot-copy-catalog-attempt-root attempt)
                      (physical-image-context-root-fd context))
                 (eql (logical-slot-copy-catalog-attempt-global attempt)
                      (cdr (assoc "global" (physical-image-context-children context) :test #'equal)))
                 (logical-slot-copy-committed-image-equal-p
                  (logical-slot-copy-catalog-attempt-seal attempt)
                  (logical-slot-copy-catalog-image attempt))
                 (logical-slot-copy-committed-image-equal-p
                  (logical-slot-copy-catalog-attempt-operation-seal attempt)
                  (logical-slot-copy-catalog-operation-image attempt)))
      (error "catalog restoration binding changed")))
  attempt)

(defun logical-slot-copy-catalog-check-observations (attempt)
  (let ((binding (logical-slot-copy-catalog-attempt-binding attempt))
        (owner (logical-slot-copy-catalog-attempt-owner attempt))
        (context (logical-slot-copy-catalog-attempt-context attempt)))
    (logical-slot-copy-import-control-fences context)
    (unless (and (eql (getf binding :restartpoints)
                      (cdr (assoc "pg_physical_restartpoints"
                                  (physical-image-context-children context) :test #'equal)))
                 (equal (getf binding :restartpoint-directory-facts)
                        (physical-image-file-facts (getf binding :restartpoints)))
                 (equal (getf binding :restartpoint-directory-facts)
                        (physical-image-file-facts (physical-image-context-root-fd context)
                                                   "pg_physical_restartpoints")))
      (error "catalog restoration restartpoint directory changed"))
    (dolist (row (append (getf binding :inputs) (getf binding :restartpoint-observations)))
      (logical-slot-copy-input-check-observations owner context (list row))
      (unless (equal row (logical-slot-copy-file-row context (first row) (second row)))
        (error "catalog restoration input or restartpoint changed")))))

(defun logical-slot-copy-catalog-revoke (attempt)
  (when (and (logical-slot-copy-catalog-attempt-p attempt)
             (functionp (logical-slot-copy-catalog-attempt-failure-operation attempt)))
    (funcall (logical-slot-copy-catalog-attempt-failure-operation attempt) :revoke attempt)))

(defmacro with-logical-slot-copy-catalog-guard ((attempt &key (files t)) &body body)
  (let ((record (gensym "ATTEMPT-")) (complete (gensym "COMPLETE-")))
    `(let ((,record ,attempt) (,complete nil))
       (unwind-protect
            (multiple-value-prog1
                (progn
                  ,(if files `(logical-slot-copy-catalog-check ,record)
                       `(logical-slot-copy-catalog-record ,record))
                  ,@body)
              (setf ,complete t))
         (unless ,complete (logical-slot-copy-catalog-revoke ,record))))))

(defun logical-slot-copy-catalog-check-source (attempt store)
  (with-logical-slot-copy-catalog-guard (attempt :files nil)
    (logical-slot-copy-catalog-check-observations attempt)
    (let* ((b (logical-slot-copy-catalog-attempt-binding attempt))
           (actual (multiple-value-list
                    (logical-slot-copy-check-retained-source
                     (getf b :certificate) (getf b :evidence) (getf b :descriptor)
                     (getf b :manifest) (getf b :control) (getf b :plan) (getf b :state)
                     (getf b :metadata) (getf b :prefix) (getf b :generation) store
                     (lambda () (logical-slot-copy-catalog-artifacts
                                 b (logical-slot-copy-catalog-attempt-owner attempt)
                                 (logical-slot-copy-catalog-attempt-context attempt)))))))
      (unless (logical-slot-copy-committed-image-equal-p (getf b :source-proof) actual)
        (error "catalog restoration retained source changed")))
    (logical-slot-copy-catalog-check-observations attempt)
    t))

(defun logical-slot-copy-catalog-check (attempt &key (source t))
  (with-logical-slot-copy-catalog-guard (attempt :files nil)
    (when source (funcall (logical-slot-copy-catalog-attempt-source-check attempt)))
    (logical-slot-copy-catalog-check-observations attempt)
    (logical-slot-copy-catalog-check-files attempt)
    t))

(defun logical-slot-copy-catalog-point (point attempt)
  (logical-slot-copy-verification-test-point point (logical-slot-copy-catalog-attempt-context attempt)))
