;;;; exec_vacuum.lisp -- VACUUM command entry point.

(in-package #:pg)

(defun exec-vacuum (stream ast)
  (when (backend-explicit *backend*)
    (set-backend-state *backend* :aborted)
    (ereport "25001" "VACUUM cannot run inside a transaction block"))
  ;; VACUUM has no user transaction or statement snapshot, but it must still
  ;; participate in wraparound admission and keep CLOG stable until it finishes.
  (prepare-xid-wraparound-for-statement *backend*)
  (with-statement-snapshot
      (snapshot
       (lambda (registration)
         (begin-statement-snapshot *backend*
                                   (backend-command-id *backend*)
                                   registration)))
    (let ((table (with-write-lock
                   (resolve-table-or-error (getf ast :table)))))
      (if (getf ast :full-p)
          (with-schema-write-lock
            (vacuum-full-table table))
          (vacuum-table table))
      (send-command-complete stream "VACUUM"))))
