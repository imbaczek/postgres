(in-package #:pg)

(defstruct (logical-slot-continuation-cold-cleanup-execution
            (:constructor %make-logical-slot-continuation-cold-cleanup-execution)
            (:print-function physical-libpq-print-private-object))
  (directory nil :read-only t)
  (guard nil :read-only t)
  (thread sb-thread:*current-thread* :read-only t)
  (active t) (failed nil)
  root rows (pending '(:observe))
  child child-name child-facts child-removed
  file file-entry file-facts file-removed
  (completed 0))

(defvar *logical-slot-continuation-cold-cleanup-test-hook* nil)

(defun logical-slot-continuation-cold-cleanup-check-held (state)
  (let ((directory (logical-slot-continuation-cold-cleanup-execution-directory state))
        (child (logical-slot-continuation-cold-cleanup-execution-child state))
        (file (logical-slot-continuation-cold-cleanup-execution-file state)))
    (when child
      (if (logical-slot-continuation-cold-cleanup-execution-child-removed state)
          (logical-slot-continuation-cold-cleanup-check-unlinked-facts
           (logical-slot-continuation-cold-cleanup-execution-child-facts state)
           (physical-image-file-facts child) t)
          (let* ((name (logical-slot-continuation-cold-cleanup-execution-child-name state))
                 (row (find name (logical-slot-continuation-cold-cleanup-execution-rows state)
                            :key (lambda (entry) (getf entry :name)) :test #'equal)))
            (unless (and row (equal (getf row :directory) (physical-image-file-facts child))
                         (equal (getf row :directory) (physical-image-file-facts directory name)))
              (error "cold cleanup held directory lost its expected binding")))))
    (when file
      (let* ((entry (logical-slot-continuation-cold-cleanup-execution-file-entry state))
             (facts (second entry)) (observed (physical-image-file-facts file)))
        (unless child (error "cold cleanup file has no held directory"))
        (if (logical-slot-continuation-cold-cleanup-execution-file-removed state)
            (progn
              (logical-slot-continuation-cold-cleanup-check-unlinked-facts facts observed nil)
              (when (member (first entry) (logical-slot-copy-import-control-names child 3) :test #'equal)
                (error "cold cleanup unlinked file name reappeared")))
            (unless (and (equal facts observed)
                         (equal facts (physical-image-file-facts child (first entry))))
              (error "cold cleanup held file lost its expected binding")))
        (unless (equalp (third entry)
                        (logical-slot-copy-import-control-read-fd file +slot-state-maximum-bytes+))
          (error "cold cleanup held file bytes changed"))))
    (when (and child (logical-slot-continuation-cold-cleanup-execution-child-removed state))
      (when (member (logical-slot-continuation-cold-cleanup-execution-child-name state)
                    (logical-slot-continuation-cold-raw-names directory) :test #'equal)
        (error "cold cleanup removed directory name reappeared"))))
  t)

(defun logical-slot-continuation-cold-cleanup-check-execution (state)
  (unless (and (logical-slot-continuation-cold-cleanup-execution-active state)
               (not (logical-slot-continuation-cold-cleanup-execution-failed state))
               (eq sb-thread:*current-thread*
                   (logical-slot-continuation-cold-cleanup-execution-thread state)))
    (error "cold cleanup execution expired, failed or changed thread"))
  (let* ((directory (logical-slot-continuation-cold-cleanup-execution-directory state))
         (guard (logical-slot-continuation-cold-cleanup-execution-guard state))
         (root nil) (rows nil))
    (funcall guard)
    (setf root (physical-image-file-facts directory)
          rows (logical-slot-continuation-cold-read-raw-inventory directory guard))
    (unless (equal root (physical-image-file-facts directory))
      (error "cold cleanup root changed during its complete observation"))
    (multiple-value-bind (next-root next-rows)
        (logical-slot-continuation-cold-cleanup-next-ledger
         (logical-slot-continuation-cold-cleanup-execution-root state)
         (logical-slot-continuation-cold-cleanup-execution-rows state)
         (logical-slot-continuation-cold-cleanup-execution-pending state) root rows)
      (sb-sys:without-interrupts
        (setf (logical-slot-continuation-cold-cleanup-execution-root state) next-root
              (logical-slot-continuation-cold-cleanup-execution-rows state) next-rows
              (logical-slot-continuation-cold-cleanup-execution-pending state) '(:observe))))
    (logical-slot-continuation-cold-cleanup-check-held state)
    (funcall guard))
  t)

(defun logical-slot-continuation-cold-cleanup-close-owned (state file-p validate)
  (declare (notinline sb-posix:close))
  (sb-sys:without-interrupts
    (when validate (logical-slot-continuation-cold-cleanup-check-held state))
    (let ((fd (if file-p (logical-slot-continuation-cold-cleanup-execution-file state)
                  (logical-slot-continuation-cold-cleanup-execution-child state)))
          (facts (if file-p (logical-slot-continuation-cold-cleanup-execution-file-facts state)
                     (logical-slot-continuation-cold-cleanup-execution-child-facts state))))
      (unless fd (error "cold cleanup close has no owned descriptor"))
      (if file-p (setf (logical-slot-continuation-cold-cleanup-execution-file state) nil)
          (setf (logical-slot-continuation-cold-cleanup-execution-child state) nil))
      (let ((current (physical-image-file-facts fd)))
        (unless (and (= (first facts) (first current))
                     (= (second facts) (second current))
                     (= (ninth facts) (ninth current)))
          (error "cold cleanup descriptor ownership was lost before close")))
      (sb-posix:close fd))))

(defun logical-slot-continuation-cold-cleanup-drain-execution (state)
  (sb-sys:without-interrupts
    (dolist (file-p '(t nil))
      (when (if file-p (logical-slot-continuation-cold-cleanup-execution-file state)
                (logical-slot-continuation-cold-cleanup-execution-child state))
        (handler-case
            (logical-slot-continuation-cold-cleanup-close-owned state file-p nil)
          (error (condition)
            (report-query-temp-cleanup-failure "cold cleanup descriptor" condition))))))
  nil)
