(in-package #:pg)

(defun logical-slot-continuation-cold-cleanup-open-input (parent name directory-p)
  (declare (notinline sb-posix:close))
  (let ((fd (physical-image-openat
             parent name (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                 +postmaster-o-cloexec+ (if directory-p sb-posix:o-directory 0)) 0))
        (complete nil))
    (when (minusp fd) (error "cannot open cold cleanup resource"))
    (unwind-protect
         (multiple-value-prog1 (values fd (physical-image-file-facts fd))
           (setf complete t))
      (unless complete
        (handler-case (sb-posix:close fd)
          (error (condition)
            (report-query-temp-cleanup-failure "new cold cleanup input" condition)))))))

(defun logical-slot-continuation-cold-cleanup-open-step (state step)
  (destructuring-bind (operation name &optional file) step
    (let* ((directory (logical-slot-continuation-cold-cleanup-execution-directory state))
           (child (logical-slot-continuation-cold-cleanup-execution-child state))
           (row (find name (logical-slot-continuation-cold-cleanup-execution-rows state)
                      :key (lambda (entry) (getf entry :name)) :test #'equal)))
      (unless row (error "cold cleanup open has no expected directory"))
      (ecase operation
        (:open-directory
         (when (or child (logical-slot-continuation-cold-cleanup-execution-file state))
           (error "cold cleanup already owns an open resource"))
         (multiple-value-bind (fd facts)
             (logical-slot-continuation-cold-cleanup-open-input directory name t)
           (setf (logical-slot-continuation-cold-cleanup-execution-child state) fd
                 (logical-slot-continuation-cold-cleanup-execution-child-name state) name
                 (logical-slot-continuation-cold-cleanup-execution-child-facts state) facts
                 (logical-slot-continuation-cold-cleanup-execution-child-removed state) nil)))
        (:open-file
         (unless (and child (equal name (logical-slot-continuation-cold-cleanup-execution-child-name state))
                      (not (logical-slot-continuation-cold-cleanup-execution-file state)))
           (error "cold cleanup file open changed its owned directory"))
         (let ((entry (logical-slot-continuation-copy (assoc file (getf row :files) :test #'equal))))
           (unless entry (error "cold cleanup file open has no captured entry"))
           (multiple-value-bind (fd facts)
               (logical-slot-continuation-cold-cleanup-open-input child file nil)
             (setf (logical-slot-continuation-cold-cleanup-execution-file state) fd
                   (logical-slot-continuation-cold-cleanup-execution-file-entry state) entry
                   (logical-slot-continuation-cold-cleanup-execution-file-facts state) facts
                   (logical-slot-continuation-cold-cleanup-execution-file-removed state) nil))))))))

(defun logical-slot-continuation-cold-cleanup-mutate-step (state step)
  (destructuring-bind (operation name &optional target) step
    (let ((directory (logical-slot-continuation-cold-cleanup-execution-directory state))
          (child (logical-slot-continuation-cold-cleanup-execution-child state)))
      (unless (and child (equal name (logical-slot-continuation-cold-cleanup-execution-child-name state))
                   (not (logical-slot-continuation-cold-cleanup-execution-child-removed state)))
        (error "cold cleanup mutation has no live named directory"))
      (ecase operation
        (:rename
         (when (logical-slot-continuation-cold-cleanup-execution-file state)
           (error "cold cleanup cannot rename with an open file"))
         (slot-state-rename-no-replace directory name directory target)
         (setf (logical-slot-continuation-cold-cleanup-execution-child-name state) target))
        (:unlink
         (unless (and (logical-slot-continuation-cold-cleanup-execution-file state)
                      (equal target
                             (first (logical-slot-continuation-cold-cleanup-execution-file-entry state)))
                      (not (logical-slot-continuation-cold-cleanup-execution-file-removed state)))
           (error "cold cleanup unlink has no held captured file"))
         (slot-state-unlink-exact child target)
         (setf (logical-slot-continuation-cold-cleanup-execution-file-removed state) t))
        (:rmdir
         (when (or (logical-slot-continuation-cold-cleanup-execution-file state)
                   (logical-slot-copy-import-control-names child 1))
           (error "cold cleanup directory is not empty and closed to files"))
         (slot-state-unlink-exact directory name t)
         (setf (logical-slot-continuation-cold-cleanup-execution-child-removed state) t)))
      (setf (logical-slot-continuation-cold-cleanup-execution-pending state) step))))

(defun logical-slot-continuation-cold-cleanup-close-step (state step)
  (declare (notinline sb-posix:close))
  (destructuring-bind (operation name &optional file) step
    (unless (equal name (logical-slot-continuation-cold-cleanup-execution-child-name state))
      (error "cold cleanup close changed its directory"))
    (ecase operation
      (:close-file
       (let ((fd (logical-slot-continuation-cold-cleanup-execution-file state)))
         (unless (and fd (logical-slot-continuation-cold-cleanup-execution-file-removed state)
                      (equal file (first (logical-slot-continuation-cold-cleanup-execution-file-entry state))))
           (error "cold cleanup close has no removed file"))
         (logical-slot-continuation-cold-cleanup-close-owned state t t)))
      (:close-directory
       (let ((fd (logical-slot-continuation-cold-cleanup-execution-child state)))
         (unless (and fd (not (logical-slot-continuation-cold-cleanup-execution-file state)))
           (error "cold cleanup directory close has no owned descriptor"))
         (logical-slot-continuation-cold-cleanup-close-owned state nil t))))))

(defun logical-slot-continuation-cold-cleanup-run-step (state step)
  (declare (notinline sb-posix:fsync))
  (with-logical-slot-continuation-observation-check
      ((logical-slot-continuation-cold-cleanup-execution-active state)
       (logical-slot-continuation-cold-cleanup-execution-failed state)
       (logical-slot-continuation-cold-cleanup-execution-thread state) "cold cleanup execution")
    (flet ((point (phase)
             (when *logical-slot-continuation-cold-cleanup-test-hook*
               (funcall *logical-slot-continuation-cold-cleanup-test-hook*
                        phase (logical-slot-continuation-copy step)))))
      (logical-slot-continuation-cold-cleanup-check-execution state)
      (point :before)
      (logical-slot-continuation-cold-cleanup-check-execution state)
      (sb-sys:without-interrupts
        (case (first step)
          ((:open-directory :open-file) (logical-slot-continuation-cold-cleanup-open-step state step))
          ((:rename :unlink :rmdir) (logical-slot-continuation-cold-cleanup-mutate-step state step))
          ((:close-directory :close-file) (logical-slot-continuation-cold-cleanup-close-step state step))
          (:sync-directory
           (unless (and (equal (second step)
                               (logical-slot-continuation-cold-cleanup-execution-child-name state))
                        (logical-slot-continuation-cold-cleanup-execution-child state)
                        (not (logical-slot-continuation-cold-cleanup-execution-child-removed state)))
             (error "cold cleanup sync has no live directory"))
           (sb-posix:fsync (logical-slot-continuation-cold-cleanup-execution-child state)))
          (:sync-root (sb-posix:fsync (logical-slot-continuation-cold-cleanup-execution-directory state)))
          (t (error "cold cleanup has an unknown planned step"))))
      (logical-slot-continuation-cold-cleanup-check-execution state)
      (point :after)
      (logical-slot-continuation-cold-cleanup-check-execution state)
      (incf (logical-slot-continuation-cold-cleanup-execution-completed state))))
  t)

(defun logical-slot-continuation-cold-execute-cleanup-plan (directory guard plan)
  (unless (functionp guard) (error "cold cleanup requires its live ownership guard"))
  (let* ((plan (logical-slot-continuation-copy plan))
         (state (%make-logical-slot-continuation-cold-cleanup-execution
                 :directory directory :guard guard :root (copy-list (getf plan :root))
                 :rows (logical-slot-continuation-copy (getf plan :ledger)))))
    (unwind-protect
         (with-logical-slot-continuation-observation-check
             ((logical-slot-continuation-cold-cleanup-execution-active state)
              (logical-slot-continuation-cold-cleanup-execution-failed state)
              (logical-slot-continuation-cold-cleanup-execution-thread state) "cold cleanup execution")
           (logical-slot-continuation-cold-cleanup-check-execution state)
           (dolist (action (getf plan :actions))
             (let ((row (find (getf action :name)
                              (logical-slot-continuation-cold-cleanup-execution-rows state)
                              :key (lambda (entry) (getf entry :name)) :test #'equal)))
               (unless (equalp row (getf action :before))
                 (error "cold cleanup action differs from its original captured row")))
             (dolist (step (logical-slot-continuation-cold-cleanup-action-steps action))
               (logical-slot-continuation-cold-cleanup-run-step state step)))
           (logical-slot-continuation-cold-cleanup-check-execution state)
           (when (or (logical-slot-continuation-cold-cleanup-execution-child state)
                     (logical-slot-continuation-cold-cleanup-execution-file state))
             (error "cold cleanup did not close all owned resources"))
           (let ((rows (logical-slot-continuation-cold-cleanup-execution-rows state)))
             (unless (equalp (getf plan :retained)
                             (mapcar (lambda (row)
                                       (list (getf row :name)
                                             (third (assoc "state" (getf row :files) :test #'equal))))
                                     rows))
               (error "cold cleanup final inventory differs from its retained images"))
             (values (copy-list (logical-slot-continuation-cold-cleanup-execution-root state))
                     (logical-slot-continuation-copy rows))))
      (sb-sys:without-interrupts
        (setf (logical-slot-continuation-cold-cleanup-execution-active state) nil)
        (logical-slot-continuation-cold-cleanup-drain-execution state)))))
