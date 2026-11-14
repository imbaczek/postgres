(in-package #:pg)

(defun cold-scratch-native-kill (mode)
  (format t "~&COLD-SCRATCH-SIGKILL ~a~%" mode)
  (finish-output)
  (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
  (error "SIGKILL returned"))

(defun cold-scratch-native-with-context (root function)
  (let ((fd (physical-backup-open-directory root)))
    (unwind-protect
         (call-with-physical-backup-image-directory
          fd (lambda (path)
               (let ((*pgdata* path))
                 (funcall function *physical-image-context*))))
      (sb-posix:close fd))))

(defun cold-scratch-native-stage (mode context)
  (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
         (history (timeline-history-path 2 *pgdata*))
         (bytes (timeline-encode-history 2 1 64 "crash" nil)))
    (cond
      ((member mode '("prefix-before-install" "prefix-after-install") :test #'equal)
       (let ((*wal-timeline-test-hook*
               (lambda (point)
                 (when (eq point (if (equal mode "prefix-before-install")
                                     :after-prefix-fsync :after-child-rename))
                   (cold-scratch-native-kill mode)))))
         (wal-install-timeline-prefix
          1 2 64 (make-array 64 :element-type '(unsigned-byte 8) :initial-element 29) :timeline directory)))
      ((member mode '("history-before-install" "history-after-install") :test #'equal)
       (let ((*timeline-history-test-hook*
               (lambda (point)
                 (when (eq point (if (equal mode "history-before-install") :after-file-fsync :after-install))
                   (cold-scratch-native-kill mode)))))
         (timeline-publish-history-bytes history bytes :directory-fd directory)))
      (t
       (timeline-publish-history-bytes history bytes :directory-fd directory)
       (dotimes (index 2)
         (sb-posix:link (namestring history)
                        (namestring (merge-pathnames (format nil ".history-~6,'0d" index)
                                                     (wal-directory)))))
       (let ((unlink (symbol-function 'port-unlinkat))
             (sync (symbol-function 'sb-posix:fsync)))
         (unwind-protect
              (progn
                (setf (symbol-function 'port-unlinkat)
                      (lambda (fd name flags)
                        (prog1 (funcall unlink fd name flags)
                          (when (and (= fd directory) (equal mode "cleanup-after-unlink"))
                            (cold-scratch-native-kill mode)))))
                (setf (symbol-function 'sb-posix:fsync)
                      (lambda (fd)
                        (prog1 (funcall sync fd)
                          (when (and (= fd directory) (equal mode "cleanup-after-directory-sync"))
                            (cold-scratch-native-kill mode)))))
                (logical-slot-copy-cold-clean-wal-scratch context (lambda () nil) (lambda () nil) 32))
           (setf (symbol-function 'port-unlinkat) unlink
                 (symbol-function 'sb-posix:fsync) sync)))))
    (error "native scratch cut was not reached")))

(defun cold-scratch-native-main ()
  (let ((mode (uiop:getenv "PG_LISP_SCRATCH_TEST_MODE"))
        (root (uiop:ensure-directory-pathname (uiop:getenv "PG_LISP_SCRATCH_TEST_ROOT"))))
    (cold-scratch-native-with-context
     root
     (lambda (context)
       (if (equal mode "recover")
           (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (format t "~&COLD-SCRATCH-REMOVED ~d~%"
                     (logical-slot-copy-cold-clean-wal-scratch context (lambda () nil) (lambda () nil) 32))
             (assert (zerop (logical-slot-copy-cold-clean-wal-scratch
                             context (lambda () nil) (lambda () nil) 32)))
             (assert (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (assert (and (zerop (hash-table-count *relation-streams*)) (null *clog-stream*) (not *wal-ready*)))
             (format t "~&COLD-SCRATCH-RECOVER-PASS~%"))
           (cold-scratch-native-stage mode context))))))
