(require :asdf)
(let ((source (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*)))))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg"))
(in-package #:pg)

(defun native-descendant-hide-copy (function &optional (seed-fence t) (restore-on-success t))
  (if seed-fence
      (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
      (assert (zerop (hash-table-count *logical-slot-continuation-fenced-roots*))))
  (let ((stash (uiop:ensure-directory-pathname (uiop:getenv "PG_NATIVE_DESCENDANT_STASH")))
        (moved nil) (success nil))
    (unwind-protect
         (progn
           (dolist (name '("logical-slot-copy.install" "logical-slot-copy.in-progress"
                           "logical-slot-copy.descriptor" "logical-slot-copy.base-verified"
                           "logical-slot-copy.base-verified.tmp" "logical_slot_wal"
                           "logical-slot-copy.import" "logical-slot-copy.import.tmp"
                           "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp"
                           "logical-slot-copy.source-proof" "logical-slot-copy.source-proof.tmp"
                           "pg_logical_continuation"))
             (let ((from (merge-pathnames name *pgdata*))
                   (to (merge-pathnames name stash)))
               (when (slot-state-lstat from)
                 (sb-posix:rename (namestring from) (namestring to))
                 (push name moved))))
           (funcall function)
           (setf success t))
      (when (or restore-on-success (not success))
        (dolist (name moved)
          (let ((from (merge-pathnames name stash))
                (to (merge-pathnames name *pgdata*)))
            (when (slot-state-lstat to) (error "promotion replaced a stashed proof name"))
            (sb-posix:rename (namestring from) (namestring to))))))))

(defun native-descendant-backup ()
  (let ((source (uiop:ensure-directory-pathname (uiop:getenv "PG_LISP_FROZEN_SOURCE")))
        (library (uiop:getenv "PG_NATIVE_HOST_LIBRARY_ONLY")))
    (unwind-protect
         (progn
           (sb-posix:setenv "PG_NATIVE_HOST_LIBRARY_ONLY" "1" 1)
           (load (merge-pathnames "t/imported-host-startup.lisp" source)))
      (if library (sb-posix:setenv "PG_NATIVE_HOST_LIBRARY_ONLY" library 1)
          (sb-posix:unsetenv "PG_NATIVE_HOST_LIBRARY_ONLY")))
    (load (merge-pathnames "t/continuation-native-writer.lisp" source))
    (load (merge-pathnames "t/continuation-native-cold.lisp" source)))
  (call-with-live-operation-startup-quiescence
   (lambda (gate)
     (call-with-postmaster-ownership
      (lambda (ownership)
        (check-primary-storage-startup-context gate ownership)
        (wal-mark-primary-startup)
        (wal-init)
        (xact-init)
        (wal-recover)
        (note-primary-startup-replay)
        (physical-promotion-restore-primary-retained)
        (assert (= 2 *wal-insert-timeline*))
        (funcall (symbol-function 'native-host-cold-empty-loader) gate ownership)))))
  (catalog-init)
  (physical-base-backup (uiop:getenv "PG_NATIVE_DESCENDANT_BACKUP") "descendant_fixture")
  (format t "~&NATIVE-DESCENDANT BACKUP PASS timeline=~d~%" *wal-insert-timeline*))

(defun native-descendant-intent ()
  (native-descendant-hide-copy
   (lambda ()
     (physical-bootstrap-check-startup)
     (physical-recovery-open-storage)
     (assert (= 2 (physical-recovery-enable-streaming)))
     (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
       (sb-thread:with-mutex (*physical-replay-pause-lock*)
         (setf *physical-promotion-stopping-p* t *physical-replay-paused-p* nil)))
     (physical-restartpoint-join-for-shutdown)
     (sb-thread:with-mutex (*physical-follow-command-lock*)
       (sb-thread:with-mutex (*reload-run-lock*)
         (with-live-operation-quiescence
           (physical-store-compact *physical-receive-store* *last-replay-lsn*
                                   #'physical-restartpoint-publish)
           (physical-promotion-publish-intent 3))))
     (assert (= 3 (getf *physical-recovery-state* :phase)))
     (assert (= 3 (getf *physical-recovery-state* :promotion-child)))
     (format t "~&NATIVE-DESCENDANT INTENT PASS child=3~%"))))

(defun native-descendant-complete ()
  (native-descendant-hide-copy
   (lambda ()
     (assert (eq :primary-ready (physical-bootstrap-check-startup)))
     (assert (not (recovery-in-progress-p)))
     (assert (= 3 *wal-insert-timeline* *wal-checkpoint-timeline*))
     (assert (not (probe-file (physical-recovery-control-path))))
     (assert (probe-file (merge-pathnames "00000003.history" (wal-directory))))
     (format t "~&NATIVE-DESCENDANT COMPLETE PASS timeline=3 checkpoint=~d end=~d~%"
             *wal-checkpoint-lsn* *wal-durable-lsn*))
   nil nil))

(defun native-descendant-main ()
  (unless (equal "1" (uiop:getenv "PG_NATIVE_HOST_DISPOSABLE"))
    (error "descendant fixture requires an owned disposable namespace"))
  (let* ((*pgdata* (uiop:ensure-directory-pathname (uiop:getenv "PG_NATIVE_PROOF_DATA")))
         (*wal-level* :logical)
         (*startup-installed-configuration* nil)
         (exit-hooks sb-ext:*exit-hooks*))
    (let ((settings (startup-default-settings (namestring *pgdata*))))
      (startup-resolve-auth-files settings *pgdata*)
      (startup-install-resource-capacity settings)
      (setf (getf (gethash "wal_level" settings) :value) "logical"
            *startup-installed-configuration*
            (startup-prepare-installation settings (make-hash-table :test #'equal)
                                          (make-hash-table :test #'equal)
                                          (authentication-read-policy settings))))
    (unwind-protect
         (progn
           (postmaster-own-data-directory "127.0.0.1" 5439)
           (let ((phase (uiop:getenv "PG_NATIVE_DESCENDANT_PHASE")))
             (cond
               ((equal phase "backup") (native-descendant-backup))
               ((equal phase "intent") (native-descendant-intent))
               ((equal phase "complete") (native-descendant-complete))
               (t (error "unknown descendant fixture phase")))))
      (setf *physical-promotion-stopping-p* t)
      (physical-restartpoint-join-for-shutdown)
      (when *primary-wal-writer* (stop-primary-wal-writer))
      (when *physical-receive-store* (physical-store-close *physical-receive-store*))
      (when *clog-stream* (close *clog-stream*))
      (when *wal-output-stream* (close *wal-output-stream*))
      (smgr-close-all)
      (postmaster-remove-pid-file)
      (when *postmaster-data-lock* (close *postmaster-data-lock*))
      (setf sb-ext:*exit-hooks* exit-hooks))))

(handler-case (native-descendant-main)
  (error (condition)
    (format *error-output* "~&NATIVE-DESCENDANT FAILURE ~a~%" condition)
    (sb-ext:exit :code 1)))
