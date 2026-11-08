;;;; Cold pending reconstruction preserves write exclusion and original progress.
(in-package #:pg-physical-tests)

(defun call-with-cold-recovery-image (function &key (streaming t))
  (call-with-cold-promotion-image
   (lambda (root driver)
     (unless streaming
       (let* ((pg::*pgdata* root)
              (pg::*catalog* (pg::load-catalog))
              (manifest (copy-list (pg::recovery-read-manifest root)))
              (pg::*wal-durable-lsn* (getf manifest :end))
              (pg::*wal-insert-timeline* (getf manifest :timeline))
              (files
                (loop for name in (pg::physical-bootstrap-inventory
                                   (pg::physical-manifest-checkpoint-start manifest))
                      for bytes = (lineage-test-bytes (merge-pathnames name root))
                      collect (list name (length bytes)
                                    (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))))
         (setf (getf manifest :files) files)
         (write-recovery-manifest-text root (pg::physical-bootstrap-manifest-text manifest))
         (pg::wal-atomic-write-bytes
          (pg::physical-recovery-control-path)
          (pg::physical-recovery-control-image
           1 (getf manifest :system-id) (getf manifest :timeline)
           (nth-value 1 (pg::recovery-read-manifest root))
           (getf manifest :checkpoint) (getf manifest :end) (getf manifest :next-xid)))
         (dolist (directory (list (pg::physical-recovery-spool-directory)
                                  (pg::physical-recovery-retained-directory)))
           (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))
     (funcall function root driver))
   :pending nil :applied (if streaming 216 152)))

(defun cold-recovery-success-child-form (root marker streaming kind)
  `(let ((pg::*pgdata* ,root))
     (unwind-protect
          (progn
            (pg::postmaster-own-data-directory "127.0.0.1" 5439)
            (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
              (sb-thread:with-mutex (pg::*reload-run-lock*)
                (pg::with-live-operation-quiescence
                  (pg::physical-recovery-prepare)
                  (let ((prepared (getf pg::*physical-recovery-state* :phase)))
                    (pg::physical-recovery-open-storage)
                    (with-open-file (out ,marker :direction :output :if-exists :error)
                      (write
                       (list
                        (sb-posix:getpid) ,kind
                        (list
                         (= prepared ,(if streaming 2 1))
                         (= (getf pg::*physical-recovery-state* :phase) prepared)
                         ,(if streaming
                              '(and pg::*physical-receive-store*
                                (pg::physical-store-check-held-lock pg::*physical-receive-store*))
                              '(null pg::*physical-receive-store*))
                         (and pg::*clog-stream* (open-stream-p pg::*clog-stream*))
                         (not (null pg::*catalog*))
                         pg::*wal-ready*
                         (not pg::*physical-recovery-failed-p*)
                         (and (= 50 pg::*next-xid*)
                              (= ,(if streaming 216 152) pg::*last-replay-lsn*))))
                       :stream out :readably t))))))
            (format t "~%COLD-RECOVERY-SUCCESS-PASS ~s~%" ,streaming))
       (when pg::*physical-receive-store* (pg::physical-store-close pg::*physical-receive-store*))
       (when pg::*clog-stream* (close pg::*clog-stream*))
       (pg::smgr-close-all)
       (pg::postmaster-remove-pid-file))))

(deftest cold-recovery-storage-retains-successful-static-and-streaming-resources
    (call-with-physical-store-directory
     (lambda (cache-root)
       (dolist (streaming '(nil t))
         (testing (if streaming "streaming" "static")
                  (call-with-cold-recovery-image
                   (lambda (root driver)
                     (loop for value in (run-promotion-handoff-child
                                         root driver streaming :error :form-builder #'cold-recovery-success-child-form
                                         :cache (merge-pathnames "cache/" cache-root))
                           for label in '("prepared phase" "runtime phase" "receive ownership"
                                          "open CLOG" "catalog installed" "WAL ready"
                                          "healthy recovery" "restored progress")
                           do (ok value label)))
                   :streaming streaming))))))

(defun cold-recovery-failure-child-form (root marker boundary kind)
  (let ((point (if (consp boundary) (second boundary) boundary)))
    `(let ((pg::*pgdata* ,root)
           (opened nil) (clog nil) (relations nil) (cuts 0) (cleanup-armed nil) (clog-armed nil)
           (tag (gensym "COLD-STORAGE-FAILURE"))
           (originals
             (mapcar (lambda (name) (cons name (symbol-function name)))
                     '(pg::physical-store-open pg::physical-recovery-open-receive-store
                       pg::physical-replay-from pg::wal-load-replication-slots
                       pg::catalog-refresh-oldest-relfrozenxid pg::smgr-close-all pg::physical-store-close
                       cl:open pg::load-catalog pg::physical-restartpoint-prepare-storage))))
       (labels ((original (name) (cdr (assoc name originals)))
                (restore ()
                  (sb-ext:without-package-locks
                    (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry)))))
                (inject ()
                  (incf cuts)
                  (ecase ,kind
                    (:error (error "injected cold storage failure"))
                    (:throw (throw tag :cut))
                    (:interrupt
                     (sb-thread:interrupt-thread
                      sb-thread:*current-thread*
                      (lambda () (error "injected cold storage failure"))))))
                (capture ()
                  (setf clog pg::*clog-stream*)
                  (pushnew (sb-thread:with-mutex (pg::*smgr-lock*) (pg::smgr-open-locked 900))
                           relations :test #'eq)))
         (unwind-protect
              (progn
                (pg::postmaster-own-data-directory "127.0.0.1" 5439)
                (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                  (sb-thread:with-mutex (pg::*reload-run-lock*)
                    (pg::with-live-operation-quiescence
                      (pg::physical-recovery-prepare)
                      (let ((phase (getf pg::*physical-recovery-state* :phase)))
                        (sb-ext:without-package-locks
                          (setf
                           (symbol-function 'cl:open)
                           (lambda (pathname &rest options)
                             (let ((stream (apply (original 'cl:open) pathname options)))
                               (when (and clog-armed
                                          (equal (pathname pathname) (pathname (pg::clog-path))))
                                 (setf clog stream clog-armed nil)
                                 (sb-thread:interrupt-thread sb-thread:*current-thread* #'inject))
                               stream))
                           (symbol-function 'pg::load-catalog)
                           (lambda ()
                             (prog1 (funcall (original 'pg::load-catalog))
                               (when (and (eq ,point :clog-open) (= phase 1))
                                 (setf clog-armed t))))
                           (symbol-function 'pg::physical-restartpoint-prepare-storage)
                           (lambda (store)
                             (prog1 (funcall (original 'pg::physical-restartpoint-prepare-storage) store)
                               (when (eq ,point :clog-open) (setf clog-armed t))))
                           (symbol-function 'pg::physical-store-open)
                           (lambda (&rest arguments)
                             (setf opened (apply (original 'pg::physical-store-open) arguments))
                             (when (eq ,point :primitive) (inject))
                             opened)
                           (symbol-function 'pg::physical-recovery-open-receive-store)
                           (lambda (&rest arguments)
                             (prog1 (apply (original 'pg::physical-recovery-open-receive-store) arguments)
                               (when (eq ,point :wrapper) (inject))))
                           (symbol-function 'pg::physical-replay-from)
                           (lambda (&rest arguments)
                             (prog1 (apply (original 'pg::physical-replay-from) arguments)
                               (capture)
                               (when (eq ,point :replay) (inject))))
                           (symbol-function 'pg::wal-load-replication-slots)
                           (lambda ()
                             (funcall (original 'pg::wal-load-replication-slots))
                             (capture)
                             (case ,point
                               (:slots (inject))
                               ((:close-smgr :close-store)
                                (setf cleanup-armed t)
                                (error "injected cold storage failure"))))
                           (symbol-function 'pg::catalog-refresh-oldest-relfrozenxid)
                           (lambda ()
                             (prog1 (funcall (original 'pg::catalog-refresh-oldest-relfrozenxid))
                               (when pg::*clog-stream*
                                 (capture)
                                 (when (eq ,point :catalog) (inject)))))
                           (symbol-function 'pg::smgr-close-all)
                           (lambda ()
                             (funcall (original 'pg::smgr-close-all))
                             (when (and cleanup-armed (eq ,point :close-smgr)) (inject)))
                           (symbol-function 'pg::physical-store-close)
                           (lambda (store)
                             (funcall (original 'pg::physical-store-close) store)
                             (when (and cleanup-armed (eq ,point :close-store) (eq store opened))
                               (inject)))))
                        (let ((exit
                                (catch tag
                                  (handler-case (pg::physical-recovery-open-storage)
                                    (error (condition)
                                      (when (search "injected cold storage failure" (princ-to-string condition))
                                        :cut))))))
                          (restore)
                          (with-open-file (out ,marker :direction :output :if-exists :error)
                            (write
                             (list
                              (sb-posix:getpid) ,kind
                              (list
                               (and (eq exit :cut) (= cuts 1))
                               pg::*physical-recovery-failed-p*
                               (if (= phase 2)
                                   (and opened (pg::physical-store-closed-p opened)
                                        (not (open-stream-p (pg::physical-store-lock-stream opened))))
                                   (null opened))
                               (or (not clog) (not (open-stream-p clog)))
                               (and (zerop (hash-table-count pg::*relation-streams*))
                                    (every (lambda (stream) (not (open-stream-p stream))) relations)
                                    ,(if (member point '(:primitive :wrapper :clog-open)) t '(not (null relations))))
                               (not (or pg::*physical-receive-store* pg::*clog-stream*))
                               (and pg::*wal-ready* pg::*catalog*
                                    (= phase (getf pg::*physical-recovery-state* :phase)))
                               (handler-case
                                   (progn (pg::check-recovery-available) nil)
                                 (pg::pg-error (condition)
                                   (equal "57P03" (pg::pg-error-sqlstate condition))))))
                             :stream out :readably t)))))))
                (format t "~%COLD-RECOVERY-FAILURE-PASS ~s ~s~%" ',boundary ,kind))
           (restore)
           (when opened (funcall (original 'pg::physical-store-close) opened))
           (when pg::*physical-receive-store*
             (funcall (original 'pg::physical-store-close) pg::*physical-receive-store*))
           (when clog (close clog))
           (when pg::*clog-stream* (close pg::*clog-stream*))
           (dolist (stream relations) (when (open-stream-p stream) (close stream)))
           (funcall (original 'pg::smgr-close-all))
           (pg::postmaster-remove-pid-file))))))

(defun check-cold-recovery-storage-cuts (boundaries &optional (kinds '(:error :throw :interrupt)))
  (call-with-physical-store-directory
   (lambda (cache-root)
     (dolist (boundary boundaries)
       (dolist (kind kinds)
         (testing (format nil "~s ~s" boundary kind)
                  (call-with-cold-recovery-image
                   (lambda (root driver)
                     (loop for value in (run-promotion-handoff-child
                                         root driver boundary kind :form-builder #'cold-recovery-failure-child-form
                                         :cache (merge-pathnames "cache/" cache-root))
                           for label in '("exact injected exit" "recovery fenced" "receive store closed"
                                          "CLOG closed" "cached relation streams closed"
                                          "stream pointers cleared" "partial recovery role retained"
                                          "SQL admission fenced")
                           do (ok value label)))
                   :streaming (not (consp boundary)))))))))

(deftest cold-recovery-storage-closes-interrupted-receive-handoffs
    (check-cold-recovery-storage-cuts '(:primitive :wrapper)))

(deftest cold-recovery-storage-cleans-streaming-resources-across-late-exits
    (check-cold-recovery-storage-cuts '(:replay :slots :catalog :close-smgr :close-store)))

(deftest cold-recovery-storage-cleans-static-resources-across-late-exits
    (check-cold-recovery-storage-cuts '((:static :slots) (:static :catalog) (:static :close-smgr))))

(deftest cold-recovery-storage-registers-clog-before-delivering-interrupts
    (check-cold-recovery-storage-cuts '(:clog-open (:static :clog-open)) '(:error :throw)))

(defun cold-recovery-guard-child-form (root marker boundary kind)
  `(let ((pg::*pgdata* ,root) (borrowed nil) (rotations 0)
                              (admission (symbol-function 'pg::physical-follow-check-startup-admission))
                              (rotate (symbol-function 'pg::wal-replace-storage-incarnation)))
     (labels ((release ()
                (when pg::*physical-receive-store* (pg::physical-store-close pg::*physical-receive-store*))
                (when (eq ,boundary :receive) (pg::physical-store-close borrowed))
                (when pg::*clog-stream* (close pg::*clog-stream*))
                (when pg::*wal-output-stream* (close pg::*wal-output-stream*))
                (when (and (member ,boundary '(:clog :wal-output :relations))
                           borrowed (open-stream-p borrowed))
                  (close borrowed))
                (pg::smgr-close-all)
                (setf pg::*catalog* nil pg::*clog-stream* nil pg::*physical-receive-store* nil
                      pg::*wal-output-stream* nil pg::*wal-ready* nil))
              (snapshot ()
                (list pg::*catalog* pg::*clog-stream* pg::*physical-receive-store*
                      pg::*wal-output-stream* pg::*wal-ready*
                      (hash-table-count pg::*relation-streams*))))
       (unwind-protect
            (progn
              (pg::postmaster-own-data-directory "127.0.0.1" 5439)
              (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                (sb-thread:with-mutex (pg::*reload-run-lock*)
                  (pg::with-live-operation-quiescence
                    (pg::physical-recovery-prepare)
                    (case ,boundary
                      (:catalog (setf borrowed (pg::load-catalog) pg::*catalog* borrowed))
                      (:clog (setf borrowed (open (pg::clog-path) :element-type '(unsigned-byte 8))
                                   pg::*clog-stream* borrowed))
                      (:receive (setf borrowed (pg::physical-recovery-open-receive-store)
                                      pg::*physical-receive-store* borrowed))
                      (:wal-output
                       (setf borrowed (open (pg::relation-file-path 900)
                                            :direction :io :if-exists :overwrite
                                            :element-type '(unsigned-byte 8))
                             pg::*wal-output-stream* borrowed))
                      (:wal-ready (setf borrowed t pg::*wal-ready* t))
                      (:relations
                       (setf borrowed
                             (sb-thread:with-mutex (pg::*smgr-lock*) (pg::smgr-open-locked 900))))
                      (:admission
                       (setf (symbol-function 'pg::physical-follow-check-startup-admission)
                             (lambda (state)
                               (declare (ignore state))
                               (error "injected startup admission rejection")))))
                    (setf (symbol-function 'pg::wal-replace-storage-incarnation)
                          (lambda () (incf rotations) (funcall rotate)))
                    (let* ((before (snapshot)) (state pg::*physical-recovery-state*)
                                               (control (pg::recovery-read-bytes (pg::physical-recovery-control-path)))
                                               (exit
                                                 (handler-case (progn (pg::physical-recovery-open-storage) nil)
                                                   (error (condition)
                                                     (let ((text (princ-to-string condition)))
                                                       (or (search "recovery storage requires cold resources" text)
                                                           (search "injected startup admission rejection" text))))))
                                               (result
                                                 (list (not (null exit))
                                                       (not pg::*physical-recovery-failed-p*)
                                                       (equal before (snapshot))
                                                       (case ,boundary
                                                         (:receive (pg::physical-store-check-held-lock borrowed))
                                                         ((:clog :wal-output :relations) (open-stream-p borrowed))
                                                         (otherwise t))
                                                       (eq state pg::*physical-recovery-state*)
                                                       (zerop rotations)
                                                       (equalp control
                                                               (pg::recovery-read-bytes (pg::physical-recovery-control-path))))))
                      (setf (symbol-function 'pg::physical-follow-check-startup-admission) admission
                            (symbol-function 'pg::wal-replace-storage-incarnation) rotate)
                      (setf result
                            (append result
                                    (list
                                     (when (every #'identity result)
                                       (release)
                                       (pg::physical-recovery-open-storage)
                                       (and (not pg::*physical-recovery-failed-p*)
                                            (pg::physical-store-check-held-lock pg::*physical-receive-store*)
                                            (open-stream-p pg::*clog-stream*))))))
                      (with-open-file (out ,marker :direction :output :if-exists :error)
                        (write (list (sb-posix:getpid) ,kind result) :stream out :readably t))))))
              (format t "~%COLD-RECOVERY-GUARD-PASS ~s~%" ,boundary))
         (setf (symbol-function 'pg::physical-follow-check-startup-admission) admission
               (symbol-function 'pg::wal-replace-storage-incarnation) rotate)
         (release)
         (pg::postmaster-remove-pid-file)))))

(deftest cold-recovery-storage-rejects-borrowed-resources-before-mutation
    (call-with-physical-store-directory
     (lambda (cache-root)
       (dolist (boundary '(:catalog :clog :receive :wal-output :wal-ready :relations :admission))
         (testing (symbol-name boundary)
                  (call-with-cold-recovery-image
                   (lambda (root driver)
                     (loop for value in (run-promotion-handoff-child
                                         root driver boundary :error :form-builder #'cold-recovery-guard-child-form
                                         :cache (merge-pathnames "cache/" cache-root))
                           for label in '("precondition rejected" "recovery remains healthy"
                                          "borrowed pointers preserved" "borrowed resource remains owned"
                                          "verified state preserved" "no storage rotation"
                                          "durable control unchanged" "healthy retry after rejection")
                           do (ok value label)))))))))

(deftest pending-store-validation-preserves-unacknowledged-tail-and-requires-existing-lock
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (pg::physical-store-input store 0 (sample-physical-record 0))
        (pg::physical-store-flush store)
        (pg::physical-store-close store))
      (let* ((path (merge-pathnames "0000000000000000.wal" root))
             (bytes (concatenate '(vector (unsigned-byte 8)) (lineage-test-bytes path) #(9 8 7))))
        (pg::wal-atomic-write-bytes path bytes)
        (let ((store (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil)))
          (unwind-protect
               (progn (ok (= 32 (pg::physical-store-flushed-lsn store)))
                      (ok (equalp bytes (lineage-test-bytes path))))
            (pg::physical-store-close store)))
        (delete-file (merge-pathnames "receive.lock" root))
        (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil))))
        (ok (not (probe-file (merge-pathnames "receive.lock" root))))
        (ok (equalp bytes (lineage-test-bytes path))))))

(deftest pending-store-validation-rejects-lock-symlinks
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (pg::physical-store-close store))
      (delete-file (merge-pathnames "receive.lock" root))
      (sb-posix:symlink "/missing-pending-receive-lock" (namestring (merge-pathnames "receive.lock" root)))
      (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil))))
      (ok (not (probe-file #P"/missing-pending-receive-lock")))))

(deftest pending-resume-requires-cold-owned-exclusion
    (ok (intake-fails-p #'pg::physical-promotion-resume-pending)))

(deftest shared-recovery-marker-preserves-bytes-and-rejects-special-files
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (path (merge-pathnames "recovery.required" root)))
        (pg::physical-bootstrap-mark-target root)
        (ok (equalp (pg::string->bytes (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%"))
                    (lineage-test-bytes path)))
        (ok (progn (pg::physical-recovery-check-required-marker) t))
        (pg::wal-atomic-write-bytes path #(1 2 3))
        (ok (intake-fails-p #'pg::physical-recovery-check-required-marker))
        (delete-file path)
        (sb-posix:mkfifo (namestring path) #o600)
        (ok (intake-fails-p #'pg::physical-recovery-check-required-marker))
        (sb-posix:unlink (namestring path))
        (sb-posix:symlink "/missing-pending-marker" (namestring path))
        (unwind-protect (ok (intake-fails-p #'pg::physical-recovery-check-required-marker))
          (sb-posix:unlink (namestring path))))))

(cl-spec:defproperty pending-open-never-trims-generated-unacknowledged-bytes
    ((length (range integer 0 256)) (byte (range integer 0 255)))
  (:about pg::physical-store-open)
  (:trials (:normal 50))
  (with-physical-store-directory (root)
    (let ((store (pg::physical-store-open root 123 1 :create t)))
      (pg::physical-store-input store 0 (sample-physical-record 0))
      (pg::physical-store-flush store)
      (pg::physical-store-close store))
    (let* ((path (merge-pathnames "0000000000000000.wal" root))
           (bytes (concatenate '(vector (unsigned-byte 8)) (sample-physical-record 0)
                               (make-array length :element-type '(unsigned-byte 8) :initial-element byte))))
      (pg::wal-atomic-write-bytes path bytes)
      (let ((store (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil)))
        (unwind-protect
             (and (= 32 (pg::physical-store-flushed-lsn store))
                  (equalp bytes (lineage-test-bytes path)))
          (pg::physical-store-close store))))))

(deftest generated-pending-store-open
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'pending-open-never-trims-generated-unacknowledged-bytes :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
