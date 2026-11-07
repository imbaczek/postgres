(in-package #:pg-physical-tests)

(cl-spec:defproperty backup-lease-historical-successors-deny-new-pins
    ((closed boolean) (suffix (range integer 0 999)))
  (:about pg::physical-backup-lease-check-successor-create)
  (:trials (:normal 100))
  (let* ((record (backup-lease-test-record :ready))
         (name (format nil "historical_successor_~d" suffix))
         (pg::*wal-lock* (sb-thread:make-mutex :name "historical successor contract"))
         (pg::*physical-backup-lease-records* (make-hash-table :test #'equal))
         (pg::*physical-backup-lease-successor-cache* nil))
    (setf (getf record :phase) (if closed :closed :closing)
          (getf record :outcome) :handed-off
          (getf record :successor) (list :name name :anchor-digest 42)
          record (pg::physical-backup-lease-copy record)
          (gethash (getf (getf record :request) :name) pg::*physical-backup-lease-records*) record)
    (sb-thread:with-mutex (pg::*wal-lock*)
      (and (handler-case
               (progn (pg::physical-backup-lease-check-successor-create name) nil)
             (pg::wal-input-validation-error (condition)
               (equal "backup lease historical successor permanently reserves this name"
                      (princ-to-string condition))))
           (progn (pg::physical-backup-lease-check-successor-create "ordinary_unassigned") t)))))

(deftest generated-backup-lease-historical-successor-creation-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-historical-successors-deny-new-pins
                                         :seed 190 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(defun call-with-backup-lease-closed-successor-fixture (function)
  (call-with-backup-lease-authority-fixture
   (lambda (root)
     (let ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
           (pg::*physical-backup-lease-successor-cache* nil))
       (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
         (declare (ignore bootstrap))
         (backup-lease-startup-save-origin record)
         (let* ((pin (backup-lease-startup-prepare-successor record))
                (anchor (pg::physical-follow-anchor-copy (pg::physical-follow-pin-anchor pin)))
                (successor (list :name (pg::physical-source-pin-name pin)
                                 :anchor-digest (pg::physical-follow-anchor-digest anchor)))
                (closed (pg::physical-backup-lease-handoff (getf record :request) successor)))
           (funcall function root closed pin anchor)))))))

(deftest backup-lease-closed-successor-denies-new-pins-after-release-and-reload
    (dolist (reload '(nil t))
      (dolist (lane '(:bootstrap :reserve :prepare))
        (call-with-backup-lease-closed-successor-fixture
         (lambda (root closed pin anchor)
           (declare (ignore root))
           (let* ((name (pg::physical-source-pin-name pin))
                  (path (pg::physical-backup-lease-path (getf closed :request)))
                  (before (lineage-test-bytes path))
                  (coordinates (pg::physical-follow-anchor-request anchor)))
             (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
             (ok (eq :released (pg::physical-source-follow-release
                                name (pg::physical-follow-anchor-digest anchor))))
             (setf pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
             (when reload (backup-lease-startup-reopen))
             (ok (equal closed (pg::physical-backup-lease-read path)))
             (ok (equal "backup lease historical successor permanently reserves this name"
                        (handler-case
                            (progn
                              (ecase lane
                                (:bootstrap (pg::physical-source-pin-create name))
                                (:reserve (pg::physical-source-follow-reserve name anchor))
                                (:prepare (pg::physical-source-follow-prepare name coordinates)))
                              :accepted)
                          (pg::wal-input-validation-error (condition) (princ-to-string condition)))))
             (ok (null (gethash name pg::*physical-source-pins*)))
             (ok (not (probe-file (pg::physical-source-pin-path name))))
             (ok (equalp before (lineage-test-bytes path)))
             (ok (not pg::*wal-poisoned*))))))))

(deftest backup-lease-closed-successor-keeps-exact-existing-pin-retries
    (call-with-backup-lease-closed-successor-fixture
     (lambda (root closed pin anchor)
       (declare (ignore root closed))
       (let* ((name (pg::physical-source-pin-name pin))
              (before (lineage-test-bytes (pg::physical-source-pin-path name)))
              (coordinates (pg::physical-follow-anchor-request anchor)))
         (ok (eq pin (pg::physical-source-follow-reserve name anchor)))
         (ok (equal anchor (pg::physical-follow-pin-anchor
                            (pg::physical-source-follow-prepare name coordinates))))
         (backup-lease-startup-reopen)
         (ok (equal anchor (pg::physical-follow-pin-anchor
                            (pg::physical-source-follow-prepare name coordinates))))
         (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-historical-successor-fences-a-preparation-after-anchor-capture
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
             (pg::*physical-backup-lease-successor-cache* nil))
         (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
           (declare (ignore bootstrap))
           (backup-lease-startup-save-origin record)
           (let* ((prime (backup-lease-startup-prepare-successor record))
                  (anchor (pg::physical-follow-anchor-copy (pg::physical-follow-pin-anchor prime)))
                  (coordinates (pg::physical-follow-anchor-request anchor))
                  (name "delayed_handoff_successor")
                  (digest (pg::physical-follow-anchor-digest anchor))
                  (reached nil)
                  (closed nil)
                  (pg::*physical-source-test-hook*
                    (lambda (point pin)
                      (declare (ignore pin))
                      (when (eq point :after-follow-anchor-capture)
                        (setf reached t)
                        (let ((pg::*physical-source-test-hook* nil))
                          (pg::physical-source-follow-ready name anchor)
                          (setf closed (pg::physical-backup-lease-handoff
                                        (getf record :request)
                                        (list :name name :anchor-digest digest)))
                          (pg::physical-source-follow-release name digest)
                          (setf pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
                          (backup-lease-startup-reopen))))))
             (ok (equal "backup lease historical successor permanently reserves this name"
                        (handler-case
                            (progn (pg::physical-source-follow-prepare name coordinates) :accepted)
                          (pg::wal-input-validation-error (condition) (princ-to-string condition)))))
             (ok reached)
             (ok (eq :closed (getf closed :phase)))
             (ok (null (gethash name pg::*physical-source-pins*)))
             (ok (not (probe-file (pg::physical-source-pin-path name))))
             (ok (not pg::*wal-poisoned*))))))))

(defun backup-lease-successor-fence-child-body (closed anchor mode)
  (let* ((request (getf closed :request))
         (successor (getf closed :successor))
         (operation
           (format nil
                   "(let* ((name (getf successor :name))
                           (anchor '~a)
                           (coordinates (pg::physical-follow-anchor-request anchor))
                           (path (pg::physical-backup-lease-path request))
                           (before (pg::read-bounded-regular-file path 8192)))
                      (format t \"~%SOURCE-NAME-FENCE-CHILD-START ~~d ~~s ~~s~%\"
                              (sb-posix:getpid) (namestring pg::*pgdata*) ~s)
                      (finish-output)
                      (assert (zerop (hash-table-count pg::*physical-follow-aborted-attempts*)))
                      (assert (equal '~a (pg::physical-backup-lease-status request)))
                      ~a
                      (assert (equalp before (pg::read-bounded-regular-file path 8192)))
                      (assert (not pg::*wal-poisoned*))
                      (format t \"~%SOURCE-NAME-FENCE-CHILD-VERIFIED ~~d ~~s ~~s~%\"
                              (sb-posix:getpid) (namestring pg::*pgdata*) ~s)
                      (finish-output))"
                   (pg::physical-bootstrap-manifest-text anchor) mode
                   (pg::physical-bootstrap-manifest-text closed)
                   (ecase mode
                     (:observe
                      "(let ((pin (gethash name pg::*physical-source-pins*)))
                         (assert pin)
                         (assert (eq pin (pg::physical-source-follow-reserve name anchor)))
                         (assert (equal anchor (pg::physical-follow-pin-anchor
                                                (pg::physical-source-follow-prepare name coordinates)))))")
                     (:absent
                      "(assert (null (gethash name pg::*physical-source-pins*)))
                       (dolist (lane '(:bootstrap :reserve :prepare))
                         (assert
                          (equal \"backup lease historical successor permanently reserves this name\"
                                 (handler-case
                                     (progn
                                       (ecase lane
                                         (:bootstrap (pg::physical-source-pin-create name))
                                         (:reserve (pg::physical-source-follow-reserve name anchor))
                                         (:prepare (pg::physical-source-follow-prepare name coordinates)))
                                       :accepted)
                                   (pg::wal-input-validation-error (condition)
                                     (princ-to-string condition)))))
                         (assert (null (gethash name pg::*physical-source-pins*)))
                         (assert (not (probe-file (pg::physical-source-pin-path name)))))"))
                   mode)))
    (format nil
            "(let ((pg::*catalog* (pg::make-catalog :cluster-identity \"backup-lease-codec\"))
                   (pg::*wal-insert-timeline* 2))
               ~a)"
            (backup-lease-handoff-child-body request successor operation))))

(deftest backup-lease-historical-successor-fence-survives-fresh-source-images
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "successor-cache.XXXXXX" cache-root))))))
        (call-with-backup-lease-closed-successor-fixture
         (lambda (root closed pin anchor)
           (dolist (mode '(:observe :absent))
             (when (eq :absent mode)
               (ok (eq :released
                       (pg::physical-source-follow-release
                        (pg::physical-source-pin-name pin) (pg::physical-follow-anchor-digest anchor)))))
             (let ((log (merge-pathnames (format nil "successor-~(~a~).log" mode) root)))
               (multiple-value-bind (status pid)
                   (reseed-follow-store-run-child
                    root log (backup-lease-successor-fence-child-body closed anchor mode) cache)
                 (let ((raw (uiop:read-file-string log)))
                   (write-string raw *standard-output*)
                   (format t "~%SOURCE-NAME-FENCE-CHILD-OWNED ~d ~s ~s ~s~%"
                           pid (namestring root) mode status)
                   (ok (equal '(:exited 0) status))
                   (ok (search (format nil "SOURCE-NAME-FENCE-CHILD-START ~d ~s ~s"
                                       pid (namestring root) mode) raw))
                   (ok (search (format nil "SOURCE-NAME-FENCE-CHILD-VERIFIED ~d ~s ~s"
                                       pid (namestring root) mode) raw)))))))))))
