(in-package #:pg-physical-tests)

(deftest backup-lease-status-interrupts-after-durable-pin-invalidation
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (backup-lease-startup-save-origin record)
         (let* ((request (getf record :request))
                (name (getf request :name))
                (base (getf record :base)))
           (ok (eq :ready (getf (pg::physical-backup-lease-status request) :phase)))
           (pg::with-wal-critical
             (pg::physical-source-invalidate-lagging-pins-under-wal-lock 0))
           (ok (pg::physical-source-pin-lost-p pin))
           (ok (pg::physical-source-pin-lost-p
                (pg::physical-source-pin-read-retirement name)))
           (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
           (let ((status (pg::physical-backup-lease-status request)))
             (ok (eq :interrupted (getf status :phase)))
             (ok (equal base (getf status :base)))
             (ok (equal request (getf status :request)))
             (ok (equal status
                        (pg::physical-backup-lease-read
                         (pg::physical-backup-lease-path request))))
             (backup-lease-startup-reopen)
             (ok (equal status (pg::physical-backup-lease-status request)))
             (ok (equal base (getf (gethash name pg::*physical-backup-lease-records*)
                                   :base)))
             (ok (pg::physical-source-pin-lost-p
                  (gethash name pg::*physical-source-pins*)))
             (ok (eq :closed (getf (pg::physical-backup-lease-abort request) :phase)))
             (ok (null (gethash name pg::*physical-source-pins*)))
             (ok (null (probe-file (pg::physical-source-pin-path name))))
             (ok (not pg::*wal-poisoned*))))))))

(deftest backup-lease-status-interrupts-on-role-or-timeline-change
    (dolist (kind '(:standby :timeline))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
           (backup-lease-startup-save-origin record)
           (let* ((request (getf record :request))
                  (base (getf record :base))
                  (pg::*physical-recovery-state*
                    (when (eq kind :standby) (list :phase 2)))
                  (pg::*wal-insert-timeline*
                    (if (eq kind :timeline) 3 2))
                  (status (pg::physical-backup-lease-status request)))
             (ok (eq :interrupted (getf status :phase)))
             (ok (equal base (getf status :base)))
             (ok (equal status
                        (pg::physical-backup-lease-read
                         (pg::physical-backup-lease-path request))))
             (ok (not (pg::physical-source-pin-lost-p pin)))
             (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok (not pg::*wal-poisoned*))))))))

(deftest backup-lease-status-refuses-missing-evidence-without-changing-state
    (dolist (kind '(:pin :origin :wal))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
           (let* ((origin (backup-lease-startup-save-origin record))
                  (request (getf record :request))
                  (name (getf request :name))
                  (path (pg::physical-backup-lease-path request))
                  (before (pg::read-bounded-regular-file
                           path pg::+physical-backup-lease-maximum-bytes+)))
             (ecase kind
               (:pin (delete-file (pg::physical-source-pin-path name)))
               (:origin (delete-file (pg::physical-export-origin-path origin)))
               (:wal (delete-file (pg::wal-segment-path 0))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-backup-lease-status request))))
             (ok (eq :ready
                     (getf (gethash name pg::*physical-backup-lease-records*) :phase)))
             (ok (equalp before
                         (pg::read-bounded-regular-file
                          path pg::+physical-backup-lease-maximum-bytes+)))
             (ok (equal record (pg::physical-backup-lease-read path)))
             (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok (not (pg::physical-source-pin-lost-p pin)))
             (ok pg::*wal-poisoned*)))))))
