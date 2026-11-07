(in-package #:pg-physical-tests)

(defun backup-lease-startup-save-origin (record)
  (let* ((request (getf record :request))
         (base (getf record :base))
         (origin (pg::make-physical-export-origin
                  :system-id (getf request :system-id) :timeline (getf request :timeline)
                  :base (getf base :base) :manifest-digest (getf base :manifest-digest)
                  :proven-lsn (getf base :base) :prefix-digest #xCBF29CE484222325)))
    (pg::with-wal-critical
      (pg::physical-export-origin-save origin)
      (setf (gethash (pg::physical-export-origin-identity origin) pg::*physical-export-origins*)
            (pg::make-physical-export-origin-entry :origin origin)))
    origin))

(defun backup-lease-startup-reopen (&optional standby)
  (let ((pg::*physical-recovery-state* (when standby (list :phase 2))))
    (pg::physical-backup-lease-load-records)
    (if standby (pg::physical-backup-lease-load-standby-pins) (pg::physical-source-pin-load))
    (pg::physical-export-origin-load)
    (pg::physical-backup-lease-reconcile-startup)))

(deftest backup-lease-startup-keeps-a-healthy-ready-pin-and-origin
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record old-pin) (backup-lease-authority-prepare t)
         (backup-lease-startup-save-origin record)
         (backup-lease-startup-reopen)
         (let* ((name (backup-lease-store-test-name record))
                (pin (gethash name pg::*physical-source-pins*)))
           (ok (not (eq old-pin pin)))
           (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (not (pg::physical-source-pin-lost-p pin)))
           (ok (equal record (gethash name pg::*physical-backup-lease-records*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-startup-fences-loss-save-exits-before-dropping-the-old-floor
    (dolist (standby '(nil t))
      (dolist (point '(:before-save :after-save))
        (call-with-backup-lease-authority-fixture
         (lambda (root)
           (declare (ignore root))
           (multiple-value-bind (record old-pin) (backup-lease-authority-prepare t)
             (backup-lease-startup-save-origin record)
             (let* ((name (backup-lease-store-test-name record))
                    (pg::*wal-insert-timeline* 3)
                    (pg::*physical-recovery-state* (when standby (list :phase 2)))
                    (seen nil))
               (pg::physical-backup-lease-load-records)
               (let ((pg::*physical-source-test-hook*
                       (lambda (actual pin)
                         (declare (ignore pin))
                         (when (eq actual point)
                           (setf seen t)
                           (throw 'lease-loss-cut :cut)))))
                 (ok (intake-fails-p
                      (lambda ()
                        (catch 'lease-loss-cut
                          (if standby
                              (pg::physical-backup-lease-load-standby-pins)
                              (pg::physical-source-pin-load)))))))
               (ok seen)
               (ok pg::*wal-poisoned*)
               (ok (eq old-pin (gethash name pg::*physical-source-pins*)))
               (ok (= 32 (gethash old-pin pg::*wal-snapshot-retention-pins*)))
               (ok (not (pg::physical-source-pin-lost-p old-pin)))
               (ok (eq (not (null (pg::physical-source-pin-lost-p
                                   (pg::physical-source-pin-read-retirement name))))
                       (eq point :after-save)))
               (let ((pg::*wal-poisoned* nil))
                 (backup-lease-startup-reopen standby)
                 (let ((current (gethash name pg::*physical-source-pins*)))
                   (ok (pg::physical-source-pin-lost-p current))
                   (ok (null (gethash current pg::*wal-snapshot-retention-pins*)))
                   (ok (null (gethash old-pin pg::*wal-snapshot-retention-pins*)))
                   (ok (eq :interrupted
                           (getf (gethash name pg::*physical-backup-lease-records*) :phase))))
                 (ok (not pg::*wal-poisoned*))))))))))

(deftest backup-lease-startup-invalidates-only-explicit-loss-or-source-role-change
    (dolist (kind '(:lost :timeline :standby))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
           (backup-lease-startup-save-origin record)
           (when (eq kind :lost)
             (setf (pg::physical-source-pin-lost-p pin) t)
             (pg::with-wal-critical (pg::physical-source-pin-save pin)))
           (delete-file (pg::wal-segment-path 0))
           (let ((pg::*wal-insert-timeline* (if (eq kind :timeline) 3 2))
                 (pg::*wal-durable-lsn* (if (eq kind :timeline) 0 152)))
             (backup-lease-startup-reopen (eq kind :standby))
             (let* ((name (backup-lease-store-test-name record))
                    (current (gethash name pg::*physical-source-pins*))
                    (actual (gethash name pg::*physical-backup-lease-records*))
                    (disk (pg::physical-source-pin-read-retirement name)))
               (ok (eq :interrupted (getf actual :phase)))
               (ok (equal (getf actual :base) (getf record :base)))
               (ok (pg::physical-source-pin-lost-p current))
               (ok (pg::physical-source-pin-lost-p disk))
               (ok (null (gethash current pg::*wal-snapshot-retention-pins*)))
               (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
               (ok (not pg::*wal-poisoned*))
               (let ((closing (copy-tree actual)))
                 (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
                 (pg::physical-backup-lease-publish actual closing)
                 (pg::physical-backup-lease-finish-closing (getf closing :request))
                 (ok (eq :closed
                         (getf (gethash name pg::*physical-backup-lease-records*) :phase)))))))))))

(deftest backup-lease-startup-refuses-unexplained-missing-pin-or-wal
    (dolist (kind '(:pin :wal))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
           (backup-lease-startup-save-origin record)
           (ecase kind
             (:pin (delete-file (pg::physical-source-pin-path (backup-lease-store-test-name record))))
             (:wal (delete-file (pg::wal-segment-path 0))))
           (let ((registry pg::*physical-source-pins*))
             (ok (intake-fails-p #'backup-lease-startup-reopen))
             (ok (eq registry pg::*physical-source-pins*))
             (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok pg::*wal-poisoned*))
           (ok (eq :ready
                   (getf (gethash (backup-lease-store-test-name record)
                                  pg::*physical-backup-lease-records*) :phase))))))))

(deftest backup-lease-startup-refuses-missing-origin-without-releasing-the-pin
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (declare (ignore pin))
         (ok (intake-fails-p #'backup-lease-startup-reopen))
         (let* ((name (backup-lease-store-test-name record))
                (current (gethash name pg::*physical-source-pins*)))
           (ok (= 32 (gethash current pg::*wal-snapshot-retention-pins*)))
           (ok (eq :ready (getf (gethash name pg::*physical-backup-lease-records*) :phase)))
           (ok pg::*wal-poisoned*))))))

(deftest backup-lease-startup-does-not-invalidate-foreign-payloads-as-expiration
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
         (backup-lease-startup-save-origin record)
         (setf (pg::physical-source-pin-manifest-digest pin) 992)
         (pg::with-wal-critical (pg::physical-source-pin-save pin))
         (let ((pg::*wal-insert-timeline* 3)
               (before (lineage-test-bytes (pg::physical-source-pin-path (backup-lease-store-test-name record)))))
           (ok (intake-fails-p #'backup-lease-startup-reopen))
           (ok (equalp before (lineage-test-bytes
                               (pg::physical-source-pin-path (backup-lease-store-test-name record)))))
           (ok (not (pg::physical-source-pin-lost-p pin)))
           (ok pg::*wal-poisoned*))))))

(deftest backup-lease-startup-does-not-relax-ordinary-pin-timeline-validation
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((pin (pg::physical-source-pin-create "ordinary_before_promotion"))
              (pg::*wal-insert-timeline* 3))
         (ok (intake-fails-p #'backup-lease-startup-reopen))
         (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (not (pg::physical-source-pin-lost-p pin)))
         (ok pg::*wal-poisoned*)))))

(defun backup-lease-startup-prepare-successor (record)
  (pg::timeline-write-history 2 1 32 "promotion")
  (with-open-file (stream (pg::wal-segment-path 0) :direction :io
                                                   :if-exists :overwrite :element-type '(unsigned-byte 8))
    (write-sequence (legacy-checkpoint-record 0) stream)
    (pg::fsync-stream stream))
  (let* ((bytes (pg::read-bounded-regular-file (pg::wal-segment-path 0) 1024))
         (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
         (base (getf record :base))
         (request (list :version 1 :system-id (pg::catalog-system-identifier)
                        :base-timeline 2 :base 152 :manifest-digest (getf base :manifest-digest)
                        :source-timeline 2 :receive-timeline 2 :receive-lsn 152
                        :receive-digest digest :overlap-start 0 :overlap-digest digest)))
    (pg::physical-source-follow-prepare "handoff_successor" request)))

(defun backup-lease-startup-inject-closing (record closing)
  (pg::with-write-lock
    (pg::with-wal-publication-fence (begin-publication)
      (let ((saved (pg::physical-backup-lease-transition record closing)))
        (begin-publication)
        (pg::physical-backup-lease-write saved)
        (setf pg::*physical-backup-lease-successor-cache* nil
              (gethash (getf (getf saved :request) :name)
                       pg::*physical-backup-lease-records*) saved)
        saved))))

(deftest backup-lease-handed-off-closing-requires-a-live-successor
    (dolist (startup '(nil t))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
           (declare (ignore pin))
           (backup-lease-startup-save-origin record)
           (let* ((request (getf record :request))
                  (name (getf request :name))
                  (closing (copy-tree record)))
             (setf (getf closing :phase) :closing
                   (getf closing :outcome) :handed-off
                   (getf closing :successor) (list :name "reseed_follow_missing" :anchor-digest 42))
             (backup-lease-startup-inject-closing record closing)
             (ok (intake-fails-p
                  (lambda ()
                    (if startup
                        (backup-lease-startup-reopen)
                        (pg::physical-backup-lease-finish-closing request)))))
             (ok (probe-file (pg::physical-source-pin-path name)))
             (ok (eq :closing (getf (gethash name pg::*physical-backup-lease-records*) :phase)))))))))

(deftest backup-lease-handoff-transfers-retention-and-settles-exact-retries
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal)))
         (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
           (declare (ignore bootstrap))
           (backup-lease-startup-save-origin record)
           (let* ((pin (backup-lease-startup-prepare-successor record))
                  (name (pg::physical-source-pin-name pin))
                  (request (getf record :request))
                  (successor (list :name name :anchor-digest
                                   (pg::physical-follow-anchor-digest
                                    (pg::physical-follow-pin-anchor pin))))
                  (closed (pg::physical-backup-lease-handoff request successor)))
             (ok (eq :closed (getf closed :phase)))
             (ok (eq :handed-off (getf closed :outcome)))
             (ok (equal successor (getf closed :successor)))
             (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
             (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok (intake-fails-p (lambda () (pg::physical-backup-lease-abort request))))
             (let ((other (copy-tree successor)))
               (incf (getf other :anchor-digest))
               (ok (intake-fails-p (lambda () (pg::physical-backup-lease-handoff request other)))))
             (pg::physical-source-follow-release name (getf successor :anchor-digest))
             (ok (equal closed (pg::physical-backup-lease-handoff request successor)))
             (backup-lease-startup-reopen)
             (ok (equal closed (pg::physical-backup-lease-handoff request successor)))))))))

(deftest backup-lease-handoff-publisher-rejects-unproved-authority
    (dolist (fault '(:missing :origin :sender :capture :digest :aborted :token))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
           (unless (eq fault :origin) (backup-lease-startup-save-origin record))
           (let* ((pin (unless (eq fault :missing) (backup-lease-startup-prepare-successor record)))
                  (request (getf record :request))
                  (name (getf request :name))
                  (successor (list :name "handoff_successor" :anchor-digest
                                   (if pin (pg::physical-follow-anchor-digest
                                            (pg::physical-follow-pin-anchor pin)) 42)))
                  (closing (pg::physical-backup-lease-copy record)))
             (ecase fault
               ((:missing :origin) nil)
               (:sender (setf (gethash name pg::*physical-source-senders*) :test-owner))
               (:capture (setf (gethash name pg::*physical-backup-lease-capture-owners*) :test-owner))
               (:digest (incf (getf successor :anchor-digest)))
               (:aborted
                (pg::physical-backup-lease-abort request)
                (setf record (gethash name pg::*physical-backup-lease-records*)))
               (:token (setf (char (getf (getf closing :request) :token) 0) #\f)))
             (let ((image (pg::physical-backup-lease-image record)))
               (setf (getf closing :phase) :closing
                     (getf closing :outcome) :handed-off
                     (getf closing :successor) successor)
               (ok (intake-fails-p
                    (lambda () (pg::physical-backup-lease-publish record closing))))
               (ok (equalp image (pg::physical-backup-lease-image
                                  (pg::physical-backup-lease-read
                                   (pg::physical-backup-lease-path request)))))
               (ok (eq (eq fault :origin) (not (null pg::*wal-poisoned*))))
               (unless (eq fault :aborted)
                 (ok (eq bootstrap (gethash name pg::*physical-source-pins*)))))))))))

(deftest backup-lease-successor-assignment-survives-closed-history
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (first bootstrap) (backup-lease-authority-prepare t)
         (declare (ignore bootstrap))
         (backup-lease-startup-save-origin first)
         (let* ((second-request (backup-lease-test-request))
                (pin (backup-lease-startup-prepare-successor first))
                (successor (list :name (pg::physical-source-pin-name pin) :anchor-digest
                                 (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin)))))
           (setf (char (getf second-request :name) 7) #\f)
           (multiple-value-bind (second second-pin)
               (backup-lease-authority-prepare t second-request)
             (pg::physical-backup-lease-handoff (getf first :request) successor)
             (ok (intake-fails-p
                  (lambda () (pg::physical-backup-lease-handoff second-request successor))))
             (ok (eq second-pin (gethash (getf second-request :name) pg::*physical-source-pins*)))
             (let ((closing (copy-tree second))
                   (image (pg::physical-source-pin-encode second-pin)))
               (setf (getf closing :phase) :closing
                     (getf closing :outcome) :handed-off
                     (getf closing :successor) successor)
               (backup-lease-startup-inject-closing second closing)
               (ok (intake-fails-p #'backup-lease-startup-reopen))
               (ok (equalp image (pg::physical-source-pin-encode
                                  (pg::physical-source-pin-read-retirement
                                   (getf second-request :name))))))))))))

(defun backup-lease-handoff-child-body (request successor operation)
  (format nil
          "(let ((pg::*wal-ready* t)
                 (pg::*wal-durable-lsn* 152)
                 (pg::*wal-end-lsn* 152)
                 (pg::*wal-checkpoint-lsn* 152)
                 (pg::*wal-checkpoint-snapshot* '~a)
                 (pg::*wal-segment-namespace* :timeline)
                 (pg::*physical-recovery-state* nil))
             (pg::physical-backup-lease-load-records)
             (pg::physical-source-pin-load)
             (pg::physical-export-origin-load)
             (pg::physical-backup-lease-reconcile-startup)
             (let ((request '~a) (successor '~a))
               ~a))"
          (pg::physical-bootstrap-manifest-text pg::*wal-checkpoint-snapshot*)
          (pg::physical-bootstrap-manifest-text request)
          (pg::physical-bootstrap-manifest-text successor)
          operation))

(deftest backup-lease-handoff-recovers-process-kills-with-successor-retention
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "handoff-cache.XXXXXX" cache-root))))))
        (dolist (cut (append
                      (loop for phase in '(:closing :closed)
                            append (loop for point in '(:before-write :after-write :after-file-fsync
                                                        :after-rename :after-directory-fsync :before-memory)
                                         collect (list :lease phase point)))
                      (loop for point in '(:before-release-unlink :after-release-unlink
                                           :after-release-directory-fsync)
                            collect (list :pin nil point))))
          (call-with-backup-lease-authority-fixture
           (lambda (root)
             (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
               (declare (ignore bootstrap))
               (backup-lease-startup-save-origin record)
               (let* ((pin (backup-lease-startup-prepare-successor record))
                      (request (getf record :request))
                      (name (pg::physical-source-pin-name pin))
                      (image (pg::physical-source-pin-encode pin))
                      (successor (list :name name :anchor-digest
                                       (pg::physical-follow-anchor-digest
                                        (pg::physical-follow-pin-anchor pin))))
                      (log (merge-pathnames "handoff-child.log" root))
                      (operation
                        (format nil
                                "(setf ~a
                                  (lambda (point value)
                                    (when (and (eq point ~s) ~a)
                                      (sb-posix:kill (sb-posix:getpid) 9))))
                               (pg::physical-backup-lease-handoff request successor)"
                                (if (eq (first cut) :lease)
                                    "pg::*physical-backup-lease-test-hook*"
                                    "pg::*physical-source-test-hook*")
                                (third cut)
                                (if (eq (first cut) :lease)
                                    (format nil "(eq (getf value :phase) ~s)" (second cut))
                                    "(equal (pg::physical-source-pin-name value) (getf request :name))")))
                      (result (backup-lease-store-run-child
                               root log (backup-lease-handoff-child-body request successor operation) cache)))
                 (ok (equal '(:signaled 9) result) (format nil "~s; ~a" cut log))
                 (ok (equalp image (pg::physical-source-pin-encode
                                    (pg::physical-source-pin-read-retirement name))))
                 (let ((recovered
                         (backup-lease-store-run-child
                          root log
                          (backup-lease-handoff-child-body
                           request successor
                           "(let ((closed (pg::physical-backup-lease-handoff request successor))
                                (pin (gethash (getf successor :name) pg::*physical-source-pins*)))
                            (assert (eq :closed (getf closed :phase)))
                            (assert (eq :handed-off (getf closed :outcome)))
                            (assert (equal successor (getf closed :successor)))
                            (assert (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
                            (assert (= 152 (gethash pin pg::*wal-snapshot-retention-pins*))))")
                          cache)))
                   (ok (equal '(:exited 0) recovered) (format nil "fresh handoff recovery ~s; ~a" cut log)))
                 (ok (equalp image (pg::physical-source-pin-encode
                                    (pg::physical-source-pin-read-retirement name))))
                 (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))))))))))

(deftest backup-lease-standby-handoff-recovers-a-disk-only-successor
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
         (declare (ignore bootstrap))
         (backup-lease-startup-save-origin record)
         (let* ((pin (backup-lease-startup-prepare-successor record))
                (name (pg::physical-source-pin-name pin))
                (image (pg::physical-source-pin-encode pin))
                (closing (copy-tree record)))
           (setf (getf closing :phase) :closing
                 (getf closing :outcome) :handed-off
                 (getf closing :successor)
                 (list :name name :anchor-digest
                       (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
           (pg::physical-backup-lease-publish record closing)
           (setf pg::*physical-source-pins* (make-hash-table :test #'equal)
                 pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (backup-lease-startup-reopen t)
           (ok (eq :closed (getf (gethash (getf (getf record :request) :name)
                                          pg::*physical-backup-lease-records*) :phase)))
           (ok (not (probe-file (pg::physical-source-pin-path (getf (getf record :request) :name)))))
           (ok (equalp image (pg::physical-source-pin-encode
                              (pg::physical-source-pin-read-retirement name))))
           (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*))))))))

(deftest backup-lease-handoff-reuses-an-interrupted-base-on-a-descendant-source
    (dolist (fork '(152 184))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (let ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal)))
           (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
             (declare (ignore bootstrap))
             (backup-lease-startup-save-origin record)
             (let* ((old (backup-lease-startup-prepare-successor record))
                    (anchor (pg::physical-follow-pin-anchor old))
                    (request (pg::physical-follow-anchor-request anchor))
                    (prefix (pg::read-bounded-regular-file (pg::wal-segment-path 0) 1024))
                    (parent (if (= fork 152) prefix
                                (concatenate '(vector (unsigned-byte 8))
                                             prefix (legacy-checkpoint-record 152))))
                    (eor (pg::wal-encode-end-of-recovery-record fork 42 3 2 1)))
               (pg::physical-source-follow-release "handoff_successor"
                                                   (pg::physical-follow-anchor-digest anchor))
               (pg::timeline-write-history 3 2 fork "promotion")
               (setf pg::*wal-insert-timeline* 3
                     pg::*wal-durable-lsn* (+ fork (length eor))
                     pg::*wal-end-lsn* pg::*wal-durable-lsn*
                     (getf request :source-timeline) 3)
               (pg::wal-atomic-write-bytes (pg::wal-segment-path 0)
                                           (concatenate '(vector (unsigned-byte 8)) parent eor))
               (backup-lease-startup-reopen)
               (let ((interrupted (gethash (getf (getf record :request) :name)
                                           pg::*physical-backup-lease-records*)))
                 (ok (eq :interrupted (getf interrupted :phase)))
                 (ok (equal (getf record :base) (getf interrupted :base))))
               (let* ((pin (pg::physical-source-follow-prepare "handoff_descendant" request))
                      (successor (list :name "handoff_descendant" :anchor-digest
                                       (pg::physical-follow-anchor-digest
                                        (pg::physical-follow-pin-anchor pin))))
                      (closed (pg::physical-backup-lease-handoff (getf record :request) successor)))
                 (ok (eq :closed (getf closed :phase)))
                 (ok (eq :handed-off (getf closed :outcome)))
                 (ok (= 2 (getf (pg::physical-follow-pin-anchor pin) :base-timeline)))
                 (ok (= 3 (pg::physical-source-pin-timeline pin)))
                 (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))
                 (backup-lease-startup-reopen)
                 (ok (equal closed
                            (pg::physical-backup-lease-handoff
                             (getf record :request) successor)))))))))))

(deftest backup-lease-closing-successor-name-denies-bootstrap-recreation
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
         (declare (ignore bootstrap))
         (let* ((closing (copy-tree record))
                (name "handoff_missing_successor"))
           (setf (getf closing :phase) :closing
                 (getf closing :outcome) :handed-off
                 (getf closing :successor) (list :name name :anchor-digest 42))
           (backup-lease-startup-inject-closing record closing)
           (ok (intake-fails-p (lambda () (pg::physical-source-pin-create name))))
           (ok (not (probe-file (pg::physical-source-pin-path name))))
           (ok (null (gethash name pg::*physical-source-pins*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-closing-successor-blocks-mutations-and-keeps-retention
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal)))
         (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
           (declare (ignore bootstrap))
           (backup-lease-startup-save-origin record)
           (let* ((pin (backup-lease-startup-prepare-successor record))
                  (name (pg::physical-source-pin-name pin))
                  (anchor (pg::physical-follow-pin-anchor pin))
                  (digest (pg::physical-follow-anchor-digest anchor))
                  (request (pg::physical-follow-anchor-request anchor))
                  (closing (copy-tree record))
                  (image (pg::physical-source-pin-encode pin)))
             (setf (getf closing :phase) :closing
                   (getf closing :outcome) :handed-off
                   (getf closing :successor) (list :name name :anchor-digest digest))
             (pg::physical-backup-lease-publish record closing)
             (dolist (operation
                      (list (lambda () (pg::physical-source-follow-release name digest))
                            (lambda () (pg::physical-source-follow-abort name request))
                            (lambda () (pg::physical-source-follow-prepare name request))
                            (lambda ()
                              (pg::with-write-lock
                                (pg::with-wal-critical (pg::physical-source-pin-unlink pin))))
                            (lambda ()
                              (sb-thread:with-mutex (pg::*wal-lock*)
                                (pg::physical-source-check-sender-under-lock name nil)))))
               (ok (intake-fails-p operation))
               (ok (not (gethash name pg::*physical-follow-aborted-attempts*)))
               (ok (equalp image (pg::physical-source-pin-encode
                                  (pg::physical-source-pin-read-retirement name)))))
             (let ((pg::*wal-durable-lsn* 153))
               (sb-thread:with-mutex (pg::*wal-lock*)
                 (pg::physical-source-invalidate-lagging-pins-under-wal-lock 0)))
             (ok (not (pg::physical-source-pin-lost-p pin)))
             (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok (equalp image (pg::physical-source-pin-encode
                                (pg::physical-source-pin-read-retirement name))))
             (pg::physical-backup-lease-finish-closing (getf record :request))
             (ok (eq :released (pg::physical-source-follow-release name digest)))))))))

(deftest backup-lease-handed-off-closing-preserves-a-proved-successor
    (dolist (startup '(nil t))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (let ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal)))
           (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
             (declare (ignore bootstrap))
             (backup-lease-startup-save-origin record)
             (let* ((pin (backup-lease-startup-prepare-successor record))
                    (name (pg::physical-source-pin-name pin))
                    (digest (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin)))
                    (request (getf record :request))
                    (closing (copy-tree record)))
               (setf (getf closing :phase) :closing
                     (getf closing :outcome) :handed-off
                     (getf closing :successor) (list :name name :anchor-digest digest))
               (pg::physical-backup-lease-publish record closing)
               (if startup
                   (backup-lease-startup-reopen)
                   (pg::physical-backup-lease-finish-closing request))
               (ok (eq :closed (getf (gethash (getf request :name)
                                              pg::*physical-backup-lease-records*) :phase)))
               (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
               (let ((successor (gethash name pg::*physical-source-pins*)))
                 (ok (pg::physical-follow-pin-ready-p successor))
                 (ok (= 152 (gethash successor pg::*wal-snapshot-retention-pins*))))
               (ok (eq :released (pg::physical-source-follow-release name digest)))
               (ok (eq :closed (getf (pg::physical-backup-lease-finish-closing request) :phase)))
               (backup-lease-startup-reopen)
               (ok (not (probe-file (pg::physical-source-pin-path name)))))))))))

(deftest backup-lease-handed-off-closing-validates-corruption-and-defers-known-loss
    (dolist (mode '(:live :primary :standby))
      (dolist (fault '(:lost :retaining :advanced :digest :manifest :sender :disk))
        (call-with-backup-lease-authority-fixture
         (lambda (root)
           (declare (ignore root))
           (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
             (backup-lease-startup-save-origin record)
             (let* ((pin (backup-lease-startup-prepare-successor record))
                    (name (pg::physical-source-pin-name pin))
                    (request (getf record :request))
                    (closing (copy-tree record))
                    (bootstrap-image (pg::physical-source-pin-encode bootstrap)))
               (setf (getf closing :phase) :closing
                     (getf closing :outcome) :handed-off
                     (getf closing :successor)
                     (list :name name :anchor-digest
                           (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
               (when (eq fault :digest)
                 (incf (getf (getf closing :successor) :anchor-digest)))
               (backup-lease-startup-inject-closing record closing)
               (ecase fault
                 (:lost (setf (pg::physical-source-pin-lost-p pin) t))
                 (:retaining
                  (setf (pg::physical-follow-pin-stage pin) :retaining
                        (pg::physical-source-pin-confirmed-lsn pin) nil
                        (pg::physical-source-pin-restart-lsn pin) 0))
                 (:advanced
                  (setf (pg::physical-source-pin-confirmed-lsn pin) 153
                        (pg::physical-source-pin-restart-lsn pin) 153))
                 (:digest nil)
                 (:manifest
                  (incf (getf (pg::physical-follow-pin-anchor pin) :manifest-digest))
                  (incf (pg::physical-source-pin-manifest-digest pin)))
                 (:sender (setf (gethash name pg::*physical-source-senders*) :test-owner))
                 (:disk (delete-file (pg::physical-source-pin-path name))))
               (unless (member fault '(:digest :sender :disk))
                 (pg::physical-source-pin-save pin))
               (let ((pending (and (not (eq mode :live))
                                   (member fault '(:lost :disk)))))
                 (ok (eq (intake-fails-p
                          (lambda ()
                            (if (eq mode :live)
                                (pg::physical-backup-lease-finish-closing request)
                                (backup-lease-startup-reopen (eq mode :standby)))))
                         (null pending)))
                 (let ((registered (gethash (getf request :name)
                                            pg::*physical-source-pins*)))
                   (ok registered)
                   (ok (not (pg::physical-source-pin-lost-p registered)))
                   (if (and pending (eq mode :standby))
                       (ok (null (gethash registered pg::*wal-snapshot-retention-pins*)))
                       (ok (= 32 (gethash registered pg::*wal-snapshot-retention-pins*)))))
                 (when pending
                   (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))))
               (ok (equalp bootstrap-image
                           (pg::physical-source-pin-encode
                            (pg::physical-source-pin-read-retirement (getf request :name)))))
               (ok (eq :closing (getf (gethash (getf request :name)
                                               pg::*physical-backup-lease-records*) :phase))))))))))

(deftest backup-lease-startup-finishes-closing-with-or-without-a-pin-on-both-roles
    (dolist (standby '(nil t))
      (dolist (present '(nil t))
        (call-with-backup-lease-authority-fixture
         (lambda (root)
           (declare (ignore root))
           (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
             (declare (ignore pin))
             (let* ((name (backup-lease-store-test-name record))
                    (request (getf record :request))
                    (path (pg::physical-source-pin-path name))
                    (closing (copy-tree record)))
               (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
               (pg::physical-backup-lease-publish record closing)
               (unless present (delete-file path))
               (delete-file (pg::wal-segment-path 0))
               (backup-lease-startup-reopen standby)
               (let ((actual (gethash name pg::*physical-backup-lease-records*)))
                 (ok (eq :closed (getf actual :phase)))
                 (ok (equal request (getf actual :request))))
               (ok (not (probe-file path)))
               (ok (null (gethash name pg::*physical-source-pins*)))
               (ok (equal (gethash name pg::*physical-backup-lease-records*)
                          (pg::physical-backup-lease-finish-closing request)))
               (ok (not pg::*wal-poisoned*)))))))))

(deftest backup-lease-startup-prevalidates-all-ready-evidence-before-retiring-another
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (multiple-value-bind (first first-pin) (backup-lease-authority-prepare t)
         (declare (ignore first-pin))
         (backup-lease-startup-save-origin first)
         (let* ((second-request (backup-lease-test-request))
                (closing (copy-tree first))
                (first-name (backup-lease-store-test-name first)))
           (setf (char (getf second-request :name) 7) #\f)
           (multiple-value-bind (second second-pin)
               (backup-lease-authority-prepare t second-request 992)
             (declare (ignore second-pin))
             (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
             (pg::physical-backup-lease-publish first closing)
             (pg::physical-backup-lease-load-records)
             (pg::physical-source-pin-load)
             (pg::physical-export-origin-load)
             (ok (intake-fails-p #'pg::physical-backup-lease-reconcile-startup))
             (ok (probe-file (pg::physical-source-pin-path first-name)))
             (ok (eq :closing (getf (gethash first-name pg::*physical-backup-lease-records*) :phase)))
             (let ((pg::*wal-poisoned* nil))
               (backup-lease-startup-save-origin second)
               (pg::physical-backup-lease-reconcile-startup)
               (ok (eq :closed (getf (gethash first-name pg::*physical-backup-lease-records*) :phase)))
               (ok (not (probe-file (pg::physical-source-pin-path first-name))))
               (ok (eq :ready
                       (getf (gethash (backup-lease-store-test-name second)
                                      pg::*physical-backup-lease-records*) :phase))))))))))

(deftest backup-lease-startup-refuses-a-pin-reappearing-under-a-closed-tombstone
    (dolist (standby '(nil t))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record pin) (backup-lease-authority-prepare t)
           (let* ((request (getf record :request))
                  (name (getf request :name))
                  (closing (copy-tree record)))
             (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
             (pg::physical-backup-lease-publish record closing)
             (pg::physical-backup-lease-finish-closing request)
             (pg::with-wal-critical (pg::physical-source-pin-save pin))
             (ok (intake-fails-p (lambda () (backup-lease-startup-reopen standby))))
             (ok (probe-file (pg::physical-source-pin-path name)))
             (ok (eq :closed (getf (gethash name pg::*physical-backup-lease-records*) :phase)))
             (ok pg::*wal-poisoned*)))))))
