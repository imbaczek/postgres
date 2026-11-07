(in-package #:pg-physical-tests)

(cl-spec:defproperty selected-lost-startup-preserves-closing-cleanup-debt
    ((allocated boolean) (closing boolean))
  (:about pg::physical-backup-lease-load-startup-pins
          pg::physical-backup-lease-reconcile-startup)
  (:trials (:normal 100))
  (call-with-backup-lease-selected-observation-fixture
   (lambda (root parent pin row)
     (declare (ignore root))
     (let* ((request (getf parent :request))
            (name (getf request :name))
            (selected-name (pg::physical-source-pin-name pin))
            (parent-path (pg::physical-backup-lease-path request))
            (parent-before (lineage-test-bytes parent-path))
            (bootstrap-before (when closing (lineage-test-bytes (pg::physical-source-pin-path name))))
            (row-before (when row (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
       (pg::with-write-lock
         (sb-thread:with-mutex (pg::*wal-lock*)
           (setf (pg::physical-source-pin-lost-p pin) t)
           (pg::physical-source-pin-save pin)))
       (let ((selected-before (lineage-test-bytes (pg::physical-source-pin-path selected-name))))
         (and
          (handler-case (progn (backup-lease-startup-reopen nil)
                               (backup-lease-startup-reopen nil) t)
            (error () nil))
          (let ((selected (gethash selected-name pg::*physical-source-pins*))
                (bootstrap (gethash name pg::*physical-source-pins*))
                (current (gethash name pg::*physical-backup-lease-records*)))
            (and (eq (if closing :closing :closed) (getf current :phase))
                 (eq :handed-off (getf current :outcome))
                 (equal parent current)
                 (pg::physical-follow-pin-ready-p selected)
                 (pg::physical-source-pin-lost-p selected)
                 (null (gethash selected pg::*wal-snapshot-retention-pins*))
                 (if closing
                     (and bootstrap (not (pg::physical-source-pin-lost-p bootstrap))
                          (= (getf (getf parent :base) :checkpoint-start)
                             (gethash bootstrap pg::*wal-snapshot-retention-pins*))
                          (equalp bootstrap-before
                                  (lineage-test-bytes (pg::physical-source-pin-path name))))
                     (null bootstrap))))
          (equalp parent-before (lineage-test-bytes parent-path))
          (equalp selected-before (lineage-test-bytes (pg::physical-source-pin-path selected-name)))
          (or (null row)
              (equalp row-before
                      (lineage-test-bytes (pg::physical-backup-lease-allocation-path row))))
          (not pg::*wal-poisoned*)))))
   allocated closing))

(deftest generated-selected-lost-startup-cleanup-debt-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'selected-lost-startup-preserves-closing-cleanup-debt
                                         :seed 195 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(defun backup-lease-selected-startup-add-suffix (pin)
  (let* ((start (pg::physical-source-pin-confirmed-lsn pin))
         (bytes (pg::wal-encode-record pg::+wal-record-commit+ 3 start
                                       (pg::wal-encode-commit-timestamp 0)))
         (end (+ start (length bytes))))
    (pg::with-write-lock
      (sb-thread:with-mutex (pg::*wal-lock*)
        (with-open-file (stream (pg::wal-segment-path 0) :direction :output
                                                         :if-exists :append
                                                         :element-type '(unsigned-byte 8))
          (unless (= start (file-length stream)) (error "selected suffix fixture has an unexpected prefix"))
          (write-sequence bytes stream)
          (pg::fsync-stream stream))
        (setf pg::*wal-end-lsn* end pg::*wal-durable-lsn* end)))
    end))

(deftest selected-startup-persists-unavailability-and-keeps-cleanup-reachable
    (dolist (allocated '(nil t))
      (dolist (closing '(nil t))
        (dolist (fault '(:lost :writer :missing :older-carrier))
          (call-with-backup-lease-selected-observation-fixture
           (lambda (root parent pin row)
             (declare (ignore root))
             (let* ((request (getf parent :request))
                    (name (pg::physical-source-pin-name pin))
                    (parent-before (lineage-test-bytes (pg::physical-backup-lease-path request)))
                    (bootstrap-before
                      (when closing (lineage-test-bytes (pg::physical-source-pin-path (getf request :name)))))
                    (original (pg::physical-source-pin-encode pin))
                    (row-before (when row (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
               (case fault
                 (:lost
                  (pg::with-write-lock
                    (sb-thread:with-mutex (pg::*wal-lock*)
                      (setf (pg::physical-source-pin-lost-p pin) t)
                      (pg::physical-source-pin-save pin))))
                 ((:missing :older-carrier)
                  (backup-lease-selected-startup-add-suffix pin)
                  (when (eq fault :older-carrier)
                    (pg::wal-atomic-write-bytes
                     (pg::wal-segment-path 0 1 :timeline) (lineage-test-bytes (pg::wal-segment-path 0))))
                  (delete-file (pg::wal-segment-path 0))))
               (let ((pg::*wal-insert-timeline* (if (eq fault :writer) 3 2)))
                 (dotimes (attempt 2)
                   (declare (ignorable attempt))
                   (backup-lease-startup-reopen nil)
                   (let* ((current (gethash name pg::*physical-source-pins*))
                          (normalized (copy-structure current)))
                     (ok (pg::physical-follow-pin-ready-p current))
                     (ok (pg::physical-source-pin-lost-p current))
                     (ok (null (gethash current pg::*wal-snapshot-retention-pins*)))
                     (setf (pg::physical-source-pin-lost-p normalized) nil)
                     (ok (equalp original (pg::physical-source-pin-encode normalized)))
                     (when closing
                       (let ((bootstrap (gethash (getf request :name) pg::*physical-source-pins*)))
                         (ok (not (pg::physical-source-pin-lost-p bootstrap)))
                         (ok (= 32 (gethash bootstrap pg::*wal-snapshot-retention-pins*)))))))
                 (ok (equalp parent-before (lineage-test-bytes (pg::physical-backup-lease-path request))))
                 (when bootstrap-before
                   (ok (equalp bootstrap-before
                               (lineage-test-bytes (pg::physical-source-pin-path (getf request :name))))))
                 (when row-before
                   (ok (equalp row-before
                               (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
                 (if closing
                     (progn
                       (ok (signals (pg::physical-source-follow-release
                                     name (getf (getf parent :successor) :anchor-digest))))
                       (ok (signals (pg::physical-source-follow-prepare
                                     name (pg::physical-follow-anchor-request
                                           (pg::physical-follow-pin-anchor pin))))))
                     (progn
                       (ok (eq :released (pg::physical-source-follow-release
                                          name (getf (getf parent :successor) :anchor-digest))))
                       (ok (eq :absent (pg::physical-source-follow-release
                                        name (getf (getf parent :successor) :anchor-digest))))))
                 (ok (not pg::*wal-poisoned*)))))
           allocated closing)))))

(deftest selected-startup-preserves-hard-source-failures-before-loss-publication
    (dolist (closing '(nil t))
      (dolist (fault '(:truncated :corrupt :symlink :fifo :history :namespace :future))
        (call-with-backup-lease-selected-observation-fixture
         (lambda (root parent pin row)
           (declare (ignore root row))
           (backup-lease-selected-startup-add-suffix pin)
           (let* ((path (pg::wal-segment-path 0))
                  (name (pg::physical-source-pin-name pin))
                  (selected-before (lineage-test-bytes (pg::physical-source-pin-path name)))
                  (parent-before (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request))))
                  (bytes (lineage-test-bytes path)))
             (ecase fault
               (:truncated (pg::wal-atomic-write-bytes path (subseq bytes 0 160)))
               (:corrupt
                (setf (aref bytes (+ 152 28)) (logxor 1 (aref bytes (+ 152 28))))
                (pg::wal-atomic-write-bytes path bytes))
               (:symlink
                (let ((older (pg::wal-segment-path 0 1 :timeline)))
                  (pg::wal-atomic-write-bytes older bytes)
                  (delete-file path)
                  (sb-posix:symlink (namestring older) (namestring path))))
               (:fifo (delete-file path) (sb-posix:mkfifo (namestring path) #o600))
               (:history (delete-file (pg::timeline-history-path 2 pg::*pgdata*)))
               ((:namespace :future) nil))
             (let ((pg::*wal-segment-namespace* (if (eq fault :namespace) :legacy :timeline))
                   (pg::*wal-durable-lsn* (if (eq fault :future) 151 pg::*wal-durable-lsn*)))
               (ok (signals (backup-lease-startup-reopen nil))))
             (ok (equalp selected-before (lineage-test-bytes (pg::physical-source-pin-path name))))
             (ok (equalp parent-before
                         (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request)))))
             (ok (eq (not (null pg::*wal-poisoned*))
                     (not (null (member fault '(:truncated :corrupt :symlink :fifo :history))))))))
         t closing))))

(deftest selected-startup-keeps-standby-role-and-range-boundaries
    (dolist (allocated '(nil t))
      (dolist (closing '(nil t))
        (dolist (nonempty '(nil t))
          (call-with-backup-lease-selected-observation-fixture
           (lambda (root parent pin row)
             (declare (ignore root row))
             (let* ((name (pg::physical-source-pin-name pin))
                    (request (getf parent :request))
                    (selected-before (lineage-test-bytes (pg::physical-source-pin-path name)))
                    (bootstrap-before
                      (when closing (lineage-test-bytes (pg::physical-source-pin-path (getf request :name))))))
               (when nonempty
                 (backup-lease-selected-startup-add-suffix pin)
                 (unless closing (delete-file (pg::wal-segment-path 0))))
               (let ((pg::*physical-recovery-state* (list :phase 2)))
                 (backup-lease-startup-reopen t)
                 (ok (null (gethash name pg::*physical-source-pins*)))
                 (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
                 (ok (equalp selected-before (lineage-test-bytes (pg::physical-source-pin-path name))))
                 (let ((current (gethash (getf request :name) pg::*physical-backup-lease-records*))
                       (bootstrap (gethash (getf request :name) pg::*physical-source-pins*)))
                   (ok (eq (if (and closing nonempty) :closing :closed) (getf current :phase)))
                   (when (and closing nonempty)
                     (ok (not (pg::physical-source-pin-lost-p bootstrap)))
                     (ok (null (gethash bootstrap pg::*wal-snapshot-retention-pins*)))
                     (ok (equalp bootstrap-before
                                 (lineage-test-bytes (pg::physical-source-pin-path (getf request :name))))))))
               (backup-lease-startup-reopen nil)
               (let ((selected (gethash name pg::*physical-source-pins*)))
                 (ok (eq :closed
                         (getf (gethash (getf request :name) pg::*physical-backup-lease-records*) :phase)))
                 (ok (eq (not (null (pg::physical-source-pin-lost-p selected)))
                         (and (not closing) nonempty)))
                 (ok (eql (unless (pg::physical-source-pin-lost-p selected)
                            (pg::physical-source-pin-restart-lsn selected))
                          (gethash selected pg::*wal-snapshot-retention-pins*))))
               (ok (not pg::*wal-poisoned*))))
           allocated closing)))))

(deftest selected-startup-rejects-wal-directory-changes-during-missing-probes
    (dolist (fault '(:missing :symlink :replacement :restore))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore row))
         (backup-lease-selected-startup-add-suffix pin)
         (delete-file (pg::wal-segment-path 0))
         (let* ((directory (pg::wal-directory))
                (saved (merge-pathnames "pg_wal.saved/" root))
                (path (string-right-trim "/" (namestring directory)))
                (saved-path (string-right-trim "/" (namestring saved)))
                (name (pg::physical-source-pin-name pin))
                (before (lineage-test-bytes (pg::physical-source-pin-path name)))
                (parent-before (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request))))
                (original (symbol-function 'pg::wal-lineage-open-segment))
                (injected 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-lineage-open-segment)
                        (lambda (&rest arguments)
                          (handler-bind
                              ((pg::wal-lineage-segment-unavailable
                                 (lambda (condition)
                                   (declare (ignore condition))
                                   (when (zerop injected)
                                     (incf injected)
                                     (sb-posix:rename path saved-path)
                                     (ecase fault
                                       (:missing)
                                       (:symlink (sb-posix:symlink saved-path path))
                                       (:replacement (sb-posix:mkdir path #o700))
                                       (:restore (sb-posix:rename saved-path path)))))))
                            (apply original arguments))))
                  (let ((condition (handler-case (progn (backup-lease-startup-reopen nil) nil)
                                     (error (condition) condition))))
                    (ok (= 1 injected))
                    (ok (typep condition 'error))
                    (ok (search "startup WAL" (princ-to-string condition))))
                  (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
                  (ok (equalp parent-before
                              (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request)))))
                  (ok pg::*wal-poisoned*))
             (setf (symbol-function 'pg::wal-lineage-open-segment) original)
             (when (and (plusp injected) (not (eq fault :restore)))
               (when (eq fault :symlink) (sb-posix:unlink path))
               (when (eq fault :replacement) (sb-posix:rmdir path))
               (sb-posix:rename saved-path path)))))
       t t)))

(deftest selected-known-loss-reopens-after-old-writer-history-is-gone
    (dolist (allocated '(nil t))
      (dolist (closing '(nil t))
        (call-with-backup-lease-selected-observation-fixture
         (lambda (root parent pin row)
           (declare (ignore root row))
           (pg::timeline-write-history 3 2 152 "promotion")
           (delete-file (pg::timeline-history-path 2 pg::*pgdata*))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf (pg::physical-source-pin-lost-p pin) t)
               (pg::physical-source-pin-save pin)))
           (let ((pg::*wal-insert-timeline* 3))
             (backup-lease-startup-reopen nil)
             (let ((selected (gethash (pg::physical-source-pin-name pin) pg::*physical-source-pins*)))
               (ok (pg::physical-follow-pin-ready-p selected))
               (ok (pg::physical-source-pin-lost-p selected))
               (ok (null (gethash selected pg::*wal-snapshot-retention-pins*))))
             (ok (eq (if closing :closing :closed)
                     (getf (gethash (getf (getf parent :request) :name)
                                    pg::*physical-backup-lease-records*) :phase)))
             (ok (not (probe-file (pg::timeline-history-path 2 pg::*pgdata*))))
             (ok (probe-file (pg::timeline-history-path 3 pg::*pgdata*)))
             (ok (not pg::*wal-poisoned*))))
         allocated closing))))

(deftest selected-startup-directory-observation-supports-held-image-descriptors
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (dolist (name '("base/" "global/" "pg_wal/"))
        (let ((path (merge-pathnames name root)))
          (ensure-directories-exist path)
          (sb-posix:chmod (namestring path) #o700)))
      (sb-sys:without-interrupts
        (let ((fd (sb-posix:open (namestring root)
                                 (logior sb-posix:o-rdonly sb-posix:o-directory #o2000000))))
          (unwind-protect
               (sb-sys:with-local-interrupts
                 (pg::call-with-physical-backup-image-directory
                  fd
                  (lambda (scope-root)
                    (let* ((pg::*pgdata* scope-root)
                           (before (pg::physical-backup-lease-startup-wal-directory-state))
                           (path (namestring (merge-pathnames "pg_wal" root)))
                           (saved (namestring (merge-pathnames "pg_wal.saved" root))))
                      (ok (equal before (pg::physical-backup-lease-startup-wal-directory-state)))
                      (ok (= (sb-posix:geteuid) (sixth before)))
                      (ok (plusp (car (last before))))
                      (sb-posix:rename path saved)
                      (sb-posix:mkdir path #o700)
                      (unwind-protect
                           (ok (signals (pg::physical-backup-lease-startup-wal-directory-state)))
                        (sb-posix:rmdir path)
                        (sb-posix:rename saved path))
                      (ok (not (equal before
                                      (pg::physical-backup-lease-startup-wal-directory-state))))))))
            (sb-posix:close fd))))))

(deftest selected-startup-fences-nonlocal-loss-and-install-exits
    (dolist (allocated '(nil t))
      (dolist (closing '(nil t))
        (dolist (point '(:before-save :after-save :before-startup-pin-install :after-startup-pin-install))
          (call-with-backup-lease-selected-observation-fixture
           (lambda (root parent pin row)
             (declare (ignore root row))
             (backup-lease-selected-startup-add-suffix pin)
             (delete-file (pg::wal-segment-path 0))
             (let* ((name (pg::physical-source-pin-name pin))
                    (request (getf parent :request))
                    (parent-before (lineage-test-bytes (pg::physical-backup-lease-path request)))
                    (seen 0)
                    (pg::*physical-source-test-hook*
                      (lambda (actual candidate)
                        (when (and (zerop seen) (eq actual point)
                                   (equal name (pg::physical-source-pin-name candidate)))
                          (incf seen)
                          (throw 'selected-loss-cut :cut))))
                    (pg::*physical-backup-lease-test-hook*
                      (lambda (actual record)
                        (declare (ignore record))
                        (when (and (zerop seen) (eq actual point))
                          (incf seen)
                          (throw 'selected-loss-cut :cut)))))
               (ok (intake-fails-p
                    (lambda () (catch 'selected-loss-cut (backup-lease-startup-reopen nil)))))
               (ok (= 1 seen))
               (ok pg::*wal-poisoned*)
               (ok (eq (not (null (pg::physical-source-pin-lost-p
                                   (pg::physical-source-pin-read-raw
                                    (pg::physical-source-pin-path name) :stabilize nil))))
                       (not (eq point :before-save))))
               (ok (equalp parent-before (lineage-test-bytes (pg::physical-backup-lease-path request))))
               (if (eq point :after-startup-pin-install)
                   (progn
                     (ok (pg::physical-source-pin-lost-p (gethash name pg::*physical-source-pins*)))
                     (ok (null (gethash pin pg::*wal-snapshot-retention-pins*))))
                   (progn
                     (ok (eq pin (gethash name pg::*physical-source-pins*)))
                     (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))))
               (when closing
                 (let ((bootstrap (gethash (getf request :name) pg::*physical-source-pins*)))
                   (ok (not (pg::physical-source-pin-lost-p bootstrap)))
                   (ok (= 32 (gethash bootstrap pg::*wal-snapshot-retention-pins*)))))
               (let ((pg::*wal-poisoned* nil))
                 (backup-lease-startup-reopen nil)
                 (let ((selected (gethash name pg::*physical-source-pins*)))
                   (ok (pg::physical-source-pin-lost-p selected))
                   (ok (null (gethash selected pg::*wal-snapshot-retention-pins*))))
                 (ok (eq (if closing :closing :closed)
                         (getf (gethash (getf request :name) pg::*physical-backup-lease-records*) :phase)))
                 (ok (not pg::*wal-poisoned*)))))
           allocated closing)))))

(deftest selected-startup-fences-file-and-directory-sync-failures
    (dolist (phase '(:file :directory))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root row))
         (backup-lease-selected-startup-add-suffix pin)
         (delete-file (pg::wal-segment-path 0))
         (let* ((path (pg::physical-source-pin-path (pg::physical-source-pin-name pin)))
                (original (symbol-function 'pg::atomic-replace-file))
                (seen 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::atomic-replace-file)
                        (lambda (target temporary directory writer &rest options)
                          (if (equal (pathname target) path)
                              (apply original target temporary directory writer
                                     (append
                                      (list (if (eq phase :file) :fsync-function :fsync-directory-function)
                                            (lambda (value)
                                              (declare (ignore value))
                                              (incf seen)
                                              (error "selected startup sync failure")))
                                      options))
                              (apply original target temporary directory writer options))))
                  (ok (signals (backup-lease-startup-reopen nil)))
                  (ok (= 1 seen))
                  (ok pg::*wal-poisoned*)
                  (ok (eq pin (gethash (pg::physical-source-pin-name pin) pg::*physical-source-pins*)))
                  (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))
                  (ok (eq (not (null (pg::physical-source-pin-lost-p
                                      (pg::physical-source-pin-read-raw path :stabilize nil))))
                          (eq phase :directory))))
             (setf (symbol-function 'pg::atomic-replace-file) original))
           (let ((pg::*wal-poisoned* nil))
             (backup-lease-startup-reopen nil)
             (ok (pg::physical-source-pin-lost-p
                  (gethash (pg::physical-source-pin-name pin) pg::*physical-source-pins*)))
             (ok (eq :closing
                     (getf (gethash (getf (getf parent :request) :name)
                                    pg::*physical-backup-lease-records*) :phase)))
             (ok (not pg::*wal-poisoned*)))))
       t t)))

(deftest selected-startup-preflights-later-corrupt-ordinary-wal-before-any-loss-write
    (call-with-backup-lease-selected-observation-fixture
     (lambda (root parent pin row)
       (declare (ignore root row))
       (let* ((size pg::+wal-segment-size+)
              (first (pg::wal-segment-path 0))
              (second (pg::wal-segment-path 1))
              (filler (lineage-test-record 152 (- size 152)))
              (tail (pg::wal-encode-record pg::+wal-record-commit+ 3 size
                                           (pg::wal-encode-commit-timestamp 0)))
              (coordinates (pg::physical-follow-anchor-request (pg::physical-follow-pin-anchor pin))))
         (with-open-file (stream first :direction :output :if-exists :append
                                       :element-type '(unsigned-byte 8))
           (write-sequence filler stream)
           (pg::fsync-stream stream))
         (pg::wal-atomic-write-bytes second tail)
         (setf pg::*wal-end-lsn* (+ size (length tail))
               pg::*wal-durable-lsn* pg::*wal-end-lsn*
               (getf coordinates :receive-lsn) size
               (getf coordinates :receive-digest) 123
               (getf coordinates :overlap-start) 0
               (getf coordinates :overlap-digest)
               (pg::physical-store-update-digest #xCBF29CE484222325 (lineage-test-bytes first)))
         (let* ((ordinary (pg::physical-source-follow-prepare "zz_ordinary_corrupt" coordinates))
                (selected-name (pg::physical-source-pin-name pin))
                (before (lineage-test-bytes (pg::physical-source-pin-path selected-name)))
                (parent-before (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request))))
                (original (symbol-function 'pg::physical-backup-lease-startup-selected-state))
                (planned 0)
                (saved 0)
                (pg::*physical-source-test-hook*
                  (lambda (point candidate)
                    (when (and (eq point :after-save)
                               (equal selected-name (pg::physical-source-pin-name candidate)))
                      (incf saved)))))
           (delete-file first)
           (setf (aref tail 28) (logxor 1 (aref tail 28)))
           (pg::wal-atomic-write-bytes second tail)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-backup-lease-startup-selected-state)
                        (lambda (&rest arguments)
                          (let ((state (apply original arguments)))
                            (when (eq state :missing-carrier) (incf planned))
                            state)))
                  (ok (signals (backup-lease-startup-reopen nil)))
                  (ok (= 1 planned))
                  (ok (zerop saved))
                  (ok pg::*wal-poisoned*)
                  (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path selected-name))))
                  (ok (equalp parent-before
                              (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request)))))
                  (ok (eq pin (gethash selected-name pg::*physical-source-pins*)))
                  (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))
                  (ok (eq ordinary (gethash "zz_ordinary_corrupt" pg::*physical-source-pins*)))
                  (ok (= size (gethash ordinary pg::*wal-snapshot-retention-pins*))))
             (setf (symbol-function 'pg::physical-backup-lease-startup-selected-state) original)))))
     t t))

(defun selected-startup-recovery-child-body (parent pin allocated closing cut setup verify
                                             &optional (end-lsn 192))
  (format nil
          "(let ((pg::*catalog* (pg::make-catalog :cluster-identity \"backup-lease-codec\"))
                 (pg::*wal-insert-timeline* 2)
                 (pg::*wal-ready* t)
                 (pg::*wal-durable-lsn* ~d)
                 (pg::*wal-end-lsn* ~d)
                 (pg::*wal-checkpoint-lsn* 152)
                 (pg::*wal-checkpoint-snapshot* '~a)
                 (pg::*wal-segment-namespace* :timeline)
                 (pg::*physical-recovery-state* nil)
                 (parent '~a)
                 (coordinates '~a)
                 (allocated ~s) (closing ~s) (cut ~s))
             (declare (ignorable parent coordinates))
             (flet ((mark (kind)
                      (format t \"SELECTED-UNAVAILABLE-CHILD-~~a ~~s~~%\"
                              kind (list (sb-posix:getpid) (namestring pg::*pgdata*)
                                         allocated closing cut))
                      (finish-output))
                    (stop-at-cut ()
                      (format t \"SELECTED-UNAVAILABLE-CHILD-CUT ~~s~~%\"
                              (list (sb-posix:getpid) (namestring pg::*pgdata*)
                                    allocated closing cut))
                      (finish-output)
                      (sb-posix:kill (sb-posix:getpid) 9)))
               (mark :start)
               ~a
               (pg::physical-backup-lease-load-records)
               (pg::physical-source-pin-load)
               (pg::physical-export-origin-load)
               (pg::physical-backup-lease-reconcile-startup)
               ~a
               (mark :verified)))"
          end-lsn end-lsn
          (pg::physical-bootstrap-manifest-text pg::*wal-checkpoint-snapshot*)
          (pg::physical-bootstrap-manifest-text parent)
          (pg::physical-bootstrap-manifest-text
           (pg::physical-follow-anchor-request (pg::physical-follow-pin-anchor pin)))
          allocated closing cut setup verify))

(defun selected-startup-recovery-cut-setup (name cut)
  (if (member cut '(:before-startup-pin-install :after-startup-pin-install))
      (format nil
              "(setf pg::*physical-backup-lease-test-hook*
                     (lambda (point record)
                       (declare (ignore record))
                       (when (eq point ~s) (stop-at-cut))))"
              cut)
      (format nil
              "(let ((original (symbol-function 'pg::atomic-replace-file))
                     (path (pg::physical-source-pin-path ~s)))
                 (setf (symbol-function 'pg::atomic-replace-file)
                       (lambda (target temporary directory writer &rest options)
                         (if (equal (pathname target) path)
                             (apply original target temporary directory writer
                                    (append
                                     (list ~s
                                           (lambda (value)
                                             ~a
                                             (stop-at-cut)))
                                     options))
                             (apply original target temporary directory writer options)))))"
              name
              (if (member cut '(:before-file-fsync :after-file-fsync))
                  :fsync-function :fsync-directory-function)
              (case cut
                (:after-file-fsync "(sb-posix:fsync value)")
                (:after-directory-fsync "(pg::fsync-directory value)")
                (otherwise "(declare (ignore value))")))))

(defun selected-startup-recovery-verification (&optional (bootstrap-present t))
  (format nil
          "(let* ((request (getf parent :request))
          (name (getf (getf parent :successor) :name))
          (selected (gethash name pg::*physical-source-pins*))
          (bootstrap (gethash (getf request :name) pg::*physical-source-pins*))
          (current (gethash (getf request :name) pg::*physical-backup-lease-records*)))
     (assert (equal parent current))
     (assert (pg::physical-follow-pin-ready-p selected))
     (assert (pg::physical-source-pin-lost-p selected))
     (assert (null (gethash selected pg::*wal-snapshot-retention-pins*)))
     (assert (= (if allocated 1 0) (hash-table-count pg::*physical-backup-lease-allocations*)))
     (maphash (lambda (key row)
                (declare (ignore key))
                (assert (eq :live (getf row :state))))
              pg::*physical-backup-lease-allocations*)
     (if closing
         (progn
           (assert (eq (not (null bootstrap)) ~s))
           (when bootstrap
             (assert (not (pg::physical-source-pin-lost-p bootstrap)))
             (assert (= 32 (gethash bootstrap pg::*wal-snapshot-retention-pins*))))
           (assert
            (handler-case
                (progn
                  (pg::physical-source-follow-release name (getf (getf parent :successor) :anchor-digest))
                  nil)
              (pg::wal-input-validation-error () t))))
         (progn
           (assert (null bootstrap))
           (assert (eq :released
                       (pg::physical-source-follow-release
                        name (getf (getf parent :successor) :anchor-digest))))
           (assert (eq :absent
                       (pg::physical-source-follow-release
                        name (getf (getf parent :successor) :anchor-digest))))))
     (assert
      (handler-case (progn (pg::physical-source-follow-prepare name coordinates) nil)
        (pg::wal-input-validation-error () t)))
     (assert (not pg::*wal-poisoned*)))"
          bootstrap-present))

(deftest selected-startup-recovers-real-process-kills-at-loss-and-install-boundaries
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp
                     (namestring (merge-pathnames "selected-recovery.XXXXXX" cache-root))))))
        (dolist (allocated '(nil t))
          (dolist (closing '(nil t))
            (dolist (cut '(:before-file-fsync :after-file-fsync
                           :before-directory-fsync :after-directory-fsync
                           :before-startup-pin-install :after-startup-pin-install))
              (call-with-backup-lease-selected-observation-fixture
               (lambda (root parent pin row)
                 (declare (ignore row))
                 (backup-lease-selected-startup-add-suffix pin)
                 (delete-file (pg::wal-segment-path 0))
                 (let ((parent-before (lineage-test-bytes
                                       (pg::physical-backup-lease-path (getf parent :request)))))
                   (dolist (purpose '(:cut :recover))
                     (let* ((log (merge-pathnames
                                  (format nil "selected-~(~a~)-~(~a~).log" cut purpose) root))
                            (body (selected-startup-recovery-child-body
                                   parent pin allocated closing cut
                                   (if (eq purpose :cut)
                                       (selected-startup-recovery-cut-setup
                                        (pg::physical-source-pin-name pin) cut)
                                       "(values)")
                                   (if (eq purpose :cut)
                                       "(error \"selected startup failed to reach its crash cut\")"
                                       (selected-startup-recovery-verification)))))
                       (multiple-value-bind (result pid)
                           (reseed-follow-store-run-child root log body cache)
                         (format t "~a" (uiop:read-file-string log))
                         (format t "SELECTED-UNAVAILABLE-CHILD-OWNED ~s~%"
                                 (list pid (namestring root) allocated closing cut purpose result))
                         (ok (equal (if (eq purpose :cut) '(:signaled 9) '(:exited 0)) result)
                             (format nil "~s ~s ~a" cut purpose log))))
                     (ok (equalp parent-before
                                 (lineage-test-bytes
                                  (pg::physical-backup-lease-path (getf parent :request))))))))
               allocated closing)))))))

(deftest selected-startup-recovers-unlinked-bootstrap-debt-after-later-wal-loss
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp
                     (namestring (merge-pathnames "selected-bootstrap.XXXXXX" cache-root))))))
        (dolist (allocated '(nil t))
          (dolist (cut '(:after-release-unlink :after-release-directory-fsync))
            (call-with-backup-lease-selected-observation-fixture
             (lambda (root parent pin row)
               (declare (ignore row))
               (let* ((request (getf parent :request))
                      (parent-before (lineage-test-bytes (pg::physical-backup-lease-path request)))
                      (setup
                        (format nil
                                "(setf pg::*physical-source-test-hook*
                                     (lambda (point candidate)
                                       (when (and (eq point ~s)
                                                  (equal ~s (pg::physical-source-pin-name candidate)))
                                         (stop-at-cut))))"
                                cut (getf request :name))))
                 (dolist (purpose '(:cut :recover))
                   (let* ((log (merge-pathnames
                                (format nil "bootstrap-~(~a~)-~(~a~).log" cut purpose) root))
                          (body (selected-startup-recovery-child-body
                                 parent pin allocated t cut
                                 (if (eq purpose :cut) setup "(values)")
                                 (if (eq purpose :cut)
                                     "(error \"bootstrap cleanup failed to reach its crash cut\")"
                                     (selected-startup-recovery-verification nil))
                                 (if (eq purpose :cut) 152 192))))
                     (multiple-value-bind (result pid)
                         (reseed-follow-store-run-child root log body cache)
                       (format t "~a" (uiop:read-file-string log))
                       (format t "SELECTED-UNAVAILABLE-CHILD-OWNED ~s~%"
                               (list pid (namestring root) allocated t cut purpose result))
                       (ok (equal (if (eq purpose :cut) '(:signaled 9) '(:exited 0)) result)
                           (format nil "~s ~s ~a" cut purpose log))))
                   (ok (equalp parent-before
                               (lineage-test-bytes (pg::physical-backup-lease-path request))))
                   (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
                   (when (eq purpose :cut)
                     (backup-lease-selected-startup-add-suffix pin)
                     (delete-file (pg::wal-segment-path 0))))))
             allocated t))))))
