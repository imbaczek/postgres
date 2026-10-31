(in-package #:pg-physical-tests)

(defun call-with-physical-slot-api-fixture (function)
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root)
          (pg::*catalog* (pg::make-catalog :cluster-identity "slot-api"))
          (pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*wal-replication-slots* (make-hash-table :test #'equal))
          (pg::*wal-slot-recycling-pending* nil)
          (pg::*wal-slot-recycling-termination-requests* (make-hash-table :test #'eq))
          (pg::*max-replication-slots* 10)
          (pg::*wal-slot-retention-policy* nil)
          (pg::*wal-keep-size-mb* 0)
          (pg::*startup-installed-configuration*
            (physical-slot-test-configuration
             (pg::make-wal-slot-retention-policy :mode :bytes :limit 1073741824) 0))
          (pg::*logical-capture-live-slots* 0)
          (pg::*logical-slot-owner-termination-requests* (make-hash-table :test #'eq))
          (pg::*exported-logical-snapshots* (make-hash-table :test #'equal))
          (pg::*expired-exported-logical-snapshots* (make-hash-table :test #'equal))
          (pg::*wal-durable-lsn* 100)
          (pg::*wal-insert-timeline* 1)
          (pg::*wal-segment-namespace* :legacy)
          (pg::*backend* (pg::make-backend :pid 1234)))
      (declare (special pg::*max-replication-slots*))
      (funcall function root))))

(defun physical-slot-test-analyze-call (sql)
  (pg::analyze-expr
   (getf (first (getf (first (pg::parse-statements sql)) :targets)) :expr) nil))

(defun physical-slot-test-retention-policy ()
  "Build an explicit component policy from the legacy fixture inputs."
  (let ((policy pg::*wal-slot-retention-policy*))
    (pg::make-wal-slot-retention-policy
     :mode (if policy (pg::wal-slot-retention-policy-mode policy) :bytes)
     :limit (if (and policy (eq :segments (pg::wal-slot-retention-policy-mode policy)))
                (pg::wal-slot-retention-policy-limit policy)
                pg::*max-slot-wal-keep-bytes*)
     :max-wal-bytes (if policy (pg::wal-slot-retention-policy-max-wal-bytes policy) 1073741824))))

(defun physical-slot-test-configuration (policy keep-mb)
  "Prepare an installed configuration from explicit retention fixture values."
  (let ((settings (pg::startup-default-settings "/tmp/physical-slot-settings/"))
        (native (make-hash-table :test #'equal))
        (limit (pg::wal-slot-retention-policy-limit policy)))
    (assert (zerop (mod (pg::wal-slot-retention-policy-max-wal-bytes policy) (* 1024 1024))))
    (when (and limit (eq :segments (pg::wal-slot-retention-policy-mode policy)))
      (assert (zerop (mod limit (* 1024 1024)))))
    (setf (getf (gethash "wal_keep_size" settings) :value) keep-mb
          (getf (gethash "max_wal_size" settings) :value)
          (truncate (pg::wal-slot-retention-policy-max-wal-bytes policy) (* 1024 1024)))
    (if (eq :bytes (pg::wal-slot-retention-policy-mode policy))
        (setf (gethash "max-slot-wal-keep-bytes" native) (format nil "~d" (or limit -1)))
        (setf (getf (gethash "max_slot_wal_keep_size" settings) :value)
              (if limit (truncate limit (* 1024 1024)) -1)))
    (prepare-reader-test-configuration settings native)))

(cl-spec:defproperty checkpoint-recycling-keeps-the-segment-containing-the-last-byte
    ((segment (range integer 0 #xfffffffffe))
     (edge (range integer 0 3)))
  (:about pg::wal-checkpoint-recycling-segment)
  (:trials (:normal 200))
  (let* ((size pg::+wal-segment-size+)
         (lsn (+ (* segment size)
                 (case edge (0 0) (1 1) (2 (1- size)) (3 size))))
         (result (pg::wal-checkpoint-recycling-segment lsn)))
    (if (zerop lsn)
        (zerop result)
        (<= (* result size) (1- lsn) (1- (* (1+ result) size))))))

(deftest generated-checkpoint-recycling-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'checkpoint-recycling-keeps-the-segment-containing-the-last-byte :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(declaim (special pg::*wal-keep-size-mb*))

(cl-spec:defproperty wal-keep-floor-obeys-whole-segment-retention
    ((segment (range integer 0 10000))
     (megabytes (range integer 0 100000))
     (offset (range integer 0 16777215)))
  (:about pg::wal-keep-retention-segment)
  (:trials (:normal 200))
  (let* ((pg::*wal-keep-size-mb* megabytes)
         (current (+ (* segment pg::+wal-segment-size+) offset))
         (result (pg::wal-keep-retention-segment current pg::*wal-keep-size-mb*)))
    (and (= result (max 0 (- segment (truncate megabytes 16))))
         (<= 0 result segment))))

(deftest generated-wal-keep-segment-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-keep-floor-obeys-whole-segment-retention :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest wal-keep-recycling-preserves-the-requested-prefix-and-slot
    (dolist (megabytes '(0 1 15 16 17 31 32 64 80))
      (call-with-physical-slot-api-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((size pg::+wal-segment-size+)
                (pg::*wal-keep-size-mb* megabytes)
                (pg::*wal-durable-lsn* (+ (* 4 size) 32))
                (pg::*wal-slot-retention-policy*
                  (pg::make-wal-slot-retention-policy :mode :segments :limit 0))
                (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
                (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
                (expected (max 0 (- 4 (floor megabytes 16))))
                (slot (physical-slot-test-object :restart 1)))
           (setf pg::*startup-installed-configuration*
                 (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                   pg::*wal-keep-size-mb*))
           (pg::wal-ensure-directory-durable (pg::wal-directory))
           (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
           (dotimes (segment 5)
             (pg::wal-atomic-write-bytes (pg::wal-segment-path segment) #(0)))
           (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
           (pg::wal-save-replication-slot slot)
           (sb-thread:with-mutex (pg::*wal-lock*)
             (pg::wal-remove-checkpointed-segments-under-wal-lock (+ (* 4 size) 32)))
           (dotimes (segment 5)
             (ok (eql (>= segment expected)
                      (not (null (probe-file (pg::wal-segment-path segment)))))))
           (ok (eq (if (zerop expected) :reserved :lost)
                   (pg::wal-replication-slot-wal-status slot)))
           (ok (eq (pg::wal-replication-slot-wal-status slot)
                   (pg::wal-replication-slot-wal-status
                    (pg::wal-read-replication-slot
                     (pg::wal-replication-slot-path "public_physical"))))))))))

(deftest wal-keep-independent-floor-still-honors-xids-and-snapshots
    (let* ((size pg::+wal-segment-size+)
           (pg::*wal-keep-size-mb* 16)
           (pg::*wal-durable-lsn* (* 10 size))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq)))
      (sb-thread:with-mutex (pg::*wal-lock*)
        (ok (= 2 (pg::wal-independent-retention-segment 8 (* 3 size) pg::*wal-keep-size-mb*)))
        (setf (gethash 'snapshot pg::*wal-snapshot-retention-pins*) (+ size 1))
        (ok (= 1 (pg::wal-independent-retention-segment 8 (* 3 size) pg::*wal-keep-size-mb*)))
        (setf (gethash 42 pg::*wal-xid-first-lsn*) 1)
        (ok (= 0 (pg::wal-independent-retention-segment 8 (* 3 size) pg::*wal-keep-size-mb*))))))

(deftest wal-keep-controls-standard-slot-status-and-safe-size
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-end-lsn* (+ (* 4 size) 32))
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit 0 :max-wal-bytes size))
              (slot (physical-slot-test-object :restart 1)))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (dolist (megabytes '(0 15 16 64))
           (let ((pg::*wal-keep-size-mb* megabytes))
             (ok (equal (if (= megabytes 64) "extended" "unreserved")
                        (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
             (ok (= (- (* (1+ (floor megabytes 16)) size) pg::*wal-end-lsn*)
                    (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))))
         (let ((pg::*wal-keep-size-mb* 64)
               (pg::*wal-slot-retention-policy*
                 (pg::make-wal-slot-retention-policy :mode :bytes :limit 0))
               (pg::*max-slot-wal-keep-bytes* 0))
           (ok (equal "unreserved" (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
           (ok (= (- 2 pg::*wal-end-lsn*) (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*))))))))

(deftest slot-catalog-snapshot-uses-one-installed-retention-generation
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (let* ((settings (pg::startup-default-settings root))
              (size pg::+wal-segment-size+)
              (pg::*wal-end-lsn* (+ (* 4 size) 32))
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit 0 :max-wal-bytes size))
              (pg::*wal-keep-size-mb* 0))
         (setf (getf (gethash "wal_keep_size" settings) :value) 64
               (getf (gethash "max_slot_wal_keep_size" settings) :value) 0
               (getf (gethash "max_wal_size" settings) :value) 16)
         (let ((first (prepare-reader-test-configuration settings)))
           (setf (getf (gethash "wal_keep_size" settings) :value) 0)
           (let* ((next (prepare-reader-test-configuration settings))
                  (pg::*startup-installed-configuration* first)
                  (capture (symbol-function 'pg::startup-capture-configuration))
                  (status (symbol-function 'pg::wal-slot-catalog-status))
                  (captures 0))
             (dolist (name '("snapshot_a" "snapshot_b"))
               (setf (gethash name pg::*wal-replication-slots*)
                     (pg::make-wal-replication-slot :name name :kind :physical
                                                    :restart-lsn 1 :wal-status :reserved)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::startup-capture-configuration)
                          (lambda () (incf captures) (funcall capture))
                          (symbol-function 'pg::wal-slot-catalog-status)
                          (lambda (&rest arguments)
                            (prog1 (apply status arguments)
                              (setf pg::*startup-installed-configuration* next))))
                    (let ((rows (pg::sql-replication-slot-rows)))
                      (ok (= 2 (length rows)))
                      (ok (= 1 captures))
                      (dolist (row rows)
                        (ok (equal "extended" (aref row 12)))
                        (ok (= (- (* 5 size) pg::*wal-end-lsn*) (aref row 13)))))
                    (setf captures 0)
                    (let ((rows (pg::sql-replication-slot-rows)))
                      (ok (= 1 captures))
                      (dolist (row rows)
                        (ok (equal "unreserved" (aref row 12)))
                        (ok (= (- size pg::*wal-end-lsn*) (aref row 13))))))
               (setf (symbol-function 'pg::startup-capture-configuration) capture
                     (symbol-function 'pg::wal-slot-catalog-status) status))))))))

(deftest slot-recycling-calculation-uses-one-installed-retention-generation
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (first (physical-slot-test-configuration
                      (pg::make-wal-slot-retention-policy :mode :segments :limit 0) 64))
              (next (physical-slot-test-configuration
                     (pg::make-wal-slot-retention-policy :mode :bytes :limit 0) 0))
              (pg::*startup-installed-configuration* first)
              (pg::*wal-durable-lsn* (+ (* 4 size) 32))
              (pg::*physical-source-pins* (make-hash-table :test #'equal))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (slot (physical-slot-test-object :restart 1))
              (capture (symbol-function 'pg::startup-capture-configuration))
              (invalidate (symbol-function 'pg::physical-source-invalidate-lagging-pins-under-wal-lock))
              (captures 0))
         (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::startup-capture-configuration)
                      (lambda () (incf captures) (funcall capture))
                      (symbol-function 'pg::physical-source-invalidate-lagging-pins-under-wal-lock)
                      (lambda (limit)
                        (prog1 (funcall invalidate limit)
                          (sb-thread:with-mutex (pg::*startup-configuration-lock*)
                            (setf pg::*startup-installed-configuration* next)))))
                (sb-thread:with-mutex (pg::*wal-lock*)
                  (ok (= 0 (pg::wal-prepare-slot-recycling-under-lock 4)))
                  (ok (= 1 captures))
                  (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
                  (ok (= 4 (pg::wal-prepare-slot-recycling-under-lock 4)))
                  (ok (= 2 captures))
                  (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))))
           (setf (symbol-function 'pg::startup-capture-configuration) capture
                 (symbol-function 'pg::physical-source-invalidate-lagging-pins-under-wal-lock) invalidate))))))

(deftest wal-keep-setting-parses-units-and-rejects-invalid-values
    (ok (= 0 (pg::startup-normalize-setting "wal_keep_size" "0")))
  (ok (= 1024 (pg::startup-normalize-setting "wal_keep_size" "1GB")))
  (dolist (value '("-1" "2147483648" "1s"))
    (ok (intake-fails-p (lambda () (pg::startup-normalize-setting "wal_keep_size" value))))))

(deftest slot-retention-policy-follows-standard-and-native-configuration-provenance
    (with-physical-store-directory (root)
      (let* ((settings (pg::startup-default-settings root))
             (native (make-hash-table :test #'equal))
             (policy (pg::startup-effective-slot-policy settings native)))
        (ok (eq :segments (pg::wal-slot-retention-policy-mode policy)))
        (ok (null (pg::wal-slot-retention-policy-limit policy)))
        (setf (getf (gethash "max_slot_wal_keep_size" settings) :value) 1)
        (setf policy (pg::startup-effective-slot-policy settings native))
        (ok (= (* 1024 1024) (pg::wal-slot-retention-policy-limit policy)))
        (setf (gethash "max-slot-wal-keep-bytes" native) "17")
        (setf policy (pg::startup-effective-slot-policy settings native))
        (ok (eq :bytes (pg::wal-slot-retention-policy-mode policy)))
        (ok (= 17 (pg::wal-slot-retention-policy-limit policy))))))

(deftest c-slot-checkpoint-loss-waits-for-the-actual-removal-horizon
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-durable-lsn* (+ (* 4 size) 32))
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit (* 1024 1024)))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (slot (physical-slot-test-object :restart 1))
              (pg::*wal-output-stream* nil)
              (pg::*wal-output-segment* nil))
         (setf pg::*startup-installed-configuration*
               (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                 pg::*wal-keep-size-mb*))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) #(0))
         (pg::wal-save-replication-slot slot)
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::wal-remove-checkpointed-segments-under-wal-lock 32))
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (probe-file (pg::wal-segment-path 0)))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::wal-remove-checkpointed-segments-under-wal-lock (+ size 32)))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (not (probe-file (pg::wal-segment-path 0))))
         (ok (eq :lost (pg::wal-replication-slot-wal-status
                        (pg::wal-read-replication-slot (pg::wal-replication-slot-path "public_physical")))))))))

(deftest standard-slot-recycling-keeps-active-owners-reserved
    (dolist (backend-present '(nil t))
      (call-with-physical-slot-api-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((size pg::+wal-segment-size+)
                (pg::*wal-durable-lsn* (+ (* 4 size) 32))
                (pg::*wal-slot-retention-policy*
                  (pg::make-wal-slot-retention-policy :mode :segments :limit 0))
                (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
                (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                (pg::*wal-output-stream* nil)
                (pg::*wal-output-segment* nil)
                (owner (gensym "ACTIVE-PHYSICAL-"))
                (backend (when backend-present pg::*backend*))
                (slot (physical-slot-test-object :restart 1)))
           (pg::wal-ensure-directory-durable (pg::wal-directory))
           (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
           (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) #(0))
           (setf (pg::wal-replication-slot-active-owner slot) owner
                 (pg::wal-replication-slot-active-owner-backend slot) backend
                 (gethash "public_physical" pg::*wal-replication-slots*) slot)
           (pg::wal-save-replication-slot slot)
           (sb-thread:with-mutex (pg::*wal-lock*)
             (pg::wal-remove-checkpointed-segments-under-wal-lock (+ size 32)))
           (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
           (ok (eq owner (pg::wal-replication-slot-active-owner slot)))
           (ok (eq backend (pg::wal-replication-slot-active-owner-backend slot)))
           (ok (probe-file (pg::wal-segment-path 0)))
           (ok (eq :reserved
                   (pg::wal-replication-slot-wal-status
                    (pg::wal-read-replication-slot
                     (pg::wal-replication-slot-path "public_physical"))))))))))

(deftest standard-slot-recycling-returns-the-exact-owner-for-unlocked-work
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-durable-lsn* (+ (* 4 size) 32))
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit 0))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (slot (physical-slot-test-object :restart 1))
              (owner (gensym "ACTIVE-PHYSICAL-")))
         (setf pg::*startup-installed-configuration*
               (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                 pg::*wal-keep-size-mb*))
         (setf (pg::wal-replication-slot-active-owner slot) owner
               (pg::wal-replication-slot-active-owner-backend slot) pg::*backend*
               (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (multiple-value-bind (floor requests)
               (pg::wal-prepare-slot-recycling-under-lock 1)
             (ok (= 1 floor))
             (ok (equal (list (list slot owner pg::*backend*)) requests))
             (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))))))))

(defun call-with-active-slot-recycling-fixture (function)
  (call-with-physical-slot-api-fixture
   (lambda (root)
     (declare (ignore root))
     (let* ((size pg::+wal-segment-size+)
            (checkpoint (+ (* 4 size) 32))
            (pg::*wal-checkpoint-lsn* checkpoint)
            (pg::*wal-ready* t) (pg::*wal-poisoned* nil)
            (pg::*wal-durable-lsn* checkpoint)
            (pg::*wal-control-metadata* nil)
            (pg::*wal-slot-retention-policy*
              (pg::make-wal-slot-retention-policy :mode :segments :limit size))
            (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
            (owner (gensym "ACTIVE-PHYSICAL-"))
            (backend pg::*backend*)
            (slot (physical-slot-test-object :restart 1))
            (pg::*startup-installed-configuration*
              (physical-slot-test-configuration pg::*wal-slot-retention-policy* 0)))
       (pg::wal-ensure-directory-durable (pg::wal-directory))
       (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
       (dolist (segment '(0 1 2 3))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path segment) #(0)))
       (setf (pg::wal-replication-slot-active-owner slot) owner
             (pg::wal-replication-slot-active-owner-backend slot) backend
             (gethash "public_physical" pg::*wal-replication-slots*) slot)
       (pg::wal-save-replication-slot slot)
       (funcall function slot owner backend checkpoint)))))

(deftest wal-keep-reload-rechecks-the-frozen-owner-endpoint
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (declare (ignore owner))
       (let* ((rescue (physical-slot-test-configuration
                       (pg::startup-configuration-slot-policy
                        (pg::startup-require-configuration)) 64))
              (signals 0)
              (pg::*backend* nil)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target) (declare (ignore target)) (incf signals)))
              (pg::*logical-test-failpoint-hook*
                (lambda (point &rest arguments)
                  (declare (ignore arguments))
                  (when (eq point :before-recycling-owner-termination-claim)
                    (sb-thread:with-mutex (pg::*startup-configuration-lock*)
                      (setf pg::*startup-installed-configuration* rescue))
                    (setf pg::*wal-durable-lsn* (* 20 pg::+wal-segment-size+))))))
         (pg::wal-remove-checkpointed-segments checkpoint)
         (ok (= 0 signals))
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (eq backend (pg::wal-replication-slot-active-owner-backend slot)))
         (ok (null pg::*wal-slot-recycling-pending*))
         (ok (probe-file (pg::wal-segment-path 0)))))))

(deftest wal-keep-does-not-revive-lost-slots-or-removed-files
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-durable-lsn* (* 4 size))
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit 0))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
              (slot (physical-slot-test-object :restart 1)))
         (setf pg::*startup-installed-configuration*
               (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                 pg::*wal-keep-size-mb*))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) #(0))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::wal-remove-checkpointed-segments-under-wal-lock (+ (* 3 size) 32))
           (setf pg::*wal-keep-size-mb* 64
                 pg::*startup-installed-configuration*
                 (physical-slot-test-configuration (physical-slot-test-retention-policy) 64))
           (pg::wal-remove-checkpointed-segments-under-wal-lock (+ (* 3 size) 32)))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (equal "lost" (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (ok (eq pg::+null+ (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (ok (not (probe-file (pg::wal-segment-path 0))))))))

(deftest wal-keep-preserves-files-without-weakening-the-native-byte-cap
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-keep-size-mb* 64)
              (pg::*wal-durable-lsn* (* 4 size))
              (pg::*max-slot-wal-keep-bytes* 0)
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :bytes :limit 0))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
              (slot (physical-slot-test-object :restart 1)))
         (setf pg::*startup-installed-configuration*
               (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                 pg::*wal-keep-size-mb*))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) #(0))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::wal-remove-checkpointed-segments-under-wal-lock (+ (* 3 size) 32)))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (probe-file (pg::wal-segment-path 0)))))))

(deftest standard-slot-recycling-preserves-pending-work-until-owner-release
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((requests 0)
              (pg::*backend* nil)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target) (ok (eq backend target)) (incf requests))))
         (pg::wal-remove-checkpointed-segments checkpoint)
         (ok (= 1 requests))
         (ok pg::*wal-slot-recycling-pending*)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (probe-file (pg::wal-segment-path 0)))
         (pg::wal-finish-pending-slot-recycling)
         (ok (= 1 requests))
         (ok pg::*wal-slot-recycling-pending*)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (pg::wal-release-replication-slot-owner "public_physical" owner)
         (pg::wal-finish-pending-slot-recycling)
         (ok (= checkpoint pg::*wal-checkpoint-lsn*))
         (ok (null pg::*wal-slot-recycling-pending*))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (not (probe-file (pg::wal-segment-path 0))))))))

(deftest standard-slot-recycling-freezes-the-retention-endpoint
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let ((pg::*backend* nil)
             (pg::*logical-test-invalidated-backend-hook*
               (lambda (target)
                 (ok (eq backend target))
                 (sb-thread:with-mutex (pg::*wal-lock*)
                   (setf pg::*wal-durable-lsn* (* 20 pg::+wal-segment-size+)
                         (pg::wal-replication-slot-restart-lsn slot) (* 3 pg::+wal-segment-size+))
                   (pg::wal-save-replication-slot slot))
                 (pg::wal-release-replication-slot-owner "public_physical" owner))))
         (pg::wal-remove-checkpointed-segments checkpoint)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (null pg::*wal-slot-recycling-pending*))
         (ok (not (probe-file (pg::wal-segment-path 0))))
         (ok (probe-file (pg::wal-segment-path 3)))
         (ok (= checkpoint pg::*wal-checkpoint-lsn*))))))

(deftest standard-slot-recycling-revalidates-before-signaling
    (dolist (change '(:owner :generation :catchup :policy :native-policy :control-system :recovery :failed :checkpoint :registry :timeline :namespace :root))
      (call-with-active-slot-recycling-fixture
       (lambda (slot owner backend checkpoint)
         (declare (ignore owner backend))
         (let* ((requests 0)
                (replacement nil)
                (pg::*backend* nil)
                (pg::*logical-test-invalidated-backend-hook*
                  (lambda (target) (declare (ignore target)) (incf requests)))
                (pg::*logical-test-failpoint-hook*
                  (lambda (point &rest details)
                    (declare (ignore details))
                    (when (eq point :before-recycling-owner-termination-claim)
                      (ecase change
                        (:owner (setf (pg::wal-replication-slot-active-owner slot) (gensym "NEW-OWNER-")))
                        (:generation
                         (setf replacement (pg::copy-wal-replication-slot slot)
                               (gethash "public_physical" pg::*wal-replication-slots*) replacement))
                        (:catchup
                         (setf (pg::wal-replication-slot-restart-lsn slot) (* 3 pg::+wal-segment-size+)))
                        (:policy
                         (setf pg::*startup-installed-configuration*
                               (physical-slot-test-configuration
                                (pg::make-wal-slot-retention-policy :mode :segments :limit nil) 0)))
                        (:native-policy
                         (setf pg::*startup-installed-configuration*
                               (physical-slot-test-configuration
                                (pg::make-wal-slot-retention-policy :mode :bytes :limit 0) 0)))
                        (:control-system (setf pg::*wal-control-metadata* (list :system-id 999)))
                        (:recovery (setf pg::*physical-recovery-state* (list :phase 2)))
                        (:failed (setf pg::*physical-recovery-failed-p* t))
                        (:checkpoint (incf pg::*wal-checkpoint-lsn*))
                        (:registry (setf pg::*wal-replication-slots* (make-hash-table :test #'equal)))
                        (:timeline (incf pg::*wal-insert-timeline*))
                        (:namespace (setf pg::*wal-segment-namespace* :timeline))
                        (:root (setf pg::*wal-control-metadata* (list :retained-reference '(1 2 3)))))))))
           (pg::wal-remove-checkpointed-segments checkpoint)
           (ok (zerop requests))
           (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
           (when replacement
             (ok (eq :reserved (pg::wal-replication-slot-wal-status replacement))))
           (when (member change '(:owner :generation :native-policy :control-system :recovery :failed :checkpoint :registry :timeline :namespace :root))
             (let ((pg::*wal-insert-timeline* 1) (pg::*wal-segment-namespace* :legacy))
               (ok (probe-file (pg::wal-segment-path 0))))))))))

(deftest standard-slot-recycling-defers-inherited-resources
    (dolist (scope '(:schema-read :schema-write :transaction-schema :reload))
      (call-with-active-slot-recycling-fixture
       (lambda (slot owner backend checkpoint)
         (let* ((requests 0)
                (pg::*logical-test-invalidated-backend-hook*
                  (lambda (target)
                    (ok (eq backend target))
                    (incf requests)
                    (pg::wal-release-replication-slot-owner "public_physical" owner))))
           (flet ((collect ()
                    (pg::wal-remove-checkpointed-segments checkpoint)
                    (ok (zerop requests))
                    (ok pg::*wal-slot-recycling-pending*)
                    (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
                    (ok (probe-file (pg::wal-segment-path 0)))))
             (ecase scope
               (:schema-read (let ((pg::*schema-read-lock-held* t)) (collect)))
               (:schema-write (let ((pg::*schema-write-lock-held* t)) (collect)))
               (:transaction-schema
                (setf (pg::backend-transaction-schema-read-lock-held-p backend) t)
                (unwind-protect (collect)
                  (setf (pg::backend-transaction-schema-read-lock-held-p backend) nil)))
               (:reload (sb-thread:with-mutex (pg::*reload-run-lock*) (collect)))))
           (let ((pg::*backend* nil))
             (pg::wal-finish-pending-slot-recycling))
           (ok (= 1 requests))
           (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
           (ok (null pg::*wal-slot-recycling-pending*)))))))

(deftest standard-slot-recycling-self-owner-is-fatal-before-durable-loss
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (checkpoint (+ size 32))
              (pg::*wal-checkpoint-lsn* checkpoint)
              (pg::*wal-ready* t) (pg::*wal-poisoned* nil)
              (pg::*wal-durable-lsn* (+ (* 4 size) 32))
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit 0))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
              (backend pg::*backend*)
              (slot (pg::wal-create-physical-replication-slot "self_temporary" :temporary t))
              (fatal nil))
         (setf pg::*startup-installed-configuration*
               (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                 pg::*wal-keep-size-mb*))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) #(0))
         (setf (pg::wal-replication-slot-restart-lsn slot) 1
               (pg::wal-replication-slot-restart-timeline slot) 1)
         (pg::wal-save-replication-slot slot)
         (handler-case (pg::wal-remove-checkpointed-segments checkpoint)
           (pg::pg-error (condition) (setf fatal (pg::fatal-error-p condition))))
         (ok fatal)
         (ok pg::*wal-slot-recycling-pending*)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (eq backend (pg::wal-replication-slot-active-owner slot)))
         (ok (probe-file (pg::wal-segment-path 0)))
         (ok (eq :reserved
                 (pg::wal-replication-slot-wal-status
                  (pg::wal-read-replication-slot
                   (pg::wal-replication-slot-path "self_temporary")))))
         (pg::wal-cleanup-backend-physical-slots backend)
         (let ((pg::*backend* nil))
           (pg::wal-finish-pending-slot-recycling))
         (ok (null (gethash "self_temporary" pg::*wal-replication-slots*)))
         (ok (null pg::*wal-slot-recycling-pending*))
         (ok (not (probe-file (pg::wal-segment-path 0))))))))

(deftest standard-slot-recycling-nonwaiting-pass-does-not-park-or-remove-active-wal
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((release (sb-thread:make-semaphore :count 0))
              (thread (sb-thread:make-thread (lambda () (sb-thread:wait-on-semaphore release))
                                             :name "slot recycling timeout owner"))
              (requests 0) (parks 0)
              (pg::*backend* nil)
              (pg::*wal-slot-owner-wait-seconds* 0.01)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target) (ok (eq backend target)) (incf requests)))
              (pg::*live-operation-parking-test-hook*
                (lambda (point token)
                  (declare (ignore token))
                  (when (eq point :after-park)
                    (incf parks)
                    (ok pg::*live-operation-parked-p*)
                    (ok (zerop pg::*live-operation-depth*))
                    (ok (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                    (ok (not (sb-thread:holding-mutex-p pg::*write-lock*)))))))
         (setf (pg::backend-thread backend) thread)
         (unwind-protect
              (progn
                (pg::wal-remove-checkpointed-segments checkpoint :wait nil)
                (ok (= 1 requests))
                (ok (zerop parks))
                (ok pg::*wal-slot-recycling-pending*)
                (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
                (ok (probe-file (pg::wal-segment-path 0)))
                (ok (sb-thread:thread-alive-p thread))
                (pg::wal-finish-pending-slot-recycling)
                (ok (= 1 requests))
                (ok (zerop parks))
                (ok (zerop (pg::live-operation-parked-count))))
           (sb-thread:signal-semaphore release)
           (sb-thread:join-thread thread :timeout 2))
         (pg::wal-release-replication-slot-owner "public_physical" owner)
         (pg::wal-finish-pending-slot-recycling)
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (null pg::*wal-slot-recycling-pending*))))))

(deftest standard-slot-recycling-cancellation-retains-discoverable-work
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((requests 0) (cancelled nil)
                           (pg::*backend* nil)
                           (pg::*logical-test-invalidated-backend-hook*
                             (lambda (target) (ok (eq backend target)) (incf requests)))
                           (pg::*live-operation-parking-test-hook*
                             (lambda (point token)
                               (declare (ignore token))
                               (when (eq point :after-park)
                                 (pg::ereport "57014" "cancel recycling wait")))))
         (handler-case (pg::wal-remove-checkpointed-segments checkpoint)
           (pg::pg-error (condition) (setf cancelled (string= "57014" (pg::pg-error-sqlstate condition)))))
         (ok cancelled)
         (ok (= 1 requests))
         (ok pg::*wal-slot-recycling-pending*)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (probe-file (pg::wal-segment-path 0)))
         (ok (zerop (pg::live-operation-parked-count)))
         (pg::wal-release-replication-slot-owner "public_physical" owner)
         (let ((pg::*live-operation-parking-test-hook* nil))
           (pg::wal-finish-pending-slot-recycling))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (null pg::*wal-slot-recycling-pending*))
         (ok (= 1 requests))))))

(deftest standard-slot-recycling-does-not-resume-after-shutdown
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((pg::*shutdown* pg::+no-shutdown+)
              (pg::*backend* nil)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target)
                  (ok (eq backend target))
                  (pg::wal-release-replication-slot-owner "public_physical" owner)
                  (setf pg::*shutdown* pg::+fast-shutdown+))))
         (pg::wal-remove-checkpointed-segments checkpoint)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok pg::*wal-slot-recycling-pending*)
         (ok (probe-file (pg::wal-segment-path 0)))))))

(deftest standard-slot-checkpoint-waits-across-timeouts-without-repeating-core
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((release (sb-thread:make-semaphore :count 0))
              (slots pg::*wal-replication-slots*)
              (claims pg::*logical-slot-owner-termination-requests*)
              (thread
                (sb-thread:make-thread
                 (lambda ()
                   (sb-thread:wait-on-semaphore release)
                   (let ((pg::*wal-replication-slots* slots)
                         (pg::*logical-slot-owner-termination-requests* claims))
                     (pg::with-live-operation
                       (pg::wal-release-replication-slot-owner "public_physical" owner))))
                 :name "slot owner released during checkpoint"))
              (original-core (symbol-function 'pg::wal-checkpoint-core))
              (core-calls 0) (requests 0) (parks 0)
              (pg::*backend* nil)
              (pg::*wal-slot-owner-wait-seconds* 0.01)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target) (ok (eq backend target)) (incf requests)))
              (pg::*live-operation-parking-test-hook*
                (lambda (point token)
                  (declare (ignore token))
                  (when (eq point :after-park)
                    (incf parks)
                    (when (= 2 parks) (sb-thread:signal-semaphore release))))))
         (setf (pg::backend-thread backend) thread)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-checkpoint-core)
                      (lambda ()
                        (incf core-calls)
                        (pg::with-write-lock
                          (pg::with-wal-critical
                            (pg::wal-remove-checkpointed-segments-under-wal-lock checkpoint)))))
                (pg::wal-checkpoint)
                (ok (= 1 core-calls))
                (ok (= 1 requests))
                (ok (>= parks 2))
                (ok (not (sb-thread:thread-alive-p thread)))
                (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
                (ok (null pg::*wal-slot-recycling-pending*))
                (ok (not (probe-file (pg::wal-segment-path 0))))
                (ok (zerop (pg::live-operation-parked-count))))
           (setf (symbol-function 'pg::wal-checkpoint-core) original-core)
           (sb-thread:signal-semaphore release)
           (sb-thread:join-thread thread :timeout 2))))))

(deftest standard-slot-recycling-wait-allows-reload-to-defer-and-retry
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (declare (ignore backend))
       (let* ((reloads 0) (attempts 0)
                          (pg::*backend* nil)
                          (pg::*logical-test-invalidated-backend-hook* (lambda (target) (declare (ignore target))))
                          (pg::*live-operation-parking-test-hook*
                            (lambda (point token)
                              (declare (ignore token))
                              (when (eq point :after-park)
                                (let ((thread
                                        (sb-thread:make-thread
                                         (lambda ()
                                           (multiple-value-bind (finished values generation)
                                               (pg::try-call-with-live-operation-reload
                                                (lambda () (incf reloads)))
                                             (declare (ignore values))
                                             (ok (not finished))
                                             (ok generation)
                                             (incf attempts)))
                                         :name "reload defers around parked slot recycling")))
                                  (sb-thread:join-thread thread :timeout 2))))))
         (pg::wal-remove-checkpointed-segments checkpoint)
         (ok (= 1 attempts))
         (ok (zerop reloads))
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (ok (probe-file (pg::wal-segment-path 0)))
         (ok (zerop (pg::live-operation-parked-count)))
         (pg::call-with-live-operation-reload (lambda () (incf reloads)))
         (ok (= 1 reloads))
         (pg::wal-release-replication-slot-owner "public_physical" owner)
         (let ((pg::*live-operation-parking-test-hook* nil))
           (pg::wal-finish-pending-slot-recycling))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))))))

(deftest standard-slot-recycling-drains-after-frontend-resource-scopes
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((original (symbol-function 'pg::%dispatch-frontend-frame))
              (requests 0)
              (owner-backend (pg::make-backend :pid 4321))
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target)
                  (ok (eq target owner-backend))
                  (ok (not (pg::backend-holds-schema-read-lock-p)))
                  (incf requests)
                  (pg::wal-release-replication-slot-owner "public_physical" owner))))
         (setf (pg::backend-transaction-schema-read-lock-held-p backend) t
               (pg::wal-replication-slot-active-owner-backend slot) owner-backend)
         (unwind-protect
              (progn
                (pg::wal-remove-checkpointed-segments checkpoint)
                (ok (zerop requests))
                (ok pg::*wal-slot-recycling-pending*)
                (setf (symbol-function 'pg::%dispatch-frontend-frame)
                      (lambda (&rest args)
                        (declare (ignore args))
                        (setf (pg::backend-transaction-schema-read-lock-held-p backend) nil)
                        (values :continue :metadata)))
                (ok (equal '(:continue :metadata)
                           (multiple-value-list (pg::dispatch-frontend-frame nil nil nil nil))))
                (ok (= 1 requests))
                (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
                (ok (null pg::*wal-slot-recycling-pending*)))
           (setf (symbol-function 'pg::%dispatch-frontend-frame) original
                 (pg::backend-transaction-schema-read-lock-held-p backend) nil))))))

(deftest standard-slot-recycling-bootstrap-drains-after-reload-and-quiescence
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (with-physical-store-directory (target)
         (let* ((original (symbol-function 'pg::physical-bootstrap-copy-under-quiescence))
                (requests 0)
                (pg::*backend* nil)
                (pg::*logical-test-invalidated-backend-hook*
                  (lambda (recipient)
                    (ok (eq backend recipient))
                    (ok (not (sb-thread:holding-mutex-p pg::*reload-run-lock*)))
                    (ok (null pg::*live-operation-quiescence-owner-thread*))
                    (incf requests)
                    (pg::wal-release-replication-slot-owner "public_physical" owner))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-bootstrap-copy-under-quiescence)
                        (lambda (directory pin)
                          (declare (ignore directory pin))
                          (ok (sb-thread:holding-mutex-p pg::*reload-run-lock*))
                          (ok (eq sb-thread:*current-thread* pg::*live-operation-quiescence-owner-thread*))
                          (pg::with-write-lock
                            (pg::with-wal-critical
                              (pg::wal-remove-checkpointed-segments-under-wal-lock checkpoint)))
                          (ok (zerop requests))
                          (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
                          (ok (probe-file (pg::wal-segment-path 0)))
                          :copied))
                  (ok (eq :copied (pg::physical-base-backup (namestring target) "bootstrap_test")))
                  (ok (= 1 requests))
                  (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
                  (ok (null pg::*wal-slot-recycling-pending*)))
             (setf (symbol-function 'pg::physical-bootstrap-copy-under-quiescence) original)))))))

(deftest standard-slot-recycling-save-failures-retain-wal-and-fence
    (dolist (after-save '(nil t))
      (call-with-active-slot-recycling-fixture
       (lambda (slot owner backend checkpoint)
         (let* ((save (symbol-function 'pg::wal-save-replication-slot))
                (pg::*backend* nil)
                (pg::*pm-requested-mode* pg::+no-shutdown+)
                (pg::*logical-test-invalidated-backend-hook*
                  (lambda (target)
                    (ok (eq backend target))
                    (pg::wal-release-replication-slot-owner "public_physical" owner))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-save-replication-slot)
                        (lambda (candidate)
                          (when after-save (funcall save candidate))
                          (error "injected slot loss publication failure")))
                  (ok (intake-fails-p (lambda () (pg::wal-remove-checkpointed-segments checkpoint))))
                  (ok pg::*wal-poisoned*)
                  (ok (= pg::+fast-shutdown+ pg::*pm-requested-mode*))
                  (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
                  (ok (probe-file (pg::wal-segment-path 0)))
                  (ok (eq (if after-save :lost :reserved)
                          (pg::wal-replication-slot-wal-status
                           (pg::wal-read-replication-slot
                            (pg::wal-replication-slot-path "public_physical"))))))
             (setf (symbol-function 'pg::wal-save-replication-slot) save)))))))

(deftest standard-slot-recycling-durable-loss-precedes-wal-removal
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (let* ((pg::*backend* nil)
              (pg::*pm-requested-mode* pg::+no-shutdown+)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target)
                  (ok (eq backend target))
                  (pg::wal-release-replication-slot-owner "public_physical" owner)))
              (pg::*logical-test-failpoint-hook*
                (lambda (point &rest details)
                  (declare (ignore details))
                  (when (eq point :before-slot-wal-removal)
                    (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
                    (ok (eq :lost
                            (pg::wal-replication-slot-wal-status
                             (pg::wal-read-replication-slot
                              (pg::wal-replication-slot-path "public_physical")))))
                    (ok (probe-file (pg::wal-segment-path 0)))
                    (error "injected failure after saved loss before unlink")))))
         (ok (intake-fails-p (lambda () (pg::wal-remove-checkpointed-segments checkpoint))))
         (ok pg::*wal-poisoned*)
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (probe-file (pg::wal-segment-path 0)))))))

(deftest standard-slot-recycling-optional-drain-does-not-wait-for-closed-admission
    (call-with-active-slot-recycling-fixture
     (lambda (slot owner backend checkpoint)
       (declare (ignore backend))
       (let* ((requests 0)
              (pg::*backend* nil)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (target) (declare (ignore target)) (incf requests))))
         (let ((pg::*schema-read-lock-held* t))
           (pg::wal-remove-checkpointed-segments checkpoint))
         (let ((thread nil) (pending pg::*wal-slot-recycling-pending*))
           (pg::with-live-operation-quiescence
             (setf thread
                   (sb-thread:make-thread
                    (lambda ()
                      (let ((pg::*wal-slot-recycling-pending* pending))
                        (pg::wal-finish-pending-slot-recycling)
                        :deferred))
                    :name "optional recycling with closed admission"))
             (ok (eq :deferred (sb-thread:join-thread thread :timeout 2)))))
         (ok (zerop requests))
         (ok pg::*wal-slot-recycling-pending*)
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (pg::wal-release-replication-slot-owner "public_physical" owner)
         (pg::wal-finish-pending-slot-recycling)
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))))))

(deftest standard-slot-recycling-stops-before-loss-and-rechecks-progress
    (dolist (caught-up '(nil t))
      (call-with-physical-slot-api-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((size pg::+wal-segment-size+)
                (checkpoint (+ size 32))
                (pg::*wal-checkpoint-lsn* checkpoint)
                (pg::*wal-ready* t) (pg::*wal-poisoned* nil)
                (pg::*wal-durable-lsn* (+ (* 4 size) 32))
                (pg::*wal-slot-retention-policy*
                  (pg::make-wal-slot-retention-policy :mode :segments :limit 0))
                (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
                (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
                (owner (gensym "ACTIVE-PHYSICAL-"))
                (backend pg::*backend*)
                (slot (physical-slot-test-object :restart 1))
                (requests 0)
                (pg::*logical-test-invalidated-backend-hook*
                  (lambda (target)
                    (incf requests)
                    (ok (eq backend target))
                    (ok (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                    (ok (not (sb-thread:holding-mutex-p pg::*write-lock*)))
                    (ok (not pg::*live-operation-parked-p*))
                    (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
                    (ok (probe-file (pg::wal-segment-path 0)))
                    (ok (eq :reserved
                            (pg::wal-replication-slot-wal-status
                             (pg::wal-read-replication-slot
                              (pg::wal-replication-slot-path "public_physical")))))
                    (when caught-up
                      (sb-thread:with-mutex (pg::*wal-lock*)
                        (setf (pg::wal-replication-slot-restart-lsn slot) (* 2 size))
                        (pg::wal-save-replication-slot slot)))
                    (pg::wal-release-replication-slot-owner "public_physical" owner))))
           (setf pg::*startup-installed-configuration*
                 (physical-slot-test-configuration (physical-slot-test-retention-policy)
                                                   pg::*wal-keep-size-mb*))
           (pg::wal-ensure-directory-durable (pg::wal-directory))
           (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
           (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) #(0))
           (setf (pg::wal-replication-slot-active-owner slot) owner
                 (pg::wal-replication-slot-active-owner-backend slot) backend
                 (gethash "public_physical" pg::*wal-replication-slots*) slot)
           (pg::wal-save-replication-slot slot)
           (let ((pg::*backend* nil))
             (pg::wal-remove-checkpointed-segments checkpoint))
           (ok (= 1 requests))
           (ok (eq (if caught-up :reserved :lost)
                   (pg::wal-replication-slot-wal-status slot)))
           (ok (null (pg::wal-replication-slot-active-owner slot)))
           (ok (not (probe-file (pg::wal-segment-path 0))))
           (ok (eq (if caught-up :reserved :lost)
                   (pg::wal-replication-slot-wal-status
                    (pg::wal-read-replication-slot
                     (pg::wal-replication-slot-path "public_physical"))))))))))

(deftest c-slot-retention-rounds-segments-and-respects-independent-horizons
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-durable-lsn* (+ (* 4 size) 32))
              (pg::*wal-end-lsn* pg::*wal-durable-lsn*)
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit (* 1024 1024)))
              (slot (physical-slot-test-object :restart 1)))
         (declare (special pg::*wal-slot-retention-policy*))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (ok (= 0 (pg::wal-slot-removal-segment 4 0 pg::*wal-durable-lsn* (physical-slot-test-retention-policy))))
         (ok (= 1 (pg::wal-slot-removal-segment 1 1 pg::*wal-durable-lsn* (physical-slot-test-retention-policy))))
         (ok (= 4 (pg::wal-slot-removal-segment 4 4 pg::*wal-durable-lsn* (physical-slot-test-retention-policy))))
         (ok (= (- size pg::*wal-end-lsn*) (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (setf pg::*wal-slot-retention-policy*
               (pg::make-wal-slot-retention-policy :mode :segments :limit (+ size (* 1024 1024))))
         (ok (= 3 (pg::wal-slot-removal-segment 4 4 pg::*wal-durable-lsn* (physical-slot-test-retention-policy))))
         (setf pg::*wal-slot-retention-policy*
               (pg::make-wal-slot-retention-policy :mode :segments :limit nil))
         (ok (= 0 (pg::wal-slot-removal-segment 4 4 pg::*wal-durable-lsn* (physical-slot-test-retention-policy))))
         (ok (eq pg::+null+ (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))))))

(deftest c-slot-retention-matches-reference-boundary-observations
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :segments :limit (* 1024 1024)
                                                    :max-wal-bytes (* 64 1024 1024)))
              (slot (physical-slot-test-object :restart #x04000060)))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         ;; Captured by physical-slots.py --retention-oracle against pinned C.
         (dolist (observation '((#x04000148 "reserved" 16776888)
                                (#x05000000 "unreserved" 0)
                                (#x06000000 "unreserved" -16777216)))
           (destructuring-bind (write status safe) observation
             (let ((pg::*wal-end-lsn* write) (pg::*wal-durable-lsn* write))
               (ok (equal status (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
               (ok (= safe (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*))))))))))

(deftest byte-slot-catalog-reports-policy-allowance-and-normal-wal-window
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (pg::*wal-slot-retention-policy*
                (pg::make-wal-slot-retention-policy :mode :bytes :limit nil :max-wal-bytes size))
              (pg::*max-slot-wal-keep-bytes* nil)
              (pg::*wal-end-lsn* (+ (* 4 size) 32))
              (slot (physical-slot-test-object :restart 1)))
         (ok (equal "extended" (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (ok (eq pg::+null+ (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (setf pg::*max-slot-wal-keep-bytes* 64
               pg::*wal-end-lsn* 65)
         (ok (equal "reserved" (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (ok (= 1 (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (setf pg::*wal-end-lsn* 66)
         (ok (equal "unreserved" (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (ok (= 0 (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (setf pg::*wal-end-lsn* 67)
         (ok (= -1 (pg::wal-slot-safe-wal-bytes slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))
         (setf pg::*max-slot-wal-keep-bytes* nil
               pg::*wal-slot-retention-policy* nil
               pg::*wal-end-lsn* (+ (* 70 size) 32))
         (ok (equal "extended" (pg::wal-slot-catalog-status slot (physical-slot-test-retention-policy) pg::*wal-keep-size-mb*)))))))

(deftest public-physical-slot-catalog-uses-c-fields-and-null-logical-state
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*max-slot-wal-keep-bytes* nil)
             (pg::*wal-slot-retention-policy* nil))
         (pg::wal-create-physical-replication-slot "catalog_physical")
         (let* ((fields (getf (pg::sql-system-relation-definition "pg_replication_slots") :fields))
                (row (first (pg::sql-replication-slot-rows))))
           (ok (= 23 (length fields)))
           (ok (equal (loop for n from 1 to 23 collect n) (mapcar #'fourth fields)))
           (ok (equal '("slot_name" "plugin" "slot_type" "datoid" "database" "temporary"
                        "active" "active_pid" "xmin" "catalog_xmin" "restart_lsn"
                        "confirmed_flush_lsn" "wal_status" "safe_wal_size" "two_phase"
                        "two_phase_at" "inactive_since" "conflicting" "invalidation_reason"
                        "failover" "synced" "slotsync_skip_reason" "retained_wal_bytes")
                      (mapcar #'first fields)))
           (ok (eq :xid (second (nth 8 fields))))
           (ok (= 28 (pg::type-oid :xid)))
           (ok (equal "physical" (aref row 2)))
           (dolist (index '(1 3 4 7 8 9 10 11 12 13 15 17 18 21))
             (ok (eq pg::+null+ (aref row index))))
           (ok (not (aref row 5)))
           (ok (not (aref row 6)))
           (ok (= 0 (aref row 22))))))))

(deftest lost-slot-catalog-hides-the-invalid-restart-position
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (dolist (kind '(:physical :logical))
         (clrhash pg::*wal-replication-slots*)
         (let ((slot (physical-slot-test-object :restart 32)))
           (setf (pg::wal-replication-slot-kind slot) kind
                 (pg::wal-replication-slot-wal-status slot) :lost
                 (gethash "public_physical" pg::*wal-replication-slots*) slot)
           (let ((row (first (pg::sql-replication-slot-rows))))
             (ok (eq pg::+null+ (aref row 10)))
             (ok (equal "lost" (aref row 12)))
             (ok (eq pg::+null+ (aref row 13)))
             (ok (equal "wal_removed" (aref row 18))))
           (multiple-value-bind (record present) (funcall (pg::make-replication-slot-result-iterator))
             (ok present)
             (ok (eq pg::+null+ (aref (pg::sql-record-values record) 9)))))))))

(deftest replication-slot-retained-byte-extension-counts-durable-wal
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*wal-end-lsn* 200)
             (slot (physical-slot-test-object :restart 32)))
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (ok (= 68 (aref (first (pg::sql-replication-slot-rows)) 22)))))))

(deftest replication-slot-srf-exposes-c-record-shape-and-snapshots-rows
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((pg::*select-set-functions-allowed-p* t)
              (call (physical-slot-test-analyze-call "SELECT pg_catalog.pg_get_replication_slots()"))
              (fields (getf (cdddr call) :fields)))
         (ok (eq :set-func (first call)))
         (ok (= 21 (length fields)))
         (ok (equal '("slot_name" "plugin" "slot_type" "datoid" "temporary" "active"
                      "active_pid" "xmin" "catalog_xmin" "restart_lsn" "confirmed_flush_lsn"
                      "wal_status" "safe_wal_size" "two_phase" "two_phase_at" "inactive_since"
                      "conflicting" "invalidation_reason" "failover" "synced" "slotsync_skip_reason")
                    (mapcar #'first fields)))
         (pg::wal-create-physical-replication-slot "srf_snapshot")
         (let ((iterator (pg::make-sql-set-function-iterator call nil)))
           (pg::wal-drop-replication-slot "srf_snapshot")
           (multiple-value-bind (record present) (funcall iterator)
             (ok present)
             (ok (equal fields (pg::sql-record-fields record)))
             (let ((values (pg::sql-record-values record)))
               (ok (= 21 (length values)))
               (ok (equal "srf_snapshot" (aref values 0)))
               (ok (equal "physical" (aref values 2)))
               (ok (eq pg::+null+ (aref values 9)))))
           (multiple-value-bind (record present) (funcall iterator)
             (ok (null record))
             (ok (not present))))
         (ok (equal "42883" (recovery-state-of
                             (lambda () (physical-slot-test-analyze-call "SELECT pg_get_replication_slots(1)"))))))
       (let ((pg::*select-set-functions-allowed-p* nil))
         (ok (equal "0A000" (recovery-state-of
                             (lambda () (physical-slot-test-analyze-call "SELECT pg_get_replication_slots()")))))))))

(deftest replication-slot-srf-is-stable-and-wal-loss-is-not-a-recovery-conflict
    (ok (not (pg::cte-function-volatile-p "pg_get_replication_slots")))
  (dolist (name '("pg_create_physical_replication_slot" "pg_replication_slot_advance"
                  "pg_drop_replication_slot"))
    (ok (pg::cte-function-volatile-p name)))
  (call-with-physical-slot-api-fixture
   (lambda (root)
     (declare (ignore root))
     (let ((slot (pg::make-wal-replication-slot :name "lost_logical" :wal-status :lost)))
       (setf (gethash "lost_logical" pg::*wal-replication-slots*) slot)
       (let ((row (first (pg::sql-replication-slot-rows))))
         (ok (equal "wal_removed" (aref row 18)))
         (ok (not (aref row 17))))
       (multiple-value-bind (record present) (funcall (pg::make-replication-slot-result-iterator))
         (ok present)
         (let ((values (pg::sql-record-values record)))
           (ok (equal "wal_removed" (aref values 17)))
           (ok (not (aref values 16)))))))))

(deftest physical-slot-sql-calls-bind-names-defaults-and-record-fields
    (let ((call (physical-slot-test-analyze-call
                 "SELECT pg_catalog.pg_create_physical_replication_slot('slot_api')")))
      (ok (eq :record (pg::expr-type call)))
      (ok (equal '(("slot_name" :name) ("lsn" :pg-lsn)) (getf (cdddr call) :fields)))
      (ok (equal '("slot_api" nil nil) (mapcar #'second (third call)))))
  (let ((call (physical-slot-test-analyze-call
               "SELECT pg_create_physical_replication_slot(temporary=>true,slot_name=>'named_slot')")))
    (ok (equal '("named_slot" nil t) (mapcar #'second (third call)))))
  (let ((call (physical-slot-test-analyze-call
               "SELECT pg_replication_slot_advance(upto_lsn=>'0/63',slot_name=>'slot_api')")))
    (ok (equal '(("slot_name" :name) ("end_lsn" :pg-lsn)) (getf (cdddr call) :fields)))
    (ok (equal '(:name :pg-lsn) (mapcar #'pg::expr-type (third call))))))

(deftest physical-slot-sql-argument-errors-follow-c-analysis-order
    (dolist (case '(("SELECT pg_create_physical_replication_slot(bogus=>1, missing_column)" "42703")
                    ("SELECT pg_create_physical_replication_slot(slot_name=>'a', slot_name=>missing_column)" "42703")
                    ("SELECT pg_create_physical_replication_slot(temporary=>'junk')" "42883")
                    ("SELECT pg_replication_slot_advance(upto_lsn=>'junk')" "42883")
                    ("SELECT pg_drop_replication_slot(slot_name=>'absent_slot')" "42883")
                    ("SELECT pg_get_replication_slots(bogus=>1)" "42883")
                    ("SELECT pg_get_replication_slots(bogus=>1,bogus=>2)" "42601")
                    ("SELECT pg_create_physical_replication_slot(bogus=>1,bogus=>2)" "42601")))
      (destructuring-bind (sql state) case
        (ok (equal state (recovery-state-of (lambda () (physical-slot-test-analyze-call sql))))))))

(deftest physical-slots-do-not-enable-the-logical-capture-mirror
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*logical-capture-live-slots* 0))
         (pg::wal-create-physical-replication-slot "physical_one")
         (pg::wal-create-physical-replication-slot "physical_two")
         (pg::wal-drop-replication-slot "physical_one")
         (ok (zerop pg::*logical-capture-live-slots*))
         (setf (gethash "logical_one" pg::*wal-replication-slots*)
               (pg::make-wal-replication-slot :name "logical_one"))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::wal-refresh-logical-capture-slot-mirror-under-wal-lock))
         (ok (= 1 pg::*logical-capture-live-slots*))))))

(deftest public-physical-slot-creation-shares-names-and-capacity
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*max-replication-slots* 1))
         (declare (special pg::*max-replication-slots*))
         (let ((slot (pg::wal-create-physical-replication-slot "physical_one")))
           (ok (pg::wal-physical-slot-p slot))
           (ok (null (pg::wal-replication-slot-restart-lsn slot)))
           (ok (null (pg::wal-replication-slot-active-owner slot)))
           (ok (probe-file (pg::wal-replication-slot-path "physical_one")))
           (ok (equal "42710" (recovery-state-of
                               (lambda () (pg::wal-create-physical-replication-slot "physical_one")))))
           (ok (equal "53400" (recovery-state-of
                               (lambda () (pg::wal-create-physical-replication-slot "second_slot"))))))
         (pg::wal-drop-replication-slot "physical_one")
         (setf (gethash "logical_name" pg::*wal-replication-slots*)
               (pg::make-wal-replication-slot :name "logical_name"))
         (ok (equal "42710" (recovery-state-of
                             (lambda () (pg::wal-create-physical-replication-slot "logical_name")))))
         (ok (equal "53400" (recovery-state-of
                             (lambda () (pg::wal-create-physical-replication-slot "second_slot")))))))))

(deftest live-standby-slot-reservation-uses-proven-floor-and-replay-bound
    (dolist (compact '(nil t))
      (let* ((catalog (pg::make-catalog :cluster-identity "history-source"))
             (system-id (pg::catalog-system-identifier catalog))
             (replay (if compact 96 64)))
        (call-with-slot-standby-fixture
         (lambda (root store)
           (declare (ignore root))
           (let ((pg::*catalog* catalog)
                 (pg::*wal-replication-slots* (make-hash-table :test #'equal))
                 (pg::*max-replication-slots* 10)
                 (pg::*wal-poisoned* nil)
                 (pg::*backend* (pg::make-backend :pid 1234)))
             (pg::with-live-operation
               (let* ((floor (pg::physical-store-floor-lsn store))
                      (slot (pg::wal-create-physical-replication-slot "live_standby" :immediately-reserve t)))
                 (ok (= (if compact 96 32) floor))
                 (ok (= floor (pg::wal-replication-slot-restart-lsn slot)))
                 (ok (= 1 (pg::wal-replication-slot-restart-timeline slot)))
                 (unless compact
                   (ok (= 63 (pg::wal-advance-physical-replication-slot "live_standby" 63))))
                 (ok (= replay (pg::wal-advance-physical-replication-slot "live_standby" #xffffffffffffffff)))
                 (ok (= replay (pg::wal-replication-slot-restart-lsn
                                (pg::wal-read-replication-slot (pg::wal-replication-slot-path "live_standby"))))))
               (when compact
                 (let ((reference (pg::physical-store-retained-root store)))
                   (unwind-protect
                        (progn
                          (setf (pg::physical-store-retained-root store) nil)
                          (ok (intake-fails-p
                               (lambda () (pg::wal-create-physical-replication-slot "unproved_standby" :immediately-reserve t)))))
                     (setf (pg::physical-store-retained-root store) reference))
                   (ok (null (gethash "unproved_standby" pg::*wal-replication-slots*)))
                   (ok (not (probe-file (pg::wal-replication-slot-path "unproved_standby")))))))))
         :compact compact :replay replay :system-id system-id))))

(deftest physical-slot-sql-advance-clamps-interior-positions-and-saves-first
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((slot (physical-slot-test-object
                    :restart 32 :system-id (pg::catalog-system-identifier))))
         (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
         (pg::wal-save-replication-slot slot)
         (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
         (setf (pg::wal-replication-slot-inactive-since slot) 1)
         (ok (= 32 (pg::wal-advance-physical-replication-slot "public_physical" 32)))
         (ok (> (pg::wal-replication-slot-inactive-since slot) 1))
         (ok (= 99 (pg::wal-advance-physical-replication-slot "public_physical" 99)))
         (ok (= 99 (pg::wal-replication-slot-restart-lsn
                    (pg::wal-read-replication-slot (pg::wal-replication-slot-path "public_physical")))))
         (ok (= 100 (pg::wal-advance-physical-replication-slot "public_physical" #xffffffffffffffff)))
         (ok (null (pg::wal-replication-slot-confirmed-flush-lsn slot)))
         (ok (equal "55000" (recovery-state-of
                             (lambda () (pg::wal-advance-physical-replication-slot "public_physical" 99)))))
         (ok (equal "22023" (recovery-state-of
                             (lambda () (pg::wal-advance-physical-replication-slot "public_physical" 0)))))))))

(deftest physical-slot-failed-publication-keeps-conservative-runtime-state
    (dolist (after-save '(nil t))
      (call-with-physical-slot-api-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((slot (physical-slot-test-object
                       :restart 32 :system-id (pg::catalog-system-identifier)))
                (save (symbol-function 'pg::wal-save-replication-slot)))
           (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
           (funcall save slot)
           (setf (gethash "public_physical" pg::*wal-replication-slots*) slot)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-save-replication-slot)
                        (lambda (candidate)
                          (when after-save (funcall save candidate))
                          (error "injected slot publication failure")))
                  (ok (intake-fails-p
                       (lambda () (pg::wal-advance-physical-replication-slot "public_physical" 99))))
                  (ok (eq slot (gethash "public_physical" pg::*wal-replication-slots*)))
                  (ok (= 32 (pg::wal-replication-slot-restart-lsn slot)))
                  (ok (= (if after-save 99 32)
                         (pg::wal-replication-slot-restart-lsn
                          (pg::wal-read-replication-slot
                           (pg::wal-replication-slot-path "public_physical")))))
                  (ok (intake-fails-p
                       (lambda () (pg::wal-create-physical-replication-slot "failed_create"))))
                  (if after-save
                      (progn
                        (ok (gethash "failed_create" pg::*wal-replication-slots*))
                        (ok (probe-file (pg::wal-replication-slot-path "failed_create")))
                        (ok (equal "42710" (recovery-state-of
                                            (lambda () (pg::wal-create-physical-replication-slot "failed_create"))))))
                      (progn
                        (ok (null (gethash "failed_create" pg::*wal-replication-slots*)))
                        (ok (not (probe-file (pg::wal-replication-slot-path "failed_create")))))))
             (setf (symbol-function 'pg::wal-save-replication-slot) save)))))))

(deftest invalidated-physical-temporary-slot-retires-creator-outside-storage-locks
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((owner pg::*backend*)
              (slot (pg::wal-create-physical-replication-slot "lost_temporary" :temporary t))
              (requests nil)
              (pg::*max-slot-wal-keep-bytes* 1)
              (pg::*logical-test-invalidated-backend-hook*
                (lambda (backend)
                  (ok (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                  (ok (not (sb-thread:holding-mutex-p pg::*write-lock*)))
                  (push backend requests)
                  (pg::wal-cleanup-backend-physical-slots backend))))
         (setf (pg::wal-replication-slot-restart-lsn slot) 32
               (pg::wal-replication-slot-restart-timeline slot) 1)
         (pg::wal-save-replication-slot slot)
         (let ((pg::*backend* nil))
           (pg::call-with-invalidated-slot-cleanup
            (lambda ()
              (sb-thread:with-mutex (pg::*wal-lock*)
                (pg::wal-invalidate-lagging-replication-slots-under-wal-lock pg::*max-slot-wal-keep-bytes*)))))
         (ok (equal (list owner) requests))
         (ok (eq :lost (pg::wal-replication-slot-wal-status slot)))
         (ok (null (gethash "lost_temporary" pg::*wal-replication-slots*)))
         (ok (not (probe-file (pg::wal-replication-slot-path "lost_temporary"))))))))

(deftest physical-slot-loss-cleanup-rediscovers-current-owner-without-resignaling
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((owner pg::*backend*)
              (slot (pg::wal-create-physical-replication-slot "retry_temporary" :temporary t))
              (requests nil)
              (pg::*logical-test-invalidated-backend-hook* (lambda (backend) (push backend requests))))
         (setf (pg::wal-replication-slot-wal-status slot) :lost)
         (pg::wal-save-replication-slot slot)
         (let ((pg::*backend* nil))
           (dotimes (attempt 2)
             (declare (ignorable attempt))
             (pg::call-with-invalidated-slot-cleanup (lambda () (values)))))
         (ok (equal (list owner) requests))
         (ok (eq slot (gethash "retry_temporary" pg::*wal-replication-slots*)))
         (pg::wal-cleanup-backend-physical-slots owner)
         (ok (null (gethash "retry_temporary" pg::*wal-replication-slots*)))))))

(deftest stale-slot-generation-cannot-terminate-the-current-backend
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((owner pg::*backend*)
              (slot (pg::wal-create-physical-replication-slot "reused_temporary" :temporary t))
              (replacement (pg::copy-wal-replication-slot slot))
              (original (symbol-function 'pg::terminate-backend))
              (terminated nil)
              (requests nil)
              (pg::*logical-test-invalidated-backend-hook* (lambda (backend) (push backend requests)))
              (pg::*logical-test-failpoint-hook*
                (lambda (point &rest details)
                  (declare (ignore details))
                  (when (eq point :before-lost-slot-owner-termination-claim)
                    (setf (gethash "reused_temporary" pg::*wal-replication-slots*) replacement)))))
         (setf (pg::wal-replication-slot-wal-status slot) :lost)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::terminate-backend)
                      (lambda (backend &rest options)
                        (declare (ignore options)) (push backend terminated)))
                (pg::cleanup-invalidated-logical-replication-state (list slot))
                (ok (null terminated))
                (ok (null requests))
                (ok (eq replacement (gethash "reused_temporary" pg::*wal-replication-slots*)))
                (ok (eq owner (pg::wal-replication-slot-active-owner-backend replacement)))
                (ok (eq :reserved (pg::wal-replication-slot-wal-status replacement))))
           (setf (symbol-function 'pg::terminate-backend) original))))))

(deftest logical-slot-acknowledgment-paths-reject-physical-state
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((slot (pg::wal-create-physical-replication-slot "physical_ack" :temporary t)))
         (dolist (operation
                  (list (lambda () (pg::wal-acknowledge-replication-slot "physical_ack" 64))
                        (lambda () (pg::wal-finalize-replication-slot-copy "physical_ack" 64 pg::*backend*))
                        (lambda ()
                          (sb-thread:with-mutex (pg::*wal-lock*)
                            (pg::wal-persist-replication-slot-ack-under-lock slot 64)))))
           (ok (equal "55000" (recovery-state-of operation))))
         (ok (null (pg::wal-replication-slot-confirmed-flush-lsn slot)))
         (ok (null (pg::wal-replication-slot-restart-lsn slot)))
         (ok (null (pg::wal-replication-slot-confirmed-flush-lsn
                    (pg::wal-read-replication-slot (pg::wal-replication-slot-path "physical_ack")))))))))

(deftest temporary-physical-slot-ownership-survives-operations-and-ends-on-error-or-exit
    (call-with-physical-slot-api-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((owner pg::*backend*))
         (let ((slot (pg::wal-create-physical-replication-slot "creator_temporary" :temporary t)))
           (ok (eq owner (pg::wal-replication-slot-temporary-owner slot)))
           (ok (pg::wal-replication-slot-active-owner slot))
           (let ((pg::*backend* (pg::make-backend :pid 1235)))
             (ok (equal "55006" (recovery-state-of
                                 (lambda () (pg::wal-drop-replication-slot "creator_temporary"))))))
           (pg::wal-drop-replication-slot "creator_temporary")
           (ok (not (probe-file (pg::wal-replication-slot-path "creator_temporary")))))
         (pg::wal-create-physical-replication-slot "error_temporary" :temporary t)
         (pg::note-query-error)
         (ok (null (gethash "error_temporary" pg::*wal-replication-slots*)))
         (pg::wal-create-physical-replication-slot "exit_temporary" :temporary t)
         (pg::end-backend-session)
         (ok (null (gethash "exit_temporary" pg::*wal-replication-slots*)))))))
