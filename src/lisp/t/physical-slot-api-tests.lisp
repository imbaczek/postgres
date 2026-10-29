(in-package #:pg-physical-tests)

(defun call-with-physical-slot-api-fixture (function)
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root)
          (pg::*catalog* (pg::make-catalog :cluster-identity "slot-api"))
          (pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*wal-replication-slots* (make-hash-table :test #'equal))
          (pg::*max-replication-slots* 10)
          (pg::*wal-slot-retention-policy* nil)
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
         (ok (= 0 (pg::wal-slot-removal-segment 4 0)))
         (ok (= 1 (pg::wal-slot-removal-segment 1 1)))
         (ok (= 4 (pg::wal-slot-removal-segment 4 4)))
         (ok (= (- size pg::*wal-end-lsn*) (pg::wal-slot-safe-wal-bytes slot)))
         (setf pg::*wal-slot-retention-policy*
               (pg::make-wal-slot-retention-policy :mode :segments :limit (+ size (* 1024 1024))))
         (ok (= 3 (pg::wal-slot-removal-segment 4 4)))
         (setf pg::*wal-slot-retention-policy*
               (pg::make-wal-slot-retention-policy :mode :segments :limit nil))
         (ok (= 0 (pg::wal-slot-removal-segment 4 4)))
         (ok (eq pg::+null+ (pg::wal-slot-safe-wal-bytes slot)))))))

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
               (ok (equal status (pg::wal-slot-catalog-status slot)))
               (ok (= safe (pg::wal-slot-safe-wal-bytes slot))))))))))

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
         (ok (equal "extended" (pg::wal-slot-catalog-status slot)))
         (ok (eq pg::+null+ (pg::wal-slot-safe-wal-bytes slot)))
         (setf pg::*max-slot-wal-keep-bytes* 64
               pg::*wal-end-lsn* 65)
         (ok (equal "reserved" (pg::wal-slot-catalog-status slot)))
         (ok (= 1 (pg::wal-slot-safe-wal-bytes slot)))
         (setf pg::*wal-end-lsn* 66)
         (ok (equal "unreserved" (pg::wal-slot-catalog-status slot)))
         (ok (= 0 (pg::wal-slot-safe-wal-bytes slot)))
         (ok (eq :reserved (pg::wal-replication-slot-wal-status slot)))
         (setf pg::*wal-end-lsn* 67)
         (ok (= -1 (pg::wal-slot-safe-wal-bytes slot)))
         (setf pg::*max-slot-wal-keep-bytes* nil
               pg::*wal-slot-retention-policy* nil
               pg::*wal-end-lsn* (+ (* 70 size) 32))
         (ok (equal "extended" (pg::wal-slot-catalog-status slot)))))))

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
                (pg::wal-invalidate-lagging-replication-slots-under-wal-lock)))))
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
