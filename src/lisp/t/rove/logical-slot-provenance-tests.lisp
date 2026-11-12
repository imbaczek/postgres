(in-package #:pg)

(cl-spec:defproperty logical-slot-generation-roundtrip
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-generation-token logical-slot-parse-generation)
  (:trials (:normal 200))
  (let ((text (logical-slot-generation-token generation)))
    (and (= 32 (length text))
         (every (lambda (char) (find char "0123456789abcdef")) text)
         (= generation (logical-slot-parse-generation text)))))

(rove:deftest logical-slot-generation-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-generation-roundtrip :seed 246))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-slot-generation-canonical-boundaries
    (dolist (pair '((0 "00000000000000000000000000000000")
                    (1 "00000000000000000000000000000001")
                    (340282366920938463463374607431768211455
                     "ffffffffffffffffffffffffffffffff")))
      (rove:ok (string= (second pair) (logical-slot-generation-token (first pair))))
      (rove:ok (= (first pair) (logical-slot-parse-generation (second pair)))))
  (dolist (bad '(nil -1 340282366920938463463374607431768211456 1.0 "1"))
    (rove:ok (rove:signals (logical-slot-generation-token bad) 'error))))

(rove:deftest logical-slot-generation-rejects-noncanonical-tokens
    (dolist (bad '(nil 0 "" "0" "0000000000000000000000000000000"
                   "000000000000000000000000000000000"
                   "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
                   "+0000000000000000000000000000000"
                   " 0000000000000000000000000000000"
                   "0000000000000000000000000000000g"))
      (rove:ok (rove:signals (logical-slot-parse-generation bad) 'error))))

(rove:deftest logical-slot-provenance-keeps-legacy-unknown-on
    (let ((proof (make-logical-slot-provenance
                  18446744073709551615 0 18446744073709551615 4294967295)))
      (rove:ok (logical-slot-provenance-valid-p proof 18446744073709551615))
      (rove:ok (= 18446744073709551615 (logical-slot-provenance-system-id proof)))
      (rove:ok (zerop (logical-slot-provenance-generation proof)))
      (rove:ok (= 4294967295 (logical-slot-provenance-birth-timeline proof)))
      (rove:ok (null (logical-slot-provenance-on-start-lsn proof)))
      (rove:ok (null (logical-slot-provenance-on-start-timeline proof)))
      (rove:ok (not (logical-slot-provenance-valid-p nil 65)))))

(rove:deftest logical-slot-provenance-validates-identity-and-interval
    (let ((proof (make-logical-slot-provenance 42 17 161 3 65 1)))
      (rove:ok (logical-slot-provenance-valid-p proof 161))
      (rove:ok (= 65 (logical-slot-provenance-on-start-lsn proof)))
      (rove:ok (= 1 (logical-slot-provenance-on-start-timeline proof)))
      (rove:ok (not (logical-slot-provenance-valid-p proof 64))))
  (dolist (args '((-1 0 65 1) (18446744073709551616 0 65 1)
                  (42 -1 65 1) (42 340282366920938463463374607431768211456 65 1)
                  (42 0 -1 1) (42 0 18446744073709551616 1)
                  (42 0 65 0) (42 0 65 4294967296)
                  (42 0 65 1 65 nil) (42 0 65 1 nil 1)
                  (42 0 65 1 32 1) (42 0 65 1 66 1)
                  (42 0 65 1 65 2)))
    (rove:ok (rove:signals (apply #'make-logical-slot-provenance args) 'error))))

(cl-spec:defproperty logical-slot-provenance-preserves-full-width
    ((system-id (range integer 0 18446744073709551615))
     (generation (range integer 0 340282366920938463463374607431768211455))
     (birth (range integer 33 18446744073709551615))
     (timeline (range integer 1 4294967295)))
  (:about make-logical-slot-provenance logical-slot-provenance-valid-p)
  (:trials (:normal 200))
  (let ((proof (make-logical-slot-provenance system-id generation birth timeline birth timeline)))
    (and (logical-slot-provenance-valid-p proof birth)
         (= system-id (logical-slot-provenance-system-id proof))
         (= generation (logical-slot-provenance-generation proof))
         (= timeline (logical-slot-provenance-birth-timeline proof))
         (= birth (logical-slot-provenance-on-start-lsn proof))
         (= timeline (logical-slot-provenance-on-start-timeline proof)))))

(rove:deftest logical-slot-provenance-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-provenance-preserves-full-width :seed 247))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-slot-restart-selection-keeps-position-role-at-fork
    (let ((history (timeline-parse-history 2 (format nil "1~C0/80~Cfork~%" #\Tab #\Tab))))
      (rove:ok (= 1 (timeline-at-point 127 history)))
      (rove:ok (= 2 (timeline-at-point 128 history)))
      (rove:ok (equal '(128 1 :prefix-end)
                      (multiple-value-list (logical-slot-select-restart-position 128 1 nil))))
      (rove:ok (equal '(128 2 :record-start)
                      (multiple-value-list
                       (logical-slot-select-restart-position 128 1 '((128 . 2))))))
      (rove:ok (equal '(64 1 :record-start)
                      (multiple-value-list
                       (logical-slot-select-restart-position 128 1 '((128 . 2) (64 . 1))))))
      (rove:ok (equal '(128 1 :prefix-end)
                      (multiple-value-list
                       (logical-slot-select-restart-position 128 1 '((160 . 2))))))))

(rove:deftest logical-slot-restart-selection-rejects-conflicting-minimum
    (rove:ok (rove:signals
              (logical-slot-select-restart-position 128 1 '((64 . 1) (64 . 2))) 'error))
  (dolist (args '((-1 1 nil) (18446744073709551616 1 nil) (65 0 nil)
                  (65 4294967296 nil) (65 1 ((-1 . 1))) (65 1 ((0 . 0)))
                  (65 1 ((0 . 4294967296))) (65 1 ((0 1))) (65 1 (nil))))
    (rove:ok (rove:signals (apply #'logical-slot-select-restart-position args) 'error))))

(cl-spec:defproperty logical-slot-restart-selection-prefers-pending-tie
    ((boundary (range integer 0 18446744073709551615))
     (parent (range integer 1 4294967294)))
  (:about logical-slot-select-restart-position)
  (:trials (:normal 200))
  (let ((child (1+ parent)))
    (and (equal (list boundary parent :prefix-end)
                (multiple-value-list (logical-slot-select-restart-position boundary parent nil)))
         (equal (list boundary child :record-start)
                (multiple-value-list
                 (logical-slot-select-restart-position boundary parent
                                                       (list (cons boundary child)
                                                             (cons boundary child))))))))

(rove:deftest logical-slot-restart-selection-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-restart-selection-prefers-pending-tie :seed 248))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-slot-public-creation-persists-captured-provenance
    (dolist (entry '(("standard" :standard) ("sql" :sql)))
      (call-with-logical-slot-creation-test
       (lambda ()
         (let ((name (first entry))
               (expected-on (wal-report-logical-decoding-status t)))
           (ecase (second entry)
             (:standard (create-standard-logical-slot name :nothing))
             (:sql (create-sql-logical-replication-slot name "pgoutput" nil nil nil)))
           (let* ((slot (gethash name *wal-replication-slots*))
                  (saved (wal-read-replication-slot (wal-replication-slot-path name)))
                  (provenance (wal-replication-slot-logical-provenance slot))
                  (restored (wal-replication-slot-logical-provenance saved)))
             (rove:ok provenance)
             (rove:ok restored)
             (when (and provenance restored)
               (rove:ok (= (catalog-system-identifier)
                           (logical-slot-provenance-system-id provenance)
                           (logical-slot-provenance-system-id restored)))
               (rove:ok (= (logical-slot-provenance-generation provenance)
                           (logical-slot-provenance-generation restored)))
               (rove:ok (= 1 (logical-slot-provenance-birth-timeline provenance)
                           (wal-replication-slot-restart-timeline slot)
                           (wal-replication-slot-confirmed-timeline slot)))
               (rove:ok (= expected-on
                           (logical-slot-provenance-on-start-lsn provenance)
                           (logical-slot-provenance-on-start-lsn restored)))
               (rove:ok (= 1 (logical-slot-provenance-on-start-timeline provenance)
                           (logical-slot-provenance-on-start-timeline restored)))
               (rove:ok (= (wal-replication-slot-consistent-lsn slot)
                           (wal-replication-slot-confirmed-flush-lsn slot)))
               (rove:ok (eq :none (wal-replication-slot-invalidation-cause saved))))
             (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))))

(rove:deftest logical-slot-private-creation-keeps-copy-unconfirmed
    (call-with-logical-slot-creation-test
     (lambda ()
       (multiple-value-bind (birth token) (create-exported-logical-snapshot-slot "private")
         (unwind-protect
              (let* ((slot (gethash "private" *wal-replication-slots*))
                     (saved (wal-read-replication-slot (wal-replication-slot-path "private"))))
                (rove:ok (plusp birth))
                (rove:ok (wal-replication-slot-logical-provenance saved))
                (rove:ok (null (wal-replication-slot-confirmed-flush-lsn slot)))
                (rove:ok (null (wal-replication-slot-confirmed-timeline saved)))
                (rove:ok (= 1 (wal-replication-slot-restart-timeline saved))))
           (remove-exported-logical-snapshot-token token))))))

(rove:deftest logical-slot-creation-rejects-identity-before-publication
    (dolist (failure '(:missing :live :control :entropy))
      (call-with-logical-slot-creation-test
       (lambda ()
         (let ((generation (symbol-function 'logical-slot-new-generation))
               (calls 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'logical-slot-new-generation)
                        (lambda ()
                          (incf calls)
                          (if (eq failure :entropy) (error "entropy fault")
                              (funcall generation))))
                  (ecase failure
                    (:missing (delete-file (catalog-path)))
                    (:live (setf *catalog* (make-catalog)))
                    (:control (setf *wal-control-metadata*
                                    (make-wal-control-metadata
                                     (logxor 1 (catalog-system-identifier))
                                     +db-in-production+)))
                    (:entropy))
                  (rove:ok (handler-case
                               (progn (create-standard-logical-slot "rejected" :nothing) nil)
                             (error () t)))
                  (rove:ok (= calls (if (eq failure :entropy) 1 0)))
                  (rove:ok (null (gethash "rejected" *wal-replication-slots*)))
                  (rove:ok (null (probe-file (wal-replication-slot-path "rejected"))))
                  (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))
             (setf (symbol-function 'logical-slot-new-generation) generation)))))))

(rove:deftest logical-slot-creation-rejects-detached-capture-evidence
    (dolist (failure '(:boundary :restart :missing :pin))
      (call-with-logical-slot-creation-test
       (lambda ()
         (call-with-logical-capture-armed
          (lambda ()
            (let ((registration (make-snapshot-registration)))
              (unwind-protect
                   (with-write-lock
                     (wal-report-logical-decoding-status t)
                     (multiple-value-bind (snapshot birth restart)
                         (wal-capture-snapshot-boundary-under-writer-lock 0 registration)
                       (declare (ignore snapshot))
                       (ecase failure
                         (:boundary (incf birth))
                         (:restart (decf restart))
                         (:missing (setf (snapshot-registration-wal-boundary registration) nil))
                         (:pin (incf (gethash registration *wal-snapshot-retention-pins*))))
                       (sb-thread:with-mutex (*wal-lock*)
                         (rove:ok (handler-case
                                      (progn
                                        (wal-publish-replication-slot-under-wal-lock
                                         "detached" birth restart registration nil nil 16384 "pgoutput")
                                        nil)
                                    (error () t)))
                         (rove:ok (null (gethash "detached" *wal-replication-slots*)))
                         (rove:ok (null (probe-file (wal-replication-slot-path "detached")))))))
                (release-snapshot-registration registration)))))))))

(rove:deftest logical-slot-created-image-restores-through-startup
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (create-standard-logical-slot "restored" :nothing)
       (let* ((before (gethash "restored" *wal-replication-slots*))
              (provenance (wal-replication-slot-logical-provenance before))
              (*catalog* nil)
              (*wal-replication-slots* (make-hash-table :test #'equal)))
         (call-with-logical-slot-owned-startup-test
          (lambda (gate-owner ownership)
            (wal-load-replication-slots :gate-owner gate-owner :ownership ownership)))
         (let* ((restored (gethash "restored" *wal-replication-slots*))
                (after (wal-replication-slot-logical-provenance restored)))
           (rove:ok (null *catalog*))
           (rove:ok (not (eq before restored)))
           (rove:ok (= (logical-slot-provenance-system-id provenance)
                       (logical-slot-provenance-system-id after)))
           (rove:ok (= (logical-slot-provenance-generation provenance)
                       (logical-slot-provenance-generation after)))
           (rove:ok (= (wal-replication-slot-consistent-lsn before)
                       (wal-replication-slot-consistent-lsn restored)))
           (rove:ok (= 1 (wal-replication-slot-restart-timeline restored)
                       (wal-replication-slot-confirmed-timeline restored)))
           (rove:ok (null (wal-replication-slot-active-owner restored))))))))

(rove:deftest logical-slot-creation-preserves-fork-position-roles
    (dolist (pending-p '(nil t))
      (call-with-logical-slot-creation-test
       (lambda ()
         (let* ((fork 100)
                (*wal-insert-timeline* 2)
                (registration (make-snapshot-registration))
                (boundary (make-snapshot-wal-boundary
                           :lsn fork :timeline 1 :restart-lsn fork
                           :restart-timeline (if pending-p 2 1)
                           :restart-role (if pending-p :record-start :prefix-end))))
           ;; Geometry only: this does not claim a child WAL record exists at FORK.
           (timeline-write-history 2 1 fork "logical creation geometry fixture")
           (sb-thread:with-mutex (*xid-lock*)
             (register-active-snapshot (make-snapshot-struct :xmin 100 :xmax 100) registration))
           (setf (snapshot-registration-wal-boundary registration) boundary
                 (snapshot-registration-wal-retention-lsn registration) fork
                 (gethash registration *wal-snapshot-retention-pins*) fork)
           (sb-thread:with-mutex (*wal-lock*)
             (multiple-value-bind (provenance restart-timeline birth-timeline)
                 (logical-slot-creation-evidence-under-lock fork fork registration)
               (rove:ok (= 1 birth-timeline
                           (logical-slot-provenance-birth-timeline provenance)))
               (rove:ok (= (if pending-p 2 1) restart-timeline))
               (rove:ok (null (logical-slot-provenance-on-start-lsn provenance))))
             (when pending-p
               (setf (snapshot-registration-wal-boundary registration)
                     (make-snapshot-wal-boundary
                      :lsn fork :timeline 1 :restart-lsn fork :restart-timeline 1
                      :restart-role :record-start))
               (rove:ok (handler-case
                            (progn
                              (logical-slot-creation-evidence-under-lock fork fork registration)
                              nil)
                          (error () t))
                        "a record start at the child fork cannot be stamped as its parent"))))))))

(rove:deftest logical-slot-factory-persists-fork-position-roles
    (dolist (pending-p '(nil t))
      (call-with-logical-slot-creation-test
       (lambda ()
         (call-with-logical-capture-armed
          (lambda ()
            (wal-report-logical-decoding-status t)
            (let* ((fork *wal-durable-lsn*)
                   (registration (make-snapshot-registration)))
              (unwind-protect
                   (progn
                     (make-snapshot 0 :register registration)
                     (timeline-write-history 2 1 fork "factory position-role fixture")
                     ;; Supply frozen geometry to the factory; child-record byte
                     ;; availability and end-to-end promotion are separate oracles.
                     (setf (snapshot-registration-wal-boundary registration)
                           (make-snapshot-wal-boundary
                            :lsn fork :timeline 1 :restart-lsn fork
                            :restart-timeline (if pending-p 2 1)
                            :restart-role (if pending-p :record-start :prefix-end))
                           (snapshot-registration-wal-retention-lsn registration) fork
                           (gethash registration *wal-snapshot-retention-pins*) fork)
                     (let* ((*wal-insert-timeline* 2)
                            (*wal-logical-policy*
                              (%make-wal-logical-policy
                               *wal-storage-incarnation*
                               (%make-wal-logical-status t 0 fork 2 nil nil) nil)))
                       (sb-thread:with-mutex (*wal-lock*)
                         (wal-publish-replication-slot-under-wal-lock
                          "fork" fork fork registration nil nil 16384 "pgoutput")))
                     (let* ((saved (wal-read-replication-slot (wal-replication-slot-path "fork")))
                            (provenance (wal-replication-slot-logical-provenance saved)))
                       (rove:ok (= 1 (logical-slot-provenance-birth-timeline provenance)
                                   (wal-replication-slot-confirmed-timeline saved)))
                       (rove:ok (= (if pending-p 2 1)
                                   (wal-replication-slot-restart-timeline saved)))
                       (rove:ok (= fork (wal-replication-slot-consistent-lsn saved)
                                   (wal-replication-slot-restart-lsn saved)
                                   (wal-replication-slot-confirmed-flush-lsn saved)))))
                (release-snapshot-registration registration)))))))))

(rove:deftest logical-slot-creation-rechecks-concurrent-snapshot-release
    (call-with-logical-slot-creation-test
     (lambda ()
       (call-with-logical-capture-armed
        (lambda ()
          (let* ((registration (make-snapshot-registration))
                 (start (sb-thread:make-semaphore))
                 (marked (sb-thread:make-semaphore))
                 (finish (sb-thread:make-semaphore))
                 (active-set *active-snapshot-set*)
                 (pins *wal-snapshot-retention-pins*)
                 (identity-reader (symbol-function 'physical-slot-startup-system-id))
                 (generation-reader (symbol-function 'logical-slot-new-generation))
                 (generations 0)
                 (release-result nil)
                 (releaser nil))
            (unwind-protect
                 (with-write-lock
                   (wal-report-logical-decoding-status t)
                   (multiple-value-bind (snapshot birth restart)
                       (wal-capture-snapshot-boundary-under-writer-lock 0 registration)
                     (declare (ignore snapshot))
                     (setf releaser
                           (sb-thread:make-thread
                            (lambda ()
                              (let ((*active-snapshot-set* active-set)
                                    (*active-snapshots* 1)
                                    (*wal-snapshot-retention-pins* pins)
                                    (*wal-test-after-snapshot-release-mark-hook*
                                      (lambda (token)
                                        (declare (ignore token))
                                        (sb-thread:signal-semaphore marked)
                                        (sb-thread:wait-on-semaphore finish))))
                                (when (sb-thread:wait-on-semaphore start :timeout 5)
                                  (release-snapshot-registration registration)
                                  (setf release-result t))))
                            :name "logical slot snapshot release"))
                     (setf (symbol-function 'logical-slot-new-generation)
                           (lambda () (incf generations) (funcall generation-reader))
                           (symbol-function 'physical-slot-startup-system-id)
                           (lambda ()
                             ;; The factory has passed its initial active check,
                             ;; but the release still cannot remove the WAL pin.
                             (sb-thread:signal-semaphore start)
                             (assert (sb-thread:wait-on-semaphore marked :timeout 5))
                             (funcall identity-reader)))
                     (sb-thread:with-mutex (*wal-lock*)
                       (rove:ok (handler-case
                                    (progn
                                      (wal-publish-replication-slot-under-wal-lock
                                       "released" birth restart registration nil nil 16384 "pgoutput")
                                      nil)
                                  (error () t)))
                       (rove:ok (zerop generations))
                       (rove:ok (null (gethash "released" *wal-replication-slots*)))
                       (rove:ok (null (probe-file (wal-replication-slot-path "released")))))))
              (setf (symbol-function 'physical-slot-startup-system-id) identity-reader
                    (symbol-function 'logical-slot-new-generation) generation-reader)
              (sb-thread:signal-semaphore finish)
              (sb-thread:signal-semaphore start)
              (when releaser (sb-thread:join-thread releaser))
              ;; The actor decremented its own dynamic counter; reconcile this
              ;; fixture's counter only after its shared registration set is empty.
              (sb-thread:with-mutex (*snapshot-count-lock*)
                (rove:ok (zerop (hash-table-count *active-snapshot-set*)))
                (setf *active-snapshots* 0))
              (release-snapshot-registration registration))
            (rove:ok release-result)))))))

(defun unit-logical-provenance-slot (&key (provenance t) (known-on t) (cause :none)
                                       (generation 0) (system-id 42) (end 80) (timeline 1)
                                       (plugin "pgoutput") (profile :text))
  (let* ((birth (- end 15))
         (slot (make-wal-replication-slot
                :name "fixture" :database-oid 7 :restart-lsn (- end 48)
                :consistent-lsn birth :confirmed-flush-lsn end :plugin plugin
                :logical-provenance (when provenance
                                      (make-logical-slot-provenance
                                       system-id generation birth timeline
                                       (when known-on birth) (when known-on timeline)))
                :restart-timeline (when provenance timeline)
                :confirmed-timeline (when provenance timeline)
                :invalidation-cause cause
                :wal-status (if (member cause '(nil :none)) :reserved :lost))))
    (wal-slot-set-initial-profile slot profile)
    slot))

(defun unit-logical-lineage-slot (kind)
  (let ((slot
          (case kind
            (:on
             (make-wal-replication-slot
              :name "fixture" :database-oid 7 :plugin "pgoutput"
              :restart-lsn 32 :restart-timeline 1 :consistent-lsn 115
              :confirmed-flush-lsn 130 :confirmed-timeline 3
              :logical-provenance (make-logical-slot-provenance 42 0 115 3 65 2)
              :invalidation-cause :none))
            (:legacy (unit-logical-provenance-slot :provenance nil :cause nil))
            (:lost (unit-logical-provenance-slot :timeline 2 :cause :wal-removed))
            (:birth (unit-logical-provenance-slot :timeline 2))
            (:child (unit-logical-provenance-slot :timeline 3 :end 130))
            (:zero (unit-logical-provenance-slot :end 15 :known-on nil))
            (t (unit-logical-provenance-slot)))))
    (case kind
      (:confirmed (setf (wal-replication-slot-confirmed-flush-lsn slot) 150
                        (wal-replication-slot-confirmed-timeline slot) 2))
      (:restart (setf (wal-replication-slot-confirmed-flush-lsn slot) 150
                      (wal-replication-slot-confirmed-timeline slot) 3
                      (wal-replication-slot-restart-timeline slot) 2))
      (:child (setf (wal-replication-slot-restart-lsn slot) 100))
      (:zero (setf (wal-replication-slot-restart-lsn slot) 0)))
    slot))

(rove:deftest logical-slot-retained-scan-survives-a-safe-clog-reset
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (let* ((*wal-replication-slots* (make-hash-table :test #'equal))
              (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (*active-snapshots* 0)
              (*active-snapshot-set* (make-hash-table :test #'eq))
              (owner (gensym "CLOG-SCAN-OWNER-"))
              (*wal-logical-slot-owner* owner)
              (boundary (wal-report-logical-decoding-status t))
              (slot (make-wal-replication-slot
                     :name "fixture" :database-oid 7 :plugin "pgoutput"
                     :restart-lsn boundary :restart-timeline 1 :consistent-lsn boundary
                     :confirmed-flush-lsn boundary :confirmed-timeline 1
                     :logical-provenance (make-logical-slot-provenance 42 0 boundary 1 boundary 1)
                     :invalidation-cause :none :active-owner owner)))
         (ensure-directories-exist (wal-replication-slot-directory))
         (wal-create-replication-slot-file slot)
         (setf (gethash "fixture" *wal-replication-slots*) slot)
         (sb-thread:with-mutex (*wal-lock*)
           (wal-refresh-logical-capture-slot-mirror-under-wal-lock))
         (let ((end (with-wal-critical
                      (wal-append-batch-start)
                      (let ((end (wal-append-record
                                  +wal-record-maintenance+ 0
                                  (make-array 0 :element-type '(unsigned-byte 8)))))
                        (wal-flush-core)
                        end))))
           (call-with-wal-retained-slot-scan
            "fixture" boundary end
            (lambda (scan)
              (let ((before *wal-storage-incarnation*))
                (clog-reset)
                (rove:ok (not (eq before *wal-storage-incarnation*))
                         "CLOG reset rotates the internal storage epoch")
                (let ((records (loop for record = (wal-retained-slot-scan-next scan)
                                     while record collect record)))
                  (rove:ok (equal (list +wal-record-batch-start+ +wal-record-maintenance+)
                                  (mapcar #'wal-record-type records))
                           "the captured WAL records remain readable after CLOG reset")
                  (rove:ok (= end (wal-record-end (car (last records))))))))))
         (rove:ok (eq slot (gethash "fixture" *wal-replication-slots*)))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))

(rove:deftest logical-slot-scan-validates-known-lineage-before-pinning
    (dolist (kind '(:valid :child :legacy :lost :birth :confirmed :on :restart :zero))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-insert-timeline* 3) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                          (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                                          (owner (gensym "LINEAGE-SCAN-OWNER-"))
                                          (*wal-logical-slot-owner* owner)
                                          (slot (unit-logical-lineage-slot kind))
                                          (entered nil)
                                          (admitted-p (not (null (member kind '(:valid :child :legacy))))))
           (unless (eq kind :legacy)
             (timeline-write-history 3 1 100 "scan lineage admission"))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot
                 (wal-replication-slot-active-owner slot) owner)
           (rove:ok
            (eq admitted-p
                (handler-case
                    (progn
                      (call-with-wal-retained-slot-scan
                       "fixture" (wal-replication-slot-confirmed-flush-lsn slot)
                       (wal-replication-slot-confirmed-flush-lsn slot)
                       (lambda (scan)
                         (setf entered t)
                         (rove:ok (null (wal-retained-slot-scan-next scan)))))
                      t)
                  (error () nil)))
            (format nil "~a scan geometry" kind))
           (rove:ok (eq admitted-p entered) "rejected geometry never reaches the scan callback")
           (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))))))

(rove:deftest logical-slot-retained-scan-rechecks-slot-pin-and-source-position
    (dolist (change '(:slot :pin :timeline :namespace))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-insert-timeline* 3) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                          (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                                          (owner (gensym "SOURCE-SCAN-OWNER-"))
                                          (*wal-logical-slot-owner* owner)
                                          (slot (unit-logical-lineage-slot :valid))
                                          (rejected nil))
           (timeline-write-history 3 1 100 "scan source identity")
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot
                 (wal-replication-slot-active-owner slot) owner)
           (call-with-wal-retained-slot-scan
            "fixture" 80 80
            (lambda (scan)
              (ecase change
                (:slot (setf (gethash "fixture" *wal-replication-slots*)
                             (wal-copy-replication-slot-with-profile slot)))
                (:pin (setf *wal-snapshot-retention-pins* (make-hash-table :test #'eq)))
                (:timeline (setf *wal-insert-timeline* 4))
                (:namespace (setf *wal-segment-namespace* :timeline)))
              (setf rejected (handler-case (progn (wal-retained-slot-scan-next scan) nil)
                               (error () t)))))
           (rove:ok rejected (format nil "~a change invalidates the scan" change))
           (rove:ok (= 80 (wal-replication-slot-confirmed-flush-lsn slot)))
           (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))))))

(rove:deftest logical-slot-startup-validates-reserved-known-lineage-before-publication
    (dolist (kind '(:valid :child :legacy :lost :birth :confirmed :on :restart :zero))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-insert-timeline* 3) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                          (*wal-control-metadata* nil)
                                          (slot (unit-logical-lineage-slot kind))
                                          (original *wal-replication-slots*)
                                          (admitted-p (not (null (member kind '(:valid :child :legacy :lost))))))
           (timeline-write-history 3 1 100 "startup lineage admission")
           (rove:ok
            (eq admitted-p
                (handler-case (progn (logical-slot-check-startup-history (list slot)) t)
                  (error () nil)))
            (format nil "~a startup geometry" kind))
           (rove:ok (eq original *wal-replication-slots*)
                    "pure geometry preparation never publishes the inventory"))))))

(rove:deftest logical-slot-legacy-acknowledgment-keeps-unknown-timelines
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-insert-timeline* 7)
              (slot (unit-logical-provenance-slot :provenance nil :cause nil)))
         (wal-create-replication-slot-file slot)
         (setf (gethash "fixture" *wal-replication-slots*) slot)
         (sb-thread:with-mutex (*wal-lock*)
           (wal-persist-replication-slot-ack-under-lock slot 90))
         (rove:ok (= 90 (wal-replication-slot-confirmed-flush-lsn slot)))
         (rove:ok (null (wal-replication-slot-restart-timeline slot)))
         (rove:ok (null (wal-replication-slot-confirmed-timeline slot)))
         (rove:ok (string= "PGSLOT4 " (bytes->string (wal-encode-replication-slot slot))
                           :end2 8))))))

(rove:deftest logical-slot-acknowledgment-cannot-retag-a-divergent-source
    (dolist (birth-diverged-p '(nil t))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-insert-timeline* 3) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                          (slot (unit-logical-provenance-slot :timeline (if birth-diverged-p 2 1)))
                                          (confirmed (if birth-diverged-p 80 150))
                                          (writes 0))
           (timeline-write-history 2 1 (if birth-diverged-p 0 100) "previous source branch")
           (timeline-write-history 3 1 (if birth-diverged-p 0 100) "divergent current branch")
           (unless birth-diverged-p
             (setf (wal-replication-slot-confirmed-flush-lsn slot) confirmed
                   (wal-replication-slot-confirmed-timeline slot) 2))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot)
           (let ((*logical-test-failpoint-hook*
                   (lambda (point &rest ignored)
                     (declare (ignore ignored))
                     (when (eq point :slot-file-before-write) (incf writes)))))
             (rove:ok (handler-case
                          (progn
                            (sb-thread:with-mutex (*wal-lock*)
                              (wal-persist-replication-slot-ack-under-lock slot 180 160))
                            nil)
                        (error () t))
                      "current history cannot replace the slot's stored source history"))
           (rove:ok (zerop writes))
           (rove:ok (= confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
           (rove:ok (not (wal-replication-slot-state-uncertain-p slot))))))))

(rove:deftest logical-slot-zero-restart-retains-only-a-proven-record-owner
    (dolist (ack '(100 150))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-insert-timeline* 1) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                          (slot (unit-logical-provenance-slot)))
           (setf (wal-replication-slot-restart-lsn slot) 0
                 (gethash 71 (wal-replication-slot-pending-xids slot)) (cons 0 150))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot)
           (sb-thread:with-mutex (*wal-lock*)
             (wal-persist-replication-slot-ack-under-lock slot ack 0))
           (rove:ok (= ack (wal-replication-slot-confirmed-flush-lsn slot)))
           (rove:ok (zerop (wal-replication-slot-restart-lsn slot)))
           (rove:ok (= 1 (wal-replication-slot-restart-timeline slot)))))))
  (dolist (wrong-history-p '(nil t))
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-insert-timeline* (if wrong-history-p 2 1))
              (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
              (slot (unit-logical-provenance-slot :timeline (if wrong-history-p 2 1))))
         (when wrong-history-p
           (timeline-write-history 2 1 0 "zero restart owner test")
           (setf (wal-replication-slot-restart-lsn slot) 0
                 (wal-replication-slot-restart-timeline slot) 1))
         (wal-create-replication-slot-file slot)
         (setf (gethash "fixture" *wal-replication-slots*) slot)
         (rove:ok
          (handler-case
              (progn
                (sb-thread:with-mutex (*wal-lock*)
                  (wal-persist-replication-slot-ack-under-lock slot 150 0))
                nil)
            (error () t))
          "zero floor requires the old zero restart and its verified record owner")
         (rove:ok (= 80 (wal-replication-slot-confirmed-flush-lsn slot)))
         (rove:ok (not (wal-replication-slot-state-uncertain-p slot))
                  "rejected ownership cannot reach the save"))))))

(rove:deftest logical-slot-acknowledgment-carries-prefix-and-pending-owners
    (dolist (case '((100 100 1 2) (150 100 2 1)))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-insert-timeline* 2) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                          (slot (unit-logical-provenance-slot)))
           (timeline-write-history 2 1 100 "acknowledgment owner test")
           (setf (gethash 71 (wal-replication-slot-pending-xids slot)) (cons 100 150))
           (when (= 150 (first case))
             (setf (wal-replication-slot-confirmed-flush-lsn slot) 100
                   (wal-replication-slot-restart-lsn slot) 100
                   (wal-replication-slot-restart-timeline slot) 2))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot)
           (sb-thread:with-mutex (*wal-lock*)
             (wal-persist-replication-slot-ack-under-lock slot (first case) (second case)))
           (rove:ok (= (first case) (wal-replication-slot-confirmed-flush-lsn slot)))
           (rove:ok (= 100 (wal-replication-slot-restart-lsn slot)))
           (rove:ok (= (third case) (wal-replication-slot-confirmed-timeline slot))
                    "acknowledgment owns a consumed prefix end")
           (rove:ok (= (fourth case) (wal-replication-slot-restart-timeline slot))
                    "surviving pending start wins a floor tie; otherwise floor owns the prefix")
           (rove:ok (equalp (wal-encode-replication-slot slot)
                            (wal-encode-replication-slot
                             (wal-read-replication-slot (wal-replication-slot-path "fixture"))))))))))

(rove:deftest logical-slot-initial-copy-confirms-frozen-birth-owner
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-insert-timeline* 2) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                        (owner (gensym "INITIAL-COPY-OWNER-"))
                                        (*wal-logical-slot-owner* owner)
                                        (slot (unit-logical-provenance-slot :plugin "pg_lisp" :end 115)))
         (timeline-write-history 2 1 100 "initial copy owner test")
         (setf (wal-replication-slot-confirmed-flush-lsn slot) nil
               (wal-replication-slot-confirmed-timeline slot) nil
               (wal-replication-slot-restart-lsn slot) 100
               (wal-replication-slot-restart-timeline slot) 2
               (wal-replication-slot-active-owner slot) owner
               (gethash 71 (wal-replication-slot-pending-xids slot)) (cons 100 150))
         (wal-create-replication-slot-file slot)
         (setf (gethash "fixture" *wal-replication-slots*) slot)
         (wal-finalize-replication-slot-copy "fixture" 100 owner)
         (rove:ok (= 100 (wal-replication-slot-confirmed-flush-lsn slot)))
         (rove:ok (= 1 (wal-replication-slot-confirmed-timeline slot)))
         (rove:ok (= 2 (wal-replication-slot-restart-timeline slot)))
         (rove:ok (not (wal-replication-slot-state-uncertain-p slot)))))))

(rove:deftest logical-slot-noop-acknowledgment-keeps-persisted-owners
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-insert-timeline* 2) (*wal-durable-lsn* 200) (*wal-end-lsn* 200)
                                        (slot (unit-logical-provenance-slot))
                                        (writes 0))
         (timeline-write-history 2 1 80 "no-op owner test")
         (wal-create-replication-slot-file slot)
         (setf (gethash "fixture" *wal-replication-slots*) slot)
         (let ((*logical-test-failpoint-hook*
                 (lambda (point &rest ignored)
                   (declare (ignore ignored))
                   (when (eq point :slot-file-before-write) (incf writes)))))
           (rove:ok (eq slot (wal-acknowledge-replication-slot "fixture" 80))))
         (rove:ok (zerop writes))
         (rove:ok (= 1 (wal-replication-slot-confirmed-timeline slot)))
         (rove:ok (= 1 (wal-replication-slot-restart-timeline slot)))))))

(rove:deftest logical-slot-capture-cannot-publish-evidence-after-release-or-reuse
    (dolist (reuse-p '(nil t))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-ready* t) (*wal-insert-timeline* 1)
                                (*active-snapshots* 0)
                                (*active-snapshot-set* (make-hash-table :test #'eq))
                                (*active-xids* nil) (*next-xid* 5000)
                                (*wal-xid-first-lsn* (make-hash-table :test #'eql))
                                (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                                (*wal-pending-page-publication* nil) (*wal-pending-page-publication-tail* nil)
                                (*clog-pending-commits* (make-hash-table :test #'eql))
                                (registration (make-snapshot-registration))
                                (fired nil))
           (unwind-protect
                (let ((*wal-test-after-snapshot-boundary-registration-hook*
                        (lambda (token)
                          (setf fired t)
                          (release-snapshot-registration token)
                          (when reuse-p (make-snapshot-under-xid-lock 8 :register token)))))
                  (let ((condition
                          (handler-case
                              (with-write-lock
                                (wal-capture-snapshot-boundary-under-writer-lock 7 registration)
                                nil)
                            (error (condition) condition))))
                    (rove:ok fired)
                    (rove:ok (and condition
                                  (search "snapshot registration was released"
                                          (princ-to-string condition)))
                             "the stale capture fails at registration validation")
                    (rove:ok (null (gethash registration *wal-snapshot-retention-pins*))))
                  (rove:ok (null (snapshot-registration-wal-boundary registration))
                           "no stale frozen evidence is published"))
             (release-snapshot-registration registration))
           (rove:ok (zerop *active-snapshots*)))))))

(rove:deftest logical-slot-snapshot-capture-freezes-actual-on-interval
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (let* ((*backend* nil)
              (*active-snapshots* 0)
              (*active-snapshot-set* (make-hash-table :test #'eq))
              (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (registration (make-snapshot-registration)))
         (unwind-protect
              (let ((first-on (wal-report-logical-decoding-status t)))
                (with-write-lock
                  (wal-capture-snapshot-boundary-under-writer-lock 7 registration))
                (let ((captured (snapshot-registration-wal-boundary registration)))
                  (rove:ok (= first-on (snapshot-wal-boundary-logical-on-start-lsn captured)))
                  (rove:ok (= 1 (snapshot-wal-boundary-logical-on-start-timeline captured)))
                  (wal-report-logical-decoding-status nil)
                  (let ((next-on (wal-report-logical-decoding-status t)))
                    (rove:ok (> next-on first-on))
                    (rove:ok (= first-on (snapshot-wal-boundary-logical-on-start-lsn captured))
                             "later OFF/ON does not replace capture-time evidence")
                    (release-snapshot-registration registration)
                    (rove:ok (null (snapshot-registration-wal-boundary registration)))
                    (with-write-lock
                      (wal-capture-snapshot-boundary-under-writer-lock 8 registration))
                    (rove:ok (= next-on
                                (snapshot-wal-boundary-logical-on-start-lsn
                                 (snapshot-registration-wal-boundary registration)))
                             "reuse captures the new interval")
                    (sb-thread:with-mutex (*wal-lock*)
                      (release-snapshot-registration-wal-pin-under-wal-lock registration))
                    (rove:ok (null (snapshot-registration-wal-boundary registration))
                             "pin transfer clears the frozen evidence too"))))
           (release-snapshot-registration registration))))))

(rove:deftest logical-slot-snapshot-capture-freezes-fork-owners
    (dolist (pending-p '(nil t))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-ready* t)
                (*wal-insert-timeline* 2)
                (*active-snapshots* 0)
                (*active-snapshot-set* (make-hash-table :test #'eq))
                (*next-xid* 5000)
                (*active-xids* (when pending-p (list 4101)))
                (*wal-xid-first-lsn* (make-hash-table :test #'eql))
                (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                (*wal-pending-page-publication* nil)
                (*wal-pending-page-publication-tail* nil)
                (*clog-pending-commits* (make-hash-table :test #'eql))
                (registration (make-snapshot-registration)))
           (timeline-write-history 2 1 100 "snapshot owner test")
           (when pending-p (setf (gethash 4101 *wal-xid-first-lsn*) 100))
           (unwind-protect
                (progn
                  (with-write-lock
                    (wal-capture-snapshot-boundary-under-writer-lock 7 registration))
                  (let ((boundary (snapshot-registration-wal-boundary registration)))
                    (rove:ok (= 100 (snapshot-wal-boundary-lsn boundary)))
                    (rove:ok (= 1 (snapshot-wal-boundary-timeline boundary))
                             "consumed prefix at the fork belongs to the parent")
                    (rove:ok (= 100 (snapshot-wal-boundary-restart-lsn boundary)))
                    (rove:ok (= (if pending-p 2 1)
                                (snapshot-wal-boundary-restart-timeline boundary))
                             "a pending child start wins the equal-LSN tie")
                    (rove:ok (eq (if pending-p :record-start :prefix-end)
                                 (snapshot-wal-boundary-restart-role boundary)))
                    (rove:ok (null (snapshot-wal-boundary-logical-on-start-lsn boundary)))
                    (rove:ok (null (snapshot-wal-boundary-logical-on-start-timeline boundary)))))
             (release-snapshot-registration registration))
           (rove:ok (null (snapshot-registration-wal-boundary registration))
                    "registration release clears frozen evidence"))))))

(rove:deftest logical-slot-zero-snapshot-prefix-has-no-invented-owner
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-ready* t) (*wal-end-lsn* 0) (*wal-durable-lsn* 0)
                              (*wal-insert-timeline* 1)
                              (*active-snapshots* 0)
                              (*active-snapshot-set* (make-hash-table :test #'eq))
                              (*next-xid* 5000) (*active-xids* nil)
                              (*wal-xid-first-lsn* (make-hash-table :test #'eql))
                              (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                              (*wal-pending-page-publication* nil) (*wal-pending-page-publication-tail* nil)
                              (*clog-pending-commits* (make-hash-table :test #'eql))
                              (registration (make-snapshot-registration)))
         (unwind-protect
              (progn
                (with-write-lock
                  (wal-capture-snapshot-boundary-under-writer-lock 7 registration))
                (let ((boundary (snapshot-registration-wal-boundary registration)))
                  (rove:ok (zerop (snapshot-wal-boundary-lsn boundary)))
                  (rove:ok (null (snapshot-wal-boundary-timeline boundary)))
                  (rove:ok (null (snapshot-wal-boundary-restart-timeline boundary)))))
           (release-snapshot-registration registration))))))

(rove:deftest logical-slot-new-generation-is-fresh-and-full-width
    (let ((generations (loop repeat 32 collect (logical-slot-new-generation))))
      (rove:ok (= 32 (length (remove-duplicates generations :test #'=)))
               "separate creations mint separate generation identities")
      (dolist (generation generations)
        (rove:ok (typep generation '(unsigned-byte 128)))
        (rove:ok (= generation
                    (logical-slot-parse-generation
                     (logical-slot-generation-token generation)))))))

(rove:deftest logical-slot-retention-invalidation-uses-guarded-save
    (dolist (fail-p '(nil t))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((slot (unit-logical-provenance-slot))
                (fired nil)
                (marker (make-condition 'simple-error :format-control "invalidation save fault")))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot)
           (let ((*logical-test-failpoint-hook*
                   (lambda (point &rest ignored)
                     (declare (ignore ignored))
                     (when (and fail-p
                                (eq point :slot-file-after-directory-fsync-before-memory-publication))
                       (setf fired t)
                       (error marker)))))
             (rove:ok
              (eq (if fail-p marker :complete)
                  (handler-case
                      (progn
                        (sb-thread:with-mutex (*wal-lock*)
                          (wal-invalidate-replication-slots-matching-under-lock (constantly t)))
                        :complete)
                    (error (condition) condition)))))
           (rove:ok (eq fail-p fired))
           (rove:ok (eq fail-p (wal-replication-slot-state-uncertain-p slot)))
           (rove:ok (= 32 (wal-replication-slot-restart-lsn slot)))
           (rove:ok (eq (if fail-p :none :wal-removed)
                        (logical-slot-effective-invalidation-cause slot)))
           (rove:ok (eq (if fail-p :reserved :lost) (wal-replication-slot-wal-status slot)))
           (rove:ok (eq :wal-removed
                        (logical-slot-effective-invalidation-cause
                         (wal-read-replication-slot (wal-replication-slot-path "fixture"))))))))))

(rove:deftest logical-slot-guard-publishes-one-checked-candidate
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((slot (unit-logical-provenance-slot))
              (candidate (wal-copy-replication-slot-with-profile slot)))
         (wal-create-replication-slot-file slot)
         (setf (gethash "fixture" *wal-replication-slots*) slot
               (wal-replication-slot-confirmed-flush-lsn candidate) 90
               (wal-replication-slot-restart-lsn candidate) 48)
         (rove:ok (eq slot
                      (sb-thread:with-mutex (*wal-lock*)
                        (logical-slot-save-update-under-lock slot candidate))))
         (rove:ok (= 90 (wal-replication-slot-confirmed-flush-lsn slot)))
         (rove:ok (= 48 (wal-replication-slot-restart-lsn slot)))
         (rove:ok (eq (wal-replication-slot-pending-xids candidate)
                      (wal-replication-slot-pending-xids slot)))
         (rove:ok (not (wal-replication-slot-state-uncertain-p slot)))
         (rove:ok (equalp (wal-encode-replication-slot slot)
                          (wal-encode-replication-slot
                           (wal-read-replication-slot (wal-replication-slot-path "fixture")))))))))

(rove:deftest logical-slot-guard-rejects-identity-and-progress-changes-before-save
    (dolist (mode '(:stale :provenance :database :birth :plugin :profile :backward))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((slot (unit-logical-provenance-slot :plugin "pg_lisp"))
                (candidate (if (eq mode :provenance)
                               (unit-logical-provenance-slot :plugin "pg_lisp" :generation 1)
                               (wal-copy-replication-slot-with-profile slot)))
                (writes 0))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot)
           (ecase mode
             (:stale (setf (gethash "fixture" *wal-replication-slots*)
                           (wal-copy-replication-slot-with-profile slot)))
             (:provenance)
             (:database (setf (wal-replication-slot-database-oid candidate) 8))
             (:birth (setf (wal-replication-slot-consistent-lsn candidate) 66))
             (:plugin (setf (wal-replication-slot-plugin candidate) "pgoutput"))
             (:profile (setf (gethash candidate *wal-slot-profiles*) :binary-bytea))
             (:backward (setf (wal-replication-slot-confirmed-flush-lsn candidate) 79)))
           (let ((*logical-test-failpoint-hook*
                   (lambda (point &rest ignored)
                     (declare (ignore ignored))
                     (when (eq point :slot-file-before-write) (incf writes)))))
             (rove:ok (handler-case
                          (progn
                            (sb-thread:with-mutex (*wal-lock*)
                              (logical-slot-save-update-under-lock slot candidate))
                            nil)
                        (error () t))
                      (format nil "~a rejected" mode)))
           (rove:ok (zerop writes) "validation precedes every disk write")
           (rove:ok (not (wal-replication-slot-state-uncertain-p slot))
                    "pre-save validation does not fence a healthy slot"))))))

(rove:deftest logical-slot-guard-keeps-first-terminal-cause
    (dolist (cause '(:wal-removed :wal-level-insufficient))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((slot (unit-logical-provenance-slot :cause cause))
                (candidate (wal-copy-replication-slot-with-profile slot)))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot
                 (wal-replication-slot-invalidation-cause candidate)
                 (if (eq cause :wal-removed) :wal-level-insufficient :wal-removed))
           (rove:ok (handler-case
                        (progn
                          (sb-thread:with-mutex (*wal-lock*)
                            (logical-slot-save-update-under-lock slot candidate))
                          nil)
                      (error () t)))
           (rove:ok (eq cause (logical-slot-effective-invalidation-cause slot)))
           (rove:ok (not (wal-replication-slot-state-uncertain-p slot))))))))

(rove:deftest logical-slot-ambiguous-ack-save-fences-old-pin
    (dolist (exit-kind '(:error :throw))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((slot (unit-logical-provenance-slot))
                (old-pending (wal-replication-slot-pending-xids slot))
                (fired nil)
                (marker (make-condition 'simple-error :format-control "ambiguous slot save")))
           (wal-create-replication-slot-file slot)
           (setf (gethash "fixture" *wal-replication-slots*) slot)
           (let ((*logical-test-failpoint-hook*
                   (lambda (point &rest ignored)
                     (declare (ignore ignored))
                     (when (eq point :slot-file-after-directory-fsync-before-memory-publication)
                       (setf fired t)
                       (if (eq exit-kind :error) (error marker) (throw :slot-save-abort :aborted))))))
             (rove:ok
              (eq (if (eq exit-kind :error) marker :aborted)
                  (catch :slot-save-abort
                    (handler-case
                        (sb-thread:with-mutex (*wal-lock*)
                          (wal-persist-replication-slot-ack-under-lock slot 90))
                      (error (condition) condition))))
              "the intended error or nonlocal exit escapes"))
           (rove:ok fired "fault reached the durable publication boundary")
           (rove:ok (= 90 (wal-replication-slot-confirmed-flush-lsn
                           (wal-read-replication-slot (wal-replication-slot-path "fixture"))))
                    "new image can be durable despite the failed call")
           (rove:ok (= 80 (wal-replication-slot-confirmed-flush-lsn slot)))
           (rove:ok (= 32 (wal-replication-slot-restart-lsn slot)) "old retention pin remains")
           (rove:ok (eq old-pending (wal-replication-slot-pending-xids slot)))
           (rove:ok (wal-replication-slot-state-uncertain-p slot))
           (rove:ok (handler-case (progn (wal-require-usable-replication-slot slot) nil)
                      (error () t)) "uncertain slot rejects further use")
           (let* ((owner (gensym "FENCED-SLOT-OWNER-"))
                  (*wal-logical-slot-owner* owner))
             (setf (wal-replication-slot-active-owner slot) owner)
             (dolist (operation
                      (list (lambda () (wal-acknowledge-replication-slot "fixture" 90 owner))
                            (lambda () (wal-save-replication-slot slot))
                            (lambda ()
                              (call-with-wal-retained-slot-scan
                               "fixture" 80 100 (lambda (scan) (declare (ignore scan)) t)))))
               (rove:ok (handler-case (progn (funcall operation) nil)
                          (pg-error (condition) (string= "55000" (pg-error-sqlstate condition))))
                        "ACK, save and retained scan all reject uncertain state")))
           (rove:ok
            (handler-case
                (progn
                  (sb-thread:with-mutex (*wal-lock*)
                    (wal-slot-removal-segment
                     10 10 (* 10 +wal-segment-size+)
                     (make-wal-slot-retention-policy :mode :segments :limit 0)))
                  nil)
              (error () t))
            "a retention cap cannot bypass an uncertain slot"))))))

(defun unit-logical-slot-startup-identity ()
  ;; Exercise identity preparation without granting startup publication authority.
  (let ((slots (slot-state-map (wal-replication-slot-directory) #'wal-decode-replication-slot)))
    (values (logical-slot-check-startup-identities slots) slots)))

(rove:deftest logical-slot-startup-reads-durable-catalog-before-catalog-init
    (dolist (known-p '(t nil))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-control-metadata* nil)
                (*catalog-identity-needs-persist-p* nil)
                (*catalog-logical-event-publication-p* nil)
                (*catalog* (make-catalog :cluster-identity
                                         (when known-p "logical-slot-startup-cluster")))
                (system-id (when known-p (catalog-system-identifier)))
                (original *wal-replication-slots*))
           (ensure-directories-exist (catalog-path))
           (write-catalog-image (catalog-image-bytes))
           (wal-create-replication-slot-file
            (unit-logical-provenance-slot :system-id (or system-id 42) :cause :wal-removed))
           (let ((before (uiop:read-file-string (catalog-path)))
                 (*catalog* nil))
             (rove:ok (eq known-p
                          (handler-case (progn (unit-logical-slot-startup-identity) t)
                            (error () nil)))
                      "a durable identity is required; a legacy catalog cannot invent one")
             (rove:ok (null *catalog*) "startup validation does not initialize the live catalog")
             (rove:ok (null *catalog-identity-needs-persist-p*)
                      "validation does not leak a catalog repair request")
             (rove:ok (string= before (uiop:read-file-string (catalog-path)))
                      "durable catalog is never rewritten")
             (if known-p
                 (rove:ok (eq original *wal-replication-slots*) "pure preparation does not publish a registry")
                 (rove:ok (eq original *wal-replication-slots*)))))))))

(rove:deftest logical-slot-startup-rejects-control-identity-mismatch
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-control-metadata* (list :system-id 43))
              (original *wal-replication-slots*)
              (reader (symbol-function 'physical-slot-startup-system-id)))
         (wal-create-replication-slot-file
          (unit-logical-provenance-slot :cause :wal-removed))
         (unwind-protect
              (progn
                (setf (symbol-function 'physical-slot-startup-system-id) (lambda () 42))
                (rove:ok (handler-case (progn (unit-logical-slot-startup-identity) nil) (error () t)))
                (rove:ok (eq original *wal-replication-slots*)))
           (setf (symbol-function 'physical-slot-startup-system-id) reader))))))

(rove:deftest logical-slot-startup-does-not-invent-catalog-identity
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let ((*wal-control-metadata* nil)
             (original *wal-replication-slots*))
         (wal-create-replication-slot-file
          (unit-logical-provenance-slot :cause :wal-removed))
         (rove:ok (null (probe-file (catalog-path))))
         (rove:ok (handler-case (progn (unit-logical-slot-startup-identity) nil) (error () t)))
         (rove:ok (null (probe-file (catalog-path))) "validation never creates a catalog")
         (rove:ok (eq original *wal-replication-slots*))))))

(rove:deftest logical-slot-startup-retains-unknown-legacy-provenance
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-control-metadata* nil)
              (reader (symbol-function 'physical-slot-startup-system-id)))
         (wal-create-replication-slot-file
          (unit-logical-provenance-slot :provenance nil :cause :wal-removed))
         (unwind-protect
              (progn
                (setf (symbol-function 'physical-slot-startup-system-id)
                      (lambda () (error "legacy identity must remain unknown")))
                (wal-load-replication-slots)
                (rove:ok (= 1 (hash-table-count *wal-replication-slots*)))
                (rove:ok (null (wal-replication-slot-logical-provenance
                                (gethash "fixture" *wal-replication-slots*)))))
           (setf (symbol-function 'physical-slot-startup-system-id) reader))))))

(rove:deftest logical-slot-startup-rejects-foreign-provenance
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*wal-control-metadata* nil)
              (original *wal-replication-slots*)
              (slot (unit-logical-provenance-slot :system-id 43 :cause :wal-removed))
              (reader (symbol-function 'physical-slot-startup-system-id))
              (reads 0))
         (wal-create-replication-slot-file slot)
         (unwind-protect
              (progn
                (setf (symbol-function 'physical-slot-startup-system-id)
                      (lambda () (incf reads) 42))
                (rove:ok (handler-case (progn (unit-logical-slot-startup-identity) nil)
                           (error () t))
                         "foreign provenance prevents startup")
                (rove:ok (eq original *wal-replication-slots*)
                         "rejected inventory never replaces the live registry")
                (rove:ok (= reads 1) "read durable identity before publication"))
           (setf (symbol-function 'physical-slot-startup-system-id) reader))))))

(rove:deftest logical-slot-startup-checks-known-identity-once
    (dolist (system-id '(0 42))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*wal-control-metadata* nil)
                (reader (symbol-function 'physical-slot-startup-system-id))
                (reads 0))
           (dolist (name '("a_known" "z_known"))
             (let ((slot (unit-logical-provenance-slot :system-id system-id :cause :wal-removed)))
               (setf (wal-replication-slot-name slot) name)
               (wal-create-replication-slot-file slot)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'physical-slot-startup-system-id)
                        (lambda () (incf reads) system-id))
                  (multiple-value-bind (checked slots) (unit-logical-slot-startup-identity)
                    (rove:ok (eql system-id checked))
                    (rove:ok (= 2 (length slots))))
                  (rove:ok (= reads 1) "one catalog read for a known-provenance inventory"))
             (setf (symbol-function 'physical-slot-startup-system-id) reader)))))))

(defun unit-logical-slot-state-fields-image (fields)
  (let ((prefix (format nil "~{~a~^ ~}" fields)))
    (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))

(rove:deftest logical-slot-state-known-provenance-golden
    (let* ((slot (unit-logical-provenance-slot))
           (bytes (wal-encode-replication-slot slot))
           (decoded (wal-decode-replication-slot-image bytes "fixture")))
      (rove:ok (string= (format nil "PGSLOT6 fixture 7 32 65 80 pgoutput text 42 00000000000000000000000000000000 1 65 1 1 1 none B6AA5E5B~%")
                        (bytes->string bytes)))
      (rove:ok (equalp bytes (wal-encode-replication-slot decoded)))
      (rove:ok (zerop (logical-slot-provenance-generation
                       (wal-replication-slot-logical-provenance decoded))))
      (rove:ok (= 1 (wal-replication-slot-restart-timeline decoded)))
      (rove:ok (= 1 (wal-replication-slot-confirmed-timeline decoded)))
      (rove:ok (eq :none (logical-slot-effective-invalidation-cause decoded)))))

(rove:deftest logical-slot-state-legacy-cause-does-not-invent-provenance
    (let* ((slot (unit-logical-provenance-slot :provenance nil :cause :wal-level-insufficient))
           (bytes (wal-encode-replication-slot slot))
           (decoded (wal-decode-replication-slot-image bytes "fixture")))
      (rove:ok (string= (format nil "PGSLOT6 fixture 7 32 65 80 pgoutput text - - - - - - - wal_level_insufficient EF61B905~%")
                        (bytes->string bytes)))
      (rove:ok (null (wal-replication-slot-logical-provenance decoded)))
      (rove:ok (null (wal-replication-slot-restart-timeline decoded)))
      (rove:ok (null (wal-replication-slot-confirmed-timeline decoded)))
      (rove:ok (= 32 (wal-replication-slot-restart-lsn decoded)))
      (rove:ok (eq :lost (wal-replication-slot-wal-status decoded)))
      (rove:ok (eq :wal-level-insufficient (logical-slot-effective-invalidation-cause decoded)))
      (rove:ok (equalp bytes (wal-encode-replication-slot decoded)))))

(rove:deftest logical-slot-state-legacy-save-bytes-stay-unchanged
    (dolist (case '((nil :reserved "reserved" "78E66DEF")
                    (:wal-removed :lost "lost" "113AABBE")))
      (let ((slot (unit-logical-provenance-slot :provenance nil :cause (first case))))
        (setf (wal-replication-slot-wal-status slot) (second case))
        (rove:ok (string= (format nil "PGSLOT4 fixture 7 32 65 80 ~a pgoutput ~a~%"
                                  (third case) (fourth case))
                          (bytes->string (wal-encode-replication-slot slot)))))))

(rove:deftest logical-slot-state-unknown-on-and-binary-profile-roundtrip
    (dolist (profile '(:text :binary-bytea))
      (let* ((slot (unit-logical-provenance-slot :known-on nil :plugin "pg_lisp" :profile profile))
             (bytes (wal-encode-replication-slot slot))
             (decoded (wal-decode-replication-slot-image bytes "fixture"))
             (provenance (wal-replication-slot-logical-provenance decoded)))
        (rove:ok (eq profile (wal-slot-profile decoded)))
        (rove:ok (null (logical-slot-provenance-on-start-lsn provenance)))
        (rove:ok (null (logical-slot-provenance-on-start-timeline provenance)))
        (rove:ok (equalp bytes (wal-encode-replication-slot decoded))))))

(rove:deftest logical-slot-state-rejects-corrupt-and-partial-fields
    (let* ((bytes (wal-encode-replication-slot (unit-logical-provenance-slot)))
           (fields (butlast (wal-split-fields (string-right-trim '(#\Newline) (bytes->string bytes))))))
      (dotimes (i (length bytes))
        (let ((bad (copy-seq bytes)))
          (setf (aref bad i) (logxor 1 (aref bad i)))
          (rove:ok (rove:signals (wal-decode-replication-slot-image bad "fixture") 'error))))
      (dolist (item '((2 "0") (3 "032") (3 "+32") (4 "81") (7 "binary-bytea")
                      (8 "-") (9 "-") (9 "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                      (10 "0") (10 "4294967296") (11 "66") (12 "2")
                      (13 "-") (14 "-") (15 "unknown")))
        (let ((bad (copy-list fields)))
          (setf (nth (first item) bad) (second item))
          (rove:ok (rove:signals
                    (wal-decode-replication-slot-image (unit-logical-slot-state-fields-image bad) "fixture")
                    'error))))
      (rove:ok (rove:signals (wal-decode-replication-slot-image bytes "other") 'error))
      (rove:ok (rove:signals (wal-decode-replication-slot-image (subseq bytes 0 (1- (length bytes))) "fixture") 'error))))

(cl-spec:defproperty logical-slot-state-detached-preserves-ahead-positions
    ((end (range integer 80 18446744073709551615))
     (generation (range integer 0 340282366920938463463374607431768211455))
     (timeline (range integer 1 4294967295)))
  (:about wal-encode-replication-slot wal-decode-replication-slot-image)
  (:trials (:normal 100))
  (let* ((slot (unit-logical-provenance-slot :end end :generation generation :timeline timeline))
         (bytes (wal-encode-replication-slot slot))
         (decoded (wal-decode-replication-slot-image bytes "fixture"))
         (*wal-durable-lsn* 0))
    (and (equalp bytes (wal-encode-replication-slot decoded))
         (= end (wal-replication-slot-confirmed-flush-lsn decoded))
         (= generation (logical-slot-provenance-generation
                        (wal-replication-slot-logical-provenance decoded)))
         (handler-case (progn (wal-decode-replication-slot bytes "fixture") nil)
           (error () t)))))

(rove:deftest logical-slot-state-detached-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-state-detached-preserves-ahead-positions :seed 249))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 100 (getf data :trials)))))

(rove:deftest logical-slot-state-restart-start-can-follow-confirmed-timeline-at-fork
    (let ((slot (unit-logical-provenance-slot :end 128)))
      (setf (wal-replication-slot-restart-lsn slot) 128
            (wal-replication-slot-restart-timeline slot) 2)
      (let* ((bytes (wal-encode-replication-slot slot))
             (decoded (wal-decode-replication-slot-image bytes "fixture")))
        (rove:ok (= 128 (wal-replication-slot-restart-lsn decoded)))
        (rove:ok (= 2 (wal-replication-slot-restart-timeline decoded)))
        (rove:ok (= 1 (wal-replication-slot-confirmed-timeline decoded)))
        (rove:ok (equalp bytes (wal-encode-replication-slot decoded))))))

(rove:deftest logical-slot-state-unconfirmed-private-copy-roundtrip
    (dolist (profile '(:text :binary-bytea))
      (dolist (cause '(:none :wal-removed :wal-level-insufficient))
        (let ((slot (unit-logical-provenance-slot :plugin "pg_lisp" :profile profile :cause cause)))
          (setf (wal-replication-slot-confirmed-flush-lsn slot) nil
                (wal-replication-slot-confirmed-timeline slot) nil)
          (let* ((bytes (wal-encode-replication-slot slot))
                 (decoded (wal-decode-replication-slot-image bytes "fixture")))
            (rove:ok (null (wal-replication-slot-confirmed-flush-lsn decoded)))
            (rove:ok (null (wal-replication-slot-confirmed-timeline decoded)))
            (rove:ok (eq cause (logical-slot-effective-invalidation-cause decoded)))
            (rove:ok (eq profile (wal-slot-profile decoded)))
            (rove:ok (equalp bytes (wal-encode-replication-slot decoded))))))))

(rove:deftest logical-slot-state-rejects-cause-mirror-and-kind-mismatch
    (let ((slot (unit-logical-provenance-slot)))
      (setf (wal-replication-slot-invalidation-cause slot) nil)
      (rove:ok (rove:signals (wal-encode-replication-slot slot) 'error)))
  (let ((slot (unit-logical-provenance-slot :cause :wal-level-insufficient)))
    (setf (wal-replication-slot-wal-status slot) :reserved)
    (rove:ok (rove:signals (wal-encode-replication-slot slot) 'error)))
  (let ((slot (make-wal-replication-slot :name "physical" :kind :physical :plugin nil
                                         :system-id 42 :restart-lsn 32 :restart-timeline 1)))
    (rove:ok (physical-slot-encode slot))
    (setf (wal-replication-slot-invalidation-cause slot) :none)
    (rove:ok (rove:signals (physical-slot-encode slot) 'error))
    (setf (wal-replication-slot-invalidation-cause slot) nil
          (wal-replication-slot-confirmed-timeline slot) 1)
    (rove:ok (rove:signals (physical-slot-encode slot) 'error)))
  (let ((slot (make-wal-replication-slot
               :name "physical" :kind :physical :plugin nil :system-id 42
               :restart-lsn 32 :restart-timeline 1
               :logical-provenance (make-logical-slot-provenance 42 0 65 1))))
    (rove:ok (rove:signals (physical-slot-encode slot) 'error))))

(rove:deftest logical-slot-state-zero-identity-values-remain-present
    (let* ((slot (unit-logical-provenance-slot :system-id 0 :generation 0))
           (bytes (wal-encode-replication-slot slot))
           (fields (wal-split-fields (string-right-trim '(#\Newline) (bytes->string bytes))))
           (decoded (wal-decode-replication-slot-image bytes "fixture"))
           (provenance (wal-replication-slot-logical-provenance decoded)))
      (rove:ok (string= "0" (nth 8 fields)))
      (rove:ok (string= "00000000000000000000000000000000" (nth 9 fields)))
      (rove:ok provenance)
      (rove:ok (zerop (logical-slot-provenance-system-id provenance)))
      (rove:ok (zerop (logical-slot-provenance-generation provenance)))
      (rove:ok (equalp bytes (wal-encode-replication-slot decoded)))))
