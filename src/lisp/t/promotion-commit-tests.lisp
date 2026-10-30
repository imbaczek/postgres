;;;; A child checkpoint bridges pending recovery to restartable primary control.
(in-package #:pg-physical-tests)

(defun sample-promotion-checkpoint-state (fork &key (bound 1002) (next 50) (child 2))
  (list :phase 3 :applied fork :timeline 1 :promotion-child child :next-xid next :state-time 1793030000
        :source-reservation (list :version 1 :through fork :bound bound :complete t)))

(deftest child-checkpoint-plan-preserves-reservation-and-native-rotation
    (dolist (fork (list 216 (- pg::+wal-segment-size+ 208) (- pg::+wal-segment-size+ 207)
                        (- pg::+wal-segment-size+ 56) (- pg::+wal-segment-size+ 55)
                        pg::+wal-segment-size+))
      (let* ((state (sample-promotion-checkpoint-state fork))
             (plan (pg::physical-promotion-checkpoint-plan state 20000 1003))
             (snapshot (getf plan :snapshot)) (frames (getf plan :frames)))
        (ok (= 1003 (getf snapshot :next-xid)))
        (ok (= 1002 (getf snapshot :xid-reserved-through)))
        (ok (= 2 (getf snapshot :timeline))) (ok (= 1 (getf snapshot :prev-timeline)))
        (ok (= 20000 (getf snapshot :next-oid)))
        (ok (= 3 (length frames)))
        (loop with previous = fork
              for (start bytes) in frames
              do (ok (<= previous start))
                 (ok (<= (+ (mod start pg::+wal-segment-size+) (length bytes)) pg::+wal-segment-size+))
                 (ok (= start (pg::wal-record-start (pg::wal-decode-record bytes start))))
                 (setf previous (+ start (length bytes))))
        (ok (equal snapshot (pg::wal-checkpoint-record-snapshot
                             (pg::wal-decode-record (second (third frames)) (first (third frames)))))))))

(deftest promotion-resource-pause-restores-workers-without-fencing-recovery
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let ((attempt (symbol-function 'pg::physical-promotion-try-online))
             (restore (symbol-function 'pg::physical-promotion-resume-recovery-workers))
             (pg::*physical-resource-pause* nil)
             (restored nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-promotion-try-online)
                      (lambda ()
                        (setf pg::*physical-resource-pause*
                              (list :fact nil :shortfalls nil :resume-requested nil))
                        :resource-paused)
                      (symbol-function 'pg::physical-promotion-resume-recovery-workers)
                      (lambda (config paused restartpoint)
                        (declare (ignore config paused restartpoint))
                        (ok pg::*physical-resource-pause*)
                        (setf restored t pg::*physical-promotion-stopping-p* nil)))
                (call-with-current-promotion-owner
                 (lambda ()
                   (ok (eq :resource-paused (pg::physical-recovery-promote)))))
                (ok restored)
                (ok (not pg::*physical-recovery-failed-p*))
                (ok (not pg::*physical-promotion-stopping-p*))
                (ok (= 2 (getf pg::*physical-recovery-state* :phase))))
           (setf (symbol-function 'pg::physical-promotion-try-online) attempt
                 (symbol-function 'pg::physical-promotion-resume-recovery-workers) restore))))))

(deftest pending-intent-encodes-and-owns-its-known-resource-fact
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (multiple-value-bind (metadata catalog prefix generation)
          (pg::physical-restartpoint-read pg::*physical-recovery-state*)
        (declare (ignore catalog))
        (let* ((metadata (copy-tree metadata))
               (fact (list :resource-parameters (list 100 8 10 0 64)
                           :resource-lsn 96 :resource-timeline 1)))
          (setf (getf metadata :source-resource-fact) fact
                (getf metadata :source-checkpoint)
                (append (getf metadata :source-checkpoint) (copy-tree fact)))
          (multiple-value-bind (bytes pending)
              (pg::physical-promotion-make-pending
               pg::*physical-recovery-state* metadata prefix generation :legacy 2 1793030000 nil)
            (let* ((control (pg::physical-recovery-decode-control bytes))
                   (restored (pg::physical-promotion-frozen-resource-fact control metadata))
                   (before (pg::physical-promotion-checkpoint-plan pending 20000 1003))
                   (cold (copy-tree pending)))
              (ok (equal fact (getf control :resource-fact)))
              (setf (getf cold :source-resource-fact) restored)
              (ok (equalp (getf before :frames)
                          (getf (pg::physical-promotion-checkpoint-plan cold 20000 1003) :frames)))
              (setf (first (getf fact :resource-parameters)) 50
                    (first (getf (getf metadata :source-checkpoint) :resource-parameters)) 60)
              (ok (= 100 (first (getf (getf pending :source-resource-fact) :resource-parameters))))
              (ok (= 100 (first (getf (getf pending :required-resource-fact) :resource-parameters))))
              (ok (= 100 (first (getf (getf pending :source-checkpoint) :resource-parameters))))
              (setf (first (getf (getf pending :source-resource-fact) :resource-parameters)) 70)
              (ok (= 100 (first (getf (getf pending :required-resource-fact) :resource-parameters))))))))))

(deftest pending-resource-fact-is-bound-to-the-selected-generation
    (let* ((fact (list :resource-parameters (list 100 8 10 0 64)
                       :resource-lsn 192 :resource-timeline 1))
           (control (list :resource-fact fact))
           (metadata (list :source-resource-fact (copy-tree fact)))
           (restored (pg::physical-promotion-frozen-resource-fact control metadata)))
      (ok (equal fact restored))
      (setf (first (getf restored :resource-parameters)) 50)
      (ok (= 100 (first (getf fact :resource-parameters))))
      (ok (equal fact (pg::physical-promotion-frozen-resource-fact nil metadata)))
      (ok (null (pg::physical-promotion-frozen-resource-fact nil nil)))
      (ok (intake-fails-p
           (lambda () (pg::physical-promotion-frozen-resource-fact control nil))))
      (setf (first (getf (getf metadata :source-resource-fact) :resource-parameters)) 50)
      (ok (intake-fails-p
           (lambda () (pg::physical-promotion-frozen-resource-fact control metadata))))))

(deftest child-checkpoint-plan-preserves-frozen-parent-resources
    (dolist (fork (list 216 (- pg::+wal-segment-size+ 256)
                        (- pg::+wal-segment-size+ 255)))
      (let* ((fact (list :resource-parameters (list 100 8 10 0 64)
                         :resource-lsn 192 :resource-timeline 1))
             (state (sample-promotion-checkpoint-state fork)))
        (setf (getf state :source-resource-fact) fact)
        (let* ((plan (pg::physical-promotion-checkpoint-plan state 20000 1003))
               (snapshot (getf plan :snapshot))
               (frame (third (getf plan :frames))))
          (ok (equal fact (pg::wal-copy-control-resource-fields snapshot)))
          (ok (= 168 (length (second frame))))
          (ok (equal snapshot
                     (pg::wal-checkpoint-record-snapshot
                      (pg::wal-decode-record (second frame) (first frame)))))
          (ok (<= (+ (mod (first frame) pg::+wal-segment-size+)
                     (length (second frame))) pg::+wal-segment-size+))
          (setf (first (getf fact :resource-parameters)) 999)
          (ok (= 100 (first (getf snapshot :resource-parameters))))))))

(deftest child-checkpoint-plan-preserves-exhaustion-and-rejects-overflow
    (let* ((state (sample-promotion-checkpoint-state 216 :bound #xffffffffffffffff
                                                         :next #x10000000000000000))
           (plan (pg::physical-promotion-checkpoint-plan state 20000 #x10000000000000000)))
      (ok (= #x10000000000000000 (getf (getf plan :snapshot) :next-xid))))
  (ok (intake-fails-p
       (lambda () (pg::physical-promotion-checkpoint-plan
                   (sample-promotion-checkpoint-state #xffffffffffffffc0) 20000 1003)))))

(defun call-with-current-promotion-owner (function)
  (let ((pg::*postmaster-data-lock* nil)
        (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
        (pg::*postmaster-start-time* nil) (pg::*postmaster-start-timestamp* nil)
        (pg::*postmaster-pid-host* nil) (pg::*postmaster-pid-port* nil)
        (pg::*postmaster-pid-inode* nil)
        (saved-exit-hooks sb-ext:*exit-hooks*))
    (sb-posix:chmod (namestring pg::*pgdata*) #o700)
    (unwind-protect
         (progn
           (pg::postmaster-own-data-directory "127.0.0.1" 5439)
           (funcall function))
      (pg::postmaster-remove-pid-file)
      (setf sb-ext:*exit-hooks* saved-exit-hooks)
      (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*)))))

(deftest live-child-floor-does-not-skip-an-unfrozen-cycle
    (let ((cycle pg::+xid-cycle+) (limit pg::+max-xid-one-past+))
      (ok (pg::physical-promotion-live-xid-floor-valid-p 50 1003 50 1003))
      (ok (pg::physical-promotion-live-xid-floor-valid-p 50 (+ cycle 3) 50 (+ cycle 3)))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 (+ cycle 4) 50 (+ cycle 4))))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 (+ (* 2 cycle) 3) 50 50)))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 49 50 50)))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 1003 2003 1003)))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 1003 50 2003)))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 cycle 50 50)))
      (ok (pg::physical-promotion-live-xid-floor-valid-p (1- limit) limit (1- limit) limit))
      (ok (pg::physical-promotion-live-xid-floor-valid-p limit limit limit limit))
      (ok (not (pg::physical-promotion-live-xid-floor-valid-p 50 limit 50 limit)))))

(cl-spec:defproperty live-child-floor-stays-before-reuse-or-at-next-epoch-gate
    ((epoch (range integer 0 1000))
     (low (range integer 3 4294967295))
     (offset (range integer 0 3)))
  (:about pg::physical-promotion-live-xid-floor-valid-p)
  (:trials (:normal 200))
  (let* ((actual (+ (* epoch pg::+xid-cycle+) low))
         (next-epoch (+ (* (1+ epoch) pg::+xid-cycle+) 3))
         (candidate (+ next-epoch offset)))
    (eql (zerop offset)
         (not (null (pg::physical-promotion-live-xid-floor-valid-p
                     actual candidate actual candidate))))))

(deftest generated-live-child-floor-epoch-gate
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'live-child-floor-stays-before-reuse-or-at-next-epoch-gate :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun call-with-live-child-fixture (function &key (local-floor 1003) (committed-child t))
  (with-promotion-fixture (root store :system-id
                                (pg::catalog-system-identifier
                                 (pg::make-catalog :cluster-identity "history-source")))
    (let* ((pg::*catalog-identity-needs-persist-p* nil)
           (pg::*catalog* (pg::load-catalog))
           (pg::*active-xids* nil) (pg::*active-snapshots* 0)
           (pg::*active-snapshot-set* (make-hash-table :test #'eq))
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-page-cache* (make-hash-table :test #'equal))
           (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (pg::*wal-replication-slots* (make-hash-table :test #'equal))
           (pg::*physical-source-pins* (make-hash-table :test #'equal))
           (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
           (pg::*wal-new-segments* nil) (pg::*wal-pending-deletes* nil)
           (pg::*wal-pending-btree-split* nil) (pg::*wal-page-batch* nil)
           (pg::*wal-page-batch-xids* nil) (pg::*wal-page-batch-split* nil)
           (pg::*wal-control-metadata* nil) (pg::*wal-checkpoint-snapshot* nil)
           (pg::*wal-xid-reserved-through* nil) (pg::*wal-prev-timeline* 1)
           (pg::*wal-poisoned* nil) (pg::*xid-wrap-pending* nil)
           (pg::*xid-wrap-failure* nil) (pg::*xid-wrap-completed-through* nil)
           (pg::*backend* (pg::make-backend :state :in-progress :explicit t
                                            :read-only t :isolation-level :repeatable-read)))
      (pg::clog-init)
      (unwind-protect
           (progn
             (let ((pg::*xid-batch-end* local-floor)) (pg::save-next-xid))
             (when committed-child
               (prepare-promotion-child)
               (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control))
             (funcall function root store))
        (pg::reset-backend-transaction pg::*backend*)
        (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))))

(defun call-with-promotion-reader-fixture (function)
  (call-with-live-child-fixture
   (lambda (root store)
     (declare (ignore root store))
     (let ((pg::*backend-registry* (make-hash-table :test #'eql))
           (pg::*shutdown-abandoned-backends* nil)
           (pg::*recovery-reader-xid-counter* 0))
       (setf (gethash 1 pg::*backend-registry*) pg::*backend*)
       (call-with-promotion-exclusion function)))
   :committed-child nil))

(deftest promotion-enrolls-old-readers-before-changing-role
    (call-with-promotion-reader-fixture
     (lambda ()
       (let* ((backend pg::*backend*)
              (snapshot (pg::ensure-transaction-snapshot backend 1))
              (old-id (pg::backend-virtual-reader-xid backend))
              (abandoned (pg::make-backend :state :in-progress :read-only nil))
              (idle (pg::make-backend :state :idle)))
         (remhash old-id pg::*recovery-reader-xids*)
         (setf (pg::backend-read-only backend) nil
               (gethash 2 pg::*backend-registry*) idle
               pg::*shutdown-abandoned-backends* (list abandoned backend))
         (pg::physical-promotion-enroll-readers)
         (ok (= old-id (pg::backend-virtual-reader-xid backend)))
         (ok (gethash old-id pg::*recovery-reader-xids*))
         (ok (eq snapshot (pg::backend-transaction-snapshot backend)))
         (ok (pg::backend-read-only backend))
         (ok (pg::backend-read-only abandoned))
         (ok (minusp (pg::backend-virtual-reader-xid abandoned)))
         (ok (/= old-id (pg::backend-virtual-reader-xid abandoned)))
         (ok (null (pg::backend-virtual-reader-xid idle)))
         (ok (= 2 (hash-table-count pg::*recovery-reader-xids*)))
         (pg::reset-backend-transaction abandoned)))))

(deftest promotion-enrolls-snapshot-only-readers-without-reusing-cached-identities
    (call-with-promotion-reader-fixture
     (lambda ()
       (let* ((backend pg::*backend*)
              (snapshot (pg::ensure-transaction-snapshot backend 1))
              (old (pg::make-backend :state :in-progress :virtual-reader-xid -100)))
         (pg::release-xid (pg::backend-virtual-reader-xid backend) backend)
         (setf (gethash 2 pg::*backend-registry*) old)
         (pg::physical-promotion-enroll-readers)
         (ok (eq snapshot (pg::backend-transaction-snapshot backend)))
         (ok (< (pg::backend-virtual-reader-xid backend) -100))
         (ok (= -100 (pg::backend-virtual-reader-xid old)))
         (ok (gethash -100 pg::*recovery-reader-xids*))
         (pg::reset-backend-transaction old)))))

(deftest promotion-reader-enrollment-rejects-unowned-or-invalid-resources
    (dolist (kind '(:orphan :relation-reader :relation-writer :duplicate :real-xid :active-xid
                    :idle-owner :invalid-id :snapshot-owner :snapshot-registration))
      (call-with-promotion-reader-fixture
       (lambda ()
         (let ((backend pg::*backend*))
           (case kind
             (:orphan (setf (gethash -90 pg::*recovery-reader-xids*) t))
             (:relation-reader (setf (gethash 900 pg::*relation-readers*) '(-90)))
             (:relation-writer (setf (gethash 900 pg::*relation-writers*) '(-90)))
             (:snapshot-owner
              (setf (gethash (pg::make-snapshot-struct :xmin 3 :xmax 50)
                             pg::*active-snapshot-set*) t
                    pg::*active-snapshots* 1))
             (:snapshot-registration
              (pg::ensure-transaction-snapshot backend 1)
              (setf (pg::snapshot-registration-active-p
                     (pg::backend-transaction-snapshot-registration backend)) nil))
             (:duplicate
              (setf (pg::backend-virtual-reader-xid backend) -80
                    (gethash 2 pg::*backend-registry*)
                    (pg::make-backend :state :in-progress :virtual-reader-xid -80)))
             (:real-xid (setf (pg::backend-xid backend) 42))
             (:active-xid (setf pg::*active-xids* '(42)))
             (:idle-owner
              (setf (pg::backend-state backend) :idle
                    (pg::backend-virtual-reader-xid backend) -80))
             (:invalid-id (setf (pg::backend-virtual-reader-xid backend) 80)))
           (unwind-protect
                (ok (intake-fails-p #'pg::physical-promotion-enroll-readers)
                    (format nil "~a" kind))
             (when (eq kind :snapshot-registration)
               (setf (pg::snapshot-registration-active-p
                      (pg::backend-transaction-snapshot-registration backend)) t))
             (setf (pg::backend-xid backend) nil
                   (pg::backend-virtual-reader-xid backend) nil)))))))

(deftest promotion-reader-enrollment-requires-owned-closed-admission
    (ok (intake-fails-p #'pg::physical-promotion-enroll-readers)))

(deftest online-promotion-audits-orphan-readers-before-unrestricted-replay
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root))
       (let ((pg::*backend-registry* (make-hash-table :test #'eql))
             (pg::*shutdown-abandoned-backends* nil))
         (setf (pg::backend-state pg::*backend*) :idle
               (gethash -90 pg::*recovery-reader-xids*) t
               (gethash 900 pg::*relation-readers*) '(-90))
         (let* ((start (pg::physical-store-written-lsn store))
                (bytes (pg::wal-encode-record pg::+wal-record-batch-start+ 0 start #()))
                (terminal (+ start (length bytes))))
           (pg::physical-store-input store start bytes)
           (pg::physical-store-input
            store terminal
            (pg::wal-encode-record pg::+wal-record-maintenance+ 0 terminal #()))
           (pg::physical-store-flush store))
         (call-with-current-promotion-owner
          (lambda ()
            (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
              (let ((message (handler-case
                                 (progn (pg::physical-promotion-try-online) nil)
                               (error (condition) (princ-to-string condition)))))
                (ok (and message (search "promotion reader registry has no backend owner" message)))))))
         (ok (= 216 pg::*last-replay-lsn*))
         (ok (eql 2 (getf pg::*physical-recovery-state* :phase)))))
     :committed-child nil))

(deftest promotion-reader-backlog-defers-complete-barriers-but-not-unfinished-tails
    (dolist (complete '(nil t))
      (call-with-promotion-reader-fixture
       (lambda ()
         (let* ((store pg::*physical-receive-store*)
                (start (pg::physical-store-written-lsn store))
                (bytes (pg::wal-encode-record pg::+wal-record-batch-start+ 0 start #())))
           (pg::ensure-transaction-snapshot pg::*backend* 1)
           (pg::physical-store-input store start bytes)
           (when complete
             (let ((terminal (+ start (length bytes))))
               (pg::physical-store-input
                store terminal
                (pg::wal-encode-record pg::+wal-record-maintenance+ 0 terminal #()))))
           (ok (eql (not complete) (pg::call-with-physical-promotion-readers #'pg::physical-promotion-replay-backlog)))
           (ok (= 216 pg::*last-replay-lsn*))
           (ok (eql 2 (getf pg::*physical-recovery-state* :phase)))
           (ok (not pg::*physical-recovery-failed-p*)))))))

(deftest live-promotion-completes-with-readers-enrolled-before-intent
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let* ((pg::*backend-registry* (make-hash-table :test #'eql))
              (pg::*shutdown-abandoned-backends* nil)
              (snapshot (pg::ensure-transaction-snapshot pg::*backend* 1))
              (token (pg::get-current-reader-xid))
              (catalog pg::*catalog*)
              (clog pg::*clog-stream*)
              (streams pg::*relation-streams*))
         (setf (gethash 1 pg::*backend-registry*) pg::*backend*
               (pg::backend-read-only pg::*backend*) nil)
         (call-with-current-promotion-owner
          (lambda ()
            (call-with-promotion-exclusion
             (lambda ()
               (ok (intake-fails-p #'pg::physical-promotion-publish-intent))
               (ok (= 424
                      (pg::call-with-physical-promotion-readers
                       (lambda ()
                         (pg::physical-promotion-publish-intent)
                         (pg::physical-promotion-install-prefix)
                         (pg::physical-promotion-complete-child)))))))))
         (ok (eq catalog pg::*catalog*))
         (ok (eq clog pg::*clog-stream*))
         (ok (eq streams pg::*relation-streams*))
         (ok (eq snapshot (pg::backend-transaction-snapshot pg::*backend*)))
         (ok (= token (pg::backend-virtual-reader-xid pg::*backend*)))
         (ok (gethash token pg::*recovery-reader-xids*))
         (ok (pg::backend-transaction-read-only-p))
         (ok (not (pg::recovery-in-progress-p)))
         (pg::commit-transaction)
         (ok (pg::xid-wrap-users-empty-p))
         (ok (not (pg::backend-transaction-read-only-p)))))
     :committed-child nil))

(deftest live-promotion-reader-scope-fences-a-failed-transition
    (call-with-promotion-reader-fixture
     (lambda ()
       (ok (eq :injected
               (catch 'reader-transition-fault
                 (pg::call-with-physical-promotion-readers
                  (lambda () (throw 'reader-transition-fault :injected))))))
       (ok pg::*physical-recovery-failed-p*))))

(deftest live-promotion-reader-scope-yields-to-shutdown-before-intent
    (call-with-promotion-reader-fixture
     (lambda ()
       (let ((pg::*shutdown* pg::+no-shutdown+))
         (ok (handler-case
                 (pg::call-with-physical-promotion-readers
                  (lambda ()
                    (setf pg::*shutdown* pg::+smart-shutdown+)
                    (pg::physical-promotion-publish-intent)))
               (pg::physical-promotion-shutdown () t)))
         (ok (eql 2 (getf pg::*physical-recovery-state* :phase)))
         (ok (not pg::*physical-recovery-failed-p*))))))

(defun call-with-live-child-adoption ()
  (call-with-current-promotion-owner
   (lambda ()
     (call-with-promotion-exclusion #'pg::physical-promotion-adopt-live-child-runtime))))

(deftest live-child-runtime-adoption-preserves-reader-and-storage-objects
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let ((catalog pg::*catalog*)
             (streams pg::*relation-streams*)
             (slots pg::*wal-replication-slots*)
             (pins pg::*wal-snapshot-retention-pins*)
             (snapshot (pg::ensure-transaction-snapshot pg::*backend* 1))
             (clog pg::*clog-stream*)
             (token (pg::get-current-reader-xid)))
         (pg::register-relation-readers '(900) token)
         (setf (gethash snapshot pins) 152)
         (pg::clog-set 49 pg::+clog-committed+ :fsync t)
         (ok (= 424 (call-with-live-child-adoption)))
         (ok (= pg::+clog-committed+ (pg::clog-get 49)))
         (ok (eq catalog pg::*catalog*))
         (ok (eq clog pg::*clog-stream*))
         (ok (open-stream-p clog))
         (ok (eq streams pg::*relation-streams*))
         (ok (eq slots pg::*wal-replication-slots*))
         (ok (eq pins pg::*wal-snapshot-retention-pins*))
         (ok (= 152 (gethash snapshot pins)))
         (ok (eq snapshot (pg::backend-transaction-snapshot pg::*backend*)))
         (ok (= token (pg::get-current-reader-xid)))
         (ok (equal (list token) (gethash 900 pg::*relation-readers*)))
         (ok (= 50 (pg::snapshot-xmax snapshot)))
         (ok (= 1003 pg::*next-xid* pg::*xid-batch-end* (pg::load-next-xid)))
         (ok (= 1002 pg::*wal-xid-reserved-through*))
         (ok (= 424 pg::*wal-checkpoint-lsn* pg::*wal-durable-lsn* pg::*wal-end-lsn*))
         (ok (= 216 pg::*last-replay-lsn*))
         (ok (= 2 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
         (ok (eq :timeline pg::*wal-segment-namespace*))
         (ok (pg::recovery-in-progress-p))
         (ok (pg::backend-read-only pg::*backend*))))))

(deftest live-child-completion-retires-recovery-without-replacing-reader-storage
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore store))
       (let ((pg::*logical-capture-live-slots* nil)
             (catalog pg::*catalog*) (clog pg::*clog-stream*)
             (snapshot (pg::ensure-transaction-snapshot pg::*backend* 1)))
         (call-with-current-promotion-owner
          (lambda ()
            (call-with-promotion-exclusion #'pg::physical-promotion-start-live-child-primary)))
         (ok (eq catalog pg::*catalog*))
         (ok (eq clog pg::*clog-stream*))
         (ok (open-stream-p clog))
         (ok (eq snapshot (pg::backend-transaction-snapshot pg::*backend*)))
         (ok (= 50 (pg::snapshot-xmax snapshot)))
         (ok (not (pg::recovery-in-progress-p)))
         (ok (eql 0 pg::*logical-capture-live-slots*))
         (ok (pg::backend-transaction-read-only-p))
         (ok (null pg::*physical-receive-store*))
         (ok (not (probe-file (merge-pathnames "physical-recovery.control" root))))
         (ok (= pg::+db-in-production+ (getf (nth-value 6 (pg::wal-read-control-fields)) :db-state)))
         (pg::commit-transaction)
         (ok (not (pg::backend-transaction-read-only-p)))
         (ok (pg::xid-wrap-users-empty-p))))))

(deftest live-child-adoption-rejects-future-clog-without-clearing-old-status
    (dolist (xid '(500 1003 100000))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (pg::clog-set 49 pg::+clog-committed+ :fsync t)
         (pg::clog-set xid pg::+clog-aborted+ :fsync t)
         (ok (intake-fails-p #'call-with-live-child-adoption))
         (ok pg::*physical-recovery-failed-p*)
         (ok (= 50 pg::*next-xid*))
         (ok (= 152 pg::*wal-end-lsn*))
         (ok (= pg::+clog-committed+ (pg::clog-get 49)))
         (ok (= pg::+clog-aborted+ (pg::clog-get xid)))))))

(deftest live-child-epoch-boundary-keeps-clog-until-readers-drain
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let ((snapshot (pg::ensure-transaction-snapshot pg::*backend* 1))
             (boundary (+ pg::+xid-cycle+ 3)))
         (pg::clog-set 3 pg::+clog-committed+ :fsync t)
         (ok (= 424 (call-with-live-child-adoption)))
         (ok (= boundary pg::*next-xid* pg::*xid-batch-end*))
         (ok (= pg::+clog-committed+ (pg::clog-get 3)))
         (ok (eq snapshot (pg::backend-transaction-snapshot pg::*backend*)))
         (ok (= 50 (pg::snapshot-xmax snapshot)))
         (ok (null pg::*xid-wrap-pending*))
         (ok (null pg::*xid-wrap-completed-through*))))
     :local-floor (+ pg::+xid-cycle+ 3)))

(deftest live-child-adoption-rejects-a-floor-past-the-next-epoch-gate
    (dolist (kind '(:disk :memory))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (if (eq kind :disk)
             (let ((pg::*xid-batch-end* (+ pg::+xid-cycle+ 4))) (pg::save-next-xid))
             (setf pg::*xid-batch-end* (+ pg::+xid-cycle+ 4)))
         (let ((message (handler-case
                            (progn (call-with-live-child-adoption) nil)
                          (error (condition) (princ-to-string condition)))))
           (ok message)
           (when (eq kind :memory)
             (ok (and message (search "promotion allocation floor skips its wraparound gate" message)))))
         (ok pg::*physical-recovery-failed-p*)
         (ok (= 50 pg::*next-xid*))
         (ok (= 152 pg::*wal-end-lsn*))))))

(deftest live-child-adoption-failures-remain-fenced-after-durable-child-control
    (dolist (point '(:after-live-child-xid-floor :after-live-child-runtime))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (let ((snapshot (pg::ensure-transaction-snapshot pg::*backend* 1))
               (pg::*physical-promotion-test-hook*
                 (lambda (seen) (when (eq point seen) (throw 'live-child-fault :injected)))))
           (ok (eq :injected (catch 'live-child-fault (call-with-live-child-adoption))))
           (ok pg::*physical-recovery-failed-p*)
           (ok (= 2 (nth-value 3 (pg::wal-read-control-fields))))
           (ok (= 1003 (pg::load-next-xid)))
           (ok (= (if (eq point :after-live-child-xid-floor) 50 1003) pg::*next-xid*))
           (ok (eq snapshot (pg::backend-transaction-snapshot pg::*backend*))))))))

(deftest public-physical-slots-survive-the-complete-live-promotion-path
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let* ((reserved (pg::with-live-operation
                          (pg::wal-create-physical-replication-slot "retained_public" :immediately-reserve t)))
              (unreserved (pg::with-live-operation
                            (pg::wal-create-physical-replication-slot "future_public")))
              (temporary (pg::with-live-operation
                           (pg::wal-create-physical-replication-slot "temporary_public"
                                                                     :immediately-reserve t :temporary t)))
              (registry pg::*wal-replication-slots*)
              (owner pg::*backend*)
              (restart (pg::wal-replication-slot-restart-lsn reserved)))
         (ok (<= restart pg::*last-replay-lsn*))
         (call-with-current-promotion-owner
          (lambda ()
            (call-with-promotion-exclusion
             (lambda ()
               (let ((pending (pg::physical-promotion-publish-intent)))
                 (ok (pg::wal-retained-reference-valid-p (getf pending :retained-reference))))
               (pg::physical-promotion-install-prefix)
               (ok (= 424 (pg::physical-promotion-publish-child-control)))
               (ok (= 424 (pg::physical-promotion-adopt-live-child-runtime)))))))
         (ok (eq registry pg::*wal-replication-slots*))
         (ok (eq reserved (gethash "retained_public" registry)))
         (ok (eq unreserved (gethash "future_public" registry)))
         (ok (eq temporary (gethash "temporary_public" registry)))
         (ok (= restart (pg::wal-replication-slot-restart-lsn reserved)))
         (ok (null (pg::wal-replication-slot-restart-lsn unreserved)))
         (ok (eq owner (pg::wal-replication-slot-active-owner temporary)))
         (ok (eq owner (pg::wal-replication-slot-temporary-owner temporary)))
         (ok (= 2 pg::*wal-insert-timeline*))))
     :committed-child nil))

(deftest promotion-preflight-rejects-unproven-public-slot-state
    (dolist (kind '(:foreign :future :timeline :logical :missing :orphan :changed :temporary-entry))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (let* ((slot (pg::with-live-operation
                        (pg::wal-create-physical-replication-slot "checked_public")))
                (state pg::*physical-recovery-state*)
                (parent (lineage-test-bytes (pg::wal-control-path)))
                (control (lineage-test-bytes (pg::physical-recovery-control-path))))
           (case kind
             (:foreign (incf (pg::wal-replication-slot-system-id slot)))
             (:future (setf (pg::wal-replication-slot-restart-lsn slot) (1+ pg::*last-replay-lsn*)
                            (pg::wal-replication-slot-restart-timeline slot) 1))
             (:timeline (setf (pg::wal-replication-slot-restart-lsn slot) 153
                              (pg::wal-replication-slot-restart-timeline slot) 2)))
           (when (member kind '(:foreign :future :timeline))
             (pg::wal-save-replication-slot slot))
           (case kind
             (:logical (setf (pg::wal-replication-slot-kind slot) :logical))
             (:missing (delete-file (pg::wal-replication-slot-path "checked_public")))
             (:orphan (remhash "checked_public" pg::*wal-replication-slots*))
             (:changed (setf (pg::wal-replication-slot-restart-lsn slot) 153
                             (pg::wal-replication-slot-restart-timeline slot) 1))
             (:temporary-entry
              (with-open-file
                  (stream (merge-pathnames ".pending.tmp" (pg::wal-replication-slot-directory))
                          :direction :output :element-type '(unsigned-byte 8))
                (finish-output stream))))
           (ok (intake-fails-p
                (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)))
               (format nil "~a" kind))
           (ok (eq state pg::*physical-recovery-state*))
           (ok (= 2 (getf state :phase)))
           (ok (not pg::*physical-recovery-failed-p*))
           (ok (equalp parent (lineage-test-bytes (pg::wal-control-path))))
           (ok (equalp control (lineage-test-bytes (pg::physical-recovery-control-path))))))
       :committed-child nil)))

(deftest live-child-adoption-preserves-public-physical-slot-generations
    (dolist (kind '(:unreserved :reserved :lost :temporary))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (let* ((owner (when (eq kind :temporary) pg::*backend*))
                (slot (pg::make-wal-replication-slot
                       :name "promotion_public" :kind :physical
                       :system-id (pg::catalog-system-identifier)
                       :restart-lsn (unless (eq kind :unreserved) 153)
                       :restart-timeline (unless (eq kind :unreserved) 1)
                       :consistent-lsn nil :confirmed-flush-lsn nil :plugin nil
                       :wal-status (if (eq kind :lost) :lost :reserved)
                       :temporary-p (eq kind :temporary) :temporary-owner owner
                       :active-owner owner :active-owner-backend owner)))
           (pg::wal-save-replication-slot slot)
           (setf (pg::wal-replication-slot-inactive-since slot) 123456
                 (gethash "promotion_public" pg::*wal-replication-slots*) slot)
           (let ((before (lineage-test-bytes
                          (pg::wal-replication-slot-path "promotion_public"))))
             (ok (= 424 (call-with-live-child-adoption)))
             (ok (eq slot (gethash "promotion_public" pg::*wal-replication-slots*)))
             (ok (eq owner (pg::wal-replication-slot-active-owner slot)))
             (ok (eq owner (pg::wal-replication-slot-temporary-owner slot)))
             (ok (eql (unless (eq kind :unreserved) 153)
                      (pg::wal-replication-slot-restart-lsn slot)))
             (ok (eql (unless (eq kind :unreserved) 1)
                      (pg::wal-replication-slot-restart-timeline slot)))
             (ok (equalp before (lineage-test-bytes
                                 (pg::wal-replication-slot-path "promotion_public"))))))))))

(deftest live-child-adoption-rejects-unproven-retention-inventories
    (dolist (kind '(:slot :pin :temporary :dangling :directory-link :live-slot :live-pin))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore store))
         (let ((before (lineage-test-bytes (pg::wal-control-path)))
               (slots (pg::wal-replication-slot-directory))
               (pins (pg::physical-source-pin-directory)))
           (case kind
             (:directory-link
              (let ((target (merge-pathnames "outside-slots/" root)))
                (pg::wal-ensure-directory-durable target)
                (sb-posix:symlink (namestring target) (namestring (merge-pathnames "pg_replslot" root)))))
             (:live-slot (setf (gethash "old" pg::*wal-replication-slots*) :unproven))
             (:live-pin (setf (gethash "old" pg::*physical-source-pins*) :unproven))
             (otherwise
              (let ((directory (if (member kind '(:pin :dangling)) pins slots)))
                (pg::wal-ensure-directory-durable directory)
                (if (eq kind :dangling)
                    (sb-posix:symlink "absent-target" (namestring (merge-pathnames "hidden.tmp" directory)))
                    (with-open-file
                        (stream (merge-pathnames
                                 (case kind (:slot "old.slot") (:pin "old.pin")
                                       (otherwise ".pending.tmp"))
                                 directory)
                                :direction :output :if-exists :supersede
                                :element-type '(unsigned-byte 8))
                      (finish-output stream))))))
           (ok (intake-fails-p #'call-with-live-child-adoption) (format nil "~a" kind))
           (ok pg::*physical-recovery-failed-p*)
           (ok (= 50 pg::*next-xid*))
           (ok (= 152 pg::*wal-end-lsn*))
           (ok (equalp before (lineage-test-bytes (pg::wal-control-path)))))))))

(deftest live-child-retention-cannot-name-an-uncopied-parent-segment
    (let ((pg::*wal-replication-slots* (make-hash-table :test #'equal))
          (pg::*physical-source-pins* (make-hash-table :test #'equal))
          (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq)))
      (with-physical-store-directory (root)
        (let ((pg::*pgdata* root)
              (fork (+ pg::+wal-segment-size+ 216))
              (owner (gensym "RETAINED-READER-")))
          (setf (gethash owner pg::*wal-snapshot-retention-pins*) 32)
          (ok (intake-fails-p (lambda () (pg::physical-promotion-check-live-retention fork))))
          (ok (= 32 (gethash owner pg::*wal-snapshot-retention-pins*)))
          (setf (gethash owner pg::*wal-snapshot-retention-pins*) pg::+wal-segment-size+)
          (ok (progn (pg::physical-promotion-check-live-retention fork) t))))))

(deftest rootless-promotion-rejects-a-public-slot-before-the-copied-fork-segment
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (fork (+ pg::+wal-segment-size+ 216))
             (state (list :phase 3 :timeline 1 :promotion-child 2 :applied fork :system-id 123))
             (slot (physical-slot-test-object :restart 32 :system-id 123))
             (message (handler-case
                          (progn (pg::physical-promotion-check-slot-prefix (list slot) state) nil)
                        (error (condition) (princ-to-string condition)))))
        (ok (and message (search "predates the copied promotion prefix" message)))
        (ok (= 32 (pg::wal-replication-slot-restart-lsn slot))))))

(deftest live-child-adoption-rejects-pending-runtime-state
    (dolist (kind (list :completed-wrap :pending-wrap :failed-wrap :wrap-marker :parent-output
                                        :new-segments :page-cache :page-contributors :batch-contributors :real-xid))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (case kind
           (:completed-wrap (setf pg::*xid-wrap-completed-through* (+ pg::+xid-cycle+ 3)))
           (:pending-wrap (setf pg::*xid-wrap-pending* t))
           (:failed-wrap (setf pg::*xid-wrap-failure* :failed))
           (:wrap-marker (pg::save-xid-wrap-marker (+ pg::+xid-cycle+ 3)))
           (:parent-output (setf pg::*wal-output-segment* 0))
           (:new-segments (setf pg::*wal-new-segments* '(0)))
           (:page-cache (setf (gethash '(900 0) pg::*wal-page-cache*) (pg::make-page)))
           (:page-contributors (setf (gethash '(900 0) pg::*wal-page-cache-xids*) '(3)))
           (:batch-contributors (setf pg::*wal-page-batch-xids* '(3)))
           (:real-xid (setf pg::*active-xids* '(3))))
         (ok (intake-fails-p #'call-with-live-child-adoption) (format nil "~a" kind))
         (ok pg::*physical-recovery-failed-p*)
         (ok (= 50 pg::*next-xid*))
         (ok (= 152 pg::*wal-end-lsn*))))))

(deftest live-child-adoption-requires-the-exact-committed-plan
    (dolist (kind '(:larger-counter :extra-durable-end))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (multiple-value-bind (checkpoint durable original insertion checkpoint-id namespace metadata)
             (pg::wal-read-control-fields)
           (let ((snapshot (copy-list original)))
             (if (eq kind :larger-counter)
                 (let* ((start (getf snapshot :start))
                        (path (pg::wal-segment-path (floor start pg::+wal-segment-size+) insertion namespace))
                        (bytes (lineage-test-bytes path)))
                   (setf (getf snapshot :next-xid) 2003
                         (getf snapshot :xid-reserved-through) 2002
                         (getf snapshot :oldest-active-xid) 2003)
                   (replace bytes
                            (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                   (pg::wal-encode-checkpoint-snapshot snapshot))
                            :start1 (mod start pg::+wal-segment-size+))
                   (pg::wal-atomic-write-bytes path bytes))
                 (incf durable 32))
             (pg::wal-atomic-write-bytes
              (pg::wal-control-path)
              (pg::wal-encode-control-image checkpoint durable snapshot :timeline insertion
                                                                        :checkpoint-timeline checkpoint-id :namespace namespace :metadata metadata))
             (ok (progn (pg::physical-promotion-check-child-control insertion checkpoint-id) t))
             (ok (intake-fails-p #'call-with-live-child-adoption) (format nil "~a" kind))
             (ok pg::*physical-recovery-failed-p*)
             (ok (= 50 pg::*next-xid*))
             (ok (= 152 pg::*wal-end-lsn*))))))))

(deftest live-child-adoption-rejects-a-divergent-retained-catalog
    (dolist (kind '(:next-oid :schema))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore root store))
         (if (eq kind :next-oid)
             (incf (pg::catalog-next-oid pg::*catalog*))
             (setf (pg::pg-class-name (pg::lookup-table "table_900")) "changed"))
         (ok (intake-fails-p #'call-with-live-child-adoption))
         (ok pg::*physical-recovery-failed-p*)
         (ok (= 50 pg::*next-xid*))
         (ok (= 152 pg::*wal-end-lsn*))))))

(deftest promotion-preflight-rejects-unsafe-state-before-durable-intent
    (dolist (kind '(:gap-clog :slot-file :memory-pin :bad-floor))
      (call-with-live-child-fixture
       (lambda (root store)
         (declare (ignore store))
         (let ((wal-before (lineage-test-bytes (pg::wal-control-path)))
               (recovery-before (lineage-test-bytes (pg::physical-recovery-control-path))))
           (case kind
             (:gap-clog (pg::clog-set 500 pg::+clog-committed+ :fsync t))
             (:slot-file
              (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
              (pg::wal-atomic-write-bytes (pg::wal-replication-slot-path "stale") (empty-octets)))
             (:memory-pin (setf (gethash "stale" pg::*physical-source-pins*) :unproven))
             (:bad-floor
              (let ((pg::*xid-batch-end* (+ pg::+xid-cycle+ 4))) (pg::save-next-xid))))
           (ok (intake-fails-p
                (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
           (ok (not pg::*physical-recovery-failed-p*))
           (ok (eql 2 (getf pg::*physical-recovery-state* :phase)))
           (ok (= 50 pg::*next-xid*))
           (ok (equalp wal-before (lineage-test-bytes (pg::wal-control-path))))
           (ok (equalp recovery-before (lineage-test-bytes (pg::physical-recovery-control-path))))
           (ok (not (probe-file (pg::timeline-history-path 2 root))))
           (when (eq kind :gap-clog)
             (let ((fresh-rc (pg::make-snapshot 1)))
               (ok (not (pg::heap-tuple-visible-p 500 0 0 fresh-rc nil)))))))
       :committed-child nil)))

(deftest clog-tail-preflight-preserves-the-existing-epoch-gate
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let ((boundary (+ pg::+xid-cycle+ 3)))
         (pg::clog-set 3 pg::+clog-committed+ :fsync t)
         (ok (progn (pg::physical-promotion-check-clog-tail boundary boundary) t))
         (ok (= pg::+clog-committed+ (pg::clog-get 3)))
         (ok (intake-fails-p
              (lambda () (pg::physical-promotion-check-clog-tail boundary (+ boundary 1000)))))))))

(deftest live-child-adoption-requires-a-joined-restartpoint-worker
    (call-with-live-child-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let* ((release (sb-thread:make-semaphore))
              (worker (sb-thread:make-thread
                       (lambda () (sb-thread:wait-on-semaphore release :timeout 10)))))
         (unwind-protect
              (let ((pg::*physical-restartpoint-worker* worker))
                (ok (intake-fails-p #'call-with-live-child-adoption))
                (ok pg::*physical-recovery-failed-p*)
                (ok (= 50 pg::*next-xid*)))
           (sb-thread:signal-semaphore release)
           (sb-thread:join-thread worker :timeout 5 :default :timed-out))))))

(deftest child-control-publication-commits-only-after-checkpoint-and-data-sync
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (prepare-promotion-child)
      (let ((old (lineage-test-bytes (pg::wal-control-path)))
            (pg::*physical-promotion-test-hook*
              (lambda (point) (when (eq point :after-child-checkpoint-flush) (throw 'staged-child :staged)))))
        (ok (eq :staged (catch 'staged-child
                          (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control))))
        (ok (equalp old (lineage-test-bytes (pg::wal-control-path))))
        (ok pg::*physical-recovery-failed-p*))
      (let ((pg::*physical-recovery-failed-p* nil))
        (ok (= 424 (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)))
        (multiple-value-bind (checkpoint durable snapshot child checkpoint-child namespace) (pg::wal-read-control-fields)
          (ok (= 424 checkpoint durable)) (ok (= 2 child checkpoint-child))
          (ok (eq :timeline namespace)) (ok (= 1003 (getf snapshot :next-xid))))
        (multiple-value-bind (snapshot history) (pg::physical-promotion-check-child-control 2 2)
          (ok (= 1003 (getf snapshot :next-xid))) (ok (= 2 (pg::timeline-at-point 304 history))))
        (ok (equal "57P03" (recovery-state-of #'pg::check-recovery-available))))))

(deftest committed-child-cannot-retire-unvalidated-pending-reference
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (prepare-promotion-child)
      (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)
      (let* ((path (pg::physical-recovery-control-path))
             (control (pg::physical-recovery-read-control pg::*pgdata*))
             (reference (list :root-id 0 :base-timeline (getf control :timeline)
                              :base (getf control :checkpoint) :manifest-digest (getf control :manifest-digest)
                              :end (getf control :applied) :end-digest 9))
             (bytes (pg::physical-recovery-control-image
                     3 (getf control :system-id) (getf control :timeline)
                     (getf control :manifest-digest) (getf control :checkpoint)
                     (getf control :applied) (getf control :next-xid)
                     :state-time (getf control :state-time)
                     :promotion-child (getf control :promotion-child)
                     :promotion-generation (getf control :promotion-generation)
                     :promotion-source-namespace (getf control :promotion-source-namespace)
                     :applied-timeline (getf control :applied-timeline (getf control :timeline))
                     :history-target (getf control :history-target)
                     :history-digest (getf control :history-digest)
                     :retained-reference reference))
             (marker-path (merge-pathnames "recovery.required" pg::*pgdata*))
             (marker (pg::physical-recovery-marker-bytes)))
        (pg::wal-atomic-write-bytes marker-path marker)
        (pg::wal-atomic-write-bytes path bytes)
        (ok (equal reference (getf (pg::physical-recovery-read-control pg::*pgdata*) :retained-reference)))
        (ok (handler-case (progn (pg::physical-promotion-startup-primary-p) nil)
              (error (condition)
                (search "committed child differs from pending retained reference" (princ-to-string condition)))))
        (ok (equalp bytes (lineage-test-bytes path)))
        (ok (equalp marker (lineage-test-bytes marker-path))))))

(deftest child-control-publication-uncertainty-never-rolls-back-parent-control
    (dolist (point '(:after-child-control-rename :after-child-control-directory-fsync))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (prepare-promotion-child)
        (let ((pg::*physical-promotion-test-hook*
                (lambda (seen) (when (eq point seen) (throw 'uncertain-child :aborted)))))
          (ok (eq :aborted (catch 'uncertain-child
                             (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)))))
        (ok pg::*physical-recovery-failed-p*)
        (ok (= 2 (nth-value 3 (pg::wal-read-control-fields))))
        (ok (progn (pg::physical-promotion-check-child-control 2 2) t)))))

(cl-spec:defproperty child-checkpoint-planning-keeps-all-records-in-the-child
    ((fork (range integer 32 33554432)) (bound (range integer 100 100000))
                                        (local (range integer 3 100000)) (child (range integer 2 4294967295)))
  (:about pg::physical-promotion-checkpoint-plan)
  (:trials (:normal 100))
  (let* ((state (sample-promotion-checkpoint-state fork :bound bound :child child))
         (plan (pg::physical-promotion-checkpoint-plan state 20000 local))
         (snapshot (getf plan :snapshot)))
    (and (= child (getf snapshot :timeline)) (= 1 (getf snapshot :prev-timeline))
         (= (pg::canonical-next-full-xid (max 50 (1+ bound) local)) (getf snapshot :next-xid))
         (every (lambda (frame)
                  (destructuring-bind (start bytes) frame
                    (and (<= fork start)
                         (<= (+ (mod start pg::+wal-segment-size+) (length bytes)) pg::+wal-segment-size+))))
                (getf plan :frames)))))

(deftest generated-child-checkpoint-plan
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'child-checkpoint-planning-keeps-all-records-in-the-child :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
