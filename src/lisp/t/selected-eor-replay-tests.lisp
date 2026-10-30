(in-package #:pg-physical-tests)

(defun selected-replay-test-prepare (store)
  (let* ((eor (pg::wal-encode-end-of-recovery-record 96 42 2 1 1))
         (state pg::*physical-recovery-state*))
    (setf (getf state :phase) 2 (getf state :replay-timeline) 1 (getf state :required-timeline) 1
          (getf state :source-reservation) (list :version 1 :through 96 :bound 999 :complete t)
          (getf state :source-xids)
          (pg::running-xacts-anchor (list :next-xid 50 :running '(3 49) :complete t) 96))
    (pg::physical-store-select-history store 2 (format nil "1~c0/00000060~%" #\Tab) eor)
    (setf pg::*physical-recovery-state* (pg::physical-recovery-attach-selected-store state store))
    (pg::physical-store-input store 96 eor)
    (pg::physical-store-flush store)
    eor))

(defun selected-replay-test-child-checkpoint (start &optional (next 2000))
  (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                         (pg::wal-encode-checkpoint-snapshot
                          (list :version 2 :start start :end (+ start (pg::wal-checkpoint-record-size)) :redo start
                                :next-xid next :xid-reserved-through (1- next) :next-oid 16384 :oldest-active-xid next
                                :time 42 :timeline 2 :prev-timeline 1))))

(deftest selected-eor-replay-publishes-a-child-pair-without-ending-recovery
    (with-restartpoint-fixture (root store)
      (let ((pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1))
        (pg::clog-init)
        (unwind-protect
             (progn
               (selected-replay-test-prepare store)
               (ok (= 1 (pg::physical-recovery-replay-timeline)))
               (ok (= 152 (pg::physical-replay-from 96)))
               (ok (= 2 (pg::physical-recovery-replay-timeline)))
               (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
               (ok (pg::recovery-in-progress-p))
               (ok (= 50 pg::*next-xid*))
               (ok (null (getf pg::*physical-recovery-state* :source-xids)))
               (ng (getf (getf pg::*physical-recovery-state* :source-reservation) :complete))
               (ok (= 0 (pg::clog-get 3)))
               (let ((control (pg::physical-recovery-read-control root)))
                 (ok (= 1 (getf control :timeline)))
                 (ok (= 2 (getf control :applied-timeline)))
                 (ok (= 152 (getf control :applied)))))
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(deftest fixed-timeline-policy-keeps-cached-child-wal-unapplied
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (pg::physical-store-input store 152 (sample-physical-record 152))
             (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184 500000))
             (pg::physical-store-flush store)
             (let ((before (pg::physical-store-control-tuple store))
                   (reservation (copy-tree (getf pg::*physical-recovery-state* :source-reservation)))
                   (members (copy-tree (getf pg::*physical-recovery-state* :source-xids))))
               (setf (getf pg::*physical-recovery-state* :timeline-policy)
                     (pg::physical-recovery-resolve-timeline-policy
                      pg::*physical-recovery-state* store nil :current))
               (ok (= 1 (getf (getf pg::*physical-recovery-state* :timeline-policy) :target)))
               (ok (pg::physical-recovery-replay-pending-p))
               (ok (= 96 (pg::physical-replay-from 96)))
               (ok (not (pg::physical-recovery-replay-pending-p)))
               (let ((selection (pg::physical-store-selection store)))
                 (unwind-protect
                      (progn
                        (setf (pg::physical-store-selection store) (copy-list selection))
                        (ok (pg::physical-recovery-replay-pending-p)))
                   (setf (pg::physical-store-selection store) selection)))
               (ok (= 96 (pg::physical-row-replay-prefix 50)))
               (ok (= 1 (pg::physical-recovery-replay-timeline)))
               (ok (= 50 pg::*next-xid*))
               (ok (equal reservation (getf pg::*physical-recovery-state* :source-reservation)))
               (ok (equal members (getf pg::*physical-recovery-state* :source-xids)))
               (ok (equal before (pg::physical-store-control-tuple store)))
               (ok (not (pg::physical-promotion-awaiting-child-initialization-p)))
               (multiple-value-bind (removed terminal)
                   (pg::physical-recovery-proven-drops store (getf pg::*physical-recovery-state* :cluster-identity)
                                                       :start-lsn 96 :required-lsn 96
                                                       :policy (getf pg::*physical-recovery-state* :timeline-policy))
                 (ok (zerop (hash-table-count removed)))
                 (ok (= 96 terminal)))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest fixed-timeline-policy-validates-corrupt-excluded-child-before-any-effects
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (setf (getf pg::*physical-recovery-state* :timeline-policy)
            (pg::physical-recovery-resolve-timeline-policy pg::*physical-recovery-state* store nil :current))
      (let* ((path (pg::physical-store-segment-path store 0 2))
             (bytes (pg::recovery-read-bytes path))
             (visits 0)
             (pg::*physical-replay-test-hook* (lambda (point record)
                                                (declare (ignore point record))
                                                (incf visits))))
        (setf (aref bytes (1- (length bytes))) (logxor 1 (aref bytes (1- (length bytes)))))
        (pg::wal-atomic-write-bytes path bytes)
        (ok (intake-fails-p (lambda () (pg::physical-replay-from 96))))
        (ok (zerop visits))
        (ok (= 96 pg::*last-replay-lsn*))
        (ok (= 50 pg::*next-xid*)))))

(deftest fixed-timeline-policy-does-not-use-child-drop-to-excuse-missing-parent-file
    (with-restartpoint-fixture (root store)
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (selected-replay-test-prepare store)
      (let* ((checkpoint (selected-replay-test-child-checkpoint 184))
             (next (+ 184 (length checkpoint)))
             (drop (pg::wal-encode-record pg::+wal-record-catalog+ 0 (+ next 32)
                                          (history-catalog-payload nil '(900)))))
        (pg::physical-store-input store 152 (sample-physical-record 152))
        (pg::physical-store-input store 184 checkpoint)
        (pg::physical-store-input store next (sample-physical-record next))
        (pg::physical-store-input store (+ next 32) drop)
        (pg::physical-store-flush store)
        (multiple-value-bind (metadata catalog prefix digest)
            (pg::physical-restartpoint-read pg::*physical-recovery-state*)
          (declare (ignore catalog prefix digest))
          (let ((policy (pg::physical-recovery-resolve-timeline-policy
                         pg::*physical-recovery-state* store metadata :current)))
            (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
            (multiple-value-bind (removed terminal)
                (pg::physical-recovery-proven-drops store "history-source" :start-lsn 96 :required-lsn 96 :policy nil)
              (ok (gethash 900 removed))
              (ok (= (+ next 32 (length drop)) terminal)))
            (multiple-value-bind (removed terminal)
                (pg::physical-recovery-proven-drops store "history-source" :start-lsn 96
                                                                           :required-lsn 96 :policy policy)
              (ok (zerop (hash-table-count removed)))
              (ok (= 96 terminal)))
            (let ((catalog-before (pg::recovery-read-bytes (pg::catalog-path)))
                  (next-before pg::*next-xid*))
              (delete-file (merge-pathnames "base/900" root))
              (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
              (ok (equalp catalog-before (pg::recovery-read-bytes (pg::catalog-path))))
              (ok (= next-before pg::*next-xid*))
              (ok (= 96 pg::*last-replay-lsn*))))))))

(deftest current-timeline-policy-uses-required-and-authoritative-restartpoint-progress
    (with-restartpoint-fixture (root store)
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (let ((parent-policy (pg::physical-recovery-resolve-timeline-policy
                                   pg::*physical-recovery-state* store nil :current)))
               (pg::physical-replay-from 96)
               (pg::physical-store-compact store 152 #'pg::physical-restartpoint-publish)
               (setf (getf pg::*physical-recovery-state* :required-applied) 96
                     (getf pg::*physical-recovery-state* :required-timeline) 1
                     (getf pg::*physical-recovery-state* :replay-timeline) 1
                     pg::*last-replay-lsn* 32)
               (multiple-value-bind (metadata catalog prefix digest)
                   (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                 (declare (ignore catalog prefix digest))
                 (let ((policy (pg::physical-recovery-resolve-timeline-policy
                                pg::*physical-recovery-state* store metadata :current)))
                   (ok (= 2 (getf policy :target) (getf policy :startup-anchor)))
                   (ok (equal '(96 1) (getf policy :original-required)))
                   (ok (equal '(152 2) (getf policy :startup-seed))))
                 (ok (intake-fails-p
                      (lambda () (pg::physical-recovery-resolve-timeline-policy
                                  pg::*physical-recovery-state* store metadata 1)))))
               (setf (getf pg::*physical-recovery-state* :timeline-policy) parent-policy)
               (let ((before (pg::recovery-read-bytes (pg::catalog-path)))
                     (next pg::*next-xid*))
                 (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
                 (ok (= next pg::*next-xid*))
                 (ok (= 32 pg::*last-replay-lsn*))
                 (ok (equalp before (pg::recovery-read-bytes (pg::catalog-path)))))
               (ok (= 1 (getf pg::*physical-recovery-state* :required-timeline)))
               (ok (= 96 (getf pg::*physical-recovery-state* :required-applied)))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest numeric-timeline-policy-validates-exact-history-and-reports-divergent-receipt
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (dolist (target '(1 2))
        (let ((policy (pg::physical-recovery-resolve-timeline-policy
                       pg::*physical-recovery-state* store nil target)))
          (ok (= target (getf policy :target)))
          (ok (eq :numeric (getf policy :goal)))))
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-resolve-timeline-policy
                       pg::*physical-recovery-state* store nil 4294967295))))
      (pg::timeline-write-history 3 1 64 "excluded required progress")
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-resolve-timeline-policy
                       pg::*physical-recovery-state* store nil 3))))
      (pg::timeline-write-history 4294967295 1 128 "future branch with numeric gap")
      (let ((policy (pg::physical-recovery-resolve-timeline-policy
                     pg::*physical-recovery-state* store nil 4294967295)))
        (ok (= 4294967295 (getf policy :target)))
        (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
        (ok (= 96 (pg::physical-replay-from 96)))
        (ok (eq :timeline-history-unavailable
                (getf (getf pg::*physical-recovery-state* :timeline-replay-fence) :reason)))
        (ok (not (pg::physical-recovery-replay-pending-p)))
        (ok (= 96 (getf pg::*physical-recovery-state* :required-applied)))
        (ok (= 50 pg::*next-xid*))
        (ok (not pg::*physical-recovery-failed-p*)))))

(deftest numeric-timeline-policy-rejects-rewritten-durable-ancestry
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (state (list :timeline 2 :end 96 :required-applied 96 :required-timeline 2
                         :observed-next-xid 50)))
        (pg::wal-ensure-directory-durable (merge-pathnames "pg_wal/" root))
        (pg::timeline-write-history 2 1 64 "known ancestor")
        (pg::timeline-write-history 3 2 256 "compatible descendant")
        (pg::timeline-write-history 5 2 192 "compatible later divergence")
        (dolist (target '(3 5))
          (ok (= target (getf (pg::physical-recovery-resolve-timeline-policy state nil nil target) :target))))
        (pg::timeline-publish-history-bytes
         (pg::timeline-history-path 4 root)
         (pg::string->bytes (format nil "1~c0/00000020~%2~c0/00000100~%" #\Tab #\Tab)))
        (ok (intake-fails-p
             (lambda () (pg::physical-recovery-resolve-timeline-policy state nil nil 4)))))))

(cl-spec:defproperty fixed-timeline-boundaries-use-lineage-at-exact-forks
    ((child (range integer 2 4294967295))
     (fork (range integer 96 18446744073709551559)))
  (:about pg::physical-recovery-policy-record-disposition)
  (:trials (:normal 200))
  (let* ((history (pg::timeline-check-lineage
                   (pg::timeline-parse-history child
                                               (format nil "1~c~a~%" #\Tab (pg::wal-format-lsn fork)))))
         (store (pg::%make-physical-store :timeline 1 :selection (list :history history)))
         (parent-policy (list :target 1 :history (pg::timeline-parse-history 1 "")))
         (child-policy (list :target child :history history))
         (parent-record (pg::wal-decode-record (sample-physical-record (- fork 32)) (- fork 32)))
         (child-record (pg::wal-decode-record
                        (pg::wal-encode-end-of-recovery-record fork 42 child 1 1) fork)))
    (and (eq :permitted (pg::physical-recovery-policy-record-disposition store parent-record parent-policy))
         (eq :timeline-target-reached
             (pg::physical-recovery-policy-record-disposition store child-record parent-policy))
         (eq :permitted (pg::physical-recovery-policy-record-disposition store child-record child-policy))
         (eq :permitted (pg::physical-recovery-policy-record-disposition store child-record nil)))))

(deftest generated-fixed-timeline-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'fixed-timeline-boundaries-use-lineage-at-exact-forks :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest selected-eor-cannot-be-used-to-finish-an-active-batch
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((raw (selected-replay-test-prepare store))
             (record (pg::wal-decode-record raw 96))
             (pg::*wal-recovery-group-active* t))
        (ok (signals (pg::physical-replay-check-selected-eor store record raw 96 1) 'error))
        (ok (= 96 pg::*last-replay-lsn*)))))

(deftest selected-eor-replay-enforces-the-minimum-recovery-timeline
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((raw (selected-replay-test-prepare store))
             (record (pg::wal-decode-record raw 96))
             (pg::*wal-recovery-group-active* nil))
        (setf (getf pg::*physical-recovery-state* :required-applied) 256)
        (ok (signals (pg::physical-replay-check-selected-eor store record raw 96 1) 'error))
        (setf (getf pg::*physical-recovery-state* :required-timeline) 2)
        (ok (= 2 (pg::physical-replay-check-selected-eor store record raw 96 1))))))

(deftest selected-eor-only-replay-rejects-an-allocation-record-before-child-checkpoint
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (pg::physical-replay-from 96)
             (let* ((start 152)
                    (record (pg::wal-decode-record
                             (pg::wal-encode-record pg::+wal-record-xid-reservation+ 0 start
                                                    (pg::wal-encode-xid-reservation 1999)) start)))
               (ok (signals (pg::physical-replay-check-child-initialization record 50) 'error)))
             (ok (= 152 pg::*last-replay-lsn*)))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-child-checkpoint-restores-allocation-without-inventing-membership
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-replay-restarting-p* t))
        (pg::clog-init)
        (unwind-protect
             (progn
               (selected-replay-test-prepare store)
               (pg::physical-replay-from 96)
               (pg::physical-store-input store 152 (sample-physical-record 152))
               (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184))
               (pg::physical-store-flush store)
               (pg::physical-replay-from 152)
               (ok (= 2000 pg::*next-xid*))
               (ok (= 2 (pg::physical-recovery-replay-timeline)))
               (ok (= 2 (getf (getf pg::*physical-recovery-state* :source-checkpoint) :timeline)))
               (ok (getf (getf pg::*physical-recovery-state* :source-reservation) :complete))
               (ok (= 1999 (getf (getf pg::*physical-recovery-state* :source-reservation) :bound)))
               (ok (null (getf pg::*physical-recovery-state* :source-xids))))
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(deftest selected-child-generation-restores-a-pair-ahead-of-ordinary-control
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (pg::physical-replay-from 96)
             (pg::physical-store-compact store 152 #'pg::physical-restartpoint-publish)
             (setf (getf pg::*physical-recovery-state* :required-applied) 96
                   (getf pg::*physical-recovery-state* :required-timeline) 1
                   (getf pg::*physical-recovery-state* :replay-timeline) 1
                   pg::*last-replay-lsn* 32)
             (ok (= 152 (pg::physical-restartpoint-prepare-storage store)))
             (ok (= 2 (pg::physical-recovery-replay-timeline)))
             (ok (= 152 (getf pg::*physical-recovery-state* :required-applied)))
             (ok (= 2 (getf pg::*physical-recovery-state* :required-timeline)))
             (ng (getf (getf pg::*physical-recovery-state* :source-reservation) :complete))
             (ok (null (getf pg::*physical-recovery-state* :source-xids))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-parent-generation-replays-eor-toward-a-newer-required-pair
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
             (selected-replay-test-prepare store)
             (pg::physical-replay-from 96)
             (setf (getf pg::*physical-recovery-state* :replay-timeline) 1 pg::*last-replay-lsn* 32)
             (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
             (ok (= 1 (pg::physical-recovery-replay-timeline)))
             (ok (= 152 (getf pg::*physical-recovery-state* :required-applied)))
             (ok (= 2 (getf pg::*physical-recovery-state* :required-timeline)))
             (multiple-value-bind (metadata catalog prefix digest)
                 (pg::physical-restartpoint-read pg::*physical-recovery-state*)
               (declare (ignore catalog prefix digest))
               (let ((policy (pg::physical-recovery-resolve-timeline-policy
                              pg::*physical-recovery-state* store metadata :current)))
                 (ok (= 2 (getf policy :target)))
                 (ok (equal '(96 1) (getf policy :startup-seed)))
                 (ok (equal '(152 2) (getf policy :original-required)))))
             (pg::physical-replay-from 96)
             (ok (= 152 pg::*last-replay-lsn*))
             (ok (= 2 (pg::physical-recovery-replay-timeline))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-recovery-identify-does-not-pair-parent-replay-with-child-receipt
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (multiple-value-bind (lsn timeline) (pg::physical-recovery-identify-position)
        (ok (= 96 lsn))
        (ok (= 1 timeline)))
      (setf pg::*last-replay-lsn* 152 (getf pg::*physical-recovery-state* :replay-timeline) 2)
      (multiple-value-bind (lsn timeline) (pg::physical-recovery-identify-position)
        (ok (= 152 lsn))
        (ok (= 2 timeline)))))

(deftest selected-promotion-waits-for-durable-child-initialization
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (ok (pg::physical-promotion-awaiting-child-initialization-p))
             (ok (= 1 (pg::physical-recovery-replay-timeline)))
             (pg::physical-replay-from 96)
             (ok (pg::physical-promotion-awaiting-child-initialization-p))
             (pg::physical-store-input store 152 (sample-physical-record 152))
             (pg::physical-store-flush store)
             (ok (pg::physical-promotion-awaiting-child-initialization-p))
             (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184))
             (pg::physical-store-flush store)
             (ng (pg::physical-promotion-awaiting-child-initialization-p))
             (ok (= 152 pg::*last-replay-lsn*))
             (ok (null (getf pg::*physical-recovery-state* :source-checkpoint))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-promotion-does-not-defer-malformed-child-initialization
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (pg::physical-store-input
       store 152
       (pg::wal-encode-record pg::+wal-record-xid-reservation+ 0 152
                              (pg::wal-encode-xid-reservation 1999)))
      (pg::physical-store-flush store)
      (ok (signals (pg::physical-promotion-awaiting-child-initialization-p) 'error))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 1 (pg::physical-recovery-replay-timeline)))))

(deftest selected-promotion-readiness-rejects-regressing-child-allocation
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (pg::physical-store-input store 152 (sample-physical-record 152))
      (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184 500))
      (pg::physical-store-flush store)
      (ok (signals (pg::physical-promotion-awaiting-child-initialization-p) 'error))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 1 (pg::physical-recovery-replay-timeline)))))

(deftest selected-receiver-statistics-use-durable-receipt-timeline
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((runtime (pg::make-physical-follow-runtime))
             (pg::*physical-follow-runtime* runtime)
             (pg::*physical-follow-statistics* (list :runtime runtime)))
        (ok (= 1 (getf (pg::physical-follow-statistics) :received-tli)))
        (selected-replay-test-prepare store)
        (ok (= 1 (pg::physical-recovery-replay-timeline)))
        (ok (= 2 (getf (pg::physical-follow-statistics) :received-tli)))
        (setf pg::*physical-follow-runtime* nil)
        (ok (null (pg::physical-follow-statistics))))))
