;;;; Covered standby snapshots keep their source exclusion set.
(in-package #:pg-physical-tests)

(deftest standby-snapshot-copies-only-complete-applied-source-membership
    (let* ((source (pg::running-xacts-anchor
                    (list :next-xid 50 :running '(3 49) :complete t) 100))
           (pg::*physical-recovery-state* (list :phase 2 :source-xids source))
           (pg::*last-replay-lsn* 100) (pg::*next-xid* 50) (pg::*active-xids* '(10))
           (snapshot (pg::make-snapshot 7)))
      (ok (pg::snapshot-source-xids-complete-p snapshot))
      (ok (= 50 (pg::snapshot-xmax snapshot)))
      (ok (= 3 (pg::snapshot-xmin snapshot)))
      (ok (= 7 (pg::snapshot-curcid snapshot)))
      (ok (equal '(3 49) (pg::snapshot-xip snapshot)))
      (setf (first (getf source :running)) 7)
      (ok (equal '(3 49) (pg::snapshot-xip snapshot)))
      (setf (getf source :complete) nil (getf source :running) nil)
      (ok (not (pg::snapshot-source-xids-complete-p (pg::make-snapshot 8))))
      (ok (pg::snapshot-source-xids-complete-p snapshot))
      (setf (getf source :complete) t (getf source :next-xid) 51)
      (ok (intake-fails-p (lambda () (pg::make-snapshot 8))))
      (setf (getf source :next-xid) 50 (getf source :through) 101)
      (ok (intake-fails-p (lambda () (pg::make-snapshot 8))))
      (let ((pg::*physical-recovery-state* nil))
        (let ((primary (pg::make-snapshot 9)))
          (ok (not (pg::snapshot-source-xids-complete-p primary)))
          (ok (equal '(10) (pg::snapshot-xip primary)))))))

(deftest standby-snapshot-keeps-running-writers-invisible-after-clog-commit
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*clog-stream* nil)
                                 (source (pg::running-xacts-anchor
                                          (list :next-xid 50 :running '(3 49) :complete t) 100))
                                 (pg::*physical-recovery-state* (list :phase 2 :source-xids source))
                                 (pg::*last-replay-lsn* 100) (pg::*next-xid* 50) (pg::*active-xids* nil))
        (pg::clog-init)
        (unwind-protect
             (let ((snapshot (pg::make-snapshot 1)))
               (pg::clog-set 3 1)
               (pg::clog-set 4 1)
               (ok (not (pg::heap-tuple-visible-p 3 0 0 snapshot nil)))
               (ok (pg::heap-tuple-visible-p 4 3 0 snapshot nil))
               (ok (pg::heap-tuple-visible-p 4 0 0 snapshot nil))
               (ok (not (pg::heap-tuple-visible-p 4 4 0 snapshot nil))))
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(deftest rr-statement-snapshots-preserve-coverage-without-upgrading
    (dolist (covered '(nil t))
      (let* ((source (pg::running-xacts-anchor
                      (list :next-xid 50 :running '(3 49) :complete t) 100))
             (pg::*physical-recovery-state* (list :phase 2))
             (pg::*last-replay-lsn* 100) (pg::*next-xid* 50) (pg::*active-xids* nil)
             (pg::*active-snapshots* 0) (pg::*active-snapshot-set* (make-hash-table :test #'eq))
             (backend (pg::make-backend))
             (registration (pg::make-snapshot-registration)))
        (when covered (setf (getf pg::*physical-recovery-state* :source-xids) source))
        (unwind-protect
             (let ((retained (pg::ensure-transaction-snapshot backend 1)))
               (setf (pg::snapshot-sequence-database-oid retained) 7
                     (pg::snapshot-sequence-state-captured-p retained) t
                     (pg::snapshot-sequence-states retained) (list (list 7 9)))
               (setf (getf pg::*physical-recovery-state* :source-xids)
                     (if covered nil source))
               (let ((statement (pg::begin-transaction-snapshot backend 2 registration)))
                 (ok (eql covered (pg::snapshot-source-xids-complete-p retained)))
                 (ok (eql covered (pg::snapshot-source-xids-complete-p statement)))
                 (ok (equal (pg::snapshot-xip retained) (pg::snapshot-xip statement)))
                 (ok (= 2 (pg::snapshot-curcid statement)))
                 (ok (null (pg::snapshot-sequence-database-oid statement)))
                 (ok (null (pg::snapshot-sequence-state-captured-p statement)))
                 (ok (null (pg::snapshot-sequence-states statement)))))
          (pg::release-snapshot-registration registration)
          (pg::release-backend-transaction-snapshot backend)))))

(deftest mixed-snapshot-horizon-only-counts-incomplete-coverage
    (let* ((pg::*active-snapshots* 0) (pg::*active-snapshot-set* (make-hash-table :test #'eq))
                                      (old (pg::make-snapshot-struct :xmin 3 :xmax 30))
                                      (covered (pg::make-snapshot-struct :xmin 3 :xmax 80 :xip '(3 50)
                                                                         :source-xids-complete-p t))
                                      (old-pin (pg::make-snapshot-registration))
                                      (covered-pin (pg::make-snapshot-registration)))
      (unwind-protect
           (progn
             (sb-thread:with-mutex (pg::*xid-lock*)
               (pg::register-active-snapshot old old-pin)
               (pg::register-active-snapshot covered covered-pin))
             (multiple-value-bind (horizon snapshots) (pg::physical-row-replay-reader-horizon)
               (ok (= 30 horizon))
               (ok (equal (list covered) snapshots)))
             (pg::release-snapshot-registration old-pin)
             (multiple-value-bind (horizon snapshots) (pg::physical-row-replay-reader-horizon)
               (ok (zerop horizon))
               (ok (equal (list covered) snapshots)))
             (ok (pg::physical-row-replay-images-allowed-p
                  (list (pg::make-wal-page-image :xids '(4 50) :provenance :row)) 0))
             (pg::physical-row-replay-check-covered-commit
              (row-replay-test-record pg::+wal-record-commit+ 50) (list covered))
             (ok (intake-fails-p
                  (lambda () (pg::physical-row-replay-check-covered-commit
                              (row-replay-test-record pg::+wal-record-commit+ 4) (list covered))))))
        (pg::release-snapshot-registration old-pin)
        (pg::release-snapshot-registration covered-pin))))

(deftest imported-snapshot-copies-coverage-and-owns-its-exclusion-list
    (let* ((pg::*active-snapshots* 0) (pg::*active-snapshot-set* (make-hash-table :test #'eq))
                                      (pg::*exported-logical-snapshots* (make-hash-table :test #'equal))
                                      (pg::*catalog* (pg::make-catalog))
                                      (database (pg::make-database :name "postgres" :oid 7))
                                      (pg::*backend* (pg::make-backend :database "postgres" :state :in-progress
                                                                       :explicit t :isolation-level :repeatable-read :read-only t))
                                      (source (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip (list 3 49) :curcid 1
                                                                        :source-xids-complete-p t
                                                                        :sequence-database-oid 7 :sequence-state-captured-p t
                                                                        :sequence-states (list (list 7 9))))
                                      (pin (pg::make-snapshot-registration))
                                      (state (pg::make-exported-logical-snapshot :token "covered" :database-oid 7
                                                                                 :snapshot source :registration pin)))
      (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
            (gethash "covered" pg::*exported-logical-snapshots*) state)
      (unwind-protect
           (progn
             (sb-thread:with-mutex (pg::*xid-lock*) (pg::register-active-snapshot source pin))
             (pg::import-exported-logical-snapshot "covered")
             (let ((imported (pg::backend-transaction-snapshot pg::*backend*)))
               (ok (pg::snapshot-source-xids-complete-p imported))
               (ok (equal '(3 49) (pg::snapshot-xip imported)))
               (setf (first (pg::snapshot-xip source)) 7)
               (ok (equal '(3 49) (pg::snapshot-xip imported)))
               (ok (= 7 (pg::snapshot-sequence-database-oid imported)))
               (ok (pg::snapshot-sequence-state-captured-p imported))
               (setf (second (first (pg::snapshot-sequence-states source))) 10)
               (ok (equal '((7 9)) (pg::snapshot-sequence-states imported)))))
        (pg::release-backend-transaction-snapshot pg::*backend*)
        (pg::release-snapshot-registration pin))))

(deftest broken-covered-commit-fences-before-page-or-clog-mutation
    (with-restartpoint-fixture (root store)
      (let* ((pg::*catalog* (pg::load-catalog))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
             (pg::*active-snapshots* 0) (pg::*active-snapshot-set* (make-hash-table :test #'eq))
             (pg::*backend* nil) (pg::*swank-listener-enabled-p* nil)
             (source (pg::running-xacts-anchor
                      (list :next-xid 50 :running '(3 49) :complete t) 64))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip '(3 49)
                                                 :source-xids-complete-p t))
             (pin (pg::make-snapshot-registration))
             (before (pg::make-page))
             (after (pg::make-page))
             (image (pg::make-wal-page-image :relfilenode 900 :block 0 :page after
                                             :xids '(4) :provenance :row))
             (end (+ 128 pg::+wal-header-size+ (pg::wal-page-payload-length (list image)))))
        (setf (getf source :through) 96
              (getf pg::*physical-recovery-state* :source-xids) source
              (aref after 200) 123)
        (pg::wal-atomic-write-bytes (merge-pathnames "base/900" root) before)
        (pg::physical-store-input store 96
                                  (pg::wal-encode-record pg::+wal-record-batch-start+ 0 96 (empty-octets)))
        (pg::physical-store-input store 128
                                  (pg::wal-encode-record pg::+wal-record-pages+ 0 128
                                                         (pg::wal-encode-page-images (list image) end)))
        (pg::physical-store-input store end
                                  (pg::wal-encode-record pg::+wal-record-commit+ 4 end (empty-octets)))
        (pg::physical-store-flush store)
        (pg::clog-init)
        (unwind-protect
             (progn
               (sb-thread:with-mutex (pg::*xid-lock*) (pg::register-active-snapshot snapshot pin))
               (let ((message (handler-case
                                  (progn (pg::physical-recovery-replay) nil)
                                (error (condition) (princ-to-string condition)))))
                 (ok (and message (search "violates retained snapshot coverage" message))))
               (ok pg::*physical-recovery-failed-p*)
               (ok (= 96 pg::*last-replay-lsn*))
               (ok (= 50 pg::*next-xid*))
               (ok (equal source (getf pg::*physical-recovery-state* :source-xids)))
               (ok (= 0 (pg::clog-get 4)))
               (ok (equalp before (pg::recovery-read-bytes (merge-pathnames "base/900" root)))))
          (pg::release-snapshot-registration pin)
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(cl-spec:defproperty mixed-snapshots-exclude-every-newly-committed-writer
    ((xid (range integer 3 1000000))
     (xmax (range integer 3 1000000))
     (listed (range integer 0 1))
     (strict (range integer 0 1000000)))
  (:about pg::physical-row-replay-check-covered-commit)
  (:trials (:normal 200))
  (let* ((snapshot (pg::make-snapshot-struct :xmin 3 :xmax xmax
                                             :xip (when (= listed 1) (list xid))
                                             :source-xids-complete-p t))
         (record (row-replay-test-record pg::+wal-record-commit+ xid)))
    (eql (and (>= xid strict) (or (>= xid xmax) (= listed 1)))
         (and (not (null (pg::physical-row-replay-allowed-p record strict)))
              (not (intake-fails-p
                    (lambda () (pg::physical-row-replay-check-covered-commit record (list snapshot)))))))))

(deftest generated-mixed-snapshot-exclusion
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'mixed-snapshots-exclude-every-newly-committed-writer
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
