;;;; Source membership belongs to an applied recovery boundary.
(in-package #:pg-physical-tests)

(defun source-xids-record (type start payload &optional (xid 0))
  (pg::wal-decode-record (pg::wal-encode-record type xid start payload) start))

(deftest source-xids-follow-ordered-anchors-gaps-and-commit-terminals
    (let* ((pg::*active-xids* '(999))
           (anchor (source-xids-record pg::+wal-record-running-xacts+ 100
                                       (pg::wal-encode-running-xacts 50 '(3 49) t)))
           (state (pg::physical-source-xids-next nil anchor 50))
           (marker (source-xids-record pg::+wal-record-batch-start+
                                       (pg::wal-record-end anchor) (empty-octets)))
           (gap (pg::physical-source-xids-next state marker 55))
           (commit (source-xids-record pg::+wal-record-commit+
                                       (pg::wal-record-end marker) (empty-octets) 3))
           (after (pg::physical-source-xids-next gap commit 55)))
      (ok (equal '(3 49) (getf state :running)))
      (ok (equal '(3 49 50 51 52 53 54) (getf gap :running)))
      (ok (equal '(49 50 51 52 53 54) (getf after :running)))
      (ok (equal '(999) pg::*active-xids*))
      (ok (= (pg::wal-record-end commit) (getf after :through)))
      (ok (= (pg::wal-record-end anchor) (getf after :anchor)))
      (ok (intake-fails-p (lambda () (pg::physical-source-xids-next after marker 55))))
      (ok (null (pg::physical-source-xids-next nil marker 55)))))

(deftest source-xids-anchors-do-not-use-target-clog-or-reservation-capacity
    (let* ((clog (symbol-function 'pg::clog-get))
           (anchor (source-xids-record pg::+wal-record-running-xacts+ 100
                                       (pg::wal-encode-running-xacts 50 '(3 49) t))))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::clog-get)
                   (lambda (xid) (declare (ignore xid)) (error "CLOG must not filter source membership")))
             (let* ((state (pg::physical-source-xids-next nil anchor 50))
                    (capacity (source-xids-record pg::+wal-record-xid-reservation+
                                                  (pg::wal-record-end anchor)
                                                  (pg::wal-encode-xid-reservation 1002)))
                    (after (pg::physical-source-xids-next state capacity 50)))
               (ok (= 50 (getf after :next-xid)))
               (ok (equal '(3 49) (getf after :running))))
             (ok (intake-fails-p (lambda () (pg::physical-source-xids-next nil anchor 51)))))
        (setf (symbol-function 'pg::clog-get) clog))))

(deftest source-xids-legacy-and-reset-records-drop-coverage
    (let* ((state (pg::running-xacts-anchor
                   (list :next-xid 50 :running '(3) :complete t) 100))
           (legacy (pg::make-wal-record :version 6 :type pg::+wal-record-batch-start+
                                        :start 100 :end 132 :xid 0 :payload (empty-octets)))
           (unknown (pg::physical-source-xids-next state legacy 50)))
      (ok (not (getf unknown :complete)))
      (ok (null (getf unknown :running)))
      (ok (getf state :complete))
      (setf (pg::wal-record-version legacy) 7 (pg::wal-record-type legacy) pg::+wal-record-clog-reset+)
      (ok (null (pg::physical-source-xids-next state legacy 50)))))

(deftest restartpoint-source-membership-is-optional-and-bound-to-counter-and-lsn
    (with-restartpoint-fixture (root store)
      (declare (ignore root store))
      (let* ((catalog (pg::recovery-read-bytes (pg::catalog-path)))
             (metadata (pg::physical-restartpoint-metadata 77 catalog))
             (source (pg::running-xacts-anchor
                      (list :next-xid 50 :running '(3 49) :complete t) 64)))
        (setf (getf source :through) 96
              (getf metadata :source-xids) source)
        (multiple-value-bind (decoded bytes)
            (pg::physical-restartpoint-decode (pg::physical-restartpoint-encode metadata catalog))
          (ok (pg::physical-restartpoint-check-metadata decoded bytes pg::*physical-recovery-state*))
          (ok (equal source (getf decoded :source-xids))))
        (dolist (mutation '((:next-xid 51) (:through 95) (:anchor 31) (:running (3 50))))
          (let ((bad (copy-tree metadata)))
            (setf (getf (getf bad :source-xids) (first mutation)) (second mutation))
            (ok (intake-fails-p
                 (lambda () (pg::physical-restartpoint-check-metadata
                             bad catalog pg::*physical-recovery-state*))))))
        (remf metadata :source-xids)
        (ok (pg::physical-restartpoint-check-metadata metadata catalog pg::*physical-recovery-state*))
        (setf (getf metadata :unexpected) nil)
        (ok (intake-fails-p
             (lambda () (pg::physical-restartpoint-check-metadata
                         metadata catalog pg::*physical-recovery-state*)))))))

(deftest restartpoint-restores-source-membership-even-with-future-clog
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((source (pg::running-xacts-anchor
                     (list :next-xid 50 :running '(3 49) :complete t) 64)))
        (setf (getf source :through) 96
              (getf pg::*physical-recovery-state* :source-xids) source)
        (pg::clog-init)
        (unwind-protect
             (progn
               (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
               (pg::clog-set 3 1 :fsync t)
               (setf (getf pg::*physical-recovery-state* :source-xids) nil
                     pg::*next-xid* 60)
               (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
               (ok (= 50 pg::*next-xid*))
               (ok (= 1 (pg::clog-get 3)))
               (ok (equal '(3 49) (getf (getf pg::*physical-recovery-state* :source-xids) :running)))
               (ok (getf (getf pg::*physical-recovery-state* :source-xids) :complete)))
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(deftest legacy-or-missing-generation-clears-unrestored-source-coverage
    (dolist (generation '(nil t))
      (with-restartpoint-fixture (root store)
        (declare (ignore root))
        (when generation
          (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish))
        (setf (getf pg::*physical-recovery-state* :source-xids)
              (pg::running-xacts-anchor
               (list :next-xid 50 :running '(3 49) :complete t) 96))
        (pg::physical-restartpoint-prepare-storage store)
        (ok (null (getf pg::*physical-recovery-state* :source-xids))))))

(deftest restartpoint-preserves-explicit-incomplete-membership
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((source (pg::running-xacts-anchor
                     (list :next-xid 50 :running nil :complete nil) 64)))
        (setf (getf source :through) 96
              (getf pg::*physical-recovery-state* :source-xids) source)
        (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
        (setf (getf pg::*physical-recovery-state* :source-xids) nil)
        (pg::physical-restartpoint-prepare-storage store)
        (let ((restored (getf pg::*physical-recovery-state* :source-xids)))
          (ok restored)
          (ok (= 50 (getf restored :next-xid)))
          (ok (null (getf restored :running)))
          (ok (null (getf restored :complete))))
        (let* ((catalog (pg::recovery-read-bytes (pg::catalog-path)))
               (metadata (pg::physical-restartpoint-metadata 77 catalog)))
          (ok (eq :version (first metadata)))
          (setf (getf metadata :source-xids) nil)
          (multiple-value-bind (decoded bytes)
              (pg::physical-restartpoint-decode (pg::physical-restartpoint-encode metadata catalog))
            (ok (pg::physical-restartpoint-check-metadata decoded bytes pg::*physical-recovery-state*))
            (ok (null (getf decoded :source-xids))))))))

(cl-spec:defproperty source-xids-progress-retains-every-unfinished-transaction
    ((first-gap (range integer 0 50))
     (second-gap (range integer 0 50))
     (selector (range integer 0 1)))
  (:about pg::physical-source-xids-next)
  (:trials (:normal 200))
  (let* ((state (pg::running-xacts-anchor
                 (list :next-xid 10 :running '(3 7) :complete t) 100))
         (marker (source-xids-record pg::+wal-record-batch-start+ 100 (empty-octets)))
         (commit (source-xids-record pg::+wal-record-commit+ 132 (empty-octets)
                                     (if (zerop selector) 3 7)))
         (end (+ 10 first-gap second-gap))
         (after (pg::physical-source-xids-next
                 (pg::physical-source-xids-next state marker (+ 10 first-gap)) commit end))
         (remaining (getf after :running)))
    (and (getf after :complete)
         (= end (getf after :next-xid))
         (not (member (pg::wal-record-xid commit) remaining))
         (member (if (zerop selector) 7 3) remaining)
         (loop for xid from 10 below end always (member xid remaining))
         (equal '(3 7) (getf state :running)))))

(deftest generated-source-xids-progress
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'source-xids-progress-retains-every-unfinished-transaction
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest source-xids-publishes-only-applied-terminals-in-both-replay-paths
    (dolist (horizon '(nil 0 56))
      (with-restartpoint-fixture (root store)
        (let* ((pg::*physical-replay-restarting-p* t)
               (pg::*catalog* (pg::load-catalog))
               (pg::*wal-page-cache* (make-hash-table :test #'equal))
               (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
               (pg::*active-xids* '(999))
               (page (make-array 8192 :element-type '(unsigned-byte 8) :initial-element 0))
               (anchor (source-xids-record pg::+wal-record-running-xacts+ 128
                                           (pg::wal-encode-running-xacts 50 '(3 49) t)))
               (end (pg::wal-record-end anchor))
               (image (pg::make-wal-page-image :relfilenode 900 :block 0 :page page
                                               :xids '(55) :provenance :row))
               (pages (source-xids-record pg::+wal-record-pages+ (+ end 32)
                                          (pg::wal-encode-page-images
                                           (list image) (+ end 32 pg::+wal-header-size+
                                                           (pg::wal-page-payload-length (list image)))))))
          (pg::wal-atomic-write-bytes (merge-pathnames "base/900" root) page)
          (pg::clog-init)
          (unwind-protect
               (progn
                 (dolist (record (list (source-xids-record pg::+wal-record-batch-start+ 96 (empty-octets))
                                       anchor
                                       (source-xids-record pg::+wal-record-batch-start+ end (empty-octets))
                                       pages))
                   (pg::physical-store-input store (pg::wal-record-start record)
                                             (pg::wal-encode-record
                                              (pg::wal-record-type record) (pg::wal-record-xid record)
                                              (pg::wal-record-start record) (pg::wal-record-payload record))))
                 (pg::physical-store-flush store)
                 (if horizon (pg::physical-row-replay-prefix horizon) (pg::physical-replay-from 96))
                 (ok (= end pg::*last-replay-lsn*))
                 (ok (= 50 pg::*next-xid*))
                 (ok (equal '(3 49) (getf (getf pg::*physical-recovery-state* :source-xids) :running)))
                 (ok (= end (getf (getf pg::*physical-recovery-state* :source-xids) :through)))
                 (let* ((start (pg::wal-record-end pages))
                        (raw (pg::wal-encode-record pg::+wal-record-commit+ 55 start (empty-octets)))
                        (finish (+ start (length raw))))
                   (pg::physical-store-input store start raw)
                   (pg::physical-store-flush store)
                   (if horizon
                       (let ((result (pg::physical-row-replay-prefix horizon)))
                         (when (= horizon 56)
                           (ok (eq :deferred result))
                           (ok (= end pg::*last-replay-lsn*))
                           (ok (= 50 pg::*next-xid*))
                           (ok (equal '(3 49)
                                      (getf (getf pg::*physical-recovery-state* :source-xids) :running)))
                           (pg::physical-replay-from end)))
                       (pg::physical-replay-from end))
                   (ok (= finish pg::*last-replay-lsn*))
                   (ok (= 56 pg::*next-xid*))
                   (ok (equal '(3 49 50 51 52 53 54)
                              (getf (getf pg::*physical-recovery-state* :source-xids) :running)))
                   (ok (= finish (getf (getf pg::*physical-recovery-state* :source-xids) :through)))
                   (ok (= 1 (pg::clog-get 55)))
                   (ok (equal '(999) pg::*active-xids*))))
            (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))))
