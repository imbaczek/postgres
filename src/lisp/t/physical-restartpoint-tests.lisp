;;;; Restartpoint generations bind durable data to a validated receive suffix.
(in-package #:pg-physical-tests)

(defun call-with-restartpoint-fixture (function &key (system-id 123))
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (pg::*physical-recovery-state*
             (list :system-id system-id :timeline 1 :manifest-digest 77 :end 32 :checkpoint 32
                   :cluster-identity "history-source" :next-xid 50 :required-applied 96 :observed-next-xid 50))
           (pg::*physical-recovery-failed-p* nil)
           (pg::*last-replay-lsn* 96) (pg::*next-xid* 50) (pg::*xid-batch-end* 50)
           (pg::*wal-checkpoint-lsn* 32) (pg::*catalog* nil) (pg::*clog-stream* nil)
           (pg::*relation-streams* (make-hash-table :test #'equal))
           (pg::*physical-replay-seen-relfilenodes* (make-hash-table :test #'eql))
           (spool (merge-pathnames "spool/" root)))
      (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
      (pg::wal-ensure-directory-durable (merge-pathnames "global/" root))
      (pg::wal-ensure-directory-durable spool)
      (let ((pg::*xid-batch-end* 1003)) (pg::save-next-xid))
      (setf (gethash 900 pg::*physical-replay-seen-relfilenodes*) t)
      (multiple-value-bind (deletes bytes) (pg::wal-decode-catalog-payload
                                            (history-catalog-payload '(900) nil))
        (declare (ignore deletes))
        (pg::wal-atomic-write-bytes (pg::catalog-path) bytes))
      (pg::wal-atomic-write-bytes (merge-pathnames "base/900" root)
                                  (make-array 32 :element-type '(unsigned-byte 8) :initial-element 0))
      (let* ((base (sample-physical-record 0))
             (name "pg_wal/0000000000000000.wal"))
        (pg::wal-atomic-write-bytes (merge-pathnames name root) base)
        (setf (getf pg::*physical-recovery-state* :wal-namespace) :legacy
              (getf pg::*physical-recovery-state* :files)
              (list (list name 32 (pg::physical-store-update-digest #xCBF29CE484222325 base)))))
      (let ((store (pg::physical-store-open spool system-id 1 :create t :start-lsn 32)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-input store 64
                                         (legacy-checkpoint-record 64))
               (pg::physical-store-flush store)
               (let ((pg::*physical-receive-store* store)) (funcall function root store)))
          (pg::physical-store-close store))))))

(defmacro with-restartpoint-fixture ((root store) &body body)
  `(call-with-restartpoint-fixture (lambda (,root ,store) ,@body)))

(defun append-resource-test-parameter (store connections)
  (let* ((start (pg::physical-store-flushed-lsn store))
         (record-start (+ start 32))
         (payload (pg::wal-encode-resource-parameters
                   (pg::make-resource-parameters connections 8 10 0 128))))
    (pg::physical-store-input store start (sample-physical-record start))
    (pg::physical-store-input store record-start
                              (pg::wal-encode-record pg::+wal-record-parameter-change+ 0 record-start payload))
    (pg::physical-store-flush store)
    (pg::physical-store-flushed-lsn store)))

(defun call-with-resource-restartpoint-fixture (function)
  (with-restartpoint-fixture (root store)
    (let ((pg::*wal-page-cache* (make-hash-table :test #'equal))
          (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
          (pg::*wal-dirty-store* nil)
          (pg::*physical-replay-restarting-p* t))
      (append-resource-test-parameter store 100)
      (pg::physical-replay-from 96)
      (funcall function root store))))

(defun publish-resource-test-generation (store &key legacy compact)
  (let ((before pg::*physical-recovery-state*))
    (unwind-protect
         (progn
           (when legacy
             (setf pg::*physical-recovery-state* (copy-list before))
             (remf pg::*physical-recovery-state* :source-resource-fact))
           (if compact
               (pg::physical-store-compact store pg::*last-replay-lsn* #'pg::physical-restartpoint-publish)
               (pg::physical-restartpoint-publish
                (pg::physical-store-prefix-digest store pg::*last-replay-lsn*))))
      (when legacy (setf pg::*physical-recovery-state* before)))))

(deftest follow-historical-resource-witness-distinguishes-unknown-from-legacy
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (let* ((state (copy-tree pg::*physical-recovery-state*))
              (fact (copy-tree (getf state :source-resource-fact)))
              (seed (list 192 1 pg::*next-xid* (getf state :source-checkpoint)
                          (getf state :source-reservation) (getf state :source-xids) fact)))
         (flet ((scan (witness)
                  (pg::physical-follow-scan-replay-from-seed
                   store state '(192 1) '(192 1) pg::*next-xid* seed
                   (lambda (function start)
                     (pg::physical-store-map-durable-records store function :start-lsn start))
                   nil witness)))
           (ok (scan (list 192 1 pg::*next-xid* fact)))
           (ok (scan (list 192 1 pg::*next-xid*)))
           (ok (intake-fails-p (lambda () (scan (list 192 1 pg::*next-xid* nil))))))))))

(deftest follow-resource-scan-proves-a-required-decrease-and-rejects-a-forged-fact
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (let* ((state (copy-tree pg::*physical-recovery-state*))
              (fact (copy-tree (getf state :source-resource-fact)))
              (required (list :resource-parameters (list 50 8 10 0 128)
                              :resource-lsn 288 :resource-timeline 1))
              (seed (list 192 1 pg::*next-xid* (getf state :source-checkpoint)
                          (getf state :source-reservation) (getf state :source-xids) fact))
              (before (lineage-test-bytes (pg::physical-recovery-control-path root))))
         (append-resource-test-parameter store 50)
         (setf (getf state :required-resource-fact) required)
         (flet ((scan ()
                  (pg::physical-follow-scan-replay-from-seed
                   store state '(192 1) '(288 1) pg::*next-xid* seed
                   (lambda (function start)
                     (pg::physical-store-map-durable-records store function :start-lsn start))
                   nil)))
           (let ((result (scan)))
             (ok (equal fact (getf result :applied-resource-fact)))
             (ok (equal required (getf result :required-resource-fact))))
           (setf (first (getf required :resource-parameters)) 75)
           (ok (intake-fails-p #'scan))
           (setf (getf state :required-resource-fact) nil)
           (ok (intake-fails-p #'scan))
           (ok (= 192 pg::*last-replay-lsn*))
           (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path root))))
           (ok (equal fact (getf pg::*physical-recovery-state* :source-resource-fact))))))))

(deftest follow-resource-scan-keeps-unapplied-tail-out-of-applied-facts
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (let* ((state (copy-tree pg::*physical-recovery-state*))
              (fact (copy-tree (getf state :source-resource-fact)))
              (seed (list 192 1 pg::*next-xid* (getf state :source-checkpoint)
                          (getf state :source-reservation) (getf state :source-xids) fact)))
         (append-resource-test-parameter store 50)
         (let ((result (pg::physical-follow-scan-replay-from-seed
                        store state '(192 1) '(192 1) pg::*next-xid* seed
                        (lambda (function start)
                          (pg::physical-store-map-durable-records store function :start-lsn start))
                        nil)))
           (ok (= 288 (getf result :terminal)))
           (ok (equal fact (getf result :applied-resource-fact)))
           (ok (equal fact (getf result :required-resource-fact)))
           (ok (= 192 pg::*last-replay-lsn*))
           (ok (equal fact (getf pg::*physical-recovery-state* :source-resource-fact))))))))

(deftest restartpoint-resources-restore-older-seed-without-borrowing-required-decrease
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (let ((older (copy-tree (getf pg::*physical-recovery-state* :source-resource-fact))))
         (ok (= 192 pg::*last-replay-lsn*))
         (publish-resource-test-generation store :compact t)
         (append-resource-test-parameter store 50)
         (pg::physical-replay-from 192)
         (let ((required (copy-tree (getf pg::*physical-recovery-state* :required-resource-fact))))
           (ok (= 288 pg::*last-replay-lsn*))
           (ok (equal '(50 8 10 0 128) (getf required :resource-parameters)))
           (dotimes (attempt 2)
             (declare (ignorable attempt))
             (setf (getf pg::*physical-recovery-state* :source-resource-fact) (copy-tree required))
             (ok (= 192 (pg::physical-restartpoint-prepare-storage store)))
             (ok (equal older (getf pg::*physical-recovery-state* :source-resource-fact)))
             (ok (equal required (getf pg::*physical-recovery-state* :required-resource-fact)))
             (setf pg::*physical-recovery-state*
                   (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 192 pg::*next-xid*))
             (let ((control (pg::physical-recovery-read-control root)))
               (ok (= 288 (getf control :applied)))
               (ok (equal required (getf control :resource-fact)))))
           (pg::physical-replay-from 192)
           (ok (= 288 pg::*last-replay-lsn*))
           (ok (equal required (getf pg::*physical-recovery-state* :source-resource-fact))))))))

(deftest legacy-resource-seed-rehydrates-facts-without-rewinding-its-data-counters
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (let* ((start (pg::physical-store-flushed-lsn store))
              (record-start (+ start 32))
              (payload (pg::wal-encode-running-xacts 80 nil t)))
         (pg::physical-store-input store start (sample-physical-record start))
         (pg::physical-store-input store record-start
                                   (pg::wal-encode-record pg::+wal-record-running-xacts+ 0 record-start payload))
         (pg::physical-store-flush store)
         (pg::physical-replay-from start))
       (let ((seed pg::*last-replay-lsn*)
             (fact (copy-tree (getf pg::*physical-recovery-state* :source-resource-fact))))
         (publish-resource-test-generation store :legacy t)
         (setf (getf pg::*physical-recovery-state* :source-resource-fact) nil pg::*next-xid* 50)
         (ok (= seed (pg::physical-restartpoint-prepare-storage store)))
         (ok (= 80 pg::*next-xid*))
         (ok (equal fact (getf pg::*physical-recovery-state* :source-resource-fact)))
         (ok (= seed (getf pg::*physical-recovery-state* :restartpoint-lsn)))))))

(deftest legacy-resource-seed-below-required-waits-for-real-forward-parameter-proof
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (publish-resource-test-generation store :legacy t :compact t)
       (append-resource-test-parameter store 50)
       (pg::physical-replay-from 192)
       (let ((minimum (copy-tree (getf pg::*physical-recovery-state* :required-resource-fact))))
         (setf (getf pg::*physical-recovery-state* :source-resource-fact) nil)
         (ok (= 192 (pg::physical-restartpoint-prepare-storage store)))
         (ok (null (getf pg::*physical-recovery-state* :source-resource-fact)))
         (ok (equal minimum (getf pg::*physical-recovery-state* :required-resource-fact)))
         (pg::physical-replay-from 192)
         (ok (equal minimum (getf pg::*physical-recovery-state* :source-resource-fact)))))))

(deftest legacy-resource-seed-cannot-borrow-an-anchor-after-the-required-boundary
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (publish-resource-test-generation store :legacy t :compact t)
       (pg::physical-store-input store 192 (sample-physical-record 192))
       (pg::physical-store-input store 224 (pg::wal-encode-record pg::+wal-record-maintenance+ 0 224 (empty-octets)))
       (pg::physical-store-flush store)
       (pg::physical-replay-from 192)
       (let* ((fact (copy-tree (getf pg::*physical-recovery-state* :source-resource-fact)))
              (snapshot (append '(:version 2 :start 288 :end 456 :redo 288 :next-xid 50
                                  :xid-reserved-through 1002 :next-oid nil :time 0
                                  :timeline 1 :prev-timeline 1 :oldest-active-xid 0) fact)))
         (pg::physical-store-input store 256 (sample-physical-record 256))
         (pg::physical-store-input store 288
                                   (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 288
                                                          (pg::wal-encode-checkpoint-snapshot snapshot)))
         (pg::physical-store-flush store)
         (setf (getf pg::*physical-recovery-state* :source-resource-fact) nil)
         (let ((before-state (copy-tree pg::*physical-recovery-state*))
               (control (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))
               (pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path)))
               (catalog (pg::recovery-read-bytes (pg::catalog-path))))
           (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
           (ok (equal before-state pg::*physical-recovery-state*))
           (ok (equalp control (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
           (ok (equalp pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path))))
           (ok (equalp catalog (pg::recovery-read-bytes (pg::catalog-path)))))))))

(defun append-resource-test-checkpoint (store)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin 32))
         (fact (pg::wal-copy-control-resource-fields (getf pg::*physical-recovery-state* :source-resource-fact)))
         (snapshot (append (list :version 2 :start start :end (+ start 168) :redo start
                                 :next-xid pg::*next-xid* :xid-reserved-through 1002 :next-oid nil :time 0
                                 :timeline 1 :prev-timeline 1 :oldest-active-xid 0) fact)))
    (pg::physical-store-input store begin (sample-physical-record begin))
    (pg::physical-store-input store start
                              (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                     (pg::wal-encode-checkpoint-snapshot snapshot)))
    (pg::physical-store-flush store)
    (pg::physical-store-flushed-lsn store)))

(deftest legacy-resource-seed-uses-a-checkpoint-anchor-at-the-required-boundary
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (publish-resource-test-generation store :legacy t :compact t)
       (append-resource-test-checkpoint store)
       (pg::physical-replay-from 192)
       (let ((minimum (copy-tree (getf pg::*physical-recovery-state* :required-resource-fact))))
         (ok (= 392 pg::*last-replay-lsn*))
         (setf (getf pg::*physical-recovery-state* :source-resource-fact) nil
               (getf pg::*physical-recovery-state* :source-checkpoint) nil)
         (ok (= 192 (pg::physical-restartpoint-prepare-storage store)))
         (ok (null (getf pg::*physical-recovery-state* :source-resource-fact)))
         (ok (null (getf pg::*physical-recovery-state* :source-checkpoint)))
         (pg::physical-replay-from 192)
         (ok (= 392 pg::*last-replay-lsn*))
         (ok (equal minimum (getf pg::*physical-recovery-state* :source-resource-fact)))))))

(deftest legacy-resource-seed-at-required-needs-a-retained-or-exact-seed-proof
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (publish-resource-test-generation store :legacy t :compact t)
       (setf (getf pg::*physical-recovery-state* :source-resource-fact) nil)
       (let ((before (copy-tree pg::*physical-recovery-state*))
             (catalog (pg::recovery-read-bytes (pg::catalog-path)))
             (control (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
         (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
         (ok (equal before pg::*physical-recovery-state*))
         (ok (equalp catalog (pg::recovery-read-bytes (pg::catalog-path))))
         (ok (equalp control (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))))))

(deftest newer-legacy-seed-can-prove-a-decrease-from-its-exact-checkpoint
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (let ((older (copy-tree (getf pg::*physical-recovery-state* :source-resource-fact))))
         (append-resource-test-parameter store 50)
         (pg::physical-replay-from 192)
         (append-resource-test-checkpoint store)
         (pg::physical-replay-from 288)
         (ok (= 488 pg::*last-replay-lsn*))
         (let ((decreased (copy-tree (getf pg::*physical-recovery-state* :source-resource-fact))))
           (publish-resource-test-generation store :legacy t :compact t)
           (pg::wal-atomic-write-bytes
            (pg::physical-recovery-control-path root)
            (pg::physical-recovery-control-image 2 123 1 77 32 192 50 :resource-fact older))
           (setf (getf pg::*physical-recovery-state* :required-applied) 192
                 (getf pg::*physical-recovery-state* :required-resource-fact) older
                 (getf pg::*physical-recovery-state* :source-resource-fact) nil
                 (getf pg::*physical-recovery-state* :source-checkpoint) nil)
           (ok (= 488 (pg::physical-restartpoint-prepare-storage store)))
           (ok (= 488 (getf pg::*physical-recovery-state* :required-applied)))
           (ok (equal decreased (getf pg::*physical-recovery-state* :source-resource-fact)))
           (ok (equal decreased (getf pg::*physical-recovery-state* :required-resource-fact)))
           (pg::physical-recovery-save-progress 488 pg::*next-xid*)
           (let ((control (pg::physical-recovery-read-control root)))
             (ok (= 488 (getf control :applied)))
             (ok (equal decreased (getf control :resource-fact)))))))))

(deftest restartpoint-generation-roundtrip-and-suffix-start
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (multiple-value-bind (metadata bytes)
          (pg::physical-restartpoint-read pg::*physical-recovery-state*)
        (ok (= 96 (getf metadata :lsn)))
        (ok (= 32 (getf metadata :base)))
        (ok (equal '(("base/900" 32)) (getf metadata :forks)))
        (ok (equal '(900) (getf metadata :seen)))
        (ok (equalp bytes (pg::recovery-read-bytes (pg::catalog-path)))))
      (setf pg::*last-replay-lsn* 32)
      (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 96 pg::*wal-checkpoint-lsn*))
      (ok (gethash 900 pg::*physical-replay-seen-relfilenodes*))))

(deftest exhausted-next-xid-survives-restartpoint-publication-and-restoration
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (setf pg::*next-xid* 18446744073709551616
            (getf pg::*physical-recovery-state* :observed-next-xid) 18446744073709551616)
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (setf pg::*next-xid* 3)
      (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
      (ok (= 18446744073709551616 pg::*next-xid*))))

(deftest legacy-restartpoint-requires-retained-full-prefix-for-actual-visibility
    (dolist (compact '(nil t))
      (with-restartpoint-fixture (root store)
        (declare (ignore root))
        (let ((reader (symbol-function 'pg::physical-restartpoint-read)))
          (unwind-protect
               (progn
                 (if compact
                     (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
                     (pg::physical-restartpoint-publish (pg::physical-store-prefix-digest store 96)))
                 (setf (getf pg::*physical-recovery-state* :files) '(("base/900" 32 0)))
                 ;; Supply an already validated legacy generation at the restore boundary.
                 (setf (symbol-function 'pg::physical-restartpoint-read)
                       (lambda (state)
                         (multiple-value-bind (metadata bytes) (funcall reader state)
                           (setf (getf metadata :version) 2 (getf metadata :next-xid) 1003)
                           (values metadata bytes))))
                 (if compact
                     (ok (handler-case
                             (progn (pg::physical-restartpoint-prepare-storage store) nil)
                           (error () t)))
                     (progn
                       (ok (= 32 (pg::physical-restartpoint-prepare-storage store)))
                       (ok (= 50 pg::*next-xid*))
                       (ok (= 1003 (pg::load-next-xid))))))
            (setf (symbol-function 'pg::physical-restartpoint-read) reader))))))

(deftest restartpoint-new-pointer-with-old-floor-is-valid
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-store-test-hook*
              (lambda (point ignored)
                (declare (ignore ignored))
                (when (eq point :after-restartpoint-publication) (error "simulated stop")))))
        (ok (intake-fails-p
             (lambda () (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)))))
      (ok (= 32 (pg::physical-store-floor-lsn store)))
      (pg::physical-store-close store)
      (let ((reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1)))
        (unwind-protect
             (ok (= 96 (pg::physical-restartpoint-prepare-storage reopened)))
          (pg::physical-store-close reopened)))))

(deftest restartpoint-nonlocal-publication-exit-fences-recovery
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store)
            (pg::*physical-recovery-failed-p* nil)
            (pg::*physical-restartpoint-test-hook*
              (lambda (point)
                (when (eq point :after-generation) (throw 'interrupted :interrupted)))))
        (ok (eq :interrupted (catch 'interrupted (pg::physical-recovery-restartpoint))))
        (ok pg::*physical-recovery-failed-p*)
        (ok (not (probe-file (pg::physical-restartpoint-pointer-path))))
        (ok (= 32 (pg::physical-store-floor-lsn store)))
        (ok (intake-fails-p (lambda () (pg::check-recovery-available)))))))

(deftest restartpoint-damaged-surviving-fork-fails-before-catalog-restore
    (dolist (damage '(:missing :truncated))
      (with-restartpoint-fixture (root store)
        (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
        (let ((path (merge-pathnames "base/900" root))
              (disk-bytes (pg::string->bytes "damaged catalog must remain unchanged")))
          (ecase damage
            (:missing (delete-file path))
            (:truncated (pg::wal-atomic-write-bytes path #(0))))
          (pg::wal-atomic-write-bytes (pg::catalog-path) disk-bytes)
          (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
          (ok (equalp disk-bytes (pg::recovery-read-bytes (pg::catalog-path))))))))

(deftest restartpoint-complete-post-snapshot-drop-exempts-missing-fork
    (with-restartpoint-fixture (root store)
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (pg::physical-store-input store 96 (sample-physical-record 96))
      (let* ((raw (pg::wal-encode-record pg::+wal-record-catalog+ 0 128
                                         (history-catalog-payload nil '(900))))
             (finish (+ 128 (length raw))))
        (pg::physical-store-input store 128 raw)
        (pg::physical-store-flush store)
        (setf (getf pg::*physical-recovery-state* :required-applied) finish)
        (delete-file (merge-pathnames "base/900" root))
        (ok (= 96 (pg::physical-restartpoint-prepare-storage store))))))

(deftest restartpoint-pointer-corruption-never-falls-back
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (let ((pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path))))
        (setf (aref pointer 16) (logxor 1 (aref pointer 16)))
        (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer))
      (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
      (delete-file (pg::physical-restartpoint-pointer-path))
      (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))))

(deftest restartpoint-missing-authoritative-generation-cannot-fall-back
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (let* ((pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path)))
             (path (pg::physical-restartpoint-generation-path
                    (pg::wal-u64 pointer 8) (pg::wal-u64 pointer 16))))
        (delete-file path)
        (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store)))))))

(deftest restartpoint-metadata-must-cover-live-forks-and-history
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((bytes (pg::recovery-read-bytes (pg::catalog-path)))
             (metadata (pg::physical-restartpoint-metadata
                        (pg::physical-store-prefix-digest store 96) bytes)))
        (dolist (key '(:forks :seen))
          (let ((bad (copy-list metadata)))
            (setf (getf bad key) nil)
            (ok (intake-fails-p
                 (lambda () (pg::physical-restartpoint-check-metadata
                             bad bytes pg::*physical-recovery-state*))))))
        (let ((image (pg::physical-restartpoint-encode metadata bytes)))
          (setf (aref image (1- (length image))) (logxor 1 (aref image (1- (length image)))))
          (ok (intake-fails-p (lambda () (pg::physical-restartpoint-decode image))))))))

(deftest restartpoint-startup-progress-cannot-lower-required-boundary
    (with-restartpoint-fixture (root store)
      (declare (ignore root store))
      (setf (getf pg::*physical-recovery-state* :required-applied) 256
            (getf pg::*physical-recovery-state* :observed-next-xid) 100)
      (pg::physical-recovery-save-progress 96 50)
      (let ((control (pg::physical-recovery-read-control pg::*pgdata*)))
        (ok (= 256 (getf control :applied)))
        (ok (= 100 (getf control :next-xid))))))

(deftest restartpoint-retained-suffix-keeps-receive-and-apply-distinct
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (pg::physical-store-input store 96 (sample-physical-record 96))
      (pg::physical-store-input
       store 128 (pg::wal-encode-record pg::+wal-record-commit+ 42 128
                                        (pg::wal-encode-commit-timestamp 1)))
      (pg::physical-store-input store 168 (sample-physical-record 168))
      (pg::physical-store-flush store)
      (setf (getf pg::*physical-recovery-state* :required-applied) 168)
      (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 200 (pg::physical-store-flushed-lsn store)))
      (setf (getf pg::*physical-recovery-state* :required-applied) 169)
      (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))))

(deftest physical-store-v2-control-migrates-without-changing-bootstrap
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store))
          (pg::physical-store-close store)))
      (let* ((path (merge-pathnames "receive.control" root))
             (v3 (pg::recovery-read-bytes path))
             (v2 (make-array 52 :element-type '(unsigned-byte 8) :initial-element 0)))
        (replace v2 v3 :end2 48)
        (setf (aref v2 7) 50)
        (pg::wal-set-u32 v2 48 (pg::wal-crc32 v2))
        (pg::wal-atomic-write-bytes path v2))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 0 (pg::physical-store-base-lsn store) (pg::physical-store-floor-lsn store)))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (ok (= 68 (length (pg::recovery-read-bytes (merge-pathnames "receive.control" root))))))
          (pg::physical-store-close store)))))

(cl-spec:defproperty restartpoint-scheduling-is-translation-invariant
    ((floor (range integer 0 4611686018427387903))
     (offset (range integer 0 4611686018427387903))
     (progress (range integer 0 1000000))
     (elapsed (range integer 0 600))
     (budget (range integer 1 1000000))
     (timeout (range integer 1 600)))
  (:about pg::physical-restartpoint-due-p)
  (:trials (:normal 200))
  (let* ((now (* elapsed internal-time-units-per-second))
         (due (pg::physical-restartpoint-due-p (+ floor progress) floor now 0 timeout budget))
         (shifted (pg::physical-restartpoint-due-p
                   (+ floor offset progress) (+ floor offset) now 0 timeout budget)))
    (and (eql due shifted)
         (or (not due)
             (pg::physical-restartpoint-due-p
              (+ floor progress budget) floor now 0 timeout budget))
         (not (pg::physical-restartpoint-due-p floor floor now 0 timeout budget)))))

(deftest generated-restartpoint-schedule
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'restartpoint-scheduling-is-translation-invariant :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest restartpoint-scheduler-uses-applied-progress-and-time
    (let ((second internal-time-units-per-second))
      (ok (not (pg::physical-restartpoint-due-p 32 32 (* 100 second) 0 1 64)))
      (ok (not (pg::physical-restartpoint-due-p 64 32 second 0 300 64)))
      (ok (pg::physical-restartpoint-due-p 96 32 second 0 300 64))
      (ok (pg::physical-restartpoint-due-p 64 32 (* 300 second) 0 300 64))
      (ok (not (pg::physical-restartpoint-due-p 64 32 (* 299 second) 0 300 64)))))

(deftest restartpoint-scheduler-ignores-unapplied-receive-backlog
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store)
            (pg::*last-replay-lsn* 32)
            (pg::*physical-restartpoint-last-time* 0)
            (pg::*physical-restartpoint-timeout* 0)
            (pg::*physical-restartpoint-wal-bytes* 1))
        (ok (not (pg::physical-restartpoint-needed-p)))
        (ok (eq :not-due (pg::physical-recovery-restartpoint :automatic t)))
        (ok (= 32 (pg::physical-store-floor-lsn store)))
        (setf pg::*last-replay-lsn* 96)
        (ok (= 96 (pg::physical-recovery-restartpoint :automatic t)))
        (ok (= 96 (pg::physical-store-floor-lsn store)))
        (ok (not (pg::physical-restartpoint-needed-p))))))

(deftest recovery-checkpoint-runs-within-sql-admission
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store)
            (pg::*backend* (pg::make-backend :state :in-progress)))
        (ok (null (recovery-state-of
                   (lambda () (pg::recovery-check-statement '(:type :checkpoint))))))
        (pg::with-live-operation
          (ok (= 96 (pg::physical-recovery-checkpoint))))
        (ok (= 96 (pg::physical-store-floor-lsn store))))))

(deftest recovery-checkpoint-core-rejects-unadmitted-callers
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-receive-store* store))
        (ok (intake-fails-p (lambda () (pg::physical-recovery-restartpoint-core))))
        (ok (= 32 (pg::physical-store-floor-lsn store))))))

(deftest recovery-checkpoint-static-base-is-unchanged
    (let ((pg::*physical-recovery-state* (sample-recovery-manifest))
          (pg::*physical-receive-store* nil))
      (pg::with-live-operation
        (ok (eq :unchanged (pg::physical-recovery-checkpoint))))))
