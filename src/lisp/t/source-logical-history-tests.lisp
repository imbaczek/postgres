(in-package #:pg-physical-tests)

(defun append-source-logical-test-status (store enabled)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin pg::+wal-header-size+)))
    (pg::physical-store-input store begin (sample-physical-record begin))
    (pg::physical-store-input store start
                              (pg::wal-encode-record 18 0 start
                                                     (pg::wal-encode-logical-decoding-status-change enabled)))
    (pg::physical-store-flush store)
    (pg::physical-store-flushed-lsn store)))

(defun append-source-logical-test-checkpoint (store enabled)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin pg::+wal-header-size+))
         (facts (append (pg::wal-copy-control-resource-fields
                         (getf pg::*physical-recovery-state* :source-resource-fact))
                        (unless (eq enabled :unknown) (list :logical-decoding-enabled enabled))))
         (end (+ start (pg::wal-checkpoint-record-size facts)))
         (snapshot (append (list :version 2 :start start :end end :redo start
                                 :next-xid pg::*next-xid* :xid-reserved-through 1002
                                 :next-oid nil :time 0 :timeline 1 :prev-timeline 1
                                 :oldest-active-xid 0) facts)))
    (pg::physical-store-input store begin (sample-physical-record begin))
    (pg::physical-store-input store start
                              (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                     (pg::wal-encode-checkpoint-snapshot snapshot)))
    (pg::physical-store-flush store)
    snapshot))

(defun scan-source-logical-test-prefix (store snapshot end &optional (logical nil supplied-p))
  (apply #'pg::physical-recovery-scan-resource-fact
         store (getf snapshot :end) end
         (pg::wal-copy-control-resource-fields snapshot) pg::*physical-recovery-state*
         (append (list :checkpoint-seed snapshot :next-xid-seed (getf snapshot :next-xid))
                 (when supplied-p (list :logical-seed logical)))))

(deftest source-logical-private-scan-folds-all-status-boundaries-without-publication
    (dolist (initial '(:unknown nil t))
      (call-with-resource-restartpoint-fixture
       (lambda (root store)
         (declare (ignore root))
         (let* ((snapshot (append-source-logical-test-checkpoint store initial))
                (seed (pg::physical-logical-fact-from-checkpoint snapshot))
                (first-on (append-source-logical-test-status store t))
                (off (append-source-logical-test-status store nil))
                (on (append-source-logical-test-status store t))
                (repeat-on (append-source-logical-test-status store t))
                (end (append-resource-test-parameter store 100))
                (state pg::*physical-recovery-state*) (before (copy-tree state))
                (position pg::*last-replay-lsn*) (policy pg::*wal-logical-policy*)
                (force pg::*logical-capture-force*))
           (multiple-value-bind (capacity fpw fact)
               (scan-source-logical-test-prefix store snapshot end seed)
             (declare (ignore fpw))
             (ok capacity)
             (ok (pg::physical-logical-fact-at-p fact end 1))
             (ok (getf fact :enabled))
             (ok (= repeat-on (getf fact :last-enable-lsn)))
             (ok (= off (getf fact :last-disable-lsn)))
             (ok (< first-on off on repeat-on end))
             (ok (= (if seed (getf snapshot :start) (- first-on 33))
                    (getf fact :coverage-start)))
             (ok (not (eq seed fact))))
           (ok (equal before state))
           (ok (= position pg::*last-replay-lsn*))
           (ok (eq policy pg::*wal-logical-policy*))
           (ok (eq force pg::*logical-capture-force*)))))))

(deftest source-logical-private-empty-scan-checks-seed-and-checkpoint-agreement
    (dolist (enabled '(nil t))
      (call-with-resource-restartpoint-fixture
       (lambda (root store)
         (declare (ignore root))
         (let* ((snapshot (append-source-logical-test-checkpoint store enabled))
                (start (getf snapshot :end))
                (seed (pg::physical-logical-fact-from-checkpoint snapshot))
                (before (copy-tree pg::*physical-recovery-state*)))
           (let ((actual (nth-value 2 (scan-source-logical-test-prefix store snapshot start nil))))
             (ok (equal seed actual))
             (ok (not (eq seed actual)))
             (ok (null (getf actual :last-enable-lsn))))
           (ok (null (nth-value 2 (scan-source-logical-test-prefix store snapshot start))))
           (ok (= 2 (length (multiple-value-list (scan-source-logical-test-prefix store snapshot start)))))
           (ok (= 3 (length (multiple-value-list (scan-source-logical-test-prefix store snapshot start nil)))))
           (dolist (mutation '((:through 0) (:through-timeline 2) (:witness-timeline 2)
                               (:coverage-timeline 2) (:enabled :invalid)))
             (let ((bad (copy-list seed)))
               (setf (getf bad (first mutation)) (second mutation))
               (ok (intake-fails-p
                    (lambda () (scan-source-logical-test-prefix store snapshot start bad))))))
           (let ((bad (copy-list seed)))
             (setf (getf bad :enabled) (not enabled))
             (ok (pg::physical-logical-fact-valid-p bad))
             (ok (intake-fails-p
                  (lambda () (scan-source-logical-test-prefix store snapshot start bad)))))
           (ok (equal before pg::*physical-recovery-state*)))))))

(deftest source-logical-private-resource-only-proof-rejects-hidden-contradiction
    (call-with-resource-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (let* ((snapshot (append-source-logical-test-checkpoint store t))
              (on (append-source-logical-test-status store t))
              (bad (append-source-logical-test-checkpoint store nil))
              (state pg::*physical-recovery-state*) (before (copy-tree state))
              (position pg::*last-replay-lsn*))
         (ok (< on (getf bad :end)))
         (ok (intake-fails-p
              (lambda () (scan-source-logical-test-prefix store snapshot (getf bad :end)))))
         (ok (equal before state))
         (ok (= position pg::*last-replay-lsn*))))))

(defun call-with-source-logical-restartpoint-fixture (enabled function)
  (call-with-resource-restartpoint-fixture
   (lambda (root store)
     (let* ((begin pg::*last-replay-lsn*)
            (snapshot (append-source-logical-test-checkpoint store enabled)))
       (append-source-logical-test-status store t)
       (append-source-logical-test-status store nil)
       (when enabled (append-source-logical-test-status store t))
       (let* ((end (pg::physical-store-flushed-lsn store))
              (seed (pg::physical-logical-fact-from-checkpoint snapshot))
              (fact (nth-value 2 (scan-source-logical-test-prefix store snapshot end seed))))
         (assert (= end (pg::physical-replay-from begin)))
         (let ((candidate (copy-list pg::*physical-recovery-state*)))
           (setf (getf candidate :source-logical-fact) fact)
           (setf pg::*physical-recovery-state*
                 (pg::physical-recovery-write-control candidate 2 end pg::*next-xid*)))
         (funcall function root store snapshot fact))))))

(deftest source-logical-restartpoint-restores-facts-after-compaction-and-scans-later-marker
    (dolist (enabled '(nil t))
      (call-with-source-logical-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (let ((end pg::*last-replay-lsn*) (primary pg::*wal-logical-policy*)
                                           (force pg::*logical-capture-force*))
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (ok (< (getf snapshot :end) (pg::physical-store-floor-lsn store)))
           (multiple-value-bind (metadata catalog) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
             (declare (ignore catalog))
             (ok (equal fact (getf metadata :source-logical-fact)))
             (ok (not (eq fact (getf metadata :source-logical-fact)))))
           (dotimes (retry 2)
             (declare (ignorable retry))
             (setf (getf pg::*physical-recovery-state* :source-logical-fact) nil)
             (ok (= end (pg::physical-restartpoint-prepare-storage store)))
             (let ((restored (getf pg::*physical-recovery-state* :source-logical-fact)))
               (ok (equal fact restored))
               (ok (not (eq fact restored)))
               (ok (equal fact (getf (pg::physical-recovery-read-control root) :logical-fact)))))
           (let* ((next (append-source-logical-test-status store (not enabled)))
                  (actual (nth-value
                           2 (pg::physical-recovery-scan-resource-fact
                              store end next (getf pg::*physical-recovery-state* :source-resource-fact)
                              pg::*physical-recovery-state* :logical-seed fact
                              :checkpoint-seed snapshot :next-xid-seed pg::*next-xid*))))
             (ok (eql (not enabled) (getf actual :enabled)))
             (ok (= next (getf actual :through)))
             (ok (equal fact (getf pg::*physical-recovery-state* :source-logical-fact))))
           (ok (eq primary pg::*wal-logical-policy*))
           (ok (eq force pg::*logical-capture-force*)))))))

(deftest source-logical-restartpoint-reconstructs-omitted-fact-from-retained-wal
    (dolist (enabled '(nil t))
      (call-with-source-logical-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore snapshot))
         (let ((end pg::*last-replay-lsn*))
           (pg::physical-restartpoint-publish (pg::physical-store-prefix-digest store end))
           (multiple-value-bind (metadata catalog prefix)
               (pg::physical-restartpoint-read pg::*physical-recovery-state*)
             (remf metadata :source-logical-fact)
             (source-fpw-publish-test-restartpoint metadata catalog prefix))
           (setf (getf pg::*physical-recovery-state* :source-logical-fact) nil)
           (ok (= end (pg::physical-restartpoint-prepare-storage store)))
           (ok (equal fact (getf pg::*physical-recovery-state* :source-logical-fact)))
           (ok (not (eq fact (getf pg::*physical-recovery-state* :source-logical-fact))))
           (ok (equal fact (getf (pg::physical-recovery-read-control root) :logical-fact))))))))

(deftest source-logical-restartpoint-rejects-bad-facts-before-restoring-files
    (call-with-source-logical-restartpoint-fixture
     t
     (lambda (root store snapshot fact)
       (declare (ignore root snapshot fact))
       (pg::physical-store-compact store pg::*last-replay-lsn* #'pg::physical-restartpoint-publish)
       (ok (= pg::*last-replay-lsn* (pg::physical-restartpoint-prepare-storage store)))
       (let ((pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path))))
         (multiple-value-bind (metadata catalog prefix)
             (pg::physical-restartpoint-read pg::*physical-recovery-state*)
           (dolist (kind '(:missing :nil-value :wrong-boundary :wrong-timeline :erased-disable :duplicate))
             (let ((bad (copy-tree metadata)))
               (case kind
                 (:missing (remf bad :source-logical-fact))
                 (:nil-value (setf (getf bad :source-logical-fact) nil))
                 (:wrong-boundary (incf (getf (getf bad :source-logical-fact) :through)))
                 (:wrong-timeline (setf (getf (getf bad :source-logical-fact) :through-timeline) 2))
                 (:erased-disable
                  (setf (getf (getf bad :source-logical-fact) :last-disable-lsn) nil
                        (getf (getf bad :source-logical-fact) :last-disable-timeline) nil))
                 (:duplicate (setf bad (append bad (list :source-logical-fact (getf bad :source-logical-fact))))))
               (unwind-protect
                    (progn (source-fpw-publish-test-restartpoint bad catalog prefix)
                           (source-fpw-check-restore-rejection store))
                 (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer)))))))))

(deftest source-logical-legacy-restartpoint-does-not-invent-coverage-after-compaction
    (dolist (enabled '(nil t))
      (call-with-source-logical-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore snapshot))
         (let ((end pg::*last-replay-lsn*))
           (remf pg::*physical-recovery-state* :source-logical-fact)
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (setf (getf pg::*physical-recovery-state* :source-logical-fact) (copy-list fact))
           (source-fpw-check-restore-rejection store)
           (remf pg::*physical-recovery-state* :source-logical-fact)
           (remf pg::*physical-recovery-state* :required-logical-fact)
           (setf pg::*physical-recovery-state*
                 (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 end pg::*next-xid*))
           (setf (getf pg::*physical-recovery-state* :source-logical-fact) (copy-list fact))
           (ok (= end (pg::physical-restartpoint-prepare-storage store)))
           (ok (null (getf pg::*physical-recovery-state* :source-logical-fact)))
           (ok (null (getf (pg::physical-recovery-read-control root) :logical-fact))))))))

(deftest source-logical-restartpoint-checks-known-seed-before-suffix-file-effects
    (call-with-source-logical-restartpoint-fixture
     t
     (lambda (root store snapshot fact)
       (declare (ignore root snapshot fact))
       (pg::physical-store-compact store pg::*last-replay-lsn* #'pg::physical-restartpoint-publish)
       (append-source-logical-test-checkpoint store nil)
       (source-fpw-check-restore-rejection store))))

(defun source-policy-restartpoint-unknown-checkpoint-case (logical enabled)
  (call-with-resource-restartpoint-fixture
   (lambda (root store)
     (let* ((begin pg::*last-replay-lsn*)
            (snapshot (append-source-logical-test-checkpoint store :unknown))
            (checkpoint (getf snapshot :end)))
       (assert (= checkpoint (pg::physical-replay-from begin)))
       (pg::physical-store-compact store checkpoint #'pg::physical-restartpoint-publish)
       (if logical
           (append-source-logical-test-status store enabled)
           (let* ((begin (pg::physical-store-flushed-lsn store))
                  (start (+ begin pg::+wal-header-size+)))
             (pg::physical-store-input store begin (sample-physical-record begin))
             (pg::physical-store-input
              store start (pg::wal-encode-record
                           17 0 start (pg::wal-encode-full-page-writes-change enabled)))
             (pg::physical-store-flush store)))
       (let* ((end (pg::physical-store-flushed-lsn store))
              (fact (nth-value
                     (if logical 2 1)
                     (pg::physical-recovery-scan-resource-fact
                      store checkpoint end (pg::wal-copy-control-resource-fields snapshot)
                      pg::*physical-recovery-state* :logical-seed nil :fpw-seed nil
                      :checkpoint-seed snapshot :next-xid-seed (getf snapshot :next-xid))))
              (source (if logical :source-logical-fact :source-fpw-fact))
              (disk-key (if logical :logical-fact :fpw-fact)))
         (assert (= end (pg::physical-replay-from checkpoint)))
         (let ((candidate (copy-list pg::*physical-recovery-state*)))
           (setf (getf candidate source) fact)
           (setf pg::*physical-recovery-state*
                 (pg::physical-recovery-write-control candidate 2 end pg::*next-xid*)))
         (pg::physical-restartpoint-publish (pg::physical-store-prefix-digest store end))
         (multiple-value-bind (metadata catalog prefix)
             (pg::physical-restartpoint-read pg::*physical-recovery-state*)
           (remf metadata source)
           (source-fpw-publish-test-restartpoint metadata catalog prefix))
         (ok (> (pg::physical-store-floor-lsn store) (getf pg::*physical-recovery-state* :end)))
         (ok (= checkpoint (pg::physical-store-floor-lsn store)))
         (setf (getf pg::*physical-recovery-state* source) nil)
         (ok (= end (pg::physical-restartpoint-prepare-storage store)))
         (ok (equal fact (getf pg::*physical-recovery-state* source)))
         (ok (not (eq fact (getf pg::*physical-recovery-state* source))))
         (ok (equal fact (getf (pg::physical-recovery-read-control root) disk-key))))))))

(deftest source-logical-restartpoint-uses-retained-marker-after-unknown-checkpoint
    (dolist (enabled '(nil t))
      (source-policy-restartpoint-unknown-checkpoint-case t enabled)))

(deftest source-fpw-restartpoint-uses-retained-marker-after-unknown-checkpoint
    (dolist (enabled '(nil t))
      (source-policy-restartpoint-unknown-checkpoint-case nil enabled)))
