(in-package #:pg-physical-tests)

(defun source-fpw-persistence-facts ()
  (let ((history (pg::timeline-parse-history 1 "")))
    (labels ((record (enabled start)
               (pg::make-wal-record :version 8 :type 17 :start start :end (+ start 48) :xid 0
                                    :payload (pg::wal-encode-full-page-writes-change enabled))))
      (let* ((off (pg::physical-fpw-fact-next nil (record nil 32) 1 history))
             (on (pg::physical-fpw-fact-next off (record t 112) 1 history))
             (later (pg::physical-fpw-fact-next on (record nil 192) 1 history)))
        (values off on later)))))

(defun source-fpw-persistence-state (source minimum)
  (list :phase 2 :system-id 42 :timeline 1 :manifest-digest 4 :checkpoint 32
        :replay-timeline 1 :source-fpw-fact source :required-fpw-fact minimum
        :required-applied (getf minimum :through) :required-timeline 1))

(deftest source-fpw-progress-preserves-later-required-fact-and-input-state
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on later) (source-fpw-persistence-facts)
         (let* ((pg::*physical-recovery-control-test-hook* nil)
                (input (source-fpw-persistence-state off on))
                (before (copy-tree input))
                (published (pg::physical-recovery-write-control input 2 80 3))
                (disk (pg::physical-recovery-read-control pg::*pgdata*)))
           (ok (equal before input))
           (ok (equal off (getf published :source-fpw-fact)))
           (ok (equal on (getf published :required-fpw-fact)))
           (ok (not (eq on (getf published :required-fpw-fact))))
           (ok (equal on (getf disk :fpw-fact)))
           (ok (= 160 (getf disk :applied) (getf published :required-applied)))
           (setf (getf published :source-fpw-fact) (copy-list on))
           (let ((reached (pg::physical-recovery-write-control published 2 160 3)))
             (ok (equal on (getf reached :required-fpw-fact)))
             (setf (getf reached :source-fpw-fact) (copy-list later))
             (let ((advanced (pg::physical-recovery-write-control reached 2 240 3)))
               (ok (= 240 (getf advanced :required-applied)))
               (ok (null (getf (getf advanced :required-fpw-fact) :enabled)))
               (dotimes (attempt 2)
                 (declare (ignorable attempt))
                 (let ((cold (pg::physical-recovery-read-control pg::*pgdata*)))
                   (ok (equal later (getf cold :fpw-fact)))
                   (ok (null (getf (getf cold :fpw-fact) :enabled)))
                   (ok (= 192 (getf (getf cold :fpw-fact) :last-disable-start))))))))))))

(deftest source-fpw-recovery-control-largest-image-fits-all-reader-bounds
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on) (source-fpw-persistence-facts)
         (declare (ignore on))
         (let ((bytes (pg::physical-recovery-control-image
                       3 42 1 4 32 80 3 :state-time 0
                       :applied-timeline 1 :history-target 3 :history-digest 7
                       :promotion-child 4 :promotion-generation 9 :promotion-source-namespace :legacy
                       :retained-reference '(:root-id 1 :base-timeline 1 :base 32
                                             :manifest-digest 4 :end 80 :end-digest 7)
                       :resource-fact '(:resource-parameters (100 8 10 0 128)
                                        :resource-lsn 16 :resource-timeline 1)
                       :fpw-fact off)))
           (ok (= 268 (length bytes)))
           (ok (= (+ 285 pg::+physical-logical-fact-size+ pg::+physical-promotion-logical-intent-size+)
                  pg::+physical-recovery-control-maximum-bytes+))
           (ok (= (+ 285 pg::+physical-logical-fact-size+ pg::+physical-promotion-logical-intent-size+) (second (assoc "physical-recovery.control"
                                                                                                                       (pg::physical-backup-metadata-bounds) :test #'equal))))
           (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bytes)
           (let ((control (pg::physical-recovery-read-control pg::*pgdata*)))
             (ok (= 3 (getf control :phase)))
             (ok (equal off (getf control :fpw-fact)))
             (ok (getf control :retained-reference))
             (ok (getf control :resource-fact))))))))

(deftest source-logical-control-largest-image-survives-bounded-disk-read
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off ignored) (source-fpw-persistence-facts)
         (declare (ignore ignored))
         (dolist (enabled '(nil t))
           (dolist (exhausted '(nil t))
             (let* ((record (pg::make-wal-record
                             :version 8 :type 18 :xid 0 :start 32 :end 65
                             :payload (pg::wal-encode-logical-decoding-status-change enabled)))
                    (fact (pg::physical-logical-fact-next nil record 1 (pg::timeline-parse-history 1 ""))))
               (setf (getf fact :through) 80)
               (let* ((bytes (pg::physical-recovery-control-image
                              3 42 1 4 32 80 (if exhausted 18446744073709551616 3) :state-time 0
                              :applied-timeline 1 :history-target 3 :history-digest 7
                              :promotion-child 4 :promotion-generation 9 :promotion-source-namespace :legacy
                              :retained-reference '(:root-id 1 :base-timeline 1 :base 32
                                                    :manifest-digest 4 :end 80 :end-digest 7)
                              :resource-fact '(:resource-parameters (100 8 10 0 128)
                                               :resource-lsn 16 :resource-timeline 1)
                              :fpw-fact off :logical-fact fact :promotion-full-page-writes nil
                              :promotion-logical-decoding-enabled (not enabled)))
                      (path (pg::physical-recovery-control-path)))
                 (ok (= 391 (length bytes) pg::+physical-recovery-control-maximum-bytes+))
                 (pg::wal-atomic-write-bytes path bytes)
                 (dotimes (attempt 2)
                   (declare (ignorable attempt))
                   (let ((control (pg::physical-recovery-read-control pg::*pgdata*)))
                     (ok (equal fact (getf control :logical-fact)))
                     (ok (not (eq fact (getf control :logical-fact))))
                     (ok (eq enabled (getf (getf control :logical-fact) :enabled)))
                     (ok (equal off (getf control :fpw-fact)))
                     (ok (get-properties control '(:promotion-full-page-writes)))
                     (ok (null (getf control :promotion-full-page-writes)))
                     (ok (get-properties control '(:promotion-logical-decoding-enabled)))
                     (ok (eql (not enabled) (getf control :promotion-logical-decoding-enabled)))
                     (ok (= (if exhausted 18446744073709551616 3) (getf control :next-xid)))))
                 (setf (aref bytes (- (length bytes) 5)) 1)
                 (pg::wal-atomic-write-bytes path bytes)
                 (ok (handler-case (progn (pg::physical-recovery-read-control pg::*pgdata*) nil)
                       (error () t)))))))))))

(deftest source-fpw-progress-publication-failures-preserve-runtime-input
    (dolist (point '(:before-write :before-fsync :after-fsync :before-rename
                     :after-rename :before-directory-fsync :after-directory-fsync :after-write))
      (call-with-fpw-wal-fixture
       (lambda ()
         (multiple-value-bind (off on) (source-fpw-persistence-facts)
           (let* ((pg::*physical-recovery-control-test-hook* nil)
                  (state (pg::physical-recovery-write-control
                          (source-fpw-persistence-state off off) 2 80 3))
                  (proposed (copy-tree state)) (attempts 0))
             (setf (getf proposed :source-fpw-fact) (copy-list on))
             (let ((before (copy-tree proposed))
                   (pg::*physical-recovery-control-test-hook*
                     (lambda (stage value)
                       (declare (ignore value))
                       (when (eq stage point) (incf attempts)
                             (error "injected source FPW control publication failure")))))
               (ok (intake-fails-p
                    (lambda () (pg::physical-recovery-write-control proposed 2 160 3))))
               (ok (= 1 attempts))
               (ok (equal before proposed)))
             (let* ((renamed (member point '(:after-rename :before-directory-fsync
                                             :after-directory-fsync :after-write)))
                    (expected (if renamed on off))
                    (disk (pg::physical-recovery-read-control pg::*pgdata*)))
               (ok (= (if renamed 160 80) (getf disk :applied)))
               (ok (equal expected (getf disk :fpw-fact))))))))))

(deftest source-fpw-replay-progress-publishes-current-fact-and-retains-required
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on later) (source-fpw-persistence-facts)
         (declare (ignore later))
         (let* ((pg::*physical-recovery-control-test-hook* nil)
                (pg::*physical-recovery-state* (source-fpw-persistence-state off on))
                (pg::*last-replay-lsn* 80)
                (pg::*next-xid* 3)
                (pg::*xid-batch-end* 3)
                (record (pg::make-wal-record :version 8 :type 17 :start 112 :end 160 :xid 0
                                             :payload (pg::wal-encode-full-page-writes-change t)))
                (before (copy-tree pg::*physical-recovery-state*))
                (input pg::*physical-recovery-state*))
           (pg::physical-replay-publish-progress record 3 nil nil :fpw-fact on)
           (ok (= 160 pg::*last-replay-lsn*))
           (ok (equal before input))
           (ok (equal on (getf pg::*physical-recovery-state* :source-fpw-fact)))
           (ok (not (eq on (getf pg::*physical-recovery-state* :source-fpw-fact))))
           (ok (equal on (getf (pg::physical-recovery-read-control pg::*pgdata*) :fpw-fact)))
           (let* ((published pg::*physical-recovery-state*)
                  (published-before (copy-tree published))
                  (later-record (pg::make-wal-record :version 8 :type 17 :start 192 :end 240 :xid 0
                                                     :payload (pg::wal-encode-full-page-writes-change nil)))
                  (later-fact (pg::physical-fpw-fact-next on later-record 1 (pg::timeline-parse-history 1 "")))
                  (attempts 0)
                  (pg::*physical-recovery-control-test-hook*
                    (lambda (point value)
                      (declare (ignore value))
                      (when (eq point :after-rename)
                        (incf attempts)
                        (error "injected replay control failure after rename")))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-replay-publish-progress later-record 3 nil nil :fpw-fact later-fact))))
             (ok (= 1 attempts))
             (ok (eq published pg::*physical-recovery-state*))
             (ok (equal published-before published))
             (ok (= 160 pg::*last-replay-lsn*))
             (ok (equal later-fact (getf (pg::physical-recovery-read-control pg::*pgdata*) :fpw-fact)))))))))

(deftest source-fpw-live-replay-advances-known-checkpoint-through-later-terminals
    (dolist (enabled '(nil t))
      (dolist (reader-safe '(nil t))
        (call-with-resource-restartpoint-fixture
         (lambda (root store)
           (let* ((begin (pg::physical-store-flushed-lsn store))
                  (start (+ begin pg::+wal-header-size+))
                  (facts (append (pg::wal-copy-control-resource-fields
                                  (getf pg::*physical-recovery-state* :source-resource-fact))
                                 (list :full-page-writes enabled)))
                  (end (+ start (pg::wal-checkpoint-record-size facts)))
                  (snapshot (append (list :version 2 :start start :end end :redo start
                                          :next-xid pg::*next-xid* :xid-reserved-through 1002
                                          :next-oid nil :time 0 :timeline 1 :prev-timeline 1
                                          :oldest-active-xid 0) facts))
                  (primary-state pg::*wal-fpw-state*))
             (pg::physical-store-input store begin (sample-physical-record begin))
             (pg::physical-store-input
              store start (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                 (pg::wal-encode-checkpoint-snapshot snapshot)))
             (pg::physical-store-flush store)
             (ok (= end (pg::physical-replay-from begin)))
             (let ((fact (copy-list (getf pg::*physical-recovery-state* :source-fpw-fact))))
               (ok (pg::physical-fpw-fact-at-p fact end 1))
               (ok (eql enabled (getf fact :enabled)))
               (ok (= start (getf fact :witness-start)))
               (ok (eq :checkpoint (getf fact :witness-kind)))
               (dotimes (attempt 2)
                 (declare (ignorable attempt))
                 (append-resource-test-parameter store 100)
                 (let ((through (pg::physical-store-flushed-lsn store)))
                   (ok (= through (if reader-safe (pg::physical-row-replay-prefix 0)
                                      (pg::physical-replay-from pg::*last-replay-lsn*))))
                   (let ((next (getf pg::*physical-recovery-state* :source-fpw-fact)))
                     (ok (pg::physical-fpw-fact-at-p next through 1))
                     (ok (eql enabled (getf next :enabled)))
                     (ok (= start (getf next :witness-start)))
                     (ok (equal next (getf (pg::physical-recovery-read-control root) :fpw-fact)))))))
             (let* ((fork pg::*last-replay-lsn*)
                    (checkpoint (copy-tree (getf pg::*physical-recovery-state* :source-checkpoint)))
                    (next-xid pg::*next-xid*)
                    (raw (pg::wal-encode-end-of-recovery-record fork pg::*next-xid* 2 1 1))
                    (end (+ fork (length raw)))
                    (prior (copy-list (getf pg::*physical-recovery-state* :source-fpw-fact))))
               (pg::physical-store-select-history store 2 (store-history-text 1 fork) raw)
               (setf pg::*physical-recovery-state*
                     (pg::physical-recovery-attach-selected-store pg::*physical-recovery-state* store))
               (pg::physical-store-input store fork raw)
               (pg::physical-store-flush store)
               (ok (= end (pg::physical-replay-from fork)))
               (let ((next (getf pg::*physical-recovery-state* :source-fpw-fact)))
                 (ok (pg::physical-fpw-fact-at-p next end 2))
                 (ok (equal next (nth-value 1 (pg::physical-recovery-scan-resource-fact
                                               store fork end nil pg::*physical-recovery-state*
                                               :fpw-seed prior :checkpoint-seed checkpoint
                                               :next-xid-seed next-xid))))
                 (ok (eql enabled (getf next :enabled)))
                 (ok (= (getf prior :witness-start) (getf next :witness-start)))
                 (ok (= 1 (getf next :witness-timeline) (getf next :coverage-timeline)))
                 (ok (equal next (getf (pg::physical-recovery-read-control root) :fpw-fact)))))
             (ok (eq primary-state pg::*wal-fpw-state*))))))))

(defun call-with-source-fpw-restartpoint-fixture (enabled function)
  (call-with-resource-restartpoint-fixture
   (lambda (root store)
     (let* ((begin (pg::physical-store-flushed-lsn store))
            (start (+ begin pg::+wal-header-size+))
            (facts (append (pg::wal-copy-control-resource-fields
                            (getf pg::*physical-recovery-state* :source-resource-fact))
                           (list :full-page-writes enabled)))
            (end (+ start (pg::wal-checkpoint-record-size facts)))
            (snapshot (append (list :version 2 :start start :end end :redo start
                                    :next-xid pg::*next-xid* :xid-reserved-through 1002
                                    :next-oid nil :time 0 :timeline 1 :prev-timeline 1
                                    :oldest-active-xid 0) facts)))
       (pg::physical-store-input store begin (sample-physical-record begin))
       (pg::physical-store-input
        store start (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                           (pg::wal-encode-checkpoint-snapshot snapshot)))
       (pg::physical-store-flush store)
       (assert (= end (pg::physical-replay-from begin)))
       (append-resource-test-parameter store 100)
       (pg::physical-replay-from end)
       (funcall function root store snapshot
                (pg::physical-fpw-copy-fact (getf pg::*physical-recovery-state* :source-fpw-fact)))))))

(deftest source-fpw-restartpoint-recovers-retained-coverage-before-checkpoint-fallback
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot prior)
         (declare (ignore root))
         (let* ((begin (pg::physical-store-flushed-lsn store))
                (start (+ begin pg::+wal-header-size+))
                (checkpoint (copy-tree snapshot))
                (end (+ start (pg::wal-checkpoint-record-size checkpoint))))
           (setf (getf checkpoint :start) start (getf checkpoint :redo) start
                 (getf checkpoint :end) end)
           (loop for (key value) on (pg::wal-copy-control-resource-fields
                                     (getf pg::*physical-recovery-state* :source-resource-fact))
                 by #'cddr
                 do (setf (getf checkpoint key) value))
           (pg::physical-store-input store begin (sample-physical-record begin))
           (pg::physical-store-input
            store start (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                               (pg::wal-encode-checkpoint-snapshot checkpoint)))
           (pg::physical-store-flush store)
           (ok (= end (pg::physical-replay-from begin)))
           (let ((expected (copy-tree (getf pg::*physical-recovery-state* :source-fpw-fact))))
             (pg::physical-restartpoint-publish (pg::physical-store-prefix-digest store end))
             (multiple-value-bind (metadata catalog prefix)
                 (pg::physical-restartpoint-read pg::*physical-recovery-state*)
               (remf metadata :source-fpw-fact)
               (source-fpw-publish-test-restartpoint metadata catalog prefix))
             (dotimes (retry 2)
               (declare (ignorable retry))
               (remf pg::*physical-recovery-state* :source-fpw-fact)
               (ok (= end (pg::physical-restartpoint-prepare-storage store)))
               (let ((actual (getf pg::*physical-recovery-state* :source-fpw-fact)))
                 (ok (equal expected actual))
                 (ok (not (eq expected actual)))
                 (ok (= (getf prior :coverage-start) (getf actual :coverage-start)))))))))))

(deftest source-fpw-private-scan-requires-exact-seed-even-at-empty-prefix
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore root snapshot))
         (let* ((start pg::*last-replay-lsn*)
                (state pg::*physical-recovery-state*)
                (resource (getf state :source-resource-fact))
                (before (copy-tree state))
                (primary pg::*wal-fpw-state*))
           (multiple-value-bind (capacity actual)
               (pg::physical-recovery-scan-resource-fact store start start resource state :fpw-seed fact)
             (ok (equal capacity resource))
             (ok (equal actual fact))
             (ok (not (eq actual fact)))
             (ok (eql enabled (getf actual :enabled))))
           (ok (null (nth-value 1 (pg::physical-recovery-scan-resource-fact
                                   store start start resource state :fpw-seed nil))))
           (dolist (mutation '((:through 0) (:through-timeline 2) (:witness-timeline 2)
                               (:coverage-timeline 2) (:enabled 3)))
             (let ((bad (copy-list fact)))
               (setf (getf bad (first mutation)) (second mutation))
               (ok (intake-fails-p
                    (lambda () (pg::physical-recovery-scan-resource-fact
                                store start start resource state :fpw-seed bad))))))
           (ok (equal before state))
           (ok (eq primary pg::*wal-fpw-state*)))))))

(deftest source-fpw-restartpoint-restores-copied-evidence-after-compaction
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (let ((end pg::*last-replay-lsn*) (primary pg::*wal-fpw-state*))
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (ok (< (getf snapshot :end) (pg::physical-store-floor-lsn store)))
           (multiple-value-bind (metadata bytes) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
             (declare (ignore bytes))
             (ok (equal fact (getf metadata :source-fpw-fact)))
             (when (getf metadata :source-fpw-fact)
               (setf (getf (getf metadata :source-fpw-fact) :enabled) (not enabled))
               (ok (eql enabled (getf (getf pg::*physical-recovery-state* :source-fpw-fact) :enabled)))))
           (dotimes (retry 2)
             (declare (ignorable retry))
             (setf (getf pg::*physical-recovery-state* :source-fpw-fact) nil)
             (ok (= end (pg::physical-restartpoint-prepare-storage store)))
             (let ((restored (getf pg::*physical-recovery-state* :source-fpw-fact)))
               (ok (equal fact restored))
               (ok (not (eq fact restored)))
               (ok (equal restored (getf (pg::physical-recovery-read-control root) :fpw-fact)))))
           (ok (eq primary pg::*wal-fpw-state*)))))))

(defun source-fpw-check-restore-rejection (store)
  (let* ((state pg::*physical-recovery-state*) (before (copy-tree state))
                                               (position pg::*last-replay-lsn*)
                                               (catalog (pg::recovery-read-bytes (pg::catalog-path)))
                                               (writer (symbol-function 'pg::wal-atomic-write-bytes)) (writes 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::wal-atomic-write-bytes)
                 (lambda (path bytes) (incf writes) (funcall writer path bytes)))
           (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store)))))
      (setf (symbol-function 'pg::wal-atomic-write-bytes) writer))
    (ok (zerop writes))
    (ok (eq state pg::*physical-recovery-state*))
    (ok (equal before state))
    (ok (= position pg::*last-replay-lsn*))
    (ok (equalp catalog (pg::recovery-read-bytes (pg::catalog-path))))))

(defun source-fpw-publish-test-restartpoint (metadata catalog prefix)
  (let* ((bytes (pg::physical-restartpoint-encode metadata catalog prefix))
         (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
         (lsn (getf metadata :lsn))
         (pointer (make-array 28 :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace pointer #(80 71 82 80 80 84 82 49))
    (pg::wal-set-u64 pointer 8 lsn)
    (pg::wal-set-u64 pointer 16 digest)
    (pg::wal-set-u32 pointer 24 (pg::wal-crc32 pointer))
    (pg::wal-atomic-write-bytes (pg::physical-restartpoint-generation-path lsn digest) bytes)
    (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer)))

(deftest source-fpw-restartpoint-rejects-invalid-facts-before-restoring-files
    (call-with-source-fpw-restartpoint-fixture
     nil
     (lambda (root store snapshot fact)
       (declare (ignore root snapshot fact))
       (pg::physical-store-compact store pg::*last-replay-lsn* #'pg::physical-restartpoint-publish)
       (let ((pointer (pg::recovery-read-bytes (pg::physical-restartpoint-pointer-path))))
         (multiple-value-bind (metadata catalog prefix) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
           (dolist (kind '(:absent-value :wrong-boundary :wrong-timeline :extra-key :duplicate-key))
             (let ((bad (copy-tree metadata)))
               (case kind
                 (:absent-value (setf (getf bad :source-fpw-fact) nil))
                 (:wrong-boundary (decf (getf (getf bad :source-fpw-fact) :through) 8))
                 (:wrong-timeline (setf (getf (getf bad :source-fpw-fact) :through-timeline) 2))
                 (:extra-key (setf bad (append bad '(:extra 0))))
                 (:duplicate-key (setf bad (append bad (list :source-fpw-fact (getf bad :source-fpw-fact))))))
               (unwind-protect
                    (progn (source-fpw-publish-test-restartpoint bad catalog prefix)
                           (source-fpw-check-restore-rejection store))
                 (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer)))))))))

(deftest source-fpw-legacy-restartpoint-gap-does-not-invent-policy
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore snapshot))
         (let ((end pg::*last-replay-lsn*))
           (remf pg::*physical-recovery-state* :source-fpw-fact)
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (setf (getf pg::*physical-recovery-state* :source-fpw-fact) (copy-list fact))
           (source-fpw-check-restore-rejection store)
           (remf pg::*physical-recovery-state* :source-fpw-fact)
           (remf pg::*physical-recovery-state* :required-fpw-fact)
           (setf pg::*physical-recovery-state*
                 (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 end pg::*next-xid*))
           (setf (getf pg::*physical-recovery-state* :source-fpw-fact) (copy-list fact))
           (dotimes (retry 2)
             (declare (ignorable retry))
             (ok (= end (pg::physical-restartpoint-prepare-storage store)))
             (ok (null (getf pg::*physical-recovery-state* :source-fpw-fact)))
             (ok (null (getf (pg::physical-recovery-read-control root) :fpw-fact)))))))))

(deftest source-fpw-private-scan-reconstructs-retained-suffix-and-required-facts
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore root))
         (let* ((start (getf snapshot :end)) (end pg::*last-replay-lsn*)
                                             (state pg::*physical-recovery-state*)
                                             (seed (pg::physical-fpw-fact-from-state
                                                    (pg::wal-fpw-state-from-checkpoint snapshot (make-symbol "FPW-SCAN-TEST"))
                                                    start 1))
                                             (metadata (list :version 4 :timeline 1 :lsn end :source-checkpoint snapshot))
                                             (resource (pg::wal-copy-control-resource-fields snapshot)))
           (multiple-value-bind (capacity actual)
               (pg::physical-recovery-scan-resource-fact store start end resource state :fpw-seed seed)
             (ok (equal capacity (getf state :source-resource-fact)))
             (ok (equal fact actual))
             (ok (not (eq fact actual))))
           (ok (equal fact (pg::physical-restartpoint-fpw-seed metadata state store end)))
           (setf (getf metadata :lsn) start)
           (ok (equal seed (pg::physical-restartpoint-fpw-seed metadata state store start)))
           (let ((bad-state (copy-tree state)))
             (setf (getf bad-state :required-applied) end
                   (getf bad-state :required-timeline) 1
                   (getf bad-state :required-fpw-fact) (copy-list fact)
                   (getf (getf bad-state :required-fpw-fact) :enabled) (not enabled))
             (ok (pg::physical-fpw-fact-at-p (getf bad-state :required-fpw-fact) end 1))
             (ok (intake-fails-p
                  (lambda () (pg::physical-restartpoint-fpw-seed metadata bad-state store start)))))
           (let ((earlier (copy-tree state)))
             (setf (getf earlier :required-applied) start
                   (getf earlier :required-fpw-fact) (copy-list seed)
                   (getf metadata :lsn) end
                   (getf metadata :source-fpw-fact) (copy-list fact))
             (ok (equal fact (pg::physical-restartpoint-fpw-seed metadata earlier store end))))
           ;; A callback error fences this receive-store handle; do not reuse it.
           (ok (intake-fails-p
                (lambda () (pg::physical-recovery-scan-resource-fact
                            store start (+ start 32) resource state :fpw-seed seed)))))))))

(deftest source-fpw-private-follow-scan-preserves-and-checks-boundary-evidence
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore root))
         (let* ((state pg::*physical-recovery-state*) (before (copy-tree state))
                                                      (primary pg::*wal-fpw-state*)
                                                      (start (getf snapshot :end)) (end pg::*last-replay-lsn*)
                                                      (reservation (copy-list (getf state :source-reservation)))
                                                      (fpw (pg::physical-fpw-fact-from-state
                                                            (pg::wal-fpw-state-from-checkpoint snapshot (make-symbol "FOLLOW-FPW-TEST"))
                                                            start 1))
                                                      (seed (list start 1 (getf snapshot :next-xid) snapshot reservation nil
                                                                  (pg::wal-copy-control-resource-fields snapshot) fpw)))
           (setf (getf reservation :through) start)
           (labels ((scan (seed base &optional witness (required end))
                      (let ((candidate (pg::physical-follow-copy-replay-candidate store)))
                        (pg::physical-follow-scan-replay-from-seed-core
                         candidate base (list end 1) (list required 1) pg::*next-xid* seed
                         (lambda (visit from) (pg::physical-store-map-durable-records candidate visit :start-lsn from))
                         nil witness))))
             (let ((result (scan seed state (list end 1 pg::*next-xid* (getf state :source-resource-fact) fact))))
               (ok (equal fact (getf result :applied-fpw-fact)))
               (ok (equal fact (getf result :required-fpw-fact)))
               (ok (not (eq fact (getf result :applied-fpw-fact)))))
             (let* ((base (copy-tree state))
                    (required (copy-list fpw)))
               (setf (getf base :required-applied) start
                     (getf base :required-timeline) 1
                     (getf base :required-fpw-fact) required
                     (getf base :required-resource-fact) (pg::wal-copy-control-resource-fields snapshot))
               (let ((result (scan seed base nil start)))
                 (ok (< start end))
                 (ok (equal fpw (getf result :required-fpw-fact)))
                 (ok (equal fact (getf result :applied-fpw-fact)))
                 (ok (not (eq (getf result :required-fpw-fact) required)))
                 (setf (getf (getf result :required-fpw-fact) :through) 0)
                 (ok (= end (getf (getf result :applied-fpw-fact) :through)))
                 (ok (= start (getf required :through))))
               (setf (getf required :enabled) (not enabled))
               (ok (intake-fails-p (lambda () (scan seed base nil start)))))
             (let ((unknown (copy-list seed)))
               (setf (eighth unknown) nil)
               (ok (intake-fails-p (lambda () (scan unknown state)))))
             (let ((bad (copy-tree seed)))
               (setf (getf (eighth bad) :through) (1- start))
               (ok (intake-fails-p (lambda () (scan bad state)))))
             (let ((witness (copy-list fact)))
               (setf (getf witness :enabled) (not enabled))
               (ok (pg::physical-fpw-fact-at-p witness end 1))
               (ok (intake-fails-p
                    (lambda () (scan seed state (list end 1 pg::*next-xid* (getf state :source-resource-fact) witness))))))
             (ok (equal before state))
             (ok (eq state pg::*physical-recovery-state*))
             (ok (eq primary pg::*wal-fpw-state*))
             (ok (not (pg::physical-store-failed-p store)))))))))

(deftest source-fpw-follow-binding-and-control-preserve-known-off-and-on
    (dolist (enabled '(nil t))
      (call-with-fpw-wal-fixture
       (lambda ()
         (let* ((config (follow-binding-test-config)) (binding (fifth config))
                                                      (record (pg::make-wal-record :version 8 :type 17 :start 48 :end 96 :xid 0
                                                                                   :payload (pg::wal-encode-full-page-writes-change enabled)))
                                                      (fact (pg::physical-fpw-fact-next nil record 1 (pg::timeline-parse-history 1 "")))
                                                      (base (list :system-id 42 :timeline 1 :manifest-digest 77 :checkpoint 64 :end 64
                                                                  :cluster-identity "binding-probe"))
                                                      (witness (pg::physical-follow-recovery-witness (getf binding :recovery) (list :fpw-fact fact))))
           (setf (getf binding :recovery) witness)
           (ok (pg::physical-follow-binding-check binding))
           (ok (not (eq fact (getf witness :fpw-fact))))
           (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
           (dolist (selected '(nil t))
             (let ((control (pg::physical-recovery-decode-control
                             (pg::physical-follow-binding-recovery-image binding 12345 :selected selected))))
               (ok (equal fact (getf control :fpw-fact)))
               (ok (eql enabled (getf (getf control :fpw-fact) :enabled)))))
           (let ((control (pg::physical-follow-publish-pending-recovery base binding)))
             (ok (equal fact (getf control :fpw-fact)))
             (ok (eq :after (pg::physical-follow-pending-recovery-position base binding control)))
             (setf (getf control :fpw-fact) nil)
             (ok (intake-fails-p (lambda () (pg::physical-follow-pending-recovery-position base binding control)))))
           (let ((bad (copy-tree binding)))
             (setf (getf (getf bad :recovery) :fpw-fact) nil)
             (ok (intake-fails-p (lambda () (pg::physical-follow-binding-check bad)))))
           (let ((bad (copy-tree binding)))
             (setf (getf (getf (getf bad :recovery) :fpw-fact) :through) 95)
             (ok (intake-fails-p (lambda () (pg::physical-follow-binding-check bad)))))
           (let ((cleared (pg::physical-follow-recovery-witness witness nil)))
             (ok (null (getf cleared :fpw-fact)))
             (ok (equal fact (getf witness :fpw-fact)))))))))

(deftest source-fpw-legacy-pending-binding-cannot-discard-reconstructed-evidence
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (let* ((base (copy-tree pg::*physical-recovery-state*))
                (end pg::*last-replay-lsn*) (start (getf snapshot :end))
                (unknown (copy-tree base))
                (reservation (copy-list (getf base :source-reservation)))
                (seed (list start 1 pg::*next-xid* snapshot reservation nil
                            (pg::wal-copy-control-resource-fields snapshot)
                            (pg::physical-fpw-fact-from-state
                             (pg::wal-fpw-state-from-checkpoint snapshot (make-symbol "LEGACY-PENDING-FPW"))
                             start 1)))
                (binding (list :required (list end 1) :recovery (list :phase 2))))
           (remf unknown :source-fpw-fact)
           (remf unknown :required-fpw-fact)
           (pg::physical-recovery-write-control unknown 2 end pg::*next-xid*)
           (ok (null (getf (pg::physical-recovery-read-control root) :fpw-fact)))
           (remf base :required-fpw-fact)
           (setf (getf reservation :through) start)
           (let* ((before (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))
                  (candidate (pg::physical-follow-copy-replay-candidate store))
                  (result (pg::physical-follow-scan-replay-from-seed-core
                           candidate base (list end 1) (list end 1) pg::*next-xid* seed
                           (lambda (visit from) (pg::physical-store-map-durable-records candidate visit :start-lsn from))
                           nil)))
             (ok (equal fact (getf result :required-fpw-fact)))
             (ok (intake-fails-p (lambda () (pg::physical-follow-check-policy-publication result binding))))
             (ok (equalp before (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
             (setf (getf (getf binding :recovery) :fpw-fact) (copy-list fact))
             (ok (eq result (pg::physical-follow-check-policy-publication result binding)))
             (ok (not (pg::physical-store-failed-p store)))))))))

(deftest source-fpw-ahead-restartpoint-keeps-older-unknown-required-boundary
    (dolist (enabled '(nil t))
      (call-with-source-fpw-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (let* ((required 192) (end pg::*last-replay-lsn*)
                               (state (copy-tree pg::*physical-recovery-state*))
                               (resource (pg::wal-copy-control-resource-fields snapshot))
                               (binding (list :required (list required 1) :recovery (list :phase 2))))
           (ok (< required (getf snapshot :start) end))
           (ok (null (nth-value 1 (pg::physical-recovery-scan-resource-fact
                                   store (pg::physical-store-base-lsn store) required nil state :fpw-seed nil))))
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (remf state :source-fpw-fact)
           (remf state :required-fpw-fact)
           (setf (getf state :required-applied) required
                 (getf state :required-timeline) 1
                 (getf state :source-resource-fact) (copy-tree resource)
                 (getf state :required-resource-fact) (copy-tree resource))
           (setf pg::*physical-recovery-state*
                 (pg::physical-recovery-write-control state 2 required pg::*next-xid*)
                 pg::*last-replay-lsn* required)
           (let* ((candidate (pg::physical-follow-copy-replay-candidate store))
                  (seed (multiple-value-list
                         (pg::physical-follow-replay-seed
                          candidate pg::*physical-recovery-state* (list end 1)
                          (multiple-value-bind (metadata catalog prefix digest)
                              (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                            (declare (ignore catalog prefix))
                            (list (getf metadata :lsn) digest)))))
                  (before (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))
                  (result (pg::physical-follow-scan-replay-from-seed-core
                           candidate pg::*physical-recovery-state* (list end 1) (list required 1)
                           pg::*next-xid* seed
                           (lambda (visit from)
                             (declare (ignore visit))
                             (assert (= from end))
                             nil)
                           nil)))
             (ok (null (getf result :required-fpw-fact)))
             (ok (equal fact (getf result :applied-fpw-fact)))
             (ok (eq result (pg::physical-follow-check-policy-publication result binding)))
             (ok (equalp before (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))))
           (ok (= end (pg::physical-restartpoint-prepare-storage store)))
           (ok (equal fact (getf pg::*physical-recovery-state* :source-fpw-fact)))
           (ok (equal fact (getf pg::*physical-recovery-state* :required-fpw-fact)))
           (ok (= end (getf pg::*physical-recovery-state* :required-applied))))))))
