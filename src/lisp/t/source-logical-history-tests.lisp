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

(deftest source-logical-follow-private-scan-preserves-status-boundaries
    (dolist (initial '(:unknown nil t))
      (call-with-resource-restartpoint-fixture
       (lambda (root store)
         (declare (ignore root))
         (let* ((snapshot (append-source-logical-test-checkpoint store initial))
                (start (getf snapshot :end))
                (seed (pg::physical-logical-fact-from-checkpoint snapshot))
                (off (append-source-logical-test-status store nil))
                (on (append-source-logical-test-status store t))
                (end (append-resource-test-parameter store 100))
                (state pg::*physical-recovery-state*) (before (copy-tree state))
                (next (getf snapshot :next-xid))
                (base (copy-tree state))
                (result
                  (pg::physical-follow-scan-replay-from-seed-core
                   store (progn (remf base :required-resource-fact) base) (list end 1) (list end 1) next
                   (list start 1 next snapshot nil nil
                         (pg::wal-copy-control-resource-fields snapshot) nil seed)
                   (lambda (function position)
                     (pg::physical-store-map-durable-records store function :start-lsn position))
                   nil))
                (fact (getf result :required-logical-fact)))
           (ok (pg::physical-logical-fact-at-p fact end 1))
           (ok (= off (getf fact :last-disable-lsn)))
           (ok (= on (getf fact :last-enable-lsn)))
           (ok (equal fact (getf result :applied-logical-fact)))
           (ok (not (eq fact (getf result :applied-logical-fact))))
           (ok (equal before state)))))))

(deftest source-logical-follow-binding-and-control-preserve-known-off-and-on
    (dolist (enabled '(nil t))
      (call-with-fpw-wal-fixture
       (lambda ()
         (let* ((config (follow-binding-test-config)) (binding (fifth config))
                                                      (record (pg::make-wal-record :version 8 :type 18 :start 63 :end 96 :xid 0
                                                                                   :payload (pg::wal-encode-logical-decoding-status-change enabled)))
                                                      (fact (pg::physical-logical-fact-next nil record 1 (pg::timeline-parse-history 1 "")))
                                                      (base (list :system-id 42 :timeline 1 :manifest-digest 77 :checkpoint 64 :end 64
                                                                  :cluster-identity "binding-probe"))
                                                      (witness (pg::physical-follow-recovery-witness (getf binding :recovery) (list :logical-fact fact))))
           (setf (getf binding :recovery) witness)
           (ok (pg::physical-follow-binding-check binding))
           (ok (not (eq fact (getf witness :logical-fact))))
           (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
           (dolist (selected '(nil t))
             (let ((control (pg::physical-recovery-decode-control
                             (pg::physical-follow-binding-recovery-image binding 12345 :selected selected))))
               (ok (equal fact (getf control :logical-fact)))
               (ok (eql enabled (getf (getf control :logical-fact) :enabled)))))
           (let ((control (pg::physical-follow-publish-pending-recovery base binding)))
             (ok (equal fact (getf control :logical-fact)))
             (ok (eq :after (pg::physical-follow-pending-recovery-position base binding control)))
             (setf (getf control :logical-fact) nil)
             (ok (intake-fails-p (lambda () (pg::physical-follow-pending-recovery-position base binding control)))))
           (let ((bad (copy-tree binding)))
             (setf (getf (getf bad :recovery) :logical-fact) nil)
             (ok (intake-fails-p (lambda () (pg::physical-follow-binding-check bad)))))
           (let ((bad (copy-tree binding)))
             (setf (getf (getf (getf bad :recovery) :logical-fact) :through) 95)
             (ok (intake-fails-p (lambda () (pg::physical-follow-binding-check bad)))))
           (let ((cleared (pg::physical-follow-recovery-witness witness nil)))
             (ok (null (getf cleared :logical-fact)))
             (ok (equal fact (getf witness :logical-fact)))))))))

(deftest source-logical-follow-private-scan-rejects-boundary-mismatches
    (dolist (enabled '(nil t))
      (call-with-source-logical-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (declare (ignore root))
         (let* ((state pg::*physical-recovery-state*) (before (copy-tree state))
                                                      (start (getf snapshot :end)) (end pg::*last-replay-lsn*)
                                                      (logical (pg::physical-logical-fact-from-checkpoint snapshot))
                                                      (reservation (copy-tree (getf state :source-reservation)))
                                                      (seed (list start 1 (getf snapshot :next-xid) snapshot reservation nil
                                                                  (pg::wal-copy-control-resource-fields snapshot) nil logical)))
           (setf (getf reservation :through) start)
           (labels ((scan (input base &optional witness (required end))
                      (let ((candidate (pg::physical-follow-copy-replay-candidate store)))
                        (pg::physical-follow-scan-replay-from-seed-core
                         candidate base (list end 1) (list required 1) pg::*next-xid* input
                         (lambda (visit from)
                           (pg::physical-store-map-durable-records candidate visit :start-lsn from))
                         nil witness))))
             (let ((witness (list end 1 pg::*next-xid*
                                  (getf state :source-resource-fact) nil (copy-tree fact))))
               (ok (equal fact (getf (scan seed state witness) :required-logical-fact)))
               (incf (getf (sixth witness) :coverage-start))
               (ok (pg::physical-logical-fact-at-p (sixth witness) end 1))
               (ok (intake-fails-p (lambda () (scan seed state witness)))))
             (let ((bad (copy-tree seed)))
               (setf (getf (ninth bad) :through) (1- start))
               (ok (intake-fails-p (lambda () (scan bad state)))))
             (let ((base (copy-tree state)))
               (setf (getf base :required-logical-fact) (copy-tree logical)
                     (getf base :required-resource-fact)
                     (pg::wal-copy-control-resource-fields snapshot))
               (let ((result (scan seed base nil start)))
                 (ok (equal logical (getf result :required-logical-fact)))
                 (ok (equal fact (getf result :applied-logical-fact)))
                 (ok (not (eq logical (getf result :required-logical-fact)))))
               (setf (getf (getf base :required-logical-fact) :enabled) (not enabled))
               (ok (intake-fails-p (lambda () (scan seed base nil start)))))
             (ok (equal before state))
             (ok (eq state pg::*physical-recovery-state*))))))))

(deftest source-logical-follow-binding-rejects-foreign-witness-geometry
    (dolist (enabled '(nil t))
      (let* ((config (same-timeline-follow-binding-config))
             (binding (fifth config))
             (anchor (same-timeline-follow-anchor 128 555 456 2))
             (tuple (list 42 1 64 128 555 64 #xCBF29CE484222325 2 123 77 nil))
             (record (pg::make-wal-record :version 8 :type 18 :start 0 :end 33 :xid 0
                                          :payload (pg::wal-encode-logical-decoding-status-change enabled)))
             (fact (pg::physical-logical-fact-next nil record 1 (pg::timeline-parse-history 1 ""))))
        (setf (getf anchor :base) 64
              (getf binding :anchor) anchor
              (getf binding :before) tuple (getf binding :after) (copy-list tuple)
              (getf binding :applied) '(96 2) (getf binding :required) '(96 2)
              (getf binding :child-history) nil
              (getf fact :through) 96 (getf fact :through-timeline) 2
              (getf (getf binding :recovery) :logical-fact) fact)
        (ok (pg::physical-follow-binding-check binding))
        (let ((bad (copy-tree config)))
          (let ((foreign (getf (getf (fifth bad) :recovery) :logical-fact)))
            (setf (getf foreign :coverage-timeline) 2
                  (getf foreign :witness-timeline) 2
                  (getf foreign (if enabled :last-enable-timeline :last-disable-timeline)) 2)
            (ok (pg::physical-logical-fact-at-p foreign 96 2)))
          (ok (intake-fails-p (lambda () (pg::physical-follow-binding-check (fifth bad)))))
          (ok (intake-fails-p (lambda () (pg::physical-follow-config-image bad))))))))

(deftest source-logical-follow-empty-scan-reconciles-checkpoint
    (dolist (enabled '(nil t))
      (call-with-resource-restartpoint-fixture
       (lambda (root store)
         (declare (ignore root))
         (let* ((snapshot (append-source-logical-test-checkpoint store enabled))
                (start (getf snapshot :end))
                (logical (pg::physical-logical-fact-from-checkpoint snapshot))
                (base (copy-tree pg::*physical-recovery-state*))
                (next (getf snapshot :next-xid)))
           (remf base :required-resource-fact)
           (remf base :required-logical-fact)
           (labels ((scan (fact)
                      (pg::physical-follow-scan-replay-from-seed-core
                       store base (list start 1) (list start 1) next
                       (list start 1 next snapshot nil nil
                             (pg::wal-copy-control-resource-fields snapshot) nil fact)
                       (lambda (visit from)
                         (pg::physical-store-map-durable-records store visit :start-lsn from))
                       nil)))
             (ok (equal logical (getf (scan nil) :required-logical-fact)))
             (ok (equal logical (getf (scan logical) :required-logical-fact)))
             (let ((bad (copy-tree logical)))
               (setf (getf bad :enabled) (not enabled))
               (ok (pg::physical-logical-fact-at-p bad start 1))
               (ok (intake-fails-p (lambda () (scan bad)))))))))))

(deftest source-logical-legacy-pending-binding-cannot-discard-reconstructed-evidence
    (dolist (enabled '(nil t))
      (call-with-source-logical-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (let* ((base (copy-tree pg::*physical-recovery-state*))
                (end pg::*last-replay-lsn*) (start (getf snapshot :end))
                (unknown (copy-tree base))
                (reservation (copy-list (getf base :source-reservation)))
                (seed (list start 1 pg::*next-xid* snapshot reservation nil
                            (pg::wal-copy-control-resource-fields snapshot)
                            nil (pg::physical-logical-fact-from-checkpoint snapshot)))
                (binding (list :required (list end 1) :recovery (list :phase 2))))
           (remf unknown :source-logical-fact)
           (remf unknown :required-logical-fact)
           (pg::physical-recovery-write-control unknown 2 end pg::*next-xid*)
           (ok (null (getf (pg::physical-recovery-read-control root) :logical-fact)))
           (remf base :required-logical-fact)
           (setf (getf reservation :through) start)
           (let* ((before (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))
                  (candidate (pg::physical-follow-copy-replay-candidate store))
                  (result (pg::physical-follow-scan-replay-from-seed-core
                           candidate base (list end 1) (list end 1) pg::*next-xid* seed
                           (lambda (visit from) (pg::physical-store-map-durable-records candidate visit :start-lsn from))
                           nil)))
             (ok (equal fact (getf result :required-logical-fact)))
             (ok (intake-fails-p (lambda () (pg::physical-follow-check-policy-publication result binding))))
             (ok (equalp before (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
             (setf (getf (getf binding :recovery) :logical-fact) (copy-list fact))
             (ok (eq result (pg::physical-follow-check-policy-publication result binding)))
             (ok (not (pg::physical-store-failed-p store)))))))))

(deftest source-logical-ahead-restartpoint-keeps-older-unknown-required-boundary
    (dolist (enabled '(nil t))
      (call-with-source-logical-restartpoint-fixture
       enabled
       (lambda (root store snapshot fact)
         (let* ((required 192) (end pg::*last-replay-lsn*)
                               (state (copy-tree pg::*physical-recovery-state*))
                               (resource (pg::wal-copy-control-resource-fields snapshot))
                               (binding (list :required (list required 1) :recovery (list :phase 2))))
           (ok (< required (getf snapshot :start) end))
           (ok (null (nth-value 2 (pg::physical-recovery-scan-resource-fact
                                   store (pg::physical-store-base-lsn store) required nil state :logical-seed nil))))
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (remf state :source-logical-fact)
           (remf state :required-logical-fact)
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
             (ok (null (getf result :required-logical-fact)))
             (ok (equal fact (getf result :applied-logical-fact)))
             (ok (eq result (pg::physical-follow-check-policy-publication result binding)))
             (ok (equalp before (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))))
           (ok (= end (pg::physical-restartpoint-prepare-storage store)))
           (ok (equal fact (getf pg::*physical-recovery-state* :source-logical-fact)))
           (ok (equal fact (getf pg::*physical-recovery-state* :required-logical-fact)))
           (ok (= end (getf pg::*physical-recovery-state* :required-applied))))))))

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
