(in-package #:pg-physical-tests)

(deftest reseed-follow-settle-persists-terminal-receipt-and-retries-without-network
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let* ((initial (pg::physical-reseed-follow-read owner))
              (pending (pg::physical-reseed-follow-copy
                        (append initial
                                (list :settlement
                                      (list :phase :handoff-pending
                                            :successor (pg::physical-reseed-follow-settlement-successor initial)
                                            :receipt nil))))))
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (let* ((record (pg::physical-reseed-follow-settle owner :helper "/unused/helper"))
                   (bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
              (ok (eq :handed-off (getf (getf record :settlement) :phase)))
              (ok (equal (getf initial :configuration) (getf record :configuration)))
              (ok (equal record (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
              (ok (equalp bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
              (ok (= 1 (getf state :statuses) (getf state :handoffs) (getf state :closes))))
            (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))

(deftest reseed-follow-settle-recovers-complete-terminal-temporary-offline
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (let* ((injected nil)
                   (pg::*physical-reseed-follow-test-hook*
                     (lambda (event path)
                       (declare (ignore path))
                       (when (eq event :after-follow-temporary-readback)
                         (setf injected t) (error "terminal temporary cut")))))
              (ok (intake-fails-p
                   (lambda () (pg::physical-reseed-follow-settle owner :helper "/unused/helper"))))
              (ok injected)
              (ok (= 1 (getf state :handoffs)))
              (ok (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))
              (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root (lambda (owner)
               (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                 (ok (eq :handed-off (getf (getf record :settlement) :phase)))
                 (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                 (ok (equal record (pg::physical-reseed-follow-read owner)))
                 (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))

(deftest reseed-follow-settle-aborted-attempt-uses-status-only
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let* ((pending (pg::physical-reseed-follow-begin-settlement owner))
              (attempt (pg::physical-reseed-attempt-copy (getf pending :attempt)))
              (replacement (merge-pathnames "replacement-attempt" root)))
         (setf (getf attempt :phase) :aborted)
         (reseed-follow-owner-test-write replacement (pg::physical-reseed-attempt-image attempt))
         (sb-posix:rename replacement (pg::physical-reseed-attempt-path root))
         (pg::fsync-directory root)
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (ok (equal pending (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
            (ok (= 0 (getf state :handoffs) (getf state :identities)))
            (setf (getf state :phase) :closed (getf state :outcome) :aborted)
            (let ((record (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
              (ok (eq :source-aborted (getf (getf record :settlement) :phase)))
              (ok (eq :inert (getf (getf record :configuration) :phase))))
            (ok (= 0 (getf state :handoffs) (getf state :identities)))
            (ok (= 2 (getf state :statuses) (getf state :closes)))))))))

(defun reseed-settlement-test-abort-attempt (root pending &optional (phase :aborted))
  (let ((attempt (pg::physical-reseed-attempt-copy (getf pending :attempt)))
        (replacement (merge-pathnames "fixture-attempt" root)))
    (setf (getf attempt :phase) phase)
    (reseed-follow-owner-test-write replacement (pg::physical-reseed-attempt-image attempt))
    (sb-posix:rename replacement (pg::physical-reseed-attempt-path root))
    (pg::fsync-directory root)))

(deftest reseed-follow-terminal-publication-refuses-stale-snapshots
    (dolist (mutation '(:follow :attempt))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
           (call-with-reseed-settlement-source
            owner root pending
            (lambda (state)
              (setf (getf state :phase) :closed)
              (multiple-value-bind (receipt expected)
                  (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper")
                (ecase mutation
                  (:follow (reseed-test-replace-follow-record root pending))
                  (:attempt (reseed-settlement-test-abort-attempt root pending :aborting)))
                (let ((before (list (reseed-mapped-test-tree-facts root)
                                    (reseed-mapped-test-tree-facts workspace)))
                      (effects 0))
                  (let ((pg::*physical-reseed-follow-test-hook*
                          (lambda (event path)
                            (declare (ignore path))
                            (when (eq event :before-follow-temporary-create) (incf effects)))))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-reseed-follow-publish-settlement-result
                                     owner receipt expected)))))
                  (ok (= 0 effects))
                  (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                  (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                  (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                           (reseed-mapped-test-tree-facts workspace)))))))))))))

(deftest reseed-follow-terminal-authentication-and-aborted-historical-handoff
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (reseed-settlement-test-abort-attempt root pending)
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (setf (getf state :phase) :closed (getf state :identity-timeline) 99)
            (let ((receipt (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper")))
              (pg::call-with-physical-reseed-follow-installation-operation
               owner
               (lambda (fd check)
                 (declare (ignore fd))
                 (let* ((context (pg::physical-reseed-follow-installation-context owner))
                        (snapshot (pg::physical-reseed-follow-installation-context-snapshot context))
                        (candidate (pg::physical-reseed-follow-copy pending)))
                   (setf (getf candidate :settlement)
                         (list :phase :handed-off :successor (getf (getf pending :settlement) :successor)
                               :receipt receipt))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-reseed-follow-configuration-publication-candidate
                                    owner context candidate))))
                   (let ((pg::*physical-reseed-follow-terminal-settlement-candidate* candidate))
                     (ok (equal candidate (pg::physical-reseed-follow-configuration-publication-candidate
                                           owner context candidate)))
                     (let ((pg::*physical-reseed-follow-settlement-candidate* candidate))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-configuration-publication-candidate
                                        owner context candidate)))))
                     (let ((pg::*physical-reseed-follow-installation-candidate* candidate))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-configuration-publication-candidate
                                        owner context candidate)))))
                     (ok (intake-fails-p
                          (lambda () (pg::physical-reseed-follow-configuration-publication-candidate
                                      (pg::copy-physical-reseed-follow-configuration-owner owner)
                                      context candidate)))))
                   (funcall check)
                   (ok (equal snapshot (pg::physical-reseed-follow-installation-context-snapshot context)))
                   (values (getf snapshot :record) snapshot)))))
            (let ((record (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
              (ok (eq :handed-off (getf (getf record :settlement) :phase)))
              (ok (eq :inert (getf (getf record :configuration) :phase))))
            (ok (= 0 (getf state :handoffs) (getf state :identities)))
            (ok (= 2 (getf state :statuses) (getf state :closes)))))))))

(deftest reseed-follow-terminal-temporary-recovers-after-aborted-attempt
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (reseed-settlement-test-abort-attempt root pending)
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (setf (getf state :phase) :closed (getf state :outcome) :aborted)
            (let* ((injected nil)
                   (pg::*physical-reseed-follow-test-hook*
                     (lambda (event path)
                       (declare (ignore path))
                       (when (eq event :after-follow-temporary-readback)
                         (setf injected t) (error "historical terminal temporary cut")))))
              (ok (intake-fails-p
                   (lambda () (pg::physical-reseed-follow-settle owner :helper "/unused/helper"))))
              (ok injected)
              (ok (= 0 (getf state :handoffs)))
              (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let ((original (symbol-function 'pg::physical-libpq-connect-frozen)) (connections 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                      (lambda (&rest arguments)
                        (declare (ignore arguments))
                        (incf connections) (error "offline recovery attempted a connection")))
                (pg::call-with-physical-reseed-follow-configuration-owner
                 root (lambda (owner)
                        (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                          (ok (eq :source-aborted (getf (getf record :settlement) :phase)))
                          (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                          (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))
                (ok (= 0 connections)))
           (setf (symbol-function 'pg::physical-libpq-connect-frozen) original))))))

(deftest reseed-follow-settle-supports-every-installed-proof-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-inert-settlement-fixture
       kind
       (lambda (owner root workspace)
         (declare (ignore workspace))
         (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
           (call-with-reseed-settlement-source
            owner root pending
            (lambda (state)
              (let ((record (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
                (ok (eq :handed-off (getf (getf record :settlement) :phase)))
                (ok (equal (getf pending :configuration) (getf record :configuration)))
                (ok (equal (getf pending :generations) (getf record :generations)))
                (ok (equal (getf pending :base) (getf record :base)))
                (ok (equal (getf (getf pending :settlement) :successor)
                           (getf (getf record :settlement) :successor)))
                (ok (equal record (pg::physical-reseed-follow-read owner)))
                (ok (= 1 (getf state :statuses) (getf state :handoffs) (getf state :closes))))
              (let* ((pg::*pgdata* root) (config (pg::physical-follow-read-config)))
                (ok (eq :pending (getf (fifth config) :phase)))
                (ok (not (getf (sixth config) :enabled))))))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :handed-off
                         (getf (getf (pg::physical-reseed-follow-read owner) :settlement) :phase)))))))))
