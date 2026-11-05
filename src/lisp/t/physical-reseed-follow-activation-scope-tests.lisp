(in-package #:pg-physical-tests)

(defun activation-test-call (name &rest arguments)
  (apply (symbol-function name) arguments))

(defun call-with-reseed-activation-fixture (kind function &optional after)
  (call-with-reseed-inert-settlement-fixture
   kind
   (lambda (owner root workspace)
     (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
       (call-with-reseed-settlement-source
        owner root pending
        (lambda (state)
          (declare (ignore state))
          (let ((settled (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
            (funcall function owner root workspace settled))))))
   after))

(defun activation-test-snapshot (owner)
  (pg::call-with-physical-directory-root
   owner (lambda (fd directory)
           (declare (ignore directory))
           (pg::physical-reseed-follow-snapshot-at fd))))

(defun activation-test-unrelated-tree (root workspace)
  (let* ((names '("physical-reseed.follow" "physical-reseed.follow.tmp"))
         (facts (remove-if
                 (lambda (row) (and (eq :file (first row))
                                    (member (first (second row)) names :test #'equal)))
                 (reseed-mapped-test-tree-facts root)))
         (directory (first facts)))
    (setf (second directory) (subseq (second directory) 0 7)
          (third directory) (set-difference (third directory) names :test #'equal))
    (list facts (reseed-mapped-test-tree-facts workspace))))

(deftest reseed-activation-context-holds-terminal-state-and-revokes-borrowed-store
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore root workspace))
       (let ((escaped nil) (store nil))
         (ok (equal settled
                    (activation-test-call
                     'pg::call-with-physical-reseed-follow-activation-operation owner
                     (lambda (fd check)
                       (declare (ignore fd))
                       (setf escaped (activation-test-call 'pg::physical-reseed-follow-activation-context owner)
                             store (activation-test-call 'pg::physical-reseed-follow-activation-context-store escaped))
                       (ok (funcall check))
                       (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))
                       (let ((refused nil))
                         (sb-thread:join-thread
                          (sb-thread:make-thread
                           (lambda ()
                             (setf refused
                                   (intake-fails-p
                                    (lambda ()
                                      (activation-test-call 'pg::physical-reseed-follow-activation-guard escaped)))))))
                         (ok refused))
                       (values (activation-test-call 'pg::physical-reseed-follow-activation-context-record escaped)
                               (activation-test-call 'pg::physical-reseed-follow-activation-context-snapshot escaped))))))
         (ok (not (activation-test-call 'pg::physical-reseed-follow-activation-context-active-p escaped)))
         (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-activation-guard escaped))))
         (ok (intake-fails-p (lambda () (pg::physical-store-check-live store))))))))

(deftest reseed-activation-aborted-attempt-allows-inspection-and-refuses-building
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace))
       (reseed-settlement-test-abort-attempt root settled)
       (ok (equal settled (pg::physical-reseed-follow-read owner)))
       (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))))
       (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))

(deftest reseed-activation-context-proves-the-actual-journal-at-each-move-prefix
    (dolist (prefix '(0 1 2))
      (call-with-reseed-activation-fixture
       :same-timeline
       (lambda (owner root workspace settled)
         (declare (ignore settled))
         (let* ((record (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))
                (pg::*pgdata* root)
                (config (pg::physical-follow-read-config))
                (path (merge-pathnames "staged/follow.active" workspace)))
           (setf (getf (fifth config) :phase) :active
                 (getf (sixth config) :enabled) t)
           (let* ((bytes (pg::physical-follow-config-image config))
                  (activation (getf record :activation)))
             (reseed-follow-owner-test-write path bytes)
             (let ((stat (sb-posix:lstat path)))
               (setf (getf activation :phase) :activating
                     (getf activation :step) (max 0 (1- prefix))
                     (getf activation :entry)
                     (list "follow.active" :file #o600 (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
                           (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
             (when (plusp prefix)
               (sb-posix:rename (pg::physical-follow-config-path)
                                (merge-pathnames "saved/follow.pending" workspace)))
             (when (= prefix 2) (sb-posix:rename path (pg::physical-follow-config-path)))
             (reseed-test-replace-follow-record root record)
             (ok (equal record (pg::physical-reseed-follow-read owner)))
             (ok (equal record
                        (activation-test-call
                         'pg::call-with-physical-reseed-follow-activation-operation owner
                         (lambda (fd check)
                           (declare (ignore fd))
                           (let ((context (activation-test-call 'pg::physical-reseed-follow-activation-context owner)))
                             (ok (= prefix (activation-test-call 'pg::physical-reseed-follow-activation-context-prefix context)))
                             (ok (funcall check))
                             (values record (activation-test-call 'pg::physical-reseed-follow-activation-context-snapshot context)))))))))))))

(deftest reseed-activation-begin-refuses-insufficient-capacity-before-a-temporary
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace settled))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (call-with-reseed-insufficient-capacity
          (lambda (calls)
            (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))))
            (ok (plusp (funcall calls)))
            (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
            (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))))

(deftest reseed-activation-begin-refuses-unsettled-source-state
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (pg::physical-reseed-follow-begin-settlement owner)
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-scope-refuses-an-actual-attempt-change-before-publication
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (intake-fails-p
              (lambda ()
                (activation-test-call
                 'pg::call-with-physical-reseed-follow-activation-operation owner
                 (lambda (fd check)
                   (declare (ignore fd))
                   (reseed-settlement-test-abort-attempt root settled)
                   (funcall check))))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-scope-refuses-a-detached-context-record
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore root workspace settled))
       (activation-test-call
        'pg::call-with-physical-reseed-follow-activation-operation owner
        (lambda (fd check)
          (declare (ignore fd))
          (let* ((context (activation-test-call 'pg::physical-reseed-follow-activation-context owner))
                 (record (activation-test-call 'pg::physical-reseed-follow-activation-context-record context))
                 (setter (fdefinition '(setf pg::physical-reseed-follow-activation-context-record))))
            (unwind-protect
                 (progn
                   (funcall setter (pg::physical-reseed-follow-core-projection record) context)
                   (ok (intake-fails-p (lambda () (funcall check)))))
              (funcall setter record context))
            (ok (funcall check))
            (values record (activation-test-call 'pg::physical-reseed-follow-activation-context-snapshot context))))))))

(deftest reseed-activation-building-candidate-refuses-a-forged-active-image
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace settled))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (intake-fails-p
              (lambda ()
                (activation-test-call
                 'pg::call-with-physical-reseed-follow-activation-operation owner
                 (lambda (fd check)
                   (declare (ignore fd))
                   (let* ((context (activation-test-call 'pg::physical-reseed-follow-activation-context owner))
                          (record (pg::physical-reseed-follow-copy
                                   (activation-test-call 'pg::physical-reseed-follow-activation-context-record context)))
                          (binding (activation-test-call 'pg::physical-reseed-follow-activation-context-binding context))
                          (bytes (pg::physical-follow-config-image
                                  (pg::physical-reseed-follow-activation-config record binding))))
                     (setf (getf record :activation)
                           (list :phase :building :step 0 :image
                                 (list (length bytes) (logxor 1 (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
                                 :entry nil))
                     (activation-test-call 'pg::physical-reseed-follow-activation-publish-building context record check)))))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-publication-rechecks-its-frozen-candidate-before-create
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace settled))
       (let* ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
              (hit nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event path)
                  (declare (ignore path))
                  (when (eq event :before-follow-temporary-create)
                    (setf hit t)
                    (let ((candidate (symbol-value 'pg::*physical-reseed-follow-activation-candidate*)))
                      (incf (second (getf (getf candidate :activation) :image))))))))
         (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))))
         (ok hit)
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-publication-refuses-a-replaced-equal-journal-before-create
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace settled))
       (let* ((path (merge-pathnames "physical-reseed.follow" root))
              (before (lineage-test-bytes path))
              (hit nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event where)
                  (declare (ignore where))
                  (when (eq event :before-follow-temporary-create)
                    (setf hit t)
                    (let ((replacement (merge-pathnames "replacement-follow" root)))
                      (reseed-follow-owner-test-write replacement before)
                      (sb-posix:rename replacement path))))))
         (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))))
         (ok hit)
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (equalp before (lineage-test-bytes path)))))))

(deftest reseed-activation-building-scope-freezes-its-output-observation
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (let* ((record (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))
              (pg::*pgdata* root)
              (config (pg::physical-follow-read-config))
              (path (merge-pathnames "staged/follow.active" workspace)))
         (setf (getf (fifth config) :phase) :active
               (getf (sixth config) :enabled) t)
         (let ((bytes (pg::physical-follow-config-image config)))
           (activation-test-call
            'pg::call-with-physical-reseed-follow-activation-operation owner
            (lambda (fd check)
              (declare (ignore fd))
              (let ((context (activation-test-call 'pg::physical-reseed-follow-activation-context owner)))
                (unwind-protect
                     (progn
                       (reseed-follow-owner-test-write path bytes)
                       (ok (intake-fails-p (lambda () (funcall check)))))
                  (delete-file path))
                (ok (funcall check))
                (values record (activation-test-call 'pg::physical-reseed-follow-activation-context-snapshot context)))))
           (reseed-follow-owner-test-write path bytes)
           (ok (equal record (pg::physical-reseed-follow-read owner))))))))

(deftest reseed-activation-publication-recovers-or-preserves-every-journal-cut
    (dolist (cut '(:after-follow-temporary-create :after-follow-temporary-fsync
                   :before-follow-rename :after-follow-rename :after-follow-directory-fsync))
      (let ((preserved nil))
        (call-with-reseed-activation-fixture
         :same-timeline
         (lambda (owner root workspace settled)
           (declare (ignore settled))
           (setf preserved (activation-test-unrelated-tree root workspace))
           (let* ((hit nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event cut) (setf hit t) (error "activation journal cut")))))
             (ok (intake-fails-p (lambda () (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))))
             (ok hit)
             (ok (pg::physical-reseed-follow-owner-uncertain-p owner))))
         (lambda (root workspace)
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                     (final (merge-pathnames "physical-reseed.follow" root))
                     (before (lineage-test-bytes final)))
                (if (eq cut :after-follow-temporary-create)
                    (let ((partial (lineage-test-bytes temporary)))
                      (ok (intake-fails-p
                           (lambda () (activation-test-call 'pg::physical-reseed-follow-recover-activation owner))))
                      (ok (equalp partial (lineage-test-bytes temporary)))
                      (ok (equalp before (lineage-test-bytes final))))
                    (let ((record (activation-test-call 'pg::physical-reseed-follow-recover-activation owner)))
                      (ok (eq :building (getf (getf record :activation) :phase)))
                      (ok (not (probe-file temporary)))
                      (ok (equal record (pg::physical-reseed-follow-read owner))))))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (activation-test-unrelated-tree root workspace))))))))

(deftest reseed-activation-begin-publishes-only-building-and-retries-without-rewriting
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-activation-fixture
       kind
       (lambda (owner root workspace settled)
         (let* ((pg::*pgdata* root)
                (pending (lineage-test-bytes (pg::physical-follow-config-path)))
                (recovery (lineage-test-bytes (pg::physical-recovery-control-path)))
                (active (pg::physical-follow-read-config)))
           (setf (getf (fifth active) :phase) :active
                 (getf (sixth active) :enabled) t)
           (let* ((bytes (pg::physical-follow-config-image active))
                  (record (activation-test-call 'pg::physical-reseed-follow-begin-activation owner))
                  (snapshot (activation-test-snapshot owner)))
             (ok (equal (getf record :activation)
                        (list :phase :building :step 0 :image
                              (list (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
                              :entry nil)))
             (let ((old (pg::physical-reseed-follow-copy record)))
               (remf old :activation)
               (ok (equal settled old)))
             (ok (equal record (activation-test-call 'pg::physical-reseed-follow-begin-activation owner)))
             (ok (equal snapshot (activation-test-snapshot owner)))
             (ok (equal record (pg::physical-reseed-follow-read owner)))
             (ok (equalp pending (lineage-test-bytes (pg::physical-follow-config-path))))
             (ok (equalp recovery (lineage-test-bytes (pg::physical-recovery-control-path))))
             (ok (not (probe-file (merge-pathnames "staged/follow.active" workspace))))
             (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :building (getf (getf (pg::physical-reseed-follow-read owner) :activation) :phase)))))))))
