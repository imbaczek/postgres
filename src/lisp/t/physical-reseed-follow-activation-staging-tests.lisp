(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-activation-staged-record-preserves-building
    ((selected boolean))
  (:about pg::physical-reseed-follow-activation-staged-record)
  (:trials (:normal 100))
  (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture selected)
    (let* ((before (reseed-follow-with-activation base (first groups)))
           (entry (getf (second groups) :entry))
           (saved (pg::physical-adoption-attempt-value-copy (list before entry)))
           (actual (funcall (symbol-function 'pg::physical-reseed-follow-activation-staged-record)
                            before entry))
           (expected (reseed-follow-with-activation base (second groups))))
      (and (equal actual expected)
           (equal saved (list before entry))))))

(defun call-with-reseed-building-activation-fixture (kind function &optional after)
  (call-with-reseed-activation-fixture
   kind
   (lambda (owner root workspace settled)
     (declare (ignore settled))
     (funcall function owner root workspace (pg::physical-reseed-follow-begin-activation owner)))
   after))

(defun check-reseed-activation-output-blocks-owner (root)
  (let* ((symbol 'pg::physical-reseed-follow-activation-output-state)
         (original (symbol-function symbol)) (proofs 0) (admitted nil))
    (unwind-protect
         (progn
           (setf (symbol-function symbol)
                 (lambda (&rest arguments) (incf proofs) (apply original arguments)))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-physical-reseed-follow-configuration-owner
                   root (lambda (owner) (declare (ignore owner)) (setf admitted t))))))
           (ok (not admitted))
           (ok (plusp proofs)))
      (setf (symbol-function symbol) original))))

(deftest generated-reseed-activation-staged-record-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-activation-staged-record-preserves-building
                                       :seed 134 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-activation-staging-is-durable-and-idempotent-in-every-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-activation-fixture
       kind
       (lambda (owner root workspace settled)
         (declare (ignore settled))
         (let* ((building (pg::physical-reseed-follow-begin-activation owner))
                (pending (lineage-test-bytes (merge-pathnames "physical-follow.control" root)))
                (recovery (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))
                (staged (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))
                (activation (getf staged :activation))
                (path (merge-pathnames "staged/follow.active" workspace))
                (bytes (lineage-test-bytes path))
                (snapshot (activation-test-snapshot owner)))
           (ok (eq :staged (getf activation :phase)))
           (ok (zerop (getf activation :step)))
           (ok (equal (getf (getf building :activation) :image) (getf activation :image)))
           (ok (equal (getf activation :image)
                      (list (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
           (ok (equal staged (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
           (ok (equal snapshot (activation-test-snapshot owner)))
           (ok (equal staged (pg::physical-reseed-follow-read owner)))
           (ok (equalp pending (lineage-test-bytes (merge-pathnames "physical-follow.control" root))))
           (ok (equalp recovery (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :staged (getf (getf (pg::physical-reseed-follow-read owner) :activation) :phase)))))))))

(deftest reseed-activation-staging-requires-building-intent
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok (not (probe-file (merge-pathnames "staged/follow.active" workspace))))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-staging-retries-complete-output-after-every-sync-cut
    (dolist (cut '(:after-configuration-file-fsync
                   :before-activation-output-fsync :after-activation-output-fsync
                   :before-activation-staged-fsync :after-activation-staged-fsync
                   :before-activation-workspace-fsync :after-activation-workspace-fsync
                   :before-activation-ancestors-fsync :after-activation-ancestors-fsync))
      (call-with-reseed-building-activation-fixture
       :same-timeline
       (lambda (owner root workspace building)
         (declare (ignore workspace))
         (let* ((hit nil)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (eq event cut) (setf hit t) (error "active output sync cut")))))
           (ok (intake-fails-p
                (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
           (ok hit)
           (ok (equalp (pg::physical-reseed-follow-image building)
                       (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
       (lambda (root workspace)
         (let* ((path (merge-pathnames "staged/follow.active" workspace))
                (facts (pg::physical-reseed-file-facts (sb-posix:lstat path)))
                (preserved (activation-test-unrelated-tree root workspace)))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let ((record (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
                (ok (eq :staged (getf (getf record :activation) :phase))))))
           (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat path))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (activation-test-unrelated-tree root workspace)))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))))

(deftest reseed-activation-staging-recovers-or-preserves-every-journal-cut
    (dolist (cut '(:after-follow-temporary-create :after-follow-temporary-fsync
                   :before-follow-rename :after-follow-rename :after-follow-directory-fsync))
      (call-with-reseed-building-activation-fixture
       :same-timeline
       (lambda (owner root workspace building)
         (declare (ignore root workspace building))
         (let* ((hit nil)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (eq event cut) (setf hit t) (error "Staged journal cut")))))
           (ok (intake-fails-p
                (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
           (ok hit)
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))))
       (lambda (root workspace)
         (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (final (merge-pathnames "physical-reseed.follow" root))
                (before (lineage-test-bytes final))
                (preserved (activation-test-unrelated-tree root workspace)))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (if (eq cut :after-follow-temporary-create)
                  (let ((partial (lineage-test-bytes temporary)))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-reseed-follow-recover-activation owner))))
                    (ok (equalp partial (lineage-test-bytes temporary)))
                    (ok (equalp before (lineage-test-bytes final))))
                  (let ((record (pg::physical-reseed-follow-recover-activation owner)))
                    (ok (eq :staged (getf (getf record :activation) :phase)))
                    (ok (not (probe-file temporary)))
                    (ok (equal record (pg::physical-reseed-follow-read owner)))))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (activation-test-unrelated-tree root workspace))))))))

(deftest reseed-activation-staging-preserves-partial-output-and-refuses-reopen
    (dolist (cut '(:after-configuration-file-create :after-configuration-write-chunk))
      (let ((preserved nil) (journal nil))
        (call-with-reseed-building-activation-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore building))
           (setf journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
           (let* ((hit nil)
                  (pg::*physical-reseed-follow-write-limit* 1)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event cut) (setf hit t) (error "partial active output cut")))))
             (ok (intake-fails-p
                  (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
             (ok hit))
           (setf preserved (list (reseed-mapped-test-tree-facts root)
                                 (reseed-mapped-test-tree-facts workspace)))
           (ok (<= (length (lineage-test-bytes (merge-pathnames "staged/follow.active" workspace))) 1))
           (throw *reseed-follow-fixture-owner-exit* :partial))
         (lambda (root workspace)
           (check-reseed-activation-output-blocks-owner root)
           (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (list (reseed-mapped-test-tree-facts root)
                                (reseed-mapped-test-tree-facts workspace)))))))))

(defun activation-staging-test-image (root)
  (let* ((pg::*pgdata* root) (config (pg::physical-follow-read-config)))
    (setf (getf (fifth config) :phase) :active
          (getf (sixth config) :enabled) t)
    (pg::physical-follow-config-image config)))

(deftest reseed-activation-staging-requires-a-durability-proof-and-uses-reserved-capacity
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (bytes (activation-staging-test-image root)))
         (with-open-file (out path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
           (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
           (write-sequence bytes out))
         (let ((snapshot (activation-test-snapshot owner)))
           (pg::call-with-physical-reseed-follow-activation-operation
            owner
            (lambda (fd check)
              (declare (ignore fd))
              (let* ((context (pg::physical-reseed-follow-activation-context owner))
                     (candidate (pg::physical-reseed-follow-activation-staged-record
                                 building (pg::physical-reseed-follow-activation-context-output-entry context)))
                     (pg::*physical-reseed-follow-activation-candidate* (pg::physical-reseed-follow-copy candidate)))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-configuration-publication-candidate owner context candidate))))
                (ok (funcall check))
                (values building (pg::physical-reseed-follow-activation-context-snapshot context)))))
           (ok (equal snapshot (activation-test-snapshot owner))))
         (call-with-reseed-insufficient-capacity
          (lambda (calls)
            (let ((record (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
              (ok (eq :staged (getf (getf record :activation) :phase))))
            (ok (zerop (funcall calls)))))))))

(deftest reseed-activation-staging-keeps-invalid-output-entries-fenced
    (dolist (kind '(:corrupt :directory :symlink :hardlink))
      (let ((preserved nil))
        (call-with-reseed-building-activation-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore building))
           (let ((path (merge-pathnames "staged/follow.active" workspace)))
             (ecase kind
               (:corrupt
                (let ((bytes (activation-staging-test-image root)))
                  (setf (aref bytes 0) (logxor 1 (aref bytes 0)))
                  (reseed-follow-owner-test-write path bytes)))
               (:directory (sb-posix:mkdir path #o700))
               (:symlink (sb-posix:symlink (merge-pathnames "physical-follow.control" root) path))
               (:hardlink (sb-posix:link (merge-pathnames "physical-follow.control" root) path)))
             (setf preserved (list (reseed-mapped-test-tree-facts root)
                                   (reseed-mapped-test-tree-facts workspace)))
             (ok (intake-fails-p
                  (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
             (ok (reseed-activation-layout-facts-equal-p
                  preserved (list (reseed-mapped-test-tree-facts root)
                                  (reseed-mapped-test-tree-facts workspace))))
             (throw *reseed-follow-fixture-owner-exit* :invalid)))
         (lambda (root workspace)
           (check-reseed-activation-output-blocks-owner root)
           (ok (reseed-activation-layout-facts-equal-p
                preserved (list (reseed-mapped-test-tree-facts root)
                                (reseed-mapped-test-tree-facts workspace)))))))))

(deftest reseed-activation-staging-refuses-a-byte-identical-file-replacement-before-sync
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (hit nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event where)
                  (declare (ignore where))
                  (when (eq event :before-activation-output-fsync)
                    (let ((replacement (merge-pathnames "staged/replacement-active" workspace)))
                      (reseed-follow-owner-test-write replacement (lineage-test-bytes path))
                      (sb-posix:rename replacement path)
                      (setf hit t))))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok hit)
         (ok (equalp (pg::physical-reseed-follow-image building)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
     (lambda (root workspace)
       (let ((inode (sb-posix:stat-ino (sb-posix:lstat (merge-pathnames "staged/follow.active" workspace)))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (let ((record (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
                   (ok (eq :staged (getf (getf record :activation) :phase)))
                   (ok (= inode (fifth (getf (getf record :activation) :entry)))))))))))

(deftest reseed-activation-staging-rejects-a-byte-identical-write-after-durability-proof
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (hit nil) (changed nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event where)
                  (declare (ignore where))
                  (when (eq event :before-follow-rename)
                    (let* ((context (pg::physical-reseed-follow-activation-context owner))
                           (parent (pg::physical-reseed-follow-configuration-role-fd
                                    (pg::physical-reseed-follow-activation-context-descriptors context) :staged))
                           (mount (pg::physical-adoption-install-mount-id nil :directory-fd parent))
                           (before (pg::physical-reseed-install-old-observe parent "follow.active" mount)))
                      (with-open-file (out path :direction :io :if-exists :overwrite :if-does-not-exist :error
                                                :element-type '(unsigned-byte 8))
                        (let ((byte (read-byte out)))
                          (file-position out 0) (write-byte byte out)))
                      (setf hit t changed
                            (not (equal before (pg::physical-reseed-install-old-observe parent "follow.active" mount)))))))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok hit)
         (ok changed)
         (ok (equalp (pg::physical-reseed-follow-image building)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root (lambda (owner)
               (let ((record (pg::physical-reseed-follow-recover-activation owner)))
                 (ok (eq :staged (getf (getf record :activation) :phase)))
                 (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))

(deftest reseed-activation-staging-rechecks-abort-before-output-sync
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore workspace))
       (let* ((hit nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event path)
                  (declare (ignore path))
                  (when (eq event :before-activation-output-fsync)
                    (reseed-settlement-test-abort-attempt root building)
                    (setf hit t)))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok hit)
         (ok (equalp (pg::physical-reseed-follow-image building)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
     (lambda (root workspace)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (facts (pg::physical-reseed-file-facts (sb-posix:lstat path))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :building (getf (getf (pg::physical-reseed-follow-read owner) :activation) :phase)))
                 (ok (intake-fails-p
                      (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))))
         (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat path))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))

(deftest reseed-activation-staging-recovers-from-real-fsync-failures
    (loop for fail-at from 1 to 3
          for leaf in '("staged/follow.active" "staged/" "")
          do (call-with-reseed-building-activation-fixture
              :same-timeline
              (lambda (owner root workspace building)
                (let* ((symbol 'sb-posix:fsync) (original (symbol-function symbol))
                                                (armed nil) (calls 0) (injected nil) (failed-inode nil)
                                                (pg::*physical-reseed-follow-test-hook*
                                                  (lambda (event path)
                                                    (declare (ignore path))
                                                    (when (eq event :before-activation-output-fsync) (setf armed t)))))
                  (unwind-protect
                       (progn
                         (setf (symbol-function symbol)
                               (lambda (fd)
                                 (when armed
                                   (incf calls)
                                   (when (= calls fail-at)
                                     (setf injected t failed-inode (sb-posix:stat-ino (sb-posix:fstat fd)))
                                     (error "injected active output fsync failure")))
                                 (funcall original fd)))
                         (ok (intake-fails-p
                              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))))
                    (setf (symbol-function symbol) original))
                  (ok injected)
                  (ok (= failed-inode (sb-posix:stat-ino (sb-posix:lstat (merge-pathnames leaf workspace)))))
                  (ok (equalp (pg::physical-reseed-follow-image building)
                              (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
              (lambda (root workspace)
                (declare (ignore workspace))
                (pg::call-with-physical-reseed-follow-configuration-owner
                 root (lambda (owner)
                        (ok (eq :staged
                                (getf (getf (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)
                                            :activation) :phase)))))))))
