(in-package #:pg-physical-tests)

(defun call-with-reseed-original-base-fixture (function)
  (call-with-reseed-follow-installed-fixture
   (lambda (root stage incoming quarantine plan)
     (declare (ignore stage incoming quarantine plan))
     (let ((fd nil))
       (unwind-protect
            (progn
              (setf fd (pg::physical-backup-open-directory root))
              (pg::call-with-physical-backup-image-directory
               fd
               (lambda (directory)
                 (let ((pg::*pgdata* directory))
                   (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
                       (pg::wal-read-control-fields)
                     (declare (ignore checkpoint durable snapshot))
                     (let ((pg::*wal-segment-namespace* namespace)
                           (pg::*wal-insert-timeline* insertion)
                           (pg::*wal-checkpoint-timeline* checkpoint-timeline))
                       (funcall function root directory (pg::physical-recovery-read-control directory))))))))
         (when fd (sb-posix:close fd)))))))

(deftest reseed-original-base-control-core-matches-public-verification
    (call-with-reseed-original-base-fixture
     (lambda (root directory control)
       (declare (ignore root))
       (let* ((public (multiple-value-list (pg::physical-recovery-verify-base-with-namespace)))
              (before (copy-tree control))
              (core (multiple-value-list (pg::physical-recovery-verify-base-control-with-namespace control))))
         (let ((same (equalp public core))) (ok same))
         (ok (equalp before control))
         (ok (pg::recovery-check-image-directories (first core) directory))
         (ok (zerop (getf (second core) :phase)))))))

(deftest reseed-original-base-control-core-retains-immutable-file-checks
    (dolist (name '("PG_VERSION" "pg_xact" "global/pg_catalog.dat" "global/pg_wal_control" :checkpoint-wal))
      (call-with-reseed-original-base-fixture
       (lambda (root directory control)
         (let* ((manifest (pg::recovery-read-manifest directory))
                (relative (if (eq name :checkpoint-wal)
                              (first (find-if (lambda (entry) (and (uiop:string-prefix-p "pg_wal/" (first entry))
                                                                   (uiop:string-suffix-p (first entry) ".wal")))
                                              (getf manifest :files)))
                              name)))
           (ok relative)
           (with-open-file (stream (merge-pathnames relative root) :direction :io :if-exists :overwrite
                                                                   :element-type '(unsigned-byte 8))
             (let ((byte (read-byte stream nil 0)))
               (file-position stream 0)
               (write-byte (logxor byte 255) stream)
               (finish-output stream)))
           (ok (intake-fails-p
                (lambda () (pg::physical-recovery-verify-base-control-with-namespace control))))
           (ok (intake-fails-p (lambda () (pg::physical-recovery-verify-base-with-namespace)))))))))

(deftest reseed-original-base-public-verifier-still-rejects-extra-child-files
    (call-with-reseed-original-base-fixture
     (lambda (root directory control)
       (let ((path (merge-pathnames "pg_wal/unrelated" root)))
         (reseed-follow-owner-test-write path #(1 2 3))
         (let ((core (pg::physical-recovery-verify-base-control-with-namespace control)))
           (ok (equal (getf control :system-id) (getf core :system-id)))
           (ok (intake-fails-p (lambda () (pg::recovery-check-image-directories core directory))))
           (ok (intake-fails-p (lambda () (pg::recovery-check-image-metadata core directory))))
           (ok (intake-fails-p (lambda () (pg::physical-recovery-verify-base-with-namespace))))
           (ok (equalp #(1 2 3) (lineage-test-bytes path))))))))

(deftest reseed-original-base-directory-scope-closes-and-revokes-on-every-exit
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((root-fd (pg::physical-backup-open-directory root)))
         (unwind-protect
              (dolist (cut '(nil t))
                (let ((owned nil) (saved-check nil))
                  (let ((result
                          (multiple-value-list
                           (catch 'base-directory-cut
                             (pg::call-with-physical-reseed-follow-base-directories
                              root-fd plan
                              (lambda (children check)
                                (setf owned (mapcar #'cdr children) saved-check check)
                                (ok (equal '(:base :global :wal) (mapcar #'car children)))
                                (funcall check)
                                (if cut (throw 'base-directory-cut :cut) (values :one :two)))
                              (lambda () t))))))
                    (ok (equal (if cut '(:cut) '(:one :two)) result)))
                  (dolist (fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
                  (ok (intake-fails-p saved-check))
                  (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))))
           (sb-posix:close root-fd))))))

(deftest reseed-original-base-directory-scope-rechecks-named-descriptors
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((root-fd (pg::physical-backup-open-directory root))
             (calls 0) (original nil))
         (unwind-protect
              (progn
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-physical-reseed-follow-base-directories
                        root-fd plan
                        (lambda (children check)
                          (funcall check)
                          (setf original (sb-posix:stat-ino (sb-posix:fstat (cdr (assoc :global children)))))
                          (sb-posix:rename (namestring (merge-pathnames "global/" root))
                                           (namestring (merge-pathnames "saved-global/" root)))
                          (sb-posix:mkdir (namestring (merge-pathnames "global/" root)) #o700)
                          (incf calls))
                        (lambda () t)))))
                (ok (= calls 1))
                (ok (= original (sb-posix:stat-ino
                                 (sb-posix:lstat (namestring (merge-pathnames "saved-global/" root))))))
                (ok (/= original (sb-posix:stat-ino
                                  (sb-posix:lstat (namestring (merge-pathnames "global/" root)))))))
           (sb-posix:close root-fd))))))

(deftest reseed-original-base-proof-kernels-use-the-saved-original-control
    (call-with-reseed-follow-building-fixture
     :later-writer
     (lambda (owner root workspace building)
       (declare (ignore root))
       (let ((core (pg::physical-reseed-follow-core-projection building)))
         (pg::call-with-physical-reseed-follow-image
          owner
          (lambda (root-fd check)
            (pg::call-with-physical-backup-image-directory
             root-fd
             (lambda (directory)
               (let ((pg::*pgdata* directory))
                 (multiple-value-bind (checked base control)
                     (pg::physical-reseed-follow-check-local-intent owner root-fd core)
                   (declare (ignore checked control))
                   (multiple-value-bind (binding model readiness)
                       (pg::physical-reseed-follow-proof-at owner root-fd core)
                     (let ((expected (pg::physical-reseed-follow-configuration-images core binding model 12345))
                           (original (pg::physical-recovery-control-path directory))
                           (saved (merge-pathnames "saved.original" workspace)))
                       (sb-posix:rename (namestring original) (namestring saved))
                       (unwind-protect
                            (let ((actual-control
                                    (pg::physical-recovery-decode-control
                                     (pg::recovery-read-bytes saved :maximum 196))))
                              (ok (not (probe-file original)))
                              (ok (pg::physical-reseed-follow-check-base-intent core base directory))
                              (multiple-value-bind (actual-binding actual-model actual-readiness)
                                  (pg::physical-reseed-follow-proof-for-base core base actual-control)
                                (let ((same (equalp expected
                                                    (pg::physical-reseed-follow-configuration-images
                                                     core actual-binding actual-model 12345))))
                                  (ok same))
                                (ok (equalp readiness actual-readiness))))
                         (sb-posix:rename (namestring saved) (namestring original)))))))))
            (funcall check)))))))

(deftest reseed-held-base-read-context-owns-only-its-duplicates
    (call-with-reseed-original-base-fixture
     (lambda (root directory control)
       (declare (ignore root directory control))
       (let* ((outer pg::*physical-image-context*)
              (borrowed (cons (pg::physical-image-context-root-fd outer)
                              (loop for name in '("base" "global" "pg_wal")
                                    collect (cdr (assoc name (pg::physical-image-context-children outer) :test #'equal))))))
         (dolist (cut '(nil t))
           (let ((saved nil) (owned nil))
             (let ((result
                     (multiple-value-list
                      (catch 'held-image-cut
                        (pg::call-with-physical-reseed-follow-held-base-directory
                         (first borrowed) (second borrowed) (third borrowed) (fourth borrowed)
                         (lambda (path)
                           (setf saved pg::*physical-image-context*
                                 owned (cons (pg::physical-image-context-root-fd saved)
                                             (mapcar #'cdr (pg::physical-image-context-children saved))))
                           (ok (typep saved 'pg::physical-image-read-context))
                           (ok (null (intersection borrowed owned)))
                           (dolist (fd owned)
                             (ok (logtest 1 (sb-posix:fcntl fd sb-posix:f-getfd))))
                           (pg::recovery-check-manifest-files (pg::recovery-read-manifest path) path)
                           (ok (intake-fails-p
                                (lambda ()
                                  (pg::call-with-physical-image-output
                                   path "forbidden-output" (lambda (stream) (declare (ignore stream)) t)))))
                           (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "forbidden-output" path))))
                           (if cut (throw 'held-image-cut :cut) (values :one :two)))
                         (lambda () (pg::physical-image-check-children outer)))))))
               (ok (equal (if cut '(:cut) '(:one :two)) result)))
             (dolist (fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
             (ok (intake-fails-p (lambda () (pg::physical-image-check-context saved))))
             (dolist (fd borrowed) (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))))))))

(deftest reseed-held-base-read-context-keeps-held-inputs-and-refuses-a-swap
    (call-with-reseed-original-base-fixture
     (lambda (root directory control)
       (declare (ignore directory control))
       (let* ((outer pg::*physical-image-context*)
              (children (pg::physical-image-context-children outer))
              (global (merge-pathnames "global/" root))
              (saved (merge-pathnames "saved-global/" root))
              (before (lineage-test-bytes (merge-pathnames "pg_wal_control" global)))
              (calls 0))
         (unwind-protect
              (progn
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-physical-reseed-follow-held-base-directory
                        (pg::physical-image-context-root-fd outer)
                        (cdr (assoc "base" children :test #'equal))
                        (cdr (assoc "global" children :test #'equal))
                        (cdr (assoc "pg_wal" children :test #'equal))
                        (lambda (path)
                          (sb-posix:rename (namestring global) (namestring saved))
                          (sb-posix:mkdir (namestring global) #o700)
                          (let ((same (equalp before
                                              (pg::recovery-read-bytes
                                               (pg::pgdata-resolve-path "global/pg_wal_control" path)))))
                            (ok same))
                          (incf calls))
                        (lambda () t)))))
                (ok (= calls 1)))
           (when (probe-file saved)
             (sb-posix:rmdir (namestring global))
             (sb-posix:rename (namestring saved) (namestring global))))))))

(deftest reseed-fixed-configuration-directory-scope-revokes-checks-and-preserves-values
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore root building))
       (let ((record (pg::physical-reseed-follow-stage-configuration owner))
             (workspace-fd (pg::physical-backup-open-directory workspace)))
         (unwind-protect
              (dolist (cut '(nil t))
                (let ((owned nil) (saved-check nil))
                  (let ((result
                          (multiple-value-list
                           (catch 'fixed-configuration-cut
                             (pg::call-with-physical-reseed-follow-fixed-configuration-directories
                              workspace-fd record
                              (lambda (staged saved check)
                                (setf owned (list staged saved) saved-check check)
                                (funcall check)
                                (if cut (throw 'fixed-configuration-cut :cut) (values :one :two)))
                              (lambda () (pg::physical-reseed-check-owner owner)))))))
                    (ok (equal (if cut '(:cut) '(:one :two)) result)))
                  (dolist (fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
                  (ok (intake-fails-p saved-check))
                  (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat workspace-fd))))))
           (sb-posix:close workspace-fd))))))

(deftest reseed-configuration-prefix-locator-bounds-observed-progress
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore building))
       (let* ((record (pg::physical-reseed-follow-stage-configuration owner))
              (installing (pg::physical-reseed-follow-copy record))
              (workspace-fd (pg::physical-backup-open-directory workspace)))
         (setf (getf (getf installing :configuration) :phase) :installing)
         (unwind-protect
              (pg::call-with-physical-reseed-follow-image
               owner
               (lambda (root-fd image-check)
                 (pg::call-with-physical-reseed-follow-fixed-configuration-directories
                  workspace-fd record
                  (lambda (staged-fd saved-fd fixed-check)
                    (flet ((observe (value)
                             (pg::physical-reseed-follow-configuration-initial-prefix
                              root-fd workspace-fd staged-fd saved-fd
                              (pg::physical-reseed-install-owner-plan owner) value
                              (pg::physical-reseed-install-extra-entries owner root-fd))))
                      (ok (zerop (observe record)))
                      (let ((receive (merge-pathnames "receive/" workspace))
                            (spool (merge-pathnames "pg_physical_receive/" root))
                            (pending (merge-pathnames "staged/follow.pending" workspace))
                            (installed (merge-pathnames "physical-follow.control" root)))
                        (sb-posix:rename (namestring receive) (namestring spool))
                        (unwind-protect
                             (progn
                               (ok (= 1 (observe installing)))
                               (ok (intake-fails-p (lambda () (observe record))))
                               (sb-posix:rename (namestring pending) (namestring installed))
                               (unwind-protect
                                    (ok (intake-fails-p (lambda () (observe installing))))
                                 (sb-posix:rename (namestring installed) (namestring pending))))
                          (sb-posix:rename (namestring spool) (namestring receive))))
                      (funcall fixed-check)))
                  image-check)))
           (sb-posix:close workspace-fd))
         (ok (equal record (pg::physical-reseed-follow-read owner)))))))

(defun call-with-reseed-configuration-input-fixture (function &optional (kind :same-timeline))
  (call-with-reseed-follow-building-fixture
   kind
   (lambda (owner root workspace building)
     (declare (ignore building))
     (let* ((record (pg::physical-reseed-follow-stage-configuration owner))
            (directories (getf (getf record :configuration) :directories)))
       (pg::call-with-physical-reseed-install-directories
        owner
        (lambda (root-fd incoming-fd quarantine-fd plan root-check)
          (pg::call-with-physical-reseed-follow-workspace
           owner root-fd workspace
           (lambda (facts workspace-fd workspace-check)
             (declare (ignore facts))
             (pg::call-with-physical-reseed-follow-configuration-guard
              owner workspace-fd ".pg_lisp.lock"
              (lambda (lock-inode lock-check)
                (unless (= lock-inode (getf directories :lock-inode)) (error "fixture workspace lock changed"))
                (pg::call-with-physical-reseed-follow-configuration-guard
                 owner workspace-fd "physical-reseed.install"
                 (lambda (fence-inode fence-check)
                   (unless (= fence-inode (getf directories :fence-inode)) (error "fixture workspace fence changed"))
                   (pg::call-with-physical-reseed-follow-fixed-configuration-directories
                    workspace-fd record
                    (lambda (staged-fd saved-fd fixed-check)
                      (pg::call-with-physical-reseed-follow-base-directories
                       root-fd plan
                       (lambda (base-fds base-check)
                         (let ((receive (pg::physical-backup-open-directory (merge-pathnames "receive/" workspace))))
                           (unwind-protect
                                (funcall function owner root workspace record plan root-fd saved-fd base-fds base-check
                                         (append (list (cons :root root-fd) (cons :incoming incoming-fd)
                                                       (cons :quarantine quarantine-fd) (cons :workspace workspace-fd)
                                                       (cons :staged staged-fd) (cons :saved saved-fd) (cons :receive receive))
                                                 base-fds))
                             (sb-posix:close receive))))
                       fixed-check))
                    fence-check))
                 lock-check))
              workspace-check))
           root-check)))))))

(deftest reseed-original-control-reader-authenticates-the-prescribed-file
    (call-with-reseed-configuration-input-fixture
     (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
       (declare (ignorable descriptors))
       (declare (ignore owner base-fds))
       (let ((original (merge-pathnames "physical-recovery.control" root))
             (saved (merge-pathnames "saved/recovery.original" workspace))
             (expected (pg::physical-libpq-datum-octets (getf (getf record :base) :recovery-control) 196)))
         (multiple-value-bind (control bytes row)
             (pg::physical-reseed-follow-configuration-original-control root-fd saved-fd plan record 0)
           (ok (zerop (getf control :phase)))
           (let ((same (equalp bytes expected))) (ok same))
           (ok (equal "physical-recovery.control" (first row))))
         (sb-posix:rename (namestring original) (namestring saved))
         (unwind-protect
              (multiple-value-bind (control bytes row)
                  (pg::physical-reseed-follow-configuration-original-control root-fd saved-fd plan record 3)
                (ok (not (probe-file original)))
                (ok (zerop (getf control :phase)))
                (let ((same (equalp bytes expected))) (ok same))
                (ok (equal (subseq (assoc "physical-recovery.control" (getf plan :new) :test #'equal) 0 8)
                           (subseq row 0 8))))
           (sb-posix:rename (namestring saved) (namestring original)))
         (funcall check)))))

(deftest reseed-original-control-reader-refuses-fallback-and-a-byte-identical-copy
    (call-with-reseed-configuration-input-fixture
     (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
       (declare (ignorable descriptors))
       (declare (ignore owner root base-fds))
       (pg::physical-reseed-follow-configuration-original-control root-fd saved-fd plan record 0)
       (ok (intake-fails-p
            (lambda () (pg::physical-reseed-follow-configuration-original-control root-fd saved-fd plan record 3))))
       (let* ((path (merge-pathnames "saved/recovery.original" workspace))
              (bytes (pg::physical-libpq-datum-octets (getf (getf record :base) :recovery-control) 196)))
         (unwind-protect
              (progn
                (reseed-follow-owner-test-write path bytes)
                (let ((same (equalp bytes (lineage-test-bytes path)))) (ok same))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-configuration-original-control root-fd saved-fd plan record 3)))))
           (when (probe-file path) (delete-file path))))
       (funcall check))))

(deftest reseed-mapped-base-proof-reconstructs-the-authenticated-saved-base
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-configuration-input-fixture
       (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
         (declare (ignorable descriptors))
         (let ((core (pg::physical-reseed-follow-core-projection record)))
           (multiple-value-bind (checked expected-base expected-control)
               (pg::physical-reseed-follow-check-local-intent owner root-fd core)
             (declare (ignore checked))
             (multiple-value-bind (expected-binding expected-model) (pg::physical-reseed-follow-proof-at owner root-fd core)
               (let ((expected-images (pg::physical-reseed-follow-configuration-images
                                       core expected-binding expected-model (getf (getf record :configuration) :time)))
                     (original (merge-pathnames "physical-recovery.control" root))
                     (saved (merge-pathnames "saved/recovery.original" workspace)))
                 (sb-posix:rename (namestring original) (namestring saved))
                 (unwind-protect
                      (multiple-value-bind (base control binding model images row)
                          (pg::physical-reseed-follow-configuration-base-proof
                           root-fd saved-fd base-fds plan record (1- (length (pg::physical-reseed-follow-configuration-moves record expected-binding))) check)
                        (ok (not (probe-file original)))
                        (ok (= (tenth row) (tenth (assoc "physical-recovery.control" (getf plan :new) :test #'equal))))
                        (let ((same (equalp expected-base base))) (ok same))
                        (let ((same (equalp expected-control control))) (ok same))
                        (let ((same (equalp expected-binding binding))) (ok same))
                        (let ((same (equalp expected-images images))) (ok same))
                        (ok (equal (pg::physical-store-control-tuple expected-model)
                                   (pg::physical-store-control-tuple model))))
                   (sb-posix:rename (namestring saved) (namestring original))))))))
       kind)))

(deftest reseed-mapped-layout-checks-every-prefix-with-one-held-receive-directory
    (dolist (kind '(:same-timeline :later-writer))
      (ok
       (eq :checked
           (catch 'mapped-layout-checked
             (call-with-reseed-configuration-input-fixture
              (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
                (declare (ignore root workspace))
                (multiple-value-bind (base control binding model images row)
                    (pg::physical-reseed-follow-configuration-base-proof
                     root-fd saved-fd base-fds plan record 0 check)
                  (declare (ignore base control model row))
                  (let* ((moves (pg::physical-reseed-follow-configuration-moves record binding))
                         (receive (cdr (assoc :receive descriptors)))
                         (inode (sb-posix:stat-ino (sb-posix:fstat receive))))
                    (loop for prefix from 0 to (length moves)
                          do (multiple-value-bind (current-base current-control current-binding current-model current-images)
                                 (pg::physical-reseed-follow-configuration-base-proof
                                  root-fd saved-fd base-fds plan record prefix check)
                               (declare (ignore current-base current-control current-binding current-model))
                               (let ((same (equalp images current-images))) (ok same)))
                             (let ((state (pg::physical-reseed-follow-configuration-layout-state
                                           owner descriptors record binding images prefix check)))
                               (ok (= (length (getf plan :new)) (length (getf state :new))))
                               (ok (= (length images) (length (getf state :images))))
                               (ok (= (length (getf plan :old)) (length (getf state :old))))
                               (ok (= 10 (length (getf state :directories)))))
                             (ok (= inode (sb-posix:stat-ino (sb-posix:fstat receive))))
                             (when (< prefix (length moves))
                               (destructuring-bind (logical from-role from-leaf to-role to-leaf) (nth prefix moves)
                                 (declare (ignore logical))
                                 (unless (zerop (pg::physical-adoption-renameat2
                                                 (cdr (assoc from-role descriptors)) from-leaf
                                                 (cdr (assoc to-role descriptors)) to-leaf
                                                 pg::+physical-adoption-rename-noreplace+))
                                   (error "fixture metadata move failed")))))
                    (throw 'mapped-layout-checked :checked))))
              kind)
             :fell-through)))))

(deftest reseed-mapped-layout-refuses-extras-and-keeps-the-wal-exception-narrow
    (ok
     (eq :checked
         (catch 'mapped-layout-negative-checked
           (call-with-reseed-configuration-input-fixture
            (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
              (declare (ignore workspace))
              (multiple-value-bind (base control binding model images row)
                  (pg::physical-reseed-follow-configuration-base-proof
                   root-fd saved-fd base-fds plan record 0 check)
                (declare (ignore base control model row))
                (pg::physical-reseed-follow-configuration-layout-state owner descriptors record binding images 0 check)
                (let ((extra (merge-pathnames "not-in-the-plan" root)))
                  (unwind-protect
                       (progn
                         (reseed-follow-owner-test-write extra #(1 2 3))
                         (ok (intake-fails-p
                              (lambda () (pg::physical-reseed-follow-configuration-layout-state
                                          owner descriptors record binding images 0 check)))))
                    (when (probe-file extra) (delete-file extra))))
                (let ((moves (pg::physical-reseed-follow-configuration-moves record binding)))
                  (dolist (move (subseq moves 0 2))
                    (destructuring-bind (logical from-role from-leaf to-role to-leaf) move
                      (declare (ignore logical))
                      (unless (zerop (pg::physical-adoption-renameat2
                                      (cdr (assoc from-role descriptors)) from-leaf
                                      (cdr (assoc to-role descriptors)) to-leaf
                                      pg::+physical-adoption-rename-noreplace+))
                        (error "fixture selected metadata move failed")))))
                (pg::physical-reseed-follow-configuration-layout-state owner descriptors record binding images 2 check)
                (sb-posix:utime (namestring (merge-pathnames "global/" root)) 1 1)
                (ok (= 1 (sb-posix:stat-mtime (sb-posix:fstat (cdr (assoc :global descriptors))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-configuration-layout-state
                                 owner descriptors record binding images 2 check))))
                (throw 'mapped-layout-negative-checked :checked)))
            :later-writer)
           :fell-through))))

(deftest reseed-mapped-layout-rejects-a-byte-identical-unused-image-replacement
    (ok
     (eq :checked
         (catch 'mapped-unused-image-checked
           (call-with-reseed-configuration-input-fixture
            (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
              (declare (ignore root))
              (multiple-value-bind (base control binding model images row)
                  (pg::physical-reseed-follow-configuration-base-proof
                   root-fd saved-fd base-fds plan record 0 check)
                (declare (ignore base control model row))
                (pg::physical-reseed-follow-configuration-layout-state owner descriptors record binding images 0 check)
                (let* ((path (merge-pathnames "staged/recovery.initial" workspace))
                       (replacement (merge-pathnames "staged/replacement.tmp" workspace))
                       (bytes (cdr (assoc "recovery.initial" images :test #'equal)))
                       (before (sb-posix:stat-ino (sb-posix:stat path))))
                  (unless bytes (error "fixture lacks the unused initial recovery image"))
                  (reseed-follow-owner-test-write replacement bytes)
                  (sb-posix:rename replacement path)
                  (ok (equalp bytes (lineage-test-bytes path)))
                  (ok (/= before (sb-posix:stat-ino (sb-posix:stat path))))
                  (ok (intake-fails-p
                       (lambda () (pg::physical-reseed-follow-configuration-layout-state
                                   owner descriptors record binding images 0 check))))))
              (throw 'mapped-unused-image-checked :checked))
            :later-writer)
           :fell-through))))
