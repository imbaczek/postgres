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

(deftest reseed-mapped-operation-scope-pins-journals-and-revokes-held-state
    (call-with-reseed-follow-building-fixture
     :later-writer
     (lambda (owner root workspace building)
       (declare (ignore root workspace building))
       (pg::physical-reseed-follow-stage-configuration owner)
       (let ((saved-check nil) (saved-context nil) (receive nil))
         (multiple-value-bind (first second)
             (pg::call-with-physical-reseed-follow-mapped-image
              owner
              (lambda (root-fd check)
                (declare (ignore root-fd))
                (setf saved-check check
                      saved-context pg::*physical-reseed-follow-installation-scope*
                      receive (cdr (assoc :receive
                                          (pg::physical-reseed-follow-installation-context-descriptors saved-context))))
                (funcall check)
                (ok (= 0 (pg::physical-reseed-follow-installation-context-prefix saved-context)))
                (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))
                (ok (sb-thread:join-thread
                     (sb-thread:make-thread (lambda () (intake-fails-p saved-check)))))
                (ok (intake-fails-p
                     (lambda () (pg::call-with-physical-reseed-follow-mapped-image
                                 owner (lambda (fd verify) (declare (ignore fd verify)))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-installation-context owner))))
                (values :first :second)))
           (ok (eq :first first))
           (ok (eq :second second)))
         (ok (intake-fails-p saved-check))
         (ok (intake-fails-p (lambda () (sb-posix:fstat receive))))
         (ok (not (pg::physical-reseed-follow-installation-context-active-p saved-context)))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))

(deftest reseed-mapped-operation-scope-refuses-an-equal-replaced-final
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore workspace building))
       (let ((record (pg::physical-reseed-follow-stage-configuration owner))
             (calls 0))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-reseed-follow-mapped-image
                 owner
                 (lambda (root-fd check)
                   (declare (ignore root-fd check))
                   (let ((temporary (merge-pathnames "replacement.follow" root))
                         (final (merge-pathnames "physical-reseed.follow" root)))
                     (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image record))
                     (sb-posix:rename temporary final)
                     (incf calls)))))))
         (ok (= calls 1))))))

(deftest reseed-mapped-operation-scope-closes-on-nonlocal-exit
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore root workspace building))
       (pg::physical-reseed-follow-stage-configuration owner)
       (let ((context nil) (check nil) (receive nil))
         (ok (eq :left
                 (catch 'leave-mapped-scope
                   (pg::call-with-physical-reseed-follow-mapped-image
                    owner
                    (lambda (fd verify)
                      (declare (ignore fd))
                      (setf context pg::*physical-reseed-follow-installation-scope*
                            check verify
                            receive (cdr (assoc :receive
                                                (pg::physical-reseed-follow-installation-context-descriptors context))))
                      (throw 'leave-mapped-scope :left))))))
         (ok (not (pg::physical-reseed-follow-installation-context-active-p context)))
         (ok (pg::physical-store-closed-p (pg::physical-reseed-follow-installation-context-store context)))
         (ok (intake-fails-p check))
         (ok (intake-fails-p (lambda () (sb-posix:fstat receive))))
         (ok (eq :reopened
                 (pg::call-with-physical-reseed-follow-mapped-image
                  owner (lambda (fd verify) (declare (ignore fd)) (funcall verify) :reopened))))))))

(defun reseed-test-replace-follow-record (root record)
  (let ((temporary (merge-pathnames "fixture.follow" root))
        (final (merge-pathnames "physical-reseed.follow" root)))
    (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image record))
    (sb-posix:rename temporary final)))

(deftest reseed-configuration-owner-reopens-every-installed-prefix-and-strict-inert-state
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (let ((staged nil) (binding nil))
        (call-with-reseed-follow-building-fixture
         kind
         (lambda (owner root workspace building)
           (declare (ignore workspace building))
           (setf staged (pg::physical-reseed-follow-stage-configuration owner))
           (pg::call-with-physical-directory-root
            owner (lambda (fd directory)
                    (declare (ignore directory))
                    (setf binding (pg::physical-reseed-follow-proof-at owner fd staged)))))
         (lambda (root workspace)
           (let* ((moves (pg::physical-reseed-follow-configuration-moves staged binding))
                  (descriptors nil)
                  (record (pg::physical-reseed-follow-copy staged)))
             (unwind-protect
                  (progn
                    (dolist (entry (list (cons :root root) (cons :workspace workspace)
                                         (cons :staged (merge-pathnames "staged/" workspace))
                                         (cons :saved (merge-pathnames "saved/" workspace))
                                         (cons :wal (merge-pathnames "pg_wal/" root))))
                      (push (cons (car entry) (pg::physical-backup-open-directory (cdr entry))) descriptors))
                    (loop for prefix from 0 to (length moves)
                          do (setf (getf (getf record :configuration) :phase) :installing
                                   (getf (getf record :configuration) :step) prefix)
                             (reseed-test-replace-follow-record root record)
                             (pg::call-with-physical-reseed-follow-configuration-owner
                              root (lambda (owner)
                                     (ok (equal record (pg::physical-reseed-follow-read owner)))
                                     (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-initial-record owner))))))
                             (when (plusp prefix)
                               (setf (getf (getf record :configuration) :step) (1- prefix))
                               (reseed-test-replace-follow-record root record)
                               (pg::call-with-physical-reseed-follow-configuration-owner
                                root (lambda (owner) (ok (equal record (pg::physical-reseed-follow-read owner)))))
                               (setf (getf (getf record :configuration) :step) prefix)
                               (reseed-test-replace-follow-record root record))
                             (when (>= prefix 2)
                               (setf (getf (getf record :configuration) :step) (- prefix 2))
                               (reseed-test-replace-follow-record root record)
                               (let ((called nil))
                                 (ok (intake-fails-p
                                      (lambda () (pg::call-with-physical-reseed-follow-configuration-owner
                                                  root (lambda (owner) (declare (ignore owner)) (setf called t))))))
                                 (ok (not called)))
                               (setf (getf (getf record :configuration) :step) prefix)
                               (reseed-test-replace-follow-record root record))
                             (when (< prefix (length moves))
                               (destructuring-bind (logical from-role from-leaf to-role to-leaf) (nth prefix moves)
                                 (declare (ignore logical))
                                 (unless (zerop (pg::physical-adoption-renameat2
                                                 (cdr (assoc from-role descriptors)) from-leaf
                                                 (cdr (assoc to-role descriptors)) to-leaf
                                                 pg::+physical-adoption-rename-noreplace+))
                                   (error "fixture metadata move failed")))))
                    (setf (getf (getf record :configuration) :phase) :inert)
                    (reseed-test-replace-follow-record root record)
                    (let ((original (symbol-function 'pg::physical-follow-check-pending-state))
                          (strict-calls 0)
                          (before (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
                      (unwind-protect
                           (progn
                             (setf (symbol-function 'pg::physical-follow-check-pending-state)
                                   (lambda (&rest arguments)
                                     (incf strict-calls)
                                     (apply original arguments)))
                             (pg::call-with-physical-reseed-follow-configuration-owner
                              root (lambda (owner) (ok (equal record (pg::physical-reseed-follow-read owner)))))
                             (ok (plusp strict-calls))
                             (ok (= 2 (getf (pg::physical-recovery-read-control root) :phase)))
                             (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))))
                        (setf (symbol-function 'pg::physical-follow-check-pending-state) original))))
               (dolist (entry descriptors) (sb-posix:close (cdr entry))))))))))

(deftest reseed-configuration-advance-installs-one-durable-move-and-finishes-inert
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-follow-building-fixture
       kind
       (lambda (owner root workspace building)
         (declare (ignore building))
         (let* ((staged (pg::physical-reseed-follow-stage-configuration owner))
                (selected (pg::physical-reseed-follow-configuration-selected-p
                           (pg::physical-reseed-follow-core-projection staged)))
                (maximum (if selected 5 4))
                (install-before (lineage-test-bytes (merge-pathnames "physical-reseed.install" root)))
                (record nil))
           (setf record (pg::physical-reseed-follow-advance-configuration owner))
           (ok (eq :installing (getf (getf record :configuration) :phase)))
           (ok (= 0 (getf (getf record :configuration) :step)))
           (loop for step from 1 to maximum
                 do (setf record (pg::physical-reseed-follow-advance-configuration owner))
                    (ok (eq :installing (getf (getf record :configuration) :phase)))
                    (ok (= step (getf (getf record :configuration) :step)))
                    (ok (equal (getf staged :base) (getf record :base)))
                    (ok (equal (getf staged :install) (getf record :install))))
           (setf record (pg::physical-reseed-follow-advance-configuration owner))
           (ok (eq :inert (getf (getf record :configuration) :phase)))
           (ok (= maximum (getf (getf record :configuration) :step)))
           (ok (equal record (pg::physical-reseed-follow-advance-configuration owner)))
           (ok (equal record (pg::physical-reseed-follow-read owner)))
           (ok (equalp install-before (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
           (ok (equalp (pg::physical-libpq-datum-octets
                        (getf (getf staged :base) :recovery-control) 196)
                       (lineage-test-bytes (merge-pathnames "saved/recovery.original" workspace))))
           (let* ((pg::*pgdata* root) (config (pg::physical-follow-read-config)))
             (ok (eq :pending (getf (fifth config) :phase)))
             (ok (not (getf (sixth config) :enabled))))
           (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
           (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :inert (getf (getf (pg::physical-reseed-follow-read owner) :configuration) :phase)))))))))

(deftest reseed-configuration-advance-resumes-one-ahead-cuts-and-syncs-both-parents
    (dolist (cut '(:after-configuration-install-move
                   :before-configuration-install-source-fsync
                   :after-configuration-install-source-fsync
                   :before-configuration-install-destination-fsync
                   :after-configuration-install-destination-fsync
                   :before-configuration-install-progress))
      (let ((receive-inode nil))
        (call-with-reseed-follow-building-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore building))
           (pg::physical-reseed-follow-stage-configuration owner)
           (pg::physical-reseed-follow-advance-configuration owner)
           (setf receive-inode (sb-posix:stat-ino (sb-posix:stat (merge-pathnames "receive/" workspace))))
           (let* ((injected nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq cut event) (setf injected t) (error "configuration move cut")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner))))
             (ok injected))
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
           (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner))))
           (pg::call-with-physical-directory-root
            owner
            (lambda (fd directory)
              (declare (ignore directory))
              (ok (= 0 (getf (getf (pg::physical-reseed-follow-read-at fd "physical-reseed.follow")
                                   :configuration) :step)))))
           (ok (= receive-inode (sb-posix:stat-ino (sb-posix:stat (merge-pathnames "pg_physical_receive/" root))))))
         (lambda (root workspace)
           (let ((renames 0) (phase nil) (syncs nil)
                             (original (symbol-function 'sb-posix:fsync)))
             (unwind-protect
                  (let ((pg::*physical-reseed-follow-test-hook*
                          (lambda (event path)
                            (declare (ignore path))
                            (case event
                              (:before-configuration-install-move (incf renames))
                              (:before-configuration-install-source-fsync (setf phase :source))
                              (:after-configuration-install-source-fsync (setf phase nil))
                              (:before-configuration-install-destination-fsync (setf phase :destination))
                              (:after-configuration-install-destination-fsync (setf phase nil))))))
                    (setf (symbol-function 'sb-posix:fsync)
                          (lambda (fd)
                            (when phase (push (list phase (sb-posix:stat-ino (sb-posix:fstat fd))) syncs))
                            (funcall original fd)))
                    (pg::call-with-physical-reseed-follow-configuration-owner
                     root (lambda (owner)
                            (let ((record (pg::physical-reseed-follow-advance-configuration owner)))
                              (ok (= 1 (getf (getf record :configuration) :step)))
                              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))
               (setf (symbol-function 'sb-posix:fsync) original))
             (ok (= 0 renames))
             (ok (equal (nreverse syncs)
                        (list (list :source (sb-posix:stat-ino (sb-posix:stat workspace)))
                              (list :destination (sb-posix:stat-ino (sb-posix:stat root))))))
             (ok (= receive-inode
                    (sb-posix:stat-ino (sb-posix:stat (merge-pathnames "pg_physical_receive/" root)))))))))))

(deftest reseed-configuration-advance-reopens-the-published-candidate-after-journal-cuts
    (dolist (cut '(:after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (call-with-reseed-follow-building-fixture
       :same-timeline
       (lambda (owner root workspace building)
         (declare (ignore workspace building))
         (pg::physical-reseed-follow-stage-configuration owner)
         (pg::physical-reseed-follow-advance-configuration owner)
         (let* ((injected nil)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (eq cut event) (setf injected t) (error "configuration journal cut")))))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner))))
           (ok injected))
         (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (pg::call-with-physical-directory-root
          owner (lambda (fd directory)
                  (declare (ignore directory))
                  (ok (= 1 (getf (getf (pg::physical-reseed-follow-read-at fd "physical-reseed.follow")
                                       :configuration) :step))))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (= 1 (getf (getf (pg::physical-reseed-follow-read owner) :configuration) :step)))
                 (ok (= 2 (getf (getf (pg::physical-reseed-follow-advance-configuration owner)
                                      :configuration) :step)))
                 (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))

(deftest reseed-configuration-recovery-promotes-complete-installation-temporaries
    (dolist (cut-phase '(:initial :progress :terminal))
      (let ((expected-phase nil) (expected-step nil))
        (call-with-reseed-follow-building-fixture
         :later-writer
         (lambda (owner root workspace building)
           (declare (ignore workspace building))
           (pg::physical-reseed-follow-stage-configuration owner)
           (ecase cut-phase
             (:initial (setf expected-phase :installing expected-step 0))
             (:progress
              (pg::physical-reseed-follow-advance-configuration owner)
              (setf expected-phase :installing expected-step 1))
             (:terminal
              (loop repeat 6 do (pg::physical-reseed-follow-advance-configuration owner))
              (setf expected-phase :inert expected-step 5)))
           (let* ((injected nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event :after-follow-temporary-readback)
                        (setf injected t)
                        (error "complete installation temporary cut")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner))))
             (ok injected))
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
           (ok (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (lambda (root workspace)
           (declare (ignore workspace))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root (lambda (owner)
                   (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                     (ok (eq expected-phase (getf (getf record :configuration) :phase)))
                     (ok (= expected-step (getf (getf record :configuration) :step)))
                     (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                     (ok (equal record (pg::physical-reseed-follow-read owner)))
                     (ok (equal record (pg::physical-reseed-follow-recover-configuration owner)))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))))

(deftest reseed-configuration-recovery-preserves-partial-and-conflicting-temporaries
    (dolist (damage '(:empty :skip))
      (let ((replacement-bytes nil))
        (call-with-reseed-follow-building-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore root workspace building))
           (pg::physical-reseed-follow-stage-configuration owner)
           (pg::physical-reseed-follow-advance-configuration owner)
           (let* ((injected nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event :after-follow-temporary-readback)
                        (setf injected t) (error "complete temporary before damage")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner))))
             (ok injected))
           (pg::call-with-physical-directory-root
            owner
            (lambda (fd directory)
              (declare (ignore directory))
              (let ((candidate (pg::physical-reseed-follow-read-at fd "physical-reseed.follow.tmp")))
                (setf (getf (getf candidate :configuration) :step) 2
                      replacement-bytes (if (eq damage :empty) #() (pg::physical-reseed-follow-image candidate)))))))
         (lambda (root workspace)
           (declare (ignore workspace))
           (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                  (replacement (merge-pathnames "fixture.follow" root))
                  (before nil) (effects 0))
             (reseed-follow-owner-test-write replacement replacement-bytes)
             (sb-posix:rename replacement temporary)
             (setf before (list (sb-posix:stat-ino (sb-posix:stat temporary)) (lineage-test-bytes temporary)))
             (let ((pg::*physical-reseed-follow-test-hook*
                     (lambda (event path)
                       (declare (ignore path))
                       (when (member event '(:before-configuration-install-move
                                             :before-configuration-install-source-fsync
                                             :before-follow-rename))
                         (incf effects)))))
               (pg::call-with-physical-reseed-follow-configuration-owner
                root (lambda (owner)
                       (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-configuration owner))))
                       (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                       (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner)))))))
             (ok (= 0 effects))
             (let ((unchanged (equalp before
                                      (list (sb-posix:stat-ino (sb-posix:stat temporary))
                                            (lineage-test-bytes temporary)))))
               (ok unchanged))))))))

(defun reseed-mapped-test-change-first-byte (path)
  (let ((before (lineage-test-bytes path))
        (inode (sb-posix:stat-ino (sb-posix:stat path))))
    (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                 :if-exists :overwrite :if-does-not-exist :error)
      (let ((byte (read-byte stream nil :eof)))
        (file-position stream 0)
        (write-byte (if (eq byte :eof) 1 (logxor byte 1)) stream)))
    (ok (= inode (sb-posix:stat-ino (sb-posix:stat path))))
    (let ((changed (not (equalp before (lineage-test-bytes path))))) (ok changed))))

(defun reseed-mapped-test-tree-facts (root)
  (labels ((facts (path)
             (let ((stat (sb-posix:lstat (namestring path))))
               (list (enough-namestring path root)
                     (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
                     (sb-posix:stat-mode stat) (sb-posix:stat-nlink stat)
                     (sb-posix:stat-uid stat) (sb-posix:stat-gid stat)
                     (sb-posix:stat-size stat) (sb-posix:stat-mtime stat) (sb-posix:stat-ctime stat))))
           (walk (directory)
             (cons (list :directory (facts directory)
                         (sort (pg::directory-entry-names directory :follow-symlinks t) #'string<))
                   (append
                    (loop for path in (sort (uiop:directory-files directory) #'string< :key #'namestring)
                          collect (list :file (facts path) (lineage-test-bytes path)))
                    (loop for child in (sort (uiop:subdirectories directory) #'string< :key #'namestring)
                          append (walk child))))))
    (walk root)))

(defun check-reseed-mapped-original-file-corruption (late)
  (dolist (kind '(:same-timeline :later-writer))
    (dolist (name '("PG_VERSION" "authentication.initialized" "global/pg_catalog.dat"
                    "global/pg_control" "global/pg_wal_control" "pg_hba.conf" "pg_ident.conf"
                    "pg_wal/0000000000000000.wal" "pg_xact" "physical-backup.manifest"
                    "physical-recovery.control" "recovery.required"))
      (ok
       (eq :checked
           (catch 'mapped-original-corruption-checked
             (call-with-reseed-follow-building-fixture
              kind
              (lambda (owner root workspace building)
                (declare (ignore building))
                (pg::physical-reseed-follow-stage-configuration owner)
                (let ((calls 0) (before nil))
                  (flet ((damage ()
                           (reseed-mapped-test-change-first-byte (merge-pathnames name root))
                           (setf before (list (reseed-mapped-test-tree-facts root)
                                              (reseed-mapped-test-tree-facts workspace)))))
                    (unless late (damage))
                    (ok (intake-fails-p
                         (lambda ()
                           (pg::call-with-physical-reseed-follow-mapped-image
                            owner
                            (lambda (fd check)
                              (declare (ignore fd check))
                              (incf calls)
                              (when late (damage)))))))
                    (ok (= calls (if late 1 0)))
                    (let ((unchanged (equalp before (list (reseed-mapped-test-tree-facts root)
                                                          (reseed-mapped-test-tree-facts workspace)))))
                      (ok unchanged))))
                (throw 'mapped-original-corruption-checked :checked)))))))))

(deftest reseed-mapped-operation-scope-rejects-early-original-file-corruption
    (check-reseed-mapped-original-file-corruption nil))

(deftest reseed-mapped-operation-scope-rejects-late-original-file-corruption
    (check-reseed-mapped-original-file-corruption t))

(defun call-with-reseed-mapped-rejection-fixture (kind function &key terminal stage-only before-inert installing-zero)
  (catch 'mapped-rejection-checked
    (call-with-reseed-follow-building-fixture
     kind
     (lambda (owner root workspace building)
       (declare (ignore building))
       (let* ((record (pg::physical-reseed-follow-stage-configuration owner))
              (maximum (if (pg::physical-reseed-follow-configuration-selected-p
                            (pg::physical-reseed-follow-core-projection record)) 5 4)))
         (loop repeat (cond (stage-only 0) (installing-zero 1)
                            (before-inert (1+ maximum)) (terminal (+ maximum 2)) (t 2))
               do (pg::physical-reseed-follow-advance-configuration owner))
         (funcall function owner root workspace)
         (throw 'mapped-rejection-checked :checked))))))

(defun check-reseed-mapped-path-replacement
    (late &key (kinds '(:same-timeline :later-writer))
            (modes '(:root-lock :workspace-lock :workspace-fence :incoming :quarantine
                     :workspace :receive :global :root)))
  (dolist (kind kinds)
    (dolist (mode modes)
      (ok (eq :checked
              (call-with-reseed-mapped-rejection-fixture kind (lambda (owner root workspace)
                                                                (let* ((plan (pg::physical-reseed-install-owner-plan owner))
                                                                       (directory (member mode '(:incoming :quarantine :workspace :receive :global :root)))
                                                                       (path (ecase mode
                                                                               (:root-lock (merge-pathnames ".pg_lisp.lock" root))
                                                                               (:workspace-lock (merge-pathnames ".pg_lisp.lock" workspace))
                                                                               (:workspace-fence (merge-pathnames "physical-reseed.install" workspace))
                                                                               (:incoming (pathname (getf (getf plan :incoming) :directory)))
                                                                               (:quarantine (pathname (getf (getf plan :quarantine) :directory)))
                                                                               (:workspace workspace) (:receive (merge-pathnames "pg_physical_receive/" root))
                                                                               (:global (merge-pathnames "global/" root)) (:root root)))
                                                                       (original-inode (sb-posix:stat-ino (sb-posix:stat path))))
                                                                  (with-physical-store-directory (backup)
                                                                    (sb-posix:chmod (namestring backup) #o700)
                                                                    (let ((saved (merge-pathnames (if directory "original/" "original.lock") backup))
                                                                          (swapped nil) (before nil) (calls 0))
                                                                      (labels ((snapshot ()
                                                                                 (mapcar #'reseed-mapped-test-tree-facts
                                                                                         (append (list root workspace backup)
                                                                                                 (when (member mode '(:incoming :quarantine)) (list path)))))
                                                                               (swap ()
                                                                                 (sb-posix:rename path saved)
                                                                                 (setf swapped t)
                                                                                 (if directory
                                                                                     (sb-posix:mkdir (namestring path) #o700)
                                                                                     (reseed-follow-owner-test-write path #()))
                                                                                 (ok (/= original-inode (sb-posix:stat-ino (sb-posix:stat path))))
                                                                                 (setf before (snapshot))))
                                                                        (unwind-protect
                                                                             (progn
                                                                               (unless late (swap))
                                                                               (ok (intake-fails-p
                                                                                    (lambda ()
                                                                                      (pg::call-with-physical-reseed-follow-mapped-image
                                                                                       owner (lambda (fd check)
                                                                                               (declare (ignore fd check))
                                                                                               (incf calls)
                                                                                               (when late (swap)))))))
                                                                               (ok (= calls (if late 1 0)))
                                                                               (let ((unchanged (equalp before (snapshot)))) (ok unchanged)))
                                                                          (when swapped
                                                                            (when (probe-file path)
                                                                              (if directory
                                                                                  (uiop:delete-directory-tree path :validate t)
                                                                                  (delete-file path)))
                                                                            (sb-posix:rename saved path))))))))))))))

(defun reseed-mapped-test-wrong-pending-control (root)
  (let* ((path (merge-pathnames "physical-recovery.control" root))
         (control (pg::physical-recovery-read-control root))
         (xid (getf control :next-xid))
         (next (if (< xid #xffffffffffffffff) (1+ xid) (1- xid)))
         (bytes (pg::physical-recovery-control-image
                 2 (getf control :system-id) (getf control :timeline) (getf control :manifest-digest)
                 (getf control :checkpoint) (getf control :applied) next
                 :db-state (getf control :db-state) :state-time (getf control :state-time)
                 :applied-timeline (getf control :applied-timeline (getf control :timeline))
                 :history-target (getf control :history-target) :history-digest (getf control :history-digest)
                 :retained-reference (getf control :retained-reference) :resource-fact (getf control :resource-fact))))
    (ok (= next (getf (pg::physical-recovery-decode-control bytes) :next-xid)))
    (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                 :if-exists :overwrite :if-does-not-exist :error)
      (write-sequence bytes stream))
    (ok (= next (getf (pg::physical-recovery-read-control root) :next-xid)))
    (ok (/= xid next))))

(deftest reseed-mapped-operation-scope-rejects-early-guard-and-path-replacement
    (check-reseed-mapped-path-replacement nil))

(deftest reseed-mapped-operation-scope-rejects-late-guard-and-path-replacement
    (check-reseed-mapped-path-replacement t))

(defun reseed-mapped-test-pending-validator-refuses-current-control (context)
  (let* ((descriptors (pg::physical-reseed-follow-installation-context-descriptors context))
         (calls 0) (refused nil))
    (flet ((guard () (pg::physical-reseed-follow-installation-guard context)))
      (pg::call-with-physical-reseed-follow-held-base-directory
       (cdr (assoc :root descriptors)) (cdr (assoc :base descriptors))
       (cdr (assoc :global descriptors)) (cdr (assoc :wal descriptors))
       (lambda (directory)
         (let ((pg::*pgdata* directory))
           (incf calls)
           (setf refused
                 (handler-case
                     (progn
                       (pg::physical-follow-check-pending-state
                        (pg::physical-reseed-follow-installation-context-store context)
                        (pg::physical-reseed-follow-installation-context-base context)
                        (pg::physical-reseed-follow-pending-config
                         (pg::physical-reseed-follow-installation-context-record context)
                         (pg::physical-reseed-follow-installation-context-binding context)))
                       nil)
                   (error (condition)
                     (not (null (search "pending follow differs from durable recovery provenance or progress"
                                        (princ-to-string condition)))))))))
       #'guard))
    (values calls refused)))

(deftest reseed-mapped-operation-scope-rejects-wrong-terminal-pending-witnesses
    (dolist (kind '(:same-timeline :later-writer))
      (dolist (late '(nil t))
        (ok (eq :checked
                (call-with-reseed-mapped-rejection-fixture kind (lambda (owner root workspace)
                                                                  (let ((calls 0) (before nil) (validator-calls 0) (validator-refused nil)
                                                                                  (saved-before (lineage-test-bytes (merge-pathnames "saved/recovery.original" workspace))))
                                                                    (flet ((damage ()
                                                                             (reseed-mapped-test-wrong-pending-control root)
                                                                             (setf before (list (reseed-mapped-test-tree-facts root)
                                                                                                (reseed-mapped-test-tree-facts workspace)))))
                                                                      (unless late (damage))
                                                                      (ok (intake-fails-p
                                                                           (lambda ()
                                                                             (pg::call-with-physical-reseed-follow-mapped-image
                                                                              owner (lambda (fd check)
                                                                                      (declare (ignore fd check))
                                                                                      (incf calls)
                                                                                      (when late
                                                                                        (damage)
                                                                                        (multiple-value-setq (validator-calls validator-refused)
                                                                                          (reseed-mapped-test-pending-validator-refuses-current-control
                                                                                           pg::*physical-reseed-follow-installation-scope*))))))))
                                                                      (ok (= calls (if late 1 0)))
                                                                      (when late (ok (= 1 validator-calls)) (ok validator-refused))
                                                                      (let ((unchanged (equalp before (list (reseed-mapped-test-tree-facts root)
                                                                                                            (reseed-mapped-test-tree-facts workspace)))))
                                                                        (ok unchanged))
                                                                      (let ((original-unchanged (equalp saved-before
                                                                                                        (lineage-test-bytes (merge-pathnames "saved/recovery.original" workspace)))))
                                                                        (ok original-unchanged))))) :terminal t))))))

(deftest reseed-configuration-advance-preflight-refuses-early-and-terminal-corruption
    (dolist (kind '(:same-timeline :later-writer))
      (dolist (terminal '(nil t))
        (dolist (fault '(:catalog :workspace-fence))
          (ok (eq :checked
                  (call-with-reseed-mapped-rejection-fixture kind (lambda (owner root workspace)
                                                                    (let ((effects 0)
                                                                          (path (if (eq fault :catalog) (merge-pathnames "global/pg_catalog.dat" root)
                                                                                    (merge-pathnames "physical-reseed.install" workspace))))
                                                                      (pg::call-with-physical-reseed-follow-mapped-image
                                                                       owner (lambda (fd check) (declare (ignore fd)) (funcall check)))
                                                                      (reseed-mapped-test-change-first-byte path)
                                                                      (let ((before (list (reseed-mapped-test-tree-facts root) (reseed-mapped-test-tree-facts workspace))))
                                                                        (let ((pg::*physical-reseed-follow-test-hook*
                                                                                (lambda (event where)
                                                                                  (declare (ignore where))
                                                                                  (when (member event '(:before-configuration-install-move :before-configuration-install-progress
                                                                                                        :before-follow-temporary-create :before-follow-rename))
                                                                                    (incf effects)))))
                                                                          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner)))))
                                                                        (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                                                                        (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                                                                        (ok (= effects 0))
                                                                        (let ((unchanged (equalp before (list (reseed-mapped-test-tree-facts root)
                                                                                                              (reseed-mapped-test-tree-facts workspace)))))
                                                                          (ok unchanged)))))
                                                             :stage-only (not terminal) :before-inert terminal)))))))

(deftest reseed-configuration-advance-rechecks-corruption-before-move-and-inert-publication
    (dolist (kind '(:same-timeline :later-writer))
      (dolist (terminal '(nil t))
        (ok (eq :checked
                (call-with-reseed-mapped-rejection-fixture kind (lambda (owner root workspace)
                                                                  (let* ((before nil) (fired nil) (effects 0)
                                                                                      (point (if terminal :before-configuration-install-progress :before-configuration-install-move))
                                                                                      (pg::*physical-reseed-follow-test-hook*
                                                                                        (lambda (event where)
                                                                                          (declare (ignore where))
                                                                                          (when (eq event point)
                                                                                            (reseed-mapped-test-change-first-byte (merge-pathnames "global/pg_catalog.dat" root))
                                                                                            (setf before (list (reseed-mapped-test-tree-facts root)
                                                                                                               (reseed-mapped-test-tree-facts workspace))
                                                                                                  fired t))
                                                                                          (when (member event '(:after-configuration-install-move :after-follow-temporary-create
                                                                                                                :after-follow-rename :after-configuration-install-progress))
                                                                                            (incf effects)))))
                                                                    (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-advance-configuration owner))))
                                                                    (ok fired)
                                                                    (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                                                                    (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                                                                    (ok (= 0 effects))
                                                                    (let ((unchanged (equalp before (list (reseed-mapped-test-tree-facts root)
                                                                                                          (reseed-mapped-test-tree-facts workspace)))))
                                                                      (ok unchanged))))
                                                           :installing-zero (not terminal) :before-inert terminal))))))

(deftest reseed-mapped-replaced-auxiliary-directories-remain-unchanged
    (check-reseed-mapped-path-replacement nil :modes '(:incoming :quarantine))
  (check-reseed-mapped-path-replacement t :modes '(:incoming :quarantine)))
