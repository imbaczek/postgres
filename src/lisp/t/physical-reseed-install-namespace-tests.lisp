(in-package #:pg-physical-tests)

(defun call-with-reseed-namespace-fixture (function &key (before-count 1) (after-count 0) (pid t) (opaque t))
  (call-with-reseed-install-owner-fixture
   (lambda (root stage incoming quarantine plan)
     (when opaque
       (let ((old-base (merge-pathnames "base/" root)))
         (sb-posix:mkdir (namestring old-base) #o700)
         (with-open-file (out (merge-pathnames "opaque-old" old-base) :direction :output)
           (write-string "old directory contents" out))
         (sb-posix:chmod (namestring (merge-pathnames "opaque-old" old-base)) 0))
       (with-open-file (out (merge-pathnames "old-hardlink" root) :direction :output)
         (write-string "opaque hardlink data" out))
       (sb-posix:link (namestring (merge-pathnames "old-hardlink" root))
                      (namestring (merge-pathnames "alias-hardlink" root)))
       (sb-posix:mkfifo (namestring (merge-pathnames "old-fifo" root)) #o600)
       (sb-posix:symlink "missing-old-referent" (namestring (merge-pathnames "old-symlink" root))))
     (multiple-value-bind (image payloads) (reseed-publication-test-image (getf (getf plan :attempt) :request))
       (let* ((bytes (make-array 8192 :element-type '(unsigned-byte 8) :initial-element 17))
              (entry (list "base/42" (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
              (manifest (getf image :manifest)))
         (push (list "base/42" bytes) payloads)
         (setf (getf manifest :files) (sort (cons entry (getf manifest :files)) #'string< :key #'first))
         (push (copy-list entry) (getf image :files))
         (let* ((encoded (pg::physical-bootstrap-manifest-image manifest))
                (digest (pg::physical-store-update-digest #xCBF29CE484222325 encoded))
                (manifest-entry (assoc "physical-backup.manifest" (getf image :files) :test #'equal)))
           (setf (second (assoc "physical-backup.manifest" payloads :test #'equal)) encoded
                 (getf image :manifest-digest) digest
                 (second manifest-entry) (length encoded)
                 (third manifest-entry) digest)))
       (dolist (name '("base/" "global/" "pg_wal/"))
         (sb-posix:mkdir (namestring (merge-pathnames name incoming)) #o700))
       (dolist (entry payloads)
         (with-open-file (out (merge-pathnames (first entry) incoming) :direction :output
                                                                       :if-exists :error :element-type '(unsigned-byte 8))
           (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
           (write-sequence (second entry) out)))
       (let ((fd (pg::physical-backup-open-directory incoming)))
         (unwind-protect
              (pg::call-with-physical-backup-image-directory
               fd (lambda (directory)
                    (declare (ignore directory))
                    (setf (getf plan :new)
                          (pg::physical-reseed-install-image-rows pg::*physical-image-context* image :synchronize t))))
           (sb-posix:close fd)))
       (setf (getf plan :manifest) (getf image :manifest)
             (getf plan :manifest-digest) (getf image :manifest-digest)))
     (let ((fd (pg::physical-backup-open-directory root)))
       (unwind-protect
            (with-open-file (journal (pg::physical-reseed-install-path root) :element-type '(unsigned-byte 8))
              (let* ((identity (getf plan :journal))
                     (mount (pg::physical-adoption-install-mount-id nil :directory-fd fd)))
                (pg::physical-reseed-install-check-held-journal
                 fd (sb-sys:fd-stream-fd journal) "physical-reseed.install"
                 (list (getf identity :device) (getf identity :inode)))
                (setf (getf plan :old)
                      (pg::physical-reseed-install-old-scan
                       fd mount (sb-posix:stat-dev (sb-posix:fstat fd)) nil :install-entry "physical-reseed.install"))))
         (sb-posix:close fd)))
     (setf plan (pg::physical-reseed-install-copy plan))
     (reseed-install-owner-test-write root plan 0)
     (funcall function root stage incoming quarantine plan))
   :before-count before-count :after-count after-count :pid pid))

(defun reseed-namespace-test-move (root incoming quarantine plan index)
  (let* ((old (getf plan :old))
         (top (remove-if (lambda (row) (find #\/ (first row))) (getf plan :new)))
         (old-p (< index (length old)))
         (row (if old-p (nth index old) (nth (- index (length old)) top))))
    (sb-posix:rename (namestring (merge-pathnames (first row) (if old-p root incoming)))
                     (namestring (merge-pathnames (first row) (if old-p quarantine root))))))

(cl-spec:defproperty reseed-install-observation-accepts-recorded-and-single-ahead-namespaces
    ((before-count (range integer 0 3)) (after-count (range integer 0 3))
                                        (index (range integer 0 200)) (ahead boolean) (partial boolean))
  (:about pg::physical-reseed-install-observe)
  (:trials (:normal 80))
  (let ((accepted nil))
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage))
       (let* ((maximum (1- (pg::physical-reseed-install-last-step plan)))
              (step (mod index (1+ maximum)))
              (moves (min maximum (+ step (if ahead 1 0)))))
         (dotimes (move moves) (reseed-namespace-test-move root incoming quarantine plan move))
         (reseed-install-owner-test-write root plan step partial)
         (let ((before (lineage-test-bytes (pg::physical-reseed-install-path root))))
           (pg::call-with-physical-reseed-install-owner
            root (lambda (owner)
                   (setf accepted
                         (equal (list :step step :observed moves :partial partial)
                                (pg::physical-reseed-install-observe owner)))))
           (setf accepted (and accepted (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))))))
     :before-count before-count :after-count after-count)
    accepted))

(deftest generated-reseed-install-whole-namespace-prefix-proof
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-install-observation-accepts-recorded-and-single-ahead-namespaces
                                       :seed 101 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest reseed-install-observation-refuses-attempt-change-inside-the-proof
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-install-owner
        root
        (lambda (owner)
          (let* ((armed nil) (openings 0) (fired nil)
                             (pg::*physical-reseed-install-test-hook*
                               (lambda (point path)
                                 (declare (ignore path))
                                 (when (eq point :after-install-namespace-files) (setf armed t))
                                 (when (and armed (eq point :after-journal-open))
                                   (incf openings)
                                   (when (and (= 2 openings) (not fired))
                                     (setf fired t)
                                     (let ((record (pg::physical-reseed-attempt-copy (pg::physical-reseed-cleanup-owner-attempt owner))))
                                       (setf (getf record :phase) :aborting)
                                       (let ((bytes (pg::physical-reseed-attempt-image record)))
                                         (with-open-file (out (pg::physical-reseed-attempt-path root)
                                                              :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
                                           (write-sequence bytes out)
                                           (finish-output out)
                                           (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes))
                                           (pg::fsync-stream out)))))))))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-install-observe owner))))
            (ok fired)))))))

(deftest reseed-install-observation-rechecks-all-evidence-after-file-inspection
    (dolist (position '(:initial :split :complete))
      (dolist (mutation '(:new-byte :child-extra :old-directory :journal-tail :guard-byte :root-extra :child-replaced))
        (call-with-reseed-namespace-fixture
         (lambda (root stage incoming quarantine plan)
           (declare (ignore stage))
           (let* ((old-count (length (getf plan :old)))
                  (maximum (1- (pg::physical-reseed-install-last-step plan)))
                  (step (ecase position (:initial 0) (:split (1+ old-count)) (:complete maximum)))
                  (new-parent (if (zerop step) incoming root))
                  (global-parent (if (eq position :complete) root incoming))
                  (old-parent (if (zerop step) root quarantine)))
             (dotimes (move step) (reseed-namespace-test-move root incoming quarantine plan move))
             (reseed-install-owner-test-write root plan step)
             (let* ((before (lineage-test-bytes (pg::physical-reseed-install-path root)))
                    (fired nil)
                    (pg::*physical-reseed-install-test-hook*
                      (lambda (point path)
                        (declare (ignore path))
                        (when (and (eq point :after-install-namespace-files) (not fired))
                          (setf fired t)
                          (ecase mutation
                            (:child-replaced
                             (sb-posix:rename (namestring (merge-pathnames "global" global-parent))
                                              (namestring (merge-pathnames "replaced-global" global-parent)))
                             (sb-posix:mkdir (namestring (merge-pathnames "global" global-parent)) #o700))
                            ((:new-byte :child-extra :old-directory :journal-tail :guard-byte :root-extra)
                             (let ((target (ecase mutation
                                             (:new-byte (merge-pathnames "PG_VERSION" new-parent))
                                             (:child-extra (merge-pathnames "global/unexpected" global-parent))
                                             (:old-directory (merge-pathnames "base/unexpected" old-parent))
                                             (:journal-tail (pg::physical-reseed-install-path root))
                                             (:guard-byte (merge-pathnames "physical-reseed.install" quarantine))
                                             (:root-extra (merge-pathnames "unexpected" root)))))
                               (with-open-file (out target :direction :output :element-type '(unsigned-byte 8)
                                                           :if-exists (if (eq mutation :journal-tail) :append :overwrite))
                                 (write-byte 99 out)))))))))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::call-with-physical-reseed-install-owner
                       root (lambda (owner) (pg::physical-reseed-install-observe owner))))))
               (ok fired)
               (unless (eq mutation :journal-tail)
                 (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))))))))))

(deftest reseed-install-namespace-scope-closes-owned-descriptors-and-check-closures
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-install-owner
        root
        (lambda (owner)
          (let ((owned nil) (saved-check nil))
            (ok (eq :cut
                    (catch 'namespace-scope-cut
                      (pg::call-with-physical-reseed-install-namespace
                       owner
                       (lambda (root-fd incoming-fd quarantine-fd state prefix check)
                         (declare (ignore state prefix))
                         (setf owned (list root-fd incoming-fd quarantine-fd) saved-check check)
                         (throw 'namespace-scope-cut :cut))))))
            (dolist (fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
            (ok (intake-fails-p saved-check))
            (ok (eq owner (pg::physical-reseed-check-owner owner)))
            (ok (equal '(:step 0 :observed 0 :partial nil) (pg::physical-reseed-install-observe owner)))))))))

(deftest reseed-install-directory-scope-preserves-values-and-revokes-held-checks
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-install-owner
        root
        (lambda (owner)
          (dolist (cut '(nil t))
            (let ((owned nil) (saved-check nil))
              (ok (equal (if cut '(:cut) '(:first :second))
                         (multiple-value-list
                          (catch 'directory-scope-cut
                            (pg::call-with-physical-reseed-install-directories
                             owner
                             (lambda (root-fd incoming-fd quarantine-fd held-plan check)
                               (ok (equal held-plan (pg::physical-reseed-install-owner-plan owner)))
                               (funcall check)
                               (setf owned (list root-fd incoming-fd quarantine-fd)
                                     saved-check check)
                               (if cut (throw 'directory-scope-cut :cut)
                                   (values :first :second))))))))
              (dolist (fd owned)
                (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
              (ok (intake-fails-p saved-check))
              (ok (equal '(:step 0 :observed 0 :partial nil)
                         (pg::physical-reseed-install-observe owner))))))))))

(deftest reseed-install-directory-scope-rechecks-guards-after-callback
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage quarantine plan))
       (pg::call-with-physical-reseed-install-owner
        root
        (lambda (owner)
          (let ((calls 0)
                (path (merge-pathnames "physical-reseed.install" incoming)))
            (ok (intake-fails-p
                 (lambda ()
                   (pg::call-with-physical-reseed-install-directories
                    owner
                    (lambda (root-fd incoming-fd quarantine-fd held-plan check)
                      (declare (ignore root-fd incoming-fd quarantine-fd held-plan))
                      (funcall check)
                      (with-open-file (stream path :direction :io :if-exists :overwrite
                                                   :element-type '(unsigned-byte 8))
                        (let ((byte (read-byte stream)))
                          (file-position stream 0)
                          (write-byte (logxor byte 255) stream)
                          (finish-output stream)
                          (incf calls))))))))
            (ok (= calls 1))))))))

(deftest reseed-install-new-directory-scope-closes-children-on-nonlocal-exit
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage quarantine))
       (let ((root-fd (pg::physical-backup-open-directory root))
             (incoming-fd (pg::physical-backup-open-directory incoming))
             (owned nil) (saved-resolve nil))
         (unwind-protect
              (progn
                (ok (eq :cut
                        (catch 'new-directory-cut
                          (pg::call-with-physical-reseed-install-new-directories
                           root-fd incoming-fd plan 0
                           (pg::physical-adoption-install-mount-id nil :directory-fd root-fd)
                           (lambda (resolve check)
                             (declare (ignore check))
                             (setf saved-resolve resolve)
                             (dolist (path '("base/unused" "global/pg_control" "pg_wal/unused"))
                               (push (funcall resolve path) owned))
                             (throw 'new-directory-cut :cut))))))
                (ok (= 3 (length owned)))
                (dolist (fd owned) (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
                (ok (intake-fails-p (lambda () (funcall saved-resolve "global/pg_control"))))
                (ok (sb-posix:fstat root-fd))
                (ok (sb-posix:fstat incoming-fd)))
           (sb-posix:close incoming-fd)
           (sb-posix:close root-fd))))))

(deftest reseed-install-observation-verifies-every-recorded-prefix
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage))
       (let ((maximum (1- (pg::physical-reseed-install-last-step plan))))
         (loop for step from 0 to maximum
               do (when (plusp step) (reseed-namespace-test-move root incoming quarantine plan (1- step)))
                  (reseed-install-owner-test-write root plan step)
                  (pg::call-with-physical-reseed-install-owner
                   root (lambda (owner)
                          (ok (equal (list :step step :observed step :partial nil)
                                     (pg::physical-reseed-install-observe owner))))))
         (reseed-install-owner-test-write root plan (1+ maximum))
         (pg::call-with-physical-reseed-install-owner
          root (lambda (owner)
                 (ok (equal (list :step (1+ maximum) :observed maximum :partial nil)
                            (pg::physical-reseed-install-observe owner)))))))))

(deftest reseed-install-observation-verifies-every-single-ahead-prefix-with-torn-progress
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage))
       (loop for step below (1- (pg::physical-reseed-install-last-step plan))
             do (reseed-namespace-test-move root incoming quarantine plan step)
                (reseed-install-owner-test-write root plan step t)
                (let ((before (lineage-test-bytes (pg::physical-reseed-install-path root))))
                  (pg::call-with-physical-reseed-install-owner
                   root (lambda (owner)
                          (ok (equal (list :step step :observed (1+ step) :partial t)
                                     (pg::physical-reseed-install-observe owner)))))
                  (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root)))))))))

(deftest reseed-install-observation-refuses-two-ahead-and-extra-names
    (dolist (mode '(:two-ahead :root-extra :incoming-extra :quarantine-extra :child-extra))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage))
         (if (eq mode :two-ahead)
             (dotimes (move 2) (reseed-namespace-test-move root incoming quarantine plan move))
             (let ((directory (ecase mode
                                (:root-extra root) (:incoming-extra incoming) (:quarantine-extra quarantine)
                                (:child-extra (merge-pathnames "global/" incoming)))))
               (with-open-file (out (merge-pathnames "unexpected" directory) :direction :output)
                 (write-string "extra" out))))
         (pg::call-with-physical-reseed-install-owner
          root (lambda (owner)
                 (ok (intake-fails-p (lambda () (pg::physical-reseed-install-observe owner))))))))))

(deftest reseed-install-observation-hashes-metadata-with-matching-stat-facts
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((row (assoc "pg_hba.conf" (getf plan :new) :test #'equal)))
         (assert row)
         (setf (tenth row) (logxor 1 (tenth row))))
       (reseed-install-owner-test-write root plan 0)
       (pg::call-with-physical-reseed-install-owner
        root (lambda (owner)
               (ok (intake-fails-p (lambda () (pg::physical-reseed-install-observe owner)))))))))
