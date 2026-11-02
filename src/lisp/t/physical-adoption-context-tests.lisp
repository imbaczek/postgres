(in-package #:pg-physical-tests)

(defvar *adoption-context-source-case* nil)
(defvar *adoption-context-observation-hook* nil)

(defun call-with-adoption-context-fixture (function &key (fixture #'call-with-adoption-stage-fixture))
  (funcall fixture
           (lambda (root state proof)
             (with-physical-store-directory (target)
               (sb-posix:chmod (namestring target) #o700)
               (let* ((stage (pg::physical-adoption-stage-base state proof target "context_attempt"))
                      (info (pg::physical-libpq-decode-connection-info
                             (libpq-test-connection-body '(("replication" "true") ("dbname" "replication")) "replication")))
                      (client (pg::%make-physical-libpq-channel :owner sb-thread:*current-thread* :info info))
                      (profile (pg::make-physical-libpq-profile
                                "host=original.invalid user=original" :mode :physical :fallback "context-test"))
                      (identity (symbol-function 'pg::physical-client-identity-row))
                      (history (symbol-function 'pg::pg-client-native-history))
                      (count 0)
                      (events nil))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-client-identity-row)
                              (lambda (actual)
                                (assert (eq actual client))
                                (incf count)
                                (push :identify events)
                                (when *adoption-context-observation-hook*
                                  (funcall *adoption-context-observation-hook* :identity root stage))
                                (vector
                                 (format nil "~d" (+ (getf state :system-id)
                                                     (if (eq *adoption-context-source-case* :system) 1 0)))
                                 (format nil "~d" (cond ((eq *adoption-context-source-case* :timeline) (getf state :timeline))
                                                        ((and (eq *adoption-context-source-case* :promotion)
                                                              (> count 1)) (+ 2 (getf state :timeline)))
                                                        (t (1+ (getf state :timeline)))))
                                 (if (eq *adoption-context-source-case* :behind) "0/00000020"
                                     (pg::wal-format-lsn (+ (max 256 (getf state :end)) count)))
                                 nil))
                              (symbol-function 'pg::pg-client-native-history)
                              (lambda (actual timeline)
                                (assert (eq actual client))
                                (assert (= timeline (1+ (getf state :timeline))))
                                (push :history events)
                                (when (eq *adoption-context-source-case* :route)
                                  (incf (pg::physical-libpq-connection-info-port info)))
                                (when *adoption-context-observation-hook*
                                  (funcall *adoption-context-observation-hook* :history root stage))
                                (pg::string->bytes
                                 (concatenate 'string
                                              (if (> (getf state :timeline) 1)
                                                  (pg::bytes->string (pg::timeline-history-bytes (getf state :timeline) root))
                                                  "")
                                              (format nil "~d~c~a~ccontext source~%" (getf state :timeline) #\Tab
                                                      (pg::wal-format-lsn
                                                       (+ (getf state :end)
                                                          (if (eq *adoption-context-source-case* :history) -1 64)))
                                                      #\Tab)))))
                        (funcall function root state proof stage client profile (lambda () (reverse events))))
                   (setf (symbol-function 'pg::physical-client-identity-row) identity
                         (symbol-function 'pg::pg-client-native-history) history)))))))

(deftest adoption-context-factory-freezes-observed-route-and-exact-staged-overlap
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore proof))
       (let* ((before (adoption-stage-source-bytes root))
              (record (pg::physical-adoption-attempt-create state stage client profile)))
         (ok (equal '(:identify :history :identify) (funcall events)))
         (ok (eq :preparing (getf record :phase)))
         (ok (equal (getf record :local) (pg::physical-adoption-attempt-local-facts state)))
         (ok (= (getf stage :manifest-digest) (getf (getf record :request) :manifest-digest)))
         (ok (= 152 (getf (getf record :request) :base) (getf (getf record :request) :receive-lsn)))
         (ok (equal (pg::physical-libpq-profile-conninfo-datum profile)
                    (getf (getf record :source) :conninfo)))
         (multiple-value-bind (ignored owner) (pg::physical-follow-source-profile (getf record :source))
           (declare (ignore ignored))
           (ok (equal (pg::physical-libpq-connection-route (pg::physical-libpq-channel-info client))
                      (pg::physical-libpq-frozen-route owner))))
         (ok (pg::physical-adoption-attempt-validate-context (getf state :owner) record))
         (ok (equalp before (adoption-stage-source-bytes root)))
         (ok (not (probe-file (pg::physical-adoption-attempt-path root))))))))

(deftest adoption-context-factory-rejects-source-or-route-drift-and-promotion-races
    (dolist (*adoption-context-source-case* '(:system :timeline :behind :history :promotion :route))
      (call-with-adoption-context-fixture
       (lambda (root state proof stage client profile events)
         (declare (ignore proof events))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-create state stage client profile))))
         (ok (not (probe-file (pg::physical-adoption-attempt-path root))))))))

(deftest adoption-context-rechecks-source-and-stage-after-readonly-observation
    (dolist (side '(:source :stage))
      (call-with-adoption-context-fixture
       (lambda (root state proof stage client profile events)
         (declare (ignore proof events))
         (let ((*adoption-context-observation-hook*
                 (lambda (point source image)
                   (when (eq point :history)
                     (pg::wal-atomic-write-bytes
                      (merge-pathnames "pg_xact" (if (eq side :source) source (getf image :directory)))
                      (pg::string->bytes "changed during observation"))))))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-create state stage client profile)))))
         (ok (not (probe-file (pg::physical-adoption-attempt-path root))))))))

(cl-spec:defproperty adoption-context-freezes-auth-beyond-the-manifest
    ((size (range integer 1 200)) (offset (range integer 0 25)))
  (:about pg::physical-adoption-attempt-validate-context)
  (:trials (:normal 100))
  (call-with-adoption-context-fixture
   (lambda (root state proof stage client profile events)
     (declare (ignore proof events))
     (let* ((record (pg::physical-adoption-attempt-create state stage client profile))
            (text (format nil "#~a~%" (make-string size :initial-element (code-char (+ 97 offset))))))
       (dolist (directory (list root (getf stage :directory)))
         (pg::wal-atomic-write-bytes (merge-pathnames "pg_ident.conf" directory) (pg::string->bytes text)))
       (intake-fails-p
        (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record)))))))

(deftest generated-adoption-context-auth-freeze
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-context-freezes-auth-beyond-the-manifest :seed 77))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest adoption-context-rejects-each-stale-capture-claim
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore root proof events))
       (let ((record (pg::physical-adoption-attempt-create state stage client profile)))
         (loop for index from 1 below 7
               do (let ((changed (copy-tree record)))
                    (setf (nth index (getf (getf changed :local) :capture)) '(1 99))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-adoption-attempt-validate-context
                                     (getf state :owner) changed))))))))))

(deftest adoption-context-refuses-stale-images-without-changing-the-saved-attempt
    (dolist (mode '(:origin :source-data :stage-data :auth :both-auth :stage-pid :stage-control))
      (call-with-adoption-context-fixture
       (lambda (root state proof stage client profile events)
         (declare (ignore events))
         (let* ((owner (getf state :owner))
                (target (getf stage :directory))
                (record (pg::physical-adoption-attempt-create state stage client profile)))
           (pg::physical-adoption-attempt-publish owner record)
           (ecase mode
             (:origin
              (destructuring-bind (system timeline base manifest) (getf proof :identity)
                (let ((key (pg::make-physical-export-origin
                            :system-id system :timeline timeline :base base :manifest-digest manifest
                            :proven-lsn base :prefix-digest #xCBF29CE484222325)))
                  (delete-file (pg::physical-export-origin-path key)))))
             (:source-data (pg::wal-atomic-write-bytes (merge-pathnames "pg_xact" root)
                                                       (pg::string->bytes "source changed")))
             (:stage-data (pg::wal-atomic-write-bytes (merge-pathnames "pg_xact" target)
                                                      (pg::string->bytes "target changed")))
             (:auth (pg::wal-atomic-write-bytes (merge-pathnames "pg_ident.conf" target)
                                                (pg::string->bytes (format nil "# different~%"))))
             (:both-auth
              (dolist (directory (list root target))
                (pg::wal-atomic-write-bytes (merge-pathnames "pg_ident.conf" directory)
                                            (pg::string->bytes (format nil "# matching edits~%")))))
             (:stage-pid (sb-posix:symlink "missing-process" (namestring (merge-pathnames "postmaster.pid" target))))
             (:stage-control
              (pg::wal-atomic-write-bytes
               (pg::physical-recovery-control-path target)
               (pg::physical-recovery-control-image
                1 (getf state :system-id) 1 (getf stage :manifest-digest) 152 152 42
                :resource-fact (pg::wal-copy-control-resource-fields (getf state :snapshot))))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-validate-context owner record))))
           (ok (equal record (pg::physical-adoption-attempt-read owner)))
           (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))))

(deftest adoption-context-cancellation-phases-remain-readable-with-a-missing-stage
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore root proof events))
       (let* ((owner (getf state :owner))
              (record (pg::physical-adoption-attempt-publish
                       owner (pg::physical-adoption-attempt-create state stage client profile)))
              (cancel (copy-tree record)))
         (setf (getf cancel :phase) :aborting)
         (setf cancel (pg::physical-adoption-attempt-publish owner cancel :previous record))
         (uiop:delete-directory-tree (getf stage :directory) :validate t)
         (ok (equal cancel (pg::physical-adoption-attempt-read owner)))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-validate-context owner cancel))))))))

(deftest adoption-context-validates-a-rooted-source-with-only-staged-control-derived
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore proof events))
       (let* ((before (lineage-test-bytes (merge-pathnames "global/pg_wal_control" root)))
              (record (pg::physical-adoption-attempt-create state stage client profile))
              (copied (lineage-test-bytes (merge-pathnames "global/pg_wal_control" (getf stage :directory))))
              (source-fields (multiple-value-list (pg::wal-decode-control-line (pg::bytes->string before))))
              (target-fields (multiple-value-list (pg::wal-decode-control-line (pg::bytes->string copied)))))
         (ok (= 2 (getf (getf record :local) :timeline)))
         (ok (= 3 (getf (getf record :request) :source-timeline)))
         (ok (pg::physical-adoption-attempt-validate-context (getf state :owner) record))
         (ok (getf (seventh source-fields) :retained-reference))
         (ok (null (getf (seventh target-fields) :retained-reference)))
         (ok (not (equalp before copied)))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "global/pg_wal_control" root))))))
     :fixture #'call-with-adoption-rooted-stage-fixture))

(defun adoption-context-copy-tree (source target)
  (ensure-directories-exist (merge-pathnames "placeholder" target))
  (sb-posix:chmod (namestring target) #o700)
  (dolist (path (uiop:directory-files source))
    (uiop:copy-file path (merge-pathnames (file-namestring path) target)))
  (dolist (path (uiop:subdirectories source))
    (adoption-context-copy-tree
     path (merge-pathnames (make-pathname :directory (list :relative (car (last (pathname-directory path))))) target))))

(deftest adoption-context-rejects-an-identical-image-at-a-replaced-stage-inode
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore proof events))
       (let* ((record (pg::physical-adoption-attempt-create state stage client profile))
              (target (getf stage :directory))
              (retired (merge-pathnames "retired-stage/" root)))
         (rename-file target retired)
         (adoption-context-copy-tree retired target)
         (ok (= (getf stage :manifest-digest) (nth-value 1 (pg::recovery-read-manifest target))))
         (ok (/= (getf (getf record :stage) :inode)
                 (sb-posix:stat-ino (sb-posix:lstat (namestring target)))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record))))))))

(deftest adoption-context-checks-late-source-and-target-edits-before-the-final-target-pass
    (dolist (side '(:source :stage))
      (call-with-adoption-context-fixture
       (lambda (root state proof stage client profile events)
         (declare (ignore proof events))
         (let* ((record (pg::physical-adoption-attempt-create state stage client profile))
                (pg::*physical-adoption-context-test-hook*
                  (lambda (point target)
                    (when (eq point :before-target-verification)
                      (pg::wal-atomic-write-bytes
                       (merge-pathnames "pg_xact" (if (eq side :source) root target))
                       (pg::string->bytes "late mutation"))))))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record)))))))))

(deftest adoption-context-rejects-unplanned-stage-root-entries
    (dolist (name '("physical-follow.control" "postgresql.conf" "postgresql.auto.conf" ".hidden-state"))
      (call-with-adoption-context-fixture
       (lambda (root state proof stage client profile events)
         (declare (ignore root proof events))
         (let* ((record (pg::physical-adoption-attempt-create state stage client profile))
                (path (merge-pathnames name (getf stage :directory))))
           (pg::wal-atomic-write-bytes path (pg::string->bytes "unexpected startup state"))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-create state stage client profile))))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record))))
           (delete-file path)
           (sb-posix:symlink "missing-state" (namestring path))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record)))))))))

(deftest adoption-context-rejects-a-symlink-for-an-expected-stage-directory
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore root proof events))
       (let* ((record (pg::physical-adoption-attempt-create state stage client profile))
              (base (merge-pathnames "base/" (getf stage :directory))))
         (with-physical-store-directory (retired)
           (let ((saved (merge-pathnames "base/" retired)))
             (rename-file base saved)
             (sb-posix:symlink (namestring saved) (string-right-trim "/" (namestring base)))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record))))))))))

(deftest adoption-context-rechecks-root-inventory-before-returning
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore root proof events))
       (let ((record (pg::physical-adoption-attempt-create state stage client profile))
             (original (symbol-function 'pg::physical-adoption-attempt-check-stage-root))
             (scans 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-adoption-attempt-check-stage-root)
                      (lambda (target source)
                        (prog1 (funcall original target source)
                          (when (= 1 (incf scans))
                            (pg::wal-atomic-write-bytes
                             (merge-pathnames "physical-follow.control" target)
                             (pg::string->bytes "late startup state"))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-adoption-attempt-validate-context (getf state :owner) record))))
                (ok (= scans 1)))
           (setf (symbol-function 'pg::physical-adoption-attempt-check-stage-root) original))))))
