(in-package #:pg-physical-tests)

(cl-spec:defproperty adoption-manifest-image-preserves-unsigned-facts
    ((system (range integer 0 18446744073709551615))
     (next (range integer 3 18446744073709551616))
     (digest (range integer 0 18446744073709551615)))
  (:about pg::physical-bootstrap-manifest-image)
  (:trials (:normal 100))
  (with-physical-store-directory (root)
    (let* ((manifest (list :version 3 :system-id system :cluster-identity "stage-λ"
                           :timeline 1 :checkpoint 152 :checkpoint-start 32 :end 152
                           :next-xid next :source-pin "staged"
                           :files (list (list "PG_VERSION" 3 digest))))
           (image (pg::physical-bootstrap-manifest-image manifest)))
      (pg::wal-atomic-write-bytes (merge-pathnames "physical-backup.manifest" root) image)
      (multiple-value-bind (decoded commitment) (pg::recovery-read-manifest root)
        (and (equal manifest decoded)
             (= commitment (pg::physical-store-update-digest #xCBF29CE484222325 image)))))))

(deftest generated-adoption-manifest-image-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-manifest-image-preserves-unsigned-facts :seed 75))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun call-with-adoption-stage-fixture (function)
  (call-with-adoption-clean-primary-fixture
   (lambda (root snapshot metadata)
     (declare (ignore snapshot metadata))
     (pg::wal-atomic-write-bytes (merge-pathnames "pg_xact" root)
                                 (make-array 0 :element-type '(unsigned-byte 8)))
     (dolist (entry (list (cons "authentication.initialized" pg::*authentication-marker-text*)
                          (cons "pg_hba.conf" (format nil "host all all 127.0.0.1/32 trust~%"))
                          (cons "pg_ident.conf" "")))
       (pg::wal-atomic-write-bytes (merge-pathnames (car entry) root)
                                   (pg::string->bytes (cdr entry))))
     (let* ((origin (pg::make-physical-export-origin
                     :system-id (pg::catalog-system-identifier) :timeline 1
                     :base 32 :manifest-digest 77 :proven-lsn 32
                     :prefix-digest #xCBF29CE484222325))
            (identity (pg::physical-export-origin-identity origin)))
       (pg::wal-ensure-directory-durable (pg::physical-export-origin-directory))
       (pg::wal-atomic-write-bytes (pg::physical-export-origin-path origin)
                                   (pg::physical-export-origin-encode origin))
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (let* ((state (pg::physical-adoption-inspect-primary owner))
                 (proof (pg::physical-adoption-prove-primary-origin state identity)))
            (funcall function root state proof))))))))

(defun adoption-stage-source-bytes (root)
  (mapcar (lambda (name) (cons name (lineage-test-bytes (merge-pathnames name root))))
          '("PG_VERSION" "pg_xact" "global/pg_control" "global/pg_catalog.dat"
            "global/pg_wal_control" "pg_wal/0000000000000000.wal"
            "authentication.initialized" "pg_hba.conf" "pg_ident.conf")))

(deftest adoption-stage-verifies-clean-e-with-actual-xid-without-changing-source
    (call-with-adoption-stage-fixture
     (lambda (root state proof)
       (with-physical-store-directory (target)
         (sb-posix:chmod (namestring target) #o700)
         (let* ((before (adoption-stage-source-bytes root))
                (result (pg::physical-adoption-stage-base state proof target "adopted_base"))
                (manifest (getf result :base-state)))
           (ok (= 152 (getf manifest :checkpoint) (getf manifest :end)))
           (ok (= 42 (getf manifest :next-xid)))
           (ok (= 1003 (getf state :allocation-next-xid)))
           (ok (= (getf result :manifest-digest) (getf manifest :manifest-digest)))
           (ok (equal proof (getf result :origin-proof)))
           (ok (pg::authentication-check-backup-policy target))
           (let ((pg::*pgdata* target))
             (ok (pg::physical-recovery-verify-base-state)))
           (ok (equalp before (adoption-stage-source-bytes root)))
           (ok (not (probe-file (merge-pathnames "recovery.required" root))))
           (ok (not (probe-file (pg::physical-source-pin-path "adopted_base")))))))))

(deftest adoption-stage-refuses-stale-origin-and-noncanonical-auth-inputs
    (dolist (mode '(:digest :control :hba :ident :redirect))
      (call-with-adoption-stage-fixture
       (lambda (root state proof)
         (with-physical-store-directory (target)
           (sb-posix:chmod (namestring target) #o700)
           (ecase mode
             (:digest (setf (getf proof :end-digest) (1+ (getf proof :end-digest))))
             (:control (let ((bytes (lineage-test-bytes (pg::wal-control-path))))
                         (setf (aref bytes 0) 0)
                         (pg::wal-atomic-write-bytes (pg::wal-control-path) bytes)))
             (:hba (delete-file (merge-pathnames "pg_hba.conf" root)))
             (:ident (pg::wal-atomic-write-bytes (merge-pathnames "pg_ident.conf" root)
                                                 (pg::string->bytes "not a valid mapping")))
             (:redirect (pg::wal-atomic-write-bytes
                         (merge-pathnames "postgresql.auto.conf" root)
                         (pg::string->bytes (format nil "hba_file = 'elsewhere.conf'~%")))))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-stage-base state proof target "adopted_base")))))))))

(deftest adoption-stage-faults-leave-guarded-private-images-and-preserve-source
    (dolist (cut '(:after-guard :after-file :before-manifest :after-manifest :before-verify))
      (call-with-adoption-stage-fixture
       (lambda (root state proof)
         (with-physical-store-directory (target)
           (sb-posix:chmod (namestring target) #o700)
           (let ((before (adoption-stage-source-bytes root))
                 (pg::*physical-adoption-stage-test-hook*
                   (lambda (point value)
                     (declare (ignore value))
                     (when (eq point cut) (error "stage cut")))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-stage-base state proof target "adopted_base"))))
             (ok (probe-file (merge-pathnames "recovery.required" target)))
             (ok (equalp before (adoption-stage-source-bytes root)))))))))

(deftest adoption-stage-detects-source-and-target-wal-changes-with-unchanged-control
    (dolist (side '(:source :target))
      (call-with-adoption-stage-fixture
       (lambda (root state proof)
         (with-physical-store-directory (target)
           (sb-posix:chmod (namestring target) #o700)
           (let ((control (lineage-test-bytes (pg::wal-control-path)))
                 (pg::*physical-adoption-stage-test-hook*
                   (lambda (point value)
                     (declare (ignore value))
                     (when (eq point :before-verify)
                       (let* ((directory (if (eq side :source) root target))
                              (path (merge-pathnames "pg_wal/0000000000000000.wal" directory))
                              (bytes (lineage-test-bytes path)))
                         (setf (aref bytes 32) (logxor 1 (aref bytes 32)))
                         (pg::wal-atomic-write-bytes path bytes))))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-stage-base state proof target "adopted_base"))))
             (ok (equalp control (lineage-test-bytes (pg::wal-control-path))))
             (ok (probe-file (merge-pathnames "recovery.required" target)))))))))

(deftest adoption-stage-refuses-public-or-symlink-targets-and-source-files
    (dolist (mode '(:public :target-link :source-link))
      (call-with-adoption-stage-fixture
       (lambda (root state proof)
         (with-physical-store-directory (target)
           (sb-posix:chmod (namestring target) (if (eq mode :public) #o755 #o700))
           (let ((alias (merge-pathnames "alias" target)))
             (unwind-protect
                  (progn
                    (case mode
                      (:target-link (sb-posix:symlink (namestring target) (namestring alias)))
                      (:source-link
                       (rename-file (merge-pathnames "pg_xact" root) (merge-pathnames "pg_xact.saved" root))
                       (sb-posix:symlink "pg_xact.saved" (namestring (merge-pathnames "pg_xact" root)))))
                    (ok (intake-fails-p
                         (lambda ()
                           (pg::physical-adoption-stage-base
                            state proof (if (eq mode :target-link) alias target) "adopted_base"))))
                    (ok (not (probe-file (merge-pathnames "recovery.required" target)))))
               (when (eq mode :target-link) (delete-file alias)))))))))

(defun call-with-adoption-rooted-stage-fixture (function)
  (handoff-test-committed
   (lambda (root store reference)
     (declare (ignore store reference))
     (let* ((pg::*pgdata* root)
            (pg::*catalog* (pg::load-catalog))
            (fields (multiple-value-list (pg::wal-read-control-fields)))
            (metadata (copy-list (seventh fields)))
            (timeline (fourth fields)))
       (dolist (name '("physical-backup.manifest" "physical-follow.control"
                       "physical-recovery.control" "physical-restartpoint.control" "recovery.required"))
         (delete-file (merge-pathnames name root)))
       (sb-posix:chmod (namestring root) #o700)
       (setf (getf metadata :db-state) pg::+db-shutdowned+)
       (pg::wal-atomic-write-bytes
        (pg::wal-control-path)
        (pg::wal-encode-control-image
         (first fields) (second fields) (third fields) :timeline timeline
         :checkpoint-timeline timeline :namespace :timeline :metadata metadata))
       (dolist (entry (list (cons "PG_VERSION" (format nil "16~%"))
                            (cons "authentication.initialized" pg::*authentication-marker-text*)
                            (cons "pg_hba.conf" (format nil "host all all 127.0.0.1/32 trust~%"))
                            (cons "pg_ident.conf" "")))
         (pg::wal-atomic-write-bytes (merge-pathnames (car entry) root)
                                     (pg::string->bytes (cdr entry))))
       (dolist (name '(".pg_lisp.lock" "pg_xact"))
         (pg::wal-atomic-write-bytes (merge-pathnames name root) (empty-octets)))
       (let ((origin (pg::make-physical-export-origin
                      :system-id (pg::catalog-system-identifier) :timeline timeline
                      :base (first fields) :manifest-digest 991 :proven-lsn (first fields)
                      :prefix-digest #xCBF29CE484222325)))
         (pg::wal-ensure-directory-durable (pg::physical-export-origin-directory))
         (pg::wal-atomic-write-bytes (pg::physical-export-origin-path origin)
                                     (pg::physical-export-origin-encode origin))
         (pg::call-with-physical-adoption-owner
          root
          (lambda (owner)
            (let* ((state (pg::physical-adoption-inspect-primary owner))
                   (proof (pg::physical-adoption-prove-primary-origin
                           state (pg::physical-export-origin-identity origin))))
              (funcall function root state proof)))))))))

(deftest adoption-stage-new-manifest-drops-only-the-copied-retained-reference
    (call-with-adoption-rooted-stage-fixture
     (lambda (root state proof)
       (declare (ignore root))
       (with-physical-store-directory (target)
         (sb-posix:chmod (namestring target) #o700)
         (let* ((before (lineage-test-bytes (pg::wal-control-path)))
                (reference (copy-tree (getf (getf state :metadata) :retained-reference)))
                (result (pg::physical-adoption-stage-base state proof target "adopted_child"))
                (bytes (lineage-test-bytes (merge-pathnames "global/pg_wal_control" target)))
                (copied (multiple-value-list (pg::wal-decode-control-line (pg::bytes->string bytes))))
                (manifest (getf result :base-state))
                (entry (find "global/pg_wal_control" (getf manifest :files) :key #'first :test #'equal)))
           (ok (equalp before (lineage-test-bytes (pg::wal-control-path))))
           (ok (equal reference (getf (seventh (multiple-value-list (pg::wal-read-control-fields)))
                                      :retained-reference)))
           (ok (null (getf (seventh copied) :retained-reference)))
           (ok (not (equalp before bytes)))
           (ok (equal entry (list "global/pg_wal_control" (length bytes)
                                  (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
           (ok (= (getf manifest :manifest-digest)
                  (nth-value 1 (pg::recovery-read-manifest target)))))))))

(deftest adoption-stage-rejects-valid-looking-changes-to-auth-adjuncts
    (dolist (mode '(:hba :ident :symlink))
      (call-with-adoption-stage-fixture
       (lambda (root state proof)
         (with-physical-store-directory (target)
           (sb-posix:chmod (namestring target) #o700)
           (let ((before (adoption-stage-source-bytes root))
                 (pg::*physical-adoption-stage-test-hook*
                   (lambda (point value)
                     (declare (ignore value))
                     (when (eq point :before-verify)
                       (ecase mode
                         (:hba
                          (pg::wal-atomic-write-bytes
                           (merge-pathnames "pg_hba.conf" target)
                           (pg::string->bytes (format nil "host all all 127.0.0.1/32 reject~%"))))
                         (:ident
                          (pg::wal-atomic-write-bytes
                           (merge-pathnames "pg_ident.conf" target)
                           (pg::string->bytes (format nil "# changed but still valid~%"))))
                         (:symlink
                          (let ((path (merge-pathnames "pg_hba.conf" target)))
                            (delete-file path)
                            (sb-posix:symlink (namestring (merge-pathnames "pg_hba.conf" root))
                                              (namestring path)))))))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-stage-base state proof target "adopted_base"))))
             (ok (equalp before (adoption-stage-source-bytes root)))))))))
