(in-package #:pg-physical-tests)

(defun authentication-test-manifest ()
  (list :version 3 :system-id 123 :cluster-identity "auth#identity"
        :timeline 1 :checkpoint 152 :checkpoint-start 32 :end 152
        :next-xid 3 :source-pin "copy" :files '(("PG_VERSION" 3 9))))

(cl-spec:defproperty authentication-manifest-required-flag-is-exact
    ((value (range integer -2 3)))
  (:about pg::recovery-check-manifest-schema)
  (:trials (:normal 200))
  (let ((manifest (append (authentication-test-manifest) (list :auth-policy-required value))))
    (eql (= value 1)
         (handler-case (progn (pg::recovery-check-manifest-schema manifest) t)
           (error () nil)))))

(deftest generated-authentication-manifest-required-flag
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'authentication-manifest-required-flag-is-exact :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest authentication-backup-manifest-requires-an-exact-optional-field
    (let* ((old (authentication-test-manifest))
           (new (append old '(:auth-policy-required 1))))
      (ok (eq old (pg::recovery-check-manifest-schema old)))
      (ok (eq new (pg::recovery-check-manifest-schema new)))
      (dolist (bad (list (append old '(:auth-policy-required 1 :auth-policy-required 1))
                         (append '(:auth-policy-required 1) old)
                         (append old '(:auth-policy-required 1.0))
                         (append old '(:auth-policy-required nil))
                         (append old '(:auth-policy-required 2))))
        (ok (signals (pg::recovery-check-manifest-schema bad))))
      (setf (getf new :version) 2)
      (ok (signals (pg::recovery-check-manifest-schema new)))))

(deftest authentication-required-backup-never-bootstraps-missing-adjuncts
    (call-with-pid-owner
     (lambda (root)
       (write-recovery-manifest-text
        root (pg::physical-bootstrap-manifest-text
              (append (authentication-test-manifest) '(:auth-policy-required 1))))
       (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
         (let ((settings (pg::startup-load-settings native overrides)))
           (ok (signals (pg::authentication-prepare-startup settings)))
           (dolist (name '("pg_hba.conf" "pg_ident.conf" "authentication.initialized"))
             (ok (not (probe-file (merge-pathnames name root)))))
           (write-startup-config (merge-pathnames "pg_hba.conf" root) "host all all all reject")
           (write-startup-config (merge-pathnames "pg_ident.conf" root) "# local mappings")
           (write-startup-config (pg::authentication-marker-path root) pg::*authentication-marker-text*)
           (ok (pg::authentication-prepare-startup settings))
           (write-startup-config (merge-pathnames "pg_hba.conf" root)
                                 "host all alice 127.0.0.1/32 reject")
           (ok (pg::authentication-prepare-startup settings))
           (delete-file (merge-pathnames "pg_ident.conf" root))
           (ok (signals (pg::authentication-prepare-startup settings))))))))

(deftest authentication-backup-copies-the-installed-generation-without-source-path-reads
    (with-physical-store-directory (root)
      (let* ((source (merge-pathnames "source/" root))
             (target (merge-pathnames "target/" root))
             (hba (merge-pathnames "pg_hba.conf" source))
             (ident (merge-pathnames "pg_ident.conf" source)))
        (write-startup-config hba "host all all all reject")
        (write-startup-config ident "# preserve comments")
        (ensure-directories-exist (merge-pathnames "placeholder" target))
        (multiple-value-bind (native overrides)
            (pg::startup-cli-inputs (list "-D" (namestring source)))
          (let* ((settings (pg::startup-load-settings native overrides))
                 (pg::*authentication-policy* (pg::authentication-read-policy settings))
                 (pg::*startup-installed-configuration*
                   (pg::startup-prepare-installation settings native overrides pg::*authentication-policy*)))
            (write-startup-config hba "host all all all md5")
            (delete-file ident)
            (pg::authentication-write-backup-policy (pg::authentication-capture-policy) target)
            (ok (equal "host all all all reject"
                       (nth-value 1 (pg::hba-read-file (merge-pathnames "pg_hba.conf" target)))))
            (ok (equal "# preserve comments" (pg::ident-read-file (merge-pathnames "pg_ident.conf" target))))
            (ok (pg::authentication-check-marker target))
            (ok (signals (pg::authentication-write-backup-policy pg::*authentication-policy* target)))))))
  (let ((pg::*startup-installed-configuration* nil) (pg::*authentication-policy* nil))
    (ok (signals (pg::authentication-capture-policy)))))

(deftest authentication-interrupted-backup-never-initializes-default-policy
    (dolist (existing-hba '(nil t))
      (call-with-pid-owner
       (lambda (root)
         (pg::physical-bootstrap-mark-target root)
         (when existing-hba
           (write-startup-config (merge-pathnames "pg_hba.conf" root) "host all all all reject"))
         (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
           (ok (signals (pg::authentication-prepare-startup
                         (pg::startup-load-settings native overrides))))
           (ok (not (probe-file (merge-pathnames "pg_ident.conf" root))))
           (ok (not (probe-file (pg::authentication-marker-path root))))
           (if existing-hba
               (ok (equal "host all all all reject"
                          (nth-value 1 (pg::hba-read-file (merge-pathnames "pg_hba.conf" root)))))
               (ok (not (probe-file (merge-pathnames "pg_hba.conf" root))))))))))
