(in-package #:pg-physical-tests)

(defun hba-test-token-view (line)
  (mapcar (lambda (field)
            (mapcar (lambda (token)
                      (list (pg::hba-token-text token) (pg::hba-token-quoted-p token)))
                    field))
          (pg::hba-tokenize-line line)))

(deftest hba-tokenization-preserves-fields-quotes-and-c-comments
    (ok (equal '(("host") ("all") ("postgres") ("127.0.0.1/32") ("trust"))
               (mapcar (lambda (field) (mapcar #'first field))
                       (hba-test-token-view "host all postgres 127.0.0.1/32 trust # ignored"))))
  (ok (equal '((("host" nil)) (("db1" nil) ("db 2" t))
               (("alice" nil) ("A,B" t)) (("all" nil)) (("reject" nil)))
             (hba-test-token-view "host db1, \"db 2\" alice,\"A,B\" all reject")))
  (ok (equal '((("all" t)) (("a\"b,# c" t)))
             (hba-test-token-view "\"all\" \"a\"\"b,# c\"")))
  (ok (equal '((("unfinished" t)))
             (hba-test-token-view "\"unfinished")))
  (ok (null (pg::hba-tokenize-line "  # comment only"))))

(cl-spec:defproperty hba-quoted-token-roundtrip
    ((number (range integer 0 100000)))
  (:about pg::hba-tokenize-line)
  (:trials (:normal 200))
  (let* ((text (format nil "name ~d,#\"suffix" number))
         (quoted (with-output-to-string (out)
                   (write-char #\" out)
                   (loop for character across text
                         do (write-char character out)
                            (when (char= character #\") (write-char character out)))
                   (write-char #\" out))))
    (equal (list (list (list text t))) (hba-test-token-view quoted))))

(deftest generated-hba-quoted-token-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'hba-quoted-token-roundtrip :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest hba-tokenization-corner-cases-and-input-boundary
    (ok (equal '((("" t))) (hba-test-token-view "\"\"")))
  (ok (equal '((("a" nil) ("b" nil))) (hba-test-token-view ",,a,,b,")))
  (ok (equal '((("a" nil)) (("b" nil))) (hba-test-token-view "a , b")))
  (ok (equal '((("all" nil)) (("all" t))) (hba-test-token-view "a\"\"ll \"\"all")))
  (ok (signals (pg::hba-tokenize-line (format nil "a~%b")) 'error))
  (ok (signals (pg::hba-tokenize-line (string (code-char 0))) 'error)))

(defun hba-test-decision (text user database address &key replication ssl-p)
  (pg::hba-match-rules (pg::hba-parse-text text)
                       user database (pg::hba-parse-ip-address address)
                       :replication replication :ssl-p ssl-p))

(deftest hba-rule-order-and-startup-replication-mode-match-c
    (let ((rules (format nil "host all alice 127.0.0.0/8 reject~%host all all 127.0.0.0/8 trust~%host replication repl 127.0.0.0/8 trust~%")))
      (ok (eq :reject (hba-test-decision rules "alice" "postgres" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "bob" "postgres" "127.255.255.255")))
      (ok (eq :implicit-reject (hba-test-decision rules "bob" "postgres" "128.0.0.1")))
      (ok (eq :implicit-reject
              (hba-test-decision rules "bob" "postgres" "127.0.0.1" :replication :physical)))
      (ok (eq :trust
              (hba-test-decision rules "repl" "ignored" "127.0.0.1" :replication :physical)))
      (ok (eq :trust
              (hba-test-decision rules "bob" "postgres" "127.0.0.1" :replication :database)))))

(deftest hba-quoted-selectors-lists-and-plaintext-transport
    (let ((rules (format nil "hostssl all all all reject~%hostnossl \"all\",sameuser alice,\"A,B\" all trust~%host \"replication\" all all trust~%")))
      (ok (eq :trust (hba-test-decision rules "alice" "all" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "alice" "alice" "127.0.0.1")))
      (ok (eq :implicit-reject (hba-test-decision rules "alice" "postgres" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "A,B" "all" "127.0.0.1")))
      (ok (eq :implicit-reject (hba-test-decision rules "ALICE" "all" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "bob" "replication" "127.0.0.1")))
      (ok (eq :implicit-reject
              (hba-test-decision rules "bob" "replication" "127.0.0.1" :replication :physical)))
      (ok (eq :reject (hba-test-decision rules "alice" "all" "::1" :ssl-p t)))))

(deftest hba-numeric-networks-and-continuations
    (let ((rules (format nil "host all all 192.0.2.129 255.255.255.128 trust~%host all all 2001:db8::/32 reject~%")))
      (ok (eq :implicit-reject (hba-test-decision rules "u" "d" "192.0.2.127")))
      (ok (eq :trust (hba-test-decision rules "u" "d" "192.0.2.128")))
      (ok (eq :trust (hba-test-decision rules "u" "d" "192.0.2.255")))
      (ok (eq :reject (hba-test-decision rules "u" "d" "2001:db8:ffff::1")))
      (ok (eq :implicit-reject (hba-test-decision rules "u" "d" "2001:db9::1")))
      (ok (eq :implicit-reject (hba-test-decision rules "u" "d" "::ffff:192.0.2.128"))))
  (ok (eq :trust
          (hba-test-decision (format nil "host same\\~%user alice 127.0.0.1/32 trust")
                             "alice" "alice" "127.0.0.1"))))

(deftest hba-unsupported-or-empty-policy-is-not-silently-accepted
    (dolist (text '("" "# no rules" "host all all all md5"
                    "host all all all trust map=anything" "host all +group all trust"
                    "host samerole all all trust" "host all all localhost trust"
                    "host all all samehost trust" "host all all 127.0.0.1/33 trust"
                    "host all all ::1/129 trust" "host all all 127.0.0.1 ::1 trust"
                    "host /regex all all trust" "host \"/regex\" all all trust"
                    "host @included all all trust" "include other.conf"))
      (ok (signals (pg::hba-parse-text text) 'error))))

(deftest hba-plus-database-and-quoted-role-are-literal-names
    (ok (eq :trust
            (hba-test-decision "host +db \"+role\" all trust"
                               "+role" "+db" "127.0.0.1")))
  (ok (eq :implicit-reject
          (hba-test-decision "host +db \"+role\" all trust"
                             "role" "+db" "127.0.0.1"))))

(deftest hba-c-numeric-address-compatibility
    (dolist (text '("127.0.0.1" "127.1" "0177.0.0.1" "0x7f000001" "2130706433"))
      (ok (equalp #(127 0 0 1) (pg::hba-parse-ip-address text))))
  (dolist (text '("" "127.0.0.1 " "127.0.0.1 x" "08.0.0.1"
                  "256.0.0.1" "4294967296" "127..1" "+127.1"))
    (ok (signals (pg::hba-parse-ip-address text))))
  (dolist (prefix '("+24" "00024" " 24" "-0"))
    (ok (pg::hba-parse-text
         (format nil "host all all ~s trust" (concatenate 'string "127.1/" prefix)))))
  (dolist (prefix '("" "+" "24 " "0x18" "33" "-1"))
    (ok (signals
         (pg::hba-parse-text
          (format nil "host all all ~s trust" (concatenate 'string "127.1/" prefix)))))))

(deftest hba-bare-at-is-a-literal-selector
    (ok (eq :trust (hba-test-decision "host @ @ all trust" "@" "@" "127.1")))
  (ok (eq :implicit-reject
          (hba-test-decision "host @ @ all trust" "other" "@" "127.1"))))

(deftest hba-continuation-errors-report-source-and-start-line
    (dolist (text (list (format nil "# comment~%host all all \\~%all md5")
                        (format nil "# comment~%host all all \\~%~a"
                                (make-string 65537 :initial-element #\a))))
      (let ((message (handler-case
                         (progn (pg::hba-parse-text text :source "policy.conf") nil)
                       (error (condition) (princ-to-string condition)))))
        (ok (and message (search "policy.conf:2:" message))))))

(deftest hba-tcp-policy-requires-a-valid-peer-address
    (let ((rules (pg::hba-parse-text "host all all all trust")))
      (dolist (peer (list nil #() #(127 0 0) #(127 0 0 256) #(127 0 0 -1)
                          #(127 0 0 nil) '(127 0 0 1) "abcd"))
        (ok (signals (pg::hba-match-rules rules "u" "d" peer))))
      (ok (eq :trust (pg::hba-match-rules rules "u" "d" #(127 0 0 1)))))
  (ok (eq :trust
          (pg::hba-match-rules (pg::hba-parse-text "local all all trust")
                               "u" "d" nil :transport :local))))

(deftest hba-setting-defaults-use-the-original-configuration-directory
    (with-physical-store-directory (root)
      (let ((data (merge-pathnames "storage/" root)))
        (write-startup-config (merge-pathnames "postgresql.conf" root)
                              (format nil "data_directory='~a'~%" (namestring data)))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (let ((settings (pg::startup-load-settings native overrides)))
            (dolist (pair '(("hba_file" . "pg_hba.conf") ("ident_file" . "pg_ident.conf")))
              (let ((entry (gethash (car pair) settings)))
                (ok (equal (namestring (merge-pathnames (cdr pair) root)) (getf entry :value)))
                (ok (eq :override (getf entry :source)))
                (ok (getf entry :implicit-path)))))))))

(deftest hba-setting-explicit-relative-paths-use-the-invocation-directory
    (with-physical-store-directory (root)
      (write-startup-config (merge-pathnames "postgresql.conf" root)
                            (format nil "include='nested/auth.conf'~%"))
      (write-startup-config (merge-pathnames "nested/auth.conf" root)
                            (format nil "hba_file='relative.hba'~%ident_file='relative.ident'~%"))
      (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
        (let ((settings (pg::startup-load-settings native overrides)))
          (dolist (pair '(("hba_file" . "relative.hba") ("ident_file" . "relative.ident")))
            (let ((entry (gethash (car pair) settings)))
              (ok (equal (namestring (pg::startup-absolute-config-path (cdr pair)))
                         (getf entry :value)))
              (ok (eq :override (getf entry :source)))
              (ok (not (getf entry :implicit-path)))))))))

(deftest hba-policy-file-reading-is-bounded-and-keeps-unicode-names
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "policy.hba" root)))
        (write-startup-config path "host café alice all trust")
        (ok (eq :trust
                (pg::hba-match-rules (pg::hba-read-file path) "alice" "café"
                                     #(127 0 0 1))))
        (write-startup-config path (make-string 1048577 :initial-element #\Space))
        (ok (signals (pg::hba-read-file path)))
        (write-startup-config path (format nil "host all all all trust~a" (code-char 0)))
        (ok (signals (pg::hba-read-file path))))))

(deftest ident-policy-reader-accepts-only-empty-or-comment-only-files
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "policy.ident" root)))
        (dolist (text (list "" (format nil "# no identity mappings~%~%")))
          (write-startup-config path text)
          (ok (equal text (pg::ident-read-file path))))
        (write-startup-config path "map system-user database-user")
        (ok (signals (pg::ident-read-file path))))))

(deftest hba-policy-reload-installs-a-complete-generation-or-keeps-the-old-one
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*authentication-policy* nil)
             (pg::*startup-server-settings* nil)
             (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
             (pg::*enable-hot-standby* t) (pg::*wal-level* :replica)
             (pg::*wal-keep-size-mb* 0)
             (pg::*physical-restartpoint-timeout* 300)
             (pg::*physical-restartpoint-wal-bytes* 1073741824)
             (pg::*max-slot-wal-keep-bytes* nil) (pg::*wal-slot-retention-policy* nil)
             (pg::*max-replication-slots* 10)
             (pg::*max-connections* 100) (pg::*max-wal-senders* 10)
             (configuration (merge-pathnames "postgresql.conf" root))
             (hba (merge-pathnames "pg_hba.conf" root)))
        (write-startup-config configuration (format nil "checkpoint_timeout=30~%"))
        (write-startup-config hba "host all all all trust")
        (write-startup-config (merge-pathnames "pg_ident.conf" root) "# no mappings")
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (setf pg::*authentication-policy*
              (pg::authentication-read-policy pg::*startup-server-settings*))
        (let ((first pg::*authentication-policy*))
          (write-startup-config hba "host all all all reject")
          (write-startup-config configuration (format nil "checkpoint_timeout=90~%"))
          (pg::startup-reload-settings)
          (ok (not (eq first pg::*authentication-policy*)))
          (ok (= 90 pg::*physical-restartpoint-timeout*))
          (ok (eq :reject
                  (pg::hba-match-rules
                   (pg::authentication-policy-rules pg::*authentication-policy*)
                   "user" "db" #(127 0 0 1)))))
        (let ((policy pg::*authentication-policy*) (settings pg::*startup-server-settings*))
          (write-startup-config hba "host all all all md5")
          (write-startup-config configuration (format nil "checkpoint_timeout=120~%"))
          (ok (signals (pg::startup-reload-settings)))
          (ok (eq policy pg::*authentication-policy*))
          (ok (eq settings pg::*startup-server-settings*))
          (ok (= 90 pg::*physical-restartpoint-timeout*)))
        (write-startup-config hba "host all all all trust")
        (write-startup-config configuration
                              (format nil "hba_file='~a'~%checkpoint_timeout=120~%"
                                      (namestring (merge-pathnames "missing.hba" root))))
        (pg::startup-reload-settings)
        (let ((entry (gethash "hba_file" pg::*startup-server-settings*)))
          (ok (getf entry :pending-restart))
          (ok (equal (namestring hba) (getf entry :value))))
        (ok (= 120 pg::*physical-restartpoint-timeout*))
        (ok (eq :trust
                (pg::hba-match-rules
                 (pg::authentication-policy-rules pg::*authentication-policy*)
                 "user" "db" #(127 0 0 1)))))))

(deftest authentication-paths-canonicalize-final-dot-and-directory-separators
    (let* ((cwd (namestring (uiop:getcwd)))
           (directory (if (= 1 (length cwd)) cwd (string-right-trim "/" cwd))))
      (dolist (text '("" "." "./" "a/.." "a/../"))
        (ok (equal directory (namestring (pg::startup-absolute-config-path text)))))
      (ok (equal "/" (namestring (pg::startup-absolute-config-path "/.././"))))
      (ok (equal (namestring (merge-pathnames "a/c" (uiop:getcwd)))
                 (namestring (pg::startup-absolute-config-path "a//b/../c/"))))))

(deftest explicit-config-without-config-directory-requires-both-auth-paths
    (with-physical-store-directory (root)
      (let ((old (uiop:getenv "PGDATA"))
            (configuration (merge-pathnames "postgresql.conf" root)))
        (unwind-protect
             (progn
               (sb-posix:unsetenv "PGDATA")
               (write-startup-config configuration
                                     (format nil "data_directory='~a'~%" (namestring root)))
               (multiple-value-bind (native overrides)
                   (pg::startup-cli-inputs (list "--config-file" (namestring configuration)))
                 (ok (signals (pg::startup-load-settings native overrides)))
                 (setf (gethash "hba_file" overrides) "explicit.hba")
                 (ok (signals (pg::startup-load-settings native overrides)))
                 (setf (gethash "ident_file" overrides) "explicit.ident")
                 (let ((settings (pg::startup-load-settings native overrides)))
                   (ok (equal (namestring (pg::startup-absolute-config-path "explicit.hba"))
                              (pg::startup-setting-value settings "hba_file"))))))
          (if old (sb-posix:setenv "PGDATA" old 1) (sb-posix:unsetenv "PGDATA"))))))

(deftest authentication-file-reader-follows-regular-symlinks-and-rejects-special-inputs
    (with-physical-store-directory (root)
      (let ((target (merge-pathnames "target.hba" root))
            (link (merge-pathnames "link.hba" root))
            (fifo (merge-pathnames "fifo.hba" root)))
        (write-startup-config target "host all all all trust")
        (sb-posix:symlink (namestring target) (namestring link))
        (ok (pg::hba-read-file link))
        (sb-posix:mkfifo (namestring fifo) #o600)
        (sb-ext:with-timeout 1
          (ok (signals (pg::hba-read-file fifo))))
        (ok (signals (pg::hba-read-file root)))
        (with-open-file (stream target :direction :output :if-exists :supersede
                                       :element-type '(unsigned-byte 8))
          (write-byte 255 stream))
        (ok (signals (pg::hba-read-file link))))))

(deftest authentication-default-initialization-requires-owned-data-and-never-regenerates-hba
    (call-with-pid-owner
     (lambda (root)
       (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
         (let* ((settings (pg::startup-load-settings native overrides))
                (policy (pg::authentication-prepare-startup settings))
                (hba (merge-pathnames "pg_hba.conf" root)))
           (ok (probe-file (pg::authentication-marker-path root)))
           (dolist (replication '(nil :physical :database))
             (ok (eq :trust (pg::hba-match-rules (pg::authentication-policy-rules policy)
                                                 "user" "db" #(127 0 0 1)
                                                 :replication replication))))
           (ok (eq :implicit-reject
                   (pg::hba-match-rules (pg::authentication-policy-rules policy)
                                        "user" "db" #(192 0 2 1))))
           (dolist (name '("pg_hba.conf" "pg_ident.conf"))
             (ok (= #o600 (logand #o777 (sb-posix:stat-mode
                                         (sb-posix:stat (merge-pathnames name root)))))))
           (delete-file hba)
           (ok (signals (pg::authentication-prepare-startup settings)))
           (ok (not (probe-file hba))))))))

(deftest authentication-initialization-preserves-an-existing-restrictive-policy
    (call-with-pid-owner
     (lambda (root)
       (let ((hba (merge-pathnames "pg_hba.conf" root)))
         (write-startup-config hba "host all all all reject")
         (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
           (let* ((settings (pg::startup-load-settings native overrides))
                  (policy (pg::authentication-prepare-startup settings)))
             (ok (equal "host all all all reject" (pg::authentication-policy-hba-text policy)))
             (ok (eq :reject
                     (pg::hba-match-rules (pg::authentication-policy-rules policy)
                                          "user" "db" #(127 0 0 1))))
             (ok (probe-file (merge-pathnames "pg_ident.conf" root)))
             (ok (probe-file (pg::authentication-marker-path root)))
             (ok (equal "host all all all reject"
                        (pg::authentication-policy-hba-text
                         (pg::authentication-prepare-startup settings))))))))))

(deftest authentication-preflight-refuses-missing-external-policy-before-creating-data
    (with-physical-store-directory (root)
      (let* ((data (merge-pathnames "storage/" root))
             (pg::*pgdata* data))
        (write-startup-config (merge-pathnames "postgresql.conf" root)
                              (format nil "data_directory='~a'~%" (namestring data)))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (let ((settings (pg::startup-load-settings native overrides)))
            (ok (signals (pg::authentication-preflight-startup settings)))
            (ok (not (probe-file data)))
            (ok (not (probe-file (merge-pathnames "pg_hba.conf" root)))))
          (setf (gethash "hba_file" overrides) (namestring (merge-pathnames "explicit-missing.hba" root)))
          (ok (signals
               (pg::authentication-preflight-startup
                (pg::startup-load-settings native overrides))))))))

(defun hba-test-sqlstate (function)
  (handler-case (progn (funcall function) nil)
    (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))

(deftest authentication-startup-identities-use-final-values-and-c-byte-limits
    (ok (equal '("last" "last")
               (multiple-value-list
                (pg::authentication-startup-identities
                 '(("user" . "first") ("user" . "last") ("database" . "db") ("database" . "")) nil))))
  (dolist (params '(nil (("USER" . "upper")) (("user" . "first") ("user" . ""))))
    (ok (equal "28000"
               (hba-test-sqlstate (lambda () (pg::authentication-startup-identities params nil))))))
  (let ((name (make-string 70 :initial-element #\a)))
    (ok (equal (list (subseq name 0 63) (subseq name 0 63))
               (multiple-value-list (pg::authentication-startup-identities
                                     (list (cons "user" name)) nil)))))
  (let ((name (concatenate 'string (make-string 61 :initial-element #\a) "é")))
    (ok (equal (list name name)
               (multiple-value-list (pg::authentication-startup-identities
                                     (list (cons "user" name)) nil)))))
  (let ((split (concatenate 'string (make-string 62 :initial-element #\a) "é")))
    (ok (equal "22021"
               (hba-test-sqlstate
                (lambda () (pg::authentication-startup-identities
                            (list (cons "user" split)) nil)))))
    (ok (equal '("user" "")
               (multiple-value-list
                (pg::authentication-startup-identities
                 (list (cons "user" "user") (cons "database" split)) :physical))))))

(deftest authentication-replication-parameters-preserve-c-ordering
    (dolist (pair '((("tr") . :physical) (("ye") . :physical) (("of") . nil)
                    (("database" "true") . :database) (("database" "false") . nil)
                    (("database" "false" "true") . :database)))
      (ok (eq (cdr pair)
              (pg::startup-replication-parameters-mode
               (mapcar (lambda (value) (cons "replication" value)) (car pair))))))
  (dolist (values '(("bad" "true") (" true") ("DATABASE")))
    (ok (equal "22023"
               (hba-test-sqlstate
                (lambda ()
                  (pg::startup-replication-parameters-mode
                   (mapcar (lambda (value) (cons "replication" value)) values))))))))

(deftest authentication-final-options-replace-earlier-options-without-reordering-gucs
    (let ((pg::*backend* (pg::make-backend)))
      (pg::session-apply-startup-options
       '(("options" . "-c extra_float_digits=4") ("options" . "")))
      (ok (equal "1" (pg::session-extra-setting-value "extra_float_digits")))
      (ok (equal "22023"
                 (hba-test-sqlstate
                  (lambda () (pg::session-apply-startup-options
                              '(("extra_float_digits" . "4") ("extra_float_digits" . "3")))))))))

(deftest authentication-publication-failures-never-leave-partial-final-files
    (with-physical-store-directory (root)
      (dolist (point '(:after-file-write :after-file-fsync :after-publish :after-directory-fsync))
        (let* ((path (merge-pathnames (format nil "~(~a~).hba" point) root))
               (text "host all all all reject")
               (pg::*authentication-publication-test-hook*
                 (lambda (where target)
                   (declare (ignore target))
                   (when (eq where point) (error "authentication publication failpoint")))))
          (ok (signals (pg::authentication-create-file path text)))
          (if (member point '(:after-file-write :after-file-fsync))
              (ok (not (probe-file path)))
              (ok (equal text (nth-value 1 (pg::hba-read-file path)))))
          (let ((pg::*authentication-publication-test-hook* nil))
            (pg::authentication-create-file path text)
            (ok (equal text (nth-value 1 (pg::hba-read-file path)))))))))

(deftest authentication-initialization-refuses-unowned-data-and-dangling-entries
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil))
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (ok (signals (pg::authentication-prepare-startup
                        (pg::startup-load-settings native overrides))))
          (ok (not (probe-file (merge-pathnames "pg_hba.conf" root)))))))
  (call-with-pid-owner
   (lambda (root)
     (let ((hba (merge-pathnames "pg_hba.conf" root)))
       (sb-posix:symlink "missing-policy" (namestring hba))
       (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
         (let ((settings (pg::startup-load-settings native overrides)))
           (ok (signals (pg::authentication-prepare-startup settings)))
           (ok (sb-posix:s-islnk (sb-posix:stat-mode (sb-posix:lstat hba))))
           (delete-file hba)
           (write-startup-config hba "host all all all reject")
           (sb-posix:symlink "missing-marker" (namestring (pg::authentication-marker-path root)))
           (ok (signals (pg::authentication-prepare-startup settings)))
           (ok (sb-posix:s-islnk
                (sb-posix:stat-mode (sb-posix:lstat (pg::authentication-marker-path root)))))))))))

(deftest authentication-temporary-descriptors-are-exclusive-and-close-on-exec
    (with-physical-store-directory (root)
      (let ((paths nil))
        (dotimes (index 2)
          (declare (ignore index))
          (multiple-value-bind (fd path)
              (pg::open-exclusive-temporary-file (merge-pathnames ".auth-XXXXXX" root))
            (unwind-protect
                 (progn
                   (ok (not (member path paths :test #'equal)))
                   (push path paths)
                   (ok (logbitp 0 (sb-posix:fcntl fd sb-posix:f-getfd)))
                   (ok (sb-posix:s-isreg (sb-posix:stat-mode (sb-posix:fstat fd))))
                   (ok (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:fstat fd))))))
              (sb-posix:close fd)
              (sb-posix:unlink (namestring path)))))
        (ok (signals (pg::open-exclusive-temporary-file
                      (merge-pathnames "missing/.auth-XXXXXX" root)))))))

(deftest authentication-authorization-requires-an-installed-matching-trust-rule
    (let ((trust (pg::make-authentication-policy
                  :rules (pg::hba-parse-text "host all alice 127.0.0.1/32 trust")))
          (reject (pg::make-authentication-policy
                   :rules (pg::hba-parse-text "host all all all reject"))))
      (ok (pg::authentication-authorize trust "alice" "db" #(127 0 0 1) nil))
      (dolist (policy (list nil reject trust))
        (ok (equal "28000"
                   (hba-test-sqlstate
                    (lambda () (pg::authentication-authorize policy "other" "db" #(127 0 0 1) nil))))))
      (ok (equal "28000"
                 (hba-test-sqlstate
                  (lambda () (pg::authentication-authorize trust "alice" "db"
                                                           #(127 0 0 1) :physical))))))
  (ok (equal "127.0.0.1" (pg::authentication-peer-text #(127 0 0 1))))
  (ok (equal "::1" (pg::authentication-peer-text #(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1)))))
