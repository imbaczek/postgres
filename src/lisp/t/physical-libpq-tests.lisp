(in-package #:pg-physical-tests)

(cl-spec:defproperty physical-libpq-headers-preserve-unsigned-identities
    ((session (range integer 1 #xffffffffffffffff))
     (request (range integer 1 #xffffffffffffffff))
     (size (range integer 0 1048576)))
  (:about pg::physical-libpq-header)
  (:trials (:normal 200))
  (let ((header (pg::physical-libpq-header session request #\T size)))
    (multiple-value-bind (kind actual)
        (pg::physical-libpq-check-header header session request '(#\T) size)
      (and (= 25 (length header)) (char= kind #\T) (= size actual)))))

(deftest generated-physical-libpq-header-identities
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-libpq-headers-preserve-unsigned-identities :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest physical-libpq-header-rejects-stale-malformed-and-oversized-responses
    (dolist (change '(:magic :session :request :kind :length :short :trailing :global-bound))
      (let ((header (pg::physical-libpq-header 42 1 #\T 4)))
        (ecase change
          (:global-bound
           (setf header (pg::physical-libpq-header 42 1 #\T #xffffffff)))
          (:magic (setf (aref header 0) 0))
          (:session (setf (aref header 11) 43))
          (:request (setf (aref header 19) 2))
          (:kind (setf (aref header 20) (char-code #\D)))
          (:length (setf (aref header 24) 5))
          (:short (setf header (subseq header 0 24)))
          (:trailing (setf header (concatenate '(vector (unsigned-byte 8)) header #(0)))))
        (ok (intake-fails-p
             (lambda ()
               (pg::physical-libpq-check-header header 42 1 '(#\T)
                                                (if (eq change :global-bound) #xffffffff 4))))))))

(deftest physical-libpq-profile-copies-input-and-keeps-secrets-out-of-printing
    (let* ((original (copy-seq "host=localhost password=private-fixture-marker"))
           (fallback (copy-seq "walreceiver"))
           (profile (pg::make-physical-libpq-profile original :fallback fallback))
           (expected (copy-seq original)))
      (setf (char original 0) #\X (char fallback 0) #\X)
      (ok (equal expected (pg::physical-libpq-profile-conninfo profile)))
      (ok (equal "walreceiver" (pg::physical-libpq-profile-fallback profile)))
      (setf (char (pg::physical-libpq-profile-conninfo profile) 0) #\Y)
      (ok (equal expected (pg::physical-libpq-profile-conninfo profile)))
      (ok (not (search "private-fixture-marker" (princ-to-string profile))))
      (ok (intake-fails-p
           (lambda () (pg::make-physical-libpq-profile (format nil "host=x~c" (code-char 0))))))))

(deftest physical-libpq-new-profiles-use-installed-cluster-name-with-explicit-fallback-precedence
    (let* ((pg::*startup-server-settings* (make-hash-table :test #'equal))
           (entry (list :value "research")))
      (setf (gethash "cluster_name" pg::*startup-server-settings*) entry)
      (let ((saved (pg::make-physical-libpq-profile "host=localhost")))
        (ok (equal "research" (pg::physical-libpq-profile-fallback saved)))
        (ok (equal "explicit"
                   (pg::physical-libpq-profile-fallback
                    (pg::make-physical-libpq-profile "host=localhost" :fallback "explicit"))))
        (ok (equal ""
                   (pg::physical-libpq-profile-fallback
                    (pg::make-physical-libpq-profile "host=localhost" :fallback ""))))
        (setf (getf entry :value) "replacement")
        (ok (equal "research" (pg::physical-libpq-profile-fallback saved)))
        (ok (equal "replacement"
                   (pg::physical-libpq-profile-fallback
                    (pg::make-physical-libpq-profile "host=localhost"))))
        (setf (getf entry :value) "")
        (ok (equal "walreceiver"
                   (pg::physical-libpq-profile-fallback
                    (pg::make-physical-libpq-profile "host=localhost")))))))

(defun libpq-test-string (payload text)
  (let ((bytes (pg::string->bytes text)))
    (pg::payload-push-u32 payload (length bytes))
    (loop for byte across bytes do (vector-push-extend byte payload))))

(defun libpq-test-connection-body (parameters)
  (let ((body (pg::make-payload)))
    (dolist (text '("localhost" "127.0.0.1" "5432" "db=literal" "postgres"))
      (libpq-test-string body text))
    (pg::payload-push-u32 body (length parameters))
    (dolist (parameter parameters)
      (libpq-test-string body (first parameter))
      (if (null (second parameter))
          (pg::payload-push-u32 body #xffffffff)
          (let ((value (second parameter)))
            (if (stringp value) (libpq-test-string body value)
                (progn
                  (pg::payload-push-u32 body (length value))
                  (loop for byte across value do (vector-push-extend byte body)))))))
    body))

(deftest physical-libpq-connection-metadata-keeps-private-options-out-of-public-text
    (let* ((body (libpq-test-connection-body
                  '(("password" "private-marker") ("options" "private-options")
                    ("scram_client_key" "private-scram") ("application_name" "receiver")
                    ("sslmode" "verify-full") ("passfile" nil) ("sslcert" ""))))
           (info (pg::physical-libpq-decode-connection-info body))
           (public (pg::physical-libpq-public-connection-info info)))
      (ok (equalp (pg::string->bytes "127.0.0.1") (pg::physical-libpq-connection-info-hostaddr info)))
      (ok (= 5432 (pg::physical-libpq-connection-info-port info)))
      (ok (equalp (pg::string->bytes "db=literal") (pg::physical-libpq-connection-info-database info)))
      (ok (not (search "private-" public)))
      (ok (not (search "private-" (princ-to-string info))))
      (ok (search "application_name='receiver'" public))
      (ok (search "sslmode='verify-full'" public))
      (ok (null (second (assoc "passfile" (pg::physical-libpq-connection-info-parameters info)
                               :test #'equal))))
      (ok (equalp (pg::string->bytes "") (second (assoc "sslcert" (pg::physical-libpq-connection-info-parameters info)
                                                        :test #'equal))))))

(deftest physical-libpq-connection-metadata-rejects-duplicates-truncation-and-trailing-bytes
    (let ((body (libpq-test-connection-body '(("host" "localhost")))))
      (dolist (bad (list (subseq body 0 (1- (length body)))
                         (concatenate '(vector (unsigned-byte 8)) body #(0))
                         (libpq-test-connection-body '(("host" "a") ("host" "b")))
                         (libpq-test-connection-body '(("bad-key" "a")))))
        (ok (physical-client-test-protocol-error-p
             (lambda () (pg::physical-libpq-decode-connection-info bad)))))))

(deftest physical-libpq-private-option-octets-survive-freezing-and-hex-roundtrip
    (let* ((raw (make-array 255 :element-type '(unsigned-byte 8)
                                :initial-contents (loop for byte from 1 to 255 collect byte)))
           (body (libpq-test-connection-body
                  (list (list "passfile" raw) (list "application_name" (subseq raw 254)))))
           (info (pg::physical-libpq-decode-connection-info body))
           (profile (pg::make-physical-libpq-frozen-profile info))
           (parameters (pg::%physical-libpq-frozen-profile-parameters profile))
           (datum (pg::physical-libpq-frozen-profile-datum profile)))
      (ok (equalp raw (second (assoc "passfile" parameters :test #'equal))))
      (ok (equalp raw (pg::physical-libpq-datum-octets (pg::physical-libpq-octets-datum raw))))
      (ok (search raw (pg::physical-libpq-frozen-profile-body profile)))
      (ok (search "\\xff" (pg::physical-libpq-public-connection-info info)))
      (let ((*read-eval* nil))
        (ok (equal datum (read-from-string (write-to-string datum :readably t)))))
      (ok (not (equal nil (pg::physical-libpq-octets-datum
                           (make-array 0 :element-type '(unsigned-byte 8))))))
      (let ((saved (pg::physical-libpq-frozen-profile-body profile)))
        (setf (aref (second (assoc "passfile"
                                   (pg::physical-libpq-connection-info-parameters info)
                                   :test #'equal)) 0) 99)
        (ok (equalp saved (pg::physical-libpq-frozen-profile-body profile))))))

(deftest physical-libpq-private-byte-datum-rejects-noncanonical-and-oversized-values
    (dolist (datum (list '(:bytes "FF") '(:bytes "f") '(:bytes "xx")
                         '(:bytes "" :extra) '(:wrong "")
                         (list :bytes (make-string 32770 :initial-element #\a))))
      (ok (intake-fails-p (lambda () (pg::physical-libpq-datum-octets datum))))))

(cl-spec:defproperty physical-libpq-private-byte-datums-preserve-unsigned-values
    ((value (range integer 0 #xffffffffffffffff)))
  (:about pg::physical-libpq-octets-datum)
  (:trials (:normal 200))
  (let ((bytes (make-array 8 :element-type '(unsigned-byte 8))))
    (loop for shift downfrom 56 to 0 by 8 for index from 0
          do (setf (aref bytes index) (ldb (byte 8 shift) value)))
    (equalp bytes (pg::physical-libpq-datum-octets
                   (pg::physical-libpq-octets-datum bytes)))))

(deftest generated-physical-libpq-private-byte-datum-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-libpq-private-byte-datums-preserve-unsigned-values
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest physical-libpq-frozen-profile-reload-rejects-malformed-route-and-datum
    (let* ((info (pg::physical-libpq-decode-connection-info
                  (libpq-test-connection-body '(("passfile" "fixture-path")))))
           (profile (pg::make-physical-libpq-frozen-profile info))
           (datum (pg::physical-libpq-frozen-profile-datum profile))
           (reloaded (pg::physical-libpq-frozen-profile-from-datum datum)))
      (ok (equalp (pg::physical-libpq-frozen-profile-body profile)
                  (pg::physical-libpq-frozen-profile-body reloaded)))
      (dolist (bad (list (append datum (list (copy-tree (first datum))))
                         (append datum '(("service" (:bytes "78"))))
                         (remove "hostaddr" datum :key #'first :test #'equal)
                         (cons '("host" (:bytes "00")) (remove "host" datum :key #'first :test #'equal))
                         (cons '("port" (:bytes "30")) (remove "port" datum :key #'first :test #'equal))
                         (cons '("extra" (:bytes "FF")) datum)
                         (cons '("extra" (:bytes "")) :dotted)))
        (ok (intake-fails-p
             (lambda () (pg::physical-libpq-frozen-profile-from-datum bad)))))))

(deftest private-atomic-output-establishes-mode-before-writing
    (with-physical-store-directory (root)
      (let ((target (merge-pathnames "private.control" root))
            (temporary (merge-pathnames "private.tmp" root))
            (observed nil))
        (pg::with-atomic-output (stream target temporary root :mode #o600)
          (setf observed (logand #o777 (sb-posix:stat-mode
                                        (sb-posix:fstat (sb-sys:fd-stream-fd stream)))))
          (write-string "private-fixture" stream))
        (ok (= #o600 observed))
        (ok (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:stat target))))))
      ))

(deftest physical-follow-private-source-and-retirement-survive-control-reload
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (owner (pg::physical-libpq-frozen-profile-datum
                     (pg::make-physical-libpq-frozen-profile
                      (pg::physical-libpq-decode-connection-info
                       (libpq-test-connection-body '(("password" "private-marker")))))))
             (source (list :conninfo (copy-seq "host=localhost password=private-marker")
                           :fallback "receiver" :owner owner))
             (entry (list :kind :follow :host "localhost" :port 5432 :pin "retired"
                          :request (follow-anchor-receiver-request (follow-anchor-request 123 456))
                          :connection owner))
             (state (pg::physical-follow-controller-copy
                     (list :version 1 :enabled t :preparing nil :cleanup (list entry) :source source)))
             (config (list 1 "localhost" 5432 "current" nil state)))
        (pg::physical-follow-save-config config)
        (let* ((loaded (pg::physical-follow-read-config))
               (reloaded-state (pg::physical-follow-controller-state loaded))
               (deduplicated (pg::physical-follow-enqueue-cleanup reloaded-state entry)))
          (ok (equal config loaded))
          (let* ((changed-state (pg::physical-follow-controller-copy reloaded-state))
                 (changed-source (getf changed-state :source)))
            (setf (getf changed-source :fallback) "different-fallback")
            (ok (not (equal (pg::physical-follow-effective-config loaded)
                            (pg::physical-follow-effective-config
                             (pg::physical-follow-with-controller loaded changed-state))))))
          (ok (= 1 (length (getf deduplicated :cleanup))))
          (ok (null (remove entry (getf reloaded-state :cleanup) :test #'equal)))
          (ok (= #o600 (logand #o777 (sb-posix:stat-mode
                                      (sb-posix:stat (pg::physical-follow-config-path))))))
          (setf (char (getf source :conninfo) 0) #\X)
          (ok (equal "host=localhost password=private-marker"
                     (getf (getf (sixth loaded) :source) :conninfo)))))))

(defun libpq-test-source (label port)
  (let ((info (pg::physical-libpq-decode-connection-info
               (libpq-test-connection-body (list (list "password" label))))))
    (setf (pg::physical-libpq-connection-info-port info) port)
    (list :conninfo (format nil "host=localhost port=~d password=~a" port label)
          :fallback label
          :owner (pg::physical-libpq-frozen-profile-datum
                  (pg::make-physical-libpq-frozen-profile info)))))

(deftest physical-follow-first-attachment-keeps-frozen-owner-separate-from-unowned-backup
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (ok (= 42 (pg::physical-store-system-id store)))
       (ok (= 152 pg::*last-replay-lsn*))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((view (list :source 1 :parent 1 :history-text ""
                          :history (pg::timeline-parse-history 1 "")))
              (source (libpq-test-source "first-private-marker" 5433))
              (operation (pg::make-physical-follow-operation
                          :host "127.0.0.1" :port 5433 :prefix "same"
                          :context (list :accepted-source source)))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (before (pg::physical-store-control-tuple store)))
         (pg::physical-follow-save-config
          (pg::physical-follow-unattached-config-for-base
           (pg::physical-recovery-verify-base-state)))
         (pg::physical-follow-operation-initial-config operation)
         (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
           (declare (ignore snapshot))
           (ok (null (getf (sixth (pg::physical-follow-read-config)) :source)))
           (ok (equal (getf source :owner) (getf entry :connection)))
           (let* ((anchor (append (copy-list (getf entry :request))
                                  (list :kind :same-timeline :history-text "")))
                  (active (pg::physical-follow-operation-publish operation view entry anchor))
                  (binding (fifth active))
                  (state (sixth active)))
             (ok (equal source (getf state :source)))
             (ok (null (getf state :mode)))
             (ok (null (getf state :cleanup)))
             (ok (equal (getf state :unowned-bootstrap)
                        (pg::physical-follow-unowned-bootstrap-from-base
                         (pg::physical-recovery-verify-base-state))))
             (ok (equal before (getf binding :before)))
             (ok (equal before (getf binding :after)))
             (ok (equal before (pg::physical-store-control-tuple store)))
             (ok (null (pg::physical-store-selection store)))
             (ok (eq :active (getf binding :phase)))
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))
             (follow-controller-test-advance store)
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))))))
     :applied 152 :system-id 42))

(deftest physical-follow-intent-keeps-candidate-owner-separate-from-current-source
    (let ((old-source (libpq-test-source "old-private-marker" 5432))
          (new-source (libpq-test-source "new-private-marker" 5433)))
      (call-with-controller-intent
       (lambda (root store operation view entry snapshot)
         (declare (ignore root store view snapshot))
         (let* ((saved (pg::physical-follow-read-config))
                (state (pg::physical-follow-controller-state saved)))
           (ok (equal (getf old-source :owner)
                      (getf (getf state :source) :owner)))
           (ok (equal (getf new-source :owner) (getf (getf state :preparing) :connection)))
           (ok (equal entry (getf state :preparing)))
           (setf (pg::physical-follow-operation-cancelled-p operation) t)
           (pg::physical-follow-operation-recover operation)
           (let ((recovered (pg::physical-follow-controller-state (pg::physical-follow-read-config))))
             (ok (equal old-source (getf recovered :source)))
             (ok (equal (getf new-source :owner) (getf (first (getf recovered :cleanup)) :connection))))
           (ok (not (search "new-private-marker" (princ-to-string operation))))))
       :before-intent
       (lambda (operation)
         (setf (getf (pg::physical-follow-operation-context operation) :accepted-source) new-source)
         (pg::physical-follow-save-config
          (pg::physical-follow-with-controller
           '(1 "127.0.0.1" 5432 "old_bootstrap")
           (list :version 1 :enabled t :preparing nil :cleanup nil :source old-source)))))))

(deftest physical-follow-publication-retains-new-owner-and-retires-old-owner
    (let ((old-source (libpq-test-source "old-private-marker" 5432))
          (new-source (libpq-test-source "new-private-marker" 5433)))
      (call-with-controller-intent
       (lambda (root store operation view entry snapshot)
         (declare (ignore root store snapshot))
         (let ((anchor (copy-list (getf entry :request))))
           (setf (getf anchor :history-text) (getf view :history-text)
                 (getf anchor :eor-octets)
                 (coerce (store-history-eor (getf view :fork) 2 1) 'list))
           (let* ((active (pg::physical-follow-operation-publish operation view entry anchor))
                  (state (pg::physical-follow-controller-state active))
                  (retired (first (getf state :cleanup))))
             (ok (equal new-source (getf state :source)))
             (ok (equal (getf old-source :owner) (getf retired :connection)))
             (ok (null (getf state :preparing)))
             (ok (eq :active (getf (fifth active) :phase)))
             (ok (not (search "private-marker" (princ-to-string
                                                (pg::make-physical-follow-runtime :config active)))))
             (ok (not (search "private-marker" (princ-to-string
                                                (pg::physical-follow-controller-status))))))))
       :before-intent
       (lambda (operation)
         (setf (getf (pg::physical-follow-operation-context operation) :accepted-source) new-source)
         (pg::physical-follow-save-config
          (pg::physical-follow-with-controller
           '(1 "127.0.0.1" 5432 "old_bootstrap")
           (list :version 1 :enabled t :preparing nil :cleanup nil :source old-source)))))))

(deftest physical-follow-controller-connect-failure-clears-published-channel
    (let* ((profile (pg::make-physical-libpq-profile "host=fixture password=private-marker"))
           (operation (pg::make-physical-follow-operation
                       :host "fixture" :port 5432 :context (list :requested-profile profile)))
           (channel (pg::physical-libpq-reserve-channel))
           (path-function (symbol-function 'pg::physical-libpq-helper-path))
           (connect-function (symbol-function 'pg::physical-libpq-connect))
           (pg::*physical-follow-controller-blocked-p* nil)
           (pg::*physical-follow-controller-stopping-p* nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-helper-path) (lambda () "/fixture/helper"))
             (setf (symbol-function 'pg::physical-libpq-connect)
                   (lambda (helper requested &key on-spawn)
                     (declare (ignore helper))
                     (ok (eq profile requested))
                     (funcall on-spawn channel)
                     (ok (eq channel (pg::physical-follow-operation-client operation)))
                     (error 'pg::pg-client-error :sqlstate "08001" :message "fixture connection failed")))
             (ok (signals (pg::physical-follow-operation-connect operation) 'pg::pg-client-error))
             (ok (null (pg::physical-follow-operation-client operation)))
             (ok (pg::physical-libpq-channel-closed channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*)))
             (ok (not (search "private-marker" (princ-to-string operation)))))
        (setf (symbol-function 'pg::physical-libpq-helper-path) path-function
              (symbol-function 'pg::physical-libpq-connect) connect-function)
        (pg::physical-client-close channel))))

(deftest physical-follow-runtime-publishes-connect-and-rejects-owner-route-changes
    (dolist (mode '(:same :changed :failure))
      (let* ((source (libpq-test-source "private-marker" 5432))
             (config (pg::physical-follow-with-controller
                      '(1 "localhost" 5432 "current")
                      (list :version 1 :enabled t :preparing nil :cleanup nil :source source)))
             (runtime (pg::make-physical-follow-runtime :host "localhost" :port 5432
                                                        :pin "current" :config config))
             (pg::*physical-follow-runtime* runtime)
             (pg::*physical-follow-statistics* nil)
             (pg::*physical-recovery-state* (list :timeline 1))
             (channel (pg::physical-libpq-reserve-channel))
             (info (pg::physical-libpq-decode-connection-info (libpq-test-connection-body nil)))
             (path-function (symbol-function 'pg::physical-libpq-helper-path))
             (connect-function (symbol-function 'pg::physical-libpq-connect))
             (flush-function (symbol-function 'pg::physical-follow-flush-on-exit))
             (called nil))
        (when (eq mode :changed) (setf (pg::physical-libpq-connection-info-port info) 5433))
        (setf (pg::physical-libpq-channel-info channel) info)
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-libpq-helper-path) (lambda () "/fixture/helper")
                     (symbol-function 'pg::physical-follow-flush-on-exit)
                     (lambda (ignored) (declare (ignore ignored)) nil))
               (setf (symbol-function 'pg::physical-libpq-connect)
                     (lambda (helper profile &key on-spawn)
                       (declare (ignore helper profile))
                       (funcall on-spawn channel)
                       (ok (eq channel (pg::physical-follow-runtime-client runtime)))
                       (when (eq mode :failure)
                         (error 'pg::pg-client-error :sqlstate "08001" :message "fixture connect failed"))
                       channel))
               (flet ((run ()
                        (pg::physical-follow-call-with-client
                         runtime 64
                         (lambda (client)
                           (setf called t)
                           (ok (eq channel client))
                           (pg::physical-follow-monitor-connected runtime)
                           (ok (not (search "private-marker"
                                            (princ-to-string pg::*physical-follow-statistics*))))
                           :connected))))
                 (ecase mode
                   (:same (ok (eq :connected (run))))
                   (:changed (ok (signals (run) 'pg::physical-follow-route-changed)))
                   (:failure (ok (signals (run) 'pg::pg-client-error)))))
               (ok (eq called (eq mode :same)))
               (ok (null (pg::physical-follow-runtime-client runtime)))
               (ok (pg::physical-libpq-channel-closed channel))
               (ok (equal source (pg::physical-follow-config-source
                                  (pg::physical-follow-runtime-config runtime)))))
          (setf (symbol-function 'pg::physical-libpq-helper-path) path-function
                (symbol-function 'pg::physical-libpq-connect) connect-function
                (symbol-function 'pg::physical-follow-flush-on-exit) flush-function)
          (pg::physical-client-close channel)))))

(deftest physical-follow-receiver-retries-libpq-connect-and-stream-errors
    (dolist (sqlstate '("08001" "08006" "08P01"))
      (with-physical-store-directory (root)
        (let* ((store (pg::physical-store-open root 123 1 :create t))
               (pg::*physical-receive-store* store)
               (pg::*physical-recovery-state* (list :end 0))
               (pg::*physical-recovery-failed-p* nil)
               (pg::*shutdown* pg::+no-shutdown+)
               (runtime (pg::make-physical-follow-runtime))
               (original (symbol-function 'pg::physical-follow-session))
               (attempts 0))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::physical-follow-session)
                       (lambda (state cursor)
                         (declare (ignore cursor))
                         (if (= (incf attempts) 1)
                             (error 'pg::pg-client-error :sqlstate sqlstate :message "fixture transport failure")
                             (setf (pg::physical-follow-runtime-stopped-p state) t))))
                 (pg::physical-follow-receiver-loop runtime)
                 (ok (= attempts (if (equal sqlstate "08P01") 1 2)))
                 (ok (not pg::*physical-recovery-failed-p*)))
            (setf (symbol-function 'pg::physical-follow-session) original)
            (pg::physical-store-close store))))))

(deftest physical-follow-controller-does-not-suppress-transport-refresh-retries
    (let ((original (symbol-function 'pg::physical-follow-operation-initial-config)))
      (unwind-protect
           (dolist (sqlstate '("08001" "08006" "08P01"))
             (let* ((operation (pg::make-physical-follow-operation))
                    (pg::*physical-follow-operation* operation)
                    (pg::*physical-follow-auto-error* nil)
                    (pg::*physical-follow-auto-retry-after* 0)
                    (pg::*physical-follow-controller-errors* (make-hash-table :test #'equal))
                    (pg::*physical-recovery-failed-p* nil))
               (setf (symbol-function 'pg::physical-follow-operation-initial-config)
                     (lambda (state)
                       (setf (pg::physical-follow-operation-config state)
                             '(1 "localhost" 5432 "current"))
                       (error 'pg::pg-client-error :sqlstate sqlstate :message "fixture transport failure")))
               (pg::physical-follow-operation-run operation)
               (ok (eq (not (null pg::*physical-follow-auto-error*)) (equal sqlstate "08P01")))
               (ok (plusp pg::*physical-follow-auto-retry-after*))
               (ok (null pg::*physical-follow-operation*))
               (ok (not pg::*physical-recovery-failed-p*))))
        (setf (symbol-function 'pg::physical-follow-operation-initial-config) original))))

(deftest physical-follow-cleanup-keeps-frozen-entry-after-credential-rejection
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*physical-follow-controller-errors* (make-hash-table :test #'equal))
             (pg::*physical-follow-cleanup-retry-after* (make-hash-table :test #'equal))
             (pg::*physical-follow-cleanup-client* nil)
             (source (libpq-test-source "private-marker" 5432))
             (entry (pg::physical-follow-cleanup-with-owner
                     (list :kind :follow :host "localhost" :port 5432 :pin "retired"
                           :request (follow-anchor-receiver-request (follow-anchor-request 123 456)))
                     source))
             (state (list :version 1 :enabled nil :preparing nil :cleanup (list entry)))
             (config (list 1 nil nil nil nil state))
             (connect-function (symbol-function 'pg::physical-follow-cleanup-connect)))
        (pg::physical-follow-save-config config)
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-follow-cleanup-connect)
                     (lambda (requested)
                       (ok (equal entry requested))
                       (error 'pg::pg-client-error :sqlstate "08001"
                                                   :message "fixture credential rejected")))
               (pg::physical-follow-controller-cleanup entry)
               (ok (equal config (pg::physical-follow-read-config)))
               (ok (gethash entry pg::*physical-follow-controller-errors*))
               (ok (plusp (gethash entry pg::*physical-follow-cleanup-retry-after*)))
               (ok (not (search "private-marker"
                                (princ-to-string (pg::physical-follow-controller-status))))))
          (setf (symbol-function 'pg::physical-follow-cleanup-connect) connect-function)))))

(defun libpq-test-result-body (&key (name "systemid") (oid 25) (format 0) (value "42")
                                 (tag "IDENTIFY_SYSTEM") (status 0))
  (let ((payload (pg::make-payload)))
    (vector-push-extend status payload)
    (libpq-test-string payload tag)
    (pg::payload-push-u32 payload 1)
    (pg::payload-push-u32 payload 1)
    (libpq-test-string payload name)
    (dolist (number (list 0 0 oid -1 -1 format))
      (pg::payload-push-u32 payload (mod number #x100000000)))
    (if (eq value pg::+null+)
        (pg::payload-push-u32 payload #xffffffff)
        (libpq-test-string payload value))
    payload))

(deftest physical-libpq-result-retains-typed-single-row-validation
    (let ((result (pg::physical-libpq-decode-result #\T (libpq-test-result-body))))
      (ok (equalp #("42") (pg::physical-libpq-check-result result '(("systemid" 25)) "IDENTIFY_SYSTEM"))))
  (dolist (body (list (libpq-test-result-body :name "wrong")
                      (libpq-test-result-body :oid 20)
                      (libpq-test-result-body :format 1)
                      (libpq-test-result-body :tag "wrong")
                      (libpq-test-result-body :value pg::+null+)
                      (libpq-test-result-body :status 2)))
    (ok (intake-fails-p
         (lambda ()
           (pg::physical-libpq-check-result
            (pg::physical-libpq-decode-result #\T body) '(("systemid" 25)) "IDENTIFY_SYSTEM"))))))

(deftest physical-libpq-result-nullability-is-explicit-and-normalized
    (let ((result (pg::physical-libpq-decode-result
                   #\T (libpq-test-result-body :value pg::+null+))))
      (ok
       (equalp #(nil)
               (pg::physical-libpq-check-result
                result '(("systemid" 25 :nullable)) "IDENTIFY_SYSTEM")))
      (ok
       (physical-client-test-protocol-error-p
        (lambda ()
          (pg::physical-libpq-check-result
           result '(("systemid" 25)) "IDENTIFY_SYSTEM"))))))

(deftest physical-libpq-result-rejects-truncation-and-trailing-bytes
    (let ((body (libpq-test-result-body)))
      (dolist (bad (list (subseq body 0 (1- (length body)))
                         (concatenate '(vector (unsigned-byte 8)) body #(0))))
        (ok (intake-fails-p (lambda () (pg::physical-libpq-decode-result #\T bad)))))))

(defun call-with-libpq-test-pipe (function)
  (let* ((process (sb-ext:run-program "/bin/cat" nil :use-posix-spawn t :wait nil
                                                     :input :stream :output :stream :error :stream))
         (channel (pg::%make-physical-libpq-channel :process process :session 42)))
    (unwind-protect
         (sb-ext:with-timeout 5 (funcall function channel))
      (close (sb-ext:process-input process))
      (sb-ext:process-wait process)
      (sb-ext:process-close process))))

(deftest physical-libpq-pipe-preserves-binary-bodies
    (call-with-libpq-test-pipe
     (lambda (channel)
       (let* ((body (make-array 256 :element-type '(unsigned-byte 8)
                                    :initial-contents (loop for byte below 256 collect byte)))
              (request (pg::physical-libpq-write-request channel #\D body)))
         (multiple-value-bind (kind received)
             (pg::physical-libpq-read-reply channel request '(#\D) 256)
           (ok (char= kind #\D))
           (ok (equalp body received)))))))

(deftest physical-libpq-pipe-rejects-body-before-reading-it
    (call-with-libpq-test-pipe
     (lambda (channel)
       (let ((stream (sb-ext:process-input (pg::physical-libpq-channel-process channel))))
         (write-sequence (pg::physical-libpq-header 42 0 #\D 1024) stream)
         (finish-output stream)
         (ok (intake-fails-p
              (lambda () (pg::physical-libpq-read-reply channel 0 '(#\D) 10))))))))

(deftest physical-libpq-pipe-preserves-checked-error-sqlstate
    (call-with-libpq-test-pipe
     (lambda (channel)
       (let ((body (pg::make-payload)))
         (loop for byte across (pg::string->bytes "28P01")
               do (vector-push-extend byte body))
         (libpq-test-string body "libpq connection failed")
         (let ((request (pg::physical-libpq-write-request channel #\! body)))
           (handler-case
               (progn (pg::physical-libpq-read-reply channel request '(#\O) 0)
                      (fail "expected checked helper error"))
             (pg::pg-client-error (condition)
               (ok (equal "28P01" (pg::pg-client-error-sqlstate condition)))
               (ok (equal "libpq connection failed" (pg::pg-client-error-message condition))))))))))

(defun call-with-owned-libpq-test-process (program arguments function)
  (let ((channel (pg::physical-libpq-reserve-channel)))
    (unwind-protect
         (progn
           (setf (pg::physical-libpq-channel-process channel)
                 (sb-ext:run-program program arguments :use-posix-spawn t :wait nil
                                                       :input :stream :output :stream :error :stream))
           (let ((fd (pg::physical-libpq-pidfd-open
                      (sb-ext:process-pid (pg::physical-libpq-channel-process channel)) 0)))
             (assert (not (minusp fd)))
             (setf (pg::physical-libpq-channel-pidfd channel) fd))
           (funcall function channel))
      (pg::physical-libpq-close channel))))

(deftest physical-libpq-terminal-child-does-not-acquire-reused-pid
    (let ((channel (pg::physical-libpq-reserve-channel))
          (original (symbol-function 'pg::physical-libpq-pidfd-open))
          (calls 0))
      (unwind-protect
           (progn
             (setf (pg::physical-libpq-channel-process channel)
                   (sb-ext:run-program "/bin/true" nil :wait t))
             (setf (symbol-function 'pg::physical-libpq-pidfd-open)
                   (lambda (pid flags)
                     (declare (ignore pid flags))
                     (incf calls)
                     -1))
             (ok (intake-fails-p
                  (lambda () (pg::physical-libpq-acquire-process-handle channel))))
             (ok (zerop calls))
             (pg::physical-libpq-close channel)
             (ok (pg::physical-libpq-channel-closed channel)))
        (setf (symbol-function 'pg::physical-libpq-pidfd-open) original)
        (pg::physical-libpq-close channel))))

(deftest physical-libpq-pidfd-failure-kills-owned-child-before-reaping
    (let ((channel (pg::physical-libpq-reserve-channel))
          (original (symbol-function 'pg::physical-libpq-pidfd-open)))
      (unwind-protect
           (progn
             (setf (pg::physical-libpq-channel-process channel)
                   (sb-ext:run-program "/bin/sleep" '("30") :use-posix-spawn t
                                                            :wait nil :input :stream :output :stream :error :stream))
             (setf (symbol-function 'pg::physical-libpq-pidfd-open)
                   (lambda (pid flags) (declare (ignore pid flags)) -1))
             (ok (intake-fails-p
                  (lambda () (pg::physical-libpq-acquire-process-handle channel))))
             (pg::physical-libpq-close channel)
             (ok (member (sb-ext:process-status (pg::physical-libpq-channel-process channel))
                         '(:exited :signaled)))
             (ok (pg::physical-libpq-channel-closed channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*))))
        (setf (symbol-function 'pg::physical-libpq-pidfd-open) original)
        (pg::physical-libpq-close channel))))

(deftest physical-libpq-capacity-is-returned-only-after-close
    (let ((first (pg::physical-libpq-reserve-channel))
          (second nil) (third nil))
      (unwind-protect
           (progn
             (setf second (pg::physical-libpq-reserve-channel)
                   third (pg::physical-libpq-reserve-channel))
             (pg::physical-libpq-stop third)
             (ok (intake-fails-p #'pg::physical-libpq-reserve-channel))
             (pg::physical-libpq-close second)
             (setf second (pg::physical-libpq-reserve-channel))
             (ok second))
        (when third (pg::physical-libpq-close third))
        (when second (pg::physical-libpq-close second))
        (pg::physical-libpq-close first))))

(deftest physical-libpq-admits-receiver-cleanup-and-transition-concurrently
    (let ((ready (sb-thread:make-semaphore :count 0))
          (release (sb-thread:make-semaphore :count 0))
          (channels (make-array 3 :initial-element nil))
          (errors (make-array 3 :initial-element nil))
          (threads nil))
      (unwind-protect
           (progn
             (dotimes (index 3)
               (let ((slot index))
                 (push (sb-thread:make-thread
                        (lambda ()
                          (let ((channel nil))
                            (unwind-protect
                                 (handler-case
                                     (progn
                                       (setf channel (pg::physical-libpq-reserve-channel)
                                             (aref channels slot) channel)
                                       (sb-thread:signal-semaphore ready)
                                       (sb-thread:wait-on-semaphore release))
                                   (error (condition)
                                     (setf (aref errors slot) condition)
                                     (sb-thread:signal-semaphore ready)))
                              (when channel (pg::physical-libpq-close channel)))))
                        :name (nth slot '("test receiver" "test cleanup" "test transition")))
                       threads)))
             (dotimes (index 3)
               (ok (sb-thread:wait-on-semaphore ready :timeout 5)))
             (ok (every #'null errors))
             (ok (every #'identity channels))
             (ok (intake-fails-p #'pg::physical-libpq-reserve-channel)))
        (sb-thread:signal-semaphore release 3)
        (dolist (thread threads) (sb-thread:join-thread thread)))))

(deftest physical-libpq-deadline-kills-and-reaps-owned-helper
    (call-with-owned-libpq-test-process
     "/bin/sleep" '("30")
     (lambda (channel)
       (let ((timed-out nil))
         (handler-case
             (pg::physical-libpq-exchange channel #\X (pg::make-payload) '(#\A) 0 :timeout 0.05)
           (sb-ext:timeout () (setf timed-out t)))
         (ok timed-out)
         (ok (member (sb-ext:process-status (pg::physical-libpq-channel-process channel))
                     '(:exited :signaled)))
         (ok (null (pg::physical-libpq-channel-pidfd channel)))
         (ok (not (gethash channel pg::*physical-libpq-channels*)))))))

(deftest physical-libpq-controller-can-stop-but-cannot-own-io
    (call-with-owned-libpq-test-process
     "/bin/cat" nil
     (lambda (channel)
       (let ((rejected nil))
         (sb-thread:join-thread
          (sb-thread:make-thread
           (lambda ()
             (setf rejected (intake-fails-p
                             (lambda () (pg::physical-libpq-require-owner channel))))
             (pg::physical-libpq-stop channel))))
         (ok rejected)
         (ok (pg::physical-libpq-channel-stopped channel))))))

(deftest physical-libpq-stopped-pipe-is-cancellation-but-unsolicited-eof-is-protocol-error
    (dolist (stopped '(nil t))
      (call-with-owned-libpq-test-process
       "/bin/cat" nil
       (lambda (channel)
         (if stopped
             (pg::physical-libpq-stop channel)
             (sb-ext:process-kill (pg::physical-libpq-channel-process channel) 9))
         (let ((condition
                 (handler-case
                     (pg::with-physical-libpq-operation (channel)
                       (pg::physical-libpq-read-reply channel 0 '(#\D) 1))
                   (error (condition) condition))))
           (if stopped
               (ok (typep condition 'pg::physical-client-stopped))
               (progn
                 (ok (typep condition 'pg::pg-client-error))
                 (when (typep condition 'pg::pg-client-error)
                   (ok (equal "08P01" (pg::pg-client-error-sqlstate condition))))))
           (ok (pg::physical-libpq-channel-closed channel)))))))

(deftest physical-libpq-stopped-write-is-cancellation
    (call-with-owned-libpq-test-process
     "/bin/cat" nil
     (lambda (channel)
       (pg::physical-libpq-stop channel)
       (ok (signals (pg::physical-libpq-write-request channel #\Q #())
                    'pg::physical-client-stopped)))))

(deftest physical-follow-controller-stop-is-expected-only-for-cancelled-operation
    (dolist (cancelled '(nil t))
      (dolist (committed '(nil t))
        (let* ((operation (pg::make-physical-follow-operation
                           :cancelled-p cancelled :context (list :committed committed)))
               (pg::*physical-follow-operation* operation)
               (pg::*physical-follow-auto-error* nil)
               (pg::*physical-follow-auto-retry-after* 0)
               (pg::*physical-follow-controller-errors* (make-hash-table :test #'equal))
               (pg::*physical-recovery-failed-p* nil)
               (original (symbol-function 'pg::physical-follow-operation-initial-config)))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::physical-follow-operation-initial-config)
                       (lambda (ignored) (declare (ignore ignored))
                         (error 'pg::physical-client-stopped)))
                 (pg::physical-follow-operation-run operation)
                 (ok (eql cancelled (null pg::*physical-follow-auto-error*)))
                 (ok (eql (and committed (not cancelled)) pg::*physical-recovery-failed-p*))
                 (ok (null pg::*physical-follow-operation*)))
            (setf (symbol-function 'pg::physical-follow-operation-initial-config) original))))))

(deftest physical-client-transport-receives-full-native-fragment-and-completion
    (let ((payload (make-array (+ 25 65536) :element-type '(unsigned-byte 8) :initial-element 0)))
      (setf (aref payload 0) (char-code #\w))
      (call-with-physical-client-wire
       (lambda (client request)
         (declare (ignore request))
         (multiple-value-bind (kind bytes) (pg::physical-client-receive client)
           (ok (eq kind :data))
           (ok (equalp payload bytes)))
         (multiple-value-bind (kind bytes) (pg::physical-client-receive client)
           (ok (eq kind :done))
           (ok (null bytes))))
       (lambda (out)
         (pg::write-message out #\d payload)
         (pg::write-message out #\c (pg::make-payload))))))

(deftest physical-client-transport-rejects-oversized-copy-before-body
    (call-with-physical-client-wire
     (lambda (client request)
       (declare (ignore request))
       (ok (intake-fails-p (lambda () (pg::physical-client-receive client)))))
     (lambda (out)
       (write-byte (char-code #\d) out)
       (write-sequence (pg::u32-to-bytes (+ 5 25 65536)) out))))

(deftest physical-libpq-native-prepare-reuses-source-proof-validation
    (let* ((expected (follow-anchor-request 123 456))
           (coordinates (follow-anchor-receiver-request expected))
           (channel (pg::physical-libpq-reserve-channel))
           (original (symbol-function 'pg::physical-libpq-query))
           (changed nil)
           (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-query)
                   (lambda (client query schema tag &key maximum command-only)
                     (ok (eq client channel))
                     (ok (search "PREPARE_FOLLOW SLOT bound" query))
                     (ok (equal schema (physical-client-test-prepare-schema)))
                     (ok (equal tag "PREPARE_FOLLOW"))
                     (ok (= maximum pg::+physical-client-proof-frame-maximum+))
                     (ok (not command-only))
                     (incf calls)
                     (let ((row (coerce (physical-client-test-prepare-row) 'vector)))
                       (when changed (setf (aref row 8) "124"))
                       row)))
             (ok (equal expected (pg::pg-client-native-prepare channel "bound" coordinates)))
             (setf changed t)
             (ok (physical-client-test-protocol-error-p
                  (lambda () (pg::pg-client-native-prepare channel "bound" coordinates))))
             (ok (= calls 2)))
        (setf (symbol-function 'pg::physical-libpq-query) original)
        (pg::physical-libpq-close channel))))

(deftest physical-libpq-query-validation-failure-closes-channel
    (let ((original (symbol-function 'pg::physical-libpq-exchange))
          (channel (pg::physical-libpq-reserve-channel)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-exchange)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (values #\T (libpq-test-result-body :name "wrong"))))
             (ok (intake-fails-p
                  (lambda ()
                    (pg::physical-libpq-query channel "IDENTIFY_SYSTEM"
                                              '(("systemid" 25)) "IDENTIFY_SYSTEM"))))
             (ok (pg::physical-libpq-channel-stopped channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*))))
        (setf (symbol-function 'pg::physical-libpq-exchange) original)
        (pg::physical-libpq-close channel))))

(defun libpq-test-switch-body ()
  (let ((body (pg::make-payload)))
    (vector-push-extend 0 body)
    (libpq-test-string body "START_STREAMING")
    (pg::payload-push-u32 body 2)
    (pg::payload-push-u32 body 1)
    (dolist (column '(("next_tli" 20 8) ("next_tli_startpos" 25 -1)))
      (libpq-test-string body (first column))
      (dolist (number (list 0 0 (second column) (third column) -1 0))
        (pg::payload-push-u32 body (mod number #x100000000))))
    (libpq-test-string body "2")
    (libpq-test-string body "0/400")
    body))

(deftest physical-libpq-finish-requires-prepared-child-and-fork
    (let ((original (symbol-function 'pg::physical-libpq-exchange))
          (channel (pg::physical-libpq-reserve-channel)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-exchange)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (values #\T (libpq-test-switch-body))))
             (ok (equal (list :switch 2 1024) (pg::physical-libpq-finish channel 2 1024)))
             (ok (intake-fails-p
                  (lambda () (pg::physical-libpq-finish channel 3 1024))))
             (ok (pg::physical-libpq-channel-stopped channel))
             (ok (not (gethash channel pg::*physical-libpq-channels*))))
        (setf (symbol-function 'pg::physical-libpq-exchange) original)
        (pg::physical-libpq-close channel))))
