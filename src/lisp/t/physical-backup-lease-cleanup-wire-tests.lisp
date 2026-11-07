(in-package #:pg-physical-tests)

(cl-spec:defproperty backup-lease-cleanup-wire-preserves-exact-owner
    ((digest (range integer 0 18446744073709551615))
     (identity-choice (range integer 0 2)))
  (:about pg::parse-native-backup-lease-command pg::pg-client-native-backup-lease-query)
  (:trials (:normal 100))
  (let* ((request (backup-lease-wire-request
                   (nth identity-choice '("cleanup" "cleanup'quoted" "cleanup\\literal"))))
         (successor (list :name "cleanup_selected_wire" :anchor-digest digest))
         (query (pg::pg-client-native-backup-lease-query
                 "BACKUP_LEASE_CLEANUP_SELECTED" request successor))
         (command (pg::parse-replication-command query)))
    (and (eq :physical-lease-cleanup-selected (pg::logical-replication-command-type command))
         (equal request (pg::physical-backup-lease-command-request command))
         (equal successor
                (cdr (assoc "successor" (pg::logical-replication-command-options command) :test #'equal)))
         (null (assoc "allocation" (pg::logical-replication-command-options command) :test #'equal))
         (pg::replication-query-payload-p (pg::string->bytes query)))))

(deftest generated-backup-lease-cleanup-wire-owner-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-cleanup-wire-preserves-exact-owner
                                         :seed 197 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-cleanup-wire-rejects-ambiguous-options-and-dispatch-prefixes
    (let* ((request (backup-lease-wire-request))
           (successor (list :name "cleanup_selected_wire" :anchor-digest 42))
           (query (pg::pg-client-native-backup-lease-query
                   "BACKUP_LEASE_CLEANUP_SELECTED" request successor)))
      (dolist (replacement
               '(("successor_name 'cleanup_selected_wire', " "")
                 (", successor_anchor_digest '42'" "")
                 ("successor_anchor_digest '42'" "successor_anchor_digest '18446744073709551616'")
                 ("successor_name 'cleanup_selected_wire'" "successor_name cleanup_selected_wire")
                 ("successor_anchor_digest '42'" "successor_anchor_digest 42")
                 ("successor_anchor_digest '42'" "successor_anchor_digest '42', successor_anchor_digest '42'")
                 ("successor_anchor_digest '42'" "successor_anchor_digest '42', allocation_generation '1'")
                 ("successor_anchor_digest '42'" "successor_anchor_digest '42', follow_base '0/98'")
                 ("successor_anchor_digest '42'" "successor_anchor_digest '42', unexpected 'value'")))
        (ok (handler-case
                (progn (pg::parse-replication-command
                        (backup-lease-wire-replace query (first replacement) (second replacement))) nil)
              (pg::pg-error () t))))
      (ok (pg::replication-query-payload-p
           (pg::string->bytes (format nil "~c~c~a" #\Tab #\Newline (string-downcase query)))))
      (dolist (text '("BACKUP_LEASE_CLEANUP_SELECTED_extra ()"
                      "/* leading */ BACKUP_LEASE_CLEANUP_SELECTED ()"
                      "-- leading comment"))
        (ok (not (pg::replication-query-payload-p (pg::string->bytes text)))))))

(deftest backup-lease-cleanup-client-requires-exact-closed-handoff
    (let* ((request (backup-lease-wire-request))
           (closed (backup-lease-wire-record 7 request))
           (successor (getf closed :successor)))
      (call-with-physical-client-wire
       (lambda (client request-file)
         (declare (ignore request-file))
         (ok (equal closed
                    (pg::pg-client-native-backup-lease-cleanup-selected client request successor))))
       (lambda (output)
         (physical-client-test-result output pg::*physical-backup-lease-result-schema*
                                      (list (pg::physical-backup-lease-result-row request closed))
                                      "BACKUP_LEASE_CLEANUP_SELECTED")))
      (dolist (fault '(:unknown :open :closing :aborted :successor :owner))
        (let ((record (pg::physical-backup-lease-copy closed)))
          (case fault
            (:unknown (setf record nil))
            (:open (setf record (backup-lease-wire-record 3 request)))
            (:closing (setf (getf record :phase) :closing))
            (:aborted (setf record (backup-lease-wire-record 6 request)))
            (:successor (incf (getf (getf record :successor) :anchor-digest))))
          (call-with-physical-client-wire
           (lambda (client request-file)
             (declare (ignore request-file))
             (ok (physical-client-test-protocol-error-p
                  (lambda ()
                    (pg::pg-client-native-backup-lease-cleanup-selected client request successor)))))
           (lambda (output)
             (let ((row (pg::physical-backup-lease-result-row request record)))
               (when (eq fault :owner) (setf (first row) "foreign_lease"))
               (physical-client-test-result output pg::*physical-backup-lease-result-schema*
                                            (list row) "BACKUP_LEASE_CLEANUP_SELECTED"))))))))

(deftest backup-lease-cleanup-wire-preserves-admission-and-recovery-guards
    (let* ((pg::*wal-sender-registry* (make-hash-table :test #'eq))
           (pg::*sync-replication-state* (pg::make-sync-replication-state :source-id 42))
           (pg::*physical-recovery-failed-p* nil)
           (pg::*backend* (pg::make-backend :pid 183 :replication-mode :physical))
           (command (pg::parse-replication-command
                     (pg::pg-client-native-backup-lease-query
                      "BACKUP_LEASE_CLEANUP_SELECTED" (backup-lease-wire-request)
                      (list :name "wire_successor" :anchor-digest 37)))))
      (pg::wal-sender-register pg::*backend*)
      (unwind-protect
           (progn
             (dolist (mode '(:database nil))
               (setf (pg::backend-replication-mode pg::*backend*) mode)
               (ok (equal "55000"
                          (handler-case
                              (progn (pg::physical-backup-lease-wire-preflight command) nil)
                            (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))
             (setf (pg::backend-replication-mode pg::*backend*) :physical)
             (dolist (state '(:transaction :aborted :standby :primary))
               (let ((pg::*physical-recovery-state* (when (eq state :standby) (list :phase 2))))
                 (setf (pg::backend-explicit pg::*backend*) (eq state :transaction)
                       (pg::backend-state pg::*backend*) (if (eq state :aborted) :aborted :idle))
                 (dolist (check (list #'pg::physical-backup-lease-wire-preflight
                                      #'pg::validate-replication-command-context))
                   (ok (equal (case state (:transaction "25001") (:aborted "25P02"))
                              (handler-case (progn (funcall check command) nil)
                                (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))))
             (dolist (state '(:failed :pending-promotion))
               (let ((pg::*physical-recovery-failed-p* (eq state :failed))
                     (pg::*physical-recovery-state*
                       (list :phase (if (eq state :pending-promotion) 3 2))))
                 (ok (equal "57P03"
                            (handler-case
                                (progn (pg::physical-backup-lease-wire-preflight command) nil)
                              (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))))
             (pg::wal-sender-unregister pg::*backend*)
             (ok (equal "55000"
                        (handler-case
                            (progn (pg::physical-backup-lease-wire-preflight command) nil)
                          (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))
        (pg::wal-sender-unregister pg::*backend*))))

(deftest backup-lease-cleanup-wire-frame-closes-unavailable-or-absent-selection
    (dolist (allocated '(nil t))
      (dolist (standby '(nil t))
        (dolist (absent '(nil t))
          (call-with-backup-lease-selected-observation-fixture
           (lambda (root parent pin row)
             (declare (ignore root row))
             (let* ((request (getf parent :request))
                    (successor (getf parent :successor))
                    (name (getf successor :name))
                    (path (pg::physical-source-pin-path name))
                    (query (pg::pg-client-native-backup-lease-query
                            "BACKUP_LEASE_CLEANUP_SELECTED" request successor))
                    (payload (concatenate '(vector (unsigned-byte 8)) (pg::string->bytes query) #(0)))
                    (pg::*wal-sender-registry* (make-hash-table :test #'eq))
                    (pg::*sync-replication-state* (pg::make-sync-replication-state :source-id 42)))
               (pg::with-write-lock
                 (sb-thread:with-mutex (pg::*wal-lock*)
                   (if absent
                       (progn
                         (delete-file path)
                         (pg::fsync-directory (pg::physical-source-pin-directory))
                         (remhash name pg::*physical-source-pins*)
                         (remhash pin pg::*wal-snapshot-retention-pins*))
                       (progn
                         (setf (pg::physical-source-pin-lost-p pin) t)
                         (pg::physical-source-pin-save pin)))
                   (delete-file (pg::wal-segment-path 0))))
               (setf (pg::backend-replication-mode pg::*backend*) :physical)
               (pg::wal-sender-register pg::*backend*)
               (unwind-protect
                    (let ((pg::*physical-recovery-state* (when standby (list :phase 2)))
                          (pg::*physical-source-pins*
                            (if standby (make-hash-table :test #'equal) pg::*physical-source-pins*))
                          (pg::*wal-snapshot-retention-pins*
                            (if standby (make-hash-table :test #'eq) pg::*wal-snapshot-retention-pins*)))
                      (dotimes (attempt 2)
                        (declare (ignorable attempt))
                        (call-with-physical-client-wire
                         (lambda (client request-file)
                           (declare (ignore request-file))
                           (let ((receipt
                                   (pg::pg-client-native-backup-lease-cleanup-selected
                                    client request successor)))
                             (ok (eq :closed (getf receipt :phase)))
                             (ok (eq :handed-off (getf receipt :outcome)))
                             (ok (equal successor (getf receipt :successor)))))
                         (lambda (output)
                           (pg::dispatch-frontend-frame output (char-code #\Q)
                                                        (pg::u32-to-bytes (+ 4 (length payload))) payload))))
                      (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
                      (ok (eq absent (not (probe-file path))))
                      (ok (not pg::*wal-poisoned*)))
                 (pg::wal-sender-unregister pg::*backend*))))
           allocated t)))))

(defun cleanup-wire-native-control (process socket driver form)
  (let ((response (reseed-main-native-control process socket driver form)))
    (when (search ";; read error:" response :test #'char-equal)
      (error "owned cleanup source rejected the control form"))
    response))

(defun cleanup-wire-native-await-primary (process root socket driver)
  (loop with deadline = (+ (get-internal-real-time) (* 180 internal-time-units-per-second))
        do (unless (eq :running (sb-ext:process-status process))
             (error "owned cleanup source ended during startup"))
           (when (probe-file socket)
             (let ((response
                     (cleanup-wire-native-control
                      process socket driver
                      `(and (= (sb-posix:getpid) ,(sb-ext:process-pid process))
                            (equal (truename pg::*pgdata*) (truename ,root))
                            (pg::postmaster-pid-owned-p)
                            pg::*wal-ready* (not (pg::recovery-in-progress-p))
                            (member "ready   "
                                    (uiop:split-string
                                     (sb-ext:octets-to-string
                                      (getf (pg::physical-reseed-read-pid pg::*pgdata*) :bytes)
                                      :external-format :utf-8)
                                     :separator '(#\Newline))
                                    :test #'string=)
                            :cleanup-wire-primary-ready))))
               (when (search ":CLEANUP-WIRE-PRIMARY-READY" response :test #'char-equal)
                 (return t))))
           (when (>= (get-internal-real-time) deadline)
             (error "owned cleanup source did not become ready"))
           (sleep 0.1)))

(defun call-with-cleanup-wire-native-source (root driver function)
  (let* ((tag (symbol-name (gensym "CLEANUP-")))
         (socket (merge-pathnames (concatenate 'string tag ".sock") driver))
         (cache (merge-pathnames (concatenate 'string tag "-cache/") driver))
         (log (merge-pathnames (concatenate 'string tag ".log") driver))
         (port (reseed-main-native-port))
         (process nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process
                   (sb-ext:run-program
                    (namestring sb-ext:*runtime-pathname*)
                    (reseed-main-native-arguments root socket port cache nil)
                    :wait nil :output log :error :output :if-output-exists :error)))
           (format t "~%CLEANUP-WIRE-NATIVE-OWNED ~d ~s ~d ~s~%"
                   (sb-ext:process-pid process) root port socket)
           (finish-output)
           (cleanup-wire-native-await-primary process root socket driver)
           (funcall function process socket port))
      (reseed-main-native-dispose process)
      (when (probe-file log)
        (format t "~%CLEANUP-WIRE-NATIVE-LOG-BEGIN~%~a~%CLEANUP-WIRE-NATIVE-LOG-END~%"
                (uiop:read-file-string log))
        (finish-output)))))

(defun call-with-cleanup-wire-native-client (port function)
  (let ((channel nil))
    (unwind-protect
         (progn
           (pg::physical-libpq-connect
            (pg::physical-libpq-helper-path)
            (pg::make-physical-libpq-profile
             (format nil "host=127.0.0.1 port=~d user=postgres dbname=replication connect_timeout=5 sslmode=disable" port)
             :mode :physical)
            :on-spawn (lambda (owned) (setf channel owned)))
           (funcall function channel))
      (when channel (pg::physical-libpq-close channel)))))

(defun cleanup-wire-native-coordinates (stage manifest parent)
  (let* ((pg::*pgdata* stage)
         (base (getf manifest :end))
         (timeline (getf manifest :timeline))
         (namespace (nth-value 5 (pg::wal-read-control-fields)))
         (segment (floor (1- base) pg::+wal-segment-size+))
         (start (* segment pg::+wal-segment-size+))
         (prefix (make-array (- base start) :element-type '(unsigned-byte 8))))
    (with-open-file (input (pg::wal-segment-path segment timeline namespace)
                           :element-type '(unsigned-byte 8))
      (assert (= (length prefix) (read-sequence prefix input))))
    (pg::physical-follow-request-copy
     (list :version 1 :system-id (getf manifest :system-id)
           :base-timeline timeline :base base
           :manifest-digest (getf (getf parent :base) :manifest-digest)
           :source-timeline timeline :receive-timeline timeline :receive-lsn base
           :receive-digest #xCBF29CE484222325 :overlap-start start
           :overlap-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix)))))

(deftest backup-lease-cleanup-native-capture-prepares-real-successor
    (dolist (allocated '(nil t))
      (dolist (absent '(nil t))
        (with-physical-store-directory (root)
          (sb-posix:chmod (namestring root) #o700)
          (with-physical-store-directory (driver)
            (sb-posix:chmod (namestring driver) #o700)
            (let ((request nil) (successor nil) (selected-bytes nil))
              (format t "~%CLEANUP-WIRE-NATIVE-CASE ~s ~s ~s~%" root allocated absent)
              (finish-output)
              (call-with-cleanup-wire-native-source
               root driver
               (lambda (process socket port)
                 (let ((request-path (merge-pathnames "request.data" driver)))
                   (cleanup-wire-native-control
                    process socket driver
                    `(progn
                       (with-open-file (pg::cleanup-wire-output ,request-path :direction :output :if-exists :error)
                         (write (list :version 1 :name "reseed_0123456789abcdef0123456789abcdef"
                                      :token (make-string 64 :initial-element #\a)
                                      :system-id (pg::catalog-system-identifier)
                                      :cluster-identity (pg::catalog-cluster-identity pg::*catalog*)
                                      :timeline pg::*wal-insert-timeline*)
                                :stream pg::cleanup-wire-output :readably t))
                       (values)))
                   (setf request (with-open-file (input request-path)
                                   (let ((*read-eval* nil)) (read input)))))
                 (with-physical-store-directory (stage)
                   (call-with-cleanup-wire-native-client
                    port
                    (lambda (client)
                      (multiple-value-bind (manifest parent)
                          (let ((pg::*pgdata* root))
                            (pg::physical-backup-lease-capture-remote stage client request))
                        (ok (eq :ready (getf parent :phase)))
                        (let* ((coordinates (cleanup-wire-native-coordinates stage manifest parent))
                               (anchor
                                 (if allocated
                                     (pg::pg-client-native-backup-lease-follow-prepare
                                      client request 1 "cleanup_native_selected" coordinates)
                                     (pg::pg-client-native-prepare client "cleanup_native_selected" coordinates))))
                          (ok (= (getf anchor :base) (getf manifest :end)))
                          (setf successor (list :name "cleanup_native_selected"
                                                :anchor-digest (pg::physical-follow-anchor-digest anchor)))
                          (cleanup-wire-native-control
                           process socket driver
                           '(let ((pg::saved-finish
                                   (symbol-function 'pg::physical-backup-lease-finish-closing-under-lock)))
                             (setf (symbol-function 'pg::physical-backup-lease-finish-closing-under-lock)
                              (lambda (pg::cleanup-request pg::begin-publication &optional pg::authority)
                                (declare (ignore pg::begin-publication pg::authority))
                                (setf (symbol-function 'pg::physical-backup-lease-finish-closing-under-lock)
                                      pg::saved-finish)
                                (pg::physical-backup-lease-copy
                                 (pg::physical-backup-lease-current-record pg::cleanup-request))))
                             (values)))
                          (ok (handler-case
                                  (progn (pg::pg-client-native-backup-lease-handoff client request successor) nil)
                                (pg::pg-client-error () t))))))))
                 (cleanup-wire-native-control
                  process socket driver
                  `(progn
                     (pg::with-write-lock
                       (sb-thread:with-mutex (pg::*wal-lock*)
                         (assert (eq :closing
                                     (getf (pg::physical-backup-lease-current-record ',request) :phase)))
                         (let ((pg::selected-pin (gethash "cleanup_native_selected" pg::*physical-source-pins*)))
                           (assert pg::selected-pin)
                           ,(if absent
                                '(progn
                                  (delete-file (pg::physical-source-pin-path "cleanup_native_selected"))
                                  (pg::fsync-directory (pg::physical-source-pin-directory))
                                  (remhash "cleanup_native_selected" pg::*physical-source-pins*))
                                '(progn
                                  (setf (pg::physical-source-pin-lost-p pg::selected-pin) t)
                                  (pg::physical-source-pin-save pg::selected-pin)))
                           (remhash pg::selected-pin pg::*wal-snapshot-retention-pins*))))
                     (values)))
                 (let* ((pg::*pgdata* root)
                        (path (pg::physical-source-pin-path "cleanup_native_selected")))
                   (setf selected-bytes (unless absent (lineage-test-bytes path))))
                 (call-with-cleanup-wire-native-client
                  port
                  (lambda (client)
                    (dotimes (retry 2)
                      (declare (ignorable retry))
                      (let ((closed (pg::pg-client-native-backup-lease-cleanup-selected client request successor)))
                        (ok (eq :closed (getf closed :phase)))
                        (ok (eq :handed-off (getf closed :outcome)))
                        (ok (equal successor (getf closed :successor)))))))
                 (let* ((pg::*pgdata* root)
                        (path (pg::physical-source-pin-path "cleanup_native_selected")))
                   (ok (if absent (not (probe-file path)) (equalp selected-bytes (lineage-test-bytes path))))
                   (ok (not (probe-file (pg::physical-source-pin-path (getf request :name))))))
                 (ok (reseed-main-native-stop process))))
              (call-with-cleanup-wire-native-source
               root driver
               (lambda (process socket port)
                 (call-with-cleanup-wire-native-client
                  port
                  (lambda (client)
                    (let ((closed (pg::pg-client-native-backup-lease-cleanup-selected client request successor)))
                      (ok (eq :closed (getf closed :phase)))
                      (ok (eq :handed-off (getf closed :outcome)))
                      (ok (equal successor (getf closed :successor))))
                    (ok (search
                         ":CLEANUP-WIRE-RESTART-STATE-CHECKED"
                         (cleanup-wire-native-control
                          process socket driver
                          `(pg::with-write-lock
                             (sb-thread:with-mutex (pg::*wal-lock*)
                               (assert (not (probe-file
                                             (pg::physical-source-pin-path ,(getf request :name)))))
                               ,(if absent
                                    '(assert (not (probe-file
                                                   (pg::physical-source-pin-path "cleanup_native_selected"))))
                                    `(assert
                                      (equalp ',selected-bytes
                                              (pg::read-bounded-regular-file
                                               (pg::physical-source-pin-path "cleanup_native_selected")
                                               65536))))
                               :cleanup-wire-restart-state-checked)))
                         :test #'char-equal))
                    (dotimes (retry 2)
                      (declare (ignorable retry))
                      (ok (pg::pg-client-native-release client "cleanup_native_selected"
                                                        (getf successor :anchor-digest))))))
                 (ok (reseed-main-native-stop process))))))))))
