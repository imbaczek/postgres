(in-package #:pg-physical-tests)

(defun reseed-main-native-wait (process seconds)
  (loop with deadline = (+ (get-internal-real-time) (* seconds internal-time-units-per-second))
        while (member (sb-ext:process-status process) '(:running :stopped))
        do (when (>= (get-internal-real-time) deadline)
             (error "owned native process ~d exceeded ~d seconds" (sb-ext:process-pid process) seconds))
           (sleep 0.05))
  (sb-ext:process-wait process)
  (list (sb-ext:process-status process) (sb-ext:process-exit-code process)))

(defun reseed-main-native-dispose (process)
  (when process
    (when (member (sb-ext:process-status process) '(:running :stopped))
      (sb-ext:process-kill process sb-posix:sigkill))
    (sb-ext:process-wait process)
    (let ((pid (sb-ext:process-pid process))
          (status (sb-ext:process-status process))
          (exit (sb-ext:process-exit-code process)))
      (sb-ext:process-close process)
      (format t "~%RESEED-MAIN-NATIVE-PROCESS-CLOSED ~d ~s ~s~%" pid status exit)
      (finish-output))))

(defun reseed-main-native-port ()
  (let ((socket (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp)))
    (unwind-protect
         (progn
           (sb-bsd-sockets:socket-bind socket #(127 0 0 1) 0)
           (nth-value 1 (sb-bsd-sockets:socket-name socket)))
      (sb-bsd-sockets:socket-close socket))))

(defun reseed-main-native-control (process socket driver form)
  (unless (eq :running (sb-ext:process-status process))
    (error "owned native server ended before control evaluation"))
  (let* ((tag (symbol-name (gensym "CONTROL-")))
         (input (merge-pathnames (concatenate 'string tag ".input") driver))
         (output (merge-pathnames (concatenate 'string tag ".output") driver))
         (client nil))
    (with-open-file (stream input :direction :output :if-exists :error)
      (let ((*package* (find-package :pg)) (*print-length* nil) (*print-level* nil))
        (write form :stream stream :readably t :pretty nil)
        (terpri stream)
        (write-line "(quit)" stream)))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf client
                   (sb-ext:run-program
                    (namestring sb-ext:*runtime-pathname*)
                    (list "--noinform" "--no-sysinit" "--no-userinit" "--script"
                          (namestring (asdf:system-relative-pathname "pg" "t/control-client.lisp"))
                          "unix" (namestring socket))
                    :wait nil :input input :output output :error :output :if-output-exists :error)))
           (unless (equal '(:exited 0) (reseed-main-native-wait client 60))
             (error "native control client failed: ~a" (uiop:read-file-string output)))
           (let ((text (uiop:read-file-string output)))
             (when (search ";; error:" text :test #'char-equal)
               (error "native control evaluation failed: ~a" text))
             text))
      (reseed-main-native-dispose client))))

(defun reseed-main-native-arguments (root socket port cache hot-standby)
  (let* ((directories (remove-duplicates
                       (remove nil (mapcar #'asdf:system-source-directory (asdf:already-loaded-systems)))
                       :test #'equal))
         (registry `(:source-registry
                     ,@(mapcar (lambda (directory) `(:directory ,directory)) directories)
                     :ignore-inherited-configuration))
         (argv (list "pg-lisp" "-D" (namestring root) "-p" (write-to-string port)
                     "--control-socket" (namestring socket)
                     "-c" (if hot-standby "hot_standby=on" "hot_standby=off")))
         (forms
           (list
            '(require :asdf)
            `(asdf:initialize-source-registry ',registry)
            `(asdf:initialize-output-translations
              '(:output-translations (t ,(namestring cache)) :ignore-inherited-configuration))
            '(asdf:load-system "pg")
            `(let ((cl-user::reseed-main-worker-start (symbol-function 'pg::start-postmaster-recovery-workers)))
               (setf (symbol-function 'pg::start-postmaster-recovery-workers)
                     (lambda ()
                       (unwind-protect
                            (progn
                              (pg::physical-restartpoint-start-worker)
                              (pg::physical-promotion-start-worker)
                              (format t "~%RESEED-MAIN-NATIVE-NETWORK-WORKERS-ISOLATED ~d~%" (sb-posix:getpid))
                              (finish-output))
                         (setf (symbol-function 'pg::start-postmaster-recovery-workers) cl-user::reseed-main-worker-start))))
               (let ((sb-ext:*posix-argv* ',argv))
                 (pg::main))))))
    (let ((*package* (find-package :cl-user)) (*print-length* nil) (*print-level* nil))
      (append '("--noinform" "--no-sysinit" "--no-userinit" "--non-interactive")
              (loop for form in forms append (list "--eval" (write-to-string form :readably t)))))))

(defun reseed-main-native-await-ready (process root socket driver)
  (loop with deadline = (+ (get-internal-real-time) (* 180 internal-time-units-per-second))
        do (unless (eq :running (sb-ext:process-status process))
             (error "owned native server ~d ended during startup: ~s"
                    (sb-ext:process-pid process)
                    (list (sb-ext:process-status process) (sb-ext:process-exit-code process))))
           (when (probe-file socket)
             (let ((response
                     (reseed-main-native-control
                      process socket driver
                      `(and (= (sb-posix:getpid) ,(sb-ext:process-pid process))
                            (equal (truename pg::*pgdata*) (truename ,root))
                            (pg::postmaster-pid-owned-p)
                            pg::*wal-ready* pg::*physical-receive-store*
                            (pg::recovery-in-progress-p)
                            (member "ready   "
                                    (uiop:split-string
                                     (sb-ext:octets-to-string
                                      (getf (pg::physical-reseed-read-pid pg::*pgdata*) :bytes)
                                      :external-format :utf-8)
                                     :separator '(#\Newline))
                                    :test #'string=)
                            (not (probe-file (merge-pathnames "physical-reseed.follow" pg::*pgdata*)))
                            (not (probe-file (merge-pathnames "physical-reseed.retired" pg::*pgdata*)))
                            :reseed-main-native-ready))))
               (when (search ":RESEED-MAIN-NATIVE-READY" response :test #'char-equal)
                 (return t))))
           (when (>= (get-internal-real-time) deadline)
             (error "owned native server did not reach control and listener readiness"))
           (sleep 0.1)))

(defun reseed-main-native-produce-wal (root start path)
  (let* ((settings (pg::startup-default-settings root))
         (pg::*pgdata* root)
         (pg::*backend* nil)
         (pg::*catalog* nil)
         (pg::*catalog-oldest-relfrozenxid* nil)
         (pg::*physical-recovery-state* nil)
         (pg::*physical-recovery-failed-p* nil)
         (pg::*last-replay-lsn* nil)
         (pg::*wal-ready* nil)
         (pg::*wal-poisoned* nil)
         (pg::*wal-control-metadata* nil)
         (pg::*wal-storage-incarnation* (gensym "RESEED-SOURCE-"))
         (pg::*wal-temp-cleanup-domain* nil)
         (pg::*wal-dirty-store* nil)
         (pg::*wal-pending-deletes* nil)
         (pg::*wal-pending-page-publication* nil)
         (pg::*wal-pending-page-publication-tail* nil)
         (pg::*clog-pending-commits* (make-hash-table :test #'eql))
         (pg::*startup-installed-configuration*
           (prepare-reader-test-configuration settings (make-hash-table :test #'equal)))
         (pg::*wal-end-lsn* 0) (pg::*wal-durable-lsn* 0)
         (pg::*wal-checkpoint-lsn* 0) (pg::*wal-checkpoint-snapshot* nil)
         (pg::*wal-xid-reserved-through* nil)
         (pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1)
         (pg::*wal-prev-timeline* 1) (pg::*wal-segment-namespace* :legacy)
         (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
         (pg::*wal-new-segments* nil)
         (pg::*wal-page-cache* (make-hash-table :test #'equal))
         (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
         (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (pg::*wal-replication-slots* (make-hash-table :test #'equal))
         (pg::*relation-streams* (make-hash-table :test #'equal))
         (pg::*logical-capture-xid-modes* (make-hash-table :test #'eql))
         (pg::*logical-binary-required-xids* (make-hash-table :test #'eql))
         (pg::*logical-capture-arming* (make-hash-table :test #'eq))
         (pg::*logical-capture-live-slots* 0)
         (pg::*clog-stream* nil) (pg::*next-xid* 3) (pg::*xid-batch-end* 3)
         (pg::*active-xids* nil) (pg::*xid-wrap-pending* nil)
         (pg::*xid-wrap-completed-through* nil)
         (pg::*pm-requested-mode* pg::+no-shutdown+)
         (pg::*accept-stop-requested* nil)
         (pg::*shutdown-finalizing-p* nil) (pg::*shutdown-finished* nil))
    (multiple-value-bind (parameters object-limit owner-limit)
        (pg::startup-resource-capacity-plan settings)
      (let ((pg::*installed-resource-parameters* parameters)
            (pg::*relation-lock-object-limit* object-limit)
            (pg::*relation-lock-owner-limit* owner-limit))
        (unwind-protect
             (progn
               (pg::wal-mark-primary-startup)
               (pg::wal-init)
               (pg::xact-init)
               (pg::wal-recover)
               (pg::note-primary-startup-replay)
               (pg::catalog-init)
               (pg::wal-report-installed-resource-capacity)
               (pg::wal-note-primary-ready)
               (uiop:with-temporary-file (:stream stream :direction :io
                                          :element-type '(unsigned-byte 8))
                 (let ((pg::*backend* (pg::make-backend :stream stream)))
                   (unwind-protect
                        (dolist (sql '("CREATE TABLE receipt_progress (id integer)"
                                       "INSERT INTO receipt_progress VALUES (17), (29)"))
                          (unless (nth-value 1 (pg::exec-simple-query stream sql))
                            (error "source SQL did not complete: ~a" sql)))
                     (pg::end-backend-session))))
               (pg::wal-require-no-pending-storage)
               (let ((types nil) (snapshots nil) (committed-xid nil))
                 (pg::wal-scan
                  (lambda (record)
                    (push (pg::wal-record-type record) types)
                    (when (= pg::+wal-record-commit+ (pg::wal-record-type record))
                      (setf committed-xid (pg::wal-record-xid record)))
                    (when (= pg::+wal-record-running-xacts+ (pg::wal-record-type record))
                      (push (pg::wal-running-xacts-record-info record) snapshots)))
                  start :end-lsn pg::*wal-durable-lsn*)
                 (list :end (funcall (symbol-function 'pg::physical-test-export-wal) start path)
                       :system-id (pg::catalog-system-identifier)
                       :committed-xid committed-xid
                       :resource-fact (pg::wal-copy-control-resource-fields pg::*wal-control-metadata*)
                       :next-xid pg::*next-xid*
                       :reserved-through pg::*wal-xid-reserved-through*
                       :types (nreverse types) :source-xids (nreverse snapshots))))
          (unwind-protect
               (pg::smgr-close-all)
            (unwind-protect
                 (progn (pg::wal-clear-page-cache) (pg::wal-dirty-close-empty-store))
              (unwind-protect
                   (when pg::*wal-output-stream* (close pg::*wal-output-stream*))
                (when pg::*clog-stream* (close pg::*clog-stream*))))))))))

(deftest reseed-main-native-source-produces-committed-progress
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore snapshot metadata))
       (with-physical-store-directory (driver)
         (load (asdf:system-relative-pathname "pg" "t/physical-replay-native.lisp"))
         (let* ((path (merge-pathnames "source.frames" driver))
                (result (reseed-main-native-produce-wal root 152 path)))
           (ok (> (getf result :end) 152))
           (ok (> (getf result :next-xid) 42))
           (ok (> (getf result :reserved-through) (getf result :next-xid)))
           (ok (member pg::+wal-record-parameter-change+ (getf result :types)))
           (ok (= 1 (count pg::+wal-record-commit+ (getf result :types))))
           (ok (member pg::+wal-record-catalog+ (getf result :types)))
           (ok (some (lambda (state)
                       (and (getf state :complete)
                            (= (getf result :next-xid) (getf state :next-xid))
                            (member (getf result :committed-xid) (getf state :running))))
                     (getf result :source-xids)))
           (ok (with-open-file (input path :element-type '(unsigned-byte 8)) (plusp (file-length input)))))
         (ok (null pg::*backend*))))))

(deftest reseed-main-native-boot-consumes-receipt-through-real-storage
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :receipt)
       (with-physical-store-directory (driver)
         (sb-posix:chmod (namestring driver) #o700)
         (call-with-reseed-main-native-server
          root driver nil
          (lambda (process socket port)
            (declare (ignore port))
            (ok (eq :running (sb-ext:process-status process)))
            (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
            (ok (not (probe-file (merge-pathnames "physical-reseed.follow" root))))
            (ok (= (sb-ext:process-pid process) (getf (pg::physical-reseed-read-pid root) :pid)))
            (ok (search ":NATIVE-STORAGE-VERIFIED"
                        (reseed-main-native-control
                         process socket driver
                         `(progn
                            (assert (= (sb-posix:getpid) ,(sb-ext:process-pid process)))
                            (assert (eq :legacy (getf pg::*physical-recovery-state* :wal-namespace)))
                            (assert (= 2 (getf pg::*physical-recovery-state* :phase)))
                            (pg::physical-store-check-live pg::*physical-receive-store*)
                            :native-storage-verified))
                        :test #'char-equal))
            (ok (reseed-main-native-stop process))))
         (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
         (ok (sb-posix:fstat root-fd))))))

(defun call-with-reseed-main-native-server (root driver hot-standby function)
  (let* ((tag (symbol-name (gensym "BOOT-")))
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
                    (reseed-main-native-arguments root socket port cache hot-standby)
                    :wait nil :output log :error :output :if-output-exists :error)))
           (format t "~%RESEED-MAIN-NATIVE-OWNED ~d ~s ~d ~s~%"
                   (sb-ext:process-pid process) root port socket)
           (finish-output)
           (reseed-main-native-await-ready process root socket driver)
           (funcall function process socket port))
      (reseed-main-native-dispose process)
      (when (probe-file log)
        (format t "~%RESEED-MAIN-NATIVE-LOG-BEGIN~%~a~%RESEED-MAIN-NATIVE-LOG-END~%"
                (uiop:read-file-string log))
        (finish-output)))))

(defun reseed-main-native-stop (process)
  (unless (eq :running (sb-ext:process-status process))
    (error "owned native server ended before requested shutdown"))
  (sb-ext:process-kill process sb-posix:sigint)
  (unless (equal '(:exited 0) (reseed-main-native-wait process 90))
    (error "owned native server failed its requested shutdown"))
  (format t "~%RESEED-MAIN-NATIVE-VERIFIED ~d ~s ~s~%"
          (sb-ext:process-pid process) (sb-ext:process-status process)
          (sb-ext:process-exit-code process))
  (finish-output)
  t)

(defun reseed-main-native-sql (port driver sql)
  (let ((output (merge-pathnames (concatenate 'string (symbol-name (gensym "SQL-")) ".log") driver))
        (process nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process
                   (sb-ext:run-program
                    "psql"
                    (list "-X" "-w" "-A" "-t" "-h" "127.0.0.1" "-p" (write-to-string port)
                          "-U" "postgres" "-d" "postgres" "-v" "ON_ERROR_STOP=1" "-c" sql)
                    :search t :wait nil :output output :error :output :if-output-exists :error)))
           (unless (equal '(:exited 0) (reseed-main-native-wait process 60))
             (error "owned native SQL client failed: ~a" (uiop:read-file-string output)))
           (uiop:read-file-string output))
      (reseed-main-native-dispose process))))

(defun reseed-main-native-reject-stale-receipt (root driver receipt)
  (let* ((socket (merge-pathnames "stale.sock" driver))
         (cache (merge-pathnames "stale-cache/" driver))
         (log (merge-pathnames "stale.log" driver))
         (receipt-path (merge-pathnames "physical-reseed.retired" root))
         (pid-path (merge-pathnames "postmaster.pid" root))
         (pid-bytes (pg::string->bytes (format nil "-~d~%stale receipt must preserve this PID file~%"
                                               (sb-posix:getpid))))
         (control-path (merge-pathnames "physical-recovery.control" root))
         (control (pg::recovery-read-bytes control-path))
         (process nil))
    (assert (not (probe-file receipt-path)))
    (assert (not (probe-file pid-path)))
    (pg::wal-atomic-write-bytes receipt-path receipt)
    (sb-posix:chmod (namestring receipt-path) #o600)
    (assert (= #o100600 (sb-posix:stat-mode (sb-posix:stat (namestring receipt-path)))))
    (pg::wal-atomic-write-bytes pid-path pid-bytes)
    (let ((pid (pg::physical-reseed-read-pid root))
          (receipt-stat (pg::physical-reseed-file-facts (sb-posix:stat (namestring receipt-path)))))
      (unwind-protect
           (progn
             (sb-sys:without-interrupts
               (setf process
                     (sb-ext:run-program
                      (namestring sb-ext:*runtime-pathname*)
                      (reseed-main-native-arguments root socket (reseed-main-native-port) cache t)
                      :wait nil :output log :error :output :if-output-exists :error)))
             (format t "~%RESEED-MAIN-STALE-OWNED ~d ~s ~s~%"
                     (sb-ext:process-pid process) root socket)
             (finish-output)
             (assert (equal '(:exited 1) (reseed-main-native-wait process 180)))
             (let ((text (uiop:read-file-string log)))
               (dolist (expected '("physical image input is not a private regular file"
                                   "\"pg_catalog.dat\""
                                   "PG::RECOVERY-CHECK-MANIFEST-FILES"
                                   "PG::PHYSICAL-RECOVERY-VERIFY-BASE-CONTROL-WITH-NAMESPACE"))
                 (assert (search expected text :test #'char-equal))))
             (assert (equalp pid (pg::physical-reseed-read-pid root)))
             (assert (equalp receipt (pg::recovery-read-bytes receipt-path)))
             (assert (equal receipt-stat
                            (pg::physical-reseed-file-facts (sb-posix:stat (namestring receipt-path)))))
             (assert (equalp control (pg::recovery-read-bytes control-path)))
             (assert (not (probe-file socket)))
             (format t "~%RESEED-MAIN-STALE-VERIFIED ~d :EXITED 1~%"
                     (sb-ext:process-pid process))
             (finish-output)
             t)
        (reseed-main-native-dispose process)
        (when (probe-file log)
          (format t "~%RESEED-MAIN-STALE-LOG-BEGIN~%~a~%RESEED-MAIN-STALE-LOG-END~%"
                  (uiop:read-file-string log))
          (finish-output))
        (when (probe-file pid-path) (delete-file pid-path))
        (when (probe-file receipt-path) (delete-file receipt-path))))))

(deftest reseed-main-native-progress-survives-ordinary-boot-without-workspace
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (let ((source-root pg::*pgdata*))
         (assert (not (equal (truename source-root) (truename root))))
         (reset-retired-reader-test-files root record :receipt)
         (with-physical-store-directory (driver)
           (sb-posix:chmod (namestring driver) #o700)
           (let ((frames (merge-pathnames "source.frames" driver))
                 (hidden (merge-pathnames "historical-workspace/" driver))
                 (result nil)
                 (receipt (pg::recovery-read-bytes (merge-pathnames "physical-reseed.retired" root)))
                 (moved nil))
             (unwind-protect
                  (progn
                    (call-with-reseed-main-native-server
                     root driver nil
                     (lambda (process socket port)
                       (declare (ignore port))
                       (reseed-main-native-control
                        process socket driver
                        '(assert (= 152 (pg::physical-store-flushed-lsn pg::*physical-receive-store*))))
                       (load (asdf:system-relative-pathname "pg" "t/physical-replay-native.lisp"))
                       (setf result (reseed-main-native-produce-wal source-root 152 frames))
                       (assert (= (getf result :system-id)
                                  (getf (getf (getf record :attempt) :request) :system-id)))
                       (format t "~%RESEED-MAIN-SOURCE-PROGRESS ~s~%" result)
                       (finish-output)
                       (reseed-main-native-control
                        process socket driver
                        `(progn
                           (load ,(namestring (asdf:system-relative-pathname "pg" "t/physical-replay-native.lisp")))
                           (funcall (symbol-function 'pg::physical-test-receive-file) ,frames)
                           (assert (= ,(getf result :end)
                                      (pg::physical-store-flushed-lsn pg::*physical-receive-store*)))))
                       (reseed-main-native-control process socket driver '(pg::physical-recovery-replay))
                       (ok (search
                            ":REPLAY-PROGRESS-VERIFIED"
                            (reseed-main-native-control
                             process socket driver
                             `(progn
                                (assert (= ,(getf result :end) pg::*last-replay-lsn*))
                                (assert (= ,(getf result :next-xid) pg::*next-xid*))
                                (assert (= ,(getf result :end)
                                           (getf (pg::physical-recovery-read-control pg::*pgdata*) :applied)))
                                (assert (= ,(getf result :next-xid)
                                           (getf (pg::physical-recovery-read-control pg::*pgdata*) :next-xid)))
                                (assert (equal ',(getf result :resource-fact)
                                               (getf pg::*physical-recovery-state* :source-resource-fact)))
                                (funcall (symbol-function 'pg::physical-test-verify-source-xids))
                                :replay-progress-verified))
                            :test #'char-equal))
                       (reseed-main-native-control process socket driver '(pg::physical-recovery-restartpoint))
                       (ok (search
                            ":RESTARTPOINT-PROGRESS-VERIFIED"
                            (reseed-main-native-control
                             process socket driver
                             `(progn
                                (assert (= ,(getf result :end)
                                           (getf pg::*physical-recovery-state* :restartpoint-lsn)))
                                (assert (pg::physical-store-retained-root pg::*physical-receive-store*))
                                (assert (= ,(getf result :end)
                                           (pg::physical-store-floor-lsn pg::*physical-receive-store*)))
                                (assert (= ,(getf result :end)
                                           (pg::physical-store-flushed-lsn pg::*physical-receive-store*)))
                                (assert (equal ',(getf result :resource-fact)
                                               (getf (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                                                     :source-resource-fact)))
                                (assert (getf (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                                              :source-reservation))
                                (funcall (symbol-function 'pg::physical-test-verify-source-xids) :generation t)
                                :restartpoint-progress-verified))
                            :test #'char-equal))
                       (ok (reseed-main-native-stop process))))
                    (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
                    (rename-file workspace hidden)
                    (setf moved t)
                    (ok (not (probe-file workspace)))
                    (call-with-reseed-main-native-server
                     root driver t
                     (lambda (process socket port)
                       (ok (search (format nil "17~%29~%")
                                   (reseed-main-native-sql port driver
                                                           "SELECT id FROM receipt_progress ORDER BY id;")))
                       (ok (search (format nil "t~%")
                                   (reseed-main-native-sql port driver "SELECT pg_is_in_recovery();")))
                       (ok (search (format nil "on~%")
                                   (reseed-main-native-sql port driver "SHOW transaction_read_only;")))
                       (ok (search "CHECKPOINT" (reseed-main-native-sql port driver "CHECKPOINT;")))
                       (ok (search
                            ":ORDINARY-PROGRESS-VERIFIED"
                            (reseed-main-native-control
                             process socket driver
                             `(progn
                                (load ,(namestring (asdf:system-relative-pathname "pg" "t/physical-replay-native.lisp")))
                                (assert (= ,(getf result :end) pg::*last-replay-lsn*))
                                (assert (= ,(getf result :next-xid) pg::*next-xid*))
                                (assert (= ,(getf result :next-xid)
                                           (getf (pg::physical-recovery-read-control pg::*pgdata*) :next-xid)))
                                (assert (pg::physical-store-retained-root pg::*physical-receive-store*))
                                (assert (= ,(getf result :end)
                                           (pg::physical-store-floor-lsn pg::*physical-receive-store*)))
                                (assert (= ,(getf result :end)
                                           (pg::physical-store-flushed-lsn pg::*physical-receive-store*)))
                                (assert (equal ',(getf result :resource-fact)
                                               (getf pg::*physical-recovery-state* :source-resource-fact)))
                                (assert (equal ',(getf result :resource-fact)
                                               (getf (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                                                     :source-resource-fact)))
                                (assert (getf (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                                              :source-reservation))
                                (funcall (symbol-function 'pg::physical-test-verify-source-xids) :generation t)
                                :ordinary-progress-verified))
                            :test #'char-equal))
                       (ok (reseed-main-native-stop process))))
                    (ok (not (probe-file workspace)))
                    (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
                    (ok (sb-posix:fstat root-fd))
                    (rename-file hidden workspace)
                    (setf moved nil)
                    (ok (probe-file workspace))
                    (ok (reseed-main-native-reject-stale-receipt root driver receipt))
                    (rename-file workspace hidden)
                    (setf moved t)
                    (ok (not (probe-file workspace)))
                    (call-with-reseed-main-native-server
                     root driver t
                     (lambda (process socket port)
                       (declare (ignore socket))
                       (ok (search (format nil "17~%29~%")
                                   (reseed-main-native-sql port driver
                                                           "SELECT id FROM receipt_progress ORDER BY id;")))
                       (ok (reseed-main-native-stop process)))))
               (when moved (rename-file hidden workspace)))))))))
