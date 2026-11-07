(in-package #:pg-physical-tests)

(defun retired-receipt-test-rows (mode inode size)
  (let* ((geometry (nth mode '(:follow :alias :receipt)))
         (row (list (copy-seq "physical-reseed.follow") 17 inode #o100600
                    (if (= mode 1) 2 1) 1000 size 100 200))
         (receipt (cons (copy-seq "physical-reseed.retired") (copy-tree (rest row)))))
    (values (unless (= mode 2) row) (unless (= mode 0) receipt) geometry)))

(cl-spec:defproperty reseed-retired-geometry-binds-exact-names-and-link-counts
    ((mode (range integer 0 2)) (inode (range integer 1 1000000)) (size (range integer 1 4096)))
  (:about pg::physical-reseed-follow-retired-geometry)
  (:trials (:normal 100))
  (multiple-value-bind (follow receipt geometry) (retired-receipt-test-rows mode inode size)
    (let ((before (copy-tree (list follow receipt))))
      (and (eq geometry
               (funcall (symbol-function 'pg::physical-reseed-follow-retired-geometry) follow receipt))
           (equal before (list follow receipt))))))

(deftest generated-reseed-retired-geometry-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retired-geometry-binds-exact-names-and-link-counts
                                       :seed 172 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-retired-geometry-refuses-inconsistent-observations
    (dolist (case '(:absent :follow-name :receipt-name :follow-extra-link :receipt-extra-link
                    :follow-single-link :receipt-single-link :third-link :inode :device :mode
                    :owner :size :mtime :ctime :row-length))
      (multiple-value-bind (follow receipt geometry)
          (retired-receipt-test-rows
           (case case ((:follow-name :follow-extra-link) 0)
                 ((:receipt-name :receipt-extra-link) 2) (otherwise 1))
           11 128)
        (declare (ignore geometry))
        (ecase case
          (:absent (setf follow nil receipt nil))
          (:follow-name (setf (first follow) "foreign"))
          (:receipt-name (setf (first receipt) "foreign"))
          (:follow-extra-link (setf (fifth follow) 2))
          (:receipt-extra-link (setf (fifth receipt) 2))
          (:follow-single-link (setf (fifth follow) 1))
          (:receipt-single-link (setf (fifth receipt) 1))
          (:third-link (setf (fifth follow) 3 (fifth receipt) 3))
          (:inode (incf (third receipt)))
          (:device (incf (second receipt)))
          (:mode (setf (fourth receipt) #o100644))
          (:owner (incf (sixth receipt)))
          (:size (incf (seventh receipt)))
          (:mtime (incf (eighth receipt)))
          (:ctime (incf (ninth receipt)))
          (:row-length (setf receipt (append receipt (list :unexpected)))))
        (ok (handler-case
                (progn
                  (funcall (symbol-function 'pg::physical-reseed-follow-retired-geometry) follow receipt)
                  nil)
              (error () t))))))

(defun call-with-retired-reader-test-fixture (kind function)
  (call-with-reseed-retirement-observation-fixture
   kind
   (lambda (owner root workspace root-fd snapshot check)
     (declare (ignore owner workspace))
     (let* ((before (prepare-retirement-terminal-test root root-fd snapshot nil))
            (record (pg::physical-reseed-follow-retirement-next-record
                     before (getf (getf before :retirement) :attempt-file))))
       (reseed-retirement-layout-write root record)
       (funcall function root root-fd record check)))))

(defun call-with-retired-startup-test-context (root function &optional observer)
  (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                            (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                            (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                            (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                            (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                            (before nil) (after nil))
    (unwind-protect
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (when observer (funcall observer :enter phase before after))
            (if (eq phase :before-pid)
                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                (setf after (pg::physical-reseed-follow-startup-observation lock phase before)))
            (when observer (funcall observer :leave phase before after)))
          :startup-reconcile (lambda () (funcall function before after)))
      (unwind-protect
           (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
        (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
        (setf sb-ext:*exit-hooks* hooks)))))

(defun call-with-retired-startup-test-fixture (kind function)
  (call-with-reseed-retirement-observation-fixture
   kind
   (lambda (owner root workspace root-fd snapshot check)
     (declare (ignore check))
     (let* ((before (prepare-retirement-terminal-test root root-fd snapshot nil))
            (record (pg::physical-reseed-follow-retirement-next-record
                     before (getf (getf before :retirement) :attempt-file))))
       (reseed-retirement-layout-write root record)
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (funcall function root workspace root-fd record)))))

(defun terminal-startup-test-observation (mode bad nonce)
  (let* ((record (list :activation (list :phase :active :step 2)
                       :retirement (list :phase :retired :step 2)
                       :opaque (list nonce "KeepCase")))
         (evidence (list :prefix 2 :snapshot (list :record record)
                         :location nil :temporary nil :temporary-file nil))
         (observation (list :branch (if (zerop mode) :retirement :retired) :evidence evidence)))
    (unless (zerop mode)
      (setf (getf evidence :journal) (list :geometry (if (= mode 1) :alias :receipt))))
    (ecase bad
      (0 nil)
      (1 (setf (getf (getf record :retirement) :phase) :retiring))
      (2 (setf (getf (getf record :retirement) :step) 1))
      (3 (setf (getf evidence :prefix) 1))
      (4 (setf (getf evidence :location) :install))
      (5 (setf (getf evidence :temporary) t))
      (6 (setf (getf evidence :temporary-file) '(:partial t)))
      (7 (setf (getf (getf record :activation) :phase) :activating))
      (8 (setf (getf (getf record :activation) :step) 1))
      (9 (setf (getf observation :branch) :unknown)))
    observation))

(cl-spec:defproperty reseed-main-terminal-gate-preserves-input-and-rejects-pending
    ((mode (range integer 0 2)) (bad (range integer 0 9)) (nonce (range integer 0 1000000)))
  (:about pg::physical-reseed-follow-check-terminal-startup)
  (:trials (:normal 100))
  (let* ((observation (terminal-startup-test-observation mode bad nonce))
         (before (copy-tree observation))
         (result (handler-case
                     (pg::physical-reseed-follow-check-terminal-startup observation)
                   (error () :rejected))))
    (and (if (zerop bad) (eq observation result) (eq :rejected result))
         (equal before observation))))

(deftest generated-reseed-main-terminal-gate-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-main-terminal-gate-preserves-input-and-rejects-pending
                                       :seed 182 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-main-terminal-gate-requires-exact-terminal-evidence
    (dolist (mode '(0 1 2))
      (dotimes (bad 10)
        (let* ((observation (terminal-startup-test-observation mode bad 42))
               (before (copy-tree observation)))
          (if (zerop bad)
              (ok (eq observation (pg::physical-reseed-follow-check-terminal-startup observation)))
              (ok (handler-case
                      (progn (pg::physical-reseed-follow-check-terminal-startup observation) nil)
                    (error (condition)
                      (search "postmaster startup requires" (princ-to-string condition))))))
          (ok (equal before observation)))))
  (let ((ordinary '(:branch :ordinary)))
    (ok (eq ordinary (pg::physical-reseed-follow-check-terminal-startup ordinary)))
    (ok (equal '(:branch :ordinary) ordinary)))
  (ok (handler-case
          (progn (pg::physical-reseed-follow-check-terminal-startup nil) nil)
        (error (condition)
          (search "recognized reseed observation" (princ-to-string condition))))))

(deftest reseed-main-startup-consumes-each-terminal-receipt-geometry
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (dolist (geometry '(:follow :alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (let ((baseline (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                               (reseed-mapped-test-tree-facts workspace)))
               (consume (symbol-function 'pg::physical-reseed-follow-retirement-consume-receipt))
               (captured nil) (entry-geometry nil))
           (unwind-protect
                (call-with-adoption-postmaster-fixture
                 root
                 (lambda ()
                   (setf (symbol-function 'pg::physical-reseed-follow-retirement-consume-receipt)
                         (lambda (owner)
                           (setf captured owner
                                 entry-geometry (getf (getf (pg::retirement-startup-evidence owner) :journal) :geometry))
                           (funcall consume owner)))
                   (ok (equal (truename root) (pg::physical-reseed-follow-own-startup "127.0.0.1" 5439)))
                   (ok (eq :receipt entry-geometry))
                   (ok captured)
                   (ok (not (pg::retirement-startup-active-p captured)))
                   (ok (null (pg::retirement-startup-evidence captured)))
                   (ok (not (pg::retirement-startup-uncertain-p captured)))
                   (ok (not (pg::retirement-startup-busy-p captured)))
                   (ok (pg::postmaster-pid-owned-p))
                   (ok (not pg::*wal-ready*))
                   (ok (null pg::*physical-receive-store*))
                   (ok (null (pg::physical-reseed-follow-check-ordinary-startup)))))
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-consume-receipt) consume))
           (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow" root))))
           (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
           (ok (reseed-activation-layout-facts-equal-p
                baseline
                (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                      (reseed-mapped-test-tree-facts workspace))))
           (ok (sb-posix:fstat root-fd)))))))

(deftest reseed-main-startup-preserves-ordinary-and-legacy-adoption-reconciliation
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (let ((reconcile (symbol-function 'pg::physical-adoption-reconcile-startup))
               (calls 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-adoption-reconcile-startup)
                        (lambda () (incf calls) (funcall reconcile)))
                  (ok (equal (truename root) (pg::physical-reseed-follow-own-startup "127.0.0.1" 5439)))
                  (ok (= 1 calls))
                  (ok (pg::postmaster-pid-owned-p)))
             (setf (symbol-function 'pg::physical-adoption-reconcile-startup) reconcile))))))
  (dolist (distance '(0 64))
    (call-with-adoption-follow-transaction-fixture
     (lambda (root owner handoff directory)
       (declare (ignore handoff))
       (pg::physical-adoption-follow-start owner directory)
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (close (pg::physical-adoption-owner-lock owner))
       (sb-posix:chmod (namestring (merge-pathnames ".pg_lisp.lock" root)) #o600)
       (let ((before (remove "postmaster.pid" (adoption-retire-test-protected-image root)
                             :key #'first :test #'equal))
             (workspace (pg::physical-adoption-install-snapshot directory)))
         (call-with-adoption-postmaster-fixture
          root
          (lambda ()
            (ok (equal (truename root) (pg::physical-reseed-follow-own-startup "127.0.0.1" 5439)))
            (ok (pg::postmaster-pid-owned-p))
            (ok (null (pg::physical-reseed-follow-check-ordinary-startup)))
            (ok (equal before (remove "postmaster.pid" (adoption-retire-test-protected-image root)
                                      :key #'first :test #'equal)))
            (ok (equal workspace (pg::physical-adoption-install-snapshot directory)))))
         (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))))
     :fork-distance distance)))

(deftest reseed-main-startup-rejects-readable-nonterminal-state-before-pid-mutation
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (let ((pending (pg::physical-adoption-attempt-value-copy record)))
         (setf (getf (getf pending :retirement) :phase) :retiring)
         (reset-retired-reader-test-files root pending :follow))
       (reseed-follow-owner-test-write
        (merge-pathnames "postmaster.pid" root)
        (sb-ext:string-to-octets (format nil "-~D~%preserve old PID bytes~%" (sb-posix:getpid))
                                 :external-format :utf-8))
       (let* ((pid (merge-pathnames "postmaster.pid" root))
              (bytes (lineage-test-bytes pid))
              (facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring pid))))
              (create (symbol-function 'pg::postmaster-create-pid-file))
              (gate (symbol-function 'pg::physical-reseed-follow-check-terminal-startup))
              (open-lock (symbol-function 'pg::postmaster-open-data-lock))
              (creates 0) (observed nil) (lock nil))
         (unwind-protect
              (call-with-adoption-postmaster-fixture
               root
               (lambda ()
                 (setf (symbol-function 'pg::postmaster-create-pid-file)
                       (lambda () (incf creates) (funcall create))
                       (symbol-function 'pg::physical-reseed-follow-check-terminal-startup)
                       (lambda (observation) (setf observed observation) (funcall gate observation))
                       (symbol-function 'pg::postmaster-open-data-lock)
                       (lambda () (setf lock (funcall open-lock))))
                 (ok (handler-case
                         (progn (pg::physical-reseed-follow-own-startup "127.0.0.1" 5439) nil)
                       (error (condition)
                         (search "requires terminal reseed retirement" (princ-to-string condition)))))
                 (ok (zerop creates))
                 (ok (eq :retirement (getf observed :branch)))
                 (ok (eq :retiring
                         (getf (getf (getf (getf (getf observed :evidence) :snapshot) :record) :retirement) :phase)))
                 (ok (null pg::*postmaster-start-timestamp*))
                 (ok (null pg::*postmaster-pid-stream*))
                 (ok (null pg::*postmaster-data-lock*))
                 (ok (not (open-stream-p lock)))))
           (setf (symbol-function 'pg::postmaster-create-pid-file) create
                 (symbol-function 'pg::physical-reseed-follow-check-terminal-startup) gate
                 (symbol-function 'pg::postmaster-open-data-lock) open-lock))
         (ok (equalp bytes (lineage-test-bytes pid)))
         (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring pid)))))
         (ok (sb-posix:fstat root-fd))))))

(deftest reseed-main-startup-rechecks-all-fences-after-the-outer-owner-guard
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (name '("physical-reseed.attempt" "physical-adoption.follow"))
         (reset-retired-reader-test-files root record :receipt)
         (let* ((path (merge-pathnames name root))
                (bytes (sb-ext:string-to-octets "late startup fence" :external-format :utf-8))
                (consume (symbol-function 'pg::physical-reseed-follow-retirement-consume-receipt))
                (read-pid (symbol-function 'pg::physical-reseed-read-pid))
                (open-lock (symbol-function 'pg::postmaster-open-data-lock))
                (captured nil) (consumed nil) (fired nil) (lock nil))
           (unwind-protect
                (progn
                  (call-with-adoption-postmaster-fixture
                   root
                   (lambda ()
                     (setf (symbol-function 'pg::physical-reseed-follow-retirement-consume-receipt)
                           (lambda (owner)
                             (setf captured owner)
                             (multiple-value-prog1 (funcall consume owner)
                               (setf consumed t)))
                           (symbol-function 'pg::physical-reseed-read-pid)
                           (lambda (directory)
                             (multiple-value-prog1 (funcall read-pid directory)
                               (when (and consumed (not fired)
                                          (pg::retirement-startup-active-p captured)
                                          (null (pg::retirement-startup-evidence captured))
                                          (not (pg::retirement-startup-busy-p captured)))
                                 (setf fired t)
                                 (reseed-follow-owner-test-write path bytes))))
                           (symbol-function 'pg::postmaster-open-data-lock)
                           (lambda () (setf lock (funcall open-lock))))
                     (ok (handler-case
                             (progn (pg::physical-reseed-follow-own-startup "127.0.0.1" 5439) nil)
                           (error (condition)
                             (search (if (string= name "physical-reseed.attempt")
                                         "pending offline reseed requires reconciliation before PID changes"
                                         "offline adoption follow publication")
                                     (princ-to-string condition)))))
                     (ok consumed)
                     (ok fired)
                     (ok (equalp bytes (lineage-test-bytes path)))
                     (ok (not (pg::retirement-startup-active-p captured)))
                     (ok (null (pg::retirement-startup-evidence captured)))
                     (ok (not (pg::retirement-startup-uncertain-p captured)))
                     (ok (null pg::*postmaster-start-timestamp*))
                     (ok (null pg::*postmaster-pid-stream*))
                     (ok (null pg::*postmaster-data-lock*))
                     (ok (not (open-stream-p lock)))
                     (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
                     (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root)))))))
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-consume-receipt) consume
                   (symbol-function 'pg::physical-reseed-read-pid) read-pid
                   (symbol-function 'pg::postmaster-open-data-lock) open-lock)
             (when (probe-file path) (delete-file path)))
           (call-with-adoption-postmaster-fixture
            root
            (lambda ()
              (ok (equal (truename root) (pg::physical-reseed-follow-own-startup "127.0.0.1" 5439)))
              (ok (pg::postmaster-pid-owned-p))
              (ok (null (pg::physical-reseed-follow-check-ordinary-startup)))))
           (ok (sb-posix:fstat root-fd)))))))

(defun check-reseed-main-storage-admission (cut recovery)
  (with-physical-store-directory (root)
    (call-with-adoption-postmaster-fixture
     root
     (lambda ()
       (let* ((pg::*query-temp-file-limit-bytes* pg::*query-temp-file-limit-bytes*)
              (pg::*query-sort-arena-limit-bytes* pg::*query-sort-arena-limit-bytes*)
              (pg::*max-slot-wal-keep-bytes* pg::*max-slot-wal-keep-bytes*)
              (noops '(pg::startup-handle-meta-request pg::authentication-preflight-startup
                       pg::startup-install-initial-settings pg::startup-install-resource-capacity
                       pg::physical-backup-cleanup-stages pg::startup-complete-authentication
                       pg::physical-promotion-initialize-requests pg::call-with-query-sort-arena
                       pg::capture-server-streams))
              (names (append noops '(pg::parse-args pg::physical-promotion-test-point
                                     pg::physical-bootstrap-check-startup pg::initialize-query-temp-storage
                                     pg::recovery-in-progress-p pg::physical-recovery-open-storage
                                     pg::initialize-primary-storage sb-sys:enable-interrupt
                                     pg::physical-export-origin-load)))
              (originals (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (path (merge-pathnames "physical-reseed.attempt" root))
              (bytes (sb-ext:string-to-octets "main admission fence" :external-format :utf-8))
              (bootstrap-calls 0) (storage-calls 0) (signal-installs 0) (cleanup-owned nil)
              (tag (gensym "MAIN-STORAGE")))
         (sb-ext:without-package-locks
           (unwind-protect
                (progn
                  (dolist (name noops)
                    (let ((target name))
                      (setf (symbol-function target)
                            (lambda (&rest arguments)
                              (declare (ignore arguments))
                              (when (eq target 'pg::physical-backup-cleanup-stages)
                                (setf cleanup-owned (pg::postmaster-pid-owned-p)))))))
                  (setf (symbol-function 'pg::parse-args)
                        (lambda (arguments)
                          (declare (ignore arguments))
                          (values "127.0.0.1" 5439 (namestring root) nil nil nil "127.0.0.1"
                                  -1 -1 -1 nil nil nil))
                        (symbol-function 'pg::physical-promotion-test-point)
                        (lambda (point)
                          (when (and (eq point :after-promotion-request-initialization)
                                     (eq cut :before-bootstrap))
                            (reseed-follow-owner-test-write path bytes)))
                        (symbol-function 'pg::physical-bootstrap-check-startup)
                        (lambda () (incf bootstrap-calls) (when (eq recovery :primary-ready) :primary-ready))
                        (symbol-function 'pg::initialize-query-temp-storage)
                        (lambda ()
                          (when (eq cut :before-storage)
                            (reseed-follow-owner-test-write path bytes)))
                        (symbol-function 'pg::recovery-in-progress-p) (lambda () (eq recovery t))
                        (symbol-function 'sb-sys:enable-interrupt)
                        (lambda (signal handler)
                          (declare (ignore handler))
                          (unless (= signal sb-unix:sigusr1) (error "unexpected main startup signal"))
                          (incf signal-installs))
                        (symbol-function 'pg::physical-export-origin-load)
                        (lambda () (throw tag :primary-ready-admitted))
                        (symbol-function 'pg::physical-recovery-open-storage)
                        (lambda ()
                          (incf storage-calls)
                          (throw tag :standby-storage))
                        (symbol-function 'pg::initialize-primary-storage)
                        (lambda ()
                          (incf storage-calls)
                          (throw tag :primary-storage)))
                  (if cut
                      (ok (handler-case
                              (progn (catch tag (pg::main) nil) nil)
                            (error (condition)
                              (search "pending offline reseed requires reconciliation before PID changes"
                                      (princ-to-string condition)))))
                      (ok (eq (cond ((eq recovery :primary-ready) :primary-ready-admitted)
                                    (recovery :standby-storage)
                                    (t :primary-storage))
                              (catch tag (pg::main) nil))))
                  (ok cleanup-owned)
                  (ok (= 1 signal-installs))
                  (ok (= (if (eq cut :before-bootstrap) 0 1) bootstrap-calls))
                  (ok (= (if (or cut (eq recovery :primary-ready)) 0 1) storage-calls))
                  (when cut (ok (equalp bytes (lineage-test-bytes path))))
                  (ok (pg::postmaster-pid-owned-p)))
             (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry))))))))))

(deftest reseed-main-entry-rechecks-fences-before-bootstrap-and-storage
    (dolist (recovery '(nil t :primary-ready))
      (dolist (cut (if (eq recovery :primary-ready)
                       '(nil :before-bootstrap)
                       '(nil :before-bootstrap :before-storage)))
        (check-reseed-main-storage-admission cut recovery))))

(deftest reseed-retired-startup-dispatch-and-owner-bind-every-receipt-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-retired-startup-test-fixture
       kind
       (lambda (root workspace root-fd record)
         (declare (ignore workspace))
         (dolist (geometry '(:follow :alias :receipt))
           (reset-retired-reader-test-files root record geometry)
           (let ((escaped nil))
             (call-with-retired-startup-test-context
              root
              (lambda (before after)
                (ok (eq (if (eq geometry :follow) :retirement :retired) (getf before :branch)))
                (ok (equal before after))
                (ok (if (eq geometry :follow)
                        (null (getf (getf after :evidence) :journal))
                        (eq geometry (getf (getf (getf after :evidence) :journal) :geometry))))
                (pg::call-with-physical-reseed-follow-retirement-startup-owner
                 after
                 (lambda (owner)
                   (setf escaped owner)
                   (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner))
                   (ok (open-stream-p pg::*postmaster-data-lock*))
                   (ok (pg::postmaster-pid-owned-p))
                   (pg::call-with-physical-reseed-follow-retirement-startup-observation
                    owner
                    (lambda (descriptors state verify)
                      (ok (= 10 (length descriptors)))
                      (ok (equal record (getf (getf state :snapshot) :record)))
                      (ok (equal (getf after :evidence)
                                 (if (eq geometry :follow)
                                     (pg::physical-reseed-follow-startup-projection descriptors state)
                                     (pg::physical-reseed-follow-retired-startup-projection descriptors state))))
                      (ok (funcall verify))))))))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-retirement-check-startup-owner escaped) nil)
                   (error () t)))
             (ng (probe-file (merge-pathnames "postmaster.pid" root)))
             (ok (sb-posix:fstat root-fd))))))))

(deftest reseed-retired-startup-rejects-journal-changes-across-pid-creation
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (case '((:alias :receipt) (:receipt :alias) (:alias :replacement) (:receipt :replacement)))
         (let* ((geometry (first case)) (change (second case))
                                        (follow (merge-pathnames "physical-reseed.follow" root))
                                        (receipt (merge-pathnames "physical-reseed.retired" root))
                                        (entered nil) (changed nil) (facts nil) (original-inode nil))
           (reset-retired-reader-test-files root record geometry)
           (setf original-inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt))))
           (ok
            (handler-case
                (progn
                  (call-with-retired-startup-test-context
                   root
                   (lambda (before after)
                     (declare (ignore before after))
                     (setf entered t))
                   (lambda (edge phase before after)
                     (declare (ignore before after))
                     (when (and (eq edge :leave) (eq phase :before-pid))
                       (ecase change
                         (:receipt (delete-file follow))
                         (:alias (sb-posix:link (namestring receipt) (namestring follow)))
                         (:replacement
                          (let ((replacement (merge-pathnames "replacement" root)))
                            (reseed-follow-owner-test-write replacement (pg::physical-reseed-follow-image record))
                            (when (eq geometry :alias) (delete-file follow))
                            (sb-posix:rename (namestring replacement) (namestring receipt))
                            (when (eq geometry :alias)
                              (sb-posix:link (namestring receipt) (namestring follow))))))
                       (setf facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt))))
                       (setf changed t))))
                  nil)
              (error (condition) (search "retirement evidence changed across PID creation"
                                         (princ-to-string condition)))))
           (ok changed)
           (ng entered)
           (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
           (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes receipt)))
           (ok (if (eq change :replacement)
                   (/= original-inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt))))
                   (= original-inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt))))))
           (ok (= (case change (:alias 2) (:receipt 1) (otherwise (if (eq geometry :alias) 2 1)))
                  (sb-posix:stat-nlink (sb-posix:lstat (namestring receipt)))))
           (ng (probe-file (merge-pathnames "postmaster.pid" root)))
           (ok (sb-posix:fstat root-fd)))))))

(deftest reseed-retired-startup-refuses-a-receipt-that-vanishes-during-recognition
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :alias)
       (let* ((receipt (merge-pathnames "physical-reseed.retired" root))
              (follow (merge-pathnames "physical-reseed.follow" root))
              (pid (merge-pathnames "postmaster.pid" root))
              (original (symbol-function 'pg::physical-adoption-path-present-p))
              (fired nil) (entered nil))
         (reseed-follow-owner-test-write
          pid (sb-ext:string-to-octets (format nil "2147483647~%") :external-format :utf-8))
         (let ((pid-bytes (lineage-test-bytes pid))
               (pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-adoption-path-present-p)
                        (lambda (path)
                          (let ((present (funcall original path)))
                            (when (and present (not fired)
                                       (equal "physical-reseed.retired" (file-namestring path)))
                              (delete-file receipt)
                              (setf fired t))
                            present)))
                  (ok (handler-case
                          (progn
                            (call-with-retired-startup-test-context
                             root (lambda (before after) (declare (ignore before after)) (setf entered t)))
                            nil)
                        (error (condition) (search "retired startup requires a named receipt"
                                                   (princ-to-string condition))))))
             (setf (symbol-function 'pg::physical-adoption-path-present-p) original))
           (ok fired)
           (ng entered)
           (ng (pg::physical-adoption-path-present-p receipt))
           (ok (= 1 (sb-posix:stat-nlink (sb-posix:lstat (namestring follow)))))
           (ok (equalp pid-bytes (lineage-test-bytes pid)))
           (ok (= pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
           (ok (sb-posix:fstat root-fd))
           (delete-file pid))))))

(defun check-retired-startup-rejected-evidence (expected)
  (dolist (fault '(:branch :missing-journal :geometry :receipt-row))
    (let* ((changed (pg::physical-adoption-attempt-value-copy expected))
           (evidence (getf changed :evidence)) (journal (getf evidence :journal))
           (entered nil) (lock pg::*postmaster-data-lock*) (pid pg::*postmaster-pid-stream*)
           (position (file-position pid)))
      (ecase fault
        (:branch (setf (getf changed :branch) :retirement))
        (:missing-journal (setf (getf evidence :journal) nil))
        (:geometry (setf (getf journal :geometry)
                         (if (eq :alias (getf journal :geometry)) :receipt :alias)))
        (:receipt-row (incf (third (getf journal :receipt)))))
      (ok (handler-case
              (progn
                (pg::call-with-physical-reseed-follow-retirement-startup-owner
                 changed (lambda (owner) (declare (ignore owner)) (setf entered t)))
                nil)
            (error (condition)
              (search (if (member fault '(:branch :missing-journal))
                          "requires published postmaster ownership and handover evidence"
                          "differs from its PID handover evidence")
                      (princ-to-string condition)))))
      (ng entered)
      (ok (and (eq lock pg::*postmaster-data-lock*) (open-stream-p lock)))
      (ok (and (eq pid pg::*postmaster-pid-stream*) (open-stream-p pid)))
      (ok (eql position (file-position pid))))))

(deftest reseed-retired-startup-owner-refuses-forged-evidence-and-preserves-borrowed-resources
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (geometry '(:alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (call-with-retired-startup-test-context
          root
          (lambda (before after)
            (declare (ignore before))
            (check-retired-startup-rejected-evidence after)
            (when (eq geometry :receipt)
              (pg::call-with-physical-reseed-follow-retirement-startup-owner
               after #'check-retirement-startup-root-lifetimes)
              (check-retirement-startup-owner-exits after))))
         (ng (probe-file (merge-pathnames "postmaster.pid" root)))
         (ok (sb-posix:fstat root-fd))))))

(deftest reseed-retired-startup-owner-rechecks-a-replaced-receipt
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :receipt)
       (let ((receipt (merge-pathnames "physical-reseed.retired" root)) (replacement-inode nil))
         (call-with-retired-startup-test-context
          root
          (lambda (before after)
            (declare (ignore before))
            (pg::call-with-physical-reseed-follow-retirement-startup-owner
             after
             (lambda (owner)
               (let ((original-inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt))))
                     (entered nil))
                 (replace-retired-reader-test-file root receipt record)
                 (setf replacement-inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt))))
                 (ok (/= original-inode replacement-inode))
                 (ok (handler-case
                         (progn
                           (pg::call-with-physical-reseed-follow-retirement-startup-observation
                            owner (lambda (descriptors state verify)
                                    (declare (ignore descriptors state verify)) (setf entered t)))
                           nil)
                       (error (condition) (search "differs from its PID handover evidence"
                                                  (princ-to-string condition)))))
                 (ng entered))))
            (let ((entered nil) (pid pg::*postmaster-pid-stream*) (lock pg::*postmaster-data-lock*))
              (ok (handler-case
                      (progn
                        (pg::call-with-physical-reseed-follow-retirement-startup-owner
                         after (lambda (owner) (declare (ignore owner)) (setf entered t)))
                        nil)
                    (error (condition) (search "differs from its PID handover evidence"
                                               (princ-to-string condition)))))
              (ng entered)
              (ok (and (open-stream-p lock) (open-stream-p pid))))))
         (ok (= replacement-inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt)))))
         (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes receipt)))
         (ng (probe-file (merge-pathnames "postmaster.pid" root)))
         (ok (sb-posix:fstat root-fd))))))

(deftest reseed-retired-startup-owner-keeps-publication-apis-phase-gated
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace root-fd))
       (dolist (geometry '(:alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (call-with-retired-startup-test-context
          root
          (lambda (before after)
            (declare (ignore before))
            (pg::call-with-physical-reseed-follow-retirement-startup-owner
             after
             (lambda (owner)
               (let ((originals
                       (loop for name in '(pg::physical-reseed-follow-retirement-intent-publish-held
                                           pg::physical-reseed-follow-retirement-archive-run
                                           pg::physical-reseed-follow-retirement-terminal-run)
                             collect (cons name (symbol-function name))))
                     (effects 0))
                 (unwind-protect
                      (progn
                        (dolist (entry originals)
                          (setf (symbol-function (car entry))
                                (lambda (&rest arguments)
                                  (declare (ignore arguments))
                                  (incf effects)
                                  (error "unexpected retirement effect entry"))))
                        (ok (handler-case
                                (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                              (error (condition)
                                (search "initial retirement publication requires" (princ-to-string condition)))))
                        (dolist (step '(0 1))
                          (ok (handler-case
                                  (progn (pg::physical-reseed-follow-retirement-archive-step owner step) nil)
                                (error (condition) (search "archive requires Retiring" (princ-to-string condition))))))
                        (multiple-value-bind (actual snapshot)
                            (pg::physical-reseed-follow-retirement-publish-terminal owner)
                          (ok (equal record actual))
                          (ok (equal (getf (getf after :evidence) :snapshot) snapshot)))
                        (ok (zerop effects))
                        (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner))
                        (ok (pg::postmaster-pid-owned-p)))
                   (dolist (entry originals)
                     (setf (symbol-function (car entry)) (cdr entry)))))))))))))

(deftest reseed-retirement-operation-final-check-keeps-admission-closed
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace root-fd record))
       (call-with-retired-startup-test-context
        root
        (lambda (before after)
          (declare (ignore before))
          (pg::call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (let ((original (symbol-function 'pg::physical-reseed-follow-retirement-check-startup-owner))
                   (body-ended nil)
                   (guard-ended nil))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-reseed-follow-retirement-check-startup-owner)
                            (lambda (current)
                              (multiple-value-prog1 (funcall original current)
                                (when body-ended (setf guard-ended t)))))
                      (dolist (exit '(:values :error :throw))
                        (setf body-ended nil guard-ended nil)
                        (let ((checked nil))
                          (flet ((finish ()
                                   (setf checked t)
                                   (ok guard-ended)
                                   (ok (pg::retirement-startup-busy-p owner))
                                   (ok (handler-case
                                           (progn
                                             (pg::call-with-physical-reseed-follow-retirement-operation
                                              owner (lambda () (error "unexpected reentry")))
                                             nil)
                                         (error (condition)
                                           (search "busy or uncertain" (princ-to-string condition)))))
                                   (ecase exit
                                     (:values (values :ignored :audit))
                                     (:error (error "final check cut"))
                                     (:throw (throw 'operation-final-check-cut :cut)))))
                            (case exit
                              (:values
                               (ok (equal '(:body :second)
                                          (multiple-value-list
                                           (pg::call-with-physical-reseed-follow-retirement-operation
                                            owner (lambda () (setf body-ended t) (values :body :second))
                                            #'finish)))))
                              (:error
                               (ok (handler-case
                                       (pg::call-with-physical-reseed-follow-retirement-operation
                                        owner (lambda () (setf body-ended t)) #'finish)
                                     (error (condition)
                                       (search "final check cut" (princ-to-string condition))))))
                              (:throw
                                  (ok (eq :cut
                                          (catch 'operation-final-check-cut
                                            (pg::call-with-physical-reseed-follow-retirement-operation
                                             owner (lambda () (setf body-ended t)) #'finish)))))))
                          (ok checked)
                          (ok (not (pg::retirement-startup-busy-p owner)))
                          (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner)))))
                 (setf (symbol-function 'pg::physical-reseed-follow-retirement-check-startup-owner)
                       original))))))))))

(deftest reseed-retirement-operation-latches-failure-before-releasing-admission
    (let* ((owner (pg::make-physical-reseed-follow-retirement-startup-owner
                   :directory (uiop:temporary-directory)))
           (original (symbol-function 'pg::physical-reseed-follow-retirement-check-startup-owner))
           (body-ended nil)
           (mode nil)
           (audit-called nil)
           (failure-called nil)
           (interrupt-delivered nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-check-startup-owner)
                   (lambda (current)
                     (when (and body-ended (eq mode :guard)) (error "guard cut"))
                     current))
             (dolist (exit '(:body :guard :audit :throw :interrupt :cleanup-interrupt))
               (setf mode exit body-ended nil audit-called nil failure-called nil interrupt-delivered nil
                     (pg::retirement-startup-uncertain-p owner) nil)
               (flet ((body ()
                        (when (eq mode :body) (error "body cut"))
                        (setf body-ended t))
                      (audit ()
                        (setf audit-called t)
                        (case mode
                          (:throw (throw 'operation-failure-cut :cut))
                          (:interrupt
                           (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                       (lambda () (error "audit interrupt"))))
                          (otherwise (error "audit cut"))))
                      (failure ()
                        (setf failure-called t)
                        (ok (pg::retirement-startup-busy-p owner))
                        (when (eq mode :cleanup-interrupt)
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda ()
                             (setf interrupt-delivered t)
                             (ok (pg::retirement-startup-uncertain-p owner))
                             (ok (not (pg::retirement-startup-busy-p owner))))))
                        (setf (pg::retirement-startup-uncertain-p owner) t)))
                 (ok (eq :cut
                         (catch 'operation-failure-cut
                           (handler-case
                               (pg::call-with-physical-reseed-follow-retirement-operation
                                owner #'body #'audit #'failure)
                             (error () :cut)))))
                 (ok failure-called)
                 (ok (eq (not (member mode '(:body :guard))) audit-called))
                 (ok (pg::retirement-startup-uncertain-p owner))
                 (ok (not (pg::retirement-startup-busy-p owner)))
                 (ok (eq (eq mode :cleanup-interrupt) interrupt-delivered)))))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-check-startup-owner) original))))

(deftest reseed-receipt-effects-require-one-use-durable-phase-claims
    (let* ((owner (pg::make-physical-reseed-follow-retirement-startup-owner))
           (witness (pg::make-physical-reseed-follow-receipt-witness :owner owner))
           (names '(pg::port-linkat pg::port-unlinkat sb-posix:fsync
                    pg::physical-reseed-follow-receipt-check-scope))
           (originals (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (calls 0))
      (unwind-protect
           (progn
             (dolist (entry originals)
               (setf (symbol-function (car entry))
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (incf calls)
                       (error "unexpected effect or parent callback"))))
             (dolist (case '((:link :follow :follow t t t t)
                             (:unlink :alias :alias t t t t)
                             (:link :linking :follow t t t t)
                             (:unlink :unlinking :alias t t t t)
                             (:link :follow-durable :follow nil t t t)
                             (:unlink :alias-durable :alias nil t t t)
                             (:sync :follow :follow nil t t t)
                             (:link :follow-durable :receipt t t t t)
                             (:link :follow-durable :follow t nil t t)
                             (:link :follow-durable :follow t t nil t)
                             (:link :follow-durable :follow t t t nil)))
               (destructuring-bind (operation phase geometry uncertain active busy same-thread) case
                 (setf (pg::retirement-startup-uncertain-p owner) uncertain
                       (pg::retirement-startup-busy-p owner) busy
                       (pg::receipt-witness-active-p witness) active
                       (pg::receipt-witness-thread witness) (when same-thread sb-thread:*current-thread*)
                       (pg::receipt-witness-phase witness) phase
                       (pg::receipt-witness-journal witness) (list :geometry geometry))
                 (ok (handler-case
                         (progn
                           (if (eq operation :sync)
                               (pg::physical-reseed-follow-receipt-sync witness :inode :root :after-inode :after-root)
                               (pg::physical-reseed-follow-receipt-transition witness operation))
                           nil)
                       (error (condition)
                         (search "cannot claim" (princ-to-string condition))))))
               (ok (zerop calls)))
             (setf (pg::retirement-startup-uncertain-p owner) t
                   (pg::retirement-startup-busy-p owner) t
                   (pg::receipt-witness-active-p witness) t
                   (pg::receipt-witness-thread witness) sb-thread:*current-thread*
                   (pg::receipt-witness-phase witness) :alias-durable
                   (pg::receipt-witness-journal witness) '(:geometry :alias))
             (ok (eq :unlinking
                     (pg::physical-reseed-follow-receipt-claim witness :alias-durable :unlinking)))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-receipt-claim witness :alias-durable :unlinking) nil)
                   (error () t)))
             (ok (zerop calls)))
        (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry))))))

(defun receipt-test-proof (mode inode delta)
  (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
    (declare (ignore descriptors))
    (remf state :pid-file)
    (multiple-value-bind (follow receipt geometry) (retired-receipt-test-rows mode inode 80)
      (dolist (row (remove nil (list follow receipt))) (incf (ninth row) delta))
      (let* ((row (or follow receipt))
             (root (cdr (assoc :root (getf (getf state :layout) :directories)))))
        (setf (getf (getf state :snapshot) :row) row
              (getf (getf state :snapshot) :identity) (pg::physical-reseed-install-row-file-facts row)
              (getf state :journal) (list :geometry geometry :follow follow :receipt receipt :mount 7)
              (getf (getf (getf state :layout) :names) :root)
              (append (when follow '("physical-reseed.follow"))
                      (when receipt '("physical-reseed.retired"))
                      '("postmaster.pid")))
        (incf (seventh root) delta)
        (incf (eighth root) delta)
        (incf (ninth root) delta)
        state))))

(cl-spec:defproperty reseed-receipt-normalization-preserves-all-unowned-facts
    ((mode (range integer 0 2)) (inode (range integer 1 1000000)) (delta (range integer 1 1000000)))
  (:about pg::physical-reseed-follow-receipt-stable-proof)
  (:trials (:normal 100))
  (let* ((before (receipt-test-proof 0 inode 0))
         (after (receipt-test-proof mode inode delta))
         (normal (pg::physical-reseed-follow-receipt-stable-proof before)))
    (and (equal normal (pg::physical-reseed-follow-receipt-stable-proof after))
         (progn
           (incf (getf (getf after :install) :digest))
           (not (equal normal (pg::physical-reseed-follow-receipt-stable-proof after))))
         (progn
           (incf (third (getf (getf normal :snapshot) :row)))
           (= inode (third (getf (getf before :snapshot) :row)))))))

(deftest generated-reseed-receipt-normalization-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-receipt-normalization-preserves-all-unowned-facts
                                       :seed 180 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-receipt-normalization-retains-every-stable-proof-field
    (let* ((proof (receipt-test-proof 1 7 100))
           (normal (pg::physical-reseed-follow-receipt-stable-proof proof)))
      (dolist (field '(1 2 3 5 6 7))
        (let ((copy (pg::physical-adoption-attempt-value-copy proof)))
          (incf (nth field (getf (getf copy :snapshot) :row)))
          (ok (not (equal normal (pg::physical-reseed-follow-receipt-stable-proof copy))))))
      (dolist (field '(0 1 2 4 5 6))
        (let ((copy (pg::physical-adoption-attempt-value-copy proof)))
          (incf (nth field (getf (getf copy :snapshot) :identity)))
          (ok (not (equal normal (pg::physical-reseed-follow-receipt-stable-proof copy))))))
      (dolist (change
               (list
                (lambda (copy) (incf (getf (getf (getf copy :snapshot) :record) :generation)))
                (lambda (copy) (incf (getf (getf copy :install) :digest)))
                (lambda (copy) (incf (getf (getf copy :attempt) :digest)))
                (lambda (copy) (incf (getf (getf copy :journal) :mount)))
                (lambda (copy) (incf (sixth (cdr (assoc :root (getf (getf copy :layout) :directories))))))
                (lambda (copy) (incf (ninth (cdr (assoc :saved (getf (getf copy :layout) :directories))))))
                (lambda (copy) (push "foreign" (getf (getf (getf copy :layout) :names) :root)))
                (lambda (copy) (push "physical-reseed.retired" (getf (getf (getf copy :layout) :names) :saved)))))
        (let ((copy (pg::physical-adoption-attempt-value-copy proof)))
          (funcall change copy)
          (ok (not (equal normal (pg::physical-reseed-follow-receipt-stable-proof copy))))))))

(defun call-with-receipt-test-counted-syncs (function)
  (let ((original (symbol-function 'sb-posix:fsync)) (calls 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'sb-posix:fsync)
                 (lambda (fd) (incf calls) (funcall original fd)))
           (values (funcall function) calls))
      (setf (symbol-function 'sb-posix:fsync) original))))

(deftest reseed-retirement-publishes-and-recovers-owned-receipts
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-retired-startup-test-fixture
       kind
       (lambda (root workspace root-fd record)
         (declare (ignore workspace))
         (dolist (geometry '(:follow :alias :receipt))
           (reset-retired-reader-test-files root record geometry)
           (call-with-retired-startup-test-context
            root
            (lambda (before after)
              (declare (ignore before))
              (pg::call-with-physical-reseed-follow-retirement-startup-owner
               after
               (lambda (owner)
                 (multiple-value-bind (evidence syncs)
                     (call-with-receipt-test-counted-syncs
                      (lambda () (pg::physical-reseed-follow-retirement-publish-receipt owner)))
                   (ok (= syncs (ecase geometry (:follow 6) (:alias 4) (:receipt 2))))
                   (ok (eq :receipt (getf (getf evidence :journal) :geometry)))
                   (ok (equal evidence (pg::retirement-startup-evidence owner)))
                   (ok (not (pg::retirement-startup-uncertain-p owner)))
                   (ok (not (pg::retirement-startup-busy-p owner)))
                   (ok (not (probe-file (merge-pathnames "physical-reseed.follow" root))))
                   (ok (= 1 (sb-posix:stat-nlink
                             (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.retired" root))))))
                   (pg::call-with-physical-reseed-follow-retirement-startup-observation
                    owner
                    (lambda (descriptors state verify)
                      (ok (equal evidence (pg::physical-reseed-follow-retired-startup-projection
                                           descriptors state)))
                      (ok (funcall verify))))
                   (multiple-value-bind (retry retry-syncs)
                       (call-with-receipt-test-counted-syncs
                        (lambda () (pg::physical-reseed-follow-retirement-publish-receipt owner)))
                     (ok (equal evidence retry))
                     (ok (= 2 retry-syncs)))
                   (ok (pg::postmaster-pid-owned-p)))))))
           (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
           (ok (sb-posix:fstat root-fd)))))))

(defun check-receipt-effect-syscall-failure (root root-fd record case)
  (destructuring-bind (geometry operation point after-real expected role) case
    (reset-retired-reader-test-files root record geometry)
    (let* ((source (pg::physical-reseed-follow-retired-journal-at root-fd (constantly t)))
           (original-row (getf (getf source :snapshot) :row))
           (run (symbol-function 'pg::physical-reseed-follow-receipt-run))
           (syscall (symbol-function operation))
           (captured nil) (armed nil) (seen 0) (failures 0) (failed-fd nil)
           (saved (merge-pathnames "saved/" (pathname
                                             (getf (getf (getf record :configuration) :workspace) :directory)))))
      (ok
       (eq :checked
           (catch 'receipt-effect-failure-checked
             (call-with-retired-startup-test-context
              root
              (lambda (before after)
                (declare (ignore before))
                (pg::call-with-physical-reseed-follow-retirement-startup-owner
                 after
                 (lambda (owner)
                   (unwind-protect
                        (progn
                          (setf (symbol-function 'pg::physical-reseed-follow-receipt-run)
                                (lambda (witness) (setf captured witness) (funcall run witness))
                                (symbol-function operation)
                                (lambda (&rest arguments)
                                  (when armed
                                    (setf armed nil)
                                    (let* ((held-root (pg::receipt-witness-root captured))
                                           (held-file (sb-sys:fd-stream-fd (pg::receipt-witness-stream captured)))
                                           (expected-arguments
                                             (ecase role
                                               (:file (list held-file))
                                               (:root (list held-root))
                                               (:link (list held-root "physical-reseed.follow"
                                                            held-root "physical-reseed.retired" 0))
                                               (:unlink (list held-root "physical-reseed.follow" 0)))))
                                      (unless (equal arguments expected-arguments)
                                        (error "wrong receipt syscall injection site"))
                                      (setf failed-fd (first arguments))
                                      (incf failures)
                                      (when after-real
                                        (unless (zerop (apply syscall arguments))
                                          (error "real receipt namespace syscall failed before injection")))
                                      (error 'sb-posix:syscall-error :name operation :errno sb-posix:eio)))
                                  (apply syscall arguments)))
                          (let ((pg::*physical-reseed-follow-test-hook*
                                  (lambda (event path)
                                    (declare (ignore path))
                                    (when (eq event point) (incf seen) (setf armed t)))))
                            (ok (handler-case
                                    (progn (pg::physical-reseed-follow-retirement-publish-receipt owner) nil)
                                  (sb-posix:syscall-error (condition)
                                    (= sb-posix:eio (sb-posix:syscall-errno condition))))))
                          (ok (= 1 seen failures))
                          (ok (not (pg::retirement-startup-busy-p owner)))
                          (ok (pg::retirement-startup-uncertain-p owner))
                          (ok (equal (getf after :evidence) (pg::retirement-startup-evidence owner)))
                          (ok (handler-case
                                  (progn (pg::physical-reseed-follow-retirement-publish-receipt owner) nil)
                                (error (condition)
                                  (search "busy or uncertain" (princ-to-string condition)))))
                          (ok (handler-case (progn (sb-posix:fstat failed-fd) nil)
                                (sb-posix:syscall-error () t)))
                          (ok (pg::postmaster-pid-owned-p)))
                     (setf (symbol-function operation) syscall
                           (symbol-function 'pg::physical-reseed-follow-receipt-run) run))
                   (throw 'receipt-effect-failure-checked :checked))))))))
      (let* ((actual (pg::physical-reseed-follow-retired-journal-at root-fd (constantly t)))
             (row (getf (getf actual :snapshot) :row)))
        (ok (eq expected (getf actual :geometry)))
        (ok (equal record (getf (getf actual :snapshot) :record)))
        (ok (equalp (getf source :bytes) (getf actual :bytes)))
        (ok (equal (subseq original-row 1 4) (subseq row 1 4)))
        (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
        (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))
      (call-with-retired-startup-test-context
       root
       (lambda (before after)
         (declare (ignore before))
         (pg::call-with-physical-reseed-follow-retirement-startup-owner
          after
          (lambda (owner)
            (let ((evidence (pg::physical-reseed-follow-retirement-publish-receipt owner)))
              (ok (eq :receipt (getf (getf evidence :journal) :geometry)))
              (ok (not (pg::retirement-startup-uncertain-p owner)))
              (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner)))))))
      (ok (probe-file (merge-pathnames "physical-reseed.install" saved)))
      (ok (probe-file (merge-pathnames "physical-reseed.attempt" saved)))
      (ok (sb-posix:fstat root-fd)))))

(deftest reseed-receipt-fences-each-inode-and-root-sync-failure
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (case '((:follow sb-posix:fsync :before-retirement-receipt-source-fsync nil :follow :file)
                       (:follow sb-posix:fsync :before-retirement-receipt-source-root-fsync nil :follow :root)
                       (:follow sb-posix:fsync :before-retirement-receipt-alias-fsync nil :alias :file)
                       (:follow sb-posix:fsync :before-retirement-receipt-alias-root-fsync nil :alias :root)
                       (:follow sb-posix:fsync :before-retirement-receipt-final-fsync nil :receipt :file)
                       (:follow sb-posix:fsync :before-retirement-receipt-final-root-fsync nil :receipt :root)))
         (check-receipt-effect-syscall-failure root root-fd record case)))))

(deftest reseed-receipt-fences-namespace-failure-before-and-after-real-effects
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (case '((:follow pg::port-linkat :before-retirement-receipt-link nil :follow :link)
                       (:follow pg::port-linkat :before-retirement-receipt-link t :alias :link)
                       (:follow pg::port-unlinkat :before-retirement-receipt-unlink nil :alias :unlink)
                       (:follow pg::port-unlinkat :before-retirement-receipt-unlink t :receipt :unlink)))
         (check-receipt-effect-syscall-failure root root-fd record case)))))

(deftest reseed-receipt-recovery-repeats-alias-and-final-durability-barriers
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (case '((:alias sb-posix:fsync :before-retirement-receipt-alias-fsync nil :alias :file)
                       (:alias sb-posix:fsync :before-retirement-receipt-alias-root-fsync nil :alias :root)
                       (:receipt sb-posix:fsync :before-retirement-receipt-final-fsync nil :receipt :file)
                       (:receipt sb-posix:fsync :before-retirement-receipt-final-root-fsync nil :receipt :root)))
         (check-receipt-effect-syscall-failure root root-fd record case)))))

(defun receipt-test-crash-child-arguments (root point cache marker &optional (operation :receipt))
  (let* ((arguments (retirement-intent-test-child-arguments root point cache marker))
         (form
           `(let ((pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event ,point)
                        (with-open-file (out ,marker :direction :output :if-exists :error)
                          (write (list (sb-posix:getpid) ,point) :stream out :readably t))
                        (format t "~%RECEIPT-CRASH-REACHED ~d ~s~%" (sb-posix:getpid) ,point)
                        (finish-output)
                        (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                        (error "receipt child SIGKILL returned")))))
              (format t "~%RETIREMENT-CRASH-CHILD ~d ~s ~s~%" (sb-posix:getpid) ,root ,point)
              (finish-output)
              (call-with-retired-startup-test-context
               ,root
               (lambda (before after)
                 (declare (ignore before))
                 (pg::call-with-physical-reseed-follow-retirement-startup-owner
                  after
                  ,(ecase operation
                     (:receipt '(function pg::physical-reseed-follow-retirement-publish-receipt))
                     (:receipt-consumption '(function pg::physical-reseed-follow-retirement-consume-receipt))))))
              (error "receipt child did not reach its crash point"))))
    (let ((*package* (find-package :cl-user))
          (*print-length* nil) (*print-level* nil) (*print-readably* t))
      (append (butlast arguments 2) (list "--eval" (write-to-string form))))))

(deftest reseed-receipt-survives-native-process-crashes-at-every-durability-boundary
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (let ((baseline (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                             (reseed-mapped-test-tree-facts workspace))))
         (with-physical-store-directory (driver)
           (let ((cache (merge-pathnames "cache/" driver)))
             (dolist (case '((:after-retirement-receipt-source-fsync :follow)
                             (:after-retirement-receipt-source-root-fsync :follow)
                             (:after-retirement-receipt-link :alias)
                             (:after-retirement-receipt-alias-fsync :alias)
                             (:after-retirement-receipt-alias-root-fsync :alias)
                             (:after-retirement-receipt-unlink :receipt)
                             (:after-retirement-receipt-final-fsync :receipt)
                             (:after-retirement-receipt-final-root-fsync :receipt)))
               (destructuring-bind (point expected) case
                 (reset-retired-reader-test-files root record :follow)
                 (let* ((marker (merge-pathnames (format nil "~(~a~).marker" point) driver))
                        (pid (run-retirement-intent-test-crash-child
                              root point cache marker :operation :receipt :timeout 1200))
                        (stale (pg::physical-reseed-read-pid root))
                        (source (pg::physical-reseed-follow-retired-journal-at root-fd (constantly t))))
                   (ok (= pid (getf stale :pid)))
                   (ok (eq expected (getf source :geometry)))
                   (ok (equal record (getf (getf source :snapshot) :record)))
                   (ok (equalp (pg::physical-reseed-follow-image record) (getf source :bytes)))
                   (ok (reseed-activation-layout-facts-equal-p
                        baseline
                        (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                              (reseed-mapped-test-tree-facts workspace))))
                   (call-with-retired-startup-test-context
                    root
                    (lambda (before after)
                      (declare (ignore before))
                      (pg::call-with-physical-reseed-follow-retirement-startup-owner
                       after
                       (lambda (owner)
                         (let ((evidence (pg::physical-reseed-follow-retirement-publish-receipt owner)))
                           (ok (eq :receipt (getf (getf evidence :journal) :geometry)))
                           (ok (not (pg::retirement-startup-uncertain-p owner)))
                           (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner))
                           (ok (pg::postmaster-pid-owned-p)))))))
                   (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
                   (ok (reseed-activation-layout-facts-equal-p
                        baseline
                        (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                              (reseed-mapped-test-tree-facts workspace))))
                   (ok (sb-posix:fstat root-fd)))))))))))

(defun check-receipt-finalization-cut (root root-fd record mode)
  (reset-retired-reader-test-files root record :receipt)
  (let* ((saved (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
         (archive (merge-pathnames "saved/physical-reseed.attempt" saved))
         (bytes (lineage-test-bytes archive))
         (stat (sb-posix:stat (namestring archive)))
         (run (symbol-function 'pg::physical-reseed-follow-receipt-run))
         (pid-reader (symbol-function 'pg::physical-reseed-read-pid))
         (stamp-check (symbol-function 'pg::physical-reseed-follow-retirement-check-stamp))
         (captured nil) (fired nil) (closed nil))
    (unwind-protect
         (ok
          (eq :checked
              (catch 'receipt-finalization-checked
                (call-with-retired-startup-test-context
                 root
                 (lambda (before after)
                   (declare (ignore before))
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    after
                    (lambda (owner)
                      (flet ((finishing ()
                               (and captured (not fired)
                                    (not (pg::receipt-witness-active-p captured))
                                    (pg::retirement-startup-busy-p owner)
                                    (not (pg::retirement-startup-uncertain-p owner))))
                             (cut ()
                               (setf fired t
                                     closed (and (not (open-stream-p (pg::receipt-witness-stream captured)))
                                                 (handler-case
                                                     (progn (sb-posix:fstat (pg::receipt-witness-root captured)) nil)
                                                   (sb-posix:syscall-error () t))))
                               (ecase mode
                                 (:pid-close-mutation
                                  (let ((changed (copy-seq bytes)))
                                    (setf (aref changed 0) (logxor 1 (aref changed 0)))
                                    (with-open-file (stream archive :direction :io
                                                                    :element-type '(unsigned-byte 8)
                                                                    :if-exists :overwrite)
                                      (write-sequence changed stream)
                                      (finish-output stream))))
                                 (:error (error "receipt final audit cut"))
                                 (:throw (throw 'receipt-final-audit-cut :cut))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'receipt-final-audit-cut :cut)))))))
                        (unwind-protect
                             (progn
                               (setf (symbol-function 'pg::physical-reseed-follow-receipt-run)
                                     (lambda (witness) (setf captured witness) (funcall run witness))
                                     (symbol-function 'pg::physical-reseed-read-pid)
                                     (lambda (directory)
                                       (multiple-value-prog1 (funcall pid-reader directory)
                                         (when (and (eq mode :pid-close-mutation) (finishing)) (cut))))
                                     (symbol-function 'pg::physical-reseed-follow-retirement-check-stamp)
                                     (lambda (stamp)
                                       (when (and (not (eq mode :pid-close-mutation)) (finishing)) (cut))
                                       (funcall stamp-check stamp)))
                               (ok (eq :cut
                                       (catch 'receipt-final-audit-cut
                                         (handler-case
                                             (progn (pg::physical-reseed-follow-retirement-publish-receipt owner) nil)
                                           (error (condition)
                                             (if (search (if (eq mode :pid-close-mutation)
                                                             "changed during resource cleanup"
                                                             "receipt final audit cut")
                                                         (princ-to-string condition))
                                                 :cut
                                                 (error condition)))))))
                               (ok fired)
                               (ok closed)
                               (ok (pg::retirement-startup-uncertain-p owner))
                               (ok (not (pg::retirement-startup-busy-p owner)))
                               (ok (eq :receipt (getf (getf (pg::retirement-startup-evidence owner) :journal)
                                                      :geometry)))
                               (ok (pg::postmaster-pid-owned-p)))
                          (setf (symbol-function 'pg::physical-reseed-follow-receipt-run) run
                                (symbol-function 'pg::physical-reseed-read-pid) pid-reader
                                (symbol-function 'pg::physical-reseed-follow-retirement-check-stamp) stamp-check)))
                      (throw 'receipt-finalization-checked :checked))))))))
      (when (eq mode :pid-close-mutation)
        (restore-retirement-cleanup-test-file archive bytes stat)))
    (when (eq mode :pid-close-mutation)
      (ok (= (length bytes) (length (lineage-test-bytes archive)))))
    (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
    (call-with-retired-startup-test-context
     root
     (lambda (before after)
       (declare (ignore before))
       (pg::call-with-physical-reseed-follow-retirement-startup-owner
        after
        (lambda (owner)
          (let ((evidence (pg::physical-reseed-follow-retirement-publish-receipt owner)))
            (ok (eq :receipt (getf (getf evidence :journal) :geometry)))
            (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner)))))))
    (ok (sb-posix:fstat root-fd))))

(deftest reseed-receipt-finalization-fences-last-pid-close-and-final-audit-nonlocal-exits
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (mode '(:pid-close-mutation :error :throw :interrupt))
         (check-receipt-finalization-cut root root-fd record mode)))))

(defun check-retired-layout-pid-geometries (kinds)
  (dolist (kind kinds)
    (call-with-retired-reader-test-fixture
     kind
     (lambda (root root-fd record check)
       (let ((follow (merge-pathnames "physical-reseed.follow" root))
             (receipt (merge-pathnames "physical-reseed.retired" root)))
         (dolist (geometry '(:follow :alias :receipt))
           (ecase geometry
             (:follow)
             (:alias (sb-posix:link (namestring follow) (namestring receipt)))
             (:receipt (delete-file follow)))
           (let ((before nil) (escaped nil) (held nil))
             (pg::call-with-physical-reseed-follow-retired-before-pid-observation
              root-fd check
              (lambda (descriptors state verify)
                (setf before (pg::physical-reseed-follow-retired-startup-projection descriptors state)
                      escaped verify held (copy-tree descriptors))
                (ok (= 10 (length descriptors)))
                (ok (= 2 (getf state :prefix)))
                (ok (eq geometry (getf (getf state :journal) :geometry)))
                (ok (equal record (getf (getf state :snapshot) :record)))
                (ok (getf state :install))
                (ok (getf state :attempt))
                (ok (getf (getf state :layout) :active))
                (ok (funcall verify))
                (setf (getf (getf state :journal) :geometry) :forged
                      (getf (getf state :snapshot) :record) nil)
                (ok (funcall verify))))
             (ok (handler-case (progn (funcall escaped) nil) (error () t)))
             (ok (sb-posix:fstat root-fd))
             (dolist (entry held)
               (unless (eq :root (car entry))
                 (ok (handler-case (progn (sb-posix:fstat (cdr entry)) nil) (sb-posix:syscall-error () t)))))
             (call-with-reseed-retirement-test-owned-pid
              root
              (lambda (stream)
                (let ((position (file-position stream)))
                  (pg::call-with-physical-reseed-follow-retired-owned-pid-observation
                   root-fd check
                   (lambda (descriptors state verify)
                     (ok (equal before (pg::physical-reseed-follow-retired-startup-projection descriptors state)))
                     (ok (= (sb-posix:getpid) (getf (getf state :pid-file) :pid)))
                     (ok (funcall verify))))
                  (ok (open-stream-p stream))
                  (ok (eql position (file-position stream))))))
             (ok (equal record
                        (getf (getf (pg::physical-reseed-follow-retired-journal-at root-fd check)
                                    :snapshot) :record))))))))))

(deftest reseed-retired-layout-proves-all-geometries-across-pid-boundaries
    (check-retired-layout-pid-geometries
     '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer)))

(deftest reseed-retired-layout-refuses-unexpected-names-and-missing-archives
    (dolist (geometry '(:follow :alias :receipt))
      (call-with-retired-reader-test-fixture
       :same-timeline
       (lambda (root root-fd record check)
         (let* ((follow (merge-pathnames "physical-reseed.follow" root))
                (receipt (merge-pathnames "physical-reseed.retired" root))
                (source (if (eq geometry :receipt) receipt follow))
                (workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
                (saved (merge-pathnames "saved/" workspace))
                (pid-path (merge-pathnames "postmaster.pid" root)))
           (unless (eq geometry :follow)
             (sb-posix:link (namestring follow) (namestring receipt)))
           (when (eq geometry :receipt) (delete-file follow))
           (reseed-follow-owner-test-write
            pid-path (sb-ext:string-to-octets (format nil "2147483647~%") :external-format :utf-8))
           (let ((pid-bytes (lineage-test-bytes pid-path))
                 (pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid-path)))))
             (dolist (fault '(:unknown :temporary :third-link :missing-install :missing-attempt))
               (let* ((name (case fault
                              (:unknown "unexpected")
                              (:temporary "physical-reseed.follow.tmp")
                              (:third-link "third-link")
                              (:missing-install "physical-reseed.install")
                              (:missing-attempt "physical-reseed.attempt")))
                      (archive-p (member fault '(:missing-install :missing-attempt)))
                      (original (merge-pathnames name (if archive-p saved root)))
                      (moved (merge-pathnames "staged/missing-archive" workspace))
                      (entered nil) (message nil))
                 (unwind-protect
                      (progn
                        (cond (archive-p (sb-posix:rename (namestring original) (namestring moved)))
                              ((eq fault :third-link) (sb-posix:link (namestring source) (namestring original)))
                              (t (reseed-follow-owner-test-write original #(1 2 3))))
                        (setf message
                              (handler-case
                                  (progn
                                    (pg::call-with-physical-reseed-follow-retired-before-pid-observation
                                     root-fd check
                                     (lambda (descriptors state verify)
                                       (declare (ignore descriptors state verify))
                                       (setf entered t)))
                                    nil)
                                (error (condition) (princ-to-string condition))))
                        (ng entered)
                        (ok (and message
                                 (search (case fault
                                           (:temporary "refuses a follow temporary")
                                           (:third-link "exact follow and receipt link geometry")
                                           (:missing-install "one root or saved location")
                                           (t "unique complete archive prefix"))
                                         message)))
                        (ok (equalp pid-bytes (lineage-test-bytes pid-path)))
                        (ok (= pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid-path)))))
                        (ok (sb-posix:fstat root-fd)))
                   (if archive-p
                       (sb-posix:rename (namestring moved) (namestring original))
                       (when (probe-file original) (delete-file original))))))
             (delete-file pid-path)))))))

(defun check-retired-layout-lifetime (root-fd check reader borrowed)
  (dolist (mode '(:normal :error :throw :interrupt))
    (let ((escaped nil) (held nil) (entered nil) (source-fd nil) (pid-fd nil)
                        (position (when borrowed (file-position borrowed)))
                        (original (symbol-function 'pg::physical-image-openat)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-image-openat)
                   (lambda (fd name flags permission)
                     (let ((opened (funcall original fd name flags permission)))
                       (cond ((member name '("physical-reseed.follow" "physical-reseed.retired") :test #'equal)
                              (setf source-fd opened))
                             ((equal name "postmaster.pid") (setf pid-fd opened)))
                       opened)))
             (let ((outcome
                     (catch 'retired-layout-cut
                       (handler-case
                           (multiple-value-list
                            (funcall reader root-fd check
                                     (lambda (descriptors state verify)
                                       (declare (ignore state))
                                       (setf entered t escaped verify held (copy-tree descriptors))
                                       (ecase mode
                                         (:normal (values :first :second))
                                         (:error (error "retired layout lifetime cut"))
                                         (:throw (throw 'retired-layout-cut :cut))
                                         (:interrupt
                                          (sb-thread:interrupt-thread
                                           sb-thread:*current-thread*
                                           (lambda () (throw 'retired-layout-cut :cut))))))))
                         (error (condition)
                           (if (search "retired layout lifetime cut" (princ-to-string condition))
                               :error :wrong-error))))))
               (ok (equal outcome (case mode (:normal '(:first :second)) (:error :error) (otherwise :cut)))))
             (ok entered)
             (ok (and source-fd
                      (handler-case (progn (sb-posix:fstat source-fd) nil) (sb-posix:syscall-error () t))))
             (ok (or (null pid-fd)
                     (handler-case (progn (sb-posix:fstat pid-fd) nil) (sb-posix:syscall-error () t))))
             (ok (handler-case (progn (funcall escaped) nil)
                   (error (condition) (search "scope ended or changed thread" (princ-to-string condition)))))
             (ok (sb-posix:fstat root-fd))
             (dolist (entry held)
               (unless (eq :root (car entry))
                 (ok (handler-case (progn (sb-posix:fstat (cdr entry)) nil)
                       (sb-posix:syscall-error () t)))))
             (when borrowed
               (ok (open-stream-p borrowed))
               (ok (eql position (file-position borrowed)))))
        (setf (symbol-function 'pg::physical-image-openat) original)))))

(deftest reseed-retired-layout-revokes-verifiers-and-closes-resources-on-every-exit
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (geometry '(:follow :alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (check-retired-layout-lifetime
          root-fd check #'pg::call-with-physical-reseed-follow-retired-before-pid-observation nil)
         (call-with-reseed-retirement-test-owned-pid
          root
          (lambda (stream)
            (check-retired-layout-lifetime
             root-fd check #'pg::call-with-physical-reseed-follow-retired-owned-pid-observation stream)))))))

(deftest reseed-retired-layout-rechecks-inventory-inside-the-full-callback
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (geometry '(:follow :alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (let ((extra (merge-pathnames "unexpected-during-observation" root)))
           (ok
            (handler-case
                (progn
                  (pg::call-with-physical-reseed-follow-retired-before-pid-observation
                   root-fd check
                   (lambda (descriptors state verify)
                     (declare (ignore descriptors state))
                     (ok (funcall verify))
                     (unwind-protect
                          (progn
                            (reseed-follow-owner-test-write extra #(1 2 3))
                            (ok (probe-file extra))
                            (ok (handler-case (progn (funcall verify) nil)
                                  (error (condition)
                                    (search "unique complete archive prefix" (princ-to-string condition))))))
                       (when (probe-file extra) (delete-file extra)))))
                  nil)
              (error (condition)
                (search "retirement layout changed during its observation scope" (princ-to-string condition)))))
           (pg::call-with-physical-reseed-follow-retired-before-pid-observation
            root-fd check
            (lambda (descriptors state verify)
              (declare (ignore descriptors state))
              (ok (funcall verify)))))))))

(deftest reseed-retired-observation-refuses-archive-mutation-during-store-close
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (reset-retired-reader-test-files root record :receipt)
       (let* ((workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
              (archive (merge-pathnames "saved/physical-reseed.attempt" workspace))
              (bytes (lineage-test-bytes archive))
              (original (symbol-function 'pg::physical-store-close))
              (fired nil) (returned nil) (message nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-store-close)
                      (lambda (store)
                        (multiple-value-prog1 (funcall original store)
                          (unless fired
                            (with-open-file (output archive :direction :io :if-exists :overwrite
                                                            :element-type '(unsigned-byte 8))
                              (write-byte (logxor 1 (aref bytes 0)) output))
                            (setf fired t)))))
                (setf message
                      (handler-case
                          (progn
                            (pg::call-with-physical-reseed-follow-retired-before-pid-observation
                             root-fd check
                             (lambda (descriptors state verify)
                               (declare (ignore descriptors state))
                               (funcall verify)))
                            (setf returned t)
                            nil)
                        (error (condition) (princ-to-string condition)))))
           (setf (symbol-function 'pg::physical-store-close) original))
         (ok fired)
         (ok (= (length bytes) (length (lineage-test-bytes archive))))
         (ng (equalp bytes (lineage-test-bytes archive)))
         (ok (and message (search "retirement observation changed during resource cleanup" message)))
         (ng returned)
         (ok (sb-posix:fstat root-fd))))))

(defun restore-retirement-cleanup-test-file (path bytes stat)
  (with-open-file (output path :direction :io :if-exists :overwrite
                               :element-type '(unsigned-byte 8))
    (write-sequence bytes output))
  (sb-posix:utime (namestring path) (sb-posix:stat-atime stat) (sb-posix:stat-mtime stat)))

(defun check-retirement-cleanup-test-cut (cut root-fd record check)
  (let* ((workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
         (archive (merge-pathnames (if (member cut '(:guard :pid))
                                       "saved/physical-reseed.install" "saved/physical-reseed.attempt")
                                   workspace))
         (bytes (lineage-test-bytes archive))
         (stat (sb-posix:lstat (namestring archive)))
         (close-store (symbol-function 'pg::physical-store-close))
         (guard (symbol-function 'pg::call-with-physical-reseed-install-guard))
         (pid-scope (symbol-function 'pg::call-with-physical-reseed-follow-retirement-before-pid))
         (old-hook pg::*physical-reseed-follow-test-hook*)
         (fired nil) (closed-guard nil) (returned nil) (message nil))
    (flet ((mutate ()
             (unless fired
               (with-open-file (output archive :direction :io :if-exists :overwrite
                                               :element-type '(unsigned-byte 8))
                 (write-byte (logxor 1 (aref bytes 0)) output))
               (setf fired t))))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-store-close)
                   (lambda (store)
                     (multiple-value-prog1 (funcall close-store store)
                       (when (eq cut :store) (mutate))))
                   (symbol-function 'pg::call-with-physical-reseed-install-guard)
                   (lambda (fd name prepare function)
                     (let ((stream nil))
                       (multiple-value-prog1
                           (funcall guard fd name prepare
                                    (lambda (held) (setf stream held) (funcall function held)))
                         (when (and (eq cut :guard) (not fired))
                           (setf closed-guard (not (open-stream-p stream)))
                           (mutate)))))
                   (symbol-function 'pg::call-with-physical-reseed-follow-retirement-before-pid)
                   (lambda (fd parent function)
                     (multiple-value-prog1 (funcall pid-scope fd parent function)
                       (when (eq cut :pid) (mutate))))
                   pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (when old-hook (funcall old-hook point path))
                     (when (and (eq cut :source) (eq point :after-retired-journal-close))
                       (mutate))))
             (setf message
                   (handler-case
                       (progn
                         (pg::call-with-physical-reseed-follow-retired-before-pid-observation
                          root-fd check
                          (lambda (descriptors state verify)
                            (declare (ignore descriptors state))
                            (funcall verify)))
                         (setf returned t)
                         nil)
                     (error (condition) (princ-to-string condition)))))
        (setf (symbol-function 'pg::physical-store-close) close-store
              (symbol-function 'pg::call-with-physical-reseed-install-guard) guard
              (symbol-function 'pg::call-with-physical-reseed-follow-retirement-before-pid) pid-scope
              pg::*physical-reseed-follow-test-hook* old-hook))
      (unwind-protect
           (progn
             (ok fired)
             (ok (= (length bytes) (length (lineage-test-bytes archive))))
             (ng (equalp bytes (lineage-test-bytes archive)))
             (ok (and message (search "retirement observation changed during resource cleanup" message)))
             (ng returned)
             (when (eq cut :guard) (ok closed-guard))
             (ok (sb-posix:fstat root-fd)))
        (restore-retirement-cleanup-test-file archive bytes stat))
      (pg::call-with-physical-reseed-follow-retired-before-pid-observation
       root-fd check
       (lambda (descriptors state verify)
         (declare (ignore descriptors state))
         (ok (funcall verify)))))))

(deftest reseed-retired-observation-audits-archives-after-every-resource-layer
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (reset-retired-reader-test-files root record :receipt)
       (let ((pid (merge-pathnames "postmaster.pid" root)))
         (reseed-follow-owner-test-write
          pid (sb-ext:string-to-octets (format nil "2147483647~%") :external-format :utf-8))
         (let ((bytes (lineage-test-bytes pid))
               (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
           (dolist (cut '(:store :guard :pid :source))
             (check-retirement-cleanup-test-cut cut root-fd record check)
             (ok (equalp bytes (lineage-test-bytes pid)))
             (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))))
           (delete-file pid))))))

(defun check-retirement-root-cleanup-cut (root root-fd record published target)
  (let* ((workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
         (archive (merge-pathnames "saved/physical-reseed.attempt" workspace))
         (bytes (lineage-test-bytes archive))
         (stat (sb-posix:lstat (namestring archive)))
         (pid (merge-pathnames "postmaster.pid" root))
         (name (if published
                   'pg::call-with-physical-reseed-follow-retirement-startup-root
                   'pg::call-with-physical-directory-root))
         (original (symbol-function name))
         (count 0) (fired nil) (closed nil) (entered nil) (inner nil) (message nil)
         (pid-bytes nil) (pid-inode nil))
    (unless published
      (when (probe-file pid) (delete-file pid))
      (reseed-follow-owner-test-write
       pid (sb-ext:string-to-octets (format nil "2147483647~%") :external-format :utf-8))
      (setf pid-bytes (lineage-test-bytes pid)
            pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
    (unwind-protect
         (progn
           (setf (symbol-function name)
                 (lambda (owner function)
                   (let ((fd nil))
                     (incf count)
                     (multiple-value-prog1
                         (funcall original owner
                                  (lambda (&rest arguments)
                                    (setf fd (first arguments))
                                    (apply function arguments)))
                       (when (= count target)
                         (setf closed
                               (handler-case (progn (sb-posix:fstat fd) nil)
                                 (sb-posix:syscall-error () t)))
                         (with-open-file (output archive :direction :io :if-exists :overwrite
                                                         :element-type '(unsigned-byte 8))
                           (write-byte (logxor 1 (aref bytes 0)) output))
                         (setf fired t))))))
           (setf message
                 (handler-case
                     (progn
                       (call-with-retired-startup-test-context
                        root
                        (lambda (before after)
                          (declare (ignore before))
                          (if published
                              (pg::call-with-physical-reseed-follow-retirement-startup-owner
                               after
                               (lambda (owner)
                                 (setf entered t)
                                 (when (= target 2)
                                   (pg::call-with-physical-reseed-follow-retirement-startup-observation
                                    owner
                                    (lambda (descriptors state verify)
                                      (declare (ignore descriptors state))
                                      (setf inner t)
                                      (funcall verify))))))
                              (setf entered t))))
                       nil)
                   (error (condition) (princ-to-string condition)))))
      (setf (symbol-function name) original))
    (unwind-protect
         (progn
           (ok fired)
           (ok closed)
           (ok (= count target))
           (ok (eql (and published (= target 2)) entered))
           (ok (eql (and published (= target 2)) inner))
           (ok (and message (search "retirement observation changed during resource cleanup" message)))
           (ok (= (length bytes) (length (lineage-test-bytes archive))))
           (ng (equalp bytes (lineage-test-bytes archive)))
           (if (and (not published) (= target 1))
               (progn
                 (ok (equalp pid-bytes (lineage-test-bytes pid)))
                 (ok (= pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
                 (delete-file pid))
               (ng (probe-file pid)))
           (ok (sb-posix:fstat root-fd)))
      (restore-retirement-cleanup-test-file archive bytes stat))
    (call-with-retired-startup-test-context
     root
     (lambda (before after)
       (ok (equal before after))
       (pg::call-with-physical-reseed-follow-retirement-startup-owner
        after (lambda (owner) (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner))))))))

(deftest reseed-retired-startup-audits-archives-after-both-root-cleanup-boundaries
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :receipt)
       (dolist (published '(nil t))
         (dolist (target '(1 2))
           (check-retirement-root-cleanup-cut root root-fd record published target))))))

(deftest reseed-retirement-cleanup-path-facts-ignore-parent-activity-and-refuse-symlinks
    (with-physical-store-directory (root)
      (let* ((parent (merge-pathnames "parent/" root))
             (child (merge-pathnames "child/" parent))
             (moved (merge-pathnames "moved/" root)))
        (sb-posix:mkdir (namestring parent) #o700)
        (sb-posix:mkdir (namestring child) #o700)
        (let* ((before (pg::physical-reseed-follow-retirement-path-facts (namestring child)))
               (links (sb-posix:stat-nlink (sb-posix:lstat (namestring parent))))
               (mount (pg::physical-adoption-install-mount-id child))
               (row (pg::physical-reseed-follow-retirement-named-row (namestring child) "." mount)))
          (sb-posix:mkdir (namestring (merge-pathnames "neighbor/" parent)) #o700)
          (ok (/= links (sb-posix:stat-nlink (sb-posix:lstat (namestring parent)))))
          (ok (equal before (pg::physical-reseed-follow-retirement-path-facts (namestring child))))
          (sb-posix:rename (namestring parent) (namestring moved))
          (unwind-protect
               (progn
                 (sb-posix:symlink (string-right-trim "/" (namestring moved))
                                   (string-right-trim "/" (namestring parent)))
                 (ok (sb-posix:s-islnk (sb-posix:stat-mode
                                        (sb-posix:lstat (string-right-trim "/" (namestring parent))))))
                 (ok (equal row (pg::physical-reseed-follow-retirement-named-row (namestring child) "." mount)))
                 (ok (handler-case
                         (progn (pg::physical-reseed-follow-retirement-path-facts (namestring child)) nil)
                       (error (condition)
                         (search "retirement observation changed during resource cleanup" (princ-to-string condition))))))
            (when (pg::physical-adoption-path-present-p (string-right-trim "/" (namestring parent)))
              (sb-posix:unlink (string-right-trim "/" (namestring parent))))
            (sb-posix:rename (namestring moved) (namestring parent)))))))

(deftest reseed-retired-cleanup-audit-keeps-private-inputs-and-opens-no-resources
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (reset-retired-reader-test-files root record :receipt)
       (let* ((audit (symbol-function 'pg::physical-reseed-follow-retirement-audit-stamp))
              (open-file (symbol-function 'pg::physical-image-openat))
              (open-store (symbol-function 'pg::physical-store-open))
              (open-directory (symbol-function 'pg::physical-backup-open-directory))
              (auditing nil) (audits 0) (files 0) (stores 0) (directories 0) (audit-opens 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-follow-retirement-audit-stamp)
                      (lambda (stamp parent)
                        (let ((previous auditing))
                          (incf audits)
                          (unwind-protect
                               (progn (setf auditing t) (funcall audit stamp parent))
                            (setf auditing previous))))
                      (symbol-function 'pg::physical-image-openat)
                      (lambda (&rest arguments)
                        (incf files)
                        (when auditing (incf audit-opens))
                        (apply open-file arguments))
                      (symbol-function 'pg::physical-store-open)
                      (lambda (&rest arguments)
                        (incf stores)
                        (when auditing (incf audit-opens))
                        (apply open-store arguments))
                      (symbol-function 'pg::physical-backup-open-directory)
                      (lambda (&rest arguments)
                        (incf directories)
                        (when auditing (incf audit-opens))
                        (apply open-directory arguments)))
                (ok
                 (equal '(:first :second)
                        (multiple-value-list
                         (pg::call-with-physical-reseed-follow-retired-before-pid-observation
                          root-fd check
                          (lambda (descriptors state verify)
                            (ok (funcall verify))
                            (setf (cdr (assoc :saved descriptors)) -1
                                  (getf (getf state :layout) :names) nil)
                            (ok (funcall verify))
                            (values :first :second))))))
                (ok (>= audits 2))
                (ok (plusp files))
                (ok (plusp stores))
                (ok (plusp directories))
                (ok (zerop audit-opens))
                (ok (sb-posix:fstat root-fd)))
           (setf (symbol-function 'pg::physical-reseed-follow-retirement-audit-stamp) audit
                 (symbol-function 'pg::physical-image-openat) open-file
                 (symbol-function 'pg::physical-store-open) open-store
                 (symbol-function 'pg::physical-backup-open-directory) open-directory))))))

(deftest reseed-retired-cleanup-audit-refuses-an-unexpected-quarantine-fifo
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (reset-retired-reader-test-files root record :receipt)
       (let ((fifo nil) (fired nil) (returned nil) (message nil))
         (unwind-protect
              (progn
                (let ((pg::*physical-reseed-follow-test-hook*
                        (lambda (point path)
                          (declare (ignore path))
                          (when (and (not fired) (eq point :after-retired-journal-close))
                            (sb-posix:mkfifo (namestring fifo) #o600)
                            (setf fired t)))))
                  (setf message
                        (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retired-before-pid-observation
                               root-fd check
                               (lambda (descriptors state verify)
                                 (declare (ignore state))
                                 (setf fifo
                                       (merge-pathnames
                                        "unexpected-cleanup.fifo"
                                        (truename (pg::physical-image-fd-path (cdr (assoc :quarantine descriptors))))))
                                 (funcall verify)))
                              (setf returned t)
                              nil)
                          (error (condition) (princ-to-string condition)))))
                (ok fired)
                (ok (and message (search "retirement observation changed during resource cleanup" message)))
                (ng returned)
                (ok (and fifo (sb-posix:s-isfifo (sb-posix:stat-mode (sb-posix:lstat (namestring fifo))))))
                (ok (sb-posix:fstat root-fd)))
           (when (and fifo (pg::physical-adoption-path-present-p fifo))
             (sb-posix:unlink (namestring fifo))))
         (pg::call-with-physical-reseed-follow-retired-before-pid-observation
          root-fd check
          (lambda (descriptors state verify)
            (declare (ignore descriptors state))
            (ok (funcall verify))))))))

(deftest reseed-retirement-follow-only-observation-refuses-archive-write-on-close
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (reset-retired-reader-test-files root record :follow)
       (let* ((workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
              (archive (merge-pathnames "saved/physical-reseed.attempt" workspace))
              (bytes (lineage-test-bytes archive))
              (original (symbol-function 'pg::physical-store-close))
              (fired nil) (returned nil) (message nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-store-close)
                      (lambda (store)
                        (multiple-value-prog1 (funcall original store)
                          (unless fired
                            (with-open-file (output archive :direction :io :if-exists :overwrite
                                                            :element-type '(unsigned-byte 8))
                              (write-byte (logxor 1 (aref bytes 0)) output))
                            (setf fired t)))))
                (setf message
                      (handler-case
                          (progn
                            (pg::call-with-physical-reseed-follow-retirement-observation
                             root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                             (lambda (descriptors state verify)
                               (declare (ignore descriptors state))
                               (funcall verify)))
                            (setf returned t)
                            nil)
                        (error (condition) (princ-to-string condition)))))
           (setf (symbol-function 'pg::physical-store-close) original))
         (ok fired)
         (ok (= (length bytes) (length (lineage-test-bytes archive))))
         (ng (equalp bytes (lineage-test-bytes archive)))
         (ok (and message (search "retirement observation changed during resource cleanup" message)))
         (ng returned)
         (ok (sb-posix:fstat root-fd))))))

(deftest reseed-retirement-follow-only-startup-audits-second-root-cleanup
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :follow)
       (check-retirement-root-cleanup-cut root root-fd record t 2))))

(defun check-retired-reader-geometries (kinds)
  (dolist (kind kinds)
    (call-with-retired-reader-test-fixture
     kind
     (lambda (root root-fd record check)
       (let ((follow (merge-pathnames "physical-reseed.follow" root))
             (receipt (merge-pathnames "physical-reseed.retired" root)))
         (dolist (geometry '(:follow :alias :receipt))
           (ecase geometry
             (:follow)
             (:alias (sb-posix:link (namestring follow) (namestring receipt)))
             (:receipt (delete-file follow)))
           (let ((original-sync (symbol-function 'sb-posix:fsync))
                 (original-link (symbol-function 'pg::port-linkat))
                 (original-unlink (symbol-function 'pg::port-unlinkat))
                 (syncs 0) (links 0) (unlinks 0)
                 (held nil) (held-fd nil) (escaped nil) (seen nil))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'sb-posix:fsync)
                          (lambda (fd) (incf syncs) (funcall original-sync fd))
                          (symbol-function 'pg::port-linkat)
                          (lambda (&rest arguments) (incf links) (apply original-link arguments))
                          (symbol-function 'pg::port-unlinkat)
                          (lambda (&rest arguments) (incf unlinks) (apply original-unlink arguments)))
                    (let ((result
                            (multiple-value-list
                             (pg::call-with-physical-reseed-follow-retired-journal
                              root-fd
                              (lambda (stream state verify)
                                (setf held stream held-fd (sb-sys:fd-stream-fd stream) escaped verify
                                      seen (pg::physical-adoption-attempt-value-copy (getf state :snapshot)))
                                (ok (eq geometry (getf state :geometry)))
                                (ok (equal record (getf seen :record)))
                                (ok (= (if (eq geometry :alias) 2 1)
                                       (sb-posix:stat-nlink (sb-posix:fstat held-fd))))
                                (ok (logbitp 0 (sb-posix:fcntl held-fd sb-posix:f-getfd)))
                                (ok (equalp (pg::physical-reseed-follow-image record) (getf state :bytes)))
                                (ok (funcall verify))
                                (let ((outcome nil))
                                  (sb-thread:join-thread
                                   (sb-thread:make-thread
                                    (lambda ()
                                      (setf outcome
                                            (handler-case (progn (funcall verify) nil)
                                              (error (condition)
                                                (search "scope ended or changed thread"
                                                        (princ-to-string condition))))))))
                                  (ok outcome))
                                (setf (getf state :geometry) :forged
                                      (getf (getf state :snapshot) :record) (list :forged t)
                                      (aref (getf state :bytes) 0) 0)
                                (ok (funcall verify))
                                (values :first :second))
                              check))))
                      (ok (equal '(:first :second) result)))
                    (ng (open-stream-p held))
                    (ok (handler-case (progn (sb-posix:fstat held-fd) nil)
                          (sb-posix:syscall-error () t)))
                    (ok (handler-case (progn (funcall escaped) nil)
                          (error (condition) (search "scope ended or changed thread" (princ-to-string condition)))))
                    (ok (sb-posix:fstat root-fd))
                    (let ((fresh (pg::physical-reseed-follow-retired-journal-at root-fd check)))
                      (ok (eq geometry (getf fresh :geometry)))
                      (ok (equal record (getf (getf fresh :snapshot) :record))))
                    (ok (handler-case
                            (progn (pg::physical-reseed-follow-entry root-fd "physical-reseed.retired") nil)
                          (error (condition) (search "unexpected target follow journal name"
                                                     (princ-to-string condition)))))
                    (ecase geometry
                      (:follow (ok (equal seen (pg::physical-reseed-follow-snapshot-at root-fd))))
                      (:alias (ok (handler-case
                                      (progn (pg::physical-reseed-follow-snapshot-at root-fd) nil)
                                    (error (condition) (search "single-link" (princ-to-string condition))))))
                      (:receipt (ng (pg::physical-reseed-follow-snapshot-at root-fd))))
                    (ok (zerop syncs))
                    (ok (zerop links))
                    (ok (zerop unlinks)))
               (setf (symbol-function 'sb-posix:fsync) original-sync
                     (symbol-function 'pg::port-linkat) original-link
                     (symbol-function 'pg::port-unlinkat) original-unlink)))))))))

(deftest reseed-retired-reader-binds-all-three-geometries-without-effects
    (check-retired-reader-geometries
     '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer)))

(defun reset-retired-reader-test-files (root record geometry)
  (dolist (name '("physical-reseed.follow" "physical-reseed.retired" "physical-reseed.follow.tmp"
                  "retired-third-link" "retired-other-link"))
    (let ((path (merge-pathnames name root)))
      (when (pg::physical-adoption-path-present-p path)
        (if (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:lstat (namestring path))))
            (sb-posix:rmdir (namestring path))
            (sb-posix:unlink (namestring path))))))
  (reseed-follow-owner-test-write
   (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image record))
  (when (member geometry '(:alias :receipt))
    (sb-posix:link (namestring (merge-pathnames "physical-reseed.follow" root))
                   (namestring (merge-pathnames "physical-reseed.retired" root))))
  (when (eq geometry :receipt)
    (delete-file (merge-pathnames "physical-reseed.follow" root))))

(deftest reseed-retired-reader-checks-parent-before-descriptor-access
    (let ((checks 0) (entered nil))
      (ok (handler-case
              (progn
                (pg::call-with-physical-reseed-follow-retired-journal
                 nil (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t))
                 (lambda () (incf checks) (error "retired parent cut")))
                nil)
            (error (condition) (search "retired parent cut" (princ-to-string condition)))))
      (ok (= 1 checks))
      (ng entered)))

(deftest reseed-retired-reader-refuses-invalid-files-before-callback
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (case '(:absent :symlink :directory :fifo :empty :mode :extra-link
                       :different-inode :temporary :wrong-phase :noncanonical))
         (reset-retired-reader-test-files
          root record (if (member case '(:extra-link :different-inode)) :alias :receipt))
         (let ((follow (merge-pathnames "physical-reseed.follow" root))
               (receipt (merge-pathnames "physical-reseed.retired" root))
               (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
               (entered nil)
               (original-open (symbol-function 'pg::physical-image-openat))
               (opened nil))
           (ecase case
             (:absent (delete-file receipt))
             ((:symlink :directory :fifo :empty)
              (delete-file receipt)
              (ecase case
                (:symlink (sb-posix:symlink "missing" (namestring receipt)))
                (:directory (sb-posix:mkdir (namestring receipt) #o700))
                (:fifo (sb-posix:mkfifo (namestring receipt) #o600))
                (:empty (reseed-follow-owner-test-write receipt (make-array 0 :element-type '(unsigned-byte 8))))))
             (:mode (sb-posix:chmod (namestring receipt) #o644))
             (:extra-link
              (sb-posix:link (namestring receipt) (namestring (merge-pathnames "retired-third-link" root))))
             (:different-inode
              (delete-file receipt)
              (reseed-follow-owner-test-write receipt (pg::physical-reseed-follow-image record))
              (sb-posix:link (namestring receipt) (namestring (merge-pathnames "retired-other-link" root)))
              (sb-posix:link (namestring follow) (namestring (merge-pathnames "retired-third-link" root))))
             (:temporary
              (reseed-follow-owner-test-write temporary (make-array 0 :element-type '(unsigned-byte 8))))
             (:wrong-phase
              (let ((previous (pg::physical-adoption-attempt-value-copy record)))
                (setf (getf (getf previous :retirement) :phase) :retiring)
                (delete-file receipt)
                (reseed-follow-owner-test-write receipt (pg::physical-reseed-follow-image previous))))
             (:noncanonical
              (let* ((text (concatenate 'string " " (pg::physical-bootstrap-manifest-text record)))
                     (bytes (pg::string->bytes
                             (format nil "PGRESEEDFOLLOW1 ~8,'0X~%~a~%"
                                     (pg::wal-crc32 (pg::string->bytes text)) text))))
                (delete-file receipt)
                (reseed-follow-owner-test-write receipt bytes))))
           (let ((names (sort (pg::directory-entry-names root) #'string<))
                 (facts (loop for path in (list follow receipt temporary)
                              collect (when (pg::physical-adoption-path-present-p path)
                                        (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-image-openat)
                          (lambda (&rest arguments)
                            (let ((fd (apply original-open arguments)))
                              (when (not (minusp fd)) (setf opened fd))
                              fd)))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retired-journal
                               root-fd
                               (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t))
                               check)
                              nil)
                          (error (condition)
                            (search (case case
                                      ((:absent :extra-link :different-inode) "link geometry")
                                      (:temporary "refuses a follow temporary")
                                      (:wrong-phase "canonical Retired2 image")
                                      (:noncanonical "noncanonical physical backup lease record")
                                      (otherwise "bounded private regular file"))
                                    (princ-to-string condition))))))
               (setf (symbol-function 'pg::physical-image-openat) original-open))
             (ok (eql (not (null opened)) (not (null (member case '(:wrong-phase :noncanonical))))))
             (when opened
               (ok (handler-case (progn (sb-posix:fstat opened) nil)
                     (sb-posix:syscall-error () t))))
             (ng entered)
             (ok (equal names (sort (pg::directory-entry-names root) #'string<)))
             (ok (equal facts
                        (loop for path in (list follow receipt temporary)
                              collect (when (pg::physical-adoption-path-present-p path)
                                        (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))))
             (ok (sb-posix:fstat root-fd))))))))

(deftest reseed-retired-reader-closes-its-own-stream-on-every-exit
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (geometry '(:follow :alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (dolist (mode '(:normal :error :throw :interrupt))
           (let ((held nil) (fd nil) (escaped nil) (entered nil))
             (let ((outcome
                     (catch 'retired-reader-lifetime-cut
                       (handler-case
                           (multiple-value-list
                            (pg::call-with-physical-reseed-follow-retired-journal
                             root-fd
                             (lambda (stream state verify)
                               (declare (ignore state))
                               (setf held stream fd (sb-sys:fd-stream-fd stream)
                                     escaped verify entered t)
                               (ecase mode
                                 (:normal (values :first :second))
                                 (:error (error "retired reader lifetime cut"))
                                 (:throw (throw 'retired-reader-lifetime-cut :cut))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'retired-reader-lifetime-cut :cut))))))
                             check))
                         (error (condition)
                           (if (search "retired reader lifetime cut" (princ-to-string condition))
                               :error
                               (progn (format t "~%RETIRED-READER-UNEXPECTED-EXIT ~a~%" condition)
                                      :wrong-error)))))))
               (ok (equal outcome (case mode (:normal '(:first :second)) (:error :error) (otherwise :cut)))))
             (ok entered)
             (ng (open-stream-p held))
             (ok (handler-case (progn (sb-posix:fstat fd) nil) (sb-posix:syscall-error () t)))
             (ok (handler-case (progn (funcall escaped) nil)
                   (error (condition) (search "scope ended or changed thread" (princ-to-string condition)))))
             (ok (sb-posix:fstat root-fd))
             (ok (equal record
                        (getf (getf (pg::physical-reseed-follow-retired-journal-at root-fd check)
                                    :snapshot) :record)))))))))

(defun replace-retired-reader-test-file (root path record)
  (let ((replacement (merge-pathnames "retired-replacement" root)))
    (reseed-follow-owner-test-write replacement (pg::physical-reseed-follow-image record))
    (sb-posix:rename (namestring replacement) (namestring path))
    (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))

(deftest reseed-retired-reader-fences-same-byte-replacement-through-cleanup
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (geometry '(:follow :alias :receipt))
         (dolist (point '(:after-retired-journal-open :callback :after-retired-journal-close))
           (reset-retired-reader-test-files root record geometry)
           (let* ((name (if (eq geometry :receipt) "physical-reseed.retired" "physical-reseed.follow"))
                  (path (merge-pathnames name root))
                  (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
                  (original-open (symbol-function 'pg::physical-image-openat))
                  (opened nil) (held nil) (escaped nil) (entered nil) (changed nil)
                  (closed-at-hook nil) (revoked-at-hook nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event observed)
                      (when (eq event point)
                        (when (eq point :after-retired-journal-close)
                          (setf closed-at-hook (not (open-stream-p held))
                                revoked-at-hook
                                (handler-case (progn (funcall escaped) nil)
                                  (error (condition)
                                    (not (null (search "scope ended or changed thread"
                                                       (princ-to-string condition))))))))
                        (setf changed (replace-retired-reader-test-file root observed record))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-image-openat)
                          (lambda (&rest arguments)
                            (let ((fd (apply original-open arguments)))
                              (when (not (minusp fd)) (setf opened fd))
                              fd)))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retired-journal
                               root-fd
                               (lambda (stream state verify)
                                 (declare (ignore state))
                                 (setf held stream escaped verify entered t)
                                 (when (eq point :callback)
                                   (setf changed (replace-retired-reader-test-file root path record))))
                               check)
                              nil)
                          (error (condition)
                            (and changed (search "retired journal" (princ-to-string condition)))))))
               (setf (symbol-function 'pg::physical-image-openat) original-open))
             (ok changed)
             (ok (eql entered (not (eq point :after-retired-journal-open))))
             (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
             (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes path)))
             (ok (handler-case (progn (sb-posix:fstat opened) nil) (sb-posix:syscall-error () t)))
             (ok (sb-posix:fstat root-fd))
             (when entered
               (ng (open-stream-p held))
               (ok (handler-case (progn (funcall escaped) nil)
                     (error (condition) (search "scope ended or changed thread" (princ-to-string condition))))))
             (when (eq point :after-retired-journal-close)
               (ok closed-at-hook)
               (ok revoked-at-hook))))))))

(deftest reseed-retired-reader-refuses-a-substituted-named-root-mount
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (declare (ignore record))
       (let ((original (symbol-function 'pg::physical-adoption-install-mount-id))
             (calls 0) (entered nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                      (lambda (directory &key directory-fd)
                        (let ((mount (funcall original directory :directory-fd directory-fd)))
                          (if (and directory (equal (pathname directory) root))
                              (progn (incf calls) (1+ mount))
                              mount))))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retired-journal
                           root-fd (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t)) check)
                          nil)
                      (error (condition) (search "canonical target root" (princ-to-string condition)))))
                (ok (= 1 calls))
                (ng entered)
                (ok (sb-posix:fstat root-fd)))
           (setf (symbol-function 'pg::physical-adoption-install-mount-id) original))))))

(defun reseed-startup-projection-fixture (offset)
  (let* ((roles '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive))
         (descriptors (loop for role in roles for fd from offset collect (cons role fd)))
         (row (list (copy-seq "follow.active") 1 7 #o100600 1 1000 40 50 60))
         (directories (loop for role in roles for inode from 1
                            collect (cons role (list (copy-seq ".") 1 inode #o40700 2 1000 80 90 100))))
         (image (list (copy-seq "follow.active") (+ offset 7) (copy-seq "follow.active") row)))
    (values descriptors
            (list :prefix 0 :snapshot (list :record (list :generation 1))
                  :install (list :identity (list 1 2 3) :digest 12)
                  :attempt (list :identity (list 4 5 6) :digest 13)
                  :temporary nil :temporary-file nil
                  :pid-file (list :bytes (make-array 2 :element-type '(unsigned-byte 8) :initial-element 1))
                  :layout (list :new (list (copy-list row)) :old nil
                                :images (list image) :active (copy-tree image)
                                :directories directories
                                :names (list :root (list (copy-seq "physical-reseed.follow"))
                                             :saved (list (copy-seq "follow.active"))))))))

(cl-spec:defproperty reseed-retired-projection-keeps-journal-facts
    ((offset (range integer 10 1000)) (mode (range integer 0 2))
                                      (inode (range integer 1 1000000)))
  (:about pg::physical-reseed-follow-retired-startup-projection)
  (:trials (:normal 100))
  (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture offset)
    (multiple-value-bind (follow receipt) (retired-receipt-test-rows mode inode 80)
      (let ((journal (list :geometry (nth mode '(:follow :alias :receipt))
                           :follow follow :receipt receipt :mount 7)))
        (setf (getf state :journal) journal)
        (let ((projection (pg::physical-reseed-follow-retired-startup-projection descriptors state)))
          (and (equal journal (getf projection :journal))
               (progn
                 (incf (getf journal :mount))
                 (not (equal projection
                             (pg::physical-reseed-follow-retired-startup-projection descriptors state))))
               (progn
                 (setf (getf (getf projection :journal) :geometry) :forged)
                 (eq (nth mode '(:follow :alias :receipt)) (getf journal :geometry)))))))))

(deftest generated-reseed-retired-projection-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retired-projection-keeps-journal-facts
                                       :seed 174 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(cl-spec:defproperty reseed-startup-projection-ignores-only-pid-boundary-changes
    ((offset (range integer 10 1000)) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-startup-projection)
  (:trials (:normal 100))
  (multiple-value-bind (before-fds before) (reseed-startup-projection-fixture offset)
    (multiple-value-bind (after-fds after) (reseed-startup-projection-fixture (+ offset delta))
      (let* ((layout (getf after :layout))
             (root (cdr (assoc :root (getf layout :directories)))))
        (setf (seventh root) delta (eighth root) (+ delta 1) (ninth root) (+ delta 2)
              (getf (getf layout :names) :root)
              (append (getf (getf layout :names) :root) (list "postmaster.pid"))
              (getf after :pid-file) nil)
        (let ((left (funcall (symbol-function 'pg::physical-reseed-follow-startup-projection) before-fds before))
              (right (funcall (symbol-function 'pg::physical-reseed-follow-startup-projection) after-fds after)))
          (and (equal left right)
               (progn
                 (incf (getf (getf after :install) :digest))
                 (not (equal left
                             (funcall (symbol-function 'pg::physical-reseed-follow-startup-projection)
                                      after-fds after))))))))))

(deftest generated-reseed-startup-projection-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-startup-projection-ignores-only-pid-boundary-changes
                                       :seed 148 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-startup-projection-preserves-every-stable-evidence-group
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
      (let ((baseline (pg::physical-reseed-follow-startup-projection descriptors state)))
        (dolist (change
                 (list
                  (lambda (copy) (incf (getf copy :prefix)))
                  (lambda (copy) (incf (getf (getf (getf copy :snapshot) :record) :generation)))
                  (lambda (copy) (incf (getf (getf copy :install) :digest)))
                  (lambda (copy) (incf (getf (getf copy :attempt) :digest)))
                  (lambda (copy) (setf (getf copy :temporary) (list :phase :retiring)))
                  (lambda (copy) (setf (getf copy :temporary-file) (list :identity (list 1 2))))
                  (lambda (copy) (incf (second (first (getf (getf copy :layout) :new)))))
                  (lambda (copy) (setf (getf (getf copy :layout) :old) (list "saved")))
                  (lambda (copy) (setf (third (first (getf (getf copy :layout) :images))) "Follow.active"))
                  (lambda (copy) (incf (ninth (fourth (getf (getf copy :layout) :active)))))
                  (lambda (copy) (incf (third (cdr (assoc :root (getf (getf copy :layout) :directories))))))
                  (lambda (copy) (incf (eighth (cdr (assoc :saved (getf (getf copy :layout) :directories))))))
                  (lambda (copy) (push "Postmaster.pid" (getf (getf (getf copy :layout) :names) :root)))
                  (lambda (copy) (push "postmaster.pid" (getf (getf (getf copy :layout) :names) :saved)))))
          (let ((copy (pg::physical-adoption-attempt-value-copy state)))
            (funcall change copy)
            (ok (not (equal baseline (pg::physical-reseed-follow-startup-projection descriptors copy))))))
        (let ((copy (pg::physical-reseed-follow-startup-projection descriptors state)))
          (setf (char (first (getf (getf (getf copy :layout) :names) :root)) 0) #\X
                (getf (getf (getf copy :snapshot) :record) :generation) 99)
          (ok (equal baseline (pg::physical-reseed-follow-startup-projection descriptors state)))))))

(deftest reseed-startup-projection-refuses-unknown-or-ambiguous-parent-fds
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
      (ok (handler-case
              (progn (pg::physical-reseed-follow-startup-projection
                      (remove :staged descriptors :key #'car) state) nil)
            (error () t)))
      (ok (handler-case
              (progn (pg::physical-reseed-follow-startup-projection
                      (acons :other (+ 20 7) descriptors) state) nil)
            (error () t)))))

(deftest reseed-startup-root-borrows-the-local-lock-at-both-boundaries
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((phases nil) (escaped nil) (held-root nil))
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (push phase phases)
            (let ((position (file-position lock)))
              (pg::call-with-physical-reseed-follow-startup-root
               lock
               (lambda (fd verify)
                 (setf held-root fd escaped verify)
                 (ok (funcall verify))
                 (ok (= (sb-posix:stat-ino (sb-posix:fstat fd))
                        (sb-posix:stat-ino (sb-posix:stat root))))
                 (ok (open-stream-p lock))
                 (ok (null pg::*postmaster-data-lock*))))
              (ok (eql position (file-position lock)))
              (ok (handler-case (progn (sb-posix:fstat held-root) nil)
                    (sb-posix:syscall-error () t)))
              (ok (handler-case (progn (funcall escaped) nil)
                    (error () t)))))
          :startup-reconcile (lambda () (ok (open-stream-p pg::*postmaster-data-lock*))))
         (ok (equal (reverse phases) '(:before-pid :owned-pid)))))))

(deftest reseed-startup-root-rechecks-published-state-thread-and-held-lock-fd
    (call-with-pid-owner
     (lambda (root)
       (declare (ignore root))
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (pg::postmaster-own-data-directory
        "127.0.0.1" 5439
        :startup-observer
        (lambda (lock phase)
          (declare (ignore phase))
          (pg::call-with-physical-reseed-follow-startup-root
           lock
           (lambda (fd verify)
             (declare (ignore fd))
             (let ((pg::*postmaster-data-lock* lock))
               (ok (handler-case (progn (funcall verify) nil)
                     (error () t))))
             (let ((pg::*postmaster-pid-directory* nil))
               (ok (handler-case (progn (funcall verify) nil)
                     (error () t))))
             (ok (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda ()
                     (handler-case (progn (funcall verify) nil)
                       (error () t))))))
             (let* ((lock-fd (sb-sys:fd-stream-fd lock))
                    (flags (sb-posix:fcntl lock-fd sb-posix:f-getfd))
                    (backup (sb-posix:dup lock-fd)))
               (unwind-protect
                    (progn
                      (sb-posix:close lock-fd)
                      (ok (handler-case (progn (funcall verify) nil)
                            (sb-posix:syscall-error () t))))
                 (sb-posix:dup2 backup lock-fd)
                 (sb-posix:fcntl lock-fd sb-posix:f-setfd flags)
                 (sb-posix:close backup)))
             (ok (funcall verify)))))))))

(deftest reseed-startup-root-closes-only-its-own-fd-on-every-exit
    (call-with-pid-owner
     (lambda (root)
       (declare (ignore root))
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (pg::postmaster-own-data-directory
        "127.0.0.1" 5439
        :startup-observer
        (lambda (lock phase)
          (declare (ignore phase))
          (dolist (mode '(:error :throw :interrupt))
            (let ((root-fd nil) (escaped nil))
              (ok
               (eq :cut
                   (catch 'startup-root-cut
                     (handler-case
                         (pg::call-with-physical-reseed-follow-startup-root
                          lock
                          (lambda (fd verify)
                            (setf root-fd fd escaped verify)
                            (ecase mode
                              (:error (error "startup root cut"))
                              (:throw (throw 'startup-root-cut :cut))
                              (:interrupt
                               (sb-thread:interrupt-thread
                                sb-thread:*current-thread*
                                (lambda () (throw 'startup-root-cut :cut)))))))
                       (error (condition)
                         (if (search "startup root cut" (princ-to-string condition))
                             :cut
                             (error condition)))))))
              (ok (handler-case (progn (sb-posix:fstat root-fd) nil)
                    (sb-posix:syscall-error () t)))
              (ok (handler-case (progn (funcall escaped) nil)
                    (error () t)))
              (ok (open-stream-p lock))
              (ok (sb-posix:fstat (sb-sys:fd-stream-fd lock)))
              (pg::call-with-physical-reseed-follow-startup-root
               lock (lambda (fd verify) (declare (ignore fd)) (ok (funcall verify)))))))))))

(deftest reseed-startup-observation-keeps-the-ordinary-branch-across-pid-creation
    (call-with-pid-owner
     (lambda (root)
       (declare (ignore root))
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((before nil) (after nil))
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (if (eq phase :before-pid)
                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                (setf after (pg::physical-reseed-follow-startup-observation lock phase before)))))
         (ok (equal before '(:branch :ordinary)))
         (ok (equal before after))
         (ok (pg::postmaster-pid-owned-p))))))

(deftest reseed-retired-receipt-fences-real-startup-before-pid-inspection
    (dolist (kind '(:file :symlink :directory :fifo))
      (with-physical-store-directory (root)
        (call-with-adoption-postmaster-fixture
         root
         (lambda ()
           (let ((pid (merge-pathnames "postmaster.pid" root))
                 (receipt (merge-pathnames "physical-reseed.retired" root))
                 (boundaries nil))
             (with-open-file (stream pid :direction :output :if-exists :error)
               (write-string "opaque old PID bytes" stream))
             (ecase kind
               (:file
                (with-open-file (stream receipt :direction :output :if-exists :error)
                  (write-string "retired receipt" stream))
                (sb-posix:chmod (namestring receipt) #o600))
               (:symlink (sb-posix:symlink "missing" (namestring receipt)))
               (:directory (sb-posix:mkdir (namestring receipt) #o700))
               (:fifo (sb-posix:mkfifo (namestring receipt) #o600)))
             (let ((bytes (lineage-test-bytes pid))
                   (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))
                   (facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
               (ok (handler-case
                       (progn
                         (pg::postmaster-own-data-directory
                          "127.0.0.1" 5439
                          :startup-observer
                          (lambda (lock phase)
                            (push phase boundaries)
                            (pg::physical-reseed-follow-startup-observation lock phase)))
                         nil)
                     (error (condition)
                       (search (if (eq kind :file)
                                   "invalid physical backup lease envelope"
                                   "bounded private regular file")
                               (princ-to-string condition)))))
               (ok (equal '(:before-pid) boundaries))
               (ok (equalp bytes (lineage-test-bytes pid)))
               (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
               (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
               (ng pg::*postmaster-pid-stream*)
               (ng pg::*postmaster-data-lock*))))))))

(deftest reseed-namespace-guards-preserve-public-name-and-install-only-policy
    (with-physical-store-directory (root)
      (flet ((message-for (function)
               (handler-case (progn (funcall function) nil)
                 (error (condition) (princ-to-string condition)))))
        (ok (null (pg::physical-directory-check-reseed-intent root)))
        (ok (null (pg::physical-directory-check-reseed-follow-intent root)))
        (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp" "physical-reseed.retired"
                        "physical-reseed.attempt" "physical-reseed.attempt.tmp"
                        "physical-reseed.install" "physical-reseed.install.tmp"))
          (let ((path (merge-pathnames name root)))
            (unwind-protect
                 (progn
                   (with-open-file (stream path :direction :output :if-exists :error)
                     (write-string "guard fixture" stream))
                   (let ((message (message-for
                                   (lambda () (pg::physical-directory-check-reseed-follow-intent root)))))
                     (ok (if (member name '("physical-reseed.follow" "physical-reseed.follow.tmp"
                                            "physical-reseed.retired") :test #'equal)
                             (and message (search "pending offline reseed follow requires journal reconciliation"
                                                  message))
                             (null message))))
                   (dolist (install-only '(nil t))
                     (let* ((installation (uiop:string-prefix-p "physical-reseed.install" name))
                            (attempt (uiop:string-prefix-p "physical-reseed.attempt" name))
                            (expected (cond
                                        ((and attempt install-only) nil)
                                        (installation "pending offline reseed installation requires journal reconciliation")
                                        (attempt "pending offline reseed requires reconciliation before PID changes")
                                        (t "pending offline reseed follow requires journal reconciliation")))
                            (message (message-for
                                      (lambda ()
                                        (pg::physical-directory-check-reseed-intent root :install-only install-only)))))
                       (ok (if expected (and message (search expected message)) (null message)))
                       (ok (probe-file path)))))
              (when (probe-file path) (delete-file path))))))))

(deftest reseed-namespace-parts-keep-receipt-fenced-while-checking-other-intents
    (with-physical-store-directory (root)
      (let ((receipt (merge-pathnames "physical-reseed.retired" root)))
        (with-open-file (stream receipt :direction :output :if-exists :error)
          (write-string "receipt guard fixture" stream))
        (let ((bytes (lineage-test-bytes receipt))
              (inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt)))))
          (ok (null (pg::physical-directory-check-reseed-follow-journals root)))
          (ok (null (pg::physical-directory-check-reseed-install-attempt-intent root)))
          (ok (handler-case
                  (progn (pg::physical-directory-check-reseed-intent root) nil)
                (error (condition)
                  (search "pending offline reseed follow requires journal reconciliation"
                          (princ-to-string condition)))))
          (dolist (name '("physical-reseed.attempt" "physical-reseed.attempt.tmp"
                          "physical-reseed.install" "physical-reseed.install.tmp"
                          "physical-reseed.follow" "physical-reseed.follow.tmp"))
            (let ((path (merge-pathnames name root)))
              (unwind-protect
                   (progn
                     (with-open-file (stream path :direction :output :if-exists :error)
                       (write-string "pending intent" stream))
                     (ok (handler-case
                             (progn
                               (pg::physical-directory-check-reseed-follow-journals root)
                               (pg::physical-directory-check-reseed-install-attempt-intent root)
                               nil)
                           (error (condition)
                             (search "pending offline reseed" (princ-to-string condition)))))
                     (ok (equalp bytes (lineage-test-bytes receipt)))
                     (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring receipt))))))
                (when (probe-file path) (delete-file path)))))))))

(deftest reseed-receipt-local-preflight-uses-held-store-without-installing-recovery
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (reset-retired-reader-test-files root record :receipt)
       (call-with-retired-startup-test-context
        root
        (lambda (before after)
          (declare (ignore before))
          (pg::call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (let ((original (symbol-function 'pg::physical-reseed-follow-retirement-check-receive))
                   (captured-store nil) (captured-check nil)
                   (recovery-state pg::*physical-recovery-state*)
                   (receive-store pg::*physical-receive-store*)
                   (wal-ready pg::*wal-ready*))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-reseed-follow-retirement-check-receive)
                            (lambda (descriptors store current base binding check)
                              (setf captured-store store captured-check check)
                              (funcall original descriptors store current base binding check)))
                      (pg::call-with-physical-reseed-follow-retirement-startup-observation
                       owner
                       (lambda (descriptors state verify)
                         (declare (ignore state))
                         (let ((baseline (list (reseed-mapped-test-tree-facts root)
                                               (reseed-mapped-test-tree-facts workspace))))
                           (ok captured-store)
                           (ok captured-check)
                           (ok (pg::physical-reseed-follow-startup-local-state-at
                                descriptors captured-store captured-check))
                           (ok (eq recovery-state pg::*physical-recovery-state*))
                           (ok (eq receive-store pg::*physical-receive-store*))
                           (ok (eq wal-ready pg::*wal-ready*))
                           (ok (reseed-activation-layout-facts-equal-p
                                baseline
                                (list (reseed-mapped-test-tree-facts root)
                                      (reseed-mapped-test-tree-facts workspace))))
                           (ok (funcall verify)))))
                      (ok (handler-case (progn (pg::physical-store-check-live captured-store) nil)
                            (error () t))))
                 (setf (symbol-function 'pg::physical-reseed-follow-retirement-check-receive) original)))))))
       (ok (sb-posix:fstat root-fd)))))

(defun check-receipt-consumption-geometry (kind)
  (call-with-retired-startup-test-fixture
   kind
   (lambda (root workspace root-fd record)
     (declare (ignore workspace))
     (reset-retired-reader-test-files root record :receipt)
     (let ((captured nil) (held-through-root-close nil))
       (call-with-retired-startup-test-context
        root
        (lambda (before after)
          (declare (ignore before))
          (pg::call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (let ((run (symbol-function 'pg::physical-reseed-follow-consumption-run))
                   (source-check (symbol-function 'pg::physical-reseed-follow-consumption-check-source)))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-reseed-follow-consumption-run)
                            (lambda (consumer) (setf captured consumer) (funcall run consumer))
                            (symbol-function 'pg::physical-reseed-follow-consumption-check-source)
                            (lambda (consumer)
                              (multiple-value-prog1 (funcall source-check consumer)
                                (when (and (pg::receipt-consumer-removed-p consumer)
                                           (open-stream-p (pg::receipt-consumer-stream consumer))
                                           (handler-case
                                               (progn (sb-posix:fstat (pg::receipt-consumer-root consumer)) nil)
                                             (sb-posix:syscall-error () t)))
                                  (setf held-through-root-close
                                        (and
                                         (= 0 (sb-posix:stat-nlink
                                               (sb-posix:fstat
                                                (sb-sys:fd-stream-fd (pg::receipt-consumer-stream consumer)))))
                                         (handler-case
                                             (progn (pg::physical-store-check-live
                                                     (pg::receipt-consumer-store consumer)) nil)
                                           (error () t))))))))
                      (ok (pg::physical-reseed-follow-retirement-consume-receipt owner))
                      (ok captured)
                      (ok held-through-root-close)
                      (ok (not (pg::receipt-consumer-active-p captured)))
                      (ok (not (open-stream-p (pg::receipt-consumer-stream captured))))
                      (ok (null (pg::retirement-startup-evidence owner)))
                      (ok (not (pg::retirement-startup-uncertain-p owner)))
                      (ok (not (pg::retirement-startup-busy-p owner)))
                      (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
                      (ok (not (probe-file (merge-pathnames "physical-reseed.follow" root))))
                      (ok (null (pg::physical-directory-check-reseed-intent root)))
                      (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner))
                      (ok (handler-case
                              (progn (pg::physical-reseed-follow-consumption-check-source captured) nil)
                            (error () t)))
                      (let* ((names '(sb-posix:fsync pg::port-linkat pg::port-unlinkat))
                             (originals (mapcar (lambda (name) (cons name (symbol-function name))) names))
                             (effects 0))
                        (unwind-protect
                             (progn
                               (dolist (entry originals)
                                 (setf (symbol-function (car entry))
                                       (lambda (&rest arguments)
                                         (declare (ignore arguments))
                                         (incf effects)
                                         (error "unexpected consumed-owner filesystem effect"))))
                               (dolist (function
                                        (list #'pg::physical-reseed-follow-retirement-publish-initial
                                              #'pg::physical-reseed-follow-retirement-archive-install
                                              #'pg::physical-reseed-follow-retirement-archive-attempt
                                              #'pg::physical-reseed-follow-retirement-publish-terminal
                                              #'pg::physical-reseed-follow-retirement-publish-receipt
                                              #'pg::physical-reseed-follow-retirement-consume-receipt))
                                 (ok (handler-case (progn (funcall function owner) nil)
                                       (error () t))))
                               (ok (zerop effects)))
                          (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry))))))
                 (setf (symbol-function 'pg::physical-reseed-follow-consumption-run) run
                       (symbol-function 'pg::physical-reseed-follow-consumption-check-source) source-check)))))))
       (call-with-retired-startup-test-context
        root
        (lambda (before after)
          (ok (equal '(:branch :ordinary) before))
          (ok (equal before after))
          (ok (null (pg::physical-adoption-startup-preflight)))))
       (ok (sb-posix:fstat root-fd))))))

(deftest reseed-receipt-consumption-removes-authority-and-holds-source-through-root-close
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (check-receipt-consumption-geometry kind)))

(deftest reseed-receipt-consumption-refuses-held-source-write-after-final-pid-close
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :receipt)
       (let ((run (symbol-function 'pg::physical-reseed-follow-consumption-run))
             (pid-reader (symbol-function 'pg::physical-reseed-read-pid))
             (captured nil) (fired nil) (changed nil) (eligible-reads 0))
         (unwind-protect
              (ok
               (eq :checked
                   (catch 'consumption-source-checked
                     (call-with-retired-startup-test-context
                      root
                      (lambda (before after)
                        (declare (ignore before))
                        (pg::call-with-physical-reseed-follow-retirement-startup-owner
                         after
                         (lambda (owner)
                           (setf (symbol-function 'pg::physical-reseed-follow-consumption-run)
                                 (lambda (consumer)
                                   (setf captured consumer)
                                   (funcall run consumer))
                                 (symbol-function 'pg::physical-reseed-read-pid)
                                 (lambda (directory)
                                   (multiple-value-prog1 (funcall pid-reader directory)
                                     (when (and captured (not fired)
                                                (pg::receipt-consumer-removed-p captured)
                                                (open-stream-p (pg::receipt-consumer-stream captured))
                                                (handler-case
                                                    (progn (sb-posix:fstat (pg::receipt-consumer-root captured)) nil)
                                                  (sb-posix:syscall-error () t))
                                                (= 2 (incf eligible-reads)))
                                       (setf fired t)
                                       (let* ((stream (pg::receipt-consumer-stream captured))
                                              (fd (sb-sys:fd-stream-fd stream))
                                              (bytes (copy-seq (pg::receipt-consumer-bytes captured))))
                                         (ok (= 0 (sb-posix:stat-nlink (sb-posix:fstat fd))))
                                         (setf (aref bytes 0) (logxor 1 (aref bytes 0)))
                                         (with-open-file (output (format nil "/proc/self/fd/~D" fd)
                                                                 :direction :io
                                                                 :element-type '(unsigned-byte 8)
                                                                 :if-exists :overwrite)
                                           (write-sequence bytes output)
                                           (finish-output output))
                                         (file-position stream 0)
                                         (let ((actual (make-array (length bytes) :element-type '(unsigned-byte 8))))
                                           (setf changed
                                                 (and (= (length actual) (read-sequence actual stream))
                                                      (equalp actual bytes)
                                                      (= (length actual) (sb-posix:stat-size (sb-posix:fstat fd)))))))))))
                           (ok (handler-case
                                   (progn (pg::physical-reseed-follow-retirement-consume-receipt owner) nil)
                                 (error (condition)
                                   (or (search "changed its held source facts" (princ-to-string condition))
                                       (search "changed its canonical bytes" (princ-to-string condition))
                                       (error condition)))))
                           (ok fired)
                           (ok (= 2 eligible-reads))
                           (ok changed)
                           (ok (pg::retirement-startup-uncertain-p owner))
                           (ok (not (pg::retirement-startup-busy-p owner)))
                           (ok (pg::retirement-startup-evidence owner))
                           (ok (not (pg::receipt-consumer-active-p captured)))
                           (ok (not (open-stream-p (pg::receipt-consumer-stream captured))))
                           (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
                           (throw 'consumption-source-checked :checked))))))))
           (setf (symbol-function 'pg::physical-reseed-follow-consumption-run) run
                 (symbol-function 'pg::physical-reseed-read-pid) pid-reader))
         (call-with-retired-startup-test-context
          root
          (lambda (before after)
            (ok (equal '(:branch :ordinary) before))
            (ok (equal before after))
            (ok (null (pg::physical-adoption-startup-preflight)))))
         (ok (sb-posix:fstat root-fd))))))

(defun check-receipt-consumption-syscall-cut (root root-fd record operation timing)
  (reset-retired-reader-test-files root record :receipt)
  (let ((run (symbol-function 'pg::physical-reseed-follow-consumption-run))
        (fsync (symbol-function 'sb-posix:fsync))
        (unlink (symbol-function 'pg::port-unlinkat))
        (captured nil) (fired nil) (performed nil))
    (unwind-protect
         (ok
          (eq :checked
              (catch 'consumption-syscall-checked
                (call-with-retired-startup-test-context
                 root
                 (lambda (before after)
                   (declare (ignore before))
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    after
                    (lambda (owner)
                      (labels ((cut (function arguments)
                                 (setf fired t)
                                 (when (eq timing :after)
                                   (apply function arguments)
                                   (setf performed t))
                                 (error "receipt consumption syscall cut"))
                               (sync-target-p (fd)
                                 (and captured (not fired)
                                      (ecase operation
                                        (:inode
                                         (and (eq :syncing (pg::receipt-consumer-phase captured))
                                              (= fd (sb-sys:fd-stream-fd
                                                     (pg::receipt-consumer-stream captured)))))
                                        (:baseline
                                         (and (eq :syncing (pg::receipt-consumer-phase captured))
                                              (= fd (pg::receipt-consumer-root captured))))
                                        (:removed-root
                                         (and (eq :syncing-removed (pg::receipt-consumer-phase captured))
                                              (= fd (pg::receipt-consumer-root captured))))
                                        (:unlink nil)))))
                        (setf (symbol-function 'pg::physical-reseed-follow-consumption-run)
                              (lambda (consumer) (setf captured consumer) (funcall run consumer))
                              (symbol-function 'sb-posix:fsync)
                              (lambda (fd)
                                (if (sync-target-p fd) (cut fsync (list fd)) (funcall fsync fd)))
                              (symbol-function 'pg::port-unlinkat)
                              (lambda (fd name flags)
                                (if (and captured (not fired) (eq operation :unlink)
                                         (eq :unlinking (pg::receipt-consumer-phase captured))
                                         (= fd (pg::receipt-consumer-root captured))
                                         (string= name "physical-reseed.retired"))
                                    (cut unlink (list fd name flags))
                                    (funcall unlink fd name flags))))
                        (ok (handler-case
                                (progn (pg::physical-reseed-follow-retirement-consume-receipt owner) nil)
                              (error (condition)
                                (or (search "receipt consumption syscall cut" (princ-to-string condition))
                                    (error condition)))))
                        (ok fired)
                        (ok (eq performed (eq timing :after)))
                        (ok (pg::retirement-startup-uncertain-p owner))
                        (ok (not (pg::retirement-startup-busy-p owner)))
                        (ok (pg::retirement-startup-evidence owner))
                        (ok (not (pg::receipt-consumer-active-p captured)))
                        (ok (not (open-stream-p (pg::receipt-consumer-stream captured))))
                        (ok (handler-case
                                (progn (pg::physical-store-check-live (pg::receipt-consumer-store captured)) nil)
                              (error () t)))
                        (ok (not (probe-file (merge-pathnames "physical-reseed.follow" root))))
                        (throw 'consumption-syscall-checked :checked)))))))))
      (setf (symbol-function 'pg::physical-reseed-follow-consumption-run) run
            (symbol-function 'sb-posix:fsync) fsync
            (symbol-function 'pg::port-unlinkat) unlink))
    (let ((removed (or (eq operation :removed-root)
                       (and (eq operation :unlink) (eq timing :after)))))
      (ok (eq (not removed) (not (null (probe-file (merge-pathnames "physical-reseed.retired" root)))))))
    (call-with-retired-startup-test-context
     root
     (lambda (before after)
       (ok (equal before after))
       (ok (eq (if (or (eq operation :removed-root)
                       (and (eq operation :unlink) (eq timing :after)))
                   :ordinary :retired)
               (getf before :branch)))))
    (ok (sb-posix:fstat root-fd))))

(deftest reseed-receipt-consumption-fences-each-sync-before-and-after-real-effects
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (operation '(:inode :baseline :removed-root))
         (dolist (timing '(:before :after))
           (check-receipt-consumption-syscall-cut root root-fd record operation timing))))))

(deftest reseed-receipt-consumption-fences-unlink-before-and-after-real-effects
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (timing '(:before :after))
         (check-receipt-consumption-syscall-cut root root-fd record :unlink timing)))))

(deftest reseed-receipt-consumption-survives-native-process-crashes-at-every-durability-boundary
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (reset-retired-reader-test-files root record :receipt)
       (let ((baseline (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                             (reseed-mapped-test-tree-facts workspace))))
         (with-physical-store-directory (driver)
           (let ((cache (merge-pathnames "cache/" driver)))
             (dolist (case '((:before-receipt-consumption-inode-fsync :retired)
                             (:after-receipt-consumption-inode-fsync :retired)
                             (:before-receipt-consumption-baseline-root-fsync :retired)
                             (:after-receipt-consumption-baseline-root-fsync :retired)
                             (:before-receipt-consumption-unlink :retired)
                             (:after-receipt-consumption-unlink :ordinary)
                             (:before-receipt-consumption-root-fsync :ordinary)
                             (:after-receipt-consumption-root-fsync :ordinary)))
               (destructuring-bind (point expected) case
                 (reset-retired-reader-test-files root record :receipt)
                 (let* ((marker (merge-pathnames (format nil "~(~a~).marker" point) driver))
                        (pid (run-retirement-intent-test-crash-child
                              root point cache marker :operation :receipt-consumption :timeout 1200))
                        (stale (pg::physical-reseed-read-pid root)))
                   (ok (= pid (getf stale :pid)))
                   (ok (eq (eq expected :retired)
                           (not (null (probe-file (merge-pathnames "physical-reseed.retired" root))))))
                   (ok (not (probe-file (merge-pathnames "physical-reseed.follow" root))))
                   (ok (reseed-activation-layout-facts-equal-p
                        baseline
                        (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                              (reseed-mapped-test-tree-facts workspace))))
                   (call-with-retired-startup-test-context
                    root
                    (lambda (before after)
                      (ok (eq expected (getf before :branch)))
                      (ok (equal before after))
                      (if (eq expected :retired)
                          (pg::call-with-physical-reseed-follow-retirement-startup-owner
                           after
                           (lambda (owner)
                             (ok (pg::physical-reseed-follow-retirement-consume-receipt owner))
                             (ok (null (pg::retirement-startup-evidence owner)))
                             (ok (not (pg::retirement-startup-uncertain-p owner)))))
                          (ok (null (pg::physical-adoption-startup-preflight))))))
                   (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
                   (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
                   (ok (reseed-activation-layout-facts-equal-p
                        baseline
                        (list (retirement-intent-test-nonjournal-facts root '("physical-reseed.retired"))
                              (reseed-mapped-test-tree-facts workspace))))
                   (ok (sb-posix:fstat root-fd)))))))))))

(defun check-receipt-consumption-finalization-cut (root root-fd record mode)
  (reset-retired-reader-test-files root record :receipt)
  (let* ((workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
         (archive (merge-pathnames "saved/physical-reseed.attempt" workspace))
         (bytes (lineage-test-bytes archive))
         (stat (sb-posix:stat (namestring archive)))
         (run (symbol-function 'pg::physical-reseed-follow-consumption-run))
         (pid-reader (symbol-function 'pg::physical-reseed-read-pid))
         (stamp-check (symbol-function 'pg::physical-reseed-follow-retirement-check-stamp))
         (captured nil) (fired nil) (closed nil) (changed nil))
    (unwind-protect
         (ok
          (eq :checked
              (catch 'consumption-finalization-checked
                (call-with-retired-startup-test-context
                 root
                 (lambda (before after)
                   (declare (ignore before))
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    after
                    (lambda (owner)
                      (flet ((finishing ()
                               (and captured (not fired)
                                    (not (pg::receipt-consumer-active-p captured))
                                    (null (pg::retirement-startup-evidence owner))
                                    (pg::retirement-startup-busy-p owner)
                                    (not (pg::retirement-startup-uncertain-p owner))))
                             (cut ()
                               (setf fired t
                                     closed (and
                                             (not (open-stream-p (pg::receipt-consumer-stream captured)))
                                             (handler-case
                                                 (progn (sb-posix:fstat (pg::receipt-consumer-root captured)) nil)
                                               (sb-posix:syscall-error () t))
                                             (handler-case
                                                 (progn (pg::physical-store-check-live
                                                         (pg::receipt-consumer-store captured)) nil)
                                               (error () t))))
                               (ok (pg::retirement-startup-busy-p owner))
                               (ok (null (pg::retirement-startup-evidence owner)))
                               (ok (handler-case
                                       (progn (pg::physical-reseed-follow-retirement-consume-receipt owner) nil)
                                     (error () t)))
                               (ecase mode
                                 (:pid-close-mutation
                                  (let ((replacement (copy-seq bytes)))
                                    (setf (aref replacement 0) (logxor 1 (aref replacement 0)))
                                    (with-open-file (stream archive :direction :io
                                                                    :element-type '(unsigned-byte 8)
                                                                    :if-exists :overwrite)
                                      (write-sequence replacement stream)
                                      (finish-output stream))
                                    (setf changed (and (= (length bytes) (length (lineage-test-bytes archive)))
                                                       (equalp replacement (lineage-test-bytes archive))))))
                                 (:error (error "consumption final audit cut"))
                                 (:throw (throw 'consumption-final-audit-cut :cut))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'consumption-final-audit-cut :cut)))))))
                        (unwind-protect
                             (progn
                               (setf (symbol-function 'pg::physical-reseed-follow-consumption-run)
                                     (lambda (consumer) (setf captured consumer) (funcall run consumer))
                                     (symbol-function 'pg::physical-reseed-read-pid)
                                     (lambda (directory)
                                       (multiple-value-prog1 (funcall pid-reader directory)
                                         (when (and (eq mode :pid-close-mutation) (finishing)) (cut))))
                                     (symbol-function 'pg::physical-reseed-follow-retirement-check-stamp)
                                     (lambda (stamp)
                                       (when (and (not (eq mode :pid-close-mutation)) (finishing)) (cut))
                                       (funcall stamp-check stamp)))
                               (ok (eq :cut
                                       (catch 'consumption-final-audit-cut
                                         (handler-case
                                             (progn (pg::physical-reseed-follow-retirement-consume-receipt owner) nil)
                                           (error (condition)
                                             (if (search (if (eq mode :pid-close-mutation)
                                                             "changed during resource cleanup"
                                                             "consumption final audit cut")
                                                         (princ-to-string condition))
                                                 :cut
                                                 (error condition)))))))
                               (ok fired)
                               (ok closed)
                               (when (eq mode :pid-close-mutation) (ok changed))
                               (ok (pg::retirement-startup-uncertain-p owner))
                               (ok (not (pg::retirement-startup-busy-p owner)))
                               (ok (null (pg::retirement-startup-evidence owner)))
                               (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
                               (ok (pg::postmaster-pid-owned-p)))
                          (setf (symbol-function 'pg::physical-reseed-follow-consumption-run) run
                                (symbol-function 'pg::physical-reseed-read-pid) pid-reader
                                (symbol-function 'pg::physical-reseed-follow-retirement-check-stamp) stamp-check)))
                      (throw 'consumption-finalization-checked :checked))))))))
      (when (eq mode :pid-close-mutation)
        (restore-retirement-cleanup-test-file archive bytes stat)))
    (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
    (call-with-retired-startup-test-context
     root
     (lambda (before after)
       (ok (equal '(:branch :ordinary) before))
       (ok (equal before after))
       (ok (null (pg::physical-adoption-startup-preflight)))))
    (ok (sb-posix:fstat root-fd))))

(deftest reseed-receipt-consumption-finalization-fences-last-pid-close-and-nonlocal-exits
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (dolist (mode '(:pid-close-mutation :error :throw :interrupt))
         (check-receipt-consumption-finalization-cut root root-fd record mode)))))

(deftest reseed-receipt-consumption-effects-require-live-thread-and-one-use-phase
    (let* ((owner (pg::make-physical-reseed-follow-retirement-startup-owner))
           (consumer (pg::make-physical-reseed-follow-receipt-consumer :owner owner))
           (names '(pg::physical-reseed-follow-consumption-check-scope
                    pg::physical-reseed-follow-consumption-point
                    pg::physical-reseed-follow-consumption-observe
                    pg::port-unlinkat sb-posix:fsync))
           (originals (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (calls 0))
      (unwind-protect
           (progn
             (dolist (entry originals)
               (setf (symbol-function (car entry))
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (incf calls)
                       (error "unexpected consumer effect or parent callback"))))
             (dolist (case '((:sync :ready t t nil t nil)
                             (:sync :syncing t t t t nil)
                             (:sync :admitted nil t nil t nil)
                             (:sync :admitted t nil nil t nil)
                             (:sync :admitted t t nil nil nil)
                             (:sync :admitted t t nil t t)
                             (:unlink :syncing t t t t nil)
                             (:unlink :unlinking t t t t nil)
                             (:unlink :synced nil t t t nil)
                             (:unlink :synced t nil t t nil)
                             (:unlink :synced t t nil t nil)
                             (:unlink :synced t t t nil nil)
                             (:root :durable t t t t t)
                             (:root :syncing-removed t t t t t)
                             (:root :removed t t nil t t)))
               (destructuring-bind (operation phase active busy uncertain same-thread removed) case
                 (setf (pg::retirement-startup-busy-p owner) busy
                       (pg::retirement-startup-uncertain-p owner) uncertain
                       (pg::receipt-consumer-active-p consumer) active
                       (pg::receipt-consumer-thread consumer) (when same-thread sb-thread:*current-thread*)
                       (pg::receipt-consumer-phase consumer) phase
                       (pg::receipt-consumer-removed-p consumer) removed)
                 (ok (handler-case
                         (progn
                           (ecase operation
                             (:sync (pg::physical-reseed-follow-consumption-sync-receipt consumer))
                             (:unlink (pg::physical-reseed-follow-consumption-unlink consumer))
                             (:root (pg::physical-reseed-follow-consumption-sync-removed-root consumer)))
                           nil)
                       (error (condition)
                         (search (if (eq operation :sync)
                                     "requires completed local admission"
                                     "cannot claim this phase")
                                 (princ-to-string condition)))))
                 (ok (zerop calls))))
             (setf (pg::retirement-startup-busy-p owner) t
                   (pg::retirement-startup-uncertain-p owner) t
                   (pg::receipt-consumer-active-p consumer) t
                   (pg::receipt-consumer-thread consumer) sb-thread:*current-thread*
                   (pg::receipt-consumer-phase consumer) :synced
                   (pg::receipt-consumer-removed-p consumer) nil)
             (ok (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda ()
                     (handler-case
                         (progn (pg::physical-reseed-follow-consumption-unlink consumer) nil)
                       (error (condition) (search "cannot claim this phase" (princ-to-string condition))))))))
             (ok (eq :synced (pg::receipt-consumer-phase consumer)))
             (ok (eq :unlinking (pg::physical-reseed-follow-consumption-claim consumer :synced :unlinking)))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-consumption-claim consumer :synced :unlinking) nil)
                   (error (condition) (search "cannot claim this phase" (princ-to-string condition)))))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-consumption-run consumer) nil)
                   (error (condition) (search "already consumed" (princ-to-string condition)))))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-consumption-check-owner nil) nil)
                   (error (condition) (search "ended, changed thread" (princ-to-string condition)))))
             (ok (zerop calls)))
        (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry))))))

(deftest reseed-receipt-consumption-preserves-authority-on-real-timeline-policy-rejection
    (call-with-retired-startup-test-fixture
     :same-timeline
     (lambda (root workspace root-fd record)
       (declare (ignore workspace))
       (reset-retired-reader-test-files root record :receipt)
       (call-with-retired-startup-test-context
        root
        (lambda (before after)
          (declare (ignore before))
          (pg::call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (let* ((evidence (pg::physical-adoption-attempt-value-copy (pg::retirement-startup-evidence owner)))
                    (source (pg::physical-reseed-follow-retired-journal-at root-fd (constantly t)))
                    (names '(sb-posix:fsync pg::port-unlinkat
                             pg::physical-recovery-resolve-timeline-policy
                             pg::physical-reseed-follow-consumption-run))
                    (originals (mapcar (lambda (name) (cons name (symbol-function name))) names))
                    (resolver (symbol-function 'pg::physical-recovery-resolve-timeline-policy))
                    (run (symbol-function 'pg::physical-reseed-follow-consumption-run))
                    (effects 0) (goal-seen nil) (captured nil))
               (ok (not (probe-file (pg::timeline-history-path 2 root))))
               (unwind-protect
                    (let ((pg::*startup-server-settings* (make-hash-table :test #'equal)))
                      (setf (gethash "recovery_target_timeline" pg::*startup-server-settings*)
                            '(:value "2" :source :command-line)
                            (symbol-function 'sb-posix:fsync)
                            (lambda (fd)
                              (declare (ignore fd))
                              (incf effects)
                              (error "unexpected policy-rejected consumer fsync"))
                            (symbol-function 'pg::port-unlinkat)
                            (lambda (&rest arguments)
                              (declare (ignore arguments))
                              (incf effects)
                              (error "unexpected policy-rejected consumer unlink"))
                            (symbol-function 'pg::physical-recovery-resolve-timeline-policy)
                            (lambda (state store metadata goal)
                              (setf goal-seen goal)
                              (funcall resolver state store metadata goal))
                            (symbol-function 'pg::physical-reseed-follow-consumption-run)
                            (lambda (consumer) (setf captured consumer) (funcall run consumer)))
                      (ok (= 2 (pg::startup-recovery-timeline-goal)))
                      (ok (handler-case
                              (progn (pg::physical-reseed-follow-retirement-consume-receipt owner) nil)
                            (error (condition)
                              (or (search "could not open timeline history file" (princ-to-string condition))
                                  (error condition)))))
                      (ok (eql 2 goal-seen))
                      (ok (zerop effects))
                      (ok (equal evidence (pg::retirement-startup-evidence owner)))
                      (ok (not (pg::retirement-startup-uncertain-p owner)))
                      (ok (not (pg::retirement-startup-busy-p owner)))
                      (ok (equalp source (pg::physical-reseed-follow-retired-journal-at root-fd (constantly t))))
                      (ok (eq :checking (pg::receipt-consumer-phase captured)))
                      (ok (not (pg::receipt-consumer-removed-p captured)))
                      (ok (not (pg::receipt-consumer-active-p captured)))
                      (ok (not (open-stream-p (pg::receipt-consumer-stream captured))))
                      (ok (pg::physical-reseed-follow-retirement-check-startup-idle owner)))
                 (dolist (entry originals) (setf (symbol-function (car entry)) (cdr entry))))
               (let ((pg::*startup-server-settings* nil))
                 (ok (pg::physical-reseed-follow-retirement-consume-receipt owner)))
               (ok (null (pg::retirement-startup-evidence owner))))))))
       (ok (not (probe-file (merge-pathnames "physical-reseed.retired" root))))
       (ok (sb-posix:fstat root-fd)))))

(deftest reseed-retired-receipt-fences-legacy-ordinary-entrypoints
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (receipt (merge-pathnames "physical-reseed.retired" root))
             (bytes (sb-ext:string-to-octets "retired receipt" :external-format :utf-8)))
        (reseed-follow-owner-test-write receipt bytes)
        (let ((facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
          (dolist (function (list #'pg::physical-reseed-check-before-pid
                                  #'pg::physical-adoption-startup-preflight
                                  #'pg::physical-adoption-reconcile-startup
                                  (lambda () (pg::physical-directory-check-reseed-intent root :install-only t))))
            (ok (handler-case
                    (progn (funcall function) nil)
                  (error (condition)
                    (search "pending offline reseed follow requires journal reconciliation"
                            (princ-to-string condition))))))
          (ok (equalp bytes (lineage-test-bytes receipt)))
          (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))))))

(deftest reseed-retired-receipt-fences-the-ordinary-owned-pid-boundary
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((before nil) (facts nil) (boundaries nil)
                          (receipt (merge-pathnames "physical-reseed.retired" root))
                          (bytes (sb-ext:string-to-octets "retired receipt" :external-format :utf-8)))
         (ok (handler-case
                 (progn
                   (pg::postmaster-own-data-directory
                    "127.0.0.1" 5439
                    :startup-observer
                    (lambda (lock phase)
                      (push phase boundaries)
                      (if (eq phase :before-pid)
                          (progn
                            (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                            (reseed-follow-owner-test-write receipt bytes)
                            (setf facts (pg::physical-reseed-file-facts
                                         (sb-posix:lstat (namestring receipt)))))
                          (pg::physical-reseed-follow-startup-observation lock phase before))))
                   nil)
               (error (condition)
                 (search "startup branch changed across PID creation"
                         (princ-to-string condition)))))
         (ok (equal before '(:branch :ordinary)))
         (ok (equal boundaries '(:owned-pid :before-pid)))
         (ok (equalp bytes (lineage-test-bytes receipt)))
         (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
         (ng pg::*postmaster-data-lock*)
         (ng pg::*postmaster-pid-stream*)
         (ng pg::*postmaster-start-timestamp*)
         (ng (probe-file (merge-pathnames "postmaster.pid" root)))))))

(deftest reseed-startup-observation-rejects-a-new-retirement-branch-after-pid-creation
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((before nil) (path (merge-pathnames "physical-reseed.follow" root))
                          (bytes (sb-ext:string-to-octets "foreign retirement" :external-format :utf-8)))
         (ok
          (handler-case
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (progn
                         (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                         (reseed-follow-owner-test-write path bytes))
                       (pg::physical-reseed-follow-startup-observation lock phase before))))
                nil)
            (error (condition)
              (search "startup branch changed" (princ-to-string condition)))))
         (ok (equalp bytes (lineage-test-bytes path)))
         (ok (null pg::*postmaster-data-lock*))
         (ok (null pg::*postmaster-pid-stream*))
         (ok (null pg::*postmaster-start-timestamp*))
         (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))))))

(deftest reseed-startup-observation-proves-retirement-under-real-postmaster-lock
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (reseed-follow-owner-test-write
        (merge-pathnames "physical-reseed.follow.tmp" root)
        (pg::physical-reseed-follow-image (reseed-retirement-test-initial-candidate root-fd snapshot)))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (after nil) (reconciled nil))
         (unwind-protect
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                       (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
                 :startup-reconcile
                 (lambda ()
                   (setf reconciled t)
                   (ok (open-stream-p pg::*postmaster-data-lock*))
                   (ok (pg::postmaster-pid-owned-p))))
                (ok (eq :retirement (getf before :branch)))
                (ok (equal before after))
                (ok reconciled)
                (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(defun check-retirement-startup-owner-context (owner)
  (flet ((refused ()
           (handler-case
               (progn (pg::physical-reseed-follow-retirement-check-startup-owner owner) nil)
             (error () t))))
    (let ((pg::*postmaster-data-lock* nil)) (ok (refused)))
    (let ((pg::*postmaster-pid-stream* nil)) (ok (refused)))
    (let ((pg::*postmaster-pid-inode* nil)) (ok (refused)))
    (let ((pg::*postmaster-pid-directory* nil)) (ok (refused)))
    (let ((pg::*wal-ready* t)) (ok (refused)))
    (ok (sb-thread:join-thread (sb-thread:make-thread #'refused)))
    (let* ((fd (sb-sys:fd-stream-fd pg::*postmaster-data-lock*))
           (flags (sb-posix:fcntl fd sb-posix:f-getfd))
           (backup (sb-posix:dup fd)))
      (unwind-protect
           (progn (sb-posix:close fd) (ok (refused)))
        (sb-posix:dup2 backup fd)
        (sb-posix:fcntl fd sb-posix:f-setfd flags)
        (sb-posix:close backup)))
    (dolist (target '(:held :named))
      (let ((original (symbol-function 'pg::physical-adoption-install-mount-id))
            (pid-fd (sb-sys:fd-stream-fd pg::*postmaster-pid-stream*))
            (pid-path (namestring (pg::postmaster-data-path "postmaster.pid")))
            (visited nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                     (lambda (path &key directory-fd)
                       (let ((mount (funcall original path :directory-fd directory-fd)))
                         (if (ecase target
                               (:held (eql directory-fd pid-fd))
                               (:named (and path (equal pid-path (namestring path)))))
                             (progn (setf visited t) (1+ mount))
                             mount))))
               (ok (refused))
               (ok visited))
          (setf (symbol-function 'pg::physical-adoption-install-mount-id) original))))
    (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner))))

(defun check-retirement-startup-root-lifetimes (owner)
  (dolist (mode '(:normal :error :throw :interrupt))
    (let ((held nil) (escaped nil))
      (ok
       (eq :finished
           (catch 'retirement-root-cut
             (handler-case
                 (pg::call-with-physical-reseed-follow-retirement-startup-root
                  owner
                  (lambda (fd verify)
                    (setf held fd escaped verify)
                    (ok (funcall verify))
                    (ecase mode
                      (:normal :finished)
                      (:error (error "retirement root cut"))
                      (:throw (throw 'retirement-root-cut :finished))
                      (:interrupt
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (throw 'retirement-root-cut :finished)))))))
               (error (condition)
                 (if (search "retirement root cut" (princ-to-string condition))
                     :finished
                     (error condition)))))))
      (ok (handler-case (progn (sb-posix:fstat held) nil)
            (sb-posix:syscall-error () t)))
      (ok (handler-case (progn (funcall escaped) nil)
            (error () t)))
      (ok (open-stream-p pg::*postmaster-data-lock*))
      (ok (open-stream-p pg::*postmaster-pid-stream*))
      (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner)))))

(defun check-retirement-startup-owner-exits (expected)
  (dolist (mode '(:error :throw :interrupt))
    (let ((escaped nil)
          (lock pg::*postmaster-data-lock*)
          (pid-stream pg::*postmaster-pid-stream*)
          (position (file-position pg::*postmaster-pid-stream*)))
      (ok
       (eq :finished
           (catch 'retirement-owner-cut
             (handler-case
                 (pg::call-with-physical-reseed-follow-retirement-startup-owner
                  expected
                  (lambda (owner)
                    (setf escaped owner)
                    (ecase mode
                      (:error (error "retirement owner cut"))
                      (:throw (throw 'retirement-owner-cut :finished))
                      (:interrupt
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (throw 'retirement-owner-cut :finished)))))))
               (error (condition)
                 (if (search "retirement owner cut" (princ-to-string condition))
                     :finished
                     (error condition)))))))
      (ok escaped)
      (ok (handler-case
              (progn (pg::physical-reseed-follow-retirement-check-startup-owner escaped) nil)
            (error () t)))
      (ok (and (eq lock pg::*postmaster-data-lock*) (open-stream-p lock)))
      (ok (and (eq pid-stream pg::*postmaster-pid-stream*) (open-stream-p pid-stream)))
      (ok (eql position (file-position pid-stream))))))

(deftest reseed-retirement-startup-owner-borrows-published-postmaster-ownership
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (reseed-follow-owner-test-write
        (merge-pathnames "physical-reseed.follow.tmp" root)
        (pg::physical-reseed-follow-image (reseed-retirement-test-initial-candidate root-fd snapshot)))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (after nil) (reconciled nil) (escaped nil))
         (unwind-protect
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                       (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
                 :startup-reconcile
                 (lambda ()
                   (setf reconciled t)
                   (ok (open-stream-p pg::*postmaster-data-lock*))
                   (ok (pg::postmaster-pid-owned-p))
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    after
                    (lambda (retirement-owner)
                      (setf escaped retirement-owner)
                      (ok (pg::physical-reseed-follow-retirement-check-startup-owner retirement-owner))
                      (ok (not (pg::physical-adoption-owner-p retirement-owner)))
                      (ok (handler-case
                              (progn (pg::physical-reseed-check-attempt-authority retirement-owner) nil)
                            (error () t)))
                      (check-retirement-startup-owner-context retirement-owner)
                      (check-retirement-startup-root-lifetimes retirement-owner)
                      (ok (handler-case (progn (pg::physical-adoption-check-owner retirement-owner) nil)
                            (error () t)))))
                   (check-retirement-startup-owner-exits after)))
                (ok (eq :retirement (getf before :branch)))
                (ok (equal before after))
                (ok reconciled)
                (ok (handler-case
                        (progn (pg::physical-reseed-follow-retirement-check-startup-owner escaped) nil)
                      (error () t)))
                (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(deftest reseed-retirement-startup-owner-refuses-replaced-handover-evidence
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (after nil) (entered nil) (replacement-inode nil) (observed nil)
                                 (path (merge-pathnames "physical-reseed.follow" root))
                                 (replacement (merge-pathnames "replacement" root))
                                 (bytes (pg::physical-reseed-follow-image (getf snapshot :record))))
         (unwind-protect
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                       (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
                 :startup-reconcile
                 (lambda ()
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    after
                    (lambda (retirement-owner)
                      (reseed-follow-owner-test-write replacement bytes)
                      (setf replacement-inode (sb-posix:stat-ino (sb-posix:lstat replacement)))
                      (sb-posix:rename replacement path)
                      (ok (handler-case
                              (progn
                                (pg::call-with-physical-reseed-follow-retirement-startup-observation
                                 retirement-owner
                                 (lambda (descriptors state verify)
                                   (declare (ignore descriptors state verify))
                                   (setf observed t)))
                                nil)
                            (error (condition)
                              (search "target follow final generation changed" (princ-to-string condition)))))
                      (ok (not observed))))
                   (let ((lock pg::*postmaster-data-lock*)
                         (pid-stream pg::*postmaster-pid-stream*)
                         (position (file-position pg::*postmaster-pid-stream*)))
                     (ok (handler-case
                             (progn
                               (pg::call-with-physical-reseed-follow-retirement-startup-owner
                                after (lambda (retirement-owner)
                                        (declare (ignore retirement-owner))
                                        (setf entered t)))
                               nil)
                           (error (condition)
                             (search "target follow final generation changed" (princ-to-string condition)))))
                     (ok (not entered))
                     (ok (and (eq lock pg::*postmaster-data-lock*) (open-stream-p lock)))
                     (ok (and (eq pid-stream pg::*postmaster-pid-stream*) (open-stream-p pid-stream)))
                     (ok (eql position (file-position pid-stream))))))
                (ok (equalp bytes (lineage-test-bytes path)))
                (ok (= replacement-inode (sb-posix:stat-ino (sb-posix:lstat path))))
                (ok (equal (getf snapshot :record)
                           (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(deftest reseed-startup-observation-rejects-byte-identical-final-replacement
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (replacement-inode nil) (reconciled nil)
                                 (path (merge-pathnames "physical-reseed.follow" root))
                                 (replacement (merge-pathnames "replacement" root))
                                 (bytes (pg::physical-reseed-follow-image (getf snapshot :record))))
         (unwind-protect
              (progn
                (ok
                 (handler-case
                     (progn
                       (pg::postmaster-own-data-directory
                        "127.0.0.1" 5439
                        :startup-observer
                        (lambda (lock phase)
                          (if (eq phase :before-pid)
                              (progn
                                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                                (reseed-follow-owner-test-write replacement bytes)
                                (setf replacement-inode (sb-posix:stat-ino (sb-posix:lstat replacement)))
                                (sb-posix:rename replacement path))
                              (pg::physical-reseed-follow-startup-observation lock phase before)))
                        :startup-reconcile (lambda () (setf reconciled t)))
                       nil)
                   (error (condition)
                     (search "retirement evidence changed" (princ-to-string condition)))))
                (ok (eq :retirement (getf before :branch)))
                (ok (not reconciled))
                (ok (equalp bytes (lineage-test-bytes path)))
                (ok (= replacement-inode (sb-posix:stat-ino (sb-posix:lstat path))))
                (ok (equal (getf snapshot :record)
                           (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record)))
                (ok (null pg::*postmaster-pid-stream*))
                (ok (null pg::*postmaster-data-lock*))
                (ok (null pg::*postmaster-start-timestamp*))
                (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(deftest reseed-startup-observation-refuses-incomplete-intent-before-stale-pid-changes
    (dolist (name '("physical-reseed.follow.tmp" "physical-reseed.follow"))
      (call-with-pid-owner
       (lambda (root)
         (let* ((pid-path (merge-pathnames "postmaster.pid" root))
                (pid-text (uiop:read-file-string pid-path))
                (pid-inode (sb-posix:stat-ino (sb-posix:lstat pid-path)))
                (path (merge-pathnames name root))
                (bytes (sb-ext:string-to-octets "incomplete" :external-format :utf-8))
                (entered nil))
           (close pg::*postmaster-pid-stream*)
           (close pg::*postmaster-data-lock*)
           (setf pg::*postmaster-pid-stream* nil pg::*postmaster-data-lock* nil
                 pg::*postmaster-pid-inode* nil pg::*postmaster-start-timestamp* nil)
           (reseed-follow-owner-test-write path bytes)
           (ok
            (handler-case
                (progn
                  (pg::postmaster-own-data-directory
                   "127.0.0.1" 5439
                   :startup-observer
                   (lambda (lock phase)
                     (setf entered phase)
                     (pg::physical-reseed-follow-startup-observation lock phase)))
                  nil)
              (error () t)))
           (ok (eq entered :before-pid))
           (ok (string= pid-text (uiop:read-file-string pid-path)))
           (ok (= pid-inode (sb-posix:stat-ino (sb-posix:lstat pid-path))))
           (ok (equalp bytes (lineage-test-bytes path)))
           (ok (null pg::*postmaster-pid-stream*))
           (ok (null pg::*postmaster-data-lock*))
           (ok (null pg::*postmaster-start-timestamp*)))))))

(deftest reseed-startup-observation-requires-the-correct-boundary-evidence
    (ok (handler-case
            (progn (pg::physical-reseed-follow-startup-observation nil :before-pid '(:branch :ordinary)) nil)
          (error (condition) (search "initial startup observation" (princ-to-string condition)))))
  (ok (handler-case
          (progn (pg::physical-reseed-follow-startup-observation nil :owned-pid) nil)
        (error (condition) (search "requires pre-PID evidence" (princ-to-string condition)))))
  (ok (handler-case
          (progn (pg::physical-reseed-follow-startup-observation nil :unknown) nil)
        (error () t))))

(deftest reseed-startup-projection-matches-real-pre-and-post-pid-observations
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-retirement-observation-fixture
       kind
       (lambda (owner root workspace root-fd snapshot check)
         (declare (ignore owner workspace))
         (let ((candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
               (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
               (pid-path (merge-pathnames "postmaster.pid" root)))
           (dolist (with-temporary '(nil t))
             (when with-temporary
               (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate)))
             (let ((before nil))
               (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                root-fd snapshot check
                (lambda (descriptors state verify)
                  (setf before (pg::physical-reseed-follow-startup-projection descriptors state))
                  (ok (funcall verify))))
               (reseed-follow-owner-test-write
                pid-path (sb-ext:string-to-octets (format nil "~d~%" (sb-posix:getpid))
                                                  :external-format :utf-8))
               (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                root-fd snapshot check
                (lambda (descriptors state verify)
                  (ok (equal before (pg::physical-reseed-follow-startup-projection descriptors state)))
                  (ok (funcall verify))))
               (let ((padding nil))
                 (unwind-protect
                      (progn
                        (dotimes (index 3)
                          (declare (ignorable index))
                          (push (sb-posix:open "/dev/null" sb-posix:o-rdonly) padding))
                        (call-with-reseed-retirement-test-owned-pid
                         root
                         (lambda (stream)
                           (let ((position (file-position stream)))
                             (pg::call-with-physical-reseed-follow-retirement-owned-pid-observation
                              root-fd snapshot check
                              (lambda (descriptors state verify)
                                (ok (equal before (pg::physical-reseed-follow-startup-projection descriptors state)))
                                (ok (funcall verify))))
                             (ok (eql position (file-position stream)))
                             (ok (open-stream-p stream))))))
                   (dolist (fd padding) (sb-posix:close fd)))))))))))
