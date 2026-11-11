(in-package #:pg-physical-tests)

(defun source-logical-promotion-fact (enabled start)
  (pg::physical-logical-fact-next
   nil (pg::make-wal-record :version 8 :type 18 :xid 0 :start start :end (+ start 33)
                            :payload (pg::wal-encode-logical-decoding-status-change enabled))
   1 (pg::timeline-parse-history 1 "")))

(deftest promotion-logical-intent-keeps-configured-level-separate-from-status
    (dolist (pair '((nil :replica) (t :replica) (t :logical)))
      (destructuring-bind (enabled level) pair
        (let ((bytes (pg::physical-logical-encode-promotion-intent enabled level)))
          (ok (= (if (eq level :replica) 1 2) (aref bytes 9)))
          (multiple-value-bind (status configured) (pg::physical-logical-decode-promotion-intent bytes)
            (ok (eql enabled status))
            (ok (eq level configured))))
        (let* ((bytes (pg::physical-recovery-control-image
                       3 42 1 4 32 152 50 :state-time 0 :promotion-child 4
                       :promotion-generation 9 :promotion-source-namespace :legacy
                       :promotion-logical-decoding-enabled enabled :promotion-wal-level level))
               (control (pg::physical-recovery-decode-control bytes)))
          (ok (eql enabled (getf control :promotion-logical-decoding-enabled)))
          (ok (eq level (getf control :promotion-wal-level))))))
  (dolist (level '(nil :minimal :unknown))
    (ok (intake-fails-p
         (lambda () (pg::physical-logical-encode-promotion-intent t level)))))
  (ok (intake-fails-p (lambda () (pg::physical-logical-encode-promotion-intent nil :logical))))
  (ok (intake-fails-p
       (lambda () (pg::physical-recovery-control-image
                   3 42 1 4 32 152 50 :state-time 0 :promotion-child 4
                   :promotion-generation 9 :promotion-source-namespace :legacy
                   :promotion-wal-level :replica)))))

(deftest promotion-logical-plan-freezes-configured-level-independently
    (dolist (pair '((nil :replica) (t :replica) (t :logical)))
      (destructuring-bind (desired level) pair
        (let ((state (promotion-logical-test-plan-state 216 :unknown desired :unknown :legacy)))
          (setf (getf state :promotion-wal-level) level)
          (let* ((plan (pg::physical-promotion-checkpoint-plan state 20000 1003))
                 (first (first (getf plan :frames)))
                 (eor (pg::wal-end-of-recovery-record-info (pg::wal-decode-record (second first) (first first)))))
            (ok (= (if (eq level :replica) 1 2) (getf eor :wal-level)))
            (ok (eql desired (getf (getf plan :snapshot) :logical-decoding-enabled)))))))
  (let ((state (promotion-logical-test-plan-state 216 t t :unknown :legacy)))
    (remf state :promotion-wal-level)
    (ok (intake-fails-p (lambda () (pg::physical-promotion-checkpoint-plan state 20000 1003))))))

(deftest promotion-logical-pending-freezes-policy-across-cold-read-and-configuration-change
    (dolist (source '(:unknown nil t))
      (dolist (desired '(nil t))
        (dolist (namespace '(:legacy :timeline))
          (call-with-promotion-fixture
           (lambda (root store)
             (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
             (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION") (pg::string->bytes (format nil "16~%")))
             (unless (eq source :unknown)
               (let ((end (append-source-logical-test-status store source)))
                 (assert (= end (pg::physical-replay-from pg::*last-replay-lsn*)))
                 (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)))
             (let* ((pg::*startup-installed-configuration*
                      (promotion-logical-test-configuration nil (if desired :logical :replica)))
                    (pg::*wal-level* (if desired :logical :replica))
                    (capture (symbol-function 'pg::startup-promotion-policy-arguments))
                    (calls 0))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::startup-promotion-policy-arguments)
                            (lambda () (incf calls) (funcall capture)))
                      (let* ((pending (call-with-promotion-exclusion
                                       (lambda () (pg::physical-promotion-publish-intent 2))))
                             (control (pg::physical-recovery-read-control root))
                             (before (pg::physical-promotion-checkpoint-plan pending 20000 1003)))
                        (ok (= 1 calls))
                        (ok (get-properties control '(:promotion-logical-decoding-enabled)))
                        (ok (eql desired (getf control :promotion-logical-decoding-enabled)))
                        (ok (eql desired (getf pending :promotion-logical-decoding-enabled)))
                        (ok (eq (if desired :logical :replica) (getf control :promotion-wal-level)))
                        (let ((frame (first (getf before :frames))))
                          (ok (= (if desired 2 1)
                                 (getf (pg::wal-end-of-recovery-record-info
                                        (pg::wal-decode-record (second frame) (first frame))) :wal-level))))
                        (ok (eql desired (getf (getf before :snapshot) :logical-decoding-enabled)))
                        (multiple-value-bind (metadata catalog prefix generation)
                            (pg::physical-restartpoint-read pending)
                          (declare (ignore catalog))
                          (dolist (fault '(:value :presence :level :level-presence))
                            (let ((bad (copy-list pending)))
                              (ecase fault
                                (:value (setf (getf bad :promotion-logical-decoding-enabled) (not desired)))
                                (:presence (remf bad :promotion-logical-decoding-enabled))
                                (:level (setf (getf bad :promotion-wal-level) (if desired :replica :logical)))
                                (:level-presence (remf bad :promotion-wal-level)))
                              (ok (intake-fails-p
                                   (lambda ()
                                     (call-with-promotion-exclusion
                                      (lambda ()
                                        (pg::physical-promotion-check-pending bad metadata prefix generation)))))))))
                        (setf pg::*startup-installed-configuration*
                              (promotion-logical-test-configuration t (if desired :replica :logical))
                              pg::*wal-level* (if desired :replica :logical)
                              (getf pending :promotion-logical-decoding-enabled) (not desired))
                        (dotimes (retry 2)
                          (declare (ignorable retry))
                          (let ((cold (pg::physical-promotion-read-pending-state)))
                            (ok (= 1 calls))
                            (ok (get-properties cold '(:promotion-logical-decoding-enabled)))
                            (ok (eql desired (getf cold :promotion-logical-decoding-enabled)))
                            (ok (eq (if desired :logical :replica) (getf cold :promotion-wal-level)))
                            (ok (equal (getf control :logical-fact) (getf cold :source-logical-fact)))
                            (ok (equalp (getf before :frames)
                                        (getf (pg::physical-promotion-checkpoint-plan cold 20000 1003) :frames)))))))
                 (setf (symbol-function 'pg::startup-promotion-policy-arguments) capture))))
           :namespace namespace
           :system-id (pg::physical-store-update-digest
                       #xCBF29CE484222325 (pg::string->bytes "history-source")))))))

(defun promotion-logical-test-configuration (fpw level)
  (let ((settings (make-hash-table :test #'equal)))
    (setf (gethash "full_page_writes" settings) (list :value fpw))
    (pg::make-startup-configuration :settings settings :wal-level level)))

(deftest promotion-policy-capture-uses-one-installed-root
    (let* ((first (promotion-logical-test-configuration nil :logical))
           (later (promotion-logical-test-configuration t :replica))
           (saved (symbol-function 'pg::startup-capture-configuration))
           (calls 0)
           (pg::*wal-level* :replica))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::startup-capture-configuration)
                   (lambda () (incf calls) (if (= calls 1) first later)))
             (ok (equal '(:promotion-full-page-writes nil :promotion-logical-decoding-enabled t :promotion-wal-level :logical)
                        (pg::startup-promotion-policy-arguments)))
             (ok (= 1 calls)))
        (setf (symbol-function 'pg::startup-capture-configuration) saved)))
  (dolist (fpw '(nil t))
    (dolist (level '(:replica :logical))
      (let ((pg::*startup-installed-configuration* (promotion-logical-test-configuration fpw level)))
        (ok (equal (list :promotion-full-page-writes fpw
                         :promotion-logical-decoding-enabled (eq level :logical) :promotion-wal-level level)
                   (pg::startup-promotion-policy-arguments))))))
  (let ((pg::*startup-installed-configuration* nil))
    (ok (null (pg::startup-promotion-policy-arguments)))))

(deftest promotion-policy-capture-rejects-invalid-installed-state
    (dolist (level '(nil :minimal :unknown))
      (let ((pg::*startup-installed-configuration* (promotion-logical-test-configuration nil level)))
        (ok (intake-fails-p #'pg::startup-promotion-policy-arguments))))
  (dolist (value '(0 1 :off))
    (let ((pg::*startup-installed-configuration* (promotion-logical-test-configuration value :replica)))
      (ok (intake-fails-p #'pg::startup-promotion-policy-arguments)))))

(defun append-promotion-logical-child-checkpoint (store logical fpw)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin pg::+wal-header-size+))
         (facts (append (unless (eq logical :unknown) (list :logical-decoding-enabled logical))
                        (unless (eq fpw :unknown) (list :full-page-writes fpw))))
         (snapshot (append
                    (list :version 2 :start start :end (+ start (pg::wal-checkpoint-record-size facts))
                          :redo start :next-xid 2000 :xid-reserved-through 1999 :next-oid 16384
                          :oldest-active-xid 2000 :time 42 :timeline 2 :prev-timeline 1)
                    facts)))
    (pg::physical-store-input store begin (sample-physical-record begin))
    (pg::physical-store-input store start
                              (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                     (pg::wal-encode-checkpoint-snapshot snapshot)))
    (pg::physical-store-flush store)))

(defun call-with-promotion-logical-readiness-case (enabled fpw fault function)
  (with-restartpoint-fixture (root store)
    (declare (ignore root))
    (selected-replay-test-prepare store)
    (unless (or (eq fpw :unknown) (eq fault :reverse))
      (append-readiness-fpw-change store fpw))
    (append-source-logical-test-status store enabled)
    (case fault
      (:duplicate (append-source-logical-test-status store enabled))
      (:reverse (append-readiness-fpw-change store nil)))
    (append-promotion-logical-child-checkpoint
     store (case fault (:missing :unknown) (:contradiction (not enabled)) (otherwise enabled))
     (if (eq fault :reverse) nil fpw))
    (funcall function store)))

(deftest promotion-logical-readiness-accepts-ordered-markers-without-publication
    (dolist (enabled '(nil t))
      (dolist (fpw '(:unknown nil t))
        (call-with-promotion-logical-readiness-case
         enabled fpw nil
         (lambda (store)
           (declare (ignore store))
           (let ((before (copy-tree pg::*physical-recovery-state*))
                 (writer pg::*wal-logical-policy*))
             (ok (not (pg::physical-promotion-awaiting-child-initialization-p)))
             (ok (equal before pg::*physical-recovery-state*))
             (ok (eq writer pg::*wal-logical-policy*))
             (ok (= 96 pg::*last-replay-lsn*))
             (ok (= 1 (pg::physical-recovery-replay-timeline)))))))))

(deftest promotion-logical-readiness-rejects-marker-order-and-checkpoint-policy-loss
    (dolist (enabled '(nil t))
      (dolist (fault '(:duplicate :reverse :missing :contradiction))
        (call-with-promotion-logical-readiness-case
         enabled :unknown fault
         (lambda (store)
           (declare (ignore store))
           (let* ((before (copy-tree pg::*physical-recovery-state*))
                  (condition (handler-case
                                 (progn (pg::physical-promotion-awaiting-child-initialization-p) nil)
                               (error (condition) condition))))
             (ok condition)
             (ok (and condition (search "logical" (princ-to-string condition) :test #'char-equal)))
             (ok (equal before pg::*physical-recovery-state*))
             (ok (= 96 pg::*last-replay-lsn*))))))))

(defun promotion-logical-test-private-scan (mode store end)
  (ecase mode
    (:resources
     (multiple-value-bind (resources fpw logical)
         (pg::physical-recovery-scan-resource-fact store 32 end nil pg::*physical-recovery-state* :logical-seed nil)
       (declare (ignore resources fpw))
       logical))
    (:drops (nth-value 1 (pg::physical-recovery-proven-drops store "history-source")))
    (:readiness (pg::physical-promotion-awaiting-child-initialization-p))
    (:follow
     (getf
      (pg::physical-follow-scan-replay-from-seed-core
       store pg::*physical-recovery-state* (list end 2) '(32 1) 50 '(32 1 50 nil nil nil nil nil)
       (lambda (function start)
         (pg::physical-recovery-map-permitted-records store function :start-lsn start))
       nil)
      :applied-logical-fact))))

(deftest promotion-logical-private-scans-fold-policy-without-publication
    (dolist (mode '(:resources :drops :readiness :follow))
      (dolist (enabled '(nil t))
        (dolist (fault '(nil :duplicate :reverse :missing :contradiction))
          (call-with-promotion-logical-readiness-case
           enabled :unknown fault
           (lambda (store)
             (let ((before (copy-tree pg::*physical-recovery-state*))
                   (writer pg::*wal-logical-policy*)
                   (end (pg::physical-store-flushed-lsn store)))
               (if fault
                   (ok (signals (promotion-logical-test-private-scan mode store end) 'error))
                   (let ((result (promotion-logical-test-private-scan mode store end)))
                     (ecase mode
                       ((:resources :follow)
                        (ok (eq enabled (getf result :enabled)))
                        (ok (= end (getf result :through)))
                        (ok (= 2 (getf result :through-timeline))))
                       (:drops (ok (= end result)))
                       (:readiness (ok (null result))))))
               (ok (equal before pg::*physical-recovery-state*))
               (ok (eq writer pg::*wal-logical-policy*))
               (ok (= 96 pg::*last-replay-lsn*)))))))))

(deftest promotion-logical-full-replay-publishes-child-checkpoint-policy
    (let ((pg::*physical-replay-restarting-p* t))
      (dolist (enabled '(nil t))
        (dolist (fpw '(:unknown nil t))
          (call-with-promotion-logical-readiness-case
           enabled fpw nil
           (lambda (store)
             (let ((end (pg::physical-store-flushed-lsn store))
                   (writer pg::*wal-logical-policy*))
               (pg::clog-init)
               (unwind-protect
                    (progn
                      (ok (= end (pg::physical-replay-from 96)))
                      (ok (= 2 (pg::physical-recovery-replay-timeline)))
                      (let ((fact (getf pg::*physical-recovery-state* :source-logical-fact)))
                        (ok (eq enabled (getf fact :enabled)))
                        (ok (= end (getf fact :through)))
                        (ok (= 2 (getf fact :through-timeline))))
                      (ok (eq writer pg::*wal-logical-policy*))
                      (ok (eq enabled
                              (getf (getf pg::*physical-recovery-state* :source-checkpoint)
                                    :logical-decoding-enabled))))
                 (close pg::*clog-stream*)
                 (setf pg::*clog-stream* nil)))))))))

(deftest promotion-logical-row-replay-preserves-child-policy-order
    (let ((pg::*physical-replay-restarting-p* t))
      (dolist (enabled '(nil t))
        (dolist (fault '(nil :duplicate :reverse :missing :contradiction))
          (call-with-promotion-logical-readiness-case
           enabled :unknown fault
           (lambda (store)
             (let ((end (pg::physical-store-flushed-lsn store))
                   (writer pg::*wal-logical-policy*))
               (setf pg::*catalog* (pg::load-catalog))
               (pg::clog-init)
               (unwind-protect
                    (progn
                      (catch 'eor-published
                        (let ((pg::*physical-replay-test-hook*
                                (lambda (point record)
                                  (declare (ignore record))
                                  (when (eq point :after-eor-progress)
                                    (throw 'eor-published t)))))
                          (pg::physical-replay-from 96)))
                      (ok (= 2 (pg::physical-recovery-replay-timeline)))
                      (if (member fault '(:duplicate :reverse))
                          (ok (signals (pg::physical-row-replay-prefix 0) 'error))
                          (progn
                            (ok (eq :deferred (pg::physical-row-replay-prefix 0)))
                            (let ((fact (getf pg::*physical-recovery-state* :source-logical-fact)))
                              (ok (eq enabled (getf fact :enabled)))
                              (ok (= pg::*last-replay-lsn* (getf fact :through)))
                              (ok (= 2 (getf fact :through-timeline))))
                            (if fault
                                (ok (signals (pg::physical-replay-from pg::*last-replay-lsn*) 'error))
                                (ok (= end (pg::physical-replay-from pg::*last-replay-lsn*))))))
                      (when fault
                        (ok (< pg::*last-replay-lsn* end))
                        (ok (not (eql 2 (getf (getf pg::*physical-recovery-state* :source-checkpoint)
                                              :timeline)))))
                      (ok (eq writer pg::*wal-logical-policy*)))
                 (close pg::*clog-stream*)
                 (setf pg::*clog-stream* nil)))))))))

(deftest promotion-logical-live-completion-installs-frozen-child-policy
    (dolist (source '(:unknown nil t))
      (dolist (enabled '(nil t))
        (call-with-live-child-fixture
         (lambda (root store)
           (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                       (pg::string->bytes (format nil "16~%")))
           (unless (eq source :unknown)
             (let* ((pg::*physical-replay-restarting-p* t)
                    (end (append-source-logical-test-status store source)))
               (assert (= end (pg::physical-replay-from pg::*last-replay-lsn*)))
               (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)))
           (let ((pg::*startup-installed-configuration*
                   (promotion-logical-test-configuration nil (if enabled :logical :replica)))
                 (catalog pg::*catalog*)
                 (clog pg::*clog-stream*)
                 (pg::*wal-level* (if enabled :logical :replica))
                 (incarnation pg::*wal-storage-incarnation*)
                 (end nil))
             (call-with-current-promotion-owner
              (lambda ()
                (call-with-promotion-exclusion
                 (lambda ()
                   (pg::call-with-physical-promotion-readers
                    (lambda ()
                      (let* ((pending (pg::physical-promotion-publish-intent))
                             (plan (pg::physical-promotion-current-checkpoint-plan pending))
                             (expected (getf plan :end)))
                        (ok (eq enabled (getf (getf plan :snapshot) :logical-decoding-enabled)))
                        (setf pg::*startup-installed-configuration*
                              (promotion-logical-test-configuration t (if enabled :replica :logical)))
                        (pg::physical-promotion-install-prefix)
                        (setf end (pg::physical-promotion-complete-child))
                        (ok (= expected end)))))))))
             (ok (not (pg::recovery-in-progress-p)))
             (ok (= 2 pg::*wal-insert-timeline*))
             (ok (<= end pg::*wal-durable-lsn* pg::*wal-end-lsn*))
             (ok (eq catalog pg::*catalog*))
             (ok (eq clog pg::*clog-stream*))
             (ok (not (eq incarnation pg::*wal-storage-incarnation*)))
             (let* ((policy pg::*wal-logical-policy*)
                    (state (pg::wal-logical-policy-state policy)))
               (ok (pg::wal-logical-policy-valid-p policy))
               (ok (eq pg::*wal-storage-incarnation* (pg::wal-logical-policy-incarnation policy)))
               (ok (eq enabled (pg::wal-logical-status-enabled state))))
             (ok (eq enabled (getf pg::*wal-checkpoint-snapshot* :logical-decoding-enabled)))))
         :committed-child nil))))

(defun promotion-logical-cold-child-form (root marker enabled kind)
  `(let ((pg::*pgdata* ,root)
         (pg::*wal-level* ,(if enabled :replica :logical))
         (pg::*installed-resource-parameters* (pg::make-resource-parameters 100 8 10 0 64))
         (calls 0) (cut nil) (end nil))
     (pg::postmaster-own-data-directory "127.0.0.1" 5439)
     (unwind-protect
          (let* ((frozen-intent (pg::physical-promotion-read-pending-state))
                 (frame (multiple-value-list (pg::physical-promotion-eor-image frozen-intent)))
                 (pg::*physical-promotion-test-hook*
                   (lambda (point)
                     (when (eq point :after-child-logical-write)
                       (incf calls)
                       ,(when (eq kind :cut) '(error "injected logical child write"))))))
            (handler-case
                (setf end
                      (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                        (sb-thread:with-mutex (pg::*reload-run-lock*)
                          (pg::with-live-operation-quiescence
                            (pg::physical-promotion-resume-pending
                             :artifact-checker #'pg::physical-promotion-checkpoint-artifacts
                             :publisher #'pg::physical-promotion-complete-child)))))
              (error (condition)
                (if (search "injected logical child write" (princ-to-string condition))
                    (setf cut t)
                    (error condition))))
            (let* ((control (when ,(eq kind :cut) (pg::physical-recovery-read-control ,root)))
                   (checks
                     ,(if (eq kind :cut)
                          `(list cut (= calls 1) pg::*physical-recovery-failed-p*
                                 (= 3 (getf control :phase))
                                 (eql ,enabled (getf control :promotion-logical-decoding-enabled))
                                 (eq ,(if enabled :logical :replica) (getf control :promotion-wal-level))
                                 (null pg::*physical-receive-store*) (null pg::*clog-stream*))
                          `(list (not cut) (plusp calls) (not pg::*physical-recovery-failed-p*)
                                 (not (pg::recovery-in-progress-p))
                                 (and (integerp end) (plusp end))
                                 (eql ,enabled (getf pg::*wal-checkpoint-snapshot* :logical-decoding-enabled))
                                 (= 2 pg::*wal-insert-timeline*)
                                 (let* ((raw (first frame)) (start (second frame))
                                                            (actual (make-array (length raw) :element-type '(unsigned-byte 8))))
                                   (with-open-file
                                       (input (pg::wal-segment-path (floor start pg::+wal-segment-size+) 2 :timeline)
                                              :element-type '(unsigned-byte 8))
                                     (file-position input (mod start pg::+wal-segment-size+))
                                     (and (= (length actual) (read-sequence actual input))
                                          (equalp raw actual))))))))
              (with-open-file (out ,marker :direction :output :if-exists :error)
                (write (list (sb-posix:getpid) ,kind checks) :stream out :readably t))))
       (when pg::*physical-receive-store* (pg::physical-store-close pg::*physical-receive-store*))
       (when pg::*clog-stream* (close pg::*clog-stream*))
       (pg::smgr-close-all)
       (pg::postmaster-remove-pid-file))))

(deftest promotion-logical-cold-resume-reuses-intent-after-status-write-failure
    (call-with-physical-store-directory
     (lambda (cache-root)
       (dolist (enabled '(nil t))
         (let ((pg::*startup-installed-configuration*
                 (promotion-logical-test-configuration nil (if enabled :logical :replica)))
               (pg::*wal-level* (if enabled :logical :replica)))
           (call-with-cold-promotion-image
            (lambda (root driver)
              (dolist (kind '(:cut :resume))
                (let ((child (merge-pathnames (format nil "~(~a~)/" kind) driver)))
                  (ensure-directories-exist (merge-pathnames "result" child))
                  (dolist (value
                           (run-promotion-handoff-child
                            root child enabled kind :form-builder #'promotion-logical-cold-child-form
                            :cache (merge-pathnames "cache/" cache-root)))
                    (ok value (format nil "~a desired ~a" kind enabled))))))))))))

(defun promotion-logical-test-plan-state (fork source desired fpw-source fpw-desired)
  (let ((state (list :phase 3 :applied fork :timeline 1 :promotion-child 2
                     :next-xid 50 :state-time 1793030000
                     :source-reservation (list :version 1 :through fork :bound 1002 :complete t))))
    (unless (eq source :unknown)
      (let ((fact (source-logical-promotion-fact source 32)))
        (setf (getf fact :through) fork (getf state :source-logical-fact) fact)))
    (unless (eq fpw-source :unknown)
      (setf (getf state :source-fpw-fact)
            (pg::physical-fpw-fact-from-state
             (pg::wal-fpw-state-from-checkpoint
              (sample-checkpoint-snapshot :version 2 :full-page-writes fpw-source)
              (make-symbol "PLAN-FPW")) fork 1)))
    (unless (eq desired :legacy)
      (setf (getf state :promotion-logical-decoding-enabled) desired
            (getf state :promotion-wal-level) (if desired :logical :replica)))
    (unless (eq fpw-desired :legacy) (setf (getf state :promotion-full-page-writes) fpw-desired))
    state))

(defun promotion-logical-test-plan-valid-p (plan source desired fpw-source fpw-desired)
  (let* ((records (mapcar (lambda (frame) (pg::wal-decode-record (second frame) (first frame)))
                          (getf plan :frames)))
         (change (and (not (eq desired :legacy)) (or (eq source :unknown) (not (eql source desired)))))
         (fpw-change (and (not (eq fpw-desired :legacy))
                          (or (eq fpw-source :unknown) (not (eql fpw-source fpw-desired)))))
         (marker (find pg::+wal-record-logical-decoding-status-change+ records :key #'pg::wal-record-type))
         (snapshot (getf plan :snapshot)))
    (and (equal (mapcar #'pg::wal-record-type records)
                (append (list pg::+wal-record-end-of-recovery+)
                        (when fpw-change (list pg::+wal-record-batch-start+ pg::+wal-record-full-page-writes-change+))
                        (when change (list pg::+wal-record-batch-start+ pg::+wal-record-logical-decoding-status-change+))
                        (list pg::+wal-record-batch-start+ pg::+wal-record-checkpoint+)))
         (eql (not (eq desired :legacy))
              (not (null (get-properties snapshot '(:logical-decoding-enabled)))))
         (or (eq desired :legacy) (eql desired (getf snapshot :logical-decoding-enabled)))
         (or (not marker) (eql desired (pg::wal-logical-decoding-status-record-info marker)))
         (equal snapshot (pg::wal-checkpoint-record-snapshot (car (last records))))
         (loop with previous = (getf plan :fork)
               for (start bytes) in (getf plan :frames)
               always (and (<= previous start)
                           (<= (+ (mod start pg::+wal-segment-size+) (length bytes)) pg::+wal-segment-size+))
               do (setf previous (+ start (length bytes)))))))

(cl-spec:defproperty promotion-logical-plans-use-only-frozen-intent
    ((fork (range integer 216 50000000)) (source (member :unknown nil t))
                                         (desired (member :legacy nil t)) (fpw-source (member :unknown nil t))
                                         (fpw-desired (member :legacy nil t)))
  (:about pg::physical-promotion-checkpoint-plan)
  (:trials (:normal 150))
  (let* ((state (promotion-logical-test-plan-state fork source desired fpw-source fpw-desired))
         (before (copy-tree state))
         (plan (pg::physical-promotion-checkpoint-plan state 20000 1003)))
    (and (equal state before)
         (promotion-logical-test-plan-valid-p plan source desired fpw-source fpw-desired))))

(deftest promotion-logical-plan-selects-marker-and-checkpoint-from-frozen-intent
    (dolist (fork (cons 216 (mapcar (lambda (delta) (- pg::+wal-segment-size+ delta))
                                    '(0 1 31 32 33 55 64 65 87 88 119 120 151 152))))
      (dolist (source '(:unknown nil t))
        (dolist (desired '(:legacy nil t))
          (dolist (fpw-source '(:unknown nil t))
            (dolist (fpw-desired '(:legacy nil t))
              (let* ((state (promotion-logical-test-plan-state fork source desired fpw-source fpw-desired))
                     (before (copy-tree state))
                     (plan (pg::physical-promotion-checkpoint-plan state 20000 1003)))
                (ok (and (equal state before)
                         (promotion-logical-test-plan-valid-p plan source desired fpw-source fpw-desired))
                    (format nil "~s" (list fork source desired fpw-source fpw-desired))))))))))

(deftest generated-promotion-logical-plan-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'promotion-logical-plans-use-only-frozen-intent
                                       :seed 7501 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest promotion-logical-plan-retries-ignore-current-configuration
    (dolist (desired '(:legacy nil t))
      (let* ((state (promotion-logical-test-plan-state 216 :unknown desired t nil))
             (pg::*wal-level* :replica)
             (before (pg::physical-promotion-checkpoint-plan state 20000 1003))
             (pg::*wal-level* :logical)
             (after (pg::physical-promotion-checkpoint-plan state 20000 1003)))
        (ok (equalp before after)))))

(deftest promotion-logical-plan-rejects-invalid-intent-or-source-frontier
    (let ((state (promotion-logical-test-plan-state 216 t nil :unknown :legacy)))
      (setf (getf state :promotion-logical-decoding-enabled) 0)
      (ok (intake-fails-p (lambda () (pg::physical-promotion-checkpoint-plan state 20000 1003)))))
  (dolist (pair '((:through 215) (:through-timeline 2) (:enabled 3)))
    (let ((state (promotion-logical-test-plan-state 216 t nil :unknown :legacy)))
      (setf (getf (getf state :source-logical-fact) (first pair)) (second pair))
      (ok (intake-fails-p (lambda () (pg::physical-promotion-checkpoint-plan state 20000 1003)))))))

(cl-spec:defgenerator promotion-logical-intent-arguments ()
  (list (zerop (random 2))))

(cl-spec:defspec-function pg::physical-logical-encode-promotion-intent
    (:args (enabled (type boolean)))
  (:args-generator promotion-logical-intent-arguments)
  (:returns (type (simple-array (unsigned-byte 8) (18))))
  (:post (eql enabled (pg::physical-logical-decode-promotion-intent result))))

(cl-spec:defproperty promotion-logical-intent-roundtrips-known-off-and-on
    ((enabled (member nil t)))
  (:about pg::physical-logical-encode-promotion-intent pg::physical-logical-decode-promotion-intent)
  (:trials (:normal 150))
  (eql enabled (pg::physical-logical-decode-promotion-intent
                (pg::physical-logical-encode-promotion-intent enabled))))

(defun promotion-logical-codec-image (logical &key (fpw :absent) resources fpw-source source selected retained)
  (let ((logical-fact (when source (source-logical-promotion-fact t 32))))
    (when logical-fact (setf (getf logical-fact :through) 152))
    (apply #'pg::physical-recovery-control-image
           (append
            (list 3 42 1 4 32 152 50 :state-time 0 :promotion-child 4
                                     :promotion-generation 9 :promotion-source-namespace :legacy
                                     :history-target (when selected 3) :history-digest (when selected 7)
                                     :retained-reference
                                     (when retained '(:root-id 1 :base-timeline 1 :base 32 :manifest-digest 4 :end 152 :end-digest 7))
                                     :resource-fact
                                     (when resources '(:resource-parameters (100 8 10 0 128) :resource-lsn 16 :resource-timeline 1))
                                     :fpw-fact (when fpw-source
                                                 (pg::physical-fpw-fact-from-state
                                                  (pg::wal-fpw-state-from-checkpoint
                                                   (sample-checkpoint-snapshot :version 2 :full-page-writes nil)
                                                   (make-symbol "CODEC-FPW")) 152 1))
                                     :logical-fact logical-fact)
            (unless (eq fpw :absent) (list :promotion-full-page-writes fpw))
            (unless (eq logical :absent) (list :promotion-logical-decoding-enabled logical
                                               :promotion-wal-level (if logical :logical :replica)))))))

(defun promotion-logical-codec-refresh-crc (bytes)
  (pg::wal-set-u32 bytes (- (length bytes) 4) 0)
  (pg::wal-set-u32 bytes (- (length bytes) 4) (pg::wal-crc32 bytes))
  bytes)

(deftest promotion-logical-intent-generated-contracts
    (dolist (data (list
                   (cl-spec:result-data
                    (cl-spec:check-function 'pg::physical-logical-encode-promotion-intent :trials 150 :seed 7401))
                   (cl-spec:result-data
                    (cl-spec:run-property 'promotion-logical-intent-roundtrips-known-off-and-on
                                          :seed 7402 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest promotion-logical-intent-rejects-noncanonical-bytes-and-values
    (dolist (enabled '(nil t))
      (let ((bytes (pg::physical-logical-encode-promotion-intent enabled)))
        (ok (= 18 (length bytes)))
        (ok (equalp #(80 71 76 79 71 80 73 49) (subseq bytes 0 8)))
        (ok (eql enabled (pg::physical-logical-decode-promotion-intent bytes)))
        (dotimes (index 18)
          (let ((bad (copy-seq bytes)))
            (setf (aref bad index) (if (= index 8) 2 (logxor 1 (aref bad index))))
            (ok (intake-fails-p (lambda () (pg::physical-logical-decode-promotion-intent bad))))))
        (dolist (bad (list (subseq bytes 0 17)
                           (concatenate '(vector (unsigned-byte 8)) bytes #(0))))
          (ok (intake-fails-p (lambda () (pg::physical-logical-decode-promotion-intent bad)))))))
  (dolist (value '(0 1 :off "on"))
    (ok (intake-fails-p (lambda () (pg::physical-logical-encode-promotion-intent value))))))

(deftest promotion-logical-control-composes-intents-with-every-optional-layout
    (ok (equal
         '(((88 16590158288205227922) (356 3657234055354259330))
           ((105 1046427146213484383) (373 8669646262245995252))
           ((105 623693058593181923) (373 9753913470866187585)))
         (loop for fpw in '(:absent nil t) collect
                                           (loop for full in '(nil t) collect
                                                                      (let ((bytes (promotion-logical-codec-image
                                                                                    :absent :fpw fpw :resources full :fpw-source full :source full
                                                                                    :selected full :retained full)))
                                                                        (list (length bytes)
                                                                              (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))))))
  (dolist (resources '(nil t))
    (dolist (fpw-source '(nil t))
      (dolist (source '(nil t))
        (dolist (selected '(nil t))
          (dolist (retained '(nil t))
            (dolist (fpw '(:absent nil t))
              (let* ((args (list :resources resources :fpw-source fpw-source :source source
                                 :selected selected :retained retained :fpw fpw))
                     (old (apply #'promotion-logical-codec-image :absent args))
                     (old-control (pg::physical-recovery-decode-control old)))
                (ok (not (get-properties old-control '(:promotion-logical-decoding-enabled))))
                (dolist (enabled '(nil t))
                  (let* ((bytes (apply #'promotion-logical-codec-image enabled args))
                         (decoded (pg::physical-recovery-decode-control bytes)))
                    (ok (= (+ 18 (length old)) (length bytes)))
                    (ok (get-properties decoded '(:promotion-logical-decoding-enabled)))
                    (ok (eql enabled (getf decoded :promotion-logical-decoding-enabled)))
                    (remf decoded :promotion-logical-decoding-enabled)
                    (remf decoded :promotion-wal-level)
                    (ok (equal old-control decoded)))))))))))
  (ok (= 391 (length (promotion-logical-codec-image
                      nil :fpw nil :resources t :fpw-source t :source t :selected t :retained t)))))

(deftest promotion-logical-control-rejects-malformed-intent-under-valid-crc
    (let* ((bytes (promotion-logical-codec-image
                   nil :fpw t :resources t :fpw-source t :source t :selected t :retained t))
           (offset (- (length bytes) 4 88 18)))
      (dotimes (index 18)
        (let ((bad (copy-seq bytes)))
          (setf (aref bad (+ offset index)) (if (= index 8) 255 (logxor 1 (aref bad (+ offset index)))))
          (promotion-logical-codec-refresh-crc bad)
          (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bad)))))))
  (let* ((bytes (promotion-logical-codec-image nil :fpw t :source t))
         (logical-offset (- (length bytes) 4 88 18))
         (fpw-offset (- logical-offset 17))
         (reordered (concatenate '(vector (unsigned-byte 8))
                                 (subseq bytes 0 fpw-offset)
                                 (subseq bytes logical-offset (+ logical-offset 18))
                                 (subseq bytes fpw-offset logical-offset)
                                 (subseq bytes (+ logical-offset 18)))))
    (promotion-logical-codec-refresh-crc reordered)
    (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control reordered)))))
  (dolist (logical '(nil t))
    (let* ((bytes (pg::physical-recovery-control-image
                   3 42 1 4 32 152 18446744073709551616
                   :state-time 0 :promotion-child 4 :promotion-generation 9
                   :promotion-source-namespace :legacy :promotion-full-page-writes nil
                   :promotion-logical-decoding-enabled logical
                   :promotion-wal-level (if logical :logical :replica)))
           (decoded (pg::physical-recovery-decode-control bytes)))
      (ok (= 18446744073709551616 (getf decoded :next-xid)))
      (ok (get-properties decoded '(:promotion-logical-decoding-enabled)))
      (ok (eql logical (getf decoded :promotion-logical-decoding-enabled)))))
  (dolist (phase '(0 1 2))
    (dolist (enabled '(nil t))
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-control-image
                       phase 42 1 0 32 32 50 :promotion-logical-decoding-enabled enabled
                       :promotion-wal-level (if enabled :logical :replica)))))))
  (ok (intake-fails-p (lambda () (promotion-logical-codec-image 1)))))

(deftest source-logical-promotion-backlog-checks-the-published-logical-seed
    (dolist (enabled '(:unknown nil t))
      (let* ((fact (unless (eq enabled :unknown) (source-logical-promotion-fact enabled 32)))
             (pg::*physical-recovery-state* (list :cluster-identity "logical-backlog"
                                                  :source-logical-fact fact))
             (pg::*physical-promotion-preserve-readers-p* t)
             (pg::*last-replay-lsn* 65) (pg::*next-xid* 3)
             (functions '(pg::physical-recovery-flush pg::physical-recovery-reader-conflict-p
                          pg::physical-recovery-replay-core pg::physical-recovery-proven-drops))
             (saved (mapcar #'symbol-function functions))
             (calls 0) (arguments nil))
        (unwind-protect
             (progn
               (setf (symbol-function (first functions)) (lambda () nil)
                     (symbol-function (second functions)) (lambda () nil)
                     (symbol-function (third functions)) (lambda () nil)
                     (symbol-function (fourth functions))
                     (lambda (store identity &rest keys)
                       (declare (ignore store identity))
                       (incf calls) (setf arguments keys)
                       (values nil 65)))
               (ok (pg::physical-promotion-replay-backlog))
               (ok (= 1 calls))
               (ok (equal fact (getf arguments :logical-seed)))
               (when (eq enabled :unknown)
                 (ok (not (get-properties arguments '(:logical-seed))))))
          (loop for name in functions for function in saved
                do (setf (symbol-function name) function))))))

(deftest source-logical-readiness-rejects-a-seed-at-the-wrong-frontier
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (let* ((fact (source-logical-promotion-fact t 32))
             (before nil))
        (setf (getf fact :through) 96
              (getf pg::*physical-recovery-state* :source-logical-fact) fact)
        (ok (pg::physical-promotion-awaiting-child-initialization-p))
        (decf (getf fact :through))
        (setf before (copy-tree pg::*physical-recovery-state*))
        (ok (intake-fails-p #'pg::physical-promotion-awaiting-child-initialization-p))
        (ok (equal before pg::*physical-recovery-state*))
        (ok (= 96 pg::*last-replay-lsn*)))))

(cl-spec:defgenerator source-logical-promotion-arguments ()
  (let* ((fact (source-logical-promotion-fact (zerop (random 2)) (+ 32 (random 1000000))))
         (end (getf fact :through)))
    (list (list :logical-fact fact)
          (list :lsn end :timeline 1 :source-logical-fact fact))))

(cl-spec:defspec-function pg::physical-promotion-frozen-logical-fact
    (:args (control (type list)) (metadata (type list)))
  (:args-generator source-logical-promotion-arguments)
  (:pre (and (pg::physical-logical-fact-valid-p (getf metadata :source-logical-fact))
             (equal (getf control :logical-fact) (getf metadata :source-logical-fact))))
  (:returns (type list))
  (:post (and (equal result (getf metadata :source-logical-fact))
              (not (eq result (getf metadata :source-logical-fact)))
              (not (eq result (getf control :logical-fact))))))

(cl-spec:defproperty frozen-source-logical-fact-preserves-generation-evidence
    ((enabled (member nil t)) (start (range integer 32 1000000)) (saved (member nil t)))
  (:about pg::physical-promotion-frozen-logical-fact)
  (:trials (:normal 150))
  (let* ((fact (source-logical-promotion-fact enabled start))
         (metadata (list :lsn (+ start 33) :timeline 1 :source-logical-fact fact))
         (control (when saved (list :logical-fact fact)))
         (result (pg::physical-promotion-frozen-logical-fact control metadata))
         (bad (copy-tree fact)))
    (decf (getf bad :coverage-start))
    (and (equal result fact) (not (eq result fact))
         (pg::physical-logical-fact-valid-p bad)
         (intake-fails-p
          (lambda () (pg::physical-promotion-frozen-logical-fact (list :logical-fact bad) metadata))))))

(deftest generated-source-logical-promotion-frozen-fact-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'pg::physical-promotion-frozen-logical-fact :trials 150 :seed 7201))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data)))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'frozen-source-logical-fact-preserves-generation-evidence
                                     :seed 7202 :profile :normal))))
    (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest source-logical-pending-promotion-keeps-frozen-source-on-cold-read
    (dolist (source '(:unknown nil t))
      (dolist (namespace '(:legacy :timeline))
        (call-with-promotion-fixture
         (lambda (root store)
           (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
           (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION")
                                       (pg::string->bytes (format nil "16~%")))
           (unless (eq source :unknown)
             (let ((end (append-source-logical-test-status store source)))
               (ok (= end (pg::physical-replay-from pg::*last-replay-lsn*)))
               (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)))
           (let* ((expected (copy-tree (getf pg::*physical-recovery-state* :source-logical-fact)))
                  (primary pg::*wal-logical-policy*)
                  (pending (call-with-promotion-exclusion
                            (lambda () (pg::physical-promotion-publish-intent 2))))
                  (disk (pg::physical-recovery-read-control root)))
             (ok (equal expected (getf disk :logical-fact)))
             (ok (equal expected (getf pending :source-logical-fact)))
             (ok (equal expected (getf pending :required-logical-fact)))
             (when expected
               (ok (not (eq (getf pending :source-logical-fact)
                            (getf pending :required-logical-fact)))))
             (dotimes (retry 2)
               (declare (ignorable retry))
               (multiple-value-bind (cold metadata prefix generation) (pg::physical-promotion-read-pending-state)
                 (declare (ignore prefix generation))
                 (ok (equal expected (getf cold :source-logical-fact)))
                 (ok (equal expected (getf cold :required-logical-fact)))
                 (when expected
                   (ok (not (eq (getf cold :source-logical-fact) (getf cold :required-logical-fact))))
                   (ok (not (eq (getf cold :source-logical-fact) (getf metadata :source-logical-fact)))))))
             (ok (eq primary pg::*wal-logical-policy*))))
         :namespace namespace
         :system-id (pg::physical-store-update-digest
                     #xCBF29CE484222325 (pg::string->bytes "history-source"))))))
