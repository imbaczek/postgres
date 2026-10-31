(in-package #:pg-physical-tests)

(cl-spec:defproperty follow-desired-generation-is-stable-for-identical-content
    ((size (range integer 0 2048)) (value (range integer 1 255)))
  (:about pg::physical-follow-next-desired)
  (:trials (:normal 200))
  (let* ((bytes (make-array size :element-type '(unsigned-byte 8) :initial-element value))
         (policy (list :kind :latest))
         (first (pg::physical-follow-next-desired nil bytes "public_slot" "receiver" policy t))
         (again (pg::physical-follow-next-desired first (copy-seq bytes)
                                                  (copy-seq "public_slot") (copy-seq "receiver") policy t)))
    (and (eq first again)
         (equalp bytes (pg::physical-follow-desired-conninfo first)))))

(deftest generated-follow-desired-generation-content-identity
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-desired-generation-is-stable-for-identical-content :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest desired-install-fences-preflight-and-publication
    (let* ((pg::*physical-follow-desired* nil)
           (desired (pg::physical-follow-next-desired nil "" "" "rx" nil nil))
           (events nil))
      (flet ((record (stage)
               (push (list stage (sb-thread:holding-mutex-p pg::*physical-follow-generation-lock*))
                     events)))
        (pg::physical-follow-call-with-desired-install
         desired (lambda () (record :publish)) (lambda () (record :preflight)))
        (ok (equal '((:preflight t) (:preflight t) (:publish t)) (reverse events)))
        (ok (eq desired pg::*physical-follow-desired*)))))

(deftest follow-desired-generation-copies-private-input-and-getters
    (let* ((bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(65 255 128)))
           (slot (copy-seq "public_slot"))
           (fallback (copy-seq "receiver"))
           (desired (pg::physical-follow-next-desired nil bytes slot fallback nil t)))
      (setf (aref bytes 0) 0 (char slot 0) #\x (char fallback 0) #\x)
      (setf (aref (pg::physical-follow-desired-conninfo desired) 1) 0
            (char (pg::physical-follow-desired-slot desired) 0) #\y
            (char (pg::physical-follow-desired-fallback desired) 0) #\y)
      (ok (equalp '(65 255 128) (coerce (pg::physical-follow-desired-conninfo desired) 'list)))
      (ok (string= "public_slot" (pg::physical-follow-desired-slot desired)))
      (ok (string= "receiver" (pg::physical-follow-desired-fallback desired)))
      (ok (not (search "receiver" (princ-to-string desired))))))

(deftest follow-desired-generation-does-not-reuse-an-a-b-a-token
    (let* ((policy (list :kind :latest))
           (a (pg::physical-follow-next-desired nil "host=a" "" "rx" policy t))
           (b (pg::physical-follow-next-desired a "host=b" "" "rx" policy t))
           (again (pg::physical-follow-next-desired b "host=a" "" "rx" policy t)))
      (ok (not (eq a b)))
      (ok (not (eq a again)))
      (ok (equalp (pg::physical-follow-desired-conninfo a)
                  (pg::physical-follow-desired-conninfo again)))))

(deftest follow-desired-generation-compares-the-full-value-before-receiver-clipping
    (let* ((bytes (make-array 1030 :element-type '(unsigned-byte 8) :initial-element 65))
           (a (pg::physical-follow-next-desired nil bytes "" "rx" nil t)))
      (setf (aref bytes 1023) 255)
      (let ((b (pg::physical-follow-next-desired a bytes "" "rx" nil t)))
        (ok (not (eq a b)))
        (ok (= 1030 (length (pg::physical-follow-desired-conninfo b))))
        (ok (= 1023 (length (pg::physical-follow-desired-receiver-conninfo b))))
        (ok (equalp (pg::physical-follow-desired-receiver-conninfo a)
                    (pg::physical-follow-desired-receiver-conninfo b))))))

(deftest follow-desired-receiver-clips-raw-bytes-through-a-multibyte-sequence
    (let ((bytes (make-array 1025 :element-type '(unsigned-byte 8) :initial-element 65)))
      (setf (aref bytes 1022) 226 (aref bytes 1023) 130 (aref bytes 1024) 172)
      (let* ((desired (pg::physical-follow-next-desired nil bytes "" "rx" nil t))
             (input (pg::physical-follow-desired-receiver-conninfo desired)))
        (ok (= 1023 (length input)))
        (ok (= 226 (aref input 1022)))
        (ok (= 1025 (length (pg::physical-follow-desired-conninfo desired)))))))

(deftest follow-desired-claim-survives-removal-and-empty-conninfo
    (let* ((unmanaged (pg::physical-follow-next-desired nil "" "" "rx" nil nil))
           (slot-only (pg::physical-follow-next-desired unmanaged "" "public_slot" "rx" nil nil))
           (claimed (pg::physical-follow-next-desired slot-only "host=source" "" "rx" nil t))
           (cleared (pg::physical-follow-next-desired claimed "" "" "rx" nil nil)))
      (ok (not (pg::physical-follow-desired-claimed-p unmanaged)))
      (ok (not (pg::physical-follow-desired-claimed-p slot-only)))
      (ok (pg::physical-follow-desired-claimed-p cleared))
      (ok (zerop (length (pg::physical-follow-desired-conninfo cleared))))
      (ok (null (pg::physical-follow-desired-slot cleared)))))

(deftest follow-desired-validates-slot-before-publication-without-parsing-conninfo
    (let* ((pg::*physical-follow-desired* nil)
           (desired (pg::physical-follow-next-desired nil "this is invalid libpq syntax" "" "rx" nil t)))
      (pg::physical-follow-install-desired desired)
      (dolist (slot (list "UPPER" "has-dash" "pg_conflict_detection" (make-string 64 :initial-element #\a)))
        (ok (intake-fails-p
             (lambda () (pg::physical-follow-next-desired desired "password=private" slot "rx" nil t)))))
      (ok (eq desired pg::*physical-follow-desired*))))

(deftest follow-desired-publication-revokes-old-and-unowned-source-authority
    (let* ((pg::*physical-follow-desired* nil)
           (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
           (b (pg::physical-follow-next-desired a "" "" "rx" nil t)))
      (ok (pg::physical-follow-generation-current-p nil))
      (pg::physical-follow-install-desired a)
      (ok (pg::physical-follow-generation-current-p a))
      (ok (not (pg::physical-follow-generation-current-p nil)))
      (pg::physical-follow-install-desired b)
      (ok (not (pg::physical-follow-generation-current-p a)))
      (ok (pg::physical-follow-generation-current-p b))
      (ok (intake-fails-p (lambda () (pg::physical-follow-require-generation a))))))

(deftest follow-desired-effective-policy-fallback-slot-and-claim-change-generation
    (let* ((policy (list :kind :latest))
           (base (pg::physical-follow-next-desired nil "host=a" "" "rx" policy nil)))
      (dolist (changed
               (list (pg::physical-follow-next-desired base "host=a" "public_slot" "rx" policy nil)
                     (pg::physical-follow-next-desired base "host=a" "" "other" policy nil)
                     (pg::physical-follow-next-desired base "host=a" "" "rx" (copy-list policy) nil)
                     (pg::physical-follow-next-desired base "host=a" "" "rx" policy t)))
        (ok (not (eq base changed))))))

(deftest follow-desired-stale-receiver-cannot-publish-telemetry-or-wal
    (let* ((pg::*physical-follow-desired* nil)
           (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
           (b (pg::physical-follow-next-desired a "" "" "rx" nil t))
           (runtime (apply #'pg::make-physical-follow-runtime (list :generation a)))
           (pg::*physical-follow-runtime* runtime)
           (pg::*physical-follow-statistics* (list :runtime runtime :latest-end-lsn 7))
           (pg::*physical-follow-last-end-lsn* 7)
           (pg::*physical-recovery-failed-p* nil)
           (receive (symbol-function 'pg::physical-recovery-receive))
           (flush (symbol-function 'pg::physical-recovery-flush))
           (received 0) (flushed 0))
      (pg::physical-follow-install-desired a)
      (pg::physical-follow-install-desired b)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-recovery-receive)
                   (lambda (&rest args) (declare (ignore args)) (incf received))
                   (symbol-function 'pg::physical-recovery-flush)
                   (lambda () (incf flushed)))
             (dolist (kind '(107 119))
               (let ((frame (make-array (if (= kind 107) 18 26)
                                        :element-type '(unsigned-byte 8) :initial-element 0)))
                 (setf (aref frame 0) kind)
                 (ok (handler-case
                         (progn (pg::with-live-operation (pg::physical-follow-process-frame frame runtime)) nil)
                       (pg::physical-follow-generation-changed () t)))))
             (dolist (action (list (lambda () (pg::physical-follow-monitor-start runtime 0))
                                   (lambda () (pg::physical-follow-monitor-connected runtime))
                                   (lambda () (pg::physical-follow-monitor-message runtime 100 100))
                                   (lambda () (pg::physical-follow-register-runtime-client runtime nil))
                                   (lambda () (pg::physical-follow-authorize-receiver runtime))))
               (ok (handler-case (progn (funcall action) nil)
                     (pg::physical-follow-generation-changed () t))))
             (ok (zerop received))
             (ok (zerop flushed))
             (ok (= 7 pg::*physical-follow-last-end-lsn* (getf pg::*physical-follow-statistics* :latest-end-lsn)))
             (ok (not (pg::physical-follow-runtime-connected-p runtime)))
             (ok (not pg::*physical-recovery-failed-p*)))
        (setf (symbol-function 'pg::physical-recovery-receive) receive
              (symbol-function 'pg::physical-recovery-flush) flush))))

(deftest follow-desired-stale-operation-and-handoff-are-cancelled
    (let* ((pg::*physical-follow-desired* nil)
           (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
           (b (pg::physical-follow-next-desired a "host=b" "" "rx" nil t))
           (runtime (apply #'pg::make-physical-follow-runtime (list :generation a)))
           (operation (pg::make-physical-follow-operation :context (list :generation a)))
           (pg::*physical-follow-operation* operation)
           (pg::*physical-follow-controller-blocked-p* nil)
           (pg::*physical-follow-controller-stopping-p* nil)
           (pg::*physical-recovery-failed-p* nil))
      (pg::physical-follow-install-desired a)
      (pg::physical-follow-install-desired b)
      (dolist (action (list (lambda () (pg::physical-follow-operation-check operation))
                            (lambda () (pg::physical-follow-operation-register-client operation nil))
                            (lambda () (pg::physical-follow-start-operation "localhost" 5432 "follow" runtime))))
        (ok (handler-case (progn (funcall action) nil)
              (pg::physical-follow-generation-changed () t))))
      (ok (pg::physical-follow-operation-cancelled-condition-p
           operation (make-condition 'pg::physical-follow-generation-changed)))
      (ok (null (pg::physical-follow-operation-client operation)))
      (ok (not pg::*physical-recovery-failed-p*))))

(deftest follow-desired-policy-snapshot-cannot-change-through-an-alias
    (let* ((history (list (pg::make-timeline-history-entry :timeline 2 :begin 64 :end nil)
                          (pg::make-timeline-history-entry :timeline 1 :begin 0 :end 64)))
           (policy (list :goal :current :target 2 :history history :startup-seed (list 64 2)))
           (desired (pg::physical-follow-next-desired nil "host=a" "" "rx" policy t))
           (copy (pg::physical-follow-desired-policy desired)))
      (ok (eq desired (pg::physical-follow-next-desired desired "host=a" "" "rx" policy t)))
      (setf (getf policy :target) 3
            (pg::timeline-history-entry-begin (first history)) 100
            (getf copy :target) 4
            (pg::timeline-history-entry-begin (first (getf copy :history))) 200)
      (let ((saved (pg::physical-follow-desired-policy desired)))
        (ok (= 2 (getf saved :target)))
        (ok (= 64 (pg::timeline-history-entry-begin (first (getf saved :history)))))
        (ok (not (eq desired (pg::physical-follow-next-desired desired "host=a" "" "rx" policy t)))))))

(deftest follow-desired-publication-drains-a-frame-before-revoking-its-generation
    (let* ((saved-desired pg::*physical-follow-desired*)
           (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
           (b (pg::physical-follow-next-desired a "" "" "rx" nil t))
           (runtime (pg::make-physical-follow-runtime :generation a))
           (frame (make-array 18 :element-type '(unsigned-byte 8) :initial-element 0))
           (entered (sb-thread:make-semaphore))
           (release (sb-thread:make-semaphore))
           (flush (symbol-function 'pg::physical-recovery-flush))
           (flushed 0) (receiver nil) (publisher nil) (failure nil))
      (setf pg::*physical-follow-desired* nil
            (aref frame 0) 107)
      (pg::physical-follow-install-desired a)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-recovery-flush) (lambda () (incf flushed)))
             (setf receiver
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case
                          (pg::with-live-operation
                            (sb-thread:signal-semaphore entered)
                            (sb-thread:wait-on-semaphore release :timeout 5)
                            (pg::physical-follow-process-frame frame runtime))
                        (error (condition) (setf failure condition))))))
             (ok (sb-thread:wait-on-semaphore entered :timeout 5))
             (setf publisher
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case (pg::physical-follow-install-desired b)
                        (error (condition) (setf failure condition))))))
             (loop repeat 5000 while pg::*live-operation-admission-open* do (sleep 0.001))
             (ok (not pg::*live-operation-admission-open*))
             (ok (eq a pg::*physical-follow-desired*))
             (sb-thread:signal-semaphore release)
             (sb-thread:join-thread receiver :timeout 5 :default :timeout)
             (sb-thread:join-thread publisher :timeout 5 :default :timeout)
             (ok (null failure))
             (ok (= 1 flushed))
             (ok (eq b pg::*physical-follow-desired*))
             (ok (handler-case
                     (progn (pg::with-live-operation (pg::physical-follow-process-frame frame runtime)) nil)
                   (pg::physical-follow-generation-changed () t)))
             (ok (= 1 flushed)))
        (sb-thread:signal-semaphore release)
        (dolist (thread (list receiver publisher))
          (when thread (sb-thread:join-thread thread :timeout 5 :default :timeout)))
        (setf pg::*physical-follow-desired* saved-desired
              (symbol-function 'pg::physical-recovery-flush) flush))))

(deftest follow-managed-marker-is-bound-to-the-verified-base
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (pg::*physical-recovery-failed-p* nil)
                                (base (list :system-id 42 :manifest-digest 77)))
        (ok (not (pg::physical-follow-managed-marker-p base)))
        (pg::physical-follow-persist-management base)
        (ok (pg::physical-follow-managed-marker-p base))
        (ok (intake-fails-p
             (lambda () (pg::physical-follow-managed-marker-p '(:system-id 43 :manifest-digest 77)))))
        (ok (intake-fails-p
             (lambda () (pg::physical-follow-managed-marker-p '(:system-id 42 :manifest-digest 78)))))
        (pg::wal-atomic-write-bytes (pg::physical-follow-managed-path) (pg::string->bytes "invalid"))
        (ok (intake-fails-p (lambda () (pg::physical-follow-managed-marker-p base)))))))

(deftest follow-managed-marker-save-failure-fences-recovery
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (pg::*physical-recovery-failed-p* nil)
                                (write (symbol-function 'pg::wal-atomic-write-bytes)))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::wal-atomic-write-bytes)
                     (lambda (&rest args) (declare (ignore args)) (error "marker save cut")))
               (ok (intake-fails-p
                    (lambda () (pg::physical-follow-persist-management '(:system-id 42 :manifest-digest 77)))))
               (ok pg::*physical-recovery-failed-p*))
          (setf (symbol-function 'pg::wal-atomic-write-bytes) write)))))

(deftest follow-desired-install-prepares-durability-before-settings-or-generation
    (let* ((pg::*physical-follow-desired* nil)
           (desired (pg::physical-follow-next-desired nil "" "" "rx" nil t))
           (prepare (symbol-function 'pg::physical-follow-prepare-managed-claim))
           (installed nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-prepare-managed-claim)
                   (lambda (snapshot)
                     (ok (eq desired snapshot))
                     (ok (null pg::*physical-follow-desired*))
                     (error "first claim save cut")))
             (ok (intake-fails-p
                  (lambda ()
                    (pg::physical-follow-call-with-desired-install desired (lambda () (setf installed t))))))
             (ok (null installed))
             (ok (null pg::*physical-follow-desired*)))
        (setf (symbol-function 'pg::physical-follow-prepare-managed-claim) prepare))))

(deftest follow-desired-stale-candidate-cannot-undo-a-claimed-publication
    (let* ((pg::*physical-follow-desired* nil)
           (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
           (unclaimed (pg::physical-follow-next-desired nil "" "" "rx" nil nil))
           (b (pg::physical-follow-next-desired a "host=b" "" "rx" nil t))
           (stale (pg::physical-follow-next-desired a "host=stale" "" "rx" nil t))
           (changed nil))
      (pg::physical-follow-install-desired a)
      (ok (handler-case (progn (pg::physical-follow-install-desired unclaimed) nil)
            (pg::physical-follow-generation-changed () t)))
      (ok (eq a pg::*physical-follow-desired*))
      (pg::physical-follow-install-desired b)
      (ok (handler-case
              (progn (pg::physical-follow-call-with-desired-install stale (lambda () (setf changed t))) nil)
            (pg::physical-follow-generation-changed () t)))
      (ok (not changed))
      (ok (eq b pg::*physical-follow-desired*))
      (ok (not (pg::physical-follow-generation-current-p a)))
      (ok (not (pg::physical-follow-generation-current-p nil)))))

(deftest follow-desired-stale-handoff-does-not-starve-private-cleanup
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired* nil)
              (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
              (b (pg::physical-follow-next-desired a "host=b" "" "rx" nil t))
              (runtime (pg::make-physical-follow-runtime
                        :config config :generation a :receiver-stopped-p t
                        :transition '(:waiting :source :retry-source t)))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-auto-error* nil)
              (pg::*physical-follow-auto-retry-after* 0)
              (names '(pg::physical-follow-start-operation pg::physical-follow-controller-next-cleanup
                       pg::physical-follow-controller-cleanup))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (started 0) (cleaned 0))
         (pg::physical-follow-install-desired a)
         (pg::physical-follow-install-desired b)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-start-operation)
                      (lambda (&rest args) (declare (ignore args)) (incf started))
                      (symbol-function 'pg::physical-follow-controller-next-cleanup)
                      (lambda (actual) (declare (ignore actual)) :private-debt)
                      (symbol-function 'pg::physical-follow-controller-cleanup)
                      (lambda (entry) (assert (eq :private-debt entry)) (incf cleaned)))
                (pg::physical-follow-controller-step)
                (ok (zerop started))
                (ok (= 1 cleaned)))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))))

(deftest configured-follow-clear-stops-receipt-and-preserves-replay-and-proof
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired* nil)
              (a (pg::physical-follow-next-desired nil "host=old" "" "rx"
                                                   (getf pg::*physical-recovery-state* :timeline-policy) t))
              (cleared (pg::physical-follow-next-desired a "" "" "rx"
                                                         (getf pg::*physical-recovery-state* :timeline-policy) t))
              (runtime (pg::make-physical-follow-runtime :generation a :config config
                                                         :replay-thread sb-thread:*current-thread*))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-desired-applied* nil)
              (pg::*physical-follow-manual-generation* nil)
              (pg::*physical-follow-desired-stopping-p* nil)
              (pg::*physical-replay-paused-p* t)
              (start (symbol-function 'pg::physical-follow-start-operation))
              (started 0))
         (pg::physical-follow-install-desired a)
         (pg::physical-follow-install-desired cleared)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-start-operation)
                      (lambda (&rest args) (declare (ignore args)) (incf started)))
                (ok (eq :applied (pg::physical-follow-reconcile-desired-step)))
                (ok (zerop started))
                (ok (eq runtime pg::*physical-follow-runtime*))
                (ok (eq sb-thread:*current-thread* (pg::physical-follow-runtime-replay-thread runtime)))
                (ok pg::*physical-replay-paused-p*)
                (ok (pg::physical-follow-runtime-receiver-stopped-p runtime))
                (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
                (let ((saved (pg::physical-follow-read-config)))
                  (ok (not (pg::physical-follow-controller-enabled-p saved)))
                  (ok (equal (pg::physical-follow-effective-config config)
                             (pg::physical-follow-effective-config saved)))
                  (ok (equal (getf (sixth config) :cleanup) (getf (sixth saved) :cleanup))))
                (ok (pg::physical-follow-managed-marker-p pg::*physical-recovery-state*)))
           (setf (symbol-function 'pg::physical-follow-start-operation) start))))))

(deftest configured-follow-failed-replacement-retries-new-source-with-old-receipt-disabled
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired* nil)
              (desired (pg::physical-follow-next-desired nil "not valid libpq syntax" "public_slot" "rx"
                                                         (getf pg::*physical-recovery-state* :timeline-policy) t))
              (runtime (pg::make-physical-follow-runtime :config config :replay-thread sb-thread:*current-thread*))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-desired-applied* nil)
              (pg::*physical-follow-manual-generation* nil)
              (pg::*physical-follow-desired-stopping-p* nil)
              (start (symbol-function 'pg::physical-follow-start-operation))
              (attempts 0))
         (pg::physical-follow-install-desired desired)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-start-operation)
                      (lambda (host port prefix &optional old profile slot generation configured-p)
                        (declare (ignore host port prefix old))
                        (incf attempts)
                        (ok (not (pg::physical-follow-controller-enabled-p (pg::physical-follow-read-config))))
                        (ok (eq :physical (pg::physical-libpq-profile-mode profile)))
                        (ok (equalp (pg::string->bytes "not valid libpq syntax")
                                    (pg::physical-libpq-profile-conninfo profile)))
                        (ok (string= "public_slot" slot))
                        (ok (eq desired generation))
                        (ok configured-p)
                        (pg::make-physical-follow-operation
                         :thread (sb-thread:make-thread (lambda () nil))
                         :error (make-condition 'pg::pg-client-error :sqlstate "08001" :message "fixture unavailable"))))
                (dotimes (attempt 2)
                  (declare (ignorable attempt))
                  (ok (eq :retry (pg::physical-follow-reconcile-desired-step))))
                (ok (= 2 attempts))
                (ok (not (pg::physical-follow-controller-enabled-p (pg::physical-follow-read-config))))
                (ok (eq runtime pg::*physical-follow-runtime*))
                (ok (not (pg::physical-follow-runtime-stopped-p runtime))))
           (setf (symbol-function 'pg::physical-follow-start-operation) start))))))

(deftest configured-follow-slot-only-default-does-not-claim-a-manual-route
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired* nil)
              (desired (pg::physical-follow-next-desired nil "" "public_slot" "rx" nil nil))
              (pg::*physical-follow-desired-applied* nil)
              (pg::*physical-follow-manual-generation* nil)
              (pg::*physical-follow-desired-stopping-p* nil))
         (pg::physical-follow-install-desired desired)
         (ok (eq :unmanaged (pg::physical-follow-reconcile-desired-step)))
         (ok (equal config (pg::physical-follow-read-config)))
         (ok (not (probe-file (pg::physical-follow-managed-path))))))))

(deftest configured-follow-cold-marker-claims-only-after-native-base-verification
    (dolist (wrong '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (pg::physical-follow-persist-management pg::*physical-recovery-state*)
         (when wrong
           (pg::wal-atomic-write-bytes
            (pg::physical-follow-managed-path)
            (pg::physical-follow-managed-marker-bytes
             (list :system-id 43 :manifest-digest (getf pg::*physical-recovery-state* :manifest-digest)))))
         (pg::physical-store-close store)
         (sb-posix:rename (namestring (pg::physical-store-directory store))
                          (namestring (pg::physical-recovery-spool-directory)))
         (call-with-current-promotion-owner
          (lambda ()
            (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                                        (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                                        (pg::*physical-restartpoint-worker* nil))
              (if wrong
                  (progn
                    (ok (intake-fails-p #'pg::physical-bootstrap-check-startup))
                    (ok (null pg::*physical-recovery-state*)))
                  (progn
                    (pg::physical-bootstrap-check-startup)
                    (ok (getf pg::*physical-recovery-state* :standard-follow-managed-p))
                    (ok (null pg::*physical-follow-runtime*))))))))
       :system-id 42)))

(deftest configured-follow-settings-are-atomic-opaque-and-stable-across-reload
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*physical-recovery-state* nil) (pg::*physical-follow-desired* nil)
                                 (pg::*startup-installed-configuration* nil)
                                 (pg::*startup-server-settings* nil) (pg::*startup-native-inputs* nil)
                                 (pg::*startup-command-overrides* nil) (pg::*enable-hot-standby* t)
                                 (pg::*wal-keep-size-mb* 0) (pg::*physical-restartpoint-timeout* 300)
                                 (pg::*physical-restartpoint-wal-bytes* 1073741824)
                                 (pg::*max-slot-wal-keep-bytes* nil) (pg::*wal-slot-retention-policy* nil)
                                 (pg::*max-replication-slots* 10) (pg::*max-connections* 100) (pg::*max-wal-senders* 10)
                                 (path (merge-pathnames "postgresql.conf" root))
                                 (text (format nil "primary_conninfo='host=source password=\\377\\200'~%primary_slot_name='public_slot'~%")))
        (write-startup-config path text)
        (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
          (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
        (let* ((installed pg::*physical-follow-desired*)
               (bytes (pg::physical-follow-desired-conninfo installed)))
          (ok (= 255 (aref bytes (- (length bytes) 2))))
          (ok (= 128 (aref bytes (1- (length bytes)))))
          (ok (string= "public_slot" (pg::physical-follow-desired-slot installed)))
          (ok (equalp bytes (pg::startup-setting-show-value "primary_conninfo")))
          (let ((row (pg::startup-settings-row "primary_conninfo"
                                               (gethash "primary_conninfo" pg::*startup-server-settings*))))
            (ok (equalp bytes (aref row 1)))
            (ok (string= "sighup" (aref row 3)))
            (ok (string= "string" (aref row 4))))
          (write-startup-config path (concatenate 'string (format nil "# provenance changed~%") text))
          (pg::startup-reload-settings)
          (ok (eq installed pg::*physical-follow-desired*))
          (let ((settings pg::*startup-server-settings*))
            (write-startup-config path (format nil "primary_conninfo='host=changed'~%primary_slot_name='pg_conflict_detection'~%"))
            (ok (intake-fails-p #'pg::startup-reload-settings))
            (ok (eq settings pg::*startup-server-settings*))
            (ok (eq installed pg::*physical-follow-desired*)))
          (write-startup-config path (format nil "primary_conninfo='not valid libpq syntax'~%"))
          (pg::startup-reload-settings)
          (ok (not (eq installed pg::*physical-follow-desired*)))
          (write-startup-config path (format nil "primary_conninfo=''~%"))
          (pg::startup-reload-settings)
          (let ((cleared pg::*physical-follow-desired*))
            (ok (pg::physical-follow-desired-claimed-p cleared))
            (ok (zerop (length (pg::physical-follow-desired-conninfo cleared))))
            (write-startup-config path "")
            (pg::startup-reload-settings)
            (ok (eq cleared pg::*physical-follow-desired*)))))))

(deftest configured-follow-raw-text-datarow-preserves-octets-in-both-formats
    (with-physical-store-directory (root)
      (dolist (format '(0 1))
        (let* ((path (merge-pathnames "row.wire" root))
               (bytes (make-array 4 :element-type '(unsigned-byte 8) :initial-contents '(65 255 128 66)))
               (pg::*select-result-formats* (vector format)))
          (ok (equalp bytes (pg::value->text bytes :text)))
          (with-open-file (stream path :direction :output :if-exists :supersede :element-type '(unsigned-byte 8))
            (pg::send-select-data-row stream (vector bytes) (list (list :name "setting" :type :text))))
          (ok (equalp '(68 0 0 0 14 0 1 0 0 0 4 65 255 128 66)
                      (coerce (pg::recovery-read-bytes path :maximum 64) 'list)))))))

(deftest configured-follow-raw-rendering-keeps-valid-utf8-as-ordinary-text
    (dolist (text (list "" "host=localhost" (string (code-char #x20ac))))
      (let ((bytes (pg::string->bytes text)))
        (ok (string= text (pg::startup-render-conninfo bytes)))
        (ok (equalp bytes (pg::string->bytes (pg::startup-render-conninfo bytes)))))))

(deftest configured-follow-manual-command-reserves-before-stopping-worker
    (let ((pg::*physical-follow-desired* nil)
          (pg::*physical-follow-desired-worker* :fixture-worker)
          (pg::*physical-follow-manual-reservation* nil)
          (pg::*live-operation-depth* 0)
          (stop (symbol-function 'pg::physical-follow-desired-stop-worker))
          (start (symbol-function 'pg::physical-follow-desired-start-worker))
          (restart (symbol-function 'pg::physical-follow-restart-desired-worker-p))
          (nested-rejected nil) (calls 0) (restarts 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-desired-stop-worker)
                   (lambda ()
                     (setf pg::*physical-follow-desired-worker* nil)
                     ;; This is the point at which another command can enter.
                     (setf nested-rejected
                           (handler-case
                               (progn
                                 (pg::physical-follow-call-with-manual-source
                                  (lambda (generation)
                                    (declare (ignore generation))
                                    (incf calls) :started))
                                 nil)
                             (error () t)))
                     t)
                   (symbol-function 'pg::physical-follow-desired-start-worker)
                   (lambda (&key (reset-applied-p t))
                     (declare (ignore reset-applied-p))
                     (incf restarts)
                     (setf pg::*physical-follow-desired-worker* :fixture-worker))
                   (symbol-function 'pg::physical-follow-restart-desired-worker-p)
                   (constantly t))
             (ok (eq :started
                     (pg::physical-follow-call-with-manual-source
                      (lambda (generation)
                        (declare (ignore generation))
                        (incf calls) :started))))
             (ok nested-rejected)
             (ok (= calls 1))
             (ok (= restarts 1))
             (ok (null pg::*physical-follow-manual-reservation*)))
        (setf (symbol-function 'pg::physical-follow-desired-stop-worker) stop
              (symbol-function 'pg::physical-follow-desired-start-worker) start
              (symbol-function 'pg::physical-follow-restart-desired-worker-p) restart))))

(deftest follow-manual-reservation-defers-handoff-but-preserves-cleanup
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired* nil)
              (a (pg::physical-follow-next-desired nil "host=a" "" "rx" nil t))
              (runtime (pg::make-physical-follow-runtime
                        :config config :generation a :receiver-stopped-p t
                        :transition '(:waiting :source :retry-source t)))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-manual-reservation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-auto-error* nil)
              (pg::*physical-follow-auto-retry-after* 0)
              (names '(pg::physical-follow-start-operation pg::physical-follow-controller-next-cleanup
                       pg::physical-follow-controller-cleanup))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (started 0) (cleaned 0))
         (pg::physical-follow-install-desired a)
         (setf pg::*physical-follow-manual-reservation* (list :fixture))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-start-operation)
                      (lambda (&rest args) (declare (ignore args)) (incf started))
                      (symbol-function 'pg::physical-follow-controller-next-cleanup)
                      (lambda (actual) (declare (ignore actual)) :private-debt)
                      (symbol-function 'pg::physical-follow-controller-cleanup)
                      (lambda (entry) (assert (eq :private-debt entry)) (incf cleaned)))
                (pg::physical-follow-controller-step)
                (ok (zerop started))
                (ok (= 1 cleaned)))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))))

(deftest configured-follow-stop-after-baseline-prevents-operation-admission
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired* nil)
              (desired (pg::physical-follow-next-desired
                        nil "host=new" "" "rx"
                        (getf pg::*physical-recovery-state* :timeline-policy) t))
              (pg::*physical-follow-runtime*
                (pg::make-physical-follow-runtime :config config
                                                  :replay-thread sb-thread:*current-thread*))
              (pg::*physical-follow-desired-worker* nil)
              (pg::*physical-follow-desired-stopping-p* nil)
              (pg::*physical-follow-desired-applied* nil)
              (pg::*physical-follow-manual-generation* nil)
              (pg::*physical-follow-manual-reservation* nil)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (disable (symbol-function 'pg::physical-follow-disable-desired-receipt))
              (run (symbol-function 'pg::physical-follow-operation-run))
              (entered 0))
         (pg::physical-follow-install-desired desired)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-disable-desired-receipt)
                      (lambda (generation)
                        (prog1 (funcall disable generation)
                          (pg::physical-follow-desired-stop-worker)))
                      (symbol-function 'pg::physical-follow-operation-run)
                      (lambda (operation) (declare (ignore operation)) (incf entered)))
                (ok (eq :superseded (pg::physical-follow-reconcile-desired-step)))
                (ok (zerop entered))
                (ok (null pg::*physical-follow-operation*))
                (ok (not (pg::physical-follow-controller-enabled-p
                          (pg::physical-follow-read-config)))))
           (when pg::*physical-follow-operation*
             (sb-thread:join-thread
              (pg::physical-follow-operation-thread pg::*physical-follow-operation*)))
           (setf (symbol-function 'pg::physical-follow-disable-desired-receipt) disable
                 (symbol-function 'pg::physical-follow-operation-run) run))))))

(deftest follow-manual-reservation-is-rechecked-before-policy-fence
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((runtime (pg::make-physical-follow-runtime :config config))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-manual-reservation* (list :fixture))
              (wait (symbol-function 'pg::physical-follow-wait-for-policy))
              (called 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-wait-for-policy)
                      (lambda (&rest args) (declare (ignore args)) (incf called)))
                (pg::physical-follow-controller-fence-receipt runtime config '(:reason :fixture))
                (ok (zerop called)))
           (setf (symbol-function 'pg::physical-follow-wait-for-policy) wait))))))

(deftest configured-follow-stop-before-final-publication-starts-no-receiver
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((pg::*physical-follow-desired*
                (pg::physical-follow-next-desired nil "host=new" "" "rx" nil t))
              (pg::*physical-follow-desired-worker* nil)
              (pg::*physical-follow-desired-stopping-p* nil)
              (pg::*physical-follow-manual-generation* nil)
              (pg::*physical-follow-manual-reservation* nil)
              (pg::*physical-follow-operation* nil)
              (launch (symbol-function 'pg::physical-follow-launch-workers))
              (decision (symbol-function 'pg::physical-follow-launch-decision)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-launch-decision)
                      (lambda (saved) (declare (ignore saved)) '(:action :receive))
                      (symbol-function 'pg::physical-follow-launch-workers)
                      (lambda (&rest args)
                        ;; Cut after validation and before worker publication.
                        (pg::physical-follow-desired-stop-worker)
                        (apply launch args)))
                (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                  (ok (handler-case
                          (progn (pg::physical-follow-launch
                                  config pg::*physical-follow-desired* t) nil)
                        (pg::physical-follow-generation-changed () t))))
                (ok (null pg::*physical-follow-runtime*))
                (ok (not pg::*physical-recovery-failed-p*)))
           (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
             (pg::physical-follow-quiesce-runtime))
           (setf (symbol-function 'pg::physical-follow-launch-workers) launch
                 (symbol-function 'pg::physical-follow-launch-decision) decision))))))

(deftest follow-operation-recovery-cancellation-does-not-poison-recovery
    (dolist (cancelled '(nil :generation :shutdown))
      (let* ((operation (pg::make-physical-follow-operation))
             (pg::*physical-follow-operation* operation)
             (pg::*physical-recovery-failed-p* nil)
             (pg::*physical-follow-auto-error* nil)
             (pg::*physical-follow-auto-retry-after* 0)
             (pg::*physical-follow-controller-errors* (make-hash-table :test #'equal))
             (initial (symbol-function 'pg::physical-follow-operation-initial-config))
             (recover (symbol-function 'pg::physical-follow-operation-recover)))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-follow-operation-initial-config)
                     (lambda (op) (declare (ignore op)) (error "fixture transition error"))
                     (symbol-function 'pg::physical-follow-operation-recover)
                     (lambda (op)
                       (declare (ignore op))
                       (case cancelled
                         (:generation (error 'pg::physical-follow-generation-changed))
                         (:shutdown (error 'pg::physical-recovery-worker-shutdown))
                         (otherwise (error "fixture storage recovery error")))))
               (pg::physical-follow-operation-run operation)
               (ok (eql (not cancelled) pg::*physical-recovery-failed-p*)))
          (setf (symbol-function 'pg::physical-follow-operation-initial-config) initial
                (symbol-function 'pg::physical-follow-operation-recover) recover)))))
