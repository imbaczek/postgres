(in-package #:pg)

(rove:deftest synchronous-standby-names-grammar
    (rove:ok (null (parse-sync-standby-names "")))
  (dolist (case '(("one,two" :first 1 ("one" "two"))
                  ("2 (one,two)" :first 2 ("one" "two"))
                  ("fIrSt 3 (one)" :first 3 ("one"))
                  ("AnY 2 (one,two,three)" :any 2 ("one" "two" "three"))
                  ("123" :first 1 ("123"))
                  ("\"FIRST\",\"ANY\",\"*\",\"\"" :first 1 ("FIRST" "ANY" "*" ""))
                  ("FIRST 1 (\"a\"\"b\",\"x,y\",ą)" :first 1 ("a\"b" "x,y" "ą"))))
    (destructuring-bind (text method count members) case
      (let ((config (parse-sync-standby-names text)))
        (rove:ok (eq method (getf config :method)))
        (rove:ok (= count (getf config :count)))
        (rove:ok (equal members (getf config :members)))))))

(rove:deftest synchronous-standby-names-reject-invalid-configuration
    (dolist (text '(" " "ANY" "FIRST" "FIRST (one)" "ANY 1 ()"
                    "1 ()" "one," ",one" "one two" "one)" "1(one) trailing"
                    "\"unterminated" "one-two" "FIRST 1 (ANY)" "FIRST 1 (one,)"))
      (rove:ok
       (equal "42601" (handler-case (progn (parse-sync-standby-names text) nil)
                        (pg-error (condition) (pg-error-sqlstate condition))))
       (format nil "reject invalid syntax ~s" text)))
  (rove:ok
   (equal "22023" (handler-case (progn (parse-sync-standby-names "0(one)") nil)
                    (pg-error (condition) (pg-error-sqlstate condition))))))

(cl-spec:defgenerator sync-valid-config-arguments ()
  (list (format nil "~a ~d (peer~d)"
                (if (zerop (random 2)) "FIRST" "ANY")
                (1+ (random 16)) (random 1000000))))

(cl-spec:defspec-function parse-sync-standby-names
    (:args (text string))
  (:args-generator sync-valid-config-arguments)
  (:returns list)
  (:post
   (and (member (getf result :method) '(:first :any))
        (<= 1 (getf result :count) 16)
        (= 1 (length (getf result :members))))))

(cl-spec:defproperty synchronous-standby-quoted-name-roundtrip
    ((count (range integer 1 16))
     (suffix (range integer 0 1000000)))
  (:about parse-sync-standby-names)
  (:trials (:normal 100))
  (let* ((name (format nil "peer~d, λ" suffix))
         (config (parse-sync-standby-names
                  (format nil "ANY ~d (\"ANY\",\"~a\")" count name))))
    (and (eq :any (getf config :method))
         (= count (getf config :count))
         (equal (list "ANY" name) (getf config :members)))))

(rove:deftest generated-synchronous-standby-name-contracts
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'parse-sync-standby-names :trials 100 :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data)))
  (unit-check-property 'synchronous-standby-quoted-name-roundtrip))

(rove:deftest synchronous-standby-priority-and-independent-frontiers
    (let* ((rows '((:index 2 :pid 12 :application-name "A" :state "streaming"
                    :write-lsn 90 :flush-lsn 70 :replay-lsn 0)
                   (:index 0 :pid 10 :application-name "b" :state "stopping"
                    :write-lsn 80 :flush-lsn 75 :replay-lsn 20)
                   (:index 1 :pid 11 :application-name "c" :state "streaming"
                    :write-lsn 100 :flush-lsn 100 :replay-lsn 100)))
           (config (parse-sync-standby-names "FIRST 2 (a,b,c)")))
      (multiple-value-bind (selected frontiers) (sync-replication-select-standbys config rows)
        (rove:ok (equal '(2 0) (mapcar (lambda (row) (getf row :index)) selected)))
        (rove:ok (equal '(:write 80 :flush 70 :apply 0) frontiers))))
  (let ((rows '((:index 0 :pid 10 :application-name "a" :state "streaming"
                 :sent-lsn 10000 :write-lsn 100 :flush-lsn 50 :replay-lsn 10)
                (:index 1 :pid 11 :application-name "b" :state "streaming"
                 :write-lsn 20 :flush-lsn 90 :replay-lsn 80)
                (:index 2 :pid 12 :application-name "c" :state "streaming"
                 :write-lsn 60 :flush-lsn 70 :replay-lsn 30))))
    (multiple-value-bind (selected frontiers)
        (sync-replication-select-standbys (parse-sync-standby-names "ANY 2 (a,b,c)") rows)
      (rove:ok (= 3 (length selected)))
      (rove:ok (equal '(:write 60 :flush 70 :apply 30) frontiers)))))

(rove:deftest synchronous-standby-selection-requires-live-flush-feedback
    (let* ((config (parse-sync-standby-names "FIRST 2 (*)"))
           (base '(:pid 1 :application-name "peer" :state "streaming"
                   :write-lsn 100 :flush-lsn 90 :replay-lsn 80))
           (rows (loop for (key value) in '((:state "catchup") (:state "startup")
                                            (:state "backup") (:flush-lsn 0)
                                            (:flush-lsn nil) (:cascading-p t) (:pid 0))
                       for index from 1
                       collect (list* :index index key value base))))
      (push (list* :index 0 :replication-mode :database base) rows)
      (multiple-value-bind (selected frontiers) (sync-replication-select-standbys config rows)
        (rove:ok (equal '(0) (mapcar (lambda (row) (getf row :index)) selected)))
        (rove:ok (null frontiers) "fewer than N candidates cannot attest any frontier")))
  (let ((config (parse-sync-standby-names "FIRST 1 (peer,peer,other)")))
    (rove:ok (= 1 (sync-standby-priority config "PEER")))
    (rove:ok (= 3 (sync-standby-priority config "other")))
    (rove:ok (= 1 (sync-standby-priority (parse-sync-standby-names "ANY 1 (peer,other)") "other")))
    (rove:ok (= 2 (sync-standby-priority (parse-sync-standby-names "1 (peer,*,other)") "other")))
    (rove:ok (zerop (sync-standby-priority config "unlisted")))
    (rove:ok (zerop (sync-standby-priority config "peer" t))))
  (rove:ok (zerop (sync-standby-priority (parse-sync-standby-names "Ā") "ā"))))

(rove:deftest synchronous-standby-index-ties-and-unknown-progress
    (let ((rows '((:index 5 :pid 11 :application-name "peer" :state "streaming"
                   :write-lsn 100 :flush-lsn 90 :replay-lsn 80)
                  (:index 0 :pid 99 :application-name "peer" :state "streaming"
                   :write-lsn 60 :flush-lsn 50 :replay-lsn 40)
                  (:index 2 :pid 1 :application-name "peer" :state "streaming"
                   :write-lsn 80 :flush-lsn 70 :replay-lsn 60))))
      (multiple-value-bind (selected frontiers)
          (sync-replication-select-standbys (parse-sync-standby-names "2 (*)") rows)
        (rove:ok (equal '(0 2) (mapcar (lambda (row) (getf row :index)) selected)))
        (rove:ok (equal '(:write 60 :flush 50 :apply 40) frontiers))))
  (let ((rows '((:index 0 :pid 1 :application-name "a" :state "streaming"
                 :write-lsn 50 :flush-lsn 40 :replay-lsn 30)
                (:index 1 :pid 2 :application-name "b" :state "streaming"
                 :write-lsn 90 :flush-lsn 80 :replay-lsn 70))))
    (dolist (text '("FIRST 2 (b,a)" "ANY 1 (b,a)"))
      (multiple-value-bind (selected frontiers)
          (sync-replication-select-standbys (parse-sync-standby-names text) rows)
        (declare (ignore frontiers))
        (rove:ok (equal '(0 1) (mapcar (lambda (row) (getf row :index)) selected))
                 "keep slot order when there is no priority cutoff"))))
  (dolist (method '("FIRST" "ANY"))
    (dolist (unknown '(nil 0))
      (dolist (reverse-p '(nil t))
        (let* ((rows (list (list :index (if reverse-p 1 0) :pid 1 :application-name "a" :state "streaming"
                                 :write-lsn unknown :flush-lsn 100 :replay-lsn unknown)
                           (list :index (if reverse-p 0 1) :pid 2 :application-name "b" :state "streaming"
                                 :write-lsn 100 :flush-lsn 100 :replay-lsn 100)))
               (config (parse-sync-standby-names (format nil "~a 2 (*)" method))))
          (multiple-value-bind (selected frontiers)
              (sync-replication-select-standbys config (if reverse-p (reverse rows) rows))
            (rove:ok (= 2 (length selected)))
            (rove:ok (equal '(:write 0 :flush 100 :apply 0) frontiers)
                     "an unknown selected peer blocks that mode in every order")))))))

(cl-spec:defproperty synchronous-quorum-frontier-is-attested
    ((a (range integer 0 100))
     (b (range integer 0 100))
     (c (range integer 0 100))
     (rank (range integer 1 3)))
  (:about sync-replication-select-standbys)
  (:trials (:normal 100))
  (let* ((positions (list a b c))
         (rows (loop for value in positions for index from 0
                     collect (list :index index :pid (1+ index) :application-name "peer"
                                   :state "streaming" :write-lsn value :flush-lsn 100
                                   :replay-lsn (- 100 value))))
         (config (parse-sync-standby-names (format nil "ANY ~d (*)" rank))))
    (multiple-value-bind (selected frontiers) (sync-replication-select-standbys config rows)
      (declare (ignore selected))
      (let ((write (getf frontiers :write)))
        (and (>= (count-if (lambda (position) (>= position write)) positions) rank)
             (< (count-if (lambda (position) (> position write)) positions) rank)
             (equal frontiers
                    (nth-value 1 (sync-replication-select-standbys config (reverse rows)))))))))

(rove:deftest generated-synchronous-quorum-frontiers
    (unit-check-property 'synchronous-quorum-frontier-is-attested))

(rove:deftest synchronous-selection-follows-current-sender-generation
    (let* ((*wal-sender-registry* (make-hash-table :test #'eq))
           (*physical-recovery-state* nil)
           (config (parse-sync-standby-names "FIRST 1 (*)"))
           (first (make-backend :pid 100 :replication-mode :physical :application-name "peer"))
           (second (make-backend :pid 101 :replication-mode :database :application-name "peer"))
           (replacement (make-backend :pid 200 :replication-mode :database :application-name "peer"))
           (old (wal-sender-register first))
           (other (wal-sender-register second)))
      (wal-sender-set-state old "streaming")
      (wal-sender-note-reply old 40 30 20 1)
      (wal-sender-set-state other "streaming")
      (wal-sender-note-reply other 80 70 60 1)
      (multiple-value-bind (selected frontiers) (sync-replication-current-selection config)
        (rove:ok (equal '(100) (mapcar (lambda (row) (getf row :pid)) selected)))
        (rove:ok (= 30 (getf frontiers :flush))))
      (wal-sender-unregister first old)
      (let ((new (wal-sender-register replacement)))
        (wal-sender-set-state new "streaming")
        (wal-sender-note-reply new 100 90 80 2)
        (rove:ok (null (wal-sender-note-reply old 9999 9999 9999 3)))
        (multiple-value-bind (selected frontiers) (sync-replication-current-selection config)
          (rove:ok (equal '(200) (mapcar (lambda (row) (getf row :pid)) selected)))
          (rove:ok (= 0 (getf (first selected) :index)) "reuse the lowest freed sender slot")
          (rove:ok (= 90 (getf frontiers :flush)) "a logical sender can attest progress")
          (setf (char (getf (first selected) :application-name) 0) #\X)
          (rove:ok (equal "peer" (backend-application-name replacement))))
        (let ((*physical-recovery-state* '(:recovering)))
          (multiple-value-bind (selected frontiers) (sync-replication-current-selection config)
            (rove:ok (null selected))
            (rove:ok (null frontiers) "matching names on a standby cannot attest primary commits")))
        (let ((*backend* replacement))
          (call-with-wal-sender-stream
           (lambda ()
             (wal-sender-set-state new "streaming")
             (multiple-value-bind (selected frontiers)
                 (sync-replication-current-selection (parse-sync-standby-names "ANY 2 (*)"))
               (rove:ok (= 1 (length selected)))
               (rove:ok (null frontiers) "a restarted stream must earn new feedback"))))))))

(defun call-with-sync-wait-state (function)
  (let ((*wal-sender-registry* (make-hash-table :test #'eq))
        (*physical-recovery-state* nil)
        (*sync-replication-state* (make-sync-replication-state :source-id :source-a)))
    (funcall function)))

(defun sync-test-enqueue (lsn mode &optional (source :source-a))
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*wal-sender-lock*)
      (sync-replication-enqueue-under-lock (make-backend) lsn mode source))))

(defun sync-test-sender (name pid write flush apply)
  (let ((sender (wal-sender-register
                 (make-backend :pid pid :replication-mode :database :application-name name))))
    (wal-sender-set-state sender "streaming")
    (wal-sender-note-reply sender write flush apply 1)
    sender))

(rove:deftest synchronous-configuration-disable-is-atomic-with-enqueue
    (call-with-sync-wait-state
     (lambda ()
       (rove:ok (= 1 (sync-replication-install-configuration "FIRST 1 (peer)" :source-a :source-a 0)))
       (let* ((sender (sync-test-sender "peer" 1 30 20 10))
              (pending (sync-test-enqueue 21 :flush))
              (done (sync-test-enqueue 20 :flush)))
         (rove:ok (eq :waiting (sync-replication-waiter-state pending)))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state done)))
         (rove:ok
          (equal "42601"
                 (handler-case
                     (sync-replication-install-configuration "FIRST (" :source-a :source-a 1)
                   (pg-error (condition) (pg-error-sqlstate condition)))))
         (rove:ok
          (equal "55000"
                 (handler-case
                     (sync-replication-install-configuration "" :source-a :source-a 0)
                   (pg-error (condition) (pg-error-sqlstate condition)))))
         (rove:ok (= 1 (sync-replication-state-generation *sync-replication-state*)))
         (rove:ok (equal "FIRST 1 (peer)" (sync-replication-state-names *sync-replication-state*)))
         (rove:ok (eq sender (wal-sender-for-backend (wal-sender-status-backend sender))))
         (rove:ok (eq :waiting (sync-replication-waiter-state pending)))
         (rove:ok (= 2 (sync-replication-install-configuration "" :source-a :source-a 1)))
         (rove:ok (eq :disabled (sync-replication-waiter-state pending)))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state done)))
         (rove:ok (eq :disabled (sync-replication-waiter-state (sync-test-enqueue 99 :flush))))
         (rove:ok (every #'null (list (getf (sync-replication-state-queues *sync-replication-state*) :write)
                                      (getf (sync-replication-state-queues *sync-replication-state*) :flush)
                                      (getf (sync-replication-state-queues *sync-replication-state*) :apply))))))))

(rove:deftest synchronous-feedback-releases-only-the-attested-mode
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (peer)" :source-a :source-a 0)
       (let* ((sender (sync-test-sender "peer" 1 100 80 40))
              (write (sync-test-enqueue 90 :write))
              (flush (sync-test-enqueue 90 :flush))
              (apply (sync-test-enqueue 90 :apply)))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state write)))
         (rove:ok (eq :waiting (sync-replication-waiter-state flush)))
         (rove:ok (eq :waiting (sync-replication-waiter-state apply)))
         (wal-sender-note-sent sender 10000)
         (rove:ok (eq :waiting (sync-replication-waiter-state flush)))
         (wal-sender-note-reply sender 100 100 50 2)
         (rove:ok (eq :acknowledged (sync-replication-waiter-state flush)))
         (rove:ok (eq :waiting (sync-replication-waiter-state apply)))
         (wal-sender-note-reply sender 100 100 90 3)
         (rove:ok (eq :acknowledged (sync-replication-waiter-state apply)))))))

(rove:deftest synchronous-disconnect-uses-replacement-proof-without-downgrade
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (a,b)" :source-a :source-a 0)
       (let* ((a (sync-test-sender "a" 1 30 20 10))
              (b (sync-test-sender "b" 2 100 90 80))
              (pending (sync-test-enqueue 60 :flush)))
         (rove:ok (eq :waiting (sync-replication-waiter-state pending)))
         (wal-sender-unregister (wal-sender-status-backend a) a)
         (rove:ok (eq :acknowledged (sync-replication-waiter-state pending)))
         (let ((blocked (sync-test-enqueue 100 :flush)))
           (wal-sender-unregister (wal-sender-status-backend b) b)
           (rove:ok (eq :waiting (sync-replication-waiter-state blocked))
                    "disconnect cannot turn an unmet wait into async success"))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state (sync-test-enqueue 60 :flush)))
                  "recorded proof remains valid after sender disconnect")))))

(rove:deftest synchronous-source-change-fences-old-proof-and-stale-installers
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (*)" :source-a :source-a 0)
       (let* ((old (sync-test-sender "peer" 1 60 60 60))
              (done (sync-test-enqueue 50 :flush))
              (pending (sync-test-enqueue 80 :flush)))
         (sync-replication-install-configuration "" :source-b :source-a 1)
         (rove:ok (eq :source-changed (sync-replication-waiter-state pending)))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state done)))
         (rove:ok (eq :source-changed
                      (sync-replication-waiter-state (sync-test-enqueue 10 :flush :source-a))))
         (rove:ok (zerop (hash-table-count *wal-sender-registry*)))
         (rove:ok (null (wal-sender-note-reply old 9999 9999 9999 2)))
         (sync-replication-install-configuration "FIRST 1 (*)" :source-b :source-b 2)
         (let ((new (sync-test-sender "peer" 1 20 20 20)))
           (rove:ok
            (equal "55000"
                   (handler-case
                       (wal-sender-register (make-backend :pid 9 :replication-mode :database) :source-a)
                     (pg-error (condition) (pg-error-sqlstate condition)))))
           (rove:ok (eq :waiting
                        (sync-replication-waiter-state (sync-test-enqueue 50 :flush :source-b))))
           (rove:ok
            (equal "55000"
                   (handler-case
                       (sync-replication-install-configuration "" :source-a :source-a 1)
                     (pg-error (condition) (pg-error-sqlstate condition)))))
           (rove:ok (eq new (wal-sender-for-backend (wal-sender-status-backend new))))
           (wal-sender-unregister (wal-sender-status-backend old) old)
           (rove:ok (eq new (wal-sender-for-backend (wal-sender-status-backend new)))))))))

(defun call-with-blocking-sync-wait (action)
  (call-with-sync-wait-state
   (lambda ()
     (sync-replication-install-configuration "FIRST 1 (peer)" :source-a :source-a 0)
     (let* ((*backend* nil)
            (runtime *sync-replication-state*)
            (registry *wal-sender-registry*)
            (backend (make-backend :pid 501 :executing t))
            (finished (sb-thread:make-semaphore))
            (outcome nil) (fatal nil) (failure nil) (pending-at-return nil)
            (thread nil))
       (unwind-protect
            (progn
              (setf thread
                    (sb-thread:make-thread
                     (lambda ()
                       (let ((*sync-replication-state* runtime)
                             (*wal-sender-registry* registry)
                             (*backend* backend))
                         (setf (backend-thread backend) sb-thread:*current-thread*)
                         (handler-case
                             (sb-sys:without-interrupts
                               (setf outcome (sync-replication-wait backend 100 :flush :source-a)
                                     pending-at-return (backend-termination-pending backend)))
                           (pg-error (condition) (setf fatal condition))
                           (error (condition) (setf failure condition)))
                         (sb-thread:signal-semaphore finished)))
                     :name "synchronous commit wait regression"))
              (loop repeat 500
                    when (sb-thread:with-mutex (*wal-sender-lock*)
                           (some (lambda (waiter)
                                   (eq backend (sync-replication-waiter-backend waiter)))
                                 (getf (sync-replication-state-queues runtime) :flush)))
                      do (return)
                    when (plusp (sb-thread:semaphore-count finished))
                      do (error "wait worker exited before enqueue: ~a ~a" failure fatal)
                    do (sleep 0.01)
                    finally (error "wait worker did not enqueue"))
              (funcall action backend finished)
              (unless (sb-thread:wait-on-semaphore finished :timeout 5)
                (error "wait worker did not finish"))
              (sb-thread:join-thread thread)
              (rove:ok (null failure) (format nil "wait worker failure: ~a" failure))
              (rove:ok (every #'null
                              (loop for mode in '(:write :flush :apply)
                                    collect (getf (sync-replication-state-queues runtime) mode))))
              (list :outcome outcome :fatal fatal :pending-at-return pending-at-return
                    :backend backend))
         (multiple-value-bind (source generation) (sync-replication-configuration-state)
           (sync-replication-install-configuration "" source source generation))
         (when thread
           (sb-thread:join-thread thread :timeout 5 :default :timeout)))))))

(rove:deftest synchronous-blocking-wait-requires-proven-flush
    (let ((result
            (call-with-blocking-sync-wait
             (lambda (backend finished)
               (declare (ignore backend))
               (sb-thread:with-mutex (*wal-lock*)
                 (rove:ok (zerop (sb-thread:semaphore-count finished))
                          "the wait leaves WAL available to other actors"))
               (let ((sender (sync-test-sender "peer" 1 150 50 0)))
                 (wal-sender-note-sent sender 10000)
                 (rove:ok (zerop (sb-thread:semaphore-count finished))
                          "sent and written WAL do not prove flush")
                 (wal-sender-note-reply sender 150 100 0 2))))))
      (rove:ok (eq :acknowledged (getf result :outcome)))
      (rove:ok (null (getf result :fatal)))))

(rove:deftest synchronous-blocking-wait-cancellation-and-configuration-exits
    (dolist (reason '(:disabled :cancelled :timeout :terminated :source-changed))
      (let ((result
              (call-with-blocking-sync-wait
               (lambda (backend finished)
                 (declare (ignore finished))
                 (ecase reason
                   (:disabled
                    (sync-replication-install-configuration "" :source-a :source-a 1))
                   (:cancelled (cancel-backend backend))
                   (:timeout (setf (backend-statement-deadline backend) 0))
                   (:terminated (terminate-backend backend))
                   (:source-changed
                    (sync-replication-install-configuration "" :source-b :source-a 1)))))))
        (rove:ok (eq reason (getf result :outcome)) (format nil "wait exits with ~s" reason))
        (if (eq reason :terminated)
            (progn
              (rove:ok (getf result :pending-at-return)
                       "termination remains pending until the caller can clean up")
              (rove:ok (typep (getf result :fatal) 'pg-error))
              (rove:ok (equal "57P01" (pg-error-sqlstate (getf result :fatal)))))
            (rove:ok (null (getf result :fatal))))
        (when (eq reason :timeout)
          (rove:ok (null (backend-statement-deadline (getf result :backend)))))
        (rove:ok (null (backend-cancel-pending (getf result :backend)))))))

(rove:deftest synchronous-wait-nonlocal-exit-unlinks-without-poisoning-wal
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (peer)" :source-a :source-a 0)
       (let ((original (symbol-function 'sync-replication-interruption))
             (*wal-poisoned* nil)
             (backend (make-backend :xid 88)))
         (unwind-protect
              (progn
                (setf (symbol-function 'sync-replication-interruption)
                      (lambda (owner)
                        (declare (ignore owner))
                        (throw 'sync-exit :escaped)))
                (rove:ok
                 (eq :escaped
                     (catch 'sync-exit
                       (sb-sys:without-interrupts
                         (sync-replication-wait backend 100 :flush :source-a)))))
                (rove:ok (null (getf (sync-replication-state-queues *sync-replication-state*) :flush)))
                (rove:ok (not *wal-poisoned*))
                (rove:ok (= 88 (backend-xid backend)) "the caller still owns transaction cleanup"))
           (setf (symbol-function 'sync-replication-interruption) original))))))

(rove:deftest synchronous-wait-interruption-priority-preserves-termination
    (let ((backend (make-backend :termination-pending t :statement-deadline 0 :cancel-pending t)))
      (rove:ok (eq :terminated (sync-replication-interruption backend)))
      (rove:ok (backend-termination-pending backend))
      (rove:ok (backend-cancel-pending backend))
      (rove:ok (eql 0 (backend-statement-deadline backend)))
      (setf (backend-termination-pending backend) nil)
      (rove:ok (eq :timeout (sync-replication-interruption backend)))
      (rove:ok (null (backend-statement-deadline backend)))
      (rove:ok (null (backend-cancel-pending backend)))
      (setf (backend-cancel-pending backend) t)
      (rove:ok (eq :cancelled (sync-replication-interruption backend)))
      (rove:ok (null (backend-cancel-pending backend)))))

(rove:deftest synchronous-queue-orders-lsns-and-preserves-completed-outcomes
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (*)" :source-a :source-a 0)
       (let ((later (sync-test-enqueue 200 :flush))
             (earlier (sync-test-enqueue 100 :flush))
             (middle (sync-test-enqueue 150 :flush)))
         (rove:ok
          (equal '(100 150 200)
                 (mapcar #'sync-replication-waiter-lsn
                         (getf (sync-replication-state-queues *sync-replication-state*) :flush))))
         (sync-test-sender "peer" 1 160 160 160)
         (rove:ok (eq :acknowledged (sync-replication-waiter-state earlier)))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state middle)))
         (rove:ok (eq :waiting (sync-replication-waiter-state later)))
         (sb-thread:with-mutex (*wal-sender-lock*)
           (sync-replication-cancel-under-lock earlier :timeout)
           (sync-replication-cancel-under-lock later :cancelled))
         (rove:ok (eq :acknowledged (sync-replication-waiter-state earlier)))
         (rove:ok (eq :cancelled (sync-replication-waiter-state later)))
         (rove:ok (null (getf (sync-replication-state-queues *sync-replication-state*) :flush)))))))

(cl-spec:defproperty synchronous-queue-release-matches-attested-prefix
    ((a (range integer 1 100))
     (b (range integer 1 100))
     (c (range integer 1 100))
     (frontier (range integer 0 100)))
  (:about sync-replication-release-under-lock)
  (:trials (:normal 100))
  (call-with-sync-wait-state
   (lambda ()
     (sync-replication-install-configuration "FIRST 1 (*)" :source-a :source-a 0)
     (let ((waiters (mapcar (lambda (lsn) (sync-test-enqueue lsn :flush)) (list a b c))))
       (sync-test-sender "peer" 1 frontier frontier frontier)
       (every (lambda (waiter)
                (eq (if (<= (sync-replication-waiter-lsn waiter) frontier)
                        :acknowledged :waiting)
                    (sync-replication-waiter-state waiter)))
              waiters)))))

(rove:deftest generated-synchronous-queue-prefix
    (unit-check-property 'synchronous-queue-release-matches-attested-prefix))

(rove:deftest synchronous-source-invalidation-terminates-captured-owners-after-unlock
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (*)" :source-a :source-a 0)
       (let* ((old-backend (make-backend :pid 1 :replication-mode :database))
              (new-backend (make-backend :pid 1 :replication-mode :database))
              (old (wal-sender-register old-backend))
              (original (symbol-function 'terminate-backend))
              (terminated nil)
              (replacement nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'terminate-backend)
                      (lambda (backend)
                        (rove:ok (not (sb-thread:holding-mutex-p *wal-sender-lock*)))
                        (rove:ok (eq backend old-backend))
                        (push backend terminated)
                        (setf replacement (wal-sender-register new-backend :source-b))))
                (sync-replication-install-configuration "FIRST 1 (*)" :source-b :source-a 1)
                (rove:ok (equal (list old-backend) terminated))
                (rove:ok (eq replacement (wal-sender-for-backend new-backend)))
                (rove:ok (null (wal-sender-note-reply old 9999 9999 9999 1))))
           (setf (symbol-function 'terminate-backend) original))))))

(rove:deftest synchronous-source-identities-must-be-immutable
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (*)" :source-a :source-a 0)
       (let ((pending (sync-test-enqueue 100 :flush)))
         (dolist (source (list (copy-seq "mutable") (list :mutable)))
           (dolist (function
                    (list (lambda () (sync-replication-install-configuration "" source :source-a 1))
                          (lambda () (sync-replication-install-configuration "" :source-a source 1))
                          (lambda () (sync-test-enqueue 100 :flush source))
                          (lambda () (wal-sender-register
                                      (make-backend :pid 1 :replication-mode :database) source))))
             (rove:ok (equal "22023"
                             (handler-case (progn (funcall function) nil)
                               (pg-error (condition) (pg-error-sqlstate condition)))))))
         (multiple-value-bind (source generation names) (sync-replication-configuration-state)
           (rove:ok (eq source :source-a))
           (rove:ok (= generation 1))
           (setf (char names 0) #\X)
           (rove:ok (equal "FIRST 1 (*)" (sync-replication-state-names *sync-replication-state*))))
         (rove:ok (eq :waiting (sync-replication-waiter-state pending)))
         (rove:ok (zerop (hash-table-count *wal-sender-registry*)))))))

(rove:deftest synchronous-source-invalidation-signals-self-last
    (call-with-sync-wait-state
     (lambda ()
       (sync-replication-install-configuration "FIRST 1 (*)" :source-a :source-a 0)
       (let* ((self (make-backend :pid 1 :replication-mode :database))
              (other (make-backend :pid 2 :replication-mode :database))
              (original (symbol-function 'terminate-backend))
              (targets nil))
         (wal-sender-register self)
         (wal-sender-register other)
         (unwind-protect
              (progn
                (setf (symbol-function 'terminate-backend)
                      (lambda (backend)
                        (push backend targets)
                        (when (eq backend self)
                          (ereport-fatal "57P01" "self termination"))))
                (let* ((*backend* self)
                       (condition
                         (handler-case
                             (sync-replication-install-configuration "" :source-b :source-a 1)
                           (pg-error (condition) condition))))
                  (rove:ok (typep condition 'pg-error))
                  (rove:ok (equal "FATAL" (pg-error-severity condition))))
                (rove:ok (equal (list other self) (nreverse targets)))
                (rove:ok (eq :source-b (sync-replication-state-source-id *sync-replication-state*)))
                (rove:ok (zerop (hash-table-count *wal-sender-registry*))))
           (setf (symbol-function 'terminate-backend) original))))))
