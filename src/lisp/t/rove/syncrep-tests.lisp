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
