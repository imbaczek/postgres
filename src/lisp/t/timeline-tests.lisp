;;;; C timeline history syntax and native ancestry checks, without promotion.
(in-package #:pg-physical-tests)

(deftest timeline-history-uses-c-half-open-point-intervals
    (let ((history (pg::timeline-parse-history 4 (format nil "# comment~%1~c0/40~cfirst~%3~c0/80~csecond~%"
                                                         #\Tab #\Tab #\Tab #\Tab))))
      (ok (= 1 (pg::timeline-at-point 63 history)))
      (ok (= 3 (pg::timeline-at-point 64 history)))
      (ok (= 4 (pg::timeline-at-point 128 history)))
      (ok (pg::timeline-prefix-boundary-p 1 64 history))
      (ok (not (pg::timeline-prefix-boundary-p 1 65 history)))
      (ok (pg::timeline-prefix-boundary-p 3 128 history))
      (ok (not (pg::timeline-prefix-boundary-p 2 64 history)))))

(deftest timeline-history-validates-identifiers-before-native-ancestry-use
    (dolist (text '("not a history" "1 nope" "3 0/40" "1 0/40
1 0/80"))
      (ok (intake-fails-p (lambda () (pg::timeline-parse-history 3 text))) text))
  ;; C's reader accepts backwards switchpoints; native ancestry must reject them.
  (let ((history (pg::timeline-parse-history 3 (format nil "1 0/80~%2 0/40~%"))))
    (ok (intake-fails-p (lambda () (pg::timeline-check-lineage history)))))
  (ok (= 1 (pg::timeline-at-point 0 (pg::timeline-parse-history 1 "")))))

(deftest timeline-history-read-is-bounded-and-does-not-create-missing-files
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "pg_wal/00000002.history" root)))
        (ensure-directories-exist path)
        (ok (intake-fails-p (lambda () (pg::timeline-history-bytes 2 root))))
        (ok (not (probe-file path)))
        (write-startup-config path (format nil "1~c0/40~creason~%" #\Tab #\Tab))
        (ok (= 2 (pg::timeline-at-point 64 (pg::timeline-read-history 2 root))))
        (delete-file path)
        (sb-posix:mkfifo (namestring path) #o600)
        (unwind-protect
             (ok (intake-fails-p (lambda () (pg::timeline-history-bytes 2 root))))
          (delete-file path)))))

(deftest timeline-history-command-parser-requires-a-positive-uint32
    (let ((command (pg::parse-replication-command "TIMELINE_HISTORY 4;")))
      (ok (eq :timeline-history (pg::logical-replication-command-type command)))
      (ok (= 4 (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)))))
  (dolist (query '("TIMELINE_HISTORY" "TIMELINE_HISTORY 0" "TIMELINE_HISTORY -1"
                   "TIMELINE_HISTORY '2'" "TIMELINE_HISTORY 4294967296" "TIMELINE_HISTORY 2 extra"))
    (ok (intake-fails-p (lambda () (pg::parse-replication-command query))) query)))

(deftest timeline-history-remains-read-only-in-recovery
    (let* ((pg::*physical-recovery-state* '(:phase 2 :timeline 1))
           (pg::*physical-recovery-failed-p* nil)
           (pg::*backend* (pg::make-backend :replication-mode :database))
           (command (pg::parse-replication-command "TIMELINE_HISTORY 2")))
      (ok (not (intake-fails-p (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-explicit pg::*backend*) t)
      (ok (intake-fails-p (lambda () (pg::validate-replication-command-context command))))))

(deftest physical-replication-startup-does-not-enable-logical-decoding
    (ok (eq :physical (pg::startup-replication-mode "true")))
  (ok (eq :physical (pg::startup-replication-mode "1")))
  (ok (null (pg::startup-replication-mode "off")))
  (ok (eq :database (pg::startup-replication-mode "database")))
  (ok (intake-fails-p (lambda () (pg::startup-replication-mode "Database"))))
  (let ((pg::*backend* (pg::make-backend :replication-mode :physical)))
    (ok (intake-fails-p
         (lambda ()
           (pg::validate-replication-command-context
            (pg::parse-replication-command "CREATE_REPLICATION_SLOT test LOGICAL pgoutput")))))))

(cl-spec:defproperty timeline-point-switches-at-the-fork-not-before
    ((fork (range integer 1 4611686018427387903)) (offset (range integer 0 100000)))
  (:about pg::timeline-at-point)
  (:trials (:normal 200))
  (let ((history (pg::timeline-parse-history
                  2 (format nil "1~c~a~cpromoted~%" #\Tab (pg::wal-format-lsn fork) #\Tab))))
    (and (= 1 (pg::timeline-at-point (1- fork) history))
         (= 2 (pg::timeline-at-point fork history))
         (= 2 (pg::timeline-at-point (+ fork offset) history))
         (pg::timeline-prefix-boundary-p 1 fork history)
         (not (pg::timeline-prefix-boundary-p 1 (1+ fork) history)))))

(deftest generated-timeline-fork-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'timeline-point-switches-at-the-fork-not-before :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest timeline-history-encoding-matches-c-parent-copy-and-line-layout
    (let* ((parent (pg::string->bytes (format nil "# preserved~%1~c0/00000040~cfirst" #\Tab #\Tab)))
           (bytes (pg::timeline-encode-history 4 3 128 "promoted" parent)))
      (ok (equalp parent (subseq bytes 0 (length parent))))
      (ok (equal (pg::bytes->string bytes)
                 (format nil "# preserved~%1~c0/00000040~cfirst~%3~c0/00000080~cpromoted~%"
                         #\Tab #\Tab #\Tab #\Tab)))
      (ok (= 4 (pg::timeline-at-point 128 (pg::timeline-parse-history 4 (pg::bytes->string bytes))))))
  (ok (equal (pg::bytes->string (pg::timeline-encode-history 2 1 64 "first" nil))
             (format nil "1~c0/00000040~cfirst~%" #\Tab #\Tab))))

(deftest timeline-history-encoding-rejects-invalid-or-unknown-native-ancestry
    (dolist (args (list (list 2 2 64 "same" nil) (list 4294967296 1 64 "overflow" nil)
                        (list 4 3 128 "missing" nil) (list 2 1 -1 "negative" nil)
                        (list 2 1 64 (format nil "injected~%3 0/80") nil)
                        (list 2 1 64 (string #\Null) nil)
                        (list 4 3 32 "backward" (pg::string->bytes "1 0/40"))))
      (ok (intake-fails-p (lambda () (apply #'pg::timeline-encode-history args))))))

(cl-spec:defproperty timeline-encoded-child-starts-at-exact-fork
    ((fork (range integer 1 18446744073709551615)))
  (:about pg::timeline-encode-history)
  (:trials (:normal 200))
  (let* ((bytes (pg::timeline-encode-history 2 1 fork "promoted" nil))
         (history (pg::timeline-parse-history 2 (pg::bytes->string bytes))))
    (and (= 1 (pg::timeline-at-point (1- fork) history))
         (= 2 (pg::timeline-at-point fork history))
         (pg::timeline-prefix-boundary-p 1 fork history))))

(deftest generated-timeline-history-encoding
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'timeline-encoded-child-starts-at-exact-fork :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest timeline-history-publication-never-overwrites-an-existing-child
    (with-physical-store-directory (root)
      (let ((path (pg::timeline-write-history 2 1 64 "first" root)))
        (ok (probe-file path))
        (ok (= 2 (pg::timeline-at-point 64 (pg::timeline-read-history 2 root))))
        (let ((before (pg::timeline-history-bytes 2 root)))
          (ok (intake-fails-p (lambda () (pg::timeline-write-history 2 1 128 "changed" root))))
          (ok (equalp before (pg::timeline-history-bytes 2 root))))
        (pg::timeline-write-history 3 2 128 "second" root)
        (ok (= 3 (pg::timeline-at-point 128 (pg::timeline-read-history 3 root))))
        (ok (= 2 (pg::timeline-at-point 127 (pg::timeline-read-history 3 root)))))))

(deftest timeline-history-publication-failure-keeps-only-complete-final-files
    (dolist (point '(:before-file-fsync :after-file-fsync :after-install :after-directory-fsync))
      (with-physical-store-directory (root)
        (let ((pg::*timeline-history-test-hook*
                (lambda (current) (when (eq point current) (error "history publication failure")))))
          (ok (intake-fails-p (lambda () (pg::timeline-write-history 2 1 64 "first" root)))))
        (if (member point '(:after-install :after-directory-fsync))
            (ok (= 2 (pg::timeline-at-point 64 (pg::timeline-read-history 2 root))))
            (ok (not (probe-file (merge-pathnames "pg_wal/00000002.history" root))))))))
