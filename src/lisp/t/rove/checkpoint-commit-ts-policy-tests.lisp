(in-package #:pg)

(defun unit-commit-ts-configuration ()
  (let ((settings (startup-default-settings (uiop:temporary-directory))))
    (startup-check-feature-support settings)
    (make-startup-configuration :settings settings)))

(defun unit-commit-ts-control-facts (known)
  (let ((snapshot (unit-commit-ts-snapshot :known known)))
    (list :system-id 42 :db-state +db-in-production+
          :checkpoint (getf snapshot :end) :checkpoint-timeline 3
          :snapshot snapshot :recovery-lsn 0 :recovery-timeline 0)))

(rove:deftest checkpoint-commit-ts-policy-requires-an-installed-canonical-false-entry
    (let ((*startup-installed-configuration* nil)
          (*startup-server-settings* (startup-default-settings (uiop:temporary-directory))))
      (rove:ok (null (startup-checkpoint-commit-ts-facts))))
  (let* ((*startup-installed-configuration* (unit-commit-ts-configuration))
         (facts (startup-checkpoint-commit-ts-facts)))
    (rove:ok (equal facts '(:oldest-commit-ts-xid 0 :newest-commit-ts-xid 0)))
    (setf (getf facts :oldest-commit-ts-xid) 3)
    (rove:ok (equal (startup-checkpoint-commit-ts-facts)
                    '(:oldest-commit-ts-xid 0 :newest-commit-ts-xid 0))))
  (dolist (entry '(nil (:source :default) (:value t) (:value "off") (:value 0)
                   (:value nil :value t) (:value nil :value nil) (:value nil :dangling)))
    (let ((settings (make-hash-table :test #'equal)))
      (when entry (setf (gethash "track_commit_timestamp" settings) entry))
      (let ((*startup-installed-configuration* (make-startup-configuration :settings settings)))
        (rove:ok (unit-commit-ts-error-p (lambda () (startup-checkpoint-commit-ts-facts)))))))
  (let ((*startup-installed-configuration* (make-startup-configuration :settings nil)))
    (rove:ok (unit-commit-ts-error-p (lambda () (startup-checkpoint-commit-ts-facts))))))

(rove:deftest checkpoint-commit-ts-policy-is-read-before-storage-locks
    (let ((policy (when (fboundp 'startup-checkpoint-commit-ts-facts)
                    (symbol-function 'startup-checkpoint-commit-ts-facts)))
          (settle (symbol-function 'wal-settle-page-publications-under-lock))
          (*write-lock* (sb-thread:make-mutex :name "checkpoint policy test writer"))
          (*wal-lock* (sb-thread:make-mutex :name "checkpoint policy test WAL"))
          (*wal-poisoned* nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'startup-checkpoint-commit-ts-facts)
                   (lambda ()
                     (rove:ok (not (sb-thread:holding-mutex-p *write-lock*)))
                     (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                     (throw 'unit-commit-ts-capture :policy))
                   (symbol-function 'wal-settle-page-publications-under-lock)
                   (lambda () (throw 'unit-commit-ts-capture :storage)))
             (rove:ok (eq :policy (catch 'unit-commit-ts-capture (wal-checkpoint-core)))))
        (if policy (setf (symbol-function 'startup-checkpoint-commit-ts-facts) policy)
            (fmakunbound 'startup-checkpoint-commit-ts-facts))
        (setf (symbol-function 'wal-settle-page-publications-under-lock) settle))))

(rove:deftest checkpoint-commit-ts-reports-source-facts-without-runtime-defaults
    (dolist (installed '(nil t))
      (let ((*startup-installed-configuration* (when installed (unit-commit-ts-configuration))))
        (dolist (known '(nil t))
          (let* ((facts (unit-commit-ts-control-facts known))
                 (values (sql-record-values (native-control-checkpoint-record facts)))
                 (rows (native-control-data-rows facts)))
            (dolist (index '(16 17))
              (rove:ok (eql (if known 0 +null+) (aref values index))))
            (dolist (label '("Latest checkpoint's oldestCommitTsXid"
                             "Latest checkpoint's newestCommitTsXid"))
              (rove:ok (equal (when known "0") (cdr (assoc label rows :test #'string=))))))))))

(cl-spec:defproperty historical-commit-ts-reporting-ignores-installed-policy
    ((known (member nil t)) (installed (member nil t)))
  (:about native-control-checkpoint-record native-control-data-rows)
  (:trials (:normal 100))
  (let* ((*startup-installed-configuration* (when installed (unit-commit-ts-configuration)))
         (facts (unit-commit-ts-control-facts known))
         (values (sql-record-values (native-control-checkpoint-record facts)))
         (rows (native-control-data-rows facts)))
    (and (eql (if known 0 +null+) (aref values 16))
         (eql (if known 0 +null+) (aref values 17))
         (equal (when known "0") (cdr (assoc "Latest checkpoint's oldestCommitTsXid" rows :test #'string=)))
         (equal (when known "0") (cdr (assoc "Latest checkpoint's newestCommitTsXid" rows :test #'string=))))))

(rove:deftest checkpoint-commit-ts-promotion-freezes-source-facts-before-placement
    (dolist (resources '(nil t))
      (dolist (known '(nil t))
        (dolist (fork (list 512 (- +wal-segment-size+ 264) (- +wal-segment-size+ 263)
                            (- +wal-segment-size+ 256) (- +wal-segment-size+ 255)
                            (- +wal-segment-size+ 216) (- +wal-segment-size+ 215)
                            (- +wal-segment-size+ 208) (- +wal-segment-size+ 207)
                            +wal-segment-size+))
          (let* ((source (unit-commit-ts-snapshot :resources resources :known known))
                 (state (list :phase 3 :applied fork :timeline 1 :promotion-child 2
                              :next-xid 50 :state-time 1793030000
                              :source-reservation
                              (list :version 1 :through fork :bound 1002 :complete t))))
            (setf (getf source :timeline) 1 (getf source :prev-timeline) 1)
            (when resources (setf (getf source :resource-timeline) 1))
            (assert (wal-checkpoint-snapshot-valid-p source))
            (setf (getf state :source-checkpoint) source
                  (getf state :source-resource-fact) (wal-copy-control-resource-fields source))
            (let* ((before (copy-tree state))
                   (*startup-installed-configuration* nil)
                   (first (physical-promotion-checkpoint-plan state 20000 1003))
                   (*startup-installed-configuration* (unit-commit-ts-configuration))
                   (retry (physical-promotion-checkpoint-plan state 20000 1003))
                   (snapshot (getf retry :snapshot))
                   (frame (third (getf retry :frames))))
              (rove:ok (equalp (getf first :frames) (getf retry :frames)))
              (rove:ok (equal before state))
              (rove:ok (eql (when known 0) (getf snapshot :oldest-commit-ts-xid)))
              (rove:ok (eql (when known 0) (getf snapshot :newest-commit-ts-xid)))
              (rove:ok (= (+ 32 (if resources 136 88) (if known 8 0))
                          (length (second frame))))
              (rove:ok (equal snapshot
                              (wal-checkpoint-record-snapshot
                               (wal-decode-record (second frame) (first frame)))))
              (dolist (entry (getf retry :frames))
                (rove:ok (<= (+ (mod (first entry) +wal-segment-size+) (length (second entry)))
                             +wal-segment-size+)))
              (when known
                (setf (getf snapshot :oldest-commit-ts-xid) 3)
                (rove:ok (zerop (getf source :oldest-commit-ts-xid))))))))))

(rove:deftest checkpoint-commit-ts-generated-historical-reporting
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'historical-commit-ts-reporting-ignores-installed-policy
                                         :seed 247))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
