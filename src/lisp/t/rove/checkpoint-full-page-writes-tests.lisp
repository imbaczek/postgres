(in-package #:pg)

(defun unit-fpw-snapshot (policy resources commit-ts)
  (append (unit-commit-ts-snapshot :resources resources :known commit-ts)
          (unless (eq policy :unknown) (list :full-page-writes policy))))

(cl-spec:defgenerator unit-fpw-fact-arguments ()
  (let ((policy (nth (random 3) '(:unknown nil t))))
    (list (append (list :unrelated 7)
                  (unless (eq policy :unknown) (list :full-page-writes policy))))))

(cl-spec:defspec-function wal-copy-checkpoint-full-page-writes-fields
    (:args (facts (type list)))
  (:args-generator unit-fpw-fact-arguments)
  (:returns (type list))
  (:post (if (get-properties facts '(:full-page-writes))
             (and (equal result (list :full-page-writes (getf facts :full-page-writes)))
                  (not (eq result facts)))
             (null result))))

(rove:deftest checkpoint-full-page-writes-capture-and-copy
    (dolist (policy '(:unknown nil t))
      (let* ((*xid-lock* (sb-thread:make-mutex :name "FPW snapshot XID"))
             (*write-lock* (sb-thread:make-mutex :name "FPW snapshot writer"))
             (*wal-lock* (sb-thread:make-mutex :name "FPW snapshot WAL"))
             (*next-xid* 3) (*xid-batch-end* 3) (*active-xids* nil) (*catalog* nil)
             (*wal-insert-timeline* 1) (*wal-prev-timeline* 1)
             (facts (unless (eq policy :unknown) (list :full-page-writes policy))))
        (sb-thread:with-mutex (*write-lock*)
          (sb-thread:with-mutex (*wal-lock*)
            (let ((snapshot (wal-capture-checkpoint-snapshot 64 facts)))
              (rove:ok (equal (wal-copy-checkpoint-full-page-writes-fields snapshot) facts))
              (rove:ok (equal facts (wal-copy-checkpoint-full-page-writes-fields facts)))
              (when facts
                (setf (getf facts :full-page-writes) (not policy))
                (rove:ok (eq policy (getf snapshot :full-page-writes)))))))))
  (let ((result (cl-spec:result-data
                 (cl-spec:check-function 'wal-copy-checkpoint-full-page-writes-fields :trials 100 :seed 249))))
    (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))

(cl-spec:defproperty checkpoint-full-page-writes-roundtrip
    ((policy (member :unknown nil t)) (resources (member nil t)) (commit-ts (member nil t)))
  (:about wal-encode-checkpoint-snapshot wal-decode-checkpoint-snapshot)
  (:trials (:normal 100))
  (let* ((snapshot (unit-fpw-snapshot policy resources commit-ts))
         (bytes (wal-encode-checkpoint-snapshot snapshot))
         (decoded (wal-decode-checkpoint-snapshot bytes)))
    (and (equal snapshot decoded)
         (= (length bytes) (+ (if resources 136 88) (if commit-ts 8 0)))
         (equalp bytes (wal-encode-checkpoint-snapshot decoded)))))

(rove:deftest checkpoint-full-page-writes-preserves-three-states
    (dolist (policy '(:unknown nil t))
      (dolist (resources '(nil t))
        (dolist (commit-ts '(nil t))
          (dolist (exhausted '(nil t))
            (let* ((snapshot (unit-fpw-snapshot policy resources commit-ts))
                   (flags (logior (if commit-ts 4 0) (if exhausted 3 0)
                                  (case policy (:unknown 0) ((nil) 8) (otherwise 24)))))
              (when exhausted
                (setf (getf snapshot :next-xid) 18446744073709551616
                      (getf snapshot :xid-reserved-through) 18446744073709551615
                      (getf snapshot :oldest-active-xid) 18446744073709551616))
              (let ((bytes (wal-encode-checkpoint-snapshot snapshot)))
                (rove:ok (wal-checkpoint-snapshot-valid-p snapshot))
                (rove:ok (= flags (wal-u32 bytes 84)))
                (rove:ok (equal snapshot (wal-decode-checkpoint-snapshot bytes)))
                (rove:ok (equal snapshot
                                (wal-checkpoint-snapshot-from-hex (wal-checkpoint-snapshot-hex snapshot))))
                (rove:ok (= (+ 32 (length bytes)) (wal-checkpoint-record-size snapshot)))
                (let* ((metadata (append (make-wal-control-metadata 42 4 0)
                                         (wal-copy-control-resource-fields snapshot)))
                       (control (wal-encode-control-image
                                 (getf snapshot :end) (getf snapshot :end) snapshot
                                 :timeline 3 :checkpoint-timeline 3 :namespace :timeline :metadata metadata)))
                  (rove:ok (equal snapshot (third (multiple-value-list
                                                   (wal-decode-control-line (bytes->string control))))))))))))))

(rove:deftest checkpoint-full-page-writes-rejects-malformed-facts-and-flags
    (dolist (facts '((:full-page-writes 0) (:full-page-writes :off)
                     (:full-page-writes t :full-page-writes nil)
                     (:full-page-writes nil :full-page-writes nil)))
      (let ((snapshot (append (unit-commit-ts-snapshot) facts)))
        (rove:ok (not (wal-checkpoint-snapshot-valid-p snapshot)))
        (rove:ok (unit-commit-ts-error-p (lambda () (wal-checkpoint-record-size snapshot))))
        (rove:ok (unit-commit-ts-error-p (lambda () (wal-encode-checkpoint-snapshot snapshot))))))
  (dolist (policy '(nil t))
    (let ((snapshot (unit-fpw-snapshot policy nil nil)))
      (setf (getf snapshot :version) 1)
      (rove:ok (not (wal-checkpoint-snapshot-valid-p snapshot)))))
  (dolist (flags '(16 20 32 255))
    (let ((bytes (wal-encode-checkpoint-snapshot (unit-commit-ts-snapshot))))
      (wal-set-u32 bytes 84 flags)
      (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bytes))))))
  (dolist (policy '(nil t))
    (let* ((snapshot (unit-fpw-snapshot policy nil nil))
           (payload (wal-encode-checkpoint-snapshot snapshot))
           (record (wal-decode-record
                    (wal-encode-record +wal-record-checkpoint+ 0 64 payload) 64)))
      (setf (wal-record-version record) 7)
      (rove:ok (unit-commit-ts-error-p (lambda () (wal-checkpoint-record-snapshot record))))
      (setf (wal-record-version record) 8)
      (rove:ok (equal snapshot (wal-checkpoint-record-snapshot record))))))

(rove:deftest checkpoint-full-page-writes-generated-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'checkpoint-full-page-writes-roundtrip :seed 248))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))

(rove:deftest checkpoint-full-page-writes-reporting-preserves-known-false
    (dolist (policy '(:unknown nil t))
      (dolist (installed '(nil t))
        (let* ((*startup-installed-configuration* (when installed (unit-commit-ts-configuration)))
               (facts (unit-commit-ts-control-facts nil))
               (snapshot (unit-fpw-snapshot policy nil nil)))
          (setf (getf facts :snapshot) snapshot)
          (let* ((row (native-control-checkpoint-record facts))
                 (record (sql-record-values row))
                 (rows (native-control-data-rows facts)))
            (rove:ok (eq (if (eq policy :unknown) +null+ policy) (aref record 5)))
            (rove:ok (search (format nil "\"full_page_writes\":~a"
                                     (case policy (:unknown "null") ((nil) "false") (otherwise "true")))
                             (sql-to-json row :record)))
            (rove:ok (equal (case policy (:unknown nil) ((nil) "off") (otherwise "on"))
                            (cdr (assoc "Latest checkpoint's full_page_writes" rows :test #'string=)))))))))
