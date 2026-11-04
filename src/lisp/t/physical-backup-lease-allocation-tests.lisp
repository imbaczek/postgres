(in-package #:pg-physical-tests)

(defun backup-lease-allocation-test-record (&optional (state :live) (generation 1) (digest 991))
  (let ((request (backup-lease-test-request)))
    (list :version 1 :request request :generation generation
          :name (copy-seq "allocated_follow")
          :coordinates (list :version 1 :system-id (getf request :system-id)
                             :base-timeline (getf request :timeline) :base 152
                             :manifest-digest digest :source-timeline 2 :receive-timeline 2
                             :receive-lsn 152 :receive-digest digest
                             :overlap-start 0 :overlap-digest digest)
          :state state)))

(cl-spec:defproperty backup-lease-allocation-envelope-preserves-ownership
    ((digest (range integer 0 18446744073709551615))
     (generation (range integer 1 16))
     (choice (range integer 0 2))
     (canceled boolean))
  (:about pg::physical-backup-lease-allocation-image pg::physical-backup-lease-allocation-copy)
  (:trials (:normal 80))
  (let* ((record (backup-lease-allocation-test-record
                  (if canceled :canceled :live) generation digest))
         (coordinates (getf record :coordinates)))
    (case choice
      (1 (setf (getf coordinates :source-timeline) 3))
      (2 (setf (getf coordinates :source-timeline) 4
               (getf record :coordinates) (append '(:kind :retained-timeline) coordinates))))
    (let* ((checked (pg::physical-backup-lease-allocation-copy record))
           (image (pg::physical-backup-lease-allocation-image record)))
      (and (equal checked (pg::physical-backup-lease-allocation-decode image))
           (equalp image (pg::physical-backup-lease-allocation-image checked))))))

(deftest generated-backup-lease-allocation-envelope-ownership
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-allocation-envelope-preserves-ownership
                                         :seed 104 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(cl-spec:defproperty backup-lease-allocation-cancellation-is-permanent
    ((digest (range integer 0 18446744073709551615))
     (generation (range integer 1 16)))
  (:about pg::physical-backup-lease-allocation-transition)
  (:trials (:normal 80))
  (let* ((live (backup-lease-allocation-test-record :live generation digest))
         (canceled (backup-lease-allocation-test-record :canceled generation digest))
         (before (pg::physical-bootstrap-manifest-text live)))
    (and (equal live (pg::physical-backup-lease-allocation-transition nil live))
         (equal canceled (pg::physical-backup-lease-allocation-transition nil canceled))
         (equal canceled (pg::physical-backup-lease-allocation-transition live canceled))
         (equal canceled (pg::physical-backup-lease-allocation-transition canceled canceled))
         (intake-fails-p (lambda () (pg::physical-backup-lease-allocation-transition canceled live)))
         (equal before (pg::physical-bootstrap-manifest-text live)))))

(deftest generated-backup-lease-allocation-cancellation-fence
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-allocation-cancellation-is-permanent
                                         :seed 105 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-allocation-rejects-foreign-or-unbounded-records
    (ok (pg::physical-backup-lease-allocation-image (backup-lease-allocation-test-record)))
  (dolist (fault '(:version :generation-zero :generation-limit :state :name :parent-name
                   :system :base-timeline :base :cursor :overlap :unaligned-overlap
                   :receive-timeline :source :extra :duplicate))
    (let* ((record (backup-lease-allocation-test-record))
           (coordinates (getf record :coordinates)))
      (ecase fault
        (:version (setf (getf record :version) 2))
        (:generation-zero (setf (getf record :generation) 0))
        (:generation-limit (setf (getf record :generation) 17))
        (:state (setf (getf record :state) :ready))
        (:name (setf (getf record :name) "../foreign"))
        (:parent-name (setf (getf record :name) (getf (getf record :request) :name)))
        (:system (incf (getf coordinates :system-id)))
        (:base-timeline (setf (getf coordinates :base-timeline) 1))
        (:base (setf (getf coordinates :base) 0))
        (:cursor (setf (getf coordinates :receive-lsn) 151))
        (:overlap (setf (getf coordinates :overlap-start) 152))
        (:unaligned-overlap (setf (getf coordinates :overlap-start) 1))
        (:receive-timeline (setf (getf coordinates :receive-timeline) 1))
        (:source (setf (getf coordinates :source-timeline) 1))
        (:extra (setf record (append record '(:unexpected t))))
        (:duplicate (setf record (append record '(:generation 1)))))
      (ok (intake-fails-p (lambda () (pg::physical-backup-lease-allocation-image record)))))))

(deftest backup-lease-allocation-copy-and-transition-detach-all-ownership
    (let* ((record (backup-lease-allocation-test-record))
           (before (pg::physical-bootstrap-manifest-text record))
           (copy (pg::physical-backup-lease-allocation-copy record)))
      (setf (char (getf copy :name) 0) #\b
            (char (getf (getf copy :request) :name) 7) #\f
            (char (getf (getf copy :request) :token) 0) #\b
            (char (getf (getf copy :request) :cluster-identity) 0) #\B
            (getf (getf copy :coordinates) :manifest-digest) 992)
      (ok (equal before (pg::physical-bootstrap-manifest-text record)))
      (dolist (field '(:request :generation :name :coordinates))
        (let ((candidate (pg::physical-backup-lease-allocation-copy record)))
          (ecase field
            (:request (setf (char (getf (getf candidate :request) :token) 0) #\b))
            (:generation (incf (getf candidate :generation)))
            (:name (setf (getf candidate :name) "another_follow"))
            (:coordinates (incf (getf (getf candidate :coordinates) :manifest-digest))))
          (ok (intake-fails-p
               (lambda () (pg::physical-backup-lease-allocation-transition record candidate))))))
      (ok (equal before (pg::physical-bootstrap-manifest-text record)))))

(deftest backup-lease-allocation-envelope-rejects-other-records-and-corruption
    (let* ((record (backup-lease-allocation-test-record))
           (image (pg::physical-backup-lease-allocation-image record))
           (lease (pg::physical-backup-lease-image (backup-lease-test-record :ready))))
      (dolist (position (list 0 15 (1- (length image))))
        (let ((bad (copy-seq image)))
          (setf (aref bad position) (logxor 1 (aref bad position)))
          (ok (intake-fails-p (lambda () (pg::physical-backup-lease-allocation-decode bad))))))
      (dolist (bad (list lease (coerce image 'list) (coerce image 'simple-vector)
                         (make-array 8193 :element-type '(unsigned-byte 8) :initial-element 0)
                         (concatenate '(vector (unsigned-byte 8)) image (pg::string->bytes "NIL"))))
        (ok (intake-fails-p (lambda () (pg::physical-backup-lease-allocation-decode bad)))))
      (ok (intake-fails-p (lambda () (pg::physical-backup-lease-decode image))))))

(deftest backup-lease-envelope-keeps-its-existing-byte-format
    (dolist (phase '(:requested :ready :interrupted :closing :closed))
      (dolist (outcome (if (member phase '(:closing :closed)) '(:aborted :handed-off) '(nil)))
        (let ((record (backup-lease-test-record (if (eq phase :requested) :requested :ready))))
          (setf (getf record :phase) phase
                (getf record :outcome) outcome)
          (when (eq outcome :handed-off)
            (setf (getf record :successor) (list :name "legacy_successor" :anchor-digest 991)))
          (let* ((text (pg::physical-bootstrap-manifest-text record))
                 (body (pg::string->bytes text))
                 (legacy (pg::string->bytes
                          (format nil "PGBACKUPLEASE1 ~8,'0X~%~a~%" (pg::wal-crc32 body) text))))
            (ok (equalp legacy (pg::physical-backup-lease-image record)))
            (ok (equal record (pg::physical-backup-lease-decode legacy))))))))

(defun backup-lease-allocation-test-check-graph (parents records)
  (let ((parent-table (make-hash-table :test #'equal))
        (allocation-table (make-hash-table :test #'equal)))
    (dolist (parent parents)
      (setf (gethash (getf (getf parent :request) :name) parent-table) parent))
    (dolist (record records)
      (setf (gethash (getf record :name) allocation-table) record))
    (pg::physical-backup-lease-allocation-check-graph parent-table allocation-table)))

(cl-spec:defproperty backup-lease-allocation-history-keeps-canceled-prefix
    ((count (range integer 1 16)) (canceled boolean) (interrupted boolean))
  (:about pg::physical-backup-lease-allocation-check-graph)
  (:trials (:normal 80))
  (let ((parent (backup-lease-test-record :ready))
        (records nil))
    (when interrupted (setf (getf parent :phase) :interrupted))
    (loop for generation from 1 to count
          for record = (backup-lease-allocation-test-record
                        (if (or canceled (< generation count)) :canceled :live) generation)
          do (setf (getf record :name) (format nil "allocated_follow_~d" generation))
             (push record records))
    (backup-lease-allocation-test-check-graph (list parent) records)
    t))

(deftest generated-backup-lease-allocation-history-check
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-allocation-history-keeps-canceled-prefix
                                         :seed 106 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-allocation-graph-requires-exact-parent-and-monotone-history
    (ok (backup-lease-allocation-test-check-graph
         (list (backup-lease-test-record :ready)) (list (backup-lease-allocation-test-record))))
  (dolist (fault '(:missing-parent :foreign-request :base :digest :requested :capturing
                   :empty-interrupted :generation-gap :older-live :generation-reuse))
    (let* ((parent (backup-lease-test-record :ready))
           (record (backup-lease-allocation-test-record))
           (parents (list parent))
           (records (list record)))
      (ecase fault
        (:missing-parent (setf parents nil))
        (:foreign-request (setf (char (getf (getf record :request) :token) 0) #\b))
        (:base (decf (getf (getf record :coordinates) :base)))
        (:digest (incf (getf (getf record :coordinates) :manifest-digest)))
        (:requested (setf parent (backup-lease-test-record) parents (list parent)))
        (:capturing (setf parent (backup-lease-test-record) parents (list parent)
                          (getf parent :phase) :capturing))
        (:empty-interrupted (setf parent (backup-lease-test-record) parents (list parent)
                                  (getf parent :phase) :interrupted))
        (:generation-gap (setf (getf record :generation) 2))
        ((:older-live :generation-reuse)
         (let ((next (backup-lease-allocation-test-record
                      :live (if (eq fault :generation-reuse) 1 2))))
           (setf (getf next :name) "different_follow")
           (push next records))))
      (ok (intake-fails-p
           (lambda () (backup-lease-allocation-test-check-graph parents records)))
          (format nil "~s" fault)))))

(deftest backup-lease-allocation-graph-preserves-terminal-and-legacy-ownership
    (let ((parent (backup-lease-test-record :ready))
          (record (backup-lease-allocation-test-record)))
      (dolist (phase '(:closing :closed))
        (setf (getf parent :phase) phase (getf parent :outcome) :aborted
              (getf parent :successor) nil)
        (ok (backup-lease-allocation-test-check-graph (list parent) (list record)))
        (setf (getf parent :outcome) :handed-off
              (getf parent :successor) (list :name (getf record :name) :anchor-digest 991))
        (ok (backup-lease-allocation-test-check-graph (list parent) (list record)))
        (ok (backup-lease-allocation-test-check-graph (list parent) nil))
        (setf (getf record :state) :canceled)
        (ok (intake-fails-p
             (lambda () (backup-lease-allocation-test-check-graph (list parent) (list record)))))
        (setf (getf record :state) :live))
      (let ((other (backup-lease-test-record :ready)))
        (setf (char (getf (getf other :request) :name) 7) #\f
              (getf parent :phase) :ready (getf parent :outcome) nil
              (getf parent :successor) nil)
        (let ((saved (getf record :name)))
          (setf (getf record :name) (getf (getf other :request) :name))
          (ok (intake-fails-p
               (lambda () (backup-lease-allocation-test-check-graph
                           (list parent other) (list record)))))
          (setf (getf record :name) saved))
        (setf (getf other :phase) :closed (getf other :outcome) :handed-off
              (getf other :successor) (list :name (getf record :name) :anchor-digest 991))
        (ok (intake-fails-p
             (lambda () (backup-lease-allocation-test-check-graph
                         (list parent other) (list record))))))))
