(in-package #:pg)

(defun unit-continuation-prepared (&key (generation 1) (identity 0) (advance 0) standard)
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs identity)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (old (logical-slot-continuation-active-snapshot binding certificate host rows))
           (new (logical-slot-continuation-copy old))
           (slot (logical-slot-state-decode (second (first (getf new :slots))) "alpha"))
           (ack (+ 128 advance))
           (floor (if standard (+ 96 advance) ack))
           (seal (+ 192 advance)))
      (when standard
        (dolist (snapshot (list old new))
          (let* ((row (first (getf snapshot :slots)))
                 (state (logical-slot-state-decode (second row) "alpha")))
            (setf (wal-replication-slot-plugin state) "pgoutput"
                  (second row) (logical-slot-state-encode state))))
        (setf (wal-replication-slot-plugin slot) "pgoutput"))
      (setf (getf old :generation) generation
            (getf new :generation) (1+ generation)
            (getf new :sealed-end) (list seal 2 :prefix-end)
            (wal-replication-slot-restart-lsn slot) 80
            (wal-replication-slot-restart-timeline slot) 1
            (wal-replication-slot-confirmed-flush-lsn slot) ack
            (wal-replication-slot-confirmed-timeline slot) 2
            (second (first (getf new :slots))) (logical-slot-state-encode slot))
      (list :version 1 :kind (if standard :standard-ack :native-ack) :name "alpha"
            :old old :new new :ack (list ack 2 :prefix-end)
            :restart-floor (list floor (if (= floor 96) 1 2) :prefix-end)
            :proof-end (list seal 2 :prefix-end)))))

(cl-spec:defproperty continuation-prepared-roundtrips-complete-snapshots
    ((generation (range integer 1 18446744073709551614))
     (identity (range integer 0 340282366920938463463374607431768211455))
     (advance (range integer 0 31))
     (standard-index (range integer 0 1)))
  (:about logical-slot-continuation-prepared-encode)
  (:trials (:normal 200))
  (let* ((record (unit-continuation-prepared :generation generation :identity identity
                                             :advance advance :standard (plusp standard-index)))
         (image (logical-slot-continuation-prepared-encode record))
         (decoded (logical-slot-continuation-prepared-decode image))
         (old (getf decoded :old)) (new (getf decoded :new)))
    (and (equalp image (logical-slot-continuation-prepared-encode decoded))
         (= generation (getf old :generation))
         (= (1+ generation) (getf new :generation))
         (equal '("alpha" "beta") (mapcar #'first (getf old :slots)))
         (equal '("alpha" "beta") (mapcar #'first (getf new :slots)))
         (not (eq (getf old :binding) (getf (getf record :old) :binding)))
         (eq :old (logical-slot-continuation-prepared-classify decoded (getf old :slots)))
         (eq :new (logical-slot-continuation-prepared-classify decoded (getf new :slots))))))

(rove:deftest continuation-prepared-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-prepared-roundtrips-complete-snapshots
                                         :seed 11505 :profile :normal))))
      (format t "~&PREPARED-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-prepared-standard-ack-requires-pgoutput
    (let ((record (unit-continuation-prepared)))
      (setf (getf record :kind) :standard-ack)
      (rove:ok (handler-case
                   (progn (logical-slot-continuation-prepared-encode record) nil)
                 (error () t)))))

(rove:deftest continuation-prepared-supports-initial-and-zero-floor-acks
    (dolist (mode '(:initial :zero-floor))
      (let* ((record (unit-continuation-prepared :standard (eq mode :zero-floor)))
             (old-row (first (getf (getf record :old) :slots)))
             (new-row (first (getf (getf record :new) :slots)))
             (prior (logical-slot-state-decode (second old-row) "alpha"))
             (next (logical-slot-state-decode (second new-row) "alpha")))
        (ecase mode
          (:initial
           (setf (wal-replication-slot-confirmed-flush-lsn prior) nil
                 (wal-replication-slot-confirmed-timeline prior) nil
                 (wal-replication-slot-confirmed-flush-lsn next) 65
                 (wal-replication-slot-confirmed-timeline next) 1
                 (wal-replication-slot-restart-lsn next) 32
                 (getf record :ack) '(65 1 :prefix-end)
                 (getf record :restart-floor) '(65 1 :prefix-end)))
          (:zero-floor
           (setf (wal-replication-slot-restart-lsn prior) 0
                 (wal-replication-slot-restart-lsn next) 0
                 (getf record :restart-floor) '(0 1 :record-start))))
        (setf (second old-row) (logical-slot-state-encode prior)
              (second new-row) (logical-slot-state-encode next))
        (rove:ok (handler-case
                     (equalp record
                             (logical-slot-continuation-prepared-decode
                              (logical-slot-continuation-prepared-encode record)))
                   (error () nil))
                 (format nil "~s roundtrips" mode)))))

(rove:deftest continuation-prepared-rejects-unrelated-or-unproved-transitions
    (dolist (mode '(:generation :binding :history :origin :beta :target :ack :floor :seal :role :noop))
      (let* ((record (unit-continuation-prepared))
             (old (getf record :old)) (new (getf record :new)))
        (ecase mode
          (:generation (incf (getf new :generation)))
          (:binding (incf (aref (getf new :binding) 0)))
          (:history (setf (getf new :history-target) 3))
          (:origin (incf (second (getf new :origin))))
          (:beta (setf (getf new :slots) (list (first (getf new :slots)))))
          (:target (setf (getf record :name) "Alpha"))
          (:ack (incf (first (getf record :ack))))
          (:floor (incf (first (getf record :restart-floor))))
          (:seal (incf (first (getf record :proof-end))))
          (:role (setf (third (getf record :ack)) :record-start))
          (:noop (setf (getf new :slots) (logical-slot-continuation-copy (getf old :slots)))))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-prepared-encode record) nil)
                   (error () t))
                 (format nil "~s is rejected" mode)))))

(rove:deftest continuation-prepared-classifies-the-complete-canonical-inventory
    (let* ((record (unit-continuation-prepared))
           (old (getf (getf record :old) :slots))
           (new (getf (getf record :new) :slots)))
      (rove:ok (eq :old (logical-slot-continuation-prepared-classify record old)))
      (rove:ok (eq :new (logical-slot-continuation-prepared-classify record new)))
      (dolist (mode '(:missing :extra :duplicate :reordered :beta :corrupt :case :cycle))
        (let ((observed (logical-slot-continuation-copy new)))
          (ecase mode
            (:missing (setf observed (cdr observed)))
            (:extra (setf observed (append observed (list (first old)))))
            (:duplicate (setf (second observed) (first observed)))
            (:reordered (setf observed (reverse observed)))
            (:beta (setf (second (second observed)) (copy-seq (second (first old)))))
            (:corrupt (setf (aref (second (first observed)) 0) 0))
            (:case (setf (first (first observed)) "Alpha"))
            (:cycle (setf (cdr (last observed)) observed)))
          (rove:ok (eq :blocked (logical-slot-continuation-prepared-classify record observed))
                   (format nil "~s cannot select a side" mode))))))

(rove:deftest continuation-prepared-classifier-preserves-interrupt-conditions
    (dolist (mode '(:cancel :fatal :wal-failure :timeout))
      (let* ((record (unit-continuation-prepared))
             (observed (logical-slot-continuation-copy (getf (getf record :old) :slots)))
             (reader (symbol-function 'logical-slot-continuation-prepared-rows))
             (wal-check (symbol-function 'current-backend-wal-failed-p))
             (latched nil)
             (condition (case mode
                          (:cancel (make-condition 'pg-error :sqlstate "57014"))
                          (:fatal (make-condition 'pg-error :severity "FATAL" :sqlstate "57P01"))
                          (:timeout (make-condition 'sb-ext:timeout))
                          (t (make-condition 'simple-error :format-control "latched failure"))))
             (caught nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'current-backend-wal-failed-p) (lambda () latched)
                     (symbol-function 'logical-slot-continuation-prepared-rows)
                     (lambda (rows)
                       (when (eq rows observed)
                         (setf latched (eq mode :wal-failure))
                         (error condition))
                       (funcall reader rows)))
               (setf caught (handler-case
                                (logical-slot-continuation-prepared-classify record observed)
                              (error (failure) failure)
                              (sb-ext:timeout (failure) failure))))
          (setf (symbol-function 'logical-slot-continuation-prepared-rows) reader
                (symbol-function 'current-backend-wal-failed-p) wal-check))
        (rove:ok (eq condition caught) (format nil "~s preserves condition identity" mode)))))

(rove:deftest continuation-prepared-rejects-canonical-incarnation-and-inventory-grafts
    (dolist (mode '(:target :unrelated))
      (let* ((record (unit-continuation-prepared))
             (rows (getf (getf record :new) :slots))
             (row (if (eq mode :target) (first rows) (second rows)))
             (name (first row))
             (slot (logical-slot-state-decode (second row) name))
             (provenance (wal-replication-slot-logical-provenance slot)))
        (setf slot
              (unit-imported-state-with-origin
               slot :provenance
               (make-logical-slot-provenance
                (logical-slot-provenance-system-id provenance)
                (1+ (logical-slot-provenance-generation provenance))
                (wal-replication-slot-consistent-lsn slot)
                (logical-slot-provenance-birth-timeline provenance)
                (logical-slot-provenance-on-start-lsn provenance)
                (logical-slot-provenance-on-start-timeline provenance)))
              (second row) (logical-slot-state-encode slot))
        (rove:ok (equalp (second row)
                         (logical-slot-state-encode
                          (logical-slot-state-decode (second row) name))))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-prepared-encode record) nil)
                   (error () t))
                 (format nil "~s canonical graft is rejected" mode))
        (rove:ok (eq :blocked (logical-slot-continuation-prepared-classify
                               (unit-continuation-prepared) rows))))))

(rove:deftest continuation-prepared-envelope-bounds-and-detachment
    (let* ((record (unit-continuation-prepared))
           (image (logical-slot-continuation-prepared-encode record))
           (saved (copy-seq image))
           (decoded (logical-slot-continuation-prepared-decode image)))
      (setf (aref (getf (getf decoded :old) :binding) 0) 99
            (aref (getf (getf decoded :new) :binding) 0) 98)
      (rove:ok (equalp saved image))
      (dolist (mode '(:crc :truncated :trailing :length :nested-length))
        (let ((bad (copy-seq image)))
          (ecase mode
            (:crc (setf (aref bad 20) 255))
            (:truncated (setf bad (subseq bad 0 (1- (length bad)))))
            (:trailing (setf bad (concatenate '(vector (unsigned-byte 8)) bad #(0))))
            (:length (wal-set-u32 bad 16 #xffffffff))
            (:nested-length (wal-set-u32 bad 30 #xffffffff)))
          (when (member mode '(:length :nested-length))
            (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4))))
          (rove:ok (handler-case
                       (progn (logical-slot-continuation-prepared-decode bad) nil)
                     (error () t))
                   (format nil "~s is rejected" mode))))))

(cl-spec:defproperty continuation-selected-generation-keeps-its-historical-seal
    ((generation (range integer 1 18446744073709551614))
     (advance (range integer 0 31)) (growth (range integer 1 63)))
  (:about logical-slot-continuation-check-selected-snapshot)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host ignored) (unit-continuation-active-inputs)
    (declare (ignore ignored))
    (let* ((prepared (unit-continuation-prepared :generation generation :advance advance))
           (snapshot (getf prepared :new))
           (saved (logical-slot-continuation-encode snapshot))
           (seal (first (getf snapshot :sealed-end)))
           (horizon (+ seal growth)))
      (setf (sixth host) horizon)
      (and (logical-slot-continuation-check-selected-snapshot
            snapshot (getf snapshot :binding) certificate host
            (getf snapshot :slots) horizon)
           (= (1+ generation) (getf snapshot :generation))
           (= seal (first (getf snapshot :sealed-end)))
           (= horizon (sixth host))
           (equalp saved (logical-slot-continuation-encode snapshot))))))

(rove:deftest continuation-selected-generation-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-selected-generation-keeps-its-historical-seal
                                         :seed 11506 :profile :normal))))
      (format t "~&SELECTED-GENERATION-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-selected-generation-rejects-foreign-or-stale-images
    (dolist (mode '(:binding :system :history :rows :horizon :future-horizon))
      (multiple-value-bind (certificate host ignored) (unit-continuation-active-inputs)
        (declare (ignore ignored))
        (let* ((snapshot (getf (unit-continuation-prepared) :new))
               (binding (copy-seq (getf snapshot :binding)))
               (rows (logical-slot-continuation-copy (getf snapshot :slots)))
               (horizon 256))
          (setf (sixth host) horizon)
          (ecase mode
            (:binding (incf (aref binding 0)))
            (:system (incf (first host)))
            (:history (setf (fifth host) 3))
            (:rows (setf rows (list (first rows))))
            (:horizon (setf horizon 191))
            (:future-horizon (incf horizon)))
          (rove:ok (handler-case
                       (progn (logical-slot-continuation-check-selected-snapshot
                               snapshot binding certificate host rows horizon) nil)
                     (error () t))
                   (format nil "~s is rejected" mode))))))
