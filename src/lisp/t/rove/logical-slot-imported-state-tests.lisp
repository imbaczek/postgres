(in-package #:pg)

(defun unit-imported-state-baseline (&key (cause :none) (generation 0) (owner 104))
  (unit-logical-provenance-slot :plugin "pg_lisp" :cause cause
                                :generation generation :child-owner owner))

(defun unit-imported-state-copy (slot)
  (logical-slot-state-decode (logical-slot-state-encode slot) (wal-replication-slot-name slot)))

(defun unit-imported-state-with-origin
    (slot &key (provenance (wal-replication-slot-logical-provenance slot))
            (owner (wal-replication-slot-child-owner slot)))
  (let ((copy (make-wal-replication-slot
               :name (wal-replication-slot-name slot)
               :database-oid (wal-replication-slot-database-oid slot)
               :restart-lsn (wal-replication-slot-restart-lsn slot)
               :consistent-lsn (wal-replication-slot-consistent-lsn slot)
               :confirmed-flush-lsn (wal-replication-slot-confirmed-flush-lsn slot)
               :plugin (wal-replication-slot-plugin slot)
               :logical-provenance provenance :child-owner owner
               :restart-timeline (wal-replication-slot-restart-timeline slot)
               :confirmed-timeline (wal-replication-slot-confirmed-timeline slot)
               :invalidation-cause (logical-slot-effective-invalidation-cause slot)
               :wal-status (wal-replication-slot-wal-status slot))))
    (wal-slot-set-initial-profile copy (wal-slot-profile slot))
    copy))

(cl-spec:defproperty imported-state-keeps-origin-through-progress
    ((advance (range integer 0 128)) (restart (range integer 0 128))
                                     (generation (range integer 0 340282366920938463463374607431768211455))
                                     (cause-index (range integer 0 2)))
  (:about logical-slot-imported-check-state)
  (:trials (:normal 200))
  (let* ((baseline (unit-imported-state-baseline :generation generation :owner (logxor generation 1)))
         (current (unit-imported-state-copy baseline))
         (cause (nth cause-index '(:none :wal-removed :wal-level-insufficient)))
         (history (timeline-parse-history 1 "")))
    (setf (wal-replication-slot-confirmed-flush-lsn current) (+ 80 advance)
          (wal-replication-slot-restart-lsn current) (+ 32 (min restart (+ 48 advance)))
          (wal-replication-slot-invalidation-cause current) cause
          (wal-replication-slot-wal-status current) (if (eq cause :none) :reserved :lost))
    (let ((before (logical-slot-state-encode current))
          (original (logical-slot-state-encode baseline)))
      (and (not (eq (wal-replication-slot-logical-provenance current)
                    (wal-replication-slot-logical-provenance baseline)))
           (eq current (logical-slot-imported-check-state current baseline history 1000))
           (equalp before (logical-slot-state-encode current))
           (equalp original (logical-slot-state-encode baseline))))))

(rove:deftest imported-state-progress-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'imported-state-keeps-origin-through-progress
                                         :seed 7524 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest imported-state-rejects-progress-regression-and-uncertainty
    (dolist (mode '(:restart :confirmed :ahead :uncertain :creation :drop :owner :generation :p6))
      (let* ((baseline (unit-imported-state-baseline))
             (current (unit-imported-state-copy baseline)))
        (ecase mode
          (:restart (setf (wal-replication-slot-restart-lsn current) 31))
          (:confirmed (setf (wal-replication-slot-confirmed-flush-lsn current) 79))
          (:ahead (setf (wal-replication-slot-confirmed-flush-lsn current) 1001))
          (:uncertain (setf (wal-replication-slot-state-uncertain-p current) t))
          (:creation (setf (wal-replication-slot-creation-failed-p current) t))
          (:drop (setf (wal-replication-slot-drop-claim current) :pending))
          (:owner (setf current (unit-imported-state-with-origin current :owner 105)))
          (:generation
           (setf current (unit-imported-state-with-origin
                          current :provenance (make-logical-slot-provenance 42 1 65 1 65 1))))
          (:p6 (setf current (unit-imported-state-with-origin current :owner nil))))
        (rove:ok (rove:signals
                  (logical-slot-imported-check-state
                   current baseline (timeline-parse-history 1 "") 1000) 'error)
                 (format nil "~a rejected" mode)))))

(rove:deftest imported-state-preserves-terminal-baseline-exactly
    (dolist (cause '(:wal-removed :wal-level-insufficient))
      (let* ((baseline (unit-imported-state-baseline :cause cause))
             (current (unit-imported-state-copy baseline))
             (history (timeline-parse-history 1 "")))
        (rove:ok (eq current (logical-slot-imported-check-state current baseline history 1000)))
        (dolist (mode '(:revive :cause :restart :confirmed))
          (let ((changed (unit-imported-state-copy current)))
            (ecase mode
              (:revive (setf (wal-replication-slot-invalidation-cause changed) :none
                             (wal-replication-slot-wal-status changed) :reserved))
              (:cause (setf (wal-replication-slot-invalidation-cause changed)
                            (if (eq cause :wal-removed) :wal-level-insufficient :wal-removed)))
              (:restart (incf (wal-replication-slot-restart-lsn changed)))
              (:confirmed (incf (wal-replication-slot-confirmed-flush-lsn changed))))
            (rove:ok (rove:signals
                      (logical-slot-imported-check-state changed baseline history 1000) 'error)))))))

(defun unit-imported-history-slot ()
  (let ((slot (unit-imported-state-with-origin
               (unit-imported-state-baseline)
               :provenance (make-logical-slot-provenance 42 0 160 3 64 1))))
    (setf (wal-replication-slot-consistent-lsn slot) 160
          (wal-replication-slot-restart-lsn slot) 32
          (wal-replication-slot-restart-timeline slot) 1
          (wal-replication-slot-confirmed-flush-lsn slot) 192
          (wal-replication-slot-confirmed-timeline slot) 3)
    slot))

(rove:deftest imported-state-rejects-valid-encoded-immutable-grafts
    (let ((history (list (make-timeline-history-entry :timeline 3 :begin 128)
                         (make-timeline-history-entry :timeline 2 :begin 64 :end 128)
                         (make-timeline-history-entry :timeline 1 :begin 0 :end 64))))
      (dolist (mode '(:name :database :plugin :profile :birth :owner
                      :system :generation :birth-timeline :on-lsn :on-timeline :on-unknown))
        (let* ((baseline (unit-imported-history-slot))
               (current (unit-imported-state-copy baseline)))
          (case mode
            (:name (setf (wal-replication-slot-name current) "other"))
            (:database (setf (wal-replication-slot-database-oid current) 8))
            (:plugin (setf (wal-replication-slot-plugin current) "pgoutput"))
            (:profile (setf (gethash current *wal-slot-profiles*) :binary-bytea))
            (:birth (setf (wal-replication-slot-consistent-lsn current) 161))
            (:owner (setf current (unit-imported-state-with-origin current :owner 105)))
            (otherwise
             (setf current
                   (unit-imported-state-with-origin
                    current :provenance
                    (make-logical-slot-provenance
                     (if (eq mode :system) 43 42) (if (eq mode :generation) 1 0) 160
                     (if (eq mode :birth-timeline) 2 3)
                     (cond ((eq mode :on-unknown) nil) ((eq mode :on-lsn) 63) (t 64))
                     (cond ((eq mode :on-unknown) nil) ((eq mode :on-timeline) 2) (t 1)))))))
          (rove:ok (logical-slot-state-valid-p current) (format nil "~a remains valid state" mode))
          (setf current (unit-imported-state-copy current))
          (rove:ok (not (logical-slot-state-identity-equal-p current baseline)))
          (rove:ok (rove:signals
                    (logical-slot-imported-check-state current baseline history 1000) 'error)
                   (format nil "~a rejects after canonical CRC round trip" mode))))))

(rove:deftest imported-state-confirmation-and-restart-owners-at-a-fork
    (let* ((history (list (make-timeline-history-entry :timeline 2 :begin 64)
                          (make-timeline-history-entry :timeline 1 :begin 0 :end 64)))
           (baseline (unit-imported-state-with-origin
                      (unit-imported-state-baseline)
                      :provenance (make-logical-slot-provenance 42 0 64 1 64 1))))
      (setf (wal-replication-slot-consistent-lsn baseline) 64
            (wal-replication-slot-confirmed-flush-lsn baseline) nil
            (wal-replication-slot-confirmed-timeline baseline) nil)
      (rove:ok (logical-slot-imported-check-state
                (unit-imported-state-copy baseline) baseline history 256))
      (dolist (confirmed '(64 160))
        (let ((current (unit-imported-state-copy baseline)))
          (setf (wal-replication-slot-confirmed-flush-lsn current) confirmed
                (wal-replication-slot-confirmed-timeline current) (if (= confirmed 64) 1 2))
          (rove:ok (eq current (logical-slot-imported-check-state current baseline history 256)))
          (setf (wal-replication-slot-confirmed-timeline current) (if (= confirmed 64) 2 1))
          (rove:ok (logical-slot-state-valid-p current))
          (rove:ok (rove:signals
                    (logical-slot-imported-check-state current baseline history 256) 'error))))
      (setf (wal-replication-slot-confirmed-flush-lsn baseline) 64
            (wal-replication-slot-confirmed-timeline baseline) 1)
      (let ((current (unit-imported-state-copy baseline)))
        (setf (wal-replication-slot-confirmed-timeline current) 2)
        (rove:ok (logical-slot-state-valid-p current))
        (rove:ok (rove:signals
                  (logical-slot-imported-check-state current baseline history 256) 'error)))
      (setf (wal-replication-slot-confirmed-flush-lsn baseline) 160
            (wal-replication-slot-confirmed-timeline baseline) 2
            (wal-replication-slot-restart-lsn baseline) 64
            (wal-replication-slot-restart-timeline baseline) 2)
      (let ((current (unit-imported-state-copy baseline)))
        (setf (wal-replication-slot-restart-timeline current) 1)
        (rove:ok (eq current (logical-slot-imported-check-state current baseline history 256)))
        (setf (wal-replication-slot-invalidation-cause baseline) :wal-removed
              (wal-replication-slot-wal-status baseline) :lost
              (wal-replication-slot-invalidation-cause current) :wal-removed
              (wal-replication-slot-wal-status current) :lost)
        (rove:ok (rove:signals
                  (logical-slot-imported-check-state current baseline history 256) 'error)))))
