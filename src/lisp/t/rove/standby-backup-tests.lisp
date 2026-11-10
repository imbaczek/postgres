(in-package #:pg)

(defun call-with-standby-backup-checkpoint-test (function)
  (let* ((checkpoint (unit-fpw-snapshot nil t t))
         (*catalog* (make-catalog :next-oid 42))
         (*physical-recovery-state* nil)
         (*physical-recovery-failed-p* nil)
         (*physical-shutdown-state* nil)
         (*swank-listener-enabled-p* nil)
         (*live-operation-depth* 0)
         (*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
         (*live-operation-drain-open* nil)
         (*physical-receive-store* (%make-physical-store))
         (*next-xid* 3)
         (*last-replay-lsn* (getf checkpoint :end)))
    (setf (getf checkpoint :next-oid) 42
          (getf checkpoint :oldest-active-xid) 3
          *physical-recovery-state*
          (list :phase 2 :timeline 3 :replay-timeline 3
                :system-id (catalog-system-identifier)
                :source-checkpoint checkpoint
                :source-reservation (list :version 1 :through *last-replay-lsn* :bound 2 :complete t)))
    (setf (physical-store-system-id *physical-receive-store*) (catalog-system-identifier)
          (physical-store-timeline *physical-receive-store*) 3
          (physical-store-base-lsn *physical-receive-store*) (getf checkpoint :end)
          (getf *physical-recovery-state* :end) (getf checkpoint :end))
    (funcall function checkpoint)))

(rove:deftest standby-backup-checkpoint-requires-the-exact-stable-source-terminal
    (call-with-standby-backup-checkpoint-test
     (lambda (checkpoint)
       (let ((copy (physical-standby-backup-checkpoint)))
         (rove:ok (equal checkpoint copy))
         (rove:ok (not (eq checkpoint copy)))
         (setf (getf copy :next-oid) 99)
         (rove:ok (= 42 (getf (physical-standby-backup-checkpoint) :next-oid))))
       (setf (getf checkpoint :oldest-active-xid) 3)
       (rove:ok (equal checkpoint (physical-standby-backup-checkpoint)))
       (setf (getf checkpoint :oldest-active-xid) 0)
       (rove:ok (unit-commit-ts-error-p #'physical-standby-backup-checkpoint))
       (setf (getf checkpoint :oldest-active-xid) 3)
       (setf (getf checkpoint :next-xid) 4
             (getf checkpoint :xid-reserved-through) 10
             *next-xid* 4
             (getf *physical-recovery-state* :source-reservation)
             (list :version 1 :through *last-replay-lsn* :bound 10 :complete t))
       (rove:ok (unit-commit-ts-error-p #'physical-standby-backup-checkpoint)))))

(rove:deftest standby-backup-checkpoint-rejects-unproved-or-mismatched-context
    (dolist (change '(:later :timeline :oid :missing-oid :unknown-reservation
                      :bound :reservation-position :counter :identity :phase :failed :store))
      (call-with-standby-backup-checkpoint-test
       (lambda (checkpoint)
         (ecase change
           (:later (incf *last-replay-lsn*))
           (:timeline (setf (getf *physical-recovery-state* :replay-timeline) 4))
           (:oid (setf (catalog-next-oid *catalog*) 43))
           (:missing-oid (setf (getf checkpoint :next-oid) nil))
           (:unknown-reservation
            (setf (getf (getf *physical-recovery-state* :source-reservation) :complete) nil))
           (:bound (incf (getf (getf *physical-recovery-state* :source-reservation) :bound)))
           (:reservation-position
            (decf (getf (getf *physical-recovery-state* :source-reservation) :through)))
           (:counter (incf *next-xid*))
           (:identity (incf (getf *physical-recovery-state* :system-id)))
           (:phase (setf (getf *physical-recovery-state* :phase) 1))
           (:failed (setf *physical-recovery-failed-p* t))
           (:store (setf *physical-receive-store* nil)))
         (rove:ok (unit-commit-ts-error-p #'physical-standby-backup-checkpoint))))))

(rove:deftest standby-backup-prefix-verifies-the-original-checkpoint-at-segment-boundaries
    (dolist (boundary '(nil t))
      (let* ((snapshot (unit-fpw-snapshot nil t t))
             (length (- (getf snapshot :end) (getf snapshot :start))))
        (setf (getf snapshot :next-oid) 42)
        (when boundary
          (setf (getf snapshot :start) (- +wal-segment-size+ length)
                (getf snapshot :redo) (- +wal-segment-size+ length)
                (getf snapshot :end) +wal-segment-size+))
        (let* ((start (getf snapshot :start))
               (offset (mod start +wal-segment-size+))
               (payload (wal-encode-checkpoint-snapshot snapshot))
               (prefix (make-array (1+ (mod (1- (getf snapshot :end)) +wal-segment-size+))
                                   :element-type '(unsigned-byte 8) :initial-element 0)))
          (replace prefix (wal-encode-record +wal-record-checkpoint+ 0 start payload) :start1 offset)
          (multiple-value-bind (copy decoded) (physical-standby-backup-check-prefix prefix snapshot)
            (rove:ok (equalp copy prefix))
            (rove:ok (not (eq copy prefix)))
            (rove:ok (equal decoded snapshot)))
          (let ((wrong (copy-list snapshot)))
            (setf (getf wrong :next-oid) 43)
            (rove:ok (unit-commit-ts-error-p
                      (lambda () (physical-standby-backup-check-prefix prefix wrong)))))
          (rove:ok (unit-commit-ts-error-p
                    (lambda ()
                      (physical-standby-backup-check-prefix
                       (subseq prefix 0 (1- (length prefix))) snapshot))))
          (let ((bad (copy-seq prefix)))
            (setf (aref bad (+ offset 28)) (logxor 1 (aref bad (+ offset 28))))
            (rove:ok (unit-commit-ts-error-p
                      (lambda () (physical-standby-backup-check-prefix bad snapshot)))))
          (let ((bad (copy-seq prefix)))
            (replace bad (wal-encode-record +wal-record-checkpoint+ 3 start payload) :start1 offset)
            (rove:ok (unit-commit-ts-error-p
                      (lambda () (physical-standby-backup-check-prefix bad snapshot)))))))))
