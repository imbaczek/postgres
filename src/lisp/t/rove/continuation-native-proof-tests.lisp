(in-package #:pg)

(rove:deftest continuation-terminal-scan-freezes-requested-end-and-real-purpose
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore end))
       (let ((escaped nil) (entered nil) (pins (hash-table-count *wal-snapshot-retention-pins*)))
         (rove:ok
          (equal '(:terminal 37)
                 (multiple-value-list
                  (call-with-logical-slot-terminal-scan
                   "cursor_source" middle
                   (lambda (current) (rove:ok (eq current slot)) (values nil nil))
                   (lambda (scan source owner role)
                     (declare (ignore owner role))
                     (setf escaped scan entered t)
                     (rove:ok (= start (wal-retained-slot-scan-start scan)))
                     (rove:ok (= middle (wal-retained-slot-scan-end scan)))
                     (rove:ok (eq :terminal-check (logical-wal-source-purpose source)))
                     (sb-thread:with-mutex (*wal-lock*)
                       (rove:ok (wal-retained-slot-scan-check-scope-under-lock scan))
                       (rove:ok
                        (eq :terminal-check
                            (wal-retained-slot-scan-scope-purpose
                             (find scan *wal-retained-slot-scan-scopes*
                                   :key #'wal-retained-slot-scan-scope-scan :test #'eq)))))
                     (values :terminal 37))
                   :end middle))))
         (rove:ok entered)
         (rove:ok (= pins (hash-table-count *wal-snapshot-retention-pins*)))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (handler-case (progn (wal-retained-slot-scan-check-scope-under-lock escaped) nil)
                      (error () t))))))))

(rove:deftest continuation-terminal-scan-short-circuit-does-not-install-a-pin
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore slot start end))
       (let ((entered nil) (pins (hash-table-count *wal-snapshot-retention-pins*)))
         (rove:ok
          (eq :already-done
              (call-with-logical-slot-terminal-scan
               "cursor_source" middle (lambda (current) (declare (ignore current)) (values t :already-done))
               (lambda (&rest ignored) (declare (ignore ignored)) (setf entered t)))))
         (rove:ok (not entered))
         (rove:ok (= pins (hash-table-count *wal-snapshot-retention-pins*)))))))

(cl-spec:defproperty continuation-native-proof-keeps-the-seal-at-a-proved-boundary
    ((advance (range integer 0 256)) (found-bit (range integer 0 1)))
  (:about logical-slot-continuation-native-proof-result)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
           (slot (logical-slot-state-decode
                  (second (find "alpha" (getf snapshot :slots) :key #'first :test #'string=)) "alpha"))
           (target (+ (wal-replication-slot-confirmed-flush-lsn slot) advance))
           (found (= found-bit 1)))
      (if (and (plusp advance) (not found))
          (handler-case
              (progn (logical-slot-continuation-native-proof-result snapshot "alpha" target found) nil)
            (error () t))
          (multiple-value-bind (kind seal)
              (logical-slot-continuation-native-proof-result snapshot "alpha" target found)
            (let* ((horizon (max target (first (getf snapshot :sealed-end))))
                   (history (logical-slot-continuation-history
                             (getf snapshot :history-target) (getf snapshot :history))))
              (and (eq kind (if (zerop advance) :unchanged :advance))
                   (equal (list horizon (timeline-at-point (1- horizon) history) :prefix-end) seal))))))))

(rove:deftest continuation-native-proof-boundary-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-native-proof-keeps-the-seal-at-a-proved-boundary
                                         :seed 11510 :profile :normal))))
      (format t "~&NATIVE-PROOF-BOUNDARY-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-native-proof-rejects-forged-receipts-before-io
    (let* ((calls 0) (reads 0)
                     (original (symbol-function 'physical-image-file-facts))
                     (fake (%make-logical-slot-continuation-native-proof
                            :old-image (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)
                            :state (lambda (&rest arguments) (declare (ignore arguments)) (incf calls) t))))
      (unwind-protect
           (progn
             (setf (symbol-function 'physical-image-file-facts)
                   (lambda (&rest arguments) (incf reads) (apply original arguments)))
             (let ((*logical-slot-continuation-native-proofs* (list fake)))
               (sb-thread:with-mutex (*wal-lock*)
                 (dolist (value (list nil '(:version 1 :kind :native-ack) fake
                                      (copy-logical-slot-continuation-native-proof fake)))
                   (rove:ok
                    (handler-case (progn (logical-slot-continuation-native-proof-check-under-lock value) nil)
                      (error () t))))))
             (rove:ok (zerop calls))
             (rove:ok (zerop reads)))
        (setf (symbol-function 'physical-image-file-facts) original))))

(defun unit-native-proof-real-continuation (start end)
  (let ((certificate nil))
    (call-with-wal-retained-slot-scan
     "cursor_source" start end
     (lambda (scan)
       (multiple-value-bind (owner role)
           (sb-thread:with-mutex (*wal-lock*) (logical-slot-wal-scan-start-position scan))
         (call-with-logical-slot-wal-cursor-from-scan
          scan owner role
          (lambda (cursor source)
            (loop while (logical-wal-cursor-next cursor))
            (sb-thread:with-mutex (*wal-lock*)
              (logical-wal-cursor-check-scope cursor)
              (setf certificate (logical-wal-certify-boundary source end))))))))
    certificate))

(rove:deftest continuation-terminal-check-preserves-fast-path-multiple-values
    (dolist (kind '(:legacy :continuation))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore end))
         (let* ((*wal-logical-slot-read-continuation*
                  (when (eq kind :continuation) (unit-native-proof-real-continuation start middle)))
                (scans 0) (finished nil)
                (original (symbol-function 'call-with-logical-terminal-cursor))
                (legacy
                  (when (eq kind :legacy)
                    (make-wal-replication-slot
                     :name (wal-replication-slot-name slot)
                     :database-oid (wal-replication-slot-database-oid slot)
                     :restart-lsn (wal-replication-slot-restart-lsn slot)
                     :consistent-lsn (wal-replication-slot-consistent-lsn slot)
                     :confirmed-flush-lsn (wal-replication-slot-confirmed-flush-lsn slot)
                     :plugin (wal-replication-slot-plugin slot)
                     :active-owner (wal-replication-slot-active-owner slot)
                     :active-owner-backend (wal-replication-slot-active-owner-backend slot)))))
           (unwind-protect
                (progn
                  (when legacy
                    (sb-thread:with-mutex (*wal-lock*)
                      (wal-save-replication-slot legacy)
                      (setf (gethash "cursor_source" *wal-replication-slots*) legacy)))
                  (setf (symbol-function 'call-with-logical-terminal-cursor)
                        (lambda (&rest ignored)
                          (declare (ignore ignored))
                          (incf scans)
                          (error "fast path unexpectedly scanned")))
                  (rove:ok
                   (equal '(:one :two)
                          (multiple-value-list
                           (call-with-logical-slot-terminal-check
                            "cursor_source" middle (lambda (current) (declare (ignore current)) (values nil nil))
                            (lambda (current found)
                              (declare (ignore current found))
                              (setf finished t)
                              (values :one :two))))))
                  (rove:ok finished)
                  (rove:ok (zerop scans)))
             (setf (symbol-function 'call-with-logical-terminal-cursor) original)
             (when legacy
               (sb-thread:with-mutex (*wal-lock*)
                 (setf (gethash "cursor_source" *wal-replication-slots*) slot)
                 (wal-save-replication-slot slot)))))))))

(rove:deftest continuation-native-ack-candidate-is-fresh-and-does-not-publish
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (let ((pending (wal-replication-slot-pending-xids slot))
             (before (wal-encode-replication-slot slot))
             (writes 0)
             (original (symbol-function 'wal-save-replication-slot)))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-save-replication-slot)
                      (lambda (&rest ignored)
                        (declare (ignore ignored))
                        (incf writes)
                        (error "candidate derivation attempted persistence")))
                (sb-thread:with-mutex (*wal-lock*)
                  (let ((first (wal-replication-slot-ack-candidate-under-lock slot middle)))
                    (rove:ok (= middle (wal-replication-slot-restart-lsn first)))
                    (setf (gethash 71 pending) (cons start end))
                    (let ((second (wal-replication-slot-ack-candidate-under-lock slot middle)))
                      (rove:ok (= start (wal-replication-slot-restart-lsn second)))
                      (rove:ok (= middle (wal-replication-slot-confirmed-flush-lsn second)))
                      (rove:ok (not (eq pending (wal-replication-slot-pending-xids second))))
                      (rove:ok (equal (cons start end)
                                      (gethash 71 (wal-replication-slot-pending-xids second))))
                      (rove:ok (null (gethash 71 (wal-replication-slot-pending-xids first))))
                      (rove:ok (eq (wal-replication-slot-logical-provenance slot)
                                   (wal-replication-slot-logical-provenance second))))))
                (rove:ok (zerop writes))
                (rove:ok (eq pending (wal-replication-slot-pending-xids slot)))
                (rove:ok (equalp before (wal-encode-replication-slot slot)))
                (rove:ok (not (wal-replication-slot-state-uncertain-p slot))))
           (setf (symbol-function 'wal-save-replication-slot) original))))))

(rove:deftest continuation-native-preflight-rejects-unregistered-receipts-before-io
    (rove:ok (fboundp 'logical-slot-continuation-native-preflight-under-lock))
  (let ((reads 0) (original (symbol-function 'physical-image-file-facts)))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-image-file-facts)
                 (lambda (&rest arguments) (incf reads) (apply original arguments)))
           (sb-thread:with-mutex (*wal-lock*)
             (dolist (value (list nil (%make-logical-slot-continuation-native-proof
                                       :state (lambda (&rest ignored)
                                                (declare (ignore ignored))
                                                (error "unregistered state must not run")))))
               (rove:ok
                (handler-case
                    (progn (logical-slot-continuation-native-preflight-under-lock value) nil)
                  (error () t)))))
           (rove:ok (zerop reads)))
      (setf (symbol-function 'physical-image-file-facts) original))))

(rove:deftest continuation-protection-rejects-forged-registration-before-io
    (rove:ok (fboundp 'logical-slot-continuation-protection-call))
  (rove:ok (fboundp 'logical-slot-continuation-make-protection-under-lock))
  (let* ((reads 0) (calls 0)
                   (original (symbol-function 'physical-image-file-facts))
                   (root '(101 202 303))
                   (fake (%make-logical-slot-continuation-protection
                          :root-identity root
                          :state (lambda (&rest ignored) (declare (ignore ignored)) (incf calls) t)))
                   (*logical-slot-continuation-protections* (make-hash-table :test #'equal)))
    (setf (gethash root *logical-slot-continuation-protections*) fake)
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-image-file-facts)
                 (lambda (&rest arguments) (incf reads) (apply original arguments)))
           (sb-thread:with-mutex (*wal-lock*)
             (dolist (value (list nil fake (copy-logical-slot-continuation-protection fake)))
               (rove:ok (handler-case
                            (progn (logical-slot-continuation-protection-call value :fail) nil)
                          (error () t))))
             (rove:ok
              (handler-case
                  (progn
                    (logical-slot-continuation-make-protection-under-lock
                     (%make-logical-slot-continuation-native-proof :root-identity root))
                    nil)
                (error () t))))
           (rove:ok (zerop calls))
           (rove:ok (zerop reads)))
      (setf (symbol-function 'physical-image-file-facts) original))))
