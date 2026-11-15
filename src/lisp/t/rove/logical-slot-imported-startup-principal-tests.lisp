(in-package #:pg)

(defun call-with-imported-startup-principal-test (function)
  (call-with-logical-slot-real-startup-test
   (lambda ()
     (create-standard-logical-slot "principal_import" :nothing)
     (let ((original (gethash "principal_import" *wal-replication-slots*)))
       (create-standard-logical-slot "principal_later" :nothing)
       (wal-checkpoint)
       (let* ((owner (if (zerop (logical-slot-provenance-generation
                                 (wal-replication-slot-logical-provenance original))) 1 0))
              (slot (logical-slot-state-decode
                     (logical-slot-copy-import-child-image original owner :none) "principal_import"))
              (confirmed
                (loop for value from (wal-replication-slot-confirmed-flush-lsn slot)
                        below *wal-durable-lsn*
                      when (= (length (write-to-string value)) (length (write-to-string (1+ value))))
                        return value)))
         (assert confirmed)
         (setf (wal-replication-slot-confirmed-flush-lsn slot) confirmed)
         (wal-atomic-write-bytes (wal-replication-slot-path "principal_import")
                                 (wal-encode-replication-slot slot))
         (call-with-logical-slot-owned-startup-test
          (lambda (gate-owner ownership)
            (call-with-logical-slot-imported-startup-source
             gate-owner ownership (logical-slot-startup-read-inventory)
             (lambda (source)
               (funcall function source
                        (find "principal_import" (logical-slot-startup-source-entries source)
                              :key (lambda (entry)
                                     (wal-replication-slot-name (logical-slot-startup-entry-slot entry)))
                              :test #'equal)))))))))))

(rove:deftest imported-startup-principal-refuses-coherent-progress-grafts
    (let ((finished nil))
      (rove:ok
       (rove:signals
        (call-with-imported-startup-principal-test
         (lambda (source entry)
           (let* ((slot (logical-slot-startup-entry-slot entry))
                  (prior (wal-replication-slot-confirmed-flush-lsn slot))
                  (bytes (copy-seq (logical-slot-startup-entry-bytes entry))))
             (sb-thread:with-mutex (*wal-lock*)
               (rove:ok (logical-slot-imported-startup-check-under-lock source)))
             (setf (wal-replication-slot-confirmed-flush-lsn slot) (1+ prior))
             (let ((changed (wal-encode-replication-slot slot)))
               (rove:ok (= (length bytes) (length changed)))
               (replace (logical-slot-startup-entry-bytes entry) changed)
               (replace (logical-slot-startup-entry-image entry) changed)
               (wal-atomic-write-bytes (wal-replication-slot-path "principal_import") changed))
             (rove:ok (logical-slot-startup-check-inventory source))
             (sb-thread:with-mutex (*wal-lock*)
               (rove:ok (logical-slot-startup-check-source source))
               (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error)))
             (setf (wal-replication-slot-confirmed-flush-lsn slot) prior
                   (logical-slot-startup-source-active-p source) t)
             (replace (logical-slot-startup-entry-bytes entry) bytes)
             (replace (logical-slot-startup-entry-image entry) bytes)
             (wal-atomic-write-bytes (wal-replication-slot-path "principal_import") bytes)
             (sb-thread:with-mutex (*wal-lock*)
               (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error)))
             (setf finished t)))) 'error))
      (rove:ok finished)))

(rove:deftest imported-startup-principal-bounds-mutable-source-observations
    (dolist (kind '(:control :history :entry-cycle :entry-order))
      (let ((finished nil))
        (rove:ok
         (rove:signals
          (call-with-imported-startup-principal-test
           (lambda (source entry)
             (declare (ignore entry))
             (let* ((entries (logical-slot-startup-source-entries source))
                    (tail (last entries))
                    (history (logical-slot-startup-source-history source))
                    (history-tail (last history))
                    (control (logical-slot-startup-source-control source))
                    (first-byte (aref control 0)))
               (ecase kind
                 (:control (setf (aref control 0) (logxor 1 first-byte)))
                 (:history (setf (cdr history-tail) history))
                 (:entry-cycle (setf (cdr tail) entries))
                 (:entry-order (rotatef (first entries) (second entries))))
               (sb-thread:with-mutex (*wal-lock*)
                 (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error)))
               (ecase kind
                 (:control (setf (aref control 0) first-byte))
                 (:history (setf (cdr history-tail) nil))
                 (:entry-cycle (setf (cdr tail) nil))
                 (:entry-order (rotatef (first entries) (second entries))))
               (setf (logical-slot-startup-source-active-p source) t)
               (sb-thread:with-mutex (*wal-lock*)
                 (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error)))
               (setf finished t)))) 'error))
        (rove:ok finished))))

(rove:deftest imported-startup-principal-keeps-derived-state-and-rejects-copies
    (let ((saved nil) (principal nil))
      (call-with-imported-startup-principal-test
       (lambda (source entry)
         (setf saved source)
         (rove:ok (rove:signals
                   (call-with-logical-slot-imported-startup-principal source #'values) 'error))
         (sb-thread:with-mutex (*wal-lock*)
           (setf principal (logical-slot-imported-startup-principal-under-lock source))
           (rove:ok (rove:signals
                     (logical-slot-imported-startup-check-under-lock
                      (copy-logical-slot-startup-source source)) 'error))
           (rove:ok (rove:signals
                     (funcall (logical-slot-imported-startup-principal-state principal)
                              :identity (copy-logical-slot-imported-startup-principal principal) source) 'error))
           (rove:ok (eq principal (logical-slot-imported-startup-principal-under-lock source))))
         (setf (wal-replication-slot-pending-xids (logical-slot-startup-entry-slot entry))
               (make-hash-table :test #'eql))
         (setf (gethash 17 (wal-replication-slot-pending-xids (logical-slot-startup-entry-slot entry))) 0)
         (let ((thread (sb-thread:make-thread
                        (lambda ()
                          (handler-case
                              (progn
                                (sb-thread:with-mutex (*wal-lock*)
                                  (logical-slot-imported-startup-check-under-lock source))
                                nil)
                            (error () t))))))
           (rove:ok (sb-thread:join-thread thread)))
         (let ((*wal-lock* (sb-thread:make-mutex :name "foreign imported startup mutex")))
           (sb-thread:with-mutex (*wal-lock*)
             (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error))))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (eq principal (logical-slot-imported-startup-principal-under-lock source))))))
      (setf (logical-slot-startup-source-active-p saved) t)
      (sb-thread:with-mutex (*wal-lock*)
        (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock saved) 'error)))
      (rove:ok (rove:signals
                (funcall (logical-slot-imported-startup-principal-state principal)
                         :check principal saved) 'error))))

(rove:deftest imported-startup-principal-rejects-repurposed-ordinary-sources
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (create-standard-logical-slot "ordinary_scope" :nothing)
       (call-with-logical-slot-owned-startup-test
        (lambda (gate-owner ownership)
          (let ((entries (logical-slot-startup-read-inventory)))
            (let* ((called nil)
                   (fake (%make-logical-slot-imported-startup-capture
                          :state (lambda (&rest arguments)
                                   (declare (ignore arguments)) (setf called t)))))
              (rove:ok (rove:signals
                        (%call-with-logical-slot-startup-source
                         gate-owner ownership entries #'values fake) 'error))
              (rove:ok (not called)))
            (let ((cycle (list (first entries))))
              (setf (cdr cycle) cycle)
              (rove:ok (rove:signals
                        (call-with-logical-slot-imported-startup-source
                         gate-owner ownership cycle #'values) 'error)))
            (rove:ok (rove:signals
                      (call-with-logical-slot-imported-startup-source
                       gate-owner ownership (cons (first entries) :tail) #'values) 'error))
            (rove:ok (rove:signals
                      (call-with-logical-slot-imported-startup-source
                       gate-owner ownership
                       (make-list (1+ *max-replication-slots*) :initial-element (first entries))
                       #'values) 'error))
            (call-with-logical-slot-startup-source
             gate-owner ownership entries
             (lambda (source)
               (let* ((entry (first (logical-slot-startup-source-entries source)))
                      (original (logical-slot-startup-entry-slot entry))
                      (owner (if (zerop (logical-slot-provenance-generation
                                         (wal-replication-slot-logical-provenance original))) 1 0))
                      (bytes (logical-slot-copy-import-child-image original owner :none))
                      (slot (logical-slot-state-decode bytes "ordinary_scope"))
                      (replacement (%make-logical-slot-startup-entry
                                    :slot slot :bytes bytes :image (wal-encode-replication-slot slot)
                                    :profile (wal-slot-profile slot)
                                    :provenance (wal-replication-slot-logical-provenance slot)))
                      (entered nil))
                 (setf (first (logical-slot-startup-source-entries source)) replacement)
                 (wal-atomic-write-bytes (wal-replication-slot-path "ordinary_scope") bytes)
                 (rove:ok (logical-slot-startup-check-inventory source))
                 (sb-thread:with-mutex (*wal-lock*)
                   (rove:ok (logical-slot-startup-check-source source)))
                 (rove:ok (rove:signals
                           (call-with-logical-slot-imported-startup-principal
                            source (lambda (value) (declare (ignore value)) (setf entered t))) 'error))
                 (rove:ok (not entered))
                 (sb-thread:with-mutex (*wal-lock*)
                   (rove:ok (logical-slot-startup-check-source source))))))))))))

(rove:deftest imported-startup-principal-revokes-on-registry-loss
    (dolist (kind '(:principal :capture))
      (let ((finished nil))
        (rove:ok
         (rove:signals
          (call-with-imported-startup-principal-test
           (lambda (source entry)
             (declare (ignore entry))
             (let ((*logical-slot-imported-startup-principals*
                     (unless (eq kind :principal) *logical-slot-imported-startup-principals*))
                   (*logical-slot-imported-startup-captures*
                     (unless (eq kind :capture) *logical-slot-imported-startup-captures*)))
               (sb-thread:with-mutex (*wal-lock*)
                 (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error))))
             (setf (logical-slot-startup-source-active-p source) t)
             (sb-thread:with-mutex (*wal-lock*)
               (rove:ok (rove:signals (logical-slot-imported-startup-check-under-lock source) 'error)))
             (setf finished t))) 'error))
        (rove:ok finished))))
