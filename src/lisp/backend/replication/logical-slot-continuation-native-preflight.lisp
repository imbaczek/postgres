(in-package #:pg)

(defun logical-slot-continuation-native-preflight-selector (proof)
  ;; Reopen the sole selector and compare its exact bytes with the receipt,
  ;; then recheck the live proof before decoding the observed image.
  (let ((directory (logical-slot-continuation-native-proof-directory proof))
        (expected (logical-slot-continuation-native-proof-old-image proof))
        (selected-facts nil))
    (unless (equal '("selector") (logical-slot-copy-import-control-names directory 3))
      (error "native writer requires one stable selector"))
    (call-with-logical-slot-copy-source-proof-fd
     directory "selector" nil
     (lambda (fd facts)
       (unless (equalp expected
                       (logical-slot-copy-import-control-read-fd
                        fd (logical-slot-continuation-maximum-bytes)))
         (error "native writer selector differs from its completed observation"))
       (setf selected-facts (copy-list facts))))
    (unless (equal '("selector") (logical-slot-copy-import-control-names directory 3))
      (error "native writer selector directory changed"))
    (logical-slot-continuation-native-proof-check-under-lock proof)
    (values (logical-slot-continuation-decode expected) selected-facts)))

;; Compare ordinary slots as decoded candidates so compatible disk encodings
;; compare by state.  Child slots still require their exact encoded image; the
;; separate inventory ledger retains the raw bytes for every slot.
(defun logical-slot-continuation-native-preflight-registry (rows)
  (unless (= (length rows) (hash-table-count *wal-replication-slots*))
    (error "native writer registry differs from its complete inventory"))
  (dolist (row rows)
    (let ((slot (wal-find-usable-replication-slot-under-lock (first row))))
      (unless (and slot
                   (if (wal-replication-slot-child-owner slot)
                       (equalp (second row) (logical-slot-state-encode slot))
                       (equalp
                        (logical-slot-startup-candidate-image slot)
                        (logical-slot-startup-candidate-image
                         (wal-decode-replication-slot-image (second row) (first row))))))
        (error "native writer live slot differs from its saved incarnation"))))
  t)

(defun logical-slot-continuation-native-preflight-under-lock (proof)
  ;; Recheck the selector, complete saved inventory, registry, exact target
  ;; object and current ACK permission.  Derive the candidate from current
  ;; pending transactions.  A no-op leaves the generation alone; an advance
  ;; builds and validates a PREPARED transition.
  ;;
  ;; The returned record, candidate and closed inventory ledger do not grant
  ;; write authority.  A later writer must recheck resources and inodes and
  ;; protect both old and new retention obligations before publishing.
  (logical-slot-continuation-native-proof-check-under-lock proof)
  (let* ((scan (logical-slot-continuation-native-proof-scan proof))
         (slot (wal-retained-slot-scan-slot scan))
         (name (wal-retained-slot-scan-name scan))
         (target (logical-slot-continuation-native-proof-target-lsn proof))
         (old (logical-slot-continuation-native-preflight-selector proof)))
    ;; Keep the ledger for all disk entries, but compare only child rows with
    ;; the selector.  The registry check below still covers the full inventory.
    (multiple-value-bind (rows ledger)
        (logical-slot-continuation-read-slot-inventory
         (logical-slot-continuation-native-proof-slot-root proof)
         (lambda () (logical-slot-continuation-native-proof-check-under-lock proof)))
      (unless (logical-slot-continuation-slot-rows-equal-p
               (logical-slot-continuation-native-proof-partition-under-lock proof rows)
               (getf old :slots))
        (error "native writer complete inventory differs from its selected image"))
      (logical-slot-continuation-native-preflight-registry rows)
      (unless (eq slot (gethash name *wal-replication-slots*))
        (error "native writer target changed registry identity"))
      (let ((confirmed (wal-check-replication-slot-ack-under-lock
                        slot name target (wal-retained-slot-scan-owner scan))))
        (when (= target confirmed)
          (multiple-value-bind (ignored selected-facts)
              (logical-slot-continuation-native-preflight-selector proof)
            (declare (ignore ignored))
            (return-from logical-slot-continuation-native-preflight-under-lock
              (values nil slot ledger selected-facts)))))
      (let* ((candidate (wal-replication-slot-ack-candidate-under-lock slot target))
             (new (logical-slot-continuation-copy old))
             (seal (copy-list (logical-slot-continuation-native-proof-seal proof)))
             (ack (list target (wal-replication-slot-confirmed-timeline candidate) :prefix-end)))
        (logical-slot-check-update-under-lock slot candidate)
        (when (= #xffffffffffffffff (getf new :generation))
          (error "native writer continuation generation exhausted"))
        (incf (getf new :generation))
        (setf (getf new :sealed-end) seal
              (second (find name (getf new :slots) :key #'first :test #'string=))
              (logical-slot-state-encode candidate))
        (let ((record (list :version 1 :kind :native-ack :name name :old old :new new
                            :ack ack :restart-floor (copy-list ack) :proof-end (copy-list seal))))
          (logical-slot-continuation-prepared-check record)
          (multiple-value-bind (ignored selected-facts)
              (logical-slot-continuation-native-preflight-selector proof)
            (declare (ignore ignored))
            (values record candidate ledger selected-facts)))))))
