(in-package #:pg-physical-tests)

(defun origin-proof-test-request (reference end)
  (list :system-id (pg::catalog-system-identifier)
        :base-timeline (getf reference :base-timeline) :base (getf reference :base)
        :manifest-digest (getf reference :manifest-digest) :end end))

(defun origin-proof-client-test-schema ()
  (let ((names (list "slot_name" "anchor_digest" "system_id" "base_timeline" "base_lsn"
                     "manifest_digest" "end_lsn" "end_digest" "root_id" "fork_lsn" "source_tli" "history")))
    (loop for name in names
          for index from 0
          collect (list name (if (member index '(3 10)) pg::+int8oid+ pg::+textoid+)))))

(defun origin-proof-client-test-row (reference prefix pin)
  (list "origin_ready" (format nil "~d" (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin)))
        (format nil "~d" (pg::catalog-system-identifier)) "1" "0/00000098"
        (format nil "~d" (getf reference :manifest-digest)) "0/000000B8"
        (format nil "~d" (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 152 184)))
        (format nil "~d" (getf reference :root-id)) "0/000000D8" "2"
        (pg::bytes->string (pg::timeline-history-bytes 2))))

(deftest origin-proof-client-requires-the-exact-prepared-anchor-and-typed-result
    (call-with-origin-proof-source
     (lambda (root reference prefix pin command)
       (declare (ignore root command))
       (let ((request (origin-proof-test-request reference 184))
             (anchor (pg::physical-follow-pin-anchor pin)))
         (dolist (mode '(:valid :extra :missing :oid :slot :anchor :system :timeline :base
                         :manifest :end :digest :root :fork :source :history))
           (let ((row (origin-proof-client-test-row reference prefix pin))
                 (schema (origin-proof-client-test-schema)))
             (case mode
               (:oid (setf (second (nth 7 schema)) pg::+int8oid+))
               (:slot (setf (first row) "different"))
               (:anchor (setf (second row) "0"))
               (:system (setf (third row) "0"))
               (:timeline (setf (fourth row) "2"))
               (:base (setf (fifth row) "0/00000099"))
               (:manifest (setf (sixth row) "0"))
               (:end (setf (seventh row) "0/000000B9"))
               (:digest (setf (eighth row) "-1"))
               (:root (setf (ninth row) "18446744073709551616"))
               (:fork (setf (tenth row) "0/000000B7"))
               (:source (setf (nth 10 row) "3"))
               (:history (setf (nth 11 row) "changed")))
             (call-with-physical-client-wire
              (lambda (client query-path)
                (declare (ignore query-path))
                (if (eq mode :valid)
                    (let ((proof (pg::pg-client-native-prove-retained client "origin_ready" anchor request)))
                      (ok (= 184 (getf proof :end)))
                      (ok (= (parse-integer (eighth row)) (getf proof :end-digest))))
                    (ok (physical-client-test-protocol-error-p
                         (lambda () (pg::pg-client-native-prove-retained
                                     client "origin_ready" anchor request))))))
              (lambda (out)
                (physical-client-test-result out schema
                                             (case mode (:extra (list row row)) (:missing nil)
                                                   (otherwise (list row)))
                                             "PROVE_RETAINED")))))))))

(defun call-with-origin-proof-source (function &key (end 184) (seed #xCBF29CE484222325))
  (call-with-historical-follow-source
   (lambda (root reference prefix eor)
     (let* ((pg::*backend* (pg::make-backend :pid 999 :thread sb-thread:*current-thread*
                                             :replication-mode :physical))
            (anchor (historical-follow-request reference prefix eor end)))
       (setf (getf anchor :base) end
             (getf anchor :manifest-digest) (1+ (getf reference :manifest-digest))
             (getf anchor :receive-digest) seed)
       (let* ((pin (pg::physical-source-follow-prepare
                    "origin_ready" (follow-anchor-receiver-request anchor)))
              (request (origin-proof-test-request reference end))
              (command (pg::parse-replication-command
                        (pg::physical-origin-proof-query
                         "origin_ready" (pg::physical-follow-anchor-digest
                                         (pg::physical-follow-pin-anchor pin)) request))))
         (funcall function root reference prefix pin command))))))

(defun origin-proof-test-advance-source (mode)
  (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
         (start (second fields))
         (snapshot (copy-tree (third fields)))
         (checkpoint (first fields))
         (path (pg::wal-segment-path 0))
         (prefix (lineage-test-bytes path))
         (raw nil))
    (if (eq mode :checkpoint)
        (progn
          (setf (getf snapshot :start) start
                (getf snapshot :redo) start
                (getf snapshot :end) (+ start (pg::wal-checkpoint-record-size snapshot))
                checkpoint (getf snapshot :end))
          (setf raw (pg::wal-encode-record
                     pg::+wal-record-checkpoint+ 0 start (pg::wal-encode-checkpoint-snapshot snapshot))))
        (setf raw (pg::wal-encode-record
                   pg::+wal-record-maintenance+ 0 start
                   (make-array 0 :element-type '(unsigned-byte 8)))))
    (pg::wal-atomic-write-bytes path (concatenate '(vector (unsigned-byte 8)) prefix raw))
    (pg::wal-atomic-write-bytes
     (pg::wal-control-path)
     (pg::wal-encode-control-image checkpoint (+ start (length raw)) snapshot
                                   :timeline (fourth fields) :checkpoint-timeline (fifth fields)
                                   :namespace (sixth fields) :metadata (seventh fields)))
    (setf pg::*wal-durable-lsn* (+ start (length raw)))))

(deftest origin-proof-run-allows-source-flush-and-checkpoint-progress
    (dolist (mode '(:flush :checkpoint))
      (call-with-origin-proof-source
       (lambda (root reference prefix pin command)
         (declare (ignore root reference pin))
         (let* ((before (multiple-value-list (pg::wal-read-control-fields)))
                (pg::*physical-origin-proof-test-hook*
                  (lambda (point value)
                    (declare (ignore value))
                    (when (eq point :after-lease) (origin-proof-test-advance-source mode))))
                (proof (pg::physical-origin-proof-run command))
                (after (multiple-value-list (pg::wal-read-control-fields))))
           (ok (= (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 152 184))
                  (getf proof :end-digest)))
           (ok (< (second before) (second after)))
           (ok (if (eq mode :checkpoint) (< (first before) (first after))
                   (= (first before) (first after))))
           (ok (zerop (hash-table-count pg::*physical-source-senders*))))))))

(deftest origin-proof-run-rejects-replaced-source-context-and-cleans-claims
    (dolist (kind '(:pin :incarnation :namespace :history :reference))
      (call-with-origin-proof-source
       (lambda (root reference prefix pin command)
         (declare (ignore root reference prefix))
         (let* ((control (lineage-test-bytes (pg::wal-control-path)))
                (history-path (pg::timeline-history-path 2 pg::*pgdata*))
                (history (lineage-test-bytes history-path))
                (incarnation pg::*wal-storage-incarnation*)
                (namespace pg::*wal-segment-namespace*)
                (roots (hash-table-count pg::*physical-retained-root-references*))
                (pg::*physical-origin-proof-test-hook*
                  (lambda (point value)
                    (when (eq point :before-final-check)
                      (case kind
                        (:pin (setf (gethash "origin_ready" pg::*physical-source-pins*)
                                    (pg::copy-physical-source-pin pin)))
                        (:incarnation (setf pg::*wal-storage-incarnation* (gensym)))
                        (:namespace (setf pg::*wal-segment-namespace* :legacy))
                        (:history (pg::wal-atomic-write-bytes
                                   history-path
                                   (pg::string->bytes
                                    (format nil "~a revised~%"
                                            (string-right-trim '(#\Newline) (pg::bytes->string history))))))
                        (:reference
                         (let ((fields (copy-tree (getf value :fields))))
                           (incf (getf (getf (seventh fields) :retained-reference) :root-id))
                           (pg::wal-atomic-write-bytes
                            (pg::wal-control-path)
                            (pg::wal-encode-control-image
                             (first fields) (second fields) (third fields) :timeline (fourth fields)
                             :checkpoint-timeline (fifth fields) :namespace (sixth fields)
                             :metadata (seventh fields))))))))))
           (unwind-protect
                (progn
                  (ok (intake-fails-p (lambda () (pg::physical-origin-proof-run command))))
                  (ok (zerop (hash-table-count pg::*physical-source-senders*)))
                  (ok (= roots (hash-table-count pg::*physical-retained-root-references*)))
                  (ok (not pg::*wal-poisoned*)))
             (setf pg::*wal-storage-incarnation* incarnation
                   pg::*wal-segment-namespace* namespace
                   (gethash "origin_ready" pg::*physical-source-pins*) pin)
             (pg::wal-atomic-write-bytes (pg::wal-control-path) control)
             (pg::wal-atomic-write-bytes history-path history)))))))

(deftest origin-proof-run-claims-the-canonical-base-and-keeps-the-ready-pin
    (call-with-origin-proof-source
     (lambda (root reference prefix pin command)
       (declare (ignore root reference))
       (let* ((before (lineage-test-bytes (pg::physical-source-pin-path "origin_ready")))
              (roots (hash-table-count pg::*physical-retained-root-references*))
              (observed nil)
              (pg::*physical-origin-proof-test-hook*
                (lambda (point value)
                  (declare (ignore value))
                  (when (eq point :after-lease)
                    (setf observed
                          (and (plusp pg::*live-operation-depth*)
                               (zerop (hash-table-count pg::*wal-sender-streams*))
                               (= 1 (hash-table-count pg::*physical-source-senders*))
                               (> (hash-table-count pg::*physical-retained-root-references*) roots)
                               (not (sb-thread:holding-mutex-p pg::*wal-lock*))))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-source-follow-release
                                     "origin_ready" (pg::physical-follow-anchor-digest
                                                     (pg::physical-follow-pin-anchor pin))))))))))
         (let ((proof (pg::physical-origin-proof-run command)))
           (ok observed)
           (ok (= 184 (getf proof :end)))
           (ok (= (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 152 184))
                  (getf proof :end-digest)))
           (ok (/= #xCBF29CE484222325 (getf proof :end-digest))))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (ok (= roots (hash-table-count pg::*physical-retained-root-references*)))
         (ok (eq pin (gethash "origin_ready" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path "origin_ready"))))))))

(deftest origin-proof-run-unwinds-cancel-and-nonlocal-exit-without-stream-registration
    (call-with-origin-proof-source
     (lambda (root reference prefix pin command)
       (declare (ignore root reference prefix))
       (let* ((roots (hash-table-count pg::*physical-retained-root-references*))
              (clean (lambda ()
                       (and (zerop (hash-table-count pg::*physical-source-senders*))
                            (= roots (hash-table-count pg::*physical-retained-root-references*))
                            (pg::physical-follow-pin-ready-p pin)))))
         (dolist (point '(:after-claim :after-lease :before-final-check))
           (let ((pg::*physical-origin-proof-test-hook*
                   (lambda (found value)
                     (declare (ignore value))
                     (when (eq point found) (throw 'origin-proof-cut :left)))))
             (ok (eq :left (catch 'origin-proof-cut (pg::physical-origin-proof-run command)))))
           (ok (funcall clean)))
         (let ((pg::*physical-origin-proof-test-hook*
                 (lambda (point value)
                   (declare (ignore value))
                   (when (eq point :after-lease)
                     (setf (pg::backend-cancel-pending pg::*backend*) t)))))
           (ok (equal "57014" (handler-case (progn (pg::physical-origin-proof-run command) nil)
                                (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))
         (ok (funcall clean))))))

(deftest origin-proof-record-scan-preserves-cancel-and-fatal-termination
    (dolist (mode '(:cancel :terminate))
      (call-with-origin-proof-source
       (lambda (root reference prefix pin command)
         (declare (ignore root reference prefix))
         (let* ((roots (hash-table-count pg::*physical-retained-root-references*))
                (observed nil)
                (pg::*physical-origin-proof-test-hook*
                  (lambda (point value)
                    (declare (ignore value))
                    (when (eq point :before-record)
                      (setf observed (plusp pg::*live-operation-depth*))
                      (if (eq mode :cancel)
                          (setf (pg::backend-cancel-pending pg::*backend*) t)
                          (setf (pg::backend-termination-pending pg::*backend*) t)))))
                (condition (handler-case (progn (pg::physical-origin-proof-run command) nil)
                             (pg::pg-error (error) error))))
           (ok observed)
           (ok condition)
           (ok (equal (if (eq mode :cancel) "57014" "57P01")
                      (pg::pg-error-sqlstate condition)))
           (ok (eq (eq mode :terminate) (not (null (pg::fatal-error-p condition)))))
           (ok (zerop (hash-table-count pg::*physical-source-senders*)))
           (ok (= roots (hash-table-count pg::*physical-retained-root-references*))))
         (ok (pg::physical-follow-pin-ready-p pin))))))

(deftest origin-proof-run-rejects-forged-carried-seed
    (call-with-origin-proof-source
     (lambda (root reference prefix pin command)
       (declare (ignore root reference prefix pin))
       (ok (intake-fails-p (lambda () (pg::physical-origin-proof-run command))))
       (ok (zerop (hash-table-count pg::*physical-source-senders*))))
     :seed 91))

(cl-spec:defproperty origin-proof-query-preserves-unsigned-coordinates
    ((system (range integer 0 18446744073709551615))
     (timeline (range integer 1 4294967295)) (base (range integer 32 1000000))
     (extra (range integer 0 1000000)) (manifest (range integer 0 18446744073709551615))
     (anchor (range integer 0 18446744073709551615)))
  (:about pg::physical-origin-proof-query)
  (:trials (:normal 100))
  (let* ((request (list :system-id system :base-timeline timeline :base base
                        :manifest-digest manifest :end (+ base extra)))
         (command (pg::parse-replication-command
                   (pg::physical-origin-proof-query "origin_proof" anchor request))))
    (and (eq :physical-prove (pg::logical-replication-command-type command))
         (equal "origin_proof" (pg::logical-replication-command-slot-name command))
         (= anchor (pg::physical-stream-u64 (pg::physical-stream-option command "anchor_digest")))
         (equal request (pg::physical-origin-proof-command-request command)))))

(deftest generated-origin-proof-query-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'origin-proof-query-preserves-unsigned-coordinates :seed 74))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest origin-proof-query-requires-exact-native-negotiation-and-transaction-context
    (let* ((query (pg::physical-origin-proof-query
                   "origin_proof" 7 '(:system-id 9 :base-timeline 1 :base 32 :manifest-digest 5 :end 64)))
           (command (pg::parse-replication-command query)))
      (ok (pg::replication-query-payload-p (pg::string->bytes query)))
      (dolist (bad
               (list "PROVE_RETAINED SLOT origin_proof"
                     "PROVE_RETAINED SLOT origin_proof (native_format 'bad')"
                     (concatenate 'string (subseq query 0 (1- (length query))) ", anchor_digest '7')")
                     (concatenate 'string (subseq query 0 (1- (length query))) ", unexpected '7')")))
        (ok (intake-fails-p (lambda () (pg::parse-replication-command bad)))))
      (let ((pg::*backend* (pg::make-backend :pid 999 :thread sb-thread:*current-thread*
                                             :replication-mode :physical :explicit t))
            (pg::*physical-recovery-state* nil)
            (pg::*physical-recovery-failed-p* nil))
        (ok (equal "25001" (handler-case
                               (progn (pg::validate-replication-command-context command) nil)
                             (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))))

(deftest origin-proof-hashes-original-base-to-exact-boundary
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root eor))
       (let ((fields (multiple-value-list (pg::wal-read-control-fields)))
             (history (pg::timeline-read-history 2))
             (base (getf reference :base)))
         (dolist (end (list base (+ base 32) (getf reference :end)))
           (ok (= (pg::physical-store-update-digest
                   #xCBF29CE484222325 (subseq prefix base end))
                  (pg::physical-origin-proof-hash-root
                   fields history (origin-proof-test-request reference end)))))
         (dolist (end (list (1- base) (1+ base) (+ base 31) (+ base 33)
                            (1+ (getf reference :end))))
           (ok (intake-fails-p
                (lambda ()
                  (pg::physical-origin-proof-hash-root
                   fields history (origin-proof-test-request reference end))))))))))

(deftest origin-proof-refuses-foreign-identity-and-valid-looking-orphan-roots
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root prefix eor))
       (let ((fields (multiple-value-list (pg::wal-read-control-fields)))
             (history (pg::timeline-read-history 2))
             (request (origin-proof-test-request reference (getf reference :end))))
         (dolist (key '(:system-id :base-timeline :base :manifest-digest))
           (let ((wrong (copy-list request)))
             (incf (getf wrong key))
             (ok (intake-fails-p
                  (lambda () (pg::physical-origin-proof-hash-root fields history wrong))))))
         (let ((no-root (copy-tree fields)))
           (remf (seventh no-root) :retained-reference)
           (ok (intake-fails-p
                (lambda () (pg::physical-origin-proof-hash-root no-root history request)))))))))

(deftest origin-proof-validates-selected-objects-after-the-requested-endpoint
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root prefix eor))
       (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
              (history (pg::timeline-read-history 2))
              (identity (pg::physical-promotion-retained-identity
                         (pg::catalog-system-identifier) reference))
              (directory (pg::physical-recovery-retained-directory))
              (index (pg::physical-retained-inventory-read-index
                      directory (getf reference :root-id) identity
                      (nth-value 2 (pg::physical-promotion-retained-parent-history history))))
              (entry (first (getf index :entries)))
              (path (pg::physical-retained-object-path directory :extent (first entry)))
              (before (lineage-test-bytes path)))
         (setf (aref before (1- (length before)))
               (logxor 1 (aref before (1- (length before)))))
         (pg::wal-atomic-write-bytes path before)
         (ok (intake-fails-p
              (lambda ()
                (pg::physical-origin-proof-hash-root
                 fields history (origin-proof-test-request reference (getf reference :base))))))
         (ok (equalp before (lineage-test-bytes path)))))))
