(in-package #:pg-physical-tests)

(defun follow-anchor-request (seed overlap)
  (list :version 1 :system-id 42 :base-timeline 1 :base 64 :manifest-digest 77
        :source-timeline 2 :receive-timeline 1 :receive-lsn 96 :receive-digest seed
        :overlap-start 0 :overlap-digest overlap
        :history-text (format nil "1~c0/00000060~cpromotion~%" #\Tab #\Tab)
        :eor-octets (coerce (pg::wal-encode-end-of-recovery-record 96 42 2 1 1) 'list)))

(defun follow-anchor-pin (seed overlap &optional ready)
  (pg::make-physical-follow-pin
   :name "sibling" :system-id 42 :timeline 2 :manifest-digest 77
   :consistent-lsn 96 :restart-lsn (if ready 96 0)
   :confirmed-lsn (when ready 96) :prefix-digest seed
   :stage (if ready :ready :retaining) :anchor (follow-anchor-request seed overlap)))

(deftest follow-anchor-format-keeps-carried-seed-separate-from-overlap
    (let* ((pin (follow-anchor-pin 123 456))
           (loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
      (ok (pg::physical-follow-pin-p loaded))
      (ok (eq :retaining (pg::physical-follow-pin-stage loaded)))
      (ok (null (pg::physical-source-pin-confirmed-lsn loaded)))
      (ok (= 123 (pg::physical-source-pin-prefix-digest loaded)))
      (ok (= 456 (getf (pg::physical-follow-pin-anchor loaded) :overlap-digest)))
      (ok (equal (pg::physical-follow-pin-anchor pin) (pg::physical-follow-pin-anchor loaded)))))

(deftest follow-anchor-ready-format-preserves-advanced-confirmation
    (let ((pin (follow-anchor-pin 123 456 t)))
      (setf (pg::physical-source-pin-confirmed-lsn pin) 512
            (pg::physical-source-pin-restart-lsn pin) 512
            (pg::physical-source-pin-prefix-digest pin) 999)
      (let ((loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
        (ok (eq :ready (pg::physical-follow-pin-stage loaded)))
        (ok (= 512 (pg::physical-source-pin-confirmed-lsn loaded)))
        (ok (= 999 (pg::physical-source-pin-prefix-digest loaded)))
        (ok (= 123 (getf (pg::physical-follow-pin-anchor loaded) :receive-digest))))))

(deftest follow-anchor-format-rejects-inconsistent-phase-and-progress
    (dolist (change (list (lambda (pin) (setf (pg::physical-source-pin-confirmed-lsn pin) 96))
                          (lambda (pin) (setf (pg::physical-source-pin-prefix-digest pin) 0))
                          (lambda (pin) (setf (pg::physical-source-pin-timeline pin) 3))
                          (lambda (pin) (setf (pg::physical-source-pin-manifest-digest pin) 78))
                          (lambda (pin) (setf (pg::physical-follow-pin-stage pin) :unknown))))
      (let ((pin (follow-anchor-pin 123 456)))
        (funcall change pin)
        (ok (signals (pg::physical-follow-pin-encode pin) 'error)))))

(deftest follow-anchor-request-rejects-empty-or-foreign-overlap
    (dolist (change
             (list (lambda (request) (setf (getf request :overlap-start) 96))
                   (lambda (request) (setf (getf request :receive-lsn) 97))
                   (lambda (request) (setf (getf request :base) 97))
                   (lambda (request) (setf (getf request :receive-timeline) 2))
                   (lambda (request) (setf (getf request :source-timeline) 3))
                   (lambda (request) (setf (getf request :eor-octets) '(0)))
                   (lambda (request) (setf (getf request :version) 2))))
      (let ((request (follow-anchor-request 123 456)))
        (funcall change request)
        (ok (signals (pg::physical-follow-anchor-check request) 'error)))))

(cl-spec:defproperty follow-anchor-format-roundtrips-independent-proof-fields
    ((seed (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615))
     (ready boolean))
  (:about pg::physical-follow-pin-encode)
  (:trials (:normal 200))
  (let* ((pin (follow-anchor-pin seed overlap ready))
         (loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
    (and (eql (pg::physical-follow-pin-stage pin) (pg::physical-follow-pin-stage loaded))
         (= seed (pg::physical-source-pin-prefix-digest loaded))
         (= overlap (getf (pg::physical-follow-pin-anchor loaded) :overlap-digest))
         (equal (pg::physical-follow-pin-anchor pin) (pg::physical-follow-pin-anchor loaded)))))

(deftest generated-follow-anchor-proof-fields
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-anchor-format-roundtrips-independent-proof-fields :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun call-with-follow-anchor-source (function)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root) (pg::*catalog* (pg::make-catalog))
                               (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                               (pg::*physical-source-pins* (make-hash-table :test #'equal))
                               (pg::*physical-source-senders* (make-hash-table :test #'equal))
                               (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                               (pg::*wal-insert-timeline* 2) (pg::*wal-checkpoint-timeline* 2)
                               (pg::*wal-segment-namespace* :timeline) (pg::*wal-poisoned* nil)
                               (pg::*pm-requested-mode* pg::+no-shutdown+) (pg::*max-slot-wal-keep-bytes* nil)
                               (prefix (concatenate '(vector (unsigned-byte 8))
                                                    (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)))
                               (eor (pg::wal-encode-end-of-recovery-record 64 42 2 1 1))
                               (pg::*wal-durable-lsn* (+ 64 (length eor)))
                               (seed (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 32)))
                               (request (follow-anchor-request seed (pg::physical-store-update-digest #xCBF29CE484222325 prefix))))
      ;; These are decoder/retention fixtures, not a complete recoverable cluster.
      (setf (getf request :system-id) (pg::catalog-system-identifier)
            (getf request :base) 32 (getf request :receive-lsn) 64
            (getf request :history-text) (format nil "1~c0/00000040~cpromotion~%" #\Tab #\Tab)
            (getf request :eor-octets) (coerce eor 'list))
      (pg::wal-ensure-directory-durable (pg::wal-directory))
      (pg::wal-atomic-write-bytes
       (pg::wal-segment-path 0) (concatenate '(vector (unsigned-byte 8)) prefix eor))
      (pg::timeline-write-history 2 1 64 "promotion")
      (funcall function root request eor))))

(defun follow-anchor-receiver-request (request)
  (loop for (key value) on request by #'cddr
        unless (member key '(:history-text :eor-octets))
          append (list key value)))

(defun follow-prepare-query (request &optional (name "wire_prepare"))
  (format nil "PREPARE_FOLLOW SLOT ~a (native_format '~a', system_id '~d', base_timeline '~d', base_lsn '~a', manifest_digest '~d', source_timeline '~d', receive_timeline '~d', receive_lsn '~a', prefix_digest '~d', overlap_start '~a', overlap_digest '~d')"
          name pg::+physical-native-format+ (getf request :system-id)
          (getf request :base-timeline) (pg::wal-format-lsn (getf request :base))
          (getf request :manifest-digest) (getf request :source-timeline)
          (getf request :receive-timeline) (pg::wal-format-lsn (getf request :receive-lsn))
          (getf request :receive-digest) (pg::wal-format-lsn (getf request :overlap-start))
          (getf request :overlap-digest)))

(deftest follow-prepare-wire-parser-preserves-receiver-coordinates
    (let* ((request (follow-anchor-request 18446744073709551615 456))
           (command (pg::parse-replication-command (follow-prepare-query request))))
      (ok (eq :physical-prepare (pg::logical-replication-command-type command)))
      (ok (equal "wire_prepare" (pg::logical-replication-command-slot-name command)))
      (ok (equal (follow-anchor-receiver-request request)
                 (pg::physical-follow-prepare-request command)))
      (ok (pg::replication-query-payload-p
           (pg::string->bytes (concatenate 'string (follow-prepare-query request) (string (code-char 0))))))))

(deftest follow-prepare-wire-obeys-transaction-and-recovery-context
    (let ((pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*backend* (pg::make-backend :explicit t :state :in-progress))
          (command (pg::parse-replication-command
                    (follow-prepare-query (follow-anchor-request 123 456)))))
      (ok (equal "25001" (recovery-state-of
                          (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-explicit pg::*backend*) nil
            pg::*physical-recovery-state* (list :phase 2))
      (ok (equal "25006" (recovery-state-of
                          (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-state pg::*backend*) :aborted)
      (ok (equal "25P02" (recovery-state-of
                          (lambda () (pg::validate-replication-command-context command)))))))

(deftest follow-prepare-wire-result-requires-durable-ready-and-returns-anchor
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((command (pg::parse-replication-command (follow-prepare-query request)))
              (row (pg::physical-follow-prepare-row command))
              (pin (gethash "wire_prepare" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (pg::physical-follow-pin-ready-p
              (pg::physical-follow-pin-decode
               (lineage-test-bytes (pg::physical-source-pin-path "wire_prepare")))))
         (ok (equal (list "wire_prepare" (format nil "~d" (getf request :system-id))
                          "1" "0/00000020" "77" "2" "1" "0/00000040"
                          (format nil "~d" (getf request :receive-digest))
                          "0/00000000" (format nil "~d" (getf request :overlap-digest))
                          (getf request :history-text) "2" "0/00000040"
                          (format nil "~(~{~2,'0X~}~)" (coerce eor 'list)))
                    row))))))

(deftest follow-prepare-wire-parser-rejects-extra-missing-and-invalid-options
    (let ((query (follow-prepare-query (follow-anchor-request 123 456))))
      (dolist (case
                  (list (list "native_format '[^']+'" "native_format 'wrong'" "0A000")
                        (list ", system_id '[^']+'" "" "0A000")
                        (list "source_timeline '2'" "source_timeline '0'" "22023")
                        (list "receive_timeline '1'" "receive_timeline '4294967296'" "22023")
                        (list "prefix_digest '123'" "prefix_digest '-1'" "22023")
                        (list "prefix_digest '123'" "prefix_digest '18446744073709551616'" "22023")
                        (list "base_lsn '[^']+'" "base_lsn 'bad'" "22P02")
                        (list "\\)$" ", system_id '42')" "0A000")
                        (list "\\)$" ", history 'forged')" "0A000")
                        (list "\\)$" ", eor_hex '00')" "0A000")))
        (destructuring-bind (pattern replacement state) case
          (ok (equal state
                     (handler-case
                         (progn (pg::parse-replication-command
                                 (cl-ppcre:regex-replace pattern query replacement)) nil)
                       (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))))))

(deftest follow-prepare-wire-does-not-return-a-row-after-failed-proof
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (incf (getf request :overlap-digest))
       (let ((command (pg::parse-replication-command (follow-prepare-query request))))
         (ok (equal "55000"
                    (handler-case (progn (pg::physical-follow-prepare-row command) nil)
                      (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))
         (let ((pin (gethash "wire_prepare" pg::*physical-source-pins*)))
           (ok pin)
           (ng (pg::physical-follow-pin-ready-p pin))
           (ng (pg::physical-source-pin-confirmed-lsn pin)))))))

(deftest follow-prepare-wire-returns-no-row-on-ambiguous-ready-publication
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*physical-source-test-hook*
               (lambda (point pin)
                 (when (and (eq point :after-save)
                            (pg::physical-follow-pin-ready-p pin))
                   (error "ambiguous ready publication")))))
         (ok (equal "XX000"
                    (recovery-state-of
                     (lambda () (pg::physical-follow-prepare-row
                                 (pg::parse-replication-command (follow-prepare-query request)))))))
         (ok pg::*wal-poisoned*)
         (ng (pg::physical-follow-pin-ready-p
              (gethash "wire_prepare" pg::*physical-source-pins*)))))))

(deftest follow-prepare-wire-reports-unavailable-source-eor-as-object-state
    (dolist (kind '(:missing :truncated :corrupt))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root))
         (let ((path (pg::wal-segment-path 0)))
           (ecase kind
             (:missing (delete-file path))
             (:truncated (pg::wal-atomic-write-bytes
                          path (make-array (+ 64 (1- (length eor)))
                                           :element-type '(unsigned-byte 8) :initial-element 0)))
             (:corrupt (pg::wal-atomic-write-bytes
                        path (make-array (+ 64 (length eor))
                                         :element-type '(unsigned-byte 8) :initial-element 0)))))
         (ok (equal "55000"
                    (recovery-state-of
                     (lambda () (pg::physical-follow-prepare-row
                                 (pg::parse-replication-command (follow-prepare-query request)))))))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ng pg::*wal-poisoned*)))))

(deftest follow-preparation-builds-source-owned-history-and-eor
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((receiver (follow-anchor-receiver-request request))
              (pin (pg::physical-source-follow-prepare "prepared" receiver)))
         (ok (equal request (pg::physical-follow-pin-anchor pin)))
         (ok (equal (coerce eor 'list)
                    (getf (pg::physical-follow-pin-anchor pin) :eor-octets)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (eq pin (pg::physical-source-follow-prepare "prepared" receiver)))
         (ok (pg::physical-follow-pin-ready-p
              (pg::physical-follow-pin-decode
               (pg::physical-follow-pin-encode pin)))))
       (ok (= 1 (hash-table-count pg::*physical-source-pins*))))))

(deftest follow-preparation-selects-immediate-successor-with-fresh-pins
    (call-with-follow-anchor-source
     (lambda (root request first-eor)
       (declare (ignore root))
       (let* ((fork pg::*wal-durable-lsn*)
              (last-eor (pg::wal-encode-end-of-recovery-record fork 43 3 2 1))
              (prefix (lineage-test-bytes (pg::wal-segment-path 0)))
              (pg::*wal-insert-timeline* 3)
              (pg::*wal-checkpoint-timeline* 3)
              (pg::*wal-durable-lsn* (+ fork (length last-eor))))
         (pg::timeline-write-history 3 2 fork "second promotion")
         (pg::wal-atomic-write-bytes
          (pg::wal-segment-path 0)
          (concatenate '(vector (unsigned-byte 8)) prefix last-eor))
         (let ((receiver (follow-anchor-receiver-request request)))
           (setf (getf receiver :source-timeline) 3)
           (let ((first (pg::physical-source-follow-prepare "first_fork" receiver)))
             (ok (equal (coerce first-eor 'list)
                        (getf (pg::physical-follow-pin-anchor first) :eor-octets))))
           (setf (getf receiver :receive-timeline) 2
                 (getf receiver :receive-lsn) fork
                 (getf receiver :receive-digest)
                 (pg::physical-store-update-digest (getf request :receive-digest) first-eor)
                 (getf receiver :overlap-digest)
                 (pg::physical-store-update-digest #xCBF29CE484222325 prefix))
           (let ((second (pg::physical-source-follow-prepare "second_fork" receiver)))
             (ok (equal (coerce last-eor 'list)
                        (getf (pg::physical-follow-pin-anchor second) :eor-octets))))
           (ok (= 2 (hash-table-count pg::*physical-source-pins*))))))))

(deftest follow-preparation-rejects-client-descriptors-and-foreign-coordinates
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (dolist (change
                (list (lambda (r) (append r (list :history-text "")))
                      (lambda (r) (append r (list :eor-octets nil)))
                      (lambda (r) (setf (getf r :source-timeline) 3) r)
                      (lambda (r) (setf (getf r :receive-timeline) 2) r)
                      (lambda (r) (setf (getf r :receive-lsn) 65) r)
                      (lambda (r) (setf (getf r :system-id) 0) r)
                      (lambda (r) (setf (getf r :receive-lsn) "64") r)))
         (ok (signals
              (pg::physical-source-follow-prepare
               "rejected" (funcall change (follow-anchor-receiver-request request)))
              'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ng (probe-file (pg::physical-source-pin-path "rejected")))))))

(deftest follow-preparation-ready-retry-does-not-reread-released-anchor-bytes
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((receiver (follow-anchor-receiver-request request))
              (pin (pg::physical-source-follow-prepare "retry" receiver))
              (owner (gensym))
              (digest (pg::physical-store-update-digest (getf request :receive-digest) eor)))
         (setf (gethash "retry" pg::*physical-source-senders*) owner)
         (pg::physical-source-confirm-receipt "retry" owner pg::*wal-durable-lsn* digest)
         (remhash "retry" pg::*physical-source-senders*)
         (let ((read-segment (symbol-function 'pg::physical-follow-source-segment)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-source-segment)
                        (lambda (&rest args) (declare (ignore args))
                          (error "released original anchor bytes must not be reread")))
                  (ok (eq pin (pg::physical-source-follow-prepare "retry" receiver)))
                  (ok (= pg::*wal-durable-lsn* (pg::physical-source-pin-confirmed-lsn pin)))
                  (ok (= digest (pg::physical-source-pin-prefix-digest pin)))
                  (setf (getf receiver :receive-digest) (1+ (getf receiver :receive-digest)))
                  (ok (signals (pg::physical-source-follow-prepare "retry" receiver)
                               'pg::wal-input-validation-error)))
             (setf (symbol-function 'pg::physical-follow-source-segment) read-segment)))))))

(deftest follow-preparation-rechecks-concurrently-published-ready-pin
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((receiver (follow-anchor-receiver-request request))
             (construct (symbol-function 'pg::physical-source-follow-anchor))
             (published nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-source-follow-anchor)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (setf published (pg::physical-source-follow-ready "concurrent" request))
                        (error "original EOR was released before delayed descriptor read")))
                (ok (eq (pg::physical-source-follow-prepare "concurrent" receiver) published))
                (ok (pg::physical-follow-pin-ready-p published)))
           (setf (symbol-function 'pg::physical-source-follow-anchor) construct))))))

(deftest follow-preparation-rejects-missing-truncated-or-corrupt-source-eor
    (dolist (kind '(:missing :truncated :corrupt))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root))
         (let ((path (pg::wal-segment-path 0)))
           (ecase kind
             (:missing (delete-file path))
             (:truncated (pg::wal-atomic-write-bytes
                          path (make-array (+ 64 (1- (length eor)))
                                           :element-type '(unsigned-byte 8) :initial-element 0)))
             (:corrupt (pg::wal-atomic-write-bytes
                        path (make-array (+ 64 (length eor))
                                         :element-type '(unsigned-byte 8) :initial-element 0)))))
         (ok (signals (pg::physical-source-follow-prepare
                       "bad_eor" (follow-anchor-receiver-request request)) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ng (probe-file (pg::physical-source-pin-path "bad_eor")))))))

(deftest follow-anchor-source-publishes-proof-before-ready-and-reloads
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pin (pg::physical-source-follow-reserve "sibling" request)))
         (ok (eq :retaining (pg::physical-follow-pin-stage pin)))
         (ng (pg::physical-source-pin-stream-ready-p pin))
         (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ok (eq pin (pg::physical-source-follow-ready "sibling" request)))
         (ok (pg::physical-source-pin-stream-ready-p pin))
         (ok (= 64 (gethash pin pg::*wal-snapshot-retention-pins*))))
       (clrhash pg::*physical-source-pins*)
       (clrhash pg::*wal-snapshot-retention-pins*)
       (pg::physical-source-pin-load)
       (let ((loaded (gethash "sibling" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p loaded))
         (ok (= 64 (pg::physical-source-pin-confirmed-lsn loaded)))
         (ok (equal request (pg::physical-follow-pin-anchor loaded)))))))

(deftest follow-anchor-bad-overlap-keeps-retention-without-granting-stream
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (setf (getf request :overlap-digest) (logxor 1 (getf request :overlap-digest)))
       (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
       (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (eq :retaining (pg::physical-follow-pin-stage pin)))
         (ng (pg::physical-source-pin-stream-ready-p pin))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*))))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-ready-retry-preserves-confirmed-digest-and-rejects-rebinding
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((pin (pg::physical-source-follow-ready "sibling" request))
              (owner (gensym "FOLLOW-SENDER"))
              (next (pg::physical-store-update-digest (getf request :receive-digest) eor))
              (proof (symbol-function 'pg::physical-follow-pin-proof-digest)))
         (setf (gethash "sibling" pg::*physical-source-senders*) owner)
         (pg::physical-source-confirm-receipt "sibling" owner pg::*wal-durable-lsn* next)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-pin-proof-digest)
                      (lambda (ignored) (declare (ignore ignored)) (error "old overlap was reread")))
                (ok (eq pin (pg::physical-source-follow-ready "sibling" request)))
                (ok (= pg::*wal-durable-lsn* (pg::physical-source-pin-confirmed-lsn pin))))
           (setf (symbol-function 'pg::physical-follow-pin-proof-digest) proof))
         (ok (= next (pg::physical-source-pin-prefix-digest pin)))
         (incf (getf request :receive-digest))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (ok (= next (pg::physical-source-pin-prefix-digest pin)))))))

(deftest follow-anchor-reservation-owns-its-name-and-request
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((name (copy-seq "sibling"))
              (original (pg::physical-follow-anchor-copy request))
              (pin (pg::physical-source-follow-reserve name request)))
         (setf (char name 0) #\x
               (char (getf request :history-text) 0) #\9
               (first (getf request :eor-octets)) 0)
         (ok (eq pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (equal "sibling" (pg::physical-source-pin-name pin)))
         (ok (equal original (pg::physical-follow-pin-anchor pin)))))))

(deftest follow-anchor-lost-retention-cannot-be-readied-or-rebound
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((pin (pg::physical-source-follow-reserve "sibling" request))
              (pg::*max-slot-wal-keep-bytes* 1))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::physical-source-invalidate-lagging-pins-under-wal-lock))
         (ok (pg::physical-source-pin-lost-p pin))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (pg::physical-source-pin-load)
         (ok (pg::physical-source-pin-lost-p (gethash "sibling" pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))))))

(deftest follow-anchor-loss-after-proof-prevents-ready-publication
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*max-slot-wal-keep-bytes* 1)
             (pg::*physical-source-test-hook*
               (lambda (point pin)
                 (declare (ignore pin))
                 (when (eq point :after-follow-proof)
                   (sb-thread:with-mutex (pg::*wal-lock*)
                     (pg::physical-source-invalidate-lagging-pins-under-wal-lock))))))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error)))
       (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (pg::physical-source-pin-lost-p pin))
         (ng (pg::physical-follow-pin-ready-p pin))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-cannot-use-bootstrap-stream-admission
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (pg::physical-source-follow-reserve "sibling" request)
       (let ((command
               (pg::parse-replication-command
                (format nil
                        "START_REPLICATION SLOT sibling PHYSICAL 0/00000040 (native_format '~a', system_id '~d', timeline '2', base_lsn '0/00000040', prefix_digest '~d', manifest_digest '77')"
                        pg::+physical-native-format+ (getf request :system-id)
                        (getf request :receive-digest)))))
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (pg::physical-source-follow-ready "sibling" request)
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))))))

(defun follow-anchor-start-command (request &optional mode)
  (pg::parse-replication-command
   (format nil
           "START_REPLICATION SLOT sibling PHYSICAL ~a (native_format '~a', system_id '~d', timeline '2', base_lsn '~a', prefix_digest '~d', manifest_digest '77'~a)"
           (pg::wal-format-lsn (getf request :receive-lsn))
           pg::+physical-native-format+ (getf request :system-id)
           (pg::wal-format-lsn (if (equal mode "follow") (getf request :base) 64))
           (getf request :receive-digest)
           (if mode (format nil ", mode '~a'" mode) ""))))

(deftest follow-anchor-stream-requires-explicit-ready-mode-and-exclusive-owner
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((pin (pg::physical-source-follow-reserve "sibling" request))
              (command (follow-anchor-start-command request "follow")))
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (pg::physical-source-follow-ready "sibling" request)
         (let ((owner (pg::physical-source-acquire-sender command)))
           (unwind-protect
                (progn
                  (ok (eq owner (gethash "sibling" pg::*physical-source-senders*)))
                  (ok (signals (pg::physical-source-acquire-sender command) 'error))
                  (ok (= 64 (pg::physical-source-validate-resume command owner)))
                  (ok (= 64 (pg::physical-source-pin-confirmed-lsn pin)))
                  (multiple-value-bind (record raw durable)
                      (pg::physical-source-read-next "sibling" owner 64)
                    (ok (= pg::+wal-record-end-of-recovery+ (pg::wal-record-type record)))
                    (ok (equalp raw eor))
                    (ok (= 120 durable))))
             (pg::physical-source-release-sender "sibling" owner)))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))))))

(deftest follow-stream-timeline-is-requested-ancestry-with-an-exact-fork-limit
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (pg::physical-source-follow-ready "sibling" request)
       (let ((command (follow-anchor-start-command request "follow")))
         (setf (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "1")
         (multiple-value-bind (owner limit next) (pg::physical-source-acquire-sender command)
           (unwind-protect (progn (ok (= 64 limit)) (ok (= 2 next)))
             (pg::physical-source-release-sender "sibling" owner)))
         (setf (pg::logical-replication-command-requested-lsn command) 65)
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (setf (pg::logical-replication-command-requested-lsn command) 64
               (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "3")
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))))))

(cl-spec:defproperty follow-stream-boundaries-preserve-c-parent-limit-and-child-prefix
    ((fork (range integer 16777216 18446744073709551614))
     (child (range integer 2 4294967295))
     (case-number (range integer 0 5)))
  (:about pg::physical-follow-stream-boundary)
  (:trials (:normal 200))
  (let* ((parent-request (< case-number 3))
         (origin (* (floor fork pg::+wal-segment-size+) pg::+wal-segment-size+))
         (cursor (ecase case-number
                   (0 (1- fork)) (1 fork) (2 (1+ fork))
                   (3 fork) (4 origin) (5 (1- origin))))
         (accepted (member case-number '(0 1 3 4)))
         (pin (pg::make-physical-follow-pin
               :timeline child :anchor (list :history-text (store-history-text 1 fork))))
         (command (pg::make-logical-replication-command
                   :requested-lsn cursor
                   :options (list (cons "timeline" (format nil "~d" (if parent-request 1 child)))))))
    (handler-case
        (multiple-value-bind (limit next) (pg::physical-follow-stream-boundary command pin)
          (and accepted (if parent-request (and (= fork limit) (= child next))
                            (and (null limit) (null next)))))
      (pg::pg-error (condition)
        (and (not accepted) (equal "55000" (pg::pg-error-sqlstate condition)))))))

(deftest generated-follow-stream-timeline-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'follow-stream-boundaries-preserve-c-parent-limit-and-child-prefix :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest historical-stream-stops-before-eor-and-reports-next-timeline-without-copy-at-fork
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (setf (getf request :receive-lsn) 32
             (getf request :receive-digest) #xCBF29CE484222325
             (getf request :overlap-digest)
             (pg::physical-store-update-digest #xCBF29CE484222325 (legacy-checkpoint-record 0)))
       (let* ((pin (pg::physical-source-follow-ready "sibling" request))
              (command (follow-anchor-start-command request "follow"))
              (names '(pg::physical-stream-send-data pg::physical-stream-feedback
                       pg::send-copy-both-response pg::physical-stream-switch-result
                       pg::physical-source-read-next))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (reads 0) (sent nil) (switched nil))
         (setf (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "1")
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-stream-send-data)
                      (lambda (stream start end bytes)
                        (declare (ignore stream))
                        (push (list start end bytes) sent))
                      (symbol-function 'pg::physical-stream-feedback)
                      (lambda (stream) (declare (ignore stream))
                        (list :write 64 :flush 64 :apply 32))
                      (symbol-function 'pg::physical-source-read-next)
                      (lambda (name owner cursor)
                        (incf reads)
                        (when (>= cursor 64) (error "historical stream attempted child WAL"))
                        (funcall (cdr (assoc 'pg::physical-source-read-next saved)) name owner cursor)))
                (multiple-value-bind (owner limit next) (pg::physical-source-acquire-sender command)
                  (unwind-protect
                       (progn
                         (ok (= 64 limit)) (ok (= 2 next))
                         (ok (eq :switch (pg::physical-stream-send-loop nil command owner limit))))
                    (pg::physical-source-release-sender "sibling" owner)))
                (ok (= 2 reads))
                (ok (= 1 (length sent)))
                (ok (= 32 (first (first sent))))
                (ok (= 64 (second (first sent))))
                (ok (equalp (third (first sent)) (legacy-checkpoint-record 32)))
                (ok (= 64 (pg::physical-source-pin-confirmed-lsn pin)))
                (setf (pg::logical-replication-command-requested-lsn command) 64
                      (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command)
                                  :test #'equal))
                      (format nil "~d" (pg::physical-source-pin-prefix-digest pin))
                      (symbol-function 'pg::send-copy-both-response)
                      (lambda (stream) (declare (ignore stream)) (error "unexpected CopyBoth at fork"))
                      (symbol-function 'pg::physical-stream-switch-result)
                      (lambda (stream next limit) (declare (ignore stream)) (setf switched (list next limit))))
                (pg::start-native-physical-replication nil command)
                (ok (equal '(2 64) switched))
                (ok (= 2 reads))
                (ok (zerop (hash-table-count pg::*physical-source-senders*))))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))))

(deftest physical-follow-copy-done-during-proof-finishes-without-prefix-confirmation
    (dolist (timeline '("1" "2"))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root eor))
         (setf (getf request :receive-lsn) 32
               (getf request :receive-digest) #xCBF29CE484222325
               (getf request :overlap-digest)
               (pg::physical-store-update-digest #xCBF29CE484222325 (legacy-checkpoint-record 0)))
         (let* ((pin (pg::physical-source-follow-ready "sibling" request))
                (command (follow-anchor-start-command request "follow"))
                (pg::*backend* (pg::make-backend :state :idle))
                (names '(pg::send-copy-both-response pg::physical-stream-feedback
                         pg::physical-stream-send-keepalive pg::physical-stream-send-data
                         pg::write-message pg::physical-stream-switch-result
                         pg::send-command-complete pg::send-ready-for-query))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
                (events nil))
           (setf (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) timeline
                 (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command) :test #'equal)) "0")
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::send-copy-both-response)
                        (lambda (stream) (declare (ignore stream)) (push :copy events))
                        (symbol-function 'pg::physical-stream-feedback)
                        (lambda (stream) (declare (ignore stream)) (values nil t))
                        (symbol-function 'pg::physical-stream-send-keepalive)
                        (lambda (stream end) (declare (ignore stream end)))
                        (symbol-function 'pg::physical-stream-send-data)
                        (lambda (&rest arguments) (declare (ignore arguments)) (error "sent unproved WAL"))
                        (symbol-function 'pg::write-message)
                        (lambda (stream kind bytes)
                          (declare (ignore stream))
                          (ok (zerop (length bytes))) (push kind events))
                        (symbol-function 'pg::physical-stream-switch-result)
                        (lambda (stream next limit)
                          (declare (ignore stream))
                          (ok (zerop (hash-table-count pg::*physical-source-senders*)))
                          (push (list next limit) events))
                        (symbol-function 'pg::send-command-complete)
                        (lambda (stream tag) (declare (ignore stream)) (push tag events))
                        (symbol-function 'pg::send-ready-for-query)
                        (lambda (stream state) (declare (ignore stream))
                          (ok (zerop (hash-table-count pg::*physical-source-senders*)))
                          (push state events)))
                  (pg::start-native-physical-replication nil command)
                  (ok (equal (reverse events)
                             (if (equal timeline "1") '(:copy #\c (2 64))
                                 '(:copy #\c "START_STREAMING" #\I))))
                  (ok (= 32 (pg::physical-source-pin-confirmed-lsn pin)))
                  (ok (= #xCBF29CE484222325 (pg::physical-source-pin-prefix-digest pin))))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))))))))

(deftest follow-stream-mode-cannot-acquire-a-bootstrap-pin
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pin (pg::make-physical-source-pin
                   :name "sibling" :system-id (getf request :system-id) :timeline 2
                   :manifest-digest 77 :consistent-lsn 64 :restart-lsn 32
                   :prefix-digest #xCBF29CE484222325)))
         (setf (gethash "sibling" pg::*physical-source-pins*) pin)
         (ok (signals (pg::physical-source-acquire-sender
                       (follow-anchor-start-command request "follow")) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (let ((owner (pg::physical-source-acquire-sender (follow-anchor-start-command request))))
           (pg::physical-source-release-sender "sibling" owner))))))

(deftest physical-stream-mode-rejects-unknown-and-duplicate-values
    (let ((request (follow-anchor-request 123 456)))
      (dolist (mode '("" "other" "FOLLOW" "bootstrap follow"))
        (ok (signals (follow-anchor-start-command request mode) 'error)))
      (let* ((query (format nil
                            "START_REPLICATION SLOT sibling PHYSICAL 0/40 (native_format '~a', system_id '42', timeline '2', base_lsn '0/40', prefix_digest '123', manifest_digest '77', mode 'follow', mode 'follow')"
                            pg::+physical-native-format+)))
        (ok (signals (pg::parse-replication-command query) 'error)))))

(deftest follow-anchor-unproved-retention-cannot-confirm-receipt
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pin (pg::physical-source-follow-reserve "sibling" request))
             (owner (gensym "UNPROVED-SENDER")))
         (setf (gethash "sibling" pg::*physical-source-senders*) owner)
         (ok (signals (pg::physical-source-confirm-receipt "sibling" owner 64
                                                           (getf request :receive-digest)) 'error))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ng (pg::physical-follow-pin-ready-p pin))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-ambiguous-ready-publication-recovers-from-disk
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*physical-source-test-hook*
               (lambda (point pin)
                 (when (and (eq point :after-save)
                            (eq :ready (pg::physical-follow-pin-stage pin)))
                   (error "injected post-ready-save failure")))))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error)))
       (let ((pin (gethash "sibling" pg::*physical-source-pins*))
             (disk (sb-thread:with-mutex (pg::*wal-lock*)
                     (pg::physical-source-pin-read (pg::physical-source-pin-path "sibling")))))
         (ok pg::*wal-poisoned*)
         (ng (pg::physical-follow-pin-ready-p pin))
         (ok (pg::physical-follow-pin-ready-p disk))
         (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*))))
       ;; A new owning process clears poison and reconstructs the durable registry.
       (clrhash pg::*physical-source-pins*)
       (clrhash pg::*wal-snapshot-retention-pins*)
       (setf pg::*wal-poisoned* nil pg::*pm-requested-mode* pg::+no-shutdown+)
       (pg::physical-source-pin-load)
       (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (= 64 (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (eq pin (pg::physical-source-follow-ready "sibling" request)))))))

(deftest restored-ready-follow-pin-rejects-a-truncated-live-tail
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (pg::physical-source-follow-ready "sibling" request)
       (let* ((path (pg::wal-segment-path 0))
              (bytes (lineage-test-bytes path)))
         (unwind-protect
              (progn
                (pg::wal-atomic-write-bytes path (subseq bytes 0 (1- (length bytes))))
                (ok (signals
                     (sb-thread:with-mutex (pg::*wal-lock*)
                       (pg::physical-source-pin-read (pg::physical-source-pin-path "sibling")))
                     'error)))
           (pg::wal-atomic-write-bytes path bytes))))))

(deftest follow-anchor-envelope-rejects-corruption-and-extra-forms
    (let ((bytes (pg::physical-follow-pin-encode (follow-anchor-pin 123 456))))
      (setf (aref bytes (1- (length bytes))) (logxor 1 (aref bytes (1- (length bytes)))))
      (ok (signals (pg::physical-follow-pin-decode bytes) 'error)))
  (let* ((payload (pg::physical-follow-pin-payload (follow-anchor-pin 123 456)))
         (body (pg::string->bytes
                (concatenate 'string (pg::physical-bootstrap-manifest-text payload) " NIL")))
         (header (pg::string->bytes
                  (format nil "PGPHYSFOLLOW1 ~d ~8,'0X~%" (length body) (pg::wal-crc32 body)))))
    (ok (signals (pg::physical-follow-pin-decode
                  (concatenate '(vector (unsigned-byte 8)) header body)) 'error))))

(deftest follow-anchor-byte-digest-alone-does-not-prove-a-record-end
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((prefix (concatenate '(vector (unsigned-byte 8))
                                  (legacy-checkpoint-record 0) (legacy-checkpoint-record 32))))
         (setf (getf request :receive-lsn) 48
               (getf request :overlap-digest)
               (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 0 48)))
         (ok (signals (pg::physical-source-follow-ready "interior" request) 'error)))
       (let ((pin (gethash "interior" pg::*physical-source-pins*)))
         (ng (pg::physical-follow-pin-ready-p pin))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin))))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-format-distinguishes-base-receipt-and-source-timelines
    (let* ((request (follow-anchor-request 123 456))
           (raw (pg::wal-encode-end-of-recovery-record 160 42 3 2 1)))
      (setf (getf request :source-timeline) 3
            (getf request :receive-timeline) 2 (getf request :receive-lsn) 152
            (getf request :history-text)
            (format nil "1~c0/00000060~cpromotion~%2~c0/000000A0~cpromotion~%"
                    #\Tab #\Tab #\Tab #\Tab)
            (getf request :eor-octets) (coerce raw 'list))
      (multiple-value-bind (history fork record) (pg::physical-follow-anchor-check request)
        (declare (ignore history record))
        (ok (= 160 fork)))
      (let* ((pin (pg::make-physical-follow-pin
                   :name "later" :system-id 42 :timeline 3 :manifest-digest 77
                   :restart-lsn 0 :consistent-lsn 152 :prefix-digest 123 :anchor request))
             (loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
        (ok (= 1 (getf (pg::physical-follow-pin-anchor loaded) :base-timeline)))
        (ok (= 2 (getf (pg::physical-follow-pin-anchor loaded) :receive-timeline)))
        (ok (= 3 (pg::physical-source-pin-timeline loaded))))))

(deftest follow-anchor-ready-proof-does-not-override-terminal-retention-loss
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((pin (pg::physical-source-follow-ready "sibling" request))
              (pg::*max-slot-wal-keep-bytes* 1))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::physical-source-invalidate-lagging-pins-under-wal-lock))
         (ok (eq :ready (pg::physical-follow-pin-stage pin)))
         (ok (pg::physical-source-pin-lost-p pin))
         (ng (pg::physical-source-pin-stream-ready-p pin))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (let ((loaded (pg::physical-source-pin-read (pg::physical-source-pin-path "sibling"))))
           (ok (eq :ready (pg::physical-follow-pin-stage loaded)))
           (ng (pg::physical-source-pin-stream-ready-p loaded)))))))

(deftest follow-anchor-native-eor-placement-crosses-page-offset-and-rotates-whole-segments
    (dolist (fork (list (- 8192 16) (- pg::+wal-segment-size+ 16)))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (pg::*wal-insert-timeline* 2)
                                   (pg::*catalog* (pg::make-catalog))
                                   (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                                   (system-id (pg::catalog-system-identifier))
                                   (pg::*wal-segment-namespace* :timeline)
                                   (start (pg::wal-record-start-after fork pg::+wal-end-of-recovery-record-size+))
                                   (raw (pg::wal-encode-end-of-recovery-record start 42 2 1 1))
                                   (pg::*wal-durable-lsn* (+ start (length raw)))
                                   (request (follow-anchor-request 123 456)))
          (setf (getf request :system-id) system-id
                (getf request :receive-lsn) fork
                (getf request :history-text)
                (format nil "1~c~X/~8,'0X~cpromotion~%" #\Tab
                        (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
                (getf request :eor-octets) (coerce raw 'list))
          (pg::wal-ensure-directory-durable (pg::wal-directory))
          (pg::timeline-write-history 2 1 fork "promotion")
          (with-open-file (out (pg::wal-segment-path (floor start pg::+wal-segment-size+))
                               :direction :output :element-type '(unsigned-byte 8))
            (file-position out (mod start pg::+wal-segment-size+))
            (write-sequence raw out) (pg::fsync-stream out))
          (ok (equal request (pg::physical-source-follow-anchor
                              (follow-anchor-receiver-request request))))
          (let ((pin (pg::make-physical-follow-pin
                      :name "boundary" :system-id system-id :timeline 2 :manifest-digest 77
                      :restart-lsn 0 :consistent-lsn fork :prefix-digest 123 :anchor request)))
            (pg::physical-follow-pin-check-fields pin)
            (ok (not (signals (sb-thread:with-mutex (pg::*wal-lock*)
                                (pg::physical-follow-pin-check-source-eor pin)) 'error))))))))

(deftest follow-anchor-replaced-reservation-cannot-inherit-an-older-proof
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((replacement (pg::physical-follow-anchor-copy request))
             (pg::*physical-source-test-hook*
               (lambda (point pin)
                 (declare (ignore pin))
                 (when (eq point :after-follow-proof)
                   (pg::physical-source-pin-release "sibling")
                   (let ((other (pg::physical-follow-anchor-copy request)))
                     (incf (getf other :manifest-digest))
                     (pg::physical-source-follow-reserve "sibling" other))))))
         (incf (getf replacement :manifest-digest))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
           (ok (equal replacement (pg::physical-follow-pin-anchor pin)))
           (ng (pg::physical-follow-pin-ready-p pin))
           (ok (null (pg::physical-source-pin-confirmed-lsn pin))))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-source-entering-recovery-cannot-publish-ready
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*physical-source-test-hook*
               (lambda (point pin)
                 (declare (ignore pin))
                 (when (eq point :after-follow-proof)
                   (setf pg::*physical-recovery-state* (list :phase 2))))))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error)))
       (ng (pg::physical-follow-pin-ready-p (gethash "sibling" pg::*physical-source-pins*)))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-source-preflight-rejections-do-not-create-a-reservation
    (dolist (change
             (list (lambda (request) (incf (getf request :system-id)))
                   (lambda (request)
                     (setf (getf request :eor-octets)
                           (coerce (pg::wal-encode-end-of-recovery-record 64 43 2 1 1) 'list)))
                   (lambda (request)
                     (setf (getf request :history-text)
                           (format nil "1~c0/00000040~canother reason~%" #\Tab #\Tab)))))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root eor))
         (funcall change request)
         (ok (signals (pg::physical-source-follow-ready "foreign" request) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))
         (ok (not (probe-file (pg::physical-source-pin-path "foreign"))))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-partial-base-file-is-not-a-retained-historical-suffix
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((fork (- pg::+wal-segment-size+ 16))
              (start pg::+wal-segment-size+)
              (raw (pg::wal-encode-end-of-recovery-record start 42 2 1 1))
              (prefix (concatenate '(vector (unsigned-byte 8))
                                   (legacy-checkpoint-record 0) (legacy-checkpoint-record 32))))
         ;; A promoted source may retain only the old backup prefix under TLI 1.
         ;; Its complete TLI-2 copy of segment 0 has already been recycled.
         (delete-file (pg::wal-segment-path 0 2 :timeline))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0 1 :timeline) prefix)
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 1 2 :timeline) raw)
         (setf pg::*wal-durable-lsn* (+ start (length raw))
               (getf request :history-text)
               (format nil "1~c~X/~8,'0X~cpromotion~%" #\Tab
                       (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
               (getf request :eor-octets) (coerce raw 'list))
         (pg::wal-atomic-write-bytes
          (pg::timeline-history-path 2 pg::*pgdata*)
          (pg::string->bytes (getf request :history-text)))
         (ok (signals (pg::physical-source-follow-ready "partial" request) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*))))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-current-file-name-does-not-prove-a-complete-suffix
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((fork (- pg::+wal-segment-size+ 16))
              (start pg::+wal-segment-size+)
              (raw (pg::wal-encode-end-of-recovery-record start 42 2 1 1))
              (prefix (concatenate '(vector (unsigned-byte 8))
                                   (legacy-checkpoint-record 0) (legacy-checkpoint-record 32))))
         ;; Current naming alone cannot justify a gap from 64 to the fork.
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0 2 :timeline) prefix)
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 1 2 :timeline) raw)
         (setf pg::*wal-durable-lsn* (+ start (length raw))
               (getf request :history-text)
               (format nil "1~c~X/~8,'0X~cpromotion~%" #\Tab
                       (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
               (getf request :eor-octets) (coerce raw 'list))
         (pg::wal-atomic-write-bytes
          (pg::timeline-history-path 2 pg::*pgdata*)
          (pg::string->bytes (getf request :history-text)))
         (ok (signals (pg::physical-source-follow-ready "partial" request) 'error))
         (let ((pin (gethash "partial" pg::*physical-source-pins*)))
           (ok pin)
           (ng (pg::physical-follow-pin-ready-p pin))
           (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*)))))
       (ng pg::*wal-poisoned*))))
