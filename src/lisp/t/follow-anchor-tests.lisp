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
