(in-package #:pg)

(defun copy-binding-test-data (generation &optional retained)
  ;; Detached controls test binding only; they do not authorize a source or import.
  (let* ((snapshot (list :version 2 :start 256 :end 512 :redo 256 :time 0
                         :timeline 1 :prev-timeline 1 :next-xid 3 :xid-reserved-through 1024
                         :next-oid 10 :oldest-active-xid 0 :logical-decoding-enabled t
                         :logical-on-start-lsn 65 :logical-on-start-timeline 1))
         (metadata (make-wal-control-metadata 42 +db-in-production+ 0))
         (base-metadata nil)
         (id (logical-slot-generation-token generation)))
    (setf (getf snapshot :end) (+ 256 (wal-checkpoint-record-size snapshot)))
    (when retained
      (setf metadata (append metadata '(:retained-reference
                                        (:root-id 9 :base-timeline 1 :base 64 :manifest-digest 0
                                         :end 200 :end-digest 0)))))
    (setf base-metadata (copy-list metadata))
    (remf base-metadata :retained-reference)
    (let* ((end (getf snapshot :end))
           (source (wal-encode-control-image end end snapshot :metadata metadata))
           (base (wal-encode-control-image end end snapshot :metadata base-metadata))
           (recovery (physical-recovery-control-image 0 42 1 0 end end 3 :state-time 0))
           (manifest (list :version 3 :system-id 42 :cluster-identity "binding-fixture"
                           :timeline 1 :checkpoint end :checkpoint-start 256 :end end :next-xid 3
                           :source-pin "copy_pin"
                           :files (list (list "PG_VERSION" 3 0)
                                        (list "global/pg_wal_control" (length base)
                                              (physical-store-update-digest #xCBF29CE484222325 base)))
                           :auth-policy-required 1 :logical-slot-copy-id id))
           (digest (physical-store-update-digest
                    #xCBF29CE484222325 (physical-bootstrap-manifest-image manifest)))
           (descriptor (make-logical-slot-copy-descriptor
                        :copy-id id :manifest-digest digest :source-control source :base-control base
                        :recovery-control recovery :history (make-array 0 :element-type '(unsigned-byte 8))
                        :slots (list (list "alpha" (copy-descriptor-test-slot generation)))
                        :wal-pieces (list (list 1 0 end 0 33 end)))))
      (values descriptor manifest digest))))

(cl-spec:defgenerator copy-binding-arguments ()
  (multiple-value-list (copy-binding-test-data (random (ash 1 128)) (zerop (random 2)))))

(cl-spec:defspec-function logical-slot-copy-check-binding
    (:args (descriptor (type logical-slot-copy-descriptor)) (manifest list) (digest (type (unsigned-byte 64))))
  (:args-generator copy-binding-arguments)
  (:returns (type logical-slot-copy-descriptor))
  (:post (eq result descriptor)))

(cl-spec:defproperty copy-binding-preserves-detached-images
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (retained (member t nil)))
  (:about logical-slot-copy-check-binding)
  (:trials (:normal 200))
  (multiple-value-bind (descriptor manifest digest) (copy-binding-test-data generation retained)
    (let ((before (logical-slot-copy-descriptor-encode descriptor))
          (text (physical-bootstrap-manifest-image manifest))
          (registry *wal-replication-slots*)
          (count (hash-table-count *wal-replication-slots*)))
      (and (eq descriptor (logical-slot-copy-check-binding descriptor manifest digest))
           (equalp before (logical-slot-copy-descriptor-encode descriptor))
           (equalp text (physical-bootstrap-manifest-image manifest))
           (eq registry *wal-replication-slots*) (= count (hash-table-count registry))))))

(rove:deftest copy-binding-generated-contracts
    (let ((function-result (cl-spec:result-data
                            (cl-spec:check-function 'logical-slot-copy-check-binding :seed 2311 :trials 200)))
          (property-result (cl-spec:result-data
                            (cl-spec:run-property 'copy-binding-preserves-detached-images :seed 2312))))
      (rove:ok (eq :passed (getf function-result :status)) (format nil "~s" function-result))
      (rove:ok (= 200 (getf function-result :trials)))
      (rove:ok (eq :passed (getf property-result :status)) (format nil "~s" property-result))
      (rove:ok (= 200 (getf property-result :trials)))))

(rove:deftest copy-binding-manifest-suffix-is-canonical
    (multiple-value-bind (descriptor manifest digest) (copy-binding-test-data 0)
      (declare (ignore descriptor digest))
      (rove:ok (recovery-manifest-keys-valid-p manifest))
      (let ((missing (copy-list manifest)) (legacy (copy-list manifest)))
        (remf missing :auth-policy-required)
        (setf (getf legacy :version) 2)
        (rove:ok (not (recovery-manifest-keys-valid-p missing)))
        (rove:ok (not (recovery-manifest-keys-valid-p legacy))))
      (dolist (id '("" "0" "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF" nil))
        (let ((copy (copy-list manifest)))
          (setf (getf copy :logical-slot-copy-id) id)
          (rove:ok (not (recovery-manifest-keys-valid-p copy)))))
      (let ((copy (append (subseq manifest 0 20)
                          (list :logical-slot-copy-id (getf manifest :logical-slot-copy-id)
                                :auth-policy-required 1))))
        (rove:ok (not (recovery-manifest-keys-valid-p copy)))))
  (multiple-value-bind (descriptor manifest digest) (copy-binding-test-data 1)
    (declare (ignore descriptor digest))
    (remf manifest :logical-slot-copy-id)
    (rove:ok (recovery-manifest-keys-valid-p manifest))))

(rove:deftest copy-binding-rejects-crossed-controls-and-identity
    (dolist (retained '(nil t))
      (multiple-value-bind (descriptor manifest digest) (copy-binding-test-data 0 retained)
        (let ((copy (copy-logical-slot-copy-descriptor descriptor)))
          (setf (logical-slot-copy-descriptor-copy-id copy) (logical-slot-generation-token 1))
          (rove:ok (rove:signals (logical-slot-copy-check-binding copy manifest digest) 'error)))
        (rove:ok (rove:signals (logical-slot-copy-check-binding descriptor manifest (logxor 1 digest)) 'error))
        (let* ((copy (copy-logical-slot-copy-descriptor descriptor))
               (source (logical-slot-copy-control-fields (logical-slot-copy-descriptor-source-control descriptor)))
               (metadata (copy-list (seventh source))))
          (setf (getf metadata :system-id) 43
                (logical-slot-copy-descriptor-source-control copy)
                (wal-encode-control-image (first source) (second source) (third source)
                                          :timeline 1 :namespace :legacy :metadata metadata))
          (rove:ok (rove:signals (logical-slot-copy-check-binding copy manifest digest) 'error)))
        (let ((copy (copy-logical-slot-copy-descriptor descriptor)))
          (setf (logical-slot-copy-descriptor-recovery-control copy)
                (physical-recovery-control-image 0 42 1 0 (getf manifest :end) (getf manifest :end) 4 :state-time 0))
          (rove:ok (rove:signals (logical-slot-copy-check-binding copy manifest digest) 'error)))
        (let* ((copy (copy-logical-slot-copy-descriptor descriptor))
               (candidate (copy-tree manifest))
               (fields (logical-slot-copy-control-fields (logical-slot-copy-descriptor-base-control descriptor)))
               (metadata (copy-list (seventh fields))))
          (setf (getf metadata :state-time) 1
                (logical-slot-copy-descriptor-base-control copy)
                (wal-encode-control-image (first fields) (second fields) (third fields) :metadata metadata))
          (let* ((bytes (logical-slot-copy-descriptor-base-control copy))
                 (row (find "global/pg_wal_control" (getf candidate :files) :key #'first :test #'equal))
                 (new-digest nil))
            (setf (second row) (length bytes)
                  (third row) (physical-store-update-digest #xCBF29CE484222325 bytes)
                  new-digest (physical-store-update-digest #xCBF29CE484222325
                                                           (physical-bootstrap-manifest-image candidate))
                  (logical-slot-copy-descriptor-manifest-digest copy) new-digest)
            (rove:ok (rove:signals (logical-slot-copy-check-binding copy candidate new-digest) 'error)))))))

(rove:deftest copy-binding-restart-requires-its-position-proof
    (let* ((slot (logical-slot-state-decode (copy-descriptor-test-slot 0) "alpha"))
           (history (timeline-parse-history 1 "")))
      (rove:ok (equal '(1 :prefix-end) (multiple-value-list (logical-slot-copy-proven-start slot 33 1 history))))
      (rove:ok (rove:signals (logical-slot-copy-proven-start slot 34 1 history) 'error))
      (setf (gethash 9 (wal-replication-slot-pending-xids slot)) (cons 33 80))
      (rove:ok (equal '(1 :record-start) (multiple-value-list (logical-slot-copy-proven-start slot 33 1 history))))
      (clrhash (wal-replication-slot-pending-xids slot))
      (setf (wal-replication-slot-restart-lsn slot) 0)
      (rove:ok (equal '(1 :record-start) (multiple-value-list (logical-slot-copy-proven-start slot 0 1 history)))))
  (let* ((slot (logical-slot-state-decode (copy-descriptor-test-slot 0) "alpha"))
         (history (timeline-parse-history 2 (format nil "1~c0/C8~cfork~%" #\Tab #\Tab))))
    (setf (wal-replication-slot-restart-lsn slot) 200
          (wal-replication-slot-restart-timeline slot) 2
          (wal-replication-slot-confirmed-flush-lsn slot) 220
          (wal-replication-slot-confirmed-timeline slot) 2)
    (rove:ok (rove:signals (logical-slot-copy-proven-start slot 200 2 history) 'error))
    (setf (gethash 9 (wal-replication-slot-pending-xids slot)) (cons 200 220))
    (rove:ok (equal '(2 :record-start) (multiple-value-list (logical-slot-copy-proven-start slot 200 2 history))))
    (clrhash (wal-replication-slot-pending-xids slot))
    (setf (wal-replication-slot-restart-timeline slot) 1)
    (rove:ok (equal '(1 :prefix-end) (multiple-value-list (logical-slot-copy-proven-start slot 200 1 history))))))

(rove:deftest copy-binding-bundle-sender-rejects-slot-adjunct
    (multiple-value-bind (descriptor manifest digest) (copy-binding-test-data 0)
      (declare (ignore descriptor digest))
      (let ((caught nil))
        (handler-case (physical-backup-bundle-entries #p"/not-opened/" manifest)
          (error (condition) (setf caught condition)))
        (rove:ok (and caught (search "cannot carry logical slot copy adjuncts" (princ-to-string caught)))))))

(rove:deftest copy-binding-held-pin-publication-preserves-existing-files
    (let* ((directory (uiop:ensure-directory-pathname
                       (sb-posix:mkdtemp
                        (namestring (merge-pathnames "copy-pin-XXXXXX" (uiop:temporary-directory))))))
           (root nil) (parent nil)
           (first (string->bytes "first pin")) (second (string->bytes "bound pin")))
      (unwind-protect
           (progn
             (setf root (physical-backup-open-directory directory)
                   parent (logical-slot-copy-open-pin-directory root :create t))
             (physical-source-pin-write-at parent "copy.pin" first t)
             (physical-source-pin-readback-at parent "copy.pin" first)
             (rove:ok (handler-case
                          (progn (physical-source-pin-write-at parent "copy.pin" second t) nil)
                        (error () t)))
             (physical-source-pin-readback-at parent "copy.pin" first)
             (rove:ok (null (wal-namespace-file-stat
                             (merge-pathnames "pg_physical_pins/copy.pin.tmp" directory) t)))
             (physical-source-pin-write-at parent "copy.pin" second nil)
             (physical-source-pin-readback-at parent "copy.pin" second)
             (rove:ok (= #o600 (logand #o7777
                                       (third (physical-image-file-facts parent "copy.pin")))))
             (let ((victim (merge-pathnames "victim" directory))
                   (temporary (merge-pathnames "pg_physical_pins/copy.pin.tmp" directory)))
               (with-open-file (stream victim :direction :output :if-exists :error)
                 (write-string "untouched" stream))
               (sb-posix:symlink (namestring victim) (namestring temporary))
               (rove:ok (handler-case
                            (progn (physical-source-pin-write-at parent "copy.pin" first nil) nil)
                          (error () t)))
               (rove:ok (equal "untouched" (uiop:read-file-string victim)))
               (physical-source-pin-readback-at parent "copy.pin" second)
               (sb-posix:unlink (namestring temporary)))
             (sb-posix:rename (namestring (merge-pathnames "pg_physical_pins" directory))
                              (namestring (merge-pathnames "old_pins" directory)))
             (sb-posix:symlink "old_pins" (namestring (merge-pathnames "pg_physical_pins" directory)))
             (rove:ok (handler-case
                          (let ((fd (logical-slot-copy-open-pin-directory root :create t)))
                            (sb-posix:close fd) nil)
                        (error () t))))
        (when parent (sb-posix:close parent))
        (when root (sb-posix:close root))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))
