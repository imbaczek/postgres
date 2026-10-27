(in-package #:pg-physical-tests)

(deftest child-recovery-control-preserves-timeline-and-phase
    (dolist (timeline '(2 7 4294967295))
      (dolist (phase '(0 1 2))
        (let* ((applied (if (= phase 2) 96 64))
               (bytes (pg::physical-recovery-control-image phase 99 timeline
                                                           (if (zerop phase) 0 123)
                                                           64 applied 100 :state-time 42))
               (control (pg::physical-recovery-decode-control bytes)))
          (ok (= 72 (length bytes)))
          (ok (= timeline (getf control :timeline)))
          (ok (= phase (getf control :phase)))
          (ok (= applied (getf control :applied)))))))

(deftest child-recovery-pending-control-preserves-parent-and-child
    (let* ((bytes (pg::physical-recovery-control-image
                   3 99 7 123 64 96 100 :state-time 42
                   :promotion-child 9 :promotion-generation 11
                   :promotion-source-namespace :timeline))
           (control (pg::physical-recovery-decode-control bytes)))
      (ok (= 88 (length bytes)))
      (ok (= 7 (getf control :timeline)))
      (ok (= 9 (getf control :promotion-child)))
      (ok (eq :timeline (getf control :promotion-source-namespace)))))

(deftest child-recovery-control-rejects-invalid-timeline-identities
    (dolist (timeline '(0 -1 4294967296))
      (ok (signals (pg::physical-recovery-control-image 2 99 timeline 123 64 96 100) 'error)))
  (dolist (child '(1 6 7))
    (ok (signals (pg::physical-recovery-control-image
                  3 99 7 123 64 96 100 :promotion-child child
                  :promotion-generation 11 :promotion-source-namespace :timeline) 'error)))
  (ok (signals (pg::physical-recovery-control-image
                3 99 7 123 64 96 100 :promotion-child 9
                                     :promotion-generation 11 :promotion-source-namespace :legacy) 'error)))

(cl-spec:defproperty child-recovery-control-roundtrip-preserves-timeline
    ((timeline (range integer 1 4294967295)) (phase (range integer 0 2)))
  (:about pg::physical-recovery-control-image)
  (:trials (:normal 200))
  (let* ((control (pg::physical-recovery-decode-control
                   (pg::physical-recovery-control-image
                    phase 99 timeline (if (zerop phase) 0 123) 64 64 100 :state-time 42))))
    (and (= timeline (getf control :timeline))
         (= phase (getf control :phase))
         (= 99 (getf control :system-id))
         (= 100 (getf control :next-xid)))))

(deftest generated-child-recovery-control-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'child-recovery-control-roundtrip-preserves-timeline :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest child-recovery-control-rejects-legacy-layouts-and-contradictory-pending-bytes
    (let ((original (pg::physical-recovery-control-image 2 99 7 123 64 96 100 :state-time 42)))
      (dolist (size '(60 72))
        (let* ((bytes (subseq original 0 size))
               (offset (- size 4)))
          (replace bytes (if (= size 60)
                             #(80 71 82 83 84 65 84 69)
                             #(80 71 82 83 84 65 84 50)))
          (pg::wal-set-u32 bytes offset 0)
          (pg::wal-set-u32 bytes offset (pg::wal-crc32 bytes))
          (ok (signals (pg::physical-recovery-decode-control bytes) 'error)))))
  (let ((bytes (pg::physical-recovery-control-image
                3 99 7 123 64 96 100 :promotion-child 9
                :promotion-generation 11 :promotion-source-namespace :timeline)))
    (pg::wal-set-u32 bytes 68 7)
    (pg::wal-set-u32 bytes 84 0)
    (pg::wal-set-u32 bytes 84 (pg::wal-crc32 bytes))
    (ok (signals (pg::physical-recovery-decode-control bytes) 'error))))

(deftest child-bootstrap-inventory-includes-available-ancestral-history
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (ensure-directories-exist (merge-pathnames "pg_wal/placeholder" root))
        (pg::timeline-write-history 2 1 64 "first")
        (pg::timeline-write-history 7 2 128 "second")
        (ok (equal '("pg_wal/00000002.history" "pg_wal/00000007.history")
                   (pg::physical-bootstrap-history-inventory 7)))
        (ok (null (pg::physical-bootstrap-history-inventory 1)))
        (delete-file (pg::timeline-history-path 2 root))
        (ok (equal '("pg_wal/00000007.history") (pg::physical-bootstrap-history-inventory 7))))))

(defun child-bootstrap-sample-manifest ()
  (list :version 3 :system-id 123 :cluster-identity "fixture#identity" :timeline 7
        :checkpoint 152 :checkpoint-start 32 :end 152 :next-xid 3 :source-pin "copy"
        :files (copy-tree '(("PG_VERSION" 3 9)
                            ("pg_wal/000000070000000000000000.wal" 152 9)
                            ("pg_wal/00000007.history" 30 9)))))

(deftest child-bootstrap-manifest-binds-timeline-to-paths-and-tip-history
    (let ((manifest (child-bootstrap-sample-manifest)))
      (ok (eq manifest (pg::recovery-check-manifest-schema manifest)))
      (setf (getf manifest :files) (butlast (getf manifest :files)))
      (ok (signals (pg::recovery-check-manifest-schema manifest) 'error)))
  (let ((manifest (child-bootstrap-sample-manifest)))
    (setf (first (second (getf manifest :files))) "pg_wal/000000020000000000000000.wal")
    (ok (signals (pg::recovery-check-manifest-schema manifest) 'error)))
  (let ((manifest (child-bootstrap-sample-manifest)))
    (setf (getf manifest :version) 2)
    (ok (signals (pg::recovery-check-manifest-schema manifest) 'error))))

(deftest child-bootstrap-nonpending-control-cannot-fall-back-to-primary
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (primary (symbol-function 'pg::physical-promotion-startup-primary-p))
            (calls 0))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-promotion-startup-primary-p)
                     (lambda () (incf calls) t))
               (dolist (phase '(0 1 2))
                 (let ((bytes (pg::physical-recovery-control-image
                               phase 99 7 (if (zerop phase) 0 123) 64 64 100 :state-time 42)))
                   (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bytes)
                   (ok (signals (pg::physical-bootstrap-check-startup) 'error))
                   (ok (equalp bytes (pg::recovery-read-bytes (pg::physical-recovery-control-path))))))
               (ok (zerop calls)))
          (setf (symbol-function 'pg::physical-promotion-startup-primary-p) primary)))))

(deftest child-bootstrap-malformed-control-cannot-authorize-primary-cleanup
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (primary (symbol-function 'pg::physical-promotion-startup-primary-p))
            (calls 0))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-promotion-startup-primary-p)
                     (lambda () (incf calls) t))
               (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) #(1 2 3 4))
               (ok (signals (pg::physical-bootstrap-check-startup) 'error))
               (ok (zerop calls)))
          (setf (symbol-function 'pg::physical-promotion-startup-primary-p) primary)))))

(deftest child-bootstrap-committed-primary-routing-survives-pending-or-absent-control
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (primary (symbol-function 'pg::physical-promotion-startup-primary-p))
            (calls 0))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-promotion-startup-primary-p)
                     (lambda () (incf calls) t))
               (pg::physical-bootstrap-check-startup)
               (ok (= 1 calls))
               (pg::wal-atomic-write-bytes
                (pg::physical-recovery-control-path)
                (pg::physical-recovery-control-image
                 3 99 7 123 64 96 100 :promotion-child 9
                 :promotion-generation 11 :promotion-source-namespace :timeline))
               (pg::physical-bootstrap-check-startup)
               (ok (= 2 calls)))
          (setf (symbol-function 'pg::physical-promotion-startup-primary-p) primary)))))

(deftest child-source-pin-roundtrip-binds-the-selected-timeline
    (call-with-physical-source-fixture
     (lambda ()
       (let* ((pg::*wal-insert-timeline* 7)
              (pg::*wal-segment-namespace* :timeline)
              (pg::*wal-durable-lsn* 0)
              (pin (funcall 'pg::make-physical-source-pin :name "child"
                                                          :system-id (pg::catalog-system-identifier)
                                                          :timeline 7 :restart-lsn 0 :consistent-lsn 0)))
         (pg::wal-ensure-directory-durable (pg::physical-source-pin-directory))
         (pg::physical-source-pin-save pin)
         (let* ((path (pg::physical-source-pin-path "child"))
                (bytes (pg::recovery-read-bytes path))
                (loaded (pg::physical-source-pin-read path)))
           (ok (= 7 (funcall 'pg::physical-source-pin-timeline loaded)))
           (let ((pg::*wal-insert-timeline* 2))
             (ok (signals (pg::physical-source-pin-read path) 'error)))
           (ok (equalp bytes (pg::recovery-read-bytes path))))))))

(deftest child-source-pin-rejects-foreign-lineage-even-when-lost
    (let ((pg::*wal-insert-timeline* 2)
          (pg::*wal-durable-lsn* 0))
      (let ((pin (funcall 'pg::make-physical-source-pin
                          :name "foreign" :system-id 99 :timeline 7
                          :restart-lsn 0 :consistent-lsn 0 :lost-p t)))
        (ok (signals (pg::physical-source-pin-check-wal pin) 'error)))))

(deftest child-stream-negotiation-and-identity-require-the-exact-base-timeline
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 7 :create t :start-lsn 64))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :system-id 123 :timeline 7 :end 64))
             (runtime (pg::make-physical-follow-runtime :pin "child")))
        (unwind-protect
             (let* ((query (pg::physical-follow-start-query runtime 64))
                    (command (pg::parse-replication-command query)))
               (ok (equal "7" (pg::physical-stream-option command "timeline")))
               (ok (not (intake-fails-p
                         (lambda () (pg::physical-follow-check-identity (list #("123" "7" "0/40" "postgres")))))))
               (dolist (timeline '("1" "2" "8"))
                 (ok (signals (pg::physical-follow-check-identity
                               (list (vector "123" timeline "0/40" "postgres"))) 'error))))
          (pg::physical-store-close store)))))

(deftest child-source-pin-manifest-binding-is-durable-and-required-for-streaming
    (call-with-physical-source-fixture
     (lambda ()
       (let* ((pg::*wal-insert-timeline* 7)
              (pg::*wal-segment-namespace* :timeline)
              (pg::*wal-checkpoint-lsn* 0)
              (pg::*wal-durable-lsn* 0)
              (pg::*physical-recovery-state* nil)
              (pg::*physical-recovery-failed-p* nil)
              (pg::*physical-source-senders* (make-hash-table :test #'equal))
              (pin (pg::make-physical-source-pin :name "child"
                                                 :system-id (pg::catalog-system-identifier) :timeline 7
                                                 :restart-lsn 0 :consistent-lsn 0)))
         (pg::wal-ensure-directory-durable (pg::physical-source-pin-directory))
         (setf (gethash "child" pg::*physical-source-pins*) pin)
         (pg::physical-source-pin-save pin)
         (pg::physical-source-pin-bind-manifest pin 456)
         (ok (= 456 (pg::physical-source-pin-manifest-digest
                     (pg::physical-source-pin-read (pg::physical-source-pin-path "child")))))
         (ok (signals (pg::physical-source-pin-bind-manifest pin 457) 'error))
         (flet ((command (digest)
                  (pg::parse-replication-command
                   (format nil "START_REPLICATION SLOT child PHYSICAL 0/0 (native_format 'pg_lisp_wal_7', system_id '~d', timeline '7', base_lsn '0/0', prefix_digest '14695981039346656037'~a)"
                           (pg::catalog-system-identifier)
                           (if digest (format nil ", manifest_digest '~d'" digest) "")))))
           (dolist (digest '(nil 457))
             (ok (signals (pg::physical-source-acquire-sender (command digest)) 'error))
             (ok (null (gethash "child" pg::*physical-source-senders*))))
           (let ((owner (pg::physical-source-acquire-sender (command 456))))
             (unwind-protect
                  (let ((pg::*wal-insert-timeline* 2))
                    (ok (signals (pg::physical-source-sender-floor "child" owner) 'error))
                    (ok (signals (pg::physical-source-read-next "child" owner 0) 'error))
                    (ok (signals (pg::physical-source-confirm-receipt "child" owner 0 456) 'error))
                    (ok (null (pg::physical-source-pin-confirmed-lsn pin))))
               (pg::physical-source-release-sender "child" owner))))))))

(deftest child-pending-startup-does-not-mistake-parent-child-control-for-commit
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (read-control (symbol-function 'pg::wal-read-control-fields))
            (supported (symbol-function 'pg::wal-require-supported-timelines))
            (retire (symbol-function 'pg::physical-promotion-retire-recovery))
            (retired 0))
        (pg::wal-atomic-write-bytes
         (pg::physical-recovery-control-path)
         (pg::physical-recovery-control-image
          3 99 7 123 64 96 100 :promotion-child 9
          :promotion-generation 11 :promotion-source-namespace :timeline))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::wal-read-control-fields)
                     (lambda () (values 64 64 nil 7 7 :timeline))
                     (symbol-function 'pg::wal-require-supported-timelines)
                     (lambda (&rest arguments) (declare (ignore arguments)) t)
                     (symbol-function 'pg::physical-promotion-retire-recovery)
                     (lambda () (incf retired)))
               (ok (null (pg::physical-promotion-startup-primary-p)))
               (ok (zerop retired)))
          (setf (symbol-function 'pg::wal-read-control-fields) read-control
                (symbol-function 'pg::wal-require-supported-timelines) supported
                (symbol-function 'pg::physical-promotion-retire-recovery) retire)))))

(deftest child-stream-manifest-option-boundaries
    (let ((prefix "START_REPLICATION SLOT copy PHYSICAL 0/0 (native_format 'pg_lisp_wal_7', system_id '123', timeline '7', base_lsn '0/0', prefix_digest '14695981039346656037'"))
      (dolist (suffix '(", manifest_digest '-1')" ", manifest_digest '18446744073709551616')"
                        ", manifest_digest 'bad')" ", manifest_digest '1', manifest_digest '2')"
                        ", unknown_manifest '1')"))
        (ok (signals (pg::parse-replication-command (concatenate 'string prefix suffix)) 'error)))
      (dolist (digest '(0 18446744073709551615))
        (let ((command (pg::parse-replication-command (format nil "~a, manifest_digest '~d')" prefix digest))))
          (ok (= digest (pg::physical-stream-u64 (pg::physical-stream-option command "manifest_digest"))))))))

(deftest child-stream-cannot-downgrade-bound-or-child-pins
    (dolist (timeline '(1 7))
      (call-with-physical-source-fixture
       (lambda ()
         (let* ((pg::*wal-insert-timeline* timeline)
                (pg::*wal-checkpoint-lsn* 0) (pg::*wal-durable-lsn* 0)
                (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                (pg::*physical-source-senders* (make-hash-table :test #'equal))
                (pin (pg::make-physical-source-pin
                      :name "copy" :system-id (pg::catalog-system-identifier)
                      :timeline timeline :manifest-digest (when (= timeline 1) 456)
                      :restart-lsn 0 :consistent-lsn 0))
                (command (pg::parse-replication-command
                          (format nil "START_REPLICATION SLOT copy PHYSICAL 0/0 (native_format 'pg_lisp_wal_7', system_id '~d', timeline '~d', base_lsn '0/0', prefix_digest '14695981039346656037')"
                                  (pg::catalog-system-identifier) timeline))))
           (setf (gethash "copy" pg::*physical-source-pins*) pin)
           (ok (signals (pg::physical-source-acquire-sender command) 'error))
           (ok (null (gethash "copy" pg::*physical-source-senders*)))
           (push (cons "manifest_digest" "'456'") (pg::logical-replication-command-options command))
           (if (= timeline 1)
               (let ((owner (pg::physical-source-acquire-sender command)))
                 (ok owner)
                 (pg::physical-source-release-sender "copy" owner))
               (ok (signals (pg::physical-source-acquire-sender command) 'error))))))))

(cl-spec:defproperty source-pin-binding-codec-preserves-unsigned-identity
    ((timeline (range integer 1 4294967295))
     (digest (range integer 0 18446744073709551615)))
  (:about pg::physical-source-pin-encode)
  (:trials (:normal 100))
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root) (pg::*catalog* (pg::make-catalog))
                               (pg::*wal-insert-timeline* timeline) (pg::*wal-segment-namespace* :timeline)
                               (pg::*wal-durable-lsn* 0)
                               (pin (pg::make-physical-source-pin
                                     :name "generated" :system-id (pg::catalog-system-identifier)
                                     :timeline timeline :manifest-digest digest :restart-lsn 0 :consistent-lsn 0)))
      (pg::wal-ensure-directory-durable (pg::wal-directory))
      (pg::wal-ensure-directory-durable (pg::physical-source-pin-directory))
      (pg::physical-source-pin-save pin)
      (let ((loaded (pg::physical-source-pin-read (pg::physical-source-pin-path "generated"))))
        (and (= timeline (pg::physical-source-pin-timeline loaded))
             (= digest (pg::physical-source-pin-manifest-digest loaded)))))))

(deftest generated-source-pin-binding-codec-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'source-pin-binding-codec-preserves-unsigned-identity :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest child-promotion-rechecks-manifest-bound-parent-history
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*wal-insert-timeline* 2) (pg::*wal-checkpoint-timeline* 2)
                                 (pg::*wal-segment-namespace* :timeline)
                                 (control-path (merge-pathnames "global/pg_wal_control" root))
                                 (history-path (pg::timeline-history-path 2 root))
                                 (control-bytes #(1 2 3 4))
                                 (history-bytes (pg::string->bytes (format nil "1~c0/00000040~cfixture~%" #\Tab #\Tab)))
                                 (original (symbol-function 'pg::wal-read-control-fields))
                                 (calls 0))
        (pg::wal-ensure-directory-durable (merge-pathnames "global/" root))
        (pg::wal-ensure-directory-durable (merge-pathnames "pg_wal/" root))
        (pg::wal-atomic-write-bytes control-path control-bytes)
        (pg::wal-atomic-write-bytes history-path history-bytes)
        (let ((state (list :timeline 2 :checkpoint 64 :end 64 :wal-namespace :timeline
                           :files (list (list "global/pg_wal_control" 4
                                              (pg::physical-store-update-digest #xCBF29CE484222325 control-bytes))
                                        (list "pg_wal/00000002.history" (length history-bytes)
                                              (pg::physical-store-update-digest #xCBF29CE484222325 history-bytes))))))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::wal-read-control-fields)
                       (lambda () (incf calls) (values 64 64 nil 2 2 :timeline)))
                 (ok (eq :timeline (pg::physical-promotion-check-base-control state)))
                 (pg::wal-atomic-write-bytes history-path
                                             (pg::string->bytes (format nil "1~c0/00000040~cchanged~%" #\Tab #\Tab)))
                 (ok (signals (pg::physical-promotion-check-base-control state) 'error))
                 (ok (= 1 calls)))
            (setf (symbol-function 'pg::wal-read-control-fields) original))))))
