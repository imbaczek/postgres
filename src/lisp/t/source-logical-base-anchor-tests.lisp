(in-package #:pg-physical-tests)

(deftest source-logical-reseed-base-copy-preserves-owned-anchor
    (dolist (enabled '(nil t))
      (let* ((record (reseed-follow-codec-fixture))
             (request (getf (getf record :attempt) :request))
             (base (getf record :base))
             (fact (source-logical-anchor-test-fact enabled 0)))
        (setf (getf fact :through) 152
              (getf fact :through-timeline) (getf request :timeline))
        (let* ((text (pg::physical-logical-encode-base-anchor fact))
               (anchored (append base (list :logical-base-anchor text)))
               (copy (pg::physical-reseed-follow-base-copy anchored request)))
          (ok (equal text (getf copy :logical-base-anchor)))
          (ok (not (eq text (getf copy :logical-base-anchor))))
          (let ((saved (copy-list record)))
            (setf (getf saved :base) anchored)
            (ok (equal (pg::physical-reseed-follow-copy saved)
                       (pg::physical-reseed-follow-decode
                        (pg::physical-reseed-follow-image saved)))))
          (setf (char (getf copy :logical-base-anchor) 0) #\X)
          (ok (char= (char text 0) #\5))
          (dolist (bad (list nil "" (string-downcase text)
                             (pg::physical-logical-encode-base-anchor
                              (let ((wrong (copy-list fact)))
                                (incf (getf wrong :through)) wrong))))
            (ok (intake-fails-p
                 (lambda ()
                   (pg::physical-reseed-follow-base-copy
                    (append base (list :logical-base-anchor bad)) request))))))
        (ok (not (get-properties
                  (pg::physical-reseed-follow-base-copy base request)
                  '(:logical-base-anchor)))))))

(defun source-logical-adoption-test-record (enabled anchored)
  (let* ((record (adoption-attempt-codec-fixture :phase :proved))
         (local (getf record :local)) (stage (getf record :stage))
         (request (getf record :request)) (anchor (getf record :anchor))
         (proof (getf record :proof))
         (snapshot (sample-checkpoint-snapshot :version 2 :start 256 :redo 256 :end 376
                                               :next-xid 42 :oldest-active-xid 42))
         (history (format nil "1~c0/000001B8~cadoption logical test~%" #\Tab #\Tab))
         (fact (source-logical-anchor-test-fact enabled 0)))
    (unless anchored (setf (getf snapshot :logical-decoding-enabled) enabled))
    (let ((bytes (pg::wal-encode-control-image
                  376 376 snapshot :metadata
                  (pg::make-wal-control-metadata 42 pg::+db-shutdowned+ 100))))
      (setf (getf local :wal-control) (pg::physical-libpq-octets-datum bytes)
            (getf local :capture)
            (list (list (length bytes)
                        (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
                  nil nil nil nil nil nil)))
    (setf (getf local :end) 376 (getf local :checkpoint-start) 256
          (getf stage :checkpoint) 376 (getf stage :checkpoint-start) 256
          (getf stage :end) 376 (getf (getf record :origin-proof) :end) 376
          (getf request :base) 376 (getf request :receive-lsn) 376
          (getf anchor :base) 376 (getf anchor :receive-lsn) 376
          (getf anchor :eor-octets)
          (coerce (pg::wal-encode-end-of-recovery-record 440 42 2 1 1) 'list)
          (getf record :source-history) history
          (getf proof :end) 376 (getf proof :fork) 440
          (getf fact :through) 376)
    (when anchored
      (setf (getf record :stage)
            (append stage (list :logical-base-anchor
                                (pg::physical-logical-encode-base-anchor fact)))))
    (setf (getf proof :anchor-digest)
          (pg::physical-follow-anchor-digest
           (pg::physical-adoption-follow-record-anchor record)))
    (values record snapshot fact)))

(deftest source-logical-adoption-binding-keeps-known-checkpoint-or-immutable-anchor
    (dolist (anchored '(nil t))
      (dolist (enabled '(nil t))
        (multiple-value-bind (record snapshot fact)
            (source-logical-adoption-test-record enabled anchored)
          (let* ((copy (pg::physical-adoption-attempt-copy record))
                 (decoded (pg::physical-adoption-attempt-decode
                           (pg::physical-adoption-attempt-image record)))
                 (binding (pg::physical-adoption-follow-planned-binding record))
                 (logical (getf (getf binding :recovery) :logical-fact))
                 (expected (if anchored fact (pg::physical-logical-fact-from-checkpoint snapshot))))
            (ok (equal copy decoded))
            (ok (equal expected logical))
            (ok (not (eq expected logical)))
            (ok (pg::physical-adoption-follow-check-binding record binding))
            (when anchored
              (ok (not (eq (getf (getf record :stage) :logical-base-anchor)
                           (getf (getf copy :stage) :logical-base-anchor)))))
            (let ((bad (copy-tree binding)))
              (remf (getf bad :recovery) :logical-fact)
              (ok (pg::physical-follow-binding-check bad))
              (ok (intake-fails-p
                   (lambda () (pg::physical-adoption-follow-check-binding record bad)))))
            (let* ((bad (copy-tree binding))
                   (opposite (if anchored (source-logical-anchor-test-fact (not enabled) 0)
                                 (let ((changed (copy-list snapshot)))
                                   (setf (getf changed :logical-decoding-enabled) (not enabled))
                                   (pg::physical-logical-fact-from-checkpoint changed)))))
              (setf (getf opposite :through) 376
                    (getf (getf bad :recovery) :logical-fact) opposite)
              (ok (pg::physical-follow-binding-check bad))
              (ok (intake-fails-p
                   (lambda () (pg::physical-adoption-follow-check-binding record bad))))))))))

(deftest source-logical-adoption-stage-rejects-invalid-anchor-before-binding
    (multiple-value-bind (record snapshot fact) (source-logical-adoption-test-record t t)
      (declare (ignore snapshot))
      (let* ((local (getf record :local)) (stage (getf record :stage))
                                          (text (getf stage :logical-base-anchor)))
        (ok (pg::physical-adoption-attempt-stage-copy stage local))
        (dolist (bad (list nil "" (string-downcase text)
                           (pg::physical-logical-encode-base-anchor
                            (let ((changed (copy-list fact)))
                              (incf (getf changed :through)) changed))))
          (let ((changed (copy-tree stage)))
            (setf (getf changed :logical-base-anchor) bad)
            (ok (intake-fails-p
                 (lambda () (pg::physical-adoption-attempt-stage-copy changed local))))))
        (let ((changed (copy-tree record)))
          (setf (getf (getf changed :stage) :logical-base-anchor)
                (pg::physical-logical-encode-base-anchor
                 (let ((invalid (copy-list fact)))
                   (setf (getf invalid :witness-start) 300
                         (getf invalid :witness-end) 333
                         (getf invalid :last-enable-lsn) 333)
                   invalid)))
          (ok (pg::physical-adoption-attempt-copy changed))
          (ok (intake-fails-p
               (lambda () (pg::physical-adoption-follow-planned-binding changed))))))))

(deftest source-logical-pending-cold-read-retains-distinct-immutable-base-anchor
    (dolist (enabled '(nil t))
      (call-with-source-logical-anchor-base
       enabled
       (lambda (root snapshot anchor)
         (let* ((base (nth-value 0 (pg::physical-recovery-verify-base-state)))
                (end (getf snapshot :end))
                (directory (merge-pathnames "anchor-spool/" root))
                (store nil))
           (pg::wal-ensure-directory-durable directory)
           (unwind-protect
                (let* ((pg::*physical-recovery-state* base)
                       (pg::*last-replay-lsn* end) (pg::*next-xid* 50)
                       (pg::*xid-batch-end* 50)
                       (pg::*physical-receive-store*
                         (setf store
                               (pg::physical-store-open
                                directory (getf base :system-id) 1 :create t :start-lsn end
                                :retained-directory (pg::physical-recovery-retained-directory)
                                :retained-identity (pg::physical-recovery-retained-identity base)))))
                  (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION")
                                              (pg::string->bytes (format nil "16~%")))
                  (let ((later (append-source-logical-test-status store (not enabled))))
                    (ok (= later (pg::physical-replay-from end)))
                    (pg::physical-store-compact store later #'pg::physical-restartpoint-publish)
                    (let* ((current (copy-tree (getf pg::*physical-recovery-state* :source-logical-fact)))
                           (pending (call-with-promotion-exclusion
                                     (lambda () (pg::physical-promotion-publish-intent 2)))))
                      (ok (equal anchor (getf pending :base-source-logical-fact)))
                      (ok (> (getf current :through) (getf anchor :through)))
                      (dotimes (retry 2)
                        (declare (ignorable retry))
                        (let ((cold (pg::physical-promotion-read-pending-state)))
                          (ok (equal anchor (getf cold :base-source-logical-fact)))
                          (ok (equal current (getf cold :source-logical-fact)))
                          (ok (equal current (getf cold :required-logical-fact)))
                          (ok (not (eq (getf cold :base-source-logical-fact)
                                       (getf cold :source-logical-fact))))
                          (ok (not (eq (getf cold :source-logical-fact)
                                       (getf cold :required-logical-fact))))
                          (ok (not (eq anchor (getf cold :base-source-logical-fact)))))))))
             (when store (pg::physical-store-close store)))))
       :system-id (pg::physical-store-update-digest
                   #xCBF29CE484222325 (pg::string->bytes "history-source")))))

(deftest source-logical-standby-export-writes-verifiable-immutable-anchor
    (dolist (case '((nil :unknown) (t :unknown) (nil nil) (t t)))
      (destructuring-bind (enabled checkpoint-status) case
        (call-with-source-logical-anchor-base
         enabled
         (lambda (root snapshot prior)
           (declare (ignore prior))
           (let* ((base (nth-value 0 (pg::physical-recovery-verify-base-state)))
                  (anchor (getf base :source-logical-fact))
                  (directory (merge-pathnames "export-spool/" root))
                  (store nil))
             (pg::wal-ensure-directory-durable directory)
             (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION")
                                         (pg::string->bytes (format nil "16~%")))
             (write-startup-config (merge-pathnames "pg_hba.conf" root)
                                   "host all all 127.0.0.1/32 trust")
             (write-startup-config (merge-pathnames "pg_ident.conf" root) "# export test")
             (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
               (let* ((settings (pg::startup-load-settings native overrides))
                      (policy (pg::authentication-read-policy settings))
                      (pg::*startup-installed-configuration*
                        (pg::startup-prepare-installation settings native overrides policy))
                      (pg::*physical-recovery-state* base)
                      (pg::*catalog* (pg::load-catalog))
                      (pg::*next-xid* 50) (pg::*xid-batch-end* 50)
                      (pg::*last-replay-lsn* (getf snapshot :end))
                      (pg::*physical-receive-store*
                        (setf store (pg::physical-store-open
                                     directory (getf base :system-id) 1 :create t
                                     :start-lsn (getf snapshot :end)
                                     :retained-directory (pg::physical-recovery-retained-directory)
                                     :retained-identity (pg::physical-recovery-retained-identity base)))))
                 (unwind-protect
                      (with-physical-store-directory (target)
                        (let ((manifest (pg::physical-standby-base-backup target "anchor_export")))
                          (ok (not (find (concatenate 'string "pg_wal/"
                                                      (pg::wal-segment-filename 0 1 :timeline))
                                         (getf manifest :files) :key #'first :test #'equal)))
                          (ok (eql (eq checkpoint-status :unknown)
                                   (not (null (getf manifest :logical-base-anchor)))))
                          (let ((pg::*pgdata* target))
                            (multiple-value-bind (restored control) (pg::physical-recovery-verify-base-state)
                              (ok (zerop (getf control :phase)))
                              (ok (equal anchor (getf control :logical-fact)))
                              (ok (equal anchor (getf restored :source-logical-fact)))
                              (ok (equal anchor (getf restored :base-source-logical-fact)))
                              (ok (not (eq (getf restored :source-logical-fact)
                                           (getf restored :base-source-logical-fact))))
                              (ok (= (getf anchor :through) (getf restored :required-applied)))))))
                   (when store (pg::physical-store-close store)))))))
         :system-id (pg::physical-store-update-digest
                     #xCBF29CE484222325 (pg::string->bytes "history-source"))
         :checkpoint-status checkpoint-status :separate-witness-segment t))))

(deftest source-logical-restartpoint-recovers-anchor-coverage-before-checkpoint-fallback
    (dolist (enabled '(nil t))
      (call-with-source-logical-anchor-base
       enabled
       (lambda (root snapshot anchor)
         (let* ((base (nth-value 0 (pg::physical-recovery-verify-base-state)))
                (directory (merge-pathnames "anchor-restartpoint/" root)) (store nil))
           (pg::wal-ensure-directory-durable directory)
           (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION")
                                       (pg::string->bytes (format nil "16~%")))
           (unwind-protect
                (let* ((pg::*physical-recovery-state* base)
                       (pg::*last-replay-lsn* (getf snapshot :end))
                       (pg::*next-xid* 50) (pg::*xid-batch-end* 50)
                       (pg::*physical-receive-store*
                         (setf store (pg::physical-store-open
                                      directory (getf base :system-id) 1 :create t
                                      :start-lsn (getf snapshot :end)
                                      :retained-directory (pg::physical-recovery-retained-directory)
                                      :retained-identity (pg::physical-recovery-retained-identity base)))))
                  (let* ((checkpoint (append-source-logical-test-checkpoint store enabled))
                         (end (getf checkpoint :end)))
                    (ok (= end (pg::physical-replay-from (getf snapshot :end))))
                    (let ((expected (copy-tree (getf pg::*physical-recovery-state* :source-logical-fact))))
                      (pg::physical-restartpoint-publish (pg::physical-store-prefix-digest store end))
                      (multiple-value-bind (metadata catalog prefix)
                          (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                        (remf metadata :source-logical-fact)
                        (source-fpw-publish-test-restartpoint metadata catalog prefix))
                      (ok (= (getf base :end) (pg::physical-store-floor-lsn store)))
                      (dotimes (retry 2)
                        (declare (ignorable retry))
                        (remf pg::*physical-recovery-state* :source-logical-fact)
                        (ok (= end (pg::physical-restartpoint-prepare-storage store)))
                        (let ((actual (getf pg::*physical-recovery-state* :source-logical-fact)))
                          (ok (equal expected actual))
                          (ok (not (eq expected actual)))
                          (ok (= (getf anchor :coverage-start) (getf actual :coverage-start)))
                          (ok (equal (getf anchor :last-disable-lsn) (getf actual :last-disable-lsn))))))))
             (when store (pg::physical-store-close store)))))
       :system-id (pg::physical-store-update-digest
                   #xCBF29CE484222325 (pg::string->bytes "history-source")))))

(defun source-logical-anchor-test-fact (enabled offset)
  (multiple-value-bind (off on later) (source-logical-persistence-facts)
    (declare (ignore later))
    (let ((fact (if enabled on off)))
      (dolist (key '(:witness-start :witness-end :coverage-start :through :last-enable-lsn :last-disable-lsn))
        (when (getf fact key) (incf (getf fact key) offset)))
      fact)))

(defun source-logical-anchor-test-checkpoint (status)
  (let ((snapshot (sample-checkpoint-snapshot :version 2 :start 256 :redo 256 :end 376)))
    (unless (eq status :unknown) (setf (getf snapshot :logical-decoding-enabled) status))
    snapshot))

(defun source-logical-anchor-test-manifest ()
  (list :version 3 :system-id 42 :cluster-identity "anchor-source" :timeline 1
        :checkpoint 376 :checkpoint-start 256 :end 376 :next-xid 42
        :source-pin "anchor_copy" :files (list (list "pg_wal/0000000000000000.wal" 376 0))))

(defun call-with-source-logical-anchor-base (enabled function &key (system-id 123) (checkpoint-status :unknown) separate-witness-segment)
  (call-with-promotion-fixture
   (lambda (root store)
     (declare (ignore store))
     (let ((cursor 0) (frames nil) (fact nil)
                      (history (pg::timeline-parse-history 1 "")))
       (dolist (status (list nil t enabled))
         (push (sample-physical-record cursor) frames)
         (incf cursor pg::+wal-header-size+)
         (let ((bytes (pg::wal-encode-record 18 0 cursor
                                             (pg::wal-encode-logical-decoding-status-change status))))
           (setf fact (pg::physical-logical-fact-next fact (pg::wal-decode-record bytes cursor) 1 history))
           (push bytes frames)
           (incf cursor (length bytes))))
       (when separate-witness-segment
         (setf cursor pg::+wal-segment-size+ frames nil))
       (push (sample-physical-record cursor) frames)
       (incf cursor pg::+wal-header-size+)
       (let* ((snapshot (sample-checkpoint-snapshot
                         :version 2 :start cursor :redo cursor :end (+ cursor 120)
                         :next-xid 50 :oldest-active-xid 50
                         :next-oid (pg::catalog-next-oid (pg::load-catalog))))
              (end (getf snapshot :end))
              (bytes (progn
                       (unless (eq checkpoint-status :unknown)
                         (setf (getf snapshot :logical-decoding-enabled) checkpoint-status))
                       (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 cursor
                                              (pg::wal-encode-checkpoint-snapshot snapshot)))))
         (setf fact (pg::physical-logical-fact-next fact (pg::wal-decode-record bytes cursor) 1 history))
         (push bytes frames)
         (let* ((base-bytes (apply #'concatenate '(vector (unsigned-byte 8)) (nreverse frames)))
                (wal-control (pg::wal-encode-control-image end end snapshot :namespace :legacy))
                (manifest (nth-value 0 (pg::recovery-read-manifest root)))
                (name (if separate-witness-segment
                          (concatenate 'string "pg_wal/" (pg::wal-segment-filename 1 1 :legacy))
                          (first (first (getf manifest :files))))))
           (setf (first (first (getf manifest :files))) name)
           (setf (getf manifest :checkpoint) end (getf manifest :checkpoint-start) cursor
                 (getf manifest :end) end)
           (setf manifest (append manifest (list :logical-base-anchor
                                                 (pg::physical-logical-encode-base-anchor fact))))
           (pg::wal-atomic-write-bytes (merge-pathnames name root) base-bytes)
           (pg::wal-atomic-write-bytes (pg::wal-control-path) wal-control)
           (dolist (entry (getf manifest :files))
             (let ((contents (if (equal name (first entry)) base-bytes wal-control)))
               (setf (second entry) (length contents)
                     (third entry) (pg::physical-store-update-digest #xCBF29CE484222325 contents))))
           (write-recovery-manifest-text root (pg::physical-bootstrap-manifest-text manifest))
           (let ((digest (nth-value 1 (pg::recovery-read-manifest root))))
             (pg::wal-atomic-write-bytes
              (pg::physical-recovery-control-path)
              (pg::physical-recovery-control-image
               2 (getf manifest :system-id) 1 digest end end 50 :logical-fact fact)))
           (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
           (funcall function root snapshot fact)))))
   :applied 152 :system-id system-id))

(cl-spec:defgenerator source-logical-base-anchor-arguments ()
  (list (source-logical-anchor-test-fact (zerop (random 2)) (random 1000000))))

(cl-spec:defspec-function pg::physical-logical-encode-base-anchor
    (:args (fact (type list)))
  (:args-generator source-logical-base-anchor-arguments)
  (:pre (pg::physical-logical-fact-valid-p fact))
  (:returns (type string))
  (:post (and (= 176 (length result))
              (equal fact (pg::physical-logical-decode-base-anchor result)))))

(cl-spec:defproperty source-logical-base-anchor-roundtrips-known-evidence
    ((enabled (member nil t)) (offset (range integer 0 1000000)))
  (:about pg::physical-logical-encode-base-anchor pg::physical-logical-decode-base-anchor)
  (:trials (:normal 150))
  (let* ((fact (source-logical-anchor-test-fact enabled offset))
         (text (pg::physical-logical-encode-base-anchor fact))
         (decoded (pg::physical-logical-decode-base-anchor text)))
    (and (= 176 (length text)) (equal fact decoded) (not (eq fact decoded)))))

(deftest source-logical-base-anchor-codec-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'pg::physical-logical-encode-base-anchor :trials 150 :seed 7301))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data)))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'source-logical-base-anchor-roundtrips-known-evidence
                                     :profile :normal :seed 7302))))
    (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest source-logical-base-anchor-rejects-noncanonical-hex-and-fact-bytes
    (let ((text (pg::physical-logical-encode-base-anchor (source-logical-anchor-test-fact t 0))))
      (dolist (bad (list nil 7 "" (subseq text 1) (concatenate 'string text "0")
                         (string-downcase text) (substitute #\G #\F text)))
        (ok (intake-fails-p (lambda () (pg::physical-logical-decode-base-anchor bad)))))
      (dolist (position '(0 24))
        (let ((bad (copy-seq text)))
          (setf (char bad position) (if (zerop position) #\0 #\1))
          (ok (intake-fails-p (lambda () (pg::physical-logical-decode-base-anchor bad))))))
      (ok (intake-fails-p (lambda () (pg::physical-logical-encode-base-anchor nil))))))

(deftest source-logical-base-anchor-keeps-unknown-checkpoint-evidence-independent
    (dolist (enabled '(nil t))
      (let* ((snapshot (source-logical-anchor-test-checkpoint :unknown))
             (fact (source-logical-anchor-test-fact enabled 0))
             (history (pg::timeline-parse-history 1 "")))
        (setf (getf fact :through) 376)
        (let* ((text (pg::physical-logical-encode-base-anchor fact))
               (manifest (list :end 376 :timeline 1 :logical-base-anchor text))
               (actual (pg::physical-logical-base-fact manifest snapshot history)))
          (ok (equal fact actual))
          (ok (not (eq fact actual)))
          (setf (char text 0) #\0)
          (ok (equal fact actual)))
        (ok (null (pg::physical-logical-base-fact (list :end 376 :timeline 1) snapshot history))))))

(deftest source-logical-base-anchor-known-checkpoint-starts-fresh-coverage
    (dolist (enabled '(nil t))
      (let* ((snapshot (source-logical-anchor-test-checkpoint enabled))
             (history (pg::timeline-parse-history 1 ""))
             (record (pg::make-wal-record
                      :version 8 :type pg::+wal-record-checkpoint+ :xid 0 :start 256 :end 376
                      :payload (pg::wal-encode-checkpoint-snapshot snapshot)))
             (prior (source-logical-anchor-test-fact enabled 0))
             (anchored (pg::physical-logical-fact-next prior record 1 history))
             (expected (pg::physical-logical-fact-from-checkpoint snapshot))
             (manifest (list :end 376 :timeline 1)))
        (ok (equal expected (pg::physical-logical-base-fact manifest snapshot history)))
        (setf (getf manifest :logical-base-anchor) (pg::physical-logical-encode-base-anchor anchored))
        (let ((actual (pg::physical-logical-base-fact manifest snapshot history)))
          (ok (equal expected actual))
          (ok (= 256 (getf actual :coverage-start)))
          (ok (null (getf actual :last-enable-lsn)))
          (ok (null (getf actual :last-disable-lsn))))
        (setf (getf anchored :enabled) (not enabled)
              (getf anchored :last-enable-lsn) nil (getf anchored :last-enable-timeline) nil
              (getf anchored :last-disable-lsn) nil (getf anchored :last-disable-timeline) nil)
        (ok (pg::physical-logical-fact-valid-p anchored))
        (setf (getf manifest :logical-base-anchor) (pg::physical-logical-encode-base-anchor anchored))
        (ok (intake-fails-p (lambda () (pg::physical-logical-base-fact manifest snapshot history)))))))

(deftest source-logical-base-anchor-rejects-wrong-frontier-and-unknown-checkpoint-witness
    (let* ((snapshot (source-logical-anchor-test-checkpoint :unknown))
           (history (pg::timeline-parse-history 1 ""))
           (fact (source-logical-anchor-test-fact t 0)))
      (dolist (mutation '((:through 375) (:through-timeline 2)))
        (let ((bad (copy-tree fact)))
          (setf (getf bad :through) 376 (getf bad (first mutation)) (second mutation))
          (ok (pg::physical-logical-fact-valid-p bad))
          (ok (intake-fails-p
               (lambda () (pg::physical-logical-base-fact
                           (list :end 376 :timeline 1
                                 :logical-base-anchor (pg::physical-logical-encode-base-anchor bad))
                           snapshot history))))))
      (let* ((claim (pg::physical-logical-fact-from-checkpoint (source-logical-anchor-test-checkpoint t)))
             (manifest (list :end 376 :timeline 1
                             :logical-base-anchor (pg::physical-logical-encode-base-anchor claim))))
        (ok (pg::physical-logical-fact-valid-p claim))
        (ok (intake-fails-p (lambda () (pg::physical-logical-base-fact manifest snapshot history)))))))

(deftest source-logical-base-anchor-manifest-suffix-and-install-copy
    (let ((fact (source-logical-anchor-test-fact t 0)))
      (setf (getf fact :through) 376)
      (let ((text (pg::physical-logical-encode-base-anchor fact)))
        (dolist (auth '(nil t))
          (let ((plain (append (source-logical-anchor-test-manifest)
                               (when auth (list :auth-policy-required 1)))))
            (ok (eq plain (pg::recovery-check-manifest-schema plain)))
            (let* ((anchored (append plain (list :logical-base-anchor (copy-seq text))))
                   (request (list :name "anchor_copy" :cluster-identity "anchor-source"
                                  :system-id 42 :timeline 1))
                   (copy (pg::physical-reseed-install-manifest-copy anchored request)))
              (ok (pg::recovery-check-manifest-schema anchored))
              (ok (equal anchored (pg::recovery-parse-manifest-text
                                   (pg::physical-bootstrap-manifest-text anchored))))
              (ok (pg::physical-reseed-install-check-literal anchored))
              (ok (equal anchored copy))
              (ok (not (eq (getf anchored :logical-base-anchor) (getf copy :logical-base-anchor))))
              (setf (char (getf anchored :logical-base-anchor) 0) #\0)
              (ok (equal text (getf copy :logical-base-anchor)))
              (ok (pg::recovery-check-manifest-schema copy))))))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-install-check-literal fact))))))

(deftest source-logical-base-anchor-manifest-rejects-malformed-and-legacy-extensions
    (let ((fact (source-logical-anchor-test-fact t 0)))
      (setf (getf fact :through) 376)
      (let* ((text (pg::physical-logical-encode-base-anchor fact))
             (base (source-logical-anchor-test-manifest))
             (good (append base (list :auth-policy-required 1 :logical-base-anchor text))))
        (dolist (bad (list (append good (list :logical-base-anchor text))
                           (append base (list :logical-base-anchor text :auth-policy-required 1))
                           (append base (list :logical-base-anchor nil))
                           (append base (list :logical-base-anchor (string-downcase text)))
                           (append base (list :logical-base-anchor (subseq text 1)))))
          (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema bad)))))
        (dolist (version '(1 2))
          (let ((bad (append (copy-tree base) (list :logical-base-anchor text))))
            (setf (getf bad :version) version)
            (when (= version 1) (remf bad :checkpoint-start))
            (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema bad))))))
        (setf (getf fact :through) 375)
        (let ((bad (append base (list :logical-base-anchor (pg::physical-logical-encode-base-anchor fact)))))
          (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema bad))))))))

(deftest source-logical-base-anchor-rejects-foreign-history-geometry
    (let* ((snapshot (source-logical-anchor-test-checkpoint :unknown))
           (fact (source-logical-anchor-test-fact t 0))
           (history (pg::timeline-parse-history 2 (format nil "1~c0/00000040~%" #\Tab))))
      (setf (getf snapshot :timeline) 2
            (getf fact :through) 376 (getf fact :through-timeline) 2)
      (ok (pg::physical-logical-fact-valid-p fact))
      (ok (intake-fails-p
           (lambda () (pg::physical-logical-base-fact
                       (list :end 376 :timeline 2
                             :logical-base-anchor (pg::physical-logical-encode-base-anchor fact))
                       snapshot history))))))

(deftest source-logical-base-anchor-reconstructs-independent-source-from-immutable-manifest
    (dolist (enabled '(nil t))
      (call-with-source-logical-anchor-base
       enabled
       (lambda (root checkpoint expected)
         (let ((runtime pg::*physical-recovery-state*)
               (before (lineage-test-bytes (pg::physical-recovery-control-path root))))
           (multiple-value-bind (base control) (pg::physical-recovery-verify-base-state)
             (ok (not (get-properties checkpoint '(:logical-decoding-enabled))))
             (ok (< (getf expected :witness-end) (getf checkpoint :start)))
             (ok (equal expected (getf base :source-logical-fact)))
             (ok (equal expected (getf base :base-source-logical-fact)))
             (ok (equal expected (getf base :required-logical-fact)))
             (ok (equal expected (getf control :logical-fact)))
             (ok (not (eq (getf base :source-logical-fact) (getf base :base-source-logical-fact))))
             (ok (not (eq (getf base :source-logical-fact) (getf base :required-logical-fact)))))
           (ok (eq runtime pg::*physical-recovery-state*))
           (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path root)))))))))

(deftest source-logical-standby-export-selects-fresh-or-anchored-base-evidence
    (dolist (status '(:unknown nil t))
      (dolist (present '(nil t))
        (let* ((snapshot (source-logical-anchor-test-checkpoint status))
               (history (pg::timeline-parse-history 1 ""))
               (current
                 (when present
                   (let ((prior (source-logical-anchor-test-fact (if (eq status :unknown) t status) 0)))
                     (if (eq status :unknown)
                         (progn (setf (getf prior :through) 376) prior)
                         (pg::physical-logical-fact-next
                          prior (pg::make-wal-record
                                 :version 8 :type pg::+wal-record-checkpoint+ :xid 0 :start 256 :end 376
                                 :payload (pg::wal-encode-checkpoint-snapshot snapshot))
                          1 history)))))
               (pg::*physical-receive-store* nil)
               (pg::*physical-recovery-state* (list :timeline 1 :source-logical-fact current))
               (before (copy-tree pg::*physical-recovery-state*))
               (expected (if (eq status :unknown) current
                             (pg::physical-logical-fact-from-checkpoint snapshot))))
          (multiple-value-bind (fact anchor) (pg::physical-standby-backup-logical-facts snapshot)
            (ok (equal expected fact))
            (when fact (ok (not (eq fact current))))
            (if (and (eq status :unknown) current)
                (ok (equal current (pg::physical-logical-decode-base-anchor anchor)))
                (ok (null anchor))))
          (ok (equal before pg::*physical-recovery-state*))))))

(deftest source-logical-standby-export-rejects-inconsistent-current-evidence
    (let ((pg::*physical-receive-store* nil)
          (pg::*physical-recovery-state* (list :timeline 1)))
      (ok (pg::physical-logical-fact-at-p
           (nth-value 0 (pg::physical-standby-backup-logical-facts
                         (source-logical-anchor-test-checkpoint t))) 376 1)))
  (dolist (fault '(:frontier :unknown-witness :known-disagreement))
    (let* ((snapshot (source-logical-anchor-test-checkpoint
                      (if (eq fault :known-disagreement) nil :unknown)))
           (current (if (eq fault :frontier)
                        (source-logical-anchor-test-fact t 0)
                        (pg::physical-logical-fact-from-checkpoint
                         (source-logical-anchor-test-checkpoint t))))
           (pg::*physical-receive-store* nil)
           (pg::*physical-recovery-state* (list :timeline 1 :source-logical-fact current)))
      (when (eq fault :frontier) (setf (getf current :through) 375))
      (ok (pg::physical-logical-fact-valid-p current))
      (let ((before (copy-tree pg::*physical-recovery-state*)))
        (ok (intake-fails-p (lambda () (pg::physical-standby-backup-logical-facts snapshot))))
        (ok (equal before pg::*physical-recovery-state*))))))
