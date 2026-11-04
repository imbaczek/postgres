(in-package #:pg-physical-tests)

(defun reseed-follow-generation-fixture (kind phase &optional (generation 1) (digest 991))
  (let* ((allocation (backup-lease-allocation-test-record :live generation digest))
         (request (getf allocation :request))
         (coordinates (getf allocation :coordinates))
         (same (eq kind :same-timeline))
         (history (if same
                      (format nil "1~c0/00000020~cpromotion~%" #\Tab #\Tab)
                      (format nil "1~c0/00000020~cpromotion~%2~c0/000000C0~cpromotion~%"
                              #\Tab #\Tab #\Tab #\Tab))))
    (setf (getf coordinates :source-timeline) (if same 2 3))
    (when (eq kind :retained-timeline) (setf (getf coordinates :kind) :retained-timeline))
    (setf coordinates (pg::physical-follow-request-copy coordinates))
    (let* ((anchor (pg::physical-follow-anchor-copy
                    (append coordinates (when same (list :kind :same-timeline))
                            (list :history-text history)
                            (when (eq kind :descendant)
                              (list :eor-octets
                                    (coerce (pg::wal-encode-end-of-recovery-record 192 42 3 2 1) 'list))))))
           (detached (loop for (key value) on anchor by #'cddr
                           unless (eq key :history-text) append (list key value))))
      (values (list :generation generation :name (format nil "target_follow_~d" generation)
                    :coordinates coordinates :source-history history :phase phase
                    :anchor (unless (eq phase :preparing) detached))
              request))))

(cl-spec:defproperty reseed-follow-generation-copies-exact-proof-and-inputs
    ((kind-choice (range integer 0 2)) (phase-choice (range integer 0 3))
                                       (generation (range integer 1 16)) (digest (range integer 0 18446744073709551615)))
  (:about pg::physical-reseed-follow-generation-copy pg::physical-reseed-follow-generation-anchor)
  (:trials (:normal 100))
  (multiple-value-bind (record request)
      (reseed-follow-generation-fixture
       (nth kind-choice '(:same-timeline :descendant :retained-timeline))
       (nth phase-choice '(:preparing :proved :canceling :canceled)) generation digest)
    (let* ((before (pg::physical-bootstrap-manifest-text record))
           (copy (pg::physical-reseed-follow-generation-copy record request))
           (anchor (pg::physical-reseed-follow-generation-anchor copy)))
      (and (equal before (pg::physical-bootstrap-manifest-text record))
           (equal copy (pg::physical-reseed-follow-generation-copy copy request))
           (equal (getf record :coordinates) (getf copy :coordinates))
           (equal (getf record :source-history) (getf copy :source-history))
           (not (eq (getf record :source-history) (getf copy :source-history)))
           (not (getf (getf copy :anchor) :history-text))
           (if (eq :preparing (getf record :phase)) (null anchor)
               (and (pg::physical-follow-request-matches-anchor-p (getf copy :coordinates) anchor)
                    (equal (getf copy :source-history) (getf anchor :history-text))))))))

(deftest generated-reseed-follow-generation-proof-copies
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-generation-copies-exact-proof-and-inputs
                                       :seed 115 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-generation-rejects-malformed-or-unbound-proofs
    (multiple-value-bind (record request) (reseed-follow-generation-fixture :descendant :proved)
      (dolist (mutation '((:generation 0) (:generation 17) (:name "bad-name") (:phase :active)
                          (:source-history "") (:anchor nil)))
        (let ((bad (copy-tree record)))
          (setf (getf bad (first mutation)) (second mutation))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-generation-copy bad request))))))
      (dolist (fault '(:coordinate :history-duplicate :preparing-proof :foreign-request :duplicate-key))
        (let ((bad (copy-tree record)) (owner (pg::physical-backup-lease-request-copy request)))
          (ecase fault
            (:coordinate (incf (getf (getf bad :coordinates) :manifest-digest)))
            (:history-duplicate
             (setf (getf (getf bad :anchor) :history-text) (getf bad :source-history)))
            (:preparing-proof (setf (getf bad :phase) :preparing))
            (:foreign-request (setf (getf owner :timeline) 1))
            (:duplicate-key (setf bad (append bad (list :phase :proved)))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-generation-copy bad owner))))))))

(deftest reseed-follow-generation-transitions-never-rebind-or-lose-proof
    (dolist (kind '(:same-timeline :descendant :retained-timeline))
      (multiple-value-bind (proved request) (reseed-follow-generation-fixture kind :proved)
        (let ((preparing (copy-tree proved)) (canceling (copy-tree proved)) (canceled (copy-tree proved)))
          (setf (getf preparing :phase) :preparing (getf preparing :anchor) nil
                (getf canceling :phase) :canceling (getf canceled :phase) :canceled)
          (dolist (pair (list (list preparing preparing) (list preparing proved)
                              (list proved canceling) (list canceling canceled) (list canceled canceled)))
            (ok (pg::physical-reseed-follow-generation-check-transition
                 (first pair) (second pair) request)))
          (dolist (pair (list (list proved preparing) (list preparing canceled)
                              (list canceling proved) (list canceled preparing)))
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-generation-check-transition
                             (first pair) (second pair) request)))))
          (let ((bad (copy-tree canceling)))
            (setf (getf bad :anchor) nil)
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-generation-check-transition proved bad request)))))
          (let ((bad (pg::physical-reseed-follow-generation-copy proved request)))
            (setf (getf bad :source-history)
                  (concatenate 'string (getf bad :source-history) "# changed reason\n"))
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-generation-check-transition proved bad request)))))))))

(defun reseed-follow-codec-fixture (&optional (kind :same-timeline) (phase :preparing))
  (let* ((plan (reseed-install-codec-fixture 0))
         (attempt (getf plan :attempt))
         (request (getf attempt :request))
         (system (getf request :system-id))
         (timeline (getf request :timeline)))
    (multiple-value-bind (generation owner) (reseed-follow-generation-fixture kind phase)
      (declare (ignore owner))
      (setf (getf (getf generation :coordinates) :receive-digest) #xCBF29CE484222325)
      (when (getf generation :anchor)
        (setf (getf (getf generation :anchor) :receive-digest) #xCBF29CE484222325))
      (list :version 1 :attempt attempt
            :install (list :identity (list 1 7 #o100600 1 (sb-posix:geteuid) 4096 20 30)
                           :digest 999 :body-crc 33 :step 10)
            :base (list :manifest-digest 991 :checkpoint-start 120
                        :recovery-control
                        (pg::physical-libpq-octets-datum
                         (pg::physical-recovery-control-image 0 system timeline 0 152 152 42 :state-time 0))
                        :wal-control
                        (pg::physical-libpq-octets-datum
                         (pg::wal-encode-control-image
                          152 152 nil :timeline timeline :checkpoint-timeline timeline :namespace :timeline
                          :metadata (pg::make-wal-control-metadata system pg::+db-in-production+ 0)))
                        :auth-files '(("authentication.initialized" 1 33) ("pg_hba.conf" 1 33) ("pg_ident.conf" 1 33)))
            :generations (list generation)))))

(cl-spec:defproperty reseed-follow-journal-roundtrips-immutable-predecessor-and-generation
    ((kind-choice (range integer 0 2)) (phase-choice (range integer 0 3)) (empty boolean))
  (:about pg::physical-reseed-follow-copy pg::physical-reseed-follow-image pg::physical-reseed-follow-decode)
  (:trials (:normal 100))
  (let ((record (reseed-follow-codec-fixture
                 (nth kind-choice '(:same-timeline :descendant :retained-timeline))
                 (nth phase-choice '(:preparing :proved :canceling :canceled)))))
    (when empty (setf (getf record :generations) nil))
    (let* ((before (pg::physical-bootstrap-manifest-text record))
           (copy (pg::physical-reseed-follow-copy record))
           (decoded (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image record))))
      (and (equal copy decoded)
           (equal before (pg::physical-bootstrap-manifest-text record))
           (not (eq (getf (getf copy :attempt) :source) (getf (getf record :attempt) :source)))))))

(deftest generated-reseed-follow-journal-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-journal-roundtrips-immutable-predecessor-and-generation
                                       :seed 116 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-journal-keeps-exhausted-actual-xid-and-zero-phase-manifest-digest
    (let* ((record (reseed-follow-codec-fixture))
           (base (getf record :base))
           (request (getf (getf record :attempt) :request)))
      (setf (getf base :recovery-control)
            (pg::physical-libpq-octets-datum
             (pg::physical-recovery-control-image
              0 (getf request :system-id) (getf request :timeline) 0 152 152 18446744073709551616 :state-time 0)))
      (let* ((copy (pg::physical-reseed-follow-copy record))
             (control (pg::physical-recovery-decode-control
                       (pg::physical-libpq-datum-octets (getf (getf copy :base) :recovery-control) 196))))
        (ok (= 18446744073709551616 (getf control :next-xid)))
        (ok (zerop (getf control :manifest-digest)))
        (ok (= 991 (getf (getf copy :base) :manifest-digest))))))

(deftest reseed-follow-journal-rejects-advanced-and-foreign-base-intent
    (dolist (fault '(:advanced :prefix :manifest :system :timeline :checkpoint-start :install-mode :install-size
                     :install-crc :attempt-phase :duplicate-name :generation-gap))
      (let* ((record (reseed-follow-codec-fixture))
             (base (getf record :base))
             (row (first (getf record :generations)))
             (coordinates (getf row :coordinates)))
        (ecase fault
          (:advanced (incf (getf coordinates :receive-lsn)))
          (:prefix (setf (getf coordinates :receive-digest) 123))
          (:manifest (incf (getf coordinates :manifest-digest)))
          (:system (setf (getf (getf record :attempt) :request) (backup-lease-wire-request "other")))
          (:timeline (setf (getf coordinates :receive-timeline) 1))
          (:checkpoint-start (setf (getf base :checkpoint-start) 119))
          (:install-mode (setf (third (getf (getf record :install) :identity)) #o100644))
          (:install-size (setf (sixth (getf (getf record :install) :identity)) 0))
          (:install-crc (setf (getf (getf record :install) :body-crc) (ash 1 32)))
          (:attempt-phase (setf (getf (getf record :attempt) :phase) :aborting))
          (:duplicate-name
           (setf (getf row :phase) :canceled
                 (getf record :generations) (list row (copy-tree row))
                 (getf (second (getf record :generations)) :generation) 2))
          (:generation-gap (setf (getf row :generation) 2)))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-copy record))) (symbol-name fault)))))

(deftest reseed-follow-journal-transitions-preserve-history-and-durable-preparation
    (let* ((first (pg::physical-reseed-follow-copy (reseed-follow-codec-fixture)))
           (empty (pg::physical-reseed-follow-copy first))
           (proved (pg::physical-reseed-follow-copy (reseed-follow-codec-fixture :same-timeline :proved))))
      (setf (getf empty :generations) nil)
      (ok (pg::physical-reseed-follow-check-transition empty first))
      (ok (pg::physical-reseed-follow-check-transition first proved))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-check-transition empty proved))))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-check-transition first empty))))
      (let* ((canceled (pg::physical-reseed-follow-copy (reseed-follow-codec-fixture :same-timeline :canceled)))
             (next (pg::physical-reseed-follow-copy first))
             (second (copy-tree (first (getf next :generations)))))
        (setf (getf second :generation) 2 (getf second :name) "target_follow_2"
              (getf next :generations) (list (pg::physical-adoption-attempt-value-copy (first (getf canceled :generations))) second))
        (ok (pg::physical-reseed-follow-check-transition canceled next))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-check-transition first next))))
        (setf (getf (first (getf next :generations)) :source-history)
              (concatenate 'string (getf (first (getf next :generations)) :source-history) "# different"))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-check-transition canceled next)))))))

(defvar *reseed-follow-reader-fired* nil)

(deftest reseed-follow-journal-rejects-cycles-corruption-and-reader-evaluation
    (let* ((record (reseed-follow-codec-fixture))
           (image (pg::physical-reseed-follow-image record))
           (corrupt (copy-seq image))
           (reordered (loop for key in '(:generations :base :install :attempt :version)
                            append (list key (getf record key)))))
      (incf (aref corrupt (1- (length corrupt))))
      (dolist (bytes (list corrupt
                           (concatenate '(vector (unsigned-byte 8)) image #(10))
                           (pg::physical-backup-lease-encode-literal
                            "PGRESEEDFOLLOW1" reordered pg::+physical-reseed-follow-maximum-bytes+)
                           #(1 2 3)))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-decode bytes)))))
      (let ((cycle (copy-list record)))
        (setf (cdr (last cycle)) cycle)
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-copy cycle)))))
      (let* ((*reseed-follow-reader-fired* nil)
             (body "#.(setf pg-physical-tests::*reseed-follow-reader-fired* t)")
             (bytes (pg::string->bytes
                     (format nil "PGRESEEDFOLLOW1 ~8,'0X~%~a~%"
                             (pg::wal-crc32 (pg::string->bytes body)) body))))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-decode bytes))))
        (ok (not *reseed-follow-reader-fired*)))))

(deftest reseed-follow-journal-copies-credentials-controls-and-proof-history
    (let* ((record (reseed-follow-codec-fixture :descendant :proved))
           (before (pg::physical-bootstrap-manifest-text record))
           (copy (pg::physical-reseed-follow-copy record)))
      (setf (char (getf (getf (getf copy :attempt) :source) :conninfo) 0) #\z
            (char (getf (getf (getf copy :attempt) :request) :token) 0) #\b
            (char (second (getf (getf copy :base) :recovery-control)) 0) #\f
            (char (getf (first (getf copy :generations)) :source-history) 0) #\#
            (first (getf (getf (first (getf copy :generations)) :anchor) :eor-octets)) 255)
      (ok (equal before (pg::physical-bootstrap-manifest-text record)))))

(deftest reseed-follow-generation-cancellation-preserves-an-absent-proof
    (multiple-value-bind (before request) (reseed-follow-generation-fixture :same-timeline :preparing)
      (let ((canceling (copy-tree before)) (canceled (copy-tree before)))
        (setf (getf canceling :phase) :canceling (getf canceled :phase) :canceled)
        (ok (pg::physical-reseed-follow-generation-check-transition before canceling request))
        (ok (pg::physical-reseed-follow-generation-check-transition canceling canceled request))
        (let ((forged (reseed-follow-generation-fixture :same-timeline :canceling)))
          (ok (intake-fails-p
               (lambda () (pg::physical-reseed-follow-generation-check-transition canceling forged request))))))))

(deftest reseed-follow-predecessor-reader-digests-the-complete-observed-file
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((fd (pg::physical-backup-open-directory root)) (digests nil))
         (unwind-protect
              (dolist (case '((0 nil) (0 t) (1 nil)))
                (reseed-install-owner-test-write root plan (first case) (second case))
                (let* ((state (pg::physical-reseed-install-read-at fd "physical-reseed.install"))
                       (bytes (lineage-test-bytes (pg::physical-reseed-install-path root)))
                       (digest (getf state :digest))
                       (expected (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
                  (ok (typep digest '(unsigned-byte 64)))
                  (ok (eql digest expected))
                  (ok (= (getf state :file-size) (length bytes)))
                  (push digest digests)))
           (sb-posix:close fd))
         (ok (= 3 (length (remove-duplicates digests))))))))

(deftest reseed-follow-generation-rejects-empty-child-history
    (dolist (phase '(:preparing :proved))
      (multiple-value-bind (record request) (reseed-follow-generation-fixture :same-timeline phase)
        (setf (getf record :source-history) "")
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-generation-copy record request)))))))

(deftest reseed-follow-generation-does-not-invent-unrecorded-ancestors
    (dolist (kind '(:same-timeline :descendant))
      (multiple-value-bind (record request) (reseed-follow-generation-fixture kind :proved)
        (setf (getf record :source-history)
              (if (eq kind :same-timeline) "# no recorded parent"
                  (format nil "2~c0/000000C0~cpromotion~%" #\Tab #\Tab)))
        (ok (pg::physical-reseed-follow-generation-copy record request)))))

(deftest reseed-follow-journal-supports-timeline-one-with-empty-history
    (dolist (phase '(:preparing :proved))
      (let* ((record (pg::physical-reseed-follow-copy (reseed-follow-codec-fixture :same-timeline phase)))
             (request (getf (getf record :attempt) :request))
             (base (getf record :base))
             (generation (first (getf record :generations))))
        (setf (getf request :timeline) 1
              (getf generation :source-history) ""
              (getf base :recovery-control)
              (pg::physical-libpq-octets-datum
               (pg::physical-recovery-control-image 0 (getf request :system-id) 1 0 152 152 42 :state-time 0))
              (getf base :wal-control)
              (pg::physical-libpq-octets-datum
               (pg::wal-encode-control-image
                152 152 nil :timeline 1 :checkpoint-timeline 1 :namespace :legacy
                :metadata (pg::make-wal-control-metadata (getf request :system-id) pg::+db-in-production+ 0))))
        (dolist (value (list (getf generation :coordinates) (getf generation :anchor)))
          (when value
            (setf (getf value :base-timeline) 1 (getf value :receive-timeline) 1
                  (getf value :source-timeline) 1)))
        (ok (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image record))))))
