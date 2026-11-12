(in-package #:pg-physical-tests)

(defun startup-slot-test-history (fork)
  (list (pg::make-timeline-history-entry :timeline 2 :begin fork)
        (pg::make-timeline-history-entry :timeline 1 :begin 0 :end fork)))

(cl-spec:defproperty startup-slot-fork-anchor-keeps-persisted-owner
    ((fork (range integer 1 1000000)))
  (:about pg::logical-slot-startup-start-position)
  (:trials (:normal 100))
  (let ((history (startup-slot-test-history fork)))
    (every
     (lambda (pair)
       (let ((slot (pg::make-wal-replication-slot :restart-lsn fork :restart-timeline (car pair))))
         (equal (list (car pair) (cdr pair))
                (multiple-value-list (pg::logical-slot-startup-start-position slot history)))))
     '((1 . :prefix-end) (2 . :record-start)))))

(deftest startup-slot-start-role-has-no-pending-map-prerequisite
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'startup-slot-fork-anchor-keeps-persisted-owner :seed 204))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 100 (getf data :trials))))
  (let* ((history (startup-slot-test-history 100))
         (slot (pg::make-wal-replication-slot :restart-lsn 0 :restart-timeline 1)))
    (ok (equal '(1 :record-start)
               (multiple-value-list (pg::logical-slot-startup-start-position slot history))))
    (setf (pg::wal-replication-slot-restart-timeline slot) 2)
    (ok (intake-fails-p (lambda () (pg::logical-slot-startup-start-position slot history))))))

(deftest startup-slot-local-eligibility-does-not-follow-byte-availability
    (let* ((history (startup-slot-test-history 100))
           (slot (pg::make-wal-replication-slot
                  :name "local" :restart-lsn 100 :restart-timeline 2
                  :consistent-lsn 150 :confirmed-flush-lsn 160 :confirmed-timeline 2
                  :logical-provenance (pg::make-logical-slot-provenance 42 1 150 2 65 1)))
           (ancestral (pg::make-wal-replication-slot
                       :name "ancestor" :restart-lsn 100 :restart-timeline 2
                       :consistent-lsn 65 :confirmed-flush-lsn 160 :confirmed-timeline 2
                       :logical-provenance (pg::make-logical-slot-provenance 42 1 65 1 65 1))))
      (ok (pg::logical-slot-startup-local-anchor-p slot history))
      (setf (pg::wal-replication-slot-restart-timeline slot) 1)
      (ok (not (pg::logical-slot-startup-local-anchor-p slot history)))
      (ok (not (pg::logical-slot-startup-local-anchor-p ancestral history)))))

(defun startup-slot-test-sql (sql)
  (uiop:with-temporary-file (:stream output :direction :io :element-type '(unsigned-byte 8))
    (pg::exec-simple-query output sql)
    (finish-output output)
    (file-position output 0)
    (loop for type = (read-byte output nil nil) while type
          do (let ((header (make-array 4 :element-type '(unsigned-byte 8))))
               (assert (= 4 (read-sequence header output)))
               (let* ((length (- (pg::bytes-to-u32 header 0) 4))
                      (payload (make-array length :element-type '(unsigned-byte 8))))
                 (assert (= length (read-sequence payload output)))
                 (when (= type (char-code #\E))
                   (error "startup fixture SQL failed: ~a" (pg::bytes->string payload))))))))

(defun startup-slot-test-pending (slot)
  (sort (loop for xid being the hash-keys of (pg::wal-replication-slot-pending-xids slot)
                using (hash-value entry) collect (list xid (car entry) (cdr entry))) #'< :key #'first))

(deftest startup-slot-real-restoration-rebuilds-pending-before-catalog-init
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (startup-slot-test-sql "CREATE TABLE startup_pending (id int)")
       (pg::create-standard-logical-slot "pending_restore" :nothing)
       (startup-slot-test-sql "INSERT INTO startup_pending VALUES (1)")
       (startup-slot-test-sql "INSERT INTO startup_pending VALUES (2)")
       (let* ((old (gethash "pending_restore" pg::*wal-replication-slots*))
              (expected (startup-slot-test-pending old))
              (pg::*catalog* nil))
         (ok (= 2 (length expected)))
         (pg::call-with-logical-slot-owned-startup-test
          (lambda (gate-owner ownership)
            (pg::wal-load-replication-slots :gate-owner gate-owner :ownership ownership)))
         (let ((restored (gethash "pending_restore" pg::*wal-replication-slots*)))
           (ok (not (eq old restored)))
           (ok (equal expected (startup-slot-test-pending restored)))
           (ok (null pg::*catalog*))
           (ok (= 1 pg::*logical-capture-live-slots*)))))))

(defun startup-slot-test-progress-record (type start &key xids (xid 0))
  (let* ((images (when (= type pg::+wal-record-pages+)
                   (list (pg::make-wal-page-image
                          :relfilenode 700 :block 0 :page (pg::page-init (pg::make-page))
                          :xids xids :provenance :row))))
         (payload
           (cond (images (pg::wal-encode-page-images
                          images (+ start pg::+wal-header-size+ (pg::wal-page-payload-length images))))
                 ((= type pg::+wal-record-commit+) (pg::wal-encode-commit-timestamp xid))
                 (t (make-array 0 :element-type '(unsigned-byte 8))))))
    (pg::make-wal-record :type type :version pg::+wal-version+ :start start
                         :end (+ start pg::+wal-header-size+ (length payload)) :xid xid :payload payload)))

(deftest startup-slot-progress-discards-only-abandoned-and-open-batch-contributions
    (let ((progress (pg::make-logical-slot-startup-progress nil))
          (position 0) (records nil) (kept-first nil) (later-first nil)
          (first-commit nil) (pg::*wal-recovery-reservation-batch-clean-p* nil))
      (flet ((emit (type &key xids (xid 0))
               (let ((record (startup-slot-test-progress-record type position :xids xids :xid xid)))
                 (push record records)
                 (pg::logical-slot-startup-accept-record progress record :ordinary)
                 (setf position (pg::wal-record-end record)))))
        (setf kept-first position)
        (emit pg::+wal-record-batch-start+)
        (emit pg::+wal-record-pages+ :xids '(7))
        (emit pg::+wal-record-maintenance+)
        (emit pg::+wal-record-batch-start+)
        (emit pg::+wal-record-pages+ :xids '(8))
        (emit pg::+wal-record-batch-start+)
        (emit pg::+wal-record-pages+ :xids '(7 9))
        (emit pg::+wal-record-commit+ :xid 7)
        (setf first-commit position later-first position)
        (emit pg::+wal-record-batch-start+)
        (emit pg::+wal-record-pages+ :xids '(8))
        (emit pg::+wal-record-commit+ :xid 8)
        (ok (equal (list (list 7 kept-first first-commit) (list 8 later-first position))
                   (sort (loop for xid being the hash-keys of (pg::logical-slot-startup-progress-pending progress)
                                 using (hash-value entry) collect (list xid (car entry) (cdr entry))) #'< :key #'first)))
        (emit pg::+wal-record-batch-start+)
        (emit pg::+wal-record-pages+ :xids '(10))
        (ok (null (gethash 10 (pg::logical-slot-startup-progress-pending progress))))
        (ok (null (gethash 10 (pg::logical-slot-startup-progress-first progress))))
        (let ((filtered (pg::make-logical-slot-startup-progress first-commit))
              (pg::*wal-recovery-reservation-batch-clean-p* nil))
          (dolist (record (reverse records)) (pg::logical-slot-startup-accept-record filtered record :ordinary))
          (ok (null (gethash 7 (pg::logical-slot-startup-progress-pending filtered))))
          (ok (gethash 8 (pg::logical-slot-startup-progress-pending filtered)))
          (ok (null (gethash 10 (pg::logical-slot-startup-progress-pending filtered)))))
        (ok (intake-fails-p
             (lambda ()
               (pg::logical-slot-startup-accept-record
                progress (pg::make-wal-record :type pg::+wal-record-end-of-recovery+) :end-of-recovery)))))))

(deftest startup-slot-second-candidate-failure-keeps-registry-and-mirrors
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (pg::create-standard-logical-slot "first_candidate" :nothing)
       (pg::create-standard-logical-slot "second_candidate" :nothing)
       (let ((registry pg::*wal-replication-slots*) (mirror pg::*logical-capture-live-slots*)
                                                    (policy pg::*wal-logical-policy*) (pending pg::*wal-logical-info-disable-pending*)
                                                    (original (symbol-function 'pg::logical-slot-startup-derive-pending))
                                                    (sentinel (make-condition 'simple-error :format-control "second candidate refused"))
                                                    (caught nil) (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::logical-slot-startup-derive-pending)
                      (lambda (&rest arguments)
                        (when (= 2 (incf calls)) (error sentinel))
                        (apply original arguments)))
                (handler-case
                    (pg::call-with-logical-slot-owned-startup-test
                     (lambda (gate-owner ownership)
                       (pg::wal-load-replication-slots :gate-owner gate-owner :ownership ownership)))
                  (error (condition) (setf caught condition))))
           (setf (symbol-function 'pg::logical-slot-startup-derive-pending) original))
         (ok (= 2 calls))
         (ok (eq sentinel caught))
         (ok (eq registry pg::*wal-replication-slots*))
         (ok (= mirror pg::*logical-capture-live-slots*))
         (ok (eq policy pg::*wal-logical-policy*))
         (ok (eq pending pg::*wal-logical-info-disable-pending*))
         (ok (pg::postmaster-pid-owned-p))))))

(deftest startup-slot-missing-authority-preserves-staging-and-state
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (pg::create-standard-logical-slot "authority_required" :nothing)
       (let* ((path (pg::wal-replication-slot-path "authority_required"))
              (staging (merge-pathnames "state.tmp" (uiop:pathname-directory-pathname path)))
              (before (pg::read-bounded-regular-file path 4096))
              (bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(7 8 9)))
              (registry pg::*wal-replication-slots*))
         (with-open-file (stream staging :direction :output :if-exists :error
                                         :element-type '(unsigned-byte 8))
           (write-sequence bytes stream))
         (ok (intake-fails-p #'pg::wal-load-replication-slots))
         (ok (probe-file staging))
         (ok (equalp bytes (pg::read-bounded-regular-file staging 3)))
         (ok (equalp before (pg::read-bounded-regular-file path 4096)))
         (ok (eq registry pg::*wal-replication-slots*))
         (delete-file staging)))))

(deftest startup-slot-source-rejects-copies-other-threads-and-expired-scope
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (pg::create-standard-logical-slot "scoped_source" :nothing)
       (let ((saved nil))
         (pg::call-with-logical-slot-owned-startup-test
          (lambda (gate-owner ownership)
            (pg::call-with-logical-slot-startup-source
             gate-owner ownership (pg::logical-slot-startup-read-inventory)
             (lambda (source)
               (setf saved source)
               (ok (eq source (pg::logical-slot-startup-check-live source)))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::logical-slot-startup-check-live
                       (pg::copy-logical-slot-startup-source source)))))
               (let ((thread
                       (sb-thread:make-thread
                        (lambda ()
                          (intake-fails-p
                           (lambda () (pg::logical-slot-startup-check-live source)))))))
                 (ok (sb-thread:join-thread thread)))
               (ok (eq source (pg::logical-slot-startup-check-live source)))))))
         (ok (not (pg::logical-slot-startup-source-active-p saved)))
         (ok (intake-fails-p
              (lambda () (pg::logical-slot-startup-check-live saved))))))))

(deftest startup-slot-source-change-permanently-revokes-capability
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (pg::create-standard-logical-slot "frozen_source" :nothing)
       (pg::call-with-logical-slot-owned-startup-test
        (lambda (gate-owner ownership)
          (dolist (binding (list (cons 'pg::*max-replication-slots*
                                       (1+ pg::*max-replication-slots*))
                                 (cons 'pg::*wal-durable-lsn* (1+ pg::*wal-durable-lsn*))
                                 (cons 'pg::*wal-storage-incarnation* (gensym "OTHER-STORE"))))
            (pg::call-with-logical-slot-startup-source
             gate-owner ownership (pg::logical-slot-startup-read-inventory)
             (lambda (source)
               (sb-thread:with-mutex (pg::*wal-lock*)
                 (ok (pg::logical-slot-startup-check-source source))
                 (progv (list (car binding)) (list (cdr binding))
                   (ok (intake-fails-p
                        (lambda () (pg::logical-slot-startup-check-source source)))))
                 (ok (not (pg::logical-slot-startup-source-active-p source)))
                 (ok (intake-fails-p
                      (lambda () (pg::logical-slot-startup-check-source source)))))))))))))

(deftest startup-slot-real-owner-rejects-foreign-reserved-and-lost-images
    (dolist (cause '(:none :wal-removed))
      (pg::call-with-logical-slot-real-startup-test
       (lambda ()
         (pg::create-standard-logical-slot "foreign_image" :nothing)
         (let* ((registry pg::*wal-replication-slots*)
                (original (gethash "foreign_image" registry))
                (provenance (pg::wal-replication-slot-logical-provenance original))
                (mirror pg::*logical-capture-live-slots*)
                (slot
                  (pg::make-wal-replication-slot
                   :name "foreign_image"
                   :database-oid (pg::wal-replication-slot-database-oid original)
                   :restart-lsn (pg::wal-replication-slot-restart-lsn original)
                   :consistent-lsn (pg::wal-replication-slot-consistent-lsn original)
                   :confirmed-flush-lsn (pg::wal-replication-slot-confirmed-flush-lsn original)
                   :plugin (pg::wal-replication-slot-plugin original)
                   :restart-timeline (pg::wal-replication-slot-restart-timeline original)
                   :confirmed-timeline (pg::wal-replication-slot-confirmed-timeline original)
                   :invalidation-cause cause :wal-status (if (eq cause :none) :reserved :lost)
                   :logical-provenance
                   (pg::make-logical-slot-provenance
                    (1+ (pg::logical-slot-provenance-system-id provenance))
                    (pg::logical-slot-provenance-generation provenance)
                    (pg::wal-replication-slot-consistent-lsn original)
                    (pg::logical-slot-provenance-birth-timeline provenance)
                    (pg::logical-slot-provenance-on-start-lsn provenance)
                    (pg::logical-slot-provenance-on-start-timeline provenance)))))
           (pg::wal-slot-set-initial-profile slot (pg::wal-slot-profile original))
           (pg::wal-atomic-write-bytes (pg::wal-replication-slot-path "foreign_image")
                                       (pg::wal-encode-replication-slot slot))
           (pg::call-with-logical-slot-owned-startup-test
            (lambda (gate-owner ownership)
              (ok (intake-fails-p
                   (lambda ()
                     (pg::wal-load-replication-slots :gate-owner gate-owner :ownership ownership))))))
           (ok (eq registry pg::*wal-replication-slots*))
           (ok (= mirror pg::*logical-capture-live-slots*)))))))

(deftest startup-slot-real-wal-ignores-open-and-abandoned-batch-pages
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (startup-slot-test-sql "CREATE TABLE startup_tail (id int)")
       (pg::create-standard-logical-slot "tail_restore" :nothing)
       (startup-slot-test-sql "INSERT INTO startup_tail VALUES (1)")
       (let ((expected (startup-slot-test-pending (gethash "tail_restore" pg::*wal-replication-slots*))))
         (flet ((append-pages (xid)
                  (pg::wal-append-record
                   pg::+wal-record-pages+ 0
                   (pg::wal-record-payload
                    (startup-slot-test-progress-record pg::+wal-record-pages+
                                                       pg::*wal-end-lsn* :xids (list xid)))))
                (restore ()
                  (pg::call-with-logical-slot-owned-startup-test
                   (lambda (gate-owner ownership)
                     (pg::wal-load-replication-slots :gate-owner gate-owner :ownership ownership)))))
           (sb-thread:with-mutex (pg::*wal-lock*)
             (pg::wal-append-batch-start)
             (append-pages 999)
             (pg::wal-flush-core))
           (restore)
           (ok (equal expected (startup-slot-test-pending
                                (gethash "tail_restore" pg::*wal-replication-slots*))))
           (let ((first nil) (end nil))
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf first pg::*wal-end-lsn*)
               (pg::wal-append-batch-start)
               (append-pages 1000)
               (pg::wal-append-record pg::+wal-record-commit+ 1000 (pg::wal-encode-commit-timestamp 1))
               (pg::wal-flush-core)
               (setf end pg::*wal-durable-lsn*))
             (restore)
             (let ((pending (startup-slot-test-pending
                             (gethash "tail_restore" pg::*wal-replication-slots*))))
               (ok (equal (append expected (list (list 1000 first end))) pending))
               (ok (not (assoc 999 pending))))))))))

(deftest startup-slot-private-archive-lease-does-not-authorize-ancestral-publication
    (call-with-historical-segment-source
     (lambda (fixture)
       (let* ((pg::*wal-ready* t) (pg::*wal-control-metadata* (getf fixture :metadata))
                                  (pg::*primary-wal-writer* nil)
                                  (pg::*postmaster-data-lock* nil) (pg::*postmaster-pid-stream* nil)
                                  (pg::*postmaster-pid-directory* nil) (pg::*postmaster-pid-inode* nil)
                                  (saved-exit-hooks sb-ext:*exit-hooks*)
                                  (registry pg::*wal-replication-slots*)
                                  (initial (hash-table-count pg::*physical-retained-root-references*))
                                  (escaped nil) (saved-source nil)
                                  (slot (pg::make-wal-replication-slot
                                         :name "ancestral_private" :database-oid 7 :plugin "pgoutput"
                                         :invalidation-cause :none
                                         :restart-lsn 96 :restart-timeline 1
                                         :consistent-lsn 96 :confirmed-flush-lsn 96 :confirmed-timeline 1
                                         :logical-provenance
                                         (pg::make-logical-slot-provenance
                                          (pg::catalog-system-identifier) 1 96 1 96 1))))
         (unwind-protect
              (progn
                (ensure-directories-exist (pg::catalog-path))
                (pg::write-catalog-image (pg::catalog-image-bytes))
                (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
                (pg::wal-slot-set-initial-profile slot :text)
                (pg::wal-create-replication-slot-file slot)
                (sb-posix:chmod (namestring pg::*pgdata*) #o700)
                (pg::postmaster-own-data-directory "127.0.0.1" 5439)
                (let ((pg::*catalog* nil))
                  (pg::call-with-logical-slot-owned-startup-test
                   (lambda (gate-owner ownership)
                     (pg::call-with-logical-slot-startup-source
                      gate-owner ownership (pg::logical-slot-startup-read-inventory)
                      (lambda (source)
                        (setf saved-source source)
                        (ok (intake-fails-p
                             (lambda () (pg::logical-slot-startup-check-eligibility source slot))))
                        (ok (eq :escaped
                                (catch 'startup-archive-exit
                                  (pg::call-with-logical-slot-startup-archive
                                   source
                                   (lambda (archive)
                                     (setf escaped archive)
                                     (ok (= (1+ initial)
                                            (hash-table-count pg::*physical-retained-root-references*)))
                                     (ok (pg::logical-wal-archive-prepare archive 0 1))
                                     (ok (equalp (getf fixture :first)
                                                 (pg::logical-wal-archive-cache-bytes archive)))
                                     (throw 'startup-archive-exit :escaped))))))
                        (ok (= initial (hash-table-count pg::*physical-retained-root-references*)))
                        (ok (null (pg::logical-wal-archive-cache-bytes escaped)))
                        (ok (null (pg::logical-wal-archive-index escaped)))
                        (ok (intake-fails-p
                             (lambda () (pg::logical-wal-archive-prepare escaped 0 1))))))))
                  (ok (null pg::*catalog*)))
                (ok (not (pg::logical-slot-startup-source-active-p saved-source)))
                (ok (eq registry pg::*wal-replication-slots*))
                (ok (pg::physical-retained-root-protected-p
                     (getf fixture :directory) :inventory
                     (getf (getf fixture :reference) :root-id))))
           (pg::postmaster-remove-pid-file)
           (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
           (setf sb-ext:*exit-hooks* saved-exit-hooks))))
     :cursor-records t))

(deftest startup-slot-catalog-replacement-before-publication-keeps-live-state
    (pg::call-with-logical-slot-real-startup-test
     (lambda ()
       (pg::create-standard-logical-slot "catalog_race" :nothing)
       (let ((registry pg::*wal-replication-slots*) (count pg::*logical-capture-live-slots*)
                                                    (policy pg::*wal-logical-policy*) (disable pg::*wal-logical-info-disable-pending*)
                                                    (catalog-bytes (pg::catalog-image-bytes))
                                                    (original (symbol-function 'pg::logical-slot-startup-prepare-mirrors))
                                                    (injected nil) (caught nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::logical-slot-startup-prepare-mirrors)
                      (lambda (restored)
                        (multiple-value-prog1 (funcall original restored)
                          (let ((pg::*catalog* (pg::make-catalog :cluster-identity "replacement-cluster")))
                            (pg::write-catalog-image (pg::catalog-image-bytes)))
                          (setf injected t))))
                (handler-case
                    (pg::call-with-logical-slot-owned-startup-test
                     (lambda (gate-owner ownership)
                       (pg::wal-load-replication-slots :gate-owner gate-owner :ownership ownership)))
                  (error (condition) (setf caught condition))))
           (setf (symbol-function 'pg::logical-slot-startup-prepare-mirrors) original)
           (pg::write-catalog-image catalog-bytes))
         (ok injected)
         (ok (and caught (search "logical slot startup source changed" (princ-to-string caught))))
         (ok (eq registry pg::*wal-replication-slots*))
         (ok (= count pg::*logical-capture-live-slots*))
         (ok (eq policy pg::*wal-logical-policy*))
         (ok (eq disable pg::*wal-logical-info-disable-pending*))))))

(defun startup-on-test-state (&key (enabled t) (on 65) (owner 1) (end 300) (timeline 1))
  (pg::%make-wal-logical-status enabled (- end 33) end timeline nil nil
                                (and enabled on) (and enabled on owner)))

(cl-spec:defgenerator startup-on-coverage-arguments ()
  (list (startup-on-test-state)
        (list (pg::make-timeline-history-entry :timeline 1 :begin 0))
        (1+ (random 330)) 1 :prefix-end 300 1))

(cl-spec:defspec-function pg::logical-slot-on-coverage
    (:args (state (type pg::wal-logical-status)) (history (type list))
           (required (type integer)) (owner (type integer)) (role (type keyword))
           (through (type integer)) (through-owner (type integer)))
  (:args-generator startup-on-coverage-arguments)
  (:returns (member :proved-on :insufficient :unknown))
  (:post (eq result (cond ((> required through) :unknown)
                          ((< required 65) :insufficient)
                          (t :proved-on)))))

(cl-spec:defproperty startup-on-coverage-keeps-unknown-ahead-of-replay
    ((required (range integer 1 330)))
  (:about pg::logical-slot-on-coverage)
  (:trials (:normal 200))
  (eq (cond ((> required 300) :unknown) ((< required 65) :insufficient) (t :proved-on))
      (pg::logical-slot-on-coverage
       (startup-on-test-state) (list (pg::make-timeline-history-entry :timeline 1 :begin 0))
       required 1 :prefix-end 300 1)))

(deftest startup-on-coverage-generated-contracts
    (dolist (result
             (list (cl-spec:check-function 'pg::logical-slot-on-coverage :trials 200 :seed 2304)
                   (cl-spec:run-property 'startup-on-coverage-keeps-unknown-ahead-of-replay :seed 2304)))
      (let ((data (cl-spec:result-data result)))
        (ok (eq :passed (getf data :status)) (format nil "~s" data)))))

(deftest startup-on-coverage-distinguishes-off-unknown-and-later-on
    (let ((history (list (pg::make-timeline-history-entry :timeline 1 :begin 0))))
      (flet ((classify (state required)
               (pg::logical-slot-on-coverage state history required 1 :prefix-end 300 1)))
        (ok (eq :unknown (classify nil 100)))
        (ok (eq :unknown (classify (startup-on-test-state :on nil) 100)))
        (ok (eq :insufficient (classify (startup-on-test-state :enabled nil) 100)))
        (ok (eq :unknown (classify (startup-on-test-state :enabled nil) 301)))
        (ok (eq :insufficient (classify (startup-on-test-state :on 165) 100)))
        (ok (eq :proved-on (classify (startup-on-test-state) 65)))
        (ok (eq :proved-on (classify (startup-on-test-state) 300)))
        (ok (eq :insufficient (classify (startup-on-test-state) 32)))
        (ok (eq :proved-on (classify (startup-on-test-state) 100))))))

(deftest startup-on-coverage-keeps-fork-position-roles
    (let ((history (startup-slot-test-history 100))
          (parent (startup-on-test-state :end 100)))
      (ok (eq :proved-on (pg::logical-slot-on-coverage parent history 100 1 :prefix-end 100 1)))
      (ok (eq :unknown (pg::logical-slot-on-coverage parent history 100 2 :record-start 100 1)))
      (ok (eq :unknown
              (pg::logical-slot-on-coverage (startup-on-test-state :enabled nil :end 100)
                                            history 100 2 :record-start 100 1)))
      (ok (eq :proved-on
              (pg::logical-slot-on-coverage (startup-on-test-state :on 100 :timeline 2)
                                            history 100 2 :record-start 300 2)))
      (dolist (arguments
               (list (list parent history 100 2 :prefix-end 100 1)
                     (list parent history 100 1 :record-start 100 1)
                     (list parent history 100 1 :prefix-end 100 2)
                     (list (startup-on-test-state :on 100 :owner 2 :timeline 2)
                           history 100 2 :record-start 300 2)
                     (list (startup-on-test-state :on 103 :owner 2 :timeline 2)
                           history 200 2 :prefix-end 300 2)))
        (ok (intake-fails-p (lambda () (apply #'pg::logical-slot-on-coverage arguments)))))))

(deftest startup-on-coverage-uses-uninterrupted-origin-across-real-status-folds
    (let* ((history (list (pg::make-timeline-history-entry :timeline 1 :begin 0)))
           (first (pg::wal-logical-status-next
                   nil (pg::make-wal-record
                        :version 8 :type pg::+wal-record-logical-decoding-status-change+
                        :xid 0 :start 32 :end 65
                        :payload (pg::wal-encode-logical-decoding-status-change t)) 1))
           (repeated (pg::wal-logical-status-next
                      first (pg::make-wal-record
                             :version 8 :type pg::+wal-record-logical-decoding-status-change+
                             :xid 0 :start 100 :end 133
                             :payload (pg::wal-encode-logical-decoding-status-change t)) 1))
           (off (pg::wal-logical-status-next
                 repeated (pg::make-wal-record
                           :version 8 :type pg::+wal-record-logical-decoding-status-change+
                           :xid 0 :start 200 :end 233
                           :payload (pg::wal-encode-logical-decoding-status-change nil)) 1))
           (later (pg::wal-logical-status-next
                   off (pg::make-wal-record
                        :version 8 :type pg::+wal-record-logical-decoding-status-change+
                        :xid 0 :start 240 :end 273
                        :payload (pg::wal-encode-logical-decoding-status-change t)) 1)))
      (ok (= 65 (pg::wal-logical-status-on-start-lsn repeated)))
      (ok (= 133 (pg::wal-logical-status-last-enable-lsn repeated)))
      (ok (eq :proved-on (pg::logical-slot-on-coverage repeated history 100 1 :prefix-end 133 1)))
      (ok (eq :insufficient (pg::logical-slot-on-coverage repeated history 32 1 :record-start 133 1)))
      (ok (eq :insufficient (pg::logical-slot-on-coverage off history 100 1 :prefix-end 233 1)))
      (ok (eq :insufficient (pg::logical-slot-on-coverage later history 100 1 :prefix-end 273 1)))
      (let* ((unknown (startup-on-test-state :on nil :end 152))
             (still-unknown (pg::wal-logical-status-next
                             unknown (pg::make-wal-record
                                      :version 8 :type pg::+wal-record-logical-decoding-status-change+
                                      :xid 0 :start 160 :end 193
                                      :payload (pg::wal-encode-logical-decoding-status-change t)) 1)))
        (ok (eq :unknown
                (pg::logical-slot-on-coverage still-unknown history 170 1 :prefix-end 193 1))))))

(deftest startup-on-coverage-checks-full-width-and-malformed-witnesses
    (let* ((history (list (pg::make-timeline-history-entry :timeline 1 :begin 0)))
           (maximum 18446744073709551615)
           (state (startup-on-test-state :on maximum :end maximum)))
      (ok (eq :proved-on
              (pg::logical-slot-on-coverage state history maximum 1 :prefix-end maximum 1)))
      (dolist (arguments
               (list (list nil history 0 1 :prefix-end 300 1)
                     (list nil history 1 1 :other 300 1)
                     (list nil history 1 1 :prefix-end 0 1)
                     (list nil history -1 1 :record-start 300 1)
                     (list nil history (1+ maximum) 1 :prefix-end maximum 1)
                     (list 42 history 1 1 :prefix-end 300 1)
                     (list (startup-on-test-state :on (1+ pg::+wal-segment-size+)
                                                  :end (+ pg::+wal-segment-size+ 200))
                           history (+ pg::+wal-segment-size+ 100) 1 :prefix-end
                           (+ pg::+wal-segment-size+ 200) 1)
                     (list (startup-on-test-state :end (1+ pg::+wal-segment-size+))
                           history pg::+wal-segment-size+ 1 :prefix-end
                           (1+ pg::+wal-segment-size+) 1)
                     (list (startup-on-test-state :end 301) history 350 1 :prefix-end 300 1)))
        (ok (intake-fails-p (lambda () (apply #'pg::logical-slot-on-coverage arguments)))))))
