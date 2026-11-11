;;;; wal-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(defun call-with-wal-scan-test-files (segments durable-lsn function)
  "Run FUNCTION with isolated WAL files and a known durable boundary.
Each entry in SEGMENTS contains a segment number followed by its byte vectors."
  (let* ((*pgdata* (merge-pathnames
                    (format nil "pg-wal-scan-~d-~d/" (sb-posix:getpid) (random 1000000))
                    (uiop:temporary-directory)))
         (*wal-checkpoint-lsn* 0)
         (*wal-checkpoint-snapshot* nil)
         (*wal-durable-lsn* durable-lsn)
         (*wal-insert-timeline* 1)
         (*wal-segment-namespace* :legacy))
    (ensure-directories-exist (wal-directory))
    (unwind-protect
         (progn
           (dolist (entry segments)
             (with-open-file (stream (wal-segment-path (first entry))
                                     :direction :output
                                     :element-type '(unsigned-byte 8)
                                     :if-exists :error)
               (dolist (bytes (rest entry))
                 (write-sequence bytes stream))))
           (funcall function))
      (uiop:delete-directory-tree *pgdata* :validate t :if-does-not-exist :ignore))))

(declaim (special *clog-pending-commits* *wal-storage-incarnation*))

(defun call-with-pending-clog-test (function)
  (let* ((*pgdata* (merge-pathnames
                    (format nil "pg-pending-clog-~d-~d/" (sb-posix:getpid) (random 1000000))
                    (uiop:temporary-directory)))
         (*clog-stream* nil)
         (*clog-pending-commits* (make-hash-table :test #'eql))
         (*wal-storage-incarnation* (gensym "TEST-WAL-STORE-"))
         (*wal-ready* t) (*wal-poisoned* nil) (*physical-recovery-failed-p* nil)
         (*wal-insert-timeline* 1) (*wal-end-lsn* 200) (*wal-durable-lsn* 100))
    (declare (special *clog-pending-commits* *wal-storage-incarnation*))
    (ensure-directories-exist *pgdata*)
    (unwind-protect
         (progn (clog-init) (funcall function))
      (when *clog-stream* (ignore-errors (close *clog-stream*)))
      (uiop:delete-directory-tree *pgdata* :validate t :if-does-not-exist :ignore))))

(defun pending-clog-raw-status (xid)
  (with-open-file (in (clog-path) :element-type '(unsigned-byte 8))
    (file-position in (clog-byte-index xid))
    (ldb (byte 2 (clog-bit-shift xid)) (read-byte in nil 0))))

(defun pending-clog-reservation-result (slot)
  (call-with-pending-clog-test
   (lambda ()
     (let* ((xid (+ 12 slot))
            (neighbor (+ 12 (mod (1+ slot) 4)))
            (owner (list :prepared-owner))
            (completion nil))
       (sb-thread:with-mutex (*wal-lock*)
         (setf completion (clog-reserve-commit-under-wal-lock xid 240 owner)))
       (clog-set neighbor +clog-aborted+ :fsync t)
       (clog-fsync)
       (and (= +clog-in-progress+ (clog-get xid))
            (= +clog-in-progress+ (pending-clog-raw-status xid))
            (= +clog-aborted+ (pending-clog-raw-status neighbor))
            (eq owner (clog-commit-completion-owner completion))
            (eq :reserved (clog-commit-completion-state completion))
            (handler-case (progn (clog-set xid +clog-aborted+) nil)
              (wal-input-validation-error () t))
            (handler-case (progn (clog-init) nil)
              (wal-input-validation-error () t))
            (sb-thread:with-mutex (*wal-lock*)
              (clog-cancel-reserved-commit-under-wal-lock completion owner)
              (zerop (hash-table-count *clog-pending-commits*))))))))

(cl-spec:defproperty reserved-clog-neighbor-writes-remain-in-progress
    ((slot (range integer 0 3)))
  (:about clog-reserve-commit-under-wal-lock)
  (:trials (:normal 100))
  (pending-clog-reservation-result slot))

(rove:deftest reserved-clog-is-invisible-but-excludes-storage-writers
    (unit-check-property 'reserved-clog-neighbor-writes-remain-in-progress))

(rove:deftest reserved-clog-retains-ambiguous-terminal-attempt
    (call-with-pending-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((owner (list :prepared-owner))
                (completion (clog-reserve-commit-under-wal-lock 12 240 owner)))
           (rove:ok
            (handler-case
                (progn (clog-check-commit-boundary-under-wal-lock 12 240) nil)
              (wal-input-validation-error () t)))
           (clog-begin-reserved-commit-under-wal-lock completion owner)
           (rove:ok
            (handler-case
                (progn (clog-cancel-reserved-commit-under-wal-lock completion owner) nil)
              (wal-input-validation-error () t)))
           (rove:ok (eq completion (gethash 12 *clog-pending-commits*)))
           (rove:ok (eq owner (clog-commit-completion-owner completion)))
           (rove:ok (= +clog-in-progress+ (clog-get 12)))
           (setf *wal-end-lsn* 241)
           (rove:ok
            (handler-case
                (progn (clog-publish-reserved-commit-under-wal-lock completion owner 240) nil)
              (wal-input-validation-error () t)))
           (rove:ok (eq :inserting (clog-commit-completion-state completion)))
           (setf *wal-end-lsn* 240)
           (clog-publish-reserved-commit-under-wal-lock completion owner 240)
           (rove:ok (= +clog-in-progress+ (clog-get 12)))
           (rove:ok
            (handler-case
                (progn (clog-complete-reserved-commit-under-wal-lock completion owner) nil)
              (wal-input-validation-error () t)))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 12)))
           (setf *wal-durable-lsn* 240)
           (clog-complete-reserved-commit-under-wal-lock completion owner)
           (rove:ok (eq :materialized (clog-commit-completion-state completion)))
           (rove:ok (zerop (hash-table-count *clog-pending-commits*)))
           (rove:ok (= +clog-committed+ (pending-clog-raw-status 12)))
           (clog-complete-reserved-commit-under-wal-lock completion owner))))))

(rove:deftest reserved-clog-write-failure-retains-completion-for-retry
    (call-with-pending-clog-test
     (lambda ()
       (let ((write (symbol-function 'clog-write-status-under-lock))
             (calls 0))
         (unwind-protect
              (sb-thread:with-mutex (*wal-lock*)
                (let* ((owner (list :prepared-owner))
                       (completion (clog-reserve-commit-under-wal-lock 12 240 owner))
                       (failure (make-condition 'simple-error :format-control "CLOG write failure")))
                  (clog-begin-reserved-commit-under-wal-lock completion owner)
                  (setf *wal-end-lsn* 240 *wal-durable-lsn* 240)
                  (clog-publish-reserved-commit-under-wal-lock completion owner 240)
                  (setf (symbol-function 'clog-write-status-under-lock)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (incf calls)
                          (error failure)))
                  (rove:ok
                   (eq failure
                       (handler-case
                           (clog-complete-reserved-commit-under-wal-lock completion owner)
                         (error (condition) condition))))
                  (rove:ok (eq completion (gethash 12 *clog-pending-commits*)))
                  (rove:ok (eq :committed (clog-commit-completion-state completion)))
                  (rove:ok (= +clog-committed+ (clog-get 12)))
                  (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 12)))
                  (setf (symbol-function 'clog-write-status-under-lock)
                        (lambda (&rest arguments) (incf calls) (apply write arguments)))
                  (clog-complete-reserved-commit-under-wal-lock completion owner)
                  (clog-complete-reserved-commit-under-wal-lock completion owner)
                  (rove:ok (= calls 2))
                  (rove:ok (eq :materialized (clog-commit-completion-state completion)))
                  (rove:ok (zerop (hash-table-count *clog-pending-commits*)))))
           (setf (symbol-function 'clog-write-status-under-lock) write))))))

(rove:deftest reserved-clog-rejects-foreign-identity-and-conflicting-slot
    (call-with-pending-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((owner (list :prepared-owner))
                (completion (clog-reserve-commit-under-wal-lock 12 240 owner))
                (token *wal-storage-incarnation*))
           (dolist (fault '(:owner :alias :end :store :timeline))
             (let ((*wal-storage-incarnation* (if (eq fault :store) (gensym "OTHER-") token))
                   (*wal-insert-timeline* (if (eq fault :timeline) 2 1)))
               (rove:ok
                (handler-case
                    (progn
                      (case fault
                        (:alias (clog-reserve-commit-under-wal-lock (+ 12 (ash 1 32)) 240 owner))
                        (:end (clog-reserve-commit-under-wal-lock 12 241 owner))
                        (t (clog-begin-reserved-commit-under-wal-lock
                            completion (if (eq fault :owner) (list :foreign) owner))))
                      nil)
                  (wal-input-validation-error () t)))))
           (rove:ok (eq :reserved (clog-commit-completion-state completion)))
           (clog-cancel-reserved-commit-under-wal-lock completion owner))))))

(defun pending-clog-neighbor-result (slot status)
  (call-with-pending-clog-test
   (lambda ()
     (let ((xid (+ 12 slot)) (neighbor (+ 12 (mod (1+ slot) 4))))
       (clog-set xid +clog-in-progress+ :fsync t)
       (let ((tag (sb-thread:with-mutex (*wal-lock*)
                    (clog-stage-commit-under-wal-lock xid 140))))
         (clog-set neighbor status :fsync t)
         (clog-fsync)
         (and (= +clog-committed+ (clog-get xid))
              (= +clog-committed+ (clog-get (+ xid (ash 1 32))))
              (= +clog-in-progress+ (pending-clog-raw-status xid))
              (= status (pending-clog-raw-status neighbor))
              (eq tag (clog-commit-completion-tag (gethash (xid32-of xid) *clog-pending-commits*)))))))))

(cl-spec:defproperty pending-clog-neighbor-writes-preserve-wal-dependency
    ((slot (range integer 0 3)) (status (range integer 1 2)))
  (:about clog-stage-commit-under-wal-lock)
  (:trials (:normal 100))
  (pending-clog-neighbor-result slot status))

(rove:deftest pending-clog-logical-visibility-does-not-write-commit-bits
    (unit-check-property 'pending-clog-neighbor-writes-preserve-wal-dependency))

(rove:deftest pending-clog-materializes-only-its-exact-durable-tag
    (call-with-pending-clog-test
     (lambda ()
       (clog-set 13 +clog-aborted+ :fsync t)
       (sb-thread:with-mutex (*wal-lock*)
         (let ((tag (clog-stage-commit-under-wal-lock 12 140)))
           (rove:ok (handler-case (progn (clog-materialize-commit-under-wal-lock tag) nil)
                      (wal-input-validation-error () t)))
           (rove:ok (eq tag (clog-commit-completion-tag (gethash 12 *clog-pending-commits*))))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 12)))
           (setf *wal-durable-lsn* 140)
           (clog-materialize-commit-under-wal-lock tag)
           (rove:ok (zerop (hash-table-count *clog-pending-commits*)))
           (rove:ok (= +clog-committed+ (clog-get 12)))
           (rove:ok (= +clog-committed+ (pending-clog-raw-status 12)))
           (rove:ok (= +clog-aborted+ (pending-clog-raw-status 13))))))))

(rove:deftest pending-clog-rejects-stale-incarnations-timelines-and-aliases
    (call-with-pending-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((xid (+ 12 (ash 1 32)))
                (tag (clog-stage-commit-under-wal-lock xid 140))
                (original *wal-storage-incarnation*))
           (setf *wal-durable-lsn* 200)
           (dolist (fault '(:incarnation :timeline :identity :alias :end))
             (let ((*wal-storage-incarnation* (if (eq fault :incarnation) (gensym "OTHER-") original))
                   (*wal-insert-timeline* (if (eq fault :timeline) 2 1)))
               (rove:ok
                (handler-case
                    (progn
                      (case fault
                        (:alias (clog-stage-commit-under-wal-lock 12 140))
                        (:end (clog-stage-commit-under-wal-lock xid 141))
                        (:identity
                         (clog-materialize-commit-under-wal-lock
                          (%make-clog-commit-tag xid original 1 140)))
                        (t (clog-materialize-commit-under-wal-lock tag)))
                      nil)
                  (wal-input-validation-error () t)))))
           (rove:ok (eq tag (clog-commit-completion-tag (gethash 12 *clog-pending-commits*))))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 12)))
           (clog-materialize-commit-under-wal-lock tag))))))

(rove:deftest pending-clog-rejects-direct-overwrite-and-invalid-boundaries
    (call-with-pending-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (dolist (entry '((2 140) (12 0) (12 201) (4294967296 140)))
           (rove:ok (handler-case
                        (progn (apply #'clog-stage-commit-under-wal-lock entry) nil)
                      (wal-input-validation-error () t))))
         (rove:ok (zerop (hash-table-count *clog-pending-commits*)))
         (clog-stage-commit-under-wal-lock 12 140))
       (dolist (xid (list 12 (+ 12 (ash 1 32))))
         (dolist (status (list +clog-committed+ +clog-aborted+))
           (rove:ok (handler-case (progn (clog-set xid status :fsync t) nil)
                      (wal-input-validation-error () t)))))
       (rove:ok (= +clog-committed+ (clog-get 12)))
       (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 12))))))

(rove:deftest pending-clog-write-and-fsync-failures-retain-the-owned-tag
    (dolist (phase '(:before-write :after-write :fsync :finish))
      (call-with-pending-clog-test
       (lambda ()
         (let ((write (symbol-function 'clog-write-status-under-lock))
               (sync (symbol-function 'fsync-stream)))
           (unwind-protect
                (sb-thread:with-mutex (*wal-lock*)
                  (let ((tag (clog-stage-commit-under-wal-lock 12 140)))
                    (setf *wal-durable-lsn* 200)
                    (if (eq phase :fsync)
                        (setf (symbol-function 'fsync-stream)
                              (lambda (stream) (declare (ignore stream)) (error "CLOG fsync failure")))
                        (setf (symbol-function 'clog-write-status-under-lock)
                              (lambda (&rest arguments)
                                (when (member phase '(:after-write :finish)) (apply write arguments))
                                (if (eq phase :finish)
                                    (close *clog-stream*)
                                    (error "CLOG byte write failure")))))
                    (rove:ok
                     (handler-case
                         (progn
                           (clog-materialize-commit-under-wal-lock tag :fsync (not (eq phase :finish)))
                           nil)
                       (error (condition)
                         (if (eq phase :finish) (typep condition 'stream-error) t))))
                    (rove:ok (eq tag (clog-commit-completion-tag (gethash 12 *clog-pending-commits*))))
                    (rove:ok (= +clog-committed+ (clog-get 12)))
                    (setf (symbol-function 'clog-write-status-under-lock) write
                          (symbol-function 'fsync-stream) sync)
                    (unless (eq phase :finish)
                      (clog-materialize-commit-under-wal-lock tag)
                      (rove:ok (zerop (hash-table-count *clog-pending-commits*))))))
             (setf (symbol-function 'clog-write-status-under-lock) write
                   (symbol-function 'fsync-stream) sync)))))))

(rove:deftest pending-clog-blocks-destructive-lifecycle-before-effects
    (call-with-pending-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*) (clog-stage-commit-under-wal-lock 12 140))
       (let ((stream *clog-stream*) (token *wal-storage-incarnation*)
                                    (append (symbol-function 'wal-append-record)) (appended nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-append-record)
                      (lambda (&rest arguments) (declare (ignore arguments)) (setf appended t)))
                (dolist (operation (list #'clog-init #'clog-reset #'wal-init #'wal-checkpoint-core
                                         #'physical-recovery-open-storage
                                         #'physical-promotion-adopt-live-child-runtime
                                         (lambda () (physical-promotion-restore-cold-state nil nil))))
                  (rove:ok (handler-case (progn (funcall operation) nil)
                             (wal-input-validation-error () t)))
                  (rove:ok (eq token *wal-storage-incarnation*))
                  (rove:ok (eq stream *clog-stream*))
                  (rove:ok (open-stream-p stream))
                  (rove:ok (not appended)))
                (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 12))))
           (setf (symbol-function 'wal-append-record) append))))))

(rove:deftest clog-reset-holds-wal-through-file-replacement
    (call-with-pending-clog-test
     (lambda ()
       (let ((record (symbol-function 'wal-log-clog-reset-under-lock))
             (attempted (sb-thread:make-semaphore))
             (acquired (sb-thread:make-semaphore))
             (waiter nil) (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-log-clog-reset-under-lock)
                      (lambda ()
                        (rove:ok (sb-thread:holding-mutex-p *wal-lock*))
                        (incf calls)))
                (clog-reset
                 :fsync-directory-function
                 (lambda (path)
                   (fsync-directory path)
                   (rove:ok (sb-thread:holding-mutex-p *wal-lock*))
                   (setf waiter
                         (sb-thread:make-thread
                          (lambda ()
                            (sb-thread:signal-semaphore attempted)
                            (sb-thread:with-mutex (*wal-lock*)
                              (sb-thread:signal-semaphore acquired)))
                          :name "CLOG reset competing WAL owner"))
                   (rove:ok (sb-thread:wait-on-semaphore attempted :timeout 2))
                   (rove:ok (not (sb-thread:wait-on-semaphore acquired :timeout 0.05)))))
                (rove:ok (= 1 calls))
                (rove:ok (sb-thread:wait-on-semaphore acquired :timeout 2))
                (sb-thread:join-thread waiter))
           (setf (symbol-function 'wal-log-clog-reset-under-lock) record)
           (when (and waiter (sb-thread:thread-alive-p waiter))
             (sb-thread:join-thread waiter :timeout 2 :default :timeout)))))))

(declaim (special *wal-pending-page-publication*))

(defun dispose-page-publication-test-owner (owner)
  (when owner
    (flet ((dispose ()
             (assert (eq owner *wal-pending-page-publication*))
             (loop while *wal-pending-page-publication*
                   for current = *wal-pending-page-publication*
                   do (wal-close-primary-page-spool (wal-page-publication-spool current))
                      (let ((stores nil))
                        (maphash (lambda (key reference)
                                   (when (wal-dirty-reference-p reference)
                                     (pushnew (wal-dirty-reference-store reference) stores))
                                   (wal-dirty-release-reference reference)
                                   (remhash key (wal-page-publication-pages current))
                                   (remhash key (wal-page-publication-contributors current)))
                                 (wal-page-publication-pages current))
                        (dolist (store stores)
                          (let ((*wal-dirty-store* store)) (wal-dirty-close-empty-store))))
                      (setf *wal-pending-page-publication* (wal-page-publication-next current)
                            (wal-page-publication-next current) nil
                            (wal-page-publication-state current) :released))
             (setf *wal-pending-page-publication-tail* nil)))
      (if (sb-thread:holding-mutex-p *wal-lock*)
          (dispose)
          (sb-thread:with-mutex (*wal-lock*) (dispose)))))
  (values))

(defun clear-wal-test-page-cache ()
  (sb-thread:with-mutex (*wal-lock*) (wal-clear-page-cache)))

(defun wal-test-read-dirty-reference (key reference)
  (sb-thread:with-mutex (*wal-lock*) (wal-dirty-read-reference key reference)))

(defun call-with-page-publication-test (function &key (resident-limit 0))
  (let* ((*pgdata* (merge-pathnames
                    (format nil "pg-page-owner-~d-~d/" (sb-posix:getpid) (random 1000000))
                    (uiop:temporary-directory)))
         (*clog-stream* nil) (*clog-pending-commits* (make-hash-table :test #'eql))
         (*query-temp-directory* *pgdata*) (*query-temp-file-limit-bytes* 0)
         (*logical-capture-memory-bytes* 0)
         (*relation-streams* (make-hash-table :test #'equal))
         (*wal-dirty-store* nil) (*wal-dirty-resident-limit-pages* resident-limit)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (*wal-page-batch* nil) (*wal-page-batch-xids* nil) (*wal-page-batch-split* nil)
         (*wal-pending-btree-split* nil) (*wal-pending-page-publication* nil)
         (*wal-pending-page-publication-tail* nil)
         (*wal-storage-incarnation* (gensym "TEST-PAGE-STORE-"))
         (*wal-logical-policy* nil) (*logical-capture-force* nil)
         (*wal-temp-cleanup-domain* nil)
         (*wal-insert-timeline* 1) (*wal-ready* t) (*wal-poisoned* nil)
         (*physical-recovery-failed-p* nil)
         (*wal-end-lsn* 200) (*wal-durable-lsn* 100)
         (*backend* (make-backend :xid 44)))
    (ensure-directories-exist (merge-pathnames "base/" *pgdata*))
    (unwind-protect
         (funcall function)
      (dispose-page-publication-test-owner *wal-pending-page-publication*)
      (sb-thread:with-mutex (*wal-lock*) (wal-clear-page-cache))
      (smgr-close-all)
      (when *clog-stream* (close *clog-stream*))
      (uiop:delete-directory-tree *pgdata* :validate t :if-does-not-exist :ignore))))

(defun call-with-queued-clog-test (function)
  (call-with-page-publication-test
   (lambda ()
     (let ((*clog-stream* nil) (*clog-pending-commits* (make-hash-table :test #'eql))
                               (*wal-output-stream* nil) (*wal-output-segment* nil) (*wal-new-segments* nil)
                               (*wal-end-lsn* 0) (*wal-durable-lsn* 0) (*wal-checkpoint-lsn* 0)
                               (*wal-control-metadata* nil) (*wal-replication-slots* (make-hash-table :test #'equal))
                               (*wal-xid-first-lsn* (make-hash-table :test #'eql))
                               (*active-xids* nil) (*next-xid* 100) (*xid-batch-end* 100))
       (ensure-directories-exist (wal-directory))
       (unwind-protect
            (progn (clog-init) (funcall function))
         (when *wal-output-stream* (close *wal-output-stream*))
         (when *clog-stream* (close *clog-stream*)))))))

(rove:deftest fresh-snapshot-settles-pending-page-and-clog-prefix
    (dolist (boundary-p '(nil t))
      (call-with-queued-clog-test
       (lambda ()
         (let ((capture (symbol-function 'make-snapshot-under-xid-lock))
               (registration (make-snapshot-registration))
               (*physical-recovery-state* nil)
               (captures 0) (last-end nil))
           (unwind-protect
                (progn
                  (loop for xid in '(44 45) for block from 0 do
                    (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
                      (setf (backend-xid *backend*) xid (aref page 100) (+ 10 block))
                      (wal-buffer-write 7310 block page))
                    (sb-thread:with-mutex (*wal-lock*)
                      (setf last-end
                            (wal-page-publication-terminal-end
                             (wal-admit-transaction-under-lock xid nil)))))
                  (setf (symbol-function 'make-snapshot-under-xid-lock)
                        (lambda (curcid &key register)
                          (assert (sb-thread:holding-mutex-p *wal-lock*))
                          (assert (sb-thread:holding-mutex-p *xid-lock*))
                          (assert (null *wal-pending-page-publication*))
                          (assert (= +clog-committed+ (pending-clog-raw-status 44)))
                          (assert (= +clog-committed+ (pending-clog-raw-status 45)))
                          (assert (= 10 (aref (smgr-read 7310 0) 100)))
                          (assert (= 11 (aref (smgr-read 7310 1) 100)))
                          (incf captures)
                          (funcall capture curcid :register register)))
                  (if boundary-p
                      (sb-thread:with-mutex (*write-lock*)
                        (multiple-value-bind (snapshot boundary restart)
                            (wal-capture-snapshot-boundary-under-writer-lock 7 registration)
                          (rove:ok (= last-end boundary restart))
                          (rove:ok (null (snapshot-xip snapshot)))))
                      (rove:ok (null (snapshot-xip (make-snapshot 7 :register registration)))))
                  (rove:ok (= 1 captures))
                  (rove:ok (snapshot-registration-active-p registration)))
             (setf (symbol-function 'make-snapshot-under-xid-lock) capture)
             (release-snapshot-registration registration)))))))

(rove:deftest fresh-snapshot-settlement-failure-skips-xid-capture
    (dolist (boundary-p '(nil t))
      (dolist (exit-kind '(:error :nonlocal))
        (call-with-queued-clog-test
         (lambda ()
           (let ((flush (symbol-function 'wal-flush-core))
                 (capture (symbol-function 'make-snapshot-under-xid-lock))
                 (stop (symbol-function 'wal-fail-stop))
                 (registration (make-snapshot-registration))
                 (*physical-recovery-state* nil)
                 (failure (make-condition 'simple-error :format-control "snapshot prefix flush failed"))
                 (owner nil) (captures 0) (stops 0) (observed nil))
             (unwind-protect
                  (progn
                    (sb-thread:with-mutex (*wal-lock*)
                      (setf owner (wal-admit-transaction-under-lock 44 nil)))
                    (setf (symbol-function 'wal-flush-core)
                          (lambda ()
                            (assert (sb-thread:holding-mutex-p *wal-lock*))
                            (assert (not (sb-thread:holding-mutex-p *xid-lock*)))
                            (if (eq exit-kind :error) (error failure) (throw 'leave :left)))
                          (symbol-function 'make-snapshot-under-xid-lock)
                          (lambda (curcid &key register)
                            (incf captures)
                            (funcall capture curcid :register register))
                          (symbol-function 'wal-fail-stop)
                          (lambda (condition)
                            (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                            (assert (not (sb-thread:holding-mutex-p *xid-lock*)))
                            (incf stops)
                            (error condition)))
                    (setf observed
                          (handler-case
                              (catch 'leave
                                (if boundary-p
                                    (sb-thread:with-mutex (*write-lock*)
                                      (wal-capture-snapshot-boundary-under-writer-lock 3 registration))
                                    (make-snapshot 3 :register registration)))
                            (error (condition) condition)))
                    (rove:ok (typep observed 'error))
                    (when (eq exit-kind :error) (rove:ok (eq failure observed)))
                    (rove:ok (= 1 stops))
                    (rove:ok (zerop captures))
                    (rove:ok *wal-poisoned*)
                    (rove:ok (eq owner *wal-pending-page-publication*))
                    (rove:ok (not (snapshot-registration-active-p registration)))
                    (rove:ok (null (gethash registration *wal-snapshot-retention-pins*))))
               (setf (symbol-function 'wal-flush-core) flush
                     (symbol-function 'make-snapshot-under-xid-lock) capture
                     (symbol-function 'wal-fail-stop) stop)
               (release-snapshot-registration registration))))))))

(rove:deftest snapshot-construction-errors-do-not-poison-settled-wal
    (call-with-queued-clog-test
     (lambda ()
       (let ((capture (symbol-function 'make-snapshot-under-xid-lock))
             (stop (symbol-function 'wal-fail-stop))
             (*physical-recovery-state* nil)
             (registration (make-snapshot-registration))
             (failure (make-condition 'simple-error :format-control "snapshot construction failed"))
             (stops 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'make-snapshot-under-xid-lock)
                      (lambda (&rest arguments) (declare (ignore arguments)) (error failure))
                      (symbol-function 'wal-fail-stop)
                      (lambda (condition) (incf stops) (error condition)))
                (rove:ok (eq failure (handler-case (make-snapshot 0 :register registration)
                                       (error (condition) condition))))
                (rove:ok (zerop stops))
                (rove:ok (not *wal-poisoned*))
                (rove:ok (not (snapshot-registration-active-p registration)))
                (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*))))
           (setf (symbol-function 'make-snapshot-under-xid-lock) capture
                 (symbol-function 'wal-fail-stop) stop)
           (release-snapshot-registration registration))))))

(rove:deftest queued-transaction-owns-clog-completion-through-drain
    (call-with-queued-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (multiple-value-bind (owner end) (wal-admit-transaction-under-lock 44 nil)
           (let ((completion (wal-page-publication-completion owner)))
             (rove:ok (eq owner *wal-pending-page-publication*))
             (rove:ok (eq owner (clog-commit-completion-owner completion)))
             (rove:ok (eq :published (clog-commit-completion-state completion)))
             (rove:ok (= end (clog-commit-tag-terminal-end (clog-commit-completion-tag completion))))
             (rove:ok (= +clog-in-progress+ (clog-get 44)))
             (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
             (rove:ok
              (handler-case (progn (wal-drain-page-publications-through-under-lock owner) nil)
                (wal-input-validation-error () t)))
             (let ((*backend* nil))
               (wal-settle-page-publications-under-lock))
             (rove:ok (eq :materialized (clog-commit-completion-state completion)))
             (rove:ok (eq :released (wal-page-publication-state owner)))
             (rove:ok (= +clog-committed+ (pending-clog-raw-status 44)))
             (rove:ok (zerop (hash-table-count *clog-pending-commits*)))
             (rove:ok (null *wal-pending-page-publication*))))))))

(defun accepted-clog-neighbor-result (slot committed-neighbor-p)
  (call-with-queued-clog-test
   (lambda ()
     (sb-thread:with-mutex (*wal-lock*)
       (let* ((xid (+ 44 slot))
              (neighbor (+ 44 (mod (1+ slot) 4)))
              (owner (wal-admit-transaction-under-lock xid nil))
              (completion (wal-page-publication-completion owner))
              (end (wal-page-publication-terminal-end owner))
              (neighbor-status (if committed-neighbor-p +clog-committed+ +clog-aborted+)))
         (clog-accept-publication-under-wal-lock owner)
         (clog-set neighbor neighbor-status :fsync t)
         (clog-fsync)
         (let ((pending-p
                 (and (eq :accepted (clog-commit-completion-state completion))
                      (= +clog-committed+ (clog-get xid))
                      (= +clog-in-progress+ (pending-clog-raw-status xid))
                      (= neighbor-status (pending-clog-raw-status neighbor))
                      (< *wal-durable-lsn* end)
                      (handler-case
                          (progn (clog-complete-reserved-commit-under-wal-lock completion owner) nil)
                        (wal-input-validation-error () t)))))
           (clog-check-queued-commits-under-wal-lock)
           (wal-settle-page-publications-under-lock)
           (and pending-p
                (eq :released (wal-page-publication-state owner))
                (eq :materialized (clog-commit-completion-state completion))
                (= +clog-committed+ (pending-clog-raw-status xid))
                (zerop (hash-table-count *clog-pending-commits*)))))))))

(cl-spec:defproperty accepted-clog-neighbor-writes-preserve-wal-order
    ((slot (range integer 0 3)) (committed-neighbor-p boolean))
  (:about clog-accept-publication-under-wal-lock)
  (:trials (:normal 100))
  (accepted-clog-neighbor-result slot committed-neighbor-p))

(rove:deftest accepted-clog-is-visible-without-writing-nondurable-status
    (unit-check-property 'accepted-clog-neighbor-writes-preserve-wal-order))

(rove:deftest running-anchor-excludes-accepted-commit-without-releasing-xid
    (call-with-queued-clog-test
     (lambda ()
       (let ((*active-xids* '(44 45))
             (*next-xid* 46)
             (captured nil)
             (original-start (symbol-function 'wal-append-batch-start))
             (original-record (symbol-function 'wal-append-record)))
         (sb-thread:with-mutex (*wal-lock*)
           (let ((owner (wal-admit-transaction-under-lock 44 nil)))
             (clog-accept-publication-under-wal-lock owner)
             (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
             (rove:ok (= +clog-committed+ (clog-get 44)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'wal-append-batch-start) (lambda () nil)
                          (symbol-function 'wal-append-record)
                          (lambda (type xid payload)
                            (declare (ignore xid))
                            (when (= type +wal-record-running-xacts+)
                              (setf captured (wal-decode-running-xacts payload)))))
                    (wal-append-running-xacts-under-lock 45)
                    (rove:ok (equal '(45) (getf captured :running)))
                    (rove:ok (getf captured :complete))
                    (rove:ok (equal '(44 45) *active-xids*)))
               (setf (symbol-function 'wal-append-batch-start) original-start
                     (symbol-function 'wal-append-record) original-record))
             (wal-settle-page-publications-under-lock)))))))

(rove:deftest running-anchor-bounds-eligible-membership-not-active-ownership
    (let* ((*wal-lock* (sb-thread:make-mutex :name "test anchor WAL"))
           (*xid-lock* (sb-thread:make-mutex :name "test anchor XIDs"))
           (eligible (loop for xid from 3 below (+ 3 +running-xacts-limit+) collect xid))
           (committed (loop for xid from (+ 4 +running-xacts-limit+)
                              below (+ 24 +running-xacts-limit+) collect xid))
           (*active-xids* (append committed eligible))
           (*next-xid* (+ 25 +running-xacts-limit+))
           (captured nil)
           (original-status (symbol-function 'clog-get))
           (original-start (symbol-function 'wal-append-batch-start))
           (original-record (symbol-function 'wal-append-record)))
      (unwind-protect
           (progn
             (setf (symbol-function 'clog-get)
                   (lambda (xid)
                     (assert (sb-thread:holding-mutex-p *wal-lock*))
                     (assert (sb-thread:holding-mutex-p *xid-lock*))
                     (if (member xid committed) +clog-committed+ +clog-in-progress+))
                   (symbol-function 'wal-append-batch-start) (lambda () nil)
                   (symbol-function 'wal-append-record)
                   (lambda (type xid payload)
                     (declare (ignore xid))
                     (when (= type +wal-record-running-xacts+)
                       (setf captured (wal-decode-running-xacts payload)))))
             (sb-thread:with-mutex (*wal-lock*)
               (wal-append-running-xacts-under-lock 3)
               (rove:ok (getf captured :complete))
               (rove:ok (= +running-xacts-limit+ (length (getf captured :running))))
               (rove:ok (> (length *active-xids*) +running-xacts-limit+))
               (push (+ 3 +running-xacts-limit+) *active-xids*)
               (wal-append-running-xacts-under-lock 3)
               (rove:ok (not (getf captured :complete)))
               (rove:ok (null (getf captured :running)))))
        (setf (symbol-function 'clog-get) original-status
              (symbol-function 'wal-append-batch-start) original-start
              (symbol-function 'wal-append-record) original-record))))

(rove:deftest accepted-clog-rejects-unlinked-stale-and-repeated-acceptance
    (call-with-queued-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((owner (wal-admit-transaction-under-lock 44 nil))
                (completion (wal-page-publication-completion owner))
                (end (wal-page-publication-terminal-end owner))
                (incarnation (wal-page-publication-incarnation owner)))
           (dolist (fault '(:foreign :unlinked :end :incarnation))
             (unwind-protect
                  (progn
                    (ecase fault
                      (:foreign)
                      (:unlinked
                       (setf *wal-pending-page-publication* nil
                             *wal-pending-page-publication-tail* nil)
                       (remhash 44 *clog-pending-commits*))
                      (:end (incf (wal-page-publication-terminal-end owner)))
                      (:incarnation
                       (setf (wal-page-publication-incarnation owner) (gensym))))
                    (rove:ok
                     (handler-case
                         (progn
                           (clog-accept-publication-under-wal-lock
                            (if (eq fault :foreign) (%make-wal-page-publication) owner))
                           nil)
                       (wal-input-validation-error () t)))
                    (rove:ok (eq :published (clog-commit-completion-state completion)))
                    (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44))))
               (setf *wal-pending-page-publication* owner
                     *wal-pending-page-publication-tail* owner
                     (gethash 44 *clog-pending-commits*) completion
                     (wal-page-publication-terminal-end owner) end
                     (wal-page-publication-incarnation owner) incarnation)))
           (clog-accept-publication-under-wal-lock owner)
           (rove:ok
            (handler-case
                (progn (clog-accept-publication-under-wal-lock owner) nil)
              (wal-input-validation-error () t)))
           (rove:ok (eq :accepted (clog-commit-completion-state completion)))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
           (wal-settle-page-publications-under-lock))))))

(rove:deftest accepted-clog-preserves-predecessor-and-poison-boundaries
    (call-with-queued-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((first (wal-admit-transaction-under-lock 44 nil))
                (second (wal-admit-transaction-under-lock 45 nil))
                (first-completion (wal-page-publication-completion first))
                (second-completion (wal-page-publication-completion second)))
           (let ((*wal-poisoned* t))
             (rove:ok
              (handler-case
                  (progn (clog-accept-publication-under-wal-lock second) nil)
                (error () t))))
           (rove:ok (eq :published (clog-commit-completion-state second-completion)))
           (clog-accept-publication-under-wal-lock second)
           (rove:ok (eq :published (clog-commit-completion-state first-completion)))
           (rove:ok (= +clog-in-progress+ (clog-get 44)))
           (rove:ok (= +clog-committed+ (clog-get 45)))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 45)))
           (rove:ok (eq first *wal-pending-page-publication*))
           (rove:ok (eq second (wal-page-publication-next first)))
           (wal-flush)
           (wal-drain-page-publications-through-under-lock first)
           (rove:ok (eq second *wal-pending-page-publication*))
           (rove:ok (eq :accepted (clog-commit-completion-state second-completion)))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 45)))
           (wal-drain-page-publications-through-under-lock second)
           (rove:ok (null *wal-pending-page-publication*))
           (rove:ok (= +clog-committed+ (pending-clog-raw-status 45))))))))

(rove:deftest async-storage-receipt-keeps-owned-debt-and-forces-required-flushes
    (dolist (mode '(:async :unavailable :pressure :flush))
      (call-with-queued-clog-test
       (lambda ()
         (let ((available (symbol-function 'primary-wal-writer-available-under-lock-p))
               (*wal-publication-pressure-limits* (copy-list *wal-publication-pressure-limits*))
               (durable-end nil) (after nil) (accepted nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'primary-wal-writer-available-under-lock-p)
                        (lambda () (not (eq mode :unavailable))))
                  (when (eq mode :pressure)
                    (setf (getf *wal-publication-pressure-limits* :owners) 0))
                  (multiple-value-bind (end receipt)
                      (wal-commit-transaction
                       44 (lambda (end) (setf after end))
                       :source-id :async-source
                       :local-durability (if (eq mode :flush) :flush :async)
                       :terminal-durable-function (lambda (end) (setf durable-end end))
                       :accepted-function (lambda (receipt) (setf accepted receipt)))
                    (rove:ok (eq receipt accepted))
                    (rove:ok (eq receipt (validate-wal-commit-receipt
                                          receipt accepted 44 :async-source end durable-end t
                                          (if (eq mode :flush) :flush :async))))
                    (if (eq mode :async)
                        (progn
                          (rove:ok (null after))
                          (rove:ok (null durable-end))
                          (rove:ok (null (wal-commit-receipt-wal-durable-p receipt)))
                          (rove:ok (< *wal-durable-lsn* end))
                          (rove:ok (= +clog-committed+ (clog-get 44)))
                          (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
                          (rove:ok (eq :accepted
                                       (clog-commit-completion-state
                                        (wal-page-publication-completion *wal-pending-page-publication*))))
                          (sb-thread:with-mutex (*wal-lock*)
                            (wal-settle-page-publications-under-lock)))
                        (progn
                          (rove:ok (= end after durable-end))
                          (rove:ok (wal-commit-receipt-wal-durable-p receipt))
                          (rove:ok (null *wal-pending-page-publication*))))
                    (rove:ok (= +clog-committed+ (pending-clog-raw-status 44)))))
             (setf (symbol-function 'primary-wal-writer-available-under-lock-p) available)))))))

(rove:deftest async-request-flushes-real-relation-replacement-before-reset
    (call-with-queued-clog-test
     (lambda ()
       (let ((available (symbol-function 'primary-wal-writer-available-under-lock-p))
             (page (page-init (make-page)))
             (durable-end nil) (after nil))
         (setf (aref page 100) 42)
         (smgr-write 7315 0 page)
         (smgr-write 7315 1 page)
         (transaction-replace-relation 7315)
         (setf (aref page 100) 99)
         (replacement-write-page (current-relation-replacement 7315 :main) 0 page)
         (unwind-protect
              (progn
                (setf (symbol-function 'primary-wal-writer-available-under-lock-p) (lambda () t))
                (multiple-value-bind (end receipt)
                    (wal-commit-transaction
                     44 (lambda (end)
                          (setf after end)
                          (discard-transaction-replacements *backend*))
                     :source-id :reset-source :local-durability :async
                     :terminal-durable-function
                     (lambda (end)
                       (setf durable-end end)
                       (rove:ok (= 2 (smgr-nblocks 7315)))))
                  (rove:ok (= end after durable-end))
                  (rove:ok (wal-commit-receipt-wal-durable-p receipt))
                  (rove:ok (null *wal-pending-page-publication*))
                  (rove:ok (= 1 (smgr-nblocks 7315)))
                  (rove:ok (= 99 (aref (smgr-read 7315 0) 100)))))
           (setf (symbol-function 'primary-wal-writer-available-under-lock-p) available)
           (discard-transaction-replacements *backend*))))))

(rove:deftest stopped-writer-settles-accepted-debt-without-an-installed-thread
    (call-with-queued-clog-test
     (lambda ()
       (let ((*primary-wal-writer* nil) (*live-operation-depth* 0))
         (sb-thread:with-mutex (*wal-lock*)
           (let ((owner (wal-admit-transaction-under-lock 44 nil)))
             (clog-accept-publication-under-wal-lock owner)))
         (stop-primary-wal-writer)
         (rove:ok (null *wal-pending-page-publication*))
         (rove:ok (= +clog-committed+ (pending-clog-raw-status 44)))))))

(rove:deftest stopped-writer-retains-poisoned-debt-and-fences-settlement-errors
    (dolist (poisoned-p '(nil t))
      (call-with-queued-clog-test
       (lambda ()
         (let ((*primary-wal-writer* nil) (*live-operation-depth* 0)
                                          (flush (symbol-function 'wal-flush-core))
                                          (stop (symbol-function 'wal-fail-stop))
                                          (failure (make-condition 'simple-error :format-control "stop settlement failure"))
                                          (fenced nil) (flushed nil))
           (sb-thread:with-mutex (*wal-lock*)
             (clog-accept-publication-under-wal-lock (wal-admit-transaction-under-lock 44 nil)))
           (unwind-protect
                (progn
                  (setf *wal-poisoned* poisoned-p
                        (symbol-function 'wal-flush-core)
                        (lambda () (setf flushed t) (error failure))
                        (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (rove:ok (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
                          (setf fenced condition)))
                  (let ((result (handler-case (progn (stop-primary-wal-writer) :stopped)
                                  (error (condition) condition))))
                    (rove:ok (if poisoned-p (eq :stopped result) (eq failure result))))
                  (rove:ok (eql (not poisoned-p) flushed))
                  (rove:ok (if poisoned-p (null fenced) (eq failure fenced)))
                  (rove:ok *wal-poisoned*)
                  (rove:ok *wal-pending-page-publication*)
                  (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44))))
             (setf (symbol-function 'wal-flush-core) flush
                   (symbol-function 'wal-fail-stop) stop
                   *wal-poisoned* nil)
             (sb-thread:with-mutex (*wal-lock*) (wal-settle-page-publications-under-lock))))))))

(rove:deftest queued-transaction-keeps-materialized-status-after-page-failure
    (call-with-queued-clog-test
     (lambda ()
       (let ((apply-pages (symbol-function 'wal-primary-apply-page-spool))
             (write (symbol-function 'clog-write-status-under-lock))
             (calls 0))
         (unwind-protect
              (sb-thread:with-mutex (*wal-lock*)
                (let* ((owner (wal-admit-transaction-under-lock 44 nil))
                       (completion (wal-page-publication-completion owner))
                       (failure (make-condition 'simple-error :format-control "page application failure")))
                  (wal-flush)
                  (setf (symbol-function 'clog-write-status-under-lock)
                        (lambda (&rest args) (incf calls) (apply write args))
                        (symbol-function 'wal-primary-apply-page-spool)
                        (lambda (&rest args) (declare (ignore args)) (error failure)))
                  (rove:ok
                   (eq failure
                       (handler-case
                           (wal-drain-page-publications-through-under-lock owner)
                         (error (condition) condition))))
                  (rove:ok (eq :materialized (clog-commit-completion-state completion)))
                  (rove:ok (eq owner *wal-pending-page-publication*))
                  (rove:ok (zerop (hash-table-count *clog-pending-commits*)))
                  (rove:ok (not (logical-spool-closed-p (wal-page-publication-spool owner))))
                  (clog-check-queued-commits-under-wal-lock)
                  (setf (symbol-function 'wal-primary-apply-page-spool) apply-pages)
                  (wal-drain-page-publications-through-under-lock owner)
                  (rove:ok (= calls 1))
                  (rove:ok (eq :released (wal-page-publication-state owner)))))
           (setf (symbol-function 'wal-primary-apply-page-spool) apply-pages
                 (symbol-function 'clog-write-status-under-lock) write))))))

(rove:deftest queued-transaction-rejects-orphan-clog-before-flush
    (call-with-queued-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((owner (wal-admit-transaction-under-lock 44 nil))
                (flush (symbol-function 'wal-flush-core))
                (flushed nil))
           (clog-stage-commit-under-wal-lock 45 (wal-page-publication-terminal-end owner))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-flush-core) (lambda () (setf flushed t)))
                  (rove:ok
                   (handler-case (progn (wal-settle-page-publications-under-lock) nil)
                     (wal-input-validation-error () t)))
                  (rove:ok (not flushed))
                  (rove:ok (not *wal-poisoned*))
                  (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
                  (rove:ok (eq owner *wal-pending-page-publication*)))
             (setf (symbol-function 'wal-flush-core) flush)
             (remhash 45 *clog-pending-commits*)))
         (wal-settle-page-publications-under-lock)))))

(rove:deftest queued-transaction-requires-a-clog-obligation
    (call-with-queued-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((owner (wal-admit-transaction-under-lock 44 nil))
                (completion (wal-page-publication-completion owner))
                (flush (symbol-function 'wal-flush-core))
                (flushed nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-flush-core) (lambda () (setf flushed t)))
                  (dolist (fault '(:missing :maintenance :empty-maintenance))
                    (unwind-protect
                         (progn
                           (ecase fault
                             (:missing
                              (setf (wal-page-publication-completion owner) nil)
                              (remhash 44 *clog-pending-commits*))
                             (:maintenance (setf (wal-page-publication-kind owner) :maintenance))
                             (:empty-maintenance
                              (setf (wal-page-publication-kind owner) :maintenance
                                    (wal-page-publication-completion owner) nil)
                              (remhash 44 *clog-pending-commits*)))
                           (rove:ok
                            (handler-case (progn (wal-settle-page-publications-under-lock) nil)
                              (wal-input-validation-error () t)))
                           (rove:ok (not flushed))
                           (rove:ok (not *wal-poisoned*))
                           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44))))
                      (setf (wal-page-publication-completion owner) completion
                            (wal-page-publication-kind owner) :transaction
                            (gethash 44 *clog-pending-commits*) completion))))
             (setf (symbol-function 'wal-flush-core) flush)))
         (call-with-wal-primary-page-spool
          (lambda (spool owner)
            (declare (ignore spool))
            (wal-prepare-page-publication-under-lock owner 45 nil nil)
            (incf *wal-end-lsn* 40)
            (rove:ok
             (handler-case
                 (progn (wal-publish-page-publication-under-lock owner *wal-end-lsn*) nil)
               (wal-input-validation-error () t)))))
         (wal-settle-page-publications-under-lock)))))

(rove:deftest queued-transaction-captures-only-the-requested-clog-prefix
    (call-with-queued-clog-test
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (let* ((first (wal-admit-transaction-under-lock 44 nil))
                (second (wal-admit-transaction-under-lock 45 nil)))
           (wal-flush)
           (wal-drain-page-publications-through-under-lock first)
           (rove:ok (= +clog-committed+ (clog-get 44)))
           (rove:ok (= +clog-in-progress+ (clog-get 45)))
           (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 45)))
           (rove:ok (eq second *wal-pending-page-publication*))
           (rove:ok (eq :published
                        (clog-commit-completion-state (wal-page-publication-completion second))))
           (wal-settle-page-publications-under-lock)
           (rove:ok (= +clog-committed+ (pending-clog-raw-status 45))))))))

(rove:deftest queued-transaction-reserves-the-rotated-terminal-end
    (call-with-queued-clog-test
     (lambda ()
       (let ((pause (symbol-function 'wal-test-pause)))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-test-pause)
                      (lambda (point)
                        (when (eq point :after-page-images)
                          ;; Sparse padding tests terminal placement, not WAL recovery.
                          (let ((position (1- +wal-segment-size+)))
                            (file-position *wal-output-stream* (1- position))
                            (write-byte 0 *wal-output-stream*)
                            (finish-output *wal-output-stream*)
                            (setf *wal-end-lsn* position)))))
                (sb-thread:with-mutex (*wal-lock*)
                  (multiple-value-bind (owner end) (wal-admit-transaction-under-lock 44 nil)
                    (rove:ok (= end (+ +wal-segment-size+ +wal-header-size+ 8)))
                    (rove:ok (= end (clog-commit-tag-terminal-end
                                     (clog-commit-completion-tag
                                      (wal-page-publication-completion owner)))))
                    (finish-output *wal-output-stream*)
                    (with-open-file (in (wal-segment-path 1) :element-type '(unsigned-byte 8))
                      (let* ((bytes (make-array (+ +wal-header-size+ 8) :element-type '(unsigned-byte 8)))
                             (read (read-sequence bytes in))
                             (record (wal-decode-record bytes +wal-segment-size+)))
                        (rove:ok (= read (length bytes)))
                        (rove:ok (= +wal-record-commit+ (wal-record-type record)))
                        (rove:ok (= 44 (wal-record-xid record)))
                        (rove:ok (= end (wal-record-end record)))))
                    (wal-flush)
                    (wal-drain-page-publications-through-under-lock owner))))
           (setf (symbol-function 'wal-test-pause) pause))))))

(rove:deftest synchronous-commit-retires-predecessor-before-own-status
    (call-with-queued-clog-test
     (lambda ()
       (let ((first (sb-thread:with-mutex (*wal-lock*)
                      (wal-admit-transaction-under-lock 44 nil)))
             (callback nil))
         (sb-thread:with-mutex (*wal-lock*)
           (let ((second (wal-admit-transaction-under-lock 45 nil)))
             (wal-flush)
             (rove:ok
              (handler-case
                  (progn (wal-complete-publication-status-under-lock second) nil)
                (wal-input-validation-error () t)))
             (rove:ok (= +clog-in-progress+ (clog-get 44)))
             (rove:ok (= +clog-in-progress+ (clog-get 45)))))
         (wal-commit-transaction
          46 (lambda (end)
               (declare (ignore end))
               (setf callback t)
               (rove:ok (eq :released (wal-page-publication-state first)))
               (rove:ok (= +clog-committed+ (pending-clog-raw-status 44)))
               (rove:ok (= +clog-committed+ (pending-clog-raw-status 45)))
               (rove:ok (= +clog-committed+ (pending-clog-raw-status 46)))
               (rove:ok (= 46 (wal-page-publication-xid *wal-pending-page-publication*))))
          :source-id :test-source)
         (rove:ok callback)
         (rove:ok (null *wal-pending-page-publication*))))))

(defun queued-clog-prefix-result (second-xid marker)
  (call-with-queued-clog-test
   (lambda ()
     (flet ((write-page (xid value)
              (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
                (setf (backend-xid *backend*) xid (aref page 100) value)
                (wal-buffer-write 7310 0 page))))
       (write-page 44 marker)
       (let ((first (sb-thread:with-mutex (*wal-lock*)
                      (wal-admit-transaction-under-lock 44 nil))))
         (write-page second-xid (1+ marker))
         (sb-thread:with-mutex (*wal-lock*)
           (let ((second (wal-admit-transaction-under-lock second-xid nil)))
             (wal-flush)
             (wal-drain-page-publications-through-under-lock first)
             (unless (and (= +clog-committed+ (pending-clog-raw-status 44))
                          (= +clog-in-progress+ (clog-get second-xid))
                          (= +clog-in-progress+ (pending-clog-raw-status second-xid))
                          (= marker (aref (smgr-read 7310 0) 100))
                          (= (1+ marker) (aref (wal-visible-page-under-lock 7310 0 :main) 100))
                          (eq second *wal-pending-page-publication*))
               (return-from queued-clog-prefix-result nil))
             (wal-settle-page-publications-under-lock)
             (and (= +clog-committed+ (pending-clog-raw-status second-xid))
                  (= (1+ marker) (aref (smgr-read 7310 0) 100))
                  (zerop (hash-table-count *clog-pending-commits*))
                  (null *wal-pending-page-publication*)))))))))

(cl-spec:defproperty queued-clog-and-pages-retire-the-same-prefix
    ((second-xid (range integer 45 99)) (marker (range integer 0 254)))
  (:about wal-admit-transaction-under-lock)
  (:trials (:normal 100))
  (queued-clog-prefix-result second-xid marker))

(rove:deftest queued-clog-and-pages-prefix-contract
    (unit-check-property 'queued-clog-and-pages-retire-the-same-prefix))

(rove:deftest queued-transaction-terminal-failures-preserve-or-release-the-owner
    (dolist (phase '(:install :reserve-return :reserved :inserting :published :linked))
      (call-with-queued-clog-test
       (lambda ()
         (let ((reserve (symbol-function 'clog-reserve-commit-under-wal-lock))
               (pause (symbol-function 'wal-test-pause))
               (stop (symbol-function 'wal-fail-stop))
               (owner nil) (failure (make-condition 'simple-error :format-control "terminal boundary failure")))
           (unwind-protect
                (progn
                  (setf (symbol-function 'clog-reserve-commit-under-wal-lock)
                        (lambda (&rest arguments)
                          (let ((completion (apply reserve arguments)))
                            (setf owner (clog-commit-completion-owner completion))
                            (when (eq phase :reserve-return) (error failure))
                            completion))
                        (symbol-function 'wal-test-pause)
                        (lambda (point)
                          (when (eq point (case phase
                                            (:install :before-clog-reservation-install)
                                            (:reserved :after-clog-reservation)
                                            (:inserting :before-terminal-append)
                                            (:published :before-page-publication-transfer)
                                            (:linked :after-page-publication-transfer)))
                            (setf owner *wal-page-publication-appender*)
                            (error failure)))
                        (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (setf *wal-poisoned* t)
                          (error condition)))
                  (rove:ok
                   (eq failure
                       (handler-case
                           (with-wal-publication-fence (begin)
                             (begin)
                             (wal-admit-transaction-under-lock 44 nil))
                         (error (condition) condition))))
                  (rove:ok *wal-poisoned*)
                  (rove:ok owner)
                  (let ((preterminal-p (member phase '(:install :reserve-return :reserved))))
                    (rove:ok (eql (not (null preterminal-p))
                                  (zerop (hash-table-count *clog-pending-commits*)))))
                  (rove:ok (eql (not (null (member phase '(:install :reserve-return :reserved))))
                                (logical-spool-closed-p (wal-page-publication-spool owner))))
                  (rove:ok (eq (if (eq phase :linked) owner nil) *wal-pending-page-publication*)))
             (setf (symbol-function 'clog-reserve-commit-under-wal-lock) reserve
                   (symbol-function 'wal-test-pause) pause
                   (symbol-function 'wal-fail-stop) stop)
             (when (and owner (not (eq owner *wal-pending-page-publication*))
                        (not (logical-spool-closed-p (wal-page-publication-spool owner))))
               (sb-thread:with-mutex (*wal-lock*)
                 (wal-close-primary-page-spool (wal-page-publication-spool owner))))))))))

(defun page-publication-test-refcount ()
  (if *wal-dirty-store*
      (+ (wal-dirty-store-resident *wal-dirty-store*)
         (wal-dirty-store-disk-refs *wal-dirty-store*))
      0))

(defun page-publication-test-spool (spool)
  (let* ((images (mapcar #'wal-primary-page-image (wal-primary-page-keys)))
         (payload (wal-encode-page-images images 100))
         (event (make-array (+ 12 (length payload)) :element-type '(unsigned-byte 8))))
    (wal-set-u32 event 0 +wal-version+)
    (wal-set-u64 event 4 100)
    (replace event payload :start1 12)
    (logical-spool-append spool event)
    (wal-primary-seal-page-spool spool)))

(defun page-publication-test-publish (owner end)
  ;; These page models supply an already appended terminal, not an inserter.
  ;; Install its data-only CLOG obligation explicitly before the real handoff.
  (unless *clog-stream* (clog-init))
  (let* ((xid (wal-page-publication-xid owner))
         (tag (%make-clog-commit-tag xid *wal-storage-incarnation* *wal-insert-timeline* end))
         (completion (%make-clog-commit-completion tag owner :published)))
    (sb-thread:with-mutex (*clog-lock*)
      (assert (null (gethash (xid32-of xid) *clog-pending-commits*)))
      (setf (wal-page-publication-completion owner) completion
            (gethash (xid32-of xid) *clog-pending-commits*) completion)))
  (wal-publish-page-publication-under-lock owner end))

(defun page-publication-model-result (marker resident-limit fork)
  (call-with-page-publication-test
   (lambda ()
     (let ((page (page-init (make-page)))
           (key (wal-page-key 7310 2 fork))
           (other (if (eq fork :main) :visibility :main))
           (owner nil) (old-map nil) (reference nil) (private-seen nil))
       (setf (aref page 100) marker)
       (wal-buffer-write 7310 0 page fork)
       (wal-buffer-write 7310 2 page fork)
       (let ((*backend* (make-backend :xid 66)) (*wal-row-page-write-p* t))
         (wal-buffer-write 7310 2 page fork))
       (sb-thread:with-mutex (*wal-lock*)
         (setf old-map *wal-page-cache* reference (gethash key old-map))
         (call-with-wal-primary-page-spool
          (lambda (spool publication)
            (page-publication-test-spool spool)
            (wal-prepare-page-publication-under-lock publication 44 nil nil)
            (page-publication-test-publish publication 140)
            (setf owner publication))))
       (unless (and (eq owner *wal-pending-page-publication*)
                    (eq old-map (wal-page-publication-pages owner))
                    (eq reference (gethash key (wal-page-publication-pages owner)))
                    (eq :pending (wal-page-publication-state owner))
                    (not (logical-spool-closed-p (wal-page-publication-spool owner)))
                    (zerop (hash-table-count *wal-page-cache*))
                    (= 2 (page-publication-test-refcount))
                    (= marker (aref (wal-buffer-read 7310 2 fork) 100))
                    (= 3 (wal-buffer-nblocks 7310 fork))
                    (zerop (wal-buffer-nblocks 7310 other))
                    (null (smgr-read 7310 2 fork)))
         (return-from page-publication-model-result nil))
       (let ((changed (copy-seq page)))
         (setf (aref changed 100) (mod (1+ marker) 256))
         (catch 'private-rollback
           (with-wal-page-batch
             (wal-buffer-write 7310 2 changed fork)
             (setf private-seen (aref (wal-buffer-read 7310 2 fork) 100))
             (throw 'private-rollback :rolled-back)))
         (unless (and (= private-seen (aref changed 100))
                      (= marker (aref (wal-buffer-read 7310 2 fork) 100))
                      (= 2 (page-publication-test-refcount)))
           (return-from page-publication-model-result nil))
         (let ((*backend* (make-backend :xid 55)) (*wal-row-page-write-p* t))
           (with-wal-page-batch
             (wal-buffer-write 7310 2 changed fork)
             (unless (= 3 (wal-buffer-extend 7310 changed fork))
               (return-from page-publication-model-result nil))))
         (unless (and (= 4 (wal-buffer-nblocks 7310 fork))
                      (= 4 (page-publication-test-refcount))
                      (every (lambda (xid) (member xid (gethash key *wal-page-cache-xids*)))
                             '(0 1 44 55 66)))
           (return-from page-publication-model-result nil))
         (let ((new (gethash key *wal-page-cache*)))
           (sb-thread:with-mutex (*wal-lock*)
             (setf *wal-durable-lsn* 140)
             (wal-drain-page-publication-under-lock owner))
           (and (null *wal-pending-page-publication*)
                (eq :released (wal-page-publication-state owner))
                (logical-spool-closed-p (wal-page-publication-spool owner))
                (eq new (gethash key *wal-page-cache*))
                (= 2 (page-publication-test-refcount))
                (= (aref changed 100) (aref (wal-buffer-read 7310 2 fork) 100))
                (= marker (aref (smgr-read 7310 2 fork) 100))
                (= 4 (wal-buffer-nblocks 7310 fork)))))))
   :resident-limit resident-limit))

(cl-spec:defproperty page-publication-keeps-owned-pages-and-newer-dirt
    ((marker (range integer 0 255)) (resident-limit (range integer 0 2))
                                    (fork-choice (range integer 0 1)))
  (:about wal-publish-page-publication-under-lock)
  (:trials (:normal 100))
  (page-publication-model-result marker resident-limit (if (zerop fork-choice) :main :visibility)))

(rove:deftest page-publication-ownership-and-visibility-contract
    (unit-check-property 'page-publication-keeps-owned-pages-and-newer-dirt))

(defun page-publication-prefix-model-result (marker resident-limit)
  (call-with-page-publication-test
   (lambda ()
     (let ((first-owner nil) (second-owner nil) (newest nil))
       (flet ((write-version (value xid)
                (setf (backend-xid *backend*) xid)
                (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
                  (setf (aref page 100) value)
                  (wal-buffer-write 7310 0 page)))
              (visible-marker ()
                (aref (wal-buffer-read 7310 0) 100)))
         (write-version marker 44)
         (sb-thread:with-mutex (*wal-lock*)
           (call-with-wal-primary-page-spool
            (lambda (spool owner)
              (page-publication-test-spool spool)
              (wal-prepare-page-publication-under-lock owner 44 nil nil)
              (page-publication-test-publish owner 140)
              (setf first-owner owner))))
         (write-version (1+ marker) 55)
         (sb-thread:with-mutex (*wal-lock*)
           (call-with-wal-primary-page-spool
            (lambda (spool owner)
              (page-publication-test-spool spool)
              (wal-prepare-page-publication-under-lock owner 55 nil nil)
              (page-publication-test-publish owner 180)
              (setf second-owner owner))))
         (unless (and (= 2 (page-publication-test-refcount))
                      (= (1+ marker) (visible-marker))
                      (eq first-owner *wal-pending-page-publication*)
                      (not (logical-spool-closed-p (wal-page-publication-spool first-owner)))
                      (not (logical-spool-closed-p (wal-page-publication-spool second-owner))))
           (return-from page-publication-prefix-model-result nil))
         (write-version (+ marker 2) 66)
         (setf newest (gethash (wal-page-key 7310 0) *wal-page-cache*))
         (catch 'rollback-prefix-model
           (with-wal-page-batch
             (write-version (+ marker 3) 77)
             (unless (= (+ marker 3) (visible-marker))
               (return-from page-publication-prefix-model-result nil))
             (throw 'rollback-prefix-model nil)))
         (sb-thread:with-mutex (*wal-lock*)
           ;; A flush may cover both owners. The captured target remains the first.
           (setf *wal-durable-lsn* 200)
           (funcall 'wal-drain-page-publications-through-under-lock first-owner))
         (unless (and (eq second-owner *wal-pending-page-publication*)
                      (eq :released (wal-page-publication-state first-owner))
                      (eq :pending (wal-page-publication-state second-owner))
                      (= 2 (page-publication-test-refcount))
                      (= marker (aref (smgr-read 7310 0) 100))
                      (= (+ marker 2) (visible-marker))
                      (eq newest (gethash (wal-page-key 7310 0) *wal-page-cache*))
                      (logical-spool-closed-p (wal-page-publication-spool first-owner))
                      (not (logical-spool-closed-p (wal-page-publication-spool second-owner))))
           (return-from page-publication-prefix-model-result nil))
         (sb-thread:with-mutex (*wal-lock*)
           (funcall 'wal-drain-page-publications-through-under-lock second-owner))
         (and (null *wal-pending-page-publication*)
              (eq :released (wal-page-publication-state second-owner))
              (= 1 (page-publication-test-refcount))
              (= (1+ marker) (aref (smgr-read 7310 0) 100))
              (= (+ marker 2) (visible-marker))
              (eq newest (gethash (wal-page-key 7310 0) *wal-page-cache*))
              (logical-spool-closed-p (wal-page-publication-spool second-owner))
              (equal '(1 44 55 66)
                     (sort (copy-list (gethash (wal-page-key 7310 0) *wal-page-cache-xids*)) #'<))))))
   :resident-limit resident-limit))

(cl-spec:defproperty page-publication-drains-only-the-captured-prefix
    ((marker (range integer 1 240)) (resident-limit (range integer 0 2)))
  (:about wal-drain-page-publications-through-under-lock)
  (:trials (:normal 100))
  (page-publication-prefix-model-result marker resident-limit))

(rove:deftest page-publication-captured-prefix-contract
    (unit-check-property 'page-publication-drains-only-the-captured-prefix))

(rove:deftest page-publication-barrier-keeps-its-target-across-flush
    (call-with-page-publication-test
     (lambda ()
       (let ((first-owner nil) (second-owner nil) (later-owner nil)
                               (flush (symbol-function 'wal-flush-core))
                               (write (symbol-function 'smgr-write))
                               (flush-count 0) (writes nil))
         (sb-thread:with-mutex (*wal-lock*)
           (flet ((stage (marker xid end)
                    (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
                      (setf (backend-xid *backend*) xid (aref page 100) marker)
                      (wal-dirty-install-page (wal-page-key 7310 0) page))
                    (call-with-wal-primary-page-spool
                     (lambda (spool owner)
                       (page-publication-test-spool spool)
                       (wal-prepare-page-publication-under-lock owner xid nil nil)
                       (page-publication-test-publish owner end)
                       owner))))
             (setf first-owner (stage 11 44 140)
                   second-owner (stage 22 55 180))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'wal-flush-core)
                          (lambda ()
                            (incf flush-count)
                            (setf later-owner (stage 33 66 190)
                                  *wal-durable-lsn* 200))
                          (symbol-function 'smgr-write)
                          (lambda (node block page &optional (fork :main))
                            (push (aref page 100) writes)
                            (funcall write node block page fork)))
                    (funcall 'wal-settle-page-publications-under-lock)
                    (rove:ok (= 1 flush-count))
                    (rove:ok (equal '(11 22) (reverse writes)))
                    (rove:ok (eq :released (wal-page-publication-state first-owner)))
                    (rove:ok (eq :released (wal-page-publication-state second-owner)))
                    (rove:ok (eq later-owner *wal-pending-page-publication*))
                    (rove:ok (eq later-owner *wal-pending-page-publication-tail*))
                    (rove:ok (eq :pending (wal-page-publication-state later-owner)))
                    (rove:ok (= 1 (page-publication-test-refcount)))
                    (rove:ok (= 22 (aref (smgr-read 7310 0) 100)))
                    (rove:ok (= 33 (aref (wal-visible-page-under-lock 7310 0 :main) 100))))
               (setf (symbol-function 'wal-flush-core) flush
                     (symbol-function 'smgr-write) write))
             (wal-drain-page-publications-through-under-lock later-owner)
             (rove:ok (null *wal-pending-page-publication*))
             (rove:ok (null *wal-pending-page-publication-tail*))))))))

(rove:deftest page-publication-barrier-failures-retain-the-unfinished-prefix
    (dolist (fault '(:flush :write :nonlocal :undurable))
      (call-with-page-publication-test
       (lambda ()
         (let ((first-owner nil) (second-owner nil)
                                 (flush (symbol-function 'wal-flush-core))
                                 (write (symbol-function 'smgr-write))
                                 (stop (symbol-function 'wal-fail-stop))
                                 (original (make-condition 'simple-error :format-control "prefix I/O failure"))
                                 (fenced nil) (outside nil))
           (sb-thread:with-mutex (*wal-lock*)
             (flet ((stage (marker xid end)
                      (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
                        (setf (backend-xid *backend*) xid (aref page 100) marker)
                        (wal-dirty-install-page (wal-page-key 7310 0) page))
                      (call-with-wal-primary-page-spool
                       (lambda (spool owner)
                         (page-publication-test-spool spool)
                         (wal-prepare-page-publication-under-lock owner xid nil nil)
                         (page-publication-test-publish owner end)
                         owner))))
               (setf first-owner (stage 11 44 140) second-owner (stage 22 55 180))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-flush-core)
                        (lambda ()
                          (when (eq fault :flush) (error original))
                          (setf *wal-durable-lsn* (if (eq fault :undurable) 160 200)))
                        (symbol-function 'smgr-write)
                        (lambda (node block page &optional (fork :main))
                          (when (= 22 (aref page 100))
                            (if (eq fault :nonlocal) (throw 'prefix-exit :escaped) (error original)))
                          (funcall write node block page fork))
                        (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (setf fenced condition
                                outside (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (error condition)))
                  (let ((caught (handler-case
                                    (catch 'prefix-exit
                                      (with-wal-critical (wal-settle-page-publications-under-lock)))
                                  (error (condition) condition)))
                        (early (member fault '(:flush :undurable))))
                    (rove:ok (typep caught 'error))
                    (rove:ok (eq caught fenced))
                    (rove:ok outside)
                    (rove:ok *wal-poisoned*)
                    (when (member fault '(:flush :write)) (rove:ok (eq original caught)))
                    (when (eq fault :undurable) (rove:ok (typep caught 'wal-input-validation-error)))
                    (rove:ok (eq (if early first-owner second-owner) *wal-pending-page-publication*))
                    (rove:ok (eq second-owner *wal-pending-page-publication-tail*))
                    (rove:ok (eq (if early :pending :released) (wal-page-publication-state first-owner)))
                    (rove:ok (eq :pending (wal-page-publication-state second-owner)))
                    (rove:ok (= (if early 2 1) (page-publication-test-refcount)))
                    (rove:ok (if early (null (smgr-read 7310 0))
                                 (= 11 (aref (smgr-read 7310 0) 100))))
                    (rove:ok (not (logical-spool-closed-p (wal-page-publication-spool second-owner))))))
             (setf (symbol-function 'wal-flush-core) flush
                   (symbol-function 'smgr-write) write
                   (symbol-function 'wal-fail-stop) stop)))))))

(rove:deftest xid-reservation-does-not-allocate-after-predecessor-failure
    (dolist (fault '(:error :nonlocal :preflight))
      (call-with-page-publication-test
       (lambda ()
         (let ((owner nil) (allocated nil) (entered nil) (fenced nil) (outside nil)
                           (flush (symbol-function 'wal-flush-core))
                           (stop (symbol-function 'wal-fail-stop))
                           (allocate (symbol-function 'xid-allocate-under-lock))
                           (original (make-condition 'simple-error :format-control "predecessor WAL failure")))
           (wal-buffer-write 7310 0 (page-init (make-page)))
           (sb-thread:with-mutex (*wal-lock*)
             (call-with-wal-primary-page-spool
              (lambda (spool publication)
                (page-publication-test-spool spool)
                (wal-prepare-page-publication-under-lock publication 44 nil nil)
                (page-publication-test-publish publication 140)
                (setf owner publication))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-flush-core)
                        (lambda ()
                          (setf entered t)
                          (assert (sb-thread:holding-mutex-p *wal-lock*))
                          (assert (not (sb-thread:holding-mutex-p *xid-lock*)))
                          (if (eq fault :nonlocal) (throw 'allocation-exit :escaped) (error original)))
                        (symbol-function 'xid-allocate-under-lock)
                        (lambda (&rest arguments) (declare (ignore arguments)) (setf allocated t) 999)
                        (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (setf fenced condition
                                outside (and (not (sb-thread:holding-mutex-p *wal-lock*))
                                             (not (sb-thread:holding-mutex-p *xid-lock*))))
                          (error condition)))
                  (let* ((*wal-storage-incarnation*
                           (if (eq fault :preflight) (gensym "WRONG-STORE-") *wal-storage-incarnation*))
                         (caught (handler-case
                                     (catch 'allocation-exit
                                       (xid-assign-with-wal-reservation 3 *backend*))
                                   (error (condition) condition))))
                    (rove:ok (typep caught 'error))
                    (rove:ok (not allocated))
                    (if (eq fault :preflight)
                        (progn
                          (rove:ok (typep caught 'wal-input-validation-error))
                          (rove:ok (null fenced))
                          (rove:ok (not entered))
                          (rove:ok (not *wal-poisoned*)))
                        (progn
                          (rove:ok (eq caught fenced))
                          (rove:ok outside)
                          (rove:ok entered)
                          (rove:ok *wal-poisoned*)))
                    (when (eq fault :error) (rove:ok (eq original caught)))
                    (rove:ok (eq owner *wal-pending-page-publication*))
                    (rove:ok (eq owner *wal-pending-page-publication-tail*))
                    (rove:ok (eq :pending (wal-page-publication-state owner)))
                    (rove:ok (= 1 (page-publication-test-refcount)))
                    (rove:ok (null (smgr-read 7310 0)))))
             (setf (symbol-function 'wal-flush-core) flush
                   (symbol-function 'wal-fail-stop) stop
                   (symbol-function 'xid-allocate-under-lock) allocate)))))))

(rove:deftest page-publication-prefix-rejects-invalid-target-before-any-write
    (call-with-page-publication-test
     (lambda ()
       (let ((first-owner nil) (second-owner nil))
         (sb-thread:with-mutex (*wal-lock*)
           (flet ((stage (marker xid end)
                    (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
                      (setf (backend-xid *backend*) xid (aref page 100) marker)
                      (wal-dirty-install-page (wal-page-key 7310 0) page))
                    (call-with-wal-primary-page-spool
                     (lambda (spool owner)
                       (page-publication-test-spool spool)
                       (wal-prepare-page-publication-under-lock owner xid nil nil)
                       (page-publication-test-publish owner end)
                       owner))))
             (setf first-owner (stage 11 44 140)
                   second-owner (stage 22 55 180)))
           (dolist (fault '(:undurable :foreign-target :incarnation :timeline :tail :skip-head :appender))
             (let ((*wal-durable-lsn* (if (eq fault :undurable) 160 200))
                   (*wal-storage-incarnation*
                     (if (eq fault :incarnation) (gensym "FOREIGN-STORE-") *wal-storage-incarnation*))
                   (*wal-insert-timeline* (if (eq fault :timeline) 2 1))
                   (*wal-pending-page-publication-tail*
                     (if (eq fault :tail) first-owner second-owner))
                   (*wal-page-publication-appender* first-owner))
               (rove:ok
                (handler-case
                    (progn
                      (case fault
                        (:skip-head (wal-drain-page-publication-under-lock second-owner))
                        (:appender (wal-append-record +wal-record-commit+ 99 #()))
                        (otherwise
                         (wal-drain-page-publications-through-under-lock
                          (if (eq fault :foreign-target)
                              (%make-wal-page-publication
                               :state :pending :terminal-end 180 :timeline *wal-insert-timeline*
                               :incarnation *wal-storage-incarnation*)
                              second-owner))))
                      nil)
                  (wal-input-validation-error () t))
                (format nil "~a is rejected before materializing the prefix" fault)))
             (rove:ok (eq first-owner *wal-pending-page-publication*))
             (rove:ok (eq second-owner *wal-pending-page-publication-tail*))
             (rove:ok (eq :pending (wal-page-publication-state first-owner)))
             (rove:ok (eq :pending (wal-page-publication-state second-owner)))
             (rove:ok (= 2 (page-publication-test-refcount)))
             (rove:ok (null (smgr-read 7310 0)))
             (rove:ok (not *wal-poisoned*)))
           (setf *wal-durable-lsn* 200)
           (wal-drain-page-publications-through-under-lock second-owner)
           (rove:ok (null *wal-pending-page-publication*))
           (rove:ok (null *wal-pending-page-publication-tail*))
           (rove:ok (= 22 (aref (smgr-read 7310 0) 100))))))))

(rove:deftest page-publication-rejects-undurable-or-foreign-drain
    (call-with-page-publication-test
     (lambda ()
       (let ((page (page-init (make-page))) (owner nil))
         (wal-buffer-write 7310 0 page)
         (sb-thread:with-mutex (*wal-lock*)
           (call-with-wal-primary-page-spool
            (lambda (spool publication)
              (page-publication-test-spool spool)
              (wal-prepare-page-publication-under-lock publication 44 nil nil)
              (page-publication-test-publish publication 140)
              (setf owner publication)))
           (dolist (fault '(:undurable :incarnation :timeline :owner))
             (let ((*wal-durable-lsn* (if (eq fault :undurable) 100 200))
                   (*wal-storage-incarnation*
                     (if (eq fault :incarnation) (gensym "OTHER-") *wal-storage-incarnation*))
                   (*wal-insert-timeline* (if (eq fault :timeline) 2 1)))
               (rove:ok
                (handler-case
                    (progn
                      (wal-drain-page-publication-under-lock
                       (if (eq fault :owner) (%make-wal-page-publication) owner))
                      nil)
                  (wal-input-validation-error () t))))
             (rove:ok (eq owner *wal-pending-page-publication*))
             (rove:ok (not (logical-spool-closed-p (wal-page-publication-spool owner))))
             (rove:ok (null (smgr-read 7310 0))))
           (setf *wal-durable-lsn* 140)
           (wal-drain-page-publication-under-lock owner))))))

(rove:deftest page-publication-blocks-lifecycle-and-later-publishers
    (call-with-page-publication-test
     (lambda ()
       (wal-buffer-write 7310 0 (page-init (make-page)))
       (let ((owner nil) (token *wal-storage-incarnation*)
                         (end *wal-end-lsn*) (durable *wal-durable-lsn*))
         (sb-thread:with-mutex (*wal-lock*)
           (call-with-wal-primary-page-spool
            (lambda (spool publication)
              (page-publication-test-spool spool)
              (wal-prepare-page-publication-under-lock publication 44 nil nil)
              (page-publication-test-publish publication 140)
              (setf owner publication))))
         (dolist (operation
                  (list #'wal-init
                        #'physical-recovery-open-storage
                        #'physical-promotion-adopt-live-child-runtime
                        (lambda () (physical-promotion-restore-cold-state nil nil))
                        (lambda () (let ((*wal-ready* nil))
                                     (wal-commit-transaction 55 #'identity)))
                        (lambda () (let ((*wal-ready* nil))
                                     (wal-commit-catalog #() #'identity)))
                        (lambda () (wal-forget-relation 7310))
                        (lambda ()
                          (sb-thread:with-mutex (*wal-lock*)
                            (wal-append-record +wal-record-commit+ 55 #())))
                        (lambda ()
                          (sb-thread:with-mutex (*wal-lock*)
                            (wal-replace-storage-incarnation-under-lock)))
                        (lambda ()
                          (sb-thread:with-mutex (*wal-lock*)
                            (wal-migrate-namespace-under-lock)))))
           (rove:ok (handler-case (progn (funcall operation) nil)
                      (wal-input-validation-error () t)))
           (rove:ok (eq owner *wal-pending-page-publication*))
           (rove:ok (eq token *wal-storage-incarnation*))
           (rove:ok (= end *wal-end-lsn*))
           (rove:ok (= durable *wal-durable-lsn*))
           (rove:ok (not *wal-poisoned*))
           (rove:ok (not (logical-spool-closed-p (wal-page-publication-spool owner))))
           (rove:ok (null (smgr-read 7310 0))))))))

(rove:deftest test-wal-scan-rejects-impossible-rotation-gap
    (let* ((size +wal-segment-size+)
           (end (+ size 64))
           (segments
             (list (list 0 (wal-encode-record +wal-record-batch-start+ 0 0 #())
                         (wal-encode-record +wal-record-maintenance+ 0 32 #()))
                   (list 1 (wal-encode-record +wal-record-batch-start+ 0 size #())
                         (wal-encode-record +wal-record-maintenance+ 0 (+ size 32) #())))))
      (call-with-wal-scan-test-files
       segments end
       (lambda ()
         (let ((seen nil))
           (rove:ok
            (handler-case
                (progn
                  (wal-scan (lambda (record) (push (wal-record-start record) seen)))
                  nil)
              (error (condition)
                (search "rotation gap" (princ-to-string condition))))
            "CRC-valid short segments cannot hide a missing durable suffix")
           (rove:ok (equal (reverse seen) '(0 32))
                    "the impossible successor never reaches the recovery callback"))
         (rove:ok
          (handler-case
              (progn
                (wal-scan-lineage
                 (lambda (&rest arguments) (declare (ignore arguments)))
                 (list (make-timeline-history-entry :timeline 1 :begin 0))
                 0 end :namespace :legacy)
                nil)
            (error (condition)
              (search "rotation gap" (princ-to-string condition))))
          "the primary and lineage readers reject the same impossible gap")
         (dolist (entry segments)
           (with-open-file (stream (wal-segment-path (first entry))
                                   :element-type '(unsigned-byte 8))
             (let ((expected (apply #'concatenate '(vector (unsigned-byte 8)) (rest entry)))
                   (actual (make-array (file-length stream) :element-type '(unsigned-byte 8))))
               (read-sequence actual stream)
               (rove:ok (equalp actual expected) "gap rejection leaves segment bytes intact"))))
         (rove:ok
          (handler-case
              (progn (wal-scan (constantly nil) 0 :end-lsn end) nil)
            (error (condition)
              (search "rotation gap" (princ-to-string condition))))
          "a bounded scan must reject an impossible gap before its requested end")
         (let ((*wal-durable-lsn* 64))
           (rove:ok
            (handler-case
                (progn (wal-scan (constantly nil)) nil)
              (error (condition)
                (search "rotation gap" (princ-to-string condition))))
            "a complete unflushed tail cannot be adopted across an impossible gap"))
         (rove:ok (= 64 (wal-scan (constantly nil) 0 :end-lsn 64))
                  "a bounded scan may stop before the damaged suffix")
         (rove:ok (= 64 (wal-scan (lambda (record)
                                    (when (= 64 (wal-record-end record)) :stop))))
                  "a callback may stop before the damaged suffix")
         (rove:ok (= end (wal-scan (constantly nil) size :end-lsn end))
                  "an exact retained scan does not inspect the preceding prefix")))))

(rove:deftest test-wal-scan-accepts-legal-rotation
    (let* ((size +wal-segment-size+)
           (prefix-end (- size 16))
           (end (+ size 64))
           (prefix (wal-encode-record
                    +wal-record-maintenance+ 0 0
                    (make-array (- prefix-end +wal-header-size+)
                                :element-type '(unsigned-byte 8) :initial-element 0))))
      (call-with-wal-scan-test-files
       (list (list 0 prefix)
             (list 1 (wal-encode-record +wal-record-batch-start+ 0 size #())
                   (wal-encode-record +wal-record-maintenance+ 0 (+ size 32) #())))
       end
       (lambda ()
         (let ((seen nil))
           (rove:ok (= end (wal-scan
                            (lambda (record) (push (wal-record-start record) seen)))))
           (rove:ok (equal (nreverse seen) (list 0 size (+ size 32)))
                    "a 32-byte successor may rotate past 16 bytes of unused capacity"))
         (rove:ok (= end (wal-scan (constantly nil) prefix-end :end-lsn end))
                  "an exact retained cursor may precede a legal rotation")
         (rove:ok
          (handler-case
              (progn (wal-scan (constantly nil) 0 :end-lsn (1- size)) nil)
            (error () t))
          "a bounded scan cannot end inside unused rotation capacity")))))

(rove:deftest test-wal-scan-exact-fit-and-full-segment-boundary
    (let* ((size +wal-segment-size+)
           (prefix-end (- size 32))
           (end (+ size 32))
           (prefix (wal-encode-record
                    +wal-record-maintenance+ 0 0
                    (make-array (- prefix-end +wal-header-size+)
                                :element-type '(unsigned-byte 8) :initial-element 0)))
           (fitting (wal-encode-record +wal-record-batch-start+ 0 prefix-end #()))
           (successor (wal-encode-record +wal-record-maintenance+ 0 size #())))
      (call-with-wal-scan-test-files
       (list (list 0 prefix fitting) (list 1 successor)) end
       (lambda ()
         (let ((seen nil))
           (rove:ok (= end (wal-scan
                            (lambda (record) (push (wal-record-start record) seen)))))
           (rove:ok (equal (nreverse seen) (list 0 prefix-end size))
                    "a fitting record fills its segment before the next record rotates"))
         (rove:ok (= end (wal-scan (constantly nil) prefix-end :end-lsn end)))
         (rove:ok (= end (wal-scan (constantly nil) size :end-lsn end)))
         (rove:ok (= size (wal-scan (constantly nil) 0 :end-lsn size)))))
      (call-with-wal-scan-test-files
       (list (list 0 prefix) (list 1 successor)) end
       (lambda ()
         (rove:ok
          (handler-case
              (progn (wal-scan (constantly nil)) nil)
            (error (condition)
              (search "rotation gap" (princ-to-string condition))))
          "a successor that exactly fits the remaining capacity cannot rotate")))))

(rove:deftest test-wal-scan-rejects-empty-intermediate-segment
    (let* ((size +wal-segment-size+)
           (prefix-end (- size 16))
           (prefix (wal-encode-record
                    +wal-record-maintenance+ 0 0
                    (make-array (- prefix-end +wal-header-size+)
                                :element-type '(unsigned-byte 8) :initial-element 0))))
      (call-with-wal-scan-test-files
       (list (list 0 prefix) (list 1)
             (list 2 (wal-encode-record +wal-record-maintenance+ 0 (* 2 size) #())))
       (+ (* 2 size) 32)
       (lambda ()
         (let ((seen nil))
           (rove:ok
            (handler-case
                (progn
                  (wal-scan (lambda (record) (push (wal-record-start record) seen)))
                  nil)
              (error (condition)
                (search "rotation gap" (princ-to-string condition))))
            "consecutive filenames cannot hide an empty intermediate segment")
           (rove:ok (equal seen '(0)) "records after the empty segment are not visited"))))))

(rove:deftest test-wal-scan-preserves-durable-tail-repair-boundary
    (let* ((start (wal-encode-record +wal-record-batch-start+ 0 0 #()))
           (terminal (wal-encode-record +wal-record-maintenance+ 0 32 #()))
           (tail (wal-encode-record
                  +wal-record-maintenance+ 0 64
                  (make-array 8 :element-type '(unsigned-byte 8) :initial-element 0)))
           (bad-crc (copy-seq tail)))
      (setf (aref bad-crc 28) (logxor 1 (aref bad-crc 28)))
      (dolist (damaged (list (subseq tail 0 7) (subseq tail 0 35) bad-crc))
        (call-with-wal-scan-test-files
         (list (list 0 start terminal damaged)) 64
         (lambda ()
           (rove:ok (= 64 (wal-scan (constantly nil)))
                    "an incomplete or corrupt unflushed tail is discarded")
           (with-open-file (stream (wal-segment-path 0) :element-type '(unsigned-byte 8))
             (rove:ok (= 64 (file-length stream))))))
        (call-with-wal-scan-test-files
         (list (list 0 start terminal damaged)) 104
         (lambda ()
           (rove:ok
            (handler-case
                (progn (wal-scan (constantly nil)) nil)
              (error () t))
            "the same damage before the durable boundary stops the scan")
           (with-open-file (stream (wal-segment-path 0) :element-type '(unsigned-byte 8))
             (rove:ok (= (+ 64 (length damaged)) (file-length stream))
                      "durable corruption is never truncated")))))))

(rove:deftest test-wal-catalog-codec-and-page-checksums
    (let* ((catalog (make-array 17 :element-type '(unsigned-byte 8)
                                   :initial-element 65))
           (payload (wal-encode-catalog-payload catalog '(123 456))))
      (multiple-value-bind (deletes decoded)
          (wal-decode-catalog-payload payload)
        (rove:ok (equal deletes '(123 456)) "WAL catalog delete list round-trips")
        (rove:ok (equalp decoded catalog) "WAL catalog image round-trips"))
      (let ((corrupt (copy-seq payload)))
        (decf (aref corrupt 12))
        (rove:ok (handler-case
                     (progn (wal-decode-catalog-payload corrupt) nil)
                   (error () t))
                 "WAL catalog image length mismatch is rejected")))
  (let ((page (page-init (make-page))))
    (rove:ok (wal-verify-page page 123 4) "unstamped page is accepted for staging")
    (wal-stamp-page page 99 123 4)
    (rove:ok (wal-verify-page page 123 4) "WAL page checksum verifies at its location")
    (rove:ok (not (wal-verify-page page 123 5))
             "WAL page checksum binds the block number")
    (setf (aref page 100) (logxor (aref page 100) #xFF))
    (rove:ok (not (wal-verify-page page 123 4))
             "WAL page checksum detects page damage")))

(rove:deftest test-wal-record-codec
    (let* ((payload (make-array 0 :element-type '(unsigned-byte 8)))
           (record (wal-encode-record +wal-record-commit+ 42 64 payload))
           (decoded (wal-decode-record record 64)))
      (rove:ok (= (wal-record-type decoded) +wal-record-commit+)
               "WAL record type round-trips")
      (rove:ok (= (wal-record-xid decoded) 42) "WAL record XID round-trips")
      (rove:ok (= (wal-record-start decoded) 64) "WAL record LSN round-trips")
      (rove:ok (= (wal-record-end decoded) (+ 64 +wal-header-size+))
               "WAL record end LSN is computed")
      (rove:ok (equalp (wal-record-payload decoded) payload)
               "WAL record payload round-trips")
      (dolist (case
                  (list
                   (cons "bad WAL record magic is rejected"
                         (lambda (bytes) (setf (aref bytes 0) 0)))
                   (cons "unsupported WAL record version is rejected"
                         (lambda (bytes) (wal-set-u16 bytes 4 1)))
                   (cons "unknown WAL record type is rejected"
                         (lambda (bytes) (wal-set-u16 bytes 6 #xFFFF)))
                   (cons "bad WAL record length is rejected"
                         (lambda (bytes) (wal-set-u32 bytes 8 31)))
                   (cons "WAL record at the wrong LSN is rejected"
                         (lambda (bytes) (wal-set-u64 bytes 12 65)))
                   (cons "bad WAL record checksum is rejected"
                         (lambda (bytes)
                           (setf (aref bytes 20) (logxor (aref bytes 20) #xFF))))))
        (let ((corrupt (copy-seq record)))
          (funcall (cdr case) corrupt)
          (rove:ok (handler-case
                       (progn (wal-decode-record corrupt 64) nil)
                     (error () t))
                   (car case))))))

(rove:deftest test-wal-page-image-xid-codec-compatibility
    (let* ((lsn 4096)
           (page (page-init (make-page)))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page page
                                       :xids '(7201 7202)))
           (payload (wal-encode-page-images (list image) lsn))
           (decoded (first (wal-decode-page-images payload lsn +wal-version+)))
           (no-xid-image (make-wal-page-image
                          :relfilenode 7311 :block 4 :page (page-init (make-page))))
           (one-xid-image (make-wal-page-image
                           :relfilenode 7312 :block 5 :page (page-init (make-page))
                           :xids '(7203)))
           (multi-payload
             (wal-encode-page-images (list image no-xid-image one-xid-image) lsn))
           (multi-decoded
             (wal-decode-page-images multi-payload lsn +wal-version+))
           (legacy-v3 (make-array (+ 4 12 +blcksz+)
                                  :element-type '(unsigned-byte 8)
                                  :initial-element 0))
           (legacy-v2 (make-array (+ 4 8 +blcksz+)
                                  :element-type '(unsigned-byte 8)
                                  :initial-element 0)))
      (rove:ok (equal (wal-page-image-xids decoded) '(7201 7202))
               "WAL v4 page images retain transaction contributors")
      (rove:ok (= (length payload) (+ 4 12 +blcksz+ 4 (* 8 2)))
               "WAL v4 page-image length includes variable contributor data")
      (rove:ok (equal (mapcar #'wal-page-image-xids multi-decoded)
                      '((7201 7202) nil (7203)))
               "WAL v4 multi-page records retain each image's contributor list")
      (wal-set-u32 legacy-v3 0 1)
      (wal-set-u32 legacy-v3 4 7310)
      (wal-set-u32 legacy-v3 8 0)
      (wal-set-u32 legacy-v3 12 3)
      (replace legacy-v3 page :start1 16)
      (wal-set-u32 legacy-v2 0 1)
      (wal-set-u32 legacy-v2 4 7310)
      (wal-set-u32 legacy-v2 8 3)
      (replace legacy-v2 page :start1 12)
      (rove:ok (null (wal-page-image-xids
                      (first (wal-decode-page-images legacy-v3 lsn +wal-version-v3+))))
               "WAL v3 page images remain readable without contributor metadata")
      (rove:ok (null (wal-page-image-xids
                      (first (wal-decode-page-images legacy-v2 lsn +wal-version-v2+))))
               "WAL v2 page images remain readable without fork metadata")))

(rove:deftest test-wal-primary-chunked-pages-match-legacy
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-pages-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*query-temp-file-limit-bytes* 0)
           (*logical-capture-memory-bytes* 1)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (old-start (symbol-function 'wal-next-record-start))
           (old-append (symbol-function 'wal-append-record))
           (position 100) (records nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (loop for block from 512 downto 0
                   for key = (wal-page-key 7310 block)
                   do (setf (gethash key *wal-page-cache*) (page-init (make-page))
                            (gethash key *wal-page-cache-xids*)
                            (if (evenp block) '(1 44 45) '(0 1 55))))
             (let ((excluded (wal-page-key 7311 0)))
               (setf (gethash excluded *wal-page-cache*) (page-init (make-page))))
             (setf (symbol-function 'wal-next-record-start)
                   (lambda (length) (declare (ignore length)) position)
                   (symbol-function 'wal-append-record)
                   (lambda (type xid payload)
                     (declare (ignore xid))
                     (incf position (+ +wal-header-size+ (length payload)))
                     (push (list type position payload) records)
                     position))
             (let* ((legacy (wal-page-images '(7311)))
                    (keys (wal-primary-page-keys '(7311))))
               (wal-append-page-images legacy)
               (let ((expected (nreverse records)))
                 (setf records nil position 100)
                 (sb-thread:with-mutex (*wal-lock*)
                   (with-wal-primary-page-spool (spool)
                     (wal-primary-preflight-page-keys keys)
                     (wal-primary-append-pages keys spool)
                     (wal-primary-seal-page-spool spool)
                     (rove:ok (= 2 (length records)))
                     (rove:ok (equalp expected (nreverse records)))
                     (let ((count 0) (last-block -1))
                       (wal-primary-map-page-spool
                        spool
                        (lambda (images)
                          (dolist (image images)
                            (incf count)
                            (rove:ok (> (wal-page-image-block image) last-block))
                            (setf last-block (wal-page-image-block image)))))
                       (rove:ok (= 513 count))))))
               (rove:ok (null (uiop:directory-files directory)))))
        (setf (symbol-function 'wal-next-record-start) old-start
              (symbol-function 'wal-append-record) old-append)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-primary-split-pause-once-across-chunks
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-split-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*logical-capture-memory-bytes* 1)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-test-pause-relfilenode* 7311)
           (old-start (symbol-function 'wal-next-record-start))
           (old-append (symbol-function 'wal-append-record))
           (old-write (symbol-function 'smgr-write))
           (old-pause (symbol-function 'wal-test-pause))
           (position 100) (writes 0) (pauses 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (loop for block below 512
                   do (setf (gethash (wal-page-key 7310 block) *wal-page-cache*)
                            (page-init (make-page))))
             (loop for block from 1 to 514
                   do (setf (gethash (wal-page-key 7311 block) *wal-page-cache*)
                            (page-init (make-page))))
             (setf (symbol-function 'wal-next-record-start)
                   (lambda (length) (declare (ignore length)) position)
                   (symbol-function 'wal-append-record)
                   (lambda (type xid payload)
                     (declare (ignore type xid))
                     (incf position (+ +wal-header-size+ (length payload))))
                   (symbol-function 'smgr-write)
                   (lambda (node block page &optional fork)
                     (declare (ignore node block page fork))
                     (incf writes))
                   (symbol-function 'wal-test-pause)
                   (lambda (point)
                     (when (eq point :after-btree-split-page-write) (incf pauses))))
             (sb-thread:with-mutex (*wal-lock*)
               (let ((keys (wal-primary-page-keys)))
                 (with-wal-primary-page-spool (spool)
                   (wal-primary-preflight-page-keys keys)
                   (wal-primary-append-pages keys spool)
                   (wal-primary-seal-page-spool spool)
                   (wal-primary-apply-page-spool spool :split-p t :fault-on-first-write t))))
             (rove:ok (= 1026 writes))
             (rove:ok (= 1 pauses))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-next-record-start) old-start
              (symbol-function 'wal-append-record) old-append
              (symbol-function 'smgr-write) old-write
              (symbol-function 'wal-test-pause) old-pause)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-primary-page-contributor-retention
    (let* ((*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (*wal-replication-slots* (make-hash-table :test #'equal))
           (*active-xids* '(44 55 66))
           (slot (make-wal-replication-slot :name "retained"))
           (one (wal-page-key 7310 0))
           (two (wal-page-key 7310 1)))
      (setf (gethash "retained" *wal-replication-slots*) slot
            (gethash one *wal-page-cache*) (page-init (make-page))
            (gethash two *wal-page-cache*) (page-init (make-page))
            (gethash one *wal-page-cache-xids*) '(1 44 55)
            (gethash two *wal-page-cache-xids*) '(0 1 55 77)
            (gethash 44 *wal-xid-first-lsn*) 10)
      (wal-primary-record-page-xids (wal-primary-page-keys) 100)
      (rove:ok (= 10 (gethash 44 *wal-xid-first-lsn*)))
      (rove:ok (= 100 (gethash 55 *wal-xid-first-lsn*)))
      (rove:ok (null (gethash 77 *wal-xid-first-lsn*)))
      (rove:ok (equal '(10) (gethash 44 (wal-replication-slot-pending-xids slot))))
      (rove:ok (equal '(100) (gethash 55 (wal-replication-slot-pending-xids slot))))
      (rove:ok (null (gethash 66 (wal-replication-slot-pending-xids slot))))))

(rove:deftest test-wal-primary-empty-page-spool
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-empty-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (visits 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*wal-lock*)
               (with-wal-primary-page-spool (spool)
                 (let ((keys (wal-primary-page-keys)))
                   (wal-primary-preflight-page-keys keys)
                   (wal-primary-append-pages keys spool)
                   (wal-primary-seal-page-spool spool)
                   (wal-primary-map-page-spool spool
                                               (lambda (images) (declare (ignore images)) (incf visits))))))
             (rove:ok (zerop visits))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-truncate-replacement-without-wal
    (let* ((directory (merge-pathnames
                       (format nil "pg-truncate-no-wal-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* directory)
           (*wal-ready* nil)
           (*wal-poisoned* nil)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*relation-streams* (make-hash-table :test #'equal))
           (*transaction-relation-replacements* (make-hash-table :test #'eq))
           (*backend* (make-backend :xid 44))
           (original (page-init (make-page)))
           (replacement (page-init (make-page)))
           (completed nil))
      (ensure-directories-exist (merge-pathnames "base/" directory))
      (unwind-protect
           (progn
             (setf (aref original 100) 42
                   (aref replacement 100) 99)
             (smgr-write 7310 0 original)
             (smgr-write 7310 1 original)
             (transaction-replace-relation 7310)
             (rove:ok (zerop (buf-nblocks 7310)))
             (buf-extend 7310 replacement)
             (rove:ok (= 99 (aref (buf-read 7310 0) 100)))
             (rove:ok (= 42 (aref (smgr-read 7310 0) 100))
                      "private writes do not erase the abort image")
             (discard-transaction-replacements *backend*)
             (rove:ok (= 42 (aref (buf-read 7310 0) 100))
                      "aborting a no-WAL replacement retains original storage")
             (transaction-replace-relation 7310)
             (buf-extend 7310 replacement)
             (wal-commit-transaction
              44
              (lambda (end)
                (rove:ok (null end))
                (rove:ok (= 99 (aref (smgr-read 7310 0) 100))
                         "replacement reaches storage before commit completion")
                (discard-transaction-replacements *backend*)
                (setf completed t)))
             (rove:ok completed)
             (smgr-forget 7310)
             (rove:ok (= 99 (aref (buf-read 7310 0) 100))
                      "committed replacement survives closing cached relation streams")
             (rove:ok (and (= 1 (smgr-nblocks 7310)) (null (buf-read 7310 1)))
                      "old physical tail is removed after commit")
             (let ((*wal-ready* t)
                   (*wal-row-page-write-p* t))
               (transaction-replace-relation 7310)
               (buf-extend 7310 replacement)
               (with-wal-critical
                 (publish-transaction-replacements-under-wal-lock *backend*))
               (rove:ok (member 0 (gethash (wal-page-key 7310 0) *wal-page-cache-xids*))
                        "destructive replacement cannot inherit row-replay certification")
               (discard-transaction-replacements *backend*)
               (wal-clear-page-cache)))
        (discard-transaction-replacements *backend*)
        (wal-clear-page-cache)
        (smgr-close-all)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-relation-reset-batches
    (let* ((directory (merge-pathnames
                       (format nil "pg-reset-redo-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* directory)
           (*relation-streams* (make-hash-table :test #'equal))
           (original (page-init (make-page)))
           (replacement (page-init (make-page)))
           (clog-set (symbol-function 'clog-set))
           (committed nil))
      (ensure-directories-exist (merge-pathnames "base/" directory))
      (setf (aref original 100) 42 (aref replacement 100) 99)
      (unwind-protect
           (progn
             (setf (symbol-function 'clog-set)
                   (lambda (xid status) (push (list xid status) committed)))
             (rove:ok (loop for version from 2 to 7
                            never (wal-valid-type-p +wal-record-relation-reset+ version)))
             (rove:ok (wal-valid-type-p +wal-record-relation-reset+ 8))
             (rove:ok (wal-valid-type-p +wal-record-running-xacts+ 7))
             (dolist (spool-p '(nil t))
               (smgr-reset-fork 7311 :main)
               (smgr-write 7311 0 original)
               (smgr-write 7311 1 original)
               (let ((*wal-recovery-page-spool-enabled* spool-p)
                     (*wal-checkpoint-lsn* 0)
                     (cursor 0))
                 (with-wal-replay-staging ()
                   (flet ((record (type xid payload)
                            (prog1 (make-wal-record :version 8 :type type :xid xid
                                                    :start cursor
                                                    :end (+ cursor +wal-header-size+ (length payload))
                                                    :payload payload)
                              (incf cursor (+ +wal-header-size+ (length payload))))))
                     (wal-replay-record (record +wal-record-batch-start+ 0 #()))
                     (wal-replay-record (record +wal-record-relation-reset+ 44
                                                (wal-encode-relation-reset 7311 :main)))
                     (rove:ok (= 2 (smgr-nblocks 7311)) "an unterminated reset changes no storage")
                     (rove:ok (handler-case
                                  (progn (wal-recovery-validate-logical-terminal :other) nil)
                                (error () t))
                              "a reset cannot attach to a maintenance or catalog terminal")
                     (rove:ok (handler-case
                                  (progn (wal-recovery-validate-logical-terminal :commit 45) nil)
                                (error () t))
                              "a reset belongs to its exact committing transaction")
                     ;; A new batch discards a reset left by a crashed transaction.
                     (wal-replay-record (record +wal-record-batch-start+ 0 #()))
                     (wal-replay-record (record +wal-record-commit+ 45
                                                (wal-encode-commit-timestamp 0)))
                     (rove:ok (= 2 (smgr-nblocks 7311)) "a new batch drops incomplete reset work")
                     (wal-replay-record (record +wal-record-batch-start+ 0 #()))
                     (wal-replay-record (record +wal-record-relation-reset+ 44
                                                (wal-encode-relation-reset 7311 :main)))
                     (let* ((image (make-wal-page-image :relfilenode 7311 :block 0
                                                        :page (copy-seq replacement)))
                            (length (wal-page-payload-length (list image)))
                            (payload (wal-encode-page-images
                                      (list image) (+ cursor +wal-header-size+ length))))
                       (wal-replay-record (record +wal-record-pages+ 0 payload)))
                     (rove:ok (handler-case
                                  (progn
                                    (wal-recovery-stage-relation-reset
                                     (record +wal-record-relation-reset+ 44
                                             (wal-encode-relation-reset 7311 :visibility)))
                                    nil)
                                (error () t))
                              "reset records cannot follow page images")
                     (wal-replay-record (record +wal-record-commit+ 44
                                                (wal-encode-commit-timestamp 0)))
                     (rove:ok (= 1 (smgr-nblocks 7311)))
                     (rove:ok (= 99 (aref (smgr-read 7311 0) 100)))
                     (rove:ok (null (smgr-read 7311 1)))
                     (smgr-fsync-all)
                     (smgr-forget 7311)
                     (rove:ok (= 1 (smgr-nblocks 7311)) "reset survives closing buffered streams")))))
             (rove:ok (member '(44 1) committed :test #'equal)))
        (setf (symbol-function 'clog-set) clog-set)
        (smgr-close-all)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-relation-reset-rejects-invalid-storage-work
    (let* ((directory (merge-pathnames
                       (format nil "pg-reset-invalid-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory) (*query-temp-directory* directory)
           (*relation-streams* (make-hash-table :test #'equal))
           (*wal-recovery-page-spool-enabled* t)
           (*logical-capture-memory-bytes* 0)
           (*wal-checkpoint-lsn* 0)
           (original (page-init (make-page)))
           (payload (wal-encode-relation-reset 7312 :main))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10))
           (reset (make-wal-record :version 8 :type +wal-record-relation-reset+
                                   :xid 44 :payload payload :end 50)))
      (ensure-directories-exist (merge-pathnames "base/" directory))
      (unwind-protect
           (progn
             (setf (aref original 100) 42)
             (smgr-write 7312 0 original)
             (smgr-write 7312 1 original)
             (dolist (bad (list
                           (make-wal-record :version 8 :xid 44 :payload #(0))
                           (make-wal-record :version 8 :xid 44
                                            :payload (make-array 8 :element-type '(unsigned-byte 8)
                                                                   :initial-element 0))
                           (make-wal-record :version 8 :xid 44
                                            :payload (let ((bytes (copy-seq payload)))
                                                       (wal-set-u32 bytes 4 2) bytes))
                           (make-wal-record :version 8 :xid 0 :payload payload)
                           (make-wal-record :version 7 :xid 44 :payload payload)))
               (rove:ok (handler-case (progn (wal-decode-relation-reset bad) nil) (error () t))))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record reset)
               (rove:ok (handler-case (progn (wal-replay-record reset) nil) (error () t))
                        "duplicate resets do not create ambiguous storage work"))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record reset)
               (let* ((image (make-wal-page-image :relfilenode 7312 :block 0
                                                  :page (page-init (make-page))))
                      (one (make-wal-record :version 8 :type +wal-record-pages+ :end 100
                                            :payload (wal-encode-page-images (list image) 100)))
                      (two (make-wal-record :version 8 :type +wal-record-pages+ :end 200
                                            :payload (wal-encode-page-images (list image) 200))))
                 (wal-replay-record one)
                 (wal-replay-record two)
                 (let ((file (logical-spool-stream *wal-recovery-page-spool*))
                       (offset (+ 8 12 (length (wal-record-payload one)) 8 12 20)))
                   (finish-output file)
                   (file-position file offset)
                   (write-byte 99 file)
                   (finish-output file))
                 (rove:ok
                  (handler-case
                      (progn
                        (wal-replay-record
                         (make-wal-record :version 8 :type +wal-record-commit+ :xid 44 :end 300
                                          :payload (wal-encode-commit-timestamp 0)))
                        nil)
                    (error (condition) (search "checksum mismatch" (princ-to-string condition))))
                  "all page records validate before a committed reset touches storage")
                 (rove:ok (= 2 (smgr-nblocks 7312)))
                 (rove:ok (= 42 (aref (smgr-read 7312 0) 100)))
                 (rove:ok (= 42 (aref (smgr-read 7312 1) 100))))))
        (smgr-close-all)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-shared-and-batch-lifecycle
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-store-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*query-temp-file-limit-bytes* 0)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (*backend* (make-backend :xid 44))
           (*wal-row-page-write-p* t)
           (page (page-init (make-page))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (aref page 100) 1)
             (wal-buffer-write 7310 0 page)
             (rove:ok (= 0 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (= 1 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (rove:ok (= 1 (aref (wal-buffer-read 7310 0) 100)))
             (rove:ok (equal '(44 1) (gethash (wal-page-key 7310 0) *wal-page-cache-xids*)))
             (rove:ok (handler-case
                          (with-wal-page-batch
                            (setf (aref page 100) 2)
                            (wal-buffer-write 7310 0 page)
                            (wal-buffer-write 7310 1 page)
                            (error "discard batch"))
                        (error (condition) (search "discard batch" (princ-to-string condition)))))
             (rove:ok (= 1 (aref (wal-buffer-read 7310 0) 100)))
             (rove:ok (= 1 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (setf (backend-xid *backend*) 55)
             (with-wal-page-batch
               (wal-buffer-write 7310 0 page)
               (wal-buffer-write 7310 1 page))
             (rove:ok (= 2 (aref (wal-buffer-read 7310 0) 100)))
             (rove:ok (= 2 (wal-buffer-nblocks 7310)))
             (rove:ok (equal '(44 55) (sort (remove-if (lambda (xid) (< xid 3))
                                                       (copy-list (gethash (wal-page-key 7310 0)
                                                                           *wal-page-cache-xids*))) #'<)))
             (let ((next (wal-dirty-store-next-slot *wal-dirty-store*)))
               (dotimes (i 5) (wal-buffer-write 7310 0 page))
               (rove:ok (<= (wal-dirty-store-next-slot *wal-dirty-store*) (1+ next))))
             (clear-wal-test-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (clear-wal-test-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-seeded-batch-model
    ;; Fixed LCG seed makes every failed batch and overwritten slot reproducible.
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-model-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*query-temp-file-limit-bytes* 0)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (*backend* (make-backend :xid 44))
           (*wal-row-page-write-p* t)
           (model (make-array 12 :initial-element nil))
           (seed 1299709)
           (rolled-back 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (labels ((draw (bound)
                      (setf seed (mod (+ (* seed 1664525) 1013904223) 4294967296))
                      (mod (ash seed -8) bound))
                    (page-with (value)
                      (let ((page (page-init (make-page))))
                        (setf (aref page 100) value)
                        page)))
             (dotimes (batch 300)
               (let ((updates nil)
                     (abort-p (zerop (draw 4))))
                 (dotimes (i (1+ (draw 5)))
                   (push (cons (draw 12) (draw 256)) updates))
                 (setf updates (nreverse updates))
                 (catch 'rollback
                   (with-wal-page-batch
                     (dolist (update updates)
                       (wal-buffer-write 7310 (car update) (page-with (cdr update))))
                     (when abort-p (throw 'rollback nil))))
                 (if abort-p
                     (incf rolled-back)
                     (dolist (update updates)
                       (setf (aref model (car update)) (cdr update))))
                 (dotimes (key 12)
                   (multiple-value-bind (reference present)
                       (gethash (wal-page-key 7310 key) *wal-page-cache*)
                     (declare (ignore reference))
                     (rove:ok (eql present (not (null (aref model key)))))
                     (when present
                       (rove:ok (= (aref model key) (aref (wal-buffer-read 7310 key) 100))))))))
             (rove:ok (> rolled-back 0))
             (rove:ok (= 0 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (= (hash-table-count *wal-page-cache*) (wal-dirty-store-disk-refs *wal-dirty-store*)))
             ;; At most 12 shared and five private references coexist.
             (rove:ok (<= (wal-dirty-store-next-slot *wal-dirty-store*) 17))
             (clear-wal-test-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (clear-wal-test-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-resident-budget
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-budget-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*wal-dirty-resident-limit-pages* 2)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (with-wal-page-batch
               (dotimes (block 6)
                 (let ((page (page-init (make-page))))
                   (setf (aref page 100) block)
                   (wal-buffer-write 7310 block page))))
             (rove:ok (= 2 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (= 4 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (dotimes (block 6)
               (rove:ok (= block (aref (wal-buffer-read 7310 block) 100))))
             (rove:ok (= 2 (wal-dirty-store-resident *wal-dirty-store*)))
             (clear-wal-test-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (clear-wal-test-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-corruption-fails-stop
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-corrupt-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (old-fail (symbol-function 'wal-fail-stop))
           (old-read (symbol-function 'smgr-read))
           (fallback nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop) (lambda (condition) (error condition))
                   (symbol-function 'smgr-read)
                   (lambda (&rest args) (declare (ignore args)) (setf fallback t) nil))
             (wal-buffer-write 7310 0 (page-init (make-page)))
             (let ((file (logical-spool-stream (wal-dirty-store-spool *wal-dirty-store*))))
               (file-position file 100)
               (write-byte 99 file)
               (finish-output file))
             (rove:ok (handler-case
                          (progn (wal-buffer-read 7310 0) nil)
                        (wal-dirty-store-error (condition)
                          (search "checksum mismatch" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (not fallback))
             (clear-wal-test-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-fail-stop) old-fail
              (symbol-function 'smgr-read) old-read)
        (clear-wal-test-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-write-and-batch-failures-preserve-shared
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-failure-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*wal-dirty-resident-limit-pages* 1)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (*backend* (make-backend :xid 44))
           (*wal-row-page-write-p* t)
           (old-fail (symbol-function 'wal-fail-stop))
           (old-ensure (symbol-function 'wal-dirty-ensure-spool))
           (old-merge (symbol-function 'wal-dirty-batch-contributors))
           (page (page-init (make-page)))
           (key (wal-page-key 7310 0)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop) (lambda (condition) (error condition)))
             (setf (aref page 100) 1)
             (wal-buffer-write 7310 0 page)
             (setf (aref page 100) 2
                   (symbol-function 'wal-dirty-ensure-spool)
                   (lambda (store) (declare (ignore store)) (error "injected scratch failure")))
             (rove:ok (handler-case (progn (wal-buffer-write 7310 0 page) nil)
                        (wal-dirty-store-error (condition)
                          (search "injected scratch failure" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (= 1 (aref (wal-test-read-dirty-reference key (gethash key *wal-page-cache*)) 100)))
             (rove:ok (equal '(44 1) (gethash key *wal-page-cache-xids*)))
             (setf (symbol-function 'wal-dirty-ensure-spool) old-ensure
                   *wal-poisoned* nil)
             (setf (symbol-function 'wal-dirty-batch-contributors)
                   (lambda ()
                     (error "injected batch publication failure")))
             (rove:ok (handler-case
                          (with-wal-page-batch
                            (wal-buffer-write 7310 0 page)
                            (wal-buffer-write 7310 1 page))
                        (wal-dirty-store-error (condition)
                          (search "injected batch publication failure" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (= 1 (hash-table-count *wal-page-cache*)))
             (rove:ok (= 1 (aref (wal-test-read-dirty-reference key (gethash key *wal-page-cache*)) 100)))
             (rove:ok (= 1 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (zerop (wal-dirty-store-disk-refs *wal-dirty-store*))))
        (setf (symbol-function 'wal-fail-stop) old-fail
              (symbol-function 'wal-dirty-ensure-spool) old-ensure
              (symbol-function 'wal-dirty-batch-contributors) old-merge)
        (clear-wal-test-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-batch-publication-error-fails-stop-with-owned-pages
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-publish-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-temp-cleanup-domain* nil)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (old-fail (symbol-function 'wal-fail-stop))
           (old-release (symbol-function 'wal-dirty-release-reference))
           (page (page-init (make-page)))
           (key (wal-page-key 7310 0))
           (previous nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop) (lambda (condition) (error condition)))
             (setf (aref page 100) 1)
             (wal-buffer-write 7310 0 page)
             (setf previous (gethash key *wal-page-cache*)
                   (aref page 100) 2
                   (symbol-function 'wal-dirty-release-reference)
                   (lambda (reference)
                     (if (eq reference previous)
                         (error "injected published-reference failure")
                         (funcall old-release reference))))
             (rove:ok
              (handler-case
                  (with-wal-page-batch
                    (wal-buffer-write 7310 0 page)
                    (wal-buffer-write 7310 1 page))
                (wal-dirty-store-error (condition)
                  (search "injected published-reference failure" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (= 2 (hash-table-count *wal-page-cache*)))
             (rove:ok (= 3 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (dolist (block '(0 1))
               (rove:ok (not (member (wal-dirty-reference-slot
                                      (gethash (wal-page-key 7310 block) *wal-page-cache*))
                                     (wal-dirty-store-free-slots *wal-dirty-store*)))))
             (rove:ok (= 2 (aref (wal-test-read-dirty-reference key (gethash key *wal-page-cache*)) 100)))
             (rove:ok (= 2 (aref (wal-test-read-dirty-reference
                                  (wal-page-key 7310 1)
                                  (gethash (wal-page-key 7310 1) *wal-page-cache*)) 100))))
        (setf (symbol-function 'wal-fail-stop) old-fail
              (symbol-function 'wal-dirty-release-reference) old-release)
        (when previous (wal-dirty-release-reference previous))
        (clear-wal-test-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-cross-thread-contributors
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-threads-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (names '(*wal-temp-cleanup-domain* *query-temp-directory* *wal-dirty-resident-limit-pages* *wal-dirty-store*
                    *wal-page-cache* *wal-page-cache-xids* *wal-ready* *wal-poisoned*))
           (saved (mapcar (lambda (name) (cons name (symbol-value name))) names))
           (key (wal-page-key 7310 0)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             ;; Child threads use the same global WAL cache and scratch owner.
             (setf *wal-temp-cleanup-domain* nil
                   *query-temp-directory* directory
                   *wal-dirty-resident-limit-pages* 0
                   *wal-dirty-store* nil
                   *wal-page-cache* (make-hash-table :test #'equal)
                   *wal-page-cache-xids* (make-hash-table :test #'equal)
                   *wal-ready* t *wal-poisoned* nil)
             (let* ((gate (sb-thread:make-semaphore :count 0))
                    (threads
                      (loop for xid in '(44 55)
                            collect
                            (let ((id xid))
                              (sb-thread:make-thread
                               (lambda ()
                                 (let ((*backend* (make-backend :xid id))
                                       (*wal-row-page-write-p* t)
                                       (page (page-init (make-page))))
                                   (setf (aref page 100) id)
                                   (sb-thread:wait-on-semaphore gate)
                                   (wal-buffer-write 7310 0 page))))))))
               (sb-thread:signal-semaphore gate 2)
               (dolist (thread threads) (sb-thread:join-thread thread)))
             (rove:ok (member (aref (wal-buffer-read 7310 0) 100) '(44 55)))
             (rove:ok (equal '(44 55) (sort (remove-if (lambda (xid) (< xid 3))
                                                       (copy-list (gethash key *wal-page-cache-xids*))) #'<)))
             (rove:ok (= 1 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (clear-wal-test-page-cache)
             (rove:ok (null (uiop:directory-files directory))))
        (clear-wal-test-page-cache)
        (dolist (entry saved) (setf (symbol-value (car entry)) (cdr entry)))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-commit-returns-own-terminal-end
    (let* ((directory (merge-pathnames
                       (format nil "pg-commit-end-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*pgdata* (merge-pathnames "data/" directory))
           (*clog-stream* nil) (*clog-pending-commits* (make-hash-table :test #'eql))
           (*wal-pending-page-publication-tail* nil)
           (*physical-recovery-failed-p* nil)
           (*backend* nil)
           (*wal-ready* t)
           (*wal-poisoned* nil)
           (*wal-output-stream* nil)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-end-lsn* 100) (*wal-durable-lsn* 100) (*wal-pending-page-publication* nil)
           (names '(wal-append-running-xacts-under-lock wal-append-batch-start
                    wal-append-record wal-flush wal-slot-record-xid-commit-under-wal-lock))
           (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (terminal nil)
           (flushed nil)
           (callback nil))
      (ensure-directories-exist *pgdata*)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-append-running-xacts-under-lock)
                   (lambda (xid) (declare (ignore xid)))
                   (symbol-function 'wal-append-batch-start) (lambda () 100)
                   (symbol-function 'wal-append-record)
                   (lambda (type xid payload)
                     (rove:ok (= type +wal-record-commit+))
                     (rove:ok (= xid 44))
                     (setf terminal (incf *wal-end-lsn* (+ +wal-header-size+ (length payload)))))
                   (symbol-function 'wal-flush) (lambda () (setf flushed t *wal-durable-lsn* *wal-end-lsn*))
                   (symbol-function 'wal-slot-record-xid-commit-under-wal-lock)
                   (lambda (xid lsn)
                     (rove:ok (= xid 44))
                     (rove:ok (= lsn terminal))))
             (let ((end (wal-commit-transaction
                         44 (lambda (end)
                              (rove:ok (= end terminal))
                              (rove:ok flushed)
                              (setf callback t)
                              (incf *wal-end-lsn* 1000))
                         :source-id :fixture-source)))
               (rove:ok callback)
               (rove:ok (eql end terminal)
                        "return the transaction terminal, not subsequent global WAL progress"))
             (let ((*wal-ready* nil))
               (rove:ok (null (wal-commit-transaction 45 (lambda (end) (rove:ok (null end)))))
                        "a no-WAL commit has no replication boundary")))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
        (dispose-page-publication-test-owner *wal-pending-page-publication*)
        (when *clog-stream* (close *clog-stream*))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(defun commit-receipt-test-result (&key (source :source-a) (supply-source t) fault)
  (let* ((directory (merge-pathnames
                     (format nil "pg-commit-receipt-~d-~d/" (sb-posix:getpid) (random 1000000))
                     (uiop:temporary-directory)))
         (*query-temp-directory* directory)
         (*pgdata* (merge-pathnames "data/" directory))
         (*clog-stream* nil) (*clog-pending-commits* (make-hash-table :test #'eql))
         (*wal-pending-page-publication-tail* nil)
         (*physical-recovery-failed-p* nil)
         (*backend* nil) (*wal-ready* t) (*wal-poisoned* nil) (*wal-output-stream* nil)
         (*wal-dirty-store* nil) (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal)) (*wal-end-lsn* 100) (*wal-durable-lsn* 100) (*wal-pending-page-publication* nil)
         (names '(wal-append-running-xacts-under-lock wal-append-batch-start wal-append-record
                  wal-flush wal-slot-record-xid-commit-under-wal-lock wal-test-pause
                  wal-primary-apply-page-spool close-logical-spool wal-fail-stop))
         (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
         (close (symbol-function 'close-logical-spool))
         (events nil) (accepted nil) (end nil) (receipt nil) (failure nil))
    (ensure-directories-exist *pgdata*)
    (unwind-protect
         (progn
           (setf (symbol-function 'wal-append-running-xacts-under-lock)
                 (lambda (xid) (declare (ignore xid)))
                 (symbol-function 'wal-append-batch-start) (lambda () 100)
                 (symbol-function 'wal-append-record)
                 (lambda (type xid payload)
                   (declare (ignore payload))
                   (unless (and (= type +wal-record-commit+) (= xid 44)) (error "unexpected record"))
                   (push :terminal events) (setf *wal-end-lsn* 140))
                 (symbol-function 'wal-flush)
                 (lambda ()
                   (when (eq fault :flush) (error "receipt flush failure"))
                   (setf *wal-durable-lsn* *wal-end-lsn*)
                   (push :flush events))
                 (symbol-function 'wal-slot-record-xid-commit-under-wal-lock)
                 (lambda (&rest arguments) (declare (ignore arguments)))
                 (symbol-function 'wal-test-pause)
                 (lambda (point)
                   (when (and (eq point :after-page-images) (eq fault :before-transfer))
                     (throw 'receipt-exit :escaped))
                   (when (and (eq point :after-page-publication-transfer)
                              (eq fault :after-transfer))
                     (throw 'receipt-exit :escaped))
                   (when (eq point :after-wal-flush)
                     (push :hook events)
                     (when (eq fault :hook) (error "receipt hook failure"))))
                 (symbol-function 'wal-primary-apply-page-spool)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (push :apply events)
                   (when (eq fault :apply) (error "receipt apply failure")))
                 (symbol-function 'close-logical-spool)
                 (lambda (spool)
                   (funcall close spool)
                   (push :close events)
                   (when (eq fault :close) (error "receipt close failure")))
                 (symbol-function 'wal-fail-stop)
                 (lambda (condition)
                   (when (sb-thread:holding-mutex-p *wal-lock*) (error "fail-stop still holds WAL"))
                   (setf *wal-poisoned* t)
                   (error condition)))
           (handler-case
               (catch 'receipt-exit
                 (multiple-value-setq (end receipt)
                   (apply #'wal-commit-transaction
                          44 (lambda (end)
                               (unless (= end 140) (error "wrong local terminal"))
                               (push :local events) (incf *wal-end-lsn* 1000))
                          (append
                           (list :terminal-durable-function
                                 (lambda (terminal)
                                   (unless (= terminal 140) (error "wrong durable terminal"))
                                   (push :durable events)
                                   (when (eq fault :durable) (error "receipt observer failure")))
                                 :accepted-function
                                 (lambda (value)
                                   (unless (sb-thread:holding-mutex-p *wal-lock*) (error "acceptance lost WAL exclusion"))
                                   (when sb-sys:*interrupts-enabled* (error "acceptance permits interrupts"))
                                   (setf accepted value)
                                   (push :accepted events)
                                   (when (eq fault :accept) (error "receipt acceptance failure"))
                                   (when (eq fault :nonlocal-accept) (throw 'receipt-exit :escaped))))
                           (when supply-source (list :source-id source))))))
             (error (condition) (setf failure (princ-to-string condition))))
           (list :events (nreverse events) :accepted accepted :end end :receipt receipt
                 :failure failure :poisoned *wal-poisoned* :global-end *wal-end-lsn*
                 :page-state (when *wal-pending-page-publication*
                               (wal-page-publication-state *wal-pending-page-publication*))
                 :page-spool-closed (when *wal-pending-page-publication*
                                      (logical-spool-closed-p
                                       (wal-page-publication-spool *wal-pending-page-publication*)))))
      (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
      (dispose-page-publication-test-owner *wal-pending-page-publication*)
      (when *clog-stream* (close *clog-stream*))
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty commit-receipt-keeps-exact-source-and-terminal
    ((source (range integer 0 1000000)))
  (:about wal-commit-transaction)
  (:trials (:normal 100))
  (let* ((result (commit-receipt-test-result :source source))
         (receipt (getf result :receipt)))
    (and (not (getf result :failure))
         (wal-commit-receipt-p receipt)
         (eq receipt (getf result :accepted))
         (= 44 (wal-commit-receipt-xid receipt))
         (= source (wal-commit-receipt-source-id receipt))
         (= 140 (wal-commit-receipt-terminal-end receipt) (getf result :end))
         (wal-commit-receipt-wal-durable-p receipt)
         (= 1140 (getf result :global-end))
         (equal '(:terminal :flush :durable :hook :local :apply :close :accepted)
                (getf result :events)))))

(rove:deftest wal-commit-acceptance-receipt-contract
    (unit-check-property 'commit-receipt-keeps-exact-source-and-terminal))

(rove:deftest page-publication-faults-retain-owner-until-complete-drain
    (dolist (fault '(:before-transfer :after-transfer :flush :durable :hook :apply :close :accept :nonlocal-accept))
      (let* ((result (commit-receipt-test-result :fault fault))
             (pending-p (member fault '(:after-transfer :flush :durable :hook :apply :close))))
        (rove:ok (getf result :failure) (format nil "~a is fenced" fault))
        (rove:ok (getf result :poisoned))
        (rove:ok (null (getf result :receipt)))
        (rove:ok (null (getf result :end)))
        (rove:ok (eq (cond ((eq fault :close) :applied) (pending-p :pending))
                     (getf result :page-state))
                 (format nil "~a preserves the publication lifecycle" fault))
        (when pending-p
          (rove:ok (eql (eq fault :close) (getf result :page-spool-closed))
                   "lexical unwinding cannot close a transferred spool"))
        (unless (member fault '(:accept :nonlocal-accept))
          (rove:ok (null (getf result :accepted))
                   "incomplete publication cannot reach acceptance")))))

(rove:deftest wal-commit-receipt-rejects-source-before-publication
    (dolist (arguments '((:supply-source nil) (:source "mutable") (:source #(1))))
      (let ((result (apply #'commit-receipt-test-result arguments)))
        (rove:ok (and (getf result :failure) (search "source" (getf result :failure))))
        (rove:ok (null (getf result :events)))
        (rove:ok (null (getf result :accepted)))
        (rove:ok (null (getf result :poisoned)))
        (rove:ok (= 100 (getf result :global-end))))))

(rove:deftest wal-commit-receipt-failures-preserve-durable-callback-order
    (dolist (fault '(:hook :apply :close :accept :nonlocal-accept))
      (let ((result (commit-receipt-test-result :fault fault)))
        (rove:ok (getf result :failure))
        (rove:ok (getf result :poisoned))
        (rove:ok (member :durable (getf result :events)))
        (rove:ok (null (getf result :receipt)))
        (rove:ok (null (getf result :end)))
        (unless (member fault '(:accept :nonlocal-accept))
          (rove:ok (null (getf result :accepted)))))))

(rove:deftest wal-commit-receipt-allows-an-explicit-nil-source
    (let* ((result (commit-receipt-test-result :source nil))
           (receipt (getf result :receipt)))
      (rove:ok (null (getf result :failure)))
      (rove:ok (wal-commit-receipt-p receipt))
      (rove:ok (null (wal-commit-receipt-source-id receipt)))
      (rove:ok (wal-commit-receipt-wal-durable-p receipt))))

(rove:deftest wal-commit-receipt-without-wal-has-no-wal-proof
    (let ((*wal-ready* nil) (*backend* nil) (events nil) (accepted nil))
      (multiple-value-bind (end receipt)
          (wal-commit-transaction
           44 (lambda (end) (rove:ok (null end)) (push :local events))
           :terminal-durable-function (lambda (end) (declare (ignore end)) (error "no-WAL completion reported WAL durability"))
           :source-id nil
           :accepted-function (lambda (value)
                                (when sb-sys:*interrupts-enabled* (error "unmasked acceptance"))
                                (setf accepted value)
                                (push :accepted events)))
        (rove:ok (null end))
        (rove:ok (eq receipt accepted))
        (rove:ok (= 44 (wal-commit-receipt-xid receipt)))
        (rove:ok (null (wal-commit-receipt-terminal-end receipt)))
        (rove:ok (null (wal-commit-receipt-wal-durable-p receipt)))
        (rove:ok (equal '(:local :accepted) (nreverse events))))))

(defun call-with-catalog-boundary-test (function &key chunked fault (wal-ready t))
  (let* ((directory (merge-pathnames
                     (format nil "pg-catalog-boundary-~d-~d/" (sb-posix:getpid) (random 1000000))
                     (uiop:temporary-directory)))
         (*query-temp-directory* directory)
         (*backend* nil)
         (*wal-ready* wal-ready)
         (*wal-poisoned* nil)
         (*wal-output-stream* nil)
         (*wal-dirty-store* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (*wal-pending-deletes* nil)
         (*wal-vacuum-full-publication* nil)
         (*wal-catalog-flush-generation* 0)
         (*wal-end-lsn* 100)
         (names '(wal-append-batch-start wal-append-record wal-flush
                  wal-primary-apply-page-spool wal-test-pause wal-fail-stop))
         (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
         (terminal nil) (durable nil) (flushed nil) (events nil)
         (returned nil) (failure nil) (result nil))
    (ensure-directories-exist directory)
    (unwind-protect
         (progn
           (setf (symbol-function 'wal-append-batch-start) (lambda () 100)
                 (symbol-function 'wal-append-record)
                 (lambda (type xid payload)
                   (declare (ignore xid))
                   (incf *wal-end-lsn* (+ +wal-header-size+ (length payload)))
                   (when (member type (list +wal-record-catalog+ +wal-record-catalog-commit+))
                     (setf terminal *wal-end-lsn*))
                   *wal-end-lsn*)
                 (symbol-function 'wal-flush) (lambda () (setf flushed t))
                 (symbol-function 'wal-primary-apply-page-spool)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (push :apply events)
                   (when (eq fault :apply) (throw 'catalog-exit :escaped)))
                 (symbol-function 'wal-test-pause)
                 (lambda (point)
                   (when (eq point :after-catalog-wal-flush)
                     (push :after-flush events)
                     (when (eq fault :hook) (throw 'catalog-exit :escaped))))
                 (symbol-function 'wal-fail-stop)
                 (lambda (condition)
                   (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                   (setf *wal-poisoned* t)
                   (error condition)))
           (setf result
                 (handler-case
                     (catch 'catalog-exit
                       (setf returned
                             (wal-commit-catalog
                              (make-array (if chunked (1+ +wal-segment-size+) 3)
                                          :element-type '(unsigned-byte 8) :initial-element 0)
                              (lambda (bytes)
                                (declare (ignore bytes))
                                (push :write events)
                                (when (eq fault :write) (error "catalog write failed"))
                                (incf *wal-end-lsn* 1000))
                              :on-record-appended
                              (lambda ()
                                (rove:ok (not flushed))
                                (push :appended events))
                              :on-durable
                              (lambda (end)
                                (rove:ok (or (not wal-ready) flushed))
                                (rove:ok (= 1 *wal-catalog-flush-generation*))
                                (setf durable (list end))
                                (push :durable events)
                                (when (eq fault :durable) (throw 'catalog-exit :escaped)))))
                       :returned)
                   (error (condition) (setf failure condition) :failed)))
           (funcall function
                    (list :result result :failure failure :returned returned
                          :terminal terminal :durable durable :events (nreverse events)
                          :poisoned *wal-poisoned* :generation *wal-catalog-flush-generation*)))
      (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest catalog-commit-retains-exact-terminal-and-durable-callback-order
    (dolist (chunked '(nil t))
      (call-with-catalog-boundary-test
       (lambda (facts)
         (rove:ok (eq :returned (getf facts :result)))
         (rove:ok (typep (getf facts :terminal) '(integer 1 18446744073709551615)))
         (rove:ok (eql (getf facts :returned) (getf facts :terminal)))
         (rove:ok (equal (list (getf facts :terminal)) (getf facts :durable)))
         (rove:ok (equal '(:appended :durable :after-flush :apply :write) (getf facts :events)))
         (rove:ok (not (getf facts :poisoned))))
       :chunked chunked)))

(rove:deftest catalog-publication-nonlocal-exits-fail-stop-after-durability
    (dolist (fault '(:durable :hook :apply))
      (call-with-catalog-boundary-test
       (lambda (facts)
         (rove:ok (eq :failed (getf facts :result)))
         (rove:ok (getf facts :poisoned))
         (rove:ok (= 1 (getf facts :generation)))
         (rove:ok (equal (list (getf facts :terminal)) (getf facts :durable)))
         (rove:ok (not (member :write (getf facts :events)))))
       :fault fault)))

(rove:deftest catalog-no-wal-durable-callback-follows-successful-direct-write
    (call-with-catalog-boundary-test
     (lambda (facts)
       (rove:ok (eq :returned (getf facts :result)))
       (rove:ok (null (getf facts :returned)))
       (rove:ok (equal '(nil) (getf facts :durable)))
       (rove:ok (equal '(:write :durable) (getf facts :events))))
     :wal-ready nil)
  (call-with-catalog-boundary-test
   (lambda (facts)
     (rove:ok (eq :failed (getf facts :result)))
     (rove:ok (null (getf facts :durable)))
     (rove:ok (zerop (getf facts :generation)))
     (rove:ok (not (getf facts :poisoned))))
   :wal-ready nil :fault :write))

(rove:deftest wal-publication-fence-retains-body-error-through-cleanup-failure
    (let* ((*wal-poisoned* nil)
           (original (make-condition 'simple-error :format-control "publication failed"))
           (secondary (make-condition 'simple-error :format-control "spool cleanup failed"))
           (fail-stop (symbol-function 'wal-fail-stop))
           (seen nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop)
                   (lambda (condition)
                     (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                     (push condition seen)
                     (setf *wal-poisoned* t)
                     (error condition)))
             (let ((result
                     (handler-case
                         (with-wal-publication-fence (begin-publication)
                           (begin-publication)
                           (unwind-protect (error original) (error secondary)))
                       (error (condition) condition))))
               (rove:ok (eq original result))
               (rove:ok (eq original (first seen))))
             (rove:ok *wal-poisoned*)
             (let ((*wal-poisoned* nil))
               (setf seen nil)
               (rove:ok
                (handler-case
                    (with-wal-publication-fence (begin-publication)
                      (wal-invalid-input "invalid before publication")
                      (begin-publication)
                      nil)
                  (wal-input-validation-error () t)))
               (rove:ok (not *wal-poisoned*))
               (rove:ok (null seen))))
        (setf (symbol-function 'wal-fail-stop) fail-stop))))

(rove:deftest wal-publication-nonlocal-exit-poisons-before-unlock
    (let ((*wal-poisoned* nil)
          (stop (symbol-function 'wal-fail-stop))
          (poisoned-before-fence nil) (fenced-outside-lock nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop)
                   (lambda (condition)
                     (declare (ignore condition))
                     (setf poisoned-before-fence *wal-poisoned*
                           fenced-outside-lock (not (sb-thread:holding-mutex-p *wal-lock*)))))
             (rove:ok (eq :escaped
                          (catch 'publication-exit
                            (with-wal-publication-fence (begin-publication)
                              (begin-publication)
                              (throw 'publication-exit :escaped)))))
             (rove:ok poisoned-before-fence)
             (rove:ok fenced-outside-lock))
        (setf (symbol-function 'wal-fail-stop) stop))))

(defun wal-lock-cancellation-probe (masked-p)
  (let ((lock (sb-thread:make-mutex :name "owned WAL lock wait"))
        (finished (sb-thread:make-semaphore))
        (thread nil) (entered nil) (result nil) (early nil)
        (failure nil) (poisoned nil))
    (sb-thread:grab-mutex lock)
    (unwind-protect
         (progn
           (setf thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*wal-lock* lock) (*wal-poisoned* nil))
                      (flet ((publish ()
                               (with-wal-publication-fence (begin-publication)
                                 (begin-publication)
                                 (setf entered t))))
                        (handler-case
                            (setf result
                                  (catch 'wal-lock-cancel
                                    (if masked-p
                                        (sb-sys:without-interrupts (publish))
                                        (publish))
                                    :published))
                          (error (condition) (setf failure condition)))
                        (setf poisoned *wal-poisoned*)
                        (sb-thread:signal-semaphore finished))))
                  :name "WAL lock cancellation probe"))
           (loop repeat 200
                 when (eq lock (sb-thread::thread-waiting-for thread)) do (return)
                   do (sleep 0.005)
                 finally (error "owned WAL actor did not wait for its mutex"))
           (sb-thread:interrupt-thread thread (lambda () (throw 'wal-lock-cancel :cancelled)))
           (setf early (not (null (sb-thread:wait-on-semaphore finished :timeout 0.2))))
           (rove:ok (eq sb-thread:*current-thread* (sb-thread:mutex-owner lock))))
      (when (eq sb-thread:*current-thread* (sb-thread:mutex-owner lock))
        (sb-thread:release-mutex lock))
      (when thread (sb-thread:join-thread thread :timeout 3 :default :timeout)))
    (rove:ok (not (sb-thread:thread-alive-p thread)))
    (rove:ok (null failure) (format nil "~a" failure))
    (rove:ok (not poisoned))
    (rove:ok (eq :cancelled result))
    (rove:ok (eq early (not masked-p)) "inherit caller permission while waiting for WAL")
    (rove:ok (eq entered masked-p) "an interruptible wait exits before publication")))

(rove:deftest wal-publication-fence-preserves-lock-wait-interruptibility
    (wal-lock-cancellation-probe nil)
  (wal-lock-cancellation-probe t))

(rove:deftest wal-critical-defers-interrupts-through-unlock-and-preserves-values
    (let* ((*wal-lock* (sb-thread:make-mutex :name "owned WAL mask handoff"))
           (*wal-poisoned* nil)
           (lock *wal-lock*)
           (delivered nil)
           (outside nil)
           (masks nil))
      (rove:ok
       (equal '(:first :second)
              (multiple-value-list
               (with-wal-critical
                 (setf masks (list sb-sys:*interrupts-enabled* sb-sys:*allow-with-interrupts*))
                 (sb-thread:interrupt-thread
                  sb-thread:*current-thread*
                  (lambda ()
                    (setf delivered t
                          outside (not (sb-thread:holding-mutex-p lock)))))
                 (values :first :second)))))
      (rove:ok (equal '(nil nil) masks))
      (rove:ok delivered)
      (rove:ok outside)
      (rove:ok (not *wal-poisoned*))))

(rove:deftest test-wal-primary-spool-corruption-fails-stop-before-and-after-commit
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-corrupt-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*pgdata* (merge-pathnames "data/" directory))
           (*clog-stream* nil) (*clog-pending-commits* (make-hash-table :test #'eql))
           (*wal-pending-page-publication-tail* nil)
           (*physical-recovery-failed-p* nil)
           (*logical-capture-memory-bytes* 0)
           (*wal-ready* t)
           (*wal-end-lsn* 100)
           (*wal-durable-lsn* 100)
           (*wal-pending-page-publication* nil)
           (*wal-output-stream* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-replication-slots* (make-hash-table :test #'equal))
           (*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (*active-xids* nil)
           (names '(wal-append-running-xacts-under-lock wal-append-batch-start
                    wal-next-record-start wal-append-record wal-flush
                    wal-slot-record-xid-commit-under-wal-lock logical-spool-map-events
                    smgr-write wal-fail-stop))
           (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (old-map (symbol-function 'logical-spool-map-events)))
      (ensure-directories-exist *pgdata*)
      (setf (gethash (wal-page-key 7310 0) *wal-page-cache*) (page-init (make-page)))
      (unwind-protect
           (dolist (corrupt-pass '(1 2))
             (let ((*wal-poisoned* nil) (position 100) (calls 0) (records nil)
                                        (flushed nil) (callback nil) (writes 0))
               (setf (symbol-function 'wal-append-running-xacts-under-lock)
                     (lambda (xid) (declare (ignore xid)))
                     (symbol-function 'wal-append-batch-start) (lambda () 100)
                     (symbol-function 'wal-next-record-start)
                     (lambda (length) (declare (ignore length)) position)
                     (symbol-function 'wal-append-record)
                     (lambda (type xid payload)
                       (declare (ignore xid))
                       (push type records)
                       (setf *wal-end-lsn*
                             (incf position (+ +wal-header-size+ (length payload)))))
                     (symbol-function 'wal-flush)
                     (lambda () (setf flushed t *wal-durable-lsn* *wal-end-lsn*))
                     (symbol-function 'wal-slot-record-xid-commit-under-wal-lock)
                     (lambda (xid lsn) (declare (ignore xid lsn)))
                     (symbol-function 'smgr-write)
                     (lambda (node block page &optional fork)
                       (declare (ignore node block page fork)) (incf writes))
                     (symbol-function 'wal-fail-stop) (lambda (condition) (error condition))
                     (symbol-function 'logical-spool-map-events)
                     (lambda (spool function)
                       (incf calls)
                       (when (= calls corrupt-pass)
                         (let ((file (logical-spool-stream spool)))
                           (file-position file (+ 8 12 20))
                           (write-byte 99 file)
                           (finish-output file)))
                       (funcall old-map spool function)))
               (rove:ok (handler-case
                            (progn (wal-commit-transaction 44 (lambda (end) (declare (ignore end)) (setf callback t)) :source-id :fixture-source) nil)
                          (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
               (rove:ok *wal-poisoned*)
               (rove:ok (zerop writes))
               (rove:ok (= (if (= corrupt-pass 1) 1 0)
                           (hash-table-count *wal-page-cache*)))
               (rove:ok (if (= corrupt-pass 2)
                            (and *wal-pending-page-publication*
                                 (= 1 (hash-table-count
                                       (wal-page-publication-pages *wal-pending-page-publication*)))
                                 (not (logical-spool-closed-p
                                       (wal-page-publication-spool *wal-pending-page-publication*))))
                            (null *wal-pending-page-publication*)))
               (rove:ok (eql (= corrupt-pass 2) flushed))
               (rove:ok (eql (= corrupt-pass 2) callback))
               (rove:ok (eql (= corrupt-pass 2)
                             (not (null (member +wal-record-commit+ records)))))
               (rove:ok (eql (= corrupt-pass 1)
                             (null (uiop:directory-files directory))))))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
        (dispose-page-publication-test-owner *wal-pending-page-publication*)
        (when *clog-stream* (close *clog-stream*))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-preflight-and-order
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-spool-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-temp-file-limit-bytes* 0)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (original (symbol-function 'wal-apply-page-images))
           (writes nil)
           (terminal nil)
           (first (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (second (make-wal-page-image :relfilenode 7310 :block 3
                                        :page (let ((page (page-init (make-page))))
                                                (setf (aref page 100) 42) page)))
           (one (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                 :payload (wal-encode-page-images (list first) 100)))
           (two (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 200
                                 :payload (wal-encode-page-images (list second) 200)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore split-p fault-on-first-write))
                     (dolist (image images)
                       (push (list (wal-page-image-block image)
                                   (aref (wal-page-image-page image) 100)) writes))))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record one)
               (wal-replay-record two)
               (rove:ok (null *wal-recovery-page-images*))
               (rove:ok (= 1 (length (uiop:directory-files directory))))
               (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
               (rove:ok (equal (nreverse writes) '((3 0) (3 42))))
               (rove:ok terminal)
               (rove:ok (null (uiop:directory-files directory))))
             (setf writes nil terminal nil)
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record one)
               (wal-replay-record two)
               (let* ((spool *wal-recovery-page-spool*)
                      (file (logical-spool-stream spool))
                      (offset (+ 8 12 (length (wal-record-payload one)) 8 12 20)))
                 (finish-output file)
                 (file-position file offset)
                 (write-byte 99 file)
                 (finish-output file))
               (rove:ok
                (handler-case
                    (progn
                      (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
                      nil)
                  (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
               (rove:ok (null writes))
               (rove:ok (null terminal)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-apply-page-images) original)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-failure-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-fault-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (original (symbol-function 'wal-apply-page-images))
           (writes nil)
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (payload (wal-encode-page-images (list image) 100))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload payload))
           (bad (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 200
                                 :payload payload))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))
                     (setf writes t)))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (rove:ok (handler-case
                            (progn (wal-replay-finish-batch (lambda () (error "invalid terminal"))) nil)
                          (error (condition) (search "invalid terminal" (princ-to-string condition)))))
               (rove:ok (null writes)))
             (rove:ok (null (uiop:directory-files directory)))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (rove:ok (handler-case (progn (wal-replay-record bad) nil) (error () t)))
               (rove:ok (null writes)))
             (rove:ok (null (uiop:directory-files directory)))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (wal-replay-record start)
               (rove:ok (null (uiop:directory-files directory)))
               (wal-replay-finish-batch (lambda () #'values))
               (rove:ok (null writes)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-apply-page-images) original)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-replay-fault-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-replay-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (old-map (symbol-function 'logical-spool-map-events))
           (old-apply (symbol-function 'wal-apply-page-images))
           (calls 0) (writes nil) (terminal nil)
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-spool-map-events)
                   (lambda (spool function)
                     (incf calls)
                     (when (= calls 2) (error "injected replay failure"))
                     (funcall old-map spool function))
                   (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))
                     (setf writes t)))
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record page)
                    (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
                    nil)
                (error (condition) (search "injected replay failure" (princ-to-string condition)))))
             (rove:ok (= calls 2))
             (rove:ok (null writes))
             (rove:ok (null terminal))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'logical-spool-map-events) old-map
              (symbol-function 'wal-apply-page-images) old-apply)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-cleanup-failure
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-cleanup-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-temp-pending-cleanups* nil)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (unlink (symbol-function 'unlink-query-temp-file))
           (apply-pages (symbol-function 'wal-apply-page-images))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))))
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record page)
                    (logical-spool-seal *wal-recovery-page-spool*)
                    (call-with-logical-spool-owner
                     *wal-recovery-page-spool*
                     (lambda ()
                       (finish-query-temp-stream
                        (logical-spool-stream *wal-recovery-page-spool*) t)))
                    (setf (symbol-function 'unlink-query-temp-file)
                          (lambda (path) (declare (ignore path)) (error "injected unlink failure")))
                    (wal-recovery-close-page-spool)
                    nil)
                (error (condition) (search "recovery page spool cleanup failed"
                                           (princ-to-string condition)))))
             (rove:ok (= 1 (getf (query-temp-pending-cleanup-status) :records)))
             (rove:ok (= 1 (length (uiop:directory-files directory))))
             (setf (symbol-function 'unlink-query-temp-file) unlink)
             (retry-pending-query-temp-cleanup)
             (rove:ok (zerop (getf (query-temp-pending-cleanup-status) :records)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'unlink-query-temp-file) unlink
              (symbol-function 'wal-apply-page-images) apply-pages)
        (retry-pending-query-temp-cleanup)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-private-namespace-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-namespace-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* nil)
           (*query-temp-instance-directory* nil)
           (*query-temp-root-directory* nil)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (apply-pages (symbol-function 'wal-apply-page-images))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (wal-replay-finish-batch (lambda () #'values))
               (rove:ok (null *wal-recovery-page-spool*)))
             (rove:ok (zerop (getf (query-temp-pending-cleanup-status) :records))))
        (setf (symbol-function 'wal-apply-page-images) apply-pages)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-cleanup-error-still-closes-logical-spool
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-mixed-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (original (symbol-function 'close-logical-spool))
           (page-spool nil)
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10))
           (event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "value"))))
           (part (make-wal-record :type +wal-record-logical-changes+ :xid 88 :end 150
                                  :payload (logical-encode-fragment
                                            0 2 (make-logical-fragment
                                                 :event event :start 0 :end (floor (length event) 2))))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record page)
                    (wal-replay-record part)
                    (setf page-spool *wal-recovery-page-spool*)
                    (rove:ok (= 2 (length (uiop:directory-files directory))))
                    (setf (symbol-function 'close-logical-spool)
                          (lambda (spool)
                            (if (eq spool page-spool)
                                (error "injected page close failure")
                                (funcall original spool))))
                    nil)
                (error (condition) (search "injected page close failure" (princ-to-string condition)))))
             (rove:ok (= 1 (length (uiop:directory-files directory)))))
        (setf (symbol-function 'close-logical-spool) original)
        (when page-spool (close-logical-spool page-spool))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-late-corruption-restarts-cleanly
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-retry-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (old-map (symbol-function 'logical-spool-map-events))
           (old-apply (symbol-function 'wal-apply-page-images))
           (calls 0) (writes nil) (terminal nil)
           (first (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (second (make-wal-page-image :relfilenode 7310 :block 4 :page (page-init (make-page))))
           (one (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                 :payload (wal-encode-page-images (list first) 100)))
           (two (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 200
                                 :payload (wal-encode-page-images (list second) 200)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore split-p fault-on-first-write))
                     (dolist (image images) (push (wal-page-image-block image) writes)))
                   (symbol-function 'logical-spool-map-events)
                   (lambda (spool function)
                     (incf calls)
                     (when (= calls 2)
                       (let ((file (logical-spool-stream spool)))
                         (file-position file (+ 8 12 (length (wal-record-payload one)) 8 12 20))
                         (write-byte 99 file)
                         (finish-output file)))
                     (funcall old-map spool function)))
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record one)
                    (wal-replay-record two)
                    (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
                    nil)
                (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
             (rove:ok (equal writes '(3)))
             (rove:ok (null terminal))
             (rove:ok (null (uiop:directory-files directory)))
             (setf (symbol-function 'logical-spool-map-events) old-map
                   writes nil)
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record one)
               (wal-replay-record two)
               (wal-replay-finish-batch (lambda () (lambda () (setf terminal t)))))
             (rove:ok (equal (nreverse writes) '(3 4)))
             (rove:ok terminal)
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'logical-spool-map-events) old-map
              (symbol-function 'wal-apply-page-images) old-apply)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-with-wal-critical-validation-errors-do-not-poison
    (let ((*wal-poisoned* nil))
      (rove:ok (handler-case
                   (progn
                     (with-wal-critical
                       (wal-invalid-input "invalid test input"))
                     nil)
                 (wal-input-validation-error () t))
               "WAL input validation errors propagate through the critical macro")
      (rove:ok (not *wal-poisoned*)
               "WAL input validation errors do not poison the critical section")
      (rove:ok (eq :healthy
                   (with-wal-critical :healthy))
               "the critical macro releases its lock after an input validation error")))

(rove:deftest wal-critical-fences-validation-after-poisoning
    (let* ((*wal-poisoned* nil)
           (original (handler-case (wal-invalid-input "publication validation after I/O")
                       (wal-input-validation-error (condition) condition)))
           (stop (symbol-function 'wal-fail-stop))
           (fenced nil) (outside nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop)
                   (lambda (condition)
                     (setf fenced condition
                           outside (not (sb-thread:holding-mutex-p *wal-lock*)))
                     (error condition)))
             (rove:ok (eq original
                          (handler-case
                              (with-wal-critical
                                (setf *wal-poisoned* t)
                                (error original))
                            (wal-input-validation-error (condition) condition))))
             (rove:ok (eq original fenced))
             (rove:ok outside)
             (rove:ok *wal-poisoned*))
        (setf (symbol-function 'wal-fail-stop) stop))))

(rove:deftest test-wal-drop-missing-slot-does-not-poison-wal
    (let ((*wal-ready* t)
          (*wal-poisoned* nil)
          (*wal-replication-slots* (make-hash-table :test #'equal)))
      (rove:ok (handler-case
                   (progn (wal-drop-replication-slot "missing_slot") nil)
                 (error () t))
               "dropping a missing slot reports an error")
      (rove:ok (not *wal-poisoned*)
               "a missing slot does not poison WAL or request fail-stop shutdown")))
