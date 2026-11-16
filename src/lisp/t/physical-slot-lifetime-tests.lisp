(in-package #:pg-physical-tests)

(declaim (special pg::*physical-slot-lifetime-seals*))

(declaim (special pg::*physical-slot-lifetimes*))

(defun call-with-physical-slot-lifetime-fixture (function &key (admit t))
  (call-with-physical-slot-api-fixture
   (lambda (root)
     (let ((pg::*backend-registry* (make-hash-table :test #'eql :synchronized t))
           (pg::*backend-lifetimes* (make-hash-table :test #'eq :synchronized t))
           (pg::*backend-lifetime-seals* (make-hash-table :test #'eq :synchronized t))
           (pg::*backend-lifetime-revocations* (make-hash-table :test #'eq :synchronized t))
           (pg::*backend-admission-classes* (make-hash-table :test #'eq))
           (pg::*physical-slot-lifetime-seals* (make-hash-table :test #'eq :synchronized t))
           (pg::*physical-slot-lifetimes* (make-hash-table :test #'eq :synchronized t))
           (pg::*n-backends* 0)
           (pg::*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
           (pg::*live-operation-cleanup-backends* nil)
           (pg::*live-operation-admission-open* t)
           (pg::*wal-storage-incarnation* (gensym "TEMPORARY-SLOT-STORE-"))
           (pg::*wal-ready* nil) (pg::*wal-logical-info* nil)
           (pg::*backend* (pg::make-backend
                           :pid (sb-thread:thread-os-tid sb-thread:*current-thread*)
                           :thread sb-thread:*current-thread*)))
       (pg::register-backend pg::*backend*)
       (unwind-protect
            (if admit
                (pg::with-live-operation (funcall function root))
                (funcall function root))
         (pg::unregister-backend pg::*backend*))))))

(defun physical-slot-test-file-bytes (path)
  (with-open-file (stream path :element-type '(unsigned-byte 8))
    (let ((bytes (make-array (file-length stream) :element-type '(unsigned-byte 8))))
      (assert (= (length bytes) (read-sequence bytes stream)))
      bytes)))

(defun physical-slot-test-lifetime (slot)
  (uiop:symbol-call :pg :wal-replication-slot-temporary-lifetime slot))

(defun physical-slot-test-live (slot)
  (sb-thread:with-mutex (pg::*wal-lock*)
    (uiop:symbol-call :pg :physical-slot-lifetime-check-under-lock slot)))

(defun physical-slot-test-lifetime-refused-p (slot)
  (assert (fboundp 'pg::physical-slot-lifetime-check-under-lock))
  (handler-case (progn (physical-slot-test-live slot) nil)
    (error () t)))

(cl-spec:defproperty temporary-physical-lifetimes-end-with-the-slot-generation
    ((count (range integer 1 4)))
  (:about pg::physical-slot-lifetime-check-under-lock)
  (:trials (:normal 80))
  (call-with-physical-slot-lifetime-fixture
   (lambda (root)
     (declare (ignore root))
     (let ((old nil))
       (loop repeat count
             always
             (let* ((slot (pg::wal-create-physical-replication-slot "lifetime" :temporary t))
                    (token (physical-slot-test-lifetime slot)))
               (and token (not (eq token old))
                    (eq slot (physical-slot-test-live slot))
                    (progn
                      (pg::wal-drop-replication-slot "lifetime" slot)
                      (setf old token)
                      (and (null (gethash "lifetime" pg::*wal-replication-slots*))
                           (physical-slot-test-lifetime-refused-p slot))))))))))

(cl-spec:defproperty copied-physical-slots-and-disk-images-carry-no-authority
    ((mode (range integer 0 1)))
  (:about pg::physical-slot-lifetime-check-under-lock)
  (:trials (:normal 80))
  (call-with-physical-slot-lifetime-fixture
   (lambda (root)
     (declare (ignore root))
     (let* ((slot (pg::wal-create-physical-replication-slot "copied_lifetime" :temporary t))
            (copy (if (zerop mode)
                      (pg::copy-wal-replication-slot slot)
                      (pg::physical-slot-decode (pg::physical-slot-encode slot) "copied_lifetime"))))
       (and (physical-slot-test-lifetime slot)
            (physical-slot-test-lifetime-refused-p copy)
            (eq slot (physical-slot-test-live slot)))))))

(cl-spec:defproperty temporary-physical-authority-loss-is-sticky
    ((mode (range integer 0 6)))
  (:about pg::physical-slot-lifetime-check-under-lock)
  (:trials (:normal 80))
  (call-with-physical-slot-lifetime-fixture
   (lambda (root)
     (declare (ignore root))
     (let* ((slot (pg::wal-create-physical-replication-slot "stale_lifetime" :temporary t))
            (owner pg::*backend*)
            (initial (eq slot (physical-slot-test-live slot)))
            (before (physical-slot-test-file-bytes (pg::wal-replication-slot-path "stale_lifetime")))
            (refused
              (ecase mode
                (0 (let ((pg::*wal-replication-slots*
                           (alexandria:copy-hash-table pg::*wal-replication-slots*)))
                     (physical-slot-test-lifetime-refused-p slot)))
                (1 (let ((pg::*wal-storage-incarnation* (gensym "OTHER-STORE-")))
                     (physical-slot-test-lifetime-refused-p slot)))
                (2 (setf (pg::wal-replication-slot-temporary-owner slot) (pg::make-backend :pid 0))
                 (unwind-protect (physical-slot-test-lifetime-refused-p slot)
                   (setf (pg::wal-replication-slot-temporary-owner slot) owner)))
                (3 (pg::unregister-backend owner)
                 (setf (pg::backend-thread owner) sb-thread:*current-thread*)
                 (pg::register-backend owner)
                 (physical-slot-test-lifetime-refused-p slot))
                (4 (with-physical-store-directory (other)
                     (let ((pg::*pgdata* other))
                       (physical-slot-test-lifetime-refused-p slot))))
                (5 (let ((token (gethash slot pg::*physical-slot-lifetimes*)))
                     (remhash slot pg::*physical-slot-lifetimes*)
                     (unwind-protect (physical-slot-test-lifetime-refused-p slot)
                       (setf (gethash slot pg::*physical-slot-lifetimes*) token))))
                (6 (let* ((token (physical-slot-test-lifetime slot))
                          (seal (gethash token pg::*physical-slot-lifetime-seals*)))
                     (remhash token pg::*physical-slot-lifetime-seals*)
                     (unwind-protect (physical-slot-test-lifetime-refused-p slot)
                       (setf (gethash token pg::*physical-slot-lifetime-seals*) seal)))))))
       (and initial refused (physical-slot-test-lifetime-refused-p slot)
            (eq slot (gethash "stale_lifetime" pg::*wal-replication-slots*))
            (equalp before (physical-slot-test-file-bytes
                            (pg::wal-replication-slot-path "stale_lifetime"))))))))

(deftest temporary-physical-lifetime-rejects-copied-and-fabricated-validators
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((slot (pg::wal-create-physical-replication-slot "forged_lifetime" :temporary t))
              (called nil)
              (fabricated
                (uiop:symbol-call
                 :pg :%make-physical-slot-lifetime
                 :state (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (setf called t)
                          slot))))
         (dolist (token (list (copy-structure (physical-slot-test-lifetime slot)) fabricated))
           (let ((copy (uiop:symbol-call
                        :pg :make-wal-replication-slot
                        :name "forged_lifetime" :kind :physical
                        :system-id (pg::wal-replication-slot-system-id slot)
                        :consistent-lsn nil :plugin nil
                        :temporary-p t :temporary-owner pg::*backend*
                        :temporary-lifetime token)))
             (ok (physical-slot-test-lifetime-refused-p copy))
             (ok (not called))
             (ok (eq slot (physical-slot-test-live slot)))))))))

(defun physical-slot-test-runtime-ordinary-registry ()
  (sb-thread:with-mutex (pg::*wal-lock*)
    (uiop:symbol-call
     :pg :logical-slot-continuation-check-runtime-ordinary-registry
     pg::*wal-replication-slots* nil (pg::catalog-system-identifier)
     (pg::logical-slot-continuation-history
      1 (make-array 0 :element-type '(unsigned-byte 8)))
     pg::*wal-durable-lsn*)))

(deftest temporary-physical-runtime-registry-requires-live-issued-authority
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((slot (pg::wal-create-physical-replication-slot "runtime_lifetime" :temporary t))
             (owner pg::*backend*))
         (ok (handler-case (physical-slot-test-runtime-ordinary-registry) (error () nil)))
         (ok
          (sb-thread:with-mutex (pg::*wal-lock*)
            (handler-case
                (progn
                  (pg::logical-slot-continuation-check-ordinary-registry
                   pg::*wal-replication-slots* nil (pg::catalog-system-identifier)
                   (pg::logical-slot-continuation-history
                    1 (make-array 0 :element-type '(unsigned-byte 8)))
                   pg::*wal-durable-lsn*)
                  nil)
              (error () t))))
         (let ((pg::*backend* (pg::make-backend :pid 1234)))
           (let ((legacy (pg::wal-create-physical-replication-slot "legacy_lifetime" :temporary t)))
             (ok (null (physical-slot-test-lifetime legacy)))
             (ok (handler-case
                     (progn (physical-slot-test-runtime-ordinary-registry) nil)
                   (error () t)))
             (pg::wal-drop-replication-slot "legacy_lifetime" legacy)))
         (ok (handler-case (physical-slot-test-runtime-ordinary-registry) (error () nil)))
         (ok (eq owner (pg::wal-replication-slot-temporary-owner slot)))
         (let ((copy (pg::copy-wal-replication-slot slot)))
           (setf (gethash "runtime_lifetime" pg::*wal-replication-slots*) copy)
           (unwind-protect
                (ok (handler-case
                        (progn (physical-slot-test-runtime-ordinary-registry) nil)
                      (error () t)))
             (setf (gethash "runtime_lifetime" pg::*wal-replication-slots*) slot)))
         (ok (pg::wal-replication-slot-state-uncertain-p slot))
         (ok (physical-slot-test-lifetime-refused-p slot))))))

(defun physical-slot-test-runtime-ordinary-candidate (candidate)
  (sb-thread:with-mutex (pg::*wal-lock*)
    (uiop:symbol-call
     :pg :logical-slot-continuation-check-runtime-ordinary-candidate
     pg::*wal-replication-slots* nil candidate (pg::catalog-system-identifier)
     (pg::logical-slot-continuation-history
      1 (make-array 0 :element-type '(unsigned-byte 8)))
     pg::*wal-durable-lsn*)))

(deftest temporary-physical-runtime-candidates-resolve-the-live-generation
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((pg::*wal-checkpoint-snapshot* '(:redo 17 :timeline 1))
              (slot (pg::wal-create-physical-replication-slot
                     "candidate_lifetime" :temporary t :immediately-reserve t))
              (token (physical-slot-test-lifetime slot))
              (candidate (pg::copy-wal-replication-slot slot)))
         (setf (pg::wal-replication-slot-restart-lsn candidate) 33)
         (ok (handler-case
                 (eq candidate (physical-slot-test-runtime-ordinary-candidate candidate))
               (error () nil)))
         (ok (physical-slot-test-lifetime-refused-p candidate))
         (ok (= 33 (pg::wal-advance-physical-replication-slot "candidate_lifetime" 33)))
         (ok (eq token (physical-slot-test-lifetime slot)))
         (ok (eq slot (physical-slot-test-live slot)))
         (dolist (mode '(:persistent :foreign-system :foreign-owner))
           (let ((bad (pg::copy-wal-replication-slot slot)))
             (ecase mode
               (:persistent (setf (pg::wal-replication-slot-temporary-p bad) nil))
               (:foreign-system (incf (pg::wal-replication-slot-system-id bad)))
               (:foreign-owner (setf (pg::wal-replication-slot-temporary-owner bad)
                                     (pg::make-backend :pid 0))))
             (ok (handler-case
                     (progn (physical-slot-test-runtime-ordinary-candidate bad) nil)
                   (error () t)))
             (ok (eq slot (physical-slot-test-live slot)))))
         (let ((earlier (pg::copy-wal-replication-slot slot)))
           (setf (pg::wal-replication-slot-restart-lsn earlier) 17)
           (ok (handler-case
                   (eq earlier (physical-slot-test-runtime-ordinary-candidate earlier))
                 (error () nil))))
         (ok (= 33 (pg::wal-replication-slot-restart-lsn slot)))))))

(defun physical-slot-test-runtime-partition (rows certificate host)
  (sb-thread:with-mutex (pg::*wal-lock*)
    (uiop:symbol-call
     :pg :logical-slot-continuation-partition-runtime-rows-under-lock rows certificate host)))

(deftest temporary-physical-runtime-partition-keeps-the-full-current-inventory
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((pg::*wal-checkpoint-snapshot* '(:redo 17 :timeline 1))
              (slot (pg::wal-create-physical-replication-slot
                     "partition_lifetime" :temporary t :immediately-reserve t))
              (sid (pg::catalog-system-identifier))
              (certificate (list :system-id sid :slots nil))
              (host (list sid nil nil (make-array 0 :element-type '(unsigned-byte 8)) 1 32)))
         (pg::wal-advance-physical-replication-slot "partition_lifetime" 75)
         (let* ((row (list "partition_lifetime"
                           (physical-slot-test-file-bytes
                            (pg::wal-replication-slot-path "partition_lifetime"))))
                (rows (list row))
                (before (copy-tree host)))
           (labels ((accepted ()
                      (handler-case
                          (multiple-value-bind (children ordinary)
                              (physical-slot-test-runtime-partition rows certificate host)
                            (and (null children) (= 1 (length ordinary)) (eq row (first ordinary))))
                        (error () nil))))
             (ok (accepted))
             (ok (equalp before host))
             (ok
              (handler-case
                  (progn (physical-slot-test-runtime-partition nil certificate host) nil)
                (error () t)))
             (let ((changed (pg::physical-slot-decode (second row) (first row))))
               (setf (pg::wal-replication-slot-restart-lsn changed) 76)
               (ok (handler-case
                       (progn
                         (physical-slot-test-runtime-partition
                          (list (list (first row) (pg::physical-slot-encode changed))) certificate host)
                         nil)
                     (error () t))))
             (let ((orphan (pg::physical-slot-decode (second row) (first row))))
               (setf (pg::wal-replication-slot-name orphan) "orphan_lifetime")
               (ok (handler-case
                       (progn
                         (physical-slot-test-runtime-partition
                          (list (list "orphan_lifetime" (pg::physical-slot-encode orphan))) certificate host)
                         nil)
                     (error () t))))
             (ok (accepted))
             (ok (eq slot (physical-slot-test-live slot)))
             (ok (= 75 (pg::wal-replication-slot-restart-lsn slot)))))))))

(deftest temporary-physical-name-cannot-be-downgraded-to-a-persistent-generation
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((name "no_downgrade")
              (slot (pg::wal-create-physical-replication-slot name :temporary t))
              (replacement (pg::physical-slot-decode (pg::physical-slot-encode slot) name))
              (bytes (physical-slot-test-file-bytes (pg::wal-replication-slot-path name))))
         (setf (pg::wal-replication-slot-temporary-p replacement) nil)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (setf (gethash name pg::*wal-replication-slots*) replacement)
           (unwind-protect
                (progn
                  (ok (handler-case
                          (progn (pg::wal-find-usable-replication-slot-under-lock name) nil)
                        (error () t)))
                  (ok (handler-case
                          (progn (pg::wal-save-replication-slot replacement) nil)
                        (error () t)))
                  (ok (handler-case
                          (progn (pg::wal-delete-replication-slot-file name replacement) nil)
                        (error () t))))
             (setf (gethash name pg::*wal-replication-slots*) slot)))
         (ok (equalp bytes (physical-slot-test-file-bytes (pg::wal-replication-slot-path name))))
         (ok (pg::wal-replication-slot-state-uncertain-p slot))
         (ok (physical-slot-test-lifetime-refused-p slot))))))

(deftest temporary-physical-name-checks-include-missing-and-downgraded-entries
    (dolist (mode '(:missing :persistent :public-drop :cleanup :owner-predicate))
      (call-with-physical-slot-lifetime-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((name "anchored_name")
                (slot (pg::wal-create-physical-replication-slot name :temporary t)))
           (ecase mode
             (:missing (remhash name pg::*wal-replication-slots*))
             ((:persistent :public-drop :cleanup :owner-predicate) (setf (pg::wal-replication-slot-temporary-p slot) nil)))
           (unwind-protect
                (ok (handler-case
                        (progn
                          (case mode
                            (:public-drop (pg::wal-drop-replication-slot name slot))
                            (:cleanup (pg::wal-cleanup-backend-physical-slots pg::*backend*))
                            (:owner-predicate
                             (sb-thread:with-mutex (pg::*wal-lock*)
                               (pg::physical-slot-owned-by-backend-p slot pg::*backend*)))
                            (otherwise (physical-slot-test-runtime-ordinary-registry)))
                          nil)
                      (error () t)))
             (setf (gethash name pg::*wal-replication-slots*) slot
                   (pg::wal-replication-slot-temporary-p slot) t))
           (ok (pg::wal-replication-slot-state-uncertain-p slot))
           (ok (physical-slot-test-lifetime-refused-p slot)))))))

(deftest temporary-physical-name-anchor-prevents-recycling-before-lookup
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((size pg::+wal-segment-size+)
              (end (+ (* 4 size) 32))
              (pg::*wal-durable-lsn* end)
              (pg::*wal-end-lsn* end)
              (pg::*wal-checkpoint-snapshot* '(:redo 1 :timeline 1))
              (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
              (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
              (pg::*wal-output-stream* nil)
              (pg::*wal-output-segment* nil)
              (slot (pg::wal-create-physical-replication-slot
                     "retained_name" :temporary t :immediately-reserve t))
              (bytes (physical-slot-test-file-bytes
                      (pg::wal-replication-slot-path "retained_name")))
              (replacement (pg::physical-slot-decode bytes "retained_name")))
         (setf pg::*startup-installed-configuration*
               (physical-slot-test-configuration
                (pg::make-wal-slot-retention-policy :mode :segments :limit nil) 0))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (dotimes (segment 5)
           (pg::wal-atomic-write-bytes (pg::wal-segment-path segment) #(0)))
         (setf (pg::wal-replication-slot-temporary-p replacement) nil
               (pg::wal-replication-slot-restart-lsn replacement) (* 3 size)
               (gethash "retained_name" pg::*wal-replication-slots*) replacement)
         (unwind-protect
              (sb-thread:with-mutex (pg::*wal-lock*)
                (ok (handler-case
                        (progn (pg::wal-remove-checkpointed-segments-under-wal-lock end) nil)
                      (error () t))))
           (setf (gethash "retained_name" pg::*wal-replication-slots*) slot))
         (dotimes (segment 5)
           (ok (probe-file (pg::wal-segment-path segment))))
         (ok (equalp bytes (physical-slot-test-file-bytes
                            (pg::wal-replication-slot-path "retained_name"))))
         (ok (pg::wal-replication-slot-state-uncertain-p slot))))))

(deftest temporary-physical-lifetime-creation-requires-admission
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (ok (zerop pg::*live-operation-depth*))
       (ok (not (eq pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)))
       (ok (handler-case
               (progn (pg::wal-create-physical-replication-slot "unadmitted_lifetime" :temporary t) nil)
             (error () t)))
       (ok (null (gethash "unadmitted_lifetime" pg::*wal-replication-slots*)))
       (ok (not (probe-file (pg::wal-replication-slot-path "unadmitted_lifetime")))))
     :admit nil))

(deftest temporary-physical-generated-lifetime-contracts
    (dolist (entry '((temporary-physical-lifetimes-end-with-the-slot-generation 11901)
                     (copied-physical-slots-and-disk-images-carry-no-authority 11902)
                     (temporary-physical-authority-loss-is-sticky 11903)))
      (let ((result (cl-spec:result-data
                     (cl-spec:run-property (first entry) :seed (second entry) :profile :normal))))
        (let ((*print-length* nil) (*print-level* nil))
          (format t "~&TEMPORARY-PHYSICAL-LIFETIME-CONTRACT ~s~%" result))
        (ok (eq :passed (getf result :status)))
        (ok (= 80 (getf result :trials))))))

(deftest temporary-physical-creation-is-private-through-directory-sync
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((observed nil) (early-publish-refused nil)
                             (pg::*logical-test-failpoint-hook*
                               (lambda (point &rest details)
                                 (declare (ignore details))
                                 (when (member point '(:slot-file-before-rename
                                                       :slot-file-after-rename
                                                       :slot-file-after-directory-fsync-before-memory-publication))
                                   (assert (sb-thread:holding-mutex-p pg::*wal-lock*))
                                   (let ((slot (gethash "creating_lifetime" pg::*wal-replication-slots*)))
                                     (when (eq point :slot-file-after-rename)
                                       (setf early-publish-refused
                                             (handler-case
                                                 (progn
                                                   (uiop:symbol-call
                                                    :pg :physical-slot-lifetime-call-under-lock
                                                    (physical-slot-test-lifetime slot) :publish slot :published)
                                                   nil)
                                               (error () t))))
                                     (push (list point (physical-slot-test-lifetime slot)
                                                 (handler-case
                                                     (progn
                                                       (uiop:symbol-call
                                                        :pg :physical-slot-lifetime-check-under-lock slot)
                                                       nil)
                                                   (error () t)))
                                           observed))))))
         (let ((slot (pg::wal-create-physical-replication-slot "creating_lifetime" :temporary t)))
           (ok (equal (mapcar #'first (reverse observed))
                      '(:slot-file-before-rename :slot-file-after-rename
                        :slot-file-after-directory-fsync-before-memory-publication)))
           (ok early-publish-refused)
           (ok (every (lambda (entry) (and (second entry) (third entry))) observed))
           (ok (eq slot (physical-slot-test-live slot))))))))

(deftest temporary-physical-create-failure-retires-only-unpublished-state
    (dolist (point '(:slot-file-before-rename :slot-file-after-rename))
      (call-with-physical-slot-lifetime-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((captured nil)
                (fired nil)
                (pg::*logical-test-failpoint-hook*
                  (lambda (event &rest details)
                    (declare (ignore details))
                    (when (eq event point)
                      (setf fired t
                            captured (gethash "failed_lifetime" pg::*wal-replication-slots*))
                      (error "temporary physical lifetime create fault")))))
           (ok (handler-case
                   (progn (pg::wal-create-physical-replication-slot "failed_lifetime" :temporary t) nil)
                 (error () t)))
           (ok fired)
           (ok (physical-slot-test-lifetime captured))
           (ok (physical-slot-test-lifetime-refused-p captured))
           (if (eq point :slot-file-before-rename)
               (progn
                 (ok (null (gethash "failed_lifetime" pg::*wal-replication-slots*)))
                 (ok (not (probe-file (pg::wal-replication-slot-path "failed_lifetime")))))
               (progn
                 (ok (probe-file (pg::wal-replication-slot-path "failed_lifetime")))
                 (ok (eq captured (gethash "failed_lifetime" pg::*wal-replication-slots*)))
                 (ok (pg::wal-replication-slot-creation-failed-p captured)))))))))

(deftest temporary-physical-drop-failure-retains-exact-retry-authority
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((slot (pg::wal-create-physical-replication-slot "retiring_lifetime" :temporary t))
              (fired nil) (saved-claim nil) (retry-claim nil)
              (pg::*logical-test-failpoint-hook*
                (lambda (point &rest details)
                  (declare (ignore details))
                  (when (and fired (eq point :slot-drop-before-directory-fsync))
                    (setf retry-claim (pg::wal-replication-slot-drop-claim slot)))
                  (when (and (eq point :slot-drop-after-rename) (not fired))
                    (setf fired t
                          saved-claim (pg::wal-replication-slot-drop-claim slot))
                    (error "temporary physical lifetime drop fault")))))
         (ok (handler-case
                 (progn (pg::wal-drop-replication-slot (copy-seq "retiring_lifetime") slot) nil)
               (error () t)))
         (ok fired)
         (let ((claim (pg::wal-replication-slot-drop-claim slot)))
           (ok claim)
           (ok (eq claim saved-claim))
           (let ((name (first claim)) (old (char (first claim) 0)))
             (setf (char name 0) #\q)
             (unwind-protect
                  (ok
                   (sb-thread:with-mutex (pg::*wal-lock*)
                     (handler-case
                         (progn
                           (uiop:symbol-call
                            :pg :physical-slot-temporary-drop-under-lock slot :drop-check)
                           nil)
                       (error () t))))
               (setf (char name 0) old)))
           (ok (eq slot (gethash "retiring_lifetime" pg::*wal-replication-slots*)))
           (ok (physical-slot-test-lifetime-refused-p slot))
           (pg::wal-drop-replication-slot "retiring_lifetime" slot)
           (ok (eq claim retry-claim))
           (ok (eq claim (pg::wal-replication-slot-drop-claim slot)))
           (ok (null (gethash "retiring_lifetime" pg::*wal-replication-slots*)))
           (ok (physical-slot-test-lifetime-refused-p slot)))))))
