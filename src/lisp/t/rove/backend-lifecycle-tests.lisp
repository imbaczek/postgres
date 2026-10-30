;;;; Isolated backend lifecycle regression.
(in-package #:pg)

(rove:deftest test-unregister-backend-clears-query-state
    (let* ((backend (make-backend))
           (*live-operation-backend-query-tokens*
             (make-hash-table :test #'eq)))
      (setf (backend-executing backend) t
            (backend-cancel-pending backend) t
            (gethash backend *live-operation-backend-query-tokens*) (cons nil nil))
      (unregister-backend backend)
      (rove:ok (and (not (backend-executing backend))
                    (not (backend-cancel-pending backend))
                    (not (gethash backend *live-operation-backend-query-tokens*)))
               "backend teardown clears query state even if query cleanup was interrupted")))

(cl-spec:defgenerator query-owner-close-arguments ()
  (list (make-query-execution-owner)))

(cl-spec:defspec-function close-query-execution-owner
  "Closing an owner retires its reload claim and is safe to repeat."
  (:args (owner (type query-execution-owner)))
  (:args-generator query-owner-close-arguments)
  (:returns (member t))
  (:post
   (and (query-execution-owner-closed-p owner)
        (not (member owner (query-execution-owner-reload-state) :test #'eq))
        (close-query-execution-owner owner))))

(cl-spec:defproperty query-owner-contexts-remain-isolated
    ((left (range integer -1000 1000)) (right (range integer -1000 1000)))
  (:about call-with-query-execution-owner query-execution-owner-binding)
  (:trials (:normal 50))
  (let ((a (make-query-execution-owner))
        (b (make-query-execution-owner))
        (*query-parameter-values* #(9999)))
    (unwind-protect
         (progn
           (setf (query-execution-owner-binding a '*query-parameter-values*) (vector left)
                 (query-execution-owner-binding b '*query-parameter-values*) (vector right))
           (call-with-query-execution-owner
            a (lambda ()
                (setf (gethash :saved *subquery-result-cache*) left)
                (incf (aref *query-parameter-values* 0))))
           (call-with-query-execution-owner
            b (lambda ()
                (unless (null (gethash :saved *subquery-result-cache*))
                  (error "another portal's cache became visible"))
                (setf (gethash :saved *subquery-result-cache*) right)))
           (and (= 9999 (aref *query-parameter-values* 0))
                (call-with-query-execution-owner
                 a (lambda ()
                     (and (= (gethash :saved *subquery-result-cache*) left)
                          (= (aref *query-parameter-values* 0) (1+ left)))))
                (call-with-query-execution-owner
                 b (lambda () (= (aref *query-parameter-values* 0) right)))))
      (close-query-execution-owner a)
      (close-query-execution-owner b))))

(rove:deftest query-owner-generated-lifecycle
    (let* ((data (cl-spec:result-data
                  (cl-spec:check-function 'close-query-execution-owner :trials 50 :seed 42)))
           (passed (eq :passed (getf data :status))))
      (rove:ok passed (if passed "query owner close Function Spec passed"
                          (format nil "~s" data))))
  (unit-check-property 'query-owner-contexts-remain-isolated))

(rove:deftest query-owner-retains-distinct-snapshot
    (let ((*active-snapshot-set* (make-hash-table :test #'eq))
          (*active-snapshots* 0)
          (owner (make-query-execution-owner))
          (saved nil))
      (unwind-protect
           (progn
             (with-statement-snapshot
                 (source (lambda (registration)
                           (make-snapshot 17 :register registration)))
               (setf saved (query-execution-owner-pin-snapshot owner source))
               (rove:ok (not (eq source saved)))
               (rove:ok (= 2 *active-snapshots*))
               (rove:ok (= 17 (snapshot-curcid saved))))
             (rove:ok (= 1 *active-snapshots*))
             (rove:ok (gethash saved *active-snapshot-set*))
             (call-with-query-execution-owner
              owner (lambda ()
                      (rove:ok (eq *current-snapshot* saved))
                      (rove:ok
                       (member (query-execution-owner-snapshot-registration owner)
                               *statement-snapshot-registrations*))))
             (close-query-execution-owner owner)
             (rove:ok (zerop *active-snapshots*))
             (rove:ok (zerop (hash-table-count *active-snapshot-set*))))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-reload-waits-with-admission-open
    (let* ((owner (make-query-execution-owner))
           (waiting (sb-thread:make-semaphore :count 0))
           (finished (sb-thread:make-semaphore :count 0))
           (ran nil)
           (worker
             (sb-thread:make-thread
              (lambda ()
                (let ((*query-execution-owner-reload-wait-hook*
                        (lambda (generation)
                          (declare (ignore generation))
                          (sb-thread:signal-semaphore waiting))))
                  (unwind-protect
                       (call-with-live-operation-reload (lambda () (setf ran t)))
                    (sb-thread:signal-semaphore finished))))
              :name "query owner reload test")))
      (unwind-protect
           (progn
             (rove:ok (sb-thread:wait-on-semaphore waiting :timeout 5))
             (rove:ok (not ran))
             (rove:ok
              (call-with-live-operation-lock (lambda () *live-operation-admission-open*)))
             (with-live-operation (rove:ok (not ran)))
             (close-query-execution-owner owner)
             (rove:ok (sb-thread:wait-on-semaphore finished :timeout 5))
             (sb-thread:join-thread worker :timeout 5)
             (rove:ok ran))
        (close-query-execution-owner owner)
        (sb-thread:join-thread worker :timeout 5))))

(rove:deftest query-owner-self-reload-queues-without-waiting
    (let ((owner (make-query-execution-owner)) (*reload-requested* nil) (ran nil))
      (unwind-protect
           (progn
             (rove:ok
              (handler-case
                  (progn
                    (call-with-live-operation-reload (lambda () (setf ran t)))
                    nil)
                (error () t)))
             (rove:ok (not ran))
             (rove:ok (eq :queued (reload)))
             (rove:ok *reload-requested*))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-close-inside-callback-does-not-save-state
    (let ((owner (make-query-execution-owner)) (closed 0))
      (unwind-protect
           (progn
             (call-with-query-execution-owner
              owner (lambda ()
                      (make-sql-iterator (lambda () (values nil nil))
                                         (lambda () (incf closed)))
                      (close-query-execution-owner owner)
                      (setf *subquery-result-cache* (make-hash-table :test #'eq))))
             (rove:ok (= closed 1))
             (rove:ok (query-execution-owner-closed-p owner))
             (rove:ok (null (query-execution-owner-bindings owner)))
             (rove:ok (close-query-execution-owner owner)))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-failed-closer-retains-snapshot-and-census
    (let ((*active-snapshot-set* (make-hash-table :test #'eq))
          (*active-snapshots* 0)
          (owner (make-query-execution-owner))
          (calls 0))
      (unwind-protect
           (progn
             (query-execution-owner-acquire-snapshot
              owner (lambda (registration) (make-snapshot 9 :register registration)))
             (query-execution-owner-add-cleanup
              owner
              (lambda ()
                ;; A closer may inspect the census: no registry mutex is held.
                (query-execution-owner-reload-state)
                (when (= (incf calls) 1) (error "retry this closer"))))
             (rove:ok (not (close-query-execution-owner owner)))
             (rove:ok (= 1 *active-snapshots*))
             (rove:ok (member owner (query-execution-owner-reload-state)))
             (rove:ok (close-query-execution-owner owner))
             (rove:ok (zerop *active-snapshots*))
             (rove:ok (= 2 calls))
             (rove:ok (close-query-execution-owner owner))
             (rove:ok (= 2 calls)))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-fatal-closer-propagates-after-stabilization
    (let ((owner (make-query-execution-owner)) (calls 0) (caught nil))
      (unwind-protect
           (progn
             (query-execution-owner-add-cleanup
              owner
              (lambda ()
                (when (= (incf calls) 1)
                  (ereport-fatal "57P01" "injected backend termination"))))
             (handler-case (close-query-execution-owner owner)
               (pg-error (condition) (setf caught condition)))
             (rove:ok (and caught (fatal-error-p caught)))
             (rove:ok (eq :cleanup-pending (query-execution-owner-state owner)))
             (rove:ok (member owner (query-execution-owner-reload-state)))
             (rove:ok (close-query-execution-owner owner))
             (rove:ok (= calls 2)))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-backend-close-attempts-all-before-fatal
    (let* ((*active-snapshot-set* (make-hash-table :test #'eq))
           (*active-snapshots* 0)
           (*backend* (make-backend))
           (a (make-query-execution-owner))
           (b (make-query-execution-owner))
           (owners (remove-if-not
                    (lambda (owner) (eq *backend* (query-execution-owner-backend owner)))
                    (query-execution-owner-reload-state)))
           (fatal-calls 0) (other-calls 0) (caught nil))
      (unwind-protect
           (progn
             (dolist (owner owners)
               (query-execution-owner-acquire-snapshot
                owner (lambda (registration) (make-snapshot 13 :register registration))))
             (query-execution-owner-add-cleanup
              (first owners)
              (lambda ()
                (when (= (incf fatal-calls) 1)
                  (ereport-fatal "57P01" "injected first-owner termination"))))
             (query-execution-owner-add-cleanup (second owners) (lambda () (incf other-calls)))
             (handler-case (close-query-execution-owners-for-backend *backend*)
               (pg-error (condition) (setf caught condition)))
             (rove:ok (and caught (fatal-error-p caught)))
             (rove:ok (and (query-execution-owner-closed-p a)
                           (query-execution-owner-closed-p b)))
             (rove:ok (= fatal-calls 2))
             (rove:ok (= other-calls 1))
             (rove:ok (zerop *active-snapshots*)))
        (close-query-execution-owner a)
        (close-query-execution-owner b))))

(rove:deftest query-owner-partial-snapshot-acquisition-is-discoverable
    (let* ((*active-snapshot-set* (make-hash-table :test #'eq))
           (*active-snapshots* 0)
           (*backend* (make-backend))
           (owner (make-query-execution-owner)))
      (unwind-protect
           (progn
             (rove:ok
              (handler-case
                  (progn
                    (query-execution-owner-acquire-snapshot
                     owner
                     (lambda (registration)
                       (make-snapshot 11 :register registration)
                       (error "interrupted after snapshot publication")))
                    nil)
                (error () t)))
             (rove:ok (= 1 *active-snapshots*))
             (rove:ok (null (close-query-execution-owners-for-backend *backend*)))
             (rove:ok (query-execution-owner-closed-p owner))
             (rove:ok (null (query-execution-owner-backend owner)))
             (rove:ok (zerop *active-snapshots*)))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-quota-includes-ordinary-scopes
    (let* ((*backend* (make-backend))
           (*query-temp-file-limit-bytes* 100000)
           (*query-temp-directory*
             (merge-pathnames
              (format nil "pg-owner-quota-~d-~d/" (sb-posix:getpid) (random 1000000))
              (uiop:temporary-directory)))
           (owner (make-query-execution-owner))
           (quota (backend-query-temp-quota *backend*))
           (used 0))
      (ensure-directories-exist *query-temp-directory*)
      (unwind-protect
           (progn
             (call-with-query-execution-owner
              owner (lambda ()
                      (let ((spool (make-query-row-spool 0)))
                        (query-row-spool-append spool (make-string 128 :initial-element #\x))
                        (query-row-spool-seal spool))))
             (setf used (query-temp-quota-used-bytes quota)
                   *query-temp-file-limit-bytes* (1+ used))
             (rove:ok (plusp used))
             (rove:ok
              (handler-case
                  (with-query-temp-files ()
                                         (rove:ok (eq quota *query-temp-quota*))
                                         (let ((spool (make-query-row-spool 0)))
                                           (query-row-spool-append spool (make-string 128 :initial-element #\y))
                                           (query-row-spool-seal spool))
                                         nil)
                (pg-error (condition) (string= (pg-error-sqlstate condition) "53400"))))
             (rove:ok (= used (query-temp-quota-used-bytes quota)))
             (close-query-execution-owner owner)
             (rove:ok (zerop (query-temp-quota-used-bytes quota))))
        (close-query-execution-owner owner)
        (uiop:delete-directory-tree *query-temp-directory* :validate t :if-does-not-exist :ignore))))

(rove:deftest query-owner-pending-temp-cleanup-retains-shared-charge
    (let* ((*backend* (make-backend))
           (*query-temp-pending-cleanups* nil)
           (*query-temp-directory*
             (merge-pathnames
              (format nil "pg-owner-pending-~d-~d/" (sb-posix:getpid) (random 1000000))
              (uiop:temporary-directory)))
           (*error-output* (make-string-output-stream))
           (owner (make-query-execution-owner))
           (quota (backend-query-temp-quota *backend*))
           (unlink (symbol-function 'unlink-query-temp-file)))
      (ensure-directories-exist *query-temp-directory*)
      (unwind-protect
           (progn
             (call-with-query-execution-owner
              owner (lambda ()
                      (let ((spool (make-query-row-spool 0)))
                        (query-row-spool-append spool "pending charge")
                        (query-row-spool-seal spool))))
             (setf (symbol-function 'unlink-query-temp-file)
                   (lambda (path) (declare (ignore path)) (error "injected unlink failure")))
             (close-query-execution-owner owner)
             (rove:ok (plusp (query-temp-quota-used-bytes quota)))
             (rove:ok (= 1 (getf (query-temp-pending-cleanup-status) :records)))
             (setf (symbol-function 'unlink-query-temp-file) unlink)
             (retry-pending-query-temp-cleanup)
             (rove:ok (zerop (query-temp-quota-used-bytes quota)))
             (rove:ok (zerop (getf (query-temp-pending-cleanup-status) :records))))
        (setf (symbol-function 'unlink-query-temp-file) unlink)
        (close-query-execution-owner owner)
        (retry-pending-query-temp-cleanup)
        (uiop:delete-directory-tree *query-temp-directory* :validate t :if-does-not-exist :ignore))))

(rove:deftest query-owner-private-directory-is-not-restored-after-close
    (let* ((*query-temp-directory* nil)
           (*query-temp-instance-directory*
             (merge-pathnames
              (format nil "pg-owner-private-~d-~d/" (sb-posix:getpid) (random 1000000))
              (uiop:temporary-directory)))
           (owner (make-query-execution-owner))
           (directory nil)
           (temporary (query-execution-owner-temp-owner owner)))
      (ensure-directories-exist *query-temp-instance-directory*)
      (sb-posix:chmod (namestring *query-temp-instance-directory*) #o700)
      (unwind-protect
           (progn
             (call-with-query-execution-owner
              owner
              (lambda ()
                (let ((spool (make-query-row-spool 0)))
                  (query-row-spool-append spool "private directory")
                  (query-row-spool-seal spool)
                  (setf directory (query-temp-cleanup-owner-directory temporary)))))
             (rove:ok (and directory (probe-file directory)))
             (rove:ok (close-query-execution-owner owner))
             (rove:ok (not (probe-file directory)))
             (rove:ok (eq :resolved (query-temp-cleanup-owner-state temporary)))
             (rove:ok (null (query-temp-cleanup-owner-directory temporary)))
             (rove:ok (not (query-temp-owner-unresolved-p temporary))))
        (close-query-execution-owner owner)
        (uiop:delete-directory-tree *query-temp-instance-directory*
                                    :validate t :if-does-not-exist :ignore))))
