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

(defun call-with-select-portal-test (function)
  (let* ((*backend* (make-backend :explicit t))
         (*extended-query-state* (extended-query-state-for-backend *backend*))
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*active-snapshots* 0)
         (*active-xids* nil) (*next-xid* 100)
         (*xid-wrap-failure* nil) (*xid-wrap-pending* nil)
         (*physical-recovery-state* nil) (*wal-ready* nil)
         (prepare (symbol-function 'prepare-xid-wraparound-for-statement))
         (path (merge-pathnames
                (format nil "pg-select-fetch-~d-~d" (sb-posix:getpid) (random 1000000))
                (uiop:temporary-directory))))
    (set-backend-state *backend* :in-progress)
    (unwind-protect
         (progn
           (setf (symbol-function 'prepare-xid-wraparound-for-statement)
                 (lambda (backend) (declare (ignore backend))))
           (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                        :if-exists :supersede :if-does-not-exist :create)
             (funcall function stream)))
      (setf (symbol-function 'prepare-xid-wraparound-for-statement) prepare)
      (forget-extended-query-state *backend*)
      (when (probe-file path) (delete-file path)))))

(defun select-portal-test-frames (stream)
  (finish-output stream)
  (file-position stream 0)
  (loop for (type length payload) = (multiple-value-list (read-message-frame stream))
        while type unless (= type (char-code #\S))
        collect (cons (code-char type) payload)))

(rove:deftest select-portal-exact-boundary-and-per-fetch-counts
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation
        stream '(:parse "s" "SELECT $1::text UNION ALL SELECT $1::text UNION ALL SELECT $1::text" (25)) #())
       (rove:ok (= 1 (backend-command-id *backend*)))
       (extended-apply-operation stream '(:bind "p" "s" () ((0 . 1)) ()) (string->bytes "v"))
       (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
              (execution (extended-select-execution-for portal))
              (plan (extended-select-execution-plan execution))
              (snapshot (query-execution-owner-snapshot (extended-select-execution-owner execution))))
         (rove:ok (= 1 (snapshot-curcid snapshot)))
         (rove:ok (= 1 *active-snapshots*))
         (extended-apply-operation stream '(:execute "p" 3) #())
         (rove:ok (= 1 (backend-command-id *backend*)))
         (rove:ok (not (extended-select-execution-at-end-p execution)))
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (eq portal (gethash "p" (extended-query-state-portals *extended-query-state*))))
         (extended-apply-operation stream '(:execute "p" -1) #())
         (rove:ok (= 2 (backend-command-id *backend*)))
         (rove:ok (eq plan (extended-select-execution-plan execution)))
         (extended-apply-operation stream '(:execute "p" 0) #())
         (rove:ok (= 3 (backend-command-id *backend*)))
         (extended-apply-operation stream '(:close #\S "s") #())
         (rove:ok (gethash "p" (extended-query-state-portals *extended-query-state*)))
         (extended-apply-operation stream '(:close #\P "p") #())
         (rove:ok (zerop *active-snapshots*)))
       (let ((frames (select-portal-test-frames stream)))
         (rove:ok (equal (mapcar #'car frames) '(#\1 #\2 #\D #\D #\D #\s #\Z #\C #\C #\3 #\3)))
         (rove:ok
          (every (lambda (frame) (equalp (cdr frame) (string->bytes (format nil "SELECT 0~c" (code-char 0)))))
                 (remove-if-not (lambda (frame) (char= (car frame) #\C)) frames)))))))

(rove:deftest select-portal-resume-emits-only-remaining-rows
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation
        stream '(:parse "s" "SELECT $1::text UNION ALL SELECT $1::text UNION ALL SELECT $1::text" (25)) #())
       (extended-apply-operation stream '(:bind "p" "s" () ((0 . 1)) (1)) (string->bytes "z"))
       (extended-apply-operation stream '(:execute "p" 1) #())
       (extended-apply-operation stream '(:execute "p" 0) #())
       (let ((frames (select-portal-test-frames stream)))
         (rove:ok (equal (mapcar #'car frames) '(#\1 #\2 #\D #\s #\D #\D #\C)))
         (rove:ok (equalp (cdr (car (last frames)))
                          (string->bytes (format nil "SELECT 2~c" (code-char 0)))))))))

(rove:deftest select-portal-reused-bind-follows-simple-command-completion
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
       (let ((execute (symbol-function 'execute-statement)) (write-cid nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'execute-statement)
                      (lambda (output ast)
                        (declare (ignore output ast))
                        (with-statement-snapshot (snapshot #'statement-start)
                          (setf write-cid (backend-command-id *backend*)))))
                (execute-query-statements stream '((:type :update)) nil))
           (setf (symbol-function 'execute-statement) execute))
         (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
         (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                (owner (extended-select-execution-owner (extended-select-execution-for portal))))
           (rove:ok (< write-cid (snapshot-curcid (query-execution-owner-snapshot owner)))))))))

(rove:deftest select-portal-sync-publishes-used-command-id
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
       (extended-apply-operation stream '(:bind "old" "s" () () ()) #())
       (let ((insert (symbol-function 'exec-insert))
             (write-cid (backend-command-id *backend*)))
         (unwind-protect
              (progn
                ;; Simulate a zero-row DML executor: ExecutorStart still uses its CID.
                (setf (symbol-function 'exec-insert)
                      (lambda (output ast) (declare (ignore output ast))))
                (let ((*statement-command-start-advance-p* nil))
                  (execute-statement stream '(:type :insert))))
           (setf (symbol-function 'exec-insert) insert))
         (extended-apply-operation stream '(:flush) #())
         (rove:ok (= write-cid (backend-command-id *backend*)))
         (extended-apply-operation stream '(:bind "before" "s" () () ()) #())
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= (1+ write-cid) (backend-command-id *backend*)))
         (extended-apply-operation stream '(:bind "after" "s" () () ()) #())
         (flet ((portal-cid (name)
                  (let* ((portal (gethash name (extended-query-state-portals *extended-query-state*)))
                         (owner (extended-select-execution-owner (extended-select-execution-for portal))))
                    (snapshot-curcid (query-execution-owner-snapshot owner)))))
           (rove:ok (= write-cid (portal-cid "old") (portal-cid "before")))
           (rove:ok (= (1+ write-cid) (portal-cid "after"))))
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= (1+ write-cid) (backend-command-id *backend*)))
         ;; Existing Parse/completion advancement consumes an earlier use logically.
         (note-backend-command-id-used)
         (extended-apply-operation stream '(:parse "later" "SELECT 2" ()) #())
         (let ((cid (backend-command-id *backend*)))
           (extended-apply-operation stream '(:sync) #())
           (rove:ok (= cid (backend-command-id *backend*))))
         (note-backend-command-id-used)
         (abort-transaction)
         (rove:ok (not (gethash *backend* *backend-used-command-ids*)))))))

(rove:deftest select-portal-sync-ignores-read-only-and-unexecuted-work
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation stream '(:parse "s" "SELECT 1 UNION ALL SELECT 2" ()) #())
       (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
       (let ((cid (backend-command-id *backend*)))
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= cid (backend-command-id *backend*)))
         (extended-apply-operation stream '(:execute "p" 1) #())
         (extended-apply-operation stream '(:sync) #())
         (extended-apply-operation stream '(:sync) #())
         (rove:ok (= cid (backend-command-id *backend*)))))
     ))

(rove:deftest select-portal-missing-name-diagnostic
    (call-with-select-portal-test
     (lambda (stream)
       (dolist (name '("edge_exact" ""))
         (dolist (operation (list (list :execute name 1) (list :describe #\P name)))
           (let ((condition
                   (handler-case (progn (extended-apply-operation stream operation #()) nil)
                     (pg-error (condition) condition))))
             (rove:ok (and condition (string= (pg-error-sqlstate condition) "34000")))
             (rove:ok (and condition
                           (string= (pg-error-message condition)
                                    (format nil "portal \"~a\" does not exist" name))))))))))

(rove:deftest select-portal-abort-keeps-identity-until-rollback
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
       (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
       (abort-transaction-block)
       (rove:ok (eq :aborted (backend-state *backend*)))
       (rove:ok (zerop *active-snapshots*))
       (dolist (operation '((:execute "p" 1) (:describe #\P "p") (:describe #\S "s")))
         (rove:ok
          (handler-case (progn (extended-apply-operation stream operation #()) nil)
            (pg-error (condition) (string= (pg-error-sqlstate condition) "25P02")))))
       (abort-transaction)
       (rove:ok (not (gethash "p" (extended-query-state-portals *extended-query-state*)))))))

(rove:deftest select-portal-suspension-retains-deadline-until-sync
    (call-with-select-portal-test
     (lambda (stream)
       (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
       (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
       (let* ((deadline (+ (get-internal-real-time) (* 10 internal-time-units-per-second)))
              (payload (concatenate '(vector (unsigned-byte 8))
                                    (string->bytes (format nil "p~c" (code-char 0)))
                                    (u32-to-bytes 1))))
         (setf (extended-query-state-statement-deadline *extended-query-state*) deadline)
         (handle-extended-query-frame stream (char-code #\E)
                                      (u32-to-bytes (+ 4 (length payload))) payload)
         (rove:ok (= deadline (extended-query-state-statement-deadline *extended-query-state*)))
         (handle-extended-query-frame
          stream (char-code #\S) (u32-to-bytes 4)
          (make-array 0 :element-type '(unsigned-byte 8)))
         (rove:ok (null (extended-query-state-statement-deadline *extended-query-state*)))
         (rove:ok (gethash "p" (extended-query-state-portals *extended-query-state*)))
         (setf (extended-query-state-statement-deadline *extended-query-state*)
               (1- (get-internal-real-time)))
         (handle-extended-query-frame stream (char-code #\E)
                                      (u32-to-bytes (+ 4 (length payload))) payload)
         (rove:ok (eq :aborted (backend-state *backend*)))
         (rove:ok (extended-query-state-ignore-until-sync *extended-query-state*))
         (rove:ok (zerop *active-snapshots*))))))

(rove:deftest select-portal-implicit-sync-retires-owner
    (call-with-select-portal-test
     (lambda (stream)
       (setf (backend-explicit *backend*) nil
             (backend-implicit-query-block *backend*) t
             (extended-query-state-implicit-p *extended-query-state*) t)
       (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
       (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
       (extended-apply-operation stream '(:execute "p" 1) #())
       (rove:ok (= 1 *active-snapshots*))
       (extended-apply-operation stream '(:sync) #())
       (rove:ok (eq :idle (backend-state *backend*)))
       (rove:ok (zerop *active-snapshots*))
       (rove:ok (zerop (hash-table-count (extended-query-state-portals *extended-query-state*)))))))

(rove:deftest select-portal-precommit-closes-before-wal-and-xid-release
    (call-with-select-portal-test
     (lambda (stream)
       (let ((wal (symbol-function 'wal-commit-transaction))
             (clog (symbol-function 'clog-set))
             (release (symbol-function 'release-xid))
             (events nil))
         (unwind-protect
              (progn
                (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
                (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
                (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                       (owner (extended-select-execution-owner (extended-select-execution-for portal))))
                  (query-execution-owner-add-cleanup owner (lambda () (push :owner events))))
                (setf (backend-xid *backend*) 777
                      (symbol-function 'wal-commit-transaction)
                      (lambda (xid after &optional changes terminal-durable)
                        (declare (ignore xid changes))
                        (push :wal events)
                        (rove:ok (zerop *active-snapshots*))
                        (when (and *wal-ready* terminal-durable)
                          (funcall terminal-durable 140))
                        (funcall after)
                        (rove:ok (eql (backend-xid *backend*) 777)
                                 "durable publication retains the transaction identity")
                        (rove:ok (not (member :release events))
                                 "storage apply precedes XID release")
                        (push :storage-applied events))
                      (symbol-function 'clog-set)
                      (lambda (&rest arguments) (declare (ignore arguments)))
                      (symbol-function 'release-xid)
                      (lambda (&rest arguments) (declare (ignore arguments)) (push :release events)))
                (commit-transaction)
                (rove:ok (equal (nreverse events) '(:owner :wal :storage-applied :release))))
           (setf (symbol-function 'wal-commit-transaction) wal
                 (symbol-function 'clog-set) clog
                 (symbol-function 'release-xid) release
                 (backend-xid *backend*) nil))))))

(rove:deftest commit-publication-failure-keeps-committed-cleanup
    (call-with-select-portal-test
     (lambda (stream)
       (declare (ignore stream))
       (let ((wal (symbol-function 'wal-commit-transaction))
             (clog (symbol-function 'clog-set))
             (release (symbol-function 'release-xid)))
         (unwind-protect
              (dolist (wal-ready '(nil t))
                (dolist (phase '(:before :clog :storage :success))
                  (let* ((*wal-ready* wal-ready)
                         (settings (make-hash-table :test #'equal))
                         (committed-p (and (not (eq phase :before))
                                           (or wal-ready (not (eq phase :clog)))))
                         (released nil))
                    (setf (backend-xid *backend*) 777
                          (backend-settings-transaction-start *backend*) settings
                          (symbol-function 'release-xid)
                          (lambda (xid &optional backend)
                            (declare (ignore backend))
                            (rove:ok (= xid 777))
                            (setf released t))
                          (symbol-function 'clog-set)
                          (lambda (&rest arguments)
                            (declare (ignore arguments))
                            (when (eq phase :clog) (error "CLOG publication failure")))
                          (symbol-function 'wal-commit-transaction)
                          (lambda (xid after &optional changes terminal-durable)
                            (declare (ignore xid changes))
                            (when (eq phase :before) (error "before durable terminal"))
                            (when (and *wal-ready* terminal-durable)
                              (funcall terminal-durable 140))
                            (funcall after)
                            (when (eq phase :storage) (error "storage apply failure"))
                            140))
                    (let ((failed (handler-case (progn (commit-transaction) nil)
                                    (error () t))))
                      (rove:ok (eql failed (not (eq phase :success)))))
                    (rove:ok (eql released (not (null committed-p))))
                    (if committed-p
                        (progn
                          (rove:ok (null (backend-xid *backend*)))
                          (rove:ok (null (backend-settings-transaction-start *backend*))
                                   "irreversible commit retires rollback settings"))
                        (progn
                          (rove:ok (= (backend-xid *backend*) 777))
                          (rove:ok (eq (backend-settings-transaction-start *backend*) settings))))
                    (setf (backend-xid *backend*) nil
                          (backend-settings-transaction-start *backend*) nil))))
           (setf (symbol-function 'wal-commit-transaction) wal
                 (symbol-function 'clog-set) clog
                 (symbol-function 'release-xid) release
                 (backend-xid *backend*) nil
                 (backend-settings-transaction-start *backend*) nil))))))

(rove:deftest commit-nonlocal-publication-exits-fail-stop
    (call-with-select-portal-test
     (lambda (stream)
       (declare (ignore stream))
       (let* ((directory (merge-pathnames
                          (format nil "pg-commit-exit-~d-~d/" (sb-posix:getpid) (random 1000000))
                          (uiop:temporary-directory)))
              (*query-temp-directory* directory)
              (*wal-ready* t)
              (*wal-output-stream* nil)
              (*wal-dirty-store* nil)
              (*wal-page-cache* (make-hash-table :test #'equal))
              (*wal-page-cache-xids* (make-hash-table :test #'equal))
              (names '(wal-append-running-xacts-under-lock wal-append-batch-start
                       wal-append-record wal-flush wal-slot-record-xid-commit-under-wal-lock
                       wal-primary-apply-page-spool wal-fail-stop clog-set release-xid))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names)))
         (ensure-directories-exist directory)
         (unwind-protect
              (dolist (phase '(:flush :slot :clog :apply :release))
                (let ((*wal-poisoned* nil)
                      (statuses nil)
                      (released nil)
                      (fenced nil))
                  (setf (backend-xid *backend*) 777
                        (backend-settings-transaction-start *backend*) (make-hash-table)
                        (symbol-function 'wal-append-running-xacts-under-lock)
                        (lambda (xid) (declare (ignore xid)))
                        (symbol-function 'wal-append-batch-start) (lambda () 100)
                        (symbol-function 'wal-append-record)
                        (lambda (&rest arguments) (declare (ignore arguments)) 140)
                        (symbol-function 'wal-flush)
                        (lambda () (when (eq phase :flush) (throw 'publication-exit :escaped)))
                        (symbol-function 'wal-slot-record-xid-commit-under-wal-lock)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (when (eq phase :slot) (throw 'publication-exit :escaped)))
                        (symbol-function 'clog-set)
                        (lambda (xid status &key fsync)
                          (declare (ignore xid fsync))
                          (push status statuses)
                          (when (eq phase :clog) (throw 'publication-exit :escaped)))
                        (symbol-function 'wal-primary-apply-page-spool)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (when (eq phase :apply) (throw 'publication-exit :escaped)))
                        (symbol-function 'release-xid)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (setf released t)
                          (when (eq phase :release) (throw 'publication-exit :escaped)))
                        (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*))
                                   "publication fencing runs after releasing WAL")
                          (setf *wal-poisoned* t fenced t)
                          (error condition)))
                  (rove:ok
                   (eq :failed
                       (handler-case
                           (catch 'publication-exit (commit-transaction) :success)
                         (error () :failed)))
                   "a nonlocal publication exit becomes fail-stop, never success")
                  (rove:ok fenced)
                  (rove:ok *wal-poisoned*)
                  (rove:ok released)
                  (rove:ok (null (backend-xid *backend*)))
                  (rove:ok (null (backend-settings-transaction-start *backend*)))
                  (rove:ok (not (member +clog-aborted+ statuses))
                           "indeterminate durability must not publish an abort")))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
           (setf (backend-xid *backend*) nil
                 (backend-settings-transaction-start *backend*) nil)
           (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))))

(rove:deftest committed-cleanup-preserves-first-error
    (let* ((backend (make-backend :xid 777))
           (original (make-condition 'simple-error :format-control "release failed"))
           (names '(release-xid finish-backend-transaction wal-fail-stop))
           (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (attempted nil)
           (fenced nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'release-xid)
                   (lambda (&rest arguments) (declare (ignore arguments)) (error original))
                   (symbol-function 'finish-backend-transaction)
                   (lambda (owner committed-p)
                     (rove:ok (eq owner backend))
                     (rove:ok committed-p)
                     (setf attempted t)
                     (error "secondary cleanup failure"))
                   (symbol-function 'wal-fail-stop)
                   (lambda (condition)
                     (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                     (setf fenced condition)
                     (error condition)))
             (rove:ok (eq original
                          (handler-case (finish-committed-transaction backend 777)
                            (error (condition) condition))))
             (rove:ok attempted)
             (rove:ok (eq fenced original)
                      "cleanup failure must not replace the original release error"))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))

(rove:deftest transaction-wal-obligations-own-source-and-survive-detachment
    (let* ((*backend* (make-backend :state :in-progress :read-only nil))
           (*sync-replication-state* (make-sync-replication-state :source-id :first))
           (other (make-backend :state :in-progress :read-only nil)))
      (multiple-value-bind (context source) (prepare-backend-wal-obligation *backend*)
        (record-transaction-wal-boundary context source 80 t)
        (record-transaction-wal-boundary context source 60 t)
        (record-transaction-wal-boundary context source 100 nil)
        (rove:ok (= 80 (transaction-wal-obligation-catalog-end context)))
        (rove:ok (= 100 (transaction-wal-obligation-user-end context)))
        (rove:ok (null (backend-wal-obligation other)))
        (record-transaction-wal-boundary context source nil t)
        (rove:ok (= 80 (transaction-wal-obligation-catalog-end context)))
        (record-transaction-wal-boundary context :second 999 t)
        (record-transaction-wal-boundary context source 1000 nil)
        (rove:ok (transaction-wal-obligation-source-changed-p context))
        (rove:ok (= 80 (transaction-wal-obligation-catalog-end context)))
        (rove:ok (= 100 (transaction-wal-obligation-user-end context)))
        (let ((empty (prepare-backend-wal-obligation other)))
          (setf (sync-replication-state-source-id *sync-replication-state*) :replacement)
          (multiple-value-bind (again captured) (prepare-backend-wal-obligation other)
            (rove:ok (eq empty again))
            (record-transaction-wal-boundary again captured 7 t)
            (rove:ok (eq :replacement (transaction-wal-obligation-source-id again)))
            (rove:ok (not (transaction-wal-obligation-source-changed-p again)))))
        (reset-backend-transaction *backend*)
        (rove:ok (null (backend-wal-obligation *backend*)))
        (rove:ok (= 80 (transaction-wal-obligation-catalog-end context)))
        (rove:ok (not (eq context (prepare-backend-wal-obligation *backend*)))))))

(rove:deftest commit-captures-own-terminal-and-ignores-empty-preparation
    (let ((wal (symbol-function 'wal-commit-transaction))
          (clog (symbol-function 'clog-set))
          (release (symbol-function 'release-xid))
          (*wal-ready* t)
          (*wal-poisoned* nil)
          (*wal-end-lsn* 100)
          (*sync-replication-state* (make-sync-replication-state :source-id :own)))
      (unwind-protect
           (progn
             (setf (symbol-function 'clog-set)
                   (lambda (&rest arguments) (declare (ignore arguments)))
                   (symbol-function 'release-xid)
                   (lambda (&rest arguments) (declare (ignore arguments)))
                   (symbol-function 'wal-commit-transaction)
                   (lambda (xid after &optional changes durable)
                     (declare (ignore xid changes))
                     (funcall durable 140)
                     (funcall after)
                     (setf *wal-end-lsn* 999)
                     999))
             (dolist (writes '(nil t))
               (let* ((*backend* (make-backend :xid 777 :state :in-progress :read-only nil
                                               :dirty-relations (when writes '(123))))
                      (context (prepare-backend-wal-obligation *backend*)))
                 (rove:ok (eql writes (not (null (backend-own-wal-effects-p *backend*)))))
                 (commit-transaction)
                 (rove:ok (null (backend-wal-obligation *backend*)))
                 (rove:ok (eql (if writes 140 nil)
                               (transaction-wal-obligation-user-end context)))
                 (rove:ok (null (transaction-wal-obligation-catalog-end context)))
                 (rove:ok (eql (if writes :own nil)
                               (transaction-wal-obligation-source-id context))))))
        (setf (symbol-function 'wal-commit-transaction) wal
              (symbol-function 'clog-set) clog
              (symbol-function 'release-xid) release))))

(rove:deftest catalog-save-records-only-active-backend-durable-ends
    (let* ((names '(catalog-compute-oldest-relfrozenxid catalog-image-bytes wal-commit-catalog
                    clog-get clog-set release-xid))
           (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (*physical-recovery-state* nil)
           (*logical-event-apply-id* nil)
           (*sync-replication-state* (make-sync-replication-state :source-id :catalog))
           (*catalog-durable-generation* 0)
           (*catalog-oldest-relfrozenxid* 3)
           (*wal-catalog-flush-generation* 0)
           (*wal-end-lsn* 0)
           (aborted nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'clog-get)
                   (lambda (xid) (declare (ignore xid)) +clog-in-progress+)
                   (symbol-function 'clog-set)
                   (lambda (xid status &key fsync)
                     (declare (ignore xid fsync))
                     (setf aborted (eql status +clog-aborted+)))
                   (symbol-function 'release-xid)
                   (lambda (&rest arguments) (declare (ignore arguments)))
                   (symbol-function 'catalog-compute-oldest-relfrozenxid) (lambda () 3)
                   (symbol-function 'catalog-image-bytes) (lambda () #())
                   (symbol-function 'wal-commit-catalog)
                   (lambda (bytes writer &key relation-deletes logical-event on-record-appended on-durable)
                     (declare (ignore bytes writer relation-deletes logical-event on-record-appended))
                     (incf *wal-catalog-flush-generation*)
                     (when on-durable (funcall on-durable (when *wal-ready* 60)))
                     (setf *wal-end-lsn* 999)
                     999))
             (dolist (ready '(nil t))
               (dolist (state '(:idle :in-progress))
                 (let* ((*wal-ready* ready)
                        (*backend* (make-backend :state state :xid 777 :read-only nil)))
                   (save-catalog)
                   (let ((context (backend-wal-obligation *backend*)))
                     (if (and ready (eq state :in-progress))
                         (progn
                           (rove:ok (= 60 (transaction-wal-obligation-catalog-end context)))
                           (rove:ok (eq :catalog (transaction-wal-obligation-source-id context)))
                           (rove:ok (null (transaction-wal-obligation-user-end context))))
                         (rove:ok (null context)))
                     (rove:ok (= 777 (backend-xid *backend*)))
                     (setf aborted nil)
                     (abort-transaction)
                     (rove:ok aborted)
                     (when context
                       (rove:ok (= 60 (transaction-wal-obligation-catalog-end context))))
                     (rove:ok (null (backend-wal-obligation *backend*))))))))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))

(cl-spec:defproperty transaction-wal-obligation-keeps-one-source
    ((first-end (range integer 1 1000))
     (second-end (range integer 1 1000))
     (switch (range integer 0 1)))
  (:about record-transaction-wal-boundary)
  (:trials (:normal 100))
  (let ((context (make-transaction-wal-obligation))
        (changed (= switch 1)))
    (record-transaction-wal-boundary context :first first-end t)
    (record-transaction-wal-boundary context (if changed :second :first) second-end t)
    (record-transaction-wal-boundary context :first 2000 nil)
    (and (= (if changed first-end (max first-end second-end))
            (transaction-wal-obligation-catalog-end context))
         (eql changed (transaction-wal-obligation-source-changed-p context))
         (eq :first (transaction-wal-obligation-source-id context))
         (eql (if changed nil 2000) (transaction-wal-obligation-user-end context)))))

(rove:deftest generated-transaction-wal-obligation-source-fence
    (unit-check-property 'transaction-wal-obligation-keeps-one-source))

(rove:deftest transaction-wal-effects-include-empty-private-replacements
    (let* ((backend (make-backend :xid 777 :state :in-progress :read-only nil))
           (other (make-backend))
           (*transaction-relation-replacements* (make-hash-table :test #'eq))
           (*wal-page-cache* (make-hash-table :test #'equal))
           (replacements (make-hash-table :test #'equal)))
      (setf (gethash '(99 0 :main) *wal-page-cache*) #()
            (gethash other *transaction-relation-replacements*) replacements
            (gethash '(123 :main) replacements) (make-relation-replacement :relfilenode 123))
      (rove:ok (not (backend-own-wal-effects-p backend)))
      (let ((private (make-hash-table :test #'equal)))
        (setf (gethash backend *transaction-relation-replacements*) private)
        (rove:ok (not (backend-own-wal-effects-p backend)))
        (setf (gethash '(456 :main) private) (make-relation-replacement :relfilenode 456))
        (rove:ok (backend-own-wal-effects-p backend))
        (clrhash private))
      (setf (backend-logical-change-bytes backend) 1)
      (rove:ok (backend-own-wal-effects-p backend))))

(rove:deftest backend-termination-is-visible-before-interrupt-delivery
    (let* ((backend (make-backend :thread sb-thread:*current-thread*))
           (*backend* nil)
           (pending-reader (lambda () (backend-termination-pending backend)))
           (unexpected nil))
      (setf unexpected
            (handler-case
                (sb-sys:without-interrupts
                  (terminate-backend backend)
                  (rove:ok (funcall pending-reader)
                           "masked code sees termination before queued delivery")
                  (unwind-protect
                       (progn
                         (begin-backend-query-execution backend)
                         (rove:ok (funcall pending-reader)))
                    (end-backend-query-execution backend))
                  (rove:ok (funcall pending-reader)
                           "query cleanup does not consume pending termination")
                  (let ((*backend* backend))
                    (setf (backend-statement-deadline backend) 0
                          (backend-cancel-pending backend) t)
                    (let ((condition (handler-case (progn (check-for-interrupts) nil)
                                       (pg-error (condition) condition))))
                      (rove:ok (typep condition 'pg-error))
                      (rove:ok (equal "57P01" (pg-error-sqlstate condition)))
                      (rove:ok (equal "FATAL" (pg-error-severity condition))))
                    (rove:ok (not (funcall pending-reader))
                             "the target consumes termination when delivering FATAL"))
                  nil)
              (pg-error (condition) condition)))
      (rove:ok (null unexpected)
               "the queued interrupt does not deliver a second FATAL")))

(rove:deftest backend-termination-is-checked-before-reload-admission
    (dolist (entry '(wait-for-live-operation-admission try-enter-live-operation))
      (let* ((*backend* (make-backend :termination-pending t))
             (*live-operation-admission-open* nil)
             (*live-operation-drain-open* nil)
             (*live-operation-terminal-shutdown* nil)
             (*live-operation-check-cancel-p* t)
             (*live-operation-count* 0)
             (*live-operation-backend-counts* (make-hash-table :test #'eq)))
        (let ((condition (handler-case (progn (funcall entry nil *backend*) nil)
                           (pg-error (condition) condition))))
          (rove:ok (typep condition 'pg-error))
          (rove:ok (equal "57P01" (pg-error-sqlstate condition)))
          (rove:ok (equal "FATAL" (pg-error-severity condition))))
        (rove:ok (null (backend-termination-pending *backend*)))
        (rove:ok (zerop *live-operation-count*))
        (rove:ok (zerop (hash-table-count *live-operation-backend-counts*)))))
  (let* ((*backend* (make-backend :termination-pending t))
         (*live-operation-admission-open* t)
         (*live-operation-terminal-shutdown* nil)
         (*live-operation-check-cancel-p* nil))
    (wait-for-live-operation-admission nil *backend*)
    (rove:ok (backend-termination-pending *backend*)
             "cleanup admission defers delivery without consuming termination")))

(rove:deftest select-portal-connection-finish-invalidates-before-retirement
    (call-with-select-portal-test
     (lambda (stream)
       (let ((close (symbol-function 'close-connection))
             (finished (symbol-function 'connection-finished))
             (endpoint-closed nil) (visible-handle nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (extended-apply-operation stream '(:parse "s" "SELECT 1" ()) #())
                (extended-apply-operation stream '(:bind "p" "s" () () ()) #())
                (let* ((portal (gethash "p" (extended-query-state-portals *extended-query-state*)))
                       (execution (extended-select-execution-for portal))
                       (owner (extended-select-execution-owner execution)))
                  (query-execution-owner-add-cleanup
                   owner
                   (lambda ()
                     (unless endpoint-closed (error "endpoint still open"))
                     (when (gethash "p" (extended-query-state-portals *extended-query-state*))
                       (setf visible-handle t))
                     (error "permanent terminal closer")))
                  (setf (symbol-function 'close-connection)
                        (lambda (socket output)
                          (declare (ignore socket output))
                          (setf endpoint-closed t))
                        (symbol-function 'connection-finished) (lambda () nil))
                  (finish-backend-connection *backend* nil stream)
                  (rove:ok endpoint-closed)
                  (rove:ok (not visible-handle))
                  (rove:ok (extended-select-execution-failed-p execution))
                  (rove:ok (query-execution-owner-closed-p owner))
                  (rove:ok (zerop *active-snapshots*))
                  (rove:ok (not (gethash *backend* *extended-query-state-registry*)))))
           (setf (symbol-function 'close-connection) close
                 (symbol-function 'connection-finished) finished))))))

(cl-spec:defproperty select-portal-execute-limit-is-signed
    ((limit (range integer -2147483648 2147483647)))
  (:about extended-parse-frame)
  (:trials (:normal 100))
  (let ((payload
          (concatenate '(vector (unsigned-byte 8))
                       (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)
                       (u32-to-bytes (logand limit #xffffffff)))))
    (equal (extended-parse-frame #\E payload) (list :execute "" limit))))

(rove:deftest select-portal-generated-signed-limits
    (unit-check-property 'select-portal-execute-limit-is-signed))

(rove:deftest query-owner-promotion-census-includes-dormant-portal
    (let* ((*active-snapshot-set* (make-hash-table :test #'eq))
           (*active-snapshots* 0)
           (*backend* (make-backend :thread sb-thread:*current-thread*))
           (owner (make-query-execution-owner)))
      (unwind-protect
           (progn
             (query-execution-owner-acquire-snapshot
              owner (lambda (registration) (make-snapshot 15 :register registration)))
             (rove:ok
              (progn (physical-promotion-check-reader-snapshots (list *backend*) nil) t)))
        (close-query-execution-owner owner))))

(rove:deftest query-owner-terminal-retirement-drops-failed-executors
    (let* ((*active-snapshot-set* (make-hash-table :test #'eq))
           (*active-snapshots* 0)
           (*backend* (make-backend))
           (*error-output* (make-string-output-stream))
           (a (make-query-execution-owner)) (b (make-query-execution-owner))
           (next-calls 0))
      (unwind-protect
           (progn
             (dolist (owner (list a b))
               (query-execution-owner-acquire-snapshot
                owner (lambda (registration) (make-snapshot 21 :register registration)))
               (call-with-query-execution-owner
                owner (lambda () (make-sql-iterator (lambda () (incf next-calls) (values #() t)))))
               (query-execution-owner-add-cleanup owner (lambda () (error "permanent closer failure"))))
             (rove:ok (not (close-query-execution-owner a)))
             (rove:ok (= 2 *active-snapshots*))
             (rove:ok (retire-query-execution-owners-for-backend *backend*))
             (rove:ok (and (query-execution-owner-closed-p a) (query-execution-owner-closed-p b)))
             (rove:ok (zerop *active-snapshots*))
             (rove:ok (zerop next-calls))
             (rove:ok (not (member a (query-execution-owner-reload-state))))
             (rove:ok (call-with-live-operation-reload (lambda () t))))
        (retire-query-execution-owners-for-backend *backend*))))

(rove:deftest query-owner-terminal-retirement-releases-lease-without-spilling
    (let* ((*backend* (make-backend))
           (*error-output* (make-string-output-stream))
           (owner (make-query-execution-owner))
           (retired nil))
      (unwind-protect
           (call-with-btree-relation-read-lease
            990021
            (lambda (lease)
              (setf (query-execution-owner-binding owner '*select-index-scans*)
                    (list (make-select-index-scan :lease lease)))
              (query-execution-owner-add-cleanup owner (lambda () (error "permanent failure")))
              (retire-query-execution-owners-for-backend *backend*)
              (setf retired t)
              (rove:ok (not (btree-read-lease-active-p lease)))
              (rove:ok (query-execution-owner-closed-p owner))))
        (unless retired (retire-query-execution-owners-for-backend *backend*)))
      (call-with-btree-relation-write-lock 990021 (lambda () (rove:ok t)))))

(rove:deftest query-owner-terminal-retirement-retains-file-charge
    (let* ((*backend* (make-backend))
           (*active-snapshot-set* (make-hash-table :test #'eq))
           (*active-snapshots* 0)
           (*query-temp-pending-cleanups* nil)
           (*error-output* (make-string-output-stream))
           (*query-temp-directory*
             (merge-pathnames
              (format nil "pg-terminal-charge-~d-~d/" (sb-posix:getpid) (random 1000000))
              (uiop:temporary-directory)))
           (owner (make-query-execution-owner))
           (quota (backend-query-temp-quota *backend*))
           (unlink (symbol-function 'unlink-query-temp-file)))
      (ensure-directories-exist *query-temp-directory*)
      (unwind-protect
           (progn
             (query-execution-owner-acquire-snapshot
              owner (lambda (registration) (make-snapshot 19 :register registration)))
             (call-with-query-execution-owner
              owner
              (lambda ()
                (let ((spool (make-query-row-spool 0)))
                  (query-row-spool-append spool "retained terminal charge")
                  (query-row-spool-seal spool))))
             (query-execution-owner-add-cleanup owner (lambda () (error "permanent callback")))
             (setf (symbol-function 'unlink-query-temp-file)
                   (lambda (path) (declare (ignore path)) (error "injected unlink failure")))
             (retire-query-execution-owners-for-backend *backend*)
             (rove:ok (query-execution-owner-closed-p owner))
             (rove:ok (zerop *active-snapshots*))
             (rove:ok (plusp (query-temp-quota-used-bytes quota)))
             (rove:ok (= 1 (getf (query-temp-pending-cleanup-status) :records)))
             (setf (symbol-function 'unlink-query-temp-file) unlink)
             (retry-pending-query-temp-cleanup)
             (rove:ok (zerop (query-temp-quota-used-bytes quota))))
        (setf (symbol-function 'unlink-query-temp-file) unlink)
        (retire-query-execution-owners-for-backend *backend*)
        (retry-pending-query-temp-cleanup)
        (uiop:delete-directory-tree *query-temp-directory* :validate t :if-does-not-exist :ignore))))
