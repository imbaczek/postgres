(in-package #:pg-physical-tests)

(cl-spec:defproperty postmaster-pid-number-preserves-signed-identity
    ((pid (range integer -2147483648 2147483648)))
  (:about pg::postmaster-parse-pid)
  (:trials (:normal 100))
  (let ((expected (and (not (zerop pid)) (<= (abs pid) 2147483647) (abs pid))))
    (and (eql expected (pg::postmaster-parse-pid (format nil "~d" pid)))
         (eql expected (pg::postmaster-parse-pid (format nil " ~d ~%" pid))))))

(deftest postmaster-pid-number-generated-contract-and-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'postmaster-pid-number-preserves-signed-identity :seed 203))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 100 (getf data :trials))))
  (dolist (pid '(-2147483648 -2147483647 -1 0 1 2147483647 2147483648))
    (ok (eql (and (not (zerop pid)) (<= (abs pid) 2147483647) (abs pid))
             (pg::postmaster-parse-pid (format nil "~d" pid)))))
  (dolist (text '("" "abc" "1junk" "--1"))
    (ok (signals (pg::postmaster-parse-pid text) 'parse-error))))

(defun call-with-postmaster-proof-fixture (function &key (mode #o700))
  (with-physical-store-directory (root)
    (call-with-adoption-postmaster-fixture
     root
     (lambda ()
       (sb-posix:chmod (namestring root) mode)
       (pg::postmaster-own-data-directory "127.0.0.1" 5439)
       (funcall function root)))))

(deftest postmaster-ownership-requires-published-ownership
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (let ((called nil))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-postmaster-ownership
                   (lambda (owner) (declare (ignore owner)) (setf called t))))))
           (ok (not called)))))))

(deftest postmaster-ownership-borrows-streams-and-preserves-values
    (dolist (mode '(#o700 #o750))
      (call-with-postmaster-proof-fixture
       (lambda (root)
         (declare (ignore root))
         (let ((escaped nil) (lock pg::*postmaster-data-lock*)
                             (pid pg::*postmaster-pid-stream*) (pg::*wal-ready* t))
           (ok (equal '(17 :owned)
                      (multiple-value-list
                       (pg::call-with-postmaster-ownership
                        (lambda (owner)
                          (setf escaped owner)
                          (ok (eq owner (pg::postmaster-check-ownership owner)))
                          (values 17 :owned))))))
           (ok (not (pg::postmaster-ownership-active-p escaped)))
           (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership escaped))))
           (ok (open-stream-p lock)) (ok (open-stream-p pid))
           (ok (pg::postmaster-pid-owned-p))))
       :mode mode)))

(deftest postmaster-ownership-rejects-other-threads-without-revoking-owner
    (call-with-postmaster-proof-fixture
     (lambda (root)
       (declare (ignore root))
       (pg::call-with-postmaster-ownership
        (lambda (owner)
          (let ((thread
                  (sb-thread:make-thread
                   (lambda () (intake-fails-p (lambda () (pg::postmaster-check-ownership owner)))))))
            (ok (sb-thread:join-thread thread)))
          (ok (eq owner (pg::postmaster-check-ownership owner))))))))

(deftest postmaster-ownership-invalidates-on-all-scope-exits
    (dolist (mode '(:return :error :nonlocal))
      (call-with-postmaster-proof-fixture
       (lambda (root)
         (declare (ignore root))
         (let ((escaped nil))
           (catch 'postmaster-proof-exit
             (handler-case
                 (pg::call-with-postmaster-ownership
                  (lambda (owner)
                    (setf escaped owner)
                    (case mode
                      (:error (error "test failure"))
                      (:nonlocal (throw 'postmaster-proof-exit :done)))))
               (error () nil)))
           (ok escaped)
           (ok (not (pg::postmaster-ownership-active-p escaped)))
           (ok (open-stream-p pg::*postmaster-data-lock*))
           (ok (open-stream-p pg::*postmaster-pid-stream*))
           (ok (pg::postmaster-pid-owned-p)))))))

(deftest postmaster-ownership-never-reacquires-a-lost-flock
    (call-with-postmaster-proof-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((lock pg::*postmaster-data-lock*) (escaped nil))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-postmaster-ownership
                 (lambda (owner)
                   (setf escaped owner)
                   (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd lock) 8)))
                   (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership owner))))
                   (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd lock) 6)))
                   (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership owner)))))))))
         (ok (not (pg::postmaster-ownership-active-p escaped)))
         (ok (intake-fails-p
              (lambda () (pg::call-with-postmaster-ownership #'identity))))
         (ok (open-stream-p lock))
         (ok (pg::postmaster-pid-owned-p))))))

(deftest postmaster-ownership-rechecks-publication-after-callback
    (dolist (role '(:lock :pid))
      (call-with-postmaster-proof-fixture
       (lambda (root)
         (declare (ignore root))
         (let ((saved (if (eq role :lock) pg::*postmaster-data-lock* pg::*postmaster-pid-stream*))
               (entered nil))
           (unwind-protect
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-postmaster-ownership
                        (lambda (owner)
                          (declare (ignore owner))
                          (setf entered t)
                          (if (eq role :lock)
                              (setf pg::*postmaster-data-lock* nil)
                              (setf pg::*postmaster-pid-stream* nil)))))))
             (if (eq role :lock)
                 (setf pg::*postmaster-data-lock* saved)
                 (setf pg::*postmaster-pid-stream* saved)))
           (ok entered)
           (ok (open-stream-p saved))
           (ok (pg::postmaster-pid-owned-p)))))))

(defun check-postmaster-proof-path-replacement (role)
  (call-with-postmaster-proof-fixture
   (lambda (root)
     (let ((escaped nil))
       (ok (intake-fails-p
            (lambda ()
              (pg::call-with-postmaster-ownership
               (lambda (owner)
                 (setf escaped owner)
                 (let* ((path (case role
                                (:root root)
                                (:lock (merge-pathnames ".pg_lisp.lock" root))
                                (:pid (merge-pathnames "postmaster.pid" root))))
                        (saved (pathname (concatenate 'string
                                                      (string-right-trim "/" (namestring path))
                                                      (if (eq role :root) ".saved/" ".saved")))))
                   (rename-file path saved)
                   (unwind-protect
                        (progn
                          (if (eq role :root)
                              (progn
                                (ensure-directories-exist (merge-pathnames "unused" path))
                                (sb-posix:chmod (namestring path) #o700))
                              (with-open-file (s path :direction :output :if-exists :error)
                                (write-line (format nil "~d" (sb-posix:getpid)) s)))
                          (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership owner)))))
                     (if (eq role :root)
                         (uiop:delete-directory-tree path :validate t)
                         (delete-file path))
                     (rename-file saved path))
                   (ok (not (pg::postmaster-ownership-active-p owner)))
                   (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership owner))))))))))
       (ok (not (pg::postmaster-ownership-active-p escaped)))
       (ok (pg::postmaster-pid-owned-p))
       (ok (open-stream-p pg::*postmaster-data-lock*))
       (ok (pg::call-with-postmaster-ownership #'identity))))))

(deftest postmaster-ownership-rejects-replaced-root-lock-and-pid
    (dolist (role '(:root :lock :pid)) (check-postmaster-proof-path-replacement role)))

(deftest postmaster-ownership-preserves-backend-interruptions
    (dolist (delivery '(:synchronous :asynchronous))
      (dolist (kind '(:termination :cancel :timeout :wal-failure))
        (call-with-postmaster-proof-fixture
         (lambda (root)
           (declare (ignore root))
           (pg::call-with-postmaster-ownership
            (lambda (owner)
              (owner-lock-test-interruption
               (lambda () (pg::postmaster-check-ownership owner))
               (lambda () (pg::postmaster-check-ownership owner))
               (sb-sys:fd-stream-fd pg::*postmaster-data-lock*) delivery kind))))))))

(deftest postmaster-ownership-rejects-changed-pid-and-permissions
    (dolist (role '(:pid :root-mode :lock-mode))
      (call-with-postmaster-proof-fixture
       (lambda (root)
         (let ((entered nil))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-postmaster-ownership
                   (lambda (owner)
                     (setf entered t)
                     (let* ((path (if (eq role :root-mode) root
                                      (merge-pathnames
                                       (if (eq role :pid) "postmaster.pid" ".pg_lisp.lock") root)))
                            (saved (if (eq role :pid)
                                       (with-open-file (s path) (read-line s))
                                       (logand #o777 (sb-posix:stat-mode
                                                      (sb-posix:lstat (namestring path)))))))
                       (unwind-protect
                            (progn
                              (if (eq role :pid)
                                  (progn
                                    (file-position pg::*postmaster-pid-stream* 0)
                                    (write-char #\0 pg::*postmaster-pid-stream*)
                                    (finish-output pg::*postmaster-pid-stream*))
                                  (sb-posix:chmod (namestring path) #o755))
                              (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership owner)))))
                         (if (eq role :pid)
                             (progn
                               (file-position pg::*postmaster-pid-stream* 0)
                               (write-string saved pg::*postmaster-pid-stream*)
                               (finish-output pg::*postmaster-pid-stream*))
                             (sb-posix:chmod (namestring path) saved)))
                       (ok (not (pg::postmaster-ownership-active-p owner)))))))))
           (ok entered))))))

(defun check-primary-entry-refusal-before-writes (expected &key writes-reached)
  (let ((original (symbol-function 'pg::wal-mark-primary-startup)) (called nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::wal-mark-primary-startup)
                 (lambda () (setf called t) (error "test stopped a startup write")))
           (let ((message (handler-case (progn (pg::initialize-primary-storage) nil)
                            (error (condition) (princ-to-string condition)))))
             (ok (and message (search expected message)) "the intended startup guard refused entry"))
           (ok (eq writes-reached called) "check whether the first lifecycle mutation was reached"))
      (setf (symbol-function 'pg::wal-mark-primary-startup) original))))

(deftest primary-storage-startup-requires-real-postmaster-before-writes
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture root (lambda () (check-primary-entry-refusal-before-writes "published lock and PID streams")))))

(deftest primary-storage-startup-rejects-idle-readers-after-operation-drain
    (call-with-postmaster-proof-fixture
     (lambda (root)
       (declare (ignore root))
       (check-primary-entry-refusal-before-writes "test stopped a startup write" :writes-reached t)
       (let ((registration (pg::make-snapshot-registration)))
         (unwind-protect
              (progn
                (sb-thread:with-mutex (pg::*xid-lock*)
                  (pg::register-active-snapshot
                   (pg::make-snapshot-struct :xmin 3 :xmax 3 :curcid 0) registration))
                (check-primary-entry-refusal-before-writes "no retained readers or idle transactions"))
           (pg::release-snapshot-registration registration)))
       (dolist (state '(:in-progress :aborted))
         (let ((backend (pg::make-backend :pid 300001 :state state)))
           (unwind-protect
                (progn
                  (pg::register-backend backend)
                  (check-primary-entry-refusal-before-writes "no retained readers or idle transactions"))
             (pg::unregister-backend backend))))
       (check-primary-entry-refusal-before-writes "test stopped a startup write" :writes-reached t))))

(deftest primary-storage-startup-rejects-writer-and-storage-debt-before-writes
    (call-with-postmaster-proof-fixture
     (lambda (root)
       (declare (ignore root))
       (check-primary-entry-refusal-before-writes "test stopped a startup write" :writes-reached t)
       (let ((pg::*primary-wal-writer* (pg::%make-primary-wal-writer 1 nil nil nil)))
         (check-primary-entry-refusal-before-writes "stopped and joined primary WAL writer"))
       (let ((pg::*wal-pending-page-publication* (pg::%make-wal-page-publication)))
         (check-primary-entry-refusal-before-writes "pending page publication"))
       (let ((pg::*clog-pending-commits* (make-hash-table :test #'eql)))
         (setf (gethash 3 pg::*clog-pending-commits*)
               (pg::%make-clog-commit-completion nil nil :reserved))
         (check-primary-entry-refusal-before-writes "pending CLOG commits"))
       (let ((pg::*wal-temp-cleanup-domain* nil) (spool nil))
         (sb-thread:with-mutex (pg::*wal-lock*) (setf spool (pg::make-wal-owned-spool)))
         (unwind-protect
              (check-primary-entry-refusal-before-writes "temporary cleanup prevents WAL storage replacement")
           (sb-thread:with-mutex (pg::*wal-lock*) (pg::wal-close-owned-spool spool))))
       (check-primary-entry-refusal-before-writes "test stopped a startup write" :writes-reached t))))

(deftest postmaster-ownership-copies-cannot-escape-the-exact-scope
    (call-with-postmaster-proof-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((copies nil))
         (pg::call-with-postmaster-ownership
          (lambda (owner)
            (setf copies (list (pg::copy-postmaster-ownership owner) (copy-structure owner)))
            (dolist (copy copies)
              (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership copy)))))
            (pg::call-with-postmaster-ownership
             (lambda (inner)
               (ok (eq inner (pg::postmaster-check-ownership inner)))
               (ok (eq owner (pg::postmaster-check-ownership owner)))))))
         (dolist (copy copies)
           (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership copy)))))))))

(deftest postmaster-ownership-rechecks-identities-after-flock-inspection
    (dolist (role '(:lock :pid :root-mode))
      (call-with-postmaster-proof-fixture
       (lambda (root)
         (let ((original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
               (lock pg::*postmaster-data-lock*) (pid pg::*postmaster-pid-stream*) (calls 0))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-postmaster-ownership
                   (lambda (owner)
                     (unwind-protect
                          (progn
                            (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                                  (lambda (fd)
                                    (incf calls)
                                    (prog1 (funcall original fd)
                                      (ecase role
                                        (:lock (setf pg::*postmaster-data-lock* nil))
                                        (:pid (setf pg::*postmaster-pid-stream* nil))
                                        (:root-mode (sb-posix:chmod (namestring root) #o755))))))
                            (ok (intake-fails-p (lambda () (pg::postmaster-check-ownership owner))))
                            (ok (not (pg::postmaster-ownership-active-p owner))))
                       (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original
                             pg::*postmaster-data-lock* lock pg::*postmaster-pid-stream* pid)
                       (sb-posix:chmod (namestring root) #o700)))))))
           (ok (= 1 calls))
           (ok (open-stream-p lock)) (ok (pg::postmaster-pid-owned-p)))))))

(deftest postmaster-pid-parsing-preserves-interrupt-condition-identity
    (dolist (kind '(:termination :cancel :timeout :wal-failure))
      (call-with-postmaster-proof-fixture
       (lambda (root)
         (declare (ignore root))
         (pg::call-with-postmaster-ownership
          (lambda (owner)
            (let* ((pg::*backend* (pg::make-backend))
                   (original (symbol-function 'pg::postmaster-parse-pid))
                   (line (format nil "~d" (sb-posix:getpid)))
                   (sentinel (if (eq kind :wal-failure)
                                 (make-condition 'simple-error :format-control "latched test WAL failure")
                                 (make-condition 'pg::pg-error
                                                 :sqlstate (if (eq kind :termination) "57P01" "57014")
                                                 :severity (if (eq kind :termination) "FATAL" "ERROR")
                                                 :message "test interruption in PID parsing")))
                   (caught nil) (calls 0))
              (when (eq kind :wal-failure)
                (setf (pg::backend-wal-failure pg::*backend*) sentinel))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'pg::postmaster-parse-pid)
                           (lambda (input)
                             (when (equal line input) (incf calls) (error sentinel))
                             (funcall original input)))
                     (handler-case (pg::postmaster-check-ownership owner)
                       (error (condition) (setf caught condition))))
                (setf (symbol-function 'pg::postmaster-parse-pid) original))
              (ok (= 1 calls))
              (ok (eq sentinel caught))
              (ok (pg::postmaster-ownership-active-p owner))
              (ok (handler-case (eq owner (pg::postmaster-check-ownership owner))
                    (error () nil))))))))))
