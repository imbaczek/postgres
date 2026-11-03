(in-package #:pg-physical-tests)

(deftest reseed-pre-pid-check-runs-with-the-real-lock-before-pid-mutation
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (let ((events nil))
           (pg::postmaster-own-data-directory
            "127.0.0.1" 5439
            :startup-before-pid
            (lambda ()
              (push :before-pid events)
              (ok (null pg::*postmaster-data-lock*))
              (ok (null pg::*postmaster-pid-stream*))
              (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
              (multiple-value-bind (lock size)
                  (pg::open-bounded-regular-file (merge-pathnames ".pg_lisp.lock" root) 0)
                (declare (ignore size))
                (unwind-protect
                     (ok (not (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd lock) 6))))
                  (close lock))))
            :startup-check
            (lambda ()
              (push :check events)
              (ok (pg::postmaster-pid-owned-p))
              (ok (null pg::*postmaster-data-lock*)))
            :startup-reconcile
            (lambda ()
              (push :reconcile events)
              (ok (pg::postmaster-pid-owned-p))
              (ok (open-stream-p pg::*postmaster-data-lock*))))
           (ok (equal '(:reconcile :check :before-pid) events)))))))

(deftest reseed-pre-pid-refusal-preserves-existing-pid-bytes-and-inode
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (let ((path (merge-pathnames "postmaster.pid" root)))
           (with-open-file (stream path :direction :output :if-exists :error)
             (write-line "unreadable old PID metadata must be preserved" stream))
           (let ((before (lineage-test-bytes path))
                 (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
             (dolist (mode '(:error :nonlocal-exit))
               (let ((entered nil))
                 (ok (eq :refused
                         (catch 'reseed-pre-pid-refused
                           (handler-case
                               (progn
                                 (pg::postmaster-own-data-directory
                                  "127.0.0.1" 5439 :startup-before-pid
                                  (lambda ()
                                    (setf entered t)
                                    (if (eq mode :error)
                                        (error "unfinished reseed")
                                        (throw 'reseed-pre-pid-refused :refused))))
                                 :accepted)
                             (error () :refused)))))
                 (ok entered)
                 (ok (equalp before (lineage-test-bytes path)))
                 (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
                 (ok (null pg::*postmaster-pid-stream*))
                 (ok (null pg::*postmaster-data-lock*))
                 (ok (null pg::*postmaster-start-timestamp*))))))))))

(deftest reseed-fence-refuses-startup-before-existing-pid-is-inspected
    (dolist (kind '(:file :symlink :directory :fifo))
      (with-physical-store-directory (root)
        (call-with-adoption-postmaster-fixture
         root
         (lambda ()
           (let ((pid (merge-pathnames "postmaster.pid" root))
                 (fence (pg::physical-reseed-attempt-path root)))
             (with-open-file (stream pid :direction :output :if-exists :error)
               (write-line "opaque old PID metadata" stream))
             (ecase kind
               (:file
                (with-open-file (stream fence :direction :output :if-exists :error)
                  (write-string "untrusted or incomplete reseed intent" stream)))
               (:symlink (sb-posix:symlink "missing" (namestring fence)))
               (:directory (sb-posix:mkdir (namestring fence) #o700))
               (:fifo (sb-posix:mkfifo (namestring fence) #o600)))
             (let* ((before (lineage-test-bytes pid))
                    (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))
                    (message (handler-case
                                 (progn
                                   (pg::postmaster-own-data-directory
                                    "127.0.0.1" 5439
                                    :startup-before-pid #'pg::physical-reseed-check-before-pid)
                                   "startup unexpectedly accepted")
                               (error (condition) (princ-to-string condition)))))
               (ok (search "pending offline reseed" message) (format nil "~s" kind))
               (ok (equalp before (lineage-test-bytes pid)))
               (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
               (ok (null pg::*postmaster-data-lock*))
               (ok (null pg::*postmaster-pid-stream*)))))))))

(defun call-with-reseed-owner-directory (function)
  (with-physical-store-directory (root)
    (sb-posix:chmod (namestring root) #o700)
    (with-open-file (stream (merge-pathnames ".pg_lisp.lock" root)
                            :direction :output :if-exists :error)
      (declare (ignore stream)))
    (sb-posix:chmod (namestring (merge-pathnames ".pg_lisp.lock" root)) #o600)
    (funcall function root)))

(defun call-with-reaped-reseed-pid (function)
  (let ((process (sb-ext:run-program "/usr/bin/true" nil :wait t)))
    (unwind-protect
         (funcall function (sb-ext:process-pid process))
      (sb-ext:process-close process))))

(defun reseed-test-write-pid (root pid)
  (let ((path (merge-pathnames "postmaster.pid" root)))
    (with-open-file (stream path :direction :output :if-exists :error)
      (format stream "~d~%preserve all old PID bytes~%" pid))
    (sb-posix:chmod (namestring path) #o600)
    path))

(deftest reseed-process-state-is-conservative-without-startup-pid-exemptions
    (ok (eq :alive (pg::physical-reseed-process-state (sb-posix:getpid))))
  (ok (member (pg::physical-reseed-process-state (sb-posix:getppid)) '(:alive :unknown)))
  (call-with-reaped-reseed-pid
   (lambda (pid) (ok (eq :dead (pg::physical-reseed-process-state pid)))))
  (dolist (pid '(0 -1 2147483648))
    (let ((called nil))
      (ok (intake-fails-p
           (lambda ()
             (pg::physical-reseed-process-state
              pid (lambda (actual signal) (declare (ignore actual signal)) (setf called t))))))
      (ok (not called)))))

(deftest reseed-owner-preserves-a-dead-pid-and-revokes-only-its-own-capability
    (call-with-reaped-reseed-pid
     (lambda (pid)
       (call-with-reseed-owner-directory
        (lambda (root)
          (let* ((path (reseed-test-write-pid root (- pid)))
                 (before (lineage-test-bytes path))
                 (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
                 (captured nil))
            (ok (intake-fails-p
                 (lambda () (pg::call-with-physical-adoption-owner root #'identity))))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (setf captured owner)
               (ok (pg::physical-reseed-owner-p owner))
               (ok (eq owner (pg::physical-reseed-check-owner owner)))
               (ok (equalp before (lineage-test-bytes path)))
               (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))))
            (ok (not (pg::physical-adoption-owner-active-p captured)))
            (ok (not (open-stream-p (pg::physical-adoption-owner-lock captured))))
            (ok (equalp before (lineage-test-bytes path)))))))))

(deftest reseed-owner-rejects-live-and-malformed-pid-files
    (dolist (value (list (format nil "~d~%" (sb-posix:getpid)) "0" "invalid" "2147483648"))
      (call-with-reseed-owner-directory
       (lambda (root)
         (let ((path (merge-pathnames "postmaster.pid" root)))
           (with-open-file (stream path :direction :output :if-exists :error)
             (write-string value stream))
           (let ((before (lineage-test-bytes path)))
             (ok (intake-fails-p
                  (lambda () (pg::call-with-physical-reseed-owner root #'identity))))
             (ok (equalp before (lineage-test-bytes path)))))))))

(deftest reseed-owner-rechecks-pid-inode-and-complete-bytes
    (call-with-reaped-reseed-pid
     (lambda (pid)
       (call-with-reseed-owner-directory
        (lambda (root)
          (let ((path (reseed-test-write-pid root pid))
                (saved (merge-pathnames "postmaster.saved" root)))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (rename-file path saved)
               (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
               (reseed-test-write-pid root pid)
               (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
               (delete-file path)
               (rename-file saved path)
               (ok (eq owner (pg::physical-reseed-check-owner owner)))
               (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                            :if-exists :overwrite)
                 (file-position stream (1- (file-length stream)))
                 (write-byte (char-code #\Space) stream))
               (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))))))))))

(deftest reseed-owner-rejects-a-pid-created-after-acquisition
    (call-with-reaped-reseed-pid
     (lambda (pid)
       (call-with-reseed-owner-directory
        (lambda (root)
          (pg::call-with-physical-reseed-owner
           root
           (lambda (owner)
             (ok (null (pg::physical-reseed-owner-old-pid owner)))
             (reseed-test-write-pid root pid)
             (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner)))))))))))

(deftest reseed-owner-does-not-authorize-clean-adoption
    (call-with-reseed-owner-directory
     (lambda (root)
       (pg::call-with-physical-reseed-owner
        root
        (lambda (owner)
          (ok (eq owner (pg::physical-reseed-check-owner owner)))
          (dolist (operation (list (lambda () (pg::physical-adoption-check-owner owner))
                                   (lambda () (pg::physical-adoption-attempt-read owner))
                                   (lambda () (pg::physical-adoption-attempt-check-owner owner nil))
                                   (lambda () (pg::physical-adoption-inspect-primary owner))
                                   (lambda () (pg::physical-adoption-install-resume owner))
                                   (lambda () (pg::physical-adoption-install-prepare owner nil))
                                   (lambda () (pg::physical-adoption-attempt-publish
                                               owner (adoption-attempt-codec-fixture)))
                                   (lambda () (pg::physical-adoption-attempt-publish
                                               owner (adoption-attempt-codec-fixture)
                                               :previous (adoption-attempt-codec-fixture)
                                               :install-state t))))
            (let ((message (handler-case (progn (funcall operation) nil)
                             (error (condition) (princ-to-string condition)))))
              (ok (and message (search "does not authorize clean adoption" message)))))
          (ok (not (probe-file (pg::physical-adoption-attempt-path root))))
          (ok (not (probe-file (pg::physical-adoption-install-path root)))))))))

(deftest reseed-pid-reader-rejects-a-different-open-descriptor
    (call-with-reaped-reseed-pid
     (lambda (pid)
       (call-with-reseed-owner-directory
        (lambda (root)
          (let* ((path (reseed-test-write-pid root pid))
                 (other (merge-pathnames "other.pid" root))
                 (before (lineage-test-bytes path))
                 (original (symbol-function 'pg::open-bounded-regular-file))
                 (captured nil))
            (with-open-file (stream other :direction :output :if-exists :error)
              (format stream "~d~%preserve all old PID bytes~%" pid))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::open-bounded-regular-file)
                         (lambda (actual maximum &optional writable)
                           (multiple-value-bind (stream size)
                               (funcall original (if (equal actual path) other actual)
                                        maximum writable)
                             (when (equal actual path) (setf captured stream))
                             (values stream size))))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-read-pid root)))))
              (setf (symbol-function 'pg::open-bounded-regular-file) original))
            (ok captured)
            (ok (not (open-stream-p captured)))
            (ok (equalp before (lineage-test-bytes path)))))))))

(deftest reseed-owner-rejects-special-linked-and-oversized-pid-files
    (dolist (kind '(:symlink :hardlink :fifo :oversized))
      (call-with-reseed-owner-directory
       (lambda (root)
         (let ((path (merge-pathnames "postmaster.pid" root))
               (kept (merge-pathnames "kept" root)))
           (with-open-file (stream kept :direction :output :if-exists :error)
             (write-string "preserve this file" stream))
           (ecase kind
             (:symlink (sb-posix:symlink "kept" (namestring path)))
             (:hardlink (sb-posix:link (namestring kept) (namestring path)))
             (:fifo (sb-posix:mkfifo (namestring path) #o600))
             (:oversized
              (with-open-file (stream path :direction :output :if-exists :error)
                (write-string (make-string 16385 :initial-element #\x) stream))))
           (let ((before (lineage-test-bytes kept)))
             (ok (intake-fails-p
                  (lambda () (pg::call-with-physical-reseed-owner root #'identity)))
                 (format nil "~s" kind))
             (ok (equalp before (lineage-test-bytes kept)))))))))

(cl-spec:defproperty reseed-pid-probe-only-esrch-proves-death
    ((pid (range integer 1 2147483647)) (event (range integer 0 2)))
  (:about pg::physical-reseed-process-state)
  (:trials (:normal 100))
  (let ((expected (nth event '(:alive :dead :unknown))))
    (eq expected
        (pg::physical-reseed-process-state
         pid
         (lambda (actual signal)
           (assert (= actual pid))
           (assert (zerop signal))
           (case event
             (0 0)
             (1 (error 'sb-posix:syscall-error :name "kill" :errno sb-posix:esrch))
             (2 (error 'sb-posix:syscall-error :name "kill" :errno sb-posix:eperm))))))))

(deftest generated-reseed-pid-probe-failures
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-pid-probe-only-esrch-proves-death
                                         :seed 88 :profile :normal))))
      (ok (eq :passed (getf result :status)))))
