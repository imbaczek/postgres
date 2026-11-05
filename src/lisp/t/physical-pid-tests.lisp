;;;; Local data ownership precedes all standby/primary storage initialization.
(in-package #:pg-physical-tests)

(defun call-with-pid-owner (function)
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                              (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                              (pg::*postmaster-start-time* nil) (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                              (pg::*postmaster-pid-port* nil) (pg::*postmaster-pid-inode* nil)
                              (saved-exit-hooks sb-ext:*exit-hooks*))
      (sb-posix:chmod (namestring root) #o700)
      (unwind-protect
           (progn (pg::postmaster-own-data-directory "127.0.0.1" 5439)
                  (funcall function root))
        (pg::postmaster-remove-pid-file)
        (setf sb-ext:*exit-hooks* saved-exit-hooks)
        (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))))))

(deftest postmaster-sql-time-initializes-once-with-owned-data
    (let ((before (pg::postgres-current-timestamp)))
      (call-with-pid-owner
       (lambda (root)
         (declare (ignore root))
         (let ((captured (pg::postmaster-start-timestamp)))
           (ok (<= before captured (pg::postgres-current-timestamp)))
           (ok (>= (+ 946684800 (floor captured 1000000)) pg::*postmaster-start-time*))
           (pg::postmaster-update-pid-status "ready   ")
           (ok (= captured (pg::postmaster-start-timestamp))))))))

(deftest failed-pid-creation-clears-the-sql-startup-time
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                (pg::*postmaster-start-time* nil) (pg::*postmaster-start-timestamp* nil)
                                (pg::*postmaster-pid-host* nil) (pg::*postmaster-pid-port* nil)
                                (pg::*postmaster-pid-inode* nil)
                                (original (symbol-function 'pg::postmaster-create-pid-file))
                                (captured nil))
        (sb-posix:chmod (namestring root) #o700)
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::postmaster-create-pid-file)
                     (lambda ()
                       (setf captured pg::*postmaster-start-timestamp*)
                       (error "PID creation failpoint")))
               (ok (handler-case (progn (pg::postmaster-own-data-directory "127.0.0.1" 5439) nil)
                     (error () t)))
               (ok captured)
               (ok (null pg::*postmaster-start-timestamp*))
               (ok (equal "55000" (handler-case (progn (pg::postmaster-start-timestamp) nil)
                                    (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))
          (setf (symbol-function 'pg::postmaster-create-pid-file) original)))))

(deftest pid-metadata-eight-lines-and-stable-status-width
    (call-with-pid-owner
     (lambda (root)
       (let* ((path (merge-pathnames "postmaster.pid" root))
              (before (pg::recovery-read-bytes path))
              (fields (uiop:split-string (pg::bytes->string before) :separator '(#\Newline))))
         (dolist (stream (list pg::*postmaster-data-lock* pg::*postmaster-pid-stream*))
           (ok (logbitp 0 (sb-posix:fcntl (sb-sys:fd-stream-fd stream) sb-posix:f-getfd))))
         (ok (= 9 (length fields)))
         (ok (= (sb-posix:getpid) (pg::postmaster-existing-pid)))
         (ok (= (sb-posix:getpid) (parse-integer (first fields))))
         (ok (= 5439 (parse-integer (fourth fields))))
         (ok (equal "" (fifth fields)))
         (ok (equal "" (seventh fields)))
         (ok (equal "starting" (eighth fields)))
         (ok (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:stat path)))))
         (pg::postmaster-update-pid-status "ready   ")
         (ok (= (length before) (length (pg::recovery-read-bytes path))))
         (ok (pg::postmaster-pid-owned-p))
         (pg::postmaster-remove-pid-file)
         (ok (not (probe-file path)))
         (ok (probe-file (merge-pathnames ".pg_lisp.lock" root)))))))

(deftest pid-cleanup-never-unlinks-a-replacement-inode
    (call-with-pid-owner
     (lambda (root)
       (let ((path (merge-pathnames "postmaster.pid" root)))
         (pg::wal-atomic-write-bytes path (pg::string->bytes "replacement\n"))
         (ok (not (pg::postmaster-pid-owned-p)))
         (ok (intake-fails-p (lambda () (pg::postmaster-update-pid-status "ready   "))))
         (pg::postmaster-remove-pid-file)
         (ok (equalp (pg::string->bytes "replacement\n") (pg::recovery-read-bytes path)))))))

(deftest pid-stable-lock-rejects-a-second-open-in-the-same-process
    (call-with-pid-owner
     (lambda (root)
       (with-open-file (stream (merge-pathnames ".pg_lisp.lock" root)
                               :direction :io :if-exists :overwrite)
         (ok (not (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd stream) 6))))))))

(deftest pid-directory-permissions-are-not-silently-repaired
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (sb-posix:chmod (namestring root) #o755)
        (ok (intake-fails-p (lambda () (pg::postmaster-check-data-directory))))
        (ok (= #o755 (logand #o777 (sb-posix:stat-mode (sb-posix:stat root)))))
        (ok (not (probe-file (merge-pathnames ".pg_lisp.lock" root)))))))

(deftest pid-data-lock-never-follows-a-symlink
    (with-physical-store-directory (root)
      (let* ((pg::*postmaster-pid-directory* root)
             (outside (merge-pathnames "other-file" root))
             (lock (merge-pathnames ".pg_lisp.lock" root)))
        (pg::wal-atomic-write-bytes outside #(1 2 3))
        (sb-posix:chmod (namestring outside) #o644)
        (sb-posix:symlink (namestring outside) (namestring lock))
        (ok (intake-fails-p (lambda () (pg::postmaster-open-data-lock))))
        (ok (= #o644 (logand #o777 (sb-posix:stat-mode (sb-posix:stat outside)))))
        (ok (equalp #(1 2 3) (pg::recovery-read-bytes outside))))))

(deftest startup-check-refusal-retires-the-owned-pid-and-releases-the-lock
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                (pg::*postmaster-start-time* nil) (pg::*postmaster-start-timestamp* nil)
                                (pg::*postmaster-pid-host* nil) (pg::*postmaster-pid-port* nil)
                                (pg::*postmaster-pid-inode* nil)
                                (saved-exit-hooks sb-ext:*exit-hooks*))
        (sb-posix:chmod (namestring root) #o700)
        (unwind-protect
             (dolist (mode '(:error :nonlocal-exit))
               (let ((entered nil))
                 (ok (eq :refused
                         (catch 'startup-refused
                           (handler-case
                               (progn
                                 (pg::postmaster-own-data-directory
                                  "127.0.0.1" 5439
                                  :startup-check
                                  (lambda ()
                                    (setf entered t)
                                    (ok (pg::postmaster-pid-owned-p))
                                    (ok (null pg::*postmaster-data-lock*))
                                    (if (eq mode :error)
                                        (error "startup refusal")
                                        (throw 'startup-refused :refused))))
                                 :accepted)
                             (error () :refused)))))
                 (ok entered)
                 (ok (null pg::*postmaster-start-timestamp*))
                 (ok (null pg::*postmaster-data-lock*))
                 (ok (null pg::*postmaster-pid-stream*))
                 (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
                 (multiple-value-bind (lock size)
                     (pg::open-bounded-regular-file (merge-pathnames ".pg_lisp.lock" root) 0)
                   (declare (ignore size))
                   (unwind-protect
                        (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd lock) 6)))
                     (close lock)))))
          (setf sb-ext:*exit-hooks* saved-exit-hooks)))))

(deftest startup-observer-receives-the-local-lock-at-both-pid-boundaries
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil
             pg::*postmaster-start-timestamp* nil)
       (let ((phases nil) (held-lock nil))
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (push phase phases)
            (ok (open-stream-p lock))
            (let ((other (pg::postmaster-open-data-lock)))
              (unwind-protect
                   (ok (not (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd other) 6))))
                (close other)))
            (ok (null pg::*postmaster-data-lock*))
            (if (eq phase :before-pid)
                (progn
                  (setf held-lock lock)
                  (ok (null pg::*postmaster-start-timestamp*))
                  (ok (null pg::*postmaster-pid-stream*))
                  (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))
                (progn
                  (ok (eq phase :owned-pid))
                  (ok (eq lock held-lock))
                  (ok (integerp pg::*postmaster-start-timestamp*))
                  (ok (pg::postmaster-pid-owned-p)))))
          :startup-reconcile
          (lambda ()
            (push :reconcile phases)
            (ok (eq held-lock pg::*postmaster-data-lock*))
            (ok (pg::postmaster-pid-owned-p))))
         (ok (equal (reverse phases) '(:before-pid :owned-pid :reconcile)))
         (ok (eq held-lock pg::*postmaster-data-lock*))))))

(deftest startup-observer-refusal-closes-the-local-lock-and-owned-pid
    (dolist (phase '(:before-pid :owned-pid))
      (dolist (mode '(:error :throw :interrupt :close))
        (call-with-pid-owner
         (lambda (root)
           (pg::postmaster-remove-pid-file)
           (close pg::*postmaster-data-lock*)
           (setf pg::*postmaster-data-lock* nil
                 pg::*postmaster-start-timestamp* nil)
           (let ((observed-lock nil))
             (ok
              (eq :refused
                  (catch 'observer-refused
                    (handler-case
                        (progn
                          (pg::postmaster-own-data-directory
                           "127.0.0.1" 5439
                           :startup-observer
                           (lambda (lock current-phase)
                             (setf observed-lock lock)
                             (when (eq phase current-phase)
                               (ecase mode
                                 (:close (close lock))
                                 (:error (error "observer refusal"))
                                 (:throw (throw 'observer-refused :refused))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'observer-refused :refused))))))))
                          :accepted)
                      (error () :refused)))))
             (ok observed-lock)
             (ok (not (open-stream-p observed-lock)))
             (ok (null pg::*postmaster-data-lock*))
             (ok (null pg::*postmaster-pid-stream*))
             (ok (null pg::*postmaster-start-timestamp*))
             (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
             (let ((lock (pg::postmaster-open-data-lock)))
               (unwind-protect
                    (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd lock) 6)))
                 (close lock)))))))))

(deftest startup-observer-rejects-mixed-callbacks-before-changing-pid-state
    (call-with-pid-owner
     (lambda (root)
       (let ((stream pg::*postmaster-pid-stream*)
             (pg::*postmaster-data-lock* nil)
             (timestamp pg::*postmaster-start-timestamp*)
             (bytes (uiop:read-file-string (merge-pathnames "postmaster.pid" root))))
         (dolist (callbacks '((:startup-before-pid) (:startup-check)
                              (:startup-before-pid :startup-check)))
           (let ((entered nil))
             (ok
              (handler-case
                  (progn
                    (apply #'pg::postmaster-own-data-directory "127.0.0.1" 5439
                           :startup-observer (lambda (&rest args)
                                               (declare (ignore args))
                                               (setf entered t))
                           (loop for key in callbacks
                                 append (list key (lambda () (setf entered t)))))
                    nil)
                (error (condition)
                  (search "startup observer cannot be combined" (princ-to-string condition)))))
             (ok (not entered))
             (ok (eq stream pg::*postmaster-pid-stream*))
             (ok (null pg::*postmaster-data-lock*))
             (ok (= timestamp pg::*postmaster-start-timestamp*))
             (ok (equalp bytes (uiop:read-file-string (merge-pathnames "postmaster.pid" root))))))))))

(deftest startup-observer-before-pid-refusal-preserves-stale-metadata
    (dolist (mode '(:error :throw :interrupt))
      (call-with-pid-owner
       (lambda (root)
         (let* ((path (merge-pathnames "postmaster.pid" root))
                (text (uiop:read-file-string path))
                (inode (sb-posix:stat-ino (sb-posix:stat path))))
           (close pg::*postmaster-pid-stream*)
           (close pg::*postmaster-data-lock*)
           (setf pg::*postmaster-pid-stream* nil
                 pg::*postmaster-pid-inode* nil
                 pg::*postmaster-data-lock* nil
                 pg::*postmaster-start-timestamp* nil)
           (let ((entered nil))
             (ok
              (eq :refused
                  (catch 'stale-pid-refused
                    (handler-case
                        (progn
                          (pg::postmaster-own-data-directory
                           "127.0.0.1" 5439
                           :startup-observer
                           (lambda (lock phase)
                             (declare (ignore lock))
                             (setf entered t)
                             (ok (eq phase :before-pid))
                             (ecase mode
                               (:error (error "stale PID refusal"))
                               (:throw (throw 'stale-pid-refused :refused))
                               (:interrupt
                                (sb-thread:interrupt-thread
                                 sb-thread:*current-thread*
                                 (lambda () (throw 'stale-pid-refused :refused)))))))
                          :accepted)
                      (error (condition)
                        (if (search "stale PID refusal" (princ-to-string condition))
                            :refused
                            (error condition)))))))
             (ok entered)
             (ok (string= text (uiop:read-file-string path)))
             (ok (= inode (sb-posix:stat-ino (sb-posix:stat path))))
             (ok (null pg::*postmaster-pid-stream*))
             (ok (null pg::*postmaster-data-lock*))
             (ok (null pg::*postmaster-start-timestamp*))))))))

(deftest adoption-attempt-presence-fences-every-file-kind-without-reading-it
    (dolist (mode '(:empty :corrupt :record :directory :dangling-symlink))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (path (merge-pathnames "physical-adoption.attempt" root)))
          (ok (null (pg::physical-adoption-check-startup)))
          (ecase mode
            (:empty (pg::wal-atomic-write-bytes path (empty-octets)))
            (:corrupt (pg::wal-atomic-write-bytes path (make-array 5 :element-type '(unsigned-byte 8)
                                                                     :initial-element 255)))
            (:record (pg::wal-atomic-write-bytes path (pg::string->bytes "PGADOPTION1 unknown")))
            (:directory (ensure-directories-exist (merge-pathnames "physical-adoption.attempt/child" root)))
            (:dangling-symlink (sb-posix:symlink "missing-attempt" (namestring path))))
          (let ((inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
            (ok (intake-fails-p #'pg::physical-adoption-check-startup))
            (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))))))))

(deftest adoption-attempt-inspection-errors-do-not-count-as-absence
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "not-a-directory" root)))
        (pg::wal-atomic-write-bytes path (empty-octets))
        (let ((pg::*pgdata* (uiop:ensure-directory-pathname path)))
          (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))
