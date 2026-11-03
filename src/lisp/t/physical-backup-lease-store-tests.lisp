(in-package #:pg-physical-tests)

(defun call-with-backup-lease-store-fixture (function)
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root)
          (pg::*catalog* (pg::make-catalog :cluster-identity "backup-lease-codec"))
          (pg::*backend* (pg::make-backend :pid 998 :thread sb-thread:*current-thread*))
          (pg::*write-lock* (sb-thread:make-mutex :name "backup lease write fixture"))
          (pg::*wal-lock* (sb-thread:make-mutex :name "backup lease WAL fixture"))
          (pg::*wal-temp-cleanup-domain* nil)
          (pg::*wal-storage-incarnation* (gensym "BACKUP-LEASE-"))
          (pg::*wal-poisoned* nil)
          (pg::*wal-insert-timeline* 2)
          (pg::*physical-recovery-state* nil)
          (pg::*physical-backup-lease-records* (make-hash-table :test #'equal))
          (pg::*physical-backup-lease-capture-owners* (make-hash-table :test #'equal))
          (pg::*physical-backup-lease-limit* 2)
          (pg::*physical-backup-lease-test-hook* nil)
          (pg::*physical-source-senders* (make-hash-table :test #'equal))
          (pg::*physical-source-pins* (make-hash-table :test #'equal))
          (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
          (pg::*pm-requested-mode* pg::+no-shutdown+))
      (funcall function root))))

(defun backup-lease-store-test-name (record)
  (getf (getf record :request) :name))

(deftest backup-lease-store-publishes-private-durable-copy-before-memory
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((record (backup-lease-test-record))
              (name (backup-lease-store-test-name record))
              (observed nil)
              (pg::*physical-backup-lease-test-hook*
                (lambda (point value)
                  (when (eq point :before-memory)
                    (setf observed
                          (and (null (gethash name pg::*physical-backup-lease-records*))
                               (equal value (pg::physical-backup-lease-read
                                             (pg::physical-backup-lease-path (getf value :request)))))))))
              (returned (pg::physical-backup-lease-publish nil record))
              (path (pg::physical-backup-lease-path (getf record :request))))
         (ok observed)
         (ok (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:lstat (namestring path))))))
         (setf (char (getf (getf returned :request) :token) 0) #\b
               (char (getf (getf record :request) :token) 0) #\c)
         (ok (equal (make-string 64 :initial-element #\a)
                    (getf (getf (gethash name pg::*physical-backup-lease-records*) :request) :token)))
         (ok (equal (gethash name pg::*physical-backup-lease-records*)
                    (pg::physical-backup-lease-read path)))))))

(deftest backup-lease-store-cold-load-interrupts-capture-without-touching-pins
    (dolist (phase '(:requested :capturing))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (let* ((requested (backup-lease-test-record))
                (capturing (backup-lease-test-record :capturing))
                (pin (gensym "RETAINED-PIN-")))
           (pg::physical-backup-lease-publish nil requested)
           (when (eq phase :capturing)
             (pg::physical-backup-lease-publish requested capturing))
           (setf (gethash pin pg::*wal-snapshot-retention-pins*) 32)
           (pg::physical-backup-lease-load-records)
           (let ((actual (gethash (backup-lease-store-test-name requested) pg::*physical-backup-lease-records*)))
             (ok (eq :interrupted (getf actual :phase)))
             (ok (equal (getf requested :request) (getf actual :request)))
             (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok (not (probe-file (merge-pathnames "pg_wal/" root))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-backup-lease-publish actual capturing))))
             (ok (not pg::*wal-poisoned*))))))))

(deftest backup-lease-store-keeps-closed-tombstones-at-capacity
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((pg::*physical-backup-lease-limit* 1)
             (closing (backup-lease-test-record)))
         (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
         (pg::physical-backup-lease-publish nil closing)
         (let ((closed (copy-tree closing)))
           (setf (getf closed :phase) :closed)
           (pg::physical-backup-lease-publish closing closed)
           (pg::physical-backup-lease-load-records)
           (ok (equal closed (gethash (backup-lease-store-test-name closing)
                                      pg::*physical-backup-lease-records*)))
           (let ((other (backup-lease-test-record)))
             (setf (char (getf (getf other :request) :name) 7) #\f)
             (ok (intake-fails-p (lambda () (pg::physical-backup-lease-publish nil other)))))
           (ok (= 1 (hash-table-count pg::*physical-backup-lease-records*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-store-rejects-stale-caller-and-foreign-source-before-writing
    (dolist (mode '(:stale :timeline :identity))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((requested (backup-lease-test-record))
                (capturing (backup-lease-test-record :capturing)))
           (pg::physical-backup-lease-publish nil requested)
           (let* ((path (pg::physical-backup-lease-path (getf requested :request)))
                  (before (lineage-test-bytes path)))
             (ecase mode
               (:stale
                (ok (intake-fails-p (lambda () (pg::physical-backup-lease-publish nil requested)))))
               (:timeline
                (let ((pg::*wal-insert-timeline* 3))
                  (ok (intake-fails-p
                       (lambda () (pg::physical-backup-lease-publish requested capturing))))))
               (:identity
                (let ((pg::*catalog* (pg::make-catalog :cluster-identity "other-source")))
                  (ok (intake-fails-p
                       (lambda () (pg::physical-backup-lease-publish requested capturing)))))))
             (ok (equalp before (lineage-test-bytes path)))
             (ok (not pg::*wal-poisoned*))))))))

(deftest backup-lease-store-load-validates-all-records-before-registry-replacement
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((requested (backup-lease-test-record))
              (registry pg::*physical-backup-lease-records*)
              (other (backup-lease-test-record)))
         (pg::physical-backup-lease-publish nil requested)
         (setf (char (getf (getf other :request) :name) 7) #\f)
         (let ((path (pg::physical-backup-lease-path (getf other :request))))
           (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                        :if-exists :error)
             (write-sequence (pg::physical-backup-lease-image requested) stream))
           (sb-posix:chmod (namestring path) #o600))
         (ok (intake-fails-p #'pg::physical-backup-lease-load-records))
         (ok (eq registry pg::*physical-backup-lease-records*))
         (ok (eq :requested (getf (gethash (backup-lease-store-test-name requested) registry) :phase)))))))

(deftest backup-lease-store-never-adopts-an-unrelated-private-pin-name
    (dolist (kind '(:registered :disk-only))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((closing (backup-lease-test-record))
                (request (getf closing :request))
                (name (getf request :name))
                (pin-path (pg::physical-source-pin-path name)))
           (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
           (ecase kind
             (:registered (setf (gethash name pg::*physical-source-pins*) :unrelated))
             (:disk-only
              (ensure-directories-exist pin-path)
              (with-open-file (stream pin-path :direction :output :if-exists :error)
                (write-string "opaque unrelated pin" stream))))
           (ok (intake-fails-p (lambda () (pg::physical-backup-lease-publish nil closing))))
           (ok (not (probe-file (pg::physical-backup-lease-path request))))
           (ok (zerop (hash-table-count pg::*physical-backup-lease-records*)))
           (ok (not pg::*wal-poisoned*))
           (ecase kind
             (:registered (ok (eq :unrelated (gethash name pg::*physical-source-pins*))))
             (:disk-only (ok (equal "opaque unrelated pin" (uiop:read-file-string pin-path))))))))))

(deftest backup-lease-store-reader-rejects-unsafe-file-kinds-and-permissions
    (dolist (kind '(:symlink :hardlink :fifo :oversized :public :directory))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (let* ((record (backup-lease-test-record))
                (path (pg::physical-backup-lease-path (getf record :request)))
                (saved (merge-pathnames "saved-lease" root)))
           (pg::physical-backup-lease-publish nil record)
           (sb-posix:rename (namestring path) (namestring saved))
           (let ((before (lineage-test-bytes saved)))
             (ecase kind
               (:symlink (sb-posix:symlink (namestring saved) (namestring path)))
               (:hardlink (sb-posix:link (namestring saved) (namestring path)))
               (:fifo (sb-posix:mkfifo (namestring path) #o600))
               (:oversized
                (with-open-file (stream path :direction :output :if-exists :error)
                  (write-string (make-string 8193 :initial-element #\x) stream)))
               (:public
                (with-open-file (stream path :direction :output :if-exists :error
                                             :element-type '(unsigned-byte 8))
                  (write-sequence before stream))
                (sb-posix:chmod (namestring path) #o644))
               (:directory (sb-posix:mkdir (namestring path) #o700)))
             (ok (intake-fails-p (lambda () (pg::physical-backup-lease-read path)))
                 (format nil "~s" kind))
             (ok (equalp before (lineage-test-bytes saved)))))))))

(deftest backup-lease-store-reader-rejects-a-size-change-before-capture
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((record (backup-lease-test-record))
              (path (pg::physical-backup-lease-path (getf record :request)))
              (original (symbol-function 'pg::open-bounded-regular-file))
              (captured nil))
         (pg::physical-backup-lease-publish nil record)
         (with-open-file (stream path :direction :output :if-exists :append
                                      :element-type '(unsigned-byte 8))
           (write-byte (char-code #\Space) stream))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::open-bounded-regular-file)
                      (lambda (actual maximum &optional writable)
                        (multiple-value-bind (stream size) (funcall original actual maximum writable)
                          (if (equal actual path)
                              (progn (setf captured stream) (values stream (1- size)))
                              (values stream size)))))
                (ok (intake-fails-p (lambda () (pg::physical-backup-lease-read path)))))
           (setf (symbol-function 'pg::open-bounded-regular-file) original))
         (ok captured)
         (ok (not (open-stream-p captured)))))))

(deftest backup-lease-store-cold-load-cleans-only-private-bounded-temporaries
    (dolist (kind '(:partial :symlink))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (let* ((record (backup-lease-test-record))
                (request (getf record :request))
                (temporary (pg::physical-backup-lease-path request t))
                (kept (merge-pathnames "kept" root)))
           (pg::physical-backup-lease-publish nil record)
           (with-open-file (stream kept :direction :output :if-exists :error)
             (write-string "preserve" stream))
           (ecase kind
             (:partial
              (with-open-file (stream temporary :direction :output :if-exists :error)
                (write-string "partial CRC record" stream))
              (sb-posix:chmod (namestring temporary) #o600)
              (pg::physical-backup-lease-load-records)
              (ok (not (probe-file temporary)))
              (ok (eq :interrupted
                      (getf (gethash (backup-lease-store-test-name record)
                                     pg::*physical-backup-lease-records*) :phase))))
             (:symlink
              (sb-posix:symlink (namestring kept) (namestring temporary))
              (ok (intake-fails-p #'pg::physical-backup-lease-load-records))
              (ok (sb-posix:s-islnk (sb-posix:stat-mode (sb-posix:lstat (namestring temporary)))))
              (ok (eq :requested
                      (getf (gethash (backup-lease-store-test-name record)
                                     pg::*physical-backup-lease-records*) :phase)))))
           (ok (equal "preserve" (uiop:read-file-string kept))))))))

(deftest backup-lease-store-cleans-a-created-temp-when-initial-validation-fails
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((record (backup-lease-test-record))
              (temporary (pg::physical-backup-lease-path (getf record :request) t))
              (original (symbol-function 'pg::physical-backup-lease-file-identity))
              (seen nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-backup-lease-file-identity)
                      (lambda (stat &optional allow-empty)
                        (when (and allow-empty (zerop (sb-posix:stat-size stat)))
                          (setf seen t)
                          (error "reject newly created temporary metadata"))
                        (funcall original stat allow-empty)))
                (ok (intake-fails-p (lambda () (pg::physical-backup-lease-publish nil record)))))
           (setf (symbol-function 'pg::physical-backup-lease-file-identity) original))
         (ok seen)
         (ok (not (probe-file temporary)))
         (ok pg::*wal-poisoned*)
         (ok (zerop (hash-table-count pg::*physical-backup-lease-records*)))))))

(deftest backup-lease-store-error-and-nonlocal-cuts-preserve-complete-records
    (dolist (point '(:before-write :after-write :after-file-fsync :after-rename
                     :after-directory-fsync :before-memory))
      (dolist (mode '(:error :nonlocal))
        (call-with-backup-lease-store-fixture
         (lambda (root)
           (declare (ignore root))
           (let* ((requested (backup-lease-test-record))
                  (path (pg::physical-backup-lease-path (getf requested :request)))
                  (seen nil)
                  (pg::*physical-backup-lease-test-hook*
                    (lambda (actual value)
                      (declare (ignore value))
                      (when (eq point actual)
                        (setf seen t)
                        (if (eq mode :error) (error "backup lease publication cut")
                            (throw 'backup-lease-cut :cut))))))
             (ok (intake-fails-p
                  (lambda () (catch 'backup-lease-cut
                               (pg::physical-backup-lease-publish nil requested)))))
             (ok seen)
             (ok pg::*wal-poisoned*)
             (ok (zerop (hash-table-count pg::*physical-backup-lease-records*)))
             (when (probe-file path)
               (ok (equal requested (pg::physical-backup-lease-read path))))
             (let ((pg::*wal-poisoned* nil)
                   (pg::*physical-backup-lease-test-hook* nil))
               (pg::physical-backup-lease-load-records)
               (let ((actual (gethash (backup-lease-store-test-name requested)
                                      pg::*physical-backup-lease-records*)))
                 (ok (or (null actual) (eq :interrupted (getf actual :phase))))))))))))

(defun backup-lease-store-run-child (root log body cache)
  (let ((stat (sb-posix:lstat (string-right-trim "/" (namestring cache)))))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                 (zerop (logand #o077 (sb-posix:stat-mode stat))))
      (error "child verification cache requires an owned private directory")))
  (let* ((source (asdf:system-source-directory "pg"))
         (code (format nil
                       "(let ((pg::*pgdata* #P~s)
                              (pg::*catalog* (pg::make-catalog :cluster-identity \"backup-lease-codec\"))
                              (pg::*wal-insert-timeline* 2))
                          ~a)"
                       (namestring root) body))
         (process (sb-ext:run-program
                   "/usr/bin/env"
                   (list (format nil "XDG_CACHE_HOME=~a" (namestring cache))
                         (format nil "CL_SOURCE_REGISTRY=~a//:" (namestring source))
                         "sbcl" "--noinform" "--no-sysinit" "--no-userinit" "--non-interactive"
                         "--eval" "(require :asdf)" "--eval" "(asdf:load-system \"pg\")"
                         "--eval" code)
                   :wait t :output log :error :output :if-output-exists :supersede)))
    (unwind-protect
         (list (sb-ext:process-status process) (sb-ext:process-exit-code process))
      (sb-ext:process-close process))))

(deftest backup-lease-store-recovers-real-sigkill-and-reopens-in-a-fresh-image
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "child-cache.XXXXXX" cache-root))))))
        (dolist (point '(:before-write :after-write :after-file-fsync :after-rename
                         :after-directory-fsync :before-memory))
          (dolist (phase '(:requested :closing))
            (call-with-backup-lease-store-fixture
             (lambda (root)
               (let* ((record (backup-lease-test-record))
                      (log (merge-pathnames "lease-child.log" root)))
                 (when (eq phase :closing)
                   (setf (getf record :phase) :closing (getf record :outcome) :aborted))
                 (let ((result
                         (backup-lease-store-run-child
                          root log
                          (format nil
                                  "(setf pg::*physical-backup-lease-test-hook*
                                   (lambda (point value)
                                     (declare (ignore value))
                                     (when (eq point ~s) (sb-posix:kill (sb-posix:getpid) 9))))
                             (pg::physical-backup-lease-publish nil '~a)"
                                  point (pg::physical-bootstrap-manifest-text record)) cache)))
                   (ok (equal '(:signaled 9) result) (format nil "~s/~s; ~a" phase point log)))
                 (let* ((path (pg::physical-backup-lease-path (getf record :request)))
                        (present (probe-file path))
                        (expected (when present (if (eq phase :requested) :interrupted :closing)))
                        (result
                          (backup-lease-store-run-child
                           root log
                           (format nil
                                   "(pg::physical-backup-lease-load-records)
                              (let ((record (gethash ~s pg::*physical-backup-lease-records*)))
                                (assert (eq ~s (getf record :phase))))"
                                   (backup-lease-store-test-name record) expected) cache)))
                   (ok (equal '(:exited 0) result) (format nil "fresh reopen; ~a" log))
                   (pg::physical-backup-lease-load-records)
                   (let ((actual (gethash (backup-lease-store-test-name record)
                                          pg::*physical-backup-lease-records*)))
                     (ok (eq expected (getf actual :phase)))
                     (when actual
                       (ok (intake-fails-p
                            (lambda () (pg::physical-backup-lease-publish actual
                                                                          (backup-lease-test-record))))))))))))))))
