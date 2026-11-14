(in-package #:pg)

(defun cold-scratch-test-write (path bytes)
  (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                            :if-exists :supersede :if-does-not-exist :create)
    (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
    (write-sequence bytes out)))

(cl-spec:defproperty cold-scratch-cleanup-preserves-canonical-history
    ((aliases (range integer 1 4)) (salt (range integer 0 255)))
  (:about logical-slot-copy-cold-clean-wal-scratch)
  (:trials (:normal 50))
  (call-with-unit-promotion-directory
   (lambda (context root)
     (declare (ignore root))
     (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
            (history (timeline-history-path 2 *pgdata*))
            (bytes (timeline-encode-history 2 1 64 "scratch" nil))
            (parent (wal-segment-path 0 1 :timeline))
            (parent-bytes (make-array 16 :element-type '(unsigned-byte 8) :initial-element salt))
            (started nil) (checks 0))
       (timeline-publish-history-bytes history bytes :directory-fd directory)
       (cold-scratch-test-write parent parent-bytes)
       (let ((before (physical-image-file-facts directory (file-namestring history))))
         (dotimes (index aliases)
           (let ((scratch (merge-pathnames (format nil ".history-~6,'0d" index) (wal-directory))))
             (sb-posix:link (namestring history) (namestring scratch))))
         (cold-scratch-test-write (merge-pathnames ".timeline-ABC123" (wal-directory)) parent-bytes)
         (let* ((count (logical-slot-copy-cold-clean-wal-scratch
                        context (lambda () (incf checks)) (lambda () (setf started t)) 32))
                (after (physical-image-file-facts directory (file-namestring history)))
                (stable (copy-tree after)))
           (and (= count (1+ aliases)) started (> checks 1)
                (= 1 (fourth after))
                (= (second before) (second after))
                (equalp bytes (read-bounded-regular-file history 4096))
                (equalp parent-bytes (read-bounded-regular-file parent 4096))
                (equal (sort (list (file-namestring history) (file-namestring parent)) #'string<)
                       (sort (directory-entry-names (wal-directory) :follow-symlinks t) #'string<))
                (zerop (logical-slot-copy-cold-clean-wal-scratch
                        context (lambda () (incf checks)) (lambda () (setf started t)) 32))
                (equal stable (physical-image-file-facts directory (file-namestring history))))))))))

(rove:deftest cold-scratch-generated-history-and-prefix-cleanup
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'cold-scratch-cleanup-preserves-canonical-history :seed 77142))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest cold-scratch-rejects-unbound-or-unsafe-residue-before-mutation
    (dolist (kind '(:symlink :directory :fifo :mode :outside-link :timeline-alias :bad-name))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (name (if (eq kind :bad-name) ".timeline-bad" ".timeline-ABC123"))
                (scratch (merge-pathnames name (wal-directory)))
                (canonical (timeline-history-path 2 *pgdata*))
                (bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 73))
                (started nil))
           (ecase kind
             (:symlink (sb-posix:symlink (namestring (merge-pathnames "absent" root)) (namestring scratch)))
             (:directory (sb-posix:mkdir (namestring scratch) #o700))
             (:fifo (sb-posix:mkfifo (namestring scratch) #o600))
             ((:mode :outside-link :bad-name)
              (cold-scratch-test-write scratch bytes)
              (when (eq kind :mode) (sb-posix:chmod (namestring scratch) #o644))
              (when (eq kind :outside-link)
                (sb-posix:link (namestring scratch) (namestring (merge-pathnames "outside" root)))))
             (:timeline-alias
              (timeline-publish-history-bytes canonical bytes :directory-fd directory)
              (sb-posix:link (namestring canonical) (namestring scratch))))
           (let ((before (physical-image-file-facts directory name)))
             (rove:ok
              (handler-case
                  (progn (logical-slot-copy-cold-clean-wal-scratch
                          context (lambda () nil) (lambda () (setf started t)) 32) nil)
                (error () t)))
             (rove:ng started)
             (rove:ok (equal before (physical-image-file-facts directory name)))))))))

(rove:deftest cold-scratch-rejects-same-byte-inode-replacement
    (call-with-unit-promotion-directory
     (lambda (context root)
       (declare (ignore root))
       (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
              (scratch (merge-pathnames ".timeline-ABC123" (wal-directory)))
              (replacement (merge-pathnames "replacement" (wal-directory)))
              (bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 73))
              (checks 0) (started nil) (swapped nil))
         (cold-scratch-test-write scratch bytes)
         (cold-scratch-test-write replacement bytes)
         (let ((before (physical-image-file-facts directory (file-namestring scratch))))
           (rove:ok
            (handler-case
                (progn
                  (logical-slot-copy-cold-clean-wal-scratch
                   context
                   (lambda ()
                     (when (= 2 (incf checks))
                       (sb-posix:rename (namestring replacement) (namestring scratch))
                       (setf swapped t)))
                   (lambda () (setf started t)) 32)
                  nil)
              (error () t)))
           (rove:ok swapped)
           (rove:ng started)
           (rove:ok (/= (second before)
                        (second (physical-image-file-facts directory (file-namestring scratch)))))
           (rove:ok (equalp bytes (read-bounded-regular-file scratch 4096))))))))

(rove:deftest cold-scratch-nonlocal-exits-close-witness-and-allow-retry
    (dolist (cut '(:unlink-before-error :unlink-after-error :unlink-after-throw :unlink-after-interrupt
                   :sync-after-error :sync-after-throw :sync-after-interrupt))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (declare (ignore root))
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (history (timeline-history-path 2 *pgdata*))
                (bytes (timeline-encode-history 2 1 64 "cut" nil))
                (unlink (symbol-function 'port-unlinkat)) (sync (symbol-function 'sb-posix:fsync))
                (marker (make-condition 'simple-error :format-control "injected scratch cut"))
                (fired nil) (started nil))
           (timeline-publish-history-bytes history bytes :directory-fd directory)
           (dotimes (index 2)
             (sb-posix:link (namestring history)
                            (namestring (merge-pathnames (format nil ".history-~6,'0d" index)
                                                         (wal-directory)))))
           (cold-scratch-test-write (merge-pathnames ".timeline-ABC123" (wal-directory)) bytes)
           (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))
                 (inode (second (physical-image-file-facts directory (file-namestring history)))))
             (unwind-protect
                  (flet ((inject ()
                           (setf fired t)
                           (case cut
                             ((:unlink-before-error :unlink-after-error :sync-after-error) (error marker))
                             ((:unlink-after-throw :sync-after-throw) (throw 'scratch-cut :cut))
                             (otherwise
                              (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                          (lambda () (throw 'scratch-cut :cut)))))))
                    (setf (symbol-function 'port-unlinkat)
                          (lambda (parent name flags)
                            (when (and (= parent directory) (not fired) (eq cut :unlink-before-error))
                              (inject))
                            (prog1 (funcall unlink parent name flags)
                              (when (and (= parent directory) (not fired)
                                         (member cut '(:unlink-after-error :unlink-after-throw
                                                       :unlink-after-interrupt)))
                                (inject)))))
                    (setf (symbol-function 'sb-posix:fsync)
                          (lambda (fd)
                            (prog1 (funcall sync fd)
                              (when (and (= fd directory) (not fired)
                                         (member cut '(:sync-after-error :sync-after-throw :sync-after-interrupt)))
                                (inject)))))
                    (let ((outcome
                            (handler-case
                                (catch 'scratch-cut
                                  (logical-slot-copy-cold-clean-wal-scratch
                                   context (lambda () nil) (lambda () (setf started t)) 32)
                                  :returned)
                              (error (condition) condition))))
                      (rove:ok fired)
                      (rove:ok started)
                      (rove:ok (eq outcome (if (member cut '(:unlink-before-error :unlink-after-error
                                                             :sync-after-error)) marker :cut)))))
               (setf (symbol-function 'port-unlinkat) unlink
                     (symbol-function 'sb-posix:fsync) sync))
             (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (rove:ok (= inode (second (physical-image-file-facts directory (file-namestring history)))))
             (rove:ok (equalp bytes (read-bounded-regular-file history 4096)))
             (logical-slot-copy-cold-clean-wal-scratch context (lambda () nil) (lambda () nil) 32)
             (rove:ok (= 1 (fourth (physical-image-file-facts directory (file-namestring history)))))
             (rove:ok (equal (list (file-namestring history))
                             (directory-entry-names (wal-directory) :follow-symlinks t)))
             (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))))

(rove:deftest cold-scratch-bound-and-context-checks-precede-mutation
    (dolist (kind '(:bound :copied :expired :directory-swap))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 73))
                (scratch (merge-pathnames ".timeline-ABC123" (wal-directory)))
                (named (merge-pathnames "pg_wal" root)) (parked (merge-pathnames "parked" root))
                (checks 0) (swapped nil) (started nil))
           (cold-scratch-test-write scratch bytes)
           (when (eq kind :bound)
             (cold-scratch-test-write (merge-pathnames ".timeline-ABC124" (wal-directory)) bytes))
           (let ((actual (if (eq kind :copied) (copy-physical-image-context context) context)))
             (unwind-protect
                  (progn
                    (when (eq kind :expired) (setf (physical-image-context-active context) nil))
                    (rove:ok
                     (handler-case
                         (progn
                           (logical-slot-copy-cold-clean-wal-scratch
                            actual
                            (lambda ()
                              (when (and (eq kind :directory-swap) (= 2 (incf checks)))
                                (sb-posix:rename (namestring named) (namestring parked))
                                (sb-posix:mkdir (namestring named) #o700)
                                (setf swapped t)))
                            (lambda () (setf started t)) (if (eq kind :bound) 1 32))
                           nil)
                       (error () t)))
                    (rove:ng started)
                    (when (eq kind :directory-swap) (rove:ok swapped))
                    (rove:ok (equalp bytes (read-bounded-regular-file
                                            (merge-pathnames ".timeline-ABC123"
                                                             (physical-image-fd-path directory)) 4096))))
               (setf (physical-image-context-active context) t)
               (when swapped
                 (sb-posix:rmdir (namestring named))
                 (sb-posix:rename (namestring parked) (namestring named))))))))))

(rove:deftest cold-scratch-does-not-adopt-unrelated-postunlink-changes
    (dolist (kind '(:replace-protected :add-entry))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (declare (ignore root))
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (scratch (merge-pathnames ".timeline-ABC123" (wal-directory)))
                (parent (wal-segment-path 0 1 :timeline))
                (replacement (merge-pathnames "replacement" (wal-directory)))
                (bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 73))
                (original (symbol-function 'port-unlinkat)) (fired nil) (started nil))
           (cold-scratch-test-write scratch bytes)
           (cold-scratch-test-write parent bytes)
           (when (eq kind :replace-protected) (cold-scratch-test-write replacement bytes))
           (let ((before (physical-image-file-facts directory (file-namestring parent)))
                 (fds (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'port-unlinkat)
                          (lambda (fd name flags)
                            (prog1 (funcall original fd name flags)
                              (when (and (= fd directory) (not fired))
                                (setf fired t)
                                (ecase kind
                                  (:replace-protected
                                   (sb-posix:rename (namestring replacement) (namestring parent)))
                                  (:add-entry (cold-scratch-test-write replacement bytes)))))))
                    (rove:ok
                     (handler-case
                         (progn (logical-slot-copy-cold-clean-wal-scratch
                                 context (lambda () nil) (lambda () (setf started t)) 32) nil)
                       (error () t))))
               (setf (symbol-function 'port-unlinkat) original))
             (rove:ok fired)
             (rove:ok started)
             (rove:ok (not (probe-file scratch)))
             (rove:ok (equalp bytes (read-bounded-regular-file parent 4096)))
             (rove:ok
              (ecase kind
                (:replace-protected (/= (second before)
                                        (second (physical-image-file-facts directory
                                                                           (file-namestring parent)))))
                (:add-entry (probe-file replacement))))
             (rove:ok (= fds (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))))

(rove:deftest cold-scratch-callback-exits-drain-open-witnesses
    (dolist (cut '(:check-error :check-throw :check-interrupt :mark-error :mark-throw :mark-interrupt))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (declare (ignore root))
         (let* ((scratch (merge-pathnames ".timeline-ABC123" (wal-directory)))
                (bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 73))
                (marker (make-condition 'simple-error :format-control "scratch callback cut"))
                (checks 0) (fired nil)
                (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (cold-scratch-test-write scratch bytes)
           (flet ((inject ()
                    (setf fired t)
                    (case cut
                      ((:check-error :mark-error) (error marker))
                      ((:check-throw :mark-throw) (throw 'scratch-callback :cut))
                      (otherwise
                       (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                   (lambda () (throw 'scratch-callback :cut)))))))
             (let ((outcome
                     (handler-case
                         (catch 'scratch-callback
                           (logical-slot-copy-cold-clean-wal-scratch
                            context
                            (lambda ()
                              (when (and (= 2 (incf checks))
                                         (member cut '(:check-error :check-throw :check-interrupt)))
                                (inject)))
                            (lambda ()
                              (when (and (not fired) (member cut '(:mark-error :mark-throw :mark-interrupt)))
                                (inject)))
                            32)
                           :returned)
                       (error (condition) condition))))
               (rove:ok fired)
               (rove:ok (eq outcome (if (member cut '(:check-error :mark-error)) marker :cut)))))
           (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (unless (eq cut :mark-interrupt)
             (rove:ok (equalp bytes (read-bounded-regular-file scratch 4096))))
           (logical-slot-copy-cold-clean-wal-scratch context (lambda () nil) (lambda () nil) 32)
           (rove:ok (null (directory-entry-names (wal-directory) :follow-symlinks t)))
           (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))))))))

(rove:deftest cold-scratch-only-residue-removes-every-name
    (call-with-unit-promotion-directory
     (lambda (context root)
       (declare (ignore root))
       (let ((bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 73)))
         (dolist (name '(".history-ABC123" ".history-ABC124" ".timeline-ABC123" ".timeline-ABC124"))
           (cold-scratch-test-write (merge-pathnames name (wal-directory)) bytes))
         (rove:ok (= 4 (logical-slot-copy-cold-clean-wal-scratch
                        context (lambda () nil) (lambda () nil) 32)))
         (rove:ok (null (directory-entry-names (wal-directory) :follow-symlinks t)))
         (rove:ok (zerop (logical-slot-copy-cold-clean-wal-scratch
                          context (lambda () nil) (lambda () nil) 32)))))))
