(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-retirement-pid-observation-preserves-signed-metadata
    ((pid (range integer 1 2147483647)) (negative boolean))
  (:about pg::physical-reseed-follow-retirement-pid-state-at)
  (:trials (:normal 100))
  (with-physical-store-directory (root)
    (let* ((bytes (sb-ext:string-to-octets
                   (format nil "~d~%" (if negative (- pid) pid)) :external-format :utf-8))
           (path (merge-pathnames "postmaster.pid" root))
           (root-fd (pg::physical-backup-open-directory root))
           (pid-fd nil))
      (unwind-protect
           (progn
             (reseed-follow-owner-test-write path bytes)
             (setf pid-fd (pg::physical-image-openat
                           root-fd "postmaster.pid"
                           (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                   pg::+postmaster-o-cloexec+) 0))
             (when (minusp pid-fd) (setf pid-fd nil) (error "cannot open PID property fixture"))
             (let* ((state (funcall (symbol-function 'pg::physical-reseed-follow-retirement-pid-state-at)
                                    root-fd pid-fd))
                    (read (getf state :bytes)))
               (and (= pid (getf state :pid))
                    (equalp bytes read)
                    (not (eq bytes read))
                    (equal (getf state :identity) (pg::physical-reseed-file-facts (sb-posix:fstat pid-fd)))
                    (equal (getf state :identity) (pg::physical-reseed-install-row-file-facts (getf state :row)))
                    (progn
                      (setf (aref read 0) 0)
                      (equalp bytes (lineage-test-bytes path))))))
        (when pid-fd (sb-posix:close pid-fd))
        (sb-posix:close root-fd)))))

(deftest generated-reseed-retirement-pid-observation-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retirement-pid-observation-preserves-signed-metadata
                                       :seed 146 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(cl-spec:defproperty reseed-retirement-pid-copy-detaches-mutable-evidence
    ((pid (range integer 1 2147483647)) (negative boolean))
  (:about pg::physical-reseed-follow-retirement-pid-state-copy)
  (:trials (:normal 100))
  (let* ((text (format nil "~d~%" (if negative (- pid) pid)))
         (bytes (sb-ext:string-to-octets text :external-format :utf-8))
         (identity (list 1 pid #o100600 1 1000 (length bytes) 0 0))
         (row (list (copy-seq "postmaster.pid") 1 pid #o100600 1 1000 (length bytes) 0 0))
         (state (list :pid pid :identity identity :bytes bytes :row row))
         (copy (funcall (symbol-function 'pg::physical-reseed-follow-retirement-pid-state-copy) state)))
    (and (eql pid (getf copy :pid))
         (equal identity (getf copy :identity))
         (equal row (getf copy :row))
         (equalp bytes (getf copy :bytes))
         (not (eq bytes (getf copy :bytes)))
         (progn
           (setf (aref (getf copy :bytes) 0) 0
                 (char (first (getf copy :row)) 0) #\X)
           (incf (second (getf copy :identity)))
           (and (equalp bytes (sb-ext:string-to-octets text :external-format :utf-8))
                (equal identity (list 1 pid #o100600 1 1000 (length bytes) 0 0))
                (equal "postmaster.pid" (first row)))))))

(deftest generated-reseed-retirement-pid-copy-contract
    (ok (null (funcall (symbol-function 'pg::physical-reseed-follow-retirement-pid-state-copy) nil)))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'reseed-retirement-pid-copy-detaches-mutable-evidence
                                     :seed 146 :profile :normal))))
    (ok (eq :passed (getf data :status)))))

(defun call-with-reseed-retirement-test-owned-pid (root function)
  (let ((pg::*postmaster-data-lock* nil) (pg::*postmaster-pid-stream* nil)
                                         (pg::*postmaster-pid-directory* root) (pg::*postmaster-pid-inode* nil)
                                         (pg::*postmaster-start-time* (pg::postmaster-process-start-time))
                                         (pg::*postmaster-pid-host* "") (pg::*postmaster-pid-port* 55432)
                                         (hooks sb-ext:*exit-hooks*))
    (unwind-protect
         (progn
           (pg::postmaster-create-pid-file)
           (funcall function pg::*postmaster-pid-stream*))
      (unwind-protect
           (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
        (setf sb-ext:*exit-hooks* hooks)))))

(deftest reseed-retirement-pid-observation-proves-before-and-owned-contexts
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner))
       (let* ((candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (pid-path (merge-pathnames "postmaster.pid" root))
              (before-state nil) (owned-state nil) (escaped nil))
         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
         (ok (eq :absent
                 (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                  root-fd snapshot check
                  (lambda (descriptors state verify)
                    (declare (ignore descriptors))
                    (ok (null (getf state :pid-file)))
                    (ok (equal candidate (getf state :temporary)))
                    (ok (funcall verify))
                    :absent))))
         (reseed-follow-owner-test-write
          pid-path (sb-ext:string-to-octets (format nil "~d~%" (sb-posix:getpid)) :external-format :utf-8))
         (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
           (ok (eq :stale
                   (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                    root-fd snapshot check
                    (lambda (descriptors state verify)
                      (declare (ignore descriptors))
                      (setf before-state state)
                      (ok (= (sb-posix:getpid) (getf (getf state :pid-file) :pid)))
                      (ok (funcall verify))
                      :stale))))
           (ok (reseed-activation-layout-facts-equal-p
                before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))))
         (ok (handler-case
                 (progn
                   (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                    root-fd snapshot check
                    (lambda (descriptors state verify)
                      (declare (ignore descriptors state verify))
                      (error "strict PID callback must stay unreachable")))
                   nil)
               (error (condition) (search "PID entries" (princ-to-string condition)))))
         (call-with-reseed-retirement-test-owned-pid
          root
          (lambda (stream)
            (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))
                  (position (file-position stream))
                  (pid-bytes (lineage-test-bytes pid-path)))
              (ok (null pg::*postmaster-data-lock*))
              (ok (eq :owned
                      (pg::call-with-physical-reseed-follow-retirement-owned-pid-observation
                       root-fd snapshot check
                       (lambda (descriptors state verify)
                         (declare (ignore descriptors))
                         (setf owned-state (pg::physical-adoption-attempt-value-copy state)
                               (getf owned-state :pid-file)
                               (pg::physical-reseed-follow-retirement-pid-state-copy (getf state :pid-file))
                               escaped verify)
                         (ok (= (sb-posix:getpid) (getf (getf state :pid-file) :pid)))
                         (ok (pg::postmaster-pid-owned-p))
                         (ok (funcall verify))
                         (setf (aref (getf (getf state :pid-file) :bytes) 0) 0)
                         (ok (funcall verify))
                         :owned))))
              (ok (and (eq stream pg::*postmaster-pid-stream*) (open-stream-p stream)))
              (ok (eql position (file-position stream)))
              (ok (equalp pid-bytes (lineage-test-bytes pid-path)))
              (ok (equalp pid-bytes (getf (getf owned-state :pid-file) :bytes)))
              (ok (reseed-activation-layout-facts-equal-p
                   before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))))))
         (ok (equal (getf before-state :snapshot) (getf owned-state :snapshot)))
         (ok (equal (getf before-state :temporary-file) (getf owned-state :temporary-file)))
         (ok (not (equal (getf (getf before-state :pid-file) :identity)
                         (getf (getf owned-state :pid-file) :identity))))
         (ok (handler-case (progn (funcall escaped) nil)
               (error (condition) (search "scope ended" (princ-to-string condition)))))
         (ok (not (pg::physical-adoption-path-present-p pid-path)))))))

(defun call-with-reseed-retirement-test-pid-directory (function)
  (with-physical-store-directory (root)
    (sb-posix:chmod (namestring root) #o700)
    (reseed-follow-owner-test-write
     (merge-pathnames ".pg_lisp.lock" root) (make-array 0 :element-type '(unsigned-byte 8)))
    (pg::call-with-physical-adoption-directory-owner
     root
     (lambda (owner)
       (pg::call-with-physical-directory-root
        owner
        (lambda (root-fd directory)
          (declare (ignore directory))
          (flet ((check () (pg::physical-directory-check-owner-common owner)))
            (funcall function root root-fd #'check))))))))

(defun reseed-retirement-test-pid-tree-facts (root)
  (loop for name in (sort (pg::directory-entry-names root :follow-symlinks t) #'string<)
        for path = (merge-pathnames name root)
        for stat = (sb-posix:lstat (namestring path))
        collect (list name (pg::physical-reseed-file-facts stat)
                      (cond ((sb-posix:s-isreg (sb-posix:stat-mode stat)) (lineage-test-bytes path))
                            ((sb-posix:s-islnk (sb-posix:stat-mode stat)) (sb-posix:readlink (namestring path)))
                            ((sb-posix:s-isdir (sb-posix:stat-mode stat))
                             (sort (pg::directory-entry-names
                                    (uiop:ensure-directory-pathname path) :follow-symlinks t) #'string<))))))

(deftest reseed-retirement-before-pid-preserves-invalid-metadata
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (let ((path (merge-pathnames "postmaster.pid" root))
             (other (merge-pathnames "other-pid" root)))
         (dolist (kind '(:empty :zero :positive-bound :negative-bound :text :trailing
                         :invalid-utf8 :oversize :directory :symlink :hardlink :fifo))
           (let ((called nil))
             (unwind-protect
                  (progn
                    (case kind
                      (:directory (sb-posix:mkdir (namestring path) #o700))
                      (:symlink (sb-posix:symlink ".pg_lisp.lock" (namestring path)))
                      (:hardlink
                       (reseed-follow-owner-test-write other (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
                       (sb-posix:link (namestring other) (namestring path)))
                      (:fifo (sb-posix:mkfifo (namestring path) #o600))
                      (otherwise
                       (reseed-follow-owner-test-write
                        path
                        (case kind
                          (:empty (make-array 0 :element-type '(unsigned-byte 8)))
                          (:oversize (make-array 16385 :element-type '(unsigned-byte 8) :initial-element 48))
                          (:invalid-utf8 (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(255 10)))
                          (otherwise
                           (pg::string->bytes
                            (format nil "~a~%"
                                    (ecase kind
                                      (:zero "0") (:positive-bound "2147483648")
                                      (:negative-bound "-2147483648") (:text "not-a-pid") (:trailing "1 junk")))))))))
                    (let ((before (reseed-retirement-test-pid-tree-facts root)))
                      (ok (handler-case
                              (progn
                                (pg::call-with-physical-reseed-follow-retirement-before-pid
                                 root-fd check (lambda (state verify)
                                                 (declare (ignore state verify))
                                                 (setf called t)))
                                nil)
                            (error () t))
                          (format nil "~a PID metadata is refused" kind))
                      (ok (not called))
                      (ok (reseed-activation-layout-facts-equal-p before (reseed-retirement-test-pid-tree-facts root)))))
               (if (eq kind :directory) (sb-posix:rmdir (namestring path)) (delete-file path))
               (when (eq kind :hardlink) (delete-file other)))))))))

(deftest reseed-retirement-before-pid-uses-c-liveness-policy
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (let ((path (merge-pathnames "postmaster.pid" root)) (escaped nil))
         (ok (eq :absent
                 (pg::call-with-physical-reseed-follow-retirement-before-pid
                  root-fd check
                  (lambda (state verify)
                    (setf escaped verify)
                    (ok (null state))
                    (ok (funcall verify))
                    :absent))))
         (ok (handler-case (progn (funcall escaped) nil)
               (error (condition) (search "scope ended" (princ-to-string condition)))))
         (dolist (pid (list (sb-posix:getpid) (sb-posix:getppid)))
           (dolist (sign '(1 -1))
             (reseed-follow-owner-test-write path (pg::string->bytes (format nil "~d~%" (* sign pid))))
             (let ((before (reseed-retirement-test-pid-tree-facts root)))
               (ok (eq :ancestor
                       (pg::call-with-physical-reseed-follow-retirement-before-pid
                        root-fd check
                        (lambda (state verify)
                          (ok (= pid (getf state :pid)))
                          (ok (funcall verify))
                          :ancestor))))
               (ok (reseed-activation-layout-facts-equal-p before (reseed-retirement-test-pid-tree-facts root))))
             (delete-file path)))
         (let ((child (sb-ext:run-program "/bin/sleep" '("60") :wait nil :output nil :error nil))
               (called nil))
           (unwind-protect
                (progn
                  (reseed-follow-owner-test-write
                   path (pg::string->bytes (format nil "~d~%" (sb-ext:process-pid child))))
                  (let ((before (reseed-retirement-test-pid-tree-facts root)))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retirement-before-pid
                               root-fd check
                               (lambda (state verify)
                                 (declare (ignore state verify))
                                 (setf called t)))
                              nil)
                          (error (condition) (search "live process" (princ-to-string condition)))))
                    (ok (not called))
                    (ok (reseed-activation-layout-facts-equal-p before (reseed-retirement-test-pid-tree-facts root))))
                  (sb-ext:process-kill child sb-posix:sigterm)
                  (sb-ext:process-wait child)
                  (ok (eq :dead
                          (pg::call-with-physical-reseed-follow-retirement-before-pid
                           root-fd check
                           (lambda (state verify)
                             (ok (= (sb-ext:process-pid child) (getf state :pid)))
                             (ok (funcall verify))
                             :dead)))))
             (when (eq :running (sb-ext:process-status child))
               (sb-ext:process-kill child sb-posix:sigterm)
               (sb-ext:process-wait child))
             (sb-ext:process-close child)))))))

(deftest reseed-retirement-before-pid-fences-probe-failure-and-mutation
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (let ((path (merge-pathnames "postmaster.pid" root))
             (original (symbol-function 'pg::postmaster-pid-is-live-p)))
         (dolist (kind '(:live :unknown :rewrite))
           (when (pg::physical-adoption-path-present-p path) (delete-file path))
           (reseed-follow-owner-test-write path (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
           (let ((visited nil) (called nil) (message nil)
                               (expected (reseed-retirement-test-pid-tree-facts root)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::postmaster-pid-is-live-p)
                          (lambda (pid)
                            (declare (ignore pid))
                            (setf visited t)
                            (ecase kind
                              (:live t)
                              (:unknown (error "injected unknown PID probe"))
                              (:rewrite
                               (reseed-retirement-test-rewrite-file
                                path (pg::string->bytes (format nil "~d~%" (1+ (sb-posix:getpid)))))
                               (setf expected (reseed-retirement-test-pid-tree-facts root))
                               nil))))
                    (handler-case
                        (pg::call-with-physical-reseed-follow-retirement-before-pid
                         root-fd check
                         (lambda (state verify) (declare (ignore state verify)) (setf called t)))
                      (error (condition) (setf message (princ-to-string condition)))))
               (setf (symbol-function 'pg::postmaster-pid-is-live-p) original))
             (ok visited)
             (ok (not called))
             (ok (and message
                      (search (ecase kind
                                (:live "live process") (:unknown "injected unknown PID probe")
                                (:rewrite "PID changed"))
                              message)))
             (ok (reseed-activation-layout-facts-equal-p expected (reseed-retirement-test-pid-tree-facts root)))))))))

(deftest reseed-retirement-pid-scope-fences-file-changes-and-copies-callback-bytes
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (let* ((path (merge-pathnames "postmaster.pid" root))
              (replacement (merge-pathnames "replacement-pid" root))
              (bytes (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
              (original-open (symbol-function 'pg::physical-image-openat)))
         (dolist (kind '(:appear :replace :rewrite :remove :chmod))
           (unless (eq kind :appear) (reseed-follow-owner-test-write path bytes))
           (let ((fd nil) (cloexec nil) (injected nil) (message nil) (expected nil) (escaped nil))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-image-openat)
                          (lambda (&rest arguments)
                            (let ((opened (apply original-open arguments)))
                              (when (and (equal (second arguments) "postmaster.pid") (not (minusp opened)))
                                (setf fd opened cloexec (logbitp 0 (sb-posix:fcntl opened sb-posix:f-getfd))))
                              opened)))
                    (handler-case
                        (pg::call-with-physical-reseed-follow-retirement-before-pid
                         root-fd check
                         (lambda (state verify)
                           (setf escaped verify)
                           (when state
                             (setf (aref (getf state :bytes) 0) 0)
                             (ok (funcall verify)))
                           (ecase kind
                             (:appear (reseed-follow-owner-test-write path bytes))
                             (:replace
                              (reseed-follow-owner-test-write replacement bytes)
                              (sb-posix:rename (namestring replacement) (namestring path))
                              (pg::fsync-directory root))
                             (:rewrite (sleep 0.01) (reseed-retirement-test-rewrite-file path bytes))
                             (:remove (delete-file path))
                             (:chmod (sb-posix:chmod (namestring path) #o640)))
                           (setf injected t expected (reseed-retirement-test-pid-tree-facts root))))
                      (error (condition) (setf message (princ-to-string condition)))))
               (setf (symbol-function 'pg::physical-image-openat) original-open))
             (ok injected)
             (ok (and (stringp message) (plusp (length message))))
             (ok (reseed-activation-layout-facts-equal-p expected (reseed-retirement-test-pid-tree-facts root)))
             (if (eq kind :appear)
                 (ok (null fd))
                 (progn
                   (ok cloexec)
                   (ok (handler-case (progn (sb-posix:fstat fd) nil) (sb-posix:syscall-error () t)))))
             (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))
             (ok (handler-case (progn (funcall escaped) nil)
                   (error (condition) (search "scope ended" (princ-to-string condition)))))
             (when (pg::physical-adoption-path-present-p path) (delete-file path))
             (reseed-follow-owner-test-write path bytes)
             (ok (eq :reopened
                     (pg::call-with-physical-reseed-follow-retirement-before-pid
                      root-fd check (lambda (state verify)
                                      (declare (ignore state))
                                      (funcall verify)
                                      :reopened)))))
           (delete-file path))))))

(deftest reseed-retirement-owned-pid-scope-pins-the-postmaster-context
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (let ((foreign (merge-pathnames "foreign/" root)) (escaped nil))
         (sb-posix:mkdir (namestring foreign) #o700)
         (unwind-protect
              (call-with-reseed-retirement-test-owned-pid
               root
               (lambda (stream)
                 (let ((before (reseed-retirement-test-pid-tree-facts root))
                       (position (file-position stream)))
                   (ok (null pg::*postmaster-data-lock*))
                   (ok (eq :checked
                           (pg::call-with-physical-reseed-follow-retirement-owned-pid
                            root-fd check
                            (lambda (state verify)
                              (setf escaped verify)
                              (flet ((refused ()
                                       (handler-case (progn (funcall verify) nil)
                                         (error (condition)
                                           (search "postmaster PID context" (princ-to-string condition))))))
                                (let ((pg::*postmaster-pid-stream* nil)) (ok (refused)))
                                (ok (funcall verify))
                                (let ((pg::*postmaster-pid-inode* (1+ pg::*postmaster-pid-inode*))) (ok (refused)))
                                (ok (funcall verify))
                                (let ((pg::*postmaster-pid-directory* foreign)) (ok (refused)))
                                (ok (funcall verify))
                                (with-open-file (other (merge-pathnames "postmaster.pid" root) :direction :input)
                                  (let ((pg::*postmaster-pid-stream* other)) (ok (refused))))
                                (ok (funcall verify)))
                              (setf (aref (getf state :bytes) 0) 0)
                              (ok (funcall verify))
                              :checked))))
                   (with-open-file (closed (merge-pathnames "postmaster.pid" root) :direction :input)
                     (close closed)
                     (let ((pg::*postmaster-pid-stream* closed))
                       (ok (handler-case
                               (progn
                                 (pg::call-with-physical-reseed-follow-retirement-owned-pid
                                  root-fd check
                                  (lambda (state verify)
                                    (declare (ignore state verify))
                                    (error "closed PID context callback must stay unreachable")))
                                 nil)
                             (error (condition) (search "postmaster PID context" (princ-to-string condition)))))))
                   (ok (and (eq stream pg::*postmaster-pid-stream*) (open-stream-p stream)))
                   (ok (eql position (file-position stream)))
                   (ok (reseed-activation-layout-facts-equal-p before (reseed-retirement-test-pid-tree-facts root)))
                   (ok (handler-case (progn (funcall escaped) nil)
                         (error (condition) (search "scope ended" (princ-to-string condition))))))))
           (sb-posix:rmdir (namestring foreign)))))))

(deftest reseed-retirement-owned-pid-scope-fences-writes-and-name-substitution
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (call-with-reseed-retirement-test-owned-pid
        root
        (lambda (stream)
          (let* ((path (merge-pathnames "postmaster.pid" root))
                 (backup (merge-pathnames "original-pid" root))
                 (bytes (lineage-test-bytes path)))
            (dolist (kind '(:rewrite :replace :remove :status))
              (let ((injected nil) (message nil) (expected nil))
                (unwind-protect
                     (progn
                       (handler-case
                           (pg::call-with-physical-reseed-follow-retirement-owned-pid
                            root-fd check
                            (lambda (state verify)
                              (declare (ignore state verify))
                              (ecase kind
                                (:rewrite
                                 (reseed-retirement-test-rewrite-file
                                  path (pg::string->bytes (format nil "~d~%" (1+ (sb-posix:getpid))))))
                                ((:replace :remove)
                                 (sb-posix:rename (namestring path) (namestring backup))
                                 (when (eq kind :replace) (reseed-follow-owner-test-write path bytes)))
                                (:status (pg::postmaster-update-pid-status "ready   ")))
                              (setf injected t expected (reseed-retirement-test-pid-tree-facts root))))
                         (error (condition) (setf message (princ-to-string condition))))
                       (ok injected)
                       (ok (and (stringp message) (plusp (length message))))
                       (ok (reseed-activation-layout-facts-equal-p expected (reseed-retirement-test-pid-tree-facts root)))
                       (ok (and (eq stream pg::*postmaster-pid-stream*) (open-stream-p stream))))
                  (when (pg::physical-adoption-path-present-p backup)
                    (when (pg::physical-adoption-path-present-p path) (delete-file path))
                    (sb-posix:rename (namestring backup) (namestring path)))
                  (reseed-retirement-test-rewrite-file path bytes))
                (ok (pg::postmaster-pid-owned-p))
                (ok (eq :reopened
                        (pg::call-with-physical-reseed-follow-retirement-owned-pid
                         root-fd check
                         (lambda (state verify) (declare (ignore state)) (funcall verify) :reopened))))))))))))

(defun check-reseed-retirement-pid-exits (root root-fd check reader &optional borrowed)
  (let ((original-open (symbol-function 'pg::physical-image-openat)))
    (dolist (cut '((:open :error) (:open :throw) (:open :interrupt)
                   (:callback :error) (:callback :throw) (:callback :interrupt)
                   (:handoff :interrupt)))
      (destructuring-bind (boundary mode) cut
        (let ((tag (gensym "PID-SCOPE-EXIT")) (injected nil) (called nil) (fd nil)
                                              (cloexec nil) (result nil)
                                              (position (when borrowed (file-position borrowed)))
                                              (before (reseed-retirement-test-pid-tree-facts root)))
          (flet ((exit-now ()
                   (setf injected t)
                   (ecase mode
                     (:error (error "injected PID scope error"))
                     (:throw (throw tag :thrown))
                     (:interrupt
                      (sb-thread:interrupt-thread
                       sb-thread:*current-thread* (lambda () (error "injected PID scope interrupt")))))))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-image-openat)
                         (lambda (&rest arguments)
                           (let ((opened (apply original-open arguments)))
                             (when (and (equal (second arguments) "postmaster.pid") (not (minusp opened)))
                               (setf fd opened
                                     cloexec (logbitp 0 (sb-posix:fcntl opened sb-posix:f-getfd)))
                               (when (eq boundary :handoff) (exit-now)))
                             opened)))
                   (let ((pg::*physical-reseed-follow-test-hook*
                           (lambda (point path)
                             (declare (ignore path))
                             (when (and (eq boundary :open) (eq point :after-retirement-pid-open))
                               (exit-now)))))
                     (setf result
                           (handler-case
                               (catch tag
                                 (funcall reader root-fd check
                                          (lambda (state verify)
                                            (declare (ignore state verify))
                                            (setf called t)
                                            (when (eq boundary :callback) (exit-now))))
                                 :returned)
                             (error (condition) (princ-to-string condition))))))
              (setf (symbol-function 'pg::physical-image-openat) original-open)))
          (ok injected)
          (ok (if (eq mode :throw) (eq result :thrown)
                  (and (stringp result) (search "injected PID scope" result))))
          (ok (eq called (eq boundary :callback)))
          (ok cloexec)
          (ok (and fd (handler-case (progn (sb-posix:fstat fd) nil) (sb-posix:syscall-error () t))))
          (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))
          (ok (reseed-activation-layout-facts-equal-p before (reseed-retirement-test-pid-tree-facts root)))
          (when borrowed
            (ok (and (eq borrowed pg::*postmaster-pid-stream*) (open-stream-p borrowed)))
            (ok (eql position (file-position borrowed))))
          (ok (eq :reopened
                  (funcall reader root-fd check
                           (lambda (state verify) (declare (ignore state)) (funcall verify) :reopened)))))))))

(deftest reseed-retirement-pid-scope-closes-only-its-own-descriptor-on-every-exit
    (call-with-reseed-retirement-test-pid-directory
     (lambda (root root-fd check)
       (let ((path (merge-pathnames "postmaster.pid" root)))
         (reseed-follow-owner-test-write path (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
         (check-reseed-retirement-pid-exits root root-fd check #'pg::call-with-physical-reseed-follow-retirement-before-pid)
         (delete-file path)
         (call-with-reseed-retirement-test-owned-pid
          root (lambda (stream)
                 (check-reseed-retirement-pid-exits
                  root root-fd check #'pg::call-with-physical-reseed-follow-retirement-owned-pid stream)))))))

(defun check-reseed-retirement-pid-prefixes (kind)
  (call-with-reseed-retirement-observation-fixture
   kind
   (lambda (owner root workspace root-fd snapshot check)
     (let* ((plan (pg::physical-reseed-install-owner-plan owner))
            (directories (list root workspace
                               (pathname (getf (getf plan :incoming) :directory))
                               (pathname (getf (getf plan :quarantine) :directory))))
            (record (pg::physical-reseed-follow-copy (getf snapshot :record)))
            (initial (reseed-retirement-test-initial-candidate root-fd snapshot))
            (reference (getf (getf initial :retirement) :attempt-file))
            (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
            (pid-path (merge-pathnames "postmaster.pid" root))
            (saved (pg::physical-backup-open-directory (merge-pathnames "saved/" workspace))))
       (unwind-protect
            (progn
              (loop for position from 0 to 3
                    for prefix = (min position 2)
                    for candidate = (pg::physical-reseed-follow-retirement-next-record record reference)
                    do (when (member position '(1 2))
                         (let ((name (if (= position 1) "physical-reseed.install" "physical-reseed.attempt")))
                           (unless (zerop (pg::physical-adoption-renameat2
                                           root-fd name saved name pg::+physical-adoption-rename-noreplace+))
                             (error "PID observation fixture archive failed"))))
                       (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
                       (reseed-follow-owner-test-write
                        pid-path (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
                       (let ((current (pg::physical-reseed-follow-snapshot-at root-fd))
                             (before-state nil))
                         (let ((before (mapcar #'reseed-mapped-test-tree-facts directories)))
                           (ok (eq :before
                                   (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                                    root-fd current check
                                    (lambda (descriptors state verify)
                                      (declare (ignore descriptors))
                                      (setf before-state state)
                                      (ok (= prefix (getf state :prefix)))
                                      (ok (equal candidate (getf state :temporary)))
                                      (ok (funcall verify))
                                      :before))))
                           (ok (reseed-activation-layout-facts-equal-p
                                before (mapcar #'reseed-mapped-test-tree-facts directories))))
                         (when (zerop position)
                           (let ((foreign (merge-pathnames "foreign.startup" root)) (called nil))
                             (unwind-protect
                                  (progn
                                    (reseed-follow-owner-test-write
                                     foreign (make-array 1 :element-type '(unsigned-byte 8) :initial-element 10))
                                    (let ((before (mapcar #'reseed-mapped-test-tree-facts directories)))
                                      (ok (handler-case
                                              (progn
                                                (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                                                 root-fd current check
                                                 (lambda (descriptors state verify)
                                                   (declare (ignore descriptors state verify))
                                                   (setf called t)))
                                                nil)
                                            (error () t)))
                                      (ok (not called))
                                      (ok (reseed-activation-layout-facts-equal-p
                                           before (mapcar #'reseed-mapped-test-tree-facts directories)))))
                               (delete-file foreign))))
                         (call-with-reseed-retirement-test-owned-pid
                          root
                          (lambda (stream)
                            (let ((before (mapcar #'reseed-mapped-test-tree-facts directories))
                                  (owned-state nil))
                              (ok (null pg::*postmaster-data-lock*))
                              (ok (eq :owned
                                      (pg::call-with-physical-reseed-follow-retirement-owned-pid-observation
                                       root-fd current check
                                       (lambda (descriptors state verify)
                                         (declare (ignore descriptors))
                                         (setf owned-state state)
                                         (ok (= prefix (getf state :prefix)))
                                         (ok (equal candidate (getf state :temporary)))
                                         (ok (funcall verify))
                                         :owned))))
                              (ok (and (eq stream pg::*postmaster-pid-stream*) (open-stream-p stream)
                                       (pg::postmaster-pid-owned-p)))
                              (ok (equal (getf before-state :snapshot) (getf owned-state :snapshot)))
                              (ok (equal (getf before-state :temporary-file) (getf owned-state :temporary-file)))
                              (ok (not (equal (getf (getf before-state :pid-file) :identity)
                                              (getf (getf owned-state :pid-file) :identity))))
                              (ok (reseed-activation-layout-facts-equal-p
                                   before (mapcar #'reseed-mapped-test-tree-facts directories)))))))
                       (delete-file temporary)
                       (setf record candidate)
                       (reseed-retirement-layout-write root record))
              (let ((current (pg::physical-reseed-follow-snapshot-at root-fd)))
                (ok (eq :absent
                        (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                         root-fd current check
                         (lambda (descriptors state verify)
                           (declare (ignore descriptors))
                           (ok (null (getf state :pid-file)))
                           (ok (null (getf state :temporary)))
                           (ok (= 2 (getf state :prefix)))
                           (ok (funcall verify))
                           :absent))))
                (call-with-reseed-retirement-test-owned-pid
                 root
                 (lambda (stream)
                   (ok (eq :retired
                           (pg::call-with-physical-reseed-follow-retirement-owned-pid-observation
                            root-fd current check
                            (lambda (descriptors state verify)
                              (declare (ignore descriptors))
                              (ok (null (getf state :temporary)))
                              (ok (= 2 (getf state :prefix)))
                              (ok (funcall verify))
                              :retired))))
                   (ok (and (eq stream pg::*postmaster-pid-stream*) (open-stream-p stream)))))))
         (sb-posix:close saved))))))

(deftest reseed-retirement-pid-observation-checks-all-prefixes-and-geometries
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (check-reseed-retirement-pid-prefixes kind)))

(deftest reseed-retirement-pid-observation-fences-late-full-proof-changes
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (let* ((plan (pg::physical-reseed-install-owner-plan owner))
              (directories (list root workspace
                                 (pathname (getf (getf plan :incoming) :directory))
                                 (pathname (getf (getf plan :quarantine) :directory))))
              (candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (pid-path (merge-pathnames "postmaster.pid" root))
              (final (merge-pathnames "physical-reseed.follow" root))
              (replacement (merge-pathnames "replacement-follow" root))
              (original-proof (symbol-function 'pg::physical-reseed-follow-configuration-base-proof)))
         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
         (reseed-follow-owner-test-write pid-path (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
         (dolist (boundary '(:proof :callback))
           (dolist (kind '(:pid :temporary :final))
             (let ((current (pg::physical-reseed-follow-snapshot-at root-fd))
                   (armed t) (injected nil) (called nil) (expected nil) (message nil))
               (flet ((inject ()
                        (if (eq kind :final)
                            (progn
                              (reseed-follow-owner-test-write replacement (lineage-test-bytes final))
                              (sb-posix:rename (namestring replacement) (namestring final))
                              (pg::fsync-directory root))
                            (let ((path (if (eq kind :pid) pid-path temporary)))
                              (reseed-retirement-test-rewrite-file path (lineage-test-bytes path))))
                        (setf injected t expected (mapcar #'reseed-mapped-test-tree-facts directories))))
                 (unwind-protect
                      (progn
                        (when (eq boundary :proof)
                          (setf (symbol-function 'pg::physical-reseed-follow-configuration-base-proof)
                                (lambda (&rest arguments)
                                  (multiple-value-prog1 (apply original-proof arguments)
                                    (when armed (setf armed nil) (inject))))))
                        (handler-case
                            (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                             root-fd current check
                             (lambda (descriptors state verify)
                               (declare (ignore descriptors state verify))
                               (setf called t)
                               (when (eq boundary :callback) (inject))))
                          (error (condition) (setf message (princ-to-string condition)))))
                   (setf (symbol-function 'pg::physical-reseed-follow-configuration-base-proof) original-proof)))
               (ok injected)
               (ok (eq called (eq boundary :callback)))
               (ok (and message
                        (search (ecase kind (:pid "pid") (:temporary "temporary") (:final "final generation"))
                                (string-downcase message))))
               (ok (reseed-activation-layout-facts-equal-p
                    expected (mapcar #'reseed-mapped-test-tree-facts directories)))
               (ok (eq :reopened
                       (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                        root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                        (lambda (descriptors state verify)
                          (declare (ignore descriptors state))
                          (funcall verify)
                          :reopened)))))))))))
