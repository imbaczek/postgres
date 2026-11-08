(in-package #:pg-physical-tests)

(defun configured-mount-context-valid-p (marker pid effective-uid rows namespace)
  (labels ((namespace-p (value)
             (and (listp value)
                  (eql 2 (list-length value))
                  (typep (first value) '(integer 0 *))
                  (typep (second value) '(integer 1 *)))))
    (handler-case
        (and (listp marker)
             (eql 6 (list-length marker))
             (destructuring-bind
                 (pid-key marked-pid uid-key parent-uid namespace-key parent-namespace)
                 marker
               (and (eq pid-key :pid)
                    (eq uid-key :parent-uid)
                    (eq namespace-key :parent-namespace)
                    (typep pid '(integer 1 *))
                    (eql marked-pid pid)
                    (eql effective-uid 0)
                    (typep parent-uid '(integer 0 *))
                    (equal rows (list (list 0 parent-uid 1)))
                    (namespace-p parent-namespace)
                    (namespace-p namespace)
                    (not (equal namespace parent-namespace)))))
      (type-error () nil))))

(cl-spec:defproperty configured-mount-context-requires-owned-namespace
    ((shape (range integer 0 11)) (pid (range integer 1 100000))
                                  (uid (range integer 1 10000)))
  (:about configured-mount-context-valid-p)
  (:trials (:normal 100))
  (let ((marker (list :pid pid :parent-uid uid :parent-namespace '(7 11)))
        (rows (list (list 0 uid 1)))
        (namespace '(7 12))
        (effective-uid 0))
    (case shape
      (1 (setf marker nil))
      (2 (setf (getf marker :pid) (1+ pid)))
      (3 (setf effective-uid 1))
      (4 (setf rows nil))
      (5 (setf (third (first rows)) 2))
      (6 (setf (second (first rows)) (1+ uid)))
      (7 (setf rows (append rows (list '(1 1 1)))))
      (8 (setf namespace '(7 11)))
      (9 (setf marker (list :pid pid)))
      (10 (setf namespace nil))
      (11 (setf (first (first rows)) 1)))
    (let ((before (copy-tree (list marker rows namespace))))
      (and (eql (zerop shape)
                (not (null (funcall (symbol-function 'configured-mount-context-valid-p)
                                    marker pid effective-uid rows namespace))))
           (equal before (list marker rows namespace))))))

(deftest generated-configured-mount-namespace-guard-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'configured-mount-context-requires-owned-namespace
                                   :seed 202 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(deftest configured-mount-namespace-guard-rejects-malformed-context
    (let* ((marker (list :pid 23 :parent-uid 1000 :parent-namespace '(7 11)))
           (rows '((0 1000 1)))
           (namespace '(7 12))
           (cycle (list 1))
           (row-cycle (list (list 0 1000 1)))
           (column-cycle (list 0 1000 1)))
      (setf (cdr cycle) cycle
            (cdr row-cycle) row-cycle
            (cdr (last column-cycle)) column-cycle)
      (ok (configured-mount-context-valid-p marker 23 0 rows namespace))
      (dolist (bad (list nil 1 '(1 . 2) cycle
                         '(:pid 23 :parent-uid 1000 :parent-namespace (7 11) :extra t)
                         '(:pid 23 :parent-uid -1 :parent-namespace (7 11))
                         '(:pid 23 :parent-uid 1000 :parent-namespace (7 0))))
        (ok (null (configured-mount-context-valid-p bad 23 0 rows namespace))))
      (dolist (bad (list nil 1 '(7 . 12) cycle '(7 0) '(-1 12) '(7 12 13)))
        (ok (null (configured-mount-context-valid-p marker 23 0 rows bad))))
      (dolist (bad (list nil 1 '(0 . 1000) cycle row-cycle (list column-cycle) '((0 1000 2)) '((0 1001 1))))
        (ok (null (configured-mount-context-valid-p marker 23 0 bad namespace))))))

(defvar *configured-mount-child-context* nil)

(defun configured-mount-namespace-identity ()
  (let ((stat (sb-posix:stat "/proc/self/ns/mnt")))
    (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat))))

(defun configured-mount-require-child-context ()
  (let ((rows
          (with-open-file (input "/proc/self/uid_map")
            (loop for line = (read-line input nil)
                  while line
                  collect (mapcar #'parse-integer
                                  (remove "" (uiop:split-string
                                              line :separator '(#\Space #\Tab))
                                          :test #'string=))))))
    (unless (configured-mount-context-valid-p
             *configured-mount-child-context* (sb-posix:getpid)
             (sb-posix:geteuid) rows (configured-mount-namespace-identity))
      (error "configured mount test requires its owned user and mount namespace")))
  t)

(sb-alien:define-alien-routine ("mount" configured-test-mount-syscall) sb-alien:int
  (source sb-alien:c-string) (target sb-alien:c-string)
  (filesystem sb-alien:c-string) (flags sb-alien:unsigned-long)
  (data sb-alien:c-string))

(sb-alien:define-alien-routine ("umount2" configured-test-umount-syscall) sb-alien:int
  (target sb-alien:c-string) (flags sb-alien:int))

(defun configured-mount-syscall (source target flags)
  (configured-mount-require-child-context)
  (unless (zerop (configured-test-mount-syscall source target nil flags nil))
    (error "configured test mount failed with errno ~d" (sb-alien:get-errno))))

(defun configured-unmount-syscall (target)
  (configured-mount-require-child-context)
  (unless (zerop (configured-test-umount-syscall target 0))
    (error "configured test unmount failed with errno ~d" (sb-alien:get-errno))))

(deftest configured-mount-guard-refuses-the-parent-context
    (let ((*configured-mount-child-context*
            (list :pid (1+ (sb-posix:getpid)) :parent-uid (sb-posix:geteuid)
                  :parent-namespace (configured-mount-namespace-identity))))
      (ok (signals (configured-mount-require-child-context) 'error))))

(defvar *configured-mount-completed-cases* nil)

(defun configured-mount-expected-cases ()
  (loop for kind in '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer)
        append (loop for phase in '(:mapped :closed)
                     append (loop for role in '(:root :incoming :quarantine :base :global :wal
                                                :workspace :staged :saved :receive :ancestor)
                                  collect (list kind phase role)))))

(defun check-configured-cancellation-one-mount-change (owner root ancestor phase role)
  (let ((observe (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
        (image (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image))
        (before (reseed-mapped-test-tree-facts ancestor))
        (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
        (record (pg::physical-reseed-follow-read-configured-cancellation owner))
        (directory nil) (old-mount nil) (identity nil) (target-fd nil)
        (mounted nil) (fired 0) (returned nil) (message nil))
    (flet ((bind-directory ()
             (sb-sys:without-interrupts
               (configured-mount-syscall (namestring directory) (namestring directory) 4096)
               (setf mounted t))
             (incf fired)
             (let ((stat (sb-posix:stat (namestring directory))))
               (ok (equal identity (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)))))
             (ok (/= old-mount (pg::physical-adoption-install-mount-id directory)))))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                   (lambda (held fd check)
                     (let* ((context (pg::physical-reseed-follow-installation-context held))
                            (descriptors (pg::physical-reseed-follow-installation-context-descriptors context))
                            (stat nil))
                       (setf target-fd
                             (pg::physical-reseed-follow-configuration-role-fd
                              descriptors (if (eq role :ancestor) :root role))
                             directory (if (eq role :ancestor)
                                           (truename ancestor)
                                           (truename (pg::physical-image-fd-path target-fd)))
                             stat (sb-posix:stat (namestring directory))
                             identity (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat))
                             old-mount (pg::physical-adoption-install-mount-id directory))
                       (let ((result (funcall observe held fd check)))
                         (when (eq phase :mapped) (bind-directory))
                         result)))
                   (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image)
                   (lambda (held function)
                     (let ((result (funcall image held function)))
                       (when (eq phase :closed)
                         (ok (handler-case
                                 (progn (sb-posix:fstat target-fd) nil)
                               (sb-posix:syscall-error (condition)
                                 (= sb-posix:ebadf (sb-posix:syscall-errno condition)))))
                         (bind-directory))
                       result)))
             (setf message
                   (handler-case
                       (progn
                         (pg::physical-reseed-follow-observe-configured-cancellation owner)
                         (setf returned t)
                         nil)
                     (error (condition) (princ-to-string condition))))
             (ok (= fired 1))
             (ok mounted)
             (ok (and message
                      (search
                       (cond
                         ((member role '(:root :quarantine))
                          "reseed auxiliary must share the owned target mount")
                         ((eq phase :closed)
                          "retirement observation changed during resource cleanup")
                         ((member role '(:workspace :ancestor))
                          "configuration workspace path changed its mount")
                         ((member role '(:staged :saved))
                          "configuration child directory changed its mount")
                         (t "reseed old inventory requires complete facts on the target mount"))
                       message)))
             (ok (not returned))
             (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
             (ok (equalp before (reseed-mapped-test-tree-facts ancestor)))
             (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
        (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe
              (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image) image)
        (when mounted (configured-unmount-syscall (namestring directory)))))
    (ok (= old-mount (pg::physical-adoption-install-mount-id directory)))
    (ok (equalp before (reseed-mapped-test-tree-facts ancestor)))
    (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner)))
    (ok (pg::physical-reseed-follow-observe-configured-cancellation owner))
    (format t "~%CONFIGURED-MOUNT-CASE ~s ~s ~s~%" phase role message)))

(defun check-configured-cancellation-mount-changes ()
  (configured-mount-require-child-context)
  (configured-mount-syscall nil "/" (logior 16384 262144))
  (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
    (call-with-physical-store-directory
     (lambda (ancestor)
       (let ((uiop/stream:*temporary-directory* ancestor))
         (call-with-populated-configured-cancellation-fixture
          kind
          (lambda (owner root workspace) (declare (ignore owner root workspace)))
          (lambda (root workspace)
            (declare (ignore workspace))
            (pg::call-with-physical-reseed-follow-cancellation-owner
             root
             (lambda (owner)
               (dolist (phase '(:mapped :closed))
                 (dolist (role '(:root :incoming :quarantine :base :global :wal
                                 :workspace :staged :saved :receive :ancestor))
                   (testing (format nil "~s ~s ~s" kind phase role)
                            (check-configured-cancellation-one-mount-change
                             owner root ancestor phase role))
                   (push (list kind phase role) *configured-mount-completed-cases*)))))))))))
  (setf *configured-mount-completed-cases* (nreverse *configured-mount-completed-cases*))
  (ok (equal *configured-mount-completed-cases* (configured-mount-expected-cases))))

(defun configured-mount-child-arguments (cache marker)
  (let* ((parent-uid (sb-posix:geteuid))
         (parent-namespace (configured-mount-namespace-identity))
         (form
           `(let ((*configured-mount-child-context*
                    (list :pid (sb-posix:getpid) :parent-uid ,parent-uid
                          :parent-namespace ',parent-namespace))
                  (*configured-mount-completed-cases* nil)
                  (*print-circle* t) (*print-level* 6) (*print-length* nil))
              (unless (rove:run-test
                       'reseed-configured-cancellation-refuses-real-mount-changes)
                (sb-ext:exit :code 1))
              (unless (equal *configured-mount-completed-cases*
                             (configured-mount-expected-cases))
                (error "configured mount child did not execute its complete case set"))
              (with-open-file (out ,marker :direction :output :if-exists :error)
                (write (list :pid (sb-posix:getpid)
                             :cases *configured-mount-completed-cases*
                             :complete t)
                       :stream out :readably t))
              (format t "~%CONFIGURED-MOUNT-CHILD-PASS~%"))))
    (let ((*package* (find-package :cl-user))
          (*print-length* nil) (*print-level* nil) (*print-readably* t))
      (append (butlast (retirement-intent-test-child-arguments nil nil cache nil) 2)
              (list "--eval" (write-to-string form))))))

(defun run-configured-mount-child (root &optional (timeout 1200))
  (let* ((cache (merge-pathnames "cache/" root))
         (marker (merge-pathnames "completed" root))
         (log (merge-pathnames "child.log" root))
         (arguments (configured-mount-child-arguments cache marker))
         (process nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process
                   (sb-ext:run-program
                    "unshare"
                    (append '("--user" "--map-root-user" "--mount")
                            (list (namestring sb-ext:*runtime-pathname*)) arguments)
                    :search t :wait nil :output log :error :output
                    :if-output-exists :error)))
           (format t "~%CONFIGURED-MOUNT-OWNED-CHILD ~d ~a~%"
                   (sb-ext:process-pid process) log)
           (finish-output)
           (loop with deadline = (+ (get-internal-real-time)
                                    (* timeout internal-time-units-per-second))
                 while (member (sb-ext:process-status process) '(:running :stopped))
                 do (when (>= (get-internal-real-time) deadline)
                      (format t "~%CONFIGURED-MOUNT-CHILD-TIMEOUT ~d ~d~%"
                              (sb-ext:process-pid process) timeout)
                      (finish-output)
                      (error "configured mount child exceeded ~d seconds" timeout))
                    (sleep 0.05))
           (sb-ext:process-wait process)
           (unless (and (eq :exited (sb-ext:process-status process))
                        (eql 0 (sb-ext:process-exit-code process)))
             (error "configured mount child failed (~s ~s)~%~a"
                    (sb-ext:process-status process) (sb-ext:process-exit-code process)
                    (uiop:read-file-string log)))
           (with-open-file (input marker)
             (let ((*read-eval* nil) (*package* (find-package :cl-user)))
               (ok (equal (read input)
                          (list :pid (sb-ext:process-pid process)
                                :cases (configured-mount-expected-cases)
                                :complete t)))
               (ok (eq :eof (read input nil :eof)))))
           (ok (search "CONFIGURED-MOUNT-CHILD-PASS" (uiop:read-file-string log))))
      (when process
        (when (member (sb-ext:process-status process) '(:running :stopped))
          (sb-ext:process-kill process sb-posix:sigkill))
        (sb-ext:process-wait process)
        (sb-ext:process-close process)
        (write-string (uiop:read-file-string log))
        (finish-output)))))

(deftest reseed-configured-cancellation-refuses-real-mount-changes
    (if *configured-mount-child-context*
        (check-configured-cancellation-mount-changes)
        (call-with-physical-store-directory #'run-configured-mount-child)))

(deftest configured-mount-child-timeout-reaps-and-closes-owned-process
    (with-physical-store-directory (root)
      (let ((original-arguments (symbol-function 'configured-mount-child-arguments))
            (original-run (symbol-function 'sb-ext:run-program))
            (original-close (symbol-function 'sb-ext:process-close))
            (captured nil) (closed 0) (started 0))
        (sb-ext:with-unlocked-packages (:sb-ext)
          (unwind-protect
               (progn
                 (setf (symbol-function 'configured-mount-child-arguments)
                       (lambda (cache marker)
                         (declare (ignore cache marker))
                         '("--noinform" "--no-sysinit" "--no-userinit"
                           "--non-interactive" "--eval" "(sleep 60)"))
                       (symbol-function 'sb-ext:run-program)
                       (lambda (&rest arguments)
                         (incf started)
                         (setf captured (apply original-run arguments)))
                       (symbol-function 'sb-ext:process-close)
                       (lambda (process)
                         (when (eq process captured) (incf closed))
                         (funcall original-close process)))
                 (ok (handler-case
                         (progn (run-configured-mount-child root 0) nil)
                       (error (condition)
                         (search "configured mount child exceeded"
                                 (princ-to-string condition))))))
            (setf (symbol-function 'configured-mount-child-arguments) original-arguments
                  (symbol-function 'sb-ext:run-program) original-run
                  (symbol-function 'sb-ext:process-close) original-close)))
        (ok (= 1 started))
        (ok (= 1 closed))
        (ok (eq :signaled (sb-ext:process-status captured)))
        (ok (= sb-posix:sigkill (sb-ext:process-exit-code captured))))))

(deftest configured-mount-child-completion-marker-roundtrips
    (with-physical-store-directory (root)
      (let ((original (symbol-function 'configured-mount-child-arguments)))
        (unwind-protect
             (progn
               (setf (symbol-function 'configured-mount-child-arguments)
                     (lambda (cache marker)
                       (declare (ignore cache))
                       (let ((*package* (find-package :cl-user))
                             (*print-length* nil) (*print-level* nil) (*print-readably* t))
                         (list "--noinform" "--no-sysinit" "--no-userinit" "--non-interactive"
                               "--eval" "(require :sb-posix)"
                               "--eval"
                               (write-to-string
                                `(progn
                                   (with-open-file (cl-user::configured-marker-output ,marker :direction :output :if-exists :error)
                                     (write (list :pid (sb-posix:getpid)
                                                  :cases ',(configured-mount-expected-cases)
                                                  :complete t)
                                            :stream cl-user::configured-marker-output :readably t))
                                   (format t "~%CONFIGURED-MOUNT-CHILD-PASS~%")))))))
               (run-configured-mount-child root))
          (setf (symbol-function 'configured-mount-child-arguments) original)))))
