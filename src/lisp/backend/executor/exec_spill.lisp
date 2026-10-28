;;;; exec_spill.lisp -- bounded-memory executor spill files and row stores.

(in-package #:pg)

(defparameter *query-work-memory-bytes* (* 4 1024 1024)
  "Approximate memory budget shared by one executor spool or sort operation.")

;; As in PostgreSQL's temp_file_limit, NIL means unlimited. The command line
;; can set a per-query cap; nested spill scopes share one quota.
(defparameter *query-temp-file-limit-bytes* nil
  "Maximum live temporary-file bytes per query, or NIL for no limit.")

;; *QUERY-TEMP-DIRECTORY* is a test override that names the exact directory for
;; spill files. Normal operation uses PGDATA/pgsql_tmp/<pid>-<start>-<pidns>/.
(defvar *query-temp-directory* nil)
(defvar *query-temp-root-directory* nil)
(defvar *query-temp-instance-directory* nil)
(defvar *query-temp-session-id* 0)
(defvar *query-temp-scope-directory* nil)
(defvar *query-temp-files* nil)
(defvar *query-temp-file-scope-active-p* nil)
(defvar *query-temp-file-sequence* 0)
(defvar *query-temp-session-sequence* 0)
(defvar *query-temp-file-lock*
  (sb-thread:make-mutex :name "pg query temporary file names"))
(defvar *query-temp-quota* nil)
(defvar *query-temp-test-pause-initialized-p* nil)
(defvar *query-temp-test-pause-done-p* nil)
(defvar *query-temp-test-ready-file* nil)
(defvar *query-temp-test-release-file* nil)
(defparameter +query-spill-eof+ (gensym "QUERY-SPILL-EOF-"))

(defstruct (query-temp-quota (:constructor make-query-temp-quota (limit)))
  limit
  (used-bytes 0)
  (file-sizes (make-hash-table :test #'equal))
  (stream-paths (make-hash-table :test #'eq)))

(defstruct query-temp-stream-entry
  "Scope ownership of a temporary stream and, when required, its pathname."
  pathname stream delete-p)

(defstruct query-temp-cleanup-owner
  "Retain a scope's streams, paths, quota and private directory through cleanup.

Unresolved owners remain reachable for retry by their original thread."
  thread quota entries directory temporary-directory link state failure-path failure)

(defvar *query-temp-cleanup-owner* nil)

(defvar *query-temp-pending-cleanups* nil)

(defvar *query-temp-pending-cleanup-lock*
  (sb-thread:make-mutex :name "pg pending query temp cleanup"))

(defun make-query-temp-scope-owner (quota)
  "Create the scope owner and its pending-list link before running the body."
  (let ((owner (make-query-temp-cleanup-owner
                :thread sb-thread:*current-thread* :quota quota :state :active)))
    (setf (query-temp-cleanup-owner-link owner) (list owner))
    owner))

(defstruct query-temp-namespace-report
  pathname
  owner-state
  (file-count 0)
  (size-bytes 0)
  oldest-file-age-seconds
  (scan-complete-p t))

(defun query-temp-close-fd (fd)
  (sb-posix:close fd))

(defmacro with-query-temp-fd ((fd open-form) &body body)
  "Bind FD to OPEN-FORM and close it on every exit when non-nil."
  `(let ((,fd nil))
     ;; Keep acquisition and cleanup masked so an interrupt cannot arrive after
     ;; OPEN-FORM returns but before its descriptor is bound. The body can
     ;; receive normal asynchronous interrupts once FD is bound.
     (sb-sys:without-interrupts
       (unwind-protect
            (progn
              (setf ,fd ,open-form)
              (sb-sys:with-local-interrupts
                (progn ,@body)))
         (when ,fd
           (query-temp-close-fd ,fd))))))

(defun next-query-temp-sequence ()
  (sb-thread:with-mutex (*query-temp-file-lock*)
    (incf *query-temp-file-sequence*)))

(defun next-query-temp-session-id ()
  "Allocate a unique spill namespace identifier for a client session."
  (sb-thread:with-mutex (*query-temp-file-lock*)
    (incf *query-temp-session-sequence*)))

(defun query-temp-directory-name (directory)
  (car (last (pathname-directory
              (uiop:ensure-directory-pathname directory)))))

(defun query-temp-private-directory (directory)
  (let ((directory (uiop:ensure-directory-pathname directory)))
    (ensure-directories-exist (merge-pathnames "placeholder" directory))
    (sb-posix:chmod (namestring directory) #o700)
    directory))

(defun query-temp-owner-identity (name)
  (let ((prefix-length (length "pgsql_tmp-")))
    (when (and (>= (length name) prefix-length)
               (string= "pgsql_tmp-" name :end2 prefix-length))
      (let* ((separator (position #\- name :start prefix-length))
             (namespace-separator
               (and separator (position #\- name :start (1+ separator))))
             (pid (ignore-errors
                   (parse-integer name :start prefix-length :end separator)))
             (start-token (and separator namespace-separator
                               (subseq name (1+ separator)
                                       namespace-separator)))
             (pid-namespace-token (and namespace-separator
                                       (subseq name
                                               (1+ namespace-separator)))))
        (values pid start-token pid-namespace-token)))))

(defun query-temp-whitespace-p (character)
  (find character '(#\Space #\Tab #\Newline #\Return)))

(defun query-temp-whitespace-token (string token-index)
  (let ((cursor 0)
        (length (length string)))
    (dotimes (index (1+ token-index))
      (setf cursor (position-if-not #'query-temp-whitespace-p
                                    string :start cursor))
      (unless cursor
        (return-from query-temp-whitespace-token nil))
      (let ((end (position-if #'query-temp-whitespace-p string :start cursor)))
        (when (= index token-index)
          (return-from query-temp-whitespace-token
            (subseq string cursor end)))
        (setf cursor (or end length))))))

(defun query-temp-process-start-token (pid)
  "Return Linux /proc's process start-time ticks for PID."
  (ignore-errors
   (with-open-file (stream (format nil "/proc/~d/stat" pid))
     (let* ((stat (read-line stream nil))
            ;; The command name is parenthesized and may itself contain spaces
            ;; and parentheses, so fields after it start at the final ')'.
            (command-end (and stat (position #\) stat :from-end t)))
            (fields (and command-end (subseq stat (1+ command-end))))
            ;; starttime is field 22; fields starts at field 3.
            (starttime (and fields (query-temp-whitespace-token fields 19))))
       (and starttime (parse-integer starttime))))))

(defun query-temp-pid-namespace-token ()
  (ignore-errors
   (let* ((target (sb-posix:readlink "/proc/self/ns/pid"))
          (start (position #\[ target))
          (end (and start (position #\] target :start (1+ start)))))
     (and start end (parse-integer target :start (1+ start) :end end)))))

(defun query-temp-valid-pid-p (pid)
  (and (integerp pid) (<= 1 pid #x7fffffff)))

(defun query-temp-process-state (pid)
  (unless (query-temp-valid-pid-p pid)
    (return-from query-temp-process-state :unknown))
  (handler-case
      (progn
        (sb-posix:kill pid 0)
        :live)
    (sb-posix:syscall-error (condition)
      (let ((errno (sb-posix:syscall-errno condition)))
        (cond ((= errno sb-posix:esrch) :dead)
              ((= errno sb-posix:eperm) :live)
              (t :unknown))))
    (error () :unknown)))

(defun query-temp-owner-state (pid start-token pid-namespace-token)
  "Classify an owner without treating missing process visibility as death."
  (let ((current-namespace-token (query-temp-pid-namespace-token))
        (expected-namespace-token
          (and pid-namespace-token
               (ignore-errors (parse-integer pid-namespace-token))))
        (expected-start-token
          (and start-token (ignore-errors (parse-integer start-token)))))
    (cond
      ;; PID-only, malformed, and cross-namespace entries cannot be associated
      ;; with a visible process safely. Preserve them until ownership is clear.
      ((or (null current-namespace-token)
           (null expected-namespace-token)
           (/= expected-namespace-token current-namespace-token)
           (not (query-temp-valid-pid-p pid))
           (null expected-start-token))
       :unknown)
      (t
       (case (query-temp-process-state pid)
         (:dead :dead)
         (:unknown :unknown)
         (:live
          (let ((actual-start-token (query-temp-process-start-token pid)))
            (cond ((null actual-start-token) :unknown)
                  ((= actual-start-token expected-start-token) :live)
                  (t :reused)))))))))

(defun query-temp-owner-state-for-directory-name (name)
  (if (and name
           (>= (length name) (length "pgsql_tmp-"))
           (string= "pgsql_tmp-" name :end2 (length "pgsql_tmp-")))
      (multiple-value-bind (pid start-token pid-namespace-token)
          (query-temp-owner-identity name)
        (if pid
            (query-temp-owner-state pid start-token pid-namespace-token)
            :unknown))
      :not-query-temp))

(defun query-temp-directory-owner-state (directory)
  "Classify a spill namespace name, preserving unparseable ownership as unknown."
  (query-temp-owner-state-for-directory-name
   (if (stringp directory)
       directory
       (query-temp-directory-name directory))))

(defun query-temp-lstat (pathname)
  "Lstat PATHNAME without following a trailing-directory symlink."
  (let* ((name (if (stringp pathname) pathname (namestring pathname)))
         (trimmed (string-right-trim "/" name)))
    (ignore-errors
     (sb-posix:lstat (if (plusp (length trimmed)) trimmed "/")))))

(defun query-temp-open-directory (pathname)
  "Open PATHNAME as a pinned directory, refusing a symlink at its final part."
  (let* ((name (string-right-trim "/" (namestring pathname)))
         (flags (logior sb-posix:o-rdonly sb-posix:o-directory
                        sb-posix:o-nofollow)))
    (ignore-errors
     (sb-posix:open (if (plusp (length name)) name "/") flags))))

(defun query-temp-fd-path (fd)
  (format nil "/proc/self/fd/~d" fd))

(defun query-temp-fd-child-path (fd name)
  (format nil "/proc/self/fd/~d/~a" fd name))

(defun query-temp-open-child-directory (fd name)
  "Open NAME below pinned directory FD without following a symlink."
  (ignore-errors
   (sb-posix:open
    (query-temp-fd-child-path fd name)
    (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow))))

(defun query-temp-open-child-file (fd name)
  "Open a regular-file candidate below pinned directory FD without blocking."
  (ignore-errors
   (sb-posix:open
    (query-temp-fd-child-path fd name)
    (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock))))

(defun query-temp-directory-entry-names (fd)
  "List immediate names from pinned directory FD; report unreadable handles."
  (handler-case
      (let ((directory (sb-posix:opendir (query-temp-fd-path fd)))
            (names nil))
        (unwind-protect
             (progn
               (loop for entry = (sb-posix:readdir directory)
                     for entry-sap = (sb-alien:alien-sap entry)
                     until (zerop (sb-sys:sap-int entry-sap))
                     for name = (sb-posix:dirent-name entry)
                     unless (member name '("." "..") :test #'string=)
                       do (push name names))
               (values (nreverse names) t))
          (sb-posix:closedir directory)))
    (error ()
      (values nil nil))))

(defun query-temp-directory-content-stats (directory-fd now)
  "Count regular files below pinned DIRECTORY-FD without following symlinks."
  (let ((file-count 0)
        (size-bytes 0)
        (oldest-mtime nil)
        (complete-p t))
    (labels ((visit (fd)
               (multiple-value-bind (names opened-p)
                   (query-temp-directory-entry-names fd)
                 (unless opened-p
                   (setf complete-p nil))
                 (dolist (name names)
                   (let* ((stat (query-temp-lstat
                                 (query-temp-fd-child-path fd name)))
                          (mode (and stat (sb-posix:stat-mode stat))))
                     (cond
                       ((null stat)
                        (setf complete-p nil))
                       ((sb-posix:s-islnk mode))
                       ((sb-posix:s-isdir mode)
                        (with-query-temp-fd
                            (child-fd (query-temp-open-child-directory fd name))
                          (if child-fd
                              (if (sb-posix:s-isdir
                                   (sb-posix:stat-mode
                                    (sb-posix:fstat child-fd)))
                                  (visit child-fd)
                                  (setf complete-p nil))
                              (setf complete-p nil))))
                       ((sb-posix:s-isreg mode)
                        (with-query-temp-fd
                            (file-fd (query-temp-open-child-file fd name))
                          (if file-fd
                              (let ((file-stat (sb-posix:fstat file-fd)))
                                (if (sb-posix:s-isreg
                                     (sb-posix:stat-mode file-stat))
                                    (progn
                                      (incf file-count)
                                      (incf size-bytes
                                            (sb-posix:stat-size file-stat))
                                      (setf oldest-mtime
                                            (if oldest-mtime
                                                (min oldest-mtime
                                                     (sb-posix:stat-mtime
                                                      file-stat))
                                                (sb-posix:stat-mtime
                                                 file-stat))))
                                    (setf complete-p nil)))
                              (setf complete-p nil))))))))))
      (visit directory-fd))
    (values file-count size-bytes
            (and oldest-mtime (max 0 (- now oldest-mtime)))
            complete-p)))

(defun query-temp-child-pathname (root name)
  (make-pathname
   :directory (append (pathname-directory
                       (uiop:ensure-directory-pathname root))
                      (list name))
   :name nil :type nil :version nil))

(defun collect-retained-query-temp-stats (root now owner-classifier)
  "Report preserved namespaces with a no-follow, descriptor-pinned file walk."
  (with-query-temp-fd (root-fd (query-temp-open-directory root))
    (unless root-fd
      (return-from collect-retained-query-temp-stats nil))
    (multiple-value-bind (names opened-p)
        (query-temp-directory-entry-names root-fd)
      (unless opened-p
        (return-from collect-retained-query-temp-stats nil))
      (sort
       (loop for name in names
             for owner-state = (funcall owner-classifier name)
             when (member owner-state '(:unknown :live))
               collect
               (with-query-temp-fd
                   (directory-fd
                    (query-temp-open-child-directory root-fd name))
                 (if directory-fd
                     (multiple-value-bind
                           (file-count size-bytes
                            oldest-file-age-seconds scan-complete-p)
                         (query-temp-directory-content-stats directory-fd now)
                       (make-query-temp-namespace-report
                        :pathname (query-temp-child-pathname root name)
                        :owner-state owner-state
                        :file-count file-count
                        :size-bytes size-bytes
                        :oldest-file-age-seconds oldest-file-age-seconds
                        :scan-complete-p scan-complete-p))
                     (make-query-temp-namespace-report
                      :pathname (query-temp-child-pathname root name)
                      :owner-state owner-state
                      :scan-complete-p nil))))
       #'string< :key (lambda (report)
                        (namestring
                         (query-temp-namespace-report-pathname report)))))))

(defun report-retained-query-temp-namespaces (root)
  "Write a startup diagnostic for preserved spill namespaces."
  (let* ((now (- (get-universal-time) 2208988800))
         (reports
           (collect-retained-query-temp-stats
            root now #'query-temp-directory-owner-state)))
    (dolist (report reports)
      (format *error-output*
              "pg-lisp: retaining query temp namespace ~a (owner=~(~a~), files=~d, bytes=~d, oldest-file-age=~a~a)~%"
              (query-temp-namespace-report-pathname report)
              (query-temp-namespace-report-owner-state report)
              (query-temp-namespace-report-file-count report)
              (query-temp-namespace-report-size-bytes report)
              (or (query-temp-namespace-report-oldest-file-age-seconds report)
                  "unknown")
              (if (query-temp-namespace-report-scan-complete-p report)
                  ""
                  ", scan=incomplete")))
    reports))

(defun remove-abandoned-query-temp-namespaces
    (root &optional (current-pid (sb-posix:getpid)))
  "Remove only server-owned namespaces below ROOT, preserving other entries."
  (when (probe-file root)
    (dolist (directory (uiop:subdirectories root))
      (let ((name (query-temp-directory-name directory)))
        (multiple-value-bind (owner-pid start-token pid-namespace-token)
            (if name
                (query-temp-owner-identity name)
                (values nil nil nil))
          (declare (ignore start-token))
          (let ((owner-state (query-temp-directory-owner-state directory))
                (same-current-namespace-p
                  (and owner-pid (= owner-pid current-pid)
                       pid-namespace-token
                       (ignore-errors
                        (= (parse-integer pid-namespace-token)
                           (query-temp-pid-namespace-token))))))
            ;; A current-PID namespace is stale at startup only when its PID
            ;; namespace also matches. Unknown ownership is preserved.
            (when (or same-current-namespace-p
                      (member owner-state '(:dead :reused)))
              (uiop:delete-directory-tree directory
                                          :validate t
                                          :if-does-not-exist :ignore)))))))
  (values))

(defun initialize-query-temp-storage (&optional (pgdata *pgdata*))
  "Prepare PGDATA's spill root and discard namespaces left by a prior server."
  (unless pgdata
    (error "Cannot initialize query spill storage without PGDATA"))
  (let* ((root (merge-pathnames "pgsql_tmp/"
                                (uiop:ensure-directory-pathname
                                 (pathname pgdata))))
         (pid (sb-posix:getpid))
         (start-token (query-temp-process-start-token pid))
         (pid-namespace-token (query-temp-pid-namespace-token))
         (instance nil))
    (unless (and start-token pid-namespace-token)
      (error "Cannot determine query spill owner identity for process ~d" pid))
    (setf instance
          (merge-pathnames
           (format nil "pgsql_tmp-~d-~d-~d/"
                   pid start-token pid-namespace-token)
           root))
    (query-temp-private-directory root)
    ;; PostgreSQL does this in postmaster startup, when no other server using
    ;; the data directory can be running. This server has one process and
    ;; thread-per-session backends, so each child namespace is session-local.
    (remove-abandoned-query-temp-namespaces root)
    (report-retained-query-temp-namespaces root)
    (setf *query-temp-root-directory* root
          *query-temp-instance-directory* instance)
    instance))

(defun create-query-temp-scope-directory (directory)
  "Create a private directory exclusively; do not adopt an existing name."
  (sb-posix:mkdir (namestring directory) #o700)
  directory)

(defun query-temp-scope-directory ()
  "Return the configured directory or create and publish a private scope directory."
  (or *query-temp-scope-directory*
      (if *query-temp-directory*
          (setf *query-temp-scope-directory*
                (uiop:ensure-directory-pathname (pathname *query-temp-directory*)))
          (let* ((instance (or *query-temp-instance-directory* (initialize-query-temp-storage)))
                 (scope (merge-pathnames
                         (format nil "session-~d-query-~d/"
                                 *query-temp-session-id* (next-query-temp-sequence))
                         instance)))
            (query-temp-private-directory instance)
            (sb-sys:without-interrupts
              (create-query-temp-scope-directory scope)
              (setf *query-temp-scope-directory* scope
                    (query-temp-cleanup-owner-directory *query-temp-cleanup-owner*) scope))))))

(defun unlink-query-temp-file (pathname)
  (delete-file pathname))

(defun forget-query-temp-file-space (pathname)
  (when *query-temp-quota*
    (let* ((quota *query-temp-quota*)
           (size (gethash pathname (query-temp-quota-file-sizes quota) 0)))
      (decf (query-temp-quota-used-bytes quota) size)
      (remhash pathname (query-temp-quota-file-sizes quota))
      (let ((streams nil))
        (maphash (lambda (stream path)
                   (when (equal path pathname)
                     (push stream streams)))
                 (query-temp-quota-stream-paths quota))
        (dolist (stream streams)
          (remhash stream (query-temp-quota-stream-paths quota))))))
  (values))

(defun report-query-temp-cleanup-failure (pathname condition)
  "Report a cleanup failure without replacing the exit being cleaned up."
  (handler-case
      (format *error-output* "pg-lisp: query temp cleanup failed for ~a: ~a~%" pathname condition)
    (serious-condition () nil))
  (values))

(defun call-with-query-temp-cleanup (pathname function)
  "Run cleanup and retain or report its failure without replacing the original exit."
  (labels ((failed (condition)
             (if *query-temp-cleanup-owner*
                 (setf (query-temp-cleanup-owner-failure-path *query-temp-cleanup-owner*) pathname
                       (query-temp-cleanup-owner-failure *query-temp-cleanup-owner*) condition)
                 (report-query-temp-cleanup-failure pathname condition))
             nil))
    (handler-case (funcall function)
      (sb-ext:timeout (condition) (failed condition))
      (error (condition) (failed condition)))))

(defun query-temp-file-absent-p (pathname)
  "Return true only when lstat confirms PATHNAME is absent."
  (handler-case
      (progn (sb-posix:lstat (namestring pathname)) nil)
    (sb-posix:syscall-error (condition)
      (if (= (sb-posix:syscall-errno condition) sb-posix:enoent)
          t
          (error condition)))))

(defun query-temp-stream-open-p (stream)
  "Treat an unknown stream state as open for cleanup."
  (handler-case (open-stream-p stream)
    (serious-condition () t)))

(defun query-temp-entry-open-p (entry)
  "Return whether ENTRY still has a stream that may be open."
  (let ((stream (query-temp-stream-entry-stream entry)))
    (and stream (query-temp-stream-open-p stream))))

(defun prune-query-temp-stream-entries ()
  "Discard entries with neither a stream nor a pathname left to delete."
  (when *query-temp-cleanup-owner*
    (setf (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*)
          (remove-if (lambda (entry)
                       (and (null (query-temp-stream-entry-stream entry))
                            (not (query-temp-stream-entry-delete-p entry))))
                     (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))))
  (values))

(defun discharge-query-temp-stream (stream)
  "Retire confirmed-closed stream references; keep paths still awaiting deletion."
  (when (and stream (not (query-temp-stream-open-p stream)))
    (when *query-temp-quota*
      (remhash stream (query-temp-quota-stream-paths *query-temp-quota*)))
    (when *query-temp-cleanup-owner*
      (dolist (entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
        (when (eq stream (query-temp-stream-entry-stream entry))
          (setf (query-temp-stream-entry-stream entry) nil))))
    (prune-query-temp-stream-entries))
  (values))

(defun abort-query-temp-stream (stream pathname)
  "Make bounded abort-close attempts without a deadline, then discharge if closed.

Use the stream API; unresolved streams remain owned for later cleanup."
  (when stream
    (sb-sys:without-interrupts
      (sb-sys:with-deadline (:seconds nil :override t)
        (loop repeat 2
              while (query-temp-stream-open-p stream)
              do (call-with-query-temp-cleanup pathname (lambda () (close stream :abort t)))))))
  (discharge-query-temp-stream stream)
  (not (and stream (query-temp-stream-open-p stream))))

(defun pending-query-temp-stream-open-p (pathname)
  "Check other pending owners with the same quota for a possibly open stream."
  (let ((owners
          (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
            (loop for owner in *query-temp-pending-cleanups*
                  when (and (not (eq owner *query-temp-cleanup-owner*))
                            (eq (query-temp-cleanup-owner-quota owner) *query-temp-quota*))
                    collect owner))))
    (some (lambda (owner)
            (some (lambda (entry)
                    (and (equal pathname (query-temp-stream-entry-pathname entry))
                         (query-temp-entry-open-p entry)))
                  (query-temp-cleanup-owner-entries owner)))
          owners)))

(defun close-query-temp-file-streams (pathname)
  "Close this owner's streams and check all known streams before deleting PATHNAME.

A foreign pending owner can block deletion; this function does not close it."
  (when *query-temp-cleanup-owner*
    (dolist (entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
      (when (equal pathname (query-temp-stream-entry-pathname entry))
        (abort-query-temp-stream (query-temp-stream-entry-stream entry) pathname))))
  (and (not (pending-query-temp-stream-open-p pathname))
       (or (null *query-temp-cleanup-owner*)
           (notany (lambda (entry)
                     (and (equal pathname (query-temp-stream-entry-pathname entry))
                          (query-temp-entry-open-p entry)))
                   (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*)))
       (or (null *query-temp-quota*)
           (loop for stream being the hash-keys of (query-temp-quota-stream-paths *query-temp-quota*)
                   using (hash-value path)
                 never (and (equal path pathname) (query-temp-stream-open-p stream))))))

(defun delete-query-temp-file (pathname)
  "Delete PATHNAME only after its streams close, then release its quota charge.

A failed deletion retains ownership unless lstat confirms the file is absent."
  (sb-sys:without-interrupts
    (sb-sys:with-deadline (:seconds nil :override t)
      (when (and (close-query-temp-file-streams pathname)
                 (or (call-with-query-temp-cleanup
                      pathname (lambda () (unless (query-temp-file-absent-p pathname)
                                            (unlink-query-temp-file pathname))))
                     (call-with-query-temp-cleanup
                      pathname (lambda () (query-temp-file-absent-p pathname)))))
        (setf *query-temp-files* (delete pathname *query-temp-files* :test #'equal))
        (forget-query-temp-file-space pathname)
        (when *query-temp-cleanup-owner*
          (dolist (entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
            (when (equal pathname (query-temp-stream-entry-pathname entry))
              (setf (query-temp-stream-entry-delete-p entry) nil)))))))
  (prune-query-temp-stream-entries)
  (values))

(defun cleanup-query-temp-files ()
  "Make bounded cleanup attempts for the current owner without a deadline.

Retain unresolved files and their charges.  Do not remove a configured shared
temporary directory."
  (sb-sys:without-interrupts
    (sb-sys:with-deadline (:seconds nil :override t)
      (loop repeat 2 do
        (dolist (entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
          (when (query-temp-stream-entry-delete-p entry)
            (let ((pathname (query-temp-stream-entry-pathname entry)))
              (call-with-query-temp-cleanup pathname (lambda () (delete-query-temp-file pathname))))))
        (dolist (entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
          (let ((stream (query-temp-stream-entry-stream entry))
                (path (query-temp-stream-entry-pathname entry)))
            (when (and stream (not (query-temp-stream-entry-delete-p entry)))
              (abort-query-temp-stream stream path)
              (unless (query-temp-stream-open-p stream)
                (remhash stream (query-temp-quota-stream-paths *query-temp-quota*)))))))
      (when (and (notany #'query-temp-stream-entry-delete-p
                         (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
                 *query-temp-scope-directory* (null *query-temp-directory*))
        (call-with-query-temp-cleanup
         *query-temp-scope-directory*
         (lambda ()
           (uiop:delete-directory-tree *query-temp-scope-directory*
                                       :validate t :if-does-not-exist :ignore)))
        (when (call-with-query-temp-cleanup
               *query-temp-scope-directory*
               (lambda () (query-temp-file-absent-p *query-temp-scope-directory*)))
          (setf *query-temp-scope-directory* nil)))))
  (values))

(defun query-temp-owner-unresolved-p (owner)
  "Return whether OWNER still holds a stream, pathname or private directory."
  (or (some (lambda (entry)
              (or (query-temp-stream-entry-delete-p entry)
                  (query-temp-entry-open-p entry)))
            (query-temp-cleanup-owner-entries owner))
      (and (query-temp-cleanup-owner-directory owner)
           (null (query-temp-cleanup-owner-temporary-directory owner)))))

(defun run-query-temp-owner-cleanup (owner)
  "Clean OWNER while it remains reachable in the pending list.

The pending lock protects list and state changes, not file I/O."
  (let ((*query-temp-cleanup-owner* owner)
        (*query-temp-quota* (query-temp-cleanup-owner-quota owner))
        (*query-temp-scope-directory* (query-temp-cleanup-owner-directory owner))
        (*query-temp-directory* (query-temp-cleanup-owner-temporary-directory owner))
        (*query-temp-files* nil))
    (setf (query-temp-cleanup-owner-failure owner) nil)
    (unwind-protect
         (call-with-query-temp-cleanup
          *query-temp-scope-directory* #'cleanup-query-temp-files)
      (setf (query-temp-cleanup-owner-directory owner) *query-temp-scope-directory*)
      (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
        (if (query-temp-owner-unresolved-p owner)
            (setf (query-temp-cleanup-owner-state owner) :pending)
            (progn
              (setf *query-temp-pending-cleanups*
                    (delete owner *query-temp-pending-cleanups* :test #'eq))
              (setf (cdr (query-temp-cleanup-owner-link owner)) nil
                    (query-temp-cleanup-owner-state owner) :resolved)))))
    (when (query-temp-cleanup-owner-failure owner)
      (report-query-temp-cleanup-failure
       (query-temp-cleanup-owner-failure-path owner) (query-temp-cleanup-owner-failure owner))))
  (values))

(defun finish-query-temp-scope ()
  "Publish the preallocated pending owner before cleanup or failure reporting."
  (sb-sys:without-interrupts
    (sb-sys:with-deadline (:seconds nil :override t)
      (let ((owner *query-temp-cleanup-owner*))
        (setf (query-temp-cleanup-owner-directory owner) *query-temp-scope-directory*)
        (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
          (setf (query-temp-cleanup-owner-state owner) :cleaning
                (cdr (query-temp-cleanup-owner-link owner)) *query-temp-pending-cleanups*
                *query-temp-pending-cleanups* (query-temp-cleanup-owner-link owner)))
        (run-query-temp-owner-cleanup owner)))))

(defun retry-pending-query-temp-cleanup ()
  "Retry pending cleanup owned by this thread; leave other owners untouched.

Owners of dead threads remain reachable until process exit."
  (sb-sys:without-interrupts
    (sb-sys:with-deadline (:seconds nil :override t)
      (let ((owners
              (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
                (loop for owner in *query-temp-pending-cleanups*
                      when (and (eq (query-temp-cleanup-owner-thread owner) sb-thread:*current-thread*)
                                (eq (query-temp-cleanup-owner-state owner) :pending))
                        collect owner))))
        (dolist (owner owners)
          (when (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
                  (when (eq (query-temp-cleanup-owner-state owner) :pending)
                    (setf (query-temp-cleanup-owner-state owner) :cleaning)))
            (run-query-temp-owner-cleanup owner))))))
  (values))

(defun ensure-query-temp-acquisition-ready ()
  "Require a scope owner and reject new files while this thread has pending cleanup."
  (when (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
          (some (lambda (owner)
                  (eq (query-temp-cleanup-owner-thread owner) sb-thread:*current-thread*))
                *query-temp-pending-cleanups*))
    (ereport "58030" "previous temporary file cleanup remains incomplete"))
  (unless *query-temp-cleanup-owner*
    (error "query temporary stream requires a scope owner")))

(defun query-temp-pending-cleanup-status (&optional (limit 10))
  "Return pending counts and up to LIMIT paths.

Unknown stream states count as open. Paths can repeat; TRUNCATED means some
paths were omitted."
  (check-type limit (integer 0 1000))
  (sb-thread:with-mutex (*query-temp-pending-cleanup-lock*)
    (let ((records 0) (streams 0) (paths nil) (listed 0) (truncated nil))
      (dolist (owner *query-temp-pending-cleanups*)
        (incf records)
        (dolist (entry (query-temp-cleanup-owner-entries owner))
          (let ((open (query-temp-entry-open-p entry)))
            (when open (incf streams))
            (when (or open (query-temp-stream-entry-delete-p entry))
              (if (< listed limit)
                  (progn (push (query-temp-stream-entry-pathname entry) paths) (incf listed))
                  (setf truncated t))))))
      (list :records records :open-streams streams :paths (nreverse paths) :truncated truncated))))

(defmacro with-query-temp-files (&body body)
  "Own temporary streams and files at the outermost scope, including callbacks.

Preallocate fallback ownership before BODY.  On exit, publish the owner and
attempt bounded cleanup without replacing an error, FATAL or nonlocal exit."
  `(if *query-temp-file-scope-active-p*
       (progn ,@body)
       (let* ((*query-temp-files* nil)
              (*query-temp-file-scope-active-p* t)
              (*query-temp-scope-directory* nil)
              (*query-temp-quota* (or *query-temp-quota*
                                      (make-query-temp-quota *query-temp-file-limit-bytes*)))
              (*query-temp-cleanup-owner* (make-query-temp-scope-owner *query-temp-quota*)))
         (setf (query-temp-cleanup-owner-temporary-directory *query-temp-cleanup-owner*)
               *query-temp-directory*)
         (sb-sys:without-interrupts
           (unwind-protect
                (progn
                  (retry-pending-query-temp-cleanup)
                  (sb-sys:with-local-interrupts (progn ,@body)))
             (finish-query-temp-scope))))))

(defun query-temp-file-pathname ()
  (let* ((sequence (next-query-temp-sequence))
         (directory (query-temp-scope-directory))
         (name (format nil "pg-query-~36r-~36r-~36r.tmp"
                       (get-universal-time) (sb-posix:getpid) sequence)))
    (merge-pathnames name directory)))

(defun register-query-temp-file (pathname)
  (unless *query-temp-file-scope-active-p*
    (error "Query spill files must be created inside WITH-QUERY-TEMP-FILES"))
  (push pathname *query-temp-files*)
  (when *query-temp-quota*
    (setf (gethash pathname (query-temp-quota-file-sizes *query-temp-quota*)) 0))
  pathname)

(defun register-query-temp-stream-path (stream pathname)
  "Record the stream's path for quota accounting and lookup.

The scope entry, not this mapping alone, owns the stream."
  (setf (gethash stream (query-temp-quota-stream-paths *query-temp-quota*)) pathname))

(defun open-and-register-query-temp-file (pathname open-arguments)
  "Open PATHNAME with ownership reserved before the open.

Publish the actual stream under interrupt masking before fallible registration."

  (ensure-query-temp-acquisition-ready)
  (let ((entry (make-query-temp-stream-entry :pathname pathname))
        (registered-p nil))
    (push entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
    (sb-sys:without-interrupts
      (unwind-protect
           (let ((stream (apply #'open pathname open-arguments)))
             (when stream
               (setf (query-temp-stream-entry-stream entry) stream
                     (query-temp-stream-entry-delete-p entry) t)
               (register-query-temp-file pathname)
               (register-query-temp-stream-path stream pathname)
               (setf registered-p t))
             stream)
        (unless registered-p
          (abort-query-temp-stream (query-temp-stream-entry-stream entry) pathname)
          (prune-query-temp-stream-entries))))))

(defun open-query-temp-output ()
  (unless *query-temp-file-scope-active-p*
    (error "Query spill files must be created inside WITH-QUERY-TEMP-FILES"))
  (loop repeat 100
        for pathname = (query-temp-file-pathname)
        for stream = (open-and-register-query-temp-file
                      pathname
                      '(:direction :output :if-exists nil
                        :if-does-not-exist :create :external-format :utf-8))
        when stream
          do (return-from open-query-temp-output (values pathname stream)))
  (error "Could not create a unique query spill file"))

(defun open-query-temp-io ()
  (unless *query-temp-file-scope-active-p*
    (error "Query spill files must be created inside WITH-QUERY-TEMP-FILES"))
  (loop repeat 100
        for pathname = (query-temp-file-pathname)
        for stream = (open-and-register-query-temp-file
                      pathname
                      '(:direction :io :element-type (unsigned-byte 8)
                        :if-exists nil :if-does-not-exist :create))
        when stream
          do (return-from open-query-temp-io (values pathname stream)))
  (error "Could not create a unique query spill file"))

(defun open-query-temp-input (pathname)
  "Reserve an entry and publish the input stream before registering its path."
  (ensure-query-temp-acquisition-ready)
  (let ((entry (make-query-temp-stream-entry :pathname pathname)))
    (push entry (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*))
    (sb-sys:without-interrupts
      (let ((stream (open pathname :direction :input :external-format :utf-8)))
        (setf (query-temp-stream-entry-stream entry) stream)
        (register-query-temp-stream-path stream pathname)
        stream))))

(defun query-temp-owned-stream-pathname (stream)
  "Find STREAM's path in explicit ownership or the quota lookup."
  (or (when *query-temp-cleanup-owner*
        (let ((entry (find stream (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*)
                           :key #'query-temp-stream-entry-stream :test #'eq)))
          (when entry (query-temp-stream-entry-pathname entry))))
      (when *query-temp-quota*
        (gethash stream (query-temp-quota-stream-paths *query-temp-quota*)))))

(defun finish-query-temp-stream (stream normal-p)
  "Close normally on success, or make guarded abort cleanup on abnormal exit.

Normal close errors propagate; a still-open stream remains owned."
  (when stream
    (if normal-p
        (unwind-protect (close stream) (discharge-query-temp-stream stream))
        (abort-query-temp-stream stream (query-temp-owned-stream-pathname stream))))
  (values))

(defun call-with-query-temp-stream-cleanup (function closer)
  "Call FUNCTION with cleanup installed and tell CLOSER whether it returned normally."
  (let ((normal-p nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (multiple-value-prog1 (sb-sys:with-local-interrupts (funcall function))
             (setf normal-p t))
        (funcall closer normal-p)))))

(defmacro with-query-temp-input ((stream pathname) &body body)
  "Open an owned input stream for BODY and attempt closure on every exit.

The scope retains ownership if closure cannot be confirmed."
  (let ((normal (gensym "NORMAL-")))
    `(let ((,stream nil))
       (call-with-query-temp-stream-cleanup
        (lambda ()
          (sb-sys:without-interrupts (setf ,stream (open-query-temp-input ,pathname)))
          ,@body)
        (lambda (,normal) (finish-query-temp-stream ,stream ,normal))))))

(defmacro with-query-temp-output ((pathname stream) &body body)
  "Own an output stream before opening it; the surrounding file scope owns PATHNAME.

Normal close errors propagate; abnormal cleanup preserves the original exit."
  (let ((normal (gensym "NORMAL-")))
    `(let ((,pathname nil) (,stream nil))
       (declare (ignorable ,pathname))
       (call-with-query-temp-stream-cleanup
        (lambda ()
          (sb-sys:without-interrupts
            (multiple-value-setq (,pathname ,stream) (open-query-temp-output)))
          ,@body)
        (lambda (,normal) (finish-query-temp-stream ,stream ,normal))))))

(defun query-spill-value-size (value)
  "Estimate retained row memory without building its printed representation."
  (cond
    ((sql-decimal-p value) (+ 32 (query-spill-value-size (sql-decimal-coefficient value))))
    ((sql-record-p value)
     (+ 64 (query-spill-value-size (sql-record-fields value))
        (query-spill-value-size (sql-record-values value))))
    ((stringp value)
     (+ 40 (* 4 (length value))))
    ((and (vectorp value) (typep value '(vector (unsigned-byte 8))))
     (+ 64 (length value)))
    ((vectorp value)
     (+ 64 (loop for element across value
                 sum (query-spill-value-size element))))
    ((consp value)
     (+ 48 (query-spill-value-size (car value))
        (query-spill-value-size (cdr value))))
    ((symbolp value)
     (+ 32 (* 2 (length (symbol-name value)))))
    ((integerp value)
     (+ 16 (ceiling (integer-length value) 8)))
    ((rationalp value)
     (+ 32 (query-spill-value-size (numerator value))
        (query-spill-value-size (denominator value))))
    ((floatp value) 32)
    ((complexp value)
     (+ 32 (query-spill-value-size (realpart value))
        (query-spill-value-size (imagpart value))))
    ((characterp value) 8)
    (t 64)))

(defun query-spill-decode (value)
  (cond
    ((and (consp value) (eq (first value) :pg-query-spill-numeric))
     (make-sql-decimal (second value) (third value)))
    ((and (consp value) (eq (first value) :pg-query-spill-record))
     (make-sql-record :fields (query-spill-decode (second value))
                      :values (query-spill-decode (third value))))
    ((and (consp value) (eq (first value) :pg-query-spill-vector))
     (let* ((element-type (second value))
            (elements (third value)))
       (if (equal (array-element-type elements) element-type)
           (progn
             (dotimes (index (length elements))
               (setf (aref elements index)
                     (query-spill-decode (aref elements index))))
             elements)
           (make-array (length elements)
                       :element-type element-type
                       :initial-contents
                       (map 'vector #'query-spill-decode elements)))))
    ((consp value)
     (cons (query-spill-decode (car value))
           (query-spill-decode (cdr value))))
    (t value)))

(defun query-spill-record-size (value)
  (+ 64 (query-spill-value-size value)))

(defun query-spill-write-value (stream value)
  ;; Preserve coefficient and display scale across spill and replay, including
  ;; decimals nested in composite rows, without evaluating reader forms.
  (cond
    ((sql-decimal-p value)
     (format stream "(:pg-query-spill-numeric ~d ~d)" (sql-decimal-coefficient value) (sql-decimal-scale value)))
    ((sql-record-p value)
     (write-string "(:pg-query-spill-record " stream)
     (query-spill-write-value stream (sql-record-fields value))
     (write-char #\Space stream)
     (query-spill-write-value stream (sql-record-values value))
     (write-char #\) stream))
    ((stringp value)
     (write value :stream stream))
    ((vectorp value)
     (write-string "(:pg-query-spill-vector " stream)
     (write (array-element-type value) :stream stream)
     (write-char #\Space stream)
     (if (typep value '(vector (unsigned-byte 8)))
         (progn
           (write-string "#U8(" stream)
           (loop with digits = "0123456789ABCDEF"
                 for byte across value
                 do (write-char (char digits (ash byte -4)) stream)
                    (write-char (char digits (logand byte #x0f)) stream))
           (write-char #\) stream))
         (progn
           (write-string "#(" stream)
           (loop for element across value
                 for first-p = t then nil
                 do (unless first-p (write-char #\Space stream))
                    (query-spill-write-value stream element))
           (write-char #\) stream)))
     (write-char #\) stream))
    ((consp value)
     (write-char #\( stream)
     (loop for tail = value then (cdr tail)
           for first-p = t then nil
           do (unless first-p (write-char #\Space stream))
              (query-spill-write-value stream (car tail))
           when (not (consp (cdr tail)))
             do (unless (null (cdr tail))
                  (write-string " . " stream)
                  (query-spill-write-value stream (cdr tail)))
                (return))
     (write-char #\) stream))
    (t
     (write value :stream stream))))

(defun query-spill-hex-digit (character)
  (position (char-upcase character) "0123456789ABCDEF"))

(defun read-query-spill-byte-vector (stream subchar argument)
  (declare (ignore subchar argument))
  (unless (char= (read-char stream) #\8)
    (error "Malformed query spill byte vector"))
  (unless (char= (read-char stream) #\()
    (error "Malformed query spill byte vector"))
  (let ((bytes (make-array 0 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
        (high-nibble nil))
    (loop for character = (read-char stream nil nil)
          do (cond
               ((null character)
                (error "Unterminated query spill byte vector"))
               ((char= character #\))
                (when high-nibble
                  (error "Odd number of hex digits in query spill byte vector"))
                (return))
               ((find character '(#\Space #\Tab #\Newline #\Return)))
               ((null high-nibble)
                (setf high-nibble (query-spill-hex-digit character))
                (unless high-nibble
                  (error "Invalid hex digit in query spill byte vector")))
               (t
                (let ((low-nibble (query-spill-hex-digit character)))
                  (unless low-nibble
                    (error "Invalid hex digit in query spill byte vector"))
                  (vector-push-extend (+ (ash high-nibble 4) low-nibble) bytes)
                  (setf high-nibble nil)))))
    bytes))

(defparameter *query-spill-readtable*
  (let ((readtable (copy-readtable nil)))
    (set-dispatch-macro-character #\# #\u
                                  #'read-query-spill-byte-vector readtable)
    (set-dispatch-macro-character #\# #\U
                                  #'read-query-spill-byte-vector readtable)
    readtable))

(defun query-temp-path-for-stream (stream)
  (and *query-temp-quota*
       (gethash stream (query-temp-quota-stream-paths *query-temp-quota*))))

(defun query-temp-character-utf8-size (character)
  (let ((code (char-code character)))
    (cond
      ((<= code #x7f) 1)
      ((<= code #x7ff) 2)
      ((<= code #xffff) 3)
      (t 4))))

(defun query-temp-string-utf8-size (string start end)
  (loop for index from start below end
        sum (query-temp-character-utf8-size (char string index))))

(defun query-temp-check-space (pathname bytes)
  (when (and pathname *query-temp-quota*)
    (let* ((quota *query-temp-quota*)
           (limit (query-temp-quota-limit quota))
           (used (query-temp-quota-used-bytes quota)))
      (when (and limit (> (+ used bytes) limit))
        (ereport "53400"
                 "temporary file size exceeds temp_file_limit (~d bytes)"
                 limit))))
  (values))

(defun query-temp-account-space (pathname bytes &optional stream)
  (when (and pathname *query-temp-quota*)
    (incf (query-temp-quota-used-bytes *query-temp-quota*) bytes)
    (incf (gethash pathname
                   (query-temp-quota-file-sizes *query-temp-quota*)
                   0)
          bytes)
    (query-temp-test-pause-after-write stream))
  (values))

(defun query-temp-test-pause-after-write (&optional stream)
  "Test hook used to SIGKILL the server after it has written a spill record."
  (unless *query-temp-test-pause-initialized-p*
    (setf *query-temp-test-ready-file*
          (sb-ext:posix-getenv "PG_QUERY_TEMP_TEST_READY_FILE")
          *query-temp-test-release-file*
          (sb-ext:posix-getenv "PG_QUERY_TEMP_TEST_RELEASE_FILE")
          *query-temp-test-pause-initialized-p* t))
  (when (and *query-temp-test-ready-file*
             *query-temp-test-release-file*
             (not *query-temp-test-pause-done-p*))
    (setf *query-temp-test-pause-done-p* t)
    (when stream
      (finish-output stream))
    (with-open-file (stream *query-temp-test-ready-file*
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-line "spill-written" stream))
    (loop until (probe-file *query-temp-test-release-file*)
          do (sleep 0.01)))
  (values))

(defclass query-temp-limited-character-stream
    (sb-gray:fundamental-character-output-stream)
  ((stream :initarg :stream :reader query-temp-limited-underlying-stream)
   (pathname :initarg :pathname :reader query-temp-limited-pathname)))

(defmethod sb-gray:stream-write-char
    ((stream query-temp-limited-character-stream) character)
  (let* ((pathname (query-temp-limited-pathname stream))
         (bytes (query-temp-character-utf8-size character)))
    (query-temp-check-space pathname bytes)
    (write-char character (query-temp-limited-underlying-stream stream))
    (query-temp-account-space
     pathname bytes (query-temp-limited-underlying-stream stream)))
  character)

(defmethod sb-gray:stream-write-string
    ((stream query-temp-limited-character-stream) string
     &optional (start 0) end)
  (let* ((end (or end (length string)))
         (pathname (query-temp-limited-pathname stream))
         (bytes (query-temp-string-utf8-size string start end)))
    (query-temp-check-space pathname bytes)
    (write-string string (query-temp-limited-underlying-stream stream)
                  :start start :end end)
    (query-temp-account-space
     pathname bytes (query-temp-limited-underlying-stream stream)))
  string)

(defun query-spill-write-record (stream value)
  (let* ((pathname (query-temp-path-for-stream stream))
         (output (if pathname
                     (make-instance 'query-temp-limited-character-stream
                                    :stream stream :pathname pathname)
                     stream)))
    (let ((*print-pretty* nil)
          (*print-readably* t)
          (*print-circle* t)
          (*print-length* nil)
          (*print-level* nil)
          (*print-base* 10)
          (*print-radix* nil)
          (*package* (find-package '#:pg)))
      (query-spill-write-value output value)
      (terpri output))))

(defun query-spill-read-record (stream)
  (let ((*read-eval* nil)
        (*readtable* *query-spill-readtable*)
        (*package* (find-package '#:pg)))
    (let ((value (read stream nil +query-spill-eof+)))
      (if (eq value +query-spill-eof+)
          +query-spill-eof+
          (query-spill-decode value)))))

(defstruct (query-row-spool (:constructor %make-query-row-spool))
  (rows nil)
  (count 0)
  (memory-bytes 0)
  (memory-limit *query-work-memory-bytes*)
  pathname
  output-stream
  sealed-p)

(defun make-query-row-spool (&optional (memory-limit *query-work-memory-bytes*))
  (%make-query-row-spool :memory-limit (max 1 memory-limit)))

(defun query-row-spool-spill (spool)
  "Move buffered rows to a query temporary file.

Publish the pathname and stream with interrupts masked so spool cleanup owns
both before writing can be interrupted."
  (unless (query-row-spool-output-stream spool)
    (sb-sys:without-interrupts
      (multiple-value-bind (pathname stream) (open-query-temp-output)
        (setf (query-row-spool-pathname spool) pathname
              (query-row-spool-output-stream spool) stream)))
    (dolist (row (nreverse (query-row-spool-rows spool)))
      (query-spill-write-record (query-row-spool-output-stream spool) row))
    (setf (query-row-spool-rows spool) nil
          (query-row-spool-memory-bytes spool) 0))
  spool)

(defun query-row-spool-append (spool row)
  (when (query-row-spool-sealed-p spool)
    (error "Cannot append to a sealed query row spool"))
  (let ((row-bytes (query-spill-record-size row)))
    (when (and (null (query-row-spool-output-stream spool))
               (> (+ row-bytes (query-row-spool-memory-bytes spool))
                  (query-row-spool-memory-limit spool)))
      (query-row-spool-spill spool))
    (if (query-row-spool-output-stream spool)
        (query-spill-write-record (query-row-spool-output-stream spool) row)
        (progn
          (push row (query-row-spool-rows spool))
          (incf (query-row-spool-memory-bytes spool) row-bytes)))
    (prog1 (query-row-spool-count spool)
      (incf (query-row-spool-count spool)))))

(defun query-row-spool-seal (spool)
  "Close the output successfully before marking SPOOL ready for reading."
  (unless (query-row-spool-sealed-p spool)
    (when (query-row-spool-output-stream spool)
      (finish-query-temp-stream (query-row-spool-output-stream spool) t)
      (setf (query-row-spool-output-stream spool) nil))
    (setf (query-row-spool-sealed-p spool) t))
  spool)

(defun close-query-row-spool (spool)
  "Abort any remaining spool output; the file scope retains unresolved ownership."
  (when (query-row-spool-output-stream spool)
    (abort-query-temp-stream (query-row-spool-output-stream spool)
                             (query-row-spool-pathname spool))
    (setf (query-row-spool-output-stream spool) nil))
  (values))

(defmacro with-query-spill-resource
    ((resource constructor closer &optional (memory-limit nil memory-limit-p))
     &body body)
  "Bind a spill resource and always call CLOSER, including on nonlocal exit."
  (let ((constructor-form (if memory-limit-p
                              `(,constructor ,memory-limit)
                              `(,constructor))))
    `(let ((,resource ,constructor-form))
       (unwind-protect
            (progn ,@body)
         (,closer ,resource)))))

(defmacro with-query-row-spool
    ((spool &optional (memory-limit nil memory-limit-p)) &body body)
  `(with-query-spill-resource
       (,spool make-query-row-spool close-query-row-spool
               ,@(when memory-limit-p (list memory-limit)))
     ,@body))

(defun map-query-row-spool (spool callback)
  (unless (query-row-spool-sealed-p spool)
    (error "Cannot read an unsealed query row spool"))
  (if (query-row-spool-pathname spool)
      (with-query-temp-input (stream (query-row-spool-pathname spool))
        (loop for index from 0
              for row = (query-spill-read-record stream)
              until (eq row +query-spill-eof+)
              do (when (eq (funcall callback row index) :stop)
                   (return-from map-query-row-spool :stop)))
        (values))
      (loop for row in (reverse (query-row-spool-rows spool))
            for index from 0
            do (when (eq (funcall callback row index) :stop)
                 (return-from map-query-row-spool :stop))
            finally (return (values)))))

(defstruct (query-match-map (:constructor %make-query-match-map))
  (bytes (make-array 0 :element-type '(unsigned-byte 8)
                       :adjustable t :fill-pointer 0))
  (count 0)
  (memory-limit 1)
  pathname
  io-stream
  (finished-p nil))

(defun make-query-match-map (&optional (memory-limit *query-work-memory-bytes*))
  (%make-query-match-map :memory-limit (max 1 memory-limit)))

(defun query-match-map-spill (match-map)
  (unless (query-match-map-io-stream match-map)
    (multiple-value-bind (pathname stream) (open-query-temp-io)
      (setf (query-match-map-pathname match-map) pathname
            (query-match-map-io-stream match-map) stream)
      (let* ((bytes (query-match-map-bytes match-map))
             (byte-count (length bytes)))
        (query-temp-check-space pathname byte-count)
        (loop for byte across bytes do (write-byte byte stream))
        (query-temp-account-space pathname byte-count stream))
      (setf (query-match-map-bytes match-map)
            (make-array 0 :element-type '(unsigned-byte 8)
                          :adjustable t :fill-pointer 0))))
  match-map)

(defun query-match-map-append (match-map)
  (when (query-match-map-finished-p match-map)
    (error "Cannot append to a finished query match map"))
  (when (and (null (query-match-map-io-stream match-map))
             (>= (fill-pointer (query-match-map-bytes match-map))
                 (query-match-map-memory-limit match-map)))
    (query-match-map-spill match-map))
  (if (query-match-map-io-stream match-map)
      (let* ((stream (query-match-map-io-stream match-map))
             (pathname (query-temp-path-for-stream stream)))
        (query-temp-check-space pathname 1)
        (file-position stream (query-match-map-count match-map))
        (write-byte 0 stream)
        (query-temp-account-space pathname 1 stream))
      (vector-push-extend 0 (query-match-map-bytes match-map)))
  (prog1 (query-match-map-count match-map)
    (incf (query-match-map-count match-map))))

(defun query-match-map-mark (match-map index)
  (unless (<= 0 index (1- (query-match-map-count match-map)))
    (error "Query match-map index is out of bounds: ~a" index))
  (if (query-match-map-io-stream match-map)
      (progn
        (file-position (query-match-map-io-stream match-map) index)
        (write-byte 1 (query-match-map-io-stream match-map)))
      (setf (aref (query-match-map-bytes match-map) index) 1))
  (values))

(defun query-match-map-finish (match-map)
  (when (query-match-map-io-stream match-map)
    (finish-output (query-match-map-io-stream match-map)))
  (setf (query-match-map-finished-p match-map) t)
  match-map)

(defun query-match-map-matched-p (match-map index)
  (unless (query-match-map-finished-p match-map)
    (error "Cannot read an unfinished query match map"))
  (unless (<= 0 index (1- (query-match-map-count match-map)))
    (error "Query match-map index is out of bounds: ~a" index))
  (= (if (query-match-map-io-stream match-map)
         (progn
           (file-position (query-match-map-io-stream match-map) index)
           (read-byte (query-match-map-io-stream match-map)))
         (aref (query-match-map-bytes match-map) index))
     1))

(defun close-query-match-map (match-map)
  "Abort the match-map stream; the file scope retains unresolved ownership."
  (when (query-match-map-io-stream match-map)
    (abort-query-temp-stream (query-match-map-io-stream match-map)
                             (query-match-map-pathname match-map))
    (setf (query-match-map-io-stream match-map) nil))
  (values))

(defmacro with-query-match-map
    ((match-map &optional (memory-limit nil memory-limit-p)) &body body)
  `(with-query-spill-resource
       (,match-map make-query-match-map close-query-match-map
                   ,@(when memory-limit-p (list memory-limit)))
     ,@body))
