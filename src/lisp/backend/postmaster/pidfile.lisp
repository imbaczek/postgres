;;;; Local PGDATA ownership and PostgreSQL-compatible process discovery metadata.
(in-package #:pg)

(defvar *postmaster-data-lock* nil)

(defvar *postmaster-pid-stream* nil)

(defvar *postmaster-pid-directory* nil)

(defvar *postmaster-start-time* nil)

(defvar *postmaster-start-timestamp* nil
  "PostgreSQL epoch microseconds when this postmaster initializes its owned PGDATA.")

(defun postmaster-start-timestamp ()
  "Return the initialized PgStartTime analogue without sampling the query clock."
  (or *postmaster-start-timestamp*
      (ereport "55000" "postmaster startup timestamp is not initialized")))

(defvar *postmaster-pid-host* nil)

(defvar *postmaster-pid-port* nil)

(defvar *postmaster-pid-inode* nil)

(sb-alien:define-alien-routine ("sysconf" postmaster-sysconf) sb-alien:long
  (name sb-alien:int))

(defun postmaster-process-start-time ()
  "Linux process creation time, not Lisp main-entry time after ASDF compilation."
  #+linux
  (let* ((line (with-open-file (stream "/proc/self/stat") (read-line stream)))
         (tail (subseq line (+ 2 (position #\) line :from-end t))))
         (fields (remove "" (uiop:split-string tail :separator '(#\Space)) :test #'equal))
         ;; Linux /proc stat field 22, after the pid and parenthesized comm fields.
         (ticks (parse-integer (nth 19 fields)))
         ;; glibc bits/confname.h: _SC_CLK_TCK is enum value 2.
         (frequency (postmaster-sysconf 2))
         (boot (with-open-file (stream "/proc/stat")
                 (loop for row = (read-line stream nil nil) while row
                       when (uiop:string-prefix-p "btime " row)
                         return (parse-integer row :start 6)))))
    (unless (and boot (plusp frequency)) (error "cannot determine process creation time"))
    (+ boot (floor ticks frequency)))
  #-linux
  (error "accurate postmaster process start metadata is not implemented on this platform"))

(defun postmaster-data-path (name)
  (merge-pathnames name *postmaster-pid-directory*))

(defun postmaster-pid-text (status)
  (unless (member status '("starting" "ready   " "stopping" "standby ") :test #'equal)
    (error "invalid postmaster process status"))
  (let* ((directory (namestring *postmaster-pid-directory*))
         (canonical (if (> (length directory) 1) (string-right-trim "/" directory) directory)))
    (format nil "~d~%~a~%~d~%~d~%~%~a~%~%~a~%"
            (sb-posix:getpid) canonical *postmaster-start-time*
            *postmaster-pid-port* *postmaster-pid-host* status)))

(defun postmaster-parse-pid (line)
  ;; Parse a signed PID and return its absolute value, or NIL for zero or a
  ;; magnitude greater than 2147483647.  Malformed input signals an error;
  ;; the file reader handles that without swallowing interruptions.
  (let ((pid (parse-integer line)))
    (and (not (zerop pid)) (<= (abs pid) 2147483647) (abs pid))))

(defun postmaster-existing-pid ()
  "Read bounded regular PID metadata without following links or blocking on a FIFO."
  (let* ((path (postmaster-data-path "postmaster.pid"))
         (fd (sb-posix:open (namestring path)
                            (logior sb-posix:o-rdonly sb-posix:o-nonblock sb-posix:o-nofollow
                                    +postmaster-o-cloexec+)))
         (stream nil))
    (unwind-protect
         (progn
           (unless (sb-posix:s-isreg (sb-posix:stat-mode (sb-posix:fstat fd)))
             (error "postmaster.pid is not a regular file"))
           (setf stream (sb-sys:make-fd-stream fd :input t :element-type 'character
                                                  :external-format :utf-8 :auto-close t))
           (unless (<= 1 (sb-posix:stat-size (sb-posix:fstat fd)) 16384) (error "invalid postmaster.pid length"))
           (let* ((line (read-line stream nil ""))
                  (pid (handler-case-noninterrupt (postmaster-parse-pid line) (condition nil))))
             (unless pid (error "invalid process ID in postmaster.pid"))
             pid))
      (if stream (close stream) (sb-posix:close fd)))))

(defun postmaster-pid-is-live-p (pid)
  "Follow C's ancestor exceptions; do not infer PID reuse from elapsed time."
  (let ((grandparent (ignore-errors (parse-integer (or (uiop:getenv "PG_GRANDPARENT_PID") "")))))
    (unless (member pid (list (sb-posix:getpid) (sb-posix:getppid) grandparent))
      (handler-case (progn (sb-posix:kill pid 0) t)
        (sb-posix:syscall-error (condition)
          (unless (member (sb-posix:syscall-errno condition) (list sb-posix:esrch sb-posix:eperm))
            (error condition)))))))

(defun postmaster-pid-owned-p ()
  (and *postmaster-pid-stream*
       (probe-file (postmaster-data-path "postmaster.pid"))
       (let ((stat (sb-posix:stat (postmaster-data-path "postmaster.pid"))))
         (and (= (sb-posix:stat-ino stat) *postmaster-pid-inode*)
              (= (sb-posix:stat-dev stat)
                 (sb-posix:stat-dev (sb-posix:fstat (sb-sys:fd-stream-fd *postmaster-pid-stream*))))))))

(defvar *postmaster-ownership-contexts*
  ;; Dynamically scoped ownership objects active on the current thread.
  nil)

(defstruct (postmaster-ownership (:constructor %make-postmaster-ownership))
  ;; This object borrows the published streams; it neither acquires the lock
  ;; nor closes either stream.  Save the canonical root and descriptor identities
  ;; so later checks can detect replacement.  A copy is not an active scope.
  (directory nil :read-only t)
  (device nil :read-only t) (inode nil :read-only t)
  (lock nil :read-only t) (lock-fd nil :read-only t)
  (lock-device nil :read-only t) (lock-inode nil :read-only t)
  (pid-stream nil :read-only t) (pid-fd nil :read-only t)
  (pid-device nil :read-only t) (pid-inode nil :read-only t)
  (thread nil :read-only t) active-p)

(defmethod print-object ((owner postmaster-ownership) stream)
  (print-unreadable-object (owner stream :type t :identity t)))

(defun postmaster-ownership-file-p (stat device inode)
  ;; Both the name and descriptor must still identify the private regular file.
  (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
       (= 1 (sb-posix:stat-nlink stat))
       (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
       (zerop (logand #o077 (sb-posix:stat-mode stat)))
       (= device (sb-posix:stat-dev stat))
       (= inode (sb-posix:stat-ino stat))))

(defun postmaster-check-ownership-identities (owner)
  ;; Check the published streams, root and both names against the saved
  ;; identities.  The root must remain owned by us with mode 0700 or 0750.
  (let ((directory (postmaster-ownership-directory owner))
        (lock (postmaster-ownership-lock owner))
        (pid-stream (postmaster-ownership-pid-stream owner)))
    (unless (and (eq lock *postmaster-data-lock*) (open-stream-p lock)
                 (= (postmaster-ownership-lock-fd owner) (sb-sys:fd-stream-fd lock))
                 (eq pid-stream *postmaster-pid-stream*) (open-stream-p pid-stream)
                 (= (postmaster-ownership-pid-fd owner) (sb-sys:fd-stream-fd pid-stream))
                 (equal directory *postmaster-pid-directory*)
                 (equal directory (uiop:ensure-directory-pathname (truename *pgdata*)))
                 (eql (postmaster-ownership-pid-inode owner) *postmaster-pid-inode*))
      (error "postmaster ownership lost its published context"))
    (let ((root (sb-posix:lstat (namestring directory))))
      (unless (and (sb-posix:s-isdir (sb-posix:stat-mode root))
                   (= (sb-posix:geteuid) (sb-posix:stat-uid root))
                   (member (logand #o777 (sb-posix:stat-mode root)) '(#o700 #o750))
                   (= (postmaster-ownership-device owner) (sb-posix:stat-dev root))
                   (= (postmaster-ownership-inode owner) (sb-posix:stat-ino root)))
        (error "postmaster data directory identity changed")))
    (flet ((check-file (path fd device inode)
             (unless (and (postmaster-ownership-file-p (sb-posix:lstat (namestring path)) device inode)
                          (postmaster-ownership-file-p (sb-posix:fstat fd) device inode))
               (error "postmaster ownership file identity changed: ~a" path))))
      (check-file (merge-pathnames ".pg_lisp.lock" directory) (postmaster-ownership-lock-fd owner)
                  (postmaster-ownership-lock-device owner) (postmaster-ownership-lock-inode owner))
      (check-file (merge-pathnames "postmaster.pid" directory) (postmaster-ownership-pid-fd owner)
                  (postmaster-ownership-pid-device owner) (postmaster-ownership-pid-inode owner)))
    (unless (and (postmaster-pid-owned-p) (eql (sb-posix:getpid) (postmaster-existing-pid)))
      (error "postmaster PID no longer identifies this process")))
  owner)

(defun postmaster-check-ownership (owner)
  ;; Only the exact object in the active dynamic scope on its creator thread
  ;; has authority.  Check identities on both sides of the passive flock proof;
  ;; the proof does not reacquire the lock.  An ordinary failed observation
  ;; revokes the object and reports the original condition.  Fatal, cancellation,
  ;; and latched WAL errors propagate unchanged.
  (unless (and (postmaster-ownership-p owner) (postmaster-ownership-active-p owner)
               (eq sb-thread:*current-thread* (postmaster-ownership-thread owner))
               (member owner *postmaster-ownership-contexts* :test #'eq))
    (error "postmaster ownership scope ended or changed thread"))
  (sb-sys:without-interrupts
    (let ((result (sb-sys:with-local-interrupts
                    (handler-case-noninterrupt
                     (progn
                       (postmaster-check-ownership-identities owner)
                       (physical-file-check-exclusive-flock (postmaster-ownership-lock owner) owner)
                       (postmaster-check-ownership-identities owner)
                       t)
                     (condition condition)))))
      (unless (eq t result)
        (setf (postmaster-ownership-active-p owner) nil)
        (error result))
      (unless (postmaster-ownership-active-p owner)
        (error "postmaster ownership was revoked during inspection"))))
  owner)

(defun call-with-postmaster-ownership (function)
  ;; Borrow the postmaster's published streams for this call.  Check ownership
  ;; before entering and again on normal return, then revoke it on every exit.
  ;; Cleanup does not close the streams or replace the reason for unwinding.
  (unless (and *postmaster-data-lock* (open-stream-p *postmaster-data-lock*)
               *postmaster-pid-stream* (open-stream-p *postmaster-pid-stream*)
               (postmaster-pid-owned-p))
    (error "postmaster ownership requires published lock and PID streams"))
  (let* ((directory (uiop:ensure-directory-pathname (truename *pgdata*)))
         (root (sb-posix:lstat (namestring directory)))
         (lock-fd (sb-sys:fd-stream-fd *postmaster-data-lock*))
         (pid-fd (sb-sys:fd-stream-fd *postmaster-pid-stream*))
         (lock (sb-posix:fstat lock-fd)) (pid (sb-posix:fstat pid-fd))
         (owner (%make-postmaster-ownership
                 :directory directory :device (sb-posix:stat-dev root) :inode (sb-posix:stat-ino root)
                 :lock *postmaster-data-lock* :lock-fd lock-fd
                 :lock-device (sb-posix:stat-dev lock) :lock-inode (sb-posix:stat-ino lock)
                 :pid-stream *postmaster-pid-stream* :pid-fd pid-fd
                 :pid-device (sb-posix:stat-dev pid) :pid-inode (sb-posix:stat-ino pid)
                 :thread sb-thread:*current-thread* :active-p t)))
    (let ((*postmaster-ownership-contexts* (cons owner *postmaster-ownership-contexts*)))
      (unwind-protect
           (progn
             (postmaster-check-ownership owner)
             (multiple-value-prog1 (funcall function owner)
               (postmaster-check-ownership owner)))
        (setf (postmaster-ownership-active-p owner) nil)))))

(defun postmaster-update-pid-status (status)
  "Update the owned inode in place; observers may see a partial metadata read."
  (unless (postmaster-pid-owned-p) (error "postmaster.pid ownership was lost"))
  (file-position *postmaster-pid-stream* 0)
  (write-string (postmaster-pid-text status) *postmaster-pid-stream*)
  (fsync-stream *postmaster-pid-stream*)
  status)

(defun postmaster-note-listener (listener)
  "Publish the actual bound TCP endpoint, then the ready status."
  (multiple-value-bind (address port) (sb-bsd-sockets:socket-name listener)
    (setf *postmaster-pid-host*
          (if (every #'zerop address) "*"
              (format nil "~{~d~^.~}" (coerce address 'list)))
          *postmaster-pid-port* port)
    (postmaster-update-pid-status "ready   ")))

(defun postmaster-remove-pid-file ()
  "Remove our PID file and sync its directory, then close and clear its stream.
Close and clear the PID stream even when removal or sync fails.  This function
leaves the separate data ownership lock to its owner."
  (when *postmaster-pid-stream*
    (let ((stream *postmaster-pid-stream*))
      (unwind-protect
           (when (postmaster-pid-owned-p)
             (delete-file (postmaster-data-path "postmaster.pid"))
             (fsync-directory *postmaster-pid-directory*))
        (unwind-protect
             (close stream)
          (when (eq stream *postmaster-pid-stream*)
            (setf *postmaster-pid-stream* nil))))))
  (values))

(defun postmaster-create-pid-file ()
  "The stable local flock excludes cooperating Lisp contenders during stale PID reclamation."
  (let ((path (postmaster-data-path "postmaster.pid")))
    (when (probe-file path)
      (let ((inode (sb-posix:stat-ino (sb-posix:stat path)))
            (pid (postmaster-existing-pid)))
        (when (postmaster-pid-is-live-p pid)
          (error "postmaster.pid already exists for live process ~d" pid))
        (unless (= inode (sb-posix:stat-ino (sb-posix:stat path)))
          (error "postmaster.pid changed during stale owner inspection"))
        (delete-file path)
        (fsync-directory *postmaster-pid-directory*)))
    (let ((fd (sb-posix:open (namestring path)
                             (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                     +postmaster-o-cloexec+) #o600)))
      (setf *postmaster-pid-stream*
            (sb-sys:make-fd-stream fd :input t :output t :element-type 'character
                                      :external-format :utf-8 :buffering :full :auto-close t)
            *postmaster-pid-inode* (sb-posix:stat-ino (sb-posix:fstat fd))))
    (pushnew 'postmaster-remove-pid-file sb-ext:*exit-hooks*)
    (postmaster-update-pid-status "starting")
    (fsync-directory *postmaster-pid-directory*)))

(defun postmaster-check-data-directory ()
  "Create only a missing native PGDATA; existing permissions must already be valid."
  (unless (probe-file *pgdata*)
    (ensure-directories-exist
     (merge-pathnames "parent" (uiop:pathname-parent-directory-pathname *pgdata*)))
    (handler-case
        (sb-posix:mkdir (namestring *pgdata*) #o700)
      (sb-posix:syscall-error (condition)
        (unless (= (sb-posix:syscall-errno condition) sb-posix:eexist)
          (error condition)))))
  (let ((stat (sb-posix:stat *pgdata*)))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:stat-uid stat) (sb-posix:geteuid))
                 (member (logand (sb-posix:stat-mode stat) #o777) '(#o700 #o750)))
      (error "data directory has wrong type, ownership or permissions; require 0700 or 0750")))
  (truename *pgdata*))

(defun postmaster-open-data-lock ()
  "Open only a dedicated regular ownership inode, never a link or special file."
  (let* ((fd (sb-posix:open (namestring (postmaster-data-path ".pg_lisp.lock"))
                            (logior sb-posix:o-rdwr sb-posix:o-creat
                                    sb-posix:o-nonblock sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
         (success nil))
    (unwind-protect
         (let ((stat (sb-posix:fstat fd)))
           (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                        (= (sb-posix:stat-uid stat) (sb-posix:geteuid))
                        (= (sb-posix:stat-nlink stat) 1))
             (error "data ownership lock is not a dedicated regular file"))
           (sb-posix:fchmod fd #o600)
           (prog1 (sb-sys:make-fd-stream fd :input t :output t
                                            :element-type '(unsigned-byte 8) :auto-close t)
             (setf success t)))
      (unless success (sb-posix:close fd)))))

(defun postmaster-own-data-directory (host port &key startup-before-pid startup-check startup-reconcile startup-observer)
  "Own PGDATA before any database storage initialization or recovery mutation.
Call read-only STARTUP-BEFORE-PID with the flock held before the startup
timestamp or PID metadata changes.  A failure leaves stale PID evidence intact.
Call STARTUP-CHECK synchronously with the local flock held and owned PID created,
before publishing *POSTMASTER-DATA-LOCK*. The check must be read-only.
Run STARTUP-RECONCILE after publishing ownership and before returning to startup,
where stage cleanup, authentication completion, storage, listeners, and workers follow.
If a callback fails or exits nonlocally, clear ownership and the timestamp,
remove the owned PID, and close the lock even if PID cleanup fails."
  ;; STARTUP-OBSERVER is read-only and receives the borrowed flocked stream
  ;; with :before-pid and :owned-pid before global lock publication.  It cannot
  ;; be combined with the legacy PID checks.  Each call must leave the stream open.
  (declare (ignore host))
  (when *postmaster-data-lock* (error "this process already owns a data directory"))
  (when (and startup-observer (or startup-before-pid startup-check))
    (error "startup observer cannot be combined with legacy PID checks"))
  (postmaster-check-data-directory)
  (let ((lock nil) (success nil))
    (unwind-protect
         (progn
           (setf *postmaster-pid-directory* (truename *pgdata*)
                 *postmaster-pid-host* "" *postmaster-pid-port* port
                 *postmaster-start-time* (postmaster-process-start-time)
                 lock (postmaster-open-data-lock))
           (unless (zerop (physical-store-flock (sb-sys:fd-stream-fd lock) 6))
             (error "data directory is already owned by another pg-lisp process"))
           (if startup-observer
               (progn
                 (funcall startup-observer lock :before-pid)
                 (unless (open-stream-p lock)
                   (error "startup observer closed the data-directory lock")))
               (when startup-before-pid (funcall startup-before-pid)))
           (setf *postmaster-start-timestamp* (postgres-current-timestamp))
           (postmaster-create-pid-file)
           (if startup-observer
               (progn
                 (funcall startup-observer lock :owned-pid)
                 (unless (open-stream-p lock)
                   (error "startup observer closed the data-directory lock")))
               (when startup-check (funcall startup-check)))
           (setf *postmaster-data-lock* lock)
           (when startup-reconcile (funcall startup-reconcile))
           (setf success t)
           *postmaster-pid-directory*)
      (unless success
        (when (eq *postmaster-data-lock* lock)
          (setf *postmaster-data-lock* nil))
        (setf *postmaster-start-timestamp* nil)
        (unwind-protect
             (when *postmaster-pid-stream* (postmaster-remove-pid-file))
          (when lock (close lock)))))))
