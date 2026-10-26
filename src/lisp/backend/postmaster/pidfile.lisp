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
                  (pid (ignore-errors (parse-integer line))))
             (unless (and pid (not (zerop pid)) (<= (abs pid) 2147483647))
               (error "invalid process ID in postmaster.pid"))
             (abs pid)))
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
  "Keep the stable ownership descriptor open until OS process exit."
  (when *postmaster-pid-stream*
    (when (postmaster-pid-owned-p)
      (delete-file (postmaster-data-path "postmaster.pid"))
      (fsync-directory *postmaster-pid-directory*))
    (close *postmaster-pid-stream*)
    (setf *postmaster-pid-stream* nil))
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

(defun postmaster-own-data-directory (host port)
  "Own PGDATA before any database storage initialization or recovery mutation."
  (declare (ignore host))
  (when *postmaster-data-lock* (error "this process already owns a data directory"))
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
           (setf *postmaster-start-timestamp* (postgres-current-timestamp))
           (postmaster-create-pid-file)
           (setf *postmaster-data-lock* lock success t)
           *postmaster-pid-directory*)
      (unless success
        (setf *postmaster-start-timestamp* nil)
        (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when lock (close lock))))))
