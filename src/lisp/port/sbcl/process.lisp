;;;; process.lisp -- process identity for the SBCL port.

(in-package #:pg)

#+linux
(sb-alien:define-alien-routine ("sysconf" port-sysconf) sb-alien:long
  (name sb-alien:int))

(defun port-process-start-token (pid)
  "Return PID's Linux process creation ticks since boot, or NIL if unreadable.
PID NIL selects the current process. Compare tokens within the same boot and
PID namespace. Use a separate probe to determine liveness."
  #+linux
  (ignore-errors
   (with-open-file (stream (if pid (format nil "/proc/~d/stat" pid) "/proc/self/stat"))
     (let* ((line (read-line stream))
            (tail (subseq line (+ 2 (position #\) line :from-end t))))
            (fields (remove "" (uiop:split-string tail :separator '(#\Space)) :test #'equal)))
       ;; The command name may contain spaces and parentheses. Start after its
       ;; final closing parenthesis. Linux stat field 22 becomes index 19.
       (parse-integer (nth 19 fields)))))
  #-linux
  (locally (declare (ignore pid)) nil))

(defun port-current-pid-namespace-token ()
  "Return the current Linux PID namespace identifier, or NIL if unavailable."
  #+linux
  (ignore-errors
   (let* ((target (sb-posix:readlink "/proc/self/ns/pid"))
          (start (position #\[ target))
          (end (and start (position #\] target :start (1+ start)))))
     (and start end (parse-integer target :start (1+ start) :end end))))
  #-linux
  nil)

(defun port-current-process-start-time ()
  "Return the OS creation time of the current process in whole Unix epoch seconds.
On Linux, add the boot time to the process start ticks converted to seconds.
Signal an error if the required metadata is unavailable."
  #+linux
  (let* ((ticks (port-process-start-token nil))
         ;; glibc bits/confname.h: _SC_CLK_TCK is enum value 2.
         (frequency (port-sysconf 2))
         (boot (with-open-file (stream "/proc/stat")
                 (loop for row = (read-line stream nil nil) while row
                       when (uiop:string-prefix-p "btime " row)
                         return (parse-integer row :start 6)))))
    (unless (and ticks boot (plusp frequency)) (error "cannot determine process creation time"))
    (+ boot (floor ticks frequency)))
  #-linux
  (error "accurate postmaster process start metadata is not implemented on this platform"))

(defun port-process-signalable-p (pid)
  "Return true when PID responds to a signal-zero probe.
An absent process or one inaccessible to this user does not establish liveness."
  (handler-case (progn (sb-posix:kill pid 0) t)
    (sb-posix:syscall-error (condition)
      (unless (member (sb-posix:syscall-errno condition) (list sb-posix:esrch sb-posix:eperm))
        (error condition)))))
