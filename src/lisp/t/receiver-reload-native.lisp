(in-package #:pg)

(defvar *receiver-reload-native-thread* nil)

(defvar *receiver-reload-native-original* nil)

(defun receiver-reload-native-uninstall ()
  (when *receiver-reload-native-original*
    (setf (symbol-function 'physical-follow-quiesce-receiver) *receiver-reload-native-original*
          *receiver-reload-native-original* nil))
  (values))

(defun receiver-reload-native-write (directory name text)
  (let ((path (merge-pathnames name directory))
        (temporary (merge-pathnames (concatenate 'string name ".tmp") directory)))
    (with-open-file (output temporary :direction :output :if-exists :supersede)
      (write-string text output))
    (sb-posix:rename temporary path)))

(defun receiver-reload-native-release-p (directory)
  (loop repeat 3000
        when (probe-file (merge-pathnames "release" directory)) return t
          do (sleep 0.01)
        finally (error "native receiver reload barrier timed out")))

(defun receiver-reload-native-install (directory)
  (when (and *receiver-reload-native-thread*
             (sb-thread:thread-alive-p *receiver-reload-native-thread*))
    (error "native receiver reload fixture is already active"))
  (let* ((runtime *physical-follow-runtime*)
         (original (symbol-function 'physical-follow-quiesce-receiver))
         (ready (sb-thread:make-semaphore))
         (closed (sb-thread:make-semaphore))
         (caller nil))
    (unless (and runtime (physical-follow-runtime-receiver-thread runtime))
      (error "native receiver reload fixture requires receipt"))
    (setf *receiver-reload-native-original* original
          (symbol-function 'physical-follow-quiesce-receiver)
          (lambda ()
            (receiver-reload-native-uninstall)
            (setf caller sb-thread:*current-thread*)
            (assert (zerop *live-operation-depth*))
            (assert (sb-thread:holding-mutex-p *physical-follow-command-lock*))
            (sb-thread:signal-semaphore ready)
            (unless (sb-thread:wait-on-semaphore closed :timeout 10)
              (error "native reload gate did not close"))
            (prog1 (funcall original)
              (receiver-reload-native-write directory "stopped" "stopped"))))
    (setf *receiver-reload-native-thread*
          (sb-thread:make-thread
           (lambda ()
             (handler-case
                 (progn
                   (unless (sb-thread:wait-on-semaphore ready :timeout 20)
                     (error "native receiver did not reach quiescence"))
                   (sb-thread:with-mutex (*reload-run-lock*)
                     (with-live-operation-quiescence
                       (sb-thread:signal-semaphore closed)
                       (unless
                           (loop repeat 1000
                                 when (eq *reload-run-lock* (sb-thread::thread-waiting-for caller))
                                   return t
                                 do (sleep 0.01))
                         (error "native receiver stop did not wait for reload exclusion"))
                       (assert (not *live-operation-admission-open*))
                       (assert (not (physical-follow-runtime-receiver-stopped-p runtime)))
                       (unless (physical-follow-runtime-stopped-p runtime)
                         (assert (sb-thread:thread-alive-p
                                  (physical-follow-runtime-receiver-thread runtime))))
                       (receiver-reload-native-write directory "closed" "closed")
                       (receiver-reload-native-release-p directory)))
                   (receiver-reload-native-write directory "opened" "opened"))
               (error (condition)
                 (receiver-reload-native-write directory "error" (princ-to-string condition)))))
           :name "native receiver reload gate")))
  (values))
