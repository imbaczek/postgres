(in-package #:pg)

(defun physical-directory-root-test-fd-count ()
  (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))

(defun physical-directory-root-test-close (mode)
  (let ((before (physical-directory-root-test-fd-count))
        (original (symbol-function 'sb-posix:close))
        (target nil) (calls 0) (outcome nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'sb-posix:close)
                 (lambda (fd)
                   (prog1 (funcall original fd)
                     (when (eql fd target)
                       (incf calls)
                       (error "handoff root close cut")))))
           (setf outcome
                 (handler-case
                     (list :value
                           (catch 'root-close-exit
                             (call-with-postmaster-ownership
                              (lambda (ownership)
                                (let ((arguments (logical-slot-copy-owner-arguments ownership)))
                                  (remf arguments :ownership)
                                  (let ((owner (apply #'make-physical-adoption-owner arguments)))
                                    (unwind-protect
                                         (call-with-physical-directory-root
                                          owner
                                          (lambda (fd path)
                                            (declare (ignore path))
                                            (setf target fd)
                                            (ecase mode
                                              (:normal :returned)
                                              (:error (error "handoff root body cut"))
                                              (:throw (throw 'root-close-exit :escaped)))))
                                      (setf (physical-adoption-owner-active-p owner) nil))))))))
                   (error (condition) (list :error (princ-to-string condition))))))
      (setf (symbol-function 'sb-posix:close) original))
    (assert (= calls 1))
    (assert (= before (physical-directory-root-test-fd-count)))
    (ecase mode
      (:normal (assert (and (eq :error (first outcome)) (search "handoff root close cut" (second outcome)))))
      (:error (assert (and (eq :error (first outcome)) (search "handoff root body cut" (second outcome)))))
      (:throw (assert (equal '(:value :escaped) outcome))))
    (format t "~&LOGICAL-INSTALL-ROOT-CLOSE-PASS ~s~%" mode)
    (finish-output)))
