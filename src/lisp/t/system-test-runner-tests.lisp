(require :asdf)
(load (merge-pathnames "system-test-runner.lisp" *load-truename*))

(defparameter *pg-runner-test-source*
  (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))

(defun check-pg-runner-compiler-lifecycle (mode)
  ;; Stub loading and execution while exercising child ownership and failure gates.
  (let ((saved (mapcar (lambda (name) (cons name (symbol-function name)))
                       '(pg-test-start-compiler pg-test-phase pg-test-selected-heads
                         pg-test-execute-heads)))
        (child nil) (executed nil) (failure nil) (result nil) (leaked nil))
    (labels ((start (source cache log)
               (declare (ignore source cache))
               (setf child
                     (sb-ext:run-program
                      (case mode (:interrupt "/usr/bin/sleep")
                            (:bad-exit "/usr/bin/false")
                            (:missing-marker "/usr/bin/true")
                            (otherwise "/usr/bin/echo"))
                      (case mode (:interrupt '("30"))
                            ((:bad-exit :missing-marker) nil)
                            (otherwise '("TEST-STRICT-COMPILE-PASS")))
                      :wait nil :output log :error :output :if-output-exists :error))
               (when (eq mode :interrupt)
                 (sb-thread:interrupt-thread sb-thread:*current-thread*
                                             (lambda () (throw 'runner-interrupted :cut))))
               child)
             (stub-phase (name function)
               (unless (member name '(:production :test-load)) (funcall function)))
             (select (suite names) (declare (ignore suite names)) '(:dummy))
             (execute (heads)
               (assert (equal heads '(:dummy)))
               (setf executed t)
               (not (eq mode :head-failure))))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg-test-start-compiler) #'start
                   (symbol-function 'pg-test-phase) #'stub-phase
                   (symbol-function 'pg-test-selected-heads) #'select
                   (symbol-function 'pg-test-execute-heads) #'execute)
             (handler-case
                 (setf result (catch 'runner-interrupted
                                (run-pg-system-tests '("unit" "--strict") *pg-runner-test-source*)))
               (error (condition) (setf failure condition)))
             (setf leaked (and child (member (sb-ext:process-status child) '(:running :stopped)))))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
        (when child (pg-test-close-compiler child))))
    (assert child)
    (assert (not leaked))
    (ecase mode
      (:interrupt (assert (eq result :cut)) (assert (null failure)) (assert (not executed)))
      ((:bad-exit :missing-marker)
       (assert failure)
       (assert (search "Strict test compiler failed" (princ-to-string failure)))
       (assert (not executed)))
      (:success (assert (and executed result (null failure))))
      (:head-failure (assert (and executed (null result) (null failure)))))
    (format t "~&RUNNER-LIFECYCLE-PASS ~s~%" mode)
    (finish-output)))

(defun check-pg-runner-seeds ()
  ;; Check both reproducibility and the no-seed preservation contract.
  (let ((*random-state* (make-random-state)))
    (dolist (seed '(nil ""))
      (let ((saved *random-state*))
        (pg-test-apply-seed seed)
        (assert (eq saved *random-state*))))
    (dolist (seed '("0" "42" "2147483648"))
      (pg-test-apply-seed seed)
      (let ((first (loop repeat 8 collect (random 256))))
        (pg-test-apply-seed seed)
        (assert (equal first (loop repeat 8 collect (random 256))))))
    (assert (handler-case (progn (pg-test-apply-seed "-1") nil) (error () t))))
  (format t "~&RUNNER-SEED-PASS~%"))

(check-pg-runner-seeds)

(dolist (mode '(:interrupt :bad-exit :missing-marker :success :head-failure))
  (check-pg-runner-compiler-lifecycle mode))
