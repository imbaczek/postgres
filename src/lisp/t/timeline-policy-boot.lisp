(require :asdf)

(defparameter *timeline-policy-test-root*
  (uiop:pathname-parent-directory-pathname
   (uiop:pathname-directory-pathname *load-truename*)))

(let ((*compile-verbose* nil) (*load-verbose* nil)
                              (*standard-output* *error-output*))
  (asdf:load-asd (merge-pathnames "pg.asd" *timeline-policy-test-root*))
  (asdf:load-system "pg"))

(let* ((name (uiop:getenv "PG_TEST_TIMELINE_GOAL"))
       (goal (cond ((equal name "current") :current)
                   ((equal name "latest") :latest)
                   (t (error "invalid test timeline goal"))))
       (original (symbol-function 'pg::physical-recovery-prepare)))
  (setf (symbol-function 'pg::physical-recovery-prepare)
        (lambda (&rest arguments)
          (apply original :timeline-goal goal arguments))))

(load (merge-pathnames "boot.lisp" *timeline-policy-test-root*))
