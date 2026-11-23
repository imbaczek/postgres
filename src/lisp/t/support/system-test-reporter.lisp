(in-package #:cl-user)

(defclass pg-system-test-reporter (rove/reporter/spec:spec-reporter) ())

(defmethod rove/core/stats:record :after
    ((reporter pg-system-test-reporter) (failure rove/core/result:failed-assertion))
  (let ((reason (rove/core/result:assertion-reason failure)))
    (when (typep reason 'condition)
      (format (rove/reporter:reporter-stream reporter)
              "~&TEST-CONDITION ~a ~a~%" (type-of reason)
              (handler-case (princ-to-string reason)
                (error () "(condition message unavailable)"))))))

(rove/reporter/registry:add-reporter :pg-system 'pg-system-test-reporter)
