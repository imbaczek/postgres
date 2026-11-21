(require :asdf)
(require :sb-posix)

(in-package #:cl-user)

(defun call-with-pg-test-coverage (function)
  "Run FUNCTION in a dedicated test process, instrumenting its FASLs when requested."
  (let ((output (uiop:getenv "PG_LISP_TEST_COVERAGE_DIR")))
    (when (or (null output) (zerop (length output)))
      (return-from call-with-pg-test-coverage (funcall function)))
    (when (uiop:getenv "PG_LISP_TEST_CAMPAIGN")
      (error "Run coverage outside the strict test campaign cache."))
    (when (member "pg" (asdf:already-loaded-systems) :test #'string=)
      (error "Coverage requires a fresh Lisp image without preloaded pg."))
    (require :sb-cover)
    (let* ((directory (uiop:ensure-directory-pathname (merge-pathnames output)))
           (cache (uiop:ensure-directory-pathname
                   (sb-posix:mkdtemp
                    (namestring (merge-pathnames "pg-coverage.XXXXXX"
                                                 (uiop:temporary-directory))))))
           ;; Rove resolves cached FASLs back to source using this cache root.
           (asdf:*user-cache* cache))
      (unwind-protect
           (let ((asdf/output-translations::*output-translations* nil)
                 (asdf/output-translations::*output-translations-parameter* nil))
             (asdf:initialize-output-translations
              '(:output-translations ("/" :user-cache)
                :ignore-inherited-configuration))
             (multiple-value-prog1
                 (with-compilation-unit (:policy '(optimize (sb-c:store-coverage-data 3)))
                   (funcall function))
               (let ((report (uiop:symbol-call :sb-cover :report directory)))
                 (unless report
                   (error "SBCL produced no coverage report."))
                 (format t "~&Coverage report: ~a~%" report))))
        (uiop:delete-directory-tree cache :validate t)))))
