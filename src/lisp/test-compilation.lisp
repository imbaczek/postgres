(defpackage #:pg-test-compilation
  (:use #:cl)
  (:export #:profile #:cache-translations #:call-with-policy #:around-compile #:call-with-cache))
(in-package #:pg-test-compilation)

(defun profile (&optional (value (uiop:getenv "PG_LISP_TEST_COMPILE_POLICY")))
  ;; Fast is opt-in for physical campaigns; the unit campaign rejects it.
  ;; The campaign marker is not identity authority.  The runner still checks
  ;; source, runtime and suite, and requires the same policy at create and run.
  (cond ((or (null value) (equal value "default")) :default)
        ((equal value "fast") :fast)
        (t (error "Unknown test compilation policy: ~s" value))))

(defun cache-translations (cache source profile)
  (dolist (root (list cache source))
    (unless (and (uiop:absolute-pathname-p root) (uiop:directory-pathname-p root))
      (error "Expected an absolute cache or source directory: ~s" root)))
  (let ((tag (ecase profile
               (:default "common-lisp/tests-default-v1/")
               (:fast "common-lisp/tests-fast-v1/"))))
    (list :output-translations
          (list (namestring (merge-pathnames "t/" source))
                (namestring (merge-pathnames tag cache)))
          (list "/" (namestring (merge-pathnames "common-lisp/" cache)))
          :ignore-inherited-configuration)))

(defun call-with-policy (profile thunk)
  ;; Scope these qualities to test compilation.  Default and production
  ;; compilation remain unchanged; DEBUG keeps its inherited value.
  (ecase profile
    (:default (funcall thunk))
    (:fast
     (with-compilation-unit
         (:policy '(optimize (compilation-speed 3) (speed 1) (safety 3)))
       (funcall thunk)))))

(defvar *test-cache-context* nil)

(defun call-with-cache (profile cache source thunk)
  ;; Explicitly isolate compilation with policy-tagged test FASLs.  The
  ;; dynamic context and output translations unwind on errors or throws.
  (let* ((translations (cache-translations cache source profile))
         (*test-cache-context*
           (list profile (merge-pathnames "t/compile-policy-probe.fasl" source)
                 (merge-pathnames "compile-policy-probe.fasl" (second (second translations))))))
    (progv (list (find-symbol "*OUTPUT-TRANSLATIONS*" :asdf/output-translations)) '(nil)
      (asdf:initialize-output-translations translations)
      (funcall thunk))))

(defun around-compile (thunk &optional (profile (profile)))
  ;; Require both the dynamic context and its actual output mapping.  In fast
  ;; mode, warnings are fatal except for style warnings.
  (ecase profile
    (:default (funcall thunk))
    (:fast
     (unless (and (eq :fast (first *test-cache-context*))
                  (equal (third *test-cache-context*)
                         (asdf:apply-output-translations (second *test-cache-context*))))
       (error "Fast test compilation requires its isolated cache context."))
     (handler-bind ((warning (lambda (condition)
                               (unless (typep condition 'style-warning)
                                 (error condition)))))
       (call-with-policy profile thunk)))))
