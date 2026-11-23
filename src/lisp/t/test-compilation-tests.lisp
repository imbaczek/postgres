(defpackage #:pg-test-compilation-tests
  (:use #:cl #:rove))
(in-package #:pg-test-compilation-tests)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-cltl2))

(cl-spec:defproperty test-compilation-profile-has-two-cache-namespaces
    ((mode (range integer 0 2)) (suffix (range integer 0 1000000)))
  (:about pg-test-compilation:cache-translations)
  (:trials (:normal 200))
  (let* ((profile (pg-test-compilation:profile (nth mode '(nil "default" "fast"))))
         (cache (pathname (format nil "/tmp/cache ~d/" suffix)))
         (source (pathname (format nil "/tmp/source ~d/" suffix)))
         (config (pg-test-compilation:cache-translations cache source profile)))
    (and (eq profile (if (= mode 2) :fast :default))
         (equal (first config) :output-translations)
         (equal (first (second config)) (namestring (merge-pathnames "t/" source)))
         (search (if (= mode 2) "tests-fast-v2/" "tests-default-v2/") (second (second config)))
         (equal (first (third config)) (namestring source))
         (equal (fourth config)
                (list "/" (namestring
                           (merge-pathnames
                            (if (member :quicklisp *features*)
                                "common-lisp/test-dependencies-v1/quicklisp/"
                                "common-lisp/test-dependencies-v1/plain/")
                            cache))))
         (eq (fifth config) :ignore-inherited-configuration))))

(deftest test-compilation-profile-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'test-compilation-profile-has-two-cache-namespaces :seed 10754))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest test-compilation-policy-restores-qualities-and-restrictions
    (dolist (exit '(:normal :error :throw))
      (let ((before (sb-cltl2:declaration-information 'optimize))
            (limits (multiple-value-list (sb-ext:restrict-compiler-policy)))
            (inside nil) (entered nil) (result nil))
        (setf result
              (handler-case
                  (catch 'policy-exit
                    (pg-test-compilation:call-with-policy
                     :fast
                     (lambda ()
                       (setf entered t inside (sb-cltl2:declaration-information 'optimize))
                       (ecase exit
                         (:normal :normal)
                         (:error (error "policy probe"))
                         (:throw (throw 'policy-exit :throw))))))
                (error (condition) (if (eq exit :error) :error (error condition)))))
        (ok entered)
        (ok (eq result exit))
        (ok (= 3 (second (assoc 'compilation-speed inside))))
        (ok (= 1 (second (assoc 'speed inside))))
        (ok (= 3 (second (assoc 'safety inside))))
        (ok (eql (second (assoc 'debug before)) (second (assoc 'debug inside))))
        (ok (equal before (sb-cltl2:declaration-information 'optimize)))
        (ok (equal limits (multiple-value-list (sb-ext:restrict-compiler-policy))))))
  (ok (equal '(1 nil 3)
             (multiple-value-list
              (pg-test-compilation:call-with-policy :fast (lambda () (values 1 nil 3)))))))

(deftest test-compilation-profile-rejects-unknown-names-and-relative-roots
    (dolist (name '("" "FAST" "other" :fast))
      (ok (handler-case (progn (pg-test-compilation:profile name) nil) (error () t))))
  (ok (handler-case
          (progn (pg-test-compilation:cache-translations #p"relative/" #p"/tmp/source/" :fast) nil)
        (error () t)))
  (ok (handler-case
          (progn (pg-test-compilation:cache-translations #p"/tmp/cache/" #p"relative/" :default) nil)
        (error () t))))

(deftest test-compilation-cache-isolates-tests-and-shares-production
    (let ((variable (find-symbol "*OUTPUT-TRANSLATIONS*" :asdf/output-translations))
          (*features* (remove :quicklisp *features*))
          (production nil))
      (dolist (profile '(:default :fast))
        (progv (list variable) '(nil)
          (asdf:initialize-output-translations
           (pg-test-compilation:cache-translations
            #p"/tmp/policy-cache/" #p"/tmp/policy-source/" profile))
          (let ((test-path (asdf:apply-output-translations #p"/tmp/policy-source/t/nested/probe.fasl"))
                (pg-path (asdf:apply-output-translations #p"/tmp/policy-source/probe.fasl")))
            (ok (equal test-path
                       (if (eq profile :fast)
                           #p"/tmp/policy-cache/common-lisp/tests-fast-v2/2/tmp/policy-source/plain/nested/probe.fasl"
                           #p"/tmp/policy-cache/common-lisp/tests-default-v2/2/tmp/policy-source/plain/nested/probe.fasl")))
            (pg-test-compilation:call-with-cache
             profile #p"/tmp/policy-cache/" #p"/tmp/policy-source/nested/"
             (lambda ()
               (let ((other-path
                       (asdf:apply-output-translations #p"/tmp/policy-source/nested/t/probe.fasl")))
                 (ok (equal other-path
                            (merge-pathnames
                             (if (eq profile :fast)
                                 "tests-fast-v2/3/tmp/policy-source/nested/plain/probe.fasl"
                                 "tests-default-v2/3/tmp/policy-source/nested/plain/probe.fasl")
                             #p"/tmp/policy-cache/common-lisp/")))
                 (ok (not (equal other-path test-path))))))
            (ok (equal pg-path #p"/tmp/policy-cache/common-lisp/tmp/policy-source/probe.fasl"))
            (when production (ok (equal production pg-path)))
            (setf production pg-path))))))

(deftest test-compilation-cache-tracks-reader-context
    (flet ((outputs (quicklisp)
             (let ((*features* (if quicklisp (adjoin :quicklisp *features*)
                                   (remove :quicklisp *features*))))
               (pg-test-compilation:call-with-cache
                :default #p"/tmp/policy-cache/" #p"/tmp/policy-source/"
                (lambda ()
                  (mapcar #'asdf:apply-output-translations
                          '(#p"/tmp/dependencies/rove/reporter.fasl"
                            #p"/tmp/policy-source/t/probe.fasl"
                            #p"/tmp/policy-source/probe.fasl")))))))
      (destructuring-bind (plain-dependency plain-test plain-production) (outputs nil)
        (destructuring-bind (ql-dependency ql-test ql-production) (outputs t)
          (ok (not (equal plain-dependency ql-dependency)))
          (ok (not (equal plain-test ql-test)))
          (ok (equal plain-production ql-production))
          (ok (equal (outputs nil) (list plain-dependency plain-test plain-production)))))))

(deftest test-compilation-hook-preserves-warning-fatality
    (let ((variable (find-symbol "*OUTPUT-TRANSLATIONS*" :asdf/output-translations)))
      (progv (list variable) '(nil)
        (pg-test-compilation:call-with-cache
         :fast #p"/tmp/policy-cache/" #p"/tmp/policy-source/"
         (lambda ()
           (ok (handler-case
                   (progn (pg-test-compilation:around-compile
                           (lambda () (warn "full warning")) :fast) nil)
                 (warning () t)))
           (ok (handler-bind ((style-warning #'muffle-warning))
                 (eq :finished
                     (pg-test-compilation:around-compile
                      (lambda ()
                        (warn 'sb-int:simple-style-warning :format-control "style warning")
                        :finished)
                      :fast))))
           (asdf:initialize-output-translations
            '(:output-translations (t "/tmp/wrong-cache/") :ignore-inherited-configuration))
           (ok (handler-case
                   (progn (pg-test-compilation:around-compile (lambda () :wrong) :fast) nil)
                 (error () t))))))
      (ok (handler-case
              (progn (pg-test-compilation:around-compile (lambda () :unguarded) :fast) nil)
            (error () t)))
      (ok (handler-bind ((warning #'muffle-warning))
            (eq :finished
                (pg-test-compilation:around-compile
                 (lambda () (warn "default warning") :finished) :default))))))

(deftest test-compilation-cache-context-unwinds
    (let ((variable (find-symbol "*OUTPUT-TRANSLATIONS*" :asdf/output-translations)))
      (dolist (exit '(:normal :error :throw))
        (let ((before (symbol-value variable)) (entered nil))
          (ok (eq exit
                  (handler-case
                      (catch 'cache-exit
                        (pg-test-compilation:call-with-cache
                         :fast #p"/tmp/policy-cache/" #p"/tmp/policy-source/"
                         (lambda ()
                           (setf entered t)
                           (ok (eq :inside
                                   (pg-test-compilation:around-compile (lambda () :inside) :fast)))
                           (ecase exit
                             (:normal :normal)
                             (:error (error "cache probe"))
                             (:throw (throw 'cache-exit :throw))))))
                    (error (condition) (if (eq exit :error) :error (error condition))))))
          (ok entered)
          (ok (eq before (symbol-value variable)))
          (ok (handler-case
                  (progn (pg-test-compilation:around-compile (lambda () :escaped) :fast) nil)
                (error () t)))))))
