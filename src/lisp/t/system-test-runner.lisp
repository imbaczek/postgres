(require :asdf)
(require :sb-posix)

(in-package #:cl-user)

(defvar *pg-test-system* "pg-unit-tests")

(defun pg-test-phase (name function)
  ;; CPU time here belongs to this image; the compiler records its own time.
  (let ((start (get-internal-real-time)) (cpu (get-internal-run-time)))
    (multiple-value-prog1 (funcall function)
      (format t "~&TEST-PHASE ~a wall=~,6f cpu=~,6f~%" name
              (/ (- (get-internal-real-time) start) internal-time-units-per-second)
              (/ (- (get-internal-run-time) cpu) internal-time-units-per-second))
      (finish-output))))

(defun pg-test-start-compiler (source cache log)
  ;; Use a fresh, no-init image and a plain cache, separate from Quicklisp FASLs.
  (let* ((forms
           (list
            '(require :asdf)
            `(asdf:initialize-source-registry
              '(:source-registry (:directory ,source) :ignore-inherited-configuration))
            `(asdf:initialize-output-translations
              '(:output-translations (t ,(namestring cache)) :ignore-inherited-configuration))
            '(setf uiop:*compile-file-warnings-behaviour* :error
              uiop:*compile-file-failure-behaviour* :error)
            '(let ((start (get-internal-real-time)) (cpu (get-internal-run-time)))
              (handler-bind
                  ((warning (lambda (condition)
                              (if (typep condition 'sb-kernel:redefinition-warning)
                                  (muffle-warning condition)
                                  (error "Compiler warning: ~a" condition)))))
                (asdf:compile-system "pg" :force t))
              (format t "~&TEST-PHASE strict-compile wall=~,6f cpu=~,6f~%"
               (/ (- (get-internal-real-time) start) internal-time-units-per-second)
               (/ (- (get-internal-run-time) cpu) internal-time-units-per-second))
              (format t "~&TEST-STRICT-COMPILE-PASS~%"))))
         (arguments
           (let ((*package* (find-package :cl-user))
                 (*print-length* nil) (*print-level* nil) (*print-readably* t))
             (append '("--noinform" "--disable-debugger" "--no-sysinit" "--no-userinit"
                       "--non-interactive")
                     (loop for form in forms append (list "--eval" (write-to-string form)))))))
    (sb-ext:run-program (or (and (uiop:getenv "PG_LISP_TEST_CAMPAIGN")
                                 (uiop:getenv "PG_LISP_TEST_CAMPAIGN_SBCL"))
                            (namestring sb-ext:*runtime-pathname*)) arguments
                        :wait nil :output log :error :output :if-output-exists :error)))

(defun pg-test-finish-compiler (process log)
  ;; Both a successful exit and the final marker are required before tests run.
  (sb-ext:process-wait process)
  (unless (and (eq :exited (sb-ext:process-status process))
               (zerop (sb-ext:process-exit-code process))
               (search "TEST-STRICT-COMPILE-PASS" (uiop:read-file-string log)))
    (error "Strict test compiler failed: ~a~%~a" log (uiop:read-file-string log)))
  (format t "~&TEST-STRICT-LOG ~s~%" (namestring log))
  t)

(defun pg-test-close-compiler (process)
  ;; The runner owns this child even when loading or waiting exits nonlocally.
  (when process
    (when (member (sb-ext:process-status process) '(:running :stopped))
      (sb-ext:process-kill process sb-posix:sigkill))
    (sb-ext:process-wait process)
    (sb-ext:process-close process)))

(defun pg-test-configure-parent-cache (cache)
  (asdf:initialize-output-translations
   `(:output-translations
     ("/" ,(namestring (merge-pathnames "common-lisp/" (uiop:ensure-directory-pathname cache))))
     :ignore-inherited-configuration)))

(defun pg-test-file-digest (path)
  (subseq (uiop:run-program (list "sha256sum" (namestring path)) :output :string) 0 64))

(defun pg-test-image-identity (source)
  (list :format 1 :source (namestring source)
        :implementation (lisp-implementation-version)
        :runtime (namestring sb-ext:*runtime-pathname*)
        :runtime-digest (pg-test-file-digest sb-ext:*runtime-pathname*)
        :core (namestring sb-ext:*core-pathname*)
        :core-digest (pg-test-file-digest sb-ext:*core-pathname*)
        :asdf (asdf:asdf-version)
        :features (sort (mapcar #'prin1-to-string *features*) #'string<)))

(defun pg-test-record-manifest (actual output-variable expected-variable keys)
  "Record or check campaign identity and inventory without reader evaluation."
  (let ((output (uiop:getenv output-variable))
        (expected (uiop:getenv expected-variable)))
    (when output
      (with-open-file (stream output :direction :output :if-exists :error
                                     :if-does-not-exist :create)
        (with-standard-io-syntax
          (let ((*print-circle* t)) (write actual :stream stream) (terpri stream)))))
    (when expected
      (let ((prior
              (with-open-file (stream expected)
                (with-standard-io-syntax
                  (let* ((*read-eval* nil) (end (gensym)) (value (read stream nil end)))
                    (unless (and (not (eq value end)) (eq end (read stream nil end)))
                      (error "Invalid test campaign manifest ~a" expected))
                    value)))))
        (unless (equal (mapcar (lambda (key) (getf actual key)) keys)
                       (mapcar (lambda (key) (getf prior key)) keys))
          (error "Test campaign manifest differs: ~a" expected))))))

(defun pg-test-suite-heads (suite)
  (let ((package (find-package :rove/core/suite/package)))
    (funcall (find-symbol "SUITE-TESTS" package)
             (funcall (find-symbol "FIND-SUITE" package) suite))))

(defun pg-test-record-suite-manifest (suite heads)
  (flet ((name (head)
           (format nil "~a::~a" (package-name (symbol-package head)) (symbol-name head))))
    (pg-test-record-manifest
     (list :format 1 :suite (symbol-name suite)
           :registered (sort (mapcar #'name (pg-test-suite-heads suite)) #'string<)
           :selected (mapcar #'name heads))
     "PG_LISP_TEST_SUITE_OUT" "PG_LISP_TEST_SUITE_EXPECTED"
     '(:format :suite :registered))))

(defun pg-test-selected-heads (suite names)
  ;; Resolve unqualified names against this suite before running any head.
  (let* ((registered (pg-test-suite-heads suite))
         (heads
           (if names
               (mapcar
                (lambda (name)
                  (or (find (string-upcase name) registered :key #'symbol-name :test #'string=)
                      (error "Unknown test ~s in ~a" name suite)))
                names)
               registered)))
    (unless heads (error "No test heads selected for ~a" suite))
    (unless (= (length heads) (length (remove-duplicates heads)))
      (error "Duplicate test heads selected"))
    heads))

(defun pg-test-execute-heads (heads)
  ;; Report every head and retain failures for the final status.
  (let ((failures nil))
    (dolist (head heads)
      (let ((start (get-internal-real-time))
            (ok (uiop:symbol-call :rove :run-test head)))
        (unless ok (push head failures))
        (format t "~&TEST-HEAD ~s ~s wall=~,6f~%" head (not (null ok))
                (/ (- (get-internal-real-time) start) internal-time-units-per-second))
        (finish-output)))
    (format t "~&TEST-RESULT heads=~d failures=~d~%" (length heads) (length failures))
    (null failures)))

(defun pg-test-apply-seed (seed)
  ;; An absent or empty seed leaves the inherited random state alone.
  (when (and seed (plusp (length seed)))
    (let ((value (parse-integer seed)))
      (unless (<= 0 value) (error "Test seed must be nonnegative"))
      (setf *random-state* (sb-ext:seed-random-state value))
      (format t "~&TEST-SEED ~d~%" value))))

(defun run-pg-system-tests (arguments source)
  (let* ((target (or (first arguments) "unit"))
         (system (cond ((string= target "unit") "pg-unit-tests")
                       ((string= target "physical") "pg-physical-tests")
                       (t (error "Expected unit or physical, got ~s" target))))
         (suite (if (string= target "unit") :pg :pg-physical-tests))
         (strict (member "--strict" (rest arguments) :test #'string=))
         (names (remove "--strict" (rest arguments) :test #'string=))
         (work (when strict
                 (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "pg-system-test.XXXXXX" (uiop:temporary-directory)))))))
         (cache (when work (merge-pathnames "plain-cache/" work)))
         (log (when work (merge-pathnames "strict-compile.log" work)))
         (compiler nil) (start (get-internal-real-time)))
    (when (uiop:getenv "PG_LISP_TEST_CAMPAIGN")
      (pg-test-record-manifest
       (pg-test-image-identity source) "PG_LISP_TEST_IDENTITY_OUT" "PG_LISP_TEST_IDENTITY_EXPECTED"
       '(:format :source :implementation :runtime :runtime-digest :core :core-digest :asdf :features))
      (pg-test-configure-parent-cache (uiop:getenv "XDG_CACHE_HOME")))
    (when (member "pg" (asdf:already-loaded-systems) :test #'string=)
      (error "Run system tests in a fresh Lisp without preloaded pg"))
    (pushnew source asdf:*central-registry* :test #'equal)
    (asdf:load-asd (merge-pathnames "pg.asd" source))
    (asdf:load-asd (merge-pathnames (format nil "~a.asd" system) source))
    (setf *print-circle* t *print-level* 6 *print-length* 12)
    ;; Start compilation alongside loading, but wait for it before executing heads.
    ;; Mask spawn and assignment so unwind cleanup always owns a returned child.
    (unwind-protect
         (progn
           (when strict
             (ensure-directories-exist (merge-pathnames "placeholder" cache))
             (sb-sys:without-interrupts
               (setf compiler (pg-test-start-compiler source cache log))))
           (pg-test-phase :production (lambda () (asdf:load-system "pg")))
           (pg-test-phase
            :test-load
            (lambda ()
              (let ((*pg-test-system* system))
                (load (merge-pathnames "t/test-dependencies.lisp" source)))))
           (let ((heads (pg-test-selected-heads suite names)))
             (when (uiop:getenv "PG_LISP_TEST_CAMPAIGN")
               (pg-test-record-suite-manifest suite heads))
             (when compiler
               (pg-test-phase :strict-wait (lambda () (pg-test-finish-compiler compiler log)))
               (pg-test-close-compiler compiler)
               (setf compiler nil))
             (pg-test-apply-seed (uiop:getenv "PG_LISP_TEST_SEED"))
             (multiple-value-prog1
                 (pg-test-phase
                  :execution
                  (lambda ()
                    (let ((cache-symbol
                            (when (eq suite :pg-physical-tests)
                              (find-symbol "*PROMOTION-TEST-PLAIN-CACHE*" :pg-physical-tests))))
                      (when (and strict (eq suite :pg-physical-tests) (null cache-symbol))
                        (error "Physical child cache binding is unavailable"))
                      ;; Promotion handoff children use the plain compiler cache,
                      ;; separate from any Quicklisp-initialized parent FASLs.
                      (progv (when cache-symbol (list cache-symbol)) (when cache-symbol (list cache))
                        (pg-test-execute-heads heads)))))
               (format t "~&TEST-TOTAL wall=~,6f~%"
                       (/ (- (get-internal-real-time) start) internal-time-units-per-second)))))
      (pg-test-close-compiler compiler))))
