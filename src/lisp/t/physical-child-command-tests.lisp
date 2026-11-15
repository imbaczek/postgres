(in-package #:pg-physical-tests)

(cl-spec:defproperty physical-child-cache-command-roundtrips
    ((suffix (range integer 0 1000000000)))
  (:about physical-test-child-command)
  (:trials (:normal 200))
  (let* ((*physical-test-plain-cache*
           (pathname (format nil "/tmp/pg child ~d/quote\" dollar$/" suffix)))
         (argv (list "owned-sbcl" "--script" "child.lisp" (format nil "argument ~d" suffix)))
         (command (physical-test-child-command argv))
         (*read-eval* nil)
         (*package* (find-package :cl-user))
         (config (read-from-string
                  (subseq (second command) (length "ASDF_OUTPUT_TRANSLATIONS=")))))
    (and (equal "env" (first command))
         (equal argv (cdddr command))
         (equal "PG_LISP_TEST_CHILD_CACHE_EXPECTED=1" (third command))
         (equal config
                (list :output-translations
                      (list t (namestring (uiop:ensure-directory-pathname *physical-test-plain-cache*)))
                      :ignore-inherited-configuration)))))

(deftest physical-child-cache-command-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-child-cache-command-roundtrips :seed 10753))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest physical-child-command-keeps-default-argv-and-parent-environment
    (let* ((argv (list "owned-sbcl" "--script" "child with spaces.lisp" "$(literal)"))
           (saved (copy-tree argv))
           (before (uiop:getenv "ASDF_OUTPUT_TRANSLATIONS")))
      (let ((*physical-test-plain-cache* nil))
        (ok (eq argv (physical-test-child-command argv))))
      (let ((*physical-test-plain-cache* #p"/tmp/strict plain cache/"))
        (ok (equal argv (cdddr (physical-test-child-command argv)))))
      (ok (equal saved argv))
      (ok (equal before (uiop:getenv "ASDF_OUTPUT_TRANSLATIONS")))))

(deftest physical-child-command-rejects-shell-strings-and-invalid-argv
    (let ((cycle (list "owned-sbcl")))
      (setf (cdr cycle) cycle)
      (dolist (argv (list nil "sbcl --script unsafe.lisp" '("owned-sbcl" 42)
                          (list "owned-sbcl" (string (code-char 0))) cycle))
        (ok (handler-case (progn (physical-test-child-command argv) nil) (error () t)))))
  (let ((*physical-test-plain-cache* #p"relative-cache/"))
    (ok (handler-case (progn (physical-test-child-command '("owned-sbcl")) nil) (error () t)))))
