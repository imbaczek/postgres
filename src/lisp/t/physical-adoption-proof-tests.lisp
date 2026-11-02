(in-package #:pg-physical-tests)

(defvar *adoption-proof-driver-hook* nil)

(defun call-with-adoption-proof-driver-fixture (function &key (fork-distance 64) (generations 1))
  (call-with-adoption-context-fixture
   (lambda (root state origin stage client profile events)
     (declare (ignore events))
     (let* ((record (pg::physical-adoption-attempt-create state stage client profile))
            (prepare (symbol-function 'pg::pg-client-native-prepare))
            (prove (symbol-function 'pg::pg-client-native-prove-retained))
            (prepares 0)
            (proofs 0))
       (unwind-protect
            (progn
              (setf (symbol-function 'pg::pg-client-native-prepare)
                    (lambda (actual name coordinates)
                      (assert (eq actual client))
                      (assert (equal name (getf (getf record :stage) :source-pin)))
                      (assert (equal coordinates (getf record :request)))
                      (assert (member (getf (pg::physical-adoption-attempt-read (getf state :owner)) :phase)
                                      '(:preparing :proved)))
                      (incf prepares)
                      (let ((anchor (pg::physical-follow-anchor-copy
                                     (append (copy-tree coordinates)
                                             (list :history-text (getf record :source-history)
                                                   :eor-octets (coerce (pg::wal-encode-end-of-recovery-record
                                                                        (+ fork-distance (getf state :end)) 42
                                                                        (1+ (getf state :timeline)) (getf state :timeline) 1) 'list))))))
                        (when *adoption-proof-driver-hook*
                          (funcall *adoption-proof-driver-hook* :prepare anchor root stage))
                        anchor))
                    (symbol-function 'pg::pg-client-native-prove-retained)
                    (lambda (actual name anchor request)
                      (assert (eq actual client))
                      (assert (equal request
                                     (list :system-id (first (getf origin :identity))
                                           :base-timeline (second (getf origin :identity))
                                           :base (third (getf origin :identity))
                                           :manifest-digest (fourth (getf origin :identity))
                                           :end (getf origin :end))))
                      (incf proofs)
                      (let ((proof
                              (list :slot-name name :anchor-digest (pg::physical-follow-anchor-digest anchor)
                                    :system-id (getf request :system-id) :base-timeline (getf request :base-timeline)
                                    :base (getf request :base) :manifest-digest (getf request :manifest-digest)
                                    :end (getf request :end) :end-digest (getf origin :end-digest)
                                    :root-id 456 :fork
                                    (pg::timeline-history-entry-begin
                                     (first (pg::timeline-parse-history
                                             (getf anchor :source-timeline) (getf anchor :history-text))))
                                    :source-timeline (getf anchor :source-timeline)
                                    :history-text (getf record :source-history))))
                        (when *adoption-proof-driver-hook*
                          (funcall *adoption-proof-driver-hook* :prove proof root stage))
                        proof)))
              (funcall function root state stage client profile record
                       (lambda () (list prepares proofs))))
         (setf (symbol-function 'pg::pg-client-native-prepare) prepare
               (symbol-function 'pg::pg-client-native-prove-retained) prove))))
   :fork-distance fork-distance :generations generations))

(deftest adoption-proof-driver-saves-intent-and-proves-the-exact-request
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root record))
       (let* ((owner (getf state :owner))
              (proved (pg::physical-adoption-attempt-start-proof state stage client profile)))
         (ok (eq :proved (getf proved :phase)))
         (ok (equal proved (pg::physical-adoption-attempt-read owner)))
         (ok (equal '(1 1) (funcall counts)))
         (ok (equal proved (pg::physical-adoption-attempt-resume-proof owner :client client)))
         (ok (equal '(2 2) (funcall counts)))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))

(deftest adoption-proof-driver-retries-lost-prepare-or-proof-replies
    (dolist (cut '(:prepare :prove))
      (call-with-adoption-proof-driver-fixture
       (lambda (root state stage client profile record counts)
         (declare (ignore root record))
         (let* ((owner (getf state :owner))
                (*adoption-proof-driver-hook*
                  (lambda (point reply source image)
                    (declare (ignore reply source image))
                    (when (eq point cut)
                      (error 'pg::pg-client-error :sqlstate "08006" :message "lost reply")))))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile))))
           (ok (eq :preparing (getf (pg::physical-adoption-attempt-read owner) :phase)))
           (setf *adoption-proof-driver-hook* nil)
           (ok (eq :proved (getf (pg::physical-adoption-attempt-resume-proof owner :client client) :phase)))
           (ok (equal (if (eq cut :prepare) '(2 1) '(2 2)) (funcall counts))))))))

(deftest adoption-proof-driver-rejects-actual-history-or-proof-mismatch-before-detaching
    (dolist (mode '(:anchor-history :proof-history :digest :root-on-retry))
      (call-with-adoption-proof-driver-fixture
       (lambda (root state stage client profile record counts)
         (declare (ignore root record counts))
         (let ((owner (getf state :owner)))
           (if (eq mode :root-on-retry)
               (pg::physical-adoption-attempt-start-proof state stage client profile)
               (pg::physical-adoption-attempt-publish
                owner (pg::physical-adoption-attempt-create state stage client profile)))
           (let ((*adoption-proof-driver-hook*
                   (lambda (point reply source image)
                     (declare (ignore source image))
                     (cond
                       ((or (and (eq mode :anchor-history) (eq point :prepare))
                            (and (eq mode :proof-history) (eq point :prove)))
                        (setf (getf reply :history-text)
                              (concatenate 'string (getf reply :history-text) (format nil "# changed~%"))))
                       ((and (eq mode :digest) (eq point :prove)) (incf (getf reply :end-digest)))
                       ((and (eq mode :root-on-retry) (eq point :prove)) (incf (getf reply :root-id)))))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
             (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase)))))))))

(deftest adoption-proof-driver-rejects-late-stage-or-source-identity-drift
    (dolist (mode '(:stage :promotion))
      (call-with-adoption-proof-driver-fixture
       (lambda (root state stage client profile record counts)
         (declare (ignore root record counts))
         (let* ((owner (getf state :owner))
                (*adoption-proof-driver-hook*
                  (lambda (point reply source image)
                    (declare (ignore reply source))
                    (when (eq point :prove)
                      (if (eq mode :stage)
                          (pg::wal-atomic-write-bytes (merge-pathnames "pg_xact" (getf image :directory))
                                                      (pg::string->bytes "changed after proof"))
                          (setf *adoption-context-source-case* :promotion))))))
           (let ((*adoption-context-source-case* nil))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile)))))
           (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase))))))))

(deftest adoption-proof-driver-does-not-prepare-before-durable-intent-readback
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root record))
       (let* ((owner (getf state :owner))
              (pg::*physical-adoption-attempt-test-hook*
                (lambda (point path)
                  (declare (ignore path))
                  (when (eq point :after-readback) (error "initial publication outcome lost")))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile))))
         (ok (equal '(0 0) (funcall counts)))
         (setf pg::*physical-adoption-attempt-test-hook* nil)
         (ok (eq :proved (getf (pg::physical-adoption-attempt-resume-proof owner :client client) :phase)))))))

(deftest adoption-proof-driver-leaves-ambiguous-proved-publication-for-exact-resume
    (dolist (cut '(:before-rename :after-rename :after-readback))
      (call-with-adoption-proof-driver-fixture
       (lambda (root state stage client profile record counts)
         (declare (ignore root record))
         (let* ((owner (getf state :owner))
                (pg::*physical-adoption-attempt-test-hook*
                  (lambda (point path)
                    (declare (ignore path))
                    (when (and (plusp (second (funcall counts))) (eq point cut))
                      (error "proved publication outcome lost")))))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile))))
           (ok (member (getf (pg::physical-adoption-attempt-read owner) :phase) '(:preparing :proved)))
           (setf pg::*physical-adoption-attempt-test-hook* nil)
           (ok (eq :proved (getf (pg::physical-adoption-attempt-resume-proof owner :client client) :phase))))))))

(deftest adoption-proof-driver-refuses-cancellation-phases
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root stage profile))
       (let ((owner (getf state :owner)))
         (pg::physical-adoption-attempt-publish owner record)
         (pg::physical-adoption-attempt-advance owner record :aborting)
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
         (ok (equal '(0 0) (funcall counts)))
         (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase)))))))

(cl-spec:defproperty adoption-proof-sends-nothing-before-intent-publication-completes
    ((cut (range integer 0 6)))
  (:about pg::physical-adoption-attempt-start-proof)
  (:trials (:normal 100))
  (call-with-adoption-proof-driver-fixture
   (lambda (root state stage client profile record counts)
     (declare (ignore root))
     (let* ((owner (getf state :owner))
            (point (nth cut '(:before-file-fsync :after-file-fsync :before-rename :after-rename
                              :after-directory-fsync :before-readback :after-readback)))
            (pg::*physical-adoption-attempt-test-hook*
              (lambda (actual path)
                (declare (ignore path))
                (when (eq point actual) (error "intent publication cut")))))
       (and (intake-fails-p
             (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile)))
            (equal '(0 0) (funcall counts))
            (let ((saved (pg::physical-adoption-attempt-read-if-present owner)))
              (or (null saved) (equal record saved))))))))

(deftest generated-adoption-proof-intent-publication-cuts
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-proof-sends-nothing-before-intent-publication-completes :seed 79))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty adoption-proof-keeps-transport-ambiguity-separate-from-rejection
    ((index (range integer 0 8)))
  (:about pg::physical-adoption-attempt-resume-proof)
  (:trials (:normal 100))
  (call-with-adoption-proof-driver-fixture
   (lambda (root state stage client profile record counts)
     (declare (ignore root record counts))
     (let* ((code (nth index '("08001" "08006" "57P01" "57P03" "53300" "55006" "08P01" "55000" "42501")))
            (retryable (member code '("08001" "08006" "57P01" "57P03" "53300" "55006") :test #'equal))
            (*adoption-proof-driver-hook*
              (lambda (point reply source image)
                (declare (ignore reply source image))
                (when (eq point :prepare)
                  (error 'pg::pg-client-error :sqlstate code :message "source failure")))))
       (and (intake-fails-p
             (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile)))
            (eq (if retryable :preparing :aborting)
                (getf (pg::physical-adoption-attempt-read (getf state :owner)) :phase)))))))

(deftest generated-adoption-proof-error-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-proof-keeps-transport-ambiguity-separate-from-rejection :seed 80))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest adoption-proof-local-stream-failure-is-terminal
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root stage profile))
       (let ((owner (getf state :owner))
             (original (symbol-function 'pg::physical-adoption-attempt-validate-context)))
         (pg::physical-adoption-attempt-publish owner record)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-adoption-attempt-validate-context)
                      (lambda (actual saved)
                        (declare (ignore actual saved))
                        (read-char (make-string-input-stream ""))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
                (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase)))
                (ok (equal '(0 0) (funcall counts))))
           (setf (symbol-function 'pg::physical-adoption-attempt-validate-context) original))))))

(deftest adoption-proof-checks-source-before-any-prepare
    (dolist (mode '(:system :promotion :route :history))
      (call-with-adoption-proof-driver-fixture
       (lambda (root state stage client profile record counts)
         (declare (ignore root stage profile))
         (let ((owner (getf state :owner))
               (*adoption-context-source-case* mode))
           (pg::physical-adoption-attempt-publish owner record)
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
           (ok (equal '(0 0) (funcall counts)))
           (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase))))))))

(deftest adoption-proof-classifies-permanent-and-busy-errors-at-either-source-command
    (dolist (point '(:prepare :prove))
      (dolist (code '("08P01" "55006"))
        (call-with-adoption-proof-driver-fixture
         (lambda (root state stage client profile record counts)
           (declare (ignore root record counts))
           (let ((*adoption-proof-driver-hook*
                   (lambda (actual reply source image)
                     (declare (ignore reply source image))
                     (when (eq actual point)
                       (error 'pg::pg-client-error :sqlstate code :message "source command failure")))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile))))
             (ok (eq (if (equal code "55006") :preparing :aborting)
                     (getf (pg::physical-adoption-attempt-read (getf state :owner)) :phase)))))))))

(deftest adoption-proof-client-cleanup-error-preserves-the-saved-phase
    (dolist (phase '(:preparing :proved))
      (call-with-adoption-proof-driver-fixture
       (lambda (root state stage client profile record counts)
         (declare (ignore root counts))
         (let* ((owner (getf state :owner))
                (saved (if (eq phase :proved)
                           (pg::physical-adoption-attempt-start-proof state stage client profile)
                           (pg::physical-adoption-attempt-publish owner record)))
                (original (symbol-function 'pg::call-with-physical-adoption-attempt-client)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::call-with-physical-adoption-attempt-client)
                        (lambda (actual supplied function)
                          (funcall original actual supplied function)
                          (error "helper cleanup failed")))
                  (ok (intake-fails-p
                       (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
                  (ok (equal saved (pg::physical-adoption-attempt-read owner))))
             (setf (symbol-function 'pg::call-with-physical-adoption-attempt-client) original)))))))
