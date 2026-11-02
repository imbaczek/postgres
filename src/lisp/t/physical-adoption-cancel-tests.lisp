(in-package #:pg-physical-tests)

(defvar *adoption-cancel-abort-hook* nil)

(defun call-with-adoption-cancel-fixture (function)
  (call-with-adoption-context-fixture
   (lambda (root state proof stage client profile events)
     (declare (ignore proof events))
     (let* ((owner (getf state :owner))
            (record (pg::physical-adoption-attempt-create state stage client profile))
            (original (symbol-function 'pg::pg-client-native-abort))
            (calls 0))
       (pg::physical-adoption-attempt-publish owner record)
       (unwind-protect
            (progn
              (setf (symbol-function 'pg::pg-client-native-abort)
                    (lambda (actual name request)
                      (assert (eq actual client))
                      (assert (equal name (getf (getf record :stage) :source-pin)))
                      (assert (equal request (getf record :request)))
                      (assert (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase)))
                      (incf calls)
                      (when *adoption-cancel-abort-hook*
                        (funcall *adoption-cancel-abort-hook*))
                      t))
              (funcall function root state stage client record (lambda () calls)))
         (setf (symbol-function 'pg::pg-client-native-abort) original))))))

(deftest adoption-cancel-saves-intent-before-abort-and-clears-after-ack
    (call-with-adoption-cancel-fixture
     (lambda (root state stage client record calls)
       (declare (ignore stage record))
       (ok (eq :retired (pg::physical-adoption-attempt-cancel (getf state :owner) :client client)))
       (ok (= 1 (funcall calls)))
       (ok (not (probe-file (pg::physical-adoption-attempt-path root))))
       (ok (not (intake-fails-p #'pg::physical-adoption-check-startup))))))

(deftest adoption-cancel-does-not-require-the-stage-or-fresh-local-data
    (call-with-adoption-cancel-fixture
     (lambda (root state stage client record calls)
       (declare (ignore record calls))
       (uiop:delete-directory-tree (getf stage :directory) :validate t)
       (pg::wal-atomic-write-bytes (merge-pathnames "pg_xact" root) (pg::string->bytes "changed"))
       (let ((*adoption-context-source-case* :promotion))
         (ok (eq :retired (pg::physical-adoption-attempt-cancel (getf state :owner) :client client)))))))

(deftest adoption-cancel-retries-an-uncertain-abort-with-the-saved-request
    (call-with-adoption-cancel-fixture
     (lambda (root state stage client record calls)
       (declare (ignore root stage record))
       (let ((owner (getf state :owner))
             (*adoption-cancel-abort-hook* (lambda () (error "lost abort reply"))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
         (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase)))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))
         (setf *adoption-cancel-abort-hook* nil)
         (ok (eq :retired (pg::physical-adoption-attempt-cancel owner :client client)))
         (ok (= 2 (funcall calls)))))))

(deftest adoption-cancel-refuses-a-wrong-system-or-route-before-abort
    (dolist (mode '(:system :route))
      (call-with-adoption-cancel-fixture
       (lambda (root state stage client record calls)
         (declare (ignore root stage record))
         (let ((owner (getf state :owner))
               (*adoption-context-source-case* mode))
           (when (eq mode :route)
             (incf (pg::physical-libpq-connection-info-port (pg::physical-libpq-channel-info client))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
           (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase)))
           (ok (zerop (funcall calls))))))))

(deftest adoption-cancel-resumes-aborted-without-a-client
    (call-with-adoption-cancel-fixture
     (lambda (root state stage client record calls)
       (declare (ignore root stage record))
       (let* ((owner (getf state :owner))
              (pg::*physical-adoption-attempt-test-hook*
                (lambda (point path)
                  (declare (ignore path))
                  (when (eq point :before-unlink) (error "cut before unlink")))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
         (ok (eq :aborted (getf (pg::physical-adoption-attempt-read owner) :phase)))
         (setf pg::*physical-adoption-attempt-test-hook* nil)
         (ok (eq :retired (pg::physical-adoption-attempt-cancel owner)))
         (ok (= 1 (funcall calls)))))))

(deftest adoption-cancel-reports-uncertain-local-removal-without-recreating-intent
    (dolist (cut '(:after-unlink :after-removal-directory-fsync))
      (call-with-adoption-cancel-fixture
       (lambda (root state stage client record calls)
         (declare (ignore stage record))
         (let* ((owner (getf state :owner))
                (pg::*physical-adoption-attempt-test-hook*
                  (lambda (point path)
                    (declare (ignore path))
                    (when (eq point cut) (error "removal cut")))))
           (ok (handler-case (progn (pg::physical-adoption-attempt-cancel owner :client client) nil)
                 (pg::physical-adoption-retirement-uncertain () t)))
           (ok (not (probe-file (pg::physical-adoption-attempt-path root))))
           (setf pg::*physical-adoption-attempt-test-hook* nil)
           (ok (eq :absent (pg::physical-adoption-attempt-cancel owner)))
           (ok (= 1 (funcall calls))))))))

(cl-spec:defproperty adoption-cancel-keeps-durable-intent-until-remote-ack
    ((cut (range integer 0 6)))
  (:about pg::physical-adoption-attempt-cancel)
  (:trials (:normal 100))
  (call-with-adoption-cancel-fixture
   (lambda (root state stage client record calls)
     (declare (ignore root stage record))
     (let* ((owner (getf state :owner))
            (point (nth cut '(:before-file-fsync :after-file-fsync :before-rename :after-rename
                              :after-directory-fsync :before-readback :after-readback)))
            (pg::*physical-adoption-attempt-test-hook*
              (lambda (where path)
                (declare (ignore path))
                (when (eq where point) (error "abort intent publication cut")))))
       (and (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner :client client)))
            (zerop (funcall calls))
            (member (getf (pg::physical-adoption-attempt-read owner) :phase)
                    '(:preparing :aborting)))))))

(deftest generated-adoption-cancel-intent-publication-cuts
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-cancel-keeps-durable-intent-until-remote-ack :seed 78))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest adoption-cancel-recovers-each-ack-publication-cut
    (dolist (cut '(:before-file-fsync :after-file-fsync :before-rename :after-rename
                   :after-directory-fsync :before-readback :after-readback))
      (call-with-adoption-cancel-fixture
       (lambda (root state stage client record calls)
         (declare (ignore root stage record))
         (let* ((owner (getf state :owner))
                (pg::*physical-adoption-attempt-test-hook*
                  (lambda (point path)
                    (declare (ignore path))
                    (when (and (plusp (funcall calls)) (eq point cut))
                      (error "abort ack publication cut")))))
           (ok (handler-case (progn (pg::physical-adoption-attempt-cancel owner :client client) nil)
                 (pg::physical-adoption-retirement-uncertain () t)))
           (ok (= 1 (funcall calls)))
           (ok (member (getf (pg::physical-adoption-attempt-read owner) :phase) '(:aborting :aborted)))
           (ok (intake-fails-p #'pg::physical-adoption-check-startup))
           (setf pg::*physical-adoption-attempt-test-hook* nil)
           (ok (eq :retired (pg::physical-adoption-attempt-cancel owner :client client))))))))

(deftest adoption-cancel-refuses-corrupt-or-nonprivate-intent-without-network
    (dolist (mode '(:corrupt :permissions))
      (call-with-adoption-cancel-fixture
       (lambda (root state stage client record calls)
         (declare (ignore stage record))
         (let ((path (pg::physical-adoption-attempt-path root)))
           (ecase mode
             (:corrupt (pg::wal-atomic-write-bytes path (pg::string->bytes "corrupt attempt")))
             (:permissions (sb-posix:chmod (namestring path) #o644)))
           (ok (intake-fails-p
                (lambda () (pg::physical-adoption-attempt-cancel (getf state :owner) :client client))))
           (ok (zerop (funcall calls)))
           (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))))

(deftest adoption-cancel-rechecks-saved-intent-before-the-remote-side-effect
    (call-with-adoption-cancel-fixture
     (lambda (root state stage client record calls)
       (declare (ignore root stage record))
       (let* ((owner (getf state :owner))
              (*adoption-context-observation-hook*
                (lambda (point source image)
                  (declare (ignore source image))
                  (when (eq point :identity)
                    (pg::physical-adoption-attempt-advance
                     owner (pg::physical-adoption-attempt-read owner) :aborted)))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
         (ok (zerop (funcall calls)))))))

(deftest adoption-cancel-an-absent-attempt-makes-no-remote-retirement-claim
    (call-with-adoption-context-fixture
     (lambda (root state proof stage client profile events)
       (declare (ignore root proof stage client profile events))
       (ok (eq :absent (pg::physical-adoption-attempt-cancel (getf state :owner)))))))

(deftest adoption-cancel-distinguishes-pre-ack-errors-from-post-ack-cleanup-errors
    (dolist (side '(:before :after))
      (call-with-adoption-cancel-fixture
       (lambda (root state stage client record calls)
         (declare (ignore root stage record))
         (let ((original (symbol-function 'pg::call-with-physical-adoption-attempt-client))
               (owner (getf state :owner)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::call-with-physical-adoption-attempt-client)
                        (lambda (saved supplied function)
                          (when (eq side :before) (error "connect failed"))
                          (funcall original saved supplied function)
                          (error "helper cleanup failed after callback")))
                  (ok (handler-case (progn (pg::physical-adoption-attempt-cancel owner :client client) nil)
                        (pg::physical-adoption-retirement-uncertain () (eq side :after))
                        (simple-error () (eq side :before))))
                  (ok (= (if (eq side :after) 1 0) (funcall calls)))
                  (ok (eq :aborting (getf (pg::physical-adoption-attempt-read owner) :phase))))
             (setf (symbol-function 'pg::call-with-physical-adoption-attempt-client) original)))))))
