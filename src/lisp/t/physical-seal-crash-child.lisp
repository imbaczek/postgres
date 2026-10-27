;;;; Crash a task-owned receive compaction at a requested durability boundary.

(require :asdf)

(let ((setup (merge-pathnames "quicklisp/setup.lisp" (user-homedir-pathname))))
  (when (probe-file setup) (load setup)))

(push (uiop:pathname-parent-directory-pathname (uiop:pathname-directory-pathname *load-truename*)) asdf:*central-registry*)

(push (pathname (third (uiop:command-line-arguments))) asdf:*central-registry*)

(asdf:load-system "pg-physical-tests")

(defun physical-seal-crash-main ()
  (let* ((args (uiop:command-line-arguments)) (marker (pathname (first args)))
                                              (token (second args)) (root-writes 0)
                                              (physical-point (find token '(:after-retained-inventory-publication :after-restartpoint-publication
                                                                            :before-control-rename :after-control-rename :after-control-directory-fsync
                                                                            :after-retained-root-handoff :after-floor-publication
                                                                            :after-obsolete-segment-delete-each)
                                                                    :key #'symbol-name :test #'string-equal))
                                              (inventory-point (when (string-equal token "STAGE-AFTER-EXTENT-PUBLICATION") :after-extent-publication))
                                              (restartpoint-point (cond ((string-equal token "RP-AFTER-GENERATION") :after-generation)
                                                                        ((string-equal token "RP-AFTER-POINTER") :after-pointer))))
    (unless (or physical-point inventory-point restartpoint-point) (error "unknown sealing crash point"))
    (pg-physical-tests::call-with-restartpoint-fixture
     (lambda (root store)
       (flet ((mark (point)
                (pg::wal-atomic-write-bytes
                 marker (pg::string->bytes
                         (pg::physical-bootstrap-manifest-text
                          (list :directory (namestring root) :point point :pid (sb-posix:getpid) :root-writes root-writes)))))
              (crash () (sb-posix:kill (sb-posix:getpid) 9)))
         (mark "STARTED")
         (setf (pg::physical-store-retained-identity store) (pg::physical-recovery-retained-identity))
         (let ((cursor (if (eq physical-point :after-obsolete-segment-delete-each)
                           (pg-physical-tests::seal-test-append-segments store 2) 96)))
           (setf pg::*last-replay-lsn* cursor)
           (let ((pg::*physical-store-test-hook*
                   (lambda (point ignored)
                     (declare (ignore ignored))
                     (when (eq point physical-point) (mark token) (crash))))
                 (pg::*physical-retained-inventory-test-hook*
                   (lambda (point id)
                     (declare (ignore id))
                     (when (eq point :after-inventory-publication) (incf root-writes))
                     (when (eq point inventory-point) (mark token) (crash))))
                 (pg::*physical-restartpoint-test-hook*
                   (lambda (point) (when (eq point restartpoint-point) (mark token) (crash)))))
             (pg::physical-store-compact store cursor #'pg::physical-restartpoint-publish)))))
     :system-id 123)
    (error "sealing crash point was not reached")))

(physical-seal-crash-main)
