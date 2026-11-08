(in-package #:pg-physical-tests)

(defun configured-cancellation-test-child-arguments (root cut cache marker form)
  (let* ((arguments (retirement-intent-test-child-arguments root cut cache marker))
         (*package* (find-package :cl-user))
         (*print-length* nil) (*print-level* nil) (*print-readably* t)
         (old-load (write-to-string '(asdf:load-system "pg-physical-tests")))
         (new-load (write-to-string '(asdf:load-system "pg"))))
    (unless (= 1 (count old-load arguments :test #'string=))
      (error "configured cancellation child lacks its expected system load"))
    (append (substitute new-load old-load (butlast arguments 2) :test #'string=)
            (list "--eval" "(make-package \"PG-PHYSICAL-TESTS\" :use '(\"CL\"))"
                  "--eval" (write-to-string form)))))

(defun configured-cancellation-test-crash-child-arguments (root cut cache marker)
  (configured-cancellation-test-child-arguments
   root cut cache marker
   `(let ((pg::*pgdata* ,root) (connections 0))
      (assert (equal (truename (asdf:system-source-directory "pg"))
                     ,(truename (asdf:system-source-directory "pg"))))
      (setf (symbol-function 'pg::physical-libpq-connect-frozen)
            (lambda (&rest arguments)
              (declare (ignore arguments))
              (incf connections)
              (error "configured cancellation crash child contacted its source")))
      (let ((pg::*physical-reseed-follow-write-limit* 257)
            (pg::*physical-reseed-follow-test-hook*
              (lambda (event path)
                (declare (ignore path))
                (when (eq event ,cut)
                  (assert (zerop connections))
                  (with-open-file (output ,marker :direction :output :if-exists :error)
                    (with-standard-io-syntax
                      (write (list (sb-posix:getpid) ,cut) :stream output)))
                  (format t "~%CONFIGURED-CANCELLATION-CRASH-REACHED ~d ~s~%"
                          (sb-posix:getpid) ,cut)
                  (finish-output)
                  (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                  (error "configured cancellation child SIGKILL returned")))))
        (pg::call-with-physical-reseed-follow-cancellation-owner
         ,root #'pg::physical-reseed-follow-begin-cancellation))
      (error "configured cancellation child did not reach its crash point"))))

(defun configured-cancellation-test-recovery-child-form (root cut pending marker)
  (let ((partial (member cut '(:after-follow-temporary-create :after-follow-write-chunk)))
        (absent (eq cut :before-follow-temporary-create)))
    `(let ((pg::*pgdata* ,root) (connections 0) (evidence nil))
       (assert (equal (truename (asdf:system-source-directory "pg"))
                      ,(truename (asdf:system-source-directory "pg"))))
       (flet ((check (value)
                (assert value () "configured recovery ~s check ~d failed"
                        ,cut (1+ (length evidence)))
                (push t evidence)))
         (setf (symbol-function 'pg::physical-libpq-connect-frozen)
               (lambda (&rest arguments)
                 (declare (ignore arguments))
                 (incf connections)
                 (error "configured cancellation recovery child contacted its source")))
         (check (handler-case (progn (pg::physical-reseed-check-before-pid) nil) (error () t)))
         ,(if partial
              `(let ((admitted nil))
                 (check (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-cancellation-owner
                               ,root (lambda (owner) (declare (ignore owner)) (setf admitted t)))
                              nil)
                          (error () t)))
                 (check (not admitted))
                 (check (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-cancellation-owner
                               ,root (lambda (owner) (declare (ignore owner)) (setf admitted t))
                               :recover t)
                              nil)
                          (error () t)))
                 (check (not admitted)))
              `(pg::call-with-physical-reseed-follow-cancellation-owner
                ,root
                (lambda (owner)
                  ,@(when absent
                      `((check (equal ',pending (pg::physical-reseed-follow-begin-cancellation owner)))))
                  (check (equal ',pending (pg::physical-reseed-follow-read-configured-cancellation owner)))
                  (check (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                  (check (not (pg::physical-reseed-follow-owner-busy-p owner))))
                :recover ,(not absent)))
         (check (handler-case (progn (pg::physical-reseed-check-before-pid) nil) (error () t)))
         (check (zerop connections))
         (with-open-file (output ,marker :direction :output :if-exists :error)
           (with-standard-io-syntax
             (write (list (sb-posix:getpid) ,cut (nreverse evidence)) :stream output)))
         (format t "~%CONFIGURED-CANCELLATION-RECOVERY-PASS ~d ~s~%" (sb-posix:getpid) ,cut)
         (finish-output)))))

(defun run-configured-cancellation-test-recovery-child (root cut pending cache marker log)
  (let* ((form (configured-cancellation-test-recovery-child-form root cut pending marker))
         (arguments (configured-cancellation-test-child-arguments root cut cache marker form))
         (checks (if (member cut '(:before-follow-temporary-create
                                   :after-follow-temporary-create :after-follow-write-chunk)) 7 6))
         (process nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process (sb-ext:run-program
                            (namestring sb-ext:*runtime-pathname*) arguments
                            :wait nil :output log :error :output :if-output-exists :error)))
           (format t "~%CONFIGURED-CANCELLATION-RECOVERY-OWNED ~d ~s~%" (sb-ext:process-pid process) cut)
           (finish-output)
           (loop with deadline = (+ (get-internal-real-time) (* 600 internal-time-units-per-second))
                 while (member (sb-ext:process-status process) '(:running :stopped))
                 do (when (>= (get-internal-real-time) deadline)
                      (error "configured cancellation recovery child exceeded its deadline"))
                    (sleep 0.05))
           (sb-ext:process-wait process)
           (let ((output (uiop:read-file-string log)))
             (write-string output)
             (unless (and (eq :exited (sb-ext:process-status process))
                          (zerop (sb-ext:process-exit-code process))
                          (search (format nil "CONFIGURED-CANCELLATION-RECOVERY-PASS ~d ~s"
                                          (sb-ext:process-pid process) cut)
                                  output))
               (error "configured cancellation recovery child failed:~%~a"
                      (subseq output (max 0 (- (length output) 12000))))))
           (with-open-file (input marker)
             (let ((*read-eval* nil) (*package* (find-package :cl-user)))
               (assert (equal (list (sb-ext:process-pid process) cut (make-list checks :initial-element t))
                              (read input)))
               (assert (eq :eof (read input nil :eof)))))
           (format t "~%CONFIGURED-CANCELLATION-RECOVERY-VERIFIED ~d ~s~%"
                   (sb-ext:process-pid process) cut)
           (finish-output)
           (sb-ext:process-pid process))
      (when process
        (when (member (sb-ext:process-status process) '(:running :stopped))
          (sb-ext:process-kill process sb-posix:sigkill))
        (sb-ext:process-wait process)
        (sb-ext:process-close process)))))

(defun check-configured-cancellation-native-cuts (cuts)
  (with-physical-store-directory (driver)
    (let ((cache (merge-pathnames "native-cache/" driver)))
      (dolist (cut cuts)
        (testing (symbol-name cut)
                 (call-with-reseed-inert-settlement-fixture
                  :same-timeline
                  (lambda (owner root workspace) (declare (ignore owner root workspace)))
                  (lambda (root workspace)
                    (declare (ignore workspace))
                    (let* ((final (merge-pathnames "physical-reseed.follow" root))
                           (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                           (before (lineage-test-bytes final))
                           (record (pg::physical-reseed-follow-decode before))
                           (pending (pg::physical-reseed-follow-cancellation-begin-record record))
                           (marker (merge-pathnames (format nil "~(~a~).crash" cut) driver))
                           (result (merge-pathnames (format nil "~(~a~).result" cut) driver))
                           (log (merge-pathnames (format nil "~(~a~).log" cut) driver))
                           (crash-log (merge-pathnames (format nil "~(~a~).crash.log" cut) driver))
                           (facts (configured-cancellation-test-preserved-file-facts root))
                           (crash-pid
                             (unwind-protect
                                  (handler-case
                                      (run-retirement-intent-test-crash-child
                                       root cut cache marker :timeout 600 :output crash-log
                                       :argument-builder #'configured-cancellation-test-crash-child-arguments)
                                    (error (condition)
                                      (let ((output (if (probe-file crash-log)
                                                        (uiop:read-file-string crash-log) "no child log")))
                                        (error "configured cancellation crash child failed: ~a~%~a"
                                               condition
                                               (subseq output (max 0 (- (length output) 12000)))))))
                               (when (probe-file crash-log)
                                 (write-string (uiop:read-file-string crash-log)))))
                           (partial (member cut '(:after-follow-temporary-create :after-follow-write-chunk)))
                           (renamed (member cut '(:after-follow-rename :after-follow-directory-fsync
                                                  :after-follow-readback)))
                           (temporary-bytes (when (pg::physical-adoption-path-present-p temporary)
                                              (lineage-test-bytes temporary)))
                           (identity (pg::physical-reseed-follow-file-identity
                                      (sb-posix:lstat (namestring (if temporary-bytes temporary final))) t)))
                      (ok (plusp crash-pid))
                      (with-open-file (input marker)
                        (let ((*read-eval* nil) (*package* (find-package :cl-user)))
                          (ok (equal (list crash-pid cut) (read input)))
                          (ok (eq :eof (read input nil :eof)))))
                      (ok (search (format nil "CONFIGURED-CANCELLATION-CRASH-REACHED ~d ~s" crash-pid cut)
                                  (uiop:read-file-string crash-log)))
                      (ok (equalp (pg::physical-reseed-follow-image (if renamed pending record))
                                  (lineage-test-bytes final)))
                      (cond
                        (renamed
                         (ng (pg::physical-adoption-path-present-p temporary)))
                        ((eq cut :before-follow-temporary-create)
                         (ng (pg::physical-adoption-path-present-p temporary)))
                        ((eq cut :after-follow-temporary-create)
                         (ok (zerop (length temporary-bytes))))
                        (partial
                         (ok (< 0 (length temporary-bytes) (length (pg::physical-reseed-follow-image pending))))
                         (ok (<= (length temporary-bytes) 257))
                         (ok (equalp temporary-bytes
                                     (subseq (pg::physical-reseed-follow-image pending)
                                             0 (length temporary-bytes)))))
                        ((not renamed)
                         (ok (equalp (pg::physical-reseed-follow-image pending) temporary-bytes))))
                      (let ((pid (run-configured-cancellation-test-recovery-child
                                  root cut pending cache result log)))
                        (ok (plusp pid))
                        (ok (/= crash-pid pid)))
                      (if partial
                          (progn
                            (ok (equalp temporary-bytes (lineage-test-bytes temporary)))
                            (ok (equal identity
                                       (pg::physical-reseed-follow-file-identity
                                        (sb-posix:lstat (namestring temporary)) t)))
                            (ok (equalp before (lineage-test-bytes final))))
                          (progn
                            (ok (equalp (pg::physical-reseed-follow-image pending) (lineage-test-bytes final)))
                            (ng (pg::physical-adoption-path-present-p temporary))
                            (unless (eq cut :before-follow-temporary-create)
                              (ok (equal (subseq identity 0 7)
                                         (subseq (pg::physical-reseed-follow-file-identity
                                                  (sb-posix:lstat (namestring final))) 0 7))))))
                      (ok (equalp facts
                                  (configured-cancellation-test-preserved-file-facts root)))))))))))

(deftest reseed-configured-cancellation-native-create-cuts
    (check-configured-cancellation-native-cuts
     '(:before-follow-temporary-create :after-follow-temporary-create)))

(deftest reseed-configured-cancellation-native-payload-cuts
    (check-configured-cancellation-native-cuts
     '(:after-follow-write-chunk :after-follow-temporary-fsync)))

(deftest reseed-configured-cancellation-native-publication-cuts
    (check-configured-cancellation-native-cuts
     '(:before-follow-rename :after-follow-rename)))

(deftest reseed-configured-cancellation-native-readback-cuts
    (check-configured-cancellation-native-cuts
     '(:after-follow-directory-fsync :after-follow-readback)))
