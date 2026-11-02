(in-package #:pg-physical-tests)

(defun call-with-adoption-follow-transaction-fixture (function &key (fork-distance 64) (generations 1))
  (call-with-adoption-follow-planning-fixture
   (lambda (root owner handoff base control store)
     (declare (ignore base control))
     (pg::physical-store-close store)
     (let ((directory (merge-pathnames (format nil "adoption-follow-~A/" (gensym))
                                       (uiop:pathname-parent-directory-pathname root))))
       (sb-posix:mkdir (namestring directory) #o700)
       (unwind-protect
            (funcall function root owner handoff directory)
         (uiop:delete-directory-tree directory :validate t))))
   :fork-distance fork-distance :generations generations))

(deftest adoption-follow-publishes-and-resumes-without-retiring-fences
    (dolist (distance '(0 64))
      (dolist (generations '(1 2 3))
        (call-with-adoption-follow-transaction-fixture
         (lambda (root owner handoff directory)
           (let* ((prepared (pg::physical-adoption-follow-prepare owner directory))
                  (value (getf prepared :plan))
                  (moves (pg::physical-adoption-follow-moves value)))
             (ok (zerop (getf prepared :step)))
             (ok (equal handoff (pg::physical-adoption-handoff-read owner)))
             (multiple-value-bind (state status) (pg::physical-adoption-follow-resume owner)
               (ok (eq :active-fenced status))
               (ok (= (length moves) (getf state :step)))
               (ok (= (length moves) (pg::physical-adoption-follow-validate owner state)))
               (ok (equal state (pg::physical-adoption-follow-resume owner))))
             (ok (signals (pg::physical-adoption-check-no-install root)))
             (ok (probe-file (merge-pathnames "saved/recovery.original" directory)))
             (ok (probe-file (pg::physical-adoption-handoff-path root)))))
         :fork-distance distance :generations generations))))

(deftest adoption-follow-resumes-every-move-and-torn-progress-boundary
    (dolist (point '(:before-follow-move :after-follow-move
                     :after-follow-source-fsync :after-follow-target-fsync
                     :after-follow-progress-write-chunk :after-follow-progress-fsync))
      (loop for occurrence from 1 to 11 do
        (call-with-adoption-follow-transaction-fixture
         (lambda (root owner handoff directory)
           (declare (ignore handoff))
           (pg::physical-adoption-follow-prepare owner directory)
           (let ((seen 0))
             (let ((pg::*physical-adoption-follow-progress-write-limit* 1)
                   (pg::*physical-adoption-install-test-hook*
                     (lambda (actual path)
                       (declare (ignore path))
                       (when (and (eq actual point) (= occurrence (incf seen)))
                         (error "follow cut ~s/~d" point occurrence)))))
               (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-resume owner)))
                   (format nil "~s/~d" point occurrence)))
             (ok (= occurrence seen))
             (let* ((state (pg::physical-adoption-follow-read owner))
                    (observed (pg::physical-adoption-follow-validate owner state)))
               (ok (<= (getf state :step) observed (1+ (getf state :step)))))
             (multiple-value-bind (state status) (pg::physical-adoption-follow-resume owner)
               (ok (eq :active-fenced status))
               (ok (= 11 (getf state :step))))
             (ok (signals (pg::physical-adoption-check-no-install root)))))
         :fork-distance 0))))

(deftest adoption-follow-preserves-the-image-before-journal-publication
    (dolist (point '(:after-follow-stage-write-chunk :after-follow-stage-file-fsync
                     :after-follow-journal-write-chunk :after-follow-journal-file-fsync
                     :before-follow-journal-rename))
      (call-with-adoption-follow-transaction-fixture
       (lambda (root owner handoff directory)
         (let ((before (pg::physical-adoption-install-snapshot root))
               (seen nil))
           (let ((pg::*physical-adoption-follow-stage-write-limit* 1)
                 (pg::*physical-adoption-follow-write-limit* 1)
                 (pg::*physical-adoption-install-test-hook*
                   (lambda (actual path)
                     (declare (ignore path))
                     (when (eq actual point)
                       (setf seen t)
                       (error "follow preparation cut ~s" point)))))
             (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-start owner directory)))))
           (ok seen)
           (ok (not (probe-file (pg::physical-adoption-follow-path root))))
           (ok (equal before (pg::physical-adoption-install-snapshot root)))
           (ok (eq :ready (pg::physical-adoption-handoff-validate owner handoff))))))
      ))

(deftest adoption-follow-rejects-unexplained-progress-and-mutated-authority
    (dolist (mutation '(:two-moves :packet :extra-name :missing-handoff))
      (call-with-adoption-follow-transaction-fixture
       (lambda (root owner handoff directory)
         (declare (ignore handoff))
         (let* ((state (pg::physical-adoption-follow-prepare owner directory))
                (value (getf state :plan)))
           (ecase mutation
             (:two-moves
              (dolist (move (subseq (pg::physical-adoption-follow-moves value) 0 2))
                (destructuring-bind (from name to target) move
                  (pg::physical-adoption-install-rename
                   (pg::physical-adoption-install-entry-path
                    (pg::physical-adoption-follow-directory value from) name)
                   (pg::physical-adoption-install-entry-path
                    (pg::physical-adoption-follow-directory value to) target)))))
             (:packet
              (with-open-file (stream (merge-pathnames "staged/follow.pending" directory)
                                      :direction :output :element-type '(unsigned-byte 8)
                                      :if-exists :append)
                (write-byte 0 stream)))
             (:extra-name
              (with-open-file (stream (merge-pathnames "unexplained" root)
                                      :direction :output :if-exists :error)
                (write-line "unexpected" stream)))
             (:missing-handoff
              (delete-file (pg::physical-adoption-handoff-path root))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-validate owner state)))
               (format nil "~s" mutation))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-resume owner))))
           (ok (signals (pg::physical-adoption-check-no-install root))))))))

(deftest adoption-follow-resumes-published-authority-before-the-first-move
    (dolist (point '(:after-follow-journal-rename :after-follow-authority-source-fsync
                     :after-follow-authority-target-fsync :after-follow-journal-fsync))
      (call-with-adoption-follow-transaction-fixture
       (lambda (root owner handoff directory)
         (declare (ignore handoff))
         (let ((seen nil))
           (let ((pg::*physical-adoption-install-test-hook*
                   (lambda (actual path)
                     (declare (ignore path))
                     (when (eq actual point)
                       (setf seen t)
                       (error "follow authority cut ~s" point)))))
             (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-start owner directory)))))
           (ok seen)
           (let ((state (pg::physical-adoption-follow-read owner)))
             (ok (zerop (getf state :step)))
             (ok (zerop (pg::physical-adoption-follow-validate owner state))))
           (multiple-value-bind (state status) (pg::physical-adoption-follow-resume owner)
             (ok (eq :active-fenced status))
             (ok (= 11 (getf state :step))))
           (ok (signals (pg::physical-adoption-check-no-install root)))))
       :fork-distance 0)))

(defun adoption-follow-run-child (root directory log &key start cut (occurrence 1) partial)
  (let* ((source (asdf:system-source-directory "pg"))
         (command (if start
                      (format nil "(pg::physical-adoption-follow-start owner #P~s)" (namestring directory))
                      "(pg::physical-adoption-follow-resume owner)"))
         (code (format nil
                       "(pg::call-with-physical-adoption-owner #P~s
                         (lambda (owner)
                           (let ((seen 0))
                             (setf pg::*physical-adoption-follow-progress-write-limit* ~s
                                   pg::*physical-adoption-install-test-hook*
                                   (lambda (point path)
                                     (declare (ignore path))
                                     (when (and (eq point ~s) (= ~d (incf seen)))
                                       (sb-posix:kill (sb-posix:getpid) 9))))
                             ~a)))"
                       (namestring root) (when partial 1) cut occurrence command))
         (process (sb-ext:run-program
                   "/usr/bin/env"
                   (list (format nil "XDG_CACHE_HOME=/tmp/pg-adoption-follow-crash-cache-~d" (sb-posix:getpid))
                         (format nil "CL_SOURCE_REGISTRY=~a//:" (namestring source))
                         "sbcl" "--noinform" "--disable-debugger" "--non-interactive"
                         "--eval" "(require :asdf)" "--eval" "(asdf:load-system \"pg\")"
                         "--eval" code)
                   :wait t :output log :error :output :if-output-exists :supersede)))
    (unwind-protect
         (list (sb-ext:process-status process) (sb-ext:process-exit-code process))
      (sb-ext:process-close process))))

(deftest adoption-follow-recovers-sigkill-with-fresh-ownership
    (dolist (scenario '((:after-follow-journal-rename 1 nil)
                        (:after-follow-move 2 nil)
                        (:after-follow-move 5 nil)
                        (:after-follow-move 8 nil)
                        (:after-follow-move 10 nil)
                        (:after-follow-progress-write-chunk 1 t)
                        (:after-follow-progress-fsync 11 nil)))
      (call-with-adoption-follow-transaction-fixture
       (lambda (root owner handoff directory)
         (declare (ignore handoff))
         (with-physical-store-directory (logs)
           (let ((root-inode (pg::physical-adoption-owner-inode owner))
                 (lock-inode (pg::physical-adoption-owner-lock-inode owner))
                 (log (merge-pathnames "child.log" logs)))
             (setf (pg::physical-adoption-owner-active-p owner) nil)
             (close (pg::physical-adoption-owner-lock owner))
             (ok (equal '(:signaled 9)
                        (adoption-follow-run-child root directory log :start t
                                                                      :cut (first scenario) :occurrence (second scenario)
                                                                      :partial (third scenario)))
                 (format nil "~s" scenario))
             (ok (equal '(:exited 0) (adoption-follow-run-child root directory log))
                 (format nil "~s" scenario))
             (pg::call-with-physical-adoption-owner
              root
              (lambda (recovered)
                (ok (= root-inode (pg::physical-adoption-owner-inode recovered)))
                (ok (= lock-inode (pg::physical-adoption-owner-lock-inode recovered)))
                (let ((state (pg::physical-adoption-follow-read recovered)))
                  (ok (= 11 (getf state :step)))
                  (ok (= 11 (pg::physical-adoption-follow-validate recovered state)))))))))
       :fork-distance 0)))
