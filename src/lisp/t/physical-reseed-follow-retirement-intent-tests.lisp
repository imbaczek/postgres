(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-retirement-intent-layout-preserves-nonjournal-facts
    ((offset (range integer 10 1000)) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-retirement-intent-layout)
  (:trials (:normal 100))
  (multiple-value-bind (before-fds before) (reseed-startup-projection-fixture offset)
    (multiple-value-bind (after-fds after) (reseed-startup-projection-fixture (+ offset delta))
      (let* ((layout (getf after :layout))
             (root (cdr (assoc :root (getf layout :directories)))))
        (incf (seventh root) delta)
        (incf (eighth root) delta)
        (incf (ninth root) delta)
        (push "physical-reseed.follow.tmp" (getf (getf layout :names) :root))
        (let ((expected (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                 before-fds (getf before :layout))))
          (and
           (equal expected
                  (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                           after-fds layout))
           (progn
             (push "foreign" (getf (getf layout :names) :root))
             (not (equal expected
                         (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                  after-fds layout))))))))))

(deftest generated-reseed-retirement-intent-layout-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retirement-intent-layout-preserves-nonjournal-facts
                                       :seed 150 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-retirement-intent-layout-preserves-case-roles-and-copies
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
      (let* ((layout (getf state :layout))
             (expected (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                descriptors layout)))
        (dolist (entry '((:root "Physical-reseed.follow.tmp")
                         (:saved "physical-reseed.follow.tmp")))
          (let ((copy (pg::physical-adoption-attempt-value-copy layout)))
            (push (second entry) (getf (getf copy :names) (first entry)))
            (ok (not (equal expected
                            (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                     descriptors copy))))))
        (let ((copy (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                             descriptors layout)))
          (setf (char (first (getf (getf copy :names) :root)) 0) #\X)
          (ok (equal expected
                     (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                              descriptors layout)))))))

(deftest reseed-retirement-intent-proof-preserves-state-during-a-partial-temporary
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner workspace))
       (let ((stream nil) (fixed nil) (before nil)
                          (path (merge-pathnames "physical-reseed.follow.tmp" root))
                          (bytes (sb-ext:string-to-octets "partial" :external-format :utf-8)))
         (unwind-protect
              (pg::call-with-physical-reseed-follow-retirement-resources
               root-fd (getf snapshot :record) check
               (lambda (descriptors store base binding verify)
                 (declare (ignore base binding))
                 (flet ((extra (fd)
                          (funcall verify)
                          (append
                           (pg::physical-reseed-follow-retirement-final-extra-at fd snapshot)
                           (when stream
                             (pg::physical-reseed-follow-store-fd-check
                              fd (sb-sys:fd-stream-fd stream) "physical-reseed.follow.tmp" fixed)
                             (list (cons "physical-reseed.follow.tmp"
                                         (nth-value 1 (pg::physical-reseed-follow-entry
                                                       fd "physical-reseed.follow.tmp" t))))))))
                   (setf before
                         (pg::physical-reseed-follow-retirement-intent-proof-at
                          descriptors store (getf snapshot :record) verify #'extra))
                   (sb-sys:without-interrupts
                     (setf stream (open path :direction :io :element-type '(unsigned-byte 8)
                                             :if-exists :error :if-does-not-exist :create)))
                   (sb-posix:fchmod (sb-sys:fd-stream-fd stream) #o600)
                   (setf fixed (subseq (pg::physical-reseed-follow-file-identity
                                        (sb-posix:fstat (sb-sys:fd-stream-fd stream)) t) 0 5))
                   (ok (equal before
                              (pg::physical-reseed-follow-retirement-intent-proof-at
                               descriptors store (getf snapshot :record) verify #'extra)))
                   (write-sequence bytes stream)
                   (finish-output stream)
                   (ok (equal before
                              (pg::physical-reseed-follow-retirement-intent-proof-at
                               descriptors store (getf snapshot :record) verify #'extra)))
                   (ok (funcall verify)))))
           (when stream (close stream)))
         (ok (equalp bytes (lineage-test-bytes path)))
         (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))))))

(deftest reseed-retirement-intent-proof-checks-parent-before-descriptor-use
    (ok (eq :refused
            (catch 'retirement-intent-parent-refused
              (pg::physical-reseed-follow-retirement-intent-proof-at
               nil nil nil
               (lambda () (throw 'retirement-intent-parent-refused :refused))
               (lambda (root)
                 (declare (ignore root))
                 (error "extra reader must stay unreachable")))))))

(deftest reseed-retirement-test-startup-scope-reopens-and-revokes-each-owner
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace fixture-fd original check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((owners nil))
         (dotimes (index 2)
           (declare (ignorable index))
           (let ((captured nil) (held nil))
             (call-with-retirement-intent-published-owner
              root
              (lambda (owner directory root-fd snapshot)
                (setf captured owner held root-fd)
                (push owner owners)
                (ok (equal root directory))
                (ok (equal original snapshot))
                (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))
                (ok (pg::postmaster-pid-owned-p))))
             (ok (not (pg::retirement-startup-active-p captured)))
             (ok (handler-case (progn (sb-posix:fstat held) nil)
                   (sb-posix:syscall-error () t)))
             (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fixture-fd))))))
         (ok (not (eq (first owners) (second owners))))))))

(defun retirement-intent-test-nonjournal-facts (root)
  (let* ((excluded '("physical-reseed.follow" "physical-reseed.follow.tmp" "postmaster.pid"))
         (facts (reseed-mapped-test-tree-facts root))
         (directory (first facts)))
    (setf (second directory) (subseq (second directory) 0 7)
          (third directory) (sort (set-difference (third directory) excluded :test #'equal) #'string<))
    (dolist (entry (rest facts))
      (when (and (eq :file (first entry)) (equal ".pg_lisp.lock" (first (second entry))))
        (setf (second entry) (butlast (second entry)))))
    (remove-if (lambda (entry)
                 (and (eq :file (first entry))
                      (member (first (second entry)) excluded :test #'equal)))
               facts)))

(defun retirement-intent-test-reset (root record)
  (let ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
        (final (merge-pathnames "physical-reseed.follow" root)))
    (when (pg::physical-adoption-path-present-p temporary) (delete-file temporary))
    (unless (equalp (pg::physical-reseed-follow-image record) (pg::recovery-read-bytes final))
      (reseed-test-replace-follow-record root record))
    (pg::fsync-directory root)))

(deftest reseed-retirement-crash-child-cache-retains-source-directory-hierarchy
    (with-physical-store-directory (driver)
      (let* ((cache (merge-pathnames "cache/" driver))
             (arguments (retirement-intent-test-child-arguments
                         driver :after-retirement-rename cache (merge-pathnames "marker" driver)))
             (*package* (find-package :cl-user))
             (*read-eval* nil)
             (forms (loop for (flag value) on (nthcdr 4 arguments) by #'cddr
                          when (string= flag "--eval") collect (read-from-string value)))
             (form (find 'asdf:initialize-output-translations forms :key #'first))
             (configuration (second (second form)))
             (destination (second (second configuration))))
        (ok (stringp destination))
        (ok (equal (namestring cache) destination)))))

(deftest reseed-retirement-crash-baseline-allows-only-the-lock-ctime-change
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames ".pg_lisp.lock" root)))
        (reseed-follow-owner-test-write path #(1))
        (let ((before (retirement-intent-test-nonjournal-facts root))
              (ctime (sb-posix:stat-ctime (sb-posix:stat path))))
          (sleep 1.1)
          (let ((fd (sb-posix:open (namestring path) sb-posix:o-rdonly)))
            (unwind-protect (sb-posix:fchmod fd #o600) (sb-posix:close fd)))
          (ok (/= ctime (sb-posix:stat-ctime (sb-posix:stat path))))
          (ok (reseed-activation-layout-facts-equal-p before (retirement-intent-test-nonjournal-facts root)))
          (sb-posix:chmod (namestring path) #o640)
          (ok (not (reseed-activation-layout-facts-equal-p before (retirement-intent-test-nonjournal-facts root))))
          (sb-posix:chmod (namestring path) #o600)
          (with-open-file (out path :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
            (write-byte 2 out))
          (ok (not (reseed-activation-layout-facts-equal-p before (retirement-intent-test-nonjournal-facts root))))))))

(defun retirement-intent-test-child-arguments (root point cache marker)
  (let* ((directories
           (remove-duplicates
            (remove nil (mapcar #'asdf:system-source-directory (asdf:already-loaded-systems)))
            :test #'equal))
         (registry `(:source-registry
                     ,@(mapcar (lambda (directory) `(:directory ,directory)) directories)
                     :ignore-inherited-configuration))
         (forms
           (list
            '(require :asdf)
            `(asdf:initialize-source-registry ',registry)
            `(asdf:initialize-output-translations
              '(:output-translations (t ,(namestring cache)) :ignore-inherited-configuration))
            '(asdf:load-system "pg-physical-tests")
            `(let ((pg::*physical-reseed-follow-test-hook*
                     (lambda (event path)
                       (declare (ignore path))
                       (when (eq event ,point)
                         (with-open-file (out ,marker :direction :output :if-exists :error)
                           (write (list (sb-posix:getpid) ,point) :stream out :readably t))
                         (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                         (error "retirement child SIGKILL returned")))))
               (format t "~%RETIREMENT-CRASH-CHILD ~d ~s ~s~%" (sb-posix:getpid) ,root ,point)
               (finish-output)
               (call-with-retirement-intent-published-owner
                ,root
                (lambda (owner directory root-fd snapshot)
                  (declare (ignore directory))
                  (let* ((record (getf snapshot :record))
                         (candidate
                           (pg::physical-reseed-follow-retirement-next-record
                            record (pg::physical-reseed-follow-retirement-next-reference root-fd record)))
                         (pg::*physical-reseed-follow-write-limit*
                           (ceiling (length (pg::physical-reseed-follow-image candidate)) 2)))
                    (pg::physical-reseed-follow-retirement-publish-initial owner))))
               (error "retirement child did not reach its crash point")))))
    (let ((*package* (find-package :cl-user))
          (*print-length* nil) (*print-level* nil) (*print-readably* t))
      (append '("--noinform" "--no-sysinit" "--no-userinit" "--non-interactive")
              (loop for form in forms append (list "--eval" (write-to-string form)))))))

(defun run-retirement-intent-test-crash-child (root point cache marker &key (timeout 300))
  (let ((process nil) (pid nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process
                   (sb-ext:run-program
                    (namestring sb-ext:*runtime-pathname*)
                    (retirement-intent-test-child-arguments root point cache marker)
                    :search t :wait nil :output t :error :output)
                   pid (sb-ext:process-pid process)))
           (format t "~%RETIREMENT-CRASH-OWNED-CHILD ~d ~s ~s~%" pid root point)
           (finish-output)
           (loop with deadline = (+ (get-internal-real-time) (* timeout internal-time-units-per-second))
                 while (member (sb-ext:process-status process) '(:running :stopped))
                 do (when (>= (get-internal-real-time) deadline)
                      (error "retirement crash child did not exit within ~a seconds" timeout))
                    (sleep 0.05))
           (sb-ext:process-wait process)
           (assert (eq :signaled (sb-ext:process-status process)))
           (assert (= sb-posix:sigkill (sb-ext:process-exit-code process)))
           (with-open-file (input marker)
             (let ((*read-eval* nil) (*package* (find-package :cl-user)))
               (assert (equal (list pid point) (read input)))
               (assert (eq :eof (read input nil :eof)))))
           pid)
      (when process
        (when (member (sb-ext:process-status process) '(:running :stopped))
          (sb-ext:process-kill process sb-posix:sigkill))
        (sb-ext:process-wait process)
        (sb-ext:process-close process)))))

(deftest reseed-retirement-crash-child-timeout-kills-reaps-and-closes-the-owned-process
    (with-physical-store-directory (driver)
      (let ((original-arguments (symbol-function 'retirement-intent-test-child-arguments))
            (original-run (symbol-function 'sb-ext:run-program))
            (original-close (symbol-function 'sb-ext:process-close))
            (captured nil) (closed 0) (started 0))
        (sb-ext:with-unlocked-packages (:sb-ext)
          (unwind-protect
               (progn
                 (setf (symbol-function 'retirement-intent-test-child-arguments)
                       (lambda (&rest values)
                         (declare (ignore values))
                         '("--noinform" "--no-sysinit" "--no-userinit" "--non-interactive" "--eval" "(sleep 60)"))
                       (symbol-function 'sb-ext:run-program)
                       (lambda (&rest values)
                         (incf started)
                         (setf captured (apply original-run values)))
                       (symbol-function 'sb-ext:process-close)
                       (lambda (process)
                         (when (eq process captured) (incf closed))
                         (funcall original-close process)))
                 (ok (handler-case
                         (progn
                           (run-retirement-intent-test-crash-child
                            driver :after-retirement-rename (merge-pathnames "cache/" driver)
                            (merge-pathnames "marker" driver) :timeout 0.05)
                           nil)
                       (error (condition) (search "did not exit within" (princ-to-string condition))))))
            (setf (symbol-function 'retirement-intent-test-child-arguments) original-arguments
                  (symbol-function 'sb-ext:run-program) original-run
                  (symbol-function 'sb-ext:process-close) original-close)))
        (ok (= 1 started))
        (ok (= 1 closed))
        (ok (eq :signaled (sb-ext:process-status captured)))
        (ok (= sb-posix:sigkill (sb-ext:process-exit-code captured))))))

(defun call-with-retirement-intent-published-owner (root function)
  (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                            (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                            (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                            (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                            (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                            (before nil) (after nil))
    (unwind-protect
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (if (eq phase :before-pid)
                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
          :startup-reconcile
          (lambda ()
            (pg::call-with-physical-reseed-follow-retirement-startup-owner
             after
             (lambda (owner)
               (pg::call-with-physical-reseed-follow-retirement-startup-root
                owner
                (lambda (root-fd check)
                  (funcall check)
                  (let ((snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                    (pg::physical-reseed-follow-check-snapshot
                     root-fd (getf (pg::retirement-startup-evidence owner) :snapshot))
                    (funcall function owner root root-fd snapshot))))))))
      (unwind-protect
           (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
        (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
        (setf sb-ext:*exit-hooks* hooks)))))

(defun call-with-retirement-intent-test-owner (function &optional before-startup)
  (call-with-reseed-retirement-observation-fixture
   :same-timeline
   (lambda (offline root workspace root-fd snapshot check)
     (declare (ignore workspace check))
     (close (pg::physical-adoption-owner-lock offline))
     (setf (pg::physical-adoption-owner-active-p offline) nil)
     (when before-startup (funcall before-startup root root-fd snapshot))
     (call-with-retirement-intent-published-owner root function))))

(defun check-retirement-operation-busy-cuts (owner)
  (dolist (mode '(:error :throw :interrupt))
    (let ((entered nil) (fired nil))
      (unwind-protect
           (let ((pg::*physical-reseed-follow-test-hook*
                   (lambda (event path)
                     (declare (ignore path))
                     (when (eq event :after-retirement-operation-busy)
                       (setf fired t)
                       (ecase mode
                         (:error (error "retirement busy cut"))
                         (:throw (throw 'retirement-busy-cut :cut))
                         (:interrupt
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda () (throw 'retirement-busy-cut :cut)))))))))
             (ok (eq :cut
                     (catch 'retirement-busy-cut
                       (handler-case
                           (pg::call-with-physical-reseed-follow-retirement-operation
                            owner (lambda () (setf entered t)))
                         (error (condition)
                           (if (search "retirement busy cut" (princ-to-string condition))
                               :cut (error condition)))))))
             (ok fired)
             (ok (not entered))
             (let ((busy (pg::retirement-startup-busy-p owner)))
               (ok (null busy))))
        (setf (pg::retirement-startup-busy-p owner) nil)))))

(deftest reseed-retirement-operation-blocks-reentry-and-retains-uncertainty
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (declare (ignore root root-fd snapshot))
       (check-retirement-operation-busy-cuts owner)
       (let ((entered nil))
         (ok (eq :finished
                 (pg::call-with-physical-reseed-follow-retirement-operation
                  owner
                  (lambda ()
                    (ok (funcall (symbol-function 'pg::retirement-startup-busy-p) owner))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retirement-operation
                               owner (lambda () (setf entered t)))
                              nil)
                          (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                    :finished))))
         (ok (not entered))
         (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner))))
       (dolist (mode '(:error :throw :interrupt))
         (ok
          (eq :cut
              (catch 'retirement-operation-cut
                (handler-case
                    (pg::call-with-physical-reseed-follow-retirement-operation
                     owner
                     (lambda ()
                       (ecase mode
                         (:error (error "retirement operation cut"))
                         (:throw (throw 'retirement-operation-cut :cut))
                         (:interrupt
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda () (throw 'retirement-operation-cut :cut)))))))
                  (error (condition)
                    (if (search "retirement operation cut" (princ-to-string condition))
                        :cut (error condition)))))))
         (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner)))
         (ok (not (funcall (symbol-function 'pg::retirement-startup-uncertain-p) owner))))
       (let ((escaped nil))
         (ok (handler-case
                 (progn
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    (list :branch :retirement :evidence (pg::retirement-startup-evidence owner))
                    (lambda (inner)
                      (setf escaped inner)
                      (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) t inner)
                      :unfinished))
                   nil)
               (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
         (ok escaped)
         (ok (not (pg::retirement-startup-active-p escaped)))
         (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner)))
       (unwind-protect
            (progn
              (ok (handler-case
                      (progn
                        (pg::call-with-physical-reseed-follow-retirement-operation
                         owner
                         (lambda ()
                           (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) t owner)
                           :unfinished))
                        nil)
                    (error (condition) (search "durable reconciliation" (princ-to-string condition)))))
              (ok (funcall (symbol-function 'pg::retirement-startup-uncertain-p) owner))
              (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner))))
         (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) nil owner))
       (unwind-protect
            (progn
              (ok (handler-case
                      (progn
                        (pg::call-with-physical-reseed-follow-retirement-operation
                         owner
                         (lambda ()
                           (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) t owner)
                           (error "uncertain operation cut")))
                        nil)
                    (error (condition) (search "uncertain operation cut" (princ-to-string condition)))))
              (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner)))
              (ok (funcall (symbol-function 'pg::retirement-startup-uncertain-p) owner))
              (let ((entered nil))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-operation
                           owner (lambda () (setf entered t)))
                          nil)
                      (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                (ok (not entered))))
         (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) nil owner)))))

(deftest reseed-retirement-startup-owner-printing-keeps-handover-evidence-private
    (let* ((secret "retirement-owner-print-test-secret")
           (owner (pg::make-physical-reseed-follow-retirement-startup-owner
                   :evidence (list :snapshot (list :record (list :private-test-value secret)))))
           (*print-level* nil) (*print-length* nil) (*print-readably* nil)
           (text (write-to-string owner)))
      (ok (search "physical-reseed-follow-retirement-startup-owner" (string-downcase text)))
      (ok (not (search secret text)))))

(deftest reseed-retirement-held-file-facts-survive-unlink-and-require-the-mount
    (with-physical-store-directory (root)
      (let* ((path (merge-pathnames "retirement-held-test" root))
             (root-fd (pg::physical-backup-open-directory root)))
        (unwind-protect
             (with-open-file (stream path :direction :io :if-exists :error
                                          :element-type '(unsigned-byte 8))
               (write-sequence #(1 2 3) stream)
               (finish-output stream)
               (let* ((fd (sb-sys:fd-stream-fd stream))
                      (mount (pg::physical-adoption-install-mount-id nil :directory-fd root-fd))
                      (named (pg::physical-reseed-install-old-observe root-fd "retirement-held-test" mount))
                      (before (pg::physical-reseed-follow-retirement-held-file-facts fd mount)))
                 (ok (equal (rest named) before))
                 (ok (= 1 (fourth before)))
                 (ok
                  (handler-case
                      (progn (pg::physical-reseed-follow-retirement-held-file-facts fd (1+ mount)) nil)
                    (error (condition) (search "target mount" (princ-to-string condition)))))
                 (delete-file path)
                 (let ((after (pg::physical-reseed-follow-retirement-held-file-facts fd mount)))
                   (ok (zerop (fourth after)))
                   (ok (equal (subseq before 0 3) (subseq after 0 3)))
                   (ok (equal (subseq before 4 7) (subseq after 4 7)))
                   (ok (not (probe-file path))))))
          (sb-posix:close root-fd)))))

(deftest reseed-retirement-journal-checks-parent-before-descriptor-use
    (let ((entered nil))
      (ok
       (eq :parent
           (catch 'retirement-journal-parent
             (pg::call-with-physical-reseed-follow-retirement-journal
              nil "physical-reseed.follow" nil
              (lambda (stream state)
                (declare (ignore stream state))
                (setf entered t))
              (lambda () (throw 'retirement-journal-parent :parent))))))
      (ok (not entered))))

(defun check-retirement-journal-open-cuts (root-fd snapshot check)
  (let ((original (symbol-function 'pg::physical-image-openat)))
    (dolist (mode '(:error :throw :interrupt))
      (let ((held nil) (injected nil) (entered nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-image-openat)
                     (lambda (&rest arguments)
                       (let ((fd (apply original arguments)))
                         (when (and (equal (second arguments) "physical-reseed.follow")
                                    (not (minusp fd)))
                           (setf held fd))
                         fd)))
               (let ((pg::*physical-reseed-follow-test-hook*
                       (lambda (point path)
                         (declare (ignore path))
                         (when (eq point :after-retirement-journal-open)
                           (setf injected t)
                           (ecase mode
                             (:error (error "retirement journal open cut"))
                             (:throw (throw 'retirement-journal-open-cut :cut))
                             (:interrupt
                              (sb-thread:interrupt-thread
                               sb-thread:*current-thread*
                               (lambda () (throw 'retirement-journal-open-cut :cut)))))))))
                 (ok
                  (eq :cut
                      (catch 'retirement-journal-open-cut
                        (handler-case
                            (pg::call-with-physical-reseed-follow-retirement-journal
                             root-fd "physical-reseed.follow" snapshot
                             (lambda (stream state)
                               (declare (ignore stream state))
                               (setf entered t))
                             check)
                          (error (condition)
                            (if (search "retirement journal open cut" (princ-to-string condition))
                                :cut (error condition)))))))))
          (setf (symbol-function 'pg::physical-image-openat) original))
        (ok injected)
        (ok (not entered))
        (ok (and held (handler-case (progn (sb-posix:fstat held) nil)
                        (sb-posix:syscall-error () t))))
        (ok (funcall check))
        (ok (pg::postmaster-pid-owned-p))))))

(deftest reseed-retirement-journal-scope-binds-the-held-file-and-closes-it-on-exit
    (call-with-retirement-intent-test-owner
     (lambda (owner root fixture-fd snapshot)
       (declare (ignore root fixture-fd))
       (pg::call-with-physical-reseed-follow-retirement-operation
        owner
        (lambda ()
          (pg::call-with-physical-reseed-follow-retirement-startup-root
           owner
           (lambda (root-fd check)
             (check-retirement-journal-open-cuts root-fd snapshot check)
             (dolist (mode '(:normal :error :throw :interrupt))
               (let ((held nil) (stream nil))
                 (ok
                  (eq :finished
                      (catch 'retirement-journal-cut
                        (handler-case
                            (pg::call-with-physical-reseed-follow-retirement-journal
                             root-fd "physical-reseed.follow" snapshot
                             (lambda (opened state)
                               (setf stream opened held (sb-sys:fd-stream-fd opened))
                               (ok (equal snapshot state))
                               (ok (logbitp 0 (sb-posix:fcntl held sb-posix:f-getfd)))
                               (ecase mode
                                 (:normal :finished)
                                 (:error (error "retirement journal cut"))
                                 (:throw (throw 'retirement-journal-cut :finished))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'retirement-journal-cut :finished))))))
                             check)
                          (error (condition)
                            (if (search "retirement journal cut" (princ-to-string condition))
                                :finished (error condition)))))))
                 (ok (not (open-stream-p stream)))
                 (ok (handler-case (progn (sb-posix:fstat held) nil)
                       (sb-posix:syscall-error () t)))
                 (ok (funcall check))
                 (ok (pg::postmaster-pid-owned-p)))))))))))

(deftest reseed-retirement-journal-rejects-same-record-name-replacement
    (call-with-retirement-intent-test-owner
     (lambda (owner root fixture-fd snapshot)
       (declare (ignore fixture-fd))
       (pg::call-with-physical-reseed-follow-retirement-operation
        owner
        (lambda ()
          (pg::call-with-physical-reseed-follow-retirement-startup-root
           owner
           (lambda (root-fd check)
             (let ((original-open (symbol-function 'pg::physical-image-openat))
                   (final (merge-pathnames "physical-reseed.follow" root))
                   (backup (merge-pathnames "retirement-test-held-original" root))
                   (bytes (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))
                   (held nil) (injected nil) (entered nil))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-image-openat)
                            (lambda (&rest arguments)
                              (let ((fd (apply original-open arguments)))
                                (when (and (equal (second arguments) "physical-reseed.follow")
                                           (not (minusp fd)))
                                  (setf held fd))
                                fd)))
                      (let ((pg::*physical-reseed-follow-test-hook*
                              (lambda (point path)
                                (declare (ignore path))
                                (when (eq point :after-retirement-journal-open)
                                  (sb-posix:rename (namestring final) (namestring backup))
                                  (setf injected t)
                                  (reseed-follow-owner-test-write final bytes)))))
                        (ok
                         (handler-case
                             (progn
                               (pg::call-with-physical-reseed-follow-retirement-journal
                                root-fd "physical-reseed.follow" snapshot
                                (lambda (stream state)
                                  (declare (ignore stream state))
                                  (setf entered t))
                                check)
                               nil)
                           (error (condition)
                             (search "differs from its held descriptor" (princ-to-string condition))))))
                      (ok injected)
                      (ok (not entered))
                      (ok (and held (handler-case (progn (sb-posix:fstat held) nil)
                                      (sb-posix:syscall-error () t))))
                      (ok (equalp bytes (pg::recovery-read-bytes final)))
                      (ok (/= (sb-posix:stat-ino (sb-posix:stat (namestring final)))
                              (sb-posix:stat-ino (sb-posix:stat (namestring backup)))))
                      (ok (funcall check))
                      (ok (pg::postmaster-pid-owned-p)))
                 (setf (symbol-function 'pg::physical-image-openat) original-open)
                 (when injected
                   (when (probe-file final) (delete-file final))
                   (sb-posix:rename (namestring backup) (namestring final))))))))))))

(deftest reseed-retirement-initial-publication-adopts-one-durable-next-record
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (let* ((record (getf snapshot :record))
              (reference (pg::physical-reseed-follow-retirement-next-reference root-fd record))
              (expected (pg::physical-reseed-follow-retirement-next-record record reference))
              (before (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))
              (pg::*physical-reseed-follow-write-limit*
                (ceiling (length (pg::physical-reseed-follow-image expected)) 2))
              (original-sync (symbol-function 'sb-posix:fsync))
              (original-close (symbol-function 'pg::physical-store-close))
              (cleanup-fenced nil) (readback nil)
              (pending nil) (events nil) (chunks 0)
              (published nil) (actual nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'sb-posix:fsync)
                      (lambda (fd)
                        (let ((stat (sb-posix:fstat fd)))
                          (push (list pending (sb-posix:stat-ino stat)
                                      (sb-posix:s-isdir (sb-posix:stat-mode stat)))
                                events))
                        (funcall original-sync fd))
                      (symbol-function 'pg::physical-store-close)
                      (lambda (store)
                        (when readback
                          (setf cleanup-fenced (pg::retirement-startup-uncertain-p owner)))
                        (funcall original-close store)))
                (let ((pg::*physical-reseed-follow-test-hook*
                        (lambda (point path)
                          (declare (ignore path))
                          (when (member point '(:before-retirement-old-final-fsync
                                                :before-retirement-predecessor-directory-fsync
                                                :before-retirement-temporary-fsync
                                                :before-retirement-candidate-fsync
                                                :before-retirement-directory-fsync))
                            (setf pending point))
                          (when (eq point :after-retirement-write-chunk) (incf chunks))
                          (when (eq point :after-retirement-readback) (setf readback t)))))
                  (multiple-value-setq (published actual)
                    (pg::physical-reseed-follow-retirement-publish-initial owner))))
           (setf (symbol-function 'sb-posix:fsync) original-sync
                 (symbol-function 'pg::physical-store-close) original-close))
         (setf events (nreverse events))
         (ok (equal (mapcar #'first events)
                    '(:before-retirement-old-final-fsync :before-retirement-predecessor-directory-fsync
                      :before-retirement-temporary-fsync :before-retirement-candidate-fsync
                      :before-retirement-directory-fsync)))
         (ok (equal (mapcar #'third events) '(nil t nil nil t)))
         (ok (= (third (getf snapshot :row)) (second (first events))))
         (ok (= (second (second events)) (second (fifth events))))
         (ok (= (second (third events)) (second (fourth events))))
         (ok (= 2 chunks))
         (ok cleanup-fenced)
         (ok (equal expected published))
         (ok (equal expected (getf actual :record)))
         (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
         (ok (not (equalp before (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (not (pg::retirement-startup-busy-p owner)))
         (ok (not (pg::retirement-startup-uncertain-p owner)))
         (ok (pg::postmaster-pid-owned-p))
         (ok
          (pg::call-with-physical-reseed-follow-retirement-startup-observation
           owner
           (lambda (descriptors state check)
             (declare (ignore descriptors))
             (ok (equal actual (getf state :snapshot)))
             (funcall check))))))))

(deftest reseed-retirement-initial-publication-recovers-the-exact-complete-temporary
    (let ((expected nil) (temporary-inode nil) (writes 0))
      (call-with-retirement-intent-test-owner
       (lambda (owner root root-fd snapshot)
         (declare (ignore snapshot))
         (let ((pg::*physical-reseed-follow-test-hook*
                 (lambda (point path)
                   (declare (ignore path))
                   (when (member point '(:before-retirement-temporary-create :after-retirement-write-chunk))
                     (incf writes)
                     (error "complete retirement temporary must be reused")))))
           (multiple-value-bind (published actual)
               (pg::physical-reseed-follow-retirement-publish-initial owner)
             (ok (equal expected published))
             (ok (equal expected (getf actual :record)))
             (ok (= temporary-inode (third (getf actual :row))))
             (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
             (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
             (ok (not (pg::retirement-startup-busy-p owner)))
             (ok (not (pg::retirement-startup-uncertain-p owner)))
             (ok (pg::postmaster-pid-owned-p))
             (ok (zerop writes)))))
       (lambda (root root-fd snapshot)
         (setf expected
               (pg::physical-reseed-follow-retirement-next-record
                (getf snapshot :record)
                (pg::physical-reseed-follow-retirement-next-reference root-fd (getf snapshot :record))))
         (reseed-follow-owner-test-write
          (merge-pathnames "physical-reseed.follow.tmp" root)
          (pg::physical-reseed-follow-image expected))
         (setf temporary-inode
               (third (nth-value 1 (pg::physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp"))))))))

(deftest reseed-retirement-publication-and-temporary-recovery-cover-every-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-retirement-observation-fixture
       kind
       (lambda (offline root workspace fixture-fd initial check)
         (declare (ignore check))
         (close (pg::physical-adoption-owner-lock offline))
         (setf (pg::physical-adoption-owner-active-p offline) nil)
         (let* ((record (getf initial :record))
                (candidate
                  (pg::physical-reseed-follow-retirement-next-record
                   record (pg::physical-reseed-follow-retirement-next-reference fixture-fd record)))
                (baseline (list (retirement-intent-test-nonjournal-facts root)
                                (reseed-mapped-test-tree-facts workspace)))
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
           (dolist (recover '(nil t))
             (retirement-intent-test-reset root record)
             (when recover
               (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate)))
             (let ((temporary-inode
                     (when recover
                       (third (nth-value 1 (pg::physical-reseed-follow-entry
                                            fixture-fd "physical-reseed.follow.tmp")))))
                   (writes 0) (captured-owner nil))
               (call-with-retirement-intent-published-owner
                root
                (lambda (owner directory root-fd snapshot)
                  (declare (ignore directory snapshot))
                  (setf captured-owner owner)
                  (let ((pg::*physical-reseed-follow-test-hook*
                          (lambda (point path)
                            (declare (ignore path))
                            (when (member point '(:before-retirement-temporary-create
                                                  :after-retirement-write-chunk))
                              (incf writes)
                              (when recover (error "complete temporary must be reused"))))))
                    (multiple-value-bind (published actual)
                        (pg::physical-reseed-follow-retirement-publish-initial owner)
                      (ok (equal candidate published))
                      (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                      (ok (equal actual (getf (pg::retirement-startup-evidence owner) :snapshot)))
                      (when recover (ok (= temporary-inode (third (getf actual :row)))))
                      (ok (not (probe-file temporary)))
                      (ok (not (pg::retirement-startup-busy-p owner)))
                      (ok (not (pg::retirement-startup-uncertain-p owner)))
                      (ok (pg::postmaster-pid-owned-p))))))
               (ok (if recover (zerop writes) (plusp writes)))
               (ok (not (pg::retirement-startup-active-p captured-owner)))
               (ok (reseed-activation-layout-facts-equal-p
                    baseline (list (retirement-intent-test-nonjournal-facts root)
                                   (reseed-mapped-test-tree-facts workspace))))))
           (retirement-intent-test-reset root record))))))

(deftest reseed-retirement-initial-publication-preserves-invalid-temporaries-before-sync
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (let* ((record (getf snapshot :record))
              (reference (pg::physical-reseed-follow-retirement-next-reference root-fd record))
              (candidate (pg::physical-reseed-follow-retirement-next-record record reference))
              (wrong-next (pg::physical-reseed-follow-retirement-next-record candidate reference))
              (path (merge-pathnames "physical-reseed.follow.tmp" root))
              (original-sync (symbol-function 'sb-posix:fsync))
              (syncs 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'sb-posix:fsync)
                      (lambda (fd) (incf syncs) (funcall original-sync fd)))
                (dolist (bytes (list (make-array 0 :element-type '(unsigned-byte 8))
                                     (pg::string->bytes "partial")
                                     (pg::physical-reseed-follow-image wrong-next)))
                  (unwind-protect
                       (progn
                         (reseed-follow-owner-test-write path bytes)
                         (ok (handler-case
                                 (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                               (error () t)))
                         (ok (zerop syncs))
                         (ok (equalp bytes (pg::read-bounded-regular-file path (max 1 (length bytes)) :minimum 0)))
                         (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                         (ok (not (pg::retirement-startup-busy-p owner)))
                         (ok (not (pg::retirement-startup-uncertain-p owner)))
                         (ok (pg::postmaster-pid-owned-p)))
                    (when (probe-file path) (delete-file path)))))
           (setf (symbol-function 'sb-posix:fsync) original-sync))))))

(defun check-retirement-intent-effect-phases (witness original-held)
  (let ((owner (pg::retirement-intent-owner witness)))
    (unwind-protect
         (progn
           (setf (pg::retirement-intent-effect-p witness) t
                 (pg::retirement-startup-uncertain-p owner) t)
           (dolist
               (case
                   (list
                    (list :complete
                          (lambda () (pg::call-with-physical-reseed-follow-retirement-intent-created
                                      witness (lambda (current) (declare (ignore current))))))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-write witness)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-promote witness)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-adopt witness)))
                    (list :writing
                          (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                      witness :old-final :negative-before-sync :negative-after-sync)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                      witness :temporary :negative-before-sync :negative-after-sync)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                      witness :candidate :negative-before-sync :negative-after-sync)))))
             (setf (pg::retirement-intent-phase witness) (first case))
             (ok (handler-case (progn (funcall (second case)) nil)
                   (error (condition) (search "requires phase" (princ-to-string condition))))))
           (setf (pg::retirement-intent-phase witness) :predecessor)
           (ok (handler-case
                   (progn (funcall original-held witness) nil)
                 (error (condition) (search "already started" (princ-to-string condition)))))
           (setf (pg::retirement-intent-temporary-stream witness) :already-held)
           (unwind-protect
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-intent-created
                           witness (lambda (current) (declare (ignore current))))
                          nil)
                      (error (condition) (search "already held" (princ-to-string condition)))))
             (setf (pg::retirement-intent-temporary-stream witness) nil))
           (let* ((hit nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (point path)
                      (declare (ignore path))
                      (when (eq point :negative-before-sync)
                        (setf hit t
                              (pg::retirement-intent-effect-p witness) nil
                              (pg::retirement-startup-uncertain-p owner) nil)))))
             (ok (handler-case
                     (progn
                       (pg::physical-reseed-follow-retirement-intent-sync
                        witness :old-final :negative-before-sync :negative-after-sync)
                       nil)
                   (error (condition)
                     (search "requires an uncertain operation" (princ-to-string condition)))))
             (ok hit)))
      (setf (pg::retirement-intent-phase witness) :predecessor
            (pg::retirement-intent-effect-p witness) nil
            (pg::retirement-startup-uncertain-p owner) nil))))

(deftest reseed-retirement-publication-rejects-inconsistent-phase-and-temporary-ownership
    (let ((scope (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope))
          (check (symbol-function 'pg::physical-reseed-follow-retirement-intent-check))
          (sync (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync))
          (effects 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope)
                   (lambda (witness) (declare (ignore witness)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-check)
                   (lambda (witness) (declare (ignore witness)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (incf effects)
                     (error "unexpected retirement publication effect")))
             (dolist (phase '(:predecessor :complete))
               (let* ((owner (pg::make-physical-reseed-follow-retirement-startup-owner))
                      (witness
                        (pg::make-physical-reseed-follow-retirement-intent-witness
                         :owner owner :phase phase
                         :temporary-stream (when (eq phase :predecessor) :held)
                         :temporary-fd (when (eq phase :predecessor) 1))))
                 (ok (handler-case
                         (progn (pg::physical-reseed-follow-retirement-intent-publish-held witness) nil)
                       (error (condition) (search "changed phase" (princ-to-string condition)))))
                 (ok (zerop effects))
                 (ok (not (pg::retirement-intent-effect-p witness)))
                 (ok (not (pg::retirement-startup-uncertain-p owner))))))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope) scope
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-check) check
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync) sync))))

(deftest reseed-retirement-effect-leaves-require-uncertainty-before-filesystem-effects
    (let ((original-held (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held))
          (captured nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held)
                   (lambda (witness)
                     (setf captured witness)
                     (let ((original-sync (symbol-function 'sb-posix:fsync))
                           (original-open (symbol-function 'pg::physical-image-openat))
                           (original-write (symbol-function 'pg::physical-adoption-install-write-bytes))
                           (effects 0))
                       (unwind-protect
                            (progn
                              (setf (symbol-function 'sb-posix:fsync)
                                    (lambda (&rest arguments)
                                      (declare (ignore arguments))
                                      (incf effects)
                                      (error "unexpected sync before uncertainty"))
                                    (symbol-function 'pg::physical-adoption-install-write-bytes)
                                    (lambda (&rest arguments)
                                      (declare (ignore arguments))
                                      (incf effects)
                                      (error "unexpected write before uncertainty"))
                                    (symbol-function 'pg::physical-image-openat)
                                    (lambda (&rest arguments)
                                      (when (logtest sb-posix:o-creat (third arguments))
                                        (incf effects)
                                        (error "unexpected create before uncertainty"))
                                      (apply original-open arguments)))
                              (dolist
                                  (effect
                                   (list
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                                witness :predecessor-root :negative-before-sync :negative-after-sync))
                                    (lambda () (pg::call-with-physical-reseed-follow-retirement-intent-created
                                                witness (lambda (current) (declare (ignore current)))))
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-write witness))
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-promote witness))
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-adopt witness))))
                                (unwind-protect
                                     (ok (handler-case (progn (funcall effect) nil)
                                           (error (condition)
                                             (search "requires an uncertain operation" (princ-to-string condition)))))
                                  (setf (pg::retirement-intent-active-p witness) t))
                                (ok (zerop effects))
                                (ok (not (pg::retirement-intent-effect-p witness)))
                                (ok (not (pg::retirement-startup-uncertain-p
                                          (pg::retirement-intent-owner witness)))))
                              (check-retirement-intent-effect-phases witness original-held)
                              (ok (zerop effects)))
                         (setf (symbol-function 'sb-posix:fsync) original-sync
                               (symbol-function 'pg::physical-image-openat) original-open
                               (symbol-function 'pg::physical-adoption-install-write-bytes) original-write)))
                     :checked))
             (call-with-retirement-intent-test-owner
              (lambda (owner root root-fd snapshot)
                (declare (ignore root))
                (ok (eq :checked (pg::physical-reseed-follow-retirement-publish-initial owner)))
                (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                (ok (not (pg::retirement-startup-busy-p owner)))
                (ok (not (pg::retirement-startup-uncertain-p owner))))))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held) original-held))
      (ok (not (pg::retirement-intent-active-p captured)))
      (ok (handler-case
              (progn (pg::physical-reseed-follow-retirement-intent-check-scope captured) nil)
            (error (condition) (search "ended or changed thread" (princ-to-string condition)))))))

(deftest reseed-retirement-initial-publication-fences-every-publication-cut
    (dolist (point '(:after-retirement-old-final-fsync
                     :after-retirement-predecessor-directory-fsync
                     :after-retirement-temporary-create
                     :after-retirement-write-chunk
                     :after-retirement-temporary-fsync
                     :after-retirement-rename
                     :after-retirement-candidate-fsync
                     :after-retirement-directory-fsync
                     :after-retirement-readback))
      (ok
       (eq :checked
           (catch 'retirement-publication-cut-checked
             (call-with-retirement-intent-test-owner
              (lambda (owner root root-fd snapshot)
                (let* ((record (getf snapshot :record))
                       (expected (pg::physical-reseed-follow-retirement-next-record
                                  record (pg::physical-reseed-follow-retirement-next-reference root-fd record)))
                       (bytes (pg::physical-reseed-follow-image expected))
                       (pg::*physical-reseed-follow-write-limit* (ceiling (length bytes) 2))
                       (injected nil)
                       (pg::*physical-reseed-follow-test-hook*
                         (lambda (event path)
                           (declare (ignore path))
                           (when (eq event point)
                             (setf injected t)
                             (error "injected retirement publication cut")))))
                  (ok
                   (handler-case
                       (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                     (error (condition)
                       (search "injected retirement publication cut" (princ-to-string condition)))))
                  (ok injected)
                  (ok (not (pg::retirement-startup-busy-p owner)))
                  (ok (pg::retirement-startup-uncertain-p owner))
                  (ok
                   (handler-case
                       (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                     (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                  (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                  (ok (pg::postmaster-pid-owned-p))
                  (let* ((actual (pg::physical-reseed-follow-snapshot-at root-fd))
                         (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                         (renamed (member point '(:after-retirement-rename
                                                  :after-retirement-candidate-fsync
                                                  :after-retirement-directory-fsync
                                                  :after-retirement-readback))))
                    (if renamed
                        (progn
                          (ok (equal expected (getf actual :record)))
                          (ok (not (probe-file temporary))))
                        (progn
                          (ok (equal snapshot actual))
                          (cond
                            ((member point '(:after-retirement-old-final-fsync
                                             :after-retirement-predecessor-directory-fsync))
                             (ok (not (probe-file temporary))))
                            ((eq point :after-retirement-temporary-create)
                             (ok (zerop (length (pg::read-bounded-regular-file temporary 1 :minimum 0)))))
                            ((eq point :after-retirement-write-chunk)
                             (ok (equalp (subseq bytes 0 (ceiling (length bytes) 2))
                                         (pg::recovery-read-bytes temporary))))
                            (t (ok (equalp bytes (pg::recovery-read-bytes temporary))))))))
                  (throw 'retirement-publication-cut-checked :checked)))))))))

(deftest reseed-retirement-publication-fences-throw-and-self-interrupt
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace fixture-fd initial check)
       (declare (ignore fixture-fd check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((record (getf initial :record))
             (baseline (list (retirement-intent-test-nonjournal-facts root)
                             (reseed-mapped-test-tree-facts workspace))))
         (dolist (mode '(:throw :interrupt))
           (dolist (point '(:after-retirement-old-final-fsync
                            :after-retirement-predecessor-directory-fsync
                            :after-retirement-temporary-create
                            :after-retirement-write-chunk
                            :after-retirement-temporary-fsync
                            :after-retirement-rename
                            :after-retirement-candidate-fsync
                            :after-retirement-directory-fsync
                            :after-retirement-readback))
             (retirement-intent-test-reset root record)
             (let ((captured-owner nil))
               (ok
                (eq :checked
                    (catch 'retirement-nonlocal-checked
                      (call-with-retirement-intent-published-owner
                       root
                       (lambda (owner directory root-fd snapshot)
                         (declare (ignore directory))
                         (setf captured-owner owner)
                         (let* ((candidate
                                  (pg::physical-reseed-follow-retirement-next-record
                                   record
                                   (pg::physical-reseed-follow-retirement-next-reference root-fd record)))
                                (bytes (pg::physical-reseed-follow-image candidate))
                                (pg::*physical-reseed-follow-write-limit* (ceiling (length bytes) 2))
                                (injected nil)
                                (pg::*physical-reseed-follow-test-hook*
                                  (lambda (event path)
                                    (declare (ignore path))
                                    (when (eq event point)
                                      (setf injected t)
                                      (ecase mode
                                        (:throw (throw 'retirement-nonlocal-cut :cut))
                                        (:interrupt
                                         (sb-thread:interrupt-thread
                                          sb-thread:*current-thread*
                                          (lambda () (throw 'retirement-nonlocal-cut :cut)))))))))
                           (ok (eq :cut
                                   (catch 'retirement-nonlocal-cut
                                     (pg::physical-reseed-follow-retirement-publish-initial owner)
                                     :returned)))
                           (ok injected)
                           (ok (not (pg::retirement-startup-busy-p owner)))
                           (ok (pg::retirement-startup-uncertain-p owner))
                           (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                           (ok (handler-case
                                   (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                                 (error (condition)
                                   (search "busy or uncertain" (princ-to-string condition)))))
                           (let ((actual (pg::physical-reseed-follow-snapshot-at root-fd))
                                 (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
                             (if (member point '(:after-retirement-rename :after-retirement-candidate-fsync
                                                 :after-retirement-directory-fsync :after-retirement-readback))
                                 (progn
                                   (ok (equal candidate (getf actual :record)))
                                   (ok (not (probe-file temporary))))
                                 (progn
                                   (ok (equal snapshot actual))
                                   (cond
                                     ((member point '(:after-retirement-old-final-fsync
                                                      :after-retirement-predecessor-directory-fsync))
                                      (ok (not (probe-file temporary))))
                                     ((eq point :after-retirement-temporary-create)
                                      (ok (zerop (length (pg::read-bounded-regular-file temporary 1 :minimum 0)))))
                                     ((eq point :after-retirement-write-chunk)
                                      (ok (equalp (subseq bytes 0 (ceiling (length bytes) 2))
                                                  (pg::recovery-read-bytes temporary))))
                                     (t (ok (equalp bytes (pg::recovery-read-bytes temporary))))))))
                           (ok (pg::postmaster-pid-owned-p))
                           (ok (reseed-activation-layout-facts-equal-p
                                baseline
                                (list (retirement-intent-test-nonjournal-facts root)
                                      (reseed-mapped-test-tree-facts workspace))))
                           (throw 'retirement-nonlocal-checked :checked)))))))
               (ok (not (pg::retirement-startup-active-p captured-owner))))))
         (retirement-intent-test-reset root record)))))

(deftest reseed-retirement-initial-publication-fences-each-fsync-failure
    (loop for fail-at from 1 to 5
          do
             (ok
              (eq :checked
                  (catch 'retirement-sync-failure-checked
                    (call-with-retirement-intent-test-owner
                     (lambda (owner root root-fd snapshot)
                       (let* ((original (symbol-function 'sb-posix:fsync))
                              (calls 0) (injected nil) (failed-fd nil) (failed-inode nil))
                         (unwind-protect
                              (progn
                                (setf (symbol-function 'sb-posix:fsync)
                                      (lambda (fd)
                                        (incf calls)
                                        (when (= calls fail-at)
                                          (setf injected t failed-fd fd
                                                failed-inode (sb-posix:stat-ino (sb-posix:fstat fd)))
                                          (error "injected retirement fsync failure"))
                                        (funcall original fd)))
                                (ok
                                 (handler-case
                                     (progn
                                       (pg::physical-reseed-follow-retirement-publish-initial owner)
                                       nil)
                                   (error (condition)
                                     (search "injected retirement fsync failure" (princ-to-string condition))))))
                           (setf (symbol-function 'sb-posix:fsync) original))
                         (ok injected)
                         (ok (= calls fail-at))
                         (ok (handler-case (progn (sb-posix:fstat failed-fd) nil)
                               (sb-posix:syscall-error () t)))
                         (ok (not (pg::retirement-startup-busy-p owner)))
                         (ok (pg::retirement-startup-uncertain-p owner))
                         (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                         (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                                (path (case fail-at
                                        ((1 4) (merge-pathnames "physical-reseed.follow" root))
                                        ((2 5) root)
                                        (3 temporary)))
                                (actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                           (ok (= failed-inode (sb-posix:stat-ino (sb-posix:stat (namestring path)))))
                           (if (< fail-at 4)
                               (progn
                                 (ok (equal snapshot actual))
                                 (if (= fail-at 3)
                                     (ok (equal
                                          (pg::physical-reseed-follow-retirement-next-record
                                           (getf snapshot :record)
                                           (pg::physical-reseed-follow-retirement-next-reference
                                            root-fd (getf snapshot :record)))
                                          (pg::physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")))
                                     (ok (not (probe-file temporary)))))
                               (progn
                                 (ok (eq :retiring (getf (getf (getf actual :record) :retirement) :phase)))
                                 (ok (zerop (getf (getf (getf actual :record) :retirement) :step)))
                                 (ok (not (probe-file temporary))))))
                         (ok (pg::postmaster-pid-owned-p))
                         (throw 'retirement-sync-failure-checked :checked)))))))))

(deftest reseed-retirement-publication-keeps-the-owner-fenced-after-postcommit-cleanup-failure
    (let ((captured-owner nil))
      (ok
       (handler-case
           (progn
             (call-with-retirement-intent-test-owner
              (lambda (owner root root-fd snapshot)
                (declare (ignore snapshot))
                (setf captured-owner owner)
                (let ((original (symbol-function 'pg::physical-store-close))
                      (armed nil) (captured-store nil) (failures 0))
                  (unwind-protect
                       (progn
                         (setf (symbol-function 'pg::physical-store-close)
                               (lambda (store)
                                 (when armed
                                   (setf captured-store store)
                                   (incf failures)
                                   (error "injected retirement store cleanup failure"))
                                 (funcall original store)))
                         (let ((pg::*physical-reseed-follow-test-hook*
                                 (lambda (point path)
                                   (declare (ignore path))
                                   (when (eq point :after-retirement-readback) (setf armed t)))))
                           (ok (handler-case
                                   (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                                 (error (condition)
                                   (search "injected retirement store cleanup failure"
                                           (princ-to-string condition))))))
                         (ok (= 1 failures))
                         (ok (not (pg::physical-store-closed-p captured-store)))
                         (ok (not (pg::retirement-startup-busy-p owner)))
                         (ok (pg::retirement-startup-uncertain-p owner))
                         (let ((actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                           (ok (eq :retiring (getf (getf (getf actual :record) :retirement) :phase)))
                           (ok (zerop (getf (getf (getf actual :record) :retirement) :step)))
                           (ok (equal actual (getf (pg::retirement-startup-evidence owner) :snapshot))))
                         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                         (let ((entered nil))
                           (ok (handler-case
                                   (progn
                                     (pg::call-with-physical-reseed-follow-retirement-operation
                                      owner (lambda () (setf entered t)))
                                     nil)
                                 (error (condition)
                                   (search "busy or uncertain" (princ-to-string condition)))))
                           (ok (not entered))))
                    (setf (symbol-function 'pg::physical-store-close) original)
                    (when captured-store (funcall original captured-store)))
                  (ok (pg::physical-store-closed-p captured-store))
                  :ignored)))
             nil)
         (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
      (ok (not (pg::retirement-startup-active-p captured-owner)))))

(deftest reseed-retirement-publication-refuses-same-byte-final-replacement-after-readback
    (ok
     (eq :checked
         (catch 'retirement-late-replacement-checked
           (call-with-retirement-intent-test-owner
            (lambda (owner root root-fd snapshot)
              (let* ((candidate
                       (pg::physical-reseed-follow-retirement-next-record
                        (getf snapshot :record)
                        (pg::physical-reseed-follow-retirement-next-reference root-fd (getf snapshot :record))))
                     (original (symbol-function 'pg::physical-reseed-follow-retirement-intent-adopt))
                     (completed-adoptions 0) (held-fd nil) (injected nil) (old-inode nil) (new-inode nil)
                     (baseline (retirement-intent-test-nonjournal-facts root)))
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-adopt)
                             (lambda (witness)
                               (setf held-fd (pg::retirement-intent-temporary-fd witness))
                               (multiple-value-prog1 (funcall original witness)
                                 (incf completed-adoptions))))
                       (let ((pg::*physical-reseed-follow-test-hook*
                               (lambda (point path)
                                 (declare (ignore path))
                                 (when (eq point :after-retirement-readback)
                                   (setf old-inode
                                         (third (getf (pg::physical-reseed-follow-snapshot-at root-fd) :row)))
                                   (reseed-test-replace-follow-record root candidate)
                                   (setf new-inode
                                         (third (getf (pg::physical-reseed-follow-snapshot-at root-fd) :row))
                                         injected t)))))
                         (ok
                          (handler-case
                              (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                            (error (condition)
                              (format t "~%RETIREMENT-LATE-REPLACEMENT-REFUSAL ~a~%" condition)
                              (search "target follow journal is not a bounded private single-link regular file"
                                      (princ-to-string condition)))))))
                  (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-adopt) original))
                (ok injected)
                (ok (and held-fd
                         (handler-case (progn (sb-posix:fstat held-fd) nil)
                           (sb-posix:syscall-error () t))))
                (ok (and old-inode new-inode (/= old-inode new-inode)))
                (ok (zerop completed-adoptions))
                (ok (equal candidate (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record)))
                (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                (ok (not (pg::retirement-startup-busy-p owner)))
                (ok (pg::retirement-startup-uncertain-p owner))
                (ok (handler-case
                        (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                      (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                (ok (reseed-activation-layout-facts-equal-p
                     baseline (retirement-intent-test-nonjournal-facts root)))
                (ok (pg::postmaster-pid-owned-p))
                (throw 'retirement-late-replacement-checked :checked))))))))

(deftest reseed-retirement-publication-survives-process-crashes-under-fresh-startup
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace fixture-fd initial check)
       (declare (ignore fixture-fd check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let* ((record (getf initial :record))
              (baseline (list (retirement-intent-test-nonjournal-facts root)
                              (reseed-mapped-test-tree-facts workspace)))
              (candidate nil))
         (call-with-retirement-intent-published-owner
          root
          (lambda (owner directory root-fd snapshot)
            (declare (ignore owner directory))
            (setf candidate
                  (pg::physical-reseed-follow-retirement-next-record
                   (getf snapshot :record)
                   (pg::physical-reseed-follow-retirement-next-reference root-fd (getf snapshot :record))))))
         (with-physical-store-directory (driver)
           (let ((cache (merge-pathnames "cache/" driver)))
             (dolist (point '(:after-retirement-old-final-fsync
                              :after-retirement-predecessor-directory-fsync
                              :after-retirement-temporary-create
                              :after-retirement-write-chunk
                              :after-retirement-temporary-fsync
                              :after-retirement-rename
                              :after-retirement-candidate-fsync
                              :after-retirement-directory-fsync
                              :after-retirement-readback))
               (retirement-intent-test-reset root record)
               (ok (reseed-activation-layout-facts-equal-p
                    baseline (list (retirement-intent-test-nonjournal-facts root)
                                   (reseed-mapped-test-tree-facts workspace))))
               (let* ((marker (merge-pathnames (format nil "~(~a~).marker" point) driver))
                      (pid (run-retirement-intent-test-crash-child root point cache marker))
                      (stale (pg::physical-reseed-read-pid root))
                      (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                      (partial (member point '(:after-retirement-temporary-create :after-retirement-write-chunk)))
                      (renamed (member point '(:after-retirement-rename :after-retirement-candidate-fsync
                                               :after-retirement-directory-fsync :after-retirement-readback))))
                 (ok (= pid (getf stale :pid)))
                 (ok (reseed-activation-layout-facts-equal-p
                      baseline (list (retirement-intent-test-nonjournal-facts root)
                                     (reseed-mapped-test-tree-facts workspace))))
                 (if partial
                     (let ((entered nil)
                           (bytes (lineage-test-bytes temporary)))
                       (ok (handler-case
                               (progn
                                 (call-with-retirement-intent-published-owner
                                  root (lambda (&rest arguments)
                                         (declare (ignore arguments))
                                         (setf entered t)))
                                 nil)
                             (error (condition)
                               (search
                                (if (eq point :after-retirement-temporary-create)
                                    "target follow journal is not a bounded private single-link regular file"
                                    "physical backup lease checksum mismatch")
                                (princ-to-string condition)))))
                       (ok (not entered))
                       (let ((after (pg::physical-reseed-read-pid root)))
                         (ok (= (getf stale :pid) (getf after :pid)))
                         (ok (equal (getf stale :identity) (getf after :identity)))
                         (ok (equalp (getf stale :bytes) (getf after :bytes))))
                       (ok (equalp bytes (lineage-test-bytes temporary)))
                       (ok (equalp (pg::physical-reseed-follow-image record)
                                   (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))))
                     (call-with-retirement-intent-published-owner
                      root
                      (lambda (owner directory root-fd snapshot)
                        (declare (ignore directory))
                        (if renamed
                            (ok (equal candidate (getf snapshot :record)))
                            (multiple-value-bind (published actual)
                                (pg::physical-reseed-follow-retirement-publish-initial owner)
                              (ok (equal candidate published))
                              (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))))
                        (ok (not (probe-file temporary)))
                        (ok (pg::postmaster-pid-owned-p)))))
                 (ok (reseed-activation-layout-facts-equal-p
                      baseline (list (retirement-intent-test-nonjournal-facts root)
                                     (reseed-mapped-test-tree-facts workspace))))))
             (retirement-intent-test-reset root record)))))))
