(in-package #:pg-physical-tests)

(defun configured-cancellation-test-child-arguments (root cut cache marker form)
  (let* ((*package* (find-package :cl-user))
         (*print-length* nil) (*print-level* nil) (*print-readably* t)
         (*print-pretty* nil) (*print-circle* t)
         (arguments (retirement-intent-test-child-arguments root cut cache marker))
         (old-load (write-to-string '(asdf:load-system "pg-physical-tests")))
         (new-load (write-to-string '(asdf:load-system "pg"))))
    (unless (= 1 (count old-load arguments :test #'string=))
      (error "configured cancellation child lacks its expected system load"))
    (append (substitute new-load old-load (butlast arguments 2) :test #'string=)
            (list "--eval" "(make-package \"PG-PHYSICAL-TESTS\" :use '(\"CL\"))"
                  "--eval" (write-to-string form)))))

(cl-spec:defproperty configured-cancellation-child-printing-preserves-shared-forms
    ((pretty boolean) (circle boolean) (downcase boolean))
  (:about configured-cancellation-test-child-arguments)
  (:trials (:normal 100))
  (let* ((*print-pretty* pretty) (*print-circle* circle)
                                 (*print-case* (if downcase :downcase :upcase))
                                 (*read-eval* nil)
                                 (binding (list 'source-live nil))
                                 (form (list 'let (list binding) (cons 'setf binding)))
                                 (arguments (configured-cancellation-test-child-arguments
                                             (pathname "/tmp/native-print-probe/") :before-follow-rename
                                             (pathname "/tmp/native-print-probe/cache/") (pathname "/tmp/native-print-probe/marker") form)))
    (equal form (read-from-string (car (last arguments))))))

(deftest generated-configured-cancellation-child-printing-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'configured-cancellation-child-printing-preserves-shared-forms
                                   :seed 227 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(defun configured-cancellation-test-cut-stage (cut)
  (ecase cut
    (:before-follow-temporary-create :absent)
    ((:after-follow-temporary-create :after-follow-write-chunk) :partial)
    ((:before-follow-temporary-fsync :after-follow-temporary-fsync
      :before-follow-temporary-readback :after-follow-temporary-readback :before-follow-rename)
     :complete-temporary)
    ((:after-follow-rename :after-follow-directory-fsync :before-follow-readback :after-follow-readback)
     :published)))

(cl-spec:defproperty configured-cancellation-native-stage-keeps-publication-boundaries
    ((index (range integer 0 11)))
  (:about configured-cancellation-test-cut-stage)
  (:trials (:normal 100))
  (let* ((cut (nth index '(:before-follow-temporary-create :after-follow-temporary-create
                           :after-follow-write-chunk :before-follow-temporary-fsync
                           :after-follow-temporary-fsync :before-follow-temporary-readback
                           :after-follow-temporary-readback :before-follow-rename
                           :after-follow-rename :after-follow-directory-fsync
                           :before-follow-readback :after-follow-readback)))
         (stage (configured-cancellation-test-cut-stage cut)))
    (and (member stage '(:absent :partial :complete-temporary :published))
         (eq (eq stage :absent) (eq cut :before-follow-temporary-create))
         (eq (eq stage :partial)
             (not (null (member cut '(:after-follow-temporary-create :after-follow-write-chunk)))))
         (eq (eq stage :published)
             (not (null (member cut '(:after-follow-rename :after-follow-directory-fsync
                                      :before-follow-readback :after-follow-readback))))))))

(deftest generated-configured-cancellation-native-stage-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'configured-cancellation-native-stage-keeps-publication-boundaries
                                   :seed 223 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(defun configured-cancellation-test-source-evidence (phase active)
  (list (if (and active (eq phase :cleaned)) 1 0)
        (if (and active (eq phase :cleaned)) 6 0)
        (when (and active (eq phase :cleaned)) '(:connect :cleanup :release :close))
        t))

(defun configured-cancellation-test-source-form (phase record body)
  `(let ((connections 0) (identities 0) (events nil) (source-live nil))
     (setf (symbol-function 'pg::physical-libpq-helper-path)
           (lambda (&rest arguments)
             (declare (ignore arguments))
             (incf connections)
             (error "native cancellation requested an unowned source helper")))
     ,(if (eq phase :cleaned)
          `(let* ((source (pg::physical-follow-source-copy ',(getf (getf record :attempt) :source)))
                  (request (pg::physical-backup-lease-request-copy ',(getf (getf record :attempt) :request)))
                  (receipt (pg::physical-backup-lease-copy ',(getf (getf record :cancellation) :receipt)))
                  (client (gensym "NATIVE-CANCELLATION-CLIENT")))
             (assert (equal request (getf receipt :request)))
             (labels ((source-event (event)
                        (assert (pg::physical-reseed-follow-cancellation-operation-p
                                 pg::*physical-reseed-follow-cancellation-operation*))
                        (assert (pg::physical-reseed-follow-owner-busy-p
                                 (pg::physical-reseed-follow-cancellation-operation-owner
                                  pg::*physical-reseed-follow-cancellation-operation*)))
                        (assert (null pg::*physical-reseed-follow-installation-scope*))
                        (push event events))
                      (source-client-check (actual)
                        (assert (and source-live (eq actual client)))))
               (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                     (lambda (helper frozen &key on-spawn)
                       (assert (and (zerop connections) (not source-live)))
                       (assert (string= helper "/unused/helper"))
                       (assert (equal (getf source :owner) (pg::physical-libpq-frozen-profile-datum frozen)))
                       (incf connections)
                       (setf source-live t)
                       (when on-spawn (funcall on-spawn client))
                       (source-event :connect)
                       client)
                     (symbol-function 'pg::physical-adoption-attempt-check-client)
                     (lambda (actual actual-source)
                       (source-client-check actual)
                       (assert (equal source actual-source))
                       t)
                     (symbol-function 'pg::physical-client-identity-row)
                     (lambda (actual)
                       (source-client-check actual)
                       (incf identities)
                       (vector (write-to-string (getf request :system-id))
                               (write-to-string (1+ (getf request :timeline))) "0/0" nil))
                     (symbol-function 'pg::pg-client-native-backup-lease-cleanup-selected)
                     (lambda (actual supplied successor)
                       (source-client-check actual)
                       (assert (equal request supplied))
                       (assert (equal (getf receipt :successor) successor))
                       (assert (equal '(:connect) (reverse events)))
                       (source-event :cleanup)
                       (pg::physical-backup-lease-copy receipt))
                     (symbol-function 'pg::pg-client-native-release)
                     (lambda (actual name digest)
                       (source-client-check actual)
                       (assert (equal name (getf (getf receipt :successor) :name)))
                       (assert (= digest (getf (getf receipt :successor) :anchor-digest)))
                       (assert (equal '(:connect :cleanup) (reverse events)))
                       (source-event :release)
                       t)
                     (symbol-function 'pg::pg-client-native-backup-lease-status)
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (error "native acknowledgment requested unexpected STATUS"))
                     (symbol-function 'pg::pg-client-native-backup-lease-abort)
                     (lambda (&rest arguments)
                       (declare (ignore arguments))
                       (error "native acknowledgment requested unexpected ABORT"))
                     (symbol-function 'pg::physical-client-close)
                     (lambda (actual)
                       (source-client-check actual)
                       (setf source-live nil)
                       (source-event :close)))
               ,body))
          `(progn
             (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (incf connections)
                     (error "local native cancellation contacted its source")))
             ,body))))

(defun configured-cancellation-test-native-action (phase candidate owner)
  (ecase phase
    (:pending `(pg::physical-reseed-follow-begin-cancellation ,owner))
    (:source-closed
     `(pg::physical-reseed-follow-record-cancellation-source-closed
       ,owner ',(getf (getf candidate :cancellation) :receipt)))
    (:cleaned
     `(pg::physical-reseed-follow-cleanup-cancellation ,owner :helper "/unused/helper"))))

(defun configured-cancellation-test-native-state (root phase)
  (assert (member phase '(:pending :source-closed :cleaned)))
  (unless (eq phase :pending)
    (pg::call-with-physical-reseed-follow-cancellation-owner
     root
     (lambda (owner)
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (when (eq phase :cleaned)
           (pg::physical-reseed-follow-record-cancellation-source-closed
            owner (reseed-cancellation-source-parent pending :closed :handed-off t)))
         (reseed-follow-prepare-test-abort root pending)))))
  (let* ((record (pg::physical-reseed-follow-decode
                  (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (candidate
           (ecase phase
             (:pending (pg::physical-reseed-follow-cancellation-begin-record record))
             (:source-closed
              (pg::physical-reseed-follow-cancellation-source-closed-record
               record (reseed-cancellation-source-parent record :closed :handed-off t)))
             (:cleaned
              (pg::physical-reseed-follow-cancellation-cleaned-record
               record (getf (getf record :cancellation) :receipt) :selected-release)))))
    (values record candidate)))

(defun configured-cancellation-test-crash-child-arguments
    (root cut cache marker &optional (phase :pending) candidate)
  (unless candidate
    (assert (eq phase :pending))
    (setf candidate
          (pg::physical-reseed-follow-cancellation-begin-record
           (pg::physical-reseed-follow-decode
            (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
  (let ((evidence-path (pathname (concatenate 'string (namestring marker) ".evidence"))))
    (configured-cancellation-test-child-arguments
     root cut cache marker
     `(let ((pg::*pgdata* ,root))
        (assert (equal (truename (asdf:system-source-directory "pg"))
                       ,(truename (asdf:system-source-directory "pg"))))
        ,(configured-cancellation-test-source-form
          phase candidate
          `(let ((pg::*physical-reseed-follow-write-limit* 257)
                 (pg::*physical-reseed-follow-test-hook*
                   (lambda (event path)
                     (declare (ignore path))
                     (let ((authority pg::*physical-reseed-follow-cancellation-authority*))
                       (when (and (eq event ,cut)
                                  (pg::physical-reseed-follow-cancellation-authority-p authority)
                                  (eq ,phase (getf (getf (pg::physical-reseed-follow-cancellation-authority-candidate
                                                          authority) :cancellation) :phase)))
                         (assert (equal ',candidate
                                        (pg::physical-reseed-follow-cancellation-authority-candidate authority)))
                         (let ((source-evidence (list connections identities (reverse events) (not source-live))))
                           (assert (equal ',(configured-cancellation-test-source-evidence phase t) source-evidence))
                           (with-open-file (output ,marker :direction :output :if-exists :error)
                             (with-standard-io-syntax
                               (write (list (sb-posix:getpid) ,cut) :stream output)))
                           (with-open-file (output ,evidence-path :direction :output :if-exists :error)
                             (with-standard-io-syntax
                               (write (list (sb-posix:getpid) ,cut ,phase source-evidence) :stream output)))
                           (format t "~%CONFIGURED-CANCELLATION-CRASH-REACHED ~d ~s ~s ~s~%"
                                   (sb-posix:getpid) ,cut ,phase source-evidence)
                           (finish-output))
                         (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                         (error "configured cancellation child SIGKILL returned"))))))
             (pg::call-with-physical-reseed-follow-cancellation-owner
              ,root
              (lambda (owner)
                ,(configured-cancellation-test-native-action phase candidate 'owner)))))
        (error "configured cancellation child did not reach its crash point")))))

(defun configured-cancellation-test-recovery-child-form
    (root cut candidate marker &optional (phase :pending))
  (let* ((stage (configured-cancellation-test-cut-stage cut))
         (partial (eq stage :partial)) (absent (eq stage :absent))
         (source-evidence
           (configured-cancellation-test-source-evidence phase (member stage '(:absent :complete-temporary)))))
    `(let ((pg::*pgdata* ,root) (evidence nil))
       (assert (equal (truename (asdf:system-source-directory "pg"))
                      ,(truename (asdf:system-source-directory "pg"))))
       ,(configured-cancellation-test-source-form
         phase candidate
         `(flet ((check (value)
                   (assert value () "configured recovery ~s ~s check ~d failed"
                           ,phase ,cut (1+ (length evidence)))
                   (push t evidence)))
            (check (equal "pending offline reseed follow requires journal reconciliation"
                          (handler-case (progn (pg::physical-reseed-check-before-pid) nil)
                            (error (condition) (princ-to-string condition)))))
            ,(if partial
                 `(dolist (mode ',(if (eq phase :cleaned) '(nil t :cleaned) '(nil t)))
                    (let ((admitted nil))
                      (check (handler-case
                                 (progn
                                   (pg::call-with-physical-reseed-follow-cancellation-owner
                                    ,root (lambda (owner) (declare (ignore owner)) (setf admitted t))
                                    :recover mode :helper "/unused/helper")
                                   nil)
                               (error () t)))
                      (check (not admitted))))
                 `(pg::call-with-physical-reseed-follow-cancellation-owner
                   ,root
                   (lambda (owner)
                     ,@(when absent
                         `((check (equal ',candidate
                                         ,(configured-cancellation-test-native-action phase candidate 'owner)))))
                     (check (equal ',candidate (pg::physical-reseed-follow-read-configured-cancellation owner)))
                     (check (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                     (check (not (pg::physical-reseed-follow-owner-busy-p owner))))
                   :recover ,(unless absent (if (eq phase :cleaned) :cleaned t))
                   :helper "/unused/helper"))
            (check (equal "pending offline reseed follow requires journal reconciliation"
                          (handler-case (progn (pg::physical-reseed-check-before-pid) nil)
                            (error (condition) (princ-to-string condition)))))
            (let ((actual-source (list connections identities (reverse events) (not source-live))))
              (check (equal ',source-evidence actual-source))
              (with-open-file (output ,marker :direction :output :if-exists :error)
                (with-standard-io-syntax
                  (write (list (sb-posix:getpid) ,cut ,phase (nreverse evidence) actual-source) :stream output)))
              (format t "~%CONFIGURED-CANCELLATION-RECOVERY-PASS ~d ~s ~s ~s~%"
                      (sb-posix:getpid) ,cut ,phase actual-source)
              (finish-output)))))))

(defun run-configured-cancellation-test-recovery-child (root cut candidate cache marker log &optional (phase :pending))
  (let* ((form (configured-cancellation-test-recovery-child-form root cut candidate marker phase))
         (arguments (configured-cancellation-test-child-arguments root cut cache marker form))
         (stage (configured-cancellation-test-cut-stage cut))
         (checks (if (member stage '(:absent :partial)) 7 6))
         (source-evidence
           (configured-cancellation-test-source-evidence
            phase (member stage '(:absent :complete-temporary))))
         (process nil))
    (when (and (eq phase :cleaned) (eq stage :partial))
      (incf checks 2))
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
               (assert (equal (list (sb-ext:process-pid process) cut phase
                                    (make-list checks :initial-element t) source-evidence)
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

(defun check-configured-cancellation-native-cuts (cuts &optional (phase :pending))
  (with-physical-store-directory (driver)
    (let ((cache (merge-pathnames "native-cache/" driver)))
      (dolist (cut cuts)
        (testing (symbol-name cut)
                 (call-with-reseed-inert-settlement-fixture
                  :same-timeline
                  (lambda (owner root workspace) (declare (ignore owner root workspace)))
                  (lambda (root workspace)
                    (declare (ignore workspace))
                    (multiple-value-bind (record candidate)
                        (configured-cancellation-test-native-state root phase)
                      (let* ((final (merge-pathnames "physical-reseed.follow" root))
                             (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                             (before (lineage-test-bytes final))
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
                                         :argument-builder
                                         (lambda (root point cache marker)
                                           (configured-cancellation-test-crash-child-arguments
                                            root point cache marker phase candidate)))
                                      (error (condition)
                                        (let ((output (if (probe-file crash-log)
                                                          (uiop:read-file-string crash-log) "no child log")))
                                          (error "configured cancellation crash child failed: ~a~%~a"
                                                 condition
                                                 (subseq output (max 0 (- (length output) 12000)))))))
                                 (when (probe-file crash-log)
                                   (write-string (uiop:read-file-string crash-log)))))
                             (stage (configured-cancellation-test-cut-stage cut))
                             (partial (eq stage :partial)) (renamed (eq stage :published))
                             (temporary-bytes (when (pg::physical-adoption-path-present-p temporary)
                                                (lineage-test-bytes temporary)))
                             (identity (pg::physical-reseed-follow-file-identity
                                        (sb-posix:lstat (namestring (if temporary-bytes temporary final))) t)))
                        (ok (plusp crash-pid))
                        (with-open-file (input (pathname (concatenate 'string (namestring marker) ".evidence")))
                          (let ((*read-eval* nil) (*package* (find-package :cl-user)))
                            (ok (equal (list crash-pid cut phase
                                             (configured-cancellation-test-source-evidence phase t))
                                       (read input)))
                            (ok (eq :eof (read input nil :eof)))))
                        (with-open-file (input marker)
                          (let ((*read-eval* nil) (*package* (find-package :cl-user)))
                            (ok (equal (list crash-pid cut) (read input)))
                            (ok (eq :eof (read input nil :eof)))))
                        (ok (search (format nil "CONFIGURED-CANCELLATION-CRASH-REACHED ~d ~s" crash-pid cut)
                                    (uiop:read-file-string crash-log)))
                        (ok (equalp (pg::physical-reseed-follow-image (if renamed candidate record))
                                    (lineage-test-bytes final)))
                        (cond
                          (renamed
                           (ng (pg::physical-adoption-path-present-p temporary)))
                          ((eq cut :before-follow-temporary-create)
                           (ng (pg::physical-adoption-path-present-p temporary)))
                          ((eq cut :after-follow-temporary-create)
                           (ok (zerop (length temporary-bytes))))
                          (partial
                           (ok (< 0 (length temporary-bytes) (length (pg::physical-reseed-follow-image candidate))))
                           (ok (<= (length temporary-bytes) 257))
                           (ok (equalp temporary-bytes
                                       (subseq (pg::physical-reseed-follow-image candidate)
                                               0 (length temporary-bytes)))))
                          ((not renamed)
                           (ok (equalp (pg::physical-reseed-follow-image candidate) temporary-bytes))))
                        (let ((pid (run-configured-cancellation-test-recovery-child
                                    root cut candidate cache result log phase)))
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
                              (ok (equalp (pg::physical-reseed-follow-image candidate) (lineage-test-bytes final)))
                              (ng (pg::physical-adoption-path-present-p temporary))
                              (unless (eq cut :before-follow-temporary-create)
                                (ok (equal (subseq identity 0 7)
                                           (subseq (pg::physical-reseed-follow-file-identity
                                                    (sb-posix:lstat (namestring final))) 0 7))))))
                        (ok (equalp facts
                                    (configured-cancellation-test-preserved-file-facts root))))))))))))

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

(deftest reseed-configured-source-closed-native-create-cuts
    (check-configured-cancellation-native-cuts
     '(:before-follow-temporary-create :after-follow-temporary-create) :source-closed))

(deftest reseed-configured-source-closed-native-payload-cuts
    (check-configured-cancellation-native-cuts
     '(:after-follow-write-chunk :after-follow-temporary-fsync) :source-closed))

(deftest reseed-configured-source-closed-native-publication-cuts
    (check-configured-cancellation-native-cuts
     '(:before-follow-rename :after-follow-rename) :source-closed))

(deftest reseed-configured-source-closed-native-readback-cuts
    (check-configured-cancellation-native-cuts
     '(:after-follow-directory-fsync :after-follow-readback) :source-closed))

(deftest reseed-configured-cleaned-native-create-cuts
    (check-configured-cancellation-native-cuts
     '(:before-follow-temporary-create :after-follow-temporary-create) :cleaned))

(deftest reseed-configured-cleaned-native-payload-cuts
    (check-configured-cancellation-native-cuts
     '(:after-follow-write-chunk :after-follow-temporary-fsync) :cleaned))

(deftest reseed-configured-cleaned-native-publication-cuts
    (check-configured-cancellation-native-cuts
     '(:before-follow-rename :after-follow-rename) :cleaned))

(deftest reseed-configured-cleaned-native-readback-cuts
    (check-configured-cancellation-native-cuts
     '(:after-follow-directory-fsync :after-follow-readback) :cleaned))
