(in-package #:pg)

(defvar *logical-install-rows-test-result* nil)
(defvar *logical-install-rows-test-thread* nil)

(defun logical-install-rows-test-parents (root token function)
  (let ((fds nil))
    (labels ((open-child (parent name)
               (let ((fd (physical-image-openat parent name
                                                (logior sb-posix:o-rdonly sb-posix:o-directory
                                                        sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
                 (assert (not (minusp fd))) (push fd fds) fd)))
      (unwind-protect
           (let* ((slots (open-child root "pg_replslot"))
                  (work (open-child slots ".logical-slot-copy.work"))
                  (container (open-child work token)))
             (funcall function slots container))
        (dolist (fd fds) (sb-posix:close fd))))))

(defun logical-install-rows-test-restore (root image)
  (let* ((value (logical-slot-copy-install-decode image)) (token (getf value :stage-id)))
    (logical-install-rows-test-parents
     root token
     (lambda (slots container)
       (dolist (row (reverse (getf value :entries)))
         (when (slot-state-lstat (slot-state-fd-path slots (first row)))
           (assert (zerop (slot-state-renameat2 slots (first row) container (first row) 1)))
           (sb-posix:fsync slots) (sb-posix:fsync container)))))
    (logical-install-journal-test-undo root token)))

(defun logical-install-rows-test-run (mode)
  (let* ((cold (not *wal-ready*)) (before (logical-install-stage-test-fd-count))
                                  (root nil) (image nil) (saved nil) (fired nil) (caught nil) (auxiliary nil)
                                  (events nil) (sync-count 0) (rename-count 0) (physical-cleanup nil) (target-state-fd nil) (target-synced nil) (open-fd-count nil)
                                  (close-completed nil) (interrupt-delivered nil)
                                  (original-rename (symbol-function 'physical-adoption-renameat2))
                                  (original-sync (symbol-function 'sb-posix:fsync))
                                  (original-close (symbol-function 'sb-posix:close))
                                  (original-source (symbol-function 'logical-slot-copy-check-committed-source))
                                  (expected (cond ((eq mode :unspent) "spent fresh handoff")
                                                  ((eq mode :source-cut) "rows source cut")
                                                  ((eq mode :caught) "scope ended") ((eq mode :throw) :nonlocal)
                                                  ((eq mode :close-cut) "rows close cut")
                                                  ((eq mode :close-interrupt) "rows close interrupt")
                                                  ((eq mode :nested-slot-mount) "binding or privacy changed")
                                                  ((eq mode :proof-swap) "proof image")
                                                  ((eq mode :closed-row-change) "closed row facts changed")
                                                  ((member mode '(:collision-alpha :collision-beta)) "cannot exclusively install")
                                                  ((and (consp mode) (eq :cut (first mode))) "rows event cut")
                                                  ((and (consp mode) (eq :fsync (first mode))) "rows fsync cut"))))
    (unless (or expected (member mode '(:fresh :resume-zero :resume-one :resume-all :physical :absent :orphan :recover :separate-slot-root))
                (and (consp mode) (eq :kill (first mode))))
      (error "unknown logical installation rows test mode"))
    (labels ((capture (publisher)
               (unless root
                 (setf root (logical-install-journal-test-root publisher)
                       image (copy-seq (logical-slot-copy-install-publisher-image publisher)))))
             (journal-hook (point publisher)
               (declare (ignore point)) (capture publisher))
             (row-hook (point writer name)
               (capture writer) (setf saved writer) (push (list point name) events)
               (assert (null *logical-slot-copy-install-inspection*))
               (when (and (eq point :row-open) (null target-state-fd))
                 (setf target-state-fd
                       (logical-slot-copy-install-node-fd
                        (find "state" (logical-slot-copy-install-publisher-nodes writer)
                              :key #'logical-slot-copy-install-node-name :test #'equal))))
               (logical-install-handoff-test-denied
                (lambda () (logical-slot-copy-install-check-row-writer
                            (copy-logical-slot-copy-install-row-writer writer))) "registered publisher")
               (let ((answer nil))
                 (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda () (setf answer (handler-case
                                               (progn (logical-slot-copy-install-check-row-writer writer) nil)
                                             (error () t))))))
                 (assert answer))
               (when (and cold (eq point :row-open))
                 (let ((count (logical-install-stage-test-fd-count)))
                   (if open-fd-count (assert (<= count open-fd-count))
                       (setf open-fd-count count))))
               (when (and (eq mode :nested-slot-mount) (eq point :row-open) (equal name "alpha"))
                 (let* ((path (namestring (merge-pathnames "pg_replslot/.logical-slot-copy.work/"
                                                           (physical-adoption-owner-directory
                                                            (logical-slot-copy-install-publisher-operation writer)))))
                        (mounted nil))
                   (push (lambda ()
                           (when mounted
                             (assert (zerop (sb-ext:process-exit-code
                                             (sb-ext:run-program "/usr/bin/sudo" (list "-n" "/usr/bin/umount" path)
                                                                 :wait t :input nil :output nil :error nil)))))) auxiliary)
                   (assert (zerop (sb-ext:process-exit-code
                                   (sb-ext:run-program "/usr/bin/sudo" (list "-n" "/usr/bin/mount" "--bind" path path)
                                                       :wait t :input nil :output nil :error nil))))
                   (setf mounted t fired t)))
               (when (and (consp mode) (member (first mode) '(:cut :kill))
                          (eq point (second mode)) (equal name (third mode)))
                 (setf fired t)
                 (if (eq :kill (first mode))
                     (progn (format t "~&LOGICAL-INSTALL-ROWS-KILL ~s ~s~%" point name) (finish-output)
                            (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                            (error "rows kill returned"))
                     (error "rows event cut")))
               (when (and (eq point :after-row-rename) (equal name "alpha") (member mode '(:caught :throw)))
                 (setf fired t)
                 (if (eq mode :throw) (throw 'rows-exit :escaped) (error "rows caught cut")))
               (when (and (eq point :row-open) (equal name "alpha") (eq mode :source-cut))
                 (setf (symbol-function 'logical-slot-copy-check-committed-source)
                       (lambda (&rest args) (declare (ignore args)) (setf fired t) (error "rows source cut"))))
               (when (and (eq point :row-open) (equal name "alpha") (eq mode :proof-swap))
                 (let* ((bytes (logical-slot-copy-install-input-image (logical-slot-copy-install-publisher-input writer)))
                        (old (aref bytes 0)))
                   (push (lambda () (setf (aref bytes 0) old)) auxiliary)
                   (setf (aref bytes 0) (logxor old 1) fired t)))
               (when (and (eq point :after-row-close) (equal name "alpha") (eq mode :closed-row-change))
                 (let* ((path (slot-state-fd-path root "pg_replslot/alpha/state"))
                        (facts (physical-image-file-facts (physical-image-context-root-fd
                                                           (logical-slot-copy-install-publisher-context writer))
                                                          "pg_replslot/alpha/state")))
                   (declare (ignore facts))
                   (sb-posix:chmod (namestring path) #o400)
                   (sb-posix:chmod (namestring path) #o600)
                   (setf fired t))))
             (prepare (prefix)
               (call-with-logical-slot-copy-install-fresh-input
                (lambda (owner context input)
                  (logical-slot-copy-install-publish-journal owner context input)
                  (logical-install-rows-test-parents
                   root (getf (logical-slot-copy-install-decode image) :stage-id)
                   (lambda (slots container)
                     (loop for row in (getf (logical-slot-copy-install-decode image) :entries)
                           for index below prefix
                           do (assert (zerop (slot-state-renameat2 container (first row) slots (first row) 1)))
                              (sb-posix:fsync slots) (sb-posix:fsync container))))))))
      (unwind-protect
           (let ((*logical-slot-copy-install-publication-test-hook* #'journal-hook)
                 (*logical-slot-copy-install-row-test-hook* #'row-hook))
             (when (eq mode :physical) (setf physical-cleanup (logical-install-input-test-physical :physical)))
             (setf (symbol-function 'physical-adoption-renameat2)
                   (lambda (from name to target flags)
                     (when (and (not (equal target "logical-slot-copy.install")) (equal name target))
                       (incf rename-count)
                       (when (and (member mode '(:collision-alpha :collision-beta))
                                  (equal name (if (eq mode :collision-alpha) "alpha" "beta")))
                         (assert (= flags +physical-adoption-rename-noreplace+))
                         (assert (zerop (physical-image-mkdirat to name #o700)))
                         (push (lambda () (sb-posix:rmdir (namestring (slot-state-fd-path root
                                                                                          (format nil "pg_replslot/~a" name))))) auxiliary)
                         (setf fired t)))
                     (funcall original-rename from name to target flags)))
             (setf (symbol-function 'sb-posix:fsync)
                   (lambda (fd)
                     (let* ((candidate (first *logical-slot-copy-install-publishers*))
                            (writer (when (logical-slot-copy-install-row-writer-p candidate) candidate))
                            (target (and writer (logical-slot-copy-install-row-writer-slot-root writer)
                                         (= fd (logical-slot-copy-install-node-fd
                                                (logical-slot-copy-install-row-writer-slot-root writer)))))
                            (source (and writer (logical-slot-copy-install-row-writer-slots writer)
                                         (= fd (logical-slot-copy-install-node-fd
                                                (logical-slot-copy-install-row-writer-slots writer)))))
                            (epoch (when writer (length (logical-slot-copy-install-row-writer-closed writer)))))
                       (when source (assert (and (eq writer (first target-synced)) (= epoch (second target-synced)))))
                       (when (and writer (consp mode) (eq :fsync (first mode))
                                  (member fd (logical-slot-copy-install-publisher-descriptors writer)))
                         (incf sync-count)
                         (when (= sync-count (second mode)) (setf fired t) (error "rows fsync cut")))
                       (prog1 (funcall original-sync fd)
                         (when target (setf target-synced (list writer epoch)))))))
             (when (member mode '(:close-cut :close-interrupt))
               (setf (symbol-function 'sb-posix:close)
                     (lambda (fd)
                       (let* ((writer (first *logical-slot-copy-install-publishers*))
                              (target (and (logical-slot-copy-install-row-writer-p writer) (not fired)
                                           (eql fd target-state-fd))))
                         (when (and target (eq mode :close-interrupt))
                           (setf fired t)
                           (sb-thread:interrupt-thread
                            sb-thread:*current-thread*
                            (lambda () (setf interrupt-delivered t)
                              (assert close-completed) (error "rows close interrupt"))))
                         (prog1 (funcall original-close fd)
                           (when target
                             (setf close-completed t)
                             (when (eq mode :close-cut) (setf fired t) (error "rows close cut"))))))))
             (handler-case
                 (let ((result
                         (catch 'rows-exit
                           (cond
                             ((eq mode :absent) (assert cold) (assert (null (logical-slot-copy-install-resume-rows))))
                             ((eq mode :orphan)
                              (assert cold)
                              (call-with-logical-slot-copy-install-handoff
                               (lambda (operation handoff) (declare (ignore operation handoff))))
                              (assert (null (logical-slot-copy-install-resume-rows))))
                             ((eq mode :unspent)
                              (setf fired t)
                              (call-with-logical-slot-copy-install-fresh-input #'logical-slot-copy-install-rows))
                             ((member mode '(:resume-zero :resume-one :resume-all))
                              (assert cold) (prepare (case mode (:resume-zero 0) (:resume-one 1) (t 2)))
                              (assert (= 2 (logical-slot-copy-install-resume-rows)))
                              (assert (= rename-count (case mode (:resume-zero 2) (:resume-one 1) (t 0)))))
                             ((eq mode :recover)
                              (assert cold)
                              (assert (= 2 (logical-slot-copy-install-resume-rows))))
                             ((eq mode :caught)
                              (call-with-logical-slot-copy-install-fresh-input
                               (lambda (owner context input)
                                 (logical-slot-copy-install-publish-journal owner context input)
                                 (handler-case (logical-slot-copy-install-rows owner context input)
                                   (error (condition) (assert (search "rows caught cut" (princ-to-string condition)))))
                                 (logical-slot-copy-install-check-input owner context input))))
                             (t (assert (= 2 (logical-slot-copy-install-fresh-rows))))))))
                   (when (eq result :escaped) (assert (eq mode :throw)) (setf caught t)))
               (error (condition)
                 (assert (and (stringp expected) (search expected (princ-to-string condition)))
                         () "unexpected rows test failure ~s: ~a" mode condition)
                 (setf caught t)))
             (when (eq mode :close-interrupt) (assert interrupt-delivered) (assert close-completed))
             (when expected (assert fired) (assert caught))
             (assert (eq caught (not (null *physical-recovery-failed-p*))))
             (unless (or expected (member mode '(:absent :orphan)))
               (assert (= 2 (count :after-row-close events :key #'first)))
               (when cold
                 (dolist (function (list #'logical-slot-copy-preflight-startup #'logical-slot-copy-check-promotion))
                   (logical-install-handoff-test-denied function "child-owner import"))))
             (when saved
               (logical-install-handoff-test-denied
                (lambda () (logical-slot-copy-install-check-row-writer saved)) "registered publisher")))
        (setf (symbol-function 'physical-adoption-renameat2) original-rename
              (symbol-function 'sb-posix:fsync) original-sync
              (symbol-function 'sb-posix:close) original-close
              (symbol-function 'logical-slot-copy-check-committed-source) original-source)
        (dolist (action auxiliary) (funcall action))
        (when root (unwind-protect (logical-install-rows-test-restore root image) (sb-posix:close root)))
        (when physical-cleanup (funcall physical-cleanup)))
      (assert (eq (not (null expected)) caught))
      (when cold
        (assert (logical-install-handoff-test-unopened-p))
        (assert (= before (logical-install-stage-test-fd-count))))))
  (format t "~&LOGICAL-INSTALL-ROWS-PASS ~s~%" mode) (finish-output))

(defun logical-install-rows-test-start (mode)
  (setf *logical-install-rows-test-result* :running
        *logical-install-rows-test-thread*
        (sb-thread:make-thread
         (lambda ()
           (handler-case (progn (logical-install-rows-test-run mode) (setf *logical-install-rows-test-result* :passed))
             (error (condition) (setf *logical-install-rows-test-result* (list :error (princ-to-string condition))))))
         :name "owned slot installation rows")))
