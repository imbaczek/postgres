(in-package #:pg)

(defvar *logical-install-journal-test-result* nil)
(defvar *logical-install-journal-test-thread* nil)

(defun logical-install-journal-test-root (publisher)
  (sb-posix:fcntl
   (logical-slot-copy-install-node-fd (logical-slot-copy-install-publisher-root publisher)) 1030 0))

(defun logical-install-journal-test-undo (root token)
  (let ((work (physical-image-openat root "logical-slot-copy.work"
                                     (logior sb-posix:o-rdonly sb-posix:o-directory
                                             sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
    (assert (not (minusp work)))
    (unwind-protect
         (when (slot-state-lstat (slot-state-fd-path root "logical-slot-copy.install"))
           (assert (zerop (slot-state-renameat2 root "logical-slot-copy.install" work token 1)))
           (sb-posix:fsync work) (sb-posix:fsync root))
      (sb-posix:close work))))

(defun logical-install-journal-test-move-row (publisher root token)
  (let* ((slot-root (physical-image-openat root "pg_replslot"
                                           (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
         (slots nil) (name nil) (moved nil) (transferred nil))
    (unwind-protect
         (progn
           (assert (not (minusp slot-root)))
           (setf slots (physical-image-openat slot-root (format nil ".logical-slot-copy.work/~a" token)
                                              (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow +postmaster-o-cloexec+) 0)
                 name (first (first (getf (logical-slot-copy-install-decode
                                           (logical-slot-copy-install-publisher-image publisher)) :entries))))
           (assert (not (minusp slots)))
           (assert (zerop (slot-state-renameat2 slots name slot-root name 1)))
           (setf moved t)
           (sb-posix:fsync slots) (sb-posix:fsync slot-root)
           (prog1 (lambda ()
                    (assert (zerop (slot-state-renameat2 slot-root name slots name 1)))
                    (sb-posix:fsync slots) (sb-posix:fsync slot-root)
                    (sb-posix:close slots) (sb-posix:close slot-root))
             (setf transferred t)))
      (unless transferred
        (when moved (assert (zerop (slot-state-renameat2 slot-root name slots name 1))))
        (when (and slots (not (minusp slots))) (sb-posix:close slots))
        (when (not (minusp slot-root)) (sb-posix:close slot-root))))))

(defun logical-install-journal-test-run (mode)
  (let* ((cold (not *wal-ready*)) (before (logical-install-stage-test-fd-count))
                                  (root nil) (token nil) (saved nil) (cleanup nil) (auxiliary nil) (physical-cleanup nil)
                                  (fired nil) (caught nil) (renamed nil) (events nil) (sync-count 0) (owned-closes 0) (interrupt-delivered nil)
                                  (expected
                                    (cond ((member mode '(:throw :caught)) "scope ended")
                                          ((eq mode :rename-collision) "cannot publish exclusive")
                                          ((eq mode :source-cut) "journal source cut")
                                          ((eq mode :close-cut) "journal close cut")
                                          ((eq mode :close-interrupt) "journal async close cut")
                                          ((eq mode :proof-swap) "proof image")
                                          ((eq mode :late-phase) "changed its phase or prefix")
                                          ((eq mode :close-phase) "phase or prefix changed during cleanup")
                                          ((and (consp mode) (eq (first mode) :fsync)) "journal fsync cut")
                                          ((and (consp mode) (eq (first mode) :cut)) "journal event cut")))
                                  (original-rename (symbol-function 'physical-adoption-renameat2))
                                  (original-close (symbol-function 'sb-posix:close))
                                  (original-sync (symbol-function 'sb-posix:fsync))
                                  (original-source (symbol-function 'logical-slot-copy-check-committed-source)))
    (labels ((capture (publisher)
               (unless root
                 (setf root (logical-install-journal-test-root publisher)
                       token (getf (logical-slot-copy-install-decode
                                    (logical-slot-copy-install-publisher-image publisher)) :stage-id)))
               (setf saved publisher))
             (hook (point publisher)
               (capture publisher)
               (push point events)
               (assert (null *logical-slot-copy-install-inspection*))
               (when (eq point :after-journal-rename) (setf renamed t))
               (logical-install-handoff-test-denied
                (lambda () (logical-slot-copy-install-check-publisher
                            (copy-logical-slot-copy-install-publisher publisher))) "registered publisher")
               (let ((answer nil))
                 (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda ()
                     (setf answer (handler-case
                                      (progn (logical-slot-copy-install-check-publisher publisher) nil)
                                    (error () t))))))
                 (assert answer))
               (cond
                 ((and (consp mode) (eq (first mode) :kill) (eq point (second mode)))
                  (format t "~&LOGICAL-INSTALL-JOURNAL-KILL ~s~%" point) (finish-output)
                  (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                  (error "journal kill returned"))
                 ((and (consp mode) (eq (first mode) :cut) (eq point (second mode)))
                  (setf fired t) (error "journal event cut"))
                 ((and (eq point :after-journal-rename) (member mode '(:throw :caught)))
                  (setf fired t)
                  (if (eq mode :throw) (throw 'journal-exit :escaped) (error "journal caught cut")))
                 ((and (eq point :before-journal-rename) (eq mode :source-cut))
                  (setf (symbol-function 'logical-slot-copy-check-committed-source)
                        (lambda (&rest args) (declare (ignore args))
                          (setf fired t) (error "journal source cut"))))
                 ((and (eq point :before-journal-rename) (eq mode :proof-swap))
                  (let* ((image (logical-slot-copy-install-input-image
                                 (logical-slot-copy-install-publisher-input publisher)))
                         (old (aref image 0)))
                    (push (lambda () (setf (aref image 0) old)) auxiliary)
                    (setf (aref image 0) (logxor old 1) fired t)))
                 ((and (eq point :after-journal-final-proof) (eq mode :late-phase))
                  (let* ((slot-root (physical-image-openat root "pg_replslot"
                                                           (logior sb-posix:o-rdonly sb-posix:o-directory
                                                                   sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
                         (slots (physical-image-openat
                                 slot-root (format nil ".logical-slot-copy.work/~a" token)
                                 (logior sb-posix:o-rdonly sb-posix:o-directory
                                         sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
                         (name (first (first (getf (logical-slot-copy-install-decode
                                                    (logical-slot-copy-install-publisher-image publisher)) :entries)))))
                    (assert (and (not (minusp slot-root)) (not (minusp slots))))
                    (push (lambda ()
                            (assert (zerop (slot-state-renameat2 slot-root name slots name 1)))
                            (sb-posix:fsync slots) (sb-posix:fsync slot-root)
                            (sb-posix:close slots) (sb-posix:close slot-root)) auxiliary)
                    (assert (zerop (slot-state-renameat2 slots name slot-root name 1)))
                    (sb-posix:fsync slots) (sb-posix:fsync slot-root) (setf fired t)))))
             (publish (owner context input)
               (if (eq mode :caught)
                   (progn
                     (handler-case (logical-slot-copy-install-publish-journal owner context input)
                       (error (condition) (assert (search "journal caught cut" (princ-to-string condition)))))
                     (logical-slot-copy-install-check-input owner context input))
                   (logical-slot-copy-install-publish-journal owner context input)))
             (fixed-layout (prefix active)
               (call-with-logical-slot-copy-install-handoff
                (lambda (operation handoff)
                  (setf cleanup (logical-install-input-test-move-layout operation handoff prefix active))))
               (assert (eq (if active :active :pending) (logical-slot-copy-install-resync-fixed-journal)))))
      (unwind-protect
           (let ((*logical-slot-copy-install-publication-test-hook* #'hook))
             (when (eq mode :physical)
               (setf physical-cleanup (logical-install-input-test-physical :physical)))
             (when (eq mode :rename-collision)
               (setf (symbol-function 'physical-adoption-renameat2)
                     (lambda (from name to target flags)
                       (when (and (equal target "logical-slot-copy.install") (not fired))
                         (assert (= flags +physical-adoption-rename-noreplace+))
                         (assert (zerop (physical-image-mkdirat to target #o700)))
                         (push (lambda () (sb-posix:rmdir (namestring (slot-state-fd-path root target)))) auxiliary)
                         (setf fired t))
                       (funcall original-rename from name to target flags))))
             (when (and (consp mode) (eq (first mode) :fsync))
               (setf (symbol-function 'sb-posix:fsync)
                     (lambda (fd)
                       (let ((publisher (first *logical-slot-copy-install-publishers*)))
                         (when (and publisher renamed
                                    (or (= fd (logical-slot-copy-install-node-fd
                                               (logical-slot-copy-install-publisher-root publisher)))
                                        (member fd (logical-slot-copy-install-publisher-descriptors publisher))))
                           (incf sync-count)
                           (when (= sync-count (second mode))
                             (setf fired t) (error "journal fsync cut"))))
                       (funcall original-sync fd))))
             (when (member mode '(:close-cut :close-interrupt :close-phase))
               (setf (symbol-function 'sb-posix:close)
                     (lambda (fd)
                       (prog1 (funcall original-close fd)
                         (let ((publisher (first *logical-slot-copy-install-publishers*)))
                           (when (and publisher
                                      (find fd (logical-slot-copy-install-publisher-nodes publisher)
                                            :key #'logical-slot-copy-install-node-fd))
                             (incf owned-closes)
                             (unless fired
                               (setf fired t)
                               (case mode
                                 (:close-cut (error "journal close cut"))
                                 (:close-phase (push (logical-install-journal-test-move-row publisher root token) auxiliary))
                                 (:close-interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda ()
                                     (setf interrupt-delivered t)
                                     (assert (= owned-closes 4))
                                     (error "journal async close cut"))))))))))))
             (handler-case
                 (let ((result
                         (catch 'journal-exit
                           (cond
                             ((eq mode :absent)
                              (assert cold) (assert (null (logical-slot-copy-install-resync-fixed-journal))))
                             ((eq mode :orphan)
                              (assert cold)
                              (call-with-logical-slot-copy-install-handoff
                               (lambda (operation handoff) (declare (ignore operation handoff))))
                              (assert (null (logical-slot-copy-install-resync-fixed-journal))))
                             ((eq mode :recover)
                              (assert cold)
                              (assert (slot-state-lstat (merge-pathnames "logical-slot-copy.install" *pgdata*)))
                              (assert (eq :pending (logical-slot-copy-install-resync-fixed-journal)))
                              (dolist (function (list #'logical-slot-copy-preflight-startup #'logical-slot-copy-check-promotion))
                                (logical-install-handoff-test-denied function "child-owner import")))
                             ((eq mode :fixed-one) (assert cold) (fixed-layout 1 nil))
                             ((eq mode :fixed-all) (assert cold) (fixed-layout 2 nil))
                             ((eq mode :fixed-active) (assert cold) (fixed-layout 2 t))
                             (t
                              (assert (eq :pending (call-with-logical-slot-copy-install-fresh-input #'publish)))
                              (when (eq mode :fixed)
                                (assert cold)
                                (assert (eq :pending (logical-slot-copy-install-resync-fixed-journal)))))))))
                   (when (eq result :escaped)
                     (assert (eq mode :throw)) (setf caught t)))
               (error (condition)
                 (assert (and expected (search expected (princ-to-string condition)))
                         () "unexpected journal test failure ~s: ~a" mode condition)
                 (setf caught t)))
             (when (eq mode :close-interrupt) (assert interrupt-delivered))
             (when expected
               (assert fired)
               (assert caught)
               (assert *physical-recovery-failed-p*))
             (unless expected (assert (not *physical-recovery-failed-p*)))
             (when (and (not expected) (not (member mode '(:absent :orphan))))
               (let ((required '(:after-journal-file-sync :after-journal-active-sync :after-journal-container-sync
                                 :after-journal-source-parent-sync :after-journal-target-parent-sync
                                 :before-journal-final-proof :after-journal-final-proof)))
                 (dolist (event required) (assert (member event events))))
               (when (eq mode :fresh) (assert renamed)))
             (when saved
               (logical-install-handoff-test-denied
                (lambda () (logical-slot-copy-install-check-publisher saved)) "registered publisher")))
        (setf (symbol-function 'physical-adoption-renameat2) original-rename
              (symbol-function 'sb-posix:close) original-close
              (symbol-function 'sb-posix:fsync) original-sync
              (symbol-function 'logical-slot-copy-check-committed-source) original-source)
        (dolist (action auxiliary) (funcall action))
        (cond (cleanup (funcall cleanup))
              ((and root token) (logical-install-journal-test-undo root token)))
        (when root (sb-posix:close root))
        (when physical-cleanup (funcall physical-cleanup)))
      (assert (eq (not (null expected)) caught))
      (when cold
        (assert (logical-install-handoff-test-unopened-p))
        (assert (= before (logical-install-stage-test-fd-count))))))
  (format t "~&LOGICAL-INSTALL-JOURNAL-PASS ~s~%" mode) (finish-output))

(defun logical-install-journal-test-start (mode)
  (setf *logical-install-journal-test-result* :running
        *logical-install-journal-test-thread*
        (sb-thread:make-thread
         (lambda ()
           (handler-case (progn (logical-install-journal-test-run mode)
                                (setf *logical-install-journal-test-result* :passed))
             (error (condition) (setf *logical-install-journal-test-result* (list :error (princ-to-string condition))))))
         :name "owned journal publication")))
