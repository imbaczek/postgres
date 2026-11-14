(in-package #:pg)

(defvar *logical-install-handoff-test-result* nil)

(defvar *logical-install-handoff-test-thread* nil)

(defun logical-install-handoff-test-denied (function message)
  (assert (handler-case (progn (funcall function) nil)
            (error (condition) (search message (princ-to-string condition))))))

(defun logical-install-handoff-test-unopened-p ()
  (and (not *wal-ready*) (null *physical-recovery-state*) (null *physical-receive-store*)
       (null *catalog*) (null *wal-output-stream*) (null *clog-stream*)
       (zerop *n-backends*) (zerop (hash-table-count *relation-streams*))
       (zerop (hash-table-count *wal-replication-slots*))))

(defun logical-install-handoff-test-inspect (operation handoff before cold)
  (logical-slot-copy-install-check-operation operation)
  (assert (null *logical-slot-copy-install-preparations*))
  (assert (null *logical-slot-copy-install-stages*))
  (assert (null *logical-slot-copy-install-file-scope*))
  (assert (null *physical-image-context*))
  (assert (= 1 (length *postmaster-ownership-contexts*)))
  (when cold (assert (= (1+ before) (logical-install-stage-test-fd-count))))
  (let* ((record (logical-slot-copy-install-operation-record operation))
         (fd (logical-slot-copy-install-operation-state-root-fd record))
         (image (logical-slot-copy-install-handoff-image operation handoff))
         (reference (copy-seq image))
         (value (logical-slot-copy-install-decode image))
         (token (getf value :stage-id)))
    (assert (eq :pending (getf (logical-slot-copy-import-decode (getf value :certificate)) :phase)))
    (logical-install-handoff-test-denied
     (lambda () (logical-slot-copy-install-handoff-image (copy-structure operation) handoff)) "scope ended")
    (logical-install-handoff-test-denied
     (lambda () (logical-slot-copy-install-handoff-image operation (copy-structure handoff))) "not available")
    (logical-install-handoff-test-denied
     (lambda () (physical-adoption-check-owner operation)) "does not authorize offline adoption")
    (logical-install-handoff-test-denied
     (lambda () (logical-slot-copy-install-open-node nil "forbidden" :directory nil t t))
     "requires its preparation scope")
    (logical-install-handoff-test-denied
     (lambda () (call-with-logical-slot-copy-install-handoff
                 (lambda (&rest values) (declare (ignore values)) (error "nested callback reached"))))
     "already active")
    (let* ((registry *logical-slot-copy-install-operations*)
           (thread (sb-thread:make-thread
                    (lambda ()
                      (let ((*logical-slot-copy-install-operations* registry))
                        (handler-case
                            (progn (logical-slot-copy-install-handoff-image operation handoff) nil)
                          (error (condition) (not (null (search "changed thread" (princ-to-string condition))))))))
                    :name "foreign handoff observer")))
      (assert (eq t (sb-thread:join-thread thread :timeout 5 :default :timeout))))
    (setf (aref image 0) 0)
    (assert (equalp reference (logical-slot-copy-install-handoff-image operation handoff)))
    (let ((taken (logical-slot-copy-install-consume-handoff operation handoff)))
      (assert (equalp reference taken))
      (setf (aref taken 0) 0))
    (logical-install-handoff-test-denied
     (lambda () (logical-slot-copy-install-consume-handoff operation handoff)) "not available")
    (assert (equalp reference
                    (read-bounded-regular-file
                     (slot-state-fd-path fd (format nil "logical-slot-copy.work/~a/journal" token))
                     +logical-slot-copy-install-maximum-bytes+ :stabilize nil)))
    (format t "~&LOGICAL-HANDOFF-STAGE-ID ~a~%" token)
    token))

(defun logical-install-handoff-test-bind-root (operation handoff)
  (let* ((record (logical-slot-copy-install-operation-record operation))
         (fd (logical-slot-copy-install-operation-state-root-fd record))
         (path (namestring (physical-adoption-owner-directory operation)))
         (process (sb-ext:run-program "sudo" (list "-n" "mount" "--bind" path path)
                                      :search t :wait t :input nil
                                      :output *standard-output* :error *error-output*)))
    (assert (zerop (sb-ext:process-exit-code process)))
    (unwind-protect
         (let ((held (physical-image-file-facts fd)) (named (physical-image-file-facts -100 path)))
           (assert (equal (subseq held 0 2) (subseq named 0 2)))
           (assert (/= (ninth held) (ninth named)))
           (format t "~&LOGICAL-HANDOFF-BIND-REJECT ~d ~d~%" (ninth held) (ninth named))
           (logical-install-handoff-test-denied
            (lambda () (logical-slot-copy-install-handoff-image operation handoff)) "root binding changed"))
      (let ((process (sb-ext:run-program "sudo" (list "-n" "umount" path)
                                         :search t :wait t :input nil
                                         :output *standard-output* :error *error-output*)))
        (assert (zerop (sb-ext:process-exit-code process)))))
    (logical-install-handoff-test-denied
     (lambda () (logical-slot-copy-install-consume-handoff operation handoff)) "scope ended")))

(defun logical-install-handoff-test-run (mode)
  (let* ((before (logical-install-stage-test-fd-count)) (cold (not *wal-ready*))
                                                        (saved nil) (callback-called nil) (fired nil) (stage-returned nil)
                                                        (close-target nil) (old-token nil)
                                                        (source (symbol-function 'logical-slot-copy-check-committed-source))
                                                        (postmaster (symbol-function 'postmaster-check-ownership))
                                                        (close (symbol-function 'sb-posix:close))
                                                        (stage-id (symbol-function 'logical-slot-copy-install-stage-id))
                                                        (*logical-slot-copy-install-handoff-test-hook*
                                                          (lambda (point operation handoff)
                                                            (when handoff (setf saved (list operation handoff)))
                                                            (when (eq point :after-stage) (setf stage-returned t))
                                                            (when (or (and (eq mode :before-stage) (eq point :before-stage))
                                                                      (and (eq mode :after-stage) (eq point :after-stage))
                                                                      (and (eq mode :after-preparation) (eq point :after-preparation))
                                                                      (and (eq mode :after-mint) (eq point :after-mint)))
                                                              (setf fired t)
                                                              (error "handoff phase cut ~s" point))
                                                            (when (and (eq mode :after-preparation-throw) (eq point :after-preparation))
                                                              (setf fired t) (throw 'handoff-exit :escaped)))))
    (labels ((callback (operation handoff)
               (setf saved (list operation handoff) callback-called t)
               (case mode
                 (:callback-error (setf fired t) (error "handoff callback cut"))
                 (:callback-throw (setf fired t) (throw 'handoff-exit :escaped))
                 (:root-bind
                  (setf fired t)
                  (logical-install-handoff-test-bind-root operation handoff))
                 (:mode-swap
                  (let ((*physical-recovery-state* (list :phase 3)))
                    (setf fired t)
                    (logical-install-handoff-test-denied
                     (lambda () (logical-slot-copy-install-handoff-image operation handoff))
                     "requires unopened storage"))
                  (logical-install-handoff-test-denied
                   (lambda () (logical-slot-copy-install-consume-handoff operation handoff)) "scope ended"))
                 (:lock-swap
                  (let ((*physical-follow-generation-lock* (sb-thread:make-mutex :name "false generation fence")))
                    (setf fired t)
                    (logical-install-handoff-test-denied
                     (lambda () (logical-slot-copy-install-handoff-image operation handoff))
                     "exclusion identities changed"))
                  (logical-install-handoff-test-denied
                   (lambda () (logical-slot-copy-install-consume-handoff operation handoff)) "scope ended"))
                 (:gate-swap
                  (let ((*live-operation-quiescence-owner* (gensym "false gate")))
                    (setf fired t)
                    (logical-install-handoff-test-denied
                     (lambda () (logical-slot-copy-install-handoff-image operation handoff))
                     "fully drained admission gate"))
                  (logical-install-handoff-test-denied
                   (lambda () (logical-slot-copy-install-consume-handoff operation handoff)) "scope ended"))
                 (t
                  (when (eq mode :root-close)
                    (setf close-target
                          (logical-slot-copy-install-operation-state-root-fd
                           (logical-slot-copy-install-operation-record operation))))
                  (logical-install-handoff-test-inspect operation handoff before cold)))))
      (unwind-protect
           (handler-case
               (progn
                 (when (eq mode :source-reproof)
                   (setf (symbol-function 'logical-slot-copy-check-committed-source)
                         (lambda (&rest arguments)
                           (when stage-returned (setf fired t) (error "handoff final source cut"))
                           (apply source arguments))))
                 (when (eq mode :final-ownership)
                   (setf (symbol-function 'postmaster-check-ownership)
                         (lambda (ownership)
                           (when (and callback-called saved
                                      (not (physical-adoption-owner-active-p (first saved))))
                             (setf fired t) (error "handoff final ownership cut"))
                           (funcall postmaster ownership))))
                 (when (eq mode :root-close)
                   (setf (symbol-function 'sb-posix:close)
                         (lambda (fd)
                           (prog1 (funcall close fd)
                             (when (eql fd close-target)
                               (setf fired t) (error "handoff outer root close cut"))))))
                 (when (eq mode :reuse-orphan)
                   (setf (symbol-function 'logical-slot-copy-install-stage-id)
                         (lambda (attempt requested)
                           (declare (ignore attempt requested))
                           (setf fired t)
                           (sb-ext:posix-getenv "PG_LISP_HANDOFF_OLD_STAGE"))))
                 (catch 'handoff-exit
                   (if (eq mode :cross-operation)
                       (progn
                         (setf old-token (call-with-logical-slot-copy-install-handoff #'callback))
                         (let ((prior saved))
                           (call-with-logical-slot-copy-install-handoff
                            (lambda (operation handoff)
                              (logical-install-handoff-test-denied
                               (lambda () (logical-slot-copy-install-handoff-image operation (second prior)))
                               "not available")
                              (assert (not (string= old-token (callback operation handoff))))))))
                       (call-with-logical-slot-copy-install-handoff #'callback)))
                 (unless (member mode '(:healthy :healthy-retry :cross-operation))
                   (error "handoff nonlocal or revoked exit")))
             (error (condition)
               (when (and (eq mode :invalid-input)
                          (search "exact child control" (princ-to-string condition)))
                 (setf fired t))
               (assert fired () "handoff fault was not reached: ~a" condition)
               (assert (eq (not (null *physical-recovery-failed-p*)) (not (member mode '(:before-stage :invalid-input)))))
               (when (member mode '(:invalid-input :before-stage :after-stage :after-preparation
                                    :after-preparation-throw :after-mint :source-reproof :reuse-orphan))
                 (assert (not callback-called)))
               (when cold
                 (assert (logical-install-handoff-test-unopened-p))
                 (assert (= before (logical-install-stage-test-fd-count))))
               (when saved
                 (logical-install-handoff-test-denied
                  (lambda () (apply #'logical-slot-copy-install-handoff-image saved)) "scope ended"))
               (error "LOGICAL-HANDOFF-EXPECTED-FAILURE ~s: ~a" mode condition)))
        (setf (symbol-function 'logical-slot-copy-check-committed-source) source
              (symbol-function 'postmaster-check-ownership) postmaster
              (symbol-function 'sb-posix:close) close
              (symbol-function 'logical-slot-copy-install-stage-id) stage-id)))
    (assert callback-called)
    (assert (not *physical-recovery-failed-p*))
    (when cold
      (assert (logical-install-handoff-test-unopened-p))
      (assert (= before (logical-install-stage-test-fd-count))))
    (logical-install-handoff-test-denied
     (lambda () (apply #'logical-slot-copy-install-handoff-image saved)) "scope ended")
    (format t "~&LOGICAL-HANDOFF-PASS ~s~%" mode)
    (finish-output)))

(defun logical-install-handoff-test-cold (mode)
  (assert (logical-install-handoff-test-unopened-p))
  (case mode
    (:root-normal (physical-directory-root-test-close :normal))
    (:root-error (physical-directory-root-test-close :error))
    (:root-throw (physical-directory-root-test-close :throw))
    (t (logical-install-handoff-test-run mode)))
  (assert (logical-install-handoff-test-unopened-p)))

(defun logical-install-handoff-test-start (mode)
  (when (and *logical-install-handoff-test-thread* (sb-thread:thread-alive-p *logical-install-handoff-test-thread*))
    (error "handoff test worker is already running"))
  (setf *logical-install-handoff-test-result* :running
        *logical-install-handoff-test-thread*
        (sb-thread:make-thread
         (lambda ()
           (handler-case
               (progn (logical-install-handoff-test-run mode)
                      (setf *logical-install-handoff-test-result* (list :passed mode)))
             (error (condition)
               (setf *logical-install-handoff-test-result* (list :error (princ-to-string condition))))))
         :name "owned live handoff test"))
  :started)
