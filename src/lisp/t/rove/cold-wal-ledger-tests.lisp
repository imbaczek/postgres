(in-package #:pg)

(defun cold-ledger-test-state (fork)
  (list :phase 3 :applied fork :timeline 1 :promotion-child 2
        :next-xid 50 :state-time 1793030000
        :wal-namespace :timeline :promotion-source-namespace :timeline
        :source-reservation (list :version 1 :through 64 :bound 1002 :complete t)))

(defun call-with-cold-ledger-test (fork salt function)
  (call-with-unit-promotion-directory
   (lambda (context root)
     (let* ((state (cold-ledger-test-state fork))
            (plan (physical-promotion-checkpoint-plan state 20000 1003))
            (prefix (make-array (mod fork +wal-segment-size+) :element-type '(unsigned-byte 8)
                                                              :initial-element salt))
            (files (physical-promotion-checkpoint-files plan prefix)))
       (funcall function context root state plan prefix files)))))

(cl-spec:defproperty cold-wal-ledger-witnesses-staging-and-history-retry
    ((fork (range integer 0 128)) (salt (range integer 0 255)))
  (:about call-with-logical-slot-copy-cold-wal-ledger)
  (:trials (:normal 50))
  (call-with-cold-ledger-test
   fork salt
   (lambda (context root state plan prefix files)
     (declare (ignore root))
     (let ((parent (wal-segment-path 0 1 :timeline))
           (orphan (wal-segment-path 1 2 :timeline))
           (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))
           (saved nil) (two-links nil))
       (cold-scratch-test-write parent #(1 2 3))
       (cold-scratch-test-write orphan #(9 9 9))
       (call-with-logical-slot-copy-cold-wal-ledger
        context state plan prefix (lambda () nil)
        (lambda (observer verify)
          (setf saved verify)
          (logical-slot-copy-cold-wal-stage-checkpoint
           state plan prefix files nil verify (lambda () nil)
           :context context :install-prefix t
           :observer (lambda (event operation name fd &optional destination)
                       (when (and (eq operation :history) (eq event :installed))
                         (setf two-links (= 2 (fourth (physical-image-file-facts fd)))))
                       (funcall observer event operation name fd destination))))
        :history-p nil :install-prefix t :maximum 64)
       (and two-links
            (handler-case (progn (funcall saved :staged) nil) (error () t))
            (equalp #(1 2 3) (read-bounded-regular-file parent 64))
            (not (probe-file orphan))
            (progn
              (call-with-logical-slot-copy-cold-wal-ledger
               context state plan prefix (lambda () nil)
               (lambda (observer verify)
                 (logical-slot-copy-cold-wal-stage-checkpoint
                  state plan prefix files t verify (lambda () nil)
                  :context context :install-prefix t :observer observer))
               :history-p t :install-prefix t :maximum 64)
              t)
            (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))

(rove:deftest cold-wal-ledger-generated-staging-and-retry
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'cold-wal-ledger-witnesses-staging-and-history-retry :seed 61913))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest cold-wal-ledger-preserves-a-later-torn-frame-until-its-write
    (call-with-cold-ledger-test
     64 17
     (lambda (context root state plan prefix files)
       (declare (ignore root))
       (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
              (last-frame (car (last (getf plan :frames))))
              (path (wal-segment-path (floor (first last-frame) +wal-segment-size+) 2 :timeline))
              (offset (mod (first last-frame) +wal-segment-size+))
              (corrupt (copy-seq (second (assoc (floor (first last-frame) +wal-segment-size+) files))))
              (observed nil))
         (dolist (entry files)
           (cold-scratch-test-write (wal-segment-path (first entry) 2 :timeline) (second entry)))
         (timeline-publish-history-bytes (timeline-history-path 2 *pgdata*)
                                         (physical-promotion-history-image state) :directory-fd directory)
         (setf (aref corrupt offset) (logxor 255 (aref corrupt offset)))
         (cold-scratch-test-write path corrupt)
         (call-with-logical-slot-copy-cold-wal-ledger
          context state plan prefix (lambda () nil)
          (lambda (observer verify)
            (logical-slot-copy-cold-wal-stage-checkpoint
             state plan prefix files t verify (lambda () nil)
             :context context :install-prefix t
             :observer (lambda (event operation name fd &optional destination)
                         (funcall observer event operation name fd destination)
                         (when (and (not observed) (eq operation :frame) (eq event :written))
                           (setf observed (equalp corrupt (read-bounded-regular-file path +wal-segment-size+)))))))
          :history-p t :install-prefix t :maximum 64)
         (rove:ok observed)
         (rove:ok (equalp (second (assoc (floor (first last-frame) +wal-segment-size+) files))
                          (read-bounded-regular-file path +wal-segment-size+)))))))

(rove:deftest cold-wal-ledger-rejects-wrong-event-and-install-destination
    (dolist (fault '(:event :destination))
      (call-with-cold-ledger-test
       64 17
       (lambda (context root state plan prefix files)
         (declare (ignore root))
         (let ((fired nil)
               (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-copy-cold-wal-ledger
                   context state plan prefix (lambda () nil)
                   (lambda (observer verify)
                     (logical-slot-copy-cold-wal-stage-checkpoint
                      state plan prefix files nil verify (lambda () nil)
                      :context context :install-prefix t
                      :observer
                      (lambda (event operation name fd &optional destination)
                        (when (and (not fired) (eq operation :prefix)
                                   (eq event (if (eq fault :event) :created :before-install)))
                          (setf fired t)
                          (if (eq fault :event) (setf event :installed)
                              (setf destination (wal-segment-path 3 2 :timeline))))
                        (funcall observer event operation name fd destination))))
                   :history-p nil :install-prefix t :maximum 64)
                  nil)
              (error () t)))
           (rove:ok fired)
           (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))))
         ))))

(rove:deftest cold-wal-ledger-rejects-prefix-substitution-after-original-close
    (call-with-cold-ledger-test
     64 17
     (lambda (context root state plan prefix files)
       (let ((scratch nil) (borrowed nil) (fired nil) (closed nil)
                           (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
         (rove:ok
          (handler-case
              (let ((*wal-timeline-test-hook*
                      (lambda (point)
                        (when (and scratch (not fired) (eq point :after-prefix-directory-fsync))
                          (setf fired t
                                closed (handler-case (progn (sb-posix:fstat borrowed) nil) (error () t)))
                          (let ((replacement (merge-pathnames "replacement" root)))
                            (cold-scratch-test-write replacement prefix)
                            (sb-posix:rename (namestring replacement) (namestring scratch)))))))
                (call-with-logical-slot-copy-cold-wal-ledger
                 context state plan prefix (lambda () nil)
                 (lambda (observer verify)
                   (logical-slot-copy-cold-wal-stage-checkpoint
                    state plan prefix files nil verify (lambda () nil)
                    :context context :install-prefix t
                    :observer (lambda (event kind path fd &optional destination)
                                (when (and (eq event :created) (eq kind :prefix))
                                  (setf scratch path borrowed fd))
                                (funcall observer event kind path fd destination))))
                 :history-p nil :install-prefix t :maximum 64)
                nil)
            (error () t)))
         (rove:ok fired)
         (rove:ok closed)
         (rove:ok (not (probe-file (wal-segment-path 0 2 :timeline))))
         (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))

(rove:deftest cold-wal-ledger-binding-failure-cannot-be-cleared
    (call-with-cold-ledger-test
     64 17
     (lambda (context root state plan prefix files)
       (declare (ignore root files))
       (let ((original (getf plan :end)) (first-failed nil) (second-failed nil)
                                         (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
         (rove:ok
          (handler-case
              (progn
                (call-with-logical-slot-copy-cold-wal-ledger
                 context state plan prefix (lambda () nil)
                 (lambda (observer verify)
                   (declare (ignore observer))
                   (unwind-protect
                        (progn
                          (incf (getf plan :end))
                          (setf first-failed (handler-case (progn (funcall verify nil) nil) (error () t))))
                     (setf (getf plan :end) original))
                   (setf second-failed (handler-case (progn (funcall verify nil) nil) (error () t))))
                 :history-p nil :install-prefix t :maximum 64)
                nil)
            (error () t)))
         (rove:ok first-failed)
         (rove:ok second-failed)
         (rove:ok (null (directory-entry-names (wal-directory) :follow-symlinks t)))
         (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))

(rove:deftest cold-wal-ledger-binds-rollover-prefix-destinations-in-order
    (call-with-cold-ledger-test
     (- +wal-segment-size+ 16) 17
     (lambda (context root state plan prefix files)
       (declare (ignore root))
       (let ((destinations nil)
             (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
         (rove:ok (> (length files) 1))
         (call-with-logical-slot-copy-cold-wal-ledger
          context state plan prefix (lambda () nil)
          (lambda (observer verify)
            (logical-slot-copy-cold-wal-stage-checkpoint
             state plan prefix files nil verify (lambda () nil)
             :context context :install-prefix t
             :observer (lambda (event kind path fd &optional destination)
                         (funcall observer event kind path fd destination)
                         (when (and (eq event :installed) (eq kind :prefix))
                           (push (file-namestring destination) destinations)))))
          :history-p nil :install-prefix t :maximum 64)
         (rove:ok
          (equal (reverse destinations)
                 (mapcar (lambda (entry) (wal-segment-filename (first entry) 2 :timeline)) files)))
         (call-with-logical-slot-copy-cold-wal-ledger
          context state plan prefix (lambda () nil)
          (lambda (observer verify)
            (logical-slot-copy-cold-wal-stage-checkpoint
             state plan prefix files t verify (lambda () nil)
             :context context :install-prefix nil :observer observer))
          :history-p t :install-prefix nil :maximum 64)
         (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))

(rove:deftest cold-wal-ledger-drains-all-owned-witnesses-on-close-error
    (dolist (cut '(:close-error :primary-error))
      (call-with-cold-ledger-test
       64 17
       (lambda (context root state plan prefix files)
         (declare (ignore root))
         (let* ((original (symbol-function 'sb-posix:close))
                (marker (make-condition 'simple-error :format-control "ledger close cut"))
                (primary (make-condition 'simple-error :format-control "ledger body cut"))
                (before (directory-entry-names "/proc/self/fd/" :follow-symlinks t))
                (owned nil) (fired nil) (armed nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'sb-posix:close)
                        (lambda (fd)
                          (prog1 (funcall original fd)
                            (when (and armed (not fired) (member fd owned))
                              (setf fired t)
                              (error marker)))))
                  (let ((outcome
                          (handler-case
                              (progn
                                (call-with-logical-slot-copy-cold-wal-ledger
                                 context state plan prefix (lambda () nil)
                                 (lambda (observer verify)
                                   (logical-slot-copy-cold-wal-stage-checkpoint
                                    state plan prefix files nil verify (lambda () nil)
                                    :context context :install-prefix t :observer observer)
                                   (setf owned
                                         (loop for name in (set-difference
                                                            (directory-entry-names "/proc/self/fd/" :follow-symlinks t)
                                                            before :test #'equal)
                                               for fd = (parse-integer name)
                                               when (handler-case (progn (sb-posix:fstat fd) t) (error () nil))
                                                 collect fd))
                                   (rove:ok owned)
                                   (setf armed t)
                                   (when (eq cut :primary-error) (error primary)))
                                 :history-p nil :install-prefix t :maximum 64)
                                :returned)
                            (error (condition) condition))))
                    (rove:ok (eq outcome (if (eq cut :primary-error) primary marker)))))
             (setf (symbol-function 'sb-posix:close) original))
           (rove:ok fired)
           (dolist (fd owned)
             (rove:ok (handler-case (progn (sb-posix:fstat fd) nil) (error () t))))
           (rove:ok (= (length before)
                       (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))))))))

(rove:deftest cold-wal-ledger-does-not-read-or-budget-orphan-payloads
    (call-with-cold-ledger-test
     64 17
     (lambda (context root state plan prefix files)
       (declare (ignore root))
       (let* ((orphan (wal-segment-path 9 2 :timeline))
              (original (symbol-function 'logical-slot-copy-cold-wal-read))
              (read-orphan nil) (reads 0))
         (cold-scratch-test-write orphan #())
         (sb-posix:truncate (namestring orphan) (* 4 +wal-segment-size+))
         (unwind-protect
              (progn
                (setf (symbol-function 'logical-slot-copy-cold-wal-read)
                      (lambda (directory name facts maximum)
                        (incf reads)
                        (when (equal name (file-namestring orphan))
                          (setf read-orphan t)
                          (error "orphan payload must not be read"))
                        (funcall original directory name facts maximum)))
                (call-with-logical-slot-copy-cold-wal-ledger
                 context state plan prefix (lambda () nil)
                 (lambda (observer verify)
                   (logical-slot-copy-cold-wal-stage-checkpoint
                    state plan prefix files nil verify (lambda () nil)
                    :context context :install-prefix t :observer observer))
                 :history-p nil :install-prefix t :maximum 64 :maximum-bytes 4096))
           (setf (symbol-function 'logical-slot-copy-cold-wal-read) original))
         (rove:ok (plusp reads))
         (rove:ng read-orphan)
         (rove:ok (not (probe-file orphan)))))))

(rove:deftest cold-wal-ledger-input-mismatch-precedes-orphan-removal
    (dolist (fault '(:child :files :byte-bound))
      (call-with-cold-ledger-test
       64 17
       (lambda (context root state plan prefix files)
         (declare (ignore root))
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (orphan (wal-segment-path 9 2 :timeline))
                (passed-files (logical-slot-copy-import-state-image files))
                (before-fds (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (cold-scratch-test-write orphan #(9 9 9))
           (when (eq fault :child) (incf (getf plan :child)))
           (when (eq fault :files) (setf (aref (second (first passed-files)) 0) 99))
           (let ((before (physical-image-file-facts directory (file-namestring orphan))))
             (rove:ok
              (handler-case
                  (progn
                    (call-with-logical-slot-copy-cold-wal-ledger
                     context state plan prefix (lambda () nil)
                     (lambda (observer verify)
                       (logical-slot-copy-cold-wal-stage-checkpoint
                        state plan prefix passed-files nil verify (lambda () nil)
                        :context context :install-prefix t :observer observer))
                     :history-p nil :install-prefix t :maximum 64
                     :maximum-bytes (if (eq fault :byte-bound) 1 4096))
                    nil)
                (error () t)))
             (rove:ok (equal before (physical-image-file-facts directory (file-namestring orphan))))
             (rove:ok (= before-fds
                         (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))))

(rove:deftest cold-wal-ledger-callback-cuts-drain-witnesses-and-permit-fresh-scope
    (dolist (boundary '(:guard :prefix-created :history-installed))
      (dolist (exit '(:error :throw :interrupt))
        (call-with-cold-ledger-test
         64 17
         (lambda (context root state plan prefix files)
           (declare (ignore root))
           (let ((marker (make-condition 'simple-error :format-control "cold WAL callback cut"))
                 (armed nil) (fired nil)
                 (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (flet ((inject ()
                      (setf fired t)
                      (ecase exit
                        (:error (error marker))
                        (:throw (throw 'wal-callback :cut))
                        (:interrupt
                         (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                     (lambda () (throw 'wal-callback :cut)))))))
               (let ((outcome
                       (handler-case
                           (catch 'wal-callback
                             (call-with-logical-slot-copy-cold-wal-ledger
                              context state plan prefix
                              (lambda ()
                                (when (and (eq boundary :guard) armed (not fired)) (inject)))
                              (lambda (observer verify)
                                (logical-slot-copy-cold-wal-stage-checkpoint
                                 state plan prefix files nil verify (lambda () nil)
                                 :context context :install-prefix t
                                 :observer
                                 (lambda (event kind path fd &optional destination)
                                   (funcall observer event kind path fd destination)
                                   (when (and (eq kind :prefix) (eq event :written)) (setf armed t))
                                   (when (and (not fired)
                                              (or (and (eq boundary :prefix-created) (eq kind :prefix)
                                                       (eq event :created))
                                                  (and (eq boundary :history-installed) (eq kind :history)
                                                       (eq event :installed))))
                                     (inject)))))
                              :history-p nil :install-prefix t :maximum 64)
                             :returned)
                         (error (condition) condition))))
                 (rove:ok fired)
                 (rove:ok (eq outcome (if (eq exit :error) marker :cut)))))
             (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (let ((history-p (not (null (probe-file (timeline-history-path 2 *pgdata*))))))
               (call-with-logical-slot-copy-cold-wal-ledger
                context state plan prefix (lambda () nil)
                (lambda (observer verify)
                  (logical-slot-copy-cold-wal-stage-checkpoint
                   state plan prefix files history-p verify (lambda () nil)
                   :context context :install-prefix t :observer observer))
                :history-p history-p :install-prefix t :maximum 64))
             (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))))

(rove:deftest cold-wal-ledger-final-snapshot-survives-close-and-rejects-replacement
    (call-with-cold-ledger-test
     64 17
     (lambda (context root state plan prefix files)
       (let ((snapshot nil)
             (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
         (call-with-logical-slot-copy-cold-wal-ledger
          context state plan prefix (lambda () nil)
          (lambda (observer verify)
            (logical-slot-copy-cold-wal-stage-checkpoint
             state plan prefix files nil verify (lambda () nil)
             :context context :install-prefix t :observer observer)
            (setf snapshot (funcall verify :snapshot)))
          :history-p nil :install-prefix t :maximum 64)
         (rove:ok (logical-slot-copy-cold-wal-check-snapshot context snapshot 64))
         (rove:ok (every (lambda (row) (= 3 (length row))) (getf snapshot :rows)))
         (let* ((path (wal-segment-path (first (first files)) 2 :timeline))
                (replacement (merge-pathnames "replacement" root)))
           (cold-scratch-test-write replacement (second (first files)))
           (sb-posix:rename (namestring replacement) (namestring path))
           (rove:ok
            (handler-case (progn (logical-slot-copy-cold-wal-check-snapshot context snapshot 64) nil)
              (error () t))))
         (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))))))
