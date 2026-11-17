(in-package #:pg)

(declaim (ftype function logical-slot-continuation-cold-cleanup-next-ledger))

(defun unit-cold-cleanup-ledger ()
  (labels ((facts (inode directory links size)
             (list 108 inode (if directory #o40700 #o100600) links 1000 size 10 10 636))
           (row (name inode)
             (list :name name :base name :staging nil :directory (facts inode t 2 40)
                   :files (list (list "state" (facts (+ inode 1) nil 1 2)
                                      (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(10 20)))
                                (list "state.tmp" (facts (+ inode 2) nil 1 1)
                                      (make-array 1 :element-type '(unsigned-byte 8) :initial-element 30))))))
    (values (facts 100 t 4 80) (list (row "ordinary" 200) (row "temporary" 300)))))

(defun unit-cold-cleanup-touch-directory (facts)
  (let ((copy (copy-list facts)))
    (incf (sixth copy)) (incf (seventh copy)) (incf (eighth copy))
    copy))

(cl-spec:defproperty continuation-cleanup-effect-keeps-unaffected-resources
    ((replacement (range integer 1000 1000000)))
  (:about logical-slot-continuation-cold-cleanup-next-ledger)
  (:trials (:normal 40))
  (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
    (let* ((observed (logical-slot-continuation-copy rows))
           (ordinary (first observed)) (temporary (second observed))
           (changed-root (unit-cold-cleanup-touch-directory root)))
      (setf (getf temporary :name) "temporary.tmp" (getf temporary :staging) t
            (getf temporary :directory)
            (unit-cold-cleanup-touch-directory (getf temporary :directory)))
      (setf (second (second (first (getf ordinary :files)))) replacement)
      (handler-case
          (progn
            (logical-slot-continuation-cold-cleanup-next-ledger
             root rows '(:rename "temporary" "temporary.tmp") changed-root observed)
            nil)
        (error () t)))))

(rove:deftest continuation-cleanup-effect-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cleanup-effect-keeps-unaffected-resources
                                         :seed 11917 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 40 (getf result :trials)))))

(rove:deftest continuation-cleanup-effect-rename-and-partial-tombstones
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (let* ((observed (logical-slot-continuation-copy rows))
             (temporary (second observed))
             (changed-root (unit-cold-cleanup-touch-directory root)))
        (setf (getf temporary :name) "temporary.tmp" (getf temporary :staging) t
              (getf temporary :directory)
              (unit-cold-cleanup-touch-directory (getf temporary :directory)))
        (multiple-value-bind (next-root next-rows)
            (logical-slot-continuation-cold-cleanup-next-ledger
             root rows '(:rename "temporary" "temporary.tmp") changed-root observed)
          (rove:ok (equalp changed-root next-root))
          (rove:ok (equalp observed next-rows))
          (rove:ok (equal "temporary" (getf (second rows) :name)))
          (dolist (file '("state.tmp" "state"))
            (setf observed (logical-slot-continuation-copy next-rows)
                  temporary (second observed)
                  (getf temporary :files)
                  (remove file (getf temporary :files) :key #'first :test #'equal)
                  (getf temporary :directory)
                  (unit-cold-cleanup-touch-directory (getf temporary :directory)))
            (multiple-value-setq (next-root next-rows)
              (logical-slot-continuation-cold-cleanup-next-ledger
               next-root next-rows (list :unlink "temporary.tmp" file) next-root observed))
            (rove:ok (equalp observed next-rows)))
          (setf changed-root (unit-cold-cleanup-touch-directory next-root))
          (decf (fourth changed-root))
          (multiple-value-bind (final-root final-rows)
              (logical-slot-continuation-cold-cleanup-next-ledger
               next-root next-rows '(:rmdir "temporary.tmp") changed-root (list (first next-rows)))
            (rove:ok (equalp final-root changed-root))
            (rove:ok (equalp final-rows (list (first rows)))))))))

(rove:deftest continuation-cleanup-effect-scratch-preserves-state-and-root
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (let* ((observed (logical-slot-continuation-copy rows)) (ordinary (first observed)))
        (setf (getf ordinary :files) (list (first (getf ordinary :files)))
              (getf ordinary :directory)
              (unit-cold-cleanup-touch-directory (getf ordinary :directory)))
        (multiple-value-bind (next-root next-rows)
            (logical-slot-continuation-cold-cleanup-next-ledger
             root rows '(:unlink "ordinary" "state.tmp") root observed)
          (rove:ok (equal root next-root))
          (rove:ok (equalp (first (getf (first rows) :files))
                           (first (getf (first next-rows) :files))))
          (setf (aref (third (first (getf (first next-rows) :files))) 0) 99)
          (rove:ok (= 10 (aref (third (first (getf (first rows) :files))) 0)))))))

(rove:deftest continuation-cleanup-effect-refuses-unplanned-deltas
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (dolist (event '((:rename "temporary" "other.tmp")
                       (:rename "temporary" "ordinary")
                       (:unlink "ordinary" "state")
                       (:unlink "ordinary" "missing")
                       (:rmdir "temporary")
                       (:rmdir "missing")
                       (:observe :extra)))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-cold-cleanup-next-ledger
                             root rows event root rows) nil)
                   (error () t))))
      (rove:ok
       (handler-case
           (progn (logical-slot-continuation-cold-cleanup-next-ledger
                   root rows '(:observe) (unit-cold-cleanup-touch-directory root) rows) nil)
         (error () t)))))

(rove:deftest continuation-cleanup-effect-accepts-inexact-parent-link-count
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (let* ((stage (logical-slot-continuation-copy (second rows)))
             (changed-root nil))
        (setf (fourth root) 1
              (getf stage :name) "temporary.tmp"
              (getf stage :staging) t
              (getf stage :files) nil
              changed-root (unit-cold-cleanup-touch-directory root))
        (multiple-value-bind (next-root next-rows)
            (logical-slot-continuation-cold-cleanup-next-ledger
             root (list (first rows) stage) '(:rmdir "temporary.tmp")
             changed-root (list (first rows)))
          (rove:ok (= 1 (fourth next-root)))
          (rove:ok (equalp (list (first rows)) next-rows))))))

(rove:deftest continuation-cleanup-effect-orders-durability-and-owned-closes
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (declare (ignore root))
      (let* ((row (second rows))
             (steps (logical-slot-continuation-cold-cleanup-action-steps
                     (logical-slot-continuation-cold-plan-action :retire-temporary row)))
             (rename (position :rename steps :key #'first))
             (first-unlink (position :unlink steps :key #'first))
             (remove-directory (position :rmdir steps :key #'first)))
        (rove:ok (< rename first-unlink remove-directory))
        (rove:ok (equal '(:sync-directory :sync-root)
                        (mapcar #'first (subseq steps (1+ rename) (1- first-unlink)))))
        (rove:ok (eq :close-directory (first (car (last steps)))))
        (rove:ok (equal '(:sync-root :close-directory)
                        (mapcar #'first (nthcdr (1+ remove-directory) steps)))))
      (let* ((row (first rows))
             (steps (logical-slot-continuation-cold-cleanup-action-steps
                     (logical-slot-continuation-cold-plan-action
                      :remove-scratch row (first (getf row :files))))))
        (rove:ok (equal '((:unlink "ordinary" "state.tmp"))
                        (remove-if-not (lambda (step) (eq :unlink (first step))) steps)))
        (rove:ok (notany (lambda (step) (member (first step) '(:rename :rmdir))) steps))
        (rove:ok (equal '(:sync-directory :close-directory)
                        (mapcar #'first (last steps 2)))))))

(rove:deftest continuation-cleanup-effect-staging-schedule-covers-every-tombstone-shape
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (declare (ignore root))
      (dolist (files '(nil ("state") ("state.tmp") ("state" "state.tmp")))
        (let* ((row (logical-slot-continuation-copy (second rows)))
               (captured (getf row :files)))
          (setf (getf row :name) "temporary.tmp" (getf row :staging) t
                (getf row :files)
                (remove-if-not (lambda (file) (member (first file) files :test #'equal)) captured))
          (let* ((steps (logical-slot-continuation-cold-cleanup-action-steps
                         (logical-slot-continuation-cold-plan-action :remove-staging row)))
                 (operations (mapcar #'first steps)))
            (rove:ok (not (member :rename operations)))
            (rove:ok (= (length files) (count :unlink operations)))
            (rove:ok (= (length files) (count :close-file operations)))
            (rove:ok (equal '(:sync-directory :rmdir :sync-root :close-directory)
                            (last operations 4))))))))

(rove:deftest continuation-cleanup-effect-checks-unlinked-descriptor-facts
    (multiple-value-bind (root rows) (unit-cold-cleanup-ledger)
      (declare (ignore root))
      (let* ((before (second (first (getf (first rows) :files))))
             (after (copy-list before)))
        (setf (fourth after) 0)
        (incf (eighth after))
        (rove:ok (logical-slot-continuation-cold-cleanup-check-unlinked-facts before after nil))
        (dolist (index '(0 1 2 3 4 5 6 8))
          (let ((changed (copy-list after)))
            (incf (nth index changed))
            (rove:ok
             (handler-case
                 (progn
                   (logical-slot-continuation-cold-cleanup-check-unlinked-facts before changed nil)
                   nil)
               (error () t))))))))

(rove:deftest continuation-cleanup-effect-checks-real-ramdisk-transitions
    (unit-continuation-inventory-fixture
     (list (list "temporary" (unit-continuation-cleanup-physical "temporary" t))
           (list "ordinary" (unit-continuation-cleanup-physical "ordinary" nil)))
     (lambda (directory check)
       (unit-continuation-cleanup-write-file directory "temporary/state.tmp" #(1 2))
       (let ((root (physical-image-file-facts directory))
             (rows (logical-slot-continuation-cold-read-raw-inventory directory check)))
         (rove:ok
          (handler-case
              (progn (slot-state-rename-no-replace directory "temporary" directory "ordinary") nil)
            (sb-posix:syscall-error () t)))
         (rove:ok (equalp rows (logical-slot-continuation-cold-read-raw-inventory directory check)))
         (labels ((accept (event)
                    (multiple-value-setq (root rows)
                      (logical-slot-continuation-cold-cleanup-next-ledger
                       root rows event (physical-image-file-facts directory)
                       (logical-slot-continuation-cold-read-raw-inventory directory check)))))
           (slot-state-rename-no-replace directory "temporary" directory "temporary.tmp")
           (accept '(:rename "temporary" "temporary.tmp"))
           (let ((child (physical-image-openat
                         directory "temporary.tmp"
                         (logior sb-posix:o-rdonly sb-posix:o-directory
                                 sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
             (assert (not (minusp child)))
             (unwind-protect
                  (progn
                    (dolist (file '("state.tmp" "state"))
                      (let* ((fd (physical-image-openat
                                  child file (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                                     +postmaster-o-cloexec+) 0))
                             (before (physical-image-file-facts fd))
                             (bytes (logical-slot-copy-import-control-read-fd fd +slot-state-maximum-bytes+)))
                        (unwind-protect
                             (progn
                               (slot-state-unlink-exact child file)
                               (accept (list :unlink "temporary.tmp" file))
                               (rove:ok
                                (logical-slot-continuation-cold-cleanup-check-unlinked-facts
                                 before (physical-image-file-facts fd) nil))
                               (rove:ok
                                (equalp bytes (logical-slot-copy-import-control-read-fd
                                               fd +slot-state-maximum-bytes+))))
                          (sb-posix:close fd))))
                    (rove:ok
                     (handler-case (progn (slot-state-unlink-exact child "state") nil)
                       (sb-posix:syscall-error () t)))
                    (let ((before (physical-image-file-facts child)))
                      (slot-state-unlink-exact directory "temporary.tmp" t)
                      (accept '(:rmdir "temporary.tmp"))
                      (rove:ok
                       (logical-slot-continuation-cold-cleanup-check-unlinked-facts
                        before (physical-image-file-facts child) t))))
               (sb-posix:close child))))
         (rove:ok (equal '("ordinary") (mapcar (lambda (row) (getf row :name)) rows)))))))

(defun unit-cold-cleanup-open-fds ()
  (sort (directory-entry-names #P"/proc/self/fd/" :follow-symlinks t
                                                  :maximum-entries 4096) #'string<))

(defun unit-cold-cleanup-effect-fixture (function)
  (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
    (unit-continuation-inventory-fixture
     (append children
             (list (list "temporary" (unit-continuation-cleanup-physical "temporary" t))
                   (list "ordinary" (unit-continuation-cleanup-physical "ordinary" nil))))
     (lambda (directory check)
       (unit-continuation-cleanup-write-file directory "temporary/state.tmp" #(1 2))
       (unit-continuation-cleanup-write-file directory "ordinary/state.tmp" #(3))
       (sb-posix:mkdir
        (namestring (merge-pathnames "temporary.tmp/" (physical-image-fd-path directory))) #o700)
       (unit-continuation-cleanup-write-file directory "temporary.tmp/state.tmp" #(4))
       (funcall function directory check
                (unit-continuation-cleanup-plan directory certificate host)
                certificate host)))))

(rove:deftest continuation-cleanup-effect-executor-preserves-retained-inodes-and-repeats
    (unit-cold-cleanup-effect-fixture
     (lambda (directory check plan certificate host)
       (let* ((registry *wal-replication-slots*)
              (before (logical-slot-continuation-copy (getf plan :ledger)))
              (retained (getf plan :retained)))
         (multiple-value-bind (root rows)
             (logical-slot-continuation-cold-execute-cleanup-plan directory check plan)
           (rove:ok (equal root (physical-image-file-facts directory)))
           (rove:ok (equal '("alpha" "beta" "ordinary")
                           (mapcar (lambda (row) (getf row :name)) rows)))
           (dolist (row rows)
             (let* ((name (getf row :name))
                    (old (find name before :key (lambda (entry) (getf entry :name)) :test #'equal))
                    (state (assoc "state" (getf row :files) :test #'equal)))
               (rove:ok (equalp state (assoc "state" (getf old :files) :test #'equal))))
             (rove:ok (equal '("state") (mapcar #'first (getf row :files)))))
           (rove:ok (equalp retained (mapcar (lambda (row)
                                               (list (getf row :name) (third (first (getf row :files)))))
                                             rows)))
           (rove:ok (eq registry *wal-replication-slots*))
           (let ((again (unit-continuation-cleanup-plan directory certificate host)))
             (rove:ok (null (getf again :actions)))
             (multiple-value-bind (second-root second-rows)
                 (logical-slot-continuation-cold-execute-cleanup-plan directory check again)
               (rove:ok (equal root second-root))
               (rove:ok (equalp rows second-rows)))))))))

(rove:deftest continuation-cleanup-effect-executor-rejects-unrelated-substitution-before-effect
    (unit-cold-cleanup-effect-fixture
     (lambda (directory check plan certificate host)
       (declare (ignore certificate host))
       (let ((fired nil) (message nil)
                         (before (logical-slot-continuation-copy (getf plan :ledger))))
         (let ((*logical-slot-continuation-cold-cleanup-test-hook*
                 (lambda (phase step)
                   (when (and (eq phase :before) (eq (first step) :open-directory) (not fired))
                     (setf fired t)
                     (with-open-file (stream (merge-pathnames "ordinary/state" (physical-image-fd-path directory))
                                             :direction :io :if-exists :overwrite
                                             :element-type '(unsigned-byte 8))
                       (write-byte 99 stream))))))
           (handler-case
               (logical-slot-continuation-cold-execute-cleanup-plan directory check plan)
             (error (condition) (setf message (princ-to-string condition)))))
         (rove:ok fired)
         (rove:ok (and message (search "cold cleanup" message)))
         (let ((after (logical-slot-continuation-cold-read-raw-inventory directory check)))
           (dolist (name '("alpha" "beta" "temporary" "temporary.tmp"))
             (rove:ok
              (equalp (find name before :key (lambda (row) (getf row :name)) :test #'equal)
                      (find name after :key (lambda (row) (getf row :name)) :test #'equal)))))))))

(rove:deftest continuation-cleanup-effect-executor-every-step-exit-replans-partial-tombstones
    (let ((cuts 0) (shapes nil))
      (unit-cold-cleanup-effect-fixture
       (lambda (directory check plan certificate host)
         (declare (ignore directory check certificate host))
         (setf cuts (* 2 (loop for action in (getf plan :actions)
                               sum (length (logical-slot-continuation-cold-cleanup-action-steps action)))))))
      (dotimes (cut cuts)
        (unit-cold-cleanup-effect-fixture
         (lambda (directory check plan certificate host)
           (let ((events 0) (fired nil) (message nil))
             (let ((*logical-slot-continuation-cold-cleanup-test-hook*
                     (lambda (phase step)
                       (declare (ignore phase step))
                       (when (= cut events)
                         (setf fired t)
                         (error "injected cold cleanup step exit"))
                       (incf events))))
               (handler-case
                   (logical-slot-continuation-cold-execute-cleanup-plan directory check plan)
                 (error (condition) (setf message (princ-to-string condition)))))
             (rove:ok (and fired message (search "injected cold cleanup step exit" message)))
             (let* ((raw (logical-slot-continuation-cold-read-raw-inventory directory check))
                    (stage (find "temporary.tmp" raw
                                 :key (lambda (row) (getf row :name)) :test #'equal)))
               (when stage
                 (pushnew (mapcar #'first (getf stage :files)) shapes :test #'equal)))
             (let ((fresh (unit-continuation-cleanup-plan directory certificate host)))
               (logical-slot-continuation-cold-execute-cleanup-plan directory check fresh)
               (let ((again (unit-continuation-cleanup-plan directory certificate host)))
                 (rove:ok (null (getf again :actions)))
                 (logical-slot-continuation-cold-execute-cleanup-plan directory check again)))))))
      (rove:ok (plusp cuts))
      (rove:ok (every (lambda (shape) (member shape shapes :test #'equal))
                      '(nil ("state") ("state.tmp") ("state" "state.tmp"))))))

(rove:deftest continuation-cleanup-effect-executor-closes-owned-inode-after-mode-change
    (dolist (purpose '(:open-directory :open-file))
      (unit-cold-cleanup-effect-fixture
       (lambda (directory check plan certificate host)
         (declare (ignore certificate host))
         (let ((fds (unit-cold-cleanup-open-fds)) (fired nil) (message nil)
                                                  (path nil) (mode nil))
           (let ((*logical-slot-continuation-cold-cleanup-test-hook*
                   (lambda (phase step)
                     (when (and (eq phase :after) (eq (first step) purpose) (not fired))
                       (setf path (merge-pathnames
                                   (if (eq purpose :open-directory)
                                       (concatenate 'string (second step) "/")
                                       (format nil "~a/~a" (second step) (third step)))
                                   (physical-image-fd-path directory))
                             mode (if (eq purpose :open-directory) #o700 #o600)
                             fired t)
                       (sb-posix:chmod (namestring path)
                                       (if (eq purpose :open-directory) #o750 #o640))))))
             (unwind-protect
                  (handler-case
                      (logical-slot-continuation-cold-execute-cleanup-plan directory check plan)
                    (error (condition) (setf message (princ-to-string condition))))
               (when fired (sb-posix:chmod (namestring path) mode))))
           (rove:ok fired)
           (rove:ok message)
           (rove:ok (equal fds (unit-cold-cleanup-open-fds))))))))

(rove:deftest continuation-cleanup-effect-executor-closes-actually-opened-substitute
    (unit-cold-cleanup-effect-fixture
     (lambda (directory check plan certificate host)
       (declare (ignore certificate host))
       (let ((fds (unit-cold-cleanup-open-fds)) (fired nil) (message nil)
                                                (original (symbol-function 'logical-slot-continuation-cold-cleanup-open-input)))
         (unwind-protect
              (progn
                (setf (symbol-function 'logical-slot-continuation-cold-cleanup-open-input)
                      (lambda (parent name directory-p)
                        (when (and directory-p (equal name "temporary.tmp") (not fired))
                          (assert (zerop (port-renameat parent name parent "saved.tmp")))
                          (sb-posix:mkdir
                           (namestring (merge-pathnames "temporary.tmp/" (physical-image-fd-path parent)))
                           #o700)
                          (setf fired t))
                        (funcall original parent name directory-p)))
                (handler-case
                    (logical-slot-continuation-cold-execute-cleanup-plan directory check plan)
                  (error (condition) (setf message (princ-to-string condition)))))
           (setf (symbol-function 'logical-slot-continuation-cold-cleanup-open-input) original))
         (rove:ok fired)
         (rove:ok message)
         (rove:ok (equal fds (unit-cold-cleanup-open-fds)))
         (when fired
           (slot-state-unlink-exact directory "temporary.tmp" t)
           (assert (zerop (port-renameat directory "saved.tmp" directory "temporary.tmp"))))))))

(rove:deftest continuation-cleanup-effect-executor-records-rename-before-queued-interrupt
    (unit-cold-cleanup-effect-fixture
     (lambda (directory check plan certificate host)
       (let* ((owner sb-thread:*current-thread*) (request (sb-thread:make-semaphore :count 0))
                                                 (queued nil) (caught nil) (captured nil) (pending nil)
                                                 (fds (unit-cold-cleanup-open-fds))
                                                 (deadline (+ (get-internal-real-time) (* 5 internal-time-units-per-second)))
                                                 (rename (symbol-function 'slot-state-rename-no-replace))
                                                 (mutate (symbol-function 'logical-slot-continuation-cold-cleanup-mutate-step))
                                                 (worker
                                                   (sb-thread:make-thread
                                                    (lambda ()
                                                      (when (sb-thread:wait-on-semaphore request :timeout 5)
                                                        (sb-thread:interrupt-thread
                                                         owner
                                                         (lambda ()
                                                           (setf pending (logical-slot-continuation-copy
                                                                          (logical-slot-continuation-cold-cleanup-execution-pending captured)))
                                                           (error 'unit-cold-cleanup-entry-interrupt)))
                                                        (setf queued t)
                                                        (sb-thread:barrier (:write)))))))
         (unwind-protect
              (progn
                (setf (symbol-function 'logical-slot-continuation-cold-cleanup-mutate-step)
                      (lambda (state step)
                        (when (eq :rename (first step)) (setf captured state))
                        (funcall mutate state step))
                      (symbol-function 'slot-state-rename-no-replace)
                      (lambda (&rest arguments)
                        (apply rename arguments)
                        (sb-thread:signal-semaphore request)
                        (loop until (progn (sb-thread:barrier (:read)) queued)
                              do (when (> (get-internal-real-time) deadline)
                                   (error "mutation interrupt queue deadline expired")))))
                (handler-case
                    (logical-slot-continuation-cold-execute-cleanup-plan directory check plan)
                  (unit-cold-cleanup-entry-interrupt () (setf caught t))))
           (setf (symbol-function 'slot-state-rename-no-replace) rename
                 (symbol-function 'logical-slot-continuation-cold-cleanup-mutate-step) mutate)
           (sb-thread:join-thread worker))
         (rove:ok caught)
         (rove:ok (equal '(:rename "temporary" "temporary.tmp") pending))
         (rove:ok (logical-slot-continuation-cold-cleanup-execution-failed captured))
         (rove:ok (not (logical-slot-continuation-cold-cleanup-execution-active captured)))
         (rove:ok (equal fds (unit-cold-cleanup-open-fds)))
         (logical-slot-continuation-cold-execute-cleanup-plan
          directory check (unit-continuation-cleanup-plan directory certificate host))
         (rove:ok (null (getf (unit-continuation-cleanup-plan directory certificate host) :actions)))))))
