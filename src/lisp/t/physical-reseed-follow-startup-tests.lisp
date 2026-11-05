(in-package #:pg-physical-tests)

(defun reseed-startup-projection-fixture (offset)
  (let* ((roles '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive))
         (descriptors (loop for role in roles for fd from offset collect (cons role fd)))
         (row (list (copy-seq "follow.active") 1 7 #o100600 1 1000 40 50 60))
         (directories (loop for role in roles for inode from 1
                            collect (cons role (list (copy-seq ".") 1 inode #o40700 2 1000 80 90 100))))
         (image (list (copy-seq "follow.active") (+ offset 7) (copy-seq "follow.active") row)))
    (values descriptors
            (list :prefix 0 :snapshot (list :record (list :generation 1))
                  :install (list :identity (list 1 2 3) :digest 12)
                  :attempt (list :identity (list 4 5 6) :digest 13)
                  :temporary nil :temporary-file nil
                  :pid-file (list :bytes (make-array 2 :element-type '(unsigned-byte 8) :initial-element 1))
                  :layout (list :new (list (copy-list row)) :old nil
                                :images (list image) :active (copy-tree image)
                                :directories directories
                                :names (list :root (list (copy-seq "physical-reseed.follow"))
                                             :saved (list (copy-seq "follow.active"))))))))

(cl-spec:defproperty reseed-startup-projection-ignores-only-pid-boundary-changes
    ((offset (range integer 10 1000)) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-startup-projection)
  (:trials (:normal 100))
  (multiple-value-bind (before-fds before) (reseed-startup-projection-fixture offset)
    (multiple-value-bind (after-fds after) (reseed-startup-projection-fixture (+ offset delta))
      (let* ((layout (getf after :layout))
             (root (cdr (assoc :root (getf layout :directories)))))
        (setf (seventh root) delta (eighth root) (+ delta 1) (ninth root) (+ delta 2)
              (getf (getf layout :names) :root)
              (append (getf (getf layout :names) :root) (list "postmaster.pid"))
              (getf after :pid-file) nil)
        (let ((left (funcall (symbol-function 'pg::physical-reseed-follow-startup-projection) before-fds before))
              (right (funcall (symbol-function 'pg::physical-reseed-follow-startup-projection) after-fds after)))
          (and (equal left right)
               (progn
                 (incf (getf (getf after :install) :digest))
                 (not (equal left
                             (funcall (symbol-function 'pg::physical-reseed-follow-startup-projection)
                                      after-fds after))))))))))

(deftest generated-reseed-startup-projection-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-startup-projection-ignores-only-pid-boundary-changes
                                       :seed 148 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-startup-projection-preserves-every-stable-evidence-group
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
      (let ((baseline (pg::physical-reseed-follow-startup-projection descriptors state)))
        (dolist (change
                 (list
                  (lambda (copy) (incf (getf copy :prefix)))
                  (lambda (copy) (incf (getf (getf (getf copy :snapshot) :record) :generation)))
                  (lambda (copy) (incf (getf (getf copy :install) :digest)))
                  (lambda (copy) (incf (getf (getf copy :attempt) :digest)))
                  (lambda (copy) (setf (getf copy :temporary) (list :phase :retiring)))
                  (lambda (copy) (setf (getf copy :temporary-file) (list :identity (list 1 2))))
                  (lambda (copy) (incf (second (first (getf (getf copy :layout) :new)))))
                  (lambda (copy) (setf (getf (getf copy :layout) :old) (list "saved")))
                  (lambda (copy) (setf (third (first (getf (getf copy :layout) :images))) "Follow.active"))
                  (lambda (copy) (incf (ninth (fourth (getf (getf copy :layout) :active)))))
                  (lambda (copy) (incf (third (cdr (assoc :root (getf (getf copy :layout) :directories))))))
                  (lambda (copy) (incf (eighth (cdr (assoc :saved (getf (getf copy :layout) :directories))))))
                  (lambda (copy) (push "Postmaster.pid" (getf (getf (getf copy :layout) :names) :root)))
                  (lambda (copy) (push "postmaster.pid" (getf (getf (getf copy :layout) :names) :saved)))))
          (let ((copy (pg::physical-adoption-attempt-value-copy state)))
            (funcall change copy)
            (ok (not (equal baseline (pg::physical-reseed-follow-startup-projection descriptors copy))))))
        (let ((copy (pg::physical-reseed-follow-startup-projection descriptors state)))
          (setf (char (first (getf (getf (getf copy :layout) :names) :root)) 0) #\X
                (getf (getf (getf copy :snapshot) :record) :generation) 99)
          (ok (equal baseline (pg::physical-reseed-follow-startup-projection descriptors state)))))))

(deftest reseed-startup-projection-refuses-unknown-or-ambiguous-parent-fds
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
      (ok (handler-case
              (progn (pg::physical-reseed-follow-startup-projection
                      (remove :staged descriptors :key #'car) state) nil)
            (error () t)))
      (ok (handler-case
              (progn (pg::physical-reseed-follow-startup-projection
                      (acons :other (+ 20 7) descriptors) state) nil)
            (error () t)))))

(deftest reseed-startup-root-borrows-the-local-lock-at-both-boundaries
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((phases nil) (escaped nil) (held-root nil))
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (push phase phases)
            (let ((position (file-position lock)))
              (pg::call-with-physical-reseed-follow-startup-root
               lock
               (lambda (fd verify)
                 (setf held-root fd escaped verify)
                 (ok (funcall verify))
                 (ok (= (sb-posix:stat-ino (sb-posix:fstat fd))
                        (sb-posix:stat-ino (sb-posix:stat root))))
                 (ok (open-stream-p lock))
                 (ok (null pg::*postmaster-data-lock*))))
              (ok (eql position (file-position lock)))
              (ok (handler-case (progn (sb-posix:fstat held-root) nil)
                    (sb-posix:syscall-error () t)))
              (ok (handler-case (progn (funcall escaped) nil)
                    (error () t)))))
          :startup-reconcile (lambda () (ok (open-stream-p pg::*postmaster-data-lock*))))
         (ok (equal (reverse phases) '(:before-pid :owned-pid)))))))

(deftest reseed-startup-root-rechecks-published-state-thread-and-held-lock-fd
    (call-with-pid-owner
     (lambda (root)
       (declare (ignore root))
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (pg::postmaster-own-data-directory
        "127.0.0.1" 5439
        :startup-observer
        (lambda (lock phase)
          (declare (ignore phase))
          (pg::call-with-physical-reseed-follow-startup-root
           lock
           (lambda (fd verify)
             (declare (ignore fd))
             (let ((pg::*postmaster-data-lock* lock))
               (ok (handler-case (progn (funcall verify) nil)
                     (error () t))))
             (let ((pg::*postmaster-pid-directory* nil))
               (ok (handler-case (progn (funcall verify) nil)
                     (error () t))))
             (ok (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda ()
                     (handler-case (progn (funcall verify) nil)
                       (error () t))))))
             (let* ((lock-fd (sb-sys:fd-stream-fd lock))
                    (flags (sb-posix:fcntl lock-fd sb-posix:f-getfd))
                    (backup (sb-posix:dup lock-fd)))
               (unwind-protect
                    (progn
                      (sb-posix:close lock-fd)
                      (ok (handler-case (progn (funcall verify) nil)
                            (sb-posix:syscall-error () t))))
                 (sb-posix:dup2 backup lock-fd)
                 (sb-posix:fcntl lock-fd sb-posix:f-setfd flags)
                 (sb-posix:close backup)))
             (ok (funcall verify)))))))))

(deftest reseed-startup-root-closes-only-its-own-fd-on-every-exit
    (call-with-pid-owner
     (lambda (root)
       (declare (ignore root))
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (pg::postmaster-own-data-directory
        "127.0.0.1" 5439
        :startup-observer
        (lambda (lock phase)
          (declare (ignore phase))
          (dolist (mode '(:error :throw :interrupt))
            (let ((root-fd nil) (escaped nil))
              (ok
               (eq :cut
                   (catch 'startup-root-cut
                     (handler-case
                         (pg::call-with-physical-reseed-follow-startup-root
                          lock
                          (lambda (fd verify)
                            (setf root-fd fd escaped verify)
                            (ecase mode
                              (:error (error "startup root cut"))
                              (:throw (throw 'startup-root-cut :cut))
                              (:interrupt
                               (sb-thread:interrupt-thread
                                sb-thread:*current-thread*
                                (lambda () (throw 'startup-root-cut :cut)))))))
                       (error (condition)
                         (if (search "startup root cut" (princ-to-string condition))
                             :cut
                             (error condition)))))))
              (ok (handler-case (progn (sb-posix:fstat root-fd) nil)
                    (sb-posix:syscall-error () t)))
              (ok (handler-case (progn (funcall escaped) nil)
                    (error () t)))
              (ok (open-stream-p lock))
              (ok (sb-posix:fstat (sb-sys:fd-stream-fd lock)))
              (pg::call-with-physical-reseed-follow-startup-root
               lock (lambda (fd verify) (declare (ignore fd)) (ok (funcall verify)))))))))))

(deftest reseed-startup-observation-keeps-the-ordinary-branch-across-pid-creation
    (call-with-pid-owner
     (lambda (root)
       (declare (ignore root))
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((before nil) (after nil))
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (if (eq phase :before-pid)
                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                (setf after (pg::physical-reseed-follow-startup-observation lock phase before)))))
         (ok (equal before '(:branch :ordinary)))
         (ok (equal before after))
         (ok (pg::postmaster-pid-owned-p))))))

(deftest reseed-startup-observation-rejects-a-new-retirement-branch-after-pid-creation
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((before nil) (path (merge-pathnames "physical-reseed.follow" root))
                          (bytes (sb-ext:string-to-octets "foreign retirement" :external-format :utf-8)))
         (ok
          (handler-case
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (progn
                         (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                         (reseed-follow-owner-test-write path bytes))
                       (pg::physical-reseed-follow-startup-observation lock phase before))))
                nil)
            (error (condition)
              (search "startup branch changed" (princ-to-string condition)))))
         (ok (equalp bytes (lineage-test-bytes path)))
         (ok (null pg::*postmaster-data-lock*))
         (ok (null pg::*postmaster-pid-stream*))
         (ok (null pg::*postmaster-start-timestamp*))
         (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))))))

(deftest reseed-startup-observation-proves-retirement-under-real-postmaster-lock
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (reseed-follow-owner-test-write
        (merge-pathnames "physical-reseed.follow.tmp" root)
        (pg::physical-reseed-follow-image (reseed-retirement-test-initial-candidate root-fd snapshot)))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (after nil) (reconciled nil))
         (unwind-protect
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                       (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
                 :startup-reconcile
                 (lambda ()
                   (setf reconciled t)
                   (ok (open-stream-p pg::*postmaster-data-lock*))
                   (ok (pg::postmaster-pid-owned-p))))
                (ok (eq :retirement (getf before :branch)))
                (ok (equal before after))
                (ok reconciled)
                (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(defun check-retirement-startup-owner-context (owner)
  (flet ((refused ()
           (handler-case
               (progn (pg::physical-reseed-follow-retirement-check-startup-owner owner) nil)
             (error () t))))
    (let ((pg::*postmaster-data-lock* nil)) (ok (refused)))
    (let ((pg::*postmaster-pid-stream* nil)) (ok (refused)))
    (let ((pg::*postmaster-pid-inode* nil)) (ok (refused)))
    (let ((pg::*postmaster-pid-directory* nil)) (ok (refused)))
    (let ((pg::*wal-ready* t)) (ok (refused)))
    (ok (sb-thread:join-thread (sb-thread:make-thread #'refused)))
    (let* ((fd (sb-sys:fd-stream-fd pg::*postmaster-data-lock*))
           (flags (sb-posix:fcntl fd sb-posix:f-getfd))
           (backup (sb-posix:dup fd)))
      (unwind-protect
           (progn (sb-posix:close fd) (ok (refused)))
        (sb-posix:dup2 backup fd)
        (sb-posix:fcntl fd sb-posix:f-setfd flags)
        (sb-posix:close backup)))
    (dolist (target '(:held :named))
      (let ((original (symbol-function 'pg::physical-adoption-install-mount-id))
            (pid-fd (sb-sys:fd-stream-fd pg::*postmaster-pid-stream*))
            (pid-path (namestring (pg::postmaster-data-path "postmaster.pid")))
            (visited nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                     (lambda (path &key directory-fd)
                       (let ((mount (funcall original path :directory-fd directory-fd)))
                         (if (ecase target
                               (:held (eql directory-fd pid-fd))
                               (:named (and path (equal pid-path (namestring path)))))
                             (progn (setf visited t) (1+ mount))
                             mount))))
               (ok (refused))
               (ok visited))
          (setf (symbol-function 'pg::physical-adoption-install-mount-id) original))))
    (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner))))

(defun check-retirement-startup-root-lifetimes (owner)
  (dolist (mode '(:normal :error :throw :interrupt))
    (let ((held nil) (escaped nil))
      (ok
       (eq :finished
           (catch 'retirement-root-cut
             (handler-case
                 (pg::call-with-physical-reseed-follow-retirement-startup-root
                  owner
                  (lambda (fd verify)
                    (setf held fd escaped verify)
                    (ok (funcall verify))
                    (ecase mode
                      (:normal :finished)
                      (:error (error "retirement root cut"))
                      (:throw (throw 'retirement-root-cut :finished))
                      (:interrupt
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (throw 'retirement-root-cut :finished)))))))
               (error (condition)
                 (if (search "retirement root cut" (princ-to-string condition))
                     :finished
                     (error condition)))))))
      (ok (handler-case (progn (sb-posix:fstat held) nil)
            (sb-posix:syscall-error () t)))
      (ok (handler-case (progn (funcall escaped) nil)
            (error () t)))
      (ok (open-stream-p pg::*postmaster-data-lock*))
      (ok (open-stream-p pg::*postmaster-pid-stream*))
      (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner)))))

(defun check-retirement-startup-owner-exits (expected)
  (dolist (mode '(:error :throw :interrupt))
    (let ((escaped nil)
          (lock pg::*postmaster-data-lock*)
          (pid-stream pg::*postmaster-pid-stream*)
          (position (file-position pg::*postmaster-pid-stream*)))
      (ok
       (eq :finished
           (catch 'retirement-owner-cut
             (handler-case
                 (pg::call-with-physical-reseed-follow-retirement-startup-owner
                  expected
                  (lambda (owner)
                    (setf escaped owner)
                    (ecase mode
                      (:error (error "retirement owner cut"))
                      (:throw (throw 'retirement-owner-cut :finished))
                      (:interrupt
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (throw 'retirement-owner-cut :finished)))))))
               (error (condition)
                 (if (search "retirement owner cut" (princ-to-string condition))
                     :finished
                     (error condition)))))))
      (ok escaped)
      (ok (handler-case
              (progn (pg::physical-reseed-follow-retirement-check-startup-owner escaped) nil)
            (error () t)))
      (ok (and (eq lock pg::*postmaster-data-lock*) (open-stream-p lock)))
      (ok (and (eq pid-stream pg::*postmaster-pid-stream*) (open-stream-p pid-stream)))
      (ok (eql position (file-position pid-stream))))))

(deftest reseed-retirement-startup-owner-borrows-published-postmaster-ownership
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (reseed-follow-owner-test-write
        (merge-pathnames "physical-reseed.follow.tmp" root)
        (pg::physical-reseed-follow-image (reseed-retirement-test-initial-candidate root-fd snapshot)))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (after nil) (reconciled nil) (escaped nil))
         (unwind-protect
              (progn
                (pg::postmaster-own-data-directory
                 "127.0.0.1" 5439
                 :startup-observer
                 (lambda (lock phase)
                   (if (eq phase :before-pid)
                       (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                       (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
                 :startup-reconcile
                 (lambda ()
                   (setf reconciled t)
                   (ok (open-stream-p pg::*postmaster-data-lock*))
                   (ok (pg::postmaster-pid-owned-p))
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    after
                    (lambda (retirement-owner)
                      (setf escaped retirement-owner)
                      (ok (pg::physical-reseed-follow-retirement-check-startup-owner retirement-owner))
                      (ok (not (pg::physical-adoption-owner-p retirement-owner)))
                      (ok (handler-case
                              (progn (pg::physical-reseed-check-attempt-authority retirement-owner) nil)
                            (error () t)))
                      (check-retirement-startup-owner-context retirement-owner)
                      (check-retirement-startup-root-lifetimes retirement-owner)
                      (ok (handler-case (progn (pg::physical-adoption-check-owner retirement-owner) nil)
                            (error () t)))))
                   (check-retirement-startup-owner-exits after)))
                (ok (eq :retirement (getf before :branch)))
                (ok (equal before after))
                (ok reconciled)
                (ok (handler-case
                        (progn (pg::physical-reseed-follow-retirement-check-startup-owner escaped) nil)
                      (error () t)))
                (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(deftest reseed-retirement-startup-owner-refuses-replaced-handover-evidence
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (after nil) (entered nil) (replacement-inode nil) (observed nil)
                                 (path (merge-pathnames "physical-reseed.follow" root))
                                 (replacement (merge-pathnames "replacement" root))
                                 (bytes (pg::physical-reseed-follow-image (getf snapshot :record))))
         (unwind-protect
              (progn
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
                    (lambda (retirement-owner)
                      (reseed-follow-owner-test-write replacement bytes)
                      (setf replacement-inode (sb-posix:stat-ino (sb-posix:lstat replacement)))
                      (sb-posix:rename replacement path)
                      (ok (handler-case
                              (progn
                                (pg::call-with-physical-reseed-follow-retirement-startup-observation
                                 retirement-owner
                                 (lambda (descriptors state verify)
                                   (declare (ignore descriptors state verify))
                                   (setf observed t)))
                                nil)
                            (error (condition)
                              (search "target follow final generation changed" (princ-to-string condition)))))
                      (ok (not observed))))
                   (let ((lock pg::*postmaster-data-lock*)
                         (pid-stream pg::*postmaster-pid-stream*)
                         (position (file-position pg::*postmaster-pid-stream*)))
                     (ok (handler-case
                             (progn
                               (pg::call-with-physical-reseed-follow-retirement-startup-owner
                                after (lambda (retirement-owner)
                                        (declare (ignore retirement-owner))
                                        (setf entered t)))
                               nil)
                           (error (condition)
                             (search "target follow final generation changed" (princ-to-string condition)))))
                     (ok (not entered))
                     (ok (and (eq lock pg::*postmaster-data-lock*) (open-stream-p lock)))
                     (ok (and (eq pid-stream pg::*postmaster-pid-stream*) (open-stream-p pid-stream)))
                     (ok (eql position (file-position pid-stream))))))
                (ok (equalp bytes (lineage-test-bytes path)))
                (ok (= replacement-inode (sb-posix:stat-ino (sb-posix:lstat path))))
                (ok (equal (getf snapshot :record)
                           (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(deftest reseed-startup-observation-rejects-byte-identical-final-replacement
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock owner))
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                                 (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                                 (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                                 (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                                 (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                                 (before nil) (replacement-inode nil) (reconciled nil)
                                 (path (merge-pathnames "physical-reseed.follow" root))
                                 (replacement (merge-pathnames "replacement" root))
                                 (bytes (pg::physical-reseed-follow-image (getf snapshot :record))))
         (unwind-protect
              (progn
                (ok
                 (handler-case
                     (progn
                       (pg::postmaster-own-data-directory
                        "127.0.0.1" 5439
                        :startup-observer
                        (lambda (lock phase)
                          (if (eq phase :before-pid)
                              (progn
                                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                                (reseed-follow-owner-test-write replacement bytes)
                                (setf replacement-inode (sb-posix:stat-ino (sb-posix:lstat replacement)))
                                (sb-posix:rename replacement path))
                              (pg::physical-reseed-follow-startup-observation lock phase before)))
                        :startup-reconcile (lambda () (setf reconciled t)))
                       nil)
                   (error (condition)
                     (search "retirement evidence changed" (princ-to-string condition)))))
                (ok (eq :retirement (getf before :branch)))
                (ok (not reconciled))
                (ok (equalp bytes (lineage-test-bytes path)))
                (ok (= replacement-inode (sb-posix:stat-ino (sb-posix:lstat path))))
                (ok (equal (getf snapshot :record)
                           (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record)))
                (ok (null pg::*postmaster-pid-stream*))
                (ok (null pg::*postmaster-data-lock*))
                (ok (null pg::*postmaster-start-timestamp*))
                (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))
           (unwind-protect
                (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
             (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
             (setf sb-ext:*exit-hooks* hooks)))))))

(deftest reseed-startup-observation-refuses-incomplete-intent-before-stale-pid-changes
    (dolist (name '("physical-reseed.follow.tmp" "physical-reseed.follow"))
      (call-with-pid-owner
       (lambda (root)
         (let* ((pid-path (merge-pathnames "postmaster.pid" root))
                (pid-text (uiop:read-file-string pid-path))
                (pid-inode (sb-posix:stat-ino (sb-posix:lstat pid-path)))
                (path (merge-pathnames name root))
                (bytes (sb-ext:string-to-octets "incomplete" :external-format :utf-8))
                (entered nil))
           (close pg::*postmaster-pid-stream*)
           (close pg::*postmaster-data-lock*)
           (setf pg::*postmaster-pid-stream* nil pg::*postmaster-data-lock* nil
                 pg::*postmaster-pid-inode* nil pg::*postmaster-start-timestamp* nil)
           (reseed-follow-owner-test-write path bytes)
           (ok
            (handler-case
                (progn
                  (pg::postmaster-own-data-directory
                   "127.0.0.1" 5439
                   :startup-observer
                   (lambda (lock phase)
                     (setf entered phase)
                     (pg::physical-reseed-follow-startup-observation lock phase)))
                  nil)
              (error () t)))
           (ok (eq entered :before-pid))
           (ok (string= pid-text (uiop:read-file-string pid-path)))
           (ok (= pid-inode (sb-posix:stat-ino (sb-posix:lstat pid-path))))
           (ok (equalp bytes (lineage-test-bytes path)))
           (ok (null pg::*postmaster-pid-stream*))
           (ok (null pg::*postmaster-data-lock*))
           (ok (null pg::*postmaster-start-timestamp*)))))))

(deftest reseed-startup-observation-requires-the-correct-boundary-evidence
    (ok (handler-case
            (progn (pg::physical-reseed-follow-startup-observation nil :before-pid '(:branch :ordinary)) nil)
          (error (condition) (search "initial startup observation" (princ-to-string condition)))))
  (ok (handler-case
          (progn (pg::physical-reseed-follow-startup-observation nil :owned-pid) nil)
        (error (condition) (search "requires pre-PID evidence" (princ-to-string condition)))))
  (ok (handler-case
          (progn (pg::physical-reseed-follow-startup-observation nil :unknown) nil)
        (error () t))))

(deftest reseed-startup-projection-matches-real-pre-and-post-pid-observations
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-retirement-observation-fixture
       kind
       (lambda (owner root workspace root-fd snapshot check)
         (declare (ignore owner workspace))
         (let ((candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
               (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
               (pid-path (merge-pathnames "postmaster.pid" root)))
           (dolist (with-temporary '(nil t))
             (when with-temporary
               (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate)))
             (let ((before nil))
               (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                root-fd snapshot check
                (lambda (descriptors state verify)
                  (setf before (pg::physical-reseed-follow-startup-projection descriptors state))
                  (ok (funcall verify))))
               (reseed-follow-owner-test-write
                pid-path (sb-ext:string-to-octets (format nil "~d~%" (sb-posix:getpid))
                                                  :external-format :utf-8))
               (pg::call-with-physical-reseed-follow-retirement-before-pid-observation
                root-fd snapshot check
                (lambda (descriptors state verify)
                  (ok (equal before (pg::physical-reseed-follow-startup-projection descriptors state)))
                  (ok (funcall verify))))
               (let ((padding nil))
                 (unwind-protect
                      (progn
                        (dotimes (index 3)
                          (declare (ignorable index))
                          (push (sb-posix:open "/dev/null" sb-posix:o-rdonly) padding))
                        (call-with-reseed-retirement-test-owned-pid
                         root
                         (lambda (stream)
                           (let ((position (file-position stream)))
                             (pg::call-with-physical-reseed-follow-retirement-owned-pid-observation
                              root-fd snapshot check
                              (lambda (descriptors state verify)
                                (ok (equal before (pg::physical-reseed-follow-startup-projection descriptors state)))
                                (ok (funcall verify))))
                             (ok (eql position (file-position stream)))
                             (ok (open-stream-p stream))))))
                   (dolist (fd padding) (sb-posix:close fd)))))))))))
