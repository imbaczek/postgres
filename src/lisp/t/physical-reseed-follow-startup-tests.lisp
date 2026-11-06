(in-package #:pg-physical-tests)

(defun retired-receipt-test-rows (mode inode size)
  (let* ((geometry (nth mode '(:follow :alias :receipt)))
         (row (list (copy-seq "physical-reseed.follow") 17 inode #o100600
                    (if (= mode 1) 2 1) 1000 size 100 200))
         (receipt (cons (copy-seq "physical-reseed.retired") (copy-tree (rest row)))))
    (values (unless (= mode 2) row) (unless (= mode 0) receipt) geometry)))

(cl-spec:defproperty reseed-retired-geometry-binds-exact-names-and-link-counts
    ((mode (range integer 0 2)) (inode (range integer 1 1000000)) (size (range integer 1 4096)))
  (:about pg::physical-reseed-follow-retired-geometry)
  (:trials (:normal 100))
  (multiple-value-bind (follow receipt geometry) (retired-receipt-test-rows mode inode size)
    (let ((before (copy-tree (list follow receipt))))
      (and (eq geometry
               (funcall (symbol-function 'pg::physical-reseed-follow-retired-geometry) follow receipt))
           (equal before (list follow receipt))))))

(deftest generated-reseed-retired-geometry-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retired-geometry-binds-exact-names-and-link-counts
                                       :seed 172 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-retired-geometry-refuses-inconsistent-observations
    (dolist (case '(:absent :follow-name :receipt-name :follow-extra-link :receipt-extra-link
                    :follow-single-link :receipt-single-link :third-link :inode :device :mode
                    :owner :size :mtime :ctime :row-length))
      (multiple-value-bind (follow receipt geometry)
          (retired-receipt-test-rows
           (case case ((:follow-name :follow-extra-link) 0)
                 ((:receipt-name :receipt-extra-link) 2) (otherwise 1))
           11 128)
        (declare (ignore geometry))
        (ecase case
          (:absent (setf follow nil receipt nil))
          (:follow-name (setf (first follow) "foreign"))
          (:receipt-name (setf (first receipt) "foreign"))
          (:follow-extra-link (setf (fifth follow) 2))
          (:receipt-extra-link (setf (fifth receipt) 2))
          (:follow-single-link (setf (fifth follow) 1))
          (:receipt-single-link (setf (fifth receipt) 1))
          (:third-link (setf (fifth follow) 3 (fifth receipt) 3))
          (:inode (incf (third receipt)))
          (:device (incf (second receipt)))
          (:mode (setf (fourth receipt) #o100644))
          (:owner (incf (sixth receipt)))
          (:size (incf (seventh receipt)))
          (:mtime (incf (eighth receipt)))
          (:ctime (incf (ninth receipt)))
          (:row-length (setf receipt (append receipt (list :unexpected)))))
        (ok (handler-case
                (progn
                  (funcall (symbol-function 'pg::physical-reseed-follow-retired-geometry) follow receipt)
                  nil)
              (error () t))))))

(defun call-with-retired-reader-test-fixture (kind function)
  (call-with-reseed-retirement-observation-fixture
   kind
   (lambda (owner root workspace root-fd snapshot check)
     (declare (ignore owner workspace))
     (let* ((before (prepare-retirement-terminal-test root root-fd snapshot nil))
            (record (pg::physical-reseed-follow-retirement-next-record
                     before (getf (getf before :retirement) :attempt-file))))
       (reseed-retirement-layout-write root record)
       (funcall function root root-fd record check)))))

(defun check-retired-reader-geometries (kinds)
  (dolist (kind kinds)
    (call-with-retired-reader-test-fixture
     kind
     (lambda (root root-fd record check)
       (let ((follow (merge-pathnames "physical-reseed.follow" root))
             (receipt (merge-pathnames "physical-reseed.retired" root)))
         (dolist (geometry '(:follow :alias :receipt))
           (ecase geometry
             (:follow)
             (:alias (sb-posix:link (namestring follow) (namestring receipt)))
             (:receipt (delete-file follow)))
           (let ((original-sync (symbol-function 'sb-posix:fsync))
                 (original-link (symbol-function 'pg::port-linkat))
                 (original-unlink (symbol-function 'pg::port-unlinkat))
                 (syncs 0) (links 0) (unlinks 0)
                 (held nil) (held-fd nil) (escaped nil) (seen nil))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'sb-posix:fsync)
                          (lambda (fd) (incf syncs) (funcall original-sync fd))
                          (symbol-function 'pg::port-linkat)
                          (lambda (&rest arguments) (incf links) (apply original-link arguments))
                          (symbol-function 'pg::port-unlinkat)
                          (lambda (&rest arguments) (incf unlinks) (apply original-unlink arguments)))
                    (let ((result
                            (multiple-value-list
                             (pg::call-with-physical-reseed-follow-retired-journal
                              root-fd
                              (lambda (stream state verify)
                                (setf held stream held-fd (sb-sys:fd-stream-fd stream) escaped verify
                                      seen (pg::physical-adoption-attempt-value-copy (getf state :snapshot)))
                                (ok (eq geometry (getf state :geometry)))
                                (ok (equal record (getf seen :record)))
                                (ok (= (if (eq geometry :alias) 2 1)
                                       (sb-posix:stat-nlink (sb-posix:fstat held-fd))))
                                (ok (logbitp 0 (sb-posix:fcntl held-fd sb-posix:f-getfd)))
                                (ok (equalp (pg::physical-reseed-follow-image record) (getf state :bytes)))
                                (ok (funcall verify))
                                (let ((outcome nil))
                                  (sb-thread:join-thread
                                   (sb-thread:make-thread
                                    (lambda ()
                                      (setf outcome
                                            (handler-case (progn (funcall verify) nil)
                                              (error (condition)
                                                (search "scope ended or changed thread"
                                                        (princ-to-string condition))))))))
                                  (ok outcome))
                                (setf (getf state :geometry) :forged
                                      (getf (getf state :snapshot) :record) (list :forged t)
                                      (aref (getf state :bytes) 0) 0)
                                (ok (funcall verify))
                                (values :first :second))
                              check))))
                      (ok (equal '(:first :second) result)))
                    (ng (open-stream-p held))
                    (ok (handler-case (progn (sb-posix:fstat held-fd) nil)
                          (sb-posix:syscall-error () t)))
                    (ok (handler-case (progn (funcall escaped) nil)
                          (error (condition) (search "scope ended or changed thread" (princ-to-string condition)))))
                    (ok (sb-posix:fstat root-fd))
                    (let ((fresh (pg::physical-reseed-follow-retired-journal-at root-fd check)))
                      (ok (eq geometry (getf fresh :geometry)))
                      (ok (equal record (getf (getf fresh :snapshot) :record))))
                    (ok (handler-case
                            (progn (pg::physical-reseed-follow-entry root-fd "physical-reseed.retired") nil)
                          (error (condition) (search "unexpected target follow journal name"
                                                     (princ-to-string condition)))))
                    (ecase geometry
                      (:follow (ok (equal seen (pg::physical-reseed-follow-snapshot-at root-fd))))
                      (:alias (ok (handler-case
                                      (progn (pg::physical-reseed-follow-snapshot-at root-fd) nil)
                                    (error (condition) (search "single-link" (princ-to-string condition))))))
                      (:receipt (ng (pg::physical-reseed-follow-snapshot-at root-fd))))
                    (ok (zerop syncs))
                    (ok (zerop links))
                    (ok (zerop unlinks)))
               (setf (symbol-function 'sb-posix:fsync) original-sync
                     (symbol-function 'pg::port-linkat) original-link
                     (symbol-function 'pg::port-unlinkat) original-unlink)))))))))

(deftest reseed-retired-reader-binds-all-three-geometries-without-effects
    (check-retired-reader-geometries
     '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer)))

(defun reset-retired-reader-test-files (root record geometry)
  (dolist (name '("physical-reseed.follow" "physical-reseed.retired" "physical-reseed.follow.tmp"
                  "retired-third-link" "retired-other-link"))
    (let ((path (merge-pathnames name root)))
      (when (pg::physical-adoption-path-present-p path)
        (if (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:lstat (namestring path))))
            (sb-posix:rmdir (namestring path))
            (sb-posix:unlink (namestring path))))))
  (reseed-follow-owner-test-write
   (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image record))
  (when (member geometry '(:alias :receipt))
    (sb-posix:link (namestring (merge-pathnames "physical-reseed.follow" root))
                   (namestring (merge-pathnames "physical-reseed.retired" root))))
  (when (eq geometry :receipt)
    (delete-file (merge-pathnames "physical-reseed.follow" root))))

(deftest reseed-retired-reader-checks-parent-before-descriptor-access
    (let ((checks 0) (entered nil))
      (ok (handler-case
              (progn
                (pg::call-with-physical-reseed-follow-retired-journal
                 nil (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t))
                 (lambda () (incf checks) (error "retired parent cut")))
                nil)
            (error (condition) (search "retired parent cut" (princ-to-string condition)))))
      (ok (= 1 checks))
      (ng entered)))

(deftest reseed-retired-reader-refuses-invalid-files-before-callback
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (case '(:absent :symlink :directory :fifo :empty :mode :extra-link
                       :different-inode :temporary :wrong-phase :noncanonical))
         (reset-retired-reader-test-files
          root record (if (member case '(:extra-link :different-inode)) :alias :receipt))
         (let ((follow (merge-pathnames "physical-reseed.follow" root))
               (receipt (merge-pathnames "physical-reseed.retired" root))
               (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
               (entered nil)
               (original-open (symbol-function 'pg::physical-image-openat))
               (opened nil))
           (ecase case
             (:absent (delete-file receipt))
             ((:symlink :directory :fifo :empty)
              (delete-file receipt)
              (ecase case
                (:symlink (sb-posix:symlink "missing" (namestring receipt)))
                (:directory (sb-posix:mkdir (namestring receipt) #o700))
                (:fifo (sb-posix:mkfifo (namestring receipt) #o600))
                (:empty (reseed-follow-owner-test-write receipt (make-array 0 :element-type '(unsigned-byte 8))))))
             (:mode (sb-posix:chmod (namestring receipt) #o644))
             (:extra-link
              (sb-posix:link (namestring receipt) (namestring (merge-pathnames "retired-third-link" root))))
             (:different-inode
              (delete-file receipt)
              (reseed-follow-owner-test-write receipt (pg::physical-reseed-follow-image record))
              (sb-posix:link (namestring receipt) (namestring (merge-pathnames "retired-other-link" root)))
              (sb-posix:link (namestring follow) (namestring (merge-pathnames "retired-third-link" root))))
             (:temporary
              (reseed-follow-owner-test-write temporary (make-array 0 :element-type '(unsigned-byte 8))))
             (:wrong-phase
              (let ((previous (pg::physical-adoption-attempt-value-copy record)))
                (setf (getf (getf previous :retirement) :phase) :retiring)
                (delete-file receipt)
                (reseed-follow-owner-test-write receipt (pg::physical-reseed-follow-image previous))))
             (:noncanonical
              (let* ((text (concatenate 'string " " (pg::physical-bootstrap-manifest-text record)))
                     (bytes (pg::string->bytes
                             (format nil "PGRESEEDFOLLOW1 ~8,'0X~%~a~%"
                                     (pg::wal-crc32 (pg::string->bytes text)) text))))
                (delete-file receipt)
                (reseed-follow-owner-test-write receipt bytes))))
           (let ((names (sort (pg::directory-entry-names root) #'string<))
                 (facts (loop for path in (list follow receipt temporary)
                              collect (when (pg::physical-adoption-path-present-p path)
                                        (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-image-openat)
                          (lambda (&rest arguments)
                            (let ((fd (apply original-open arguments)))
                              (when (not (minusp fd)) (setf opened fd))
                              fd)))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retired-journal
                               root-fd
                               (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t))
                               check)
                              nil)
                          (error (condition)
                            (search (case case
                                      ((:absent :extra-link :different-inode) "link geometry")
                                      (:temporary "refuses a follow temporary")
                                      (:wrong-phase "canonical Retired2 image")
                                      (:noncanonical "noncanonical physical backup lease record")
                                      (otherwise "bounded private regular file"))
                                    (princ-to-string condition))))))
               (setf (symbol-function 'pg::physical-image-openat) original-open))
             (ok (eql (not (null opened)) (not (null (member case '(:wrong-phase :noncanonical))))))
             (when opened
               (ok (handler-case (progn (sb-posix:fstat opened) nil)
                     (sb-posix:syscall-error () t))))
             (ng entered)
             (ok (equal names (sort (pg::directory-entry-names root) #'string<)))
             (ok (equal facts
                        (loop for path in (list follow receipt temporary)
                              collect (when (pg::physical-adoption-path-present-p path)
                                        (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))))
             (ok (sb-posix:fstat root-fd))))))))

(deftest reseed-retired-reader-closes-its-own-stream-on-every-exit
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (geometry '(:follow :alias :receipt))
         (reset-retired-reader-test-files root record geometry)
         (dolist (mode '(:normal :error :throw :interrupt))
           (let ((held nil) (fd nil) (escaped nil) (entered nil))
             (let ((outcome
                     (catch 'retired-reader-lifetime-cut
                       (handler-case
                           (multiple-value-list
                            (pg::call-with-physical-reseed-follow-retired-journal
                             root-fd
                             (lambda (stream state verify)
                               (declare (ignore state))
                               (setf held stream fd (sb-sys:fd-stream-fd stream)
                                     escaped verify entered t)
                               (ecase mode
                                 (:normal (values :first :second))
                                 (:error (error "retired reader lifetime cut"))
                                 (:throw (throw 'retired-reader-lifetime-cut :cut))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'retired-reader-lifetime-cut :cut))))))
                             check))
                         (error (condition)
                           (if (search "retired reader lifetime cut" (princ-to-string condition))
                               :error
                               (progn (format t "~%RETIRED-READER-UNEXPECTED-EXIT ~a~%" condition)
                                      :wrong-error)))))))
               (ok (equal outcome (case mode (:normal '(:first :second)) (:error :error) (otherwise :cut)))))
             (ok entered)
             (ng (open-stream-p held))
             (ok (handler-case (progn (sb-posix:fstat fd) nil) (sb-posix:syscall-error () t)))
             (ok (handler-case (progn (funcall escaped) nil)
                   (error (condition) (search "scope ended or changed thread" (princ-to-string condition)))))
             (ok (sb-posix:fstat root-fd))
             (ok (equal record
                        (getf (getf (pg::physical-reseed-follow-retired-journal-at root-fd check)
                                    :snapshot) :record)))))))))

(defun replace-retired-reader-test-file (root path record)
  (let ((replacement (merge-pathnames "retired-replacement" root)))
    (reseed-follow-owner-test-write replacement (pg::physical-reseed-follow-image record))
    (sb-posix:rename (namestring replacement) (namestring path))
    (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))

(deftest reseed-retired-reader-fences-same-byte-replacement-through-cleanup
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (dolist (geometry '(:follow :alias :receipt))
         (dolist (point '(:after-retired-journal-open :callback :after-retired-journal-close))
           (reset-retired-reader-test-files root record geometry)
           (let* ((name (if (eq geometry :receipt) "physical-reseed.retired" "physical-reseed.follow"))
                  (path (merge-pathnames name root))
                  (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
                  (original-open (symbol-function 'pg::physical-image-openat))
                  (opened nil) (held nil) (escaped nil) (entered nil) (changed nil)
                  (closed-at-hook nil) (revoked-at-hook nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event observed)
                      (when (eq event point)
                        (when (eq point :after-retired-journal-close)
                          (setf closed-at-hook (not (open-stream-p held))
                                revoked-at-hook
                                (handler-case (progn (funcall escaped) nil)
                                  (error (condition)
                                    (not (null (search "scope ended or changed thread"
                                                       (princ-to-string condition))))))))
                        (setf changed (replace-retired-reader-test-file root observed record))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-image-openat)
                          (lambda (&rest arguments)
                            (let ((fd (apply original-open arguments)))
                              (when (not (minusp fd)) (setf opened fd))
                              fd)))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retired-journal
                               root-fd
                               (lambda (stream state verify)
                                 (declare (ignore state))
                                 (setf held stream escaped verify entered t)
                                 (when (eq point :callback)
                                   (setf changed (replace-retired-reader-test-file root path record))))
                               check)
                              nil)
                          (error (condition)
                            (and changed (search "retired journal" (princ-to-string condition)))))))
               (setf (symbol-function 'pg::physical-image-openat) original-open))
             (ok changed)
             (ok (eql entered (not (eq point :after-retired-journal-open))))
             (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
             (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes path)))
             (ok (handler-case (progn (sb-posix:fstat opened) nil) (sb-posix:syscall-error () t)))
             (ok (sb-posix:fstat root-fd))
             (when entered
               (ng (open-stream-p held))
               (ok (handler-case (progn (funcall escaped) nil)
                     (error (condition) (search "scope ended or changed thread" (princ-to-string condition))))))
             (when (eq point :after-retired-journal-close)
               (ok closed-at-hook)
               (ok revoked-at-hook))))))))

(deftest reseed-retired-reader-refuses-a-substituted-named-root-mount
    (call-with-retired-reader-test-fixture
     :same-timeline
     (lambda (root root-fd record check)
       (declare (ignore record))
       (let ((original (symbol-function 'pg::physical-adoption-install-mount-id))
             (calls 0) (entered nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                      (lambda (directory &key directory-fd)
                        (let ((mount (funcall original directory :directory-fd directory-fd)))
                          (if (and directory (equal (pathname directory) root))
                              (progn (incf calls) (1+ mount))
                              mount))))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retired-journal
                           root-fd (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t)) check)
                          nil)
                      (error (condition) (search "canonical target root" (princ-to-string condition)))))
                (ok (= 1 calls))
                (ng entered)
                (ok (sb-posix:fstat root-fd)))
           (setf (symbol-function 'pg::physical-adoption-install-mount-id) original))))))

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

(deftest reseed-retired-receipt-fences-real-startup-before-pid-inspection
    (dolist (kind '(:file :symlink :directory :fifo))
      (with-physical-store-directory (root)
        (call-with-adoption-postmaster-fixture
         root
         (lambda ()
           (let ((pid (merge-pathnames "postmaster.pid" root))
                 (receipt (merge-pathnames "physical-reseed.retired" root))
                 (boundaries nil))
             (with-open-file (stream pid :direction :output :if-exists :error)
               (write-string "opaque old PID bytes" stream))
             (ecase kind
               (:file
                (with-open-file (stream receipt :direction :output :if-exists :error)
                  (write-string "retired receipt" stream)))
               (:symlink (sb-posix:symlink "missing" (namestring receipt)))
               (:directory (sb-posix:mkdir (namestring receipt) #o700))
               (:fifo (sb-posix:mkfifo (namestring receipt) #o600)))
             (let ((bytes (lineage-test-bytes pid))
                   (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))
                   (facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
               (ok (handler-case
                       (progn
                         (pg::postmaster-own-data-directory
                          "127.0.0.1" 5439
                          :startup-observer
                          (lambda (lock phase)
                            (push phase boundaries)
                            (pg::physical-reseed-follow-startup-observation lock phase)))
                         nil)
                     (error (condition)
                       (search "pending offline reseed follow requires journal reconciliation"
                               (princ-to-string condition)))))
               (ok (equal '(:before-pid) boundaries))
               (ok (equalp bytes (lineage-test-bytes pid)))
               (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
               (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
               (ng pg::*postmaster-pid-stream*)
               (ng pg::*postmaster-data-lock*))))))))

(deftest reseed-retired-receipt-fences-legacy-ordinary-entrypoints
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (receipt (merge-pathnames "physical-reseed.retired" root))
             (bytes (sb-ext:string-to-octets "retired receipt" :external-format :utf-8)))
        (reseed-follow-owner-test-write receipt bytes)
        (let ((facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
          (dolist (function (list #'pg::physical-reseed-check-before-pid
                                  #'pg::physical-adoption-startup-preflight
                                  #'pg::physical-adoption-reconcile-startup
                                  (lambda () (pg::physical-directory-check-reseed-intent root :install-only t))))
            (ok (handler-case
                    (progn (funcall function) nil)
                  (error (condition)
                    (search "pending offline reseed follow requires journal reconciliation"
                            (princ-to-string condition))))))
          (ok (equalp bytes (lineage-test-bytes receipt)))
          (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))))))

(deftest reseed-retired-receipt-fences-the-ordinary-owned-pid-boundary
    (call-with-pid-owner
     (lambda (root)
       (pg::postmaster-remove-pid-file)
       (close pg::*postmaster-data-lock*)
       (setf pg::*postmaster-data-lock* nil pg::*postmaster-start-timestamp* nil)
       (let ((before nil) (facts nil) (boundaries nil)
                          (receipt (merge-pathnames "physical-reseed.retired" root))
                          (bytes (sb-ext:string-to-octets "retired receipt" :external-format :utf-8)))
         (ok (handler-case
                 (progn
                   (pg::postmaster-own-data-directory
                    "127.0.0.1" 5439
                    :startup-observer
                    (lambda (lock phase)
                      (push phase boundaries)
                      (if (eq phase :before-pid)
                          (progn
                            (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                            (reseed-follow-owner-test-write receipt bytes)
                            (setf facts (pg::physical-reseed-file-facts
                                         (sb-posix:lstat (namestring receipt)))))
                          (pg::physical-reseed-follow-startup-observation lock phase before))))
                   nil)
               (error (condition)
                 (search "pending offline reseed follow requires journal reconciliation"
                         (princ-to-string condition)))))
         (ok (equal before '(:branch :ordinary)))
         (ok (equal boundaries '(:owned-pid :before-pid)))
         (ok (equalp bytes (lineage-test-bytes receipt)))
         (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring receipt)))))
         (ng pg::*postmaster-data-lock*)
         (ng pg::*postmaster-pid-stream*)
         (ng pg::*postmaster-start-timestamp*)
         (ng (probe-file (merge-pathnames "postmaster.pid" root)))))))

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
