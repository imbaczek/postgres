(in-package #:pg)

(defun unit-continuation-active-inputs (&optional (generation 0) (restart 32))
  (let* ((alpha (unit-imported-state-baseline :generation generation))
         (beta (unit-imported-state-baseline :generation (logxor generation 1) :cause :wal-removed))
         (history (timeline-encode-history 2 1 96 "origin" nil)))
    (setf (wal-replication-slot-name alpha) "alpha"
          (wal-replication-slot-name beta) "beta")
    (let* ((baseline (logical-slot-state-encode alpha))
           (lost (logical-slot-state-encode beta)))
      (setf (wal-replication-slot-restart-lsn alpha) restart)
      (values (list :system-id 42 :child-timeline 2 :fork 96
                    :slots (list (list "alpha" nil nil (bytes->string baseline))
                                 (list "beta" nil nil (bytes->string lost))))
              (list 42 nil nil history 2 192)
              (list (list "beta" lost) (list "alpha" (logical-slot-state-encode alpha)))))))

(defun unit-continuation-directory-fixture (function)
  (let* ((root (uiop:ensure-directory-pathname
                (sb-posix:mkdtemp
                 (namestring (merge-pathnames "continuation-directories.XXXXXX"
                                              (uiop:temporary-directory))))))
         (fd nil))
    (unwind-protect
         (progn
           (dolist (name '("pg_wal/" "pg_replslot/"))
             (sb-posix:mkdir (namestring (merge-pathnames name root)) #o700))
           (setf fd (sb-posix:open (namestring root)
                                   (logior sb-posix:o-rdonly sb-posix:o-directory
                                           sb-posix:o-nofollow +postmaster-o-cloexec+)))
           (funcall function fd root))
      (when fd (sb-posix:close fd))
      (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(rove:deftest continuation-owned-directory-checks-thread-and-expiry
    (dolist (purpose '(:wal :slots))
      (unit-continuation-directory-fixture
       (lambda (parent root)
         (declare (ignore root))
         (let ((stale nil) (calls 0)
                           (original (symbol-function 'physical-image-file-facts)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'physical-image-file-facts)
                        (lambda (&rest arguments)
                          (incf calls)
                          (apply original arguments)))
                  (rove:ok
                   (equal (list 37 purpose)
                          (multiple-value-list
                           (call-with-logical-slot-continuation-owned-directory
                            parent purpose
                            (lambda (directory check)
                              (declare (ignore directory))
                              (setf stale check calls 0)
                              (funcall check)
                              (rove:ok (plusp calls))
                              (setf calls 0)
                              (rove:ok
                               (sb-thread:join-thread
                                (sb-thread:make-thread
                                 (lambda ()
                                   (handler-case (progn (funcall check) nil)
                                     (error () t))))))
                              (rove:ok (zerop calls))
                              (funcall check)
                              (values 37 purpose))
                            t))))
                  (setf calls 0)
                  (rove:ok (handler-case (progn (funcall stale) nil) (error () t)))
                  (rove:ok (zerop calls)))
             (setf (symbol-function 'physical-image-file-facts) original)))))))

(rove:deftest continuation-owned-directory-distinguishes-strict-and-mutable-facts
    (dolist (purpose '(:wal :slots))
      (dolist (mutable '(nil t))
        (unit-continuation-directory-fixture
         (lambda (parent root)
           (declare (ignore root))
           (let ((entered nil) (result nil) (failed nil))
             (handler-case
                 (setf result
                       (call-with-logical-slot-continuation-owned-directory
                        parent purpose
                        (lambda (directory check)
                          (funcall check)
                          (setf entered t)
                          (with-open-file (stream (merge-pathnames "probe" (physical-image-fd-path directory))
                                                  :direction :output :if-does-not-exist :create
                                                  :if-exists :error :element-type '(unsigned-byte 8))
                            (write-byte 7 stream))
                          (sb-posix:utime (namestring (physical-image-fd-path directory)) 0 0)
                          :finished)
                        mutable))
               (error () (setf failed t)))
             (rove:ok entered)
             (rove:ok (if mutable (and (not failed) (eq :finished result)) failed))))))))

(rove:deftest continuation-owned-directory-rejects-named-replacement
    (dolist (purpose '(:wal :slots))
      (unit-continuation-directory-fixture
       (lambda (parent root)
         (let* ((name (ecase purpose (:wal "pg_wal/") (:slots "pg_replslot/")))
                (named (merge-pathnames name root))
                (saved (merge-pathnames "saved/" root))
                (entered nil))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-owned-directory
                   parent purpose
                   (lambda (directory check)
                     (declare (ignore directory check))
                     (setf entered t)
                     (sb-posix:rename (namestring named) (namestring saved))
                     (sb-posix:mkdir (namestring named) #o700)
                     :ignored)
                   t)
                  nil)
              (error () t)))
           (rove:ok entered))))))

(rove:deftest continuation-owned-directory-rejects-unknown-purpose-before-io
    (let ((calls 0) (original (symbol-function 'physical-image-file-facts)))
      (unwind-protect
           (progn
             (setf (symbol-function 'physical-image-file-facts)
                   (lambda (&rest arguments) (incf calls) (apply original arguments)))
             (rove:ok
              (handler-case
                  (progn (call-with-logical-slot-continuation-owned-directory
                          -1 :unrecognized (lambda (&rest arguments) (declare (ignore arguments))) t)
                         nil)
                (error () t)))
             (rove:ok (zerop calls)))
        (setf (symbol-function 'physical-image-file-facts) original))))

(rove:deftest continuation-slot-root-rejects-invalid-purpose-before-io
    (let ((calls 0) (opens 0)
                    (original (symbol-function 'call-with-logical-slot-continuation-owned-directory)))
      (unwind-protect
           (progn
             (setf (symbol-function 'call-with-logical-slot-continuation-owned-directory)
                   (lambda (&rest arguments)
                     (incf opens)
                     (apply original arguments)))
             (dolist (arguments
                      (list (list nil (lambda (&rest ignored) (declare (ignore ignored))) (lambda () (incf calls)))
                            (list t nil (lambda () (incf calls)))
                            (list t (lambda (&rest ignored) (declare (ignore ignored))) nil)))
               (rove:ok (handler-case
                            (progn (apply #'call-with-logical-slot-continuation-slot-root -1 arguments) nil)
                          (error () t))))
             (rove:ok (zerop calls))
             (rove:ok (zerop opens)))
        (setf (symbol-function 'call-with-logical-slot-continuation-owned-directory) original))))

(defun unit-continuation-replacement-fixture (purpose function &optional prefix)
  (unit-continuation-directory-fixture
   (lambda (parent root)
     (declare (ignore root))
     (call-with-logical-slot-continuation-owned-directory
      parent :slots
      (lambda (directory check)
        (let ((name (ecase purpose (:selector "selector") (:slot-state "state")))
              (old (string->bytes "old image")) (new (string->bytes "new image")))
          (labels ((write-image (name image)
                     (with-open-file (stream (merge-pathnames name (physical-image-fd-path directory))
                                             :direction :output :if-exists :error
                                             :element-type '(unsigned-byte 8))
                       (write-sequence image stream))
                     (sb-posix:chmod
                      (namestring (merge-pathnames name (physical-image-fd-path directory))) #o600)))
            (write-image name old)
            (when prefix (write-image (concatenate 'string name ".tmp") (subseq new 0 prefix)))
            (funcall function directory check name old new))))
      t))))

(rove:deftest continuation-replacement-owns-phases-and-expires
    (dolist (purpose '(:selector :slot-state))
      (dolist (prefix '(nil 0 4 9))
        (unit-continuation-replacement-fixture
         purpose
         (lambda (directory check name old new)
           (let ((escaped nil))
             (rove:ok
              (equal '(17 :finished)
                     (multiple-value-list
                      (call-with-logical-slot-continuation-file-replacement
                       directory purpose old new check
                       (lambda (operation)
                         (setf escaped operation)
                         (rove:ok (eq :old (funcall operation :check)))
                         (rove:ok (eq :prepared (funcall operation :prepare)))
                         (rove:ok (eq :selected (funcall operation :replace)))
                         (rove:ok (eq :synced (funcall operation :sync)))
                         (rove:ok (equalp new (read-bounded-regular-file (merge-pathnames name (physical-image-fd-path directory)) 2048)))
                         (values 17 :finished))))))
             (rove:ok (handler-case (progn (funcall escaped :check) nil) (error () t)))))
         prefix))))

(rove:deftest continuation-replacement-failed-operation-stays-failed
    (unit-continuation-replacement-fixture
     :selector
     (lambda (directory check name old new)
       (declare (ignore name))
       (let ((entered nil) (refused nil) (sticky nil))
         (rove:ok
          (handler-case
              (progn
                (call-with-logical-slot-continuation-file-replacement
                 directory :selector old new check
                 (lambda (operation)
                   (setf entered t)
                   (setf refused (handler-case (progn (funcall operation :replace) nil) (error () t)))
                   (setf sticky (handler-case (progn (funcall operation :check) nil) (error () t)))))
                nil)
            (error () t)))
         (rove:ok (and entered refused sticky))))))

(rove:deftest continuation-replacement-rejects-foreign-operation
    (unit-continuation-replacement-fixture
     :slot-state
     (lambda (directory check name old new)
       (declare (ignore name))
       (call-with-logical-slot-continuation-file-replacement
        directory :slot-state old new check
        (lambda (operation)
          (rove:ok
           (sb-thread:join-thread
            (sb-thread:make-thread
             (lambda () (handler-case (progn (funcall operation :prepare) nil) (error () t))))))
          (rove:ok (eq :old (funcall operation :check))))))))

(rove:deftest continuation-replacement-preserves-files-at-every-cut
    (dolist (purpose '(:selector :slot-state))
      (dolist (cut '(:after-write :after-temporary-sync :after-replace :after-selected-sync :after-directory-sync))
        (unit-continuation-replacement-fixture
         purpose
         (lambda (directory check name old new)
           (let* ((fds (length (directory #p"/proc/self/fd/*")))
                  (tag (gensym "REPLACEMENT-CUT")) (hits 0)
                  (*logical-slot-continuation-replacement-test-hook*
                    (lambda (point)
                      (when (eq point cut) (incf hits) (throw tag :injected))))
                  (result
                    (catch tag
                      (call-with-logical-slot-continuation-file-replacement
                       directory purpose old new check
                       (lambda (operation)
                         (funcall operation :prepare)
                         (funcall operation :replace)
                         (funcall operation :sync)
                         :unexpected)))))
             (rove:ok (and (eq result :injected) (= hits 1)))
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
             (rove:ok
              (equalp (if (member cut '(:after-write :after-temporary-sync)) old new)
                      (read-bounded-regular-file (merge-pathnames name (physical-image-fd-path directory)) 2048)))
             (rove:ok
              (eq (not (null (member cut '(:after-write :after-temporary-sync))))
                  (not (null (probe-file
                              (merge-pathnames (concatenate 'string name ".tmp") (physical-image-fd-path directory)))))))
             (funcall check)))))))

(rove:deftest continuation-replacement-rejects-changed-temporary
    (dolist (purpose '(:selector :slot-state))
      (unit-continuation-replacement-fixture
       purpose
       (lambda (directory check name old new)
         (let* ((path (merge-pathnames (concatenate 'string name ".tmp") (physical-image-fd-path directory)))
                (entered nil) (fds (length (directory #p"/proc/self/fd/*"))))
           (with-open-file (stream path :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
             (write-byte 255 stream))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-file-replacement
                   directory purpose old new check (lambda (operation) (declare (ignore operation)) (setf entered t)))
                  nil)
              (error () t)))
           (rove:ok (not entered))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok (= 255 (aref (read-bounded-regular-file path 2048) 0)))))
       4)))

(rove:deftest continuation-replacement-rejects-selected-output-swap
    (dolist (purpose '(:selector :slot-state))
      (unit-continuation-replacement-fixture
       purpose
       (lambda (directory check name old new)
         (let* ((selected (merge-pathnames name (physical-image-fd-path directory)))
                (saved (merge-pathnames "../foreign-saved" (physical-image-fd-path directory)))
                (entered nil) (refused nil) (sticky nil)
                (fds (length (directory #p"/proc/self/fd/*")))
                (*logical-slot-continuation-replacement-test-hook*
                  (lambda (point)
                    (when (eq point :after-replace)
                      (setf entered t)
                      (sb-posix:rename (namestring selected) (namestring saved))
                      (with-open-file (stream selected :direction :output :if-exists :error
                                                       :element-type '(unsigned-byte 8))
                        (write-sequence new stream))
                      (sb-posix:chmod (namestring selected) #o600)))))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-file-replacement
                   directory purpose old new check
                   (lambda (operation)
                     (funcall operation :prepare)
                     (setf refused (handler-case (progn (funcall operation :replace) nil) (error () t)))
                     (setf sticky (handler-case (progn (funcall operation :sync) nil) (error () t)))))
                  nil)
              (error () t)))
           (rove:ok (and entered refused sticky))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok (probe-file saved))
           (rove:ok (equalp new (read-bounded-regular-file selected 2048))))))))

(cl-spec:defproperty continuation-replacement-resumes-exact-generated-prefixes
    ((size (range integer 1 131089)) (seed (range integer 0 255)) (part (range integer 0 131089)))
  (:about call-with-logical-slot-continuation-file-replacement)
  (:trials (:normal 200))
  (let* ((purpose (if (oddp seed) :slot-state :selector))
         (size (if (eq purpose :slot-state) (1+ (mod size 2048)) size))
         (prefix (mod part (1+ size)))
         (image (make-array size :element-type '(unsigned-byte 8))))
    (dotimes (i size) (setf (aref image i) (mod (+ seed (* 17 i)) 256)))
    (unit-continuation-replacement-fixture
     purpose
     (lambda (directory check name old ignored)
       (declare (ignore ignored))
       (let ((path (merge-pathnames (concatenate 'string name ".tmp") (physical-image-fd-path directory))))
         (with-open-file (stream path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
           (write-sequence image stream :end prefix))
         (sb-posix:chmod (namestring path) #o600))
       (and
        (eq :synced
            (call-with-logical-slot-continuation-file-replacement
             directory purpose old image check
             (lambda (operation)
               (funcall operation :prepare)
               (funcall operation :replace)
               (funcall operation :sync))))
        (equalp image
                (read-bounded-regular-file
                 (merge-pathnames name (physical-image-fd-path directory)) 131089)))))))

(rove:deftest continuation-replacement-generated-prefix-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'continuation-replacement-resumes-exact-generated-prefixes :seed 11508 :profile :normal))))
      (format t "~&REPLACEMENT-PREFIX-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-replacement-closes-owned-inodes-after-mode-change
    (dolist (purpose '(:selector :slot-state))
      (dolist (kind '(:old :temporary :selected))
        (unit-continuation-replacement-fixture
         purpose
         (lambda (directory check name old new)
           (let ((fds (length (directory #p"/proc/self/fd/*")))
                 (entered nil))
             (rove:ok
              (handler-case
                  (progn
                    (call-with-logical-slot-continuation-file-replacement
                     directory purpose old new check
                     (lambda (operation)
                       (unless (eq kind :old) (funcall operation :prepare))
                       (when (eq kind :selected) (funcall operation :replace))
                       (setf entered t)
                       (sb-posix:chmod
                        (namestring
                         (merge-pathnames (if (eq kind :temporary) (concatenate 'string name ".tmp") name)
                                          (physical-image-fd-path directory)))
                        #o640)))
                    nil)
                (error () t)))
             (rove:ok entered)
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))))))))

(rove:deftest continuation-replacement-leaves-a-reused-descriptor-open
    (dolist (kind '(:old :temporary :selected))
      (unit-continuation-replacement-fixture
       :selector
       (lambda (directory check name old new)
         (let ((foreign nil) (entered nil) (refused nil)
                             (fds (length (directory #p"/proc/self/fd/*")))
                             (path (merge-pathnames "../replacement-foreign" (physical-image-fd-path directory))))
           (unwind-protect
                (progn
                  (rove:ok
                   (handler-case
                       (progn
                         (call-with-logical-slot-continuation-file-replacement
                          directory :selector old new check
                          (lambda (operation)
                            (unless (eq kind :old) (funcall operation :prepare))
                            (when (eq kind :selected) (funcall operation :replace))
                            (let* ((target (physical-image-file-facts
                                            directory (if (eq kind :temporary) "selector.tmp" name)))
                                   (matches
                                     (loop for entry in (directory-entry-names #p"/proc/self/fd/" :follow-symlinks t)
                                           for fd = (parse-integer entry)
                                           for facts = (handler-case (physical-image-file-facts fd) (error () nil))
                                           when (and facts
                                                     (equal (logical-slot-copy-install-file-identity target)
                                                            (logical-slot-copy-install-file-identity facts)))
                                             collect fd)))
                              (assert (= 1 (length matches)))
                              (sb-posix:close (first matches))
                              (setf foreign (sb-posix:open (namestring path)
                                                           (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                                                   +postmaster-o-cloexec+) #o600))
                              (assert (= foreign (first matches)))
                              (setf entered t)
                              (setf refused
                                    (handler-case (progn (funcall operation :check) nil) (error () t))))))
                         nil)
                     (error () t)))
                  (rove:ok (and entered refused))
                  (rove:ok (equal (logical-slot-copy-install-file-identity (physical-image-file-facts foreign))
                                  (logical-slot-copy-install-file-identity
                                   (physical-image-file-facts -100 (namestring path))))))
             (when foreign (sb-posix:close foreign)))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*")))))))))

(rove:deftest continuation-replacement-rejects-normal-partial-return
    (dolist (phase '(:prepared :selected))
      (unit-continuation-replacement-fixture
       :selector
       (lambda (directory check name old new)
         (let ((fds (length (directory #p"/proc/self/fd/*"))) (entered nil))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-file-replacement
                   directory :selector old new check
                   (lambda (operation)
                     (funcall operation :prepare)
                     (when (eq phase :selected) (funcall operation :replace))
                     (setf entered t)
                     :partial))
                  nil)
              (error (condition) (search "returned before synchronization" (princ-to-string condition)))))
           (rove:ok entered)
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok
            (equalp (if (eq phase :selected) new old)
                    (read-bounded-regular-file (merge-pathnames name (physical-image-fd-path directory)) 2048))))))))

(cl-spec:defproperty continuation-initial-active-keeps-exact-saved-obligations
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (restart (range integer 32 64)))
  (:about logical-slot-continuation-active-snapshot)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation restart)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
           (plan (logical-slot-continuation-check snapshot))
           (encoded (logical-slot-continuation-encode snapshot)))
      (and (equal '("alpha" "beta") (mapcar #'first (getf snapshot :slots)))
           (equalp (reverse rows) (getf snapshot :slots))
           (not (eq (second (first rows)) (second (second (getf snapshot :slots)))))
           (= 96 (logical-slot-continuation-plan-floor plan))
           (equal '((96 2 :record-start) 2) (logical-slot-continuation-plan-tail-request plan))
           (null (logical-slot-continuation-plan-root-requests plan))
           (null (logical-slot-continuation-plan-run-requests plan))
           (equalp encoded (logical-slot-continuation-encode
                            (logical-slot-continuation-decode encoded)))))))

(rove:deftest continuation-initial-active-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-initial-active-keeps-exact-saved-obligations
                                         :seed 11401 :profile :normal))))
      (format t "~&ACTIVE-CONTRACT status=~s trials=~s seed=11401 digest=~s failure=~s shrunk=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest)
              (getf result :failure) (getf result :shrunk-failure))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-initial-active-rejects-unbound-saved-images
    (dolist (mode '(:empty :duplicate :generation :unknown-name :foreign-system :later-child))
      (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
        (ecase mode
          (:empty (setf rows nil))
          (:duplicate (setf rows (append rows (list (first rows)))))
          (:generation
           (let ((slot (unit-imported-state-baseline :generation 100)))
             (setf (wal-replication-slot-name slot) "alpha"
                   (second (second rows)) (logical-slot-state-encode slot))))
          (:unknown-name (setf (getf certificate :slots) nil))
          (:foreign-system (setf (first host) 100))
          (:later-child (setf (fifth host) 3)))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-active-snapshot
                             (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)) certificate host rows) nil)
                   (error () t))))))

(rove:deftest continuation-initial-active-snapshot-detaches-source-images
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
             (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
             (saved (logical-slot-continuation-encode snapshot)))
        (setf (aref binding 0) 99
              (aref (fourth host) 0) 0
              (aref (second (first rows)) 0) 0)
        (rove:ok (equalp saved (logical-slot-continuation-encode snapshot))))))

(rove:deftest continuation-initial-active-does-not-resurrect-historical-slots
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((current (remove "beta" rows :key #'first :test #'equal))
             (snapshot (logical-slot-continuation-active-snapshot
                        (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3))
                        certificate host current)))
        (rove:ok (= 2 (length (getf certificate :slots))))
        (rove:ok (equal '("alpha") (mapcar #'first (getf snapshot :slots))))
        (rove:ok (logical-slot-continuation-plan-tail-request
                  (logical-slot-continuation-check snapshot))))))

(rove:deftest continuation-tail-files-reject-real-held-and-named-changes
    (dolist (mode '(:replace :truncate :append :symlink))
      (call-with-logical-slot-real-startup-test
       (lambda ()
         (let* ((path (merge-pathnames "continuation-tail-probe" *pgdata*))
                (bytes (make-array 5 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3 4 5))))
           (wal-atomic-write-bytes path bytes)
           (with-slot-state-directory (directory (namestring *pgdata*))
             (multiple-value-bind (stream size) (open-bounded-regular-file path 64)
               (unwind-protect
                    (let* ((facts (physical-image-file-facts (sb-sys:fd-stream-fd stream)))
                           (held (list (list "continuation-tail-probe" stream facts size))))
                      (rove:ok (logical-slot-continuation-check-tail-files directory held))
                      (ecase mode
                        (:replace (wal-atomic-write-bytes path bytes))
                        (:truncate
                         (with-open-file (output path :direction :io :element-type '(unsigned-byte 8)
                                                      :if-exists :overwrite)
                           (sb-posix:ftruncate (sb-sys:fd-stream-fd output) 2)))
                        (:append
                         (with-open-file (output path :direction :output :element-type '(unsigned-byte 8)
                                                      :if-exists :append)
                           (write-byte 6 output)))
                        (:symlink
                         (let ((other (merge-pathnames "continuation-tail-other" *pgdata*)))
                           (wal-atomic-write-bytes other bytes)
                           (delete-file path)
                           (sb-posix:symlink (namestring other) (namestring path)))))
                      (rove:ok (handler-case
                                   (progn (logical-slot-continuation-check-tail-files directory held) nil)
                                 (error () t))))
                 (close stream)))))))))

(rove:deftest continuation-initial-active-validates-row-shapes-before-copying
    (let ((original (symbol-function 'logical-slot-continuation-copy)))
      (unwind-protect
           (dolist (mode '(:cycle :dotted :name :blob))
             (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
               (let ((called nil) (row (first rows)))
                 (ecase mode
                   (:cycle (setf (cddr row) row))
                   (:dotted (setf (cddr row) 1))
                   (:name (setf (first row) (list "beta")))
                   (:blob (setf (second row) (list 1 2 3))))
                 (setf (symbol-function 'logical-slot-continuation-copy)
                       (lambda (value) (declare (ignore value)) (setf called t)
                               (error "malformed row reached copier")))
                 (rove:ok (handler-case
                              (progn
                                (logical-slot-continuation-active-snapshot
                                 (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3))
                                 certificate host rows)
                                nil)
                            (error () t)))
                 (rove:ok (not called))
                 (setf (symbol-function 'logical-slot-continuation-copy) original))))
        (setf (symbol-function 'logical-slot-continuation-copy) original))))

(cl-spec:defproperty continuation-tail-transcript-keeps-coordinates-and-exact-bytes
    ((size (range integer 1 256)) (byte (range integer 0 255))
                                  (carrier (range integer 1 4294967295)))
  (:about logical-slot-continuation-tail-frame)
  (:trials (:normal 200))
  (let* ((raw (make-array size :element-type '(unsigned-byte 8) :initial-element byte))
         (record (make-wal-record :start 96 :end (+ 96 size)))
         (frame (logical-slot-continuation-tail-frame record raw 2 carrier :ordinary 80)))
    (and (= 80 (wal-u64 frame 0)) (= 96 (wal-u64 frame 8))
         (= (+ 96 size) (wal-u64 frame 16))
         (= 2 (wal-u32 frame 24)) (= carrier (wal-u32 frame 28))
         (= 0 (aref frame 32)) (= size (wal-u32 frame 36))
         (= (+ 40 size) (length frame))
         (equalp raw (subseq frame 40)))))

(rove:deftest continuation-tail-transcript-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-tail-transcript-keeps-coordinates-and-exact-bytes
                                         :seed 11402 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-tail-transcript-spills-outside-pgdata-and-unwinds
    (dolist (exit '(:normal :error :throw))
      (call-with-logical-slot-real-startup-test
       (lambda ()
         (let ((path nil) (entered nil)
                          (facts (physical-image-file-facts -100 (namestring *pgdata*)))
                          (fds (length (directory #p"/proc/self/fd/*")))
                          (*logical-capture-memory-bytes* 0))
           (rove:ok
            (eq exit
                (handler-case
                    (catch 'transcript-exit
                      (call-with-logical-slot-continuation-transcript
                       (lambda (spool)
                         (let ((bytes (make-array 128 :element-type '(unsigned-byte 8) :initial-element 9)))
                           (logical-spool-append spool bytes)
                           (logical-spool-seal spool)
                           (setf path (logical-spool-path spool) entered t)
                           (rove:ok path)
                           (rove:ok (not (uiop:subpathp path *pgdata*)))
                           (logical-spool-map-events spool (lambda (observed) (rove:ok (equalp bytes observed))))
                           (sb-thread:with-mutex (*wal-lock*) (wal-require-no-temp-cleanup-under-lock))
                           (ecase exit
                             (:normal :normal)
                             (:error (error "transcript probe"))
                             (:throw (throw 'transcript-exit :throw)))))))
                  (error (condition) (if (eq exit :error) :error (error condition))))))
           (rove:ok entered)
           (rove:ok (not (probe-file path)))
           (rove:ok (not (probe-file (uiop:pathname-directory-pathname path))))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok (equal facts (physical-image-file-facts -100 (namestring *pgdata*)))))))))

(defun unit-continuation-boundary-positions ()
  (let* ((fork 80)
         (eor-end (+ fork +wal-end-of-recovery-record-size+))
         (batch-end (+ eor-end +wal-header-size+))
         (commit-end (+ batch-end +wal-header-size+ 8)))
    (values fork eor-end batch-end commit-end)))

(defun unit-continuation-boundary-records (observe)
  (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
    (declare (ignore commit-end))
    (let* ((eor (wal-encode-end-of-recovery-record fork 0 2 1 2))
           (batch (wal-encode-record +wal-record-batch-start+ 0 eor-end
                                     (make-array 0 :element-type '(unsigned-byte 8))))
           (commit (wal-encode-record +wal-record-commit+ 10 batch-end (wal-encode-commit-timestamp 0))))
      (funcall observe (wal-decode-record eor fork) eor 2 2 :end-of-recovery fork)
      (funcall observe (wal-decode-record batch eor-end) batch 2 2 :ordinary eor-end)
      (funcall observe (wal-decode-record commit batch-end) commit 2 2 :ordinary batch-end))))

(cl-spec:defproperty continuation-boundary-classification-keeps-restart-and-ack-distinct
    ((mode (range integer 0 4)))
  (:about logical-slot-continuation-boundary-observer)
  (:trials (:normal 200))
  (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
    (let ((result
            (handler-case
                (multiple-value-bind (observe finish)
                    (logical-slot-continuation-boundary-observer
                     (nth mode
                          (list (list (list :restart fork 2 :record-start)
                                      (list :restart eor-end 2 :record-start)
                                      (list :restart commit-end 2 :prefix-end)
                                      (list :confirmed commit-end 2 :prefix-end))
                                (list (list :restart (1+ fork) 2 :prefix-end))
                                (list (list :confirmed eor-end 2 :prefix-end))
                                (list (list :restart batch-end 3 :prefix-end))
                                (list (list :confirmed batch-end 2 :prefix-end))))
                     :fork fork)
                  (unit-continuation-boundary-records observe)
                  (funcall finish)
                  :accepted)
              (error () :rejected))))
      (eq result (if (zerop mode) :accepted :rejected)))))

(rove:deftest continuation-boundary-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-boundary-classification-keeps-restart-and-ack-distinct
                                         :seed 11403 :profile :normal))))
      (format t "~&BOUNDARY-CONTRACT status=~s trials=~s seed=11403 digest=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-boundary-requires-records-and-completed-ordinary-acks
    (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :restart fork 2 :record-start) (list :restart eor-end 2 :record-start)
                 (list :restart commit-end 2 :prefix-end)
                 (list :confirmed commit-end 2 :prefix-end))
           :fork fork)
        (unit-continuation-boundary-records observe)
        (rove:ok (funcall finish)))
      (dolist (request (list (list :restart (1+ fork) 2 :prefix-end)
                             (list :confirmed eor-end 2 :prefix-end)
                             (list :restart batch-end 3 :prefix-end)
                             (list :confirmed batch-end 2 :prefix-end)))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer (list request) :fork fork)
          (unit-continuation-boundary-records observe)
          (rove:ok (handler-case (progn (funcall finish) nil) (error () t))))))
  (multiple-value-bind (observe finish) (logical-slot-continuation-boundary-observer nil)
    (let ((raw (wal-encode-record +wal-record-batch-start+ 0 96
                                  (make-array 0 :element-type '(unsigned-byte 8)))))
      (funcall observe (wal-decode-record raw 96) raw 2 2 :ordinary 96)
      (rove:ok (handler-case (progn (funcall finish) nil) (error () t))))))

(rove:deftest continuation-boundary-rejects-interior-batch-restarts
    (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
      (declare (ignore eor-end commit-end))
      (dolist (role '(:record-start :prefix-end))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer
             (list (list :restart batch-end 2 role)) :fork fork)
          (unit-continuation-boundary-records observe)
          (rove:ok (handler-case (progn (funcall finish) nil) (error () t))
                   "A commit start and preceding BATCH end cannot advance a slot restart")))))

(rove:deftest continuation-boundary-checkpoint-does-not-certify-an-ack
    (let* ((start 80)
           (raw (wal-encode-record +wal-record-checkpoint+ 0 start
                                   (make-array 0 :element-type '(unsigned-byte 8))))
           (record (wal-decode-record raw start)))
      (dolist (kind '(:confirmed :restart))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer
             (list (list kind (wal-record-end record) 1 :prefix-end))
             :checkpoint (list start raw))
          (funcall observe record raw 1 1 :ordinary start)
          (rove:ok (handler-case (progn (funcall finish) nil) (error () t))
                   "An exact standalone checkpoint is traversal evidence, not a completed ACK batch")))))

(rove:deftest continuation-boundary-keeps-persisted-pending-batch-start-role
    (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
      (declare (ignore batch-end commit-end))
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :restart eor-end 2 :prefix-end)) :fork fork)
        (unit-continuation-boundary-records observe)
        (rove:ok (handler-case (funcall finish) (error () nil))
                 "A persisted pending restart can use prefix-end geometry at an exact BATCH start"))
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :restart fork 2 :prefix-end)) :fork fork)
        (unit-continuation-boundary-records observe)
        (rove:ok (handler-case (progn (funcall finish) nil) (error () t))
                 "The child EOR never certifies a prefix-end restart"))))

(cl-spec:defproperty startup-publication-image-detects-derived-state-mutation
    ((xid (range integer 3 10000)) (start (range integer 32 128)) (span (range integer 1 128)))
  (:about logical-slot-publication-check-registry)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
    (declare (ignore certificate host))
    (let* ((slot (logical-slot-state-decode (second (find "alpha" rows :key #'first :test #'equal)) "alpha"))
           (pending (wal-replication-slot-pending-xids slot))
           (registry (make-hash-table :test #'equal)))
      (setf (gethash xid pending) (cons start (+ start span))
            (gethash "alpha" registry) slot)
      (let ((image (logical-slot-publication-registry-image registry)))
        (flet ((rejected ()
                 (handler-case
                     (progn (logical-slot-publication-check-registry registry image) nil)
                   (error () t))))
          (and (logical-slot-publication-check-registry registry image)
               (progn (incf (cdr (gethash xid pending))) (rejected))
               (progn (decf (cdr (gethash xid pending)))
                      (logical-slot-publication-check-registry registry image))
               (progn (setf (gethash "alpha" registry) (copy-wal-replication-slot slot)) (rejected))
               (progn (setf (gethash "alpha" registry) slot
                            (wal-replication-slot-state-uncertain-p slot) t)
                      (rejected))))))))

(rove:deftest startup-publication-image-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'startup-publication-image-detects-derived-state-mutation
                                         :seed 11502 :profile :normal))))
      (format t "~&STARTUP-PUBLICATION-IMAGE status=~s trials=~s seed=11502 digest=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(cl-spec:defproperty continuation-spool-pull-preserves-owned-frames
    ((size (range integer 1 128)) (count (range integer 0 12)) (spill (range integer 0 1)))
  (:about call-with-logical-spool-reader)
  (:trials (:normal 200))
  (let* ((*logical-capture-memory-bytes* (if (zerop spill) 0 1048576))
         (events (loop for index below count
                       collect (make-array size :element-type '(unsigned-byte 8)
                                                :initial-element (mod (+ size index) 256))))
         (expected (mapcar #'copy-seq events)))
    (call-with-logical-slot-continuation-transcript
     (lambda (spool)
       (dolist (event events) (logical-spool-append spool event))
       (logical-spool-seal spool)
       (let ((actual nil) (expired nil))
         (call-with-logical-spool-reader
          spool 128
          (lambda (next)
            (setf expired next)
            (loop for frame = (funcall next) while frame
                  do (push (copy-seq frame) actual)
                     (setf (aref frame 0) (logxor 255 (aref frame 0))))))
         (and (equalp expected (nreverse actual))
              (equalp expected events)
              (not (logical-spool-failed-p spool))
              (handler-case (progn (funcall expired) nil) (error () t))
              (equal '(:one :two)
                     (multiple-value-list
                      (call-with-logical-spool-reader
                       spool 128 (lambda (next) (funcall next) (values :one :two)))))
              (not (logical-spool-failed-p spool))
              (let ((reread nil))
                (logical-spool-map-events spool (lambda (event) (push (copy-seq event) reread)))
                (equalp expected (nreverse reread)))))))))

(rove:deftest continuation-spool-pull-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-spool-pull-preserves-owned-frames
                                         :seed 11503 :profile :normal))))
      (format t "~&CONTINUATION-SPOOL-PULL status=~s trials=~s seed=11503 digest=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-spool-pull-rejects-bounds-and-corruption
    (dolist (kind '(:bound :length :crc :trailing))
      (let ((*logical-capture-memory-bytes* 0))
        (call-with-logical-slot-continuation-transcript
         (lambda (spool)
           (logical-spool-append spool (make-array 32 :element-type '(unsigned-byte 8) :initial-element 7))
           (logical-spool-seal spool)
           (let ((stream (logical-spool-stream spool)) (caught nil) (returned nil) (completed nil))
             (ecase kind
               (:bound)
               (:length
                (file-position stream 0)
                (dotimes (index 4) (write-byte 255 stream)))
               (:crc (file-position stream 8) (write-byte 8 stream))
               (:trailing (file-position stream :end) (write-byte 42 stream)))
             (finish-output stream)
             (handler-case
                 (progn
                   (call-with-logical-spool-reader
                    spool (if (eq kind :bound) 16 64)
                    (lambda (next)
                      (handler-case (progn (funcall next) (funcall next))
                        (error () (setf caught t)))
                      (setf returned t)))
                   (setf completed t))
               (error () nil))
             (rove:ok caught (format nil "~s fails inside NEXT" kind))
             (rove:ok returned "The test caught the read failure inside its callback")
             (rove:ok (not completed) "A caught read failure rejects the enclosing reader scope")
             (rove:ok (logical-spool-failed-p spool))
             (rove:ok (handler-case
                          (progn (call-with-logical-spool-reader spool 64 (lambda (next) (funcall next))) nil)
                        (error () t)))))))))

(rove:deftest continuation-spool-pull-rejects-foreign-nested-and-expired-use
    (dolist (limit '(0 1024))
      (let ((*logical-capture-memory-bytes* limit) (retained nil))
        (call-with-logical-slot-continuation-transcript
         (lambda (spool)
           (logical-spool-append spool (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(7 8)))
           (logical-spool-seal spool)
           (call-with-logical-spool-reader
            spool 2
            (lambda (next)
              (setf retained next)
              (rove:ok (sb-thread:join-thread
                        (sb-thread:make-thread
                         (lambda () (handler-case (progn (funcall next) nil) (error () t))))))
              (rove:ok (handler-case (progn (logical-spool-map-events spool #'identity) nil) (error () t)))
              (rove:ok (equalp #(7 8) (funcall next)))
              (rove:ok (null (funcall next)))))
           (rove:ok (not (logical-spool-failed-p spool)))
           (rove:ok (handler-case (progn (funcall retained) nil) (error () t)))
           (rove:ok (not (logical-spool-failed-p spool))))))))

(rove:deftest continuation-spool-pull-unwind-and-memory-topology
    (dolist (case '(:throw :cycle :bound))
      (let ((*logical-capture-memory-bytes* 1024))
        (call-with-logical-slot-continuation-transcript
         (lambda (spool)
           (logical-spool-append spool (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(7 8)))
           (logical-spool-seal spool)
           (if (eq case :throw)
               (rove:ok
                (eq :cut
                    (catch 'continuation-reader-cut
                      (call-with-logical-spool-reader
                       spool 2 (lambda (next) (funcall next) (throw 'continuation-reader-cut :cut))))))
               (progn
                 (when (eq case :cycle)
                   (setf (cdr (logical-spool-events spool)) (logical-spool-events spool)))
                 (rove:ok
                  (handler-case
                      (progn (call-with-logical-spool-reader spool (if (eq case :bound) 1 2) (lambda (next) (funcall next))) nil)
                    (error () t)))))
           (rove:ok (logical-spool-failed-p spool)))))))

(cl-spec:defproperty continuation-runtime-tail-allows-open-and-abandoned-extension
    ((count (range integer 0 12)))
  (:about logical-slot-continuation-runtime-tail-observer)
  (:trials (:normal 200))
  (multiple-value-bind (fork eor-end batch-end sealed) (unit-continuation-boundary-positions)
    (declare (ignore eor-end batch-end))
    (multiple-value-bind (observe finish)
        (logical-slot-continuation-runtime-tail-observer fork sealed)
      (unit-continuation-boundary-records observe)
      (let ((position sealed))
        (dotimes (i count)
          (declare (ignore i))
          (let* ((raw (wal-encode-record +wal-record-batch-start+ 0 position
                                         (make-array 0 :element-type '(unsigned-byte 8))))
                 (record (wal-decode-record raw position)))
            (funcall observe record raw 2 2 :ordinary position)
            (setf position (wal-record-end record)))))
      (funcall finish))))

(rove:deftest continuation-runtime-tail-extension-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'continuation-runtime-tail-allows-open-and-abandoned-extension
                    :seed 11504 :profile :normal))))
      (format t "~&RUNTIME-TAIL-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-runtime-tail-keeps-the-historical-seal-strict
    (multiple-value-bind (fork eor-end batch-end sealed) (unit-continuation-boundary-positions)
      (declare (ignore eor-end))
      (dolist (mode '(:unfinished-seal :missing-seal :orphan-extension))
        (rove:ok
         (handler-case
             (progn
               (multiple-value-bind (observe finish)
                   (logical-slot-continuation-runtime-tail-observer
                    fork (if (eq mode :unfinished-seal) batch-end sealed))
                 (if (eq mode :missing-seal)
                     (funcall finish)
                     (progn
                       (unit-continuation-boundary-records observe)
                       (when (eq mode :orphan-extension)
                         (let* ((raw (wal-encode-record +wal-record-commit+ 10 sealed
                                                        (wal-encode-commit-timestamp 0)))
                                (record (wal-decode-record raw sealed)))
                           (funcall observe record raw 2 2 :ordinary sealed)))
                       (funcall finish))))
               nil)
           (error () t))
         (format nil "~s must be rejected" mode)))))
