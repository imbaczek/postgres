(in-package #:pg-physical-tests)

(deftest reseed-attempt-writer-never-creates-intent-in-a-replaced-root
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let* ((candidate (copy-tree record))
              (moved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) ".moved/")))
              (check (symbol-function 'pg::physical-reseed-attempt-check-owner))
              (fired nil))
         (setf (getf candidate :phase) :aborting)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-attempt-check-owner)
                      (lambda (actual value)
                        (prog1 (funcall check actual value)
                          (unless fired
                            (setf fired t)
                            (rename-file root moved)
                            (sb-posix:mkdir (namestring root) #o700)))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-write-temporary owner candidate))))
                (ok fired)
                (ok (null (pg::directory-entry-names root))))
           (setf (symbol-function 'pg::physical-reseed-attempt-check-owner) check)
           (when fired
             (uiop:delete-directory-tree root :validate t)
             (rename-file moved root)))
         (ok (equal record (pg::physical-reseed-attempt-read owner)))))))

(deftest reseed-attempt-promotion-never-renames-entries-in-a-replaced-root
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (let* ((candidate (copy-tree record))
              (moved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) ".moved/")))
              (check (symbol-function 'pg::physical-reseed-attempt-check-previous))
              (fired nil)
              (final (pg::physical-reseed-attempt-path root))
              (temp (pg::physical-reseed-attempt-temporary-path root)))
         (setf (getf candidate :phase) :aborting)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-attempt-check-previous)
                      (lambda (actual previous identity)
                        (prog1 (funcall check actual previous identity)
                          (unless fired
                            (setf fired t)
                            (rename-file root moved)
                            (sb-posix:mkdir (namestring root) #o700)
                            (with-open-file (output final :direction :output) (write-string "foreign final" output))
                            (with-open-file (output temp :direction :output) (write-string "foreign temporary" output))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-publish owner candidate :previous record))))
                (ok fired)
                (ok (equalp (pg::string->bytes "foreign final") (lineage-test-bytes final)))
                (ok (probe-file temp)))
           (setf (symbol-function 'pg::physical-reseed-attempt-check-previous) check)
           (when fired
             (uiop:delete-directory-tree root :validate t)
             (rename-file moved root)))
         (ok (eq owner (pg::physical-reseed-check-owner owner)))))))

(deftest reseed-attempt-raw-reader-keeps-borrowed-root-and-fixed-name-boundary
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (declare (ignore owner))
       (let ((fd (pg::physical-backup-open-directory root)))
         (unwind-protect
              (progn
                (ok (equal record (pg::physical-reseed-attempt-read-at fd "physical-reseed.attempt")))
                (ok (equal record (pg::physical-reseed-attempt-read-at fd "physical-reseed.attempt" :stabilize t)))
                (dolist (name '("../physical-reseed.attempt" "postmaster.pid" "physical-reseed.install" ""))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-read-at fd name)))))
                (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))
           (sb-posix:close fd))))))

(deftest reseed-attempt-raw-reader-rejects-unsafe-or-malformed-metadata
    (dolist (kind '(:symlink :fifo :directory :public :hardlink :empty :invalid))
      (call-with-reseed-inventory-fixture
       (lambda (root owner record)
         (declare (ignore owner record))
         (let* ((path (pg::physical-reseed-attempt-path root))
                (saved (merge-pathnames "saved-private-attempt" root))
                (fd (pg::physical-backup-open-directory root)))
           (sb-posix:rename (namestring path) (namestring saved))
           (unwind-protect
                (progn
                  (ecase kind
                    (:symlink (sb-posix:symlink (namestring saved) (namestring path)))
                    (:fifo (sb-posix:mkfifo (namestring path) #o600))
                    (:directory (sb-posix:mkdir (namestring path) #o700))
                    (:hardlink (sb-posix:link (namestring saved) (namestring path)))
                    ((:public :empty :invalid)
                     (with-open-file (output path :direction :output)
                       (unless (eq kind :empty) (write-string "malformed intent" output)))
                     (sb-posix:chmod (namestring path) (if (eq kind :public) #o644 #o600))))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-read-at fd "physical-reseed.attempt"))))
                  (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))
             (sb-posix:close fd)
             (if (eq kind :directory) (sb-posix:rmdir (namestring path)) (sb-posix:unlink (namestring path)))
             (sb-posix:rename (namestring saved) (namestring path))))))))

(defun call-with-reseed-cleanup-fixture (function &key (kind :file))
  (call-with-reaped-reseed-pid
   (lambda (dead)
     (call-with-reseed-owner-directory
      (lambda (root)
        (let* ((pid (reseed-test-write-pid root (- dead)))
               (before (lineage-test-bytes pid))
               (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))
               (intent (pg::physical-reseed-install-temporary-path root))
               (record nil))
          (with-open-file (output (merge-pathnames "old-dirty-data" root) :direction :output)
            (write-string "opaque old data" output))
          (pg::call-with-physical-reseed-owner
           root
           (lambda (owner)
             (let ((candidate (reseed-attempt-test-record owner)))
               (setf (getf candidate :stage)
                     (list :directory (concatenate 'string (string-right-trim "/" (namestring root)) ".missing-stage/")
                           :device (pg::physical-adoption-owner-device owner) :inode 424242)
                     record (pg::physical-reseed-attempt-publish owner candidate)))))
          (reseed-install-test-entry intent kind)
          (let ((intent-inode (sb-posix:stat-ino (sb-posix:lstat (namestring intent)))))
            (pg::call-with-physical-reseed-cleanup-owner
             root (lambda (owner) (funcall function root owner record)))
            (ok (= intent-inode (sb-posix:stat-ino (sb-posix:lstat (namestring intent))))))
          (ok (equalp before (lineage-test-bytes pid)))
          (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))))))))

(defun call-with-reseed-cleanup-client-fixture (owner record function)
  (let* ((state (list :opened 0 :closed 0 :statuses 0 :aborts 0 :discoveries 0 :captures 0
                      :abort-phases '(:closing :closed)))
         (request (getf record :request))
         (symbols '(pg::physical-libpq-connect-frozen pg::physical-libpq-connect
                    pg::physical-client-identity-row pg::pg-client-native-backup-lease-status
                    pg::pg-client-native-backup-lease-abort pg::physical-libpq-close
                    pg::physical-backup-lease-capture-remote))
         (originals (mapcar #'symbol-function symbols)))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                 (lambda (helper frozen &key on-spawn)
                   (declare (ignore helper))
                   (ok (equal (getf (getf record :source) :owner)
                              (pg::physical-libpq-frozen-profile-datum frozen)))
                   (incf (getf state :opened))
                   (let ((channel (pg::%make-physical-libpq-channel
                                   :owner sb-thread:*current-thread*
                                   :info (pg::physical-libpq-decode-connection-info
                                          (libpq-test-connection-body
                                           '(("replication" "true") ("dbname" "replication")
                                             ("password" "journal-secret")) "replication")))))
                     (when on-spawn (funcall on-spawn channel))
                     channel))
                 (symbol-function 'pg::physical-libpq-connect)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (incf (getf state :discoveries))
                   (error "cleanup must use the saved source"))
                 (symbol-function 'pg::physical-client-identity-row)
                 (lambda (client)
                   (declare (ignore client))
                   (vector (format nil "~d" (getf request :system-id)) "3" "0/00000098" nil))
                 (symbol-function 'pg::pg-client-native-backup-lease-status)
                 (lambda (client actual)
                   (declare (ignore client))
                   (ok (equal request actual))
                   (incf (getf state :statuses))
                   (backup-lease-test-record :ready))
                 (symbol-function 'pg::pg-client-native-backup-lease-abort)
                 (lambda (client actual)
                   (declare (ignore client))
                   (ok (equal request actual))
                   (ok (eq :aborting (getf (pg::physical-reseed-attempt-read owner) :phase)))
                   (incf (getf state :aborts))
                   (let ((receipt (reseed-attempt-test-abort-receipt record)))
                     (setf (getf receipt :phase) (or (pop (getf state :abort-phases)) :closed))
                     receipt))
                 (symbol-function 'pg::physical-libpq-close)
                 (lambda (client)
                   (unless (pg::physical-libpq-channel-closed client)
                     (setf (pg::physical-libpq-channel-closed client) t)
                     (incf (getf state :closed))))
                 (symbol-function 'pg::physical-backup-lease-capture-remote)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (incf (getf state :captures))
                   (error "cleanup cannot capture")))
           (funcall function state)
           (ok (zerop (getf state :captures)))
           (ok (zerop (getf state :discoveries)))
           (ok (= (getf state :opened) (getf state :closed))))
      (loop for symbol in symbols for original in originals do (setf (symbol-function symbol) original)))))

(deftest reseed-cleanup-owner-retires-saved-source-through-every-initial-install-fence-kind
    (dolist (kind '(:file :symlink :directory :fifo))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (ok (not (probe-file (getf (getf record :stage) :directory))))
         (call-with-reseed-cleanup-client-fixture
          owner record
          (lambda (state)
            (ok (eq :ready (getf (pg::physical-reseed-status owner :helper "/unused/helper") :phase)))
            (ok (eq :aborting (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
            (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
            (let ((opens (getf state :opened)))
              (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
              (ok (= opens (getf state :opened))))
            (ok (= 1 (getf state :statuses)))
            (ok (= 2 (getf state :aborts)))
            (let ((pg::*pgdata* root))
              (ok (intake-fails-p #'pg::physical-reseed-check-before-pid))))))
       :kind kind)))

(deftest reseed-cleanup-owner-never-grants-clean-adoption-preparation-or-recapture
    (call-with-reseed-cleanup-fixture
     (lambda (root owner record)
       (call-with-reseed-cleanup-client-fixture
        owner record
        (lambda (state)
          (declare (ignore state))
          (ok (eq owner (pg::physical-reseed-check-owner owner)))
          (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-prepare-image owner root))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner record))))
          (ok (intake-fails-p
               (lambda ()
                 (pg::physical-reseed-start-remote
                  owner root (getf record :request)
                  (pg::make-physical-libpq-profile "host=localhost" :mode :physical)
                  :helper "/unused/helper"))))
          (ok (not (probe-file (pg::physical-reseed-install-path root)))))))))

(deftest reseed-cleanup-owner-requires-original-initial-intent-and-trusted-final-attempt
    (dolist (case '(:no-install-temp :has-install-final :missing-attempt :bad-attempt :temp-only-attempt))
      (call-with-reseed-owner-directory
       (lambda (root)
         (let ((entered nil))
           (pg::call-with-physical-reseed-owner
            root (lambda (owner)
                   (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner))))
           (unless (eq case :no-install-temp)
             (reseed-install-test-entry (pg::physical-reseed-install-temporary-path root) :file))
           (case case
             (:has-install-final (reseed-install-test-entry (pg::physical-reseed-install-path root) :file))
             (:missing-attempt (delete-file (pg::physical-reseed-attempt-path root)))
             (:bad-attempt
              (with-open-file (output (pg::physical-reseed-attempt-path root) :direction :output :if-exists :supersede)
                (write-string "malformed final intent" output)))
             (:temp-only-attempt
              (sb-posix:rename (namestring (pg::physical-reseed-attempt-path root))
                               (namestring (pg::physical-reseed-attempt-temporary-path root)))))
           (ok (intake-fails-p
                (lambda () (pg::call-with-physical-reseed-cleanup-owner
                            root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
           (ok (not entered)))))))

(deftest reseed-cleanup-owner-rejects-rebound-attempt-and-changed-original-pid
    (dolist (change '(:request :pid))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (let* ((path (if (eq change :pid) (merge-pathnames "postmaster.pid" root)
                          (pg::physical-reseed-attempt-path root)))
                (bytes (lineage-test-bytes path)))
           (unwind-protect
                (progn
                  (with-open-file (output path :direction :output :if-exists :overwrite
                                               :element-type '(unsigned-byte 8))
                    (if (eq change :pid)
                        (write-sequence #(48 10) output)
                        (let ((changed (copy-tree record)))
                          (setf (getf (getf changed :request) :token) (make-string 64 :initial-element #\b))
                          (write-sequence (pg::physical-reseed-attempt-image changed) output))))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner)))))
             (with-open-file (output path :direction :output :if-exists :overwrite
                                          :element-type '(unsigned-byte 8))
               (write-sequence bytes output))))))))

(deftest reseed-cleanup-owner-revokes-thread-and-scope-bound-metadata-access
    (let ((saved nil) (root-path nil))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (declare (ignore record))
         (setf saved owner root-path root)
         (let ((thread (sb-thread:make-thread
                        (lambda ()
                          (intake-fails-p
                           (lambda () (pg::physical-reseed-attempt-read-owned owner "physical-reseed.attempt")))))))
           (ok (sb-thread:join-thread thread)))))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner saved))))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-attempt-read-owned saved "physical-reseed.attempt"))))
      (ok root-path)))

(deftest reseed-cleanup-owner-recovers-complete-cancel-temp-and-fences-partial-temp
    (dolist (cut '(:after-create :after-write))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (declare (ignore root))
         (call-with-reseed-cleanup-client-fixture
          owner record
          (lambda (state)
            (let* ((fired nil)
                   (pg::*physical-reseed-attempt-test-hook*
                     (lambda (point path)
                       (declare (ignore path))
                       (when (and (eq point cut) (not fired))
                         (setf fired t)
                         (error "injected cancellation publication cut")))))
              (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper"))))
              (ok fired))
            (ok (zerop (getf state :opened)))
            (ok (zerop (getf state :aborts)))
            (if (eq cut :after-create)
                (progn
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-status owner :helper "/unused/helper"))))
                  (ok (zerop (getf state :opened))))
                (progn
                  (ok (eq :ready (getf (pg::physical-reseed-status owner :helper "/unused/helper") :phase)))
                  (ok (eq :aborting (getf (pg::physical-reseed-attempt-read owner) :phase)))
                  (setf (getf state :abort-phases) '(:closed))
                  (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
                  (ok (= 1 (getf state :aborts)))))))))))

(deftest reseed-cleanup-cancel-retries-an-empty-phase-publication
    (dolist (phase '(:premanifest :aborting))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (when (eq phase :aborting)
           (let ((next (copy-tree record)))
             (setf (getf next :phase) :aborting)
             (pg::physical-reseed-attempt-publish owner next :previous record)))
         (call-with-reseed-cleanup-client-fixture
          owner record
          (lambda (state)
            (setf (getf state :abort-phases) '(:closed))
            (let ((fired nil))
              (let ((pg::*physical-reseed-attempt-test-hook*
                      (lambda (point path)
                        (declare (ignore path))
                        (when (and (eq point :after-create) (not fired))
                          (setf fired t)
                          (error "interrupted phase publication")))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper")))))
              (ok fired))
            (ok (probe-file (pg::physical-reseed-attempt-temporary-path root)))
            (ok (eq :aborted
                    (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
            (ok (null (probe-file (pg::physical-reseed-attempt-temporary-path root))))
            (ok (= (if (eq phase :premanifest) 1 2) (getf state :aborts)))
            (ok (= (getf state :opened) (getf state :closed)))))))))

(cl-spec:defproperty reseed-cancellation-preserves-identity-across-canonical-prefixes
    ((aborting boolean) (next boolean) (index (range integer 0 65535)) (complete boolean))
  (:about pg::physical-reseed-cancel pg::physical-reseed-attempt-recover-for-cancel)
  (:trials (:normal 80))
  (let ((accepted nil))
    (call-with-reseed-cleanup-fixture
     (lambda (root owner record)
       (let ((previous (copy-tree record)))
         (when aborting
           (setf (getf previous :phase) :aborting)
           (pg::physical-reseed-attempt-publish owner previous :previous record))
         (let* ((candidate (copy-tree previous))
                (phase (if next (if aborting :aborted :aborting) (getf previous :phase))))
           (setf (getf candidate :phase) phase)
           (let ((receipt (when (eq phase :aborted) (reseed-attempt-test-abort-receipt previous))))
             (pg::physical-reseed-attempt-check-abort-receipt candidate receipt)
             (if complete
                 (pg::physical-reseed-attempt-write-temporary owner candidate :receipt receipt)
                 (let* ((image (pg::physical-reseed-attempt-image candidate))
                        (length (mod index (length image))))
                   (with-open-file (out (pg::physical-reseed-attempt-temporary-path root)
                                        :direction :output :if-exists :error :element-type '(unsigned-byte 8))
                     (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                     (write-sequence image out :end length)))))
           (call-with-reseed-cleanup-client-fixture
            owner record
            (lambda (state)
              (setf (getf state :abort-phases) '(:closed))
              (let ((actual (pg::physical-reseed-cancel owner :helper "/unused/helper")))
                (setf accepted
                      (and (eq :aborted (getf actual :phase))
                           (every (lambda (key) (equal (getf previous key) (getf actual key)))
                                  pg::*physical-reseed-attempt-identity-keys*)
                           (not (probe-file (pg::physical-reseed-attempt-temporary-path root)))
                           (= (if (and complete (eq phase :aborted)) 0 1) (getf state :aborts))
                           (= (getf state :opened) (getf state :closed)))))))))))
    accepted))

(deftest generated-reseed-cancellation-canonical-prefix-recovery
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-cancellation-preserves-identity-across-canonical-prefixes
                                       :seed 102 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest reseed-cleanup-cancel-retries-interrupted-normalization
    (dolist (phase '(:premanifest :aborting))
      (dolist (cut '(:before-cancel-repair-truncate :after-cancel-repair-truncate
                     :after-cancel-repair-empty-fsync :after-cancel-repair-write-chunk
                     :after-cancel-repair-write :after-cancel-repair-file-fsync
                     :before-rename :after-rename :after-directory-fsync :before-readback :after-readback))
        (call-with-reseed-cleanup-fixture
         (lambda (root owner record)
           (let ((previous (copy-tree record)))
             (when (eq phase :aborting)
               (setf (getf previous :phase) :aborting)
               (pg::physical-reseed-attempt-publish owner previous :previous record))
             (let ((candidate (copy-tree previous)))
               (setf (getf candidate :phase) (if (eq phase :premanifest) :aborting :aborted))
               (let ((image (pg::physical-reseed-attempt-image candidate)))
                 (with-open-file (out (pg::physical-reseed-attempt-temporary-path root)
                                      :direction :output :if-exists :error :element-type '(unsigned-byte 8))
                   (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                   (write-sequence image out :end (floor (length image) 2)))))
             (call-with-reseed-cleanup-client-fixture
              owner record
              (lambda (state)
                (setf (getf state :abort-phases) '(:closed))
                (let ((fired nil))
                  (let ((pg::*physical-reseed-cancel-write-limit* 1)
                        (pg::*physical-reseed-attempt-test-hook*
                          (lambda (point path)
                            (declare (ignore path))
                            (when (and (eq point cut) (not fired))
                              (setf fired t)
                              (error "interrupted cancellation normalization")))))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper")))))
                  (ok fired))
                (ok (zerop (getf state :aborts)))
                (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
                (ok (= 1 (getf state :aborts)))
                (ok (null (probe-file (pg::physical-reseed-attempt-temporary-path root))))
                (ok (= (getf state :opened) (getf state :closed)))))))))))

(deftest reseed-cleanup-cancel-refuses-unsafe-or-incompatible-temporaries
    (dolist (kind '(:garbage :trailing :foreign :public :hardlink :symlink :directory :fifo))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (let* ((temporary (pg::physical-reseed-attempt-temporary-path root))
                (other (merge-pathnames "foreign-temporary" root))
                (candidate (copy-tree record))
                (before (lineage-test-bytes (pg::physical-reseed-attempt-path root))))
           (setf (getf candidate :phase) :aborting)
           (when (eq kind :foreign)
             (setf (getf (getf candidate :request) :token) (make-string 64 :initial-element #\b)))
           (let ((image (pg::physical-reseed-attempt-image candidate)))
             (with-open-file (out temporary :direction :output :if-exists :error :element-type '(unsigned-byte 8))
               (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
               (case kind
                 (:garbage (write-byte 0 out))
                 (:trailing (write-sequence image out) (write-byte 0 out))
                 (otherwise (write-sequence image out :end (1- (length image)))))))
           (case kind
             (:public (sb-posix:chmod (namestring temporary) #o644))
             (:hardlink (sb-posix:link (namestring temporary) (namestring other)))
             (:symlink (sb-posix:rename (namestring temporary) (namestring other))
              (sb-posix:symlink (namestring other) (namestring temporary)))
             (:directory (delete-file temporary) (sb-posix:mkdir (namestring temporary) #o700))
             (:fifo (delete-file temporary) (sb-posix:mkfifo (namestring temporary) #o600)))
           (call-with-reseed-cleanup-client-fixture
            owner record
            (lambda (state)
              (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper"))))
              (ok (zerop (getf state :opened)))
              (ok (zerop (getf state :aborts)))))
           (ok (equalp before (lineage-test-bytes (pg::physical-reseed-attempt-path root))))
           (ok (sb-posix:lstat (namestring temporary))))))))

(deftest reseed-cleanup-cancel-syncs-empty-before-writing-replacement
    (call-with-reseed-cleanup-fixture
     (lambda (root owner record)
       (let* ((temporary (pg::physical-reseed-attempt-temporary-path root))
              (candidate (copy-tree record))
              (fsync (symbol-function 'sb-posix:fsync))
              (writer (symbol-function 'pg::physical-adoption-install-write-bytes))
              (fail-empty t) (empty-synced nil) (writes 0))
         (setf (getf candidate :phase) :aborting)
         (let ((image (pg::physical-reseed-attempt-image candidate)))
           (with-open-file (out temporary :direction :output :if-exists :error :element-type '(unsigned-byte 8))
             (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
             (write-sequence image out :end (floor (length image) 2))))
         (let ((inode (sb-posix:stat-ino (sb-posix:lstat (namestring temporary)))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'sb-posix:fsync)
                        (lambda (fd)
                          (let* ((stat (sb-posix:fstat fd))
                                 (empty (and (= inode (sb-posix:stat-ino stat))
                                             (zerop (sb-posix:stat-size stat)))))
                            (when (and empty fail-empty)
                              (setf fail-empty nil)
                              (error "empty barrier fsync failed"))
                            (prog1 (funcall fsync fd)
                              (when empty (setf empty-synced t)))))
                        (symbol-function 'pg::physical-adoption-install-write-bytes)
                        (lambda (fd bytes offset path &key limit point)
                          (incf writes)
                          (ok empty-synced)
                          (ok (zerop (sb-posix:stat-size (sb-posix:fstat fd))))
                          (funcall writer fd bytes offset path :limit limit :point point)))
                  (call-with-reseed-cleanup-client-fixture
                   owner record
                   (lambda (state)
                     (setf (getf state :abort-phases) '(:closed))
                     (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper"))))
                     (ok (zerop writes))
                     (ok (zerop (getf state :opened)))
                     (ok (zerop (sb-posix:stat-size (sb-posix:lstat (namestring temporary)))))
                     (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
                     (ok (= 1 writes))
                     (ok empty-synced)
                     (ok (= 1 (getf state :aborts))))))
             (setf (symbol-function 'sb-posix:fsync) fsync
                   (symbol-function 'pg::physical-adoption-install-write-bytes) writer)))))))

(deftest reseed-cleanup-cancel-rechecks-metadata-at-repair-boundaries
    (dolist (cut '(:before-cancel-repair-truncate :after-cancel-repair-empty-fsync
                   :after-cancel-repair-write))
      (dolist (kind '(:temporary :predecessor :bytes))
        (call-with-reseed-cleanup-fixture
         (lambda (root owner record)
           (let* ((temporary (pg::physical-reseed-attempt-temporary-path root))
                  (final (pg::physical-reseed-attempt-path root))
                  (held (merge-pathnames "held-cancel-metadata" root))
                  (candidate (copy-tree record))
                  (fired nil) (foreign nil) (target nil))
             (setf (getf candidate :phase) :aborting)
             (let ((image (pg::physical-reseed-attempt-image candidate)))
               (with-open-file (out temporary :direction :output :if-exists :error :element-type '(unsigned-byte 8))
                 (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                 (write-sequence image out :end (floor (length image) 2))))
             (unwind-protect
                  (call-with-reseed-cleanup-client-fixture
                   owner record
                   (lambda (state)
                     (let ((pg::*physical-reseed-attempt-test-hook*
                             (lambda (point path)
                               (declare (ignore path))
                               (when (and (eq point cut) (not fired))
                                 (setf fired t target (if (eq kind :predecessor) final temporary))
                                 (if (eq kind :bytes)
                                     (with-open-file (out target :direction :io :if-exists :overwrite :element-type '(unsigned-byte 8))
                                       (write-byte 0 out))
                                     (let ((saved (lineage-test-bytes target)))
                                       (sb-posix:rename (namestring target) (namestring held))
                                       (with-open-file (out target :direction :output :if-exists :error
                                                                   :element-type '(unsigned-byte 8))
                                         (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                                         (write-sequence saved out))))
                                 (setf foreign (lineage-test-bytes target))))))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper")))))
                     (ok fired)
                     (ok (zerop (getf state :opened)))
                     (ok (zerop (getf state :aborts)))
                     (ok (equalp foreign (lineage-test-bytes target)))))
               (when (probe-file held)
                 (delete-file target)
                 (sb-posix:rename (namestring held) (namestring target))))))))))

(deftest reseed-cleanup-cancel-recovers-every-partial-publication-byte
    (dolist (phase '(:premanifest :aborting))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (let* ((previous (copy-tree record))
                (final (pg::physical-reseed-attempt-path root))
                (temporary (pg::physical-reseed-attempt-temporary-path root)))
           (when (eq phase :aborting)
             (setf (getf previous :phase) :aborting)
             (pg::physical-reseed-attempt-publish owner previous :previous record))
           (let* ((candidate (copy-tree previous))
                  (previous-bytes (pg::physical-reseed-attempt-image previous)))
             (setf (getf candidate :phase) (if (eq phase :premanifest) :aborting :aborted))
             (pg::physical-reseed-attempt-check-abort-receipt
              candidate (when (eq phase :aborting) (reseed-attempt-test-abort-receipt previous)))
             (let ((image (pg::physical-reseed-attempt-image candidate)))
               (call-with-reseed-cleanup-client-fixture
                owner record
                (lambda (state)
                  (setf (getf state :abort-phases) '(:closed))
                  (dotimes (index (length image))
                    (with-open-file (out final :direction :output :if-exists :overwrite
                                               :element-type '(unsigned-byte 8))
                      (sb-posix:ftruncate (sb-sys:fd-stream-fd out) 0)
                      (write-sequence previous-bytes out))
                    (with-open-file (out temporary :direction :output :if-exists :error
                                                   :element-type '(unsigned-byte 8))
                      (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                      (write-sequence image out :end index))
                    (let ((actual (pg::physical-reseed-cancel owner :helper "/unused/helper")))
                      (unless (and (eq :aborted (getf actual :phase))
                                   (every (lambda (key) (equal (getf previous key) (getf actual key)))
                                          pg::*physical-reseed-attempt-identity-keys*)
                                   (not (probe-file temporary))
                                   (= (1+ index) (getf state :aborts)))
                        (error "cancellation failed at partial byte ~d" index))))
                  (ok (= (length image) (getf state :aborts)))
                  (ok (= (getf state :opened) (getf state :closed))))))))))))

(deftest reseed-cleanup-cancel-temporary-scope-closes-and-revokes
    (dolist (escape '(nil t))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (let ((fd nil) (check nil) (refresh nil))
           (with-open-file (out (pg::physical-reseed-attempt-temporary-path root)
                                :direction :output :if-exists :error :element-type '(unsigned-byte 8))
             (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600))
           (ok (eq (if escape :escaped :returned)
                   (catch 'cancel-scope
                     (pg::call-with-physical-reseed-cancel-temporary
                      owner
                      (lambda (root-fd temporary path previous bytes validate accept)
                        (declare (ignore root-fd path previous bytes))
                        (setf fd temporary check validate refresh accept)
                        (when escape (throw 'cancel-scope :escaped))
                        :returned)))))
           (ok (intake-fails-p (lambda () (sb-posix:fstat fd))))
           (ok (intake-fails-p check))
           (ok (intake-fails-p (lambda () (funcall refresh (make-array 0 :element-type '(unsigned-byte 8))))))
           (ok (equal record (pg::physical-reseed-attempt-read-owned owner "physical-reseed.attempt"))))))))

(deftest reseed-cleanup-owner-printing-keeps-request-and-connection-data-private
    (call-with-reseed-cleanup-fixture
     (lambda (root owner record)
       (declare (ignore root))
       (let* ((*print-readably* nil)
              (printed (write-to-string owner))
              (safe (and (not (search (getf (getf record :request) :token) printed))
                         (not (search (getf (getf record :source) :conninfo) printed))
                         (not (search "SOURCE" printed))
                         (search "PHYSICAL-RESEED-CLEANUP-OWNER" (string-upcase printed)))))
         (ok (not (null safe)))))))
