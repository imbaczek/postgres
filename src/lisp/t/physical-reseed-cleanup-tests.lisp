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
