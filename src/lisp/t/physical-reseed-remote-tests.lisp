(in-package #:pg-physical-tests)

(defun call-with-reseed-remote-fixture (function)
  (call-with-reseed-owner-directory
   (lambda (root)
     (with-physical-store-directory (stage)
       (sb-posix:chmod (namestring stage) #o700)
       (let ((old-data (merge-pathnames "old-dirty-data" root)))
         (with-open-file (output old-data :direction :output) (write-string "preserve opaque old data" output))
         (let ((old-bytes (lineage-test-bytes old-data))
               (old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring old-data)))))
           (pg::call-with-physical-reseed-owner
            root
            (lambda (owner)
              (let* ((request (backup-lease-test-request))
                     (profile (pg::make-physical-libpq-profile
                               "host=localhost password=driver-supplied-secret" :fallback "reseed" :mode :physical))
                     (state (list :captures 0 :discovery 0 :frozen 0 :closes 0 :channels nil :requests nil
                                  :statuses 0 :aborts 0 :reader-owner nil :capture-fd nil :capture-write-witness t
                                  :capture-action nil :frozen-action nil :discovery-action nil :identity-action nil
                                  :system (getf request :system-id) :timeline (getf request :timeline)
                                  :status-result nil :abort-phases '(:closed) :abort-action nil :frozen-owner nil))
                     (symbols '(pg::physical-libpq-connect pg::physical-libpq-connect-frozen
                                pg::physical-client-identity-row pg::physical-backup-lease-capture-remote
                                pg::pg-client-native-backup-lease-status pg::pg-client-native-backup-lease-abort
                                pg::physical-libpq-close))
                     (originals (mapcar #'symbol-function symbols)))
                (labels ((channel ()
                           (let ((channel (pg::%make-physical-libpq-channel
                                           :owner sb-thread:*current-thread*
                                           :info (pg::physical-libpq-decode-connection-info
                                                  (libpq-test-connection-body
                                                   '(("replication" "true") ("dbname" "replication")
                                                     ("password" "driver-effective-secret"))
                                                   "replication")))))
                             (push channel (getf state :channels))
                             channel))
                         (receipt (phase)
                           (let ((record (reseed-attempt-test-abort-receipt
                                          (list :request request))))
                             (setf (getf record :phase) phase)
                             record)))
                  (unwind-protect
                       (progn
                         (setf (symbol-function 'pg::physical-libpq-connect)
                               (lambda (helper supplied &key on-spawn)
                                 (declare (ignore helper supplied))
                                 (incf (getf state :discovery))
                                 (let ((client (channel)))
                                   (when on-spawn (funcall on-spawn client))
                                   (when (getf state :discovery-action)
                                     (funcall (getf state :discovery-action) client))
                                   client))
                               (symbol-function 'pg::physical-libpq-connect-frozen)
                               (lambda (helper frozen &key on-spawn)
                                 (declare (ignore helper))
                                 (incf (getf state :frozen))
                                 (let ((expected (getf (getf (pg::physical-reseed-attempt-read (or (getf state :reader-owner) owner)) :source) :owner)))
                                   (ok (equal expected (pg::physical-libpq-frozen-profile-datum frozen)))
                                   (setf (getf state :frozen-owner) expected))
                                 (let ((client (channel)))
                                   (when on-spawn (funcall on-spawn client))
                                   (when (getf state :frozen-action) (funcall (getf state :frozen-action) client))
                                   client))
                               (symbol-function 'pg::physical-client-identity-row)
                               (lambda (client)
                                 (when (getf state :identity-action) (funcall (getf state :identity-action) client))
                                 (vector (format nil "~d" (getf state :system))
                                         (format nil "~d" (getf state :timeline)) "0/00000098" nil))
                               (symbol-function 'pg::physical-backup-lease-capture-remote)
                               (lambda (directory client original &key directory-fd)
                                 (setf (getf state :capture-fd) directory-fd)
                                 (incf (getf state :captures))
                                 (setf (getf state :status-result) (backup-lease-test-record :ready))
                                 (push (pg::physical-backup-lease-request-copy original) (getf state :requests))
                                 (let ((saved (pg::physical-reseed-attempt-read owner)))
                                   (ok (equal original (getf saved :request)))
                                   (ok (equal (namestring (truename directory)) (getf (getf saved :stage) :directory))))
                                 (when (getf state :capture-write-witness)
                                   (with-open-file (output (merge-pathnames "received.witness" directory) :direction :output)
                                     (write-string "owned partial or complete image" output)))
                                 (when (getf state :capture-action)
                                   (funcall (getf state :capture-action) directory client original))
                                 (values :received (backup-lease-test-record :ready)))
                               (symbol-function 'pg::pg-client-native-backup-lease-status)
                               (lambda (client original)
                                 (declare (ignore client))
                                 (incf (getf state :statuses))
                                 (ok (equal request original))
                                 (getf state :status-result))
                               (symbol-function 'pg::pg-client-native-backup-lease-abort)
                               (lambda (client original)
                                 (incf (getf state :aborts))
                                 (ok (equal request original))
                                 (if (getf state :abort-action)
                                     (funcall (getf state :abort-action) client original)
                                     (receipt (or (pop (getf state :abort-phases)) :closed))))
                               (symbol-function 'pg::physical-libpq-close)
                               (lambda (client)
                                 (unless (pg::physical-libpq-channel-closed client)
                                   (setf (pg::physical-libpq-channel-closed client) t)
                                   (incf (getf state :closes)))))
                         (funcall function root stage owner request profile state))
                    (loop for symbol in symbols for original in originals
                          do (setf (symbol-function symbol) original)))))))
           (ok (equalp old-bytes (lineage-test-bytes old-data)))
           (ok (= old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring old-data)))))))))))

(deftest reseed-remote-captures-once-only-after-durable-frozen-journal
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (declare (ignore root))
       (ok (eq :received (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")))
       (when (= 1 (getf state :captures))
         (ok (= 1 (getf state :discovery)))
         (ok (= 1 (getf state :frozen)))
         (ok (= 2 (getf state :closes)))
         (delete-file (merge-pathnames "received.witness" stage))
         (ok (intake-fails-p
              (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
         (ok (= 1 (getf state :captures)))
         (ok (= 1 (getf state :discovery)))
         (setf (getf state :status-result) nil)
         (ok (null (pg::physical-reseed-status owner :helper "/unused/helper")))
         (ok (= 1 (getf state :captures)))))))

(deftest reseed-remote-cancellation-survives-missing-stage-and-source-promotion
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (setf (getf state :capture-action)
             (lambda (&rest arguments) (declare (ignore arguments)) (error "injected capture failure")))
       (ok (intake-fails-p
            (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
       (when (probe-file (pg::physical-reseed-attempt-path root))
         (ok (= 1 (getf state :captures)))
         (uiop:delete-directory-tree stage :validate t)
         (setf (getf state :timeline) 3 (getf state :abort-phases) '(:closing :closed))
         (ok (eq :ready (getf (pg::physical-reseed-status owner :helper "/unused/helper") :phase)))
         (ok (eq :aborting (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
         (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
         (let ((opens (getf state :frozen)))
           (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
           (ok (= opens (getf state :frozen))))
         (let ((pg::*pgdata* root)) (ok (intake-fails-p #'pg::physical-reseed-check-before-pid)))
         (ok (= 1 (getf state :captures)))
         (ok (= (+ (getf state :discovery) (getf state :frozen)) (getf state :closes)))))))

(deftest reseed-remote-publication-errors-and-exits-never-dispatch-capture
    (dolist (cut '(:after-create :after-write :after-file-fsync :before-rename
                   :after-rename :after-directory-fsync :before-readback :after-readback))
      (dolist (mode '(:error :nonlocal))
        (call-with-reseed-remote-fixture
         (lambda (root stage owner request profile state)
           (let* ((fired nil)
                  (pg::*physical-reseed-attempt-test-hook*
                    (lambda (point path)
                      (declare (ignore path))
                      (when (and (eq point cut) (not fired))
                        (setf fired t)
                        (if (eq mode :error) (error "injected target journal failure")
                            (throw 'remote-publication-cut :cut))))))
             (ok (eq :cut
                     (catch 'remote-publication-cut
                       (handler-case
                           (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")
                         (error () :cut)))))
             (ok fired)
             (ok (zerop (getf state :captures)))
             (ok (= 1 (getf state :discovery)))
             (ok (= 1 (getf state :closes)))
             (if (eq cut :after-create)
                 (progn
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-status owner :helper "/unused/helper"))))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper"))))
                   (ok (zerop (getf state :frozen))))
                 (progn
                   (ok (null (pg::physical-reseed-status owner :helper "/unused/helper")))
                   (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
             (ok (zerop (getf state :captures)))
             (ok (or (probe-file (pg::physical-reseed-attempt-path root))
                     (probe-file (pg::physical-reseed-attempt-temporary-path root))))))))))

(deftest reseed-remote-nonlocal-exits-close-channels-and-release-stage-ownership
    (dolist (cut '(:after-journal :after-connect :before-capture :after-capture))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (declare (ignore root))
         (let ((pg::*physical-reseed-remote-test-hook*
                 (lambda (point) (when (eq point cut) (throw 'remote-driver-cut :cut)))))
           (ok (eq :cut
                   (catch 'remote-driver-cut
                     (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")))))
         (ok (= (if (eq cut :after-capture) 1 0) (getf state :captures)))
         (ok (= (+ (getf state :discovery) (getf state :frozen)) (getf state :closes)))
         (let ((fd (pg::physical-backup-open-directory stage)))
           (unwind-protect (ok (zerop (pg::physical-store-flock fd 6)))
             (sb-posix:close fd)))
         (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))))))

(deftest reseed-remote-rejects-source-changes-before-lease-commands
    (dolist (kind '(:route :system :capture-timeline))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (setf (getf state :frozen-action)
               (lambda (client)
                 (ecase kind
                   (:route (incf (pg::physical-libpq-connection-info-port (pg::physical-libpq-channel-info client))))
                   (:system (incf (getf state :system)))
                   (:capture-timeline (setf (getf state :timeline) 3)))))
         (ok (intake-fails-p
              (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
         (ok (zerop (getf state :captures)))
         (ok (probe-file (pg::physical-reseed-attempt-path root)))
         (ok (= 2 (getf state :closes)))
         (setf (getf state :frozen-action) nil
               (getf state :system) (1+ (getf request :system-id)))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-status owner :helper "/unused/helper"))))
         (ok (zerop (getf state :statuses)))
         (setf (getf state :system) (getf request :system-id))
         (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
         (ok (zerop (getf state :captures)))))))

(deftest reseed-remote-freezes-effective-credentials-and-keeps-legacy-cancellation-usable
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (declare (ignore root))
       (let ((pg::*physical-reseed-remote-test-hook*
               (lambda (point) (when (eq point :after-journal) (throw 'save-only :saved)))))
         (ok (eq :saved
                 (catch 'save-only
                   (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")))))
       (let* ((record (pg::physical-reseed-attempt-read owner))
              (source (getf record :source))
              (password (second (assoc "password" (getf source :owner) :test #'equal))))
         (ok (equal "driver-effective-secret" (pg::bytes->string (pg::physical-libpq-datum-octets password))))
         (ok (search "driver-supplied-secret" (getf source :conninfo))))
       (fill (pg::%physical-libpq-profile-conninfo profile) #\x)
       (ok (null (pg::physical-reseed-status owner :helper "/unused/helper")))
       (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
       (ok (zerop (getf state :captures)))))
  (call-with-reseed-remote-fixture
   (lambda (root stage owner request profile state)
     (declare (ignore root stage request profile))
     (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner))
     (ok (null (pg::physical-reseed-status owner :helper "/unused/helper")))
     (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
     (ok (zerop (getf state :captures))))))

(deftest reseed-remote-stage-flock-prevents-concurrent-receivers
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (let ((fd (pg::physical-backup-open-directory stage)))
         (unwind-protect
              (progn
                (ok (zerop (pg::physical-store-flock fd 6)))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
                (ok (zerop (getf state :discovery)))
                (ok (not (probe-file (pg::physical-reseed-attempt-path root)))))
           (sb-posix:close fd)))
       (ok (eq :received (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")))
       (ok (= 1 (getf state :captures))))))

(deftest reseed-remote-resolves-stage-aliases-before-checking-target-separation
    (dolist (kind '(:target :descendant))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (let ((saved (uiop:ensure-directory-pathname
                       (format nil "~a.saved/" (string-right-trim "/" (namestring stage)))))
               (referent (if (eq kind :target) root (merge-pathnames "preexisting-child/" root))))
           (ensure-directories-exist referent)
           (sb-posix:chmod (namestring referent) #o700)
           (rename-file stage saved)
           (unwind-protect
                (progn
                  (sb-posix:symlink (namestring referent) (string-right-trim "/" (namestring stage)))
                  (ok (intake-fails-p
                       (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
                  (ok (zerop (getf state :captures)))
                  (ok (zerop (getf state :discovery)))
                  (ok (not (probe-file (pg::physical-reseed-attempt-path root)))))
             (sb-posix:unlink (string-right-trim "/" (namestring stage)))
             (rename-file saved stage)))))))

(deftest reseed-remote-rechecks-stage-and-target-before-first-capture
    (dolist (kind '(:file :dangling :fifo :replacement :permissions :pid :journal))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (let* ((saved (uiop:ensure-directory-pathname
                        (format nil "~a.saved/" (string-right-trim "/" (namestring stage)))))
                (pid-path (merge-pathnames "postmaster.pid" root))
                (pg::*physical-reseed-remote-test-hook*
                  (lambda (point)
                    (when (eq point :before-capture)
                      (ecase kind
                        (:file (with-open-file (output (merge-pathnames "unexpected" stage) :direction :output)
                                 (write-string "preserve this entry" output)))
                        (:dangling (sb-posix:symlink "missing" (namestring (merge-pathnames "unexpected" stage))))
                        (:fifo (sb-posix:mkfifo (namestring (merge-pathnames "unexpected" stage)) #o600))
                        (:replacement
                         (rename-file stage saved)
                         (sb-posix:mkdir (string-right-trim "/" (namestring stage)) #o700))
                        (:permissions (sb-posix:chmod (namestring stage) #o755))
                        (:pid (reseed-test-write-pid root (sb-posix:getpid)))
                        (:journal
                         (let* ((old (pg::physical-reseed-attempt-read owner))
                                (next (copy-tree old)))
                           (setf (getf next :phase) :aborting)
                           (pg::physical-reseed-attempt-publish owner next :previous old))))))))
           (unwind-protect
                (progn
                  (ok (intake-fails-p
                       (lambda () (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper"))))
                  (ok (zerop (getf state :captures)))
                  (ok (= 2 (getf state :closes)))
                  (when (eq kind :pid)
                    (ok (probe-file pid-path))
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper"))))
                    (ok (zerop (getf state :aborts)))
                    (delete-file pid-path))
                  (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
                  (ok (zerop (getf state :captures))))
             (when (probe-file pid-path) (delete-file pid-path))
             (when (eq kind :permissions) (sb-posix:chmod (namestring stage) #o700))
             (when (probe-file saved)
               (uiop:delete-directory-tree stage :validate t)
               (rename-file saved stage))))))))

(deftest reseed-remote-retries-source-abort-after-losing-local-acknowledgment
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (declare (ignore root))
       (let ((pg::*physical-reseed-remote-test-hook*
               (lambda (point) (when (eq point :after-journal) (throw 'save-journal :saved)))))
         (ok (eq :saved
                 (catch 'save-journal
                   (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")))))
       (let ((pg::*physical-reseed-remote-test-hook*
               (lambda (point) (when (eq point :after-abort) (error "lost target-side abort acknowledgment")))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper")))))
       (ok (eq :aborting (getf (pg::physical-reseed-attempt-read owner) :phase)))
       (ok (= 1 (getf state :aborts)))
       (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
       (ok (= 2 (getf state :aborts)))
       (ok (intake-fails-p (lambda () (pg::physical-reseed-status owner :helper "/unused/helper"))))
       (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
       (ok (= 2 (getf state :aborts)))
       (ok (zerop (getf state :captures))))))

(deftest reseed-remote-does-not-mark-aborted-from-an-incoherent-source-reply
    (dolist (kind '(:unknown :ready :foreign :handed-off))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (declare (ignore root))
         (let ((pg::*physical-reseed-remote-test-hook*
                 (lambda (point) (when (eq point :after-journal) (throw 'save-journal :saved)))))
           (catch 'save-journal
             (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")))
         (setf (getf state :abort-action)
               (lambda (&rest arguments)
                 (declare (ignore arguments))
                 (ecase kind
                   (:unknown nil)
                   (:ready (backup-lease-test-record :ready))
                   (:foreign
                    (let ((receipt (reseed-attempt-test-abort-receipt (list :request request))))
                      (setf (getf (getf receipt :request) :token) (make-string 64 :initial-element #\b))
                      receipt))
                   (:handed-off
                    (let ((receipt (backup-lease-test-record :ready)))
                      (setf (getf receipt :phase) :closed
                            (getf receipt :outcome) :handed-off
                            (getf receipt :successor) (list :name "other_receiver" :anchor-digest 77))
                      receipt)))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-cancel owner :helper "/unused/helper"))))
         (ok (eq :aborting (getf (pg::physical-reseed-attempt-read owner) :phase)))
         (setf (getf state :abort-action) nil)
         (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
         (ok (zerop (getf state :captures)))))))

(deftest reseed-remote-stage-replacement-during-file-header-cannot-write-old-target
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (with-physical-store-directory (source)
         (let* ((name "PG_VERSION")
                (source-path (merge-pathnames name source))
                (redirected (merge-pathnames name root))
                (saved (uiop:ensure-directory-pathname
                        (format nil "~a.receiving/" (string-right-trim "/" (namestring stage)))))
                (frames nil)
                (replaced nil))
           (with-open-file (output source-path :direction :output)
             (write-line "17" output))
           (let ((bytes (lineage-test-bytes source-path)))
             (pg::physical-backup-send-file
              source
              (list name (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
              1 (lambda (frame) (push (copy-seq frame) frames))))
           (setf frames (nreverse frames)
                 (getf state :capture-write-witness) nil
                 (getf state :capture-action)
                 (lambda (directory client original)
                   (declare (ignore client original))
                   (flet ((receive (anchored)
                            (pg::physical-backup-receive-file
                             anchored name 1024
                             (lambda ()
                               (unless replaced
                                 (rename-file stage saved)
                                 (sb-posix:symlink
                                  (namestring root) (string-right-trim "/" (namestring stage)))
                                 (setf replaced t))
                               (pop frames)))))
                     (if (getf state :capture-fd)
                         (pg::call-with-physical-backup-receive-directory
                          (getf state :capture-fd) #'receive)
                         (receive directory)))))
           (unwind-protect
                (progn
                  (ok (intake-fails-p
                       (lambda ()
                         (pg::physical-reseed-start-remote
                          owner stage request profile :helper "/unused/helper"))))
                  (ok replaced)
                  (ok (= 1 (getf state :captures)))
                  (ok (not (probe-file redirected))
                      "receipt never creates a file in the preserved old target")
                  (ok (= 2 (getf state :closes)))
                  (ok (eq :aborted
                          (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase))))
             (when replaced
               (sb-posix:unlink (string-right-trim "/" (namestring stage)))
               (rename-file saved stage))
             (when (probe-file redirected) (delete-file redirected))))))))
