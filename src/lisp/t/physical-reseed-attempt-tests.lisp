(in-package #:pg-physical-tests)

(defun reseed-attempt-test-record (&optional owner)
  (let* ((info (pg::physical-libpq-decode-connection-info
                (libpq-test-connection-body
                 '(("replication" "true") ("password" "journal-secret")) "replication")))
         (pid (when owner (pg::physical-reseed-owner-old-pid owner))))
    (list :version 1 :phase :premanifest
          :local (list :directory (if owner (namestring (pg::physical-adoption-owner-directory owner))
                                      "/tmp/reseed-attempt/")
                       :device (if owner (pg::physical-adoption-owner-device owner) 1)
                       :inode (if owner (pg::physical-adoption-owner-inode owner) 2)
                       :lock-device (if owner (pg::physical-adoption-owner-lock-device owner) 1)
                       :lock-inode (if owner (pg::physical-adoption-owner-lock-inode owner) 3)
                       :pid (when pid
                              (list :pid (getf pid :pid) :identity (copy-list (getf pid :identity))
                                    :bytes (pg::physical-libpq-octets-datum (getf pid :bytes)))))
          :request (backup-lease-test-request)
          :source (list :conninfo "host=localhost password=journal-secret" :fallback "reseed"
                        :owner (pg::physical-libpq-frozen-profile-datum
                                (pg::make-physical-libpq-frozen-profile info))))))

(cl-spec:defproperty reseed-attempt-codec-preserves-intent-and-private-ownership
    ((number (range integer 0 18446744073709551615)) (phase (range integer 0 2)))
  (:about pg::physical-reseed-attempt-copy pg::physical-reseed-attempt-decode)
  (:trials (:normal 100))
  (let ((record (reseed-attempt-test-record)))
    (setf (getf record :phase) (nth phase '(:premanifest :aborting :aborted))
          (getf (getf record :local) :lock-inode) number)
    (let ((checked (pg::physical-reseed-attempt-copy record)))
      (equal checked (pg::physical-reseed-attempt-decode
                      (pg::physical-reseed-attempt-image checked))))))

(deftest generated-reseed-attempt-codec-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-attempt-codec-preserves-intent-and-private-ownership
                                         :seed 94 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-attempt-codec-refuses-rebinding-and-reversing-cancellation
    (let* ((record (reseed-attempt-test-record))
           (aborting (copy-tree record))
           (aborted (copy-tree record)))
      (setf (getf aborting :phase) :aborting (getf aborted :phase) :aborted)
      (ok (pg::physical-reseed-attempt-check-transition record record))
      (ok (pg::physical-reseed-attempt-check-transition record aborting))
      (ok (pg::physical-reseed-attempt-check-transition aborting aborted))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-check-transition record aborted))))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-check-transition aborted record))))
      (dolist (key '(:local :source :request))
        (let ((changed (copy-tree record)))
          (ecase key
            (:local (incf (getf (getf changed :local) :lock-inode)))
            (:source (setf (getf (getf changed :source) :fallback) "different"))
            (:request (setf (getf (getf changed :request) :token) (make-string 64 :initial-element #\b))))
          (ok (intake-fails-p
               (lambda () (pg::physical-reseed-attempt-check-transition record changed))))))))

(deftest reseed-attempt-codec-rejects-damage-noncanonical-and-unsafe-fields
    (let* ((record (reseed-attempt-test-record))
           (bytes (pg::physical-reseed-attempt-image record))
           (bad (copy-seq bytes)))
      (setf (aref bad (1- (length bad))) 0)
      (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-decode bad))))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-attempt-decode
                       (make-array (1+ pg::+physical-reseed-attempt-maximum-bytes+)
                                   :element-type '(unsigned-byte 8))))))
      (dolist (key '(:version :phase :local :request :source))
        (let ((bad (copy-tree record)))
          (setf (getf bad key) :invalid)
          (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy bad))))))
      (let ((bad (copy-tree record)))
        (setf (getf bad :source) (libpq-test-source "database-mode" 5439))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy bad)))))
      (let ((bad (copy-tree record)))
        (setf (getf (getf bad :local) :directory) "../relative/")
        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy bad)))))))

(deftest reseed-attempt-persists-private-intent-and-reopens-under-stopped-ownership
    (call-with-reaped-reseed-pid
     (lambda (pid)
       (call-with-reseed-owner-directory
        (lambda (root)
          (let* ((path (reseed-test-write-pid root (- pid)))
                 (pid-bytes (lineage-test-bytes path))
                 (pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
                 (saved nil))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (setf saved (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))
               (ok (equal saved (pg::physical-reseed-attempt-read owner)))
               (ok (= #o600 (logand #o7777
                                    (sb-posix:stat-mode
                                     (sb-posix:lstat (namestring (pg::physical-reseed-attempt-path root)))))))
               (ok (intake-fails-p
                    (lambda () (let ((pg::*pgdata* root)) (pg::physical-reseed-check-before-pid)))))))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (ok (equal saved (pg::physical-reseed-attempt-recover owner)))
               (dolist (phase '(:aborting :aborted))
                 (let ((next (copy-tree saved)))
                   (setf (getf next :phase) phase
                         saved (pg::physical-reseed-attempt-publish
                                owner next :previous saved
                                :receipt (when (eq phase :aborted) (reseed-attempt-test-abort-receipt saved))))))
               (ok (eq :aborted (getf (pg::physical-reseed-attempt-read owner) :phase)))
               (ok (intake-fails-p
                    (lambda () (let ((pg::*pgdata* root)) (pg::physical-reseed-check-before-pid)))))))
            (ok (equalp pid-bytes (lineage-test-bytes path)))
            (ok (= pid-inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))))))))

(deftest reseed-temporary-intent-fences-real-startup-before-pid-inspection
    (dolist (kind '(:file :symlink :directory :fifo))
      (with-physical-store-directory (root)
        (call-with-adoption-postmaster-fixture
         root
         (lambda ()
           (let ((pid (merge-pathnames "postmaster.pid" root))
                 (temp (pg::physical-reseed-attempt-temporary-path root)))
             (with-open-file (stream pid :direction :output :if-exists :error)
               (write-string "opaque old PID bytes" stream))
             (ecase kind
               (:file (with-open-file (stream temp :direction :output) (write-string "partial" stream)))
               (:symlink (sb-posix:symlink "missing" (namestring temp)))
               (:directory (sb-posix:mkdir (namestring temp) #o700))
               (:fifo (sb-posix:mkfifo (namestring temp) #o600)))
             (let ((before (lineage-test-bytes pid))
                   (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::postmaster-own-data-directory
                       "127.0.0.1" 5439 :startup-before-pid #'pg::physical-reseed-check-before-pid))))
               (ok (equalp before (lineage-test-bytes pid)))
               (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
               (ok (null pg::*postmaster-pid-stream*))
               (ok (null pg::*postmaster-data-lock*)))))))))

(deftest reseed-attempt-publication-cuts-preserve-the-original-intent-or-fence
    (dolist (point '(:after-create :after-write :after-file-fsync :before-rename
                     :after-rename :after-directory-fsync :before-readback :after-readback))
      (dolist (mode '(:error :nonlocal))
        (call-with-reseed-owner-directory
         (lambda (root)
           (pg::call-with-physical-reseed-owner
            root
            (lambda (owner)
              (let* ((record (reseed-attempt-test-record owner))
                     (fired nil)
                     (pg::*physical-reseed-attempt-test-hook*
                       (lambda (where path)
                         (declare (ignore path))
                         (when (eq point where)
                           (setf fired t)
                           (if (eq mode :error) (error "injected reseed publication failure")
                               (throw 'reseed-cut :cut))))))
                (ok (eq :cut
                        (catch 'reseed-cut
                          (handler-case (pg::physical-reseed-attempt-publish owner record)
                            (error () :cut)))))
                (ok fired)
                (let ((pg::*physical-reseed-attempt-test-hook* nil)
                      (pg::*pgdata* root))
                  (ok (intake-fails-p #'pg::physical-reseed-check-before-pid))
                  (if (eq point :after-create)
                      (progn
                        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-recover owner))))
                        (ok (intake-fails-p
                             (lambda () (pg::physical-reseed-attempt-publish owner record)))))
                      (ok (equal record (pg::physical-reseed-attempt-recover owner)))))
                (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))))))))))

(deftest reseed-attempt-cancellation-publication-cuts-never-rebind-the-request
    (dolist (point '(:after-create :after-write :after-file-fsync :before-rename :after-rename
                     :after-directory-fsync :before-readback :after-readback))
      (call-with-reseed-owner-directory
       (lambda (root)
         (pg::call-with-physical-reseed-owner
          root
          (lambda (owner)
            (let* ((old (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))
                   (next (copy-tree old)))
              (setf (getf next :phase) :aborting)
              (let ((pg::*physical-reseed-attempt-test-hook*
                      (lambda (where path)
                        (declare (ignore path))
                        (when (eq point where) (error "injected cancellation intent failure")))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-publish owner next :previous old)))))
              (if (eq point :after-create)
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-recover owner))))
                  (ok (equal next (pg::physical-reseed-attempt-recover owner)))))))))))

(deftest reseed-attempt-reader-rejects-changed-pid-and-nonprivate-records
    (call-with-reaped-reseed-pid
     (lambda (pid)
       (call-with-reseed-owner-directory
        (lambda (root)
          (let ((pid-path (reseed-test-write-pid root pid)))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner))))
            (with-open-file (stream pid-path :direction :output :if-exists :append)
              (write-string "new PID evidence" stream))
            (pg::call-with-physical-reseed-owner
             root
             (lambda (owner)
               (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-read owner)))))))))))
  (dolist (kind '(:mode :hardlink :symlink :directory :fifo))
    (call-with-reseed-owner-directory
     (lambda (root)
       (pg::call-with-physical-reseed-owner
        root
        (lambda (owner)
          (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner))
          (let ((path (pg::physical-reseed-attempt-path root))
                (other (merge-pathnames "journal.saved" root)))
            (ecase kind
              (:mode (sb-posix:chmod (namestring path) #o644))
              (:hardlink (sb-posix:link (namestring path) (namestring other)))
              (:symlink
               (rename-file path other)
               (sb-posix:symlink (namestring other) (namestring path)))
              (:directory (delete-file path) (sb-posix:mkdir (namestring path) #o700))
              (:fifo (delete-file path) (sb-posix:mkfifo (namestring path) #o600)))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-read owner)))))))))))

(deftest reseed-attempt-pid-codec-preserves-opaque-bytes-and-copies-input
    (let* ((record (reseed-attempt-test-record))
           (bytes (make-array 8 :element-type '(unsigned-byte 8)
                                :initial-contents '(45 49 50 10 0 255 13 10)))
           (pid (list :pid 12 :identity '(1 2 33152 1 1000 8 -1 -1)
                      :bytes (pg::physical-libpq-octets-datum bytes))))
      (setf (getf (getf record :local) :pid) pid)
      (let* ((copy (pg::physical-reseed-attempt-copy record))
             (copied-pid (getf (getf copy :local) :pid)))
        (ok (equal copy (pg::physical-reseed-attempt-decode (pg::physical-reseed-attempt-image record))))
        (ok (equalp bytes (pg::physical-libpq-datum-octets (getf copied-pid :bytes))))
        (setf (first (getf pid :identity)) 9)
        (ok (= 1 (first (getf copied-pid :identity))))
        (setf (getf pid :pid) 13)
        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy record)))))))

(deftest reseed-attempt-envelope-refuses-valid-crc-noncanonical-and-reader-forms
    (let* ((record (reseed-attempt-test-record))
           (reordered (loop for key in '(:source :request :local :phase :version)
                            append (list key (getf record key))))
           (text (pg::physical-bootstrap-manifest-text reordered)))
      (dolist (body (list text (concatenate 'string text " nil") "#.(error \"reader evaluation\")"))
        (let ((image (pg::string->bytes
                      (format nil "PGRESEEDATTEMPT1 ~8,'0X~%~a~%"
                              (pg::wal-crc32 (pg::string->bytes body)) body))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-decode image))))))))

(deftest reseed-attempt-rechecks-journal-inodes-before-replacing-the-intent
    (dolist (kind '(:temporary :previous))
      (call-with-reseed-owner-directory
       (lambda (root)
         (pg::call-with-physical-reseed-owner
          root
          (lambda (owner)
            (let* ((record (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))
                   (next (copy-tree record))
                   (path (pg::physical-reseed-attempt-path root))
                   (temporary (pg::physical-reseed-attempt-temporary-path root))
                   (saved (merge-pathnames "original-journal.saved" root))
                   (before (lineage-test-bytes path))
                   (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
              (setf (getf next :phase) :aborting)
              (let ((pg::*physical-reseed-attempt-test-hook*
                      (lambda (point actual)
                        (declare (ignore actual))
                        (when (eq point :before-rename)
                          (let* ((victim (if (eq kind :temporary) temporary path))
                                 (image (lineage-test-bytes victim)))
                            (rename-file victim saved)
                            (with-open-file (stream victim :direction :output :if-exists :error
                                                           :element-type '(unsigned-byte 8))
                              (write-sequence image stream))
                            (sb-posix:chmod (namestring victim) #o600))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-publish owner next :previous record)))))
              (ok (equalp before (lineage-test-bytes path)))
              (when (eq kind :temporary)
                (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))))
              (ok (probe-file temporary)))))))))

(deftest reseed-attempt-freezes-callers-data-and-refuses-clean-adoption-overlap
    (call-with-reseed-owner-directory
     (lambda (root)
       (pg::call-with-physical-reseed-owner
        root
        (lambda (owner)
          (let* ((record (reseed-attempt-test-record owner))
                 (expected (pg::physical-reseed-attempt-copy record))
                 (pg::*physical-reseed-attempt-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (eq point :after-create)
                       (setf (getf (getf record :source) :fallback) "mutated"
                             (getf (getf record :request) :token) (make-string 64 :initial-element #\b))))))
            (ok (equal expected (pg::physical-reseed-attempt-publish owner record))))))))
  (call-with-reseed-owner-directory
   (lambda (root)
     (with-open-file (stream (pg::physical-adoption-attempt-path root) :direction :output)
       (write-string "unreconciled clean-adoption attempt" stream))
     (pg::call-with-physical-reseed-owner
      root
      (lambda (owner)
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))))
        (ok (not (probe-file (pg::physical-reseed-attempt-path root))))
        (ok (not (probe-file (pg::physical-reseed-attempt-temporary-path root)))))))))

(defun reseed-attempt-test-abort-receipt (record)
  (list :version 1 :request (pg::physical-backup-lease-request-copy (getf record :request))
        :phase :closed :outcome :aborted :base nil :successor nil))

(deftest reseed-attempt-aborted-publication-requires-the-original-closed-receipt
    (call-with-reseed-owner-directory
     (lambda (root)
       (pg::call-with-physical-reseed-owner
        root
        (lambda (owner)
          (let* ((old (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))
                 (next (copy-tree old)))
            (setf (getf next :phase) :aborting
                  old (pg::physical-reseed-attempt-publish owner next :previous old)
                  next (copy-tree old)
                  (getf next :phase) :aborted)
            (let* ((path (pg::physical-reseed-attempt-path root))
                   (before (lineage-test-bytes path))
                   (receipt (reseed-attempt-test-abort-receipt old))
                   (closing (copy-tree receipt))
                   (foreign (copy-tree receipt))
                   (ready (backup-lease-test-record :ready))
                   (handoff (backup-lease-test-record :ready)))
              (setf (getf closing :phase) :closing
                    (getf (getf foreign :request) :token) (make-string 64 :initial-element #\b)
                    (getf handoff :phase) :closed
                    (getf handoff :outcome) :handed-off
                    (getf handoff :successor) (list :name "receipt_successor" :anchor-digest 77))
              (ok (intake-fails-p
                   (lambda () (pg::physical-reseed-attempt-publish owner next :previous old))))
              (dolist (bad (list nil closing foreign ready handoff))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-attempt-publish owner next :previous old :receipt bad))))
                (ok (equalp before (lineage-test-bytes path)))
                (ok (not (probe-file (pg::physical-reseed-attempt-temporary-path root)))))
              (ok (intake-fails-p
                   (lambda () (pg::physical-reseed-attempt-write-temporary owner next))))
              (ok (not (probe-file (pg::physical-reseed-attempt-temporary-path root))))
              (setf (getf receipt :request)
                    (loop for key in '(:timeline :cluster-identity :system-id :token :name :version)
                          append (list key (getf (getf old :request) key))))
              (ok (equal next (pg::physical-reseed-attempt-publish owner next :previous old :receipt receipt)))
              (ok (eq :aborted (getf (pg::physical-reseed-attempt-read owner) :phase))))))))))

(deftest reseed-attempt-recovery-finishes-only-an-already-authorized-aborted-publication
    (dolist (cut '(:after-write :after-file-fsync :before-rename :after-rename
                   :after-directory-fsync :before-readback :after-readback))
      (call-with-reseed-owner-directory
       (lambda (root)
         (pg::call-with-physical-reseed-owner
          root
          (lambda (owner)
            (let* ((old (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner)))
                   (next (copy-tree old)))
              (setf (getf next :phase) :aborting
                    old (pg::physical-reseed-attempt-publish owner next :previous old)
                    next (copy-tree old)
                    (getf next :phase) :aborted)
              (let ((pg::*physical-reseed-attempt-test-hook*
                      (lambda (point path)
                        (declare (ignore path))
                        (when (eq point cut) (error "injected acknowledged abort publication failure")))))
                (ok (intake-fails-p
                     (lambda ()
                       (pg::physical-reseed-attempt-publish
                        owner next :previous old :receipt (reseed-attempt-test-abort-receipt old))))))
              (ok (equal next (pg::physical-reseed-attempt-recover owner)))
              (let ((pg::*pgdata* root))
                (ok (intake-fails-p #'pg::physical-reseed-check-before-pid))))))))))

(deftest reseed-attempt-recovers-real-sigkill-in-a-fresh-image
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "reseed-cache.XXXXXX" cache-root))))))
        (dolist (phase '(:premanifest :aborting :aborted))
          (dolist (point '(:after-create :after-write :after-file-fsync :before-rename
                           :after-rename :after-directory-fsync :before-readback :after-readback))
            (call-with-reaped-reseed-pid
             (lambda (pid)
               (call-with-reseed-owner-directory
                (lambda (root)
                  (let* ((pid-path (reseed-test-write-pid root pid))
                         (old-bytes (lineage-test-bytes pid-path))
                         (old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid-path))))
                         (log (merge-pathnames "reseed-child.log" cache-root))
                         (record nil))
                    (pg::call-with-physical-reseed-owner
                     root (lambda (owner) (setf record (reseed-attempt-test-record owner))))
                    (let ((result
                            (backup-lease-store-run-child
                             root log
                             (format nil
                                     "(pg::call-with-physical-reseed-owner #P~s
                                        (lambda (owner)
                                          (let* ((record (pg::physical-reseed-attempt-copy '~a))
                                                 (previous (unless (eq ~s :premanifest)
                                                             (pg::physical-reseed-attempt-publish owner record))))
                                            (when (eq ~s :aborted)
                                              (setf (getf record :phase) :aborting
                                                    previous (pg::physical-reseed-attempt-publish
                                                              owner record :previous previous)))
                                            (setf (getf record :phase) ~s
                                                  pg::*physical-reseed-attempt-test-hook*
                                                  (lambda (where path)
                                                    (declare (ignore path))
                                                    (when (eq where ~s)
                                                      (sb-posix:kill (sb-posix:getpid) 9))))
                                            (pg::physical-reseed-attempt-publish
                                             owner record :previous previous :receipt '~a))))"
                                     (namestring root) (pg::physical-bootstrap-manifest-text record)
                                     phase phase phase point
                                     (pg::physical-bootstrap-manifest-text
                                      (reseed-attempt-test-abort-receipt record))) cache)))
                      (ok (equal '(:signaled 9) result) (format nil "~s/~s ~a" phase point log)))
                    (let ((result
                            (backup-lease-store-run-child
                             root log
                             (format nil
                                     "(assert (handler-case (progn (pg::physical-reseed-check-before-pid) nil)
                                                (error (condition)
                                                  (search \"pending offline reseed\" (princ-to-string condition)))))
                                      (pg::call-with-physical-reseed-owner #P~s
                                        (lambda (owner)
                                          (if (eq ~s :after-create)
                                              (assert (handler-case
                                                          (progn (pg::physical-reseed-attempt-recover owner) nil)
                                                        (error () t)))
                                              (assert (eq ~s (getf (pg::physical-reseed-attempt-recover owner) :phase))))))
                                      (assert (handler-case (progn (pg::physical-reseed-check-before-pid) nil)
                                                (error () t)))"
                                     (namestring root) point phase) cache)))
                      (ok (equal '(:exited 0) result) (format nil "fresh reopen ~s/~s ~a" phase point log)))
                    (ok (equalp old-bytes (lineage-test-bytes pid-path)))
                    (ok (= old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid-path)))))))))))))))
