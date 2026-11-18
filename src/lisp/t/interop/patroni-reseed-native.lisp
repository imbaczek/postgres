(in-package #:pg)

(defun native-patroni-reseed-activate (target workspace port helper)
  (declare (ignore port))
  (physical-reseed-follow-configure-and-activate
   target workspace (postgres-current-timestamp) :helper helper))

(defun native-patroni-reseed-retire (target port)
  (physical-reseed-follow-retire-offline target port))

(defun native-patroni-reseed-finalize (target port)
  (physical-reseed-follow-finalize-offline target port))

(defun native-patroni-rewind-shared-history (target origin-directory source-directory expected-fork)
  (multiple-value-bind (manifest digest) (recovery-read-manifest origin-directory)
    (let ((identity (list (getf manifest :system-id) (getf manifest :timeline)
                          (getf manifest :end) digest))
          (source-fd (physical-backup-open-directory source-directory)))
      (unwind-protect
           (call-with-physical-adoption-owner
            target
            (lambda (owner)
              (call-with-physical-backup-image-directory
               source-fd
               (lambda (source-root)
                 (let* ((image (physical-reseed-image-check source-root))
                        (proof (physical-rewind-prove-shared-history owner source-root image identity)))
                   (assert (= (getf proof :fork) expected-fork))
                   (assert (< (getf proof :start) expected-fork
                              (min (getf proof :target-end) (getf proof :source-end))))
                   (format t "~&REWIND-SHARED-HISTORY PASS ~s~%" proof))))))
        (sb-posix:close source-fd)))))

(defun native-patroni-rewind-check-reuse (owner manifest reused)
  (let* ((target (physical-rewind-inspect-primary owner))
         (database (gethash "postgres" (catalog-databases (getf target :catalog))))
         (tables (database-tables database))
         (stable (gethash "rewind_reuse_payload" tables))
         (changed (gethash "patroni_reseed_probe" tables))
         (toast (pg-class-toast stable))
         (stable-files
           (mapcar (lambda (node) (format nil "base/~d" node))
                   (cons (pg-class-relfilenode stable)
                         (when toast (list (pg-toast-relfilenode toast)
                                           (pg-toast-index-relfilenode toast))))))
         (changed-file (format nil "base/~d" (pg-class-relfilenode changed))))
    (assert (>= (loop for (name size) in reused
                      when (member name stable-files :test #'equal) sum size)
                65536))
    (assert (assoc changed-file (getf manifest :files) :test #'equal))
    (assert (probe-file (merge-pathnames changed-file (physical-adoption-owner-directory owner))))
    (assert (not (assoc changed-file reused :test #'equal)))))

(defun native-patroni-rewind-leased-origin (target origin-directory source-directory port helper fork digest)
  (multiple-value-bind (manifest manifest-digest) (recovery-read-manifest origin-directory)
    (let* ((identity (list (getf manifest :system-id) (getf manifest :timeline)
                           (getf manifest :end) manifest-digest))
           (request (list :version 1 :name "reseed_10101010101010101010101010101010"
                          :token (make-string 64 :initial-element #\a)
                          :system-id (first identity)
                          :cluster-identity (getf manifest :cluster-identity) :timeline 2))
           (profile (make-physical-libpq-profile
                     (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres connect_timeout=3" port)
                     :mode :physical))
           (client (physical-libpq-connect helper profile))
           (fd nil)
           (attempted nil)
           (complete nil))
      (unwind-protect
           (progn
             (setf fd (physical-backup-open-directory source-directory)
                   attempted t)
             (call-with-physical-adoption-owner
              target
              (lambda (owner)
                (multiple-value-bind (discovered discovered-fork timeline)
                    (physical-rewind-discover-history owner client)
                  (assert (equal identity discovered))
                  (assert (= fork discovered-fork))
                  (assert (= (getf request :timeline) timeline)))
                (multiple-value-bind (capture record reused)
                    (physical-rewind-capture-image owner source-directory fd client request)
                  (declare (ignore record))
                  (native-patroni-rewind-check-reuse owner capture reused)
                  (format t "~&REWIND-REUSED ~s~%" reused))
                (call-with-physical-backup-image-directory
                 fd
                 (lambda (source-root)
                   (let* ((image (physical-reseed-image-check source-root))
                          (proof (physical-rewind-prove-leased-origin
                                  owner source-root image identity client request)))
                     (assert (= (getf proof :fork) fork))
                     (assert (= (getf proof :fork-digest) digest))
                     (format t "~&REWIND-LEASED-ORIGIN PASS ~s~%" proof))))))
             (setf complete t))
        (unwind-protect
             (unwind-protect (physical-libpq-close client)
               (when fd (sb-posix:close fd)))
          (when attempted
            (handler-case
                (let ((cleanup (physical-libpq-connect helper profile)))
                  (unwind-protect
                       (let ((record (pg-client-native-backup-lease-status cleanup request)))
                         (when complete (assert record))
                         (when record
                           (let ((closed (pg-client-native-backup-lease-command cleanup "BACKUP_LEASE_ABORT" request)))
                             (assert (and (eq :closed (getf closed :phase))
                                          (eq :aborted (getf closed :outcome)))))))
                    (physical-libpq-close cleanup)))
              (error (condition)
                (if complete
                    (error condition)
                    (format *error-output* "~&Rewind fixture lease cleanup failed: ~a~%" condition))))))))))

(declaim (ftype function native-cancellation-publish-selection))

(defun native-patroni-rewind-capture (target origin stage port helper)
  (multiple-value-bind (manifest digest) (recovery-read-manifest origin)
    (let ((identity (list (getf manifest :system-id) (getf manifest :timeline)
                          (getf manifest :end) digest))
          (profile (make-physical-libpq-profile
                    (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres" port)
                    :mode :physical :fallback "native-target-follow")))
      (call-with-physical-reseed-owner
       target
       (lambda (owner)
         (multiple-value-bind (capture record reused)
             (physical-rewind-start-discovered-remote owner stage profile :helper helper)
           (declare (ignore record))
           (assert (equal identity (getf (physical-reseed-attempt-recover owner) :rewind-origin)))
           (native-patroni-rewind-check-reuse owner capture reused))))
      (write-line "REWIND-JOURNALED-CAPTURE PASS"))))

(defun native-patroni-rewind-crash-after-capture ()
  (setf *physical-reseed-remote-test-hook*
        (lambda (point)
          (when (eq point :after-capture)
            (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))

(defun native-patroni-rewind-replay (target replay helper)
  (call-with-physical-reseed-owner
   target
   (lambda (owner)
     (let* ((*pgdata* (physical-adoption-owner-directory owner))
            (attempt (physical-reseed-attempt-read owner))
            (request (getf attempt :request))
            (stage (pathname (getf (getf attempt :stage) :directory)))
            (wrong (copy-tree request)))
       (setf (getf wrong :token)
             (make-string 64 :initial-element
                          (if (char= #\a (char (getf request :token) 0)) #\b #\a)))
       (call-with-physical-reseed-client
        owner attempt
        (lambda (client)
          (assert (handler-case
                      (progn
                        (physical-libpq-start-copy-out
                         client (pg-client-native-backup-lease-query "REPLAY_REWIND_NATIVE" wrong))
                        nil)
                    (pg-client-error (condition)
                      (equal "55000" (pg-client-error-sqlstate condition))))))
        :helper helper)
       (call-with-physical-reseed-stage
        owner replay
        (lambda (directory facts fd)
          (declare (ignore facts))
          (call-with-physical-reseed-client
           owner attempt
           (lambda (client)
             (multiple-value-bind (manifest record)
                 (physical-backup-lease-capture-remote
                  directory client request :directory-fd fd :replay t)
               (assert (eq :ready (getf record :phase)))
               (assert (equal (physical-backup-bundle-entries directory manifest)
                              (physical-backup-bundle-entries stage manifest)))))
           :helper helper))))))
  (write-line "REWIND-REPLAY PASS"))

(defun native-patroni-rewind-crash-before-capture ()
  (setf *physical-reseed-remote-test-hook*
        (lambda (point)
          (when (eq point :before-capture)
            (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))

(defun native-patroni-rewind-crash-after-install-step ()
  (setf *physical-reseed-install-test-hook*
        (lambda (point directory)
          (declare (ignore directory))
          (when (eq point :after-install-step-fsync)
            (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))

(defun native-patroni-rewind-crash-after-configuration-step ()
  (setf *physical-reseed-follow-test-hook*
        (lambda (point path)
          (declare (ignore path))
          (when (eq point :after-configuration-install-progress)
            (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))

(defun native-patroni-rewind-crash-after-retirement-move ()
  (setf *physical-reseed-follow-test-hook*
        (lambda (point path)
          (declare (ignore path))
          (when (eq point :after-retirement-archive-unlink)
            (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))

(defun native-patroni-rewind-install (target incoming quarantine helper)
  (call-with-physical-reseed-owner
   target
   (lambda (owner)
     (assert (getf (physical-reseed-attempt-recover owner) :rewind-origin))
     (physical-reseed-prepare-image owner incoming :helper helper)
     (physical-reseed-publish-install owner incoming quarantine :helper helper)))
  (let ((plan (getf (physical-reseed-install-initial-arguments target) :plan)))
    (call-with-physical-reseed-install-owner
     target
     (lambda (owner)
       (loop repeat (physical-reseed-install-last-step plan)
             do (physical-reseed-install-advance owner :helper helper)))))
  (physical-rewind-select-installed-follow target "native_target_follow_rewind")
  (write-line "REWIND-JOURNALED-INSTALL PASS"))
