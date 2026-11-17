(in-package #:pg)

(defun native-patroni-reseed-activate (target workspace port helper)
  (let ((*pgdata* target))
    (call-with-physical-reseed-follow-configuration-owner
     target
     (lambda (owner)
       (physical-reseed-follow-begin-configuration owner workspace port)
       (physical-reseed-follow-stage-configuration owner)
       (loop repeat 8
             for record = (physical-reseed-follow-advance-configuration owner)
             until (eq :inert (getf (getf record :configuration) :phase))
             finally (assert (eq :inert (getf (getf record :configuration) :phase))))
       (physical-reseed-follow-settle owner :helper helper)
       (physical-reseed-follow-begin-activation owner)
       (physical-reseed-follow-stage-activation owner)
       (physical-reseed-follow-start-activation owner :helper helper)
       (loop repeat 4
             for record = (physical-reseed-follow-advance-activation owner)
             until (eq :active (getf (getf record :activation) :phase))
             finally (assert (eq :active (getf (getf record :activation) :phase))))))))

(defun native-patroni-reseed-retire (target port)
  (let ((*pgdata* target)
        (before nil)
        (after nil))
    (unwind-protect
         (postmaster-own-data-directory
          "127.0.0.1" port
          :startup-observer
          (lambda (lock phase)
            (if (eq phase :before-pid)
                (setf before (physical-reseed-follow-startup-observation lock phase))
                (setf after (physical-reseed-follow-startup-observation lock phase before))))
          :startup-reconcile
          (lambda ()
            (call-with-physical-reseed-follow-retirement-startup-owner
             after
             (lambda (owner)
               (physical-reseed-follow-retirement-publish-initial owner)
               (physical-reseed-follow-retirement-archive-install owner)
               (physical-reseed-follow-retirement-archive-attempt owner)
               (physical-reseed-follow-retirement-publish-terminal owner)))))
      (when *postmaster-pid-stream* (postmaster-remove-pid-file))))
  (values))

(defun native-patroni-reseed-finalize (target port)
  (let ((*pgdata* target))
    (unwind-protect
         (progn
           ;; Consume the receipt before creating ordinary configuration under the data lock.
           (physical-reseed-follow-own-startup "127.0.0.1" port)
           (physical-reseed-follow-check-ordinary-startup)
           (let ((path (merge-pathnames "postgresql.conf" target)))
             (assert (not (probe-file path)))
             (wal-atomic-write-bytes path (make-array 0 :element-type '(unsigned-byte 8)))
             (fsync-directory target)))
      (unwind-protect
           (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock*
          (close *postmaster-data-lock*)
          (setf *postmaster-data-lock* nil)))))
  (values))

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
          (request (list :version 1 :name "reseed_20202020202020202020202020202020"
                         :token (make-string 64 :initial-element #\b)
                         :system-id (getf manifest :system-id)
                         :cluster-identity (getf manifest :cluster-identity) :timeline 2))
          (profile (make-physical-libpq-profile
                    (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres" port)
                    :mode :physical :fallback "native-target-follow")))
      (call-with-physical-reseed-owner
       target
       (lambda (owner)
         (multiple-value-bind (capture record reused)
             (physical-rewind-start-remote owner stage request profile identity :helper helper)
           (declare (ignore record))
           (native-patroni-rewind-check-reuse owner capture reused))))
      (write-line "REWIND-JOURNALED-CAPTURE PASS"))))

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
  (call-with-physical-reseed-follow-owner
   target
   (lambda (owner)
     (physical-reseed-follow-publish-intent owner (physical-reseed-follow-initial-record owner))))
  (native-cancellation-publish-selection target "native_target_follow_rewind")
  (write-line "REWIND-JOURNALED-INSTALL PASS"))
