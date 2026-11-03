(in-package #:pg)

;; Offline reseed receipt journals the original request, frozen source and
;; stage identity before one CAPTURE.  Reopened attempts can ask STATUS or
;; ABORT; they cannot issue another CAPTURE.  This layer does not install or
;; hand off an image, or coordinate a replacement server.

(defvar *physical-reseed-remote-test-hook* nil)

(defun physical-reseed-remote-test-point (point)
  (when *physical-reseed-remote-test-hook* (funcall *physical-reseed-remote-test-hook* point)))

(defun physical-reseed-require-journal (owner record)
  (physical-reseed-attempt-check-owner owner record)
  (unless (equal record (physical-reseed-attempt-read owner))
    (error "reseed journal changed during the remote operation"))
  record)

;; Match the private held directory to its name and, when supplied, the saved
;; identity.  Empty is required for a new destination, not for a received stage.

(defun physical-reseed-stage-facts (owner directory fd &key empty expected)
  (physical-reseed-check-owner owner)
  (let* ((directory (truename (uiop:ensure-directory-pathname directory)))
         (stat (sb-posix:fstat fd))
         (path-stat (sb-posix:lstat (string-right-trim "/" (namestring directory)))))
    (physical-image-check-directory fd)
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode path-stat))
                 (= (sb-posix:stat-dev stat) (sb-posix:stat-dev path-stat))
                 (= (sb-posix:stat-ino stat) (sb-posix:stat-ino path-stat)))
      (error "reseed receive stage pathname no longer names its owned inode"))
    (let ((facts (physical-reseed-attempt-stage-copy
                  (list :directory (namestring directory)
                        :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))
                  (physical-reseed-attempt-local-facts owner))))
      (when (and expected (not (equal expected facts)))
        (error "reseed receive stage identity changed"))
      (when (and empty (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t))
        (error "reseed receive stage is not empty"))
      facts)))

;; Hold an exclusive lock and check the private stage's pathname and descriptor
;; identities.  New destinations must be empty; a saved stage may be nonempty.

(defun call-with-physical-reseed-stage (owner directory function &key (empty t) expected)
  (physical-reseed-check-owner owner)
  (let ((directory (truename (uiop:ensure-directory-pathname directory))) (fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (unless (zerop (physical-store-flock fd 6))
             (error "reseed receive stage is already owned"))
           (let ((facts (physical-reseed-stage-facts owner directory fd :empty empty :expected expected)))
             (multiple-value-prog1 (funcall function directory facts fd)
               (physical-reseed-stage-facts owner directory fd :expected facts))))
      (when fd (sb-posix:close fd)))))

(defun physical-reseed-source-identity (client request &key current-timeline)
  (let ((row (physical-client-identity-row client)))
    (unless (and (= (pg-client-native-u64 (aref row 0)) (getf request :system-id))
                 (let ((timeline (pg-client-native-u64 (aref row 1) t)))
                   (or (not current-timeline) (= timeline (getf request :timeline)))))
      (error "reseed source identity or capture timeline changed")))
  t)

;; Identify the source and capture timeline before freezing its connection
;; facts, as base backup identifies its source before starting the backup.

(defun physical-reseed-discover-source (request profile helper)
  (unless (eq :physical (physical-libpq-profile-mode profile))
    (error "reseed requires a physical replication profile"))
  (let ((client nil) (resolved nil))
    (unwind-protect
         (progn
           (setf resolved (or helper (physical-libpq-helper-path
                                      :on-spawn (lambda (actor) (setf client actor))))
                 client (physical-libpq-connect resolved profile
                                                :on-spawn (lambda (actor) (setf client actor))))
           (physical-reseed-source-identity client request :current-timeline t)
           (values (physical-adoption-attempt-accepted-source client profile) resolved))
      (when client (physical-client-close client)))))

;; Reconnect using the saved route and credential owner, then recheck the
;; journal and source before the callback.  Track the helper as soon as it is
;; spawned so every exit closes the owned client.

(defun call-with-physical-reseed-client (owner record function &key helper capture)
  (physical-reseed-require-journal owner record)
  (let ((client nil))
    (unwind-protect
         (let ((frozen (nth-value 1 (physical-follow-source-profile (getf record :source)))))
           (setf client
                 (physical-libpq-connect-frozen
                  (or helper (physical-libpq-helper-path :on-spawn (lambda (actor) (setf client actor))))
                  frozen :on-spawn (lambda (actor) (setf client actor))))
           (physical-reseed-remote-test-point :after-connect)
           (physical-adoption-attempt-check-client client (getf record :source))
           (physical-reseed-source-identity client (getf record :request) :current-timeline capture)
           (physical-reseed-require-journal owner record)
           (funcall function client))
      (when client (physical-client-close client)))))

(defun physical-reseed-start-remote (owner directory request profile &key helper)
  (physical-reseed-check-owner owner)
  (let* ((request (physical-backup-lease-request-copy request))
         (*pgdata* (physical-adoption-owner-directory owner)))
    (physical-reseed-check-before-pid)
    (pg-client-native-backup-lease-query "CAPTURE_BACKUP_NATIVE" request)
    (call-with-physical-reseed-stage
     owner directory
     (lambda (stage facts fd)
       (multiple-value-bind (source resolved) (physical-reseed-discover-source request profile helper)
         (physical-reseed-remote-test-point :after-discovery)
         (physical-reseed-stage-facts owner stage fd :empty t :expected facts)
         (let ((record (physical-reseed-attempt-publish
                        owner (list :version 1 :phase :premanifest
                                    :local (physical-reseed-attempt-local-facts owner)
                                    :request request :source source :stage facts))))
           (physical-reseed-remote-test-point :after-journal)
           (call-with-physical-reseed-client
            owner record
            (lambda (client)
              (physical-reseed-remote-test-point :before-capture)
              (physical-reseed-require-journal owner record)
              (physical-reseed-stage-facts owner stage fd :empty t :expected facts)
              (multiple-value-prog1 (physical-backup-lease-capture-remote stage client (getf record :request)
                                                                          :directory-fd fd)
                (physical-reseed-remote-test-point :after-capture)
                (physical-reseed-require-journal owner record)
                (physical-reseed-stage-facts owner stage fd :expected facts)))
            :helper resolved :capture t)))))))

(defun physical-reseed-source-result (record source)
  (let ((source (when source (physical-backup-lease-copy source))))
    (when (and source (not (equal (getf record :request) (getf source :request))))
      (error "reseed source reply belongs to a different request"))
    (when (and (eq :aborted (getf record :phase))
               (not (and (eq :closed (getf source :phase)) (eq :aborted (getf source :outcome)))))
      (error "reseed source lost its acknowledged abort tombstone"))
    source))

(defun physical-reseed-status (owner &key helper)
  (let ((record (physical-reseed-attempt-recover owner)))
    (call-with-physical-reseed-client
     owner record
     (lambda (client)
       (physical-reseed-remote-test-point :before-status)
       (physical-reseed-require-journal owner record)
       (physical-reseed-source-result
        record (pg-client-native-backup-lease-status client (getf record :request))))
     :helper helper)))

;; Persist abort intent before contacting the source.  Remote cancellation does
;; not need a healthy stage or the current timeline, but checks the same system
;; identifier.  An already-aborted attempt returns locally.  Neither path
;; removes the local startup fence.

(defun physical-reseed-cancel (owner &key helper)
  (let ((record (physical-reseed-attempt-recover owner)))
    (when (eq :aborted (getf record :phase)) (return-from physical-reseed-cancel record))
    (unless (eq :aborting (getf record :phase))
      (let ((next (copy-list record)))
        (setf (getf next :phase) :aborting
              record (physical-reseed-attempt-publish owner next :previous record))))
    (call-with-physical-reseed-client
     owner record
     (lambda (client)
       (physical-reseed-remote-test-point :before-abort)
       (physical-reseed-require-journal owner record)
       (let ((source (physical-reseed-source-result
                      record (pg-client-native-backup-lease-abort client (getf record :request)))))
         (unless (and (member (getf source :phase) '(:closing :closed)) (eq :aborted (getf source :outcome)))
           (error "reseed source did not confirm cancellation"))
         (physical-reseed-remote-test-point :after-abort)
         (if (eq :closed (getf source :phase))
             (let ((next (copy-list record)))
               (setf (getf next :phase) :aborted)
               (physical-reseed-attempt-publish owner next :previous record :receipt source))
             record)))
     :helper helper)))
