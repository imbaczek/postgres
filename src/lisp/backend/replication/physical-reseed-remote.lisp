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

(defun call-with-physical-reseed-discovery (profile helper function)
  (unless (eq :physical (physical-libpq-profile-mode profile))
    (error "reseed requires a physical replication profile"))
  (let ((client nil))
    (unwind-protect
         (let ((resolved (or helper (physical-libpq-helper-path
                                     :on-spawn (lambda (actor) (setf client actor))))))
           (setf client (physical-libpq-connect resolved profile
                                                :on-spawn (lambda (actor) (setf client actor))))
           (funcall function client resolved))
      (when client (physical-client-close client)))))

(defun physical-reseed-discover-source (request profile helper)
  (call-with-physical-reseed-discovery
   profile helper
   (lambda (client resolved)
     (physical-reseed-source-identity client request :current-timeline t)
     (values (physical-adoption-attempt-accepted-source client profile) resolved))))

;; Reconnect using the saved route and credential owner, then recheck the
;; journal and source before the callback.  Track the helper as soon as it is
;; spawned so every exit closes the owned client.

(defun call-with-physical-reseed-client (owner record function &key helper capture)
  (physical-reseed-check-attempt-authority owner)
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

(defun %physical-reseed-start-remote (owner directory request profile &key helper stage-check rewind-origin accepted-source)
  ;; The optional check adds caller-specific observations at each stage check.
  ;; Publish the ordinary attempt journal before CAPTURE so a reopened attempt
  ;; can use status or cancellation without issuing CAPTURE again.
  (physical-reseed-check-owner owner)
  (let* ((request (physical-backup-lease-request-copy request))
         (*pgdata* (physical-adoption-owner-directory owner)))
    (physical-reseed-check-before-pid)
    (pg-client-native-backup-lease-query "CAPTURE_BACKUP_NATIVE" request)
    (call-with-physical-reseed-stage
     owner directory
     (lambda (stage facts fd)
       (flet ((check-stage (empty)
                (physical-reseed-stage-facts owner stage fd :empty empty :expected facts)
                (when stage-check (funcall stage-check stage fd))))
         (check-stage t)
         (multiple-value-bind (source resolved) (if accepted-source
                                                    (values accepted-source helper)
                                                    (physical-reseed-discover-source request profile helper))
           (physical-reseed-remote-test-point :after-discovery)
           (check-stage t)
           (let ((record (physical-reseed-attempt-publish
                          owner (append (list :version 1 :phase :premanifest
                                              :local (physical-reseed-attempt-local-facts owner)
                                              :request request :source source :stage facts)
                                        (when rewind-origin (list :rewind-origin rewind-origin))))))
             (physical-reseed-remote-test-point :after-journal)
             (call-with-physical-reseed-client
              owner record
              (lambda (client)
                (physical-reseed-remote-test-point :before-capture)
                (physical-reseed-require-journal owner record)
                (check-stage t)
                (multiple-value-prog1
                    (if (getf record :rewind-origin)
                        (physical-rewind-capture-image owner stage fd client (getf record :request))
                        (physical-backup-lease-capture-remote stage client (getf record :request)
                                                              :directory-fd fd))
                  (physical-reseed-remote-test-point :after-capture)
                  (physical-reseed-require-journal owner record)
                  (check-stage nil)))
              :helper resolved :capture t))))))))

(defun physical-reseed-start-remote (owner directory request profile &key helper)
  ;; Start capture with the ordinary stage validation and durable journal.
  (%physical-reseed-start-remote owner directory request profile :helper helper))

(declaim (ftype function physical-rewind-capture-image))

(defun physical-rewind-discover-history (owner client)
  (let* ((target (physical-rewind-inspect-primary owner))
         (system (getf target :system-id))
         (timeline (getf target :timeline)))
    (multiple-value-bind (origin fork child) (pg-client-native-rewind-origin client)
      (unless (and (= system (first origin)) (= timeline (second origin))
                   (< (third origin) fork (getf target :end)))
        (error "source rewind origin does not match the divergent target"))
      (let* ((history (pg-client-native-history client child))
             (child-history (timeline-parse-history child (bytes->string history))))
        (multiple-value-bind (common shared-end)
            (timeline-common-ancestor (getf target :history) child-history)
          (unless (and (= common timeline) (eql shared-end fork)
                       (= (timeline-history-entry-begin (first child-history)) fork)
                       (second child-history)
                       (= (timeline-history-entry-timeline (second child-history)) timeline))
            (error "source rewind origin does not match its timeline history"))))
      (physical-prove-clean-primary-origin target origin #'physical-rewind-check-reader)
      (physical-reseed-source-identity client (list :system-id system :timeline child)
                                       :current-timeline t)
      (values origin fork child))))

(defun physical-rewind-start-remote (owner directory request profile origin &key helper)
  "Start a remote rewind capture for a clean stopped target.
Record ORIGIN in the attempt journal before capture. Preparation and install
publication verify that historical export identity against the source."
  (physical-reseed-check-owner owner)
  (unless origin (error "native rewind requires its historical export identity"))
  (physical-rewind-inspect-primary owner)
  (%physical-reseed-start-remote
   owner directory request profile :helper helper :rewind-origin origin))

(defun physical-rewind-start-discovered-remote (owner directory profile &key helper)
  (physical-reseed-check-owner owner)
  (multiple-value-bind (origin request source resolved)
      (call-with-physical-reseed-discovery
       profile helper
       (lambda (client resolved)
         (multiple-value-bind (origin fork child) (physical-rewind-discover-history owner client)
           (declare (ignore fork))
           (let* ((target (physical-rewind-inspect-primary owner))
                  (request
                    (list :version 1
                          :name (string-downcase (format nil "reseed_~32,'0x" (logical-slot-new-generation)))
                          :token (string-downcase
                                  (format nil "~32,'0x~32,'0x" (logical-slot-new-generation)
                                          (logical-slot-new-generation)))
                          :system-id (getf target :system-id)
                          :cluster-identity (getf target :cluster-identity)
                          :timeline child)))
             (values origin request (physical-adoption-attempt-accepted-source client profile) resolved)))))
    (%physical-reseed-start-remote
     owner directory request profile :helper resolved :rewind-origin origin :accepted-source source)))

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
  ;; This module loads before the cancellation recovery module and its shared
  ;; filesystem and writer dependencies.
  (declare (ftype (function (t) t) physical-reseed-attempt-recover-for-cancel))
  (let ((record (physical-reseed-attempt-recover-for-cancel owner)))
    (when (eq :aborted (getf record :phase)) (return-from physical-reseed-cancel record))
    (unless (eq :aborting (getf record :phase))
      (let ((next (copy-list record)))
        (setf (getf next :phase) :aborting
              record (physical-reseed-attempt-publish owner next :previous record))))
    ;; Use the original frozen route and publish ABORTED only after a fresh
    ;; source response confirms both CLOSED and the ABORTED outcome.
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
