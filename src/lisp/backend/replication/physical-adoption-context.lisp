;;; Fresh context checks for offline former-primary adoption.
;;;
;;; The record codec checks saved facts for structural consistency. This module
;;; checks those facts against the stopped primary, its private stage and an
;;; observed source connection. The caller owns the stopped primary and keeps
;;; the stage immutable. These checks do not install an image or publish an attempt.

(in-package #:pg)

(defvar *physical-adoption-context-test-hook* nil)

(defun physical-adoption-context-test-point (point path)
  (when *physical-adoption-context-test-hook*
    (funcall *physical-adoption-context-test-hook* point path)))

(defun physical-adoption-attempt-local-facts (state)
  "Capture the owner, clean control and artifact commitments saved by an attempt."
  (let* ((owner (physical-adoption-check-owner (getf state :owner)))
         (capture (getf state :capture)))
    (physical-adoption-attempt-local-copy
     (append
      (list :directory (namestring (physical-adoption-owner-directory owner))
            :device (physical-adoption-owner-device owner) :inode (physical-adoption-owner-inode owner)
            :lock-device (physical-adoption-owner-lock-device owner)
            :lock-inode (physical-adoption-owner-lock-inode owner))
      (loop for key in '(:system-id :cluster-identity :timeline :namespace :end :checkpoint-start
                         :next-xid :allocation-next-xid)
            append (list key (getf state key)))
      (list :wal-control (physical-libpq-octets-datum (first capture))
            :capture (mapcar (lambda (image)
                               (when image
                                 (list (length image)
                                       (physical-store-update-digest #xCBF29CE484222325 image))))
                             capture))))))

(defun physical-adoption-attempt-stage-directory (state directory &optional expected)
  "Check the canonical private stage directory and, if given, its saved identity."
  (let* ((requested (uiop:ensure-directory-pathname directory))
         (identity (physical-adoption-stage-directory-identity requested))
         (target (uiop:ensure-directory-pathname (truename requested)))
         (source (physical-adoption-owner-directory (physical-adoption-check-owner (getf state :owner)))))
    (unless (and (equal requested target)
                 (= (first identity) (physical-adoption-owner-device (getf state :owner)))
                 (not (or (uiop:subpathp target source) (uiop:subpathp source target)))
                 (or (null expected)
                     (equal identity (list (getf expected :device) (getf expected :inode)))))
      (error "adoption stage directory lost its canonical private identity"))
    (handler-case
        (progn
          (sb-posix:lstat (namestring (merge-pathnames "postmaster.pid" target)))
          (error "adoption stage is owned by a postmaster"))
      (sb-posix:syscall-error (condition)
        (unless (= sb-posix:enoent (sb-posix:syscall-errno condition)) (error condition))))
    (values target identity)))

(defun physical-adoption-attempt-source-snapshot (state)
  "Capture source files, expected base inventory and authentication commitments."
  (call-with-physical-adoption-source
   state
   (lambda ()
     (let ((auth (physical-adoption-stage-auth-inputs)))
       (multiple-value-bind (plan names) (physical-adoption-stage-copy-plan auth)
         (let* ((files (mapcar (lambda (entry) (apply #'physical-adoption-stage-file-state entry)) plan))
                (derived (physical-bootstrap-derived-wal-image))
                (expected
                  (loop for name in names
                        collect (if (and derived (equal name "global/pg_wal_control"))
                                    (list name (length derived)
                                          (physical-store-update-digest #xCBF29CE484222325 derived))
                                    (subseq (find name files :key #'first :test #'equal) 0 3))))
                (auth-files (physical-adoption-attempt-auth-copy
                             (mapcar (lambda (name)
                                       (subseq (find name files :key #'first :test #'equal) 0 3))
                                     auth))))
           (list :plan plan :names names :auth auth :files files
                 :expected expected :auth-files auth-files)))))))

(defun physical-adoption-attempt-image-facts (state target identity verified auth-files)
  (physical-adoption-attempt-stage-copy
   (append (list :directory (namestring target) :device (first identity) :inode (second identity))
           (loop for key in '(:system-id :cluster-identity :timeline :checkpoint :checkpoint-start
                              :end :next-xid :manifest-digest :source-pin)
                 append (list key (getf verified key)))
           (list :auth-files auth-files))
   (physical-adoption-attempt-local-facts state)))

(defun physical-adoption-attempt-check-stage-root (target source)
  "Reject missing, extra or wrongly typed stage root entries."
  (let* ((expected
           (remove-duplicates
            (append '("base" "physical-backup.manifest" "physical-recovery.control" "recovery.required")
                    (mapcar (lambda (name) (first (uiop:split-string name :separator "/")))
                            (append (getf source :names) (getf source :auth))))
            :test #'equal))
         (directory (sb-posix:opendir (namestring target)))
         (actual nil))
    (unwind-protect
         (loop for entry = (sb-posix:readdir directory)
               until (zerop (sb-sys:sap-int (sb-alien:alien-sap entry)))
               for name = (sb-posix:dirent-name entry)
               unless (member name '("." "..") :test #'equal)
                 do (push name actual))
      (sb-posix:closedir directory))
    (unless (equal (sort expected #'string<) (setf actual (sort actual #'string<)))
      (error "adoption stage contains missing or unplanned root entries"))
    (dolist (name actual)
      (let ((mode (sb-posix:stat-mode (sb-posix:lstat (namestring (merge-pathnames name target))))))
        (unless (if (member name '("base" "global" "pg_wal") :test #'equal)
                    (sb-posix:s-isdir mode)
                    (sb-posix:s-isreg mode))
          (error "adoption stage root contains a symlink or wrong entry type: ~a" name))))))

(defun physical-adoption-attempt-check-image (state proof directory expected-digest
                                              &key expected snapshot)
  "Verify the complete stage once, with source, authentication and identity rechecks."
  (multiple-value-bind (target identity)
      (physical-adoption-attempt-stage-directory state directory expected)
    (let* ((source (or snapshot (physical-adoption-attempt-source-snapshot state)))
           (auth (getf source :auth-files))
           (verified nil)
           (overlap nil))
      (when (and expected (not (equal auth (getf expected :auth-files))))
        (error "adoption source authentication changed since its attempt"))
      (physical-adoption-context-test-point :before-target-verification target)
      (physical-adoption-attempt-stage-directory state target
                                                 (list :device (first identity) :inode (second identity)))
      (physical-adoption-attempt-check-stage-root target source)
      (let ((*pgdata* target))
        (dolist (name (append (getf source :names) (getf source :auth)
                              '("physical-backup.manifest" "physical-recovery.control" "recovery.required")))
          (physical-adoption-stage-check-components name))
        (physical-adoption-stage-check-auth-copy target (getf source :auth) (getf source :files))
        (authentication-check-backup-policy target)
        (multiple-value-bind (base control) (physical-recovery-verify-base-state)
          (physical-adoption-stage-check-base-facts state base control expected-digest)
          (unless (equal (sort (copy-tree (getf base :files)) #'string< :key #'first)
                         (sort (copy-tree (getf source :expected)) #'string< :key #'first))
            (error "adoption stage files differ from the stopped primary"))
          (setf verified base
                overlap (physical-recovery-read-base-overlap base target))))
      (physical-adoption-stage-recheck-context
       state proof target identity (getf source :plan) (getf source :files))
      (physical-adoption-stage-check-auth-copy target (getf source :auth) (getf source :files))
      (physical-adoption-attempt-check-stage-root target source)
      (physical-adoption-attempt-stage-directory state target
                                                 (list :device (first identity) :inode (second identity)))
      (values verified overlap
              (physical-adoption-attempt-image-facts state target identity verified auth)))))

(defun physical-adoption-attempt-validate-context (owner record)
  "Revalidate local attempt and stage facts without observing the remote source.
The caller must keep the offline private stage immutable during validation."
  (let* ((record (physical-adoption-attempt-copy record))
         (owner (physical-adoption-attempt-check-owner owner record))
         (state (physical-adoption-inspect-primary owner)))
    (unless (member (getf record :phase) '(:preparing :proved))
      (error "cancelled adoption attempts cannot prepare or prove"))
    (unless (equal (getf record :local) (physical-adoption-attempt-local-facts state))
      (error "adoption local facts or captured artifacts changed"))
    (physical-adoption-stage-check-proof state (getf record :origin-proof))
    (multiple-value-bind (base overlap stage)
        (physical-adoption-attempt-check-image
         state (getf record :origin-proof) (getf (getf record :stage) :directory)
         (getf (getf record :stage) :manifest-digest) :expected (getf record :stage))
      (unless (and (equal stage (getf record :stage))
                   (equal (getf record :local)
                          (physical-adoption-attempt-local-facts (physical-adoption-inspect-primary owner))))
        (error "adoption context changed during stage verification"))
      (let* ((request (getf record :request))
             (end (getf state :end)))
        (unless (and (= (getf request :overlap-start) (- end (length overlap)))
                     (= (getf request :overlap-digest)
                        (physical-store-update-digest #xCBF29CE484222325 overlap)))
          (error "adoption request differs from the verified stage overlap")))
      (values state base overlap))))

(defun physical-adoption-attempt-accepted-source (client profile)
  "Save the actual accepted connection route with the supplied profile."
  (physical-libpq-require-owner client)
  (when (or (physical-libpq-channel-closed client) (physical-libpq-channel-stopped client))
    (error "adoption source channel is closed or stopped"))
  (let ((info (physical-libpq-check-connection-mode
               (physical-libpq-channel-info client) (physical-libpq-profile-mode profile))))
    (physical-follow-source-copy
     (list :conninfo (physical-libpq-profile-conninfo-datum profile)
           :fallback (physical-libpq-profile-fallback profile)
           :owner (physical-libpq-frozen-profile-datum (make-physical-libpq-frozen-profile info))))))

(defun physical-adoption-attempt-check-client (client source)
  (physical-libpq-require-owner client)
  (when (or (physical-libpq-channel-closed client) (physical-libpq-channel-stopped client))
    (error "adoption source channel is closed or stopped"))
  (multiple-value-bind (profile owner) (physical-follow-source-profile source)
    (physical-libpq-check-connection-mode (physical-libpq-channel-info client)
                                          (physical-libpq-profile-mode profile))
    (unless (equal (physical-libpq-connection-route (physical-libpq-channel-info client))
                   (physical-libpq-frozen-route owner))
      (error "adoption source route changed during observation")))
  t)

(defun physical-adoption-attempt-source-identity (client state)
  (let* ((row (physical-client-identity-row client))
         (system (pg-client-native-u64 (aref row 0)))
         (timeline (pg-client-native-u64 (aref row 1) t))
         (flush (pg-client-native-lsn (aref row 2))))
    (unless (and (= system (getf state :system-id))
                 (> timeline (getf state :timeline)) (>= flush (getf state :end)))
      (error "adoption source does not name a descendant containing the clean endpoint"))
    (values timeline flush)))

(defun physical-adoption-attempt-observe-source (client source state)
  "Read identity, history and identity again to reject a promotion race."
  (physical-adoption-attempt-check-client client source)
  (multiple-value-bind (timeline flush) (physical-adoption-attempt-source-identity client state)
    (let ((history (bytes->string (pg-client-native-history client timeline))))
      (multiple-value-bind (again position) (physical-adoption-attempt-source-identity client state)
        (unless (and (= again timeline) (>= position flush))
          (error "adoption source changed during history observation")))
      (physical-adoption-attempt-check-client client source)
      (values timeline history))))

(defun physical-adoption-attempt-create (state image client profile)
  "Build a checked attempt from the stopped primary, private stage and live source.
The caller must keep the offline private stage immutable during construction.
This does not publish the record or send PREPARE or PROVE."
  (let* ((owner (physical-adoption-check-owner (getf state :owner)))
         (fresh (physical-adoption-inspect-primary owner))
         (local (physical-adoption-attempt-local-facts fresh))
         (proof (getf image :origin-proof))
         (source (physical-adoption-attempt-accepted-source client profile))
         (snapshot (physical-adoption-attempt-source-snapshot fresh)))
    (unless (equal local (physical-adoption-attempt-local-facts state))
      (error "adoption source state changed before attempt construction"))
    (physical-adoption-stage-check-proof fresh proof)
    (multiple-value-bind (source-timeline history) (physical-adoption-attempt-observe-source client source fresh)
      (physical-adoption-context-test-point :after-source-observation (getf image :directory))
      (multiple-value-bind (base overlap stage)
          (physical-adoption-attempt-check-image
           fresh proof (getf image :directory) (getf image :manifest-digest) :snapshot snapshot)
        (declare (ignore base))
        (let* ((end (getf fresh :end))
               (request (list :version 1 :system-id (getf fresh :system-id)
                              :base-timeline (getf fresh :timeline) :base end
                              :manifest-digest (getf stage :manifest-digest)
                              :source-timeline source-timeline :receive-timeline (getf fresh :timeline)
                              :receive-lsn end :receive-digest #xCBF29CE484222325
                              :overlap-start (- end (length overlap))
                              :overlap-digest (physical-store-update-digest #xCBF29CE484222325 overlap)))
               (record (physical-adoption-attempt-copy
                        (list :version 1 :phase :preparing :local local :stage stage
                              :origin-proof proof :source source :request request :source-history history
                              :anchor nil :proof nil))))
          (unless (equal local
                         (physical-adoption-attempt-local-facts (physical-adoption-inspect-primary owner)))
            (error "adoption local context changed during source observation"))
          (physical-adoption-attempt-check-client client source)
          record)))))
