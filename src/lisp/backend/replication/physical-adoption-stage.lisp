;;;; Build a guarded canonical base from a cooperatively owned stopped primary.
;;;; Staging preserves separate historical evidence and does not install the image.
(in-package #:pg)

(defvar *physical-adoption-stage-test-hook* nil)

(defun physical-adoption-stage-test-point (point value)
  (when *physical-adoption-stage-test-hook*
    (funcall *physical-adoption-stage-test-hook* point value)))

(defun physical-adoption-stage-directory-identity (directory)
  "Check and identify an owned, private, nonsymlink stage directory."
  (let ((stat (sb-posix:lstat (string-right-trim "/" (namestring directory)))))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                 (= #o700 (logand #o7777 (sb-posix:stat-mode stat))))
      (error "adoption stage requires an owned private nonsymlink directory"))
    (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat))))

(defun physical-adoption-stage-stat-identity (stat)
  (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat) (sb-posix:stat-mode stat)
        (sb-posix:stat-size stat) (sb-posix:stat-mtime stat) (sb-posix:stat-ctime stat)))

(defun physical-adoption-stage-check-components (relative)
  "Reject symlink or non-directory source components and nonregular source files."
  (let ((parts (uiop:split-string relative :separator "/")))
    (loop for count from 1 below (length parts)
          for path = (format nil "~a~{~a~^/~}" (namestring *pgdata*) (subseq parts 0 count))
          unless (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:lstat path)))
            do (error "adoption source contains a non-directory or symlink component: ~a" path))
    (unless (sb-posix:s-isreg
             (sb-posix:stat-mode (sb-posix:lstat (namestring (merge-pathnames relative *pgdata*)))))
      (error "adoption source is not a regular nonsymlink file: ~a" relative))))

(defun physical-adoption-stage-file-state (relative &key limit (maximum 18446744073709551615))
  "Hash a bounded source file or prefix while checking path and open-file identity.
The caller must keep the stopped source stable under cooperative offline ownership."
  (physical-adoption-stage-check-components relative)
  (let* ((path (merge-pathnames relative *pgdata*))
         (before (physical-adoption-stage-stat-identity (sb-posix:lstat (namestring path)))))
    (multiple-value-bind (stream size) (open-bounded-regular-file path maximum)
      (unwind-protect
           (let ((length (or limit size))
                 (digest #xCBF29CE484222325)
                 (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
             (unless (and (<= length size)
                          (equal before (physical-adoption-stage-stat-identity
                                         (sb-posix:fstat (sb-sys:fd-stream-fd stream)))))
               (error "adoption source file changed before reading: ~a" relative))
             (loop with remaining = length
                   for count = (read-sequence buffer stream :end (min remaining (length buffer)))
                   while (plusp remaining)
                   do (when (zerop count) (error "short adoption source read: ~a" relative))
                      (decf remaining count)
                      (setf digest (physical-store-update-digest digest (subseq buffer 0 count))))
             (unless (and (equal before (physical-adoption-stage-stat-identity
                                         (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                          (equal before (physical-adoption-stage-stat-identity
                                         (sb-posix:lstat (namestring path)))))
               (error "adoption source file changed while reading: ~a" relative))
             (list relative length digest before))
        (close stream)))))

(defun physical-adoption-stage-auth-inputs ()
  "Check canonical on-disk authentication paths and validate their inputs.
These checks reject observed redirects; they cannot prove the policy installed by
an unknown earlier command line."
  (let ((native (make-hash-table :test #'equal))
        (overrides (make-hash-table :test #'equal))
        (names '("authentication.initialized" "pg_hba.conf" "pg_ident.conf")))
    (setf (gethash "pgdata" native) (namestring *pgdata*))
    (let ((settings (startup-load-settings native overrides)))
      (unless (and (equal (pathname (startup-setting-value settings "data_directory")) *pgdata*)
                   (every (lambda (entry)
                            (equal (pathname (startup-setting-value settings (car entry)))
                                   (merge-pathnames (cdr entry) *pgdata*)))
                          '(("hba_file" . "pg_hba.conf") ("ident_file" . "pg_ident.conf"))))
        (error "adoption staging requires canonical on-disk data and authentication paths")))
    (dolist (name names) (physical-adoption-stage-check-components name))
    (authentication-check-marker *pgdata*)
    (hba-read-file (merge-pathnames "pg_hba.conf" *pgdata*))
    (ident-read-file (merge-pathnames "pg_ident.conf" *pgdata*))
    names))

(defun physical-adoption-stage-copy-plan (auth)
  "Plan the checkpoint-start through E WAL window and the remaining base files.
The first WAL segment keeps its prefix; the final segment is bounded at E."
  (let* ((end *wal-durable-lsn*)
         (last (concatenate 'string "pg_wal/"
                            (file-namestring (wal-segment-path (floor (1- end) +wal-segment-size+)))))
         (files (physical-bootstrap-inventory)))
    (values
     (mapcar (lambda (name)
               (list name
                     :limit (when (equal name last) (1+ (mod (1- end) +wal-segment-size+)))
                     :maximum (if (member name auth :test #'equal) 1048576 18446744073709551615)))
             (append files auth))
     files)))

(defun physical-adoption-stage-check-proof (state proof)
  "Recompute the source origin witness and reject any change."
  (let ((fresh (physical-adoption-prove-primary-origin state (getf proof :identity))))
    (unless (equalp proof fresh)
      (error "adoption stage origin proof changed"))
    fresh))

(defun physical-adoption-stage-check-auth-copy (target names source-files)
  "Read back authentication adjuncts exactly; they are outside the file inventory."
  (let ((*pgdata* target))
    (dolist (name names)
      (let ((expected (find name source-files :key #'first :test #'equal))
            (actual (physical-adoption-stage-file-state name :maximum 1048576)))
        (unless (and expected (equal (subseq expected 0 3) (subseq actual 0 3)))
          (error "adoption authentication copy differs from its source: ~a" name))))))

(defun physical-adoption-stage-recheck-context (state proof target identity plan before)
  "Recheck source file hashes and identities, origin proof and target identity."
  (let* ((owner (physical-adoption-check-owner (getf state :owner)))
         (*pgdata* (physical-adoption-owner-directory owner)))
    (unless (equalp before
                    (mapcar (lambda (entry) (apply #'physical-adoption-stage-file-state entry)) plan))
      (error "adoption source files changed during staging"))
    (physical-adoption-stage-auth-inputs)
    (physical-adoption-stage-check-proof state proof))
  (unless (equal identity (physical-adoption-stage-directory-identity target))
    (error "adoption stage directory changed during verification")))

(defun physical-adoption-stage-check-base-facts (state verified control expected-digest)
  "Check verified base and recovery control against the clean source endpoint."
  (unless (and (eql 3 (getf verified :version)) (eql 1 (getf verified :auth-policy-required))
               (eql 3 (getf control :control-version)) (zerop (getf control :phase))
               (= +db-startup+ (getf control :db-state))
               (= expected-digest (getf verified :manifest-digest))
               (every (lambda (key) (equal (getf state key) (getf verified key)))
                      '(:system-id :cluster-identity :timeline :checkpoint-start :end :next-xid))
               (= (getf state :end) (getf verified :checkpoint) (getf control :checkpoint)
                  (getf control :applied))
               (= (getf state :next-xid) (getf control :next-xid))
               (= (getf state :timeline) (getf control :applied-timeline (getf control :timeline)))
               (null (getf control :history-target)) (null (getf control :retained-reference))
               (equalp (wal-copy-control-resource-fields (getf state :snapshot))
                       (getf control :resource-fact))
               (null (getf (seventh (multiple-value-list (wal-read-control-fields))) :retained-reference)))
    (error "adoption stage differs from its canonical clean endpoint"))
  t)

(defun physical-adoption-stage-build (state proof target source-pin)
  "Guard the target in phase zero before copying and verify the completed image.
The copied WAL control drops the retained-root reference; the source is unchanged.
Use actual next XID, not allocator capacity. Failure can leave a guarded, nonempty
target which cannot be retried as an empty stage."
  (let* ((auth (physical-adoption-stage-auth-inputs))
         (identity (physical-adoption-stage-directory-identity target)))
    (multiple-value-bind (plan names) (physical-adoption-stage-copy-plan auth)
      (let ((before (mapcar (lambda (entry) (apply #'physical-adoption-stage-file-state entry)) plan))
            (files nil))
        (physical-bootstrap-mark-target target)
        (physical-recovery-mark-target target)
        (wal-ensure-directory-durable (merge-pathnames "base/" target))
        (physical-adoption-stage-test-point :after-guard target)
        (loop for entry in plan for expected in before
              for name = (first entry)
              for copied = (physical-bootstrap-copy-file name target :limit (getf (rest entry) :limit))
              do (unless (or (and (equal name "global/pg_wal_control")
                                  (getf *wal-control-metadata* :retained-reference))
                             (equal copied (subseq expected 0 3)))
                   (error "adoption copied file differs from its checked source: ~a" name))
                 (when (member name names :test #'equal) (push copied files))
                 (physical-adoption-stage-test-point :after-file name))
        (let* ((manifest (list :version 3 :system-id (getf state :system-id)
                               :cluster-identity (getf state :cluster-identity)
                               :timeline *wal-insert-timeline* :checkpoint *wal-checkpoint-lsn*
                               :checkpoint-start (getf state :checkpoint-start) :end *wal-durable-lsn*
                               :next-xid *next-xid* :source-pin source-pin
                               :files (nreverse files) :auth-policy-required 1))
               (image (physical-bootstrap-manifest-image manifest)))
          (physical-adoption-stage-test-point :before-manifest target)
          (wal-atomic-write-bytes (merge-pathnames "physical-backup.manifest" target) image)
          (fsync-directory target)
          (physical-adoption-stage-test-point :after-manifest target)
          (physical-adoption-stage-recheck-context state proof target identity plan before)
          (physical-adoption-stage-test-point :before-verify target)
          (let ((*pgdata* target))
            (physical-adoption-stage-check-auth-copy target auth before)
            (authentication-check-backup-policy target)
            (multiple-value-bind (verified control) (physical-recovery-verify-base-state)
              (physical-adoption-stage-check-base-facts
               state verified control (physical-store-update-digest #xCBF29CE484222325 image))
              (physical-adoption-stage-recheck-context state proof target identity plan before)
              (physical-adoption-stage-check-auth-copy target auth before)
              (list :directory target :base-state verified
                    :manifest-digest (getf verified :manifest-digest)
                    :origin-proof (copy-tree proof)))))))))

(defun call-with-physical-adoption-source (state function)
  "Call FUNCTION with the stopped primary's inspected state bound as the source."
  (let* ((owner (physical-adoption-check-owner (getf state :owner)))
         (*pgdata* (physical-adoption-owner-directory owner))
         (*catalog* (getf state :catalog))
         (*wal-checkpoint-lsn* (getf state :end))
         (*wal-durable-lsn* (getf state :end))
         (*wal-checkpoint-snapshot* (getf state :snapshot))
         (*wal-control-metadata* (getf state :metadata))
         (*wal-insert-timeline* (getf state :timeline))
         (*wal-checkpoint-timeline* (getf state :timeline))
         (*wal-segment-namespace* (getf state :namespace))
         (*next-xid* (getf state :next-xid)))
    (funcall function)))

(defun physical-adoption-stage-base (state origin-proof directory source-pin)
  "Build a verified canonical E/Mnew image from an unchanged stopped primary.
The caller owns the existing source lock and must keep its files stable. DIRECTORY
must be an empty private 0700 nonsymlink on the same filesystem, disjoint from
the source. SOURCE-PIN is only a manifest label; no source pin is created.
Return the historical B/M/H_E witness separately from the new manifest digest.
This does not install, follow, start, or grant writer authority."
  (unless (wal-valid-replication-slot-name-p source-pin)
    (error "invalid adoption stage manifest pin name"))
  (let* ((owner (physical-adoption-check-owner (getf state :owner)))
         (*pgdata* (physical-adoption-owner-directory owner))
         (fresh (physical-adoption-inspect-primary owner))
         (proof (physical-adoption-stage-check-proof state origin-proof)))
    (unless (every (lambda (key) (equalp (getf state key) (getf fresh key)))
                   '(:capture :system-id :cluster-identity :timeline :namespace :end
                     :checkpoint-start :snapshot :metadata :next-xid :allocation-next-xid :history))
      (error "adoption stage requires the unchanged inspected primary"))
    (unless (= (physical-adoption-owner-device owner)
               (first (physical-adoption-stage-directory-identity
                       (uiop:ensure-directory-pathname directory))))
      (error "adoption stage must share the owned source filesystem"))
    (let ((target (physical-bootstrap-target directory)))
      (call-with-physical-adoption-source
       fresh (lambda () (physical-adoption-stage-build fresh proof target source-pin))))))
