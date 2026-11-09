;; Start an independent capture while retaining a configured cancellation tree.
;; The old owner protects saved cleanup history.  The new root's attempt journal
;; records capture progress for status and cancellation after an interruption.

(in-package #:pg)

(defun physical-reseed-replacement-request-copy (old new)
  ;; Both cluster identifiers must match.  A fresh name and token keep the
  ;; new lease separate from the cancelled lease, even if the timeline changed.
  (let ((old (physical-backup-lease-request-copy old))
        (new (physical-backup-lease-request-copy new)))
    (unless (and (= (getf old :system-id) (getf new :system-id))
                 (string= (getf old :cluster-identity) (getf new :cluster-identity))
                 (not (string= (getf old :name) (getf new :name)))
                 (not (string= (getf old :token) (getf new :token))))
      (error "replacement requires the same cluster and a fresh lease name and token"))
    new))

(defun physical-reseed-replacement-directories-disjoint-p (left right)
  ;; Rows contain (canonical-path device inode mount-id).  Refuse path
  ;; containment and inode aliases.  Different mounts on one device can alias
  ;; protected history, so refuse that layout too.
  (let ((left-path (pathname (first left))) (right-path (pathname (first right))))
    (and (not (uiop:subpathp left-path right-path))
         (not (uiop:subpathp right-path left-path))
         (not (and (= (second left) (second right)) (= (third left) (third right))))
         (or (/= (second left) (second right)) (= (fourth left) (fourth right))))))

(defun physical-reseed-replacement-directory (observation directory fd)
  ;; Compare the held directory with all ten protected directories in the
  ;; old cancellation snapshot.
  (physical-image-check-target directory fd)
  (let* ((path (namestring (uiop:ensure-directory-pathname (truename (physical-image-fd-path fd)))))
         (stat (sb-posix:fstat fd))
         (row (list path (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
                    (physical-adoption-install-mount-id nil :directory-fd fd))))
    (dolist (role (getf (getf observation :stamp) :roles))
      (let* ((old (fourth role))
             (protected (list (second role) (second old) (third old) (third role))))
        (unless (physical-reseed-replacement-directories-disjoint-p row protected)
          (error "replacement directory overlaps or aliases protected cancellation history"))))
    row))

(defun physical-reseed-replacement-observation (owner &optional expected)
  ;; Read the complete cancellation snapshot again while holding its owner.
  ;; Reject a changed snapshot before continuing capture.
  (let ((actual (physical-reseed-follow-cancellation-observation-data
                 (physical-reseed-follow-observe-configured-cancellation owner))))
    (when (and expected (not (equal expected actual)))
      (error "configured cancellation history changed during replacement capture"))
    actual))

(defun physical-reseed-replacement-target (observation owner fd &key empty)
  ;; The new root starts with only its private, empty lock.  After journal
  ;; publication that journal is the only other permitted entry.
  (let* ((row (physical-reseed-replacement-directory
               observation (physical-adoption-owner-directory owner) fd))
         (lock (sb-posix:fstat (sb-sys:fd-stream-fd (physical-adoption-owner-lock owner))))
         (names (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t)))
    (unless (and (zerop (sb-posix:stat-size lock))
                 (= #o600 (logand #o7777 (sb-posix:stat-mode lock)))
                 (equal '(".pg_lisp.lock")
                        (if empty names (remove "physical-reseed.attempt" names :test #'string=))))
      (error "replacement target requires a private empty data lock and only its capture journal"))
    row))

(defun physical-reseed-start-configured-replacement-remote
    (old-directory directory stage request profile &key helper)
  "Capture a replacement for a durable configured cancellation.

OLD-DIRECTORY names a dedicated configured cancellation in PENDING,
SOURCE-CLOSED or CLEANED phase.  Reconcile temporary cancellation files before
calling.  DIRECTORY must exist with mode 0700 and contain only an empty
.pg_lisp.lock file with mode 0600.  STAGE must be a separate empty private
directory on the same mount.  Both must be disjoint from every protected old
role.  The request must identify the same cluster and system and use a fresh
lease name and token.  Its timeline may differ from the cancelled request.

The old owner stays held through capture.  The old snapshot, new root and
stage are checked before and after source discovery and capture, and the old
snapshot is checked on unwind.  The new root's durable capture journal fences
retries.  Use the existing status, cancellation and recovery operations for an
interrupted attempt.  Keep the old root, saved debt and frozen route for later
cleanup.

Return the received manifest and source lease record.  Installation, activation,
retirement and finalization use the existing operations.  The caller chooses
the directories and coordinates any competing replacements."
  (call-with-physical-reseed-follow-cancellation-owner
   old-directory
   (lambda (old-owner)
     (let* ((observation (physical-reseed-replacement-observation old-owner))
            (record (getf observation :record))
            (request (physical-reseed-replacement-request-copy
                      (getf (getf record :attempt) :request) request)))
       (unless (member (getf (getf record :cancellation) :phase) '(:pending :source-closed :cleaned))
         (error "replacement requires a stable durable configured cancellation"))
       (unwind-protect
            (call-with-physical-reseed-owner
             directory
             (lambda (owner)
               (call-with-physical-directory-root
                owner
                (lambda (root-fd scoped)
                  (declare (ignore scoped))
                  (let ((target (physical-reseed-replacement-target observation owner root-fd :empty t)))
                    (%physical-reseed-start-remote
                     owner stage request profile :helper helper
                     :stage-check
                     (lambda (directory fd)
                       (physical-reseed-replacement-observation old-owner observation)
                       (unless (equal target (physical-reseed-replacement-target observation owner root-fd))
                         (error "replacement target directory changed during capture"))
                       (let ((receive (physical-reseed-replacement-directory observation directory fd)))
                         (unless (and (= (fourth target) (fourth receive))
                                      (physical-reseed-replacement-directories-disjoint-p target receive))
                           (error "replacement target and stage require separate directories on one mount"))))))))))
         (physical-reseed-replacement-observation old-owner observation))))))
