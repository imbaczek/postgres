(in-package #:pg)

;;; Publish the first activation intent and recover its interrupted write.
;;; Building binds the intended active image before output creation.

(defun physical-reseed-follow-activation-require-attempt (context)
  ;; Effects require the exact active original attempt.  Read-only scope
  ;; checks can still inspect an attempt that has advanced to abort.
  (unless (and (physical-reseed-follow-activation-context-active-p context)
               (eq sb-thread:*current-thread* (physical-reseed-follow-activation-context-thread context)))
    (error "activation attempt check requires its active thread-bound scope"))
  (let* ((owner (physical-reseed-follow-activation-context-owner context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (snapshot (physical-reseed-follow-activation-context-attempt context))
         (record (getf snapshot :record)))
    (funcall (physical-reseed-follow-activation-context-check context))
    (physical-reseed-check-owner owner)
    (physical-reseed-follow-check-attempt-snapshot-at root snapshot)
    (unless (and (eq :premanifest (getf record :phase))
                 (equal record (getf (physical-reseed-follow-activation-context-record context) :attempt)))
      (error "activation requires the exact active original attempt")))
  t)

(defun physical-reseed-follow-activation-staged-record (value entry)
  ;; Construct and check the Staged successor without changing disk state.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation)))
    (unless (and (eq :building (getf activation :phase)) (zerop (getf activation :step)))
      (error "staged activation requires Building at step zero"))
    (setf (getf activation :phase) :staged
          (getf activation :entry) (physical-adoption-attempt-value-copy entry))
    (let ((candidate (physical-reseed-follow-copy record)))
      (physical-reseed-follow-check-transition value candidate)
      candidate)))

(defun physical-reseed-follow-activation-building-candidate (context value)
  ;; Accept only the first Building record, bound to the derived image and
  ;; an absent output at prefix zero.
  (let* ((before (physical-reseed-follow-activation-context-record context))
         (candidate (physical-reseed-follow-copy value))
         (binding (physical-reseed-follow-activation-context-binding context))
         (bytes (physical-follow-config-image
                 (physical-reseed-follow-activation-config before binding)))
         (expected (list :phase :building :step 0
                         :image (list (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))
                         :entry nil)))
    (physical-reseed-follow-check-transition before candidate)
    (unless (and (null (getf before :activation))
                 (null (getf before :retirement)) (null (getf candidate :retirement))
                 (zerop (physical-reseed-follow-activation-context-prefix context))
                 (null (physical-reseed-follow-activation-context-output-entry context))
                 (equal expected (getf candidate :activation)))
      (error "activation publication requires the exact first Building image and absent output"))
    candidate))

(defun physical-reseed-follow-activation-check-durable-output (context candidate)
  ;; Match the candidate and current named metadata to the output witness
  ;; captured after synchronization in this scope.
  (let* ((descriptors (physical-reseed-follow-activation-context-descriptors context))
         (parent (physical-reseed-follow-configuration-role-fd descriptors :staged))
         (proof (physical-reseed-follow-activation-context-durable-output context))
         (entry (getf (getf candidate :activation) :entry))
         (mount (physical-adoption-install-mount-id nil :directory-fd parent)))
    (unless (and proof entry
                 (equal entry (getf proof :entry))
                 (equal entry (physical-reseed-follow-activation-context-output-entry context))
                 (equal (getf proof :row)
                        (physical-reseed-install-old-observe parent "follow.active" mount)))
      (error "staged activation lacks its unchanged durable output proof")))
  t)

(defun physical-reseed-follow-activation-staged-candidate (context value &optional (durable t))
  ;; Check the exact Building successor.  Recovery first checks its shape,
  ;; then establishes and checks the durability witness before publication.
  (let* ((before (physical-reseed-follow-activation-context-record context))
         (candidate (physical-reseed-follow-copy value))
         (entry (physical-reseed-follow-activation-context-output-entry context)))
    (unless (and entry (zerop (physical-reseed-follow-activation-context-prefix context))
                 (null (getf before :retirement)) (null (getf candidate :retirement))
                 (equal candidate (physical-reseed-follow-activation-staged-record before entry)))
      (error "staged activation differs from its exact Building predecessor and output"))
    (when durable (physical-reseed-follow-activation-check-durable-output context candidate))
    candidate))

(defun physical-reseed-follow-activation-publication-check (owner context candidate)
  ;; Keep the candidate and operation kind frozen throughout publication.
  ;; Other publication kinds cannot use this activation scope.
  (unless (and (eq context (physical-reseed-follow-activation-context owner))
               (equal candidate *physical-reseed-follow-activation-candidate*)
               (not (or *physical-reseed-follow-installation-scope*
                        *physical-reseed-follow-installation-candidate*
                        *physical-reseed-follow-settlement-candidate*
                        *physical-reseed-follow-terminal-settlement-candidate*)))
    (error "activation publication lacks its exclusive frozen candidate"))
  (physical-reseed-follow-activation-require-attempt context)
  (when (eq :staged (getf (getf candidate :activation) :phase))
    (physical-reseed-follow-activation-check-durable-output context candidate))
  t)

(defmethod physical-reseed-follow-configuration-publication-candidate
    ((owner physical-reseed-follow-configuration-owner)
     (context physical-reseed-follow-activation-context) candidate)
  (physical-reseed-follow-activation-publication-check owner context candidate)
  (case (getf (getf candidate :activation) :phase)
    (:building (physical-reseed-follow-activation-building-candidate context candidate))
    (:staged (physical-reseed-follow-activation-staged-candidate context candidate))
    (otherwise (error "unsupported activation publication phase"))))

(defmethod physical-reseed-follow-published-snapshot :around
    ((owner physical-reseed-follow-configuration-owner) root-fd candidate snapshot previous)
  ;; Check the exact prior snapshot and frozen candidate after rename.
  ;; Adopt the published snapshot before the next guard runs.
  (unless *physical-reseed-follow-activation-scope*
    (return-from physical-reseed-follow-published-snapshot (call-next-method)))
  (let ((context (physical-reseed-follow-activation-context owner)))
    (unless (and (equal candidate (physical-reseed-follow-configuration-publication-candidate
                                   owner context candidate))
                 (= root-fd (physical-reseed-follow-configuration-role-fd
                             (physical-reseed-follow-activation-context-descriptors context) :root))
                 (equal previous (physical-reseed-follow-activation-context-snapshot context))
                 (equal (physical-reseed-follow-activation-context-record context) (getf previous :record))
                 (equal candidate (getf snapshot :record)))
      (error "published activation differs from its frozen candidate or predecessor"))
    (setf (physical-reseed-follow-activation-context-record context) (physical-reseed-follow-copy candidate)
          (physical-reseed-follow-activation-context-snapshot context) snapshot))
  t)

(defun physical-reseed-follow-activation-publish (context value check)
  ;; Check the candidate before creating the temporary.  Building reserves
  ;; future capacity, and Staged uses that reservation.  Keep checking the
  ;; frozen kind and attempt during the write.
  (let* ((candidate (physical-reseed-follow-copy value))
         (*physical-reseed-follow-activation-candidate* (physical-reseed-follow-copy candidate))
         (owner (physical-reseed-follow-activation-context-owner context))
         (previous (physical-reseed-follow-activation-context-snapshot context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-activation-context-descriptors context) :root)))
    (funcall check)
    (physical-reseed-follow-configuration-publication-candidate owner context candidate)
    (when (eq :building (getf (getf candidate :activation) :phase))
      (physical-reseed-follow-check-future-capacity candidate))
    (flet ((guard ()
             (funcall check)
             (physical-reseed-follow-activation-publication-check owner context candidate)))
      (multiple-value-bind (record snapshot)
          (physical-reseed-follow-write-intent
           owner root candidate (physical-reseed-follow-image candidate) previous #'guard)
        (unless (and (equal record (physical-reseed-follow-activation-context-record context))
                     (equal snapshot (physical-reseed-follow-activation-context-snapshot context)))
          (error "activation publication did not adopt its canonical readback"))
        (values record snapshot)))))

(defun physical-reseed-follow-activation-publish-building (context value check)
  ;; Restrict this entry point to the first Building candidate.
  (physical-reseed-follow-activation-publish
   context (physical-reseed-follow-activation-building-candidate context value) check))

(defun physical-reseed-follow-begin-activation (owner)
  ;; Publish the first Building intent or return the existing Building record.
  ;; An existing temporary must be recovered explicitly.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (record (physical-reseed-follow-copy (physical-reseed-follow-activation-context-record context)))
            (activation (getf record :activation)))
       (funcall check)
       (physical-reseed-follow-activation-require-attempt context)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "activation temporary requires explicit recovery")
       (if activation
           (progn
             (unless (eq :building (getf activation :phase))
               (error "activation has already advanced beyond Building"))
             (values record (physical-reseed-follow-activation-context-snapshot context)))
           (let* ((bytes (physical-follow-config-image
                          (physical-reseed-follow-activation-config
                           record (physical-reseed-follow-activation-context-binding context))))
                  (image (list (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))))
             (setf (getf record :activation)
                   (list :phase :building :step 0 :image image :entry nil))
             (physical-reseed-follow-activation-publish-building context record check)))))))

(defun physical-reseed-follow-recover-activation (owner)
  ;; Preflight the exact next Building or Staged temporary before promotion.
  ;; Recheck its held identity and active attempt.  An uncertain failure
  ;; fences the owner.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (previous (physical-reseed-follow-activation-context-snapshot context))
            (temporary (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)))
       (if temporary
           (let* ((candidate (physical-reseed-follow-read-at root "physical-reseed.follow.tmp"))
                  (*physical-reseed-follow-activation-candidate* (physical-reseed-follow-copy candidate))
                  (fd nil))
             (physical-reseed-follow-activation-prepare-recovery context candidate check)
             (physical-reseed-follow-configuration-publication-candidate owner context candidate)
             (unwind-protect
                  (progn
                    (funcall check)
                    (physical-reseed-follow-activation-require-attempt context)
                    (sb-sys:without-interrupts
                      (setf fd (physical-image-openat
                                root "physical-reseed.follow.tmp"
                                (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                        +postmaster-o-cloexec+) 0)))
                    (when (minusp fd) (setf fd nil) (error "cannot open activation recovery temporary"))
                    (unless (equal temporary (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                      (error "activation recovery temporary changed"))
                    (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                    (physical-reseed-follow-promote-intent
                     owner root fd (subseq temporary 0 5) candidate previous
                     (lambda ()
                       (funcall check)
                       (physical-reseed-follow-activation-publication-check owner context candidate))))
               (when fd (sb-posix:close fd))))
           (progn
             (setf (physical-reseed-follow-owner-uncertain-p owner) t)
             (physical-reseed-follow-read-at root "physical-reseed.follow" :stabilize t)
             (funcall check)
             (values (physical-reseed-follow-copy
                      (physical-reseed-follow-activation-context-record context))
                     (physical-reseed-follow-activation-context-snapshot context))))))))

(defun physical-reseed-follow-activation-output-guard (context)
  ;; Guard output work while the layout may be partial.  The complete proof
  ;; runs again after the output has been checked and adopted.
  (physical-reseed-follow-activation-guard context)
  (physical-reseed-follow-activation-require-attempt context))

(defun physical-reseed-follow-activation-sync-output (context entry check)
  ;; Hold the same file descriptor through synchronization and identity checks.
  ;; Sync the file and its directories, then bind the checked entry and named
  ;; metadata as the scope's durability witness.  Recheck after adoption.
  (declare (notinline sb-posix:fsync))
  (let* ((owner (physical-reseed-follow-activation-context-owner context))
         (descriptors (physical-reseed-follow-activation-context-descriptors context))
         (record (physical-reseed-follow-activation-context-record context))
         (binding (physical-reseed-follow-activation-context-binding context))
         (parent (physical-reseed-follow-configuration-role-fd descriptors :staged))
         (workspace (physical-reseed-follow-configuration-role-fd descriptors :workspace))
         (path (merge-pathnames "follow.active" (physical-image-fd-path parent)))
         (bytes (physical-follow-config-image (physical-reseed-follow-activation-config record binding))))
    (unless (and entry (eq :building (getf (getf record :activation) :phase))
                 (zerop (physical-reseed-follow-activation-context-prefix context)))
      (error "output synchronization requires the exact Building file"))
    (physical-reseed-follow-activation-output-guard context)
    (call-with-physical-image-input-at
     parent "follow.active"
     (lambda (stream)
       (let* ((fd (sb-sys:fd-stream-fd stream))
              (fixed (subseq (physical-reseed-follow-file-identity (sb-posix:fstat fd) t) 0 5)))
         (labels ((held ()
                    (physical-reseed-follow-activation-output-guard context)
                    (physical-reseed-follow-configuration-output-check parent fd path fixed)
                    (unless (equal entry (physical-reseed-follow-configuration-file-row
                                          parent "follow.active" "follow.active" bytes))
                      (error "active output differs from its captured file"))
                    (physical-reseed-follow-configuration-output-check parent fd path fixed))
                  (point (event)
                    (held)
                    (physical-reseed-follow-test-point event path)
                    (held)))
           (held)
           (setf (physical-reseed-follow-owner-uncertain-p owner) t)
           (point :before-activation-output-fsync)
           (sb-posix:fsync fd)
           (point :after-activation-output-fsync)
           (point :before-activation-staged-fsync)
           (sb-posix:fsync parent)
           (point :after-activation-staged-fsync)
           (point :before-activation-workspace-fsync)
           (sb-posix:fsync workspace)
           (point :after-activation-workspace-fsync)
           (point :before-activation-ancestors-fsync)
           (physical-adoption-install-sync-ancestors
            (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
           (point :after-activation-ancestors-fsync)
           (let* ((observed (physical-reseed-follow-activation-output-state
                             owner descriptors record binding
                             (physical-reseed-follow-activation-context-images context)
                             (lambda () (physical-reseed-follow-activation-output-guard context))))
                  (actual (getf observed :entry))
                  (mount (physical-adoption-install-mount-id nil :directory-fd parent)))
             (unless (equal entry actual) (error "durable active output changed before adoption"))
             (held)
             (setf (physical-reseed-follow-activation-context-output-entry context)
                   (physical-adoption-attempt-value-copy actual)
                   (physical-reseed-follow-activation-context-durable-output context)
                   (list :entry (physical-adoption-attempt-value-copy actual)
                         :row (physical-reseed-install-old-observe parent "follow.active" mount)))
             (funcall check)
             (held)
             actual))))
     :element-type '(unsigned-byte 8))))

(defun physical-reseed-follow-stage-activation (owner)
  ;; Create or check the active image, make it durable, and publish Staged.
  ;; This establishes local durability.  Current source checks are separate.
  (call-with-physical-reseed-follow-activation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-activation-context owner))
            (record (physical-reseed-follow-activation-context-record context))
            (phase (getf (getf record :activation) :phase)))
       (funcall check)
       (physical-reseed-follow-activation-require-attempt context)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "activation temporary requires explicit recovery")
       (unless (member phase '(:building :staged))
         (error "activation staging requires Building or Staged intent"))
       (if (eq phase :staged)
           (values (physical-reseed-follow-copy record)
                   (physical-reseed-follow-activation-context-snapshot context))
           (let* ((descriptors (physical-reseed-follow-activation-context-descriptors context))
                  (parent (physical-reseed-follow-configuration-role-fd descriptors :staged))
                  (bytes (physical-follow-config-image
                          (physical-reseed-follow-activation-config
                           record (physical-reseed-follow-activation-context-binding context))))
                  (entry (physical-reseed-follow-configuration-write-file
                          owner parent "follow.active" "follow.active" bytes
                          (lambda () (physical-reseed-follow-activation-output-guard context)) :prepare t)))
             (physical-reseed-follow-activation-sync-output context entry check)
             (physical-reseed-follow-activation-publish
              context (physical-reseed-follow-activation-staged-record record entry) check)))))))

(defun physical-reseed-follow-activation-prepare-recovery (context candidate check)
  ;; Validate the next transition before any recovery fsync.  For Staged,
  ;; reattach the output witness and check it before promoting the temporary.
  (case (getf (getf candidate :activation) :phase)
    (:building (physical-reseed-follow-activation-building-candidate context candidate))
    (:staged
     (physical-reseed-follow-activation-staged-candidate context candidate nil)
     (physical-reseed-follow-activation-sync-output
      context (getf (getf candidate :activation) :entry) check)
     (physical-reseed-follow-activation-staged-candidate context candidate))
    (otherwise (error "unsupported activation recovery transition"))))
