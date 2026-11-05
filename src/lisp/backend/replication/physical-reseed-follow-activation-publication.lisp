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
  t)

(defmethod physical-reseed-follow-configuration-publication-candidate
    ((owner physical-reseed-follow-configuration-owner)
     (context physical-reseed-follow-activation-context) candidate)
  ;; Revalidate the operation authority and exact Building value.
  (physical-reseed-follow-activation-publication-check owner context candidate)
  (physical-reseed-follow-activation-building-candidate context candidate))

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

(defun physical-reseed-follow-activation-publish-building (context value check)
  ;; Check the canonical candidate and future capacity before creating the
  ;; temporary.  Keep checking the frozen kind and attempt during the write.
  (let* ((candidate (physical-reseed-follow-activation-building-candidate context value))
         (*physical-reseed-follow-activation-candidate* (physical-reseed-follow-copy candidate))
         (owner (physical-reseed-follow-activation-context-owner context))
         (previous (physical-reseed-follow-activation-context-snapshot context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-activation-context-descriptors context) :root)))
    (funcall check)
    (physical-reseed-follow-configuration-publication-candidate owner context candidate)
    (physical-reseed-follow-check-future-capacity candidate)
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
  ;; Promote only the exact next Building temporary after checking its held
  ;; identity and the active attempt.  An uncertain failure fences the owner.
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
