(in-package #:pg)

;; A scoped file replacement resource, following the temporary write, fsync,
;; rename, file and directory sync order used by SaveSlotToPath in slot.c.
;; The caller supplies the protocol and any additional outer-directory sync.

(defstruct (logical-slot-continuation-replacement
            (:constructor %make-logical-slot-continuation-replacement)
            (:print-function physical-libpq-print-private-object))
  ;; The parent directory is borrowed; the scope owns the old and new file
  ;; descriptors. Images are detached copies.
  directory parent-facts name temporary old-image new-image guard
  old-fd old-facts new-fd new-identity
  (prefix 0) (temporary-present nil) (phase :old))

(defvar *logical-slot-continuation-replacement-test-hook* nil)

(defun logical-slot-continuation-replacement-point (point)
  (when *logical-slot-continuation-replacement-test-hook*
    (funcall *logical-slot-continuation-replacement-test-hook* point)))

(defun logical-slot-continuation-replacement-open (resource new)
  ;; Open and retain independent old and new descriptors, checking the named
  ;; private inode against the held descriptor before accepting either one.
  (let* ((directory (logical-slot-continuation-replacement-directory resource))
         (name (if new (logical-slot-continuation-replacement-temporary resource)
                   (logical-slot-continuation-replacement-name resource)))
         (before (when (slot-state-lstat (slot-state-fd-path directory name))
                   (physical-image-file-facts directory name))))
    (sb-sys:without-interrupts
      (let ((fd (physical-image-openat
                 directory name
                 (logior (if new sb-posix:o-rdwr sb-posix:o-rdonly)
                         sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+
                         (if (and new (not before)) (logior sb-posix:o-creat sb-posix:o-excl) 0))
                 #o600)))
        (when (minusp fd) (error "cannot open continuation replacement input"))
        (if new (setf (logical-slot-continuation-replacement-new-fd resource) fd)
            (setf (logical-slot-continuation-replacement-old-fd resource) fd))
        (let ((facts (physical-image-file-facts fd)))
          (if new
              (setf (logical-slot-continuation-replacement-new-identity resource)
                    (logical-slot-copy-source-proof-directory-identity facts))
              (setf (logical-slot-continuation-replacement-old-facts resource) facts))
          (unless (and (or (and new (not before)) (equal before facts))
                       (equal facts (physical-image-file-facts directory name))
                       (logical-slot-copy-import-control-private-facts-p
                        facts (physical-image-file-facts directory)))
            (error "replacement input lost its private named inode")))))))

(defun logical-slot-continuation-replacement-check (resource)
  ;; Before rename the old file must retain its full named facts.  After rename
  ;; its held, unlinked inode and bytes must remain stable.  The new file must
  ;; retain its named identity and the exact written prefix in either phase.
  (funcall (logical-slot-continuation-replacement-guard resource))
  (let* ((directory (logical-slot-continuation-replacement-directory resource))
         (parent (physical-image-file-facts directory))
         (name (logical-slot-continuation-replacement-name resource))
         (temporary (logical-slot-continuation-replacement-temporary resource))
         (selected (member (logical-slot-continuation-replacement-phase resource) '(:selected :synced)))
         (old-fd (logical-slot-continuation-replacement-old-fd resource))
         (old-facts (logical-slot-continuation-replacement-old-facts resource))
         (old-image (logical-slot-continuation-replacement-old-image resource))
         (new-fd (logical-slot-continuation-replacement-new-fd resource)))
    (unless (and (equal (logical-slot-copy-source-proof-directory-identity parent)
                        (logical-slot-continuation-replacement-parent-facts resource))
                 (equal (sort (append (list name)
                                      (when (and (not selected)
                                                 (logical-slot-continuation-replacement-temporary-present resource))
                                        (list temporary))) #'string<)
                        (logical-slot-copy-import-control-names directory 3)))
      (error "continuation replacement directory changed"))
    (if selected
        (unless (and (equal (logical-slot-copy-source-proof-directory-identity old-facts)
                            (logical-slot-copy-source-proof-directory-identity (physical-image-file-facts old-fd)))
                     (equalp old-image (logical-slot-copy-import-control-read-fd old-fd (length old-image))))
          (error "unlinked continuation replacement input changed"))
        (logical-slot-imported-check-held-file directory name old-fd old-facts old-image))
    (when new-fd
      (let* ((facts (physical-image-file-facts new-fd))
             (image (logical-slot-continuation-replacement-new-image resource))
             (prefix (logical-slot-continuation-replacement-prefix resource))
             (observed (logical-slot-copy-import-control-read-fd new-fd (length image))))
        (unless (and (equal (logical-slot-continuation-replacement-new-identity resource)
                            (logical-slot-copy-source-proof-directory-identity facts))
                     (logical-slot-copy-import-control-private-facts-p facts parent)
                     (equal facts (physical-image-file-facts directory (if selected name temporary)))
                     (= prefix (length observed))
                     (null (mismatch observed image :end2 prefix))
                     (equal facts (physical-image-file-facts new-fd)))
          (error "continuation replacement output changed"))))
    (funcall (logical-slot-continuation-replacement-guard resource))
    (logical-slot-continuation-replacement-phase resource)))

(defun logical-slot-continuation-replacement-open-temporary (resource)
  ;; Resume only an existing temporary that is an exact prefix of the new bytes.
  (logical-slot-continuation-replacement-open resource t)
  (let* ((image (logical-slot-continuation-replacement-new-image resource))
         (prefix (logical-slot-copy-import-control-read-fd
                  (logical-slot-continuation-replacement-new-fd resource) (length image))))
    (unless (null (mismatch prefix image :end2 (length prefix)))
      (error "continuation replacement temporary is not an exact prefix"))
    (setf (logical-slot-continuation-replacement-prefix resource) (length prefix)
          (logical-slot-continuation-replacement-temporary-present resource) t)))

(defun logical-slot-continuation-replacement-prepare (resource)
  ;; Complete the checked prefix and sync the temporary before selection.
  (declare (notinline sb-posix:write sb-posix:fsync))
  (unless (eq :old (logical-slot-continuation-replacement-phase resource))
    (error "replacement preparation requires the old selected file"))
  (unless (logical-slot-continuation-replacement-new-fd resource)
    (logical-slot-continuation-replacement-open-temporary resource))
  (let ((image (logical-slot-continuation-replacement-new-image resource))
        (fd (logical-slot-continuation-replacement-new-fd resource)))
    (sb-posix:lseek fd (logical-slot-continuation-replacement-prefix resource) sb-posix:seek-set)
    (sb-sys:with-pinned-objects (image)
      (loop for position = (logical-slot-continuation-replacement-prefix resource)
            while (< position (length image))
            for count = (min 65536 (- (length image) position))
            do (logical-slot-continuation-replacement-check resource)
               (let ((written (sb-posix:write fd (sb-sys:sap+ (sb-sys:vector-sap image) position) count)))
                 (unless (<= 1 written count) (error "short continuation replacement write"))
                 (incf (logical-slot-continuation-replacement-prefix resource) written))
               (logical-slot-continuation-replacement-point :after-write)))
    (logical-slot-continuation-replacement-check resource)
    (sb-posix:fsync fd)
    (logical-slot-continuation-replacement-point :after-temporary-sync)
    (logical-slot-continuation-replacement-check resource)
    (setf (logical-slot-continuation-replacement-phase resource) :prepared)))

(defun logical-slot-continuation-replacement-replace (resource)
  ;; Rename and record selection without an interrupt between the two steps.
  (unless (eq :prepared (logical-slot-continuation-replacement-phase resource))
    (error "replacement requires a synchronized temporary"))
  (logical-slot-continuation-replacement-check resource)
  (let ((directory (logical-slot-continuation-replacement-directory resource)))
    (sb-sys:without-interrupts
      (unless (zerop (port-renameat directory (logical-slot-continuation-replacement-temporary resource)
                                    directory (logical-slot-continuation-replacement-name resource)))
        (error 'sb-posix:syscall-error :name 'renameat :errno (sb-alien:get-errno)))
      (setf (logical-slot-continuation-replacement-phase resource) :selected)))
  (logical-slot-continuation-replacement-point :after-replace)
  :selected)

(defun logical-slot-continuation-replacement-sync (resource)
  ;; Sync the selected file and its directory.  For slot state this does not
  ;; sync the outer pg_replslot root; a writer must do that before declaring P7N.
  (declare (notinline sb-posix:fsync))
  (unless (eq :selected (logical-slot-continuation-replacement-phase resource))
    (error "replacement sync requires the new selected file"))
  (sb-posix:fsync (logical-slot-continuation-replacement-new-fd resource))
  (logical-slot-continuation-replacement-point :after-selected-sync)
  (logical-slot-continuation-replacement-check resource)
  (sb-posix:fsync (logical-slot-continuation-replacement-directory resource))
  (logical-slot-continuation-replacement-point :after-directory-sync)
  (logical-slot-continuation-replacement-check resource)
  (setf (logical-slot-continuation-replacement-phase resource) :synced))

(defun logical-slot-continuation-replacement-close (resource complete)
  ;; Close owned descriptors without unlinking staged files or rolling back.
  ;; Report cleanup failures without hiding the original unsuccessful exit.
  (declare (notinline sb-posix:close))
  (let ((failure nil))
    (dolist (kind '(:new :old))
      (let ((fd (ecase kind
                  (:new (shiftf (logical-slot-continuation-replacement-new-fd resource) nil))
                  (:old (shiftf (logical-slot-continuation-replacement-old-fd resource) nil))))
            (expected (ecase kind
                        (:new (logical-slot-continuation-replacement-new-identity resource))
                        (:old (let ((facts (logical-slot-continuation-replacement-old-facts resource)))
                                (when facts (logical-slot-copy-source-proof-directory-identity facts)))))))
        (when fd
          (handler-case
              (progn
                (when (and expected
                           (not (equal (list (first expected) (second expected) (fifth expected))
                                       (logical-slot-copy-install-file-identity
                                        (physical-image-file-facts fd)))))
                  (error "continuation replacement descriptor was reused"))
                (sb-posix:close fd))
            (error (condition) (unless failure (setf failure condition)))))))
    (when failure
      (if complete (error failure)
          (report-query-temp-cleanup-failure "continuation replacement" failure)))))

(defun logical-slot-continuation-replacement-receipt (resource)
  (copy-list
   (physical-image-file-facts
    (ecase (logical-slot-continuation-replacement-phase resource)
      (:old (logical-slot-continuation-replacement-old-fd resource))
      (:synced (logical-slot-continuation-replacement-new-fd resource))))))

(defun call-with-logical-slot-continuation-file-replacement
    (directory purpose old-image new-image guard function)
  ;; Lend a thread-bound operation callback over bounded opaque images and the
  ;; sealed selector or slot-state names.  Canonical form, protocol, complete
  ;; inventory and authority are the caller's responsibility.  The guard must
  ;; follow the actual authority across phases: an old runtime-snapshot guard
  ;; is no longer valid after P7N.  This scope does not mint authority.
  ;;
  ;; A normal return requires :old (no mutation) or :synced.  A failed operation
  ;; revokes the callback; interrupted partial stages remain for higher-level
  ;; recovery and the callback expires on return.
  (multiple-value-bind (name temporary maximum)
      (ecase purpose
        (:selector (values "selector" "selector.tmp" (logical-slot-continuation-prepared-maximum-bytes)))
        (:slot-state (values "state" "state.tmp" 2048)))
    (unless (and (functionp guard) (functionp function)
                 (every (lambda (image)
                          (and (typep image '(vector (unsigned-byte 8))) (<= 1 (length image) maximum)))
                        (list old-image new-image)))
      (error "invalid continuation replacement inputs"))
    (funcall guard)
    (let* ((facts (physical-image-file-facts directory))
           (resource (%make-logical-slot-continuation-replacement
                      :directory directory :parent-facts (logical-slot-copy-source-proof-directory-identity facts)
                      :name name :temporary temporary :old-image (copy-seq old-image) :new-image (copy-seq new-image)
                      :guard guard))
           (active t) (failed nil) (complete nil) (thread sb-thread:*current-thread*))
      (unless (and (sb-posix:s-isdir (third facts)) (= #o700 (logand #o7777 (third facts)))
                   (= (fifth facts) (sb-posix:geteuid)))
        (error "continuation replacement requires a private directory"))
      (labels ((operation (operation)
                 (unless (and active (eq thread sb-thread:*current-thread*))
                   (error "continuation replacement ended or changed thread"))
                 (when failed (error "continuation replacement failed"))
                 (let ((finished nil))
                   (sb-sys:without-interrupts
                     (unwind-protect
                          (multiple-value-prog1
                              (sb-sys:with-local-interrupts
                                (logical-slot-continuation-replacement-check resource)
                                (let ((result
                                        (ecase operation
                                          (:check nil)
                                          (:prepare (logical-slot-continuation-replacement-prepare resource))
                                          (:replace (logical-slot-continuation-replacement-replace resource))
                                          (:sync (logical-slot-continuation-replacement-sync resource))
                                          (:receipt (logical-slot-continuation-replacement-receipt resource)))))
                                  (let ((phase (logical-slot-continuation-replacement-check resource)))
                                    (if (eq operation :receipt) result phase))))
                            (setf finished t))
                       (unless finished (setf failed t)))))))
        (sb-sys:without-interrupts
          (unwind-protect
               (sb-sys:with-local-interrupts
                 (logical-slot-continuation-replacement-open resource nil)
                 (when (slot-state-lstat (slot-state-fd-path directory temporary))
                   (logical-slot-continuation-replacement-open-temporary resource))
                 (operation :check)
                 (multiple-value-prog1 (funcall function #'operation)
                   (operation :check)
                   (unless (member (logical-slot-continuation-replacement-phase resource) '(:old :synced))
                     (error "continuation replacement returned before synchronization"))
                   (setf complete t)))
            (setf active nil)
            (logical-slot-continuation-replacement-close resource complete)))))))
