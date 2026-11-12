(in-package #:pg)

;; Copy from a read scope into an exclusive receive scope.  Check length, digest,
;; and descriptor and pathname facts before accepting the file.  An error leaves
;; any partial destination in place for the caller to inspect or clean up.

(defun physical-image-copy-file
    (source-context target-context relative size expected-digest &key (source-size size))
  (physical-image-check-context source-context)
  (physical-image-check-context target-context)
  (unless (and (physical-image-read-context-p source-context)
               (not (physical-image-read-context-p target-context))
               (stringp relative)
               (typep size '(unsigned-byte 64))
               (typep source-size '(unsigned-byte 64)) (<= size source-size)
               (typep expected-digest '(unsigned-byte 64)))
    (error "physical image copy requires read and receive scopes with a bounded file entry"))
  (let* ((*physical-image-context* source-context)
         (path (pgdata-resolve-path relative (physical-image-context-root-path source-context))))
    (with-physical-image-input (input path :element-type '(unsigned-byte 8))
      (let* ((input-fd (sb-sys:fd-stream-fd input))
             (before (physical-image-file-facts input-fd)))
        (unless (= source-size (sixth before)) (error "physical image copy source length changed"))
        (let ((*physical-image-context* target-context))
          (call-with-physical-image-output
           (physical-image-context-root-path target-context) relative
           (lambda (output)
             (let ((buffer (make-array 65536 :element-type '(unsigned-byte 8)))
                   (digest #xCBF29CE484222325))
               (loop with remaining = size
                     while (plusp remaining)
                     for count = (read-sequence buffer input :end (min remaining (length buffer)))
                     do (when (zerop count) (error "physical image copy source was truncated"))
                        (setf digest (physical-store-update-digest digest (subseq buffer 0 count)))
                        (write-sequence buffer output :end count)
                        (decf remaining count)
                        (when *physical-image-test-hook*
                          (funcall *physical-image-test-hook* :after-copy-chunk relative)))
               (unless (and (or (< size source-size) (eq :eof (read-byte input nil :eof)))
                            (= expected-digest digest)
                            (equal before (physical-image-file-facts input-fd))
                            (equal before (physical-image-file-facts -100 (namestring path))))
                 (error "physical image copy source identity or checksum changed"))
               (list relative size digest)))))))))

(defvar *physical-reseed-image-test-hook* nil)

(defun physical-reseed-image-test-point (point directory)
  (when *physical-reseed-image-test-hook*
    (funcall *physical-reseed-image-test-hook* point directory)))

;; Verify the phase-zero base and its authentication policy, returning the
;; manifest and digest for comparison with the saved installation plan.
(defun physical-reseed-image-base-state (directory)
  (multiple-value-bind (manifest digest) (recovery-read-manifest directory)
    (recovery-check-manifest-schema manifest)
    (unless (zerop (getf (physical-recovery-read-control directory) :phase))
      (error "reseed preparation requires an immutable phase-zero base"))
    (let ((*pgdata* directory))
      (physical-recovery-verify-base-state)
      (authentication-check-backup-policy directory))
    (values manifest digest)))

;; Verify the phase-zero image and inventory, including digests for all six
;; metadata files and the manifest files. Compare with EXPECTED when supplied.
;; The optional reseed installation flag defaults to NIL.  When true, the
;; descriptor inventory admits the auxiliary guard names at the root.  The
;; auxiliary scope must check their bytes and identities before and after this
;; image verification.
(defun physical-reseed-image-check (directory &optional expected reseed-installation)
  (multiple-value-bind (manifest digest) (physical-reseed-image-base-state directory)
    (physical-backup-check-received-inventory directory manifest :reseed-installation reseed-installation)
    (let ((actual (list :manifest manifest :manifest-digest digest
                        :files (physical-backup-bundle-entries directory manifest))))
      (when (and expected (not (equal actual expected)))
        (error "reseed image changed during preparation"))
      actual)))

;; Return current file identity observations alongside the verified sizes and
;; digests.  A later installation must check them again.

(defun physical-reseed-image-file-facts (directory image)
  (loop for (name size digest) in (getf image :files)
        collect
        (let ((path (pgdata-resolve-path name directory)))
          (with-physical-image-input (input path :element-type '(unsigned-byte 8))
            (let* ((fd (sb-sys:fd-stream-fd input))
                   (facts (physical-reseed-file-facts (sb-posix:fstat fd))))
              (unless (and (= size (sixth facts))
                           (equal facts (physical-reseed-file-facts (sb-posix:fstat fd)))
                           (equal facts (physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
                (error "reseed incoming file identity changed"))
              (list name size digest facts))))))

;; Compare mount IDs through held descriptors, including the rechecked target,
;; so equal device numbers alone do not conceal distinct mounts.

(defun physical-reseed-image-check-mounts (owner stage-fd incoming-fd)
  (let ((root-fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf root-fd (physical-backup-open-directory (physical-adoption-owner-directory owner))))
           (physical-reseed-check-owner owner)
           (let ((root (sb-posix:fstat root-fd)))
             (unless (and (= (physical-adoption-owner-device owner) (sb-posix:stat-dev root))
                          (= (physical-adoption-owner-inode owner) (sb-posix:stat-ino root))
                          (= (physical-adoption-install-mount-id nil :directory-fd root-fd)
                             (physical-adoption-install-mount-id nil :directory-fd stage-fd)
                             (physical-adoption-install-mount-id nil :directory-fd incoming-fd)))
               (error "reseed workspaces must share the owned target mount"))))
      (when root-fd (sb-posix:close root-fd)))))

;; READY must describe this manifest and the original lease request.

(defun physical-reseed-image-check-source (client record image)
  (physical-backup-lease-check-received-status
   (getf image :manifest) (getf image :manifest-digest) (getf record :request)
   (pg-client-native-backup-lease-status client (getf record :request))))

;; Copy a verified received stage into a separate empty incoming directory.
;; Check the original source is READY before and after the copy, and recheck the
;; journal, both images, and workspace identities.  The returned facts are
;; observations only: no install journal, moves, handoff, or startup retirement
;; are performed here.  Failed copies retain their partial incoming contents.

(defun physical-reseed-prepare-image (owner incoming &key helper)
  (unless (eq (type-of owner) 'physical-reseed-owner)
    (error "reseed image preparation requires ordinary stopped-reseed ownership"))
  (let ((record (physical-reseed-attempt-recover owner)))
    (unless (and (eq :premanifest (getf record :phase)) (getf record :stage))
      (error "reseed preparation requires the original active attempt and frozen stage"))
    (call-with-physical-reseed-stage
     owner (getf (getf record :stage) :directory)
     (lambda (stage stage-facts stage-fd)
       (call-with-physical-reseed-stage
        owner incoming
        (lambda (incoming incoming-facts incoming-fd)
          (when (or (uiop:subpathp (pathname stage) (pathname incoming))
                    (uiop:subpathp (pathname incoming) (pathname stage)))
            (error "reseed incoming workspace must be separate from its stage"))
          (physical-reseed-image-check-mounts owner stage-fd incoming-fd)
          (call-with-physical-reseed-client
           owner record
           (lambda (client)
             (call-with-physical-backup-image-directory
              stage-fd
              (lambda (source-root)
                (let ((source-context *physical-image-context*)
                      (image (physical-reseed-image-check source-root)))
                  (physical-reseed-image-check-source client record image)
                  (physical-reseed-image-test-point :before-clone incoming)
                  (physical-reseed-require-journal owner record)
                  (physical-reseed-stage-facts owner stage stage-fd :expected stage-facts)
                  (physical-reseed-stage-facts owner incoming incoming-fd :empty t :expected incoming-facts)
                  (call-with-physical-backup-receive-directory
                   incoming-fd
                   (lambda (target-root)
                     (let ((target-context *physical-image-context*))
                       (dolist (entry (getf image :files))
                         (destructuring-bind (name size digest) entry
                           (physical-image-copy-file source-context target-context name size digest)))
                       (physical-reseed-image-check target-root image)
                       (physical-reseed-image-test-point :after-clone incoming)
                       (let ((*physical-image-context* source-context))
                         (physical-reseed-image-check source-root image))
                       (physical-reseed-image-check-source client record image)
                       (physical-reseed-require-journal owner record)
                       (physical-reseed-image-check target-root image)
                       (physical-reseed-stage-facts owner stage stage-fd :expected stage-facts)
                       (physical-reseed-stage-facts owner incoming incoming-fd :expected incoming-facts)
                       (list :incoming incoming-facts :stage stage-facts
                             :manifest (getf image :manifest)
                             :manifest-digest (getf image :manifest-digest)
                             :files (physical-reseed-image-file-facts target-root image)))))))))
           :helper helper))
        :empty t))
     :empty nil :expected (getf record :stage))))
