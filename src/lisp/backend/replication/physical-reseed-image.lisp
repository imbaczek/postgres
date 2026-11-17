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

(defun physical-rewind-prove-shared-history (owner source-root source-image origin-identity)
  "Compare shared WAL from the target's export base up to the fork.
Hold the stopped target's lock and the verified source image's descriptor scope.
Require matching cluster identities and a source timeline descended from the
target timeline. Return the verified interval, endpoints and source manifest
digest. Establish the source's ancestry before the export base separately."
  (unless (and source-image *physical-image-context*)
    (error "native rewind requires a previously verified source image and its descriptor scope"))
  (let* ((target (let ((*physical-image-context* nil))
                   (physical-adoption-inspect-primary owner)))
         (origin (let ((*physical-image-context* nil))
                   (physical-adoption-prove-primary-origin target origin-identity)))
         (manifest (getf (physical-reseed-image-check source-root source-image) :manifest))
         (source-timeline (getf manifest :timeline))
         (start (third (getf origin :identity)))
         (source-history (timeline-read-history source-timeline source-root))
         (source-namespace
           (let ((*pgdata* source-root))
             (nth-value 5 (wal-read-control-fields)))))
    (unless (and (= (getf target :system-id) (getf manifest :system-id))
                 (equalp (getf target :cluster-identity) (getf manifest :cluster-identity))
                 (/= (getf target :timeline) source-timeline))
      (error "native rewind requires matching cluster identities on different timelines"))
    (multiple-value-bind (common-timeline fork)
        (timeline-common-ancestor (getf target :history) source-history)
      (unless (and (= common-timeline (getf target :timeline))
                   fork (< start fork (getf target :end)) (< fork (getf manifest :end)))
        (error "native rewind requires retained shared WAL and complete divergent endpoints"))
      (let ((*pgdata* (physical-adoption-owner-directory owner))
            (*physical-image-context* nil))
        (wal-check-shared-lineage-prefix
         (getf target :history) source-root source-history start fork
         :left-namespace (getf target :namespace) :right-namespace source-namespace)
        (physical-adoption-check-owner owner)
        (unless (and (equalp (getf target :capture) (native-control-capture))
                     (equalp (getf target :history) (timeline-read-history (getf target :timeline))))
          (error "native rewind target changed during shared WAL inspection")))
      (physical-reseed-image-check source-root source-image)
      (list :system-id (getf target :system-id)
            :cluster-identity (getf target :cluster-identity)
            :common-timeline common-timeline :start start :fork fork
            :target-end (getf target :end) :source-end (getf manifest :end)
            :source-manifest-digest (getf source-image :manifest-digest)))))

(defun physical-rewind-prove-leased-origin
    (owner source-root source-image origin-identity client request)
  "Prove shared WAL for the locked target and a verified source image.
Hold the image's descriptor scope and use the admitted source connection.
Bind the image to the source's READY lease and match the shared interval and
digest against the retained root. Recheck the target owner and source image.
Return the interval proof with the retained root ID and fork digest.
Installation requires separate checks."
  (let* ((proof (physical-rewind-prove-shared-history owner source-root source-image origin-identity))
         (manifest (getf source-image :manifest))
         (fork (getf proof :fork))
         (history-text (bytes->string (timeline-history-bytes (getf manifest :timeline) source-root)))
         (digest #xCBF29CE484222325))
    (let* ((*pgdata* (physical-adoption-owner-directory owner))
           (*physical-image-context* nil)
           (target (physical-adoption-inspect-primary owner))
           (capture (getf target :capture)))
      (unless (= (getf target :end) (getf proof :target-end))
        (error "rewind target changed before hashing shared WAL"))
      (multiple-value-bind (cursor files complete)
          (wal-scan-lineage
           (lambda (record timeline carrier bytes)
             (declare (ignore record timeline carrier))
             (setf digest (physical-store-update-digest digest bytes))
             nil)
           (getf target :history) (getf proof :start) fork
           :namespace (getf target :namespace) :include-raw t)
        (declare (ignore files))
        (unless (and complete (= cursor fork))
          (error "rewind target did not reach the shared fork")))
      (physical-adoption-check-owner owner)
      (unless (equalp capture (native-control-capture))
        (error "rewind target changed while hashing shared WAL")))
    (let ((root
            (pg-client-native-backup-lease-rewind-proof
             client request manifest (getf source-image :manifest-digest)
             (list :system-id (getf proof :system-id)
                   :base-timeline (getf proof :common-timeline) :base (getf proof :start)
                   :manifest-digest (fourth origin-identity) :end fork)
             history-text digest)))
      (physical-adoption-check-owner owner)
      (physical-reseed-image-check source-root source-image)
      (append proof (list :root-id root :fork-digest digest)))))

(defun physical-rewind-relation-offer (directory name)
  (let ((path (pgdata-resolve-path name directory)))
    (with-physical-image-input (stream path :element-type '(unsigned-byte 8))
      (let* ((fd (sb-sys:fd-stream-fd stream))
             (before (physical-image-file-facts fd))
             (size (sixth before))
             (buffer (make-array 65536 :element-type '(unsigned-byte 8)))
             (digest #xCBF29CE484222325))
        (loop with remaining = size
              while (plusp remaining)
              for count = (read-sequence buffer stream :end (min remaining (length buffer)))
              do (when (zerop count) (error "native rewind offer file was truncated"))
                 (setf digest (physical-store-update-digest-span digest buffer 0 count))
                 (decf remaining count))
        (unless (and (eq :eof (read-byte stream nil :eof))
                     (equal before (physical-image-file-facts fd))
                     (equal before (physical-image-file-facts -100 (namestring path))))
          (error "native rewind offer file changed"))
        (list name size digest)))))

(defun physical-rewind-capture-image (owner destination destination-fd client request)
  (let* ((target (physical-adoption-inspect-primary owner))
         (reused nil))
    (call-with-physical-directory-root
     owner
     (lambda (root-fd anchored)
       (declare (ignore anchored))
       (call-with-physical-backup-image-directory
        root-fd
        (lambda (source-root)
          (let* ((source-context *physical-image-context*)
                 (*pgdata* source-root)
                 (*catalog* (getf target :catalog))
                 (offers
                   (loop for name in (sort (physical-bootstrap-relation-inventory) #'string<)
                         repeat +native-rewind-offer-limit+
                         collect (physical-rewind-relation-offer source-root name)))
                 (offered (make-hash-table :test #'equal)))
            (dolist (entry offers)
              (setf (gethash (first entry) offered) entry))
            (multiple-value-bind (manifest record)
                (let ((*pgdata* (physical-adoption-owner-directory owner))
                      (*physical-image-context* nil))
                  (physical-backup-lease-capture-remote
                   destination client request :directory-fd destination-fd :offers offers
                   :reuse (when offers
                            (lambda (entry)
                              (destructuring-bind (name size digest) entry
                                (unless (equal entry (gethash name offered))
                                  (error "native rewind source requested an unoffered relation"))
                                (physical-image-copy-file source-context *physical-image-context*
                                                          name size digest)
                                (push entry reused))))))
              (physical-adoption-check-owner owner)
              (let ((*pgdata* (physical-adoption-owner-directory owner))
                    (*physical-image-context* nil))
                (unless (equalp (getf target :capture) (native-control-capture))
                  (error "native rewind target changed during capture")))
              (values manifest record (nreverse reused))))))))))

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
